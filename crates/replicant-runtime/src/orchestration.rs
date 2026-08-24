//! Empire-level Automation Director.
//!
//! The Director owns *desired strategic state*, not game commands. It observes
//! managed state, expands standing goals into regional campaign work, assigns
//! permanently regional Replicants, and creates durable workflows when policy
//! allows. Workflow executors remain responsible for all mechanical actions.
//!
//! Workforce management is deliberately grow-only. The Director may provision
//! additional Replicants when sustained useful work is blocked on capacity, but
//! it never deletes, retires, or automatically unassigns an existing Replicant.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::{StreamExt, stream};
use replicant_client::{
    Client, Device, DeviceType, Location, Replicant, Star,
    domain::{GalacticPosition, Inventory, InventoryOwner},
    raw::RequestPriority,
};
use replicant_protocol::{
    DirectorGoalKind, DirectorGoalStatus, DirectorGoalSummary, DirectorMode, DirectorRegionStatus,
    DirectorRegionSummary, DirectorReplicantAssignment, DirectorSnapshot, DirectorWorkforceSummary,
    SnapshotMetadata, WorkflowId as ProtocolWorkflowId,
};
use replicant_transport::ResourceMap;
use replicant_workflow::{
    ResourceKey, WorkflowId, WorkflowInstance, WorkflowRepository, WorkflowStatus,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{
    ApplicationError,
    automation::{
        BeltSearchCampaignIntent, BlueprintAcquireIntent, BlueprintShopPurchaseIntent,
        EventCampaignIntent, ExplorationIntent, LogisticsManifestIntent, MiningCampaignIntent,
        ObservatoryIntent, RegionEstablishIntent, ReplicantProvisionIntent, ScanTourIntent,
        blueprint_acquire_workflow_kind, blueprint_source_is_candidate, blueprint_source_location,
        exploration_workflow_kind, new_belt_search_campaign_workflow,
        new_blueprint_acquire_workflow, new_event_campaign_workflow, new_exploration_workflow,
        new_logistics_manifest_workflow, new_mining_campaign_workflow, new_observatory_workflow,
        new_region_establish_workflow, new_replicant_provision_workflow, new_scan_tour_workflow,
    },
    director_requirements::{
        DirectorRequirement, DirectorRequirementGraph, load_requirement_summaries,
    },
    event::active_events,
    trade::{TradeBundle, TraderSummary, shop_trades, trader_directory},
};

const SETTINGS_NS: &str = "director.settings";
const SETTINGS_KEY: &str = "singleton";
const GOAL_CONTROL_NS: &str = "director.goal_control";
const GOAL_RUNTIME_NS: &str = "director.goal_runtime";
const REPLICANT_NS: &str = "director.replicant";
const WORKFORCE_NS: &str = "director.workforce";
const SNAPSHOT_NS: &str = "director.snapshot";
const BLUEPRINT_SHOP_NS: &str = "director.blueprint_shop_opportunity";
const BLUEPRINT_SHOP_CACHE_NS: &str = "director.blueprint_shop_snapshot";
const BLUEPRINT_SHOP_CACHE_KEY: &str = "latest";
const BLUEPRINT_CATALOGUE_CACHE_NS: &str = "director.blueprint_catalogue";
const BLUEPRINT_CATALOGUE_CACHE_KEY: &str = "latest";
const HUB_REFRESH_CACHE_NS: &str = "director.system_hub_refresh";
const HUB_REFRESH_CACHE_KEY: &str = "latest";
const ACTIVE_EVENT_CACHE_NS: &str = "director.active_event_snapshot";
const ACTIVE_EVENT_CACHE_KEY: &str = "latest";
const SNAPSHOT_KEY: &str = "latest";

const DEFAULT_HOLD_MS: i64 = 30 * 60 * 1000;
const DEFAULT_SCALE_COOLDOWN_MS: i64 = 6 * 60 * 60 * 1000;
const DEFAULT_PROSPECT_COOLDOWN_MS: i64 = 10 * 60 * 1000;
const DEFAULT_RETRY_COOLDOWN_MS: i64 = 5 * 60 * 1000;
const DEFAULT_IDLE_TARGET: f64 = 0.15;
const DEFAULT_SCALE_THRESHOLD: f64 = 0.10;
const MINING_BATCH_SIZE: usize = 12;
const CATALOGUE_SYSTEMS_PER_WORKER: usize = 20;
const MAX_PARALLEL_CATALOGUE_WORKERS: usize = 4;
// A system hub has 15 LY operational reach. An owned hub just outside a named
// region can therefore serve as that region's gateway capital when it can
// directly reach at least one known star inside the region.
pub(crate) const REGION_GATEWAY_HUB_RANGE_LY: f64 = 15.0;
const EVENT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(12);
const BLUEPRINT_SHOP_TIMEOUT: Duration = Duration::from_secs(10);
const BLUEPRINT_SHOP_CONCURRENCY: usize = 6;
const BLUEPRINT_SHOP_CACHE_TTL_MS: i64 = 10 * 60 * 1000;
const BLUEPRINT_SHOP_PARTIAL_CACHE_TTL_MS: i64 = 2 * 60 * 1000;
const BLUEPRINT_CATALOGUE_CACHE_TTL_MS: i64 = 30 * 60 * 1000;
const HUB_REFRESH_CACHE_TTL_MS: i64 = 5 * 60 * 1000;
const ACTIVE_EVENT_CACHE_TTL_MS: i64 = 2 * 60 * 1000;
const ACTIVE_EVENT_STALE_FALLBACK_MS: i64 = 10 * 60 * 1000;

const PRIORITY_REGION_ESTABLISHMENT: u32 = 900;
const PRIORITY_EVENT_COMPLETION: u32 = 700;
const PRIORITY_FTL_EXPANSION: u32 = 650;
const PRIORITY_CATALOGUE: u32 = 500;
const PRIORITY_MINING: u32 = 450;
const PRIORITY_CATALOGUE_BLUEPRINT: u32 = 400;

/// Durable Automation Director settings.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DirectorSettings {
    /// Whether planning is off, advisory-only, or automatic.
    pub mode: DirectorMode,
    /// Desired empire/regional idle reserve shown in the workforce readout.
    pub idle_target: f64,
    /// Scale-up threshold when useful work is blocked on worker capacity.
    pub scale_up_idle_threshold: f64,
    /// How long ordinary worker pressure must persist before cloning.
    pub scale_up_hold_ms: i64,
    /// Minimum time between successful scale-up decisions in one region.
    pub scale_up_cooldown_ms: i64,
    /// Minimum delay between observatory prospect attempts.
    pub prospect_cooldown_ms: i64,
}

impl Default for DirectorSettings {
    fn default() -> Self {
        Self {
            mode: DirectorMode::Advisory,
            idle_target: DEFAULT_IDLE_TARGET,
            scale_up_idle_threshold: DEFAULT_SCALE_THRESHOLD,
            scale_up_hold_ms: DEFAULT_HOLD_MS,
            scale_up_cooldown_ms: DEFAULT_SCALE_COOLDOWN_MS,
            prospect_cooldown_ms: DEFAULT_PROSPECT_COOLDOWN_MS,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct GoalControl {
    enabled: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct GoalRuntime {
    #[serde(default)]
    active_workflows: Vec<WorkflowId>,
    last_launch_at_ms: Option<i64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct ReplicantAssignmentRecord {
    region: Option<String>,
    role_affinity: Option<String>,
    assigned_at_ms: i64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct RegionWorkforceState {
    pressure_since_ms: Option<i64>,
    last_scaled_at_ms: Option<i64>,
    provision_workflow_id: Option<WorkflowId>,
}

#[derive(Clone, Debug)]
struct RegionView {
    region: String,
    status: DirectorRegionStatus,
    hub_system: Option<String>,
    hub_location: Option<String>,
    known_systems: BTreeSet<String>,
}

#[derive(Clone, Debug)]
struct WorkerView {
    replicant: Replicant,
    region: Option<String>,
    role_affinity: Option<String>,
    busy_workflow: Option<WorkflowId>,
    racing_vessel: Option<String>,
}

#[derive(Clone, Debug)]
struct HubMaintenanceView {
    system: String,
    location: String,
    deficits: ResourceMap,
    grace_period_remaining: Option<i64>,
    degraded: bool,
}

#[derive(Clone, Debug)]
struct HubSupplySource {
    origin: String,
    description: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BlueprintShopOpportunity {
    device_type: String,
    controller_code: String,
    trade_code: String,
    current_stock: i64,
    criteria: TradeBundle,
    shop_location: String,
    shop_system: String,
    shop_name: Option<String>,
    last_seen_at_ms: i64,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct BlueprintShopSnapshot {
    opportunities: Vec<BlueprintShopOpportunity>,
    directory_errors: usize,
    trade_errors: usize,
    hidden_or_unlocated: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BlueprintShopCache {
    refreshed_at_ms: i64,
    #[serde(default)]
    requested_blueprints: BTreeSet<String>,
    snapshot: BlueprintShopSnapshot,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct BlueprintCatalogueCache {
    refreshed_at_ms: i64,
    #[serde(default)]
    requirement_signature: BTreeSet<String>,
    #[serde(default)]
    unlocked_device_types: BTreeSet<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct HubRefreshCache {
    #[serde(default, deserialize_with = "deserialize_hub_refresh_time")]
    refreshed_at_ms: i64,
}

fn deserialize_hub_refresh_time<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer)? {
        Value::Number(value) => value
            .as_i64()
            .ok_or_else(|| serde::de::Error::custom("hub refresh timestamp must be an integer")),
        Value::Object(_) => Ok(0),
        _ => Err(serde::de::Error::custom(
            "hub refresh timestamp must be an integer or legacy device map",
        )),
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ActiveEventCache {
    refreshed_at_ms: i64,
    #[serde(default)]
    events: Vec<CachedActiveEvent>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct CachedActiveEvent {
    designation: Option<String>,
    location: Option<String>,
}

#[derive(Clone, Debug)]
enum BlueprintAcquisitionSelection {
    Owned {
        device_type: DeviceType,
        source_code: String,
        factory_code: String,
        preferred_region: Option<String>,
    },
    Shop {
        device_type: DeviceType,
        opportunity: Box<BlueprintShopOpportunity>,
        factory_code: String,
        replicant_code: String,
        preferred_region: Option<String>,
    },
}

#[derive(Clone, Debug)]
struct StockLocation {
    location: String,
    system: String,
    region: Option<String>,
    is_belt: bool,
    resources: ResourceMap,
}

struct GoalReconcileContext<'a> {
    repository: &'a WorkflowRepository,
    workflows: &'a [WorkflowInstance],
    controls: &'a BTreeMap<DirectorGoalKind, bool>,
    automatic: bool,
    now: i64,
}

struct BlueprintReconcileContext<'a> {
    devices: &'a [Device],
    workers: &'a [WorkerView],
    reserved_workers: &'a mut BTreeSet<String>,
    unlocked_blueprints: Option<&'a BTreeSet<DeviceType>>,
    shop_snapshot: &'a BlueprintShopSnapshot,
    location_systems: &'a BTreeMap<String, String>,
    system_regions: &'a BTreeMap<String, String>,
}

/// Returns the current Director settings, creating defaults lazily when absent.
pub fn director_settings(
    repository: &WorkflowRepository,
) -> Result<DirectorSettings, ApplicationError> {
    if let Some((value, _)) = repository.read_document(SETTINGS_NS, SETTINGS_KEY)? {
        return Ok(serde_json::from_value(value)?);
    }
    let settings = DirectorSettings::default();
    repository.put_document(SETTINGS_NS, SETTINGS_KEY, &settings)?;
    Ok(settings)
}

/// Updates the Director mode without changing goal configuration.
pub fn set_director_mode(
    repository: &WorkflowRepository,
    mode: DirectorMode,
) -> Result<DirectorSettings, ApplicationError> {
    let mut settings = director_settings(repository)?;
    settings.mode = mode;
    repository.put_document(SETTINGS_NS, SETTINGS_KEY, &settings)?;
    Ok(settings)
}

/// Enables or disables one standing goal type globally.
pub fn set_goal_enabled(
    repository: &WorkflowRepository,
    kind: DirectorGoalKind,
    enabled: bool,
) -> Result<(), ApplicationError> {
    repository.put_document(
        GOAL_CONTROL_NS,
        goal_kind_key(kind),
        &GoalControl { enabled },
    )?;
    Ok(())
}

/// Permanently assigns an existing Replicant to a region.
///
/// Passing `None` is an explicit operator action that clears the assignment;
/// the Director itself never clears or moves an assignment automatically.
pub fn assign_replicant_region(
    repository: &WorkflowRepository,
    replicant: &str,
    region: Option<&str>,
    role_affinity: Option<&str>,
) -> Result<(), ApplicationError> {
    let record = ReplicantAssignmentRecord {
        region: region.map(canonical_region),
        role_affinity: role_affinity
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        assigned_at_ms: now_millis(),
    };
    repository.put_document(REPLICANT_NS, replicant, &record)?;
    Ok(())
}

/// Returns the last successful Director projection without touching managed game state.
///
/// Before the first reconciliation completes, this returns a lightweight warming-up
/// projection so the operator UI can render immediately while the background Director
/// builds the first full snapshot.
pub fn cached_director_snapshot(
    repository: &WorkflowRepository,
    revision: u64,
) -> Result<DirectorSnapshot, ApplicationError> {
    let mut snapshot = if let Some((value, _)) =
        repository.read_document(SNAPSHOT_NS, SNAPSHOT_KEY)?
    {
        serde_json::from_value(value)?
    } else {
        let settings = director_settings(repository)?;
        let controls = load_goal_controls(repository)?;
        let goals = all_goal_kinds()
            .into_iter()
            .map(|kind| {
                waiting_goal(
                    kind,
                    None,
                    &controls,
                    initial_goal_objective(kind),
                    "The Director has not completed its first reconciliation yet",
                    "Wait for the background Director pass or run Reconcile now",
                )
            })
            .collect();

        DirectorSnapshot {
                metadata: SnapshotMetadata {
                    revision,
                    generated_at_ms: now_millis(),
                },
                mode: settings.mode,
                regions: Vec::new(),
                goals,
                replicants: Vec::new(),
                requirements: load_requirement_summaries(repository)?,
                workforce: DirectorWorkforceSummary {
                    total: 0,
                    busy: 0,
                    idle: 0,
                    idle_ratio: 1.0,
                    pending_worker_demand: 0,
                    scale_up_recommended: false,
                    scale_reason: Some(
                        "Automation Director is warming up; the last successful projection is not available yet"
                            .to_owned(),
                    ),
                },
            }
    };

    apply_durable_snapshot_overrides(repository, &mut snapshot)?;
    Ok(snapshot)
}

fn apply_durable_snapshot_overrides(
    repository: &WorkflowRepository,
    snapshot: &mut DirectorSnapshot,
) -> Result<(), ApplicationError> {
    snapshot.mode = director_settings(repository)?.mode;

    let controls = load_goal_controls(repository)?;
    for goal in &mut snapshot.goals {
        goal.enabled = goal_enabled(&controls, goal.kind);
    }

    let assignments = load_assignments(repository)?;
    for replicant in &mut snapshot.replicants {
        if let Some(assignment) = assignments.get(&replicant.code) {
            replicant.region = assignment.region.clone();
            replicant.role_affinity = assignment.role_affinity.clone();
        }
    }
    for region in &mut snapshot.regions {
        region.replicants = snapshot
            .replicants
            .iter()
            .filter(|replicant| replicant.region.as_deref() == Some(region.region.as_str()))
            .map(|replicant| replicant.code.clone())
            .collect();
    }
    snapshot.requirements = load_requirement_summaries(repository)?;

    Ok(())
}

async fn refresh_system_hubs(
    client: &Client,
    repository: &WorkflowRepository,
    devices: &mut Vec<Device>,
    now: i64,
    force: bool,
) -> Result<BTreeMap<String, String>, ApplicationError> {
    let mut errors = BTreeMap::new();
    let mut expected = devices
        .iter()
        .filter(|device| device.device_type.as_ref() == Some(&DeviceType::SystemHub))
        .map(|device| device.key.id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    if expected.is_empty() {
        return Ok(errors);
    }

    let cache = repository
        .read_document(HUB_REFRESH_CACHE_NS, HUB_REFRESH_CACHE_KEY)?
        .map(|(value, _)| serde_json::from_value::<HubRefreshCache>(value))
        .transpose()?
        .unwrap_or_default();
    let age_ms = now.saturating_sub(cache.refreshed_at_ms);
    if !force && cache.refreshed_at_ms > 0 && age_ms <= HUB_REFRESH_CACHE_TTL_MS {
        tracing::debug!(
            age_ms,
            ttl_ms = HUB_REFRESH_CACHE_TTL_MS,
            phase = "devices",
            "Director reused SSE-backed System Hub state"
        );
        return Ok(errors);
    }

    match client
        .devices()
        .refresh_many()
        .of_type(DeviceType::SystemHub)
        .page_size(50)
        .collect()
        .await
    {
        Ok(handles) => {
            for handle in handles {
                let code = handle.id().as_str().to_owned();
                expected.remove(&code);
                match handle.snapshot().await {
                    Ok(refreshed) => {
                        if let Some(device) = devices
                            .iter_mut()
                            .find(|device| device.key.id.as_str() == code)
                        {
                            *device = refreshed;
                        } else {
                            devices.push(refreshed);
                        }
                    }
                    Err(error) => {
                        errors.insert(code, error.to_string());
                    }
                }
            }
            for code in expected {
                errors.insert(code, "missing from bulk System Hub refresh".to_owned());
            }
            if errors.is_empty() {
                repository.put_document(
                    HUB_REFRESH_CACHE_NS,
                    HUB_REFRESH_CACHE_KEY,
                    &HubRefreshCache {
                        refreshed_at_ms: now,
                    },
                )?;
            }
        }
        Err(error) => {
            let error = error.to_string();
            errors.extend(expected.into_iter().map(|code| (code, error.clone())));
        }
    }
    Ok(errors)
}

/// Evaluates all standing goals and, in automatic mode, creates the batch work
/// required to move them forward.
pub async fn reconcile_director(
    client: &Client,
    repository: Arc<WorkflowRepository>,
    revision: u64,
    allow_launch: bool,
    force_slow_refresh: bool,
) -> Result<DirectorSnapshot, ApplicationError> {
    let client = &client.with_priority(RequestPriority::Background);
    let started = Instant::now();
    let settings = director_settings(&repository)?;
    let now = now_millis();
    tracing::info!(
        revision,
        mode = ?settings.mode,
        allow_launch,
        priority = ?RequestPriority::Background,
        "Director reconciliation started"
    );
    let catalogue = client.galaxy().catalogue();
    tracing::debug!(phase = "locations", "Director loading managed world state");
    let locations = client.locations().find().collect().await?;
    tracing::debug!(
        phase = "locations",
        count = locations.len(),
        elapsed_ms = started.elapsed().as_millis(),
        "Director managed world phase complete"
    );
    tracing::debug!(phase = "devices", "Director loading managed world state");
    let mut devices = client.state().owned_devices()?;
    tracing::debug!(
        phase = "devices",
        count = devices.len(),
        elapsed_ms = started.elapsed().as_millis(),
        "Director managed world phase complete"
    );
    let hub_refresh_errors =
        refresh_system_hubs(client, &repository, &mut devices, now, force_slow_refresh).await?;
    let inventories = client.state().inventories()?;
    tracing::debug!(
        phase = "inventory",
        count = inventories.len(),
        elapsed_ms = started.elapsed().as_millis(),
        "Director loaded durable managed inventory projections"
    );
    tracing::debug!(phase = "replicants", "Director loading managed world state");
    let replicants = client.state().owned_replicants()?;
    tracing::debug!(
        phase = "replicants",
        count = replicants.len(),
        elapsed_ms = started.elapsed().as_millis(),
        "Director managed world phase complete"
    );
    let workflows = repository.list()?;
    tracing::debug!(
        phase = "workflows",
        count = workflows.len(),
        "Director durable workflow state loaded"
    );

    let location_systems = location_system_map(&locations);
    let system_regions = system_region_map(&catalogue);
    let mut regions = build_regions(&catalogue, &devices, &location_systems, &system_regions);
    mark_establishing_regions(&mut regions, &workflows)?;

    absorb_completed_provisions(&repository, &workflows, now)?;
    auto_assign_unassigned_replicants(
        &repository,
        &replicants,
        &location_systems,
        &system_regions,
        &regions,
        now,
    )?;

    let busy = busy_replicants(&repository, &workflows)?;
    let assignments = load_assignments(&repository)?;
    let racing_vessels = hosted_racing_vessels(&devices);
    let mut workers = replicants
        .iter()
        .cloned()
        .map(|replicant| {
            let code = replicant.key.id.as_str().to_owned();
            let assignment = assignments.get(&code);
            WorkerView {
                region: assignment.and_then(|value| value.region.clone()),
                role_affinity: assignment.and_then(|value| value.role_affinity.clone()),
                busy_workflow: busy.get(&code).copied(),
                racing_vessel: racing_vessels.get(&code).cloned(),
                replicant,
            }
        })
        .collect::<Vec<_>>();
    workers.sort_by(|left, right| left.replicant.key.id.cmp(&right.replicant.key.id));
    mark_partial_region_footholds(&mut regions, &workers, &location_systems, &system_regions);
    mark_manufacturing_footholds(&mut regions, &devices, &location_systems, &system_regions);

    let goal_controls = load_goal_controls(&repository)?;
    let mut requirements = DirectorRequirementGraph::load(&repository, now)?;
    let blueprint_catalogue_needed =
        goal_enabled(&goal_controls, DirectorGoalKind::BlueprintAcquisition)
            || (goal_enabled(&goal_controls, DirectorGoalKind::ExpandStarCatalogue)
                && !devices.iter().any(|device| {
                    device.device_type.as_ref() == Some(&DeviceType::GalacticObservatory)
                }));
    let mut blueprint_cache = load_blueprint_catalogue_cache(&repository)?;
    let mut blueprint_refreshed_this_pass = false;
    let mut unlocked_blueprints = if blueprint_catalogue_needed {
        let refresh_due = blueprint_catalogue_refresh_due(
            &repository,
            blueprint_cache.as_ref(),
            now,
            force_slow_refresh,
            None,
        )?;
        if refresh_due {
            match refresh_blueprint_catalogue(client).await {
                Ok(blueprints) => {
                    blueprint_refreshed_this_pass = true;
                    let cache = store_blueprint_catalogue_cache(
                        &repository,
                        now,
                        blueprint_cache
                            .as_ref()
                            .map(|cache| cache.requirement_signature.clone())
                            .unwrap_or_default(),
                        &blueprints,
                    )?;
                    blueprint_cache = Some(cache);
                    Some(blueprints)
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        phase = "requirements",
                        "Director could not refresh managed blueprint state; using the last cached catalogue when available"
                    );
                    blueprint_cache.as_ref().map(cached_blueprint_types)
                }
            }
        } else {
            if let Some(cache) = blueprint_cache.as_ref() {
                tracing::debug!(
                    age_ms = now.saturating_sub(cache.refreshed_at_ms),
                    ttl_ms = BLUEPRINT_CATALOGUE_CACHE_TTL_MS,
                    blueprints = cache.unlocked_device_types.len(),
                    phase = "requirements",
                    "Director reused cached unlocked blueprint catalogue"
                );
            }
            blueprint_cache.as_ref().map(cached_blueprint_types)
        }
    } else {
        None
    };
    let observatory_blueprint_known = unlocked_blueprints
        .as_ref()
        .map(|blueprints| blueprints.contains(&DeviceType::GalacticObservatory));

    let mut goals = Vec::new();
    let mut reserved_workers = BTreeSet::new();
    let automatic = settings.mode == DirectorMode::Automatic && allow_launch;
    let goal_context = GoalReconcileContext {
        repository: repository.as_ref(),
        workflows: &workflows,
        controls: &goal_controls,
        automatic,
        now,
    };

    goals.push(reconcile_establish_regions(
        &goal_context,
        &regions,
        &workers,
        &mut reserved_workers,
        &mut requirements,
    )?);

    goals.push(reconcile_expand_star_catalogue(
        &goal_context,
        &devices,
        &settings,
        observatory_blueprint_known,
        &mut requirements,
    )?);

    let established_regions = regions
        .values()
        .filter(|region| region.status == DirectorRegionStatus::Established)
        .cloned()
        .collect::<Vec<_>>();
    let establishing_regions = regions
        .values()
        .filter(|region| region.status == DirectorRegionStatus::Establishing)
        .cloned()
        .collect::<Vec<_>>();

    // A partially bootstrapped region can use its staged Replicants and
    // manufacturing foothold to discover belts, extend relay coverage, and
    // deploy mines while its System Hub is still being completed. Event and
    // hub-maintenance goals remain established-region concerns.
    for region in &establishing_regions {
        goals.push(reconcile_enhance_catalogue(
            client,
            &goal_context,
            region,
            &workers,
            &mut reserved_workers,
            &mut requirements,
        )?);
        goals.push(reconcile_discover_belts(
            &goal_context,
            region,
            &workers,
            &mut reserved_workers,
            &mut requirements,
            &locations,
            &location_systems,
        )?);
        goals.push(reconcile_expand_mining(
            &repository,
            region,
            &workers,
            &workflows,
            &devices,
            &locations,
            &location_systems,
            &system_regions,
            &goal_controls,
            automatic,
            &mut reserved_workers,
            &mut requirements,
            now,
        )?);
        goals.push(reconcile_expand_ftl_network(
            &goal_context,
            region,
            &workers,
            &mut reserved_workers,
            &mut requirements,
            &devices,
            &locations,
            &location_systems,
            &BTreeSet::new(),
        )?);
    }

    let mut event_discovery_error = None;
    let mut event_systems_by_region = BTreeMap::<String, BTreeSet<String>>::new();
    let event_designations_by_region =
        if (goal_enabled(&goal_controls, DirectorGoalKind::EventCompletion)
            || goal_enabled(&goal_controls, DirectorGoalKind::ExpandFtlNetwork))
            && !established_regions.is_empty()
        {
            match active_events_for_director(
                client,
                repository.as_ref(),
                now,
                force_slow_refresh,
                established_regions.len(),
            )
            .await
            {
                Ok(active_events) => {
                    event_systems_by_region = group_active_event_systems_by_region(
                        &active_events,
                        &location_systems,
                        &system_regions,
                        &regions,
                    );
                    group_active_events_by_region(
                        &active_events,
                        &location_systems,
                        &system_regions,
                        &regions,
                    )
                }
                Err(error) => {
                    let message = format!("active-event discovery failed: {error}");
                    tracing::warn!(
                        error = %error,
                        phase = "events",
                        "Director active-event snapshot failed; continuing without event planning"
                    );
                    event_discovery_error = Some(message);
                    BTreeMap::new()
                }
            }
        } else {
            BTreeMap::new()
        };

    for region in &established_regions {
        goals.push(reconcile_maintain_system_hubs(
            &goal_context,
            region,
            &regions,
            &devices,
            &locations,
            &inventories,
            &hub_refresh_errors,
            &location_systems,
            &system_regions,
        )?);
        let regional_events = event_designations_by_region
            .get(&region.region)
            .map(Vec::as_slice)
            .unwrap_or_default();
        goals.push(reconcile_event_completion(
            &goal_context,
            region,
            regional_events,
            event_discovery_error.as_deref(),
            &workers,
            &mut reserved_workers,
            &mut requirements,
        )?);
        goals.push(reconcile_enhance_catalogue(
            client,
            &goal_context,
            region,
            &workers,
            &mut reserved_workers,
            &mut requirements,
        )?);
        goals.push(reconcile_discover_belts(
            &goal_context,
            region,
            &workers,
            &mut reserved_workers,
            &mut requirements,
            &locations,
            &location_systems,
        )?);
        goals.push(reconcile_expand_mining(
            &repository,
            region,
            &workers,
            &workflows,
            &devices,
            &locations,
            &location_systems,
            &system_regions,
            &goal_controls,
            automatic,
            &mut reserved_workers,
            &mut requirements,
            now,
        )?);
        let regional_event_systems = event_systems_by_region
            .get(&region.region)
            .cloned()
            .unwrap_or_default();
        goals.push(reconcile_expand_ftl_network(
            &goal_context,
            region,
            &workers,
            &mut reserved_workers,
            &mut requirements,
            &devices,
            &locations,
            &location_systems,
            &regional_event_systems,
        )?);
        goals.push(waiting_goal(
            DirectorGoalKind::EstablishBeacons,
            Some(&region.region),
            &goal_controls,
            "Maintain beacon coverage at useful known systems",
            "Beacon placement policy is not yet enabled in the Director planner",
            "Existing event/bootstrap automation may still deploy required beacons",
        ));
    }

    if blueprint_catalogue_needed {
        let requirement_signature = requirements
            .current_blueprint_priorities()
            .keys()
            .map(|device_type| device_type.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        let signature_changed = blueprint_cache
            .as_ref()
            .is_none_or(|cache| cache.requirement_signature != requirement_signature);
        if signature_changed && !blueprint_refreshed_this_pass {
            match refresh_blueprint_catalogue(client).await {
                Ok(blueprints) => {
                    unlocked_blueprints = Some(blueprints.clone());
                    store_blueprint_catalogue_cache(
                        &repository,
                        now,
                        requirement_signature,
                        &blueprints,
                    )?;
                }
                Err(error) => tracing::warn!(
                    error = %error,
                    phase = "requirements",
                    "Director could not refresh blueprint state after the requirement set changed"
                ),
            }
        } else if signature_changed && let Some(cache) = blueprint_cache.as_mut() {
            cache.requirement_signature = requirement_signature;
            repository.put_document(
                BLUEPRINT_CATALOGUE_CACHE_NS,
                BLUEPRINT_CATALOGUE_CACHE_KEY,
                cache,
            )?;
        }
    }

    // Shop discovery is intentionally demand-driven and happens only after
    // the standing goals have raised this pass's Blueprint requirements. Owned
    // copies can be learned without touching the trade directory, and an active
    // acquisition already has enough information to finish.
    let mut shop_requested_blueprints = BTreeSet::new();
    if goal_enabled(&goal_controls, DirectorGoalKind::BlueprintAcquisition)
        && active_blueprint_acquisition_workflow(&workflows).is_none()
    {
        for device_type in requirements.current_blueprint_priorities().keys() {
            let kind = DeviceType::from(device_type.as_str());
            let already_known = unlocked_blueprints
                .as_ref()
                .is_some_and(|known| known.contains(&kind));
            let owned_source = devices
                .iter()
                .any(|device| blueprint_source_is_candidate(device, kind.as_str(), &devices));
            if !already_known && !owned_source {
                shop_requested_blueprints.insert(device_type.to_ascii_lowercase());
            }
        }
    }
    let blueprint_shop_snapshot = blueprint_shop_snapshot_for_requirements(
        client,
        &replicants,
        &repository,
        now,
        &shop_requested_blueprints,
        force_slow_refresh,
    )
    .await?;

    let mut blueprint_context = BlueprintReconcileContext {
        devices: &devices,
        workers: &workers,
        reserved_workers: &mut reserved_workers,
        unlocked_blueprints: unlocked_blueprints.as_ref(),
        shop_snapshot: &blueprint_shop_snapshot,
        location_systems: &location_systems,
        system_regions: &system_regions,
    };
    goals.push(reconcile_blueprint_acquisition(
        &goal_context,
        &mut blueprint_context,
        &mut requirements,
    )?);

    let worker_demand = requirements.worker_demand_by_region();
    let pending_worker_demand = worker_demand.values().sum::<usize>();
    let mut workforce_states = load_workforce_states(&repository)?;
    let scale_recommendations = reconcile_workforce(
        &repository,
        &settings,
        &regions,
        &workers,
        &workflows,
        &reserved_workers,
        &worker_demand,
        &mut workforce_states,
        automatic,
        now,
    )?;

    let total = workers.len();
    let busy_count = workers
        .iter()
        .filter(|worker| worker.busy_workflow.is_some())
        .count();
    let idle = total.saturating_sub(busy_count);
    let idle_ratio = if total == 0 {
        1.0
    } else {
        idle as f64 / total as f64
    };
    let scale_up_recommended = !scale_recommendations.is_empty();
    let scale_reason = scale_recommendations.first().cloned().or_else(|| {
        (pending_worker_demand > 0).then(|| format!(
            "{pending_worker_demand} regional assignment(s) are worker-blocked; waiting for the grow-only scale policy"
        ))
    });

    let mut region_summaries = regions
        .values()
        .map(|region| DirectorRegionSummary {
            region: region.region.clone(),
            status: region.status,
            hub_system: region.hub_system.clone(),
            hub_location: region.hub_location.clone(),
            replicants: workers
                .iter()
                .filter(|worker| worker.region.as_deref() == Some(region.region.as_str()))
                .map(|worker| worker.replicant.key.id.as_str().to_owned())
                .collect(),
            known_systems: region.known_systems.len(),
        })
        .collect::<Vec<_>>();
    region_summaries.sort_by(|left, right| left.region.cmp(&right.region));
    goals.sort_by(|left, right| {
        left.region
            .cmp(&right.region)
            .then_with(|| goal_kind_key(left.kind).cmp(goal_kind_key(right.kind)))
    });
    let requirement_summaries = requirements.persist(&repository)?;

    tracing::info!(
        regions = regions.len(),
        established_regions = established_regions.len(),
        workers = workers.len(),
        busy_workers = busy_count,
        pending_worker_demand,
        scale_up_recommended,
        elapsed_ms = started.elapsed().as_millis(),
        "Director planning pass complete"
    );

    let snapshot = DirectorSnapshot {
        metadata: SnapshotMetadata {
            revision,
            generated_at_ms: now,
        },
        mode: settings.mode,
        regions: region_summaries,
        goals,
        replicants: workers
            .into_iter()
            .map(|worker| DirectorReplicantAssignment {
                code: worker.replicant.key.id.as_str().to_owned(),
                name: worker.replicant.name,
                region: worker.region,
                busy: worker.busy_workflow.is_some(),
                workflow_id: worker
                    .busy_workflow
                    .map(|id| ProtocolWorkflowId(id.to_string())),
                role_affinity: worker.role_affinity,
            })
            .collect(),
        requirements: requirement_summaries,
        workforce: DirectorWorkforceSummary {
            total,
            busy: busy_count,
            idle,
            idle_ratio,
            pending_worker_demand,
            scale_up_recommended,
            scale_reason,
        },
    };
    repository.put_document(SNAPSHOT_NS, SNAPSHOT_KEY, &snapshot)?;
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Director snapshot persisted"
    );
    Ok(snapshot)
}

fn reconcile_establish_regions(
    context: &GoalReconcileContext<'_>,
    regions: &BTreeMap<String, RegionView>,
    workers: &[WorkerView],
    reserved: &mut BTreeSet<String>,
    requirements: &mut DirectorRequirementGraph,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::EstablishRegions;
    let enabled = goal_enabled(context.controls, kind);
    let id = goal_instance_id(kind, None);
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);
    let established = regions
        .values()
        .filter(|region| region.status == DirectorRegionStatus::Established)
        .count();
    let total = regions.len();
    let missing = regions
        .values()
        .filter(|region| region.status != DirectorRegionStatus::Established)
        .collect::<Vec<_>>();
    let active = nonterminal_ids(&runtime, context.workflows);
    let recently_launched = launch_is_recent(&runtime, context.now, DEFAULT_RETRY_COOLDOWN_MS);
    let mut blocker = None;
    let mut next_action = None;
    let status = if !enabled {
        next_action =
            Some("Enable this standing goal to establish newly discovered regions".to_owned());
        DirectorGoalStatus::Waiting
    } else if missing.is_empty() {
        next_action = Some("Wait for a newly discovered region".to_owned());
        DirectorGoalStatus::Satisfied
    } else if !active.is_empty() {
        next_action = Some("Continue the active regional bootstrap campaign".to_owned());
        DirectorGoalStatus::Active
    } else if recently_launched {
        next_action =
            Some("Wait for the regional-bootstrap retry cooldown before replanning".to_owned());
        DirectorGoalStatus::Waiting
    } else {
        let target = missing[0];
        let target_workers = workers
            .iter()
            .filter(|worker| worker.region.as_deref() == Some(target.region.as_str()))
            .collect::<Vec<_>>();
        if target.status == DirectorRegionStatus::Establishing {
            next_action = Some(format!(
                "Continue useful {} bootstrap work while the regional System Hub becomes available",
                target.region
            ));
            DirectorGoalStatus::Active
        } else if target_workers.len() < 2 {
            let needed = 2usize.saturating_sub(target_workers.len());
            let reason = format!(
                "{} needs {needed} additional permanently assigned bootstrap worker(s)",
                target.region
            );
            requirements.raise(
                DirectorRequirement::WorkerCapacity {
                    region: target.region.clone(),
                    count: needed,
                    affinity: Some("bootstrap".to_owned()),
                },
                &id,
                reason.clone(),
                PRIORITY_REGION_ESTABLISHMENT,
            )?;
            blocker = Some(reason);
            next_action = Some(format!(
                "Grow the {} workforce to two Replicants before dispatching the regional ark",
                target.region
            ));
            DirectorGoalStatus::Blocked
        } else if let Some((source_hub, operator, explorer)) =
            bootstrap_assignment(regions, &target_workers)
        {
            let landing_star = target.known_systems.iter().next().cloned();
            if let Some(landing_star) = landing_star {
                next_action = Some(format!(
                    "Bootstrap {} at {landing_star} from {source_hub} with {operator} and {explorer}",
                    target.region
                ));
                if context.automatic {
                    let workflow = context.repository.create(new_region_establish_workflow(
                        RegionEstablishIntent {
                            region: target.region.clone(),
                            landing_star,
                            source_hub,
                            operator: operator.clone(),
                            explorer: explorer.clone(),
                        },
                    ))?;
                    tracing::info!(
                        workflow_id = %workflow.id,
                        region = %target.region,
                        operator = %operator,
                        explorer = %explorer,
                        "Director launched regional establishment campaign"
                    );
                    runtime.active_workflows = vec![workflow.id];
                    runtime.last_launch_at_ms = Some(context.now);
                    reserved.insert(operator.clone());
                    reserved.insert(explorer.clone());
                }
                DirectorGoalStatus::Active
            } else {
                blocker = Some(format!(
                    "{} is known but has no catalogue star to use as a landing target",
                    target.region
                ));
                DirectorGoalStatus::Blocked
            }
        } else {
            blocker = Some(format!(
                "{} has bootstrap workers, but they are not co-located at an established regional manufacturing home",
                target.region
            ));
            next_action = Some("Move the assigned bootstrap workers to an established hub or reassign appropriate workers".to_owned());
            DirectorGoalStatus::Blocked
        }
    };
    save_goal_runtime(context.repository, &id, &runtime)?;
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: None,
        status,
        objective: "Establish a durable foothold in every discovered region".to_owned(),
        blocker,
        next_action,
        progress_current: established as u64,
        progress_total: total as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}

fn reconcile_expand_star_catalogue(
    context: &GoalReconcileContext<'_>,
    devices: &[Device],
    settings: &DirectorSettings,
    observatory_blueprint_known: Option<bool>,
    requirements: &mut DirectorRequirementGraph,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::ExpandStarCatalogue;
    let enabled = goal_enabled(context.controls, kind);
    let id = goal_instance_id(kind, None);
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);
    let observatories = devices
        .iter()
        .filter(|device| device.device_type.as_ref() == Some(&DeviceType::GalacticObservatory))
        .count();
    let active = nonterminal_ids(&runtime, context.workflows);
    let recently_launched = runtime
        .last_launch_at_ms
        .is_some_and(|last| context.now.saturating_sub(last) < settings.prospect_cooldown_ms);
    let (status, blocker, next_action) = if !enabled {
        (
            DirectorGoalStatus::Waiting,
            None,
            Some("Enable this standing goal to prospect for undiscovered stars".to_owned()),
        )
    } else if observatories == 0 {
        if observatory_blueprint_known == Some(false) {
            requirements.raise(
                DirectorRequirement::Blueprint {
                    device_type: DeviceType::GalacticObservatory.as_str().to_owned(),
                },
                &id,
                "Expanding the star catalogue requires a galactic observatory blueprint",
                PRIORITY_CATALOGUE_BLUEPRINT,
            )?;
        }
        (
            DirectorGoalStatus::Blocked,
            Some("No owned galactic observatory is available".to_owned()),
            Some(if observatory_blueprint_known == Some(false) {
                "Acquire the galactic observatory blueprint, then build and deploy one".to_owned()
            } else {
                "Build and deploy a galactic observatory".to_owned()
            }),
        )
    } else if !active.is_empty() {
        (
            DirectorGoalStatus::Active,
            None,
            Some("Allow the current observatory prospect to finish".to_owned()),
        )
    } else if recently_launched {
        (
            DirectorGoalStatus::Waiting,
            None,
            Some(
                "Wait for the prospect cooldown before trying another sparse direction".to_owned(),
            ),
        )
    } else {
        if context.automatic {
            let workflow = context
                .repository
                .create(new_observatory_workflow(ObservatoryIntent::default()))?;
            tracing::info!(
                workflow_id = %workflow.id,
                "Director launched observatory prospect campaign"
            );
            runtime.active_workflows = vec![workflow.id];
            runtime.last_launch_at_ms = Some(context.now);
        }
        (
            DirectorGoalStatus::Active,
            None,
            Some(
                "Prospect from the sparsest eligible observatory to expand the catalogue"
                    .to_owned(),
            ),
        )
    };
    save_goal_runtime(context.repository, &id, &runtime)?;
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: None,
        status,
        objective: "Discover stars that are not yet present in the catalogue".to_owned(),
        blocker,
        next_action,
        progress_current: 0,
        progress_total: 0,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}

fn load_blueprint_catalogue_cache(
    repository: &WorkflowRepository,
) -> Result<Option<BlueprintCatalogueCache>, ApplicationError> {
    repository
        .read_document(BLUEPRINT_CATALOGUE_CACHE_NS, BLUEPRINT_CATALOGUE_CACHE_KEY)?
        .map(|(value, _)| serde_json::from_value(value).map_err(ApplicationError::from))
        .transpose()
}

fn cached_blueprint_types(cache: &BlueprintCatalogueCache) -> BTreeSet<DeviceType> {
    cache
        .unlocked_device_types
        .iter()
        .map(|device_type| DeviceType::from(device_type.as_str()))
        .collect()
}

fn blueprint_catalogue_refresh_due(
    repository: &WorkflowRepository,
    cache: Option<&BlueprintCatalogueCache>,
    now: i64,
    force_refresh: bool,
    requirement_signature: Option<&BTreeSet<String>>,
) -> Result<bool, ApplicationError> {
    let Some(cache) = cache else {
        return Ok(true);
    };
    if force_refresh
        || now.saturating_sub(cache.refreshed_at_ms) >= BLUEPRINT_CATALOGUE_CACHE_TTL_MS
        || requirement_signature.is_some_and(|signature| cache.requirement_signature != *signature)
    {
        return Ok(true);
    }
    Ok(repository.list()?.iter().any(|workflow| {
        workflow.kind == blueprint_acquire_workflow_kind()
            && workflow.status == WorkflowStatus::Succeeded
            && workflow.updated_at > cache.refreshed_at_ms
    }))
}

async fn refresh_blueprint_catalogue(
    client: &Client,
) -> Result<BTreeSet<DeviceType>, replicant_client::Error> {
    client.blueprints().unlocked_device_types().await
}

fn store_blueprint_catalogue_cache(
    repository: &WorkflowRepository,
    now: i64,
    requirement_signature: BTreeSet<String>,
    blueprints: &BTreeSet<DeviceType>,
) -> Result<BlueprintCatalogueCache, ApplicationError> {
    let cache = BlueprintCatalogueCache {
        refreshed_at_ms: now,
        requirement_signature,
        unlocked_device_types: blueprints
            .iter()
            .map(|device_type| device_type.as_str().to_owned())
            .collect(),
    };
    repository.put_document(
        BLUEPRINT_CATALOGUE_CACHE_NS,
        BLUEPRINT_CATALOGUE_CACHE_KEY,
        &cache,
    )?;
    Ok(cache)
}

async fn blueprint_shop_snapshot_for_requirements(
    client: &Client,
    replicants: &[Replicant],
    repository: &WorkflowRepository,
    now: i64,
    requested_blueprints: &BTreeSet<String>,
    force_refresh: bool,
) -> Result<BlueprintShopSnapshot, ApplicationError> {
    if requested_blueprints.is_empty() {
        tracing::debug!(
            event = "director.blueprint.snapshot_skipped",
            "Director skipped shop discovery because no unresolved Blueprint requirement needs a shop"
        );
        return Ok(BlueprintShopSnapshot::default());
    }

    let cached = repository
        .read_document(BLUEPRINT_SHOP_CACHE_NS, BLUEPRINT_SHOP_CACHE_KEY)?
        .map(|(value, _)| serde_json::from_value::<BlueprintShopCache>(value))
        .transpose()?;
    if let Some(cache) = cached.as_ref() {
        let ttl_ms = if cache.snapshot.directory_errors == 0 && cache.snapshot.trade_errors == 0 {
            BLUEPRINT_SHOP_CACHE_TTL_MS
        } else {
            BLUEPRINT_SHOP_PARTIAL_CACHE_TTL_MS
        };
        let age_ms = now.saturating_sub(cache.refreshed_at_ms);
        let covers_requirements = requested_blueprints.is_subset(&cache.requested_blueprints);
        if !force_refresh && age_ms <= ttl_ms && covers_requirements {
            tracing::debug!(
                event = "director.blueprint.snapshot_cache_hit",
                age_ms,
                ttl_ms,
                requirements = requested_blueprints.len(),
                opportunities = cache.snapshot.opportunities.len(),
                "Director reused cached blueprint shop snapshot"
            );
            return Ok(cache.snapshot.clone());
        }
    }

    let snapshot = snapshot_blueprint_shop_opportunities(client, replicants, repository, now).await;
    repository.put_document(
        BLUEPRINT_SHOP_CACHE_NS,
        BLUEPRINT_SHOP_CACHE_KEY,
        &BlueprintShopCache {
            refreshed_at_ms: now,
            requested_blueprints: requested_blueprints.clone(),
            snapshot: snapshot.clone(),
        },
    )?;
    Ok(snapshot)
}

async fn active_events_for_director(
    client: &Client,
    repository: &WorkflowRepository,
    now: i64,
    force_refresh: bool,
    region_count: usize,
) -> Result<Vec<CachedActiveEvent>, ApplicationError> {
    let cached = repository
        .read_document(ACTIVE_EVENT_CACHE_NS, ACTIVE_EVENT_CACHE_KEY)?
        .map(|(value, _)| serde_json::from_value::<ActiveEventCache>(value))
        .transpose()?;
    if let Some(cache) = cached.as_ref() {
        let age_ms = now.saturating_sub(cache.refreshed_at_ms);
        if !force_refresh && age_ms <= ACTIVE_EVENT_CACHE_TTL_MS {
            tracing::debug!(
                event = "director.events.snapshot_cache_hit",
                age_ms,
                ttl_ms = ACTIVE_EVENT_CACHE_TTL_MS,
                events = cache.events.len(),
                "Director reused cached active-event snapshot"
            );
            return Ok(cache.events.clone());
        }
    }

    let started = Instant::now();
    tracing::info!(
        regions = region_count,
        phase = "events",
        "Director loading one account-wide active-event snapshot"
    );
    match tokio::time::timeout(EVENT_DISCOVERY_TIMEOUT, active_events(client)).await {
        Ok(Ok(events)) => {
            let events = events
                .into_iter()
                .map(|event| CachedActiveEvent {
                    designation: event.designation,
                    location: event.location,
                })
                .collect::<Vec<_>>();
            repository.put_document(
                ACTIVE_EVENT_CACHE_NS,
                ACTIVE_EVENT_CACHE_KEY,
                &ActiveEventCache {
                    refreshed_at_ms: now,
                    events: events.clone(),
                },
            )?;
            tracing::info!(
                events = events.len(),
                elapsed_ms = started.elapsed().as_millis(),
                phase = "events",
                "Director active-event snapshot complete"
            );
            Ok(events)
        }
        outcome => {
            let error = match outcome {
                Ok(Err(error)) => format!("{error}"),
                Err(_) => format!(
                    "active-event discovery exceeded {} seconds",
                    EVENT_DISCOVERY_TIMEOUT.as_secs()
                ),
                Ok(Ok(_)) => unreachable!("successful event discovery handled above"),
            };
            if let Some(cache) = cached {
                let age_ms = now.saturating_sub(cache.refreshed_at_ms);
                if age_ms <= ACTIVE_EVENT_STALE_FALLBACK_MS {
                    tracing::warn!(
                        error = %error,
                        age_ms,
                        events = cache.events.len(),
                        phase = "events",
                        "Director active-event refresh failed; using recent cached snapshot"
                    );
                    return Ok(cache.events);
                }
            }
            Err(std::io::Error::other(error).into())
        }
    }
}

async fn snapshot_blueprint_shop_opportunities(
    client: &Client,
    replicants: &[Replicant],
    repository: &WorkflowRepository,
    now: i64,
) -> BlueprintShopSnapshot {
    let started = Instant::now();
    tracing::info!(
        event = "director.blueprint.snapshot_started",
        viewers = replicants.len(),
        "Director collecting one account-wide blueprint shop snapshot"
    );
    let viewer_codes = replicants
        .iter()
        .map(|replicant| replicant.key.id.as_str().to_owned())
        .collect::<Vec<_>>();
    let directory_results = stream::iter(viewer_codes.into_iter().map(|code| {
        let client = client.clone();
        async move {
            let result =
                tokio::time::timeout(BLUEPRINT_SHOP_TIMEOUT, trader_directory(&client, &code))
                    .await;
            (code, result)
        }
    }))
    .buffer_unordered(BLUEPRINT_SHOP_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    let mut snapshot = BlueprintShopSnapshot::default();
    let mut traders = BTreeMap::<String, TraderSummary>::new();
    for (viewer, result) in directory_results {
        let directory = match result {
            Ok(Ok(directory)) => directory,
            Ok(Err(error)) => {
                snapshot.directory_errors += 1;
                tracing::warn!(
                    event = "director.blueprint.directory_failed",
                    viewer = %viewer,
                    error = %error,
                    "Director could not inspect one trade directory"
                );
                continue;
            }
            Err(_) => {
                snapshot.directory_errors += 1;
                tracing::warn!(
                    event = "director.blueprint.directory_failed",
                    viewer = %viewer,
                    timeout_ms = BLUEPRINT_SHOP_TIMEOUT.as_millis(),
                    "Director trade-directory inspection timed out"
                );
                continue;
            }
        };
        for trader in directory {
            traders
                .entry(trader.controller_code.clone())
                .and_modify(|existing| {
                    if existing.location.is_none() && trader.location.is_some() {
                        *existing = trader.clone();
                    }
                })
                .or_insert(trader);
        }
    }

    let inspectable = traders
        .into_values()
        .filter_map(|trader| {
            let Some(location) = trader.location.clone() else {
                snapshot.hidden_or_unlocated += 1;
                return None;
            };
            let system = trader
                .star
                .clone()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| location.split('-').next().map(str::to_owned));
            let Some(system) = system.filter(|value| !value.trim().is_empty()) else {
                snapshot.hidden_or_unlocated += 1;
                return None;
            };
            Some((trader, location, system))
        })
        .collect::<Vec<_>>();

    let trade_results = stream::iter(inspectable.into_iter().map(|(trader, location, system)| {
        let client = client.clone();
        async move {
            let result = tokio::time::timeout(
                BLUEPRINT_SHOP_TIMEOUT,
                shop_trades(&client, &trader.controller_code),
            )
            .await;
            (trader, location, system, result)
        }
    }))
    .buffer_unordered(BLUEPRINT_SHOP_CONCURRENCY)
    .collect::<Vec<_>>()
    .await;

    for (trader, location, system, result) in trade_results {
        let trades = match result {
            Ok(Ok(trades)) => trades,
            Ok(Err(error)) => {
                snapshot.trade_errors += 1;
                tracing::warn!(
                    event = "director.blueprint.shop_failed",
                    controller = %trader.controller_code,
                    error = %error,
                    "Director could not inspect one shop's live trades"
                );
                continue;
            }
            Err(_) => {
                snapshot.trade_errors += 1;
                tracing::warn!(
                    event = "director.blueprint.shop_failed",
                    controller = %trader.controller_code,
                    timeout_ms = BLUEPRINT_SHOP_TIMEOUT.as_millis(),
                    "Director shop inspection timed out"
                );
                continue;
            }
        };
        for trade in trades.into_iter().filter(|trade| {
            !trade.trade_code.is_empty() && trade.current_stock.unwrap_or_default() > 0
        }) {
            let rewards = trade.rewards_bundle();
            let criteria = trade.criteria_bundle();
            for (device_type, quantity) in rewards.devices {
                if quantity <= 0 {
                    continue;
                }
                let opportunity = BlueprintShopOpportunity {
                    device_type: device_type.clone(),
                    controller_code: trader.controller_code.clone(),
                    trade_code: trade.trade_code.clone(),
                    current_stock: trade.current_stock.unwrap_or_default(),
                    criteria: criteria.clone(),
                    shop_location: location.clone(),
                    shop_system: system.clone(),
                    shop_name: trader.shop_name.clone(),
                    last_seen_at_ms: now,
                };
                let key = format!(
                    "{}:{}:{}",
                    opportunity.controller_code, opportunity.trade_code, device_type
                );
                if let Err(error) = repository.put_document(BLUEPRINT_SHOP_NS, &key, &opportunity) {
                    tracing::warn!(
                        event = "director.blueprint.opportunity_persist_failed",
                        opportunity = %key,
                        error = %error,
                        "Director could not persist blueprint shop opportunity"
                    );
                }
                tracing::debug!(
                    event = "director.blueprint.opportunity",
                    device_type = %opportunity.device_type,
                    controller = %opportunity.controller_code,
                    trade = %opportunity.trade_code,
                    stock = opportunity.current_stock,
                    shop_location = %opportunity.shop_location,
                    "Director observed a stocked blueprint acquisition opportunity"
                );
                snapshot.opportunities.push(opportunity);
            }
        }
    }
    snapshot.opportunities.sort_by(|left, right| {
        left.device_type
            .cmp(&right.device_type)
            .then_with(|| left.current_stock.cmp(&right.current_stock))
            .then_with(|| right.last_seen_at_ms.cmp(&left.last_seen_at_ms))
            .then_with(|| left.controller_code.cmp(&right.controller_code))
            .then_with(|| left.trade_code.cmp(&right.trade_code))
    });
    tracing::info!(
        event = "director.blueprint.snapshot_completed",
        opportunities = snapshot.opportunities.len(),
        directory_errors = snapshot.directory_errors,
        trade_errors = snapshot.trade_errors,
        hidden_or_unlocated = snapshot.hidden_or_unlocated,
        elapsed_ms = started.elapsed().as_millis(),
        "Director blueprint shop snapshot complete"
    );
    snapshot
}

fn shop_opportunities_for<'a>(
    snapshot: &'a BlueprintShopSnapshot,
    device_type: &DeviceType,
) -> impl Iterator<Item = &'a BlueprintShopOpportunity> {
    snapshot.opportunities.iter().filter(move |opportunity| {
        opportunity
            .device_type
            .eq_ignore_ascii_case(device_type.as_str())
            && opportunity.current_stock > 0
    })
}

fn blueprint_acquisition_target(device_type: &DeviceType) -> bool {
    !device_type
        .as_str()
        .eq_ignore_ascii_case("replicant_matrix")
}

fn blueprint_shop_dependency_cycle(
    snapshot: &BlueprintShopSnapshot,
    target: &str,
    criterion: &str,
) -> bool {
    if target.eq_ignore_ascii_case(criterion) {
        return true;
    }
    let mut pending = vec![criterion.to_ascii_lowercase()];
    let mut visited = BTreeSet::new();
    let target = target.to_ascii_lowercase();
    while let Some(current) = pending.pop() {
        if !visited.insert(current.clone()) {
            continue;
        }
        for opportunity in snapshot
            .opportunities
            .iter()
            .filter(|opportunity| opportunity.device_type.eq_ignore_ascii_case(&current))
        {
            for dependency in opportunity.criteria.devices.keys() {
                let dependency = dependency.to_ascii_lowercase();
                if dependency == target {
                    return true;
                }
                if !visited.contains(&dependency) {
                    pending.push(dependency);
                }
            }
        }
    }
    false
}

fn reconcile_blueprint_acquisition(
    context: &GoalReconcileContext<'_>,
    blueprint: &mut BlueprintReconcileContext<'_>,
    requirements: &mut DirectorRequirementGraph,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let devices = blueprint.devices;
    let workers = blueprint.workers;
    let unlocked_blueprints = blueprint.unlocked_blueprints;
    let shop_snapshot = blueprint.shop_snapshot;
    let location_systems = blueprint.location_systems;
    let system_regions = blueprint.system_regions;
    let kind = DirectorGoalKind::BlueprintAcquisition;
    let enabled = goal_enabled(context.controls, kind);
    let id = goal_instance_id(kind, None);
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);
    let mut active = nonterminal_ids(&runtime, context.workflows);
    let recently_launched = launch_is_recent(&runtime, context.now, DEFAULT_RETRY_COOLDOWN_MS);
    if active.is_empty()
        && let Some(existing) = active_blueprint_acquisition_workflow(context.workflows)
    {
        runtime.active_workflows = vec![existing];
        active = vec![existing];
    }

    if !enabled {
        save_goal_runtime(context.repository, &id, &runtime)?;
        return Ok(DirectorGoalSummary {
            id,
            kind,
            region: None,
            status: DirectorGoalStatus::Waiting,
            objective: "Acquire missing blueprints from owned copies or known stocked shops"
                .to_owned(),
            blocker: None,
            next_action: Some("Enable this standing goal to acquire missing blueprints".to_owned()),
            progress_current: 0,
            progress_total: 0,
            active_workflows: protocol_workflow_ids(&runtime.active_workflows),
            enabled,
        });
    }

    let Some(unlocked_blueprints) = unlocked_blueprints else {
        save_goal_runtime(context.repository, &id, &runtime)?;
        return Ok(DirectorGoalSummary {
            id,
            kind,
            region: None,
            status: DirectorGoalStatus::Blocked,
            objective: "Acquire missing blueprints from owned copies or known stocked shops"
                .to_owned(),
            blocker: Some("Managed blueprint catalogue could not be refreshed".to_owned()),
            next_action: Some(
                "Retry the managed blueprint snapshot before selecting a sacrificial device"
                    .to_owned(),
            ),
            progress_current: 0,
            progress_total: 0,
            active_workflows: protocol_workflow_ids(&runtime.active_workflows),
            enabled,
        });
    };

    let priorities = requirements.current_blueprint_priorities();
    let owned_types = devices
        .iter()
        .filter_map(|device| device.device_type.clone())
        .collect::<BTreeSet<_>>();
    let mut tracked_types = owned_types.clone();
    tracked_types.extend(
        priorities
            .keys()
            .map(|device_type| DeviceType::from(device_type.as_str())),
    );
    tracked_types.extend(
        shop_snapshot
            .opportunities
            .iter()
            .map(|opportunity| DeviceType::from(opportunity.device_type.as_str())),
    );
    tracked_types.retain(blueprint_acquisition_target);
    let known_tracked = tracked_types
        .iter()
        .filter(|device_type| unlocked_blueprints.contains(*device_type))
        .count();
    let mut missing = tracked_types
        .iter()
        .filter(|device_type| !unlocked_blueprints.contains(*device_type))
        .cloned()
        .collect::<Vec<_>>();
    missing.sort_by(|left, right| {
        priorities
            .get(right.as_str())
            .copied()
            .unwrap_or_default()
            .cmp(&priorities.get(left.as_str()).copied().unwrap_or_default())
            .then_with(|| {
                let left_stock = shop_opportunities_for(shop_snapshot, left)
                    .map(|opportunity| opportunity.current_stock)
                    .min()
                    .unwrap_or(i64::MAX);
                let right_stock = shop_opportunities_for(shop_snapshot, right)
                    .map(|opportunity| opportunity.current_stock)
                    .min()
                    .unwrap_or(i64::MAX);
                left_stock.cmp(&right_stock)
            })
            .then_with(|| left.cmp(right))
    });

    if missing.is_empty() && (shop_snapshot.directory_errors > 0 || shop_snapshot.trade_errors > 0)
    {
        save_goal_runtime(context.repository, &id, &runtime)?;
        return Ok(DirectorGoalSummary {
            id,
            kind,
            region: None,
            status: DirectorGoalStatus::Blocked,
            objective: "Acquire missing blueprints from owned copies or known stocked shops"
                .to_owned(),
            blocker: Some(format!(
                "Blueprint shop discovery was incomplete ({} directory errors, {} shop errors)",
                shop_snapshot.directory_errors, shop_snapshot.trade_errors
            )),
            next_action: Some(
                "Retry the account-wide shop snapshot before declaring all known acquisition opportunities satisfied"
                    .to_owned(),
            ),
            progress_current: known_tracked as u64,
            progress_total: tracked_types.len() as u64,
            active_workflows: protocol_workflow_ids(&runtime.active_workflows),
            enabled,
        });
    }

    if missing.is_empty() {
        save_goal_runtime(context.repository, &id, &runtime)?;
        return Ok(DirectorGoalSummary {
            id,
            kind,
            region: None,
            status: DirectorGoalStatus::Satisfied,
            objective: "Acquire missing blueprints from owned copies or known stocked shops"
                .to_owned(),
            blocker: None,
            next_action: Some(
                "No known owned-copy or stocked-shop blueprint opportunities are currently missing"
                    .to_owned(),
            ),
            progress_current: known_tracked as u64,
            progress_total: tracked_types.len() as u64,
            active_workflows: protocol_workflow_ids(&runtime.active_workflows),
            enabled,
        });
    }

    if !active.is_empty() {
        save_goal_runtime(context.repository, &id, &runtime)?;
        return Ok(DirectorGoalSummary {
            id,
            kind,
            region: None,
            status: DirectorGoalStatus::Active,
            objective: "Acquire missing blueprints from owned copies or known stocked shops"
                .to_owned(),
            blocker: None,
            next_action: Some(
                "Allow the current blueprint acquisition to finish and verify its blueprint"
                    .to_owned(),
            ),
            progress_current: known_tracked as u64,
            progress_total: tracked_types.len() as u64,
            active_workflows: protocol_workflow_ids(&runtime.active_workflows),
            enabled,
        });
    }

    if recently_launched {
        save_goal_runtime(context.repository, &id, &runtime)?;
        return Ok(DirectorGoalSummary {
            id,
            kind,
            region: None,
            status: DirectorGoalStatus::Waiting,
            objective: "Acquire missing blueprints from owned copies or known stocked shops"
                .to_owned(),
            blocker: None,
            next_action: Some(
                "Wait for the blueprint acquisition retry cooldown before starting another irreversible acquisition"
                    .to_owned(),
            ),
            progress_current: known_tracked as u64,
            progress_total: tracked_types.len() as u64,
            active_workflows: protocol_workflow_ids(&runtime.active_workflows),
            enabled,
        });
    }

    let (claimed, claimed_factories) =
        active_blueprint_claims(context.repository, context.workflows)?;
    let factories = devices
        .iter()
        .filter(|device| {
            device.device_type.as_ref() == Some(&DeviceType::Autofactory)
                && device.location.is_some()
                && !claimed_factories.contains(device.key.id.as_str())
        })
        .collect::<Vec<_>>();
    let owned_selection = missing.iter().find_map(|device_type| {
        let source = devices
            .iter()
            .filter(|device| {
                blueprint_source_is_candidate(device, device_type.as_str(), devices)
                    && !claimed.contains(device.key.id.as_str())
            })
            .min_by_key(|device| device.key.id.clone())?;
        let source_location = blueprint_source_location(source, devices)?;
        let source_system = location_systems.get(source_location).map(String::as_str);
        let source_region = source_system.and_then(|system| system_regions.get(system));
        let factory = factories
            .iter()
            .copied()
            .filter(|factory| factory.key.id.as_str() != source.key.id.as_str())
            .min_by_key(|factory| {
                let factory_location = factory
                    .location
                    .as_ref()
                    .map(|location| location.id.as_str())
                    .unwrap_or_default();
                let factory_system = location_systems.get(factory_location).map(String::as_str);
                let factory_region = factory_system.and_then(|system| system_regions.get(system));
                let rank = if factory_location.eq_ignore_ascii_case(source_location) {
                    0
                } else if source_system.is_some() && factory_system == source_system {
                    1
                } else if source_region.is_some() && factory_region == source_region {
                    2
                } else {
                    3
                };
                (rank, factory.key.id.clone())
            })?;
        Some(BlueprintAcquisitionSelection::Owned {
            device_type: device_type.clone(),
            source_code: source.key.id.as_str().to_owned(),
            factory_code: factory.key.id.as_str().to_owned(),
            preferred_region: source_region.cloned(),
        })
    });

    let mut criterion_blockers = Vec::new();
    let selection = if owned_selection.is_some() {
        owned_selection
    } else {
        let mut selected = None;
        'device: for device_type in &missing {
            for opportunity in shop_opportunities_for(shop_snapshot, device_type) {
                if !opportunity.criteria.unknown.is_empty() {
                    criterion_blockers.push(format!(
                        "{} trade {} has unsupported criteria fields",
                        device_type.as_str(),
                        opportunity.trade_code
                    ));
                    continue;
                }
                let target_priority = priorities.get(device_type.as_str()).copied().unwrap_or(100);
                let mut dependency_blocked = false;
                for criterion_type in opportunity.criteria.devices.keys() {
                    let criterion = DeviceType::from(criterion_type.as_str());
                    if unlocked_blueprints.contains(&criterion) {
                        continue;
                    }
                    let criterion_has_owned_source = devices.iter().any(|device| {
                        blueprint_source_is_candidate(device, criterion.as_str(), devices)
                            && !claimed.contains(device.key.id.as_str())
                    });
                    if !criterion_has_owned_source
                        && blueprint_shop_dependency_cycle(
                            shop_snapshot,
                            device_type.as_str(),
                            criterion_type,
                        )
                    {
                        criterion_blockers.push(format!(
                            "blueprint dependency cycle detected while acquiring {} through criterion {}",
                            device_type.as_str(), criterion_type
                        ));
                        dependency_blocked = true;
                        continue;
                    }
                    requirements.raise(
                        DirectorRequirement::Blueprint {
                            device_type: criterion_type.clone(),
                        },
                        &id,
                        format!(
                            "Acquiring {} from shop trade {} requires expendable {} devices",
                            device_type.as_str(),
                            opportunity.trade_code,
                            criterion_type
                        ),
                        target_priority.saturating_add(1),
                    )?;
                    dependency_blocked = true;
                }
                if dependency_blocked {
                    continue;
                }

                let shop_region = system_regions.get(&opportunity.shop_system).cloned();
                let Some(factory) = factories.iter().copied().min_by_key(|factory| {
                    let factory_location = factory
                        .location
                        .as_ref()
                        .map(|location| location.id.as_str())
                        .unwrap_or_default();
                    let factory_system = location_systems.get(factory_location).map(String::as_str);
                    let factory_region =
                        factory_system.and_then(|system| system_regions.get(system));
                    let rank = if factory_location.eq_ignore_ascii_case(&opportunity.shop_location)
                    {
                        0
                    } else if factory_system == Some(opportunity.shop_system.as_str()) {
                        1
                    } else if shop_region.as_ref().is_some_and(|region| {
                        factory_region.is_some_and(|candidate| candidate == region)
                    }) {
                        2
                    } else {
                        3
                    };
                    (rank, factory.key.id.clone())
                }) else {
                    criterion_blockers.push(format!(
                        "{} is stocked at {}, but no unclaimed owned Autofactory is available",
                        device_type.as_str(),
                        opportunity.shop_location
                    ));
                    continue;
                };

                let worker = workers
                    .iter()
                    .filter(|worker| {
                        worker.busy_workflow.is_none()
                            && worker.replicant.travel.is_none()
                            && !blueprint
                                .reserved_workers
                                .contains(worker.replicant.key.id.as_str())
                    })
                    .min_by_key(|worker| {
                        let worker_location = worker
                            .replicant
                            .location
                            .as_ref()
                            .map(|location| location.id.as_str());
                        let worker_system = worker_location
                            .and_then(|location| location_systems.get(location))
                            .map(String::as_str);
                        let rank = if worker_location.is_some_and(|location| {
                            location.eq_ignore_ascii_case(&opportunity.shop_location)
                        }) {
                            0
                        } else if worker_system == Some(opportunity.shop_system.as_str()) {
                            1
                        } else if shop_region.as_ref().is_some_and(|region| {
                            worker
                                .region
                                .as_ref()
                                .is_some_and(|candidate| candidate == region)
                        }) {
                            2
                        } else {
                            3
                        };
                        (rank, worker.replicant.key.id.clone())
                    });
                let Some(worker) = worker else {
                    if let Some(region) = shop_region.as_deref() {
                        requirements.raise(
                            DirectorRequirement::WorkerCapacity {
                                region: region.to_owned(),
                                count: 1,
                                affinity: Some("trade".to_owned()),
                            },
                            &id,
                            format!(
                                "Acquiring {} from {} requires a free Replicant at the shop",
                                device_type.as_str(),
                                opportunity.shop_location
                            ),
                            target_priority,
                        )?;
                    }
                    criterion_blockers.push(format!(
                        "{} is stocked at {}, but no free Replicant is available to execute the trade",
                        device_type.as_str(), opportunity.shop_location
                    ));
                    continue;
                };

                selected = Some(BlueprintAcquisitionSelection::Shop {
                    device_type: device_type.clone(),
                    opportunity: Box::new(opportunity.clone()),
                    factory_code: factory.key.id.as_str().to_owned(),
                    replicant_code: worker.replicant.key.id.as_str().to_owned(),
                    preferred_region: shop_region,
                });
                break 'device;
            }
        }
        selected
    };

    let Some(selection) = selection else {
        let requested_unavailable = missing
            .iter()
            .find(|device_type| {
                priorities
                    .get(device_type.as_str())
                    .copied()
                    .unwrap_or_default()
                    > 0
            })
            .map(|device_type| device_type.as_str().to_owned());
        let discovery_partial =
            shop_snapshot.directory_errors > 0 || shop_snapshot.trade_errors > 0;
        let missing_names = missing
            .iter()
            .map(|device_type| device_type.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let blocker = criterion_blockers.first().cloned().unwrap_or_else(|| {
            if let Some(device_type) = requested_unavailable {
                format!(
                    "Blueprint {device_type} is required, but there is no available owned copy or currently inspectable in-stock shop opportunity"
                )
            } else if discovery_partial {
                format!(
                    "Missing blueprints remain ({missing_names}), but the account-wide shop snapshot was incomplete and no safe acquisition was selected"
                )
            } else {
                format!(
                    "Missing blueprints remain ({missing_names}), but no available owned copy or safe in-stock shop opportunity is currently usable"
                )
            }
        });
        save_goal_runtime(context.repository, &id, &runtime)?;
        return Ok(DirectorGoalSummary {
            id,
            kind,
            region: None,
            status: DirectorGoalStatus::Blocked,
            objective: "Acquire missing blueprints from owned copies or known stocked shops".to_owned(),
            blocker: Some(blocker),
            next_action: Some(
                "Wait for an owned copy, criterion dependency, worker, or inspectable shop opportunity to become available"
                    .to_owned(),
            ),
            progress_current: known_tracked as u64,
            progress_total: tracked_types.len() as u64,
            active_workflows: protocol_workflow_ids(&runtime.active_workflows),
            enabled,
        });
    };

    let (device_type, preferred_region, next_action, intent, strategy, selected_worker) =
        match selection {
            BlueprintAcquisitionSelection::Owned {
                device_type,
                source_code,
                factory_code,
                preferred_region,
            } => {
                let requirement = DirectorRequirement::Blueprint {
                    device_type: device_type.as_str().to_owned(),
                };
                let requirement_id = requirement.identity()?;
                let is_required = priorities.contains_key(device_type.as_str());
                let intent = BlueprintAcquireIntent {
                    device_type: device_type.as_str().to_owned(),
                    preferred_region: preferred_region.clone(),
                    requested_by: is_required.then_some(requirement_id).into_iter().collect(),
                    source_device: Some(source_code.clone()),
                    autofactory: Some(factory_code.clone()),
                    acquisition_replicant: None,
                    shop: None,
                };
                (
                    device_type.clone(),
                    preferred_region,
                    format!(
                        "Sacrifice owned {} {} at Autofactory {} to learn {}",
                        device_type.as_str(),
                        source_code,
                        factory_code,
                        device_type.as_str()
                    ),
                    intent,
                    "owned",
                    None,
                )
            }
            BlueprintAcquisitionSelection::Shop {
                device_type,
                opportunity,
                factory_code,
                replicant_code,
                preferred_region,
            } => {
                let requirement = DirectorRequirement::Blueprint {
                    device_type: device_type.as_str().to_owned(),
                };
                let requirement_id = requirement.identity()?;
                let is_required = priorities.contains_key(device_type.as_str());
                let shop_name = opportunity
                    .shop_name
                    .clone()
                    .unwrap_or_else(|| opportunity.controller_code.clone());
                let intent = BlueprintAcquireIntent {
                    device_type: device_type.as_str().to_owned(),
                    preferred_region: preferred_region.clone(),
                    requested_by: is_required.then_some(requirement_id).into_iter().collect(),
                    source_device: None,
                    autofactory: Some(factory_code.clone()),
                    acquisition_replicant: Some(replicant_code.clone()),
                    shop: Some(BlueprintShopPurchaseIntent {
                        controller_code: opportunity.controller_code.clone(),
                        trade_code: opportunity.trade_code.clone(),
                        shop_location: opportunity.shop_location.clone(),
                        shop_system: opportunity.shop_system.clone(),
                        criteria: opportunity.criteria.clone(),
                    }),
                };
                (
                    device_type.clone(),
                    preferred_region,
                    format!(
                        "Stage criteria and send Replicant {} to {} for trade {}, then decommission the purchased {} at Autofactory {}",
                        replicant_code,
                        shop_name,
                        opportunity.trade_code,
                        device_type.as_str(),
                        factory_code
                    ),
                    intent,
                    "shop",
                    Some(replicant_code),
                )
            }
        };

    let requirement = DirectorRequirement::Blueprint {
        device_type: device_type.as_str().to_owned(),
    };
    let requirement_id = requirement.identity()?;
    let is_required = priorities.contains_key(device_type.as_str());
    if context.automatic {
        if let Some(worker) = selected_worker.as_deref() {
            blueprint.reserved_workers.insert(worker.to_owned());
        }
        let workflow = context
            .repository
            .create(new_blueprint_acquire_workflow(intent))?;
        if is_required {
            requirements.attach_workflow(&requirement_id, workflow.id)?;
        }
        tracing::info!(
            event = "director.blueprint.strategy_selected",
            workflow_id = %workflow.id,
            device_type = %device_type.as_str(),
            strategy,
            region = preferred_region.as_deref().unwrap_or("global"),
            "Director launched blueprint acquisition"
        );
        runtime.active_workflows = vec![workflow.id];
        runtime.last_launch_at_ms = Some(context.now);
    }
    save_goal_runtime(context.repository, &id, &runtime)?;
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: None,
        status: DirectorGoalStatus::Active,
        objective: "Acquire missing blueprints from owned copies or known stocked shops".to_owned(),
        blocker: None,
        next_action: Some(next_action),
        progress_current: known_tracked as u64,
        progress_total: tracked_types.len() as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}

fn active_blueprint_acquisition_workflow(workflows: &[WorkflowInstance]) -> Option<WorkflowId> {
    workflows
        .iter()
        .filter(|workflow| {
            workflow.kind == blueprint_acquire_workflow_kind() && !workflow.status.is_terminal()
        })
        .max_by_key(|workflow| workflow.created_at)
        .map(|workflow| workflow.id)
}

fn active_blueprint_claims(
    repository: &WorkflowRepository,
    workflows: &[WorkflowInstance],
) -> Result<(BTreeSet<String>, BTreeSet<String>), ApplicationError> {
    let mut devices = BTreeSet::new();
    let mut autofactories = BTreeSet::new();
    for workflow in workflows
        .iter()
        .filter(|workflow| !workflow.status.is_terminal())
    {
        for claim in repository.claims(workflow.id)? {
            match claim.resource {
                ResourceKey::Device(code) => {
                    devices.insert(code);
                }
                ResourceKey::Autofactory(code) => {
                    autofactories.insert(code);
                }
                _ => {}
            }
        }
    }
    Ok((devices, autofactories))
}

#[allow(clippy::too_many_arguments)]
fn reconcile_maintain_system_hubs(
    context: &GoalReconcileContext<'_>,
    region: &RegionView,
    regions: &BTreeMap<String, RegionView>,
    devices: &[Device],
    locations: &[Location],
    inventories: &[Inventory],
    hub_refresh_errors: &BTreeMap<String, String>,
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::MaintainSystemHubs;
    let enabled = goal_enabled(context.controls, kind);
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);
    let recently_launched = launch_is_recent(&runtime, context.now, DEFAULT_RETRY_COOLDOWN_MS);
    let stocks = stock_locations(
        inventories,
        locations,
        location_systems,
        system_regions,
        regions,
    );
    let mut hubs = devices
        .iter()
        .filter(|device| device.device_type.as_ref() == Some(&DeviceType::SystemHub))
        .filter(|device| {
            device
                .status
                .as_ref()
                .is_some_and(|status| status.as_str() == "active")
        })
        .filter_map(|device| {
            let system = device_system(device, location_systems)?;
            let belongs = system_regions
                .get(&system)
                .is_some_and(|candidate| candidate == &region.region)
                || (system_regions.get(&system).is_none()
                    && region.hub_system.as_deref() == Some(system.as_str()));
            belongs.then_some((device, system))
        })
        .collect::<Vec<_>>();
    hubs.sort_by(|(left, _), (right, _)| left.key.id.cmp(&right.key.id));

    if !enabled {
        save_goal_runtime(context.repository, &id, &runtime)?;
        return Ok(DirectorGoalSummary {
            id,
            kind,
            region: Some(region.region.clone()),
            status: DirectorGoalStatus::Waiting,
            objective: format!(
                "Keep every operational System Hub in {} supplied",
                region.region
            ),
            blocker: None,
            next_action: Some("Enable this standing goal".to_owned()),
            progress_current: 0,
            progress_total: hubs.len() as u64,
            active_workflows: protocol_workflow_ids(&runtime.active_workflows),
            enabled,
        });
    }

    let mut supplied = 0usize;
    let mut blockers = Vec::new();
    let mut actions = Vec::new();
    let mut active_count = 0usize;
    let mut ready_count = 0usize;
    let mut launched_any = false;

    for (hub, system) in &hubs {
        let code = hub.key.id.as_str().to_owned();
        let Some(location) = hub
            .location
            .as_ref()
            .map(|location| location.id.as_str().to_owned())
        else {
            blockers.push(format!("hub {code} has no exact managed location"));
            continue;
        };
        if let Some(error) = hub_refresh_errors.get(&code) {
            blockers.push(format!(
                "{code} @ {location}: managed hub refresh failed ({error}); refusing automatic supply from stale upkeep state"
            ));
            continue;
        }
        let deficits = match exact_upkeep_deficits(hub) {
            Ok(deficits) => deficits,
            Err(error) => {
                tracing::warn!(
                    event = "director.hub.upkeep_unrecognized",
                    region = %region.region,
                    hub = %code,
                    error = %error,
                    "Director cannot safely normalize System Hub upkeep"
                );
                blockers.push(format!("{code} @ {location}: {error}"));
                continue;
            }
        };
        let patrol_available =
            patrol_available_in_system(devices, system.as_str(), location_systems);
        let degraded = hub_is_degraded(hub);
        if deficits.is_empty() {
            supplied += 1;
            tracing::debug!(
                event = "director.hub.upkeep_observed",
                region = %region.region,
                hub = %code,
                location = %location,
                missing = "none",
                "System Hub has no reported upkeep deficit"
            );
            if degraded && !patrol_available {
                blockers.push(format!(
                    "{code} @ {location} is degraded but {system} has no maintenance drone on patrol"
                ));
            }
            continue;
        }

        let manifest = format_resource_manifest(&deficits);
        tracing::info!(
            event = "director.hub.supply_required",
            region = %region.region,
            hub = %code,
            location = %location,
            grace_period_remaining = ?hub.grace_period_remaining,
            missing = %manifest,
            "System Hub requires upkeep supply"
        );

        if !patrol_available {
            blockers.push(format!(
                "{code} @ {location} needs {manifest}, and {system} has no maintenance drone on patrol"
            ));
        }

        if let Some(workflow_id) =
            active_hub_supply_workflow(context.workflows, &region.region, &code, &location)?
        {
            active_count += 1;
            if !runtime.active_workflows.contains(&workflow_id) {
                runtime.active_workflows.push(workflow_id);
            }
            actions.push(format!("finish supplying {code} @ {location}: {manifest}"));
            continue;
        }

        let view = HubMaintenanceView {
            system: (*system).clone(),
            location: location.clone(),
            deficits: deficits.clone(),
            grace_period_remaining: hub.grace_period_remaining,
            degraded,
        };
        let Some(source) = choose_hub_supply_source(&view, region, &stocks) else {
            blockers.push(format!(
                "{code} @ {location} is missing {manifest}, but no {} source can supply the complete manifest",
                if hub_at_risk(&view) { "reachable" } else { "regional" }
            ));
            continue;
        };

        ready_count += 1;
        actions.push(format!(
            "move {manifest} from {} to {location} for {code}",
            source.description
        ));
        if context.automatic && !recently_launched {
            let purpose = hub_supply_purpose(&region.region, &code);
            let workflow = context.repository.create(new_logistics_manifest_workflow(
                LogisticsManifestIntent {
                    origin: source.origin,
                    destination: location.clone(),
                    resources: deficits,
                    devices: Vec::new(),
                    device_codes: Vec::new(),
                    device_tags: Vec::new(),
                    return_transports: true,
                    allow_transport_staging: false,
                    region: Some(region.region.clone()),
                    purpose,
                },
            ))?;
            tracing::info!(
                event = "director.hub.supply_workflow_created",
                workflow_id = %workflow.id,
                region = %region.region,
                hub = %code,
                location = %location,
                missing = %manifest,
                "Director launched System Hub supply manifest"
            );
            runtime.active_workflows.push(workflow.id);
            active_count += 1;
            launched_any = true;
        }
    }

    runtime
        .active_workflows
        .sort_by_key(|workflow_id| workflow_id.to_string());
    runtime.active_workflows.dedup();
    if launched_any {
        runtime.last_launch_at_ms = Some(context.now);
    }
    save_goal_runtime(context.repository, &id, &runtime)?;

    let total = hubs.len();
    let blocker = (!blockers.is_empty()).then(|| summarize_messages(&blockers));
    let next_action = if !actions.is_empty() {
        Some(summarize_messages(&actions))
    } else if total == 0 {
        Some("Wait for an operational regional System Hub".to_owned())
    } else if supplied == total && blocker.is_none() {
        Some("Keep observing reported hub upkeep requirements".to_owned())
    } else if recently_launched {
        Some("Wait briefly before retrying failed hub supply work".to_owned())
    } else {
        blocker
            .as_ref()
            .map(|_| "Resolve the reported System Hub maintenance blocker".to_owned())
    };
    let status = if total == 0 {
        DirectorGoalStatus::Waiting
    } else if active_count > 0 || ready_count > 0 {
        DirectorGoalStatus::Active
    } else if blocker.is_some() {
        DirectorGoalStatus::Blocked
    } else if supplied == total {
        DirectorGoalStatus::Satisfied
    } else {
        DirectorGoalStatus::Waiting
    };

    Ok(DirectorGoalSummary {
        id,
        kind,
        region: Some(region.region.clone()),
        status,
        objective: format!(
            "Keep every operational System Hub in {} supplied",
            region.region
        ),
        blocker,
        next_action,
        progress_current: supplied as u64,
        progress_total: total as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}

fn exact_upkeep_deficits(device: &Device) -> Result<ResourceMap, String> {
    let mut deficits = ResourceMap::new();
    for requirement in &device.upkeep_requirements {
        let resource = ["resource_type", "resource"]
            .into_iter()
            .find_map(|key| requirement.get(key).and_then(Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                format!(
                    "upkeep requirement has no explicit resource field (keys: {})",
                    requirement.keys().cloned().collect::<Vec<_>>().join(", ")
                )
            })?;
        let missing = [
            "missing",
            "remaining",
            "deficit",
            "missing_quantity",
            "quantity_missing",
        ]
        .into_iter()
        .find_map(|key| requirement.get(key).and_then(json_integral_i64))
        .ok_or_else(|| {
            format!(
                "upkeep requirement for {resource} has no explicit deficit field; refusing to infer from total requirement"
            )
        })?;
        if missing < 0 {
            return Err(format!(
                "upkeep requirement for {resource} reports negative deficit {missing}"
            ));
        }
        if missing > 0 {
            *deficits.entry(resource.to_owned()).or_default() += missing;
        }
    }
    Ok(deficits)
}

fn json_integral_i64(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
}

fn hub_is_degraded(device: &Device) -> bool {
    let Some(capacity) = device.operational_capacity else {
        return false;
    };
    capacity.percent() < 100.0
}

fn patrol_available_in_system(
    devices: &[Device],
    system: &str,
    location_systems: &BTreeMap<String, String>,
) -> bool {
    devices.iter().any(|device| {
        device.device_type.as_ref() == Some(&DeviceType::MaintenanceDrone)
            && device_system(device, location_systems).as_deref() == Some(system)
            && !device
                .status
                .as_ref()
                .is_some_and(|status| matches!(status.as_str(), "offline" | "deactivated"))
            && device
                .active_directive
                .as_ref()
                .and_then(|directive| directive.directive.as_ref())
                .is_some_and(|directive| directive.as_str() == "patrol")
    })
}

fn stock_locations(
    inventories: &[Inventory],
    locations: &[Location],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
    regions: &BTreeMap<String, RegionView>,
) -> Vec<StockLocation> {
    let belt_locations = locations
        .iter()
        .filter(|location| {
            location
                .location_type
                .as_ref()
                .is_some_and(|kind| kind.as_str() == "belt")
        })
        .map(|location| location.key.id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let mut stocks = inventories
        .iter()
        .filter_map(|inventory| {
            let InventoryOwner::Location(owner) = &inventory.owner else {
                return None;
            };
            let location = owner.id.as_str().to_owned();
            let system = location_systems
                .get(&location)
                .cloned()
                .unwrap_or_else(|| system_prefix(&location).to_owned());
            let region = system_regions
                .get(&system)
                .cloned()
                .or_else(|| operational_region_for_system(&system, regions));
            let resources = inventory
                .items
                .iter()
                .filter(|item| item.quantity > 0)
                .map(|item| (item.resource.clone(), item.quantity))
                .collect::<ResourceMap>();
            (!resources.is_empty()).then_some(StockLocation {
                is_belt: belt_locations.contains(&location),
                location,
                system,
                region,
                resources,
            })
        })
        .collect::<Vec<_>>();
    stocks.sort_by(|left, right| left.location.cmp(&right.location));
    stocks
}

fn choose_hub_supply_source(
    hub: &HubMaintenanceView,
    region: &RegionView,
    stocks: &[StockLocation],
) -> Option<HubSupplySource> {
    let usable = stocks
        .iter()
        .filter(|stock| stock.location != hub.location)
        .collect::<Vec<_>>();

    // Regional hubs are the normal consolidation warehouses. Prefer the exact
    // manufacturing home first (for example SCEPTURUM-BELT-1), then the hub
    // system as an aggregate scope. Remote belt stock is only a fallback.
    if let Some(home_location) = region.hub_location.as_deref()
        && home_location != hub.location
        && usable.iter().copied().any(|stock| {
            stock.location.eq_ignore_ascii_case(home_location)
                && resources_cover(&stock.resources, &hub.deficits)
        })
    {
        return Some(HubSupplySource {
            origin: home_location.to_owned(),
            description: format!("{home_location} regional consolidation hub"),
        });
    }
    if let Some(home_system) = region.hub_system.as_deref()
        && system_resources_cover(&usable, home_system, &hub.deficits)
    {
        return Some(HubSupplySource {
            origin: home_system.to_owned(),
            description: format!("{home_system} regional consolidation hub"),
        });
    }
    if let Some(stock) = usable.iter().copied().find(|stock| {
        stock.system == hub.system
            && stock.is_belt
            && resources_cover(&stock.resources, &hub.deficits)
    }) {
        return Some(HubSupplySource {
            origin: stock.location.clone(),
            description: format!("{} local fallback stock", stock.location),
        });
    }
    if system_resources_cover(&usable, &hub.system, &hub.deficits) {
        return Some(HubSupplySource {
            origin: hub.system.clone(),
            description: format!("{} local fallback inventory", hub.system),
        });
    }

    let mut regional_systems = usable
        .iter()
        .filter(|stock| stock.region.as_deref() == Some(region.region.as_str()))
        .map(|stock| stock.system.clone())
        .collect::<BTreeSet<_>>();
    regional_systems.remove(&hub.system);
    if let Some(system) = regional_systems
        .into_iter()
        .find(|system| system_resources_cover(&usable, system, &hub.deficits))
    {
        return Some(HubSupplySource {
            description: format!("{system} regional inventory"),
            origin: system,
        });
    }

    if hub_at_risk(hub) {
        let local_region = &region.region;
        let cross_region_systems = usable
            .iter()
            .filter(|stock| stock.region.as_deref() != Some(local_region.as_str()))
            .map(|stock| stock.system.clone())
            .collect::<BTreeSet<_>>();
        if let Some(system) = cross_region_systems
            .into_iter()
            .find(|system| system_resources_cover(&usable, system, &hub.deficits))
        {
            return Some(HubSupplySource {
                description: format!("{system} emergency cross-region inventory"),
                origin: system,
            });
        }
    }
    None
}

fn resources_cover(resources: &ResourceMap, deficits: &ResourceMap) -> bool {
    deficits.iter().all(|(resource, required)| {
        resources
            .iter()
            .find(|(candidate, _)| candidate.eq_ignore_ascii_case(resource))
            .map(|(_, available)| *available)
            .unwrap_or(0)
            >= *required
    })
}

fn system_resources_cover(stocks: &[&StockLocation], system: &str, deficits: &ResourceMap) -> bool {
    let mut aggregate = ResourceMap::new();
    for stock in stocks.iter().filter(|stock| stock.system == system) {
        for (resource, quantity) in &stock.resources {
            *aggregate.entry(resource.clone()).or_default() += quantity;
        }
    }
    resources_cover(&aggregate, deficits)
}

fn hub_at_risk(hub: &HubMaintenanceView) -> bool {
    hub.grace_period_remaining
        .is_some_and(|remaining| remaining <= 0)
        || hub.degraded
}

fn hub_supply_purpose(region: &str, hub: &str) -> String {
    format!("director:maintain_system_hubs:{region}:{hub}")
}

fn active_hub_supply_workflow(
    workflows: &[WorkflowInstance],
    region: &str,
    hub: &str,
    destination: &str,
) -> Result<Option<WorkflowId>, ApplicationError> {
    let purpose = hub_supply_purpose(region, hub);
    for workflow in workflows.iter().filter(|workflow| {
        workflow.kind.as_str() == "logistics.manifest" && !workflow.status.is_terminal()
    }) {
        let intent = workflow.config::<LogisticsManifestIntent>()?;
        if intent.purpose == purpose && intent.destination.eq_ignore_ascii_case(destination) {
            return Ok(Some(workflow.id));
        }
    }
    Ok(None)
}

fn format_resource_manifest(resources: &ResourceMap) -> String {
    resources
        .iter()
        .map(|(resource, quantity)| format!("{resource} {quantity}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn summarize_messages(messages: &[String]) -> String {
    const LIMIT: usize = 3;
    let mut summary = messages
        .iter()
        .take(LIMIT)
        .cloned()
        .collect::<Vec<_>>()
        .join("; ");
    if messages.len() > LIMIT {
        summary.push_str(&format!("; +{} more", messages.len() - LIMIT));
    }
    summary
}

fn reconcile_event_completion(
    context: &GoalReconcileContext<'_>,
    region: &RegionView,
    events: &[String],
    event_discovery_error: Option<&str>,
    workers: &[WorkerView],
    reserved: &mut BTreeSet<String>,
    requirements: &mut DirectorRequirementGraph,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::EventCompletion;
    let enabled = goal_enabled(context.controls, kind);
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);
    let active = nonterminal_ids(&runtime, context.workflows);
    let recently_launched = launch_is_recent(&runtime, context.now, DEFAULT_RETRY_COOLDOWN_MS);
    let mut blocker = None;
    let mut next_action = None;
    let status = if !enabled {
        DirectorGoalStatus::Waiting
    } else if let Some(error) = event_discovery_error {
        blocker = Some(format!(
            "{} event discovery is unavailable: {error}",
            region.region
        ));
        next_action = Some("Retry regional event discovery on the next Director pass".to_owned());
        DirectorGoalStatus::Blocked
    } else if events.is_empty() {
        next_action = Some("Wait for new regional events".to_owned());
        DirectorGoalStatus::Satisfied
    } else if !active.is_empty() {
        next_action = Some(format!(
            "Finish the active {}-event regional campaign",
            events.len()
        ));
        DirectorGoalStatus::Active
    } else if recently_launched {
        next_action = Some("Wait briefly before retrying the regional event campaign".to_owned());
        DirectorGoalStatus::Waiting
    } else if let Some(worker) = select_idle_worker(workers, &region.region, reserved, false) {
        next_action = Some(format!(
            "Batch-plan and execute {} active event(s) with {worker}",
            events.len()
        ));
        if context.automatic {
            let home = region
                .hub_location
                .clone()
                .or_else(|| region.hub_system.clone());
            let workflow =
                context
                    .repository
                    .create(new_event_campaign_workflow(EventCampaignIntent {
                        region: region.region.clone(),
                        replicant: Some(worker.clone()),
                        home,
                    }))?;
            tracing::info!(
                workflow_id = %workflow.id,
                region = %region.region,
                replicant = %worker,
                events = events.len(),
                "Director launched regional event campaign"
            );
            runtime.active_workflows = vec![workflow.id];
            runtime.last_launch_at_ms = Some(context.now);
            reserved.insert(worker);
        }
        DirectorGoalStatus::Active
    } else {
        let reason = format!(
            "{} has active events but no idle regional Replicant",
            region.region
        );
        requirements.raise(
            DirectorRequirement::WorkerCapacity {
                region: region.region.clone(),
                count: 1,
                affinity: Some("events".to_owned()),
            },
            &id,
            reason.clone(),
            PRIORITY_EVENT_COMPLETION,
        )?;
        blocker = Some(reason);
        next_action = Some("Wait for a regional worker or grow the regional workforce".to_owned());
        DirectorGoalStatus::Blocked
    };
    save_goal_runtime(context.repository, &id, &runtime)?;
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: Some(region.region.clone()),
        status,
        objective: format!(
            "Batch-plan and complete worthwhile events in {}",
            region.region
        ),
        blocker,
        next_action,
        progress_current: 0,
        progress_total: events.len() as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}

fn reconcile_enhance_catalogue(
    client: &Client,
    context: &GoalReconcileContext<'_>,
    region: &RegionView,
    workers: &[WorkerView],
    reserved: &mut BTreeSet<String>,
    requirements: &mut DirectorRequirementGraph,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::EnhanceStarCatalogue;
    let enabled = goal_enabled(context.controls, kind);
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);

    let explored = client
        .galaxy()
        .catalogue()
        .into_iter()
        .filter(|star| star.explored == Some(true))
        .map(|star| star.key.id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let routable = client
        .galaxy()
        .catalogue()
        .into_iter()
        .filter(|star| star.position.is_some())
        .map(|star| star.key.id.as_str().to_owned())
        .filter(|system| region.known_systems.contains(system))
        .collect::<BTreeSet<_>>();
    let unsurveyed = routable.difference(&explored).cloned().collect::<Vec<_>>();
    let surveyed = region.known_systems.intersection(&explored).count();
    let missing_positions = region.known_systems.difference(&routable).count();
    let active = nonterminal_ids(&runtime, context.workflows);
    let recently_launched = launch_is_recent(&runtime, context.now, DEFAULT_RETRY_COOLDOWN_MS);

    // New Director-created tours carry an exact system allowlist. If an older
    // unbounded scan.tour is already active, let it finish before sharding more
    // work so migration cannot create overlapping survey assignments.
    let mut assigned_targets = BTreeSet::new();
    let mut has_unbounded_active_tour = false;
    for workflow_id in &active {
        let Some(workflow) = context
            .workflows
            .iter()
            .find(|workflow| workflow.id == *workflow_id)
        else {
            continue;
        };
        let intent = workflow.config::<ScanTourIntent>()?;
        if let Some(targets) = intent.target_systems {
            assigned_targets.extend(targets);
        } else {
            has_unbounded_active_tour = true;
        }
    }
    let pending = if has_unbounded_active_tour {
        Vec::new()
    } else {
        unsurveyed
            .iter()
            .filter(|system| !assigned_targets.contains(*system))
            .cloned()
            .collect::<Vec<_>>()
    };

    let desired_parallel = if unsurveyed.is_empty() {
        0
    } else {
        unsurveyed
            .len()
            .div_ceil(CATALOGUE_SYSTEMS_PER_WORKER)
            .clamp(1, MAX_PARALLEL_CATALOGUE_WORKERS)
    };
    let open_slots = desired_parallel.saturating_sub(active.len());
    let available_workers = idle_catalogue_workers(workers, &region.region, reserved);
    let launch_slots = open_slots.min(available_workers.len());
    let worker_shortage = open_slots.saturating_sub(available_workers.len());

    let mut blocker = None;
    let mut next_action = None;
    let status = if !enabled {
        DirectorGoalStatus::Waiting
    } else if unsurveyed.is_empty() && missing_positions == 0 {
        next_action =
            Some("Wait for newly discovered systems or stale catalogue coverage".to_owned());
        DirectorGoalStatus::Satisfied
    } else if unsurveyed.is_empty() {
        blocker = Some(format!(
            "{} known system(s) in {} do not yet have routeable catalogue positions",
            missing_positions, region.region
        ));
        next_action = Some(
            "Wait for catalogue position data before assigning another survey tour".to_owned(),
        );
        DirectorGoalStatus::Blocked
    } else if has_unbounded_active_tour {
        next_action = Some(format!(
            "Finish the existing regional survey tour before repartitioning {} remaining system(s)",
            unsurveyed.len()
        ));
        DirectorGoalStatus::Active
    } else {
        if worker_shortage > 0 && !pending.is_empty() {
            let reason = format!(
                "{} catalogue backlog can use {desired_parallel} parallel worker(s), but {worker_shortage} slot(s) lack an idle regional Replicant with a racing vessel",
                region.region
            );
            requirements.raise(
                DirectorRequirement::WorkerCapacity {
                    region: region.region.clone(),
                    count: worker_shortage,
                    affinity: Some("catalogue".to_owned()),
                },
                &id,
                reason.clone(),
                PRIORITY_CATALOGUE,
            )?;
            blocker = Some(reason);
        }

        if context.automatic && launch_slots > 0 && !pending.is_empty() && !recently_launched {
            let center = region
                .hub_system
                .clone()
                .or_else(|| region.known_systems.iter().next().cloned());
            if let Some(center) = center {
                let shards = partition_systems(&pending, launch_slots);
                for ((worker, vessel), systems) in available_workers.into_iter().zip(shards) {
                    if systems.is_empty() {
                        continue;
                    }
                    let system_count = systems.len();
                    let workflow =
                        context
                            .repository
                            .create(new_scan_tour_workflow(ScanTourIntent {
                                center: center.clone(),
                                radius_ly: regional_radius(region, client),
                                system_limit: systems.len().saturating_add(1),
                                target_systems: Some(systems),
                                replicant: Some(worker.clone()),
                                vessel: Some(vessel),
                                include_explored: false,
                            }))?;
                    tracing::info!(
                        workflow_id = %workflow.id,
                        region = %region.region,
                        replicant = %worker,
                        systems = system_count,
                        "Director launched catalogue survey shard"
                    );
                    runtime.active_workflows.push(workflow.id);
                    reserved.insert(worker);
                }
                if runtime.active_workflows.len() > active.len() {
                    runtime.last_launch_at_ms = Some(context.now);
                }
            }
        }

        let active_after = runtime.active_workflows.len();
        if active_after > 0 {
            next_action = Some(format!(
                "Survey {} remaining system(s) across {} regional catalogue worker(s)",
                unsurveyed.len(),
                active_after
            ));
            DirectorGoalStatus::Active
        } else if recently_launched {
            next_action =
                Some("Wait briefly before repartitioning the next survey batch".to_owned());
            DirectorGoalStatus::Waiting
        } else if launch_slots > 0 {
            next_action = Some(format!(
                "Partition {} unsurveyed system(s) across up to {desired_parallel} regional worker(s)",
                unsurveyed.len()
            ));
            DirectorGoalStatus::Active
        } else {
            next_action = Some(
                "Free catalogue-capable regional workers or grow the regional workforce".to_owned(),
            );
            DirectorGoalStatus::Blocked
        }
    };
    save_goal_runtime(context.repository, &id, &runtime)?;
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: Some(region.region.clone()),
        status,
        objective: format!("Survey known systems throughout {}", region.region),
        blocker,
        next_action,
        progress_current: surveyed as u64,
        progress_total: region.known_systems.len() as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}

#[allow(clippy::too_many_arguments)]
fn reconcile_discover_belts(
    context: &GoalReconcileContext<'_>,
    region: &RegionView,
    workers: &[WorkerView],
    reserved: &mut BTreeSet<String>,
    requirements: &mut DirectorRequirementGraph,
    locations: &[Location],
    location_systems: &BTreeMap<String, String>,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::DiscoverBelts;
    let enabled = goal_enabled(context.controls, kind);
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);
    let searched = belt_searched_systems(locations, location_systems);
    let targets = region
        .known_systems
        .difference(&searched)
        .take(MINING_BATCH_SIZE)
        .cloned()
        .collect::<Vec<_>>();
    let covered = region.known_systems.intersection(&searched).count();
    let active = nonterminal_ids(&runtime, context.workflows);
    let recently_launched = launch_is_recent(&runtime, context.now, DEFAULT_RETRY_COOLDOWN_MS);
    let mut blocker = None;
    let next_action;
    let status = if !enabled {
        next_action =
            Some("Enable this standing goal to search known systems for belts".to_owned());
        DirectorGoalStatus::Waiting
    } else if targets.is_empty() {
        next_action = Some("Wait for newly discovered regional systems".to_owned());
        DirectorGoalStatus::Satisfied
    } else if !active.is_empty() {
        next_action = Some("Continue the active regional fast belt search".to_owned());
        DirectorGoalStatus::Active
    } else if recently_launched {
        next_action = Some("Wait briefly before retrying the next belt-search batch".to_owned());
        DirectorGoalStatus::Waiting
    } else if let Some(worker) = select_idle_worker(workers, &region.region, reserved, false) {
        next_action = Some(format!(
            "Search {} unscanned regional system(s) for belts with {worker}",
            targets.len()
        ));
        if context.automatic {
            let workflow = context
                .repository
                .create(new_belt_search_campaign_workflow(
                    BeltSearchCampaignIntent {
                        systems: targets,
                        replicant: Some(worker.clone()),
                    },
                ))?;
            runtime.active_workflows = vec![workflow.id];
            runtime.last_launch_at_ms = Some(context.now);
            reserved.insert(worker);
        }
        DirectorGoalStatus::Active
    } else {
        let reason = format!(
            "{} has unscanned systems but no idle regional Replicant",
            region.region
        );
        requirements.raise(
            DirectorRequirement::WorkerCapacity {
                region: region.region.clone(),
                count: 1,
                affinity: Some("survey".to_owned()),
            },
            &id,
            reason.clone(),
            PRIORITY_CATALOGUE,
        )?;
        blocker = Some(reason);
        next_action = Some("Free a regional worker or grow the regional workforce".to_owned());
        DirectorGoalStatus::Blocked
    };
    save_goal_runtime(context.repository, &id, &runtime)?;
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: Some(region.region.clone()),
        status,
        objective: format!("Discover asteroid belts throughout {}", region.region),
        blocker,
        next_action,
        progress_current: covered as u64,
        progress_total: region.known_systems.len() as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}

fn belt_searched_systems(
    locations: &[Location],
    location_systems: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    locations
        .iter()
        .filter(|location| {
            location.system_scanned == Some(true)
                || location
                    .location_type
                    .as_ref()
                    .is_some_and(|kind| kind.as_str() == "belt")
        })
        .filter_map(|location| {
            location
                .system
                .clone()
                .or_else(|| location_systems.get(location.key.id.as_str()).cloned())
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn reconcile_expand_mining(
    repository: &WorkflowRepository,
    region: &RegionView,
    workers: &[WorkerView],
    workflows: &[WorkflowInstance],
    devices: &[Device],
    locations: &[Location],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
    controls: &BTreeMap<DirectorGoalKind, bool>,
    automatic: bool,
    reserved: &mut BTreeSet<String>,
    requirements: &mut DirectorRequirementGraph,
    now: i64,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::ExpandMiningOps;
    let enabled = goal_enabled(controls, kind);
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(repository, &id)?;
    prune_runtime_workflows(&mut runtime, workflows);
    let belt_systems = locations
        .iter()
        .filter(|location| {
            location
                .location_type
                .as_ref()
                .is_some_and(|kind| kind.as_str() == "belt")
        })
        .filter_map(|location| {
            location
                .system
                .clone()
                .or_else(|| location_systems.get(location.key.id.as_str()).cloned())
        })
        .filter(|system| {
            system_regions
                .get(system)
                .is_some_and(|candidate| candidate == &region.region)
        })
        .collect::<BTreeSet<_>>();
    let staffed_systems = devices
        .iter()
        .filter(|device| device.device_type.as_ref() == Some(&DeviceType::MiningController))
        .filter_map(|device| device_system(device, location_systems))
        .collect::<BTreeSet<_>>();
    let relay_systems = relay_device_systems(devices, location_systems);
    let unstaffed = belt_systems
        .difference(&staffed_systems)
        .cloned()
        .collect::<BTreeSet<_>>();
    let targets = relay_connected_mining_targets(&unstaffed, &relay_systems);
    let covered = belt_systems
        .len()
        .saturating_sub(belt_systems.difference(&staffed_systems).count());
    let active = nonterminal_ids(&runtime, workflows);
    let recently_launched = launch_is_recent(&runtime, now, DEFAULT_RETRY_COOLDOWN_MS);
    let mut blocker = None;
    let mut next_action = None;
    let status = if !enabled {
        DirectorGoalStatus::Waiting
    } else if unstaffed.is_empty() {
        next_action = Some("Wait for newly discovered belts or depleted mining spokes".to_owned());
        DirectorGoalStatus::Satisfied
    } else if !active.is_empty() {
        next_action = Some("Continue the active regional mining expansion batch".to_owned());
        DirectorGoalStatus::Active
    } else if recently_launched {
        next_action =
            Some("Wait briefly before replanning the next mining expansion batch".to_owned());
        DirectorGoalStatus::Waiting
    } else if targets.is_empty() {
        next_action =
            Some("Wait for FTL relay coverage to reach a discovered belt system".to_owned());
        DirectorGoalStatus::Waiting
    } else if let Some(worker) = select_idle_worker(workers, &region.region, reserved, false) {
        next_action = Some(format!(
            "Expand mining into {} known belt system(s) as one batch",
            targets.len()
        ));
        if automatic
            && let Some(hub) = region
                .hub_location
                .clone()
                .or_else(|| region.hub_system.clone())
        {
            let workflow =
                repository.create(new_mining_campaign_workflow(MiningCampaignIntent {
                    systems: targets,
                    replicant: Some(worker.clone()),
                    hub: Some(hub),
                    max_concurrency: 4,
                }))?;
            tracing::info!(
                workflow_id = %workflow.id,
                region = %region.region,
                replicant = %worker,
                "Director launched regional mining campaign"
            );
            runtime.active_workflows = vec![workflow.id];
            runtime.last_launch_at_ms = Some(now);
            reserved.insert(worker);
        }
        DirectorGoalStatus::Active
    } else {
        let reason = format!(
            "{} has unstaffed known belts but no idle regional Replicant",
            region.region
        );
        requirements.raise(
            DirectorRequirement::WorkerCapacity {
                region: region.region.clone(),
                count: 1,
                affinity: Some("mining".to_owned()),
            },
            &id,
            reason.clone(),
            PRIORITY_MINING,
        )?;
        blocker = Some(reason);
        next_action = Some("Free a regional worker or grow the regional workforce".to_owned());
        DirectorGoalStatus::Blocked
    };
    save_goal_runtime(repository, &id, &runtime)?;
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: Some(region.region.clone()),
        status,
        objective: format!(
            "Continually extend useful mining coverage throughout {}",
            region.region
        ),
        blocker,
        next_action,
        progress_current: covered as u64,
        progress_total: belt_systems.len() as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}

fn relay_connected_mining_targets(
    unstaffed_belt_systems: &BTreeSet<String>,
    relay_systems: &BTreeSet<String>,
) -> Vec<String> {
    unstaffed_belt_systems
        .intersection(relay_systems)
        .take(MINING_BATCH_SIZE)
        .cloned()
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn reconcile_expand_ftl_network(
    context: &GoalReconcileContext<'_>,
    region: &RegionView,
    workers: &[WorkerView],
    reserved: &mut BTreeSet<String>,
    requirements: &mut DirectorRequirementGraph,
    devices: &[Device],
    locations: &[Location],
    location_systems: &BTreeMap<String, String>,
    event_systems: &BTreeSet<String>,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::ExpandFtlNetwork;
    let enabled = goal_enabled(context.controls, kind);
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);

    let relay_systems = relay_device_systems(devices, location_systems);
    let belt_density = belt_density_priorities(locations, location_systems);
    let covered = region.known_systems.intersection(&relay_systems).count();
    let mut uncovered = region
        .known_systems
        .iter()
        .filter(|system| !relay_systems.contains(*system))
        .map(|system| {
            let event_priority = event_systems.contains(system);
            let density_priority = belt_density.get(system).copied().unwrap_or_default();
            let score = ftl_priority_score(event_priority, density_priority);
            (system.clone(), score, event_priority, density_priority)
        })
        .collect::<Vec<_>>();
    uncovered.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    let mut active = nonterminal_ids(&runtime, context.workflows);
    let recently_launched = launch_is_recent(&runtime, context.now, DEFAULT_RETRY_COOLDOWN_MS);
    let mut blocker = None;
    let (status, next_action) = if !enabled {
        (
            DirectorGoalStatus::Waiting,
            Some("Enable this standing goal to extend regional FTL coverage".to_owned()),
        )
    } else if uncovered.is_empty() {
        (
            DirectorGoalStatus::Satisfied,
            Some("Wait for newly discovered regional systems or new strategic demand".to_owned()),
        )
    } else if !active.is_empty() {
        (
            DirectorGoalStatus::Active,
            Some("Continue the active regional FTL expansion campaign".to_owned()),
        )
    } else {
        let (target, _, event_priority, density_priority) = &uncovered[0];
        if let Some(existing) = active_exploration_workflow_for_target(context.workflows, target)? {
            runtime.active_workflows = vec![existing];
            active = vec![existing];
        }
        if !active.is_empty() {
            (
                DirectorGoalStatus::Active,
                Some(format!(
                    "Continue the existing FTL expansion toward {target}{}",
                    ftl_priority_suffix(*event_priority, *density_priority)
                )),
            )
        } else if recently_launched {
            (
                DirectorGoalStatus::Waiting,
                Some(format!(
                    "Wait briefly before retrying FTL expansion toward {target}{}",
                    ftl_priority_suffix(*event_priority, *density_priority)
                )),
            )
        } else if let Some(worker) =
            select_idle_ftl_worker(workers, &region.region, reserved, devices)
        {
            let Some(hub) = region
                .hub_location
                .clone()
                .or_else(|| region.hub_system.clone())
            else {
                blocker = Some(format!(
                    "{} has no operational manufacturing hub for relay expansion",
                    region.region
                ));
                let next_action =
                    Some("Establish a regional System Hub before expanding FTL reach".to_owned());
                save_goal_runtime(context.repository, &id, &runtime)?;
                return Ok(DirectorGoalSummary {
                    id,
                    kind,
                    region: Some(region.region.clone()),
                    status: DirectorGoalStatus::Blocked,
                    objective: format!(
                        "Connect strategic event and mining systems across {}",
                        region.region
                    ),
                    blocker,
                    next_action,
                    progress_current: covered as u64,
                    progress_total: region.known_systems.len() as u64,
                    active_workflows: protocol_workflow_ids(&runtime.active_workflows),
                    enabled,
                });
            };
            let next_action = Some(format!(
                "Extend FTL coverage toward {target} with {worker}{}",
                ftl_priority_suffix(*event_priority, *density_priority)
            ));
            if context.automatic {
                let workflow =
                    context
                        .repository
                        .create(new_exploration_workflow(ExplorationIntent {
                            target: target.clone(),
                            replicant: Some(worker.clone()),
                            hub: Some(hub),
                        }))?;
                tracing::info!(
                    event = "director.ftl.connection_required",
                    workflow_id = %workflow.id,
                    region = %region.region,
                    target = %target,
                    event_priority = *event_priority,
                    belt_density_priority = *density_priority,
                    replicant = %worker,
                    "Director launched prioritized regional FTL expansion"
                );
                runtime.active_workflows = vec![workflow.id];
                runtime.last_launch_at_ms = Some(context.now);
                reserved.insert(worker);
            }
            (DirectorGoalStatus::Active, next_action)
        } else {
            let has_idle_racing_worker = workers.iter().any(|worker| {
                worker.region.as_deref() == Some(region.region.as_str())
                    && worker.busy_workflow.is_none()
                    && !reserved.contains(worker.replicant.key.id.as_str())
                    && worker.racing_vessel.is_some()
            });
            let reason = if has_idle_racing_worker {
                format!(
                    "{} has an idle FTL-capable regional worker, but its vessel has no free stow slot for a mission relay toward {target}",
                    region.region
                )
            } else {
                format!(
                    "{} needs an idle regional Replicant hosted in a racing vessel to extend FTL coverage toward {target}",
                    region.region
                )
            };
            if !has_idle_racing_worker {
                requirements.raise(
                    DirectorRequirement::WorkerCapacity {
                        region: region.region.clone(),
                        count: 1,
                        affinity: Some("ftl".to_owned()),
                    },
                    &id,
                    reason.clone(),
                    PRIORITY_FTL_EXPANSION,
                )?;
            }
            blocker = Some(reason);
            (
                DirectorGoalStatus::Blocked,
                Some(if has_idle_racing_worker {
                    "Free at least one stow slot on a regional racing vessel before retrying FTL expansion".to_owned()
                } else {
                    "Free a regional worker or grow the regional workforce".to_owned()
                }),
            )
        }
    };
    save_goal_runtime(context.repository, &id, &runtime)?;
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: Some(region.region.clone()),
        status,
        objective: format!(
            "Connect strategic event and mining systems across {}",
            region.region
        ),
        blocker,
        next_action,
        progress_current: covered as u64,
        progress_total: region.known_systems.len() as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}

fn relay_device_systems(
    devices: &[Device],
    location_systems: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    devices
        .iter()
        .filter(|device| {
            device.device_type.as_ref().is_some_and(|device_type| {
                matches!(device_type, DeviceType::FtlRelay | DeviceType::SystemHub)
                    || device_type.as_str() == "deep_space_relay_station"
            })
        })
        .filter_map(|device| device_system(device, location_systems))
        .collect()
}

fn belt_density_priorities(
    locations: &[Location],
    location_systems: &BTreeMap<String, String>,
) -> BTreeMap<String, u8> {
    let mut priorities = BTreeMap::<String, u8>::new();
    for location in locations {
        let density = ["belt", "asteroid_belt"]
            .iter()
            .filter_map(|field| location.unknown.get(*field))
            .map(belt_density_priority)
            .max()
            .unwrap_or_default();
        if density == 0 {
            continue;
        }
        let Some(system) = location
            .system
            .clone()
            .or_else(|| location_systems.get(location.key.id.as_str()).cloned())
        else {
            continue;
        };
        priorities
            .entry(system)
            .and_modify(|priority| *priority = (*priority).max(density))
            .or_insert(density);
    }
    priorities
}

fn belt_density_priority(value: &Value) -> u8 {
    let rank = |density: &str| match density.to_ascii_lowercase().as_str() {
        "dense" => 2,
        "moderate" => 1,
        _ => 0,
    };
    value
        .get("belts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|belt| belt.get("density").and_then(Value::as_str))
        .chain(value.get("density").and_then(Value::as_str))
        .map(rank)
        .max()
        .unwrap_or_default()
}

fn ftl_priority_score(event_priority: bool, density_priority: u8) -> u32 {
    (if event_priority { 200 } else { 0 }) + u32::from(density_priority) * 80
}

fn ftl_priority_suffix(event_priority: bool, density_priority: u8) -> &'static str {
    match (event_priority, density_priority) {
        (true, 2..) => " (active events + dense belt)",
        (true, 1) => " (active events + moderate belt)",
        (true, _) => " (active events)",
        (false, 2..) => " (dense belt)",
        (false, 1) => " (moderate belt)",
        (false, _) => "",
    }
}

fn active_exploration_workflow_for_target(
    workflows: &[WorkflowInstance],
    target: &str,
) -> Result<Option<WorkflowId>, ApplicationError> {
    for workflow in workflows.iter().filter(|workflow| {
        workflow.kind == exploration_workflow_kind() && !workflow.status.is_terminal()
    }) {
        let intent = workflow.config::<ExplorationIntent>()?;
        if intent.target == target {
            return Ok(Some(workflow.id));
        }
    }
    Ok(None)
}

fn bootstrap_assignment(
    regions: &BTreeMap<String, RegionView>,
    target_workers: &[&WorkerView],
) -> Option<(String, String, String)> {
    if target_workers.len() < 2 {
        return None;
    }
    for source in regions
        .values()
        .filter(|region| region.status == DirectorRegionStatus::Established)
    {
        let Some(home) = source
            .hub_location
            .as_deref()
            .or(source.hub_system.as_deref())
        else {
            continue;
        };
        let colocated = target_workers
            .iter()
            .filter(|worker| replicant_near_home(&worker.replicant, home))
            .take(2)
            .collect::<Vec<_>>();
        if colocated.len() == 2 {
            return Some((
                home.to_owned(),
                colocated[0].replicant.key.id.as_str().to_owned(),
                colocated[1].replicant.key.id.as_str().to_owned(),
            ));
        }
    }
    None
}

#[allow(clippy::too_many_arguments)]
fn reconcile_workforce(
    repository: &WorkflowRepository,
    settings: &DirectorSettings,
    regions: &BTreeMap<String, RegionView>,
    workers: &[WorkerView],
    workflows: &[WorkflowInstance],
    reserved: &BTreeSet<String>,
    demand: &BTreeMap<String, usize>,
    states: &mut BTreeMap<String, RegionWorkforceState>,
    automatic: bool,
    now: i64,
) -> Result<Vec<String>, ApplicationError> {
    let mut recommendations = Vec::new();
    for (region_name, pending) in demand {
        if *pending == 0 {
            continue;
        }
        let state = states.entry(region_name.clone()).or_default();
        let Some(region) = regions.get(region_name) else {
            continue;
        };
        let bootstrap_region = region.status != DirectorRegionStatus::Established;
        if let Some(workflow_id) = state.provision_workflow_id {
            if let Some(workflow) = workflows.iter().find(|workflow| workflow.id == workflow_id) {
                if !workflow.status.is_terminal() {
                    recommendations.push(format!("{region_name} workforce is already growing"));
                    continue;
                }
                if workflow.status == WorkflowStatus::Failed
                    && state
                        .last_scaled_at_ms
                        .is_some_and(|last| now.saturating_sub(last) < DEFAULT_RETRY_COOLDOWN_MS)
                {
                    recommendations.push(format!(
                        "{region_name} worker provisioning failed recently; waiting for retry cooldown"
                    ));
                    continue;
                }
                if workflow.status == WorkflowStatus::Succeeded && bootstrap_region {
                    // Missing regions intentionally grow to the bootstrap pair one at a time.
                    state.last_scaled_at_ms = None;
                }
            }
            state.provision_workflow_id = None;
        }
        let regional = workers
            .iter()
            .filter(|worker| worker.region.as_deref() == Some(region_name.as_str()))
            .collect::<Vec<_>>();
        let idle = regional
            .iter()
            .filter(|worker| worker.busy_workflow.is_none())
            .count();
        let idle_ratio = if regional.is_empty() {
            0.0
        } else {
            idle as f64 / regional.len() as f64
        };
        if !bootstrap_region && idle_ratio >= settings.scale_up_idle_threshold {
            state.pressure_since_ms = None;
            continue;
        }
        let since = *state.pressure_since_ms.get_or_insert(now);
        let held_long_enough =
            bootstrap_region || now.saturating_sub(since) >= settings.scale_up_hold_ms;
        let cooled_down = bootstrap_region
            || state
                .last_scaled_at_ms
                .is_none_or(|last| now.saturating_sub(last) >= settings.scale_up_cooldown_ms);
        if !held_long_enough || !cooled_down {
            continue;
        }
        let (home, source) = if bootstrap_region {
            let mut homes = regions
                .values()
                .filter(|candidate| candidate.status == DirectorRegionStatus::Established)
                .filter_map(|candidate| {
                    candidate
                        .hub_location
                        .as_deref()
                        .or(candidate.hub_system.as_deref())
                        .map(str::to_owned)
                })
                .collect::<Vec<_>>();
            // Once the first target-region worker exists, keep subsequent
            // bootstrap clones at the same established home so the pair is
            // immediately eligible for the regional ark workflow.
            if let Some(preferred) = regional.iter().find_map(|target_worker| {
                homes
                    .iter()
                    .find(|home| replicant_near_home(&target_worker.replicant, home))
                    .cloned()
            }) {
                homes.sort_by_key(|home| home != &preferred);
            }
            let candidate = homes.into_iter().find_map(|home| {
                workers
                    .iter()
                    .filter(|worker| worker.region.as_deref() != Some(region_name.as_str()))
                    .find(|worker| {
                        worker.busy_workflow.is_none()
                            && !reserved.contains(worker.replicant.key.id.as_str())
                            && replicant_near_home(&worker.replicant, &home)
                    })
                    .map(|worker| (home, worker))
            });
            let Some(candidate) = candidate else {
                recommendations.push(format!("{region_name} needs bootstrap workers but no idle source Replicant is at an established manufacturing home"));
                continue;
            };
            candidate
        } else {
            let Some(home) = region
                .hub_location
                .clone()
                .or_else(|| region.hub_system.clone())
            else {
                recommendations.push(format!(
                    "{region_name} needs another worker but has no regional manufacturing home"
                ));
                continue;
            };
            let source = regional
                .iter()
                .find(|worker| {
                    worker.busy_workflow.is_none()
                        && !reserved.contains(worker.replicant.key.id.as_str())
                        && replicant_near_home(&worker.replicant, &home)
                })
                .copied();
            let Some(source) = source else {
                recommendations.push(format!("{region_name} needs another worker but no idle assigned Replicant is at the regional home"));
                continue;
            };
            (home, source)
        };
        recommendations.push(format!(
            "{region_name} has {pending} worker-blocked campaign(s) and {:.0}% idle reserve; provision one additional Replicant",
            idle_ratio * 100.0
        ));
        if automatic {
            let workflow =
                repository.create(new_replicant_provision_workflow(ReplicantProvisionIntent {
                    region: region_name.clone(),
                    home,
                    source_replicant: source.replicant.key.id.as_str().to_owned(),
                    cradle_type: "racing_vessel".to_owned(),
                    name: None,
                }))?;
            tracing::info!(
                workflow_id = %workflow.id,
                region = %region_name,
                source_replicant = %source.replicant.key.id.as_str(),
                "Director launched grow-only workforce provisioning"
            );
            state.provision_workflow_id = Some(workflow.id);
            state.last_scaled_at_ms = Some(now);
            state.pressure_since_ms = None;
        }
    }
    for (region, state) in states.iter_mut() {
        if !demand.contains_key(region) {
            state.pressure_since_ms = None;
        }
        repository.put_document(WORKFORCE_NS, region, state)?;
    }
    Ok(recommendations)
}

fn group_active_events_by_region(
    events: &[CachedActiveEvent],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
    regions: &BTreeMap<String, RegionView>,
) -> BTreeMap<String, Vec<String>> {
    let mut grouped = BTreeMap::<String, Vec<String>>::new();
    for event in events {
        let Some(designation) = event.designation.as_deref() else {
            continue;
        };
        let Some(location) = event.location.as_deref() else {
            continue;
        };
        let system = location_systems
            .get(location)
            .cloned()
            .unwrap_or_else(|| system_prefix(location).to_owned());
        let region = system_regions
            .get(&system)
            .cloned()
            .or_else(|| operational_region_for_system(&system, regions));
        let Some(region) = region else {
            continue;
        };
        grouped
            .entry(region)
            .or_default()
            .push(designation.to_owned());
    }
    for designations in grouped.values_mut() {
        designations.sort();
        designations.dedup();
    }
    grouped
}

fn group_active_event_systems_by_region(
    events: &[CachedActiveEvent],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
    regions: &BTreeMap<String, RegionView>,
) -> BTreeMap<String, BTreeSet<String>> {
    let mut grouped = BTreeMap::<String, BTreeSet<String>>::new();
    for event in events {
        let Some(location) = event.location.as_deref() else {
            continue;
        };
        let system = location_systems
            .get(location)
            .cloned()
            .unwrap_or_else(|| system_prefix(location).to_owned());
        let region = system_regions
            .get(&system)
            .cloned()
            .or_else(|| operational_region_for_system(&system, regions));
        let Some(region) = region else {
            continue;
        };
        grouped.entry(region).or_default().insert(system);
    }
    grouped
}

fn build_regions(
    catalogue: &[Star],
    devices: &[Device],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
) -> BTreeMap<String, RegionView> {
    let mut regions = BTreeMap::<String, RegionView>::new();
    for star in catalogue {
        let Some(raw_region) = star.region.as_deref() else {
            continue;
        };
        let region = canonical_region(raw_region);
        regions
            .entry(region.clone())
            .or_insert_with(|| RegionView {
                region,
                status: DirectorRegionStatus::Discovered,
                hub_system: None,
                hub_location: None,
                known_systems: BTreeSet::new(),
            })
            .known_systems
            .insert(star.key.id.as_str().to_owned());
    }

    // A region may contain many system hubs. Treat the hub system with the
    // strongest manufacturing footprint as the regional capital instead of
    // depending on API/device iteration order. This keeps established empires
    // anchored on their actual operating hub as relay coverage grows outward.
    let mut infrastructure = BTreeMap::<String, (usize, usize)>::new();
    for device in devices {
        let Some(system) = device_system(device, location_systems) else {
            continue;
        };
        let counts = infrastructure.entry(system).or_default();
        counts.1 += 1;
        if device.device_type.as_ref() == Some(&DeviceType::Autofactory) {
            counts.0 += 1;
        }
    }
    let catalogue_positions = catalogue
        .iter()
        .filter_map(|star| {
            star.position
                .map(|position| (star.key.id.as_str().to_owned(), position))
        })
        .collect::<BTreeMap<_, _>>();
    let hub_candidates = devices
        .iter()
        .filter(|device| device.device_type.as_ref() == Some(&DeviceType::SystemHub))
        .filter_map(|device| {
            let system = device_system(device, location_systems)?;
            let formal_region = system_regions.get(&system).cloned();
            let location = device
                .location
                .as_ref()
                .map(|location| location.id.as_str().to_owned());
            let (factories, owned_devices) =
                infrastructure.get(&system).copied().unwrap_or_default();
            let position = catalogue_positions.get(&system).copied();
            Some((
                formal_region,
                system,
                location,
                factories,
                owned_devices,
                position,
            ))
        })
        .collect::<Vec<_>>();

    // A formally in-region hub is always the preferred capital.
    let mut formal_hubs = hub_candidates
        .iter()
        .filter_map(|candidate| {
            candidate.0.as_ref().map(|region| {
                (
                    region.clone(),
                    candidate.1.clone(),
                    candidate.2.clone(),
                    candidate.3,
                    candidate.4,
                )
            })
        })
        .collect::<Vec<_>>();
    formal_hubs.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| right.3.cmp(&left.3))
            .then_with(|| right.4.cmp(&left.4))
            .then_with(|| left.1.cmp(&right.1))
    });
    for (region_name, system, location, _, _) in formal_hubs {
        let Some(region) = regions.get_mut(&region_name) else {
            continue;
        };
        if region.status == DirectorRegionStatus::Established {
            continue;
        }
        region.status = DirectorRegionStatus::Established;
        region.hub_system = Some(system);
        region.hub_location = location;
    }

    // Some established empires intentionally place their manufacturing capital
    // just outside the game's formal region boundary. Treat an *unregioned*
    // owned hub as a gateway for a still-unestablished region when its 15 LY
    // system-hub reach touches that region. This accounts for border capitals
    // such as SCEPTURUM serving Alpha without allowing a hub formally belonging
    // to Beta to silently establish Alpha as well.
    for region in regions
        .values_mut()
        .filter(|region| region.status != DirectorRegionStatus::Established)
    {
        let gateway = hub_candidates
            .iter()
            .filter(|candidate| candidate.0.is_none())
            .filter_map(|candidate| {
                let hub_position = candidate.5?;
                let distance = region
                    .known_systems
                    .iter()
                    .filter_map(|system| catalogue_positions.get(system).copied())
                    .map(|position| galactic_distance(hub_position, position))
                    .min_by(f64::total_cmp)?;
                (distance <= REGION_GATEWAY_HUB_RANGE_LY).then_some((distance, candidate))
            })
            .min_by(|(left_distance, left), (right_distance, right)| {
                right
                    .3
                    .cmp(&left.3)
                    .then_with(|| right.4.cmp(&left.4))
                    .then_with(|| left_distance.total_cmp(right_distance))
                    .then_with(|| left.1.cmp(&right.1))
            });
        if let Some((_, candidate)) = gateway {
            region.status = DirectorRegionStatus::Established;
            region.hub_system = Some(candidate.1.clone());
            region.hub_location = candidate.2.clone();
        }
    }
    regions
}

fn mark_establishing_regions(
    regions: &mut BTreeMap<String, RegionView>,
    workflows: &[WorkflowInstance],
) -> Result<(), ApplicationError> {
    for workflow in workflows.iter().filter(|workflow| {
        workflow.kind.as_str() == "region.establish" && !workflow.status.is_terminal()
    }) {
        let intent = workflow.config::<RegionEstablishIntent>()?;
        let region = canonical_region(&intent.region);
        if let Some(view) = regions.get_mut(&region)
            && view.status != DirectorRegionStatus::Established
        {
            view.status = DirectorRegionStatus::Establishing;
        }
    }
    Ok(())
}

fn mark_partial_region_footholds(
    regions: &mut BTreeMap<String, RegionView>,
    workers: &[WorkerView],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
) {
    for region in regions
        .values_mut()
        .filter(|region| region.status == DirectorRegionStatus::Discovered)
    {
        let staged_workers = workers
            .iter()
            .filter(|worker| worker.region.as_deref() == Some(region.region.as_str()))
            .filter(|worker| {
                worker.replicant.location.as_ref().is_some_and(|location| {
                    let location = location.id.as_str();
                    let system = location_systems
                        .get(location)
                        .map(String::as_str)
                        .unwrap_or_else(|| system_prefix(location));
                    system_regions
                        .get(system)
                        .is_some_and(|candidate| candidate == &region.region)
                })
            })
            .count();
        if staged_workers >= 2 {
            tracing::info!(
                event = "director.region.partial_foothold_observed",
                region = %region.region,
                staged_workers,
                "Director recognized a partially established regional foothold"
            );
            region.status = DirectorRegionStatus::Establishing;
        }
    }
}

fn mark_manufacturing_footholds(
    regions: &mut BTreeMap<String, RegionView>,
    devices: &[Device],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
) {
    for region in regions.values_mut() {
        let Some(home) = preferred_home_location(
            &region.region,
            region.hub_system.as_deref(),
            devices,
            location_systems,
            system_regions,
        ) else {
            continue;
        };
        let system = location_systems
            .get(&home)
            .cloned()
            .unwrap_or_else(|| system_prefix(&home).to_owned());
        if region.status != DirectorRegionStatus::Established
            && system_regions
                .get(&system)
                .is_some_and(|candidate| candidate == &region.region)
        {
            region.status = DirectorRegionStatus::Establishing;
            region.hub_system = Some(system);
        }
        region.hub_location = Some(home);
    }
}

fn preferred_home_location(
    region: &str,
    hub_system: Option<&str>,
    devices: &[Device],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
) -> Option<String> {
    let mut factories = devices
        .iter()
        .filter(|device| device.device_type.as_ref() == Some(&DeviceType::Autofactory))
        .filter_map(|device| {
            let system = device_system(device, location_systems)?;
            let location = device.location.as_ref()?.id.as_str().to_owned();
            Some((system, location))
        })
        .collect::<Vec<_>>();
    factories.sort();

    // The selected capital may intentionally be an unregioned border system.
    // Prefer its manufacturing location before falling back to factories that
    // are formally inside the region.
    hub_system
        .and_then(|hub| {
            factories
                .iter()
                .find(|(system, _)| system.as_str() == hub)
                .map(|(_, location)| location.clone())
        })
        .or_else(|| {
            factories.into_iter().find_map(|(system, location)| {
                system_regions
                    .get(&system)
                    .is_some_and(|candidate| candidate == region)
                    .then_some(location)
            })
        })
}

fn location_system_map(locations: &[Location]) -> BTreeMap<String, String> {
    locations
        .iter()
        .filter_map(|location| {
            location
                .system
                .as_ref()
                .map(|system| (location.key.id.as_str().to_owned(), system.clone()))
        })
        .collect()
}

fn system_region_map(catalogue: &[Star]) -> BTreeMap<String, String> {
    catalogue
        .iter()
        .filter_map(|star| {
            star.region
                .as_deref()
                .map(|region| (star.key.id.as_str().to_owned(), canonical_region(region)))
        })
        .collect()
}

/// Maps catalogue systems to their formal region, extending each region by
/// the same 15 LY gateway margin used for unregioned automation footholds.
#[must_use]
pub fn expanded_system_region_map(catalogue: &[Star]) -> BTreeMap<String, String> {
    let mut regions = system_region_map(catalogue);
    let regional_systems = catalogue
        .iter()
        .filter_map(|star| {
            Some((
                star.key.id.as_str(),
                canonical_region(star.region.as_deref()?),
                star.position?,
            ))
        })
        .collect::<Vec<_>>();
    for star in catalogue.iter().filter(|star| star.region.is_none()) {
        let Some(position) = star.position else {
            continue;
        };
        let nearest = regional_systems
            .iter()
            .map(|(system, region, candidate)| {
                (
                    galactic_distance(position, *candidate),
                    region.as_str(),
                    *system,
                )
            })
            .filter(|(distance, _, _)| *distance <= REGION_GATEWAY_HUB_RANGE_LY)
            .min_by(|left, right| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| left.1.cmp(right.1))
                    .then_with(|| left.2.cmp(right.2))
            });
        if let Some((_, region, _)) = nearest {
            regions.insert(star.key.id.as_str().to_owned(), region.to_owned());
        }
    }
    regions
}

fn device_system(device: &Device, location_systems: &BTreeMap<String, String>) -> Option<String> {
    device
        .location
        .as_ref()
        .and_then(|location| location_systems.get(location.id.as_str()).cloned())
        .or_else(|| {
            device.location.as_ref().and_then(|location| {
                let id = location.id.as_str();
                id.split_once('-').map(|(system, _)| system.to_owned())
            })
        })
}

fn operational_region_for_system(
    system: &str,
    regions: &BTreeMap<String, RegionView>,
) -> Option<String> {
    let mut matches = regions
        .values()
        .filter(|region| region.hub_system.as_deref() == Some(system))
        .map(|region| region.region.clone());
    let first = matches.next()?;
    matches.next().is_none().then_some(first)
}

fn galactic_distance(left: GalacticPosition, right: GalacticPosition) -> f64 {
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let dz = left.z - right.z;
    (dx * dx + dy * dy + dz * dz).sqrt()
}

fn hosted_racing_vessels(devices: &[Device]) -> BTreeMap<String, String> {
    devices
        .iter()
        .filter(|device| device.device_type.as_ref() == Some(&DeviceType::RacingVessel))
        .filter_map(|device| {
            device
                .relationships
                .hosting_replicant
                .as_ref()
                .map(|replicant| {
                    (
                        replicant.id.as_str().to_owned(),
                        device.key.id.as_str().to_owned(),
                    )
                })
        })
        .collect()
}

fn busy_replicants(
    repository: &WorkflowRepository,
    workflows: &[WorkflowInstance],
) -> Result<BTreeMap<String, WorkflowId>, ApplicationError> {
    let mut busy = BTreeMap::new();
    for workflow in workflows
        .iter()
        .filter(|workflow| !workflow.status.is_terminal())
    {
        // A newly queued workflow has not executed yet, so it may not have
        // acquired its durable Replicant claim. Honor an explicit configured
        // assignment immediately so the next Director reconcile cannot hand
        // the same worker to another workflow during that scheduler gap.
        let config = workflow.config::<Value>()?;
        if let Some(code) = config
            .get("replicant")
            .and_then(Value::as_str)
            .filter(|code| !code.trim().is_empty())
        {
            busy.entry(code.to_owned()).or_insert(workflow.id);
        }
        for claim in repository.claims(workflow.id)? {
            if let ResourceKey::Replicant(code) = claim.resource {
                busy.entry(code).or_insert(workflow.id);
            }
        }
    }
    Ok(busy)
}

fn auto_assign_unassigned_replicants(
    repository: &WorkflowRepository,
    replicants: &[Replicant],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
    regions: &BTreeMap<String, RegionView>,
    now: i64,
) -> Result<(), ApplicationError> {
    let existing = load_assignments(repository)?;
    for replicant in replicants {
        let code = replicant.key.id.as_str();
        if existing.contains_key(code) {
            continue;
        }
        let region = replicant.location.as_ref().and_then(|location| {
            let system = location_systems
                .get(location.id.as_str())
                .cloned()
                .unwrap_or_else(|| system_prefix(location.id.as_str()).to_owned());
            system_regions
                .get(&system)
                .cloned()
                .or_else(|| operational_region_for_system(&system, regions))
        });
        let Some(region) = region else { continue };
        repository.put_document(
            REPLICANT_NS,
            code,
            &ReplicantAssignmentRecord {
                region: Some(region),
                role_affinity: None,
                assigned_at_ms: now,
            },
        )?;
    }
    Ok(())
}

fn absorb_completed_provisions(
    repository: &WorkflowRepository,
    workflows: &[WorkflowInstance],
    now: i64,
) -> Result<(), ApplicationError> {
    for workflow in workflows.iter().filter(|workflow| {
        workflow.kind.as_str() == "replicant.provision"
            && workflow.status == WorkflowStatus::Succeeded
    }) {
        let Some(result) = workflow.result::<Value>()? else {
            continue;
        };
        let Some(replicant) = result.get("replicant").and_then(Value::as_str) else {
            continue;
        };
        let Some(region) = result.get("region").and_then(Value::as_str) else {
            continue;
        };
        if repository.read_document(REPLICANT_NS, replicant)?.is_none() {
            repository.put_document(
                REPLICANT_NS,
                replicant,
                &ReplicantAssignmentRecord {
                    region: Some(canonical_region(region)),
                    role_affinity: None,
                    assigned_at_ms: now,
                },
            )?;
        }
    }
    Ok(())
}

fn load_assignments(
    repository: &WorkflowRepository,
) -> Result<BTreeMap<String, ReplicantAssignmentRecord>, ApplicationError> {
    repository
        .list_documents(REPLICANT_NS)?
        .into_iter()
        .map(|(key, value, _)| Ok((key, serde_json::from_value(value)?)))
        .collect()
}

fn load_goal_controls(
    repository: &WorkflowRepository,
) -> Result<BTreeMap<DirectorGoalKind, bool>, ApplicationError> {
    let mut controls = BTreeMap::new();
    for kind in all_goal_kinds() {
        let enabled = repository
            .read_document(GOAL_CONTROL_NS, goal_kind_key(kind))?
            .map(|(value, _)| serde_json::from_value::<GoalControl>(value))
            .transpose()?
            .map(|control| control.enabled)
            .unwrap_or_else(|| default_goal_enabled(kind));
        controls.insert(kind, enabled);
    }
    Ok(controls)
}

fn load_workforce_states(
    repository: &WorkflowRepository,
) -> Result<BTreeMap<String, RegionWorkforceState>, ApplicationError> {
    repository
        .list_documents(WORKFORCE_NS)?
        .into_iter()
        .map(|(key, value, _)| Ok((key, serde_json::from_value(value)?)))
        .collect()
}

fn load_goal_runtime(
    repository: &WorkflowRepository,
    id: &str,
) -> Result<GoalRuntime, ApplicationError> {
    repository
        .read_document(GOAL_RUNTIME_NS, id)?
        .map(|(value, _)| serde_json::from_value(value))
        .transpose()
        .map(|value| value.unwrap_or_default())
        .map_err(Into::into)
}

fn save_goal_runtime(
    repository: &WorkflowRepository,
    id: &str,
    runtime: &GoalRuntime,
) -> Result<(), ApplicationError> {
    repository.put_document(GOAL_RUNTIME_NS, id, runtime)?;
    Ok(())
}

fn prune_runtime_workflows(runtime: &mut GoalRuntime, workflows: &[WorkflowInstance]) {
    runtime.active_workflows.retain(|id| {
        workflows
            .iter()
            .find(|workflow| workflow.id == *id)
            .is_some_and(|workflow| !workflow.status.is_terminal())
    });
}

fn launch_is_recent(runtime: &GoalRuntime, now: i64, cooldown_ms: i64) -> bool {
    runtime
        .last_launch_at_ms
        .is_some_and(|last| now.saturating_sub(last) < cooldown_ms)
}

fn protocol_workflow_ids(ids: &[WorkflowId]) -> Vec<ProtocolWorkflowId> {
    ids.iter()
        .map(|id| ProtocolWorkflowId(id.to_string()))
        .collect()
}

fn nonterminal_ids(runtime: &GoalRuntime, workflows: &[WorkflowInstance]) -> Vec<WorkflowId> {
    runtime
        .active_workflows
        .iter()
        .copied()
        .filter(|id| {
            workflows
                .iter()
                .find(|workflow| workflow.id == *id)
                .is_some_and(|workflow| !workflow.status.is_terminal())
        })
        .collect()
}

fn idle_catalogue_workers(
    workers: &[WorkerView],
    region: &str,
    reserved: &BTreeSet<String>,
) -> Vec<(String, String)> {
    let mut candidates = workers
        .iter()
        .filter(|worker| worker.region.as_deref() == Some(region))
        .filter(|worker| worker.busy_workflow.is_none())
        .filter(|worker| !reserved.contains(worker.replicant.key.id.as_str()))
        .filter_map(|worker| {
            worker.racing_vessel.as_ref().map(|vessel| {
                (
                    worker.replicant.key.id.as_str().to_owned(),
                    vessel.clone(),
                    worker.role_affinity.is_none(),
                )
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.2.cmp(&right.2).then_with(|| left.0.cmp(&right.0)));
    candidates
        .into_iter()
        .map(|(replicant, vessel, _)| (replicant, vessel))
        .collect()
}

fn partition_systems(systems: &[String], workers: usize) -> Vec<Vec<String>> {
    if systems.is_empty() || workers == 0 {
        return Vec::new();
    }
    let shard_count = workers.min(systems.len());
    let mut shards = vec![Vec::new(); shard_count];
    for (index, system) in systems.iter().enumerate() {
        shards[index % shard_count].push(system.clone());
    }
    shards
}

fn select_idle_ftl_worker(
    workers: &[WorkerView],
    region: &str,
    reserved: &BTreeSet<String>,
    devices: &[Device],
) -> Option<String> {
    workers
        .iter()
        .filter(|worker| worker.region.as_deref() == Some(region))
        .filter(|worker| worker.busy_workflow.is_none())
        .filter(|worker| !reserved.contains(worker.replicant.key.id.as_str()))
        .filter_map(|worker| {
            let vessel_code = worker.racing_vessel.as_deref()?;
            let vessel = devices
                .iter()
                .find(|device| device.key.id.as_str() == vessel_code)?;
            let free_stow = vessel
                .stow_capacity
                .unwrap_or_default()
                .saturating_sub(vessel.stow_used.unwrap_or_default());
            (free_stow > 0).then_some((worker, free_stow))
        })
        .min_by(|(left, left_free), (right, right_free)| {
            (left.role_affinity.as_deref() != Some("ftl"))
                .cmp(&(right.role_affinity.as_deref() != Some("ftl")))
                .then_with(|| right_free.cmp(left_free))
                .then_with(|| left.replicant.key.id.cmp(&right.replicant.key.id))
        })
        .map(|(worker, _)| worker.replicant.key.id.as_str().to_owned())
}

fn select_idle_worker(
    workers: &[WorkerView],
    region: &str,
    reserved: &BTreeSet<String>,
    require_racing_vessel: bool,
) -> Option<String> {
    workers
        .iter()
        .filter(|worker| worker.region.as_deref() == Some(region))
        .filter(|worker| worker.busy_workflow.is_none())
        .filter(|worker| !reserved.contains(worker.replicant.key.id.as_str()))
        .filter(|worker| !require_racing_vessel || worker.racing_vessel.is_some())
        .min_by_key(|worker| worker.role_affinity.is_none())
        .map(|worker| worker.replicant.key.id.as_str().to_owned())
}

fn regional_radius(region: &RegionView, client: &Client) -> f64 {
    let catalogue = client.galaxy().catalogue();
    let Some(center_code) = region.hub_system.as_deref() else {
        return 30.0;
    };
    let Some(center) = catalogue
        .iter()
        .find(|star| star.key.id.as_str() == center_code)
        .and_then(|star| star.position)
    else {
        return 30.0;
    };
    catalogue
        .iter()
        .filter(|star| region.known_systems.contains(star.key.id.as_str()))
        .filter_map(|star| star.position)
        .map(|position| {
            let dx = position.x - center.x;
            let dy = position.y - center.y;
            let dz = position.z - center.z;
            (dx * dx + dy * dy + dz * dz).sqrt()
        })
        .fold(7.5, f64::max)
        + 0.1
}

fn replicant_near_home(replicant: &Replicant, home: &str) -> bool {
    replicant.location.as_ref().is_some_and(|location| {
        let current = location.id.as_str();
        current == home || same_system(current, home)
    })
}

fn same_system(left: &str, right: &str) -> bool {
    system_prefix(left) == system_prefix(right)
}

fn system_prefix(value: &str) -> &str {
    value.split_once('-').map_or(value, |(system, _)| system)
}

fn waiting_goal(
    kind: DirectorGoalKind,
    region: Option<&str>,
    controls: &BTreeMap<DirectorGoalKind, bool>,
    objective: &str,
    blocker: &str,
    next_action: &str,
) -> DirectorGoalSummary {
    let enabled = goal_enabled(controls, kind);
    DirectorGoalSummary {
        id: goal_instance_id(kind, region),
        kind,
        region: region.map(str::to_owned),
        status: DirectorGoalStatus::Waiting,
        objective: objective.to_owned(),
        blocker: enabled.then(|| blocker.to_owned()),
        next_action: Some(
            if enabled {
                next_action
            } else {
                "Enable this standing goal"
            }
            .to_owned(),
        ),
        progress_current: 0,
        progress_total: 0,
        active_workflows: Vec::new(),
        enabled,
    }
}

fn goal_enabled(controls: &BTreeMap<DirectorGoalKind, bool>, kind: DirectorGoalKind) -> bool {
    controls
        .get(&kind)
        .copied()
        .unwrap_or_else(|| default_goal_enabled(kind))
}

fn default_goal_enabled(kind: DirectorGoalKind) -> bool {
    !matches!(kind, DirectorGoalKind::EstablishBeacons)
}

fn initial_goal_objective(kind: DirectorGoalKind) -> &'static str {
    match kind {
        DirectorGoalKind::EstablishRegions => {
            "Establish a durable foothold in every discovered region"
        }
        DirectorGoalKind::ExpandStarCatalogue => {
            "Discover new stars through observatory prospecting"
        }
        DirectorGoalKind::EnhanceStarCatalogue => "Survey known regional star systems",
        DirectorGoalKind::DiscoverBelts => "Search known regional systems for asteroid belts",
        DirectorGoalKind::ExpandMiningOps => "Expand useful regional mining infrastructure",
        DirectorGoalKind::EventCompletion => "Complete worthwhile active regional events",
        DirectorGoalKind::BlueprintAcquisition => {
            "Learn missing blueprints from known owned-device opportunities"
        }
        DirectorGoalKind::MaintainSystemHubs => {
            "Keep operational System Hubs supplied with reported upkeep resources"
        }
        DirectorGoalKind::ExpandFtlNetwork => "Maintain and extend regional FTL reach",
        DirectorGoalKind::EstablishBeacons => "Maintain beacon coverage at useful known systems",
    }
}

fn all_goal_kinds() -> [DirectorGoalKind; 10] {
    [
        DirectorGoalKind::EstablishRegions,
        DirectorGoalKind::ExpandStarCatalogue,
        DirectorGoalKind::EnhanceStarCatalogue,
        DirectorGoalKind::DiscoverBelts,
        DirectorGoalKind::ExpandMiningOps,
        DirectorGoalKind::EventCompletion,
        DirectorGoalKind::BlueprintAcquisition,
        DirectorGoalKind::MaintainSystemHubs,
        DirectorGoalKind::ExpandFtlNetwork,
        DirectorGoalKind::EstablishBeacons,
    ]
}

/// Stable string form for URL/document identities.
#[must_use]
pub fn goal_kind_key(kind: DirectorGoalKind) -> &'static str {
    match kind {
        DirectorGoalKind::EstablishRegions => "establish_regions",
        DirectorGoalKind::ExpandStarCatalogue => "expand_star_catalogue",
        DirectorGoalKind::EnhanceStarCatalogue => "enhance_star_catalogue",
        DirectorGoalKind::DiscoverBelts => "discover_belts",
        DirectorGoalKind::ExpandMiningOps => "expand_mining_ops",
        DirectorGoalKind::EventCompletion => "event_completion",
        DirectorGoalKind::BlueprintAcquisition => "blueprint_acquisition",
        DirectorGoalKind::MaintainSystemHubs => "maintain_system_hubs",
        DirectorGoalKind::ExpandFtlNetwork => "expand_ftl_network",
        DirectorGoalKind::EstablishBeacons => "establish_beacons",
    }
}

/// Parses the stable goal-kind string used by daemon routes.
pub fn parse_goal_kind(value: &str) -> Option<DirectorGoalKind> {
    all_goal_kinds()
        .into_iter()
        .find(|kind| goal_kind_key(*kind) == value)
}

fn goal_instance_id(kind: DirectorGoalKind, region: Option<&str>) -> String {
    match region {
        Some(region) => format!("{}:{region}", goal_kind_key(kind)),
        None => goal_kind_key(kind).to_owned(),
    }
}

/// Canonicalizes region aliases without constraining future region names.
#[must_use]
pub fn canonical_region(value: &str) -> String {
    match value.trim().to_ascii_lowercase().as_str() {
        "sol" | "sol-region" | "sol_region" | "solregion" | "sol-zone" | "sol_zone" | "solzone" => {
            "solzone".to_owned()
        }
        other => other.to_owned(),
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use replicant_client::{SecretString, StartupPolicy, raw::Url};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;

    async fn test_client_at(server: &MockServer) -> Client {
        Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .authentication_token(SecretString::from("test-token"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("start test client")
    }

    fn test_worker(code: &str, region: &str, location: &str) -> WorkerView {
        WorkerView {
            replicant: Replicant {
                key: replicant_client::ReplicantKey::live(code.into()),
                name: Some(code.to_owned()),
                is_npc: Some(false),
                status: Some(replicant_client::domain::ReplicantStatus::Active),
                location: Some(replicant_client::LocationKey::live(location.into())),
                hosted_device: None,
                travel: None,
                private: None,
                access: replicant_client::domain::AccessScope::Owned,
            },
            region: Some(region.to_owned()),
            role_affinity: None,
            busy_workflow: None,
            racing_vessel: None,
        }
    }

    fn test_hub_device() -> Device {
        Device {
            key: replicant_client::DeviceKey::live("HUB1".into()),
            device_type: Some(DeviceType::SystemHub),
            status: Some(replicant_client::DeviceStatus::Active),
            location: Some(replicant_client::LocationKey::live("SCEPTURUM-7-L4".into())),
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            relationships: replicant_client::DeviceRelationships::default(),
            cargo: Default::default(),
            cargo_capacity: None,
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: replicant_client::domain::OperationalCapacity::new(100.0),
            grace_period_remaining: Some(86_400),
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            access: replicant_client::domain::AccessScope::Owned,
        }
    }

    fn test_hubs(count: usize) -> Vec<Device> {
        (0..count)
            .map(|index| {
                let mut hub = test_hub_device();
                hub.key = replicant_client::DeviceKey::live(format!("HUB-{index}").into());
                hub
            })
            .collect()
    }

    fn positioned_star(name: &str, x: f64, region: Option<&str>) -> Star {
        Star {
            key: replicant_client::domain::StarKey::live(replicant_client::StarId::from(name)),
            name: None,
            spectral_type: None,
            entry_point: None,
            position: Some(GalacticPosition { x, y: 0.0, z: 0.0 }),
            has_hub: None,
            has_ward: None,
            knowledge_observed: false,
            explored: None,
            has_life: None,
            region: region.map(str::to_owned),
        }
    }

    #[test]
    fn legacy_per_device_hub_refresh_cache_requires_one_bulk_sweep() {
        let cache = serde_json::from_value::<HubRefreshCache>(serde_json::json!({
            "refreshed_at_ms": {"HUB-0": 123, "HUB-1": 456}
        }))
        .expect("deserialize legacy cache");

        assert_eq!(cache.refreshed_at_ms, 0);
    }

    fn hub_collection(count: usize) -> ResponseTemplate {
        let devices = (0..count)
            .map(|index| {
                serde_json::json!({
                    "device_code": format!("HUB-{index}"),
                    "device_type": "system_hub",
                    "status": "active",
                    "location": "SCEPTURUM-7-L4"
                })
            })
            .collect::<Vec<_>>();
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "devices": devices,
            "next_cursor": null
        }))
    }

    #[tokio::test]
    async fn system_hubs_refresh_in_one_page_instead_of_one_request_each() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .and(query_param("device_type", "system_hub"))
            .and(query_param("limit", "50"))
            .respond_with(hub_collection(20))
            .expect(1)
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        let repository = WorkflowRepository::open_in_memory().expect("open workflow repository");
        let mut devices = test_hubs(20);

        let errors = refresh_system_hubs(&client, &repository, &mut devices, 1_000_000, false)
            .await
            .expect("refresh hubs");

        assert!(errors.is_empty());
        server.verify().await;
    }

    #[tokio::test]
    async fn system_hub_refresh_cache_suppresses_sweeps_unless_forced() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(hub_collection(1))
            .expect(2)
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        let repository = WorkflowRepository::open_in_memory().expect("open workflow repository");
        let mut devices = test_hubs(1);
        let now = 1_000_000;

        refresh_system_hubs(&client, &repository, &mut devices, now, false)
            .await
            .expect("initial refresh");
        refresh_system_hubs(
            &client,
            &repository,
            &mut devices,
            now + HUB_REFRESH_CACHE_TTL_MS,
            false,
        )
        .await
        .expect("cached refresh");
        refresh_system_hubs(
            &client,
            &repository,
            &mut devices,
            now + HUB_REFRESH_CACHE_TTL_MS,
            true,
        )
        .await
        .expect("forced refresh");

        server.verify().await;
    }

    #[tokio::test]
    async fn bulk_hub_refresh_failures_are_reported_for_each_device() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
            .expect(1)
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        let repository = WorkflowRepository::open_in_memory().expect("open workflow repository");
        let mut devices = test_hubs(3);

        let errors = refresh_system_hubs(&client, &repository, &mut devices, 1_000_000, false)
            .await
            .expect("report bulk failure");

        assert_eq!(errors.len(), 3);
        assert!((0..3).all(|index| errors.contains_key(&format!("HUB-{index}"))));
        server.verify().await;
    }

    #[test]
    fn blueprint_catalogue_cache_uses_thirty_minute_ttl_and_requirement_invalidation() {
        let repository = WorkflowRepository::open_in_memory().expect("open workflow repository");
        let now = 10_000_000_i64;
        let cache = BlueprintCatalogueCache {
            refreshed_at_ms: now - BLUEPRINT_CATALOGUE_CACHE_TTL_MS + 1,
            requirement_signature: BTreeSet::from(["comm_satellite".to_owned()]),
            unlocked_device_types: BTreeSet::from(["ftl_relay".to_owned()]),
        };
        let same_requirements = BTreeSet::from(["comm_satellite".to_owned()]);
        assert!(
            !blueprint_catalogue_refresh_due(
                &repository,
                Some(&cache),
                now,
                false,
                Some(&same_requirements),
            )
            .expect("cache decision")
        );

        let changed_requirements = BTreeSet::from(["signal_booster".to_owned()]);
        assert!(
            blueprint_catalogue_refresh_due(
                &repository,
                Some(&cache),
                now,
                false,
                Some(&changed_requirements),
            )
            .expect("requirement invalidation")
        );

        let expired = BlueprintCatalogueCache {
            refreshed_at_ms: now - BLUEPRINT_CATALOGUE_CACHE_TTL_MS,
            ..cache
        };
        assert!(
            blueprint_catalogue_refresh_due(
                &repository,
                Some(&expired),
                now,
                false,
                Some(&same_requirements),
            )
            .expect("ttl expiration")
        );
    }

    #[test]
    fn sol_aliases_are_canonical() {
        for value in ["SOL", "solregion", "sol-zone", "sol_zone", "solzone"] {
            assert_eq!(canonical_region(value), "solzone");
        }
    }

    #[test]
    fn goal_instance_ids_are_region_scoped() {
        assert_eq!(
            goal_instance_id(DirectorGoalKind::EventCompletion, Some("alpha")),
            "event_completion:alpha"
        );
        assert_eq!(
            goal_instance_id(DirectorGoalKind::EstablishRegions, None),
            "establish_regions"
        );
    }

    #[test]
    fn production_ready_ftl_goal_defaults_enabled() {
        assert!(default_goal_enabled(DirectorGoalKind::ExpandFtlNetwork));
        assert!(default_goal_enabled(DirectorGoalKind::DiscoverBelts));
        assert!(!default_goal_enabled(DirectorGoalKind::EstablishBeacons));
        assert!(default_goal_enabled(DirectorGoalKind::EventCompletion));
        assert!(default_goal_enabled(DirectorGoalKind::BlueprintAcquisition));
        assert!(default_goal_enabled(DirectorGoalKind::MaintainSystemHubs));
    }

    #[test]
    fn staged_regional_bootstrap_workers_mark_partial_foothold() {
        let mut regions = BTreeMap::from([(
            "beta".to_owned(),
            RegionView {
                region: "beta".to_owned(),
                status: DirectorRegionStatus::Discovered,
                hub_system: None,
                hub_location: None,
                known_systems: BTreeSet::from(["BETA-STAR".to_owned()]),
            },
        )]);
        let workers = vec![
            test_worker("BETA-1", "beta", "BETA-STAR-2"),
            test_worker("BETA-2", "beta", "BETA-STAR-3"),
        ];
        let location_systems = BTreeMap::from([
            ("BETA-STAR-2".to_owned(), "BETA-STAR".to_owned()),
            ("BETA-STAR-3".to_owned(), "BETA-STAR".to_owned()),
        ]);
        let system_regions = BTreeMap::from([("BETA-STAR".to_owned(), "beta".to_owned())]);

        mark_partial_region_footholds(&mut regions, &workers, &location_systems, &system_regions);

        assert_eq!(regions["beta"].status, DirectorRegionStatus::Establishing);
    }

    #[test]
    fn regional_autofactory_marks_a_stable_manufacturing_foothold() {
        let mut regions = BTreeMap::from([(
            "delta".to_owned(),
            RegionView {
                region: "delta".to_owned(),
                status: DirectorRegionStatus::Discovered,
                hub_system: None,
                hub_location: None,
                known_systems: BTreeSet::from(["PHASYRIS".to_owned()]),
            },
        )]);
        let mut factory = test_hub_device();
        factory.device_type = Some(DeviceType::Autofactory);
        factory.location = Some(replicant_client::LocationKey::live(
            "PHASYRIS-BELT-1".into(),
        ));
        mark_manufacturing_footholds(
            &mut regions,
            &[factory],
            &BTreeMap::from([("PHASYRIS-BELT-1".to_owned(), "PHASYRIS".to_owned())]),
            &BTreeMap::from([("PHASYRIS".to_owned(), "delta".to_owned())]),
        );

        assert_eq!(regions["delta"].status, DirectorRegionStatus::Establishing);
        assert_eq!(regions["delta"].hub_system.as_deref(), Some("PHASYRIS"));
        assert_eq!(
            regions["delta"].hub_location.as_deref(),
            Some("PHASYRIS-BELT-1")
        );
    }

    #[test]
    fn ftl_worker_requires_free_stow_capacity() {
        let mut full_worker = test_worker("CHAT-1", "alpha", "SCEPTURUM-BELT-1");
        full_worker.racing_vessel = Some("VESSEL-FULL".to_owned());
        let mut free_worker = test_worker("CHAT-2", "alpha", "SCEPTURUM-BELT-1");
        free_worker.racing_vessel = Some("VESSEL-FREE".to_owned());

        let mut full_vessel = test_hub_device();
        full_vessel.key = replicant_client::DeviceKey::live("VESSEL-FULL".into());
        full_vessel.stow_capacity = Some(4);
        full_vessel.stow_used = Some(4);
        let mut free_vessel = test_hub_device();
        free_vessel.key = replicant_client::DeviceKey::live("VESSEL-FREE".into());
        free_vessel.stow_capacity = Some(4);
        free_vessel.stow_used = Some(3);

        assert_eq!(
            select_idle_ftl_worker(
                &[full_worker, free_worker],
                "alpha",
                &BTreeSet::new(),
                &[full_vessel, free_vessel],
            ),
            Some("CHAT-2".to_owned())
        );
    }

    #[test]
    fn queued_explicit_replicant_assignment_is_busy_before_executor_claims() {
        let repository = WorkflowRepository::open_in_memory().expect("open workflow repository");
        let workflow = repository
            .create(new_scan_tour_workflow(ScanTourIntent {
                center: "SCEPTURUM".to_owned(),
                radius_ly: 30.0,
                system_limit: 10,
                target_systems: Some(vec!["TARGET".to_owned()]),
                replicant: Some("CHAT-1".to_owned()),
                vessel: Some("VESSEL-1".to_owned()),
                include_explored: false,
            }))
            .expect("create queued survey");

        let workflows = repository.list().expect("list workflows");
        let busy = busy_replicants(&repository, &workflows).expect("resolve busy replicants");

        assert_eq!(busy.get("CHAT-1"), Some(&workflow.id));
        assert!(repository.claims(workflow.id).expect("claims").is_empty());
    }

    #[test]
    fn belt_density_priority_uses_managed_location_payload() {
        let location = Location {
            key: replicant_client::LocationKey::live("BETA-STAR-BELT-1".into()),
            location_type: None,
            scanned: Some(true),
            system_scanned: Some(true),
            system_tags: Vec::new(),
            system: Some("BETA-STAR".to_owned()),
            parent: None,
            survey_progress: replicant_client::domain::LocationSurveyProgress::default(),
            environment: replicant_client::domain::LocationEnvironment::default(),
            unknown: BTreeMap::from([("belt".to_owned(), serde_json::json!({"density": "dense"}))]),
        };
        let moderate = Location {
            key: replicant_client::LocationKey::live("GAMMA-STAR-BELT-1".into()),
            system: Some("GAMMA-STAR".to_owned()),
            unknown: BTreeMap::from([(
                "asteroid_belt".to_owned(),
                serde_json::json!({"belts": [{"density": "moderate"}]}),
            )]),
            ..location.clone()
        };

        assert_eq!(
            belt_density_priorities(&[location, moderate], &BTreeMap::new()),
            BTreeMap::from([("BETA-STAR".to_owned(), 2), ("GAMMA-STAR".to_owned(), 1)])
        );
        assert!(ftl_priority_score(true, 0) > ftl_priority_score(false, 2));
        assert!(ftl_priority_score(false, 2) > ftl_priority_score(false, 1));
        assert!(ftl_priority_score(false, 1) > ftl_priority_score(false, 0));
        assert_eq!(
            ftl_priority_suffix(true, 2),
            " (active events + dense belt)"
        );
        assert_eq!(ftl_priority_suffix(false, 1), " (moderate belt)");
    }

    #[test]
    fn mining_waits_for_relay_connected_belt_systems() {
        let belts = BTreeSet::from(["CONNECTED".to_owned(), "UNREACHABLE".to_owned()]);
        let relays = BTreeSet::from(["CONNECTED".to_owned(), "OTHER".to_owned()]);

        assert_eq!(
            relay_connected_mining_targets(&belts, &relays),
            vec!["CONNECTED".to_owned()]
        );
    }

    #[test]
    fn blueprint_source_accepts_idle_stowed_and_out_of_range_owned_copies() {
        let mut idle = test_hub_device();
        idle.key = replicant_client::DeviceKey::live("DEVICE-1".into());
        idle.device_type = Some(DeviceType::from("service_bot"));
        idle.status = Some(replicant_client::DeviceStatus::Idle);
        idle.location = Some(replicant_client::LocationKey::live(
            "SCEPTURUM-BELT-1".into(),
        ));

        let mut vessel = test_hub_device();
        vessel.key = replicant_client::DeviceKey::live("VESSEL-1".into());
        vessel.device_type = Some(DeviceType::from("heaven_vessel"));
        vessel.location = Some(replicant_client::LocationKey::live("SCEPTURUM-7-L4".into()));

        let mut stowed = idle.clone();
        stowed.key = replicant_client::DeviceKey::live("SLING-1".into());
        stowed.device_type = Some(DeviceType::FtlSlingshot);
        stowed.status = Some(replicant_client::DeviceStatus::from("stowed"));
        stowed.location = None;
        stowed.relationships.stowed_in = Some(vessel.key.clone());
        stowed.available_commands = vec![replicant_client::DeviceCommand::Deploy];

        let mut remote = idle.clone();
        remote.key = replicant_client::DeviceKey::live("WARD-1".into());
        remote.device_type = Some(DeviceType::SystemWard);
        remote.status = Some(replicant_client::DeviceStatus::from("out_of_range"));
        remote.location = Some(replicant_client::LocationKey::live("RHYVENAI".into()));
        remote.available_commands = vec![replicant_client::DeviceCommand::from("decommission")];

        let devices = vec![idle.clone(), vessel, stowed.clone(), remote.clone()];
        assert!(blueprint_source_is_candidate(
            &idle,
            "service_bot",
            &devices
        ));
        assert!(blueprint_source_is_candidate(
            &stowed,
            "ftl_slingshot",
            &devices
        ));
        assert_eq!(
            blueprint_source_location(&stowed, &devices),
            Some("SCEPTURUM-7-L4")
        );
        assert!(blueprint_source_is_candidate(
            &remote,
            "system_ward",
            &devices
        ));

        let mut hosted = idle;
        hosted.relationships.hosting_replicant =
            Some(replicant_client::ReplicantKey::live("CHAT-1".into()));
        assert!(!blueprint_source_is_candidate(
            &hosted,
            "service_bot",
            &devices
        ));
    }

    #[test]
    fn blueprint_acquisition_excludes_occupied_replicant_matrices() {
        assert!(!blueprint_acquisition_target(&DeviceType::from(
            "replicant_matrix"
        )));
        assert!(blueprint_acquisition_target(
            &DeviceType::EmptyReplicantMatrix
        ));
    }

    #[test]
    fn blueprint_shop_dependency_cycles_are_detected() {
        let snapshot = BlueprintShopSnapshot {
            opportunities: vec![
                BlueprintShopOpportunity {
                    device_type: "device_a".to_owned(),
                    controller_code: "SHOP-A".to_owned(),
                    trade_code: "TRADE-A".to_owned(),
                    current_stock: 1,
                    criteria: TradeBundle {
                        devices: BTreeMap::from([("device_b".to_owned(), 1)]),
                        ..TradeBundle::default()
                    },
                    shop_location: "SOL-4".to_owned(),
                    shop_system: "SOL".to_owned(),
                    shop_name: None,
                    last_seen_at_ms: 1,
                },
                BlueprintShopOpportunity {
                    device_type: "device_b".to_owned(),
                    controller_code: "SHOP-B".to_owned(),
                    trade_code: "TRADE-B".to_owned(),
                    current_stock: 1,
                    criteria: TradeBundle {
                        devices: BTreeMap::from([("device_a".to_owned(), 1)]),
                        ..TradeBundle::default()
                    },
                    shop_location: "SOL-5".to_owned(),
                    shop_system: "SOL".to_owned(),
                    shop_name: None,
                    last_seen_at_ms: 1,
                },
            ],
            ..BlueprintShopSnapshot::default()
        };

        assert!(blueprint_shop_dependency_cycle(
            &snapshot, "device_a", "device_b"
        ));
        assert!(blueprint_shop_dependency_cycle(
            &snapshot, "device_b", "device_a"
        ));
        assert!(!blueprint_shop_dependency_cycle(
            &snapshot, "device_a", "device_c"
        ));
    }

    #[test]
    fn shop_opportunities_require_matching_stocked_device_rewards() {
        let snapshot = BlueprintShopSnapshot {
            opportunities: vec![BlueprintShopOpportunity {
                device_type: "service_bot".to_owned(),
                controller_code: "SHOP-1".to_owned(),
                trade_code: "TRADE-1".to_owned(),
                current_stock: 2,
                criteria: TradeBundle::default(),
                shop_location: "SOL-4".to_owned(),
                shop_system: "SOL".to_owned(),
                shop_name: None,
                last_seen_at_ms: 1,
            }],
            ..BlueprintShopSnapshot::default()
        };

        assert_eq!(
            shop_opportunities_for(&snapshot, &DeviceType::from("service_bot")).count(),
            1
        );
        assert_eq!(
            shop_opportunities_for(&snapshot, &DeviceType::from("system_hub")).count(),
            0
        );
    }

    #[test]
    fn hub_upkeep_parser_uses_only_explicit_deficits() {
        let mut hub = test_hub_device();
        hub.upkeep_requirements = vec![
            BTreeMap::from([
                ("resource".to_owned(), Value::from("structural")),
                ("required".to_owned(), Value::from(400)),
                ("missing".to_owned(), Value::from(120)),
            ]),
            BTreeMap::from([
                ("resource_type".to_owned(), Value::from("carbon")),
                ("remaining".to_owned(), Value::from(80)),
            ]),
        ];

        let deficits = exact_upkeep_deficits(&hub).expect("parse explicit deficits");
        assert_eq!(deficits.get("structural"), Some(&120));
        assert_eq!(deficits.get("carbon"), Some(&80));

        hub.upkeep_requirements = vec![BTreeMap::from([
            ("resource".to_owned(), Value::from("structural")),
            ("required".to_owned(), Value::from(400)),
        ])];
        let error = exact_upkeep_deficits(&hub).expect_err("total alone is ambiguous");
        assert!(error.contains("refusing to infer"));
    }

    #[test]
    fn hub_supply_prefers_regional_consolidation_home() {
        let region = RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("SCEPTURUM".to_owned()),
            hub_location: Some("SCEPTURUM-BELT-1".to_owned()),
            known_systems: BTreeSet::new(),
        };
        let hub = HubMaintenanceView {
            system: "SCEPTURUM".to_owned(),
            location: "SCEPTURUM-7-L4".to_owned(),
            deficits: BTreeMap::from([("carbon".to_owned(), 80), ("structural".to_owned(), 400)]),
            grace_period_remaining: Some(86_400),
            degraded: false,
        };
        let stocks = vec![
            StockLocation {
                location: "SCEPTURUM-4".to_owned(),
                system: "SCEPTURUM".to_owned(),
                region: Some("alpha".to_owned()),
                is_belt: false,
                resources: BTreeMap::from([
                    ("carbon".to_owned(), 500),
                    ("structural".to_owned(), 500),
                ]),
            },
            StockLocation {
                location: "SCEPTURUM-BELT-1".to_owned(),
                system: "SCEPTURUM".to_owned(),
                region: Some("alpha".to_owned()),
                is_belt: true,
                resources: BTreeMap::from([
                    ("carbon".to_owned(), 80),
                    ("structural".to_owned(), 400),
                ]),
            },
        ];

        let source = choose_hub_supply_source(&hub, &region, &stocks).expect("source");
        assert_eq!(source.origin, "SCEPTURUM-BELT-1");
    }

    #[test]
    fn hub_supply_prefers_regional_home_over_remote_local_belt() {
        let region = RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("SCEPTURUM".to_owned()),
            hub_location: Some("SCEPTURUM-BELT-1".to_owned()),
            known_systems: BTreeSet::new(),
        };
        let hub = HubMaintenanceView {
            system: "ALPHA-EDGE".to_owned(),
            location: "ALPHA-EDGE-3-L4".to_owned(),
            deficits: BTreeMap::from([("structural".to_owned(), 400)]),
            grace_period_remaining: Some(86_400),
            degraded: false,
        };
        let stocks = vec![
            StockLocation {
                location: "ALPHA-EDGE-BELT-1".to_owned(),
                system: "ALPHA-EDGE".to_owned(),
                region: Some("alpha".to_owned()),
                is_belt: true,
                resources: BTreeMap::from([("structural".to_owned(), 1_000)]),
            },
            StockLocation {
                location: "SCEPTURUM-BELT-1".to_owned(),
                system: "SCEPTURUM".to_owned(),
                region: Some("alpha".to_owned()),
                is_belt: true,
                resources: BTreeMap::from([("structural".to_owned(), 1_000)]),
            },
        ];

        let source = choose_hub_supply_source(&hub, &region, &stocks).expect("source");
        assert_eq!(source.origin, "SCEPTURUM-BELT-1");
    }

    #[test]
    fn hub_supply_uses_cross_region_stock_only_when_at_risk() {
        let region = RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("SCEPTURUM".to_owned()),
            hub_location: Some("SCEPTURUM-BELT-1".to_owned()),
            known_systems: BTreeSet::new(),
        };
        let mut hub = HubMaintenanceView {
            system: "ALPHA-EDGE".to_owned(),
            location: "ALPHA-EDGE-3-L4".to_owned(),
            deficits: BTreeMap::from([("structural".to_owned(), 400)]),
            grace_period_remaining: Some(86_400),
            degraded: false,
        };
        let stocks = vec![StockLocation {
            location: "THYFFAWFF-BELT-1".to_owned(),
            system: "THYFFAWFF".to_owned(),
            region: Some("beta".to_owned()),
            is_belt: true,
            resources: BTreeMap::from([("structural".to_owned(), 1_000)]),
        }];

        assert!(choose_hub_supply_source(&hub, &region, &stocks).is_none());
        hub.degraded = true;
        let source = choose_hub_supply_source(&hub, &region, &stocks).expect("emergency source");
        assert_eq!(source.origin, "THYFFAWFF");
    }

    #[test]
    fn hub_supply_workflow_is_reused_by_deterministic_purpose() {
        let path = std::env::temp_dir().join(format!(
            "replicant-director-hub-supply-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let repository = WorkflowRepository::open(&path).expect("open workflow repository");
        let created = repository
            .create(new_logistics_manifest_workflow(LogisticsManifestIntent {
                origin: "SCEPTURUM-BELT-1".to_owned(),
                destination: "SCEPTURUM-7-L4".to_owned(),
                resources: BTreeMap::from([("structural".to_owned(), 400)]),
                devices: Vec::new(),
                device_codes: Vec::new(),
                device_tags: Vec::new(),
                return_transports: true,
                allow_transport_staging: false,
                region: Some("alpha".to_owned()),
                purpose: hub_supply_purpose("alpha", "HUB1"),
            }))
            .expect("create manifest");
        let workflows = repository.list().expect("list workflows");

        assert_eq!(
            active_hub_supply_workflow(&workflows, "alpha", "HUB1", "SCEPTURUM-7-L4")
                .expect("inspect manifests"),
            Some(created.id)
        );

        drop(repository);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn catalogue_partitioning_is_disjoint_and_balanced() {
        let systems = (1..=7)
            .map(|index| format!("SYS-{index}"))
            .collect::<Vec<_>>();
        let shards = partition_systems(&systems, 3);
        assert_eq!(shards.len(), 3);
        assert_eq!(
            shards.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![3, 2, 2]
        );
        let assigned = shards.into_iter().flatten().collect::<BTreeSet<_>>();
        assert_eq!(assigned, systems.into_iter().collect());
    }

    #[test]
    fn catalogue_partitioning_never_creates_empty_workers() {
        let systems = vec!["ALPHA".to_owned(), "BETA".to_owned()];
        assert_eq!(partition_systems(&systems, 8).len(), 2);
        assert!(partition_systems(&systems, 0).is_empty());
    }

    #[test]
    fn gateway_distance_uses_three_dimensional_ly_distance() {
        let left = GalacticPosition {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        };
        let right = GalacticPosition {
            x: 3.0,
            y: 4.0,
            z: 12.0,
        };
        assert!((galactic_distance(left, right) - 13.0).abs() < f64::EPSILON);
    }

    #[test]
    fn device_region_map_extends_formal_bounds_by_fifteen_ly() {
        let regions = expanded_system_region_map(&[
            positioned_star("ALPHA-EDGE", 0.0, Some("Alpha")),
            positioned_star("SCEPTURUM", 15.0, None),
            positioned_star("TOO-FAR", 15.01, None),
        ]);

        assert_eq!(regions.get("ALPHA-EDGE").map(String::as_str), Some("alpha"));
        assert_eq!(regions.get("SCEPTURUM").map(String::as_str), Some("alpha"));
        assert!(!regions.contains_key("TOO-FAR"));
        let formal = expanded_system_region_map(&[
            positioned_star("ALPHA-EDGE", 0.0, Some("Alpha")),
            positioned_star("FORMAL-BETA", 1.0, Some("Beta")),
        ]);
        assert_eq!(formal.get("FORMAL-BETA").map(String::as_str), Some("beta"));
    }

    #[test]
    fn operational_region_requires_an_unambiguous_gateway_system() {
        let alpha = RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("SCEPTURUM".to_owned()),
            hub_location: Some("SCEPTURUM-BELT-1".to_owned()),
            known_systems: BTreeSet::new(),
        };
        let regions = BTreeMap::from([("alpha".to_owned(), alpha.clone())]);
        assert_eq!(
            operational_region_for_system("SCEPTURUM", &regions).as_deref(),
            Some("alpha")
        );

        let mut beta = alpha;
        beta.region = "beta".to_owned();
        let ambiguous = BTreeMap::from([
            ("alpha".to_owned(), regions["alpha"].clone()),
            ("beta".to_owned(), beta),
        ]);
        assert_eq!(operational_region_for_system("SCEPTURUM", &ambiguous), None);
    }

    #[test]
    fn cached_snapshot_returns_warming_projection_before_first_reconcile() {
        let path = std::env::temp_dir().join(format!(
            "replicant-director-cache-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let repository = WorkflowRepository::open(&path).expect("open workflow repository");

        let snapshot = cached_director_snapshot(&repository, 42).expect("read warming snapshot");

        assert_eq!(snapshot.metadata.revision, 42);
        assert_eq!(snapshot.mode, DirectorMode::Advisory);
        assert!(snapshot.regions.is_empty());
        assert!(snapshot.replicants.is_empty());
        assert_eq!(snapshot.goals.len(), all_goal_kinds().len());
        assert!(snapshot.workforce.scale_reason.is_some());

        drop(repository);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cached_snapshot_overlays_durable_operator_controls() {
        let path = std::env::temp_dir().join(format!(
            "replicant-director-cache-controls-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let repository = WorkflowRepository::open(&path).expect("open workflow repository");
        let mut stored = cached_director_snapshot(&repository, 1).expect("build warming snapshot");
        stored.replicants.push(DirectorReplicantAssignment {
            code: "CHAT-1".to_owned(),
            name: Some("Chats-1".to_owned()),
            region: None,
            busy: false,
            workflow_id: None,
            role_affinity: None,
        });
        stored.regions.push(DirectorRegionSummary {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("SCEPTURUM".to_owned()),
            hub_location: Some("SCEPTURUM-BELT-1".to_owned()),
            replicants: Vec::new(),
            known_systems: 4,
        });
        repository
            .put_document(SNAPSHOT_NS, SNAPSHOT_KEY, &stored)
            .expect("persist cached projection");

        set_director_mode(&repository, DirectorMode::Automatic).expect("set Director mode");
        set_goal_enabled(&repository, DirectorGoalKind::ExpandFtlNetwork, true)
            .expect("enable FTL goal");
        assign_replicant_region(&repository, "CHAT-1", Some("Alpha"), Some("catalogue"))
            .expect("assign regional worker");

        let cached = cached_director_snapshot(&repository, 2).expect("read updated cache");

        assert_eq!(cached.mode, DirectorMode::Automatic);
        assert!(
            cached
                .goals
                .iter()
                .find(|goal| goal.kind == DirectorGoalKind::ExpandFtlNetwork)
                .expect("FTL goal")
                .enabled
        );
        assert_eq!(cached.replicants[0].region.as_deref(), Some("alpha"));
        assert_eq!(
            cached.replicants[0].role_affinity.as_deref(),
            Some("catalogue")
        );
        assert_eq!(cached.regions[0].replicants, vec!["CHAT-1".to_owned()]);

        drop(repository);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cached_snapshot_prefers_last_successful_projection() {
        let path = std::env::temp_dir().join(format!(
            "replicant-director-cache-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let repository = WorkflowRepository::open(&path).expect("open workflow repository");
        let mut expected =
            cached_director_snapshot(&repository, 1).expect("build warming snapshot");
        expected.metadata.revision = 77;
        expected.workforce.total = 9;
        repository
            .put_document(SNAPSHOT_NS, SNAPSHOT_KEY, &expected)
            .expect("persist Director snapshot");

        let cached = cached_director_snapshot(&repository, 99).expect("read cached snapshot");

        assert_eq!(cached, expected);

        drop(repository);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn no_scale_down_goal_or_operation_exists() {
        assert!(
            all_goal_kinds()
                .into_iter()
                .all(|kind| !goal_kind_key(kind).contains("delete"))
        );
        assert!(!goal_kind_key(DirectorGoalKind::EstablishRegions).contains("retire"));
    }
}
