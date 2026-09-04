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
    domain::{GalacticPosition, Inventory, InventoryOwner, LocationType},
    managed::ReadinessComponent,
    raw::RequestPriority,
};
use replicant_protocol::{
    DirectorGoalKind, DirectorGoalStatus, DirectorGoalSummary, DirectorMiningPolicySummary,
    DirectorMode, DirectorRegionStatus, DirectorRegionSummary, DirectorRegionalWorkforceSummary,
    DirectorReplicantAssignment, DirectorSnapshot, DirectorUrgencyFact, DirectorWorkerState,
    DirectorWorkforceSummary, SnapshotMetadata, WorkflowId as ProtocolWorkflowId,
};
use replicant_transport::ResourceMap;
use replicant_workflow::{
    RepositoryError, ResourceKey, WorkflowFailureDisposition, WorkflowId, WorkflowInstance,
    WorkflowPlacementIntentSnapshot, WorkflowPlacementIntentSubject, WorkflowPlacementProvenance,
    WorkflowPlacementResolution, WorkflowRegistry, WorkflowRepository,
    WorkflowServiceIntentSnapshot, WorkflowServiceIntentState, WorkflowStatus,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{
    ApplicationError,
    asteroid_diversion::{
        AsteroidDiversionIntent, AsteroidHistorySnapshot, AsteroidLifecycle, AsteroidOccurrence,
        asteroid_diversion_workflow_matches, asteroid_history_snapshot,
        new_asteroid_diversion_workflow,
    },
    automation::{
        BeltSearchCampaignIntent, BlueprintAcquireIntent, BlueprintShopPurchaseIntent,
        EventCampaignIntent, ExplorationIntent, LogisticsManifestIntent,
        LogisticsWorkflowCheckpoint, MiningCampaignIntent, ObservatoryIntent,
        PlacementRecoveryMetadata, RegionEstablishIntent, ReplicantProvisionIntent,
        SalvageRecoveryHistorySnapshot, SalvageRecoveryIntent, ScanTourIntent,
        blueprint_acquire_workflow_kind, blueprint_source_is_candidate, blueprint_source_location,
        completed_salvage_sites, exploration_workflow_kind, new_belt_search_campaign_workflow,
        new_blueprint_acquire_workflow, new_event_campaign_workflow, new_exploration_workflow,
        new_logistics_manifest_workflow, new_mining_campaign_workflow, new_observatory_workflow,
        new_region_establish_workflow, new_replicant_provision_workflow,
        new_salvage_recovery_workflow, new_scan_tour_workflow, placement_recovery_authorization,
        placement_recovery_authorization_matches, placement_recovery_metadata_matches_snapshot,
        read_placement_recovery_authorization, recoverable_salvage_sites,
        replicant_provision_workflow_kind, revoke_placement_recovery_authorization,
        salvage_recovery_history_snapshot, salvage_recovery_workflow_kind,
        salvage_recovery_workflow_matches, write_placement_recovery_authorization,
    },
    canonical_region,
    device_placement::{DevicePlacementClass, DevicePlacementContext, classify_device_placement},
    director_requirements::{
        DirectorRequirement, DirectorRequirementGraph, load_requirement_summaries,
    },
    event::active_events,
    trade::{TradeBundle, TraderSummary, shop_trades, trader_directory},
    worker_state::{WorkerState, classify_regional_worker},
};

const SETTINGS_NS: &str = "director.settings";
const SETTINGS_KEY: &str = "singleton";
const GOAL_CONTROL_NS: &str = "director.goal_control";
const MINING_POLICY_NS: &str = "director.mining_policy";
const GOAL_RUNTIME_NS: &str = "director.goal_runtime";
const REPLICANT_NS: &str = "director.replicant";
const WORKFORCE_NS: &str = "director.workforce";
const SNAPSHOT_NS: &str = "director.snapshot";
const BLUEPRINT_SHOP_NS: &str = "director.blueprint_shop_opportunity";
const BLUEPRINT_SHOP_CACHE_NS: &str = "director.blueprint_shop_snapshot";
const SALVAGE_RECOVERY_CACHE_NS: &str = "director.salvage_recovery_snapshot";
const SALVAGE_RECOVERY_CACHE_KEY: &str = "latest";
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
const REGION_BOOTSTRAP_TARGET: usize = 2;
const MINING_WARD_SITES_PER_REGION: usize = 4;
const MINING_EXPANSION_RADIUS_LY: f64 = 30.0;
const MINING_BATCH_SIZE: usize = 4;
const CATALOGUE_SYSTEMS_PER_WORKER: usize = 20;
const REGIONAL_AUTOMATION_RADIUS_LY: f64 = 30.0;
const MAX_PARALLEL_CATALOGUE_WORKERS: usize = 4;
// A system hub has 15 LY operational reach. An owned hub just outside a named
// region can therefore serve as that region's gateway capital when it can
// directly reach at least one known star inside the region.
pub(crate) const REGION_GATEWAY_HUB_RANGE_LY: f64 = 15.0;
const EVENT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(12);
const BLUEPRINT_SHOP_TIMEOUT: Duration = Duration::from_secs(10);
const SALVAGE_RECOVERY_CACHE_TTL_MS: i64 = 2 * 60 * 1000;
const SALVAGE_RECOVERY_STALE_FALLBACK_MS: i64 = 10 * 60 * 1000;
const SALVAGE_RECOVERY_TIMEOUT: Duration = Duration::from_secs(12);
const BLUEPRINT_SHOP_CONCURRENCY: usize = 6;
const BLUEPRINT_SHOP_CACHE_TTL_MS: i64 = 10 * 60 * 1000;
const BLUEPRINT_SHOP_PARTIAL_CACHE_TTL_MS: i64 = 2 * 60 * 1000;
const BLUEPRINT_CATALOGUE_CACHE_TTL_MS: i64 = 30 * 60 * 1000;
const HUB_REFRESH_CACHE_TTL_MS: i64 = 5 * 60 * 1000;
const ACTIVE_EVENT_CACHE_TTL_MS: i64 = 2 * 60 * 1000;
const ACTIVE_EVENT_STALE_FALLBACK_MS: i64 = 10 * 60 * 1000;

const PRIORITY_REGION_ESTABLISHMENT: u32 = 900;
const PRIORITY_ASTEROID_DIVERSION: u32 = 800;
const PRIORITY_EVENT_COMPLETION: u32 = 700;
const PRIORITY_FTL_EXPANSION: u32 = 650;
const PRIORITY_CATALOGUE: u32 = 500;
const PRIORITY_CATALOGUE_BLUEPRINT: u32 = 400;
const _: () = assert!(
    PRIORITY_ASTEROID_DIVERSION < PRIORITY_REGION_ESTABLISHMENT
        && PRIORITY_ASTEROID_DIVERSION > PRIORITY_EVENT_COMPLETION
);

/// Durable Automation Director settings.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
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

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
struct MiningExpansionPolicy {
    #[serde(default = "default_true")]
    expand_moderate: bool,
    #[serde(default = "default_true")]
    expand_sparse: bool,
}

impl Default for MiningExpansionPolicy {
    fn default() -> Self {
        Self {
            expand_moderate: true,
            expand_sparse: true,
        }
    }
}

const fn default_true() -> bool {
    true
}

#[derive(Default)]
struct GoalControls {
    global: BTreeMap<DirectorGoalKind, bool>,
    regional: BTreeMap<DirectorGoalKind, BTreeMap<String, bool>>,
}

impl GoalControls {
    fn enabled(&self, kind: DirectorGoalKind, region: Option<&str>) -> bool {
        region
            .and_then(|region| {
                self.regional
                    .get(&kind)
                    .and_then(|controls| controls.get(region))
            })
            .copied()
            .or_else(|| self.global.get(&kind).copied())
            .unwrap_or_else(|| default_goal_enabled(kind))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct GoalRuntime {
    #[serde(default)]
    active_workflows: Vec<WorkflowId>,
    last_launch_at_ms: Option<i64>,
    #[serde(default)]
    launch_records: Vec<GoalLaunchRecord>,
    #[serde(default)]
    prospect_exhausted_signature: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
enum GoalWorkIdentity {
    EventCampaign {
        region: String,
        events: BTreeSet<String>,
    },
    AsteroidDiversion {
        region: String,
        occurrences: BTreeSet<String>,
    },
    Exploration {
        target: String,
    },
    SalvageRecovery {
        region: String,
        sites: BTreeSet<String>,
    },
    StrandedDeviceRecovery {
        region: String,
        device_code: String,
        origin: String,
        destination: String,
        failed_provenance: BTreeMap<String, Vec<WorkflowPlacementProvenance>>,
        release_tags: Vec<String>,
        #[serde(default)]
        placement_resolutions: Vec<WorkflowPlacementResolution>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
struct GoalLaunchRecord {
    workflow_id: WorkflowId,
    identity: GoalWorkIdentity,
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
struct StrandedRecoveryCandidate {
    device_code: String,
    origin: String,
    destination: String,
    metadata: PlacementRecoveryMetadata,
}

fn recovery_metadata_identity(
    region: &str,
    candidate: &StrandedRecoveryCandidate,
) -> GoalWorkIdentity {
    GoalWorkIdentity::StrandedDeviceRecovery {
        region: region.to_owned(),
        device_code: candidate.device_code.clone(),
        origin: candidate.origin.clone(),
        destination: candidate.destination.clone(),
        failed_provenance: candidate.metadata.failed_provenance.clone(),
        release_tags: candidate
            .metadata
            .release_device_tags
            .get(&candidate.device_code)
            .cloned()
            .unwrap_or_default(),
        placement_resolutions: candidate.metadata.placement_resolutions.clone(),
    }
}

#[derive(Clone, Debug)]
struct WorkerView {
    replicant: Replicant,
    region: Option<String>,
    role_affinity: Option<String>,
    busy_workflow: Option<WorkflowId>,
    racing_vessel: Option<String>,
    physical_location: Option<String>,
    state: WorkerState,
}

#[derive(Clone, Debug)]
struct ManufacturingHomeSelection {
    location: String,
    reason: String,
    local: bool,
}

#[derive(Clone, Debug, Default)]
struct WorkforceReconciliation {
    recommendations: Vec<String>,
    regions: Vec<DirectorRegionalWorkforceSummary>,
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
struct SalvageRecoveryCache {
    refreshed_at_ms: i64,
    snapshot: SalvageRecoveryHistorySnapshot,
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
    controls: &'a GoalControls,
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
    placement_reserved_devices: &'a BTreeSet<String>,
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

/// Enables or disables one global or regional standing goal instance.
pub fn set_goal_enabled(
    repository: &WorkflowRepository,
    kind: DirectorGoalKind,
    region: Option<&str>,
    enabled: bool,
) -> Result<(), ApplicationError> {
    let region = region.map(canonical_region);
    repository.put_document(
        GOAL_CONTROL_NS,
        &goal_instance_id(kind, region.as_deref()),
        &GoalControl { enabled },
    )?;
    Ok(())
}

/// Updates which non-dense belt classes may receive new mining deployments in
/// one region. Dense belts are always eligible. Disabling a density stops new
/// expansion into that class; an existing lower-density site remains managed.
/// System Ward priority is reconciled independently from the mining footprint.
pub fn set_mining_expansion_policy(
    repository: &WorkflowRepository,
    region: &str,
    expand_moderate: bool,
    expand_sparse: bool,
) -> Result<(), ApplicationError> {
    let region = canonical_region(region);
    repository.put_document(
        MINING_POLICY_NS,
        &region,
        &MiningExpansionPolicy {
            expand_moderate,
            expand_sparse,
        },
    )?;
    Ok(())
}

fn mining_expansion_policy(
    repository: &WorkflowRepository,
    region: &str,
) -> Result<MiningExpansionPolicy, ApplicationError> {
    repository
        .read_document(MINING_POLICY_NS, &canonical_region(region))?
        .map(|(value, _)| serde_json::from_value(value))
        .transpose()
        .map(|value| value.unwrap_or_default())
        .map_err(Into::into)
}

fn mining_policy_summaries<'a>(
    repository: &WorkflowRepository,
    regions: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<DirectorMiningPolicySummary>, ApplicationError> {
    regions
        .into_iter()
        .map(|region| {
            let policy = mining_expansion_policy(repository, region)?;
            Ok(DirectorMiningPolicySummary {
                region: canonical_region(region),
                expand_moderate: policy.expand_moderate,
                expand_sparse: policy.expand_sparse,
            })
        })
        .collect()
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
        let controls = load_goal_controls(repository, std::iter::empty::<&str>())?;
        let goals = all_goal_kinds()
            .into_iter()
            .filter(|kind| !goal_is_regional(*kind))
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
                mining_policies: Vec::new(),
                replicants: Vec::new(),
                requirements: load_requirement_summaries(repository)?,
                workforce: DirectorWorkforceSummary {
                    total: 0,
                    busy: 0,
                    operational: 0,
                    in_transit: 0,
                    unavailable: 0,
                    idle: 0,
                    idle_ratio: 1.0,
                    pending_worker_demand: 0,
                    scale_up_recommended: false,
                    scale_reason: Some(
                        "Automation Director is warming up; the last successful projection is not available yet"
                            .to_owned(),
                    ),
                    regions: Vec::new(),
                },
                urgency: Vec::new(),
            }
    };

    apply_durable_snapshot_overrides(repository, &mut snapshot)?;
    snapshot.metadata = SnapshotMetadata {
        revision,
        generated_at_ms: now_millis(),
    };
    Ok(snapshot)
}

fn apply_durable_snapshot_overrides(
    repository: &WorkflowRepository,
    snapshot: &mut DirectorSnapshot,
) -> Result<(), ApplicationError> {
    snapshot.mode = director_settings(repository)?.mode;

    let controls = load_goal_controls(
        repository,
        snapshot.regions.iter().map(|region| region.region.as_str()),
    )?;
    for goal in &mut snapshot.goals {
        goal.enabled = goal_enabled(&controls, goal.kind, goal.region.as_deref());
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
    snapshot.mining_policies = mining_policy_summaries(
        repository,
        snapshot.regions.iter().map(|region| region.region.as_str()),
    )?;
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
    workflow_registry: &WorkflowRegistry,
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
    let catalogue_positions = catalogue
        .iter()
        .filter_map(|star| {
            star.position
                .map(|position| (star.key.id.as_str().to_owned(), position))
        })
        .collect::<BTreeMap<_, _>>();
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
            let region = assignment.and_then(|value| value.region.clone());
            let racing_vessel = racing_vessels.get(&code).cloned();
            let vessel = racing_vessel.as_deref().and_then(|vessel_code| {
                devices
                    .iter()
                    .find(|device| device.key.id.as_str() == vessel_code)
            });
            let physical_location = vessel
                .and_then(|vessel| vessel.location.as_ref())
                .map(|location| location.id.as_str().to_owned());
            let physical_region = vessel
                .and_then(|vessel| device_system(vessel, &location_systems))
                .and_then(|system| {
                    system_regions
                        .get(&system)
                        .cloned()
                        .or_else(|| operational_region_for_system(&system, &regions))
                });
            let busy_workflow = busy.get(&code).copied();
            let state = if busy_workflow.is_some() {
                WorkerState::Busy
            } else {
                region
                    .as_deref()
                    .map_or(WorkerState::WrongRegion, |assigned_region| {
                        classify_regional_worker(
                            &replicant,
                            vessel,
                            Some(assigned_region),
                            Some(assigned_region),
                            physical_region.as_deref(),
                            false,
                        )
                    })
            };
            WorkerView {
                region,
                role_affinity: assignment.and_then(|value| value.role_affinity.clone()),
                busy_workflow,
                racing_vessel,
                physical_location,
                replicant,
                state,
            }
        })
        .collect::<Vec<_>>();
    workers.sort_by(|left, right| left.replicant.key.id.cmp(&right.replicant.key.id));
    mark_partial_region_footholds(&mut regions, &workers);
    let manufacturing_homes =
        mark_manufacturing_footholds(&mut regions, &devices, &location_systems, &system_regions);

    let goal_controls = load_goal_controls(&repository, regions.keys().map(String::as_str))?;
    let mut requirements = DirectorRequirementGraph::load(&repository, now)?;
    let blueprint_catalogue_needed =
        goal_enabled(&goal_controls, DirectorGoalKind::BlueprintAcquisition, None)
            || (goal_enabled(&goal_controls, DirectorGoalKind::ExpandStarCatalogue, None)
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
    // Placement recovery is intentionally evaluated only after global goals
    // have persisted their work, so this snapshot includes same-pass intent.
    let placement_authority_complete = complete_owned_device_census(client);
    let placement_snapshot = workflow_registry.placement_intent_snapshot(&repository, None);
    let placement_snapshot_error = placement_snapshot.is_err();

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
            &catalogue_positions,
        )?);
        goals.push(reconcile_expand_mining(
            &repository,
            region,
            &workers,
            &workflows,
            &devices,
            &catalogue,
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
            &catalogue_positions,
            &BTreeSet::new(),
        )?);
    }

    let mut event_discovery_error = None;
    let mut event_systems_by_region = BTreeMap::<String, BTreeSet<String>>::new();
    let event_controls_enabled = established_regions.iter().any(|region| {
        goal_enabled(
            &goal_controls,
            DirectorGoalKind::EventCompletion,
            Some(&region.region),
        ) || goal_enabled(
            &goal_controls,
            DirectorGoalKind::ExpandFtlNetwork,
            Some(&region.region),
        )
    });
    let salvage_controls_enabled = established_regions.iter().any(|region| {
        goal_enabled(
            &goal_controls,
            DirectorGoalKind::SalvageRecovery,
            Some(&region.region),
        )
    });
    let mut salvage_discovery_error = None;
    let salvage_history = if salvage_controls_enabled {
        match salvage_recovery_history_for_director(
            client,
            repository.as_ref(),
            now,
            force_slow_refresh,
        )
        .await
        {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                tracing::warn!(error = %error, "Director salvage recovery history unavailable");
                salvage_discovery_error = Some(format!(
                    "salvage recovery history discovery failed: {error}"
                ));
                None
            }
        }
    } else {
        None
    };
    let salvage_completed = match completed_salvage_sites(&repository) {
        Ok(completed) => completed,
        Err(error) => {
            let message = format!("salvage recovery completion discovery failed: {error}");
            tracing::warn!(error = %message, "Director could not load salvage completion documents");
            if salvage_discovery_error.is_none() {
                salvage_discovery_error = Some(message);
            }
            BTreeSet::new()
        }
    };
    // Asteroid history is expensive remote history. Fetch it once only when at
    // least one regional diversion goal is actually enabled; enabling a goal
    // naturally triggers a fresh reconciliation, so disabled automation should
    // not continuously page an unrelated event stream.
    let asteroid_controls_enabled = established_regions.iter().any(|region| {
        goal_enabled(
            &goal_controls,
            DirectorGoalKind::AsteroidDiversion,
            Some(&region.region),
        )
    });
    let mut asteroid_discovery_error = None;
    let asteroid_history = if asteroid_controls_enabled {
        match asteroid_history_snapshot(client, now).await {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                tracing::warn!(error = %error, "Director asteroid diversion history unavailable");
                asteroid_discovery_error = Some(format!(
                    "asteroid diversion history discovery failed: {error}"
                ));
                None
            }
        }
    } else {
        None
    };
    let (
        asteroid_occurrences_by_region,
        asteroid_conflicts_by_region,
        asteroid_unavailable_by_region,
    ) = partition_asteroid_occurrences(asteroid_history.as_ref(), &catalogue, &regions);
    let event_designations_by_region = if event_controls_enabled && !established_regions.is_empty()
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

    let mut unserviced_launch_available = true;
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
        goals.push(reconcile_stranded_device_recovery(
            &goal_context,
            region,
            &devices,
            &locations,
            &location_systems,
            &system_regions,
            &regions,
            placement_snapshot.as_ref().ok(),
            placement_authority_complete,
            placement_snapshot_error,
        )?);
        goals.push(reconcile_salvage_recovery(
            &goal_context,
            region,
            salvage_history.as_ref(),
            &salvage_completed,
            salvage_discovery_error.as_deref(),
        )?);
        goals.push(reconcile_asteroid_diversion(
            &goal_context,
            region,
            asteroid_occurrences_by_region
                .get(&region.region)
                .unwrap_or(&BTreeMap::new()),
            asteroid_conflicts_by_region
                .get(&region.region)
                .copied()
                .unwrap_or(0),
            asteroid_unavailable_by_region
                .get(&region.region)
                .copied()
                .unwrap_or(0),
            asteroid_discovery_error.as_deref(),
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
            &catalogue_positions,
        )?);
        goals.push(reconcile_expand_mining(
            &repository,
            region,
            &workers,
            &workflows,
            &devices,
            &catalogue,
            &locations,
            &location_systems,
            &system_regions,
            &goal_controls,
            automatic,
            &mut reserved_workers,
            &mut requirements,
            now,
        )?);
        // Expand Mining may have created the exact route campaign above.  Read
        // the repository and service projection again so this goal never races
        // a same-pass mining expansion with a duplicate campaign.
        let unserviced_workflows = repository.list()?;
        let service_snapshot = workflow_registry.service_intent_snapshot(&repository, None)?;
        goals.push(reconcile_unserviced_resources(
            &goal_context,
            workflow_registry,
            &service_snapshot,
            region,
            &devices,
            &locations,
            &inventories,
            &system_regions,
            &unserviced_workflows,
            placement_authority_complete,
            &mut unserviced_launch_available,
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
            &catalogue_positions,
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
    // Recovery may have queued an exact-device manifest earlier in this pass.
    // Refresh both workflow rows and placement projections before Blueprint
    // Acquisition selects any source that it could irreversibly consume.
    let blueprint_workflows = repository.list()?;
    let blueprint_placement_reserved =
        match workflow_registry.placement_intent_snapshot(&repository, None) {
            Ok(snapshot) => devices
                .iter()
                .filter(|device| {
                    !snapshot
                        .explain_device(device.key.id.as_str(), &device.tags)
                        .live
                        .is_empty()
                })
                .map(|device| device.key.id.as_str().to_owned())
                .collect::<BTreeSet<_>>(),
            Err(_) => devices
                .iter()
                .map(|device| device.key.id.as_str().to_owned())
                .collect::<BTreeSet<_>>(),
        };
    let blueprint_goal_context = GoalReconcileContext {
        repository: repository.as_ref(),
        workflows: &blueprint_workflows,
        controls: &goal_controls,
        automatic,
        now,
    };

    // Shop discovery is intentionally demand-driven and happens only after
    // the standing goals have raised this pass's Blueprint requirements. Owned
    // copies can be learned without touching the trade directory, and an active
    // acquisition already has enough information to finish.
    let mut shop_requested_blueprints = BTreeSet::new();
    if goal_enabled(&goal_controls, DirectorGoalKind::BlueprintAcquisition, None)
        && active_blueprint_acquisition_workflow(&blueprint_workflows).is_none()
    {
        for device_type in requirements.current_blueprint_priorities().keys() {
            let kind = DeviceType::from(device_type.as_str());
            let already_known = unlocked_blueprints
                .as_ref()
                .is_some_and(|known| known.contains(&kind));
            let owned_source = devices.iter().any(|device| {
                blueprint_source_is_candidate(device, kind.as_str(), &devices)
                    && !blueprint_placement_reserved.contains(device.key.id.as_str())
            });
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
        placement_reserved_devices: &blueprint_placement_reserved,
    };
    goals.push(reconcile_blueprint_acquisition(
        &blueprint_goal_context,
        &mut blueprint_context,
        &mut requirements,
    )?);

    let worker_demand = requirements.worker_demand_by_region();
    let mut pending_worker_demand = worker_demand.values().sum::<usize>();
    let mut workforce_states = load_workforce_states(&repository)?;
    let workforce_reconciliation = reconcile_workforce(
        &repository,
        &settings,
        &regions,
        &manufacturing_homes,
        &workers,
        &workflows,
        &reserved_workers,
        &worker_demand,
        &mut workforce_states,
        automatic,
        now,
    )?;
    let total = workers.len();
    let broker = crate::assignment::ResourceBroker::with_managed_client(
        repository.clone(),
        (*client).clone(),
    );
    let allocation_candidates = broker.discover_candidates()?;
    let schedule = crate::scheduler::repository_schedule(
        &repository,
        &allocation_candidates,
        u32::try_from(total).unwrap_or(u32::MAX),
        now,
    )?;
    let unmet_scheduler_floors = schedule
        .iter()
        .filter(|decision| decision.action == crate::scheduler::ScheduleAction::GrowWorkforce)
        .count();
    pending_worker_demand = pending_worker_demand.saturating_add(unmet_scheduler_floors);
    repository.put_document("automation.scheduler", "latest", &schedule)?;
    let busy_count = workers
        .iter()
        .filter(|worker| worker.state == WorkerState::Busy)
        .count();
    let operational_count = workers
        .iter()
        .filter(|worker| worker.state == WorkerState::Operational)
        .count();
    let in_transit_count = workers
        .iter()
        .filter(|worker| worker.state == WorkerState::InTransit)
        .count();
    let unavailable_count = total
        .saturating_sub(busy_count)
        .saturating_sub(operational_count)
        .saturating_sub(in_transit_count);
    let idle = workers
        .iter()
        .filter(|worker| {
            worker.state == WorkerState::Operational
                && worker.busy_workflow.is_none()
                && !reserved_workers.contains(worker.replicant.key.id.as_str())
        })
        .count();
    let idle_ratio = if total == 0 {
        1.0
    } else {
        idle as f64 / total as f64
    };
    let scale_up_recommended =
        !workforce_reconciliation.recommendations.is_empty() || unmet_scheduler_floors != 0;
    let scale_reason = workforce_reconciliation
        .recommendations
        .first()
        .cloned()
        .or_else(|| {
        (pending_worker_demand > 0).then(|| format!(
            "{pending_worker_demand} regional assignment(s) are worker-blocked; waiting for the grow-only scale policy"
        ))
    });

    let mut region_summaries = regions
        .values()
        .map(|region| {
            let regional_workers = workers
                .iter()
                .filter(|worker| worker.region.as_deref() == Some(region.region.as_str()))
                .collect::<Vec<_>>();
            DirectorRegionSummary {
                region: region.region.clone(),
                status: region.status,
                hub_system: region.hub_system.clone(),
                hub_location: region.hub_location.clone(),
                replicants: regional_workers
                    .iter()
                    .map(|worker| worker.replicant.key.id.as_str().to_owned())
                    .collect(),
                known_systems: region.known_systems.len(),
                operational_workers: regional_workers
                    .iter()
                    .filter(|worker| worker.state == WorkerState::Operational)
                    .count(),
                workers_in_transit: regional_workers
                    .iter()
                    .filter(|worker| worker.state == WorkerState::InTransit)
                    .count(),
                busy_workers: regional_workers
                    .iter()
                    .filter(|worker| worker.state == WorkerState::Busy)
                    .count(),
            }
        })
        .collect::<Vec<_>>();
    region_summaries.sort_by(|left, right| left.region.cmp(&right.region));
    goals.sort_by(|left, right| {
        left.region
            .cmp(&right.region)
            .then_with(|| goal_kind_key(left.kind).cmp(goal_kind_key(right.kind)))
    });
    let mining_policies = mining_policy_summaries(
        repository.as_ref(),
        region_summaries.iter().map(|region| region.region.as_str()),
    )?;
    let requirement_summaries = requirements.persist(&repository)?;

    tracing::info!(
        regions = regions.len(),
        established_regions = established_regions.len(),
        workers = workers.len(),
        operational_workers = operational_count,
        workers_in_transit = in_transit_count,
        unavailable_workers = unavailable_count,
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
        mining_policies,
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
                state: protocol_worker_state(worker.state),
            })
            .collect(),
        requirements: requirement_summaries,
        workforce: DirectorWorkforceSummary {
            total,
            busy: busy_count,
            operational: operational_count,
            in_transit: in_transit_count,
            unavailable: unavailable_count,
            idle,
            idle_ratio,
            pending_worker_demand,
            scale_up_recommended,
            scale_reason,
            regions: workforce_reconciliation.regions,
        },
        urgency: schedule
            .into_iter()
            .map(|decision| DirectorUrgencyFact {
                automation: decision.automation,
                campaign: decision.campaign,
                item: decision.item,
                buffer: decision.buffer,
                burn_rate_per_hour: decision.burn_rate_per_hour,
                deadline_at_ms: decision.deadline_at_ms,
                lateness_cost: serde_json::to_value(decision.lateness_cost).unwrap_or(Value::Null),
                loss_over_one_hour: decision.loss_over_one_hour,
                floor: decision.floor,
                ceiling: decision.ceiling,
                current_grants: decision.current_grants,
                target_grants: decision.target_grants,
                urgency: decision.urgency,
                hysteresis_ratio: decision.hysteresis_ratio,
                action: match decision.action {
                    crate::scheduler::ScheduleAction::Hold => "hold",
                    crate::scheduler::ScheduleAction::Grant => "grant",
                    crate::scheduler::ScheduleAction::Reclaim => "reclaim",
                    crate::scheduler::ScheduleAction::GrowWorkforce => "grow_workforce",
                    crate::scheduler::ScheduleAction::Idle => "idle",
                }
                .into(),
                reasons: decision.reasons,
            })
            .collect(),
    };
    repository.put_document(SNAPSHOT_NS, SNAPSHOT_KEY, &snapshot)?;
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "Director snapshot persisted"
    );
    Ok(snapshot)
}

fn incoming_replicant_provisions(
    workflows: &[WorkflowInstance],
) -> Result<BTreeMap<String, usize>, ApplicationError> {
    let mut incoming = BTreeMap::new();
    for workflow in workflows.iter().filter(|workflow| {
        workflow.kind == replicant_provision_workflow_kind() && !workflow.status.is_terminal()
    }) {
        let intent = workflow.config::<ReplicantProvisionIntent>()?;
        *incoming
            .entry(canonical_region(&intent.region))
            .or_default() += 1;
    }
    Ok(incoming)
}

fn reconcile_establish_regions(
    context: &GoalReconcileContext<'_>,
    regions: &BTreeMap<String, RegionView>,
    workers: &[WorkerView],
    reserved: &mut BTreeSet<String>,
    requirements: &mut DirectorRequirementGraph,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::EstablishRegions;
    let enabled = goal_enabled(context.controls, kind, None);
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
        let incoming = incoming_replicant_provisions(context.workflows)?
            .get(&target.region)
            .copied()
            .unwrap_or_default();
        let bootstrap_population = target_workers.len().saturating_add(incoming);
        if bootstrap_population < REGION_BOOTSTRAP_TARGET {
            let needed = REGION_BOOTSTRAP_TARGET.saturating_sub(bootstrap_population);
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
                "Grow the {} workforce to {REGION_BOOTSTRAP_TARGET} Replicants before dispatching the regional ark",
                target.region
            ));
            DirectorGoalStatus::Blocked
        } else if target.status == DirectorRegionStatus::Establishing {
            next_action = Some(format!(
                "Continue useful {} bootstrap work while the regional System Hub becomes available",
                target.region
            ));
            DirectorGoalStatus::Active
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
    let enabled = goal_enabled(context.controls, kind, None);
    let id = goal_instance_id(kind, None);
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    let deployment_signature = observatory_deployment_signature(devices);
    if runtime.prospect_exhausted_signature != deployment_signature {
        runtime.prospect_exhausted_signature = None;
    }
    if runtime.active_workflows.iter().any(|workflow_id| {
        context
            .workflows
            .iter()
            .find(|workflow| workflow.id == *workflow_id && workflow.status.is_terminal())
            .is_some_and(observatory_workflow_exhausted)
    }) {
        runtime.prospect_exhausted_signature = deployment_signature.clone();
    }
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
    } else if deployment_signature.is_some()
        && runtime.prospect_exhausted_signature == deployment_signature
    {
        (
            DirectorGoalStatus::Blocked,
            Some(
                "The current observatory deployment has no newly visible stars in its sampled sparse directions"
                    .to_owned(),
            ),
            Some(
                "Move an observatory to a different frontier system or deploy another observatory before prospecting again"
                    .to_owned(),
            ),
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

fn observatory_deployment_signature(devices: &[Device]) -> Option<String> {
    let mut observatories = devices
        .iter()
        .filter(|device| device.device_type.as_ref() == Some(&DeviceType::GalacticObservatory))
        .map(|device| {
            format!(
                "{}@{}",
                device.key.id.as_str(),
                device
                    .location
                    .as_ref()
                    .map(|location| location.id.as_str())
                    .unwrap_or("<unplaced>")
            )
        })
        .collect::<Vec<_>>();
    observatories.sort();
    (!observatories.is_empty()).then(|| observatories.join("|"))
}

fn observatory_workflow_exhausted(workflow: &WorkflowInstance) -> bool {
    if workflow.status == WorkflowStatus::Failed
        && workflow.last_error.as_deref().is_some_and(|error| {
            error.contains("had no sparse prospect direction accepted after")
                || error.to_ascii_lowercase().contains("no new stars visible")
        })
    {
        return true;
    }
    workflow
        .result::<crate::observatory::AutoProspectReport>()
        .ok()
        .flatten()
        .is_some_and(|report| report.status == "exhausted")
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

/// Loads the Director's cached, history-derived salvage observation.
///
/// Completion documents are deliberately not part of this cache: callers load
/// them on every pass so a completed designation cannot be recreated while a
/// history snapshot is still fresh.
async fn salvage_recovery_history_for_director(
    client: &Client,
    repository: &WorkflowRepository,
    now: i64,
    force_refresh: bool,
) -> Result<SalvageRecoveryHistorySnapshot, String> {
    let cached = repository
        .read_document(SALVAGE_RECOVERY_CACHE_NS, SALVAGE_RECOVERY_CACHE_KEY)
        .map_err(|error| error.to_string())?
        .map(|(value, _)| serde_json::from_value::<SalvageRecoveryCache>(value))
        .transpose()
        .map_err(|error| error.to_string())?;
    if let Some(cache) = cached.as_ref() {
        let age_ms = now.saturating_sub(cache.refreshed_at_ms);
        if !force_refresh && age_ms <= SALVAGE_RECOVERY_CACHE_TTL_MS {
            tracing::debug!(
                event = "director.salvage_recovery.snapshot_cache_hit",
                age_ms,
                ttl_ms = SALVAGE_RECOVERY_CACHE_TTL_MS,
                discovered = cache.snapshot.discovered_count,
                "Director reused cached salvage recovery history"
            );
            return Ok(cache.snapshot.clone());
        }
    }

    let refresh = tokio::time::timeout(
        SALVAGE_RECOVERY_TIMEOUT,
        salvage_recovery_history_snapshot(client),
    )
    .await
    .map_err(|_| {
        format!(
            "salvage recovery history discovery exceeded {} seconds",
            SALVAGE_RECOVERY_TIMEOUT.as_secs()
        )
    })
    .and_then(|result| result);
    match refresh {
        Ok(snapshot) => {
            repository
                .put_document(
                    SALVAGE_RECOVERY_CACHE_NS,
                    SALVAGE_RECOVERY_CACHE_KEY,
                    &SalvageRecoveryCache {
                        refreshed_at_ms: now,
                        snapshot: snapshot.clone(),
                    },
                )
                .map_err(|error| error.to_string())?;
            Ok(snapshot)
        }
        Err(error) => {
            if let Some(cache) = cached {
                let age_ms = now.saturating_sub(cache.refreshed_at_ms);
                if age_ms <= SALVAGE_RECOVERY_STALE_FALLBACK_MS {
                    tracing::warn!(
                        error = %error,
                        age_ms,
                        discovered = cache.snapshot.discovered_count,
                        "Director salvage history refresh failed; using recent cached snapshot"
                    );
                    return Ok(cache.snapshot);
                }
            }
            Err(error)
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
    let enabled = goal_enabled(context.controls, kind, None);
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
                    && !blueprint
                        .placement_reserved_devices
                        .contains(device.key.id.as_str())
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
                    let criterion_has_owned_source = devices.iter().any(|device| {
                        blueprint_source_is_candidate(device, criterion.as_str(), devices)
                            && !claimed.contains(device.key.id.as_str())
                            && !blueprint
                                .placement_reserved_devices
                                .contains(device.key.id.as_str())
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
fn reconcile_stranded_device_recovery(
    context: &GoalReconcileContext<'_>,
    region: &RegionView,
    devices: &[Device],
    _locations: &[Location],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
    regions: &BTreeMap<String, RegionView>,
    placement_snapshot: Option<&WorkflowPlacementIntentSnapshot>,
    complete_owned_census: bool,
    placement_snapshot_error: bool,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::StrandedDeviceRecovery;
    let id = goal_instance_id(kind, Some(&region.region));
    let enabled = goal_enabled(context.controls, kind, Some(&region.region));
    let objective = "Recover stranded owned devices to regional System Hubs";
    let disabled_next = "Enable this standing goal to recover stranded owned devices";
    let authority_blocker =
        "Complete managed device and workflow authority before recovering stranded devices";
    let ambiguous_blocker =
        "One or more owned devices have unresolved workflow custody and cannot be recovered safely";
    let missing_home =
        "No exact regional System Hub location is available for stranded device recovery";
    let active_next = "Continue the active stranded device recovery";
    let satisfied_next = "Wait for newly stranded owned devices";

    let mut workflows = context.repository.list()?;
    workflows.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    let is_authorized_in_flight = |workflow: &replicant_workflow::WorkflowInstance| {
        let Ok(intent) = workflow.config::<LogisticsManifestIntent>() else {
            return false;
        };
        let Ok(checkpoint) = workflow.checkpoint::<LogisticsWorkflowCheckpoint>() else {
            return false;
        };
        if !checkpoint.started && checkpoint.placement_recovery_cleanup.is_empty() {
            return false;
        }
        read_placement_recovery_authorization(context.repository, workflow.id)
            .ok()
            .flatten()
            .is_some_and(|authorization| {
                placement_recovery_authorization_matches(&authorization, workflow.id, &intent)
            })
    };
    let scoped_recovery_workflows = workflows
        .iter()
        .filter(|workflow| {
            !workflow.status.is_terminal()
                && workflow.kind == crate::automation::logistics_manifest_workflow_kind()
                && !is_authorized_in_flight(workflow)
        })
        .filter_map(
            |workflow| match workflow.config::<LogisticsManifestIntent>() {
                Ok(intent) => {
                    intent.placement_recovery.as_ref()?;
                    let belongs_here = intent
                        .region
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .is_none_or(|value| crate::canonical_region(value) == region.region);
                    belongs_here.then_some(workflow.id)
                }
                Err(_) => Some(workflow.id),
            },
        )
        .collect::<Vec<_>>();
    let revoke_scoped_authorizations = || -> Result<(), ApplicationError> {
        for workflow_id in &scoped_recovery_workflows {
            revoke_placement_recovery_authorization(context.repository, *workflow_id)?;
        }
        Ok(())
    };
    let active_recovery_config_error = workflows.iter().any(|workflow| {
        if workflow.status.is_terminal()
            || workflow.kind != crate::automation::logistics_manifest_workflow_kind()
        {
            return false;
        }
        let Ok(intent) = workflow.config::<LogisticsManifestIntent>() else {
            return true;
        };
        let declared_region = intent
            .region
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(crate::canonical_region);
        if declared_region
            .as_deref()
            .is_some_and(|value| value != region.region)
        {
            return false;
        }
        let Some(metadata) = intent.placement_recovery.as_ref() else {
            return false;
        };
        let owns_code = intent
            .device_codes
            .iter()
            .all(|code| devices.iter().any(|device| device.key.id.as_str() == code));
        let valid = crate::automation::validate_placement_recovery_intent(&intent).is_ok();
        let resolutions_valid = metadata.placement_resolutions.iter().all(|resolution| {
            intent
                .device_codes
                .iter()
                .any(|code| code == &resolution.device_code)
        });
        if is_authorized_in_flight(workflow) {
            return !valid || !owns_code || !resolutions_valid;
        }
        let evidence_valid = placement_snapshot.is_some_and(|snapshot| {
            let filtered = recovery_snapshot_matches_for_workflow(snapshot, workflow.id);
            placement_recovery_metadata_matches_snapshot(metadata, &filtered).is_ok()
        });
        !valid
            || !evidence_valid
            || !owns_code
            || !resolutions_valid
            || region
                .hub_location
                .as_ref()
                .is_some_and(|hub| intent.destination != *hub)
    });
    let active_recovery = workflows
        .iter()
        .filter(|workflow| {
            !workflow.status.is_terminal()
                && workflow.kind == crate::automation::logistics_manifest_workflow_kind()
        })
        .filter_map(|workflow| {
            let intent = workflow.config::<LogisticsManifestIntent>().ok()?;
            let metadata = intent.placement_recovery.as_ref()?;
            let belongs_here = intent
                .region
                .as_deref()
                .is_some_and(|current| crate::canonical_region(current) == region.region);
            if !belongs_here {
                return None;
            }
            let valid = crate::automation::validate_placement_recovery_intent(&intent).is_ok()
                && !metadata.failed_provenance.is_empty();
            if is_authorized_in_flight(workflow) {
                return valid.then_some(workflow.id);
            }
            let destination_matches = region
                .hub_location
                .as_ref()
                .is_some_and(|hub| intent.destination == *hub);
            let metadata_matches_retained_evidence = placement_snapshot.is_some_and(|snapshot| {
                let filtered = recovery_snapshot_matches_for_workflow(snapshot, workflow.id);
                placement_recovery_metadata_matches_snapshot(metadata, &filtered).is_ok()
            });
            (destination_matches && valid && metadata_matches_retained_evidence)
                .then_some(workflow.id)
        })
        .collect::<Vec<_>>();
    let active_recovery_ids = protocol_workflow_ids(&active_recovery);

    let summary =
        |status: DirectorGoalStatus,
         blocker: Option<&str>,
         next_action: Option<String>,
         current: usize,
         total: usize,
         active_workflows: Vec<ProtocolWorkflowId>| DirectorGoalSummary {
            id: id.clone(),
            kind,
            region: Some(region.region.clone()),
            status,
            objective: objective.to_owned(),
            blocker: blocker.map(str::to_owned),
            next_action,
            progress_current: current as u64,
            progress_total: total as u64,
            active_workflows,
            enabled,
        };

    if !enabled {
        return Ok(summary(
            DirectorGoalStatus::Waiting,
            None,
            Some(disabled_next.to_owned()),
            0,
            active_recovery.len(),
            active_recovery_ids.clone(),
        ));
    }
    if active_recovery_config_error {
        revoke_scoped_authorizations()?;
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some(authority_blocker),
            Some(authority_blocker.to_owned()),
            0,
            active_recovery.len(),
            active_recovery_ids.clone(),
        ));
    }
    if !complete_owned_census || placement_snapshot_error || placement_snapshot.is_none() {
        revoke_scoped_authorizations()?;
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some(authority_blocker),
            Some(authority_blocker.to_owned()),
            0,
            active_recovery.len(),
            active_recovery_ids.clone(),
        ));
    }
    let snapshot = placement_snapshot.expect("checked above");
    if !snapshot.unknown_live_workflows.is_empty() || !snapshot.unknown_terminal_outcomes.is_empty()
    {
        revoke_scoped_authorizations()?;
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some(authority_blocker),
            Some(authority_blocker.to_owned()),
            0,
            active_recovery.len(),
            active_recovery_ids.clone(),
        ));
    }
    let Some(destination) = region.hub_location.clone() else {
        revoke_scoped_authorizations()?;
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some(missing_home),
            Some(missing_home.to_owned()),
            0,
            active_recovery.len(),
            active_recovery_ids.clone(),
        ));
    };
    if destination.trim().is_empty() {
        revoke_scoped_authorizations()?;
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some(missing_home),
            Some(missing_home.to_owned()),
            0,
            active_recovery.len(),
            active_recovery_ids,
        ));
    }
    let hub_authority_is_exact = region.hub_system.as_deref().is_some_and(|hub_system| {
        let location_system = location_systems
            .iter()
            .find(|(location, _)| location.eq_ignore_ascii_case(&destination))
            .map(|(_, system)| system);
        let Some(location_system) = location_system else {
            return false;
        };
        if !location_system.eq_ignore_ascii_case(hub_system) {
            return false;
        }
        // Gateway hubs intentionally have no formal system→region entry. The
        // RegionView itself is the authoritative registration in that case;
        // a formal mapping, when present, must still agree exactly.
        system_regions
            .iter()
            .find(|(system, _)| system.eq_ignore_ascii_case(hub_system))
            .is_none_or(|(_, mapped_region)| {
                crate::canonical_region(mapped_region) == region.region
            })
    });
    if !hub_authority_is_exact {
        revoke_scoped_authorizations()?;
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some(authority_blocker),
            Some(authority_blocker.to_owned()),
            0,
            active_recovery.len(),
            active_recovery_ids,
        ));
    }
    if active_recovery.len() > 1 {
        revoke_scoped_authorizations()?;
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some(authority_blocker),
            Some(authority_blocker.to_owned()),
            0,
            active_recovery.len(),
            active_recovery_ids.clone(),
        ));
    }

    let registered_homes = regions
        .values()
        .filter(|candidate| candidate.status == DirectorRegionStatus::Established)
        .filter_map(|candidate| {
            candidate
                .hub_location
                .as_ref()
                .filter(|location| !location.trim().is_empty())
                .map(|location| {
                    (
                        crate::canonical_region(&candidate.region),
                        BTreeSet::from([location.clone()]),
                    )
                })
        })
        .fold(
            BTreeMap::<String, BTreeSet<String>>::new(),
            |mut homes, (region, locations)| {
                homes.entry(region).or_default().extend(locations);
                homes
            },
        );
    let device_map = devices
        .iter()
        .map(|device| (device.key.id.as_str().to_owned(), device.clone()))
        .collect::<BTreeMap<_, _>>();
    let filtered_active_snapshot = active_recovery
        .first()
        .map(|workflow_id| recovery_snapshot_matches_for_workflow(snapshot, *workflow_id));
    let classification_snapshot = filtered_active_snapshot.as_ref().unwrap_or(snapshot);
    let placement_context = DevicePlacementContext {
        complete_owned_census,
        devices: &device_map,
        registered_homes: &registered_homes,
        location_systems,
        system_regions,
        workflow_snapshot: classification_snapshot,
    };

    let mut candidates = Vec::new();
    let mut ambiguous = 0usize;
    for device in devices {
        let classification = classify_device_placement(device, &placement_context);
        let relevant_ambiguous = classification.class == DevicePlacementClass::Ambiguous
            && (!classification.workflow_evidence.failed_transient.is_empty()
                || !classification
                    .workflow_evidence
                    .terminal_residuals
                    .is_empty()
                || !classification
                    .workflow_evidence
                    .unknown_live_workflows
                    .is_empty()
                || !classification
                    .workflow_evidence
                    .unknown_terminal_outcomes
                    .is_empty()
                || device
                    .tags
                    .iter()
                    .any(|tag| replicant_protocol::workflow_tag_reserved(tag)));
        if relevant_ambiguous && classification.region.as_deref() == Some(region.region.as_str()) {
            ambiguous += 1;
        }
        if classification.class != DevicePlacementClass::Stranded
            || classification.region.as_deref() != Some(region.region.as_str())
        {
            continue;
        }
        let Some(origin) = classification.effective_location.clone() else {
            continue;
        };
        let code = classification.device_code.clone();
        let mut failed_provenance =
            BTreeMap::<String, BTreeSet<WorkflowPlacementProvenance>>::new();
        let mut release_tags = BTreeSet::new();
        let mut resolutions = BTreeSet::new();
        for evidence in &classification.workflow_evidence.failed_transient {
            let provenance = WorkflowPlacementProvenance {
                workflow_id: evidence.workflow_id,
                work_item_id: evidence.intent.work_item_id,
            };
            failed_provenance
                .entry(code.clone())
                .or_default()
                .insert(provenance.clone());
            match &evidence.intent.subject {
                WorkflowPlacementIntentSubject::Device(subject)
                    if subject.eq_ignore_ascii_case(&code) =>
                {
                    resolutions.insert(WorkflowPlacementResolution {
                        device_code: code.clone(),
                        provenance,
                    });
                }
                WorkflowPlacementIntentSubject::DeviceTag(tag)
                    if device.tags.iter().any(|candidate| candidate == tag) =>
                {
                    release_tags.insert(tag.clone());
                }
                _ => {}
            }
        }
        let failed_provenance = failed_provenance
            .into_iter()
            .map(|(key, values)| (key, values.into_iter().collect()))
            .collect::<BTreeMap<_, _>>();
        let metadata = PlacementRecoveryMetadata {
            failed_provenance,
            release_device_tags: BTreeMap::from([(
                code.clone(),
                release_tags.into_iter().collect(),
            )]),
            placement_resolutions: resolutions.into_iter().collect(),
        };
        candidates.push(StrandedRecoveryCandidate {
            device_code: code,
            origin,
            destination: destination.clone(),
            metadata,
        });
    }
    candidates.sort_by(|left, right| {
        left.device_code
            .cmp(&right.device_code)
            .then_with(|| left.origin.cmp(&right.origin))
            .then_with(|| left.destination.cmp(&right.destination))
    });

    let total = candidates.len() + active_recovery.len() + ambiguous;
    if ambiguous > 0 {
        revoke_scoped_authorizations()?;
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some(ambiguous_blocker),
            Some(ambiguous_blocker.to_owned()),
            0,
            total,
            active_recovery_ids.clone(),
        ));
    }
    if !active_recovery.is_empty() {
        // Re-adoption requires a current classified candidate, not just a
        // durable workflow config.
        for workflow_id in &active_recovery {
            let workflow = workflows
                .iter()
                .find(|workflow| workflow.id == *workflow_id)
                .expect("active recovery workflow came from workflow list");
            let intent = workflow.config::<LogisticsManifestIntent>()?;
            if is_authorized_in_flight(workflow) {
                continue;
            }
            let Some(candidate) = candidates.iter().find(|candidate| {
                intent_matches_recovery(
                    &intent,
                    &region.region,
                    &candidate.device_code,
                    &candidate.origin,
                    &candidate.destination,
                    &candidate.metadata,
                )
            }) else {
                revoke_scoped_authorizations()?;
                return Ok(summary(
                    DirectorGoalStatus::Blocked,
                    Some(authority_blocker),
                    Some(authority_blocker.to_owned()),
                    0,
                    total,
                    active_recovery_ids.clone(),
                ));
            };
            let authorization = placement_recovery_authorization(
                *workflow_id,
                &region.region,
                &candidate.device_code,
                &candidate.origin,
                &candidate.destination,
                candidate.metadata.clone(),
            );
            write_placement_recovery_authorization(context.repository, &authorization)?;
        }
        return Ok(summary(
            DirectorGoalStatus::Active,
            None,
            Some(active_next.to_owned()),
            0,
            total,
            active_recovery_ids.clone(),
        ));
    }
    if candidates.is_empty() {
        return Ok(summary(
            DirectorGoalStatus::Satisfied,
            None,
            Some(satisfied_next.to_owned()),
            0,
            total,
            Vec::new(),
        ));
    }
    let candidate = &candidates[0];
    let identity = recovery_metadata_identity(&region.region, candidate);
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, &workflows);
    retain_work_identity(&mut runtime, &identity);
    let exact_failures = workflows
        .iter()
        .filter(|workflow| workflow.status == WorkflowStatus::Failed)
        .filter_map(|workflow| {
            let intent = workflow.config::<LogisticsManifestIntent>().ok()?;
            intent_matches_recovery(
                &intent,
                &region.region,
                &candidate.device_code,
                &candidate.origin,
                &candidate.destination,
                &candidate.metadata,
            )
            .then_some(workflow)
        })
        .collect::<Vec<_>>();
    let permanent_failure = exact_failures.iter().copied().find(|workflow| {
        workflow.failure_disposition == Some(WorkflowFailureDisposition::Permanent)
    });
    let retry_at = exact_failures
        .into_iter()
        .max_by_key(|workflow| workflow.updated_at)
        .map(|workflow| workflow.updated_at)
        .or(runtime.last_launch_at_ms);
    let retry_cooldown =
        retry_at.is_some_and(|last| context.now.saturating_sub(last) < DEFAULT_RETRY_COOLDOWN_MS);
    let action = format!(
        "Recover stranded device {} from {} to {}",
        candidate.device_code, candidate.origin, candidate.destination
    );
    if let Some(failure) = permanent_failure {
        save_goal_runtime(context.repository, &id, &runtime)?;
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            failure.last_error.as_deref().or(Some(
                "The exact stranded device recovery previously failed permanently",
            )),
            Some(action),
            0,
            total,
            Vec::new(),
        ));
    }
    if retry_cooldown {
        save_goal_runtime(context.repository, &id, &runtime)?;
        return Ok(summary(
            DirectorGoalStatus::Waiting,
            None,
            Some("Wait briefly before retrying stranded device recovery".to_owned()),
            0,
            total,
            Vec::new(),
        ));
    }
    if !context.automatic {
        save_goal_runtime(context.repository, &id, &runtime)?;
        return Ok(summary(
            DirectorGoalStatus::Active,
            None,
            Some(action),
            0,
            total,
            Vec::new(),
        ));
    }
    let intent = LogisticsManifestIntent {
        origin: candidate.origin.clone(),
        destination: candidate.destination.clone(),
        resources: ResourceMap::new(),
        devices: Vec::new(),
        device_codes: vec![candidate.device_code.clone()],
        device_tags: Vec::new(),
        pre_deactivate_device_codes: Vec::new(),
        release_mining_reservations: false,
        return_transports: true,
        allow_transport_staging: true,
        region: Some(region.region.clone()),
        purpose: format!(
            "director:stranded_device_recovery:{}:{}",
            candidate.device_code, candidate.destination
        ),
        placement_recovery: Some(candidate.metadata.clone()),
    };
    let created = context.repository.create_or_reuse_active(
        new_logistics_manifest_workflow(intent),
        |workflow| {
            let Ok(intent) = workflow.config::<LogisticsManifestIntent>() else {
                return Ok(false);
            };
            Ok(intent_matches_recovery(
                &intent,
                &region.region,
                &candidate.device_code,
                &candidate.origin,
                &candidate.destination,
                &candidate.metadata,
            ))
        },
    );
    match created {
        Ok(result) => {
            let authorization = placement_recovery_authorization(
                result.instance.id,
                &region.region,
                &candidate.device_code,
                &candidate.origin,
                &candidate.destination,
                candidate.metadata.clone(),
            );
            write_placement_recovery_authorization(context.repository, &authorization)?;
            runtime.active_workflows = vec![result.instance.id];
            runtime.last_launch_at_ms = Some(if result.created {
                context.now
            } else {
                result.instance.created_at
            });
            record_goal_launch(&mut runtime, result.instance.id, identity);
            save_goal_runtime(context.repository, &id, &runtime)?;
            Ok(summary(
                DirectorGoalStatus::Active,
                None,
                Some(action),
                0,
                total,
                vec![ProtocolWorkflowId(result.instance.id.to_string())],
            ))
        }
        Err(error) => {
            save_goal_runtime(context.repository, &id, &runtime)?;
            Ok(summary(
                DirectorGoalStatus::Blocked,
                Some(&error.to_string()),
                Some(action),
                0,
                total,
                Vec::new(),
            ))
        }
    }
}

fn recovery_manifest_well_formed(intent: &LogisticsManifestIntent) -> bool {
    crate::automation::validate_placement_recovery_intent(intent).is_ok()
}

fn recovery_snapshot_matches_for_workflow(
    snapshot: &WorkflowPlacementIntentSnapshot,
    workflow_id: WorkflowId,
) -> WorkflowPlacementIntentSnapshot {
    let mut filtered = snapshot.clone();
    filtered
        .live
        .retain(|evidence| evidence.workflow_id != workflow_id);
    filtered
        .settled_placements
        .retain(|evidence| evidence.workflow_id != workflow_id);
    filtered
        .terminal_residuals
        .retain(|evidence| evidence.workflow_id != workflow_id);
    filtered
        .failed_transient
        .retain(|evidence| evidence.workflow_id != workflow_id);
    filtered
        .resolved_transient
        .retain(|evidence| evidence.workflow_id != workflow_id);
    filtered
}

fn intent_matches_recovery(
    intent: &LogisticsManifestIntent,
    region: &str,
    device_code: &str,
    origin: &str,
    destination: &str,
    metadata: &PlacementRecoveryMetadata,
) -> bool {
    recovery_manifest_well_formed(intent)
        && intent
            .region
            .as_deref()
            .is_some_and(|current| crate::canonical_region(current) == region)
        && intent.origin == origin
        && intent.destination == destination
        && intent.device_codes.len() == 1
        && intent
            .device_codes
            .first()
            .is_some_and(|code| code == device_code)
        && intent.placement_recovery.as_ref().is_some_and(|current| {
            current.failed_provenance == metadata.failed_provenance
                && current.release_device_tags == metadata.release_device_tags
                && current.placement_resolutions == metadata.placement_resolutions
        })
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
    let enabled = goal_enabled(context.controls, kind, Some(&region.region));
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
                    pre_deactivate_device_codes: Vec::new(),
                    release_mining_reservations: false,
                    placement_recovery: None,
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
type AsteroidOccurrencesByRegion = BTreeMap<String, BTreeMap<String, AsteroidOccurrence>>;
type AsteroidPartition = (
    AsteroidOccurrencesByRegion,
    BTreeMap<String, usize>,
    BTreeMap<String, usize>,
);

fn partition_asteroid_occurrences(
    history: Option<&AsteroidHistorySnapshot>,
    catalogue: &[Star],
    regions: &BTreeMap<String, RegionView>,
) -> AsteroidPartition {
    let mut occurrences_by_region = BTreeMap::new();
    let mut conflicts_by_region = BTreeMap::new();
    let mut unavailable_by_region = BTreeMap::new();
    let Some(history) = history else {
        return (
            occurrences_by_region,
            conflicts_by_region,
            unavailable_by_region,
        );
    };
    let system_regions = expanded_system_region_map(catalogue);
    for (occurrence_id, occurrence) in &history.occurrences {
        let Some(system) = catalogue_system_for_target(&occurrence.impact_target, &system_regions)
        else {
            continue;
        };
        let Some(region) = system_regions.get(&system) else {
            continue;
        };
        if !regions.contains_key(region) {
            continue;
        }
        match history
            .lifecycle
            .get(occurrence_id)
            .copied()
            .unwrap_or(AsteroidLifecycle::ObservationUnavailable)
        {
            AsteroidLifecycle::IdentityConflict => {
                *conflicts_by_region.entry(region.clone()).or_insert(0) += 1;
            }
            AsteroidLifecycle::ObservationUnavailable => {
                *unavailable_by_region.entry(region.clone()).or_insert(0) += 1;
            }
            AsteroidLifecycle::Detected
            | AsteroidLifecycle::DiversionActive
            | AsteroidLifecycle::Partial => {
                occurrences_by_region
                    .entry(region.clone())
                    .or_insert_with(BTreeMap::new)
                    .insert(occurrence_id.clone(), occurrence.clone());
            }
            AsteroidLifecycle::Diverted
            | AsteroidLifecycle::Impacted
            | AsteroidLifecycle::Expired
            | AsteroidLifecycle::Superseded => {}
        }
    }
    (
        occurrences_by_region,
        conflicts_by_region,
        unavailable_by_region,
    )
}

fn catalogue_system_for_target(
    target: &str,
    system_regions: &BTreeMap<String, String>,
) -> Option<String> {
    system_regions
        .keys()
        .find(|system| system.eq_ignore_ascii_case(target))
        .cloned()
        .or_else(|| {
            system_regions
                .keys()
                .find(|system| system_prefix(system).eq_ignore_ascii_case(system_prefix(target)))
                .cloned()
        })
}

fn matching_asteroid_diversion_workflows<'a>(
    workflows: &'a [WorkflowInstance],
    region: &str,
) -> Result<Vec<&'a WorkflowInstance>, RepositoryError> {
    let mut matches = Vec::new();
    for workflow in workflows {
        if asteroid_diversion_workflow_matches(workflow, region)? {
            matches.push(workflow);
        }
    }
    matches.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(matches)
}

fn reconcile_asteroid_diversion(
    context: &GoalReconcileContext<'_>,
    region: &RegionView,
    occurrences: &BTreeMap<String, AsteroidOccurrence>,
    identity_conflicts: usize,
    unavailable: usize,
    discovery_error: Option<&str>,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::AsteroidDiversion;
    let enabled = goal_enabled(context.controls, kind, Some(&region.region));
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(context.repository, &id)?;

    if !enabled {
        return Ok(DirectorGoalSummary {
            id,
            kind,
            region: Some(region.region.clone()),
            status: DirectorGoalStatus::Waiting,
            objective: initial_goal_objective(kind).to_owned(),
            blocker: None,
            next_action: Some("Enable Asteroid Diversion for this region".to_owned()),
            progress_current: 0,
            progress_total: 0,
            active_workflows: Vec::new(),
            enabled,
        });
    }

    prune_runtime_workflows(&mut runtime, context.workflows);
    let adopted = matching_asteroid_diversion_workflows(context.workflows, &region.region)?;
    let adopted_id = adopted.first().map(|workflow| workflow.id);
    runtime.active_workflows = adopted_id.into_iter().collect();
    let identity = GoalWorkIdentity::AsteroidDiversion {
        region: region.region.clone(),
        occurrences: occurrences.keys().cloned().collect(),
    };
    retain_work_identity(&mut runtime, &identity);
    if let Some(workflow_id) = adopted_id
        && !runtime
            .launch_records
            .iter()
            .any(|record| record.workflow_id == workflow_id)
    {
        runtime.launch_records.push(GoalLaunchRecord {
            workflow_id,
            identity: identity.clone(),
        });
    }

    let active = nonterminal_ids(&runtime, context.workflows);
    let permanent_failure = permanent_failure_for_identity(&runtime, context.workflows, &identity);
    let mut progress_current = 0;
    if let Some(workflow_id) = active.first().copied() {
        progress_current = context
            .repository
            .list_work_items(workflow_id)?
            .into_iter()
            .filter(|item| item.state.status.is_terminal())
            .count() as u64;
    }
    let progress_total = occurrences.len() as u64;
    let mut blocker = None;
    let status = if let Some(error) = discovery_error {
        blocker = Some(error.to_owned());
        if active.is_empty() {
            DirectorGoalStatus::Blocked
        } else {
            DirectorGoalStatus::Active
        }
    } else if !active.is_empty() {
        DirectorGoalStatus::Active
    } else if occurrences.is_empty() {
        if identity_conflicts > 0 {
            blocker = Some(format!(
                "{identity_conflicts} asteroid occurrence identity conflict(s) need authoritative disambiguation"
            ));
            DirectorGoalStatus::Blocked
        } else if unavailable > 0 {
            DirectorGoalStatus::Waiting
        } else {
            runtime.launch_records.clear();
            runtime.last_launch_at_ms = None;
            DirectorGoalStatus::Satisfied
        }
    } else if let Some(failure) = permanent_failure {
        blocker = failure.last_error.clone();
        DirectorGoalStatus::Blocked
    } else if launch_is_recent(&runtime, context.now, DEFAULT_RETRY_COOLDOWN_MS) {
        DirectorGoalStatus::Waiting
    } else {
        let Some(home) = region
            .hub_location
            .clone()
            .or_else(|| region.hub_system.clone())
        else {
            blocker = Some(format!(
                "{} has no operational regional home for asteroid diversion",
                region.region
            ));
            save_goal_runtime(context.repository, &id, &runtime)?;
            return Ok(DirectorGoalSummary {
                id,
                kind,
                region: Some(region.region.clone()),
                status: DirectorGoalStatus::Blocked,
                objective: initial_goal_objective(kind).to_owned(),
                blocker,
                next_action: Some(
                    "Establish a regional System Hub before diverting asteroids".to_owned(),
                ),
                progress_current,
                progress_total,
                active_workflows: Vec::new(),
                enabled,
            });
        };
        if context.automatic {
            let result = context.repository.create_or_reuse_active(
                new_asteroid_diversion_workflow(AsteroidDiversionIntent {
                    region: region.region.clone(),
                    home,
                }),
                |workflow| asteroid_diversion_workflow_matches(workflow, &region.region),
            )?;
            runtime.active_workflows = vec![result.instance.id];
            runtime.last_launch_at_ms = Some(if result.created {
                context.now
            } else {
                result.instance.created_at
            });
            record_goal_launch(&mut runtime, result.instance.id, identity.clone());
        }
        DirectorGoalStatus::Active
    };
    let next_action = match status {
        DirectorGoalStatus::Satisfied => {
            Some("Wait for a new incoming asteroid detection".to_owned())
        }
        DirectorGoalStatus::Active if runtime.active_workflows.is_empty() => Some(format!(
            "Divert {} incoming asteroid(s) in this region",
            occurrences.len()
        )),
        DirectorGoalStatus::Active => {
            Some("Continue the active regional asteroid diversion campaign".to_owned())
        }
        DirectorGoalStatus::Blocked if discovery_error.is_some() => {
            Some("Retry asteroid diversion history discovery on the next Director pass".to_owned())
        }
        DirectorGoalStatus::Blocked => {
            Some("Resolve the regional asteroid diversion blocker before retrying".to_owned())
        }
        DirectorGoalStatus::Waiting if unavailable > 0 => {
            Some("Wait for authoritative asteroid evidence".to_owned())
        }
        DirectorGoalStatus::Waiting => {
            Some("Wait briefly before retrying asteroid diversion".to_owned())
        }
    };
    save_goal_runtime(context.repository, &id, &runtime)?;
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: Some(region.region.clone()),
        status,
        objective: initial_goal_objective(kind).to_owned(),
        blocker,
        next_action,
        progress_current,
        progress_total,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
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

fn matching_salvage_recovery_workflows<'a>(
    workflows: &'a [WorkflowInstance],
    region: &str,
) -> Result<Vec<&'a WorkflowInstance>, RepositoryError> {
    let mut matches = Vec::new();
    for workflow in workflows {
        if salvage_recovery_workflow_matches(workflow, region)? {
            matches.push(workflow);
        }
    }
    matches.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(matches)
}

fn reconcile_salvage_recovery(
    context: &GoalReconcileContext<'_>,
    region: &RegionView,
    history: Option<&SalvageRecoveryHistorySnapshot>,
    completed: &BTreeSet<String>,
    discovery_error: Option<&str>,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::SalvageRecovery;
    let enabled = goal_enabled(context.controls, kind, Some(&region.region));
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(context.repository, &id)?;

    if !enabled {
        return Ok(DirectorGoalSummary {
            id,
            kind,
            region: Some(region.region.clone()),
            status: DirectorGoalStatus::Waiting,
            objective: initial_goal_objective(kind).to_owned(),
            blocker: None,
            next_action: Some("Enable Salvage Recovery for this region".to_owned()),
            progress_current: 0,
            progress_total: 0,
            active_workflows: Vec::new(),
            enabled,
        });
    }

    prune_runtime_workflows(&mut runtime, context.workflows);
    let (adopted, compatibility_error) =
        match matching_salvage_recovery_workflows(context.workflows, &region.region) {
            Ok(workflows) => (workflows, None),
            Err(error) => (
                Vec::new(),
                Some(format!(
                    "salvage recovery workflow compatibility check failed: {error}"
                )),
            ),
        };
    let discovery_error = compatibility_error.as_deref().or(discovery_error);
    let adopted_ids = adopted
        .iter()
        .map(|workflow| workflow.id)
        .collect::<Vec<_>>();
    runtime.active_workflows = adopted_ids.clone();
    if runtime.last_launch_at_ms.is_none() {
        let adopted_launch = adopted.iter().map(|workflow| workflow.created_at).max();
        let legacy_terminal_launch = context
            .workflows
            .iter()
            .filter(|workflow| {
                workflow.kind == salvage_recovery_workflow_kind() && workflow.status.is_terminal()
            })
            .filter_map(|workflow| {
                let intent = workflow.config::<SalvageRecoveryIntent>().ok()?;
                (!intent.home.trim().is_empty()
                    && canonical_region(&intent.region) == canonical_region(&region.region))
                .then_some(workflow.created_at)
            })
            .max();
        runtime.last_launch_at_ms = adopted_launch
            .into_iter()
            .chain(legacy_terminal_launch)
            .max();
    }

    let recoverable = history
        .map(|snapshot| recoverable_salvage_sites(snapshot, completed, &region.region))
        .unwrap_or_default();
    let identity = GoalWorkIdentity::SalvageRecovery {
        region: region.region.clone(),
        sites: recoverable.keys().cloned().collect(),
    };
    if history.is_some() {
        if recoverable.is_empty() {
            runtime.launch_records.clear();
            runtime.last_launch_at_ms = None;
        } else {
            retain_work_identity(&mut runtime, &identity);
            let adopted_set = adopted_ids.iter().copied().collect::<BTreeSet<_>>();
            runtime.launch_records.retain(|record| {
                adopted_set.contains(&record.workflow_id)
                    || context.workflows.iter().any(|workflow| {
                        workflow.id == record.workflow_id
                            && workflow.status.is_terminal()
                            && workflow.failure_disposition
                                == Some(WorkflowFailureDisposition::Permanent)
                    })
            });
            for workflow_id in adopted_ids.iter().copied() {
                if !runtime
                    .launch_records
                    .iter()
                    .any(|record| record.workflow_id == workflow_id)
                {
                    runtime.launch_records.push(GoalLaunchRecord {
                        workflow_id,
                        identity: identity.clone(),
                    });
                }
            }
        }
    }

    let active = adopted_ids;
    let status = if !active.is_empty() {
        DirectorGoalStatus::Active
    } else if discovery_error.is_some() {
        DirectorGoalStatus::Blocked
    } else if recoverable.is_empty() {
        runtime.launch_records.clear();
        runtime.last_launch_at_ms = None;
        DirectorGoalStatus::Satisfied
    } else if history.is_some()
        && permanent_failure_for_identity(&runtime, context.workflows, &identity).is_some()
    {
        DirectorGoalStatus::Blocked
    } else if launch_is_recent(&runtime, context.now, DEFAULT_RETRY_COOLDOWN_MS) {
        DirectorGoalStatus::Waiting
    } else {
        let Some(home) = region
            .hub_location
            .clone()
            .or_else(|| region.hub_system.clone())
        else {
            let blocker = format!(
                "{} has no operational regional home for salvage recovery",
                region.region
            );
            save_goal_runtime(context.repository, &id, &runtime)?;
            return Ok(DirectorGoalSummary {
                id,
                kind,
                region: Some(region.region.clone()),
                status: DirectorGoalStatus::Blocked,
                objective: initial_goal_objective(kind).to_owned(),
                blocker: Some(blocker),
                next_action: Some(
                    "Establish a regional System Hub before recovering salvage".to_owned(),
                ),
                progress_current: 0,
                progress_total: recoverable.len() as u64,
                active_workflows: Vec::new(),
                enabled,
            });
        };
        if !context.automatic {
            DirectorGoalStatus::Active
        } else {
            let result = context.repository.create_or_reuse_active(
                new_salvage_recovery_workflow(SalvageRecoveryIntent {
                    region: region.region.clone(),
                    home,
                }),
                |workflow| salvage_recovery_workflow_matches(workflow, &region.region),
            )?;
            runtime.active_workflows = vec![result.instance.id];
            runtime.last_launch_at_ms = Some(if result.created {
                context.now
            } else {
                result.instance.created_at
            });
            record_goal_launch(&mut runtime, result.instance.id, identity.clone());
            DirectorGoalStatus::Active
        }
    };
    let blocker = if matches!(status, DirectorGoalStatus::Blocked) {
        discovery_error.map(str::to_owned).or_else(|| {
            permanent_failure_for_identity(&runtime, context.workflows, &identity)
                .and_then(|workflow| workflow.last_error.clone())
        })
    } else {
        None
    };
    let next_action = match status {
        DirectorGoalStatus::Satisfied => Some("Wait for newly discovered regional salvage".to_owned()),
        DirectorGoalStatus::Active if runtime.active_workflows.is_empty() => Some(format!(
            "Recover {} discovered regional salvage site(s)",
            recoverable.len()
        )),
        DirectorGoalStatus::Active => {
            Some("Continue the active regional salvage recovery campaign".to_owned())
        }
        DirectorGoalStatus::Blocked if discovery_error.is_some() => Some(
            "Retry salvage history discovery on the next Director pass".to_owned(),
        ),
        DirectorGoalStatus::Blocked => Some(
            "Change the regional salvage designation set before launching replacement campaign work"
                .to_owned(),
        ),
        DirectorGoalStatus::Waiting => Some("Wait briefly before retrying regional salvage recovery".to_owned()),
    };
    save_goal_runtime(context.repository, &id, &runtime)?;
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: Some(region.region.clone()),
        status,
        objective: initial_goal_objective(kind).to_owned(),
        blocker,
        next_action,
        progress_current: 0,
        progress_total: recoverable.len() as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
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
    let enabled = goal_enabled(context.controls, kind, Some(&region.region));
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);
    let identity = GoalWorkIdentity::EventCampaign {
        region: region.region.clone(),
        events: events.iter().cloned().collect(),
    };
    let permanent_failure = if event_discovery_error.is_none() {
        retain_work_identity(&mut runtime, &identity);
        permanent_failure_for_identity(&runtime, context.workflows, &identity)
    } else {
        None
    };
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
    } else if let Some(failure) = permanent_failure {
        blocker = failure.last_error.clone();
        next_action = Some(
            "Change the regional event set before launching replacement campaign work".to_owned(),
        );
        DirectorGoalStatus::Blocked
    } else if recently_launched {
        next_action = Some("Wait briefly before retrying the regional event campaign".to_owned());
        DirectorGoalStatus::Waiting
    } else if let Some(worker) = select_idle_worker(workers, &region.region, reserved, false) {
        next_action = Some(format!(
            "Batch-plan and execute {} active event(s) with {worker}",
            events.len()
        ));
        if context.automatic
            && let Some(home) = region
                .hub_location
                .clone()
                .or_else(|| region.hub_system.clone())
        {
            let workflow =
                context
                    .repository
                    .create(new_event_campaign_workflow(EventCampaignIntent {
                        region: region.region.clone(),
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
            record_goal_launch(&mut runtime, workflow.id, identity.clone());
            reserved.insert(worker);
        }
        DirectorGoalStatus::Active
    } else if regional_workers_in_transit(workers, &region.region, reserved) > 0 {
        blocker = Some("assigned regional workers are still in transit".to_owned());
        next_action = Some("Wait for an assigned regional worker to arrive".to_owned());
        DirectorGoalStatus::Waiting
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

fn planetary_survey_complete(location: &Location) -> bool {
    location.survey_progress.system_survey_complete() == Some(true)
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
    let enabled = goal_enabled(context.controls, kind, Some(&region.region));
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);

    // Season Three regenerated every planet and moon. `Star::explored` is
    // catalogue history, not proof that the current planetary generation has
    // been surveyed. Exact current aggregate counters are preferred when the
    // API supplies them; a terminal post-reset `survey_system` digest is the
    // durable fallback on live responses that omit those aggregate counters.
    // The v9 managed-store migration clears pre-3.0 survey evidence, so
    // existing installations naturally schedule a fresh survey.
    let catalogue = client.galaxy().catalogue();
    let survey_scope = catalogue_survey_scope_from_hub(region, &catalogue);
    let (survey_targets, missing_positions) = survey_scope
        .as_ref()
        .map(|scope| (scope.systems.clone(), scope.missing_positions))
        .unwrap_or_default();
    let surveyed_systems = survey_targets
        .iter()
        .filter(|system| {
            client
                .locations()
                .cached(system)
                .as_ref()
                .is_some_and(planetary_survey_complete)
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let unsurveyed = survey_targets
        .difference(&surveyed_systems)
        .cloned()
        .collect::<Vec<_>>();
    let surveyed = surveyed_systems.len();
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
    let in_transit_workers = regional_workers_in_transit(workers, &region.region, reserved);
    let launch_slots = open_slots.min(available_workers.len());
    let worker_shortage =
        open_slots.saturating_sub(available_workers.len().saturating_add(in_transit_workers));

    let mut blocker = None;
    let mut next_action = None;
    let status = if !enabled {
        DirectorGoalStatus::Waiting
    } else if survey_scope.is_none() {
        blocker = Some(match region.hub_system.as_deref() {
            Some(hub) => format!(
                "{} regional hub {hub} has no catalogue position, so its {:.0} LY survey footprint cannot be resolved",
                region.region, REGIONAL_AUTOMATION_RADIUS_LY
            ),
            None => format!(
                "{} has no selected regional hub, so its {:.0} LY survey footprint cannot be resolved",
                region.region, REGIONAL_AUTOMATION_RADIUS_LY
            ),
        });
        next_action = Some(
            "Resolve the regional hub and catalogue position before assigning survey tours"
                .to_owned(),
        );
        DirectorGoalStatus::Blocked
    } else if unsurveyed.is_empty() && missing_positions == 0 {
        next_action = Some(format!(
            "Wait for newly discovered systems within {:.0} LY of the regional hub or stale catalogue coverage",
            REGIONAL_AUTOMATION_RADIUS_LY
        ));
        DirectorGoalStatus::Satisfied
    } else if unsurveyed.is_empty() {
        blocker = Some(format!(
            "{} known system(s) in {} lack catalogue positions, so their inclusion in the {:.0} LY survey footprint cannot yet be determined",
            missing_positions, region.region, REGIONAL_AUTOMATION_RADIUS_LY
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
            let center = region.hub_system.clone();
            if let Some(center) = center {
                // Keep each durable survey tour bounded. The old partitioning
                // spread the entire post-reset regional backlog across the
                // four available workers, producing multi-thousand-system
                // workflows that were expensive to initialize, difficult to
                // inspect, and slow to recover. The Director already
                // reconciles continuously, so launch one small batch per
                // worker and pick up the remaining systems on later passes.
                let shards = partition_catalogue_batch(&pending, launch_slots);
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
                                radius_ly: REGIONAL_AUTOMATION_RADIUS_LY,
                                system_limit: systems.len(),
                                target_systems: Some(systems),
                                replicant: Some(worker.clone()),
                                vessel: Some(vessel),
                                // Exact targets are selected from current planetary
                                // survey completeness, so catalogue `explored` must not
                                // suppress Season Three re-surveys.
                                include_explored: true,
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
        } else if in_transit_workers > 0 {
            blocker = Some("assigned regional workers are still in transit".to_owned());
            next_action = Some("Wait for an assigned catalogue worker to arrive".to_owned());
            DirectorGoalStatus::Waiting
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
        objective: format!(
            "Maintain current planet/moon survey coverage within {:.0} LY of the regional hub in {}",
            REGIONAL_AUTOMATION_RADIUS_LY, region.region
        ),
        blocker,
        next_action,
        progress_current: surveyed as u64,
        progress_total: survey_targets.len().saturating_add(missing_positions) as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}

#[allow(clippy::too_many_arguments)]
fn reconcile_discover_belts(
    context: &GoalReconcileContext<'_>,
    region: &RegionView,
    _workers: &[WorkerView],
    _reserved: &mut BTreeSet<String>,
    _requirements: &mut DirectorRequirementGraph,
    locations: &[Location],
    location_systems: &BTreeMap<String, String>,
    catalogue_positions: &BTreeMap<String, GalacticPosition>,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::DiscoverBelts;
    let enabled = goal_enabled(context.controls, kind, Some(&region.region));
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);
    let searched = belt_searched_systems(locations, location_systems);
    let search_scope =
        regional_system_scope_from_hub(region, catalogue_positions, REGIONAL_AUTOMATION_RADIUS_LY);
    let (targets, missing_positions, covered, scoped_systems) = search_scope
        .as_ref()
        .map(|scope| {
            (
                belt_search_targets_from_hub(
                    region,
                    &scope.systems,
                    &searched,
                    catalogue_positions,
                ),
                scope.missing_positions,
                scope.systems.intersection(&searched).count(),
                scope.systems.len(),
            )
        })
        .unwrap_or_default();
    let active = nonterminal_ids(&runtime, context.workflows);
    let recently_launched = launch_is_recent(&runtime, context.now, DEFAULT_RETRY_COOLDOWN_MS);
    let mut blocker = None;
    let next_action;
    let status = if !enabled {
        next_action =
            Some("Enable this standing goal to search known systems for belts".to_owned());
        DirectorGoalStatus::Waiting
    } else if search_scope.is_none() {
        blocker = Some(regional_radius_resolution_blocker(
            region,
            REGIONAL_AUTOMATION_RADIUS_LY,
            "belt-search",
        ));
        next_action = Some(
            "Resolve the regional hub and catalogue position before assigning belt searches"
                .to_owned(),
        );
        DirectorGoalStatus::Blocked
    } else if targets.is_empty() && missing_positions == 0 {
        next_action = Some(format!(
            "Wait for newly discovered systems within {:.0} LY of the regional hub",
            REGIONAL_AUTOMATION_RADIUS_LY
        ));
        DirectorGoalStatus::Satisfied
    } else if targets.is_empty() {
        blocker = Some(format!(
            "{missing_positions} known system(s) in {} lack catalogue positions, so their inclusion in the {:.0} LY belt-search footprint cannot yet be determined",
            region.region, REGIONAL_AUTOMATION_RADIUS_LY
        ));
        next_action = Some(
            "Wait for catalogue position data before assigning another belt search".to_owned(),
        );
        DirectorGoalStatus::Blocked
    } else if !active.is_empty() {
        next_action = Some("Continue the active regional fast belt search".to_owned());
        DirectorGoalStatus::Active
    } else if recently_launched {
        next_action = Some("Wait briefly before retrying the next belt-search batch".to_owned());
        DirectorGoalStatus::Waiting
    } else {
        next_action = Some(format!(
            "Schedule {} unscanned system(s) within {:.0} LY of the regional hub across the {} worker pool",
            targets.len(),
            REGIONAL_AUTOMATION_RADIUS_LY,
            region.region
        ));
        if context.automatic {
            let workflow = context
                .repository
                .create(new_belt_search_campaign_workflow(
                    BeltSearchCampaignIntent {
                        systems: targets,
                        region: region.region.clone(),
                    },
                ))?;
            runtime.active_workflows = vec![workflow.id];
            runtime.last_launch_at_ms = Some(context.now);
        }
        DirectorGoalStatus::Active
    };
    save_goal_runtime(context.repository, &id, &runtime)?;
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: Some(region.region.clone()),
        status,
        objective: format!(
            "Discover asteroid belts within {:.0} LY of the regional hub in {}",
            REGIONAL_AUTOMATION_RADIUS_LY, region.region
        ),
        blocker,
        next_action,
        progress_current: covered as u64,
        progress_total: scoped_systems.saturating_add(missing_positions) as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}
fn belt_search_targets_from_hub(
    region: &RegionView,
    systems: &BTreeSet<String>,
    searched: &BTreeSet<String>,
    positions: &BTreeMap<String, GalacticPosition>,
) -> Vec<String> {
    let hub_position = region
        .hub_system
        .as_deref()
        .and_then(|hub| positions.get(hub))
        .copied();
    let mut targets = systems.difference(searched).cloned().collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        let by_distance = hub_position.map_or(std::cmp::Ordering::Equal, |hub| {
            match (positions.get(left), positions.get(right)) {
                (Some(left), Some(right)) => {
                    galactic_distance(hub, *left).total_cmp(&galactic_distance(hub, *right))
                }
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });
        by_distance.then_with(|| left.cmp(right))
    });
    targets
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
#[derive(Clone, Debug)]
struct UnservicedRouteCandidate {
    route: crate::mining::AmiTransportRouteIntent,
    resource_state: crate::mining::EvidenceState,
}

#[allow(clippy::too_many_arguments)]
fn reconcile_unserviced_resources(
    context: &GoalReconcileContext<'_>,
    workflow_registry: &WorkflowRegistry,
    service_snapshot: &WorkflowServiceIntentSnapshot,
    region: &RegionView,
    devices: &[Device],
    locations: &[Location],
    inventories: &[Inventory],
    system_regions: &BTreeMap<String, String>,
    workflows: &[WorkflowInstance],
    complete_authority: bool,
    launch_available: &mut bool,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::UnservicedResources;
    let id = goal_instance_id(kind, Some(&region.region));
    let enabled = goal_enabled(context.controls, kind, Some(&region.region));
    let objective = initial_goal_objective(kind);
    let disabled_next = "Enable this standing goal to reconcile AMI transport coverage";
    let missing_home =
        "No exact regional System Hub location is available for AMI transport delivery";
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, workflows);

    let summary =
        |status: DirectorGoalStatus,
         blocker: Option<String>,
         next_action: Option<String>,
         current: usize,
         total: usize,
         active_workflows: Vec<ProtocolWorkflowId>| DirectorGoalSummary {
            id: id.clone(),
            kind,
            region: Some(region.region.clone()),
            status,
            objective: objective.to_owned(),
            blocker,
            next_action,
            progress_current: current as u64,
            progress_total: total as u64,
            active_workflows,
            enabled,
        };
    let retained = workflows
        .iter()
        .filter(|workflow| {
            !workflow.status.is_terminal() && runtime.active_workflows.contains(&workflow.id)
        })
        .filter_map(|workflow| {
            let intent = workflow.config::<MiningCampaignIntent>().ok()?;
            (intent.region == region.region
                && intent.transport_routes.iter().any(|route| {
                    !route.system.trim().is_empty()
                        && route.collect != route.deliver
                        && route.deliver == region.hub_location.as_deref().unwrap_or_default()
                }))
            .then_some(workflow.id)
        })
        .collect::<Vec<_>>();
    let retained_route = workflows
        .iter()
        .filter(|workflow| retained.contains(&workflow.id))
        .filter_map(|workflow| workflow.config::<MiningCampaignIntent>().ok())
        .flat_map(|intent| intent.transport_routes)
        .find(|route| route.deliver == region.hub_location.as_deref().unwrap_or_default());
    if !enabled {
        return Ok(summary(
            DirectorGoalStatus::Waiting,
            None,
            Some(disabled_next.to_owned()),
            0,
            retained.len(),
            protocol_workflow_ids(&retained),
        ));
    }
    if !retained.is_empty() {
        return Ok(summary(
            DirectorGoalStatus::Active,
            None,
            Some(retained_route.as_ref().map_or_else(
                || "Continue active AMI transport service provisioning".to_owned(),
                |route| {
                    format!(
                        "Wait for existing durable work to establish AMI transport service from {} to {}",
                        route.collect, route.deliver
                    )
                },
            )),
            0,
            retained.len(),
            protocol_workflow_ids(&retained),
        ));
    }
    if !complete_authority {
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some("Managed resource and device authority is incomplete".to_owned()),
            Some(
                "Complete managed synchronization before reconciling AMI transport coverage"
                    .to_owned(),
            ),
            0,
            0,
            Vec::new(),
        ));
    }
    let mut positive_location_count = 0usize;
    let mut unknown_location_count = 0usize;
    let mut seen_stock = BTreeSet::new();
    for inventory in inventories {
        let InventoryOwner::Location(owner) = &inventory.owner else {
            continue;
        };
        let collect = owner.id.as_str();
        if !inventory.items.iter().any(|item| item.quantity > 0)
            || !seen_stock.insert(collect.to_owned())
        {
            continue;
        }
        let Some(location) = locations
            .iter()
            .find(|location| location.key.id == owner.id)
        else {
            unknown_location_count += 1;
            continue;
        };
        match location.location_type.as_ref() {
            None => {
                unknown_location_count += 1;
                continue;
            }
            Some(LocationType::Belt) => {}
            Some(_) => continue,
        }
        let Some(system) = location.system.as_deref() else {
            unknown_location_count += 1;
            continue;
        };
        match system_regions.get(system) {
            Some(mapped) if mapped != &region.region => continue,
            None => {
                unknown_location_count += 1;
                continue;
            }
            Some(_) => {}
        }
        match crate::mining::resource_present_with_authority(
            devices,
            locations,
            inventories,
            collect,
            complete_authority,
        ) {
            crate::mining::EvidenceState::Present => positive_location_count += 1,
            crate::mining::EvidenceState::Unknown => unknown_location_count += 1,
            crate::mining::EvidenceState::Absent => {}
        }
    }
    if unknown_location_count > 0 {
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some("Managed resource evidence is incomplete for AMI transport".to_owned()),
            Some(
                "Complete managed resource authority before reconciling AMI transport coverage"
                    .to_owned(),
            ),
            0,
            positive_location_count + unknown_location_count,
            Vec::new(),
        ));
    }
    if positive_location_count == 0 {
        return Ok(summary(
            DirectorGoalStatus::Satisfied,
            None,
            Some("Wait for newly producing regional resources".to_owned()),
            0,
            0,
            protocol_workflow_ids(&retained),
        ));
    }
    let Some(hub) = region
        .hub_location
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some(missing_home.to_owned()),
            Some(missing_home.to_owned()),
            0,
            positive_location_count,
            protocol_workflow_ids(&retained),
        ));
    };

    let mut candidates = Vec::new();
    let mut seen = BTreeSet::new();
    for inventory in inventories {
        let InventoryOwner::Location(owner) = &inventory.owner else {
            continue;
        };
        let collect = owner.id.as_str();
        if !inventory.items.iter().any(|item| item.quantity > 0) || !seen.insert(collect.to_owned())
        {
            continue;
        }
        let Some(system) = locations
            .iter()
            .find(|location| location.key.id.as_str() == collect)
            .and_then(|location| location.system.clone())
        else {
            candidates.push(UnservicedRouteCandidate {
                route: crate::mining::AmiTransportRouteIntent {
                    system: String::new(),
                    collect: collect.to_owned(),
                    deliver: hub.to_owned(),
                },
                resource_state: crate::mining::EvidenceState::Unknown,
            });
            continue;
        };
        let resource_state = if !system_regions.contains_key(&system)
            || system_regions.get(&system) != Some(&region.region)
        {
            if system_regions
                .get(&system)
                .is_some_and(|mapped| mapped != &region.region)
            {
                crate::mining::EvidenceState::Absent
            } else {
                crate::mining::EvidenceState::Unknown
            }
        } else {
            crate::mining::resource_present_with_authority(
                devices,
                locations,
                inventories,
                collect,
                complete_authority,
            )
        };
        if collect == hub {
            continue;
        }
        if resource_state != crate::mining::EvidenceState::Absent {
            candidates.push(UnservicedRouteCandidate {
                route: crate::mining::AmiTransportRouteIntent {
                    system,
                    collect: collect.to_owned(),
                    deliver: hub.to_owned(),
                },
                resource_state,
            });
        }
    }
    candidates.sort_by(|left, right| left.route.cmp(&right.route));
    let total = candidates.len() + retained.len();
    if candidates
        .iter()
        .any(|candidate| candidate.resource_state == crate::mining::EvidenceState::Unknown)
    {
        return Ok(summary(
            DirectorGoalStatus::Blocked,
            Some("Managed resource evidence is incomplete for AMI transport".to_owned()),
            Some(
                "Complete managed resource authority before reconciling AMI transport coverage"
                    .to_owned(),
            ),
            0,
            total,
            Vec::new(),
        ));
    }
    if candidates.is_empty() {
        return Ok(summary(
            DirectorGoalStatus::Satisfied,
            None,
            Some("Wait for newly producing regional resources".to_owned()),
            0,
            0,
            Vec::new(),
        ));
    }
    let mut pending_route = None;
    let mut covered = 0usize;
    let mut pending = Vec::new();
    let mut uncovered = Vec::new();
    for candidate in &candidates {
        let route = &candidate.route;
        let service = if route.collect == route.deliver {
            crate::mining::EvidenceState::Present
        } else {
            crate::mining::transport_service_present(
                devices,
                &route.system,
                &route.collect,
                &route.deliver,
            )
            .state
        };
        if service == crate::mining::EvidenceState::Unknown {
            return Ok(summary(
                DirectorGoalStatus::Blocked,
                Some(format!(
                    "Durable workflow intent is incomplete for AMI transport from {} to {}",
                    route.collect, route.deliver
                )),
                Some(format!(
                    "Durable workflow intent is incomplete for AMI transport from {} to {}",
                    route.collect, route.deliver
                )),
                covered,
                total,
                Vec::new(),
            ));
        }
        if service == crate::mining::EvidenceState::Present {
            covered += 1;
            continue;
        }
        let target = route.workflow_service_intent();
        match service_snapshot.state_for(&target, Some(&region.region), Some(&route.system)) {
            WorkflowServiceIntentState::Present(ids) => {
                pending.extend(ids);
                if pending_route.is_none() {
                    pending_route = Some(route);
                }
            }
            WorkflowServiceIntentState::Unknown(_) => {
                return Ok(summary(
                    DirectorGoalStatus::Blocked,
                    Some(format!(
                        "Durable workflow intent is incomplete for AMI transport from {} to {}",
                        route.collect, route.deliver
                    )),
                    Some(format!(
                        "Complete durable workflow intent for AMI transport from {} to {}",
                        route.collect, route.deliver
                    )),
                    covered,
                    total,
                    Vec::new(),
                ));
            }
            WorkflowServiceIntentState::Absent => uncovered.push(candidate),
        }
    }
    pending.sort();
    pending.dedup();
    if !pending.is_empty() {
        return Ok(summary(
            DirectorGoalStatus::Active,
            None,
            Some(format!(
                "Wait for existing durable work to establish AMI transport service from {} to {}",
                pending_route
                    .map(|route| route.collect.as_str())
                    .unwrap_or(""),
                pending_route
                    .map(|route| route.deliver.as_str())
                    .unwrap_or(""),
            )),
            covered,
            total,
            protocol_workflow_ids(&pending),
        ));
    }
    if uncovered.is_empty() {
        return Ok(summary(
            DirectorGoalStatus::Satisfied,
            None,
            Some("Maintain AMI transport coverage for producing regional resources".to_owned()),
            covered,
            total,
            Vec::new(),
        ));
    }
    let candidate = uncovered[0];
    let route = candidate.route.clone();
    let next_action = format!(
        "Establish AMI {} service from {} to {}",
        if same_system(&route.system, &route.deliver) {
            "shuttle"
        } else {
            "ferry"
        },
        route.collect,
        route.deliver
    );
    if !context.automatic {
        return Ok(summary(
            DirectorGoalStatus::Active,
            None,
            Some(next_action),
            covered,
            total,
            Vec::new(),
        ));
    }
    if !*launch_available {
        return Ok(summary(
            DirectorGoalStatus::Active,
            None,
            Some(format!("{next_action} on the next Director pass")),
            covered,
            total,
            Vec::new(),
        ));
    }
    let target = route.workflow_service_intent();
    let intent = MiningCampaignIntent {
        systems: vec![route.system.clone()],
        region: region.region.clone(),
        hub: route.deliver.clone(),
        max_concurrency: 1,
        transport_routes: vec![route.clone()],
    };
    let created = context.repository.create_or_reuse_active(
        new_mining_campaign_workflow(intent),
        |instance| match workflow_registry.service_intent_state_for_instance(
            instance,
            &target,
            Some(&region.region),
            Some(&route.system),
        ) {
            WorkflowServiceIntentState::Present(_) => Ok(true),
            WorkflowServiceIntentState::Absent => Ok(false),
            WorkflowServiceIntentState::Unknown(_) => Err(RepositoryError::Compatibility(format!(
                "AMI transport intent is unknown for {} to {}",
                route.collect, route.deliver
            ))),
        },
    );
    match created {
        Ok(result) => {
            if result.created {
                *launch_available = false;
            }
            runtime.active_workflows = vec![result.instance.id];
            runtime.last_launch_at_ms = Some(context.now);
            save_goal_runtime(context.repository, &id, &runtime)?;
            Ok(summary(
                DirectorGoalStatus::Active,
                None,
                Some(next_action),
                covered,
                total,
                vec![ProtocolWorkflowId(result.instance.id.to_string())],
            ))
        }
        Err(error) => Ok(summary(
            DirectorGoalStatus::Blocked,
            Some(error.to_string()),
            Some(next_action),
            covered,
            total,
            Vec::new(),
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn reconcile_expand_mining(
    repository: &WorkflowRepository,
    region: &RegionView,
    _workers: &[WorkerView],
    workflows: &[WorkflowInstance],
    devices: &[Device],
    catalogue: &[Star],
    locations: &[Location],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
    controls: &GoalControls,
    automatic: bool,
    _reserved: &mut BTreeSet<String>,
    requirements: &mut DirectorRequirementGraph,
    now: i64,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::ExpandMiningOps;
    let enabled = goal_enabled(controls, kind, Some(&region.region));
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(repository, &id)?;
    prune_runtime_workflows(&mut runtime, workflows);

    let policy = mining_expansion_policy(repository, &region.region)?;
    let belt_systems =
        known_belt_systems(locations, location_systems, system_regions, &region.region);
    let belt_density = belt_density_priorities(locations, location_systems);
    let belt_designations = known_belt_designations(locations, location_systems);

    // The regional mining footprint is not capped by System Ward availability.
    // Every known belt system within the mining radius of the regional hub and
    // allowed by the regional density policy is a mining target. Already-managed
    // sites within that radius remain managed even if their density class is
    // later disabled.
    //
    // System Wards are a separate follow-up hardening policy: select at most
    // four non-hub mining systems from that same in-range footprint by density
    // (dense > moderate > sparse), then by distance from the regional hub. Owned
    // System Hubs already provide protection and therefore do not consume one of
    // those four ward slots.
    let managed_systems = managed_mining_systems(devices, location_systems)
        .intersection(&belt_systems)
        .cloned()
        .collect::<BTreeSet<_>>();
    let hub_systems = owned_mining_hub_systems(devices, catalogue, location_systems);
    let mining_hub_system = region.hub_system.as_deref().or_else(|| {
        region
            .hub_location
            .as_deref()
            .and_then(|location| location_systems.get(location).map(String::as_str))
    });
    let desired_systems = desired_mining_systems(
        &belt_systems,
        &managed_systems,
        &belt_density,
        mining_hub_system,
        catalogue,
        policy,
    );
    let selected_ward_systems = selected_mining_ward_systems(
        &belt_systems,
        &managed_systems,
        &hub_systems,
        &belt_density,
        mining_hub_system,
        catalogue,
        policy,
    );

    let healthy_systems = desired_systems
        .iter()
        .filter(|system| {
            let Some(belts) = belt_designations.get(*system) else {
                return false;
            };
            belts
                .iter()
                .any(|belt| crate::mining::audit_site(devices, system, belt).operational)
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let pending = desired_systems
        .difference(&healthy_systems)
        .cloned()
        .collect::<BTreeSet<_>>();

    let relay_systems = relay_device_systems(devices, location_systems);
    let disconnected = pending
        .difference(&relay_systems)
        .cloned()
        .collect::<BTreeSet<_>>();
    let connectivity_targets =
        prioritized_mining_repair_targets(&disconnected, &managed_systems, &belt_density);
    if enabled {
        for target in connectivity_targets.iter().take(MINING_BATCH_SIZE) {
            requirements.raise(
                DirectorRequirement::Connectivity {
                    region: region.region.clone(),
                    target_system: target.clone(),
                },
                &id,
                format!(
                    "Mining expansion or repair in {target} requires active regional FTL coverage"
                ),
                PRIORITY_FTL_EXPANSION,
            )?;
        }
    }

    // Ward allocation is intentionally dynamic, but protection is follow-up
    // hardening. Never delay a missing mining stack just to move a ward. Once
    // the selected mining footprint is operational, a redundant/displaced ward
    // can be relocated to a preferred target. Never strip protection from the
    // donor until the destination itself is relay-reachable.
    let relocations = plan_mining_ward_relocations(MiningWardRebalanceContext {
        devices,
        managed_systems: &managed_systems,
        selected_ward_systems: &selected_ward_systems,
        relay_systems: &relay_systems,
        hub_systems: &hub_systems,
        belt_density: &belt_density,
        belt_designations: &belt_designations,
        location_systems,
    });

    let covered = healthy_systems.len();
    let active = nonterminal_ids(&runtime, workflows);
    let recently_launched = launch_is_recent(&runtime, now, DEFAULT_RETRY_COOLDOWN_MS);
    let mut blocker = None;
    let next_action;

    let status = if !enabled {
        next_action =
            Some("Enable this standing goal to reconcile regional mining sites".to_owned());
        DirectorGoalStatus::Waiting
    } else if !active.is_empty() {
        next_action = Some(
            "Continue the active regional mining repair, expansion, or ward-rebalance workflow"
                .to_owned(),
        );
        DirectorGoalStatus::Active
    } else if recently_launched {
        next_action = Some(
            "Wait briefly before replanning the next mining or ward-rebalance action".to_owned(),
        );
        DirectorGoalStatus::Waiting
    } else if pending.is_empty()
        && let Some(relocation) = relocations.first()
    {
        next_action = Some(format!(
            "Move System Ward {} from {} to {} so the higher-priority mining belt is protected",
            relocation.ward_code, relocation.source_system, relocation.target_system
        ));
        if automatic {
            let mut pre_deactivate_device_codes = relocation.pause_devices.clone();
            if !pre_deactivate_device_codes.contains(&relocation.ward_code) {
                // Keep the ward last: displaced mining/survey controllers must
                // be quiesced before the system protection is removed.
                pre_deactivate_device_codes.push(relocation.ward_code.clone());
            }
            let workflow =
                repository.create(new_logistics_manifest_workflow(LogisticsManifestIntent {
                    origin: relocation.origin.clone(),
                    destination: relocation.target_belt.clone(),
                    resources: ResourceMap::new(),
                    devices: Vec::new(),
                    device_codes: vec![relocation.ward_code.clone()],
                    device_tags: Vec::new(),
                    pre_deactivate_device_codes,
                    release_mining_reservations: true,
                    placement_recovery: None,
                    return_transports: relocation.origin.contains('-'),
                    allow_transport_staging: true,
                    region: Some(region.region.clone()),
                    purpose: mining_ward_relocation_purpose(
                        &region.region,
                        &relocation.ward_code,
                        &relocation.target_system,
                    ),
                }))?;
            tracing::info!(
                workflow_id = %workflow.id,
                region = %region.region,
                ward = %relocation.ward_code,
                source_system = %relocation.source_system,
                target_system = %relocation.target_system,
                "Director launched mining System Ward rebalance"
            );
            runtime.active_workflows = vec![workflow.id];
            runtime.last_launch_at_ms = Some(now);
        }
        DirectorGoalStatus::Active
    } else if pending.is_empty() {
        next_action = Some(format!(
            "Maintain all eligible regional mining belts within {MINING_EXPANSION_RADIUS_LY:.0} LY of the regional hub; keep up to {MINING_WARD_SITES_PER_REGION} priority non-hub mining systems protected with System Wards"
        ));
        DirectorGoalStatus::Satisfied
    } else {
        // System Ward availability must never prevent the base mining stack
        // from coming online. Protection is reconciled opportunistically after
        // the selected sites are operational.
        let targets = relay_connected_mining_targets(
            &pending,
            &relay_systems,
            &managed_systems,
            &belt_density,
        );

        if targets.is_empty() && !disconnected.is_empty() {
            blocker = Some(format!(
                "{} mining system(s) needing deployment or repair require active FTL coverage",
                disconnected.len()
            ));
            next_action = Some(
                "Extend regional FTL coverage to the highest-priority mining systems".to_owned(),
            );
            DirectorGoalStatus::Blocked
        } else if targets.is_empty() {
            blocker = Some("No pending mining site is currently actionable".to_owned());
            next_action = Some("Wait for managed state to refresh before replanning".to_owned());
            DirectorGoalStatus::Waiting
        } else {
            next_action = Some(format!(
                "Deploy or repair mining stacks in {} reachable system(s) as one regional campaign",
                targets.len()
            ));
            if automatic
                && let Some(hub) = region
                    .hub_location
                    .clone()
                    .or_else(|| region.hub_system.clone())
            {
                let transport_routes = region
                    .hub_location
                    .as_ref()
                    .filter(|location| !location.trim().is_empty())
                    .map(|deliver| {
                        targets
                            .iter()
                            .filter_map(|system| {
                                belt_designations
                                    .get(system)
                                    .and_then(|belts| belts.first())
                                    .map(|collect| crate::mining::AmiTransportRouteIntent {
                                        system: system.clone(),
                                        collect: collect.clone(),
                                        deliver: deliver.clone(),
                                    })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let workflow =
                    repository.create(new_mining_campaign_workflow(MiningCampaignIntent {
                        systems: targets,
                        region: region.region.clone(),
                        hub,
                        max_concurrency: 4,
                        transport_routes,
                    }))?;
                tracing::info!(
                    workflow_id = %workflow.id,
                    region = %region.region,
                    "Director launched regional mining expansion/repair campaign"
                );
                runtime.active_workflows = vec![workflow.id];
                runtime.last_launch_at_ms = Some(now);
            }
            DirectorGoalStatus::Active
        }
    };
    save_goal_runtime(repository, &id, &runtime)?;

    let mut density_scope = vec!["dense"];
    if policy.expand_moderate {
        density_scope.push("moderate");
    }
    if policy.expand_sparse {
        density_scope.push("sparse");
    }
    Ok(DirectorGoalSummary {
        id,
        kind,
        region: Some(region.region.clone()),
        status,
        objective: format!(
            "Expand mining across eligible belts within {MINING_EXPANSION_RADIUS_LY:.0} LY of the regional hub in {} ({}); harden up to {MINING_WARD_SITES_PER_REGION} priority non-hub mining systems with System Wards",
            region.region,
            density_scope.join(", ")
        ),
        blocker,
        next_action,
        progress_current: covered as u64,
        progress_total: desired_systems.len() as u64,
        active_workflows: protocol_workflow_ids(&runtime.active_workflows),
        enabled,
    })
}

fn managed_mining_systems(
    devices: &[Device],
    location_systems: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    devices
        .iter()
        .filter(|device| {
            device.device_type.as_ref().is_some_and(|device_type| {
                matches!(
                    device_type,
                    DeviceType::MiningController
                        | DeviceType::MiningDrone
                        | DeviceType::SurveyController
                        | DeviceType::SurveyDrone
                ) || (device_type == &DeviceType::MaintenanceDrone
                    && device.tags.iter().any(|tag| tag.starts_with("mine-s:")))
            })
        })
        .filter_map(|device| device_system(device, location_systems))
        .collect()
}

fn mining_density_allowed(priority: u8, policy: MiningExpansionPolicy) -> bool {
    match priority {
        3.. => true,
        2 => policy.expand_moderate,
        1 => policy.expand_sparse,
        _ => false,
    }
}

fn desired_mining_systems(
    belt_systems: &BTreeSet<String>,
    managed_systems: &BTreeSet<String>,
    belt_density: &BTreeMap<String, u8>,
    hub_system: Option<&str>,
    catalogue: &[Star],
    policy: MiningExpansionPolicy,
) -> BTreeSet<String> {
    let positions = catalogue
        .iter()
        .filter_map(|star| {
            star.position
                .map(|position| (star.key.id.as_str().to_owned(), position))
        })
        .collect::<BTreeMap<_, _>>();
    let hub_position = hub_system.and_then(|system| positions.get(system).copied());

    belt_systems
        .iter()
        .filter(|system| {
            let density_allowed = managed_systems.contains(*system)
                || mining_density_allowed(
                    belt_density.get(*system).copied().unwrap_or_default(),
                    policy,
                );
            if !density_allowed {
                return false;
            }
            if hub_system == Some(system.as_str()) {
                return true;
            }

            match (hub_position, positions.get(*system).copied()) {
                (Some(hub), Some(target)) => {
                    galactic_distance(hub, target) <= MINING_EXPANSION_RADIUS_LY
                }
                // Incomplete catalogue coordinates should not cause an already-managed
                // mining site to fall out of management, but they must not permit new
                // expansion whose range cannot be verified.
                _ => managed_systems.contains(*system),
            }
        })
        .cloned()
        .collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MiningWardRelocation {
    ward_code: String,
    source_system: String,
    origin: String,
    target_system: String,
    target_belt: String,
    pause_devices: Vec<String>,
}

fn selected_mining_ward_systems(
    belt_systems: &BTreeSet<String>,
    managed_systems: &BTreeSet<String>,
    hub_systems: &BTreeSet<String>,
    belt_density: &BTreeMap<String, u8>,
    hub_system: Option<&str>,
    catalogue: &[Star],
    policy: MiningExpansionPolicy,
) -> BTreeSet<String> {
    let eligible_mining_systems = desired_mining_systems(
        belt_systems,
        managed_systems,
        belt_density,
        hub_system,
        catalogue,
        policy,
    );
    let mut candidates = eligible_mining_systems
        .difference(hub_systems)
        .cloned()
        .collect::<Vec<_>>();
    let positions = catalogue
        .iter()
        .filter_map(|star| {
            star.position
                .map(|position| (star.key.id.as_str().to_owned(), position))
        })
        .collect::<BTreeMap<_, _>>();
    let hub_position = hub_system.and_then(|system| positions.get(system).copied());
    let distance = |system: &str| {
        hub_position
            .zip(positions.get(system).copied())
            .map(|(hub, target)| galactic_distance(hub, target))
    };
    candidates.sort_by(|left, right| {
        belt_density
            .get(right)
            .copied()
            .unwrap_or_default()
            .cmp(&belt_density.get(left).copied().unwrap_or_default())
            .then_with(|| match (distance(left), distance(right)) {
                (Some(left_distance), Some(right_distance)) => {
                    left_distance.total_cmp(&right_distance)
                }
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
            .then_with(|| {
                managed_systems
                    .contains(right)
                    .cmp(&managed_systems.contains(left))
            })
            .then_with(|| left.cmp(right))
    });
    candidates
        .into_iter()
        .take(MINING_WARD_SITES_PER_REGION)
        .collect()
}

fn owned_mining_hub_systems(
    devices: &[Device],
    catalogue: &[Star],
    location_systems: &BTreeMap<String, String>,
) -> BTreeSet<String> {
    catalogue
        .iter()
        .filter(|star| star.has_hub == Some(true))
        .filter_map(|star| {
            let system = star.key.id.as_str();
            devices
                .iter()
                .any(|device| {
                    device.device_type.as_ref() == Some(&DeviceType::SystemHub)
                        && device_system(device, location_systems).as_deref() == Some(system)
                })
                .then_some(system.to_owned())
        })
        .collect()
}

fn owned_mining_wards_by_system(
    devices: &[Device],
    location_systems: &BTreeMap<String, String>,
) -> BTreeMap<String, Vec<(String, String)>> {
    let mut wards = BTreeMap::<String, Vec<(String, String)>>::new();
    for device in devices.iter().filter(|device| {
        device.device_type.as_ref() == Some(&DeviceType::SystemWard)
            && device.relationships.attached_to.is_none()
            && device.relationships.stowed_in.is_none()
            && device.travel.is_none()
    }) {
        let Some(system) = device_system(device, location_systems) else {
            continue;
        };
        let Some(location) = device.location.as_ref() else {
            continue;
        };
        wards.entry(system).or_default().push((
            device.key.id.as_str().to_owned(),
            location.id.as_str().to_owned(),
        ));
    }
    for devices in wards.values_mut() {
        devices.sort();
    }
    wards
}

fn mining_site_pause_devices(
    devices: &[Device],
    system: &str,
    belt_designations: &BTreeMap<String, Vec<String>>,
) -> Vec<String> {
    let Some(belt) = belt_designations
        .get(system)
        .and_then(|belts| belts.first())
    else {
        return Vec::new();
    };
    let audit = crate::mining::audit_site(devices, system, belt);
    let mut controllers = audit
        .assets
        .mining_controller
        .into_iter()
        .chain(audit.assets.survey_controller)
        .collect::<Vec<_>>();
    controllers.sort();
    controllers.dedup();
    controllers
}

struct MiningWardRebalanceContext<'a> {
    devices: &'a [Device],
    managed_systems: &'a BTreeSet<String>,
    selected_ward_systems: &'a BTreeSet<String>,
    relay_systems: &'a BTreeSet<String>,
    hub_systems: &'a BTreeSet<String>,
    belt_density: &'a BTreeMap<String, u8>,
    belt_designations: &'a BTreeMap<String, Vec<String>>,
    location_systems: &'a BTreeMap<String, String>,
}

fn plan_mining_ward_relocations(
    context: MiningWardRebalanceContext<'_>,
) -> Vec<MiningWardRelocation> {
    let wards = owned_mining_wards_by_system(context.devices, context.location_systems);
    let warded_systems = wards.keys().cloned().collect::<BTreeSet<_>>();
    let mut targets = context
        .selected_ward_systems
        .difference(&warded_systems)
        .filter(|system| context.relay_systems.contains(*system))
        .filter_map(|system| {
            context
                .belt_designations
                .get(system)
                .and_then(|belts| belts.first())
                .map(|belt| (system.clone(), belt.clone()))
        })
        .collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        context
            .belt_density
            .get(&right.0)
            .copied()
            .unwrap_or_default()
            .cmp(
                &context
                    .belt_density
                    .get(&left.0)
                    .copied()
                    .unwrap_or_default(),
            )
            .then_with(|| left.0.cmp(&right.0))
    });

    // Only repurpose wards from mining-managed systems. A System Hub in an
    // unrelated system must not cause the mining Director to steal a ward that
    // belongs to some other standing goal or manual setup. Hub-protected
    // mining systems are preferred donors because moving their ward does not
    // interrupt mining there. Among remaining donors, lower-density sites give
    // up their wards first.
    let donor_systems = context
        .managed_systems
        .difference(context.selected_ward_systems)
        .filter(|system| context.relay_systems.contains(*system))
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut donors = donor_systems
        .iter()
        .flat_map(|system| {
            wards
                .get(system)
                .into_iter()
                .flatten()
                .map(move |(code, origin)| (system.clone(), code.clone(), origin.clone()))
        })
        .collect::<Vec<_>>();
    donors.sort_by(|left, right| {
        context
            .hub_systems
            .contains(&right.0)
            .cmp(&context.hub_systems.contains(&left.0))
            .then_with(|| {
                context
                    .belt_density
                    .get(&left.0)
                    .copied()
                    .unwrap_or_default()
                    .cmp(
                        &context
                            .belt_density
                            .get(&right.0)
                            .copied()
                            .unwrap_or_default(),
                    )
            })
            .then_with(|| left.0.cmp(&right.0))
            .then_with(|| left.1.cmp(&right.1))
    });

    donors
        .into_iter()
        .zip(targets)
        .map(
            |((source_system, ward_code, origin), (target_system, target_belt))| {
                let pause_devices = if context.hub_systems.contains(&source_system) {
                    Vec::new()
                } else {
                    mining_site_pause_devices(
                        context.devices,
                        &source_system,
                        context.belt_designations,
                    )
                };
                MiningWardRelocation {
                    ward_code,
                    source_system,
                    origin,
                    target_system,
                    target_belt,
                    pause_devices,
                }
            },
        )
        .collect()
}

fn mining_ward_relocation_purpose(region: &str, ward: &str, target_system: &str) -> String {
    format!(
        "director:expand_mining_ops:ward-rebalance:{}:{}:{}",
        canonical_region(region),
        ward.to_ascii_lowercase(),
        target_system.to_ascii_uppercase()
    )
}

fn known_belt_designations(
    locations: &[Location],
    location_systems: &BTreeMap<String, String>,
) -> BTreeMap<String, Vec<String>> {
    let mut choices = BTreeMap::<String, Vec<(u8, String)>>::new();
    for location in locations {
        let Some(system) = location
            .system
            .clone()
            .or_else(|| location_systems.get(location.key.id.as_str()).cloned())
        else {
            continue;
        };

        let mut candidates = Vec::<(u8, String)>::new();
        if location
            .location_type
            .as_ref()
            .is_some_and(|kind| kind.as_str() == "belt")
        {
            let density = ["belt", "asteroid_belt"]
                .iter()
                .filter_map(|field| location.unknown.get(*field))
                .map(belt_density_priority)
                .max()
                .unwrap_or_default();
            candidates.push((density, location.key.id.as_str().to_owned()));
        }
        if let Some(value) = location.unknown.get("asteroid_belt") {
            let belts = value
                .get("belts")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_else(|| std::slice::from_ref(value));
            for belt in belts {
                let Some(designation) = belt.get("designation").and_then(Value::as_str) else {
                    continue;
                };
                let density = belt
                    .get("density")
                    .and_then(Value::as_str)
                    .map(belt_density_name_priority)
                    .unwrap_or_default();
                candidates.push((density, designation.to_owned()));
            }
        }
        choices.entry(system).or_default().extend(candidates);
    }

    choices
        .into_iter()
        .map(|(system, mut belts)| {
            belts.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
            belts.dedup_by(|left, right| left.1 == right.1);
            (
                system,
                belts
                    .into_iter()
                    .map(|(_, designation)| designation)
                    .collect(),
            )
        })
        .collect()
}

fn relay_connected_mining_targets(
    pending_systems: &BTreeSet<String>,
    relay_systems: &BTreeSet<String>,
    managed_systems: &BTreeSet<String>,
    belt_density: &BTreeMap<String, u8>,
) -> Vec<String> {
    let connected = pending_systems
        .intersection(relay_systems)
        .cloned()
        .collect::<BTreeSet<_>>();
    prioritized_mining_repair_targets(&connected, managed_systems, belt_density)
        .into_iter()
        .take(MINING_BATCH_SIZE)
        .collect()
}

fn prioritized_mining_repair_targets(
    systems: &BTreeSet<String>,
    managed_systems: &BTreeSet<String>,
    belt_density: &BTreeMap<String, u8>,
) -> Vec<String> {
    let mut targets = systems.iter().cloned().collect::<Vec<_>>();
    targets.sort_by(|left, right| {
        managed_systems
            .contains(right)
            .cmp(&managed_systems.contains(left))
            .then_with(|| {
                belt_density
                    .get(right)
                    .copied()
                    .unwrap_or_default()
                    .cmp(&belt_density.get(left).copied().unwrap_or_default())
            })
            .then_with(|| left.cmp(right))
    });
    targets
}

fn known_belt_systems(
    locations: &[Location],
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
    region: &str,
) -> BTreeSet<String> {
    locations
        .iter()
        .filter(|location| location_has_known_belt(location))
        .filter_map(|location| {
            location
                .system
                .clone()
                .or_else(|| location_systems.get(location.key.id.as_str()).cloned())
        })
        .filter(|system| {
            system_regions
                .get(system)
                .is_some_and(|candidate| candidate == region)
        })
        .collect()
}

fn location_has_known_belt(location: &Location) -> bool {
    if location
        .location_type
        .as_ref()
        .is_some_and(|kind| kind.as_str() == "belt")
    {
        return true;
    }
    ["belt", "asteroid_belt"]
        .iter()
        .filter_map(|field| location.unknown.get(*field))
        .any(|value| {
            value
                .get("belts")
                .and_then(Value::as_array)
                .is_some_and(|belts| !belts.is_empty())
                || value.get("present").and_then(Value::as_bool) == Some(true)
                || value.get("density").and_then(Value::as_str).is_some()
        })
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
    catalogue_positions: &BTreeMap<String, GalacticPosition>,
    event_systems: &BTreeSet<String>,
) -> Result<DirectorGoalSummary, ApplicationError> {
    let kind = DirectorGoalKind::ExpandFtlNetwork;
    let enabled = goal_enabled(context.controls, kind, Some(&region.region));
    let id = goal_instance_id(kind, Some(&region.region));
    let mut runtime = load_goal_runtime(context.repository, &id)?;
    prune_runtime_workflows(&mut runtime, context.workflows);

    let relay_systems = relay_device_systems(devices, location_systems);
    let belt_density = belt_density_priorities(locations, location_systems);
    let connectivity_requirements = requirements.current_connectivity_requirements(&region.region);
    let expansion_scope =
        regional_system_scope_from_hub(region, catalogue_positions, REGIONAL_AUTOMATION_RADIUS_LY);
    let empty_systems = BTreeSet::new();
    let (scoped_systems, missing_positions) = expansion_scope
        .as_ref()
        .map(|scope| (&scope.systems, scope.missing_positions))
        .unwrap_or((&empty_systems, 0));
    let covered = scoped_systems.intersection(&relay_systems).count();
    let mut uncovered = scoped_systems
        .iter()
        .filter(|system| !relay_systems.contains(*system))
        .map(|system| {
            let event_priority = event_systems.contains(system);
            let density_priority = belt_density.get(system).copied().unwrap_or_default();
            let connectivity_priority = connectivity_requirements
                .get(system)
                .map(|(_, priority)| *priority)
                .unwrap_or_default();
            let score = ftl_priority_score(connectivity_priority, event_priority, density_priority);
            (
                system.clone(),
                score,
                connectivity_priority,
                event_priority,
                density_priority,
            )
        })
        .collect::<Vec<_>>();
    uncovered.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let identity = uncovered
        .first()
        .map(|(target, ..)| GoalWorkIdentity::Exploration {
            target: target.clone(),
        });
    let permanent_failure = if let Some(identity) = identity.as_ref() {
        retain_work_identity(&mut runtime, identity);
        permanent_failure_for_identity(&runtime, context.workflows, identity)
    } else {
        runtime.launch_records.clear();
        None
    };

    let mut active = nonterminal_ids(&runtime, context.workflows);
    let recently_launched = launch_is_recent(&runtime, context.now, DEFAULT_RETRY_COOLDOWN_MS);
    let mut blocker = None;
    let (status, next_action) = if !enabled {
        (
            DirectorGoalStatus::Waiting,
            Some("Enable this standing goal to extend regional FTL coverage".to_owned()),
        )
    } else if expansion_scope.is_none() {
        blocker = Some(regional_radius_resolution_blocker(
            region,
            REGIONAL_AUTOMATION_RADIUS_LY,
            "FTL-expansion",
        ));
        (
            DirectorGoalStatus::Blocked,
            Some(
                "Resolve the regional hub and catalogue position before extending FTL coverage"
                    .to_owned(),
            ),
        )
    } else if uncovered.is_empty() && missing_positions == 0 {
        (
            DirectorGoalStatus::Satisfied,
            Some(format!(
                "Wait for newly discovered systems within {:.0} LY of the regional hub or new strategic demand",
                REGIONAL_AUTOMATION_RADIUS_LY
            )),
        )
    } else if uncovered.is_empty() {
        blocker = Some(format!(
            "{missing_positions} known system(s) in {} lack catalogue positions, so their inclusion in the {:.0} LY FTL footprint cannot yet be determined",
            region.region, REGIONAL_AUTOMATION_RADIUS_LY
        ));
        (
            DirectorGoalStatus::Blocked,
            Some("Wait for catalogue position data before extending FTL coverage".to_owned()),
        )
    } else if !active.is_empty() {
        (
            DirectorGoalStatus::Active,
            Some("Continue the active regional FTL expansion campaign".to_owned()),
        )
    } else {
        let (target, _, connectivity_priority, event_priority, density_priority) = &uncovered[0];
        let connectivity_requirement_id = connectivity_requirements
            .get(target)
            .map(|(requirement_id, _)| requirement_id.clone());
        if let Some(existing) = active_exploration_workflow_for_target(context.workflows, target)? {
            runtime.active_workflows = vec![existing];
            active = vec![existing];
            record_goal_launch(
                &mut runtime,
                existing,
                GoalWorkIdentity::Exploration {
                    target: target.clone(),
                },
            );
            if let Some(requirement_id) = connectivity_requirement_id.as_deref() {
                requirements.attach_workflow(requirement_id, existing)?;
            }
        }
        if !active.is_empty() {
            (
                DirectorGoalStatus::Active,
                Some(format!(
                    "Continue the existing FTL expansion toward {target}{}",
                    ftl_priority_suffix(*connectivity_priority, *event_priority, *density_priority,)
                )),
            )
        } else if let Some(failure) = permanent_failure {
            blocker = failure.last_error.clone();
            (
                DirectorGoalStatus::Blocked,
                Some(format!(
                    "Change the requested FTL target before replacing permanently failed work toward {target}"
                )),
            )
        } else if recently_launched {
            (
                DirectorGoalStatus::Waiting,
                Some(format!(
                    "Wait briefly before retrying FTL expansion toward {target}{}",
                    ftl_priority_suffix(*connectivity_priority, *event_priority, *density_priority,)
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
                        "Connect strategic systems within {:.0} LY of the regional hub in {}",
                        REGIONAL_AUTOMATION_RADIUS_LY, region.region
                    ),
                    blocker,
                    next_action,
                    progress_current: covered as u64,
                    progress_total: scoped_systems.len().saturating_add(missing_positions) as u64,
                    active_workflows: protocol_workflow_ids(&runtime.active_workflows),
                    enabled,
                });
            };
            let next_action = Some(format!(
                "Extend FTL coverage toward {target} with {worker}{}",
                ftl_priority_suffix(*connectivity_priority, *event_priority, *density_priority,)
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
                    connectivity_priority = *connectivity_priority,
                    event_priority = *event_priority,
                    belt_density_priority = *density_priority,
                    replicant = %worker,
                    "Director launched prioritized regional FTL expansion"
                );
                runtime.active_workflows = vec![workflow.id];
                runtime.last_launch_at_ms = Some(context.now);
                record_goal_launch(
                    &mut runtime,
                    workflow.id,
                    GoalWorkIdentity::Exploration {
                        target: target.clone(),
                    },
                );
                if let Some(requirement_id) = connectivity_requirement_id.as_deref() {
                    requirements.attach_workflow(requirement_id, workflow.id)?;
                }
                reserved.insert(worker);
            }
            (DirectorGoalStatus::Active, next_action)
        } else if regional_workers_in_transit(workers, &region.region, reserved) > 0 {
            blocker = Some("assigned regional workers are still in transit".to_owned());
            (
                DirectorGoalStatus::Waiting,
                Some("Wait for an assigned regional worker to arrive".to_owned()),
            )
        } else {
            let has_idle_racing_worker = workers.iter().any(|worker| {
                worker.region.as_deref() == Some(region.region.as_str())
                    && worker.busy_workflow.is_none()
                    && worker.state == WorkerState::Operational
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
            "Connect strategic systems within {:.0} LY of the regional hub in {}",
            REGIONAL_AUTOMATION_RADIUS_LY, region.region
        ),
        blocker,
        next_action,
        progress_current: covered as u64,
        progress_total: scoped_systems.len().saturating_add(missing_positions) as u64,
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
        .filter(|device| relay_device_is_active(device))
        .filter_map(|device| device_system(device, location_systems))
        .collect()
}

fn relay_device_is_active(device: &Device) -> bool {
    device.relationships.stowed_in.is_none()
        && device.relationships.attached_to.is_none()
        && device
            .status
            .as_ref()
            .is_some_and(|status| matches!(status.as_str(), "active" | "relaying"))
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

fn belt_density_name_priority(density: &str) -> u8 {
    match density.to_ascii_lowercase().as_str() {
        "dense" => 3,
        "moderate" => 2,
        "sparse" => 1,
        _ => 0,
    }
}

fn belt_density_priority(value: &Value) -> u8 {
    let rank = belt_density_name_priority;
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

fn ftl_priority_score(
    connectivity_priority: u32,
    event_priority: bool,
    density_priority: u8,
) -> u32 {
    let strategic_priority = connectivity_priority.max(if event_priority {
        PRIORITY_EVENT_COMPLETION
    } else {
        0
    });
    strategic_priority
        .saturating_mul(1_000)
        .saturating_add(u32::from(density_priority) * 80)
}

fn ftl_priority_suffix(
    connectivity_priority: u32,
    event_priority: bool,
    density_priority: u8,
) -> &'static str {
    match (connectivity_priority > 0, event_priority, density_priority) {
        (true, true, 3..) => " (goal dependency + active events + dense belt)",
        (true, true, 2) => " (goal dependency + active events + moderate belt)",
        (true, true, 1) => " (goal dependency + active events + sparse belt)",
        (true, true, _) => " (goal dependency + active events)",
        (true, false, 3..) => " (goal dependency + dense belt)",
        (true, false, 2) => " (goal dependency + moderate belt)",
        (true, false, 1) => " (goal dependency + sparse belt)",
        (true, false, _) => " (goal dependency)",
        (false, true, 3..) => " (active events + dense belt)",
        (false, true, 2) => " (active events + moderate belt)",
        (false, true, 1) => " (active events + sparse belt)",
        (false, true, _) => " (active events)",
        (false, false, 3..) => " (dense belt)",
        (false, false, 2) => " (moderate belt)",
        (false, false, 1) => " (sparse belt)",
        (false, false, _) => "",
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
            .filter(|worker| worker_near_home(worker, home))
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

fn manufacturing_home_candidates(
    region_name: &str,
    regions: &BTreeMap<String, RegionView>,
    manufacturing_homes: &BTreeMap<String, String>,
) -> Vec<ManufacturingHomeSelection> {
    let mut candidates = Vec::new();
    if let Some(location) = manufacturing_homes.get(region_name) {
        candidates.push(ManufacturingHomeSelection {
            location: location.clone(),
            reason: "owned Autofactory at the designated regional home".to_owned(),
            local: true,
        });
    }
    for (source_region, location) in manufacturing_homes {
        if source_region == region_name
            || regions
                .get(source_region)
                .is_none_or(|region| region.status != DirectorRegionStatus::Established)
            || candidates
                .iter()
                .any(|candidate| candidate.location == *location)
        {
            continue;
        }
        candidates.push(ManufacturingHomeSelection {
            location: location.clone(),
            reason: format!(
                "fallback to established {source_region} manufacturing because {region_name} has no usable local source"
            ),
            local: false,
        });
    }
    candidates
}

#[allow(clippy::too_many_arguments)]
fn reconcile_workforce(
    repository: &WorkflowRepository,
    settings: &DirectorSettings,
    regions: &BTreeMap<String, RegionView>,
    manufacturing_homes: &BTreeMap<String, String>,
    workers: &[WorkerView],
    workflows: &[WorkflowInstance],
    reserved: &BTreeSet<String>,
    demand: &BTreeMap<String, usize>,
    states: &mut BTreeMap<String, RegionWorkforceState>,
    automatic: bool,
    now: i64,
) -> Result<WorkforceReconciliation, ApplicationError> {
    let incoming = incoming_replicant_provisions(workflows)?;
    let mut result = WorkforceReconciliation::default();
    for region_name in regions.keys() {
        let pending = demand.get(region_name).copied().unwrap_or_default();
        let state = states.entry(region_name.clone()).or_default();
        let regional = workers
            .iter()
            .filter(|worker| worker.region.as_deref() == Some(region_name.as_str()))
            .collect::<Vec<_>>();
        let assigned = regional.len();
        let incoming_count = incoming.get(region_name).copied().unwrap_or_default();
        let bootstrap_population = assigned.saturating_add(incoming_count);
        let bootstrap_deficit = REGION_BOOTSTRAP_TARGET.saturating_sub(bootstrap_population);
        let operational = regional
            .iter()
            .filter(|worker| worker.state == WorkerState::Operational)
            .count();
        let in_transit = regional
            .iter()
            .filter(|worker| worker.state == WorkerState::InTransit)
            .count();
        let busy = regional
            .iter()
            .filter(|worker| worker.state == WorkerState::Busy)
            .count();
        let temporarily_unavailable = regional
            .iter()
            .filter(|worker| {
                matches!(worker.state, WorkerState::Busy | WorkerState::InTransit)
                    || reserved.contains(worker.replicant.key.id.as_str())
            })
            .count();
        let ordinary_shortfall = pending.saturating_sub(temporarily_unavailable);
        let idle = regional
            .iter()
            .filter(|worker| {
                worker.state == WorkerState::Operational
                    && worker.busy_workflow.is_none()
                    && !reserved.contains(worker.replicant.key.id.as_str())
            })
            .count();
        let idle_ratio = if assigned == 0 {
            0.0
        } else {
            idle as f64 / assigned as f64
        };
        let homes = manufacturing_home_candidates(region_name, regions, manufacturing_homes);
        let selected_home = homes.first().cloned();
        let mut diagnostic = DirectorRegionalWorkforceSummary {
            region: region_name.clone(),
            bootstrap_target: REGION_BOOTSTRAP_TARGET,
            assigned,
            incoming: incoming_count,
            operational,
            in_transit,
            busy,
            desired_ordinary_capacity: if bootstrap_deficit == 0 {
                assigned.saturating_add(ordinary_shortfall)
            } else {
                assigned
            },
            scale_up_suppressed: false,
            scale_up_suppression_reason: None,
            manufacturing_home: selected_home
                .as_ref()
                .map(|selection| selection.location.clone()),
            manufacturing_home_reason: selected_home.map(|selection| selection.reason),
        };

        if pending == 0 {
            state.pressure_since_ms = None;
            result.regions.push(diagnostic);
            continue;
        }

        if let Some(workflow_id) = state.provision_workflow_id {
            if let Some(workflow) = workflows.iter().find(|workflow| workflow.id == workflow_id) {
                if !workflow.status.is_terminal() {
                    state.pressure_since_ms = None;
                    diagnostic.scale_up_suppressed = true;
                    diagnostic.scale_up_suppression_reason =
                        Some("an existing Replicant provision is still in progress".to_owned());
                    result.regions.push(diagnostic);
                    continue;
                }
                if workflow.status == WorkflowStatus::Failed
                    && state
                        .last_scaled_at_ms
                        .is_some_and(|last| now.saturating_sub(last) < DEFAULT_RETRY_COOLDOWN_MS)
                {
                    diagnostic.scale_up_suppressed = true;
                    diagnostic.scale_up_suppression_reason = Some(
                        "worker provisioning failed recently; retry cooldown is active".to_owned(),
                    );
                    result.regions.push(diagnostic);
                    continue;
                }
            }
            state.provision_workflow_id = None;
        }

        let bootstrap_growth = bootstrap_deficit > 0;
        if bootstrap_growth && incoming_count > 0 {
            state.pressure_since_ms = None;
            diagnostic.scale_up_suppressed = true;
            diagnostic.scale_up_suppression_reason = Some(format!(
                "{incoming_count} incoming bootstrap worker(s) already count toward the target"
            ));
            result.regions.push(diagnostic);
            continue;
        }
        if !bootstrap_growth && ordinary_shortfall == 0 {
            state.pressure_since_ms = None;
            diagnostic.scale_up_suppressed = true;
            diagnostic.scale_up_suppression_reason = Some(format!(
                "current pressure is transient: {temporarily_unavailable} assigned worker(s) are travelling, busy, or reserved"
            ));
            result.regions.push(diagnostic);
            continue;
        }
        if !bootstrap_growth && idle_ratio >= settings.scale_up_idle_threshold {
            state.pressure_since_ms = None;
            diagnostic.scale_up_suppressed = true;
            diagnostic.scale_up_suppression_reason = Some(format!(
                "idle reserve is {:.0}%, at or above the {:.0}% scale-up threshold",
                idle_ratio * 100.0,
                settings.scale_up_idle_threshold * 100.0
            ));
            result.regions.push(diagnostic);
            continue;
        }

        if !bootstrap_growth {
            let since = *state.pressure_since_ms.get_or_insert(now);
            if now.saturating_sub(since) < settings.scale_up_hold_ms {
                diagnostic.scale_up_suppressed = true;
                diagnostic.scale_up_suppression_reason =
                    Some("ordinary capacity pressure has not reached the hold time".to_owned());
                result.regions.push(diagnostic);
                continue;
            }
            if state
                .last_scaled_at_ms
                .is_some_and(|last| now.saturating_sub(last) < settings.scale_up_cooldown_ms)
            {
                diagnostic.scale_up_suppressed = true;
                diagnostic.scale_up_suppression_reason =
                    Some("ordinary scale-up cooldown is active".to_owned());
                result.regions.push(diagnostic);
                continue;
            }
        }

        let source = homes.iter().find_map(|selection| {
            workers
                .iter()
                .filter(|worker| {
                    bootstrap_growth
                        || !selection.local
                        || worker.region.as_deref() == Some(region_name.as_str())
                })
                .find(|worker| {
                    worker.busy_workflow.is_none()
                        && worker.state == WorkerState::Operational
                        && !reserved.contains(worker.replicant.key.id.as_str())
                        && worker_near_home(worker, &selection.location)
                })
                .map(|worker| (selection, worker))
        });
        let Some((home, source)) = source else {
            diagnostic.scale_up_suppressed = true;
            diagnostic.scale_up_suppression_reason = Some(
                "no idle source Replicant is available at a viable manufacturing home".to_owned(),
            );
            result.regions.push(diagnostic);
            continue;
        };
        diagnostic.manufacturing_home = Some(home.location.clone());
        diagnostic.manufacturing_home_reason = Some(home.reason.clone());
        result.recommendations.push(format!(
            "{region_name} has {pending} worker-blocked campaign(s), {} assigned and {incoming_count} incoming; provision one additional Replicant at {}",
            assigned,
            home.location
        ));
        if automatic {
            let workflow =
                repository.create(new_replicant_provision_workflow(ReplicantProvisionIntent {
                    region: region_name.clone(),
                    home: home.location.clone(),
                    source_replicant: source.replicant.key.id.as_str().to_owned(),
                    cradle_type: "racing_vessel".to_owned(),
                    name: None,
                }))?;
            tracing::info!(
                workflow_id = %workflow.id,
                region = %region_name,
                home = %home.location,
                source_replicant = %source.replicant.key.id.as_str(),
                bootstrap_growth,
                "Director launched grow-only workforce provisioning"
            );
            state.provision_workflow_id = Some(workflow.id);
            state.last_scaled_at_ms = Some(now);
            state.pressure_since_ms = None;
        }
        result.regions.push(diagnostic);
    }
    result
        .regions
        .sort_by(|left, right| left.region.cmp(&right.region));
    for (region, state) in states.iter_mut() {
        if !demand.contains_key(region) {
            state.pressure_since_ms = None;
        }
        repository.put_document(WORKFORCE_NS, region, state)?;
    }
    Ok(result)
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
) {
    for region in regions
        .values_mut()
        .filter(|region| region.status == DirectorRegionStatus::Discovered)
    {
        // WorkerState::Operational already guarantees that the authoritative
        // hosted-vessel location resolves to the worker's assigned region.
        let staged_workers = workers
            .iter()
            .filter(|worker| worker.region.as_deref() == Some(region.region.as_str()))
            .filter(|worker| worker.state == WorkerState::Operational)
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
) -> BTreeMap<String, String> {
    let mut manufacturing_homes = BTreeMap::new();
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
        region.hub_location = Some(home.clone());
        manufacturing_homes.insert(region.region.clone(), home);
    }
    manufacturing_homes
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
            let location_id = location.key.id.as_str();
            location.system.as_ref().map_or_else(
                || {
                    location
                        .location_type
                        .as_ref()
                        .is_some_and(|kind| kind.as_str() == "star")
                        .then(|| (location_id.to_owned(), location_id.to_owned()))
                },
                |system| Some((location_id.to_owned(), system.clone())),
            )
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

fn load_goal_controls<'a>(
    repository: &WorkflowRepository,
    regions: impl IntoIterator<Item = &'a str>,
) -> Result<GoalControls, ApplicationError> {
    let mut controls = GoalControls::default();
    for kind in all_goal_kinds() {
        let enabled = repository
            .read_document(GOAL_CONTROL_NS, goal_kind_key(kind))?
            .map(|(value, _)| serde_json::from_value::<GoalControl>(value))
            .transpose()?
            .map(|control| control.enabled)
            .unwrap_or_else(|| default_goal_enabled(kind));
        controls.global.insert(kind, enabled);
    }
    for region in regions {
        for kind in all_goal_kinds()
            .into_iter()
            .filter(|kind| goal_is_regional(*kind))
        {
            let enabled = repository
                .read_document(GOAL_CONTROL_NS, &goal_instance_id(kind, Some(region)))?
                .map(|(value, _)| serde_json::from_value::<GoalControl>(value))
                .transpose()?
                .map(|control| control.enabled)
                .unwrap_or_else(|| controls.global[&kind]);
            controls
                .regional
                .entry(kind)
                .or_default()
                .insert(region.to_owned(), enabled);
        }
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
    runtime.launch_records.retain(|record| {
        workflows
            .iter()
            .find(|workflow| workflow.id == record.workflow_id)
            .is_some_and(|workflow| {
                !workflow.status.is_terminal()
                    || workflow.status == WorkflowStatus::Failed
                        && workflow.failure_disposition
                            == Some(WorkflowFailureDisposition::Permanent)
            })
    });
}

fn retain_work_identity(runtime: &mut GoalRuntime, identity: &GoalWorkIdentity) {
    let removed_obsolete = runtime
        .launch_records
        .iter()
        .any(|record| &record.identity != identity);
    runtime
        .launch_records
        .retain(|record| &record.identity == identity);
    if removed_obsolete {
        runtime.last_launch_at_ms = None;
    }
}

fn record_goal_launch(
    runtime: &mut GoalRuntime,
    workflow_id: WorkflowId,
    identity: GoalWorkIdentity,
) {
    runtime.launch_records = vec![GoalLaunchRecord {
        workflow_id,
        identity,
    }];
}

fn permanent_failure_for_identity<'a>(
    runtime: &GoalRuntime,
    workflows: &'a [WorkflowInstance],
    identity: &GoalWorkIdentity,
) -> Option<&'a WorkflowInstance> {
    runtime
        .launch_records
        .iter()
        .find(|record| &record.identity == identity)
        .and_then(|record| {
            workflows
                .iter()
                .find(|workflow| workflow.id == record.workflow_id)
        })
        .filter(|workflow| {
            workflow.status == WorkflowStatus::Failed
                && workflow.failure_disposition == Some(WorkflowFailureDisposition::Permanent)
        })
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

fn regional_workers_in_transit(
    workers: &[WorkerView],
    region: &str,
    reserved: &BTreeSet<String>,
) -> usize {
    workers
        .iter()
        .filter(|worker| worker.region.as_deref() == Some(region))
        .filter(|worker| worker.state == WorkerState::InTransit)
        .filter(|worker| !reserved.contains(worker.replicant.key.id.as_str()))
        .count()
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
        .filter(|worker| worker.state == WorkerState::Operational)
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

fn partition_catalogue_batch(systems: &[String], workers: usize) -> Vec<Vec<String>> {
    let batch_limit = workers.saturating_mul(CATALOGUE_SYSTEMS_PER_WORKER);
    let batch = systems
        .iter()
        .take(batch_limit)
        .cloned()
        .collect::<Vec<_>>();
    partition_systems(&batch, workers)
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
        .filter(|worker| worker.state == WorkerState::Operational)
        .filter(|worker| !reserved.contains(worker.replicant.key.id.as_str()))
        .filter_map(|worker| {
            let vessel_code = worker.racing_vessel.as_deref()?;
            let vessel = devices
                .iter()
                .find(|device| device.key.id.as_str() == vessel_code)?;
            let free_stow = vessel.free_stow_capacity();
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
        .filter(|worker| worker.state == WorkerState::Operational)
        .filter(|worker| !reserved.contains(worker.replicant.key.id.as_str()))
        .filter(|worker| !require_racing_vessel || worker.racing_vessel.is_some())
        .min_by_key(|worker| worker.role_affinity.is_none())
        .map(|worker| worker.replicant.key.id.as_str().to_owned())
}

fn protocol_worker_state(state: WorkerState) -> DirectorWorkerState {
    match state {
        WorkerState::Operational => DirectorWorkerState::Operational,
        WorkerState::InTransit => DirectorWorkerState::InTransit,
        WorkerState::Busy => DirectorWorkerState::Busy,
        WorkerState::WrongRegion
        | WorkerState::MissingVessel
        | WorkerState::UnknownLocation
        | WorkerState::LocationMismatch
        | WorkerState::Unavailable => DirectorWorkerState::Unavailable,
    }
}

fn regional_radius_resolution_blocker(
    region: &RegionView,
    radius_ly: f64,
    footprint: &str,
) -> String {
    match region.hub_system.as_deref() {
        Some(hub) => format!(
            "{} regional hub {hub} has no catalogue position, so its {radius_ly:.0} LY {footprint} footprint cannot be resolved",
            region.region
        ),
        None => format!(
            "{} has no selected regional hub, so its {radius_ly:.0} LY {footprint} footprint cannot be resolved",
            region.region
        ),
    }
}

#[derive(Clone, Debug, Default)]
struct RegionalSystemScope {
    systems: BTreeSet<String>,
    missing_positions: usize,
}

fn regional_system_scope_from_hub(
    region: &RegionView,
    positions: &BTreeMap<String, GalacticPosition>,
    radius_ly: f64,
) -> Option<RegionalSystemScope> {
    let hub_system = region.hub_system.as_deref()?;
    let hub_position = positions.get(hub_system).copied()?;

    let mut scope = RegionalSystemScope::default();
    for system in &region.known_systems {
        let Some(position) = positions.get(system).copied() else {
            scope.missing_positions += 1;
            continue;
        };
        if galactic_distance(hub_position, position) <= radius_ly {
            scope.systems.insert(system.clone());
        }
    }
    Some(scope)
}

fn catalogue_survey_scope_from_hub(
    region: &RegionView,
    catalogue: &[Star],
) -> Option<RegionalSystemScope> {
    let positions = catalogue
        .iter()
        .filter_map(|star| {
            star.position
                .map(|position| (star.key.id.as_str().to_owned(), position))
        })
        .collect::<BTreeMap<_, _>>();
    regional_system_scope_from_hub(region, &positions, REGIONAL_AUTOMATION_RADIUS_LY)
}

fn worker_near_home(worker: &WorkerView, home: &str) -> bool {
    worker
        .physical_location
        .as_deref()
        .is_some_and(|current| current == home || same_system(current, home))
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
    controls: &GoalControls,
    objective: &str,
    blocker: &str,
    next_action: &str,
) -> DirectorGoalSummary {
    let enabled = goal_enabled(controls, kind, region);
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

fn goal_enabled(controls: &GoalControls, kind: DirectorGoalKind, region: Option<&str>) -> bool {
    controls.enabled(kind, region)
}

fn default_goal_enabled(kind: DirectorGoalKind) -> bool {
    !matches!(
        kind,
        DirectorGoalKind::EstablishBeacons
            | DirectorGoalKind::SalvageRecovery
            | DirectorGoalKind::AsteroidDiversion
            | DirectorGoalKind::StrandedDeviceRecovery
            | DirectorGoalKind::UnservicedResources
    )
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
        DirectorGoalKind::SalvageRecovery => "Recover discovered regional salvage",
        DirectorGoalKind::EventCompletion => "Complete worthwhile active regional events",
        DirectorGoalKind::AsteroidDiversion => {
            "Divert incoming asteroids threatening regional systems"
        }
        DirectorGoalKind::BlueprintAcquisition => {
            "Learn missing blueprints from known owned-device opportunities"
        }
        DirectorGoalKind::MaintainSystemHubs => {
            "Keep operational System Hubs supplied with reported upkeep resources"
        }
        DirectorGoalKind::StrandedDeviceRecovery => {
            "Recover stranded owned devices to regional System Hubs"
        }
        DirectorGoalKind::UnservicedResources => {
            "Establish AMI transport service for producing regional resources"
        }
        DirectorGoalKind::ExpandFtlNetwork => "Maintain and extend regional FTL reach",
        DirectorGoalKind::EstablishBeacons => "Maintain beacon coverage at useful known systems",
    }
}

fn all_goal_kinds() -> [DirectorGoalKind; 14] {
    [
        DirectorGoalKind::EstablishRegions,
        DirectorGoalKind::ExpandStarCatalogue,
        DirectorGoalKind::EnhanceStarCatalogue,
        DirectorGoalKind::DiscoverBelts,
        DirectorGoalKind::ExpandMiningOps,
        DirectorGoalKind::SalvageRecovery,
        DirectorGoalKind::EventCompletion,
        DirectorGoalKind::AsteroidDiversion,
        DirectorGoalKind::BlueprintAcquisition,
        DirectorGoalKind::MaintainSystemHubs,
        DirectorGoalKind::StrandedDeviceRecovery,
        DirectorGoalKind::UnservicedResources,
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
        DirectorGoalKind::SalvageRecovery => "salvage_recovery",
        DirectorGoalKind::EventCompletion => "event_completion",
        DirectorGoalKind::AsteroidDiversion => "asteroid_diversion",
        DirectorGoalKind::BlueprintAcquisition => "blueprint_acquisition",
        DirectorGoalKind::MaintainSystemHubs => "maintain_system_hubs",
        DirectorGoalKind::StrandedDeviceRecovery => "stranded_device_recovery",
        DirectorGoalKind::UnservicedResources => "unserviced_resources",
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

/// Whether a standing goal is controlled independently for each region.
#[must_use]
pub fn goal_is_regional(kind: DirectorGoalKind) -> bool {
    matches!(
        kind,
        DirectorGoalKind::EnhanceStarCatalogue
            | DirectorGoalKind::DiscoverBelts
            | DirectorGoalKind::ExpandMiningOps
            | DirectorGoalKind::SalvageRecovery
            | DirectorGoalKind::EventCompletion
            | DirectorGoalKind::AsteroidDiversion
            | DirectorGoalKind::MaintainSystemHubs
            | DirectorGoalKind::StrandedDeviceRecovery
            | DirectorGoalKind::UnservicedResources
            | DirectorGoalKind::ExpandFtlNetwork
            | DirectorGoalKind::EstablishBeacons
    )
}
/// Managed events that should wake an in-flight Director reconciliation.
#[must_use]
pub fn director_reconcile_event_names() -> &'static [&'static str] {
    &[
        "system.object_detected",
        "diversion.activated",
        "diversion.deactivated",
        "diversion.partial",
        "diversion.diverted",
        "diversion.impacted",
        "travel.arrived",
        "travel.cancelled",
        "travel.departed",
        "replicant.transferred",
        "device.attached",
        "device.compacted",
        "device.deployed",
        "device.detached",
        "device.stowed",
        "device.unfurled",
        "ami.adopted",
        "ami.launched",
        "ami.released",
        "ami.withdrawn",
        "ami.transport.digest",
        "device.decommissioned",
        "directive.set",
        "directive.cleared",
        "directive.paused",
        "directive.resumed",
        "directive.completed",
        "mining.started",
        "mining.stopped",
        "mining.retargeted",
    ]
}

fn goal_instance_id(kind: DirectorGoalKind, region: Option<&str>) -> String {
    match region {
        Some(region) => format!("{}:{region}", goal_kind_key(kind)),
        None => goal_kind_key(kind).to_owned(),
    }
}

fn complete_owned_device_census(client: &Client) -> bool {
    let readiness = client.readiness();
    [
        readiness.full_rest,
        readiness.event_catchup,
        readiness.sse_connectivity,
        readiness.store_health,
    ]
    .into_iter()
    .all(|component| component == ReadinessComponent::Ready)
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
    use crate::automation::SalvageSiteRecord;

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

    fn salvage_test_region(home: Option<&str>) -> RegionView {
        RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: home.map(|_| "ALPHA".to_owned()),
            hub_location: home.map(str::to_owned),
            known_systems: BTreeSet::from(["ALPHA".to_owned()]),
        }
    }

    fn salvage_test_snapshot(designations: &[&str]) -> SalvageRecoveryHistorySnapshot {
        let sites = designations
            .iter()
            .map(|designation| {
                (
                    (*designation).to_owned(),
                    SalvageSiteRecord {
                        designation: (*designation).to_owned(),
                        location: "ALPHA-BELT-1".to_owned(),
                        resources: BTreeMap::new(),
                        event_id: format!("event-{designation}"),
                    },
                )
            })
            .collect();
        SalvageRecoveryHistorySnapshot {
            discovered_count: designations.len(),
            depleted_count: 0,
            sites_by_region: BTreeMap::from([("alpha".to_owned(), sites)]),
        }
    }

    fn salvage_enabled_controls(enabled: bool) -> GoalControls {
        let mut controls = GoalControls::default();
        controls
            .regional
            .entry(DirectorGoalKind::SalvageRecovery)
            .or_default()
            .insert("alpha".to_owned(), enabled);
        controls
    }

    fn salvage_context<'a>(
        repository: &'a WorkflowRepository,
        workflows: &'a [WorkflowInstance],
        controls: &'a GoalControls,
        automatic: bool,
        now: i64,
    ) -> GoalReconcileContext<'a> {
        GoalReconcileContext {
            repository,
            workflows,
            controls,
            automatic,
            now,
        }
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
            physical_location: Some(location.to_owned()),
            state: WorkerState::Operational,
        }
    }

    #[test]
    fn in_transit_worker_remains_assigned_capacity_and_recovers_after_arrival() {
        let mut worker = test_worker("R-1", "alpha", "ALPHA-1");
        worker.racing_vessel = Some("V-1".to_owned());
        worker.state = WorkerState::InTransit;
        let workers = vec![worker];

        assert_eq!(
            workers
                .iter()
                .filter(|worker| worker.region.as_deref() == Some("alpha"))
                .count(),
            1
        );
        assert_eq!(
            regional_workers_in_transit(&workers, "alpha", &BTreeSet::new()),
            1
        );
        assert!(idle_catalogue_workers(&workers, "alpha", &BTreeSet::new()).is_empty());

        let mut arrived = workers;
        arrived[0].state = WorkerState::Operational;
        assert_eq!(
            idle_catalogue_workers(&arrived, "alpha", &BTreeSet::new()),
            vec![(String::from("R-1"), String::from("V-1"))]
        );
    }
    fn workforce_test_regions(status: DirectorRegionStatus) -> BTreeMap<String, RegionView> {
        BTreeMap::from([
            (
                "alpha".to_owned(),
                RegionView {
                    region: "alpha".to_owned(),
                    status: DirectorRegionStatus::Established,
                    hub_system: Some("SCEPTURUM".to_owned()),
                    hub_location: Some("SCEPTURUM-BELT-1".to_owned()),
                    known_systems: BTreeSet::from(["SCEPTURUM".to_owned()]),
                },
            ),
            (
                "delta".to_owned(),
                RegionView {
                    region: "delta".to_owned(),
                    status,
                    hub_system: Some("PHASYRIS".to_owned()),
                    hub_location: Some("PHASYRIS-BELT-1".to_owned()),
                    known_systems: BTreeSet::from(["PHASYRIS".to_owned()]),
                },
            ),
        ])
    }

    fn global_manufacturing_home() -> BTreeMap<String, String> {
        BTreeMap::from([("alpha".to_owned(), "SCEPTURUM-BELT-1".to_owned())])
    }

    fn workforce_settings_without_delays() -> DirectorSettings {
        DirectorSettings {
            scale_up_idle_threshold: 1.0,
            scale_up_hold_ms: 0,
            scale_up_cooldown_ms: 0,
            ..DirectorSettings::default()
        }
    }

    fn provision_intents(repository: &WorkflowRepository) -> Vec<ReplicantProvisionIntent> {
        repository
            .list()
            .expect("provision workflows")
            .into_iter()
            .filter(|workflow| workflow.kind == replicant_provision_workflow_kind())
            .map(|workflow| workflow.config().expect("provision intent"))
            .collect()
    }

    fn reconcile_test_workforce(
        repository: &WorkflowRepository,
        settings: &DirectorSettings,
        regions: &BTreeMap<String, RegionView>,
        manufacturing_homes: &BTreeMap<String, String>,
        workers: &[WorkerView],
        demand: usize,
        states: &mut BTreeMap<String, RegionWorkforceState>,
    ) -> WorkforceReconciliation {
        reconcile_workforce(
            repository,
            settings,
            regions,
            manufacturing_homes,
            workers,
            &repository.list().expect("workflows"),
            &BTreeSet::new(),
            &BTreeMap::from([("delta".to_owned(), demand)]),
            states,
            true,
            100,
        )
        .expect("reconcile workforce")
    }
    fn establishment_worker_demand(
        repository: &WorkflowRepository,
        regions: &BTreeMap<String, RegionView>,
        workers: &[WorkerView],
    ) -> usize {
        let controls = GoalControls::default();
        let workflows = repository.list().expect("workflows");
        let context = GoalReconcileContext {
            repository,
            workflows: &workflows,
            controls: &controls,
            automatic: false,
            now: 100,
        };
        let mut requirements =
            DirectorRequirementGraph::load(repository, context.now).expect("requirements");
        reconcile_establish_regions(
            &context,
            regions,
            workers,
            &mut BTreeSet::new(),
            &mut requirements,
        )
        .expect("reconcile establishment");
        requirements
            .worker_demand_by_region()
            .get("delta")
            .copied()
            .unwrap_or_default()
    }

    #[test]
    fn establishing_region_with_zero_assigned_provisions_toward_bootstrap_target() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let workers = vec![test_worker("A-1", "alpha", "SCEPTURUM-BELT-1")];
        let regions = workforce_test_regions(DirectorRegionStatus::Establishing);
        let demand = establishment_worker_demand(&repository, &regions, &workers);
        let result = reconcile_test_workforce(
            &repository,
            &DirectorSettings::default(),
            &regions,
            &global_manufacturing_home(),
            &workers,
            demand,
            &mut BTreeMap::new(),
        );

        let intents = provision_intents(&repository);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].region, "delta");
        assert_eq!(result.regions[1].bootstrap_target, REGION_BOOTSTRAP_TARGET);
        assert_eq!(result.regions[1].assigned, 0);
    }

    #[test]
    fn establishing_region_with_one_assigned_provisions_one_more() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let workers = vec![
            test_worker("A-1", "alpha", "SCEPTURUM-BELT-1"),
            test_worker("D-1", "delta", "SCEPTURUM-BELT-1"),
        ];
        let regions = workforce_test_regions(DirectorRegionStatus::Establishing);
        let demand = establishment_worker_demand(&repository, &regions, &workers);
        reconcile_test_workforce(
            &repository,
            &DirectorSettings::default(),
            &regions,
            &global_manufacturing_home(),
            &workers,
            demand,
            &mut BTreeMap::new(),
        );

        assert_eq!(provision_intents(&repository).len(), 1);
    }

    #[test]
    fn assigned_plus_incoming_worker_satisfies_bootstrap_target() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        repository
            .create(new_replicant_provision_workflow(ReplicantProvisionIntent {
                region: "delta".to_owned(),
                home: "SCEPTURUM-BELT-1".to_owned(),
                source_replicant: "A-1".to_owned(),
                cradle_type: "racing_vessel".to_owned(),
                name: None,
            }))
            .expect("incoming provision");
        let workers = vec![
            test_worker("A-1", "alpha", "SCEPTURUM-BELT-1"),
            test_worker("D-1", "delta", "SCEPTURUM-BELT-1"),
        ];
        let regions = workforce_test_regions(DirectorRegionStatus::Establishing);
        let demand = establishment_worker_demand(&repository, &regions, &workers);
        let result = reconcile_test_workforce(
            &repository,
            &DirectorSettings::default(),
            &regions,
            &global_manufacturing_home(),
            &workers,
            demand,
            &mut BTreeMap::new(),
        );

        assert_eq!(demand, 0);
        assert_eq!(provision_intents(&repository).len(), 1);
        assert_eq!(result.regions[1].assigned, 1);
        assert_eq!(result.regions[1].incoming, 1);
    }

    #[test]
    fn establishing_region_with_two_assigned_does_not_bootstrap_a_third() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let workers = vec![
            test_worker("A-1", "alpha", "SCEPTURUM-BELT-1"),
            test_worker("D-1", "delta", "PHASYRIS-BELT-1"),
            test_worker("D-2", "delta", "PHASYRIS-BELT-1"),
        ];
        let result = reconcile_test_workforce(
            &repository,
            &DirectorSettings::default(),
            &workforce_test_regions(DirectorRegionStatus::Establishing),
            &global_manufacturing_home(),
            &workers,
            1,
            &mut BTreeMap::new(),
        );

        assert!(provision_intents(&repository).is_empty());
        assert!(
            result.regions[1]
                .scale_up_suppression_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("idle reserve"))
        );
    }

    #[test]
    fn travelling_assigned_workers_satisfy_bootstrap_population() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let mut first = test_worker("D-1", "delta", "SCEPTURUM-BELT-1");
        first.state = WorkerState::InTransit;
        let mut second = test_worker("D-2", "delta", "SCEPTURUM-BELT-1");
        second.state = WorkerState::InTransit;
        let workers = vec![
            test_worker("A-1", "alpha", "SCEPTURUM-BELT-1"),
            first,
            second,
        ];
        let result = reconcile_test_workforce(
            &repository,
            &DirectorSettings::default(),
            &workforce_test_regions(DirectorRegionStatus::Establishing),
            &global_manufacturing_home(),
            &workers,
            2,
            &mut BTreeMap::new(),
        );

        assert!(provision_intents(&repository).is_empty());
        assert_eq!(result.regions[1].assigned, REGION_BOOTSTRAP_TARGET);
        assert_eq!(result.regions[1].in_transit, REGION_BOOTSTRAP_TARGET);
    }

    #[test]
    fn seven_assigned_workers_cannot_trigger_bootstrap_scale_eighth() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let mut workers = vec![test_worker("A-1", "alpha", "SCEPTURUM-BELT-1")];
        for index in 1..=7 {
            let mut worker = test_worker(&format!("D-{index}"), "delta", "PHASYRIS-BELT-1");
            worker.state = WorkerState::Unavailable;
            workers.push(worker);
        }
        let mut states = BTreeMap::from([(
            "delta".to_owned(),
            RegionWorkforceState {
                pressure_since_ms: Some(0),
                last_scaled_at_ms: Some(90),
                provision_workflow_id: None,
            },
        )]);
        let result = reconcile_test_workforce(
            &repository,
            &DirectorSettings {
                scale_up_idle_threshold: 1.0,
                scale_up_hold_ms: 0,
                scale_up_cooldown_ms: 100,
                ..DirectorSettings::default()
            },
            &workforce_test_regions(DirectorRegionStatus::Establishing),
            &global_manufacturing_home(),
            &workers,
            1,
            &mut states,
        );

        assert!(provision_intents(&repository).is_empty());
        assert_eq!(result.regions[1].assigned, 7);
        assert_eq!(
            result.regions[1].scale_up_suppression_reason.as_deref(),
            Some("ordinary scale-up cooldown is active")
        );
    }

    #[test]
    fn ordinary_scale_up_after_bootstrap_respects_idle_and_cooldown_guards() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let workers = vec![
            test_worker("A-1", "alpha", "SCEPTURUM-BELT-1"),
            test_worker("D-1", "delta", "PHASYRIS-BELT-1"),
            test_worker("D-2", "delta", "PHASYRIS-BELT-1"),
        ];
        let idle_result = reconcile_test_workforce(
            &repository,
            &DirectorSettings::default(),
            &workforce_test_regions(DirectorRegionStatus::Establishing),
            &global_manufacturing_home(),
            &workers,
            1,
            &mut BTreeMap::new(),
        );
        assert!(
            idle_result.regions[1]
                .scale_up_suppression_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("idle reserve"))
        );

        let unavailable = workers
            .into_iter()
            .map(|mut worker| {
                if worker.region.as_deref() == Some("delta") {
                    worker.state = WorkerState::Unavailable;
                }
                worker
            })
            .collect::<Vec<_>>();
        let mut states = BTreeMap::from([(
            "delta".to_owned(),
            RegionWorkforceState {
                pressure_since_ms: Some(0),
                last_scaled_at_ms: Some(90),
                provision_workflow_id: None,
            },
        )]);
        let cooldown_result = reconcile_test_workforce(
            &repository,
            &DirectorSettings {
                scale_up_idle_threshold: 1.0,
                scale_up_hold_ms: 0,
                scale_up_cooldown_ms: 100,
                ..DirectorSettings::default()
            },
            &workforce_test_regions(DirectorRegionStatus::Establishing),
            &global_manufacturing_home(),
            &unavailable,
            1,
            &mut states,
        );
        assert_eq!(
            cooldown_result.regions[1]
                .scale_up_suppression_reason
                .as_deref(),
            Some("ordinary scale-up cooldown is active")
        );
        assert!(provision_intents(&repository).is_empty());
    }

    #[test]
    fn temporary_campaign_claim_does_not_create_growth_pressure() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let mut busy = test_worker("D-1", "delta", "PHASYRIS-BELT-1");
        busy.state = WorkerState::Busy;
        busy.busy_workflow = Some(WorkflowId::new());
        let mut unavailable = test_worker("D-2", "delta", "PHASYRIS-BELT-1");
        unavailable.state = WorkerState::Unavailable;
        let workers = vec![
            test_worker("A-1", "alpha", "SCEPTURUM-BELT-1"),
            busy,
            unavailable,
        ];
        let result = reconcile_test_workforce(
            &repository,
            &workforce_settings_without_delays(),
            &workforce_test_regions(DirectorRegionStatus::Establishing),
            &global_manufacturing_home(),
            &workers,
            1,
            &mut BTreeMap::new(),
        );

        assert!(provision_intents(&repository).is_empty());
        assert!(
            result.regions[1]
                .scale_up_suppression_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("transient"))
        );
    }

    #[test]
    fn post_bootstrap_growth_prefers_local_manufacturing_foothold() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let mut unavailable = test_worker("D-2", "delta", "PHASYRIS-BELT-1");
        unavailable.state = WorkerState::Unavailable;
        let workers = vec![
            test_worker("A-1", "alpha", "SCEPTURUM-BELT-1"),
            test_worker("D-1", "delta", "PHASYRIS-BELT-1"),
            unavailable,
        ];
        let homes = BTreeMap::from([
            ("alpha".to_owned(), "SCEPTURUM-BELT-1".to_owned()),
            ("delta".to_owned(), "PHASYRIS-BELT-1".to_owned()),
        ]);
        let result = reconcile_test_workforce(
            &repository,
            &workforce_settings_without_delays(),
            &workforce_test_regions(DirectorRegionStatus::Establishing),
            &homes,
            &workers,
            1,
            &mut BTreeMap::new(),
        );

        let intents = provision_intents(&repository);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].home, "PHASYRIS-BELT-1");
        assert_eq!(
            result.regions[1].manufacturing_home.as_deref(),
            Some("PHASYRIS-BELT-1")
        );
    }

    #[test]
    fn post_bootstrap_growth_falls_back_when_local_manufacturing_is_unavailable() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let mut first = test_worker("D-1", "delta", "PHASYRIS-BELT-1");
        first.state = WorkerState::Unavailable;
        let mut second = test_worker("D-2", "delta", "PHASYRIS-BELT-1");
        second.state = WorkerState::Unavailable;
        let workers = vec![
            test_worker("A-1", "alpha", "SCEPTURUM-BELT-1"),
            first,
            second,
        ];
        let result = reconcile_test_workforce(
            &repository,
            &workforce_settings_without_delays(),
            &workforce_test_regions(DirectorRegionStatus::Establishing),
            &global_manufacturing_home(),
            &workers,
            1,
            &mut BTreeMap::new(),
        );

        let intents = provision_intents(&repository);
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].home, "SCEPTURUM-BELT-1");
        assert!(
            result.regions[1]
                .manufacturing_home_reason
                .as_deref()
                .is_some_and(|reason| reason.contains("fallback"))
        );
    }

    fn test_hub_device() -> Device {
        Device {
            key: replicant_client::DeviceKey::live("HUB1".into()),
            device_type: Some(DeviceType::SystemHub),
            status: Some(replicant_client::DeviceStatus::Active),
            location: Some(replicant_client::LocationKey::live("SCEPTURUM-7-L4".into())),
            deployed_at: None,
            in_control_range: None,
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            settings: Default::default(),
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
            runtime: Default::default(),
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
        mark_partial_region_footholds(&mut regions, &workers);

        assert_eq!(regions["beta"].status, DirectorRegionStatus::Establishing);
    }

    #[test]
    fn in_transit_bootstrap_workers_do_not_mark_a_physical_foothold() {
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
        let mut workers = vec![
            test_worker("BETA-1", "beta", "SOURCE-1"),
            test_worker("BETA-2", "beta", "SOURCE-2"),
        ];
        for worker in &mut workers {
            worker.state = WorkerState::InTransit;
        }

        mark_partial_region_footholds(&mut regions, &workers);

        assert_eq!(regions["beta"].status, DirectorRegionStatus::Discovered);
        assert_eq!(
            workers
                .iter()
                .filter(|worker| worker.region.as_deref() == Some("beta"))
                .count(),
            2
        );
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
        let homes = mark_manufacturing_footholds(
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
        assert_eq!(
            homes.get("delta").map(String::as_str),
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
        full_vessel.stow_used = Some(0);
        full_vessel.relationships.stowed_devices = (0..4)
            .map(|index| {
                replicant_client::DeviceKey::live(replicant_client::DeviceId::new(format!(
                    "PAYLOAD-{index}"
                )))
            })
            .collect();
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
            custom_name: None,
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
            BTreeMap::from([("BETA-STAR".to_owned(), 3), ("GAMMA-STAR".to_owned(), 2)])
        );
        assert!(ftl_priority_score(0, true, 0) > ftl_priority_score(650, false, 3));
        assert!(ftl_priority_score(650, false, 0) > ftl_priority_score(0, false, 3));
        assert!(ftl_priority_score(0, false, 3) > ftl_priority_score(0, false, 2));
        assert!(ftl_priority_score(0, false, 2) > ftl_priority_score(0, false, 1));
        assert!(ftl_priority_score(0, false, 1) > ftl_priority_score(0, false, 0));
        assert_eq!(
            ftl_priority_suffix(0, true, 3),
            " (active events + dense belt)"
        );
        assert_eq!(
            ftl_priority_suffix(650, false, 2),
            " (goal dependency + moderate belt)"
        );
        assert_eq!(
            ftl_priority_suffix(650, false, 1),
            " (goal dependency + sparse belt)"
        );
    }

    #[test]
    fn mining_density_policy_only_filters_new_expansion() {
        let dense_only = MiningExpansionPolicy {
            expand_moderate: false,
            expand_sparse: false,
        };
        assert!(mining_density_allowed(3, dense_only));
        assert!(!mining_density_allowed(2, dense_only));
        assert!(!mining_density_allowed(1, dense_only));

        let through_moderate = MiningExpansionPolicy {
            expand_moderate: true,
            expand_sparse: false,
        };
        assert!(mining_density_allowed(3, through_moderate));
        assert!(mining_density_allowed(2, through_moderate));
        assert!(!mining_density_allowed(1, through_moderate));
    }

    #[test]
    fn mining_expansion_is_not_capped_by_system_ward_limit() {
        let belts = [
            "HUB", "DENSE-A", "DENSE-B", "DENSE-C", "DENSE-D", "DENSE-E", "DENSE-F",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
        let density = belts
            .iter()
            .cloned()
            .map(|system| (system, 3))
            .collect::<BTreeMap<_, _>>();
        let managed = BTreeSet::new();
        let hubs = BTreeSet::from(["HUB".to_owned()]);
        let catalogue = vec![
            positioned_star("HUB", 0.0, Some("delta")),
            positioned_star("DENSE-A", 2.0, Some("delta")),
            positioned_star("DENSE-B", 4.0, Some("delta")),
            positioned_star("DENSE-C", 6.0, Some("delta")),
            positioned_star("DENSE-D", 8.0, Some("delta")),
            positioned_star("DENSE-E", 10.0, Some("delta")),
            positioned_star("DENSE-F", 12.0, Some("delta")),
        ];
        let policy = MiningExpansionPolicy::default();

        let desired =
            desired_mining_systems(&belts, &managed, &density, Some("HUB"), &catalogue, policy);
        let warded = selected_mining_ward_systems(
            &belts,
            &managed,
            &hubs,
            &density,
            Some("HUB"),
            &catalogue,
            policy,
        );

        assert_eq!(desired, belts);
        assert!(desired.len() > MINING_WARD_SITES_PER_REGION);
        assert_eq!(warded.len(), MINING_WARD_SITES_PER_REGION);
        assert!(!warded.contains("HUB"));
    }

    #[test]
    fn mining_expansion_is_limited_to_thirty_light_years_from_regional_hub() {
        let belts = ["HUB", "INSIDE", "EDGE", "OUTSIDE"]
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let density = belts
            .iter()
            .cloned()
            .map(|system| (system, 3))
            .collect::<BTreeMap<_, _>>();
        let catalogue = vec![
            positioned_star("HUB", 0.0, Some("delta")),
            positioned_star("INSIDE", MINING_EXPANSION_RADIUS_LY - 0.1, Some("delta")),
            positioned_star("EDGE", MINING_EXPANSION_RADIUS_LY, Some("delta")),
            positioned_star("OUTSIDE", MINING_EXPANSION_RADIUS_LY + 0.1, Some("delta")),
        ];

        let desired = desired_mining_systems(
            &belts,
            &BTreeSet::new(),
            &density,
            Some("HUB"),
            &catalogue,
            MiningExpansionPolicy::default(),
        );

        assert_eq!(
            desired,
            ["HUB", "INSIDE", "EDGE"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        );
        assert!(!desired.contains("OUTSIDE"));
    }

    #[test]
    fn managed_mining_site_outside_radius_is_not_a_desired_target() {
        let belts = BTreeSet::from(["OUTSIDE".to_owned()]);
        let managed = belts.clone();
        let density = BTreeMap::from([("OUTSIDE".to_owned(), 3)]);
        let catalogue = vec![
            positioned_star("HUB", 0.0, Some("delta")),
            positioned_star("OUTSIDE", MINING_EXPANSION_RADIUS_LY + 1.0, Some("delta")),
        ];

        let desired = desired_mining_systems(
            &belts,
            &managed,
            &density,
            Some("HUB"),
            &catalogue,
            MiningExpansionPolicy::default(),
        );

        assert!(desired.is_empty());
    }

    #[test]
    fn mining_ward_allocation_selects_only_four_non_hub_sites() {
        let belts = ["HUB", "DENSE-A", "DENSE-B", "DENSE-C", "DENSE-D", "DENSE-E"]
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let density = belts
            .iter()
            .cloned()
            .map(|system| (system, 3))
            .collect::<BTreeMap<_, _>>();

        let catalogue = vec![
            positioned_star("HUB", 0.0, Some("delta")),
            positioned_star("DENSE-A", 1.0, Some("delta")),
            positioned_star("DENSE-B", 2.0, Some("delta")),
            positioned_star("DENSE-C", 3.0, Some("delta")),
            positioned_star("DENSE-D", 4.0, Some("delta")),
            positioned_star("DENSE-E", 5.0, Some("delta")),
        ];
        let selected = selected_mining_ward_systems(
            &belts,
            &BTreeSet::new(),
            &BTreeSet::from(["HUB".to_owned()]),
            &density,
            Some("HUB"),
            &catalogue,
            MiningExpansionPolicy::default(),
        );

        assert_eq!(selected.len(), MINING_WARD_SITES_PER_REGION);
        assert!(!selected.contains("HUB"));
        assert_eq!(
            selected,
            ["DENSE-A", "DENSE-B", "DENSE-C", "DENSE-D"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        );
    }

    #[test]
    fn mining_selection_prefers_closest_systems_within_density() {
        let belts = ["NEAR", "NEAR-2", "MID", "FAR", "FARTHEST"]
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let density = belts
            .iter()
            .cloned()
            .map(|system| (system, 3))
            .collect::<BTreeMap<_, _>>();
        let catalogue = vec![
            positioned_star("HUB", 0.0, Some("delta")),
            positioned_star("NEAR", 2.0, Some("delta")),
            positioned_star("NEAR-2", 4.0, Some("delta")),
            positioned_star("MID", 8.0, Some("delta")),
            positioned_star("FAR", 12.0, Some("delta")),
            positioned_star("FARTHEST", 20.0, Some("delta")),
        ];

        let selected = selected_mining_ward_systems(
            &belts,
            &BTreeSet::new(),
            &BTreeSet::new(),
            &density,
            Some("HUB"),
            &catalogue,
            MiningExpansionPolicy::default(),
        );

        assert_eq!(selected.len(), MINING_WARD_SITES_PER_REGION);
        assert!(selected.contains("NEAR"));
        assert!(selected.contains("NEAR-2"));
        assert!(selected.contains("MID"));
        assert!(selected.contains("FAR"));
        assert!(!selected.contains("FARTHEST"));
    }

    #[test]
    fn mining_ward_allocation_prefers_existing_sites_only_within_same_density() {
        let belts = ["EXISTING", "DENSE-A", "DENSE-B", "DENSE-C", "DENSE-D"]
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let managed = BTreeSet::from(["EXISTING".to_owned()]);
        let density = belts
            .iter()
            .cloned()
            .map(|system| (system, 3))
            .collect::<BTreeMap<_, _>>();

        let catalogue = vec![
            positioned_star("HUB", 0.0, Some("delta")),
            positioned_star("EXISTING", 1.0, Some("delta")),
            positioned_star("DENSE-A", 2.0, Some("delta")),
            positioned_star("DENSE-B", 3.0, Some("delta")),
            positioned_star("DENSE-C", 4.0, Some("delta")),
            positioned_star("DENSE-D", 5.0, Some("delta")),
        ];
        let selected = selected_mining_ward_systems(
            &belts,
            &managed,
            &BTreeSet::new(),
            &density,
            Some("HUB"),
            &catalogue,
            MiningExpansionPolicy::default(),
        );

        assert_eq!(selected.len(), MINING_WARD_SITES_PER_REGION);
        assert!(selected.contains("EXISTING"));
        assert!(!selected.contains("DENSE-D"));
    }

    #[test]
    fn newly_discovered_dense_belt_displaces_lower_density_ward_slot() {
        let belts = ["DENSE-A", "DENSE-B", "DENSE-C", "MOD-A", "MOD-B"]
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let managed = ["DENSE-A", "DENSE-B", "MOD-A", "MOD-B"]
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>();
        let density = BTreeMap::from([
            ("DENSE-A".to_owned(), 3),
            ("DENSE-B".to_owned(), 3),
            ("DENSE-C".to_owned(), 3),
            ("MOD-A".to_owned(), 2),
            ("MOD-B".to_owned(), 2),
        ]);

        let catalogue = vec![
            positioned_star("HUB", 0.0, Some("delta")),
            positioned_star("DENSE-A", 1.0, Some("delta")),
            positioned_star("DENSE-B", 2.0, Some("delta")),
            positioned_star("DENSE-C", 3.0, Some("delta")),
            positioned_star("MOD-A", 4.0, Some("delta")),
            positioned_star("MOD-B", 5.0, Some("delta")),
        ];
        let selected = selected_mining_ward_systems(
            &belts,
            &managed,
            &BTreeSet::new(),
            &density,
            Some("HUB"),
            &catalogue,
            MiningExpansionPolicy::default(),
        );

        assert_eq!(
            selected,
            ["DENSE-A", "DENSE-B", "DENSE-C", "MOD-A"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        );
    }

    #[test]
    fn ward_rebalance_moves_displaced_lower_density_ward_to_dense_target() {
        let mut ward = test_hub_device();
        ward.key = replicant_client::DeviceKey::live("WARD-MOD-B".into());
        ward.device_type = Some(DeviceType::SystemWard);
        ward.location = Some(replicant_client::LocationKey::live("MOD-B-L4".into()));

        let mut mining_controller = test_hub_device();
        mining_controller.key = replicant_client::DeviceKey::live("MC-MOD-B".into());
        mining_controller.device_type = Some(DeviceType::MiningController);
        mining_controller.location =
            Some(replicant_client::LocationKey::live("MOD-B-BELT-1".into()));
        let mut survey_controller = mining_controller.clone();
        survey_controller.key = replicant_client::DeviceKey::live("SC-MOD-B".into());
        survey_controller.device_type = Some(DeviceType::SurveyController);

        let devices = vec![ward, mining_controller, survey_controller];
        let managed = BTreeSet::from(["MOD-B".to_owned()]);
        let selected = BTreeSet::from(["DENSE-C".to_owned()]);
        let relays = BTreeSet::from(["DENSE-C".to_owned(), "MOD-B".to_owned()]);
        let density = BTreeMap::from([("DENSE-C".to_owned(), 3), ("MOD-B".to_owned(), 2)]);
        let belts = BTreeMap::from([
            ("DENSE-C".to_owned(), vec!["DENSE-C-BELT-1".to_owned()]),
            ("MOD-B".to_owned(), vec!["MOD-B-BELT-1".to_owned()]),
        ]);
        let locations = BTreeMap::from([("MOD-B-L4".to_owned(), "MOD-B".to_owned())]);

        let relocations = plan_mining_ward_relocations(MiningWardRebalanceContext {
            devices: &devices,
            managed_systems: &managed,
            selected_ward_systems: &selected,
            relay_systems: &relays,
            hub_systems: &BTreeSet::new(),
            belt_density: &density,
            belt_designations: &belts,
            location_systems: &locations,
        });

        assert_eq!(relocations.len(), 1);
        assert_eq!(relocations[0].ward_code, "WARD-MOD-B");
        assert_eq!(relocations[0].source_system, "MOD-B");
        assert_eq!(relocations[0].target_system, "DENSE-C");
        assert_eq!(relocations[0].target_belt, "DENSE-C-BELT-1");
        assert_eq!(
            relocations[0].pause_devices,
            ["MC-MOD-B", "SC-MOD-B"].map(str::to_owned)
        );
    }

    #[test]
    fn hub_protected_donor_ward_is_reused_before_displaced_site_ward() {
        let mut hub_ward = test_hub_device();
        hub_ward.key = replicant_client::DeviceKey::live("WARD-HUB".into());
        hub_ward.device_type = Some(DeviceType::SystemWard);
        hub_ward.location = Some(replicant_client::LocationKey::live("HUB-L4".into()));
        let mut moderate_ward = hub_ward.clone();
        moderate_ward.key = replicant_client::DeviceKey::live("WARD-MOD".into());
        moderate_ward.location = Some(replicant_client::LocationKey::live("MOD-L4".into()));

        let devices = vec![hub_ward, moderate_ward];
        let managed = BTreeSet::from(["HUB".to_owned(), "MOD".to_owned()]);
        let selected = BTreeSet::from(["DENSE".to_owned()]);
        let relays = BTreeSet::from(["DENSE".to_owned(), "HUB".to_owned(), "MOD".to_owned()]);
        let hubs = BTreeSet::from(["HUB".to_owned()]);
        let density = BTreeMap::from([
            ("DENSE".to_owned(), 3),
            ("HUB".to_owned(), 3),
            ("MOD".to_owned(), 2),
        ]);
        let belts = BTreeMap::from([("DENSE".to_owned(), vec!["DENSE-BELT-1".to_owned()])]);
        let locations = BTreeMap::from([
            ("HUB-L4".to_owned(), "HUB".to_owned()),
            ("MOD-L4".to_owned(), "MOD".to_owned()),
        ]);

        let relocations = plan_mining_ward_relocations(MiningWardRebalanceContext {
            devices: &devices,
            managed_systems: &managed,
            selected_ward_systems: &selected,
            relay_systems: &relays,
            hub_systems: &hubs,
            belt_density: &density,
            belt_designations: &belts,
            location_systems: &locations,
        });

        assert_eq!(relocations.len(), 1);
        assert_eq!(relocations[0].ward_code, "WARD-HUB");
        assert_eq!(relocations[0].source_system, "HUB");
        assert_eq!(relocations[0].target_system, "DENSE");
        assert!(relocations[0].pause_devices.is_empty());
    }

    #[test]
    fn ward_rebalance_never_strips_donor_for_disconnected_target() {
        let mut ward = test_hub_device();
        ward.key = replicant_client::DeviceKey::live("WARD-MOD".into());
        ward.device_type = Some(DeviceType::SystemWard);
        ward.location = Some(replicant_client::LocationKey::live("MOD-L4".into()));

        let relocations = plan_mining_ward_relocations(MiningWardRebalanceContext {
            devices: &[ward],
            managed_systems: &BTreeSet::from(["MOD".to_owned()]),
            selected_ward_systems: &BTreeSet::from(["DENSE".to_owned()]),
            relay_systems: &BTreeSet::from(["MOD".to_owned()]),
            hub_systems: &BTreeSet::new(),
            belt_density: &BTreeMap::from([("DENSE".to_owned(), 3), ("MOD".to_owned(), 2)]),
            belt_designations: &BTreeMap::from([(
                "DENSE".to_owned(),
                vec!["DENSE-BELT-1".to_owned()],
            )]),
            location_systems: &BTreeMap::from([("MOD-L4".to_owned(), "MOD".to_owned())]),
        });

        assert!(relocations.is_empty());
    }

    #[test]
    fn mining_waits_for_relay_connected_belt_systems() {
        let belts = BTreeSet::from(["CONNECTED".to_owned(), "UNREACHABLE".to_owned()]);
        let relays = BTreeSet::from(["CONNECTED".to_owned(), "OTHER".to_owned()]);
        let density = BTreeMap::from([("CONNECTED".to_owned(), 1), ("UNREACHABLE".to_owned(), 2)]);

        assert_eq!(
            relay_connected_mining_targets(&belts, &relays, &BTreeSet::new(), &density),
            vec!["CONNECTED".to_owned()]
        );
    }

    #[test]
    fn system_root_asteroid_belts_feed_director_mining_and_ftl_priority() {
        let root = Location {
            key: replicant_client::LocationKey::live("DELTA-STAR".into()),
            location_type: Some(replicant_client::domain::LocationType::from("star")),
            scanned: Some(true),
            system_scanned: Some(true),
            system_tags: Vec::new(),
            system: None,
            parent: None,
            custom_name: None,
            survey_progress: replicant_client::domain::LocationSurveyProgress::default(),
            environment: replicant_client::domain::LocationEnvironment::default(),
            unknown: BTreeMap::from([(
                "asteroid_belt".to_owned(),
                serde_json::json!({
                    "present": true,
                    "belts": [{"density": "dense"}]
                }),
            )]),
        };
        let locations = vec![root];
        let location_systems = location_system_map(&locations);
        let regions = BTreeMap::from([("DELTA-STAR".to_owned(), "delta".to_owned())]);

        assert_eq!(
            location_systems.get("DELTA-STAR").map(String::as_str),
            Some("DELTA-STAR")
        );
        assert_eq!(
            known_belt_systems(&locations, &location_systems, &regions, "delta"),
            BTreeSet::from(["DELTA-STAR".to_owned()])
        );
        assert_eq!(
            belt_density_priorities(&locations, &location_systems),
            BTreeMap::from([("DELTA-STAR".to_owned(), 3)])
        );
    }

    #[test]
    fn relay_coverage_counts_only_active_unstowed_relays() {
        let mut active = test_hub_device();
        active.key = replicant_client::DeviceKey::live("RELAY-ACTIVE".into());
        active.device_type = Some(DeviceType::FtlRelay);
        active.location = Some(replicant_client::LocationKey::live("CONNECTED-L4".into()));

        let mut inactive = active.clone();
        inactive.key = replicant_client::DeviceKey::live("RELAY-IDLE".into());
        inactive.status = Some(replicant_client::DeviceStatus::from("inactive"));
        inactive.location = Some(replicant_client::LocationKey::live("IDLE-L4".into()));

        let mut stowed = active.clone();
        stowed.key = replicant_client::DeviceKey::live("RELAY-STOWED".into());
        stowed.location = Some(replicant_client::LocationKey::live("STOWED-L4".into()));
        stowed.relationships.stowed_in = Some(replicant_client::DeviceKey::live("VESSEL".into()));

        let systems = BTreeMap::from([
            ("CONNECTED-L4".to_owned(), "CONNECTED".to_owned()),
            ("IDLE-L4".to_owned(), "IDLE".to_owned()),
            ("STOWED-L4".to_owned(), "STOWED".to_owned()),
        ]);

        assert_eq!(
            relay_device_systems(&[active, inactive, stowed], &systems),
            BTreeSet::from(["CONNECTED".to_owned()])
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
                pre_deactivate_device_codes: Vec::new(),
                release_mining_reservations: false,
                placement_recovery: None,
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
    fn catalogue_batches_cap_each_worker_at_twenty_systems() {
        let systems = (1..=500)
            .map(|index| format!("SYS-{index:03}"))
            .collect::<Vec<_>>();
        let shards = partition_catalogue_batch(&systems, 4);
        assert_eq!(shards.len(), 4);
        assert!(
            shards
                .iter()
                .all(|shard| shard.len() <= CATALOGUE_SYSTEMS_PER_WORKER)
        );
        assert_eq!(shards.iter().map(Vec::len).sum::<usize>(), 80);
    }

    #[test]
    fn catalogue_survey_scope_is_limited_to_thirty_ly_from_regional_hub() {
        let region = RegionView {
            region: "delta".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("HUB".to_owned()),
            hub_location: Some("HUB-BELT-1".to_owned()),
            known_systems: BTreeSet::from([
                "INSIDE".to_owned(),
                "EDGE".to_owned(),
                "OUTSIDE".to_owned(),
                "UNKNOWN".to_owned(),
            ]),
        };
        let catalogue = vec![
            positioned_star("HUB", 0.0, None),
            positioned_star("INSIDE", REGIONAL_AUTOMATION_RADIUS_LY - 0.1, Some("delta")),
            positioned_star("EDGE", REGIONAL_AUTOMATION_RADIUS_LY, Some("delta")),
            positioned_star(
                "OUTSIDE",
                REGIONAL_AUTOMATION_RADIUS_LY + 0.1,
                Some("delta"),
            ),
        ];

        let scope = catalogue_survey_scope_from_hub(&region, &catalogue)
            .expect("regional hub survey scope");

        assert_eq!(
            scope.systems,
            BTreeSet::from(["EDGE".to_owned(), "INSIDE".to_owned()])
        );
        assert_eq!(scope.missing_positions, 1);
    }

    #[test]
    fn catalogue_survey_scope_requires_a_positioned_regional_hub() {
        let region = RegionView {
            region: "delta".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("HUB".to_owned()),
            hub_location: Some("HUB-BELT-1".to_owned()),
            known_systems: BTreeSet::from(["INSIDE".to_owned()]),
        };
        let catalogue = vec![positioned_star("INSIDE", 1.0, Some("delta"))];

        assert!(catalogue_survey_scope_from_hub(&region, &catalogue).is_none());
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
    fn belt_discovery_launches_only_within_thirty_ly_of_the_regional_hub() {
        let repository = WorkflowRepository::open_in_memory().expect("workflow repository");
        let workflows = Vec::new();
        let controls = GoalControls::default();
        let context = GoalReconcileContext {
            repository: &repository,
            workflows: &workflows,
            controls: &controls,
            automatic: true,
            now: 0,
        };
        let region = RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("HUB".to_owned()),
            hub_location: Some("HUB-BELT-1".to_owned()),
            known_systems: BTreeSet::from([
                "INSIDE-NEAR".to_owned(),
                "INSIDE-FAR".to_owned(),
                "OUTSIDE".to_owned(),
                "UNKNOWN".to_owned(),
            ]),
        };
        let positions = BTreeMap::from([
            (
                "HUB".to_owned(),
                GalacticPosition {
                    x: 0.0,
                    y: 0.0,
                    z: 0.0,
                },
            ),
            (
                "INSIDE-NEAR".to_owned(),
                GalacticPosition {
                    x: 1.0,
                    y: 0.0,
                    z: 0.0,
                },
            ),
            (
                "INSIDE-FAR".to_owned(),
                GalacticPosition {
                    x: REGIONAL_AUTOMATION_RADIUS_LY,
                    y: 0.0,
                    z: 0.0,
                },
            ),
            (
                "OUTSIDE".to_owned(),
                GalacticPosition {
                    x: REGIONAL_AUTOMATION_RADIUS_LY + 0.1,
                    y: 0.0,
                    z: 0.0,
                },
            ),
        ]);
        let mut reserved = BTreeSet::new();
        let mut requirements =
            DirectorRequirementGraph::load(&repository, context.now).expect("requirements");

        let summary = reconcile_discover_belts(
            &context,
            &region,
            &[],
            &mut reserved,
            &mut requirements,
            &[],
            &BTreeMap::new(),
            &positions,
        )
        .expect("reconcile belt discovery");

        assert_eq!(summary.status, DirectorGoalStatus::Active);
        assert_eq!(summary.progress_total, 3);
        let workflow = repository
            .list()
            .expect("belt workflows")
            .into_iter()
            .next()
            .expect("belt workflow");
        let intent = workflow
            .config::<BeltSearchCampaignIntent>()
            .expect("belt intent");
        assert_eq!(
            intent.systems,
            ["INSIDE-NEAR".to_owned(), "INSIDE-FAR".to_owned()]
        );
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
        assert_eq!(
            snapshot.goals.len(),
            all_goal_kinds()
                .into_iter()
                .filter(|kind| !goal_is_regional(*kind))
                .count()
        );
        assert!(snapshot.workforce.scale_reason.is_some());

        drop(repository);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn cached_snapshot_overlays_durable_regional_goal_controls() {
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
            state: DirectorWorkerState::Unavailable,
        });
        stored.regions.push(DirectorRegionSummary {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("SCEPTURUM".to_owned()),
            hub_location: Some("SCEPTURUM-BELT-1".to_owned()),
            replicants: Vec::new(),
            known_systems: 4,
            operational_workers: 0,
            workers_in_transit: 0,
            busy_workers: 0,
        });
        stored.goals.push(DirectorGoalSummary {
            id: goal_instance_id(DirectorGoalKind::ExpandFtlNetwork, Some("alpha")),
            kind: DirectorGoalKind::ExpandFtlNetwork,
            region: Some("alpha".to_owned()),
            status: DirectorGoalStatus::Waiting,
            objective: "Extend regional relay reach".to_owned(),
            blocker: None,
            next_action: None,
            progress_current: 0,
            progress_total: 4,
            active_workflows: Vec::new(),
            enabled: true,
        });
        repository
            .put_document(SNAPSHOT_NS, SNAPSHOT_KEY, &stored)
            .expect("persist cached projection");

        set_director_mode(&repository, DirectorMode::Automatic).expect("set Director mode");
        set_goal_enabled(
            &repository,
            DirectorGoalKind::ExpandFtlNetwork,
            Some("alpha"),
            false,
        )
        .expect("disable Alpha FTL goal");
        assign_replicant_region(&repository, "CHAT-1", Some("Alpha"), Some("catalogue"))
            .expect("assign regional worker");

        let cached = cached_director_snapshot(&repository, 2).expect("read updated cache");
        assert_eq!(cached.metadata.revision, 2);

        assert_eq!(cached.mode, DirectorMode::Automatic);
        assert!(
            !cached
                .goals
                .iter()
                .find(|goal| {
                    goal.kind == DirectorGoalKind::ExpandFtlNetwork
                        && goal.region.as_deref() == Some("alpha")
                })
                .expect("Alpha FTL goal")
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
    fn regional_goal_controls_are_isolated() {
        let repository = WorkflowRepository::open_in_memory().expect("open workflow repository");
        set_goal_enabled(
            &repository,
            DirectorGoalKind::EnhanceStarCatalogue,
            Some("Alpha"),
            false,
        )
        .expect("disable Alpha catalogue goal");

        let controls =
            load_goal_controls(&repository, ["alpha", "beta"]).expect("load goal controls");

        assert!(!goal_enabled(
            &controls,
            DirectorGoalKind::EnhanceStarCatalogue,
            Some("alpha")
        ));
        assert!(goal_enabled(
            &controls,
            DirectorGoalKind::EnhanceStarCatalogue,
            Some("beta")
        ));
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
        assert_eq!(cached.metadata.revision, 99);
        assert!(cached.metadata.generated_at_ms >= expected.metadata.generated_at_ms);
        expected.metadata = cached.metadata.clone();
        assert_eq!(cached, expected);

        drop(repository);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn permanent_failure_is_retained_only_for_equivalent_director_work() {
        let repository = WorkflowRepository::open_in_memory().expect("open workflow repository");
        let workflow = repository
            .create(replicant_workflow::NewWorkflow {
                kind: exploration_workflow_kind(),
                schema_version: 1,
                config: ExplorationIntent {
                    target: "BETA".to_owned(),
                    replicant: None,
                    hub: None,
                },
                checkpoint: serde_json::Value::Null,
                current_step: None,
                parent_id: None,
            })
            .expect("create exploration workflow");
        let identity = GoalWorkIdentity::Exploration {
            target: "BETA".to_owned(),
        };
        let mut permanent = workflow.clone();
        permanent.status = WorkflowStatus::Failed;
        permanent.failure_disposition = Some(WorkflowFailureDisposition::Permanent);
        permanent.last_error = Some("immutable target is unavailable".to_owned());
        let mut runtime = GoalRuntime {
            active_workflows: vec![workflow.id],
            last_launch_at_ms: Some(10),
            launch_records: vec![GoalLaunchRecord {
                workflow_id: workflow.id,
                identity: identity.clone(),
            }],
            prospect_exhausted_signature: None,
        };

        let initial_rows = repository.list().expect("initial workflows").len();
        for _ in 0..2 {
            prune_runtime_workflows(&mut runtime, std::slice::from_ref(&permanent));
            assert!(runtime.active_workflows.is_empty());
            assert_eq!(runtime.launch_records.len(), 1);
            assert_eq!(
                permanent_failure_for_identity(
                    &runtime,
                    std::slice::from_ref(&permanent),
                    &identity
                )
                .and_then(|workflow| workflow.last_error.as_deref()),
                Some("immutable target is unavailable")
            );
            assert_eq!(
                repository.list().expect("workflows after reconcile").len(),
                initial_rows
            );
        }

        let changed = GoalWorkIdentity::Exploration {
            target: "GAMMA".to_owned(),
        };
        retain_work_identity(&mut runtime, &changed);
        assert!(runtime.launch_records.is_empty());
        assert_eq!(runtime.last_launch_at_ms, None);

        for disposition in [Some(WorkflowFailureDisposition::Retryable), None] {
            let mut failed = permanent.clone();
            failed.failure_disposition = disposition;
            let mut runtime = GoalRuntime {
                active_workflows: vec![failed.id],

                last_launch_at_ms: Some(10),
                launch_records: vec![GoalLaunchRecord {
                    workflow_id: failed.id,
                    identity: identity.clone(),
                }],
                prospect_exhausted_signature: None,
            };
            prune_runtime_workflows(&mut runtime, std::slice::from_ref(&failed));
            assert!(runtime.launch_records.is_empty());
        }
    }

    #[test]
    fn repeated_event_reconciliation_blocks_equivalent_permanent_work() {
        let repository = WorkflowRepository::open_in_memory().expect("workflow repository");
        let failed = repository
            .create(new_event_campaign_workflow(EventCampaignIntent {
                region: "alpha".to_owned(),
                home: "ALPHA-HUB".to_owned(),
            }))
            .expect("create failed campaign");
        let events = vec!["ALPHA-EVENT-1".to_owned()];
        let identity = GoalWorkIdentity::EventCampaign {
            region: "alpha".to_owned(),
            events: events.iter().cloned().collect(),
        };
        let mut failed_projection = failed.clone();
        failed_projection.status = WorkflowStatus::Failed;
        failed_projection.failure_disposition = Some(WorkflowFailureDisposition::Permanent);
        failed_projection.last_error = Some("campaign target cannot be fulfilled".to_owned());
        let workflows = vec![failed_projection];
        let goal_id = goal_instance_id(DirectorGoalKind::EventCompletion, Some("alpha"));
        save_goal_runtime(
            &repository,
            &goal_id,
            &GoalRuntime {
                active_workflows: vec![failed.id],
                last_launch_at_ms: Some(0),
                launch_records: vec![GoalLaunchRecord {
                    workflow_id: failed.id,
                    identity,
                }],
                prospect_exhausted_signature: None,
            },
        )
        .expect("save goal runtime");
        let controls = GoalControls::default();
        let context = GoalReconcileContext {
            repository: &repository,
            workflows: &workflows,
            controls: &controls,
            automatic: true,
            now: DEFAULT_RETRY_COOLDOWN_MS * 2,
        };
        let region = RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("ALPHA".to_owned()),
            hub_location: Some("ALPHA-HUB".to_owned()),
            known_systems: BTreeSet::from(["ALPHA".to_owned()]),
        };
        let workers = vec![test_worker("CHAT-1", "alpha", "ALPHA-HUB")];
        let initial_rows = repository.list().expect("initial workflows").len();

        for _ in 0..2 {
            let mut reserved = BTreeSet::new();
            let mut requirements =
                DirectorRequirementGraph::load(&repository, context.now).expect("requirements");
            let summary = reconcile_event_completion(
                &context,
                &region,
                &events,
                None,
                &workers,
                &mut reserved,
                &mut requirements,
            )
            .expect("reconcile equivalent events");
            assert_eq!(summary.status, DirectorGoalStatus::Blocked);
            assert_eq!(
                summary.blocker.as_deref(),
                Some("campaign target cannot be fulfilled")
            );
            assert_eq!(
                repository
                    .list()
                    .expect("workflows after blocked pass")
                    .len(),
                initial_rows
            );
        }

        let changed_events = vec!["ALPHA-EVENT-2".to_owned()];
        let mut reserved = BTreeSet::new();
        let mut requirements =
            DirectorRequirementGraph::load(&repository, context.now).expect("requirements");
        let summary = reconcile_event_completion(
            &context,
            &region,
            &changed_events,
            None,
            &workers,
            &mut reserved,
            &mut requirements,
        )
        .expect("reconcile changed events");
        assert_eq!(summary.status, DirectorGoalStatus::Active);
        assert_eq!(
            repository
                .list()
                .expect("workflows after identity change")
                .len(),
            initial_rows + 1
        );
    }

    #[test]
    fn repeated_ftl_reconciliation_blocks_equivalent_permanent_work() {
        let repository = WorkflowRepository::open_in_memory().expect("workflow repository");
        let failed = repository
            .create(new_exploration_workflow(ExplorationIntent {
                target: "TARGET".to_owned(),
                replicant: Some("CHAT-1".to_owned()),
                hub: Some("ROOT-HUB".to_owned()),
            }))
            .expect("create failed exploration");
        let identity = GoalWorkIdentity::Exploration {
            target: "TARGET".to_owned(),
        };
        let mut failed_projection = failed.clone();
        failed_projection.status = WorkflowStatus::Failed;
        failed_projection.failure_disposition = Some(WorkflowFailureDisposition::Permanent);
        failed_projection.last_error = Some("target cannot be connected".to_owned());
        let workflows = vec![failed_projection];
        let goal_id = goal_instance_id(DirectorGoalKind::ExpandFtlNetwork, Some("alpha"));
        save_goal_runtime(
            &repository,
            &goal_id,
            &GoalRuntime {
                active_workflows: vec![failed.id],
                last_launch_at_ms: Some(0),
                launch_records: vec![GoalLaunchRecord {
                    workflow_id: failed.id,
                    identity,
                }],
                prospect_exhausted_signature: None,
            },
        )
        .expect("save goal runtime");
        let controls = GoalControls::default();
        let context = GoalReconcileContext {
            repository: &repository,
            workflows: &workflows,
            controls: &controls,
            automatic: true,
            now: DEFAULT_RETRY_COOLDOWN_MS * 2,
        };
        let region = RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("ROOT".to_owned()),
            hub_location: Some("ROOT-HUB".to_owned()),
            known_systems: BTreeSet::from(["TARGET".to_owned()]),
        };
        let catalogue_positions = BTreeMap::from([
            (
                "ROOT".to_owned(),
                GalacticPosition {
                    x: 0.0,
                    y: 0.0,
                    z: 0.0,
                },
            ),
            (
                "TARGET".to_owned(),
                GalacticPosition {
                    x: 1.0,
                    y: 0.0,
                    z: 0.0,
                },
            ),
            (
                "NEXT-TARGET".to_owned(),
                GalacticPosition {
                    x: 2.0,
                    y: 0.0,
                    z: 0.0,
                },
            ),
        ]);
        let mut worker = test_worker("CHAT-1", "alpha", "ROOT-HUB");
        worker.racing_vessel = Some("VESSEL-1".to_owned());
        let workers = vec![worker];
        let mut vessel = test_hub_device();
        vessel.key = replicant_client::DeviceKey::live("VESSEL-1".into());
        vessel.device_type = Some(DeviceType::RacingVessel);
        vessel.stow_capacity = Some(2);
        vessel.stow_used = Some(0);
        let devices = vec![vessel];
        let initial_rows = repository.list().expect("initial workflows").len();

        for _ in 0..2 {
            let mut reserved = BTreeSet::new();
            let mut requirements =
                DirectorRequirementGraph::load(&repository, context.now).expect("requirements");
            let summary = reconcile_expand_ftl_network(
                &context,
                &region,
                &workers,
                &mut reserved,
                &mut requirements,
                &devices,
                &[],
                &BTreeMap::new(),
                &catalogue_positions,
                &BTreeSet::from(["TARGET".to_owned()]),
            )
            .expect("reconcile equivalent FTL target");
            assert_eq!(summary.status, DirectorGoalStatus::Blocked);
            assert_eq!(
                summary.blocker.as_deref(),
                Some("target cannot be connected")
            );
            assert_eq!(
                repository
                    .list()
                    .expect("workflows after blocked pass")
                    .len(),
                initial_rows
            );
        }

        let changed_region = RegionView {
            known_systems: BTreeSet::from(["NEXT-TARGET".to_owned()]),
            ..region
        };
        let mut reserved = BTreeSet::new();
        let mut requirements =
            DirectorRequirementGraph::load(&repository, context.now).expect("requirements");
        let summary = reconcile_expand_ftl_network(
            &context,
            &changed_region,
            &workers,
            &mut reserved,
            &mut requirements,
            &devices,
            &[],
            &BTreeMap::new(),
            &catalogue_positions,
            &BTreeSet::from(["NEXT-TARGET".to_owned()]),
        )
        .expect("reconcile changed FTL target");
        assert_eq!(summary.status, DirectorGoalStatus::Active);
        assert_eq!(
            repository
                .list()
                .expect("workflows after changed target")
                .len(),
            initial_rows + 1
        );
    }

    #[test]
    fn ftl_expansion_ignores_prioritized_targets_beyond_thirty_ly() {
        let repository = WorkflowRepository::open_in_memory().expect("workflow repository");
        let workflows = Vec::new();
        let controls = GoalControls::default();
        let context = GoalReconcileContext {
            repository: &repository,
            workflows: &workflows,
            controls: &controls,
            automatic: true,
            now: 0,
        };
        let region = RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("HUB".to_owned()),
            hub_location: Some("HUB-BELT-1".to_owned()),
            known_systems: BTreeSet::from([
                "INSIDE".to_owned(),
                "OUTSIDE".to_owned(),
                "UNKNOWN".to_owned(),
            ]),
        };
        let positions = BTreeMap::from([
            (
                "HUB".to_owned(),
                GalacticPosition {
                    x: 0.0,
                    y: 0.0,
                    z: 0.0,
                },
            ),
            (
                "INSIDE".to_owned(),
                GalacticPosition {
                    x: REGIONAL_AUTOMATION_RADIUS_LY,
                    y: 0.0,
                    z: 0.0,
                },
            ),
            (
                "OUTSIDE".to_owned(),
                GalacticPosition {
                    x: REGIONAL_AUTOMATION_RADIUS_LY + 0.1,
                    y: 0.0,
                    z: 0.0,
                },
            ),
        ]);
        let mut worker = test_worker("CHAT-1", "alpha", "HUB-BELT-1");
        worker.racing_vessel = Some("VESSEL-1".to_owned());
        let mut vessel = test_hub_device();
        vessel.key = replicant_client::DeviceKey::live("VESSEL-1".into());
        vessel.device_type = Some(DeviceType::RacingVessel);
        vessel.stow_capacity = Some(2);
        vessel.stow_used = Some(0);
        let mut reserved = BTreeSet::new();
        let mut requirements =
            DirectorRequirementGraph::load(&repository, context.now).expect("requirements");

        let summary = reconcile_expand_ftl_network(
            &context,
            &region,
            &[worker],
            &mut reserved,
            &mut requirements,
            &[vessel],
            &[],
            &BTreeMap::new(),
            &positions,
            &BTreeSet::from(["OUTSIDE".to_owned()]),
        )
        .expect("reconcile FTL expansion");

        assert_eq!(summary.status, DirectorGoalStatus::Active);
        assert_eq!(summary.progress_total, 2);
        let workflow = repository
            .list()
            .expect("FTL workflows")
            .into_iter()
            .next()
            .expect("FTL workflow");
        let intent = workflow
            .config::<ExplorationIntent>()
            .expect("exploration intent");
        assert_eq!(intent.target, "INSIDE");
    }

    #[test]
    fn event_reconciliation_recreates_retryable_and_legacy_failures_after_cooldown() {
        for disposition in [Some(WorkflowFailureDisposition::Retryable), None] {
            let repository = WorkflowRepository::open_in_memory().expect("workflow repository");
            let failed = repository
                .create(new_event_campaign_workflow(EventCampaignIntent {
                    region: "alpha".to_owned(),
                    home: "ALPHA-HUB".to_owned(),
                }))
                .expect("create failed campaign");
            let events = vec!["ALPHA-EVENT-1".to_owned()];
            let identity = GoalWorkIdentity::EventCampaign {
                region: "alpha".to_owned(),
                events: events.iter().cloned().collect(),
            };
            let mut failed_projection = failed.clone();
            failed_projection.status = WorkflowStatus::Failed;
            failed_projection.failure_disposition = disposition;
            let workflows = vec![failed_projection];
            save_goal_runtime(
                &repository,
                &goal_instance_id(DirectorGoalKind::EventCompletion, Some("alpha")),
                &GoalRuntime {
                    active_workflows: vec![failed.id],
                    last_launch_at_ms: Some(0),
                    launch_records: vec![GoalLaunchRecord {
                        workflow_id: failed.id,
                        identity,
                    }],
                    prospect_exhausted_signature: None,
                },
            )
            .expect("save goal runtime");
            let controls = GoalControls::default();
            let context = GoalReconcileContext {
                repository: &repository,
                workflows: &workflows,
                controls: &controls,
                automatic: true,
                now: DEFAULT_RETRY_COOLDOWN_MS + 1,
            };
            let region = RegionView {
                region: "alpha".to_owned(),
                status: DirectorRegionStatus::Established,
                hub_system: Some("ALPHA".to_owned()),
                hub_location: Some("ALPHA-HUB".to_owned()),
                known_systems: BTreeSet::from(["ALPHA".to_owned()]),
            };
            let workers = vec![test_worker("CHAT-1", "alpha", "ALPHA-HUB")];
            let initial_rows = repository.list().expect("initial workflows").len();
            let mut reserved = BTreeSet::new();
            let mut requirements =
                DirectorRequirementGraph::load(&repository, context.now).expect("requirements");

            let summary = reconcile_event_completion(
                &context,
                &region,
                &events,
                None,
                &workers,
                &mut reserved,
                &mut requirements,
            )
            .expect("reconcile retryable campaign");

            assert_eq!(summary.status, DirectorGoalStatus::Active);
            assert_eq!(
                repository.list().expect("workflows after retry").len(),
                initial_rows + 1
            );
        }
    }
    #[test]
    fn salvage_recovery_goal_defaults_disabled_and_is_regional() {
        assert!(!default_goal_enabled(DirectorGoalKind::SalvageRecovery));
        assert!(goal_is_regional(DirectorGoalKind::SalvageRecovery));
        assert_eq!(
            goal_kind_key(DirectorGoalKind::SalvageRecovery),
            "salvage_recovery"
        );
        assert_eq!(
            parse_goal_kind("salvage_recovery"),
            Some(DirectorGoalKind::SalvageRecovery)
        );
        assert_eq!(
            initial_goal_objective(DirectorGoalKind::SalvageRecovery),
            "Recover discovered regional salvage"
        );
    }

    #[test]
    fn salvage_recovery_workflow_matching_requires_nonblank_home_and_active_exact_region() {
        let repository = WorkflowRepository::open_in_memory().expect("workflow repository");
        let workflow = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: " SOL ".to_owned(),
                home: " SOL-HUB ".to_owned(),
            }))
            .expect("salvage recovery workflow");
        assert!(
            salvage_recovery_workflow_matches(&workflow, "solzone")
                .expect("match compatible workflow")
        );
        assert!(
            !salvage_recovery_workflow_matches(&workflow, "alpha")
                .expect("check incompatible region")
        );

        let whitespace = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "alpha".to_owned(),
                home: " \t".to_owned(),
            }))
            .expect("whitespace-home workflow");
        assert!(
            !salvage_recovery_workflow_matches(&whitespace, "alpha")
                .expect("reject whitespace home")
        );
    }

    #[test]
    fn salvage_recovery_goal_disabled_does_not_adopt_or_create() {
        let repository = WorkflowRepository::open_in_memory().expect("workflow repository");
        let workflow = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "alpha".to_owned(),
                home: "ALPHA-HUB".to_owned(),
            }))
            .expect("salvage recovery workflow");
        let controls = GoalControls::default();
        let context = GoalReconcileContext {
            repository: &repository,
            workflows: std::slice::from_ref(&workflow),
            controls: &controls,
            automatic: true,
            now: 10,
        };
        let region = RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("ALPHA".to_owned()),
            hub_location: Some("ALPHA-HUB".to_owned()),
            known_systems: BTreeSet::from(["ALPHA".to_owned()]),
        };
        let summary = reconcile_salvage_recovery(&context, &region, None, &BTreeSet::new(), None)
            .expect("disabled salvage reconciliation");
        assert_eq!(summary.status, DirectorGoalStatus::Waiting);
        assert!(summary.active_workflows.is_empty());
        assert_eq!(repository.list().expect("workflow rows").len(), 1);
    }

    #[test]
    fn salvage_recovery_goal_adopts_all_compatible_manual_campaigns() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let first = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "alpha".into(),
                home: "ALPHA-HUB".into(),
            }))
            .expect("first campaign");
        let second = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "ALPHA".into(),
                home: "OTHER-HUB".into(),
            }))
            .expect("second campaign");
        let workflows = repository.list().expect("workflows");
        let controls = salvage_enabled_controls(true);
        let context = salvage_context(&repository, &workflows, &controls, false, 10);
        let summary = reconcile_salvage_recovery(
            &context,
            &salvage_test_region(Some("ALPHA-HUB")),
            Some(&salvage_test_snapshot(&["SITE-1"])),
            &BTreeSet::new(),
            None,
        )
        .expect("adopt campaigns");
        assert_eq!(summary.status, DirectorGoalStatus::Active);
        assert_eq!(summary.active_workflows.len(), 2);
        assert!(
            summary
                .active_workflows
                .contains(&ProtocolWorkflowId(first.id.to_string()))
        );
        assert!(
            summary
                .active_workflows
                .contains(&ProtocolWorkflowId(second.id.to_string()))
        );
    }

    #[test]
    fn salvage_recovery_goal_records_adopted_retry_and_permanent_identity() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let first = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "alpha".into(),
                home: "ALPHA-HUB".into(),
            }))
            .expect("first campaign");
        let second = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "alpha".into(),
                home: "OTHER-HUB".into(),
            }))
            .expect("second campaign");
        let controls = salvage_enabled_controls(true);
        let snapshot = salvage_test_snapshot(&["SITE-1"]);
        let region = salvage_test_region(Some("ALPHA-HUB"));
        let workflows = repository.list().expect("workflows");
        reconcile_salvage_recovery(
            &salvage_context(&repository, &workflows, &controls, false, 10),
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("adopt campaigns");

        let mut failed = first;
        failed.status = WorkflowStatus::Failed;
        failed.failure_disposition = Some(WorkflowFailureDisposition::Permanent);
        failed.last_error = Some("salvage campaign failed".into());
        let active_and_failed = [failed.clone(), second.clone()];
        let active = reconcile_salvage_recovery(
            &salvage_context(
                &repository,
                &active_and_failed,
                &controls,
                true,
                DEFAULT_RETRY_COOLDOWN_MS + 1,
            ),
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("retain permanent identity while peer remains active");
        assert_eq!(active.status, DirectorGoalStatus::Active);

        let mut succeeded = second;
        succeeded.status = WorkflowStatus::Succeeded;
        let terminal = [failed, succeeded];
        let summary = reconcile_salvage_recovery(
            &salvage_context(
                &repository,
                &terminal,
                &controls,
                true,
                DEFAULT_RETRY_COOLDOWN_MS + 2,
            ),
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("retain permanent terminal identity");
        assert_eq!(summary.status, DirectorGoalStatus::Blocked);
        assert_eq!(summary.blocker.as_deref(), Some("salvage campaign failed"));
    }

    #[test]
    fn salvage_recovery_goal_treats_unobserved_manual_failure_as_retryable() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let campaign = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "alpha".into(),
                home: "ALPHA-HUB".into(),
            }))
            .expect("campaign");
        let failed = repository
            .update(
                campaign.id,
                campaign.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Failed,
                    current_step: Some("failed".to_owned()),
                    checkpoint: Value::Null,
                    last_error: Some("unobserved failure".to_owned()),
                    result: None::<Value>,
                },
            )
            .expect("persist failed campaign");
        assert_eq!(
            failed.failure_disposition,
            Some(WorkflowFailureDisposition::Retryable)
        );
        let controls = salvage_enabled_controls(true);
        let region = salvage_test_region(Some("ALPHA-HUB"));
        let snapshot = salvage_test_snapshot(&["SITE-1"]);
        let before_cooldown = salvage_context(
            &repository,
            std::slice::from_ref(&failed),
            &controls,
            true,
            failed
                .created_at
                .saturating_add(DEFAULT_RETRY_COOLDOWN_MS - 1),
        );
        let waiting = reconcile_salvage_recovery(
            &before_cooldown,
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("wait for unobserved failure cooldown");
        assert_eq!(waiting.status, DirectorGoalStatus::Waiting);
        assert_eq!(repository.list().expect("waiting workflows").len(), 1);

        let after_cooldown = salvage_context(
            &repository,
            std::slice::from_ref(&failed),
            &controls,
            true,
            failed
                .created_at
                .saturating_add(DEFAULT_RETRY_COOLDOWN_MS + 1),
        );
        let summary = reconcile_salvage_recovery(
            &after_cooldown,
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("retry unobserved failure");
        assert_eq!(summary.status, DirectorGoalStatus::Active);
        assert_eq!(repository.list().expect("workflows").len(), 2);

        let legacy_repository =
            WorkflowRepository::open_in_memory().expect("legacy workflow repository");
        let legacy_campaign = legacy_repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "alpha".into(),
                home: "ALPHA-HUB".into(),
            }))
            .expect("legacy campaign");
        let mut legacy_failed = legacy_repository
            .update(
                legacy_campaign.id,
                legacy_campaign.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Failed,
                    current_step: Some("failed".to_owned()),
                    checkpoint: Value::Null,
                    last_error: Some("legacy unobserved failure".to_owned()),
                    result: None::<Value>,
                },
            )
            .expect("persist legacy failed campaign");
        legacy_failed.failure_disposition = None;
        let legacy_before_cooldown = salvage_context(
            &legacy_repository,
            std::slice::from_ref(&legacy_failed),
            &controls,
            true,
            legacy_failed
                .created_at
                .saturating_add(DEFAULT_RETRY_COOLDOWN_MS - 1),
        );
        let legacy_waiting = reconcile_salvage_recovery(
            &legacy_before_cooldown,
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("wait for legacy failure cooldown");
        assert_eq!(legacy_waiting.status, DirectorGoalStatus::Waiting);
        assert_eq!(
            legacy_repository
                .list()
                .expect("legacy waiting workflows")
                .len(),
            1
        );

        let legacy_after_cooldown = salvage_context(
            &legacy_repository,
            std::slice::from_ref(&legacy_failed),
            &controls,
            true,
            legacy_failed
                .created_at
                .saturating_add(DEFAULT_RETRY_COOLDOWN_MS + 1),
        );
        let legacy_summary = reconcile_salvage_recovery(
            &legacy_after_cooldown,
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("retry legacy unobserved failure");
        assert_eq!(legacy_summary.status, DirectorGoalStatus::Active);
        assert_eq!(
            legacy_repository
                .list()
                .expect("legacy retried workflows")
                .len(),
            2
        );
    }

    #[test]
    fn salvage_recovery_goal_creates_or_reuses_atomically() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let controls = salvage_enabled_controls(true);
        let region = salvage_test_region(Some("ALPHA-HUB"));
        let snapshot = salvage_test_snapshot(&["SITE-1"]);
        let first = reconcile_salvage_recovery(
            &salvage_context(&repository, &[], &controls, true, 10),
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("create campaign");
        let workflows = repository.list().expect("workflows");
        let second = reconcile_salvage_recovery(
            &salvage_context(&repository, &workflows, &controls, true, 20),
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("reuse campaign");
        assert_eq!(first.active_workflows, second.active_workflows);
        assert_eq!(repository.list().expect("workflows").len(), 1);
    }

    #[test]
    fn salvage_recovery_goal_disabled_or_advisory_does_not_create() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let region = salvage_test_region(Some("ALPHA-HUB"));
        let snapshot = salvage_test_snapshot(&["SITE-1"]);
        let disabled_controls = salvage_enabled_controls(false);
        let disabled = salvage_context(&repository, &[], &disabled_controls, true, 10);
        assert_eq!(
            reconcile_salvage_recovery(
                &disabled,
                &region,
                Some(&snapshot),
                &BTreeSet::new(),
                None,
            )
            .expect("disabled reconcile")
            .status,
            DirectorGoalStatus::Waiting
        );
        let advisory_controls = salvage_enabled_controls(true);
        let advisory = salvage_context(&repository, &[], &advisory_controls, false, 10);
        assert_eq!(
            reconcile_salvage_recovery(
                &advisory,
                &region,
                Some(&snapshot),
                &BTreeSet::new(),
                None,
            )
            .expect("advisory reconcile")
            .status,
            DirectorGoalStatus::Active
        );
        assert!(repository.list().expect("workflows").is_empty());
    }

    #[test]
    fn salvage_recovery_goal_reports_discovery_and_home_blockers() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let controls = salvage_enabled_controls(true);
        let context = salvage_context(&repository, &[], &controls, true, 10);
        let summary = reconcile_salvage_recovery(
            &context,
            &salvage_test_region(Some("ALPHA-HUB")),
            None,
            &BTreeSet::new(),
            Some("remote history unavailable"),
        )
        .expect("discovery blocker");
        assert_eq!(summary.status, DirectorGoalStatus::Blocked);
        assert!(
            summary
                .blocker
                .as_deref()
                .unwrap()
                .contains("remote history unavailable")
        );
        let summary = reconcile_salvage_recovery(
            &context,
            &salvage_test_region(None),
            Some(&salvage_test_snapshot(&["SITE-1"])),
            &BTreeSet::new(),
            None,
        )
        .expect("home blocker");
        assert_eq!(summary.status, DirectorGoalStatus::Blocked);
        assert_eq!(
            summary.blocker.as_deref(),
            Some("alpha has no operational regional home for salvage recovery")
        );
    }

    #[test]
    fn salvage_recovery_goal_satisfies_after_completion_and_reopens_for_new_site() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let controls = salvage_enabled_controls(true);
        let region = salvage_test_region(Some("ALPHA-HUB"));
        let initial = salvage_test_snapshot(&["SITE-1"]);
        let context = salvage_context(&repository, &[], &controls, false, 10);
        assert_eq!(
            reconcile_salvage_recovery(&context, &region, Some(&initial), &BTreeSet::new(), None)
                .expect("initial backlog")
                .status,
            DirectorGoalStatus::Active
        );
        let completed = BTreeSet::from(["SITE-1".to_owned()]);
        assert_eq!(
            reconcile_salvage_recovery(&context, &region, Some(&initial), &completed, None)
                .expect("completed backlog")
                .status,
            DirectorGoalStatus::Satisfied
        );
        let reopened = salvage_test_snapshot(&["SITE-1", "SITE-2"]);
        let summary =
            reconcile_salvage_recovery(&context, &region, Some(&reopened), &completed, None)
                .expect("new backlog");
        assert_eq!(summary.status, DirectorGoalStatus::Active);
        assert_eq!(summary.progress_total, 1);
    }

    #[test]
    fn salvage_recovery_goal_blocks_equivalent_permanent_failure_but_allows_changed_sites() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let campaign = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "alpha".into(),
                home: "ALPHA-HUB".into(),
            }))
            .expect("campaign");
        let controls = salvage_enabled_controls(true);
        let region = salvage_test_region(Some("ALPHA-HUB"));
        let snapshot = salvage_test_snapshot(&["SITE-1"]);
        reconcile_salvage_recovery(
            &salvage_context(
                &repository,
                &repository.list().expect("workflows"),
                &controls,
                false,
                1,
            ),
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("observe campaign");
        let mut failed = repository
            .update(
                campaign.id,
                campaign.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Failed,
                    current_step: Some("failed".to_owned()),
                    checkpoint: Value::Null,
                    last_error: Some("permanent".to_owned()),
                    result: None::<Value>,
                },
            )
            .expect("persist terminal campaign");
        failed.failure_disposition = Some(WorkflowFailureDisposition::Permanent);
        let summary = reconcile_salvage_recovery(
            &salvage_context(
                &repository,
                std::slice::from_ref(&failed),
                &controls,
                true,
                DEFAULT_RETRY_COOLDOWN_MS + 1,
            ),
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("block equivalent");
        assert_eq!(summary.status, DirectorGoalStatus::Blocked);
        let changed = salvage_test_snapshot(&["SITE-2"]);
        let summary = reconcile_salvage_recovery(
            &salvage_context(
                &repository,
                std::slice::from_ref(&failed),
                &controls,
                true,
                DEFAULT_RETRY_COOLDOWN_MS + 1,
            ),
            &region,
            Some(&changed),
            &BTreeSet::new(),
            None,
        )
        .expect("allow changed sites");
        assert_eq!(repository.list().expect("changed campaigns").len(), 2);
        assert_ne!(
            summary.active_workflows,
            vec![ProtocolWorkflowId(campaign.id.to_string())]
        );
        assert_eq!(summary.status, DirectorGoalStatus::Active);
    }

    #[tokio::test]
    async fn salvage_recovery_cache_reuses_snapshot_until_forced_refresh() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/stars"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "generated_at": "2026-08-28T00:00:00Z",
                "stars": [{"designation": "ROOT", "region": "alpha"}]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("event", "salvage.discovered"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [{
                    "id": "2-0", "version": 1, "category": "salvage",
                    "event": "salvage.discovered", "created_at": "2026-08-28T00:00:00Z",
                    "payload": {"designation": "SITE-REMOTE", "location": "ROOT-1-L4"}
                }],
                "next_cursor": null
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("event", "salvage.depleted"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [], "next_cursor": null
            })))
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let snapshot = salvage_test_snapshot(&["SITE-1"]);
        repository
            .put_document(
                SALVAGE_RECOVERY_CACHE_NS,
                SALVAGE_RECOVERY_CACHE_KEY,
                &SalvageRecoveryCache {
                    refreshed_at_ms: 100,
                    snapshot: snapshot.clone(),
                },
            )
            .expect("cache snapshot");
        let cached = salvage_recovery_history_for_director(&client, &repository, 101, false)
            .await
            .expect("fresh cache");
        assert_eq!(cached.discovered_count, snapshot.discovered_count);
        assert!(
            cached
                .sites_by_region
                .get("alpha")
                .is_some_and(|sites| sites.contains_key("SITE-1"))
        );
        let refreshed = salvage_recovery_history_for_director(&client, &repository, 101, true)
            .await
            .expect("forced refresh");
        assert_eq!(refreshed.discovered_count, 1);
        assert!(
            refreshed
                .sites_by_region
                .get("alpha")
                .is_some_and(|sites| sites.contains_key("SITE-REMOTE"))
        );
        client.close().await.expect("close client");
    }

    #[test]
    fn salvage_recovery_prechange_documents_and_manual_campaign_open_unchanged() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        repository
            .put_document(
                "director.settings",
                "singleton",
                &serde_json::json!({"mode":"advisory"}),
            )
            .expect("settings");
        repository
            .put_document("legacy.runtime", "keep", &serde_json::json!({"value": 7}))
            .expect("legacy document");
        let campaign = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "alpha".into(),
                home: "ALPHA-HUB".into(),
            }))
            .expect("manual campaign");
        let settings = director_settings(&repository).expect("legacy settings decode");
        assert_eq!(settings.mode, DirectorMode::Advisory);
        assert_eq!(settings.idle_target, DEFAULT_IDLE_TARGET);
        assert_eq!(settings.scale_up_idle_threshold, DEFAULT_SCALE_THRESHOLD);
        assert_eq!(settings.scale_up_hold_ms, DEFAULT_HOLD_MS);
        assert_eq!(settings.scale_up_cooldown_ms, DEFAULT_SCALE_COOLDOWN_MS);
        assert_eq!(settings.prospect_cooldown_ms, DEFAULT_PROSPECT_COOLDOWN_MS);
        let controls = GoalControls::default();
        let context = salvage_context(
            &repository,
            std::slice::from_ref(&campaign),
            &controls,
            true,
            10,
        );
        let summary = reconcile_salvage_recovery(
            &context,
            &salvage_test_region(Some("ALPHA-HUB")),
            None,
            &BTreeSet::new(),
            None,
        )
        .expect("disabled goal");
        assert!(!summary.enabled);
        assert_eq!(repository.list().expect("campaigns").len(), 1);
        assert_eq!(
            repository
                .read_document("legacy.runtime", "keep")
                .expect("legacy read")
                .map(|(value, _)| value["value"].as_i64()),
            Some(Some(7))
        );
    }

    #[tokio::test]
    async fn salvage_recovery_director_lifecycle_is_continuous_and_idempotent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/stars"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "generated_at": "2026-08-28T00:00:00Z",
                "stars": [{"designation": "ROOT", "region": "alpha"}]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("event", "salvage.discovered"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [{
                    "id": "1-0", "version": 1, "category": "salvage",
                    "event": "salvage.discovered", "created_at": "2026-08-28T00:00:00Z",
                    "payload": {"designation": "SITE-REMOTE", "location": "ROOT-1-L4"}
                }],
                "next_cursor": null
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("event", "salvage.depleted"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [], "next_cursor": null
            })))
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let region = salvage_test_region(Some("ALPHA-HUB"));
        let disabled = salvage_enabled_controls(false);
        assert_eq!(
            reconcile_salvage_recovery(
                &salvage_context(&repository, &[], &disabled, true, 1),
                &region,
                Some(&salvage_test_snapshot(&["SITE-1"])),
                &BTreeSet::new(),
                None,
            )
            .expect("disabled")
            .status,
            DirectorGoalStatus::Waiting
        );
        let enabled = salvage_enabled_controls(true);
        assert_eq!(
            reconcile_salvage_recovery(
                &salvage_context(&repository, &[], &enabled, false, 2),
                &region,
                Some(&salvage_test_snapshot(&["SITE-1"])),
                &BTreeSet::new(),
                None,
            )
            .expect("advisory")
            .status,
            DirectorGoalStatus::Active
        );
        let snapshot = salvage_test_snapshot(&["SITE-1"]);
        let catalogue = crate::catalogue::OperationCatalogue::new().expect("catalogue");
        let manual = catalogue
            .create_workflow(
                &repository,
                "salvage.recovery",
                BTreeMap::from([
                    ("region".to_owned(), serde_json::json!("alpha")),
                    ("home".to_owned(), serde_json::json!("ALPHA-HUB")),
                ]),
            )
            .expect("manual Template campaign");
        assert_eq!(repository.list().expect("campaigns").len(), 1);
        let workflows = repository.list().expect("campaigns");
        let created = reconcile_salvage_recovery(
            &salvage_context(&repository, &workflows, &enabled, true, 3),
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("automatic adoption");
        assert_eq!(
            created.active_workflows,
            vec![ProtocolWorkflowId(manual.id.to_string())]
        );
        let repeated = reconcile_salvage_recovery(
            &salvage_context(&repository, &workflows, &enabled, true, 4),
            &region,
            Some(&snapshot),
            &BTreeSet::new(),
            None,
        )
        .expect("idempotent");
        assert_eq!(created.active_workflows, repeated.active_workflows);
        assert_eq!(repository.list().expect("campaigns").len(), 1);
        let completed = BTreeSet::from(["SITE-1".to_owned()]);
        let no_active = Vec::new();
        assert_eq!(
            reconcile_salvage_recovery(
                &salvage_context(&repository, &no_active, &enabled, false, 5),
                &region,
                Some(&snapshot),
                &completed,
                None,
            )
            .expect("completion")
            .status,
            DirectorGoalStatus::Satisfied
        );
        let reopened = salvage_test_snapshot(&["SITE-1", "SITE-2"]);
        assert_eq!(
            reconcile_salvage_recovery(
                &salvage_context(&repository, &no_active, &enabled, false, 6),
                &region,
                Some(&reopened),
                &completed,
                None,
            )
            .expect("reopened")
            .status,
            DirectorGoalStatus::Active
        );
        repository
            .put_document(
                SALVAGE_RECOVERY_CACHE_NS,
                SALVAGE_RECOVERY_CACHE_KEY,
                &SalvageRecoveryCache {
                    refreshed_at_ms: 1,
                    snapshot: salvage_test_snapshot(&["SITE-1"]),
                },
            )
            .expect("seed history cache");
        let refreshed = salvage_recovery_history_for_director(&client, &repository, 2, true)
            .await
            .expect("forced history refresh");
        assert!(
            refreshed
                .sites_by_region
                .get("alpha")
                .is_some_and(|sites| sites.contains_key("SITE-REMOTE"))
        );
        let refreshed_summary = reconcile_salvage_recovery(
            &salvage_context(&repository, &no_active, &enabled, false, 7),
            &region,
            Some(&refreshed),
            &completed,
            None,
        )
        .expect("reconcile forced history refresh");
        assert_eq!(refreshed_summary.status, DirectorGoalStatus::Active);
        assert_eq!(refreshed_summary.progress_total, 1);
        client.close().await.expect("close client");
    }

    #[test]
    fn salvage_recovery_director_repeated_passes_are_idempotent() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let region = salvage_test_region(Some("ALPHA-HUB"));
        let snapshot = salvage_test_snapshot(&["SITE-1"]);
        let disabled = salvage_enabled_controls(false);
        for pass in 0_i64..1_440_i64 {
            let workflows = repository.list().expect("workflows");
            let summary = reconcile_salvage_recovery(
                &salvage_context(&repository, &workflows, &disabled, true, pass * 30_000),
                &region,
                Some(&snapshot),
                &BTreeSet::new(),
                None,
            )
            .expect("disabled reconcile pass");
            assert_eq!(summary.status, DirectorGoalStatus::Waiting);
            assert!(repository.list().expect("workflows").is_empty());
        }
        let controls = salvage_enabled_controls(true);
        for pass in 0_i64..1_440_i64 {
            let workflows = repository.list().expect("workflows");
            let summary = reconcile_salvage_recovery(
                &salvage_context(&repository, &workflows, &controls, true, pass * 30_000),
                &region,
                Some(&snapshot),
                &BTreeSet::new(),
                None,
            )
            .expect("enabled reconcile pass");
            assert_eq!(summary.status, DirectorGoalStatus::Active);
        }
        assert_eq!(repository.list().expect("workflows").len(), 1);
    }

    #[test]
    fn director_asteroid_diversion_registry_is_disabled_regional_and_wakes_on_lifecycle_events() {
        assert!(!default_goal_enabled(DirectorGoalKind::AsteroidDiversion));
        assert!(goal_is_regional(DirectorGoalKind::AsteroidDiversion));
        assert_eq!(
            goal_kind_key(DirectorGoalKind::AsteroidDiversion),
            "asteroid_diversion"
        );
        assert_eq!(
            parse_goal_kind("asteroid_diversion"),
            Some(DirectorGoalKind::AsteroidDiversion)
        );
        assert_eq!(
            initial_goal_objective(DirectorGoalKind::AsteroidDiversion),
            "Divert incoming asteroids threatening regional systems"
        );
        assert_eq!(
            director_reconcile_event_names(),
            &[
                "system.object_detected",
                "diversion.activated",
                "diversion.deactivated",
                "diversion.partial",
                "diversion.diverted",
                "diversion.impacted",
                "travel.arrived",
                "travel.cancelled",
                "travel.departed",
                "replicant.transferred",
                "device.attached",
                "device.compacted",
                "device.deployed",
                "device.detached",
                "device.stowed",
                "device.unfurled",
                "ami.adopted",
                "ami.launched",
                "ami.released",
                "ami.withdrawn",
                "ami.transport.digest",
                "device.decommissioned",
                "directive.set",
                "directive.cleared",
                "directive.paused",
                "directive.resumed",
                "directive.completed",
                "mining.started",
                "mining.stopped",
                "mining.retargeted",
            ]
        );
    }

    #[test]
    fn stranded_device_recovery_goal_is_regional_disabled_and_wakes_on_device_events() {
        assert!(!default_goal_enabled(
            DirectorGoalKind::StrandedDeviceRecovery
        ));
        assert!(goal_is_regional(DirectorGoalKind::StrandedDeviceRecovery));
        assert_eq!(
            goal_kind_key(DirectorGoalKind::StrandedDeviceRecovery),
            "stranded_device_recovery"
        );
        assert_eq!(
            parse_goal_kind("stranded_device_recovery"),
            Some(DirectorGoalKind::StrandedDeviceRecovery)
        );
        assert_eq!(
            initial_goal_objective(DirectorGoalKind::StrandedDeviceRecovery),
            "Recover stranded owned devices to regional System Hubs"
        );
        for event in [
            "device.attached",
            "device.compacted",
            "device.deployed",
            "device.detached",
            "device.stowed",
            "device.unfurled",
        ] {
            assert!(director_reconcile_event_names().contains(&event));
        }
    }

    #[test]
    fn unserviced_resources_goal_is_regional_disabled_and_wakes_on_transport_events() {
        assert!(!default_goal_enabled(DirectorGoalKind::UnservicedResources));
        assert!(goal_is_regional(DirectorGoalKind::UnservicedResources));
        assert_eq!(
            goal_kind_key(DirectorGoalKind::UnservicedResources),
            "unserviced_resources"
        );
        assert_eq!(
            parse_goal_kind("unserviced_resources"),
            Some(DirectorGoalKind::UnservicedResources)
        );
        assert_eq!(
            initial_goal_objective(DirectorGoalKind::UnservicedResources),
            "Establish AMI transport service for producing regional resources"
        );
        for event in [
            "ami.adopted",
            "ami.launched",
            "ami.released",
            "ami.withdrawn",
            "ami.transport.digest",
            "device.deployed",
            "device.decommissioned",
            "directive.set",
            "directive.cleared",
            "directive.paused",
            "directive.resumed",
            "directive.completed",
            "mining.started",
            "mining.stopped",
            "mining.retargeted",
        ] {
            assert!(director_reconcile_event_names().contains(&event));
        }
    }

    #[test]
    fn unserviced_resources_retains_work_and_requires_complete_authority() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let mut controls = GoalControls::default();
        controls
            .regional
            .entry(DirectorGoalKind::UnservicedResources)
            .or_default()
            .insert("alpha".to_owned(), true);
        let region = RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("ALPHA".to_owned()),
            hub_location: Some("ALPHA-HUB".to_owned()),
            known_systems: BTreeSet::from(["ALPHA".to_owned()]),
        };
        let registry = WorkflowRegistry::new();
        let service_snapshot = WorkflowServiceIntentSnapshot::default();
        let empty_workflows = Vec::new();
        let context = GoalReconcileContext {
            repository: &repository,
            workflows: &empty_workflows,
            controls: &controls,
            automatic: true,
            now: 10,
        };
        let mut launch_available = true;

        let blocked = reconcile_unserviced_resources(
            &context,
            &registry,
            &service_snapshot,
            &region,
            &[],
            &[],
            &[],
            &BTreeMap::new(),
            &[],
            false,
            &mut launch_available,
        )
        .expect("incomplete authority summary");
        assert_eq!(blocked.status, DirectorGoalStatus::Blocked);

        let route = crate::mining::AmiTransportRouteIntent {
            system: "ALPHA".to_owned(),
            collect: "ALPHA-BELT-1".to_owned(),
            deliver: "ALPHA-HUB".to_owned(),
        };
        let workflow = repository
            .create(new_mining_campaign_workflow(MiningCampaignIntent {
                systems: vec![route.system.clone()],
                region: region.region.clone(),
                hub: route.deliver.clone(),
                transport_routes: vec![route],
                max_concurrency: 1,
            }))
            .expect("route campaign");
        save_goal_runtime(
            &repository,
            &goal_instance_id(DirectorGoalKind::UnservicedResources, Some("alpha")),
            &GoalRuntime {
                active_workflows: vec![workflow.id],
                last_launch_at_ms: Some(0),
                launch_records: Vec::new(),
                prospect_exhausted_signature: None,
            },
        )
        .expect("save runtime");
        let workflows = vec![workflow];
        let context = GoalReconcileContext {
            repository: &repository,
            workflows: &workflows,
            controls: &controls,
            automatic: true,
            now: 20,
        };
        let active = reconcile_unserviced_resources(
            &context,
            &registry,
            &service_snapshot,
            &region,
            &[],
            &[],
            &[],
            &BTreeMap::new(),
            &workflows,
            false,
            &mut launch_available,
        )
        .expect("retained work summary");
        assert_eq!(active.status, DirectorGoalStatus::Active);
        assert_eq!(
            active.active_workflows,
            vec![ProtocolWorkflowId(workflows[0].id.to_string())]
        );
    }

    #[test]
    fn unserviced_route_campaign_reuses_exact_intent_after_repository_reopen() {
        let directory =
            std::env::temp_dir().join(format!("replicant-unserviced-{}", uuid::Uuid::new_v4()));
        let path = directory.join("workflow.sqlite");
        let route = crate::mining::AmiTransportRouteIntent {
            system: "ALPHA".to_owned(),
            collect: "ALPHA-BELT-1".to_owned(),
            deliver: "ALPHA-HUB".to_owned(),
        };
        let target = route.workflow_service_intent();
        let new_campaign = || {
            new_mining_campaign_workflow(MiningCampaignIntent {
                systems: vec![route.system.clone()],
                region: "alpha".to_owned(),
                hub: route.deliver.clone(),
                transport_routes: vec![route.clone()],
                max_concurrency: 1,
            })
        };

        let repository = WorkflowRepository::open(&path).expect("repository");
        let first = repository
            .create_or_reuse_active(new_campaign(), |_| Ok(false))
            .expect("create route campaign");
        assert!(first.created);
        drop(repository);

        let repository = WorkflowRepository::open(&path).expect("reopen repository");
        let mut registry = WorkflowRegistry::new();
        crate::automation::register(&mut registry).expect("register automation");
        let second = repository
            .create_or_reuse_active(new_campaign(), |instance| {
                match registry.service_intent_state_for_instance(
                    instance,
                    &target,
                    Some("alpha"),
                    Some("ALPHA"),
                ) {
                    WorkflowServiceIntentState::Present(_) => Ok(true),
                    WorkflowServiceIntentState::Absent => Ok(false),
                    WorkflowServiceIntentState::Unknown(_) => Err(RepositoryError::Compatibility(
                        "unknown AMI route intent".to_owned(),
                    )),
                }
            })
            .expect("reuse route campaign");
        assert!(!second.created);
        assert_eq!(second.instance.id, first.instance.id);
        assert_eq!(repository.list().expect("workflows").len(), 1);

        drop(repository);
        std::fs::remove_dir_all(directory).expect("cleanup repository");
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
    #[test]
    fn recovery_adoption_requires_exact_identity_and_strict_metadata() {
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let metadata = PlacementRecoveryMetadata {
            failed_provenance: BTreeMap::from([("DEVICE-1".to_owned(), vec![provenance.clone()])]),
            release_device_tags: BTreeMap::from([(
                "DEVICE-1".to_owned(),
                vec!["mine-m:DEVICE-1".to_owned()],
            )]),
            placement_resolutions: vec![WorkflowPlacementResolution {
                device_code: "DEVICE-1".to_owned(),
                provenance,
            }],
        };
        let intent = LogisticsManifestIntent {
            origin: "BELT-1".to_owned(),
            destination: "ALPHA-HUB".to_owned(),
            device_codes: vec!["DEVICE-1".to_owned()],
            region: Some("Alpha".to_owned()),
            placement_recovery: Some(metadata.clone()),
            return_transports: true,
            allow_transport_staging: true,
            ..LogisticsManifestIntent::default()
        };
        assert!(recovery_manifest_well_formed(&intent));
        assert!(intent_matches_recovery(
            &intent,
            "alpha",
            "DEVICE-1",
            "BELT-1",
            "ALPHA-HUB",
            &metadata,
        ));
        assert!(!intent_matches_recovery(
            &intent,
            "alpha",
            "DEVICE-1",
            "BELT-1",
            "OTHER-HUB",
            &metadata,
        ));
        let mut malformed = intent.clone();
        malformed
            .placement_recovery
            .as_mut()
            .expect("metadata")
            .placement_resolutions[0]
            .device_code = "DEVICE-2".to_owned();
        assert!(!recovery_manifest_well_formed(&malformed));
    }
    fn recovery_controls(enabled: bool) -> GoalControls {
        let mut controls = GoalControls::default();
        controls
            .regional
            .entry(DirectorGoalKind::StrandedDeviceRecovery)
            .or_default()
            .insert("alpha".to_owned(), enabled);
        controls
    }

    fn recovery_region(hub: Option<&str>) -> RegionView {
        RegionView {
            region: "alpha".to_owned(),
            status: DirectorRegionStatus::Established,
            hub_system: Some("ALPHA".to_owned()),
            hub_location: hub.map(str::to_owned),
            known_systems: BTreeSet::from(["ALPHA".to_owned()]),
        }
    }

    fn recovery_device(code: &str, location: &str) -> Device {
        let mut device = test_hub_device();
        device.key = replicant_client::DeviceKey::live(code.into());
        device.device_type = Some(DeviceType::from("mining_drone"));
        device.status = Some(replicant_client::DeviceStatus::Idle);
        device.location = Some(replicant_client::LocationKey::live(location.into()));
        device.available_commands = vec![replicant_client::DeviceCommand::from("attach")];
        device.tags = vec![format!("mine-m:{code}")];
        device
    }

    fn recovery_snapshot(
        provenance: &WorkflowPlacementProvenance,
        code: &str,
        include_tag: bool,
        terminal_residual: bool,
    ) -> WorkflowPlacementIntentSnapshot {
        let kind = replicant_workflow::WorkflowKind::new("test").expect("workflow kind");
        let mut failed = vec![replicant_workflow::WorkflowPlacementIntentEvidence {
            workflow_id: provenance.workflow_id,
            workflow_kind: kind.clone(),
            workflow_status: replicant_workflow::WorkflowStatus::Failed,
            intent: replicant_workflow::WorkflowPlacementIntent {
                subject: WorkflowPlacementIntentSubject::Device(code.to_owned()),
                relation: replicant_workflow::WorkflowPlacementIntentRelation::Staged,
                work_item_id: provenance.work_item_id,
                expected_location: None,
            },
        }];
        if include_tag {
            failed.push(replicant_workflow::WorkflowPlacementIntentEvidence {
                workflow_id: provenance.workflow_id,
                workflow_kind: kind.clone(),
                workflow_status: replicant_workflow::WorkflowStatus::Failed,
                intent: replicant_workflow::WorkflowPlacementIntent {
                    subject: WorkflowPlacementIntentSubject::DeviceTag(format!("mine-m:{code}")),
                    relation: replicant_workflow::WorkflowPlacementIntentRelation::Staged,
                    work_item_id: provenance.work_item_id,
                    expected_location: None,
                },
            });
        }
        let terminal = terminal_residual.then(|| {
            vec![replicant_workflow::WorkflowPlacementIntentEvidence {
                workflow_id: provenance.workflow_id,
                workflow_kind: kind,
                workflow_status: replicant_workflow::WorkflowStatus::Succeeded,
                intent: replicant_workflow::WorkflowPlacementIntent {
                    subject: WorkflowPlacementIntentSubject::Device(code.to_owned()),
                    relation: replicant_workflow::WorkflowPlacementIntentRelation::Staged,
                    work_item_id: provenance.work_item_id,
                    expected_location: None,
                },
            }]
        });
        WorkflowPlacementIntentSnapshot {
            failed_transient: failed,
            terminal_residuals: terminal.unwrap_or_default(),
            ..WorkflowPlacementIntentSnapshot::default()
        }
    }

    fn recovery_metadata(
        provenance: &WorkflowPlacementProvenance,
        code: &str,
    ) -> PlacementRecoveryMetadata {
        PlacementRecoveryMetadata {
            failed_provenance: BTreeMap::from([(code.to_owned(), vec![provenance.clone()])]),
            release_device_tags: BTreeMap::from([(
                code.to_owned(),
                vec!["mine-m:DEVICE-1".to_owned()],
            )]),
            placement_resolutions: vec![WorkflowPlacementResolution {
                device_code: code.to_owned(),
                provenance: provenance.clone(),
            }],
        }
    }

    fn recovery_intent(
        metadata: &PlacementRecoveryMetadata,
        code: &str,
        origin: &str,
        destination: &str,
    ) -> LogisticsManifestIntent {
        LogisticsManifestIntent {
            origin: origin.to_owned(),
            destination: destination.to_owned(),
            device_codes: vec![code.to_owned()],
            region: Some("alpha".to_owned()),
            placement_recovery: Some(metadata.clone()),
            return_transports: true,
            allow_transport_staging: true,
            ..LogisticsManifestIntent::default()
        }
    }

    struct RecoveryRunContext<'a> {
        repository: &'a WorkflowRepository,
        controls: &'a GoalControls,
        automatic: bool,
        now: i64,
    }

    fn run_recovery(
        run: RecoveryRunContext<'_>,
        devices: &[Device],
        region: &RegionView,
        snapshot: Option<&WorkflowPlacementIntentSnapshot>,
        complete_owned_census: bool,
        placement_snapshot_error: bool,
    ) -> DirectorGoalSummary {
        let RecoveryRunContext {
            repository,
            controls,
            automatic,
            now,
        } = run;
        let workflows = repository.list().expect("list workflows");
        let context = GoalReconcileContext {
            repository,
            workflows: &workflows,
            controls,
            automatic,
            now,
        };
        let mut location_systems = devices
            .iter()
            .filter_map(|device| {
                device
                    .location
                    .as_ref()
                    .map(|location| (location.id.as_str().to_owned(), "ALPHA".to_owned()))
            })
            .collect::<BTreeMap<_, _>>();
        if let (Some(hub_location), Some(hub_system)) =
            (region.hub_location.as_ref(), region.hub_system.as_ref())
        {
            location_systems.insert(hub_location.clone(), hub_system.clone());
        }
        let system_regions = BTreeMap::from([("ALPHA".to_owned(), "alpha".to_owned())]);
        let regions = BTreeMap::from([("alpha".to_owned(), region.clone())]);
        reconcile_stranded_device_recovery(
            &context,
            region,
            devices,
            &[],
            &location_systems,
            &system_regions,
            &regions,
            snapshot,
            complete_owned_census,
            placement_snapshot_error,
        )
        .expect("reconcile stranded-device recovery")
    }

    #[test]
    fn recovery_director_status_matrix_reports_exact_authority_and_custody() {
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let device = recovery_device("DEVICE-1", "ALPHA-BELT-1");
        let snapshot = recovery_snapshot(&provenance, "DEVICE-1", true, false);
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let region = recovery_region(Some("ALPHA-HUB"));

        let disabled = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(false),
                automatic: true,
                now: 1,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&snapshot),
            false,
            false,
        );
        assert_eq!(disabled.status, DirectorGoalStatus::Waiting);
        assert_eq!(
            disabled.next_action.as_deref(),
            Some("Enable this standing goal to recover stranded owned devices")
        );
        assert!(disabled.active_workflows.is_empty());
        assert_eq!(repository.list().expect("rows").len(), 0);

        let blocked = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: 2,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&snapshot),
            false,
            false,
        );
        assert_eq!(blocked.status, DirectorGoalStatus::Blocked);
        assert_eq!(
            blocked.blocker.as_deref(),
            Some(
                "Complete managed device and workflow authority before recovering stranded devices"
            )
        );
        assert_eq!(blocked.next_action, blocked.blocker.clone());

        let no_home = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: 3,
            },
            std::slice::from_ref(&device),
            &recovery_region(None),
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(no_home.status, DirectorGoalStatus::Blocked);
        assert_eq!(
            no_home.blocker.as_deref(),
            Some("No exact regional System Hub location is available for stranded device recovery")
        );

        let ambiguous_snapshot = recovery_snapshot(&provenance, "DEVICE-1", true, true);
        let ambiguous = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: 4,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&ambiguous_snapshot),
            true,
            false,
        );
        assert_eq!(ambiguous.status, DirectorGoalStatus::Blocked);
        assert_eq!(
            ambiguous.blocker.as_deref(),
            Some(
                "One or more owned devices have unresolved workflow custody and cannot be recovered safely"
            )
        );
        assert_eq!(ambiguous.progress_total, 1);

        let satisfied = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: 5,
            },
            &[],
            &region,
            Some(&WorkflowPlacementIntentSnapshot::default()),
            true,
            false,
        );
        assert_eq!(satisfied.status, DirectorGoalStatus::Satisfied);
        assert_eq!(
            satisfied.next_action.as_deref(),
            Some("Wait for newly stranded owned devices")
        );
        assert_eq!(satisfied.progress_total, 0);
    }

    #[test]
    fn recovery_rejects_hub_location_without_exact_regional_authority() {
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let device = recovery_device("DEVICE-1", "ALPHA-BELT-1");
        let region = recovery_region(Some("ALPHA-HUB"));
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let workflows = repository.list().expect("workflows");
        let controls = recovery_controls(true);
        let context = GoalReconcileContext {
            repository: &repository,
            workflows: &workflows,
            controls: &controls,
            automatic: true,
            now: 6,
        };
        let snapshot = recovery_snapshot(&provenance, "DEVICE-1", true, false);
        let location_systems = BTreeMap::from([
            ("ALPHA-BELT-1".to_owned(), "ALPHA".to_owned()),
            ("ALPHA-HUB".to_owned(), "BETA".to_owned()),
        ]);
        let system_regions = BTreeMap::from([
            ("ALPHA".to_owned(), "alpha".to_owned()),
            ("BETA".to_owned(), "beta".to_owned()),
        ]);
        let regions = BTreeMap::from([("alpha".to_owned(), region.clone())]);
        let summary = reconcile_stranded_device_recovery(
            &context,
            &region,
            std::slice::from_ref(&device),
            &[],
            &location_systems,
            &system_regions,
            &regions,
            Some(&snapshot),
            true,
            false,
        )
        .expect("reconcile");
        assert_eq!(summary.status, DirectorGoalStatus::Blocked);
        assert_eq!(
            summary.blocker.as_deref(),
            Some(
                "Complete managed device and workflow authority before recovering stranded devices"
            )
        );
        assert!(repository.list().expect("rows").is_empty());
    }

    #[test]
    fn recovery_advisory_and_automatic_creation_report_exact_manifest_identity() {
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let device = recovery_device("DEVICE-1", "ALPHA-BELT-1");
        let snapshot = recovery_snapshot(&provenance, "DEVICE-1", true, false);
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let controls = recovery_controls(true);
        let region = recovery_region(Some("ALPHA-HUB"));
        let advisory = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &controls,
                automatic: false,
                now: 10,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(advisory.status, DirectorGoalStatus::Active);
        assert_eq!(
            advisory.next_action.as_deref(),
            Some("Recover stranded device DEVICE-1 from ALPHA-BELT-1 to ALPHA-HUB")
        );
        assert_eq!(advisory.progress_total, 1);
        assert!(advisory.active_workflows.is_empty());
        assert!(repository.list().expect("advisory rows").is_empty());

        let created = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &controls,
                automatic: true,
                now: 11,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(created.status, DirectorGoalStatus::Active);
        assert_eq!(created.active_workflows.len(), 1);
        let rows = repository.list().expect("created rows");
        assert_eq!(rows.len(), 1);
        let intent = rows[0]
            .config::<LogisticsManifestIntent>()
            .expect("manifest config");
        assert_eq!(intent.origin, "ALPHA-BELT-1");
        assert_eq!(intent.destination, "ALPHA-HUB");
        assert_eq!(intent.device_codes, vec!["DEVICE-1".to_owned()]);
        assert_eq!(
            intent.placement_recovery,
            Some(recovery_metadata(&provenance, "DEVICE-1"))
        );
        assert_eq!(
            intent.purpose,
            "director:stranded_device_recovery:DEVICE-1:ALPHA-HUB"
        );

        let repeated = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &controls,
                automatic: true,
                now: 12,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(repeated.status, DirectorGoalStatus::Active);
        assert_eq!(repeated.active_workflows, created.active_workflows);
        assert_eq!(repository.list().expect("repeated rows").len(), 1);
    }

    #[test]
    fn recovery_rejects_multi_device_manifests_as_not_closed_shape() {
        let provenance_one = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let provenance_two = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let provenance_three = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let metadata = PlacementRecoveryMetadata {
            failed_provenance: BTreeMap::from([
                ("DEVICE-1".to_owned(), vec![provenance_one.clone()]),
                ("DEVICE-2".to_owned(), vec![provenance_two.clone()]),
            ]),
            release_device_tags: BTreeMap::from([
                ("DEVICE-1".to_owned(), vec!["mine-m:DEVICE-1".to_owned()]),
                ("DEVICE-2".to_owned(), vec!["mine-m:DEVICE-2".to_owned()]),
            ]),
            placement_resolutions: vec![
                WorkflowPlacementResolution {
                    device_code: "DEVICE-1".to_owned(),
                    provenance: provenance_one.clone(),
                },
                WorkflowPlacementResolution {
                    device_code: "DEVICE-2".to_owned(),
                    provenance: provenance_two.clone(),
                },
            ],
        };
        let intent = LogisticsManifestIntent {
            origin: "ALPHA-BELT-1".to_owned(),
            destination: "ALPHA-HUB".to_owned(),
            device_codes: vec!["DEVICE-1".to_owned(), "DEVICE-2".to_owned()],
            region: Some("alpha".to_owned()),
            placement_recovery: Some(metadata),
            return_transports: true,
            allow_transport_staging: true,
            ..LogisticsManifestIntent::default()
        };
        assert!(!recovery_manifest_well_formed(&intent));

        let repository = WorkflowRepository::open_in_memory().expect("repository");
        repository
            .create(new_logistics_manifest_workflow(intent.clone()))
            .expect("active multi-device recovery manifest");
        let devices = vec![
            recovery_device("DEVICE-1", "ALPHA-BELT-1"),
            recovery_device("DEVICE-2", "ALPHA-BELT-2"),
            recovery_device("DEVICE-3", "ALPHA-BELT-3"),
        ];
        let mut snapshot = WorkflowPlacementIntentSnapshot::default();
        for (provenance, code) in [
            (&provenance_one, "DEVICE-1"),
            (&provenance_two, "DEVICE-2"),
            (&provenance_three, "DEVICE-3"),
        ] {
            snapshot
                .failed_transient
                .extend(recovery_snapshot(provenance, code, true, false).failed_transient);
        }
        let summary = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: 13,
            },
            &devices,
            &recovery_region(Some("ALPHA-HUB")),
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(summary.status, DirectorGoalStatus::Blocked);
        assert!(summary.active_workflows.is_empty());
        assert_eq!(repository.list().expect("multi-device rows").len(), 1);

        let mut malformed = intent;
        malformed
            .placement_recovery
            .as_mut()
            .expect("multi-device metadata")
            .placement_resolutions[0]
            .device_code = "DEVICE-3".to_owned();
        assert!(!recovery_manifest_well_formed(&malformed));
        let malformed_repository = WorkflowRepository::open_in_memory().expect("repository");
        malformed_repository
            .create(new_logistics_manifest_workflow(malformed))
            .expect("malformed multi-device recovery manifest");
        let blocked = run_recovery(
            RecoveryRunContext {
                repository: &malformed_repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: 14,
            },
            &devices,
            &recovery_region(Some("ALPHA-HUB")),
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(blocked.status, DirectorGoalStatus::Blocked);
        assert_eq!(
            blocked.blocker.as_deref(),
            Some(
                "Complete managed device and workflow authority before recovering stranded devices"
            )
        );
        assert!(blocked.active_workflows.is_empty());
        assert_eq!(
            malformed_repository
                .list()
                .expect("malformed multi-device rows")
                .len(),
            1
        );
    }

    #[test]
    fn recovery_adopts_only_valid_exact_manual_identity_and_blocks_malformed_rows() {
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let metadata = recovery_metadata(&provenance, "DEVICE-1");
        let device = recovery_device("DEVICE-1", "ALPHA-BELT-1");
        let snapshot = recovery_snapshot(&provenance, "DEVICE-1", true, false);
        let region = recovery_region(Some("ALPHA-HUB"));

        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let manual = repository
            .create(new_logistics_manifest_workflow(recovery_intent(
                &metadata,
                "DEVICE-1",
                "ALPHA-BELT-1",
                "ALPHA-HUB",
            )))
            .expect("manual recovery manifest");
        let adopted = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: false,
                now: 20,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(adopted.status, DirectorGoalStatus::Active);
        assert_eq!(
            adopted.active_workflows,
            vec![ProtocolWorkflowId(manual.id.to_string())]
        );
        assert_eq!(repository.list().expect("manual rows").len(), 1);

        for (destination, malformed) in [("OTHER-HUB", false), ("ALPHA-HUB", true)] {
            let isolated = WorkflowRepository::open_in_memory().expect("isolated repository");
            let mut intent = recovery_intent(&metadata, "DEVICE-1", "ALPHA-BELT-1", destination);
            if malformed {
                intent
                    .placement_recovery
                    .as_mut()
                    .expect("metadata")
                    .placement_resolutions[0]
                    .device_code = "DEVICE-2".to_owned();
            }
            isolated
                .create(new_logistics_manifest_workflow(intent))
                .expect("malformed manual manifest row");
            let summary = run_recovery(
                RecoveryRunContext {
                    repository: &isolated,
                    controls: &recovery_controls(true),
                    automatic: true,
                    now: 21,
                },
                std::slice::from_ref(&device),
                &region,
                Some(&snapshot),
                true,
                false,
            );
            assert_eq!(summary.status, DirectorGoalStatus::Blocked);
            assert_eq!(
                summary.blocker.as_deref(),
                Some(
                    "Complete managed device and workflow authority before recovering stranded devices"
                )
            );
            assert!(summary.active_workflows.is_empty());
            assert_eq!(isolated.list().expect("blocked rows").len(), 1);
        }
    }

    #[test]
    fn recovery_retries_retryable_exact_failure_after_cooldown() {
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let metadata = recovery_metadata(&provenance, "DEVICE-1");
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let created = repository
            .create(new_logistics_manifest_workflow(recovery_intent(
                &metadata,
                "DEVICE-1",
                "ALPHA-BELT-1",
                "ALPHA-HUB",
            )))
            .expect("recovery manifest");
        let failed = repository
            .update(
                created.id,
                created.revision,
                replicant_workflow::WorkflowState {
                    status: replicant_workflow::WorkflowStatus::Failed,
                    current_step: Some("failed".to_owned()),
                    checkpoint: Value::Null,
                    last_error: Some("transient recovery failure".to_owned()),
                    result: None::<Value>,
                },
            )
            .expect("persist failed manifest");
        assert_eq!(
            failed.failure_disposition,
            Some(replicant_workflow::WorkflowFailureDisposition::Retryable)
        );
        let device = recovery_device("DEVICE-1", "ALPHA-BELT-1");
        let snapshot = recovery_snapshot(&provenance, "DEVICE-1", true, false);
        let region = recovery_region(Some("ALPHA-HUB"));
        let before = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: failed.updated_at + DEFAULT_RETRY_COOLDOWN_MS - 1,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(before.status, DirectorGoalStatus::Waiting);
        assert_eq!(
            before.next_action.as_deref(),
            Some("Wait briefly before retrying stranded device recovery")
        );
        assert_eq!(repository.list().expect("cooldown rows").len(), 1);
        let after = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: failed.updated_at + DEFAULT_RETRY_COOLDOWN_MS + 1,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(after.status, DirectorGoalStatus::Active);
        assert_eq!(repository.list().expect("retry rows").len(), 2);
    }

    #[test]
    fn recovery_legacy_failed_exact_identity_retries_after_cooldown() {
        let path = std::env::temp_dir().join(format!(
            "replicant-stranded-recovery-legacy-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let metadata = recovery_metadata(&provenance, "DEVICE-1");
        let repository = WorkflowRepository::open(&path).expect("open repository");
        repository
            .create(new_logistics_manifest_workflow(recovery_intent(
                &metadata,
                "DEVICE-1",
                "ALPHA-BELT-1",
                "ALPHA-HUB",
            )))
            .expect("legacy recovery manifest");
        let created = repository.list().expect("created row").pop().expect("row");
        drop(repository);
        let connection = rusqlite::Connection::open(&path).expect("open fixture database");
        connection
            .execute(
                "UPDATE workflow_instances
                 SET status = 'failed',
                     failure_disposition = NULL,
                     last_error = 'legacy recovery failure'
                 WHERE id = ?1",
                rusqlite::params![created.id.to_string()],
            )
            .expect("persist legacy failure fixture");
        drop(connection);

        let repository = WorkflowRepository::open(&path).expect("reopen repository");
        let failed = repository
            .list()
            .expect("legacy failed row")
            .pop()
            .expect("row");
        let device = recovery_device("DEVICE-1", "ALPHA-BELT-1");
        let snapshot = recovery_snapshot(&provenance, "DEVICE-1", true, false);
        let region = recovery_region(Some("ALPHA-HUB"));
        let before = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: failed.updated_at + DEFAULT_RETRY_COOLDOWN_MS - 1,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(before.status, DirectorGoalStatus::Waiting);
        let after = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: failed.updated_at + DEFAULT_RETRY_COOLDOWN_MS + 1,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(after.status, DirectorGoalStatus::Active);
        assert_eq!(
            before.next_action.as_deref(),
            Some("Wait briefly before retrying stranded device recovery")
        );
        assert_eq!(
            after.next_action.as_deref(),
            Some("Recover stranded device DEVICE-1 from ALPHA-BELT-1 to ALPHA-HUB")
        );
        assert_eq!(repository.list().expect("legacy retry rows").len(), 2);
        drop(repository);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_reopen_and_separate_handles_reuse_one_row_then_new_identity_launches() {
        let path = std::env::temp_dir().join(format!(
            "replicant-stranded-recovery-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let device = recovery_device("DEVICE-1", "ALPHA-BELT-1");
        let snapshot = recovery_snapshot(&provenance, "DEVICE-1", true, false);
        let region = recovery_region(Some("ALPHA-HUB"));
        let first = WorkflowRepository::open(&path).expect("open first handle");
        let second = WorkflowRepository::open(&path).expect("open concurrent handle");
        let first_controls = recovery_controls(true);
        let second_controls = recovery_controls(true);
        let (first_summary, concurrent_summary) = std::thread::scope(|scope| {
            let first_job = scope.spawn(|| {
                run_recovery(
                    RecoveryRunContext {
                        repository: &first,
                        controls: &first_controls,
                        automatic: true,
                        now: 30,
                    },
                    std::slice::from_ref(&device),
                    &region,
                    Some(&snapshot),
                    true,
                    false,
                )
            });
            let second_job = scope.spawn(|| {
                run_recovery(
                    RecoveryRunContext {
                        repository: &second,
                        controls: &second_controls,
                        automatic: true,
                        now: 31,
                    },
                    std::slice::from_ref(&device),
                    &region,
                    Some(&snapshot),
                    true,
                    false,
                )
            });
            (
                first_job.join().expect("first concurrent reconcile"),
                second_job.join().expect("second concurrent reconcile"),
            )
        });
        assert_eq!(first_summary.active_workflows.len(), 1);
        assert_eq!(
            concurrent_summary.active_workflows,
            first_summary.active_workflows
        );
        assert_eq!(second.list().expect("concurrent rows").len(), 1);

        drop(first);
        drop(second);
        let second = WorkflowRepository::open(&path).expect("reopen second handle");
        let reopened = run_recovery(
            RecoveryRunContext {
                repository: &second,
                controls: &recovery_controls(true),
                automatic: true,
                now: 32,
            },
            std::slice::from_ref(&device),
            &region,
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(reopened.active_workflows, first_summary.active_workflows);
        assert_eq!(second.list().expect("reopened rows").len(), 1);

        let old = second
            .list()
            .expect("old row")
            .into_iter()
            .next()
            .expect("old manifest");
        let failed = second
            .update(
                old.id,
                old.revision,
                replicant_workflow::WorkflowState {
                    status: replicant_workflow::WorkflowStatus::Failed,
                    current_step: Some("failed".to_owned()),
                    checkpoint: Value::Null,
                    last_error: Some("changed identity".to_owned()),
                    result: None::<Value>,
                },
            )
            .expect("fail old identity");
        let changed_provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let changed_device = recovery_device("DEVICE-2", "ALPHA-BELT-2");
        let changed_snapshot = recovery_snapshot(&changed_provenance, "DEVICE-2", true, false);
        let changed = run_recovery(
            RecoveryRunContext {
                repository: &second,
                controls: &recovery_controls(true),
                automatic: true,
                now: failed.updated_at + DEFAULT_RETRY_COOLDOWN_MS + 1,
            },
            std::slice::from_ref(&changed_device),
            &RegionView {
                hub_location: Some("NEW-HUB".to_owned()),
                ..region
            },
            Some(&changed_snapshot),
            true,
            false,
        );
        assert_eq!(changed.status, DirectorGoalStatus::Active);
        assert_eq!(second.list().expect("changed identity rows").len(), 2);
        drop(second);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn recovery_excludes_fresh_blueprints_in_the_same_pass() {
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let snapshot = recovery_snapshot(&provenance, "DEVICE-1", true, false);
        let device = recovery_device("DEVICE-1", "ALPHA-BELT-1");
        let mut blueprint = recovery_device("BLUEPRINT-1", "ALPHA-BELT-2");
        blueprint.device_type = Some(DeviceType::EmptyReplicantMatrix);
        blueprint.tags.clear();
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let summary = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: 40,
            },
            &[device.clone(), blueprint],
            &recovery_region(Some("ALPHA-HUB")),
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(summary.status, DirectorGoalStatus::Active);
        assert_eq!(summary.progress_total, 1);
        let rows = repository.list().expect("blueprint exclusion rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0]
                .config::<LogisticsManifestIntent>()
                .expect("manifest")
                .device_codes,
            vec!["DEVICE-1".to_owned()]
        );
    }
    #[test]
    fn recovery_permanent_exact_failure_blocks_without_new_manifest() {
        let path = std::env::temp_dir().join(format!(
            "replicant-stranded-recovery-permanent-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let metadata = recovery_metadata(&provenance, "DEVICE-1");
        let repository = WorkflowRepository::open(&path).expect("open repository");
        let created = repository
            .create(new_logistics_manifest_workflow(recovery_intent(
                &metadata,
                "DEVICE-1",
                "ALPHA-BELT-1",
                "ALPHA-HUB",
            )))
            .expect("recovery manifest");
        drop(repository);
        let connection = rusqlite::Connection::open(&path).expect("open fixture database");
        connection
            .execute(
                "UPDATE workflow_instances
                 SET status = 'failed',
                     failure_disposition = 'permanent',
                     last_error = 'exact recovery is permanently unavailable'
                 WHERE id = ?1",
                rusqlite::params![created.id.to_string()],
            )
            .expect("persist permanent failure fixture");
        drop(connection);

        let repository = WorkflowRepository::open(&path).expect("reopen repository");
        let device = recovery_device("DEVICE-1", "ALPHA-BELT-1");
        let snapshot = recovery_snapshot(&provenance, "DEVICE-1", true, false);
        let summary = run_recovery(
            RecoveryRunContext {
                repository: &repository,
                controls: &recovery_controls(true),
                automatic: true,
                now: created.updated_at + DEFAULT_RETRY_COOLDOWN_MS + 1,
            },
            std::slice::from_ref(&device),
            &recovery_region(Some("ALPHA-HUB")),
            Some(&snapshot),
            true,
            false,
        );
        assert_eq!(summary.status, DirectorGoalStatus::Blocked);
        assert_eq!(
            summary.blocker.as_deref(),
            Some("exact recovery is permanently unavailable")
        );
        assert_eq!(
            summary.next_action.as_deref(),
            Some("Recover stranded device DEVICE-1 from ALPHA-BELT-1 to ALPHA-HUB")
        );
        assert!(summary.active_workflows.is_empty());
        assert_eq!(repository.list().expect("permanent rows").len(), 1);
        drop(repository);
        let _ = std::fs::remove_file(path);
    }
}
