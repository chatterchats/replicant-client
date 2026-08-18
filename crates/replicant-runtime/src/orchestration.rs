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

use replicant_client::{
    Client, Device, DeviceType, Location, Replicant, Star, domain::GalacticPosition,
};
use replicant_protocol::{
    DirectorGoalKind, DirectorGoalStatus, DirectorGoalSummary, DirectorMode, DirectorRegionStatus,
    DirectorRegionSummary, DirectorReplicantAssignment, DirectorSnapshot, DirectorWorkforceSummary,
    SnapshotMetadata, WorkflowId as ProtocolWorkflowId,
};
use replicant_workflow::{
    ResourceKey, WorkflowId, WorkflowInstance, WorkflowRepository, WorkflowStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    ApplicationError,
    automation::{
        EventCampaignIntent, MiningCampaignIntent, ObservatoryIntent, RegionEstablishIntent,
        ReplicantProvisionIntent, ScanTourIntent, new_event_campaign_workflow,
        new_mining_campaign_workflow, new_observatory_workflow, new_region_establish_workflow,
        new_replicant_provision_workflow, new_scan_tour_workflow,
    },
    director_requirements::{
        DirectorRequirement, DirectorRequirementGraph, load_requirement_summaries,
    },
    event::active_events,
};

const SETTINGS_NS: &str = "director.settings";
const SETTINGS_KEY: &str = "singleton";
const GOAL_CONTROL_NS: &str = "director.goal_control";
const GOAL_RUNTIME_NS: &str = "director.goal_runtime";
const REPLICANT_NS: &str = "director.replicant";
const WORKFORCE_NS: &str = "director.workforce";
const SNAPSHOT_NS: &str = "director.snapshot";
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
const REGION_GATEWAY_HUB_RANGE_LY: f64 = 15.0;
const EVENT_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(12);

const PRIORITY_REGION_ESTABLISHMENT: u32 = 900;
const PRIORITY_EVENT_COMPLETION: u32 = 700;
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

struct GoalReconcileContext<'a> {
    repository: &'a WorkflowRepository,
    workflows: &'a [WorkflowInstance],
    controls: &'a BTreeMap<DirectorGoalKind, bool>,
    automatic: bool,
    now: i64,
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

/// Evaluates all standing goals and, in automatic mode, creates the batch work
/// required to move them forward.
pub async fn reconcile_director(
    client: &Client,
    repository: Arc<WorkflowRepository>,
    revision: u64,
    allow_launch: bool,
) -> Result<DirectorSnapshot, ApplicationError> {
    let started = Instant::now();
    let settings = director_settings(&repository)?;
    let now = now_millis();
    tracing::info!(
        revision,
        mode = ?settings.mode,
        allow_launch,
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
    let device_handles = client.devices().find().owned().collect().await?;
    let mut devices = Vec::with_capacity(device_handles.len());
    for handle in device_handles {
        devices.push(handle.snapshot().await?);
    }
    tracing::debug!(
        phase = "devices",
        count = devices.len(),
        elapsed_ms = started.elapsed().as_millis(),
        "Director managed world phase complete"
    );
    tracing::debug!(phase = "replicants", "Director loading managed world state");
    let replicant_handles = client.replicants().find().owned().collect().await?;
    let mut replicants = Vec::with_capacity(replicant_handles.len());
    for handle in replicant_handles {
        replicants.push(handle.snapshot().await?);
    }
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

    for region in regions.values_mut() {
        if let Some(home) = preferred_home_location(
            &region.region,
            region.hub_system.as_deref(),
            &devices,
            &location_systems,
            &system_regions,
        ) {
            region.hub_location = Some(home);
        }
    }

    let goal_controls = load_goal_controls(&repository)?;
    let mut requirements = DirectorRequirementGraph::load(&repository, now)?;
    let observatory_blueprint_known = if goal_enabled(
        &goal_controls,
        DirectorGoalKind::ExpandStarCatalogue,
    ) && !devices.iter().any(|device| {
        device.device_type.as_ref() == Some(&DeviceType::GalacticObservatory)
    }) {
        match client.blueprints().unlocked_device_types().await {
            Ok(blueprints) => Some(blueprints.contains(&DeviceType::GalacticObservatory)),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    phase = "requirements",
                    "Director could not inspect managed blueprint state for catalogue blocker"
                );
                None
            }
        }
    } else {
        None
    };
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

    let mut event_discovery_error = None;
    let event_designations_by_region = if goal_enabled(
        &goal_controls,
        DirectorGoalKind::EventCompletion,
    ) && !established_regions.is_empty()
    {
        let event_started = Instant::now();
        tracing::info!(
            regions = established_regions.len(),
            phase = "events",
            "Director loading one account-wide active-event snapshot"
        );
        match tokio::time::timeout(EVENT_DISCOVERY_TIMEOUT, active_events(client)).await {
            Ok(Ok(active_events)) => {
                let grouped = group_active_events_by_region(
                    active_events,
                    &location_systems,
                    &system_regions,
                    &regions,
                );
                tracing::info!(
                    events = grouped.values().map(Vec::len).sum::<usize>(),
                    regions = grouped.len(),
                    elapsed_ms = event_started.elapsed().as_millis(),
                    phase = "events",
                    "Director active-event snapshot complete"
                );
                grouped
            }
            Ok(Err(error)) => {
                let message = format!("active-event discovery failed: {error}");
                tracing::warn!(
                    error = %error,
                    elapsed_ms = event_started.elapsed().as_millis(),
                    phase = "events",
                    "Director active-event snapshot failed; continuing without event planning"
                );
                event_discovery_error = Some(message);
                BTreeMap::new()
            }
            Err(_) => {
                let message = format!(
                    "active-event discovery exceeded {} seconds",
                    EVENT_DISCOVERY_TIMEOUT.as_secs()
                );
                tracing::warn!(
                    timeout_ms = EVENT_DISCOVERY_TIMEOUT.as_millis(),
                    elapsed_ms = event_started.elapsed().as_millis(),
                    phase = "events",
                    "Director active-event snapshot timed out; continuing without event planning"
                );
                event_discovery_error = Some(message);
                BTreeMap::new()
            }
        }
    } else {
        BTreeMap::new()
    };

    for region in &established_regions {
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
        goals.push(waiting_goal(
            DirectorGoalKind::ExpandFtlNetwork,
            Some(&region.region),
            &goal_controls,
            "Maintain and extend regional FTL reach",
            "FTL coverage scoring is not yet enabled in the Director planner",
            "Existing exploration.frontier workflows remain available for explicit expansion",
        ));
        goals.push(waiting_goal(
            DirectorGoalKind::EstablishBeacons,
            Some(&region.region),
            &goal_controls,
            "Maintain beacon coverage at useful known systems",
            "Beacon placement policy is not yet enabled in the Director planner",
            "Existing event/bootstrap automation may still deploy required beacons",
        ));
    }

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
        if target_workers.len() < 2 {
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
        blocker = Some(format!("{} event discovery is unavailable: {error}", region.region));
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

    let regional_codes = workers
        .iter()
        .filter(|worker| worker.region.as_deref() == Some(region.region.as_str()))
        .map(|worker| worker.replicant.key.id.as_str().to_owned())
        .collect::<Vec<_>>();
    let explored = regional_codes
        .iter()
        .flat_map(|code| client.galaxy().replicant_star_knowledge(code))
        .filter(|knowledge| knowledge.explored == Some(true))
        .map(|knowledge| knowledge.star.id.as_str().to_owned())
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
    let targets = belt_systems
        .difference(&staffed_systems)
        .take(MINING_BATCH_SIZE)
        .cloned()
        .collect::<Vec<_>>();
    let covered = belt_systems
        .len()
        .saturating_sub(belt_systems.difference(&staffed_systems).count());
    let active = nonterminal_ids(&runtime, workflows);
    let recently_launched = launch_is_recent(&runtime, now, DEFAULT_RETRY_COOLDOWN_MS);
    let mut blocker = None;
    let mut next_action = None;
    let status = if !enabled {
        DirectorGoalStatus::Waiting
    } else if targets.is_empty() {
        next_action = Some("Wait for newly discovered belts or depleted mining spokes".to_owned());
        DirectorGoalStatus::Satisfied
    } else if !active.is_empty() {
        next_action = Some("Continue the active regional mining expansion batch".to_owned());
        DirectorGoalStatus::Active
    } else if recently_launched {
        next_action =
            Some("Wait briefly before replanning the next mining expansion batch".to_owned());
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
    events: Vec<replicant_client::raw::events::LocationEvent>,
    location_systems: &BTreeMap<String, String>,
    system_regions: &BTreeMap<String, String>,
    regions: &BTreeMap<String, RegionView>,
) -> BTreeMap<String, Vec<String>> {
    let mut grouped = BTreeMap::<String, Vec<String>>::new();
    for event in events {
        let Some(designation) = event.designation else {
            continue;
        };
        let Some(location) = event.location else {
            continue;
        };
        let system = location_systems
            .get(&location)
            .cloned()
            .unwrap_or_else(|| system_prefix(&location).to_owned());
        let region = system_regions
            .get(&system)
            .cloned()
            .or_else(|| operational_region_for_system(&system, regions));
        let Some(region) = region else {
            continue;
        };
        grouped.entry(region).or_default().push(designation);
    }
    for designations in grouped.values_mut() {
        designations.sort();
        designations.dedup();
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
    !matches!(
        kind,
        DirectorGoalKind::ExpandFtlNetwork | DirectorGoalKind::EstablishBeacons
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
        DirectorGoalKind::ExpandMiningOps => "Expand useful regional mining infrastructure",
        DirectorGoalKind::EventCompletion => "Complete worthwhile active regional events",
        DirectorGoalKind::ExpandFtlNetwork => "Maintain and extend regional FTL reach",
        DirectorGoalKind::EstablishBeacons => "Maintain beacon coverage at useful known systems",
    }
}

fn all_goal_kinds() -> [DirectorGoalKind; 7] {
    [
        DirectorGoalKind::EstablishRegions,
        DirectorGoalKind::ExpandStarCatalogue,
        DirectorGoalKind::EnhanceStarCatalogue,
        DirectorGoalKind::ExpandMiningOps,
        DirectorGoalKind::EventCompletion,
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
        DirectorGoalKind::ExpandMiningOps => "expand_mining_ops",
        DirectorGoalKind::EventCompletion => "event_completion",
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
    use super::*;

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
    fn autonomous_placement_goals_start_disabled_until_planners_exist() {
        assert!(!default_goal_enabled(DirectorGoalKind::ExpandFtlNetwork));
        assert!(!default_goal_enabled(DirectorGoalKind::EstablishBeacons));
        assert!(default_goal_enabled(DirectorGoalKind::EventCompletion));
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
