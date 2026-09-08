use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use replicant_client::{
    Client, Star,
    domain::{Device, DeviceStatus, DeviceType, Location},
};
#[cfg(test)]
use replicant_mining_planner::role_tag;
use replicant_mining_planner::{
    BlueprintSpec, CARGO_FREIGHTER, FactoryWorkload, MAINTENANCE_DRONE, MINING_CONTROLLER,
    MINING_DRONE, PrintBatch, QuantityMap, SURGE_CARRIER, SURVEY_CONTROLLER, SURVEY_DRONE,
    SYSTEM_WARD, TRANSPORT_CONTROLLER, add_quantities, blueprint_resource_cost,
    mining_site_requirements, schedule_prints, shortages, site_tag,
};
use replicant_printing::managed::discover_factories;
use replicant_workflow::{
    AllocationSet, RequirementScope, ResourceKey, ResourceRequirement, WorkItemSpec, WorkflowId,
    WorkflowKind, WorkflowServiceIntent,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

use crate::worker_state::OPERATIONAL_REGIONAL_WORKER_CAPABILITY;

mod executor;
mod validation;

const PLAN_VERSION: u32 = 1;
pub(crate) const MINING_MAINTENANCE_ROTATION_THRESHOLD_PCT: f64 = 30.0;
pub(crate) const HUB_MAINTENANCE_READY_PCT: f64 = 95.0;
pub(crate) const HUB_MAINTENANCE_MIN_HEALTHY: usize = 2;
pub(crate) const HUB_MAINTENANCE_TARGET_HEALTHY: usize = 3;
pub(crate) const HUB_MAINTENANCE_ROLE: &str = "maintenance-pool";

pub(crate) const CARGO_FREIGHTER_CAPACITY_UNITS: i64 = 500;
pub(crate) const MIN_TRANSPORT_FREIGHTERS: usize = 1;
pub(crate) const MAX_TRANSPORT_FREIGHTERS: usize = 6;
pub(crate) const TRANSPORT_LOW_BACKLOG_LOADS: i64 = 2;
pub(crate) const TRANSPORT_SCALE_UP_LOADS: i64 = 10;
pub(crate) const TRANSPORT_CRITICAL_BACKLOG_LOADS: i64 = 30;
pub(crate) const TRANSPORT_CAPACITY_COOLDOWN_MS: i64 = 15 * 60 * 1_000;
pub(crate) const TRANSPORT_SCALE_DOWN_HOLD_MS: i64 = 60 * 60 * 1_000;
pub(crate) const TRANSPORT_CAPACITY_DOCUMENT_NS: &str = "director.mining.transport_capacity";

const fn default_transport_freighter_count() -> usize {
    MIN_TRANSPORT_FREIGHTERS
}

/// Error type returned by the reusable mining workflow.
pub type AnyError = Box<dyn StdError + Send + Sync + 'static>;
/// Result type returned by the reusable mining workflow.
pub type AnyResult<T> = Result<T, AnyError>;

fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> AnyError {
    io::Error::new(kind, message.into()).into()
}

/// Durable physical-asset custody for a Mining Ops workflow invocation.
#[derive(Clone)]
pub struct MiningWorkflowClaims {
    /// Workflow repository that arbitrates physical asset ownership.
    pub repository: std::sync::Arc<replicant_workflow::WorkflowRepository>,
    /// The workflow owning all resources used by this invocation.
    pub workflow_id: replicant_workflow::WorkflowId,
}

impl std::fmt::Debug for MiningWorkflowClaims {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MiningWorkflowClaims")
            .field("workflow_id", &self.workflow_id)
            .finish_non_exhaustive()
    }
}

impl MiningWorkflowClaims {
    fn acquire_devices(&self, codes: &[String]) -> AnyResult<()> {
        let resources = codes
            .iter()
            .cloned()
            .map(replicant_workflow::ResourceKey::Device)
            .collect::<Vec<_>>();
        self.repository
            .acquire_claims(self.workflow_id, &resources)?;
        Ok(())
    }
}

struct Config {
    systems: Vec<String>,
    replicant: Option<String>,
    hub: String,
    transport_routes: Vec<AmiTransportRouteIntent>,
    plan_path: PathBuf,
    replace_plan: bool,
    wait_timeout: Duration,
    max_concurrency: usize,
    claims: Option<MiningWorkflowClaims>,
}

impl Config {
    fn claim_devices(&self, codes: &[String]) -> AnyResult<()> {
        if let Some(claims) = &self.claims {
            claims.acquire_devices(codes)?;
        }
        Ok(())
    }

    fn requested_systems(&self) -> AnyResult<Vec<String>> {
        let systems = self
            .systems
            .iter()
            .map(|value| value.trim().to_ascii_uppercase())
            .filter(|value| !value.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if systems.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "planning requires at least one system",
            ));
        }
        Ok(systems)
    }
}

/// Durable top-level mining expansion phase.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionPhase {
    Planned,
    ManufacturingSites,
    DeployingSites,
    ManufacturingRoutes,
    ActivatingRoutes,
    ReturningCarriers,
    Completed,
    CompletedWithWarnings,
}

impl MissionPhase {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::CompletedWithWarnings)
    }

    fn advance_to(self, next: Self) -> Self {
        if self.is_terminal() || self.rank() > next.rank() {
            self
        } else {
            next
        }
    }

    const fn rank(self) -> u8 {
        match self {
            Self::Planned => 0,
            Self::ManufacturingSites => 1,
            Self::DeployingSites => 2,
            Self::ManufacturingRoutes => 3,
            Self::ActivatingRoutes => 4,
            Self::ReturningCarriers => 5,
            Self::Completed | Self::CompletedWithWarnings => 6,
        }
    }
}

/// Durable deployment phase for one mining site.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SitePhase {
    Planned,
    Ready,
    Outbound,
    Deploying,
    Adopting,
    Verifying,
    /// Legacy checkpoint read from version-one missions as deployment resumes.
    Configuring,
    Operational,
}

/// Exact AMI transport route requested by a mining campaign.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct AmiTransportRouteIntent {
    /// Origin system containing the collection belt.
    pub system: String,
    /// Exact discovered belt location to collect from.
    pub collect: String,
    /// Exact System Hub location to deliver to.
    pub deliver: String,
    /// Desired Cargo Freighter capacity. Legacy intents default to one.
    #[serde(default = "default_transport_freighter_count")]
    pub desired_freighters: usize,
}

impl AmiTransportRouteIntent {
    /// Projects this route into the generic durable service-intent contract.
    #[must_use]
    pub fn workflow_service_intent(&self) -> WorkflowServiceIntent {
        WorkflowServiceIntent {
            service: "ami_transport".to_owned(),
            dimensions: [
                ("collect".to_owned(), self.collect.clone()),
                ("deliver".to_owned(), self.deliver.clone()),
            ]
            .into_iter()
            .collect(),
        }
    }
}

/// Tri-state evidence used by resource and transport reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EvidenceState {
    /// Complete positive evidence exists.
    Present,
    /// Complete evidence proves the predicate false.
    Absent,
    /// Authority or required fields are incomplete.
    Unknown,
}

/// Strict health audit of an AMI transport route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransportServiceAudit {
    pub(crate) state: EvidenceState,
    pub(crate) collect: String,
    pub(crate) deliver: String,
    pub(crate) controller: Option<String>,
    pub(crate) controller_operational: EvidenceState,
    pub(crate) controller_directive: EvidenceState,
    pub(crate) controller_configuration: EvidenceState,
    pub(crate) adopted_freighters: Vec<String>,
    pub(crate) usable_freighters: Vec<String>,
    pub(crate) unknown_freighters: Vec<String>,
    pub(crate) unusable_freighters: Vec<String>,
    pub(crate) freighter: Option<String>,
}

impl TransportServiceAudit {
    pub(crate) fn usable_freighter_count(&self) -> usize {
        self.usable_freighters.len()
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MaintenanceHubPoolAudit {
    pub(crate) healthy_patrol: Vec<String>,
    pub(crate) repairing_patrol: Vec<String>,
    pub(crate) unknown_patrol: Vec<String>,
    pub(crate) configurable_ready: Vec<String>,
}

impl MaintenanceHubPoolAudit {
    pub(crate) fn desired_healthy_count(
        &self,
        available_healthy: usize,
        higher_priority_faults: bool,
    ) -> usize {
        if !self.unknown_patrol.is_empty() {
            return available_healthy;
        }
        if available_healthy < HUB_MAINTENANCE_MIN_HEALTHY {
            HUB_MAINTENANCE_MIN_HEALTHY
        } else if available_healthy == HUB_MAINTENANCE_MIN_HEALTHY
            && self.repairing_patrol.is_empty()
            && self.unknown_patrol.is_empty()
            && !higher_priority_faults
        {
            HUB_MAINTENANCE_TARGET_HEALTHY
        } else {
            available_healthy
        }
    }
}

pub(crate) fn maintenance_replacement_ready(device: &Device) -> bool {
    site_asset_usable(device)
        && device.status.is_some()
        && device
            .operational_capacity
            .is_some_and(|capacity| capacity.percent() >= HUB_MAINTENANCE_READY_PCT)
}

pub(crate) fn maintenance_rotation_due(device: &Device) -> bool {
    site_asset_usable(device)
        && has_directive(device, "patrol")
        && device
            .operational_capacity
            .is_some_and(|capacity| capacity.percent() < MINING_MAINTENANCE_ROTATION_THRESHOLD_PCT)
}

pub(crate) fn maintenance_remote_healthy(device: &Device) -> bool {
    site_asset_usable(device)
        && device.status.is_some()
        && has_directive(device, "patrol")
        && device
            .operational_capacity
            .is_some_and(|capacity| capacity.percent() >= MINING_MAINTENANCE_ROTATION_THRESHOLD_PCT)
}

pub(crate) fn maintenance_hub_pool_audit(
    devices: &[Device],
    hub_belt: &str,
) -> MaintenanceHubPoolAudit {
    let pool_role = replicant_mining_planner::role_tag(HUB_MAINTENANCE_ROLE);
    let mut audit = MaintenanceHubPoolAudit::default();
    for device in devices.iter().filter(|device| {
        device_location(device) == Some(hub_belt)
            && device_type(device) == Some(MAINTENANCE_DRONE)
            && site_asset_usable(device)
            && device.relationships.controller.is_none()
            && !device
                .tags
                .iter()
                .any(|tag| replicant_protocol::workflow_tag_reserved(tag) && tag != &pool_role)
    }) {
        let code = device.key.id.as_str().to_owned();
        if device.status.is_none() {
            audit.unknown_patrol.push(code);
            continue;
        }
        if has_directive(device, "patrol") {
            match device
                .operational_capacity
                .as_ref()
                .map(|capacity| capacity.percent())
            {
                Some(percent) if percent >= HUB_MAINTENANCE_READY_PCT => {
                    audit.healthy_patrol.push(code);
                }
                Some(_) => audit.repairing_patrol.push(code),
                None => audit.unknown_patrol.push(code),
            }
        } else if maintenance_patrol_capability(device) != EvidenceState::Absent
            && maintenance_replacement_ready(device)
        {
            audit.configurable_ready.push(code);
        }
    }
    audit.healthy_patrol.sort();
    audit.repairing_patrol.sort();
    audit.unknown_patrol.sort();
    audit.configurable_ready.sort();
    audit
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportBacklogClass {
    Low,
    HealthyBand,
    ScaleUp,
    Critical,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub(crate) struct TransportCapacityTrend {
    #[serde(default)]
    pub(crate) last_observed_backlog_units: Option<i64>,
    #[serde(default)]
    pub(crate) last_observation_at_ms: Option<i64>,
    #[serde(default)]
    pub(crate) last_capacity_change_at_ms: Option<i64>,
    #[serde(default)]
    pub(crate) low_backlog_since_ms: Option<i64>,
}

pub(crate) fn transport_backlog_loads(backlog_units: i64) -> f64 {
    backlog_units.max(0) as f64 / CARGO_FREIGHTER_CAPACITY_UNITS as f64
}

pub(crate) fn transport_backlog_class(backlog_units: i64) -> TransportBacklogClass {
    if backlog_units < TRANSPORT_LOW_BACKLOG_LOADS * CARGO_FREIGHTER_CAPACITY_UNITS {
        TransportBacklogClass::Low
    } else if backlog_units > TRANSPORT_CRITICAL_BACKLOG_LOADS * CARGO_FREIGHTER_CAPACITY_UNITS {
        TransportBacklogClass::Critical
    } else if backlog_units > TRANSPORT_SCALE_UP_LOADS * CARGO_FREIGHTER_CAPACITY_UNITS {
        TransportBacklogClass::ScaleUp
    } else {
        TransportBacklogClass::HealthyBand
    }
}

pub(crate) fn transport_scale_up_delta(backlog_units: i64) -> usize {
    match transport_backlog_class(backlog_units) {
        TransportBacklogClass::Critical => 2,
        TransportBacklogClass::ScaleUp => 1,
        TransportBacklogClass::Low | TransportBacklogClass::HealthyBand => 0,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportCapacityAction {
    Hold,
    Add(usize),
    RemoveOne,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TransportThroughputState {
    ImprovingOrUnproven,
    Sufficient,
    Insufficient,
    InsufficientAtMaximum,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransportCapacityEvaluation {
    pub(crate) action: TransportCapacityAction,
    pub(crate) trend: TransportCapacityTrend,
    pub(crate) throughput: TransportThroughputState,
    pub(crate) refresh_before_decision: bool,
}

pub(crate) fn evaluate_transport_capacity(
    backlog_units: i64,
    usable_freighters: usize,
    now: i64,
    previous: &TransportCapacityTrend,
) -> TransportCapacityEvaluation {
    let previous_backlog = previous.last_observed_backlog_units;
    let falling = previous_backlog.is_some_and(|value| backlog_units < value);
    let flat_or_rising = previous_backlog.is_some_and(|value| backlog_units >= value);
    let cooldown_active = previous
        .last_capacity_change_at_ms
        .is_some_and(|changed| now.saturating_sub(changed) < TRANSPORT_CAPACITY_COOLDOWN_MS);
    let class = transport_backlog_class(backlog_units);
    let throughput_insufficient = matches!(
        class,
        TransportBacklogClass::ScaleUp | TransportBacklogClass::Critical
    ) && flat_or_rising
        && previous.last_observation_at_ms.is_some()
        && !cooldown_active;
    let mut trend = previous.clone();
    trend.last_observed_backlog_units = Some(backlog_units.max(0));
    trend.last_observation_at_ms = Some(now);
    if class == TransportBacklogClass::Low {
        if trend.low_backlog_since_ms.is_none() {
            trend.low_backlog_since_ms = Some(now);
        }
    } else {
        trend.low_backlog_since_ms = None;
    }

    let action = if cooldown_active {
        TransportCapacityAction::Hold
    } else if class == TransportBacklogClass::Low
        && usable_freighters > MIN_TRANSPORT_FREIGHTERS
        && trend
            .low_backlog_since_ms
            .is_some_and(|since| now.saturating_sub(since) >= TRANSPORT_SCALE_DOWN_HOLD_MS)
    {
        TransportCapacityAction::RemoveOne
    } else {
        let requested = transport_scale_up_delta(backlog_units);
        let available = MAX_TRANSPORT_FREIGHTERS.saturating_sub(usable_freighters);
        let add = requested.min(available);
        if add == 0 || (falling && previous.last_capacity_change_at_ms.is_some()) {
            TransportCapacityAction::Hold
        } else {
            TransportCapacityAction::Add(add)
        }
    };

    let throughput = if throughput_insufficient && usable_freighters >= MAX_TRANSPORT_FREIGHTERS {
        TransportThroughputState::InsufficientAtMaximum
    } else if throughput_insufficient {
        TransportThroughputState::Insufficient
    } else if matches!(
        class,
        TransportBacklogClass::Low | TransportBacklogClass::HealthyBand
    ) {
        TransportThroughputState::Sufficient
    } else {
        TransportThroughputState::ImprovingOrUnproven
    };
    TransportCapacityEvaluation {
        action,
        trend,
        throughput,
        refresh_before_decision: throughput_insufficient,
    }
}

pub(crate) fn record_transport_capacity_change(trend: &mut TransportCapacityTrend, now: i64) {
    trend.last_capacity_change_at_ms = Some(now);
    trend.low_backlog_since_ms = None;
}

/// Durable activation phase for one mining transport route.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutePhase {
    Planned,
    Ready,
    Activating,
    Active,
}

/// Manufacturing purpose recorded on one print batch.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrintPurpose {
    Site,
    Route,
}

/// Device assignments checkpointed for one mining site.
#[allow(missing_docs)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SiteAssets {
    pub mining_controller: Option<String>,
    pub mining_drones: Vec<String>,
    pub survey_controller: Option<String>,
    pub survey_drones: Vec<String>,
    pub maintenance_drone: Option<String>,
    /// Owned System Ward assigned to protect this mining system. A System Hub
    /// may satisfy protection without populating this field because wards and
    /// hubs cannot be active in the same system.
    #[serde(default)]
    pub system_ward: Option<String>,
}

impl SiteAssets {
    /// Returns checkpointed productive site devices, excluding optional protection.
    pub(crate) fn productive_codes(&self) -> Vec<String> {
        self.mining_controller
            .iter()
            .chain(&self.mining_drones)
            .chain(self.survey_controller.iter())
            .chain(&self.survey_drones)
            .chain(self.maintenance_drone.iter())
            .cloned()
            .collect()
    }

    /// Returns every checkpointed device selected for this site.
    pub(crate) fn codes(&self) -> Vec<String> {
        self.productive_codes()
            .into_iter()
            .chain(self.system_ward.iter().cloned())
            .collect()
    }

    fn counts(&self) -> QuantityMap {
        let mut counts = QuantityMap::new();
        counts.insert(
            MINING_CONTROLLER.into(),
            if self.mining_controller.is_some() {
                1
            } else {
                0
            },
        );
        counts.insert(
            MINING_DRONE.into(),
            i64::try_from(self.mining_drones.len()).unwrap_or(i64::MAX),
        );
        counts.insert(
            SURVEY_CONTROLLER.into(),
            if self.survey_controller.is_some() {
                1
            } else {
                0
            },
        );
        counts.insert(
            SURVEY_DRONE.into(),
            i64::try_from(self.survey_drones.len()).unwrap_or(i64::MAX),
        );
        counts.insert(
            MAINTENANCE_DRONE.into(),
            if self.maintenance_drone.is_some() {
                1
            } else {
                0
            },
        );
        counts.insert(
            SYSTEM_WARD.into(),
            if self.system_ward.is_some() { 1 } else { 0 },
        );
        counts
    }
}

/// Durable checkpoint for one mining-site deployment.
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SiteMission {
    pub system: String,
    pub belt: String,
    pub density: String,
    pub tag: String,
    pub phase: SitePhase,
    pub assets: SiteAssets,
    pub missing: QuantityMap,
    pub carrier: Option<String>,
}

/// Durable checkpoint for one mining ferry route.
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouteMission {
    pub system: String,
    pub belt: String,
    pub tag: String,
    pub phase: RoutePhase,
    pub controller: Option<String>,
    /// Primary Cargo Freighter retained for checkpoint compatibility.
    pub freighter: Option<String>,
    /// Additional adopted Cargo Freighters on the same AMI route.
    #[serde(default)]
    pub additional_freighters: Vec<String>,
    /// Desired route capacity. Legacy route checkpoints default to one.
    #[serde(default = "default_transport_freighter_count")]
    pub desired_freighters: usize,
}

impl RouteMission {
    pub(crate) fn freighters(&self) -> Vec<String> {
        self.freighter
            .iter()
            .chain(&self.additional_freighters)
            .cloned()
            .collect()
    }
}

/// Durable manufacturing checkpoint for mining execution.
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionPrintBatch {
    pub purpose: PrintPurpose,
    pub factory_code: String,
    pub device_type: String,
    pub quantity: i64,
    pub projected_finish_seconds: f64,
    pub batch_tag: String,
    pub submission_started: bool,
    pub submitted: bool,
    pub operation_id: Option<String>,
    pub produced_codes: Vec<String>,
}

/// Complete durable mining expansion state suitable for workflow checkpointing.
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MiningMission {
    pub version: u32,
    pub mission_id: String,
    pub mission_tag: String,
    /// Historical UUID-derived mission tags still recognized while queued
    /// prints and already-produced stock are migrated to the system tag.
    #[serde(default)]
    pub legacy_mission_tags: Vec<String>,
    pub phase: MissionPhase,
    pub selected_replicant: String,
    pub hub_location: String,
    pub sites: Vec<SiteMission>,
    pub routes: Vec<RouteMission>,
    pub print_batches: Vec<ExecutionPrintBatch>,
    pub site_print_requirements: QuantityMap,
    pub route_print_requirements: QuantityMap,
    pub total_material_cost: QuantityMap,
    pub warnings: Vec<String>,
}

/// Compact structured progress derived from a durable mining checkpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MiningProgress {
    /// Current top-level execution phase.
    pub phase: MissionPhase,
    /// Operational sites and total planned sites.
    pub sites: (usize, usize),
    /// Active routes and total planned routes.
    pub routes: (usize, usize),
    /// Produced devices and total planned print quantity.
    pub printing: (usize, usize),
}

impl MiningMission {
    /// Returns progress without consulting live managed state.
    #[must_use]
    pub fn progress(&self) -> MiningProgress {
        MiningProgress {
            phase: self.phase,
            sites: (
                self.sites
                    .iter()
                    .filter(|site| site.phase == SitePhase::Operational)
                    .count(),
                self.sites.len(),
            ),
            routes: (
                self.routes
                    .iter()
                    .filter(|route| route.phase == RoutePhase::Active)
                    .count(),
                self.routes.len(),
            ),
            printing: (
                self.print_batches
                    .iter()
                    .map(|batch| batch.produced_codes.len())
                    .sum(),
                self.print_batches
                    .iter()
                    .filter_map(|batch| usize::try_from(batch.quantity).ok())
                    .sum(),
            ),
        }
    }
}

/// Materializes mining sites, routes, and shared manufacturing as durable work items.
pub fn mining_work_item_specs(
    workflow_id: WorkflowId,
    mission: &MiningMission,
    region: &str,
) -> Result<Vec<WorkItemSpec>, replicant_workflow::RepositoryError> {
    let mut specs = Vec::new();
    let site_kind = WorkflowKind::new("mining.site")?;
    for (index, site) in mission.sites.iter().enumerate() {
        if site.phase == SitePhase::Operational {
            continue;
        }
        specs.push(WorkItemSpec {
            workflow_id,
            dedupe_key: format!("mining.site:{}", site.belt),
            kind: site_kind.clone(),
            sort_key: format!("1:{index:08}:{}", site.belt),
            payload_json: serde_json::json!({
                "type": "site",
                "index": index,
                "system": site.system,
                "belt": site.belt,
                "legacy_complete": site.phase == SitePhase::Operational,
            }),
            preconditions_json: serde_json::json!([{
                "kind": "mining.site_incomplete",
                "parameters": { "belt": site.belt }
            }]),
            requirements_json: serde_json::to_value(mining_site_item_requirements(region, site))?,
            deadline_at_ms: None,
        });
    }
    let route_kind = WorkflowKind::new("mining.route")?;
    for (index, route) in mission.routes.iter().enumerate() {
        specs.push(WorkItemSpec {
            workflow_id,
            dedupe_key: format!("mining.route:{}:{}", route.belt, mission.hub_location),
            kind: route_kind.clone(),
            sort_key: format!("2:{index:08}:{}", route.belt),
            payload_json: serde_json::json!({
                "type": "route",
                "index": index,
                "system": route.system,
                "belt": route.belt,
                "legacy_complete": route.phase == RoutePhase::Active,
            }),
            preconditions_json: serde_json::json!([{
                "kind": "mining.route_inactive",
                "parameters": { "belt": route.belt, "hub": mission.hub_location }
            }]),
            requirements_json: serde_json::to_value(mining_route_item_requirements(region, route))?,
            deadline_at_ms: None,
        });
    }
    if !mission.print_batches.is_empty() {
        let stage_kind = WorkflowKind::new("mining.stage")?;
        let mut requirements = vec![
            ResourceRequirement {
                key: "worker".into(),
                kind: "replicant".into(),
                capabilities: vec![OPERATIONAL_REGIONAL_WORKER_CAPABILITY.into()],
                scope: RequirementScope::Region(region.to_owned()),
                count: 1,
                quantity: 1,
            },
            ResourceRequirement {
                key: "autofactory".into(),
                kind: "autofactory".into(),
                capabilities: Vec::new(),
                scope: RequirementScope::Location(mission.hub_location.clone()),
                count: 1,
                quantity: 1,
            },
        ];
        requirements.extend(mission.total_material_cost.iter().filter_map(
            |(resource, quantity)| {
                let quantity = u64::try_from(*quantity).ok()?;
                (quantity != 0).then(|| ResourceRequirement {
                    key: format!("material:{resource}"),
                    kind: "material".into(),
                    capabilities: vec![resource.clone()],
                    scope: RequirementScope::Location(mission.hub_location.clone()),
                    count: 1,
                    quantity,
                })
            },
        ));
        specs.push(WorkItemSpec {
            workflow_id,
            dedupe_key: "mining.stage:manufacturing".into(),
            kind: stage_kind,
            sort_key: "0:manufacturing".into(),
            payload_json: serde_json::json!({
                "type": "stage",
                "index": 0,
                // This is part of the immutable work-item specification. Manufacturing
                // completion is derived from the checkpoint after reconciliation.
                "legacy_complete": false,
            }),
            preconditions_json: serde_json::json!([]),
            requirements_json: serde_json::to_value(requirements)?,
            deadline_at_ms: None,
        });
    }
    Ok(specs)
}

fn mining_site_positive_count(quantity: i64) -> Option<u32> {
    u32::try_from(quantity).ok().filter(|count| *count > 0)
}

fn mining_site_has_missing(missing: &QuantityMap, device_type: &str) -> bool {
    missing
        .get(device_type)
        .and_then(|quantity| mining_site_positive_count(*quantity))
        .is_some()
}

fn mining_site_transport_quantity(missing: &QuantityMap) -> u64 {
    missing
        .iter()
        .filter(|(device_type, _)| {
            matches!(
                device_type.as_str(),
                MINING_CONTROLLER
                    | MINING_DRONE
                    | SURVEY_CONTROLLER
                    | SURVEY_DRONE
                    | MAINTENANCE_DRONE
            )
        })
        .filter_map(|(_, quantity)| mining_site_positive_count(*quantity).map(u64::from))
        .fold(0, u64::saturating_add)
}

fn mining_site_item_requirements(region: &str, site: &SiteMission) -> Vec<ResourceRequirement> {
    let scope = || RequirementScope::Region(region.to_owned());
    let mut requirements = vec![ResourceRequirement {
        key: "worker".into(),
        kind: "replicant".into(),
        capabilities: vec![OPERATIONAL_REGIONAL_WORKER_CAPABILITY.into()],
        scope: scope(),
        count: 1,
        quantity: 1,
    }];
    for (device_type, quantity) in &site.missing {
        let Some(count) = mining_site_positive_count(*quantity) else {
            continue;
        };
        let key = match device_type.as_str() {
            MINING_CONTROLLER => "mining_controller",
            MINING_DRONE => "mining_drones",
            SURVEY_CONTROLLER => "survey_controller",
            SURVEY_DRONE => "survey_drones",
            MAINTENANCE_DRONE => "maintenance_drone",
            SYSTEM_WARD => "system_ward",
            _ => continue,
        };
        requirements.push(ResourceRequirement {
            key: key.into(),
            kind: "device".into(),
            capabilities: vec![device_type.clone()],
            scope: scope(),
            count,
            quantity: 1,
        });
    }
    let missing_devices = mining_site_transport_quantity(&site.missing);
    if missing_devices > 0 {
        requirements.push(ResourceRequirement {
            key: "carrier".into(),
            kind: "device".into(),
            capabilities: vec![SURGE_CARRIER.into()],
            scope: scope(),
            count: 1,
            quantity: 1,
        });
        requirements.push(ResourceRequirement {
            key: "attach".into(),
            kind: "attach".into(),
            capabilities: Vec::new(),
            scope: scope(),
            count: 1,
            quantity: missing_devices,
        });
    }
    requirements
}

fn mining_route_item_requirements(region: &str, route: &RouteMission) -> Vec<ResourceRequirement> {
    let scope = || RequirementScope::Region(region.to_owned());
    let mut requirements = vec![ResourceRequirement {
        key: "worker".into(),
        kind: "replicant".into(),
        capabilities: vec![OPERATIONAL_REGIONAL_WORKER_CAPABILITY.into()],
        scope: scope(),
        count: 1,
        quantity: 1,
    }];
    if route.controller.is_none() {
        requirements.push(ResourceRequirement {
            key: "transport_controller".into(),
            kind: "device".into(),
            capabilities: vec![TRANSPORT_CONTROLLER.into()],
            scope: scope(),
            count: 1,
            quantity: 1,
        });
    }
    let missing_freighters = route
        .desired_freighters
        .saturating_sub(route.freighters().len());
    if let Ok(count) = u32::try_from(missing_freighters)
        && count > 0
    {
        requirements.push(ResourceRequirement {
            key: "freighters".into(),
            kind: "device".into(),
            capabilities: vec![CARGO_FREIGHTER.into()],
            scope: scope(),
            count,
            quantity: 1,
        });
    }
    requirements
}

/// Executes one isolated mining site, route, or manufacturing stage.
pub async fn execute_mining_item(
    client: &Client,
    mission: &MiningMission,
    item_type: &str,
    index: usize,
    allocations: &AllocationSet,
    wait_timeout: Duration,
    claims: MiningWorkflowClaims,
) -> AnyResult<MiningMission> {
    let worker = mining_allocated_identity(allocations, "worker", "replicant")?;
    let mut lane = mission.clone();
    lane.selected_replicant.clone_from(&worker);
    match item_type {
        "site" => {
            let mut site = lane.sites.get(index).cloned().ok_or_else(|| {
                app_error(io::ErrorKind::InvalidInput, "mining site index is invalid")
            })?;
            let missing = site.missing.clone();
            if mining_site_has_missing(&missing, MINING_CONTROLLER) {
                site.assets.mining_controller = Some(mining_allocated_identity(
                    allocations,
                    "mining_controller",
                    "device",
                )?);
            }
            if mining_site_has_missing(&missing, MINING_DRONE) {
                site.assets
                    .mining_drones
                    .extend(mining_allocated_identities(
                        allocations,
                        "mining_drones",
                        "device",
                    )?);
                site.assets.mining_drones.sort();
                site.assets.mining_drones.dedup();
            }
            if mining_site_has_missing(&missing, SURVEY_CONTROLLER) {
                site.assets.survey_controller = Some(mining_allocated_identity(
                    allocations,
                    "survey_controller",
                    "device",
                )?);
            }
            if mining_site_has_missing(&missing, SURVEY_DRONE) {
                site.assets
                    .survey_drones
                    .extend(mining_allocated_identities(
                        allocations,
                        "survey_drones",
                        "device",
                    )?);
                site.assets.survey_drones.sort();
                site.assets.survey_drones.dedup();
            }
            if mining_site_has_missing(&missing, MAINTENANCE_DRONE) {
                site.assets.maintenance_drone = Some(mining_allocated_identity(
                    allocations,
                    "maintenance_drone",
                    "device",
                )?);
            }
            if mining_site_has_missing(&missing, SYSTEM_WARD) {
                // Keep the allocated ward reserved for the follow-up delivery.
                // The nine mining devices can deploy and begin configuring first.
                site.assets.system_ward = Some(mining_allocated_identity(
                    allocations,
                    "system_ward",
                    "device",
                )?);
            }
            if mining_site_transport_quantity(&missing) > 0 {
                site.carrier = Some(mining_allocated_identity(allocations, "carrier", "device")?);
                mining_validate_carrier_capacity_owner(allocations)?;
            }
            site.missing
                .retain(|device_type, _| device_type == SYSTEM_WARD);
            lane.sites = vec![site];
            lane.routes.clear();
            lane.print_batches
                .retain(|batch| batch.purpose == PrintPurpose::Site);
        }
        "route" => {
            let mut route = lane.routes.get(index).cloned().ok_or_else(|| {
                app_error(io::ErrorKind::InvalidInput, "mining route index is invalid")
            })?;
            if route.controller.is_none() {
                route.controller = Some(mining_allocated_identity(
                    allocations,
                    "transport_controller",
                    "device",
                )?);
            }
            let missing_freighters = route
                .desired_freighters
                .saturating_sub(route.freighters().len());
            if missing_freighters > 0 {
                let mut allocated =
                    mining_allocated_identities(allocations, "freighters", "device")?;
                allocated.truncate(missing_freighters);
                if route.freighter.is_none() {
                    route.freighter = allocated.first().cloned();
                    if !allocated.is_empty() {
                        allocated.remove(0);
                    }
                }
                route.additional_freighters.extend(allocated);
                route.additional_freighters.sort();
                route.additional_freighters.dedup();
            }
            lane.routes = vec![route];
            lane.sites.clear();
            lane.print_batches
                .retain(|batch| batch.purpose == PrintPurpose::Route);
        }
        "stage" => {
            let _ = mining_allocated_identity(allocations, "autofactory", "autofactory")?;
            for (resource, quantity) in &mission.total_material_cost {
                if *quantity > 0 {
                    let key = format!("material:{resource}");
                    let allocated = allocations
                        .by_requirement
                        .get(&key)
                        .into_iter()
                        .flatten()
                        .map(|allocation| allocation.quantity)
                        .sum::<u64>();
                    if allocated < u64::try_from(*quantity).unwrap_or(u64::MAX) {
                        return Err(app_error(
                            io::ErrorKind::WouldBlock,
                            format!("mining stage allocation omitted material {resource}"),
                        ));
                    }
                }
            }
            lane.sites.clear();
            lane.routes.clear();
        }
        _ => {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("unknown mining item type {item_type}"),
            ));
        }
    }
    let plan_path = std::env::temp_dir().join(format!(
        "replicant-mining-item-{}-{item_type}-{index}.json",
        lane.mission_id
    ));
    save_plan(&plan_path, &lane)?;
    let request = MiningExpansionRequest {
        systems: lane
            .sites
            .iter()
            .map(|site| site.system.clone())
            .chain(lane.routes.iter().map(|route| route.system.clone()))
            .collect(),
        replicant: worker,
        hub: lane.hub_location.clone(),
        transport_routes: Vec::new(),
        mission_file: plan_path.clone(),
        wait_timeout,
        max_concurrency: 1,
    };
    let result = execute_expansion(client, &request, Some(claims)).await;
    let final_state = load_expansion(&plan_path);
    let _ = fs::remove_file(plan_path);
    result?;
    final_state
}

fn mining_allocated_identity(
    allocations: &AllocationSet,
    requirement: &str,
    expected: &str,
) -> AnyResult<String> {
    mining_allocated_identities(allocations, requirement, expected)?
        .into_iter()
        .next()
        .ok_or_else(|| app_error(io::ErrorKind::InvalidData, "empty mining allocation"))
}

fn mining_allocated_identities(
    allocations: &AllocationSet,
    requirement: &str,
    expected: &str,
) -> AnyResult<Vec<String>> {
    allocations
        .by_requirement
        .get(requirement)
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("mining item allocation omitted {requirement}"),
            )
        })?
        .iter()
        .map(|allocation| match (&allocation.resource, expected) {
            (ResourceKey::Replicant(code), "replicant")
            | (ResourceKey::Device(code), "device")
            | (ResourceKey::Autofactory(code), "autofactory") => Ok(code.clone()),
            _ => Err(app_error(
                io::ErrorKind::InvalidData,
                format!("mining allocation {requirement} has the wrong resource kind"),
            )),
        })
        .collect()
}

fn mining_validate_carrier_capacity_owner(allocations: &AllocationSet) -> AnyResult<()> {
    let carrier = mining_allocated_identity(allocations, "carrier", "device")?;
    let matching = ["attach", "stow"].into_iter().any(|requirement| {
        allocations
            .by_requirement
            .get(requirement)
            .into_iter()
            .flatten()
            .any(|allocation| {
                matches!(
                    &allocation.resource,
                    ResourceKey::Namespaced { namespace, key }
                        if matches!(
                            (requirement, namespace.as_str()),
                            ("attach", "attach") | ("stow", "attach" | "stow")
                        ) && key == &carrier
                )
            })
    });
    if !matching {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("mining item attachment-capacity allocation does not belong to {carrier}"),
        ));
    }
    Ok(())
}

/// Merges one terminal mining item checkpoint into campaign state.
pub fn merge_mining_item_state(
    mission: &mut MiningMission,
    lane: &MiningMission,
    item_type: &str,
    index: usize,
) {
    match item_type {
        "site" => {
            if let (Some(target), Some(source)) = (mission.sites.get_mut(index), lane.sites.first())
            {
                target.clone_from(source);
            }
        }
        "route" => {
            if let (Some(target), Some(source)) =
                (mission.routes.get_mut(index), lane.routes.first())
            {
                target.clone_from(source);
            }
        }
        "stage" => mission.print_batches.clone_from(&lane.print_batches),
        _ => {}
    }
    if mission
        .sites
        .iter()
        .all(|site| site.phase == SitePhase::Operational)
        && mission
            .routes
            .iter()
            .all(|route| route.phase == RoutePhase::Active)
    {
        mission.phase = MissionPhase::Completed;
    }
}

/// Returns whether an isolated mining item reached its domain terminal state.
#[must_use]
pub fn mining_item_completed(mission: &MiningMission, item_type: &str) -> bool {
    match item_type {
        "site" => mission
            .sites
            .first()
            .is_some_and(|site| site.phase == SitePhase::Operational),
        "route" => mission
            .routes
            .first()
            .is_some_and(|route| route.phase == RoutePhase::Active),
        "stage" => mission.print_batches.iter().all(|batch| {
            usize::try_from(batch.quantity)
                .is_ok_and(|quantity| batch.produced_codes.len() >= quantity)
        }),
        _ => false,
    }
}

struct MissionLock {
    path: PathBuf,
}

impl MissionLock {
    fn acquire(mission_path: &Path) -> AnyResult<Self> {
        let lock_path = mission_path.with_extension("lock");
        if let Some(parent) = lock_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        for attempt in 0..2 {
            match OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&lock_path)
            {
                Ok(mut file) => {
                    writeln!(file, "{}", std::process::id())?;
                    file.sync_all()?;
                    return Ok(Self { path: lock_path });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists && attempt == 0 => {
                    let owner = fs::read_to_string(&lock_path)
                        .ok()
                        .and_then(|value| value.trim().parse::<u32>().ok());
                    let owner_is_running =
                        owner.is_some_and(|pid| PathBuf::from(format!("/proc/{pid}")).exists());
                    if owner_is_running {
                        return Err(app_error(
                            io::ErrorKind::WouldBlock,
                            format!(
                                "another mining executor holds {} (pid {})",
                                lock_path.display(),
                                owner.unwrap_or_default()
                            ),
                        ));
                    }
                    fs::remove_file(&lock_path)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(app_error(
            io::ErrorKind::WouldBlock,
            format!("could not acquire {}", lock_path.display()),
        ))
    }
}

impl Drop for MissionLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

async fn create_plan(client: &Client, config: &Config) -> AnyResult<MiningMission> {
    if config.plan_path.exists() && !config.replace_plan {
        let existing = load_expansion(&config.plan_path)?;
        if !existing.phase.is_terminal() {
            return Err(app_error(
                io::ErrorKind::AlreadyExists,
                format!(
                    "incomplete mission {} exists at {}; use `run`, `status`, or plan --replace-plan",
                    existing.mission_id,
                    config.plan_path.display()
                ),
            ));
        }
    }

    info!("planning mining expansion from committed managed state");
    let selected_replicant = select_replicant(client, config.replicant.as_deref()).await?;
    let mut systems = config.requested_systems()?;
    let transport_routes =
        validate_transport_routes(client, &systems, &config.hub, &config.transport_routes).await?;
    let explicit_routes = transport_routes
        .iter()
        .map(|route| (route.system.as_str(), route))
        .collect::<BTreeMap<_, _>>();
    let devices = device_snapshots(client).await?;
    let catalogue = client.galaxy().catalogue();
    sort_systems_by_hub_distance(&mut systems, &config.hub, &catalogue);
    let blueprints = fetch_blueprints(client).await?;
    let factories = factory_workloads(client, &blueprints, &config.hub).await?;

    let mut sites = Vec::new();
    for system in systems {
        let belt = match explicit_routes.get(system.as_str()) {
            Some(route) => selected_belt_for_route(client, route).await?,
            None => select_belt(client, &system, &devices).await?,
        };
        let audit = audit_site(&devices, &system, &belt.designation);
        if !audit.mutation_safe {
            return Err(app_error(
                io::ErrorKind::WouldBlock,
                format!(
                    "mining site {} requires authoritative evidence before provisioning: {:?}",
                    belt.designation, audit.issues
                ),
            ));
        }
        let missing = site_shortages(&audit);
        sites.push(SiteMission {
            system: system.clone(),
            belt: belt.designation,
            density: belt.density,
            tag: site_tag(&system),
            phase: if audit.operational {
                SitePhase::Operational
            } else {
                SitePhase::Planned
            },
            assets: audit.assets,
            missing,
            carrier: None,
        });
    }

    let mission_tag = mining_mission_tag(&config.hub);
    let mut site_required = QuantityMap::new();
    for site in &sites {
        add_quantities(&mut site_required, &site.missing);
    }
    let reusable_site = reusable_counts(&devices, &config.hub, &mission_tag, true);
    let site_print_requirements = shortages(&site_required, &reusable_site);

    let mut routes = Vec::new();
    let mut route_required = QuantityMap::new();
    for site in &sites {
        if site.belt == config.hub {
            continue;
        }
        let desired_freighters = explicit_routes
            .get(site.system.as_str())
            .map_or(MIN_TRANSPORT_FREIGHTERS, |route| route.desired_freighters);
        let audit = transport_service_present(&devices, &site.system, &site.belt, &config.hub);
        if audit.state == EvidenceState::Unknown {
            return Err(app_error(
                io::ErrorKind::WouldBlock,
                format!(
                    "mining route {} to {} requires authoritative evidence before provisioning",
                    site.belt, config.hub
                ),
            ));
        }
        let usable_freighters = audit.usable_freighters.clone();
        let freighter = usable_freighters.first().cloned();
        let additional_freighters = usable_freighters.into_iter().skip(1).collect::<Vec<_>>();
        let missing_freighters = desired_freighters.saturating_sub(audit.usable_freighter_count());
        if audit.controller.is_none() {
            *route_required
                .entry(TRANSPORT_CONTROLLER.to_owned())
                .or_default() += 1;
        }
        *route_required
            .entry(CARGO_FREIGHTER.to_owned())
            .or_default() += i64::try_from(missing_freighters)?;
        routes.push(RouteMission {
            system: site.system.clone(),
            belt: site.belt.clone(),
            tag: site.tag.clone(),
            phase: if audit.state == EvidenceState::Present && missing_freighters == 0 {
                RoutePhase::Active
            } else {
                RoutePhase::Planned
            },
            controller: audit.controller,
            freighter,
            additional_freighters,
            desired_freighters,
        });
    }
    let reusable_route = reusable_counts(&devices, &config.hub, &mission_tag, false);
    let route_print_requirements = shortages(&route_required, &reusable_route);

    let site_schedule = schedule_prints(&site_print_requirements, &blueprints, &factories)?;
    let route_factories =
        site_schedule
            .batches
            .iter()
            .fold(factories.clone(), |mut factories, batch| {
                if let Some(factory) = factories
                    .iter_mut()
                    .find(|factory| factory.code == batch.factory_code)
                {
                    factory.remaining_seconds = batch.projected_finish_seconds;
                }
                factories
            });
    let route_schedule = schedule_prints(&route_print_requirements, &blueprints, &route_factories)?;
    let mission_id = uuid::Uuid::new_v4().simple().to_string();
    let mut print_batches =
        execution_batches(&mission_id, PrintPurpose::Site, &site_schedule.batches);
    print_batches.extend(execution_batches(
        &mission_id,
        PrintPurpose::Route,
        &route_schedule.batches,
    ));
    let mut total_material_cost = QuantityMap::new();
    for (device_type, quantity) in site_print_requirements
        .iter()
        .chain(&route_print_requirements)
    {
        add_quantities(
            &mut total_material_cost,
            &blueprint_resource_cost(device_type, *quantity, &blueprints)?,
        );
    }

    let mission = MiningMission {
        version: PLAN_VERSION,
        mission_id,
        mission_tag,
        legacy_mission_tags: Vec::new(),
        phase: MissionPhase::Planned,
        selected_replicant,
        hub_location: config.hub.clone(),
        sites,
        routes,
        print_batches,
        site_print_requirements,
        route_print_requirements,
        total_material_cost,
        warnings: Vec::new(),
    };
    save_plan(&config.plan_path, &mission)?;
    Ok(mission)
}

async fn validate_transport_routes(
    client: &Client,
    systems: &[String],
    hub: &str,
    routes: &[AmiTransportRouteIntent],
) -> AnyResult<Vec<AmiTransportRouteIntent>> {
    let systems = systems.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut normalized = Vec::with_capacity(routes.len());
    let mut pairs = BTreeSet::new();
    let mut systems_seen = BTreeSet::new();
    for route in routes {
        let route = AmiTransportRouteIntent {
            system: route.system.trim().to_ascii_uppercase(),
            collect: route.collect.trim().to_ascii_uppercase(),
            deliver: route.deliver.trim().to_ascii_uppercase(),
            desired_freighters: route.desired_freighters,
        };
        if route.system.is_empty() || route.collect.is_empty() || route.deliver.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "transport route fields must be nonblank",
            ));
        }
        if !(MIN_TRANSPORT_FREIGHTERS..=MAX_TRANSPORT_FREIGHTERS)
            .contains(&route.desired_freighters)
        {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "transport route desired_freighters must be between {MIN_TRANSPORT_FREIGHTERS} and {MAX_TRANSPORT_FREIGHTERS}"
                ),
            ));
        }
        if !systems.contains(route.system.as_str()) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("transport route system {} is not in systems", route.system),
            ));
        }
        if route.deliver != hub {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("transport route delivery must equal hub {hub}"),
            ));
        }
        if !pairs.insert((route.collect.clone(), route.deliver.clone())) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "duplicate transport route {} -> {}",
                    route.collect, route.deliver
                ),
            ));
        }
        if !systems_seen.insert(route.system.clone()) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "at most one explicit transport route is allowed for {}",
                    route.system
                ),
            ));
        }
        let location = match client.locations().cached(&route.system) {
            Some(location) => location,
            None => {
                validation::location(
                    client,
                    &route.system,
                    validation::ValidationReason::Mutation,
                )
                .await?
            }
        };
        if !belts_from_location(&location)
            .iter()
            .any(|belt| belt.designation == route.collect)
        {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "transport route collect location {} is not a discovered belt in {}",
                    route.collect, route.system
                ),
            ));
        }
        normalized.push(route);
    }
    Ok(normalized)
}

fn sort_systems_by_hub_distance(systems: &mut [String], hub: &str, catalogue: &[Star]) {
    let positions = catalogue
        .iter()
        .filter_map(|star| {
            star.position
                .map(|position| (star.key.id.as_str(), position))
        })
        .collect::<BTreeMap<_, _>>();
    let hub_position = catalogue
        .iter()
        .filter(|star| location_is_in_system(hub, star.key.id.as_str()))
        .filter_map(|star| star.position)
        .next();
    systems.sort_by(|left, right| {
        let distance = |system: &str| {
            let position = positions.get(system)?;
            let hub = hub_position?;
            Some(
                (position.x - hub.x).powi(2)
                    + (position.y - hub.y).powi(2)
                    + (position.z - hub.z).powi(2),
            )
        };
        match (distance(left), distance(right)) {
            (Some(left_distance), Some(right_distance)) => left_distance
                .total_cmp(&right_distance)
                .then_with(|| left.cmp(right)),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => left.cmp(right),
        }
    });
}

fn location_is_in_system(location: &str, system: &str) -> bool {
    location == system
        || location
            .strip_prefix(system)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

fn execution_batches(
    mission_id: &str,
    purpose: PrintPurpose,
    batches: &[PrintBatch],
) -> Vec<ExecutionPrintBatch> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.quantity).map(move |unit_index| ExecutionPrintBatch {
                purpose,
                factory_code: batch.factory_code.clone(),
                device_type: batch.device_type.clone(),
                quantity: 1,
                projected_finish_seconds: batch.projected_finish_seconds,
                batch_tag: format!(
                    "mine-b:{:016x}",
                    stable_hash(&format!(
                        "{mission_id}:{purpose:?}:{}:{}:{}:{unit_index}",
                        batch.factory_code, batch.sequence, batch.device_type
                    ))
                ),
                submission_started: false,
                submitted: false,
                operation_id: None,
                produced_codes: Vec::new(),
            })
        })
        .collect()
}

const MINING_MISSION_TAG_PREFIX: &str = "mine-m:";
const MAX_DEVICE_TAG_CHARS: usize = 32;

/// Returns the stable mining reservation tag for a manufacturing hub system.
pub(crate) fn mining_mission_tag(hub_location: &str) -> String {
    let system = hub_location
        .split('-')
        .next()
        .filter(|system| !system.is_empty())
        .unwrap_or(hub_location);
    bounded_system_tag(MINING_MISSION_TAG_PREFIX, system)
}

/// Upgrades an in-memory legacy mining checkpoint while retaining old aliases.
pub(crate) fn migrate_mission_tag_metadata(mission: &mut MiningMission) -> bool {
    let desired = mining_mission_tag(&mission.hub_location);
    let mut changed = false;
    if mission.mission_tag != desired {
        let previous = std::mem::replace(&mut mission.mission_tag, desired.clone());
        if previous.starts_with(MINING_MISSION_TAG_PREFIX)
            && !mission.legacy_mission_tags.contains(&previous)
        {
            mission.legacy_mission_tags.push(previous);
        }
        changed = true;
    }
    let before = mission.legacy_mission_tags.len();
    mission
        .legacy_mission_tags
        .retain(|tag| tag.starts_with(MINING_MISSION_TAG_PREFIX) && tag != &desired);
    mission.legacy_mission_tags.sort();
    mission.legacy_mission_tags.dedup();
    changed || mission.legacy_mission_tags.len() != before
}

/// Returns whether a mining mission tag uses the old 16-hex hash identity.
pub(crate) fn is_opaque_mining_mission_tag(tag: &str) -> bool {
    tag.strip_prefix(MINING_MISSION_TAG_PREFIX)
        .is_some_and(|suffix| {
            suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

fn bounded_system_tag(prefix: &str, system: &str) -> String {
    const HASH_CHARS: usize = 12;
    let normalized = system
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let normalized = normalized.trim_matches('-');
    let direct = format!("{prefix}{normalized}");
    if direct.chars().count() <= MAX_DEVICE_TAG_CHARS {
        return direct;
    }

    let fixed = prefix.chars().count() + 1 + HASH_CHARS;
    let head_budget = MAX_DEVICE_TAG_CHARS.saturating_sub(fixed).max(1);
    let mut head = normalized.chars().take(head_budget).collect::<String>();
    head = head.trim_end_matches('-').to_owned();
    if head.is_empty() {
        head.push('s');
    }
    let hash = stable_hash(normalized) & 0x0000_ffff_ffff_ffff;
    format!("{prefix}{head}-{hash:012x}")
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub(crate) async fn device_snapshots(client: &Client) -> AnyResult<Vec<Device>> {
    let handles = client.devices().find().owned().collect().await?;
    let mut devices = Vec::with_capacity(handles.len());
    for handle in handles {
        devices.push(handle.snapshot().await?);
    }
    devices.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(devices)
}

async fn select_replicant(client: &Client, requested: Option<&str>) -> AnyResult<String> {
    let requested = requested.ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidInput,
            "mining expansion requires a replicant name or code",
        )
    })?;
    let mut matches = client
        .state()
        .owned_replicants()?
        .into_iter()
        .filter(|replicant| {
            replicant.key.id.as_str().eq_ignore_ascii_case(requested)
                || replicant
                    .name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(requested))
        })
        .collect::<Vec<_>>();
    match matches.len() {
        1 => Ok(matches.remove(0).key.id.as_str().to_owned()),
        0 => Err(app_error(
            io::ErrorKind::NotFound,
            format!("no owned replicant matches {requested:?}"),
        )),
        _ => Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("replicant name {requested:?} is ambiguous; use its code"),
        )),
    }
}

#[derive(Clone, Debug)]
struct SelectedBelt {
    designation: String,
    density: String,
}

async fn select_belt(client: &Client, system: &str, devices: &[Device]) -> AnyResult<SelectedBelt> {
    let location = match client.locations().cached(system) {
        Some(location) => location,
        None => {
            validation::location(client, system, validation::ValidationReason::Mutation).await?
        }
    };
    preferred_belt(belts_from_location(&location), devices, system)
}

fn preferred_belt(
    mut belts: Vec<SelectedBelt>,
    devices: &[Device],
    system: &str,
) -> AnyResult<SelectedBelt> {
    belts.sort_by(|left, right| {
        managed_belt_asset_count(devices, &right.designation)
            .cmp(&managed_belt_asset_count(devices, &left.designation))
            .then_with(|| density_rank(&right.density).cmp(&density_rank(&left.density)))
            .then_with(|| left.designation.cmp(&right.designation))
    });
    belts.into_iter().next().ok_or_else(|| {
        app_error(
            io::ErrorKind::NotFound,
            format!("system {system} has no discovered asteroid belt"),
        )
    })
}

async fn selected_belt_for_route(
    client: &Client,
    route: &AmiTransportRouteIntent,
) -> AnyResult<SelectedBelt> {
    let location = match client.locations().cached(&route.system) {
        Some(location) => location,
        None => {
            validation::location(
                client,
                &route.system,
                validation::ValidationReason::Mutation,
            )
            .await?
        }
    };
    belts_from_location(&location)
        .into_iter()
        .find(|belt| belt.designation == route.collect)
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "transport route collect location {} is not a discovered belt in {}",
                    route.collect, route.system
                ),
            )
        })
}

fn managed_belt_asset_count(devices: &[Device], belt: &str) -> usize {
    devices
        .iter()
        .filter(|device| device_location(device) == Some(belt))
        .filter(|device| {
            matches!(
                device_type(device),
                Some(
                    MINING_CONTROLLER
                        | MINING_DRONE
                        | SURVEY_CONTROLLER
                        | SURVEY_DRONE
                        | MAINTENANCE_DRONE
                )
            )
        })
        .count()
}

fn belts_from_location(location: &Location) -> Vec<SelectedBelt> {
    let Some(asteroid_belt) = location.unknown.get("asteroid_belt") else {
        return Vec::new();
    };
    asteroid_belt
        .get("belts")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(asteroid_belt))
        .iter()
        .filter_map(|value| {
            let object = value.as_object()?;
            Some(SelectedBelt {
                designation: object.get("designation")?.as_str()?.to_owned(),
                density: object
                    .get("density")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
            })
        })
        .collect()
}

fn density_rank(density: &str) -> u8 {
    match density.to_ascii_lowercase().as_str() {
        "dense" => 3,
        "moderate" => 2,
        "sparse" => 1,
        _ => 0,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MiningSiteRepair {
    Adopt {
        device: String,
        controller: String,
    },
    SetDirective {
        device: String,
        directive: &'static str,
    },
    LaunchController {
        device: String,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MiningSiteIssue {
    MissingDevice { device_type: String, count: i64 },
    UnusableDevice { device: String, device_type: String },
    CommittedElsewhere { device: String, device_type: String },
    EvidenceIncomplete { device: String, field: &'static str },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SiteAudit {
    pub(crate) assets: SiteAssets,
    /// Productive readiness only. A maintenance rotation that is due does not
    /// turn an otherwise-working site into a hardware shortage.
    pub(crate) operational: bool,
    pub(crate) mining_adoption_correct: bool,
    pub(crate) survey_adoption_correct: bool,
    pub(crate) normal_repair_needed: bool,
    pub(crate) maintenance_rotation_due: bool,
    /// Whether the evidence used for a repair is complete enough to mutate.
    pub(crate) mutation_safe: bool,
    pub(crate) repairs: Vec<MiningSiteRepair>,
    pub(crate) issues: Vec<MiningSiteIssue>,
    shortages: QuantityMap,
}

impl SiteAudit {
    pub(crate) fn director_reason(&self) -> Option<String> {
        if !self.mutation_safe
            && let Some(reason) = self.issues.iter().find_map(|issue| match issue {
                MiningSiteIssue::EvidenceIncomplete { device, field } => {
                    Some(format!("{device} has incomplete {field} evidence"))
                }
                _ => None,
            })
        {
            return Some(reason);
        }
        if (!self.mining_adoption_correct || !self.survey_adoption_correct)
            && let Some(reason) = self.repairs.iter().find_map(|repair| match repair {
                MiningSiteRepair::Adopt { device, controller } => {
                    Some(format!("re-adopt {device} to {controller}"))
                }
                _ => None,
            })
        {
            return Some(reason);
        }
        if let Some(reason) = self.issues.iter().find_map(|issue| match issue {
            MiningSiteIssue::UnusableDevice {
                device,
                device_type,
            } => Some(format!("replace unusable {device_type} {device}")),
            _ => None,
        }) {
            return Some(reason);
        }
        if let Some(reason) = self.issues.iter().find_map(|issue| match issue {
            MiningSiteIssue::CommittedElsewhere {
                device,
                device_type,
            } => Some(format!(
                "{device_type} {device} is committed elsewhere and cannot be reused"
            )),
            _ => None,
        }) {
            return Some(reason);
        }
        if let Some(reason) = self.issues.iter().find_map(|issue| match issue {
            MiningSiteIssue::MissingDevice { device_type, count } => {
                Some(format!("provision {count} missing {device_type}"))
            }
            _ => None,
        }) {
            return Some(reason);
        }
        self.repairs.iter().find_map(|repair| match repair {
            MiningSiteRepair::SetDirective { device, directive } => {
                Some(format!("set {device} directive to {directive}"))
            }
            MiningSiteRepair::LaunchController { device } => {
                Some(format!("restore controller {device} to coordinating state"))
            }
            MiningSiteRepair::Adopt { device, controller } => {
                Some(format!("re-adopt {device} to {controller}"))
            }
        })
    }
}

pub(crate) fn audit_site(devices: &[Device], system: &str, belt: &str) -> SiteAudit {
    let expected_site_tag = site_tag(system);
    let at_belt = devices
        .iter()
        .filter(|device| device_location(device) == Some(belt))
        .collect::<Vec<_>>();
    let mining_controller = select_controller(
        &at_belt,
        MINING_CONTROLLER,
        MINING_DRONE,
        &expected_site_tag,
    );
    let survey_controller = select_controller(
        &at_belt,
        SURVEY_CONTROLLER,
        SURVEY_DRONE,
        &expected_site_tag,
    );
    let mut repairs = Vec::new();
    let mut issues = Vec::new();
    let mut mutation_safe = true;
    for device in devices.iter().filter(|device| {
        device.access == replicant_client::domain::AccessScope::Owned
            && (device_location(device) == Some(belt) || device.tags.contains(&expected_site_tag))
            && (matches!(
                device_type(device),
                Some(
                    MINING_CONTROLLER
                        | MINING_DRONE
                        | SURVEY_CONTROLLER
                        | SURVEY_DRONE
                        | MAINTENANCE_DRONE
                )
            ) || (device.device_type.is_none() && device.tags.contains(&expected_site_tag)))
    }) {
        let field = if device.device_type.is_none() {
            Some("device_type")
        } else if device.location.is_none() {
            Some("location")
        } else if device.status.is_none() {
            Some("status")
        } else if device_type(device) == Some(MAINTENANCE_DRONE)
            && device.operational_capacity.is_none()
        {
            Some("operational_capacity")
        } else {
            None
        };
        if let Some(field) = field {
            mutation_safe = false;
            issues.push(MiningSiteIssue::EvidenceIncomplete {
                device: device.key.id.as_str().to_owned(),
                field,
            });
        }
    }
    let mining_drones = select_site_children(
        devices,
        &at_belt,
        mining_controller.as_deref(),
        MINING_CONTROLLER,
        MINING_DRONE,
        4,
        &expected_site_tag,
        &mut repairs,
        &mut issues,
        &mut mutation_safe,
    );
    let survey_drones = select_site_children(
        devices,
        &at_belt,
        survey_controller.as_deref(),
        SURVEY_CONTROLLER,
        SURVEY_DRONE,
        2,
        &expected_site_tag,
        &mut repairs,
        &mut issues,
        &mut mutation_safe,
    );
    let maintenance = at_belt
        .iter()
        .filter(|device| device_type(device) == Some(MAINTENANCE_DRONE))
        .filter(|device| site_asset_usable(device))
        .filter(|device| !has_conflicting_site_commitment(device, &expected_site_tag))
        .filter(|device| maintenance_patrol_capability(device) != EvidenceState::Absent)
        .filter(|device| {
            !device
                .tags
                .iter()
                .any(|tag| tag == &replicant_mining_planner::role_tag(HUB_MAINTENANCE_ROLE))
        })
        .min_by(|left, right| {
            (!left.tags.contains(&expected_site_tag))
                .cmp(&!right.tags.contains(&expected_site_tag))
                .then_with(|| {
                    (!has_directive(left, "patrol")).cmp(&!has_directive(right, "patrol"))
                })
                .then_with(|| left.key.id.as_str().cmp(right.key.id.as_str()))
        })
        .map(|device| device.key.id.as_str().to_owned());
    let system_ward = devices
        .iter()
        .filter(|device| device_type(device) == Some(SYSTEM_WARD))
        .filter(|device| device_is_in_system(device, system))
        .min_by_key(|device| device.key.id.as_str())
        .map(|device| device.key.id.as_str().to_owned());
    let assets = SiteAssets {
        mining_controller,
        mining_drones,
        survey_controller,
        survey_drones,
        maintenance_drone: maintenance,
        system_ward,
    };
    let mining_adoption_correct = adoption_correct(
        devices,
        assets.mining_controller.as_deref(),
        &assets.mining_drones,
    );
    let survey_adoption_correct = adoption_correct(
        devices,
        assets.survey_controller.as_deref(),
        &assets.survey_drones,
    );
    audit_controller_health(
        devices,
        assets.mining_controller.as_deref(),
        "deplete_smallest",
        &mut repairs,
        &mut issues,
        &mut mutation_safe,
    );
    audit_controller_health(
        devices,
        assets.survey_controller.as_deref(),
        "belt_search",
        &mut repairs,
        &mut issues,
        &mut mutation_safe,
    );
    if let Some(maintenance) = assets
        .maintenance_drone
        .as_deref()
        .and_then(|code| find_device(devices, code))
        && !has_directive(maintenance, "patrol")
    {
        if site_directive_evidence(maintenance, "patrol") == EvidenceState::Unknown {
            mutation_safe = false;
            issues.push(MiningSiteIssue::EvidenceIncomplete {
                device: maintenance.key.id.as_str().to_owned(),
                field: "active_directive",
            });
        } else if maintenance_patrol_capability(maintenance) == EvidenceState::Unknown {
            mutation_safe = false;
            issues.push(MiningSiteIssue::EvidenceIncomplete {
                device: maintenance.key.id.as_str().to_owned(),
                field: "patrol_capability",
            });
        } else {
            repairs.push(MiningSiteRepair::SetDirective {
                device: maintenance.key.id.as_str().to_owned(),
                directive: "patrol",
            });
        }
    }
    let maintenance_rotation_due = assets
        .maintenance_drone
        .as_deref()
        .and_then(|code| find_device(devices, code))
        .is_some_and(maintenance_rotation_due);
    let shortages = shortages(&mining_site_requirements(), &assets.counts());
    for (device_type_name, count) in &shortages {
        issues.push(MiningSiteIssue::MissingDevice {
            device_type: device_type_name.clone(),
            count: *count,
        });
        for device in at_belt
            .iter()
            .filter(|device| device_type(device) == Some(device_type_name.as_str()))
        {
            if !site_asset_usable(device) {
                issues.push(MiningSiteIssue::UnusableDevice {
                    device: device.key.id.as_str().to_owned(),
                    device_type: device_type_name.clone(),
                });
            } else if has_conflicting_site_commitment(device, &expected_site_tag) {
                issues.push(MiningSiteIssue::CommittedElsewhere {
                    device: device.key.id.as_str().to_owned(),
                    device_type: device_type_name.clone(),
                });
            }
        }
    }
    // Protection is intentionally outside operational readiness: the Director
    // backfills or relocates System Wards after the productive stack is online.
    let operational = shortages.is_empty()
        && mutation_safe
        && assets
            .mining_controller
            .as_deref()
            .and_then(|code| find_device(devices, code))
            .is_some_and(|device| {
                has_directive(device, "deplete_smallest")
                    && device
                        .status
                        .as_ref()
                        .is_some_and(|status| status.as_str() == "coordinating")
            })
        && assets.mining_drones.len() >= 4
        && assets
            .survey_controller
            .as_deref()
            .and_then(|code| find_device(devices, code))
            .is_some_and(|device| {
                has_directive(device, "belt_search")
                    && device
                        .status
                        .as_ref()
                        .is_some_and(|status| status.as_str() == "coordinating")
            })
        && assets.survey_drones.len() >= 2
        && assets
            .maintenance_drone
            .as_deref()
            .and_then(|code| find_device(devices, code))
            .is_some_and(|device| has_directive(device, "patrol"))
        && mining_adoption_correct
        && survey_adoption_correct;
    let normal_repair_needed = !shortages.is_empty() || !repairs.is_empty();
    SiteAudit {
        assets,
        operational,
        mining_adoption_correct,
        survey_adoption_correct,
        normal_repair_needed,
        maintenance_rotation_due,
        mutation_safe,
        repairs,
        issues,
        shortages,
    }
}

fn site_shortages(audit: &SiteAudit) -> QuantityMap {
    // System Wards harden a mining system, but they are deliberately not part
    // of the initial mining payload. Getting the controller/drone stack online
    // produces resources sooner; the Director can backfill or relocate a ward
    // after the site is operational when protection is available.
    audit.shortages.clone()
}

fn device_is_in_system(device: &Device, system: &str) -> bool {
    device_location(device).is_some_and(|location| location_is_in_system(location, system))
}

fn select_controller(
    devices: &[&Device],
    controller_type: &str,
    child_type: &str,
    expected_site_tag: &str,
) -> Option<String> {
    devices
        .iter()
        .filter(|device| device_type(device) == Some(controller_type))
        .filter(|device| site_asset_usable(device))
        .filter(|device| !has_conflicting_site_commitment(device, expected_site_tag))
        .max_by(|left, right| {
            let left_site_tagged = left.tags.iter().any(|tag| tag == expected_site_tag);
            let right_site_tagged = right.tags.iter().any(|tag| tag == expected_site_tag);
            let left_count = devices
                .iter()
                .filter(|device| {
                    device_type(device) == Some(child_type)
                        && controller_code(device) == Some(left.key.id.as_str())
                })
                .count();
            let right_count = devices
                .iter()
                .filter(|device| {
                    device_type(device) == Some(child_type)
                        && controller_code(device) == Some(right.key.id.as_str())
                })
                .count();
            left_site_tagged
                .cmp(&right_site_tagged)
                .then_with(|| left_count.cmp(&right_count))
                .then_with(|| right.key.id.as_str().cmp(left.key.id.as_str()))
        })
        .map(|device| device.key.id.as_str().to_owned())
}

fn select_site_children(
    devices: &[Device],
    at_belt: &[&Device],
    controller: Option<&str>,
    controller_type_name: &str,
    child_type: &str,
    required: usize,
    expected_site_tag: &str,
    repairs: &mut Vec<MiningSiteRepair>,
    issues: &mut Vec<MiningSiteIssue>,
    mutation_safe: &mut bool,
) -> Vec<String> {
    let Some(controller) = controller else {
        return Vec::new();
    };
    let mut selected = at_belt
        .iter()
        .filter(|device| device_type(device) == Some(child_type))
        .filter(|device| site_asset_usable(device))
        .filter(|device| controller_code(device) == Some(controller))
        .map(|device| device.key.id.as_str().to_owned())
        .collect::<Vec<_>>();
    selected.sort();
    selected.truncate(required);

    let mut wrong_controller = at_belt
        .iter()
        .filter(|device| device_type(device) == Some(child_type))
        .filter(|device| site_asset_usable(device))
        .filter(|device| {
            controller_code(device).is_some_and(|actual| actual != controller)
                && safely_reassignable_child(
                    devices,
                    device,
                    controller_type_name,
                    expected_site_tag,
                )
        })
        .map(|device| device.key.id.as_str().to_owned())
        .collect::<Vec<_>>();
    wrong_controller.sort();
    for code in wrong_controller {
        if selected.len() >= required {
            break;
        }
        if selected.contains(&code) {
            continue;
        }
        repairs.push(MiningSiteRepair::Adopt {
            device: code.clone(),
            controller: controller.to_owned(),
        });
        selected.push(code);
    }

    let mut free = at_belt
        .iter()
        .filter(|device| device_type(device) == Some(child_type))
        .filter(|device| site_asset_usable(device))
        .filter(|device| controller_code(device).is_none())
        .filter(|device| !has_conflicting_site_commitment(device, expected_site_tag))
        .map(|device| device.key.id.as_str().to_owned())
        .collect::<Vec<_>>();
    free.sort();
    for code in free {
        if selected.len() >= required {
            break;
        }
        if selected.contains(&code) {
            continue;
        }
        if devices.iter().any(|parent| {
            parent
                .relationships
                .controlled_devices
                .iter()
                .any(|child| child.id.as_str() == code)
        }) {
            *mutation_safe = false;
            issues.push(MiningSiteIssue::EvidenceIncomplete {
                device: code.clone(),
                field: "controller_relationship",
            });
            selected.push(code);
            continue;
        }
        repairs.push(MiningSiteRepair::Adopt {
            device: code.clone(),
            controller: controller.to_owned(),
        });
        selected.push(code);
    }

    let mut uncertain_relationship = at_belt
        .iter()
        .filter(|device| device_type(device) == Some(child_type))
        .filter(|device| site_asset_usable(device))
        .filter(|device| {
            controller_code(device).is_some_and(|actual| actual != controller)
                && !has_conflicting_site_commitment(device, expected_site_tag)
                && !safely_reassignable_child(
                    devices,
                    device,
                    controller_type_name,
                    expected_site_tag,
                )
                && relationship_reuse_is_uncertain(
                    devices,
                    device,
                    controller_type_name,
                    expected_site_tag,
                )
        })
        .map(|device| device.key.id.as_str().to_owned())
        .collect::<Vec<_>>();
    uncertain_relationship.sort();
    for code in uncertain_relationship {
        if selected.len() >= required {
            break;
        }
        if selected.contains(&code) {
            continue;
        }
        *mutation_safe = false;
        issues.push(MiningSiteIssue::EvidenceIncomplete {
            device: code.clone(),
            field: "controller_relationship",
        });
        selected.push(code);
    }
    selected.sort();
    selected.dedup();
    selected
}

fn safely_reassignable_child(
    devices: &[Device],
    device: &Device,
    controller_type_name: &str,
    expected_site_tag: &str,
) -> bool {
    if has_conflicting_site_commitment(device, expected_site_tag) {
        return false;
    }
    let Some(actual_controller) = controller_code(device) else {
        return false;
    };
    let Some(controller) = find_device(devices, actual_controller) else {
        return false;
    };
    if device_type(controller) != Some(controller_type_name)
        || device_location(controller) != device_location(device)
        || !site_asset_usable(controller)
        || has_conflicting_site_commitment(controller, expected_site_tag)
    {
        return false;
    }
    device.tags.iter().any(|tag| tag == expected_site_tag)
        || controller.tags.iter().any(|tag| tag == expected_site_tag)
}

fn relationship_reuse_is_uncertain(
    devices: &[Device],
    device: &Device,
    controller_type_name: &str,
    expected_site_tag: &str,
) -> bool {
    let Some(actual_controller) = controller_code(device) else {
        return false;
    };
    let Some(controller) = find_device(devices, actual_controller) else {
        return true;
    };
    if has_conflicting_site_commitment(controller, expected_site_tag) {
        return false;
    }
    device_type(controller) != Some(controller_type_name)
        || device_location(controller) != device_location(device)
        || !site_asset_usable(controller)
        || (!device.tags.iter().any(|tag| tag == expected_site_tag)
            && !controller.tags.iter().any(|tag| tag == expected_site_tag))
}

fn adoption_correct(devices: &[Device], controller: Option<&str>, selected: &[String]) -> bool {
    let Some(controller) = controller else {
        return false;
    };
    selected.iter().all(|code| {
        find_device(devices, code).is_some_and(|device| controller_code(device) == Some(controller))
    })
}

/// Configuration evidence, not merely the absence of a matching directive.
/// The contract defines `idle` as deployed with no active task.
pub(crate) fn site_directive_evidence(device: &Device, expected: &str) -> EvidenceState {
    match device.active_directive.as_ref() {
        Some(active) => match active.directive.as_ref() {
            Some(directive) if directive.as_str() == expected => EvidenceState::Present,
            Some(_) => EvidenceState::Absent,
            None => EvidenceState::Unknown,
        },
        None if device
            .status
            .as_ref()
            .is_some_and(|status| status.as_str() == "idle") =>
        {
            EvidenceState::Absent
        }
        None => EvidenceState::Unknown,
    }
}

fn audit_controller_health(
    devices: &[Device],
    controller: Option<&str>,
    expected_directive: &'static str,
    repairs: &mut Vec<MiningSiteRepair>,
    issues: &mut Vec<MiningSiteIssue>,
    mutation_safe: &mut bool,
) {
    let Some(controller) = controller.and_then(|code| find_device(devices, code)) else {
        return;
    };
    match site_directive_evidence(controller, expected_directive) {
        EvidenceState::Present => {}
        EvidenceState::Absent => repairs.push(MiningSiteRepair::SetDirective {
            device: controller.key.id.as_str().to_owned(),
            directive: expected_directive,
        }),
        EvidenceState::Unknown => {
            *mutation_safe = false;
            issues.push(MiningSiteIssue::EvidenceIncomplete {
                device: controller.key.id.as_str().to_owned(),
                field: "active_directive",
            });
        }
    }
    match controller.status.as_ref().map(DeviceStatus::as_str) {
        Some("coordinating") => {}
        Some(_) => repairs.push(MiningSiteRepair::LaunchController {
            device: controller.key.id.as_str().to_owned(),
        }),
        None => {
            *mutation_safe = false;
            issues.push(MiningSiteIssue::EvidenceIncomplete {
                device: controller.key.id.as_str().to_owned(),
                field: "status",
            });
        }
    }
}

fn site_asset_usable(device: &Device) -> bool {
    device.access == replicant_client::domain::AccessScope::Owned
        && device.relationships.attached_to.is_none()
        && device.relationships.stowed_in.is_none()
        && device.travel.is_none()
        && !device
            .status
            .as_ref()
            .is_some_and(|status| matches!(status.as_str(), "offline" | "deactivated"))
}

fn has_conflicting_site_commitment(device: &Device, expected_site_tag: &str) -> bool {
    let has_expected_site_tag = device.tags.iter().any(|tag| tag == expected_site_tag);
    device.tags.iter().any(|tag| {
        (!has_expected_site_tag && tag.starts_with("mine-s:") && tag != expected_site_tag)
            || tag.starts_with("evt-")
            || tag.starts_with("evt_")
            || tag.starts_with("relay-")
            || (!has_expected_site_tag
                && (tag.starts_with("mine-m:") || tag.starts_with("mine-b:")))
    })
}

fn maintenance_patrol_capability(device: &Device) -> EvidenceState {
    if has_directive(device, "patrol")
        || device
            .available_directives
            .iter()
            .any(|directive| directive.as_str() == "patrol")
    {
        return EvidenceState::Present;
    }
    if device.available_directives.is_empty() {
        EvidenceState::Unknown
    } else {
        EvidenceState::Absent
    }
}

fn documented_transport_status(status: &str) -> bool {
    const DOCUMENTED: [&str; 27] = [
        "stowed",
        "idle",
        "travelling",
        "cruising",
        "surging",
        "recalling",
        "recall_waiting",
        "decommissioning",
        "collecting",
        "depositing",
        "waiting_for_surge_plate",
        "prospecting",
        "tracking",
        "scanning",
        "monitoring",
        "printing",
        "waiting_for_resources",
        "repairing",
        "diverting",
        "patrolling",
        "coordinating",
        "relaying",
        "inactive",
        "compacting",
        "compacted",
        "unfurling",
        "mining",
    ];
    DOCUMENTED.iter().any(|documented| {
        status == *documented
            || status
                .strip_prefix(documented)
                .is_some_and(|suffix| suffix.starts_with(" ("))
    })
}

fn transport_status_state(device: &Device) -> EvidenceState {
    const USABLE: [&str; 9] = [
        "idle",
        "travelling",
        "cruising",
        "surging",
        "recalling",
        "recall_waiting",
        "collecting",
        "depositing",
        "waiting_for_surge_plate",
    ];
    match device.status.as_ref().map(DeviceStatus::as_str) {
        None => EvidenceState::Unknown,
        Some(status) if USABLE.contains(&status) => EvidenceState::Present,
        Some(status) if documented_transport_status(status) => EvidenceState::Absent,
        Some(_) => EvidenceState::Unknown,
    }
}

fn transport_controller_status_state(device: &Device) -> EvidenceState {
    match device.status.as_ref().map(DeviceStatus::as_str) {
        None => EvidenceState::Unknown,
        Some("coordinating") => EvidenceState::Present,
        Some(status) if documented_transport_status(status) => EvidenceState::Absent,
        Some(_) => EvidenceState::Unknown,
    }
}

/// Audits exact owned AMI transport coverage conservatively when device
/// membership may be incomplete.
pub(crate) fn transport_service_present(
    devices: &[Device],
    system: &str,
    collect: &str,
    deliver: &str,
) -> TransportServiceAudit {
    transport_service_present_with_authority(devices, system, collect, deliver, false)
}

/// Audits exact owned AMI transport coverage after an authoritative freighter census.
pub(crate) fn transport_service_present_with_authority(
    devices: &[Device],
    system: &str,
    collect: &str,
    deliver: &str,
    membership_authoritative: bool,
) -> TransportServiceAudit {
    if collect == deliver {
        return TransportServiceAudit {
            state: EvidenceState::Present,
            collect: collect.to_owned(),
            deliver: deliver.to_owned(),
            controller: None,
            controller_operational: EvidenceState::Present,
            controller_directive: EvidenceState::Present,
            controller_configuration: EvidenceState::Present,
            adopted_freighters: Vec::new(),
            usable_freighters: Vec::new(),
            unknown_freighters: Vec::new(),
            unusable_freighters: Vec::new(),
            freighter: None,
        };
    }
    let expected = if location_is_in_system(deliver, system) {
        "shuttle"
    } else {
        "ferry"
    };
    let expected_site_tag = site_tag(system);
    let expected_role_tag = replicant_mining_planner::role_tag("transport-controller");
    let mut candidates = devices
        .iter()
        .filter(|device| {
            device.access == replicant_client::domain::AccessScope::Owned
                && (device_type(device) == Some(TRANSPORT_CONTROLLER)
                    || device.device_type.is_none())
        })
        .filter_map(|controller| {
            let active = controller.active_directive.as_ref();
            let config_matches = active
                .and_then(|active| active.details.get("config"))
                .and_then(Value::as_object)
                .is_some_and(|config| {
                    config.get("collect").and_then(Value::as_str) == Some(collect)
                        && config.get("deliver").and_then(Value::as_str) == Some(deliver)
                });
            let tagged = controller.tags.iter().any(|tag| tag == &expected_site_tag)
                && controller.tags.iter().any(|tag| tag == &expected_role_tag);
            (config_matches || tagged).then_some((
                if config_matches { 2_u8 } else { 0 } + if tagged { 1 } else { 0 },
                controller,
            ))
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        right
            .0
            .cmp(&left.0)
            .then_with(|| left.1.key.id.cmp(&right.1.key.id))
    });

    let Some((best_score, controller)) = candidates.first().copied() else {
        return TransportServiceAudit {
            state: EvidenceState::Absent,
            collect: collect.to_owned(),
            deliver: deliver.to_owned(),
            controller: None,
            controller_operational: EvidenceState::Absent,
            controller_directive: EvidenceState::Absent,
            controller_configuration: EvidenceState::Absent,
            adopted_freighters: Vec::new(),
            usable_freighters: Vec::new(),
            unknown_freighters: Vec::new(),
            unusable_freighters: Vec::new(),
            freighter: None,
        };
    };
    if candidates
        .get(1)
        .is_some_and(|candidate| candidate.0 == best_score)
    {
        return TransportServiceAudit {
            state: EvidenceState::Unknown,
            collect: collect.to_owned(),
            deliver: deliver.to_owned(),
            controller: None,
            controller_operational: EvidenceState::Unknown,
            controller_directive: EvidenceState::Unknown,
            controller_configuration: EvidenceState::Unknown,
            adopted_freighters: Vec::new(),
            usable_freighters: Vec::new(),
            unknown_freighters: Vec::new(),
            unusable_freighters: Vec::new(),
            freighter: None,
        };
    }

    let selected_controller_code = controller.key.id.as_str().to_owned();
    let controller_operational = if controller.device_type.is_none() {
        EvidenceState::Unknown
    } else {
        transport_controller_status_state(controller)
    };
    let (controller_directive, controller_configuration) =
        match controller.active_directive.as_ref() {
            None if controller_operational == EvidenceState::Present => {
                (EvidenceState::Unknown, EvidenceState::Unknown)
            }
            None => (EvidenceState::Absent, EvidenceState::Absent),
            Some(active) => {
                let directive = active.directive.as_ref().map(|value| value.as_str());
                let active_status = active.status.as_deref();
                let config = active.details.get("config").and_then(Value::as_object);
                let directive_state = if directive.is_none() || active_status.is_none() {
                    EvidenceState::Unknown
                } else if directive == Some(expected) && active_status == Some("active") {
                    EvidenceState::Present
                } else {
                    EvidenceState::Absent
                };
                let configuration_state = match config.map(|config| {
                    (
                        config.get("collect").and_then(Value::as_str),
                        config.get("deliver").and_then(Value::as_str),
                    )
                }) {
                    Some((Some(actual_collect), Some(actual_deliver))) => {
                        if actual_collect == collect && actual_deliver == deliver {
                            EvidenceState::Present
                        } else {
                            EvidenceState::Absent
                        }
                    }
                    _ => EvidenceState::Unknown,
                };
                (directive_state, configuration_state)
            }
        };

    let mut adopted_freighters = Vec::new();
    let mut usable_freighters = Vec::new();
    let mut unknown_freighters = Vec::new();
    let mut unusable_freighters = Vec::new();
    for freighter in devices.iter().filter(|device| {
        device.access == replicant_client::domain::AccessScope::Owned
            && device_type(device) == Some(CARGO_FREIGHTER)
            && controller_code(device) == Some(selected_controller_code.as_str())
    }) {
        let code = freighter.key.id.as_str().to_owned();
        adopted_freighters.push(code.clone());
        match transport_status_state(freighter) {
            EvidenceState::Present => usable_freighters.push(code),
            EvidenceState::Unknown => unknown_freighters.push(code),
            EvidenceState::Absent => unusable_freighters.push(code),
        }
    }
    adopted_freighters.sort();
    usable_freighters.sort();
    unknown_freighters.sort();
    unusable_freighters.sort();
    // Both directions must agree before a count can authorize capacity changes.
    // An empty relationship is known zero only after an authoritative census.
    let relationships_incomplete = (!membership_authoritative
        && controller.relationships.controlled_devices.is_empty())
        || controller
            .relationships
            .controlled_devices
            .iter()
            .any(|child| {
                find_device(devices, child.id.as_str()).is_none_or(|device| {
                    controller_code(device) != Some(selected_controller_code.as_str())
                        || device.device_type.is_none()
                })
            })
        || devices.iter().any(|device| {
            controller_code(device) == Some(selected_controller_code.as_str())
                && device.device_type.is_none()
        })
        || adopted_freighters.iter().any(|code| {
            !controller
                .relationships
                .controlled_devices
                .iter()
                .any(|child| child.id.as_str() == code)
        });

    let state = if controller_operational == EvidenceState::Unknown
        || controller_directive == EvidenceState::Unknown
        || controller_configuration == EvidenceState::Unknown
        || !unknown_freighters.is_empty()
        || relationships_incomplete
    {
        EvidenceState::Unknown
    } else if controller_operational == EvidenceState::Present
        && controller_directive == EvidenceState::Present
        && controller_configuration == EvidenceState::Present
        && !usable_freighters.is_empty()
    {
        EvidenceState::Present
    } else {
        EvidenceState::Absent
    };
    let freighter = usable_freighters.first().cloned();
    TransportServiceAudit {
        state,
        collect: collect.to_owned(),
        deliver: deliver.to_owned(),
        controller: Some(selected_controller_code),
        controller_operational,
        controller_directive,
        controller_configuration,
        adopted_freighters,
        usable_freighters,
        unknown_freighters,
        unusable_freighters,
        freighter,
    }
}

fn reusable_counts(
    devices: &[Device],
    hub: &str,
    mission_tag: &str,
    site_devices: bool,
) -> QuantityMap {
    let allowed: BTreeSet<&str> = if site_devices {
        [
            MINING_CONTROLLER,
            MINING_DRONE,
            SURVEY_CONTROLLER,
            SURVEY_DRONE,
            MAINTENANCE_DRONE,
            SYSTEM_WARD,
        ]
        .into_iter()
        .collect()
    } else {
        [TRANSPORT_CONTROLLER, CARGO_FREIGHTER]
            .into_iter()
            .collect()
    };
    let mut counts = QuantityMap::new();
    for device in devices.iter().filter(|device| {
        device_location(device) == Some(hub)
            && device
                .device_type
                .as_ref()
                .is_some_and(|value| allowed.contains(value.as_str()))
            && device
                .status
                .as_ref()
                .is_some_and(|value| value.as_str() == "idle")
            && device.relationships.controller.is_none()
            && device.relationships.attached_to.is_none()
            && device.relationships.stowed_in.is_none()
            && device.travel.is_none()
            && (device.tags.iter().any(|tag| tag == mission_tag) || !has_reservation_tag(device))
    }) {
        if let Some(device_type) = &device.device_type {
            *counts.entry(device_type.as_str().to_owned()).or_default() += 1;
        }
    }
    counts
}

fn has_reservation_tag(device: &Device) -> bool {
    device.tags.iter().any(|tag| {
        tag.starts_with("evt-")
            || tag.starts_with("evt_")
            || tag.starts_with("mine-")
            || tag.starts_with("relay-")
    })
}

fn device_type(device: &Device) -> Option<&str> {
    device.device_type.as_ref().map(DeviceType::as_str)
}

fn device_location(device: &Device) -> Option<&str> {
    device
        .location
        .as_ref()
        .map(|location| location.id.as_str())
}

fn controller_code(device: &Device) -> Option<&str> {
    device
        .relationships
        .controller
        .as_ref()
        .map(|controller| controller.id.as_str())
}

fn has_directive(device: &Device, directive_name: &str) -> bool {
    device
        .active_directive
        .as_ref()
        .and_then(|active| active.directive.as_ref())
        .is_some_and(|directive| directive.as_str() == directive_name)
}

fn find_device<'a>(devices: &'a [Device], code: &str) -> Option<&'a Device> {
    devices.iter().find(|device| device.key.id.as_str() == code)
}

async fn fetch_blueprints(client: &Client) -> AnyResult<BTreeMap<String, BlueprintSpec>> {
    Ok(client
        .raw()
        .blueprints()
        .list()
        .await?
        .value
        .blueprints
        .into_iter()
        .filter_map(|blueprint| {
            let device_type = blueprint.device_type?;
            Some((
                device_type.clone(),
                BlueprintSpec {
                    device_type,
                    print_time_seconds: blueprint.print_time.unwrap_or(0.0),
                    resources: blueprint.resources.unwrap_or_default(),
                    components: blueprint.components.unwrap_or_default(),
                },
            ))
        })
        .collect())
}

async fn factory_workloads(
    client: &Client,
    blueprints: &BTreeMap<String, BlueprintSpec>,
    hub: &str,
) -> AnyResult<Vec<FactoryWorkload>> {
    let mut factories = discover_factories(client, hub, blueprints)
        .await?
        .into_iter()
        .map(|factory| factory.workload())
        .collect::<Vec<_>>();
    factories.sort_by(|left, right| left.code.cmp(&right.code));
    Ok(factories)
}

fn save_plan(path: &Path, mission: &MiningMission) -> AnyResult<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    let file = File::create(&temporary)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, mission)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::rename(&temporary, path)?;
    Ok(())
}

/// Loads a durable mining expansion checkpoint from disk.
pub fn load_expansion(path: &Path) -> AnyResult<MiningMission> {
    let mut mission: MiningMission = serde_json::from_slice(&fs::read(path)?)?;
    if mission.version != PLAN_VERSION {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "mission version {} is unsupported; expected {PLAN_VERSION}",
                mission.version
            ),
        ));
    }
    migrate_mission_tag_metadata(&mut mission);
    Ok(mission)
}

/// Inputs for invoking the durable mining workflow from another automation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MiningExpansionRequest {
    /// Systems whose best discovered belts should receive mining setups.
    pub systems: Vec<String>,
    /// Owned replicant name or code responsible for the assets.
    pub replicant: String,
    /// Manufacturing hub and route delivery location.
    pub hub: String,
    /// Exact AMI routes to provision. Systems without an explicit route use
    /// the deterministic preferred-belt selector.
    #[serde(default)]
    pub transport_routes: Vec<AmiTransportRouteIntent>,
    /// Child mission file used for restart-safe reconciliation.
    pub mission_file: PathBuf,
    /// Maximum wait for one manufacturing or travel stage.
    pub wait_timeout: Duration,
    /// Maximum number of carrier deployments in flight at once.
    pub max_concurrency: usize,
}

/// Result of a reusable mining expansion run.
#[derive(Clone, Debug, Serialize)]
pub struct MiningExpansionReport {
    /// Systems represented by the completed child mission.
    pub systems: Vec<String>,
    /// Belt locations made operational.
    pub belts: Vec<String>,
    /// Final durable state, including per-site and per-route checkpoints.
    pub mission: MiningMission,
}

/// Creates and persists a mining expansion plan without executing it.
pub async fn plan_expansion(
    client: &Client,
    request: &MiningExpansionRequest,
    replace_existing: bool,
) -> AnyResult<MiningMission> {
    validate_request(request)?;
    let config = Config {
        systems: request.systems.clone(),
        replicant: Some(request.replicant.clone()),
        hub: request.hub.to_ascii_uppercase(),
        transport_routes: request.transport_routes.clone(),
        plan_path: request.mission_file.clone(),
        replace_plan: replace_existing,
        wait_timeout: request.wait_timeout,
        max_concurrency: request.max_concurrency,
        claims: None,
    };
    create_plan(client, &config).await
}

/// Creates a mining expansion plan from the daemon's committed managed state.
///
/// Durable Director workflows use this path so both planning entry points
/// consume the same projection-first planner.
pub(crate) async fn plan_expansion_from_managed_state(
    client: &Client,
    request: &MiningExpansionRequest,
    replace_existing: bool,
) -> AnyResult<MiningMission> {
    validate_request(request)?;
    let config = Config {
        systems: request.systems.clone(),
        replicant: Some(request.replicant.clone()),
        hub: request.hub.to_ascii_uppercase(),
        transport_routes: request.transport_routes.clone(),
        plan_path: request.mission_file.clone(),
        replace_plan: replace_existing,
        wait_timeout: request.wait_timeout,
        max_concurrency: request.max_concurrency,
        claims: None,
    };
    create_plan(client, &config).await
}

/// Creates or resumes a mining expansion using an already-running managed client.
pub async fn execute_expansion(
    client: &Client,
    request: &MiningExpansionRequest,
    claims: Option<MiningWorkflowClaims>,
) -> AnyResult<MiningExpansionReport> {
    validate_request(request)?;
    if request.systems.is_empty() && !request.mission_file.exists() {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "a new mining child mission requires at least one system",
        ));
    }
    if !request.mission_file.exists() {
        plan_expansion(client, request, false).await?;
    }

    let execution = Config {
        systems: Vec::new(),
        replicant: None,
        hub: request.hub.to_ascii_uppercase(),
        transport_routes: request.transport_routes.clone(),
        plan_path: request.mission_file.clone(),
        replace_plan: false,
        wait_timeout: request.wait_timeout,
        max_concurrency: request.max_concurrency,
        claims,
    };
    let _lock = MissionLock::acquire(&request.mission_file)?;
    let mut mission = load_expansion(&request.mission_file)?;
    executor::execute(client, &execution, &mut mission).await?;
    let report = MiningExpansionReport {
        systems: mission
            .sites
            .iter()
            .map(|site| site.system.clone())
            .collect(),
        belts: mission.sites.iter().map(|site| site.belt.clone()).collect(),
        mission,
    };
    Ok(report)
}

fn validate_request(request: &MiningExpansionRequest) -> AnyResult<()> {
    if request.replicant.trim().is_empty() || request.hub.trim().is_empty() {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "mining expansion requires a replicant and hub",
        ));
    }
    if !(1..=32).contains(&request.max_concurrency) {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "max concurrency must be between 1 and 32",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_client::{
        SecretString,
        domain::{
            AccessScope, ActiveDeviceDirective, DeviceDirective, DeviceId, DeviceKey,
            DeviceRelationships, DeviceStatus, GalacticPosition, StarKey,
        },
        raw::Url,
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    #[test]
    fn mining_mission_tags_are_system_scoped_and_bounded() {
        assert_eq!(mining_mission_tag("SCEPTURUM-BELT-1"), "mine-m:scepturum");
        let long =
            mining_mission_tag("A-SYSTEM-NAME-THAT-IS-WELL-PAST-THE-DEVICE-TAG-LIMIT-BELT-1");
        assert!(long.chars().count() <= MAX_DEVICE_TAG_CHARS);
    }

    #[test]
    fn mining_site_delivery_requires_attachment_capacity() {
        let site = SiteMission {
            system: "PHASYRIS".into(),
            belt: "PHASYRIS-BELT-1".into(),
            density: "dense".into(),
            tag: "mine-site:phasyris".into(),
            phase: SitePhase::Planned,
            assets: SiteAssets::default(),
            missing: [
                (MINING_CONTROLLER.into(), 1),
                (MINING_DRONE.into(), 8),
                (SYSTEM_WARD.into(), 1),
            ]
            .into_iter()
            .collect(),
            carrier: None,
        };
        let requirements = mining_site_item_requirements("delta", &site);
        let attachment = requirements
            .iter()
            .find(|requirement| requirement.key == "attach")
            .expect("attachment-capacity requirement");

        assert_eq!(attachment.kind, "attach");
        assert_eq!(attachment.quantity, 9);
        assert!(
            requirements
                .iter()
                .all(|requirement| requirement.key != "stow")
        );

        let mut ward_only = site;
        ward_only.missing = [
            (SYSTEM_WARD.into(), 1),
            (MINING_DRONE.into(), 0),
            ("unknown_device".into(), 3),
        ]
        .into_iter()
        .collect();
        let ward_requirements = mining_site_item_requirements("delta", &ward_only);
        assert!(
            ward_requirements
                .iter()
                .all(|requirement| requirement.key != "carrier")
        );
        assert!(
            ward_requirements
                .iter()
                .all(|requirement| requirement.key != "attach")
        );
    }

    #[test]
    fn mining_transport_routes_do_not_require_stow_capacity() {
        let route = RouteMission {
            system: "SOL".into(),
            belt: "SOL-BELT-1".into(),
            tag: site_tag("SOL"),
            phase: RoutePhase::Planned,
            controller: None,
            freighter: None,
            additional_freighters: Vec::new(),
            desired_freighters: 1,
        };
        let requirements = mining_route_item_requirements("delta", &route);
        let keys = requirements
            .iter()
            .map(|requirement| requirement.key.as_str())
            .collect::<Vec<_>>();

        assert_eq!(keys, ["worker", "transport_controller", "freighters"]);
        assert!(
            requirements
                .iter()
                .all(|requirement| requirement.kind != "stow")
        );
    }

    #[tokio::test]
    async fn planning_uses_projection_then_exact_location_without_full_sync() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/replicants/WORKER"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "replicant_code": "WORKER",
                "status": "active"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/blueprints"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"blueprints": []})),
            )
            .mount(&server)
            .await;
        let client = Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .authentication_token(SecretString::from("test-token"))
            .in_memory()
            .startup_policy(replicant_client::StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only startup");
        client
            .replicants()
            .get_owned("WORKER")
            .await
            .expect("seed committed worker projection");
        let mission_file = std::env::temp_dir().join(format!(
            "replicant-mining-no-full-sync-{}.json",
            uuid::Uuid::new_v4()
        ));
        let request = MiningExpansionRequest {
            systems: vec!["SOL".into()],
            replicant: "WORKER".into(),
            hub: "SOL-L4".into(),
            transport_routes: Vec::new(),
            mission_file,
            wait_timeout: Duration::from_secs(1),
            max_concurrency: 1,
        };

        assert!(plan_expansion(&client, &request, false).await.is_err());
        let requests = server.received_requests().await.expect("received requests");
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].url.path(), "/v1/replicants/WORKER");
        assert_eq!(requests[1].url.path(), "/v1/blueprints");
        assert_eq!(requests[2].url.path(), "/v1/locations/SOL");
        client.close().await.expect("close");
    }

    fn device(code: &str, device_type_name: &str, location: &str) -> Device {
        Device {
            key: DeviceKey::live(DeviceId::from(code)),
            device_type: Some(DeviceType::from(device_type_name)),
            status: Some(DeviceStatus::from("idle")),
            location: Some(replicant_client::domain::LocationKey::live(location.into())),
            deployed_at: None,
            in_control_range: None,
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            settings: Default::default(),
            relationships: DeviceRelationships::default(),
            cargo: Default::default(),
            cargo_capacity: None,
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            runtime: Default::default(),
            access: AccessScope::Owned,
        }
    }

    fn directive(device: &mut Device, name: &str) {
        device.active_directive = Some(ActiveDeviceDirective {
            directive: Some(DeviceDirective::from(name)),
            status: Some("active".into()),
            details: BTreeMap::new(),
        });
    }

    fn adopt(device: &mut Device, controller: &str) {
        device.relationships.controller = Some(DeviceKey::live(DeviceId::from(controller)));
    }

    fn healthy_transport_controller(
        code: &str,
        system: &str,
        belt: &str,
        hub: &str,
        freighters: &[&str],
    ) -> Device {
        let mut controller = device(code, TRANSPORT_CONTROLLER, belt);
        controller.tags = vec![site_tag(system), role_tag("transport-controller")];
        controller.status = Some(DeviceStatus::from("coordinating"));
        controller.active_directive = Some(ActiveDeviceDirective {
            directive: Some(DeviceDirective::from(
                if location_is_in_system(hub, system) {
                    "shuttle"
                } else {
                    "ferry"
                },
            )),
            status: Some("active".into()),
            details: [(
                "config".into(),
                serde_json::json!({"collect": belt, "deliver": hub}),
            )]
            .into_iter()
            .collect(),
        });
        controller.relationships.controlled_devices = freighters
            .iter()
            .map(|code| DeviceKey::live(DeviceId::from(*code)))
            .collect();
        controller
    }

    fn transport_freighter(code: &str, controller: &str, location: &str, status: &str) -> Device {
        let mut freighter = device(code, CARGO_FREIGHTER, location);
        freighter.status = Some(DeviceStatus::from(status));
        adopt(&mut freighter, controller);
        freighter
    }

    fn healthy_site_devices(system: &str, belt: &str) -> Vec<Device> {
        let stable_site_tag = site_tag(system);
        let mut mining_controller = device("MC", MINING_CONTROLLER, belt);
        mining_controller.tags = vec![stable_site_tag.clone(), role_tag("mining-controller")];
        mining_controller.status = Some(DeviceStatus::from("coordinating"));
        directive(&mut mining_controller, "deplete_smallest");

        let mut survey_controller = device("SC", SURVEY_CONTROLLER, belt);
        survey_controller.tags = vec![stable_site_tag.clone(), role_tag("survey-controller")];
        survey_controller.status = Some(DeviceStatus::from("coordinating"));
        directive(&mut survey_controller, "belt_search");

        let mut maintenance = device("MD", MAINTENANCE_DRONE, belt);
        maintenance.operational_capacity =
            replicant_client::domain::OperationalCapacity::new(100.0);
        maintenance.tags = vec![stable_site_tag.clone(), role_tag("maintenance")];
        directive(&mut maintenance, "patrol");

        let mut devices = vec![mining_controller, survey_controller, maintenance];
        for index in 0..4 {
            let mut drone = device(&format!("M{index}"), MINING_DRONE, belt);
            drone.tags = vec![stable_site_tag.clone(), role_tag("mining-drone")];
            adopt(&mut drone, "MC");
            devices.push(drone);
        }
        for index in 0..2 {
            let mut drone = device(&format!("S{index}"), SURVEY_DRONE, belt);
            drone.tags = vec![stable_site_tag.clone(), role_tag("survey-drone")];
            adopt(&mut drone, "SC");
            devices.push(drone);
        }
        devices
    }

    fn star(name: &str, x: f64, y: f64, z: f64) -> Star {
        Star {
            key: StarKey::live(replicant_client::StarId::from(name)),
            name: None,
            spectral_type: None,
            entry_point: None,
            position: Some(GalacticPosition { x, y, z }),
            has_hub: None,
            has_ward: None,
            knowledge_observed: true,
            explored: None,
            has_life: None,
            region: None,
        }
    }

    #[test]
    fn mining_sites_are_ordered_nearest_to_the_hub() {
        let catalogue = vec![
            star("HUB", 0.0, 0.0, 0.0),
            star("NEAR", 1.0, 1.0, 0.0),
            star("FAR", 8.0, 0.0, 0.0),
        ];
        let mut systems = vec!["UNKNOWN".into(), "FAR".into(), "NEAR".into()];

        sort_systems_by_hub_distance(&mut systems, "HUB-BELT-1", &catalogue);

        assert_eq!(systems, ["NEAR", "FAR", "UNKNOWN"]);
    }

    #[test]
    fn same_location_needs_no_transport_service() {
        assert_eq!(
            transport_service_present(&[], "BETA", "BETA-BELT-1", "BETA-BELT-1").state,
            EvidenceState::Present
        );
        assert_eq!(
            transport_service_present(&[], "BETA", "BETA-BELT-2", "BETA-BELT-1").state,
            EvidenceState::Absent
        );
    }

    #[test]
    fn complete_site_is_recognized_from_child_relationships() {
        let belt = "SOL-BELT-1";
        let devices = healthy_site_devices("SOL", belt);
        let audit = audit_site(&devices, "SOL", belt);
        assert!(audit.operational);
        assert!(audit.mining_adoption_correct);
        assert!(audit.survey_adoption_correct);
        assert!(!audit.normal_repair_needed);
        assert!(!audit.maintenance_rotation_due);
        assert!(audit.mutation_safe);
        assert!(audit.repairs.is_empty());
        assert!(site_shortages(&audit).is_empty());
    }

    #[test]
    fn wrong_local_mining_adoption_is_repaired_before_printing() {
        let belt = "SOL-BELT-1";
        let stable_site_tag = site_tag("SOL");
        let mut devices = healthy_site_devices("SOL", belt);
        let moved = devices
            .iter_mut()
            .find(|device| device.key.id.as_str() == "M3")
            .expect("fourth mining drone");
        adopt(moved, "ZZ-MC");
        let mut wrong_controller = device("ZZ-MC", MINING_CONTROLLER, belt);
        wrong_controller.tags = vec![stable_site_tag, role_tag("mining-controller")];
        devices.push(wrong_controller);

        let audit = audit_site(&devices, "SOL", belt);

        assert_eq!(audit.assets.mining_controller.as_deref(), Some("MC"));
        assert_eq!(audit.assets.mining_drones.len(), 4);
        assert!(!audit.mining_adoption_correct);
        assert!(audit.normal_repair_needed);
        assert!(!site_shortages(&audit).contains_key(MINING_DRONE));
        assert!(audit.repairs.contains(&MiningSiteRepair::Adopt {
            device: "M3".into(),
            controller: "MC".into(),
        }));
    }

    #[test]
    fn wrong_local_survey_adoption_is_repaired_before_printing() {
        let belt = "SOL-BELT-1";
        let stable_site_tag = site_tag("SOL");
        let mut devices = healthy_site_devices("SOL", belt);
        let moved = devices
            .iter_mut()
            .find(|device| device.key.id.as_str() == "S1")
            .expect("second survey drone");
        adopt(moved, "ZZ-SC");
        let mut wrong_controller = device("ZZ-SC", SURVEY_CONTROLLER, belt);
        wrong_controller.tags = vec![stable_site_tag, role_tag("survey-controller")];
        devices.push(wrong_controller);

        let audit = audit_site(&devices, "SOL", belt);

        assert_eq!(audit.assets.survey_controller.as_deref(), Some("SC"));
        assert_eq!(audit.assets.survey_drones.len(), 2);
        assert!(!audit.survey_adoption_correct);
        assert!(audit.normal_repair_needed);
        assert!(!site_shortages(&audit).contains_key(SURVEY_DRONE));
        assert!(audit.repairs.contains(&MiningSiteRepair::Adopt {
            device: "S1".into(),
            controller: "SC".into(),
        }));
    }

    #[test]
    fn ambiguous_wrong_controller_relationship_waits_instead_of_printing() {
        let belt = "SOL-BELT-1";
        let mut devices = healthy_site_devices("SOL", belt);
        let moved = devices
            .iter_mut()
            .find(|device| device.key.id.as_str() == "M3")
            .expect("fourth mining drone");
        adopt(moved, "MISSING-MC");

        let audit = audit_site(&devices, "SOL", belt);

        assert_eq!(audit.assets.mining_drones.len(), 4);
        assert!(!audit.mutation_safe);
        assert!(!site_shortages(&audit).contains_key(MINING_DRONE));
        assert!(audit.issues.iter().any(|issue| matches!(
            issue,
            MiningSiteIssue::EvidenceIncomplete { device, field }
                if device == "M3" && *field == "controller_relationship"
        )));
    }

    #[test]
    fn drone_committed_to_another_site_is_never_stolen() {
        let belt = "SOL-BELT-1";
        let mut devices = healthy_site_devices("SOL", belt);
        let moved = devices
            .iter_mut()
            .find(|device| device.key.id.as_str() == "M3")
            .expect("fourth mining drone");
        moved.tags = vec![site_tag("BETA"), role_tag("mining-drone")];
        adopt(moved, "OTHER-MC");
        let mut other_controller = device("OTHER-MC", MINING_CONTROLLER, belt);
        other_controller.tags = vec![site_tag("BETA"), role_tag("mining-controller")];
        devices.push(other_controller);

        let audit = audit_site(&devices, "SOL", belt);

        assert_eq!(audit.assets.mining_drones.len(), 3);
        assert_eq!(site_shortages(&audit).get(MINING_DRONE), Some(&1));
        assert!(!audit.repairs.iter().any(|repair| matches!(
            repair,
            MiningSiteRepair::Adopt { device, .. } if device == "M3"
        )));
        assert!(audit.issues.iter().any(|issue| matches!(
            issue,
            MiningSiteIssue::CommittedElsewhere { device, .. } if device == "M3"
        )));
    }

    #[test]
    fn drone_reserved_by_another_workflow_is_never_stolen() {
        let belt = "SOL-BELT-1";
        let mut devices = healthy_site_devices("SOL", belt);
        let moved = devices
            .iter_mut()
            .find(|device| device.key.id.as_str() == "M3")
            .expect("fourth mining drone");
        moved.tags = vec!["mine-m:other".into(), role_tag("mining-drone")];
        adopt(moved, "OTHER-MC");
        let mut other_controller = device("OTHER-MC", MINING_CONTROLLER, belt);
        other_controller.tags = vec!["mine-m:other".into(), role_tag("mining-controller")];
        devices.push(other_controller);

        let audit = audit_site(&devices, "SOL", belt);

        assert_eq!(audit.assets.mining_drones.len(), 3);
        assert_eq!(site_shortages(&audit).get(MINING_DRONE), Some(&1));
        assert!(!audit.repairs.iter().any(|repair| matches!(
            repair,
            MiningSiteRepair::Adopt { device, .. } if device == "M3"
        )));
        assert!(audit.issues.iter().any(|issue| matches!(
            issue,
            MiningSiteIssue::CommittedElsewhere { device, .. } if device == "M3"
        )));
    }

    #[test]
    fn wrong_controller_directive_is_repairable_not_missing_hardware() {
        let belt = "SOL-BELT-1";
        let mut devices = healthy_site_devices("SOL", belt);
        directive(&mut devices[0], "largest_first");

        let audit = audit_site(&devices, "SOL", belt);

        assert!(!audit.operational);
        assert!(audit.normal_repair_needed);
        assert!(site_shortages(&audit).is_empty());
        assert!(audit.repairs.contains(&MiningSiteRepair::SetDirective {
            device: "MC".into(),
            directive: "deplete_smallest",
        }));
    }

    #[test]
    fn low_capacity_patrolling_maintenance_is_rotation_due_not_missing() {
        let belt = "SOL-BELT-1";
        let mut devices = healthy_site_devices("SOL", belt);
        devices[2].operational_capacity = replicant_client::domain::OperationalCapacity::new(10.0);

        let audit = audit_site(&devices, "SOL", belt);

        assert!(audit.operational);
        assert!(audit.maintenance_rotation_due);
        assert!(!audit.normal_repair_needed);
        assert_eq!(audit.assets.maintenance_drone.as_deref(), Some("MD"));
        assert!(!site_shortages(&audit).contains_key(MAINTENANCE_DRONE));
    }

    #[test]
    fn maintenance_rotation_threshold_is_strictly_below_thirty_percent() {
        let belt = "SOL-BELT-1";
        let mut devices = healthy_site_devices("SOL", belt);
        devices[2].operational_capacity = replicant_client::domain::OperationalCapacity::new(29.9);

        assert!(audit_site(&devices, "SOL", belt).maintenance_rotation_due);

        devices[2].operational_capacity = replicant_client::domain::OperationalCapacity::new(30.0);
        assert!(!audit_site(&devices, "SOL", belt).maintenance_rotation_due);
    }

    #[test]
    fn maintenance_replacement_ready_threshold_is_ninety_five_percent() {
        let mut drone = device("MD", MAINTENANCE_DRONE, "HUB-BELT-1");
        drone.operational_capacity = replicant_client::domain::OperationalCapacity::new(95.0);
        assert!(maintenance_replacement_ready(&drone));

        drone.operational_capacity = replicant_client::domain::OperationalCapacity::new(94.9);
        assert!(!maintenance_replacement_ready(&drone));
    }

    #[test]
    fn mining_site_omitted_location_or_status_blocks_replacement_mutations() {
        for field in [
            "location",
            "device_type",
            "status",
            "operational_capacity",
            "controller_relationship",
        ] {
            let mut devices = healthy_site_devices("SOL", "SOL-BELT-1");
            match field {
                "location" => devices[2].location = None,
                "device_type" => devices[2].device_type = None,
                "status" => devices[2].status = None,
                "operational_capacity" => devices[2].operational_capacity = None,
                _ => {
                    let child = devices[3].key.clone();
                    devices[0].relationships.controlled_devices.push(child);
                    devices[3].relationships.controller = None;
                }
            }
            let audit = audit_site(&devices, "SOL", "SOL-BELT-1");
            assert!(!audit.mutation_safe, "{field}");
            assert!(!audit.operational, "{field}");
            assert!(audit.issues.iter().any(|issue| matches!(
                issue, MiningSiteIssue::EvidenceIncomplete { field: missing, .. } if *missing == field
            )));
        }
    }

    #[test]
    fn three_healthy_hub_patrol_drones_can_spare_one_but_two_cannot() {
        let hub = "HUB-BELT-1";
        let pool_role = role_tag(HUB_MAINTENANCE_ROLE);
        let mut devices = (0..3)
            .map(|index| {
                let mut drone = device(&format!("H{index}"), MAINTENANCE_DRONE, hub);
                drone.tags = vec![pool_role.clone()];
                drone.operational_capacity =
                    replicant_client::domain::OperationalCapacity::new(100.0);
                directive(&mut drone, "patrol");
                drone
            })
            .collect::<Vec<_>>();

        let audit = maintenance_hub_pool_audit(&devices, hub);
        assert_eq!(audit.healthy_patrol.len(), 3);
        assert!(audit.healthy_patrol.len() > HUB_MAINTENANCE_MIN_HEALTHY);

        devices.pop();
        let audit = maintenance_hub_pool_audit(&devices, hub);
        assert_eq!(audit.healthy_patrol.len(), 2);
        assert!(audit.healthy_patrol.len() <= HUB_MAINTENANCE_MIN_HEALTHY);
        assert_eq!(
            audit.desired_healthy_count(audit.healthy_patrol.len(), false),
            HUB_MAINTENANCE_TARGET_HEALTHY
        );
    }

    #[test]
    fn worn_hub_patrol_drone_repairs_before_becoming_replacement_ready() {
        let hub = "HUB-BELT-1";
        let mut drone = device("WORN", MAINTENANCE_DRONE, hub);
        drone.tags = vec![role_tag(HUB_MAINTENANCE_ROLE)];
        directive(&mut drone, "patrol");
        drone.operational_capacity = replicant_client::domain::OperationalCapacity::new(94.9);

        let audit = maintenance_hub_pool_audit(std::slice::from_ref(&drone), hub);
        assert!(audit.healthy_patrol.is_empty());
        assert_eq!(audit.repairing_patrol, ["WORN"]);
        assert!(!maintenance_replacement_ready(&drone));

        drone.operational_capacity = replicant_client::domain::OperationalCapacity::new(95.0);
        let audit = maintenance_hub_pool_audit(std::slice::from_ref(&drone), hub);
        assert_eq!(audit.healthy_patrol, ["WORN"]);
        assert!(audit.repairing_patrol.is_empty());
        assert!(maintenance_replacement_ready(&drone));
    }

    #[test]
    fn repairing_hub_patrol_does_not_trigger_target_replenishment_above_hard_minimum() {
        let hub = "HUB-BELT-1";
        let pool_role = role_tag(HUB_MAINTENANCE_ROLE);
        let mut devices = Vec::new();
        for (code, capacity) in [("H0", 100.0), ("H1", 100.0), ("REPAIR", 60.0)] {
            let mut drone = device(code, MAINTENANCE_DRONE, hub);
            drone.tags = vec![pool_role.clone()];
            drone.operational_capacity =
                replicant_client::domain::OperationalCapacity::new(capacity);
            directive(&mut drone, "patrol");
            devices.push(drone);
        }

        let audit = maintenance_hub_pool_audit(&devices, hub);
        assert_eq!(audit.healthy_patrol.len(), 2);
        assert_eq!(audit.repairing_patrol, ["REPAIR"]);
        assert_eq!(
            audit.desired_healthy_count(audit.healthy_patrol.len(), false),
            2
        );
    }

    #[test]
    fn hub_pool_ignores_foreign_workflow_reserved_maintenance_drones() {
        let hub = "HUB-BELT-1";
        let mut drone = device("FOREIGN", MAINTENANCE_DRONE, hub);
        drone.tags = vec!["evt-m:other".into()];
        drone.operational_capacity = replicant_client::domain::OperationalCapacity::new(100.0);
        directive(&mut drone, "patrol");

        let audit = maintenance_hub_pool_audit(&[drone], hub);
        assert!(audit.healthy_patrol.is_empty());
    }

    #[test]
    fn absent_or_unusable_maintenance_remains_a_normal_repair() {
        let belt = "SOL-BELT-1";
        let absent = healthy_site_devices("SOL", belt)
            .into_iter()
            .filter(|device| device.key.id.as_str() != "MD")
            .collect::<Vec<_>>();
        let absent_audit = audit_site(&absent, "SOL", belt);
        assert_eq!(
            site_shortages(&absent_audit).get(MAINTENANCE_DRONE),
            Some(&1)
        );
        assert!(absent_audit.normal_repair_needed);
        assert!(!absent_audit.maintenance_rotation_due);

        let mut unusable = healthy_site_devices("SOL", belt);
        unusable[2].status = Some(DeviceStatus::from("deactivated"));
        let unusable_audit = audit_site(&unusable, "SOL", belt);
        assert_eq!(
            site_shortages(&unusable_audit).get(MAINTENANCE_DRONE),
            Some(&1)
        );
        assert!(unusable_audit.issues.iter().any(|issue| matches!(
            issue,
            MiningSiteIssue::UnusableDevice { device, .. } if device == "MD"
        )));
    }

    #[test]
    fn incomplete_controller_state_blocks_mutation_without_printing_replacement() {
        let belt = "SOL-BELT-1";
        let mut devices = healthy_site_devices("SOL", belt);
        devices[0].status = None;

        let audit = audit_site(&devices, "SOL", belt);

        assert!(!audit.operational);
        assert!(!audit.mutation_safe);
        assert!(site_shortages(&audit).is_empty());
        assert!(audit.issues.iter().any(|issue| matches!(
            issue,
            MiningSiteIssue::EvidenceIncomplete { device, field }
                if device == "MC" && *field == "status"
        )));
    }

    #[test]
    fn legacy_mining_checkpoint_deserializes_without_losing_site_assets() {
        let legacy = serde_json::json!({
            "version": 1,
            "mission_id": "legacy-mining",
            "mission_tag": "mine-m:legacy",
            "phase": "deploying_sites",
            "selected_replicant": "WORKER",
            "hub_location": "SOL-L4",
            "sites": [{
                "system": "SOL",
                "belt": "SOL-BELT-1",
                "density": "dense",
                "tag": "mine-s:sol",
                "phase": "configuring",
                "assets": {
                    "mining_controller": "MC",
                    "mining_drones": ["M0", "M1", "M2", "M3"],
                    "survey_controller": "SC",
                    "survey_drones": ["S0", "S1"],
                    "maintenance_drone": "MD"
                },
                "missing": {},
                "carrier": null
            }],
            "routes": [],
            "print_batches": [],
            "site_print_requirements": {},
            "route_print_requirements": {},
            "total_material_cost": {},
            "warnings": []
        });

        let mission: MiningMission = serde_json::from_value(legacy).expect("legacy mission");
        let site = &mission.sites[0];
        assert_eq!(site.phase, SitePhase::Configuring);
        assert_eq!(site.assets.mining_controller.as_deref(), Some("MC"));
        assert_eq!(
            site.assets.mining_drones,
            ["M0", "M1", "M2", "M3"]
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        );
        assert_eq!(site.assets.survey_controller.as_deref(), Some("SC"));
        assert_eq!(
            site.assets.survey_drones,
            ["S0", "S1"]
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        );
        assert_eq!(site.assets.maintenance_drone.as_deref(), Some("MD"));
        assert!(site.assets.system_ward.is_none());
        assert!(mission.legacy_mission_tags.is_empty());
    }

    #[test]
    fn deployed_inactive_ward_is_a_configuration_repair_not_a_print_shortage() {
        let belt = "SOL-BELT-1";
        let devices = vec![device("WARD", SYSTEM_WARD, "SOL-OORT")];
        let audit = audit_site(&devices, "SOL", belt);
        assert_eq!(audit.assets.system_ward.as_deref(), Some("WARD"));
        assert!(!audit.operational);
        assert!(!site_shortages(&audit).contains_key(SYSTEM_WARD));
    }

    #[test]
    fn tags_are_normalized_for_uppercase_systems() {
        assert_eq!(site_tag("ILPHARD"), "mine-s:ilphard");
    }

    #[test]
    fn execution_batches_use_single_queue_units() {
        let scheduled = PrintBatch {
            factory_code: "AF1".into(),
            device_type: MINING_DRONE.into(),
            quantity: 3,
            sequence: 0,
            projected_finish_seconds: 300.0,
        };
        let batches = execution_batches("mission", PrintPurpose::Site, &[scheduled]);
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|batch| batch.quantity == 1));
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.batch_tag.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );
    }

    #[test]
    fn planner_reuses_output_reserved_for_the_same_stable_mining_mission() {
        let hub = "ROOT-1-L4";
        let mission_tag = mining_mission_tag(hub);
        let mut same_mission = device("WARD-SAME", SYSTEM_WARD, hub);
        same_mission.tags = vec![mission_tag.clone(), "mine-b:old-batch".into()];
        let mut other_mission = device("WARD-OTHER", SYSTEM_WARD, hub);
        other_mission.tags = vec!["mine-m:other".into(), "mine-b:other-batch".into()];

        let same_counts = reusable_counts(&[same_mission], hub, &mission_tag, true);
        assert_eq!(same_counts.get(SYSTEM_WARD), Some(&1));
        let other_counts = reusable_counts(&[other_mission], hub, &mission_tag, true);
        assert_eq!(
            other_counts.get(SYSTEM_WARD).copied().unwrap_or_default(),
            0
        );
    }
    #[test]
    fn traveling_freighter_relationship_completes_existing_route() {
        let hub = "SCEPTURUM-BELT-1";
        let belt = "ILPHARD-BELT-1";
        let mut controller = device("TC", TRANSPORT_CONTROLLER, hub);
        controller.relationships.controlled_devices = vec![DeviceKey::live(DeviceId::from("CF"))];
        controller.active_directive = Some(ActiveDeviceDirective {
            directive: Some(DeviceDirective::from("ferry")),
            status: Some("active".into()),
            details: [(
                "config".into(),
                serde_json::json!({
                    "collect": belt,
                    "deliver": hub,
                    "priority": ["rares", "volatiles"]
                }),
            )]
            .into_iter()
            .collect(),
        });
        let mut freighter = device("CF", CARGO_FREIGHTER, hub);
        freighter.location = None;
        freighter.relationships.controller = Some(DeviceKey::live(DeviceId::from("TC")));
        let idle_audit = transport_service_present(
            &[controller.clone(), freighter.clone()],
            "ILPHARD",
            belt,
            hub,
        );
        assert_eq!(idle_audit.state, EvidenceState::Absent);

        controller.status = Some(DeviceStatus::from("coordinating"));

        let audit = transport_service_present(&[controller, freighter], "ILPHARD", belt, hub);
        assert_eq!(audit.state, EvidenceState::Present);
        assert_eq!(audit.controller.as_deref(), Some("TC"));
        assert_eq!(audit.freighter.as_deref(), Some("CF"));
    }

    #[test]
    fn legacy_transport_route_intent_defaults_to_one_freighter() {
        let route: AmiTransportRouteIntent = serde_json::from_value(serde_json::json!({
            "system": "ALPHA",
            "collect": "ALPHA-BELT-1",
            "deliver": "ALPHA-HUB"
        }))
        .expect("legacy route intent");
        assert_eq!(route.desired_freighters, MIN_TRANSPORT_FREIGHTERS);
    }

    #[test]
    fn legacy_one_freighter_route_checkpoint_defaults_to_one_capacity() {
        let legacy = serde_json::json!({
            "system": "ALPHA",
            "belt": "ALPHA-BELT-1",
            "tag": "mine-s:alpha",
            "phase": "active",
            "controller": "TC",
            "freighter": "CF-1"
        });
        let route: RouteMission = serde_json::from_value(legacy).expect("legacy route");
        assert_eq!(route.freighter.as_deref(), Some("CF-1"));
        assert!(route.additional_freighters.is_empty());
        assert_eq!(route.desired_freighters, 1);
        assert_eq!(route.freighters(), vec!["CF-1".to_owned()]);
    }

    #[test]
    fn transport_audit_counts_all_adopted_healthy_freighters() {
        let system = "ILPHARD";
        let belt = "ILPHARD-BELT-1";
        let hub = "SCEPTURUM-BELT-1";
        let controller =
            healthy_transport_controller("TC", system, belt, hub, &["CF-1", "CF-2", "CF-3"]);
        let devices = vec![
            controller,
            transport_freighter("CF-1", "TC", belt, "idle"),
            transport_freighter("CF-2", "TC", belt, "travelling"),
            transport_freighter("CF-3", "TC", hub, "depositing"),
        ];
        let audit = transport_service_present(&devices, system, belt, hub);
        assert_eq!(audit.state, EvidenceState::Present);
        assert_eq!(audit.collect, belt);
        assert_eq!(audit.deliver, hub);
        assert_eq!(audit.controller_operational, EvidenceState::Present);
        assert_eq!(audit.controller_directive, EvidenceState::Present);
        assert_eq!(audit.controller_configuration, EvidenceState::Present);
        assert_eq!(audit.adopted_freighters, ["CF-1", "CF-2", "CF-3"]);
        assert_eq!(audit.usable_freighter_count(), 3);
        assert!(audit.unknown_freighters.is_empty());
        assert!(audit.unusable_freighters.is_empty());
        let mut incomplete = devices;
        incomplete[1].device_type = None;
        incomplete[0].relationships.controlled_devices.remove(0);
        assert_eq!(
            transport_service_present(&incomplete, system, belt, hub).state,
            EvidenceState::Unknown,
            "an untyped inverse relationship is not proof of spare route capacity",
        );
    }

    #[test]
    fn authoritative_empty_transport_membership_is_known_zero_capacity() {
        let system = "ILPHARD";
        let belt = "ILPHARD-BELT-1";
        let hub = "SCEPTURUM-BELT-1";
        let controller = healthy_transport_controller("TC", system, belt, hub, &[]);

        let conservative =
            transport_service_present(std::slice::from_ref(&controller), system, belt, hub);
        assert_eq!(conservative.state, EvidenceState::Unknown);

        let authoritative =
            transport_service_present_with_authority(&[controller], system, belt, hub, true);
        assert_eq!(authoritative.state, EvidenceState::Absent);
        assert_eq!(authoritative.usable_freighter_count(), 0);
        assert!(authoritative.adopted_freighters.is_empty());
    }

    #[test]
    fn wrong_transport_controller_directive_is_structurally_broken_not_healthy() {
        let system = "ILPHARD";
        let belt = "ILPHARD-BELT-1";
        let hub = "SCEPTURUM-BELT-1";
        let mut controller = healthy_transport_controller("TC", system, belt, hub, &["CF-1"]);
        controller
            .active_directive
            .as_mut()
            .expect("directive")
            .directive = Some(DeviceDirective::from("shuttle"));
        let freighter = transport_freighter("CF-1", "TC", belt, "idle");
        let audit = transport_service_present(&[controller, freighter], system, belt, hub);
        assert_eq!(audit.controller_operational, EvidenceState::Present);
        assert_eq!(audit.controller_directive, EvidenceState::Absent);
        assert_eq!(audit.controller_configuration, EvidenceState::Present);
        assert_eq!(audit.state, EvidenceState::Absent);
        assert_eq!(audit.usable_freighter_count(), 1);
    }

    #[test]
    fn wrong_controller_and_unusable_freighters_do_not_count_as_capacity() {
        let system = "ILPHARD";
        let belt = "ILPHARD-BELT-1";
        let hub = "SCEPTURUM-BELT-1";
        let controller =
            healthy_transport_controller("TC", system, belt, hub, &["GOOD", "UNUSABLE"]);
        let devices = vec![
            controller,
            transport_freighter("GOOD", "TC", belt, "idle"),
            transport_freighter("UNUSABLE", "TC", belt, "mining"),
            transport_freighter("WRONG", "OTHER", belt, "idle"),
        ];
        let audit = transport_service_present(&devices, system, belt, hub);
        assert_eq!(audit.state, EvidenceState::Present);
        assert_eq!(audit.usable_freighters, ["GOOD"]);
        assert_eq!(audit.unusable_freighters, ["UNUSABLE"]);
        assert!(!audit.adopted_freighters.contains(&"WRONG".to_owned()));
        assert_eq!(audit.usable_freighter_count(), 1);
    }

    #[test]
    fn transport_backlog_thresholds_use_exact_freighter_load_bands() {
        for (units, class, delta) in [
            (-1, TransportBacklogClass::Low, 0),
            (0, TransportBacklogClass::Low, 0),
            (999, TransportBacklogClass::Low, 0),
            (1_000, TransportBacklogClass::HealthyBand, 0),
            (5_000, TransportBacklogClass::HealthyBand, 0),
            (5_001, TransportBacklogClass::ScaleUp, 1),
            (15_000, TransportBacklogClass::ScaleUp, 1),
            (15_001, TransportBacklogClass::Critical, 2),
            (i64::MAX, TransportBacklogClass::Critical, 2),
        ] {
            assert_eq!(transport_backlog_class(units), class, "{units}");
            assert_eq!(transport_scale_up_delta(units), delta, "{units}");
            for count in 1..=MAX_TRANSPORT_FREIGHTERS {
                let evaluation = evaluate_transport_capacity(
                    units,
                    count,
                    0,
                    &TransportCapacityTrend::default(),
                );
                let expected = delta.min(MAX_TRANSPORT_FREIGHTERS - count);
                assert_eq!(
                    evaluation.action,
                    if expected == 0 {
                        TransportCapacityAction::Hold
                    } else {
                        TransportCapacityAction::Add(expected)
                    }
                );
            }
        }
    }

    #[test]
    fn critical_backlog_requests_two_freighters_bounded_by_route_maximum() {
        let evaluation =
            evaluate_transport_capacity(54_979, 1, 1_000, &TransportCapacityTrend::default());
        assert_eq!(transport_backlog_loads(54_979), 54_979_f64 / 500_f64);
        assert_eq!(
            transport_backlog_class(54_979),
            TransportBacklogClass::Critical
        );
        assert_eq!(evaluation.action, TransportCapacityAction::Add(2));

        let bounded =
            evaluate_transport_capacity(54_979, 5, 1_000, &TransportCapacityTrend::default());
        assert_eq!(bounded.action, TransportCapacityAction::Add(1));
    }

    #[test]
    fn transport_capacity_cooldown_and_falling_backlog_suppress_repeated_additions() {
        let previous = TransportCapacityTrend {
            last_observed_backlog_units: Some(20_000),
            last_observation_at_ms: Some(0),
            last_capacity_change_at_ms: Some(0),
            low_backlog_since_ms: None,
        };
        let during_cooldown =
            evaluate_transport_capacity(22_000, 2, TRANSPORT_CAPACITY_COOLDOWN_MS - 1, &previous);
        assert_eq!(during_cooldown.action, TransportCapacityAction::Hold);

        let falling =
            evaluate_transport_capacity(18_000, 2, TRANSPORT_CAPACITY_COOLDOWN_MS + 1, &previous);
        assert_eq!(falling.action, TransportCapacityAction::Hold);

        let flat_after_cooldown =
            evaluate_transport_capacity(20_000, 2, TRANSPORT_CAPACITY_COOLDOWN_MS, &previous);
        assert_eq!(flat_after_cooldown.action, TransportCapacityAction::Add(2));
        assert!(flat_after_cooldown.refresh_before_decision);
        assert_eq!(
            flat_after_cooldown.throughput,
            TransportThroughputState::Insufficient
        );
    }
    #[test]
    fn flat_critical_backlog_at_route_maximum_is_an_actionable_stuck_state() {
        let previous = TransportCapacityTrend {
            last_observed_backlog_units: Some(54_979),
            last_observation_at_ms: Some(0),
            last_capacity_change_at_ms: None,
            low_backlog_since_ms: None,
        };

        let evaluation = evaluate_transport_capacity(
            54_979,
            MAX_TRANSPORT_FREIGHTERS,
            TRANSPORT_CAPACITY_COOLDOWN_MS + 1,
            &previous,
        );

        assert_eq!(evaluation.action, TransportCapacityAction::Hold);
        assert!(evaluation.refresh_before_decision);
        assert_eq!(
            evaluation.throughput,
            TransportThroughputState::InsufficientAtMaximum
        );
    }

    #[test]
    fn scale_down_requires_continuous_low_backlog_hold_and_never_drops_below_one() {
        let first = evaluate_transport_capacity(500, 2, 0, &TransportCapacityTrend::default());
        assert_eq!(first.action, TransportCapacityAction::Hold);
        assert_eq!(first.trend.low_backlog_since_ms, Some(0));

        let before_hold =
            evaluate_transport_capacity(500, 2, TRANSPORT_SCALE_DOWN_HOLD_MS - 1, &first.trend);
        assert_eq!(before_hold.action, TransportCapacityAction::Hold);

        let due =
            evaluate_transport_capacity(500, 2, TRANSPORT_SCALE_DOWN_HOLD_MS, &before_hold.trend);
        assert_eq!(due.action, TransportCapacityAction::RemoveOne);

        let minimum =
            evaluate_transport_capacity(500, 1, TRANSPORT_SCALE_DOWN_HOLD_MS, &before_hold.trend);
        assert_eq!(minimum.action, TransportCapacityAction::Hold);
        let interruption =
            evaluate_transport_capacity(1_000, 2, TRANSPORT_SCALE_DOWN_HOLD_MS, &before_hold.trend);
        let low_again = evaluate_transport_capacity(
            999,
            2,
            TRANSPORT_SCALE_DOWN_HOLD_MS + 1,
            &interruption.trend,
        );
        assert_eq!(low_again.action, TransportCapacityAction::Hold);

        let mut changed = due.trend;
        record_transport_capacity_change(&mut changed, TRANSPORT_SCALE_DOWN_HOLD_MS);
        assert_eq!(
            changed.last_capacity_change_at_ms,
            Some(TRANSPORT_SCALE_DOWN_HOLD_MS)
        );
        assert!(changed.low_backlog_since_ms.is_none());
    }

    #[test]
    fn mission_phase_advances_without_regressing_after_resume() {
        assert_eq!(
            MissionPhase::Planned.advance_to(MissionPhase::ManufacturingSites),
            MissionPhase::ManufacturingSites
        );
        assert_eq!(
            MissionPhase::ActivatingRoutes.advance_to(MissionPhase::ManufacturingSites),
            MissionPhase::ActivatingRoutes
        );
        assert_eq!(
            MissionPhase::Completed.advance_to(MissionPhase::ReturningCarriers),
            MissionPhase::Completed
        );
    }
}
