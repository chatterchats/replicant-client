//! Intent-driven workflow layer for web/Tauri automation.
//!
//! These workflows accept player goals instead of CLI execution plumbing.
//! They compose the managed client, reusable runtime services, and durable
//! child workflows while keeping workflow checkpoints authoritative.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::bootstrap::{
    BootstrapExecutionRequest, BootstrapPlanningRequest, plan_bootstrap, run_bootstrap,
};
use replicant_client::{
    Client, Device, DeviceHandle, DeviceType, MiningDirective, Operation, OperationId,
    OperationStatus, SurveyDirective, domain::AccessScope,
};
#[cfg(test)]
use replicant_printing::managed::TrackedPrintAssignment;
use replicant_printing::{
    PrintRequest,
    managed::{
        FactoryPrintStatus, PrintingError, QueueOptions, SystemPrintingStatus, TrackedPrintRequest,
        TrackedPrintUpdate, fetch_blueprints, printing_status_in_system, queue_print_prerequisites,
        queue_prints_with_components, queue_tracked_prints_once,
    },
};
use replicant_protocol::{workflow_reserved, workflow_tag_reserved};
#[cfg(test)]
use replicant_transport::PayloadDevice;
use replicant_transport::{
    DeliveryOptions, DeliveryPlan, DeliveryReport, DeliveryRequest, DeviceRequest, ResourceMap,
    TransportError, execute_delivery, plan_delivery, validate_resource_pickups,
};
use replicant_workflow::{
    AllocationSet, BoxWorkflowFuture, ClaimAcquireOutcome, ControlRequest, NewWorkflow,
    RegistryError, RepositoryError, RequirementScope, ResourceKey, ResourceRequirement, WaitIntent,
    WaitOutcome, WaitSignal, WorkItem, WorkItemSpec, WorkItemStatus, WorkItemTransition,
    WorkflowContext, WorkflowExecutor, WorkflowFactory, WorkflowId, WorkflowKind,
    WorkflowMigration, WorkflowPlacementIntent, WorkflowPlacementIntentCoverage,
    WorkflowPlacementIntentProjection, WorkflowPlacementIntentRelation,
    WorkflowPlacementIntentSnapshot, WorkflowPlacementIntentSubject, WorkflowPlacementProvenance,
    WorkflowPlacementResolution, WorkflowRegistry, WorkflowServiceIntentProjection,
    WorkflowServiceScope, WorkflowStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    belt_search::{BeltOperationRejection, execute_belt_search_system, travel_to_system},
    canonical_region,
    event::{
        EventCampaignArchive, EventCampaignPlanningRequest, EventExecutionRequest, EventItemStage,
        EventPlanningRequest, EventStockReconcileOptions, archive_event_campaign,
        event_campaign_target_systems, event_campaign_work_item_specs,
        event_campaign_workflow_targets, event_mission_target_system,
        event_mission_workflow_target, execute_event_item, execute_event_mission,
        haul_allocated_resources, plan_event_campaign, plan_event_mission, prestage_event_mission,
        reconcile_event_stock, restore_event_campaign,
    },
    failure::{FailureClass, failure_class, failure_class_from_message, failure_disposition},
    mining::{AmiTransportRouteIntent, MiningExpansionRequest, MiningMission, execute_expansion},
    observatory::auto_prospect,
    relay::{
        RelayExecutionState, RelayExpansionRequest, execute_relay_workflow,
        ftl_network_reachable_systems, relay_failure_is_topology_impossible,
        relay_topology_signature, restore_relay_checkpoint,
    },
    survey::{
        SurveyExecutionState, SurveyMode, SurveyOptions, execute_survey_workflow,
        restore_survey_checkpoint,
    },
    trade::{TradeBundle, shop_trades},
    worker_state::{OPERATIONAL_REGIONAL_WORKER_CAPABILITY, WorkerState, classify_regional_worker},
    workflows::{
        ManagedMiningItemExecutor, MiningWorkflowCheckpoint, MiningWorkflowConfig,
        execute_mining_pool_config,
    },
};

const SCHEMA_VERSION: u32 = 1;
const DEFAULT_WAIT_SECONDS: u64 = 21_600;
const IDLE_CAMPAIGN_RETRY_INTERVAL: Duration = Duration::from_secs(300);
pub(crate) const EVENT_DEPENDENCY_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(60);
const EVENT_CONNECTIVITY_RETRY_COOLDOWN: Duration = Duration::from_secs(30 * 60);
const CAMPAIGN_RESOURCE_EVENT_NAMES: [&str; 11] = [
    "device.attached",
    "device.changed_owner",
    "device.compacted",
    "device.compacting",
    "device.decommissioned",
    "device.deployed",
    "device.detached",
    "device.stowed",
    "device.unfurled",
    "device.unfurling",
    "replicant.transferred",
];
pub(crate) const EVENT_CAMPAIGN_DEPENDENCY_EVENT_NAMES: [&str; 14] = [
    "device.attached",
    "device.changed_owner",
    "device.compacted",
    "device.compacting",
    "device.decommissioned",
    "device.deployed",
    "device.detached",
    "device.stowed",
    "device.unfurled",
    "device.unfurling",
    "event.completed",
    "print.completed",
    "relay.activated",
    "replicant.transferred",
];

/// Intent-native workflow that fully surveys one system with an AMI survey controller.
pub fn scan_system_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("scan.system").expect("static workflow kind is valid")
}

/// Intent-native workflow that searches one system's asteroid belt.
pub fn scan_belt_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("scan.belt").expect("static workflow kind is valid")
}

/// Intent-native workflow that surveys a bounded area with one racing vessel.
pub fn scan_tour_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("scan.tour").expect("static workflow kind is valid")
}

/// Intent-native batch belt-discovery workflow.
pub fn belt_search_campaign_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("belt_search.campaign").expect("static workflow kind is valid")
}

/// Intent-native workflow that salvages one site to depletion.
pub fn salvage_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("salvage.site").expect("static workflow kind is valid")
}

/// Regional recovery campaign for remotely discovered salvage sites.
pub fn salvage_recovery_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("salvage.recovery").expect("static workflow kind is valid")
}

/// Intent-native workflow that deploys one mining installation.
pub fn mining_deploy_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("mining.deploy").expect("static workflow kind is valid")
}

/// Intent-native batch mining expansion workflow.
pub fn mining_campaign_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("mining.campaign").expect("static workflow kind is valid")
}

/// Intent-native point-to-point logistics workflow.
pub fn logistics_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("logistics.delivery").expect("static workflow kind is valid")
}
/// Intent-native workflow that provisions a regional shipment and dispatches it.
pub fn regional_dispatch_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("logistics.regional_dispatch").expect("static workflow kind is valid")
}

/// Internal mixed-manifest logistics workflow used by Director coordinators.
pub fn logistics_manifest_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("logistics.manifest").expect("static workflow kind is valid")
}

/// Durable coordinator for provisioning, executing, and returning from one shop trade.
pub fn trade_fulfillment_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("trade.fulfillment").expect("static workflow kind is valid")
}

/// Durable coordinator for learning one missing blueprint from an owned device.
pub fn blueprint_acquire_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("blueprint.acquire").expect("static workflow kind is valid")
}

/// Intent-native directed exploration workflow backed by the relay expansion engine.
pub fn exploration_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("exploration.frontier").expect("static workflow kind is valid")
}

/// Event workflow that prepares event payloads without claiming the assigned replicant.
pub fn event_delivery_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("event.delivery").expect("static workflow kind is valid")
}

/// Event workflow that ensures delivery is ready, then dispatches the replicant to resolve it.
pub fn event_tour_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("event.tour").expect("static workflow kind is valid")
}

/// Regional batch event campaign workflow.
pub fn event_campaign_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("event.campaign").expect("static workflow kind is valid")
}

/// Intent-native bounded observatory prospect workflow.
pub fn observatory_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("observatory.search").expect("static workflow kind is valid")
}

/// Grow-only workforce provisioning workflow.
pub fn replicant_provision_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("replicant.provision").expect("static workflow kind is valid")
}

/// Stable kind for autonomous regional bootstrap.
#[must_use]
pub fn region_establish_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("region.establish").expect("valid workflow kind")
}

/// Registers intent-native application workflows.
pub fn register(registry: &mut WorkflowRegistry) -> Result<(), RegistryError> {
    registry.register(Arc::new(ScanSystemWorkflowFactory::new()))?;
    registry.register(Arc::new(ScanBeltWorkflowFactory::new()))?;
    registry.register(Arc::new(ScanTourWorkflowFactory::new()))?;
    registry.register(Arc::new(BeltSearchCampaignWorkflowFactory::new()))?;
    registry.register(Arc::new(SalvageWorkflowFactory::new()))?;
    registry.register(Arc::new(SalvageRecoveryWorkflowFactory::new()))?;
    registry.register(Arc::new(MiningDeployWorkflowFactory::new()))?;
    registry.register(Arc::new(MiningCampaignWorkflowFactory::new()))?;
    registry.register(Arc::new(LogisticsWorkflowFactory::new()))?;
    registry.register(Arc::new(RegionalDispatchWorkflowFactory::new()))?;
    registry.register(Arc::new(LogisticsManifestWorkflowFactory::new()))?;
    registry.register(Arc::new(TradeFulfillmentWorkflowFactory::new()))?;
    registry.register(Arc::new(BlueprintAcquireWorkflowFactory::new()))?;
    registry.register(Arc::new(ExplorationWorkflowFactory::new()))?;
    registry.register(Arc::new(EventDeliveryWorkflowFactory::new()))?;
    registry.register(Arc::new(EventTourWorkflowFactory::new()))?;
    registry.register(Arc::new(EventCampaignWorkflowFactory::new()))?;
    registry.register(Arc::new(ObservatoryWorkflowFactory::new()))?;
    registry.register(Arc::new(ReplicantProvisionWorkflowFactory::new()))?;
    registry.register(Arc::new(
        crate::asteroid_diversion::AsteroidDiversionWorkflowFactory::new(),
    ))?;
    registry.register(Arc::new(RegionEstablishWorkflowFactory::new()))
}

/// Shared player intent for system/belt survey automation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScanIntent {
    /// System to survey or belt-search.
    pub system: String,
    /// Optional survey controller to pin. When omitted an idle owned controller in-system is used.
    #[serde(default)]
    pub controller: Option<String>,
    /// Whether the controller should recall its fleet when the directive supports it.
    #[serde(default = "default_true")]
    pub recall: bool,
}

/// Goal-level input for a bounded survey tour.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScanTourIntent {
    /// Centre system for route planning.
    pub center: String,
    /// Radius around the centre system.
    #[serde(default = "default_tour_radius")]
    pub radius_ly: f64,
    /// Maximum systems to include in one route.
    #[serde(default = "default_tour_limit")]
    pub system_limit: usize,
    /// Optional exact system allowlist. When present, the route is constrained to these systems.
    #[serde(default)]
    pub target_systems: Option<Vec<String>>,
    /// Optional replicant to pin.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Optional racing vessel to pin.
    #[serde(default)]
    pub vessel: Option<String>,
    /// Include systems already marked explored.
    #[serde(default)]
    pub include_explored: bool,
}

/// Authoritative checkpoint for an intent-native survey tour.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ScanTourCheckpoint {
    /// Resolved replicant.
    pub replicant: Option<String>,
    /// Resolved racing vessel.
    pub vessel: Option<String>,
    /// Resolved maintenance home.
    pub maintenance_home: Option<String>,
    /// Stable tag applied to any survey-fleet devices manufactured for this tour.
    #[serde(default)]
    pub fleet_print_tag: Option<String>,
    /// Child manifest currently staging newly printed survey-fleet devices.
    #[serde(default)]
    pub fleet_logistics_child: Option<WorkflowId>,
    /// Exact survey controller reserved for this tour.
    #[serde(default)]
    pub fleet_controller: Option<String>,
    /// Exact survey drones reserved for this tour.
    #[serde(default)]
    pub fleet_drones: Vec<String>,
    /// Last authoritative survey executor state.
    pub state: Option<SurveyExecutionState>,
}

/// Goal-level input for a bounded fast belt-search campaign.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BeltSearchCampaignIntent {
    /// Exact systems to visit and inspect.
    pub systems: Vec<String>,
    /// Canonical operating region whose worker pool may execute the items.
    pub region: String,
}

/// Restart-safe fast belt-search checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct BeltSearchCampaignCheckpoint {
    /// Original version-one checkpoint retained as migration evidence.
    #[serde(default)]
    pub legacy_checkpoint: Option<Value>,
}

/// Restart-safe controller workflow checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct ControllerWorkflowCheckpoint {
    /// Controller selected by the workflow.
    pub controller: Option<String>,
    /// Whether the desired directive has been accepted.
    pub directive_set: bool,
    /// Whether the controller fleet has been launched.
    pub launched: bool,
    /// Whether managed state has observed the controller actively coordinating.
    pub observed_active: bool,
    /// Consecutive idle observations after launch, used to tolerate fast directives.
    #[serde(default)]
    pub idle_observations: u8,
}

/// Player intent for one salvage site.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SalvageIntent {
    /// Exact salvage-site location designation.
    pub location: String,
    /// Optional AMI mining controller to pin.
    #[serde(default)]
    pub controller: Option<String>,
    /// Recall the fleet after depletion.
    #[serde(default = "default_true")]
    pub recall: bool,
}

/// Identity-free regional salvage recovery intent.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SalvageRecoveryIntent {
    /// Director region containing eligible discovered sites.
    pub region: String,
    /// Home location for recovered resources and idle capacity.
    pub home: String,
}

/// One authoritative salvage discovery retained after depletion/ledger filtering.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SalvageSiteRecord {
    /// Exact site designation used by `GatherSalvage`.
    pub designation: String,
    /// Parent body/location used for travel.
    pub location: String,
    /// Declared resource quantities, when supplied by the event.
    #[serde(default)]
    pub resources: BTreeMap<String, i64>,
    /// Remote event cursor used for newest-wins reconciliation.
    pub event_id: String,
}

/// Durable salvage campaign checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct SalvageRecoveryCheckpoint {
    /// Last authoritative worklist, keyed by site designation.
    pub sites: BTreeMap<String, SalvageSiteRecord>,
}

/// Goal-level input for deploying one mining installation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MiningDeployIntent {
    /// Target system.
    pub system: String,
    /// Optional replicant to pin.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Optional manufacturing hub.
    #[serde(default)]
    pub hub: Option<String>,
}

/// Goal-level input for a regional broker-allocated mining expansion.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MiningCampaignIntent {
    /// Target systems selected by the regional campaign planner.
    pub systems: Vec<String>,
    /// Director region whose worker pool may execute the campaign.
    pub region: String,
    /// Regional manufacturing hub.
    pub hub: String,
    /// Exact AMI transport routes to establish.
    #[serde(default)]
    pub transport_routes: Vec<AmiTransportRouteIntent>,
    /// Scheduler ceiling for simultaneously runnable items.
    #[serde(default = "default_mining_concurrency")]
    pub max_concurrency: usize,
}

#[derive(Clone, Debug, Deserialize)]
struct LegacyMiningCampaignIntent {
    systems: Vec<String>,
    #[serde(default)]
    region: Option<String>,
    #[serde(default)]
    replicant: Option<String>,
    #[serde(default)]
    hub: Option<String>,
    #[serde(default = "default_mining_concurrency")]
    max_concurrency: usize,
}

/// Restart-safe mining deployment checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct MiningDeployCheckpoint {
    /// Resolved replicant.
    pub replicant: Option<String>,
    /// Resolved manufacturing hub.
    pub hub: Option<String>,
    /// Last serialized legacy mining mission state.
    pub plan_json: Option<String>,
    /// Whether execution entered the reusable mining executor.
    pub started: bool,
}

/// Schema-version-three mining campaign checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct MiningCampaignCheckpoint {
    /// Last merged mission state.
    #[serde(default)]
    pub mission: Option<MiningMission>,
    /// Legacy actor used only for region evidence resolution.
    #[serde(default)]
    pub migration_worker: Option<String>,
    /// Whether pooled execution began.
    pub started: bool,
}

/// Human-facing payload selector for one logistics delivery.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogisticsPayloadKind {
    /// Resource type and quantity.
    Resource,
    /// Device type and quantity.
    Device,
    /// Every eligible device carrying the supplied tag.
    Tag,
}

/// Player intent for point-to-point logistics.
///
/// The legacy `payload_kind`/`item`/`quantity` fields remain optional so
/// persisted one-item workflows continue to deserialize. New callers should
/// prefer the mixed manifest fields.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LogisticsIntent {
    /// Origin location or system scope.
    pub origin: String,
    /// Exact destination location.
    pub destination: String,
    /// Legacy selector describing `item`.
    #[serde(default)]
    pub payload_kind: Option<LogisticsPayloadKind>,
    /// Legacy resource type, device type, or tag.
    #[serde(default)]
    pub item: Option<String>,
    /// Requested resource/device quantity. Ignored for tag payloads.
    #[serde(default = "default_quantity")]
    pub quantity: i64,
    /// Resource quantities to move together.
    #[serde(default)]
    pub resources: ResourceMap,
    /// Device-type quantities to move together.
    #[serde(default)]
    pub devices: Vec<DeviceRequest>,
    /// Every eligible device carrying any supplied tag.
    #[serde(default)]
    pub device_tags: Vec<String>,
    /// Return transports after delivery.
    #[serde(default)]
    pub return_transports: bool,
}

/// Player intent for manufacturing and dispatching a complete regional shipment.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RegionalDispatchIntent {
    /// Regional hub system or location. Execution resolves this to the hub's exact manufacturing location.
    pub source: String,
    /// Exact destination location.
    pub destination: String,
    /// Resource quantities to deliver.
    #[serde(default)]
    pub resources: ResourceMap,
    /// Racing vessels to deliver, each paired with an empty or replicated matrix.
    #[serde(default)]
    pub racing_vessels: i64,
    /// HEAVEN vessels to deliver, each paired with an empty or replicated matrix.
    #[serde(default)]
    pub heaven_vessels: i64,
    /// Cargo vessels to deliver, each paired with an empty or replicated matrix.
    #[serde(default)]
    pub cargo_vessels: i64,
    /// Additional device-type quantities to deliver.
    #[serde(default)]
    pub devices: Vec<DeviceRequest>,
}

/// Restart-safe provisioning and delivery state for a regional dispatch.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct RegionalDispatchCheckpoint {
    /// Exact manufacturing location resolved from the operator's regional hub selection.
    pub source_location: Option<String>,
    /// Durable tag applied to every device printed for this dispatch.
    pub print_tag: String,
    /// Exact vessel codes selected for the requested Replicant fleet.
    pub vessels: Vec<String>,
    /// Whether existing stock selection and print-deficit calculation completed.
    pub selection_complete: bool,
    /// Exact empty matrix paired by index with `vessels`; missing entries await printing.
    pub matrices: Vec<Option<String>>,
    /// Exact additional payload device codes.
    pub devices: Vec<String>,
    /// Stable print deficits calculated after existing unclaimed stock is selected.
    pub print_requests: Vec<PrintRequest>,
    /// Matrices that have been stowed into their paired vessel.
    pub stowed_matrices: BTreeSet<String>,
    /// Target matrices into which replication completed.
    pub replicated_matrices: BTreeSet<String>,
    /// Whether tagged printed devices have been incorporated into the exact manifest.
    pub manufacturing_complete: bool,
    /// Concrete smart-routed transport plan persisted before delivery starts.
    pub plan: Option<DeliveryPlan>,
    /// Whether delivery execution has begun.
    pub delivery_started: bool,
}

/// Exact failed-custody evidence and stale reservation tags for a recovery
/// manifest. Device-code map keys are canonical uppercase codes.
#[derive(Clone, Debug, Default, Eq, PartialEq, Deserialize, Serialize)]
pub struct PlacementRecoveryMetadata {
    /// Exact failed transient custody episodes keyed by canonical device code.
    #[serde(default)]
    pub failed_provenance: BTreeMap<String, Vec<WorkflowPlacementProvenance>>,
    /// Reserved workflow tags to remove, keyed by canonical device code.
    #[serde(default)]
    pub release_device_tags: BTreeMap<String, Vec<String>>,
    /// Exact failed episodes resolved after successful placement recovery.
    #[serde(default)]
    pub placement_resolutions: Vec<WorkflowPlacementResolution>,
}

/// Exact Director authorization for one placement-recovery workflow.
///
/// The document key is the workflow ID.  Keeping the complete candidate
/// identity in the value prevents a workflow row (or a manually fabricated
/// config) from being rebound to a different device, origin, destination, or
/// failed custody episode after the Director has issued authorization.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub(crate) struct PlacementRecoveryAuthorization {
    pub(crate) workflow_id: WorkflowId,
    pub(crate) region: String,
    pub(crate) device_code: String,
    pub(crate) origin: String,
    pub(crate) destination: String,
    pub(crate) metadata: PlacementRecoveryMetadata,
}

const PLACEMENT_RECOVERY_AUTHORIZATION_NS: &str = "director.placement_recovery_authorization";

pub(crate) fn placement_recovery_authorization(
    workflow_id: WorkflowId,
    region: &str,
    device_code: &str,
    origin: &str,
    destination: &str,
    metadata: PlacementRecoveryMetadata,
) -> PlacementRecoveryAuthorization {
    PlacementRecoveryAuthorization {
        workflow_id,
        region: canonical_region(region),
        device_code: canonical_manifest_device_code(device_code),
        origin: origin.to_owned(),
        destination: destination.to_owned(),
        metadata,
    }
}

pub(crate) fn read_placement_recovery_authorization(
    repository: &replicant_workflow::WorkflowRepository,
    workflow_id: WorkflowId,
) -> Result<Option<PlacementRecoveryAuthorization>, String> {
    let Some((value, _revision)) = repository
        .read_document(
            PLACEMENT_RECOVERY_AUTHORIZATION_NS,
            &workflow_id.to_string(),
        )
        .map_err(string_error)?
    else {
        return Ok(None);
    };
    serde_json::from_value(value)
        .map(Some)
        .map_err(|error| format!("invalid placement recovery authorization: {error}"))
}

pub(crate) fn write_placement_recovery_authorization(
    repository: &replicant_workflow::WorkflowRepository,
    authorization: &PlacementRecoveryAuthorization,
) -> Result<(), RepositoryError> {
    repository
        .put_document(
            PLACEMENT_RECOVERY_AUTHORIZATION_NS,
            &authorization.workflow_id.to_string(),
            authorization,
        )
        .map(|_| ())
}

pub(crate) fn revoke_placement_recovery_authorization(
    repository: &replicant_workflow::WorkflowRepository,
    workflow_id: WorkflowId,
) -> Result<(), RepositoryError> {
    repository
        .put_document(
            PLACEMENT_RECOVERY_AUTHORIZATION_NS,
            &workflow_id.to_string(),
            &serde_json::json!({"revoked": true}),
        )
        .map(|_| ())
}

/// Checks that a recovery workflow's immutable config is exactly what the
/// Director authorized under its workflow-ID document.
pub(crate) fn placement_recovery_authorization_matches(
    authorization: &PlacementRecoveryAuthorization,
    workflow_id: WorkflowId,
    intent: &LogisticsManifestIntent,
) -> bool {
    let Some(metadata) = intent.placement_recovery.as_ref() else {
        return false;
    };
    authorization.workflow_id == workflow_id
        && intent
            .region
            .as_deref()
            .is_some_and(|region| canonical_region(region) == authorization.region)
        && intent.origin == authorization.origin
        && intent.destination == authorization.destination
        && intent.device_codes.len() == 1
        && intent.device_codes[0] == authorization.device_code
        && metadata == &authorization.metadata
}

/// Internal Director/coordinator intent for one mixed resource/device shipment.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LogisticsManifestIntent {
    /// Origin location or system scope.
    pub origin: String,
    /// Exact destination location.
    pub destination: String,
    /// Resource quantities to move together.
    #[serde(default)]
    pub resources: ResourceMap,
    /// Device quantities to move together.
    #[serde(default)]
    pub devices: Vec<DeviceRequest>,
    /// Exact physical device codes to move. Coordinators use this when a
    /// selected device identity must survive restart/replanning.
    #[serde(default)]
    pub device_codes: Vec<String>,
    /// Optional tagged device groups to include.
    #[serde(default)]
    pub device_tags: Vec<String>,
    /// Exact devices that must be paused before the shipment begins. This is
    /// used by mining ward rebalancing so an old mining site is quiesced
    /// before its protection is removed.
    #[serde(default)]
    pub pre_deactivate_device_codes: Vec<String>,
    /// Remove mining-workflow reservation tags from exact payload devices
    /// after this manifest has claimed them. Used when reassigning a System
    /// Ward away from a retired or hub-protected mining site.
    #[serde(default)]
    pub release_mining_reservations: bool,
    /// Optional exact failed-custody metadata for Stranded Device Recovery.
    #[serde(default)]
    pub placement_recovery: Option<PlacementRecoveryMetadata>,
    /// Return borrowed transport carriers to their starting scope after delivery.
    #[serde(default)]
    pub return_transports: bool,
    /// Allow the transport planner to self-stage a free carrier from outside
    /// the origin system when no suitable local carrier is available.
    #[serde(default)]
    pub allow_transport_staging: bool,
    /// Optional regional ownership hint for observability/policy.
    #[serde(default)]
    pub region: Option<String>,
    /// Human-readable reason this manifest exists.
    #[serde(default)]
    pub purpose: String,
}

/// Durable state for one recovery manifest tag-cleanup operation.
///
/// The deterministic operation ID is written before invoking managed
/// configure, so a resumed executor can recreate or observe the original
/// mutation instead of issuing a second configure request.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PlacementRecoveryCleanup {
    /// Managed operation identity for the exact configure mutation.
    #[serde(default)]
    pub operation_id: Option<String>,
    /// Exact tags included in the configure request.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Last observed operation state, persisted for operator/restart evidence.
    #[serde(default)]
    pub state: Option<String>,
}

/// Restart-safe logistics checkpoint. The concrete plan is persisted before mutation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct LogisticsWorkflowCheckpoint {
    /// Concrete transport plan selected from managed state.
    pub plan: Option<DeliveryPlan>,
    /// Whether execution entered the reusable transport executor.
    pub started: bool,
    #[serde(default)]
    failure_class: Option<FailureClass>,
    /// Per-device recovery tag cleanup operation and its last observed state.
    #[serde(default)]
    pub placement_recovery_cleanup: BTreeMap<String, PlacementRecoveryCleanup>,
}

/// Intent for a fully provisioned buyer-side trade run.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TradeFulfillmentIntent {
    /// Trade-controller device code.
    pub controller: String,
    /// Stable trade code to fulfill once.
    pub trade_code: String,
    /// Exact shop location where the buyer must arrive.
    pub shop_location: String,
    /// Exact home hub where provisioning starts and all assets return.
    pub home: String,
    /// Optional Replicant to execute the trade.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Optional regional affinity used only for observability/policy.
    #[serde(default)]
    pub preferred_region: Option<String>,
    /// Optional device reward that must still be present when the trade is executed.
    #[serde(default)]
    pub expected_reward_device: Option<String>,
}

/// Restart-safe checkpoint for a provisioned buyer-side trade run.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct TradeFulfillmentCheckpoint {
    /// Resolved exact home hub.
    pub home: Option<String>,
    /// Resolved home system used for local provisioning.
    pub home_system: Option<String>,
    /// Replicant claimed for the run.
    pub replicant: Option<String>,
    /// Live criteria snapshot persisted before any staging.
    pub criteria: Option<TradeBundle>,
    /// Live reward snapshot persisted before any staging.
    pub rewards: Option<TradeBundle>,
    /// Durable tag used for criterion-device prints.
    pub payment_print_tag: Option<String>,
    /// Exact criterion devices selected for this purchase.
    pub payment_device_codes: Vec<String>,
    /// Child workflow that transports the still-missing payment manifest.
    pub payment_logistics_child: Option<WorkflowId>,
    /// Concrete outbound logistics plan retained after the child succeeds.
    pub outbound_plan: Option<DeliveryPlan>,
    /// Extra attachment/cargo escorts staged only to carry rewards home.
    pub escort_carriers: Vec<String>,
    /// Owned reward-device identities observed before purchase, keyed by device type.
    pub pre_purchase_devices: BTreeMap<String, Vec<String>>,
    /// Shop stock immediately before the irreversible purchase.
    pub pre_purchase_stock: Option<i64>,
    /// Irreversible purchase intent was durably checkpointed.
    pub purchase_authorized: bool,
    /// A managed trade operation was submitted during some invocation.
    pub purchase_submitted: bool,
    /// Durable managed operation ID for restart-safe trade recovery.
    pub purchase_operation: Option<String>,
    /// The purchase has positive evidence of completion and may be reconciled/returned.
    pub purchase_observed: bool,
    /// Reward devices acquired by this execution.
    pub reward_devices: Vec<String>,
    /// Reward-device transport mode keyed by device code (`stowed` or carrier code).
    pub reward_storage: BTreeMap<String, String>,
    /// Reward resources have been loaded into return cargo transports.
    pub reward_resources_loaded: bool,
    /// Buyer, transports, and rewards have all returned to the home hub.
    pub returned_home: bool,
}

/// Shop purchase selected for a blueprint acquisition.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BlueprintShopPurchaseIntent {
    /// Trade-controller device code.
    pub controller_code: String,
    /// Stable trade code to execute.
    pub trade_code: String,
    /// Exact shop location where criteria and a Replicant must be staged.
    pub shop_location: String,
    /// Shop system used for observability and affinity.
    pub shop_system: String,
    /// Live criteria observed when the Director selected this opportunity.
    pub criteria: TradeBundle,
}

/// Intent for learning one blueprint from an owned copy or a known shop.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct BlueprintAcquireIntent {
    /// Device type whose blueprint must become unlocked.
    pub device_type: String,
    /// Optional regional affinity used by Director selection and observability.
    #[serde(default)]
    pub preferred_region: Option<String>,
    /// Goal/requirement identities that requested this capability.
    #[serde(default)]
    pub requested_by: Vec<String>,
    /// Optional preselected owned sacrificial device.
    #[serde(default)]
    pub source_device: Option<String>,
    /// Optional preselected owned Autofactory code.
    #[serde(default)]
    pub autofactory: Option<String>,
    /// Optional Replicant selected for shop execution or remote-source control.
    #[serde(default)]
    pub acquisition_replicant: Option<String>,
    /// Optional live shop opportunity. When absent, use the owned-copy path.
    #[serde(default)]
    pub shop: Option<BlueprintShopPurchaseIntent>,
}

/// Restart-safe blueprint acquisition checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct BlueprintAcquireCheckpoint {
    /// Selected sacrificial device code.
    pub source_device: Option<String>,
    /// Selected owned Autofactory code.
    pub autofactory: Option<String>,
    /// Exact Autofactory location used as the decommission destination.
    pub autofactory_location: Option<String>,
    /// New reusable trade workflow used by shop-backed blueprint acquisition.
    pub trade_child: Option<WorkflowId>,
    /// Legacy child manifest that staged trade criteria before `trade.fulfillment`.
    pub criteria_logistics_child: Option<WorkflowId>,
    /// Child manifest that moves the selected physical device when necessary.
    pub logistics_child: Option<WorkflowId>,
    /// Replicant claimed to execute a shop trade or provide local control for
    /// an otherwise out-of-range owned blueprint source.
    pub acquisition_replicant: Option<String>,
    /// An owned source was selected while out of comms range and needs a
    /// Replicant escort before logistics can command it.
    pub control_escort_required: bool,
    /// Trade controller selected before any shop mutation.
    pub controller_code: Option<String>,
    /// Trade code selected before any shop mutation.
    pub trade_code: Option<String>,
    /// Exact shop location selected before any shop mutation.
    pub shop_location: Option<String>,
    /// Criteria snapshot checkpointed before staging.
    pub criteria: Option<TradeBundle>,
    /// Durable tag used for any criterion-device prints.
    pub criteria_print_tag: Option<String>,
    /// Target-device codes owned immediately before trade execution.
    pub pre_purchase_devices: Vec<String>,
    /// Irreversible purchase intent was durably checkpointed.
    pub purchase_authorized: bool,
    /// A managed trade operation was submitted during some invocation.
    pub purchase_submitted: bool,
    /// Durable managed operation ID for restart-safe trade recovery.
    pub purchase_operation: Option<String>,
    /// The rewarded target device has been observed in managed ownership.
    pub purchase_observed: bool,
    /// Irreversible decommission intent was durably checkpointed.
    pub decommission_authorized: bool,
    /// A managed decommission operation was submitted during some invocation.
    pub decommission_submitted: bool,
    /// Durable managed operation ID for restart-safe decommission recovery.
    pub decommission_operation: Option<String>,
    /// The account blueprint catalogue has observed the desired blueprint.
    pub blueprint_verified: bool,
}

/// Directed frontier-expansion intent.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExplorationIntent {
    /// System the relay network should reach.
    pub target: String,
    /// Optional replicant. When omitted the first owned replicant is selected deterministically.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Optional manufacturing hub. When omitted an owned autofactory location is selected.
    #[serde(default)]
    pub hub: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ExplorationTopologyBlocker {
    signature: String,
}

/// Authoritative exploration checkpoint. The old relay mission file is only an ephemeral adapter.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ExplorationWorkflowCheckpoint {
    /// Resolved replicant retained across restarts.
    pub replicant: Option<String>,
    /// Resolved manufacturing hub retained across restarts.
    pub hub: Option<String>,
    /// Last authoritative relay executor state.
    pub state: Option<RelayExecutionState>,
    #[serde(default)]
    failure_class: Option<FailureClass>,
    #[serde(default)]
    topology_blocker: Option<ExplorationTopologyBlocker>,
}

/// Goal-level event input shared by delivery and tour workflows.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventIntent {
    /// Event designation.
    pub event: String,
    /// Optional completion criterion.
    #[serde(default)]
    pub criterion: Option<String>,
    /// Optional replicant to use when the event is ultimately resolved.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Optional manufacturing/staging home.
    #[serde(default)]
    pub home: Option<String>,
}

/// Authoritative delivery checkpoint. `plan_json` replaces the GUI-facing mission file.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventDeliveryCheckpoint {
    /// Resolved replicant recorded by planning without reserving it for staging.
    pub replicant: Option<String>,
    /// Resolved manufacturing home.
    pub home: Option<String>,
    /// Serialized event plan at the last durable workflow boundary.
    pub plan_json: Option<String>,
    /// Whether all requirements are physically staged at the event.
    pub ready: bool,
    /// Relay-expansion workflow satisfying this event's disconnected destination.
    #[serde(default)]
    pub connectivity_workflows: BTreeMap<String, WorkflowId>,
    /// Whether the mission must be replanned once prerequisite connectivity lands.
    #[serde(default)]
    pub replan_after_connectivity: bool,
}

/// Parent event-tour checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventTourCheckpoint {
    /// Child delivery workflow created for this event.
    pub delivery_child: Option<WorkflowId>,
    /// Resolved replicant used for the final event visit.
    pub replicant: Option<String>,
    /// Final serialized plan snapshot.
    pub plan_json: Option<String>,
}
/// Regional event-completion campaign intent.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventCampaignIntent {
    /// Catalogue region whose active events should be planned as one campaign.
    pub region: String,
    /// Regional manufacturing and staging home.
    pub home: String,
}

#[derive(Clone, Debug, Deserialize)]
struct LegacyEventCampaignIntent {
    region: String,
    #[serde(default)]
    replicant: Option<String>,
    #[serde(default)]
    home: Option<String>,
}

/// Durable regional event campaign checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventCampaignCheckpoint {
    /// Resolved regional replicant.
    pub replicant: Option<String>,
    /// Resolved regional home.
    pub home: Option<String>,
    /// Authoritative archive of campaign and child mission compatibility files.
    pub archive: Option<EventCampaignArchive>,
    /// Relay-expansion workflow currently satisfying each disconnected event system.
    #[serde(default)]
    pub connectivity_workflows: BTreeMap<String, WorkflowId>,
    /// A connectivity dependency changed the world after this campaign was planned.
    /// Replanning prevents hours-old event/inventory assumptions from being executed.
    #[serde(default)]
    pub replan_after_connectivity: bool,
}

/// Intent for creating one additional regional Replicant.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicantProvisionIntent {
    /// Region that will permanently own the new worker.
    pub region: String,
    /// Manufacturing/staging location shared with the source Replicant.
    pub home: String,
    /// Existing Replicant used as the replication source.
    pub source_replicant: String,
    /// Cradle vessel type to manufacture for the new worker.
    #[serde(default = "default_worker_cradle")]
    pub cradle_type: String,
    /// Optional explicit display name for the new Replicant.
    #[serde(default)]
    pub name: Option<String>,
}

/// Restart-safe workforce provisioning checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ReplicantProvisionCheckpoint {
    /// Unique manufacturing tag for this provisioning request.
    pub tag: Option<String>,
    /// Durable direct-output manufacturing intents.
    #[serde(default)]
    pub manufacturing: Option<ReplicantManufacturingCheckpoint>,
    /// Printed empty matrix code.
    pub matrix: Option<String>,
    /// Printed cradle vessel code.
    pub cradle: Option<String>,
    /// Whether the target matrix has been stowed into its cradle.
    pub stowed: bool,
    /// New Replicant code after successful replication.
    pub new_replicant: Option<String>,
}

/// Durable manufacturing state for the two provisioning outputs.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicantManufacturingCheckpoint {
    /// Empty Replicant matrix print intent.
    pub matrix: ReplicantPrintIntent,
    /// Cradle vessel print intent.
    pub cradle: ReplicantPrintIntent,
}

/// One durably tracked provisioning print submission.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ReplicantPrintIntent {
    /// Immutable requested device type.
    pub device_type: String,
    /// Deterministic tag identifying this output within the workflow.
    pub job_tag: String,
    /// Autofactory selected immediately before submission.
    pub factory_code: Option<String>,
    /// Whether the external-submission boundary has been crossed.
    #[serde(default)]
    pub submission_started: bool,
    /// Whether operation or queue evidence established acceptance.
    #[serde(default)]
    pub accepted: bool,
    /// Managed operation identity, when the submission response was recorded.
    #[serde(default)]
    pub operation_id: Option<String>,
}

/// Goal-level intent for establishing one newly discovered region.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RegionEstablishIntent {
    /// Canonical target region.
    pub region: String,
    /// Known landing star in that region.
    pub landing_star: String,
    /// Existing source manufacturing hub.
    pub source_hub: String,
    /// Replicant assigned as regional operator.
    pub operator: String,
    /// Replicant assigned as regional explorer.
    pub explorer: String,
}

/// Durable regional bootstrap adapter checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RegionEstablishCheckpoint {
    /// Latest serialized parent bootstrap mission.
    pub mission_json: Option<String>,
}

/// Optional observatory pin for automatic prospecting.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ObservatoryIntent {
    /// Observatory device code. Omit to let the runtime choose an eligible observatory.
    #[serde(default)]
    pub observatory: Option<String>,
}
fn placement_device(code: impl AsRef<str>) -> Option<WorkflowPlacementIntentSubject> {
    let code = code.as_ref().trim();
    (!code.is_empty()).then(|| WorkflowPlacementIntentSubject::Device(code.to_ascii_uppercase()))
}

fn placement_tag(tag: impl AsRef<str>) -> Option<WorkflowPlacementIntentSubject> {
    let tag = tag.as_ref();
    (!tag.is_empty()).then(|| WorkflowPlacementIntentSubject::DeviceTag(tag.to_owned()))
}

fn placement_intent(
    subject: WorkflowPlacementIntentSubject,
    relation: WorkflowPlacementIntentRelation,
    work_item_id: Option<replicant_workflow::WorkItemId>,
    expected_location: Option<String>,
) -> WorkflowPlacementIntent {
    WorkflowPlacementIntent {
        subject,
        relation,
        work_item_id,
        expected_location: (relation == WorkflowPlacementIntentRelation::Deployed)
            .then_some(expected_location)
            .flatten(),
    }
}

fn complete_placement_projection(
    intents: impl IntoIterator<Item = WorkflowPlacementIntent>,
) -> WorkflowPlacementIntentProjection {
    let mut intents = intents.into_iter().collect::<Vec<_>>();
    intents.sort();
    intents.dedup();
    WorkflowPlacementIntentProjection {
        coverage: WorkflowPlacementIntentCoverage::Complete,
        intents,
        resolutions: Vec::new(),
    }
}
fn bundle_has_unknown(bundle: Option<&crate::trade::TradeBundle>) -> bool {
    bundle.is_some_and(|bundle| !bundle.unknown.is_empty())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PlacementProjectionPhase {
    Live,
    Succeeded,
    Failed,
    Cancelled,
}

fn placement_projection_phase(status: WorkflowStatus) -> PlacementProjectionPhase {
    match status {
        WorkflowStatus::Queued
        | WorkflowStatus::Running
        | WorkflowStatus::Waiting
        | WorkflowStatus::Reconciling
        | WorkflowStatus::Paused => PlacementProjectionPhase::Live,
        WorkflowStatus::Succeeded => PlacementProjectionPhase::Succeeded,
        WorkflowStatus::Failed => PlacementProjectionPhase::Failed,
        WorkflowStatus::Cancelled => PlacementProjectionPhase::Cancelled,
    }
}

fn workflow_placement_projection(
    instance: &replicant_workflow::WorkflowInstance,
    work_items: &[WorkItem],
    config_refs: impl FnOnce(&mut Vec<WorkflowPlacementIntent>),
    checkpoint_refs: impl FnOnce(PlacementProjectionPhase, &mut Vec<WorkflowPlacementIntent>),
) -> Result<WorkflowPlacementIntentProjection, String> {
    if !work_items.is_empty() {
        return Err("automation work-item schema is not typed by this factory".into());
    }
    let mut intents = Vec::new();
    let phase = placement_projection_phase(instance.status);
    if phase == PlacementProjectionPhase::Live {
        config_refs(&mut intents);
    }
    checkpoint_refs(phase, &mut intents);
    Ok(complete_placement_projection(intents))
}

fn scan_system_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: ScanIntent = instance.config().map_err(string_error)?;
    let checkpoint: ControllerWorkflowCheckpoint = instance.checkpoint().map_err(string_error)?;
    workflow_placement_projection(
        instance,
        items,
        |intents| {
            if let Some(code) = config.controller.as_deref().and_then(placement_device) {
                intents.push(placement_intent(
                    code,
                    WorkflowPlacementIntentRelation::Claimed,
                    None,
                    None,
                ));
            }
        },
        |phase, intents| {
            // A selected controller is not custody.  Only a durable directive
            // boundary proves that a terminal workflow touched it.
            if (phase == PlacementProjectionPhase::Live
                || checkpoint.directive_set
                || checkpoint.launched
                || checkpoint.observed_active)
                && let Some(code) = checkpoint.controller.as_deref().and_then(placement_device)
            {
                intents.push(placement_intent(
                    code,
                    WorkflowPlacementIntentRelation::Claimed,
                    None,
                    None,
                ));
            }
        },
    )
}

fn scan_tour_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: ScanTourIntent = instance.config().map_err(string_error)?;
    let checkpoint: ScanTourCheckpoint = instance.checkpoint().map_err(string_error)?;
    workflow_placement_projection(
        instance,
        items,
        |intents| {
            if let Some(subject) = config.vessel.as_deref().and_then(placement_device) {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Claimed,
                    None,
                    None,
                ));
            }
        },
        |phase, intents| {
            let started = checkpoint.state.is_some() || checkpoint.fleet_logistics_child.is_some();
            if phase != PlacementProjectionPhase::Live && !started {
                return;
            }
            for code in checkpoint
                .vessel
                .iter()
                .chain(checkpoint.fleet_controller.iter())
                .chain(checkpoint.fleet_drones.iter())
            {
                if let Some(subject) = placement_device(code) {
                    intents.push(placement_intent(
                        subject,
                        WorkflowPlacementIntentRelation::Staged,
                        None,
                        None,
                    ));
                }
            }
            if let Some(subject) = checkpoint
                .fleet_print_tag
                .as_deref()
                .and_then(placement_tag)
            {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Staged,
                    None,
                    None,
                ));
            }
        },
    )
}

fn salvage_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: SalvageIntent = instance.config().map_err(string_error)?;
    let checkpoint: ControllerWorkflowCheckpoint = instance.checkpoint().map_err(string_error)?;
    workflow_placement_projection(
        instance,
        items,
        |intents| {
            if let Some(subject) = config.controller.as_deref().and_then(placement_device) {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Claimed,
                    None,
                    None,
                ));
            }
        },
        |phase, intents| {
            if (phase == PlacementProjectionPhase::Live
                || checkpoint.directive_set
                || checkpoint.launched
                || checkpoint.observed_active)
                && let Some(subject) = checkpoint.controller.as_deref().and_then(placement_device)
            {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Staged,
                    None,
                    None,
                ));
            }
        },
    )
}

fn salvage_recovery_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let _: SalvageRecoveryIntent = instance.config().map_err(string_error)?;
    let _: SalvageRecoveryCheckpoint = instance.checkpoint().map_err(string_error)?;
    workflow_placement_projection(instance, items, |_| {}, |_, _| {})
}

fn mining_deploy_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: MiningDeployIntent = instance.config().map_err(string_error)?;
    let checkpoint: MiningDeployCheckpoint = instance.checkpoint().map_err(string_error)?;
    if checkpoint.plan_json.is_some() {
        return Err("legacy mining mission state is opaque".into());
    }
    workflow_placement_projection(
        instance,
        items,
        |intents| {
            if let Some(subject) = config.hub.as_deref().and_then(placement_device) {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Claimed,
                    None,
                    None,
                ));
            }
        },
        |phase, intents| {
            if (phase == PlacementProjectionPhase::Live || checkpoint.started)
                && let Some(subject) = checkpoint.hub.as_deref().and_then(placement_device)
            {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Staged,
                    None,
                    None,
                ));
            }
        },
    )
}

fn mining_campaign_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    if !matches!(instance.schema_version, 2 | 3) {
        return Err("legacy mining campaign state is opaque".into());
    }
    let config: MiningCampaignIntent = instance.config().map_err(string_error)?;
    let checkpoint: MiningCampaignCheckpoint = instance.checkpoint().map_err(string_error)?;
    workflow_placement_projection(
        instance,
        items,
        |intents| {
            if let Some(subject) = placement_device(&config.hub) {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Claimed,
                    None,
                    None,
                ));
            }
        },
        |phase, intents| {
            let terminal = phase != PlacementProjectionPhase::Live;
            let Some(mission) = checkpoint.mission.as_ref() else {
                return;
            };
            let has_custody = checkpoint.started
                && (mission.print_batches.iter().any(|batch| {
                    batch.submission_started || batch.submitted || !batch.produced_codes.is_empty()
                }) || mission.sites.iter().any(|site| {
                    matches!(
                        site.phase,
                        crate::mining::SitePhase::Outbound
                            | crate::mining::SitePhase::Deploying
                            | crate::mining::SitePhase::Adopting
                            | crate::mining::SitePhase::Verifying
                            | crate::mining::SitePhase::Configuring
                    )
                }) || mission.routes.iter().any(|route| {
                    matches!(
                        route.phase,
                        crate::mining::RoutePhase::Activating | crate::mining::RoutePhase::Active
                    )
                }));
            if terminal && !has_custody {
                return;
            }
            if let Some(subject) = placement_tag(&mission.mission_tag) {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Staged,
                    None,
                    None,
                ));
            }
            for batch in &mission.print_batches {
                let batch_started = !terminal
                    || batch.submission_started
                    || batch.submitted
                    || !batch.produced_codes.is_empty();
                if !batch_started {
                    continue;
                }
                if let Some(subject) = placement_device(&batch.factory_code) {
                    intents.push(placement_intent(
                        subject,
                        WorkflowPlacementIntentRelation::Claimed,
                        None,
                        None,
                    ));
                }
                if batch.submitted || !batch.produced_codes.is_empty() {
                    if let Some(subject) = placement_tag(&batch.batch_tag) {
                        intents.push(placement_intent(
                            subject,
                            WorkflowPlacementIntentRelation::Staged,
                            None,
                            None,
                        ));
                    }
                    for code in &batch.produced_codes {
                        if let Some(subject) = placement_device(code) {
                            intents.push(placement_intent(
                                subject,
                                WorkflowPlacementIntentRelation::Staged,
                                None,
                                None,
                            ));
                        }
                    }
                }
            }
            for site in &mission.sites {
                let relation = match site.phase {
                    crate::mining::SitePhase::Planned | crate::mining::SitePhase::Ready => continue,
                    crate::mining::SitePhase::Operational
                        if phase == PlacementProjectionPhase::Live
                            || phase == PlacementProjectionPhase::Succeeded =>
                    {
                        WorkflowPlacementIntentRelation::Deployed
                    }
                    crate::mining::SitePhase::Operational => continue,
                    _ => WorkflowPlacementIntentRelation::Transported,
                };
                let location = (relation == WorkflowPlacementIntentRelation::Deployed)
                    .then(|| site.system.clone());
                if let Some(subject) = placement_tag(&site.tag) {
                    intents.push(placement_intent(subject, relation, None, location.clone()));
                }
                for code in site
                    .assets
                    .mining_controller
                    .iter()
                    .chain(site.assets.mining_drones.iter())
                    .chain(site.assets.survey_controller.iter())
                    .chain(site.assets.survey_drones.iter())
                    .chain(site.assets.maintenance_drone.iter())
                    .chain(site.assets.system_ward.iter())
                    .chain(site.carrier.iter())
                {
                    if let Some(subject) = placement_device(code) {
                        intents.push(placement_intent(subject, relation, None, location.clone()));
                    }
                }
            }
            for route in &mission.routes {
                let relation = match route.phase {
                    crate::mining::RoutePhase::Planned | crate::mining::RoutePhase::Ready => {
                        continue;
                    }
                    _ => WorkflowPlacementIntentRelation::Transported,
                };
                if let Some(subject) = placement_tag(&route.tag) {
                    intents.push(placement_intent(subject, relation, None, None));
                }
                for code in route.controller.iter().chain(route.freighter.iter()) {
                    if let Some(subject) = placement_device(code) {
                        intents.push(placement_intent(subject, relation, None, None));
                    }
                }
            }
        },
    )
}

fn regional_dispatch_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: RegionalDispatchIntent = instance.config().map_err(string_error)?;
    let checkpoint: RegionalDispatchCheckpoint = instance.checkpoint().map_err(string_error)?;
    workflow_placement_projection(
        instance,
        items,
        |_| {},
        |phase, intents| {
            let delivered = phase == PlacementProjectionPhase::Succeeded;
            let relation = if delivered {
                WorkflowPlacementIntentRelation::Deployed
            } else if checkpoint.delivery_started {
                WorkflowPlacementIntentRelation::Transported
            } else {
                WorkflowPlacementIntentRelation::Staged
            };
            for code in checkpoint.vessels.iter().chain(checkpoint.devices.iter()) {
                if let Some(subject) = placement_device(code) {
                    intents.push(placement_intent(
                        subject,
                        relation,
                        None,
                        delivered.then(|| config.destination.clone()),
                    ));
                }
            }
        },
    )
}

fn logistics_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: LogisticsIntent = instance.config().map_err(string_error)?;
    let checkpoint: LogisticsWorkflowCheckpoint = instance.checkpoint().map_err(string_error)?;
    let plan = checkpoint.plan.as_ref();
    let delivered = if instance.status == WorkflowStatus::Succeeded {
        instance
            .result::<DeliveryReport>()
            .map_err(string_error)?
            .map(|report| {
                report
                    .devices_delivered
                    .into_iter()
                    .collect::<BTreeSet<_>>()
            })
    } else {
        None
    };
    workflow_placement_projection(
        instance,
        items,
        |intents| {
            for tag in &config.device_tags {
                if let Some(subject) = placement_tag(tag) {
                    intents.push(placement_intent(
                        subject,
                        WorkflowPlacementIntentRelation::Claimed,
                        None,
                        None,
                    ));
                }
            }
        },
        |phase, intents| {
            if phase != PlacementProjectionPhase::Live && !checkpoint.started {
                return;
            }
            let Some(plan) = plan else { return };
            for payload in &plan.payload_devices {
                if let Some(subject) = placement_device(&payload.code) {
                    let deployed = delivered
                        .as_ref()
                        .is_some_and(|codes| codes.contains(&payload.code));
                    let relation = if deployed {
                        WorkflowPlacementIntentRelation::Deployed
                    } else if checkpoint.started {
                        WorkflowPlacementIntentRelation::Transported
                    } else {
                        WorkflowPlacementIntentRelation::Staged
                    };
                    intents.push(placement_intent(
                        subject,
                        relation,
                        None,
                        deployed.then(|| plan.destination.clone()),
                    ));
                }
            }
            for code in &plan.device_carriers {
                if let Some(subject) = placement_device(code) {
                    intents.push(placement_intent(
                        subject,
                        WorkflowPlacementIntentRelation::Claimed,
                        None,
                        None,
                    ));
                }
            }
        },
    )
}

fn logistics_manifest_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: LogisticsManifestIntent = instance.config().map_err(string_error)?;
    if config.placement_recovery.is_some() && validate_placement_recovery_intent(&config).is_err() {
        return Ok(WorkflowPlacementIntentProjection::unknown());
    }
    let checkpoint: LogisticsWorkflowCheckpoint = instance.checkpoint().map_err(string_error)?;
    let plan = checkpoint.plan.as_ref();
    let delivered = if instance.status == WorkflowStatus::Succeeded {
        let result = instance.result::<DeliveryReport>();
        if config.placement_recovery.is_some() {
            let Ok(Some(report)) = result else {
                // A successful manifest without an unambiguous delivery
                // report cannot resolve failed custody.
                return Ok(WorkflowPlacementIntentProjection::unknown());
            };
            let delivered = report
                .devices_delivered
                .into_iter()
                .map(|code| canonical_manifest_device_code(&code))
                .collect::<BTreeSet<_>>();
            let expected = config
                .device_codes
                .iter()
                .map(|code| canonical_manifest_device_code(code))
                .collect::<BTreeSet<_>>();
            let Some(plan) = plan else {
                // A recovery manifest that was marked successful without a
                // durable concrete plan cannot establish where its payload
                // was delivered. Do not resolve failed custody from its
                // report alone.
                return Ok(WorkflowPlacementIntentProjection::unknown());
            };
            let planned = plan
                .payload_devices
                .iter()
                .map(|payload| canonical_manifest_device_code(&payload.code))
                .collect::<BTreeSet<_>>();
            if delivered != expected
                || planned != expected
                || plan.destination != config.destination
                || !checkpoint.started
            {
                // Recovery resolution is all-or-nothing: every exact code
                // must be in the persisted payload plan and in the delivery
                // report for the manifest's exact destination.
                return Ok(WorkflowPlacementIntentProjection::unknown());
            }
            Some(delivered)
        } else {
            result.map_err(string_error)?.map(|report| {
                report
                    .devices_delivered
                    .into_iter()
                    .collect::<BTreeSet<_>>()
            })
        }
    } else {
        None
    };
    let mut projection = workflow_placement_projection(
        instance,
        items,
        |intents| {
            for code in config
                .device_codes
                .iter()
                .chain(config.pre_deactivate_device_codes.iter())
            {
                if let Some(subject) = placement_device(code) {
                    intents.push(placement_intent(
                        subject,
                        WorkflowPlacementIntentRelation::Claimed,
                        None,
                        None,
                    ));
                }
            }
            for tag in &config.device_tags {
                if let Some(subject) = placement_tag(tag) {
                    intents.push(placement_intent(
                        subject,
                        WorkflowPlacementIntentRelation::Claimed,
                        None,
                        None,
                    ));
                }
            }
            if let Some(metadata) = config.placement_recovery.as_ref() {
                for (code, tags) in &metadata.release_device_tags {
                    let cleanup_applied = checkpoint
                        .placement_recovery_cleanup
                        .get(code)
                        .and_then(|cleanup| cleanup.state.as_deref())
                        .is_some_and(|state| state == "completed" || state == "absent");
                    if cleanup_applied {
                        continue;
                    }
                    for tag in tags {
                        if let Some(subject) = placement_tag(tag) {
                            intents.push(placement_intent(
                                subject,
                                WorkflowPlacementIntentRelation::Claimed,
                                None,
                                None,
                            ));
                        }
                    }
                }
            }
        },
        |phase, intents| {
            if matches!(
                phase,
                PlacementProjectionPhase::Failed | PlacementProjectionPhase::Cancelled
            ) && checkpoint.started
            {
                for code in config
                    .device_codes
                    .iter()
                    .chain(config.pre_deactivate_device_codes.iter())
                {
                    if let Some(subject) = placement_device(code) {
                        intents.push(placement_intent(
                            subject,
                            WorkflowPlacementIntentRelation::Claimed,
                            None,
                            None,
                        ));
                    }
                }
                for tag in &config.device_tags {
                    if let Some(subject) = placement_tag(tag) {
                        intents.push(placement_intent(
                            subject,
                            WorkflowPlacementIntentRelation::Claimed,
                            None,
                            None,
                        ));
                    }
                }
            }
            if phase != PlacementProjectionPhase::Live && !checkpoint.started {
                return;
            }
            let Some(plan) = plan else { return };
            for payload in &plan.payload_devices {
                if let Some(subject) = placement_device(&payload.code) {
                    let deployed = delivered.as_ref().is_some_and(|codes| {
                        codes.contains(&canonical_manifest_device_code(&payload.code))
                    });
                    let relation = if deployed {
                        WorkflowPlacementIntentRelation::Deployed
                    } else if checkpoint.started {
                        WorkflowPlacementIntentRelation::Transported
                    } else {
                        WorkflowPlacementIntentRelation::Staged
                    };
                    intents.push(placement_intent(
                        subject,
                        relation,
                        None,
                        deployed.then(|| plan.destination.clone()),
                    ));
                }
            }
            for code in &plan.device_carriers {
                if let Some(subject) = placement_device(code) {
                    intents.push(placement_intent(
                        subject,
                        WorkflowPlacementIntentRelation::Claimed,
                        None,
                        None,
                    ));
                }
            }
        },
    )?;
    if instance.status == WorkflowStatus::Succeeded
        && let Some(metadata) = config.placement_recovery.as_ref()
    {
        let Some(delivered) = delivered.as_ref() else {
            return Ok(WorkflowPlacementIntentProjection::unknown());
        };
        projection.resolutions = metadata
            .placement_resolutions
            .iter()
            .filter(|resolution| delivered.contains(&resolution.device_code))
            .cloned()
            .collect();
    }
    Ok(projection)
}

fn trade_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: TradeFulfillmentIntent = instance.config().map_err(string_error)?;
    let checkpoint: TradeFulfillmentCheckpoint = instance.checkpoint().map_err(string_error)?;
    if bundle_has_unknown(checkpoint.criteria.as_ref())
        || bundle_has_unknown(checkpoint.rewards.as_ref())
    {
        return Err("opaque trade bundle fields".into());
    }
    let terminal_started = checkpoint.purchase_submitted
        || checkpoint.purchase_observed
        || checkpoint.reward_resources_loaded
        || checkpoint.returned_home;
    workflow_placement_projection(
        instance,
        items,
        |intents| {
            for code in
                std::iter::once(&config.controller).chain(config.expected_reward_device.iter())
            {
                if let Some(subject) = placement_device(code) {
                    intents.push(placement_intent(
                        subject,
                        WorkflowPlacementIntentRelation::Claimed,
                        None,
                        None,
                    ));
                }
            }
        },
        |phase, intents| {
            if phase != PlacementProjectionPhase::Live && !terminal_started {
                return;
            }
            for code in checkpoint
                .payment_device_codes
                .iter()
                .chain(checkpoint.escort_carriers.iter())
                .chain(checkpoint.reward_devices.iter())
                .chain(checkpoint.reward_storage.keys())
            {
                if let Some(subject) = placement_device(code) {
                    let deployed = checkpoint.returned_home
                        && checkpoint
                            .reward_devices
                            .iter()
                            .any(|reward| reward == code);
                    intents.push(placement_intent(
                        subject,
                        if deployed {
                            WorkflowPlacementIntentRelation::Deployed
                        } else {
                            WorkflowPlacementIntentRelation::Staged
                        },
                        None,
                        deployed.then(|| config.home.clone()),
                    ));
                }
            }
            if let Some(tag) = checkpoint
                .payment_print_tag
                .as_deref()
                .and_then(placement_tag)
            {
                intents.push(placement_intent(
                    tag,
                    WorkflowPlacementIntentRelation::Staged,
                    None,
                    None,
                ));
            }
        },
    )
}

fn blueprint_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: BlueprintAcquireIntent = instance.config().map_err(string_error)?;
    let checkpoint: BlueprintAcquireCheckpoint = instance.checkpoint().map_err(string_error)?;
    let terminal_started = checkpoint.purchase_submitted
        || checkpoint.purchase_observed
        || checkpoint.decommission_submitted
        || checkpoint.blueprint_verified;
    workflow_placement_projection(
        instance,
        items,
        |intents| {
            for code in config
                .source_device
                .iter()
                .chain(config.autofactory.iter())
                .chain(config.acquisition_replicant.iter())
                .chain(config.shop.iter().map(|shop| &shop.controller_code))
            {
                if let Some(subject) = placement_device(code) {
                    intents.push(placement_intent(
                        subject,
                        WorkflowPlacementIntentRelation::Claimed,
                        None,
                        None,
                    ));
                }
            }
        },
        |phase, intents| {
            if phase != PlacementProjectionPhase::Live && !terminal_started {
                return;
            }
            for code in checkpoint
                .source_device
                .iter()
                .chain(checkpoint.autofactory.iter())
                .chain(checkpoint.acquisition_replicant.iter())
                .chain(checkpoint.controller_code.iter())
                .chain(checkpoint.pre_purchase_devices.iter())
            {
                if let Some(subject) = placement_device(code) {
                    intents.push(placement_intent(
                        subject,
                        WorkflowPlacementIntentRelation::Staged,
                        None,
                        None,
                    ));
                }
            }
            if let Some(tag) = checkpoint
                .criteria_print_tag
                .as_deref()
                .and_then(placement_tag)
            {
                intents.push(placement_intent(
                    tag,
                    WorkflowPlacementIntentRelation::Staged,
                    None,
                    None,
                ));
            }
        },
    )
}

fn exploration_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: ExplorationIntent = instance.config().map_err(string_error)?;
    let checkpoint: ExplorationWorkflowCheckpoint = instance.checkpoint().map_err(string_error)?;
    workflow_placement_projection(
        instance,
        items,
        |intents| {
            if let Some(subject) = config.hub.as_deref().and_then(placement_device) {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Claimed,
                    None,
                    None,
                ));
            }
        },
        |phase, intents| {
            if (phase == PlacementProjectionPhase::Live || checkpoint.state.is_some())
                && let Some(subject) = checkpoint.hub.as_deref().and_then(placement_device)
            {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Staged,
                    None,
                    None,
                ));
            }
        },
    )
}

fn event_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let _: EventIntent = instance.config().map_err(string_error)?;
    let checkpoint: EventDeliveryCheckpoint = instance.checkpoint().map_err(string_error)?;
    if checkpoint.plan_json.is_some() {
        return Err("opaque event mission state".into());
    }
    workflow_placement_projection(instance, items, |_| {}, |_, _| {})
}

fn event_tour_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let _: EventIntent = instance.config().map_err(string_error)?;
    let checkpoint: EventTourCheckpoint = instance.checkpoint().map_err(string_error)?;
    if checkpoint.plan_json.is_some() {
        return Err("opaque event mission state".into());
    }
    workflow_placement_projection(instance, items, |_| {}, |_, _| {})
}

fn event_campaign_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let _: EventCampaignIntent = instance.config().map_err(string_error)?;
    let checkpoint: EventCampaignCheckpoint = instance.checkpoint().map_err(string_error)?;
    if checkpoint.archive.is_some() {
        return Err("opaque event campaign archive".into());
    }
    workflow_placement_projection(instance, items, |_| {}, |_, _| {})
}

fn observatory_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: ObservatoryIntent = instance.config().map_err(string_error)?;
    let checkpoint: Value = instance.checkpoint().map_err(string_error)?;
    if !checkpoint.is_null() {
        return Err("opaque observatory checkpoint".into());
    }
    workflow_placement_projection(
        instance,
        items,
        |intents| {
            if let Some(subject) = config.observatory.as_deref().and_then(placement_device) {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Claimed,
                    None,
                    None,
                ));
            }
        },
        |_, _| {},
    )
}

fn provision_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let _: ReplicantProvisionIntent = instance.config().map_err(string_error)?;
    let checkpoint: ReplicantProvisionCheckpoint = instance.checkpoint().map_err(string_error)?;
    workflow_placement_projection(
        instance,
        items,
        |_| {},
        |phase, intents| {
            if phase != PlacementProjectionPhase::Live && !checkpoint.stowed {
                return;
            }
            for code in checkpoint.matrix.iter().chain(checkpoint.cradle.iter()) {
                if let Some(subject) = placement_device(code) {
                    intents.push(placement_intent(
                        subject,
                        WorkflowPlacementIntentRelation::Staged,
                        None,
                        None,
                    ));
                }
            }
            if let Some(tag) = checkpoint.tag.as_deref().and_then(placement_tag) {
                intents.push(placement_intent(
                    tag,
                    WorkflowPlacementIntentRelation::Staged,
                    None,
                    None,
                ));
            }
        },
    )
}

fn region_establish_placement(
    instance: &replicant_workflow::WorkflowInstance,
    items: &[WorkItem],
) -> Result<WorkflowPlacementIntentProjection, String> {
    let config: RegionEstablishIntent = instance.config().map_err(string_error)?;
    let checkpoint: RegionEstablishCheckpoint = instance.checkpoint().map_err(string_error)?;
    if checkpoint.mission_json.is_some() {
        return Err("opaque bootstrap mission state".into());
    }
    workflow_placement_projection(
        instance,
        items,
        |intents| {
            if let Some(subject) = placement_device(&config.source_hub) {
                intents.push(placement_intent(
                    subject,
                    WorkflowPlacementIntentRelation::Claimed,
                    None,
                    None,
                ));
            }
        },
        |_, _| {},
    )
}

fn service_intents_for_factory(
    kind: &str,
    instance: &replicant_workflow::WorkflowInstance,
) -> Result<WorkflowServiceIntentProjection, String> {
    match kind {
        "mining.deploy" => {
            let intent: MiningDeployIntent = instance.config().map_err(string_error)?;
            let checkpoint: MiningDeployCheckpoint = instance.checkpoint().map_err(string_error)?;
            if let Some(plan) = checkpoint.plan_json.as_deref()
                && let Ok(mission) = serde_json::from_str::<MiningMission>(plan)
                && !mission.routes.is_empty()
            {
                let destination = mission.hub_location;
                return Ok(WorkflowServiceIntentProjection::complete(
                    mission
                        .routes
                        .into_iter()
                        .map(|route| {
                            AmiTransportRouteIntent {
                                system: route.system,
                                collect: route.belt,
                                deliver: destination.clone(),
                            }
                            .workflow_service_intent()
                        })
                        .collect(),
                ));
            }
            Ok(WorkflowServiceIntentProjection::unknown([
                WorkflowServiceScope::System(intent.system.trim().to_ascii_uppercase()),
            ]))
        }
        "region.establish" => {
            let intent: RegionEstablishIntent = instance.config().map_err(string_error)?;
            Ok(WorkflowServiceIntentProjection::unknown([
                WorkflowServiceScope::Region(canonical_region(&intent.region)),
            ]))
        }
        _ => Ok(WorkflowServiceIntentProjection::not_applicable()),
    }
}

macro_rules! workflow_factory {
    ($name:ident, $executor:ident, $kind_fn:ident, $projector:ident) => {
        /// Registered factory for this intent-native workflow.
        pub struct $name(WorkflowKind);

        impl $name {
            /// Creates the stable factory.
            #[must_use]
            pub fn new() -> Self {
                Self($kind_fn())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl WorkflowFactory for $name {
            fn kind(&self) -> &WorkflowKind {
                &self.0
            }

            fn current_schema_version(&self) -> u32 {
                SCHEMA_VERSION
            }

            fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
                Some(Box::new($executor))
            }

            fn placement_intents(
                &self,
                instance: &replicant_workflow::WorkflowInstance,
                work_items: &[WorkItem],
            ) -> Result<WorkflowPlacementIntentProjection, String> {
                if instance.schema_version != SCHEMA_VERSION {
                    return Err("unsupported automation schema is opaque".into());
                }
                $projector(instance, work_items)
            }
            fn service_intents(
                &self,
                instance: &replicant_workflow::WorkflowInstance,
            ) -> Result<WorkflowServiceIntentProjection, String> {
                service_intents_for_factory(self.kind().as_str(), instance)
            }
        }
    };
}

workflow_factory!(
    ScanSystemWorkflowFactory,
    ScanSystemWorkflow,
    scan_system_workflow_kind,
    scan_system_placement
);
workflow_factory!(
    ScanBeltWorkflowFactory,
    ScanBeltWorkflow,
    scan_belt_workflow_kind,
    scan_system_placement
);
workflow_factory!(
    ScanTourWorkflowFactory,
    ScanTourWorkflow,
    scan_tour_workflow_kind,
    scan_tour_placement
);

/// Schema-versioned factory for pooled belt-search campaigns.
pub struct BeltSearchCampaignWorkflowFactory(WorkflowKind);

impl BeltSearchCampaignWorkflowFactory {
    /// Creates the stable pooled campaign factory.
    #[must_use]
    pub fn new() -> Self {
        Self(belt_search_campaign_workflow_kind())
    }
}

impl Default for BeltSearchCampaignWorkflowFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowFactory for BeltSearchCampaignWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.0
    }

    fn current_schema_version(&self) -> u32 {
        2
    }

    fn supports_schema_version(&self, version: u32) -> bool {
        matches!(version, 1 | 2)
    }

    fn migrate(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
    ) -> Result<Option<WorkflowMigration>, String> {
        if instance.schema_version == 2 {
            return Ok(None);
        }
        let config = instance
            .config::<Value>()
            .map_err(|error| error.to_string())?;
        let systems = config
            .get("systems")
            .cloned()
            .unwrap_or_else(|| Value::Array(Vec::new()));
        let region = config
            .get("region")
            .cloned()
            .unwrap_or_else(|| Value::String(String::new()));
        let checkpoint = instance
            .checkpoint::<Value>()
            .map_err(|error| error.to_string())?;
        Ok(Some(WorkflowMigration::new(
            serde_json::json!({ "systems": systems, "region": region }),
            serde_json::json!({ "legacy_checkpoint": checkpoint }),
        )))
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(BeltSearchCampaignWorkflow))
    }

    fn placement_intents(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
        work_items: &[WorkItem],
    ) -> Result<WorkflowPlacementIntentProjection, String> {
        if instance.schema_version != 2 {
            return Err("legacy belt-search state is opaque".into());
        }
        let _: BeltSearchCampaignIntent = instance.config().map_err(string_error)?;
        let checkpoint: BeltSearchCampaignCheckpoint =
            instance.checkpoint().map_err(string_error)?;
        if checkpoint.legacy_checkpoint.is_some() {
            return Err("legacy belt-search checkpoint is opaque".into());
        }
        workflow_placement_projection(instance, work_items, |_| {}, |_, _| {})
    }
}
workflow_factory!(
    SalvageWorkflowFactory,
    SalvageWorkflow,
    salvage_workflow_kind,
    salvage_placement
);
workflow_factory!(
    MiningDeployWorkflowFactory,
    MiningDeployWorkflow,
    mining_deploy_workflow_kind,
    mining_deploy_placement
);
/// Factory for schema-version-three pooled regional mining campaigns.
pub struct MiningCampaignWorkflowFactory {
    kind: WorkflowKind,
    item_executor: Arc<dyn crate::workflows::MiningItemExecutor>,
}

impl MiningCampaignWorkflowFactory {
    fn new() -> Self {
        Self {
            kind: mining_campaign_workflow_kind(),
            item_executor: Arc::new(ManagedMiningItemExecutor),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_item_executor(
        item_executor: Arc<dyn crate::workflows::MiningItemExecutor>,
    ) -> Self {
        Self {
            kind: mining_campaign_workflow_kind(),
            item_executor,
        }
    }
}

impl WorkflowFactory for MiningCampaignWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    fn current_schema_version(&self) -> u32 {
        3
    }

    fn supports_schema_version(&self, version: u32) -> bool {
        matches!(version, 1..=3)
    }

    fn migrate(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
    ) -> Result<Option<WorkflowMigration>, String> {
        if instance.schema_version == 3 {
            return Ok(None);
        }
        if instance.schema_version == 2 {
            let mut config: MiningCampaignIntent = instance.config().map_err(string_error)?;
            config.transport_routes = Vec::new();
            let checkpoint: MiningCampaignCheckpoint =
                instance.checkpoint().map_err(string_error)?;
            return Ok(Some(WorkflowMigration::new(
                serde_json::to_value(config).map_err(string_error)?,
                serde_json::to_value(checkpoint).map_err(string_error)?,
            )));
        }
        let legacy: LegacyMiningCampaignIntent = instance.config().map_err(string_error)?;
        let checkpoint: MiningDeployCheckpoint = instance.checkpoint().map_err(string_error)?;
        let mission = checkpoint
            .plan_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(string_error)?;
        let config = MiningCampaignIntent {
            systems: legacy.systems,
            region: legacy.region.unwrap_or_default(),
            hub: checkpoint.hub.or(legacy.hub).unwrap_or_default(),
            transport_routes: Vec::new(),
            max_concurrency: legacy.max_concurrency,
        };
        let checkpoint = MiningCampaignCheckpoint {
            mission,
            migration_worker: checkpoint.replicant.or(legacy.replicant),
            started: checkpoint.started,
        };
        Ok(Some(WorkflowMigration::new(
            serde_json::to_value(config).map_err(string_error)?,
            serde_json::to_value(checkpoint).map_err(string_error)?,
        )))
    }

    fn service_intents(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
    ) -> Result<WorkflowServiceIntentProjection, String> {
        let config: MiningCampaignIntent = instance.config().map_err(string_error)?;
        let checkpoint: MiningCampaignCheckpoint = instance.checkpoint().map_err(string_error)?;
        if let Some(mission) = checkpoint.mission
            && !mission.routes.is_empty()
            && !mission.hub_location.trim().is_empty()
            && mission
                .routes
                .iter()
                .all(|route| !route.system.trim().is_empty() && !route.belt.trim().is_empty())
        {
            let destination = mission.hub_location.clone();
            let intents = mission
                .routes
                .into_iter()
                .map(|route| {
                    AmiTransportRouteIntent {
                        system: route.system,
                        collect: route.belt,
                        deliver: destination.clone(),
                    }
                    .workflow_service_intent()
                })
                .collect();
            return Ok(WorkflowServiceIntentProjection::complete(intents));
        }
        if !config.transport_routes.is_empty() {
            return Ok(WorkflowServiceIntentProjection::complete(
                config
                    .transport_routes
                    .iter()
                    .map(AmiTransportRouteIntent::workflow_service_intent)
                    .collect(),
            ));
        }
        let scopes = config
            .systems
            .iter()
            .filter(|system| !system.trim().is_empty())
            .map(|system| WorkflowServiceScope::System(system.trim().to_ascii_uppercase()))
            .collect::<BTreeSet<_>>();
        if scopes.is_empty() {
            return Err("mining campaign has no service scope".into());
        }
        Ok(WorkflowServiceIntentProjection::unknown(scopes))
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(MiningCampaignWorkflow {
            item_executor: self.item_executor.clone(),
        }))
    }
    fn placement_intents(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
        work_items: &[WorkItem],
    ) -> Result<WorkflowPlacementIntentProjection, String> {
        mining_campaign_placement(instance, work_items)
    }
}
workflow_factory!(
    SalvageRecoveryWorkflowFactory,
    SalvageRecoveryWorkflow,
    salvage_recovery_workflow_kind,
    salvage_recovery_placement
);
workflow_factory!(
    LogisticsWorkflowFactory,
    LogisticsWorkflow,
    logistics_workflow_kind,
    logistics_placement
);
workflow_factory!(
    RegionalDispatchWorkflowFactory,
    RegionalDispatchWorkflow,
    regional_dispatch_workflow_kind,
    regional_dispatch_placement
);
workflow_factory!(
    LogisticsManifestWorkflowFactory,
    LogisticsManifestWorkflow,
    logistics_manifest_workflow_kind,
    logistics_manifest_placement
);
workflow_factory!(
    TradeFulfillmentWorkflowFactory,
    TradeFulfillmentWorkflow,
    trade_fulfillment_workflow_kind,
    trade_placement
);
workflow_factory!(
    BlueprintAcquireWorkflowFactory,
    BlueprintAcquireWorkflow,
    blueprint_acquire_workflow_kind,
    blueprint_placement
);
workflow_factory!(
    ExplorationWorkflowFactory,
    ExplorationWorkflow,
    exploration_workflow_kind,
    exploration_placement
);
workflow_factory!(
    EventDeliveryWorkflowFactory,
    EventDeliveryWorkflow,
    event_delivery_workflow_kind,
    event_placement
);
workflow_factory!(
    EventTourWorkflowFactory,
    EventTourWorkflow,
    event_tour_workflow_kind,
    event_tour_placement
);
pub(crate) type EventItemFuture<'a> =
    std::pin::Pin<Box<dyn Future<Output = Result<String, crate::event::AnyError>> + Send + 'a>>;

pub(crate) trait EventItemExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        client: &'a Client,
        mission_json: &'a str,
        stage: EventItemStage,
        allocations: &'a AllocationSet,
        wait_timeout: Duration,
    ) -> EventItemFuture<'a>;
}

#[derive(Debug, thiserror::Error)]
#[error("allocated event resource for requirement {requirement} is missing")]
pub(crate) struct EventMissingAllocationError {
    pub(crate) requirement: String,
    pub(crate) allocation_id: replicant_workflow::AllocationId,
}

pub(crate) struct ManagedEventItemExecutor;

impl EventItemExecutor for ManagedEventItemExecutor {
    fn execute<'a>(
        &'a self,
        client: &'a Client,
        mission_json: &'a str,
        stage: EventItemStage,
        allocations: &'a AllocationSet,
        wait_timeout: Duration,
    ) -> EventItemFuture<'a> {
        Box::pin(execute_event_item(
            client,
            mission_json,
            stage,
            allocations,
            wait_timeout,
        ))
    }
}

/// Factory for pooled regional event campaigns.
pub struct EventCampaignWorkflowFactory {
    kind: WorkflowKind,
    item_executor: Arc<dyn EventItemExecutor>,
}

impl EventCampaignWorkflowFactory {
    fn new() -> Self {
        Self {
            kind: event_campaign_workflow_kind(),
            item_executor: Arc::new(ManagedEventItemExecutor),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_item_executor(item_executor: Arc<dyn EventItemExecutor>) -> Self {
        Self {
            kind: event_campaign_workflow_kind(),
            item_executor,
        }
    }
}

impl WorkflowFactory for EventCampaignWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    fn current_schema_version(&self) -> u32 {
        2
    }

    fn supports_schema_version(&self, version: u32) -> bool {
        matches!(version, 1 | 2)
    }

    fn migrate(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
    ) -> Result<Option<WorkflowMigration>, String> {
        if instance.schema_version == 2 {
            return Ok(None);
        }
        let legacy: LegacyEventCampaignIntent = instance.config().map_err(string_error)?;
        let mut checkpoint: EventCampaignCheckpoint =
            instance.checkpoint().map_err(string_error)?;
        checkpoint.replicant = checkpoint.replicant.or(legacy.replicant);
        let config = EventCampaignIntent {
            region: legacy.region,
            home: checkpoint.home.clone().or(legacy.home).unwrap_or_default(),
        };
        Ok(Some(WorkflowMigration::new(
            serde_json::to_value(config).map_err(string_error)?,
            serde_json::to_value(checkpoint).map_err(string_error)?,
        )))
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(EventCampaignWorkflow {
            item_executor: self.item_executor.clone(),
        }))
    }
    fn placement_intents(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
        work_items: &[WorkItem],
    ) -> Result<WorkflowPlacementIntentProjection, String> {
        event_campaign_placement(instance, work_items)
    }
}
workflow_factory!(
    ObservatoryWorkflowFactory,
    ObservatoryWorkflow,
    observatory_workflow_kind,
    observatory_placement
);
workflow_factory!(
    ReplicantProvisionWorkflowFactory,
    ReplicantProvisionWorkflow,
    replicant_provision_workflow_kind,
    provision_placement
);
workflow_factory!(
    RegionEstablishWorkflowFactory,
    RegionEstablishWorkflow,
    region_establish_workflow_kind,
    region_establish_placement
);

struct ScanSystemWorkflow;
impl WorkflowExecutor for ScanSystemWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: ScanIntent = context.config().map_err(string_error)?;
            run_survey_controller(context, intent, SurveyModeIntent::System).await
        })
    }
}

struct ScanBeltWorkflow;
impl WorkflowExecutor for ScanBeltWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: ScanIntent = context.config().map_err(string_error)?;
            run_survey_controller(context, intent, SurveyModeIntent::Belt).await
        })
    }
}

#[derive(Clone, Copy)]
enum SurveyModeIntent {
    System,
    Belt,
}

async fn run_survey_controller(
    context: &mut WorkflowContext,
    intent: ScanIntent,
    mode: SurveyModeIntent,
) -> Result<(), String> {
    let client = managed_client(context)?;
    let mut checkpoint: ControllerWorkflowCheckpoint =
        context.checkpoint().map_err(string_error)?;
    let controller = resolve_controller(
        &client,
        checkpoint
            .controller
            .as_deref()
            .or(intent.controller.as_deref()),
        DeviceType::SurveyController,
        Some(&intent.system),
    )
    .await?;
    checkpoint.controller = Some(controller.clone());
    claim_device(context, &controller)?;
    claim_target(context, "survey-system", &intent.system)?;
    context
        .advance_to("configuring", &checkpoint)
        .map_err(string_error)?;

    let survey = client
        .devices()
        .get(&controller)
        .await
        .map_err(string_error)?
        .as_survey_controller()
        .map_err(string_error)?;
    if !checkpoint.directive_set {
        let operation = match mode {
            SurveyModeIntent::System => {
                survey
                    .set_directive(SurveyDirective::SurveySystem {
                        planets: "all".to_owned(),
                        moons: "all".to_owned(),
                        recall: intent.recall,
                    })
                    .await
            }
            SurveyModeIntent::Belt => survey.set_directive(SurveyDirective::BeltSearch).await,
        }
        .map_err(string_error)?;
        await_success(&operation).await?;
        checkpoint.directive_set = true;
        context
            .persist_checkpoint(&checkpoint)
            .map_err(string_error)?;
    }
    if !checkpoint.launched {
        context
            .advance_to("launching", &checkpoint)
            .map_err(string_error)?;
        let operation = survey.launch().await.map_err(string_error)?;
        await_success(&operation).await?;
        checkpoint.launched = true;
        context
            .persist_checkpoint(&checkpoint)
            .map_err(string_error)?;
    }
    if !wait_controller_completion(context, &client, &controller, &mut checkpoint).await? {
        return Ok(());
    }
    context
        .mark_succeeded(Some(serde_json::json!({
            "controller": controller,
            "system": intent.system,
            "directive": match mode { SurveyModeIntent::System => "survey_system", SurveyModeIntent::Belt => "belt_search" },
        })))
        .map_err(string_error)
}

struct ScanTourWorkflow;
impl WorkflowExecutor for ScanTourWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: ScanTourIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: ScanTourCheckpoint = context.checkpoint().map_err(string_error)?;
            let assignment = if let (Some(replicant), Some(vessel)) =
                (checkpoint.replicant.clone(), checkpoint.vessel.clone())
            {
                Some((replicant, vessel))
            } else {
                resolve_survey_assignment(
                    &client,
                    intent.replicant.as_deref(),
                    intent.vessel.as_deref(),
                )
                .await?
            };
            let Some((replicant, vessel)) = assignment else {
                context
                    .advance_to("waiting_for_operational_worker", &checkpoint)
                    .map_err(string_error)?;
                context
                    .emit_activity("assigned regional workers are still in transit")
                    .map_err(string_error)?;
                context.mark_waiting().map_err(string_error)?;
                return Ok(());
            };
            let worker_state = survey_worker_state(&client, &replicant, &vessel).await?;
            if let Some(reason) = scan_tour_worker_wait_reason(worker_state) {
                context
                    .advance_to("waiting_for_operational_worker", &checkpoint)
                    .map_err(string_error)?;
                context.emit_activity(reason).map_err(string_error)?;
                context.mark_waiting().map_err(string_error)?;
                return Ok(());
            }
            let maintenance_home = if checkpoint.state.is_none() {
                // Re-resolve pre-launch checkpoints so workflows created by an
                // older build do not stay pinned to a global factory after a
                // regional manufacturing home is available. Once the survey
                // executor has durable state, preserve its existing home for
                // restart consistency.
                resolve_scan_tour_home(&client, &intent.center).await?
            } else {
                match checkpoint.maintenance_home.clone() {
                    Some(value) => value,
                    None => resolve_scan_tour_home(&client, &intent.center).await?,
                }
            };
            checkpoint.replicant = Some(replicant.clone());
            checkpoint.vessel = Some(vessel.clone());
            checkpoint.maintenance_home = Some(maintenance_home.clone());
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;
            if let Some(owner) = reserve_scan_tour_scope(context, &intent, &replicant, &vessel)? {
                context
                    .advance_to("waiting_for_scan_claims", &checkpoint)
                    .map_err(string_error)?;
                context
                    .emit_activity(format!(
                        "survey assignment is temporarily reserved by workflow {owner}; waiting to retry"
                    ))
                    .map_err(string_error)?;
                context.mark_waiting().map_err(string_error)?;
                return Ok(());
            }

            if !ensure_scan_tour_fleet_capacity(
                context,
                &client,
                &vessel,
                &maintenance_home,
                &mut checkpoint,
            )
            .await?
            {
                return Ok(());
            }

            let plan_file = scratch_file(context.id(), "survey-plan.json")?;
            if let Some(state) = checkpoint.state.as_ref() {
                restore_survey_checkpoint(&plan_file, state).map_err(string_error)?;
            } else {
                clear_scratch_file(&plan_file)?;
            }
            let options = SurveyOptions {
                mode: SurveyMode::Run,
                replicant,

                vessel,
                center: intent.center,
                radius_ly: intent.radius_ly,
                system_limit: intent.system_limit.max(1),
                target_systems: intent.target_systems,
                star_detail_concurrency: 8,
                mission_file: plan_file,
                controller: checkpoint.fleet_controller.clone(),
                drones: (!checkpoint.fleet_drones.is_empty())
                    .then(|| checkpoint.fleet_drones.clone()),
                replace_plan: false,
                include_explored: intent.include_explored,
                travel_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
                survey_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
                maintenance_home,
                maintenance_interval: 40,
                maintenance_threshold_pct: 25.0,
                maintenance_resume_pct: 95.0,
                maintenance_check_interval: Duration::from_secs(900),
            };
            let result = execute_survey_workflow(&client, &options, |state| {
                let (replicant, vessel, devices) = state.resources();
                claim(context, ResourceKey::Replicant(replicant.to_owned()))?;
                claim_device(context, vessel)?;
                for device in devices {
                    claim_device(context, device)?;
                }
                checkpoint.state = Some(state.clone());
                context
                    .advance_to(state.step_name(), &checkpoint)
                    .map_err(|error| error.to_string().into())
            })
            .await;
            match result {
                Ok(summary) => context.mark_succeeded(Some(summary)).map_err(string_error),
                Err(error) => Err(error.to_string()),
            }
        })
    }
}
/// A complete, remote-authoritative salvage observation used by campaigns and
/// the Automation Director.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub(crate) struct SalvageRecoveryHistorySnapshot {
    /// Number of discovered events returned by the remote history query.
    pub(crate) discovered_count: usize,
    /// Number of depleted events returned by the remote history query.
    pub(crate) depleted_count: usize,
    /// Recoverable sites grouped by canonical region.
    pub(crate) sites_by_region: BTreeMap<String, BTreeMap<String, SalvageSiteRecord>>,
}

/// Fetches the complete salvage histories and resolves each parent location
/// against the durable galaxy catalogue exactly once.
///
/// The event histories are deliberately the authority here.  In particular,
/// this must not use the transient resource-site projection, which can omit
/// historical discoveries and can be ahead of the durable event log.
pub(crate) async fn salvage_recovery_history_snapshot(
    client: &Client,
) -> Result<SalvageRecoveryHistorySnapshot, String> {
    let mut catalogue = client.galaxy().catalogue();
    if catalogue.is_empty() {
        client
            .galaxy()
            .refresh_catalogue()
            .await
            .map_err(string_error)?;
        catalogue = client.galaxy().catalogue();
    }
    let catalogue_systems = catalogue
        .iter()
        .map(|star| star.key.id.to_string())
        .collect::<BTreeSet<_>>();
    let system_regions = catalogue
        .into_iter()
        .filter_map(|star| {
            star.region
                .map(|region| (star.key.id.to_string(), canonical_region(&region)))
        })
        .collect::<BTreeMap<_, _>>();

    let discovered = client
        .events()
        .full_history_named("salvage.discovered")
        .await
        .map_err(string_error)?;
    let depleted = client
        .events()
        .full_history_named("salvage.depleted")
        .await
        .map_err(string_error)?;

    let mut location_regions = BTreeMap::new();
    for location in discovered
        .iter()
        .filter_map(|event| event.payload.get("location").and_then(Value::as_str))
    {
        if location_regions.contains_key(location) {
            continue;
        }
        let system = resolve_location_system_from_catalogue(location, &catalogue_systems)
            .ok_or_else(|| format!("{location} does not resolve to a known system"))?;
        if let Some(region) = system_regions.get(&system) {
            location_regions.insert(location.to_owned(), region.clone());
        }
    }

    let observed_regions = location_regions.values().cloned().collect::<BTreeSet<_>>();
    let mut sites_by_region = BTreeMap::new();
    let completed = BTreeSet::new();
    for region in observed_regions {
        let sites = salvage_recovery_ledger(
            &discovered,
            &depleted,
            &completed,
            &location_regions,
            &region,
        );
        sites_by_region.insert(region, sites);
    }

    Ok(SalvageRecoveryHistorySnapshot {
        discovered_count: discovered.len(),
        depleted_count: depleted.len(),
        sites_by_region,
    })
}

fn resolve_location_system_from_catalogue(
    location: &str,
    catalogue: &BTreeSet<String>,
) -> Option<String> {
    catalogue
        .iter()
        .map(String::as_str)
        .filter(|system| designation_in_system(location, system))
        .max_by_key(|system| system.len())
        .map(str::to_owned)
}

/// Returns designations completed by durable salvage-site state documents.
pub(crate) fn completed_salvage_sites(
    repository: &replicant_workflow::WorkflowRepository,
) -> Result<BTreeSet<String>, RepositoryError> {
    Ok(repository
        .list_documents("salvage.site_state")?
        .into_iter()
        .map(|(designation, _, _)| designation)
        .collect())
}

/// Applies live completion authority to one canonical region's cached history.
pub(crate) fn recoverable_salvage_sites(
    snapshot: &SalvageRecoveryHistorySnapshot,
    completed: &BTreeSet<String>,
    region: &str,
) -> BTreeMap<String, SalvageSiteRecord> {
    let region = canonical_region(region);
    snapshot
        .sites_by_region
        .get(&region)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|(designation, _)| !completed.contains(designation))
        .collect()
}

/// Returns whether a workflow is a compatible active regional salvage
/// recovery campaign.
pub(crate) fn salvage_recovery_workflow_matches(
    workflow: &replicant_workflow::WorkflowInstance,
    region: &str,
) -> Result<bool, RepositoryError> {
    if workflow.kind != salvage_recovery_workflow_kind() || workflow.status.is_terminal() {
        return Ok(false);
    }
    let intent: SalvageRecoveryIntent = workflow.config()?;
    Ok(!intent.home.trim().is_empty()
        && canonical_region(&intent.region) == canonical_region(region))
}

/// Reconciles newest-wins salvage discovery history against depletion and completion authority.
pub fn salvage_recovery_ledger(
    discovered: &[replicant_client::domain::Event],
    depleted: &[replicant_client::domain::Event],
    completed: &BTreeSet<String>,
    location_regions: &BTreeMap<String, String>,
    region: &str,
) -> BTreeMap<String, SalvageSiteRecord> {
    let depleted = depleted
        .iter()
        .filter_map(|event| {
            event
                .payload
                .get("designation")
                .or_else(|| event.payload.get("site"))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect::<BTreeSet<_>>();
    let mut sites = BTreeMap::new();
    for event in discovered {
        let Some(designation) = event
            .payload
            .get("designation")
            .or_else(|| event.payload.get("site"))
            .and_then(Value::as_str)
        else {
            continue;
        };
        let Some(location) = event.payload.get("location").and_then(Value::as_str) else {
            continue;
        };
        if location_regions.get(location).map(String::as_str) != Some(region) {
            continue;
        }
        let resources = event
            .payload
            .get("resources")
            .and_then(Value::as_object)
            .into_iter()
            .flat_map(|resources| resources.iter())
            .filter_map(|(resource, quantity)| {
                quantity
                    .as_i64()
                    .map(|quantity| (resource.clone(), quantity))
            })
            .collect();
        sites.insert(
            designation.to_owned(),
            SalvageSiteRecord {
                designation: designation.to_owned(),
                location: location.to_owned(),
                resources,
                event_id: event.id.as_str().to_owned(),
            },
        );
    }
    sites.retain(|designation, _| {
        !depleted.contains(designation) && !completed.contains(designation)
    });
    sites
}

fn salvage_recovery_item_specs(
    workflow_id: WorkflowId,
    sites: &BTreeMap<String, SalvageSiteRecord>,
    region: &str,
    home: &str,
    capacities: &[(String, u64)],
) -> Result<Vec<WorkItemSpec>, RepositoryError> {
    let kind = WorkflowKind::new("salvage.site")?;
    let mut specs = Vec::new();
    for (site_index, site) in sites.values().enumerate() {
        let quantity = site
            .resources
            .values()
            .filter_map(|quantity| u64::try_from(*quantity).ok())
            .sum::<u64>()
            .max(1);
        let mut shards = salvage_capacity_shards(quantity, capacities);
        if shards.is_empty() {
            shards.push((String::new(), 1));
        }
        for (trip_index, (carrier_hint, trip_quantity)) in shards.into_iter().enumerate() {
            specs.push(WorkItemSpec {
                workflow_id,
                dedupe_key: format!("salvage.site:{}:trip:{trip_index}", site.designation),
                kind: kind.clone(),
                sort_key: format!("{site_index:08}:{trip_index:08}:{}", site.designation),
                payload_json: serde_json::json!({
                    "designation": site.designation,
                    "location": site.location,
                    "resources": site.resources,
                    "home": home,
                    "trip_quantity": trip_quantity,
                    "carrier_hint": carrier_hint,
                }),
                preconditions_json: serde_json::json!([{
                    "kind": "salvage.not_depleted",
                    "parameters": {"designation": site.designation}
                }]),
                requirements_json: serde_json::to_value([
                    ResourceRequirement {
                        key: "worker".into(),
                        kind: "replicant".into(),
                        capabilities: vec![OPERATIONAL_REGIONAL_WORKER_CAPABILITY.into()],
                        scope: RequirementScope::Region(region.to_owned()),
                        count: 1,
                        quantity: 1,
                    },
                    ResourceRequirement {
                        key: "controller".into(),
                        kind: "device".into(),
                        capabilities: vec!["ami_mining_controller".into()],
                        scope: RequirementScope::Region(region.to_owned()),
                        count: 1,
                        quantity: 1,
                    },
                    ResourceRequirement {
                        key: "drones".into(),
                        kind: "device".into(),
                        capabilities: vec!["mining_drone".into()],
                        scope: RequirementScope::Region(region.to_owned()),
                        count: 1,
                        quantity: 1,
                    },
                    ResourceRequirement {
                        key: "freighters".into(),
                        kind: "device".into(),
                        capabilities: vec!["cargo_freighter".into()],
                        scope: RequirementScope::Region(region.to_owned()),
                        count: 1,
                        quantity: 1,
                    },
                    ResourceRequirement {
                        key: "stow".into(),
                        kind: "stow".into(),
                        capabilities: Vec::new(),
                        scope: RequirementScope::Region(region.to_owned()),
                        count: 1,
                        quantity: trip_quantity,
                    },
                ])?,
                deadline_at_ms: None,
            });
        }
    }
    Ok(specs)
}

/// Deterministically shards a salvage quantity across actual free freighter capacities.
pub fn salvage_capacity_shards(
    mut quantity: u64,
    capacities: &[(String, u64)],
) -> Vec<(String, u64)> {
    let mut capacities = capacities
        .iter()
        .filter(|(_, capacity)| *capacity > 0)
        .cloned()
        .collect::<Vec<_>>();
    capacities.sort();
    let mut shards = Vec::new();
    while quantity > 0 && !capacities.is_empty() {
        let before = quantity;
        for (carrier, capacity) in &capacities {
            if quantity == 0 {
                break;
            }
            let assigned = quantity.min(*capacity);
            shards.push((carrier.clone(), assigned));
            quantity -= assigned;
        }
        if quantity == before {
            break;
        }
    }
    shards
}

fn salvage_freighter_capacities(
    candidates: &[replicant_workflow::AllocationCandidate],
) -> Vec<(String, u64)> {
    let stow = candidates
        .iter()
        .filter_map(|candidate| match &candidate.resource {
            ResourceKey::Namespaced { namespace, key } if namespace == "stow" => {
                Some((key.clone(), candidate.available_quantity))
            }
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut capacities = candidates
        .iter()
        .filter_map(|candidate| match &candidate.resource {
            ResourceKey::Device(code)
                if candidate
                    .capabilities
                    .iter()
                    .any(|capability| capability == "cargo_freighter") =>
            {
                Some((code.clone(), stow.get(code).copied().unwrap_or(0)))
            }
            _ => None,
        })
        .filter(|(_, capacity)| *capacity > 0)
        .collect::<Vec<_>>();
    capacities.sort();
    capacities
}

async fn wait_salvage_recovery_completion(
    client: &Client,
    controller: &str,
    timeout: Duration,
) -> Result<(), String> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut observed_active = false;
    let mut idle_observations = 0_u8;
    loop {
        let device = client
            .devices()
            .get(controller)
            .await
            .map_err(string_error)?
            .snapshot()
            .await
            .map_err(string_error)?;
        let active = device
            .active_directive
            .as_ref()
            .and_then(|directive| directive.status.as_deref())
            .is_some_and(|status| status.eq_ignore_ascii_case("active"));
        if active {
            observed_active = true;
            idle_observations = 0;
        } else {
            idle_observations = idle_observations.saturating_add(1);
            if observed_active || idle_observations >= 2 {
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!(
                "salvage controller {controller} did not complete before timeout"
            ));
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

struct SalvageRecoveryWorkflow;
impl WorkflowExecutor for SalvageRecoveryWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: SalvageRecoveryIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let repository = context.repository_handle();
            let region = canonical_region(&intent.region);
            let snapshot = salvage_recovery_history_snapshot(&client).await?;
            let completed = completed_salvage_sites(repository.as_ref()).map_err(string_error)?;
            let sites = recoverable_salvage_sites(&snapshot, &completed, &region);
            repository
                .put_document(
                    "automation.scheduler.salvage",
                    &context.id().to_string(),
                    &serde_json::json!({
                        "discovery_count": snapshot.discovered_count,
                        "depleted_count": snapshot.depleted_count,
                        "ledger_count": completed.len(),
                        "worklist_count": sites.len(),
                    }),
                )
                .map_err(string_error)?;
            let mut checkpoint = SalvageRecoveryCheckpoint {
                sites: sites.clone(),
            };
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;
            let broker = crate::assignment::ResourceBroker::with_managed_client(
                repository.clone(),
                client.clone(),
            );
            let observed_candidates = crate::workflows::regional_relay_candidates(
                repository.as_ref(),
                &client,
                broker.discover_candidates().map_err(string_error)?,
                &region,
            )?;
            let capacities = salvage_freighter_capacities(&observed_candidates);
            repository
                .reconcile_work_items(
                    context.id(),
                    &salvage_recovery_item_specs(
                        context.id(),
                        &sites,
                        &region,
                        &intent.home,
                        &capacities,
                    )
                    .map_err(string_error)?,
                    unix_millis(),
                )
                .map_err(string_error)?;
            context
                .advance_to("recovering", &checkpoint)
                .map_err(string_error)?;
            let broker = crate::assignment::ResourceBroker::with_managed_client(
                repository.clone(),
                client.clone(),
            );
            'items: loop {
                let candidates = crate::workflows::regional_relay_candidates(
                    repository.as_ref(),
                    &client,
                    broker.discover_candidates().map_err(string_error)?,
                    &region,
                )?;
                let Some(assigned) = repository
                    .claim_next_work_item(context.id(), unix_millis())
                    .map_err(string_error)?
                else {
                    break;
                };
                let mut allocations =
                    match broker.allocate(assigned.id, assigned.state.revision, &candidates) {
                        Ok(allocations) => allocations,
                        Err(_) => {
                            repository
                                .transition_work_item(
                                    assigned.id,
                                    assigned.state.revision,
                                    WorkItemTransition::Reclaimed {
                                        checkpoint_json: assigned.state.checkpoint_json.clone(),
                                    },
                                    unix_millis(),
                                )
                                .map_err(string_error)?;
                            break;
                        }
                    };
                let worker = allocation_worker(&allocations)
                    .ok_or_else(|| "salvage allocation omitted worker".to_owned())?;
                let assignment_id = format!("salvage:{}:{worker}", assigned.id);
                let location = assigned.spec.payload_json["location"]
                    .as_str()
                    .ok_or_else(|| "salvage item omitted parent location".to_owned())?;
                let designation = assigned.spec.payload_json["designation"]
                    .as_str()
                    .ok_or_else(|| "salvage item omitted designation".to_owned())?;
                let completed_live = repository
                    .read_document("salvage.site_state", designation)
                    .map_err(string_error)?
                    .is_some();
                let depleted_live = client
                    .events()
                    .full_history_named("salvage.depleted")
                    .await
                    .map_err(string_error)?
                    .iter()
                    .any(|event| {
                        event
                            .payload
                            .get("designation")
                            .or_else(|| event.payload.get("site"))
                            .and_then(Value::as_str)
                            == Some(designation)
                    });
                if completed_live || depleted_live {
                    repository
                        .transition_work_item(
                            assigned.id,
                            assigned.state.revision,
                            WorkItemTransition::Skipped {
                                reason: "salvage site already depleted or completed".into(),
                                result_json: Some(serde_json::json!({
                                    "designation": designation,
                                    "location": location,
                                })),
                            },
                            unix_millis(),
                        )
                        .map_err(string_error)?;
                    continue;
                }
                repository
                    .assign_work_item(
                        assigned.id,
                        assigned.state.revision,
                        &assignment_id,
                        &ResourceKey::Replicant(worker.clone()),
                        unix_millis(),
                    )
                    .map_err(string_error)?;
                let started = repository
                    .start_work_item(
                        assigned.id,
                        assigned.state.revision,
                        &worker,
                        &assignment_id,
                        unix_millis(),
                    )
                    .map_err(string_error)?;
                let location = started.spec.payload_json["location"]
                    .as_str()
                    .ok_or_else(|| "salvage item omitted parent location".to_owned())?;
                let designation = started.spec.payload_json["designation"]
                    .as_str()
                    .ok_or_else(|| "salvage item omitted designation".to_owned())?;
                travel_to_system(
                    &client,
                    &worker,
                    location,
                    Duration::from_secs(DEFAULT_WAIT_SECONDS),
                )
                .await
                .map_err(string_error)?;
                let controller = allocations
                    .by_requirement
                    .get("controller")
                    .and_then(|allocations| allocations.first())
                    .and_then(|allocation| match &allocation.resource {
                        ResourceKey::Device(code) => Some(code.clone()),
                        _ => None,
                    })
                    .ok_or_else(|| "salvage allocation omitted mining controller".to_owned())?;
                let mining = client
                    .devices()
                    .get(&controller)
                    .await
                    .map_err(string_error)?
                    .as_mining_controller()
                    .map_err(string_error)?;
                let operation = mining
                    .set_directive(MiningDirective::GatherSalvage {
                        location: designation.to_owned(),
                        recall: true,
                    })
                    .await
                    .map_err(string_error)?;
                await_success(&operation).await?;
                let operation = mining.launch().await.map_err(string_error)?;
                await_success(&operation).await?;
                wait_salvage_recovery_completion(
                    &client,
                    &controller,
                    Duration::from_secs(DEFAULT_WAIT_SECONDS),
                )
                .await?;
                let trip_quantity = started.spec.payload_json["trip_quantity"]
                    .as_u64()
                    .ok_or_else(|| "salvage item omitted trip quantity".to_owned())?;
                let (hauled, site_complete) = loop {
                    let freighter_allocations = allocations
                        .by_requirement
                        .get("freighters")
                        .ok_or_else(|| "salvage allocation omitted freighter".to_owned())?;
                    let freighters = freighter_allocations
                        .iter()
                        .filter_map(|allocation| match &allocation.resource {
                            ResourceKey::Device(code) => Some(code.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    match haul_allocated_resources(
                        &client,
                        &freighters,
                        location,
                        &intent.home,
                        trip_quantity,
                        Duration::from_secs(DEFAULT_WAIT_SECONDS),
                    )
                    .await
                    {
                        Ok(result) => break result,
                        Err(error)
                            if failure_class(error.as_ref())
                                == Some(FailureClass::DeviceTargetMissing) =>
                        {
                            let allocation_id = freighter_allocations
                                .first()
                                .map(|allocation| allocation.id)
                                .ok_or_else(|| "salvage allocation omitted freighter".to_owned())?;
                            match broker
                                .replace_dead_allocation_from(
                                    started.id,
                                    allocation_id,
                                    &candidates,
                                )
                                .map_err(string_error)?
                            {
                                replicant_workflow::ReplacementOutcome::Replaced(replacement) => {
                                    let allocation = allocations
                                        .by_requirement
                                        .get_mut("freighters")
                                        .and_then(|allocations| {
                                            allocations
                                                .iter_mut()
                                                .find(|allocation| allocation.id == allocation_id)
                                        })
                                        .ok_or_else(|| {
                                            format!(
                                                "salvage allocation {allocation_id} disappeared"
                                            )
                                        })?;
                                    *allocation = replacement;
                                }
                                replicant_workflow::ReplacementOutcome::Waiting => {
                                    repository
                                        .transition_work_item(
                                            started.id,
                                            started.state.revision,
                                            WorkItemTransition::Waiting {
                                                checkpoint_json: Some(serde_json::json!({
                                                    "directive_launched": true,
                                                    "designation": designation,
                                                    "location": location,
                                                })),
                                                reason: error.to_string(),
                                                retry_at_ms: Some(
                                                    unix_millis().saturating_add(300_000),
                                                ),
                                            },
                                            unix_millis(),
                                        )
                                        .map_err(string_error)?;
                                    continue 'items;
                                }
                                replicant_workflow::ReplacementOutcome::Unavailable => {
                                    continue 'items;
                                }
                            }
                        }
                        Err(error) => {
                            repository
                                .transition_work_item(
                                    started.id,
                                    started.state.revision,
                                    WorkItemTransition::Waiting {
                                        checkpoint_json: Some(serde_json::json!({
                                            "directive_launched": true,
                                            "designation": designation,
                                            "location": location,
                                        })),
                                        reason: error.to_string(),
                                        retry_at_ms: Some(unix_millis().saturating_add(300_000)),
                                    },
                                    unix_millis(),
                                )
                                .map_err(string_error)?;
                            continue 'items;
                        }
                    }
                };
                if site_complete {
                    repository
                        .put_document(
                            "salvage.site_state",
                            designation,
                            &serde_json::json!({
                                "completed": true,
                                "location": location,
                                "completed_at_ms": unix_millis(),
                            }),
                        )
                        .map_err(string_error)?;
                }
                repository
                    .transition_work_item(
                        started.id,
                        started.state.revision,
                        WorkItemTransition::Succeeded {
                            checkpoint_json: Some(serde_json::json!({
                                "directive_launched": true,
                                "location": location,
                                "designation": designation,
                            })),
                            result_json: Some(serde_json::json!({
                                "designation": designation,
                                "location": location,
                                "trip_quantity": trip_quantity,
                                "hauled": hauled,
                                "site_complete": site_complete,
                            })),
                        },
                        unix_millis(),
                    )
                    .map_err(string_error)?;
                if site_complete {
                    checkpoint.sites.remove(designation);
                }
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            match repository
                .aggregate_campaign_result(context.id())
                .map_err(string_error)?
            {
                Some(result) if result.workflow_status() == WorkflowStatus::Succeeded => {
                    context.mark_succeeded(Some(result)).map_err(string_error)
                }
                Some(result) => context
                    .mark_failed_with_result(
                        "salvage recovery completed without a successful site",
                        result,
                        replicant_workflow::WorkflowFailureDisposition::Permanent,
                    )
                    .map_err(string_error),
                None if sites.is_empty() => context
                    .mark_succeeded(Some(serde_json::json!({"sites": 0})))
                    .map_err(string_error),
                None => context.mark_waiting().map_err(string_error),
            }
        })
    }
}

struct SalvageWorkflow;
impl WorkflowExecutor for SalvageWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: SalvageIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: ControllerWorkflowCheckpoint =
                context.checkpoint().map_err(string_error)?;
            let system = resolve_location_system(&client, &intent.location).await?;
            let controller = resolve_controller(
                &client,
                checkpoint
                    .controller
                    .as_deref()
                    .or(intent.controller.as_deref()),
                DeviceType::MiningController,
                Some(&system),
            )
            .await?;
            checkpoint.controller = Some(controller.clone());
            claim_device(context, &controller)?;
            claim_target(context, "salvage-site", &intent.location)?;
            context
                .advance_to("configuring", &checkpoint)
                .map_err(string_error)?;

            let mining = client
                .devices()
                .get(&controller)
                .await
                .map_err(string_error)?
                .as_mining_controller()
                .map_err(string_error)?;
            if !checkpoint.directive_set {
                let operation = mining
                    .set_directive(MiningDirective::GatherSalvage {
                        location: intent.location.clone(),
                        recall: intent.recall,
                    })
                    .await
                    .map_err(string_error)?;
                await_success(&operation).await?;
                checkpoint.directive_set = true;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            if !checkpoint.launched {
                context
                    .advance_to("launching", &checkpoint)
                    .map_err(string_error)?;
                let operation = mining.launch().await.map_err(string_error)?;
                await_success(&operation).await?;
                checkpoint.launched = true;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            if !wait_controller_completion(context, &client, &controller, &mut checkpoint).await? {
                return Ok(());
            }
            context
                .mark_succeeded(Some(serde_json::json!({
                    "controller": controller,
                    "location": intent.location,
                    "directive": "gather_salvage",
                })))
                .map_err(string_error)
        })
    }
}

struct MiningDeployWorkflow;
impl WorkflowExecutor for MiningDeployWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: MiningDeployIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: MiningDeployCheckpoint =
                context.checkpoint().map_err(string_error)?;
            let replicant = match checkpoint.replicant.clone() {
                Some(value) => value,
                None => resolve_replicant(&client, intent.replicant.as_deref()).await?,
            };
            let hub = match checkpoint.hub.clone() {
                Some(value) => value,
                None => resolve_home(&client, intent.hub.as_deref()).await?,
            };
            checkpoint.replicant = Some(replicant.clone());
            checkpoint.hub = Some(hub.clone());
            claim(context, ResourceKey::Replicant(replicant.clone()))?;
            claim_target(context, "mining-target", &intent.system)?;
            claim_target(context, "location", &hub)?;

            let plan_file = scratch_file(context.id(), "mining-plan.json")?;
            materialize_json(&plan_file, checkpoint.plan_json.as_deref())?;
            checkpoint.started = true;
            context
                .advance_to("deploying", &checkpoint)
                .map_err(string_error)?;
            let request = MiningExpansionRequest {
                systems: vec![intent.system],
                replicant,
                hub,
                transport_routes: Vec::new(),
                mission_file: plan_file.clone(),
                wait_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
                max_concurrency: 1,
            };
            let execution = execute_expansion(&client, &request);
            tokio::pin!(execution);
            let mut checkpoint_interval = tokio::time::interval(Duration::from_secs(2));
            let result = loop {
                tokio::select! {
                    result = &mut execution => break result,
                    _ = checkpoint_interval.tick() => {
                        match context.control_request().map_err(string_error)? {
                            replicant_workflow::ControlRequest::Continue => {}
                            replicant_workflow::ControlRequest::Pause
                            | replicant_workflow::ControlRequest::Cancel => return Ok(()),
                        }
                        if plan_file.exists() {
                            checkpoint.plan_json = Some(read_json(&plan_file)?);
                            context.persist_checkpoint(&checkpoint).map_err(string_error)?;
                        }
                    }
                }
            };
            if plan_file.exists() {
                checkpoint.plan_json = Some(read_json(&plan_file)?);
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            match result {
                Ok(report) => context.mark_succeeded(Some(report)).map_err(string_error),
                Err(error) => Err(error.to_string()),
            }
        })
    }
}

struct BeltSearchCampaignWorkflow;
impl WorkflowExecutor for BeltSearchCampaignWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let mut intent: BeltSearchCampaignIntent = context.config().map_err(string_error)?;
            let checkpoint: BeltSearchCampaignCheckpoint =
                context.checkpoint().map_err(string_error)?;
            let client = managed_client(context)?;
            let repository = context.repository_handle();
            let region = loop {
                if let Some(region) =
                    resolve_belt_campaign_region(repository.as_ref(), &intent, &checkpoint)
                        .map_err(string_error)?
                {
                    break region;
                }
                if !wait_for_campaign_work(
                    context,
                    "belt campaign is waiting for its durable regional assignment",
                    &CAMPAIGN_RESOURCE_EVENT_NAMES,
                    None,
                    IDLE_CAMPAIGN_RETRY_INTERVAL,
                )
                .await?
                {
                    return Ok(());
                }
            };
            intent.region = region;
            let specs = belt_search_item_specs(context.id(), &intent).map_err(string_error)?;
            let specs = belt_specs_for_reconciliation(repository.as_ref(), context.id(), specs)
                .map_err(string_error)?;
            repository
                .reconcile_work_items(context.id(), &specs, unix_millis())
                .map_err(string_error)?;
            context
                .advance_to(
                    "searching_for_belts",
                    &BeltSearchCampaignCheckpoint::default(),
                )
                .map_err(string_error)?;

            loop {
                let mut excluded_workers = BTreeSet::new();
                loop {
                    let broker = crate::assignment::ResourceBroker::with_managed_client(
                        repository.clone(),
                        client.clone(),
                    );
                    let mut candidates = broker.discover_candidates().map_err(string_error)?;
                    let assignments = repository
                        .list_documents("director.replicant")
                        .map_err(string_error)?
                        .into_iter()
                        .filter_map(|(worker, value, _)| {
                            (value.get("region").and_then(Value::as_str)
                                == Some(intent.region.as_str()))
                            .then_some(worker)
                        })
                        .collect::<BTreeSet<_>>();
                    candidates.retain(|candidate| {
                        belt_worker_candidate(candidate)
                            && matches!(
                                &candidate.resource,
                                ResourceKey::Replicant(worker)
                                    if assignments.contains(worker)
                                        && !excluded_workers.contains(worker)
                                        && candidate.location.as_ref().and_then(|location| {
                                            location.region.as_deref()
                                        }).is_some_and(|physical_region| {
                                            canonical_region(physical_region)
                                                == canonical_region(&intent.region)
                                        })
                            )
                    });
                    let mut running = Vec::new();
                    while running.len() < candidates.len() {
                        let Some(assigned) = repository
                            .claim_next_work_item(context.id(), unix_millis())
                            .map_err(string_error)?
                        else {
                            break;
                        };
                        let allocations = match broker.allocate(
                            assigned.id,
                            assigned.state.revision,
                            &candidates,
                        ) {
                            Ok(allocations) => allocations,
                            Err(error) => {
                                repository
                                    .transition_work_item(
                                        assigned.id,
                                        assigned.state.revision,
                                        WorkItemTransition::Waiting {
                                            checkpoint_json: None,
                                            reason: error.to_string(),
                                            retry_at_ms: Some(
                                                unix_millis().saturating_add(300_000),
                                            ),
                                        },
                                        unix_millis(),
                                    )
                                    .map_err(string_error)?;
                                // Every remaining belt item has the same
                                // regional worker requirement. Once the broker
                                // proves that no candidate can satisfy one
                                // item, claiming thousands of sibling items
                                // only stamps the same capacity error onto the
                                // whole campaign and inflates worker-pressure
                                // diagnostics. Leave the rest pending and let
                                // this batch's running work finish before the
                                // campaign retries capacity.
                                break;
                            }
                        };
                        let worker = allocation_worker(&allocations).ok_or_else(|| {
                            "belt item allocation omitted its Replicant".to_owned()
                        })?;
                        let started = repository
                            .start_work_item(
                                assigned.id,
                                assigned.state.revision,
                                &worker,
                                &format!("belt:{}:{worker}", assigned.id),
                                unix_millis(),
                            )
                            .map_err(string_error)?;
                        let system = started
                            .spec
                            .payload_json
                            .get("system")
                            .and_then(Value::as_str)
                            .ok_or_else(|| "belt item payload omitted system".to_owned())?
                            .to_owned();
                        running.push(run_belt_item(
                            repository.clone(),
                            client.clone(),
                            started,
                            worker,
                            system,
                        ));
                    }
                    if running.is_empty() {
                        break;
                    }
                    for worker in futures::future::join_all(running)
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .flatten()
                    {
                        excluded_workers.insert(worker);
                    }
                }

                match repository
                    .aggregate_campaign_result(context.id())
                    .map_err(string_error)?
                {
                    Some(result) if result.workflow_status() == WorkflowStatus::Succeeded => {
                        return context.mark_succeeded(Some(result)).map_err(string_error);
                    }
                    Some(result) => {
                        return context
                            .mark_failed_with_result(
                                "belt-search campaign completed without a successful item",
                                result,
                                replicant_workflow::WorkflowFailureDisposition::Permanent,
                            )
                            .map_err(string_error);
                    }
                    None => {
                        let deadline = campaign_retry_deadline(
                            repository.as_ref(),
                            context.id(),
                            unix_millis().saturating_add(
                                i64::try_from(IDLE_CAMPAIGN_RETRY_INTERVAL.as_millis())
                                    .unwrap_or(i64::MAX),
                            ),
                        )
                        .map_err(string_error)?;
                        if !wait_for_campaign_work(
                            context,
                            "belt campaign is waiting for a scan-capable regional Replicant or item retry",
                            &CAMPAIGN_RESOURCE_EVENT_NAMES,
                            Some(deadline),
                            IDLE_CAMPAIGN_RETRY_INTERVAL,
                        )
                        .await?
                        {
                            return Ok(());
                        }
                    }
                }
            }
        })
    }
}

/// Fixed-window execution metrics for a pooled belt-search campaign.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct BeltSearchPoolMetrics {
    /// Sum of clipped active item minutes divided by wall-clock minutes.
    pub effective_parallelism: f64,
    /// Maximum overlapping attempt intervals.
    pub peak_overlap: usize,
    /// Distinct workers observed in attempt history.
    pub unique_workers: usize,
    /// Attempts closed as safely reclaimed.
    pub reclaim_count: usize,
    /// Terminal and nonterminal item counts keyed by snake-case status.
    pub item_outcomes: BTreeMap<String, usize>,
    /// Terminal campaign outcome, when all work is complete.
    pub campaign_outcome: Option<replicant_workflow::CampaignOutcome>,
}

/// Derives exact attempt-interval metrics for one wall-clock window.
pub fn belt_search_pool_metrics(
    repository: &replicant_workflow::WorkflowRepository,
    workflow_id: WorkflowId,
    window_start_ms: i64,
    window_end_ms: i64,
) -> Result<BeltSearchPoolMetrics, RepositoryError> {
    let items = repository.list_work_items(workflow_id)?;
    let mut active_ms = 0_i64;
    let mut events = Vec::new();
    let mut workers = BTreeSet::new();
    let mut reclaim_count = 0;
    let mut item_outcomes = BTreeMap::new();
    for item in &items {
        let status = match item.state.status {
            replicant_workflow::WorkItemStatus::Pending => "pending",
            replicant_workflow::WorkItemStatus::Assigned => "assigned",
            replicant_workflow::WorkItemStatus::Running => "running",
            replicant_workflow::WorkItemStatus::Waiting => "waiting",
            replicant_workflow::WorkItemStatus::Succeeded => "succeeded",
            replicant_workflow::WorkItemStatus::Skipped => "skipped",
            replicant_workflow::WorkItemStatus::Failed => "failed",
            replicant_workflow::WorkItemStatus::Abandoned => "abandoned",
        };
        *item_outcomes.entry(status.to_owned()).or_default() += 1;
        for attempt in repository.list_work_item_attempts(item.id)? {
            workers.insert(attempt.worker_identity);
            if attempt.outcome == Some(replicant_workflow::WorkItemAttemptOutcome::Reclaimed) {
                reclaim_count += 1;
            }
            let start = attempt.started_at_ms.max(window_start_ms);
            let end = attempt
                .ended_at_ms
                .unwrap_or(window_end_ms)
                .min(window_end_ms);
            if end > start {
                active_ms = active_ms.saturating_add(end - start);
                events.push((start, 1_i32));
                events.push((end, -1_i32));
            }
        }
    }
    events.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let mut overlap = 0_i32;
    let mut peak_overlap = 0_i32;
    for (_, delta) in events {
        overlap += delta;
        peak_overlap = peak_overlap.max(overlap);
    }
    let wall_ms = window_end_ms.saturating_sub(window_start_ms);
    let effective_parallelism = if wall_ms > 0 {
        active_ms as f64 / wall_ms as f64
    } else {
        0.0
    };
    Ok(BeltSearchPoolMetrics {
        effective_parallelism,
        peak_overlap: usize::try_from(peak_overlap).unwrap_or(0),
        unique_workers: workers.len(),

        reclaim_count,
        item_outcomes,
        campaign_outcome: repository
            .aggregate_campaign_result(workflow_id)?
            .map(|result| result.outcome),
    })
}
fn resolve_belt_campaign_region(
    repository: &replicant_workflow::WorkflowRepository,
    intent: &BeltSearchCampaignIntent,
    checkpoint: &BeltSearchCampaignCheckpoint,
) -> Result<Option<String>, RepositoryError> {
    if !intent.region.trim().is_empty() {
        return Ok(Some(intent.region.clone()));
    }
    let legacy_worker = checkpoint
        .legacy_checkpoint
        .as_ref()
        .and_then(|value| value.get("replicant"))
        .and_then(Value::as_str);
    let Some(legacy_worker) = legacy_worker else {
        return Ok(None);
    };
    Ok(repository
        .read_document("director.replicant", legacy_worker)?
        .and_then(|(value, _)| {
            value
                .get("region")
                .and_then(Value::as_str)
                .map(str::to_owned)
        }))
}

const BELT_SCAN_CAPABILITIES: [&str; 3] = [
    "census",
    "system_scan",
    OPERATIONAL_REGIONAL_WORKER_CAPABILITY,
];

fn belt_worker_candidate(candidate: &replicant_workflow::AllocationCandidate) -> bool {
    BELT_SCAN_CAPABILITIES.iter().all(|required| {
        candidate
            .capabilities
            .iter()
            .any(|capability| capability == required)
    })
}

fn belt_specs_for_reconciliation(
    repository: &replicant_workflow::WorkflowRepository,
    workflow_id: WorkflowId,
    mut desired: Vec<WorkItemSpec>,
) -> Result<Vec<WorkItemSpec>, RepositoryError> {
    for existing in repository.list_work_items(workflow_id)? {
        let Some(spec) = desired
            .iter_mut()
            .find(|spec| spec.dedupe_key == existing.spec.dedupe_key)
        else {
            continue;
        };
        let legacy_requirements =
            existing
                .spec
                .requirements_json
                .as_array()
                .is_some_and(|requirements| {
                    requirements.len() == 1
                        && requirements[0]["capabilities"]
                            .as_array()
                            .is_some_and(Vec::is_empty)
                });
        let mut upgraded = existing.spec.clone();
        upgraded.requirements_json = spec.requirements_json.clone();
        if legacy_requirements && upgraded == *spec {
            *spec = existing.spec;
        }
    }
    Ok(desired)
}

fn belt_search_item_specs(
    workflow_id: WorkflowId,
    intent: &BeltSearchCampaignIntent,
) -> Result<Vec<WorkItemSpec>, RepositoryError> {
    let kind = WorkflowKind::new("belt.system")?;
    let mut seen = BTreeSet::new();
    let mut specs = Vec::new();
    for (index, system) in intent.systems.iter().enumerate() {
        if !seen.insert(system.clone()) {
            continue;
        }
        specs.push(WorkItemSpec {
            workflow_id,
            dedupe_key: format!("belt.system:{system}"),
            kind: kind.clone(),
            sort_key: format!("{index:08}:{system}"),
            payload_json: serde_json::json!({ "system": system }),
            preconditions_json: serde_json::json!([{
                "kind": "system.unexplored",
                "parameters": { "system": system }
            }]),
            requirements_json: serde_json::json!([{
                "key": "worker",
                "kind": "replicant",
                "capabilities": [
                    "census",
                    "system_scan",
                    OPERATIONAL_REGIONAL_WORKER_CAPABILITY
                ],
                "scope": {
                    "kind": "region",
                    "value": intent.region
                },
                "count": 1,
                "quantity": 1
            }]),
            deadline_at_ms: None,
        });
    }
    Ok(specs)
}

fn allocation_worker(allocations: &AllocationSet) -> Option<String> {
    allocations
        .iter()
        .find_map(|allocation| match &allocation.resource {
            ResourceKey::Replicant(worker) => Some(worker.clone()),
            _ => None,
        })
}

async fn run_belt_item(
    repository: Arc<replicant_workflow::WorkflowRepository>,
    client: Client,
    item: WorkItem,
    worker: String,
    system: String,
) -> Result<Option<String>, String> {
    match execute_belt_search_system(
        &client,
        &worker,
        &system,
        Duration::from_secs(DEFAULT_WAIT_SECONDS),
        false,
    )
    .await
    {
        Ok(stop) => {
            let transition = if stop.scanned {
                WorkItemTransition::Succeeded {
                    checkpoint_json: None,
                    result_json: Some(serde_json::to_value(stop).map_err(string_error)?),
                }
            } else {
                WorkItemTransition::Skipped {
                    reason: "system already explored".into(),
                    result_json: None,
                }
            };
            repository
                .transition_work_item(item.id, item.state.revision, transition, unix_millis())
                .map(|_| None)
                .map_err(string_error)
        }
        Err(error) => {
            let capability_mismatch = belt_capability_mismatch(error.as_ref());
            let transition = belt_item_failure_transition_with_checkpoint(
                error.as_ref(),
                item.state.checkpoint_json,
            );
            repository
                .transition_work_item(item.id, item.state.revision, transition, unix_millis())
                .map_err(string_error)?;
            Ok(capability_mismatch.then_some(worker))
        }
    }
}

fn belt_capability_mismatch(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if let Some(error) = source.downcast_ref::<replicant_client::Error>()
            && error.status() == Some(400)
            && error.details().is_some_and(|details| {
                [
                    details.message.as_deref(),
                    details.field_errors.as_deref(),
                    details.body_excerpt.as_deref(),
                ]
                .into_iter()
                .flatten()
                .any(known_belt_capability_message)
            })
        {
            return true;
        }
        if let Some(rejection) = source.downcast_ref::<BeltOperationRejection>()
            && rejection.status() == Some(400)
            && rejection
                .message()
                .is_some_and(known_belt_capability_message)
        {
            return true;
        }
        current = source.source();
    }
    false
}

fn known_belt_capability_message(message: &str) -> bool {
    let message = message.to_ascii_lowercase().replace('-', "_");
    [
        "does not have census capability",
        "does not have the census capability",
        "doesn't have census capability",
        "does not have system_scan capability",
        "does not have the system_scan capability",
        "doesn't have system_scan capability",
        "does not support census",
        "does not support system_scan",
        "census capability is not available",
        "system_scan capability is not available",
        "missing census capability",
        "missing system_scan capability",
    ]
    .iter()
    .any(|fragment| message.contains(fragment))
}

fn belt_item_failure_transition_with_checkpoint(
    error: &(dyn std::error::Error + 'static),
    checkpoint_json: Option<Value>,
) -> WorkItemTransition {
    let message = error.to_string();
    if belt_capability_mismatch(error) {
        return WorkItemTransition::Reclaimed { checkpoint_json };
    }
    let mut current = Some(error);
    let mut structured_missing = false;
    while let Some(source) = current {
        if source
            .downcast_ref::<replicant_client::Error>()
            .is_some_and(|error| error.status() == Some(404))
        {
            structured_missing = true;
            break;
        }
        current = source.source();
    }
    if structured_missing
        || failure_disposition(error) == replicant_workflow::WorkflowFailureDisposition::Permanent
    {
        WorkItemTransition::Failed {
            error: message,
            result_json: None,
        }
    } else {
        WorkItemTransition::RetryableFailure {
            checkpoint_json: None,
            error: message,
        }
    }
}
#[cfg(test)]
fn belt_item_failure_transition(error: &(dyn std::error::Error + 'static)) -> WorkItemTransition {
    belt_item_failure_transition_with_checkpoint(error, None)
}

struct MiningCampaignWorkflow {
    item_executor: Arc<dyn crate::workflows::MiningItemExecutor>,
}
impl WorkflowExecutor for MiningCampaignWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        let item_executor = self.item_executor.clone();
        Box::pin(async move {
            let intent: MiningCampaignIntent = context.config().map_err(string_error)?;
            if intent.systems.is_empty() {
                return context
                    .mark_succeeded(Some(serde_json::json!({"systems": []})))
                    .map_err(string_error);
            }
            let checkpoint: MiningCampaignCheckpoint =
                context.checkpoint().map_err(string_error)?;
            let hub = if intent.hub.is_empty() {
                resolve_home(&managed_client(context)?, None).await?
            } else {
                intent.hub
            };
            execute_mining_pool_config(
                context,
                item_executor,
                MiningWorkflowConfig {
                    systems: intent.systems,
                    region: intent.region,
                    hub,
                    transport_routes: intent.transport_routes,
                    mission_file: scratch_file(context.id(), "mining-campaign.json")?,
                    wait_timeout_seconds: DEFAULT_WAIT_SECONDS,
                    max_concurrency: intent.max_concurrency.max(1),
                },
                MiningWorkflowCheckpoint {
                    mission: checkpoint.mission,
                    migration_worker: checkpoint.migration_worker,
                    started: checkpoint.started,
                },
            )
            .await
        })
    }
}

struct LogisticsWorkflow;
impl WorkflowExecutor for LogisticsWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: LogisticsIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: LogisticsWorkflowCheckpoint =
                context.checkpoint().map_err(string_error)?;
            let request = delivery_request(&intent);
            if request.resources.is_empty()
                && request.devices.is_empty()
                && request.device_tags.is_empty()
            {
                return Err("logistics delivery must contain at least one payload".to_owned());
            }
            if request.resources.values().any(|quantity| *quantity <= 0)
                || request.devices.iter().any(|request| request.quantity <= 0)
            {
                return Err("logistics quantities must be greater than zero".to_owned());
            }
            let plan = if let Some(plan) = checkpoint.plan.clone() {
                plan
            } else {
                context
                    .advance_to("planning", &checkpoint)
                    .map_err(string_error)?;
                let plan = plan_delivery(&client, &request)
                    .await
                    .map_err(string_error)?;
                for code in plan
                    .cargo_transports
                    .iter()
                    .chain(plan.device_carriers.iter())
                    .chain(plan.payload_devices.iter().map(|device| &device.code))
                {
                    claim_device(context, code)?;
                }
                checkpoint.plan = Some(plan.clone());
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
                plan
            };
            checkpoint.started = true;
            context
                .advance_to("delivering", &checkpoint)
                .map_err(string_error)?;
            let options = DeliveryOptions {
                return_transports: intent.return_transports,
                ..DeliveryOptions::default()
            };
            let report = match execute_delivery(&client, &plan, options).await {
                Ok(report) => report,
                Err(error) => {
                    checkpoint.failure_class = logistics_failure_class(&error);
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                    return Err(string_error(error));
                }
            };
            context.mark_succeeded(Some(report)).map_err(string_error)
        })
    }
}

struct RegionalDispatchWorkflow;
impl WorkflowExecutor for RegionalDispatchWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let mut intent: RegionalDispatchIntent = context.config().map_err(string_error)?;
            validate_regional_dispatch(&intent)?;
            let client = managed_client(context)?;
            let mut checkpoint: RegionalDispatchCheckpoint =
                context.checkpoint().map_err(string_error)?;
            if checkpoint.print_tag.is_empty() {
                checkpoint.print_tag = format!("dispatch:{}", &context.id().to_string()[..8]);
            }
            let requested_source = intent.source.clone();
            let source = if let Some(source) = checkpoint.source_location.clone() {
                source
            } else {
                let source = resolve_regional_dispatch_source(&client, &requested_source).await?;
                checkpoint.source_location = Some(source.clone());
                if !source.eq_ignore_ascii_case(&requested_source) {
                    context
                        .emit_activity(format!(
                            "resolved regional hub {requested_source} to manufacturing location {source}"
                        ))
                        .map_err(string_error)?;
                }
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
                source
            };
            intent.source = source.clone();
            claim_target(context, "regional-dispatch-source", &source)?;

            if !checkpoint.selection_complete {
                context
                    .advance_to("selecting_stock", &checkpoint)
                    .map_err(string_error)?;
                select_regional_dispatch_stock(context, &client, &intent, &mut checkpoint).await?;
                checkpoint.selection_complete = true;
                context
                    .emit_activity(regional_dispatch_deficit_message(
                        &checkpoint.print_requests,
                    ))
                    .map_err(string_error)?;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }

            if !checkpoint.manufacturing_complete {
                context
                    .advance_to("manufacturing", &checkpoint)
                    .map_err(string_error)?;
                if !manufacture_regional_dispatch(context, &client, &intent, &mut checkpoint)
                    .await?
                {
                    return Ok(());
                }
                checkpoint.manufacturing_complete = true;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }

            stow_dispatch_matrices(context, &client, &mut checkpoint).await?;
            replicate_dispatch_matrices(context, &client, &intent, &mut checkpoint).await?;

            let plan = if let Some(plan) = checkpoint.plan.clone() {
                plan
            } else {
                context
                    .advance_to("planning_delivery", &checkpoint)
                    .map_err(string_error)?;
                let request = regional_dispatch_delivery_request(&intent, &checkpoint)?;
                let plan = plan_delivery(&client, &request)
                    .await
                    .map_err(string_error)?;
                for pickup in &plan.resource_pickups {
                    claim(
                        context,
                        ResourceKey::Namespaced {
                            namespace: "logistics-resource-source".to_owned(),
                            key: pickup.location.to_ascii_uppercase(),
                        },
                    )?;
                }
                validate_resource_pickups(&client, &plan)
                    .await
                    .map_err(string_error)?;
                for code in plan
                    .cargo_transports
                    .iter()
                    .chain(plan.device_carriers.iter())
                    .chain(plan.payload_devices.iter().map(|device| &device.code))
                {
                    claim_device(context, code)?;
                }
                checkpoint.plan = Some(plan.clone());
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
                plan
            };
            checkpoint.delivery_started = true;
            context
                .advance_to("delivering", &checkpoint)
                .map_err(string_error)?;
            let report = execute_delivery(&client, &plan, DeliveryOptions::default())
                .await
                .map_err(string_error)?;
            context.mark_succeeded(Some(report)).map_err(string_error)
        })
    }
}

struct LogisticsManifestWorkflow;
impl WorkflowExecutor for LogisticsManifestWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: LogisticsManifestIntent = context.config().map_err(string_error)?;
            validate_placement_recovery_intent(&intent)?;
            if let Some(metadata) = intent.placement_recovery.as_ref() {
                // A recovery row is executable only when the Director has
                // durably authorized this exact workflow identity. This read
                // intentionally precedes every claim and managed API read.
                let authorization =
                    read_placement_recovery_authorization(context.repository(), context.id())?
                        .ok_or_else(|| {
                            "placement recovery has no Director authorization for this workflow"
                                .to_owned()
                        })?;
                if !placement_recovery_authorization_matches(&authorization, context.id(), &intent)
                {
                    return Err(
                        "placement recovery Director authorization is missing or stale".to_owned(),
                    );
                }
                // The Director owns the physical census/topology boundary. A
                // workflow can only rebuild typed workflow-side custody; it
                // must not fall back to generic transport eligibility when
                // that authority is unavailable or incomplete.
                let snapshot = context
                    .workflow_registry()
                    .placement_intent_snapshot(context.repository(), Some(context.id()))
                    .map_err(|error| {
                        format!("placement recovery authority unavailable before cleanup: {error}")
                    })?;
                placement_recovery_metadata_matches_snapshot(metadata, &snapshot)?;
            }
            let request = manifest_delivery_request(&intent);
            if request.resources.is_empty()
                && request.devices.is_empty()
                && request.device_codes.is_empty()
                && request.device_tags.is_empty()
            {
                return Err("logistics manifest must contain at least one payload".to_owned());
            }
            let client = managed_client(context)?;
            let mut checkpoint: LogisticsWorkflowCheckpoint =
                context.checkpoint().map_err(string_error)?;
            let plan =
                if checkpoint.started {
                    Some(checkpoint.plan.clone().ok_or_else(|| {
                        "started logistics manifest lost its durable plan".to_owned()
                    })?)
                } else {
                    checkpoint.plan.clone()
                };
            // Claims are established before any recovery reservation tag is
            // touched.  In particular, a resumed workflow must not perform
            // cleanup merely because its intent names a device.
            if let Some((code, owner)) = claim_devices_until_conflict(
                context,
                intent
                    .device_codes
                    .iter()
                    .chain(intent.pre_deactivate_device_codes.iter()),
            )? {
                context
                    .advance_to("waiting_for_payload_claim", &checkpoint)
                    .map_err(string_error)?;
                context
                    .emit_activity(format!(
                        "manifest payload device {code} is reserved by workflow {owner}; waiting for the claim instead of failing the delivery"
                    ))
                    .map_err(string_error)?;
                context.mark_waiting().map_err(string_error)?;
                return Ok(());
            }
            ensure_logistics_pre_deactivation(&client, &intent.pre_deactivate_device_codes).await?;
            if let Some(metadata) = intent.placement_recovery.as_ref()
                && !release_placement_recovery_tags(
                    context,
                    &client,
                    metadata,
                    &intent.origin,
                    &mut checkpoint,
                )
                .await?
            {
                context
                    .advance_to("waiting_for_recovery_cleanup", &checkpoint)
                    .map_err(string_error)?;
                context.mark_waiting().map_err(string_error)?;
                return Ok(());
            }
            if intent.placement_recovery.is_none() && intent.release_mining_reservations {
                release_mining_reservation_tags(&client, &intent.device_codes).await?;
            }
            if checkpoint.started {
                return execute_logistics_manifest_plan(
                    context,
                    &client,
                    &intent,
                    checkpoint,
                    plan.expect("started checkpoint plan checked above"),
                )
                .await;
            }
            let plan = if let Some(plan) = plan {
                plan
            } else {
                context
                    .advance_to("planning", &checkpoint)
                    .map_err(string_error)?;
                let plan = match plan_delivery(&client, &request).await {
                    Ok(plan) => plan,
                    Err(error) if retryable_manifest_planning_failure(&error) => {
                        checkpoint.plan = None;
                        checkpoint.started = false;
                        context
                            .advance_to("waiting_to_replan", &checkpoint)
                            .map_err(string_error)?;
                        context
                            .emit_activity(format!(
                                "logistics manifest cannot currently be planned ({error}); waiting for fresh managed state or hub stock before replanning"
                            ))
                            .map_err(string_error)?;
                        context.mark_waiting().map_err(string_error)?;
                        return Ok(());
                    }
                    Err(error) => return Err(string_error(error)),
                };

                // Resource stock is mutable account state, so reserve each
                // concrete pickup location before accepting the plan. This is
                // deliberately coarse-grained: two logistics workflows may
                // use different locations concurrently, but they cannot race
                // to consume different snapshots of the same location.
                let mut source_claims = Vec::new();
                for pickup in &plan.resource_pickups {
                    let resource = ResourceKey::Namespaced {
                        namespace: "logistics-resource-source".to_owned(),
                        key: pickup.location.to_ascii_uppercase(),
                    };
                    match context.acquire_claim(resource.clone()) {
                        Ok(_) => source_claims.push(resource),
                        Err(RepositoryError::ClaimConflict { owner, .. }) => {
                            for resource in &source_claims {
                                context.release_claim(resource).map_err(string_error)?;
                            }
                            checkpoint.plan = None;
                            checkpoint.started = false;
                            context
                                .advance_to("waiting_for_resource_source", &checkpoint)
                                .map_err(string_error)?;
                            context
                                .emit_activity(format!(
                                    "resource pickup {} is reserved by workflow {owner}; discarding the stale plan and waiting to replan",
                                    pickup.location
                                ))
                                .map_err(string_error)?;
                            context.mark_waiting().map_err(string_error)?;
                            return Ok(());
                        }
                        Err(error) => return Err(string_error(error)),
                    }
                }

                match validate_resource_pickups(&client, &plan).await {
                    Ok(()) => {}
                    Err(TransportError::NotFound(error)) => {
                        for resource in &source_claims {
                            context.release_claim(resource).map_err(string_error)?;
                        }
                        checkpoint.plan = None;
                        checkpoint.started = false;
                        context
                            .advance_to("replanning_stale_resources", &checkpoint)
                            .map_err(string_error)?;
                        context
                            .emit_activity(format!(
                                "resource pickup snapshot changed before delivery: {error}; waiting to replan from fresh account inventory"
                            ))
                            .map_err(string_error)?;
                        context.mark_waiting().map_err(string_error)?;
                        return Ok(());
                    }
                    Err(error) => return Err(string_error(error)),
                }

                if let Some((code, owner)) = claim_devices_until_conflict(
                    context,
                    plan.cargo_transports
                        .iter()
                        .chain(plan.device_carriers.iter())
                        .chain(plan.payload_devices.iter().map(|device| &device.code)),
                )? {
                    for resource in &source_claims {
                        context.release_claim(resource).map_err(string_error)?;
                    }
                    checkpoint.plan = None;
                    checkpoint.started = false;
                    context
                        .advance_to("waiting_for_transport_claim", &checkpoint)
                        .map_err(string_error)?;
                    context
                        .emit_activity(format!(
                            "delivery device {code} is reserved by workflow {owner}; discarding the stale transport plan and waiting to replan"
                        ))
                        .map_err(string_error)?;
                    context.mark_waiting().map_err(string_error)?;
                    return Ok(());
                }
                checkpoint.plan = Some(plan.clone());
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
                plan
            };
            execute_logistics_manifest_plan(context, &client, &intent, checkpoint, plan).await
        })
    }
}

async fn execute_logistics_manifest_plan(
    context: &mut WorkflowContext,
    client: &Client,
    intent: &LogisticsManifestIntent,
    mut checkpoint: LogisticsWorkflowCheckpoint,
    plan: DeliveryPlan,
) -> Result<(), String> {
    checkpoint.plan = Some(plan.clone());
    checkpoint.started = true;
    context
        .advance_to("delivering", &checkpoint)
        .map_err(string_error)?;
    let operation_namespace =
        if intent.placement_recovery.is_some() {
            let workflow_id = context.id().to_string();
            Some(uuid::Uuid::parse_str(&workflow_id).map_err(|error| {
                format!("workflow ID {workflow_id} is not a valid UUID: {error}")
            })?)
        } else {
            None
        };
    let options = DeliveryOptions {
        return_transports: intent.return_transports,
        operation_namespace,
        ..DeliveryOptions::default()
    };
    let report = match execute_delivery(client, &plan, options).await {
        Ok(report) => report,
        Err(error) => {
            checkpoint.failure_class = logistics_failure_class(&error);
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;
            return Err(string_error(error));
        }
    };
    context.mark_succeeded(Some(report)).map_err(string_error)
}
/// Derives the stable operation identity used for one workflow's exact
/// recovery-device configure mutation. Length-prefixing the components keeps
/// the identity unambiguous even if a future device-code alphabet changes.
fn recovery_configure_operation_id(workflow_id: WorkflowId, device_code: &str) -> OperationId {
    let canonical_code = device_code.to_ascii_uppercase();
    let workflow = workflow_id.to_string();
    let mut hasher = Sha256::new();
    hasher.update(b"replicant.recovery.device-configure.v1\0");
    hasher.update((workflow.len() as u64).to_le_bytes());
    hasher.update(workflow.as_bytes());
    hasher.update((canonical_code.len() as u64).to_le_bytes());
    hasher.update(canonical_code.as_bytes());
    let digest = hasher.finalize();
    let hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    OperationId::new(format!("recovery-configure:{hex}"))
}

fn validate_recovery_device_authority(
    device: &Device,
    code: &str,
    origin: &str,
    configured_tags: &[String],
) -> Result<(), String> {
    if device.access != AccessScope::Owned {
        return Err(format!("recovery device {code} is no longer account-owned"));
    }
    let status = device
        .status
        .as_ref()
        .map(|status| status.as_str().to_ascii_lowercase())
        .ok_or_else(|| format!("recovery device {code} has unknown lifecycle status"))?;
    if !matches!(status.as_str(), "idle" | "inactive" | "deactivated") {
        return Err(format!(
            "recovery device {code} is no longer inactive/deactivated ({status})"
        ));
    }
    if device
        .location
        .as_ref()
        .is_none_or(|location| !location.id.as_str().eq_ignore_ascii_case(origin))
    {
        return Err(format!(
            "recovery device {code} is no longer at exact origin {origin}"
        ));
    }
    if device.travel.is_some()
        || device.relationships.attached_to.is_some()
        || device.relationships.stowed_in.is_some()
        || device.relationships.controller.is_some()
        || device.relationships.assigned_replicant.is_some()
        || device.relationships.hosting_replicant.is_some()
        || device.relationships.linked_device.is_some()
    {
        return Err(format!(
            "recovery device {code} is no longer free-standing and unassigned"
        ));
    }
    if device.active_directive.is_some() {
        return Err(format!(
            "recovery device {code} has an active or unavailable directive"
        ));
    }
    if device.tags.iter().any(|tag| !configured_tags.contains(tag)) {
        return Err(format!(
            "recovery device {code} has a tag outside its authenticated recovery tags"
        ));
    }
    Ok(())
}

async fn release_placement_recovery_tags(
    context: &mut WorkflowContext,
    client: &Client,
    metadata: &PlacementRecoveryMetadata,
    origin: &str,
    checkpoint: &mut LogisticsWorkflowCheckpoint,
) -> Result<bool, String> {
    for (code, configured_tags) in &metadata.release_device_tags {
        let exact_claim = context
            .claims()
            .map_err(string_error)?
            .into_iter()
            .any(|claim| claim.resource == ResourceKey::Device(code.clone()));
        if !exact_claim {
            return Err(format!(
                "recovery cleanup for {code} requires an exact device claim"
            ));
        }

        let handle = client.devices().get(code).await.map_err(string_error)?;
        let prior = checkpoint.placement_recovery_cleanup.get(code).cloned();
        let operation_id = prior
            .as_ref()
            .and_then(|cleanup| cleanup.operation_id.as_deref())
            .map(|value| OperationId::new(value.to_owned()))
            .unwrap_or_else(|| recovery_configure_operation_id(context.id(), code));
        let snapshot = handle
            .refresh()
            .await
            .map_err(string_error)?
            .snapshot()
            .await
            .map_err(string_error)?;
        validate_recovery_device_authority(&snapshot, code, origin, configured_tags)?;
        let present = configured_tags
            .iter()
            .filter(|tag| snapshot.tags.iter().any(|existing| existing == *tag))
            .cloned()
            .collect::<Vec<_>>();

        // The current managed tag postcondition is authoritative even when a
        // prior operation was submitted, accepted, ambiguous, or nonterminal.
        // An absent tag completes cleanup without replaying the mutation.
        if present.is_empty() && !configured_tags.is_empty() && prior.is_none() {
            return Err(format!(
                "recovery target {code} does not physically carry its configured recovery tag"
            ));
        }
        if present.is_empty() {
            let mut cleanup = prior.unwrap_or_default();
            cleanup.state = Some("absent".to_owned());
            if cleanup.tags.is_empty() {
                cleanup.tags = configured_tags.clone();
            }
            checkpoint
                .placement_recovery_cleanup
                .insert(code.clone(), cleanup);
            context
                .persist_checkpoint(checkpoint)
                .map_err(string_error)?;
            continue;
        }

        let request_tags = prior
            .as_ref()
            .map(|cleanup| {
                if cleanup.tags.is_empty() {
                    present.clone()
                } else {
                    cleanup.tags.clone()
                }
            })
            .unwrap_or_else(|| present.clone());
        if request_tags.is_empty()
            || request_tags
                .iter()
                .any(|tag| !configured_tags.contains(tag))
        {
            return Err(format!(
                "recovery cleanup operation for {code} has unauthenticated tags"
            ));
        }

        // the managed operation boundary. A crash here resumes through the
        // same configure_with_id call; no separate in-flight marker is needed.
        checkpoint.placement_recovery_cleanup.insert(
            code.clone(),
            PlacementRecoveryCleanup {
                operation_id: Some(operation_id.as_str().to_owned()),
                tags: request_tags.clone(),
                state: Some("prepared".to_owned()),
            },
        );
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
        let operation = handle
            .configure_with_id(
                operation_id,
                replicant_client::raw::devices::DeviceConfiguration {
                    remove_tags: Some(request_tags.clone()),
                    ..Default::default()
                },
            )
            .await
            .map_err(string_error)?;
        if let Some(saved) = checkpoint.placement_recovery_cleanup.get_mut(code) {
            saved.state = Some("submitted".to_owned());
        }
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
        let result = await_recovery_operation(&operation).await;
        let refreshed = handle.refresh().await.map_err(string_error)?;
        let current = refreshed.snapshot().await.map_err(string_error)?;
        let remaining = request_tags
            .iter()
            .filter(|tag| current.tags.iter().any(|present| present == *tag))
            .count();
        if remaining == 0 {
            if let Some(saved) = checkpoint.placement_recovery_cleanup.get_mut(code) {
                saved.state = Some("completed".to_owned());
            }
            context
                .persist_checkpoint(checkpoint)
                .map_err(string_error)?;
            continue;
        }

        let state = match &result {
            Ok(true) => "ambiguous",
            Ok(false) => "pending",
            Err(_) => "failed",
        };
        if let Some(saved) = checkpoint.placement_recovery_cleanup.get_mut(code) {
            saved.state = Some(state.to_owned());
        }
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
        match result {
            Ok(_) => return Ok(false),
            Err(error) => return Err(error),
        }
    }
    Ok(true)
}

/// Recovery cleanup is applied only by a resolved managed operation.  In
/// particular `Accepted` is not evidence that a configure mutation happened.
async fn await_recovery_operation(operation: &Operation) -> Result<bool, String> {
    let initial = operation.status().await.map_err(string_error)?;
    if initial == OperationStatus::Accepted {
        return Ok(false);
    }
    if initial == OperationStatus::Completed {
        return Ok(true);
    }
    if matches!(
        initial,
        OperationStatus::ReconciliationRequired | OperationStatus::Ambiguous
    ) {
        let outcome = operation.reconcile().await.map_err(string_error)?;
        return match outcome.status {
            OperationStatus::Completed => Ok(true),
            OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed => {
                Err(format!(
                    "managed cleanup operation {} ended as {:?}: {}",
                    operation.id(),
                    outcome.status,
                    outcome.response.unwrap_or(Value::Null)
                ))
            }
            _ => Ok(false),
        };
    }
    let outcome = operation
        .wait_timeout(Duration::from_secs(30))
        .await
        .map_err(string_error)?;
    match outcome.status {
        OperationStatus::Completed => Ok(true),
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed => {
            Err(format!(
                "managed cleanup operation {} ended as {:?}: {}",
                operation.id(),
                outcome.status,
                outcome.response.unwrap_or(Value::Null)
            ))
        }
        status => {
            // Prepared/submitted/in-progress/awaiting-evidence remain durable
            // and must be observed on a later executor invocation.
            let _ = status;
            Ok(false)
        }
    }
}

async fn release_mining_reservation_tags(
    client: &Client,
    device_codes: &[String],
) -> Result<(), String> {
    for code in device_codes {
        let handle = client.devices().get(code).await.map_err(string_error)?;
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        let removable = snapshot
            .tags
            .iter()
            .filter(|tag| tag.starts_with("mine-"))
            .cloned()
            .collect::<Vec<_>>();
        if removable.is_empty() {
            continue;
        }
        let operation = handle
            .configure(replicant_client::raw::devices::DeviceConfiguration {
                remove_tags: Some(removable),
                ..Default::default()
            })
            .await
            .map_err(string_error)?;
        await_success(&operation).await?;
    }
    Ok(())
}

async fn ensure_logistics_pre_deactivation(
    client: &Client,
    device_codes: &[String],
) -> Result<(), String> {
    for code in device_codes {
        let detail = client
            .raw()
            .devices()
            .get(code)
            .await
            .map_err(string_error)?
            .value;
        let status = detail.status.as_deref().unwrap_or_default();
        if ["idle", "inactive", "deactivated", "stopped", "paused"]
            .iter()
            .any(|candidate| status.eq_ignore_ascii_case(candidate))
        {
            continue;
        }
        let can_deactivate = detail
            .available_commands
            .iter()
            .chain(detail.commands.iter())
            .any(|command| command.eq_ignore_ascii_case("deactivate"));
        if can_deactivate {
            let operation = client
                .devices()
                .get(code)
                .await
                .map_err(string_error)?
                .deactivate()
                .await
                .map_err(string_error)?;
            await_success(&operation).await?;
            continue;
        }
        return Err(format!(
            "pre-delivery device {code} is {status:?} and cannot be deactivated safely"
        ));
    }
    Ok(())
}

fn retryable_manifest_planning_failure(error: &TransportError) -> bool {
    matches!(
        error,
        TransportError::NotFound(_) | TransportError::PayloadUnavailable(_)
    )
}

fn logistics_failure_class(error: &TransportError) -> Option<FailureClass> {
    matches!(
        error,
        TransportError::StaleResourcePickup(_) | TransportError::PayloadUnavailable(_)
    )
    .then_some(FailureClass::LogisticsStateStale)
}

struct TradeFulfillmentWorkflow;
impl WorkflowExecutor for TradeFulfillmentWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: TradeFulfillmentIntent = context.config().map_err(string_error)?;
            validate_trade_fulfillment_intent(&intent)?;
            let client = managed_client(context)?;
            let mut checkpoint: TradeFulfillmentCheckpoint =
                context.checkpoint().map_err(string_error)?;
            claim_target(
                context,
                "trade-fulfillment",
                &format!(
                    "{}:{}",
                    intent.controller.to_ascii_uppercase(),
                    intent.trade_code.to_ascii_uppercase()
                ),
            )?;

            let home = checkpoint
                .home
                .clone()
                .unwrap_or_else(|| intent.home.trim().to_ascii_uppercase());
            let home_system = match checkpoint.home_system.clone() {
                Some(system) => system,
                None => resolve_location_system(&client, &home).await?,
            };
            checkpoint.home = Some(home.clone());
            checkpoint.home_system = Some(home_system.clone());

            let replicant = resolve_and_claim_trade_replicant(
                context,
                &client,
                checkpoint
                    .replicant
                    .as_deref()
                    .or(intent.replicant.as_deref()),
                &home,
                &home_system,
            )
            .await?;
            checkpoint.replicant = Some(replicant.clone());
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;

            if checkpoint.returned_home {
                return context
                    .mark_succeeded(Some(trade_fulfillment_report(&intent, &checkpoint)))
                    .map_err(string_error);
            }

            if checkpoint.criteria.is_none() || checkpoint.rewards.is_none() {
                let live_trade =
                    live_shop_trade(&client, &intent.controller, &intent.trade_code).await?;
                validate_live_trade_for_fulfillment(&intent, &live_trade)?;
                checkpoint.criteria = Some(live_trade.criteria_bundle());
                checkpoint.rewards = Some(live_trade.rewards_bundle());
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }

            let criteria = checkpoint
                .criteria
                .clone()
                .ok_or_else(|| "trade fulfillment has no criteria snapshot".to_owned())?;
            let rewards = checkpoint
                .rewards
                .clone()
                .ok_or_else(|| "trade fulfillment has no reward snapshot".to_owned())?;
            validate_trade_bundle(&criteria, "criteria")?;
            validate_trade_bundle(&rewards, "rewards")?;

            if !checkpoint.purchase_authorized {
                if !ensure_trade_replicant_at(
                    context,
                    &client,
                    &checkpoint,
                    &replicant,
                    &home,
                    "assembling_at_home",
                )
                .await?
                {
                    return Ok(());
                }

                if !ensure_trade_payment_ready(
                    context,
                    &client,
                    &intent,
                    &mut checkpoint,
                    &home,
                    &home_system,
                    &criteria,
                )
                .await?
                {
                    return Ok(());
                }

                if !ensure_trade_reward_capacity(
                    context,
                    &client,
                    &mut checkpoint,
                    &home_system,
                    &intent.shop_location,
                    &rewards,
                    &replicant,
                )
                .await?
                {
                    return Ok(());
                }

                // The server chooses the most recently arrived Replicant as the
                // buyer. Stage all logistics/escorts first, then send our claimed
                // buyer as the final arrival immediately before the mutation.
                if !ensure_trade_replicant_at(
                    context,
                    &client,
                    &checkpoint,
                    &replicant,
                    &intent.shop_location,
                    "travelling_to_shop",
                )
                .await?
                {
                    return Ok(());
                }

                let live_trade =
                    live_shop_trade(&client, &intent.controller, &intent.trade_code).await?;
                validate_live_trade_for_fulfillment(&intent, &live_trade)?;
                if live_trade.criteria_bundle() != criteria
                    || live_trade.rewards_bundle() != rewards
                {
                    return Err(format!(
                        "trade {} changed after provisioning; refusing to execute stale criteria/rewards",
                        intent.trade_code
                    ));
                }
                let stock = live_trade.current_stock.unwrap_or_default();
                if stock <= 0 {
                    return Err(format!(
                        "trade {} on controller {} is out of stock",
                        intent.trade_code, intent.controller
                    ));
                }

                checkpoint.pre_purchase_devices =
                    snapshot_reward_device_codes(&client, &rewards).await?;
                checkpoint.pre_purchase_stock = Some(stock);
                checkpoint.purchase_authorized = true;
                context
                    .advance_to("executing_trade", &checkpoint)
                    .map_err(string_error)?;

                let operation = client
                    .trading()
                    .execute(&intent.controller, &intent.trade_code)
                    .await
                    .map_err(string_error)?;
                checkpoint.purchase_submitted = true;
                checkpoint.purchase_operation = Some(operation.id().as_str().to_owned());
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
                await_success(&operation).await?;
                checkpoint.purchase_observed = true;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            } else if let Some(operation_id) = checkpoint.purchase_operation.clone() {
                context
                    .advance_to("recovering_trade", &checkpoint)
                    .map_err(string_error)?;
                let operation = client
                    .operations()
                    .get(OperationId::new(operation_id.clone()));
                await_success(&operation).await.map_err(|error| {
                    format!(
                        "managed trade operation {operation_id} could not be recovered: {error}"
                    )
                })?;
                checkpoint.purchase_observed = true;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            } else if !checkpoint.purchase_observed {
                // Conservative ambiguity handling for the tiny crash window after
                // authorization but before the managed operation ID is persisted.
                // Device rewards can still prove success; otherwise do not submit
                // a second irreversible trade.
                let observed = observe_trade_reward_devices(
                    &client,
                    &rewards,
                    &checkpoint.pre_purchase_devices,
                    &intent.shop_location,
                    1,
                )
                .await?;
                if !observed.is_empty() {
                    checkpoint.reward_devices = observed;
                    checkpoint.purchase_observed = true;
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                } else if trade_stock_decreased(
                    &client,
                    &intent.controller,
                    &intent.trade_code,
                    checkpoint.pre_purchase_stock,
                )
                .await?
                {
                    checkpoint.purchase_observed = true;
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                } else {
                    context
                        .advance_to("reconciling_trade", &checkpoint)
                        .map_err(string_error)?;
                    context
                        .emit_activity(
                            "trade was authorized but no managed operation ID was checkpointed; waiting for reward or stock-change evidence rather than resubmitting"
                                .to_owned(),
                        )
                        .map_err(string_error)?;
                    context.mark_waiting().map_err(string_error)?;
                    return Ok(());
                }
            }

            if checkpoint.reward_devices.is_empty() && !rewards.devices.is_empty() {
                let observed = observe_trade_reward_devices(
                    &client,
                    &rewards,
                    &checkpoint.pre_purchase_devices,
                    &intent.shop_location,
                    24,
                )
                .await?;
                let expected = trade_bundle_device_count(&rewards)?;
                if observed.len() < expected {
                    context
                        .advance_to("awaiting_trade_rewards", &checkpoint)
                        .map_err(string_error)?;
                    context
                        .emit_activity(format!(
                            "trade completed but only {}/{} rewarded devices are visible at {}; waiting for managed ownership reconciliation",
                            observed.len(), expected, intent.shop_location
                        ))
                        .map_err(string_error)?;
                    context.mark_waiting().map_err(string_error)?;
                    return Ok(());
                }
                checkpoint.reward_devices = observed;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }

            if !secure_trade_device_rewards(
                context,
                &client,
                &mut checkpoint,
                &replicant,
                &intent.shop_location,
            )
            .await?
            {
                return Ok(());
            }

            if !checkpoint.reward_resources_loaded && !rewards.resources.is_empty() {
                load_trade_reward_resources(
                    context,
                    &client,
                    &mut checkpoint,
                    &intent.shop_location,
                    &rewards.resources,
                )
                .await?;
                checkpoint.reward_resources_loaded = true;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }

            if !return_trade_assets_home(
                context,
                &client,
                &mut checkpoint,
                &replicant,
                &home,
                &rewards.resources,
            )
            .await?
            {
                return Ok(());
            }
            checkpoint.returned_home = true;
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;
            context
                .mark_succeeded(Some(trade_fulfillment_report(&intent, &checkpoint)))
                .map_err(string_error)
        })
    }
}

struct BlueprintAcquireWorkflow;
impl WorkflowExecutor for BlueprintAcquireWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: BlueprintAcquireIntent = context.config().map_err(string_error)?;
            let device_type = intent.device_type.trim();
            if device_type.is_empty() {
                return Err("blueprint acquisition requires a device type".to_owned());
            }
            let client = managed_client(context)?;
            let mut checkpoint: BlueprintAcquireCheckpoint =
                context.checkpoint().map_err(string_error)?;
            claim_target(
                context,
                "blueprint-acquire",
                &device_type.to_ascii_lowercase(),
            )?;

            if blueprint_is_known(&client, device_type).await? {
                checkpoint.blueprint_verified = true;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
                return context
                    .mark_succeeded(Some(serde_json::json!({
                        "device_type": device_type,
                        "strategy": "already_known",
                    })))
                    .map_err(string_error);
            }

            if let Some(operation_id) = checkpoint.decommission_operation.clone() {
                context
                    .advance_to("recovering_decommission", &checkpoint)
                    .map_err(string_error)?;
                let operation = client
                    .operations()
                    .get(OperationId::new(operation_id.clone()));
                if let Err(error) = await_success(&operation).await
                    && !blueprint_is_known(&client, device_type).await?
                {
                    return Err(format!(
                        "managed decommission operation {operation_id} could not be recovered: {error}"
                    ));
                }
                if wait_for_blueprint(&client, device_type, 24).await? {
                    checkpoint.blueprint_verified = true;
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                    return context
                        .mark_succeeded(Some(serde_json::json!({
                            "device_type": device_type,
                            "strategy": blueprint_acquisition_strategy(&intent),
                            "source_device": checkpoint.source_device,
                            "autofactory": checkpoint.autofactory,
                            "recovered_operation": operation_id,
                        })))
                        .map_err(string_error);
                }
                return Err(format!(
                    "decommission operation {operation_id} completed but blueprint {device_type} was not observed"
                ));
            }

            let devices = owned_device_snapshots(&client).await?;
            if checkpoint.decommission_authorized {
                // There is an intentionally conservative crash window between durably
                // authorizing this irreversible command and receiving/persisting the
                // managed operation ID. Never submit a second decommission from that
                // state. The managed operation journal will recover a prepared
                // operation after restart; this workflow only observes the resulting
                // blueprint until the ambiguity resolves.
                if wait_for_blueprint(&client, device_type, 24).await? {
                    checkpoint.blueprint_verified = true;
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                    return context
                        .mark_succeeded(Some(serde_json::json!({
                            "device_type": device_type,
                            "strategy": blueprint_acquisition_strategy(&intent),
                            "source_device": checkpoint.source_device,
                            "autofactory": checkpoint.autofactory,
                            "reconciled_after_authorization": true,
                        })))
                        .map_err(string_error);
                }
                let source_state = checkpoint
                    .source_device
                    .as_deref()
                    .and_then(|code| {
                        devices
                            .iter()
                            .find(|device| device.key.id.as_str().eq_ignore_ascii_case(code))
                    })
                    .map(|device| {
                        if blueprint_source_is_releasable(device, device_type) {
                            "still present"
                        } else {
                            "present but no longer releasable"
                        }
                    })
                    .unwrap_or("no longer present");
                context
                    .advance_to("reconciling_decommission", &checkpoint)
                    .map_err(string_error)?;
                context
                    .emit_activity(format!(
                        "decommission authorization is awaiting blueprint evidence; source is {source_state}; automatic resubmission is suppressed"
                    ))
                    .map_err(string_error)?;
                context.mark_waiting().map_err(string_error)?;
                return Ok(());
            }

            if intent.shop.is_some() && checkpoint.source_device.is_none() {
                let ready = if legacy_shop_purchase_in_progress(&checkpoint) {
                    prepare_shop_blueprint_source(context, &client, &intent, &mut checkpoint)
                        .await?
                } else {
                    prepare_shop_blueprint_source_via_trade(
                        context,
                        &client,
                        &intent,
                        &mut checkpoint,
                    )
                    .await?
                };
                if !ready {
                    return Ok(());
                }
            }
            let devices = owned_device_snapshots(&client).await?;
            let source = resolve_blueprint_source(&intent, &checkpoint, &devices)?;
            let factory = resolve_blueprint_factory(&intent, &checkpoint, &devices, &source)?;
            let factory_location = factory
                .location
                .as_ref()
                .map(|location| location.id.as_str().to_owned())
                .ok_or_else(|| format!("Autofactory {} has no location", factory.key.id))?;
            let source_code = source.key.id.as_str().to_owned();
            let factory_code = factory.key.id.as_str().to_owned();

            checkpoint.source_device = Some(source_code.clone());
            checkpoint.autofactory = Some(factory_code.clone());
            checkpoint.autofactory_location = Some(factory_location.clone());
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;
            claim(context, ResourceKey::Autofactory(factory_code.clone()))?;

            let source_needs_control = checkpoint.control_escort_required
                || source
                    .status
                    .as_ref()
                    .is_some_and(|status| status.as_str().eq_ignore_ascii_case("out_of_range"));
            if source_needs_control {
                checkpoint.control_escort_required = true;
                let source_location = blueprint_source_location(&source, &devices)
                    .ok_or_else(|| {
                        format!(
                            "blueprint source {} has no resolvable location for control-range staging",
                            source.key.id
                        )
                    })?
                    .to_owned();
                let source_system = resolve_location_system(&client, &source_location).await?;
                let pinned_replicant = checkpoint
                    .acquisition_replicant
                    .as_deref()
                    .or(intent.acquisition_replicant.as_deref())
                    .or_else(|| {
                        source
                            .relationships
                            .assigned_replicant
                            .as_ref()
                            .map(|replicant| replicant.id.as_str())
                    });
                let escort =
                    resolve_and_claim_replicant(context, &client, pinned_replicant).await?;
                let Some(escort) = escort else {
                    context
                        .advance_to("awaiting_blueprint_control_replicant", &checkpoint)
                        .map_err(string_error)?;
                    context
                        .emit_activity(format!(
                            "blueprint source {source_code} is in {source_system} but no unclaimed Replicant is available to establish local control"
                        ))
                        .map_err(string_error)?;
                    context.mark_waiting().map_err(string_error)?;
                    return Ok(());
                };
                checkpoint.acquisition_replicant = Some(escort.clone());
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
                if !ensure_blueprint_replicant_at(
                    context,
                    &client,
                    &checkpoint,
                    &escort,
                    &source_system,
                    true,
                    "positioning_blueprint_control",
                )
                .await?
                {
                    return Ok(());
                }
            }

            let source = client
                .devices()
                .refresh(&source_code)
                .await
                .map_err(string_error)?
                .snapshot()
                .await
                .map_err(string_error)?;
            let source =
                prepare_blueprint_source_for_transport(context, &client, &checkpoint, source)
                    .await?;
            let source_location = source
                .location
                .as_ref()
                .map(|location| location.id.as_str().to_owned())
                .ok_or_else(|| {
                    format!(
                        "blueprint source {} has no standing location after preparation",
                        source.key.id
                    )
                })?;

            if !source_location.eq_ignore_ascii_case(&factory_location) {
                context
                    .release_claim(&ResourceKey::Device(source_code.clone()))
                    .map_err(string_error)?;
                let child_id = match checkpoint.logistics_child {
                    Some(id) => id,
                    None => {
                        let existing = context
                            .child_workflows()
                            .map_err(string_error)?
                            .into_iter()
                            .filter(|workflow| workflow.kind == logistics_manifest_workflow_kind())
                            .find_map(|workflow| {
                                let config = workflow.config::<LogisticsManifestIntent>().ok()?;
                                (config.device_codes.iter().any(|code| code == &source_code)
                                    && config.destination.eq_ignore_ascii_case(&factory_location))
                                .then_some(workflow.id)
                            });
                        let id = match existing {
                            Some(id) => id,
                            None => {
                                let child = context
                                    .create_child(new_logistics_manifest_workflow(
                                        blueprint_transport_manifest(
                                            device_type,
                                            &source_code,
                                            &source_location,
                                            &factory_location,
                                        ),
                                    ))
                                    .map_err(string_error)?;
                                context
                                    .repository()
                                    .acquire_claim(
                                        child.id,
                                        ResourceKey::Device(source_code.clone()),
                                    )
                                    .map_err(string_error)?;
                                child.id
                            }
                        };
                        checkpoint.logistics_child = Some(id);
                        context
                            .persist_checkpoint(&checkpoint)
                            .map_err(string_error)?;
                        id
                    }
                };

                loop {
                    let Some(child) = context.repository().read(child_id).map_err(string_error)?
                    else {
                        return Err(format!("blueprint logistics child {child_id} disappeared"));
                    };
                    match child.status {
                        WorkflowStatus::Succeeded => break,
                        WorkflowStatus::Failed | WorkflowStatus::Cancelled => {
                            return Err(format!(
                                "blueprint logistics child {child_id} ended as {:?}: {}",
                                child.status,
                                child.last_error.unwrap_or_default()
                            ));
                        }
                        _ => {
                            context
                                .advance_to("awaiting_transport", &checkpoint)
                                .map_err(string_error)?;
                            match context.control_request().map_err(string_error)? {
                                replicant_workflow::ControlRequest::Continue => {}
                                replicant_workflow::ControlRequest::Pause
                                | replicant_workflow::ControlRequest::Cancel => return Ok(()),
                            }
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }

                if checkpoint.control_escort_required
                    && let Some(escort) = checkpoint.acquisition_replicant.as_deref()
                    && !ensure_blueprint_replicant_at(
                        context,
                        &client,
                        &checkpoint,
                        escort,
                        &factory_location,
                        false,
                        "returning_blueprint_control",
                    )
                    .await?
                {
                    return Ok(());
                }
            }

            if blueprint_is_known(&client, device_type).await? {
                checkpoint.blueprint_verified = true;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
                return context
                    .mark_succeeded(Some(serde_json::json!({
                        "device_type": device_type,
                        "strategy": blueprint_acquisition_strategy(&intent),
                        "source_device": source_code,
                        "autofactory": factory_code,
                    })))
                    .map_err(string_error);
            }

            let source_handle = client
                .devices()
                .get(&source_code)
                .await
                .map_err(string_error)?;
            let source_now = source_handle.snapshot().await.map_err(string_error)?;
            let current_location = source_now
                .location
                .as_ref()
                .map(|location| location.id.as_str())
                .unwrap_or_default();
            if !current_location.eq_ignore_ascii_case(&factory_location) {
                return Err(format!(
                    "selected blueprint source {source_code} is at {current_location}, not Autofactory location {factory_location}"
                ));
            }
            claim_device(context, &source_code)?;

            checkpoint.decommission_authorized = true;
            context
                .advance_to("decommissioning", &checkpoint)
                .map_err(string_error)?;
            let operation = source_handle
                .command(replicant_client::raw::devices::DeviceCommand::Decommission)
                .await
                .map_err(string_error)?;
            checkpoint.decommission_submitted = true;
            checkpoint.decommission_operation = Some(operation.id().as_str().to_owned());
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;
            await_success(&operation).await?;

            if wait_for_blueprint(&client, device_type, 24).await? {
                checkpoint.blueprint_verified = true;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
                return context
                    .mark_succeeded(Some(serde_json::json!({
                        "device_type": device_type,
                        "strategy": blueprint_acquisition_strategy(&intent),
                        "source_device": source_code,
                        "autofactory": factory_code,
                    })))
                    .map_err(string_error);
            }
            Err(format!(
                "decommission of {source_code} completed but blueprint {device_type} was not observed"
            ))
        })
    }
}

struct ExplorationWorkflow;
impl WorkflowExecutor for ExplorationWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: ExplorationIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: ExplorationWorkflowCheckpoint =
                context.checkpoint().map_err(string_error)?;
            release_legacy_exploration_location_claims(context)?;

            let requested_hub = checkpoint.hub.as_deref().or(intent.hub.as_deref());
            let Some(home) = resolve_exploration_home(context, &client, requested_hub).await?
            else {
                return wait_for_exploration_capacity(
                    context,
                    &checkpoint,
                    "awaiting_available_autofactory",
                    "all Autofactories at eligible manufacturing homes are currently claimed by other workflows",
                );
            };
            let ExplorationHomeSelection {
                location: hub,
                unavailable_autofactories,
            } = home;

            let replicant_was_checkpointed = checkpoint.replicant.is_some();
            let replicant = if let Some(value) = checkpoint.replicant.clone() {
                if !try_claim_available(context, ResourceKey::Replicant(value.clone()))? {
                    return wait_for_exploration_capacity(
                        context,
                        &checkpoint,
                        "awaiting_available_replicant",
                        "selected Replicant is currently claimed by another workflow",
                    );
                }
                value
            } else {
                let Some(value) =
                    resolve_and_claim_replicant(context, &client, intent.replicant.as_deref())
                        .await?
                else {
                    return wait_for_exploration_capacity(
                        context,
                        &checkpoint,
                        "awaiting_available_replicant",
                        "all eligible Replicants are currently claimed by other workflows",
                    );
                };
                value
            };

            if !try_claim_available(
                context,
                ResourceKey::Namespaced {
                    namespace: "exploration-target".to_owned(),
                    key: intent.target.clone(),
                },
            )? {
                if !replicant_was_checkpointed {
                    release_exploration_claim(context, &ResourceKey::Replicant(replicant.clone()))?;
                }
                return wait_for_exploration_capacity(
                    context,
                    &checkpoint,
                    "awaiting_exploration_target",
                    "the requested exploration target is already being handled by another workflow",
                );
            }

            checkpoint.replicant = Some(replicant.clone());
            checkpoint.hub = Some(hub.clone());

            let plan_file = scratch_file(context.id(), "relay-plan.json")?;
            if let Some(state) = checkpoint.state.as_ref() {
                restore_relay_checkpoint(&plan_file, state).map_err(string_error)?;
            } else {
                clear_scratch_file(&plan_file)?;
            }
            let request = RelayExpansionRequest {
                replicant,
                hub,
                targets: vec![intent.target.clone()],
                mission_file: plan_file.clone(),
                max_hop_ly: 7.499,
                wait_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
                unavailable_autofactories,
            };
            let planning_signature = if checkpoint.state.is_none() {
                match relay_topology_signature(&client, &request).await {
                    Ok(signature) => Some(signature),
                    Err(error) => {
                        tracing::debug!(
                            workflow_id = %context.id(),
                            target = %intent.target,
                            error = %error,
                            "could not snapshot relay topology; preserving ordinary planner retry behavior"
                        );
                        None
                    }
                }
            } else {
                None
            };
            if let Some(signature) = planning_signature.as_deref()
                && !prepare_topology_replan(&mut checkpoint, signature)
            {
                context
                    .advance_to("awaiting_relay_prerequisites", &checkpoint)
                    .map_err(string_error)?;
                return context.mark_waiting().map_err(string_error);
            }
            context
                .advance_to("exploring", &checkpoint)
                .map_err(string_error)?;
            let result = execute_relay_workflow(&client, &request, |state| {
                let (replicant, devices, factories) = state.resources();
                claim_relay_resource(context, ResourceKey::Replicant(replicant.to_owned()))?;
                for device in devices {
                    claim_relay_resource(context, ResourceKey::Device(device.to_owned()))?;
                }
                let factories = factories
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<BTreeSet<_>>();
                reconcile_exploration_autofactory_claims(context, &factories)?;
                checkpoint.state = Some(state.clone());
                context
                    .advance_to(state.step_name(), &checkpoint)
                    .map_err(|error| error.to_string().into())
            })
            .await;
            match result {
                Ok(report) => {
                    release_exploration_autofactory_claims(context)?;
                    context.mark_succeeded(Some(report)).map_err(string_error)
                }
                Err(error) => {
                    if persist_stale_exploration_replan(
                        context,
                        &mut checkpoint,
                        &plan_file,
                        &intent.target,
                        error.as_ref(),
                    )? {
                        return Ok(());
                    }
                    let message = error.to_string();
                    let class = failure_class(error.as_ref());
                    if resource_claim_contention(error.as_ref()) {
                        release_exploration_autofactory_claims(context)?;
                        tracing::warn!(
                            workflow_id = %context.id(),
                            target = %intent.target,
                            error = %message,
                            "relay expansion encountered temporary workflow resource contention; replanning after the owner releases it"
                        );
                        checkpoint.state = None;
                        clear_scratch_file(&plan_file)?;
                        context
                            .advance_to("awaiting_available_resources", &checkpoint)
                            .map_err(string_error)?;
                        context.mark_waiting().map_err(string_error)
                    } else if relay_failure_is_topology_impossible(error.as_ref())
                        && let Some(signature) = planning_signature
                    {
                        let wait_reason = message.clone();
                        checkpoint.failure_class = class;
                        checkpoint.topology_blocker =
                            Some(ExplorationTopologyBlocker { signature });
                        tracing::warn!(
                            workflow_id = %context.id(),
                            target = %intent.target,
                            error = %wait_reason,
                            "relay expansion topology is disconnected for the current planning inputs; waiting for those inputs to change"
                        );
                        wait_for_exploration_relay_prerequisites(context, &checkpoint, wait_reason)
                    } else if retryable_connectivity_dependency_failure(error.as_ref()) {
                        let wait_reason = message.clone();
                        tracing::warn!(
                            workflow_id = %context.id(),
                            target = %intent.target,
                            error = %wait_reason,
                            "relay expansion is blocked on a recoverable prerequisite; waiting to retry"
                        );
                        wait_for_exploration_relay_prerequisites(context, &checkpoint, wait_reason)
                    } else {
                        checkpoint.failure_class = class;
                        context
                            .persist_checkpoint(&checkpoint)
                            .map_err(string_error)?;
                        if failure_disposition(error.as_ref())
                            == replicant_workflow::WorkflowFailureDisposition::Permanent
                        {
                            context
                                .mark_failed_permanently(message)
                                .map_err(string_error)
                        } else {
                            Err(message)
                        }
                    }
                }
            }
        })
    }
}

struct EventDeliveryWorkflow;
impl WorkflowExecutor for EventDeliveryWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: EventIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: EventDeliveryCheckpoint =
                context.checkpoint().map_err(string_error)?;
            claim_target(context, "event-delivery", &intent.event)?;
            let replicant = match checkpoint.replicant.clone() {
                Some(value) => value,
                None => resolve_replicant(&client, intent.replicant.as_deref()).await?,
            };
            let home = match checkpoint.home.clone() {
                Some(value) => value,
                None => resolve_home(&client, intent.home.as_deref()).await?,
            };
            checkpoint.replicant = Some(replicant.clone());
            checkpoint.home = Some(home.clone());
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;

            let plan_file = scratch_file(context.id(), "event-plan.json")?;
            loop {
                materialize_json(&plan_file, checkpoint.plan_json.as_deref())?;
                if checkpoint.plan_json.is_none() {
                    context
                        .advance_to("planning", &checkpoint)
                        .map_err(string_error)?;
                    let reclaimed = reconcile_terminal_event_stock(context, &client).await?;
                    if reclaimed > 0 {
                        context
                            .emit_activity(format!(
                                "reclaimed {reclaimed} legacy event-stock device(s) before event planning"
                            ))
                            .map_err(string_error)?;
                    }
                    plan_event_mission(
                        &client,
                        &EventPlanningRequest {
                            event: intent.event.clone(),
                            criterion: intent.criterion.clone(),
                            replicant: replicant.clone(),
                            home: home.clone(),
                            plan_file: plan_file.clone(),
                            replace_plan: true,
                        },
                    )
                    .await
                    .map_err(string_error)?;
                    checkpoint.plan_json = Some(read_json(&plan_file)?);
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                }

                let workflow_target =
                    event_mission_workflow_target(&plan_file).map_err(string_error)?;
                context
                    .record_target(workflow_target)
                    .map_err(string_error)?;
                let target = event_mission_target_system(&plan_file).map_err(string_error)?;
                let targets = BTreeSet::from([target]);
                if !reconcile_event_connectivity(
                    context,
                    &client,
                    &mut checkpoint.connectivity_workflows,
                    &mut checkpoint.replan_after_connectivity,
                    &replicant,
                    &home,
                    &targets,
                )
                .await?
                {
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                    context
                        .advance_to("awaiting_ftl_connectivity", &checkpoint)
                        .map_err(string_error)?;
                    context.mark_waiting().map_err(string_error)?;
                    return Ok(());
                }

                if checkpoint.replan_after_connectivity {
                    tracing::info!(
                        workflow_id = %context.id(),
                        event = %intent.event,
                        "FTL connectivity is ready; replanning event mission against fresh state"
                    );
                    checkpoint.plan_json = None;
                    checkpoint.connectivity_workflows.clear();
                    checkpoint.replan_after_connectivity = false;
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                    clear_scratch_file(&plan_file)?;
                    continue;
                }
                break;
            }

            loop {
                materialize_json(&plan_file, checkpoint.plan_json.as_deref())?;
                context
                    .advance_to("staging", &checkpoint)
                    .map_err(string_error)?;
                let result = prestage_event_mission(
                    &client,
                    &EventExecutionRequest::new(
                        plan_file.clone(),
                        Duration::from_secs(DEFAULT_WAIT_SECONDS),
                    ),
                )
                .await
                .map_err(string_error)?;
                checkpoint.plan_json = Some(read_json(&plan_file)?);
                checkpoint.ready = result.ready;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
                if result.ready {
                    return context
                        .mark_succeeded(Some(serde_json::json!({
                            "event": intent.event,
                            "ready": true,
                            "state": result.state,
                        })))
                        .map_err(string_error);
                }
                match context.control_request().map_err(string_error)? {
                    replicant_workflow::ControlRequest::Continue => {}
                    replicant_workflow::ControlRequest::Pause => return Ok(()),
                    replicant_workflow::ControlRequest::Cancel => return Ok(()),
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        })
    }
}

struct EventTourWorkflow;
impl WorkflowExecutor for EventTourWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: EventIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: EventTourCheckpoint = context.checkpoint().map_err(string_error)?;
            claim_target(context, "event-tour", &intent.event)?;

            let child_id = match checkpoint.delivery_child {
                Some(id) => id,
                None => {
                    let existing = find_event_delivery(context, &intent.event)?;
                    let child = match existing {
                        Some(child) => child,
                        None => context
                            .create_child(new_event_delivery_workflow(intent.clone()))
                            .map_err(string_error)?,
                    };
                    checkpoint.delivery_child = Some(child.id);
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                    child.id
                }
            };

            match context.control_request().map_err(string_error)? {
                replicant_workflow::ControlRequest::Continue => {}
                replicant_workflow::ControlRequest::Pause
                | replicant_workflow::ControlRequest::Cancel => return Ok(()),
            }
            let Some(child) = context.repository().read(child_id).map_err(string_error)? else {
                return Err(format!("event delivery child {child_id} disappeared"));
            };
            let child = match child.status {
                WorkflowStatus::Succeeded => child,
                WorkflowStatus::Failed | WorkflowStatus::Cancelled => {
                    return Err(format!(
                        "event delivery child {child_id} ended as {:?}: {}",
                        child.status,
                        child.last_error.unwrap_or_default()
                    ));
                }
                _ => {
                    context
                        .advance_to("awaiting_delivery", &checkpoint)
                        .map_err(string_error)?;
                    context.mark_waiting().map_err(string_error)?;
                    return Ok(());
                }
            };
            let delivery: EventDeliveryCheckpoint = child.checkpoint().map_err(string_error)?;
            let plan_json = delivery.plan_json.ok_or_else(|| {
                "completed event delivery child has no plan checkpoint".to_owned()
            })?;
            let replicant = delivery
                .replicant
                .or(intent.replicant.clone())
                .ok_or_else(|| "event tour could not resolve a replicant".to_owned())?;
            checkpoint.replicant = Some(replicant.clone());
            checkpoint.plan_json = Some(plan_json.clone());
            claim(context, ResourceKey::Replicant(replicant))?;
            context
                .advance_to("resolving", &checkpoint)
                .map_err(string_error)?;
            let plan_file = scratch_file(context.id(), "event-plan.json")?;
            materialize_json(&plan_file, Some(&plan_json))?;
            context
                .record_target(event_mission_workflow_target(&plan_file).map_err(string_error)?)
                .map_err(string_error)?;
            let state = execute_event_mission(
                &client,
                &EventExecutionRequest::new(
                    plan_file.clone(),
                    Duration::from_secs(DEFAULT_WAIT_SECONDS),
                ),
            )
            .await
            .map_err(string_error)?;
            checkpoint.plan_json = Some(read_json(&plan_file)?);
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;
            context.mark_succeeded(Some(state)).map_err(string_error)
        })
    }
}

struct EventCampaignWorkflow {
    item_executor: Arc<dyn EventItemExecutor>,
}
impl WorkflowExecutor for EventCampaignWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        let item_executor = self.item_executor.clone();
        Box::pin(async move {
            let intent: EventCampaignIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: EventCampaignCheckpoint =
                context.checkpoint().map_err(string_error)?;
            claim_target(context, "event-campaign", &intent.region)?;
            let replicant = match checkpoint.replicant.clone() {
                Some(value) => value,
                None => resolve_replicant(&client, None).await?,
            };
            let home = match checkpoint.home.clone() {
                Some(value) => value,
                None if !intent.home.is_empty() => intent.home.clone(),
                None => resolve_home(&client, None).await?,
            };
            checkpoint.replicant = Some(replicant.clone());
            checkpoint.home = Some(home.clone());
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;

            let plan_file = scratch_file(context.id(), "event-campaign.json")?;
            loop {
                if let Some(archive) = checkpoint.archive.as_ref() {
                    restore_event_campaign(&plan_file, archive).map_err(string_error)?;
                } else {
                    clear_scratch_file(&plan_file)?;
                    context
                        .advance_to("planning", &checkpoint)
                        .map_err(string_error)?;
                    let reclaimed = reconcile_terminal_event_stock(context, &client).await?;
                    if reclaimed > 0 {
                        context
                            .emit_activity(format!(
                                "reclaimed {reclaimed} legacy event-stock device(s) before campaign planning"
                            ))
                            .map_err(string_error)?;
                    }
                    plan_event_campaign(
                        &client,
                        &EventCampaignPlanningRequest {
                            region: intent.region.clone(),
                            replicant: replicant.clone(),
                            home: home.clone(),
                            plan_file: plan_file.clone(),
                            replace_plan: true,
                        },
                    )
                    .await
                    .map_err(string_error)?;
                    checkpoint.archive =
                        Some(archive_event_campaign(&plan_file).map_err(string_error)?);
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                }

                if let Some(archive) = checkpoint.archive.as_ref() {
                    let targets = event_campaign_workflow_targets(archive).map_err(string_error)?;
                    context.replace_targets(&targets).map_err(string_error)?;
                }

                if !ensure_event_campaign_connectivity(
                    context,
                    &client,
                    &mut checkpoint,
                    &replicant,
                    &home,
                    &plan_file,
                )
                .await?
                {
                    if context.control_request().map_err(string_error)? != ControlRequest::Continue
                    {
                        return Ok(());
                    }
                    continue;
                }

                if checkpoint.replan_after_connectivity {
                    tracing::info!(
                        workflow_id = %context.id(),
                        region = %intent.region,
                        "FTL connectivity is ready; replanning event campaign against fresh state"
                    );
                    checkpoint.archive = None;
                    checkpoint.connectivity_workflows.clear();
                    checkpoint.replan_after_connectivity = false;
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                    clear_scratch_file(&plan_file)?;
                    continue;
                }
                break;
            }

            let repository = context.repository_handle();
            let archive = checkpoint
                .archive
                .as_ref()
                .ok_or_else(|| "event campaign planning produced no archive".to_owned())?;
            let reconciled = repository
                .reconcile_work_items(
                    context.id(),
                    &event_campaign_work_item_specs(context.id(), archive, &intent.region)
                        .map_err(string_error)?,
                    unix_millis(),
                )
                .map_err(string_error)?;
            for item in reconciled {
                let complete = item.spec.payload_json["legacy_complete"].as_bool() == Some(true);
                if !item.state.status.is_terminal() && complete {
                    repository
                        .transition_work_item(
                            item.id,
                            item.state.revision,
                            WorkItemTransition::Skipped {
                                reason: "completed in migrated event checkpoint".into(),
                                result_json: Some(item.spec.payload_json.clone()),
                            },
                            unix_millis(),
                        )
                        .map_err(string_error)?;
                }
            }
            context
                .advance_to("executing", &checkpoint)
                .map_err(string_error)?;
            loop {
                loop {
                    let broker = crate::assignment::ResourceBroker::with_managed_client(
                        repository.clone(),
                        client.clone(),
                    );
                    let candidates = crate::workflows::regional_relay_candidates(
                        repository.as_ref(),
                        &client,
                        broker.discover_candidates().map_err(string_error)?,
                        &intent.region,
                    )?;
                    let mut running = Vec::new();
                    while running.len() < candidates.len().max(1) {
                        let Some(assigned) = repository
                            .claim_next_work_item(context.id(), unix_millis())
                            .map_err(string_error)?
                        else {
                            break;
                        };
                        let allocations = match broker.allocate(
                            assigned.id,
                            assigned.state.revision,
                            &candidates,
                        ) {
                            Ok(allocations) => allocations,
                            Err(_) => {
                                repository
                                    .transition_work_item(
                                        assigned.id,
                                        assigned.state.revision,
                                        WorkItemTransition::Reclaimed {
                                            checkpoint_json: assigned.state.checkpoint_json.clone(),
                                        },
                                        unix_millis(),
                                    )
                                    .map_err(string_error)?;
                                break;
                            }
                        };
                        let worker = allocation_worker(&allocations)
                            .ok_or_else(|| "event item allocation omitted worker".to_owned())?;
                        let assignment_id = format!("event:{}:{worker}", assigned.id);
                        repository
                            .assign_work_item(
                                assigned.id,
                                assigned.state.revision,
                                &assignment_id,
                                &ResourceKey::Replicant(worker.clone()),
                                unix_millis(),
                            )
                            .map_err(string_error)?;
                        let started = repository
                            .start_work_item(
                                assigned.id,
                                assigned.state.revision,
                                &worker,
                                &assignment_id,
                                unix_millis(),
                            )
                            .map_err(string_error)?;
                        let mission_json =
                            event_item_input_checkpoint(repository.as_ref(), &started)?;
                        running.push(run_event_campaign_item(
                            repository.clone(),
                            client.clone(),
                            item_executor.clone(),
                            broker.clone(),
                            EventItemRun {
                                replacement_candidates: candidates.clone(),
                                item: started,
                                allocations,
                                mission_json,
                            },
                        ));
                    }
                    if running.is_empty() {
                        break;
                    }
                    for result in futures::future::join_all(running).await {
                        let (mission_path, mission_json) = result?;
                        if let Some(archive) = checkpoint.archive.as_mut() {
                            archive.mission_json.insert(mission_path, mission_json);
                        }
                        context
                            .persist_checkpoint(&checkpoint)
                            .map_err(string_error)?;
                    }
                }
                match repository
                    .aggregate_campaign_result(context.id())
                    .map_err(string_error)?
                {
                    Some(result) if result.workflow_status() == WorkflowStatus::Succeeded => {
                        return context.mark_succeeded(Some(result)).map_err(string_error);
                    }
                    Some(result) => {
                        return context
                            .mark_failed_with_result(
                                "event campaign completed without a successful criterion",
                                result,
                                replicant_workflow::WorkflowFailureDisposition::Permanent,
                            )
                            .map_err(string_error);
                    }
                    None => {
                        let deadline = campaign_retry_deadline(
                            repository.as_ref(),
                            context.id(),
                            unix_millis().saturating_add(
                                i64::try_from(IDLE_CAMPAIGN_RETRY_INTERVAL.as_millis())
                                    .unwrap_or(i64::MAX),
                            ),
                        )
                        .map_err(string_error)?;
                        if !wait_for_campaign_work(
                            context,
                            "event campaign is waiting for durable item dependencies or resource availability",
                            &EVENT_CAMPAIGN_DEPENDENCY_EVENT_NAMES,
                            Some(deadline),
                            IDLE_CAMPAIGN_RETRY_INTERVAL,
                        )
                        .await?
                        {
                            return Ok(());
                        }
                    }
                }
            }
        })
    }
}

pub(crate) fn event_item_input_checkpoint(
    repository: &replicant_workflow::WorkflowRepository,
    item: &WorkItem,
) -> Result<String, String> {
    if let Some(checkpoint) = item.state.checkpoint_json.as_ref().and_then(Value::as_str) {
        return Ok(checkpoint.to_owned());
    }
    if let Some(dependency) = item
        .spec
        .preconditions_json
        .as_array()
        .and_then(|dependencies| dependencies.first())
        .and_then(|dependency| dependency["parameters"]["dedupe_key"].as_str())
        && let Some(checkpoint) = repository
            .list_work_items(item.spec.workflow_id)
            .map_err(string_error)?
            .into_iter()
            .find(|candidate| candidate.spec.dedupe_key == dependency)
            .and_then(|candidate| candidate.state.checkpoint_json)
            .and_then(|checkpoint| checkpoint.as_str().map(str::to_owned))
    {
        return Ok(checkpoint);
    }
    item.spec.payload_json["mission_json"]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| "event item payload omitted mission_json".to_owned())
}

pub(crate) struct EventItemRun {
    pub(crate) replacement_candidates: Vec<replicant_workflow::AllocationCandidate>,
    pub(crate) item: WorkItem,
    pub(crate) allocations: AllocationSet,
    pub(crate) mission_json: String,
}

pub(crate) async fn run_event_campaign_item(
    repository: Arc<replicant_workflow::WorkflowRepository>,
    client: Client,
    item_executor: Arc<dyn EventItemExecutor>,
    broker: crate::assignment::ResourceBroker,
    run: EventItemRun,
) -> Result<(String, String), String> {
    let EventItemRun {
        replacement_candidates,
        item,
        mut allocations,
        mission_json,
    } = run;
    let stage: EventItemStage =
        serde_json::from_value(item.spec.payload_json["stage"].clone()).map_err(string_error)?;
    let mission_path = item.spec.payload_json["mission_path"]
        .as_str()
        .ok_or_else(|| "event item payload omitted mission_path".to_owned())?
        .to_owned();
    loop {
        match item_executor
            .execute(
                &client,
                &mission_json,
                stage,
                &allocations,
                Duration::from_secs(DEFAULT_WAIT_SECONDS),
            )
            .await
        {
            Ok(checkpoint) => {
                repository
                    .transition_work_item(
                        item.id,
                        item.state.revision,
                        WorkItemTransition::Succeeded {
                            checkpoint_json: Some(Value::String(checkpoint.clone())),
                            result_json: Some(serde_json::json!({
                                "event": item.spec.payload_json["event"],
                                "criterion": item.spec.payload_json["criterion"],
                                "stage": stage,
                            })),
                        },
                        unix_millis(),
                    )
                    .map_err(string_error)?;
                return Ok((mission_path, checkpoint));
            }
            Err(error)
                if error
                    .downcast_ref::<EventMissingAllocationError>()
                    .is_some()
                    || failure_class(error.as_ref()) == Some(FailureClass::DeviceTargetMissing) =>
            {
                let typed_missing = error
                    .downcast_ref::<EventMissingAllocationError>()
                    .map(|error| (error.requirement.clone(), error.allocation_id));
                if let Some((requirement, allocation_id)) = match typed_missing {
                    Some(missing) => Some(missing),
                    None => missing_event_allocation(&client, &allocations).await?,
                } {
                    match broker
                        .replace_dead_allocation_from(
                            item.id,
                            allocation_id,
                            &replacement_candidates,
                        )
                        .map_err(string_error)?
                    {
                        replicant_workflow::ReplacementOutcome::Replaced(replacement) => {
                            let allocation = allocations
                                .by_requirement
                                .get_mut(&requirement)
                                .and_then(|allocations| {
                                    allocations
                                        .iter_mut()
                                        .find(|allocation| allocation.id == allocation_id)
                                })
                                .ok_or_else(|| {
                                    format!("event allocation {allocation_id} disappeared")
                                })?;
                            *allocation = replacement;
                            continue;
                        }
                        replicant_workflow::ReplacementOutcome::Waiting => {
                            repository
                                .transition_work_item(
                                    item.id,
                                    item.state.revision,
                                    WorkItemTransition::Waiting {
                                        checkpoint_json: Some(Value::String(mission_json.clone())),
                                        reason: error.to_string(),
                                        retry_at_ms: Some(unix_millis().saturating_add(300_000)),
                                    },
                                    unix_millis(),
                                )
                                .map_err(string_error)?;
                            return Ok((mission_path, mission_json));
                        }
                        replicant_workflow::ReplacementOutcome::Unavailable => {
                            return Ok((mission_path, mission_json));
                        }
                    }
                }
                repository
                    .transition_work_item(
                        item.id,
                        item.state.revision,
                        WorkItemTransition::Waiting {
                            checkpoint_json: Some(Value::String(mission_json.clone())),
                            reason: error.to_string(),
                            retry_at_ms: Some(unix_millis().saturating_add(300_000)),
                        },
                        unix_millis(),
                    )
                    .map_err(string_error)?;
                return Ok((mission_path, mission_json));
            }
            Err(error) => {
                let transition = if failure_disposition(error.as_ref())
                    == replicant_workflow::WorkflowFailureDisposition::Permanent
                {
                    WorkItemTransition::Failed {
                        error: error.to_string(),
                        result_json: None,
                    }
                } else {
                    WorkItemTransition::Waiting {
                        checkpoint_json: Some(Value::String(mission_json.clone())),
                        reason: error.to_string(),
                        retry_at_ms: Some(unix_millis().saturating_add(300_000)),
                    }
                };
                repository
                    .transition_work_item(item.id, item.state.revision, transition, unix_millis())
                    .map_err(string_error)?;
                return Ok((mission_path, mission_json));
            }
        }
    }
}

async fn missing_event_allocation(
    client: &Client,
    allocations: &AllocationSet,
) -> Result<Option<(String, replicant_workflow::AllocationId)>, String> {
    for (requirement, values) in &allocations.by_requirement {
        for allocation in values {
            let ResourceKey::Device(code) = &allocation.resource else {
                continue;
            };
            if let Err(error) = client.devices().get(code).await
                && crate::failure::device_fetch_is_missing(&error)
            {
                return Ok(Some((requirement.clone(), allocation.id)));
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
fn persist_retryable_event_campaign_failure(
    context: &mut WorkflowContext,
    checkpoint: &mut EventCampaignCheckpoint,
    plan_file: &Path,
    error: &(dyn std::error::Error + 'static),
) -> Result<bool, String> {
    if !retryable_event_campaign_failure(error) {
        return Ok(false);
    }
    let message = error.to_string();
    if event_campaign_failure_requires_replan(error) {
        checkpoint.archive = None;
        clear_scratch_file(plan_file)?;
        context
            .advance_to("replanning_after_stale_asset", checkpoint)
            .map_err(string_error)?;
    } else {
        if let Ok(archive) = archive_event_campaign(plan_file) {
            checkpoint.archive = Some(archive);
        }
        context
            .advance_to(event_campaign_wait_step(error), checkpoint)
            .map_err(string_error)?;
    }
    context
        .persist_checkpoint(checkpoint)
        .map_err(string_error)?;
    context
        .emit_activity(format!(
            "event campaign hit a recoverable execution condition ({message}); waiting to retry"
        ))
        .map_err(string_error)?;
    context.mark_waiting().map_err(string_error)?;
    Ok(true)
}

#[cfg(test)]
fn retryable_event_campaign_failure(error: &(dyn std::error::Error + 'static)) -> bool {
    matches!(
        failure_class(error),
        Some(
            FailureClass::EventInputsUnavailable
                | FailureClass::EventControlUnavailable
                | FailureClass::EventAssetStale
                | FailureClass::DeviceTargetMissing
                | FailureClass::EventExecutorContention
                | FailureClass::TransientUpstream
        )
    )
}

#[cfg(test)]
fn event_campaign_wait_step(error: &(dyn std::error::Error + 'static)) -> &'static str {
    match failure_class(error) {
        Some(FailureClass::EventInputsUnavailable) => "waiting_for_event_inputs",
        Some(FailureClass::TransientUpstream) => "waiting_for_managed_client",
        Some(FailureClass::EventExecutorContention) => "waiting_for_event_executor",
        _ => "waiting_for_control_range",
    }
}

#[cfg(test)]
fn event_campaign_failure_requires_replan(error: &(dyn std::error::Error + 'static)) -> bool {
    matches!(
        failure_class(error),
        Some(FailureClass::EventAssetStale | FailureClass::DeviceTargetMissing)
    )
}

async fn ensure_event_campaign_connectivity(
    context: &mut WorkflowContext,
    client: &Client,
    checkpoint: &mut EventCampaignCheckpoint,
    replicant: &str,
    home: &str,
    plan_file: &Path,
) -> Result<bool, String> {
    let targets = event_campaign_target_systems(plan_file).map_err(string_error)?;
    let ready = reconcile_event_connectivity(
        context,
        client,
        &mut checkpoint.connectivity_workflows,
        &mut checkpoint.replan_after_connectivity,
        replicant,
        home,
        &targets,
    )
    .await?;
    context
        .persist_checkpoint(checkpoint)
        .map_err(string_error)?;
    if !ready {
        context
            .advance_to("awaiting_ftl_connectivity", checkpoint)
            .map_err(string_error)?;
        let deadline = event_connectivity_retry_deadline(
            context.repository(),
            &checkpoint.connectivity_workflows,
        )
        .map_err(string_error)?;
        wait_for_campaign_work(
            context,
            "event campaign is waiting for a durable FTL connectivity dependency",
            &EVENT_CAMPAIGN_DEPENDENCY_EVENT_NAMES,
            deadline,
            EVENT_DEPENDENCY_RECONCILIATION_INTERVAL,
        )
        .await?;
    }
    Ok(ready)
}

pub(crate) async fn reconcile_event_connectivity(
    context: &mut WorkflowContext,
    client: &Client,
    connectivity_workflows: &mut BTreeMap<String, WorkflowId>,
    replan_after_connectivity: &mut bool,
    replicant: &str,
    home: &str,
    targets: &BTreeSet<String>,
) -> Result<bool, String> {
    const EVENT_FTL_RANGE_LY: f64 = 7.499;

    let home_system = system_designation(home);
    let mut completed_dependencies = BTreeMap::<String, WorkflowId>::new();
    // A live relay child is authoritative evidence that the dependency is still
    // being worked. Avoid rebuilding the whole local relay graph every 30
    // seconds while that child is running; only re-evaluate topology after it
    // reaches a terminal state.
    for target in targets {
        let Some(workflow_id) = connectivity_workflows.get(target).copied() else {
            continue;
        };
        let Some(workflow) = context
            .repository()
            .read(workflow_id)
            .map_err(string_error)?
        else {
            connectivity_workflows.remove(target);
            continue;
        };
        match workflow.status {
            WorkflowStatus::Succeeded => {
                completed_dependencies.insert(target.clone(), workflow_id);
            }
            WorkflowStatus::Failed | WorkflowStatus::Cancelled => {
                let class = workflow
                    .checkpoint::<ExplorationWorkflowCheckpoint>()
                    .ok()
                    .and_then(|checkpoint| checkpoint.failure_class)
                    .or_else(|| {
                        workflow
                            .last_error
                            .as_deref()
                            .and_then(failure_class_from_message)
                    });
                let error = workflow
                    .last_error
                    .as_deref()
                    .unwrap_or("no error was recorded");
                if workflow.status == WorkflowStatus::Failed
                    && matches!(
                        class,
                        Some(
                            FailureClass::ConnectivityDependency | FailureClass::TransientUpstream
                        )
                    )
                {
                    let retry_cooldown_ms =
                        i64::try_from(EVENT_CONNECTIVITY_RETRY_COOLDOWN.as_millis())
                            .unwrap_or(i64::MAX);
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
                        .unwrap_or_default();
                    if now.saturating_sub(workflow.updated_at) < retry_cooldown_ms {
                        tracing::debug!(
                            workflow_id = %context.id(),
                            connectivity_workflow_id = %workflow_id,
                            target = %target,
                            error = %error,
                            "FTL connectivity dependency is blocked; retaining failed child during retry cooldown"
                        );
                        return Ok(false);
                    }
                    tracing::info!(
                        workflow_id = %context.id(),
                        connectivity_workflow_id = %workflow_id,
                        target = %target,
                        "FTL connectivity blocker cooldown elapsed; retrying expansion"
                    );
                    connectivity_workflows.remove(target);
                    continue;
                }
                return Err(format!(
                    "FTL connectivity dependency {workflow_id} for {target} ended as {:?}: {error}",
                    workflow.status
                ));
            }
            _ => return Ok(false),
        }
    }

    let reachable =
        ftl_network_reachable_systems(client, &home_system, targets, EVENT_FTL_RANGE_LY)
            .await
            .map_err(string_error)?;
    // Expansion children share the event worker, so satisfy disconnected
    // systems serially. This avoids creating several workflows that would all
    // contend for the same Replicant claim while still reusing any existing
    // compatible expansion already in flight.
    for target in targets {
        if reachable.contains(target) {
            connectivity_workflows.remove(target);
            continue;
        }

        if let Some(workflow_id) = completed_dependencies.get(target) {
            tracing::warn!(
                workflow_id = %context.id(),
                connectivity_workflow_id = %workflow_id,
                target = %target,
                home_system = %home_system,
                "FTL expansion succeeded but managed relay topology has not observed connectivity yet"
            );
            return Ok(false);
        }

        let workflow_id =
            if let Some(existing) = active_connectivity_workflow(context, target, &home_system)? {
                tracing::info!(
                    workflow_id = %context.id(),
                    connectivity_workflow_id = %existing,
                    target = %target,
                    home_system = %home_system,
                    "event workflow is reusing active FTL expansion"
                );
                existing
            } else {
                let child = context
                    .create_child(new_exploration_workflow(ExplorationIntent {
                        target: target.clone(),
                        replicant: Some(replicant.to_owned()),
                        hub: Some(home.to_owned()),
                    }))
                    .map_err(string_error)?;
                tracing::info!(
                    event = "event.ftl.connectivity_required",
                    workflow_id = %context.id(),
                    connectivity_workflow_id = %child.id,
                    target = %target,
                    home_system = %home_system,
                    replicant = %replicant,
                    "event workflow launched prerequisite FTL expansion"
                );
                child.id
            };
        connectivity_workflows.insert(target.clone(), workflow_id);
        *replan_after_connectivity = true;
        return Ok(false);
    }
    Ok(true)
}

fn persist_stale_exploration_replan(
    context: &mut WorkflowContext,
    checkpoint: &mut ExplorationWorkflowCheckpoint,
    plan_file: &Path,
    target: &str,
    error: &(dyn std::error::Error + 'static),
) -> Result<bool, String> {
    if !stale_relay_plan_failure(error) {
        return Ok(false);
    }
    release_exploration_autofactory_claims(context)?;
    tracing::warn!(
        workflow_id = %context.id(),
        target,
        error = %error,
        "relay topology changed underneath the saved plan; discarding it and replanning"
    );
    checkpoint.state = None;
    checkpoint.topology_blocker = None;
    clear_scratch_file(plan_file)?;
    context
        .advance_to("replanning_relay_coverage", checkpoint)
        .map_err(string_error)?;
    context.mark_waiting().map_err(string_error)?;
    Ok(true)
}
fn stale_relay_plan_failure(error: &(dyn std::error::Error + 'static)) -> bool {
    matches!(
        failure_class(error),
        Some(FailureClass::RelayPlanStale | FailureClass::DeviceTargetMissing)
    )
}

fn resource_claim_contention(error: &(dyn std::error::Error + 'static)) -> bool {
    failure_class(error) == Some(FailureClass::ResourceClaimContention)
}
fn prepare_topology_replan(
    checkpoint: &mut ExplorationWorkflowCheckpoint,
    current_signature: &str,
) -> bool {
    if checkpoint
        .topology_blocker
        .as_ref()
        .is_some_and(|blocker| blocker.signature == current_signature)
    {
        return false;
    }
    checkpoint.topology_blocker = None;
    checkpoint.failure_class = None;
    true
}

fn retryable_connectivity_dependency_failure(error: &(dyn std::error::Error + 'static)) -> bool {
    matches!(
        failure_class(error),
        Some(
            FailureClass::ConnectivityDependency
                | FailureClass::ManufacturingCapacity
                | FailureClass::TransientUpstream
        )
    )
}

fn wait_for_exploration_relay_prerequisites(
    context: &mut WorkflowContext,
    checkpoint: &ExplorationWorkflowCheckpoint,
    reason: impl Into<String>,
) -> Result<(), String> {
    record_exploration_wait_reason(context, reason)?;
    context
        .advance_to("awaiting_relay_prerequisites", checkpoint)
        .map_err(string_error)?;
    context.mark_waiting().map_err(string_error)
}

fn record_exploration_wait_reason(
    context: &WorkflowContext,
    reason: impl Into<String>,
) -> Result<(), String> {
    context
        .emit_activity(
            serde_json::to_string(&crate::workflows::WorkflowActivityEvent::WaitReason {
                step: "awaiting_relay_prerequisites".to_owned(),
                reason: reason.into(),
            })
            .map_err(string_error)?,
        )
        .map_err(string_error)
}

fn active_connectivity_workflow(
    context: &WorkflowContext,
    target: &str,
    home_system: &str,
) -> Result<Option<WorkflowId>, String> {
    for workflow in context
        .repository()
        .list_active()
        .map_err(string_error)?
        .into_iter()
        .filter(|workflow| workflow.kind == exploration_workflow_kind())
    {
        let intent = workflow
            .config::<ExplorationIntent>()
            .map_err(string_error)?;
        if intent.target == target
            && intent
                .hub
                .as_deref()
                .is_some_and(|hub| system_designation(hub) == home_system)
        {
            return Ok(Some(workflow.id));
        }
    }
    Ok(None)
}

fn system_designation(location_or_system: &str) -> String {
    location_or_system
        .split('-')
        .next()
        .filter(|system| !system.is_empty())
        .unwrap_or(location_or_system)
        .to_ascii_uppercase()
}

struct RegionEstablishWorkflow;
impl WorkflowExecutor for RegionEstablishWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: RegionEstablishIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: RegionEstablishCheckpoint =
                context.checkpoint().map_err(string_error)?;
            claim(context, ResourceKey::Replicant(intent.operator.clone()))?;
            claim(context, ResourceKey::Replicant(intent.explorer.clone()))?;
            claim_target(context, "region-establish", &intent.region)?;
            claim_target(context, "bootstrap-source", &intent.source_hub)?;
            let plan_file = scratch_file(context.id(), "regional-bootstrap.json")?;
            if let Some(json) = checkpoint.mission_json.as_deref() {
                std::fs::write(&plan_file, json).map_err(string_error)?;
            } else {
                clear_scratch_file(&plan_file)?;
                context
                    .advance_to("planning", &checkpoint)
                    .map_err(string_error)?;
                let mission = plan_bootstrap(
                    &client,
                    &BootstrapPlanningRequest {
                        landing_star: intent.landing_star.clone(),
                        source_hub: intent.source_hub.clone(),
                        operator: intent.operator.clone(),
                        explorer: intent.explorer.clone(),
                        mission_file: plan_file.clone(),
                        replace_plan: true,
                    },
                )
                .await
                .map_err(string_error)?;
                checkpoint.mission_json =
                    Some(serde_json::to_string(&mission).map_err(string_error)?);
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            context
                .advance_to("bootstrapping", &checkpoint)
                .map_err(string_error)?;
            let mission = run_bootstrap(
                &client,
                &BootstrapExecutionRequest::new(
                    plan_file,
                    Duration::from_secs(DEFAULT_WAIT_SECONDS),
                ),
            )
            .await
            .map_err(string_error)?;
            checkpoint.mission_json = Some(serde_json::to_string(&mission).map_err(string_error)?);
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;
            context
                .mark_succeeded(Some(serde_json::json!({
                    "region": mission.region,
                    "capital_system": mission.capital_system,
                    "capital_belt": mission.capital_belt,
                })))
                .map_err(string_error)
        })
    }
}

struct ObservatoryWorkflow;
impl WorkflowExecutor for ObservatoryWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: ObservatoryIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            if let Some(observatory) = intent.observatory.as_deref() {
                claim_device(context, observatory)?;
            }
            context
                .advance_to("prospecting", &Value::Null)
                .map_err(string_error)?;
            let report = auto_prospect(&client, intent.observatory.as_deref())
                .await
                .map_err(string_error)?;
            context.mark_succeeded(Some(report)).map_err(string_error)
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProvisionPrintRole {
    Matrix,
    Cradle,
}

#[derive(Clone, Debug)]
struct ProvisionTaggedDevice {
    code: String,
    device_type: String,
    tags: Vec<String>,
}

#[derive(Default)]
struct ProvisionReconciliation {
    changed: bool,
    completed: usize,
    in_flight: usize,
    duplicate_outputs: usize,
    duplicate_jobs: usize,
}

fn provision_workflow_tag(workflow_id: WorkflowId) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"replicant.provision.manufacturing.v1\0");
    hasher.update(workflow_id.to_string().as_bytes());
    let digest = hasher.finalize();
    let suffix = digest[..10]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("dir-p:{suffix}")
}
fn new_provision_manufacturing(
    workflow_tag: &str,
    cradle_type: &str,
) -> ReplicantManufacturingCheckpoint {
    ReplicantManufacturingCheckpoint {
        matrix: ReplicantPrintIntent {
            device_type: "empty_replicant_matrix".to_owned(),
            job_tag: format!("{workflow_tag}:m"),
            factory_code: None,
            submission_started: false,
            accepted: false,
            operation_id: None,
        },
        cradle: ReplicantPrintIntent {
            device_type: cradle_type.to_owned(),
            job_tag: format!("{workflow_tag}:c"),
            factory_code: None,
            submission_started: false,
            accepted: false,
            operation_id: None,
        },
    }
}

fn provision_print(
    manufacturing: &ReplicantManufacturingCheckpoint,
    role: ProvisionPrintRole,
) -> &ReplicantPrintIntent {
    match role {
        ProvisionPrintRole::Matrix => &manufacturing.matrix,
        ProvisionPrintRole::Cradle => &manufacturing.cradle,
    }
}

fn provision_print_mut(
    manufacturing: &mut ReplicantManufacturingCheckpoint,
    role: ProvisionPrintRole,
) -> &mut ReplicantPrintIntent {
    match role {
        ProvisionPrintRole::Matrix => &mut manufacturing.matrix,
        ProvisionPrintRole::Cradle => &mut manufacturing.cradle,
    }
}

fn provision_output(
    checkpoint: &ReplicantProvisionCheckpoint,
    role: ProvisionPrintRole,
) -> Option<&str> {
    match role {
        ProvisionPrintRole::Matrix => checkpoint.matrix.as_deref(),
        ProvisionPrintRole::Cradle => checkpoint.cradle.as_deref(),
    }
}

fn provision_output_mut(
    checkpoint: &mut ReplicantProvisionCheckpoint,
    role: ProvisionPrintRole,
) -> &mut Option<String> {
    match role {
        ProvisionPrintRole::Matrix => &mut checkpoint.matrix,
        ProvisionPrintRole::Cradle => &mut checkpoint.cradle,
    }
}

fn reconcile_provision_evidence(
    checkpoint: &mut ReplicantProvisionCheckpoint,
    workflow_tag: &str,
    devices: &[ProvisionTaggedDevice],
    status: &SystemPrintingStatus,
) -> Result<ProvisionReconciliation, String> {
    let mut report = ProvisionReconciliation::default();
    for role in [ProvisionPrintRole::Matrix, ProvisionPrintRole::Cradle] {
        let device_type = checkpoint
            .manufacturing
            .as_ref()
            .map(|manufacturing| provision_print(manufacturing, role).device_type.clone())
            .ok_or_else(|| "provisioning manufacturing intent is missing".to_owned())?;
        let mut completed = devices
            .iter()
            .filter(|device| {
                device.device_type == device_type
                    && device.tags.iter().any(|tag| tag == workflow_tag)
            })
            .map(|device| device.code.clone())
            .collect::<Vec<_>>();
        completed.sort();
        completed.dedup();
        report.completed += usize::from(!completed.is_empty());
        report.duplicate_outputs += completed.len().saturating_sub(1);
        if provision_output(checkpoint, role).is_none()
            && let Some(code) = completed.first()
        {
            *provision_output_mut(checkpoint, role) = Some(code.clone());
            report.changed = true;
        }
        if provision_output(checkpoint, role).is_some() {
            continue;
        }

        let mut matching_factories = Vec::new();
        let mut matching_jobs = 0usize;
        for factory in &status.factories {
            let active = factory.active.iter();
            let queued = factory.queued.iter();
            let matches = active.chain(queued).filter(|job| {
                job.device_type == device_type
                    && job.quantity >= 1
                    && job.tags.iter().any(|tag| tag == workflow_tag)
            });
            let count = matches.count();
            if count > 0 {
                matching_factories.push(factory.code.clone());
                matching_jobs += count;
            }
        }
        matching_factories.sort();
        matching_factories.dedup();
        if let Some(factory_code) = matching_factories.first() {
            let manufacturing = checkpoint
                .manufacturing
                .as_mut()
                .ok_or_else(|| "provisioning manufacturing intent is missing".to_owned())?;
            let print = provision_print_mut(manufacturing, role);
            if print.factory_code.as_ref() != Some(factory_code)
                || !print.submission_started
                || !print.accepted
            {
                print.factory_code = Some(factory_code.clone());
                print.submission_started = true;
                print.accepted = true;
                report.changed = true;
            }
            report.in_flight += 1;
            report.duplicate_jobs += matching_jobs.saturating_sub(1);
        }
    }
    Ok(report)
}

fn provision_pending_roles(checkpoint: &ReplicantProvisionCheckpoint) -> Vec<ProvisionPrintRole> {
    let Some(manufacturing) = checkpoint.manufacturing.as_ref() else {
        return Vec::new();
    };
    [ProvisionPrintRole::Matrix, ProvisionPrintRole::Cradle]
        .into_iter()
        .filter(|role| {
            provision_output(checkpoint, *role).is_none()
                && !provision_print(manufacturing, *role).submission_started
        })
        .collect()
}

fn provision_tracked_requests(
    checkpoint: &ReplicantProvisionCheckpoint,
    roles: &[ProvisionPrintRole],
) -> Result<Vec<TrackedPrintRequest>, String> {
    let manufacturing = checkpoint
        .manufacturing
        .as_ref()
        .ok_or_else(|| "provisioning manufacturing intent is missing".to_owned())?;
    Ok(roles
        .iter()
        .map(|role| {
            TrackedPrintRequest::new(provision_print(manufacturing, *role).device_type.clone(), 1)
                .authoritative_factory_check()
        })
        .collect())
}

fn apply_provision_print_update(
    checkpoint: &mut ReplicantProvisionCheckpoint,
    roles: &[ProvisionPrintRole],
    workflow_tag: &str,
    update: TrackedPrintUpdate,
) -> Result<Option<Vec<String>>, String> {
    match update {
        TrackedPrintUpdate::Preparing(assignment) => {
            let role = roles
                .get(assignment.request_index)
                .copied()
                .ok_or_else(|| "tracked provisioning request index is invalid".to_owned())?;
            let manufacturing = checkpoint
                .manufacturing
                .as_mut()
                .ok_or_else(|| "provisioning manufacturing intent is missing".to_owned())?;
            let print = provision_print_mut(manufacturing, role);
            print.factory_code = Some(assignment.factory_code);
            print.submission_started = true;
            Ok(Some(vec![workflow_tag.to_owned(), print.job_tag.clone()]))
        }
        TrackedPrintUpdate::OperationRecorded {
            assignment,
            operation_id,
        } => {
            let role = roles
                .get(assignment.request_index)
                .copied()
                .ok_or_else(|| "tracked provisioning request index is invalid".to_owned())?;
            let manufacturing = checkpoint
                .manufacturing
                .as_mut()
                .ok_or_else(|| "provisioning manufacturing intent is missing".to_owned())?;
            provision_print_mut(manufacturing, role).operation_id = Some(operation_id);
            Ok(None)
        }
    }
}

fn matching_factory_jobs(
    factory: &FactoryPrintStatus,
    device_type: &str,
    workflow_tag: &str,
) -> usize {
    factory
        .active
        .iter()
        .chain(factory.queued.iter())
        .filter(|job| {
            job.device_type == device_type
                && job.quantity >= 1
                && job.tags.iter().any(|tag| tag == workflow_tag)
        })
        .count()
}

fn apply_provision_operation_status(
    print: &mut ReplicantPrintIntent,
    status: OperationStatus,
    queue_visible: bool,
) -> bool {
    if status == OperationStatus::Rejected && !queue_visible {
        print.factory_code = None;
        print.submission_started = false;
        print.accepted = false;
        print.operation_id = None;
        return true;
    }
    if (queue_visible
        || matches!(
            status,
            OperationStatus::Accepted
                | OperationStatus::InProgress
                | OperationStatus::AwaitingEvidence
                | OperationStatus::Completed
        ))
        && !print.accepted
    {
        print.accepted = true;
        return true;
    }
    false
}

async fn reconcile_provision_operations(
    client: &Client,
    checkpoint: &mut ReplicantProvisionCheckpoint,
    status: &SystemPrintingStatus,
    workflow_tag: &str,
) -> Result<bool, String> {
    let mut changed = false;
    for role in [ProvisionPrintRole::Matrix, ProvisionPrintRole::Cradle] {
        if provision_output(checkpoint, role).is_some() {
            continue;
        }
        let manufacturing = checkpoint
            .manufacturing
            .as_mut()
            .ok_or_else(|| "provisioning manufacturing intent is missing".to_owned())?;
        let print = provision_print_mut(manufacturing, role);
        let Some(operation_id) = print.operation_id.clone() else {
            continue;
        };
        let operation = client.operations().get(OperationId::from(operation_id));
        let outcome = operation.outcome().await.map_err(string_error)?;
        let queue_visible = print.factory_code.as_deref().is_some_and(|factory_code| {
            status
                .factories
                .iter()
                .find(|factory| factory.code == factory_code)
                .is_some_and(|factory| {
                    matching_factory_jobs(factory, &print.device_type, workflow_tag) > 0
                })
        });
        changed |= apply_provision_operation_status(print, outcome.status, queue_visible);
    }
    Ok(changed)
}

fn validate_provision_manufacturing(
    manufacturing: &ReplicantManufacturingCheckpoint,
    cradle_type: &str,
) -> Result<(), String> {
    if manufacturing.matrix.device_type != "empty_replicant_matrix" {
        return Err("provisioning matrix intent has an incompatible device type".to_owned());
    }
    if manufacturing.cradle.device_type != cradle_type {
        return Err("provisioning cradle intent has an incompatible device type".to_owned());
    }
    Ok(())
}

async fn provision_tagged_devices(
    client: &Client,
    workflow_tag: &str,
    authoritative: bool,
) -> Result<Vec<ProvisionTaggedDevice>, String> {
    let handles = if authoritative {
        client
            .devices()
            .refresh_many()
            .with_tag(workflow_tag.to_owned())
            .collect()
            .await
            .map_err(string_error)?
    } else {
        client
            .devices()
            .find()
            .owned()
            .with_tag(workflow_tag.to_owned())
            .collect()
            .await
            .map_err(string_error)?
    };
    let mut devices = Vec::new();
    for handle in handles {
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if let Some(device_type) = snapshot.device_type.as_ref() {
            devices.push(ProvisionTaggedDevice {
                code: handle.id().as_str().to_owned(),
                device_type: device_type.as_str().to_owned(),
                tags: snapshot.tags,
            });
        }
    }
    devices.sort_by(|left, right| left.code.cmp(&right.code));
    Ok(devices)
}

struct ReplicantProvisionWorkflow;
impl WorkflowExecutor for ReplicantProvisionWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: ReplicantProvisionIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: ReplicantProvisionCheckpoint =
                context.checkpoint().map_err(string_error)?;
            claim_target(context, "regional-workforce", &intent.region)?;
            claim(
                context,
                ResourceKey::Replicant(intent.source_replicant.clone()),
            )?;
            let tag = checkpoint
                .tag
                .get_or_insert_with(|| provision_workflow_tag(context.id()))
                .clone();
            if checkpoint.manufacturing.is_none() {
                checkpoint.manufacturing =
                    Some(new_provision_manufacturing(&tag, &intent.cradle_type));
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            validate_provision_manufacturing(
                checkpoint
                    .manufacturing
                    .as_ref()
                    .ok_or_else(|| "provisioning manufacturing intent is missing".to_owned())?,
                &intent.cradle_type,
            )?;
            context
                .advance_to("manufacturing", &checkpoint)
                .map_err(string_error)?;

            let all_requests = [
                PrintRequest::new("empty_replicant_matrix", 1),
                PrintRequest::new(intent.cradle_type.clone(), 1),
            ];
            let mut authoritative = true;
            loop {
                let initial_reconciliation = authoritative;
                let devices = provision_tagged_devices(&client, &tag, authoritative).await?;
                authoritative = false;
                let status = printing_status_in_system(
                    &client,
                    &intent.home,
                    &all_requests,
                    std::slice::from_ref(&tag),
                )
                .await
                .map_err(string_error)?;
                let mut reconciliation =
                    reconcile_provision_evidence(&mut checkpoint, &tag, &devices, &status)?;
                reconciliation.changed |=
                    reconcile_provision_operations(&client, &mut checkpoint, &status, &tag).await?;
                if reconciliation.changed {
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                }
                if (initial_reconciliation || reconciliation.changed)
                    && (reconciliation.duplicate_outputs > 0 || reconciliation.duplicate_jobs > 0)
                {
                    tracing::warn!(
                        workflow_id = %context.id(),
                        manufacturing_tag = %tag,
                        duplicate_outputs = reconciliation.duplicate_outputs,
                        duplicate_jobs = reconciliation.duplicate_jobs,
                        "provisioning reconciliation found duplicate or orphaned manufacturing output"
                    );
                }
                if initial_reconciliation || reconciliation.changed {
                    let manufacturing = checkpoint
                        .manufacturing
                        .as_ref()
                        .ok_or_else(|| "provisioning manufacturing intent is missing".to_owned())?;
                    let in_flight_intents =
                        [ProvisionPrintRole::Matrix, ProvisionPrintRole::Cradle]
                            .into_iter()
                            .filter(|role| {
                                provision_output(&checkpoint, *role).is_none()
                                    && provision_print(manufacturing, *role).submission_started
                            })
                            .count();
                    tracing::info!(
                        workflow_id = %context.id(),
                        manufacturing_tag = %tag,
                        completed_outputs = reconciliation.completed,
                        in_flight_intents,
                        "reconciled provisioning manufacturing evidence"
                    );
                }
                if checkpoint.matrix.is_some() && checkpoint.cradle.is_some() {
                    break;
                }

                let pending_roles = provision_pending_roles(&checkpoint);
                let unresolved_intents = 2usize
                    .saturating_sub(reconciliation.completed)
                    .saturating_sub(pending_roles.len());
                if initial_reconciliation || reconciliation.changed {
                    tracing::info!(
                        workflow_id = %context.id(),
                        manufacturing_tag = %tag,
                        remaining_print_deficit = pending_roles.len(),
                        unresolved_intents,
                        "calculated provisioning manufacturing deficit"
                    );
                }
                if !pending_roles.is_empty() {
                    let manufacturing = checkpoint
                        .manufacturing
                        .as_ref()
                        .ok_or_else(|| "provisioning manufacturing intent is missing".to_owned())?;
                    let pending_requests = pending_roles
                        .iter()
                        .map(|role| {
                            PrintRequest::new(
                                provision_print(manufacturing, *role).device_type.clone(),
                                1,
                            )
                        })
                        .collect::<Vec<_>>();
                    let mut options = QueueOptions::at(intent.home.clone());
                    options.tags = vec![tag.clone()];
                    options.wait_timeout = Duration::from_secs(DEFAULT_WAIT_SECONDS);
                    queue_print_prerequisites(&client, &pending_requests, &options)
                        .await
                        .map_err(string_error)?;
                    let blueprints = fetch_blueprints(&client).await.map_err(string_error)?;
                    let tracked_requests = provision_tracked_requests(&checkpoint, &pending_roles)?;
                    let report = match queue_tracked_prints_once(
                        &client,
                        &tracked_requests,
                        &options,
                        &blueprints,
                        |update| {
                            let tags = apply_provision_print_update(
                                &mut checkpoint,
                                &pending_roles,
                                &tag,
                                update,
                            )?;
                            context
                                .persist_checkpoint(&checkpoint)
                                .map_err(string_error)?;
                            Ok(tags)
                        },
                    )
                    .await
                    {
                        Ok(report) => Some(report),
                        Err(PrintingError::SubmissionUnresolved {
                            operation_id,
                            status,
                        }) => {
                            tracing::warn!(
                                workflow_id = %context.id(),
                                manufacturing_tag = %tag,
                                operation_id,
                                ?status,
                                "provisioning print submission is unresolved; awaiting durable evidence"
                            );
                            None
                        }
                        Err(PrintingError::SubmissionRejected {
                            operation_id,
                            status,
                            ..
                        }) => {
                            tracing::warn!(
                                workflow_id = %context.id(),
                                manufacturing_tag = %tag,
                                operation_id,
                                ?status,
                                "provisioning print operation ended without acceptance; reconciling before any retry"
                            );
                            None
                        }
                        Err(error) => return Err(string_error(error)),
                    };
                    if let Some(report) = report
                        && !report.submissions.is_empty()
                    {
                        let manufacturing = checkpoint.manufacturing.as_mut().ok_or_else(|| {
                            "provisioning manufacturing intent is missing".to_owned()
                        })?;
                        for submission in report.submissions {
                            let role = pending_roles[submission.assignment.request_index];
                            provision_print_mut(manufacturing, role).accepted = true;
                        }
                        context
                            .persist_checkpoint(&checkpoint)
                            .map_err(string_error)?;
                        continue;
                    }
                }

                match context.control_request().map_err(string_error)? {
                    ControlRequest::Continue => {}
                    ControlRequest::Pause | ControlRequest::Cancel => return Ok(()),
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }

            let matrix = checkpoint
                .matrix
                .clone()
                .ok_or_else(|| "provisioned empty Replicant matrix was not found".to_owned())?;
            let cradle = checkpoint
                .cradle
                .clone()
                .ok_or_else(|| "provisioned cradle vessel was not found".to_owned())?;
            tracing::info!(
                workflow_id = %context.id(),
                manufacturing_tag = %tag,
                matrix = %matrix,
                cradle = %cradle,
                "adopted reconciled provisioning outputs"
            );
            claim_device(context, &matrix)?;
            claim_device(context, &cradle)?;
            if !checkpoint.stowed {
                context
                    .advance_to("stowing_matrix", &checkpoint)
                    .map_err(string_error)?;
                let operation = client
                    .devices()
                    .get(&matrix)
                    .await
                    .map_err(string_error)?
                    .command(replicant_client::raw::devices::DeviceCommand::Stow {
                        target: Some(cradle.clone()),
                    })
                    .await
                    .map_err(string_error)?;
                await_success(&operation).await?;
                checkpoint.stowed = true;
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            if checkpoint.new_replicant.is_none() {
                context
                    .advance_to("replicating", &checkpoint)
                    .map_err(string_error)?;
                let source_matrix =
                    source_matrix_for_replicant(&client, &intent.source_replicant).await?;
                claim_device(context, &source_matrix)?;
                let operation = client
                    .devices()
                    .get(&source_matrix)
                    .await
                    .map_err(string_error)?
                    .command(replicant_client::raw::devices::DeviceCommand::Replicate {
                        target: matrix.clone(),
                        name: intent.name.clone(),
                    })
                    .await
                    .map_err(string_error)?;
                await_success(&operation).await?;
                client
                    .sync()
                    .domain(replicant_client::SyncDomain::Replicants)
                    .await
                    .map_err(string_error)?;
                let cradle_snapshot = client
                    .devices()
                    .get(&cradle)
                    .await
                    .map_err(string_error)?
                    .snapshot()
                    .await
                    .map_err(string_error)?;
                checkpoint.new_replicant = cradle_snapshot
                    .relationships
                    .hosting_replicant
                    .as_ref()
                    .map(|replicant| replicant.id.as_str().to_owned());
                if checkpoint.new_replicant.is_none() {
                    return Err("replication completed but the new Replicant is not yet visible in managed state".to_owned());
                }
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            context
                .mark_succeeded(Some(serde_json::json!({
                    "region": intent.region,
                    "replicant": checkpoint.new_replicant,
                    "cradle": cradle,
                })))
                .map_err(string_error)
        })
    }
}

/// Creates a queued system-scan workflow.
pub fn new_scan_system_workflow(
    intent: ScanIntent,
) -> NewWorkflow<ScanIntent, ControllerWorkflowCheckpoint> {
    queued_workflow(
        scan_system_workflow_kind(),
        intent,
        ControllerWorkflowCheckpoint::default(),
    )
}

/// Creates a queued belt-search workflow.
pub fn new_scan_belt_workflow(
    intent: ScanIntent,
) -> NewWorkflow<ScanIntent, ControllerWorkflowCheckpoint> {
    queued_workflow(
        scan_belt_workflow_kind(),
        intent,
        ControllerWorkflowCheckpoint::default(),
    )
}

/// Creates a queued bounded survey-tour workflow.
pub fn new_scan_tour_workflow(
    intent: ScanTourIntent,
) -> NewWorkflow<ScanTourIntent, ScanTourCheckpoint> {
    queued_workflow(
        scan_tour_workflow_kind(),
        intent,
        ScanTourCheckpoint::default(),
    )
}

/// Creates a queued bounded fast belt-search campaign.
pub fn new_belt_search_campaign_workflow(
    intent: BeltSearchCampaignIntent,
) -> NewWorkflow<BeltSearchCampaignIntent, BeltSearchCampaignCheckpoint> {
    NewWorkflow {
        kind: belt_search_campaign_workflow_kind(),
        schema_version: 2,
        config: intent,
        checkpoint: BeltSearchCampaignCheckpoint::default(),
        current_step: None,
        parent_id: None,
    }
}

/// Creates a queued salvage workflow.
pub fn new_salvage_workflow(
    intent: SalvageIntent,
) -> NewWorkflow<SalvageIntent, ControllerWorkflowCheckpoint> {
    queued_workflow(
        salvage_workflow_kind(),
        intent,
        ControllerWorkflowCheckpoint::default(),
    )
}

/// Creates a queued regional salvage recovery campaign.
pub fn new_salvage_recovery_workflow(
    intent: SalvageRecoveryIntent,
) -> NewWorkflow<SalvageRecoveryIntent, SalvageRecoveryCheckpoint> {
    queued_workflow(
        salvage_recovery_workflow_kind(),
        intent,
        SalvageRecoveryCheckpoint::default(),
    )
}

/// Creates a queued one-system mining deployment workflow.
pub fn new_mining_deploy_workflow(
    intent: MiningDeployIntent,
) -> NewWorkflow<MiningDeployIntent, MiningDeployCheckpoint> {
    queued_workflow(
        mining_deploy_workflow_kind(),
        intent,
        MiningDeployCheckpoint::default(),
    )
}

/// Creates a queued batch mining expansion workflow.
pub fn new_mining_campaign_workflow(
    intent: MiningCampaignIntent,
) -> NewWorkflow<MiningCampaignIntent, MiningCampaignCheckpoint> {
    NewWorkflow {
        kind: mining_campaign_workflow_kind(),
        schema_version: 3,
        config: intent,
        checkpoint: MiningCampaignCheckpoint::default(),
        current_step: Some("queued".to_owned()),
        parent_id: None,
    }
}

/// Creates a queued logistics workflow.
pub fn new_logistics_workflow(
    intent: LogisticsIntent,
) -> NewWorkflow<LogisticsIntent, LogisticsWorkflowCheckpoint> {
    queued_workflow(
        logistics_workflow_kind(),
        intent,
        LogisticsWorkflowCheckpoint::default(),
    )
}

/// Creates a queued regional provisioning and dispatch workflow.
pub fn new_regional_dispatch_workflow(
    intent: RegionalDispatchIntent,
) -> NewWorkflow<RegionalDispatchIntent, RegionalDispatchCheckpoint> {
    queued_workflow(
        regional_dispatch_workflow_kind(),
        intent,
        RegionalDispatchCheckpoint::default(),
    )
}

/// Creates a queued mixed-manifest logistics workflow for Director/coordinator use.
pub fn new_logistics_manifest_workflow(
    intent: LogisticsManifestIntent,
) -> NewWorkflow<LogisticsManifestIntent, LogisticsWorkflowCheckpoint> {
    queued_workflow(
        logistics_manifest_workflow_kind(),
        intent,
        LogisticsWorkflowCheckpoint::default(),
    )
}

/// Creates a queued provisioned trade-fulfillment workflow.
pub fn new_trade_fulfillment_workflow(
    intent: TradeFulfillmentIntent,
) -> NewWorkflow<TradeFulfillmentIntent, TradeFulfillmentCheckpoint> {
    queued_workflow(
        trade_fulfillment_workflow_kind(),
        intent,
        TradeFulfillmentCheckpoint::default(),
    )
}

/// Creates a queued owned-copy or shop blueprint acquisition workflow.
pub fn new_blueprint_acquire_workflow(
    intent: BlueprintAcquireIntent,
) -> NewWorkflow<BlueprintAcquireIntent, BlueprintAcquireCheckpoint> {
    queued_workflow(
        blueprint_acquire_workflow_kind(),
        intent,
        BlueprintAcquireCheckpoint::default(),
    )
}

/// Creates a queued directed exploration workflow.
pub fn new_exploration_workflow(
    intent: ExplorationIntent,
) -> NewWorkflow<ExplorationIntent, ExplorationWorkflowCheckpoint> {
    queued_workflow(
        exploration_workflow_kind(),
        intent,
        ExplorationWorkflowCheckpoint::default(),
    )
}

/// Creates a queued event-delivery workflow.
pub fn new_event_delivery_workflow(
    intent: EventIntent,
) -> NewWorkflow<EventIntent, EventDeliveryCheckpoint> {
    queued_workflow(
        event_delivery_workflow_kind(),
        intent,
        EventDeliveryCheckpoint::default(),
    )
}

/// Creates a queued event-tour workflow.
pub fn new_event_tour_workflow(
    intent: EventIntent,
) -> NewWorkflow<EventIntent, EventTourCheckpoint> {
    queued_workflow(
        event_tour_workflow_kind(),
        intent,
        EventTourCheckpoint::default(),
    )
}

/// Creates a queued grow-only Replicant provisioning workflow.
pub fn new_replicant_provision_workflow(
    intent: ReplicantProvisionIntent,
) -> NewWorkflow<ReplicantProvisionIntent, ReplicantProvisionCheckpoint> {
    queued_workflow(
        replicant_provision_workflow_kind(),
        intent,
        ReplicantProvisionCheckpoint::default(),
    )
}

/// Creates a queued regional event campaign workflow.
pub fn new_event_campaign_workflow(
    intent: EventCampaignIntent,
) -> NewWorkflow<EventCampaignIntent, EventCampaignCheckpoint> {
    queued_workflow(
        event_campaign_workflow_kind(),
        intent,
        EventCampaignCheckpoint::default(),
    )
}

/// Creates a queued autonomous regional bootstrap workflow.
pub fn new_region_establish_workflow(
    intent: RegionEstablishIntent,
) -> NewWorkflow<RegionEstablishIntent, RegionEstablishCheckpoint> {
    queued_workflow(
        region_establish_workflow_kind(),
        intent,
        RegionEstablishCheckpoint::default(),
    )
}

/// Creates a queued observatory prospect workflow.
pub fn new_observatory_workflow(
    intent: ObservatoryIntent,
) -> NewWorkflow<ObservatoryIntent, Value> {
    queued_workflow(observatory_workflow_kind(), intent, Value::Null)
}

fn queued_workflow<C, P>(kind: WorkflowKind, config: C, checkpoint: P) -> NewWorkflow<C, P> {
    NewWorkflow {
        kind,
        schema_version: SCHEMA_VERSION,
        config,
        checkpoint,
        current_step: Some("queued".to_owned()),
        parent_id: None,
    }
}

fn default_tour_radius() -> f64 {
    30.0
}

fn default_tour_limit() -> usize {
    80
}

fn default_true() -> bool {
    true
}

fn default_quantity() -> i64 {
    1
}

fn event_delivery_matches(
    workflow: &replicant_workflow::WorkflowInstance,
    kind: &replicant_workflow::WorkflowKind,
    event: &str,
) -> bool {
    if &workflow.kind != kind
        || matches!(
            workflow.status,
            WorkflowStatus::Failed | WorkflowStatus::Cancelled
        )
    {
        return false;
    }
    workflow
        .config::<EventIntent>()
        .is_ok_and(|intent| intent.event == event)
}

fn find_event_delivery(
    context: &WorkflowContext,
    event: &str,
) -> Result<Option<replicant_workflow::WorkflowInstance>, String> {
    let kind = event_delivery_workflow_kind();

    if let Some(child) = context
        .child_workflows()
        .map_err(string_error)?
        .into_iter()
        .filter(|workflow| event_delivery_matches(workflow, &kind, event))
        .max_by_key(|workflow| workflow.created_at)
    {
        return Ok(Some(child));
    }

    let existing = context
        .repository()
        .list()
        .map_err(string_error)?
        .into_iter()
        .filter(|workflow| event_delivery_matches(workflow, &kind, event))
        .max_by_key(|workflow| workflow.created_at);
    Ok(existing)
}

fn managed_client(context: &WorkflowContext) -> Result<Client, String> {
    context
        .managed_client()
        .cloned()
        .ok_or_else(|| "automation workflow requires a managed client".to_owned())
}

async fn reconcile_terminal_event_stock(
    context: &WorkflowContext,
    client: &Client,
) -> Result<usize, String> {
    let report = reconcile_event_stock(
        client,
        context.repository(),
        EventStockReconcileOptions {
            execute: true,
            reclaim_unknown_orphans: false,
        },
    )
    .await
    .map_err(string_error)?;
    Ok(report.event_reclaims + report.regional_stock_reclaims)
}

fn string_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn claim(context: &WorkflowContext, key: ResourceKey) -> Result<(), String> {
    match context.acquire_claim(key) {
        Ok(_) => Ok(()),
        Err(RepositoryError::ClaimConflict { owner, .. }) => {
            Err(format!("resource is already claimed by workflow {owner}"))
        }
        Err(error) => Err(string_error(error)),
    }
}

fn claim_device(context: &WorkflowContext, code: &str) -> Result<(), String> {
    claim(context, ResourceKey::Device(code.to_owned()))
}

fn claim_devices_until_conflict<I, S>(
    context: &WorkflowContext,
    codes: I,
) -> Result<Option<(String, WorkflowId)>, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut newly_acquired = Vec::new();
    for code in codes {
        let code = code.as_ref();
        let resource = ResourceKey::Device(code.to_owned());
        match context.acquire_claim(resource.clone()) {
            Ok(ClaimAcquireOutcome::Acquired(_)) => newly_acquired.push(resource),
            Ok(ClaimAcquireOutcome::AlreadyOwned(_)) => {}
            Err(RepositoryError::ClaimConflict { owner, .. }) => {
                for resource in &newly_acquired {
                    context.release_claim(resource).map_err(string_error)?;
                }
                return Ok(Some((code.to_owned(), owner)));
            }
            Err(error) => {
                for resource in &newly_acquired {
                    context.release_claim(resource).map_err(string_error)?;
                }
                return Err(string_error(error));
            }
        }
    }
    Ok(None)
}

fn claim_relay_resource(
    context: &WorkflowContext,
    key: ResourceKey,
) -> crate::relay::AnyResult<()> {
    context
        .acquire_claim(key)
        .map(|_| ())
        .map_err(|error| Box::new(error) as crate::relay::AnyError)
}

fn claim_target(context: &WorkflowContext, namespace: &str, key: &str) -> Result<(), String> {
    claim(
        context,
        ResourceKey::Namespaced {
            namespace: namespace.to_owned(),
            key: key.to_owned(),
        },
    )
}

fn scan_tour_target_claims(intent: &ScanTourIntent) -> Vec<ResourceKey> {
    match intent
        .target_systems
        .as_ref()
        .filter(|targets| !targets.is_empty())
    {
        Some(targets) => targets
            .iter()
            .map(|system| system.trim().to_ascii_uppercase())
            .filter(|system| !system.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|system| ResourceKey::Namespaced {
                namespace: "survey-system".to_owned(),
                key: system,
            })
            .collect(),
        None => vec![ResourceKey::Namespaced {
            namespace: "survey-tour".to_owned(),
            key: intent.center.trim().to_ascii_uppercase(),
        }],
    }
}

fn reserve_scan_tour_scope(
    context: &WorkflowContext,
    intent: &ScanTourIntent,
    replicant: &str,
    vessel: &str,
) -> Result<Option<WorkflowId>, String> {
    if intent
        .target_systems
        .as_ref()
        .is_some_and(|targets| !targets.is_empty())
    {
        let legacy_center = ResourceKey::Namespaced {
            namespace: "survey-tour".to_owned(),
            key: intent.center.trim().to_ascii_uppercase(),
        };
        context
            .release_claim(&legacy_center)
            .map_err(string_error)?;
    }

    let mut resources = vec![
        ResourceKey::Replicant(replicant.to_owned()),
        ResourceKey::Device(vessel.to_owned()),
    ];
    resources.extend(scan_tour_target_claims(intent));

    let mut newly_acquired = Vec::new();
    for resource in resources {
        match context.acquire_claim(resource.clone()) {
            Ok(ClaimAcquireOutcome::Acquired(_)) => newly_acquired.push(resource),
            Ok(ClaimAcquireOutcome::AlreadyOwned(_)) => {}
            Err(RepositoryError::ClaimConflict { owner, .. }) => {
                for acquired in &newly_acquired {
                    context.release_claim(acquired).map_err(string_error)?;
                }
                return Ok(Some(owner));
            }
            Err(error) => return Err(string_error(error)),
        }
    }
    Ok(None)
}

async fn owned_device_snapshots(client: &Client) -> Result<Vec<Device>, String> {
    // Workflow planning normally consumes the managed projection maintained by
    // SSE, targeted reads, and explicit reconciliation. Do not turn each
    // checkpoint pass into an unfiltered account-wide device traversal.
    let handles = client
        .devices()
        .find()
        .owned()
        .collect()
        .await
        .map_err(string_error)?;
    device_snapshots(client, handles).await
}

async fn device_snapshots(
    client: &Client,
    handles: Vec<DeviceHandle>,
) -> Result<Vec<Device>, String> {
    let mut devices = Vec::with_capacity(handles.len());
    let mut missing = Vec::new();
    for handle in handles {
        match handle.snapshot().await {
            Ok(device) => devices.push(device),
            Err(_) => missing.push(handle),
        }
    }
    if !missing.is_empty() {
        client
            .devices()
            .refresh_many()
            .collect()
            .await
            .map_err(string_error)?;
        for handle in missing {
            let device = match handle.snapshot().await {
                Ok(device) => device,
                Err(_) => client
                    .devices()
                    .get(handle.id().as_str())
                    .await
                    .map_err(string_error)?
                    .snapshot()
                    .await
                    .map_err(string_error)?,
            };
            devices.push(device);
        }
    }
    devices.sort_by(|left, right| left.key.id.cmp(&right.key.id));
    Ok(devices)
}

#[derive(Default)]
struct DeviceCensus {
    devices: Option<Vec<Device>>,
}

impl DeviceCensus {
    async fn snapshots<'a>(&'a mut self, client: &Client) -> Result<&'a [Device], String> {
        if self.devices.is_none() {
            self.devices = Some(owned_device_snapshots(client).await?);
        }
        Ok(self.devices.as_deref().unwrap_or_default())
    }

    fn invalidate(&mut self) {
        self.devices = None;
    }
}

fn validate_trade_fulfillment_intent(intent: &TradeFulfillmentIntent) -> Result<(), String> {
    for (label, value) in [
        ("trade controller", intent.controller.as_str()),
        ("trade code", intent.trade_code.as_str()),
        ("shop location", intent.shop_location.as_str()),
        ("home hub", intent.home.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("trade fulfillment requires a {label}"));
        }
    }
    Ok(())
}

fn validate_trade_bundle(bundle: &TradeBundle, label: &str) -> Result<(), String> {
    if !bundle.unknown.is_empty() {
        return Err(format!(
            "trade {label} contain unsupported fields {:?}; refusing automatic execution",
            bundle.unknown.keys().collect::<Vec<_>>()
        ));
    }
    if bundle
        .resources
        .values()
        .chain(bundle.devices.values())
        .any(|quantity| *quantity < 0)
    {
        return Err(format!("trade {label} contain a negative quantity"));
    }
    Ok(())
}

async fn live_shop_trade(
    client: &Client,
    controller: &str,
    trade_code: &str,
) -> Result<crate::trade::ShopTrade, String> {
    shop_trades(client, controller)
        .await
        .map_err(string_error)?
        .into_iter()
        .find(|trade| trade.trade_code.eq_ignore_ascii_case(trade_code))
        .ok_or_else(|| format!("trade {trade_code} on controller {controller} is not available"))
}

async fn trade_stock_decreased(
    client: &Client,
    controller: &str,
    trade_code: &str,
    before: Option<i64>,
) -> Result<bool, String> {
    let Some(before) = before else {
        return Ok(false);
    };
    let current = shop_trades(client, controller)
        .await
        .map_err(string_error)?
        .into_iter()
        .find(|trade| trade.trade_code.eq_ignore_ascii_case(trade_code));
    Ok(match current {
        Some(trade) => trade.current_stock.is_some_and(|stock| stock < before),
        None => before > 0,
    })
}

fn validate_live_trade_for_fulfillment(
    intent: &TradeFulfillmentIntent,
    trade: &crate::trade::ShopTrade,
) -> Result<(), String> {
    if trade.current_stock.unwrap_or_default() <= 0 {
        return Err(format!(
            "trade {} on controller {} is out of stock",
            intent.trade_code, intent.controller
        ));
    }
    let criteria = trade.criteria_bundle();
    let rewards = trade.rewards_bundle();
    validate_trade_bundle(&criteria, "criteria")?;
    validate_trade_bundle(&rewards, "rewards")?;
    if let Some(expected) = intent
        .expected_reward_device
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        && rewards.devices.get(expected).copied().unwrap_or_default() <= 0
    {
        return Err(format!(
            "trade {} no longer rewards expected device {}",
            intent.trade_code, expected
        ));
    }
    Ok(())
}

async fn resolve_and_claim_trade_replicant(
    context: &WorkflowContext,
    client: &Client,
    pinned: Option<&str>,
    home: &str,
    home_system: &str,
) -> Result<String, String> {
    if let Some(code) = pinned.filter(|value| !value.trim().is_empty()) {
        client
            .replicants()
            .get_owned(code)
            .await
            .map_err(string_error)?;
        if try_claim_available(context, ResourceKey::Replicant(code.to_owned()))? {
            return Ok(code.to_owned());
        }
        return Err(format!(
            "selected trade Replicant {code} is claimed by another workflow"
        ));
    }

    let handles = client
        .replicants()
        .find()
        .owned()
        .collect()
        .await
        .map_err(string_error)?;
    let mut candidates = Vec::new();
    for handle in handles {
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if snapshot.travel.is_some() {
            continue;
        }
        let location = snapshot
            .location
            .as_ref()
            .map(|location| location.id.as_str())
            .unwrap_or_default();
        let rank = if location.eq_ignore_ascii_case(home) {
            0
        } else if designation_in_system(location, home_system) {
            1
        } else {
            2
        };
        candidates.push((rank, snapshot.key.id.as_str().to_owned()));
    }
    candidates.sort();
    for (_, code) in candidates {
        if try_claim_available(context, ResourceKey::Replicant(code.clone()))? {
            return Ok(code);
        }
    }
    Err("no unclaimed owned Replicant is available for trade fulfillment".to_owned())
}

async fn ensure_trade_replicant_at(
    context: &mut WorkflowContext,
    client: &Client,
    checkpoint: &TradeFulfillmentCheckpoint,
    replicant_code: &str,
    destination: &str,
    step: &str,
) -> Result<bool, String> {
    let handle = client
        .replicants()
        .get_owned(replicant_code)
        .await
        .map_err(string_error)?;
    let snapshot = handle.snapshot().await.map_err(string_error)?;
    if snapshot
        .location
        .as_ref()
        .is_some_and(|location| location.id.as_str().eq_ignore_ascii_case(destination))
    {
        return Ok(true);
    }
    if let Some(travel) = &snapshot.travel {
        let planned = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        if planned.is_some_and(|planned| planned.eq_ignore_ascii_case(destination)) {
            context.advance_to(step, checkpoint).map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
        return Err(format!(
            "trade Replicant {replicant_code} is already travelling to {:?}, not {destination}",
            planned
        ));
    }
    context.advance_to(step, checkpoint).map_err(string_error)?;
    let operation = handle
        .travel()
        .to(destination.to_owned())
        .depart()
        .await
        .map_err(string_error)?;
    await_success(&operation).await?;
    let refreshed = handle.refresh().await.map_err(string_error)?;
    let snapshot = refreshed.snapshot().await.map_err(string_error)?;
    if snapshot
        .location
        .as_ref()
        .is_some_and(|location| location.id.as_str().eq_ignore_ascii_case(destination))
    {
        Ok(true)
    } else {
        context.mark_waiting().map_err(string_error)?;
        Ok(false)
    }
}

async fn ensure_trade_payment_ready(
    context: &mut WorkflowContext,
    client: &Client,
    intent: &TradeFulfillmentIntent,
    checkpoint: &mut TradeFulfillmentCheckpoint,
    home: &str,
    home_system: &str,
    criteria: &TradeBundle,
) -> Result<bool, String> {
    if let Some(child_id) = checkpoint.payment_logistics_child {
        let Some(child) = context.repository().read(child_id).map_err(string_error)? else {
            return Err(format!(
                "trade payment logistics child {child_id} disappeared"
            ));
        };
        match child.status {
            WorkflowStatus::Succeeded => {
                let child_checkpoint: LogisticsWorkflowCheckpoint =
                    child.checkpoint().map_err(string_error)?;
                checkpoint.outbound_plan = child_checkpoint.plan;
                checkpoint.payment_logistics_child = None;
                context
                    .persist_checkpoint(checkpoint)
                    .map_err(string_error)?;
            }
            WorkflowStatus::Failed | WorkflowStatus::Cancelled => {
                let class = child
                    .checkpoint::<LogisticsWorkflowCheckpoint>()
                    .ok()
                    .and_then(|checkpoint| checkpoint.failure_class);
                let error = child.last_error.unwrap_or_default();
                if child.status == WorkflowStatus::Failed
                    && retryable_trade_criteria_logistics_failure(class, &error)
                {
                    checkpoint.payment_logistics_child = None;
                    checkpoint.outbound_plan = None;
                    context
                        .advance_to("replanning_trade_payment", checkpoint)
                        .map_err(string_error)?;
                    context
                        .emit_activity(format!(
                            "trade payment logistics hit stale source state ({error}); recomputing the remaining manifest"
                        ))
                        .map_err(string_error)?;
                    context.mark_waiting().map_err(string_error)?;
                    return Ok(false);
                }
                return Err(format!(
                    "trade payment logistics child {child_id} ended as {:?}: {error}",
                    child.status
                ));
            }
            _ => {
                context
                    .advance_to("staging_trade_payment", checkpoint)
                    .map_err(string_error)?;
                context.mark_waiting().map_err(string_error)?;
                return Ok(false);
            }
        }
    }

    let mut census = DeviceCensus::default();
    let mut devices = census.snapshots(client).await?;
    let unlocked = client
        .blueprints()
        .unlocked_device_types()
        .await
        .map_err(string_error)?;
    let mut print_requests = Vec::new();
    for (device_type, required) in &criteria.devices {
        let required = usize::try_from(*required)
            .map_err(|_| format!("invalid trade device criterion quantity {required}"))?;
        let available = devices
            .iter()
            .filter(|device| {
                trade_payment_device_is_releasable(device, device_type)
                    && device.location.as_ref().is_some_and(|location| {
                        location
                            .id
                            .as_str()
                            .eq_ignore_ascii_case(&intent.shop_location)
                            || designation_in_system(location.id.as_str(), home_system)
                    })
            })
            .count();
        if available >= required {
            continue;
        }
        let kind = DeviceType::from(device_type.as_str());
        if !unlocked.contains(&kind) {
            context
                .advance_to("waiting_for_trade_payment_blueprint", checkpoint)
                .map_err(string_error)?;
            context
                .emit_activity(format!(
                    "trade requires {required} {device_type}, but only {available} releasable copies are in the home/shop scope and its blueprint is not unlocked"
                ))
                .map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
        print_requests.push(PrintRequest::new(
            device_type.clone(),
            i64::try_from(required - available)
                .map_err(|_| "trade print quantity exceeded i64".to_owned())?,
        ));
    }

    if !print_requests.is_empty() {
        let tag = checkpoint
            .payment_print_tag
            .get_or_insert_with(|| format!("trade-pay:{}", &context.id().to_string()[..8]))
            .clone();
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
        context
            .advance_to("printing_trade_payment", checkpoint)
            .map_err(string_error)?;
        let mut options = QueueOptions::at(home.to_owned());
        options.tags = vec![tag.clone()];
        options.wait_timeout = Duration::from_secs(DEFAULT_WAIT_SECONDS);
        queue_prints_with_components(client, &print_requests, &options)
            .await
            .map_err(string_error)?;
        loop {
            let status = printing_status_in_system(
                client,
                home,
                &print_requests,
                std::slice::from_ref(&tag),
            )
            .await
            .map_err(string_error)?;
            if status
                .requested
                .iter()
                .all(|line| line.available >= line.required)
            {
                break;
            }
            match context.control_request().map_err(string_error)? {
                replicant_workflow::ControlRequest::Continue => {}
                replicant_workflow::ControlRequest::Pause
                | replicant_workflow::ControlRequest::Cancel => return Ok(false),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        client
            .devices()
            .refresh_many()
            .with_tag(tag)
            .at(home.to_owned())
            .collect()
            .await
            .map_err(string_error)?;
        census.invalidate();
        devices = census.snapshots(client).await?;
    }

    let inventories = fetch_account_inventories(client).await?;
    let resources_at_shop = inventory_at_location(&inventories, &intent.shop_location);
    let mut missing_resources = ResourceMap::new();
    for (resource, required) in &criteria.resources {
        let present = resources_at_shop.get(resource).copied().unwrap_or_default();
        if present < *required {
            missing_resources.insert(resource.clone(), required - present);
        }
    }
    let resources_at_home =
        inventory_in_system_excluding(&inventories, home_system, &intent.shop_location);
    for (resource, missing) in &missing_resources {
        let available = resources_at_home.get(resource).copied().unwrap_or_default();
        if available < *missing {
            context
                .advance_to("waiting_for_trade_payment_resources", checkpoint)
                .map_err(string_error)?;
            context
                .emit_activity(format!(
                    "trade needs {missing} more {resource} at {}, but home system {home_system} only has {available} available outside the shop",
                    intent.shop_location
                ))
                .map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
    }

    let mut payment_codes = Vec::new();
    for (device_type, required) in &criteria.devices {
        let required = usize::try_from(*required)
            .map_err(|_| format!("invalid trade device criterion quantity {required}"))?;
        let mut at_shop = devices
            .iter()
            .filter(|device| {
                trade_payment_device_is_releasable(device, device_type)
                    && device.location.as_ref().is_some_and(|location| {
                        location
                            .id
                            .as_str()
                            .eq_ignore_ascii_case(&intent.shop_location)
                    })
            })
            .map(|device| device.key.id.as_str().to_owned())
            .collect::<Vec<_>>();
        at_shop.sort();
        if at_shop.len() >= required {
            continue;
        }
        let missing = required - at_shop.len();
        let mut at_home = devices
            .iter()
            .filter(|device| {
                trade_payment_device_is_releasable(device, device_type)
                    && device.location.as_ref().is_some_and(|location| {
                        designation_in_system(location.id.as_str(), home_system)
                            && !location
                                .id
                                .as_str()
                                .eq_ignore_ascii_case(&intent.shop_location)
                    })
            })
            .map(|device| device.key.id.as_str().to_owned())
            .collect::<Vec<_>>();
        at_home.sort();
        if at_home.len() < missing {
            context
                .advance_to("waiting_for_trade_payment_devices", checkpoint)
                .map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
        payment_codes.extend(at_home.into_iter().take(missing));
    }
    payment_codes.sort();
    payment_codes.dedup();
    checkpoint.payment_device_codes = payment_codes.clone();
    context
        .persist_checkpoint(checkpoint)
        .map_err(string_error)?;

    if !missing_resources.is_empty() || !payment_codes.is_empty() {
        let child = context
            .create_child(new_logistics_manifest_workflow(LogisticsManifestIntent {
                origin: home_system.to_owned(),
                destination: intent.shop_location.clone(),
                resources: missing_resources,
                devices: Vec::new(),
                device_codes: payment_codes.clone(),
                device_tags: Vec::new(),
                pre_deactivate_device_codes: Vec::new(),
                release_mining_reservations: false,
                return_transports: false,
                placement_recovery: None,
                allow_transport_staging: true,
                region: intent.preferred_region.clone(),
                purpose: format!(
                    "trade-fulfillment:{}:{}:payment",
                    intent.controller, intent.trade_code
                ),
            }))
            .map_err(string_error)?;
        for code in &payment_codes {
            context
                .repository()
                .acquire_claim(child.id, ResourceKey::Device(code.clone()))
                .map_err(string_error)?;
        }
        checkpoint.payment_logistics_child = Some(child.id);
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
        context
            .advance_to("staging_trade_payment", checkpoint)
            .map_err(string_error)?;
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }

    // Reserve the exact shop-side criterion devices through the irreversible
    // trade so parallel workflows cannot consume them after preflight.
    for (device_type, required) in &criteria.devices {
        let required = usize::try_from(*required)
            .map_err(|_| format!("invalid trade device criterion quantity {required}"))?;
        let mut at_shop = devices
            .iter()
            .filter(|device| {
                trade_payment_device_is_releasable(device, device_type)
                    && device.location.as_ref().is_some_and(|location| {
                        location
                            .id
                            .as_str()
                            .eq_ignore_ascii_case(&intent.shop_location)
                    })
            })
            .map(|device| device.key.id.as_str().to_owned())
            .collect::<Vec<_>>();
        at_shop.sort();
        if at_shop.len() < required {
            context
                .advance_to("waiting_for_trade_payment", checkpoint)
                .map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
        for code in at_shop.into_iter().take(required) {
            if !try_claim_available(context, ResourceKey::Device(code.clone()))? {
                context
                    .advance_to("waiting_for_trade_payment_claim", checkpoint)
                    .map_err(string_error)?;
                context.mark_waiting().map_err(string_error)?;
                return Ok(false);
            }
        }
    }
    let resources_now = fetch_inventory_at_location(client, &intent.shop_location).await?;
    if criteria.resources.iter().any(|(resource, required)| {
        resources_now.get(resource).copied().unwrap_or_default() < *required
    }) {
        context
            .advance_to("waiting_for_trade_payment", checkpoint)
            .map_err(string_error)?;
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }
    Ok(true)
}

fn inventory_in_system_excluding(
    inventories: &[replicant_client::domain::Inventory],
    system: &str,
    excluded_location: &str,
) -> ResourceMap {
    let mut resources = ResourceMap::new();
    for inventory in inventories.iter().filter(|inventory| {
        inventory.location.as_ref().is_some_and(|key| {
            designation_in_system(key.id.as_str(), system)
                && !key.id.as_str().eq_ignore_ascii_case(excluded_location)
        })
    }) {
        for item in &inventory.items {
            *resources.entry(item.resource.clone()).or_default() += item.quantity;
        }
    }
    resources
}

fn trade_bundle_device_count(bundle: &TradeBundle) -> Result<usize, String> {
    bundle.devices.values().try_fold(0usize, |total, quantity| {
        let quantity = usize::try_from(*quantity)
            .map_err(|_| format!("invalid trade reward device quantity {quantity}"))?;
        total
            .checked_add(quantity)
            .ok_or_else(|| "trade reward device quantity overflowed usize".to_owned())
    })
}

fn trade_bundle_resource_count(bundle: &TradeBundle) -> i64 {
    bundle
        .resources
        .values()
        .copied()
        .fold(0i64, i64::saturating_add)
        .max(0)
}

async fn trade_replicant_stow_free(client: &Client, replicant: &str) -> Result<i64, String> {
    let snapshot = client
        .replicants()
        .get_owned(replicant)
        .await
        .map_err(string_error)?
        .snapshot()
        .await
        .map_err(string_error)?;
    let Some(vessel) = snapshot.hosted_device.as_ref() else {
        return Ok(0);
    };
    let vessel = client
        .devices()
        .refresh(vessel.id.as_str())
        .await
        .map_err(string_error)?
        .snapshot()
        .await
        .map_err(string_error)?;
    Ok(vessel.free_stow_capacity())
}

fn trade_transport_codes(checkpoint: &TradeFulfillmentCheckpoint) -> Vec<String> {
    let mut codes = checkpoint
        .outbound_plan
        .as_ref()
        .into_iter()
        .flat_map(|plan| {
            plan.cargo_transports
                .iter()
                .chain(plan.device_carriers.iter())
                .cloned()
        })
        .chain(checkpoint.escort_carriers.iter().cloned())
        .collect::<Vec<_>>();
    codes.sort();
    codes.dedup();
    codes
}

async fn free_trade_transport_capacity(client: &Client, code: &str) -> Result<(i64, i64), String> {
    let detail = client
        .raw()
        .devices()
        .get(code)
        .await
        .map_err(string_error)?
        .value;
    let cargo_used = detail
        .cargo
        .iter()
        .filter_map(|item| item.quantity)
        .fold(0i64, i64::saturating_add)
        .max(0);
    let attached = i64::try_from(detail.attached_devices.len())
        .map_err(|_| "attachment count exceeded i64".to_owned())?;
    Ok((
        detail
            .cargo_capacity
            .unwrap_or_default()
            .saturating_sub(cargo_used)
            .max(0),
        detail
            .attach_capacity
            .unwrap_or_default()
            .saturating_sub(attached)
            .max(0),
    ))
}

async fn trade_transport_is_empty(client: &Client, code: &str) -> Result<bool, String> {
    let detail = client
        .raw()
        .devices()
        .get(code)
        .await
        .map_err(string_error)?
        .value;
    Ok(raw_cargo_quantity(&detail) == 0
        && detail.attached_devices.is_empty()
        && detail.stowed_devices.is_empty()
        && detail.controller_device_code.is_none()
        && detail.hosting_replicant.is_none())
}

async fn ensure_trade_reward_capacity(
    context: &mut WorkflowContext,
    client: &Client,
    checkpoint: &mut TradeFulfillmentCheckpoint,
    home_system: &str,
    shop_location: &str,
    rewards: &TradeBundle,
    replicant: &str,
) -> Result<bool, String> {
    let stow_free = trade_replicant_stow_free(client, replicant).await?;
    let device_rewards = i64::try_from(trade_bundle_device_count(rewards)?)
        .map_err(|_| "trade reward device count exceeded i64".to_owned())?;
    let attach_needed = device_rewards.saturating_sub(stow_free).max(0);
    let cargo_needed = trade_bundle_resource_count(rewards);

    let mut transport_codes = trade_transport_codes(checkpoint);
    for code in &transport_codes {
        if !try_claim_available(context, ResourceKey::Device(code.clone()))? {
            context
                .advance_to("waiting_for_trade_return_transport", checkpoint)
                .map_err(string_error)?;
            context
                .emit_activity(format!(
                    "trade transport {code} was claimed after payment staging; waiting to reserve the return fleet"
                ))
                .map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
    }

    let mut cargo_capacity = 0i64;
    let mut attach_capacity = 0i64;
    for code in &transport_codes {
        let (cargo, attach) = free_trade_transport_capacity(client, code).await?;
        cargo_capacity = cargo_capacity.saturating_add(cargo);
        attach_capacity = attach_capacity.saturating_add(attach);
    }

    if cargo_capacity >= cargo_needed && attach_capacity >= attach_needed {
        return Ok(true);
    }

    let devices = owned_device_snapshots(client).await?;
    let excluded = transport_codes
        .iter()
        .map(|code| code.to_ascii_uppercase())
        .collect::<BTreeSet<_>>();
    let mut candidates = devices
        .iter()
        .filter(|device| {
            let code = device.key.id.as_str();
            !excluded.contains(&code.to_ascii_uppercase())
                && device.access == AccessScope::Owned
                && !workflow_reserved(&device.tags)
                && device.travel.is_none()
                && device.location.as_ref().is_some_and(|location| {
                    designation_in_system(location.id.as_str(), home_system)
                })
                && device.relationships.attached_to.is_none()
                && device.relationships.stowed_in.is_none()
                && device.relationships.controller.is_none()
                && device.relationships.hosting_replicant.is_none()
                && device.relationships.attached_devices.is_empty()
                && device.relationships.stowed_devices.is_empty()
                && device.relationships.controlled_devices.is_empty()
                && device
                    .available_commands
                    .iter()
                    .any(|command| command.as_str().eq_ignore_ascii_case("travel"))
                && (device.attach_capacity.unwrap_or_default() > 0
                    || device
                        .available_commands
                        .iter()
                        .any(|command| command.as_str().eq_ignore_ascii_case("collect_resources")))
        })
        .map(|device| device.key.id.as_str().to_owned())
        .collect::<Vec<_>>();
    candidates.sort();

    for code in candidates {
        if cargo_capacity >= cargo_needed && attach_capacity >= attach_needed {
            break;
        }
        if !trade_transport_is_empty(client, &code).await? {
            continue;
        }
        let (cargo, attach) = free_trade_transport_capacity(client, &code).await?;
        if cargo == 0 && attach == 0 {
            continue;
        }
        if !try_claim_available(context, ResourceKey::Device(code.clone()))? {
            continue;
        }
        checkpoint.escort_carriers.push(code.clone());
        checkpoint.escort_carriers.sort();
        checkpoint.escort_carriers.dedup();
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
        if !ensure_trade_device_at(
            context,
            client,
            checkpoint,
            &code,
            shop_location,
            "staging_trade_reward_carriers",
        )
        .await?
        {
            return Ok(false);
        }
        cargo_capacity = cargo_capacity.saturating_add(cargo);
        attach_capacity = attach_capacity.saturating_add(attach);
        transport_codes.push(code);
    }

    if cargo_capacity < cargo_needed || attach_capacity < attach_needed {
        context
            .advance_to("waiting_for_trade_reward_capacity", checkpoint)
            .map_err(string_error)?;
        context
            .emit_activity(format!(
                "trade rewards need cargo {cargo_needed} and attachment {attach_needed} return capacity after Replicant stow space; reserved capacity is cargo {cargo_capacity}, attachment {attach_capacity}"
            ))
            .map_err(string_error)?;
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }
    Ok(true)
}

async fn ensure_trade_device_at(
    context: &mut WorkflowContext,
    client: &Client,
    checkpoint: &TradeFulfillmentCheckpoint,
    code: &str,
    destination: &str,
    step: &str,
) -> Result<bool, String> {
    let handle = client.devices().get(code).await.map_err(string_error)?;
    let snapshot = handle.snapshot().await.map_err(string_error)?;
    if snapshot
        .location
        .as_ref()
        .is_some_and(|location| location.id.as_str().eq_ignore_ascii_case(destination))
    {
        return Ok(true);
    }
    if let Some(travel) = &snapshot.travel {
        let planned = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        if planned.is_some_and(|planned| planned.eq_ignore_ascii_case(destination)) {
            context.advance_to(step, checkpoint).map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
        return Err(format!(
            "trade transport {code} is already travelling to {:?}, not {destination}",
            planned
        ));
    }
    context.advance_to(step, checkpoint).map_err(string_error)?;
    let via = client
        .smart_travel()
        .route_for_device(code, destination)
        .await
        .map_err(string_error)?
        .filter(|plan| !plan.is_direct && !plan.intermediate_systems.is_empty())
        .map(|plan| {
            Value::Array(
                plan.explicit_waypoints_for(destination)
                    .into_iter()
                    .map(Value::String)
                    .collect(),
            )
        });
    let operation = handle
        .command(replicant_client::raw::devices::DeviceCommand::Travel {
            destination: destination.to_owned(),
            dry_run: None,
            via,
        })
        .await
        .map_err(string_error)?;
    await_success(&operation).await?;
    let snapshot = handle
        .refresh()
        .await
        .map_err(string_error)?
        .snapshot()
        .await
        .map_err(string_error)?;
    if snapshot
        .location
        .as_ref()
        .is_some_and(|location| location.id.as_str().eq_ignore_ascii_case(destination))
    {
        Ok(true)
    } else {
        context.mark_waiting().map_err(string_error)?;
        Ok(false)
    }
}

async fn snapshot_reward_device_codes(
    client: &Client,
    rewards: &TradeBundle,
) -> Result<BTreeMap<String, Vec<String>>, String> {
    let devices = owned_device_snapshots(client).await?;
    let mut result = BTreeMap::new();
    for device_type in rewards.devices.keys() {
        let mut codes = devices
            .iter()
            .filter(|device| {
                device
                    .device_type
                    .as_ref()
                    .is_some_and(|kind| kind.as_str().eq_ignore_ascii_case(device_type))
            })
            .map(|device| device.key.id.as_str().to_owned())
            .collect::<Vec<_>>();
        codes.sort();
        result.insert(device_type.clone(), codes);
    }
    Ok(result)
}

async fn observe_trade_reward_devices(
    client: &Client,
    rewards: &TradeBundle,
    before: &BTreeMap<String, Vec<String>>,
    shop_location: &str,
    attempts: usize,
) -> Result<Vec<String>, String> {
    for attempt in 0..attempts.max(1) {
        let mut selected = Vec::new();
        let mut complete = true;
        for (device_type, quantity) in &rewards.devices {
            let required = usize::try_from(*quantity)
                .map_err(|_| format!("invalid trade reward quantity {quantity}"))?;
            let before = before
                .get(device_type)
                .into_iter()
                .flatten()
                .map(|code| code.to_ascii_uppercase())
                .collect::<BTreeSet<_>>();
            let handles = client
                .devices()
                .refresh_many()
                .of_type(DeviceType::from(device_type.as_str()))
                .at(shop_location.to_owned())
                .collect()
                .await
                .map_err(string_error)?;
            let mut candidates = Vec::new();
            for handle in handles {
                let device = handle.snapshot().await.map_err(string_error)?;
                if device.access != AccessScope::Owned
                    || before.contains(&device.key.id.as_str().to_ascii_uppercase())
                    || device.location.as_ref().is_none_or(|location| {
                        !location.id.as_str().eq_ignore_ascii_case(shop_location)
                    })
                {
                    continue;
                }
                candidates.push(device.key.id.as_str().to_owned());
            }
            candidates.sort();
            if candidates.len() < required {
                complete = false;
                break;
            }
            selected.extend(candidates.into_iter().take(required));
        }
        if complete {
            selected.sort();
            selected.dedup();
            return Ok(selected);
        }
        if attempt + 1 < attempts.max(1) {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    Ok(Vec::new())
}

async fn ensure_trade_reward_attachable(client: &Client, code: &str) -> Result<(), String> {
    let handle = client.devices().get(code).await.map_err(string_error)?;
    let mut detail = client
        .raw()
        .devices()
        .get(code)
        .await
        .map_err(string_error)?
        .value;

    let status_is = |detail: &replicant_client::raw::devices::DeviceStatus, expected: &str| {
        detail
            .status
            .as_deref()
            .is_some_and(|status| status.eq_ignore_ascii_case(expected))
    };
    let command_available = |detail: &replicant_client::raw::devices::DeviceStatus,
                             expected: &str| {
        detail
            .available_commands
            .iter()
            .chain(detail.commands.iter())
            .any(|command| command.eq_ignore_ascii_case(expected))
    };

    if status_is(&detail, "active") {
        if !command_available(&detail, "deactivate") {
            return Err(format!(
                "trade reward {code} is active and cannot currently be deactivated for transport"
            ));
        }
        let operation = handle.deactivate().await.map_err(string_error)?;
        await_success(&operation).await?;
        detail = client
            .raw()
            .devices()
            .get(code)
            .await
            .map_err(string_error)?
            .value;
    }

    let modular = command_available(&detail, "compact")
        || command_available(&detail, "unfurl")
        || detail.status.as_deref().is_some_and(|status| {
            matches!(
                status.to_ascii_lowercase().as_str(),
                "compacting" | "compacted" | "unfurling"
            )
        });
    if !modular || status_is(&detail, "compacted") {
        return Ok(());
    }

    for attempt in 0..24 {
        if status_is(&detail, "compacted") {
            return Ok(());
        }
        if status_is(&detail, "compacting") || status_is(&detail, "unfurling") {
            if attempt + 1 < 24 {
                tokio::time::sleep(Duration::from_secs(5)).await;
                detail = client
                    .raw()
                    .devices()
                    .get(code)
                    .await
                    .map_err(string_error)?
                    .value;
                continue;
            }
            break;
        }
        if detail.printing.is_some() || !detail.print_queue.is_empty() {
            return Err(format!(
                "trade reward {code} must finish its Autofactory work before it can be compacted"
            ));
        }
        if !command_available(&detail, "compact") {
            return Err(format!(
                "trade reward {code} is {:?} and cannot currently be compacted for transport",
                detail.status
            ));
        }
        let operation = handle.compact().await.map_err(string_error)?;
        await_success(&operation).await?;
        detail = client
            .raw()
            .devices()
            .get(code)
            .await
            .map_err(string_error)?
            .value;
    }

    if status_is(&detail, "compacted") {
        Ok(())
    } else {
        Err(format!(
            "trade reward {code} did not reach compacted state before transport timeout"
        ))
    }
}

async fn secure_trade_device_rewards(
    context: &mut WorkflowContext,
    client: &Client,
    checkpoint: &mut TradeFulfillmentCheckpoint,
    replicant: &str,
    shop_location: &str,
) -> Result<bool, String> {
    if checkpoint.reward_devices.is_empty() {
        return Ok(true);
    }
    let replicant_snapshot = client
        .replicants()
        .get_owned(replicant)
        .await
        .map_err(string_error)?
        .snapshot()
        .await
        .map_err(string_error)?;
    let vessel_code = replicant_snapshot
        .hosted_device
        .as_ref()
        .map(|device| device.id.as_str().to_owned());
    let mut stow_free = trade_replicant_stow_free(client, replicant).await?;
    let transports = trade_transport_codes(checkpoint);

    for code in checkpoint.reward_devices.clone() {
        claim_device(context, &code)?;
        if checkpoint.reward_storage.contains_key(&code) {
            continue;
        }
        let handle = client.devices().get(&code).await.map_err(string_error)?;
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if let Some(container) = snapshot.relationships.stowed_in.as_ref()
            && vessel_code
                .as_deref()
                .is_some_and(|vessel| container.id.as_str().eq_ignore_ascii_case(vessel))
        {
            checkpoint
                .reward_storage
                .insert(code.clone(), "stowed".to_owned());
            continue;
        }
        if let Some(carrier) = snapshot.relationships.attached_to.as_ref()
            && transports
                .iter()
                .any(|candidate| candidate.eq_ignore_ascii_case(carrier.id.as_str()))
        {
            checkpoint
                .reward_storage
                .insert(code.clone(), carrier.id.as_str().to_owned());
            continue;
        }
        if snapshot
            .location
            .as_ref()
            .is_none_or(|location| !location.id.as_str().eq_ignore_ascii_case(shop_location))
        {
            return Err(format!(
                "reward device {code} is no longer free at shop location {shop_location}"
            ));
        }

        ensure_trade_reward_attachable(client, &code).await?;
        let snapshot = handle
            .refresh()
            .await
            .map_err(string_error)?
            .snapshot()
            .await
            .map_err(string_error)?;
        if stow_free > 0
            && snapshot
                .available_commands
                .iter()
                .any(|command| command.as_str().eq_ignore_ascii_case("stow"))
        {
            context
                .advance_to("stowing_trade_reward", checkpoint)
                .map_err(string_error)?;
            let operation = handle.stow(None).await.map_err(string_error)?;
            await_success(&operation).await?;
            checkpoint
                .reward_storage
                .insert(code.clone(), "stowed".to_owned());
            stow_free = stow_free.saturating_sub(1);
            context
                .persist_checkpoint(checkpoint)
                .map_err(string_error)?;
            continue;
        }

        let mut carrier_candidates = Vec::new();
        for carrier in &transports {
            let carrier_snapshot = client
                .devices()
                .get(carrier)
                .await
                .map_err(string_error)?
                .snapshot()
                .await
                .map_err(string_error)?;
            if carrier_snapshot
                .location
                .as_ref()
                .is_none_or(|location| !location.id.as_str().eq_ignore_ascii_case(shop_location))
            {
                continue;
            }
            let (_, attach) = free_trade_transport_capacity(client, carrier).await?;
            if attach > 0 {
                carrier_candidates.push((attach, carrier.clone()));
            }
        }
        carrier_candidates
            .sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
        let Some((_, carrier)) = carrier_candidates.into_iter().next() else {
            context
                .advance_to("waiting_for_trade_reward_carrier", checkpoint)
                .map_err(string_error)?;
            context
                .emit_activity(format!(
                    "reward device {code} cannot be stowed and no reserved attachment carrier currently has free capacity at {shop_location}"
                ))
                .map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        };
        context
            .advance_to("attaching_trade_reward", checkpoint)
            .map_err(string_error)?;
        let operation = client
            .devices()
            .get(&carrier)
            .await
            .map_err(string_error)?
            .attach(replicant_client::raw::devices::TargetsCommand {
                devices: Some(serde_json::json!([code.clone()])),
                ..replicant_client::raw::devices::TargetsCommand::default()
            })
            .await
            .map_err(string_error)?;
        await_success(&operation).await?;
        checkpoint.reward_storage.insert(code, carrier);
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
    }
    context
        .persist_checkpoint(checkpoint)
        .map_err(string_error)?;
    Ok(true)
}

fn resource_command_object(resources: &ResourceMap) -> Result<BTreeMap<String, f64>, String> {
    Ok(resources
        .iter()
        .map(|(resource, quantity)| (resource.clone(), *quantity as f64))
        .collect())
}

fn raw_cargo_quantity(detail: &replicant_client::raw::devices::DeviceStatus) -> i64 {
    detail
        .cargo
        .iter()
        .filter_map(|item| item.quantity)
        .fold(0i64, i64::saturating_add)
        .max(0)
}

async fn wait_for_trade_reward_resources(
    client: &Client,
    shop_location: &str,
    rewards: &ResourceMap,
    attempts: usize,
) -> Result<bool, String> {
    let attempts = attempts.max(1);
    let deadline = tokio::time::Instant::now()
        + Duration::from_secs(5).saturating_mul(u32::try_from(attempts).unwrap_or(u32::MAX));
    let mut watch = client.events().watch().await.map_err(string_error)?;
    for attempt in 0..attempts {
        let cached = inventory_at_location(
            &client.state().inventories().map_err(string_error)?,
            shop_location,
        );
        if trade_rewards_satisfied(&cached, rewards) {
            return Ok(true);
        }

        let inventory = fetch_inventory_at_location(client, shop_location).await?;
        if trade_rewards_satisfied(&inventory, rewards) {
            return Ok(true);
        }
        if attempt + 1 == attempts {
            break;
        }

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining.min(Duration::from_secs(30)), watch.next()).await {
                Ok(Ok(event)) if event.name.as_str() == "trade.completed" => break,
                Ok(Ok(_)) => {}
                Ok(Err(_)) | Err(_) => break,
            }
        }
    }
    Ok(false)
}

fn trade_rewards_satisfied(inventory: &ResourceMap, rewards: &ResourceMap) -> bool {
    rewards.iter().all(|(resource, quantity)| {
        inventory.get(resource).copied().unwrap_or_default() >= *quantity
    })
}

async fn load_trade_reward_resources(
    context: &mut WorkflowContext,
    client: &Client,
    checkpoint: &mut TradeFulfillmentCheckpoint,
    shop_location: &str,
    rewards: &ResourceMap,
) -> Result<(), String> {
    let mut remaining = rewards.clone();
    remaining.retain(|_, quantity| *quantity > 0);
    if remaining.is_empty() {
        return Ok(());
    }
    if !wait_for_trade_reward_resources(client, shop_location, &remaining, 24).await? {
        return Err(format!(
            "trade completed but reward resources {:?} were not observed at {shop_location}",
            remaining
        ));
    }
    let mut carriers = Vec::new();
    for code in trade_transport_codes(checkpoint) {
        let detail = client
            .raw()
            .devices()
            .get(&code)
            .await
            .map_err(string_error)?
            .value;
        if detail
            .location
            .as_deref()
            .is_some_and(|location| location.eq_ignore_ascii_case(shop_location))
        {
            let free = detail
                .cargo_capacity
                .unwrap_or_default()
                .saturating_sub(raw_cargo_quantity(&detail))
                .max(0);
            if free > 0 {
                carriers.push((free, code));
            }
        }
    }
    carriers.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));

    for (capacity, code) in carriers {
        if remaining.is_empty() {
            break;
        }
        let mut capacity = capacity;
        let mut manifest = ResourceMap::new();
        for (resource, quantity) in remaining.clone() {
            if capacity <= 0 {
                break;
            }
            let take = quantity.min(capacity);
            if take > 0 {
                manifest.insert(resource.clone(), take);
                capacity -= take;
                let left = quantity - take;
                if left == 0 {
                    remaining.remove(&resource);
                } else {
                    remaining.insert(resource, left);
                }
            }
        }
        if manifest.is_empty() {
            continue;
        }
        context
            .advance_to("loading_trade_reward_resources", checkpoint)
            .map_err(string_error)?;
        let operation = client
            .devices()
            .get(&code)
            .await
            .map_err(string_error)?
            .command(
                replicant_client::raw::devices::DeviceCommand::CollectResources {
                    resources: resource_command_object(&manifest)?,
                },
            )
            .await
            .map_err(string_error)?;
        await_success(&operation).await?;
    }
    if !remaining.is_empty() {
        return Err(format!(
            "reserved trade return transports could not load reward resources {:?}",
            remaining
        ));
    }
    Ok(())
}

async fn return_trade_assets_home(
    context: &mut WorkflowContext,
    client: &Client,
    checkpoint: &mut TradeFulfillmentCheckpoint,
    replicant: &str,
    home: &str,
    reward_resources: &ResourceMap,
) -> Result<bool, String> {
    context
        .advance_to("returning_trade_assets", checkpoint)
        .map_err(string_error)?;
    for code in trade_transport_codes(checkpoint) {
        if !ensure_trade_device_at(
            context,
            client,
            checkpoint,
            &code,
            home,
            "returning_trade_transports",
        )
        .await?
        {
            return Ok(false);
        }
        let detail = client
            .raw()
            .devices()
            .get(&code)
            .await
            .map_err(string_error)?
            .value;
        if !reward_resources.is_empty() && raw_cargo_quantity(&detail) > 0 {
            let operation = client
                .devices()
                .get(&code)
                .await
                .map_err(string_error)?
                .command(
                    replicant_client::raw::devices::DeviceCommand::DepositResources {
                        resources: None,
                    },
                )
                .await
                .map_err(string_error)?;
            await_success(&operation).await?;
        }
        let attached_rewards = checkpoint
            .reward_storage
            .iter()
            .filter_map(|(reward, storage)| {
                storage
                    .eq_ignore_ascii_case(&code)
                    .then_some(reward.clone())
            })
            .collect::<Vec<_>>();
        if !attached_rewards.is_empty() {
            let operation = client
                .devices()
                .get(&code)
                .await
                .map_err(string_error)?
                .command(replicant_client::raw::devices::DeviceCommand::Detach(
                    replicant_client::raw::devices::TargetsCommand {
                        devices: Some(serde_json::json!(attached_rewards)),
                        ..replicant_client::raw::devices::TargetsCommand::default()
                    },
                ))
                .await
                .map_err(string_error)?;
            await_success(&operation).await?;
        }
    }

    if !ensure_trade_replicant_at(
        context,
        client,
        checkpoint,
        replicant,
        home,
        "returning_trade_replicant",
    )
    .await?
    {
        return Ok(false);
    }

    for code in checkpoint.reward_devices.clone() {
        if checkpoint
            .reward_storage
            .get(&code)
            .is_none_or(|storage| storage != "stowed")
        {
            continue;
        }
        let handle = client.devices().get(&code).await.map_err(string_error)?;
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if snapshot.relationships.stowed_in.is_some() {
            let operation = handle.deploy().await.map_err(string_error)?;
            await_success(&operation).await?;
        }
    }
    Ok(true)
}

fn trade_fulfillment_report(
    intent: &TradeFulfillmentIntent,
    checkpoint: &TradeFulfillmentCheckpoint,
) -> Value {
    serde_json::json!({
        "controller": intent.controller,
        "trade_code": intent.trade_code,
        "shop_location": intent.shop_location,
        "home": checkpoint.home,
        "replicant": checkpoint.replicant,
        "reward_devices": checkpoint.reward_devices,
        "transports": trade_transport_codes(checkpoint),
        "returned_home": checkpoint.returned_home,
    })
}

async fn blueprint_is_known(client: &Client, device_type: &str) -> Result<bool, String> {
    Ok(client
        .blueprints()
        .unlocked_device_types()
        .await
        .map_err(string_error)?
        .into_iter()
        .any(|known| known.as_str().eq_ignore_ascii_case(device_type)))
}

async fn wait_for_blueprint(
    client: &Client,
    device_type: &str,
    attempts: usize,
) -> Result<bool, String> {
    for attempt in 0..attempts.max(1) {
        if blueprint_is_known(client, device_type).await? {
            return Ok(true);
        }
        if attempt + 1 < attempts.max(1) {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    Ok(false)
}

fn blueprint_acquisition_strategy(intent: &BlueprintAcquireIntent) -> &'static str {
    if intent.shop.is_some() {
        "shop"
    } else {
        "owned"
    }
}

fn legacy_shop_purchase_in_progress(checkpoint: &BlueprintAcquireCheckpoint) -> bool {
    checkpoint.criteria_logistics_child.is_some()
        || checkpoint.purchase_authorized
        || checkpoint.purchase_submitted
        || checkpoint.purchase_operation.is_some()
        || checkpoint.criteria_print_tag.is_some()
        || !checkpoint.pre_purchase_devices.is_empty()
}

async fn prepare_shop_blueprint_source_via_trade(
    context: &mut WorkflowContext,
    client: &Client,
    intent: &BlueprintAcquireIntent,
    checkpoint: &mut BlueprintAcquireCheckpoint,
) -> Result<bool, String> {
    let shop = intent
        .shop
        .as_ref()
        .ok_or_else(|| "shop blueprint acquisition is missing its shop intent".to_owned())?;
    let factory_code = intent
        .autofactory
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "shop blueprint acquisition requires an Autofactory".to_owned())?;

    let mut census = DeviceCensus::default();
    let devices = census.snapshots(client).await?;
    let factory = devices
        .iter()
        .find(|device| {
            device.key.id.as_str().eq_ignore_ascii_case(factory_code)
                && device.access == AccessScope::Owned
                && device.device_type.as_ref() == Some(&DeviceType::Autofactory)
                && device.location.is_some()
        })
        .ok_or_else(|| format!("selected Autofactory {factory_code} is not available"))?;
    let factory_location = factory
        .location
        .as_ref()
        .map(|location| location.id.as_str().to_owned())
        .ok_or_else(|| format!("Autofactory {factory_code} has no location"))?;
    claim(context, ResourceKey::Autofactory(factory_code.to_owned()))?;
    checkpoint.autofactory = Some(factory_code.to_owned());
    checkpoint.autofactory_location = Some(factory_location.clone());
    context
        .persist_checkpoint(checkpoint)
        .map_err(string_error)?;

    let child_id = match checkpoint.trade_child {
        Some(id) => id,
        None => {
            let existing =
                context
                    .child_workflows()
                    .map_err(string_error)?
                    .into_iter()
                    .filter(|workflow| workflow.kind == trade_fulfillment_workflow_kind())
                    .find_map(|workflow| {
                        let config = workflow.config::<TradeFulfillmentIntent>().ok()?;
                        (config
                            .controller
                            .eq_ignore_ascii_case(&shop.controller_code)
                            && config.trade_code.eq_ignore_ascii_case(&shop.trade_code)
                            && config.home.eq_ignore_ascii_case(&factory_location)
                            && config.expected_reward_device.as_deref().is_some_and(
                                |device_type| device_type.eq_ignore_ascii_case(&intent.device_type),
                            ))
                        .then_some(workflow.id)
                    });
            let id = match existing {
                Some(id) => id,
                None => {
                    context
                        .create_child(new_trade_fulfillment_workflow(TradeFulfillmentIntent {
                            controller: shop.controller_code.clone(),
                            trade_code: shop.trade_code.clone(),
                            shop_location: shop.shop_location.clone(),
                            home: factory_location.clone(),
                            replicant: intent.acquisition_replicant.clone(),
                            preferred_region: intent.preferred_region.clone(),
                            expected_reward_device: Some(intent.device_type.clone()),
                        }))
                        .map_err(string_error)?
                        .id
                }
            };
            checkpoint.trade_child = Some(id);
            context
                .persist_checkpoint(checkpoint)
                .map_err(string_error)?;
            id
        }
    };

    let Some(child) = context.repository().read(child_id).map_err(string_error)? else {
        return Err(format!("blueprint trade child {child_id} disappeared"));
    };
    match child.status {
        WorkflowStatus::Succeeded => {
            let trade_checkpoint: TradeFulfillmentCheckpoint =
                child.checkpoint().map_err(string_error)?;
            let refreshed = census.snapshots(client).await?;
            let mut candidates = trade_checkpoint
                .reward_devices
                .iter()
                .filter_map(|code| {
                    refreshed.iter().find(|device| {
                        device.key.id.as_str().eq_ignore_ascii_case(code)
                            && blueprint_source_is_candidate(device, &intent.device_type, refreshed)
                            && blueprint_source_location(device, refreshed).is_some_and(
                                |location| location.eq_ignore_ascii_case(&factory_location),
                            )
                    })
                })
                .map(|device| device.key.id.as_str().to_owned())
                .collect::<Vec<_>>();
            candidates.sort();
            let Some(source) = candidates.into_iter().next() else {
                return Err(format!(
                    "trade fulfillment {child_id} succeeded but no returned {} reward is available at Autofactory {} ({factory_location})",
                    intent.device_type, factory_code
                ));
            };
            checkpoint.source_device = Some(source);
            checkpoint.acquisition_replicant = trade_checkpoint.replicant;
            checkpoint.purchase_observed = true;
            context
                .persist_checkpoint(checkpoint)
                .map_err(string_error)?;
            Ok(true)
        }
        WorkflowStatus::Failed | WorkflowStatus::Cancelled => Err(format!(
            "blueprint trade child {child_id} ended as {:?}: {}",
            child.status,
            child.last_error.unwrap_or_default()
        )),
        _ => {
            context
                .advance_to("awaiting_trade_fulfillment", checkpoint)
                .map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            Ok(false)
        }
    }
}

async fn prepare_shop_blueprint_source(
    context: &mut WorkflowContext,
    client: &Client,
    intent: &BlueprintAcquireIntent,
    checkpoint: &mut BlueprintAcquireCheckpoint,
) -> Result<bool, String> {
    let shop = intent
        .shop
        .as_ref()
        .ok_or_else(|| "shop blueprint acquisition is missing its shop intent".to_owned())?;
    let replicant = intent
        .acquisition_replicant
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "shop blueprint acquisition requires an acquisition Replicant".to_owned())?;
    let factory_code = intent
        .autofactory
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "shop blueprint acquisition requires an Autofactory".to_owned())?;

    checkpoint.acquisition_replicant = Some(replicant.to_owned());
    checkpoint.controller_code = Some(shop.controller_code.clone());
    checkpoint.trade_code = Some(shop.trade_code.clone());
    checkpoint.shop_location = Some(shop.shop_location.clone());
    checkpoint.criteria = Some(shop.criteria.clone());
    context
        .persist_checkpoint(checkpoint)
        .map_err(string_error)?;

    claim(context, ResourceKey::Replicant(replicant.to_owned()))?;
    claim(context, ResourceKey::Autofactory(factory_code.to_owned()))?;
    claim(
        context,
        ResourceKey::Namespaced {
            namespace: "blueprint-shop-trade".to_owned(),
            key: format!("{}:{}", shop.controller_code, shop.trade_code),
        },
    )?;
    claim(
        context,
        ResourceKey::Namespaced {
            namespace: "trade-fulfillment".to_owned(),
            key: format!(
                "{}:{}",
                shop.controller_code.to_ascii_uppercase(),
                shop.trade_code.to_ascii_uppercase()
            ),
        },
    )?;

    let devices = owned_device_snapshots(client).await?;
    let factory = devices
        .iter()
        .find(|device| {
            device.key.id.as_str().eq_ignore_ascii_case(factory_code)
                && device.access == AccessScope::Owned
                && device.device_type.as_ref() == Some(&DeviceType::Autofactory)
                && device.location.is_some()
        })
        .ok_or_else(|| format!("selected Autofactory {factory_code} is not available"))?;
    let factory_location = factory
        .location
        .as_ref()
        .map(|location| location.id.as_str().to_owned())
        .ok_or_else(|| format!("Autofactory {factory_code} has no location"))?;
    checkpoint.autofactory = Some(factory_code.to_owned());
    checkpoint.autofactory_location = Some(factory_location.clone());
    context
        .persist_checkpoint(checkpoint)
        .map_err(string_error)?;

    if checkpoint.purchase_authorized {
        if let Some(source) = observe_purchased_blueprint_device(
            client,
            &intent.device_type,
            &checkpoint.pre_purchase_devices,
            &shop.shop_location,
            1,
        )
        .await?
        {
            checkpoint.source_device = Some(source);
            checkpoint.purchase_observed = true;
            context
                .persist_checkpoint(checkpoint)
                .map_err(string_error)?;
            return Ok(true);
        }

        if let Some(operation_id) = checkpoint.purchase_operation.as_deref() {
            let operation = client
                .operations()
                .get(OperationId::new(operation_id.to_owned()));
            let status = operation.status().await.map_err(string_error)?;
            if matches!(
                status,
                OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
            ) {
                return Err(format!(
                    "managed trade operation {operation_id} ended as {status:?} before the target device was observed"
                ));
            }
        }

        if let Some(source) = observe_purchased_blueprint_device(
            client,
            &intent.device_type,
            &checkpoint.pre_purchase_devices,
            &shop.shop_location,
            12,
        )
        .await?
        {
            checkpoint.source_device = Some(source);
            checkpoint.purchase_observed = true;
            context
                .persist_checkpoint(checkpoint)
                .map_err(string_error)?;
            return Ok(true);
        }

        context
            .advance_to("reconciling_purchase", checkpoint)
            .map_err(string_error)?;
        context
            .emit_activity(
                "shop trade was already authorized; waiting for managed ownership evidence before any further mutation"
                    .to_owned(),
            )
            .map_err(string_error)?;
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }

    if !ensure_shop_trade_criteria(
        context,
        client,
        intent,
        checkpoint,
        &factory_location,
        &shop.shop_location,
    )
    .await?
    {
        return Ok(false);
    }

    if !ensure_replicant_at_shop(context, client, checkpoint, replicant, &shop.shop_location)
        .await?
    {
        return Ok(false);
    }

    let live_trade = shop_trades(client, &shop.controller_code)
        .await
        .map_err(string_error)?
        .into_iter()
        .find(|trade| trade.trade_code.eq_ignore_ascii_case(&shop.trade_code))
        .ok_or_else(|| {
            format!(
                "shop trade {} on controller {} is no longer available",
                shop.trade_code, shop.controller_code
            )
        })?;
    if live_trade.current_stock.unwrap_or_default() <= 0 {
        return Err(format!(
            "shop trade {} on controller {} is out of stock",
            shop.trade_code, shop.controller_code
        ));
    }
    if live_trade.criteria_bundle() != shop.criteria {
        return Err(format!(
            "shop trade {} criteria changed after staging; refusing to execute stale criteria",
            shop.trade_code
        ));
    }
    if live_trade
        .rewards_bundle()
        .devices
        .get(&intent.device_type)
        .copied()
        .unwrap_or_default()
        <= 0
    {
        return Err(format!(
            "shop trade {} no longer rewards {}",
            shop.trade_code, intent.device_type
        ));
    }

    let devices = owned_device_snapshots(client).await?;
    checkpoint.pre_purchase_devices = devices
        .iter()
        .filter(|device| {
            device
                .device_type
                .as_ref()
                .is_some_and(|kind| kind.as_str().eq_ignore_ascii_case(&intent.device_type))
        })
        .map(|device| device.key.id.as_str().to_owned())
        .collect();
    checkpoint.pre_purchase_devices.sort();
    checkpoint.purchase_authorized = true;
    context
        .advance_to("purchasing", checkpoint)
        .map_err(string_error)?;

    let operation = client
        .trading()
        .execute(&shop.controller_code, &shop.trade_code)
        .await
        .map_err(string_error)?;
    checkpoint.purchase_submitted = true;
    checkpoint.purchase_operation = Some(operation.id().as_str().to_owned());
    context
        .persist_checkpoint(checkpoint)
        .map_err(string_error)?;

    if let Some(source) = observe_purchased_blueprint_device(
        client,
        &intent.device_type,
        &checkpoint.pre_purchase_devices,
        &shop.shop_location,
        24,
    )
    .await?
    {
        checkpoint.source_device = Some(source);
        checkpoint.purchase_observed = true;
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
        return Ok(true);
    }

    let status = operation.status().await.map_err(string_error)?;
    if matches!(
        status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(format!(
            "managed trade operation {} ended as {status:?} and no rewarded {} device was observed",
            operation.id(),
            intent.device_type
        ));
    }
    context
        .advance_to("awaiting_purchase_evidence", checkpoint)
        .map_err(string_error)?;
    context.mark_waiting().map_err(string_error)?;
    Ok(false)
}

async fn ensure_shop_trade_criteria(
    context: &mut WorkflowContext,
    client: &Client,
    intent: &BlueprintAcquireIntent,
    checkpoint: &mut BlueprintAcquireCheckpoint,
    factory_location: &str,
    shop_location: &str,
) -> Result<bool, String> {
    let criteria = checkpoint
        .criteria
        .clone()
        .or_else(|| intent.shop.as_ref().map(|shop| shop.criteria.clone()))
        .ok_or_else(|| "shop blueprint acquisition has no criteria snapshot".to_owned())?;
    if !criteria.unknown.is_empty() {
        return Err(format!(
            "shop criteria contain unsupported fields {:?}; refusing automatic purchase",
            criteria.unknown.keys().collect::<Vec<_>>()
        ));
    }
    if criteria
        .resources
        .values()
        .chain(criteria.devices.values())
        .any(|quantity| *quantity < 0)
    {
        return Err("shop criteria contain a negative quantity".to_owned());
    }

    let unlocked = client
        .blueprints()
        .unlocked_device_types()
        .await
        .map_err(string_error)?;
    for device_type in criteria.devices.keys() {
        let kind = DeviceType::from(device_type.as_str());
        if !unlocked.contains(&kind) {
            context
                .advance_to("waiting_for_criterion_blueprint", checkpoint)
                .map_err(string_error)?;
            context
                .emit_activity(format!(
                    "trade criterion device {device_type} has no unlocked blueprint; waiting for its Blueprint requirement"
                ))
                .map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
    }

    // Reattach to previously-created staging work first. Once it succeeds,
    // re-enter from fresh managed inventory rather than carrying the old
    // missing-resource/device calculation forward.
    if let Some(child_id) = checkpoint.criteria_logistics_child {
        if !await_child_workflow(context, child_id, "trade-criteria logistics", checkpoint).await? {
            return Ok(false);
        }
        checkpoint.criteria_logistics_child = None;
        context
            .advance_to("rechecking_trade_criteria", checkpoint)
            .map_err(string_error)?;
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }

    let mut census = DeviceCensus::default();
    let mut devices = census.snapshots(client).await?;
    let mut print_requests = Vec::new();
    for (device_type, required) in &criteria.devices {
        let available = devices
            .iter()
            .filter(|device| trade_payment_device_is_releasable(device, device_type))
            .count() as i64;
        if available < *required {
            print_requests.push(PrintRequest::new(device_type.clone(), required - available));
        }
    }
    if !print_requests.is_empty() {
        let tag = checkpoint
            .criteria_print_tag
            .get_or_insert_with(|| format!("dir-bp-pay:{}", &context.id().to_string()[..8]))
            .clone();
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
        context
            .advance_to("printing_trade_criteria", checkpoint)
            .map_err(string_error)?;
        let mut options = QueueOptions::at(factory_location.to_owned());
        options.tags = vec![tag.clone()];
        options.wait_timeout = Duration::from_secs(DEFAULT_WAIT_SECONDS);
        queue_prints_with_components(client, &print_requests, &options)
            .await
            .map_err(string_error)?;
        client
            .devices()
            .refresh_many()
            .with_tag(tag)
            .at(factory_location.to_owned())
            .collect()
            .await
            .map_err(string_error)?;
        census.invalidate();
        devices = census.snapshots(client).await?;
    }

    let inventories = fetch_account_inventories(client).await?;
    let resources_at_shop = inventory_at_location(&inventories, shop_location);
    let mut missing_resources = ResourceMap::new();
    for (resource, required) in &criteria.resources {
        let present = resources_at_shop.get(resource).copied().unwrap_or_default();
        if present < *required {
            missing_resources.insert(resource.clone(), required - present);
        }
    }

    // Director trade resources come from the selected manufacturing home, not
    // arbitrary account stock. In the current empire layout this resolves to
    // consolidation hubs such as SCEPTURUM or THYFFAWFF.
    let resource_origin = resolve_location_system(client, factory_location).await?;
    let resources_at_home = inventory_in_system(&inventories, &resource_origin);
    for (resource, missing) in &missing_resources {
        let available = resources_at_home.get(resource).copied().unwrap_or_default();
        if available < *missing {
            context
                .advance_to("waiting_for_trade_criteria", checkpoint)
                .map_err(string_error)?;
            context
                .emit_activity(format!(
                    "shop trade needs {missing} more {resource}, but regional consolidation hub {resource_origin} only has {available}"
                ))
                .map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
    }

    let mut staged_codes = Vec::new();
    for (device_type, required) in &criteria.devices {
        let mut at_shop = devices
            .iter()
            .filter(|device| {
                trade_payment_device_is_releasable(device, device_type)
                    && device.location.as_ref().is_some_and(|location| {
                        location.id.as_str().eq_ignore_ascii_case(shop_location)
                    })
            })
            .map(|device| device.key.id.as_str().to_owned())
            .collect::<Vec<_>>();
        at_shop.sort();
        let required_count = usize::try_from(*required)
            .map_err(|_| format!("invalid trade device criterion quantity {required}"))?;
        let missing = required_count.saturating_sub(at_shop.len());
        if missing == 0 {
            continue;
        }
        let mut elsewhere = devices
            .iter()
            .filter(|device| {
                trade_payment_device_is_releasable(device, device_type)
                    && device.location.as_ref().is_some_and(|location| {
                        !location.id.as_str().eq_ignore_ascii_case(shop_location)
                    })
            })
            .map(|device| device.key.id.as_str().to_owned())
            .collect::<Vec<_>>();
        elsewhere.sort();
        if elsewhere.len() < missing {
            context
                .advance_to("waiting_for_trade_criteria", checkpoint)
                .map_err(string_error)?;
            context
                .emit_activity(format!(
                    "trade requires {required} {device_type}, but only {} releasable account copies are available after printing",
                    at_shop.len() + elsewhere.len()
                ))
                .map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
        staged_codes.extend(elsewhere.into_iter().take(missing));
    }

    // Stage resource criteria separately so `account`-wide device sourcing can
    // never cause resource pickups to wander out to arbitrary belts.
    if !missing_resources.is_empty() {
        let shop = intent
            .shop
            .as_ref()
            .ok_or_else(|| "shop blueprint acquisition lost its shop intent".to_owned())?;
        let child = context
            .create_child(new_logistics_manifest_workflow(LogisticsManifestIntent {
                origin: resource_origin.clone(),
                destination: shop_location.to_owned(),
                resources: missing_resources,
                devices: Vec::new(),
                device_codes: Vec::new(),
                pre_deactivate_device_codes: Vec::new(),
                device_tags: Vec::new(),
                release_mining_reservations: false,
                placement_recovery: None,
                return_transports: false,
                allow_transport_staging: true,
                region: intent.preferred_region.clone(),
                purpose: format!(
                    "director:blueprint_shop_criteria:{}:{}:{}:resources",
                    intent.device_type, shop.controller_code, shop.trade_code
                ),
            }))
            .map_err(string_error)?;
        checkpoint.criteria_logistics_child = Some(child.id);
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
        if !await_child_workflow(
            context,
            child.id,
            "trade-criteria resource logistics",
            checkpoint,
        )
        .await?
        {
            return Ok(false);
        }
        checkpoint.criteria_logistics_child = None;
        context
            .advance_to("rechecking_trade_criteria", checkpoint)
            .map_err(string_error)?;
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }

    if !staged_codes.is_empty() {
        let shop = intent
            .shop
            .as_ref()
            .ok_or_else(|| "shop blueprint acquisition lost its shop intent".to_owned())?;
        let child = context
            .create_child(new_logistics_manifest_workflow(LogisticsManifestIntent {
                origin: "account".to_owned(),
                destination: shop_location.to_owned(),
                resources: ResourceMap::new(),
                devices: Vec::new(),
                device_codes: staged_codes.clone(),
                pre_deactivate_device_codes: Vec::new(),
                device_tags: Vec::new(),
                release_mining_reservations: false,
                placement_recovery: None,
                return_transports: false,
                allow_transport_staging: true,
                region: intent.preferred_region.clone(),
                purpose: format!(
                    "director:blueprint_shop_criteria:{}:{}:{}:devices",
                    intent.device_type, shop.controller_code, shop.trade_code
                ),
            }))
            .map_err(string_error)?;
        for code in &staged_codes {
            context
                .repository()
                .acquire_claim(child.id, ResourceKey::Device(code.clone()))
                .map_err(string_error)?;
        }
        checkpoint.criteria_logistics_child = Some(child.id);
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
        if !await_child_workflow(
            context,
            child.id,
            "trade-criteria device logistics",
            checkpoint,
        )
        .await?
        {
            return Ok(false);
        }
        checkpoint.criteria_logistics_child = None;
        context
            .advance_to("rechecking_trade_criteria", checkpoint)
            .map_err(string_error)?;
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }

    for (device_type, required) in &criteria.devices {
        let mut at_shop = devices
            .iter()
            .filter(|device| {
                trade_payment_device_is_releasable(device, device_type)
                    && device.location.as_ref().is_some_and(|location| {
                        location.id.as_str().eq_ignore_ascii_case(shop_location)
                    })
            })
            .map(|device| device.key.id.as_str().to_owned())
            .collect::<Vec<_>>();
        at_shop.sort();
        let required_count = usize::try_from(*required)
            .map_err(|_| format!("invalid trade device criterion quantity {required}"))?;
        if at_shop.len() < required_count {
            context
                .advance_to("waiting_for_trade_criteria", checkpoint)
                .map_err(string_error)?;
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
        for code in at_shop.into_iter().take(required_count) {
            claim_device(context, &code)?;
        }
    }
    let resources_now = fetch_inventory_at_location(client, shop_location).await?;
    if criteria.resources.iter().any(|(resource, required)| {
        resources_now.get(resource).copied().unwrap_or_default() < *required
    }) {
        context
            .advance_to("waiting_for_trade_criteria", checkpoint)
            .map_err(string_error)?;
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }

    Ok(true)
}

async fn await_child_workflow(
    context: &mut WorkflowContext,
    child_id: WorkflowId,
    label: &str,
    checkpoint: &mut BlueprintAcquireCheckpoint,
) -> Result<bool, String> {
    loop {
        let Some(child) = context.repository().read(child_id).map_err(string_error)? else {
            return Err(format!("{label} child {child_id} disappeared"));
        };
        match child.status {
            WorkflowStatus::Succeeded => return Ok(true),
            WorkflowStatus::Failed | WorkflowStatus::Cancelled => {
                let class = child
                    .checkpoint::<LogisticsWorkflowCheckpoint>()
                    .ok()
                    .and_then(|checkpoint| checkpoint.failure_class);
                let error = child.last_error.unwrap_or_default();
                if child.status == WorkflowStatus::Failed
                    && retryable_trade_criteria_logistics_failure(class, &error)
                {
                    checkpoint.criteria_logistics_child = None;
                    context
                        .advance_to("replanning_trade_criteria", checkpoint)
                        .map_err(string_error)?;
                    context
                        .emit_activity(format!(
                            "{label} child {child_id} hit stale source stock ({error}); recomputing the remaining shop criteria"
                        ))
                        .map_err(string_error)?;
                    context.mark_waiting().map_err(string_error)?;
                    return Ok(false);
                }
                return Err(format!(
                    "{label} child {child_id} ended as {:?}: {error}",
                    child.status
                ));
            }
            _ => {
                context
                    .advance_to("awaiting_trade_criteria", checkpoint)
                    .map_err(string_error)?;
                match context.control_request().map_err(string_error)? {
                    replicant_workflow::ControlRequest::Continue => {}
                    replicant_workflow::ControlRequest::Pause
                    | replicant_workflow::ControlRequest::Cancel => return Ok(false),
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

fn retryable_trade_criteria_logistics_failure(
    class: Option<FailureClass>,
    legacy_message: &str,
) -> bool {
    class.or_else(|| failure_class_from_message(legacy_message))
        == Some(FailureClass::LogisticsStateStale)
}

async fn ensure_replicant_at_shop(
    context: &mut WorkflowContext,
    client: &Client,
    checkpoint: &BlueprintAcquireCheckpoint,
    replicant_code: &str,
    destination: &str,
) -> Result<bool, String> {
    let handle = client
        .replicants()
        .get_owned(replicant_code)
        .await
        .map_err(string_error)?;
    let snapshot = handle.snapshot().await.map_err(string_error)?;
    if snapshot
        .location
        .as_ref()
        .is_some_and(|location| location.id.as_str().eq_ignore_ascii_case(destination))
    {
        return Ok(true);
    }
    if let Some(travel) = &snapshot.travel {
        let planned = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        if planned.is_some_and(|planned| planned.eq_ignore_ascii_case(destination)) {
            context.mark_waiting().map_err(string_error)?;
            return Ok(false);
        }
        return Err(format!(
            "acquisition Replicant {replicant_code} is already travelling to {:?}, not {destination}",
            planned
        ));
    }
    context
        .advance_to("travelling_to_shop", checkpoint)
        .map_err(string_error)?;
    let operation = handle
        .travel()
        .to(destination.to_owned())
        .depart()
        .await
        .map_err(string_error)?;
    await_success(&operation).await?;
    let refreshed = handle.refresh().await.map_err(string_error)?;
    let snapshot = refreshed.snapshot().await.map_err(string_error)?;
    if snapshot
        .location
        .as_ref()
        .is_some_and(|location| location.id.as_str().eq_ignore_ascii_case(destination))
    {
        Ok(true)
    } else {
        context.mark_waiting().map_err(string_error)?;
        Ok(false)
    }
}

async fn fetch_account_inventories(
    client: &Client,
) -> Result<Vec<replicant_client::domain::Inventory>, String> {
    let mut cursor = None;
    let mut inventories = Vec::new();
    for _ in 0..100 {
        let (mut page, next_cursor) = client
            .inventory()
            .list(&replicant_client::raw::inventory::AccountInventoryQuery {
                location: None,
                cursor,
                limit: Some(100),
            })
            .await
            .map_err(string_error)?;
        inventories.append(&mut page);
        let Some(next) = next_cursor else {
            return Ok(inventories);
        };
        cursor = Some(next);
    }
    Err("account inventory exceeded the 100-page safety bound".to_owned())
}

async fn fetch_inventory_at_location(
    client: &Client,
    location: &str,
) -> Result<ResourceMap, String> {
    let (inventories, _) = client
        .inventory()
        .list(&replicant_client::raw::inventory::AccountInventoryQuery {
            location: Some(location.to_owned()),
            cursor: None,
            limit: Some(100),
        })
        .await
        .map_err(string_error)?;
    Ok(inventory_at_location(&inventories, location))
}

fn inventory_at_location(
    inventories: &[replicant_client::domain::Inventory],
    location: &str,
) -> ResourceMap {
    let mut resources = ResourceMap::new();
    for inventory in inventories.iter().filter(|inventory| {
        inventory
            .location
            .as_ref()
            .is_some_and(|key| key.id.as_str().eq_ignore_ascii_case(location))
    }) {
        for item in &inventory.items {
            *resources.entry(item.resource.clone()).or_default() += item.quantity;
        }
    }
    resources
}

fn inventory_in_system(
    inventories: &[replicant_client::domain::Inventory],
    system: &str,
) -> ResourceMap {
    let mut resources = ResourceMap::new();
    for inventory in inventories.iter().filter(|inventory| {
        inventory
            .location
            .as_ref()
            .is_some_and(|key| designation_in_system(key.id.as_str(), system))
    }) {
        for item in &inventory.items {
            *resources.entry(item.resource.clone()).or_default() += item.quantity;
        }
    }
    resources
}

fn trade_payment_device_is_releasable(device: &Device, device_type: &str) -> bool {
    blueprint_source_is_releasable(device, device_type)
}

async fn observe_purchased_blueprint_device(
    client: &Client,
    device_type: &str,
    before: &[String],
    shop_location: &str,
    attempts: usize,
) -> Result<Option<String>, String> {
    let before = before
        .iter()
        .map(|code| code.to_ascii_uppercase())
        .collect::<std::collections::BTreeSet<_>>();
    for attempt in 0..attempts.max(1) {
        let handles = client
            .devices()
            .refresh_many()
            .of_type(DeviceType::from(device_type))
            .at(shop_location.to_owned())
            .collect()
            .await
            .map_err(string_error)?;
        let mut refreshed = Vec::with_capacity(handles.len());
        for handle in handles {
            refreshed.push(handle.snapshot().await.map_err(string_error)?);
        }
        let mut candidates = refreshed
            .into_iter()
            .filter(|device| {
                !before.contains(&device.key.id.as_str().to_ascii_uppercase())
                    && blueprint_source_is_releasable(device, device_type)
                    && device.location.as_ref().is_some_and(|location| {
                        location.id.as_str().eq_ignore_ascii_case(shop_location)
                    })
            })
            .collect::<Vec<_>>();
        candidates.sort_by_key(|device| device.key.id.clone());
        if let Some(device) = candidates.first() {
            return Ok(Some(device.key.id.as_str().to_owned()));
        }
        if attempt + 1 < attempts.max(1) {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    Ok(None)
}

fn blueprint_replicant_destination_matches(
    location: &str,
    destination: &str,
    accept_system_location: bool,
) -> bool {
    location.eq_ignore_ascii_case(destination)
        || (accept_system_location && designation_in_system(location, destination))
}

async fn ensure_blueprint_replicant_at(
    context: &mut WorkflowContext,
    client: &Client,
    checkpoint: &BlueprintAcquireCheckpoint,
    replicant_code: &str,
    destination: &str,
    accept_system_location: bool,
    step: &str,
) -> Result<bool, String> {
    let handle = client
        .replicants()
        .get_owned(replicant_code)
        .await
        .map_err(string_error)?;
    let snapshot = handle.snapshot().await.map_err(string_error)?;
    if snapshot.location.as_ref().is_some_and(|location| {
        snapshot.travel.is_none()
            && blueprint_replicant_destination_matches(
                location.id.as_str(),
                destination,
                accept_system_location,
            )
    }) {
        return Ok(true);
    }

    if let Some(travel) = &snapshot.travel {
        let planned = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        context.advance_to(step, checkpoint).map_err(string_error)?;
        if !planned.is_some_and(|planned| {
            blueprint_replicant_destination_matches(planned, destination, accept_system_location)
        }) {
            context
                .emit_activity(format!(
                    "blueprint control Replicant {replicant_code} is already travelling to {:?}; waiting to continue toward {destination}",
                    planned
                ))
                .map_err(string_error)?;
        }
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }

    context.advance_to(step, checkpoint).map_err(string_error)?;
    context
        .emit_activity(format!(
            "dispatching blueprint control Replicant {replicant_code} to {destination}"
        ))
        .map_err(string_error)?;
    let operation = handle
        .travel()
        .to(destination.to_owned())
        .depart()
        .await
        .map_err(string_error)?;
    await_success(&operation).await?;

    let snapshot = handle
        .refresh()
        .await
        .map_err(string_error)?
        .snapshot()
        .await
        .map_err(string_error)?;
    if snapshot.location.as_ref().is_some_and(|location| {
        snapshot.travel.is_none()
            && blueprint_replicant_destination_matches(
                location.id.as_str(),
                destination,
                accept_system_location,
            )
    }) {
        Ok(true)
    } else {
        context.mark_waiting().map_err(string_error)?;
        Ok(false)
    }
}

async fn prepare_blueprint_source_for_transport(
    context: &mut WorkflowContext,
    client: &Client,
    checkpoint: &BlueprintAcquireCheckpoint,
    source: Device,
) -> Result<Device, String> {
    if source.relationships.stowed_in.is_none() {
        return Ok(source);
    }

    let code = source.key.id.as_str().to_owned();
    claim_device(context, &code)?;
    context
        .advance_to("deploying_blueprint_source", checkpoint)
        .map_err(string_error)?;
    context
        .emit_activity(format!(
            "deploying stowed {device_type} {code} before blueprint acquisition",
            device_type = source
                .device_type
                .as_ref()
                .map_or("device", |kind| kind.as_str())
        ))
        .map_err(string_error)?;

    let operation = client
        .devices()
        .get(&code)
        .await
        .map_err(string_error)?
        .deploy()
        .await
        .map_err(string_error)?;
    await_success(&operation).await?;

    for attempt in 0..24 {
        if let Ok(handle) = client.devices().refresh(&code).await {
            let refreshed = handle.snapshot().await.map_err(string_error)?;
            if refreshed.relationships.stowed_in.is_none()
                && refreshed.location.is_some()
                && refreshed.travel.is_none()
            {
                return Ok(refreshed);
            }
        }
        if attempt + 1 < 24 {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    Err(format!(
        "stowed blueprint source {code} did not become a free-standing device after deploy"
    ))
}

fn resolve_blueprint_source(
    intent: &BlueprintAcquireIntent,
    checkpoint: &BlueprintAcquireCheckpoint,
    devices: &[Device],
) -> Result<Device, String> {
    let pinned = checkpoint
        .source_device
        .as_deref()
        .or(intent.source_device.as_deref());
    if let Some(code) = pinned.filter(|code| !code.trim().is_empty()) {
        let device = devices
            .iter()
            .find(|device| device.key.id.as_str().eq_ignore_ascii_case(code))
            .ok_or_else(|| format!("selected blueprint source {code} is no longer owned"))?;
        let is_candidate = blueprint_source_is_candidate(device, &intent.device_type, devices)
            || (checkpoint.control_escort_required
                && blueprint_source_is_control_revealed_candidate(
                    device,
                    &intent.device_type,
                    devices,
                ));
        if !is_candidate {
            return Err(format!(
                "selected blueprint source {code} is no longer available for blueprint acquisition"
            ));
        }
        return Ok(device.clone());
    }
    devices
        .iter()
        .filter(|device| blueprint_source_is_candidate(device, &intent.device_type, devices))
        .min_by_key(|device| device.key.id.clone())
        .cloned()
        .ok_or_else(|| {
            format!(
                "no owned {} device is currently available for blueprint acquisition",
                intent.device_type
            )
        })
}

fn resolve_blueprint_factory(
    intent: &BlueprintAcquireIntent,
    checkpoint: &BlueprintAcquireCheckpoint,
    devices: &[Device],
    source: &Device,
) -> Result<Device, String> {
    let pinned = checkpoint
        .autofactory
        .as_deref()
        .or(intent.autofactory.as_deref());
    if let Some(code) = pinned.filter(|code| !code.trim().is_empty()) {
        return devices
            .iter()
            .find(|device| {
                device.key != source.key
                    && device.key.id.as_str().eq_ignore_ascii_case(code)
                    && device.access == AccessScope::Owned
                    && device.device_type.as_ref() == Some(&DeviceType::Autofactory)
                    && device.location.is_some()
            })
            .cloned()
            .ok_or_else(|| format!("selected Autofactory {code} is not available"));
    }
    let source_location = blueprint_source_location(source, devices);
    devices
        .iter()
        .filter(|device| {
            device.key != source.key
                && device.access == AccessScope::Owned
                && device.device_type.as_ref() == Some(&DeviceType::Autofactory)
                && device.location.is_some()
        })
        .min_by_key(|factory| {
            let same_location = source_location.is_some_and(|source_location| {
                factory.location.as_ref().is_some_and(|location| {
                    location.id.as_str().eq_ignore_ascii_case(source_location)
                })
            });
            (!same_location, factory.key.id.clone())
        })
        .cloned()
        .ok_or_else(|| "no owned Autofactory is available for blueprint acquisition".to_owned())
}

fn blueprint_source_is_releasable(device: &Device, device_type: &str) -> bool {
    device.access == AccessScope::Owned
        && !workflow_reserved(&device.tags)
        && device
            .device_type
            .as_ref()
            .is_some_and(|kind| kind.as_str().eq_ignore_ascii_case(device_type))
        && device.location.is_some()
        && device.travel.is_none()
        && device.relationships.attached_to.is_none()
        && device.relationships.stowed_in.is_none()
        && device.relationships.controller.is_none()
        && device.relationships.hosting_replicant.is_none()
        && device.relationships.attached_devices.is_empty()
        && device.relationships.stowed_devices.is_empty()
        && device.relationships.controlled_devices.is_empty()
        && device.status.as_ref().is_some_and(|status| {
            matches!(
                status.as_str().to_ascii_lowercase().as_str(),
                "inactive" | "deactivated" | "idle" | "recalled" | "compacted"
            )
        })
}

/// Returns the physical location that can be used to stage an owned blueprint
/// source. Stowed devices intentionally have no direct location in managed
/// state, so use their stationary container's location until they are deployed.
pub(crate) fn blueprint_source_location<'a>(
    device: &'a Device,
    devices: &'a [Device],
) -> Option<&'a str> {
    if device.travel.is_none()
        && let Some(location) = device.location.as_ref()
    {
        return Some(location.id.as_str());
    }

    let container = device.relationships.stowed_in.as_ref()?;
    let container = devices
        .iter()
        .find(|candidate| candidate.key == *container)?;
    if container.travel.is_some() {
        return None;
    }
    container
        .location
        .as_ref()
        .map(|location| location.id.as_str())
}

/// Whether an owned device can safely become the sacrificial source for a
/// missing blueprint. This is deliberately broader than
/// `blueprint_source_is_releasable`: starter equipment may be stowed, while
/// devices in disconnected systems are represented as `out_of_range`. Both
/// are preparable states and should not make the Director pretend no owned
/// source exists.
fn blueprint_source_is_structurally_safe(
    device: &Device,
    device_type: &str,
    devices: &[Device],
) -> bool {
    device.access == AccessScope::Owned
        && !workflow_reserved(&device.tags)
        && device
            .device_type
            .as_ref()
            .is_some_and(|kind| kind.as_str().eq_ignore_ascii_case(device_type))
        && device.travel.is_none()
        && device.relationships.attached_to.is_none()
        && device.relationships.controller.is_none()
        && device.relationships.hosting_replicant.is_none()
        && device.relationships.attached_devices.is_empty()
        && device.relationships.stowed_devices.is_empty()
        && device.relationships.controlled_devices.is_empty()
        && blueprint_source_location(device, devices).is_some()
}

fn blueprint_source_is_control_revealed_candidate(
    device: &Device,
    device_type: &str,
    devices: &[Device],
) -> bool {
    blueprint_source_is_structurally_safe(device, device_type, devices)
        && device
            .available_commands
            .iter()
            .any(|command| command.as_str().eq_ignore_ascii_case("deactivate"))
}

pub(crate) fn blueprint_source_is_candidate(
    device: &Device,
    device_type: &str,
    devices: &[Device],
) -> bool {
    if !blueprint_source_is_structurally_safe(device, device_type, devices) {
        return false;
    }

    let status = device
        .status
        .as_ref()
        .map(|status| status.as_str().to_ascii_lowercase())
        .unwrap_or_default();
    match status.as_str() {
        "inactive" | "deactivated" | "idle" | "recalled" | "compacted" => {
            device.relationships.stowed_in.is_none()
        }
        "stowed" => {
            device.relationships.stowed_in.is_some()
                && device
                    .available_commands
                    .iter()
                    .any(|command| command.as_str().eq_ignore_ascii_case("deploy"))
        }
        "out_of_range" => {
            device.relationships.stowed_in.is_none()
                && device.available_commands.iter().any(|command| {
                    command.as_str().eq_ignore_ascii_case("decommission")
                        || command.as_str().eq_ignore_ascii_case("travel")
                })
        }
        _ => false,
    }
}

const SCAN_TOUR_SURVEY_DRONES: i64 = 3;

fn scan_tour_fleet_print_requests(
    controller_count: usize,
    drone_count: usize,
) -> Vec<PrintRequest> {
    let mut requests = Vec::new();
    if controller_count == 0 {
        requests.push(PrintRequest::new("ami_survey_controller", 1));
    }
    let drone_shortfall = SCAN_TOUR_SURVEY_DRONES.saturating_sub(drone_count as i64);
    if drone_shortfall > 0 {
        requests.push(PrintRequest::new("survey_drone", drone_shortfall));
    }
    requests
}

fn claimed_scan_tour_devices(context: &WorkflowContext) -> Result<BTreeSet<String>, String> {
    Ok(context
        .repository()
        .device_claims()
        .map_err(string_error)?
        .into_iter()
        .filter(|claim| claim.workflow_id != context.id())
        .filter_map(|claim| match claim.resource {
            ResourceKey::Device(code) => Some(code),
            _ => None,
        })
        .collect())
}

#[derive(Clone, Debug)]
struct ScanTourFleetDeviceCandidate {
    code: String,
    stowed: bool,
    controller: Option<String>,
    controlled_devices: BTreeSet<String>,
}

#[derive(Clone, Debug, Default)]
struct ScanTourFleetAvailability {
    controllers: Vec<String>,
    drones: Vec<String>,
    stowed_selected: usize,
}

fn scan_tour_available_device(device: &Device, device_type: &str, stowed: bool) -> bool {
    device
        .device_type
        .as_ref()
        .is_some_and(|kind| kind.as_str().eq_ignore_ascii_case(device_type))
        && device.status.as_ref().is_some_and(|status| {
            status.as_str().eq_ignore_ascii_case("idle")
                || (stowed && status.as_str().eq_ignore_ascii_case("stowed"))
        })
}

fn scan_tour_candidate(device: &Device, stowed: bool) -> ScanTourFleetDeviceCandidate {
    ScanTourFleetDeviceCandidate {
        code: device.key.id.as_str().to_owned(),
        stowed,
        controller: device
            .relationships
            .controller
            .as_ref()
            .map(|controller| controller.id.as_str().to_owned()),
        controlled_devices: device
            .relationships
            .controlled_devices
            .iter()
            .map(|device| device.id.as_str().to_owned())
            .collect(),
    }
}

async fn available_scan_tour_fleet(
    context: &WorkflowContext,
    client: &Client,
    staging_location: &str,
    vessel: &Device,
) -> Result<ScanTourFleetAvailability, String> {
    let claimed = claimed_scan_tour_devices(context)?;
    let vessel_code = vessel.key.id.as_str();
    let mut controllers = Vec::new();
    let mut drones = Vec::new();

    // A completed survey tour normally leaves its controller and drones stowed
    // in the assigned racing vessel. These devices have no ordinary location,
    // so a location-only query misses them and causes the next tour to print a
    // duplicate fleet. Inspect the vessel relationship first and prefer those
    // already-stowed assets.
    for key in &vessel.relationships.stowed_devices {
        let code = key.id.as_str();
        if claimed.contains(code) {
            continue;
        }
        let snapshot = client
            .devices()
            .get(code)
            .await
            .map_err(string_error)?
            .snapshot()
            .await
            .map_err(string_error)?;
        if snapshot
            .relationships
            .stowed_in
            .as_ref()
            .is_none_or(|container| container.id.as_str() != vessel_code)
        {
            continue;
        }
        if scan_tour_available_device(&snapshot, DeviceType::SurveyController.as_str(), true) {
            controllers.push(scan_tour_candidate(&snapshot, true));
        } else if scan_tour_available_device(&snapshot, "survey_drone", true) {
            drones.push(scan_tour_candidate(&snapshot, true));
        }
    }

    let controller_handles = client
        .devices()
        .controllers(DeviceType::SurveyController)
        .owned()
        .idle()
        .at(staging_location)
        .without_adopted_devices()
        .collect()
        .await
        .map_err(string_error)?;
    for handle in controller_handles {
        let code = handle.id().as_str();
        if claimed.contains(code) {
            continue;
        }
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        controllers.push(scan_tour_candidate(&snapshot, false));
    }

    let drone_handles = client
        .devices()
        .find()
        .owned()
        .of_type(DeviceType::from("survey_drone"))
        .idle()
        .at(staging_location)
        .without_controller()
        .collect()
        .await
        .map_err(string_error)?;
    for handle in drone_handles {
        let code = handle.id().as_str();
        if claimed.contains(code) {
            continue;
        }
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        drones.push(scan_tour_candidate(&snapshot, false));
    }

    select_scan_tour_fleet_availability(controllers, drones)
}

fn select_scan_tour_fleet_availability(
    mut controllers: Vec<ScanTourFleetDeviceCandidate>,
    mut drones: Vec<ScanTourFleetDeviceCandidate>,
) -> Result<ScanTourFleetAvailability, String> {
    controllers.sort_by(|left, right| {
        right
            .stowed
            .cmp(&left.stowed)
            .then_with(|| left.code.cmp(&right.code))
    });
    drones.sort_by(|left, right| {
        right
            .stowed
            .cmp(&left.stowed)
            .then_with(|| left.code.cmp(&right.code))
    });

    let required_drones = usize::try_from(SCAN_TOUR_SURVEY_DRONES)
        .map_err(|_| "invalid survey drone requirement".to_owned())?;
    let drone_by_code = drones
        .iter()
        .map(|drone| (drone.code.as_str(), drone))
        .collect::<BTreeMap<_, _>>();
    let mut best: Option<(usize, usize, bool, String, Vec<String>)> = None;

    for controller in &controllers {
        if controller.controlled_devices.len() > required_drones
            || controller.controlled_devices.iter().any(|code| {
                drone_by_code.get(code.as_str()).is_none_or(|drone| {
                    drone
                        .controller
                        .as_deref()
                        .is_some_and(|owner| owner != controller.code.as_str())
                })
            })
        {
            continue;
        }

        let mut selected = controller
            .controlled_devices
            .iter()
            .filter_map(|code| drone_by_code.get(code.as_str()).copied())
            .collect::<Vec<_>>();
        selected.sort_by(|left, right| {
            right
                .stowed
                .cmp(&left.stowed)
                .then_with(|| left.code.cmp(&right.code))
        });
        let already_selected = selected
            .iter()
            .map(|drone| drone.code.as_str())
            .collect::<BTreeSet<_>>();
        let mut fill = drones
            .iter()
            .filter(|drone| !already_selected.contains(drone.code.as_str()))
            .filter(|drone| {
                drone
                    .controller
                    .as_deref()
                    .is_none_or(|owner| owner == controller.code.as_str())
            })
            .collect::<Vec<_>>();
        fill.sort_by(|left, right| {
            right
                .stowed
                .cmp(&left.stowed)
                .then_with(|| left.code.cmp(&right.code))
        });
        selected.extend(fill);
        selected.truncate(required_drones);

        let stowed_selected =
            usize::from(controller.stowed) + selected.iter().filter(|drone| drone.stowed).count();
        let score = (selected.len(), stowed_selected, controller.stowed);
        let replace = best
            .as_ref()
            .is_none_or(|(count, stowed, controller_stowed, _, _)| {
                score > (*count, *stowed, *controller_stowed)
            });
        if replace {
            best = Some((
                selected.len(),
                stowed_selected,
                controller.stowed,
                controller.code.clone(),
                selected
                    .into_iter()
                    .map(|drone| drone.code.clone())
                    .collect(),
            ));
        }
    }

    if let Some((_, stowed_selected, _, controller, selected_drones)) = best {
        return Ok(ScanTourFleetAvailability {
            controllers: vec![controller],
            drones: selected_drones,
            stowed_selected,
        });
    }

    // If there is no usable controller yet, only controller-free drones can be
    // paired safely with a newly printed controller. A drone still adopted by
    // some other controller is not counted toward the manufacturing deficit.
    let mut free_drones = drones
        .into_iter()
        .filter(|drone| drone.controller.is_none())
        .collect::<Vec<_>>();
    free_drones.sort_by(|left, right| {
        right
            .stowed
            .cmp(&left.stowed)
            .then_with(|| left.code.cmp(&right.code))
    });
    free_drones.truncate(required_drones);
    let stowed_selected = free_drones.iter().filter(|drone| drone.stowed).count();
    Ok(ScanTourFleetAvailability {
        controllers: Vec::new(),
        drones: free_drones.into_iter().map(|drone| drone.code).collect(),
        stowed_selected,
    })
}

fn ensure_scan_tour_stow_capacity(
    context: &mut WorkflowContext,
    checkpoint: &ScanTourCheckpoint,
    vessel: &Device,
    availability: &ScanTourFleetAvailability,
) -> Result<bool, String> {
    let Some(capacity) = vessel.stow_capacity else {
        return Ok(true);
    };
    let required_fleet_slots = 1_i64.saturating_add(SCAN_TOUR_SURVEY_DRONES.max(0));
    let stowed_selected = i64::try_from(availability.stowed_selected)
        .map_err(|_| "survey fleet slot requirement overflowed".to_owned())?;
    let additional_slots = required_fleet_slots.saturating_sub(stowed_selected);
    let used = vessel.effective_stow_used();
    if used.saturating_add(additional_slots) <= capacity {
        return Ok(true);
    }

    context
        .advance_to("waiting_for_survey_fleet_capacity", checkpoint)
        .map_err(string_error)?;
    context
        .emit_activity(format!(
            "survey vessel {} has stow capacity {capacity}, currently uses {used}, and needs {additional_slots} free slot(s) for the selected/replacement survey fleet; waiting instead of manufacturing redundant devices",
            vessel.key.id.as_str()
        ))
        .map_err(string_error)?;
    context.mark_waiting().map_err(string_error)?;
    Ok(false)
}

fn reserve_scan_tour_fleet(
    context: &mut WorkflowContext,
    checkpoint: &mut ScanTourCheckpoint,
    controllers: &[String],
    drones: &[String],
) -> Result<bool, String> {
    let required_drones = usize::try_from(SCAN_TOUR_SURVEY_DRONES)
        .map_err(|_| "invalid survey drone requirement".to_owned())?;
    let Some(controller) = controllers.first() else {
        return Ok(false);
    };
    if drones.len() < required_drones {
        return Ok(false);
    }
    let selected_drones = drones[..required_drones].to_vec();
    let mut acquired = Vec::new();
    for code in std::iter::once(controller).chain(selected_drones.iter()) {
        let resource = ResourceKey::Device(code.clone());
        match context.acquire_claim(resource.clone()) {
            Ok(_) => acquired.push(resource),
            Err(RepositoryError::ClaimConflict { .. }) => {
                for resource in &acquired {
                    context.release_claim(resource).map_err(string_error)?;
                }
                return Ok(false);
            }
            Err(error) => return Err(string_error(error)),
        }
    }
    checkpoint.fleet_controller = Some(controller.clone());
    checkpoint.fleet_drones = selected_drones;
    context
        .persist_checkpoint(checkpoint)
        .map_err(string_error)?;
    Ok(true)
}

async fn ensure_scan_tour_fleet_capacity(
    context: &mut WorkflowContext,
    client: &Client,
    vessel: &str,
    maintenance_home: &str,
    checkpoint: &mut ScanTourCheckpoint,
) -> Result<bool, String> {
    if checkpoint
        .state
        .as_ref()
        .is_some_and(|state| state.resources().2.len() > SCAN_TOUR_SURVEY_DRONES as usize)
    {
        return Ok(true);
    }

    let required_drones = usize::try_from(SCAN_TOUR_SURVEY_DRONES)
        .map_err(|_| "invalid survey drone requirement".to_owned())?;
    if let Some(controller) = checkpoint.fleet_controller.clone()
        && checkpoint.fleet_drones.len() == required_drones
    {
        let drones = checkpoint.fleet_drones.clone();
        if reserve_scan_tour_fleet(context, checkpoint, &[controller], &drones)? {
            return Ok(true);
        }
        checkpoint.fleet_controller = None;
        checkpoint.fleet_drones.clear();
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
    } else if checkpoint.fleet_controller.is_some() || !checkpoint.fleet_drones.is_empty() {
        checkpoint.fleet_controller = None;
        checkpoint.fleet_drones.clear();
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
    }

    if let Some(child_id) = checkpoint.fleet_logistics_child {
        let Some(child) = context.repository().read(child_id).map_err(string_error)? else {
            return Err(format!(
                "survey-fleet logistics child {child_id} disappeared"
            ));
        };
        match child.status {
            WorkflowStatus::Succeeded => {
                checkpoint.fleet_logistics_child = None;
                context
                    .persist_checkpoint(checkpoint)
                    .map_err(string_error)?;
            }
            WorkflowStatus::Failed | WorkflowStatus::Cancelled => {
                return Err(format!(
                    "survey-fleet logistics child {child_id} ended as {:?}: {}",
                    child.status,
                    child.last_error.unwrap_or_default()
                ));
            }
            _ => {
                context
                    .advance_to("staging_survey_fleet", checkpoint)
                    .map_err(string_error)?;
                context.mark_waiting().map_err(string_error)?;
                return Ok(false);
            }
        }
    }

    let vessel_snapshot = client
        .devices()
        .get(vessel)
        .await
        .map_err(string_error)?
        .snapshot()
        .await
        .map_err(string_error)?;
    if vessel_snapshot.travel.is_some() || vessel_snapshot.location.is_none() {
        context
            .advance_to("waiting_for_operational_worker", checkpoint)
            .map_err(string_error)?;
        context
            .emit_activity("assigned regional workers are still in transit or lack an authoritative vessel location")
            .map_err(string_error)?;
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }
    let staging_location = vessel_snapshot
        .location
        .as_ref()
        .map(|location| location.id.as_str().to_owned())
        .ok_or_else(|| format!("survey vessel {vessel} has no current staging location"))?;

    let availability =
        available_scan_tour_fleet(context, client, &staging_location, &vessel_snapshot).await?;
    if !ensure_scan_tour_stow_capacity(context, checkpoint, &vessel_snapshot, &availability)? {
        return Ok(false);
    }
    if reserve_scan_tour_fleet(
        context,
        checkpoint,
        &availability.controllers,
        &availability.drones,
    )? {
        context
            .emit_activity(format!(
                "reserved an exclusive survey fleet at {staging_location}: controller {}, drones {}",
                checkpoint.fleet_controller.as_deref().unwrap_or_default(),
                checkpoint.fleet_drones.join(", ")
            ))
            .map_err(string_error)?;
        return Ok(true);
    }

    // A parallel shard may have won the claim race between discovery and
    // reservation. Refresh the claim-aware view before deciding what to print.
    let availability =
        available_scan_tour_fleet(context, client, &staging_location, &vessel_snapshot).await?;
    if !ensure_scan_tour_stow_capacity(context, checkpoint, &vessel_snapshot, &availability)? {
        return Ok(false);
    }
    if reserve_scan_tour_fleet(
        context,
        checkpoint,
        &availability.controllers,
        &availability.drones,
    )? {
        return Ok(true);
    }

    // Only unclaimed devices count toward this tour. Parallel catalogue shards
    // therefore manufacture independent fleets instead of racing to claim the
    // same idle controller/drones after the preflight succeeds.
    let requests =
        scan_tour_fleet_print_requests(availability.controllers.len(), availability.drones.len());
    if requests.is_empty() {
        context
            .advance_to("waiting_for_survey_fleet_claim", checkpoint)
            .map_err(string_error)?;
        context
            .emit_activity(
                "survey fleet changed during reservation; waiting for a fresh claim-aware preflight",
            )
            .map_err(string_error)?;
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }
    let print_location = maintenance_home.to_owned();
    let tag = checkpoint
        .fleet_print_tag
        .get_or_insert_with(|| format!("scan-fleet:{}", &context.id().to_string()[..8]))
        .clone();
    context
        .persist_checkpoint(checkpoint)
        .map_err(string_error)?;
    context
        .advance_to("manufacturing_survey_fleet", checkpoint)
        .map_err(string_error)?;
    context
        .emit_activity(format!(
            "exclusive survey fleet at {staging_location} is short {}; manufacturing at {print_location}",
            requests
                .iter()
                .map(|request| format!("{} {}", request.quantity, request.device_type))
                .collect::<Vec<_>>()
                .join(", ")
        ))
        .map_err(string_error)?;

    let mut options = QueueOptions::at(print_location.clone());
    options.tags = vec![tag.clone()];
    options.wait_timeout = Duration::from_secs(DEFAULT_WAIT_SECONDS);
    queue_prints_with_components(client, &requests, &options)
        .await
        .map_err(string_error)?;

    loop {
        let status = printing_status_in_system(
            client,
            &print_location,
            &requests,
            std::slice::from_ref(&tag),
        )
        .await
        .map_err(string_error)?;
        if status
            .requested
            .iter()
            .all(|line| line.available >= line.required)
        {
            break;
        }
        match context.control_request().map_err(string_error)? {
            replicant_workflow::ControlRequest::Continue => {}
            replicant_workflow::ControlRequest::Pause
            | replicant_workflow::ControlRequest::Cancel => return Ok(false),
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }

    if print_location.eq_ignore_ascii_case(&staging_location) {
        let availability =
            available_scan_tour_fleet(context, client, &staging_location, &vessel_snapshot).await?;
        if !ensure_scan_tour_stow_capacity(context, checkpoint, &vessel_snapshot, &availability)? {
            return Ok(false);
        }
        if reserve_scan_tour_fleet(
            context,
            checkpoint,
            &availability.controllers,
            &availability.drones,
        )? {
            return Ok(true);
        }
        context
            .advance_to("waiting_for_survey_fleet_claim", checkpoint)
            .map_err(string_error)?;
        context
            .emit_activity(format!(
                "survey-fleet manufacturing completed at {staging_location}, but the exclusive controller/{required_drones}-drone reservation changed before it could be claimed; waiting to re-evaluate"
            ))
            .map_err(string_error)?;
        context.mark_waiting().map_err(string_error)?;
        return Ok(false);
    }

    let printed_codes =
        tagged_device_codes_for_requests(client, &tag, &print_location, &requests).await?;
    let child = context
        .create_child(new_logistics_manifest_workflow(LogisticsManifestIntent {
            origin: print_location,
            destination: staging_location,
            resources: ResourceMap::new(),
            devices: Vec::new(),
            device_codes: printed_codes.clone(),
            device_tags: Vec::new(),
            pre_deactivate_device_codes: Vec::new(),
            release_mining_reservations: false,
            placement_recovery: None,
            return_transports: true,
            allow_transport_staging: true,
            region: None,
            purpose: format!("scan-tour-fleet:{}", context.id()),
        }))
        .map_err(string_error)?;
    for code in &printed_codes {
        context
            .repository()
            .acquire_claim(child.id, ResourceKey::Device(code.clone()))
            .map_err(string_error)?;
    }
    checkpoint.fleet_logistics_child = Some(child.id);
    context
        .advance_to("staging_survey_fleet", checkpoint)
        .map_err(string_error)?;
    context.mark_waiting().map_err(string_error)?;
    Ok(false)
}

async fn tagged_device_codes_for_requests(
    client: &Client,
    tag: &str,
    location: &str,
    requests: &[PrintRequest],
) -> Result<Vec<String>, String> {
    let handles = client
        .devices()
        .find()
        .owned()
        .with_tag(tag.to_owned())
        .collect()
        .await
        .map_err(string_error)?;
    let mut candidates = Vec::new();
    for handle in handles {
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if snapshot
            .location
            .as_ref()
            .is_none_or(|candidate| !candidate.id.as_str().eq_ignore_ascii_case(location))
        {
            continue;
        }
        let Some(device_type) = snapshot.device_type.as_ref() else {
            continue;
        };
        candidates.push((
            device_type.as_str().to_owned(),
            handle.id().as_str().to_owned(),
        ));
    }
    candidates.sort();

    let mut selected = Vec::new();
    for request in requests {
        let required = usize::try_from(request.quantity)
            .map_err(|_| format!("invalid survey-fleet print quantity {}", request.quantity))?;
        let matches = candidates
            .iter()
            .filter(|(device_type, _)| device_type.eq_ignore_ascii_case(&request.device_type))
            .map(|(_, code)| code.clone())
            .take(required)
            .collect::<Vec<_>>();
        if matches.len() < required {
            return Err(format!(
                "survey-fleet manufacturing completed but only {} of {} tagged {} devices were found at {}",
                matches.len(),
                required,
                request.device_type,
                location
            ));
        }
        selected.extend(matches);
    }
    selected.sort();
    selected.dedup();
    Ok(selected)
}

fn scan_tour_worker_wait_reason(state: WorkerState) -> Option<&'static str> {
    match state {
        WorkerState::Operational => None,
        WorkerState::InTransit => Some("assigned regional workers are still in transit"),
        WorkerState::Busy
        | WorkerState::WrongRegion
        | WorkerState::MissingVessel
        | WorkerState::UnknownLocation
        | WorkerState::LocationMismatch
        | WorkerState::Unavailable => {
            Some("assigned regional worker has no authoritative stationary vessel location")
        }
    }
}

async fn survey_worker_state(
    client: &Client,
    replicant_code: &str,
    vessel_code: &str,
) -> Result<WorkerState, String> {
    let replicant = client
        .replicants()
        .get_owned(replicant_code)
        .await
        .map_err(string_error)?
        .snapshot()
        .await
        .map_err(string_error)?;
    let vessel = client
        .devices()
        .get(vessel_code)
        .await
        .map_err(string_error)?
        .snapshot()
        .await
        .map_err(string_error)?;
    if vessel
        .relationships
        .hosting_replicant
        .as_ref()
        .is_none_or(|hosted| hosted.id.as_str() != replicant_code)
    {
        return Ok(WorkerState::MissingVessel);
    }
    Ok(classify_regional_worker(
        &replicant,
        Some(&vessel),
        None,
        None,
        None,
        false,
    ))
}

async fn resolve_survey_assignment(
    client: &Client,
    pinned_replicant: Option<&str>,
    pinned_vessel: Option<&str>,
) -> Result<Option<(String, String)>, String> {
    if let Some(vessel_code) = pinned_vessel.filter(|value| !value.trim().is_empty()) {
        let handle = client
            .devices()
            .get(vessel_code)
            .await
            .map_err(string_error)?;
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if snapshot.access != AccessScope::Owned {
            return Err(format!("racing vessel {vessel_code} is not account-owned"));
        }
        if snapshot
            .device_type
            .as_ref()
            .is_none_or(|device_type| device_type.as_str() != DeviceType::RacingVessel.as_str())
        {
            return Err(format!("{vessel_code} is not a racing vessel"));
        }
        let hosted = snapshot
            .relationships
            .hosting_replicant
            .as_ref()
            .map(|key| key.id.as_str().to_owned())
            .ok_or_else(|| format!("racing vessel {vessel_code} is not hosting a replicant"))?;
        if let Some(expected) = pinned_replicant.filter(|value| !value.trim().is_empty())
            && expected != hosted
        {
            return Err(format!(
                "racing vessel {vessel_code} hosts {hosted}, not requested replicant {expected}"
            ));
        }
        return survey_worker_state(client, &hosted, vessel_code)
            .await
            .map(|state| {
                state
                    .is_operational()
                    .then(|| (hosted, vessel_code.to_owned()))
            });
    }

    let handles = client
        .devices()
        .find()
        .owned()
        .of_type(DeviceType::RacingVessel)
        .collect()
        .await
        .map_err(string_error)?;
    for handle in handles {
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        let Some(hosted) = snapshot.relationships.hosting_replicant.as_ref() else {
            continue;
        };
        if pinned_replicant
            .filter(|value| !value.trim().is_empty())
            .is_some_and(|expected| expected != hosted.id.as_str())
        {
            continue;
        }
        if survey_worker_state(client, hosted.id.as_str(), handle.id().as_str())
            .await?
            .is_operational()
        {
            return Ok(Some((
                hosted.id.as_str().to_owned(),
                handle.id().as_str().to_owned(),
            )));
        }
    }
    Ok(None)
}

async fn resolve_controller(
    client: &Client,
    pinned: Option<&str>,
    device_type: DeviceType,
    system: Option<&str>,
) -> Result<String, String> {
    if let Some(code) = pinned.filter(|value| !value.trim().is_empty()) {
        let snapshot = client
            .devices()
            .get(code)
            .await
            .map_err(string_error)?
            .snapshot()
            .await
            .map_err(string_error)?;
        if snapshot.access != AccessScope::Owned {
            return Err(format!("controller {code} is not account-owned"));
        }
        if snapshot.device_type.as_ref() != Some(&device_type) {
            return Err(format!(
                "controller {code} is {}, expected {}",
                snapshot
                    .device_type
                    .as_ref()
                    .map_or("unknown", DeviceType::as_str),
                device_type.as_str(),
            ));
        }
        if snapshot.relationships.controlled_devices.is_empty() {
            return Err(format!("controller {code} has no adopted fleet"));
        }
        if let Some(system) = system.filter(|value| !value.is_empty()) {
            let location = snapshot
                .location
                .as_ref()
                .map(|location| location.id.as_str())
                .unwrap_or_default();
            if !designation_in_system(location, system) {
                return Err(format!(
                    "controller {code} is at {location}, outside requested system {system}"
                ));
            }
        }
        return Ok(code.to_owned());
    }
    let mut query = client.devices().find().owned().of_type(device_type).idle();
    if let Some(system) = system.filter(|value| !value.is_empty()) {
        query = query.in_system(system.to_owned());
    }
    let handles = query.collect().await.map_err(string_error)?;
    for handle in handles {
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if !snapshot.relationships.controlled_devices.is_empty() {
            return Ok(handle.id().as_str().to_owned());
        }
    }
    Err("no eligible idle owned controller with an adopted fleet is available in scope".to_owned())
}

fn try_claim_available(context: &WorkflowContext, key: ResourceKey) -> Result<bool, String> {
    match context.acquire_claim(key) {
        Ok(ClaimAcquireOutcome::Acquired(_) | ClaimAcquireOutcome::AlreadyOwned(_)) => Ok(true),
        Err(RepositoryError::ClaimConflict { owner, .. }) => {
            tracing::debug!(
                workflow_id = %context.id(),
                claim_owner = %owner,
                "workflow resource candidate is already claimed"
            );
            Ok(false)
        }
        Err(error) => Err(string_error(error)),
    }
}

fn release_exploration_claim(context: &WorkflowContext, key: &ResourceKey) -> Result<(), String> {
    context.release_claim(key).map_err(string_error)?;
    Ok(())
}

fn wait_for_exploration_capacity(
    context: &mut WorkflowContext,
    checkpoint: &ExplorationWorkflowCheckpoint,
    step: &str,
    reason: &str,
) -> Result<(), String> {
    tracing::info!(
        workflow_id = %context.id(),
        reason = %reason,
        "exploration frontier is waiting for workflow capacity"
    );
    context.advance_to(step, checkpoint).map_err(string_error)?;
    context.mark_waiting().map_err(string_error)
}

async fn resolve_and_claim_replicant(
    context: &WorkflowContext,
    client: &Client,
    pinned: Option<&str>,
) -> Result<Option<String>, String> {
    let pinned = pinned.filter(|value| !value.trim().is_empty());
    if pinned.is_none()
        && let Some(code) = context
            .claims()
            .map_err(string_error)?
            .into_iter()
            .find_map(|claim| match claim.resource {
                ResourceKey::Replicant(code) => Some(code),
                _ => None,
            })
    {
        return Ok(Some(code));
    }

    if let Some(code) = pinned {
        client
            .replicants()
            .get_owned(code)
            .await
            .map_err(string_error)?;
        return try_claim_available(context, ResourceKey::Replicant(code.to_owned()))
            .map(|available| available.then_some(code.to_owned()));
    }

    let handles = client
        .replicants()
        .find()
        .owned()
        .collect()
        .await
        .map_err(string_error)?;
    for handle in handles {
        let code = handle.id().as_str().to_owned();
        if try_claim_available(context, ResourceKey::Replicant(code.clone()))? {
            return Ok(Some(code));
        }
    }
    Ok(None)
}

struct ExplorationHomeSelection {
    location: String,
    unavailable_autofactories: BTreeSet<String>,
}

fn exploration_claimed_autofactories(
    context: &WorkflowContext,
) -> Result<BTreeSet<String>, String> {
    context
        .repository()
        .autofactory_claims()
        .map_err(string_error)
        .map(|claims| {
            claims
                .into_iter()
                .filter(|claim| claim.workflow_id != context.id())
                .filter_map(|claim| match claim.resource {
                    ResourceKey::Autofactory(code) => Some(code),
                    _ => None,
                })
                .collect()
        })
}

async fn resolve_exploration_home(
    context: &WorkflowContext,
    client: &Client,
    pinned: Option<&str>,
) -> Result<Option<ExplorationHomeSelection>, String> {
    let claimed = exploration_claimed_autofactories(context)?;
    let handles = client
        .devices()
        .find()
        .owned()
        .of_type(DeviceType::Autofactory)
        .collect()
        .await
        .map_err(string_error)?;
    let mut factories_by_location = BTreeMap::<String, Vec<String>>::new();
    for handle in handles {
        let code = handle.id().as_str().to_owned();
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if let Some(location) = snapshot.location.as_ref() {
            factories_by_location
                .entry(location.id.as_str().to_owned())
                .or_default()
                .push(code);
        }
    }
    if factories_by_location.is_empty() {
        return Err("no owned autofactory location is available for staging".to_owned());
    }

    let select = |location: &str, factories: &[String]| {
        let unavailable_autofactories = factories
            .iter()
            .filter(|code| claimed.contains(code.as_str()))
            .cloned()
            .collect::<BTreeSet<_>>();
        let available = factories
            .len()
            .saturating_sub(unavailable_autofactories.len());
        (available, unavailable_autofactories, location.to_owned())
    };

    if let Some(location) = pinned.filter(|value| !value.trim().is_empty()) {
        let factories = factories_by_location.get(location).ok_or_else(|| {
            format!("no owned Autofactory is present at requested manufacturing home {location}")
        })?;
        let (available, unavailable_autofactories, location) = select(location, factories);
        return Ok((available != 0).then_some(ExplorationHomeSelection {
            location,
            unavailable_autofactories,
        }));
    }

    let mut best = None::<(usize, ExplorationHomeSelection)>;
    for (location, factories) in &factories_by_location {
        let (available, unavailable_autofactories, location) = select(location, factories);
        if available == 0 {
            continue;
        }
        if best.as_ref().is_none_or(|(count, _)| available > *count) {
            best = Some((
                available,
                ExplorationHomeSelection {
                    location,
                    unavailable_autofactories,
                },
            ));
        }
    }
    Ok(best.map(|(_, selection)| selection))
}

fn reconcile_exploration_autofactory_claims(
    context: &WorkflowContext,
    required: &BTreeSet<String>,
) -> Result<(), RepositoryError> {
    let stale = context
        .claims()?
        .into_iter()
        .filter_map(|claim| match claim.resource {
            ResourceKey::Autofactory(code) if !required.contains(&code) => {
                Some(ResourceKey::Autofactory(code))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    for resource in stale {
        context.release_claim(&resource)?;
    }
    for code in required {
        context.acquire_claim(ResourceKey::Autofactory(code.clone()))?;
    }
    Ok(())
}

fn release_exploration_autofactory_claims(context: &WorkflowContext) -> Result<(), String> {
    reconcile_exploration_autofactory_claims(context, &BTreeSet::new()).map_err(string_error)
}

fn release_legacy_exploration_location_claims(context: &WorkflowContext) -> Result<(), String> {
    let legacy = context
        .claims()
        .map_err(string_error)?
        .into_iter()
        .filter_map(|claim| match claim.resource {
            ResourceKey::Namespaced { namespace, key } if namespace == "location" => {
                Some(ResourceKey::Namespaced { namespace, key })
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    for resource in &legacy {
        context.release_claim(resource).map_err(string_error)?;
    }
    if !legacy.is_empty() {
        tracing::info!(
            workflow_id = %context.id(),
            released = legacy.len(),
            "released legacy exploration manufacturing-location claim"
        );
    }
    Ok(())
}

async fn resolve_replicant(client: &Client, pinned: Option<&str>) -> Result<String, String> {
    if let Some(code) = pinned.filter(|value| !value.trim().is_empty()) {
        client
            .replicants()
            .get_owned(code)
            .await
            .map_err(string_error)?;
        return Ok(code.to_owned());
    }
    client
        .replicants()
        .find()
        .owned()
        .collect()
        .await
        .map_err(string_error)?
        .first()
        .map(|handle| handle.id().as_str().to_owned())
        .ok_or_else(|| "no owned replicant is available".to_owned())
}

async fn resolve_home(client: &Client, pinned: Option<&str>) -> Result<String, String> {
    if let Some(location) = pinned.filter(|value| !value.trim().is_empty()) {
        return Ok(location.to_owned());
    }
    let handles = client
        .devices()
        .find()
        .owned()
        .of_type(DeviceType::Autofactory)
        .collect()
        .await
        .map_err(string_error)?;
    for handle in handles {
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if let Some(location) = snapshot.location.as_ref() {
            return Ok(location.id.as_str().to_owned());
        }
    }
    Err("no owned autofactory location is available for staging".to_owned())
}

async fn resolve_scan_tour_home(client: &Client, center: &str) -> Result<String, String> {
    let handles = client
        .devices()
        .find()
        .owned()
        .of_type(DeviceType::Autofactory)
        .collect()
        .await
        .map_err(string_error)?;
    let mut locations = Vec::new();
    for handle in handles {
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        let Some(location) = snapshot.location.as_ref() else {
            continue;
        };
        locations.push(location.id.as_str().to_owned());
    }
    scan_tour_factory_home(center, &locations)
        .ok_or_else(|| "no owned autofactory location is available for staging".to_owned())
}

fn scan_tour_factory_home(center: &str, locations: &[String]) -> Option<String> {
    let mut local_factories = BTreeMap::<String, usize>::new();
    for location in locations {
        if designation_in_system(location, center) {
            *local_factories.entry(location.clone()).or_default() += 1;
        }
    }
    if let Some((location, _)) = local_factories.into_iter().max_by(
        |(left_location, left_count), (right_location, right_count)| {
            left_count
                .cmp(right_count)
                .then_with(|| right_location.cmp(left_location))
        },
    ) {
        return Some(location);
    }
    locations.first().cloned()
}

async fn wait_controller_completion(
    context: &mut WorkflowContext,
    client: &Client,
    controller: &str,
    checkpoint: &mut ControllerWorkflowCheckpoint,
) -> Result<bool, String> {
    context
        .advance_to("running", checkpoint)
        .map_err(string_error)?;
    loop {
        match context.control_request().map_err(string_error)? {
            replicant_workflow::ControlRequest::Continue => {}
            replicant_workflow::ControlRequest::Pause
            | replicant_workflow::ControlRequest::Cancel => return Ok(false),
        }
        let snapshot = client
            .devices()
            .get(controller)
            .await
            .map_err(string_error)?
            .refresh()
            .await
            .map_err(string_error)?
            .snapshot()
            .await
            .map_err(string_error)?;
        let coordinating = snapshot
            .status
            .as_ref()
            .is_some_and(|status| status.as_str() == "coordinating");
        if coordinating {
            if !checkpoint.observed_active || checkpoint.idle_observations != 0 {
                checkpoint.observed_active = true;
                checkpoint.idle_observations = 0;
                context
                    .persist_checkpoint(checkpoint)
                    .map_err(string_error)?;
            }
        } else {
            checkpoint.idle_observations = checkpoint.idle_observations.saturating_add(1);
            context
                .persist_checkpoint(checkpoint)
                .map_err(string_error)?;
            if checkpoint.observed_active || checkpoint.idle_observations >= 6 {
                return Ok(true);
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn await_success(operation: &Operation) -> Result<(), String> {
    loop {
        let outcome = operation
            .wait_timeout(Duration::from_secs(30))
            .await
            .map_err(string_error)?;
        match outcome.status {
            OperationStatus::Completed => return Ok(()),
            OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed => {
                return Err(format!(
                    "managed operation {} ended as {:?}: {}",
                    operation.id(),
                    outcome.status,
                    outcome.response.unwrap_or(Value::Null)
                ));
            }
            // Accepted is explicitly nonterminal. Keep observing until the
            // managed journal resolves it rather than treating acceptance as
            // proof that the mutation applied.
            _ => {}
        }
    }
}

fn blueprint_transport_manifest(
    device_type: &str,
    source_code: &str,
    source_location: &str,
    factory_location: &str,
) -> LogisticsManifestIntent {
    LogisticsManifestIntent {
        origin: source_location.to_owned(),
        destination: factory_location.to_owned(),
        device_codes: vec![source_code.to_owned()],
        return_transports: true,
        allow_transport_staging: true,
        purpose: format!(
            "blueprint-acquire:{}:{}",
            device_type.to_ascii_lowercase(),
            source_code
        ),
        ..LogisticsManifestIntent::default()
    }
}

fn delivery_request(intent: &LogisticsIntent) -> DeliveryRequest {
    let mut resources = intent.resources.clone();
    let mut devices = intent.devices.clone();
    let mut device_tags = intent.device_tags.clone();
    if let (Some(payload_kind), Some(item)) = (&intent.payload_kind, intent.item.as_ref())
        && !item.trim().is_empty()
    {
        match payload_kind {
            LogisticsPayloadKind::Resource => {
                *resources.entry(item.clone()).or_default() += intent.quantity;
            }
            LogisticsPayloadKind::Device => devices.push(DeviceRequest {
                quantity: intent.quantity,
                device_type: item.clone(),
            }),
            LogisticsPayloadKind::Tag => device_tags.push(item.clone()),
        }
    }
    DeliveryRequest {
        origin: intent.origin.clone(),
        destination: intent.destination.clone(),
        resources,
        devices,
        device_codes: Vec::new(),
        device_tags,
        carrier: None,
        allow_transport_staging: false,
    }
}

fn strictly_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn canonical_manifest_device_code(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}
/// Validates recovery identity before any workflow claim or managed mutation.
///
/// A placement-recovery manifest is intentionally a closed shape: it may move
/// only its exact canonical device codes and may not smuggle in selectors,
/// resource payloads, pre-deactivation work, or alternate transport policy.
/// Director adoption calls this same validator, so malformed
/// manually-created manifests cannot suppress a valid recovery candidate.
pub(crate) fn validate_placement_recovery_intent(
    intent: &LogisticsManifestIntent,
) -> Result<(), String> {
    let Some(metadata) = intent.placement_recovery.as_ref() else {
        return Ok(());
    };
    if intent
        .region
        .as_deref()
        .is_none_or(|region| region.trim().is_empty())
    {
        return Err("placement recovery requires a nonempty region".to_owned());
    }
    if intent.origin.trim().is_empty() || intent.destination.trim().is_empty() {
        return Err("placement recovery requires exact origin and destination".to_owned());
    }
    if !intent.resources.is_empty()
        || !intent.devices.is_empty()
        || !intent.device_tags.is_empty()
        || !intent.pre_deactivate_device_codes.is_empty()
        || intent.release_mining_reservations
    {
        return Err(
            "placement recovery must contain only exact device codes and recovery metadata"
                .to_owned(),
        );
    }
    if !intent.return_transports || !intent.allow_transport_staging {
        return Err(
            "placement recovery requires return_transports and allow_transport_staging".to_owned(),
        );
    }
    if intent.device_codes.len() != 1
        || !strictly_sorted_unique(&intent.device_codes)
        || intent
            .device_codes
            .iter()
            .any(|code| code != &canonical_manifest_device_code(code))
    {
        return Err("placement recovery requires one canonical exact device code".to_owned());
    }
    let codes = intent.device_codes.iter().cloned().collect::<BTreeSet<_>>();
    if metadata.failed_provenance.len() != codes.len()
        || metadata
            .failed_provenance
            .keys()
            .any(|code| !codes.contains(code))
    {
        return Err("placement recovery provenance must cover every exact device".to_owned());
    }
    if metadata.release_device_tags.len() != codes.len()
        || metadata
            .release_device_tags
            .keys()
            .any(|code| !codes.contains(code))
    {
        return Err("placement recovery release tags must cover every exact device".to_owned());
    }
    for (code, provenance) in &metadata.failed_provenance {
        if code != &canonical_manifest_device_code(code)
            || provenance.is_empty()
            || !strictly_sorted_unique(provenance)
        {
            return Err(format!(
                "placement recovery provenance for {code} is not canonical"
            ));
        }
    }
    for (code, tags) in &metadata.release_device_tags {
        if !codes.contains(code)
            || code != &canonical_manifest_device_code(code)
            || !strictly_sorted_unique(tags)
            || tags
                .iter()
                .any(|tag| tag.trim().is_empty() || !workflow_tag_reserved(tag))
        {
            return Err(format!(
                "placement recovery release tags for {code} are not canonical reserved tags"
            ));
        }
    }
    if !strictly_sorted_unique(&metadata.placement_resolutions) {
        return Err("placement recovery resolutions must be sorted and unique".to_owned());
    }
    for resolution in &metadata.placement_resolutions {
        if resolution.device_code != canonical_manifest_device_code(&resolution.device_code)
            || !codes.contains(&resolution.device_code)
            || !metadata
                .failed_provenance
                .get(&resolution.device_code)
                .is_some_and(|provenance| provenance.contains(&resolution.provenance))
        {
            return Err(format!(
                "placement recovery resolution has an unmatched device or provenance: {}",
                resolution.device_code
            ));
        }
    }
    Ok(())
}

/// Authenticates recovery metadata against the retained typed failed-custody
/// projection. The workflow context has no physical census/topology, so this
/// is deliberately limited to exact workflow evidence; the Director remains
/// responsible for complete physical authority before launching recovery.
pub(crate) fn placement_recovery_metadata_matches_snapshot(
    metadata: &PlacementRecoveryMetadata,
    snapshot: &WorkflowPlacementIntentSnapshot,
) -> Result<(), String> {
    if !snapshot.unknown_live_workflows.is_empty() || !snapshot.unknown_terminal_outcomes.is_empty()
    {
        return Err(
            "placement recovery workflow authority is incomplete; unknown workflow coverage remains"
                .to_owned(),
        );
    }
    let authenticated_tags = metadata
        .release_device_tags
        .values()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    for code in metadata.failed_provenance.keys() {
        let evidence = snapshot.explain_device(code, &authenticated_tags);
        if !evidence.live.is_empty() {
            return Err(format!(
                "placement recovery target {code} has current live workflow placement evidence"
            ));
        }
        if !evidence.settled_placements.is_empty() {
            return Err(format!(
                "placement recovery target {code} has settled workflow placement evidence"
            ));
        }
        if !evidence.terminal_residuals.is_empty() {
            return Err(format!(
                "placement recovery target {code} has terminal residual workflow placement evidence"
            ));
        }
    }
    let mut direct = BTreeMap::<String, BTreeSet<WorkflowPlacementProvenance>>::new();
    let mut tag_provenance = BTreeMap::<String, BTreeSet<WorkflowPlacementProvenance>>::new();
    for evidence in &snapshot.failed_transient {
        let provenance = WorkflowPlacementProvenance {
            workflow_id: evidence.workflow_id,
            work_item_id: evidence.intent.work_item_id,
        };
        match &evidence.intent.subject {
            WorkflowPlacementIntentSubject::Device(code) => {
                direct
                    .entry(canonical_manifest_device_code(code))
                    .or_default()
                    .insert(provenance);
            }
            WorkflowPlacementIntentSubject::DeviceTag(tag) => {
                tag_provenance
                    .entry(tag.clone())
                    .or_default()
                    .insert(provenance);
            }
        }
    }
    let mut expected_resolutions = BTreeSet::new();
    for (code, provenances) in &metadata.failed_provenance {
        let configured_tags = metadata
            .release_device_tags
            .get(code)
            .ok_or_else(|| format!("recovery release tags missing exact device {code}"))?;
        if direct.get(code).is_some_and(|values| {
            values
                .iter()
                .any(|provenance| !provenances.contains(provenance))
        }) {
            return Err(format!(
                "recovery provenance for {code} omits retained exact failed placement evidence"
            ));
        }
        for provenance in provenances {
            let direct_match = direct
                .get(code)
                .is_some_and(|values| values.contains(provenance));
            let tag_match = configured_tags.iter().any(|tag| {
                tag_provenance
                    .get(tag)
                    .is_some_and(|values| values.contains(provenance))
            });
            if !direct_match && !tag_match {
                return Err(format!(
                    "recovery provenance for {code} is not retained exact failed placement evidence"
                ));
            }
        }
        if let Some(values) = direct.get(code) {
            for provenance in values {
                if provenances.contains(provenance) {
                    expected_resolutions.insert(WorkflowPlacementResolution {
                        device_code: code.clone(),
                        provenance: provenance.clone(),
                    });
                }
            }
        }
        for tag in configured_tags {
            if !tag_provenance.get(tag).is_some_and(|values| {
                values
                    .iter()
                    .any(|provenance| provenances.contains(provenance))
            }) {
                return Err(format!(
                    "recovery cleanup tag {tag} for {code} is not retained exact failed placement evidence"
                ));
            }
        }
    }
    let actual_resolutions = metadata
        .placement_resolutions
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if actual_resolutions != expected_resolutions {
        return Err(
            "placement recovery resolutions do not match retained exact failed custody".to_owned(),
        );
    }
    Ok(())
}
fn validate_regional_dispatch(intent: &RegionalDispatchIntent) -> Result<(), String> {
    if intent.source.trim().is_empty() || intent.destination.trim().is_empty() {
        return Err("regional dispatch requires a source hub and destination".to_owned());
    }
    if intent.resources.values().any(|quantity| *quantity <= 0)
        || intent.devices.iter().any(|request| request.quantity <= 0)
        || [
            intent.racing_vessels,
            intent.heaven_vessels,
            intent.cargo_vessels,
        ]
        .into_iter()
        .any(|quantity| quantity < 0)
    {
        return Err("regional dispatch quantities must be greater than zero".to_owned());
    }
    if intent.resources.is_empty()
        && intent.devices.is_empty()
        && desired_replicant_vessel_types(intent).is_empty()
    {
        return Err("regional dispatch must contain at least one payload".to_owned());
    }
    Ok(())
}

fn desired_replicant_vessel_types(intent: &RegionalDispatchIntent) -> Vec<String> {
    [
        ("racing_vessel", intent.racing_vessels),
        ("heaven_vessel", intent.heaven_vessels),
        ("cargo_vessel", intent.cargo_vessels),
    ]
    .into_iter()
    .flat_map(|(device_type, quantity)| {
        std::iter::repeat_n(
            device_type.to_owned(),
            usize::try_from(quantity).unwrap_or(0),
        )
    })
    .collect()
}

async fn dispatch_owned_device_snapshots(client: &Client) -> Result<Vec<Device>, String> {
    let handles = client
        .devices()
        .find()
        .owned()
        .collect()
        .await
        .map_err(string_error)?;
    let mut devices = Vec::with_capacity(handles.len());
    for handle in handles {
        devices.push(handle.snapshot().await.map_err(string_error)?);
    }
    devices.sort_by(|left, right| left.key.id.as_str().cmp(right.key.id.as_str()));
    Ok(devices)
}

fn device_at(device: &Device, location: &str) -> bool {
    device
        .location
        .as_ref()
        .is_some_and(|current| current.id.as_str().eq_ignore_ascii_case(location))
}

fn device_is_type(device: &Device, device_type: &str) -> bool {
    device
        .device_type
        .as_ref()
        .is_some_and(|kind| kind.as_str() == device_type)
}

fn regional_dispatch_source_location(
    source: &str,
    system: &str,
    devices: &[Device],
) -> Result<String, String> {
    let has_hub = devices.iter().any(|device| {
        device_is_type(device, DeviceType::SystemHub.as_str())
            && device
                .location
                .as_ref()
                .is_some_and(|location| designation_in_system(location.id.as_str(), system))
    });
    if !has_hub {
        return Err(format!(
            "regional dispatch source {source} resolves to {system}, which does not contain an owned System Hub"
        ));
    }

    let mut factory_locations = BTreeMap::<String, usize>::new();
    for device in devices
        .iter()
        .filter(|device| device_is_type(device, DeviceType::Autofactory.as_str()))
    {
        let Some(location) = device
            .location
            .as_ref()
            .map(|location| location.id.as_str())
        else {
            continue;
        };
        if designation_in_system(location, system) {
            *factory_locations.entry(location.to_owned()).or_default() += 1;
        }
    }
    if factory_locations.is_empty() {
        return Err(format!(
            "regional dispatch source {source} resolves to owned hub system {system}, but no account-owned Autofactory is available in that system"
        ));
    }
    if let Some((location, _)) = factory_locations
        .iter()
        .find(|(location, _)| location.eq_ignore_ascii_case(source))
    {
        return Ok(location.clone());
    }
    factory_locations
        .into_iter()
        .max_by(
            |(left_location, left_count), (right_location, right_count)| {
                left_count
                    .cmp(right_count)
                    .then_with(|| right_location.cmp(left_location))
            },
        )
        .map(|(location, _)| location)
        .ok_or_else(|| format!("no manufacturing location is available in hub system {system}"))
}

async fn resolve_regional_dispatch_source(client: &Client, source: &str) -> Result<String, String> {
    let system = resolve_location_system(client, source).await?;
    let devices = dispatch_owned_device_snapshots(client).await?;
    regional_dispatch_source_location(source, &system, &devices)
}

fn try_claim_dispatch_device(context: &WorkflowContext, code: &str) -> Result<bool, String> {
    match context.acquire_claim(ResourceKey::Device(code.to_owned())) {
        Ok(_) => Ok(true),
        Err(RepositoryError::ClaimConflict { .. }) => Ok(false),
        Err(error) => Err(string_error(error)),
    }
}

fn dispatch_status_is_payload_safe(device: &Device) -> bool {
    device.status.as_ref().is_some_and(|status| {
        matches!(
            status.as_str().to_ascii_lowercase().as_str(),
            "inactive"
                | "deactivated"
                | "idle"
                | "stowed"
                | "recalled"
                | "compacted"
                | "out_of_range"
                | "monitoring"
        )
    })
}

fn dispatch_top_level_device_is_free(device: &Device, source: &str) -> bool {
    device.access == AccessScope::Owned
        && device_at(device, source)
        && device.travel.is_none()
        && device.relationships.attached_to.is_none()
        && device.relationships.stowed_in.is_none()
        && device.relationships.controller.is_none()
        && device.relationships.assigned_replicant.is_none()
        && device.relationships.hosting_replicant.is_none()
        && !workflow_reserved(&device.tags)
}

fn dispatch_payload_device_is_free(device: &Device, source: &str) -> bool {
    dispatch_top_level_device_is_free(device, source)
        && dispatch_status_is_payload_safe(device)
        && device.relationships.attached_devices.is_empty()
        && device.relationships.stowed_devices.is_empty()
        && device.relationships.controlled_devices.is_empty()
}

fn dispatch_vessel_onboard_empty_matrix<'a>(
    vessel: &Device,
    devices: &'a [Device],
) -> Option<&'a Device> {
    if vessel.relationships.stowed_devices.len() != 1 {
        return None;
    }
    let matrix_key = vessel.relationships.stowed_devices.first()?;
    devices.iter().find(|candidate| {
        candidate.key == *matrix_key
            && device_is_type(candidate, "empty_replicant_matrix")
            && !workflow_reserved(&candidate.tags)
            && candidate.travel.is_none()
            && candidate.relationships.stowed_in.as_ref() == Some(&vessel.key)
            && candidate.relationships.attached_to.is_none()
            && candidate.relationships.controller.is_none()
            && candidate.relationships.hosting_replicant.is_none()
    })
}

fn dispatch_vessel_is_free(
    vessel: &Device,
    vessel_type: &str,
    source: &str,
    devices: &[Device],
) -> bool {
    if !dispatch_top_level_device_is_free(vessel, source)
        || !dispatch_status_is_payload_safe(vessel)
        || !device_is_type(vessel, vessel_type)
        || !vessel.cargo.is_empty()
        || !vessel.relationships.attached_devices.is_empty()
        || !vessel.relationships.controlled_devices.is_empty()
    {
        return false;
    }
    match vessel.relationships.stowed_devices.len() {
        0 => true,
        1 => dispatch_vessel_onboard_empty_matrix(vessel, devices).is_some(),
        _ => false,
    }
}

fn dispatch_loose_empty_matrix_is_free(device: &Device, source: &str) -> bool {
    dispatch_top_level_device_is_free(device, source)
        && device_is_type(device, "empty_replicant_matrix")
        && device.relationships.attached_devices.is_empty()
        && device.relationships.stowed_devices.is_empty()
        && device.relationships.controlled_devices.is_empty()
}

fn dispatch_transport_is_free(
    device: &Device,
    source: &str,
    used: &BTreeSet<String>,
    claimed_elsewhere: &BTreeSet<String>,
) -> bool {
    let code = device.key.id.as_str();
    dispatch_top_level_device_is_free(device, source)
        && !used.contains(code)
        && !claimed_elsewhere.contains(&code.to_ascii_uppercase())
        && device.relationships.attached_devices.is_empty()
        && device.relationships.stowed_devices.is_empty()
        && device.relationships.controlled_devices.is_empty()
        && device
            .available_commands
            .iter()
            .any(|command| command.as_str().eq_ignore_ascii_case("travel"))
}

fn regional_dispatch_has_device_payload(intent: &RegionalDispatchIntent) -> bool {
    !intent.devices.is_empty()
        || intent.racing_vessels > 0
        || intent.heaven_vessels > 0
        || intent.cargo_vessels > 0
}

async fn select_regional_dispatch_stock(
    context: &WorkflowContext,
    client: &Client,
    intent: &RegionalDispatchIntent,
    checkpoint: &mut RegionalDispatchCheckpoint,
) -> Result<(), String> {
    let devices = dispatch_owned_device_snapshots(client).await?;
    let mut used = BTreeSet::new();
    let claimed_elsewhere = context
        .repository()
        .device_claims()
        .map_err(string_error)?
        .into_iter()
        .filter(|claim| claim.workflow_id != context.id())
        .filter_map(|claim| match claim.resource {
            ResourceKey::Device(code) => Some(code.to_ascii_uppercase()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let desired_vessels = desired_replicant_vessel_types(intent);

    for vessel_type in &desired_vessels {
        let mut selected = None;
        let mut selected_matrix = None;

        // Prefer a vessel that already contains an empty matrix. That avoids an
        // unnecessary stow operation and, more importantly, avoids printing a
        // second matrix merely because the existing one is nested in the hull.
        for prefer_onboard_matrix in [true, false] {
            for vessel in &devices {
                let code = vessel.key.id.as_str();
                if used.contains(code)
                    || !dispatch_vessel_is_free(vessel, vessel_type, &intent.source, &devices)
                {
                    continue;
                }
                let onboard_matrix = dispatch_vessel_onboard_empty_matrix(vessel, &devices);
                if onboard_matrix.is_some() != prefer_onboard_matrix {
                    continue;
                }
                if !try_claim_dispatch_device(context, code)? {
                    continue;
                }
                if let Some(matrix) = onboard_matrix {
                    let matrix_code = matrix.key.id.as_str();
                    if used.contains(matrix_code)
                        || !try_claim_dispatch_device(context, matrix_code)?
                    {
                        context
                            .release_claim(&ResourceKey::Device(code.to_owned()))
                            .map_err(string_error)?;
                        continue;
                    }
                    used.insert(matrix_code.to_owned());
                    selected_matrix = Some(matrix_code.to_owned());
                }
                used.insert(code.to_owned());
                selected = Some(code.to_owned());
                break;
            }
            if selected.is_some() {
                break;
            }
        }

        checkpoint.vessels.push(selected.unwrap_or_default());
        checkpoint.matrices.push(selected_matrix);
    }

    // Loose empty matrices are useful even when the corresponding vessel must
    // still be printed. Reserve them now so the print deficit reflects the
    // complete stock at the source hub rather than only matrices already paired
    // with an existing hull.
    for matrix in &mut checkpoint.matrices {
        if matrix.is_some() {
            continue;
        }
        for candidate in &devices {
            let code = candidate.key.id.as_str();
            if used.contains(code)
                || !dispatch_loose_empty_matrix_is_free(candidate, &intent.source)
            {
                continue;
            }
            if try_claim_dispatch_device(context, code)? {
                used.insert(code.to_owned());
                *matrix = Some(code.to_owned());
                break;
            }
        }
    }

    let mut requested_devices = BTreeMap::<String, i64>::new();
    for request in &intent.devices {
        *requested_devices
            .entry(request.device_type.clone())
            .or_default() += request.quantity;
    }
    for (device_type, quantity) in requested_devices {
        let mut found = 0_i64;
        for device in &devices {
            let code = device.key.id.as_str();
            if found >= quantity {
                break;
            }
            if used.contains(code)
                || !device_is_type(device, &device_type)
                || !dispatch_payload_device_is_free(device, &intent.source)
            {
                continue;
            }
            if try_claim_dispatch_device(context, code)? {
                checkpoint.devices.push(code.to_owned());
                used.insert(code.to_owned());
                found += 1;
            }
        }
    }

    let mut deficits = BTreeMap::<String, i64>::new();
    let mut selected_by_type = BTreeMap::<String, i64>::new();
    for vessel in &checkpoint.vessels {
        if let Some(device_type) = devices
            .iter()
            .find(|device| device.key.id.as_str() == vessel)
            .and_then(|device| device.device_type.as_ref())
        {
            *selected_by_type
                .entry(device_type.as_str().to_owned())
                .or_default() += 1;
        }
    }
    for device_type in desired_vessels {
        let selected = selected_by_type.entry(device_type.clone()).or_default();
        if *selected > 0 {
            *selected -= 1;
        } else {
            *deficits.entry(device_type).or_default() += 1;
        }
    }
    let missing_matrices = i64::try_from(
        checkpoint
            .matrices
            .iter()
            .filter(|matrix| matrix.is_none())
            .count(),
    )
    .unwrap_or(i64::MAX);
    if missing_matrices > 0 {
        deficits.insert("empty_replicant_matrix".to_owned(), missing_matrices);
    }
    let selected_payload_types = checkpoint
        .devices
        .iter()
        .filter_map(|code| {
            devices
                .iter()
                .find(|device| device.key.id.as_str() == code)
                .and_then(|device| device.device_type.as_ref())
                .map(|kind| kind.as_str().to_owned())
        })
        .fold(BTreeMap::<String, i64>::new(), |mut counts, kind| {
            *counts.entry(kind).or_default() += 1;
            counts
        });
    let mut desired_payload_types = BTreeMap::<String, i64>::new();
    for request in &intent.devices {
        *desired_payload_types
            .entry(request.device_type.clone())
            .or_default() += request.quantity;
    }
    for (device_type, desired) in desired_payload_types {
        let selected = selected_payload_types
            .get(&device_type)
            .copied()
            .unwrap_or(0);
        let missing = desired.saturating_sub(selected);
        if missing > 0 {
            *deficits.entry(device_type).or_default() += missing;
        }
    }

    // Transport itself is provisioning stock too. Only count a carrier that is
    // actually free at the source; a reserved, nested, travelling, loaded, or
    // occupied hull must not suppress the print deficit.
    let has_cargo_transport = devices.iter().any(|device| {
        dispatch_transport_is_free(device, &intent.source, &used, &claimed_elsewhere)
            && device.cargo_capacity.unwrap_or(0) > 0
            && device.cargo.is_empty()
    });
    if !intent.resources.is_empty() && !has_cargo_transport {
        *deficits.entry("cargo_freighter".to_owned()).or_default() += 1;
    }
    let has_device_carrier = devices.iter().any(|device| {
        dispatch_transport_is_free(device, &intent.source, &used, &claimed_elsewhere)
            && device.attach_capacity.unwrap_or(0) > 0
    });
    if regional_dispatch_has_device_payload(intent) && !has_device_carrier {
        *deficits.entry("surge_carrier".to_owned()).or_default() += 1;
    }
    checkpoint.print_requests = deficits
        .into_iter()
        .filter(|(_, quantity)| *quantity > 0)
        .map(|(device_type, quantity)| PrintRequest::new(device_type, quantity))
        .collect();
    Ok(())
}

fn regional_dispatch_deficit_message(requests: &[PrintRequest]) -> String {
    if requests.is_empty() {
        return "regional dispatch missing 0 devices; existing unclaimed stock satisfies the manifest"
            .to_owned();
    }
    let missing = requests.iter().map(|request| request.quantity).sum::<i64>();
    let details = requests
        .iter()
        .map(|request| format!("{} {}", request.quantity, request.device_type))
        .collect::<Vec<_>>()
        .join(", ");
    format!("regional dispatch missing {missing} devices; printing {details}")
}

async fn manufacture_regional_dispatch(
    context: &mut WorkflowContext,
    client: &Client,
    intent: &RegionalDispatchIntent,
    checkpoint: &mut RegionalDispatchCheckpoint,
) -> Result<bool, String> {
    if !checkpoint.print_requests.is_empty() {
        let mut options = QueueOptions::at(intent.source.clone());
        options.tags = vec![checkpoint.print_tag.clone()];
        options.wait_timeout = Duration::from_secs(DEFAULT_WAIT_SECONDS);
        queue_prints_with_components(client, &checkpoint.print_requests, &options)
            .await
            .map_err(string_error)?;
        loop {
            let status = printing_status_in_system(
                client,
                &intent.source,
                &checkpoint.print_requests,
                std::slice::from_ref(&checkpoint.print_tag),
            )
            .await
            .map_err(string_error)?;
            if status
                .requested
                .iter()
                .all(|line| line.available >= line.required)
            {
                break;
            }
            match context.control_request().map_err(string_error)? {
                ControlRequest::Continue => {}
                ControlRequest::Pause | ControlRequest::Cancel => return Ok(false),
            }
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }

    let tagged = tagged_devices(client, &checkpoint.print_tag).await?;
    let mut unused = tagged.into_iter().collect::<BTreeMap<_, _>>();
    for code in checkpoint
        .vessels
        .iter()
        .chain(checkpoint.devices.iter())
        .chain(checkpoint.matrices.iter().flatten())
    {
        unused.remove(code);
    }
    let desired_vessels = desired_replicant_vessel_types(intent);
    for (index, device_type) in desired_vessels.iter().enumerate() {
        if !checkpoint.vessels[index].is_empty() {
            continue;
        }
        let code = take_tagged_device(&mut unused, device_type)
            .ok_or_else(|| format!("printed {device_type} was not found by dispatch tag"))?;
        claim_device(context, &code)?;
        checkpoint.vessels[index] = code;
    }
    for matrix in &mut checkpoint.matrices {
        if matrix.is_some() {
            continue;
        }
        let code = take_tagged_device(&mut unused, "empty_replicant_matrix").ok_or_else(|| {
            "printed empty Replicant matrix was not found by dispatch tag".to_owned()
        })?;
        claim_device(context, &code)?;
        *matrix = Some(code);
    }
    let mut required_devices = BTreeMap::<String, i64>::new();
    for request in &intent.devices {
        *required_devices
            .entry(request.device_type.clone())
            .or_default() += request.quantity;
    }
    let existing_types = dispatch_owned_device_snapshots(client)
        .await?
        .into_iter()
        .filter(|device| {
            checkpoint
                .devices
                .iter()
                .any(|code| code == device.key.id.as_str())
        })
        .filter_map(|device| device.device_type.map(|kind| kind.as_str().to_owned()))
        .fold(BTreeMap::<String, i64>::new(), |mut counts, kind| {
            *counts.entry(kind).or_default() += 1;
            counts
        });
    for (device_type, required) in required_devices {
        let missing =
            required.saturating_sub(existing_types.get(&device_type).copied().unwrap_or(0));
        for _ in 0..missing {
            let code = take_tagged_device(&mut unused, &device_type)
                .ok_or_else(|| format!("printed {device_type} was not found by dispatch tag"))?;
            claim_device(context, &code)?;
            checkpoint.devices.push(code);
        }
    }
    Ok(true)
}

fn take_tagged_device(devices: &mut BTreeMap<String, String>, device_type: &str) -> Option<String> {
    let code = devices
        .iter()
        .find_map(|(code, kind)| (kind == device_type).then(|| code.clone()))?;
    devices.remove(&code);
    Some(code)
}

async fn stow_dispatch_matrices(
    context: &mut WorkflowContext,
    client: &Client,
    checkpoint: &mut RegionalDispatchCheckpoint,
) -> Result<(), String> {
    for (vessel, matrix) in checkpoint.vessels.iter().zip(&checkpoint.matrices) {
        let matrix = matrix
            .as_ref()
            .ok_or_else(|| format!("vessel {vessel} has no selected empty matrix"))?;
        if checkpoint.stowed_matrices.contains(matrix) {
            continue;
        }
        let snapshot = client
            .devices()
            .get(matrix)
            .await
            .map_err(string_error)?
            .snapshot()
            .await
            .map_err(string_error)?;
        if snapshot
            .relationships
            .stowed_in
            .as_ref()
            .is_some_and(|parent| parent.id.as_str() == vessel)
        {
            checkpoint.stowed_matrices.insert(matrix.clone());
            context
                .persist_checkpoint(checkpoint)
                .map_err(string_error)?;
            continue;
        }
        if let Some(parent) = snapshot.relationships.stowed_in.as_ref() {
            return Err(format!(
                "empty matrix {matrix} is already stowed in {}, not selected vessel {vessel}",
                parent.id.as_str()
            ));
        }
        context
            .advance_to("stowing_matrices", checkpoint)
            .map_err(string_error)?;
        let operation = client
            .devices()
            .get(matrix)
            .await
            .map_err(string_error)?
            .command(replicant_client::raw::devices::DeviceCommand::Stow {
                target: Some(vessel.clone()),
            })
            .await
            .map_err(string_error)?;
        await_success(&operation).await?;
        checkpoint.stowed_matrices.insert(matrix.clone());
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
    }
    Ok(())
}

async fn replicate_dispatch_matrices(
    context: &mut WorkflowContext,
    client: &Client,
    intent: &RegionalDispatchIntent,
    checkpoint: &mut RegionalDispatchCheckpoint,
) -> Result<(), String> {
    let Some(source_matrix) = claim_dispatch_source_matrix(context, client, &intent.source).await?
    else {
        context
            .emit_activity(
                "no unclaimed Replicant matrix is available at the source hub; dispatching requested vessels with empty matrices",
            )
            .map_err(string_error)?;
        return Ok(());
    };
    for matrix in checkpoint.matrices.iter().flatten() {
        if checkpoint.replicated_matrices.contains(matrix) {
            continue;
        }
        context
            .advance_to("replicating", checkpoint)
            .map_err(string_error)?;
        let operation = client
            .devices()
            .get(&source_matrix)
            .await
            .map_err(string_error)?
            .command(replicant_client::raw::devices::DeviceCommand::Replicate {
                target: matrix.clone(),
                name: None,
            })
            .await
            .map_err(string_error)?;
        await_success(&operation).await?;
        checkpoint.replicated_matrices.insert(matrix.clone());
        context
            .persist_checkpoint(checkpoint)
            .map_err(string_error)?;
    }
    Ok(())
}

async fn claim_dispatch_source_matrix(
    context: &WorkflowContext,
    client: &Client,
    source: &str,
) -> Result<Option<String>, String> {
    let devices = dispatch_owned_device_snapshots(client).await?;
    for vessel in &devices {
        let Some(replicant) = vessel.relationships.hosting_replicant.as_ref() else {
            continue;
        };
        if !device_at(vessel, source) || vessel.travel.is_some() || workflow_reserved(&vessel.tags)
        {
            continue;
        }
        let Some(matrix) = devices.iter().find(|candidate| {
            device_is_type(candidate, "replicant_matrix")
                && !workflow_reserved(&candidate.tags)
                && candidate.travel.is_none()
                && candidate
                    .relationships
                    .stowed_in
                    .as_ref()
                    .is_some_and(|parent| parent == &vessel.key)
        }) else {
            continue;
        };
        let resources = [
            ResourceKey::Device(vessel.key.id.as_str().to_owned()),
            ResourceKey::Device(matrix.key.id.as_str().to_owned()),
            ResourceKey::Replicant(replicant.id.as_str().to_owned()),
        ];
        let mut acquired = Vec::new();
        let mut conflict = false;
        for resource in resources {
            match context.acquire_claim(resource.clone()) {
                Ok(ClaimAcquireOutcome::Acquired(_)) => acquired.push(resource),
                Ok(ClaimAcquireOutcome::AlreadyOwned(_)) => {}
                Err(RepositoryError::ClaimConflict { .. }) => {
                    conflict = true;
                    break;
                }
                Err(error) => return Err(string_error(error)),
            }
        }
        if conflict {
            for resource in acquired {
                context.release_claim(&resource).map_err(string_error)?;
            }
            continue;
        }
        return Ok(Some(matrix.key.id.as_str().to_owned()));
    }
    Ok(None)
}

fn regional_dispatch_delivery_request(
    intent: &RegionalDispatchIntent,
    checkpoint: &RegionalDispatchCheckpoint,
) -> Result<DeliveryRequest, String> {
    if checkpoint.vessels.len() != checkpoint.matrices.len()
        || checkpoint.matrices.iter().any(Option::is_none)
    {
        return Err("regional dispatch vessel/matrix manifest is incomplete".to_owned());
    }
    Ok(DeliveryRequest {
        origin: intent.source.clone(),
        destination: intent.destination.clone(),
        resources: intent.resources.clone(),
        devices: Vec::new(),
        device_codes: checkpoint
            .vessels
            .iter()
            .chain(checkpoint.devices.iter())
            .cloned()
            .collect(),
        device_tags: Vec::new(),
        carrier: None,
        allow_transport_staging: false,
    })
}

fn manifest_delivery_request(intent: &LogisticsManifestIntent) -> DeliveryRequest {
    DeliveryRequest {
        origin: intent.origin.clone(),
        destination: intent.destination.clone(),
        resources: intent.resources.clone(),
        devices: intent.devices.clone(),
        device_codes: intent.device_codes.clone(),
        device_tags: intent.device_tags.clone(),
        carrier: None,
        allow_transport_staging: intent.allow_transport_staging,
    }
}

async fn resolve_location_system(client: &Client, location: &str) -> Result<String, String> {
    let mut catalogue = client.galaxy().catalogue();
    if catalogue.is_empty() {
        client
            .galaxy()
            .refresh_catalogue()
            .await
            .map_err(string_error)?;
        catalogue = client.galaxy().catalogue();
    }
    catalogue
        .iter()
        .map(|star| star.key.id.as_str())
        .filter(|system| designation_in_system(location, system))
        .max_by_key(|system| system.len())
        .map(str::to_owned)
        .ok_or_else(|| format!("{location} does not resolve to a known system"))
}

fn designation_in_system(location: &str, system: &str) -> bool {
    location == system || location.starts_with(&format!("{system}-"))
}
async fn tagged_devices(client: &Client, tag: &str) -> Result<Vec<(String, String)>, String> {
    let handles = client
        .devices()
        .find()
        .owned()
        .with_tag(tag.to_owned())
        .collect()
        .await
        .map_err(string_error)?;
    let mut devices = Vec::new();
    for handle in handles {
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if let Some(kind) = snapshot.device_type.as_ref() {
            devices.push((handle.id().as_str().to_owned(), kind.as_str().to_owned()));
        }
    }
    devices.sort();
    Ok(devices)
}

async fn source_matrix_for_replicant(client: &Client, replicant: &str) -> Result<String, String> {
    let snapshot = client
        .replicants()
        .get_owned(replicant)
        .await
        .map_err(string_error)?
        .snapshot()
        .await
        .map_err(string_error)?;
    let host = snapshot
        .hosted_device
        .as_ref()
        .ok_or_else(|| format!("Replicant {replicant} has no hosted cradle device"))?;
    let handles = client
        .devices()
        .find()
        .owned()
        .collect()
        .await
        .map_err(string_error)?;
    for handle in handles {
        let device = handle.snapshot().await.map_err(string_error)?;
        if device
            .device_type
            .as_ref()
            .is_some_and(|kind| kind.as_str() == "replicant_matrix")
            && device.relationships.stowed_in.as_ref() == Some(host)
        {
            return Ok(handle.id().as_str().to_owned());
        }
    }
    Err(format!("could not locate Replicant matrix for {replicant}"))
}

fn default_worker_cradle() -> String {
    "racing_vessel".to_owned()
}

fn default_mining_concurrency() -> usize {
    4
}

fn scratch_file(workflow_id: WorkflowId, name: &str) -> Result<PathBuf, String> {
    let directory = std::env::temp_dir()
        .join("replicant-client-automation")
        .join(workflow_id.to_string());
    fs::create_dir_all(&directory).map_err(string_error)?;
    Ok(directory.join(name))
}

fn materialize_json(path: &Path, contents: Option<&str>) -> Result<(), String> {
    let Some(contents) = contents else {
        return clear_scratch_file(path);
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(string_error)?;
    }
    fs::write(path, contents).map_err(string_error)
}

fn clear_scratch_file(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn read_json(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(string_error)
}
fn event_connectivity_retry_deadline(
    repository: &replicant_workflow::WorkflowRepository,
    dependencies: &BTreeMap<String, WorkflowId>,
) -> Result<Option<i64>, RepositoryError> {
    let cooldown_ms =
        i64::try_from(EVENT_CONNECTIVITY_RETRY_COOLDOWN.as_millis()).unwrap_or(i64::MAX);
    dependencies
        .values()
        .try_fold(None, |earliest: Option<i64>, workflow_id| {
            let deadline = repository
                .read(*workflow_id)?
                .filter(|workflow| workflow.status == WorkflowStatus::Failed)
                .map(|workflow| workflow.updated_at.saturating_add(cooldown_ms));
            Ok(match (earliest, deadline) {
                (Some(earliest), Some(deadline)) => Some(earliest.min(deadline)),
                (None, Some(deadline)) => Some(deadline),
                (earliest, None) => earliest,
            })
        })
}

pub(crate) fn campaign_retry_deadline(
    repository: &replicant_workflow::WorkflowRepository,
    workflow_id: WorkflowId,
    fallback_deadline_ms: i64,
) -> Result<i64, RepositoryError> {
    Ok(repository
        .list_work_items(workflow_id)?
        .into_iter()
        .filter(|item| {
            matches!(
                item.state.status,
                WorkItemStatus::Pending | WorkItemStatus::Waiting
            )
        })
        .filter_map(|item| item.state.next_attempt_at_ms)
        .min()
        .unwrap_or(fallback_deadline_ms))
}

fn campaign_wait_intent(
    description: &str,
    event_names: &[&str],
    deadline_ms: Option<i64>,
    poll_interval: Duration,
) -> WaitIntent {
    let intent = WaitIntent::state(description)
        .for_events(event_names.iter().map(|name| (*name).to_owned()))
        .polling_every(poll_interval);
    match deadline_ms {
        Some(deadline_ms) => intent.until(deadline_ms),
        None => intent,
    }
}

fn campaign_wait_signal_is_actionable(signal: WaitSignal) -> bool {
    matches!(
        signal,
        WaitSignal::History
            | WaitSignal::Event
            | WaitSignal::StateRevision
            | WaitSignal::Poll
            | WaitSignal::WatcherGap
    )
}

pub(crate) async fn wait_for_campaign_work(
    context: &mut WorkflowContext,
    description: &str,
    event_names: &[&str],
    deadline_ms: Option<i64>,
    poll_interval: Duration,
) -> Result<bool, String> {
    let intent = campaign_wait_intent(description, event_names, deadline_ms, poll_interval);
    let outcome = context
        .wait_until(intent, |_client, signal| {
            std::future::ready(Ok(campaign_wait_signal_is_actionable(signal)))
        })
        .await
        .map_err(string_error)?;
    Ok(matches!(
        outcome,
        WaitOutcome::Satisfied | WaitOutcome::Deadline
    ))
}

fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::io;

    use replicant_client::{
        SecretString, StartupPolicy,
        domain::{DeviceId, DeviceKey, DeviceRelationships, DeviceStatus, LocationId, LocationKey},
        raw::Url,
    };
    use replicant_workflow::WorkflowRepository;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param, query_param_is_missing},
    };

    use super::*;
    use crate::failure::ClassifiedError;

    fn provision_fixture(tag: &str) -> ReplicantProvisionCheckpoint {
        ReplicantProvisionCheckpoint {
            tag: Some(tag.to_owned()),
            manufacturing: Some(new_provision_manufacturing(tag, "racing_vessel")),
            ..ReplicantProvisionCheckpoint::default()
        }
    }

    #[test]
    fn provision_legacy_checkpoint_initializes_durable_intents_without_losing_tag() {
        let mut checkpoint: ReplicantProvisionCheckpoint =
            serde_json::from_value(serde_json::json!({
                "tag": "dir-p:legacy",
                "matrix": null,
                "cradle": null,
                "stowed": false,
                "new_replicant": null
            }))
            .expect("legacy checkpoint");
        assert!(checkpoint.manufacturing.is_none());
        checkpoint.manufacturing = Some(new_provision_manufacturing(
            checkpoint.tag.as_deref().expect("tag"),
            "racing_vessel",
        ));
        assert_eq!(checkpoint.tag.as_deref(), Some("dir-p:legacy"));
        assert_eq!(
            provision_pending_roles(&checkpoint),
            vec![ProvisionPrintRole::Matrix, ProvisionPrintRole::Cradle]
        );
    }

    #[test]
    fn provision_full_workflow_ids_prevent_short_prefix_tag_collisions() {
        let left = "b6342fb5-0000-4000-8000-000000000001"
            .parse::<WorkflowId>()
            .expect("left workflow id");
        let right = "b6342fb5-0000-4000-8000-000000000002"
            .parse::<WorkflowId>()
            .expect("right workflow id");
        let left_tag = provision_workflow_tag(left);
        let right_tag = provision_workflow_tag(right);
        assert_ne!(left_tag, right_tag);
        let intents = new_provision_manufacturing(&left_tag, "racing_vessel");
        assert!(left_tag.len() <= 32);
        assert!(intents.matrix.job_tag.len() <= 32);
        assert!(intents.cradle.job_tag.len() <= 32);
    }

    #[test]
    fn provision_legacy_checkpoint_retains_already_adopted_output() {
        let mut checkpoint: ReplicantProvisionCheckpoint =
            serde_json::from_value(serde_json::json!({
                "tag": "dir-p:legacy-partial",
                "matrix": "M-EXISTING",
                "cradle": null,
                "stowed": false,
                "new_replicant": null
            }))
            .expect("legacy checkpoint");
        checkpoint.manufacturing = Some(new_provision_manufacturing(
            checkpoint.tag.as_deref().expect("tag"),
            "racing_vessel",
        ));
        assert_eq!(checkpoint.matrix.as_deref(), Some("M-EXISTING"));
        assert_eq!(
            provision_pending_roles(&checkpoint),
            vec![ProvisionPrintRole::Cradle]
        );
    }

    fn provision_device(code: &str, device_type: &str, tag: &str) -> ProvisionTaggedDevice {
        ProvisionTaggedDevice {
            code: code.to_owned(),
            device_type: device_type.to_owned(),
            tags: vec![tag.to_owned()],
        }
    }

    fn provision_status(jobs: &[(&str, &str, &str)]) -> SystemPrintingStatus {
        let mut factories = BTreeMap::<String, FactoryPrintStatus>::new();
        for (factory_code, device_type, tag) in jobs {
            let factory = factories
                .entry((*factory_code).to_owned())
                .or_insert_with(|| FactoryPrintStatus {
                    code: (*factory_code).to_owned(),
                    ..FactoryPrintStatus::default()
                });
            factory
                .queued
                .push(replicant_printing::managed::FactoryPrintJobStatus {
                    device_type: (*device_type).to_owned(),
                    quantity: 1,
                    tags: vec![(*tag).to_owned()],
                    matches_filter: true,
                    ..replicant_printing::managed::FactoryPrintJobStatus::default()
                });
        }
        SystemPrintingStatus {
            factories: factories.into_values().collect(),
            ..SystemPrintingStatus::default()
        }
    }

    #[test]
    fn provision_first_queue_pass_records_one_submission_per_output() {
        let tag = "dir-p:first";
        let mut checkpoint = provision_fixture(tag);
        let roles = provision_pending_roles(&checkpoint);
        let requests = provision_tracked_requests(&checkpoint, &roles).expect("requests");
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].device_type, "empty_replicant_matrix");
        assert_eq!(requests[0].quantity, 1);
        assert_eq!(requests[1].device_type, "racing_vessel");
        assert_eq!(requests[1].quantity, 1);

        for (request_index, request) in requests.iter().enumerate() {
            let assignment = TrackedPrintAssignment {
                request_index,
                factory_code: "F-1".into(),
                device_type: request.device_type.clone(),
                quantity: request.quantity,
                flatpack: request.flatpack,
            };
            let tags = apply_provision_print_update(
                &mut checkpoint,
                &roles,
                tag,
                TrackedPrintUpdate::Preparing(assignment.clone()),
            )
            .expect("persist preparing")
            .expect("submission tags");
            assert!(tags.iter().any(|candidate| candidate == tag));
            apply_provision_print_update(
                &mut checkpoint,
                &roles,
                tag,
                TrackedPrintUpdate::OperationRecorded {
                    assignment,
                    operation_id: format!("op-{request_index}"),
                },
            )
            .expect("persist operation");
        }

        let manufacturing = checkpoint.manufacturing.as_ref().expect("manufacturing");
        assert_eq!(manufacturing.matrix.operation_id.as_deref(), Some("op-0"));
        assert_eq!(manufacturing.cradle.operation_id.as_deref(), Some("op-1"));
        assert!(provision_pending_roles(&checkpoint).is_empty());
    }

    #[tokio::test]
    async fn provision_first_queue_pass_sends_exactly_two_enqueue_requests() {
        let server = MockServer::start().await;
        let factory_code = "PROVISION-FIRST-F";
        Mock::given(method("GET"))
            .and(path(format!("/v1/devices/{factory_code}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": factory_code,
                "device_type": "autofactory",
                "location": "PROVISION-HOME",
                "status": "idle",
                "queue_size": 4,
                "print_queue": [],
                "available_commands": ["enqueue_print"]
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/v1/devices/{factory_code}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "printing"
            })))
            .expect(2)
            .mount(&server)
            .await;

        let client = test_client_at(&server).await;
        client
            .devices()
            .get(factory_code)
            .await
            .expect("seed Autofactory");
        let tag = "dir-p:first-boundary";
        let mut checkpoint = provision_fixture(tag);
        let roles = provision_pending_roles(&checkpoint);
        let requests = provision_tracked_requests(&checkpoint, &roles).expect("requests");
        let blueprints = [
            (
                "empty_replicant_matrix".to_owned(),
                replicant_printing::Blueprint {
                    device_type: "empty_replicant_matrix".to_owned(),
                    print_time_seconds: 10.0,
                    features: Vec::new(),
                    components: BTreeMap::new(),
                },
            ),
            (
                "racing_vessel".to_owned(),
                replicant_printing::Blueprint {
                    device_type: "racing_vessel".to_owned(),
                    print_time_seconds: 20.0,
                    features: Vec::new(),
                    components: BTreeMap::new(),
                },
            ),
        ]
        .into_iter()
        .collect();
        let report = queue_tracked_prints_once(
            &client,
            &requests,
            &QueueOptions::at("PROVISION-HOME"),
            &blueprints,
            |update| apply_provision_print_update(&mut checkpoint, &roles, tag, update),
        )
        .await
        .expect("queue both provisioning outputs");

        assert_eq!(report.submissions.len(), 2);
        assert!(provision_pending_roles(&checkpoint).is_empty());
        server.verify().await;
        client.close().await.expect("close client");
    }
    #[test]
    fn provision_restart_adopts_both_queued_prints_without_duplicates() {
        let tag = "dir-p:restart";
        let mut checkpoint = provision_fixture(tag);
        let report = reconcile_provision_evidence(
            &mut checkpoint,
            tag,
            &[],
            &provision_status(&[
                ("F-1", "empty_replicant_matrix", tag),
                ("F-1", "racing_vessel", tag),
            ]),
        )
        .expect("reconcile");
        assert_eq!(report.in_flight, 2);
        assert!(provision_pending_roles(&checkpoint).is_empty());
    }

    #[test]
    fn provision_accepted_operations_block_resubmission_without_devices() {
        let mut checkpoint = provision_fixture("dir-p:accepted");
        let manufacturing = checkpoint.manufacturing.as_mut().expect("manufacturing");
        manufacturing.matrix.factory_code = Some("F-1".into());
        manufacturing.matrix.submission_started = true;
        manufacturing.matrix.operation_id = Some("op-matrix".into());
        manufacturing.cradle.factory_code = Some("F-1".into());
        manufacturing.cradle.submission_started = true;
        manufacturing.cradle.operation_id = Some("op-vessel".into());
        assert!(apply_provision_operation_status(
            &mut manufacturing.matrix,
            OperationStatus::Accepted,
            false
        ));
        assert!(apply_provision_operation_status(
            &mut manufacturing.cradle,
            OperationStatus::AwaitingEvidence,
            false
        ));
        assert!(manufacturing.matrix.accepted);
        assert!(manufacturing.cradle.accepted);
        assert!(provision_pending_roles(&checkpoint).is_empty());
    }

    #[test]
    fn provision_only_definitive_rejection_reopens_print_deficit() {
        for status in [
            OperationStatus::Cancelled,
            OperationStatus::Failed,
            OperationStatus::Ambiguous,
            OperationStatus::Submitted,
            OperationStatus::ReconciliationRequired,
        ] {
            let mut print = new_provision_manufacturing("dir-p:status", "racing_vessel").matrix;
            print.factory_code = Some("F-1".into());
            print.submission_started = true;
            print.operation_id = Some("op-matrix".into());
            assert!(!apply_provision_operation_status(&mut print, status, false));
            assert!(print.submission_started, "{status:?}");
            assert_eq!(
                print.operation_id.as_deref(),
                Some("op-matrix"),
                "{status:?}"
            );
        }

        let mut rejected = new_provision_manufacturing("dir-p:rejected", "racing_vessel").matrix;
        rejected.factory_code = Some("F-1".into());
        rejected.submission_started = true;
        rejected.operation_id = Some("op-rejected".into());
        assert!(apply_provision_operation_status(
            &mut rejected,
            OperationStatus::Rejected,
            false
        ));
        assert!(!rejected.submission_started);
        assert!(rejected.operation_id.is_none());
    }

    #[test]
    fn provision_partial_completion_waits_only_for_other_output() {
        let tag = "dir-p:partial";
        let mut checkpoint = provision_fixture(tag);
        let report = reconcile_provision_evidence(
            &mut checkpoint,
            tag,
            &[provision_device("M-1", "empty_replicant_matrix", tag)],
            &provision_status(&[("F-1", "racing_vessel", tag)]),
        )
        .expect("reconcile");
        assert_eq!(checkpoint.matrix.as_deref(), Some("M-1"));
        assert!(checkpoint.cradle.is_none());
        assert_eq!(report.completed, 1);
        assert_eq!(report.in_flight, 1);
        assert!(provision_pending_roles(&checkpoint).is_empty());
    }

    #[test]
    fn provision_completed_outputs_are_adopted_and_ready_to_advance() {
        let tag = "dir-p:complete";
        let mut checkpoint = provision_fixture(tag);
        let report = reconcile_provision_evidence(
            &mut checkpoint,
            tag,
            &[
                provision_device("M-1", "empty_replicant_matrix", tag),
                provision_device("V-1", "racing_vessel", tag),
            ],
            &SystemPrintingStatus::default(),
        )
        .expect("reconcile");
        assert_eq!(report.completed, 2);
        assert_eq!(checkpoint.matrix.as_deref(), Some("M-1"));
        assert_eq!(checkpoint.cradle.as_deref(), Some("V-1"));
    }

    #[test]
    fn provision_duplicate_outputs_adopt_lexicographically_first_without_printing() {
        let tag = "dir-p:duplicate";
        let mut checkpoint = provision_fixture(tag);
        let report = reconcile_provision_evidence(
            &mut checkpoint,
            tag,
            &[
                provision_device("M-9", "empty_replicant_matrix", tag),
                provision_device("M-1", "empty_replicant_matrix", tag),
                provision_device("V-8", "racing_vessel", tag),
                provision_device("V-2", "racing_vessel", tag),
            ],
            &SystemPrintingStatus::default(),
        )
        .expect("reconcile");
        assert_eq!(checkpoint.matrix.as_deref(), Some("M-1"));
        assert_eq!(checkpoint.cradle.as_deref(), Some("V-2"));
        assert_eq!(report.duplicate_outputs, 2);
        assert!(provision_pending_roles(&checkpoint).is_empty());
    }

    #[test]
    fn provision_cancelled_workflow_retains_accepted_print_intents() {
        let mut checkpoint = provision_fixture("dir-p:cancelled");
        let manufacturing = checkpoint.manufacturing.as_mut().expect("manufacturing");
        manufacturing.matrix.submission_started = true;
        manufacturing.matrix.accepted = true;
        manufacturing.matrix.operation_id = Some("op-matrix".into());
        manufacturing.cradle.submission_started = true;
        manufacturing.cradle.accepted = true;
        manufacturing.cradle.operation_id = Some("op-vessel".into());
        let json = serde_json::to_value(&checkpoint).expect("serialize terminal checkpoint");
        let mut recovered: ReplicantProvisionCheckpoint =
            serde_json::from_value(json).expect("recover terminal checkpoint");
        let report = reconcile_provision_evidence(
            &mut recovered,
            "dir-p:cancelled",
            &[],
            &provision_status(&[
                ("F-1", "empty_replicant_matrix", "dir-p:cancelled"),
                ("F-1", "racing_vessel", "dir-p:cancelled"),
            ]),
        )
        .expect("reconcile accepted orphan work");

        assert_eq!(report.in_flight, 2);
        assert!(provision_pending_roles(&recovered).is_empty());
        assert_eq!(
            recovered
                .manufacturing
                .as_ref()
                .expect("manufacturing")
                .matrix
                .operation_id
                .as_deref(),
            Some("op-matrix")
        );
    }

    #[test]
    fn provision_workflows_do_not_adopt_foreign_tagged_outputs() {
        let own_tag = "dir-p:own";
        let mut checkpoint = provision_fixture(own_tag);
        let report = reconcile_provision_evidence(
            &mut checkpoint,
            own_tag,
            &[
                provision_device("M-FOREIGN", "empty_replicant_matrix", "dir-p:other"),
                provision_device("V-FOREIGN", "racing_vessel", "dir-p:other"),
                provision_device("M-OWN", "empty_replicant_matrix", own_tag),
            ],
            &SystemPrintingStatus::default(),
        )
        .expect("reconcile");
        assert_eq!(checkpoint.matrix.as_deref(), Some("M-OWN"));
        assert!(checkpoint.cradle.is_none());
        assert_eq!(report.completed, 1);
        assert_eq!(
            provision_pending_roles(&checkpoint),
            vec![ProvisionPrintRole::Cradle]
        );
    }

    #[test]
    fn provision_pre_operation_crash_is_ambiguous_and_never_resubmitted() {
        let mut checkpoint = provision_fixture("dir-p:crash");
        {
            let manufacturing = checkpoint.manufacturing.as_mut().expect("manufacturing");
            manufacturing.matrix.factory_code = Some("F-1".into());
            manufacturing.matrix.submission_started = true;
        }
        let json = serde_json::to_value(&checkpoint).expect("persist preparing checkpoint");
        let recovered: ReplicantProvisionCheckpoint =
            serde_json::from_value(json).expect("recover preparing checkpoint");
        assert_eq!(
            provision_pending_roles(&recovered),
            vec![ProvisionPrintRole::Cradle]
        );
        let matrix = &recovered
            .manufacturing
            .as_ref()
            .expect("manufacturing")
            .matrix;
        assert!(!matrix.accepted);
        assert!(matrix.operation_id.is_none());
    }

    struct FixtureEventItemExecutor {
        calls: std::sync::atomic::AtomicUsize,
        active: std::sync::atomic::AtomicUsize,
        peak: std::sync::atomic::AtomicUsize,
        missing_injected: std::sync::atomic::AtomicBool,
        first_wave: tokio::sync::Barrier,
        order: std::sync::Mutex<Vec<EventItemStage>>,
    }

    impl FixtureEventItemExecutor {
        fn new() -> Self {
            Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                active: std::sync::atomic::AtomicUsize::new(0),
                peak: std::sync::atomic::AtomicUsize::new(0),
                missing_injected: std::sync::atomic::AtomicBool::new(false),
                first_wave: tokio::sync::Barrier::new(2),
                order: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    impl EventItemExecutor for FixtureEventItemExecutor {
        fn execute<'a>(
            &'a self,
            _client: &'a Client,
            mission_json: &'a str,
            stage: EventItemStage,
            allocations: &'a AllocationSet,
            _wait_timeout: Duration,
        ) -> EventItemFuture<'a> {
            let missing_allocation = allocations
                .by_requirement
                .get("device:survey_drone")
                .and_then(|allocations| allocations.first())
                .map(|allocation| allocation.id);
            Box::pin(async move {
                let call = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                let active = self
                    .active
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    + 1;
                self.peak
                    .fetch_max(active, std::sync::atomic::Ordering::SeqCst);
                if call <= 2 {
                    self.first_wave.wait().await;
                }
                if stage == EventItemStage::Stage
                    && !self
                        .missing_injected
                        .swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    self.active
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    return Err(Box::new(EventMissingAllocationError {
                        requirement: "device:survey_drone".into(),
                        allocation_id: missing_allocation.expect("event device allocation"),
                    }) as crate::event::AnyError);
                }
                self.order.lock().expect("event order").push(stage);
                self.active
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                Ok(mission_json.to_owned())
            })
        }
    }

    async fn seed_event_pool_worker(server: &MockServer, client: &Client, worker: &str) {
        let vessel = format!("{worker}-VESSEL");
        Mock::given(method("GET"))
            .and(path(format!("/v1/replicants/{worker}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "replicant_code": worker,
                "hosted_device_code": vessel,
                "location": "ROOT-1-L4",
                "status": "stationary"
            })))
            .expect(1)
            .mount(server)
            .await;
        client
            .replicants()
            .get_owned(worker)
            .await
            .expect("seed event worker");
        Mock::given(method("GET"))
            .and(path(format!("/v1/devices/{vessel}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": vessel,
                "device_type": "racing_vessel",
                "replicant_code": worker,
                "hosting_replicant": worker,
                "location": "ROOT-1-L4",
                "status": "idle"
            })))
            .expect(1)
            .mount(server)
            .await;
        client
            .devices()
            .get(&vessel)
            .await
            .expect("seed event vessel");
    }

    async fn seed_event_pool_device(
        server: &MockServer,
        client: &Client,
        worker: &str,
        code: &str,
    ) {
        Mock::given(method("GET"))
            .and(path(format!("/v1/devices/{code}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": code,
                "device_type": "survey_drone",
                "replicant_code": worker,
                "location": "ROOT-1-L4",
                "status": "idle"
            })))
            .expect(1)
            .mount(server)
            .await;
        client.devices().get(code).await.expect("seed event device");
    }

    #[tokio::test]
    async fn event_campaign_pool_registered_schema_one_workflow_orders_all_criteria() {
        let server = MockServer::start().await;
        let client = test_client_at(&server).await;
        Mock::given(method("GET"))
            .and(path("/v1/stars"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "generated_at": "2026-08-26T00:00:00Z",
                "total": 1,
                "stars": [{
                    "designation": "ROOT",
                    "region": "alpha",
                    "position": {"x": 0.0, "y": 0.0, "z": 0.0}
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        client
            .galaxy()
            .refresh_catalogue()
            .await
            .expect("seed event catalogue");
        seed_event_pool_worker(&server, &client, "REP-A").await;
        seed_event_pool_worker(&server, &client, "REP-B").await;
        seed_event_pool_device(&server, &client, "REP-A", "DRONE-A").await;
        seed_event_pool_device(&server, &client, "REP-B", "DRONE-B").await;
        seed_event_pool_device(&server, &client, "REP-A", "DRONE-SPARE").await;
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
        for worker in ["LEGACY", "REP-A", "REP-B"] {
            repository
                .put_document(
                    "director.replicant",
                    worker,
                    &serde_json::json!({"region": "alpha"}),
                )
                .expect("Director region");
        }
        let paths = [
            std::env::temp_dir().join("event-pool-one.json"),
            std::env::temp_dir().join("event-pool-two.json"),
        ];
        let archive = crate::event::event_campaign_pool_fixture_archive(&paths);
        let workflow = repository
            .create(NewWorkflow {
                kind: event_campaign_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({
                    "region": "alpha",
                    "replicant": "LEGACY",
                    "home": "ROOT-1-L4"
                }),
                checkpoint: EventCampaignCheckpoint {
                    replicant: Some("LEGACY".into()),
                    home: Some("ROOT-1-L4".into()),
                    archive: Some(archive),
                    connectivity_workflows: BTreeMap::new(),
                    replan_after_connectivity: false,
                },
                current_step: Some("executing".into()),
                parent_id: None,
            })
            .expect("legacy event campaign");
        let executor = Arc::new(FixtureEventItemExecutor::new());
        let mut registry = WorkflowRegistry::new();
        registry
            .register(Arc::new(EventCampaignWorkflowFactory::with_item_executor(
                executor.clone(),
            )))
            .expect("register event campaign");
        let supervisor = replicant_workflow::WorkflowSupervisor::with_managed_client(
            repository.clone(),
            Arc::new(registry),
            client.clone(),
        );
        for _ in 0..200 {
            supervisor.tick().await.expect("supervisor tick");
            if repository
                .read(workflow.id)
                .expect("workflow")
                .is_some_and(|workflow| workflow.status.is_terminal())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let workflow = repository
            .read(workflow.id)
            .expect("workflow")
            .expect("workflow exists");
        assert_eq!(workflow.schema_version, 2);
        let items = repository.list_work_items(workflow.id).expect("items");
        assert_eq!(
            workflow.status,
            WorkflowStatus::Succeeded,
            "error={:?}, items={items:#?}",
            workflow.last_error
        );
        assert_eq!(
            items.len(),
            16,
            "two events times two criteria times four stages"
        );
        assert!(
            items
                .iter()
                .all(|item| item.state.status == replicant_workflow::WorkItemStatus::Succeeded)
        );
        assert_eq!(
            executor.calls.load(std::sync::atomic::Ordering::SeqCst),
            17,
            "one structured missing device retries in place"
        );
        let replaced = items
            .iter()
            .find(|item| {
                item.spec.kind.as_str() == "event.stage" && item.state.checkpoint_json.is_some()
            })
            .expect("replaced event stage");
        assert_eq!(
            repository
                .list_work_item_attempts(replaced.id)
                .expect("event attempts")
                .len(),
            1
        );
        assert_eq!(executor.peak.load(std::sync::atomic::Ordering::SeqCst), 2);
        let stages_ordered = {
            let order = executor.order.lock().expect("event order");
            let first_delivery = order
                .iter()
                .position(|stage| *stage == EventItemStage::Delivery)
                .expect("delivery stage");
            order[..first_delivery]
                .iter()
                .all(|stage| *stage == EventItemStage::Stage)
        };
        assert!(stages_ordered);
        assert!(
            !paths
                .iter()
                .any(|path| path.with_extension("lock").exists())
        );
        let calls = executor.calls.load(std::sync::atomic::Ordering::SeqCst);
        drop(supervisor);
        let mut restarted_registry = WorkflowRegistry::new();
        restarted_registry
            .register(Arc::new(EventCampaignWorkflowFactory::with_item_executor(
                executor.clone(),
            )))
            .expect("register restarted event campaign");
        let restarted = replicant_workflow::WorkflowSupervisor::with_managed_client(
            repository,
            Arc::new(restarted_registry),
            client.clone(),
        );
        restarted.tick().await.expect("restart tick");
        assert_eq!(
            executor.calls.load(std::sync::atomic::Ordering::SeqCst),
            calls,
            "restart reuses terminal items and archived assets"
        );
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn salvage_recovery_history_remote_pages_are_authoritative_and_region_filtered() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/stars"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "generated_at": "2026-08-28T00:00:00Z",
                "stars": [
                    {"designation": "ROOT", "region": "alpha"},
                    {"designation": "BETA", "region": "beta"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        let mut discoveries = (0..462)
            .map(|index| {
                serde_json::json!({
                    "id": format!("{index:04}-0"),
                    "version": 1,
                    "category": "salvage",
                    "event": "salvage.discovered",
                    "created_at": "2026-08-26T00:00:00Z",
                    "payload": {
                        "designation": format!("SITE-{index}"),
                        "location": if index == 4 {"BETA-1-L4"} else {"ROOT-1-L4"},
                        "resources": {"structural": index + 1}
                    }
                })
            })
            .collect::<Vec<_>>();
        discoveries.push(serde_json::json!({
            "id": "9999-0",
            "version": 1,
            "category": "salvage",
            "event": "salvage.discovered",
            "created_at": "2026-08-27T00:00:00Z",
            "payload": {
                "designation": "SITE-3",
                "location": "ROOT-1-L4",
                "resources": {"structural": 999}
            }
        }));
        let page_count = discoveries.len().div_ceil(100);
        for (page_index, page) in discoveries.chunks(100).enumerate() {
            let cursor = (page_index != 0).then(|| format!("cursor-{page_index}"));
            let next_cursor =
                (page_index + 1 < page_count).then(|| format!("cursor-{}", page_index + 1));
            let mut mock = Mock::given(method("GET"))
                .and(path("/v1/events"))
                .and(query_param("filtered", "false"))
                .and(query_param("event", "salvage.discovered"))
                .and(query_param("limit", "100"));
            mock = if let Some(cursor) = cursor {
                mock.and(query_param("cursor", cursor))
            } else {
                mock.and(query_param_is_missing("cursor"))
            };
            mock.respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": page,
                "next_cursor": next_cursor
            })))
            .expect(1)
            .mount(&server)
            .await;
        }
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("filtered", "false"))
            .and(query_param("event", "salvage.depleted"))
            .and(query_param("limit", "100"))
            .and(query_param_is_missing("cursor"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [{
                    "id": "5000-0",
                    "version": 1,
                    "category": "salvage",
                    "event": "salvage.depleted",
                    "created_at": "2026-08-27T00:00:01Z",
                    "payload": {"designation": "SITE-1"}
                }],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;
        let snapshot = salvage_recovery_history_snapshot(&client)
            .await
            .expect("salvage history snapshot");
        assert_eq!(snapshot.discovered_count, 463);
        assert_eq!(snapshot.depleted_count, 1);
        let ledger =
            recoverable_salvage_sites(&snapshot, &BTreeSet::from(["SITE-2".into()]), "alpha");
        assert_eq!(ledger.len(), 459);
        assert_eq!(ledger["SITE-3"].resources["structural"], 999);
        assert!(!ledger.contains_key("SITE-1"));
        assert!(!ledger.contains_key("SITE-2"));
        assert!(!ledger.contains_key("SITE-4"));
        let capacity_site = BTreeMap::from([(
            "CAPACITY-SITE".into(),
            SalvageSiteRecord {
                designation: "CAPACITY-SITE".into(),
                location: "ROOT-1-L4".into(),
                resources: BTreeMap::from([("structural".into(), 17)]),
                event_id: "capacity-1".into(),
            },
        )]);
        let capacity_specs = salvage_recovery_item_specs(
            WorkflowId::new(),
            &capacity_site,
            "alpha",
            "ROOT-1-L4",
            &[("F-4".into(), 4), ("F-9".into(), 9)],
        )
        .expect("capacity items");
        assert_eq!(
            capacity_specs
                .iter()
                .map(|spec| spec.payload_json["trip_quantity"]
                    .as_u64()
                    .expect("quantity"))
                .collect::<Vec<_>>(),
            [4, 9, 4]
        );
        for spec in &capacity_specs {
            let requirements: Vec<ResourceRequirement> =
                serde_json::from_value(spec.requirements_json.clone()).expect("requirements");
            let stow = requirements
                .iter()
                .find(|requirement| requirement.key == "stow")
                .expect("stow requirement");
            assert_eq!(
                stow.quantity,
                spec.payload_json["trip_quantity"]
                    .as_u64()
                    .expect("quantity")
            );
        }
        let specs =
            salvage_recovery_item_specs(WorkflowId::new(), &ledger, "alpha", "ROOT-1-L4", &[])
                .expect("salvage items");
        assert_eq!(specs.len(), 459);
        assert_eq!(specs[0].payload_json["location"], "ROOT-1-L4");
        assert_ne!(
            specs[0].payload_json["designation"],
            specs[0].payload_json["location"]
        );
        assert_eq!(
            salvage_capacity_shards(17, &[("F-4".into(), 4), ("F-9".into(), 9)]),
            [("F-4".into(), 4), ("F-9".into(), 9), ("F-4".into(), 4),]
        );
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("event", "salvage.repeat"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [],
                "next_cursor": "same"
            })))
            .mount(&server)
            .await;
        let repeated = client
            .events()
            .full_history_named("salvage.repeat")
            .await
            .expect_err("repeated cursor must fail");
        assert!(repeated.to_string().contains("cursor repeated"));
        client.close().await.expect("close client");
    }

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
    #[tokio::test]
    async fn salvage_recovery_history_refreshes_empty_catalogue_and_canonicalizes_region() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/stars"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "generated_at": "2026-08-28T00:00:00Z",
                "stars": [
                    {"designation": "ROOT", "region": "Alpha"},
                    {"designation": "ORPHAN"}
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("filtered", "false"))
            .and(query_param("event", "salvage.discovered"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [
                    {
                        "id": "1-0",
                        "version": 1,
                        "category": "salvage",
                        "event": "salvage.discovered",
                        "created_at": "2026-08-28T00:00:00Z",
                        "payload": {
                            "designation": "SITE-A",
                            "location": "ROOT-1-L4",
                            "resources": {"structural": 2}
                        }
                    },
                    {
                        "id": "2-0",
                        "version": 1,
                        "category": "salvage",
                        "event": "salvage.discovered",
                        "created_at": "2026-08-28T00:00:01Z",
                        "payload": {
                            "designation": "SITE-B",
                            "location": "ROOT-1-L4",
                            "resources": {"structural": 3}
                        }
                    },
                    {
                        "id": "2-1",
                        "version": 1,
                        "category": "salvage",
                        "event": "salvage.discovered",
                        "created_at": "2026-08-28T00:00:02Z",
                        "payload": {
                            "designation": "SITE-UNREGIONED",
                            "location": "ORPHAN-1-L4",
                            "resources": {"structural": 1}
                        }
                    }
                ],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("filtered", "false"))
            .and(query_param("event", "salvage.depleted"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [{
                    "id": "3-0",
                    "version": 1,
                    "category": "salvage",
                    "event": "salvage.depleted",
                    "created_at": "2026-08-28T00:00:02Z",
                    "payload": {"site": "SITE-A"}
                }],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = test_client_at(&server).await;
        assert!(client.galaxy().catalogue().is_empty());
        let snapshot = salvage_recovery_history_snapshot(&client)
            .await
            .expect("salvage history snapshot");
        assert_eq!(snapshot.discovered_count, 3);
        assert_eq!(snapshot.depleted_count, 1);
        let sites = recoverable_salvage_sites(&snapshot, &BTreeSet::new(), "ALPHA");
        assert_eq!(
            sites.keys().map(String::as_str).collect::<Vec<_>>(),
            ["SITE-B"]
        );
        client.close().await.expect("close client");
    }

    #[test]
    fn salvage_recovery_workflow_matches_requires_active_decodable_intent() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let active = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "Alpha".into(),
                home: "ROOT-1-L4".into(),
            }))
            .expect("active salvage recovery");
        assert!(salvage_recovery_workflow_matches(&active, "alpha").expect("compatible workflow"));

        let whitespace_home = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "alpha".into(),
                home: " \t".into(),
            }))
            .expect("whitespace-home salvage recovery");
        assert!(
            !salvage_recovery_workflow_matches(&whitespace_home, "alpha")
                .expect("whitespace home is incompatible")
        );

        let malformed = repository
            .create(NewWorkflow {
                kind: salvage_recovery_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!("not an intent"),
                checkpoint: Value::Null,
                current_step: None,
                parent_id: None,
            })
            .expect("malformed salvage recovery");
        assert!(salvage_recovery_workflow_matches(&malformed, "alpha").is_err());
    }

    #[tokio::test]
    async fn salvage_recovery_history_registered_empty_campaign_executes_through_supervisor() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/stars"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "generated_at": "2026-08-28T00:00:00Z",
                "stars": []
            })))
            .expect(1)
            .mount(&server)
            .await;
        for event_name in ["salvage.discovered", "salvage.depleted"] {
            Mock::given(method("GET"))
                .and(path("/v1/events"))
                .and(query_param("event", event_name))
                .and(query_param("filtered", "false"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "events": [],
                    "next_cursor": null
                })))
                .expect(1)
                .mount(&server)
                .await;
        }
        let client = test_client_at(&server).await;
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
        let workflow = repository
            .create(new_salvage_recovery_workflow(SalvageRecoveryIntent {
                region: "alpha".into(),
                home: "ROOT-1-L4".into(),
            }))
            .expect("salvage recovery workflow");
        let mut registry = WorkflowRegistry::new();
        super::register(&mut registry).expect("register runtime workflows");
        let supervisor = replicant_workflow::WorkflowSupervisor::with_managed_client(
            repository.clone(),
            Arc::new(registry),
            client.clone(),
        );
        for _ in 0..20 {
            supervisor.tick().await.expect("supervisor tick");
            if repository
                .read(workflow.id)
                .expect("workflow")
                .is_some_and(|workflow| workflow.status.is_terminal())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let workflow = repository
            .read(workflow.id)
            .expect("workflow")
            .expect("workflow exists");
        assert_eq!(
            workflow.status,
            WorkflowStatus::Succeeded,
            "{:?}",
            workflow.last_error
        );
        client.close().await.expect("close client");
    }

    fn inventory_response(quantity: i64, next_cursor: Option<&str>) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "locations": [{
                "location": "SHOP-1",
                "items": [{"resource_type": "rares", "quantity": quantity}]
            }],
            "next_cursor": next_cursor
        }))
    }

    fn device_list_response(codes: &[&str]) -> ResponseTemplate {
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "devices": codes
                .iter()
                .map(|code| serde_json::json!({
                    "device_code": code,
                    "device_type": "survey_drone",
                    "status": "idle"
                }))
                .collect::<Vec<_>>(),
            "next_cursor": null
        }))
    }

    async fn mount_device_list(server: &MockServer, codes: &[&str]) {
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(device_list_response(codes))
            .expect(1)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn guarded_empty_device_refresh_preserves_cached_snapshots() {
        let server = MockServer::start().await;
        mount_device_list(&server, &["DEVICE-B", "DEVICE-A"]).await;
        let client = test_client_at(&server).await;
        let stale_handles = client
            .devices()
            .refresh_many()
            .collect()
            .await
            .expect("seed device handles");

        server.reset().await;
        mount_device_list(&server, &[]).await;
        let error = client
            .devices()
            .refresh_many()
            .collect()
            .await
            .expect_err("empty enumeration requires guarded approval");
        assert!(error.to_string().contains("empty device enumeration"));
        let snapshots = device_snapshots(&client, stale_handles)
            .await
            .expect("guard retains cached snapshots");
        assert_eq!(
            snapshots
                .iter()
                .map(|device| device.key.id.as_str())
                .collect::<Vec<_>>(),
            ["DEVICE-A", "DEVICE-B"]
        );
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn device_census_reuses_results_and_invalidates_after_mutation() {
        let server = MockServer::start().await;
        mount_device_list(&server, &["DEVICE-A"]).await;
        let client = test_client_at(&server).await;
        client
            .devices()
            .refresh_many()
            .collect()
            .await
            .expect("seed device cache");

        let expected = owned_device_snapshots(&client)
            .await
            .expect("direct census");
        let mut census = DeviceCensus::default();
        let first = census.snapshots(&client).await.expect("memoized census");
        assert_eq!(first, expected);
        let first_ptr = first.as_ptr();
        assert_eq!(
            census
                .snapshots(&client)
                .await
                .expect("reused census")
                .as_ptr(),
            first_ptr
        );

        server.reset().await;
        mount_device_list(&server, &["DEVICE-A", "DEVICE-B"]).await;
        client
            .devices()
            .refresh_many()
            .collect()
            .await
            .expect("expand device projection");
        census.invalidate();

        assert!(
            census
                .snapshots(&client)
                .await
                .expect("fresh census")
                .iter()
                .any(|device| device.key.id.as_str() == "DEVICE-B")
        );
        server.verify().await;
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn trade_reward_wait_fetches_one_filtered_page_per_check() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/inventory"))
            .and(query_param("location", "SHOP-1"))
            .and(query_param("limit", "100"))
            .respond_with(inventory_response(10, Some("must-not-follow")))
            .expect(1)
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;

        assert!(
            wait_for_trade_reward_resources(
                &client,
                "SHOP-1",
                &ResourceMap::from([("rares".to_owned(), 10)]),
                1,
            )
            .await
            .expect("wait for rewards")
        );

        server.verify().await;
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn trade_reward_wait_uses_satisfied_projection_without_a_request() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/inventory"))
            .respond_with(inventory_response(10, None))
            .expect(1)
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        fetch_inventory_at_location(&client, "SHOP-1")
            .await
            .expect("seed projection");

        assert!(
            wait_for_trade_reward_resources(
                &client,
                "SHOP-1",
                &ResourceMap::from([("rares".to_owned(), 10)]),
                1,
            )
            .await
            .expect("wait for rewards")
        );

        server.verify().await;
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn trade_reward_wait_returns_false_on_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/inventory"))
            .respond_with(inventory_response(9, None))
            .expect(1)
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;

        assert!(
            !wait_for_trade_reward_resources(
                &client,
                "SHOP-1",
                &ResourceMap::from([("rares".to_owned(), 10)]),
                1,
            )
            .await
            .expect("wait for rewards")
        );

        server.verify().await;
        client.close().await.expect("close client");
    }

    #[test]
    fn logistics_intent_builds_resource_delivery_without_executor_plumbing() {
        let request = delivery_request(&LogisticsIntent {
            origin: "SCEPTURUM".to_owned(),
            destination: "TWAFFY-OBJ-1".to_owned(),
            payload_kind: Some(LogisticsPayloadKind::Resource),
            item: Some("rares".to_owned()),
            quantity: 400,
            resources: ResourceMap::new(),
            devices: Vec::new(),
            device_tags: Vec::new(),
            return_transports: false,
        });
        assert_eq!(request.resources.get("rares"), Some(&400));
        assert!(request.devices.is_empty());
        assert!(request.device_tags.is_empty());
    }

    #[test]
    fn logistics_intent_combines_multiple_payload_kinds() {
        let mut resources = ResourceMap::new();
        resources.insert("rares".to_owned(), 400);
        resources.insert("volatiles".to_owned(), 100);
        let request = delivery_request(&LogisticsIntent {
            origin: "SCEPTURUM".to_owned(),
            destination: "TWAFFY-OBJ-1".to_owned(),
            payload_kind: None,
            item: None,
            quantity: 1,
            resources,
            devices: vec![DeviceRequest {
                device_type: "exotic_matter_injector".to_owned(),
                quantity: 36,
            }],
            device_tags: vec!["twaffy-obj-1".to_owned()],
            return_transports: true,
        });
        assert_eq!(request.resources.get("rares"), Some(&400));
        assert_eq!(request.resources.get("volatiles"), Some(&100));
        assert_eq!(request.devices.len(), 1);
        assert_eq!(request.devices[0].quantity, 36);
        assert_eq!(request.device_tags, vec!["twaffy-obj-1"]);
    }

    #[test]
    fn scan_tour_prints_only_missing_survey_fleet_devices() {
        let requests = scan_tour_fleet_print_requests(1, 2);
        assert_eq!(requests, vec![PrintRequest::new("survey_drone", 1)]);

        let requests = scan_tour_fleet_print_requests(0, 1);
        assert_eq!(
            requests,
            vec![
                PrintRequest::new("ami_survey_controller", 1),
                PrintRequest::new("survey_drone", 2),
            ]
        );

        assert!(scan_tour_fleet_print_requests(1, 3).is_empty());
    }

    #[test]
    fn scan_tour_factory_home_prefers_the_center_system() {
        let locations = vec![
            "SCEPTURUM-BELT-1".to_owned(),
            "PHASYRIS-BELT-2".to_owned(),
            "PHASYRIS-BELT-1".to_owned(),
            "PHASYRIS-BELT-1".to_owned(),
        ];

        assert_eq!(
            scan_tour_factory_home("PHASYRIS", &locations).as_deref(),
            Some("PHASYRIS-BELT-1")
        );
    }

    #[test]
    fn scan_tour_reuses_complete_fleet_already_stowed_in_vessel() {
        let controller = ScanTourFleetDeviceCandidate {
            code: "CONTROLLER".to_owned(),
            stowed: true,
            controller: None,
            // The live collection projection can omit the controller-side
            // reverse relationship even while each drone points at it.
            controlled_devices: BTreeSet::new(),
        };
        let drones = ["D1", "D2", "D3"]
            .into_iter()
            .map(|code| ScanTourFleetDeviceCandidate {
                code: code.to_owned(),
                stowed: true,
                controller: Some("CONTROLLER".to_owned()),
                controlled_devices: BTreeSet::new(),
            })
            .collect();

        let availability = select_scan_tour_fleet_availability(vec![controller], drones)
            .expect("select stowed survey fleet");

        assert_eq!(availability.controllers, vec!["CONTROLLER".to_owned()]);
        assert_eq!(
            availability.drones,
            vec!["D1".to_owned(), "D2".to_owned(), "D3".to_owned()]
        );
        assert_eq!(availability.stowed_selected, 4);
        assert!(
            scan_tour_fleet_print_requests(
                availability.controllers.len(),
                availability.drones.len(),
            )
            .is_empty()
        );
    }

    #[test]
    fn scan_tour_counts_partial_stowed_fleet_before_printing() {
        let controller = ScanTourFleetDeviceCandidate {
            code: "CONTROLLER".to_owned(),
            stowed: true,
            controller: None,
            controlled_devices: BTreeSet::new(),
        };
        let drones = vec![
            ScanTourFleetDeviceCandidate {
                code: "D1".to_owned(),
                stowed: true,
                controller: Some("CONTROLLER".to_owned()),
                controlled_devices: BTreeSet::new(),
            },
            ScanTourFleetDeviceCandidate {
                code: "D2".to_owned(),
                stowed: true,
                controller: Some("CONTROLLER".to_owned()),
                controlled_devices: BTreeSet::new(),
            },
        ];

        let availability = select_scan_tour_fleet_availability(vec![controller], drones)
            .expect("select partial stowed survey fleet");

        assert_eq!(availability.controllers, vec!["CONTROLLER".to_owned()]);
        assert_eq!(availability.drones, vec!["D1".to_owned(), "D2".to_owned()]);
        assert_eq!(availability.stowed_selected, 3);
        assert_eq!(
            scan_tour_fleet_print_requests(
                availability.controllers.len(),
                availability.drones.len(),
            ),
            vec![PrintRequest::new("survey_drone", 1)]
        );
    }

    #[test]
    fn scan_tour_claims_exact_targets_for_sharded_routes() {
        let intent = ScanTourIntent {
            center: "SCEPTURUM".to_owned(),
            radius_ly: 20.0,
            system_limit: 10,
            target_systems: Some(vec![
                " beta-two ".to_owned(),
                "BETA-ONE".to_owned(),
                "beta-one".to_owned(),
            ]),
            replicant: None,
            vessel: None,
            include_explored: false,
        };
        assert_eq!(
            scan_tour_target_claims(&intent),
            vec![
                ResourceKey::Namespaced {
                    namespace: "survey-system".to_owned(),
                    key: "BETA-ONE".to_owned(),
                },
                ResourceKey::Namespaced {
                    namespace: "survey-system".to_owned(),
                    key: "BETA-TWO".to_owned(),
                },
            ]
        );

        let unbounded = ScanTourIntent {
            target_systems: None,
            ..intent
        };
        assert_eq!(
            scan_tour_target_claims(&unbounded),
            vec![ResourceKey::Namespaced {
                namespace: "survey-tour".to_owned(),
                key: "SCEPTURUM".to_owned(),
            }]
        );
    }

    #[derive(Clone, Deserialize, Serialize)]
    enum FailureRoutingFixture {
        EventInputs,
        ManagedClientClosed,
        EventExecutorContention,
        EventDeviceMissing,
        ExplorationDeviceMissing,
        ExplorationRelayPrerequisite,
    }

    struct FailureRoutingFactory {
        kind: WorkflowKind,
    }

    impl WorkflowFactory for FailureRoutingFactory {
        fn kind(&self) -> &WorkflowKind {
            &self.kind
        }

        fn current_schema_version(&self) -> u32 {
            1
        }

        fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
            Some(Box::new(FailureRoutingWorkflow))
        }
    }

    struct FailureRoutingWorkflow;

    impl WorkflowExecutor for FailureRoutingWorkflow {
        fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
            Box::pin(async move {
                let fixture: FailureRoutingFixture = context.config().map_err(string_error)?;
                let plan_file = scratch_file(context.id(), "failure-routing.json")?;
                fs::write(&plan_file, "stale scratch state").map_err(string_error)?;
                match fixture {
                    FailureRoutingFixture::EventInputs
                    | FailureRoutingFixture::ManagedClientClosed
                    | FailureRoutingFixture::EventExecutorContention
                    | FailureRoutingFixture::EventDeviceMissing => {
                        let mut checkpoint: EventCampaignCheckpoint =
                            context.checkpoint().map_err(string_error)?;
                        let handled = match fixture {
                            FailureRoutingFixture::EventInputs => {
                                let error = ClassifiedError::new(
                                    FailureClass::EventInputsUnavailable,
                                    io::ErrorKind::WouldBlock,
                                    "blocked events remain",
                                );
                                persist_retryable_event_campaign_failure(
                                    context,
                                    &mut checkpoint,
                                    &plan_file,
                                    &error,
                                )?
                            }
                            FailureRoutingFixture::ManagedClientClosed => {
                                persist_retryable_event_campaign_failure(
                                    context,
                                    &mut checkpoint,
                                    &plan_file,
                                    &replicant_client::Error::Closed,
                                )?
                            }
                            FailureRoutingFixture::EventExecutorContention => {
                                let error = ClassifiedError::new(
                                    FailureClass::EventExecutorContention,
                                    io::ErrorKind::WouldBlock,
                                    "campaign file is locked",
                                );
                                persist_retryable_event_campaign_failure(
                                    context,
                                    &mut checkpoint,
                                    &plan_file,
                                    &error,
                                )?
                            }
                            FailureRoutingFixture::EventDeviceMissing => {
                                let error = ClassifiedError::permanent(
                                    FailureClass::DeviceTargetMissing,
                                    io::ErrorKind::NotFound,
                                    "selected device no longer exists",
                                );
                                persist_retryable_event_campaign_failure(
                                    context,
                                    &mut checkpoint,
                                    &plan_file,
                                    &error,
                                )?
                            }
                            FailureRoutingFixture::ExplorationDeviceMissing
                            | FailureRoutingFixture::ExplorationRelayPrerequisite => {
                                unreachable!()
                            }
                        };
                        if handled {
                            Ok(())
                        } else {
                            Err("event failure fixture was not routed".to_owned())
                        }
                    }
                    FailureRoutingFixture::ExplorationDeviceMissing => {
                        let mut checkpoint: ExplorationWorkflowCheckpoint =
                            context.checkpoint().map_err(string_error)?;
                        let error = ClassifiedError::permanent(
                            FailureClass::DeviceTargetMissing,
                            io::ErrorKind::NotFound,
                            "selected relay no longer exists",
                        );
                        if persist_stale_exploration_replan(
                            context,
                            &mut checkpoint,
                            &plan_file,
                            "BETA",
                            &error,
                        )? {
                            Ok(())
                        } else {
                            Err("exploration failure fixture was not routed".to_owned())
                        }
                    }
                    FailureRoutingFixture::ExplorationRelayPrerequisite => {
                        let checkpoint: ExplorationWorkflowCheckpoint =
                            context.checkpoint().map_err(string_error)?;
                        let error = ClassifiedError::new(
                            FailureClass::ManufacturingCapacity,
                            io::ErrorKind::TimedOut,
                            "timed out waiting for next relay deployment load: ALPHA, BETA",
                        );
                        if !retryable_connectivity_dependency_failure(&error) {
                            return Err("manufacturing prerequisite was not retryable".to_owned());
                        }
                        wait_for_exploration_relay_prerequisites(
                            context,
                            &checkpoint,
                            error.to_string(),
                        )
                    }
                }
            })
        }
    }

    fn relay_execution_fixture() -> RelayExecutionState {
        serde_json::from_value(serde_json::json!({
            "version": 2,
            "mission_id": "relay-test",
            "replicant_code": "REP-1",
            "vessel_code": "VESSEL-1",
            "hub_location": "ROOT-1-L4",
            "start_system": "ROOT",
            "targets": ["TARGET"],
            "max_hop_ly": 7.499,
            "network": {
                "start": "ROOT",
                "requested_targets": ["TARGET"],
                "max_hop_ly": 7.499,
                "nodes": [],
                "edges": [],
                "new_relay_systems": ["TARGET"],
                "activation_systems": [],
                "active_relay_systems": ["ROOT"],
                "execution_order": ["TARGET"],
                "execution_order_optimal": true,
                "execution_hops": 2,
                "execution_distance_ly": 12.0,
                "total_edge_distance_ly": 6.0
            },
            "stops": [{
                "system": "TARGET",
                "location": "TARGET-1-L4",
                "parent_system": "ROOT",
                "action": "deploy_and_activate",
                "relay_code": null,
                "completed": false
            }],
            "hub_stock_relays": [],
            "print_jobs": [],
            "planned_transport_capacity": 1,
            "supply": null,
            "returned_to_hub": false
        }))
        .expect("relay execution fixture")
    }

    #[tokio::test]
    async fn event_campaign_pool_failure_routes_persist_waiting_checkpoints_without_new_rows() {
        let repository =
            std::sync::Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
        let kind = WorkflowKind::new("test.failure-routing").expect("valid kind");
        let event_checkpoint = EventCampaignCheckpoint {
            archive: Some(EventCampaignArchive {
                campaign_json: "durable campaign".to_owned(),
                mission_json: BTreeMap::new(),
            }),
            ..EventCampaignCheckpoint::default()
        };
        let fixtures = [
            (
                FailureRoutingFixture::EventInputs,
                serde_json::to_value(&event_checkpoint).expect("event checkpoint"),
                "waiting_for_event_inputs",
            ),
            (
                FailureRoutingFixture::ManagedClientClosed,
                serde_json::to_value(&event_checkpoint).expect("event checkpoint"),
                "waiting_for_managed_client",
            ),
            (
                FailureRoutingFixture::EventExecutorContention,
                serde_json::to_value(&event_checkpoint).expect("event checkpoint"),
                "waiting_for_event_executor",
            ),
            (
                FailureRoutingFixture::EventDeviceMissing,
                serde_json::to_value(&event_checkpoint).expect("event checkpoint"),
                "replanning_after_stale_asset",
            ),
            (
                FailureRoutingFixture::ExplorationDeviceMissing,
                serde_json::to_value(ExplorationWorkflowCheckpoint {
                    state: Some(relay_execution_fixture()),
                    ..ExplorationWorkflowCheckpoint::default()
                })
                .expect("exploration checkpoint"),
                "replanning_relay_coverage",
            ),
            (
                FailureRoutingFixture::ExplorationRelayPrerequisite,
                serde_json::to_value(ExplorationWorkflowCheckpoint {
                    replicant: Some("REP-1".into()),
                    hub: Some("ROOT-1-L4".into()),
                    state: Some(relay_execution_fixture()),
                    ..ExplorationWorkflowCheckpoint::default()
                })
                .expect("exploration prerequisite checkpoint"),
                "awaiting_relay_prerequisites",
            ),
        ];
        let mut ids = Vec::new();
        for (fixture, checkpoint, expected_step) in fixtures {
            let workflow = repository
                .create(NewWorkflow {
                    kind: kind.clone(),
                    schema_version: 1,
                    config: fixture,
                    checkpoint,
                    current_step: None,
                    parent_id: None,
                })
                .expect("create routing fixture");
            ids.push((workflow.id, expected_step));
        }
        let initial_rows = repository.list().expect("initial rows").len();
        let mut registry = WorkflowRegistry::new();
        registry
            .register(std::sync::Arc::new(FailureRoutingFactory { kind }))
            .expect("register routing fixture");
        let supervisor = replicant_workflow::WorkflowSupervisor::new(
            repository.clone(),
            std::sync::Arc::new(registry),
        );

        for _ in 0..100 {
            supervisor.tick().await.expect("supervisor tick");
            if ids.iter().all(|(id, _)| {
                repository
                    .read(*id)
                    .expect("read fixture")
                    .is_some_and(|workflow| workflow.status == WorkflowStatus::Waiting)
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert_eq!(repository.list().expect("final rows").len(), initial_rows);
        for (id, expected_step) in ids {
            let workflow = repository
                .read(id)
                .expect("read routed workflow")
                .expect("routed workflow");
            assert_eq!(workflow.status, WorkflowStatus::Waiting);
            assert_eq!(workflow.current_step.as_deref(), Some(expected_step));
            assert_eq!(workflow.last_error, None);
            if expected_step == "replanning_after_stale_asset" {
                let checkpoint: EventCampaignCheckpoint =
                    workflow.checkpoint().expect("event checkpoint");
                assert!(checkpoint.archive.is_none());
                assert!(
                    !scratch_file(id, "failure-routing.json")
                        .expect("scratch path")
                        .exists()
                );
            } else if expected_step == "replanning_relay_coverage" {
                let checkpoint: ExplorationWorkflowCheckpoint =
                    workflow.checkpoint().expect("exploration checkpoint");
                assert!(checkpoint.state.is_none());
                assert!(
                    !scratch_file(id, "failure-routing.json")
                        .expect("scratch path")
                        .exists()
                );
            } else if expected_step == "awaiting_relay_prerequisites" {
                let checkpoint: ExplorationWorkflowCheckpoint =
                    workflow.checkpoint().expect("exploration checkpoint");
                let state = checkpoint.state.expect("retained relay plan");
                let encoded = serde_json::to_value(state).expect("retained relay plan JSON");
                assert_eq!(encoded["mission_id"], "relay-test");
                assert_eq!(encoded["stops"][0]["system"], "TARGET");
                let reason = "timed out waiting for next relay deployment load: ALPHA, BETA";
                assert!(
                    repository
                        .activity(id)
                        .expect("workflow activity")
                        .iter()
                        .any(|activity| {
                            serde_json::from_str::<crate::workflows::WorkflowActivityEvent>(
                                &activity.message,
                            )
                            .ok()
                                == Some(crate::workflows::WorkflowActivityEvent::WaitReason {
                                    step: "awaiting_relay_prerequisites".to_owned(),
                                    reason: reason.to_owned(),
                                })
                        })
                );
            } else {
                let checkpoint: EventCampaignCheckpoint =
                    workflow.checkpoint().expect("event checkpoint");
                assert_eq!(
                    checkpoint
                        .archive
                        .as_ref()
                        .map(|archive| archive.campaign_json.as_str()),
                    Some("durable campaign")
                );
            }
        }
    }

    #[test]
    fn stale_trade_criteria_resource_failures_are_retryable() {
        let stale = TransportError::StaleResourcePickup(
            "operator wording can change without changing retry behavior".to_owned(),
        );
        let payload = TransportError::PayloadUnavailable("payload copy was reworded".to_owned());
        assert!(retryable_trade_criteria_logistics_failure(
            logistics_failure_class(&stale),
            ""
        ));
        assert!(retryable_trade_criteria_logistics_failure(
            logistics_failure_class(&payload),
            ""
        ));
        assert!(retryable_trade_criteria_logistics_failure(
            None,
            "operation rejected: Insufficient structural at location: need 49.0, have 0"
        ));
        assert!(!retryable_trade_criteria_logistics_failure(
            None,
            "transport CARRIER-1 has no usable cargo capacity"
        ));
    }

    #[test]
    fn event_campaign_pool_failure_routing() {
        let control = ClassifiedError::new(
            FailureClass::EventControlUnavailable,
            io::ErrorKind::WouldBlock,
            "completely reworded control failure",
        );
        let stale = ClassifiedError::new(
            FailureClass::EventAssetStale,
            io::ErrorKind::NotFound,
            "completely reworded stale asset failure",
        );
        let blocked = ClassifiedError::new(
            FailureClass::EventInputsUnavailable,
            io::ErrorKind::Other,
            "completely reworded input failure",
        );
        let missing = ClassifiedError::permanent(
            FailureClass::DeviceTargetMissing,
            io::ErrorKind::NotFound,
            "selected device no longer exists",
        );
        let contention = ClassifiedError::new(
            FailureClass::EventExecutorContention,
            io::ErrorKind::WouldBlock,
            "another executor owns the campaign file",
        );
        assert!(retryable_event_campaign_failure(&control));
        assert!(!event_campaign_failure_requires_replan(&control));
        assert!(retryable_event_campaign_failure(&stale));
        assert!(event_campaign_failure_requires_replan(&stale));
        assert!(retryable_event_campaign_failure(&blocked));
        assert_eq!(
            event_campaign_wait_step(&blocked),
            "waiting_for_event_inputs"
        );
        assert!(!event_campaign_failure_requires_replan(&blocked));
        assert!(retryable_event_campaign_failure(&missing));
        assert!(event_campaign_failure_requires_replan(&missing));
        assert!(stale_relay_plan_failure(&missing));
        assert!(retryable_event_campaign_failure(&contention));
        assert_eq!(
            event_campaign_wait_step(&contention),
            "waiting_for_event_executor"
        );
        assert!(retryable_event_campaign_failure(
            &replicant_client::Error::Closed
        ));
        assert_eq!(
            event_campaign_wait_step(&replicant_client::Error::Closed),
            "waiting_for_managed_client"
        );
        let legacy_upstream = io::Error::other("403 Not your device");
        assert!(retryable_event_campaign_failure(&legacy_upstream));
        assert!(event_campaign_failure_requires_replan(&legacy_upstream));
        assert!(!retryable_event_campaign_failure(&io::Error::other(
            "event criterion is structurally invalid"
        )));
    }

    #[test]
    fn exploration_failure_routing() {
        let missing = ClassifiedError::permanent(
            FailureClass::DeviceTargetMissing,
            io::ErrorKind::NotFound,
            "selected relay no longer exists",
        );
        assert!(stale_relay_plan_failure(&missing));
        assert_eq!(
            failure_disposition(&missing),
            replicant_workflow::WorkflowFailureDisposition::Permanent
        );

        let contention = ClassifiedError::new(
            FailureClass::ResourceClaimContention,
            io::ErrorKind::WouldBlock,
            "resource owner is still active",
        );
        assert!(resource_claim_contention(&contention));
        assert!(!stale_relay_plan_failure(&contention));

        let travel_timeout =
            io::Error::new(io::ErrorKind::TimedOut, "timed out traveling to TARGET");
        assert!(retryable_connectivity_dependency_failure(&travel_timeout));
    }

    #[test]
    fn relay_connectivity_capacity_blockers_are_retryable_without_campaign_failure() {
        let route = replicant_route_planner::PlannerError::Disconnected;
        let local = ClassifiedError::new(
            FailureClass::ConnectivityDependency,
            io::ErrorKind::NotFound,
            "completely reworded relay prerequisite",
        );
        let manufacturing = ClassifiedError::new(
            FailureClass::ManufacturingCapacity,
            io::ErrorKind::TimedOut,
            "timed out waiting for next relay deployment load: ALPHA, BETA",
        );
        assert!(retryable_connectivity_dependency_failure(&manufacturing));
        assert_eq!(
            failure_disposition(&manufacturing),
            replicant_workflow::WorkflowFailureDisposition::Retryable
        );
        assert!(retryable_connectivity_dependency_failure(&route));
        assert!(relay_failure_is_topology_impossible(&route));
        assert!(!relay_failure_is_topology_impossible(&manufacturing));
        assert!(retryable_connectivity_dependency_failure(&local));
        assert!(retryable_connectivity_dependency_failure(
            &replicant_client::Error::Closed
        ));
        assert!(retryable_connectivity_dependency_failure(
            &io::Error::other("missing blueprint for requested device type `comm_satellite`")
        ));
        assert!(!retryable_connectivity_dependency_failure(
            &io::Error::other("relay checkpoint is malformed")
        ));
    }
    #[test]
    fn unchanged_topology_blocker_suppresses_planner_until_signature_changes() {
        let mut checkpoint: ExplorationWorkflowCheckpoint =
            serde_json::from_value(serde_json::json!({
                "replicant": "REP-1",
                "hub": "ROOT-1-L4"
            }))
            .expect("legacy exploration checkpoint");
        let mut planner_calls = 0;

        if prepare_topology_replan(&mut checkpoint, "relay-topology-v1:catalogue-a-range-a") {
            planner_calls += 1;
        }
        assert_eq!(
            planner_calls, 1,
            "legacy checkpoint performs one safe replan"
        );

        checkpoint.topology_blocker = Some(ExplorationTopologyBlocker {
            signature: "relay-topology-v1:catalogue-a-range-a".to_owned(),
        });
        for _ in 0..3 {
            if prepare_topology_replan(&mut checkpoint, "relay-topology-v1:catalogue-a-range-a") {
                planner_calls += 1;
            }
        }
        assert_eq!(
            planner_calls, 1,
            "supervisor wakes do not rerun the planner"
        );

        if prepare_topology_replan(&mut checkpoint, "relay-topology-v1:catalogue-b-range-a") {
            planner_calls += 1;
        }
        assert_eq!(planner_calls, 2, "changed topology invalidates the blocker");
        assert!(checkpoint.topology_blocker.is_none());

        checkpoint.topology_blocker = Some(ExplorationTopologyBlocker {
            signature: "relay-topology-v1:catalogue-b-range-a".to_owned(),
        });
        if prepare_topology_replan(&mut checkpoint, "relay-topology-v1:catalogue-b-range-b") {
            planner_calls += 1;
            checkpoint.state = Some(relay_execution_fixture());
        }
        assert_eq!(
            planner_calls, 3,
            "changed usable range invalidates the blocker"
        );
        assert!(checkpoint.topology_blocker.is_none());
        assert!(
            checkpoint.state.is_some(),
            "new connectivity can persist a live plan after invalidation"
        );
    }

    #[test]
    fn relay_plan_and_repository_claim_errors_keep_their_types() {
        let stale = ClassifiedError::new(
            FailureClass::RelayPlanStale,
            io::ErrorKind::InvalidData,
            "the relay message was reworded",
        );
        let contention = RepositoryError::ClaimConflict {
            resource: ResourceKey::Device("D-1".to_owned()),
            owner: WorkflowId::new(),
        };
        assert!(stale_relay_plan_failure(&stale));
        assert!(resource_claim_contention(&contention));
        assert!(!stale_relay_plan_failure(&contention));
    }

    #[test]
    fn mutable_manifest_planning_blockers_wait_for_replan() {
        assert!(retryable_manifest_planning_failure(
            &TransportError::NotFound(
                "origin SCEPTURUM has only 0 conductive; 30 requested".to_owned(),
            )
        ));
        assert!(retryable_manifest_planning_failure(
            &TransportError::PayloadUnavailable("payload message was reworded".to_owned())
        ));
        assert!(!retryable_manifest_planning_failure(
            &TransportError::Invalid("destination must be an exact location".to_owned(),)
        ));
    }

    #[test]
    fn event_connectivity_checkpoint_fields_are_backward_compatible() {
        let delivery: EventDeliveryCheckpoint = serde_json::from_value(serde_json::json!({
            "replicant": "REP-1",
            "home": "SCEPTURUM-BELT-1",
            "plan_json": null,
            "ready": false
        }))
        .expect("restore legacy delivery checkpoint");
        assert!(delivery.connectivity_workflows.is_empty());
        assert!(!delivery.replan_after_connectivity);

        let campaign: EventCampaignCheckpoint = serde_json::from_value(serde_json::json!({
            "replicant": "REP-1",
            "home": "SCEPTURUM-BELT-1",
            "archive": null
        }))
        .expect("restore legacy campaign checkpoint");
        assert!(campaign.connectivity_workflows.is_empty());
        assert!(!campaign.replan_after_connectivity);
    }

    #[test]
    fn event_connectivity_uses_the_home_star_system() {
        assert_eq!(system_designation("SCEPTURUM-BELT-1"), "SCEPTURUM");
        assert_eq!(system_designation("THYFFAWFF"), "THYFFAWFF");
    }

    #[test]
    fn intent_workflow_kinds_are_goal_oriented() {
        assert_eq!(scan_system_workflow_kind().as_str(), "scan.system");
        assert_eq!(scan_belt_workflow_kind().as_str(), "scan.belt");
        assert_eq!(scan_tour_workflow_kind().as_str(), "scan.tour");
        assert_eq!(
            belt_search_campaign_workflow_kind().as_str(),
            "belt_search.campaign"
        );
        assert_eq!(salvage_workflow_kind().as_str(), "salvage.site");
        assert_eq!(mining_deploy_workflow_kind().as_str(), "mining.deploy");
        assert_eq!(logistics_workflow_kind().as_str(), "logistics.delivery");
        assert_eq!(
            logistics_manifest_workflow_kind().as_str(),
            "logistics.manifest"
        );
        assert_eq!(
            trade_fulfillment_workflow_kind().as_str(),
            "trade.fulfillment"
        );
        assert_eq!(
            blueprint_acquire_workflow_kind().as_str(),
            "blueprint.acquire"
        );
        assert_eq!(exploration_workflow_kind().as_str(), "exploration.frontier");
        assert_eq!(event_delivery_workflow_kind().as_str(), "event.delivery");
        assert_eq!(event_tour_workflow_kind().as_str(), "event.tour");
        assert_eq!(event_campaign_workflow_kind().as_str(), "event.campaign");
        assert_eq!(mining_campaign_workflow_kind().as_str(), "mining.campaign");
        assert_eq!(
            replicant_provision_workflow_kind().as_str(),
            "replicant.provision"
        );
        assert_eq!(
            region_establish_workflow_kind().as_str(),
            "region.establish"
        );
    }

    #[test]
    fn trade_fulfillment_checkpoint_is_restart_safe() {
        let checkpoint = TradeFulfillmentCheckpoint {
            home: Some("SCEPTURUM-BELT-1".to_owned()),
            home_system: Some("SCEPTURUM".to_owned()),
            replicant: Some("CHAT-1".to_owned()),
            criteria: Some(TradeBundle {
                resources: BTreeMap::from([("structural".to_owned(), 200)]),
                ..TradeBundle::default()
            }),
            rewards: Some(TradeBundle {
                devices: BTreeMap::from([("service_bot".to_owned(), 1)]),
                ..TradeBundle::default()
            }),
            purchase_authorized: true,
            purchase_submitted: true,
            purchase_operation: Some("OP-TRADE".to_owned()),
            purchase_observed: true,
            reward_devices: vec!["BOT-1".to_owned()],
            reward_storage: BTreeMap::from([("BOT-1".to_owned(), "stowed".to_owned())]),
            ..TradeFulfillmentCheckpoint::default()
        };

        let encoded = serde_json::to_value(&checkpoint).expect("serialize checkpoint");
        let restored: TradeFulfillmentCheckpoint =
            serde_json::from_value(encoded).expect("restore checkpoint");
        assert_eq!(restored.home.as_deref(), Some("SCEPTURUM-BELT-1"));
        assert_eq!(restored.replicant.as_deref(), Some("CHAT-1"));
        assert!(restored.purchase_authorized);
        assert!(restored.purchase_observed);
        assert_eq!(restored.reward_devices, vec!["BOT-1"]);
        assert_eq!(
            restored.reward_storage.get("BOT-1").map(String::as_str),
            Some("stowed")
        );
    }

    #[test]
    fn blueprint_control_positioning_accepts_any_location_in_target_system() {
        assert!(blueprint_replicant_destination_matches(
            "RHYVENAI-OORT",
            "RHYVENAI",
            true
        ));
        assert!(blueprint_replicant_destination_matches(
            "RHYVENAI-3",
            "RHYVENAI",
            true
        ));
        assert!(!blueprint_replicant_destination_matches(
            "OTHER-3", "RHYVENAI", true
        ));
        assert!(!blueprint_replicant_destination_matches(
            "SCEPTURUM-7-L4",
            "SCEPTURUM-BELT-1",
            false
        ));
    }

    #[test]
    fn blueprint_checkpoint_preserves_irreversible_operation_across_restart() {
        let path = std::env::temp_dir().join(format!(
            "replicant-blueprint-acquire-restart-{}.sqlite",
            uuid::Uuid::new_v4()
        ));
        let repository = WorkflowRepository::open(&path).expect("open workflow repository");
        let workflow = repository
            .create(queued_workflow(
                blueprint_acquire_workflow_kind(),
                BlueprintAcquireIntent {
                    device_type: "service_bot".to_owned(),
                    preferred_region: Some("alpha".to_owned()),
                    requested_by: vec!["blueprint:service_bot".to_owned()],
                    source_device: Some("DEVICE-1".to_owned()),
                    autofactory: Some("FACTORY-1".to_owned()),
                    acquisition_replicant: None,
                    shop: None,
                },
                BlueprintAcquireCheckpoint {
                    source_device: Some("DEVICE-1".to_owned()),
                    autofactory: Some("FACTORY-1".to_owned()),
                    autofactory_location: Some("SCEPTURUM-BELT-1".to_owned()),
                    logistics_child: None,
                    decommission_authorized: true,
                    decommission_submitted: true,
                    decommission_operation: Some("OP-1".to_owned()),
                    blueprint_verified: false,
                    control_escort_required: true,
                    ..BlueprintAcquireCheckpoint::default()
                },
            ))
            .expect("create blueprint workflow");
        let workflow_id = workflow.id;
        drop(repository);

        let repository = WorkflowRepository::open(&path).expect("reopen workflow repository");
        let restored = repository
            .read(workflow_id)
            .expect("read workflow")
            .expect("workflow exists");
        let checkpoint: BlueprintAcquireCheckpoint = restored.checkpoint().expect("checkpoint");
        assert!(checkpoint.decommission_authorized);
        assert!(checkpoint.decommission_submitted);
        assert_eq!(checkpoint.decommission_operation.as_deref(), Some("OP-1"));
        assert_eq!(checkpoint.source_device.as_deref(), Some("DEVICE-1"));
        assert!(checkpoint.control_escort_required);

        drop(repository);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn blueprint_shop_checkpoint_preserves_irreversible_purchase_state() {
        let checkpoint = BlueprintAcquireCheckpoint {
            acquisition_replicant: Some("CHAT-2".to_owned()),
            controller_code: Some("SHOP-1".to_owned()),
            trade_code: Some("TRADE-1".to_owned()),
            shop_location: Some("SOL-4".to_owned()),
            criteria: Some(TradeBundle {
                resources: std::collections::BTreeMap::from([("structural".to_owned(), 200)]),
                devices: std::collections::BTreeMap::from([("compute_core".to_owned(), 1)]),
                unknown: std::collections::BTreeMap::new(),
            }),
            pre_purchase_devices: vec!["OLD-1".to_owned()],
            purchase_authorized: true,
            purchase_submitted: true,
            purchase_operation: Some("OP-TRADE".to_owned()),
            ..BlueprintAcquireCheckpoint::default()
        };

        let encoded = serde_json::to_value(&checkpoint).expect("serialize checkpoint");
        let restored: BlueprintAcquireCheckpoint =
            serde_json::from_value(encoded).expect("restore checkpoint");
        assert!(restored.purchase_authorized);
        assert!(restored.purchase_submitted);
        assert_eq!(restored.purchase_operation.as_deref(), Some("OP-TRADE"));
        assert_eq!(restored.pre_purchase_devices, vec!["OLD-1"]);
        assert_eq!(
            restored
                .criteria
                .as_ref()
                .and_then(|criteria| criteria.devices.get("compute_core")),
            Some(&1)
        );
    }

    #[test]
    fn manifest_intent_builds_one_mixed_delivery_request() {
        let request = manifest_delivery_request(&LogisticsManifestIntent {
            origin: "SCEPTURUM-BELT-1".to_owned(),
            destination: "SCEPTURUM-7-L4".to_owned(),
            resources: [("structural".to_owned(), 400), ("carbon".to_owned(), 80)]
                .into_iter()
                .collect(),
            devices: vec![DeviceRequest {
                quantity: 1,
                device_type: "maintenance_drone".to_owned(),
            }],
            device_codes: vec!["DEVICE-EXACT".to_owned()],
            allow_transport_staging: true,
            purpose: "system hub upkeep".to_owned(),
            ..LogisticsManifestIntent::default()
        });
        assert_eq!(request.resources.get("structural"), Some(&400));
        assert_eq!(request.resources.get("carbon"), Some(&80));
        assert_eq!(request.devices.len(), 1);
        assert_eq!(request.device_codes, ["DEVICE-EXACT"]);
        assert!(request.allow_transport_staging);
    }

    #[test]
    fn logistics_manifest_recovery_metadata_is_backward_compatible_and_exact() {
        let legacy: LogisticsManifestIntent = serde_json::from_value(serde_json::json!({
            "origin": "ORIGIN",
            "destination": "DESTINATION"
        }))
        .expect("legacy manifest");
        assert!(legacy.placement_recovery.is_none());

        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let mut intent = LogisticsManifestIntent {
            origin: "ORIGIN".to_owned(),
            destination: "DESTINATION".to_owned(),
            region: Some("alpha".to_owned()),
            device_codes: vec!["DEVICE-1".to_owned()],
            placement_recovery: Some(PlacementRecoveryMetadata {
                failed_provenance: BTreeMap::from([(
                    "DEVICE-1".to_owned(),
                    vec![provenance.clone()],
                )]),
                release_device_tags: BTreeMap::from([(
                    "DEVICE-1".to_owned(),
                    vec!["mine-m:one".to_owned()],
                )]),
                placement_resolutions: Vec::new(),
            }),
            return_transports: true,
            allow_transport_staging: true,
            ..LogisticsManifestIntent::default()
        };
        validate_placement_recovery_intent(&intent).expect("valid recovery metadata");

        intent.device_codes = vec!["device-1".to_owned()];
        assert!(
            validate_placement_recovery_intent(&intent)
                .expect_err("lowercase exact code rejected")
                .contains("canonical")
        );
    }

    #[test]
    fn logistics_checkpoint_legacy_shape_restores_cleanup_defaults() {
        let checkpoint: LogisticsWorkflowCheckpoint = serde_json::from_value(serde_json::json!({
            "plan": null,
            "started": false
        }))
        .expect("legacy logistics checkpoint");
        assert!(checkpoint.placement_recovery_cleanup.is_empty());

        let checkpoint = LogisticsWorkflowCheckpoint {
            placement_recovery_cleanup: BTreeMap::from([(
                "DEVICE-1".to_owned(),
                PlacementRecoveryCleanup {
                    operation_id: Some("OP-CLEANUP".to_owned()),
                    tags: vec!["mine-m:one".to_owned()],
                    state: Some("submitted".to_owned()),
                },
            )]),
            ..checkpoint
        };
        let restored: LogisticsWorkflowCheckpoint =
            serde_json::from_value(serde_json::to_value(checkpoint).expect("checkpoint"))
                .expect("restored checkpoint");
        let cleanup = restored
            .placement_recovery_cleanup
            .get("DEVICE-1")
            .expect("cleanup state");
        assert_eq!(cleanup.operation_id.as_deref(), Some("OP-CLEANUP"));
        assert_eq!(cleanup.state.as_deref(), Some("submitted"));
        assert_eq!(cleanup.tags, vec!["mine-m:one".to_owned()]);
    }

    #[test]
    fn logistics_manifest_recovery_malformed_projection_is_unknown() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let workflow = repository
            .create(NewWorkflow {
                kind: logistics_manifest_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({
                    "origin": "ORIGIN",
                    "destination": "DESTINATION",
                    "region": "alpha",
                    "device_codes": ["device-1"],
                    "placement_recovery": {
                        "failed_provenance": {},
                        "release_device_tags": {},
                        "placement_resolutions": []
                    }
                }),
                checkpoint: serde_json::json!({"plan": null, "started": false}),
                current_step: None,
                parent_id: None,
            })
            .expect("manifest");
        let projection =
            logistics_manifest_placement(&workflow, &[]).expect("project malformed manifest");
        assert_eq!(
            projection.coverage,
            WorkflowPlacementIntentCoverage::Unknown
        );
        assert!(projection.resolutions.is_empty());
    }

    #[test]
    fn blueprint_transport_manifest_allows_cross_system_carrier_staging() {
        let intent = blueprint_transport_manifest(
            "service_bot",
            "DEVICE-1",
            "REMOTE-BELT-1",
            "SCEPTURUM-BELT-1",
        );

        assert_eq!(intent.origin, "REMOTE-BELT-1");
        assert_eq!(intent.destination, "SCEPTURUM-BELT-1");
        assert_eq!(intent.device_codes, ["DEVICE-1"]);
        assert!(intent.return_transports);
        assert!(intent.allow_transport_staging);
    }
    #[test]
    fn logistics_manifest_recovery_resolves_only_succeeded_delivered_devices() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let intent = LogisticsManifestIntent {
            origin: "ORIGIN".to_owned(),
            destination: "DESTINATION".to_owned(),
            region: Some("alpha".to_owned()),
            device_codes: vec!["DEVICE-1".to_owned()],
            placement_recovery: Some(PlacementRecoveryMetadata {
                failed_provenance: BTreeMap::from([(
                    "DEVICE-1".to_owned(),
                    vec![provenance.clone()],
                )]),
                release_device_tags: BTreeMap::from([(
                    "DEVICE-1".to_owned(),
                    vec!["mine-m:DEVICE-1".to_owned()],
                )]),
                placement_resolutions: vec![WorkflowPlacementResolution {
                    device_code: "DEVICE-1".to_owned(),
                    provenance,
                }],
            }),
            return_transports: true,
            allow_transport_staging: true,
            ..LogisticsManifestIntent::default()
        };
        let config = serde_json::to_value(&intent).expect("intent");
        let new_manifest = || {
            repository.create(NewWorkflow {
                kind: logistics_manifest_workflow_kind(),
                schema_version: 1,
                config: config.clone(),
                checkpoint: serde_json::json!({"plan": null, "started": false}),
                current_step: None,
                parent_id: None,
            })
        };
        let workflow = new_manifest().expect("manifest");

        let failed = repository
            .update(
                workflow.id,
                workflow.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Failed,
                    current_step: None,
                    checkpoint: LogisticsWorkflowCheckpoint::default(),
                    last_error: Some("delivery failed".to_owned()),
                    result: None::<serde_json::Value>,
                },
            )
            .expect("failed manifest");
        let failed_projection =
            logistics_manifest_placement(&failed, &[]).expect("failed projection");
        assert!(failed_projection.resolutions.is_empty());

        let queued = new_manifest().expect("second manifest");
        let workflow = repository
            .update(
                queued.id,
                queued.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Running,
                    current_step: None,
                    checkpoint: LogisticsWorkflowCheckpoint::default(),
                    last_error: None,
                    result: None::<serde_json::Value>,
                },
            )
            .expect("running manifest");
        let succeeded = repository
            .update(
                workflow.id,
                workflow.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Succeeded,
                    current_step: None,
                    checkpoint: LogisticsWorkflowCheckpoint {
                        plan: Some(DeliveryPlan {
                            origin: "ORIGIN".to_owned(),
                            destination: "DESTINATION".to_owned(),
                            payload_devices: vec![PayloadDevice {
                                code: "DEVICE-1".to_owned(),
                                device_type: "service_bot".to_owned(),
                                origin: "ORIGIN".to_owned(),
                            }],
                            ..DeliveryPlan::default()
                        }),
                        started: true,
                        ..LogisticsWorkflowCheckpoint::default()
                    },
                    last_error: None,
                    result: Some(
                        serde_json::to_value(DeliveryReport {
                            devices_delivered: vec!["DEVICE-1".to_owned()],
                            ..DeliveryReport::default()
                        })
                        .expect("delivery report"),
                    ),
                },
            )
            .expect("succeeded manifest");
        let succeeded_projection =
            logistics_manifest_placement(&succeeded, &[]).expect("succeeded projection");
        assert_eq!(succeeded_projection.resolutions.len(), 1);
        assert_eq!(succeeded_projection.resolutions[0].device_code, "DEVICE-1");
        let deployed = succeeded_projection
            .intents
            .iter()
            .find(|intent| {
                intent.subject == WorkflowPlacementIntentSubject::Device("DEVICE-1".to_owned())
            })
            .expect("deployed device intent");
        assert_eq!(deployed.relation, WorkflowPlacementIntentRelation::Deployed);
        assert_eq!(deployed.expected_location.as_deref(), Some("DESTINATION"));
        let queued = new_manifest().expect("partial report manifest");
        let partial = repository
            .update(
                queued.id,
                queued.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Running,
                    current_step: None,
                    checkpoint: LogisticsWorkflowCheckpoint::default(),
                    last_error: None,
                    result: None::<serde_json::Value>,
                },
            )
            .expect("running partial report manifest");
        let partial = repository
            .update(
                partial.id,
                partial.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Succeeded,
                    current_step: None,
                    checkpoint: LogisticsWorkflowCheckpoint::default(),
                    last_error: None,
                    result: Some(
                        serde_json::to_value(DeliveryReport::default())
                            .expect("partial delivery report"),
                    ),
                },
            )
            .expect("partial report update");
        let partial_projection =
            logistics_manifest_placement(&partial, &[]).expect("partial projection");
        assert!(partial_projection.resolutions.is_empty());

        let queued = new_manifest().expect("missing report manifest");
        let missing = repository
            .update(
                queued.id,
                queued.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Running,
                    current_step: None,
                    checkpoint: LogisticsWorkflowCheckpoint::default(),
                    last_error: None,
                    result: None::<serde_json::Value>,
                },
            )
            .expect("running missing report manifest");
        let missing = repository
            .update(
                missing.id,
                missing.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Succeeded,
                    current_step: None,
                    checkpoint: LogisticsWorkflowCheckpoint::default(),
                    last_error: None,
                    result: None::<serde_json::Value>,
                },
            )
            .expect("missing report update");
        let missing_projection =
            logistics_manifest_placement(&missing, &[]).expect("missing projection");
        assert_eq!(
            missing_projection.coverage,
            WorkflowPlacementIntentCoverage::Unknown
        );

        let queued = new_manifest().expect("malformed report manifest");
        let malformed = repository
            .update(
                queued.id,
                queued.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Running,
                    current_step: None,
                    checkpoint: LogisticsWorkflowCheckpoint::default(),
                    last_error: None,
                    result: None::<serde_json::Value>,
                },
            )
            .expect("running malformed report manifest");
        let malformed = repository
            .update(
                malformed.id,
                malformed.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Succeeded,
                    current_step: None,
                    checkpoint: LogisticsWorkflowCheckpoint::default(),
                    last_error: None,
                    result: Some(serde_json::json!({"devices_delivered": "not-a-list"})),
                },
            )
            .expect("malformed report update");
        let malformed_projection =
            logistics_manifest_placement(&malformed, &[]).expect("malformed projection");
        assert_eq!(
            malformed_projection.coverage,
            WorkflowPlacementIntentCoverage::Unknown
        );

        let cancelled = new_manifest().expect("cancelled manifest");
        let cancelled = repository
            .update(
                cancelled.id,
                cancelled.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Cancelled,
                    current_step: None,
                    checkpoint: LogisticsWorkflowCheckpoint::default(),
                    last_error: Some("cancelled".to_owned()),
                    result: None::<serde_json::Value>,
                },
            )
            .expect("cancelled report update");
        let cancelled_projection =
            logistics_manifest_placement(&cancelled, &[]).expect("cancelled projection");
        assert!(cancelled_projection.resolutions.is_empty());
    }
    #[test]
    fn recovery_success_without_matching_durable_plan_is_unknown() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let provenance = WorkflowPlacementProvenance {
            workflow_id: WorkflowId::new(),
            work_item_id: None,
        };
        let intent = recovery_test_intent(provenance, "mine-m:DEVICE-1");
        let workflow = repository
            .create(NewWorkflow {
                kind: logistics_manifest_workflow_kind(),
                schema_version: 1,
                config: serde_json::to_value(intent).expect("recovery intent"),
                checkpoint: LogisticsWorkflowCheckpoint {
                    started: true,
                    ..LogisticsWorkflowCheckpoint::default()
                },
                current_step: Some("delivering".to_owned()),
                parent_id: None,
            })
            .expect("recovery manifest");
        let workflow = repository
            .update(
                workflow.id,
                workflow.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Running,
                    current_step: Some("delivering".to_owned()),
                    checkpoint: LogisticsWorkflowCheckpoint {
                        started: true,
                        ..LogisticsWorkflowCheckpoint::default()
                    },
                    last_error: None,
                    result: None::<serde_json::Value>,
                },
            )
            .expect("running recovery");
        let succeeded = repository
            .update(
                workflow.id,
                workflow.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Succeeded,
                    current_step: Some("delivering".to_owned()),
                    checkpoint: LogisticsWorkflowCheckpoint {
                        started: true,
                        ..LogisticsWorkflowCheckpoint::default()
                    },
                    last_error: None,
                    result: Some(
                        serde_json::to_value(DeliveryReport {
                            devices_delivered: vec!["DEVICE-1".to_owned()],
                            ..DeliveryReport::default()
                        })
                        .expect("delivery report"),
                    ),
                },
            )
            .expect("succeeded recovery");
        let projection =
            logistics_manifest_placement(&succeeded, &[]).expect("recovery projection");
        assert_eq!(
            projection.coverage,
            WorkflowPlacementIntentCoverage::Unknown
        );
        assert!(projection.resolutions.is_empty());
    }

    #[test]
    fn manufacturing_capacity_failure_waits_instead_of_failing_exploration() {
        let error = crate::failure::ClassifiedError::new(
            FailureClass::ManufacturingCapacity,
            std::io::ErrorKind::TimedOut,
            "timed out waiting for autofactory queue capacity",
        );

        assert!(retryable_connectivity_dependency_failure(&error));
    }
    #[test]
    fn belt_search_pool_materializes_unique_unpinned_items() {
        let intent = BeltSearchCampaignIntent {
            systems: (0..24)
                .map(|index| format!("SYSTEM-{index:02}"))
                .chain(["SYSTEM-00".to_owned()])
                .collect(),
            region: "Alpha".into(),
        };
        let specs = belt_search_item_specs(WorkflowId::new(), &intent).expect("materialize items");
        assert_eq!(specs.len(), 24);
        assert!(
            specs
                .iter()
                .all(|spec| spec.requirements_json[0]["capabilities"]
                    == serde_json::json!([
                        "census",
                        "system_scan",
                        OPERATIONAL_REGIONAL_WORKER_CAPABILITY
                    ]))
        );
        assert!(
            specs
                .iter()
                .all(|spec| spec.requirements_json[0]["scope"]["kind"] == "region")
        );
        assert!(
            specs
                .iter()
                .all(|spec| spec.requirements_json[0]["scope"]["value"] == "Alpha")
        );
        assert!(
            specs
                .iter()
                .all(|spec| spec.requirements_json[0]["count"] == 1)
        );
        assert!(
            specs
                .iter()
                .all(|spec| spec.requirements_json[0]["quantity"] == 1)
        );
        assert!(
            serde_json::to_value(intent)
                .expect("serialize intent")
                .get("replicant")
                .is_none()
        );
    }

    #[test]
    fn belt_worker_requires_both_scan_capabilities() {
        let candidate = |capabilities: &[&str]| replicant_workflow::AllocationCandidate {
            resource: ResourceKey::Replicant("R-1".into()),
            kind: "replicant".into(),
            capabilities: capabilities
                .iter()
                .map(|capability| (*capability).to_owned())
                .collect(),
            location: None,
            available_quantity: 1,
            observed_revision: 1,
            observed_at_ms: 0,
        };

        assert!(!belt_worker_candidate(&candidate(&[])));
        assert!(!belt_worker_candidate(&candidate(&["census"])));
        assert!(!belt_worker_candidate(&candidate(&["system_scan"])));
        assert!(!belt_worker_candidate(&candidate(&[
            "census",
            "system_scan"
        ])));
        assert!(belt_worker_candidate(&candidate(&[
            "census",
            "system_scan",
            OPERATIONAL_REGIONAL_WORKER_CAPABILITY
        ])));
    }

    #[test]
    fn scan_tour_waits_while_assigned_worker_is_in_transit() {
        assert_eq!(
            scan_tour_worker_wait_reason(WorkerState::InTransit),
            Some("assigned regional workers are still in transit")
        );
        assert_eq!(scan_tour_worker_wait_reason(WorkerState::Operational), None);
    }

    #[test]
    fn campaign_retry_deadline_uses_earliest_waiting_item() {
        let repository = WorkflowRepository::open_in_memory().expect("open repository");
        let intent = BeltSearchCampaignIntent {
            systems: vec!["SOL".into(), "ALPHA".into()],
            region: "Alpha".into(),
        };
        let campaign = repository
            .create(new_belt_search_campaign_workflow(intent.clone()))
            .expect("create campaign");
        repository
            .reconcile_work_items(
                campaign.id,
                &belt_search_item_specs(campaign.id, &intent).expect("specs"),
                0,
            )
            .expect("reconcile items");
        for retry_at_ms in [120_000, 60_000] {
            let item = repository
                .claim_next_work_item(campaign.id, 0)
                .expect("claim item")
                .expect("item remains");
            repository
                .transition_work_item(
                    item.id,
                    item.state.revision,
                    WorkItemTransition::Waiting {
                        checkpoint_json: None,
                        reason: "fixture resource contention".into(),
                        retry_at_ms: Some(retry_at_ms),
                    },
                    0,
                )
                .expect("wait item");
        }

        assert_eq!(
            campaign_retry_deadline(&repository, campaign.id, 300_000).expect("derive deadline"),
            60_000
        );
    }

    #[test]
    fn idle_campaign_wait_carries_deadline_and_managed_wake_evidence() {
        let intent = campaign_wait_intent(
            "fixture",
            &CAMPAIGN_RESOURCE_EVENT_NAMES,
            Some(300_000),
            IDLE_CAMPAIGN_RETRY_INTERVAL,
        );

        assert_eq!(intent.deadline_millis, Some(300_000));
        assert_eq!(intent.poll_interval_millis, Some(300_000));
        for event_name in [
            "device.attached",
            "device.compacted",
            "device.compacting",
            "device.unfurled",
            "device.unfurling",
            "replicant.transferred",
        ] {
            assert!(
                intent.event_names.iter().any(|name| name == event_name),
                "missing managed wake event {event_name}"
            );
        }
        assert!(campaign_wait_signal_is_actionable(
            WaitSignal::StateRevision
        ));
        assert!(campaign_wait_signal_is_actionable(WaitSignal::Event));
        assert!(!campaign_wait_signal_is_actionable(WaitSignal::Initial));
    }

    #[test]
    fn event_dependency_wait_uses_child_cooldown_without_five_second_polling() {
        let repository = WorkflowRepository::open_in_memory().expect("open repository");
        let child = repository
            .create(new_exploration_workflow(ExplorationIntent {
                target: "ALPHA".into(),
                replicant: Some("R-1".into()),
                hub: Some("SOL".into()),
            }))
            .expect("create dependency");
        let failed = repository
            .update(
                child.id,
                child.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Failed,
                    current_step: Some("awaiting_relay_prerequisites".into()),
                    checkpoint: child.checkpoint::<Value>().expect("dependency checkpoint"),
                    last_error: Some("fixture connectivity dependency".into()),
                    result: None::<Value>,
                },
            )
            .expect("fail dependency");
        let dependencies = BTreeMap::from([("ALPHA".into(), failed.id)]);
        let deadline = event_connectivity_retry_deadline(&repository, &dependencies)
            .expect("derive dependency deadline");
        let expected = failed.updated_at.saturating_add(
            i64::try_from(EVENT_CONNECTIVITY_RETRY_COOLDOWN.as_millis()).unwrap_or(i64::MAX),
        );
        let intent = campaign_wait_intent(
            "fixture dependency",
            &EVENT_CAMPAIGN_DEPENDENCY_EVENT_NAMES,
            deadline,
            EVENT_DEPENDENCY_RECONCILIATION_INTERVAL,
        );

        assert_eq!(intent.deadline_millis, Some(expected));
        assert_eq!(intent.poll_interval_millis, Some(60_000));
        for event_name in ["device.compacted", "device.compacting", "device.unfurling"] {
            assert!(
                intent.event_names.iter().any(|name| name == event_name),
                "missing managed wake event {event_name}"
            );
        }
        assert!(
            intent
                .event_names
                .iter()
                .any(|name| name == "print.completed")
        );
        assert!(
            intent
                .event_names
                .iter()
                .any(|name| name == "relay.activated")
        );
    }
    #[test]
    fn belt_reconciliation_preserves_legacy_item_specs() {
        let repository = WorkflowRepository::open_in_memory().expect("open repository");
        let intent = BeltSearchCampaignIntent {
            systems: vec!["SOL".into()],
            region: "Alpha".into(),
        };
        let campaign = repository
            .create(new_belt_search_campaign_workflow(intent.clone()))
            .expect("create campaign");
        let mut legacy = belt_search_item_specs(campaign.id, &intent).expect("legacy specs");
        legacy[0].requirements_json[0]["capabilities"] = serde_json::json!([]);
        repository
            .reconcile_work_items(campaign.id, &legacy, 0)
            .expect("persist legacy item");

        let desired = belt_search_item_specs(campaign.id, &intent).expect("current specs");
        let compatible = belt_specs_for_reconciliation(&repository, campaign.id, desired)
            .expect("compatibility");
        assert_eq!(compatible, legacy);
        repository
            .reconcile_work_items(campaign.id, &compatible, 1)
            .expect("legacy item remains usable");
    }

    #[test]
    fn belt_search_pool_factory_migrates_version_one_configuration() {
        let repository = WorkflowRepository::open_in_memory().expect("open repository");
        let legacy = repository
            .create(NewWorkflow {
                kind: belt_search_campaign_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({
                    "systems": ["SOL", "ALPHA"],
                    "replicant": "R-1"
                }),
                checkpoint: serde_json::json!({ "replicant": "R-1" }),
                current_step: None,
                parent_id: None,
            })
            .expect("create legacy campaign");
        let factory = BeltSearchCampaignWorkflowFactory::new();
        assert!(factory.supports_schema_version(1));
        let migration = factory
            .migrate(&legacy)
            .expect("migrate legacy")
            .expect("migration exists");
        assert_eq!(
            migration.config()["systems"],
            serde_json::json!(["SOL", "ALPHA"])
        );
        assert_eq!(migration.config()["region"], "");
        assert!(migration.config().get("replicant").is_none());
        assert_eq!(
            migration.checkpoint()["legacy_checkpoint"]["replicant"],
            "R-1"
        );
        repository
            .put_document(
                "director.replicant",
                "R-1",
                &serde_json::json!({ "region": "Alpha" }),
            )
            .expect("persist legacy worker region");
        let migrated_intent: BeltSearchCampaignIntent =
            serde_json::from_value(migration.config().clone()).expect("decode migrated intent");
        let migrated_checkpoint: BeltSearchCampaignCheckpoint =
            serde_json::from_value(migration.checkpoint().clone())
                .expect("decode migrated checkpoint");
        assert_eq!(
            resolve_belt_campaign_region(&repository, &migrated_intent, &migrated_checkpoint,)
                .expect("resolve migrated region")
                .as_deref(),
            Some("Alpha")
        );
        assert_eq!(
            new_belt_search_campaign_workflow(BeltSearchCampaignIntent {
                systems: vec!["SOL".into()],
                region: "Alpha".into(),
            })
            .schema_version,
            2
        );
    }

    #[test]
    fn mining_campaign_factory_migrates_legacy_and_projects_schema_three_routes() {
        let repository = WorkflowRepository::open_in_memory().expect("open repository");
        let legacy = repository
            .create(NewWorkflow {
                kind: mining_campaign_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({
                    "systems": ["SOL"],
                    "region": "delta",
                    "hub": "ROOT-1-L4",
                    "max_concurrency": 1
                }),
                checkpoint: MiningDeployCheckpoint::default(),
                current_step: Some("queued".into()),
                parent_id: None,
            })
            .expect("create accidentally versioned campaign");
        let factory = MiningCampaignWorkflowFactory::new();
        let migration = factory
            .migrate(&legacy)
            .expect("migrate campaign")
            .expect("migration exists");
        assert_eq!(migration.config()["region"], "delta");
        assert_eq!(
            migration.config()["transport_routes"],
            serde_json::json!([])
        );

        let route = AmiTransportRouteIntent {
            system: "SOL".into(),
            collect: "SOL-BELT-1".into(),
            deliver: "ROOT-1-L4".into(),
        };
        let current = repository
            .create(new_mining_campaign_workflow(MiningCampaignIntent {
                systems: vec!["SOL".into()],
                region: "delta".into(),
                hub: "ROOT-1-L4".into(),
                transport_routes: vec![route.clone()],
                max_concurrency: 1,
            }))
            .expect("create schema-three campaign");
        assert_eq!(current.schema_version, 3);
        assert_eq!(
            current
                .config::<MiningCampaignIntent>()
                .expect("config")
                .region,
            "delta"
        );
        let projection = factory.service_intents(&current).expect("service intent");
        assert_eq!(
            projection.coverage,
            replicant_workflow::WorkflowServiceIntentCoverage::Complete
        );
        assert_eq!(projection.intents, vec![route.workflow_service_intent()]);
    }

    #[test]
    fn belt_search_pool_metrics_use_exact_intervals_and_partial_outcome() {
        let repository = WorkflowRepository::open_in_memory().expect("open repository");
        let intent = BeltSearchCampaignIntent {
            systems: vec!["A".into(), "B".into()],
            region: "Alpha".into(),
        };
        let campaign = repository
            .create(new_belt_search_campaign_workflow(intent.clone()))
            .expect("create campaign");
        let specs = belt_search_item_specs(campaign.id, &intent).expect("materialize specs");
        repository
            .reconcile_work_items(campaign.id, &specs, 0)
            .expect("reconcile items");
        let first = repository
            .claim_next_work_item(campaign.id, 0)
            .expect("claim first")
            .expect("first item");
        let second = repository
            .claim_next_work_item(campaign.id, 0)
            .expect("claim second")
            .expect("second item");
        let first = repository
            .start_work_item(first.id, first.state.revision, "R-1", "first", 0)
            .expect("start first");
        let second = repository
            .start_work_item(second.id, second.state.revision, "R-2", "second", 500)
            .expect("start second");
        repository
            .transition_work_item(
                first.id,
                first.state.revision,
                WorkItemTransition::Succeeded {
                    checkpoint_json: None,
                    result_json: Some(serde_json::json!({ "system": "A" })),
                },
                1_500,
            )
            .expect("succeed first");
        repository
            .transition_work_item(
                second.id,
                second.state.revision,
                WorkItemTransition::Failed {
                    error: "permanent fixture".into(),
                    result_json: None,
                },
                2_000,
            )
            .expect("fail second");
        let metrics =
            belt_search_pool_metrics(&repository, campaign.id, 0, 2_000).expect("metrics");
        assert_eq!(metrics.effective_parallelism, 1.5);
        assert_eq!(metrics.peak_overlap, 2);
        assert_eq!(metrics.unique_workers, 2);
        assert_eq!(
            metrics.campaign_outcome,
            Some(replicant_workflow::CampaignOutcome::PartialSuccess)
        );
    }

    #[test]
    fn belt_search_pool_assigns_four_workers_and_isolates_outcomes() {
        let repository = WorkflowRepository::open_in_memory().expect("open repository");
        let intent = BeltSearchCampaignIntent {
            systems: (0..24).map(|index| format!("SYSTEM-{index:02}")).collect(),
            region: "Alpha".into(),
        };
        let campaign = repository
            .create(new_belt_search_campaign_workflow(intent.clone()))
            .expect("create campaign");
        repository
            .reconcile_work_items(
                campaign.id,
                &belt_search_item_specs(campaign.id, &intent).expect("specs"),
                0,
            )
            .expect("reconcile");
        let candidates = (0..4)
            .map(|index| replicant_workflow::AllocationCandidate {
                resource: ResourceKey::Replicant(format!("R-{index}")),
                kind: "replicant".into(),
                capabilities: vec![
                    "census".into(),
                    "system_scan".into(),
                    OPERATIONAL_REGIONAL_WORKER_CAPABILITY.into(),
                ],
                location: Some(replicant_workflow::AllocationLocation {
                    region: Some("Alpha".into()),
                    ..replicant_workflow::AllocationLocation::default()
                }),
                available_quantity: 1,
                observed_revision: 1,
                observed_at_ms: 0,
            })
            .collect::<Vec<_>>();
        let mut running = Vec::new();
        for index in 0..4 {
            let assigned = repository
                .claim_next_work_item(campaign.id, 0)
                .expect("claim")
                .expect("item");
            let allocations = repository
                .allocate_requirements(assigned.id, assigned.state.revision, &candidates)
                .expect("allocate");
            let worker = allocation_worker(&allocations).expect("worker");
            running.push(
                repository
                    .start_work_item(
                        assigned.id,
                        assigned.state.revision,
                        &worker,
                        &format!("grant-{index}"),
                        i64::from(index),
                    )
                    .expect("start"),
            );
        }
        let worker_count = running
            .iter()
            .flat_map(|item| {
                repository
                    .list_work_item_attempts(item.id)
                    .expect("attempts")
            })
            .map(|attempt| attempt.worker_identity)
            .collect::<BTreeSet<_>>()
            .len();
        assert_eq!(worker_count, 4);
        let transitions = [
            WorkItemTransition::Skipped {
                reason: "system already explored".into(),
                result_json: None,
            },
            WorkItemTransition::Reclaimed {
                checkpoint_json: Some(serde_json::json!({ "safe": true })),
            },
            belt_item_failure_transition(&crate::failure::ClassifiedError::permanent(
                FailureClass::DeviceTargetMissing,
                std::io::ErrorKind::NotFound,
                "structured missing worker fixture",
            )),
            WorkItemTransition::Succeeded {
                checkpoint_json: None,
                result_json: Some(serde_json::json!({ "system": "SYSTEM-03" })),
            },
        ];
        for (index, transition) in transitions.into_iter().enumerate() {
            repository
                .transition_work_item(
                    running[index].id,
                    running[index].state.revision,
                    transition,
                    10 + i64::try_from(index).expect("index fits"),
                )
                .expect("transition item");
        }
        assert_eq!(
            repository
                .read_work_item(running[1].id)
                .expect("read reclaimed")
                .expect("item exists")
                .state
                .status,
            replicant_workflow::WorkItemStatus::Pending
        );
        let reclaimed = repository
            .claim_next_work_item(campaign.id, 20)
            .expect("claim reclaimed item")
            .expect("reclaimed item is pending");
        let allocations = repository
            .allocate_requirements(reclaimed.id, reclaimed.state.revision, &candidates)
            .expect("reclaimed item can allocate released worker");
        assert!(allocation_worker(&allocations).is_some());
        assert_eq!(
            repository
                .read_work_item(running[3].id)
                .expect("read sibling")
                .expect("item exists")
                .state
                .status,
            replicant_workflow::WorkItemStatus::Succeeded
        );
    }
    #[test]
    fn belt_capability_mismatch_reclaims_with_checkpoint_but_arbitrary_400_retries() {
        let mut mismatch_details = replicant_client::ErrorDetails::default();
        mismatch_details.message = Some("device does not support system-scan".into());
        let mismatch = replicant_client::Error::Contract {
            status: 400,
            details: Box::new(mismatch_details),
        };
        assert!(belt_capability_mismatch(&mismatch));
        assert_eq!(
            belt_item_failure_transition_with_checkpoint(
                &mismatch,
                Some(serde_json::json!({"safe": true}))
            ),
            WorkItemTransition::Reclaimed {
                checkpoint_json: Some(serde_json::json!({"safe": true}))
            }
        );

        let mut arbitrary_details = replicant_client::ErrorDetails::default();
        arbitrary_details.message = Some("invalid belt search destination".into());
        let arbitrary = replicant_client::Error::Contract {
            status: 400,
            details: Box::new(arbitrary_details),
        };
        assert!(!belt_capability_mismatch(&arbitrary));
        assert!(matches!(
            belt_item_failure_transition(&arbitrary),
            WorkItemTransition::RetryableFailure { .. }
        ));
    }

    #[tokio::test]
    async fn census_capability_400_reclaims_once_and_releases_worker() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/replicants/R-1/stars/SOL"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "Device does not have census capability"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
        let intent = BeltSearchCampaignIntent {
            systems: vec!["SOL".into()],
            region: "Alpha".into(),
        };
        let campaign = repository
            .create(new_belt_search_campaign_workflow(intent.clone()))
            .expect("create campaign");
        repository
            .reconcile_work_items(
                campaign.id,
                &belt_search_item_specs(campaign.id, &intent).expect("specs"),
                0,
            )
            .expect("reconcile");
        let item = repository
            .claim_next_work_item(campaign.id, 0)
            .expect("claim item")
            .expect("item available");
        let candidate = |worker: &str| replicant_workflow::AllocationCandidate {
            resource: ResourceKey::Replicant(worker.into()),
            kind: "replicant".into(),
            capabilities: vec![
                "census".into(),
                "system_scan".into(),
                OPERATIONAL_REGIONAL_WORKER_CAPABILITY.into(),
            ],
            location: Some(replicant_workflow::AllocationLocation {
                region: Some("Alpha".into()),
                ..replicant_workflow::AllocationLocation::default()
            }),
            available_quantity: 1,
            observed_revision: 1,
            observed_at_ms: 0,
        };
        let allocations = repository
            .allocate_requirements(item.id, item.state.revision, &[candidate("R-1")])
            .expect("allocate stale worker");
        let worker = allocation_worker(&allocations).expect("worker");
        let running = repository
            .start_work_item(item.id, item.state.revision, &worker, "grant-1", 1)
            .expect("start item");
        let running = repository
            .transition_work_item(
                running.id,
                running.state.revision,
                WorkItemTransition::CheckpointCommitted {
                    checkpoint_json: serde_json::json!({"safe": true}),
                },
                2,
            )
            .expect("checkpoint item");

        assert_eq!(
            run_belt_item(
                repository.clone(),
                client.clone(),
                running,
                worker,
                "SOL".into(),
            )
            .await
            .expect("capability mismatch stays item-local"),
            Some("R-1".into())
        );
        let reclaimed = repository
            .read_work_item(item.id)
            .expect("read item")
            .expect("item exists");
        assert_eq!(
            reclaimed.state.status,
            replicant_workflow::WorkItemStatus::Pending
        );
        assert_eq!(
            reclaimed.state.checkpoint_json,
            Some(serde_json::json!({"safe": true}))
        );
        let reassigned = repository
            .allocate_requirements(reclaimed.id, reclaimed.state.revision, &[candidate("R-2")])
            .expect("released item accepts replacement worker");
        assert_eq!(allocation_worker(&reassigned).as_deref(), Some("R-2"));
        client.close().await.expect("close client");
    }
    #[test]
    fn every_automation_factory_projects_current_schema_or_explicitly_unknown() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let cases = [
            (
                scan_system_workflow_kind(),
                serde_json::json!({"system":"SYS"}),
                serde_json::json!({}),
                1,
            ),
            (
                scan_belt_workflow_kind(),
                serde_json::json!({"system":"SYS"}),
                serde_json::json!({}),
                1,
            ),
            (
                scan_tour_workflow_kind(),
                serde_json::json!({"center":"SYS"}),
                serde_json::json!({}),
                1,
            ),
            (
                belt_search_campaign_workflow_kind(),
                serde_json::json!({"systems":[],"region":"alpha"}),
                serde_json::json!({"legacy_checkpoint":null}),
                2,
            ),
            (
                salvage_workflow_kind(),
                serde_json::json!({"location":"SYS"}),
                serde_json::json!({}),
                1,
            ),
            (
                salvage_recovery_workflow_kind(),
                serde_json::json!({"region":"alpha","home":"HOME"}),
                serde_json::json!({}),
                1,
            ),
            (
                mining_deploy_workflow_kind(),
                serde_json::json!({"system":"SYS"}),
                serde_json::json!({}),
                1,
            ),
            (
                mining_campaign_workflow_kind(),
                serde_json::json!({"systems":[],"region":"alpha","hub":"HUB","transport_routes":[]}),
                serde_json::json!({"mission":null,"migration_worker":null,"started":false}),
                3,
            ),
            (
                logistics_workflow_kind(),
                serde_json::json!({"origin":"ORIGIN","destination":"HOME"}),
                serde_json::json!({"plan":null,"started":false}),
                1,
            ),
            (
                logistics_manifest_workflow_kind(),
                serde_json::json!({"origin":"ORIGIN","destination":"HOME"}),
                serde_json::json!({"plan":null,"started":false}),
                1,
            ),
            (
                trade_fulfillment_workflow_kind(),
                serde_json::json!({"controller":"CTRL","trade_code":"TRADE","shop_location":"SHOP","home":"HOME"}),
                serde_json::json!({}),
                1,
            ),
            (
                blueprint_acquire_workflow_kind(),
                serde_json::json!({"device_type":"survey_drone"}),
                serde_json::json!({}),
                1,
            ),
            (
                exploration_workflow_kind(),
                serde_json::json!({"target":"SYS"}),
                serde_json::json!({"replicant":null,"hub":null,"state":null}),
                1,
            ),
            (
                event_delivery_workflow_kind(),
                serde_json::json!({"event":"EVENT"}),
                serde_json::json!({"replicant":null,"home":null,"plan_json":null,"ready":false,"connectivity_workflows":{},"replan_after_connectivity":false}),
                1,
            ),
            (
                event_tour_workflow_kind(),
                serde_json::json!({"event":"EVENT"}),
                serde_json::json!({"delivery_child":null,"replicant":null,"plan_json":null}),
                1,
            ),
            (
                event_campaign_workflow_kind(),
                serde_json::json!({"region":"alpha","home":"HOME"}),
                serde_json::json!({"replicant":null,"home":null,"archive":null,"connectivity_workflows":{},"replan_after_connectivity":false}),
                2,
            ),
            (
                observatory_workflow_kind(),
                serde_json::json!({}),
                Value::Null,
                1,
            ),
            (
                replicant_provision_workflow_kind(),
                serde_json::json!({"region":"alpha","home":"HOME","source_replicant":"REP"}),
                serde_json::json!({"tag":null,"manufacturing":null,"matrix":null,"cradle":null,"stowed":false,"new_replicant":null}),
                1,
            ),
            (
                region_establish_workflow_kind(),
                serde_json::json!({"region":"alpha","landing_star":"STAR","source_hub":"HUB","operator":"OP","explorer":"EXP"}),
                serde_json::json!({"mission_json":null}),
                1,
            ),
        ];
        let mut registry = WorkflowRegistry::new();
        register(&mut registry).expect("automation registry");
        for (kind, config, checkpoint, schema_version) in cases {
            let workflow = repository
                .create(NewWorkflow {
                    kind: kind.clone(),
                    schema_version,
                    config,
                    checkpoint,
                    current_step: None,
                    parent_id: None,
                })
                .expect("current workflow");
            let projection = registry
                .resolve(&workflow)
                .expect("registered automation factory")
                .placement_intents(&workflow, &[])
                .expect("current typed state");
            assert_eq!(
                projection.coverage,
                WorkflowPlacementIntentCoverage::Complete,
                "factory {kind} must not inherit unknown for current typed state"
            );
        }
    }
    #[test]
    fn placement_projectors_keep_untyped_current_items_unknown() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let workflow = repository
            .create(NewWorkflow {
                kind: logistics_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({
                    "origin": "ORIGIN",
                    "destination": "HOME",
                    "payload_kind": "device",
                    "item": "survey_drone",
                    "quantity": 1,
                    "resources": {},
                    "devices": [],
                    "device_tags": [],
                    "return_transports": false
                }),
                checkpoint: serde_json::json!({
                    "plan": null,
                    "started": false
                }),
                current_step: None,
                parent_id: None,
            })
            .expect("logistics workflow");
        let item = WorkItem {
            id: replicant_workflow::WorkItemId::default(),
            spec: WorkItemSpec {
                workflow_id: workflow.id,
                dedupe_key: "unknown".into(),
                kind: logistics_workflow_kind(),
                sort_key: "unknown".into(),
                payload_json: serde_json::json!({"device_code": "D-1"}),
                preconditions_json: Value::Array(Vec::new()),
                requirements_json: Value::Array(Vec::new()),
                deadline_at_ms: None,
            },
            state: replicant_workflow::WorkItemState {
                status: WorkItemStatus::Pending,
                checkpoint_json: None,
                result_json: None,
                last_error: None,
                attempt_count: 0,
                consecutive_failure_count: 0,
                next_attempt_at_ms: None,
                ever_started: false,
                created_at_ms: 0,
                updated_at_ms: 0,
                revision: 0,
            },
        };
        assert!(
            LogisticsWorkflowFactory::new()
                .placement_intents(&workflow, &[item])
                .is_err()
        );
    }

    #[test]
    fn failed_scan_before_directive_has_no_custody_evidence() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let workflow = repository
            .create(NewWorkflow {
                kind: scan_system_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({
                    "system": "SYS",
                    "controller": "CTRL",
                    "recall": true
                }),
                checkpoint: serde_json::to_value(ControllerWorkflowCheckpoint {
                    controller: Some("CTRL".into()),
                    ..ControllerWorkflowCheckpoint::default()
                })
                .expect("checkpoint"),
                current_step: None,
                parent_id: None,
            })
            .expect("scan workflow");
        let running = repository
            .update(
                workflow.id,
                workflow.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Running,
                    current_step: None,
                    checkpoint: ControllerWorkflowCheckpoint {
                        controller: Some("CTRL".into()),
                        ..ControllerWorkflowCheckpoint::default()
                    },
                    last_error: None,
                    result: Option::<Value>::None,
                },
            )
            .expect("running workflow");
        let failed = repository
            .update(
                running.id,
                running.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Failed,
                    current_step: None,
                    checkpoint: ControllerWorkflowCheckpoint {
                        controller: Some("CTRL".into()),
                        ..ControllerWorkflowCheckpoint::default()
                    },
                    last_error: Some("controller unavailable".into()),
                    result: Option::<Value>::None,
                },
            )
            .expect("failed workflow");
        let projection = ScanSystemWorkflowFactory::new()
            .placement_intents(&failed, &[])
            .expect("typed failed projection");
        assert!(projection.intents.is_empty());

        let after_custody = repository
            .create(NewWorkflow {
                kind: scan_system_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({
                    "system": "SYS",
                    "controller": "CTRL",
                    "recall": true
                }),
                checkpoint: serde_json::to_value(ControllerWorkflowCheckpoint {
                    controller: Some("CTRL".into()),
                    directive_set: true,
                    ..ControllerWorkflowCheckpoint::default()
                })
                .expect("checkpoint"),
                current_step: None,
                parent_id: None,
            })
            .expect("second scan workflow");
        let running = repository
            .update(
                after_custody.id,
                after_custody.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Running,
                    current_step: None,
                    checkpoint: ControllerWorkflowCheckpoint {
                        controller: Some("CTRL".into()),
                        directive_set: true,
                        ..ControllerWorkflowCheckpoint::default()
                    },
                    last_error: None,
                    result: Option::<Value>::None,
                },
            )
            .expect("running workflow");
        let failed = repository
            .update(
                running.id,
                running.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Failed,
                    current_step: None,
                    checkpoint: ControllerWorkflowCheckpoint {
                        controller: Some("CTRL".into()),
                        directive_set: true,
                        ..ControllerWorkflowCheckpoint::default()
                    },
                    last_error: Some("directive failed".into()),
                    result: Option::<Value>::None,
                },
            )
            .expect("failed after directive");
        let projection = ScanSystemWorkflowFactory::new()
            .placement_intents(&failed, &[])
            .expect("typed failed projection");
        assert_eq!(projection.intents.len(), 1);
        assert_eq!(
            projection.intents[0].relation,
            WorkflowPlacementIntentRelation::Claimed
        );
    }
    #[test]
    fn mining_terminal_projection_requires_site_or_print_custody() {
        fn failed_mining(site_phase: &str) -> replicant_workflow::WorkflowInstance {
            let repository = WorkflowRepository::open_in_memory().expect("repository");
            let workflow = repository
                .create(NewWorkflow {
                    kind: mining_campaign_workflow_kind(),
                    schema_version: 2,
                    config: serde_json::json!({
                        "systems": ["SYS"],
                        "region": "alpha",
                        "hub": "HUB",
                        "max_concurrency": 1
                    }),
                    checkpoint: serde_json::json!({
                        "mission": {
                            "version": 1,
                            "mission_id": "M",
                            "mission_tag": "mine:M",
                            "legacy_mission_tags": [],
                            "phase": "deploying_sites",
                            "selected_replicant": "REP",
                            "hub_location": "HUB",
                            "sites": [{
                                "system": "SYS",
                                "belt": "BELT",
                                "density": "high",
                                "tag": "mine:site",
                                "phase": site_phase,
                                "assets": {
                                    "mining_controller": null,
                                    "mining_drones": [],
                                    "survey_controller": null,
                                    "survey_drones": [],
                                    "maintenance_drone": null,
                                    "system_ward": null
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
                        },
                        "migration_worker": null,
                        "started": true
                    }),
                    current_step: None,
                    parent_id: None,
                })
                .expect("mining workflow");
            let running = repository
                .update(
                    workflow.id,
                    workflow.revision,
                    replicant_workflow::WorkflowState {
                        status: WorkflowStatus::Running,
                        current_step: None,
                        checkpoint: workflow
                            .checkpoint::<MiningCampaignCheckpoint>()
                            .expect("checkpoint"),
                        last_error: None,
                        result: Option::<Value>::None,
                    },
                )
                .expect("running workflow");
            repository
                .update(
                    running.id,
                    running.revision,
                    replicant_workflow::WorkflowState {
                        status: WorkflowStatus::Failed,
                        current_step: None,
                        checkpoint: running
                            .checkpoint::<MiningCampaignCheckpoint>()
                            .expect("checkpoint"),
                        last_error: Some("fixture failure".into()),
                        result: Option::<Value>::None,
                    },
                )
                .expect("failed workflow")
        }

        let before = failed_mining("ready");
        let before_projection = MiningCampaignWorkflowFactory::new()
            .placement_intents(&before, &[])
            .expect("before-custody projection");
        assert!(before_projection.intents.is_empty());

        let after = failed_mining("outbound");
        let after_projection = MiningCampaignWorkflowFactory::new()
            .placement_intents(&after, &[])
            .expect("after-custody projection");
        assert!(
            after_projection
                .intents
                .iter()
                .any(|intent| { intent.relation == WorkflowPlacementIntentRelation::Transported })
        );
    }

    #[test]
    fn placement_projectors_keep_legacy_belt_state_unknown() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let workflow = repository
            .create(NewWorkflow {
                kind: belt_search_campaign_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({"systems": [], "region": "alpha"}),
                checkpoint: Value::Null,
                current_step: None,
                parent_id: None,
            })
            .expect("legacy belt workflow");
        assert!(
            BeltSearchCampaignWorkflowFactory::new()
                .placement_intents(&workflow, &[])
                .is_err()
        );
    }
    fn recovery_test_metadata(
        provenance: WorkflowPlacementProvenance,
        tag: &str,
    ) -> PlacementRecoveryMetadata {
        PlacementRecoveryMetadata {
            failed_provenance: BTreeMap::from([("DEVICE-1".into(), vec![provenance.clone()])]),
            release_device_tags: BTreeMap::from([("DEVICE-1".into(), vec![tag.into()])]),
            placement_resolutions: vec![WorkflowPlacementResolution {
                device_code: "DEVICE-1".into(),
                provenance,
            }],
        }
    }

    fn seed_failed_recovery_source(
        repository: &WorkflowRepository,
        tag: &str,
    ) -> WorkflowPlacementProvenance {
        let source = repository
            .create(NewWorkflow {
                kind: logistics_manifest_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({
                    "origin": "ALPHA-BELT-1",
                    "destination": "ALPHA-HUB",
                    "device_codes": ["DEVICE-1"],
                    "device_tags": [tag]
                }),
                checkpoint: LogisticsWorkflowCheckpoint {
                    started: true,
                    ..LogisticsWorkflowCheckpoint::default()
                },
                current_step: None,
                parent_id: None,
            })
            .expect("failed recovery source");
        let source = repository
            .update(
                source.id,
                source.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Failed,
                    current_step: None,
                    checkpoint: LogisticsWorkflowCheckpoint {
                        started: true,
                        ..LogisticsWorkflowCheckpoint::default()
                    },
                    last_error: Some("fixture transport failure".to_owned()),
                    result: None::<Value>,
                },
            )
            .expect("terminal failed recovery source");
        WorkflowPlacementProvenance {
            workflow_id: source.id,
            work_item_id: None,
        }
    }

    fn recovery_test_intent(
        provenance: WorkflowPlacementProvenance,
        tag: &str,
    ) -> LogisticsManifestIntent {
        LogisticsManifestIntent {
            origin: "ALPHA-BELT-1".into(),
            destination: "ALPHA-HUB".into(),
            region: Some("alpha".into()),
            device_codes: vec!["DEVICE-1".into()],
            placement_recovery: Some(recovery_test_metadata(provenance, tag)),
            return_transports: true,
            allow_transport_staging: true,
            ..LogisticsManifestIntent::default()
        }
    }

    fn authorize_recovery_test(
        repository: &WorkflowRepository,
        workflow_id: WorkflowId,
        intent: &LogisticsManifestIntent,
    ) {
        let metadata = intent
            .placement_recovery
            .clone()
            .expect("recovery metadata");
        let authorization = placement_recovery_authorization(
            workflow_id,
            intent.region.as_deref().expect("recovery region"),
            intent.device_codes.first().expect("recovery device"),
            &intent.origin,
            &intent.destination,
            metadata,
        );
        write_placement_recovery_authorization(repository, &authorization)
            .expect("write recovery authorization");
    }

    async fn tick_until_terminal(
        supervisor: &replicant_workflow::WorkflowSupervisor,
        repository: &WorkflowRepository,
        workflow_id: WorkflowId,
    ) {
        for _ in 0..64 {
            supervisor.tick().await.expect("supervisor tick");
            if repository
                .read(workflow_id)
                .expect("workflow")
                .is_some_and(|workflow| workflow.status.is_terminal())
            {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("workflow did not reach a terminal state");
    }

    #[tokio::test]
    async fn logistics_manifest_recovery_rejects_metadata_and_claims_before_transport() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/DEVICE-1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/v1/devices/DEVICE-1"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let client = test_client_at(&server).await;
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
        let malformed = repository
            .create(NewWorkflow {
                kind: logistics_manifest_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({
                    "origin": "ALPHA-BELT-1",
                    "destination": "ALPHA-HUB",
                    "region": "alpha",
                    "device_codes": ["DEVICE-1"],
                    "placement_recovery": {
                        "failed_provenance": {},
                        "release_device_tags": {
                            "DEVICE-1": ["mine-m:DEVICE-1"]
                        },
                        "placement_resolutions": []
                    }
                }),
                checkpoint: LogisticsWorkflowCheckpoint::default(),
                current_step: None,
                parent_id: None,
            })
            .expect("malformed recovery manifest");

        let blocker = repository
            .create(NewWorkflow {
                kind: logistics_manifest_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({"origin": "X", "destination": "Y"}),
                checkpoint: LogisticsWorkflowCheckpoint::default(),
                current_step: None,
                parent_id: None,
            })
            .expect("claim blocker");
        let blocker = repository
            .update(
                blocker.id,
                blocker.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Running,
                    current_step: Some("claim-holder".into()),
                    checkpoint: LogisticsWorkflowCheckpoint::default(),
                    last_error: None,
                    result: None::<Value>,
                },
            )
            .expect("running claim blocker");
        repository
            .update(
                blocker.id,
                blocker.revision,
                replicant_workflow::WorkflowState {
                    status: WorkflowStatus::Waiting,
                    current_step: Some("claim-holder".into()),
                    checkpoint: LogisticsWorkflowCheckpoint::default(),
                    last_error: None,
                    result: None::<Value>,
                },
            )
            .expect("waiting claim blocker");
        repository
            .acquire_claim(blocker.id, ResourceKey::Device("DEVICE-1".into()))
            .expect("claim blocker resource");

        let claim_conflict = repository
            .create(NewWorkflow {
                kind: logistics_manifest_workflow_kind(),
                schema_version: 1,
                config: serde_json::to_value(recovery_test_intent(
                    WorkflowPlacementProvenance {
                        workflow_id: WorkflowId::new(),
                        work_item_id: None,
                    },
                    "mine-m:DEVICE-1",
                ))
                .expect("recovery intent"),
                checkpoint: LogisticsWorkflowCheckpoint::default(),
                current_step: None,
                parent_id: None,
            })
            .expect("claim conflict manifest");

        let mut registry = WorkflowRegistry::new();
        register(&mut registry).expect("register runtime workflows");
        let supervisor = replicant_workflow::WorkflowSupervisor::with_managed_client(
            repository.clone(),
            Arc::new(registry),
            client.clone(),
        );
        supervisor.tick().await.expect("initial supervisor tick");
        tokio::task::yield_now().await;
        supervisor.tick().await.expect("claim supervisor tick");

        assert_eq!(
            repository
                .read(malformed.id)
                .expect("malformed row")
                .expect("malformed workflow")
                .status,
            WorkflowStatus::Failed
        );
        assert_eq!(
            repository
                .read(claim_conflict.id)
                .expect("claim conflict row")
                .expect("claim conflict workflow")
                .status,
            WorkflowStatus::Failed
        );
        server.verify().await;
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn logistics_manifest_recovery_configure_rejection_never_plans_transport() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/DEVICE-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "DEVICE-1",
                "device_type": "survey_drone",
                "location": "ALPHA-BELT-1",
                "status": "idle",
                "tags": ["mine-m:DEVICE-1"]
            })))
            .expect(3)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/v1/devices/DEVICE-1"))
            .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
                "error": "configuration rejected"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/inventory"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/blueprints"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let client = test_client_at(&server).await;
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
        let provenance = seed_failed_recovery_source(&repository, "mine-m:DEVICE-1");
        let workflow = repository
            .create(NewWorkflow {
                kind: logistics_manifest_workflow_kind(),
                schema_version: 1,
                config: recovery_test_intent(provenance, "mine-m:DEVICE-1"),
                checkpoint: LogisticsWorkflowCheckpoint::default(),
                current_step: None,
                parent_id: None,
            })
            .expect("recovery manifest");
        authorize_recovery_test(
            &repository,
            workflow.id,
            &workflow.config().expect("recovery intent"),
        );
        let mut registry = WorkflowRegistry::new();
        registry
            .register(Arc::new(LogisticsManifestWorkflowFactory::new()))
            .expect("register manifest workflow");
        let supervisor = replicant_workflow::WorkflowSupervisor::with_managed_client(
            repository.clone(),
            Arc::new(registry),
            client.clone(),
        );
        tick_until_terminal(&supervisor, &repository, workflow.id).await;
        let failed = repository
            .read(workflow.id)
            .expect("manifest row")
            .expect("manifest workflow");
        assert_eq!(failed.status, WorkflowStatus::Failed);
        assert!(
            failed
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("400") || error.contains("rejected")),
            "unexpected configure rejection error: {:?}",
            failed.last_error
        );
        assert!(
            failed
                .checkpoint::<LogisticsWorkflowCheckpoint>()
                .expect("checkpoint")
                .plan
                .is_none(),
            "configure rejection must precede transport planning"
        );
        server.verify().await;
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn logistics_manifest_recovery_pending_cleanup_waits_before_planning() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/DEVICE-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "DEVICE-1",
                "device_type": "survey_drone",
                "location": "ALPHA-BELT-1",
                "status": "idle",
                "tags": ["mine-m:DEVICE-1"]
            })))
            .up_to_n_times(1)
            .with_priority(1)
            .mount(&server)
            .await;
        // Reconciliation sees the reserved tag still present, so a successful
        // HTTP response is not completion evidence.
        Mock::given(method("GET"))
            .and(path("/v1/devices/DEVICE-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "DEVICE-1",
                "device_type": "survey_drone",
                "location": "ALPHA-BELT-1",
                "status": "idle",
                "tags": ["mine-m:DEVICE-1"]
            })))
            .with_priority(2)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/v1/devices/DEVICE-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/inventory"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let client = test_client_at(&server).await;
        let directory = std::env::temp_dir().join(format!(
            "replicant-recovery-cleanup-{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::create_dir_all(&directory).expect("recovery cleanup test directory");
        let database = directory.join("workflow.sqlite");
        let repository = Arc::new(WorkflowRepository::open(&database).expect("repository"));
        let provenance = seed_failed_recovery_source(&repository, "mine-m:DEVICE-1");
        let workflow = repository
            .create(NewWorkflow {
                kind: logistics_manifest_workflow_kind(),
                schema_version: 1,
                config: recovery_test_intent(provenance, "mine-m:DEVICE-1"),
                checkpoint: LogisticsWorkflowCheckpoint::default(),
                current_step: None,
                parent_id: None,
            })
            .expect("recovery manifest");
        authorize_recovery_test(
            &repository,
            workflow.id,
            &workflow.config().expect("recovery intent"),
        );
        let mut registry = WorkflowRegistry::new();
        registry
            .register(Arc::new(LogisticsManifestWorkflowFactory::new()))
            .expect("register recovery manifest");
        let supervisor = replicant_workflow::WorkflowSupervisor::with_managed_client(
            repository.clone(),
            Arc::new(registry),
            client.clone(),
        );
        for _ in 0..64 {
            supervisor.tick().await.expect("supervisor tick");
            if repository
                .read(workflow.id)
                .expect("workflow")
                .is_some_and(|workflow| workflow.status == WorkflowStatus::Waiting)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let waiting = repository
            .read(workflow.id)
            .expect("waiting row")
            .expect("waiting workflow");
        assert_eq!(
            waiting.status,
            WorkflowStatus::Waiting,
            "unexpected pending cleanup error: {:?}",
            waiting.last_error
        );
        assert_eq!(
            waiting.current_step.as_deref(),
            Some("waiting_for_recovery_cleanup")
        );
        let checkpoint = waiting
            .checkpoint::<LogisticsWorkflowCheckpoint>()
            .expect("waiting checkpoint");
        assert!(checkpoint.plan.is_none());
        assert_eq!(
            checkpoint
                .placement_recovery_cleanup
                .get("DEVICE-1")
                .and_then(|cleanup| cleanup.state.as_deref()),
            Some("pending")
        );
        assert_eq!(
            checkpoint
                .placement_recovery_cleanup
                .get("DEVICE-1")
                .and_then(|cleanup| cleanup.operation_id.as_deref()),
            Some(recovery_configure_operation_id(workflow.id, "DEVICE-1").as_str())
        );
        drop(checkpoint);
        drop(waiting);
        drop(supervisor);
        drop(repository);
        let reopened = WorkflowRepository::open(&database).expect("reopened repository");
        let resumed = reopened
            .read(workflow.id)
            .expect("reopened cleanup row")
            .expect("persisted cleanup workflow");
        let resumed_checkpoint = resumed
            .checkpoint::<LogisticsWorkflowCheckpoint>()
            .expect("reopened cleanup checkpoint");
        assert_eq!(resumed.status, WorkflowStatus::Waiting);
        assert!(resumed_checkpoint.plan.is_none());
        assert_eq!(
            resumed_checkpoint
                .placement_recovery_cleanup
                .get("DEVICE-1")
                .and_then(|cleanup| cleanup.operation_id.as_deref()),
            Some(recovery_configure_operation_id(workflow.id, "DEVICE-1").as_str())
        );
        drop(reopened);
        server.verify().await;
        client.close().await.expect("close client");
        std::fs::remove_dir_all(directory).expect("remove recovery cleanup test directory");
    }

    #[tokio::test]
    async fn logistics_manifest_recovery_smoke_executes_delivery_and_rebuilds_placement_snapshot() {
        let server = MockServer::start().await;
        let client = test_client_at(&server).await;
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("repository"));

        // First execute a real manifest that fails during planning. Its
        // factory projection, rather than a hand-authored terminal row, is
        // the retained exact Device+DeviceTag failed episode.
        let failed = repository
            .create(NewWorkflow {
                kind: logistics_manifest_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({
                    "origin": "ALPHA-BELT-1",
                    "destination": "ALPHA-HUB",
                    "device_codes": ["DEVICE-1"],
                    "device_tags": ["mine-m:DEVICE-1"]
                }),
                checkpoint: LogisticsWorkflowCheckpoint {
                    plan: Some(DeliveryPlan {
                        origin: "ALPHA-BELT-1".into(),
                        destination: "ALPHA-HUB".into(),
                        payload_devices: vec![PayloadDevice {
                            code: "DEVICE-1".into(),
                            device_type: "survey_drone".into(),
                            origin: "ALPHA-BELT-1".into(),
                        }],
                        ..DeliveryPlan::default()
                    }),
                    started: true,
                    ..LogisticsWorkflowCheckpoint::default()
                },
                current_step: None,
                parent_id: None,
            })
            .expect("failed source manifest");
        let mut source_registry = WorkflowRegistry::new();
        source_registry
            .register(Arc::new(LogisticsManifestWorkflowFactory::new()))
            .expect("register source manifest");
        let registry = Arc::new(source_registry);
        let source_supervisor = replicant_workflow::WorkflowSupervisor::with_managed_client(
            repository.clone(),
            registry.clone(),
            client.clone(),
        );
        tick_until_terminal(&source_supervisor, &repository, failed.id).await;
        assert_eq!(
            repository
                .read(failed.id)
                .expect("failed source row")
                .expect("failed source")
                .status,
            WorkflowStatus::Failed
        );

        let failed_provenance = WorkflowPlacementProvenance {
            workflow_id: failed.id,
            work_item_id: None,
        };
        let mut recovery_intent =
            recovery_test_intent(failed_provenance.clone(), "mine-m:DEVICE-1");
        recovery_intent
            .placement_recovery
            .as_mut()
            .expect("metadata")
            .placement_resolutions = vec![WorkflowPlacementResolution {
            device_code: "DEVICE-1".into(),
            provenance: failed_provenance,
        }];

        // Cleanup observes the original tag once; reconciliation observes the
        // exact current destination with the tag absent. Priority keeps the
        // two authoritative reads distinct while allowing later refreshes to
        // reuse the destination projection.
        Mock::given(method("GET"))
            .and(path("/v1/devices/DEVICE-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "DEVICE-1",
                "device_type": "survey_drone",
                "location": "ALPHA-BELT-1",
                "status": "idle",
                "tags": ["mine-m:DEVICE-1"]
            })))
            .up_to_n_times(2)
            .with_priority(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/DEVICE-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "DEVICE-1",
                "device_type": "survey_drone",
                "location": "ALPHA-HUB",
                "status": "idle",
                "tags": []
            })))
            .with_priority(2)
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/v1/devices/DEVICE-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/CARRIER-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "CARRIER-1",
                "device_type": "cargo_freighter",
                "location": "ALPHA-HUB",
                "status": "idle",
                "attached_devices": []
            })))
            .mount(&server)
            .await;

        let recovery = repository
            .create(NewWorkflow {
                kind: logistics_manifest_workflow_kind(),
                schema_version: 1,
                config: recovery_intent,
                checkpoint: LogisticsWorkflowCheckpoint {
                    plan: Some(DeliveryPlan {
                        origin: "ALPHA-BELT-1".into(),
                        destination: "ALPHA-HUB".into(),
                        payload_devices: vec![PayloadDevice {
                            code: "DEVICE-1".into(),
                            device_type: "survey_drone".into(),
                            origin: "ALPHA-BELT-1".into(),
                        }],
                        device_carriers: vec!["CARRIER-1".into()],
                        transport_origins: BTreeMap::from([(
                            "CARRIER-1".into(),
                            "ALPHA-HUB".into(),
                        )]),
                        ..DeliveryPlan::default()
                    }),
                    ..LogisticsWorkflowCheckpoint::default()
                },
                current_step: None,
                parent_id: None,
            })
            .expect("recovery manifest");
        authorize_recovery_test(
            &repository,
            recovery.id,
            &recovery.config().expect("recovery intent"),
        );

        let supervisor = replicant_workflow::WorkflowSupervisor::with_managed_client(
            repository.clone(),
            registry.clone(),
            client.clone(),
        );
        tick_until_terminal(&supervisor, &repository, recovery.id).await;
        let succeeded = repository
            .read(recovery.id)
            .expect("recovery row")
            .expect("recovery workflow");
        assert_eq!(
            succeeded.status,
            WorkflowStatus::Succeeded,
            "unexpected recovery smoke error: {:?}",
            succeeded.last_error
        );
        let checkpoint = succeeded
            .checkpoint::<LogisticsWorkflowCheckpoint>()
            .expect("recovery checkpoint");
        assert_eq!(
            checkpoint
                .placement_recovery_cleanup
                .get("DEVICE-1")
                .and_then(|cleanup| cleanup.operation_id.as_deref()),
            Some(recovery_configure_operation_id(recovery.id, "DEVICE-1").as_str())
        );
        let report = succeeded
            .result::<DeliveryReport>()
            .expect("delivery result")
            .expect("delivery report");
        assert_eq!(report.devices_delivered, vec!["DEVICE-1"]);

        let snapshot = registry
            .placement_intent_snapshot(&repository, None)
            .expect("rebuilt placement snapshot");
        assert!(snapshot.failed_transient.iter().all(|evidence| {
            !matches!(
                &evidence.intent.subject,
                WorkflowPlacementIntentSubject::Device(code) if code == "DEVICE-1"
            )
        }));
        assert!(snapshot.resolved_transient.iter().any(|evidence| {
            matches!(
                &evidence.intent.subject,
                WorkflowPlacementIntentSubject::Device(code) if code == "DEVICE-1"
            ) && evidence.workflow_id == failed.id
        }));

        let device = client
            .devices()
            .get("DEVICE-1")
            .await
            .expect("delivered device")
            .snapshot()
            .await
            .expect("delivered device snapshot");
        let devices = BTreeMap::from([("DEVICE-1".into(), device.clone())]);
        let homes = BTreeMap::from([("alpha".into(), BTreeSet::from(["ALPHA-HUB".into()]))]);
        let location_systems = BTreeMap::from([("ALPHA-HUB".into(), "ALPHA".into())]);
        let system_regions = BTreeMap::from([("ALPHA".into(), "alpha".into())]);
        let classification = crate::device_placement::classify_device_placement(
            &device,
            &crate::device_placement::DevicePlacementContext {
                complete_owned_census: true,
                devices: &devices,
                registered_homes: &homes,
                location_systems: &location_systems,
                system_regions: &system_regions,
                workflow_snapshot: &snapshot,
            },
        );
        assert_eq!(
            classification.class,
            crate::device_placement::DevicePlacementClass::Intentional
        );
        assert!(
            classification
                .workflow_evidence
                .live
                .iter()
                .chain(&classification.workflow_evidence.settled_placements)
                .chain(&classification.workflow_evidence.terminal_residuals)
                .chain(&classification.workflow_evidence.failed_transient)
                .chain(&classification.workflow_evidence.resolved_transient)
                .all(|evidence| {
                    !matches!(
                        &evidence.intent.subject,
                        WorkflowPlacementIntentSubject::DeviceTag(_)
                    )
                }),
            "removed recovery tag must not match the delivered device"
        );
        server.verify().await;
        client.close().await.expect("close client");
    }

    #[test]
    fn regional_dispatch_expands_vessel_selectors_in_stable_order() {
        let intent = RegionalDispatchIntent {
            racing_vessels: 2,
            heaven_vessels: 1,
            cargo_vessels: 1,
            ..RegionalDispatchIntent::default()
        };
        assert_eq!(
            desired_replicant_vessel_types(&intent),
            vec![
                "racing_vessel",
                "racing_vessel",
                "heaven_vessel",
                "cargo_vessel"
            ]
        );
    }

    fn dispatch_test_device(code: &str, device_type: &str, location: &str) -> Device {
        Device {
            key: DeviceKey::live(DeviceId::from(code)),
            device_type: Some(DeviceType::from(device_type)),
            status: Some(DeviceStatus::Idle),
            location: Some(LocationKey::live(LocationId::from(location))),
            deployed_at: None,
            in_control_range: None,
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            settings: BTreeMap::new(),
            relationships: DeviceRelationships::default(),
            cargo: BTreeMap::new(),
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

    #[test]
    fn regional_dispatch_resolves_hub_device_or_system_to_manufacturing_location() {
        let devices = vec![
            dispatch_test_device("HUB-1", "system_hub", "SCEPTURUM-7-L4"),
            dispatch_test_device("FACTORY-1", "autofactory", "SCEPTURUM-BELT-1"),
            dispatch_test_device("FACTORY-2", "autofactory", "SCEPTURUM-BELT-1"),
        ];

        assert_eq!(
            regional_dispatch_source_location("SCEPTURUM", "SCEPTURUM", &devices)
                .expect("system source"),
            "SCEPTURUM-BELT-1"
        );
        assert_eq!(
            regional_dispatch_source_location("SCEPTURUM-7-L4", "SCEPTURUM", &devices)
                .expect("System Hub device location"),
            "SCEPTURUM-BELT-1"
        );
        assert_eq!(
            regional_dispatch_source_location("SCEPTURUM-BELT-1", "SCEPTURUM", &devices)
                .expect("manufacturing location"),
            "SCEPTURUM-BELT-1"
        );
    }

    #[test]
    fn regional_dispatch_source_requires_both_hub_and_factory_in_same_system() {
        let hub_only = vec![dispatch_test_device(
            "HUB-1",
            "system_hub",
            "SCEPTURUM-7-L4",
        )];
        let error = regional_dispatch_source_location("SCEPTURUM", "SCEPTURUM", &hub_only)
            .expect_err("hub without factory must fail");
        assert!(error.contains("no account-owned Autofactory"));

        let factory_only = vec![dispatch_test_device(
            "FACTORY-1",
            "autofactory",
            "SCEPTURUM-BELT-1",
        )];
        let error =
            regional_dispatch_source_location("SCEPTURUM-BELT-1", "SCEPTURUM", &factory_only)
                .expect_err("factory without hub must fail");
        assert!(error.contains("does not contain an owned System Hub"));
    }

    #[test]
    fn regional_dispatch_accepts_vessels_with_only_an_empty_matrix() {
        let mut vessel = dispatch_test_device("RACE-1", "racing_vessel", "HUB");
        let mut matrix = dispatch_test_device("EMPTY-1", "empty_replicant_matrix", "HUB");
        matrix.status = Some(DeviceStatus::from("stowed"));
        matrix.location = None;
        matrix.relationships.stowed_in = Some(vessel.key.clone());
        vessel.relationships.stowed_devices = vec![matrix.key.clone()];
        let devices = vec![vessel.clone(), matrix.clone()];

        assert!(dispatch_vessel_is_free(
            &vessel,
            "racing_vessel",
            "HUB",
            &devices
        ));
        assert_eq!(
            dispatch_vessel_onboard_empty_matrix(&vessel, &devices)
                .map(|device| device.key.id.as_str()),
            Some("EMPTY-1")
        );

        matrix.device_type = Some(DeviceType::ReplicantMatrix);
        let devices = vec![vessel.clone(), matrix];
        assert!(!dispatch_vessel_is_free(
            &vessel,
            "racing_vessel",
            "HUB",
            &devices
        ));
    }

    #[test]
    fn regional_dispatch_reuses_loose_empty_matrices_and_respects_reservations() {
        let matrix = dispatch_test_device("EMPTY-1", "empty_replicant_matrix", "HUB");
        assert!(dispatch_loose_empty_matrix_is_free(&matrix, "HUB"));

        let mut reserved = matrix;
        reserved.tags.push("mine-m:other-workflow".to_owned());
        assert!(!dispatch_loose_empty_matrix_is_free(&reserved, "HUB"));
    }

    #[test]
    fn regional_dispatch_vessel_only_manifest_requires_device_transport() {
        let intent = RegionalDispatchIntent {
            racing_vessels: 3,
            ..RegionalDispatchIntent::default()
        };
        assert!(regional_dispatch_has_device_payload(&intent));
    }

    #[test]
    fn regional_dispatch_refuses_incomplete_vessel_matrix_pairs() {
        let intent = RegionalDispatchIntent {
            source: "HUB".to_owned(),
            destination: "TARGET".to_owned(),
            racing_vessels: 1,
            ..RegionalDispatchIntent::default()
        };
        let checkpoint = RegionalDispatchCheckpoint {
            vessels: vec!["RACE-1".to_owned()],
            matrices: vec![None],
            ..RegionalDispatchCheckpoint::default()
        };
        assert!(regional_dispatch_delivery_request(&intent, &checkpoint).is_err());
    }
}
