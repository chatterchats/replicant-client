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

use replicant_client::{
    Client, Device, DeviceHandle, DeviceType, MiningDirective, Operation, OperationId,
    OperationStatus, SurveyDirective, domain::AccessScope,
};
use replicant_printing::{
    PrintRequest,
    managed::{QueueOptions, printing_status_in_system, queue_prints_with_components},
};
use replicant_protocol::workflow_reserved;
use replicant_transport::{
    DeliveryOptions, DeliveryPlan, DeliveryRequest, DeviceRequest, ResourceMap, TransportError,
    execute_delivery, plan_delivery, validate_resource_pickups,
};
use replicant_workflow::{
    AllocationSet, BoxWorkflowFuture, ClaimAcquireOutcome, ControlRequest, NewWorkflow,
    RegistryError, RepositoryError, RequirementScope, ResourceKey, ResourceRequirement, WaitIntent,
    WaitOutcome, WaitSignal, WorkItem, WorkItemSpec, WorkItemStatus, WorkItemTransition,
    WorkflowContext, WorkflowExecutor, WorkflowFactory, WorkflowId, WorkflowKind,
    WorkflowMigration, WorkflowRegistry, WorkflowStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::bootstrap::{
    BootstrapExecutionRequest, BootstrapPlanningRequest, plan_bootstrap, run_bootstrap,
};

use crate::{
    belt_search::{BeltOperationRejection, execute_belt_search_system, travel_to_system},
    canonical_region,
    event::{
        EventCampaignArchive, EventCampaignPlanningRequest, EventExecutionRequest, EventItemStage,
        EventPlanningRequest, EventStockReconcileOptions, archive_event_campaign,
        event_campaign_target_systems, event_campaign_work_item_specs, event_mission_target_system,
        execute_event_item, execute_event_mission, haul_allocated_resources, plan_event_campaign,
        plan_event_mission, prestage_event_mission, reconcile_event_stock, restore_event_campaign,
    },
    failure::{FailureClass, failure_class, failure_class_from_message, failure_disposition},
    mining::{MiningExpansionRequest, MiningMission, execute_expansion},
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
    workflows::{
        ManagedMiningItemExecutor, MiningWorkflowCheckpoint, MiningWorkflowConfig,
        execute_mining_pool_config,
    },
};

const SCHEMA_VERSION: u32 = 1;
const DEFAULT_WAIT_SECONDS: u64 = 21_600;
const IDLE_CAMPAIGN_RETRY_INTERVAL: Duration = Duration::from_secs(300);
const EVENT_DEPENDENCY_RECONCILIATION_INTERVAL: Duration = Duration::from_secs(60);
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
const EVENT_CAMPAIGN_DEPENDENCY_EVENT_NAMES: [&str; 14] = [
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
    registry.register(Arc::new(LogisticsManifestWorkflowFactory::new()))?;
    registry.register(Arc::new(TradeFulfillmentWorkflowFactory::new()))?;
    registry.register(Arc::new(BlueprintAcquireWorkflowFactory::new()))?;
    registry.register(Arc::new(ExplorationWorkflowFactory::new()))?;
    registry.register(Arc::new(EventDeliveryWorkflowFactory::new()))?;
    registry.register(Arc::new(EventTourWorkflowFactory::new()))?;
    registry.register(Arc::new(EventCampaignWorkflowFactory::new()))?;
    registry.register(Arc::new(ObservatoryWorkflowFactory::new()))?;
    registry.register(Arc::new(ReplicantProvisionWorkflowFactory::new()))?;
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
    /// Scheduler ceiling for simultaneously runnable items.
    #[serde(default = "default_mining_concurrency")]
    pub max_concurrency: usize,
}

#[derive(Clone, Debug, Deserialize)]
struct LegacyMiningCampaignIntent {
    systems: Vec<String>,
    #[serde(default)]
    replicant: Option<String>,
    #[serde(default)]
    hub: Option<String>,
    #[serde(default = "default_mining_concurrency")]
    max_concurrency: usize,
}

/// Restart-safe mining deployment checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
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

/// Schema-version-two mining campaign checkpoint.
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
    /// Return transports after delivery.
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

/// Restart-safe logistics checkpoint. The concrete plan is persisted before mutation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LogisticsWorkflowCheckpoint {
    /// Concrete transport plan selected from managed state.
    pub plan: Option<DeliveryPlan>,
    /// Whether execution entered the reusable transport executor.
    pub started: bool,
    #[serde(default)]
    failure_class: Option<FailureClass>,
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
    /// Printed empty matrix code.
    pub matrix: Option<String>,
    /// Printed cradle vessel code.
    pub cradle: Option<String>,
    /// Whether the target matrix has been stowed into its cradle.
    pub stowed: bool,
    /// New Replicant code after successful replication.
    pub new_replicant: Option<String>,
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

macro_rules! workflow_factory {
    ($name:ident, $executor:ident, $kind_fn:ident) => {
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
        }
    };
}

workflow_factory!(
    ScanSystemWorkflowFactory,
    ScanSystemWorkflow,
    scan_system_workflow_kind
);
workflow_factory!(
    ScanBeltWorkflowFactory,
    ScanBeltWorkflow,
    scan_belt_workflow_kind
);
workflow_factory!(
    ScanTourWorkflowFactory,
    ScanTourWorkflow,
    scan_tour_workflow_kind
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
}
workflow_factory!(
    SalvageWorkflowFactory,
    SalvageWorkflow,
    salvage_workflow_kind
);
workflow_factory!(
    MiningDeployWorkflowFactory,
    MiningDeployWorkflow,
    mining_deploy_workflow_kind
);
/// Factory for schema-version-two pooled regional mining campaigns.
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
            region: String::new(),
            hub: checkpoint.hub.or(legacy.hub).unwrap_or_default(),
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

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(MiningCampaignWorkflow {
            item_executor: self.item_executor.clone(),
        }))
    }
}
workflow_factory!(
    SalvageRecoveryWorkflowFactory,
    SalvageRecoveryWorkflow,
    salvage_recovery_workflow_kind
);
workflow_factory!(
    LogisticsWorkflowFactory,
    LogisticsWorkflow,
    logistics_workflow_kind
);
workflow_factory!(
    LogisticsManifestWorkflowFactory,
    LogisticsManifestWorkflow,
    logistics_manifest_workflow_kind
);
workflow_factory!(
    TradeFulfillmentWorkflowFactory,
    TradeFulfillmentWorkflow,
    trade_fulfillment_workflow_kind
);
workflow_factory!(
    BlueprintAcquireWorkflowFactory,
    BlueprintAcquireWorkflow,
    blueprint_acquire_workflow_kind
);
workflow_factory!(
    ExplorationWorkflowFactory,
    ExplorationWorkflow,
    exploration_workflow_kind
);
workflow_factory!(
    EventDeliveryWorkflowFactory,
    EventDeliveryWorkflow,
    event_delivery_workflow_kind
);
workflow_factory!(
    EventTourWorkflowFactory,
    EventTourWorkflow,
    event_tour_workflow_kind
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
}
workflow_factory!(
    ObservatoryWorkflowFactory,
    ObservatoryWorkflow,
    observatory_workflow_kind
);
workflow_factory!(
    ReplicantProvisionWorkflowFactory,
    ReplicantProvisionWorkflow,
    replicant_provision_workflow_kind
);
workflow_factory!(
    RegionEstablishWorkflowFactory,
    RegionEstablishWorkflow,
    region_establish_workflow_kind
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
            let (replicant, vessel) = if let (Some(replicant), Some(vessel)) =
                (checkpoint.replicant.clone(), checkpoint.vessel.clone())
            {
                (replicant, vessel)
            } else {
                resolve_survey_assignment(
                    &client,
                    intent.replicant.as_deref(),
                    intent.vessel.as_deref(),
                )
                .await?
            };
            let maintenance_home = match checkpoint.maintenance_home.clone() {
                Some(value) => value,
                None => resolve_home(&client, None).await?,
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
                        capabilities: Vec::new(),
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
                            )
                    });
                    for candidate in &mut candidates {
                        if let Some(location) = &mut candidate.location {
                            location.region = Some(intent.region.clone());
                        } else {
                            candidate.location = Some(replicant_workflow::AllocationLocation {
                                region: Some(intent.region.clone()),
                                ..replicant_workflow::AllocationLocation::default()
                            });
                        }
                    }
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
                                continue;
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

const BELT_SCAN_CAPABILITIES: [&str; 2] = ["census", "system_scan"];

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
                "capabilities": ["census", "system_scan"],
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

struct LogisticsManifestWorkflow;
impl WorkflowExecutor for LogisticsManifestWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: LogisticsManifestIntent = context.config().map_err(string_error)?;
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
            let plan = if checkpoint.started {
                checkpoint
                    .plan
                    .clone()
                    .ok_or_else(|| "started logistics manifest lost its durable plan".to_owned())?
            } else {
                context
                    .advance_to("planning", &checkpoint)
                    .map_err(string_error)?;
                let plan = match checkpoint.plan.clone() {
                    Some(plan) => plan,
                    None => match plan_delivery(&client, &request).await {
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
                    },
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
                .get_or_insert_with(|| format!("dir-p:{}", &context.id().to_string()[..8]))
                .clone();
            let requests = [
                PrintRequest::new("empty_replicant_matrix", 1),
                PrintRequest::new(intent.cradle_type.clone(), 1),
            ];
            if checkpoint.matrix.is_none() || checkpoint.cradle.is_none() {
                context
                    .advance_to("manufacturing", &checkpoint)
                    .map_err(string_error)?;
                let mut options = QueueOptions::at(intent.home.clone());
                options.tags = vec![tag.clone()];
                options.wait_timeout = Duration::from_secs(DEFAULT_WAIT_SECONDS);
                queue_prints_with_components(&client, &requests, &options)
                    .await
                    .map_err(string_error)?;
                loop {
                    let status = printing_status_in_system(
                        &client,
                        &intent.home,
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
                        | replicant_workflow::ControlRequest::Cancel => return Ok(()),
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                let devices = tagged_devices(&client, &tag).await?;
                checkpoint.matrix = devices
                    .iter()
                    .find(|(_, kind)| kind == "empty_replicant_matrix")
                    .map(|(code, _)| code.clone());
                checkpoint.cradle = devices
                    .iter()
                    .find(|(_, kind)| kind == &intent.cradle_type)
                    .map(|(code, _)| code.clone());
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            let matrix = checkpoint
                .matrix
                .clone()
                .ok_or_else(|| "provisioned empty Replicant matrix was not found".to_owned())?;
            let cradle = checkpoint
                .cradle
                .clone()
                .ok_or_else(|| "provisioned cradle vessel was not found".to_owned())?;
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
) -> NewWorkflow<MiningCampaignIntent, MiningDeployCheckpoint> {
    queued_workflow(
        mining_campaign_workflow_kind(),
        intent,
        MiningDeployCheckpoint::default(),
    )
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
                return_transports: false,
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
    Ok(vessel
        .stow_capacity
        .unwrap_or_default()
        .saturating_sub(vessel.stow_used.unwrap_or_default())
        .max(0))
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
    let operation = handle
        .command(replicant_client::raw::devices::DeviceCommand::Travel {
            destination: destination.to_owned(),
            dry_run: None,
            via: None,
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
                device_tags: Vec::new(),
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
                device_tags: Vec::new(),
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

async fn available_scan_tour_fleet(
    context: &WorkflowContext,
    client: &Client,
    staging_location: &str,
) -> Result<(Vec<String>, Vec<String>), String> {
    let claimed = claimed_scan_tour_devices(context)?;
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

    let mut controllers = controller_handles
        .into_iter()
        .map(|handle| handle.id().as_str().to_owned())
        .filter(|code| !claimed.contains(code))
        .collect::<Vec<_>>();
    let mut drones = drone_handles
        .into_iter()
        .map(|handle| handle.id().as_str().to_owned())
        .filter(|code| !claimed.contains(code))
        .collect::<Vec<_>>();
    controllers.sort();
    drones.sort();
    Ok((controllers, drones))
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
    let staging_location = vessel_snapshot
        .location
        .as_ref()
        .map(|location| location.id.as_str().to_owned())
        .ok_or_else(|| format!("survey vessel {vessel} has no current staging location"))?;

    let (controllers, drones) =
        available_scan_tour_fleet(context, client, &staging_location).await?;
    if reserve_scan_tour_fleet(context, checkpoint, &controllers, &drones)? {
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
    let (controllers, drones) =
        available_scan_tour_fleet(context, client, &staging_location).await?;
    if reserve_scan_tour_fleet(context, checkpoint, &controllers, &drones)? {
        return Ok(true);
    }

    // Only unclaimed devices count toward this tour. Parallel catalogue shards
    // therefore manufacture independent fleets instead of racing to claim the
    // same idle controller/drones after the preflight succeeds.
    let requests = scan_tour_fleet_print_requests(controllers.len(), drones.len());
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
        let (controllers, drones) =
            available_scan_tour_fleet(context, client, &staging_location).await?;
        if reserve_scan_tour_fleet(context, checkpoint, &controllers, &drones)? {
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

async fn resolve_survey_assignment(
    client: &Client,
    pinned_replicant: Option<&str>,
    pinned_vessel: Option<&str>,
) -> Result<(String, String), String> {
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
        client
            .replicants()
            .get_owned(&hosted)
            .await
            .map_err(string_error)?;
        return Ok((hosted, vessel_code.to_owned()));
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
        client
            .replicants()
            .get_owned(hosted.id.as_str())
            .await
            .map_err(string_error)?;
        return Ok((
            hosted.id.as_str().to_owned(),
            handle.id().as_str().to_owned(),
        ));
    }
    Err("no owned racing vessel hosting an eligible replicant is available".to_owned())
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
            OperationStatus::Completed | OperationStatus::Accepted => return Ok(()),
            OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed => {
                return Err(format!(
                    "managed operation {} ended as {:?}: {}",
                    operation.id(),
                    outcome.status,
                    outcome.response.unwrap_or(Value::Null)
                ));
            }
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

fn campaign_retry_deadline(
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

async fn wait_for_campaign_work(
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

    use replicant_client::{SecretString, StartupPolicy, raw::Url};
    use replicant_workflow::WorkflowRepository;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param, query_param_is_missing},
    };

    use super::*;
    use crate::failure::ClassifiedError;

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
        Mock::given(method("GET"))
            .and(path(format!("/v1/replicants/{worker}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "replicant_code": worker,
                "location": "ROOT-1-L4",
                "status": "active"
            })))
            .expect(1)
            .mount(server)
            .await;
        client
            .replicants()
            .get_owned(worker)
            .await
            .expect("seed event worker");
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
                    == serde_json::json!(["census", "system_scan"]))
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
        assert!(belt_worker_candidate(&candidate(&[
            "census",
            "system_scan"
        ])));
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
                capabilities: vec!["census".into(), "system_scan".into()],
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
            capabilities: vec!["census".into(), "system_scan".into()],
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
}
