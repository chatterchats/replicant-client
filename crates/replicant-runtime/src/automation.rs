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
    Client, Device, DeviceType, MiningDirective, Operation, OperationId, OperationStatus,
    SurveyDirective, domain::AccessScope,
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
    BoxWorkflowFuture, ClaimAcquireOutcome, NewWorkflow, RegistryError, RepositoryError,
    ResourceKey, WorkflowContext, WorkflowExecutor, WorkflowFactory, WorkflowId, WorkflowKind,
    WorkflowRegistry, WorkflowStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::bootstrap::{
    BootstrapExecutionRequest, BootstrapPlanningRequest, plan_bootstrap, run_bootstrap,
};

use crate::{
    event::{
        EventCampaignArchive, EventCampaignPlanningRequest, EventExecutionRequest,
        EventPlanningRequest, EventStockReconcileOptions, archive_event_campaign,
        event_campaign_target_systems, event_mission_target_system, execute_event_campaign,
        execute_event_mission, plan_event_campaign, plan_event_mission, prestage_event_mission,
        reconcile_event_stock, restore_event_campaign,
    },
    mining::{MiningExpansionRequest, execute_expansion},
    observatory::auto_prospect,
    relay::{
        RelayExecutionState, RelayExpansionRequest, execute_relay_workflow,
        ftl_network_reachable_systems, restore_relay_checkpoint,
    },
    survey::{
        SurveyExecutionState, SurveyMode, SurveyOptions, execute_survey_workflow,
        restore_survey_checkpoint,
    },
    trade::{TradeBundle, shop_trades},
};

const SCHEMA_VERSION: u32 = 1;
const DEFAULT_WAIT_SECONDS: u64 = 21_600;

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

/// Intent-native workflow that salvages one site to depletion.
pub fn salvage_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("salvage.site").expect("static workflow kind is valid")
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
    registry.register(Arc::new(SalvageWorkflowFactory::new()))?;
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

/// Goal-level input for a batch mining expansion.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MiningCampaignIntent {
    /// Target systems selected by the regional campaign planner.
    pub systems: Vec<String>,
    /// Optional regional Replicant to pin.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Optional regional manufacturing hub.
    #[serde(default)]
    pub hub: Option<String>,
    /// Maximum concurrently dispatched site workers.
    #[serde(default = "default_mining_concurrency")]
    pub max_concurrency: usize,
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

/// Authoritative exploration checkpoint. The old relay mission file is only an ephemeral adapter.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ExplorationWorkflowCheckpoint {
    /// Resolved replicant retained across restarts.
    pub replicant: Option<String>,
    /// Resolved manufacturing hub retained across restarts.
    pub hub: Option<String>,
    /// Last authoritative relay executor state.
    pub state: Option<RelayExecutionState>,
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
    /// Optional regional replicant to pin.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Optional regional manufacturing/staging home.
    #[serde(default)]
    pub home: Option<String>,
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
workflow_factory!(
    MiningCampaignWorkflowFactory,
    MiningCampaignWorkflow,
    mining_campaign_workflow_kind
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
workflow_factory!(
    EventCampaignWorkflowFactory,
    EventCampaignWorkflow,
    event_campaign_workflow_kind
);
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

struct MiningCampaignWorkflow;
impl WorkflowExecutor for MiningCampaignWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: MiningCampaignIntent = context.config().map_err(string_error)?;
            if intent.systems.is_empty() {
                return context
                    .mark_succeeded(Some(serde_json::json!({"systems": []})))
                    .map_err(string_error);
            }
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
            for system in &intent.systems {
                claim_target(context, "mining-target", system)?;
            }
            claim_target(context, "location", &hub)?;
            let plan_file = scratch_file(context.id(), "mining-campaign.json")?;
            materialize_json(&plan_file, checkpoint.plan_json.as_deref())?;
            checkpoint.started = true;
            context
                .advance_to("expanding", &checkpoint)
                .map_err(string_error)?;
            let request = MiningExpansionRequest {
                systems: intent.systems,
                replicant,
                hub,
                mission_file: plan_file.clone(),
                wait_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
                max_concurrency: intent.max_concurrency.max(1),
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
            let report = execute_delivery(&client, &plan, options)
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
            let report = execute_delivery(&client, &plan, options)
                .await
                .map_err(string_error)?;
            context.mark_succeeded(Some(report)).map_err(string_error)
        })
    }
}

fn retryable_manifest_planning_failure(error: &TransportError) -> bool {
    match error {
        // Missing stock, carriers, or payloads are mutable world-state blockers,
        // not terminal workflow defects. Director manifests should wait for the
        // hub/projection to change and then select a fresh plan.
        TransportError::NotFound(_) => true,
        TransportError::Invalid(message) => {
            message.contains("not a free inactive payload")
                || message.contains("reserved by another workflow")
        }
        _ => false,
    }
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
            context
                .advance_to("exploring", &checkpoint)
                .map_err(string_error)?;
            let request = RelayExpansionRequest {
                replicant,
                hub,
                targets: vec![intent.target.clone()],
                mission_file: plan_file.clone(),
                max_hop_ly: 7.499,
                wait_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
                unavailable_autofactories,
            };
            let result = execute_relay_workflow(&client, &request, |state| {
                let (replicant, devices, factories) = state.resources();
                claim(context, ResourceKey::Replicant(replicant.to_owned()))?;
                for device in devices {
                    claim_device(context, device)?;
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
                    let message = error.to_string();
                    if stale_relay_plan_failure(&message) {
                        release_exploration_autofactory_claims(context)?;
                        tracing::warn!(
                            workflow_id = %context.id(),
                            target = %intent.target,
                            error = %message,
                            "relay topology changed underneath the saved plan; discarding it and replanning"
                        );
                        checkpoint.state = None;
                        clear_scratch_file(&plan_file)?;
                        context
                            .advance_to("replanning_relay_coverage", &checkpoint)
                            .map_err(string_error)?;
                        context.mark_waiting().map_err(string_error)
                    } else if resource_claim_contention(&message) {
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
                    } else if retryable_connectivity_dependency_failure(&message) {
                        tracing::warn!(
                            workflow_id = %context.id(),
                            target = %intent.target,
                            error = %message,
                            "relay expansion is blocked on a recoverable prerequisite; waiting to retry"
                        );
                        context
                            .advance_to("awaiting_relay_prerequisites", &checkpoint)
                            .map_err(string_error)?;
                        context.mark_waiting().map_err(string_error)
                    } else {
                        Err(message)
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

struct EventCampaignWorkflow;
impl WorkflowExecutor for EventCampaignWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: EventCampaignIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: EventCampaignCheckpoint =
                context.checkpoint().map_err(string_error)?;
            claim_target(context, "event-campaign", &intent.region)?;
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
                    return Ok(());
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

            // Connectivity expansion is allowed to use the configured event
            // worker. Claim it only after relay dependencies are satisfied so
            // the child workflow does not collide with its parent claim. The
            // explicit workflow config still keeps the worker Director-busy.
            claim(context, ResourceKey::Replicant(replicant.clone()))?;
            context
                .advance_to("executing", &checkpoint)
                .map_err(string_error)?;
            let execution_request = EventExecutionRequest::new(
                plan_file.clone(),
                Duration::from_secs(DEFAULT_WAIT_SECONDS),
            );
            let execution = execute_event_campaign(&client, &execution_request);
            tokio::pin!(execution);
            let mut checkpoint_interval = tokio::time::interval(Duration::from_secs(2));
            let state = loop {
                tokio::select! {
                    result = &mut execution => match result {
                        Ok(state) => break state,
                        Err(error) => {
                            let message = error.to_string();
                            if retryable_event_campaign_failure(&message) {
                                if event_campaign_failure_requires_replan(&message) {
                                    checkpoint.archive = None;
                                    clear_scratch_file(&plan_file)?;
                                    context
                                        .advance_to("replanning_after_stale_asset", &checkpoint)
                                        .map_err(string_error)?;
                                } else {
                                    if let Ok(archive) = archive_event_campaign(&plan_file) {
                                        checkpoint.archive = Some(archive);
                                    }
                                    let step = if event_campaign_failure_waits_for_inputs(&message)
                                    {
                                        "waiting_for_event_inputs"
                                    } else {
                                        "waiting_for_control_range"
                                    };
                                    context
                                        .advance_to(step, &checkpoint)
                                        .map_err(string_error)?;
                                }
                                context.persist_checkpoint(&checkpoint).map_err(string_error)?;
                                context
                                    .emit_activity(format!(
                                        "event campaign hit a recoverable execution condition ({message}); waiting to retry"
                                    ))
                                    .map_err(string_error)?;
                                context.mark_waiting().map_err(string_error)?;
                                return Ok(());
                            }
                            return Err(string_error(error));
                        }
                    },
                    _ = checkpoint_interval.tick() => {
                        match context.control_request().map_err(string_error)? {
                            replicant_workflow::ControlRequest::Continue => {}
                            replicant_workflow::ControlRequest::Pause
                            | replicant_workflow::ControlRequest::Cancel => return Ok(()),
                        }
                        if let Ok(archive) = archive_event_campaign(&plan_file) {
                            checkpoint.archive = Some(archive);
                            context.persist_checkpoint(&checkpoint).map_err(string_error)?;
                        }
                    }
                }
            };
            checkpoint.archive = Some(archive_event_campaign(&plan_file).map_err(string_error)?);
            context
                .persist_checkpoint(&checkpoint)
                .map_err(string_error)?;
            context.mark_succeeded(Some(state)).map_err(string_error)
        })
    }
}

fn event_campaign_failure_waits_for_inputs(message: &str) -> bool {
    message
        .to_ascii_lowercase()
        .contains("all currently feasible events completed, but blocked events remain")
}

fn retryable_event_campaign_failure(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    event_campaign_failure_waits_for_inputs(&message)
        || message.contains("out of comms range")
        || message.contains("out of control range")
        || message.contains("not your device")
        || message.contains("not present in the account-owned device projection")
        || message.contains("unexpected http status 500")
        || message.contains("internal server error")
        || message.contains("client is closed")
}

fn event_campaign_failure_requires_replan(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("not your device")
        || message.contains("not present in the account-owned device projection")
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
        context.mark_waiting().map_err(string_error)?;
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
                let error = workflow
                    .last_error
                    .as_deref()
                    .unwrap_or("no error was recorded");
                if workflow.status == WorkflowStatus::Failed
                    && retryable_connectivity_dependency_failure(error)
                {
                    const CONNECTIVITY_RETRY_COOLDOWN_MS: i64 = 30 * 60 * 1_000;
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
                        .unwrap_or_default();
                    if now.saturating_sub(workflow.updated_at) < CONNECTIVITY_RETRY_COOLDOWN_MS {
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

fn stale_relay_plan_failure(message: &str) -> bool {
    message
        .to_ascii_lowercase()
        .contains("planned account-owned relay coverage is no longer relaying")
}

fn resource_claim_contention(message: &str) -> bool {
    message
        .to_ascii_lowercase()
        .contains("resource is already claimed by workflow")
}

fn retryable_connectivity_dependency_failure(message: &str) -> bool {
    let message = message.to_ascii_lowercase();
    message.contains("no relay network connects")
        || message.contains("blueprint is not unlocked")
        || message.contains("missing blueprint")
        || message.contains("insufficient manufacturing inventory")
        || message.contains("requires an idle attachment carrier")
        || message.contains("no usable stow capacity")
        || message.contains("not currently projected as stationary in a star system")
        || message.contains("has no known l4 or l5 deployment location")
        || message.contains("client is closed")
        || message.contains("no eligible autofactory")
        || message.contains("internal server error")
        || message.contains("unexpected http status 500")
}

fn active_connectivity_workflow(
    context: &WorkflowContext,
    target: &str,
    home_system: &str,
) -> Result<Option<WorkflowId>, String> {
    for workflow in context
        .repository()
        .list()
        .map_err(string_error)?
        .into_iter()
        .filter(|workflow| {
            workflow.kind == exploration_workflow_kind() && !workflow.status.is_terminal()
        })
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
    let mut devices = Vec::with_capacity(handles.len());
    for handle in handles {
        let device = match handle.snapshot().await {
            Ok(device) => device,
            Err(_) => handle
                .refresh()
                .await
                .map_err(string_error)?
                .snapshot()
                .await
                .map_err(string_error)?,
        };
        devices.push(device);
    }
    devices.sort_by(|left, right| left.key.id.cmp(&right.key.id));
    Ok(devices)
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
                let error = child.last_error.unwrap_or_default();
                if child.status == WorkflowStatus::Failed
                    && retryable_trade_criteria_logistics_failure(&error)
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

    let mut devices = owned_device_snapshots(client).await?;
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
        devices = owned_device_snapshots(client).await?;
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
    let resources_now = inventory_at_location(
        &fetch_account_inventories(client).await?,
        &intent.shop_location,
    );
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

fn resource_command_object(
    resources: &ResourceMap,
) -> Result<serde_json::Map<String, Value>, String> {
    let value = serde_json::to_value(resources).map_err(string_error)?;
    value
        .as_object()
        .cloned()
        .ok_or_else(|| "resource manifest did not serialize as an object".to_owned())
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
    for attempt in 0..attempts.max(1) {
        let inventory =
            inventory_at_location(&fetch_account_inventories(client).await?, shop_location);
        if rewards.iter().all(|(resource, quantity)| {
            inventory.get(resource).copied().unwrap_or_default() >= *quantity
        }) {
            return Ok(true);
        }
        if attempt + 1 < attempts.max(1) {
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
    }
    Ok(false)
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
            let refreshed = owned_device_snapshots(client).await?;
            let mut candidates = trade_checkpoint
                .reward_devices
                .iter()
                .filter_map(|code| {
                    refreshed.iter().find(|device| {
                        device.key.id.as_str().eq_ignore_ascii_case(code)
                            && blueprint_source_is_candidate(
                                device,
                                &intent.device_type,
                                &refreshed,
                            )
                            && blueprint_source_location(device, &refreshed).is_some_and(
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

    let mut devices = owned_device_snapshots(client).await?;
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
        devices = owned_device_snapshots(client).await?;
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
    let resources_now =
        inventory_at_location(&fetch_account_inventories(client).await?, shop_location);
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
                let error = child.last_error.unwrap_or_default();
                if child.status == WorkflowStatus::Failed
                    && retryable_trade_criteria_logistics_failure(&error)
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

fn retryable_trade_criteria_logistics_failure(error: &str) -> bool {
    let stale_snapshot =
        error.contains("planned resource pickup at ") && error.contains(" is stale: need ");
    let insufficient_at_source = error.contains("Insufficient ")
        && error.contains(" at location: need ")
        && error.contains(", have ");
    let stale_payload = error.contains("not a free inactive payload")
        || error.contains("reserved by another workflow");
    stale_snapshot || insufficient_at_source || stale_payload
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
                limit: Some(50),
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
) -> Result<(), String> {
    let stale = context
        .claims()
        .map_err(string_error)?
        .into_iter()
        .filter_map(|claim| match claim.resource {
            ResourceKey::Autofactory(code) if !required.contains(&code) => {
                Some(ResourceKey::Autofactory(code))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    for resource in stale {
        context.release_claim(&resource).map_err(string_error)?;
    }
    for code in required {
        claim(context, ResourceKey::Autofactory(code.clone()))?;
    }
    Ok(())
}

fn release_exploration_autofactory_claims(context: &WorkflowContext) -> Result<(), String> {
    reconcile_exploration_autofactory_claims(context, &BTreeSet::new())
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

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn stale_trade_criteria_resource_failures_are_retryable() {
        assert!(retryable_trade_criteria_logistics_failure(
            "operation rejected: Insufficient structural at location: need 49.0, have 0"
        ));
        assert!(retryable_trade_criteria_logistics_failure(
            "planned resource pickup at SCEPTURUM-BELT-1 is stale: need 17 conductive, have 0"
        ));
        assert!(retryable_trade_criteria_logistics_failure(
            "payload device DEVICE-1 is not a free inactive payload"
        ));
        assert!(!retryable_trade_criteria_logistics_failure(
            "transport CARRIER-1 has no usable cargo capacity"
        ));
    }

    #[test]
    fn event_campaign_runtime_failures_choose_waiting_or_replan() {
        assert!(retryable_event_campaign_failure(
            "Device is out of comms range"
        ));
        assert!(!event_campaign_failure_requires_replan(
            "Device is out of comms range"
        ));
        assert!(retryable_event_campaign_failure("403 Not your device"));
        assert!(event_campaign_failure_requires_replan(
            "403 Not your device"
        ));
        assert!(event_campaign_failure_requires_replan(
            "event asset D-1 is not present in the account-owned device projection; replan required"
        ));
        assert!(retryable_event_campaign_failure(
            "unexpected HTTP status 500: Internal server error"
        ));
        assert!(!event_campaign_failure_requires_replan(
            "unexpected HTTP status 500: Internal server error"
        ));
        assert!(retryable_event_campaign_failure("client is closed"));
        assert!(!event_campaign_failure_requires_replan("client is closed"));
        let blocked = "all currently feasible events completed, but blocked events remain; replenish resources";
        assert!(retryable_event_campaign_failure(blocked));
        assert!(event_campaign_failure_waits_for_inputs(blocked));
        assert!(!event_campaign_failure_requires_replan(blocked));
        assert!(!retryable_event_campaign_failure(
            "event criterion is structurally invalid"
        ));
    }

    #[test]
    fn relay_connectivity_capacity_blockers_are_retryable_without_campaign_failure() {
        assert!(retryable_connectivity_dependency_failure(
            "no relay network connects SCEPTURUM to ALIPHERATZ; closest gap is ANTAR -> ALIPHERATZ at 8.403 ly"
        ));
        assert!(retryable_connectivity_dependency_failure(
            "deep_space_relay_station blueprint is not unlocked"
        ));
        assert!(retryable_connectivity_dependency_failure(
            "missing blueprint for requested device type `comm_satellite`"
        ));
        assert!(retryable_connectivity_dependency_failure(
            "EIRFARYR has no known L4 or L5 deployment location"
        ));
        assert!(retryable_connectivity_dependency_failure(
            "insufficient manufacturing inventory at SCEPTURUM-BELT-1"
        ));
        assert!(retryable_connectivity_dependency_failure(
            "Deep Space Relay Station deployment from SCEPTURUM-BELT-1 requires an idle attachment carrier in system SCEPTURUM"
        ));
        assert!(!retryable_connectivity_dependency_failure(
            "relay checkpoint is malformed"
        ));
    }

    #[test]
    fn mutable_manifest_planning_blockers_wait_for_replan() {
        assert!(retryable_manifest_planning_failure(
            &TransportError::NotFound(
                "origin SCEPTURUM has only 0 conductive; 30 requested".to_owned(),
            )
        ));
        assert!(retryable_manifest_planning_failure(
            &TransportError::Invalid(
                "payload device DEVICE-1 is not a free inactive payload".to_owned(),
            )
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
}
