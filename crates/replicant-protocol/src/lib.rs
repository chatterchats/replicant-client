//! Stable, versioned DTOs shared by `replicantd` and its local frontends.
//!
//! This crate contains only the application's normalized local protocol. Raw
//! upstream Replicant Space events and authentication data do not belong here.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current local application protocol version.
pub const PROTOCOL_VERSION: u16 = 1;

/// Device-tag prefixes that mark a device as claimed by a running workflow.
///
/// Every workflow tags the devices it owns so that other workflows do not
/// select, move, or repurpose them mid-mission. This list is the single
/// authority: it previously existed as verbatim copies in the transport and
/// relay layers, where a prefix added to one copy and not the other would let
/// one workflow fly off with another's claimed hardware.
///
/// Use [`workflow_tag_reserved`] / [`workflow_reserved`] rather than matching
/// these directly.
pub const RESERVED_WORKFLOW_TAG_PREFIXES: &[&str] = &[
    "evt-m:", "evt-r:", "boot-m:", "boot-r:", "region:", "mine-m:", "mine-b:", "mine-r:",
    "mine-s:", "relay-m:", "relay-b:", "relay-s:", "infra-r:", "infra-s:",
];

/// Returns whether one tag marks a device as claimed by a running workflow.
#[must_use]
pub fn workflow_tag_reserved(tag: &str) -> bool {
    RESERVED_WORKFLOW_TAG_PREFIXES
        .iter()
        .any(|prefix| tag.starts_with(prefix))
}

/// Returns whether any tag marks a device as claimed by a running workflow.
#[must_use]
pub fn workflow_reserved(tags: &[String]) -> bool {
    tags.iter().any(|tag| workflow_tag_reserved(tag.as_str()))
}

/// A request or response payload carrying its wire protocol version.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Versioned<T> {
    /// Wire protocol version used to encode the payload.
    pub protocol_version: u16,
    /// Typed message body.
    pub payload: T,
}

impl<T> Versioned<T> {
    /// Wraps a payload with the current protocol version.
    pub fn current(payload: T) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            payload,
        }
    }
}

/// Stable identifier for a workflow instance.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowId(pub String);

/// Stable identifier for a persisted automation trigger.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TriggerId(pub String);

/// Stable identifier for a registered report, action, or workflow kind.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationKind(pub String);

/// Stable identifier for an application entity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EntityId(pub String);

/// Stable identifier for a saved or running query.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct QueryId(pub String);

/// Kind of normalized entity addressable by the local application.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum EntityKind {
    /// Star system.
    System,
    /// Location within a system.
    Location,
    /// Player-controlled replicant.
    Replicant,
    /// Device or vessel.
    Device,
    /// Inventory record.
    Inventory,
    /// Autofactory.
    Autofactory,
    /// Cargo record.
    Cargo,
    /// Managed client operation.
    Operation,
    /// Persisted workflow.
    Workflow,
}

/// Typed reference to a normalized application entity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct EntityRef {
    /// Entity category.
    pub kind: EntityKind,
    /// Stable entity identifier.
    pub id: EntityId,
}

/// Small frontend-safe description of an entity used by cross-cutting UI.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EntitySummary {
    /// Stable entity address.
    pub entity: EntityRef,
    /// Primary human-readable label.
    pub label: String,
    /// Optional supporting label such as a device type or replicant name.
    pub secondary_label: Option<String>,
    /// Containing system, when known.
    pub system: Option<String>,
    /// Current location, when known.
    pub location: Option<String>,
    /// Domain-specific type rendered as a stable wire value, when useful.
    pub entity_type: Option<String>,
    /// Domain-specific status rendered as a stable wire value, when useful.
    pub status: Option<String>,
}

/// Daemon availability state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    /// Daemon and required services are available.
    Healthy,
    /// Daemon is available but at least one service is impaired.
    Degraded,
    /// Daemon cannot currently serve normal application traffic.
    Unhealthy,
}

/// Daemon health and build information.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DaemonHealth {
    /// Overall availability.
    pub status: HealthStatus,
    /// Daemon package version.
    pub daemon_version: String,
    /// Human-readable, non-secret status detail.
    pub detail: Option<String>,
}

/// Managed-client synchronization phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPhase {
    /// Managed client is starting.
    Starting,
    /// Initial or recovery synchronization is running.
    Syncing,
    /// Durable state is current and the event stream is connected.
    Ready,
    /// Durable state remains usable but synchronization is impaired.
    Degraded,
    /// No upstream connection is available.
    Offline,
}

/// Runtime view of managed-client synchronization.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSyncStatus {
    /// Current synchronization phase.
    pub phase: SyncPhase,
    /// Latest durable managed-state revision.
    pub revision: u64,
    /// Unix milliseconds of the latest upstream event, when known.
    pub last_event_at_ms: Option<i64>,
    /// Human-readable, non-secret status detail.
    pub detail: Option<String>,
}

/// Frontend-safe global automation safety state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AutomationStatus {
    /// Whether non-manual triggers may launch new work.
    pub automatic_triggers_enabled: bool,
    /// Whether workflow execution is globally paused.
    pub workflows_paused: bool,
}

/// Metadata describing an application snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Monotonically increasing application revision.
    pub revision: u64,
    /// Unix milliseconds when the snapshot was produced.
    pub generated_at_ms: i64,
}

/// Cross-cutting entity summaries returned by the daemon.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EntityIndexSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Stable, normalized summaries sorted by entity address.
    pub entities: Vec<EntitySummary>,
}

/// Workflow ownership of an exclusively claimed device.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeviceClaim {
    /// Workflow holding the claim.
    pub workflow_id: WorkflowId,
    /// Registered workflow kind.
    pub workflow_kind: OperationKind,
    /// Current workflow lifecycle state.
    pub workflow_status: WorkflowStatus,
}

/// Frontend-safe operational view of one managed device.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeviceSummary {
    /// Stable device address.
    pub entity: EntityRef,
    /// Forward-compatible device type wire value.
    pub device_type: Option<String>,
    /// Forward-compatible device status wire value.
    pub status: Option<String>,
    /// Managed-state ownership scope.
    pub ownership: String,
    /// Assigned replicant code, when present.
    pub owner: Option<String>,
    /// Assigned replicant display name, when known.
    #[serde(default)]
    pub owner_name: Option<String>,
    /// Containing system, when known.
    pub system: Option<String>,
    /// Current location, when known.
    pub location: Option<String>,
    /// User-defined device tags.
    pub tags: Vec<String>,
    /// Parent attachment relationship.
    pub attached_to: Option<String>,
    /// Parent stow relationship.
    pub stowed_in: Option<String>,
    /// AMI controller relationship.
    pub controller: Option<String>,
    /// Configured linked device relationship.
    pub linked_device: Option<String>,
    /// Directly attached child devices.
    pub attached_devices: Vec<String>,
    /// Devices adopted by this controller.
    pub controlled_devices: Vec<String>,
    /// Devices carried in this device.
    pub stowed_devices: Vec<String>,
    /// Maximum attached-device count, when reported.
    pub attach_capacity: Option<i64>,
    /// Cargo/stow capacity, when reported.
    pub cargo_capacity: Option<i64>,
    /// Used cargo/stow capacity, when reported.
    pub cargo_used: Option<i64>,
    /// Normalized operational capacity in percentage points.
    pub operational_capacity_percent: Option<f64>,
    /// Active AMI directive wire value, when present.
    pub active_directive: Option<String>,
    /// Active AMI directive status, when present.
    pub directive_status: Option<String>,
    /// Final travel destination, when traveling.
    pub travel_destination: Option<String>,
    /// Exclusive runtime claim, when held.
    pub claim: Option<DeviceClaim>,
}

/// Typed device fleet projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DevicesSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Stable device rows sorted by code.
    pub devices: Vec<DeviceSummary>,
}

/// One durable Survey workflow and its structured route progress.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SurveyMissionSummary {
    /// Persisted workflow lifecycle.
    pub workflow: WorkflowSummary,
    /// Assigned replicant.
    pub replicant: String,
    /// Survey carrier or vessel.
    pub vessel: String,
    /// Route centre.
    pub center: String,
    /// Current execution phase.
    pub phase: String,
    /// Number of completed route stops.
    pub completed_systems: usize,
    /// Total route stops.
    pub total_systems: usize,
    /// Next system in the route, when present.
    pub next_system: Option<String>,
    /// Assigned survey controller.
    pub controller: Option<String>,
    /// Assigned survey drones.
    pub drones: Vec<String>,
}

/// Typed Survey mission dashboard projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SurveySnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Active durable Survey missions.
    pub missions: Vec<SurveyMissionSummary>,
    /// Managed devices assigned to those missions.
    pub fleet: Vec<DeviceSummary>,
}

/// Completeness of one discovered mining installation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MiningInstallationStatus {
    /// Every required managed device is present and adopted.
    Complete,
    /// At least one installation device exists, but the set is incomplete.
    Partial,
}

/// Managed devices forming one mining installation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MiningInstallationSummary {
    /// Stable location-derived row identity.
    pub id: String,
    /// Containing system, when known.
    pub system: Option<String>,
    /// Installation location, when known.
    pub location: Option<String>,
    /// Mining controller, when present.
    pub controller: Option<DeviceSummary>,
    /// Mining drones at this installation.
    pub miners: Vec<DeviceSummary>,
    /// Survey controller, when present.
    pub survey_controller: Option<DeviceSummary>,
    /// Survey drones at this installation.
    pub survey_drones: Vec<DeviceSummary>,
    /// Maintenance device, when present.
    pub maintenance_device: Option<DeviceSummary>,
    /// Missing device requirements, suitable for operator display.
    pub missing: Vec<String>,
    /// Derived installation completeness.
    pub status: MiningInstallationStatus,
}

/// Typed Mining mission dashboard projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MiningSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Discovered managed mining installations.
    pub installations: Vec<MiningInstallationSummary>,
    /// Active mining-related durable workflows, when registered.
    pub workflows: Vec<WorkflowSummary>,
}

/// One durable relay expansion and its current route progress.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RelayExpansionSummary {
    /// Persisted workflow lifecycle.
    pub workflow: WorkflowSummary,
    /// Assigned replicant.
    pub replicant: String,
    /// Manufacturing hub.
    pub hub: String,
    /// Requested target systems.
    pub targets: Vec<String>,
    /// Current relay executor phase.
    pub phase: String,
    /// Completed deployment stops.
    pub completed_stops: usize,
    /// Total planned stops, when a checkpoint exists.
    pub total_stops: Option<usize>,
    /// Next incomplete system.
    pub next_system: Option<String>,
    /// Relays still awaiting manufacture or discovery.
    pub pending_relays: Option<usize>,
}

/// Typed Relay mission dashboard projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RelaySnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Owned, deployed relay-capable devices.
    pub relays: Vec<DeviceSummary>,
    /// Relay devices staged or claimed for expansion.
    pub staged_relays: Vec<DeviceSummary>,
    /// Systems covered by an active relay.
    pub connected_systems: usize,
    /// Active relay-network edges owned by the galaxy projection.
    pub relay_edges: Vec<GalaxyEdge>,
    /// Active durable relay expansions.
    pub expansions: Vec<RelayExpansionSummary>,
}

/// One recent regional bootstrap mission projected from finite action history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BootstrapMissionSummary {
    /// Durable bootstrap mission identifier.
    pub mission_id: String,
    /// Latest finite action execution identifier.
    pub execution_id: String,
    /// Target region.
    pub region: String,
    /// Source manufacturing hub.
    pub source_hub: String,
    /// Planned landing system.
    pub target_system: String,
    /// Planned landing location or entry point.
    pub target_location: String,
    /// Current persisted mission phase.
    pub phase: String,
    /// Devices reserved by the mission.
    pub reserved_devices: usize,
    /// Devices assigned to carrier loads.
    pub loaded_devices: usize,
    /// Established regional capital, when known.
    pub capital_system: Option<String>,
    /// Selected mining systems or belts.
    pub selected_sites: usize,
    /// Persisted mission warnings.
    pub warnings: Vec<String>,
    /// Whether the persisted phase is terminal.
    pub completed: bool,
    /// Latest action completion time.
    pub updated_at_ms: i64,
}

/// Typed Bootstrap mission dashboard projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BootstrapSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Active and recent missions observed through registered bootstrap actions.
    pub missions: Vec<BootstrapMissionSummary>,
}

/// Kind of requirement contributing to a location event.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventRequirementKind {
    /// A resource quantity.
    Resource,
    /// A device quantity.
    Device,
}

/// Normalized requirement and progress for one event criterion.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventRequirementSummary {
    /// Requirement category.
    pub kind: EventRequirementKind,
    /// Open resource or device type.
    pub item: String,
    /// Total quantity required.
    pub required: i64,
    /// Quantity confirmed by event progress.
    pub completed: i64,
    /// Quantity still outstanding.
    pub remaining: i64,
}

/// One alternative method for completing an event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventCriterionSummary {
    /// Stable criterion name supplied by the event.
    pub name: String,
    /// Structured requirements and current progress.
    pub requirements: Vec<EventRequirementSummary>,
    /// Whether all known requirements are complete.
    pub complete: bool,
}

/// A normalized resource or device reward.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventRewardItem {
    /// Open resource or device type.
    pub item: String,
    /// Reward quantity.
    pub quantity: i64,
}

/// Structured rewards supplied by an event.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventRewardsSummary {
    /// Resource rewards.
    pub resources: Vec<EventRewardItem>,
    /// Device rewards, when the upstream event model supplies them.
    pub devices: Vec<EventRewardItem>,
    /// Experience reward.
    pub xp: Option<i64>,
    /// Civilisation-point reward.
    pub civilisation_points: Option<i64>,
    /// Completion achievement key.
    pub completion_achievement: Option<String>,
}

/// One discovered location event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventSummary {
    /// Stable event designation.
    pub designation: String,
    /// Display title.
    pub title: String,
    /// Open event type.
    pub event_type: Option<String>,
    /// Open event category when supplied separately.
    pub category: Option<String>,
    /// Event tier.
    pub tier: Option<i64>,
    /// Containing system.
    pub system: String,
    /// Event location.
    pub location: String,
    /// Display description.
    pub description: Option<String>,
    /// Alternative completion criteria.
    pub criteria: Vec<EventCriterionSummary>,
    /// Structured event rewards.
    pub rewards: EventRewardsSummary,
    /// Open completion status.
    pub status: Option<String>,
    /// Discovery timestamp supplied by the API.
    pub discovered_at: Option<String>,
    /// Completion timestamp supplied by the API.
    pub completed_at: Option<String>,
}

/// Typed discovered-event dashboard projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventsSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Discovered location events.
    pub events: Vec<EventSummary>,
}

/// One durable account event from the managed event journal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountEventSummary {
    /// Stable stream/event identifier.
    pub id: String,
    /// Event name, for example `ami.survey.digest`.
    pub name: String,
    /// Normalized event category.
    pub category: String,
    /// Related device when one was identified by the managed event reducer.
    pub device: Option<EntityRef>,
    /// Related replicant when one was identified.
    pub replicant: Option<EntityRef>,
    /// Related system when one was identified.
    pub system: Option<String>,
    /// Related location when one was identified.
    pub location: Option<String>,
    /// Upstream event timestamp.
    pub occurred_at: String,
    /// Sanitized event-specific payload.
    pub payload: Value,
    /// Whether this is an AMI digest intended as a fleet activity summary.
    pub ami_digest: bool,
}

/// Filterable account event-log projection, distinct from location events.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountEventsSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Durable account-event cursor after the most recent managed apply.
    pub cursor: Option<String>,
    /// Matching events in newest-first presentation order.
    pub events: Vec<AccountEventSummary>,
}

/// One diagnostic entry from an owned device's upstream event log.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeviceLogSummary {
    /// Server numeric log identifier when supplied.
    pub id: Option<i64>,
    /// Event timestamp.
    pub created_at: Option<String>,
    /// Device code supplied by the log entry.
    pub device_code: Option<String>,
    /// Device type supplied by the log entry.
    pub device_type: Option<String>,
    /// Open event type.
    pub event_type: Option<String>,
    /// Human-readable log message.
    pub message: Option<String>,
    /// Event-specific payload.
    pub payload: Value,
}

/// Bounded diagnostic log projection for one device.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeviceLogsSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Device whose log was requested.
    pub device: EntityRef,
    /// Log entries returned by the upstream diagnostic endpoint.
    pub events: Vec<DeviceLogSummary>,
    /// Cursor for the next upstream page, when any.
    pub next_cursor: Option<i64>,
}

/// One normalized item on either side of a player trade.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeItemSummary {
    /// Open item category inferred from the upstream exchange object.
    pub kind: String,
    /// Open resource, device, currency, or item key.
    pub item: String,
    /// Quantity when modeled numerically.
    pub quantity: Option<f64>,
}

/// One current trade offered by a controller.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeSummary {
    /// Stable trade code.
    pub trade_code: String,
    /// Public trade name.
    pub name: Option<String>,
    /// Remaining stock.
    pub current_stock: Option<i64>,
    /// Initial stock.
    pub initial_stock: Option<i64>,
    /// Items required from the buyer.
    pub requested: Vec<TradeItemSummary>,
    /// Items returned to the buyer.
    pub offered: Vec<TradeItemSummary>,
    /// Server creation timestamp when supplied.
    pub created_at: Option<String>,
}

/// One visible player trade controller and its current trades.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeControllerSummary {
    /// Controller device address.
    pub entity: EntityRef,
    /// Public shop name.
    pub shop_name: Option<String>,
    /// Public description.
    pub description: Option<String>,
    /// Whether the shop is local to the viewing replicant.
    pub is_local: bool,
    /// Public owner name.
    pub owner_name: Option<String>,
    /// Public owner replicant code.
    pub owner_replicant: Option<String>,
    /// Public system.
    pub system: Option<String>,
    /// Public location.
    pub location: Option<String>,
    /// Total stock reported by the directory.
    pub total_stock: Option<i64>,
    /// Number of trades reported by the directory.
    pub trade_count: Option<i64>,
    /// Current normalized trades.
    pub trades: Vec<TradeSummary>,
    /// Active workflow claiming this controller, when present.
    pub workflow: Option<WorkflowSummary>,
}

/// Typed managed trading dashboard projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TradeSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Replicant whose visible trade directory was used.
    pub viewer: Option<EntityRef>,
    /// Visible trade controllers and their current trades.
    pub controllers: Vec<TradeControllerSummary>,
}

/// One simulation scenario offered by a replicant interface.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SimulationScenarioSummary {
    /// Stable scenario code.
    pub code: String,
    /// Display name.
    pub name: Option<String>,
    /// Short scenario description.
    pub description: Option<String>,
    /// Extended rules description.
    pub long_description: Option<String>,
    /// Objective type.
    pub objective_type: Option<String>,
    /// Objective target.
    pub objective_target: Option<i64>,
    /// Timeout in hours.
    pub timeout_hours: Option<f64>,
    /// Scenario version.
    pub version: Option<i64>,
    /// Entry cost as a normalized resource list.
    pub entry_cost: Vec<InventoryQuantity>,
}

/// One active or archived simulation run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SimulationRunSummary {
    /// Simulation run identifier.
    pub id: i64,
    /// Interface device for active runs, when known.
    pub interface: Option<EntityRef>,
    /// Whether the authenticated account owns the run.
    pub is_mine: bool,
    /// Replicant code when available from durable history.
    pub replicant: Option<EntityRef>,
    /// Replicant display name for active public runs.
    pub replicant_name: Option<String>,
    /// Scenario code.
    pub scenario_code: Option<String>,
    /// Scenario display name.
    pub scenario_name: Option<String>,
    /// Managed lifecycle for locally tracked owned runs.
    pub lifecycle: Option<String>,
    /// Start timestamp.
    pub started_at: Option<String>,
    /// Completion timestamp.
    pub completed_at: Option<String>,
    /// Abandonment timestamp.
    pub abandoned_at: Option<String>,
    /// Timeout timestamp.
    pub timed_out_at: Option<String>,
    /// Competitive score in seconds for completed history.
    pub score_seconds: Option<i64>,
    /// Resources mined in the run.
    pub resources_mined: Option<i64>,
    /// Devices printed in the run.
    pub devices_printed: Option<i64>,
    /// Timeout in hours for an active run.
    pub timeout_hours: Option<f64>,
}

/// One discovered datacentre `replicant_interface` and its live scenario state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SimulationInterfaceSummary {
    /// Interface device.
    pub device: DeviceSummary,
    /// Scenarios currently offered by this interface.
    pub scenarios: Vec<SimulationScenarioSummary>,
    /// Runs currently active on this interface.
    pub active: Vec<SimulationRunSummary>,
    /// Non-fatal live-read error, allowing other interfaces/history to render.
    pub error: Option<String>,
}

/// Simulation browser, active-run, and personal-history projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SimulationsSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Known owned simulator interfaces.
    pub interfaces: Vec<SimulationInterfaceSummary>,
    /// Durable managed simulation realm history.
    pub managed_history: Vec<SimulationRunSummary>,
    /// Fresh account run history, including score/outcome counters.
    pub account_history: Vec<SimulationRunSummary>,
}

/// One unlocked account blueprint with manufacturing metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlueprintSummary {
    /// Device type printed by the blueprint.
    pub device_type: String,
    /// Short description.
    pub short_description: Option<String>,
    /// Full description.
    pub description: Option<String>,
    /// Base print time in seconds.
    pub print_time_seconds: Option<f64>,
    /// Resource cost.
    pub resources: Vec<InventoryQuantity>,
    /// Component cost.
    pub components: Vec<InventoryQuantity>,
    /// Device feature flags.
    pub features: Vec<String>,
    /// Supported AMI directives.
    pub directives: Vec<String>,
    /// Cargo capacity.
    pub cargo_capacity: Option<i64>,
    /// Attach capacity.
    pub attach_capacity: Option<i64>,
    /// Stow capacity.
    pub stow_capacity: Option<i64>,
    /// Autofactory queue size, when applicable.
    pub queue_size: Option<i64>,
}

/// Unlocked blueprint catalogue projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BlueprintsSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Unlocked blueprints.
    pub blueprints: Vec<BlueprintSummary>,
}

/// One result from the public replicant directory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectoryReplicantSummary {
    /// Replicant address.
    pub entity: EntityRef,
    /// Public display name.
    pub name: Option<String>,
    /// Last known location.
    pub last_location: Option<String>,
    /// Whether the entry represents an NPC.
    pub is_npc: Option<bool>,
}

/// Public multiplayer directory projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectorySnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Optional search phrase that produced the page.
    pub query: Option<String>,
    /// Directory entries.
    pub replicants: Vec<DirectoryReplicantSummary>,
}

/// Public profile detail for one replicant selected from the directory.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectoryReplicantDetail {
    /// Replicant address.
    pub entity: EntityRef,
    /// Public display name.
    pub name: Option<String>,
    /// Whether the profile represents an NPC.
    pub is_npc: Option<bool>,
    /// Public status when supplied by the game API.
    pub status: Option<String>,
    /// Current public location when supplied.
    pub location: Option<String>,
    /// Hosted vessel/device when publicly visible.
    pub hosted_device: Option<EntityRef>,
}

/// Typed response for one public replicant profile.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectoryReplicantDetailSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Selected public profile.
    pub replicant: DirectoryReplicantDetail,
}

/// One tutorial objective.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TutorialStepSummary {
    /// Stable objective key.
    pub key: Option<String>,
    /// Objective description.
    pub description: Option<String>,
    /// API/gameplay hint.
    pub hint: Option<String>,
    /// Whether the objective is complete.
    pub completed: Option<bool>,
    /// Whether this is the current objective.
    pub current: Option<bool>,
}

/// Tutorial progress with optional detail steps.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TutorialSummary {
    /// Stable tutorial slug.
    pub slug: String,
    /// Display name.
    pub name: Option<String>,
    /// Description.
    pub description: Option<String>,
    /// Tutorial order.
    pub order: Option<i64>,
    /// Whether the tutorial is complete.
    pub completed: Option<bool>,
    /// Current step index.
    pub current_step: Option<i64>,
    /// Total steps.
    pub total_steps: Option<i64>,
    /// Detailed steps when requested for this tutorial.
    pub steps: Vec<TutorialStepSummary>,
}

/// Account tutorial/onboarding projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TutorialsSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Tutorials in server-defined order.
    pub tutorials: Vec<TutorialSummary>,
    /// Detailed tutorial whose steps were fetched, when any.
    pub selected: Option<TutorialSummary>,
}

/// Typed report catalogue and recent execution projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReportsSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Registered read-only report descriptors.
    pub reports: Vec<ReportDescriptor>,
    /// Recent report executions, newest first.
    pub executions: Vec<FiniteExecution>,
}

/// One account-inbox message.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InboxMessageSummary {
    /// Upstream message identifier when supplied.
    pub id: Option<i64>,
    /// Display title.
    pub title: Option<String>,
    /// Message body.
    pub body: Option<String>,
    /// Message category.
    pub category: Option<String>,
    /// Message type.
    pub message_type: Option<String>,
    /// Whether the account has read the message.
    pub is_read: Option<bool>,
    /// Server timestamp when supplied.
    pub created_at: Option<String>,
}

/// One channel observed by a relay device.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BobnetChannelSummary {
    /// Channel name.
    pub name: String,
    /// Last activity timestamp when supplied.
    pub last_active: Option<String>,
}

/// One message observed in a relay history.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BobnetMessageSummary {
    /// Upstream message identifier when supplied.
    pub id: Option<i64>,
    /// Channel the message was sent on.
    pub channel: Option<String>,
    /// Message body.
    pub body: Option<String>,
    /// Sending replicant code when supplied; NPC senders may also have codes.
    pub sender: Option<String>,
    /// Sending replicant display name when supplied; NPC senders may also have names.
    pub sender_name: Option<String>,
    /// Whether the message was identified as NPC/system chatter.
    pub is_npc_or_system: bool,
    /// Sender's current system when supplied.
    pub current_system: Option<String>,
    /// Server timestamp when supplied.
    pub created_at: Option<String>,
}

/// Typed account notification inbox projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MessagesSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Account-wide notification inbox.
    pub inbox: Vec<InboxMessageSummary>,
    /// Account-wide unread count when supplied.
    pub unread_count: Option<i64>,
}

/// One owned replicant that can be selected as a BobNet sender.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BobnetReplicantSummary {
    /// Replicant address.
    pub entity: EntityRef,
    /// Display name when supplied.
    pub name: Option<String>,
    /// Current managed status when supplied.
    pub status: Option<String>,
    /// Current location when supplied.
    pub location: Option<String>,
}

/// IRC-style BobNet session projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BobnetSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Active relay/hub devices that can provide channel history.
    pub sources: Vec<DeviceSummary>,
    /// History source selected for this response.
    pub selected_source: Option<String>,
    /// Channels visible through the selected source.
    pub channels: Vec<BobnetChannelSummary>,
    /// Recent history visible through the selected source.
    pub messages: Vec<BobnetMessageSummary>,
    /// Owned replicants available as message senders.
    pub replicants: Vec<BobnetReplicantSummary>,
    /// Opaque cursor for older history, when supplied.
    pub next_cursor: Option<i64>,
    /// Total messages visible to the source when supplied.
    pub total_messages: Option<i64>,
    /// Non-fatal history/channel read warning.
    pub error: Option<String>,
}

/// One relay-capable managed device and its observed channel availability.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NetworkRelaySummary {
    /// Managed device projection.
    pub device: DeviceSummary,
    /// Channels currently observable through the relay.
    pub channels: Vec<BobnetChannelSummary>,
    /// Safe-read failure for this relay, when other network data remains useful.
    pub error: Option<String>,
}

/// One replicant related to the authenticated account.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountReplicantSummary {
    /// Replicant address.
    pub entity: EntityRef,
    /// Display name when supplied.
    pub name: Option<String>,
    /// Current system when supplied.
    pub system: Option<String>,
    /// Current location when supplied.
    pub location: Option<String>,
    /// Hosted device when supplied.
    pub hosted_device: Option<EntityRef>,
}

/// Typed account and BobNet network-status projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NetworkSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Authenticated account display name when supplied.
    pub account_name: Option<String>,
    /// Account status when supplied.
    pub account_status: Option<String>,
    /// Account's subscribed BobNet channels.
    pub subscribed_channels: Vec<String>,
    /// Replicants belonging to the account.
    pub replicants: Vec<AccountReplicantSummary>,
    /// Known managed relay devices.
    pub relays: Vec<NetworkRelaySummary>,
}

/// One earned account achievement.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AchievementSummary {
    /// Stable achievement key.
    pub key: String,
    /// Display title when supplied.
    pub title: Option<String>,
    /// Display description when supplied.
    pub description: Option<String>,
    /// Achievement category when supplied.
    pub category: Option<String>,
    /// XP reward when supplied.
    pub xp_reward: Option<i64>,
    /// Earned timestamp when supplied.
    pub achieved_at: Option<String>,
}

/// One account-level species reputation entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReputationSummary {
    /// Stable species key.
    pub species: String,
    /// Species display name when supplied.
    pub name: Option<String>,
    /// Aggregated account reputation when supplied.
    pub value: Option<f64>,
    /// Standing description when supplied.
    pub description: Option<String>,
    /// Dominant species trait when supplied.
    pub trait_name: Option<String>,
}

/// Typed account progression and standing projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StandingSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Account-wide XP total when supplied.
    pub experience_points_total: Option<i64>,
    /// Civilisation points are currently not exposed by the account API.
    pub civilisation_points: Option<i64>,
    /// Earned achievements.
    pub achievements: Vec<AchievementSummary>,
    /// Aggregated species reputation.
    pub reputation: Vec<ReputationSummary>,
}

/// One published leaderboard descriptor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaderboardBoardSummary {
    /// Stable board key.
    pub key: String,
    /// Display name when supplied.
    pub name: Option<String>,
    /// Description when supplied.
    pub description: Option<String>,
    /// Board type when supplied.
    pub board_type: Option<String>,
}

/// One normalized ranked leaderboard entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeaderboardEntrySummary {
    /// Rank when supplied.
    pub rank: Option<i64>,
    /// Replicant link when supplied.
    pub replicant: Option<EntityRef>,
    /// Display name when supplied.
    pub name: Option<String>,
    /// Colony designation when supplied.
    pub designation: Option<String>,
    /// Ranked value when supplied.
    pub value: Option<f64>,
    /// Contribution count when supplied.
    pub contribution_count: Option<i64>,
}

/// Typed refresh-on-demand leaderboard projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LeaderboardsSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Published standard leaderboards.
    pub boards: Vec<LeaderboardBoardSummary>,
    /// Selected board key.
    pub selected_board: Option<String>,
    /// Ranked rows for the selected board.
    pub entries: Vec<LeaderboardEntrySummary>,
}

/// One active or queued Autofactory print job.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FactoryJobSummary {
    /// Canonical device type, or `unknown` when omitted upstream.
    pub device_type: String,
    /// Number of units represented by the job.
    pub quantity: i64,
    /// Reported seconds remaining, when available.
    pub eta_seconds: Option<f64>,
    /// Tags applied to the completed device.
    pub tags: Vec<String>,
}

/// Whether an Autofactory can accept work or is currently occupied.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutofactoryAvailability {
    /// Ready to accept work.
    Available,
    /// Printing or holding queued work.
    Busy,
    /// Compacted or transitioning and unable to print.
    Unavailable,
}

/// Operational manufacturing row for one Autofactory.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AutofactorySummary {
    /// Shared managed-device projection.
    pub device: DeviceSummary,
    /// Current availability derived from live factory state.
    pub availability: AutofactoryAvailability,
    /// Maximum queued units reported by the factory.
    pub queue_capacity: Option<i64>,
    /// Units currently occupying the queue.
    pub queued_units: i64,
    /// Active print job, when present.
    pub current_job: Option<FactoryJobSummary>,
    /// Jobs waiting behind the active print.
    pub queued_jobs: Vec<FactoryJobSummary>,
}

/// Aggregate manufacturing utilization.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AutofactoryUtilization {
    /// Total managed Autofactories.
    pub total: usize,
    /// Factories with active or queued work.
    pub busy: usize,
    /// Factories ready to accept work.
    pub available: usize,
    /// Factories unable to print in their current state.
    pub unavailable: usize,
    /// Total queued print units.
    pub queued_units: i64,
    /// Busy factories as a percentage of printable factories.
    pub utilization_percent: f64,
}

/// Typed Autofactory dashboard projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AutofactorySnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Aggregate manufacturing state.
    pub utilization: AutofactoryUtilization,
    /// Stable rows sorted by factory code.
    pub factories: Vec<AutofactorySummary>,
}

/// Resource quantity currently carried by a device.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CargoResourceSummary {
    /// Canonical resource type.
    pub resource: String,
    /// Positive carried quantity.
    pub quantity: i64,
}

/// Operational cargo row for one capability-bearing carrier.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CargoCarrierSummary {
    /// Shared managed-device projection.
    pub device: DeviceSummary,
    /// Resources currently in cargo.
    pub resources: Vec<CargoResourceSummary>,
    /// Number of occupied attachment slots.
    pub attachment_used: i64,
}

/// Typed cargo and transport dashboard projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CargoSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Total reported cargo use across carriers.
    pub cargo_used: i64,
    /// Total reported cargo capacity across carriers.
    pub cargo_capacity: i64,
    /// Total occupied attachment slots.
    pub attachment_used: i64,
    /// Total reported attachment capacity.
    pub attachment_capacity: i64,
    /// Stable carrier rows sorted by device code.
    pub carriers: Vec<CargoCarrierSummary>,
}

/// Managed inventory ownership scope.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InventoryOwnerKind {
    /// Account-wide inventory.
    Account,
    /// Inventory carried by a replicant.
    Replicant,
    /// Inventory stored at a location.
    Location,
}

/// One resource quantity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryQuantity {
    /// Stable managed resource wire value.
    pub resource: String,
    /// Positive quantity available.
    pub quantity: i64,
}

/// Inventory grouped by its managed owner and physical location.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryLocationSummary {
    /// Managed ownership scope.
    pub owner_kind: InventoryOwnerKind,
    /// Managed owner identifier.
    pub owner: String,
    /// Containing system, when known.
    pub system: Option<String>,
    /// Physical location, when known.
    pub location: Option<String>,
    /// Total positive quantity at this scope.
    pub total_quantity: i64,
    /// Stable resource rows sorted by resource name.
    pub resources: Vec<InventoryQuantity>,
}

/// One resource's quantity at an inventory scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryDistribution {
    /// Managed ownership scope.
    pub owner_kind: InventoryOwnerKind,
    /// Managed owner identifier.
    pub owner: String,
    /// Containing system, when known.
    pub system: Option<String>,
    /// Physical location, when known.
    pub location: Option<String>,
    /// Positive quantity at this scope.
    pub quantity: i64,
}

/// One resource aggregated across all managed inventory scopes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryResourceSummary {
    /// Stable managed resource wire value.
    pub resource: String,
    /// Account-wide total quantity.
    pub total_quantity: i64,
    /// Stable distribution rows sorted by location and owner.
    pub distribution: Vec<InventoryDistribution>,
}

/// Typed managed inventory projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventorySnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Account-wide quantity across positive resource stacks.
    pub total_quantity: i64,
    /// Inventory scopes sorted by system, location, and owner.
    pub locations: Vec<InventoryLocationSummary>,
    /// Resources sorted by stable wire value.
    pub resources: Vec<InventoryResourceSummary>,
}

/// Three-dimensional galactic coordinates in light-years.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GalaxyPoint {
    /// Galactic X coordinate.
    pub x: f64,
    /// Galactic Y coordinate.
    pub y: f64,
    /// Galactic Z coordinate.
    pub z: f64,
}

/// Application-level exploration state for a known star.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GalaxyExploration {
    /// The system has not been explored by an owned replicant.
    Undiscovered,
    /// The system is known but not confirmed explored.
    Partial,
    /// An owned replicant has confirmed exploration.
    Explored,
}

/// One renderer-ready star without upstream API structure leakage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GalaxyStar {
    /// Canonical system designation.
    pub id: String,
    /// Display name, when distinct from the designation.
    pub name: Option<String>,
    /// Known spectral classification.
    pub spectral_type: Option<String>,
    /// Absolute galactic coordinates.
    pub position: GalaxyPoint,
    /// Best application-owned exploration state.
    pub exploration: GalaxyExploration,
    /// Whether an owned replicant is currently in this system.
    pub current: bool,
    /// Whether the system contains a known hub.
    pub has_hub: bool,
    /// Whether life has been discovered in the system.
    pub has_life: bool,
    /// Whether an active owned relay is present.
    pub has_relay: bool,
    /// Whether committed location data identifies a megastructure in the system.
    #[serde(default)]
    pub has_megastructure: bool,
}

/// A connection between two known systems.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GalaxyEdge {
    /// Origin system designation.
    pub from: String,
    /// Destination system designation.
    pub to: String,
}

/// Active travel projected onto known systems.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GalaxyTravel {
    /// Traveling replicant or device.
    pub entity: EntityRef,
    /// Origin system designation.
    pub from: String,
    /// Destination system designation.
    pub to: String,
    /// ISO-8601 departure time, when known.
    pub started_at: Option<String>,
    /// ISO-8601 arrival time, when known.
    pub arrives_at: Option<String>,
}

/// A player-discovered signal at absolute galactic coordinates.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GalaxySignal {
    /// Stable signal identifier.
    pub id: String,
    /// Human-readable label, when known.
    pub label: Option<String>,
    /// Absolute galactic coordinates.
    pub position: GalaxyPoint,
}

/// Visual overlay supported by the galaxy renderer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GalaxyOverlayKind {
    /// Discovered life.
    Life,
    /// Owned device presence.
    Device,
    /// Relay-network influence.
    Influence,
}

/// One system-centered renderer overlay.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GalaxyOverlay {
    /// Overlay category.
    pub kind: GalaxyOverlayKind,
    /// System designation.
    pub system: String,
    /// System coordinates.
    pub position: GalaxyPoint,
    /// Number of represented entities.
    pub count: u32,
}

/// Highlight connecting a workflow's anchor and target system.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GalaxyHighlight {
    /// Owning workflow.
    pub workflow_id: WorkflowId,
    /// Anchor system.
    pub from: String,
    /// Target system.
    pub to: String,
}

/// A system targeted by a currently active workflow.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GalaxyWorkflowTarget {
    /// Owning workflow.
    pub workflow_id: WorkflowId,
    /// Registered workflow kind.
    pub workflow_kind: OperationKind,
    /// Target system designation.
    pub system: String,
}

/// Complete application-owned galaxy scene returned independently of the general snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct GalaxySceneSnapshot {
    /// Key used to avoid reapplying unchanged scene geometry.
    pub revision: u64,
    /// Unix milliseconds when this scene was built.
    pub generated_at_ms: i64,
    /// Known stars with coordinates.
    pub stars: Vec<GalaxyStar>,
    /// Connections between active relay systems.
    pub relay_edges: Vec<GalaxyEdge>,
    /// Current device and replicant travel.
    pub active_travel: Vec<GalaxyTravel>,
    /// Player-discovered off-system signals.
    pub signals: Vec<GalaxySignal>,
    /// Workflow route highlights.
    pub highlights: Vec<GalaxyHighlight>,
    /// Device, life, and influence overlays.
    pub overlays: Vec<GalaxyOverlay>,
    /// Current workflow targets.
    pub workflow_targets: Vec<GalaxyWorkflowTarget>,
}

/// Two-dimensional renderer coordinates within one star system.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SystemPoint {
    /// Horizontal scene coordinate.
    pub x: f64,
    /// Vertical scene coordinate.
    pub y: f64,
}

/// Semantic marker category used by the system map and entity actions.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SystemMarkerKind {
    /// Central star.
    Star,
    /// Planetary body.
    Planet,
    /// Moon.
    Moon,
    /// Asteroid or other belt.
    Belt,
    /// Lagrange point.
    Lagrange,
    /// Other known location.
    Location,
    /// Traveling or stationary vessel.
    Vessel,
    /// General device.
    Device,
    /// Autofactory or factory-like device.
    Factory,
    /// Relay or system-hub device.
    Relay,
    /// Known location event.
    Event,
    /// Known resource extraction site.
    ResourceSite,
    /// Known datacentre or other megastructure location.
    Megastructure,
}

/// One renderer-ready object in a system scene.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemMarker {
    /// Stable marker identity.
    pub id: String,
    /// Human-readable marker label.
    pub label: String,
    /// Marker category.
    pub kind: SystemMarkerKind,
    /// Entity selected and inspected when the marker is activated.
    pub entity: EntityRef,
    /// Hosting location designation.
    pub location: String,
    /// Parent location for orbit rendering, when known.
    pub parent: Option<String>,
    /// Whether this orbital body is known to be in the star's habitable zone.
    #[serde(default)]
    pub in_habitable_zone: Option<bool>,
    /// Stable application-generated scene position.
    pub position: SystemPoint,
    /// Number of represented objects when the marker is an aggregate.
    pub count: u32,
}

/// Active travel between known locations in one system.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemTravel {
    /// Traveling replicant or device.
    pub entity: EntityRef,
    /// Origin location designation.
    pub from: String,
    /// Destination location designation.
    pub to: String,
    /// ISO-8601 departure time, when known.
    pub started_at: Option<String>,
    /// ISO-8601 arrival time, when known.
    pub arrives_at: Option<String>,
}

/// Active workflow projected into a system.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SystemWorkflowMarker {
    /// Workflow instance.
    pub workflow_id: WorkflowId,
    /// Registered workflow kind.
    pub workflow_kind: OperationKind,
    /// Location used to place the workflow marker.
    pub location: String,
}

/// Complete application-owned scene for one star system.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SystemSceneSnapshot {
    /// Canonical system designation.
    pub system: String,
    /// Key used to avoid reapplying unchanged scene geometry.
    pub revision: u64,
    /// Unix milliseconds when this scene was built.
    pub generated_at_ms: i64,
    /// Bodies, locations, devices, events, and resource sites.
    pub markers: Vec<SystemMarker>,
    /// Active in-system travel.
    pub active_travel: Vec<SystemTravel>,
    /// Active workflow locations.
    pub workflow_markers: Vec<SystemWorkflowMarker>,
}

/// Current daemon/runtime state returned to frontends as one consistent view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RuntimeSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Current managed-client synchronization state.
    pub sync: RuntimeSyncStatus,
    /// Persisted global automation safety state.
    pub automation: AutomationStatus,
    /// Current persisted workflows.
    pub workflows: Vec<WorkflowSummary>,
    /// Desired-state requirements with current fulfillment progress.
    #[serde(default)]
    pub requirements: Vec<RequirementSummary>,
    /// Current operational issues requiring visibility.
    #[serde(default)]
    pub notifications: Vec<Notification>,
    /// Revision each domain slice had reached when this snapshot was produced.
    ///
    /// Lets a reconnecting or lagging client decide which projections are
    /// stale by comparison instead of discarding everything and refetching.
    #[serde(default)]
    pub slice_revisions: BTreeMap<DomainSlice, u64>,
}

/// Summary-oriented operations dashboard projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OverviewSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Daemon availability.
    pub health: DaemonHealth,
    /// Managed-client synchronization state.
    pub sync: RuntimeSyncStatus,
    /// Global automation safety state.
    pub automation: AutomationStatus,
    /// Owned replicants and their current locations.
    pub replicants: Vec<OverviewReplicant>,
    /// Replicants currently traveling.
    pub active_travel: Vec<OverviewTravel>,
    /// Non-terminal workflows.
    pub active_workflows: Vec<WorkflowSummary>,
    /// Workflow totals grouped by lifecycle state.
    pub workflow_counts: Vec<WorkflowStatusCount>,
    /// Workflows with a persisted error or failed status.
    pub attention_workflows: Vec<WorkflowSummary>,
    /// Current operational notifications.
    pub notifications: Vec<Notification>,
    /// Most recent durable workflow activity, newest first.
    pub recent_activity: Vec<WorkflowActivity>,
}

/// One owned replicant on the operations dashboard.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OverviewReplicant {
    /// Replicant entity address.
    pub entity: EntityRef,
    /// Optional display name.
    pub name: Option<String>,
    /// Current containing system, when known.
    pub system: Option<String>,
    /// Current location, when known.
    pub location: Option<String>,
    /// Stable managed status wire value, when known.
    pub status: Option<String>,
}

/// Active owned-replicant travel summary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OverviewTravel {
    /// Traveling replicant.
    pub entity: EntityRef,
    /// Origin location, when known.
    pub from: Option<String>,
    /// Destination location, when known.
    pub to: Option<String>,
    /// ISO-8601 arrival time, when known.
    pub arrives_at: Option<String>,
}

/// Number of workflows in one lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowStatusCount {
    /// Lifecycle state.
    pub status: WorkflowStatus,
    /// Number of workflows in this state.
    pub count: usize,
}

/// Frontend-safe desired-state requirement evaluation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequirementSummary {
    /// Stable requirement identity.
    pub id: String,
    /// Human-readable label.
    pub name: String,
    /// Human-readable target summary.
    pub target: String,
    /// Human-readable system or location scope.
    pub scope: String,
    /// Desired count or quantity.
    pub desired: u64,
    /// Quantity currently present in managed state.
    pub actual: u64,
    /// Quantity covered by active child work.
    pub in_progress: u64,
    /// Remaining gap.
    pub missing: u64,
    /// Owning fulfillment workflow.
    pub workflow_id: WorkflowId,
    /// Parent workflow lifecycle state.
    pub status: WorkflowStatus,
}

/// Persisted workflow lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    /// Created but not yet started.
    Queued,
    /// Actively executing.
    Running,
    /// Durably waiting for an event, state change, or time.
    Waiting,
    /// Cooperatively paused.
    Paused,
    /// Reconciling persisted intent with managed state.
    Reconciling,
    /// Completed successfully.
    Succeeded,
    /// Stopped after an unrecoverable error.
    Failed,
    /// Cooperatively cancelled.
    Cancelled,
}

/// Compact workflow representation used by lists and deltas.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowSummary {
    /// Workflow instance identifier.
    pub id: WorkflowId,
    /// Registered workflow kind.
    pub kind: OperationKind,
    /// Current lifecycle state.
    pub status: WorkflowStatus,
    /// Current logical step, when known.
    pub current_step: Option<String>,
    /// Latest persisted revision for this workflow.
    pub revision: u64,
    /// Unix milliseconds of the latest update.
    pub updated_at_ms: i64,
}

/// Full frontend-safe workflow representation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDetail {
    /// Compact workflow fields.
    pub summary: WorkflowSummary,
    /// Version of the persisted workflow configuration/checkpoint schema.
    pub schema_version: u32,
    /// Non-secret parameter values supplied when the workflow was started.
    pub parameters: BTreeMap<String, Value>,
    /// Human-readable reason while waiting.
    pub wait_reason: Option<String>,
    /// Parent workflow, when this instance was started by another workflow.
    pub parent_id: Option<WorkflowId>,
    /// Resources exclusively claimed by this workflow.
    pub claims: Vec<EntityRef>,
    /// Unix milliseconds when the workflow was created.
    pub created_at_ms: i64,
    /// Unix milliseconds when the workflow finished, if terminal.
    pub finished_at_ms: Option<i64>,
    /// Frontend-safe terminal error summary.
    pub error: Option<String>,
}

/// Filter for a workflow list request.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowListRequest {
    /// Optional lifecycle state filter.
    pub status: Option<WorkflowStatus>,
}

/// Workflow list response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowListResponse {
    /// Matching workflows.
    pub workflows: Vec<WorkflowSummary>,
}

/// Workflow detail request.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDetailRequest {
    /// Requested workflow identifier.
    pub workflow_id: WorkflowId,
}

/// Request to start a registered workflow.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StartWorkflowRequest {
    /// Registered workflow kind.
    pub kind: OperationKind,
    /// Typed parameter values keyed by descriptor name.
    pub parameters: BTreeMap<String, Value>,
}

/// Request to execute a finite report or action.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunOperationRequest {
    /// Typed parameter values keyed by descriptor name.
    pub parameters: BTreeMap<String, Value>,
}

/// Frontend-renderable result from a finite report or action.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunOperationResponse {
    /// Typed operation result.
    pub result: Value,
    /// Persisted application-level execution record.
    pub execution: FiniteExecution,
}

/// Terminal status of a finite report or action execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FiniteExecutionStatus {
    /// Execution is still in progress.
    Running,
    /// Execution completed useful work.
    Succeeded,
    /// Execution completed but found no work to perform.
    Skipped,
    /// Execution failed.
    Failed,
}

/// Success/skipped/failure counts derived from structured result events.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResultSummary {
    /// Successful or planned result items.
    pub succeeded: usize,
    /// Items intentionally skipped.
    pub skipped: usize,
    /// Failed result items.
    pub failed: usize,
}

/// Persisted application-level result of a finite report or action.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FiniteExecution {
    /// Stable execution identifier.
    pub id: String,
    /// Report or action class.
    pub operation_class: OperationClass,
    /// Registered operation kind.
    pub kind: OperationKind,
    /// Terminal status.
    pub status: FiniteExecutionStatus,
    /// Structured outcome counts.
    pub summary: ResultSummary,
    /// Start time in Unix milliseconds.
    pub started_at_ms: i64,
    /// Finish time in Unix milliseconds.
    pub finished_at_ms: i64,
    /// Sanitized structured result, when successful.
    pub result: Option<Value>,
    /// Sanitized error summary, when failed.
    pub error: Option<String>,
    /// Affected entities, workflows, and managed operations discovered in the result.
    pub links: Vec<EntityRef>,
}

/// Finite application execution history response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FiniteExecutionHistoryResponse {
    /// Executions newest first.
    pub executions: Vec<FiniteExecution>,
}

/// Response after creating a workflow instance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StartWorkflowResponse {
    /// Newly created workflow.
    pub workflow: WorkflowSummary,
}

/// Request targeting an existing workflow for pause, resume, or cancellation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowControlRequest {
    /// Target workflow identifier.
    pub workflow_id: WorkflowId,
}

/// Response after a workflow control request is accepted.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowControlResponse {
    /// Updated workflow state.
    pub workflow: WorkflowSummary,
}

/// Global automation safety command.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutomationControlAction {
    /// Permit automatic trigger launches.
    EnableTriggers,
    /// Suppress new automatic trigger launches.
    DisableTriggers,
    /// Pause all eligible workflows.
    PauseAll,
    /// Resume all paused workflows.
    ResumeAll,
    /// Cancel selected workflows, or all eligible workflows when no IDs are supplied.
    Cancel,
}

/// Request to change global automation safety state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AutomationControlRequest {
    /// Requested safety operation.
    pub action: AutomationControlAction,
    /// Selected workflows for cancellation; empty means all eligible workflows.
    #[serde(default)]
    pub workflow_ids: Vec<WorkflowId>,
    /// Explicit destructive-operation confirmation.
    #[serde(default)]
    pub confirmed: bool,
}

/// Result of a global automation safety command.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AutomationControlResponse {
    /// Updated global safety state.
    pub automation: AutomationStatus,
    /// Number of workflows changed by the command.
    pub affected_workflows: usize,
}

/// Severity of one workflow activity record.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivityLevel {
    /// Diagnostic detail.
    Debug,
    /// Normal progress information.
    Info,
    /// Recoverable problem or delay.
    Warning,
    /// Failed step or operation.
    Error,
}

/// Frontend-safe workflow activity record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowActivity {
    /// Durable activity sequence identifier.
    pub id: u64,
    /// Owning workflow.
    pub workflow_id: WorkflowId,
    /// Unix milliseconds when the activity occurred.
    pub occurred_at_ms: i64,
    /// Activity severity.
    pub level: ActivityLevel,
    /// Logical step, when applicable.
    pub step: Option<String>,
    /// Human-readable, non-secret message.
    pub message: String,
}

/// Durable workflow activity response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowActivityResponse {
    /// Activity records in durable emission order.
    pub activity: Vec<WorkflowActivity>,
}

/// Semantic parameter kind used to select an appropriate frontend control.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ParameterKind {
    /// Free-form text.
    String,
    /// Whole number.
    Integer,
    /// Floating-point number.
    Number,
    /// Boolean toggle.
    Boolean,
    /// One value from the descriptor's options.
    Enum,
    /// Star-system selector.
    System,
    /// In-system location selector.
    Location,
    /// Replicant selector.
    Replicant,
    /// Device selector.
    Device,
    /// Device-type selector.
    DeviceType,
    /// Tag value.
    Tag,
    /// Generic normalized entity selector.
    Entity {
        /// Required entity category.
        entity_kind: EntityKind,
    },
}

/// One selectable enum value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ParameterOption {
    /// Serialized parameter value.
    pub value: String,
    /// Human-readable label.
    pub label: String,
}

/// Basic frontend validation hints; the daemon remains authoritative.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ParameterValidation {
    /// Inclusive numeric minimum.
    pub minimum: Option<f64>,
    /// Inclusive numeric maximum.
    pub maximum: Option<f64>,
    /// Minimum string length.
    pub min_length: Option<u32>,
    /// Maximum string length.
    pub max_length: Option<u32>,
}

/// Descriptor for one report, action, or workflow parameter.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ParameterDescriptor {
    /// Stable parameter name.
    pub name: String,
    /// Human-readable label.
    pub label: String,
    /// Help text.
    pub description: String,
    /// Semantic value kind.
    pub kind: ParameterKind,
    /// Whether a value must be supplied.
    pub required: bool,
    /// Frontend-safe default value.
    pub default: Option<Value>,
    /// Allowed values for enum parameters.
    pub options: Vec<ParameterOption>,
    /// Optional validation hints.
    pub validation: ParameterValidation,
}

/// Mutation risk presented to users before running an action or workflow.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationRisk {
    /// Read-only or no gameplay mutation.
    None,
    /// Routine reversible or low-impact mutation.
    Low,
    /// Material mutation requiring clear user intent.
    Elevated,
}

/// Lifecycle class for a user-invokable operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationClass {
    /// Read-only, finite query.
    Report,
    /// Finite mutation.
    Action,
    /// Durable persisted state machine.
    Workflow,
}

/// Supported automation trigger. Upstream game events are delivered through SSE.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerKind {
    /// Started directly by a user or local client.
    Manual,
    /// Started by a persisted schedule.
    Schedule,
    /// Started by a normalized managed game event.
    GameEvent,
    /// Started when managed state satisfies a condition.
    StateCondition,
    /// Started by another workflow.
    ParentWorkflow,
}

/// Durable automation condition. Game events come from the managed SSE journal.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TriggerCondition {
    /// Explicit local-client invocation.
    Manual,
    /// Repeating fixed interval.
    Schedule {
        /// Interval between firings in whole seconds.
        interval_seconds: u64,
    },
    /// Exact normalized managed game event.
    GameEvent {
        /// Open dotted event name.
        event_name: String,
        /// Optional device-code filter.
        device_code: Option<String>,
    },
    /// Fires once when managed projections reach a revision.
    StateCondition {
        /// Minimum managed projection revision.
        minimum_revision: u64,
    },
    /// Fires for matching terminal parent workflows.
    ParentWorkflow {
        /// Optional exact registered parent workflow kind.
        parent_kind: Option<OperationKind>,
        /// Required terminal parent status.
        status: WorkflowStatus,
    },
}

/// Registered action or workflow launched by a trigger.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TriggerTarget {
    /// Action or workflow. Reports cannot mutate automatically.
    pub operation_class: OperationClass,
    /// Stable registered operation kind.
    pub kind: OperationKind,
    /// Descriptor parameters.
    #[serde(default)]
    pub parameters: BTreeMap<String, Value>,
}

/// Persisted trigger definition and visible evaluation status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AutomationTrigger {
    /// Stable trigger identifier.
    pub id: TriggerId,
    /// Human-readable name.
    pub name: String,
    /// Durable condition.
    pub condition: TriggerCondition,
    /// Registered launch target.
    pub target: TriggerTarget,
    /// Explicit firing permission.
    pub enabled: bool,
    /// Creation time in Unix milliseconds.
    pub created_at_ms: i64,
    /// Last update time in Unix milliseconds.
    pub updated_at_ms: i64,
    /// Most recent claimed firing time.
    pub last_fired_at_ms: Option<i64>,
    /// Next schedule time.
    pub next_run_at_ms: Option<i64>,
    /// Most recent sanitized evaluation or launch error.
    pub last_error: Option<String>,
    /// Optimistic concurrency revision.
    pub revision: u64,
}

/// Request to create a disabled or explicitly enabled trigger.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CreateTriggerRequest {
    /// Human-readable name.
    pub name: String,
    /// Durable condition.
    pub condition: TriggerCondition,
    /// Registered launch target.
    pub target: TriggerTarget,
    /// Explicit firing permission.
    pub enabled: bool,
}

/// Full trigger replacement request.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UpdateTriggerRequest {
    /// Revision returned by the most recent read.
    pub expected_revision: u64,
    /// Human-readable name.
    pub name: String,
    /// Durable condition.
    pub condition: TriggerCondition,
    /// Registered launch target.
    pub target: TriggerTarget,
    /// Explicit firing permission.
    pub enabled: bool,
}

/// Trigger collection returned to local frontends.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TriggerListResponse {
    /// Persisted definitions in creation order.
    pub triggers: Vec<AutomationTrigger>,
}

/// Descriptor for a read-only report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReportDescriptor {
    /// Stable report kind.
    pub kind: OperationKind,
    /// Human-readable name.
    pub display_name: String,
    /// Alternative discoverable names, including former CLI/example names.
    #[serde(default)]
    pub aliases: Vec<String>,
    /// Human-readable description.
    pub description: String,
    /// Navigation category.
    pub category: String,
    /// Operation lifecycle class.
    pub operation_class: OperationClass,
    /// Mutation risk; reports are always [`MutationRisk::None`].
    pub risk: MutationRisk,
    /// Entity contexts from which this report is useful.
    #[serde(default)]
    pub applicable_to: Vec<EntityKind>,
    /// Accepted parameters.
    pub parameters: Vec<ParameterDescriptor>,
}

/// Descriptor for a finite mutation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ActionDescriptor {
    /// Stable action kind.
    pub kind: OperationKind,
    /// Human-readable name.
    pub display_name: String,
    /// Alternative discoverable names, including former CLI/example names.
    #[serde(default)]
    pub aliases: Vec<String>,
    /// Human-readable description.
    pub description: String,
    /// Navigation category.
    pub category: String,
    /// Operation lifecycle class.
    pub operation_class: OperationClass,
    /// Mutation risk.
    pub risk: MutationRisk,
    /// Entity contexts from which this action is useful.
    #[serde(default)]
    pub applicable_to: Vec<EntityKind>,
    /// Accepted parameters.
    pub parameters: Vec<ParameterDescriptor>,
}

/// Descriptor for a durable workflow.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDescriptor {
    /// Stable workflow kind.
    pub kind: OperationKind,
    /// Human-readable name.
    pub display_name: String,
    /// Alternative discoverable names, including former CLI names.
    #[serde(default)]
    pub aliases: Vec<String>,
    /// Human-readable description.
    pub description: String,
    /// Navigation category.
    pub category: String,
    /// Operation lifecycle class.
    pub operation_class: OperationClass,
    /// Mutation risk.
    pub risk: MutationRisk,
    /// Entity contexts from which this workflow is useful.
    #[serde(default)]
    pub applicable_to: Vec<EntityKind>,
    /// Accepted parameters.
    pub parameters: Vec<ParameterDescriptor>,
    /// Trigger kinds supported by this workflow.
    pub supported_triggers: Vec<TriggerKind>,
}

/// Registry response used to render report, action, and workflow forms.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DescriptorCatalog {
    /// Registered reports.
    pub reports: Vec<ReportDescriptor>,
    /// Registered actions.
    pub actions: Vec<ActionDescriptor>,
    /// Registered workflows.
    pub workflows: Vec<WorkflowDescriptor>,
}

/// Application slice that a frontend should reload after invalidation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DomainSlice {
    /// Cross-cutting entity summaries.
    Entities,
    /// Universe and galaxy data.
    Universe,
    /// Operations overview.
    Overview,
    /// Devices and their state.
    Devices,
    /// Inventory and cargo.
    Inventory,
    /// Autofactory state.
    Autofactories,
    /// Cargo and carriers.
    Cargo,
    /// Survey, mining, relay, and bootstrap missions.
    Missions,
    /// Finite report and action execution history.
    History,
    /// Discovered location events.
    Events,
    /// Durable account event journal and AMI digests.
    Activity,
    /// Trade controllers, orders, and trades.
    Trade,
    /// Simulation interfaces, runs, and scenarios.
    Simulations,
    /// Unlocked manufacturing blueprints.
    Blueprints,
    /// Public replicant directory.
    Directory,
    /// Tutorial progress.
    Tutorials,
    /// Account notification inbox.
    Messages,
    /// BobNet channel discovery and relay history.
    Bobnet,
    /// Relay and account network state.
    Network,
    /// Achievement and reputation state.
    Standing,
    /// Leaderboard state.
    Leaderboards,
    /// Workflow state.
    Workflows,
    /// Managed operation state.
    Operations,
}

/// Managed operation lifecycle presented by the application.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    /// Accepted but not submitted.
    Pending,
    /// Submitted and awaiting a durable outcome.
    Running,
    /// Completed successfully.
    Succeeded,
    /// Completed with an error.
    Failed,
    /// Outcome is not yet known and requires reconciliation.
    Ambiguous,
}

/// Frontend-safe managed operation status.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OperationUpdate {
    /// Stable operation identifier.
    pub id: EntityId,
    /// Initiating workflow, when applicable.
    pub workflow_id: Option<WorkflowId>,
    /// Operation lifecycle state.
    pub status: OperationStatus,
    /// Human-readable, non-secret summary.
    pub message: Option<String>,
    /// Unix milliseconds of the latest update.
    pub updated_at_ms: i64,
}

/// User-facing notification severity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NotificationLevel {
    /// Informational notification.
    Info,
    /// Notification requiring attention.
    Warning,
    /// Error notification.
    Error,
}

/// User-facing application notification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Notification {
    /// Stable notification identifier.
    pub id: EntityId,
    /// Notification severity.
    pub level: NotificationLevel,
    /// Short title.
    pub title: String,
    /// Human-readable, non-secret detail.
    pub message: String,
    /// Unix milliseconds when raised.
    pub created_at_ms: i64,
}

/// One normalized local live update sent over WebSocket.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum LiveDelta {
    /// A fresh snapshot is available at the supplied revision.
    Snapshot(SnapshotMetadata),
    /// A normalized entity was inserted or replaced.
    EntityUpsert {
        /// Entity address.
        entity: EntityRef,
        /// Normalized frontend summary.
        value: EntitySummary,
    },
    /// A normalized entity was removed.
    EntityRemove {
        /// Removed entity address.
        entity: EntityRef,
    },
    /// A domain slice should be queried again.
    DomainInvalidated {
        /// Invalidated application slice.
        slice: DomainSlice,
    },
    /// One or more domain slices should be queried again.
    ///
    /// Coalesces a tick's worth of invalidations into a single message and
    /// carries the revision each slice reached, so a client that missed
    /// messages recovers by comparing revisions instead of reconnecting. A
    /// single managed-state change previously produced fourteen separate
    /// `DomainInvalidated` messages.
    DomainsInvalidated {
        /// Revision reached per invalidated slice.
        slices: BTreeMap<DomainSlice, u64>,
    },
    /// A workflow was created.
    WorkflowCreated(WorkflowSummary),
    /// A workflow changed.
    WorkflowUpdated(WorkflowSummary),
    /// Workflow activity was appended.
    WorkflowActivity(WorkflowActivity),
    /// A managed operation changed.
    OperationUpdated(OperationUpdate),
    /// A notification was raised.
    Notification(Notification),
    /// Global automation safety state changed.
    AutomationChanged(AutomationStatus),
    /// Daemon health or synchronization changed.
    DaemonStatusChanged {
        /// Current daemon health.
        health: DaemonHealth,
        /// Current managed-client synchronization.
        sync: RuntimeSyncStatus,
    },
}

/// Versioned WebSocket message with the resulting application revision.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LiveMessage {
    /// Wire protocol version used to encode this message.
    pub protocol_version: u16,
    /// Application revision after applying this delta.
    pub revision: u64,
    /// Normalized local update.
    pub delta: LiveDelta,
}

impl LiveMessage {
    /// Creates a live message using the current protocol version.
    pub fn current(revision: u64, delta: LiveDelta) -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            revision,
            delta,
        }
    }
}

/// Frontend-safe error response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Stable machine-readable error code.
    pub code: String,
    /// Human-readable, non-secret message.
    pub message: String,
}

/// How the upstream Replicant Space API token is currently configured.
///
/// The token value itself is never included in any frontend-safe DTO.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApiTokenSource {
    /// Resolved from an environment variable.
    Environment,
    /// Resolved from a secret file path.
    SecretFile,
    /// Not currently resolvable.
    Unset,
}

/// Frontend-safe, non-secret application and runtime settings projection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SettingsSnapshot {
    /// Snapshot identity and creation time.
    pub metadata: SnapshotMetadata,
    /// Active application profile name.
    pub profile: String,
    /// Local HTTP address the daemon listens on.
    pub bind_address: String,
    /// Managed SDK SQLite database location.
    pub managed_database_path: String,
    /// Workflow/runtime SQLite database location.
    pub runtime_database_path: String,
    /// Effective `tracing` log filter directive.
    pub log_filter: String,
    /// Whether the daemon process is running inside a Docker container.
    pub docker: bool,
    /// How the upstream API token is configured, never its value.
    pub api_token_source: ApiTokenSource,
    /// Whether changing the daemon-level settings above requires a restart.
    pub daemon_settings_require_restart: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T>(value: &T)
    where
        T: std::fmt::Debug + PartialEq + Serialize + for<'de> Deserialize<'de>,
    {
        let json = serde_json::to_string(value).expect("serialize protocol DTO");
        let decoded = serde_json::from_str(&json).expect("deserialize protocol DTO");
        assert_eq!(value, &decoded);
    }

    fn workflow() -> WorkflowSummary {
        WorkflowSummary {
            id: WorkflowId("0198f4f0-example".to_owned()),
            kind: OperationKind("survey.route".to_owned()),
            status: WorkflowStatus::Waiting,
            current_step: Some("surveying".to_owned()),
            revision: 7,
            updated_at_ms: 1_765_000_000_000,
        }
    }

    fn device() -> DeviceSummary {
        DeviceSummary {
            entity: EntityRef {
                kind: EntityKind::Device,
                id: EntityId("D-1".to_owned()),
            },
            device_type: Some("future_device".to_owned()),
            status: None,
            ownership: "owned".to_owned(),
            owner: None,
            owner_name: None,
            system: None,
            location: None,
            tags: Vec::new(),
            attached_to: None,
            stowed_in: None,
            controller: None,
            linked_device: None,
            attached_devices: Vec::new(),
            controlled_devices: Vec::new(),
            stowed_devices: Vec::new(),
            attach_capacity: None,
            cargo_capacity: None,
            cargo_used: None,
            operational_capacity_percent: None,
            active_directive: None,
            directive_status: None,
            travel_destination: None,
            claim: None,
        }
    }

    #[test]
    fn request_response_and_descriptor_round_trip() {
        round_trip(&Versioned::current(StartWorkflowRequest {
            kind: OperationKind("survey.route".to_owned()),
            parameters: BTreeMap::from([("system".to_owned(), Value::String("SOL".to_owned()))]),
        }));
        round_trip(&Versioned::current(WorkflowListResponse {
            workflows: vec![workflow()],
        }));
        round_trip(&Versioned::current(WorkflowDescriptor {
            kind: OperationKind("survey.route".to_owned()),
            display_name: "Survey route".to_owned(),
            aliases: vec!["survey".to_owned()],
            description: "Survey a sequence of systems".to_owned(),
            category: "survey".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Low,
            applicable_to: vec![EntityKind::System],
            parameters: vec![ParameterDescriptor {
                name: "system".to_owned(),
                label: "System".to_owned(),
                description: "Starting system".to_owned(),
                kind: ParameterKind::System,
                required: true,
                default: None,
                options: Vec::new(),
                validation: ParameterValidation::default(),
            }],
            supported_triggers: vec![TriggerKind::Manual, TriggerKind::GameEvent],
        }));
    }

    #[test]
    fn live_delta_round_trips_without_upstream_event_shape() {
        let message = LiveMessage::current(42, LiveDelta::WorkflowUpdated(workflow()));
        round_trip(&message);

        round_trip(&Versioned::current(EntityIndexSnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1_765_000_000_000,
            },
            entities: vec![EntitySummary {
                entity: EntityRef {
                    kind: EntityKind::Replicant,
                    id: EntityId("R-1".to_owned()),
                },
                label: "R-1".to_owned(),
                secondary_label: Some("Ada".to_owned()),
                system: Some("SOL".to_owned()),
                location: Some("EARTH".to_owned()),
                entity_type: None,
                status: Some("idle".to_owned()),
            }],
        }));
        round_trip(&Versioned::current(DevicesSnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1_765_000_000_000,
            },
            devices: vec![device()],
        }));
        round_trip(&Versioned::current(SurveySnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1,
            },
            missions: vec![SurveyMissionSummary {
                workflow: workflow(),
                replicant: "R-1".to_owned(),
                vessel: "V-1".to_owned(),
                center: "SOL".to_owned(),
                phase: "surveying".to_owned(),
                completed_systems: 2,
                total_systems: 4,
                next_system: Some("VEGA".to_owned()),
                controller: Some("SC-1".to_owned()),
                drones: vec!["SD-1".to_owned()],
            }],
            fleet: vec![device()],
        }));
        round_trip(&Versioned::current(MiningSnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1,
            },
            installations: vec![MiningInstallationSummary {
                id: "SOL/SOL-BELT".to_owned(),
                system: Some("SOL".to_owned()),
                location: Some("SOL-BELT".to_owned()),
                controller: None,
                miners: Vec::new(),
                survey_controller: None,
                survey_drones: Vec::new(),
                maintenance_device: None,
                missing: vec!["mining controller".to_owned()],
                status: MiningInstallationStatus::Partial,
            }],
            workflows: Vec::new(),
        }));
        round_trip(&Versioned::current(RelaySnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1,
            },
            relays: vec![device()],
            staged_relays: Vec::new(),
            connected_systems: 2,
            relay_edges: vec![GalaxyEdge {
                from: "SOL".to_owned(),
                to: "VEGA".to_owned(),
            }],
            expansions: vec![RelayExpansionSummary {
                workflow: workflow(),
                replicant: "R-1".to_owned(),
                hub: "SOL-1".to_owned(),
                targets: vec!["VEGA".to_owned()],
                phase: "deploying".to_owned(),
                completed_stops: 1,
                total_stops: Some(2),
                next_system: Some("VEGA".to_owned()),
                pending_relays: Some(0),
            }],
        }));
        round_trip(&Versioned::current(BootstrapSnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1,
            },
            missions: vec![BootstrapMissionSummary {
                mission_id: "BOOT-1".to_owned(),
                execution_id: "EXEC-1".to_owned(),
                region: "beta".to_owned(),
                source_hub: "SOL-1".to_owned(),
                target_system: "VEGA".to_owned(),
                target_location: "VEGA-ENTRY".to_owned(),
                phase: "staged_at_source".to_owned(),
                reserved_devices: 10,
                loaded_devices: 8,
                capital_system: None,
                selected_sites: 0,
                warnings: Vec::new(),
                completed: false,
                updated_at_ms: 10,
            }],
        }));
        round_trip(&Versioned::current(EventsSnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1,
            },
            events: vec![EventSummary {
                designation: "EVT-1".to_owned(),
                title: "First contact".to_owned(),
                event_type: Some("unknown_future_type".to_owned()),
                category: Some("unknown_future_category".to_owned()),
                tier: Some(2),
                system: "SOL".to_owned(),
                location: "SOL-1".to_owned(),
                description: None,
                criteria: vec![EventCriterionSummary {
                    name: "supply".to_owned(),
                    requirements: vec![EventRequirementSummary {
                        kind: EventRequirementKind::Resource,
                        item: "iron".to_owned(),
                        required: 10,
                        completed: 4,
                        remaining: 6,
                    }],
                    complete: false,
                }],
                rewards: EventRewardsSummary {
                    resources: vec![EventRewardItem {
                        item: "water".to_owned(),
                        quantity: 2,
                    }],
                    ..EventRewardsSummary::default()
                },
                status: Some("active".to_owned()),
                discovered_at: Some("2026-01-01T00:00:00Z".to_owned()),
                completed_at: None,
            }],
        }));
        round_trip(&Versioned::current(TradeSnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1,
            },
            viewer: Some(EntityRef {
                kind: EntityKind::Replicant,
                id: EntityId("R-1".to_owned()),
            }),
            controllers: vec![TradeControllerSummary {
                entity: EntityRef {
                    kind: EntityKind::Device,
                    id: EntityId("TC-1".to_owned()),
                },
                shop_name: Some("Exchange".to_owned()),
                description: None,
                is_local: true,
                owner_name: None,
                owner_replicant: None,
                system: Some("SOL".to_owned()),
                location: Some("SOL-1".to_owned()),
                total_stock: Some(1),
                trade_count: Some(1),
                trades: vec![TradeSummary {
                    trade_code: "TRD-1".to_owned(),
                    name: None,
                    current_stock: Some(1),
                    initial_stock: None,
                    requested: Vec::new(),
                    offered: vec![TradeItemSummary {
                        kind: "resource".to_owned(),
                        item: "iron".to_owned(),
                        quantity: Some(2.0),
                    }],
                    created_at: None,
                }],
                workflow: None,
            }],
        }));
        let intelligence_metadata = SnapshotMetadata {
            revision: 42,
            generated_at_ms: 1,
        };
        round_trip(&Versioned::current(ReportsSnapshot {
            metadata: intelligence_metadata.clone(),
            reports: Vec::new(),
            executions: Vec::new(),
        }));
        round_trip(&Versioned::current(MessagesSnapshot {
            metadata: intelligence_metadata.clone(),
            inbox: Vec::new(),
            unread_count: None,
        }));
        round_trip(&Versioned::current(BobnetSnapshot {
            metadata: intelligence_metadata.clone(),
            sources: Vec::new(),
            selected_source: None,
            channels: Vec::new(),
            messages: Vec::new(),
            replicants: Vec::new(),
            next_cursor: None,
            total_messages: None,
            error: None,
        }));
        round_trip(&Versioned::current(NetworkSnapshot {
            metadata: intelligence_metadata.clone(),
            account_name: None,
            account_status: None,
            subscribed_channels: Vec::new(),
            replicants: Vec::new(),
            relays: Vec::new(),
        }));
        round_trip(&Versioned::current(StandingSnapshot {
            metadata: intelligence_metadata.clone(),
            experience_points_total: Some(10),
            civilisation_points: None,
            achievements: Vec::new(),
            reputation: Vec::new(),
        }));
        round_trip(&Versioned::current(LeaderboardsSnapshot {
            metadata: intelligence_metadata,
            boards: Vec::new(),
            selected_board: None,
            entries: Vec::new(),
        }));
        round_trip(&Versioned::current(SettingsSnapshot {
            metadata: SnapshotMetadata {
                revision: 3,
                generated_at_ms: 1,
            },
            profile: "default".to_owned(),
            bind_address: "127.0.0.1:8080".to_owned(),
            managed_database_path: "replicant-client.sqlite".to_owned(),
            runtime_database_path: "replicant-runtime.sqlite".to_owned(),
            log_filter: "info".to_owned(),
            docker: false,
            api_token_source: ApiTokenSource::Environment,
            daemon_settings_require_restart: true,
        }));
        round_trip(&Versioned::current(AutofactorySnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1,
            },
            utilization: AutofactoryUtilization {
                total: 1,
                busy: 1,
                available: 0,
                unavailable: 0,
                queued_units: 2,
                utilization_percent: 100.0,
            },
            factories: vec![AutofactorySummary {
                device: device(),
                availability: AutofactoryAvailability::Busy,
                queue_capacity: Some(4),
                queued_units: 2,
                current_job: Some(FactoryJobSummary {
                    device_type: "relay".to_owned(),
                    quantity: 1,
                    eta_seconds: Some(60.0),
                    tags: Vec::new(),
                }),
                queued_jobs: Vec::new(),
            }],
        }));
        round_trip(&Versioned::current(CargoSnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1,
            },
            cargo_used: 3,
            cargo_capacity: 10,
            attachment_used: 1,
            attachment_capacity: 2,
            carriers: vec![CargoCarrierSummary {
                device: device(),
                resources: vec![CargoResourceSummary {
                    resource: "silicates".to_owned(),
                    quantity: 3,
                }],
                attachment_used: 1,
            }],
        }));
        round_trip(&Versioned::current(InventorySnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1_765_000_000_000,
            },
            total_quantity: 12,
            locations: vec![InventoryLocationSummary {
                owner_kind: InventoryOwnerKind::Location,
                owner: "EARTH".to_owned(),
                system: Some("SOL".to_owned()),
                location: Some("EARTH".to_owned()),
                total_quantity: 12,
                resources: vec![InventoryQuantity {
                    resource: "silicates".to_owned(),
                    quantity: 12,
                }],
            }],
            resources: vec![InventoryResourceSummary {
                resource: "silicates".to_owned(),
                total_quantity: 12,
                distribution: vec![InventoryDistribution {
                    owner_kind: InventoryOwnerKind::Location,
                    owner: "EARTH".to_owned(),
                    system: Some("SOL".to_owned()),
                    location: Some("EARTH".to_owned()),
                    quantity: 12,
                }],
            }],
        }));
        round_trip(&Versioned::current(OverviewSnapshot {
            metadata: SnapshotMetadata {
                revision: 42,
                generated_at_ms: 1_765_000_000_000,
            },
            health: DaemonHealth {
                status: HealthStatus::Healthy,
                daemon_version: "0.1.0".to_owned(),
                detail: None,
            },
            sync: RuntimeSyncStatus {
                phase: SyncPhase::Ready,
                revision: 42,
                last_event_at_ms: None,
                detail: None,
            },
            automation: AutomationStatus {
                automatic_triggers_enabled: true,
                workflows_paused: false,
            },
            replicants: Vec::new(),
            active_travel: Vec::new(),
            active_workflows: vec![workflow()],
            workflow_counts: vec![WorkflowStatusCount {
                status: WorkflowStatus::Waiting,
                count: 1,
            }],
            attention_workflows: Vec::new(),
            notifications: Vec::new(),
            recent_activity: Vec::new(),
        }));

        let json = serde_json::to_value(message).expect("serialize live message");
        assert_eq!(json["protocol_version"], PROTOCOL_VERSION);
        assert_eq!(json["delta"]["type"], "workflow_updated");
        assert!(json.get("upstream_event").is_none());
    }

    #[test]
    fn protocol_version_is_stable_and_present_on_wire_messages() {
        assert_eq!(PROTOCOL_VERSION, 1);
        assert_eq!(Versioned::current(()).protocol_version, 1);
        assert_eq!(
            LiveMessage::current(
                0,
                LiveDelta::DomainInvalidated {
                    slice: DomainSlice::Universe,
                },
            )
            .protocol_version,
            1
        );
    }
}
