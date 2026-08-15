//! Stable, versioned DTOs shared by `replicantd` and its local frontends.
//!
//! This crate contains only the application's normalized local protocol. Raw
//! upstream Replicant Space events and authentication data do not belong here.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Current local application protocol version.
pub const PROTOCOL_VERSION: u16 = 1;

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
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct EntityRef {
    /// Entity category.
    pub kind: EntityKind,
    /// Stable entity identifier.
    pub id: EntityId,
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

/// Metadata describing an application snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SnapshotMetadata {
    /// Monotonically increasing application revision.
    pub revision: u64,
    /// Unix milliseconds when the snapshot was produced.
    pub generated_at_ms: i64,
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
    /// Current persisted workflows.
    pub workflows: Vec<WorkflowSummary>,
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
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DomainSlice {
    /// Universe and galaxy data.
    Universe,
    /// Devices and their state.
    Devices,
    /// Inventory and cargo.
    Inventory,
    /// Autofactory state.
    Autofactories,
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
        /// Normalized frontend representation.
        value: Value,
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
