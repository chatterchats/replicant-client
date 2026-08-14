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
    /// Mutation risk.
    pub risk: MutationRisk,
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
    /// Mutation risk.
    pub risk: MutationRisk,
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
            risk: MutationRisk::Low,
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
