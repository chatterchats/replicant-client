use std::{collections::BTreeMap, fmt, str::FromStr};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::RepositoryError;

/// Persisted global automation safety policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AutomationPolicy {
    /// Whether non-manual triggers may launch new work.
    pub automatic_triggers_enabled: bool,
    /// Whether workflow executors are globally paused.
    pub workflows_paused: bool,
}

impl Default for AutomationPolicy {
    fn default() -> Self {
        Self {
            automatic_triggers_enabled: true,
            workflows_paused: false,
        }
    }
}

/// Stable identifier for a persisted automation trigger.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TriggerId(Uuid);

impl TriggerId {
    /// Creates a unique trigger identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TriggerId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TriggerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for TriggerId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Registered operation class launched by a trigger.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TriggerTargetClass {
    /// Finite mutating action.
    Action,
    /// Durable workflow.
    Workflow,
}

/// Registered action or workflow invocation persisted with a trigger.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TriggerTarget {
    /// Operation lifecycle class.
    pub operation_class: TriggerTargetClass,
    /// Stable registered operation kind.
    pub kind: String,
    /// Typed descriptor parameters at the JSON boundary.
    #[serde(default)]
    pub parameters: BTreeMap<String, serde_json::Value>,
}

/// Durable condition that can launch a registered action or workflow.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TriggerCondition {
    /// Explicit local-client invocation.
    Manual,
    /// Repeating fixed interval schedule.
    Schedule {
        /// Interval between firings in milliseconds.
        interval_millis: i64,
    },
    /// Exact managed game event delivered through the SSE-backed journal.
    GameEvent {
        /// Open dotted managed event name.
        event_name: String,
        /// Optional device-code filter.
        device_code: Option<String>,
    },
    /// Fires once when the durable managed projection reaches a revision.
    StateCondition {
        /// Minimum managed projection revision.
        minimum_revision: u64,
    },
    /// Fires once for each matching terminal parent workflow.
    ParentWorkflow {
        /// Optional exact registered parent workflow kind.
        parent_kind: Option<WorkflowKind>,
        /// Required terminal parent status.
        status: WorkflowStatus,
    },
}

/// Input used to create a persisted automation trigger.
pub struct NewTrigger {
    /// Human-readable trigger name.
    pub name: String,
    /// Durable launch condition.
    pub condition: TriggerCondition,
    /// Registered action or workflow invocation.
    pub target: TriggerTarget,
    /// Whether automatic or manual firing is permitted.
    pub enabled: bool,
    /// First due time for a schedule, in Unix milliseconds.
    pub next_run_at: Option<i64>,
    /// Initial managed event cursor for event triggers.
    pub event_cursor: Option<String>,
}

/// Complete editable trigger definition.
pub struct TriggerState {
    /// Human-readable trigger name.
    pub name: String,
    /// Durable launch condition.
    pub condition: TriggerCondition,
    /// Registered action or workflow invocation.
    pub target: TriggerTarget,
    /// Whether firing is permitted.
    pub enabled: bool,
    /// Next due schedule time, in Unix milliseconds.
    pub next_run_at: Option<i64>,
    /// Durable managed event cursor.
    pub event_cursor: Option<String>,
}

/// Persisted automation trigger and its visible status.
#[derive(Clone, Debug, PartialEq)]
pub struct AutomationTrigger {
    /// Stable trigger identifier.
    pub id: TriggerId,
    /// Human-readable trigger name.
    pub name: String,
    /// Durable launch condition.
    pub condition: TriggerCondition,
    /// Registered action or workflow invocation.
    pub target: TriggerTarget,
    /// Whether firing is permitted.
    pub enabled: bool,
    /// Creation time in Unix milliseconds.
    pub created_at: i64,
    /// Last update time in Unix milliseconds.
    pub updated_at: i64,
    /// Most recent claimed firing time.
    pub last_fired_at: Option<i64>,
    /// Next due schedule time.
    pub next_run_at: Option<i64>,
    /// Most recent launch or evaluation error.
    pub last_error: Option<String>,
    /// Last managed event cursor consumed by this trigger.
    pub event_cursor: Option<String>,
    /// Optimistic concurrency revision.
    pub revision: u64,
}

/// Application-level class for one persisted finite execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FiniteExecutionClass {
    /// Read-only report.
    Report,
    /// Bounded mutating action.
    Action,
}

impl FiniteExecutionClass {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Report => "report",
            Self::Action => "action",
        }
    }
}

/// Terminal application-level status for one finite execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FiniteExecutionStatus {
    /// Execution completed useful work.
    Succeeded,
    /// Execution completed but found no work to perform.
    Skipped,
    /// Execution failed.
    Failed,
}

impl FiniteExecutionStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
        }
    }
}

/// Durable, frontend-safe result of a finite report or action.
#[derive(Clone, Debug, PartialEq)]
pub struct FiniteExecution {
    /// Stable execution identifier.
    pub id: String,
    /// Report or action.
    pub operation_class: FiniteExecutionClass,
    /// Registered descriptor kind.
    pub kind: String,
    /// Terminal execution status.
    pub status: FiniteExecutionStatus,
    /// Start time in Unix milliseconds.
    pub started_at: i64,
    /// Finish time in Unix milliseconds.
    pub finished_at: i64,
    /// Sanitized structured result, when successful.
    pub result: Option<serde_json::Value>,
    /// Sanitized error summary, when failed.
    pub error: Option<String>,
}

/// Stable identifier for a persisted workflow instance.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowId(Uuid);

impl WorkflowId {
    /// Creates a unique workflow identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for WorkflowId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkflowId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for WorkflowId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Stable, machine-readable workflow kind used by the registry and database.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowKind(String);

impl WorkflowKind {
    /// Validates and creates a workflow kind.
    pub fn new(value: impl Into<String>) -> Result<Self, RepositoryError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || !value.bytes().all(|byte| {
                byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
            })
        {
            return Err(RepositoryError::InvalidKind(value));
        }
        Ok(Self(value))
    }

    /// Returns the persisted kind string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkflowKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Stable lifecycle state for a workflow instance.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowStatus {
    /// Created but not yet started.
    Queued,
    /// Actively executing a step.
    Running,
    /// Durably waiting for an external state change or time.
    Waiting,
    /// Cooperatively paused.
    Paused,
    /// Comparing persisted intent with current managed state.
    Reconciling,
    /// Completed successfully.
    Succeeded,
    /// Stopped after an unrecoverable error.
    Failed,
    /// Cooperatively cancelled.
    Cancelled,
}

/// Durable description of why a workflow is waiting.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WaitIntent {
    /// Human-readable, non-secret condition description.
    pub description: String,
    /// Optional exact managed event name used as wake-up evidence.
    pub event_name: Option<String>,
    /// Optional device code narrowing event evidence.
    pub device_code: Option<String>,
    /// Durable managed event cursor from which recovery should continue.
    pub cursor: Option<String>,
    /// Optional absolute Unix deadline in milliseconds.
    pub deadline_millis: Option<i64>,
}

impl WaitIntent {
    /// Creates a managed-state predicate wait.
    #[must_use]
    pub fn state(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            event_name: None,
            device_code: None,
            cursor: None,
            deadline_millis: None,
        }
    }

    /// Adds exact managed event evidence to a state-verified wait.
    #[must_use]
    pub fn for_event(mut self, event_name: impl Into<String>) -> Self {
        self.event_name = Some(event_name.into());
        self
    }

    /// Narrows event evidence to one device code.
    #[must_use]
    pub fn for_device(mut self, device_code: impl Into<String>) -> Self {
        self.device_code = Some(device_code.into());
        self
    }

    /// Adds an absolute Unix deadline in milliseconds.
    #[must_use]
    pub fn until(mut self, deadline_millis: i64) -> Self {
        self.deadline_millis = Some(deadline_millis);
        self
    }
}

/// Result of a cooperative workflow wait.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitOutcome {
    /// Durable managed state verifies the requested predicate.
    Satisfied,
    /// The persisted deadline elapsed first.
    Deadline,
    /// A cooperative pause was requested.
    Paused,
    /// A cooperative cancellation was requested.
    Cancelled,
}

impl WorkflowStatus {
    /// Returns whether this workflow can no longer execute.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }

    /// Returns whether a state replacement may use `next`.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        use WorkflowStatus::{
            Cancelled, Failed, Paused, Queued, Reconciling, Running, Succeeded, Waiting,
        };
        self == next
            || matches!(
                (self, next),
                (Queued, Running | Paused | Failed | Cancelled)
                    | (
                        Running,
                        Waiting | Paused | Reconciling | Succeeded | Failed | Cancelled
                    )
                    | (Waiting, Running | Paused | Reconciling | Failed | Cancelled)
                    | (Paused, Running | Reconciling | Cancelled)
                    | (
                        Reconciling,
                        Running | Waiting | Paused | Succeeded | Failed | Cancelled
                    )
            )
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Paused => "paused",
            Self::Reconciling => "reconciling",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Exclusive gameplay resource identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "identity")]
pub enum ResourceKey {
    /// Replicant code or name.
    Replicant(String),
    /// Device or vessel code.
    Device(String),
    /// Autofactory code.
    Autofactory(String),
    /// Application-defined namespace and stable identity.
    Namespaced {
        /// Stable application-defined namespace.
        namespace: String,
        /// Stable resource identity within the namespace.
        key: String,
    },
}

impl ResourceKey {
    pub(crate) fn persisted_parts(&self) -> Result<(String, &str), RepositoryError> {
        let (namespace, key) = match self {
            Self::Replicant(key) => ("replicant".to_owned(), key.as_str()),
            Self::Device(key) => ("device".to_owned(), key.as_str()),
            Self::Autofactory(key) => ("autofactory".to_owned(), key.as_str()),
            Self::Namespaced { namespace, key } => {
                if namespace.is_empty()
                    || namespace.len() > 128
                    || !namespace.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
                    })
                {
                    return Err(RepositoryError::InvalidResourceKey(self.clone()));
                }
                (format!("custom:{namespace}"), key.as_str())
            }
        };
        if key.is_empty() || key.len() > 256 {
            return Err(RepositoryError::InvalidResourceKey(self.clone()));
        }
        Ok((namespace, key))
    }
}

/// One persisted exclusive resource claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceClaim {
    /// Claimed gameplay resource.
    pub resource: ResourceKey,
    /// Workflow that owns the claim.
    pub workflow_id: WorkflowId,
    /// First acquisition time in Unix milliseconds.
    pub acquired_at: i64,
    /// Most recent idempotent acquisition time in Unix milliseconds.
    pub updated_at: i64,
}

/// Result of atomically acquiring a claim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClaimAcquireOutcome {
    /// The resource was unclaimed and is now owned by the workflow.
    Acquired(ResourceClaim),
    /// The workflow already owned the claim; its update timestamp was refreshed.
    AlreadyOwned(ResourceClaim),
}

impl FromStr for WorkflowStatus {
    type Err = RepositoryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "waiting" => Ok(Self::Waiting),
            "paused" => Ok(Self::Paused),
            "reconciling" => Ok(Self::Reconciling),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            _ => Err(RepositoryError::InvalidStoredStatus(value.to_owned())),
        }
    }
}

/// Initial data used to create a queued workflow.
pub struct NewWorkflow<C, P> {
    /// Stable workflow kind.
    pub kind: WorkflowKind,
    /// Version of the serialized config and checkpoint schema.
    pub schema_version: u32,
    /// Typed, non-secret workflow configuration.
    pub config: C,
    /// Typed initial checkpoint.
    pub checkpoint: P,
    /// First logical step, when one is known.
    pub current_step: Option<String>,
    /// Parent orchestration, if this workflow was created by another workflow.
    pub parent_id: Option<WorkflowId>,
}

/// Complete mutable state written in one atomic workflow update.
pub struct WorkflowState<P, R> {
    /// New lifecycle status.
    pub status: WorkflowStatus,
    /// Current logical step.
    pub current_step: Option<String>,
    /// Typed durable checkpoint.
    pub checkpoint: P,
    /// Last error message, without secrets.
    pub last_error: Option<String>,
    /// Typed terminal or intermediate result metadata.
    pub result: Option<R>,
}

/// One durable workflow activity message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowActivity {
    /// Monotonic database identifier.
    pub id: i64,
    /// Workflow that emitted the message.
    pub workflow_id: WorkflowId,
    /// Creation time in Unix milliseconds.
    pub created_at: i64,
    /// Human-readable, non-secret activity message.
    pub message: String,
}

/// Persisted workflow instance.
///
/// Serialized payloads are intentionally omitted from `Debug`; callers decode
/// them into their registered typed models. Configs, checkpoints, errors, and
/// results must never contain credentials or other secrets.
#[derive(Clone)]
pub struct WorkflowInstance {
    /// Stable instance identifier.
    pub id: WorkflowId,
    /// Registered workflow kind.
    pub kind: WorkflowKind,
    /// Version of the serialized config and checkpoint schema.
    pub schema_version: u32,
    /// Current lifecycle status.
    pub status: WorkflowStatus,
    /// Current logical step.
    pub current_step: Option<String>,
    /// Creation time in Unix milliseconds.
    pub created_at: i64,
    /// Last update time in Unix milliseconds.
    pub updated_at: i64,
    /// Last error message, if any.
    pub last_error: Option<String>,
    /// Parent workflow, if any.
    pub parent_id: Option<WorkflowId>,
    /// Optimistic concurrency revision.
    pub revision: u64,
    pub(crate) config_json: String,
    pub(crate) checkpoint_json: String,
    pub(crate) result_json: Option<String>,
    pub(crate) wait_intent_json: Option<String>,
}

impl WorkflowInstance {
    /// Decodes the typed workflow configuration.
    pub fn config<C: DeserializeOwned>(&self) -> Result<C, RepositoryError> {
        Ok(serde_json::from_str(&self.config_json)?)
    }

    /// Decodes the typed workflow checkpoint.
    pub fn checkpoint<P: DeserializeOwned>(&self) -> Result<P, RepositoryError> {
        Ok(serde_json::from_str(&self.checkpoint_json)?)
    }

    /// Decodes typed result metadata when present.
    pub fn result<R: DeserializeOwned>(&self) -> Result<Option<R>, RepositoryError> {
        self.result_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(Into::into)
    }

    /// Decodes the durable wait intent when present.
    pub fn wait_intent(&self) -> Result<Option<WaitIntent>, RepositoryError> {
        self.wait_intent_json
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(Into::into)
    }
}

impl fmt::Debug for WorkflowInstance {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowInstance")
            .field("id", &self.id)
            .field("kind", &self.kind)
            .field("schema_version", &self.schema_version)
            .field("status", &self.status)
            .field("current_step", &self.current_step)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("parent_id", &self.parent_id)
            .field("revision", &self.revision)
            .finish_non_exhaustive()
    }
}
