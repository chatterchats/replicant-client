use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::{RepositoryError, work::WorkItemId};

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

/// Application-level status for one finite execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FiniteExecutionStatus {
    /// Execution is still in progress.
    ///
    /// Long actions are recorded before they start so the caller gets an id
    /// immediately and can follow progress instead of holding an HTTP request
    /// open for the whole run.
    Running,
    /// Execution completed useful work.
    Succeeded,
    /// Execution completed but found no work to perform.
    Skipped,
    /// Execution failed.
    Failed,
    /// Execution was cancelled by the operator.
    Cancelled,
}

impl FiniteExecutionStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
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
    /// Current execution status.
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
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
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
/// A structural description of a durable service capability requested or
/// established by a workflow.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct WorkflowServiceIntent {
    /// Stable service identifier (for example, an integration-owned literal).
    pub service: String,
    /// Exact dimensions that identify the service operation.
    pub dimensions: BTreeMap<String, String>,
}

impl WorkflowServiceIntent {
    /// Creates a structural service intent from a service and dimensions.
    #[must_use]
    pub fn new(service: impl Into<String>, dimensions: BTreeMap<String, String>) -> Self {
        Self {
            service: service.into(),
            dimensions,
        }
    }
}

/// Scope whose service coverage may be unknown.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum WorkflowServiceScope {
    /// Coverage is unknown for every service target.
    Global,
    /// Coverage is unknown within one canonical region.
    Region(String),
    /// Coverage is unknown within one canonical system.
    System(String),
}

impl WorkflowServiceScope {
    /// Canonicalizes a scope identity for deterministic matching.
    #[must_use]
    pub fn canonical(self) -> Self {
        match self {
            Self::Global => Self::Global,
            Self::Region(value) => Self::Region(canonical_scope_value(&value)),
            Self::System(value) => Self::System(canonical_scope_value(&value)),
        }
    }
}

/// Completeness of a workflow's service-intent projection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowServiceIntentCoverage {
    /// This workflow does not provide a service projection.
    #[default]
    NotApplicable,
    /// All service evidence supported by the workflow was decoded.
    Complete,
    /// At least one service scope could not be decoded safely.
    Unknown,
}

/// Typed service evidence projected from one workflow.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowServiceIntentProjection {
    /// Whether this workflow's service evidence is complete.
    pub coverage: WorkflowServiceIntentCoverage,
    /// Exact service intents decoded from durable state.
    pub intents: Vec<WorkflowServiceIntent>,
    /// Scopes where service coverage remains unknown.
    pub unknown_scopes: BTreeSet<WorkflowServiceScope>,
}

impl WorkflowServiceIntentProjection {
    /// Creates a projection for a workflow that does not provide a service.
    #[must_use]
    pub const fn not_applicable() -> Self {
        Self {
            coverage: WorkflowServiceIntentCoverage::NotApplicable,
            intents: Vec::new(),
            unknown_scopes: BTreeSet::new(),
        }
    }

    /// Creates a complete exact-intent projection.
    #[must_use]
    pub fn complete(intents: Vec<WorkflowServiceIntent>) -> Self {
        Self {
            coverage: WorkflowServiceIntentCoverage::Complete,
            intents,
            unknown_scopes: BTreeSet::new(),
        }
    }

    /// Creates an unknown projection scoped to the supplied identities.
    #[must_use]
    pub fn unknown(unknown_scopes: impl IntoIterator<Item = WorkflowServiceScope>) -> Self {
        Self {
            coverage: WorkflowServiceIntentCoverage::Unknown,
            intents: Vec::new(),
            unknown_scopes: unknown_scopes.into_iter().collect(),
        }
    }
}

impl Default for WorkflowServiceIntentProjection {
    fn default() -> Self {
        Self::not_applicable()
    }
}

/// Service evidence retained with its durable workflow provenance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowServiceIntentEvidence {
    /// Workflow producing this evidence.
    pub workflow_id: WorkflowId,
    /// Registered workflow kind producing this evidence.
    pub workflow_kind: WorkflowKind,
    /// Live lifecycle status when evidence was projected.
    pub workflow_status: WorkflowStatus,
    /// Exact intents decoded from the workflow.
    pub intents: Vec<WorkflowServiceIntent>,
    /// Scopes where the workflow's service evidence is unknown.
    pub unknown_scopes: BTreeSet<WorkflowServiceScope>,
}

/// Live service-intent evidence from all registered workflow instances.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowServiceIntentSnapshot {
    /// Evidence from live workflows only.
    pub live: Vec<WorkflowServiceIntentEvidence>,
}

/// Result of querying one exact service intent.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum WorkflowServiceIntentState {
    /// One or more live workflows project the exact intent.
    Present(Vec<WorkflowId>),
    /// No live workflow projects the intent or applicable unknown scope.
    Absent,
    /// Applicable live workflows have unresolved service coverage.
    Unknown(Vec<WorkflowId>),
}

impl WorkflowServiceIntentSnapshot {
    /// Returns the exact service state for a target and optional scope.
    ///
    /// Exact `Present` evidence always wins over unknown evidence. Region and
    /// system names are compared after trimming and lowercasing, matching the
    /// canonical scope identity used by runtime integrations.
    #[must_use]
    pub fn state_for(
        &self,
        target: &WorkflowServiceIntent,
        region: Option<&str>,
        system: Option<&str>,
    ) -> WorkflowServiceIntentState {
        let mut present = Vec::new();
        let mut unknown = Vec::new();
        for evidence in &self.live {
            if evidence.intents.iter().any(|intent| intent == target) {
                present.push(evidence.workflow_id);
                continue;
            }
            if evidence
                .unknown_scopes
                .iter()
                .any(|scope| scope_applies(scope, region, system))
            {
                unknown.push(evidence.workflow_id);
            }
        }
        present.sort();
        present.dedup();
        if !present.is_empty() {
            return WorkflowServiceIntentState::Present(present);
        }
        unknown.sort();
        unknown.dedup();
        if unknown.is_empty() {
            WorkflowServiceIntentState::Absent
        } else {
            WorkflowServiceIntentState::Unknown(unknown)
        }
    }
}

fn canonical_scope_value(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn scope_applies(scope: &WorkflowServiceScope, region: Option<&str>, system: Option<&str>) -> bool {
    match scope {
        WorkflowServiceScope::Global => true,
        WorkflowServiceScope::Region(value) => region
            .is_some_and(|target| canonical_scope_value(value) == canonical_scope_value(target)),
        WorkflowServiceScope::System(value) => system
            .is_some_and(|target| canonical_scope_value(value) == canonical_scope_value(target)),
    }
}

/// Subject of a workflow placement intent.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum WorkflowPlacementIntentSubject {
    /// Exact device code.
    Device(String),
    /// Exact whole-device reservation tag.
    DeviceTag(String),
}

/// Canonicalizes a device code at workflow placement snapshot boundaries.
///
/// Device codes are compared case-insensitively and tolerate surrounding
/// whitespace in upstream evidence. Tags intentionally do not use this helper:
/// they remain exact, whole-tag, case-sensitive values.
pub(crate) fn canonical_device_code(code: &str) -> String {
    code.trim().to_ascii_uppercase()
}

/// Physical relation asserted by a workflow placement intent.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowPlacementIntentRelation {
    /// The workflow has claimed the device.
    Claimed,
    /// The device has been staged for workflow work.
    Staged,
    /// The device is being transported by the workflow.
    Transported,
    /// The device is awaited by the workflow.
    Awaited,
    /// The workflow deployed the device at an exact location.
    Deployed,
}

/// Completeness of a factory's typed placement projection.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowPlacementIntentCoverage {
    /// All supported placement evidence was decoded.
    Complete,
    /// The factory cannot safely decode all placement evidence.
    #[default]
    Unknown,
}

/// Durable provenance for a placement assertion.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct WorkflowPlacementProvenance {
    /// Workflow that produced the evidence.
    pub workflow_id: WorkflowId,
    /// Optional exact work item that produced the evidence.
    pub work_item_id: Option<WorkItemId>,
}

/// Resolution of one exact failed placement episode.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct WorkflowPlacementResolution {
    /// Device whose failed episode was resolved.
    pub device_code: String,
    /// Failed workflow provenance being resolved.
    pub provenance: WorkflowPlacementProvenance,
}

/// One typed placement assertion emitted by a workflow factory.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct WorkflowPlacementIntent {
    /// Exact device or whole reservation tag covered by the intent.
    pub subject: WorkflowPlacementIntentSubject,
    /// Physical relation asserted by the workflow.
    pub relation: WorkflowPlacementIntentRelation,
    /// Optional exact work item producing this intent.
    pub work_item_id: Option<WorkItemId>,
    /// Exact expected location for a deployed device.
    pub expected_location: Option<String>,
}

/// Typed placement evidence projected from one persisted workflow.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowPlacementIntentProjection {
    /// Whether all supported evidence was decoded.
    pub coverage: WorkflowPlacementIntentCoverage,
    /// Current and retained placement assertions.
    pub intents: Vec<WorkflowPlacementIntent>,
    /// Exact failed episodes resolved by this workflow.
    pub resolutions: Vec<WorkflowPlacementResolution>,
}

impl WorkflowPlacementIntentProjection {
    /// Returns the safe default for a factory without a typed projector.
    #[must_use]
    pub const fn unknown() -> Self {
        Self {
            coverage: WorkflowPlacementIntentCoverage::Unknown,
            intents: Vec::new(),
            resolutions: Vec::new(),
        }
    }
}

impl Default for WorkflowPlacementIntentProjection {
    fn default() -> Self {
        Self::unknown()
    }
}

/// Placement evidence with its durable workflow provenance.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowPlacementIntentEvidence {
    /// Workflow producing this evidence.
    pub workflow_id: WorkflowId,
    /// Registered kind producing this evidence.
    pub workflow_kind: WorkflowKind,
    /// Lifecycle status when evidence was retained.
    pub workflow_status: WorkflowStatus,
    /// Typed placement assertion.
    pub intent: WorkflowPlacementIntent,
}

/// All workflow-derived placement evidence relevant to one device.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowPlacementEvidence {
    /// Evidence from live workflows.
    pub live: Vec<WorkflowPlacementIntentEvidence>,
    /// Succeeded exact deployed placements.
    pub settled_placements: Vec<WorkflowPlacementIntentEvidence>,
    /// Custody left by succeeded or cancelled workflows.
    pub terminal_residuals: Vec<WorkflowPlacementIntentEvidence>,
    /// Unresolved transient custody from failed workflows.
    pub failed_transient: Vec<WorkflowPlacementIntentEvidence>,
    /// Failed evidence removed by an exact succeeded resolution.
    pub resolved_transient: Vec<WorkflowPlacementIntentEvidence>,
    /// Live workflows whose placement evidence is not typed.
    pub unknown_live_workflows: Vec<WorkflowId>,
    /// Succeeded/cancelled workflows whose placement evidence is not typed.
    pub unknown_terminal_outcomes: Vec<WorkflowId>,
}

/// Derived, on-demand placement evidence for all device codes and tags.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowPlacementIntentSnapshot {
    /// Typed live workflow evidence.
    pub live: Vec<WorkflowPlacementIntentEvidence>,
    /// Typed succeeded deployed placement evidence.
    pub settled_placements: Vec<WorkflowPlacementIntentEvidence>,
    /// Typed terminal residual custody evidence.
    pub terminal_residuals: Vec<WorkflowPlacementIntentEvidence>,
    /// Typed failed transient custody evidence.
    pub failed_transient: Vec<WorkflowPlacementIntentEvidence>,
    /// Failed evidence removed by an exact succeeded resolution.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub resolved_transient: Vec<WorkflowPlacementIntentEvidence>,
    /// Typed failed evidence resolutions.
    pub resolutions: Vec<WorkflowPlacementResolution>,
    /// Live workflows with unknown placement coverage.
    pub unknown_live_workflows: Vec<WorkflowId>,
    /// Terminal workflows with unknown placement coverage.
    pub unknown_terminal_outcomes: Vec<WorkflowId>,
}

impl WorkflowPlacementIntentSnapshot {
    /// Explains workflow placement evidence matching a device code or exact tag.
    #[must_use]
    pub fn explain_device(&self, code: &str, tags: &[String]) -> WorkflowPlacementEvidence {
        let canonical_code = canonical_device_code(code);
        let matches = |evidence: &WorkflowPlacementIntentEvidence| match &evidence.intent.subject {
            WorkflowPlacementIntentSubject::Device(subject) => {
                canonical_device_code(subject) == canonical_code
            }
            WorkflowPlacementIntentSubject::DeviceTag(tag) => {
                tags.iter().any(|candidate| candidate == tag)
            }
        };
        WorkflowPlacementEvidence {
            live: self
                .live
                .iter()
                .filter(|item| matches(item))
                .cloned()
                .collect(),
            settled_placements: self
                .settled_placements
                .iter()
                .filter(|item| matches(item))
                .cloned()
                .collect(),
            terminal_residuals: self
                .terminal_residuals
                .iter()
                .filter(|item| matches(item))
                .cloned()
                .collect(),
            failed_transient: self
                .failed_transient
                .iter()
                .filter(|item| matches(item))
                .cloned()
                .collect(),
            resolved_transient: self
                .resolved_transient
                .iter()
                .filter(|item| matches(item))
                .cloned()
                .collect(),
            unknown_live_workflows: self.unknown_live_workflows.clone(),
            unknown_terminal_outcomes: self.unknown_terminal_outcomes.clone(),
        }
    }
}

/// Director retry policy for a terminal workflow failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowFailureDisposition {
    /// Equivalent work may be launched again.
    Retryable,
    /// Equivalent work must remain blocked until its identity changes.
    Permanent,
}

impl WorkflowFailureDisposition {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::Permanent => "permanent",
        }
    }
}

/// Durable description of why a workflow is waiting.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WaitIntent {
    /// Human-readable, non-secret condition description.
    pub description: String,
    /// Optional exact managed event name used as wake-up evidence.
    pub event_name: Option<String>,
    /// Optional exact managed event names used as alternative wake-up evidence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub event_names: Vec<String>,
    /// Optional device code narrowing event evidence.
    pub device_code: Option<String>,
    /// Optional device codes narrowing event evidence to any listed device.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub device_codes: Vec<String>,
    /// Durable managed event cursor from which recovery should continue.
    pub cursor: Option<String>,
    /// Optional absolute Unix deadline in milliseconds.
    pub deadline_millis: Option<i64>,
    /// Milliseconds between authoritative poll fallbacks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub poll_interval_millis: Option<u64>,
}

impl WaitIntent {
    /// Creates a managed-state predicate wait.
    #[must_use]
    pub fn state(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            event_name: None,
            event_names: Vec::new(),
            device_code: None,
            device_codes: Vec::new(),
            cursor: None,
            deadline_millis: None,
            poll_interval_millis: None,
        }
    }

    /// Adds exact managed event evidence to a state-verified wait.
    #[must_use]
    pub fn for_event(mut self, event_name: impl Into<String>) -> Self {
        self.event_name = Some(event_name.into());
        self.event_names.clear();
        self
    }
    /// Adds any of the exact managed event names as wake-up evidence.
    #[must_use]
    pub fn for_events(mut self, event_names: impl IntoIterator<Item = String>) -> Self {
        self.event_name = None;
        self.event_names = event_names.into_iter().collect();
        self
    }

    /// Narrows event evidence to one device code.
    #[must_use]
    pub fn for_device(mut self, device_code: impl Into<String>) -> Self {
        self.device_code = Some(device_code.into());
        self.device_codes.clear();
        self
    }

    /// Narrows event evidence to any of the listed device codes.
    #[must_use]
    pub fn for_devices(mut self, device_codes: impl IntoIterator<Item = String>) -> Self {
        self.device_code = None;
        self.device_codes = device_codes.into_iter().collect();
        self
    }

    /// Adds an absolute Unix deadline in milliseconds.
    #[must_use]
    pub fn until(mut self, deadline_millis: i64) -> Self {
        self.deadline_millis = Some(deadline_millis);
        self
    }

    /// Sets the authoritative poll fallback interval.
    #[must_use]
    pub fn polling_every(mut self, interval: std::time::Duration) -> Self {
        self.poll_interval_millis = Some(
            u64::try_from(interval.as_millis())
                .unwrap_or(u64::MAX)
                .max(1),
        );
        self
    }
}

/// Evidence that caused a durable wait predicate to be rechecked.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitSignal {
    /// The wait was just entered or resumed.
    Initial,
    /// Relevant durable history was recovered after the persisted cursor.
    History,
    /// A matching managed event arrived.
    Event,
    /// Managed state published a revision.
    StateRevision,
    /// The authoritative poll fallback became due.
    Poll,
    /// The bounded event watcher lagged and durable history was recovered.
    WatcherGap,
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

impl FromStr for WorkflowFailureDisposition {
    type Err = RepositoryError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "retryable" => Ok(Self::Retryable),
            "permanent" => Ok(Self::Permanent),
            _ => Err(RepositoryError::InvalidStoredFailureDisposition(
                value.to_owned(),
            )),
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
    /// Director retry policy when the lifecycle status is [`WorkflowStatus::Failed`].
    pub failure_disposition: Option<WorkflowFailureDisposition>,
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

/// Blob-free workflow row for hot status and revision queries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowSummary {
    /// Stable instance identifier.
    pub id: WorkflowId,
    /// Registered workflow kind.
    pub kind: WorkflowKind,
    /// Current lifecycle status.
    pub status: WorkflowStatus,
    /// Current logical step.
    pub current_step: Option<String>,
    /// Last update time in Unix milliseconds.
    pub updated_at: i64,
    /// Optimistic concurrency revision.
    pub revision: u64,
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{
        WaitIntent, WorkflowId, WorkflowKind, WorkflowServiceIntent, WorkflowServiceIntentEvidence,
        WorkflowServiceIntentSnapshot, WorkflowServiceIntentState, WorkflowServiceScope,
        WorkflowStatus,
    };

    fn intent(collect: &str) -> WorkflowServiceIntent {
        WorkflowServiceIntent {
            service: "service".to_owned(),
            dimensions: BTreeMap::from([
                ("collect".to_owned(), collect.to_owned()),
                ("deliver".to_owned(), "hub".to_owned()),
            ]),
        }
    }

    fn evidence(
        id: WorkflowId,
        intents: Vec<WorkflowServiceIntent>,
        unknown_scopes: BTreeSet<WorkflowServiceScope>,
    ) -> WorkflowServiceIntentEvidence {
        WorkflowServiceIntentEvidence {
            workflow_id: id,
            workflow_kind: WorkflowKind::new("test.service").expect("kind"),
            workflow_status: WorkflowStatus::Running,
            intents,
            unknown_scopes,
        }
    }

    #[test]
    fn old_wait_intent_json_remains_compatible() {
        let wait: WaitIntent = serde_json::from_str(
            r#"{"description":"ready","event_name":null,"device_code":"D1","cursor":"1-0","deadline_millis":null}"#,
        )
        .expect("deserialize old wait intent");

        assert_eq!(wait.device_code.as_deref(), Some("D1"));
        assert!(wait.device_codes.is_empty());
        assert_eq!(wait.poll_interval_millis, None);
    }

    #[test]
    fn service_state_prefers_exact_intents_and_matches_scopes_canonically() {
        let exact_id = WorkflowId::new();
        let unknown_id = WorkflowId::new();
        let target = intent("belt-1");
        let snapshot = WorkflowServiceIntentSnapshot {
            live: vec![
                evidence(
                    unknown_id,
                    Vec::new(),
                    BTreeSet::from([WorkflowServiceScope::Region(" Alpha ".to_owned())]),
                ),
                evidence(exact_id, vec![target.clone()], BTreeSet::new()),
            ],
        };

        assert_eq!(
            snapshot.state_for(&target, Some("alpha"), Some("system")),
            WorkflowServiceIntentState::Present(vec![exact_id])
        );
        assert_eq!(
            snapshot.state_for(&intent("other"), Some(" ALPHA "), None),
            WorkflowServiceIntentState::Unknown(vec![unknown_id])
        );
        assert_eq!(
            snapshot.state_for(&intent("other"), Some("beta"), None),
            WorkflowServiceIntentState::Absent
        );
    }
}
