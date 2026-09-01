//! Durable workflow state, execution, and supervision primitives.
//!
//! Upstream game events arrive through the managed client's SSE pipeline.
//! Workflows consume its local watcher and durable journal; they must not open
//! independent upstream event connections. Events only wake a workflow—the
//! managed durable state predicate remains the source of truth.

mod allocation;
mod model;
mod registry;
mod repository;
mod supervisor;
mod telemetry;
mod work;

pub use allocation::{
    AllocationCandidate, AllocationId, AllocationLocation, AllocationSet, AllocationState,
    ReplacementOutcome, RequirementScope, ResourceAllocation, ResourceRequirement,
};
pub use model::{
    AutomationPolicy, AutomationTrigger, ClaimAcquireOutcome, FiniteExecution,
    FiniteExecutionClass, FiniteExecutionStatus, NewTrigger, NewWorkflow, ResourceClaim,
    ResourceKey, TriggerCondition, TriggerId, TriggerState, TriggerTarget, TriggerTargetClass,
    WaitIntent, WaitOutcome, WaitSignal, WorkflowActivity, WorkflowFailureDisposition, WorkflowId,
    WorkflowInstance, WorkflowKind, WorkflowPlacementEvidence, WorkflowPlacementIntent,
    WorkflowPlacementIntentCoverage, WorkflowPlacementIntentEvidence,
    WorkflowPlacementIntentProjection, WorkflowPlacementIntentRelation,
    WorkflowPlacementIntentSnapshot, WorkflowPlacementIntentSubject, WorkflowPlacementProvenance,
    WorkflowPlacementResolution, WorkflowServiceIntent, WorkflowServiceIntentCoverage,
    WorkflowServiceIntentEvidence, WorkflowServiceIntentProjection, WorkflowServiceIntentSnapshot,
    WorkflowServiceIntentState, WorkflowServiceScope, WorkflowState, WorkflowStatus,
    WorkflowSummary,
};
pub use registry::{RegistryError, WorkflowFactory, WorkflowMigration, WorkflowRegistry};
pub use repository::{CreateOrReuseWorkflow, RepositoryError, WorkflowRepository};
pub use supervisor::{
    BoxWorkflowFuture, ControlRequest, SupervisorError, WorkflowContext, WorkflowExecutor,
    WorkflowSupervisor, WorkflowWaitError,
};
pub use telemetry::{WorkflowTelemetrySample, WorkflowTelemetrySink};
pub use work::{
    CampaignCounts, CampaignItemResult, CampaignOutcome, CampaignResult, WorkItem, WorkItemAttempt,
    WorkItemAttemptOutcome, WorkItemId, WorkItemSpec, WorkItemState, WorkItemStatus,
    WorkItemTransition,
};
