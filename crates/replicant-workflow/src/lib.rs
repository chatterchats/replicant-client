//! Durable workflow state, execution, and supervision primitives.
//!
//! Upstream game events arrive through the managed client's SSE pipeline.
//! Workflows consume its local watcher and durable journal; they must not open
//! independent upstream event connections. Events only wake a workflow—the
//! managed durable state predicate remains the source of truth.

mod model;
mod registry;
mod repository;
mod supervisor;

pub use model::{
    AutomationPolicy, AutomationTrigger, ClaimAcquireOutcome, FiniteExecution,
    FiniteExecutionClass, FiniteExecutionStatus, NewTrigger, NewWorkflow, ResourceClaim,
    ResourceKey, TriggerCondition, TriggerId, TriggerState, TriggerTarget, TriggerTargetClass,
    WaitIntent, WaitOutcome, WorkflowActivity, WorkflowId, WorkflowInstance, WorkflowKind,
    WorkflowState, WorkflowStatus,
};
pub use registry::{RegistryError, WorkflowFactory, WorkflowRegistry};
pub use repository::{RepositoryError, WorkflowRepository};
pub use supervisor::{
    BoxWorkflowFuture, ControlRequest, SupervisorError, WorkflowContext, WorkflowExecutor,
    WorkflowSupervisor, WorkflowWaitError,
};
