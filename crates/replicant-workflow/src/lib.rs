//! Durable workflow state, execution, and supervision primitives.

mod model;
mod registry;
mod repository;
mod supervisor;

pub use model::{
    ClaimAcquireOutcome, NewWorkflow, ResourceClaim, ResourceKey, WorkflowActivity, WorkflowId,
    WorkflowInstance, WorkflowKind, WorkflowState, WorkflowStatus,
};
pub use registry::{RegistryError, WorkflowFactory, WorkflowRegistry};
pub use repository::{RepositoryError, WorkflowRepository};
pub use supervisor::{
    BoxWorkflowFuture, ControlRequest, SupervisorError, WorkflowContext, WorkflowExecutor,
    WorkflowSupervisor,
};
