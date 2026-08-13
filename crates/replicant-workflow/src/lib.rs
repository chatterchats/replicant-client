//! Durable workflow state and registry primitives.
//!
//! The SQLite row is authoritative. This crate deliberately does not start or
//! supervise workflow tasks.

mod model;
mod registry;
mod repository;

pub use model::{
    NewWorkflow, WorkflowId, WorkflowInstance, WorkflowKind, WorkflowState, WorkflowStatus,
};
pub use registry::{RegistryError, WorkflowFactory, WorkflowRegistry};
pub use repository::{RepositoryError, WorkflowRepository};
