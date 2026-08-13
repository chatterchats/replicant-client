use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize, de::DeserializeOwned};
use uuid::Uuid;

use crate::RepositoryError;

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

impl WorkflowStatus {
    /// Returns whether a state replacement may use `next`.
    #[must_use]
    pub fn can_transition_to(self, next: Self) -> bool {
        use WorkflowStatus::{
            Cancelled, Failed, Paused, Queued, Reconciling, Running, Succeeded, Waiting,
        };
        self == next
            || matches!(
                (self, next),
                (Queued, Running | Paused | Cancelled)
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
