use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{WorkflowId, WorkflowKind, WorkflowStatus};

/// Stable identifier for a persisted workflow work item.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkItemId(Uuid);

impl WorkItemId {
    /// Creates a unique work-item identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub(crate) fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for WorkItemId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for WorkItemId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for WorkItemId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Durable lifecycle state of one workflow-owned work item.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkItemStatus {
    /// Eligible to be assigned when its retry time is due.
    Pending,
    /// Claimed by a scheduler but not yet executing.
    Assigned,
    /// Executing under an open attempt interval.
    Running,
    /// Blocked on a retryable precondition or resource.
    Waiting,
    /// Completed successfully.
    Succeeded,
    /// Completed because its desired state was already satisfied.
    Skipped,
    /// Permanently infeasible or terminally unsuccessful.
    Failed,
    /// Cancelled because its owning campaign terminated.
    Abandoned,
}

impl WorkItemStatus {
    /// Returns whether no further transition is permitted.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Skipped | Self::Failed | Self::Abandoned
        )
    }

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Assigned => "assigned",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Succeeded => "succeeded",
            Self::Skipped => "skipped",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

/// Outcome recorded when a work-item attempt interval closes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkItemAttemptOutcome {
    /// The attempted work completed successfully.
    Succeeded,
    /// The attempted work failed and entered retry or terminal handling.
    Failed,
    /// Execution yielded safely without counting as a failure.
    Reclaimed,
    /// The owning workflow cancelled or abandoned the work.
    Cancelled,
}

impl WorkItemAttemptOutcome {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Reclaimed => "reclaimed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Terminal semantic outcome of an aggregated campaign.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CampaignOutcome {
    /// At least one item succeeded and none failed or were abandoned.
    AllSucceeded,
    /// Successful work coexists with failed or abandoned work.
    PartialSuccess,
    /// No item began execution before the campaign became terminal.
    NothingCouldStart,
    /// Work began, but no item succeeded.
    NoSuccess,
}

/// Immutable desired specification of a workflow-owned work item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkItemSpec {
    /// Owning campaign workflow.
    pub workflow_id: WorkflowId,
    /// Stable idempotency key within the campaign.
    pub dedupe_key: String,
    /// Registered executor kind.
    pub kind: WorkflowKind,
    /// Deterministic scheduler ordering key.
    pub sort_key: String,
    /// Executor-owned typed payload encoded as JSON.
    pub payload_json: Value,
    /// Stored typed preconditions encoded as JSON.
    pub preconditions_json: Value,
    /// Stored resource requirements encoded as JSON.
    pub requirements_json: Value,
    /// Optional explicit Unix deadline in milliseconds.
    pub deadline_at_ms: Option<i64>,
}

/// Mutable durable state of a workflow-owned work item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkItemState {
    /// Current lifecycle status.
    pub status: WorkItemStatus,
    /// Latest executor checkpoint.
    pub checkpoint_json: Option<Value>,
    /// Terminal structured result.
    pub result_json: Option<Value>,
    /// Latest sanitized error or wait reason.
    pub last_error: Option<String>,
    /// Number of execution attempts started.
    pub attempt_count: u32,
    /// Consecutive retryable failures since a successful commit.
    pub consecutive_failure_count: u32,
    /// Earliest next eligibility time in Unix milliseconds.
    pub next_attempt_at_ms: Option<i64>,
    /// Whether any attempt has ever started.
    pub ever_started: bool,
    /// Creation time in Unix milliseconds.
    pub created_at_ms: i64,
    /// Last update time in Unix milliseconds.
    pub updated_at_ms: i64,
    /// Optimistic concurrency revision.
    pub revision: u64,
}

/// One persisted workflow-owned work item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkItem {
    /// Stable item identifier.
    pub id: WorkItemId,
    /// Immutable desired specification.
    pub spec: WorkItemSpec,
    /// Mutable durable state.
    pub state: WorkItemState,
}

/// One execution interval for a work item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkItemAttempt {
    /// Work item whose execution was attempted.
    pub item_id: WorkItemId,
    /// Scheduler assignment identifier.
    pub assignment_id: String,
    /// Replicant or worker identity used by the attempt.
    pub worker_identity: String,
    /// One-based attempt ordinal for this item.
    pub attempt_ordinal: u32,
    /// Attempt start time in Unix milliseconds.
    pub started_at_ms: i64,
    /// Attempt end time in Unix milliseconds, when closed.
    pub ended_at_ms: Option<i64>,
    /// Terminal interval outcome, when closed.
    pub outcome: Option<WorkItemAttemptOutcome>,
    /// Sanitized attempt error, when applicable.
    pub error: Option<String>,
}

/// Counts of work items by lifecycle status.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CampaignCounts {
    /// Total item count.
    pub total: u32,
    /// Pending item count.
    pub pending: u32,
    /// Assigned item count.
    pub assigned: u32,
    /// Running item count.
    pub running: u32,
    /// Waiting item count.
    pub waiting: u32,
    /// Successful item count.
    pub succeeded: u32,
    /// Already-satisfied item count.
    pub skipped: u32,
    /// Failed item count.
    pub failed: u32,
    /// Abandoned item count.
    pub abandoned: u32,
}

/// Terminal result projection for one campaign item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CampaignItemResult {
    /// Stable item identifier.
    pub item_id: WorkItemId,
    /// Stable campaign-local deduplication key.
    pub dedupe_key: String,
    /// Terminal item status.
    pub status: WorkItemStatus,
    /// Optional structured executor result.
    pub result_json: Option<Value>,
    /// Optional sanitized error.
    pub error: Option<String>,
}

/// Aggregated terminal result for a workflow campaign.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CampaignResult {
    /// Semantic campaign outcome.
    pub outcome: CampaignOutcome,
    /// Counts of every item status.
    pub counts: CampaignCounts,
    /// Deterministically ordered item results.
    pub items: Vec<CampaignItemResult>,
}

impl CampaignResult {
    /// Maps the semantic campaign outcome to the existing workflow lifecycle.
    #[must_use]
    pub const fn workflow_status(&self) -> WorkflowStatus {
        match self.outcome {
            CampaignOutcome::AllSucceeded | CampaignOutcome::PartialSuccess => {
                WorkflowStatus::Succeeded
            }
            CampaignOutcome::NoSuccess => WorkflowStatus::Failed,
            CampaignOutcome::NothingCouldStart => {
                if self.counts.failed != 0 || self.counts.abandoned != 0 {
                    WorkflowStatus::Failed
                } else {
                    WorkflowStatus::Succeeded
                }
            }
        }
    }
}

/// Requested atomic state transition for one work item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "transition")]
pub enum WorkItemTransition {
    /// Persist a safe commit boundary while the attempt remains running.
    CheckpointCommitted {
        /// Replacement checkpoint.
        checkpoint_json: Value,
    },
    /// Wait for a retryable condition without counting a failure.
    Waiting {
        /// Optional replacement checkpoint.
        checkpoint_json: Option<Value>,
        /// Sanitized wait reason.
        reason: String,
        /// Optional exact retry time in Unix milliseconds.
        retry_at_ms: Option<i64>,
    },
    /// Close the attempt as failed and schedule item-local retry.
    RetryableFailure {
        /// Optional replacement checkpoint.
        checkpoint_json: Option<Value>,
        /// Sanitized failure summary.
        error: String,
    },
    /// Complete the item successfully.
    Succeeded {
        /// Optional final checkpoint.
        checkpoint_json: Option<Value>,
        /// Optional structured result.
        result_json: Option<Value>,
    },
    /// Complete without mutation because the state is already satisfied.
    Skipped {
        /// Sanitized skip reason.
        reason: String,
        /// Optional structured result.
        result_json: Option<Value>,
    },
    /// Complete with permanent failure.
    Failed {
        /// Sanitized failure summary.
        error: String,
        /// Optional structured result.
        result_json: Option<Value>,
    },
    /// Cancel remaining work.
    Abandoned {
        /// Sanitized abandonment reason.
        reason: String,
    },
    /// Yield safely back to the pending pool.
    Reclaimed {
        /// Optional replacement checkpoint.
        checkpoint_json: Option<Value>,
    },
}
