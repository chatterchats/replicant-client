//! Best-effort workflow execution telemetry hooks.
//!
//! The workflow crate owns measurement points but deliberately does not own a
//! telemetry backend. Applications may install a sink that only enqueues
//! samples; workflow execution must never block on metrics persistence.

/// One workflow lifecycle observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowTelemetrySample {
    /// Observation timestamp in Unix epoch milliseconds.
    pub observed_at_ms: i64,
    /// Durable workflow identifier. Retained only in short-lived raw telemetry.
    pub workflow_id: String,
    /// Stable workflow kind.
    pub workflow_kind: String,
    /// Stable metric name such as `executor_started` or `claim_conflict`.
    pub metric: &'static str,
    /// Stable outcome/status associated with the metric.
    pub outcome: String,
    /// Optional bounded-cardinality detail such as prior status or resource kind.
    pub detail: Option<String>,
    /// Optional duration associated with this observation.
    pub duration_ms: Option<u64>,
}

/// Destination for best-effort workflow telemetry.
pub trait WorkflowTelemetrySink: Send + Sync + 'static {
    /// Records one workflow observation without performing slow I/O inline.
    fn record(&self, sample: WorkflowTelemetrySample);
}
