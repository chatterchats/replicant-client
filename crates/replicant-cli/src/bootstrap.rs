//! Compatibility frontend for regional bootstrap execution.

/// Runs the bootstrap command through the reusable runtime service.
pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    replicant_runtime::bootstrap::run_cli(arguments).await
}
