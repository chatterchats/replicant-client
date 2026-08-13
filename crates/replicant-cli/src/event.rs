//! Compatibility frontend for event fulfillment.

/// Runs the event command through the reusable runtime service.
pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    replicant_runtime::event::run_cli(arguments).await
}
