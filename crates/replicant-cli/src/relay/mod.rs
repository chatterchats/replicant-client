//! Compatibility frontend for relay expansion.

pub(crate) use replicant_runtime::relay::{RelayExpansionRequest, execute_expansion};

/// Runs the relay command through the reusable runtime service.
pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    replicant_runtime::relay::run_cli(arguments).await
}
