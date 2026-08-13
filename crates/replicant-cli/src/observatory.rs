//! Compatibility frontend for observatory reports, plans, and actions.

pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    replicant_runtime::observatory::run_cli(arguments).await
}
