//! Compatibility frontend for regional ownership reassignment.

pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    replicant_runtime::ownership::run_cli(arguments).await
}
