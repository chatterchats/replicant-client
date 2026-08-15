//! `replicantd` process entry point.

use std::{error::Error, sync::Arc};

use replicant_runtime::{config::ManagedClientConfig, config::RuntimeConfig, start_managed_client};
use replicant_server::{AppState, DaemonConfig, router, run_supervisor, run_trigger_engine};
use replicant_workflow::WorkflowRepository;
use tokio::{net::TcpListener, sync::watch};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let log_filter = replicant_runtime::config::log_filter_directive();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(&log_filter).unwrap_or_else(|_| EnvFilter::new("info")))
        .try_init()?;

    let config = DaemonConfig::from_env()?;
    let client =
        start_managed_client(ManagedClientConfig::from_env(&config.managed_database)?).await?;
    let repository = Arc::new(WorkflowRepository::open(&config.runtime_database)?);
    let state = AppState::new(
        client.clone(),
        RuntimeConfig::new(&config.profile),
        repository,
        config.clone(),
    )?;
    let listener = TcpListener::bind(config.bind).await?;
    tracing::info!(address = %config.bind, profile = %config.profile, "replicantd listening");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = tokio::spawn(run_supervisor(state.clone(), shutdown_rx.clone()));
    let triggers = tokio::spawn(run_trigger_engine(state.clone(), shutdown_rx.clone()));
    let signal = tokio::spawn(shutdown_signal(shutdown_tx.clone()));
    let server_result = axum::serve(listener, router(state))
        .with_graceful_shutdown(wait_for_shutdown(shutdown_rx))
        .await;
    let _ = shutdown_tx.send(true);
    supervisor.await?;
    triggers.await?;
    signal.abort();
    client.close().await?;
    server_result?;
    Ok(())
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
}

async fn shutdown_signal(shutdown: watch::Sender<bool>) {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => tokio::select! {
                result = tokio::signal::ctrl_c() => {
                    if let Err(error) = result {
                        tracing::error!(error = %error, "Ctrl-C handler failed");
                    }
                },
                _ = terminate.recv() => {}
            },
            Err(error) => {
                tracing::error!(error = %error, "SIGTERM handler failed");
                if let Err(error) = tokio::signal::ctrl_c().await {
                    tracing::error!(error = %error, "Ctrl-C handler failed");
                }
            }
        }
    }
    #[cfg(not(unix))]
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %error, "Ctrl-C handler failed");
    }
    let _ = shutdown.send(true);
}
