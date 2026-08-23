//! `replicantd` process entry point.

use std::{
    error::Error,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::Path,
    sync::{Arc, Mutex},
};

use replicant_runtime::{
    config::ManagedClientConfig,
    config::RuntimeConfig,
    empire_telemetry::EmpireTelemetryService,
    mission_stock::reconcile_legacy_mission_tags,
    start_managed_client,
    telemetry::{RuntimeTelemetrySample, TelemetryService},
};
use replicant_server::{
    AppState, DaemonConfig, router, run_director, run_supervisor, run_trigger_engine,
};
use replicant_workflow::WorkflowRepository;
use tokio::{net::TcpListener, sync::watch};
use tracing_subscriber::{EnvFilter, fmt::MakeWriter, prelude::*};

#[derive(Clone)]
struct PersistentLogWriter {
    file: Arc<Mutex<File>>,
}

impl PersistentLogWriter {
    fn open(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }
}

impl Write for PersistentLogWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.file
            .lock()
            .map_err(|_| io::Error::other("persistent log writer lock poisoned"))?
            .write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file
            .lock()
            .map_err(|_| io::Error::other("persistent log writer lock poisoned"))?
            .flush()
    }
}

impl<'writer> MakeWriter<'writer> for PersistentLogWriter {
    type Writer = Self;

    fn make_writer(&'writer self) -> Self::Writer {
        self.clone()
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = DaemonConfig::from_env()?;
    let log_filter = replicant_runtime::config::log_filter_directive();
    init_logging(&config, &log_filter)?;

    tracing::info!(
        managed_database = %config.managed_database.display(),
        runtime_database = %config.runtime_database.display(),
        telemetry_database = %config.telemetry_database.display(),
        log_directory = %config.log_directory.display(),
        log_filter = %log_filter,
        "replicantd startup configuration resolved"
    );

    let telemetry = TelemetryService::start(&config.telemetry_database)?;
    let runtime_telemetry = telemetry.runtime_sink();
    runtime_telemetry.record(RuntimeTelemetrySample {
        observed_at_ms: unix_millis(),
        metric: "daemon_lifecycle",
        series: "started".to_owned(),
        value: 1,
        duration_ms: None,
    });
    let client = start_managed_client(
        ManagedClientConfig::from_env(&config.managed_database)?
            .with_api_telemetry_sink(telemetry.api_sink())
            .with_event_telemetry_sink(telemetry.event_sink()),
    )
    .await?;
    let empire_blueprints = match client.blueprints().list().await {
        Ok(blueprints) => blueprints,
        Err(error) => {
            tracing::warn!(
                error = %error,
                "empire telemetry could not snapshot the current blueprint catalogue"
            );
            Vec::new()
        }
    };
    let history_database =
        replicant_client::default_history_database_path(&config.managed_database);
    let empire_telemetry = match EmpireTelemetryService::start(
        &config.managed_database,
        &history_database,
        &config.telemetry_database,
        empire_blueprints,
    ) {
        Ok(service) => Some(service),
        Err(error) => {
            tracing::warn!(
                error = %error,
                history_database = %history_database.display(),
                "empire telemetry is unavailable; continuing without in-game history projection"
            );
            None
        }
    };
    let repository = Arc::new(WorkflowRepository::open(&config.runtime_database)?);
    match reconcile_legacy_mission_tags(&client, repository.as_ref()).await {
        Ok(report) if report.migrated_devices > 0 => {
            tracing::info!(
                mapped_legacy_tags = report.mapped_legacy_tags,
                migrated_devices = report.migrated_devices,
                ambiguous_devices = report.ambiguous_devices,
                "reconciled UUID-derived mission tags into reusable system stock"
            );
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(
                error = %error,
                "legacy mission-tag reconciliation failed; continuing daemon startup"
            );
        }
    }
    let state = AppState::new_with_telemetry(
        client.clone(),
        RuntimeConfig::new(&config.profile),
        repository,
        config.clone(),
        Some(telemetry.workflow_sink()),
        Some(telemetry.runtime_sink()),
    )?;
    let listener = TcpListener::bind(config.bind).await?;
    tracing::info!(
        address = %config.bind,
        profile = %config.profile,
        authenticated = config.token.is_some(),
        "replicantd listening"
    );
    if config.token.is_none() {
        tracing::info!(
            "no REPLICANTD_TOKEN set; loopback-only access is assumed to be local and trusted"
        );
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = tokio::spawn(run_supervisor(state.clone(), shutdown_rx.clone()));
    let triggers = tokio::spawn(run_trigger_engine(state.clone(), shutdown_rx.clone()));
    let director = tokio::spawn(run_director(state.clone(), shutdown_rx.clone()));
    let signal = tokio::spawn(shutdown_signal(shutdown_tx.clone()));
    let server_result = axum::serve(listener, router(state))
        .with_graceful_shutdown(wait_for_shutdown(shutdown_rx))
        .await;
    let _ = shutdown_tx.send(true);
    supervisor.await?;
    triggers.await?;
    director.await?;
    signal.abort();
    runtime_telemetry.record(RuntimeTelemetrySample {
        observed_at_ms: unix_millis(),
        metric: "daemon_lifecycle",
        series: "stopped".to_owned(),
        value: 1,
        duration_ms: None,
    });
    if let Some(empire_telemetry) = empire_telemetry
        && let Err(error) = empire_telemetry.shutdown()
    {
        tracing::warn!(error = %error, "empire telemetry shutdown did not complete cleanly");
    }
    client.close().await?;
    telemetry.shutdown()?;
    server_result?;
    tracing::info!("replicantd shutdown complete");
    Ok(())
}

fn init_logging(
    config: &DaemonConfig,
    log_filter: &str,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    fs::create_dir_all(&config.log_directory)?;
    let log_path = config.log_directory.join("replicantd.log");
    let file_writer = PersistentLogWriter::open(&log_path)?;
    let (filter, filter_error) = match EnvFilter::try_new(log_filter) {
        Ok(filter) => (filter, None),
        Err(error) => (EnvFilter::new("info"), Some(error.to_string())),
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(true)
                .with_writer(io::stderr),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_target(true)
                .with_writer(file_writer),
        )
        .try_init()?;

    if let Some(error) = filter_error {
        tracing::warn!(
            requested = %log_filter,
            error = %error,
            "invalid RUST_LOG directive; falling back to info"
        );
    }
    tracing::info!(path = %log_path.display(), "persistent daemon logging initialized");
    Ok(())
}

fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
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
