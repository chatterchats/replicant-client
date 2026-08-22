//! Durable, non-blocking application telemetry for Grafana and other readers.
//!
//! The Replicant Space transport emits typed request-attempt samples through
//! [`replicant_client::raw::ApiTelemetrySink`]. This module persists those
//! samples into an isolated SQLite database, maintains mergeable time-series
//! rollups, and prunes high-resolution history without ever blocking API work.

use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError, TrySendError},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use replicant_client::{
    managed::{EventTelemetrySample, EventTelemetrySink},
    raw::{ApiAttemptTelemetry, ApiTelemetrySink},
};
use replicant_workflow::{WorkflowTelemetrySample, WorkflowTelemetrySink};
use rusqlite::{Connection, Transaction, params};
use thiserror::Error;

const CHANNEL_CAPACITY: usize = 8_192;
const BATCH_LIMIT: usize = 256;
const BATCH_WAIT: Duration = Duration::from_millis(250);
const MAINTENANCE_INTERVAL_MS: i64 = 6 * 60 * 60 * 1_000;
const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
const RAW_RETENTION_MS: i64 = 7 * DAY_MS;
const ONE_MINUTE_RETENTION_MS: i64 = 7 * DAY_MS;
const TEN_MINUTE_RETENTION_MS: i64 = 30 * DAY_MS;
const HOURLY_RETENTION_MS: i64 = 90 * DAY_MS;
const RESOLUTIONS_SECONDS: [i64; 4] = [60, 600, 3_600, 86_400];

const SCHEMA: &str = r#"
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS telemetry_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS api_request_attempt (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at_ms INTEGER NOT NULL,
    local_request_id TEXT NOT NULL,
    server_request_id TEXT,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    route_key TEXT NOT NULL,
    rate_limit_bucket TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    status_code INTEGER,
    outcome TEXT NOT NULL,
    response_bytes INTEGER,
    rate_limit_wait_ms INTEGER NOT NULL,
    request_prepare_ms INTEGER,
    time_to_headers_ms INTEGER,
    metadata_ms INTEGER,
    body_read_ms INTEGER,
    decode_ms INTEGER,
    elapsed_ms INTEGER NOT NULL,
    rate_limit_limit INTEGER,
    rate_limit_remaining INTEGER,
    rate_limit_reset_epoch_seconds INTEGER,
    retry_after_ms INTEGER
);

CREATE INDEX IF NOT EXISTS idx_api_attempt_time
    ON api_request_attempt(observed_at_ms);
CREATE INDEX IF NOT EXISTS idx_api_attempt_route_time
    ON api_request_attempt(route_key, observed_at_ms);
CREATE INDEX IF NOT EXISTS idx_api_attempt_status_time
    ON api_request_attempt(status_code, observed_at_ms);
CREATE INDEX IF NOT EXISTS idx_api_attempt_request
    ON api_request_attempt(local_request_id, attempt);

CREATE TABLE IF NOT EXISTS api_request_rollup (
    bucket_start_ms INTEGER NOT NULL,
    resolution_seconds INTEGER NOT NULL,
    method TEXT NOT NULL,
    route_key TEXT NOT NULL,
    rate_limit_bucket TEXT NOT NULL,
    status_code INTEGER NOT NULL,
    outcome TEXT NOT NULL,
    request_count INTEGER NOT NULL,
    logical_request_count INTEGER NOT NULL DEFAULT 0,
    retry_attempt_count INTEGER NOT NULL,
    elapsed_sum_ms INTEGER NOT NULL,
    elapsed_max_ms INTEGER NOT NULL,
    request_prepare_sum_ms INTEGER NOT NULL DEFAULT 0,
    request_prepare_count INTEGER NOT NULL DEFAULT 0,
    time_to_headers_sum_ms INTEGER NOT NULL,
    time_to_headers_count INTEGER NOT NULL,
    metadata_sum_ms INTEGER NOT NULL DEFAULT 0,
    metadata_count INTEGER NOT NULL DEFAULT 0,
    body_read_sum_ms INTEGER NOT NULL DEFAULT 0,
    body_read_count INTEGER NOT NULL DEFAULT 0,
    decode_sum_ms INTEGER NOT NULL DEFAULT 0,
    decode_count INTEGER NOT NULL DEFAULT 0,
    rate_limit_wait_sum_ms INTEGER NOT NULL,
    response_bytes_sum INTEGER NOT NULL,
    response_bytes_count INTEGER NOT NULL,
    elapsed_le_25_count INTEGER NOT NULL,
    elapsed_le_50_count INTEGER NOT NULL,
    elapsed_le_100_count INTEGER NOT NULL,
    elapsed_le_200_count INTEGER NOT NULL,
    elapsed_le_350_count INTEGER NOT NULL,
    elapsed_le_500_count INTEGER NOT NULL,
    elapsed_le_750_count INTEGER NOT NULL,
    elapsed_le_1000_count INTEGER NOT NULL,
    elapsed_le_1500_count INTEGER NOT NULL,
    elapsed_le_2500_count INTEGER NOT NULL,
    elapsed_le_5000_count INTEGER NOT NULL,
    elapsed_le_10000_count INTEGER NOT NULL,
    elapsed_le_30000_count INTEGER NOT NULL,
    rate_limit_limit_last INTEGER,
    rate_limit_remaining_last INTEGER,
    rate_limit_reset_epoch_seconds_last INTEGER,
    PRIMARY KEY (
        bucket_start_ms,
        resolution_seconds,
        method,
        route_key,
        rate_limit_bucket,
        status_code,
        outcome
    )
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_api_rollup_resolution_time
    ON api_request_rollup(resolution_seconds, bucket_start_ms);
CREATE INDEX IF NOT EXISTS idx_api_rollup_status_time
    ON api_request_rollup(resolution_seconds, status_code, bucket_start_ms);
CREATE INDEX IF NOT EXISTS idx_api_rollup_route_time
    ON api_request_rollup(resolution_seconds, route_key, bucket_start_ms);

CREATE TABLE IF NOT EXISTS event_telemetry_sample (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at_ms INTEGER NOT NULL,
    metric TEXT NOT NULL,
    outcome TEXT NOT NULL,
    event_name TEXT,
    event_count INTEGER NOT NULL,
    page_count INTEGER NOT NULL,
    duration_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_event_telemetry_time
    ON event_telemetry_sample(observed_at_ms);
CREATE INDEX IF NOT EXISTS idx_event_telemetry_metric_time
    ON event_telemetry_sample(metric, observed_at_ms);

CREATE TABLE IF NOT EXISTS event_telemetry_rollup (
    bucket_start_ms INTEGER NOT NULL,
    resolution_seconds INTEGER NOT NULL,
    metric TEXT NOT NULL,
    outcome TEXT NOT NULL,
    event_name TEXT NOT NULL,
    sample_count INTEGER NOT NULL,
    event_count_sum INTEGER NOT NULL,
    page_count_sum INTEGER NOT NULL,
    duration_sum_ms INTEGER NOT NULL,
    duration_count INTEGER NOT NULL,
    duration_max_ms INTEGER NOT NULL,
    PRIMARY KEY (bucket_start_ms, resolution_seconds, metric, outcome, event_name)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_event_rollup_resolution_time
    ON event_telemetry_rollup(resolution_seconds, bucket_start_ms);

CREATE TABLE IF NOT EXISTS workflow_telemetry_sample (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at_ms INTEGER NOT NULL,
    workflow_id TEXT NOT NULL,
    workflow_kind TEXT NOT NULL,
    metric TEXT NOT NULL,
    outcome TEXT NOT NULL,
    detail TEXT,
    duration_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_workflow_telemetry_time
    ON workflow_telemetry_sample(observed_at_ms);
CREATE INDEX IF NOT EXISTS idx_workflow_telemetry_kind_time
    ON workflow_telemetry_sample(workflow_kind, observed_at_ms);

CREATE TABLE IF NOT EXISTS workflow_telemetry_rollup (
    bucket_start_ms INTEGER NOT NULL,
    resolution_seconds INTEGER NOT NULL,
    workflow_kind TEXT NOT NULL,
    metric TEXT NOT NULL,
    outcome TEXT NOT NULL,
    detail TEXT NOT NULL,
    sample_count INTEGER NOT NULL,
    duration_sum_ms INTEGER NOT NULL,
    duration_count INTEGER NOT NULL,
    duration_max_ms INTEGER NOT NULL,
    PRIMARY KEY (bucket_start_ms, resolution_seconds, workflow_kind, metric, outcome, detail)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_workflow_rollup_resolution_time
    ON workflow_telemetry_rollup(resolution_seconds, bucket_start_ms);
CREATE INDEX IF NOT EXISTS idx_workflow_rollup_kind_time
    ON workflow_telemetry_rollup(resolution_seconds, workflow_kind, bucket_start_ms);

CREATE TABLE IF NOT EXISTS runtime_telemetry_sample (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    observed_at_ms INTEGER NOT NULL,
    metric TEXT NOT NULL,
    series TEXT NOT NULL,
    value INTEGER NOT NULL,
    duration_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_runtime_telemetry_time
    ON runtime_telemetry_sample(observed_at_ms);
CREATE INDEX IF NOT EXISTS idx_runtime_telemetry_metric_time
    ON runtime_telemetry_sample(metric, observed_at_ms);

CREATE TABLE IF NOT EXISTS runtime_telemetry_rollup (
    bucket_start_ms INTEGER NOT NULL,
    resolution_seconds INTEGER NOT NULL,
    metric TEXT NOT NULL,
    series TEXT NOT NULL,
    sample_count INTEGER NOT NULL,
    value_sum INTEGER NOT NULL,
    value_max INTEGER NOT NULL,
    duration_sum_ms INTEGER NOT NULL,
    duration_count INTEGER NOT NULL,
    duration_max_ms INTEGER NOT NULL,
    PRIMARY KEY (bucket_start_ms, resolution_seconds, metric, series)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_runtime_rollup_resolution_time
    ON runtime_telemetry_rollup(resolution_seconds, bucket_start_ms);
"#;

/// Errors opening, migrating, or closing the telemetry service.
#[derive(Debug, Error)]
pub enum TelemetryError {
    /// SQLite telemetry storage could not be opened or updated.
    #[error("telemetry SQLite failure: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Telemetry filesystem setup failed.
    #[error("telemetry filesystem failure: {0}")]
    Io(#[from] std::io::Error),
    /// The telemetry writer stopped before shutdown completed.
    #[error("telemetry writer stopped unexpectedly")]
    WriterStopped,
    /// The telemetry writer thread panicked.
    #[error("telemetry writer thread panicked")]
    WriterPanicked,
}

/// Returns the sibling database path used for daemon telemetry.
#[must_use]
pub fn default_telemetry_database_path(managed_database: impl AsRef<Path>) -> PathBuf {
    let path = managed_database.as_ref();
    if path.file_name().and_then(|name| name.to_str()) == Some("replicant-client.sqlite") {
        return path.with_file_name("replicant-telemetry.sqlite");
    }
    path.with_extension("telemetry.sqlite")
}

/// One low-volume daemon/runtime measurement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeTelemetrySample {
    /// Unix timestamp in milliseconds.
    pub observed_at_ms: i64,
    /// Stable metric name.
    pub metric: &'static str,
    /// Series/category within the metric.
    pub series: String,
    /// Additive measurement value.
    pub value: i64,
    /// Optional duration associated with the measurement.
    pub duration_ms: Option<u64>,
}

/// Best-effort destination for daemon/runtime measurements.
pub trait RuntimeTelemetrySink: Send + Sync + 'static {
    /// Records one measurement without blocking gameplay work.
    fn record(&self, sample: RuntimeTelemetrySample);
}

#[derive(Clone)]
struct ChannelTelemetrySink {
    sender: SyncSender<TelemetryMessage>,
    dropped: Arc<AtomicU64>,
}

impl ChannelTelemetrySink {
    fn try_record(&self, sample: TelemetrySample) {
        match self.sender.try_send(TelemetryMessage::Sample(sample)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl ApiTelemetrySink for ChannelTelemetrySink {
    fn record(&self, sample: ApiAttemptTelemetry) {
        self.try_record(TelemetrySample::Api(Box::new(sample)));
    }
}

impl EventTelemetrySink for ChannelTelemetrySink {
    fn record(&self, sample: EventTelemetrySample) {
        self.try_record(TelemetrySample::Event(Box::new(sample)));
    }
}

impl WorkflowTelemetrySink for ChannelTelemetrySink {
    fn record(&self, sample: WorkflowTelemetrySample) {
        self.try_record(TelemetrySample::Workflow(Box::new(sample)));
    }
}

impl RuntimeTelemetrySink for ChannelTelemetrySink {
    fn record(&self, sample: RuntimeTelemetrySample) {
        self.try_record(TelemetrySample::Runtime(Box::new(sample)));
    }
}

enum TelemetrySample {
    Api(Box<ApiAttemptTelemetry>),
    Event(Box<EventTelemetrySample>),
    Workflow(Box<WorkflowTelemetrySample>),
    Runtime(Box<RuntimeTelemetrySample>),
}

enum TelemetryMessage {
    Sample(TelemetrySample),
    Shutdown(mpsc::Sender<()>),
}

/// Running telemetry writer and lifecycle handle.
///
/// Producers only enqueue into a bounded channel. SQLite I/O and rollup
/// maintenance happen on the dedicated writer thread.
pub struct TelemetryService {
    sender: SyncSender<TelemetryMessage>,
    sink: Arc<ChannelTelemetrySink>,
    dropped: Arc<AtomicU64>,
    write_failures: Arc<AtomicU64>,
    writer: Option<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for TelemetryService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TelemetryService")
            .field("dropped_samples", &self.dropped_samples())
            .field("write_failures", &self.write_failures())
            .finish_non_exhaustive()
    }
}

impl TelemetryService {
    /// Opens/migrates a telemetry database and starts its bounded writer.
    pub fn start(path: impl AsRef<Path>) -> Result<Self, TelemetryError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        let connection = open_database(&path)?;
        let initial_dropped = meta_u64(&connection, "dropped_samples")?;
        let initial_write_failures = meta_u64(&connection, "write_failures")?;
        drop(connection);

        let (sender, receiver) = mpsc::sync_channel(CHANNEL_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(initial_dropped));
        let write_failures = Arc::new(AtomicU64::new(initial_write_failures));
        let sink = Arc::new(ChannelTelemetrySink {
            sender: sender.clone(),
            dropped: dropped.clone(),
        });
        let worker_dropped = dropped.clone();
        let worker_write_failures = write_failures.clone();
        let writer = thread::Builder::new()
            .name("replicant-telemetry".to_owned())
            .spawn(move || writer_main(&path, receiver, worker_dropped, worker_write_failures))?;

        Ok(Self {
            sender,
            sink,
            dropped,
            write_failures,
            writer: Some(writer),
        })
    }

    /// Returns the sink installed into the managed/raw HTTP client.
    #[must_use]
    pub fn api_sink(&self) -> Arc<dyn ApiTelemetrySink> {
        self.sink.clone()
    }

    /// Returns the sink installed into managed event/SSE processing.
    #[must_use]
    pub fn event_sink(&self) -> Arc<dyn EventTelemetrySink> {
        self.sink.clone()
    }

    /// Returns the sink installed into durable workflow execution.
    #[must_use]
    pub fn workflow_sink(&self) -> Arc<dyn WorkflowTelemetrySink> {
        self.sink.clone()
    }

    /// Returns the sink used by daemon/runtime loops.
    #[must_use]
    pub fn runtime_sink(&self) -> Arc<dyn RuntimeTelemetrySink> {
        self.sink.clone()
    }

    /// Number of samples dropped because the bounded queue was unavailable.
    #[must_use]
    pub fn dropped_samples(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Number of SQLite persistence/maintenance failures observed by the writer.
    #[must_use]
    pub fn write_failures(&self) -> u64 {
        self.write_failures.load(Ordering::Relaxed)
    }

    /// Flushes queued telemetry and stops the writer thread.
    pub fn shutdown(mut self) -> Result<(), TelemetryError> {
        let (ack_tx, ack_rx) = mpsc::channel();
        self.sender
            .send(TelemetryMessage::Shutdown(ack_tx))
            .map_err(|_| TelemetryError::WriterStopped)?;
        ack_rx.recv().map_err(|_| TelemetryError::WriterStopped)?;
        let Some(writer) = self.writer.take() else {
            return Ok(());
        };
        writer.join().map_err(|_| TelemetryError::WriterPanicked)?;
        Ok(())
    }
}

fn open_database(path: &Path) -> Result<Connection, TelemetryError> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(15))?;
    let table_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'",
        [],
        |row| row.get(0),
    )?;
    if table_count == 0 {
        connection.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;")?;
    }
    connection.execute_batch(
        "PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL; PRAGMA foreign_keys = ON;",
    )?;
    connection.execute_batch(SCHEMA)?;
    migrate_api_rollup(&connection)?;
    connection.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('schema_version', '2') \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [],
    )?;
    connection.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('dropped_samples', '0') \
         ON CONFLICT(key) DO NOTHING",
        [],
    )?;
    connection.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('write_failures', '0') \
         ON CONFLICT(key) DO NOTHING",
        [],
    )?;
    Ok(connection)
}

fn migrate_api_rollup(connection: &Connection) -> rusqlite::Result<()> {
    for (column, statement) in [
        (
            "logical_request_count",
            "ALTER TABLE api_request_rollup ADD COLUMN logical_request_count INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "request_prepare_sum_ms",
            "ALTER TABLE api_request_rollup ADD COLUMN request_prepare_sum_ms INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "request_prepare_count",
            "ALTER TABLE api_request_rollup ADD COLUMN request_prepare_count INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "metadata_sum_ms",
            "ALTER TABLE api_request_rollup ADD COLUMN metadata_sum_ms INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "metadata_count",
            "ALTER TABLE api_request_rollup ADD COLUMN metadata_count INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "body_read_sum_ms",
            "ALTER TABLE api_request_rollup ADD COLUMN body_read_sum_ms INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "body_read_count",
            "ALTER TABLE api_request_rollup ADD COLUMN body_read_count INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "decode_sum_ms",
            "ALTER TABLE api_request_rollup ADD COLUMN decode_sum_ms INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "decode_count",
            "ALTER TABLE api_request_rollup ADD COLUMN decode_count INTEGER NOT NULL DEFAULT 0",
        ),
        (
            "rate_limit_limit_last",
            "ALTER TABLE api_request_rollup ADD COLUMN rate_limit_limit_last INTEGER",
        ),
        (
            "rate_limit_remaining_last",
            "ALTER TABLE api_request_rollup ADD COLUMN rate_limit_remaining_last INTEGER",
        ),
        (
            "rate_limit_reset_epoch_seconds_last",
            "ALTER TABLE api_request_rollup ADD COLUMN rate_limit_reset_epoch_seconds_last INTEGER",
        ),
    ] {
        ensure_api_rollup_column(connection, column, statement)?;
    }
    connection.execute(
        "UPDATE api_request_rollup \
         SET logical_request_count = MAX(request_count - retry_attempt_count, 0) \
         WHERE logical_request_count = 0 AND request_count > 0",
        [],
    )?;
    Ok(())
}

fn ensure_api_rollup_column(
    connection: &Connection,
    column: &str,
    statement: &str,
) -> rusqlite::Result<()> {
    let exists: i64 = connection.query_row(
        "SELECT COUNT(*) FROM pragma_table_info('api_request_rollup') WHERE name = ?1",
        [column],
        |row| row.get(0),
    )?;
    if exists == 0 {
        connection.execute_batch(statement)?;
    }
    Ok(())
}

fn meta_u64(connection: &Connection, key: &str) -> rusqlite::Result<u64> {
    connection
        .query_row(
            "SELECT COALESCE((SELECT CAST(value AS INTEGER) FROM telemetry_meta WHERE key = ?1), 0)",
            [key],
            |row| row.get::<_, i64>(0),
        )
        .map(|value| u64::try_from(value).unwrap_or_default())
}

fn writer_main(
    path: &Path,
    receiver: Receiver<TelemetryMessage>,
    dropped: Arc<AtomicU64>,
    write_failures: Arc<AtomicU64>,
) {
    let Ok(mut connection) = open_database(path) else {
        tracing::error!(path = %path.display(), "telemetry writer could not reopen database");
        write_failures.fetch_add(1, Ordering::Relaxed);
        return;
    };
    let mut last_maintenance = 0_i64;
    let mut pending = Vec::with_capacity(BATCH_LIMIT);

    loop {
        match receiver.recv_timeout(BATCH_WAIT) {
            Ok(TelemetryMessage::Sample(sample)) => pending.push(sample),
            Ok(TelemetryMessage::Shutdown(ack)) => {
                flush_batch(&mut connection, &mut pending, &dropped, &write_failures);
                if let Err(error) =
                    run_maintenance(&connection, now_millis(), &dropped, &write_failures)
                {
                    write_failures.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(error = %error, "telemetry retention maintenance failed");
                }
                let _ = ack.send(());
                break;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                flush_batch(&mut connection, &mut pending, &dropped, &write_failures);
                break;
            }
        }

        let shutdown = drain_pending(&receiver, &mut pending);
        if !pending.is_empty() {
            flush_batch(&mut connection, &mut pending, &dropped, &write_failures);
        } else {
            update_health(&connection, &dropped, &write_failures);
        }
        if let Some(ack) = shutdown {
            if let Err(error) =
                run_maintenance(&connection, now_millis(), &dropped, &write_failures)
            {
                write_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(error = %error, "telemetry retention maintenance failed");
            }
            let _ = ack.send(());
            break;
        }
        let now = now_millis();
        if now.saturating_sub(last_maintenance) >= MAINTENANCE_INTERVAL_MS {
            if let Err(error) = run_maintenance(&connection, now, &dropped, &write_failures) {
                write_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(error = %error, "telemetry retention maintenance failed");
            }
            last_maintenance = now;
        }
    }
}

fn drain_pending(
    receiver: &Receiver<TelemetryMessage>,
    pending: &mut Vec<TelemetrySample>,
) -> Option<mpsc::Sender<()>> {
    while pending.len() < BATCH_LIMIT {
        match receiver.try_recv() {
            Ok(TelemetryMessage::Sample(sample)) => pending.push(sample),
            Ok(TelemetryMessage::Shutdown(ack)) => return Some(ack),
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
        }
    }
    None
}

fn flush_batch(
    connection: &mut Connection,
    pending: &mut Vec<TelemetrySample>,
    dropped: &AtomicU64,
    write_failures: &AtomicU64,
) {
    if pending.is_empty() {
        update_health(connection, dropped, write_failures);
        return;
    }
    let transaction = match connection.transaction() {
        Ok(transaction) => transaction,
        Err(error) => {
            write_failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %error, samples = pending.len(), "telemetry batch transaction failed");
            pending.clear();
            return;
        }
    };
    let mut failed = false;
    for sample in pending.drain(..) {
        let result = match sample {
            TelemetrySample::Api(sample) => insert_api_sample(&transaction, &sample),
            TelemetrySample::Event(sample) => insert_event_sample(&transaction, &sample),
            TelemetrySample::Workflow(sample) => insert_workflow_sample(&transaction, &sample),
            TelemetrySample::Runtime(sample) => insert_runtime_sample(&transaction, &sample),
        };
        if let Err(error) = result {
            write_failures.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(error = %error, "telemetry sample persistence failed");
            failed = true;
            break;
        }
    }
    if failed {
        let _ = transaction.rollback();
        return;
    }
    if let Err(error) = update_health_transaction(&transaction, dropped, write_failures) {
        write_failures.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(error = %error, "telemetry health update failed");
    }
    if let Err(error) = transaction.commit() {
        write_failures.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(error = %error, "telemetry batch commit failed");
    }
}

fn insert_api_sample(
    transaction: &Transaction<'_>,
    sample: &ApiAttemptTelemetry,
) -> rusqlite::Result<()> {
    transaction.execute(
        "INSERT INTO api_request_attempt(\
            observed_at_ms, local_request_id, server_request_id, method, path, route_key,\
            rate_limit_bucket, attempt, status_code, outcome, response_bytes, rate_limit_wait_ms,\
            request_prepare_ms, time_to_headers_ms, metadata_ms, body_read_ms, decode_ms, elapsed_ms,\
            rate_limit_limit, rate_limit_remaining, rate_limit_reset_epoch_seconds, retry_after_ms\
         ) VALUES (\
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22\
         )",
        params![
            sample.observed_at_ms,
            sample.local_request_id,
            sample.server_request_id,
            sample.method,
            sample.path,
            sample.route_key,
            sample.rate_limit_bucket,
            sample.attempt,
            sample.status_code,
            sample.outcome.as_str(),
            sqlite_optional_u64(sample.response_bytes),
            sqlite_u64(sample.timings.rate_limit_wait_ms),
            sqlite_optional_u64(sample.timings.request_prepare_ms),
            sqlite_optional_u64(sample.timings.time_to_headers_ms),
            sqlite_optional_u64(sample.timings.metadata_ms),
            sqlite_optional_u64(sample.timings.body_read_ms),
            sqlite_optional_u64(sample.timings.decode_ms),
            sqlite_u64(sample.timings.elapsed_ms),
            sample.rate_limit.limit,
            sample.rate_limit.remaining,
            sqlite_optional_u64(sample.rate_limit.reset_epoch_seconds),
            sqlite_optional_u64(sample.rate_limit.retry_after_ms),
        ],
    )?;

    for resolution_seconds in RESOLUTIONS_SECONDS {
        upsert_api_rollup(transaction, sample, resolution_seconds)?;
    }
    Ok(())
}

fn upsert_api_rollup(
    transaction: &Transaction<'_>,
    sample: &ApiAttemptTelemetry,
    resolution_seconds: i64,
) -> rusqlite::Result<()> {
    let resolution_ms = resolution_seconds.saturating_mul(1_000);
    let bucket_start_ms = sample.observed_at_ms.div_euclid(resolution_ms) * resolution_ms;
    let elapsed = sqlite_u64(sample.timings.elapsed_ms);
    let request_prepare = sqlite_u64(sample.timings.request_prepare_ms.unwrap_or_default());
    let headers = sqlite_u64(sample.timings.time_to_headers_ms.unwrap_or_default());
    let metadata = sqlite_u64(sample.timings.metadata_ms.unwrap_or_default());
    let body_read = sqlite_u64(sample.timings.body_read_ms.unwrap_or_default());
    let decode = sqlite_u64(sample.timings.decode_ms.unwrap_or_default());
    let response_bytes = sqlite_u64(sample.response_bytes.unwrap_or_default());
    let rate_limit_wait = sqlite_u64(sample.timings.rate_limit_wait_ms);
    let status_code = sample.status_code.map(i64::from).unwrap_or(-1);
    transaction.execute(
        r#"INSERT INTO api_request_rollup(
            bucket_start_ms, resolution_seconds, method, route_key, rate_limit_bucket, status_code, outcome,
            request_count, logical_request_count, retry_attempt_count, elapsed_sum_ms, elapsed_max_ms,
            request_prepare_sum_ms, request_prepare_count,
            time_to_headers_sum_ms, time_to_headers_count,
            metadata_sum_ms, metadata_count, body_read_sum_ms, body_read_count,
            decode_sum_ms, decode_count, rate_limit_wait_sum_ms,
            response_bytes_sum, response_bytes_count,
            elapsed_le_25_count, elapsed_le_50_count, elapsed_le_100_count, elapsed_le_200_count,
            elapsed_le_350_count, elapsed_le_500_count, elapsed_le_750_count, elapsed_le_1000_count,
            elapsed_le_1500_count, elapsed_le_2500_count, elapsed_le_5000_count, elapsed_le_10000_count,
            elapsed_le_30000_count, rate_limit_limit_last, rate_limit_remaining_last,
            rate_limit_reset_epoch_seconds_last
        ) VALUES (
            ?1, ?2, ?3, ?4, ?5, ?6, ?7,
            1, ?8, ?9, ?10, ?10,
            ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23,
            ?24, ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34, ?35, ?36,
            ?37, ?38, ?39
        )
        ON CONFLICT(bucket_start_ms, resolution_seconds, method, route_key, rate_limit_bucket, status_code, outcome)
        DO UPDATE SET
            request_count = request_count + 1,
            logical_request_count = logical_request_count + excluded.logical_request_count,
            retry_attempt_count = retry_attempt_count + excluded.retry_attempt_count,
            elapsed_sum_ms = elapsed_sum_ms + excluded.elapsed_sum_ms,
            elapsed_max_ms = MAX(elapsed_max_ms, excluded.elapsed_max_ms),
            request_prepare_sum_ms = request_prepare_sum_ms + excluded.request_prepare_sum_ms,
            request_prepare_count = request_prepare_count + excluded.request_prepare_count,
            time_to_headers_sum_ms = time_to_headers_sum_ms + excluded.time_to_headers_sum_ms,
            time_to_headers_count = time_to_headers_count + excluded.time_to_headers_count,
            metadata_sum_ms = metadata_sum_ms + excluded.metadata_sum_ms,
            metadata_count = metadata_count + excluded.metadata_count,
            body_read_sum_ms = body_read_sum_ms + excluded.body_read_sum_ms,
            body_read_count = body_read_count + excluded.body_read_count,
            decode_sum_ms = decode_sum_ms + excluded.decode_sum_ms,
            decode_count = decode_count + excluded.decode_count,
            rate_limit_wait_sum_ms = rate_limit_wait_sum_ms + excluded.rate_limit_wait_sum_ms,
            response_bytes_sum = response_bytes_sum + excluded.response_bytes_sum,
            response_bytes_count = response_bytes_count + excluded.response_bytes_count,
            elapsed_le_25_count = elapsed_le_25_count + excluded.elapsed_le_25_count,
            elapsed_le_50_count = elapsed_le_50_count + excluded.elapsed_le_50_count,
            elapsed_le_100_count = elapsed_le_100_count + excluded.elapsed_le_100_count,
            elapsed_le_200_count = elapsed_le_200_count + excluded.elapsed_le_200_count,
            elapsed_le_350_count = elapsed_le_350_count + excluded.elapsed_le_350_count,
            elapsed_le_500_count = elapsed_le_500_count + excluded.elapsed_le_500_count,
            elapsed_le_750_count = elapsed_le_750_count + excluded.elapsed_le_750_count,
            elapsed_le_1000_count = elapsed_le_1000_count + excluded.elapsed_le_1000_count,
            elapsed_le_1500_count = elapsed_le_1500_count + excluded.elapsed_le_1500_count,
            elapsed_le_2500_count = elapsed_le_2500_count + excluded.elapsed_le_2500_count,
            elapsed_le_5000_count = elapsed_le_5000_count + excluded.elapsed_le_5000_count,
            elapsed_le_10000_count = elapsed_le_10000_count + excluded.elapsed_le_10000_count,
            elapsed_le_30000_count = elapsed_le_30000_count + excluded.elapsed_le_30000_count,
            rate_limit_limit_last = COALESCE(excluded.rate_limit_limit_last, rate_limit_limit_last),
            rate_limit_remaining_last = COALESCE(excluded.rate_limit_remaining_last, rate_limit_remaining_last),
            rate_limit_reset_epoch_seconds_last = COALESCE(
                excluded.rate_limit_reset_epoch_seconds_last,
                rate_limit_reset_epoch_seconds_last
            )"#,
        params![
            bucket_start_ms,
            resolution_seconds,
            sample.method,
            sample.route_key,
            sample.rate_limit_bucket,
            status_code,
            sample.outcome.as_str(),
            if sample.attempt == 1 { 1_i64 } else { 0_i64 },
            if sample.attempt > 1 { 1_i64 } else { 0_i64 },
            elapsed,
            request_prepare,
            option_count(sample.timings.request_prepare_ms),
            headers,
            option_count(sample.timings.time_to_headers_ms),
            metadata,
            option_count(sample.timings.metadata_ms),
            body_read,
            option_count(sample.timings.body_read_ms),
            decode,
            option_count(sample.timings.decode_ms),
            rate_limit_wait,
            response_bytes,
            option_count(sample.response_bytes),
            le_count(elapsed, 25),
            le_count(elapsed, 50),
            le_count(elapsed, 100),
            le_count(elapsed, 200),
            le_count(elapsed, 350),
            le_count(elapsed, 500),
            le_count(elapsed, 750),
            le_count(elapsed, 1_000),
            le_count(elapsed, 1_500),
            le_count(elapsed, 2_500),
            le_count(elapsed, 5_000),
            le_count(elapsed, 10_000),
            le_count(elapsed, 30_000),
            sample.rate_limit.limit,
            sample.rate_limit.remaining,
            sqlite_optional_u64(sample.rate_limit.reset_epoch_seconds),
        ],
    )?;
    Ok(())
}

fn insert_event_sample(
    transaction: &Transaction<'_>,
    sample: &EventTelemetrySample,
) -> rusqlite::Result<()> {
    transaction.execute(
        "INSERT INTO event_telemetry_sample(\
            observed_at_ms, metric, outcome, event_name, event_count, page_count, duration_ms\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            sample.observed_at_ms,
            sample.metric,
            sample.outcome,
            sample.event_name,
            sqlite_u64(sample.event_count),
            sqlite_u64(sample.page_count),
            sqlite_optional_u64(sample.duration_ms),
        ],
    )?;
    for resolution_seconds in RESOLUTIONS_SECONDS {
        upsert_event_rollup(transaction, sample, resolution_seconds)?;
    }
    Ok(())
}

fn upsert_event_rollup(
    transaction: &Transaction<'_>,
    sample: &EventTelemetrySample,
    resolution_seconds: i64,
) -> rusqlite::Result<()> {
    let bucket_start_ms = bucket_start(sample.observed_at_ms, resolution_seconds);
    let duration = sqlite_u64(sample.duration_ms.unwrap_or_default());
    transaction.execute(
        r#"INSERT INTO event_telemetry_rollup(
            bucket_start_ms, resolution_seconds, metric, outcome, event_name,
            sample_count, event_count_sum, page_count_sum, duration_sum_ms, duration_count, duration_max_ms
        ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7, ?8, ?9, ?8)
        ON CONFLICT(bucket_start_ms, resolution_seconds, metric, outcome, event_name)
        DO UPDATE SET
            sample_count = sample_count + 1,
            event_count_sum = event_count_sum + excluded.event_count_sum,
            page_count_sum = page_count_sum + excluded.page_count_sum,
            duration_sum_ms = duration_sum_ms + excluded.duration_sum_ms,
            duration_count = duration_count + excluded.duration_count,
            duration_max_ms = MAX(duration_max_ms, excluded.duration_max_ms)"#,
        params![
            bucket_start_ms,
            resolution_seconds,
            sample.metric,
            sample.outcome,
            sample.event_name.as_deref().unwrap_or(""),
            sqlite_u64(sample.event_count),
            sqlite_u64(sample.page_count),
            duration,
            option_count(sample.duration_ms),
        ],
    )?;
    Ok(())
}

fn insert_workflow_sample(
    transaction: &Transaction<'_>,
    sample: &WorkflowTelemetrySample,
) -> rusqlite::Result<()> {
    transaction.execute(
        "INSERT INTO workflow_telemetry_sample(\
            observed_at_ms, workflow_id, workflow_kind, metric, outcome, detail, duration_ms\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            sample.observed_at_ms,
            sample.workflow_id,
            sample.workflow_kind,
            sample.metric,
            sample.outcome,
            sample.detail,
            sqlite_optional_u64(sample.duration_ms),
        ],
    )?;
    for resolution_seconds in RESOLUTIONS_SECONDS {
        upsert_workflow_rollup(transaction, sample, resolution_seconds)?;
    }
    Ok(())
}

fn upsert_workflow_rollup(
    transaction: &Transaction<'_>,
    sample: &WorkflowTelemetrySample,
    resolution_seconds: i64,
) -> rusqlite::Result<()> {
    let bucket_start_ms = bucket_start(sample.observed_at_ms, resolution_seconds);
    let duration = sqlite_u64(sample.duration_ms.unwrap_or_default());
    transaction.execute(
        r#"INSERT INTO workflow_telemetry_rollup(
            bucket_start_ms, resolution_seconds, workflow_kind, metric, outcome, detail,
            sample_count, duration_sum_ms, duration_count, duration_max_ms
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1, ?7, ?8, ?7)
        ON CONFLICT(bucket_start_ms, resolution_seconds, workflow_kind, metric, outcome, detail)
        DO UPDATE SET
            sample_count = sample_count + 1,
            duration_sum_ms = duration_sum_ms + excluded.duration_sum_ms,
            duration_count = duration_count + excluded.duration_count,
            duration_max_ms = MAX(duration_max_ms, excluded.duration_max_ms)"#,
        params![
            bucket_start_ms,
            resolution_seconds,
            sample.workflow_kind,
            sample.metric,
            sample.outcome,
            sample.detail.as_deref().unwrap_or(""),
            duration,
            option_count(sample.duration_ms),
        ],
    )?;
    Ok(())
}

fn insert_runtime_sample(
    transaction: &Transaction<'_>,
    sample: &RuntimeTelemetrySample,
) -> rusqlite::Result<()> {
    transaction.execute(
        "INSERT INTO runtime_telemetry_sample(\
            observed_at_ms, metric, series, value, duration_ms\
         ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            sample.observed_at_ms,
            sample.metric,
            sample.series,
            sample.value,
            sqlite_optional_u64(sample.duration_ms),
        ],
    )?;
    for resolution_seconds in RESOLUTIONS_SECONDS {
        upsert_runtime_rollup(transaction, sample, resolution_seconds)?;
    }
    Ok(())
}

fn upsert_runtime_rollup(
    transaction: &Transaction<'_>,
    sample: &RuntimeTelemetrySample,
    resolution_seconds: i64,
) -> rusqlite::Result<()> {
    let bucket_start_ms = bucket_start(sample.observed_at_ms, resolution_seconds);
    let duration = sqlite_u64(sample.duration_ms.unwrap_or_default());
    transaction.execute(
        r#"INSERT INTO runtime_telemetry_rollup(
            bucket_start_ms, resolution_seconds, metric, series,
            sample_count, value_sum, value_max, duration_sum_ms, duration_count, duration_max_ms
        ) VALUES (?1, ?2, ?3, ?4, 1, ?5, ?5, ?6, ?7, ?6)
        ON CONFLICT(bucket_start_ms, resolution_seconds, metric, series)
        DO UPDATE SET
            sample_count = sample_count + 1,
            value_sum = value_sum + excluded.value_sum,
            value_max = MAX(value_max, excluded.value_max),
            duration_sum_ms = duration_sum_ms + excluded.duration_sum_ms,
            duration_count = duration_count + excluded.duration_count,
            duration_max_ms = MAX(duration_max_ms, excluded.duration_max_ms)"#,
        params![
            bucket_start_ms,
            resolution_seconds,
            sample.metric,
            sample.series,
            sample.value,
            duration,
            option_count(sample.duration_ms),
        ],
    )?;
    Ok(())
}

fn run_maintenance(
    connection: &Connection,
    now: i64,
    dropped: &AtomicU64,
    write_failures: &AtomicU64,
) -> rusqlite::Result<()> {
    for statement in [
        "DELETE FROM api_request_attempt WHERE observed_at_ms < ?1",
        "DELETE FROM event_telemetry_sample WHERE observed_at_ms < ?1",
        "DELETE FROM workflow_telemetry_sample WHERE observed_at_ms < ?1",
        "DELETE FROM runtime_telemetry_sample WHERE observed_at_ms < ?1",
    ] {
        connection.execute(statement, [now.saturating_sub(RAW_RETENTION_MS)])?;
    }
    for statement in [
        "DELETE FROM api_request_rollup WHERE resolution_seconds = 60 AND bucket_start_ms < ?1",
        "DELETE FROM event_telemetry_rollup WHERE resolution_seconds = 60 AND bucket_start_ms < ?1",
        "DELETE FROM workflow_telemetry_rollup WHERE resolution_seconds = 60 AND bucket_start_ms < ?1",
        "DELETE FROM runtime_telemetry_rollup WHERE resolution_seconds = 60 AND bucket_start_ms < ?1",
    ] {
        connection.execute(statement, [now.saturating_sub(ONE_MINUTE_RETENTION_MS)])?;
    }
    for statement in [
        "DELETE FROM api_request_rollup WHERE resolution_seconds = 600 AND bucket_start_ms < ?1",
        "DELETE FROM event_telemetry_rollup WHERE resolution_seconds = 600 AND bucket_start_ms < ?1",
        "DELETE FROM workflow_telemetry_rollup WHERE resolution_seconds = 600 AND bucket_start_ms < ?1",
        "DELETE FROM runtime_telemetry_rollup WHERE resolution_seconds = 600 AND bucket_start_ms < ?1",
    ] {
        connection.execute(statement, [now.saturating_sub(TEN_MINUTE_RETENTION_MS)])?;
    }
    for statement in [
        "DELETE FROM api_request_rollup WHERE resolution_seconds = 3600 AND bucket_start_ms < ?1",
        "DELETE FROM event_telemetry_rollup WHERE resolution_seconds = 3600 AND bucket_start_ms < ?1",
        "DELETE FROM workflow_telemetry_rollup WHERE resolution_seconds = 3600 AND bucket_start_ms < ?1",
        "DELETE FROM runtime_telemetry_rollup WHERE resolution_seconds = 3600 AND bucket_start_ms < ?1",
    ] {
        connection.execute(statement, [now.saturating_sub(HOURLY_RETENTION_MS)])?;
    }
    connection.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('last_maintenance_ms', ?1) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [now.to_string()],
    )?;
    update_health(connection, dropped, write_failures);
    connection.execute_batch("PRAGMA wal_checkpoint(PASSIVE); PRAGMA incremental_vacuum(2000);")?;
    Ok(())
}

fn update_health(connection: &Connection, dropped: &AtomicU64, write_failures: &AtomicU64) {
    let result = connection.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('dropped_samples', ?1) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [dropped.load(Ordering::Relaxed).to_string()],
    );
    if let Err(error) = result {
        write_failures.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(error = %error, "telemetry health update failed");
        return;
    }
    if let Err(error) = connection.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('write_failures', ?1) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [write_failures.load(Ordering::Relaxed).to_string()],
    ) {
        write_failures.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(error = %error, "telemetry health update failed");
    }
}

fn update_health_transaction(
    transaction: &Transaction<'_>,
    dropped: &AtomicU64,
    write_failures: &AtomicU64,
) -> rusqlite::Result<()> {
    transaction.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('dropped_samples', ?1) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [dropped.load(Ordering::Relaxed).to_string()],
    )?;
    transaction.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('write_failures', ?1) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [write_failures.load(Ordering::Relaxed).to_string()],
    )?;
    Ok(())
}

fn bucket_start(observed_at_ms: i64, resolution_seconds: i64) -> i64 {
    let resolution_ms = resolution_seconds.saturating_mul(1_000);
    observed_at_ms.div_euclid(resolution_ms) * resolution_ms
}

fn option_count<T>(value: Option<T>) -> i64 {
    i64::from(value.is_some())
}

fn le_count(value: i64, upper: i64) -> i64 {
    i64::from(value <= upper)
}

fn sqlite_u64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn sqlite_optional_u64(value: Option<u64>) -> Option<i64> {
    value.map(sqlite_u64)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use replicant_client::{
        managed::{EventTelemetrySample, EventTelemetrySink},
        raw::{
            ApiAttemptOutcome, ApiAttemptTelemetry, ApiAttemptTimings, ApiRateLimitTelemetry,
            ApiTelemetrySink,
        },
    };
    use replicant_workflow::{WorkflowTelemetrySample, WorkflowTelemetrySink};
    use rusqlite::Connection;

    use super::{
        RuntimeTelemetrySample, RuntimeTelemetrySink, TelemetryService,
        default_telemetry_database_path,
    };

    fn temp_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("replicant-{name}-{}.sqlite", std::process::id()))
    }

    #[test]
    fn telemetry_path_isolated_from_managed_history() {
        assert_eq!(
            default_telemetry_database_path("replicant-client.sqlite"),
            std::path::PathBuf::from("replicant-telemetry.sqlite")
        );
        assert_eq!(
            default_telemetry_database_path("private/state.sqlite"),
            std::path::PathBuf::from("private/state.telemetry.sqlite")
        );
    }

    #[test]
    fn request_samples_are_persisted_and_rolled_up() {
        let path = temp_path("telemetry");
        let service = TelemetryService::start(&path).expect("start telemetry");
        let sink: Arc<dyn ApiTelemetrySink> = service.api_sink();
        sink.record(ApiAttemptTelemetry {
            observed_at_ms: 1_800_000_000_000,
            local_request_id: "local-1".to_owned(),
            server_request_id: Some("server-1".to_owned()),
            method: "GET".to_owned(),
            path: "v1/devices/D1".to_owned(),
            route_key: "v1/devices/{device}".to_owned(),
            rate_limit_bucket: "read".to_owned(),
            attempt: 2,
            status_code: Some(429),
            outcome: ApiAttemptOutcome::HttpError,
            response_bytes: Some(123),
            timings: ApiAttemptTimings {
                rate_limit_wait_ms: 10,
                request_prepare_ms: Some(1),
                time_to_headers_ms: Some(100),
                metadata_ms: Some(1),
                body_read_ms: Some(2),
                decode_ms: None,
                elapsed_ms: 114,
            },
            rate_limit: ApiRateLimitTelemetry {
                limit: Some(120),
                remaining: Some(0),
                reset_epoch_seconds: Some(1_800_000_060),
                retry_after_ms: Some(500),
            },
        });
        let event_sink: Arc<dyn EventTelemetrySink> = service.event_sink();
        event_sink.record(EventTelemetrySample {
            observed_at_ms: 1_800_000_000_100,
            metric: "sse_disconnect",
            outcome: "stream_error".to_owned(),
            event_name: None,
            event_count: 12,
            page_count: 0,
            duration_ms: Some(30_000),
        });
        let workflow_sink: Arc<dyn WorkflowTelemetrySink> = service.workflow_sink();
        workflow_sink.record(WorkflowTelemetrySample {
            observed_at_ms: 1_800_000_000_200,
            workflow_id: "workflow-1".to_owned(),
            workflow_kind: "scan.tour".to_owned(),
            metric: "executor_finished",
            outcome: "waiting".to_owned(),
            detail: Some("surveying".to_owned()),
            duration_ms: Some(250),
        });
        let runtime_sink: Arc<dyn RuntimeTelemetrySink> = service.runtime_sink();
        runtime_sink.record(RuntimeTelemetrySample {
            observed_at_ms: 1_800_000_000_300,
            metric: "watcher_lag",
            series: "trigger_events".to_owned(),
            value: 1,
            duration_ms: None,
        });
        service.shutdown().expect("shutdown telemetry");

        let connection = Connection::open(&path).expect("open telemetry db");
        let raw_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM api_request_attempt", [], |row| {
                row.get(0)
            })
            .expect("raw count");
        let rollup_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM api_request_rollup", [], |row| {
                row.get(0)
            })
            .expect("rollup count");
        let retry_count: i64 = connection
            .query_row(
                "SELECT SUM(retry_attempt_count) FROM api_request_rollup WHERE resolution_seconds = 60",
                [],
                |row| row.get(0),
            )
            .expect("retry count");
        let event_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM event_telemetry_sample", [], |row| {
                row.get(0)
            })
            .expect("event telemetry count");
        let workflow_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM workflow_telemetry_sample",
                [],
                |row| row.get(0),
            )
            .expect("workflow telemetry count");
        let runtime_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM runtime_telemetry_sample", [], |row| {
                row.get(0)
            })
            .expect("runtime telemetry count");
        let logical_count: i64 = connection
            .query_row(
                "SELECT SUM(logical_request_count) FROM api_request_rollup WHERE resolution_seconds = 60",
                [],
                |row| row.get(0),
            )
            .expect("logical request count");
        assert_eq!(raw_count, 1);
        assert_eq!(rollup_count, 4);
        assert_eq!(retry_count, 1);
        assert_eq!(logical_count, 0);
        assert_eq!(event_count, 1);
        assert_eq!(workflow_count, 1);
        assert_eq!(runtime_count, 1);

        drop(connection);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
