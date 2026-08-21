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

use replicant_client::raw::{ApiAttemptTelemetry, ApiTelemetrySink};
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
    retry_attempt_count INTEGER NOT NULL,
    elapsed_sum_ms INTEGER NOT NULL,
    elapsed_max_ms INTEGER NOT NULL,
    time_to_headers_sum_ms INTEGER NOT NULL,
    time_to_headers_count INTEGER NOT NULL,
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

#[derive(Clone)]
struct ChannelApiTelemetrySink {
    sender: SyncSender<TelemetryMessage>,
    dropped: Arc<AtomicU64>,
}

impl ApiTelemetrySink for ChannelApiTelemetrySink {
    fn record(&self, sample: ApiAttemptTelemetry) {
        match self.sender.try_send(TelemetryMessage::Api(sample)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

enum TelemetryMessage {
    Api(ApiAttemptTelemetry),
    Shutdown(mpsc::Sender<()>),
}

/// Running telemetry writer and lifecycle handle.
///
/// API request paths only enqueue into a bounded channel. SQLite I/O and
/// rollup maintenance happen on the dedicated writer thread.
pub struct TelemetryService {
    sender: SyncSender<TelemetryMessage>,
    sink: Arc<ChannelApiTelemetrySink>,
    dropped: Arc<AtomicU64>,
    writer: Option<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for TelemetryService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TelemetryService")
            .field("dropped_samples", &self.dropped_samples())
            .finish_non_exhaustive()
    }
}

impl TelemetryService {
    /// Opens/migrates a telemetry database and starts its bounded writer.
    pub fn start(path: impl AsRef<Path>) -> Result<Self, TelemetryError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)?;
        }
        let connection = open_database(&path)?;
        let initial_dropped = connection.query_row(
            "SELECT COALESCE((SELECT CAST(value AS INTEGER) FROM telemetry_meta WHERE key = 'dropped_samples'), 0)",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        drop(connection);

        let (sender, receiver) = mpsc::sync_channel(CHANNEL_CAPACITY);
        let dropped = Arc::new(AtomicU64::new(initial_dropped));
        let sink = Arc::new(ChannelApiTelemetrySink {
            sender: sender.clone(),
            dropped: dropped.clone(),
        });
        let worker_dropped = dropped.clone();
        let writer = thread::Builder::new()
            .name("replicant-telemetry".to_owned())
            .spawn(move || writer_main(&path, receiver, worker_dropped))?;

        Ok(Self {
            sender,
            sink,
            dropped,
            writer: Some(writer),
        })
    }

    /// Returns the sink installed into the managed/raw HTTP client.
    #[must_use]
    pub fn api_sink(&self) -> Arc<dyn ApiTelemetrySink> {
        self.sink.clone()
    }

    /// Number of samples dropped because the bounded queue was unavailable.
    #[must_use]
    pub fn dropped_samples(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
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
    connection.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('schema_version', '1') \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [],
    )?;
    Ok(connection)
}

fn writer_main(path: &Path, receiver: Receiver<TelemetryMessage>, dropped: Arc<AtomicU64>) {
    let Ok(mut connection) = open_database(path) else {
        tracing::error!(path = %path.display(), "telemetry writer could not reopen database");
        return;
    };
    let mut last_maintenance = 0_i64;
    let mut pending = Vec::with_capacity(BATCH_LIMIT);

    loop {
        match receiver.recv_timeout(BATCH_WAIT) {
            Ok(TelemetryMessage::Api(sample)) => pending.push(sample),
            Ok(TelemetryMessage::Shutdown(ack)) => {
                flush_batch(&mut connection, &mut pending, dropped.load(Ordering::Relaxed));
                run_maintenance(&connection, now_millis(), dropped.load(Ordering::Relaxed));
                let _ = ack.send(());
                break;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => {
                flush_batch(&mut connection, &mut pending, dropped.load(Ordering::Relaxed));
                break;
            }
        }

        let shutdown = drain_pending(&receiver, &mut pending);
        if !pending.is_empty() {
            flush_batch(&mut connection, &mut pending, dropped.load(Ordering::Relaxed));
        }
        if let Some(ack) = shutdown {
            run_maintenance(&connection, now_millis(), dropped.load(Ordering::Relaxed));
            let _ = ack.send(());
            break;
        }
        let now = now_millis();
        if now.saturating_sub(last_maintenance) >= MAINTENANCE_INTERVAL_MS {
            run_maintenance(&connection, now, dropped.load(Ordering::Relaxed));
            last_maintenance = now;
        }
    }
}

fn drain_pending(
    receiver: &Receiver<TelemetryMessage>,
    pending: &mut Vec<ApiAttemptTelemetry>,
) -> Option<mpsc::Sender<()>> {
    while pending.len() < BATCH_LIMIT {
        match receiver.try_recv() {
            Ok(TelemetryMessage::Api(sample)) => pending.push(sample),
            Ok(TelemetryMessage::Shutdown(ack)) => return Some(ack),
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
        }
    }
    None
}

fn flush_batch(connection: &mut Connection, pending: &mut Vec<ApiAttemptTelemetry>, dropped: u64) {
    if pending.is_empty() {
        update_health(connection, dropped);
        return;
    }
    let transaction = match connection.transaction() {
        Ok(transaction) => transaction,
        Err(error) => {
            tracing::warn!(error = %error, samples = pending.len(), "telemetry batch transaction failed");
            pending.clear();
            return;
        }
    };
    let mut failed = false;
    for sample in pending.drain(..) {
        if let Err(error) = insert_api_sample(&transaction, &sample) {
            tracing::warn!(error = %error, "telemetry sample persistence failed");
            failed = true;
            break;
        }
    }
    if failed {
        let _ = transaction.rollback();
        return;
    }
    if let Err(error) = transaction.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('dropped_samples', ?1) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [dropped.to_string()],
    ) {
        tracing::warn!(error = %error, "telemetry health update failed");
    }
    if let Err(error) = transaction.commit() {
        tracing::warn!(error = %error, "telemetry batch commit failed");
    }
}

fn insert_api_sample(transaction: &Transaction<'_>, sample: &ApiAttemptTelemetry) -> rusqlite::Result<()> {
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
            sample.response_bytes,
            sample.timings.rate_limit_wait_ms,
            sample.timings.request_prepare_ms,
            sample.timings.time_to_headers_ms,
            sample.timings.metadata_ms,
            sample.timings.body_read_ms,
            sample.timings.decode_ms,
            sample.timings.elapsed_ms,
            sample.rate_limit.limit,
            sample.rate_limit.remaining,
            sample.rate_limit.reset_epoch_seconds,
            sample.rate_limit.retry_after_ms,
        ],
    )?;

    for resolution_seconds in RESOLUTIONS_SECONDS {
        upsert_rollup(transaction, sample, resolution_seconds)?;
    }
    Ok(())
}

fn upsert_rollup(
    transaction: &Transaction<'_>,
    sample: &ApiAttemptTelemetry,
    resolution_seconds: i64,
) -> rusqlite::Result<()> {
    let resolution_ms = resolution_seconds.saturating_mul(1_000);
    let bucket_start_ms = sample.observed_at_ms.div_euclid(resolution_ms) * resolution_ms;
    let elapsed = sample.timings.elapsed_ms;
    let headers = sample.timings.time_to_headers_ms.unwrap_or_default();
    let response_bytes = sample.response_bytes.unwrap_or_default();
    let status_code = sample.status_code.map(i64::from).unwrap_or(-1);
    transaction.execute(
        "INSERT INTO api_request_rollup(\
            bucket_start_ms, resolution_seconds, method, route_key, rate_limit_bucket, status_code, outcome,\
            request_count, retry_attempt_count, elapsed_sum_ms, elapsed_max_ms,\
            time_to_headers_sum_ms, time_to_headers_count, rate_limit_wait_sum_ms,\
            response_bytes_sum, response_bytes_count,\
            elapsed_le_25_count, elapsed_le_50_count, elapsed_le_100_count, elapsed_le_200_count,\
            elapsed_le_350_count, elapsed_le_500_count, elapsed_le_750_count, elapsed_le_1000_count,\
            elapsed_le_1500_count, elapsed_le_2500_count, elapsed_le_5000_count, elapsed_le_10000_count,\
            elapsed_le_30000_count\
         ) VALUES (\
            ?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, ?9, ?9, ?10, ?11, ?12, ?13, ?14,\
            ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24, ?25, ?26, ?27\
         )\
         ON CONFLICT(bucket_start_ms, resolution_seconds, method, route_key, rate_limit_bucket, status_code, outcome)\
         DO UPDATE SET\
            request_count = request_count + 1,\
            retry_attempt_count = retry_attempt_count + excluded.retry_attempt_count,\
            elapsed_sum_ms = elapsed_sum_ms + excluded.elapsed_sum_ms,\
            elapsed_max_ms = MAX(elapsed_max_ms, excluded.elapsed_max_ms),\
            time_to_headers_sum_ms = time_to_headers_sum_ms + excluded.time_to_headers_sum_ms,\
            time_to_headers_count = time_to_headers_count + excluded.time_to_headers_count,\
            rate_limit_wait_sum_ms = rate_limit_wait_sum_ms + excluded.rate_limit_wait_sum_ms,\
            response_bytes_sum = response_bytes_sum + excluded.response_bytes_sum,\
            response_bytes_count = response_bytes_count + excluded.response_bytes_count,\
            elapsed_le_25_count = elapsed_le_25_count + excluded.elapsed_le_25_count,\
            elapsed_le_50_count = elapsed_le_50_count + excluded.elapsed_le_50_count,\
            elapsed_le_100_count = elapsed_le_100_count + excluded.elapsed_le_100_count,\
            elapsed_le_200_count = elapsed_le_200_count + excluded.elapsed_le_200_count,\
            elapsed_le_350_count = elapsed_le_350_count + excluded.elapsed_le_350_count,\
            elapsed_le_500_count = elapsed_le_500_count + excluded.elapsed_le_500_count,\
            elapsed_le_750_count = elapsed_le_750_count + excluded.elapsed_le_750_count,\
            elapsed_le_1000_count = elapsed_le_1000_count + excluded.elapsed_le_1000_count,\
            elapsed_le_1500_count = elapsed_le_1500_count + excluded.elapsed_le_1500_count,\
            elapsed_le_2500_count = elapsed_le_2500_count + excluded.elapsed_le_2500_count,\
            elapsed_le_5000_count = elapsed_le_5000_count + excluded.elapsed_le_5000_count,\
            elapsed_le_10000_count = elapsed_le_10000_count + excluded.elapsed_le_10000_count,\
            elapsed_le_30000_count = elapsed_le_30000_count + excluded.elapsed_le_30000_count",
        params![
            bucket_start_ms,
            resolution_seconds,
            sample.method,
            sample.route_key,
            sample.rate_limit_bucket,
            status_code,
            sample.outcome.as_str(),
            if sample.attempt > 1 { 1_i64 } else { 0_i64 },
            elapsed,
            headers,
            if sample.timings.time_to_headers_ms.is_some() { 1_i64 } else { 0_i64 },
            sample.timings.rate_limit_wait_ms,
            response_bytes,
            if sample.response_bytes.is_some() { 1_i64 } else { 0_i64 },
            if elapsed <= 25 { 1_i64 } else { 0_i64 },
            if elapsed <= 50 { 1_i64 } else { 0_i64 },
            if elapsed <= 100 { 1_i64 } else { 0_i64 },
            if elapsed <= 200 { 1_i64 } else { 0_i64 },
            if elapsed <= 350 { 1_i64 } else { 0_i64 },
            if elapsed <= 500 { 1_i64 } else { 0_i64 },
            if elapsed <= 750 { 1_i64 } else { 0_i64 },
            if elapsed <= 1_000 { 1_i64 } else { 0_i64 },
            if elapsed <= 1_500 { 1_i64 } else { 0_i64 },
            if elapsed <= 2_500 { 1_i64 } else { 0_i64 },
            if elapsed <= 5_000 { 1_i64 } else { 0_i64 },
            if elapsed <= 10_000 { 1_i64 } else { 0_i64 },
            if elapsed <= 30_000 { 1_i64 } else { 0_i64 },
        ],
    )?;
    Ok(())
}

fn run_maintenance(connection: &Connection, now: i64, dropped: u64) {
    let maintenance = || -> rusqlite::Result<()> {
        connection.execute(
            "DELETE FROM api_request_attempt WHERE observed_at_ms < ?1",
            [now.saturating_sub(RAW_RETENTION_MS)],
        )?;
        connection.execute(
            "DELETE FROM api_request_rollup WHERE resolution_seconds = 60 AND bucket_start_ms < ?1",
            [now.saturating_sub(ONE_MINUTE_RETENTION_MS)],
        )?;
        connection.execute(
            "DELETE FROM api_request_rollup WHERE resolution_seconds = 600 AND bucket_start_ms < ?1",
            [now.saturating_sub(TEN_MINUTE_RETENTION_MS)],
        )?;
        connection.execute(
            "DELETE FROM api_request_rollup WHERE resolution_seconds = 3600 AND bucket_start_ms < ?1",
            [now.saturating_sub(HOURLY_RETENTION_MS)],
        )?;
        connection.execute(
            "INSERT INTO telemetry_meta(key, value) VALUES ('last_maintenance_ms', ?1) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [now.to_string()],
        )?;
        connection.execute(
            "INSERT INTO telemetry_meta(key, value) VALUES ('dropped_samples', ?1) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [dropped.to_string()],
        )?;
        connection.execute_batch("PRAGMA wal_checkpoint(PASSIVE); PRAGMA incremental_vacuum(2000);")?;
        Ok(())
    };
    if let Err(error) = maintenance() {
        tracing::warn!(error = %error, "telemetry retention maintenance failed");
    }
}

fn update_health(connection: &Connection, dropped: u64) {
    if let Err(error) = connection.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('dropped_samples', ?1) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [dropped.to_string()],
    ) {
        tracing::warn!(error = %error, "telemetry health update failed");
    }
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

    use replicant_client::raw::{
        ApiAttemptOutcome, ApiAttemptTelemetry, ApiAttemptTimings, ApiRateLimitTelemetry,
        ApiTelemetrySink,
    };
    use rusqlite::Connection;

    use super::{TelemetryService, default_telemetry_database_path};

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
        service.shutdown().expect("shutdown telemetry");

        let connection = Connection::open(&path).expect("open telemetry db");
        let raw_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM api_request_attempt", [], |row| row.get(0))
            .expect("raw count");
        let rollup_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM api_request_rollup", [], |row| row.get(0))
            .expect("rollup count");
        let retry_count: i64 = connection
            .query_row(
                "SELECT SUM(retry_attempt_count) FROM api_request_rollup WHERE resolution_seconds = 60",
                [],
                |row| row.get(0),
            )
            .expect("retry count");
        assert_eq!(raw_count, 1);
        assert_eq!(rollup_count, 4);
        assert_eq!(retry_count, 1);

        drop(connection);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
    }
}
