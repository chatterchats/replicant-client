//! Historical in-game/empire telemetry derived from managed state and event history.
//!
//! The projector deliberately keeps historical inference separate from the
//! authoritative managed SDK state. Event-derived deltas are persisted into
//! the isolated telemetry database for Grafana, while periodic managed-state
//! snapshots reconcile gaps that cannot be reconstructed from the event log
//! (notably AMI digests that omit resource quantities).

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use replicant_client::domain::{Blueprint, Inventory, InventoryOwner, Observation};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Transaction, params};
use serde_json::Value;
use thiserror::Error;

const EVENT_POLL_INTERVAL: Duration = Duration::from_secs(15);
const SNAPSHOT_INTERVAL_MS: i64 = 10 * 60 * 1_000;
const MAINTENANCE_INTERVAL_MS: i64 = 6 * 60 * 60 * 1_000;
const DAY_MS: i64 = 24 * 60 * 60 * 1_000;
const TEN_MINUTE_RETENTION_MS: i64 = 30 * DAY_MS;
const HOURLY_RETENTION_MS: i64 = 90 * DAY_MS;
const PROJECTOR_VERSION: i64 = 4;
const SNAPSHOT_RESOLUTION_SECONDS: i64 = 600;
const ACTIVE_FTL_RELAY: &str = "active_ftl_relay";
const ACTIVE_DEEP_SPACE_RELAY_STATION: &str = "active_deep_space_relay_station";
const ACTIVE_FTL_BEACON: &str = "active_ftl_beacon";

const EMPIRE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS telemetry_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS empire_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS empire_blueprint_recipe_history (
    observed_at_ms INTEGER NOT NULL,
    device_type TEXT NOT NULL,
    resources_json TEXT NOT NULL,
    components_json TEXT NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (device_type, observed_at_ms)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_empire_blueprint_time
    ON empire_blueprint_recipe_history(observed_at_ms);

CREATE TABLE IF NOT EXISTS empire_device_identity (
    device_code TEXT PRIMARY KEY,
    device_type TEXT NOT NULL,
    first_seen_ms INTEGER NOT NULL,
    last_seen_ms INTEGER NOT NULL,
    source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS empire_print_job (
    start_event_id TEXT PRIMARY KEY,
    printer_code TEXT NOT NULL,
    device_type TEXT NOT NULL,
    location TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    resources_json TEXT NOT NULL,
    completed_event_id TEXT,
    completed_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_empire_print_job_open
    ON empire_print_job(printer_code, device_type, started_at_ms)
    WHERE completed_event_id IS NULL;

CREATE TABLE IF NOT EXISTS empire_resource_delta (
    event_id TEXT NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    location TEXT NOT NULL,
    resource TEXT NOT NULL,
    physical_delta INTEGER NOT NULL,
    reserved_delta INTEGER NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (event_id, location, resource, reason)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_empire_resource_delta_time
    ON empire_resource_delta(occurred_at_ms);
CREATE INDEX IF NOT EXISTS idx_empire_resource_delta_resource_time
    ON empire_resource_delta(resource, occurred_at_ms);
CREATE INDEX IF NOT EXISTS idx_empire_resource_delta_location_time
    ON empire_resource_delta(location, occurred_at_ms);

CREATE TABLE IF NOT EXISTS empire_resource_baseline (
    as_of_ms INTEGER NOT NULL,
    location TEXT NOT NULL,
    resource TEXT NOT NULL,
    physical_quantity INTEGER NOT NULL,
    reserved_quantity INTEGER NOT NULL,
    confidence TEXT NOT NULL,
    PRIMARY KEY (location, resource)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS empire_resource_snapshot (
    observed_at_ms INTEGER NOT NULL,
    resolution_seconds INTEGER NOT NULL,
    location TEXT NOT NULL,
    resource TEXT NOT NULL,
    reported_quantity INTEGER NOT NULL,
    reserved_quantity INTEGER NOT NULL,
    available_quantity INTEGER NOT NULL,
    PRIMARY KEY (observed_at_ms, resolution_seconds, location, resource)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_empire_resource_snapshot_resolution_time
    ON empire_resource_snapshot(resolution_seconds, observed_at_ms);

CREATE TABLE IF NOT EXISTS empire_device_delta (
    event_id TEXT NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    device_type TEXT NOT NULL,
    device_code TEXT NOT NULL,
    delta INTEGER NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (event_id, device_type, device_code, reason)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_empire_device_delta_time
    ON empire_device_delta(occurred_at_ms);
CREATE INDEX IF NOT EXISTS idx_empire_device_delta_type_time
    ON empire_device_delta(device_type, occurred_at_ms);

CREATE TABLE IF NOT EXISTS empire_device_baseline (
    as_of_ms INTEGER NOT NULL,
    device_type TEXT PRIMARY KEY,
    total_count INTEGER NOT NULL,
    confidence TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS empire_device_snapshot (
    observed_at_ms INTEGER NOT NULL,
    resolution_seconds INTEGER NOT NULL,
    device_type TEXT NOT NULL,
    status TEXT NOT NULL,
    device_count INTEGER NOT NULL,
    PRIMARY KEY (observed_at_ms, resolution_seconds, device_type, status)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_empire_device_snapshot_resolution_time
    ON empire_device_snapshot(resolution_seconds, observed_at_ms);

CREATE TABLE IF NOT EXISTS empire_infrastructure_delta (
    event_id TEXT NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    delta INTEGER NOT NULL,
    reason TEXT NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (event_id, kind, reason)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_empire_infrastructure_delta_time
    ON empire_infrastructure_delta(occurred_at_ms);

CREATE TABLE IF NOT EXISTS empire_infrastructure_baseline (
    as_of_ms INTEGER NOT NULL,
    kind TEXT PRIMARY KEY,
    active_count INTEGER NOT NULL,
    confidence TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS empire_travel (
    departed_event_id TEXT PRIMARY KEY,
    departed_at_ms INTEGER NOT NULL,
    arrived_event_id TEXT,
    arrived_at_ms INTEGER,
    device_code TEXT NOT NULL,
    origin TEXT NOT NULL,
    destination TEXT NOT NULL,
    origin_system TEXT NOT NULL,
    destination_system TEXT NOT NULL,
    inter_system INTEGER NOT NULL,
    travel_type TEXT NOT NULL,
    distance_ly REAL,
    planned_duration_seconds INTEGER,
    actual_duration_ms INTEGER,
    attached_device_count INTEGER NOT NULL,
    source TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_empire_travel_departed
    ON empire_travel(departed_at_ms);
CREATE INDEX IF NOT EXISTS idx_empire_travel_route
    ON empire_travel(origin_system, destination_system, departed_at_ms);

CREATE TABLE IF NOT EXISTS empire_activity (
    event_id TEXT NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    metric TEXT NOT NULL,
    series TEXT NOT NULL,
    value INTEGER NOT NULL,
    location TEXT,
    system TEXT,
    source TEXT NOT NULL,
    PRIMARY KEY (event_id, metric, series)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_empire_activity_time
    ON empire_activity(occurred_at_ms);
CREATE INDEX IF NOT EXISTS idx_empire_activity_metric_time
    ON empire_activity(metric, occurred_at_ms);

CREATE TABLE IF NOT EXISTS empire_projection_gap (
    event_id TEXT NOT NULL,
    occurred_at_ms INTEGER NOT NULL,
    kind TEXT NOT NULL,
    location TEXT,
    resource TEXT,
    detail TEXT NOT NULL,
    PRIMARY KEY (event_id, kind)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_empire_projection_gap_time
    ON empire_projection_gap(occurred_at_ms);
"#;

/// Errors opening, running, or closing the empire telemetry projector.
#[derive(Debug, Error)]
pub enum EmpireTelemetryError {
    /// SQLite state/history/telemetry storage could not be opened or updated.
    #[error("empire telemetry SQLite failure: {0}")]
    Sqlite(#[from] rusqlite::Error),
    /// Managed JSON observations could not be decoded.
    #[error("empire telemetry JSON failure: {0}")]
    Json(#[from] serde_json::Error),
    /// The projector worker could not start.
    #[error("empire telemetry worker could not start: {0}")]
    WorkerStart(#[from] std::io::Error),
    /// The projector stopped before shutdown completed.
    #[error("empire telemetry worker stopped unexpectedly")]
    WriterStopped,
    /// The projector worker panicked.
    #[error("empire telemetry worker panicked")]
    WriterPanicked,
}

#[derive(Clone, Debug)]
struct EmpirePaths {
    managed: PathBuf,
    history: PathBuf,
    telemetry: PathBuf,
}

#[derive(Debug)]
enum EmpireCommand {
    Shutdown(mpsc::Sender<()>),
}

/// Background projector that derives Grafana-friendly empire history.
///
/// The worker reads the managed/history databases in WAL read-only mode and
/// writes only to the isolated telemetry database. It never participates in
/// managed-state authority or gameplay operations.
pub struct EmpireTelemetryService {
    sender: SyncSender<EmpireCommand>,
    worker: Option<thread::JoinHandle<()>>,
}

impl std::fmt::Debug for EmpireTelemetryService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("EmpireTelemetryService").finish_non_exhaustive()
    }
}

impl EmpireTelemetryService {
    /// Starts the historical projector and records the currently known recipes.
    pub fn start(
        managed_database: impl AsRef<Path>,
        history_database: impl AsRef<Path>,
        telemetry_database: impl AsRef<Path>,
        blueprints: Vec<Blueprint>,
    ) -> Result<Self, EmpireTelemetryError> {
        let paths = EmpirePaths {
            managed: managed_database.as_ref().to_path_buf(),
            history: history_database.as_ref().to_path_buf(),
            telemetry: telemetry_database.as_ref().to_path_buf(),
        };
        let telemetry = open_telemetry(&paths.telemetry)?;
        ensure_schema(&telemetry)?;
        drop(telemetry);
        drop(open_read_only(&paths.managed)?);
        drop(open_read_only(&paths.history)?);

        let (sender, receiver) = mpsc::sync_channel(1);
        let worker = thread::Builder::new()
            .name("replicant-empire-telemetry".to_owned())
            .spawn(move || worker_main(paths, receiver, blueprints))?;
        Ok(Self {
            sender,
            worker: Some(worker),
        })
    }

    /// Flushes the newest event history/current snapshot and stops the worker.
    pub fn shutdown(mut self) -> Result<(), EmpireTelemetryError> {
        let (ack_tx, ack_rx) = mpsc::channel();
        self.sender
            .send(EmpireCommand::Shutdown(ack_tx))
            .map_err(|_| EmpireTelemetryError::WriterStopped)?;
        ack_rx
            .recv()
            .map_err(|_| EmpireTelemetryError::WriterStopped)?;
        if let Some(worker) = self.worker.take() {
            worker
                .join()
                .map_err(|_| EmpireTelemetryError::WriterPanicked)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct HistoryEventRow {
    rowid: i64,
    event_id: String,
    event_name: String,
    device_code: Option<String>,
    location_id: Option<String>,
    star_id: Option<String>,
    occurred_at_ms: i64,
    payload: Value,
}

#[derive(Clone, Debug)]
struct OpenPrintJob {
    start_event_id: String,
    started_at_ms: i64,
    resources: BTreeMap<String, i64>,
}

#[derive(Clone, Debug)]
struct BlueprintRecipe {
    resources: BTreeMap<String, i64>,
    components: BTreeMap<String, i64>,
    source: String,
}

#[derive(Clone, Copy, Debug)]
struct ResourceDelta<'a> {
    location: &'a str,
    resource: &'a str,
    physical_delta: i64,
    reserved_delta: i64,
    reason: &'a str,
    source: &'a str,
}

#[derive(Clone, Debug, Default)]
struct CurrentEmpireState {
    resources: BTreeMap<(String, String), i64>,
    device_status: BTreeMap<(String, String), i64>,
    device_totals: BTreeMap<String, i64>,
    device_identities: BTreeMap<String, String>,
    active_infrastructure: BTreeMap<String, i64>,
}

fn worker_main(paths: EmpirePaths, receiver: Receiver<EmpireCommand>, blueprints: Vec<Blueprint>) {
    let result = run_worker(&paths, &receiver, &blueprints);
    if let Err(error) = result {
        tracing::error!(error = %error, "empire telemetry worker stopped after an error");
    }
}

fn run_worker(
    paths: &EmpirePaths,
    receiver: &Receiver<EmpireCommand>,
    blueprints: &[Blueprint],
) -> Result<(), EmpireTelemetryError> {
    let managed = open_read_only(&paths.managed)?;
    let history = open_read_only(&paths.history)?;
    let mut telemetry = open_telemetry(&paths.telemetry)?;
    ensure_schema(&telemetry)?;
    reset_projector_if_needed(&telemetry)?;
    record_blueprint_catalogue(&telemetry, blueprints, now_millis(), "managed_catalogue")?;
    seed_current_device_identities(&managed, &mut telemetry, now_millis())?;
    let mut last_snapshot = meta_i64(&telemetry, "empire_last_snapshot_ms")?.unwrap_or_default();
    let mut last_maintenance = 0_i64;
    run_cycle(
        &managed,
        &history,
        &mut telemetry,
        &mut last_snapshot,
        &mut last_maintenance,
        true,
    );

    loop {
        match receiver.recv_timeout(EVENT_POLL_INTERVAL) {
            Ok(EmpireCommand::Shutdown(ack)) => {
                run_cycle(
                    &managed,
                    &history,
                    &mut telemetry,
                    &mut last_snapshot,
                    &mut last_maintenance,
                    true,
                );
                let _ = ack.send(());
                return Ok(());
            }
            Err(RecvTimeoutError::Timeout) => run_cycle(
                &managed,
                &history,
                &mut telemetry,
                &mut last_snapshot,
                &mut last_maintenance,
                false,
            ),
            Err(RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }
}

fn run_cycle(
    managed: &Connection,
    history: &Connection,
    telemetry: &mut Connection,
    last_snapshot: &mut i64,
    last_maintenance: &mut i64,
    force_snapshot: bool,
) {
    if let Err(error) = project_pending_history(history, telemetry) {
        tracing::warn!(error = %error, "empire history projection failed");
    }

    let now = now_millis();
    if force_snapshot || now.saturating_sub(*last_snapshot) >= SNAPSHOT_INTERVAL_MS {
        match snapshot_current_state(managed, telemetry, now) {
            Ok(()) => *last_snapshot = now,
            Err(error) => tracing::warn!(error = %error, "empire current-state snapshot failed"),
        }
    }
    if now.saturating_sub(*last_maintenance) >= MAINTENANCE_INTERVAL_MS {
        match maintain_snapshots(telemetry, now) {
            Ok(()) => *last_maintenance = now,
            Err(error) => tracing::warn!(error = %error, "empire telemetry maintenance failed"),
        }
    }
}

fn open_read_only(path: &Path) -> Result<Connection, rusqlite::Error> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    connection.busy_timeout(Duration::from_secs(15))?;
    Ok(connection)
}

fn open_telemetry(path: &Path) -> Result<Connection, rusqlite::Error> {
    let connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(15))?;
    connection.execute_batch("PRAGMA journal_mode = WAL; PRAGMA synchronous = NORMAL;")?;
    Ok(connection)
}

fn ensure_schema(connection: &Connection) -> Result<(), rusqlite::Error> {
    connection.execute_batch(EMPIRE_SCHEMA)?;
    connection.execute(
        "INSERT INTO telemetry_meta(key, value) VALUES ('empire_schema_version', '1') \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [],
    )?;
    Ok(())
}

fn reset_projector_if_needed(connection: &Connection) -> Result<(), rusqlite::Error> {
    let version = meta_i64(connection, "empire_projector_version")?.unwrap_or_default();
    if version == PROJECTOR_VERSION {
        return Ok(());
    }
    connection.execute_batch(
        "DELETE FROM empire_device_identity;
         DELETE FROM empire_print_job;
         DELETE FROM empire_resource_delta;
         DELETE FROM empire_resource_baseline;
         DELETE FROM empire_device_delta;
         DELETE FROM empire_device_baseline;
         DELETE FROM empire_device_snapshot;
         DELETE FROM empire_infrastructure_delta;
         DELETE FROM empire_infrastructure_baseline;
         DELETE FROM empire_travel;
         DELETE FROM empire_activity;
         DELETE FROM empire_projection_gap;",
    )?;
    set_meta(connection, "empire_projector_version", PROJECTOR_VERSION)?;
    set_meta(connection, "empire_last_history_rowid", 0)?;
    connection.execute("DELETE FROM empire_meta WHERE key = 'history_start_ms'", [])?;
    Ok(())
}

fn record_blueprint_catalogue(
    connection: &Connection,
    blueprints: &[Blueprint],
    observed_at_ms: i64,
    source: &str,
) -> Result<(), EmpireTelemetryError> {
    for blueprint in blueprints {
        let device_type = blueprint
            .device_type
            .as_ref()
            .map(|value| value.as_str())
            .unwrap_or_else(|| blueprint.id.as_str());
        record_recipe(
            connection,
            observed_at_ms,
            device_type,
            &blueprint.resources,
            &blueprint.components,
            source,
        )?;
    }
    Ok(())
}

fn record_recipe(
    connection: &Connection,
    observed_at_ms: i64,
    device_type: &str,
    resources: &BTreeMap<String, i64>,
    components: &BTreeMap<String, i64>,
    source: &str,
) -> Result<(), EmpireTelemetryError> {
    let resources_json = serde_json::to_string(resources)?;
    let components_json = serde_json::to_string(components)?;
    let latest = connection
        .query_row(
            "SELECT resources_json, components_json FROM empire_blueprint_recipe_history \
             WHERE device_type = ?1 ORDER BY observed_at_ms DESC LIMIT 1",
            [device_type],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .optional()?;
    if latest
        .as_ref()
        .is_some_and(|(old_resources, old_components)| {
            old_resources == &resources_json && old_components == &components_json
        })
    {
        return Ok(());
    }
    connection.execute(
        "INSERT OR REPLACE INTO empire_blueprint_recipe_history(\
            observed_at_ms, device_type, resources_json, components_json, source\
         ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            observed_at_ms,
            device_type,
            resources_json,
            components_json,
            source
        ],
    )?;
    Ok(())
}

fn project_pending_history(
    history: &Connection,
    telemetry: &mut Connection,
) -> Result<(), EmpireTelemetryError> {
    let mut cursor = meta_i64(telemetry, "empire_last_history_rowid")?.unwrap_or_default();
    loop {
        let events = read_history_batch(history, cursor)?;
        if events.is_empty() {
            break;
        }
        let transaction = telemetry.transaction()?;
        let mut last_rowid = cursor;
        for event in &events {
            project_event(&transaction, event)?;
            last_rowid = last_rowid.max(event.rowid);
            ensure_history_start(&transaction, event.occurred_at_ms)?;
        }
        set_meta_transaction(&transaction, "empire_last_history_rowid", last_rowid)?;
        transaction.commit()?;
        cursor = last_rowid;
    }
    Ok(())
}

fn read_history_batch(
    history: &Connection,
    cursor: i64,
) -> Result<Vec<HistoryEventRow>, EmpireTelemetryError> {
    let mut statement = history.prepare(
        "SELECT rowid, event_id, event_name, device_code, location_id, star_id, occurred_at, payload_json \
         FROM event_history WHERE applied_at IS NOT NULL AND rowid > ?1 ORDER BY rowid LIMIT 1000",
    )?;
    let rows = statement.query_map([cursor], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, Option<String>>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
        ))
    })?;
    let mut events = Vec::new();
    for row in rows {
        let (rowid, event_id, event_name, device_code, location_id, star_id, _occurred_at, payload) =
            row?;
        events.push(HistoryEventRow {
            rowid,
            occurred_at_ms: event_millis(&event_id).unwrap_or_default(),
            event_id,
            event_name,
            device_code,
            location_id,
            star_id,
            payload: serde_json::from_str(&payload)?,
        });
    }
    Ok(events)
}

fn project_event(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    match event.event_name.as_str() {
        "blueprint.unlocked" => project_blueprint_unlocked(transaction, event)?,
        "print.started" => project_print_started(transaction, event)?,
        "print.completed" | "device.print_completed" => {
            project_print_completed(transaction, event)?;
        }
        "device.decommissioned" => project_decommission(transaction, event)?,
        "mining.stopped" => project_mining_stopped(transaction, event)?,
        "ami.mining.digest" => project_ami_mining(transaction, event)?,
        "transport.collected" => project_transport(transaction, event, false)?,
        "transport.delivered" => project_transport(transaction, event, true)?,
        "ami.transport.digest" => project_ami_transport(transaction, event)?,
        "travel.departed" => project_travel_departed(transaction, event)?,
        "travel.arrived" => project_travel_arrived(transaction, event)?,
        "device.deployed" => project_device_deployed(transaction, event)?,
        "device.stowed" => project_device_stowed(transaction, event)?,
        "device.compacted" => project_device_compacted(transaction, event)?,
        "relay.activated" => project_relay_activated(transaction, event)?,
        "event.completed" => project_event_completed(transaction, event)?,
        "trade.completed" => project_trade_completed(transaction, event)?,
        _ => {}
    }
    Ok(())
}

fn project_blueprint_unlocked(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let Some(device_type) = string_field(&event.payload, "device_type") else {
        return Ok(());
    };
    let resources = resource_map(event.payload.get("resources"));
    let components = resource_map(event.payload.get("components"));
    record_recipe(
        transaction,
        event.occurred_at_ms,
        &device_type,
        &resources,
        &components,
        "blueprint.unlocked",
    )
}

fn project_print_started(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let Some(device_type) = string_field(&event.payload, "device_type") else {
        record_gap(
            transaction,
            event,
            "print_missing_device_type",
            None,
            None,
            "print.started did not contain device_type",
        )?;
        return Ok(());
    };
    let location = event_location(event);
    let Some(recipe) = recipe_for_event(transaction, &device_type, event.occurred_at_ms)? else {
        record_gap(
            transaction,
            event,
            "print_missing_blueprint",
            location.as_deref(),
            None,
            &format!("no blueprint recipe is known for {device_type}"),
        )?;
        record_activity(transaction, event, "print_started", &device_type, 1, location.as_deref(), "observed")?;
        return Ok(());
    };
    let BlueprintRecipe {
        resources,
        source: recipe_source,
        ..
    } = recipe;
    let location = location.unwrap_or_else(|| "unknown".to_owned());
    let printer_code = event.device_code.clone().unwrap_or_default();
    transaction.execute(
        "INSERT OR IGNORE INTO empire_print_job(\
            start_event_id, printer_code, device_type, location, started_at_ms, resources_json\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            event.event_id,
            printer_code,
            device_type,
            location,
            event.occurred_at_ms,
            serde_json::to_string(&resources)?
        ],
    )?;
    for (resource, quantity) in resources {
        insert_resource_delta(
            transaction,
            event,
            ResourceDelta {
                location: &location,
                resource: &resource,
                physical_delta: 0,
                reserved_delta: quantity,
                reason: "print_reserved",
                source: &recipe_source,
            },
        )?;
    }
    record_activity(
        transaction,
        event,
        "print_started",
        &device_type,
        1,
        Some(&location),
        "observed",
    )?;
    Ok(())
}

fn project_print_completed(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let Some(device_type) = string_field(&event.payload, "device_type") else {
        record_gap(
            transaction,
            event,
            "print_missing_device_type",
            event_location(event).as_deref(),
            None,
            "print.completed did not contain device_type",
        )?;
        return Ok(());
    };
    let location = event_location(event).unwrap_or_else(|| "unknown".to_owned());
    let matched = match_open_print_job(transaction, event, &device_type, &location)?;
    let (resources, components, source, release_reserved) = if let Some(job) = matched {
        transaction.execute(
            "UPDATE empire_print_job SET completed_event_id = ?1, completed_at_ms = ?2 \
             WHERE start_event_id = ?3",
            params![event.event_id, event.occurred_at_ms, job.start_event_id],
        )?;
        let components = recipe_for_event(transaction, &device_type, job.started_at_ms)?
            .map(|recipe| recipe.components)
            .unwrap_or_default();
        (
            job.resources,
            components,
            "inferred_blueprint_at_start".to_owned(),
            true,
        )
    } else if let Some(recipe) = recipe_for_event(transaction, &device_type, event.occurred_at_ms)? {
        let BlueprintRecipe {
            resources,
            components,
            source,
        } = recipe;
        record_gap(
            transaction,
            event,
            "print_completion_without_start",
            Some(&location),
            None,
            "print completion had no retained matching print.started; physical/component consumption is inferred but reservation history is unknown",
        )?;
        (resources, components, source, false)
    } else {
        record_gap(
            transaction,
            event,
            "print_missing_blueprint",
            Some(&location),
            None,
            &format!("no blueprint recipe is known for completed {device_type} print"),
        )?;
        (
            BTreeMap::new(),
            BTreeMap::new(),
            "unknown".to_owned(),
            false,
        )
    };
    for (resource, quantity) in resources {
        insert_resource_delta(
            transaction,
            event,
            ResourceDelta {
                location: &location,
                resource: &resource,
                physical_delta: -quantity,
                reserved_delta: if release_reserved { -quantity } else { 0 },
                reason: "print_consumed",
                source: &source,
            },
        )?;
    }

    if let Some(device_code) = string_field(&event.payload, "new_device_code") {
        upsert_identity(
            transaction,
            &device_code,
            &device_type,
            event.occurred_at_ms,
            "print.completed",
        )?;
        insert_device_delta(
            transaction,
            event,
            &device_type,
            &device_code,
            1,
            "printed",
            "observed",
        )?;
    }
    project_print_components_consumed(
        transaction,
        event,
        &location,
        &components,
        string_array(event.payload.get("consumed_device_codes")),
    )?;
    record_activity(
        transaction,
        event,
        "print_completed",
        &device_type,
        1,
        Some(&location),
        "observed",
    )?;
    Ok(())
}

fn project_print_components_consumed(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
    location: &str,
    expected_components: &BTreeMap<String, i64>,
    consumed_device_codes: Vec<String>,
) -> Result<(), EmpireTelemetryError> {
    let mut remaining = expected_components
        .iter()
        .filter_map(|(device_type, quantity)| {
            if *quantity > 0 {
                Some((device_type.clone(), *quantity))
            } else {
                None
            }
        })
        .collect::<BTreeMap<_, _>>();
    let expected_count = remaining.values().copied().sum::<i64>();
    let observed_count = i64::try_from(consumed_device_codes.len()).unwrap_or(i64::MAX);
    if expected_count != observed_count && observed_count != 0 {
        record_gap(
            transaction,
            event,
            "print_component_count_mismatch",
            Some(location),
            None,
            &format!(
                "blueprint expected {expected_count} component devices but print.completed named {observed_count} consumed codes"
            ),
        )?;
    }

    let mut unknown_codes = Vec::new();
    for device_code in consumed_device_codes {
        if let Some(device_type) = identity_type(transaction, &device_code)? {
            insert_device_delta(
                transaction,
                event,
                &device_type,
                &device_code,
                -1,
                "print_component_consumed",
                "observed_identity",
            )?;
            if let Some(quantity) = remaining.get_mut(&device_type) {
                *quantity = quantity.saturating_sub(1);
            }
        } else {
            unknown_codes.push(device_code);
        }
    }
    remaining.retain(|_, quantity| *quantity > 0);

    if remaining.len() == 1 {
        let (device_type, remaining_quantity) = remaining
            .iter_mut()
            .next()
            .expect("single remaining component type");
        let assignable = usize::try_from(*remaining_quantity)
            .unwrap_or(usize::MAX)
            .min(unknown_codes.len());
        for device_code in unknown_codes.drain(..assignable) {
            upsert_identity(
                transaction,
                &device_code,
                device_type,
                event.occurred_at_ms,
                "print.completed blueprint inference",
            )?;
            insert_device_delta(
                transaction,
                event,
                device_type,
                &device_code,
                -1,
                "print_component_consumed",
                "inferred_blueprint_component_type",
            )?;
            *remaining_quantity = remaining_quantity.saturating_sub(1);
        }
        remaining.retain(|_, quantity| *quantity > 0);
    } else if !remaining.is_empty() && !unknown_codes.is_empty() {
        record_gap(
            transaction,
            event,
            "print_component_code_type_ambiguous",
            Some(location),
            None,
            "print.completed named consumed component codes, but multiple blueprint component types remain and the event does not map codes to types; population counts are inferred from the blueprint without assigning exact code identities",
        )?;
    }

    let inferred_remaining = remaining.values().copied().sum::<i64>();
    let inferred_remaining_usize = usize::try_from(inferred_remaining).unwrap_or(usize::MAX);
    if unknown_codes.len() >= inferred_remaining_usize {
        unknown_codes.drain(..inferred_remaining_usize);
    } else {
        unknown_codes.clear();
    }
    for (device_type, quantity) in remaining {
        for ordinal in 0..quantity {
            insert_device_delta(
                transaction,
                event,
                &device_type,
                &format!("inferred:{}:{device_type}:{ordinal}", event.event_id),
                -1,
                "print_component_consumed",
                "inferred_blueprint_missing_or_untyped_codes",
            )?;
        }
    }

    for device_code in unknown_codes {
        record_gap(
            transaction,
            event,
            "consumed_component_type_unknown",
            Some(location),
            None,
            &format!(
                "component {device_code} was consumed but its historical device type could not be resolved"
            ),
        )?;
        insert_device_delta(
            transaction,
            event,
            "__unknown__",
            &device_code,
            -1,
            "print_component_consumed",
            "observed_type_unknown",
        )?;
    }

    Ok(())
}

fn project_decommission(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let location = event_location(event).unwrap_or_else(|| "unknown".to_owned());
    for (resource, quantity) in resource_map(event.payload.get("resources_recovered")) {
        insert_resource_delta(
            transaction,
            event,
            ResourceDelta {
                location: &location,
                resource: &resource,
                physical_delta: quantity,
                reserved_delta: 0,
                reason: "decommission_recovered",
                source: "observed",
            },
        )?;
    }
    let device_code = event.device_code.clone().unwrap_or_default();
    let device_type = identity_type(transaction, &device_code)?
        .unwrap_or_else(|| "__unknown__".to_owned());
    if device_type == "__unknown__" {
        record_gap(
            transaction,
            event,
            "decommission_device_type_unknown",
            Some(&location),
            None,
            "decommissioned device predates retained type evidence",
        )?;
    }
    insert_device_delta(
        transaction,
        event,
        &device_type,
        &device_code,
        -1,
        "decommissioned",
        "observed",
    )?;
    record_activity(
        transaction,
        event,
        "device_decommissioned",
        &device_type,
        1,
        Some(&location),
        "observed",
    )?;
    Ok(())
}

fn project_mining_stopped(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let location = string_field(&event.payload, "location")
        .or_else(|| event_location(event))
        .unwrap_or_else(|| "unknown".to_owned());
    let Some(resource) = string_field(&event.payload, "resource_type") else {
        return Ok(());
    };
    let Some(quantity) = i64_field(&event.payload, "quantity_mined") else {
        record_gap(
            transaction,
            event,
            "mining_quantity_missing",
            Some(&location),
            Some(&resource),
            "mining.stopped did not contain quantity_mined",
        )?;
        return Ok(());
    };
    insert_resource_delta(
        transaction,
        event,
        ResourceDelta {
            location: &location,
            resource: &resource,
            physical_delta: quantity,
            reserved_delta: 0,
            reason: "mined",
            source: "observed",
        },
    )?;
    record_activity(
        transaction,
        event,
        "resource_mined",
        &resource,
        quantity,
        Some(&location),
        "observed",
    )?;
    Ok(())
}

fn project_ami_mining(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let activity = event.payload.get("activity");
    let stopped = nested_i64(activity, &["counts", "mining.stopped"]).unwrap_or_default();
    if stopped <= 0 {
        return Ok(());
    }
    let location = nested_string(event.payload.get("report"), &["location"])
        .or_else(|| event_location(event));
    record_activity(
        transaction,
        event,
        "mining_sessions_completed",
        "ami",
        stopped,
        location.as_deref(),
        "observed_count_only",
    )?;
    record_gap(
        transaction,
        event,
        "ami_mining_quantity_unavailable",
        location.as_deref(),
        None,
        "AMI mining digest reports mining activity but not quantity_mined; the next authoritative inventory snapshot reconciles the missing quantity",
    )?;
    Ok(())
}

fn project_transport(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
    delivered: bool,
) -> Result<(), EmpireTelemetryError> {
    let location = event_location(event).unwrap_or_else(|| "unknown".to_owned());
    let resources = resource_map(event.payload.get("resources"));
    let reason = if delivered {
        "transport_delivered"
    } else {
        "transport_collected"
    };
    let metric = if delivered {
        "resource_delivered"
    } else {
        "resource_collected"
    };
    for (resource, quantity) in resources {
        insert_resource_delta(
            transaction,
            event,
            ResourceDelta {
                location: &location,
                resource: &resource,
                physical_delta: if delivered { quantity } else { -quantity },
                reserved_delta: 0,
                reason,
                source: "observed",
            },
        )?;
        record_activity(
            transaction,
            event,
            metric,
            &resource,
            quantity,
            Some(&location),
            "observed",
        )?;
    }
    Ok(())
}

fn project_ami_transport(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let report = event.payload.get("report");
    let collect = nested_string(report, &["collect"]);
    let deliver = nested_string(report, &["deliver"]);
    let departed = nested_i64(event.payload.get("activity"), &["counts", "travel.departed"])
        .unwrap_or_default();
    if departed > 0 {
        let series = match (collect.as_deref(), deliver.as_deref()) {
            (Some(origin), Some(destination)) => travel_scope(origin, destination),
            _ => "unknown",
        };
        record_activity(
            transaction,
            event,
            "travel_departed",
            series,
            departed,
            collect.as_deref(),
            "ami_digest_route_inferred",
        )?;
    }
    let collected = nested_i64(event.payload.get("activity"), &["counts", "transport.collected"])
        .unwrap_or_default();
    let delivered = nested_i64(event.payload.get("activity"), &["counts", "transport.delivered"])
        .unwrap_or_default();
    if collected > 0 || delivered > 0 {
        record_gap(
            transaction,
            event,
            "ami_transport_quantity_unavailable",
            collect.as_deref(),
            None,
            "AMI transport digest reports collection/delivery activity but not per-operation resource quantities; authoritative inventory snapshots reconcile the location balance",
        )?;
    }
    Ok(())
}

fn project_travel_departed(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let Some(origin) = string_field(&event.payload, "origin") else {
        return Ok(());
    };
    let Some(destination) = string_field(&event.payload, "destination") else {
        return Ok(());
    };
    let origin_system = system_designation(&origin).to_owned();
    let destination_system = system_designation(&destination).to_owned();
    let inter_system = i64::from(origin_system != destination_system);
    let travel_type = string_field(&event.payload, "travel_type").unwrap_or_else(|| "unknown".to_owned());
    let distance_ly = event.payload.get("distance_ly").and_then(Value::as_f64);
    let planned_duration_seconds = i64_field(&event.payload, "travel_time_seconds");
    let attached_device_count = i64::try_from(string_array(event.payload.get("attached_devices")).len())
        .unwrap_or(i64::MAX);
    transaction.execute(
        "INSERT OR IGNORE INTO empire_travel(\
            departed_event_id, departed_at_ms, device_code, origin, destination, origin_system,\
            destination_system, inter_system, travel_type, distance_ly, planned_duration_seconds,\
            attached_device_count, source\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, 'observed')",
        params![
            event.event_id,
            event.occurred_at_ms,
            event.device_code.as_deref().unwrap_or(""),
            origin,
            destination,
            origin_system,
            destination_system,
            inter_system,
            travel_type,
            distance_ly,
            planned_duration_seconds,
            attached_device_count,
        ],
    )?;
    record_activity(
        transaction,
        event,
        "travel_departed",
        if inter_system == 1 { "inter_system" } else { "intra_system" },
        1,
        event.location_id.as_deref(),
        "observed",
    )?;
    Ok(())
}

fn project_travel_arrived(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let device_code = event.device_code.as_deref().unwrap_or("");
    if device_code.is_empty() {
        return Ok(());
    }
    let origin = string_field(&event.payload, "origin");
    let destination = string_field(&event.payload, "destination");
    let departed = transaction
        .query_row(
            "SELECT departed_event_id, departed_at_ms FROM empire_travel \
             WHERE device_code = ?1 AND arrived_event_id IS NULL \
               AND (?2 IS NULL OR origin = ?2) AND (?3 IS NULL OR destination = ?3) \
             ORDER BY departed_at_ms DESC LIMIT 1",
            params![device_code, origin, destination],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
        )
        .optional()?;
    let Some((departed_event_id, departed_at_ms)) = departed else {
        record_gap(
            transaction,
            event,
            "travel_arrival_without_departure",
            event_location(event).as_deref(),
            None,
            "travel.arrived had no retained matching travel.departed",
        )?;
        return Ok(());
    };
    transaction.execute(
        "UPDATE empire_travel SET arrived_event_id = ?1, arrived_at_ms = ?2, actual_duration_ms = ?3 \
         WHERE departed_event_id = ?4",
        params![
            event.event_id,
            event.occurred_at_ms,
            event.occurred_at_ms.saturating_sub(departed_at_ms).max(0),
            departed_event_id
        ],
    )?;
    Ok(())
}

fn project_relay_activated(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let device_code = event.device_code.as_deref().unwrap_or_default();
    let Some(device_type) = identity_type(transaction, device_code)? else {
        record_gap(
            transaction,
            event,
            "relay_activation_device_type_unknown",
            event_location(event).as_deref(),
            None,
            "relay.activated could not be assigned to an FTL relay or deep-space relay station",
        )?;
        return Ok(());
    };
    let Some(kind) = relay_infrastructure_kind(&device_type) else {
        record_gap(
            transaction,
            event,
            "relay_activation_device_type_unexpected",
            event_location(event).as_deref(),
            None,
            &format!("relay.activated referenced device type `{device_type}`"),
        )?;
        return Ok(());
    };
    insert_infrastructure_delta(
        transaction,
        event,
        kind,
        1,
        "relay_activated",
        "observed",
    )?;
    record_activity(
        transaction,
        event,
        "relay_activated",
        &device_type,
        1,
        event_location(event).as_deref(),
        "observed",
    )?;
    Ok(())
}

fn project_device_deployed(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let device_code = event.device_code.as_deref().unwrap_or_default();
    let Some(device_type) = identity_type(transaction, device_code)? else {
        return Ok(());
    };
    if device_type != "ftl_beacon" {
        return Ok(());
    }
    insert_infrastructure_delta(
        transaction,
        event,
        ACTIVE_FTL_BEACON,
        1,
        "beacon_deployed",
        "inferred_device_lifecycle",
    )?;
    Ok(())
}

fn project_device_stowed(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let device_code = event.device_code.as_deref().unwrap_or_default();
    let Some(device_type) = identity_type(transaction, device_code)? else {
        return Ok(());
    };
    let kind = match device_type.as_str() {
        "ftl_relay" => ACTIVE_FTL_RELAY,
        "ftl_beacon" => ACTIVE_FTL_BEACON,
        _ => return Ok(()),
    };
    insert_infrastructure_delta(
        transaction,
        event,
        kind,
        -1,
        "infrastructure_stowed",
        "inferred_device_lifecycle",
    )?;
    Ok(())
}

fn project_device_compacted(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let device_code = event.device_code.as_deref().unwrap_or_default();
    let Some(device_type) = identity_type(transaction, device_code)? else {
        return Ok(());
    };
    let Some(kind) = relay_infrastructure_kind(&device_type) else {
        return Ok(());
    };
    insert_infrastructure_delta(
        transaction,
        event,
        kind,
        -1,
        "relay_compacted",
        "inferred_device_lifecycle",
    )?;
    Ok(())
}

fn relay_infrastructure_kind(device_type: &str) -> Option<&'static str> {
    match device_type {
        "ftl_relay" => Some(ACTIVE_FTL_RELAY),
        "deep_space_relay_station" => Some(ACTIVE_DEEP_SPACE_RELAY_STATION),
        _ => None,
    }
}

fn project_event_completed(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let location = nested_string(Some(&event.payload), &["location"])
        .or_else(|| event_location(event))
        .unwrap_or_else(|| "unknown".to_owned());
    for (resource, quantity) in resource_map(event.payload.pointer("/consumed/resources")) {
        insert_resource_delta(
            transaction,
            event,
            ResourceDelta {
                location: &location,
                resource: &resource,
                physical_delta: -quantity,
                reserved_delta: 0,
                reason: "event_consumed",
                source: "observed",
            },
        )?;
    }
    for (resource, quantity) in resource_map(event.payload.pointer("/rewards/resources")) {
        insert_resource_delta(
            transaction,
            event,
            ResourceDelta {
                location: &location,
                resource: &resource,
                physical_delta: quantity,
                reserved_delta: 0,
                reason: "event_reward",
                source: "observed",
            },
        )?;
    }
    if let Some(devices) = event.payload.pointer("/consumed/devices").and_then(Value::as_array) {
        for device in devices {
            let device_code = string_field(device, "device_code").unwrap_or_default();
            let device_type = string_field(device, "device_type")
                .or_else(|| identity_type(transaction, &device_code).ok().flatten())
                .unwrap_or_else(|| "__unknown__".to_owned());
            if !device_code.is_empty() && device_type != "__unknown__" {
                upsert_identity(
                    transaction,
                    &device_code,
                    &device_type,
                    event.occurred_at_ms,
                    "event.completed",
                )?;
            }
            insert_device_delta(
                transaction,
                event,
                &device_type,
                &device_code,
                -1,
                "event_consumed",
                "observed",
            )?;
        }
    }
    if let Some(devices) = event.payload.pointer("/rewards/devices").and_then(Value::as_array) {
        for device in devices {
            let (device_code, device_type) = if let Some(code) = device.as_str() {
                (code.to_owned(), "__unknown__".to_owned())
            } else {
                (
                    string_field(device, "device_code").unwrap_or_default(),
                    string_field(device, "device_type").unwrap_or_else(|| "__unknown__".to_owned()),
                )
            };
            if !device_code.is_empty() && device_type != "__unknown__" {
                upsert_identity(
                    transaction,
                    &device_code,
                    &device_type,
                    event.occurred_at_ms,
                    "event.completed",
                )?;
            }
            insert_device_delta(
                transaction,
                event,
                &device_type,
                &device_code,
                1,
                "event_reward",
                "observed",
            )?;
        }
    }
    record_activity(
        transaction,
        event,
        "location_event_completed",
        string_field(&event.payload, "event_type")
            .as_deref()
            .unwrap_or("unknown"),
        1,
        Some(&location),
        "observed",
    )?;
    Ok(())
}

fn project_trade_completed(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
) -> Result<(), EmpireTelemetryError> {
    let role = string_field(&event.payload, "role").unwrap_or_else(|| "unknown".to_owned());
    record_activity(
        transaction,
        event,
        "trade_completed",
        &role,
        1,
        event_location(event).as_deref(),
        "observed",
    )?;
    let location = event_location(event).unwrap_or_else(|| "unknown".to_owned());
    for path in ["/rewards_received/resources", "/criteria_received/resources"] {
        for (resource, quantity) in resource_map(event.payload.pointer(path)) {
            insert_resource_delta(
                transaction,
                event,
                ResourceDelta {
                    location: &location,
                    resource: &resource,
                    physical_delta: quantity,
                    reserved_delta: 0,
                    reason: "trade_received",
                    source: "observed_received_side",
                },
            )?;
        }
    }
    record_gap(
        transaction,
        event,
        "trade_net_resources_incomplete",
        Some(&location),
        None,
        "trade.completed exposes the resources/devices received by this role but not the complete outgoing side of the exchange; inventory snapshots reconcile the unknown net change",
    )?;
    for path in ["/rewards_received/devices", "/criteria_received/devices", "/new_device_codes"] {
        if let Some(devices) = event.payload.pointer(path).and_then(Value::as_array) {
            for device in devices {
                let Some(device_code) = device.as_str() else {
                    continue;
                };
                insert_device_delta(
                    transaction,
                    event,
                    "__unknown__",
                    device_code,
                    1,
                    "trade_received",
                    "observed_type_unknown",
                )?;
            }
        }
    }
    Ok(())
}

fn match_open_print_job(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
    device_type: &str,
    location: &str,
) -> Result<Option<OpenPrintJob>, EmpireTelemetryError> {
    let printer_code = event.device_code.as_deref().unwrap_or("");
    let row = if printer_code.is_empty() {
        transaction
            .query_row(
                "SELECT start_event_id, started_at_ms, resources_json FROM empire_print_job \
                 WHERE completed_event_id IS NULL AND device_type = ?1 AND location = ?2 \
                 ORDER BY started_at_ms LIMIT 1",
                params![device_type, location],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
    } else {
        transaction
            .query_row(
                "SELECT start_event_id, started_at_ms, resources_json FROM empire_print_job \
                 WHERE completed_event_id IS NULL AND printer_code = ?1 AND device_type = ?2 \
                 ORDER BY started_at_ms LIMIT 1",
                params![printer_code, device_type],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?
    };
    row.map(|(start_event_id, started_at_ms, resources)| {
        serde_json::from_str(&resources).map(|resources| OpenPrintJob {
            start_event_id,
            started_at_ms,
            resources,
        })
    })
    .transpose()
    .map_err(EmpireTelemetryError::from)
}

fn recipe_for_event(
    connection: &Connection,
    device_type: &str,
    occurred_at_ms: i64,
) -> Result<Option<BlueprintRecipe>, EmpireTelemetryError> {
    let at_or_before = connection
        .query_row(
            "SELECT resources_json, components_json, source FROM empire_blueprint_recipe_history \
             WHERE device_type = ?1 AND observed_at_ms <= ?2 ORDER BY observed_at_ms DESC LIMIT 1",
            params![device_type, occurred_at_ms],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?;
    let (resources_json, components_json, source) = if let Some(value) = at_or_before {
        value
    } else if let Some((resources, components, _source)) = connection
        .query_row(
            "SELECT resources_json, components_json, source FROM empire_blueprint_recipe_history \
             WHERE device_type = ?1 ORDER BY observed_at_ms ASC LIMIT 1",
            [device_type],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        )
        .optional()?
    {
        (resources, components, "inferred_current_recipe".to_owned())
    } else {
        return Ok(None);
    };
    Ok(Some(BlueprintRecipe {
        resources: serde_json::from_str(&resources_json)?,
        components: serde_json::from_str(&components_json)?,
        source,
    }))
}

fn insert_resource_delta(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
    delta: ResourceDelta<'_>,
) -> Result<(), rusqlite::Error> {
    transaction.execute(
        "INSERT OR IGNORE INTO empire_resource_delta(\
            event_id, occurred_at_ms, location, resource, physical_delta, reserved_delta, reason, source\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            event.event_id,
            event.occurred_at_ms,
            delta.location,
            delta.resource,
            delta.physical_delta,
            delta.reserved_delta,
            delta.reason,
            delta.source
        ],
    )?;
    Ok(())
}

fn insert_device_delta(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
    device_type: &str,
    device_code: &str,
    delta: i64,
    reason: &str,
    source: &str,
) -> Result<(), rusqlite::Error> {
    transaction.execute(
        "INSERT OR IGNORE INTO empire_device_delta(\
            event_id, occurred_at_ms, device_type, device_code, delta, reason, source\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            event.event_id,
            event.occurred_at_ms,
            device_type,
            device_code,
            delta,
            reason,
            source
        ],
    )?;
    Ok(())
}

fn insert_infrastructure_delta(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
    kind: &str,
    delta: i64,
    reason: &str,
    source: &str,
) -> Result<(), rusqlite::Error> {
    transaction.execute(
        "INSERT OR IGNORE INTO empire_infrastructure_delta(\
            event_id, occurred_at_ms, kind, delta, reason, source\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![event.event_id, event.occurred_at_ms, kind, delta, reason, source],
    )?;
    Ok(())
}

fn record_activity(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
    metric: &str,
    series: &str,
    value: i64,
    location: Option<&str>,
    source: &str,
) -> Result<(), rusqlite::Error> {
    let system = location.map(system_designation);
    transaction.execute(
        "INSERT OR IGNORE INTO empire_activity(\
            event_id, occurred_at_ms, metric, series, value, location, system, source\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            event.event_id,
            event.occurred_at_ms,
            metric,
            series,
            value,
            location,
            system,
            source
        ],
    )?;
    Ok(())
}

fn record_gap(
    transaction: &Transaction<'_>,
    event: &HistoryEventRow,
    kind: &str,
    location: Option<&str>,
    resource: Option<&str>,
    detail: &str,
) -> Result<(), rusqlite::Error> {
    transaction.execute(
        "INSERT OR IGNORE INTO empire_projection_gap(\
            event_id, occurred_at_ms, kind, location, resource, detail\
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            event.event_id,
            event.occurred_at_ms,
            kind,
            location,
            resource,
            detail
        ],
    )?;
    Ok(())
}

fn upsert_identity(
    connection: &Connection,
    device_code: &str,
    device_type: &str,
    observed_at_ms: i64,
    source: &str,
) -> Result<(), rusqlite::Error> {
    if device_code.is_empty() || device_type.is_empty() {
        return Ok(());
    }
    connection.execute(
        "INSERT INTO empire_device_identity(\
            device_code, device_type, first_seen_ms, last_seen_ms, source\
         ) VALUES (?1, ?2, ?3, ?3, ?4) \
         ON CONFLICT(device_code) DO UPDATE SET \
            device_type = CASE WHEN excluded.device_type != '__unknown__' THEN excluded.device_type ELSE device_type END, \
            first_seen_ms = MIN(first_seen_ms, excluded.first_seen_ms), \
            last_seen_ms = MAX(last_seen_ms, excluded.last_seen_ms), \
            source = CASE WHEN excluded.device_type != '__unknown__' THEN excluded.source ELSE source END",
        params![device_code, device_type, observed_at_ms, source],
    )?;
    Ok(())
}

fn identity_type(
    connection: &Connection,
    device_code: &str,
) -> Result<Option<String>, rusqlite::Error> {
    if device_code.is_empty() {
        return Ok(None);
    }
    connection
        .query_row(
            "SELECT device_type FROM empire_device_identity WHERE device_code = ?1",
            [device_code],
            |row| row.get(0),
        )
        .optional()
}

fn seed_current_device_identities(
    managed: &Connection,
    telemetry: &mut Connection,
    observed_at_ms: i64,
) -> Result<(), EmpireTelemetryError> {
    let mut statement = managed.prepare(
        "SELECT device_id, COALESCE(device_type, '__unknown__') \
         FROM devices WHERE realm = 'live' AND access_scope = 'owned'",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let transaction = telemetry.transaction()?;
    for row in rows {
        let (device_code, device_type) = row?;
        upsert_identity(
            &transaction,
            &device_code,
            &device_type,
            observed_at_ms,
            "managed_seed",
        )?;
    }
    transaction.commit()?;
    Ok(())
}

fn snapshot_current_state(
    managed: &Connection,
    telemetry: &mut Connection,
    observed_at_ms: i64,
) -> Result<(), EmpireTelemetryError> {
    let current = read_current_state(managed)?;
    if current.resources.is_empty() && current.device_status.is_empty() {
        tracing::debug!("empire snapshot skipped because managed projections are still empty");
        return Ok(());
    }
    let transaction = telemetry.transaction()?;
    for (device_code, device_type) in &current.device_identities {
        upsert_identity(
            &transaction,
            device_code,
            device_type,
            observed_at_ms,
            "managed_snapshot",
        )?;
    }
    let history_start = meta_i64_transaction(&transaction, "history_start_ms")?
        .unwrap_or(observed_at_ms);
    ensure_resource_baselines(&transaction, &current, history_start)?;
    reanchor_device_baselines(&transaction, &current, history_start, observed_at_ms)?;
    reanchor_infrastructure_baseline(&transaction, &current, history_start, observed_at_ms)?;
    reconcile_resources(&transaction, &current, observed_at_ms)?;
    insert_current_snapshots(&transaction, &current, observed_at_ms)?;
    set_meta_transaction(&transaction, "empire_last_snapshot_ms", observed_at_ms)?;
    transaction.commit()?;
    Ok(())
}

fn read_current_state(managed: &Connection) -> Result<CurrentEmpireState, EmpireTelemetryError> {
    let mut state = CurrentEmpireState::default();
    let mut inventory_statement = managed.prepare(
        "SELECT inventory_json FROM inventories WHERE realm = 'live' AND owner_kind = 'location'",
    )?;
    let inventories = inventory_statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in inventories {
        let observation: Observation<Inventory> = serde_json::from_str(&row?)?;
        let location = observation
            .value
            .location
            .as_ref()
            .map(|key| key.id.as_str().to_owned())
            .or_else(|| match &observation.value.owner {
                InventoryOwner::Location(key) => Some(key.id.as_str().to_owned()),
                _ => None,
            });
        let Some(location) = location else {
            continue;
        };
        for item in observation.value.items {
            if item.quantity == 0 {
                continue;
            }
            *state
                .resources
                .entry((location.clone(), item.resource))
                .or_default() += item.quantity;
        }
    }

    let mut device_statement = managed.prepare(
        "SELECT device_id, COALESCE(device_type, '__unknown__'), COALESCE(status, 'unknown') \
         FROM devices WHERE realm = 'live' AND access_scope = 'owned'",
    )?;
    let devices = device_statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    for row in devices {
        let (device_code, device_type, status) = row?;
        *state
            .device_status
            .entry((device_type.clone(), status.clone()))
            .or_default() += 1;
        *state.device_totals.entry(device_type.clone()).or_default() += 1;
        state.device_identities.insert(device_code, device_type.clone());
        let infrastructure_kind = match device_type.as_str() {
            "ftl_relay" if matches!(status.as_str(), "active" | "relaying") => {
                Some(ACTIVE_FTL_RELAY)
            }
            "deep_space_relay_station" if matches!(status.as_str(), "active" | "relaying") => {
                Some(ACTIVE_DEEP_SPACE_RELAY_STATION)
            }
            "ftl_beacon" if status == "monitoring" => Some(ACTIVE_FTL_BEACON),
            _ => None,
        };
        if let Some(kind) = infrastructure_kind {
            *state.active_infrastructure.entry(kind.to_owned()).or_default() += 1;
        }
    }
    Ok(state)
}

fn ensure_resource_baselines(
    transaction: &Transaction<'_>,
    current: &CurrentEmpireState,
    history_start: i64,
) -> Result<(), EmpireTelemetryError> {
    let existing: i64 = transaction.query_row(
        "SELECT COUNT(*) FROM empire_resource_baseline",
        [],
        |row| row.get(0),
    )?;
    if existing > 0 {
        return Ok(());
    }
    let mut keys = current.resources.keys().cloned().collect::<BTreeSet<_>>();
    let mut statement = transaction.prepare("SELECT DISTINCT location, resource FROM empire_resource_delta")?;
    let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
    for row in rows {
        keys.insert(row?);
    }
    drop(statement);
    let current_reserved = current_reserved_resources(transaction)?;
    for (location, resource) in keys {
        let physical_delta: i64 = transaction.query_row(
            "SELECT COALESCE(SUM(physical_delta), 0) FROM empire_resource_delta WHERE location = ?1 AND resource = ?2",
            params![location, resource],
            |row| row.get(0),
        )?;
        let reserved_delta: i64 = transaction.query_row(
            "SELECT COALESCE(SUM(reserved_delta), 0) FROM empire_resource_delta WHERE location = ?1 AND resource = ?2",
            params![location, resource],
            |row| row.get(0),
        )?;
        let current_physical = current
            .resources
            .get(&(location.clone(), resource.clone()))
            .copied()
            .unwrap_or_default();
        let current_reserved = current_reserved
            .get(&(location.clone(), resource.clone()))
            .copied()
            .unwrap_or_default();
        let confidence = resource_confidence(transaction, &location, &resource)?;
        transaction.execute(
            "INSERT INTO empire_resource_baseline(\
                as_of_ms, location, resource, physical_quantity, reserved_quantity, confidence\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                history_start,
                location,
                resource,
                current_physical.saturating_sub(physical_delta),
                current_reserved.saturating_sub(reserved_delta),
                confidence
            ],
        )?;
    }
    Ok(())
}

fn reanchor_device_baselines(
    transaction: &Transaction<'_>,
    current: &CurrentEmpireState,
    history_start: i64,
    observed_at_ms: i64,
) -> Result<(), rusqlite::Error> {
    // Device history is anchored from authoritative managed state and walked
    // backwards through retained lifecycle deltas.  Do not model an observed
    // mismatch as another lifecycle event: doing so turns projection gaps into
    // fake prints/decommissions and lets errors accumulate over time.
    transaction.execute(
        "DELETE FROM empire_device_delta WHERE source = 'authoritative_snapshot' OR reason = 'reconciliation'",
        [],
    )?;
    let mut types = current.device_totals.keys().cloned().collect::<BTreeSet<_>>();
    let mut statement = transaction.prepare("SELECT DISTINCT device_type FROM empire_device_delta")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        types.insert(row?);
    }
    drop(statement);
    for device_type in types {
        let delta: i64 = transaction.query_row(
            "SELECT COALESCE(SUM(delta), 0) FROM empire_device_delta \
             WHERE device_type = ?1 AND occurred_at_ms <= ?2",
            params![device_type, observed_at_ms],
            |row| row.get(0),
        )?;
        let current_count = current.device_totals.get(&device_type).copied().unwrap_or_default();
        let confidence = if device_type == "__unknown__" {
            "incomplete"
        } else {
            "reverse_anchored"
        };
        transaction.execute(
            "INSERT INTO empire_device_baseline(as_of_ms, device_type, total_count, confidence) \
             VALUES (?1, ?2, ?3, ?4) \
             ON CONFLICT(device_type) DO UPDATE SET \
                as_of_ms = excluded.as_of_ms, \
                total_count = excluded.total_count, \
                confidence = excluded.confidence",
            params![
                history_start,
                device_type,
                current_count.saturating_sub(delta),
                confidence
            ],
        )?;
    }
    Ok(())
}

fn reanchor_infrastructure_baseline(
    transaction: &Transaction<'_>,
    current: &CurrentEmpireState,
    history_start: i64,
    observed_at_ms: i64,
) -> Result<(), rusqlite::Error> {
    transaction.execute(
        "DELETE FROM empire_infrastructure_delta WHERE source = 'authoritative_snapshot' OR reason = 'reconciliation'",
        [],
    )?;
    for kind in [
        ACTIVE_FTL_RELAY,
        ACTIVE_DEEP_SPACE_RELAY_STATION,
        ACTIVE_FTL_BEACON,
    ] {
        let delta: i64 = transaction.query_row(
            "SELECT COALESCE(SUM(delta), 0) FROM empire_infrastructure_delta \
             WHERE kind = ?1 AND occurred_at_ms <= ?2",
            params![kind, observed_at_ms],
            |row| row.get(0),
        )?;
        let current_count = current
            .active_infrastructure
            .get(kind)
            .copied()
            .unwrap_or_default();
        transaction.execute(
            "INSERT INTO empire_infrastructure_baseline(as_of_ms, kind, active_count, confidence) \
             VALUES (?1, ?2, ?3, 'reverse_anchored') \
             ON CONFLICT(kind) DO UPDATE SET \
                as_of_ms = excluded.as_of_ms, \
                active_count = excluded.active_count, \
                confidence = excluded.confidence",
            params![history_start, kind, current_count.saturating_sub(delta)],
        )?;
    }
    Ok(())
}

fn reconcile_resources(
    transaction: &Transaction<'_>,
    current: &CurrentEmpireState,
    observed_at_ms: i64,
) -> Result<(), EmpireTelemetryError> {
    let mut baselines = BTreeMap::new();
    let mut statement = transaction.prepare(
        "SELECT location, resource, physical_quantity FROM empire_resource_baseline",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })?;
    for row in rows {
        let (location, resource, quantity) = row?;
        baselines.insert((location, resource), quantity);
    }
    drop(statement);
    let mut keys = baselines.keys().cloned().collect::<BTreeSet<_>>();
    keys.extend(current.resources.keys().cloned());
    let history_start = meta_i64_transaction(transaction, "history_start_ms")?.unwrap_or(observed_at_ms);
    for (location, resource) in keys {
        let baseline = if let Some(value) = baselines.get(&(location.clone(), resource.clone())) {
            *value
        } else {
            transaction.execute(
                "INSERT INTO empire_resource_baseline(\
                    as_of_ms, location, resource, physical_quantity, reserved_quantity, confidence\
                 ) VALUES (?1, ?2, ?3, 0, 0, 'reconciled')",
                params![history_start, location, resource],
            )?;
            0
        };
        let delta: i64 = transaction.query_row(
            "SELECT COALESCE(SUM(physical_delta), 0) FROM empire_resource_delta \
             WHERE location = ?1 AND resource = ?2 AND occurred_at_ms <= ?3",
            params![location, resource, observed_at_ms],
            |row| row.get(0),
        )?;
        let projected = baseline.saturating_add(delta);
        let actual = current
            .resources
            .get(&(location.clone(), resource.clone()))
            .copied()
            .unwrap_or_default();
        let correction = actual.saturating_sub(projected);
        if correction != 0 {
            let event_id = format!("reconcile-resource:{observed_at_ms}:{location}:{resource}");
            transaction.execute(
                "INSERT OR IGNORE INTO empire_resource_delta(\
                    event_id, occurred_at_ms, location, resource, physical_delta, reserved_delta, reason, source\
                 ) VALUES (?1, ?2, ?3, ?4, ?5, 0, 'reconciliation', 'authoritative_snapshot')",
                params![event_id, observed_at_ms, location, resource, correction],
            )?;
        }
    }
    Ok(())
}

fn insert_current_snapshots(
    transaction: &Transaction<'_>,
    current: &CurrentEmpireState,
    observed_at_ms: i64,
) -> Result<(), EmpireTelemetryError> {
    let resource_bucket = bucket_start(observed_at_ms, SNAPSHOT_RESOLUTION_SECONDS);
    transaction.execute(
        "DELETE FROM empire_resource_snapshot WHERE observed_at_ms = ?1 AND resolution_seconds = ?2",
        params![resource_bucket, SNAPSHOT_RESOLUTION_SECONDS],
    )?;
    let reserved = current_reserved_resources(transaction)?;
    let mut keys = current.resources.keys().cloned().collect::<BTreeSet<_>>();
    keys.extend(reserved.keys().cloned());
    for (location, resource) in keys {
        let reported = current
            .resources
            .get(&(location.clone(), resource.clone()))
            .copied()
            .unwrap_or_default();
        let reserved_quantity = reserved
            .get(&(location.clone(), resource.clone()))
            .copied()
            .unwrap_or_default();
        transaction.execute(
            "INSERT INTO empire_resource_snapshot(\
                observed_at_ms, resolution_seconds, location, resource, reported_quantity, reserved_quantity, available_quantity\
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                resource_bucket,
                SNAPSHOT_RESOLUTION_SECONDS,
                location,
                resource,
                reported,
                reserved_quantity,
                reported.saturating_sub(reserved_quantity)
            ],
        )?;
    }
    // Keep the exact observation time for device anchors.  Historical device
    // population is reconstructed backwards from the first authoritative
    // anchor and uses later anchors directly, so rounding the anchor to the
    // start of a ten-minute bucket can place events on the wrong side of it.
    transaction.execute(
        "DELETE FROM empire_device_snapshot WHERE observed_at_ms = ?1 AND resolution_seconds = ?2",
        params![observed_at_ms, SNAPSHOT_RESOLUTION_SECONDS],
    )?;
    for ((device_type, status), count) in &current.device_status {
        transaction.execute(
            "INSERT INTO empire_device_snapshot(\
                observed_at_ms, resolution_seconds, device_type, status, device_count\
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                observed_at_ms,
                SNAPSHOT_RESOLUTION_SECONDS,
                device_type,
                status,
                count
            ],
        )?;
    }
    Ok(())
}

fn current_reserved_resources(
    connection: &Connection,
) -> Result<BTreeMap<(String, String), i64>, EmpireTelemetryError> {
    let mut result = BTreeMap::new();
    let mut statement = connection.prepare(
        "SELECT location, resources_json FROM empire_print_job WHERE completed_event_id IS NULL",
    )?;
    let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?;
    for row in rows {
        let (location, resources_json) = row?;
        let resources: BTreeMap<String, i64> = serde_json::from_str(&resources_json)?;
        for (resource, quantity) in resources {
            *result.entry((location.clone(), resource)).or_default() += quantity;
        }
    }
    Ok(result)
}

fn resource_confidence(
    connection: &Connection,
    location: &str,
    resource: &str,
) -> Result<&'static str, rusqlite::Error> {
    let gaps: i64 = connection.query_row(
        "SELECT COUNT(*) FROM empire_projection_gap \
         WHERE (location = ?1 OR location IS NULL) AND (resource = ?2 OR resource IS NULL) \
           AND kind IN (\
             'ami_mining_quantity_unavailable', 'ami_transport_quantity_unavailable',\
             'trade_net_resources_incomplete', 'print_missing_blueprint', 'print_completion_without_start'\
           )",
        params![location, resource],
        |row| row.get(0),
    )?;
    Ok(if gaps > 0 { "incomplete" } else { "reconstructed" })
}

fn maintain_snapshots(connection: &Connection, now: i64) -> Result<(), rusqlite::Error> {
    compact_resource_snapshots(connection, now)?;
    compact_device_snapshots(connection, now)?;
    set_meta(connection, "empire_last_maintenance_ms", now)?;
    connection.execute_batch("PRAGMA wal_checkpoint(PASSIVE); PRAGMA incremental_vacuum(1000);")?;
    Ok(())
}

fn compact_resource_snapshots(connection: &Connection, now: i64) -> Result<(), rusqlite::Error> {
    let ten_minute_cutoff = now.saturating_sub(TEN_MINUTE_RETENTION_MS);
    connection.execute(
        r#"INSERT OR REPLACE INTO empire_resource_snapshot(
            observed_at_ms, resolution_seconds, location, resource,
            reported_quantity, reserved_quantity, available_quantity
        )
        SELECT hour_ms, 3600, location, resource, reported_quantity, reserved_quantity, available_quantity
        FROM (
            SELECT
                (observed_at_ms / 3600000) * 3600000 AS hour_ms,
                location, resource, reported_quantity, reserved_quantity, available_quantity,
                ROW_NUMBER() OVER (
                    PARTITION BY (observed_at_ms / 3600000), location, resource
                    ORDER BY observed_at_ms DESC
                ) AS row_number
            FROM empire_resource_snapshot
            WHERE resolution_seconds = 600 AND observed_at_ms < ?1
        )
        WHERE row_number = 1"#,
        [ten_minute_cutoff],
    )?;
    connection.execute(
        "DELETE FROM empire_resource_snapshot WHERE resolution_seconds = 600 AND observed_at_ms < ?1",
        [ten_minute_cutoff],
    )?;

    let hourly_cutoff = now.saturating_sub(HOURLY_RETENTION_MS);
    connection.execute(
        r#"INSERT OR REPLACE INTO empire_resource_snapshot(
            observed_at_ms, resolution_seconds, location, resource,
            reported_quantity, reserved_quantity, available_quantity
        )
        SELECT day_ms, 86400, location, resource, reported_quantity, reserved_quantity, available_quantity
        FROM (
            SELECT
                (observed_at_ms / 86400000) * 86400000 AS day_ms,
                location, resource, reported_quantity, reserved_quantity, available_quantity,
                ROW_NUMBER() OVER (
                    PARTITION BY (observed_at_ms / 86400000), location, resource
                    ORDER BY observed_at_ms DESC
                ) AS row_number
            FROM empire_resource_snapshot
            WHERE resolution_seconds = 3600 AND observed_at_ms < ?1
        )
        WHERE row_number = 1"#,
        [hourly_cutoff],
    )?;
    connection.execute(
        "DELETE FROM empire_resource_snapshot WHERE resolution_seconds = 3600 AND observed_at_ms < ?1",
        [hourly_cutoff],
    )?;
    Ok(())
}

fn compact_device_snapshots(connection: &Connection, now: i64) -> Result<(), rusqlite::Error> {
    let ten_minute_cutoff = now.saturating_sub(TEN_MINUTE_RETENTION_MS);
    connection.execute(
        r#"INSERT OR REPLACE INTO empire_device_snapshot(
            observed_at_ms, resolution_seconds, device_type, status, device_count
        )
        SELECT hour_ms, 3600, device_type, status, device_count
        FROM (
            SELECT
                (observed_at_ms / 3600000) * 3600000 AS hour_ms,
                device_type, status, device_count,
                ROW_NUMBER() OVER (
                    PARTITION BY (observed_at_ms / 3600000), device_type, status
                    ORDER BY observed_at_ms DESC
                ) AS row_number
            FROM empire_device_snapshot
            WHERE resolution_seconds = 600 AND observed_at_ms < ?1
        )
        WHERE row_number = 1"#,
        [ten_minute_cutoff],
    )?;
    connection.execute(
        "DELETE FROM empire_device_snapshot WHERE resolution_seconds = 600 AND observed_at_ms < ?1",
        [ten_minute_cutoff],
    )?;

    let hourly_cutoff = now.saturating_sub(HOURLY_RETENTION_MS);
    connection.execute(
        r#"INSERT OR REPLACE INTO empire_device_snapshot(
            observed_at_ms, resolution_seconds, device_type, status, device_count
        )
        SELECT day_ms, 86400, device_type, status, device_count
        FROM (
            SELECT
                (observed_at_ms / 86400000) * 86400000 AS day_ms,
                device_type, status, device_count,
                ROW_NUMBER() OVER (
                    PARTITION BY (observed_at_ms / 86400000), device_type, status
                    ORDER BY observed_at_ms DESC
                ) AS row_number
            FROM empire_device_snapshot
            WHERE resolution_seconds = 3600 AND observed_at_ms < ?1
        )
        WHERE row_number = 1"#,
        [hourly_cutoff],
    )?;
    connection.execute(
        "DELETE FROM empire_device_snapshot WHERE resolution_seconds = 3600 AND observed_at_ms < ?1",
        [hourly_cutoff],
    )?;
    Ok(())
}

fn event_location(event: &HistoryEventRow) -> Option<String> {
    string_field(&event.payload, "location")
        .or_else(|| event.location_id.clone())
        .or_else(|| event.star_id.clone())
}

fn system_designation(value: &str) -> &str {
    value.split_once('-').map_or(value, |(system, _)| system)
}

fn travel_scope(origin: &str, destination: &str) -> &'static str {
    if system_designation(origin) == system_designation(destination) {
        "intra_system"
    } else {
        "inter_system"
    }
}

fn resource_map(value: Option<&Value>) -> BTreeMap<String, i64> {
    value
        .and_then(Value::as_object)
        .map(|object| {
            object
                .iter()
                .filter_map(|(resource, quantity)| {
                    quantity.as_i64().map(|quantity| (resource.clone(), quantity))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value.get(key)?.as_str().map(str::to_owned)
}

fn i64_field(value: &Value, key: &str) -> Option<i64> {
    value.get(key)?.as_i64()
}

fn nested_string(value: Option<&Value>, path: &[&str]) -> Option<String> {
    let mut current = value?;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str().map(str::to_owned)
}

fn nested_i64(value: Option<&Value>, path: &[&str]) -> Option<i64> {
    let mut current = value?;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_i64()
}

fn string_array(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

fn event_millis(event_id: &str) -> Option<i64> {
    event_id
        .split_once('-')
        .and_then(|(milliseconds, _)| milliseconds.parse::<i64>().ok())
}

fn ensure_history_start(
    transaction: &Transaction<'_>,
    observed_at_ms: i64,
) -> Result<(), rusqlite::Error> {
    let current = meta_i64_transaction(transaction, "history_start_ms")?;
    if current.is_none_or(|current| observed_at_ms < current) {
        set_meta_transaction(transaction, "history_start_ms", observed_at_ms)?;
    }
    Ok(())
}

fn meta_i64(connection: &Connection, key: &str) -> Result<Option<i64>, rusqlite::Error> {
    connection
        .query_row(
            "SELECT CAST(value AS INTEGER) FROM empire_meta WHERE key = ?1",
            [key],
            |row| row.get(0),
        )
        .optional()
}

fn meta_i64_transaction(
    transaction: &Transaction<'_>,
    key: &str,
) -> Result<Option<i64>, rusqlite::Error> {
    transaction
        .query_row(
            "SELECT CAST(value AS INTEGER) FROM empire_meta WHERE key = ?1",
            [key],
            |row| row.get(0),
        )
        .optional()
}

fn set_meta(connection: &Connection, key: &str, value: i64) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO empire_meta(key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value.to_string()],
    )?;
    Ok(())
}

fn set_meta_transaction(
    transaction: &Transaction<'_>,
    key: &str,
    value: i64,
) -> Result<(), rusqlite::Error> {
    transaction.execute(
        "INSERT INTO empire_meta(key, value) VALUES (?1, ?2) \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, value.to_string()],
    )?;
    Ok(())
}

fn bucket_start(observed_at_ms: i64, resolution_seconds: i64) -> i64 {
    let resolution_ms = resolution_seconds.saturating_mul(1_000);
    observed_at_ms.div_euclid(resolution_ms) * resolution_ms
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
    use super::*;

    fn history_event(event_id: &str, name: &str, payload: Value) -> HistoryEventRow {
        HistoryEventRow {
            rowid: 1,
            event_id: event_id.to_owned(),
            event_name: name.to_owned(),
            device_code: Some("FACTORY".to_owned()),
            location_id: Some("SCEPTURUM-BELT-1".to_owned()),
            star_id: Some("SCEPTURUM".to_owned()),
            occurred_at_ms: event_millis(event_id).expect("event time"),
            payload,
        }
    }

    fn telemetry() -> Connection {
        let connection = Connection::open_in_memory().expect("telemetry");
        ensure_schema(&connection).expect("empire schema");
        connection
    }

    #[test]
    fn print_reservation_and_consumption_use_the_blueprint_recipe() {
        let mut connection = telemetry();
        record_recipe(
            &connection,
            1_000,
            "freighter",
            &BTreeMap::from([("structural".to_owned(), 800)]),
            &BTreeMap::new(),
            "test",
        )
        .expect("recipe");
        let started = history_event(
            "2000-0",
            "print.started",
            serde_json::json!({"device_type":"freighter"}),
        );
        let completed = history_event(
            "3000-0",
            "print.completed",
            serde_json::json!({"device_type":"freighter","new_device_code":"F1"}),
        );
        {
            let tx = connection.transaction().expect("transaction");
            project_event(&tx, &started).expect("start projection");
            project_event(&tx, &completed).expect("completion projection");
            tx.commit().expect("commit");
        }
        let deltas = connection
            .prepare(
                "SELECT physical_delta, reserved_delta FROM empire_resource_delta \
                 WHERE resource = 'structural' ORDER BY occurred_at_ms",
            )
            .expect("statement")
            .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
            .expect("rows")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");
        assert_eq!(deltas, vec![(0, 800), (-800, -800)]);
    }

    #[test]
    fn print_components_reduce_device_population_when_codes_are_missing() {
        let mut connection = telemetry();
        record_recipe(
            &connection,
            1_000,
            "freighter",
            &BTreeMap::from([("structural".to_owned(), 800)]),
            &BTreeMap::from([("propulsor".to_owned(), 2), ("cargo_pod".to_owned(), 1)]),
            "test",
        )
        .expect("recipe");
        let completed = history_event(
            "3000-0",
            "print.completed",
            serde_json::json!({"device_type":"freighter","new_device_code":"F1"}),
        );
        {
            let tx = connection.transaction().expect("transaction");
            project_event(&tx, &completed).expect("completion projection");
            tx.commit().expect("commit");
        }
        let deltas = connection
            .prepare(
                "SELECT device_type, SUM(delta) FROM empire_device_delta \
                 GROUP BY device_type ORDER BY device_type",
            )
            .expect("statement")
            .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)))
            .expect("rows")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect");
        assert_eq!(
            deltas,
            vec![
                ("cargo_pod".to_owned(), -1),
                ("freighter".to_owned(), 1),
                ("propulsor".to_owned(), -2),
            ]
        );
    }

    #[test]
    fn print_consumed_codes_are_typed_from_the_blueprint_when_identity_is_missing() {
        let mut connection = telemetry();
        record_recipe(
            &connection,
            1_000,
            "freighter",
            &BTreeMap::new(),
            &BTreeMap::from([("propulsor".to_owned(), 2)]),
            "test",
        )
        .expect("recipe");
        let completed = history_event(
            "3000-0",
            "print.completed",
            serde_json::json!({
                "device_type":"freighter",
                "new_device_code":"F1",
                "consumed_device_codes":["P1","P2"]
            }),
        );
        {
            let tx = connection.transaction().expect("transaction");
            project_event(&tx, &completed).expect("completion projection");
            tx.commit().expect("commit");
        }
        let consumed: i64 = connection
            .query_row(
                "SELECT COALESCE(SUM(delta), 0) FROM empire_device_delta \
                 WHERE device_type = 'propulsor' AND reason = 'print_component_consumed'",
                [],
                |row| row.get(0),
            )
            .expect("consumed components");
        assert_eq!(consumed, -2);
    }

    #[test]
    fn event_completed_consumed_devices_reduce_device_population() {
        let mut connection = telemetry();
        let completed = history_event(
            "5000-0",
            "event.completed",
            serde_json::json!({
                "location":"SCEPTURUM-3",
                "event_type":"test",
                "consumed":{
                    "devices":[
                        {"device_code":"D1","device_type":"mining_drone"},
                        {"device_code":"D2","device_type":"mining_drone"}
                    ]
                }
            }),
        );
        {
            let tx = connection.transaction().expect("transaction");
            project_event(&tx, &completed).expect("event projection");
            tx.commit().expect("commit");
        }
        let consumed: i64 = connection
            .query_row(
                "SELECT COALESCE(SUM(delta), 0) FROM empire_device_delta \
                 WHERE device_type = 'mining_drone' AND reason = 'event_consumed'",
                [],
                |row| row.get(0),
            )
            .expect("consumed event devices");
        assert_eq!(consumed, -2);
    }

    #[test]
    fn device_baseline_reanchors_to_authoritative_current_state() {
        let mut connection = telemetry();
        connection
            .execute(
                "INSERT INTO empire_device_delta(\
                    event_id, occurred_at_ms, device_type, device_code, delta, reason, source\
                 ) VALUES ('print-1', 2000, 'ftl_relay', 'R1', 1, 'printed', 'observed')",
                [],
            )
            .expect("device delta");
        connection
            .execute(
                "INSERT INTO empire_device_delta(\
                    event_id, occurred_at_ms, device_type, device_code, delta, reason, source\
                 ) VALUES ('print-2', 3000, 'ftl_relay', 'R2', 1, 'printed', 'observed')",
                [],
            )
            .expect("device delta");
        let mut current = CurrentEmpireState::default();
        current.device_totals.insert("ftl_relay".to_owned(), 5);
        {
            let tx = connection.transaction().expect("transaction");
            reanchor_device_baselines(&tx, &current, 1_000, 5_000).expect("anchor");
            tx.commit().expect("commit");
        }
        let baseline: i64 = connection
            .query_row(
                "SELECT total_count FROM empire_device_baseline WHERE device_type = 'ftl_relay'",
                [],
                |row| row.get(0),
            )
            .expect("baseline");
        assert_eq!(baseline, 3);

        current.device_totals.insert("ftl_relay".to_owned(), 6);
        {
            let tx = connection.transaction().expect("transaction");
            reanchor_device_baselines(&tx, &current, 1_000, 6_000).expect("re-anchor");
            tx.commit().expect("commit");
        }
        let baseline: i64 = connection
            .query_row(
                "SELECT total_count FROM empire_device_baseline WHERE device_type = 'ftl_relay'",
                [],
                |row| row.get(0),
            )
            .expect("re-anchored baseline");
        let corrections: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM empire_device_delta WHERE reason = 'reconciliation'",
                [],
                |row| row.get(0),
            )
            .expect("reconciliation rows");
        assert_eq!(baseline, 4);
        assert_eq!(corrections, 0);
    }

    #[test]
    fn relay_baseline_reanchors_without_fake_lifecycle_events() {
        let mut connection = telemetry();
        connection
            .execute(
                "INSERT INTO empire_infrastructure_delta(\
                    event_id, occurred_at_ms, kind, delta, reason, source\
                 ) VALUES ('relay-1', 2000, 'active_ftl_relay', 1, 'relay_activated', 'observed')",
                [],
            )
            .expect("relay delta");
        let mut current = CurrentEmpireState::default();
        current
            .active_infrastructure
            .insert(ACTIVE_FTL_RELAY.to_owned(), 4);
        current
            .active_infrastructure
            .insert(ACTIVE_DEEP_SPACE_RELAY_STATION.to_owned(), 2);
        current
            .active_infrastructure
            .insert(ACTIVE_FTL_BEACON.to_owned(), 7);
        {
            let tx = connection.transaction().expect("transaction");
            reanchor_infrastructure_baseline(&tx, &current, 1_000, 5_000).expect("anchor");
            tx.commit().expect("commit");
        }
        let baseline: i64 = connection
            .query_row(
                "SELECT active_count FROM empire_infrastructure_baseline WHERE kind = 'active_ftl_relay'",
                [],
                |row| row.get(0),
            )
            .expect("relay baseline");
        let dsr_baseline: i64 = connection
            .query_row(
                "SELECT active_count FROM empire_infrastructure_baseline \
                 WHERE kind = 'active_deep_space_relay_station'",
                [],
                |row| row.get(0),
            )
            .expect("DSR baseline");
        let beacon_baseline: i64 = connection
            .query_row(
                "SELECT active_count FROM empire_infrastructure_baseline \
                 WHERE kind = 'active_ftl_beacon'",
                [],
                |row| row.get(0),
            )
            .expect("beacon baseline");
        let corrections: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM empire_infrastructure_delta WHERE reason = 'reconciliation'",
                [],
                |row| row.get(0),
            )
            .expect("reconciliation rows");
        assert_eq!(baseline, 3);
        assert_eq!(dsr_baseline, 2);
        assert_eq!(beacon_baseline, 7);
        assert_eq!(corrections, 0);
    }

    #[test]
    fn relay_activation_uses_the_historical_device_type() {
        let mut connection = telemetry();
        upsert_identity(
            &connection,
            "DSR1",
            "deep_space_relay_station",
            1_000,
            "test",
        )
        .expect("identity");
        let mut activated = history_event("2000-0", "relay.activated", serde_json::json!({}));
        activated.device_code = Some("DSR1".to_owned());
        {
            let tx = connection.transaction().expect("transaction");
            project_event(&tx, &activated).expect("event projection");
            tx.commit().expect("commit");
        }
        let kind: String = connection
            .query_row(
                "SELECT kind FROM empire_infrastructure_delta WHERE event_id = ?1",
                [activated.event_id],
                |row| row.get(0),
            )
            .expect("infrastructure kind");
        assert_eq!(kind, ACTIVE_DEEP_SPACE_RELAY_STATION);
    }

    #[test]
    fn beacon_deploy_and_stow_change_active_infrastructure() {
        let mut connection = telemetry();
        upsert_identity(&connection, "B1", "ftl_beacon", 1_000, "test").expect("identity");
        let mut deployed = history_event("2000-0", "device.deployed", serde_json::json!({}));
        deployed.device_code = Some("B1".to_owned());
        let mut stowed = history_event("3000-0", "device.stowed", serde_json::json!({}));
        stowed.device_code = Some("B1".to_owned());
        {
            let tx = connection.transaction().expect("transaction");
            project_event(&tx, &deployed).expect("deploy projection");
            project_event(&tx, &stowed).expect("stow projection");
            tx.commit().expect("commit");
        }
        let total: i64 = connection
            .query_row(
                "SELECT COALESCE(SUM(delta), 0) FROM empire_infrastructure_delta \
                 WHERE kind = 'active_ftl_beacon'",
                [],
                |row| row.get(0),
            )
            .expect("beacon delta");
        assert_eq!(total, 0);
    }

    #[test]
    fn ami_mining_records_an_explicit_quantity_gap() {
        let mut connection = telemetry();
        let event = history_event(
            "4000-0",
            "ami.mining.digest",
            serde_json::json!({
                "activity":{"counts":{"mining.stopped":3}},
                "report":{"location":"SCEPTURUM-BELT-1"}
            }),
        );
        {
            let tx = connection.transaction().expect("transaction");
            project_event(&tx, &event).expect("projection");
            tx.commit().expect("commit");
        }
        let gaps: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM empire_projection_gap WHERE kind = 'ami_mining_quantity_unavailable'",
                [],
                |row| row.get(0),
            )
            .expect("gap count");
        let sessions: i64 = connection
            .query_row(
                "SELECT value FROM empire_activity WHERE metric = 'mining_sessions_completed'",
                [],
                |row| row.get(0),
            )
            .expect("session count");
        assert_eq!(gaps, 1);
        assert_eq!(sessions, 3);
    }

    #[test]
    fn inter_system_travel_is_derived_from_designations() {
        assert_eq!(travel_scope("SCEPTURUM-7-L4", "SCEPTURUM-BELT-1"), "intra_system");
        assert_eq!(travel_scope("SCEPTURUM-7-L4", "THYFFAWFF-3"), "inter_system");
    }
}
