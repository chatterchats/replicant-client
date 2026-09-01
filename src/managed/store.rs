//! Crate-private SQLite persistence for normalized managed state.

#![allow(dead_code)] // Later managed engines own the remaining journals.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
    mpsc,
};
use std::time::{Duration, Instant};

#[cfg(test)]
std::thread_local! {
    static INTERRUPT_NEXT_MIGRATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

use rusqlite::{
    Connection, OptionalExtension, Transaction, TransactionBehavior, params, params_from_iter,
    types::Value as SqlValue,
};
use serde_json::Value;
use tokio::sync::{Mutex as TokioMutex, mpsc as tokio_mpsc, oneshot};
use tracing::{Span, debug, info, warn};

use super::refresh::{
    RefreshDelta, RefreshMode, RefreshPhase, RefreshPhaseState, RefreshPhaseStatus,
    RefreshReadiness, RefreshRunId, RefreshRunState, RefreshRunStatus,
};
use crate::domain::{
    Account, AccountId, Blueprint, Device, DeviceId, DeviceKey, Event, IncomingObject, Inventory,
    InventoryOwner, Location, LocationEvent, LocationId, LocationKey, Message, Observation,
    ObservationMetadata, ObservationTime, Realm, Replicant, ReplicantId, ReplicantKey,
    ResourceSite, Simulation, SimulationId, Star, StarId, StarKey, StarKnowledge, Trade,
};

const INITIAL_SCHEMA: &str = include_str!("../../migrations/0001_initial.sql");
const DEVICE_RELATIONSHIP_SEMANTICS_SCHEMA: &str =
    include_str!("../../migrations/0002_device_relationship_semantics.sql");
const RECONCILIATION_LEADER_SCHEMA: &str =
    include_str!("../../migrations/0003_reconciliation_leader.sql");
const HISTORY_SPLIT_SCHEMA: &str = include_str!("../../migrations/0004_history_split.sql");
const EVENT_PROJECTION_METADATA_SCHEMA: &str =
    include_str!("../../migrations/0006_event_projection_metadata.sql");
const MESSAGE_METADATA_SCHEMA: &str = include_str!("../../migrations/0005_message_metadata.sql");
const HISTORY_INITIAL_SCHEMA: &str = include_str!("../../migrations/history/0001_initial.sql");
const HISTORY_INDEX_SCHEMA: &str = include_str!("../../migrations/history/0002_indexes.sql");
const HISTORY_REFRESH_SCHEMA: &str =
    include_str!("../../migrations/history/0003_refresh_archive.sql");
const REFRESH_SCHEMA: &str = include_str!("../../migrations/0007_refresh.sql");
const MESSAGE_METADATA_REVISION_SCHEMA: &str =
    include_str!("../../migrations/0008_message_metadata_revision.sql");
const CURRENT_SCHEMA_VERSION: i64 = 8;
const CURRENT_HISTORY_SCHEMA_VERSION: i64 = 2;
const REFRESH_LEASE_MILLIS: i64 = 300_000;
const OPERATION_TERMINAL_RETENTION_DAYS: i64 = 30;
const HISTORY_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
    #[error("SQLite failure: {0}")]
    Sql(#[from] rusqlite::Error),
    #[error("database directory failure: {0}")]
    Io(#[from] std::io::Error),
    #[error("state serialization failure: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("store is bound to account {stored_account_id}, not {supplied_account_id}")]
    AccountMismatch {
        stored_account_id: String,
        supplied_account_id: String,
    },
    #[error("injected commit failure")]
    InjectedCommitFailure,
    #[error("injected migration interruption")]
    InjectedMigrationInterruption,
    #[error("store is closed")]
    Closed,
    #[error("store worker queue is full")]
    Backpressure,
    #[error("store worker could not start: {0}")]
    WorkerStart(#[source] std::io::Error),
    #[error("invalid Redis stream event ID: {0}")]
    InvalidEventId(String),
    #[error("unsupported event projection tombstone kind: {0}")]
    UnsupportedProjectionKind(&'static str),
    #[error("database schema version {found} is newer than supported version {supported}")]
    UnsupportedSchemaVersion { found: i64, supported: i64 },
    #[error("history database schema version {found} is newer than supported version {supported}")]
    UnsupportedHistorySchemaVersion { found: i64, supported: i64 },
    #[error("durable refresh state failure: {0}")]
    Refresh(String),
}

/// Internal durable store. No database handle crosses the crate boundary.
pub(crate) struct Store {
    connection: Connection,
    history: Connection,
    last_history_maintenance: Instant,
    #[cfg(test)]
    fail_next_commit: bool,
}

/// The one serialized, bounded execution path to SQLite. The connection is
/// created, used, flushed, and dropped only by its dedicated OS thread.
#[derive(Clone)]
pub(crate) struct StoreHandle {
    sender: tokio_mpsc::Sender<StoreCommand>,
    accepting: Arc<AtomicBool>,
    close: Arc<TokioMutex<Option<CloseResponse>>>,
}

/// Compatibility facade for the remaining synchronous state methods. It is
/// intentionally not a database guard: each method sends its work to the
/// dedicated worker and waits for that worker's response.
pub(crate) struct StoreProxy(StoreHandle);

const STORE_QUEUE_CAPACITY: usize = 64;
type CloseResponse = oneshot::Receiver<Result<(), StoreError>>;
type CatalogueRows = (BTreeMap<StarKey, Observation<Star>>, Option<String>);

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MessageMetadata {
    pub(crate) last_cursor: Option<i64>,
    pub(crate) unread_count: Option<i64>,
    pub(crate) refreshed_at: Option<ObservationTime>,
    pub(crate) revision: u64,
    pub(crate) last_error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProjectionTombstone {
    pub(crate) realm: Realm,
    pub(crate) kind: &'static str,
    pub(crate) item_id: String,
    pub(crate) evidence: &'static str,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReconciliationTarget {
    pub(crate) work_id: String,
    pub(crate) realm: Realm,
    pub(crate) kind: &'static str,
    pub(crate) payload: Value,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct EventProjectionBatch {
    pub(crate) devices: Vec<Observation<Device>>,
    pub(crate) replicants: Vec<Observation<Replicant>>,
    pub(crate) locations: Vec<Observation<Location>>,
    pub(crate) stars: Vec<Observation<Star>>,
    pub(crate) resource_sites: Vec<Observation<ResourceSite>>,
    pub(crate) location_events: Vec<Observation<LocationEvent>>,
    pub(crate) incoming_objects: Vec<Observation<IncomingObject>>,
    pub(crate) messages: Vec<Observation<Message>>,
    pub(crate) blueprints: Vec<Observation<Blueprint>>,
    pub(crate) trades: Vec<Observation<Trade>>,
    pub(crate) simulations: Vec<Observation<Simulation>>,
    pub(crate) deletions: Vec<ProjectionTombstone>,
    pub(crate) reconciliation: Vec<ReconciliationTarget>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProjectionReplayState {
    pub(crate) last_history_rowid: i64,
    pub(crate) high_water_rowid: i64,
    pub(crate) complete: bool,
}

enum StoreCommand {
    Execute {
        id: u64,
        operation_type: &'static str,
        /// Starts when submission begins; queue_wait telemetry is submission-to-start.
        queued_at: Instant,
        span: Span,
        dispatcher: tracing::Dispatch,
        command: Box<dyn FnOnce(&mut Store) + Send + 'static>,
    },
    Close(oneshot::Sender<Result<(), StoreError>>),
}

static NEXT_STORE_COMMAND_ID: AtomicU64 = AtomicU64::new(1);

impl StoreHandle {
    pub(crate) fn lock(&self) -> StoreProxy {
        StoreProxy(self.clone())
    }
    pub(crate) async fn open_memory() -> Result<Self, StoreError> {
        tokio::task::spawn_blocking(|| Self::start(None))
            .await
            .map_err(|_| StoreError::Closed)?
    }

    pub(crate) async fn open_file(path: PathBuf) -> Result<Self, StoreError> {
        tokio::task::spawn_blocking(move || Self::start(Some(path)))
            .await
            .map_err(|_| StoreError::Closed)?
    }

    pub(crate) fn open_memory_blocking() -> Result<Self, StoreError> {
        Self::start(None)
    }

    pub(crate) fn open_file_blocking(path: PathBuf) -> Result<Self, StoreError> {
        Self::start(Some(path))
    }

    fn start(path: Option<PathBuf>) -> Result<Self, StoreError> {
        let storage = path
            .as_ref()
            .map_or_else(|| "memory".to_owned(), |path| path.display().to_string());
        let (ready_sender, ready_receiver) = mpsc::sync_channel(1);
        let (sender, mut receiver) = tokio_mpsc::channel(STORE_QUEUE_CAPACITY);
        std::thread::Builder::new()
            .name("replicant-store".into())
            .spawn(move || {
                let open_started = Instant::now();
                info!(
                    target: "replicant_client::store",
                    event = "store.open_started",
                    storage = %storage,
                    queue_capacity = STORE_QUEUE_CAPACITY,
                    "opening SQLite store worker"
                );
                let opened = match path {
                    Some(path) => Store::open_file(&path),
                    None => Store::open_memory(),
                };
                let mut store = match opened {
                    Ok(store) => {
                        info!(
                            target: "replicant_client::store",
                            event = "store.open_completed",
                            storage = %storage,
                            elapsed_ms = open_started.elapsed().as_millis() as u64,
                            "SQLite store worker opened"
                        );
                        store
                    }
                    Err(error) => {
                        warn!(
                            target: "replicant_client::store",
                            event = "store.open_failed",
                            storage = %storage,
                            elapsed_ms = open_started.elapsed().as_millis() as u64,
                            error = %error,
                            "SQLite store worker failed to open"
                        );
                        let _ = ready_sender.send(Err(error));
                        return;
                    }
                };
                let _ = ready_sender.send(Ok(()));
                while let Some(command) = receiver.blocking_recv() {
                    match command {
                        StoreCommand::Execute {
                            id,
                            operation_type,
                            queued_at,
                            span,
                            dispatcher,
                            command,
                        } => {
                            let queue_wait = queued_at.elapsed();
                            tracing::dispatcher::with_default(&dispatcher, || {
                                let _span_guard = span.enter();
                                let execute_started = Instant::now();
                                command(&mut store);
                                let execute = execute_started.elapsed();
                                let elapsed = queued_at.elapsed();
                                if queue_wait >= Duration::from_secs(1)
                                    || execute >= Duration::from_secs(1)
                                {
                                    warn!(
                                        target: "replicant_client::store",
                                        event = "store.command_slow",
                                        command_id = id,
                                        operation_type,
                                        queue_wait_ms = queue_wait.as_millis() as u64,
                                        execute_ms = execute.as_millis() as u64,
                                        elapsed_ms = elapsed.as_millis() as u64,
                                        "SQLite store command exceeded responsiveness threshold"
                                    );
                                } else {
                                    debug!(
                                        target: "replicant_client::store",
                                        event = "store.command_completed",
                                        command_id = id,
                                        operation_type,
                                        queue_wait_ms = queue_wait.as_millis() as u64,
                                        execute_ms = execute.as_millis() as u64,
                                        elapsed_ms = elapsed.as_millis() as u64,
                                        "SQLite store command completed"
                                    );
                                }
                            });
                        }
                        StoreCommand::Close(response) => {
                            let close_started = Instant::now();
                            let result = store.flush();
                            info!(
                                target: "replicant_client::store",
                                event = "store.close_completed",
                                elapsed_ms = close_started.elapsed().as_millis() as u64,
                                success = result.is_ok(),
                                "SQLite store worker flushed and closed"
                            );
                            let _ = response.send(result);
                            break;
                        }
                    }
                }
            })
            .map_err(StoreError::WorkerStart)?;
        ready_receiver.recv().map_err(|_| StoreError::Closed)??;
        Ok(Self {
            sender,
            accepting: Arc::new(AtomicBool::new(true)),
            close: Arc::new(TokioMutex::new(None)),
        })
    }

    pub(crate) async fn execute<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store) -> Result<T, StoreError> + Send + 'static,
    {
        if !self.accepting.load(AtomicOrdering::Acquire) {
            return Err(StoreError::Closed);
        }
        let id = NEXT_STORE_COMMAND_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let operation_type = std::any::type_name::<F>();
        let queued_at = Instant::now();
        let span = Span::current();
        let dispatcher = tracing::dispatcher::get_default(Clone::clone);
        let (response_sender, response_receiver) = oneshot::channel();
        self.sender
            .send(StoreCommand::Execute {
                id,
                operation_type,
                queued_at,
                span,
                dispatcher,
                command: Box::new(move |store| {
                    let _ = response_sender.send(operation(store));
                }),
            })
            .await
            .map_err(|_| StoreError::Closed)?;
        response_receiver.await.map_err(|_| StoreError::Closed)?
    }

    /// Synchronous compatibility bridge for startup and test-only callers.
    /// Async managed flows use [`Self::execute`]; this never touches SQLite or
    /// a store mutex on the caller thread.
    pub(crate) fn execute_blocking<T, F>(&self, operation: F) -> Result<T, StoreError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store) -> Result<T, StoreError> + Send + 'static,
    {
        if !self.accepting.load(AtomicOrdering::Acquire) {
            return Err(StoreError::Closed);
        }
        let id = NEXT_STORE_COMMAND_ID.fetch_add(1, AtomicOrdering::Relaxed);
        let operation_type = std::any::type_name::<F>();
        let queued_at = Instant::now();
        let span = Span::current();
        let dispatcher = tracing::dispatcher::get_default(Clone::clone);
        let (response_sender, response_receiver) = mpsc::sync_channel(1);
        self.sender
            .try_send(StoreCommand::Execute {
                id,
                operation_type,
                queued_at,
                span,
                dispatcher,
                command: Box::new(move |store| {
                    let _ = response_sender.send(operation(store));
                }),
            })
            .map_err(|error| match error {
                tokio_mpsc::error::TrySendError::Full(_) => StoreError::Backpressure,
                tokio_mpsc::error::TrySendError::Closed(_) => StoreError::Closed,
            })?;
        response_receiver.recv().map_err(|_| StoreError::Closed)?
    }

    pub(crate) async fn close(&self) -> Result<(), StoreError> {
        self.accepting.store(false, AtomicOrdering::Release);
        let mut close = self.close.lock().await;
        if close.is_none() {
            let (response_sender, response_receiver) = oneshot::channel();
            self.sender
                .send(StoreCommand::Close(response_sender))
                .await
                .map_err(|_| StoreError::Closed)?;
            *close = Some(response_receiver);
        }
        if let Some(response) = close.take() {
            return response.await.map_err(|_| StoreError::Closed)?;
        }
        Err(StoreError::Closed)
    }
}

impl StoreProxy {
    pub(crate) fn as_ref(&self) -> Option<&Self> {
        Some(self)
    }
    pub(crate) fn as_mut(&mut self) -> Option<&mut Self> {
        Some(self)
    }

    pub(crate) fn bind_account(&mut self, account_id: &AccountId) -> Result<(), StoreError> {
        let account_id = account_id.clone();
        self.0
            .execute_blocking(move |store| store.bind_account(&account_id))
    }
    pub(crate) fn bound_account_id(&self) -> Result<Option<String>, StoreError> {
        self.0.execute_blocking(|store| store.bound_account_id())
    }
    pub(crate) fn rebind_account_and_persist(
        &mut self,
        previous: &AccountId,
        account: &Observation<Account>,
    ) -> Result<(), StoreError> {
        let previous = previous.clone();
        let account = account.clone();
        self.0
            .execute_blocking(move |store| store.rebind_account_and_persist(&previous, &account))
    }
    pub(crate) fn persist_account(
        &mut self,
        value: &Observation<Account>,
    ) -> Result<(), StoreError> {
        let value = value.clone();
        self.0.execute_blocking(move |s| s.persist_account(&value))
    }
    pub(crate) fn persist_replicant(
        &mut self,
        value: &Observation<Replicant>,
    ) -> Result<(), StoreError> {
        let value = value.clone();
        self.0
            .execute_blocking(move |s| s.persist_replicant(&value))
    }
    pub(crate) fn replace_catalogue(
        &mut self,
        stars: &[Observation<Star>],
        generated_at: Option<&str>,
    ) -> Result<(), StoreError> {
        let stars = stars.to_vec();
        let generated_at = generated_at.map(str::to_owned);
        self.0
            .execute_blocking(move |s| s.replace_catalogue(&stars, generated_at.as_deref()))
    }
    pub(crate) fn persist_stars(&mut self, values: &[Observation<Star>]) -> Result<(), StoreError> {
        let values = values.to_vec();
        self.0.execute_blocking(move |s| s.persist_stars(&values))
    }
    pub(crate) fn persist_location(
        &mut self,
        value: &Observation<Location>,
    ) -> Result<(), StoreError> {
        let value = value.clone();
        self.0.execute_blocking(move |s| s.persist_location(&value))
    }
    pub(crate) fn persist_devices(
        &mut self,
        values: &[Observation<Device>],
    ) -> Result<(), StoreError> {
        let values = values.to_vec();
        self.0.execute_blocking(move |s| s.persist_devices(&values))
    }
    pub(crate) fn persist_devices_and_touch(
        &mut self,
        changed: &[Observation<Device>],
        touches: &[(DeviceKey, crate::domain::ObservationTime)],
    ) -> Result<(), StoreError> {
        let changed = changed.to_vec();
        let touches = touches.to_vec();
        self.0
            .execute_blocking(move |s| s.persist_devices_and_touch(&changed, &touches))
    }
    pub(crate) fn persist_simulation_and_devices(
        &mut self,
        simulation: &Observation<Simulation>,
        devices: &[Observation<Device>],
    ) -> Result<(), StoreError> {
        let simulation = simulation.clone();
        let devices = devices.to_vec();
        self.0
            .execute_blocking(move |s| s.persist_simulation_and_devices(&simulation, &devices))
    }
    pub(crate) fn reconcile_owned_devices(
        &mut self,
        values: &BTreeSet<DeviceKey>,
    ) -> Result<(), StoreError> {
        let values = values.clone();
        self.0
            .execute_blocking(move |s| s.reconcile_owned_devices(&values))
    }
    pub(crate) fn purge_realm_devices(
        &mut self,
        realm: &Realm,
    ) -> Result<Vec<DeviceKey>, StoreError> {
        let realm = realm.clone();
        self.0
            .execute_blocking(move |s| s.purge_realm_devices(&realm))
    }
    pub(crate) fn persist_simulation(
        &mut self,
        value: &Observation<Simulation>,
    ) -> Result<(), StoreError> {
        let value = value.clone();
        self.0
            .execute_blocking(move |s| s.persist_simulation(&value))
    }
    pub(crate) fn persist_inventory(
        &mut self,
        value: &Observation<Inventory>,
    ) -> Result<(), StoreError> {
        let value = value.clone();
        self.0
            .execute_blocking(move |s| s.persist_inventory(&value))
    }
    pub(crate) fn persist_messages(
        &mut self,
        values: &[Observation<Message>],
    ) -> Result<(), StoreError> {
        let values = values.to_vec();
        self.0
            .execute_blocking(move |s| s.persist_messages(&values))
    }
    pub(crate) fn commit_messages_and_metadata(
        &mut self,
        messages: &[Observation<Message>],
        metadata: MessageMetadata,
    ) -> Result<(), StoreError> {
        let messages = messages.to_vec();
        self.0
            .execute_blocking(move |s| s.commit_messages_and_metadata(&messages, metadata))
    }
    pub(crate) fn persist_message_error(&mut self, error: &str) -> Result<(), StoreError> {
        let error = error.to_owned();
        self.0
            .execute_blocking(move |s| s.persist_message_error(&error))
    }
    pub(crate) fn restore_messages(&self) -> Result<Vec<Observation<Message>>, StoreError> {
        self.0.execute_blocking(|store| store.restore_messages())
    }
    pub(crate) fn restore_resource_sites(
        &self,
    ) -> Result<Vec<Observation<ResourceSite>>, StoreError> {
        self.0
            .execute_blocking(|store| store.restore_resource_sites())
    }
    pub(crate) fn restore_location_events(
        &self,
    ) -> Result<Vec<Observation<LocationEvent>>, StoreError> {
        self.0
            .execute_blocking(|store| store.restore_location_events())
    }
    pub(crate) fn restore_incoming_objects(
        &self,
    ) -> Result<Vec<Observation<IncomingObject>>, StoreError> {
        self.0
            .execute_blocking(|store| store.restore_incoming_objects())
    }
    pub(crate) fn message_metadata(&self) -> Result<MessageMetadata, StoreError> {
        self.0.execute_blocking(|store| store.message_metadata())
    }
    pub(crate) fn persist_message_metadata(
        &mut self,
        metadata: MessageMetadata,
    ) -> Result<(), StoreError> {
        self.0
            .execute_blocking(move |s| s.persist_message_metadata(metadata))
    }
    pub(crate) fn has_event(&self, id: &str) -> Result<bool, StoreError> {
        let id = id.to_owned();
        self.0.execute_blocking(move |s| s.has_event(&id))
    }
    pub(crate) fn event_cursor(&self) -> Result<Option<String>, StoreError> {
        self.0.execute_blocking(|s| s.event_cursor())
    }
    pub(crate) fn read_events(
        &self,
        after: Option<String>,
        device_code: Option<String>,
        event_name: Option<String>,
        latest: Option<usize>,
    ) -> Result<Vec<Event>, StoreError> {
        self.0.execute_blocking(move |store| {
            store.read_events(
                after.as_deref(),
                device_code.as_deref(),
                event_name.as_deref(),
                latest,
            )
        })
    }
    pub(crate) fn prepare_projection_replay(
        &mut self,
        projection: &str,
        version: i64,
    ) -> Result<ProjectionReplayState, StoreError> {
        let projection = projection.to_owned();
        self.0
            .execute_blocking(move |store| store.prepare_projection_replay(&projection, version))
    }
    pub(crate) fn read_projection_history(
        &self,
        after_rowid: i64,
        high_water_rowid: i64,
        limit: usize,
    ) -> Result<Vec<(i64, Event)>, StoreError> {
        self.0.execute_blocking(move |store| {
            store.read_projection_history(after_rowid, high_water_rowid, limit)
        })
    }
    pub(crate) fn apply_replay_projection(
        &mut self,
        projection: &str,
        version: i64,
        last_history_rowid: i64,
        high_water_rowid: i64,
        batch: &EventProjectionBatch,
    ) -> Result<(), StoreError> {
        let projection = projection.to_owned();
        let batch = batch.clone();
        self.0.execute_blocking(move |store| {
            store.apply_replay_projection(
                &projection,
                version,
                last_history_rowid,
                high_water_rowid,
                &batch,
            )
        })
    }
    pub(crate) fn complete_projection_replay(
        &mut self,
        projection: &str,
        version: i64,
        high_water_rowid: i64,
    ) -> Result<(), StoreError> {
        let projection = projection.to_owned();
        self.0.execute_blocking(move |store| {
            store.complete_projection_replay(&projection, version, high_water_rowid)
        })
    }
    pub(crate) fn set_event_cursor(&mut self, cursor: &str) -> Result<(), StoreError> {
        let cursor = cursor.to_owned();
        self.0
            .execute_blocking(move |s| s.set_event_cursor(&cursor))
    }
    pub(crate) fn event_cursor_is_stale(&self, threshold: i64) -> Result<bool, StoreError> {
        self.0
            .execute_blocking(move |s| s.event_cursor_is_stale(threshold))
    }
    #[cfg(test)]
    pub(crate) fn backdate_event_cursor(&mut self, seconds: i64) -> Result<(), StoreError> {
        self.0
            .execute_blocking(move |s| s.backdate_event_cursor(seconds))
    }
    pub(crate) fn apply_event_projection(
        &mut self,
        event: &Event,
        cursor: &str,
        batch: &EventProjectionBatch,
    ) -> Result<bool, StoreError> {
        let event = event.clone();
        let cursor = cursor.to_owned();
        let batch = batch.clone();
        self.0
            .execute_blocking(move |store| store.apply_event_projection(&event, &cursor, &batch))
    }
    pub(crate) fn enqueue_reconciliation(
        &mut self,
        id: &str,
        realm: &Realm,
        kind: &str,
        payload: &Value,
    ) -> Result<(), StoreError> {
        let id = id.to_owned();
        let realm = realm.clone();
        let kind = kind.to_owned();
        let payload = payload.clone();
        self.0
            .execute_blocking(move |s| s.enqueue_reconciliation(&id, &realm, &kind, &payload))
    }
    pub(crate) fn acquire_reconciliation_leadership(
        &mut self,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, StoreError> {
        let owner = owner.to_owned();
        self.0
            .execute_blocking(move |s| s.acquire_reconciliation_leadership(&owner, lease_seconds))
    }

    pub(crate) fn claim_reconciliation_work(
        &mut self,
    ) -> Result<Option<ReconciliationWork>, StoreError> {
        self.0.execute_blocking(|s| s.claim_reconciliation_work())
    }
    pub(crate) fn complete_reconciliation_work(&mut self, id: &str) -> Result<(), StoreError> {
        let id = id.to_owned();
        self.0
            .execute_blocking(move |s| s.complete_reconciliation_work(&id))
    }
    pub(crate) fn retry_reconciliation_work(&mut self, id: &str) -> Result<(), StoreError> {
        let id = id.to_owned();
        self.0
            .execute_blocking(move |s| s.retry_reconciliation_work(&id))
    }
    pub(crate) fn record_operation(
        &mut self,
        id: &str,
        state: &str,
        realm: Option<&str>,
        kind: Option<&str>,
        target: Option<&str>,
        intent: &Value,
    ) -> Result<(), StoreError> {
        let id = id.to_owned();
        let state = state.to_owned();
        let realm = realm.map(str::to_owned);
        let kind = kind.map(str::to_owned);
        let target = target.map(str::to_owned);
        let intent = intent.clone();
        self.0.execute_blocking(move |s| {
            s.record_operation(
                &id,
                &state,
                realm.as_deref(),
                kind.as_deref(),
                target.as_deref(),
                &intent,
            )
        })
    }
    /// Atomically records a new operation only when `operation_id` is absent.
    /// Returns the existing untouched journal entry when another invocation
    /// already owns the ID; existing rows are never overwritten.
    pub(crate) fn record_operation_if_absent(
        &mut self,
        id: &str,
        state: &str,
        realm: Option<&str>,
        kind: Option<&str>,
        target: Option<&str>,
        intent: &Value,
    ) -> Result<Option<OperationJournalEntry>, StoreError> {
        let id = id.to_owned();
        let state = state.to_owned();
        let realm = realm.map(str::to_owned);
        let kind = kind.map(str::to_owned);
        let target = target.map(str::to_owned);
        let intent = intent.clone();
        self.0.execute_blocking(move |s| {
            s.record_operation_if_absent(
                &id,
                &state,
                realm.as_deref(),
                kind.as_deref(),
                target.as_deref(),
                &intent,
            )
        })
    }
    pub(crate) fn set_operation_state(&mut self, id: &str, state: &str) -> Result<(), StoreError> {
        let id = id.to_owned();
        let state = state.to_owned();
        self.0
            .execute_blocking(move |s| s.set_operation_state(&id, &state))
    }
    pub(crate) fn claim_operation_submission(
        &mut self,
        id: &str,
        attempt: &str,
    ) -> Result<bool, StoreError> {
        let id = id.to_owned();
        let attempt = attempt.to_owned();
        self.0
            .execute_blocking(move |s| s.claim_operation_submission(&id, &attempt))
    }
    pub(crate) fn record_operation_and_project(
        &mut self,
        id: &str,
        state: &str,
        devices: &[Observation<Device>],
    ) -> Result<(), StoreError> {
        let id = id.to_owned();
        let state = state.to_owned();
        let devices = devices.to_vec();
        self.0
            .execute_blocking(move |s| s.record_operation_and_project(&id, &state, &devices))
    }
    pub(crate) fn append_operation_projection(
        &mut self,
        id: &str,
        state: &str,
        value: &Value,
    ) -> Result<(), StoreError> {
        let id = id.to_owned();
        let state = state.to_owned();
        let value = value.clone();
        self.0
            .execute_blocking(move |s| s.append_operation_projection(&id, &state, &value))
    }
    pub(crate) fn read_operation(
        &self,
        id: &str,
    ) -> Result<Option<OperationJournalEntry>, StoreError> {
        let id = id.to_owned();
        self.0.execute_blocking(move |s| s.read_operation(&id))
    }
    pub(crate) fn promote_crashed_submissions(&mut self) -> Result<usize, StoreError> {
        self.0.execute_blocking(|s| s.promote_crashed_submissions())
    }
    pub(crate) fn list_unresolved_operations(
        &self,
    ) -> Result<Vec<(String, OperationJournalEntry)>, StoreError> {
        self.0.execute_blocking(|s| s.list_unresolved_operations())
    }
    pub(crate) fn find_operations_awaiting_evidence(
        &self,
        realm: &str,
        kind: &str,
        target: &str,
    ) -> Result<Vec<String>, StoreError> {
        let realm = realm.to_owned();
        let kind = kind.to_owned();
        let target = target.to_owned();
        self.0
            .execute_blocking(move |s| s.find_operations_awaiting_evidence(&realm, &kind, &target))
    }
    #[cfg(test)]
    pub(crate) fn fail_next_commit(&mut self) {
        let _ = self.0.execute_blocking(|s| {
            s.fail_next_commit();
            Ok(())
        });
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OperationJournalEntry {
    pub(crate) state: String,
    pub(crate) target_realm: Option<String>,
    pub(crate) target_kind: Option<String>,
    pub(crate) target_id: Option<String>,
    pub(crate) intent: Value,
    pub(crate) projection: Option<Value>,
    pub(crate) submission_attempt_id: Option<String>,
    pub(crate) submitted_at: Option<String>,
    pub(crate) submission_cursor: Option<String>,
}

/// States an operation can never automatically leave without external
/// evidence, a caller-requested reconciliation, or a fresh restart-recovery
/// decision. Used to scope both restart recovery and evidence-matching scans.
const OPERATION_TERMINAL_STATES: [&str; 4] = ["completed", "cancelled", "rejected", "failed"];

/// Durable, coalesced reconciliation work restored after a client restart.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReconciliationWork {
    pub(crate) work_id: String,
    pub(crate) realm: Realm,
    pub(crate) kind: String,
    pub(crate) payload: Value,
    pub(crate) attempts: u32,
}

pub(crate) fn history_database_path(path: &Path) -> PathBuf {
    if path.file_name().and_then(|name| name.to_str()) == Some("replicant-client.sqlite") {
        return path.with_file_name("replicant-history.sqlite");
    }
    path.with_extension("history.sqlite")
}

impl Store {
    pub(crate) fn open_memory() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory()?;
        let history = Connection::open_in_memory()?;
        Self::configure(&connection, false)?;
        Self::configure_history(&history, false)?;
        Self::migrate(connection, history, false)
    }

    pub(crate) fn open_file(path: &Path) -> Result<Self, StoreError> {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        let history_path = history_database_path(path);
        let connection = Connection::open(path)?;
        let history = Connection::open(&history_path)?;
        Self::configure(&connection, true)?;
        Self::configure_history(&history, true)?;
        Self::migrate(connection, history, true)
    }

    fn configure(connection: &Connection, file_database: bool) -> Result<(), StoreError> {
        connection.busy_timeout(Duration::from_secs(15))?;
        if file_database {
            let table_count: i64 = connection.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'",
                [],
                |row| row.get(0),
            )?;
            if table_count == 0 {
                connection.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;")?;
            }
            connection.execute_batch("PRAGMA journal_mode = WAL;")?;
        }
        connection.execute_batch("PRAGMA foreign_keys = ON; PRAGMA synchronous = NORMAL;")?;
        Ok(())
    }

    fn configure_history(connection: &Connection, file_database: bool) -> Result<(), StoreError> {
        connection.busy_timeout(Duration::from_secs(15))?;
        if file_database {
            let table_count: i64 = connection.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'",
                [],
                |row| row.get(0),
            )?;
            if table_count == 0 {
                connection.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;")?;
            }
            connection.execute_batch("PRAGMA journal_mode = WAL;")?;
        }
        connection.execute_batch("PRAGMA synchronous = NORMAL;")?;
        Ok(())
    }

    fn migrate(
        mut connection: Connection,
        mut history: Connection,
        file_database: bool,
    ) -> Result<Self, StoreError> {
        history.execute_batch(HISTORY_INITIAL_SCHEMA)?;
        let mut history_version: i64 = history.query_row(
            "SELECT CAST(value AS INTEGER) FROM history_schema_metadata WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )?;
        if history_version > CURRENT_HISTORY_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedHistorySchemaVersion {
                found: history_version,
                supported: CURRENT_HISTORY_SCHEMA_VERSION,
            });
        }
        if history_version == 1 {
            migrate_refresh_history(&mut history)?;
            history_version = 2;
        }
        if history_version != CURRENT_HISTORY_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedHistorySchemaVersion {
                found: history_version,
                supported: CURRENT_HISTORY_SCHEMA_VERSION,
            });
        }
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY NOT NULL);",
        )?;
        let version: Option<i64> = connection
            .query_row("SELECT version FROM schema_migrations LIMIT 1", [], |row| {
                row.get(0)
            })
            .optional()?;

        let mut version = version.unwrap_or(0);
        if version == 0 {
            let transaction = connection.transaction()?;
            transaction.execute_batch(INITIAL_SCHEMA)?;
            #[cfg(test)]
            if INTERRUPT_NEXT_MIGRATION.with(|interrupted| interrupted.replace(false)) {
                return Err(StoreError::InjectedMigrationInterruption);
            }
            transaction.execute_batch(DEVICE_RELATIONSHIP_SEMANTICS_SCHEMA)?;
            migrate_device_relationship_observations(&transaction)?;
            transaction.execute_batch(RECONCILIATION_LEADER_SCHEMA)?;
            transaction.execute(
                "INSERT INTO schema_migrations(version) VALUES (3) ON CONFLICT(version) DO NOTHING",
                [],
            )?;
            transaction.execute("DELETE FROM schema_migrations WHERE version != 3", [])?;
            transaction.execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', '3') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [],
            )?;
            transaction.commit()?;
            version = 3;
        } else if version == 1 {
            let transaction = connection.transaction()?;
            transaction.execute_batch(DEVICE_RELATIONSHIP_SEMANTICS_SCHEMA)?;
            migrate_device_relationship_observations(&transaction)?;
            transaction.execute_batch(RECONCILIATION_LEADER_SCHEMA)?;
            transaction.execute(
                "UPDATE schema_migrations SET version = 3 WHERE version = 1",
                [],
            )?;
            transaction.execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', '3') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [],
            )?;
            transaction.commit()?;
            version = 3;
        } else if version == 2 {
            let transaction = connection.transaction()?;
            transaction.execute_batch(RECONCILIATION_LEADER_SCHEMA)?;
            transaction.execute(
                "UPDATE schema_migrations SET version = 3 WHERE version = 2",
                [],
            )?;
            transaction.execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', '3') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [],
            )?;
            transaction.commit()?;
            version = 3;
        } else if version > CURRENT_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchemaVersion {
                found: version,
                supported: CURRENT_SCHEMA_VERSION,
            });
        }

        let migrated_history_split = if version == 3 {
            Self::migrate_history_split(&mut connection, &mut history)?;
            version = 4;
            true
        } else {
            false
        };
        if version == 4 {
            let transaction = connection.transaction()?;
            transaction.execute_batch(MESSAGE_METADATA_SCHEMA)?;
            transaction.execute(
                "UPDATE schema_migrations SET version = 5 WHERE version = 4",
                [],
            )?;
            transaction.execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', '5') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [],
            )?;
            transaction.commit()?;
            version = 5;
        }
        if version == 5 {
            let transaction = connection.transaction()?;
            transaction.execute_batch(EVENT_PROJECTION_METADATA_SCHEMA)?;
            transaction.execute(
                "UPDATE schema_migrations SET version = 6 WHERE version = 5",
                [],
            )?;
            transaction.execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', '6') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [],
            )?;
            transaction.commit()?;
            version = 6;
        }
        if version == 6 {
            let transaction = connection.transaction()?;
            transaction.execute_batch(REFRESH_SCHEMA)?;
            transaction.execute(
                "UPDATE schema_migrations SET version = 7 WHERE version = 6",
                [],
            )?;
            transaction.execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', '7') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [],
            )?;
            transaction.commit()?;
            version = 7;
        }
        if version == 7 {
            let transaction = connection.transaction()?;
            transaction.execute_batch(MESSAGE_METADATA_REVISION_SCHEMA)?;
            transaction.execute(
                "UPDATE schema_migrations SET version = 8 WHERE version = 7",
                [],
            )?;
            transaction.execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', '8') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [],
            )?;
            transaction.commit()?;
            version = 8;
        }
        if version != CURRENT_SCHEMA_VERSION {
            return Err(StoreError::UnsupportedSchemaVersion {
                found: version,
                supported: CURRENT_SCHEMA_VERSION,
            });
        }

        history.execute_batch(HISTORY_INDEX_SCHEMA)?;

        if migrated_history_split && file_database {
            let page_count: i64 =
                connection.query_row("PRAGMA page_count", [], |row| row.get(0))?;
            let free_pages: i64 =
                connection.query_row("PRAGMA freelist_count", [], |row| row.get(0))?;
            if page_count > 0 && free_pages * 4 >= page_count {
                info!(
                    target: "replicant_client::store",
                    event = "store.primary_compaction_started",
                    page_count,
                    free_pages,
                    "compacting primary SQLite database after history split"
                );
                if let Err(error) = connection.execute_batch("VACUUM;") {
                    warn!(
                        target: "replicant_client::store",
                        event = "store.primary_compaction_failed",
                        error = %error,
                        "primary database migration succeeded but VACUUM could not reclaim free pages"
                    );
                }
            }
        }

        let mut store = Self {
            connection,
            history,
            last_history_maintenance: Instant::now() - HISTORY_MAINTENANCE_INTERVAL,
            #[cfg(test)]
            fail_next_commit: false,
        };
        store.reconcile_history_visibility()?;
        store.recover_orphaned_reconciliation_work()?;
        store.maintain_history()?;
        Ok(store)
    }

    fn migrate_history_split(
        connection: &mut Connection,
        history: &mut Connection,
    ) -> Result<(), StoreError> {
        let (legacy_event_cursor, migrated_events) =
            Self::copy_legacy_events_to_history(connection, history)?;

        let mut stars = {
            let mut statement = connection.prepare("SELECT payload_json FROM stars")?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            let mut stars = BTreeMap::new();
            for row in rows {
                let observation = serde_json::from_str::<Observation<Star>>(&row?)?;
                stars.insert(observation.value.key.clone(), observation);
            }
            stars
        };
        if table_exists(connection, "replicant_star_knowledge")? {
            let mut statement = connection.prepare(
                "SELECT observation_json FROM replicant_star_knowledge ORDER BY realm, star_id, replicant_id",
            )?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            for row in rows {
                let knowledge = serde_json::from_str::<Observation<StarKnowledge>>(&row?)?;
                let star = crate::domain::account_star_from_knowledge(knowledge);
                let key = star.value.key.clone();
                let merged = if let Some(current) = stars.remove(&key) {
                    merge_migrated_star_knowledge(current, star)
                } else {
                    star
                };
                stars.insert(key, merged);
            }
        }

        let transaction = connection.transaction()?;
        transaction.execute_batch(HISTORY_SPLIT_SCHEMA)?;
        {
            let mut statement = transaction.prepare(
                "INSERT INTO stars(realm, star_id, payload_json) VALUES (?1, ?2, ?3) ON CONFLICT(realm, star_id) DO UPDATE SET payload_json = excluded.payload_json",
            )?;
            for star in stars.values() {
                statement.execute(params![
                    realm_key(&star.value.key.realm),
                    star.value.key.id.as_str(),
                    serde_json::to_string(star)?,
                ])?;
            }
        }
        if let Some(cursor) = legacy_event_cursor.as_deref() {
            advance_event_cursor(&transaction, cursor)?;
        }
        transaction.execute("DROP INDEX IF EXISTS event_journal_realm", [])?;
        transaction.execute("DROP TABLE IF EXISTS event_journal", [])?;
        transaction.execute("DROP INDEX IF EXISTS replicant_star_knowledge_star", [])?;
        transaction.execute("DROP TABLE IF EXISTS replicant_star_knowledge", [])?;
        transaction.execute(
            "UPDATE schema_migrations SET version = 4 WHERE version = 3",
            [],
        )?;
        transaction.execute(
            "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', '4') ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [],
        )?;
        transaction.commit()?;
        history.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        info!(
            target: "replicant_client::store",
            event = "store.history_split_completed",
            events = migrated_events,
            stars = stars.len(),
            "migrated event history to the history database and normalized star knowledge"
        );
        Ok(())
    }

    fn copy_legacy_events_to_history(
        connection: &Connection,
        history: &mut Connection,
    ) -> Result<(Option<String>, usize), StoreError> {
        if !table_exists(connection, "event_journal")? {
            return Ok((None, 0));
        }
        let total: i64 =
            connection.query_row("SELECT COUNT(*) FROM event_journal", [], |row| row.get(0))?;
        info!(
            target: "replicant_client::store",
            event = "store.history_split_copy_started",
            total,
            "copying legacy event journal into the history database"
        );
        let mut source = connection.prepare(
            "SELECT event_json, appended_at FROM event_journal ORDER BY appended_at, event_id",
        )?;
        let rows = source.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let transaction = history.transaction()?;
        let mut max_event_id: Option<String> = None;
        let mut copied = 0usize;
        {
            let mut insert = transaction.prepare(
                "INSERT OR IGNORE INTO event_history(event_id, realm, event_name, category, device_code, replicant_code, star_id, location_id, occurred_at, payload_json, appended_at, applied_at, archived_only, stream_millis, stream_sequence) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11, 0, ?12, ?13)",
            )?;
            for row in rows {
                let (serialized, appended_at) = row?;
                let event = serde_json::from_str::<Event>(&serialized)?;
                let event_id = event.id.as_str();
                let is_newer = match max_event_id.as_deref() {
                    Some(current) => compare_event_ids(event_id, current)?.is_gt(),
                    None => true,
                };
                if is_newer {
                    max_event_id = Some(event_id.to_owned());
                }
                insert_history_event(&mut insert, &event, &appended_at)?;
                copied += 1;
                if copied.is_multiple_of(100_000) {
                    info!(
                        target: "replicant_client::store",
                        event = "store.history_split_copy_progress",
                        copied,
                        total,
                        "copying legacy event journal into the history database"
                    );
                }
            }
        }
        transaction.commit()?;
        info!(
            target: "replicant_client::store",
            event = "store.history_split_copy_completed",
            copied,
            total,
            "copied legacy event journal into the history database"
        );
        Ok((max_event_id, copied))
    }

    fn maintain_history(&mut self) -> Result<(), StoreError> {
        if self.last_history_maintenance.elapsed() < HISTORY_MAINTENANCE_INTERVAL {
            return Ok(());
        }
        let operation_modifier = format!("-{OPERATION_TERMINAL_RETENTION_DAYS} days");
        let operations_deleted = self.connection.execute(
            "DELETE FROM operation_journal WHERE state IN ('completed', 'rejected') AND datetime(updated_at) < datetime('now', ?1)",
            [operation_modifier],
        )?;
        if operations_deleted > 0 {
            self.connection.execute_batch(
                "PRAGMA wal_checkpoint(PASSIVE); PRAGMA incremental_vacuum(2000);",
            )?;
            info!(
                target: "replicant_client::store",
                event = "operation.journal_pruned",
                deleted = operations_deleted,
                retention_days = OPERATION_TERMINAL_RETENTION_DAYS,
                "pruned expired terminal operations"
            );
        }
        self.last_history_maintenance = Instant::now();
        Ok(())
    }

    fn recover_orphaned_reconciliation_work(&mut self) -> Result<(), StoreError> {
        let recovered = self.connection.execute(
            "UPDATE reconciliation_queue SET state = 'queued' WHERE state = 'running' AND NOT EXISTS (SELECT 1 FROM reconciliation_leader WHERE lease_until > CAST(strftime('%s','now') AS INTEGER))",
            [],
        )?;
        if recovered > 0 {
            info!(
                target: "replicant_client::store",
                event = "reconciliation.orphaned_work_recovered",
                recovered,
                "returned orphaned reconciliation work to the queue"
            );
        }
        Ok(())
    }

    fn append_history_event(&mut self, event: &Event) -> Result<(), StoreError> {
        let payload = serde_json::to_string(&event.payload)?;
        let stream = parse_event_id(event.id.as_str()).ok();
        let transaction = self.history.transaction()?;
        transaction.execute(
            "INSERT OR IGNORE INTO event_history(event_id, realm, event_name, category, device_code, replicant_code, star_id, location_id, occurred_at, payload_json, appended_at, applied_at, archived_only, stream_millis, stream_sequence) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, datetime('now'), NULL, 0, ?11, ?12)",
            params![
                event.id.as_str(),
                event.realm.as_ref().map(realm_key),
                event.name.as_str(),
                event.category.as_str(),
                event.device.as_ref().map(|key| key.id.as_str()),
                event.replicant.as_ref().map(|key| key.id.as_str()),
                event.star.as_ref().map(|key| key.id.as_str()),
                event.location.as_ref().map(|key| key.id.as_str()),
                &event.occurred_at,
                payload,
                stream.map(|value| value.0),
                stream.map(|value| value.1),
            ],
        )?;
        transaction.commit()?;
        self.maintain_history()
    }

    fn mark_history_event_applied(&mut self, event_id: &str) -> Result<(), StoreError> {
        self.history.execute(
            "UPDATE event_history SET applied_at = COALESCE(applied_at, datetime('now')) WHERE event_id = ?1",
            [event_id],
        )?;
        Ok(())
    }

    fn mark_history_event_applied_best_effort(&mut self, event_id: &str) {
        if let Err(error) = self.mark_history_event_applied(event_id) {
            warn!(
                target: "replicant_client::store",
                event = "history.event_visibility_deferred",
                event_id,
                error = %error,
                "primary event projection committed; history visibility will be repaired from the applied cursor"
            );
        }
    }

    fn reconcile_history_visibility(&mut self) -> Result<(), StoreError> {
        let Some(cursor) = self.event_cursor()? else {
            return Ok(());
        };
        let mut statement = self.history.prepare(
            "SELECT event_id FROM event_history
             WHERE applied_at IS NULL AND archived_only = 0",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut applied = Vec::new();
        for row in rows {
            let event_id = row?;
            if !compare_event_ids(&event_id, &cursor)?.is_gt() {
                applied.push(event_id);
            }
        }
        drop(statement);
        if applied.is_empty() {
            return Ok(());
        }
        let transaction = self.history.transaction()?;
        {
            let mut update = transaction.prepare(
                "UPDATE event_history SET applied_at = COALESCE(applied_at, datetime('now')) WHERE event_id = ?1",
            )?;
            for event_id in &applied {
                update.execute([event_id])?;
            }
        }
        transaction.commit()?;
        info!(
            target: "replicant_client::store",
            event = "history.visibility_reconciled",
            rows = applied.len(),
            cursor,
            "reconciled staged history rows against the authoritative applied event cursor"
        );
        Ok(())
    }

    #[cfg(test)]
    fn interrupt_next_migration_for_test() {
        INTERRUPT_NEXT_MIGRATION.with(|interrupted| interrupted.set(true));
    }

    pub(crate) fn bind_account(&mut self, account_id: &AccountId) -> Result<(), StoreError> {
        let existing: Option<String> = self
            .connection
            .query_row(
                "SELECT account_id FROM account_binding WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        match existing {
            Some(stored) if stored != account_id.as_str() => Err(StoreError::AccountMismatch {
                stored_account_id: stored,
                supplied_account_id: account_id.as_str().to_owned(),
            }),
            Some(_) => Ok(()),
            None => {
                self.connection.execute(
                    "INSERT INTO account_binding(singleton, account_id, bound_at) VALUES (1, ?1, datetime('now'))",
                    [account_id.as_str()],
                )?;
                Ok(())
            }
        }
    }

    /// The account ID bound to this store, if any account has bound yet.
    pub(crate) fn bound_account_id(&self) -> Result<Option<String>, StoreError> {
        self.connection
            .query_row(
                "SELECT account_id FROM account_binding WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::from)
    }

    /// Explicitly replaces the mutable email-derived binding after the server
    /// has verified the new address. Normal startup never takes this path.
    pub(crate) fn rebind_account_and_persist(
        &mut self,
        previous: &AccountId,
        account: &Observation<Account>,
    ) -> Result<(), StoreError> {
        let stored = self
            .bound_account_id()?
            .ok_or_else(|| StoreError::AccountMismatch {
                stored_account_id: String::new(),
                supplied_account_id: previous.as_str().to_owned(),
            })?;
        if stored != previous.as_str() {
            return Err(StoreError::AccountMismatch {
                stored_account_id: stored,
                supplied_account_id: previous.as_str().to_owned(),
            });
        }
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE account_binding SET account_id = ?1, bound_at = datetime('now') WHERE singleton = 1",
            [account.value.id.as_str()],
        )?;
        transaction.execute("DELETE FROM accounts WHERE realm = 'live'", [])?;
        transaction.execute(
            "INSERT INTO accounts(realm, account_id, observation_json) VALUES ('live', ?1, ?2)",
            params![account.value.id.as_str(), serde_json::to_string(account)?],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Forces file-backed SQLite state to durable storage before shutdown.
    pub(crate) fn flush(&mut self) -> Result<(), StoreError> {
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        self.history
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }

    pub(crate) fn restore_devices(
        &self,
    ) -> Result<BTreeMap<DeviceKey, Observation<Device>>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT observation_json, observed_at FROM devices ORDER BY realm, device_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        let mut devices = BTreeMap::new();
        for row in rows {
            let (serialized, observed_at) = row?;
            let mut observation = serde_json::from_str::<Observation<Device>>(&serialized)?;
            observation.metadata.observed_at = crate::domain::ObservationTime::from_unix_millis(
                observation
                    .metadata
                    .observed_at
                    .unix_millis()
                    .max(observed_at),
            );
            devices.insert(observation.value.key.clone(), observation);
        }
        Ok(devices)
    }

    pub(crate) fn restore_account(&self) -> Result<Option<Observation<Account>>, StoreError> {
        self.connection
            .query_row(
                "SELECT observation_json FROM accounts WHERE realm = 'live' ORDER BY account_id LIMIT 1",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| serde_json::from_str(&value).map_err(StoreError::from))
            .transpose()
    }

    pub(crate) fn restore_replicants(
        &self,
    ) -> Result<BTreeMap<ReplicantKey, Observation<Replicant>>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT observation_json FROM replicants ORDER BY realm, replicant_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut replicants = BTreeMap::new();
        for row in rows {
            let observation = serde_json::from_str::<Observation<Replicant>>(&row?)?;
            replicants.insert(observation.value.key.clone(), observation);
        }
        Ok(replicants)
    }

    pub(crate) fn restore_locations(
        &self,
    ) -> Result<BTreeMap<LocationKey, Observation<Location>>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT observation_json FROM locations ORDER BY realm, location_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut locations = BTreeMap::new();
        for row in rows {
            let observation = serde_json::from_str::<Observation<Location>>(&row?)?;
            locations.insert(observation.value.key.clone(), observation);
        }
        Ok(locations)
    }

    pub(crate) fn restore_catalogue(&self) -> Result<CatalogueRows, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT payload_json FROM stars ORDER BY realm, star_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut stars = BTreeMap::new();
        for row in rows {
            let observation = serde_json::from_str::<Observation<Star>>(&row?)?;
            stars.insert(observation.value.key.clone(), observation);
        }
        let generated_at = self
            .connection
            .query_row(
                "SELECT generated_at FROM catalogue_metadata WHERE singleton = 1",
                [],
                |row| row.get(0),
            )
            .optional()?;
        Ok((stars, generated_at))
    }

    pub(crate) fn persist_account(
        &mut self,
        account: &Observation<Account>,
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        transaction.execute("INSERT INTO accounts(realm, account_id, observation_json) VALUES ('live', ?1, ?2) ON CONFLICT(realm, account_id) DO UPDATE SET observation_json = excluded.observation_json", params![account.value.id.as_str(), serde_json::to_string(account)?])?;
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn persist_replicant(
        &mut self,
        replicant: &Observation<Replicant>,
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        transaction.execute("INSERT INTO replicants(realm, replicant_id, observation_json) VALUES (?1, ?2, ?3) ON CONFLICT(realm, replicant_id) DO UPDATE SET observation_json = excluded.observation_json", params![realm_key(&replicant.value.key.realm), replicant.value.key.id.as_str(), serde_json::to_string(replicant)?])?;
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn persist_location(
        &mut self,
        location: &Observation<Location>,
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO locations(realm, location_id, observation_json) VALUES (?1, ?2, ?3) ON CONFLICT(realm, location_id) DO UPDATE SET observation_json = excluded.observation_json",
            params![realm_key(&location.value.key.realm), location.value.key.id.as_str(), serde_json::to_string(location)?],
        )?;
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn replace_catalogue(
        &mut self,
        stars: &[Observation<Star>],
        generated_at: Option<&str>,
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        for star in stars {
            transaction.execute(
                "INSERT INTO stars(realm, star_id, payload_json) VALUES (?1, ?2, ?3)
                 ON CONFLICT(realm, star_id) DO UPDATE SET payload_json = excluded.payload_json",
                params![
                    realm_key(&star.value.key.realm),
                    star.value.key.id.as_str(),
                    serde_json::to_string(star)?
                ],
            )?;
        }
        transaction.execute(
            "INSERT INTO catalogue_metadata(singleton, generated_at) VALUES (1, ?1)
             ON CONFLICT(singleton) DO UPDATE SET generated_at = excluded.generated_at",
            [generated_at],
        )?;
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn persist_stars(&mut self, stars: &[Observation<Star>]) -> Result<(), StoreError> {
        if stars.is_empty() {
            return Ok(());
        }
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        {
            let mut statement = transaction.prepare(
                "INSERT INTO stars(realm, star_id, payload_json) VALUES (?1, ?2, ?3) ON CONFLICT(realm, star_id) DO UPDATE SET payload_json = excluded.payload_json",
            )?;
            for star in stars {
                statement.execute(params![
                    realm_key(&star.value.key.realm),
                    star.value.key.id.as_str(),
                    serde_json::to_string(star)?,
                ])?;
            }
        }
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn persist_devices(
        &mut self,
        devices: &[Observation<Device>],
    ) -> Result<(), StoreError> {
        self.persist_devices_and_touch(devices, &[])
    }

    pub(crate) fn persist_devices_and_touch(
        &mut self,
        changed: &[Observation<Device>],
        touches: &[(DeviceKey, crate::domain::ObservationTime)],
    ) -> Result<(), StoreError> {
        if changed.is_empty() && touches.is_empty() {
            return Ok(());
        }
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        for device in changed {
            persist_device(&transaction, device)?;
        }
        if !touches.is_empty() {
            let mut statement = transaction.prepare(
                "UPDATE devices SET observed_at = MAX(observed_at, ?3) \
                 WHERE realm = ?1 AND device_id = ?2",
            )?;
            for (key, observed_at) in touches {
                statement.execute(params![
                    realm_key(&key.realm),
                    key.id.as_str(),
                    observed_at.unix_millis(),
                ])?;
            }
        }
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn persist_simulation_and_devices(
        &mut self,
        simulation: &Observation<Simulation>,
        devices: &[Observation<Device>],
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO simulations(simulation_id, payload_json) VALUES (?1, ?2) ON CONFLICT(simulation_id) DO UPDATE SET payload_json = excluded.payload_json",
            params![simulation.value.id.get(), serde_json::to_string(simulation)?],
        )?;
        for device in devices {
            persist_device(&transaction, device)?;
        }
        Self::commit(transaction, fail_commit)
    }

    /// Removes only reachable owned devices proven absent by a completed full
    /// traversal; historical and inaccessible observations are not absence evidence.
    pub(crate) fn reconcile_owned_devices(
        &mut self,
        present: &BTreeSet<DeviceKey>,
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        let mut statement = transaction.prepare(
            "SELECT realm, device_id, observation_json FROM devices WHERE access_scope = 'owned'",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut eligible = 0usize;
        let mut missing = Vec::new();
        for row in rows {
            let (realm, id, serialized) = row?;
            let observation = serde_json::from_str::<Observation<Device>>(&serialized)?;
            if observation.metadata.reachability == crate::domain::Reachability::Reachable {
                eligible += 1;
                if !present.contains(&observation.value.key) {
                    missing.push((realm, id));
                }
            }
        }
        drop(statement);
        if present.is_empty() && eligible > 0 {
            return Err(StoreError::Refresh(
                "empty device enumeration cannot remove non-empty local state".into(),
            ));
        }
        if eligible > 0 && missing.len() * 100 > eligible * 20 {
            return Err(StoreError::Refresh(
                "device enumeration shrink exceeds the guarded approval threshold".into(),
            ));
        }
        for (realm, id) in missing {
            transaction.execute(
                "DELETE FROM devices WHERE realm = ?1 AND device_id = ?2",
                params![&realm, &id],
            )?;
            transaction.execute(
                "INSERT OR REPLACE INTO tombstones(realm, kind, item_id, removed_at, evidence) VALUES (?1, 'device', ?2, datetime('now'), 'complete-unfiltered-device-traversal')",
                params![&realm, &id],
            )?;
            transaction.execute(
                "DELETE FROM reconciliation_queue WHERE realm = ?1 AND kind = 'device' AND work_id = ?2",
                params![&realm, format!("device:{id}")],
            )?;
        }
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn apply_event_projection(
        &mut self,
        event: &Event,
        cursor: &str,
        batch: &EventProjectionBatch,
    ) -> Result<bool, StoreError> {
        if self.has_event(cursor)? {
            self.mark_history_event_applied_best_effort(event.id.as_str());
            return Ok(false);
        }
        self.append_history_event(event)?;
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        persist_projection_batch(&transaction, batch)?;
        advance_event_cursor(&transaction, cursor)?;
        Self::commit(transaction, fail_commit)?;
        self.mark_history_event_applied_best_effort(event.id.as_str());
        Ok(true)
    }

    /// Durable dedup check based on the monotonically advancing applied cursor.
    ///
    /// History is intentionally stored in a separate database, so it cannot be
    /// the atomic source of truth for whether an event projection committed.
    pub(crate) fn has_event(&self, event_id: &str) -> Result<bool, StoreError> {
        let Some(cursor) = self.event_cursor()? else {
            return Ok(false);
        };
        Ok(!compare_event_ids(event_id, &cursor)?.is_gt())
    }

    /// Persists a baseline watermark cursor with no accompanying event, used
    /// when a first-start account has no applied cursor yet.
    pub(crate) fn set_event_cursor(&mut self, cursor: &str) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        advance_event_cursor(&transaction, cursor)?;
        transaction.commit()?;
        Ok(())
    }

    /// Reports whether the applied cursor is old enough that continuity
    /// cannot be assumed, without relying on any explicit server rejection.
    /// A missing cursor is conservatively treated as stale.
    pub(crate) fn event_cursor_is_stale(&self, threshold_secs: i64) -> Result<bool, StoreError> {
        let fresh: Option<i64> = self
            .connection
            .query_row(
                "SELECT CASE WHEN updated_at > datetime('now', ?1) THEN 1 ELSE 0 END FROM event_cursors WHERE stream = 'account'",
                params![format!("-{threshold_secs} seconds")],
                |row| row.get(0),
            )
            .optional()?;
        Ok(fresh != Some(1))
    }
    /// Atomically inserts an operation journal row when its ID is absent.
    /// When the ID already exists, returns the untouched journal entry so the
    /// caller can verify identity without a check-then-insert race.
    pub(crate) fn record_operation_if_absent(
        &mut self,
        operation_id: &str,
        state: &str,
        target_realm: Option<&str>,
        target_kind: Option<&str>,
        target_id: Option<&str>,
        intent: &Value,
    ) -> Result<Option<OperationJournalEntry>, StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        let changed = transaction.execute(
            "INSERT INTO operation_journal(operation_id, state, target_realm, target_kind, target_id, intent_json, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now')) ON CONFLICT(operation_id) DO NOTHING",
            params![
                operation_id,
                state,
                target_realm,
                target_kind,
                target_id,
                serde_json::to_string(intent)?
            ],
        )?;
        if changed == 1 {
            Self::commit(transaction, fail_commit)?;
            return Ok(None);
        }
        let row = transaction
            .query_row(
                "SELECT state, target_realm, target_kind, target_id, intent_json, projection_json, submission_attempt_id, submitted_at, submission_cursor FROM operation_journal WHERE operation_id = ?1",
                [operation_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                    ))
                },
            )
            .optional()?;
        let existing = row
            .map(
                |(
                    state,
                    target_realm,
                    target_kind,
                    target_id,
                    intent,
                    projection,
                    submission_attempt_id,
                    submitted_at,
                    submission_cursor,
                )| {
                    Ok::<OperationJournalEntry, StoreError>(OperationJournalEntry {
                        state,
                        target_realm,
                        target_kind,
                        target_id,
                        intent: serde_json::from_str(&intent)?,
                        projection: projection
                            .map(|value| serde_json::from_str(&value))
                            .transpose()?,
                        submission_attempt_id,
                        submitted_at,
                        submission_cursor,
                    })
                },
            )
            .transpose()?;
        Self::commit(transaction, fail_commit)?;
        Ok(existing)
    }

    #[cfg(test)]
    pub(crate) fn backdate_event_cursor(&mut self, seconds: i64) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE event_cursors SET updated_at = datetime('now', ?1) WHERE stream = 'account'",
            params![format!("-{seconds} seconds")],
        )?;
        Ok(())
    }
    /// Persists a durable operation's initial intent, before any unsafe
    /// network transmission is attempted. Duplicate IDs are rejected; callers
    /// that need idempotent insertion must use `record_operation_if_absent`.
    /// `target_*` are `None` for operations with no single affected entity
    /// (for example, marking messages read).
    pub(crate) fn record_operation(
        &mut self,
        operation_id: &str,
        state: &str,
        target_realm: Option<&str>,
        target_kind: Option<&str>,
        target_id: Option<&str>,
        intent: &Value,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO operation_journal(operation_id, state, target_realm, target_kind, target_id, intent_json, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now'))",
            params![
                operation_id,
                state,
                target_realm,
                target_kind,
                target_id,
                serde_json::to_string(intent)?
            ],
        )?;
        Ok(())
    }

    /// Advances an operation's state with no projection change (for example,
    /// `prepared` -> `submitted` immediately before the one automatic
    /// transmission attempt).
    pub(crate) fn set_operation_state(
        &mut self,
        operation_id: &str,
        state: &str,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE operation_journal SET state = ?2, updated_at = datetime('now') WHERE operation_id = ?1",
            params![operation_id, state],
        )?;
        Ok(())
    }

    /// Atomically claims the single automatic send attempt.  The request must
    /// not leave the process unless this returns `true`.
    pub(crate) fn claim_operation_submission(
        &mut self,
        operation_id: &str,
        attempt_id: &str,
    ) -> Result<bool, StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        let cursor: Option<String> = transaction
            .query_row("SELECT cursor FROM event_cursors LIMIT 1", [], |row| {
                row.get(0)
            })
            .optional()?;
        let changed = transaction.execute(
            "UPDATE operation_journal
             SET state = 'submitted', submission_attempt_id = ?2,
                 submitted_at = datetime('now'), submission_cursor = ?3,
                 updated_at = datetime('now')
             WHERE operation_id = ?1 AND state = 'prepared'",
            params![operation_id, attempt_id, cursor],
        )?;
        Self::commit(transaction, fail_commit)?;
        Ok(changed == 1)
    }

    /// Atomically commits a device projection produced by an operation's
    /// response alongside the operation's resolved state, so a crash between
    /// the two is impossible.
    pub(crate) fn record_operation_and_project(
        &mut self,
        operation_id: &str,
        state: &str,
        devices: &[Observation<Device>],
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE operation_journal SET state = ?2, updated_at = datetime('now') WHERE operation_id = ?1",
            params![operation_id, state],
        )?;
        for device in devices {
            persist_device(&transaction, device)?;
        }
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn append_operation_projection(
        &mut self,
        operation_id: &str,
        state: &str,
        projection: &Value,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE operation_journal SET state = ?2, projection_json = ?3, updated_at = datetime('now') WHERE operation_id = ?1",
            params![operation_id, state, serde_json::to_string(projection)?],
        )?;
        Ok(())
    }

    /// Restart recovery: promotes any operation caught mid-transmission
    /// (`submitted`) to `ambiguous`. A process can only observe `submitted`
    /// without ever reaching a later state if it crashed during or just after
    /// the one automatic send attempt, which is definitionally ambiguous —
    /// never safe to blindly resubmit. Returns the number of rows promoted.
    pub(crate) fn promote_crashed_submissions(&mut self) -> Result<usize, StoreError> {
        Ok(self.connection.execute(
            "UPDATE operation_journal SET state = 'ambiguous', updated_at = datetime('now') WHERE state = 'submitted'",
            [],
        )?)
    }

    /// Every operation not yet in a terminal state, for restart recovery and
    /// caller-visible unresolved-operation listings.
    pub(crate) fn list_unresolved_operations(
        &self,
    ) -> Result<Vec<(String, OperationJournalEntry)>, StoreError> {
        let placeholders = OPERATION_TERMINAL_STATES
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT operation_id, state, target_realm, target_kind, target_id, intent_json, projection_json, submission_attempt_id, submitted_at, submission_cursor FROM operation_journal WHERE state NOT IN ({placeholders}) ORDER BY updated_at"
        );
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement.query_map(
            rusqlite::params_from_iter(OPERATION_TERMINAL_STATES.iter()),
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                ))
            },
        )?;
        let mut operations = Vec::new();
        for row in rows {
            let (
                operation_id,
                state,
                target_realm,
                target_kind,
                target_id,
                intent,
                projection,
                submission_attempt_id,
                submitted_at,
                submission_cursor,
            ) = row?;
            operations.push((
                operation_id,
                OperationJournalEntry {
                    state,
                    target_realm,
                    target_kind,
                    target_id,
                    intent: serde_json::from_str(&intent)?,
                    projection: projection
                        .map(|value| serde_json::from_str(&value))
                        .transpose()?,
                    submission_attempt_id,
                    submitted_at,
                    submission_cursor,
                },
            ));
        }
        Ok(operations)
    }

    /// Operation IDs awaiting event evidence for a specific target entity,
    /// used to resolve operations when a matching account event arrives.
    pub(crate) fn find_operations_awaiting_evidence(
        &self,
        target_realm: &str,
        target_kind: &str,
        target_id: &str,
    ) -> Result<Vec<String>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT operation_id FROM operation_journal WHERE state = 'awaiting_evidence' AND target_realm = ?1 AND target_kind = ?2 AND target_id = ?3",
        )?;
        let rows = statement.query_map(params![target_realm, target_kind, target_id], |row| {
            row.get::<_, String>(0)
        })?;
        let mut ids = Vec::new();
        for row in rows {
            ids.push(row?);
        }
        Ok(ids)
    }

    pub(crate) fn prepare_projection_replay(
        &mut self,
        projection: &str,
        version: i64,
    ) -> Result<ProjectionReplayState, StoreError> {
        let current_high_water = self.history.query_row(
            "SELECT COALESCE(MAX(rowid), 0) FROM event_history WHERE applied_at IS NOT NULL",
            [],
            |row| row.get::<_, i64>(0),
        )?;
        let existing = self
            .connection
            .query_row(
                "SELECT version, last_history_rowid, high_water_rowid, state FROM event_projection_metadata WHERE projection = ?1",
                [projection],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()?;
        if let Some((stored_version, last, high_water, state)) = existing
            && stored_version == version
        {
            return Ok(ProjectionReplayState {
                last_history_rowid: last,
                high_water_rowid: high_water,
                complete: state == "complete",
            });
        }
        self.connection.execute(
            "INSERT INTO event_projection_metadata(projection, version, last_history_rowid, high_water_rowid, state, coverage, updated_at) VALUES (?1, ?2, 0, ?3, 'running', 'retained_only', datetime('now')) ON CONFLICT(projection) DO UPDATE SET version = excluded.version, last_history_rowid = 0, high_water_rowid = excluded.high_water_rowid, state = 'running', coverage = 'retained_only', updated_at = excluded.updated_at",
            params![projection, version, current_high_water],
        )?;
        Ok(ProjectionReplayState {
            last_history_rowid: 0,
            high_water_rowid: current_high_water,
            complete: false,
        })
    }

    pub(crate) fn read_projection_history(
        &self,
        after_rowid: i64,
        high_water_rowid: i64,
        limit: usize,
    ) -> Result<Vec<(i64, Event)>, StoreError> {
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut statement = self.history.prepare(
            "SELECT rowid, event_id, realm, event_name, category, device_code, replicant_code, star_id, location_id, occurred_at, payload_json FROM event_history WHERE applied_at IS NOT NULL AND rowid > ?1 AND rowid <= ?2 ORDER BY rowid LIMIT ?3",
        )?;
        let rows = statement.query_map(params![after_rowid, high_water_rowid, limit], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                (
                    row.get::<_, String>(1)?,
                    row.get::<_, Option<String>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<String>>(5)?,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, String>(9)?,
                    row.get::<_, String>(10)?,
                ),
            ))
        })?;
        rows.map(|row| {
            let (rowid, event) = row?;
            Ok((rowid, decode_history_event(event)?))
        })
        .collect()
    }

    pub(crate) fn apply_replay_projection(
        &mut self,
        projection: &str,
        version: i64,
        last_history_rowid: i64,
        high_water_rowid: i64,
        batch: &EventProjectionBatch,
    ) -> Result<(), StoreError> {
        debug_assert!(batch.reconciliation.is_empty());
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        persist_projection_batch(&transaction, batch)?;
        transaction.execute(
            "UPDATE event_projection_metadata SET last_history_rowid = ?3, high_water_rowid = ?4, state = 'running', coverage = 'retained_only', updated_at = datetime('now') WHERE projection = ?1 AND version = ?2",
            params![projection, version, last_history_rowid, high_water_rowid],
        )?;
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn complete_projection_replay(
        &mut self,
        projection: &str,
        version: i64,
        high_water_rowid: i64,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE event_projection_metadata SET last_history_rowid = ?3, high_water_rowid = ?3, state = 'complete', coverage = 'retained_only', updated_at = datetime('now') WHERE projection = ?1 AND version = ?2",
            params![projection, version, high_water_rowid],
        )?;
        Ok(())
    }

    pub(crate) fn read_events(
        &self,
        after: Option<&str>,
        device_code: Option<&str>,
        event_name: Option<&str>,
        latest: Option<usize>,
    ) -> Result<Vec<Event>, StoreError> {
        let started = Instant::now();
        let after_parts = after.map(parse_event_id).transpose()?;
        let mut sql = String::from(
            "SELECT event_id, realm, event_name, category, device_code, replicant_code, star_id, location_id, occurred_at, payload_json \
             FROM event_history WHERE (applied_at IS NOT NULL OR archived_only = 1)",
        );
        let mut parameters = Vec::<SqlValue>::new();
        if let Some((milliseconds, sequence)) = after_parts {
            sql.push_str(" AND (stream_millis, stream_sequence, event_id) > (?, ?, ?)");
            parameters.push(milliseconds.into());
            parameters.push(sequence.into());
            parameters.push(after.unwrap_or_default().to_owned().into());
        }
        if let Some(device_code) = device_code {
            sql.push_str(" AND device_code = ?");
            parameters.push(device_code.to_owned().into());
        }
        if let Some(event_name) = event_name {
            sql.push_str(" AND event_name = ?");
            parameters.push(event_name.to_owned().into());
        }
        if let Some(limit) = latest {
            sql.push_str(
                " ORDER BY stream_millis DESC, stream_sequence DESC, event_id DESC LIMIT ?",
            );
            parameters.push(i64::try_from(limit).unwrap_or(i64::MAX).into());
        } else {
            sql.push_str(" ORDER BY stream_millis, stream_sequence, event_id");
        }

        let prepare_started = Instant::now();
        let mut statement = self.history.prepare(&sql)?;
        let prepare_ms = prepare_started.elapsed().as_millis() as u64;
        let query_started = Instant::now();
        let mut rows = statement.query(params_from_iter(parameters.iter()))?;
        let query_ms = query_started.elapsed().as_millis() as u64;
        let mut events = Vec::with_capacity(latest.unwrap_or_default());
        let mut row_ms = 0u64;
        let mut decode_ms = 0u64;
        loop {
            let row_started = Instant::now();
            let Some(row) = rows.next()? else {
                row_ms = row_ms.saturating_add(row_started.elapsed().as_millis() as u64);
                break;
            };
            let row = history_event_row(row)?;
            row_ms = row_ms.saturating_add(row_started.elapsed().as_millis() as u64);
            let decode_started = Instant::now();
            events.push(decode_history_event(row)?);
            decode_ms = decode_ms.saturating_add(decode_started.elapsed().as_millis() as u64);
        }
        drop(rows);
        drop(statement);
        if latest.is_some() {
            events.reverse();
        }
        let elapsed = started.elapsed();
        if elapsed >= Duration::from_secs(1) {
            warn!(
                target: "replicant_client::store",
                event = "store.read_events_slow",
                requested_after_id = after.unwrap_or(""),
                requested_device_code = device_code.unwrap_or(""),
                requested_event_name = event_name.unwrap_or(""),
                limit = latest.unwrap_or_default(),
                rows_returned = events.len(),
                prepare_ms,
                query_ms,
                row_ms,
                decode_ms,
                elapsed_ms = elapsed.as_millis() as u64,
                "managed event-history query exceeded responsiveness threshold"
            );
        } else {
            debug!(
                target: "replicant_client::store",
                event = "store.read_events_completed",
                requested_after_id = after.unwrap_or(""),
                requested_device_code = device_code.unwrap_or(""),
                requested_event_name = event_name.unwrap_or(""),
                limit = latest.unwrap_or_default(),
                rows_returned = events.len(),
                prepare_ms,
                query_ms,
                row_ms,
                decode_ms,
                elapsed_ms = elapsed.as_millis() as u64,
                "read indexed managed event history"
            );
        }
        Ok(events)
    }

    pub(crate) fn read_operation(
        &self,
        operation_id: &str,
    ) -> Result<Option<OperationJournalEntry>, StoreError> {
        self.connection
            .query_row(
                "SELECT state, target_realm, target_kind, target_id, intent_json, projection_json, submission_attempt_id, submitted_at, submission_cursor FROM operation_journal WHERE operation_id = ?1",
                [operation_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                    ))
                },
            )
            .optional()?
            .map(
                |(state, target_realm, target_kind, target_id, intent, projection, submission_attempt_id, submitted_at, submission_cursor)| {
                    Ok(OperationJournalEntry {
                        state,
                        target_realm,
                        target_kind,
                        target_id,
                        intent: serde_json::from_str(&intent)?,
                        projection: projection
                            .map(|value| serde_json::from_str(&value))
                            .transpose()?,
                        submission_attempt_id,
                        submitted_at,
                        submission_cursor,
                    })
                },
            )
            .transpose()
    }

    pub(crate) fn enqueue_reconciliation(
        &mut self,
        work_id: &str,
        realm: &Realm,
        kind: &str,
        payload: &Value,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO reconciliation_queue(work_id, realm, kind, payload_json, not_before, attempts, state) VALUES (?1, ?2, ?3, ?4, NULL, 0, 'queued') ON CONFLICT(work_id) DO UPDATE SET realm = excluded.realm, kind = excluded.kind, payload_json = excluded.payload_json, not_before = NULL, attempts = 0, state = 'queued'",
            params![work_id, realm_key(realm), kind, serde_json::to_string(payload)?],
        )?;
        Ok(())
    }

    /// Acquires or renews the single cross-process reconciliation-worker lease.
    /// When a previous leader's lease has expired, its in-flight work is
    /// returned to the queue before the new leader begins draining it.
    pub(crate) fn acquire_reconciliation_leadership(
        &mut self,
        owner: &str,
        lease_seconds: i64,
    ) -> Result<bool, StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now =
            transaction.query_row("SELECT CAST(strftime('%s','now') AS INTEGER)", [], |row| {
                row.get::<_, i64>(0)
            })?;
        let current = transaction
            .query_row(
                "SELECT owner, lease_until FROM reconciliation_leader WHERE singleton = 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        let can_lead = current.as_ref().is_none_or(|(current_owner, lease_until)| {
            current_owner == owner || *lease_until <= now
        });
        if !can_lead {
            transaction.commit()?;
            return Ok(false);
        }
        let replacing_expired_leader = current.is_none()
            || current
                .as_ref()
                .is_some_and(|(current_owner, lease_until)| {
                    current_owner != owner && *lease_until <= now
                });
        if replacing_expired_leader {
            transaction.execute(
                "UPDATE reconciliation_queue SET state = 'queued' WHERE state = 'running'",
                [],
            )?;
        }
        transaction.execute(
            "INSERT INTO reconciliation_leader(singleton, owner, lease_until) VALUES (1, ?1, ?2) \
             ON CONFLICT(singleton) DO UPDATE SET owner = excluded.owner, lease_until = excluded.lease_until",
            params![owner, now.saturating_add(lease_seconds.max(1))],
        )?;
        transaction.commit()?;
        Ok(true)
    }

    /// Claims due work and marks it running before the network request begins.
    /// Cross-process exclusivity is provided by `acquire_reconciliation_leadership`.
    pub(crate) fn claim_reconciliation_work(
        &mut self,
    ) -> Result<Option<ReconciliationWork>, StoreError> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "DELETE FROM reconciliation_queue WHERE kind = 'device' AND EXISTS (SELECT 1 FROM tombstones WHERE tombstones.realm = reconciliation_queue.realm AND tombstones.kind = 'device' AND reconciliation_queue.work_id = 'device:' || tombstones.item_id)",
            [],
        )?;
        let row = transaction
            .query_row(
                "SELECT work_id, realm, kind, payload_json, attempts FROM reconciliation_queue WHERE state = 'queued' AND (not_before IS NULL OR CAST(not_before AS INTEGER) <= CAST(strftime('%s','now') AS INTEGER)) ORDER BY rowid LIMIT 1",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, String>(3)?, row.get::<_, u32>(4)?)),
            )
            .optional()?;
        let Some((work_id, realm, kind, payload, attempts)) = row else {
            transaction.commit()?;
            return Ok(None);
        };
        transaction.execute(
            "UPDATE reconciliation_queue SET state = 'running' WHERE work_id = ?1",
            [&work_id],
        )?;
        transaction.commit()?;
        Ok(Some(ReconciliationWork {
            work_id,
            realm: realm_from_key(&realm),
            kind,
            payload: serde_json::from_str(&payload)?,
            attempts,
        }))
    }

    /// Completes successfully claimed work.
    pub(crate) fn complete_reconciliation_work(&mut self, work_id: &str) -> Result<(), StoreError> {
        self.connection.execute(
            "DELETE FROM reconciliation_queue WHERE work_id = ?1",
            [work_id],
        )?;
        Ok(())
    }

    /// Requeues failed work with bounded exponential backoff. `running` rows
    /// are also returned to `queued` on open, so a crash cannot lose work.
    pub(crate) fn retry_reconciliation_work(&mut self, work_id: &str) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE reconciliation_queue SET state = 'queued', attempts = attempts + 1, not_before = CAST(strftime('%s','now') AS INTEGER) + MIN(300, (1 << MIN(16, attempts + 1))) WHERE work_id = ?1",
            [work_id],
        )?;
        Ok(())
    }

    /// Removes every device observation in `realm` (simulation cleanup on
    /// abandonment/completion/expiry), returning the removed keys so the
    /// in-memory snapshot can be pruned identically.
    pub(crate) fn purge_realm_devices(
        &mut self,
        realm: &Realm,
    ) -> Result<Vec<DeviceKey>, StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        let key = realm_key(realm);
        let mut statement =
            transaction.prepare("SELECT device_id FROM devices WHERE realm = ?1")?;
        let rows = statement.query_map([&key], |row| row.get::<_, String>(0))?;
        let mut removed = Vec::new();
        for row in rows {
            removed.push(row?);
        }
        drop(statement);
        transaction.execute("DELETE FROM devices WHERE realm = ?1", [&key])?;
        Self::commit(transaction, fail_commit)?;
        Ok(removed
            .into_iter()
            .map(|id| DeviceKey::in_realm(realm.clone(), DeviceId::new(id)))
            .collect())
    }

    /// Upserts a simulation run's current observation (start, then later its
    /// archived result). Simulation rows are never deleted: they are the
    /// account's simulation history, distinct from the simulation realm's
    /// ephemeral device projections.
    pub(crate) fn persist_simulation(
        &mut self,
        simulation: &Observation<Simulation>,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO simulations(simulation_id, payload_json) VALUES (?1, ?2) ON CONFLICT(simulation_id) DO UPDATE SET payload_json = excluded.payload_json",
            params![simulation.value.id.get(), serde_json::to_string(simulation)?],
        )?;
        Ok(())
    }

    pub(crate) fn restore_simulations(
        &self,
    ) -> Result<BTreeMap<SimulationId, Observation<Simulation>>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT payload_json FROM simulations ORDER BY simulation_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut simulations = BTreeMap::new();
        for row in rows {
            let observation: Observation<Simulation> = serde_json::from_str(&row?)?;
            simulations.insert(observation.value.id, observation);
        }
        Ok(simulations)
    }

    /// Commits a targeted inventory observation. Location and replicant
    /// inventory reads share this; account-level ownership is always `live`.
    pub(crate) fn persist_inventory(
        &mut self,
        inventory: &Observation<Inventory>,
    ) -> Result<(), StoreError> {
        let (realm, owner_kind, owner_id) = inventory_owner_key(&inventory.value.owner);
        self.connection.execute(
            "INSERT INTO inventories(realm, owner_kind, owner_id, inventory_json) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(realm, owner_kind, owner_id) DO UPDATE SET inventory_json = excluded.inventory_json",
            params![realm, owner_kind, owner_id, serde_json::to_string(inventory)?],
        )?;
        Ok(())
    }

    pub(crate) fn restore_inventories(
        &self,
    ) -> Result<BTreeMap<InventoryOwner, Observation<Inventory>>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT inventory_json FROM inventories ORDER BY realm, owner_kind, owner_id",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut inventories = BTreeMap::new();
        for row in rows {
            let observation = serde_json::from_str::<Observation<Inventory>>(&row?)?;
            inventories.insert(observation.value.owner.clone(), observation);
        }
        Ok(inventories)
    }

    pub(crate) fn restore_messages(&self) -> Result<Vec<Observation<Message>>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT payload_json FROM messages")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut messages = rows
            .map(|row| {
                row.map_err(StoreError::from).and_then(|value| {
                    serde_json::from_str::<Observation<Message>>(&value).map_err(StoreError::from)
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        messages.sort_by(|left, right| {
            right
                .value
                .created_at
                .cmp(&left.value.created_at)
                .then_with(|| right.value.id.cmp(&left.value.id))
        });
        Ok(messages)
    }

    pub(crate) fn persist_messages(
        &mut self,
        messages: &[Observation<Message>],
    ) -> Result<(), StoreError> {
        if messages.is_empty() {
            return Ok(());
        }
        let transaction = self.connection.transaction()?;
        {
            let mut statement = transaction.prepare(
                "INSERT INTO messages(message_id, payload_json) VALUES (?1, ?2) ON CONFLICT(message_id) DO UPDATE SET payload_json = excluded.payload_json",
            )?;
            for message in messages {
                let key = match message.value.id {
                    Some(id) => id.to_string(),
                    None => format!("anonymous:{}", serde_json::to_string(&message.value)?),
                };
                statement.execute(params![key, serde_json::to_string(message)?])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }
    pub(crate) fn commit_messages_and_metadata(
        &mut self,
        messages: &[Observation<Message>],
        mut metadata: MessageMetadata,
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        let current_revision = transaction
            .query_row(
                "SELECT revision FROM message_metadata WHERE singleton = 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .unwrap_or(0)
            .max(0) as u64;
        let mut preserved_read_ids = BTreeSet::new();
        let mut statement = transaction.prepare(
            "INSERT INTO messages(message_id, payload_json) VALUES (?1, ?2) ON CONFLICT(message_id) DO UPDATE SET payload_json = excluded.payload_json",
        )?;
        for message in messages {
            let key = match message.value.id {
                Some(id) => id.to_string(),
                None => format!("anonymous:{}", serde_json::to_string(&message.value)?),
            };
            let mut merged = message.clone();
            if let Some(existing) = transaction
                .query_row(
                    "SELECT payload_json FROM messages WHERE message_id = ?1",
                    params![key],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
            {
                let existing = serde_json::from_str::<Observation<Message>>(&existing)?;
                if existing.value.is_read == Some(true) && merged.value.is_read != Some(true) {
                    if merged.value.is_read == Some(false) {
                        preserved_read_ids.insert(key.clone());
                    }
                    merged.value.is_read = Some(true);
                }
            }
            statement.execute(params![key, serde_json::to_string(&merged)?])?;
        }
        drop(statement);
        if let Some(unread_count) = metadata.unread_count {
            metadata.unread_count = Some(
                unread_count
                    .saturating_sub(i64::try_from(preserved_read_ids.len()).unwrap_or(i64::MAX)),
            );
        }
        metadata.revision = current_revision.saturating_add(1);
        transaction.execute(
            "INSERT INTO message_metadata(singleton, last_cursor, unread_count, refreshed_at, revision, last_error) VALUES (1, ?1, ?2, ?3, ?4, ?5) ON CONFLICT(singleton) DO UPDATE SET last_cursor = excluded.last_cursor, unread_count = excluded.unread_count, refreshed_at = excluded.refreshed_at, revision = excluded.revision, last_error = excluded.last_error",
            params![
                metadata.last_cursor,
                metadata.unread_count,
                metadata.refreshed_at.map(ObservationTime::unix_millis),
                i64::try_from(metadata.revision).unwrap_or(i64::MAX),
                metadata.last_error,
            ],
        )?;
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn persist_message_error(&mut self, error: &str) -> Result<(), StoreError> {
        let bounded = error.chars().take(512).collect::<String>();
        self.connection.execute(
            "INSERT INTO message_metadata(singleton, last_error) VALUES (1, ?1) ON CONFLICT(singleton) DO UPDATE SET last_error = excluded.last_error",
            params![bounded],
        )?;
        Ok(())
    }
    pub(crate) fn message_metadata(&self) -> Result<MessageMetadata, StoreError> {
        self.connection
            .query_row(
                "SELECT last_cursor, unread_count, refreshed_at, revision, last_error FROM message_metadata WHERE singleton = 1",
                [],
                |row| {
                    Ok(MessageMetadata {
                        last_cursor: row.get(0)?,
                        unread_count: row.get(1)?,
                        refreshed_at: row
                            .get::<_, Option<i64>>(2)?
                            .map(ObservationTime::from_unix_millis),
                        revision: row.get::<_, i64>(3)?.max(0) as u64,
                        last_error: row.get(4)?,
                    })
                },
            )
            .optional()
            .map(Option::unwrap_or_default)
            .map_err(StoreError::from)
    }

    pub(crate) fn persist_message_metadata(
        &mut self,
        metadata: MessageMetadata,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO message_metadata(singleton, last_cursor, unread_count, refreshed_at, revision, last_error) VALUES (1, ?1, ?2, ?3, ?4, ?5) ON CONFLICT(singleton) DO UPDATE SET last_cursor = excluded.last_cursor, unread_count = excluded.unread_count, refreshed_at = excluded.refreshed_at, revision = MAX(message_metadata.revision, excluded.revision), last_error = excluded.last_error",
            params![
                metadata.last_cursor,
                metadata.unread_count,
                metadata.refreshed_at.map(ObservationTime::unix_millis),
                i64::try_from(metadata.revision).unwrap_or(i64::MAX),
                metadata.last_error,
            ],
        )?;
        Ok(())
    }

    pub(crate) fn restore_resource_sites(
        &self,
    ) -> Result<Vec<Observation<ResourceSite>>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT payload_json FROM resource_sites ORDER BY realm, site_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| {
            row.map_err(StoreError::from).and_then(|value| {
                serde_json::from_str::<Observation<ResourceSite>>(&value).map_err(StoreError::from)
            })
        })
        .collect()
    }

    pub(crate) fn restore_location_events(
        &self,
    ) -> Result<Vec<Observation<LocationEvent>>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT payload_json FROM location_events ORDER BY realm, event_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| {
            row.map_err(StoreError::from).and_then(|value| {
                serde_json::from_str::<Observation<LocationEvent>>(&value).map_err(StoreError::from)
            })
        })
        .collect()
    }

    pub(crate) fn restore_incoming_objects(
        &self,
    ) -> Result<Vec<Observation<IncomingObject>>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT payload_json FROM discovery_data WHERE kind = 'incoming_object' ORDER BY realm, item_id",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.map(|row| {
            row.map_err(StoreError::from).and_then(|value| {
                serde_json::from_str::<Observation<IncomingObject>>(&value)
                    .map_err(StoreError::from)
            })
        })
        .collect()
    }

    pub(crate) fn event_cursor(&self) -> Result<Option<String>, StoreError> {
        self.connection
            .query_row(
                "SELECT cursor FROM event_cursors WHERE stream = 'account'",
                [],
                |row| row.get(0),
            )
            .optional()
            .map_err(StoreError::from)
    }

    fn commit(transaction: Transaction<'_>, fail_commit: bool) -> Result<(), StoreError> {
        if fail_commit {
            return Err(StoreError::InjectedCommitFailure);
        }
        transaction.commit()?;
        Ok(())
    }

    #[cfg(test)]
    fn take_commit_failure(&mut self) -> bool {
        std::mem::take(&mut self.fail_next_commit)
    }

    #[cfg(not(test))]
    fn take_commit_failure(&mut self) -> bool {
        false
    }

    #[cfg(test)]
    pub(crate) fn fail_next_commit(&mut self) {
        self.fail_next_commit = true;
    }

    #[cfg(test)]
    pub(crate) fn foreign_keys_enabled(&self) -> Result<i64, StoreError> {
        self.connection
            .query_row("PRAGMA foreign_keys", [], |row| row.get(0))
            .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) fn journal_mode(&self) -> Result<String, StoreError> {
        self.connection
            .query_row("PRAGMA journal_mode", [], |row| row.get(0))
            .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) fn busy_timeout(&self) -> Result<i64, StoreError> {
        self.connection
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) fn device_count(&self) -> Result<i64, StoreError> {
        self.connection
            .query_row("SELECT COUNT(*) FROM devices", [], |row| row.get(0))
            .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) fn event_count(&self) -> Result<i64, StoreError> {
        self.history
            .query_row(
                "SELECT COUNT(*) FROM event_history WHERE applied_at IS NOT NULL",
                [],
                |row| row.get(0),
            )
            .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) fn projection_row_count(&self) -> Result<i64, StoreError> {
        self.connection
            .query_row(
                "SELECT
                    (SELECT COUNT(*) FROM devices) +
                    (SELECT COUNT(*) FROM replicants) +
                    (SELECT COUNT(*) FROM locations) +
                    (SELECT COUNT(*) FROM stars) +
                    (SELECT COUNT(*) FROM resource_sites) +
                    (SELECT COUNT(*) FROM location_events) +
                    (SELECT COUNT(*) FROM discovery_data WHERE kind = 'incoming_object') +
                    (SELECT COUNT(*) FROM messages) +
                    (SELECT COUNT(*) FROM blueprints) +
                    (SELECT COUNT(*) FROM trades) +
                    (SELECT COUNT(*) FROM simulations) +
                    (SELECT COUNT(*) FROM tombstones)",
                [],
                |row| row.get(0),
            )
            .map_err(StoreError::from)
    }

    #[cfg(test)]
    pub(crate) fn projection_batch_matches(
        &self,
        batch: &EventProjectionBatch,
    ) -> Result<bool, StoreError> {
        for observation in &batch.devices {
            let stored = self
                .connection
                .query_row(
                    "SELECT observation_json FROM devices WHERE realm = ?1 AND device_id = ?2",
                    params![
                        realm_key(&observation.value.key.realm),
                        observation.value.key.id.as_str()
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if stored.as_deref() != Some(serde_json::to_string(observation)?.as_str()) {
                return Ok(false);
            }
        }
        for observation in &batch.replicants {
            let stored = self
                .connection
                .query_row(
                    "SELECT observation_json FROM replicants WHERE realm = ?1 AND replicant_id = ?2",
                    params![
                        realm_key(&observation.value.key.realm),
                        observation.value.key.id.as_str()
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if stored.as_deref() != Some(serde_json::to_string(observation)?.as_str()) {
                return Ok(false);
            }
        }
        for observation in &batch.locations {
            let stored = self
                .connection
                .query_row(
                    "SELECT observation_json FROM locations WHERE realm = ?1 AND location_id = ?2",
                    params![
                        realm_key(&observation.value.key.realm),
                        observation.value.key.id.as_str()
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if stored.as_deref() != Some(serde_json::to_string(observation)?.as_str()) {
                return Ok(false);
            }
        }
        for observation in &batch.stars {
            let stored = self
                .connection
                .query_row(
                    "SELECT payload_json FROM stars WHERE realm = ?1 AND star_id = ?2",
                    params![
                        realm_key(&observation.value.key.realm),
                        observation.value.key.id.as_str()
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if stored.as_deref() != Some(serde_json::to_string(observation)?.as_str()) {
                return Ok(false);
            }
        }
        for observation in &batch.resource_sites {
            let stored = self
                .connection
                .query_row(
                    "SELECT payload_json FROM resource_sites WHERE realm = ?1 AND site_id = ?2",
                    params![
                        realm_key(&observation.value.key.realm),
                        observation.value.key.id.as_str()
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if stored.as_deref() != Some(serde_json::to_string(observation)?.as_str()) {
                return Ok(false);
            }
        }
        for observation in &batch.location_events {
            let stored = self
                .connection
                .query_row(
                    "SELECT payload_json FROM location_events WHERE realm = ?1 AND event_id = ?2",
                    params![
                        realm_key(&observation.value.key.realm),
                        observation.value.key.id.as_str()
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if stored.as_deref() != Some(serde_json::to_string(observation)?.as_str()) {
                return Ok(false);
            }
        }
        for observation in &batch.incoming_objects {
            let stored = self
                .connection
                .query_row(
                    "SELECT payload_json FROM discovery_data WHERE realm = ?1 AND kind = 'incoming_object' AND item_id = ?2",
                    params![
                        realm_key(&observation.value.key.realm),
                        observation.value.key.id.as_str()
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if stored.as_deref() != Some(serde_json::to_string(observation)?.as_str()) {
                return Ok(false);
            }
        }
        for observation in &batch.messages {
            let key = observation.value.id.map_or_else(
                || {
                    format!(
                        "anonymous:{}",
                        observation.metadata.observed_at.unix_millis()
                    )
                },
                |id| id.to_string(),
            );
            let stored = self
                .connection
                .query_row(
                    "SELECT payload_json FROM messages WHERE message_id = ?1",
                    [&key],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if stored.as_deref() != Some(serde_json::to_string(observation)?.as_str()) {
                return Ok(false);
            }
        }
        for observation in &batch.blueprints {
            let stored = self
                .connection
                .query_row(
                    "SELECT payload_json FROM blueprints WHERE blueprint_id = ?1",
                    [observation.value.id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if stored.as_deref() != Some(serde_json::to_string(observation)?.as_str()) {
                return Ok(false);
            }
        }
        for observation in &batch.trades {
            let stored = self
                .connection
                .query_row(
                    "SELECT payload_json FROM trades WHERE realm = ?1 AND trade_id = ?2",
                    params![
                        realm_key(&observation.value.key.realm),
                        observation.value.key.id.as_str()
                    ],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if stored.as_deref() != Some(serde_json::to_string(observation)?.as_str()) {
                return Ok(false);
            }
        }
        for observation in &batch.simulations {
            let stored = self
                .connection
                .query_row(
                    "SELECT payload_json FROM simulations WHERE simulation_id = ?1",
                    [observation.value.id.get()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if stored.as_deref() != Some(serde_json::to_string(observation)?.as_str()) {
                return Ok(false);
            }
        }
        for deletion in &batch.deletions {
            let realm = realm_key(&deletion.realm);
            let evidence = self
                .connection
                .query_row(
                    "SELECT evidence FROM tombstones WHERE realm = ?1 AND kind = ?2 AND item_id = ?3",
                    params![&realm, deletion.kind, &deletion.item_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if evidence.as_deref() != Some(deletion.evidence) {
                return Ok(false);
            }
            let present = match deletion.kind {
                "device" => self.connection.query_row(
                    "SELECT COUNT(*) FROM devices WHERE realm = ?1 AND device_id = ?2",
                    params![&realm, &deletion.item_id],
                    |row| row.get::<_, i64>(0),
                )?,
                "resource_site" => self.connection.query_row(
                    "SELECT COUNT(*) FROM resource_sites WHERE realm = ?1 AND site_id = ?2",
                    params![&realm, &deletion.item_id],
                    |row| row.get::<_, i64>(0),
                )?,
                "location_event" => self.connection.query_row(
                    "SELECT COUNT(*) FROM location_events WHERE realm = ?1 AND event_id = ?2",
                    params![&realm, &deletion.item_id],
                    |row| row.get::<_, i64>(0),
                )?,
                "incoming_object" => self.connection.query_row(
                    "SELECT COUNT(*) FROM discovery_data WHERE realm = ?1 AND kind = 'incoming_object' AND item_id = ?2",
                    params![&realm, &deletion.item_id],
                    |row| row.get::<_, i64>(0),
                )?,
                "trade" => self.connection.query_row(
                    "SELECT COUNT(*) FROM trades WHERE realm = ?1 AND trade_id = ?2",
                    params![&realm, &deletion.item_id],
                    |row| row.get::<_, i64>(0),
                )?,
                kind => return Err(StoreError::UnsupportedProjectionKind(kind)),
            };
            if present != 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
impl Store {
    pub(crate) fn create_refresh_run(
        &mut self,
        run_id: &RefreshRunId,
        mode: RefreshMode,
        phases: &[RefreshPhase],
        read_requests_per_minute: u32,
        now: i64,
    ) -> Result<(), StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO refresh_runs(
                run_id, mode, requested_phases_json, read_requests_per_minute, status,
                created_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, 'queued', ?5, ?5)",
            params![
                run_id.as_str(),
                mode.as_str(),
                serde_json::to_string(phases)?,
                read_requests_per_minute,
                now
            ],
        )?;
        {
            let mut insert = transaction.prepare(
                "INSERT INTO refresh_phase_checkpoints(run_id, phase, status, checkpoint_json, updated_at_ms)
                 VALUES (?1, ?2, 'pending', '{}', ?3)",
            )?;
            for phase in phases {
                insert.execute(params![run_id.as_str(), phase.as_str(), now])?;
            }
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn list_refresh_runs(
        &self,
        limit: usize,
        live_catchup: super::ReadinessComponent,
    ) -> Result<Vec<RefreshRunStatus>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT run_id FROM refresh_runs ORDER BY updated_at_ms DESC, created_at_ms DESC LIMIT ?1",
        )?;
        let ids = statement
            .query_map([i64::try_from(limit).unwrap_or(100)], |row| {
                row.get::<_, String>(0)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);
        ids.into_iter()
            .map(|id| {
                let id = RefreshRunId::from_str(&id).map_err(StoreError::Refresh)?;
                self.refresh_run_status(&id, live_catchup)?
                    .ok_or_else(|| StoreError::Refresh("refresh run disappeared".into()))
            })
            .collect()
    }

    pub(crate) fn refresh_run_status(
        &self,
        run_id: &RefreshRunId,
        live_catchup: super::ReadinessComponent,
    ) -> Result<Option<RefreshRunStatus>, StoreError> {
        type RunRow = (
            String,
            String,
            String,
            Option<String>,
            i64,
            i64,
            Option<i64>,
            Option<String>,
            i64,
            i64,
            Option<i64>,
        );
        let row = self
            .connection
            .query_row(
                "SELECT mode, requested_phases_json, status, current_phase,
                        read_requests_per_minute, request_attempts, retry_not_before_ms,
                        failure_kind, created_at_ms, updated_at_ms, completed_at_ms
                 FROM refresh_runs WHERE run_id = ?1",
                [run_id.as_str()],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                    ))
                },
            )
            .optional()?;
        let Some((
            mode,
            requested,
            status,
            current_phase,
            read_requests_per_minute,
            request_attempts,
            retry_not_before_ms,
            failure_kind,
            created_at_ms,
            updated_at_ms,
            completed_at_ms,
        )): Option<RunRow> = row
        else {
            return Ok(None);
        };
        let mode = match mode.as_str() {
            "apply" => RefreshMode::Apply,
            "dry_run" => RefreshMode::DryRun,
            _ => {
                return Err(StoreError::Refresh(format!(
                    "invalid refresh mode `{mode}`"
                )));
            }
        };
        let requested_phases = serde_json::from_str::<Vec<RefreshPhase>>(&requested)?;
        let status = RefreshRunState::parse(&status).map_err(StoreError::Refresh)?;
        let current_phase = current_phase
            .map(|phase| RefreshPhase::from_str(&phase).map_err(StoreError::Refresh))
            .transpose()?;
        let mut statement = self.connection.prepare(
            "SELECT phase, status, pages, items, request_attempts,
                    proposed_inserts, proposed_updates, proposed_tombstones,
                    applied_inserts, applied_updates, applied_tombstones,
                    retry_not_before_ms, approval_digest, failure_kind, checkpoint_json
             FROM refresh_phase_checkpoints WHERE run_id = ?1",
        )?;
        let rows = statement.query_map([run_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, i64>(7)?,
                row.get::<_, i64>(8)?,
                row.get::<_, i64>(9)?,
                row.get::<_, i64>(10)?,
                row.get::<_, Option<i64>>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, String>(14)?,
            ))
        })?;
        let mut by_phase = BTreeMap::new();
        let mut history_backfilled_through = None;
        for row in rows {
            let (
                phase,
                phase_status,
                pages,
                items,
                attempts,
                proposed_inserts,
                proposed_updates,
                proposed_tombstones,
                applied_inserts,
                applied_updates,
                applied_tombstones,
                retry,
                approval,
                failure,
                checkpoint,
            ) = row?;
            let phase = RefreshPhase::from_str(&phase).map_err(StoreError::Refresh)?;
            if phase == RefreshPhase::Events {
                history_backfilled_through = serde_json::from_str::<Value>(&checkpoint)?
                    .get("before")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
            }
            by_phase.insert(
                phase,
                RefreshPhaseStatus {
                    phase,
                    status: RefreshPhaseState::parse(&phase_status).map_err(StoreError::Refresh)?,
                    pages: pages.max(0) as u64,
                    items: items.max(0) as u64,
                    request_attempts: attempts.max(0) as u64,
                    delta: RefreshDelta {
                        proposed_inserts: proposed_inserts.max(0) as u64,
                        proposed_updates: proposed_updates.max(0) as u64,
                        proposed_tombstones: proposed_tombstones.max(0) as u64,
                        applied_inserts: applied_inserts.max(0) as u64,
                        applied_updates: applied_updates.max(0) as u64,
                        applied_tombstones: applied_tombstones.max(0) as u64,
                    },
                    retry_not_before_ms: retry,
                    approval_digest: approval,
                    failure_kind: failure,
                },
            );
        }
        let phases = requested_phases
            .iter()
            .filter_map(|phase| by_phase.remove(phase))
            .collect::<Vec<_>>();
        let mut delta = RefreshDelta::default();
        for phase in &phases {
            delta += phase.delta;
        }
        let account_complete = phases.iter().any(|phase| {
            phase.phase == RefreshPhase::Account && phase.status == RefreshPhaseState::Complete
        });
        let devices_complete = phases.iter().any(|phase| {
            phase.phase == RefreshPhase::Devices && phase.status == RefreshPhaseState::Complete
        });
        let all_complete = phases
            .iter()
            .all(|phase| phase.status == RefreshPhaseState::Complete);
        let readiness = if mode == RefreshMode::DryRun || !account_complete || !devices_complete {
            RefreshReadiness::Unavailable
        } else if all_complete {
            RefreshReadiness::Complete
        } else {
            RefreshReadiness::RestBaseline
        };
        Ok(Some(RefreshRunStatus {
            run_id: run_id.clone(),
            mode,
            status,
            requested_phases,
            current_phase,
            read_requests_per_minute: u32::try_from(read_requests_per_minute)
                .unwrap_or(MAX_REFRESH_RATE),
            request_attempts: request_attempts.max(0) as u64,
            delta,
            readiness,
            history_backfilled_through,
            live_catchup,
            retry_not_before_ms,
            failure_kind,
            created_at_ms,
            updated_at_ms,
            completed_at_ms,
            phases,
        }))
    }

    pub(crate) fn claim_refresh_run(
        &mut self,
        owner: &str,
        now: i64,
        lease_expires: i64,
    ) -> Result<Option<RefreshRunId>, StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let active: i64 = transaction.query_row(
            "SELECT COUNT(*) FROM refresh_runs
             WHERE status = 'running' AND lease_expires_at_ms > ?1",
            [now],
            |row| row.get(0),
        )?;
        if active > 0 {
            transaction.commit()?;
            return Ok(None);
        }
        let id = transaction
            .query_row(
                "SELECT run_id FROM refresh_runs
                 WHERE cancel_requested = 0 AND (
                    status = 'queued'
                    OR (status = 'running' AND COALESCE(lease_expires_at_ms, 0) <= ?1)
                    OR (status = 'backing_off' AND COALESCE(retry_not_before_ms, 0) <= ?1)
                 )
                 ORDER BY created_at_ms, run_id LIMIT 1",
                [now],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(id) = id else {
            transaction.commit()?;
            return Ok(None);
        };
        transaction.execute(
            "UPDATE refresh_runs SET status = 'running', lease_owner = ?2,
                    lease_expires_at_ms = ?3, updated_at_ms = ?4
             WHERE run_id = ?1",
            params![&id, owner, lease_expires, now],
        )?;
        transaction.commit()?;
        RefreshRunId::from_str(&id)
            .map(Some)
            .map_err(StoreError::Refresh)
    }

    pub(crate) fn begin_refresh_phase(
        &mut self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        owner: &str,
        now: i64,
    ) -> Result<bool, StoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cancelled: i64 = transaction.query_row(
            "SELECT cancel_requested FROM refresh_runs WHERE run_id = ?1",
            [run_id.as_str()],
            |row| row.get(0),
        )?;
        if cancelled != 0 {
            transaction.execute(
                "UPDATE refresh_runs SET status = 'cancelled', current_phase = NULL,
                        completed_at_ms = ?2, lease_owner = NULL, lease_expires_at_ms = NULL,
                        updated_at_ms = ?2 WHERE run_id = ?1",
                params![run_id.as_str(), now],
            )?;
            transaction.execute(
                "UPDATE refresh_phase_checkpoints SET status = 'cancelled', updated_at_ms = ?3
                 WHERE run_id = ?1 AND phase = ?2 AND status != 'complete'",
                params![run_id.as_str(), phase.as_str(), now],
            )?;
            transaction.execute(
                "DELETE FROM refresh_stage WHERE run_id = ?1",
                [run_id.as_str()],
            )?;
            transaction.commit()?;
            return Ok(true);
        }
        transaction.execute(
            "UPDATE refresh_runs SET status = 'running', current_phase = ?2,
                    lease_owner = ?3, lease_expires_at_ms = ?4, updated_at_ms = ?5
             WHERE run_id = ?1",
            params![
                run_id.as_str(),
                phase.as_str(),
                owner,
                now + REFRESH_LEASE_MILLIS,
                now
            ],
        )?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET status = 'running',
                    phase_started_at_ms = COALESCE(phase_started_at_ms, ?3), updated_at_ms = ?3
             WHERE run_id = ?1 AND phase = ?2",
            params![run_id.as_str(), phase.as_str(), now],
        )?;
        transaction.commit()?;
        Ok(false)
    }

    pub(crate) fn complete_refresh_phase(
        &mut self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        now: i64,
    ) -> Result<bool, StoreError> {
        let status: String = self.connection.query_row(
            "SELECT status FROM refresh_phase_checkpoints WHERE run_id = ?1 AND phase = ?2",
            params![run_id.as_str(), phase.as_str()],
            |row| row.get(0),
        )?;
        if status == "awaiting_approval" {
            self.connection.execute(
                "UPDATE refresh_runs SET status = 'awaiting_approval', current_phase = ?2,
                        lease_owner = NULL, lease_expires_at_ms = NULL, updated_at_ms = ?3
                 WHERE run_id = ?1",
                params![run_id.as_str(), phase.as_str(), now],
            )?;
            return Ok(true);
        }
        self.connection.execute(
            "UPDATE refresh_phase_checkpoints SET status = 'complete', updated_at_ms = ?3
             WHERE run_id = ?1 AND phase = ?2",
            params![run_id.as_str(), phase.as_str(), now],
        )?;
        Ok(false)
    }

    pub(crate) fn complete_refresh_run(
        &mut self,
        run_id: &RefreshRunId,
        now: i64,
    ) -> Result<(), StoreError> {
        let mode: String = self.connection.query_row(
            "SELECT mode FROM refresh_runs WHERE run_id = ?1",
            [run_id.as_str()],
            |row| row.get(0),
        )?;
        let status = if mode == "dry_run" {
            "completed_dry_run"
        } else {
            "completed"
        };
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE refresh_runs SET status = ?2, current_phase = NULL, completed_at_ms = ?3,
                    lease_owner = NULL, lease_expires_at_ms = NULL, updated_at_ms = ?3
             WHERE run_id = ?1",
            params![run_id.as_str(), status, now],
        )?;
        transaction.execute(
            "DELETE FROM refresh_stage WHERE run_id = ?1",
            [run_id.as_str()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn fail_refresh_run(
        &mut self,
        run_id: &RefreshRunId,
        failure: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET status = 'failed', failure_kind = ?2,
                    updated_at_ms = ?3
             WHERE run_id = ?1 AND status = 'running'",
            params![run_id.as_str(), failure, now],
        )?;
        transaction.execute(
            "UPDATE refresh_runs SET status = 'failed', failure_kind = ?2,
                    completed_at_ms = ?3, lease_owner = NULL, lease_expires_at_ms = NULL,
                    updated_at_ms = ?3 WHERE run_id = ?1",
            params![run_id.as_str(), failure, now],
        )?;
        transaction.execute(
            "DELETE FROM refresh_stage WHERE run_id = ?1",
            [run_id.as_str()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn backoff_refresh_run(
        &mut self,
        run_id: &RefreshRunId,
        retry_not_before: i64,
        now: i64,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET status = 'backing_off',
                    retry_not_before_ms = ?2, updated_at_ms = ?3
             WHERE run_id = ?1 AND status = 'running'",
            params![run_id.as_str(), retry_not_before, now],
        )?;
        transaction.execute(
            "UPDATE refresh_runs SET status = 'backing_off', retry_not_before_ms = ?2,
                    lease_owner = NULL, lease_expires_at_ms = NULL, updated_at_ms = ?3
             WHERE run_id = ?1",
            params![run_id.as_str(), retry_not_before, now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn finish_cancelled_refresh_run(
        &mut self,
        run_id: &RefreshRunId,
        now: i64,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET status = 'cancelled', updated_at_ms = ?2
             WHERE run_id = ?1 AND status != 'complete'",
            params![run_id.as_str(), now],
        )?;
        transaction.execute(
            "UPDATE refresh_runs SET status = 'cancelled', current_phase = NULL,
                    completed_at_ms = ?2, lease_owner = NULL, lease_expires_at_ms = NULL,
                    updated_at_ms = ?2 WHERE run_id = ?1",
            params![run_id.as_str(), now],
        )?;
        transaction.execute(
            "DELETE FROM refresh_stage WHERE run_id = ?1",
            [run_id.as_str()],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn block_refresh_phase(
        &mut self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        now: i64,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET status = 'blocked',
                    failure_kind = 'dependency', updated_at_ms = ?3
             WHERE run_id = ?1 AND phase = ?2",
            params![run_id.as_str(), phase.as_str(), now],
        )?;
        transaction.execute(
            "UPDATE refresh_runs SET status = 'blocked', current_phase = ?2,
                    failure_kind = 'dependency', lease_owner = NULL,
                    lease_expires_at_ms = NULL, updated_at_ms = ?3 WHERE run_id = ?1",
            params![run_id.as_str(), phase.as_str(), now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn approve_refresh_phase(
        &mut self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        digest: &str,
        now: i64,
    ) -> Result<(), StoreError> {
        let expected = self
            .connection
            .query_row(
                "SELECT approval_digest FROM refresh_phase_checkpoints
                 WHERE run_id = ?1 AND phase = ?2 AND status = 'awaiting_approval'",
                params![run_id.as_str(), phase.as_str()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten()
            .ok_or_else(|| {
                StoreError::Refresh("phase is not awaiting guarded shrink approval".into())
            })?;
        if expected != digest {
            return Err(StoreError::Refresh(
                "refresh approval digest does not match current staging".into(),
            ));
        }
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET status = 'pending', approved_at_ms = ?3,
                    updated_at_ms = ?3 WHERE run_id = ?1 AND phase = ?2",
            params![run_id.as_str(), phase.as_str(), now],
        )?;
        transaction.execute(
            "UPDATE refresh_runs SET status = 'queued', failure_kind = NULL,
                    lease_owner = NULL, lease_expires_at_ms = NULL, updated_at_ms = ?2
             WHERE run_id = ?1",
            params![run_id.as_str(), now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn cancel_refresh_run(
        &mut self,
        run_id: &RefreshRunId,
        now: i64,
    ) -> Result<(), StoreError> {
        let status = self
            .connection
            .query_row(
                "SELECT status FROM refresh_runs WHERE run_id = ?1",
                [run_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::Refresh("unknown refresh run".into()))?;
        if RefreshRunState::parse(&status)
            .map_err(StoreError::Refresh)?
            .terminal()
        {
            return Ok(());
        }
        self.connection.execute(
            "UPDATE refresh_runs SET cancel_requested = 1, updated_at_ms = ?2 WHERE run_id = ?1",
            params![run_id.as_str(), now],
        )?;
        Ok(())
    }

    pub(crate) fn refresh_cancel_requested(
        &self,
        run_id: &RefreshRunId,
    ) -> Result<bool, StoreError> {
        self.connection
            .query_row(
                "SELECT cancel_requested != 0 FROM refresh_runs WHERE run_id = ?1",
                [run_id.as_str()],
                |row| row.get(0),
            )
            .map_err(StoreError::from)
    }

    pub(crate) fn refresh_checkpoint(
        &self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
    ) -> Result<Value, StoreError> {
        let value: String = self.connection.query_row(
            "SELECT checkpoint_json FROM refresh_phase_checkpoints
             WHERE run_id = ?1 AND phase = ?2",
            params![run_id.as_str(), phase.as_str()],
            |row| row.get(0),
        )?;
        serde_json::from_str(&value).map_err(StoreError::from)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn update_refresh_checkpoint(
        &mut self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        checkpoint: &Value,
        pages: u64,
        items: u64,
        enumeration_complete: bool,
        unfiltered: bool,
        now: i64,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET checkpoint_json = ?3, pages = ?4,
                    items = ?5, enumeration_complete = ?6, unfiltered = ?7,
                    updated_at_ms = ?8 WHERE run_id = ?1 AND phase = ?2",
            params![
                run_id.as_str(),
                phase.as_str(),
                serde_json::to_string(checkpoint)?,
                i64::try_from(pages).unwrap_or(i64::MAX),
                i64::try_from(items).unwrap_or(i64::MAX),
                enumeration_complete,
                unfiltered,
                now
            ],
        )?;
        transaction.execute(
            "UPDATE refresh_runs SET lease_expires_at_ms = ?2, updated_at_ms = ?3
             WHERE run_id = ?1 AND status = 'running'",
            params![run_id.as_str(), now + REFRESH_LEASE_MILLIS, now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn update_refresh_attempts(
        &mut self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        attempts: u64,
    ) -> Result<(), StoreError> {
        let attempts = i64::try_from(attempts).unwrap_or(i64::MAX);
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET request_attempts = ?3
             WHERE run_id = ?1 AND phase = ?2",
            params![run_id.as_str(), phase.as_str(), attempts],
        )?;
        transaction.execute(
            "UPDATE refresh_runs SET request_attempts = ?2 WHERE run_id = ?1",
            params![run_id.as_str(), attempts],
        )?;
        transaction.commit()?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn stage_refresh_item(
        &mut self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        key: &str,
        payload: Option<&str>,
        disposition: &str,
        observed_at_ms: Option<i64>,
        delta: RefreshDelta,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        let existed: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM refresh_stage
                           WHERE run_id = ?1 AND phase = ?2 AND item_key = ?3)",
            params![run_id.as_str(), phase.as_str(), key],
            |row| row.get(0),
        )?;
        transaction.execute(
            "INSERT INTO refresh_stage(
                run_id, phase, item_key, payload_json, disposition, observed_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(run_id, phase, item_key) DO UPDATE SET
                payload_json = excluded.payload_json,
                disposition = excluded.disposition,
                observed_at_ms = excluded.observed_at_ms",
            params![
                run_id.as_str(),
                phase.as_str(),
                key,
                payload,
                disposition,
                observed_at_ms
            ],
        )?;
        if !existed {
            transaction.execute(
                "UPDATE refresh_phase_checkpoints SET
                    proposed_inserts = proposed_inserts + ?3,
                    proposed_updates = proposed_updates + ?4,
                    proposed_tombstones = proposed_tombstones + ?5,
                    applied_inserts = applied_inserts + ?6,
                    applied_updates = applied_updates + ?7,
                    applied_tombstones = applied_tombstones + ?8
                 WHERE run_id = ?1 AND phase = ?2",
                params![
                    run_id.as_str(),
                    phase.as_str(),
                    i64::try_from(delta.proposed_inserts).unwrap_or(i64::MAX),
                    i64::try_from(delta.proposed_updates).unwrap_or(i64::MAX),
                    i64::try_from(delta.proposed_tombstones).unwrap_or(i64::MAX),
                    i64::try_from(delta.applied_inserts).unwrap_or(i64::MAX),
                    i64::try_from(delta.applied_updates).unwrap_or(i64::MAX),
                    i64::try_from(delta.applied_tombstones).unwrap_or(i64::MAX),
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn refresh_stage_keys(
        &self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        tombstones_only: bool,
    ) -> Result<Vec<String>, StoreError> {
        let sql = if tombstones_only {
            "SELECT item_key FROM refresh_stage
             WHERE run_id = ?1 AND phase = ?2 AND disposition = 'tombstone_candidate'
             ORDER BY item_key"
        } else {
            "SELECT item_key FROM refresh_stage
             WHERE run_id = ?1 AND phase = ?2 ORDER BY item_key"
        };
        let mut statement = self.connection.prepare(sql)?;
        statement
            .query_map(params![run_id.as_str(), phase.as_str()], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub(crate) fn refresh_stage_payloads(
        &self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
    ) -> Result<Vec<String>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT payload_json FROM refresh_stage
             WHERE run_id = ?1 AND phase = ?2 AND payload_json IS NOT NULL
             ORDER BY item_key",
        )?;
        statement
            .query_map(params![run_id.as_str(), phase.as_str()], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub(crate) fn refresh_stage_prefix(
        &self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        prefix: &str,
    ) -> Result<Vec<(String, String)>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT item_key, payload_json FROM refresh_stage
             WHERE run_id = ?1 AND phase = ?2 AND item_key LIKE ?3 || '%'
                   AND payload_json IS NOT NULL ORDER BY item_key",
        )?;
        statement
            .query_map(params![run_id.as_str(), phase.as_str(), prefix], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(StoreError::from)
    }

    pub(crate) fn refresh_stage_prefix_keys(
        &self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        prefix: &str,
    ) -> Result<Vec<String>, StoreError> {
        self.refresh_stage_prefix(run_id, phase, prefix)
            .map(|rows| rows.into_iter().map(|row| row.0).collect())
    }

    pub(crate) fn mark_refresh_stage_prefix_applied(
        &mut self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        prefix: &str,
    ) -> Result<(), StoreError> {
        let transaction = self.connection.transaction()?;
        let (inserts, updates): (i64, i64) = transaction.query_row(
            "SELECT
                COALESCE(SUM(disposition = 'insert'), 0),
                COALESCE(SUM(disposition = 'update'), 0)
             FROM refresh_stage
             WHERE run_id = ?1 AND phase = ?2 AND item_key LIKE ?3 || '%'",
            params![run_id.as_str(), phase.as_str(), prefix],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET
                applied_inserts = applied_inserts + ?3,
                applied_updates = applied_updates + ?4
             WHERE run_id = ?1 AND phase = ?2",
            params![run_id.as_str(), phase.as_str(), inserts, updates],
        )?;
        transaction.execute(
            "UPDATE refresh_stage SET disposition = 'unchanged'
             WHERE run_id = ?1 AND phase = ?2 AND item_key LIKE ?3 || '%'",
            params![run_id.as_str(), phase.as_str(), prefix],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn discard_refresh_stage_prefix(
        &mut self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        prefix: &str,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "DELETE FROM refresh_stage
             WHERE run_id = ?1 AND phase = ?2 AND item_key LIKE ?3 || '%'",
            params![run_id.as_str(), phase.as_str(), prefix],
        )?;
        Ok(())
    }

    pub(crate) fn append_archived_events(&mut self, events: &[Event]) -> Result<usize, StoreError> {
        let transaction = self.history.transaction()?;
        let mut inserted = 0;
        {
            let mut statement = transaction.prepare(
                "INSERT OR IGNORE INTO event_history(
                    event_id, realm, event_name, category, device_code, replicant_code,
                    star_id, location_id, occurred_at, payload_json, appended_at,
                    applied_at, archived_only, stream_millis, stream_sequence
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                           datetime('now'), NULL, 1, ?11, ?12)",
            )?;
            for event in events {
                let stream = parse_event_id(event.id.as_str()).ok();
                inserted += statement.execute(params![
                    event.id.as_str(),
                    event.realm.as_ref().map(realm_key),
                    event.name.as_str(),
                    event.category.as_str(),
                    event.device.as_ref().map(|key| key.id.as_str()),
                    event.replicant.as_ref().map(|key| key.id.as_str()),
                    event.star.as_ref().map(|key| key.id.as_str()),
                    event.location.as_ref().map(|key| key.id.as_str()),
                    &event.occurred_at,
                    serde_json::to_string(&event.payload)?,
                    stream.map(|value| value.0),
                    stream.map(|value| value.1),
                ])?;
            }
        }
        transaction.commit()?;
        Ok(inserted)
    }

    pub(crate) fn finalize_refresh_devices(
        &mut self,
        run_id: &RefreshRunId,
        mode: RefreshMode,
        seen: &BTreeSet<String>,
        now: i64,
    ) -> Result<(), StoreError> {
        let (complete, unfiltered, started, approved, expected): (
            bool,
            bool,
            Option<i64>,
            Option<i64>,
            Option<String>,
        ) = self.connection.query_row(
            "SELECT enumeration_complete, unfiltered, phase_started_at_ms,
                    approved_at_ms, approval_digest
             FROM refresh_phase_checkpoints WHERE run_id = ?1 AND phase = 'devices'",
            [run_id.as_str()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;
        if !complete || !unfiltered {
            return Err(StoreError::Refresh(
                "device absence finalizer requires terminal unfiltered proof".into(),
            ));
        }
        let started = started.ok_or_else(|| {
            StoreError::Refresh("device refresh phase watermark is missing".into())
        })?;
        let mut statement = self.connection.prepare(
            "SELECT device_id, observed_at, observation_json FROM devices
             WHERE realm = 'live' AND access_scope = 'owned'",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut eligible = 0usize;
        let mut missing = Vec::new();
        for row in rows {
            let (id, observed_at, payload) = row?;
            let observation = serde_json::from_str::<Observation<Device>>(&payload)?;
            if observation.metadata.reachability == crate::domain::Reachability::Reachable {
                eligible += 1;
                if observed_at <= started && !seen.contains(&id) {
                    missing.push(id);
                }
            }
        }
        drop(statement);
        if seen.is_empty() && eligible > 0 {
            return Err(StoreError::Refresh(
                "empty device enumeration cannot remove non-empty local state".into(),
            ));
        }
        let digest = refresh_removal_digest("GET /v1/devices?limit=50", started, seen);
        if expected.as_deref().is_some_and(|value| value != digest) && approved.is_some() {
            return Err(StoreError::Refresh(
                "approved device refresh digest became stale".into(),
            ));
        }
        let shrink = if eligible == 0 {
            0.0
        } else {
            missing.len() as f64 / eligible as f64
        };
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET local_count = ?3, upstream_count = ?4,
                    membership_digest = ?5, approval_digest = ?5,
                    proposed_tombstones = ?6, updated_at_ms = ?7
             WHERE run_id = ?1 AND phase = ?2",
            params![
                run_id.as_str(),
                RefreshPhase::Devices.as_str(),
                i64::try_from(eligible).unwrap_or(i64::MAX),
                i64::try_from(seen.len()).unwrap_or(i64::MAX),
                &digest,
                i64::try_from(missing.len()).unwrap_or(i64::MAX),
                now
            ],
        )?;
        if mode == RefreshMode::DryRun {
            transaction.commit()?;
            return Ok(());
        }
        if shrink > 0.20 && approved.is_none() {
            transaction.execute(
                "UPDATE refresh_phase_checkpoints SET status = 'awaiting_approval'
                 WHERE run_id = ?1 AND phase = 'devices'",
                [run_id.as_str()],
            )?;
            transaction.commit()?;
            return Ok(());
        }
        for id in &missing {
            transaction.execute(
                "DELETE FROM devices WHERE realm = 'live' AND device_id = ?1
                 AND observed_at <= ?2",
                params![id, started],
            )?;
            transaction.execute(
                "INSERT OR REPLACE INTO tombstones(realm, kind, item_id, removed_at, evidence)
                 VALUES ('live', 'device', ?1, datetime('now'), ?2)",
                params![id, format!("full-refresh:{run_id}:devices:{digest}")],
            )?;
        }
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET applied_tombstones = ?2
             WHERE run_id = ?1 AND phase = 'devices'",
            params![
                run_id.as_str(),
                i64::try_from(missing.len()).unwrap_or(i64::MAX)
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub(crate) fn finalize_refresh_stars(
        &mut self,
        run_id: &RefreshRunId,
        mode: RefreshMode,
        stars: &[Observation<Star>],
        generated_at: Option<&str>,
        now: i64,
    ) -> Result<(), StoreError> {
        let (complete, unfiltered, started, approved, expected): (
            bool,
            bool,
            Option<i64>,
            Option<i64>,
            Option<String>,
        ) = self.connection.query_row(
            "SELECT enumeration_complete, unfiltered, phase_started_at_ms,
                    approved_at_ms, approval_digest
             FROM refresh_phase_checkpoints WHERE run_id = ?1 AND phase = 'stars'",
            [run_id.as_str()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )?;
        if !complete || !unfiltered {
            return Err(StoreError::Refresh(
                "star absence finalizer requires complete response proof".into(),
            ));
        }
        let started = started
            .ok_or_else(|| StoreError::Refresh("star refresh phase watermark is missing".into()))?;
        let seen = stars
            .iter()
            .map(|star| star.value.key.id.as_str().to_owned())
            .collect::<BTreeSet<_>>();
        let mut statement = self
            .connection
            .prepare("SELECT star_id, payload_json FROM stars WHERE realm = 'live'")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut eligible = 0usize;
        let mut missing = Vec::new();
        for row in rows {
            let (id, payload) = row?;
            eligible += 1;
            let observation = serde_json::from_str::<Observation<Star>>(&payload)?;
            if observation.metadata.observed_at.unix_millis() <= started && !seen.contains(&id) {
                missing.push(id);
            }
        }
        drop(statement);
        if seen.is_empty() && eligible > 0 {
            return Err(StoreError::Refresh(
                "empty star catalogue cannot remove non-empty local state".into(),
            ));
        }
        let digest = refresh_removal_digest("GET /v1/stars", started, &seen);
        if expected.as_deref().is_some_and(|value| value != digest) && approved.is_some() {
            return Err(StoreError::Refresh(
                "approved star refresh digest became stale".into(),
            ));
        }
        let shrink = if eligible == 0 {
            0.0
        } else {
            missing.len() as f64 / eligible as f64
        };
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET local_count = ?3, upstream_count = ?4,
                    membership_digest = ?5, approval_digest = ?5,
                    proposed_tombstones = ?6, updated_at_ms = ?7
             WHERE run_id = ?1 AND phase = ?2",
            params![
                run_id.as_str(),
                RefreshPhase::Stars.as_str(),
                i64::try_from(eligible).unwrap_or(i64::MAX),
                i64::try_from(seen.len()).unwrap_or(i64::MAX),
                &digest,
                i64::try_from(missing.len()).unwrap_or(i64::MAX),
                now
            ],
        )?;
        if mode == RefreshMode::DryRun {
            transaction.commit()?;
            return Ok(());
        }
        if shrink > 0.20 && approved.is_none() {
            transaction.execute(
                "UPDATE refresh_phase_checkpoints SET status = 'awaiting_approval'
                 WHERE run_id = ?1 AND phase = 'stars'",
                [run_id.as_str()],
            )?;
            transaction.commit()?;
            return Ok(());
        }
        for star in stars {
            let existing = transaction
                .query_row(
                    "SELECT payload_json FROM stars WHERE realm = ?1 AND star_id = ?2",
                    params![realm_key(&star.value.key.realm), star.value.key.id.as_str()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?
                .map(|payload| serde_json::from_str::<Observation<Star>>(&payload))
                .transpose()?;
            let star = existing
                .map(|current| merge_catalogue_account_knowledge(star.clone(), current))
                .unwrap_or_else(|| star.clone());
            transaction.execute(
                "INSERT INTO stars(realm, star_id, payload_json) VALUES (?1, ?2, ?3)
                 ON CONFLICT(realm, star_id) DO UPDATE SET payload_json = excluded.payload_json",
                params![
                    realm_key(&star.value.key.realm),
                    star.value.key.id.as_str(),
                    serde_json::to_string(&star)?
                ],
            )?;
        }
        for id in &missing {
            transaction.execute(
                "DELETE FROM stars WHERE realm = 'live' AND star_id = ?1",
                [id],
            )?;
            transaction.execute(
                "INSERT OR REPLACE INTO tombstones(realm, kind, item_id, removed_at, evidence)
                 VALUES ('live', 'star', ?1, datetime('now'), ?2)",
                params![id, format!("full-refresh:{run_id}:stars:{digest}")],
            )?;
        }
        transaction.execute(
            "INSERT INTO catalogue_metadata(singleton, generated_at) VALUES (1, ?1)
             ON CONFLICT(singleton) DO UPDATE SET generated_at = excluded.generated_at",
            [generated_at],
        )?;
        transaction.execute(
            "UPDATE refresh_phase_checkpoints SET applied_tombstones = ?2
             WHERE run_id = ?1 AND phase = 'stars'",
            params![
                run_id.as_str(),
                i64::try_from(missing.len()).unwrap_or(i64::MAX)
            ],
        )?;
        transaction.commit()?;
        Ok(())
    }
}

const MAX_REFRESH_RATE: u32 = 60;

fn refresh_removal_digest(endpoint: &str, started: i64, seen: &BTreeSet<String>) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(endpoint.as_bytes());
    digest.update(started.to_be_bytes());
    digest.update(b"terminal-unfiltered");
    for key in seen {
        digest.update((key.len() as u64).to_be_bytes());
        digest.update(key.as_bytes());
    }
    format!("{:x}", digest.finalize())
}

fn migrate_device_relationship_observations(
    transaction: &Transaction<'_>,
) -> Result<(), StoreError> {
    let mut statement =
        transaction.prepare("SELECT realm, device_id, observation_json FROM devices")?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;
    let rows = rows.collect::<Result<Vec<_>, _>>()?;
    drop(statement);
    for (realm, device_id, observation_json) in rows {
        let mut observation: Value = serde_json::from_str(&observation_json)?;
        let Some(relationships) = observation
            .pointer_mut("/value/relationships")
            .and_then(Value::as_object_mut)
        else {
            continue;
        };
        if let Some(assigned_replicant) = relationships.remove("hosted_by") {
            relationships
                .entry("assigned_replicant")
                .or_insert(assigned_replicant);
            transaction.execute(
                "UPDATE devices SET observation_json = ?3 WHERE realm = ?1 AND device_id = ?2",
                params![realm, device_id, serde_json::to_string(&observation)?],
            )?;
        }
    }
    Ok(())
}

fn merge_catalogue_account_knowledge(
    mut catalogue: Observation<Star>,
    current: Observation<Star>,
) -> Observation<Star> {
    catalogue.value.knowledge_observed |= current.value.knowledge_observed;
    catalogue.value.explored = match (catalogue.value.explored, current.value.explored) {
        (Some(left), Some(right)) => Some(left || right),
        (left @ Some(_), None) => left,
        (None, right) => right,
    };
    catalogue.value.has_life = match (catalogue.value.has_life, current.value.has_life) {
        (Some(left), Some(right)) => Some(left || right),
        (left @ Some(_), None) => left,
        (None, right) => right,
    };
    catalogue.value.has_ward = match (catalogue.value.has_ward, current.value.has_ward) {
        (Some(left), Some(right)) => Some(left || right),
        (left @ Some(_), None) => left,
        (None, right) => right,
    };
    catalogue
}

fn merge_migrated_star_knowledge(
    mut current: Observation<Star>,
    incoming: Observation<Star>,
) -> Observation<Star> {
    if current.value.spectral_type.is_none() {
        current.value.spectral_type = incoming.value.spectral_type;
    }
    if current.value.entry_point.is_none() {
        current.value.entry_point = incoming.value.entry_point;
    }
    if current.value.position.is_none() {
        current.value.position = incoming.value.position;
    }
    if current.value.has_hub.is_none() {
        current.value.has_hub = incoming.value.has_hub;
    }
    if current.value.has_ward.is_none() {
        current.value.has_ward = incoming.value.has_ward;
    }
    current.value.knowledge_observed |= incoming.value.knowledge_observed;
    current.value.explored = match (current.value.explored, incoming.value.explored) {
        (Some(left), Some(right)) => Some(left || right),
        (left @ Some(_), None) => left,
        (None, right) => right,
    };
    current.value.has_life = match (current.value.has_life, incoming.value.has_life) {
        (Some(left), Some(right)) => Some(left || right),
        (left @ Some(_), None) => left,
        (None, right) => right,
    };
    if current.value.region.is_none() {
        current.value.region = incoming.value.region;
    }
    current
}

fn migrate_refresh_history(history: &mut Connection) -> Result<(), StoreError> {
    let transaction = history.transaction()?;
    transaction.execute_batch(HISTORY_REFRESH_SCHEMA)?;
    let event_ids = {
        let mut statement = transaction.prepare("SELECT event_id FROM event_history")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<Result<Vec<_>, _>>()?
    };
    {
        let mut update = transaction.prepare(
            "UPDATE event_history SET stream_millis = ?2, stream_sequence = ?3 WHERE event_id = ?1",
        )?;
        for event_id in event_ids {
            if let Ok((milliseconds, sequence)) = parse_event_id(&event_id) {
                update.execute(params![event_id, milliseconds, sequence])?;
            }
        }
    }
    transaction.execute(
        "INSERT INTO history_schema_metadata(key, value) VALUES ('schema_version', '2') \
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        [],
    )?;
    transaction.commit()?;
    Ok(())
}

fn table_exists(connection: &Connection, table: &str) -> Result<bool, StoreError> {
    connection
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |_| Ok(()),
        )
        .optional()
        .map(|found| found.is_some())
        .map_err(StoreError::from)
}

fn insert_history_event(
    statement: &mut rusqlite::Statement<'_>,
    event: &Event,
    appended_at: &str,
) -> Result<(), StoreError> {
    let stream = parse_event_id(event.id.as_str()).ok();
    statement.execute(params![
        event.id.as_str(),
        event.realm.as_ref().map(realm_key),
        event.name.as_str(),
        event.category.as_str(),
        event.device.as_ref().map(|key| key.id.as_str()),
        event.replicant.as_ref().map(|key| key.id.as_str()),
        event.star.as_ref().map(|key| key.id.as_str()),
        event.location.as_ref().map(|key| key.id.as_str()),
        &event.occurred_at,
        serde_json::to_string(&event.payload)?,
        appended_at,
        stream.map(|value| value.0),
        stream.map(|value| value.1),
    ])?;
    Ok(())
}

type HistoryEventRow = (
    String,
    Option<String>,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    String,
);

fn history_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<HistoryEventRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
    ))
}

fn decode_history_event(row: HistoryEventRow) -> Result<Event, StoreError> {
    let (
        event_id,
        realm_key_value,
        event_name,
        category,
        device_code,
        replicant_code,
        star_id,
        location_id,
        occurred_at,
        payload_json,
    ) = row;
    let realm = realm_key_value.as_deref().map(realm_from_key);
    Ok(Event {
        id: crate::domain::EventId::new(event_id),
        name: crate::domain::EventName::from(event_name),
        category: crate::domain::EventCategory::from(category),
        device: realm
            .clone()
            .zip(device_code)
            .map(|(realm, code)| DeviceKey::in_realm(realm, DeviceId::new(code))),
        replicant: realm
            .clone()
            .zip(replicant_code)
            .map(|(realm, code)| ReplicantKey::in_realm(realm, ReplicantId::new(code))),
        location: realm
            .clone()
            .zip(location_id)
            .map(|(realm, id)| LocationKey::in_realm(realm, LocationId::new(id))),
        star: realm
            .clone()
            .zip(star_id)
            .map(|(realm, id)| StarKey::in_realm(realm, StarId::new(id))),
        realm,
        occurred_at,
        payload: serde_json::from_str(&payload_json)?,
    })
}

/// Parses a Redis stream ID as its numeric `<milliseconds>-<sequence>` pair.
fn parse_event_id(value: &str) -> Result<(i64, i64), StoreError> {
    let Some((milliseconds, sequence)) = value.split_once('-') else {
        return Err(StoreError::InvalidEventId(value.into()));
    };
    if sequence.contains('-') {
        return Err(StoreError::InvalidEventId(value.into()));
    }
    let milliseconds = milliseconds
        .parse()
        .map_err(|_| StoreError::InvalidEventId(value.into()))?;
    let sequence = sequence
        .parse()
        .map_err(|_| StoreError::InvalidEventId(value.into()))?;
    Ok((milliseconds, sequence))
}

/// Redis stream IDs are `<milliseconds>-<sequence>` decimal pairs. They are
/// ordered numerically; lexical comparison misorders values such as `10-0`
/// and `9-999`.
fn compare_event_ids(left: &str, right: &str) -> Result<Ordering, StoreError> {
    if left == right {
        return Ok(Ordering::Equal);
    }
    Ok(parse_event_id(left)?.cmp(&parse_event_id(right)?))
}

/// Advances the applied account cursor only when `cursor` is numerically newer
/// than the durable Redis stream ID already present in this transaction.
fn advance_event_cursor(transaction: &Transaction<'_>, cursor: &str) -> Result<(), StoreError> {
    let previous: Option<String> = transaction
        .query_row(
            "SELECT cursor FROM event_cursors WHERE stream = 'account'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if previous
        .as_deref()
        .map(|previous| compare_event_ids(cursor, previous))
        .transpose()?
        .is_none_or(|ordering| ordering == Ordering::Greater)
    {
        transaction.execute(
            "INSERT INTO event_cursors(stream, cursor, updated_at) VALUES ('account', ?1, datetime('now')) ON CONFLICT(stream) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at",
            [cursor],
        )?;
    }
    Ok(())
}

fn persist_projection_batch(
    transaction: &Transaction<'_>,
    batch: &EventProjectionBatch,
) -> Result<(), StoreError> {
    for device in &batch.devices {
        persist_device(transaction, device)?;
    }
    for replicant in &batch.replicants {
        transaction.execute(
            "INSERT INTO replicants(realm, replicant_id, observation_json) VALUES (?1, ?2, ?3) ON CONFLICT(realm, replicant_id) DO UPDATE SET observation_json = excluded.observation_json",
            params![
                realm_key(&replicant.value.key.realm),
                replicant.value.key.id.as_str(),
                serde_json::to_string(replicant)?
            ],
        )?;
    }
    for location in &batch.locations {
        transaction.execute(
            "INSERT INTO locations(realm, location_id, observation_json) VALUES (?1, ?2, ?3) ON CONFLICT(realm, location_id) DO UPDATE SET observation_json = excluded.observation_json",
            params![
                realm_key(&location.value.key.realm),
                location.value.key.id.as_str(),
                serde_json::to_string(location)?
            ],
        )?;
    }
    for star in &batch.stars {
        transaction.execute(
            "INSERT INTO stars(realm, star_id, payload_json) VALUES (?1, ?2, ?3) ON CONFLICT(realm, star_id) DO UPDATE SET payload_json = excluded.payload_json",
            params![
                realm_key(&star.value.key.realm),
                star.value.key.id.as_str(),
                serde_json::to_string(star)?
            ],
        )?;
    }
    for site in &batch.resource_sites {
        transaction.execute(
            "INSERT INTO resource_sites(realm, site_id, payload_json) VALUES (?1, ?2, ?3) ON CONFLICT(realm, site_id) DO UPDATE SET payload_json = excluded.payload_json",
            params![
                realm_key(&site.value.key.realm),
                site.value.key.id.as_str(),
                serde_json::to_string(site)?
            ],
        )?;
    }
    for location_event in &batch.location_events {
        transaction.execute(
            "INSERT INTO location_events(realm, event_id, payload_json) VALUES (?1, ?2, ?3) ON CONFLICT(realm, event_id) DO UPDATE SET payload_json = excluded.payload_json",
            params![
                realm_key(&location_event.value.key.realm),
                location_event.value.key.id.as_str(),
                serde_json::to_string(location_event)?
            ],
        )?;
    }
    for object in &batch.incoming_objects {
        transaction.execute(
            "INSERT INTO discovery_data(realm, kind, item_id, payload_json) VALUES (?1, 'incoming_object', ?2, ?3) ON CONFLICT(realm, kind, item_id) DO UPDATE SET payload_json = excluded.payload_json",
            params![
                realm_key(&object.value.key.realm),
                object.value.key.id.as_str(),
                serde_json::to_string(object)?
            ],
        )?;
    }
    for message in &batch.messages {
        let key = message.value.id.map_or_else(
            || format!("anonymous:{}", message.metadata.observed_at.unix_millis()),
            |id| id.to_string(),
        );
        transaction.execute(
            "INSERT INTO messages(message_id, payload_json) VALUES (?1, ?2) ON CONFLICT(message_id) DO UPDATE SET payload_json = excluded.payload_json",
            params![key, serde_json::to_string(message)?],
        )?;
    }
    for blueprint in &batch.blueprints {
        transaction.execute(
            "INSERT INTO blueprints(blueprint_id, payload_json) VALUES (?1, ?2) ON CONFLICT(blueprint_id) DO UPDATE SET payload_json = excluded.payload_json",
            params![
                blueprint.value.id.as_str(),
                serde_json::to_string(blueprint)?
            ],
        )?;
    }
    for trade in &batch.trades {
        transaction.execute(
            "INSERT INTO trades(realm, trade_id, payload_json) VALUES (?1, ?2, ?3) ON CONFLICT(realm, trade_id) DO UPDATE SET payload_json = excluded.payload_json",
            params![
                realm_key(&trade.value.key.realm),
                trade.value.key.id.as_str(),
                serde_json::to_string(trade)?
            ],
        )?;
    }
    for simulation in &batch.simulations {
        transaction.execute(
            "INSERT INTO simulations(simulation_id, payload_json) VALUES (?1, ?2) ON CONFLICT(simulation_id) DO UPDATE SET payload_json = excluded.payload_json",
            params![
                simulation.value.id.get(),
                serde_json::to_string(simulation)?
            ],
        )?;
    }
    for deletion in &batch.deletions {
        let realm = realm_key(&deletion.realm);
        match deletion.kind {
            "device" => {
                transaction.execute(
                    "DELETE FROM devices WHERE realm = ?1 AND device_id = ?2",
                    params![&realm, &deletion.item_id],
                )?;
            }
            "resource_site" => {
                transaction.execute(
                    "DELETE FROM resource_sites WHERE realm = ?1 AND site_id = ?2",
                    params![&realm, &deletion.item_id],
                )?;
            }
            "location_event" => {
                transaction.execute(
                    "DELETE FROM location_events WHERE realm = ?1 AND event_id = ?2",
                    params![&realm, &deletion.item_id],
                )?;
            }
            "incoming_object" => {
                transaction.execute(
                    "DELETE FROM discovery_data WHERE realm = ?1 AND kind = 'incoming_object' AND item_id = ?2",
                    params![&realm, &deletion.item_id],
                )?;
            }
            "trade" => {
                transaction.execute(
                    "DELETE FROM trades WHERE realm = ?1 AND trade_id = ?2",
                    params![&realm, &deletion.item_id],
                )?;
            }
            kind => return Err(StoreError::UnsupportedProjectionKind(kind)),
        }
        transaction.execute(
            "INSERT OR REPLACE INTO tombstones(realm, kind, item_id, removed_at, evidence) VALUES (?1, ?2, ?3, datetime('now'), ?4)",
            params![&realm, deletion.kind, &deletion.item_id, deletion.evidence],
        )?;
        transaction.execute(
            "DELETE FROM reconciliation_queue WHERE realm = ?1 AND work_id = ?2",
            params![&realm, format!("{}:{}", deletion.kind, deletion.item_id)],
        )?;
    }
    for target in &batch.reconciliation {
        transaction.execute(
            "INSERT INTO reconciliation_queue(work_id, realm, kind, payload_json, not_before, attempts, state) VALUES (?1, ?2, ?3, ?4, NULL, 0, 'queued') ON CONFLICT(work_id) DO UPDATE SET realm = excluded.realm, kind = excluded.kind, payload_json = excluded.payload_json, not_before = NULL, attempts = 0, state = 'queued'",
            params![
                &target.work_id,
                realm_key(&target.realm),
                target.kind,
                serde_json::to_string(&target.payload)?
            ],
        )?;
    }
    Ok(())
}

fn persist_device(
    transaction: &Transaction<'_>,
    observation: &Observation<Device>,
) -> Result<(), StoreError> {
    let source_document_id = persist_source_document(transaction, &observation.metadata)?;
    let device = &observation.value;
    let (location_realm, location_id) = device
        .location
        .as_ref()
        .map(|key| (Some(realm_key(&key.realm)), Some(key.id.as_str())))
        .unwrap_or((None, None));
    transaction.execute(
        "INSERT INTO devices(realm, device_id, device_type, status, location_realm, location_id, access_scope, observed_at, observation_json, source_document_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) ON CONFLICT(realm, device_id) DO UPDATE SET device_type = excluded.device_type, status = excluded.status, location_realm = excluded.location_realm, location_id = excluded.location_id, access_scope = excluded.access_scope, observed_at = excluded.observed_at, observation_json = excluded.observation_json, source_document_id = excluded.source_document_id",
        params![
            realm_key(&device.key.realm),
            device.key.id.as_str(),
            device.device_type.as_ref().map(|value| value.as_str()),
            device.status.as_ref().map(|value| value.as_str()),
            location_realm,
            location_id,
            access_key(&observation.metadata.access),
            observation.metadata.observed_at.unix_millis(),
            serde_json::to_string(observation)?,
            source_document_id,
        ],
    )?;
    transaction.execute(
        "DELETE FROM device_relationships WHERE realm = ?1 AND device_id = ?2",
        params![realm_key(&device.key.realm), device.key.id.as_str()],
    )?;
    if let Some(target) = &device.relationships.attached_to {
        persist_relationship(
            transaction,
            device,
            "attached_to",
            &target.realm,
            target.id.as_str(),
        )?;
    }
    if let Some(target) = &device.relationships.controller {
        persist_relationship(
            transaction,
            device,
            "controller",
            &target.realm,
            target.id.as_str(),
        )?;
    }
    if let Some(target) = &device.relationships.linked_device {
        persist_relationship(
            transaction,
            device,
            "linked_device",
            &target.realm,
            target.id.as_str(),
        )?;
    }
    if let Some(target) = &device.relationships.assigned_replicant {
        persist_relationship(
            transaction,
            device,
            "assigned_replicant",
            &target.realm,
            target.id.as_str(),
        )?;
    }
    if let Some(target) = &device.relationships.hosting_replicant {
        persist_relationship(
            transaction,
            device,
            "hosting_replicant",
            &target.realm,
            target.id.as_str(),
        )?;
    }
    Ok(())
}

fn persist_relationship(
    transaction: &Transaction<'_>,
    device: &Device,
    relationship: &str,
    target_realm: &Realm,
    target_id: &str,
) -> Result<(), StoreError> {
    transaction.execute(
        "INSERT INTO device_relationships(realm, device_id, relationship, target_realm, target_id) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![realm_key(&device.key.realm), device.key.id.as_str(), relationship, realm_key(target_realm), target_id],
    )?;
    Ok(())
}

fn persist_source_document(
    transaction: &Transaction<'_>,
    metadata: &ObservationMetadata,
) -> Result<Option<String>, StoreError> {
    let source = &metadata.source_document;
    let id = source.document_id.clone().or_else(|| {
        source
            .request_id
            .as_ref()
            .map(|request_id| format!("{}:{request_id}", source.operation))
    });
    if let Some(id) = &id {
        transaction.execute(
            "INSERT OR IGNORE INTO source_documents(id, operation, request_id, captured_at) VALUES (?1, ?2, ?3, ?4)",
            params![id, source.operation, source.request_id, metadata.observed_at.unix_millis()],
        )?;
    }
    Ok(id)
}

fn realm_key(realm: &Realm) -> String {
    match realm {
        Realm::Live => "live".to_owned(),
        Realm::Simulation(id) => format!("simulation:{}", id.get()),
    }
}

fn realm_from_key(value: &str) -> Realm {
    value
        .strip_prefix("simulation:")
        .and_then(|id| id.parse::<i64>().ok())
        .map(crate::domain::SimulationId::new)
        .map(Realm::Simulation)
        .unwrap_or(Realm::Live)
}

fn inventory_owner_key(owner: &InventoryOwner) -> (String, &'static str, String) {
    match owner {
        InventoryOwner::Account(id) => ("live".to_owned(), "account", id.as_str().to_owned()),
        InventoryOwner::Replicant(key) => (
            realm_key(&key.realm),
            "replicant",
            key.id.as_str().to_owned(),
        ),
        InventoryOwner::Location(key) => (
            realm_key(&key.realm),
            "location",
            key.id.as_str().to_owned(),
        ),
    }
}

fn access_key(access: &crate::domain::AccessScope) -> &'static str {
    match access {
        crate::domain::AccessScope::Owned => "owned",
        crate::domain::AccessScope::SiblingShared => "sibling_shared",
        crate::domain::AccessScope::Granted => "granted",
        crate::domain::AccessScope::Public => "public",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use std::time::{SystemTime, UNIX_EPOCH};

    use serde_json::json;

    use super::*;
    use crate::domain::{
        AccessScope, DeviceRelationships, DeviceStatus, DeviceType, EventCategory, EventId,
        EventName, ObservationAuthority, ObservationSource, Reachability, SourceDocument,
    };

    fn test_path(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("replicant-client-{name}-{nonce}.sqlite"))
    }

    fn reconciliation_count(store: &Store, work_id: &str) -> i64 {
        store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM reconciliation_queue WHERE work_id = ?1",
                [work_id],
                |row| row.get(0),
            )
            .expect("count reconciliation work")
    }

    fn device(realm: Realm, id: &str) -> Observation<Device> {
        Observation {
            value: Device {
                key: DeviceKey::in_realm(realm, id.into()),
                device_type: Some(DeviceType::from("miner")),
                status: Some(DeviceStatus::from("idle")),
                location: None,
                deployed_at: None,
                in_control_range: None,
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives: Vec::new(),
                tags: Vec::new(),
                relationships: DeviceRelationships::default(),
                cargo: Default::default(),
                cargo_capacity: None,
                attach_capacity: None,
                stow_capacity: None,
                stow_used: None,
                operational_capacity: None,
                grace_period_remaining: None,
                upkeep_requirements: Vec::new(),
                system_status: None,
                active_directive: None,
                travel: None,
                access: AccessScope::Owned,
            },
            metadata: ObservationMetadata {
                source: ObservationSource::RestDetail,
                authority: ObservationAuthority::EntitySnapshot,
                observed_at: "2026-07-25T00:00:00Z".into(),
                access: AccessScope::Owned,
                reachability: Reachability::Reachable,
                stale: false,
                source_document: SourceDocument {
                    operation: "GET /v1/devices/{device_code}".into(),
                    request_id: Some("request-1".into()),
                    document_id: None,
                },
            },
        }
    }

    fn event() -> Event {
        Event {
            id: EventId::new("1-0"),
            realm: Some(Realm::Live),
            name: EventName::from("device.updated"),
            category: EventCategory::from("device"),
            device: None,
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-25T00:00:00Z".into(),
            payload: BTreeMap::new(),
        }
    }

    fn seed_realistic_event_history(store: &Store) {
        store
            .history
            .execute_batch(
                "WITH RECURSIVE sequence(value) AS (
                    VALUES(0)
                    UNION ALL
                    SELECT value + 1 FROM sequence WHERE value < 24999
                )
                INSERT INTO event_history(
                    event_id, realm, event_name, category, device_code, replicant_code,
                    star_id, location_id, occurred_at, payload_json, appended_at, applied_at,
                    archived_only, stream_millis, stream_sequence
                )
                SELECT
                    printf('%d-0', 1000000 + value),
                    'live',
                    CASE value % 2 WHEN 0 THEN 'device.updated' ELSE 'travel.arrived' END,
                    'activity',
                    printf('D%d', value % 20),
                    NULL,
                    NULL,
                    NULL,
                    '2026-08-30T00:00:00Z',
                    '{\"detail\":\"representative event payload\"}',
                    datetime('now'),
                    datetime('now'),
                    0,
                    1000000 + value,
                    0
                FROM sequence;",
            )
            .expect("seed realistic event history");
    }

    fn star(id: &str) -> Observation<Star> {
        Observation {
            value: Star {
                key: StarKey::live(id.into()),
                name: None,
                spectral_type: Some("G".to_owned()),
                entry_point: None,
                position: None,
                has_hub: None,
                has_ward: None,
                knowledge_observed: false,
                explored: None,
                has_life: None,
                region: None,
            },
            metadata: device(Realm::Live, "metadata").metadata,
        }
    }

    fn legacy_star_knowledge(replicant: &str, id: &str) -> Observation<StarKnowledge> {
        Observation {
            value: StarKnowledge {
                replicant: ReplicantKey::live(replicant.into()),
                star: StarKey::live(id.into()),
                position: None,
                spectral_type: None,
                entry_point: None,
                explored: Some(true),
                has_hub: None,
                has_ward: None,
                has_life: Some(true),
                region: Some("alpha".to_owned()),
                distance_from_replicant: Some(42.0),
                estimated_travel_time: Some(123),
            },
            metadata: device(Realm::Live, "metadata").metadata,
        }
    }

    #[test]
    fn fresh_migration_is_idempotent_and_configures_file_database() {
        let path = test_path("migration");
        {
            let store = Store::open_file(&path).expect("open fresh store");
            assert_eq!(store.foreign_keys_enabled().expect("foreign key pragma"), 1);
            assert_eq!(store.journal_mode().expect("journal mode"), "wal");
            assert_eq!(store.busy_timeout().expect("busy timeout"), 15_000);
        }
        let store = Store::open_file(&path).expect("reopen migrated store");
        assert_eq!(store.device_count().expect("device count"), 0);
        drop(store);
        fs::remove_file(path).expect("remove test database");
    }

    #[test]
    fn file_store_creates_its_parent_directory() {
        let directory = test_path("directory");
        let path = directory.join("replicant-client.sqlite");
        let store = Store::open_file(&path).expect("open store in new directory");
        drop(store);

        assert!(path.is_file());
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn interrupted_migration_rolls_back_and_retry_succeeds() {
        let path = test_path("interrupted-migration");
        let connection = Connection::open(&path).expect("open pre-migration database");
        connection
            .execute_batch(
                "CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY NOT NULL);\
                 INSERT INTO schema_migrations(version) VALUES (0);\
                 CREATE TABLE preserved_before_migration (value INTEGER NOT NULL);\
                 INSERT INTO preserved_before_migration(value) VALUES (7);",
            )
            .expect("seed prior schema and data");
        drop(connection);

        Store::interrupt_next_migration_for_test();
        assert!(matches!(
            Store::open_file(&path),
            Err(StoreError::InjectedMigrationInterruption)
        ));

        let connection = Connection::open(&path).expect("reopen rolled-back database");
        assert_eq!(
            connection
                .query_row("SELECT value FROM preserved_before_migration", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("prior data remains readable"),
            7
        );
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'devices'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("inspect rolled-back schema"),
            0
        );
        drop(connection);

        let store = Store::open_file(&path).expect("migration retry succeeds");
        assert_eq!(store.device_count().expect("new schema is usable"), 0);
        drop(store);
        fs::remove_file(path).expect("remove test database");
    }

    #[test]
    fn future_schema_version_is_rejected() {
        let path = test_path("future-schema");
        let connection = Connection::open(&path).expect("open database");
        connection
            .execute_batch("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY NOT NULL); INSERT INTO schema_migrations VALUES (9);")
            .expect("seed future schema");
        drop(connection);
        assert!(matches!(
            Store::open_file(&path),
            Err(StoreError::UnsupportedSchemaVersion {
                found: 9,
                supported: 8
            })
        ));
        fs::remove_file(path).expect("remove test database");
    }

    #[test]
    fn version_one_device_relationships_migrate_and_restore_as_assignments() {
        let path = test_path("device-relationship-v1");
        let mut legacy = device(Realm::Live, "D1");
        legacy.value.relationships.assigned_replicant = Some(ReplicantKey::live("OWNER".into()));
        let mut observation = serde_json::to_value(&legacy).expect("serialize v1 fixture");
        let relationships = observation
            .pointer_mut("/value/relationships")
            .and_then(Value::as_object_mut)
            .expect("fixture relationships");
        let assigned = relationships
            .remove("assigned_replicant")
            .expect("fixture assignment");
        relationships.insert("hosted_by".into(), assigned);

        let connection = Connection::open(&path).expect("create v1 database");
        Store::configure(&connection, true).expect("configure v1 database");
        connection
            .execute_batch("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY NOT NULL);")
            .expect("create v1 migration ledger");
        connection
            .execute_batch(INITIAL_SCHEMA)
            .expect("create v1 schema");
        connection
            .execute("INSERT INTO schema_migrations(version) VALUES (1)", [])
            .expect("record v1 schema");
        connection
            .execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', '1')",
                [],
            )
            .expect("record v1 metadata");
        connection
            .execute(
                "INSERT INTO devices(realm, device_id, access_scope, observed_at, observation_json) VALUES ('live', 'D1', 'owned', 0, ?1)",
                [serde_json::to_string(&observation).expect("encode v1 observation")],
            )
            .expect("insert v1 device");
        connection
            .execute(
                "INSERT INTO device_relationships(realm, device_id, relationship, target_realm, target_id) VALUES ('live', 'D1', 'hosted_by', 'live', 'OWNER')",
                [],
            )
            .expect("insert v1 relationship");
        drop(connection);

        let store = Store::open_file(&path).expect("migrate v1 database");
        let restored = store.restore_devices().expect("restore migrated device");
        let restored = restored
            .get(&DeviceKey::live("D1".into()))
            .expect("restored device");
        assert_eq!(
            restored
                .value
                .relationships
                .assigned_replicant
                .as_ref()
                .map(|key| key.id.as_str()),
            Some("OWNER")
        );
        assert!(restored.value.relationships.hosting_replicant.is_none());
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT relationship FROM device_relationships WHERE realm = 'live' AND device_id = 'D1'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .expect("migrated relationship row"),
            "assigned_replicant"
        );
        drop(store);
        std::fs::remove_file(path).expect("remove migrated database");
    }

    #[test]
    fn version_three_migrates_events_to_history_and_normalizes_star_knowledge() {
        let path = test_path("history-split-v3");
        let history_path = history_database_path(&path);
        let connection = Connection::open(&path).expect("create v3 database");
        Store::configure(&connection, true).expect("configure v3 database");
        connection
            .execute_batch("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY NOT NULL);")
            .expect("create migration ledger");
        connection
            .execute_batch(INITIAL_SCHEMA)
            .expect("create v1 schema");
        connection
            .execute_batch(DEVICE_RELATIONSHIP_SEMANTICS_SCHEMA)
            .expect("apply v2 schema");
        connection
            .execute_batch(RECONCILIATION_LEADER_SCHEMA)
            .expect("apply v3 schema");
        connection
            .execute("INSERT INTO schema_migrations(version) VALUES (3)", [])
            .expect("record v3 schema");
        connection
            .execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', '3')",
                [],
            )
            .expect("record v3 metadata");
        let star = star("SOL");
        connection
            .execute(
                "INSERT INTO stars(realm, star_id, payload_json) VALUES ('live', 'SOL', ?1)",
                [serde_json::to_string(&star).expect("encode star")],
            )
            .expect("insert star");
        for replicant in ["R1", "R2"] {
            let knowledge = legacy_star_knowledge(replicant, "SOL");
            connection
                .execute(
                    "INSERT INTO replicant_star_knowledge(realm, replicant_id, star_id, observation_json) VALUES ('live', ?1, 'SOL', ?2)",
                    params![
                        replicant,
                        serde_json::to_string(&knowledge).expect("encode knowledge")
                    ],
                )
                .expect("insert legacy knowledge");
        }
        let event = event();
        connection
            .execute(
                "INSERT INTO event_journal(event_id, realm, event_json, appended_at) VALUES (?1, 'live', ?2, datetime('now'))",
                params![
                    event.id.as_str(),
                    serde_json::to_string(&event).expect("encode event")
                ],
            )
            .expect("insert legacy event");
        connection
            .execute(
                "INSERT INTO event_cursors(stream, cursor, updated_at) VALUES ('account', '1-0', datetime('now'))",
                [],
            )
            .expect("seed applied cursor");
        drop(connection);

        let store = Store::open_file(&path).expect("migrate v3 store");
        assert_eq!(
            store
                .read_events(None, None, None, None)
                .expect("history events"),
            vec![event]
        );
        let catalogue = store.restore_catalogue().expect("restore catalogue").0;
        let sol = catalogue
            .get(&StarKey::live("SOL".into()))
            .expect("SOL star");
        assert!(sol.value.knowledge_observed);
        assert_eq!(sol.value.explored, Some(true));
        assert_eq!(sol.value.has_life, Some(true));
        assert_eq!(sol.value.region.as_deref(), Some("alpha"));
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name IN ('event_journal', 'replicant_star_knowledge')",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("legacy table count"),
            0
        );
        assert_eq!(
            store
                .history
                .query_row("SELECT COUNT(*) FROM event_history", [], |row| row
                    .get::<_, i64>(0))
                .expect("history row count"),
            1
        );
        drop(store);
        fs::remove_file(&path).expect("remove primary database");
        fs::remove_file(&history_path).expect("remove history database");
    }

    #[test]
    fn in_memory_store_supports_account_binding_without_secrets() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .bind_account(&AccountId::new("account-a"))
            .expect("bind account");
        store
            .bind_account(&AccountId::new("account-a"))
            .expect("repeat binding");
        let mismatch = store
            .bind_account(&AccountId::new("account-b"))
            .expect_err("mismatch must fail");
        assert!(matches!(mismatch, StoreError::AccountMismatch { .. }));
        // Store APIs accept account identity and normalized observations only;
        // authentication tokens are never an input to any persisted record.
        assert_eq!(store.device_count().expect("device count"), 0);
    }

    #[test]
    fn authentication_tokens_are_not_persisted() {
        let path = test_path("secret");
        let authentication_token = "super-secret-authentication-token";
        let mut store = Store::open_file(&path).expect("open file store");
        store
            .bind_account(&AccountId::new("account-a"))
            .expect("bind account identity only");
        drop(store);
        let bytes = fs::read(&path).expect("read database");
        assert!(
            !bytes
                .windows(authentication_token.len())
                .any(|window| window == authentication_token.as_bytes())
        );
        fs::remove_file(path).expect("remove test database");
    }

    #[test]
    fn realm_composite_keys_keep_same_device_id_isolated() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .persist_devices(&[
                device(Realm::Live, "same-code"),
                device(
                    Realm::Simulation(crate::domain::SimulationId::new(7)),
                    "same-code",
                ),
            ])
            .expect("persist realm-qualified devices");
        let restored = store.restore_devices().expect("restore devices");
        assert_eq!(restored.len(), 2);
        assert!(restored.contains_key(&DeviceKey::live("same-code".into())));
    }

    #[test]
    fn device_operational_state_survives_store_round_trip() {
        let mut store = Store::open_memory().expect("open memory store");
        let mut observation = device(Realm::Live, "DRONE");
        observation.value.relationships.stowed_in = Some(DeviceKey::live("VESSEL".into()));
        observation.value.relationships.controller = Some(DeviceKey::live("CTRL".into()));
        observation.value.relationships.stowed_devices = vec![DeviceKey::live("CHILD".into())];
        observation.value.stow_capacity = Some(5);
        observation.value.stow_used = Some(2);
        observation.value.active_directive = Some(crate::domain::ActiveDeviceDirective {
            directive: Some(crate::domain::DeviceDirective::from("survey_system")),
            status: Some("active".into()),
            details: BTreeMap::from([(
                "directive".into(),
                serde_json::Value::String("survey_system".into()),
            )]),
        });
        observation.value.travel = Some(crate::domain::TravelState {
            destination: Some(crate::domain::LocationKey::live("SOL-4-L4".into())),
            eta_seconds: Some(42),
            stage: Some("recalling".into()),
            ..crate::domain::TravelState::default()
        });
        let expected = observation.value.clone();

        store
            .persist_devices(&[observation])
            .expect("persist operational device");
        let restored = store.restore_devices().expect("restore devices");
        assert_eq!(
            restored
                .get(&DeviceKey::live("DRONE".into()))
                .expect("restored operational device")
                .value,
            expected
        );
    }

    #[test]
    fn linked_device_relationship_survives_store_round_trip_without_schema_change() {
        let mut store = Store::open_memory().expect("open memory store");
        let mut observation = device(Realm::Live, "SLING1");
        observation.value.device_type = Some(DeviceType::FtlSlingshot);
        observation.value.relationships.linked_device = Some(DeviceKey::live("MATRIX1".into()));

        store
            .persist_devices(&[observation])
            .expect("persist linked slingshot");
        let restored = store.restore_devices().expect("restore devices");
        assert_eq!(
            restored
                .get(&DeviceKey::live("SLING1".into()))
                .expect("restored slingshot")
                .value
                .relationships
                .linked_device
                .as_ref()
                .map(|key| key.id.as_str()),
            Some("MATRIX1")
        );
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT target_id FROM device_relationships WHERE realm = 'live' AND device_id = 'SLING1' AND relationship = 'linked_device'",
                    [],
                    |row| row.get::<_, String>(0),
                )
                .expect("linked relationship row"),
            "MATRIX1"
        );
    }

    #[test]
    fn event_cursor_and_projection_are_atomic() {
        let mut store = Store::open_memory().expect("open memory store");
        store.fail_next_commit();
        assert!(matches!(
            store.apply_event_projection(
                &event(),
                "cursor-1",
                &EventProjectionBatch {
                    devices: vec![device(Realm::Live, "d1")],
                    ..EventProjectionBatch::default()
                },
            ),
            Err(StoreError::InjectedCommitFailure)
        ));
        assert_eq!(store.event_count().expect("event count"), 0);
        assert_eq!(store.event_cursor().expect("cursor"), None);
        assert_eq!(store.device_count().expect("device count"), 0);
        assert!(
            store
                .read_events(None, None, None, None)
                .expect("event journal")
                .is_empty()
        );
    }

    #[test]
    fn operation_projection_and_device_commit_are_atomic() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .record_operation(
                "operation-1",
                "prepared",
                Some("live"),
                Some("device"),
                Some("d1"),
                &json!({"kind": "activate"}),
            )
            .expect("record intent");
        store.fail_next_commit();
        assert!(matches!(
            store.record_operation_and_project(
                "operation-1",
                "completed",
                &[device(Realm::Live, "d1")]
            ),
            Err(StoreError::InjectedCommitFailure)
        ));
        assert_eq!(store.device_count().expect("device count"), 0);
        let entry = store
            .read_operation("operation-1")
            .expect("operation journal")
            .expect("operation still recorded");
        assert_eq!(entry.state, "prepared");
    }

    #[test]
    fn journal_primitives_round_trip_event_and_operation_records() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .apply_event_projection(&event(), "cursor-1", &EventProjectionBatch::default())
            .expect("append event");
        assert_eq!(
            store
                .read_events(None, None, None, None)
                .expect("read events"),
            vec![event()]
        );
        store
            .record_operation(
                "operation-1",
                "prepared",
                Some("live"),
                Some("device"),
                Some("d1"),
                &json!({"kind": "activate"}),
            )
            .expect("register operation");
        store
            .append_operation_projection("operation-1", "completed", &json!({"device": "d1"}))
            .expect("append projection");
        let entry = store
            .read_operation("operation-1")
            .expect("read operation")
            .expect("operation exists");
        assert_eq!(entry.state, "completed");
        assert_eq!(entry.target_kind.as_deref(), Some("device"));
        assert_eq!(entry.intent, json!({"kind": "activate"}));
        assert_eq!(entry.projection, Some(json!({"device": "d1"})));
    }

    #[test]
    fn cursor_bounded_event_history_uses_indexed_search() {
        let store = Store::open_memory().expect("open memory store");
        seed_realistic_event_history(&store);

        let plan = {
            let mut statement = store
                .history
                .prepare(
                    "EXPLAIN QUERY PLAN SELECT event_id, realm, event_name, category, device_code, replicant_code, star_id, location_id, occurred_at, payload_json
                     FROM event_history
                     WHERE (applied_at IS NOT NULL OR archived_only = 1)
                       AND (stream_millis, stream_sequence, event_id) > (?1, ?2, ?3)
                     ORDER BY stream_millis, stream_sequence, event_id",
                )
                .expect("prepare query plan");
            statement
                .query_map(params![1_024_990_i64, 0_i64, "1024990-0"], |row| {
                    row.get::<_, String>(3)
                })
                .expect("query plan")
                .collect::<Result<Vec<_>, _>>()
                .expect("collect query plan")
        };
        assert!(
            plan.iter().any(|detail| {
                detail.contains(
                    "SEARCH event_history USING INDEX event_history_retained_stream_order",
                )
            }),
            "expected cursor search through retained stream index, got {plan:?}"
        );

        let legacy_started = Instant::now();
        let legacy_rows = {
            let mut statement = store
                .history
                .prepare(
                    "SELECT event_id, realm, event_name, category, device_code, replicant_code, star_id, location_id, occurred_at, payload_json
                     FROM event_history
                     WHERE applied_at IS NOT NULL OR archived_only = 1
                     ORDER BY stream_millis, stream_sequence",
                )
                .expect("prepare legacy history read");
            statement
                .query_map([], history_event_row)
                .expect("query legacy history")
                .map(|row| decode_history_event(row.expect("extract legacy row")))
                .collect::<Result<Vec<_>, _>>()
                .expect("decode legacy history")
        };
        let legacy_elapsed = legacy_started.elapsed();

        let bounded_started = Instant::now();
        let bounded_rows = store
            .read_events(Some("1024990-0"), None, None, None)
            .expect("read cursor-bounded history");
        let bounded_elapsed = bounded_started.elapsed();

        assert_eq!(legacy_rows.len(), 25_000);
        assert_eq!(bounded_rows.len(), 9);
        assert_eq!(
            bounded_rows.first().map(|event| event.id.as_str()),
            Some("1024991-0")
        );
        assert_eq!(
            bounded_rows.last().map(|event| event.id.as_str()),
            Some("1024999-0")
        );
        assert!(
            bounded_elapsed < legacy_elapsed,
            "bounded read {bounded_elapsed:?} should be faster than full decode {legacy_elapsed:?}"
        );
        eprintln!(
            "event history benchmark: before_full_decode_ms={} after_cursor_read_ms={} before_rows={} after_rows={}",
            legacy_elapsed.as_millis(),
            bounded_elapsed.as_millis(),
            legacy_rows.len(),
            bounded_rows.len()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cursor_bounded_history_does_not_starve_following_store_commands() {
        let store = StoreHandle::open_memory().await.expect("open worker store");
        store
            .execute(|store| {
                seed_realistic_event_history(store);
                Ok(())
            })
            .await
            .expect("seed worker history");

        let (history, write, read) = tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(
                store.execute(|store| { store.read_events(Some("1024990-0"), None, None, None) }),
                store.execute(|store| store.set_event_cursor("1024999-0")),
                store.execute(|store| store.event_cursor()),
            )
        })
        .await
        .expect("bounded history and queued follower commands must complete promptly");

        assert_eq!(history.expect("history read").len(), 9);
        write.expect("small cursor write");
        assert_eq!(
            read.expect("small cursor read").as_deref(),
            Some("1024999-0")
        );
        store.close().await.expect("close worker store");
    }

    #[test]
    fn promote_crashed_submissions_marks_only_submitted_rows_ambiguous() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .record_operation("op-submitted", "submitted", None, None, None, &json!({}))
            .expect("record submitted");
        store
            .record_operation("op-prepared", "prepared", None, None, None, &json!({}))
            .expect("record prepared");
        let promoted = store
            .promote_crashed_submissions()
            .expect("promote crashed submissions");
        assert_eq!(promoted, 1);
        assert_eq!(
            store
                .read_operation("op-submitted")
                .expect("read")
                .expect("exists")
                .state,
            "ambiguous"
        );
        assert_eq!(
            store
                .read_operation("op-prepared")
                .expect("read")
                .expect("exists")
                .state,
            "prepared"
        );
    }

    #[test]
    fn list_unresolved_operations_excludes_terminal_states() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .record_operation("op-open", "awaiting_evidence", None, None, None, &json!({}))
            .expect("record open");
        store
            .record_operation("op-done", "completed", None, None, None, &json!({}))
            .expect("record done");
        let unresolved = store.list_unresolved_operations().expect("list unresolved");
        assert_eq!(unresolved.len(), 1);
        assert_eq!(unresolved[0].0, "op-open");
    }

    #[test]
    fn find_operations_awaiting_evidence_matches_target() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .record_operation(
                "op-1",
                "awaiting_evidence",
                Some("live"),
                Some("device"),
                Some("d1"),
                &json!({}),
            )
            .expect("record");
        let found = store
            .find_operations_awaiting_evidence("live", "device", "d1")
            .expect("find");
        assert_eq!(found, vec!["op-1".to_string()]);
        assert!(
            store
                .find_operations_awaiting_evidence("live", "device", "d2")
                .expect("find other")
                .is_empty()
        );
    }

    #[test]
    fn has_event_detects_journaled_ids_regardless_of_source() {
        let mut store = Store::open_memory().expect("open memory store");
        assert!(!store.has_event("1-0").expect("has_event before append"));
        store
            .apply_event_projection(&event(), "1-0", &EventProjectionBatch::default())
            .expect("append event");
        assert!(store.has_event("1-0").expect("has_event after append"));
        assert!(!store.has_event("2-0").expect("has_event for unseen id"));
    }

    #[test]
    fn apply_event_projection_removes_device_and_tombstones_atomically() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .persist_devices(&[device(Realm::Live, "d1")])
            .expect("seed device");
        let key = DeviceKey::live(crate::domain::DeviceId::new("d1"));
        store
            .enqueue_reconciliation("device:d1", &Realm::Live, "device", &json!({"id": "d1"}))
            .expect("queue stale reconciliation");

        let mut decommission_event = event();
        decommission_event.name = EventName::from("device.decommissioned");
        decommission_event.device = Some(key.clone());
        store
            .apply_event_projection(
                &decommission_event,
                "cursor-decom",
                &EventProjectionBatch {
                    deletions: vec![ProjectionTombstone {
                        realm: Realm::Live,
                        kind: "device",
                        item_id: key.id.as_str().to_owned(),
                        evidence: "device.decommissioned",
                    }],
                    ..EventProjectionBatch::default()
                },
            )
            .expect("decommission");

        assert_eq!(store.device_count().expect("device count"), 0);
        assert_eq!(
            store.event_cursor().expect("cursor").as_deref(),
            Some("cursor-decom")
        );
        let restored = store.restore_devices().expect("restore devices");
        assert!(!restored.contains_key(&key));
        assert_eq!(reconciliation_count(&store, "device:d1"), 0);
    }

    #[test]
    fn decommission_failure_leaves_device_event_and_cursor_untouched() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .persist_devices(&[device(Realm::Live, "d1")])
            .expect("seed device");
        let key = DeviceKey::live(crate::domain::DeviceId::new("d1"));
        store
            .enqueue_reconciliation("device:d1", &Realm::Live, "device", &json!({"id": "d1"}))
            .expect("queue reconciliation");
        store.fail_next_commit();
        assert!(matches!(
            store.apply_event_projection(
                &event(),
                "cursor-decom",
                &EventProjectionBatch {
                    deletions: vec![ProjectionTombstone {
                        realm: Realm::Live,
                        kind: "device",
                        item_id: key.id.as_str().to_owned(),
                        evidence: "device.decommissioned",
                    }],
                    ..EventProjectionBatch::default()
                },
            ),
            Err(StoreError::InjectedCommitFailure)
        ));
        assert_eq!(store.device_count().expect("device count"), 1);
        assert_eq!(store.event_cursor().expect("cursor"), None);
        assert!(
            store
                .read_events(None, None, None, None)
                .expect("event journal")
                .is_empty()
        );
        assert_eq!(reconciliation_count(&store, "device:d1"), 1);
    }

    #[test]
    fn full_device_reconciliation_cancels_removed_device_work() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .persist_devices(&[
                device(Realm::Live, "d1"),
                device(Realm::Live, "d2"),
                device(Realm::Live, "d3"),
                device(Realm::Live, "d4"),
                device(Realm::Live, "d5"),
            ])
            .expect("seed devices");
        store
            .enqueue_reconciliation("device:d1", &Realm::Live, "device", &json!({"id": "d1"}))
            .expect("queue reconciliation");
        let present = ["d2", "d3", "d4", "d5"]
            .into_iter()
            .map(|id| DeviceKey::live(DeviceId::new(id)))
            .collect();

        store
            .reconcile_owned_devices(&present)
            .expect("reconcile under-threshold missing device");

        assert_eq!(reconciliation_count(&store, "device:d1"), 0);
    }

    #[test]
    fn reconciliation_claim_prunes_device_work_already_tombstoned() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .connection
            .execute(
                "INSERT INTO tombstones(realm, kind, item_id, removed_at, evidence) VALUES ('live', 'device', 'd1', datetime('now'), 'explicit-decommission-event')",
                [],
            )
            .expect("seed tombstone");
        store
            .enqueue_reconciliation("device:d1", &Realm::Live, "device", &json!({"id": "d1"}))
            .expect("queue stale reconciliation");
        assert_eq!(reconciliation_count(&store, "device:d1"), 1);

        assert!(
            store
                .claim_reconciliation_work()
                .expect("claim after tombstone")
                .is_none()
        );
        assert_eq!(reconciliation_count(&store, "device:d1"), 0);
    }

    #[test]
    fn event_cursor_is_stale_after_the_configured_threshold() {
        let mut store = Store::open_memory().expect("open memory store");
        // No cursor at all is conservatively treated as stale.
        assert!(store.event_cursor_is_stale(3600).expect("stale check"));
        store.set_event_cursor("1-0").expect("set cursor");
        assert!(!store.event_cursor_is_stale(3600).expect("fresh cursor"));
        store.backdate_event_cursor(7200).expect("backdate");
        assert!(store.event_cursor_is_stale(3600).expect("stale cursor"));
    }

    #[test]
    fn crash_before_commit_leaves_no_trace_and_replay_remains_safe() {
        let path = test_path("crash-resume");
        {
            let mut store = Store::open_file(&path).expect("open file store");
            // Simulate a process that decoded an event ("received" it) but
            // crashed before the atomic store-and-advance-cursor commit.
            store.fail_next_commit();
            assert!(matches!(
                store.apply_event_projection(
                    &event(),
                    "cursor-1",
                    &EventProjectionBatch::default()
                ),
                Err(StoreError::InjectedCommitFailure)
            ));
        }
        let restored = Store::open_file(&path).expect("reopen after crash");
        assert!(!restored.has_event("1-0").expect("event not journaled"));
        assert_eq!(restored.event_cursor().expect("cursor"), None);
        drop(restored);
        fs::remove_file(&path).expect("remove test database");
    }

    #[test]
    fn reconciliation_queue_coalesces_and_recovers_after_leader_expiry() {
        let path = test_path("queue");
        let mut store = Store::open_file(&path).expect("open file store");
        store
            .enqueue_reconciliation("device:d1", &Realm::Live, "device", &json!({"id": "d1"}))
            .expect("enqueue");
        store
            .enqueue_reconciliation(
                "device:d1",
                &Realm::Live,
                "device",
                &json!({"id": "d1-new"}),
            )
            .expect("coalesce");
        assert!(
            store
                .acquire_reconciliation_leadership("worker-a", 30)
                .expect("acquire leader")
        );
        let claimed = store
            .claim_reconciliation_work()
            .expect("claim")
            .expect("work exists");
        assert_eq!(claimed.payload, json!({"id": "d1-new"}));
        drop(store);

        let mut restored = Store::open_file(&path).expect("second process opens store");
        assert!(
            restored
                .claim_reconciliation_work()
                .expect("running work stays claimed")
                .is_none()
        );
        assert!(
            !restored
                .acquire_reconciliation_leadership("worker-b", 30)
                .expect("live leader blocks takeover")
        );
        restored
            .connection
            .execute("UPDATE reconciliation_leader SET lease_until = 0", [])
            .expect("expire leader");
        assert!(
            restored
                .acquire_reconciliation_leadership("worker-b", 30)
                .expect("take over expired leader")
        );
        let recovered = restored
            .claim_reconciliation_work()
            .expect("claim recovered")
            .expect("expired leader work requeued");
        restored
            .retry_reconciliation_work(&recovered.work_id)
            .expect("backoff");
        assert!(
            restored
                .claim_reconciliation_work()
                .expect("not due")
                .is_none()
        );
        fs::remove_file(path).expect("remove test database");
    }

    #[test]
    fn startup_recovers_running_reconciliation_work_without_a_live_owner() {
        let path = test_path("orphaned-reconciliation");
        {
            let mut store = Store::open_file(&path).expect("open file store");
            store
                .enqueue_reconciliation("device:d1", &Realm::Live, "device", &json!({"id": "d1"}))
                .expect("enqueue");
            store
                .connection
                .execute(
                    "UPDATE reconciliation_queue SET state = 'running' WHERE work_id = 'device:d1'",
                    [],
                )
                .expect("orphan work");
            store
                .connection
                .execute("DELETE FROM reconciliation_leader", [])
                .expect("remove owner");
        }

        let mut restored = Store::open_file(&path).expect("restart store");
        assert_eq!(
            restored
                .claim_reconciliation_work()
                .expect("claim recovered work")
                .expect("work is queued")
                .work_id,
            "device:d1"
        );
        drop(restored);
        fs::remove_file(path).expect("remove test database");
    }

    #[test]
    fn operation_retention_removes_only_expired_completed_and_rejected_rows() {
        let mut store = Store::open_memory().expect("open memory store");
        for (id, state) in [
            ("old-completed", "completed"),
            ("old-rejected", "rejected"),
            ("old-awaiting", "awaiting_evidence"),
            ("recent-completed", "completed"),
        ] {
            store
                .record_operation(id, state, None, None, None, &json!({}))
                .expect("record operation");
        }
        store
            .connection
            .execute(
                "UPDATE operation_journal SET updated_at = datetime('now', '-31 days') WHERE operation_id LIKE 'old-%'",
                [],
            )
            .expect("backdate operations");
        store.last_history_maintenance = Instant::now() - HISTORY_MAINTENANCE_INTERVAL;
        store.maintain_history().expect("run retention");

        let ids = store
            .connection
            .prepare("SELECT operation_id FROM operation_journal ORDER BY operation_id")
            .expect("prepare remaining operations")
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query remaining operations")
            .collect::<rusqlite::Result<Vec<_>>>()
            .expect("collect remaining operations");
        assert_eq!(ids, vec!["old-awaiting", "recent-completed"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_executes_store_requests_off_the_caller_runtime_thread() {
        let store = StoreHandle::open_memory().await.expect("open worker store");
        let caller = std::thread::current().id();
        let worker = store
            .execute(|_| Ok::<_, StoreError>(std::thread::current().id()))
            .await
            .expect("worker response");
        assert_ne!(caller, worker);
        store.close().await.expect("close worker store");
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn worker_propagates_request_spans_in_command_order() {
        let store = StoreHandle::open_memory().await.expect("open worker store");
        let subscriber = tracing_subscriber::fmt().with_test_writer().finish();
        let (order_sender, order_receiver) = mpsc::sync_channel(2);

        let (async_observed, async_expected, blocking_observed, blocking_expected) =
            tracing::subscriber::with_default(subscriber, || {
                let async_span = tracing::info_span!("store.async_request");
                let async_expected = async_span.id();
                let async_sender = order_sender.clone();
                let async_observed = tokio::task::block_in_place(|| {
                    async_span.in_scope(|| {
                        tokio::runtime::Handle::current().block_on(store.execute(move |_| {
                            async_sender.send("async").expect("record async command");
                            Ok::<_, StoreError>(Span::current().id())
                        }))
                    })
                })
                .expect("async worker response");

                let blocking_span = tracing::info_span!("store.blocking_request");
                let blocking_expected = blocking_span.id();
                let blocking_observed = blocking_span.in_scope(|| {
                    store
                        .execute_blocking(move |_| {
                            order_sender
                                .send("blocking")
                                .expect("record blocking command");
                            Ok::<_, StoreError>(Span::current().id())
                        })
                        .expect("blocking worker response")
                });
                (
                    async_observed,
                    async_expected,
                    blocking_observed,
                    blocking_expected,
                )
            });

        assert_eq!(async_observed, async_expected);
        assert_eq!(blocking_observed, blocking_expected);
        assert_eq!(
            [
                order_receiver.recv().expect("first command"),
                order_receiver.recv().expect("second command"),
            ],
            ["async", "blocking"]
        );
        store.close().await.expect("close worker store");
    }

    #[tokio::test]
    async fn worker_rejects_requests_once_close_begins() {
        let store = StoreHandle::open_memory().await.expect("open worker store");
        store.close().await.expect("close worker store");
        assert!(matches!(
            store.execute(|_| Ok::<_, StoreError>(())).await,
            Err(StoreError::Closed)
        ));
    }

    #[tokio::test]
    async fn close_waits_for_the_active_transaction_boundary() {
        let store = StoreHandle::open_memory().await.expect("open worker store");
        let (entered_sender, entered_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let active_store = store.clone();
        let active = tokio::spawn(async move {
            active_store
                .execute(move |_| {
                    entered_sender.send(()).expect("entered");
                    release_receiver.recv().expect("release");
                    Ok(())
                })
                .await
        });
        tokio::task::spawn_blocking(move || entered_receiver.recv())
            .await
            .expect("join entered wait")
            .expect("worker is active");
        let mut closing = tokio::spawn({
            let store = store.clone();
            async move { store.close().await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut closing)
                .await
                .is_err()
        );
        release_sender.send(()).expect("release worker");
        active
            .await
            .expect("active request joins")
            .expect("active request succeeds");
        closing.await.expect("close joins").expect("close succeeds");
    }

    #[test]
    fn bounded_worker_queue_reports_backpressure() {
        let store = StoreHandle::open_memory_blocking().expect("open worker store");
        let (entered_sender, entered_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let worker = store.clone();
        let active = std::thread::spawn(move || {
            worker.execute_blocking(move |_| {
                entered_sender.send(()).expect("entered");
                release_receiver.recv().expect("release");
                Ok(())
            })
        });
        entered_receiver.recv().expect("worker is occupied");
        for _ in 0..STORE_QUEUE_CAPACITY {
            store
                .sender
                .try_send(StoreCommand::Execute {
                    id: NEXT_STORE_COMMAND_ID.fetch_add(1, AtomicOrdering::Relaxed),
                    operation_type: "test.queue_fill",
                    queued_at: Instant::now(),
                    span: Span::none(),
                    dispatcher: tracing::dispatcher::get_default(Clone::clone),
                    command: Box::new(|_| {}),
                })
                .expect("fill bounded queue");
        }
        assert!(matches!(
            store.sender.try_send(StoreCommand::Execute {
                id: NEXT_STORE_COMMAND_ID.fetch_add(1, AtomicOrdering::Relaxed),
                operation_type: "test.queue_overflow",
                queued_at: Instant::now(),
                span: Span::none(),
                dispatcher: tracing::dispatcher::get_default(Clone::clone),
                command: Box::new(|_| {}),
            }),
            Err(tokio_mpsc::error::TrySendError::Full(_))
        ));
        release_sender.send(()).expect("release worker");
        active
            .join()
            .expect("active request joins")
            .expect("active request succeeds");
        std::thread::sleep(Duration::from_millis(10));
    }

    #[test]
    fn replay_projection_commit_failure_rolls_back_effect_and_checkpoint() {
        let mut store = Store::open_memory().expect("open replay store");
        let replay = store
            .prepare_projection_replay("event_owned", 1)
            .expect("prepare replay");
        let site = Observation {
            value: ResourceSite {
                key: crate::domain::ResourceSiteKey::live(crate::domain::ResourceSiteId::new(
                    "SITE-1",
                )),
                location: None,
                site_type: Some("salvage".to_owned()),
                name: None,
                resources: BTreeMap::new(),
                extra: BTreeMap::new(),
            },
            metadata: device(Realm::Live, "metadata").metadata,
        };
        let batch = EventProjectionBatch {
            resource_sites: vec![site],
            ..EventProjectionBatch::default()
        };
        store.fail_next_commit();
        assert!(matches!(
            store.apply_replay_projection("event_owned", 1, 10, replay.high_water_rowid, &batch),
            Err(StoreError::InjectedCommitFailure)
        ));
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT last_history_rowid FROM event_projection_metadata WHERE projection = 'event_owned'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("checkpoint after failed replay"),
            0
        );
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM resource_sites", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("site count after failed replay"),
            0
        );

        store
            .apply_replay_projection("event_owned", 1, 10, replay.high_water_rowid, &batch)
            .expect("retry replay batch");
        assert_eq!(
            store
                .connection
                .query_row(
                    "SELECT last_history_rowid FROM event_projection_metadata WHERE projection = 'event_owned'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
                .expect("checkpoint after retry"),
            10
        );
        assert_eq!(
            store
                .connection
                .query_row("SELECT COUNT(*) FROM resource_sites", [], |row| {
                    row.get::<_, i64>(0)
                })
                .expect("site count after retry"),
            1
        );
    }
}
