//! Crate-private SQLite persistence for normalized managed state.

#![allow(dead_code)] // Later managed engines own the remaining journals.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::path::PathBuf;
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

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::Value;
use tokio::sync::{Mutex as TokioMutex, mpsc as tokio_mpsc, oneshot};
use tracing::{debug, info, warn};

use crate::domain::{
    Account, AccountId, Device, DeviceId, DeviceKey, Event, Inventory, InventoryOwner, Location,
    LocationKey, Observation, ObservationMetadata, Realm, Replicant, ReplicantKey, Simulation,
    SimulationId, Star, StarKey, StarKnowledge,
};

const INITIAL_SCHEMA: &str = include_str!("../../migrations/0001_initial.sql");
const DEVICE_RELATIONSHIP_SEMANTICS_SCHEMA: &str =
    include_str!("../../migrations/0002_device_relationship_semantics.sql");
const RECONCILIATION_LEADER_SCHEMA: &str =
    include_str!("../../migrations/0003_reconciliation_leader.sql");
const CURRENT_SCHEMA_VERSION: i64 = 3;

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
    #[error("SQLite failure: {0}")]
    Sql(#[from] rusqlite::Error),
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
    #[error("database schema version {found} is newer than supported version {supported}")]
    UnsupportedSchemaVersion { found: i64, supported: i64 },
}

/// Internal durable store. No database handle crosses the crate boundary.
pub(crate) struct Store {
    connection: Connection,
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

enum StoreCommand {
    Execute {
        id: u64,
        operation_type: &'static str,
        queued_at: Instant,
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
                            command,
                        } => {
                            let queue_wait = queued_at.elapsed();
                            let execute_started = Instant::now();
                            command(&mut store);
                            debug!(
                                target: "replicant_client::store",
                                event = "store.command_completed",
                                command_id = id,
                                operation_type,
                                queue_wait_ms = queue_wait.as_millis() as u64,
                                execute_ms = execute_started.elapsed().as_millis() as u64,
                                elapsed_ms = queued_at.elapsed().as_millis() as u64,
                                "SQLite store command completed"
                            );
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
        let (response_sender, response_receiver) = oneshot::channel();
        self.sender
            .send(StoreCommand::Execute {
                id,
                operation_type,
                queued_at,
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
        let (response_sender, response_receiver) = mpsc::sync_channel(1);
        self.sender
            .try_send(StoreCommand::Execute {
                id,
                operation_type,
                queued_at,
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
    pub(crate) fn persist_star_knowledge(
        &mut self,
        value: &Observation<StarKnowledge>,
    ) -> Result<(), StoreError> {
        self.persist_star_knowledge_batch(std::slice::from_ref(value))
    }

    pub(crate) fn persist_star_knowledge_batch(
        &mut self,
        values: &[Observation<StarKnowledge>],
    ) -> Result<(), StoreError> {
        let values = values.to_vec();
        self.0
            .execute_blocking(move |s| s.persist_star_knowledge_batch(&values))
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
    pub(crate) fn has_event(&self, id: &str) -> Result<bool, StoreError> {
        let id = id.to_owned();
        self.0.execute_blocking(move |s| s.has_event(&id))
    }
    pub(crate) fn event_cursor(&self) -> Result<Option<String>, StoreError> {
        self.0.execute_blocking(|s| s.event_cursor())
    }
    pub(crate) fn read_events(&self) -> Result<Vec<Event>, StoreError> {
        self.0.execute_blocking(|store| store.read_events())
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
    pub(crate) fn append_event_and_project(
        &mut self,
        event: &Event,
        cursor: &str,
        devices: &[Observation<Device>],
        locations: &[Observation<Location>],
        reconciliation_targets: &[(Realm, String)],
    ) -> Result<bool, StoreError> {
        let event = event.clone();
        let cursor = cursor.to_owned();
        let devices = devices.to_vec();
        let locations = locations.to_vec();
        let reconciliation_targets = reconciliation_targets.to_vec();
        self.0.execute_blocking(move |s| {
            s.append_event_and_project(
                &event,
                &cursor,
                &devices,
                &locations,
                &reconciliation_targets,
            )
        })
    }
    pub(crate) fn append_event_and_decommission(
        &mut self,
        event: &Event,
        cursor: &str,
        keys: &[DeviceKey],
    ) -> Result<bool, StoreError> {
        let event = event.clone();
        let cursor = cursor.to_owned();
        let keys = keys.to_vec();
        self.0
            .execute_blocking(move |s| s.append_event_and_decommission(&event, &cursor, &keys))
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

impl Store {
    pub(crate) fn open_memory() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory()?;
        Self::configure(&connection, false)?;
        Self::migrate(connection)
    }

    pub(crate) fn open_file(path: &Path) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        Self::configure(&connection, true)?;
        Self::migrate(connection)
    }

    fn configure(connection: &Connection, file_database: bool) -> Result<(), StoreError> {
        connection.execute_batch("PRAGMA foreign_keys = ON;")?;
        connection.busy_timeout(Duration::from_secs(15))?;
        if file_database {
            connection.execute_batch("PRAGMA journal_mode = WAL;")?;
        }
        connection.execute_batch("PRAGMA synchronous = NORMAL;")?;
        Ok(())
    }

    fn migrate(mut connection: Connection) -> Result<Self, StoreError> {
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY NOT NULL);",
        )?;
        let version: Option<i64> = connection
            .query_row("SELECT version FROM schema_migrations LIMIT 1", [], |row| {
                row.get(0)
            })
            .optional()?;
        if version.is_none() || version == Some(0) {
            let transaction = connection.transaction()?;
            transaction.execute_batch(INITIAL_SCHEMA)?;
            #[cfg(test)]
            if INTERRUPT_NEXT_MIGRATION.with(|interrupted| interrupted.replace(false)) {
                return Err(StoreError::InjectedMigrationInterruption);
            }
            transaction.execute(
                "INSERT INTO schema_migrations(version) VALUES (?1) ON CONFLICT(version) DO NOTHING",
                [CURRENT_SCHEMA_VERSION],
            )?;
            transaction.execute("DELETE FROM schema_migrations WHERE version = 0", [])?;
            transaction.execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', ?1)",
                [CURRENT_SCHEMA_VERSION.to_string()],
            )?;
            transaction.commit()?;
        } else if version == Some(1) {
            let transaction = connection.transaction()?;
            transaction.execute_batch(DEVICE_RELATIONSHIP_SEMANTICS_SCHEMA)?;
            migrate_device_relationship_observations(&transaction)?;
            transaction.execute_batch(RECONCILIATION_LEADER_SCHEMA)?;
            transaction.execute(
                "UPDATE schema_migrations SET version = ?1 WHERE version = 1",
                [CURRENT_SCHEMA_VERSION],
            )?;
            transaction.execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', ?1) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [CURRENT_SCHEMA_VERSION.to_string()],
            )?;
            transaction.commit()?;
        } else if version == Some(2) {
            let transaction = connection.transaction()?;
            transaction.execute_batch(RECONCILIATION_LEADER_SCHEMA)?;
            transaction.execute(
                "UPDATE schema_migrations SET version = ?1 WHERE version = 2",
                [CURRENT_SCHEMA_VERSION],
            )?;
            transaction.execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', ?1) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                [CURRENT_SCHEMA_VERSION.to_string()],
            )?;
            transaction.commit()?;
        } else if let Some(found) = version.filter(|version| *version != CURRENT_SCHEMA_VERSION) {
            return Err(StoreError::UnsupportedSchemaVersion {
                found,
                supported: CURRENT_SCHEMA_VERSION,
            });
        }
        Ok(Self {
            connection,
            #[cfg(test)]
            fail_next_commit: false,
        })
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

    pub(crate) fn restore_star_knowledge(
        &self,
    ) -> Result<BTreeMap<(ReplicantKey, StarKey), Observation<StarKnowledge>>, StoreError> {
        let mut statement = self.connection.prepare("SELECT observation_json FROM replicant_star_knowledge ORDER BY realm, replicant_id, star_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut knowledge = BTreeMap::new();
        for row in rows {
            let observation = serde_json::from_str::<Observation<StarKnowledge>>(&row?)?;
            knowledge.insert(
                (
                    observation.value.replicant.clone(),
                    observation.value.star.clone(),
                ),
                observation,
            );
        }
        Ok(knowledge)
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
        transaction.execute("DELETE FROM stars", [])?;
        for star in stars {
            transaction.execute(
                "INSERT INTO stars(realm, star_id, payload_json) VALUES (?1, ?2, ?3)",
                params![
                    realm_key(&star.value.key.realm),
                    star.value.key.id.as_str(),
                    serde_json::to_string(star)?
                ],
            )?;
        }
        transaction.execute(
            "INSERT INTO catalogue_metadata(singleton, generated_at) VALUES (1, ?1) ON CONFLICT(singleton) DO UPDATE SET generated_at = excluded.generated_at",
            [generated_at],
        )?;
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn persist_star_knowledge(
        &mut self,
        knowledge: &Observation<StarKnowledge>,
    ) -> Result<(), StoreError> {
        self.persist_star_knowledge_batch(std::slice::from_ref(knowledge))
    }

    pub(crate) fn persist_star_knowledge_batch(
        &mut self,
        knowledge: &[Observation<StarKnowledge>],
    ) -> Result<(), StoreError> {
        if knowledge.is_empty() {
            return Ok(());
        }

        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        {
            let mut statement = transaction.prepare(
                "INSERT INTO replicant_star_knowledge(realm, replicant_id, star_id, observation_json) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(realm, replicant_id, star_id) DO UPDATE SET observation_json = excluded.observation_json",
            )?;
            for observation in knowledge {
                statement.execute(params![
                    realm_key(&observation.value.star.realm),
                    observation.value.replicant.id.as_str(),
                    observation.value.star.id.as_str(),
                    serde_json::to_string(observation)?,
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
        let mut missing = Vec::new();
        for row in rows {
            let (realm, id, serialized) = row?;
            let observation = serde_json::from_str::<Observation<Device>>(&serialized)?;
            if observation.metadata.reachability == crate::domain::Reachability::Reachable
                && !present.contains(&observation.value.key)
            {
                missing.push((realm, id));
            }
        }
        drop(statement);
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

    pub(crate) fn append_event_and_project(
        &mut self,
        event: &Event,
        cursor: &str,
        devices: &[Observation<Device>],
        locations: &[Observation<Location>],
        reconciliation_targets: &[(Realm, String)],
    ) -> Result<bool, StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO event_journal(event_id, realm, event_json, appended_at) VALUES (?1, ?2, ?3, datetime('now'))",
            params![event.id.as_str(), event.realm.as_ref().map(realm_key), serde_json::to_string(event)?],
        )? == 1;
        if !inserted {
            Self::commit(transaction, fail_commit)?;
            return Ok(false);
        }
        for device in devices {
            persist_device(&transaction, device)?;
        }
        for location in locations {
            transaction.execute(
                "INSERT INTO locations(realm, location_id, observation_json) VALUES (?1, ?2, ?3) ON CONFLICT(realm, location_id) DO UPDATE SET observation_json = excluded.observation_json",
                params![
                    realm_key(&location.value.key.realm),
                    location.value.key.id.as_str(),
                    serde_json::to_string(location)?,
                ],
            )?;
        }
        for target in reconciliation_targets {
            transaction.execute(
                "INSERT INTO reconciliation_queue(work_id, realm, kind, payload_json, not_before, attempts, state) VALUES (?1, ?2, 'location', ?3, NULL, 0, 'queued') ON CONFLICT(work_id) DO UPDATE SET realm = excluded.realm, kind = excluded.kind, payload_json = excluded.payload_json, not_before = NULL, attempts = 0, state = 'queued'",
                params![
                    format!("location:{}", target.1),
                    realm_key(&target.0),
                    serde_json::to_string(&serde_json::json!({ "id": target.1 }))?,
                ],
            )?;
        }
        advance_event_cursor(&transaction, cursor)?;
        Self::commit(transaction, fail_commit)?;
        Ok(true)
    }

    /// Same atomic guarantee as [`Store::append_event_and_project`], but also
    /// tombstones devices proven decommissioned by this event (an explicit
    /// removal signal, unlike a filtered/visibility-scoped collection page).
    pub(crate) fn append_event_and_decommission(
        &mut self,
        event: &Event,
        cursor: &str,
        decommissioned: &[DeviceKey],
    ) -> Result<bool, StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO event_journal(event_id, realm, event_json, appended_at) VALUES (?1, ?2, ?3, datetime('now'))",
            params![event.id.as_str(), event.realm.as_ref().map(realm_key), serde_json::to_string(event)?],
        )? == 1;
        if !inserted {
            Self::commit(transaction, fail_commit)?;
            return Ok(false);
        }
        for key in decommissioned {
            let realm = realm_key(&key.realm);
            let device_id = key.id.as_str();
            transaction.execute(
                "DELETE FROM devices WHERE realm = ?1 AND device_id = ?2",
                params![&realm, device_id],
            )?;
            transaction.execute(
                "INSERT OR REPLACE INTO tombstones(realm, kind, item_id, removed_at, evidence) VALUES (?1, 'device', ?2, datetime('now'), 'explicit-decommission-event')",
                params![&realm, device_id],
            )?;
            transaction.execute(
                "DELETE FROM reconciliation_queue WHERE realm = ?1 AND kind = 'device' AND work_id = ?2",
                params![&realm, format!("device:{device_id}")],
            )?;
        }
        advance_event_cursor(&transaction, cursor)?;
        Self::commit(transaction, fail_commit)?;
        Ok(true)
    }

    /// Durable dedup check: reports whether this event ID was already
    /// journaled, regardless of whether it arrived through the unfiltered log
    /// or the filtered SSE stream.
    pub(crate) fn has_event(&self, event_id: &str) -> Result<bool, StoreError> {
        self.connection
            .query_row(
                "SELECT 1 FROM event_journal WHERE event_id = ?1",
                [event_id],
                |_| Ok(()),
            )
            .optional()
            .map(|found| found.is_some())
            .map_err(StoreError::from)
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

    #[cfg(test)]
    pub(crate) fn backdate_event_cursor(&mut self, seconds: i64) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE event_cursors SET updated_at = datetime('now', ?1) WHERE stream = 'account'",
            params![format!("-{seconds} seconds")],
        )?;
        Ok(())
    }

    /// Persists a durable operation's initial intent, before any unsafe
    /// network transmission is attempted. `target_*` are `None` for
    /// operations with no single affected entity (for example, marking
    /// messages read).
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
            "INSERT INTO operation_journal(operation_id, state, target_realm, target_kind, target_id, intent_json, updated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, datetime('now')) ON CONFLICT(operation_id) DO UPDATE SET state = excluded.state, target_realm = excluded.target_realm, target_kind = excluded.target_kind, target_id = excluded.target_id, intent_json = excluded.intent_json, updated_at = excluded.updated_at",
            params![operation_id, state, target_realm, target_kind, target_id, serde_json::to_string(intent)?],
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

    pub(crate) fn read_events(&self) -> Result<Vec<Event>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT event_json FROM event_journal ORDER BY appended_at, event_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut events = Vec::new();
        for row in rows {
            events.push(serde_json::from_str(&row?)?);
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
        self.connection
            .query_row("SELECT COUNT(*) FROM event_journal", [], |row| row.get(0))
            .map_err(StoreError::from)
    }
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

/// Redis stream IDs are `<milliseconds>-<sequence>` decimal pairs.  They are
/// ordered numerically; lexical comparison misorders values such as `10-0`
/// and `9-999`.
fn compare_event_ids(left: &str, right: &str) -> Result<Ordering, StoreError> {
    fn parse(value: &str) -> Result<(u64, u64), StoreError> {
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

    Ok(parse(left)?.cmp(&parse(right)?))
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
    use std::collections::{BTreeMap, BTreeSet};
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
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives: Vec::new(),
                tags: Vec::new(),
                relationships: DeviceRelationships::default(),
                attach_capacity: None,
                stow_capacity: None,
                stow_used: None,
                operational_capacity: None,
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
            .execute_batch("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY NOT NULL); INSERT INTO schema_migrations VALUES (4);")
            .expect("seed future schema");
        drop(connection);
        assert!(matches!(
            Store::open_file(&path),
            Err(StoreError::UnsupportedSchemaVersion {
                found: 4,
                supported: 3
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
            store.append_event_and_project(
                &event(),
                "cursor-1",
                &[device(Realm::Live, "d1")],
                &[],
                &[]
            ),
            Err(StoreError::InjectedCommitFailure)
        ));
        assert_eq!(store.event_count().expect("event count"), 0);
        assert_eq!(store.event_cursor().expect("cursor"), None);
        assert_eq!(store.device_count().expect("device count"), 0);
        assert!(store.read_events().expect("event journal").is_empty());
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
            .append_event_and_project(&event(), "cursor-1", &[], &[], &[])
            .expect("append event");
        assert_eq!(store.read_events().expect("read events"), vec![event()]);
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
            .append_event_and_project(&event(), "1-0", &[], &[], &[])
            .expect("append event");
        assert!(store.has_event("1-0").expect("has_event after append"));
        assert!(!store.has_event("2-0").expect("has_event for unseen id"));
    }

    #[test]
    fn append_event_and_decommission_removes_device_and_tombstones_atomically() {
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
            .append_event_and_decommission(
                &decommission_event,
                "cursor-decom",
                std::slice::from_ref(&key),
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
            store.append_event_and_decommission(
                &event(),
                "cursor-decom",
                std::slice::from_ref(&key)
            ),
            Err(StoreError::InjectedCommitFailure)
        ));
        assert_eq!(store.device_count().expect("device count"), 1);
        assert_eq!(store.event_cursor().expect("cursor"), None);
        assert!(store.read_events().expect("event journal").is_empty());
        assert_eq!(reconciliation_count(&store, "device:d1"), 1);
    }

    #[test]
    fn full_device_reconciliation_cancels_removed_device_work() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .persist_devices(&[device(Realm::Live, "d1")])
            .expect("seed device");
        store
            .enqueue_reconciliation("device:d1", &Realm::Live, "device", &json!({"id": "d1"}))
            .expect("queue reconciliation");

        store
            .reconcile_owned_devices(&BTreeSet::new())
            .expect("reconcile missing device");

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
                store.append_event_and_project(&event(), "cursor-1", &[], &[], &[]),
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
                    command: Box::new(|_| {}),
                })
                .expect("fill bounded queue");
        }
        assert!(matches!(
            store.sender.try_send(StoreCommand::Execute {
                id: NEXT_STORE_COMMAND_ID.fetch_add(1, AtomicOrdering::Relaxed),
                operation_type: "test.queue_overflow",
                queued_at: Instant::now(),
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
}
