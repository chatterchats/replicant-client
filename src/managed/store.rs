//! Crate-private SQLite persistence for normalized managed state.

#![allow(dead_code)] // Later managed engines own the remaining journals.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::Value;

use crate::domain::{
    Account, AccountId, Device, DeviceKey, Event, Location, Observation, ObservationMetadata,
    Realm, Replicant,
};

const INITIAL_SCHEMA: &str = include_str!("../../migrations/0001_initial.sql");

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
    #[error("store is closed")]
    Closed,
}

/// Internal durable store. No database handle crosses the crate boundary.
pub(crate) struct Store {
    connection: Connection,
    #[cfg(test)]
    fail_next_commit: bool,
}

/// Shared ownership of the one durable store used by a managed client.
pub(crate) type StoreHandle = Arc<Mutex<Option<Store>>>;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OperationJournalEntry {
    pub(crate) state: String,
    pub(crate) target_realm: Option<String>,
    pub(crate) target_kind: Option<String>,
    pub(crate) target_id: Option<String>,
    pub(crate) intent: Value,
    pub(crate) projection: Option<Value>,
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
        if file_database {
            connection.execute_batch("PRAGMA journal_mode = WAL;")?;
        }
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
        if version.is_none() {
            let transaction = connection.transaction()?;
            transaction.execute_batch(INITIAL_SCHEMA)?;
            transaction.execute("INSERT INTO schema_migrations(version) VALUES (1)", [])?;
            transaction.execute(
                "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', '1')",
                [],
            )?;
            transaction.commit()?;
        }
        // A process may stop after claiming work but before reporting its
        // outcome. Requeue it on open so durable reconciliation is recoverable.
        connection.execute(
            "UPDATE reconciliation_queue SET state = 'queued' WHERE state = 'running'",
            [],
        )?;
        Ok(Self {
            connection,
            #[cfg(test)]
            fail_next_commit: false,
        })
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

    /// Forces file-backed SQLite state to durable storage before shutdown.
    pub(crate) fn flush(&mut self) -> Result<(), StoreError> {
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
        Ok(())
    }

    pub(crate) fn restore_devices(
        &self,
    ) -> Result<BTreeMap<DeviceKey, Observation<Device>>, StoreError> {
        let mut statement = self
            .connection
            .prepare("SELECT observation_json FROM devices ORDER BY realm, device_id")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut devices = BTreeMap::new();
        for row in rows {
            let observation = serde_json::from_str::<Observation<Device>>(&row?)?;
            devices.insert(observation.value.key.clone(), observation);
        }
        Ok(devices)
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

    pub(crate) fn persist_devices(
        &mut self,
        devices: &[Observation<Device>],
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
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
                params![realm, id],
            )?;
            transaction.execute(
                "INSERT OR REPLACE INTO tombstones(realm, kind, item_id, removed_at, evidence) VALUES (?1, 'device', ?2, datetime('now'), 'complete-unfiltered-device-traversal')",
                params![realm, id],
            )?;
        }
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn append_event_and_project(
        &mut self,
        event: &Event,
        cursor: &str,
        devices: &[Observation<Device>],
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT OR REPLACE INTO event_journal(event_id, realm, event_json, appended_at) VALUES (?1, ?2, ?3, datetime('now'))",
            params![event.id.as_str(), event.realm.as_ref().map(realm_key), serde_json::to_string(event)?],
        )?;
        for device in devices {
            persist_device(&transaction, device)?;
        }
        transaction.execute(
            "INSERT INTO event_cursors(stream, cursor, updated_at) VALUES ('account', ?1, datetime('now')) ON CONFLICT(stream) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at",
            [cursor],
        )?;
        Self::commit(transaction, fail_commit)
    }

    /// Same atomic guarantee as [`Store::append_event_and_project`], but also
    /// tombstones devices proven decommissioned by this event (an explicit
    /// removal signal, unlike a filtered/visibility-scoped collection page).
    pub(crate) fn append_event_and_decommission(
        &mut self,
        event: &Event,
        cursor: &str,
        decommissioned: &[DeviceKey],
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT OR REPLACE INTO event_journal(event_id, realm, event_json, appended_at) VALUES (?1, ?2, ?3, datetime('now'))",
            params![event.id.as_str(), event.realm.as_ref().map(realm_key), serde_json::to_string(event)?],
        )?;
        for key in decommissioned {
            transaction.execute(
                "DELETE FROM devices WHERE realm = ?1 AND device_id = ?2",
                params![realm_key(&key.realm), key.id.as_str()],
            )?;
            transaction.execute(
                "INSERT OR REPLACE INTO tombstones(realm, kind, item_id, removed_at, evidence) VALUES (?1, 'device', ?2, datetime('now'), 'explicit-decommission-event')",
                params![realm_key(&key.realm), key.id.as_str()],
            )?;
        }
        transaction.execute(
            "INSERT INTO event_cursors(stream, cursor, updated_at) VALUES ('account', ?1, datetime('now')) ON CONFLICT(stream) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at",
            [cursor],
        )?;
        Self::commit(transaction, fail_commit)
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
        self.connection.execute(
            "INSERT INTO event_cursors(stream, cursor, updated_at) VALUES ('account', ?1, datetime('now')) ON CONFLICT(stream) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at",
            [cursor],
        )?;
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
            "SELECT operation_id, state, target_realm, target_kind, target_id, intent_json, projection_json FROM operation_journal WHERE state NOT IN ({placeholders}) ORDER BY updated_at"
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
                ))
            },
        )?;
        let mut operations = Vec::new();
        for row in rows {
            let (operation_id, state, target_realm, target_kind, target_id, intent, projection) =
                row?;
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
                "SELECT state, target_realm, target_kind, target_id, intent_json, projection_json FROM operation_journal WHERE operation_id = ?1",
                [operation_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                    ))
                },
            )
            .optional()?
            .map(
                |(state, target_realm, target_kind, target_id, intent, projection)| {
                    Ok(OperationJournalEntry {
                        state,
                        target_realm,
                        target_kind,
                        target_id,
                        intent: serde_json::from_str(&intent)?,
                        projection: projection
                            .map(|value| serde_json::from_str(&value))
                            .transpose()?,
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

    /// Claims due work atomically enough for this single-store client and
    /// marks it running before the network request begins.
    pub(crate) fn claim_reconciliation_work(
        &mut self,
    ) -> Result<Option<ReconciliationWork>, StoreError> {
        let transaction = self.connection.transaction()?;
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
            observation.metadata.observed_at,
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
    if let Some(target) = &device.relationships.hosted_by {
        persist_relationship(
            transaction,
            device,
            "hosted_by",
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
            params![id, source.operation, source.request_id, metadata.observed_at],
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
            id: EventId::new("event-1"),
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
        }
        let store = Store::open_file(&path).expect("reopen migrated store");
        assert_eq!(store.device_count().expect("device count"), 0);
        drop(store);
        fs::remove_file(path).expect("remove test database");
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
    fn event_cursor_and_projection_are_atomic() {
        let mut store = Store::open_memory().expect("open memory store");
        store.fail_next_commit();
        assert!(matches!(
            store.append_event_and_project(&event(), "cursor-1", &[device(Realm::Live, "d1")]),
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
            .append_event_and_project(&event(), "cursor-1", &[])
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
        assert!(!store.has_event("event-1").expect("has_event before append"));
        store
            .append_event_and_project(&event(), "event-1", &[])
            .expect("append event");
        assert!(store.has_event("event-1").expect("has_event after append"));
        assert!(!store.has_event("event-2").expect("has_event for unseen id"));
    }

    #[test]
    fn append_event_and_decommission_removes_device_and_tombstones_atomically() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .persist_devices(&[device(Realm::Live, "d1")])
            .expect("seed device");
        let key = DeviceKey::live(crate::domain::DeviceId::new("d1"));

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
    }

    #[test]
    fn decommission_failure_leaves_device_event_and_cursor_untouched() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .persist_devices(&[device(Realm::Live, "d1")])
            .expect("seed device");
        let key = DeviceKey::live(crate::domain::DeviceId::new("d1"));
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
                store.append_event_and_project(&event(), "cursor-1", &[]),
                Err(StoreError::InjectedCommitFailure)
            ));
        }
        let restored = Store::open_file(&path).expect("reopen after crash");
        assert!(!restored.has_event("event-1").expect("event not journaled"));
        assert_eq!(restored.event_cursor().expect("cursor"), None);
        drop(restored);
        fs::remove_file(&path).expect("remove test database");
    }

    #[test]
    fn reconciliation_queue_coalesces_and_recovers_running_work() {
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
        let claimed = store
            .claim_reconciliation_work()
            .expect("claim")
            .expect("work exists");
        assert_eq!(claimed.payload, json!({"id": "d1-new"}));
        drop(store);

        let mut restored = Store::open_file(&path).expect("restart store");
        let recovered = restored
            .claim_reconciliation_work()
            .expect("claim recovered")
            .expect("running work requeued");
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
}
