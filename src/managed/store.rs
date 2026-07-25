//! Crate-private SQLite persistence for normalized managed state.

#![allow(dead_code)] // Later managed engines own the remaining journals.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::Value;

use crate::domain::{
    Account, AccountId, Device, DeviceKey, Event, Observation, ObservationMetadata, Realm,
    Replicant,
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
    pub(crate) intent: Value,
    pub(crate) projection: Option<Value>,
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

    pub(crate) fn record_operation_and_project(
        &mut self,
        operation_id: &str,
        state: &str,
        intent: &Value,
        devices: &[Observation<Device>],
    ) -> Result<(), StoreError> {
        let fail_commit = self.take_commit_failure();
        let transaction = self.connection.transaction()?;
        transaction.execute(
            "INSERT INTO operation_journal(operation_id, state, intent_json, updated_at) VALUES (?1, ?2, ?3, datetime('now')) ON CONFLICT(operation_id) DO UPDATE SET state = excluded.state, intent_json = excluded.intent_json, updated_at = excluded.updated_at",
            params![operation_id, state, serde_json::to_string(intent)?],
        )?;
        for device in devices {
            persist_device(&transaction, device)?;
        }
        Self::commit(transaction, fail_commit)
    }

    pub(crate) fn append_operation_projection(
        &mut self,
        operation_id: &str,
        projection: &Value,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE operation_journal SET projection_json = ?2, updated_at = datetime('now') WHERE operation_id = ?1",
            params![operation_id, serde_json::to_string(projection)?],
        )?;
        Ok(())
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
                "SELECT state, intent_json, projection_json FROM operation_journal WHERE operation_id = ?1",
                [operation_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Option<String>>(2)?,
                    ))
                },
            )
            .optional()?
            .map(|(state, intent, projection)| {
                Ok(OperationJournalEntry {
                    state,
                    intent: serde_json::from_str(&intent)?,
                    projection: projection
                        .map(|value| serde_json::from_str(&value))
                        .transpose()?,
                })
            })
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
            "INSERT OR REPLACE INTO reconciliation_queue(work_id, realm, kind, payload_json) VALUES (?1, ?2, ?3, ?4)",
            params![work_id, realm_key(realm), kind, serde_json::to_string(payload)?],
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
    fn operation_intent_and_projection_are_atomic() {
        let mut store = Store::open_memory().expect("open memory store");
        store.fail_next_commit();
        assert!(matches!(
            store.record_operation_and_project(
                "operation-1",
                "registered",
                &json!({"kind": "activate"}),
                &[device(Realm::Live, "d1")]
            ),
            Err(StoreError::InjectedCommitFailure)
        ));
        assert_eq!(store.device_count().expect("device count"), 0);
        assert!(
            store
                .read_operation("operation-1")
                .expect("operation journal")
                .is_none()
        );
    }

    #[test]
    fn journal_primitives_round_trip_event_and_operation_records() {
        let mut store = Store::open_memory().expect("open memory store");
        store
            .append_event_and_project(&event(), "cursor-1", &[])
            .expect("append event");
        assert_eq!(store.read_events().expect("read events"), vec![event()]);
        store
            .record_operation_and_project(
                "operation-1",
                "registered",
                &json!({"kind": "activate"}),
                &[],
            )
            .expect("register operation");
        store
            .append_operation_projection("operation-1", &json!({"device": "d1"}))
            .expect("append projection");
        let entry = store
            .read_operation("operation-1")
            .expect("read operation")
            .expect("operation exists");
        assert_eq!(entry.state, "registered");
        assert_eq!(entry.intent, json!({"kind": "activate"}));
        assert_eq!(entry.projection, Some(json!({"device": "d1"})));
    }
}
