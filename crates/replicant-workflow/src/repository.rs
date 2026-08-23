use std::{
    path::Path,
    str::FromStr,
    sync::{Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

use crate::{
    AutomationPolicy, AutomationTrigger, ClaimAcquireOutcome, FiniteExecution,
    FiniteExecutionClass, FiniteExecutionStatus, NewTrigger, NewWorkflow, ResourceClaim,
    ResourceKey, TriggerId, TriggerState, WorkflowActivity, WorkflowId, WorkflowInstance,
    WorkflowKind, WorkflowState, WorkflowStatus, WorkflowSummary,
};

const INITIAL_SCHEMA: &str = include_str!("../migrations/0001_initial.sql");
const ACTIVITY_SCHEMA: &str = include_str!("../migrations/0002_activity.sql");
const RESOURCE_CLAIMS_SCHEMA: &str = include_str!("../migrations/0003_resource_claims.sql");
const WAIT_INTENT_SCHEMA: &str = include_str!("../migrations/0004_wait_intent.sql");
const FINITE_EXECUTION_SCHEMA: &str =
    include_str!("../migrations/0005_finite_execution_history.sql");
const AUTOMATION_TRIGGER_SCHEMA: &str = include_str!("../migrations/0006_automation_triggers.sql");
const AUTOMATION_POLICY_SCHEMA: &str = include_str!("../migrations/0007_automation_policy.sql");
const RUNTIME_DOCUMENT_SCHEMA: &str = include_str!("../migrations/0008_runtime_documents.sql");
const FINITE_EXECUTION_RUNNING_SCHEMA: &str =
    include_str!("../migrations/0009_finite_execution_running.sql");
const CURRENT_DATABASE_SCHEMA: i64 = 9;

/// Runtime workflow persistence failures.
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    /// SQLite operation failed.
    #[error("SQLite failure: {0}")]
    Sql(#[from] rusqlite::Error),
    /// Typed payload serialization or deserialization failed.
    #[error("workflow payload serialization failure: {0}")]
    Serialization(#[from] serde_json::Error),
    /// A stable workflow kind was malformed.
    #[error("invalid workflow kind {0:?}")]
    InvalidKind(String),
    /// A resource namespace or identity was malformed.
    #[error("invalid resource key {0:?}")]
    InvalidResourceKey(ResourceKey),
    /// A malformed lifecycle status was found in SQLite.
    #[error("invalid persisted workflow status {0:?}")]
    InvalidStoredStatus(String),
    /// A malformed workflow ID was found in SQLite.
    #[error("invalid persisted workflow ID {0:?}")]
    InvalidStoredId(String),
    /// A malformed trigger ID was found in SQLite.
    #[error("invalid persisted trigger ID {0:?}")]
    InvalidStoredTriggerId(String),
    /// A malformed negative revision was found in SQLite.
    #[error("invalid persisted workflow revision {0}")]
    InvalidStoredRevision(i64),
    /// Workflow schema versions start at one.
    #[error("workflow schema version must be greater than zero")]
    InvalidWorkflowSchemaVersion,
    /// The runtime database was written by newer code.
    #[error("runtime database schema version {found} is newer than supported version {supported}")]
    UnsupportedDatabaseSchema {
        /// Version found in SQLite.
        found: i64,
        /// Highest version supported by this crate.
        supported: i64,
    },
    /// The recorded database migration sequence has a gap or invalid version.
    #[error("invalid runtime database migration history: {0:?}")]
    InvalidMigrationHistory(Vec<i64>),
    /// SQLite detected database corruption.
    #[error("runtime database integrity check failed: {0}")]
    DatabaseIntegrity(String),
    /// No workflow has the requested ID.
    #[error("workflow {0} was not found")]
    NotFound(WorkflowId),
    /// A terminal workflow cannot acquire new resources.
    #[error("terminal workflow {workflow_id} cannot acquire resources")]
    TerminalClaimOwner {
        /// Workflow attempting the acquisition.
        workflow_id: WorkflowId,
    },
    /// Another workflow exclusively owns the resource.
    #[error("resource {resource:?} is claimed by workflow {owner}")]
    ClaimConflict {
        /// Requested resource.
        resource: ResourceKey,
        /// Current owning workflow.
        owner: WorkflowId,
    },
    /// The requested lifecycle transition is not valid.
    #[error("invalid workflow transition from {from:?} to {to:?}")]
    InvalidTransition {
        /// Persisted status.
        from: WorkflowStatus,
        /// Requested status.
        to: WorkflowStatus,
    },
    /// Another writer updated this row first.
    #[error("workflow {id} revision changed; expected {expected}")]
    ConcurrentUpdate {
        /// Workflow ID.
        id: WorkflowId,
        /// Revision supplied by the caller.
        expected: u64,
    },
    /// No trigger has the requested ID.
    #[error("trigger {0} was not found")]
    TriggerNotFound(TriggerId),
    /// Another writer updated this trigger row first.
    #[error("trigger {id} revision changed; expected {expected}")]
    ConcurrentTriggerUpdate {
        /// Trigger ID.
        id: TriggerId,
        /// Revision supplied by the caller.
        expected: u64,
    },
    /// SQLite revisions are signed 64-bit integers.
    #[error("workflow revision {0} is outside SQLite's supported range")]
    RevisionOutOfRange(u64),
    /// System clock cannot produce a Unix timestamp.
    #[error("system clock is before the Unix epoch")]
    Clock,
    /// A previous thread panicked while holding the repository connection.
    #[error("workflow repository lock is poisoned")]
    LockPoisoned,
}

/// SQLite repository for authoritative workflow state.
pub struct WorkflowRepository {
    connection: Mutex<Connection>,
}

impl WorkflowRepository {
    /// Opens or creates a runtime database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RepositoryError> {
        let connection = Connection::open(path)?;
        Self::from_connection(connection, true)
    }

    /// Creates an isolated in-memory runtime database.
    pub fn open_in_memory() -> Result<Self, RepositoryError> {
        Self::from_connection(Connection::open_in_memory()?, false)
    }

    fn from_connection(
        connection: Connection,
        file_database: bool,
    ) -> Result<Self, RepositoryError> {
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let integrity: String =
            connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err(RepositoryError::DatabaseIntegrity(integrity));
        }
        if file_database {
            connection.execute_batch("PRAGMA journal_mode = WAL;")?;
        }
        connection.execute_batch("PRAGMA synchronous = NORMAL;")?;
        let repository = Self {
            connection: Mutex::new(connection),
        };
        repository.migrate()?;
        Ok(repository)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, RepositoryError> {
        self.connection
            .lock()
            .map_err(|_| RepositoryError::LockPoisoned)
    }

    fn migrate(&self) -> Result<(), RepositoryError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "CREATE TABLE IF NOT EXISTS runtime_schema_migrations (version INTEGER PRIMARY KEY NOT NULL)",
            [],
        )?;
        let found: i64 = transaction.query_row(
            "SELECT COALESCE(MAX(version), 0) FROM runtime_schema_migrations",
            [],
            |row| row.get(0),
        )?;
        if found > CURRENT_DATABASE_SCHEMA {
            return Err(RepositoryError::UnsupportedDatabaseSchema {
                found,
                supported: CURRENT_DATABASE_SCHEMA,
            });
        }
        let versions = {
            let mut statement = transaction
                .prepare("SELECT version FROM runtime_schema_migrations ORDER BY version")?;
            statement
                .query_map([], |row| row.get(0))?
                .collect::<Result<Vec<i64>, _>>()?
        };
        if versions != (1..=found).collect::<Vec<_>>() {
            return Err(RepositoryError::InvalidMigrationHistory(versions));
        }
        if found < 1 {
            transaction.execute_batch(INITIAL_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (1)",
                [],
            )?;
        }
        if found < 2 {
            transaction.execute_batch(ACTIVITY_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (2)",
                [],
            )?;
        }
        if found < 3 {
            transaction.execute_batch(RESOURCE_CLAIMS_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (3)",
                [],
            )?;
        }
        if found < 4 {
            transaction.execute_batch(WAIT_INTENT_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (4)",
                [],
            )?;
        }
        if found < 5 {
            transaction.execute_batch(FINITE_EXECUTION_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (5)",
                [],
            )?;
        }
        if found < 6 {
            transaction.execute_batch(AUTOMATION_TRIGGER_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (6)",
                [],
            )?;
        }
        if found < 7 {
            transaction.execute_batch(AUTOMATION_POLICY_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (7)",
                [],
            )?;
        }
        if found < 8 {
            transaction.execute_batch(RUNTIME_DOCUMENT_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (8)",
                [],
            )?;
        }
        if found < 9 {
            transaction.execute_batch(FINITE_EXECUTION_RUNNING_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (9)",
                [],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    /// Reads the persisted global automation safety policy.
    pub fn automation_policy(&self) -> Result<AutomationPolicy, RepositoryError> {
        Ok(self.connection()?.query_row(
            "SELECT automatic_triggers_enabled, workflows_paused
             FROM automation_policy WHERE singleton = 1",
            [],
            |row| {
                Ok(AutomationPolicy {
                    automatic_triggers_enabled: row.get(0)?,
                    workflows_paused: row.get(1)?,
                })
            },
        )?)
    }

    /// Replaces the persisted global automation safety policy.
    pub fn set_automation_policy(
        &self,
        policy: AutomationPolicy,
    ) -> Result<AutomationPolicy, RepositoryError> {
        self.connection()?.execute(
            "UPDATE automation_policy SET automatic_triggers_enabled = ?1, workflows_paused = ?2
             WHERE singleton = 1",
            params![policy.automatic_triggers_enabled, policy.workflows_paused],
        )?;
        Ok(policy)
    }

    /// Creates a persisted trigger definition.
    pub fn create_trigger(
        &self,
        trigger: NewTrigger,
    ) -> Result<AutomationTrigger, RepositoryError> {
        let id = TriggerId::new();
        let now = now_millis()?;
        let condition = serde_json::to_string(&trigger.condition)?;
        let target = serde_json::to_string(&trigger.target)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO automation_triggers (
                id, name, condition_json, target_json, enabled, created_at, updated_at,
                next_run_at, event_cursor
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6, ?7, ?8)",
            params![
                id.to_string(),
                trigger.name,
                condition,
                target,
                trigger.enabled,
                now,
                trigger.next_run_at,
                trigger.event_cursor,
            ],
        )?;
        let trigger =
            read_trigger_in(&transaction, id)?.ok_or(RepositoryError::TriggerNotFound(id))?;
        transaction.commit()?;
        Ok(trigger)
    }

    /// Reads one persisted trigger.
    pub fn read_trigger(
        &self,
        id: TriggerId,
    ) -> Result<Option<AutomationTrigger>, RepositoryError> {
        let connection = self.connection()?;
        read_trigger_in(&connection, id)
    }

    /// Lists trigger definitions in creation order.
    pub fn list_triggers(&self) -> Result<Vec<AutomationTrigger>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(&format!(
            "SELECT {TRIGGER_COLUMNS} FROM automation_triggers ORDER BY created_at, id"
        ))?;
        let rows = statement.query_map([], row_to_trigger)?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Replaces an editable trigger definition with optimistic concurrency.
    pub fn update_trigger(
        &self,
        id: TriggerId,
        expected_revision: u64,
        state: TriggerState,
    ) -> Result<AutomationTrigger, RepositoryError> {
        let expected = i64::try_from(expected_revision)
            .map_err(|_| RepositoryError::RevisionOutOfRange(expected_revision))?;
        let now = now_millis()?;
        let condition = serde_json::to_string(&state.condition)?;
        let target = serde_json::to_string(&state.target)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if read_trigger_in(&transaction, id)?.is_none() {
            return Err(RepositoryError::TriggerNotFound(id));
        }
        let changed = transaction.execute(
            "UPDATE automation_triggers SET name = ?1, condition_json = ?2, target_json = ?3,
                enabled = ?4, next_run_at = ?5, event_cursor = ?6, updated_at = ?7,
                last_error = NULL, revision = revision + 1
             WHERE id = ?8 AND revision = ?9",
            params![
                state.name,
                condition,
                target,
                state.enabled,
                state.next_run_at,
                state.event_cursor,
                now,
                id.to_string(),
                expected,
            ],
        )?;
        if changed == 0 {
            return Err(RepositoryError::ConcurrentTriggerUpdate {
                id,
                expected: expected_revision,
            });
        }
        let trigger =
            read_trigger_in(&transaction, id)?.ok_or(RepositoryError::TriggerNotFound(id))?;
        transaction.commit()?;
        Ok(trigger)
    }

    /// Deletes a trigger and its dedupe receipts.
    pub fn delete_trigger(&self, id: TriggerId) -> Result<bool, RepositoryError> {
        Ok(self.connection()?.execute(
            "DELETE FROM automation_triggers WHERE id = ?1",
            [id.to_string()],
        )? != 0)
    }

    /// Atomically claims one logical firing. Duplicate keys never launch twice.
    pub fn claim_trigger_firing(
        &self,
        id: TriggerId,
        dedupe_key: &str,
        fired_at: i64,
        next_run_at: Option<i64>,
    ) -> Result<bool, RepositoryError> {
        self.claim_trigger_firing_with_policy(id, dedupe_key, fired_at, next_run_at, false)
    }

    /// Atomically claims one automatic firing only while global automation permits it.
    pub fn claim_automatic_trigger_firing(
        &self,
        id: TriggerId,
        dedupe_key: &str,
        fired_at: i64,
        next_run_at: Option<i64>,
    ) -> Result<bool, RepositoryError> {
        self.claim_trigger_firing_with_policy(id, dedupe_key, fired_at, next_run_at, true)
    }

    fn claim_trigger_firing_with_policy(
        &self,
        id: TriggerId,
        dedupe_key: &str,
        fired_at: i64,
        next_run_at: Option<i64>,
        automatic: bool,
    ) -> Result<bool, RepositoryError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if automatic
            && !transaction.query_row(
                "SELECT automatic_triggers_enabled FROM automation_policy WHERE singleton = 1",
                [],
                |row| row.get::<_, bool>(0),
            )?
        {
            return Ok(false);
        }
        let enabled = transaction
            .query_row(
                "SELECT enabled FROM automation_triggers WHERE id = ?1",
                [id.to_string()],
                |row| row.get::<_, bool>(0),
            )
            .optional()?
            .ok_or(RepositoryError::TriggerNotFound(id))?;
        if !enabled {
            return Ok(false);
        }
        let inserted = transaction.execute(
            "INSERT OR IGNORE INTO automation_trigger_firings (trigger_id, dedupe_key, claimed_at)
             VALUES (?1, ?2, ?3)",
            params![id.to_string(), dedupe_key, fired_at],
        )? != 0;
        if inserted {
            transaction.execute(
                "UPDATE automation_triggers SET last_fired_at = ?1, next_run_at = ?2,
                    last_error = NULL, updated_at = ?1, revision = revision + 1 WHERE id = ?3",
                params![fired_at, next_run_at, id.to_string()],
            )?;
        }
        transaction.commit()?;
        Ok(inserted)
    }

    /// Advances one event trigger's durable managed-event cursor.
    pub fn set_trigger_cursor(&self, id: TriggerId, cursor: &str) -> Result<(), RepositoryError> {
        let changed = self.connection()?.execute(
            "UPDATE automation_triggers SET event_cursor = ?1 WHERE id = ?2",
            params![cursor, id.to_string()],
        )?;
        if changed == 0 {
            return Err(RepositoryError::TriggerNotFound(id));
        }
        Ok(())
    }

    /// Records a visible trigger evaluation or launch error.
    pub fn set_trigger_error(
        &self,
        id: TriggerId,
        error: Option<&str>,
    ) -> Result<(), RepositoryError> {
        let changed = self.connection()?.execute(
            "UPDATE automation_triggers SET last_error = ?1, updated_at = ?2,
                revision = revision + 1 WHERE id = ?3",
            params![error, now_millis()?, id.to_string()],
        )?;
        if changed == 0 {
            return Err(RepositoryError::TriggerNotFound(id));
        }
        Ok(())
    }

    /// Reads one application-owned durable JSON document.
    pub fn read_document(
        &self,
        namespace: &str,
        key: &str,
    ) -> Result<Option<(Value, u64)>, RepositoryError> {
        validate_document_identity(namespace, key)?;
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT value_json, revision FROM runtime_documents WHERE namespace = ?1 AND key = ?2",
                params![namespace, key],
                |row| {
                    let json: String = row.get(0)?;
                    let revision: i64 = row.get(1)?;
                    let value = serde_json::from_str(&json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                    let revision = u64::try_from(revision).map_err(|_| {
                        to_sql_conversion_error(RepositoryError::InvalidStoredRevision(revision))
                    })?;
                    Ok((value, revision))
                },
            )
            .optional()
            .map_err(Into::into)
    }

    /// Lists application-owned durable JSON documents in stable key order.
    pub fn list_documents(
        &self,
        namespace: &str,
    ) -> Result<Vec<(String, Value, u64)>, RepositoryError> {
        validate_document_identity(namespace, "_")?;
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT key, value_json, revision FROM runtime_documents WHERE namespace = ?1 ORDER BY key",
        )?;
        let rows = statement.query_map([namespace], |row| {
            let key: String = row.get(0)?;
            let json: String = row.get(1)?;
            let revision: i64 = row.get(2)?;
            let value = serde_json::from_str(&json).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    1,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
            let revision = u64::try_from(revision).map_err(|_| {
                to_sql_conversion_error(RepositoryError::InvalidStoredRevision(revision))
            })?;
            Ok((key, value, revision))
        })?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Inserts or replaces one application-owned durable JSON document.
    pub fn put_document<T: Serialize>(
        &self,
        namespace: &str,
        key: &str,
        value: &T,
    ) -> Result<u64, RepositoryError> {
        validate_document_identity(namespace, key)?;
        let json = serde_json::to_string(value)?;
        let now = now_millis()?;
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO runtime_documents (namespace, key, value_json, revision, created_at, updated_at)
             VALUES (?1, ?2, ?3, 0, ?4, ?4)
             ON CONFLICT(namespace, key) DO UPDATE SET
                value_json = excluded.value_json,
                revision = runtime_documents.revision + 1,
                updated_at = excluded.updated_at",
            params![namespace, key, json, now],
        )?;
        let revision: i64 = connection.query_row(
            "SELECT revision FROM runtime_documents WHERE namespace = ?1 AND key = ?2",
            params![namespace, key],
            |row| row.get(0),
        )?;
        u64::try_from(revision).map_err(|_| RepositoryError::InvalidStoredRevision(revision))
    }

    /// Deletes one application-owned durable JSON document.
    pub fn delete_document(&self, namespace: &str, key: &str) -> Result<bool, RepositoryError> {
        validate_document_identity(namespace, key)?;
        Ok(self.connection()?.execute(
            "DELETE FROM runtime_documents WHERE namespace = ?1 AND key = ?2",
            params![namespace, key],
        )? != 0)
    }

    /// Creates and returns a queued workflow atomically.
    pub fn create<C: Serialize, P: Serialize>(
        &self,
        workflow: NewWorkflow<C, P>,
    ) -> Result<WorkflowInstance, RepositoryError> {
        if workflow.schema_version == 0 {
            return Err(RepositoryError::InvalidWorkflowSchemaVersion);
        }
        let id = WorkflowId::new();
        let now = now_millis()?;
        let config_json = serde_json::to_string(&workflow.config)?;
        let checkpoint_json = serde_json::to_string(&workflow.checkpoint)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT INTO workflow_instances (
                id, kind, schema_version, config_json, checkpoint_json, status,
                current_step, created_at, updated_at, parent_id
             ) VALUES (?1, ?2, ?3, ?4, ?5, 'queued', ?6, ?7, ?7, ?8)",
            params![
                id.to_string(),
                workflow.kind.as_str(),
                workflow.schema_version,
                config_json,
                checkpoint_json,
                workflow.current_step,
                now,
                workflow.parent_id.map(|parent| parent.to_string()),
            ],
        )?;
        let instance = read_in(&transaction, id)?.ok_or(RepositoryError::NotFound(id))?;
        transaction.commit()?;
        Ok(instance)
    }

    /// Reads one workflow instance.
    pub fn read(&self, id: WorkflowId) -> Result<Option<WorkflowInstance>, RepositoryError> {
        let connection = self.connection()?;
        read_in(&connection, id)
    }

    /// Lists workflow instances in creation order.
    pub fn list(&self) -> Result<Vec<WorkflowInstance>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(&format!(
            "SELECT {COLUMNS} FROM workflow_instances ORDER BY created_at, id"
        ))?;
        let rows = statement.query_map([], row_to_instance)?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Lists non-terminal workflow instances in creation order.
    pub fn list_active(&self) -> Result<Vec<WorkflowInstance>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(&format!(
            "SELECT {COLUMNS} FROM workflow_instances
             WHERE status IN ('queued', 'running', 'waiting', 'reconciling', 'paused')
             ORDER BY created_at, id"
        ))?;
        let rows = statement.query_map([], row_to_instance)?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Lists direct child workflows in creation order.
    pub fn list_children(
        &self,
        parent_id: WorkflowId,
    ) -> Result<Vec<WorkflowInstance>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(&format!(
            "SELECT {COLUMNS} FROM workflow_instances WHERE parent_id = ?1 ORDER BY created_at, id"
        ))?;
        let rows = statement.query_map([parent_id.to_string()], row_to_instance)?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Lists blob-free workflow summaries in creation order.
    pub fn list_summaries(&self) -> Result<Vec<WorkflowSummary>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(&format!(
            "SELECT {SUMMARY_COLUMNS} FROM workflow_instances ORDER BY created_at, id"
        ))?;
        let rows = statement.query_map([], row_to_summary)?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Lists blob-free summaries for non-terminal workflows in creation order.
    pub fn list_active_summaries(&self) -> Result<Vec<WorkflowSummary>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(&format!(
            "SELECT {SUMMARY_COLUMNS} FROM workflow_instances
             WHERE status IN ('queued', 'running', 'waiting', 'reconciling', 'paused')
             ORDER BY created_at, id"
        ))?;
        let rows = statement.query_map([], row_to_summary)?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Removes terminal leaf rows older than `cutoff_millis`, including completed trees.
    ///
    /// Rows with claims or any retained child are preserved. Activity is removed in the
    /// same transaction before each leaf row so foreign keys remain valid.
    pub fn prune_terminal_before(&self, cutoff_millis: i64) -> Result<usize, RepositoryError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let mut removed = 0;
        loop {
            let ids = {
                let mut statement = transaction.prepare(
                    "SELECT workflow.id FROM workflow_instances workflow
                     WHERE workflow.status IN ('succeeded', 'failed', 'cancelled')
                       AND workflow.updated_at < ?1
                       AND NOT EXISTS (
                           SELECT 1 FROM workflow_resource_claims claim
                           WHERE claim.workflow_id = workflow.id
                       )
                       AND NOT EXISTS (
                           SELECT 1 FROM workflow_instances child
                           WHERE child.parent_id = workflow.id
                       )",
                )?;
                statement
                    .query_map([cutoff_millis], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            };
            if ids.is_empty() {
                break;
            }
            for id in &ids {
                transaction
                    .execute("DELETE FROM workflow_activity WHERE workflow_id = ?1", [id])?;
                transaction.execute("DELETE FROM workflow_instances WHERE id = ?1", [id])?;
            }
            removed += ids.len();
        }
        transaction.commit()?;
        Ok(removed)
    }

    /// Persists a completed, sanitized report or action execution.
    pub fn record_finite_execution(
        &self,
        operation_class: FiniteExecutionClass,
        kind: &str,
        status: FiniteExecutionStatus,
        started_at: i64,
        result: Option<&Value>,
        error: Option<&str>,
    ) -> Result<FiniteExecution, RepositoryError> {
        let execution = FiniteExecution {
            id: Uuid::new_v4().to_string(),
            operation_class,
            kind: kind.to_owned(),
            status,
            started_at,
            finished_at: now_millis()?,
            result: result.cloned(),
            error: error.map(str::to_owned),
        };
        let result_json = execution
            .result
            .as_ref()
            .map(serde_json::to_string)
            .transpose()?;
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO finite_executions (
                id, operation_class, kind, status, started_at, finished_at, result_json, error
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                execution.id,
                execution.operation_class.as_str(),
                execution.kind,
                execution.status.as_str(),
                execution.started_at,
                execution.finished_at,
                result_json,
                execution.error,
            ],
        )?;
        Ok(execution)
    }

    /// Records an execution that has started but not finished.
    ///
    /// Returned immediately to the caller so long-running actions can be
    /// followed through history and live updates rather than by blocking an
    /// HTTP request until they complete.
    pub fn begin_finite_execution(
        &self,
        operation_class: FiniteExecutionClass,
        kind: &str,
        started_at: i64,
    ) -> Result<FiniteExecution, RepositoryError> {
        let execution = FiniteExecution {
            id: Uuid::new_v4().to_string(),
            operation_class,
            kind: kind.to_owned(),
            status: FiniteExecutionStatus::Running,
            started_at,
            finished_at: started_at,
            result: None,
            error: None,
        };
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO finite_executions (
                id, operation_class, kind, status, started_at, finished_at, result_json, error
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL)",
            params![
                execution.id,
                execution.operation_class.as_str(),
                execution.kind,
                execution.status.as_str(),
                execution.started_at,
                execution.finished_at,
            ],
        )?;
        Ok(execution)
    }

    /// Completes an execution previously opened by
    /// [`WorkflowRepository::begin_finite_execution`].
    pub fn complete_finite_execution(
        &self,
        id: &str,
        status: FiniteExecutionStatus,
        result: Option<&Value>,
        error: Option<&str>,
    ) -> Result<(), RepositoryError> {
        let result_json = result.map(serde_json::to_string).transpose()?;
        let connection = self.connection()?;
        connection.execute(
            "UPDATE finite_executions
             SET status = ?2, finished_at = ?3, result_json = ?4, error = ?5
             WHERE id = ?1",
            params![id, status.as_str(), now_millis()?, result_json, error],
        )?;
        Ok(())
    }

    /// Lists finite executions newest first.
    pub fn finite_execution_history(&self) -> Result<Vec<FiniteExecution>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, operation_class, kind, status, started_at, finished_at, result_json, error
             FROM finite_executions ORDER BY finished_at DESC, id DESC",
        )?;
        let rows = statement.query_map([], |row| {
            let operation_class = match row.get::<_, String>(1)?.as_str() {
                "report" => FiniteExecutionClass::Report,
                "action" => FiniteExecutionClass::Action,
                value => return Err(invalid_stored_execution(value)),
            };
            let status = match row.get::<_, String>(3)?.as_str() {
                "running" => FiniteExecutionStatus::Running,
                "succeeded" => FiniteExecutionStatus::Succeeded,
                "skipped" => FiniteExecutionStatus::Skipped,
                "failed" => FiniteExecutionStatus::Failed,
                value => return Err(invalid_stored_execution(value)),
            };
            let result_json = row.get::<_, Option<String>>(6)?;
            Ok(FiniteExecution {
                id: row.get(0)?,
                operation_class,
                kind: row.get(2)?,
                status,
                started_at: row.get(4)?,
                finished_at: row.get(5)?,
                result: result_json
                    .map(|value| serde_json::from_str(&value))
                    .transpose()
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?,
                error: row.get(7)?,
            })
        })?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Appends a durable activity message.
    pub fn append_activity(
        &self,
        workflow_id: WorkflowId,
        message: impl Into<String>,
    ) -> Result<WorkflowActivity, RepositoryError> {
        let created_at = now_millis()?;
        let message = message.into();
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO workflow_activity (workflow_id, created_at, message) VALUES (?1, ?2, ?3)",
            params![workflow_id.to_string(), created_at, message],
        )?;
        Ok(WorkflowActivity {
            id: connection.last_insert_rowid(),
            workflow_id,
            created_at,
            message,
        })
    }

    /// Lists durable activity for one workflow in emission order.
    pub fn activity(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<Vec<WorkflowActivity>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, created_at, message FROM workflow_activity WHERE workflow_id = ?1 ORDER BY id",
        )?;
        let rows = statement.query_map([workflow_id.to_string()], |row| {
            Ok(WorkflowActivity {
                id: row.get(0)?,
                workflow_id,
                created_at: row.get(1)?,
                message: row.get(2)?,
            })
        })?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Lists durable activity after a global sequence cursor.
    pub fn activity_since(&self, after_id: i64) -> Result<Vec<WorkflowActivity>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT id, workflow_id, created_at, message FROM workflow_activity WHERE id > ?1 ORDER BY id",
        )?;
        let rows = statement.query_map([after_id], |row| {
            let stored_id = row.get::<_, String>(1)?;
            let workflow_id: WorkflowId = stored_id
                .parse()
                .map_err(|_| rusqlite::Error::InvalidParameterName(stored_id))?;
            Ok(WorkflowActivity {
                id: row.get(0)?,
                workflow_id,
                created_at: row.get(2)?,
                message: row.get(3)?,
            })
        })?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Returns the latest global activity sequence without loading history.
    pub fn latest_activity_id(&self) -> Result<i64, RepositoryError> {
        Ok(self.connection()?.query_row(
            "SELECT COALESCE(MAX(id), 0) FROM workflow_activity",
            [],
            |row| row.get(0),
        )?)
    }

    /// Atomically acquires an exclusive resource claim.
    ///
    /// Reacquiring a claim owned by `workflow_id` is idempotent and refreshes
    /// its update timestamp. A claim owned by any other workflow conflicts
    /// until it is explicitly released or startup reconciliation removes it.
    pub fn acquire_claim(
        &self,
        workflow_id: WorkflowId,
        resource: ResourceKey,
    ) -> Result<ClaimAcquireOutcome, RepositoryError> {
        let (namespace, key) = resource.persisted_parts()?;
        let now = now_millis()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let owner =
            read_in(&transaction, workflow_id)?.ok_or(RepositoryError::NotFound(workflow_id))?;
        if owner.status.is_terminal() {
            return Err(RepositoryError::TerminalClaimOwner { workflow_id });
        }
        let existing = transaction
            .query_row(
                "SELECT workflow_id, acquired_at FROM workflow_resource_claims
                 WHERE resource_namespace = ?1 AND resource_key = ?2",
                params![namespace, key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        if let Some((existing_owner, acquired_at)) = existing {
            let existing_owner = parse_id(existing_owner)?;
            if existing_owner != workflow_id {
                return Err(RepositoryError::ClaimConflict {
                    resource,
                    owner: existing_owner,
                });
            }
            transaction.execute(
                "UPDATE workflow_resource_claims SET updated_at = ?1
                 WHERE resource_namespace = ?2 AND resource_key = ?3",
                params![now, namespace, key],
            )?;
            transaction.commit()?;
            return Ok(ClaimAcquireOutcome::AlreadyOwned(ResourceClaim {
                resource,
                workflow_id,
                acquired_at,
                updated_at: now,
            }));
        }
        transaction.execute(
            "INSERT INTO workflow_resource_claims (
                resource_namespace, resource_key, workflow_id, acquired_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?4)",
            params![namespace, key, workflow_id.to_string(), now],
        )?;
        transaction.commit()?;
        Ok(ClaimAcquireOutcome::Acquired(ResourceClaim {
            resource,
            workflow_id,
            acquired_at: now,
            updated_at: now,
        }))
    }

    /// Atomically releases a resource only when `workflow_id` owns it.
    pub fn release_claim(
        &self,
        workflow_id: WorkflowId,
        resource: &ResourceKey,
    ) -> Result<bool, RepositoryError> {
        let (namespace, key) = resource.persisted_parts()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let released = transaction.execute(
            "DELETE FROM workflow_resource_claims
             WHERE resource_namespace = ?1 AND resource_key = ?2 AND workflow_id = ?3",
            params![namespace, key, workflow_id.to_string()],
        )? != 0;
        transaction.commit()?;
        Ok(released)
    }

    /// Atomically releases every claim owned by one workflow.
    pub fn release_claims(&self, workflow_id: WorkflowId) -> Result<usize, RepositoryError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let released = transaction.execute(
            "DELETE FROM workflow_resource_claims WHERE workflow_id = ?1",
            [workflow_id.to_string()],
        )?;
        transaction.commit()?;
        Ok(released)
    }

    /// Lists claims owned by one workflow.
    pub fn claims(&self, workflow_id: WorkflowId) -> Result<Vec<ResourceClaim>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT resource_namespace, resource_key, acquired_at, updated_at
             FROM workflow_resource_claims WHERE workflow_id = ?1
             ORDER BY resource_namespace, resource_key",
        )?;
        let rows = statement.query_map([workflow_id.to_string()], |row| {
            Ok(ResourceClaim {
                resource: resource_key(row.get(0)?, row.get(1)?),
                workflow_id,
                acquired_at: row.get(2)?,
                updated_at: row.get(3)?,
            })
        })?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Lists every persisted device claim across active workflows.
    ///
    /// This is intentionally a bulk query so coordinators that need to select
    /// an unclaimed fleet do not perform one SQLite query per workflow.
    pub fn device_claims(&self) -> Result<Vec<ResourceClaim>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT resource_key, workflow_id, acquired_at, updated_at
             FROM workflow_resource_claims
             WHERE resource_namespace = 'device'
             ORDER BY resource_key",
        )?;
        let rows = statement.query_map([], |row| {
            let key = row.get::<_, String>(0)?;
            let workflow_id = row.get::<_, String>(1)?;
            Ok((
                key,
                workflow_id,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        rows.map(|row| {
            let (key, workflow_id, acquired_at, updated_at) = row?;
            let workflow_id = parse_id(workflow_id)?;
            Ok(ResourceClaim {
                resource: ResourceKey::Device(key),
                workflow_id,
                acquired_at,
                updated_at,
            })
        })
        .collect()
    }

    /// Lists every persisted Autofactory claim across active workflows.
    ///
    /// Expansion coordinators use this to share manufacturing locations while
    /// reserving only the specific printers assigned to active print work.
    pub fn autofactory_claims(&self) -> Result<Vec<ResourceClaim>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT resource_key, workflow_id, acquired_at, updated_at
             FROM workflow_resource_claims
             WHERE resource_namespace = 'autofactory'
             ORDER BY resource_key",
        )?;
        let rows = statement.query_map([], |row| {
            let key = row.get::<_, String>(0)?;
            let workflow_id = row.get::<_, String>(1)?;
            Ok((
                key,
                workflow_id,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        rows.map(|row| {
            let (key, workflow_id, acquired_at, updated_at) = row?;
            let workflow_id = parse_id(workflow_id)?;
            Ok(ResourceClaim {
                resource: ResourceKey::Autofactory(key),
                workflow_id,
                acquired_at,
                updated_at,
            })
        })
        .collect()
    }

    /// Removes claims whose owner is missing or terminal after a restart.
    pub fn reconcile_claims(&self) -> Result<usize, RepositoryError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let released = transaction.execute(
            "DELETE FROM workflow_resource_claims
             WHERE workflow_id NOT IN (
                 SELECT id FROM workflow_instances
                 WHERE status NOT IN ('succeeded', 'failed', 'cancelled')
             )",
            [],
        )?;
        transaction.commit()?;
        Ok(released)
    }

    /// Replaces mutable workflow state if `expected_revision` is current.
    pub fn update<P: Serialize, R: Serialize>(
        &self,
        id: WorkflowId,
        expected_revision: u64,
        state: WorkflowState<P, R>,
    ) -> Result<WorkflowInstance, RepositoryError> {
        self.update_with_wait(id, expected_revision, state, None)
    }

    pub(crate) fn update_with_wait<P: Serialize, R: Serialize>(
        &self,
        id: WorkflowId,
        expected_revision: u64,
        state: WorkflowState<P, R>,
        wait_intent: Option<&crate::WaitIntent>,
    ) -> Result<WorkflowInstance, RepositoryError> {
        let checkpoint_json = serde_json::to_string(&state.checkpoint)?;
        let result_json = state
            .result
            .map(|result| serde_json::to_string(&result))
            .transpose()?;
        let wait_intent_json = wait_intent.map(serde_json::to_string).transpose()?;
        let expected_revision = i64::try_from(expected_revision)
            .map_err(|_| RepositoryError::RevisionOutOfRange(expected_revision))?;
        let now = now_millis()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = read_in(&transaction, id)?.ok_or(RepositoryError::NotFound(id))?;
        if current.revision != expected_revision as u64 {
            return Err(RepositoryError::ConcurrentUpdate {
                id,
                expected: expected_revision as u64,
            });
        }
        if !current.status.can_transition_to(state.status) {
            return Err(RepositoryError::InvalidTransition {
                from: current.status,
                to: state.status,
            });
        }
        transaction.execute(
            "UPDATE workflow_instances SET
                status = ?1, current_step = ?2, checkpoint_json = ?3,
                last_error = ?4, result_json = ?5, updated_at = ?6,
                revision = revision + 1, wait_intent_json = ?9
             WHERE id = ?7 AND revision = ?8",
            params![
                state.status.as_str(),
                state.current_step,
                checkpoint_json,
                state.last_error,
                result_json,
                now,
                id.to_string(),
                expected_revision,
                wait_intent_json,
            ],
        )?;
        let updated = read_in(&transaction, id)?.ok_or(RepositoryError::NotFound(id))?;
        transaction.commit()?;
        Ok(updated)
    }

    /// Atomically replaces an old workflow payload with its explicitly migrated form.
    pub(crate) fn migrate_workflow(
        &self,
        instance: &WorkflowInstance,
        target_version: u32,
        migration: crate::WorkflowMigration,
    ) -> Result<WorkflowInstance, RepositoryError> {
        if target_version <= instance.schema_version {
            return Err(RepositoryError::InvalidWorkflowSchemaVersion);
        }
        let revision = i64::try_from(instance.revision)
            .map_err(|_| RepositoryError::RevisionOutOfRange(instance.revision))?;
        let config = serde_json::to_string(&migration.config)?;
        let checkpoint = serde_json::to_string(&migration.checkpoint)?;
        let now = now_millis()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE workflow_instances SET schema_version = ?1, config_json = ?2,
                checkpoint_json = ?3, updated_at = ?4, revision = revision + 1
             WHERE id = ?5 AND revision = ?6 AND schema_version = ?7",
            params![
                target_version,
                config,
                checkpoint,
                now,
                instance.id.to_string(),
                revision,
                instance.schema_version,
            ],
        )?;
        if changed == 0 {
            return Err(RepositoryError::ConcurrentUpdate {
                id: instance.id,
                expected: instance.revision,
            });
        }
        let updated =
            read_in(&transaction, instance.id)?.ok_or(RepositoryError::NotFound(instance.id))?;
        transaction.commit()?;
        Ok(updated)
    }
}

const COLUMNS: &str = "id, kind, schema_version, config_json, checkpoint_json, status, \
                       current_step, created_at, updated_at, last_error, result_json, \
                       parent_id, revision, wait_intent_json";

const SUMMARY_COLUMNS: &str = "id, kind, status, revision, current_step, updated_at";

const TRIGGER_COLUMNS: &str = "id, name, condition_json, target_json, enabled, created_at, \
                               updated_at, last_fired_at, next_run_at, last_error, \
                               event_cursor, revision";

fn read_trigger_in(
    connection: &Connection,
    id: TriggerId,
) -> Result<Option<AutomationTrigger>, RepositoryError> {
    connection
        .query_row(
            &format!("SELECT {TRIGGER_COLUMNS} FROM automation_triggers WHERE id = ?1"),
            [id.to_string()],
            row_to_trigger,
        )
        .optional()
        .map_err(Into::into)
}

fn row_to_trigger(row: &rusqlite::Row<'_>) -> Result<AutomationTrigger, rusqlite::Error> {
    let stored_id = row.get::<_, String>(0)?;
    let revision = row.get::<_, i64>(11)?;
    Ok(AutomationTrigger {
        id: TriggerId::from_str(&stored_id).map_err(|_| {
            to_sql_conversion_error(RepositoryError::InvalidStoredTriggerId(stored_id))
        })?,
        name: row.get(1)?,
        condition: serde_json::from_str(&row.get::<_, String>(2)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                2,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        target: serde_json::from_str(&row.get::<_, String>(3)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                3,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        enabled: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
        last_fired_at: row.get(7)?,
        next_run_at: row.get(8)?,
        last_error: row.get(9)?,
        event_cursor: row.get(10)?,
        revision: u64::try_from(revision).map_err(|_| {
            to_sql_conversion_error(RepositoryError::InvalidStoredRevision(revision))
        })?,
    })
}

fn read_in(
    connection: &Connection,
    id: WorkflowId,
) -> Result<Option<WorkflowInstance>, RepositoryError> {
    connection
        .query_row(
            &format!("SELECT {COLUMNS} FROM workflow_instances WHERE id = ?1"),
            [id.to_string()],
            row_to_instance,
        )
        .optional()
        .map_err(Into::into)
}

fn row_to_instance(row: &rusqlite::Row<'_>) -> Result<WorkflowInstance, rusqlite::Error> {
    let id: String = row.get(0)?;
    let kind: String = row.get(1)?;
    let status: String = row.get(5)?;
    let parent_id: Option<String> = row.get(11)?;
    let revision: i64 = row.get(12)?;
    Ok(WorkflowInstance {
        id: parse_id(id)?,
        kind: WorkflowKind::new(kind).map_err(to_sql_conversion_error)?,
        schema_version: row.get(2)?,
        config_json: row.get(3)?,
        checkpoint_json: row.get(4)?,
        status: WorkflowStatus::from_str(&status).map_err(to_sql_conversion_error)?,
        current_step: row.get(6)?,
        created_at: row.get(7)?,
        updated_at: row.get(8)?,
        last_error: row.get(9)?,
        result_json: row.get(10)?,
        parent_id: parent_id.map(parse_id).transpose()?,
        revision: u64::try_from(revision).map_err(|_| {
            to_sql_conversion_error(RepositoryError::InvalidStoredRevision(revision))
        })?,
        wait_intent_json: row.get(13)?,
    })
}

fn row_to_summary(row: &rusqlite::Row<'_>) -> Result<WorkflowSummary, rusqlite::Error> {
    let id = row.get::<_, String>(0)?;
    let kind = row.get::<_, String>(1)?;
    let status = row.get::<_, String>(2)?;
    let revision = row.get::<_, i64>(3)?;
    Ok(WorkflowSummary {
        id: parse_id(id)?,
        kind: WorkflowKind::new(kind).map_err(to_sql_conversion_error)?,
        status: WorkflowStatus::from_str(&status).map_err(to_sql_conversion_error)?,
        revision: u64::try_from(revision).map_err(|_| {
            to_sql_conversion_error(RepositoryError::InvalidStoredRevision(revision))
        })?,
        current_step: row.get(4)?,
        updated_at: row.get(5)?,
    })
}

fn parse_id(value: String) -> Result<WorkflowId, rusqlite::Error> {
    WorkflowId::from_str(&value)
        .map_err(|_| to_sql_conversion_error(RepositoryError::InvalidStoredId(value)))
}

fn resource_key(namespace: String, key: String) -> ResourceKey {
    match namespace.as_str() {
        "replicant" => ResourceKey::Replicant(key),
        "device" => ResourceKey::Device(key),
        "autofactory" => ResourceKey::Autofactory(key),
        namespace => ResourceKey::Namespaced {
            namespace: namespace
                .strip_prefix("custom:")
                .unwrap_or(namespace)
                .to_owned(),
            key,
        },
    }
}

fn to_sql_conversion_error(error: RepositoryError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn invalid_stored_execution(value: &str) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(
        0,
        rusqlite::types::Type::Text,
        format!("invalid persisted finite execution value {value:?}").into(),
    )
}

fn validate_document_identity(namespace: &str, key: &str) -> Result<(), RepositoryError> {
    let valid_namespace = !namespace.is_empty()
        && namespace.len() <= 128
        && namespace.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        });
    if !valid_namespace || key.is_empty() || key.len() > 256 {
        return Err(RepositoryError::InvalidKind(format!("{namespace}:{key}")));
    }
    Ok(())
}

fn now_millis() -> Result<i64, RepositoryError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryError::Clock)?
        .as_millis();
    i64::try_from(millis).map_err(|_| RepositoryError::Clock)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filtered_queries_use_existing_indexes_and_skip_terminal_payloads() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let parent = WorkflowId::new();
        {
            let mut connection = repository.connection().expect("connection");
            let transaction = connection.transaction().expect("transaction");
            transaction
                .execute(
                    "INSERT INTO workflow_instances
                     (id, kind, schema_version, config_json, checkpoint_json, status,
                      current_step, created_at, updated_at, parent_id)
                     VALUES (?1, 'test.active', 1, '{}', '{}', 'queued', NULL, 0, 0, NULL)",
                    [parent.to_string()],
                )
                .expect("insert active workflow");
            for created_at in 1..=3_000_i64 {
                transaction
                    .execute(
                        "INSERT INTO workflow_instances
                         (id, kind, schema_version, config_json, checkpoint_json, status,
                          current_step, created_at, updated_at, parent_id)
                         VALUES (?1, 'test.terminal', 1, '{}', '{}', 'succeeded',
                                 NULL, ?2, ?2, ?3)",
                        params![
                            WorkflowId::new().to_string(),
                            created_at,
                            parent.to_string()
                        ],
                    )
                    .expect("insert terminal workflow");
            }
            transaction.commit().expect("commit fixtures");
        }

        assert_eq!(repository.list().expect("all workflows").len(), 3_001);
        assert_eq!(repository.list_active().expect("active workflows").len(), 1);

        let connection = repository.connection().expect("connection");
        let plans = [
            (
                format!(
                    "EXPLAIN QUERY PLAN SELECT {COLUMNS} FROM workflow_instances
                     WHERE status IN ('queued', 'running', 'waiting', 'reconciling', 'paused')
                     ORDER BY created_at, id"
                ),
                "workflow_instances_status_idx",
            ),
            (
                format!(
                    "EXPLAIN QUERY PLAN SELECT {COLUMNS} FROM workflow_instances
                     WHERE parent_id = '{}' ORDER BY created_at, id",
                    parent
                ),
                "workflow_instances_parent_idx",
            ),
        ];
        for (query, index) in plans {
            let mut statement = connection.prepare(&query).expect("query plan");
            let details = statement
                .query_map([], |row| row.get::<_, String>(3))
                .expect("plan rows")
                .collect::<Result<Vec<_>, _>>()
                .expect("plan details");
            assert!(
                details.iter().any(|detail| detail.contains(index)),
                "{index} missing from {details:?}"
            );
        }
    }
}
