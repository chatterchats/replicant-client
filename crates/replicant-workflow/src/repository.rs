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
    AutomationTrigger, ClaimAcquireOutcome, FiniteExecution, FiniteExecutionClass,
    FiniteExecutionStatus, NewTrigger, NewWorkflow, ResourceClaim, ResourceKey, TriggerId,
    TriggerState, WorkflowActivity, WorkflowId, WorkflowInstance, WorkflowKind, WorkflowState,
    WorkflowStatus,
};

const INITIAL_SCHEMA: &str = include_str!("../migrations/0001_initial.sql");
const ACTIVITY_SCHEMA: &str = include_str!("../migrations/0002_activity.sql");
const RESOURCE_CLAIMS_SCHEMA: &str = include_str!("../migrations/0003_resource_claims.sql");
const WAIT_INTENT_SCHEMA: &str = include_str!("../migrations/0004_wait_intent.sql");
const FINITE_EXECUTION_SCHEMA: &str =
    include_str!("../migrations/0005_finite_execution_history.sql");
const AUTOMATION_TRIGGER_SCHEMA: &str = include_str!("../migrations/0006_automation_triggers.sql");
const CURRENT_DATABASE_SCHEMA: i64 = 6;

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
        Self::from_connection(connection)
    }

    /// Creates an isolated in-memory runtime database.
    pub fn open_in_memory() -> Result<Self, RepositoryError> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(connection: Connection) -> Result<Self, RepositoryError> {
        connection.busy_timeout(std::time::Duration::from_secs(5))?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
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
        transaction.commit()?;
        Ok(())
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
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
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
}

const COLUMNS: &str = "id, kind, schema_version, config_json, checkpoint_json, status, \
                       current_step, created_at, updated_at, last_error, result_json, \
                       parent_id, revision, wait_intent_json";

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

fn now_millis() -> Result<i64, RepositoryError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryError::Clock)?
        .as_millis();
    i64::try_from(millis).map_err(|_| RepositoryError::Clock)
}
