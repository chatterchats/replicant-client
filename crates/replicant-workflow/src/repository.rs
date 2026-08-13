use std::{
    path::Path,
    str::FromStr,
    sync::{Mutex, MutexGuard},
    time::{SystemTime, UNIX_EPOCH},
};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;

use crate::{
    NewWorkflow, WorkflowActivity, WorkflowId, WorkflowInstance, WorkflowKind, WorkflowState,
    WorkflowStatus,
};

const INITIAL_SCHEMA: &str = include_str!("../migrations/0001_initial.sql");
const ACTIVITY_SCHEMA: &str = include_str!("../migrations/0002_activity.sql");
const CURRENT_DATABASE_SCHEMA: i64 = 2;

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
    /// A malformed lifecycle status was found in SQLite.
    #[error("invalid persisted workflow status {0:?}")]
    InvalidStoredStatus(String),
    /// A malformed workflow ID was found in SQLite.
    #[error("invalid persisted workflow ID {0:?}")]
    InvalidStoredId(String),
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
        transaction.commit()?;
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

    /// Replaces mutable workflow state if `expected_revision` is current.
    pub fn update<P: Serialize, R: Serialize>(
        &self,
        id: WorkflowId,
        expected_revision: u64,
        state: WorkflowState<P, R>,
    ) -> Result<WorkflowInstance, RepositoryError> {
        let checkpoint_json = serde_json::to_string(&state.checkpoint)?;
        let result_json = state
            .result
            .map(|result| serde_json::to_string(&result))
            .transpose()?;
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
                revision = revision + 1
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
            ],
        )?;
        let updated = read_in(&transaction, id)?.ok_or(RepositoryError::NotFound(id))?;
        transaction.commit()?;
        Ok(updated)
    }
}

const COLUMNS: &str = "id, kind, schema_version, config_json, checkpoint_json, status, \
                       current_step, created_at, updated_at, last_error, result_json, \
                       parent_id, revision";

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
    })
}

fn parse_id(value: String) -> Result<WorkflowId, rusqlite::Error> {
    WorkflowId::from_str(&value)
        .map_err(|_| to_sql_conversion_error(RepositoryError::InvalidStoredId(value)))
}

fn to_sql_conversion_error(error: RepositoryError) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

fn now_millis() -> Result<i64, RepositoryError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RepositoryError::Clock)?
        .as_millis();
    i64::try_from(millis).map_err(|_| RepositoryError::Clock)
}
