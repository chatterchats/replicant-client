use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    ops::{Deref, DerefMut},
    path::Path,
    str::FromStr,
    sync::{Mutex, MutexGuard},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use crate::{
    AllocationCandidate, AllocationId, AllocationSet, AllocationState, AutomationPolicy,
    AutomationTrigger, CampaignCounts, CampaignItemResult, CampaignOutcome, CampaignResult,
    ClaimAcquireOutcome, FiniteExecution, FiniteExecutionClass, FiniteExecutionStatus, NewTrigger,
    NewWorkflow, ReplacementOutcome, RequirementScope, ResourceAllocation, ResourceClaim,
    ResourceKey, ResourceRequirement, TriggerId, TriggerState, WorkItem, WorkItemAttempt,
    WorkItemAttemptOutcome, WorkItemId, WorkItemSpec, WorkItemState, WorkItemStatus,
    WorkItemTransition, WorkflowActivity, WorkflowFailureDisposition, WorkflowId, WorkflowInstance,
    WorkflowKind, WorkflowState, WorkflowStatus, WorkflowSummary,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

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
const FINITE_EXECUTION_CANCELLED_SCHEMA: &str =
    include_str!("../migrations/0010_finite_execution_cancelled.sql");
const WORKFLOW_FAILURE_DISPOSITION_SCHEMA: &str =
    include_str!("../migrations/0011_workflow_failure_disposition.sql");
const WORK_ITEMS_SCHEMA: &str = include_str!("../migrations/0012_work_items.sql");
const RESOURCE_ALLOCATIONS_SCHEMA: &str =
    include_str!("../migrations/0013_resource_allocations.sql");
const CURRENT_DATABASE_SCHEMA: i64 = 13;

/// Runtime workflow persistence failures.
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    /// SQLite operation failed.
    #[error("SQLite failure: {0}")]
    Sql(#[from] rusqlite::Error),
    /// Database directory creation failed.
    #[error("database directory failure: {0}")]
    Io(#[from] std::io::Error),
    /// Typed payload serialization or deserialization failed.
    #[error("workflow payload serialization failure: {0}")]
    Serialization(#[from] serde_json::Error),
    /// A transactional active-workflow compatibility check could not prove safe reuse.
    #[error("workflow compatibility check failed: {0}")]
    Compatibility(String),
    /// A stable workflow kind was malformed.
    #[error("invalid workflow kind {0:?}")]
    InvalidKind(String),
    /// A resource namespace or identity was malformed.
    #[error("invalid resource key {0:?}")]
    InvalidResourceKey(ResourceKey),
    /// A malformed lifecycle status was found in SQLite.
    #[error("invalid persisted workflow status {0:?}")]
    InvalidStoredStatus(String),
    /// A malformed workflow failure disposition was found in SQLite.
    #[error("invalid persisted workflow failure disposition {0:?}")]
    InvalidStoredFailureDisposition(String),
    /// A malformed workflow ID was found in SQLite.
    #[error("invalid persisted workflow ID {0:?}")]
    InvalidStoredId(String),
    /// A malformed trigger ID was found in SQLite.
    #[error("invalid persisted trigger ID {0:?}")]
    InvalidStoredTriggerId(String),
    /// A malformed negative revision was found in SQLite.
    #[error("invalid persisted workflow revision {0}")]
    InvalidStoredRevision(i64),
    /// A malformed work-item ID was found in SQLite.
    #[error("invalid persisted work item ID {0:?}")]
    InvalidStoredWorkItemId(String),
    /// A malformed work-item lifecycle status was found in SQLite.
    #[error("invalid persisted work item status {0:?}")]
    InvalidStoredWorkItemStatus(String),
    /// A malformed work-item attempt outcome was found in SQLite.
    #[error("invalid persisted work item attempt outcome {0:?}")]
    InvalidStoredWorkItemAttemptOutcome(String),
    /// A malformed negative work-item revision was found in SQLite.
    #[error("invalid persisted work item revision {0}")]
    InvalidStoredWorkItemRevision(i64),
    /// A malformed negative or overflowing work-item count was found in SQLite.
    #[error("invalid persisted work item {field} count {value}")]
    InvalidStoredWorkItemCount {
        /// Name of the malformed count column.
        field: &'static str,
        /// Signed value read from SQLite.
        value: i64,
    },
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
    /// No work item has the requested ID.
    #[error("work item {0} was not found")]
    WorkItemNotFound(WorkItemId),
    /// A terminal workflow cannot own new or changed work items.
    #[error("terminal workflow {0} cannot own work items")]
    TerminalWorkItemOwner(WorkflowId),
    /// A campaign deduplication key was reused with a different immutable specification.
    #[error(
        "workflow {workflow_id} work item {dedupe_key:?} conflicts with its stored specification"
    )]
    WorkItemSpecConflict {
        /// Owning workflow.
        workflow_id: WorkflowId,
        /// Conflicting campaign-local deduplication key.
        dedupe_key: String,
    },
    /// Another writer updated this work item first.
    #[error("work item {id} revision changed; expected {expected}")]
    ConcurrentWorkItemUpdate {
        /// Work-item ID.
        id: WorkItemId,
        /// Revision supplied by the caller.
        expected: u64,
    },
    /// The requested work-item lifecycle transition is not valid.
    #[error("invalid work item transition from {from:?} to {to:?}")]
    InvalidWorkItemTransition {
        /// Persisted status.
        from: WorkItemStatus,
        /// Requested status.
        to: WorkItemStatus,
    },
    /// Available candidate capacity cannot satisfy one requirement.
    #[error(
        "resource requirement {requirement_key:?} cannot be fully allocated ({missing_count} missing){details}"
    )]
    AllocationShortage {
        /// Stable requirement key.
        requirement_key: String,
        /// Number of additional pool members required.
        missing_count: u32,
        /// Human-readable explanation of why discovered candidates were rejected.
        details: String,
    },
    /// A work-item assignment identifier was empty.
    #[error("invalid work item assignment: {0}")]
    InvalidWorkItemAssignment(&'static str),
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

/// Result of atomically finding or creating an active workflow.
///
/// The returned [`WorkflowInstance`] is either the oldest active instance for
/// which the caller's compatibility predicate returned `true`, or the newly
/// inserted queued workflow. [`Self::created`] distinguishes these cases.
pub struct CreateOrReuseWorkflow {
    /// The existing compatible or newly created workflow instance.
    pub instance: WorkflowInstance,
    /// Whether `instance` was inserted by this operation.
    pub created: bool,
}

struct ConnectionGuard<'a> {
    connection: MutexGuard<'a, Connection>,
    acquired_at: Instant,
    wait_micros: u64,
    span: tracing::Span,
}

impl Deref for ConnectionGuard<'_> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl DerefMut for ConnectionGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

impl Drop for ConnectionGuard<'_> {
    fn drop(&mut self) {
        let held_micros = u64::try_from(self.acquired_at.elapsed().as_micros()).unwrap_or(u64::MAX);
        self.span.in_scope(|| {
            tracing::debug!(
                event = "workflow.repository.connection_hold_complete",
                wait_micros = self.wait_micros,
                held_micros,
                "workflow repository connection released"
            );
            if self.wait_micros >= 1_000_000 || held_micros >= 1_000_000 {
                tracing::warn!(
                    event = "workflow.repository.connection_slow",
                    wait_micros = self.wait_micros,
                    held_micros,
                    "workflow repository connection exceeded responsiveness threshold"
                );
            }
        });
    }
}

impl WorkflowRepository {
    /// Opens or creates a runtime database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, RepositoryError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
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

    fn connection(&self) -> Result<ConnectionGuard<'_>, RepositoryError> {
        let waiting_at = Instant::now();
        let span = tracing::Span::current();
        let connection = self
            .connection
            .lock()
            .map_err(|_| RepositoryError::LockPoisoned)?;
        let wait_micros = u64::try_from(waiting_at.elapsed().as_micros()).unwrap_or(u64::MAX);
        span.in_scope(|| {
            tracing::debug!(
                event = "workflow.repository.connection_wait",
                wait_micros,
                "workflow repository connection acquired"
            );
        });
        Ok(ConnectionGuard {
            connection,
            acquired_at: Instant::now(),
            wait_micros,
            span,
        })
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
        if found < 10 {
            transaction.execute_batch(FINITE_EXECUTION_CANCELLED_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (10)",
                [],
            )?;
        }
        if found < 11 {
            transaction.execute_batch(WORKFLOW_FAILURE_DISPOSITION_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (11)",
                [],
            )?;
        }
        if found < 12 {
            transaction.execute_batch(WORK_ITEMS_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (12)",
                [],
            )?;
        }
        if found < 13 {
            transaction.execute_batch(RESOURCE_ALLOCATIONS_SCHEMA)?;
            transaction.execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (13)",
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
        let config_json = serde_json::to_string(&workflow.config)?;
        let checkpoint_json = serde_json::to_string(&workflow.checkpoint)?;
        let id = WorkflowId::new();
        let now = now_millis()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let instance = insert_workflow_in(
            &transaction,
            &workflow,
            id,
            now,
            config_json,
            checkpoint_json,
        )?;
        transaction.commit()?;
        Ok(instance)
    }

    /// Atomically reuses the oldest compatible active workflow or creates one.
    ///
    /// The proposed workflow is validated and serialized before an immediate
    /// transaction takes SQLite's write lock. Active rows are visited in
    /// `(created_at, id)` order, and the first row accepted by `compatible` is
    /// returned. If no row matches, the proposed workflow is inserted as
    /// queued work and returned.
    pub fn create_or_reuse_active<C, P, F>(
        &self,
        workflow: NewWorkflow<C, P>,
        mut compatible: F,
    ) -> Result<CreateOrReuseWorkflow, RepositoryError>
    where
        C: Serialize,
        P: Serialize,
        F: FnMut(&WorkflowInstance) -> Result<bool, RepositoryError>,
    {
        if workflow.schema_version == 0 {
            return Err(RepositoryError::InvalidWorkflowSchemaVersion);
        }
        let config_json = serde_json::to_string(&workflow.config)?;
        let checkpoint_json = serde_json::to_string(&workflow.checkpoint)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let id = WorkflowId::new();
        let now = now_millis()?;
        let active = {
            let mut statement = transaction.prepare(&format!(
                "SELECT {COLUMNS} FROM workflow_instances
                 WHERE status IN ('queued', 'running', 'waiting', 'reconciling', 'paused')
                 ORDER BY created_at, id"
            ))?;
            statement
                .query_map([], row_to_instance)?
                .collect::<Result<Vec<_>, _>>()?
        };
        for instance in active {
            if compatible(&instance)? {
                transaction.commit()?;
                return Ok(CreateOrReuseWorkflow {
                    instance,
                    created: false,
                });
            }
        }
        let instance = insert_workflow_in(
            &transaction,
            &workflow,
            id,
            now,
            config_json,
            checkpoint_json,
        )?;
        transaction.commit()?;
        Ok(CreateOrReuseWorkflow {
            instance,
            created: true,
        })
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
                "cancelled" => FiniteExecutionStatus::Cancelled,
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

    /// Reconciles the immutable desired work-item set for one campaign.
    pub fn reconcile_work_items(
        &self,
        workflow_id: WorkflowId,
        desired: &[WorkItemSpec],
        now_ms: i64,
    ) -> Result<Vec<WorkItem>, RepositoryError> {
        let mut desired_by_key = BTreeMap::new();
        for spec in desired {
            let conflict = spec.workflow_id != workflow_id
                || desired_by_key
                    .get(&spec.dedupe_key)
                    .is_some_and(|existing| *existing != *spec);
            if conflict {
                return Err(RepositoryError::WorkItemSpecConflict {
                    workflow_id,
                    dedupe_key: spec.dedupe_key.clone(),
                });
            }
            desired_by_key
                .entry(spec.dedupe_key.clone())
                .or_insert_with(|| spec.clone());
        }

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let owner =
            read_in(&transaction, workflow_id)?.ok_or(RepositoryError::NotFound(workflow_id))?;
        if owner.status.is_terminal() {
            return Err(RepositoryError::TerminalWorkItemOwner(workflow_id));
        }

        for spec in desired_by_key.values() {
            let existing = read_work_item_by_key_in(&transaction, workflow_id, &spec.dedupe_key)?;
            if let Some(existing) = existing {
                if existing.spec != *spec {
                    return Err(RepositoryError::WorkItemSpecConflict {
                        workflow_id,
                        dedupe_key: spec.dedupe_key.clone(),
                    });
                }
                continue;
            }
            let id = WorkItemId::new();
            transaction.execute(
                "INSERT INTO workflow_work_items (
                    id, workflow_id, dedupe_key, kind, sort_key, payload_json,
                    preconditions_json, requirements_json, deadline_at_ms, status,
                    created_at_ms, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, 'pending', ?10, ?10)",
                params![
                    id.to_string(),
                    workflow_id.to_string(),
                    spec.dedupe_key,
                    spec.kind.as_str(),
                    spec.sort_key,
                    serde_json::to_string(&spec.payload_json)?,
                    serde_json::to_string(&spec.preconditions_json)?,
                    serde_json::to_string(&spec.requirements_json)?,
                    spec.deadline_at_ms,
                    now_ms,
                ],
            )?;
        }
        let items = list_work_items_in(&transaction, workflow_id)?;
        transaction.commit()?;
        Ok(items)
    }

    /// Reads one persisted work item.
    pub fn read_work_item(&self, id: WorkItemId) -> Result<Option<WorkItem>, RepositoryError> {
        let connection = self.connection()?;
        read_work_item_in(&connection, id)
    }

    /// Lists one campaign's work items in deterministic scheduler order.
    pub fn list_work_items(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<Vec<WorkItem>, RepositoryError> {
        let connection = self.connection()?;
        list_work_items_in(&connection, workflow_id)
    }

    /// Lists work items for several workflows with one deterministic query.
    ///
    /// The result is grouped by owning workflow, avoiding one SQLite query per
    /// workflow during placement-intent projection.
    pub fn list_work_items_for_workflows(
        &self,
        workflow_ids: &[WorkflowId],
    ) -> Result<BTreeMap<WorkflowId, Vec<WorkItem>>, RepositoryError> {
        let mut grouped = workflow_ids
            .iter()
            .copied()
            .map(|id| (id, Vec::new()))
            .collect::<BTreeMap<_, _>>();
        if workflow_ids.is_empty() {
            return Ok(grouped);
        }
        let connection = self.connection()?;
        let placeholders = std::iter::repeat_n("?", workflow_ids.len())
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {WORK_ITEM_COLUMNS} FROM workflow_work_items \
             WHERE workflow_id IN ({placeholders}) \
             ORDER BY workflow_id, sort_key, id"
        );
        let mut statement = connection.prepare(&sql)?;
        let params = workflow_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let rows = statement.query_map(rusqlite::params_from_iter(params), row_to_work_item)?;
        for row in rows {
            let item = row?;
            grouped.entry(item.spec.workflow_id).or_default().push(item);
        }
        Ok(grouped)
    }

    /// Lists all execution attempts for one work item by ordinal.
    pub fn list_work_item_attempts(
        &self,
        item_id: WorkItemId,
    ) -> Result<Vec<WorkItemAttempt>, RepositoryError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(&format!(
            "SELECT {WORK_ITEM_ATTEMPT_COLUMNS}
             FROM workflow_work_item_attempts
             WHERE item_id = ?1
             ORDER BY attempt_ordinal"
        ))?;
        let rows = statement.query_map([item_id.to_string()], row_to_attempt)?;
        rows.map(|row| row.map_err(Into::into)).collect()
    }

    /// Atomically claims the first eligible pending or due waiting work item.
    pub fn claim_next_work_item(
        &self,
        workflow_id: WorkflowId,
        now_ms: i64,
    ) -> Result<Option<WorkItem>, RepositoryError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let owner =
            read_in(&transaction, workflow_id)?.ok_or(RepositoryError::NotFound(workflow_id))?;
        if owner.status.is_terminal() {
            return Err(RepositoryError::TerminalWorkItemOwner(workflow_id));
        }
        let id = transaction
            .query_row(
                "SELECT id FROM workflow_work_items
                 WHERE workflow_id = ?1
                   AND (
                     (status = 'pending'
                       AND (next_attempt_at_ms IS NULL OR next_attempt_at_ms <= ?2))
                     OR (status = 'waiting'
                       AND next_attempt_at_ms IS NOT NULL
                       AND next_attempt_at_ms <= ?2)
                   )
                   AND NOT EXISTS (
                     SELECT 1
                     FROM json_each(workflow_work_items.preconditions_json) AS dependency
                     WHERE json_extract(dependency.value, '$.kind') = 'work_item.succeeded'
                       AND NOT EXISTS (
                         SELECT 1
                         FROM workflow_work_items AS prerequisite
                         WHERE prerequisite.workflow_id = workflow_work_items.workflow_id
                           AND prerequisite.dedupe_key =
                               json_extract(dependency.value, '$.parameters.dedupe_key')
                           AND prerequisite.status IN ('succeeded', 'skipped')
                       )
                   )
                 ORDER BY sort_key, dedupe_key, id
                 LIMIT 1",
                params![workflow_id.to_string(), now_ms],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        let Some(id) = id else {
            transaction.commit()?;
            return Ok(None);
        };
        let id = parse_work_item_id(id)?;
        transaction.execute(
            "UPDATE workflow_work_items
             SET status = 'assigned', next_attempt_at_ms = NULL,
                 updated_at_ms = ?2, revision = revision + 1
             WHERE id = ?1",
            params![id.to_string(), now_ms],
        )?;
        let claimed =
            read_work_item_in(&transaction, id)?.ok_or(RepositoryError::WorkItemNotFound(id))?;
        transaction.commit()?;
        Ok(Some(claimed))
    }

    /// Starts an assigned work item and opens its next attempt interval.
    pub fn start_work_item(
        &self,
        id: WorkItemId,
        expected_revision: u64,
        worker_identity: &str,
        assignment_id: &str,
        started_at_ms: i64,
    ) -> Result<WorkItem, RepositoryError> {
        if assignment_id.is_empty() {
            return Err(RepositoryError::InvalidWorkItemAssignment(
                "assignment_id must not be empty",
            ));
        }
        if worker_identity.is_empty() {
            return Err(RepositoryError::InvalidWorkItemAssignment(
                "worker_identity must not be empty",
            ));
        }
        let expected = work_item_revision_to_sql(expected_revision)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current =
            read_work_item_in(&transaction, id)?.ok_or(RepositoryError::WorkItemNotFound(id))?;
        verify_work_item_revision(&current, expected_revision)?;
        if current.state.status != WorkItemStatus::Assigned {
            return Err(RepositoryError::InvalidWorkItemTransition {
                from: current.state.status,
                to: WorkItemStatus::Running,
            });
        }
        let attempt_ordinal = current.state.attempt_count.checked_add(1).ok_or(
            RepositoryError::InvalidStoredWorkItemCount {
                field: "attempt",
                value: i64::from(current.state.attempt_count),
            },
        )?;
        transaction.execute(
            "INSERT INTO workflow_work_item_attempts (
                item_id, attempt_ordinal, assignment_id, worker_identity, started_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                id.to_string(),
                i64::from(attempt_ordinal),
                assignment_id,
                worker_identity,
                started_at_ms,
            ],
        )?;
        let changed = transaction.execute(
            "UPDATE workflow_work_items
             SET status = 'running', attempt_count = ?1, ever_started = 1,
                 updated_at_ms = ?2, revision = revision + 1
             WHERE id = ?3 AND revision = ?4",
            params![
                i64::from(attempt_ordinal),
                started_at_ms,
                id.to_string(),
                expected
            ],
        )?;
        if changed == 0 {
            return Err(RepositoryError::ConcurrentWorkItemUpdate {
                id,
                expected: expected_revision,
            });
        }
        let started =
            read_work_item_in(&transaction, id)?.ok_or(RepositoryError::WorkItemNotFound(id))?;
        transaction.commit()?;
        Ok(started)
    }

    /// Applies one optimistic, atomic work-item state transition.
    pub fn transition_work_item(
        &self,
        id: WorkItemId,
        expected_revision: u64,
        transition: WorkItemTransition,
        now_ms: i64,
    ) -> Result<WorkItem, RepositoryError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current =
            read_work_item_in(&transaction, id)?.ok_or(RepositoryError::WorkItemNotFound(id))?;
        verify_work_item_revision(&current, expected_revision)?;
        transition_work_item_in(&transaction, &current, transition, now_ms)?;
        let updated =
            read_work_item_in(&transaction, id)?.ok_or(RepositoryError::WorkItemNotFound(id))?;
        transaction.commit()?;
        Ok(updated)
    }

    /// Aggregates a terminal campaign result, or returns `None` while work remains.
    pub fn aggregate_campaign_result(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<Option<CampaignResult>, RepositoryError> {
        let items = self.list_work_items(workflow_id)?;
        if items.iter().any(|item| !item.state.status.is_terminal()) {
            return Ok(None);
        }
        let mut counts = CampaignCounts {
            total: u32::try_from(items.len()).map_err(|_| {
                RepositoryError::InvalidStoredWorkItemCount {
                    field: "total",
                    value: i64::MAX,
                }
            })?,
            ..CampaignCounts::default()
        };
        for item in &items {
            match item.state.status {
                WorkItemStatus::Pending => counts.pending += 1,
                WorkItemStatus::Assigned => counts.assigned += 1,
                WorkItemStatus::Running => counts.running += 1,
                WorkItemStatus::Waiting => counts.waiting += 1,
                WorkItemStatus::Succeeded => counts.succeeded += 1,
                WorkItemStatus::Skipped => counts.skipped += 1,
                WorkItemStatus::Failed => counts.failed += 1,
                WorkItemStatus::Abandoned => counts.abandoned += 1,
            }
        }
        let any_started = items.iter().any(|item| item.state.ever_started);
        let has_terminal_failure = counts.failed != 0 || counts.abandoned != 0;
        let outcome = if counts.succeeded != 0 && has_terminal_failure {
            CampaignOutcome::PartialSuccess
        } else if counts.succeeded != 0 {
            CampaignOutcome::AllSucceeded
        } else if !any_started {
            CampaignOutcome::NothingCouldStart
        } else {
            CampaignOutcome::NoSuccess
        };
        let item_results = items
            .into_iter()
            .map(|item| CampaignItemResult {
                item_id: item.id,
                dedupe_key: item.spec.dedupe_key,
                status: item.state.status,
                result_json: item.state.result_json,
                error: item.state.last_error,
            })
            .collect();
        Ok(Some(CampaignResult {
            outcome,
            counts,
            items: item_results,
        }))
    }

    /// Reclaims assigned or running items left orphaned by restart or resume.
    pub fn reconcile_orphaned_work_items(
        &self,
        workflow_id: Option<WorkflowId>,
        now_ms: i64,
    ) -> Result<usize, RepositoryError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let owner_id = workflow_id.map(|id| id.to_string());
        let rows = {
            let mut statement = transaction.prepare(
                "SELECT item.id, item.status
                 FROM workflow_work_items item
                 JOIN workflow_instances workflow ON workflow.id = item.workflow_id
                 WHERE item.status IN ('assigned', 'running')
                   AND (
                     workflow.status IN ('queued', 'running', 'waiting', 'reconciling')
                     OR (?1 IS NOT NULL AND workflow.status = 'paused')
                   )
                   AND (?1 IS NULL OR item.workflow_id = ?1)
                 ORDER BY item.id",
            )?;
            statement
                .query_map([owner_id], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        for (id, status) in &rows {
            let id = parse_work_item_id(id.clone())?;
            let status = parse_work_item_status(status.clone())?;
            if status == WorkItemStatus::Running {
                close_open_attempt(
                    &transaction,
                    id,
                    WorkItemAttemptOutcome::Reclaimed,
                    None,
                    now_ms,
                )?;
            }
            transaction.execute(
                "DELETE FROM workflow_resource_claims
                 WHERE EXISTS (
                   SELECT 1 FROM workflow_resource_allocations allocation
                   JOIN workflow_work_items item ON item.id = allocation.item_id
                   WHERE allocation.item_id = ?1
                     AND allocation.state = 'active'
                     AND item.workflow_id = workflow_resource_claims.workflow_id
                     AND allocation.resource_namespace =
                         workflow_resource_claims.resource_namespace
                     AND allocation.resource_key = workflow_resource_claims.resource_key
                 )",
                [id.to_string()],
            )?;
            transaction.execute(
                "UPDATE workflow_resource_allocations
                 SET state = 'released', updated_at_ms = ?2
                 WHERE item_id = ?1 AND state = 'active'",
                params![id.to_string(), now_ms],
            )?;
            transaction.execute(
                "UPDATE workflow_assignments
                 SET state = 'released', reclaim_requested_at_ms = NULL, updated_at_ms = ?2
                 WHERE item_id = ?1 AND state != 'released'",
                params![id.to_string(), now_ms],
            )?;
            transaction.execute(
                "UPDATE workflow_work_items
                 SET status = 'pending', next_attempt_at_ms = NULL,
                     updated_at_ms = ?2, revision = revision + 1
                 WHERE id = ?1",
                params![id.to_string(), now_ms],
            )?;
        }
        let reclaimed = rows.len();
        transaction.commit()?;
        Ok(reclaimed)
    }

    /// Atomically observes candidates and allocates every stored item requirement.
    pub fn allocate_requirements(
        &self,
        item_id: WorkItemId,
        expected_revision: u64,
        candidates: &[AllocationCandidate],
    ) -> Result<AllocationSet, RepositoryError> {
        self.allocate_requirements_with_affinity(item_id, expected_revision, candidates, &[])
    }

    /// Atomically allocates every stored item requirement while enforcing
    /// caller-supplied same-resource-key affinity between requirement pairs.
    ///
    /// Affinity is execution policy rather than persisted work-item schema so
    /// callers can strengthen allocation safety without invalidating durable
    /// work items created by older runtime versions.
    pub fn allocate_requirements_with_affinity(
        &self,
        item_id: WorkItemId,
        expected_revision: u64,
        candidates: &[AllocationCandidate],
        affinities: &[(&str, &str)],
    ) -> Result<AllocationSet, RepositoryError> {
        self.allocate_requirements_with_policy(
            item_id,
            expected_revision,
            candidates,
            affinities,
            &[],
        )
    }

    /// Atomically allocates stored requirements with runtime-only affinity and compatibility
    /// policy.
    ///
    /// Ignored keys remain persisted in immutable work-item specs but are not allocated. This is
    /// intended only for retiring requirements that older runtime versions persisted incorrectly.
    pub fn allocate_requirements_with_policy(
        &self,
        item_id: WorkItemId,
        expected_revision: u64,
        candidates: &[AllocationCandidate],
        affinities: &[(&str, &str)],
        ignored_requirement_keys: &[&str],
    ) -> Result<AllocationSet, RepositoryError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let item = read_work_item_in(&transaction, item_id)?
            .ok_or(RepositoryError::WorkItemNotFound(item_id))?;
        verify_work_item_revision(&item, expected_revision)?;
        let owner = read_in(&transaction, item.spec.workflow_id)?
            .ok_or(RepositoryError::NotFound(item.spec.workflow_id))?;
        if owner.status.is_terminal() || item.state.status.is_terminal() {
            return Err(RepositoryError::TerminalWorkItemOwner(
                item.spec.workflow_id,
            ));
        }
        let requirements: Vec<ResourceRequirement> =
            serde_json::from_value(item.spec.requirements_json.clone())?;
        let ignored_requirement_keys = ignored_requirement_keys
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut allocation_set = list_active_allocations_in(&transaction, item_id)?;
        for requirement_key in &ignored_requirement_keys {
            if let Some(allocations) = allocation_set.by_requirement.remove(*requirement_key) {
                for allocation in allocations {
                    release_active_allocation_in(
                        &transaction,
                        item.spec.workflow_id,
                        &allocation,
                        item.state.updated_at_ms,
                    )?;
                }
            }
        }
        let requirements = requirements
            .into_iter()
            .filter(|requirement| !ignored_requirement_keys.contains(requirement.key.as_str()))
            .collect::<Vec<_>>();

        let mut ordered_candidates = candidates.to_vec();
        ordered_candidates.sort_by(|left, right| {
            resource_sort_key(&left.resource).cmp(&resource_sort_key(&right.resource))
        });
        for candidate in &ordered_candidates {
            let pool_key = serde_json::to_string(&candidate.resource)?;
            let (namespace, key) = candidate.resource.persisted_parts()?;
            let revision = i64::try_from(candidate.observed_revision)
                .map_err(|_| RepositoryError::RevisionOutOfRange(candidate.observed_revision))?;
            let quantity = i64::try_from(candidate.available_quantity).map_err(|_| {
                RepositoryError::InvalidStoredWorkItemCount {
                    field: "available quantity",
                    value: i64::MAX,
                }
            })?;
            transaction.execute(
                "INSERT INTO workflow_resource_pools (
                    pool_key, resource_namespace, resource_key, kind, capabilities_json,
                    location_json, available_quantity, observed_revision, observed_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(pool_key) DO UPDATE SET
                    kind = excluded.kind,
                    capabilities_json = excluded.capabilities_json,
                    location_json = excluded.location_json,
                    available_quantity = excluded.available_quantity,
                    observed_revision = excluded.observed_revision,
                    observed_at_ms = excluded.observed_at_ms
                 WHERE excluded.observed_revision > workflow_resource_pools.observed_revision",
                params![
                    pool_key,
                    namespace,
                    key,
                    candidate.kind,
                    serde_json::to_string(&candidate.capabilities)?,
                    candidate
                        .location
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    quantity,
                    revision,
                    candidate.observed_at_ms,
                ],
            )?;
        }

        for requirement in &requirements {
            let mut selected = allocation_set
                .by_requirement
                .remove(&requirement.key)
                .unwrap_or_default();
            if affinities
                .iter()
                .any(|(_, parent)| *parent == requirement.key.as_str())
            {
                let mut retained = Vec::with_capacity(selected.len());
                for allocation in selected {
                    let still_usable = if let Some(candidate) = ordered_candidates
                        .iter()
                        .find(|candidate| candidate.resource == allocation.resource)
                    {
                        candidate_matches_requirement(candidate, requirement)
                            && candidate_supports_affined_dependents(
                                &transaction,
                                candidate,
                                requirement,
                                &requirements,
                                &ordered_candidates,
                                &allocation_set,
                                item.spec.workflow_id,
                                affinities,
                            )?
                    } else {
                        false
                    };
                    if still_usable {
                        retained.push(allocation);
                    } else {
                        release_active_allocation_in(
                            &transaction,
                            item.spec.workflow_id,
                            &allocation,
                            item.state.updated_at_ms,
                        )?;
                    }
                }
                selected = retained;
            }
            if let Some(parent_requirement) = affinity_parent(&requirement.key, affinities) {
                let allowed_keys = allocation_set
                    .by_requirement
                    .get(parent_requirement)
                    .into_iter()
                    .flatten()
                    .map(|allocation| resource_identity_key(&allocation.resource))
                    .collect::<BTreeSet<_>>();
                let mut retained = Vec::with_capacity(selected.len());
                for allocation in selected {
                    let candidate_is_current = ordered_candidates.iter().any(|candidate| {
                        candidate.resource == allocation.resource
                            && candidate_matches_requirement(candidate, requirement)
                    });
                    if allowed_keys.contains(resource_identity_key(&allocation.resource))
                        && candidate_is_current
                    {
                        retained.push(allocation);
                        continue;
                    }
                    release_active_allocation_in(
                        &transaction,
                        item.spec.workflow_id,
                        &allocation,
                        item.state.updated_at_ms,
                    )?;
                }
                selected = retained;
            }
            let mut diagnostics = AllocationShortageDiagnostics::default();
            for candidate in &ordered_candidates {
                if selected.len() >= usize::try_from(requirement.count).unwrap_or(usize::MAX) {
                    break;
                }
                if !candidate_matches_kind_capabilities(candidate, requirement) {
                    continue;
                }
                diagnostics.kind_matches = diagnostics.kind_matches.saturating_add(1);
                if !candidate_matches_scope(candidate, &requirement.scope) {
                    diagnostics.scope_rejected = diagnostics.scope_rejected.saturating_add(1);
                    continue;
                }
                diagnostics.scope_matches = diagnostics.scope_matches.saturating_add(1);
                if !candidate_matches_affinity(
                    candidate,
                    &requirement.key,
                    affinities,
                    &allocation_set,
                ) {
                    diagnostics.affinity_rejected = diagnostics.affinity_rejected.saturating_add(1);
                    continue;
                }
                if !candidate_supports_affined_dependents(
                    &transaction,
                    candidate,
                    requirement,
                    &requirements,
                    &ordered_candidates,
                    &allocation_set,
                    item.spec.workflow_id,
                    affinities,
                )? {
                    diagnostics.dependent_affinity_rejected =
                        diagnostics.dependent_affinity_rejected.saturating_add(1);
                    continue;
                }
                let pool_key = serde_json::to_string(&candidate.resource)?;
                let active_quantity: i64 = transaction.query_row(
                    "SELECT COALESCE(SUM(quantity), 0)
                     FROM workflow_resource_allocations
                     WHERE pool_key = ?1 AND state = 'active'",
                    [&pool_key],
                    |row| row.get(0),
                )?;
                let available_quantity: i64 = transaction.query_row(
                    "SELECT available_quantity FROM workflow_resource_pools WHERE pool_key = ?1",
                    [&pool_key],
                    |row| row.get(0),
                )?;
                let requested_quantity = i64::try_from(requirement.quantity).map_err(|_| {
                    RepositoryError::InvalidStoredWorkItemCount {
                        field: "required quantity",
                        value: i64::MAX,
                    }
                })?;
                if available_quantity.saturating_sub(active_quantity) < requested_quantity {
                    diagnostics.capacity_rejected = diagnostics.capacity_rejected.saturating_add(1);
                    continue;
                }
                let (namespace, key) = candidate.resource.persisted_parts()?;
                if namespace == "replicant" {
                    let has_active_assignment: bool = transaction.query_row(
                        "SELECT EXISTS (
                            SELECT 1 FROM workflow_assignments
                            WHERE worker_namespace = ?1
                              AND worker_key = ?2
                              AND state != 'released'
                         )",
                        params![namespace, key],
                        |row| row.get(0),
                    )?;
                    if has_active_assignment {
                        diagnostics.assignment_rejected =
                            diagnostics.assignment_rejected.saturating_add(1);
                        continue;
                    }
                }
                if is_exclusive_namespace(&namespace) {
                    let claim_owner = transaction
                        .query_row(
                            "SELECT workflow_id FROM workflow_resource_claims
                             WHERE resource_namespace = ?1 AND resource_key = ?2",
                            params![namespace, key],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()?;
                    if claim_owner
                        .as_deref()
                        .is_some_and(|owner| owner != item.spec.workflow_id.to_string())
                    {
                        diagnostics.claim_rejected = diagnostics.claim_rejected.saturating_add(1);
                        continue;
                    }
                }
                let allocation = ResourceAllocation {
                    id: AllocationId::new(),
                    requirement_key: requirement.key.clone(),
                    resource: candidate.resource.clone(),
                    quantity: requirement.quantity,
                    state: AllocationState::Active,
                };
                transaction.execute(
                    "INSERT INTO workflow_resource_allocations (
                        id, item_id, requirement_key, pool_key, resource_namespace,
                        resource_key, quantity, state, created_at_ms, updated_at_ms
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'active', ?8, ?8)",
                    params![
                        allocation.id.to_string(),
                        item_id.to_string(),
                        requirement.key.as_str(),
                        pool_key,
                        namespace,
                        key,
                        requested_quantity,
                        candidate.observed_at_ms,
                    ],
                )?;
                if is_exclusive_namespace(&namespace) {
                    transaction.execute(
                        "INSERT INTO workflow_resource_claims (
                            resource_namespace, resource_key, workflow_id, acquired_at, updated_at
                         ) VALUES (?1, ?2, ?3, ?4, ?4)
                         ON CONFLICT(resource_namespace, resource_key) DO UPDATE SET
                            updated_at = excluded.updated_at
                         WHERE workflow_resource_claims.workflow_id = excluded.workflow_id",
                        params![
                            namespace,
                            key,
                            item.spec.workflow_id.to_string(),
                            candidate.observed_at_ms,
                        ],
                    )?;
                }
                selected.push(allocation);
            }
            let selected_count = u32::try_from(selected.len()).unwrap_or(u32::MAX);
            if selected_count < requirement.count {
                return Err(RepositoryError::AllocationShortage {
                    requirement_key: requirement.key.clone(),
                    missing_count: requirement.count - selected_count,
                    details: diagnostics.render(requirement, &requirements, affinities),
                });
            }
            allocation_set
                .by_requirement
                .insert(requirement.key.clone(), selected);
        }
        transaction.commit()?;
        Ok(allocation_set)
    }

    /// Marks one missing allocation dead and atomically attempts a replacement.
    pub fn replace_dead_allocation(
        &self,
        item_id: WorkItemId,
        allocation_id: AllocationId,
        candidates: &[AllocationCandidate],
        now_ms: i64,
    ) -> Result<ReplacementOutcome, RepositoryError> {
        self.replace_dead_allocation_with_affinity(item_id, allocation_id, candidates, now_ms, &[])
    }

    /// Replaces one missing allocation while preserving caller-supplied
    /// same-resource-key affinity between requirement pairs.
    pub fn replace_dead_allocation_with_affinity(
        &self,
        item_id: WorkItemId,
        allocation_id: AllocationId,
        candidates: &[AllocationCandidate],
        now_ms: i64,
        affinities: &[(&str, &str)],
    ) -> Result<ReplacementOutcome, RepositoryError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let item = read_work_item_in(&transaction, item_id)?
            .ok_or(RepositoryError::WorkItemNotFound(item_id))?;
        let (requirement_key, dead_namespace, dead_key, quantity) = transaction
            .query_row(
                "SELECT requirement_key, resource_namespace, resource_key, quantity
                 FROM workflow_resource_allocations
                 WHERE id = ?1 AND item_id = ?2 AND state = 'active'",
                params![allocation_id.to_string(), item_id.to_string()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or(RepositoryError::WorkItemNotFound(item_id))?;
        let requirements: Vec<ResourceRequirement> =
            serde_json::from_value(item.spec.requirements_json.clone())?;
        let requirement = requirements
            .iter()
            .find(|requirement| requirement.key == requirement_key)
            .cloned()
            .ok_or_else(|| RepositoryError::AllocationShortage {
                requirement_key: requirement_key.clone(),
                missing_count: 1,
                details: "; persisted requirement is no longer present in the work item".to_owned(),
            })?;
        transaction.execute(
            "UPDATE workflow_resource_allocations
             SET state = 'dead', updated_at_ms = ?2 WHERE id = ?1",
            params![allocation_id.to_string(), now_ms],
        )?;
        if is_exclusive_namespace(&dead_namespace) {
            transaction.execute(
                "DELETE FROM workflow_resource_claims
                 WHERE resource_namespace = ?1 AND resource_key = ?2 AND workflow_id = ?3",
                params![dead_namespace, dead_key, item.spec.workflow_id.to_string()],
            )?;
        }

        let active_allocations = list_active_allocations_in(&transaction, item_id)?;
        let mut eligible_owned = false;
        let mut ordered = candidates.to_vec();
        ordered.sort_by(|left, right| {
            resource_sort_key(&left.resource).cmp(&resource_sort_key(&right.resource))
        });
        for candidate in &ordered {
            if !candidate_matches_kind_capabilities(candidate, &requirement) {
                continue;
            }
            eligible_owned = true;
            if !candidate_matches_scope(candidate, &requirement.scope)
                || !candidate_matches_affinity(
                    candidate,
                    &requirement.key,
                    affinities,
                    &active_allocations,
                )
                || !candidate_supports_affined_dependents(
                    &transaction,
                    candidate,
                    &requirement,
                    &requirements,
                    &ordered,
                    &active_allocations,
                    item.spec.workflow_id,
                    affinities,
                )?
            {
                continue;
            }
            let (namespace, key) = candidate.resource.persisted_parts()?;
            if namespace == dead_namespace && key == dead_key {
                continue;
            }
            eligible_owned = true;
            let pool_key = serde_json::to_string(&candidate.resource)?;
            let active_quantity: i64 = transaction.query_row(
                "SELECT COALESCE(SUM(quantity), 0)
                 FROM workflow_resource_allocations
                 WHERE pool_key = ?1 AND state = 'active'",
                [&pool_key],
                |row| row.get(0),
            )?;
            let available = i64::try_from(candidate.available_quantity).unwrap_or(i64::MAX);
            if available.saturating_sub(active_quantity) < quantity {
                continue;
            }
            if namespace == "replicant" {
                let has_active_assignment: bool = transaction.query_row(
                    "SELECT EXISTS (
                        SELECT 1 FROM workflow_assignments
                        WHERE worker_namespace = ?1
                          AND worker_key = ?2
                          AND state != 'released'
                     )",
                    params![namespace, key],
                    |row| row.get(0),
                )?;
                if has_active_assignment {
                    continue;
                }
            }
            if is_exclusive_namespace(&namespace) {
                let claim_owner = transaction
                    .query_row(
                        "SELECT workflow_id FROM workflow_resource_claims
                         WHERE resource_namespace = ?1 AND resource_key = ?2",
                        params![namespace, key],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                if claim_owner
                    .as_deref()
                    .is_some_and(|owner| owner != item.spec.workflow_id.to_string())
                {
                    continue;
                }
            }
            let revision = i64::try_from(candidate.observed_revision)
                .map_err(|_| RepositoryError::RevisionOutOfRange(candidate.observed_revision))?;
            transaction.execute(
                "INSERT INTO workflow_resource_pools (
                    pool_key, resource_namespace, resource_key, kind, capabilities_json,
                    location_json, available_quantity, observed_revision, observed_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(pool_key) DO UPDATE SET
                    available_quantity = excluded.available_quantity,
                    observed_revision = excluded.observed_revision,
                    observed_at_ms = excluded.observed_at_ms
                 WHERE excluded.observed_revision > workflow_resource_pools.observed_revision",
                params![
                    pool_key,
                    namespace,
                    key,
                    candidate.kind,
                    serde_json::to_string(&candidate.capabilities)?,
                    candidate
                        .location
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()?,
                    available,
                    revision,
                    candidate.observed_at_ms,
                ],
            )?;
            let replacement = ResourceAllocation {
                id: AllocationId::new(),
                requirement_key: requirement_key.clone(),
                resource: candidate.resource.clone(),
                quantity: u64::try_from(quantity).map_err(|_| {
                    RepositoryError::InvalidStoredWorkItemCount {
                        field: "allocation quantity",
                        value: quantity,
                    }
                })?,
                state: AllocationState::Active,
            };
            transaction.execute(
                "INSERT INTO workflow_resource_allocations (
                    id, item_id, requirement_key, pool_key, resource_namespace,
                    resource_key, quantity, state, created_at_ms, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'active', ?8, ?8)",
                params![
                    replacement.id.to_string(),
                    item_id.to_string(),
                    requirement_key,
                    pool_key,
                    namespace,
                    key,
                    quantity,
                    now_ms,
                ],
            )?;
            if is_exclusive_namespace(&namespace) {
                transaction.execute(
                    "INSERT INTO workflow_resource_claims (
                        resource_namespace, resource_key, workflow_id, acquired_at, updated_at
                     ) VALUES (?1, ?2, ?3, ?4, ?4)",
                    params![namespace, key, item.spec.workflow_id.to_string(), now_ms],
                )?;
            }
            transaction.commit()?;
            return Ok(ReplacementOutcome::Replaced(replacement));
        }
        let outcome = if eligible_owned {
            ReplacementOutcome::Waiting
        } else {
            transition_work_item_in(
                &transaction,
                &item,
                WorkItemTransition::Failed {
                    error: "ReplacementUnavailable".into(),
                    result_json: None,
                },
                now_ms,
            )?;
            ReplacementOutcome::Unavailable
        };
        transaction.commit()?;
        Ok(outcome)
    }

    /// Persists one active worker assignment for an assigned item.
    pub fn assign_work_item(
        &self,
        item_id: WorkItemId,
        expected_revision: u64,
        assignment_id: &str,
        worker: &ResourceKey,
        now_ms: i64,
    ) -> Result<(), RepositoryError> {
        if assignment_id.is_empty() {
            return Err(RepositoryError::InvalidWorkItemAssignment(
                "assignment_id must not be empty",
            ));
        }
        let (namespace, key) = worker.persisted_parts()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let item = read_work_item_in(&transaction, item_id)?
            .ok_or(RepositoryError::WorkItemNotFound(item_id))?;
        verify_work_item_revision(&item, expected_revision)?;
        if item.state.status != WorkItemStatus::Assigned {
            return Err(invalid_work_item_transition(
                &item,
                WorkItemStatus::Assigned,
            ));
        }
        transaction.execute(
            "INSERT INTO workflow_assignments (
                id, item_id, worker_namespace, worker_key, state, created_at_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, 'active', ?5, ?5)",
            params![assignment_id, item_id.to_string(), namespace, key, now_ms],
        )?;
        transaction.commit()?;
        Ok(())
    }

    /// Requests cooperative reclaim at the executor's next safe boundary.
    pub fn request_work_item_reclaim(
        &self,
        item_id: WorkItemId,
        now_ms: i64,
    ) -> Result<bool, RepositoryError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let changed = transaction.execute(
            "UPDATE workflow_assignments
             SET state = 'reclaim_requested', reclaim_requested_at_ms = ?2, updated_at_ms = ?2
             WHERE item_id = ?1 AND state = 'active'",
            params![item_id.to_string(), now_ms],
        )? != 0;
        transaction.commit()?;
        Ok(changed)
    }

    /// Returns whether cooperative reclaim is requested for this item.
    pub fn work_item_reclaim_requested(
        &self,
        item_id: WorkItemId,
    ) -> Result<bool, RepositoryError> {
        Ok(self.connection()?.query_row(
            "SELECT EXISTS (
                SELECT 1 FROM workflow_assignments
                WHERE item_id = ?1 AND state = 'reclaim_requested'
             )",
            [item_id.to_string()],
            |row| row.get(0),
        )?)
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
        let failure_disposition = (state.status == WorkflowStatus::Failed)
            .then_some(WorkflowFailureDisposition::Retryable);
        self.update_state(id, expected_revision, state, None, failure_disposition)
    }

    pub(crate) fn update_with_wait<P: Serialize, R: Serialize>(
        &self,
        id: WorkflowId,
        expected_revision: u64,
        state: WorkflowState<P, R>,
        wait_intent: Option<&crate::WaitIntent>,
    ) -> Result<WorkflowInstance, RepositoryError> {
        let failure_disposition = (state.status == WorkflowStatus::Failed)
            .then_some(WorkflowFailureDisposition::Retryable);
        self.update_state(
            id,
            expected_revision,
            state,
            wait_intent,
            failure_disposition,
        )
    }

    pub(crate) fn update_with_failure_disposition<P: Serialize, R: Serialize>(
        &self,
        id: WorkflowId,
        expected_revision: u64,
        state: WorkflowState<P, R>,
        failure_disposition: WorkflowFailureDisposition,
    ) -> Result<WorkflowInstance, RepositoryError> {
        self.update_state(
            id,
            expected_revision,
            state,
            None,
            Some(failure_disposition),
        )
    }

    fn update_state<P: Serialize, R: Serialize>(
        &self,
        id: WorkflowId,
        expected_revision: u64,
        state: WorkflowState<P, R>,
        wait_intent: Option<&crate::WaitIntent>,
        failure_disposition: Option<WorkflowFailureDisposition>,
    ) -> Result<WorkflowInstance, RepositoryError> {
        let failure_disposition = (state.status == WorkflowStatus::Failed)
            .then(|| failure_disposition.map(WorkflowFailureDisposition::as_str))
            .flatten();
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
                revision = revision + 1, wait_intent_json = ?9,
                failure_disposition = ?10
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
                failure_disposition,
            ],
        )?;
        if state.status.is_terminal() {
            transaction.execute(
                "UPDATE workflow_work_item_attempts
                 SET ended_at_ms = ?2, outcome = 'cancelled'
                 WHERE ended_at_ms IS NULL
                   AND item_id IN (
                     SELECT id FROM workflow_work_items WHERE workflow_id = ?1
                   )",
                params![id.to_string(), now],
            )?;
            transaction.execute(
                "DELETE FROM workflow_resource_claims
                 WHERE workflow_id = ?1
                   AND EXISTS (
                     SELECT 1 FROM workflow_resource_allocations allocation
                     JOIN workflow_work_items item ON item.id = allocation.item_id
                     WHERE item.workflow_id = ?1
                       AND allocation.state = 'active'
                       AND allocation.resource_namespace =
                           workflow_resource_claims.resource_namespace
                       AND allocation.resource_key = workflow_resource_claims.resource_key
                   )",
                [id.to_string()],
            )?;
            transaction.execute(
                "UPDATE workflow_resource_allocations
                 SET state = 'released', updated_at_ms = ?2
                 WHERE state = 'active'
                   AND item_id IN (
                     SELECT id FROM workflow_work_items WHERE workflow_id = ?1
                   )",
                params![id.to_string(), now],
            )?;
            transaction.execute(
                "UPDATE workflow_assignments
                 SET state = 'released', reclaim_requested_at_ms = NULL, updated_at_ms = ?2
                 WHERE state != 'released'
                   AND item_id IN (
                     SELECT id FROM workflow_work_items WHERE workflow_id = ?1
                   )",
                params![id.to_string(), now],
            )?;
            transaction.execute(
                "UPDATE workflow_work_items
                 SET status = 'abandoned', next_attempt_at_ms = NULL,
                     consecutive_failure_count = 0, updated_at_ms = ?2,
                     revision = revision + 1
                 WHERE workflow_id = ?1
                   AND status NOT IN ('succeeded', 'skipped', 'failed', 'abandoned')",
                params![id.to_string(), now],
            )?;
        }
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
                       parent_id, revision, wait_intent_json, failure_disposition";

const SUMMARY_COLUMNS: &str = "id, kind, status, revision, current_step, updated_at";

const TRIGGER_COLUMNS: &str = "id, name, condition_json, target_json, enabled, created_at, \
                               updated_at, last_fired_at, next_run_at, last_error, \
                               event_cursor, revision";

const WORK_ITEM_COLUMNS: &str = "id, workflow_id, dedupe_key, kind, sort_key, payload_json, \
    preconditions_json, requirements_json, deadline_at_ms, status, checkpoint_json, result_json, \
    last_error, attempt_count, consecutive_failure_count, next_attempt_at_ms, ever_started, \
    created_at_ms, updated_at_ms, revision";

const WORK_ITEM_ATTEMPT_COLUMNS: &str = "item_id, assignment_id, worker_identity, attempt_ordinal, \
    started_at_ms, ended_at_ms, outcome, error";

fn read_work_item_in(
    connection: &Connection,
    id: WorkItemId,
) -> Result<Option<WorkItem>, RepositoryError> {
    connection
        .query_row(
            &format!("SELECT {WORK_ITEM_COLUMNS} FROM workflow_work_items WHERE id = ?1"),
            [id.to_string()],
            row_to_work_item,
        )
        .optional()
        .map_err(Into::into)
}

fn read_work_item_by_key_in(
    connection: &Connection,
    workflow_id: WorkflowId,
    dedupe_key: &str,
) -> Result<Option<WorkItem>, RepositoryError> {
    connection
        .query_row(
            &format!(
                "SELECT {WORK_ITEM_COLUMNS} FROM workflow_work_items
                 WHERE workflow_id = ?1 AND dedupe_key = ?2"
            ),
            params![workflow_id.to_string(), dedupe_key],
            row_to_work_item,
        )
        .optional()
        .map_err(Into::into)
}

fn list_work_items_in(
    connection: &Connection,
    workflow_id: WorkflowId,
) -> Result<Vec<WorkItem>, RepositoryError> {
    let mut statement = connection.prepare(&format!(
        "SELECT {WORK_ITEM_COLUMNS} FROM workflow_work_items
         WHERE workflow_id = ?1
         ORDER BY sort_key, dedupe_key, id"
    ))?;
    let rows = statement.query_map([workflow_id.to_string()], row_to_work_item)?;
    rows.map(|row| row.map_err(Into::into)).collect()
}

fn row_to_work_item(row: &rusqlite::Row<'_>) -> Result<WorkItem, rusqlite::Error> {
    let id = parse_work_item_id(row.get(0)?).map_err(to_sql_conversion_error)?;
    let workflow_id = parse_id(row.get(1)?)?;
    let kind = WorkflowKind::new(row.get::<_, String>(3)?).map_err(to_sql_conversion_error)?;
    let attempt_count = parse_work_item_count("attempt", row.get(13)?)?;
    let consecutive_failure_count = parse_work_item_count("consecutive failure", row.get(14)?)?;
    let revision = row.get::<_, i64>(19)?;
    let checkpoint_json = row
        .get::<_, Option<String>>(10)?
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                10,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    let result_json = row
        .get::<_, Option<String>>(11)?
        .map(|json| serde_json::from_str(&json))
        .transpose()
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                11,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    Ok(WorkItem {
        id,
        spec: WorkItemSpec {
            workflow_id,
            dedupe_key: row.get(2)?,
            kind,
            sort_key: row.get(4)?,
            payload_json: parse_work_item_json(row, 5)?,
            preconditions_json: parse_work_item_json(row, 6)?,
            requirements_json: parse_work_item_json(row, 7)?,
            deadline_at_ms: row.get(8)?,
        },
        state: WorkItemState {
            status: parse_work_item_status(row.get(9)?).map_err(to_sql_conversion_error)?,
            checkpoint_json,
            result_json,
            last_error: row.get(12)?,
            attempt_count,
            consecutive_failure_count,
            next_attempt_at_ms: row.get(15)?,
            ever_started: row.get(16)?,
            created_at_ms: row.get(17)?,
            updated_at_ms: row.get(18)?,
            revision: u64::try_from(revision).map_err(|_| {
                to_sql_conversion_error(RepositoryError::InvalidStoredWorkItemRevision(revision))
            })?,
        },
    })
}

fn row_to_attempt(row: &rusqlite::Row<'_>) -> Result<WorkItemAttempt, rusqlite::Error> {
    let item_id = parse_work_item_id(row.get(0)?).map_err(to_sql_conversion_error)?;
    let outcome = row
        .get::<_, Option<String>>(6)?
        .map(parse_work_item_attempt_outcome)
        .transpose()
        .map_err(to_sql_conversion_error)?;
    Ok(WorkItemAttempt {
        item_id,
        assignment_id: row.get(1)?,
        worker_identity: row.get(2)?,
        attempt_ordinal: parse_work_item_count("attempt ordinal", row.get(3)?)?,
        started_at_ms: row.get(4)?,
        ended_at_ms: row.get(5)?,
        outcome,
        error: row.get(7)?,
    })
}

fn parse_work_item_json(row: &rusqlite::Row<'_>, index: usize) -> Result<Value, rusqlite::Error> {
    serde_json::from_str(&row.get::<_, String>(index)?).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn list_active_allocations_in(
    connection: &Connection,
    item_id: WorkItemId,
) -> Result<AllocationSet, RepositoryError> {
    let mut statement = connection.prepare(
        "SELECT id, requirement_key, resource_namespace, resource_key, quantity, state
         FROM workflow_resource_allocations
         WHERE item_id = ?1 AND state = 'active'
         ORDER BY requirement_key, resource_namespace, resource_key, id",
    )?;
    let rows = statement
        .query_map([item_id.to_string()], |row| {
            let id = row.get::<_, String>(0)?;
            let quantity = row.get::<_, i64>(4)?;
            Ok((
                id,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                quantity,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut set = AllocationSet::default();
    for (id, requirement_key, namespace, key, quantity) in rows {
        let allocation = ResourceAllocation {
            id: AllocationId::from_str(&id)
                .map_err(|_| RepositoryError::InvalidStoredWorkItemId(id))?,
            requirement_key: requirement_key.clone(),
            resource: resource_key(namespace, key),
            quantity: u64::try_from(quantity).map_err(|_| {
                RepositoryError::InvalidStoredWorkItemCount {
                    field: "allocation quantity",
                    value: quantity,
                }
            })?,
            state: AllocationState::Active,
        };
        set.by_requirement
            .entry(requirement_key)
            .or_default()
            .push(allocation);
    }
    Ok(set)
}

fn resource_sort_key(resource: &ResourceKey) -> String {
    serde_json::to_string(resource).unwrap_or_default()
}

fn release_active_allocation_in(
    transaction: &rusqlite::Transaction<'_>,
    workflow_id: WorkflowId,
    allocation: &ResourceAllocation,
    now_ms: i64,
) -> Result<(), RepositoryError> {
    let (namespace, key) = allocation.resource.persisted_parts()?;
    transaction.execute(
        "UPDATE workflow_resource_allocations
         SET state = 'released', updated_at_ms = ?2
         WHERE id = ?1 AND state = 'active'",
        params![allocation.id.to_string(), now_ms],
    )?;
    if is_exclusive_namespace(&namespace) {
        transaction.execute(
            "DELETE FROM workflow_resource_claims
             WHERE workflow_id = ?1
               AND resource_namespace = ?2
               AND resource_key = ?3
               AND NOT EXISTS (
                 SELECT 1
                 FROM workflow_resource_allocations allocation
                 JOIN workflow_work_items item ON item.id = allocation.item_id
                 WHERE item.workflow_id = ?1
                   AND allocation.state = 'active'
                   AND allocation.resource_namespace = ?2
                   AND allocation.resource_key = ?3
               )",
            params![workflow_id.to_string(), namespace, key],
        )?;
    }
    Ok(())
}

#[derive(Default)]
struct AllocationShortageDiagnostics {
    kind_matches: usize,
    scope_matches: usize,
    scope_rejected: usize,
    affinity_rejected: usize,
    dependent_affinity_rejected: usize,
    capacity_rejected: usize,
    assignment_rejected: usize,
    claim_rejected: usize,
}

impl AllocationShortageDiagnostics {
    fn render(
        &self,
        requirement: &ResourceRequirement,
        requirements: &[ResourceRequirement],
        affinities: &[(&str, &str)],
    ) -> String {
        let label = allocation_requirement_label(requirement);
        if self.kind_matches == 0 {
            return format!(
                "; no owned {label} candidates match the required type or capabilities"
            );
        }
        if self.scope_matches == 0 {
            return format!(
                "; {} owned {label} candidate{} found, but none are in {}",
                self.kind_matches,
                plural_suffix(self.kind_matches),
                allocation_scope_description(&requirement.scope),
            );
        }

        let mut blockers = Vec::new();
        if self.scope_rejected > 0 {
            blockers.push(format!(
                "{} outside {}",
                self.scope_rejected,
                allocation_scope_description(&requirement.scope),
            ));
        }
        if self.affinity_rejected > 0 {
            blockers.push(format!(
                "{} do not match the required resource pairing",
                self.affinity_rejected
            ));
        }
        if self.dependent_affinity_rejected > 0 {
            let dependency = affined_dependency_description(requirement, requirements, affinities);
            blockers.push(format!(
                "{} cannot satisfy {dependency}",
                self.dependent_affinity_rejected
            ));
        }
        if self.capacity_rejected > 0 {
            blockers.push(format!("{} lack free capacity", self.capacity_rejected));
        }
        if self.assignment_rejected > 0 {
            blockers.push(format!(
                "{} already assigned to active work",
                self.assignment_rejected
            ));
        }
        if self.claim_rejected > 0 {
            blockers.push(format!(
                "{} claimed by other workflows",
                self.claim_rejected
            ));
        }

        let match_verb = if self.scope_matches == 1 {
            "matches"
        } else {
            "match"
        };
        let summary = format!(
            "; {} {label} candidate{} {match_verb} the requirement",
            self.scope_matches,
            plural_suffix(self.scope_matches),
        );
        if blockers.is_empty() {
            summary
        } else {
            format!("{summary}; unavailable: {}", blockers.join(", "))
        }
    }
}

fn allocation_requirement_label(requirement: &ResourceRequirement) -> String {
    requirement.capabilities.first().map_or_else(
        || humanize_allocation_token(&requirement.kind),
        |capability| humanize_allocation_token(capability),
    )
}

fn humanize_allocation_token(value: &str) -> String {
    value.replace(['_', '-'], " ")
}

fn allocation_scope_description(scope: &RequirementScope) -> String {
    match scope {
        RequirementScope::Anywhere => "the available resource pool".to_owned(),
        RequirementScope::Region(region) => format!("region {region}"),
        RequirementScope::System(system) => format!("system {system}"),
        RequirementScope::Location(location) => format!("location {location}"),
        RequirementScope::WithinLy { origin, range_ly } => {
            format!("{range_ly} LY of {origin}")
        }
    }
}

fn affined_dependency_description(
    parent: &ResourceRequirement,
    requirements: &[ResourceRequirement],
    affinities: &[(&str, &str)],
) -> String {
    let Some(dependent) = requirements.iter().find(|requirement| {
        affinity_parent(&requirement.key, affinities) == Some(parent.key.as_str())
    }) else {
        return "a dependent resource requirement".to_owned();
    };
    let label = allocation_requirement_label(dependent);
    match dependent.kind.as_str() {
        "stow" => format!(
            "dependent {label} capacity ({} free stow slot{} on the same resource)",
            dependent.quantity,
            plural_suffix(usize::try_from(dependent.quantity).unwrap_or(usize::MAX)),
        ),
        "attach" => format!(
            "dependent attachment capacity ({} free attachment slot{} on the same resource)",
            dependent.quantity,
            plural_suffix(usize::try_from(dependent.quantity).unwrap_or(usize::MAX)),
        ),
        _ => format!(
            "dependent {label} capacity ({} unit{} on the same resource)",
            dependent.quantity,
            plural_suffix(usize::try_from(dependent.quantity).unwrap_or(usize::MAX)),
        ),
    }
}

fn plural_suffix(count: usize) -> &'static str {
    if count == 1 { "" } else { "s" }
}

fn affinity_parent<'a>(requirement_key: &str, affinities: &'a [(&str, &str)]) -> Option<&'a str> {
    affinities
        .iter()
        .find_map(|(dependent, parent)| (*dependent == requirement_key).then_some(*parent))
}

fn candidate_matches_affinity(
    candidate: &AllocationCandidate,
    requirement_key: &str,
    affinities: &[(&str, &str)],
    allocations: &AllocationSet,
) -> bool {
    let Some(parent_requirement) = affinity_parent(requirement_key, affinities) else {
        return true;
    };
    let candidate_key = resource_identity_key(&candidate.resource);
    allocations
        .by_requirement
        .get(parent_requirement)
        .is_some_and(|selected| {
            selected
                .iter()
                .any(|allocation| resource_identity_key(&allocation.resource) == candidate_key)
        })
}

#[allow(clippy::too_many_arguments)]
fn candidate_supports_affined_dependents(
    transaction: &rusqlite::Transaction<'_>,
    candidate: &AllocationCandidate,
    parent_requirement: &ResourceRequirement,
    requirements: &[ResourceRequirement],
    candidates: &[AllocationCandidate],
    allocations: &AllocationSet,
    workflow_id: WorkflowId,
    affinities: &[(&str, &str)],
) -> Result<bool, RepositoryError> {
    let candidate_identity = resource_identity_key(&candidate.resource);
    for dependent in requirements.iter().filter(|requirement| {
        affinity_parent(&requirement.key, affinities) == Some(parent_requirement.key.as_str())
    }) {
        let existing_count = allocations
            .by_requirement
            .get(&dependent.key)
            .into_iter()
            .flatten()
            .filter(|allocation| resource_identity_key(&allocation.resource) == candidate_identity)
            .count();
        if existing_count >= usize::try_from(dependent.count).unwrap_or(usize::MAX) {
            continue;
        }

        let requested_quantity = i64::try_from(dependent.quantity).map_err(|_| {
            RepositoryError::InvalidStoredWorkItemCount {
                field: "required quantity",
                value: i64::MAX,
            }
        })?;
        let mut eligible = 0usize;
        for dependent_candidate in candidates {
            if resource_identity_key(&dependent_candidate.resource) != candidate_identity
                || !candidate_matches_requirement(dependent_candidate, dependent)
            {
                continue;
            }
            let pool_key = serde_json::to_string(&dependent_candidate.resource)?;
            let active_quantity: i64 = transaction.query_row(
                "SELECT COALESCE(SUM(quantity), 0)
                 FROM workflow_resource_allocations
                 WHERE pool_key = ?1 AND state = 'active'",
                [&pool_key],
                |row| row.get(0),
            )?;
            let available_quantity =
                i64::try_from(dependent_candidate.available_quantity).unwrap_or(i64::MAX);
            if available_quantity.saturating_sub(active_quantity) < requested_quantity {
                continue;
            }
            let (namespace, key) = dependent_candidate.resource.persisted_parts()?;
            if is_exclusive_namespace(&namespace) {
                let claim_owner = transaction
                    .query_row(
                        "SELECT workflow_id FROM workflow_resource_claims
                         WHERE resource_namespace = ?1 AND resource_key = ?2",
                        params![namespace, key],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                if claim_owner
                    .as_deref()
                    .is_some_and(|owner| owner != workflow_id.to_string())
                {
                    continue;
                }
            }
            eligible = eligible.saturating_add(1);
        }

        let needed = usize::try_from(dependent.count)
            .unwrap_or(usize::MAX)
            .saturating_sub(existing_count);
        if eligible < needed {
            return Ok(false);
        }
    }
    Ok(true)
}

fn resource_identity_key(resource: &ResourceKey) -> &str {
    match resource {
        ResourceKey::Replicant(key) | ResourceKey::Device(key) | ResourceKey::Autofactory(key) => {
            key
        }
        ResourceKey::Namespaced { key, .. } => key,
    }
}

fn candidate_matches_requirement(
    candidate: &AllocationCandidate,
    requirement: &ResourceRequirement,
) -> bool {
    candidate_matches_kind_capabilities(candidate, requirement)
        && candidate_matches_scope(candidate, &requirement.scope)
}

fn candidate_matches_kind_capabilities(
    candidate: &AllocationCandidate,
    requirement: &ResourceRequirement,
) -> bool {
    candidate.kind == requirement.kind
        && requirement
            .capabilities
            .iter()
            .all(|required| candidate.capabilities.contains(required))
}

fn candidate_matches_scope(candidate: &AllocationCandidate, scope: &RequirementScope) -> bool {
    match scope {
        RequirementScope::Anywhere => true,
        RequirementScope::Region(region) => {
            candidate
                .location
                .as_ref()
                .and_then(|location| location.region.as_ref())
                == Some(region)
        }
        RequirementScope::System(system) => {
            candidate
                .location
                .as_ref()
                .and_then(|location| location.system.as_ref())
                == Some(system)
        }
        RequirementScope::Location(designation) => {
            candidate
                .location
                .as_ref()
                .and_then(|location| location.designation.as_ref())
                == Some(designation)
        }
        RequirementScope::WithinLy { origin, range_ly } => {
            range_ly.is_finite()
                && *range_ly >= 0.0
                && candidate
                    .location
                    .as_ref()
                    .and_then(|location| location.distances_ly.get(origin))
                    .is_some_and(|distance| {
                        distance.is_finite() && *distance >= 0.0 && distance <= range_ly
                    })
        }
    }
}

fn is_exclusive_namespace(namespace: &str) -> bool {
    matches!(namespace, "replicant" | "device" | "autofactory")
}

fn parse_work_item_id(value: String) -> Result<WorkItemId, RepositoryError> {
    WorkItemId::from_str(&value).map_err(|_| RepositoryError::InvalidStoredWorkItemId(value))
}

fn parse_work_item_status(value: String) -> Result<WorkItemStatus, RepositoryError> {
    match value.as_str() {
        "pending" => Ok(WorkItemStatus::Pending),
        "assigned" => Ok(WorkItemStatus::Assigned),
        "running" => Ok(WorkItemStatus::Running),
        "waiting" => Ok(WorkItemStatus::Waiting),
        "succeeded" => Ok(WorkItemStatus::Succeeded),
        "skipped" => Ok(WorkItemStatus::Skipped),
        "failed" => Ok(WorkItemStatus::Failed),
        "abandoned" => Ok(WorkItemStatus::Abandoned),
        _ => Err(RepositoryError::InvalidStoredWorkItemStatus(value)),
    }
}

fn parse_work_item_attempt_outcome(
    value: String,
) -> Result<WorkItemAttemptOutcome, RepositoryError> {
    match value.as_str() {
        "succeeded" => Ok(WorkItemAttemptOutcome::Succeeded),
        "failed" => Ok(WorkItemAttemptOutcome::Failed),
        "reclaimed" => Ok(WorkItemAttemptOutcome::Reclaimed),
        "cancelled" => Ok(WorkItemAttemptOutcome::Cancelled),
        _ => Err(RepositoryError::InvalidStoredWorkItemAttemptOutcome(value)),
    }
}

fn parse_work_item_count(field: &'static str, value: i64) -> Result<u32, rusqlite::Error> {
    u32::try_from(value).map_err(|_| {
        to_sql_conversion_error(RepositoryError::InvalidStoredWorkItemCount { field, value })
    })
}

fn work_item_revision_to_sql(revision: u64) -> Result<i64, RepositoryError> {
    i64::try_from(revision).map_err(|_| RepositoryError::RevisionOutOfRange(revision))
}

fn verify_work_item_revision(
    item: &WorkItem,
    expected_revision: u64,
) -> Result<(), RepositoryError> {
    if item.state.revision == expected_revision {
        Ok(())
    } else {
        Err(RepositoryError::ConcurrentWorkItemUpdate {
            id: item.id,
            expected: expected_revision,
        })
    }
}

fn invalid_work_item_transition(item: &WorkItem, to: WorkItemStatus) -> RepositoryError {
    RepositoryError::InvalidWorkItemTransition {
        from: item.state.status,
        to,
    }
}

fn transition_work_item_in(
    transaction: &rusqlite::Transaction<'_>,
    current: &WorkItem,
    transition: WorkItemTransition,
    now_ms: i64,
) -> Result<(), RepositoryError> {
    if current.state.status.is_terminal() {
        return Err(invalid_work_item_transition(current, current.state.status));
    }
    let mut status = current.state.status;
    let mut checkpoint = current.state.checkpoint_json.clone();
    let mut result = current.state.result_json.clone();
    let mut last_error = current.state.last_error.clone();
    let mut consecutive_failures = current.state.consecutive_failure_count;
    let next_attempt_at_ms: Option<i64>;
    let mut attempt_close: Option<(WorkItemAttemptOutcome, Option<String>)> = None;
    let mut release_allocations = false;

    match transition {
        WorkItemTransition::CheckpointCommitted { checkpoint_json } => {
            if current.state.status != WorkItemStatus::Running {
                return Err(invalid_work_item_transition(
                    current,
                    WorkItemStatus::Running,
                ));
            }
            checkpoint = Some(checkpoint_json);
            consecutive_failures = 0;
            last_error = None;
            next_attempt_at_ms = None;
        }
        WorkItemTransition::Waiting {
            checkpoint_json,
            reason,
            retry_at_ms,
        } => {
            if !matches!(
                current.state.status,
                WorkItemStatus::Pending
                    | WorkItemStatus::Assigned
                    | WorkItemStatus::Running
                    | WorkItemStatus::Waiting
            ) {
                return Err(invalid_work_item_transition(
                    current,
                    WorkItemStatus::Waiting,
                ));
            }
            if current.state.status == WorkItemStatus::Running {
                attempt_close = Some((WorkItemAttemptOutcome::Reclaimed, None));
            }
            status = WorkItemStatus::Waiting;
            checkpoint = checkpoint_json.or(checkpoint);
            last_error = Some(reason);
            next_attempt_at_ms = retry_at_ms;
        }
        WorkItemTransition::RetryableFailure {
            checkpoint_json,
            error,
        } => {
            if current.state.status != WorkItemStatus::Running {
                return Err(invalid_work_item_transition(
                    current,
                    WorkItemStatus::Pending,
                ));
            }
            status = WorkItemStatus::Pending;
            checkpoint = checkpoint_json.or(checkpoint);
            last_error = Some(error.clone());
            consecutive_failures = consecutive_failures.checked_add(1).ok_or(
                RepositoryError::InvalidStoredWorkItemCount {
                    field: "consecutive failure",
                    value: i64::from(consecutive_failures),
                },
            )?;
            let delay = work_item_retry_delay_ms(
                current.id,
                current.state.attempt_count,
                consecutive_failures,
            );
            next_attempt_at_ms = Some(now_ms.saturating_add(delay));
            attempt_close = Some((WorkItemAttemptOutcome::Failed, Some(error)));
        }
        WorkItemTransition::Succeeded {
            checkpoint_json,
            result_json,
        } => {
            if current.state.status != WorkItemStatus::Running {
                return Err(invalid_work_item_transition(
                    current,
                    WorkItemStatus::Succeeded,
                ));
            }
            status = WorkItemStatus::Succeeded;
            checkpoint = checkpoint_json.or(checkpoint);
            result = result_json;
            last_error = None;
            consecutive_failures = 0;
            next_attempt_at_ms = None;
            attempt_close = Some((WorkItemAttemptOutcome::Succeeded, None));
        }
        WorkItemTransition::Skipped {
            reason,
            result_json,
        } => {
            if current.state.status == WorkItemStatus::Running {
                attempt_close = Some((WorkItemAttemptOutcome::Succeeded, None));
            }
            status = WorkItemStatus::Skipped;
            result = result_json;
            last_error = Some(reason);
            consecutive_failures = 0;
            next_attempt_at_ms = None;
        }
        WorkItemTransition::Failed { error, result_json } => {
            if current.state.status == WorkItemStatus::Running {
                attempt_close = Some((WorkItemAttemptOutcome::Failed, Some(error.clone())));
            }
            status = WorkItemStatus::Failed;
            result = result_json;
            last_error = Some(error);
            consecutive_failures = 0;
            next_attempt_at_ms = None;
        }
        WorkItemTransition::Abandoned { reason } => {
            if current.state.status == WorkItemStatus::Running {
                attempt_close = Some((WorkItemAttemptOutcome::Cancelled, None));
            }
            status = WorkItemStatus::Abandoned;
            last_error = Some(reason);
            consecutive_failures = 0;
            next_attempt_at_ms = None;
        }
        WorkItemTransition::Reclaimed { checkpoint_json } => {
            if !matches!(
                current.state.status,
                WorkItemStatus::Assigned | WorkItemStatus::Running
            ) {
                return Err(invalid_work_item_transition(
                    current,
                    WorkItemStatus::Pending,
                ));
            }
            if current.state.status == WorkItemStatus::Running {
                attempt_close = Some((WorkItemAttemptOutcome::Reclaimed, None));
            }
            status = WorkItemStatus::Pending;
            checkpoint = checkpoint_json.or(checkpoint);
            next_attempt_at_ms = None;
            release_allocations = true;
        }
    }

    if let Some((outcome, error)) = attempt_close {
        close_open_attempt(transaction, current.id, outcome, error.as_deref(), now_ms)?;
    }
    if status.is_terminal() {
        transaction.execute(
            "DELETE FROM workflow_resource_claims
             WHERE workflow_id = ?1
               AND EXISTS (
                 SELECT 1 FROM workflow_resource_allocations allocation
                 WHERE allocation.item_id = ?2
                   AND allocation.state = 'active'
                   AND allocation.resource_namespace =
                       workflow_resource_claims.resource_namespace
                   AND allocation.resource_key = workflow_resource_claims.resource_key
               )",
            params![current.spec.workflow_id.to_string(), current.id.to_string()],
        )?;
        transaction.execute(
            "UPDATE workflow_resource_allocations
             SET state = 'released', updated_at_ms = ?2
             WHERE item_id = ?1 AND state = 'active'",
            params![current.id.to_string(), now_ms],
        )?;
    } else if release_allocations {
        transaction.execute(
            "DELETE FROM workflow_resource_claims
             WHERE workflow_id = ?1
               AND EXISTS (
                 SELECT 1 FROM workflow_resource_allocations allocation
                 WHERE allocation.item_id = ?2
                   AND allocation.state = 'active'
                   AND allocation.resource_namespace =
                       workflow_resource_claims.resource_namespace
                   AND allocation.resource_key = workflow_resource_claims.resource_key
               )",
            params![current.spec.workflow_id.to_string(), current.id.to_string()],
        )?;
        transaction.execute(
            "UPDATE workflow_resource_allocations
             SET state = 'released', updated_at_ms = ?2
             WHERE item_id = ?1 AND state = 'active'",
            params![current.id.to_string(), now_ms],
        )?;
    }
    if !matches!(status, WorkItemStatus::Assigned | WorkItemStatus::Running) {
        transaction.execute(
            "UPDATE workflow_assignments
             SET state = 'released', reclaim_requested_at_ms = NULL, updated_at_ms = ?2
             WHERE item_id = ?1 AND state != 'released'",
            params![current.id.to_string(), now_ms],
        )?;
    }
    let expected_revision = work_item_revision_to_sql(current.state.revision)?;
    let changed = transaction.execute(
        "UPDATE workflow_work_items
         SET status = ?1, checkpoint_json = ?2, result_json = ?3, last_error = ?4,
             consecutive_failure_count = ?5, next_attempt_at_ms = ?6,
             updated_at_ms = ?7, revision = revision + 1
         WHERE id = ?8 AND revision = ?9",
        params![
            status.as_str(),
            checkpoint.as_ref().map(serde_json::to_string).transpose()?,
            result.as_ref().map(serde_json::to_string).transpose()?,
            last_error,
            i64::from(consecutive_failures),
            next_attempt_at_ms,
            now_ms,
            current.id.to_string(),
            expected_revision,
        ],
    )?;
    if changed == 0 {
        return Err(RepositoryError::ConcurrentWorkItemUpdate {
            id: current.id,
            expected: current.state.revision,
        });
    }
    Ok(())
}

fn close_open_attempt(
    transaction: &rusqlite::Transaction<'_>,
    item_id: WorkItemId,
    outcome: WorkItemAttemptOutcome,
    error: Option<&str>,
    ended_at_ms: i64,
) -> Result<(), RepositoryError> {
    let changed = transaction.execute(
        "UPDATE workflow_work_item_attempts
         SET ended_at_ms = ?1, outcome = ?2, error = ?3
         WHERE item_id = ?4 AND ended_at_ms IS NULL",
        params![ended_at_ms, outcome.as_str(), error, item_id.to_string()],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        Err(RepositoryError::InvalidWorkItemAssignment(
            "running item must have exactly one open attempt",
        ))
    }
}

fn work_item_retry_delay_ms(
    item_id: WorkItemId,
    attempt_ordinal: u32,
    consecutive_failure_count: u32,
) -> i64 {
    const BASE_MS: u64 = 300_000;
    const CAP_MS: u64 = 21_600_000;
    let exponent = consecutive_failure_count.saturating_sub(1);
    let unjittered = if exponent >= 7 {
        CAP_MS
    } else {
        BASE_MS.saturating_mul(1_u64 << exponent).min(CAP_MS)
    };
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in item_id
        .as_bytes()
        .iter()
        .chain(attempt_ordinal.to_le_bytes().iter())
    {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let jitter_basis_points = i64::try_from(hash % 2_001).unwrap_or(0) - 1_000;
    let factor = u64::try_from(10_000 + jitter_basis_points).unwrap_or(10_000);
    let jittered = u64::try_from(
        u128::from(unjittered)
            .saturating_mul(u128::from(factor))
            .checked_div(10_000)
            .unwrap_or(u128::from(CAP_MS)),
    )
    .unwrap_or(CAP_MS)
    .min(CAP_MS);
    i64::try_from(jittered).unwrap_or(i64::MAX)
}

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

fn insert_workflow_in<C, P>(
    transaction: &rusqlite::Transaction<'_>,
    workflow: &NewWorkflow<C, P>,
    id: WorkflowId,
    now: i64,
    config_json: String,
    checkpoint_json: String,
) -> Result<WorkflowInstance, RepositoryError> {
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
    read_in(transaction, id)?.ok_or(RepositoryError::NotFound(id))
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
        failure_disposition: row
            .get::<_, Option<String>>(14)?
            .map(|value| WorkflowFailureDisposition::from_str(&value))
            .transpose()
            .map_err(to_sql_conversion_error)?,
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
    fn file_repository_creates_its_parent_directory() {
        let directory = std::env::temp_dir().join(format!("replicant-workflow-{}", Uuid::new_v4()));
        let path = directory.join("replicant-runtime.sqlite");
        let repository = WorkflowRepository::open(&path).expect("repository");
        drop(repository);

        assert!(path.is_file());
        fs::remove_dir_all(directory).expect("remove test directory");
    }

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

    #[test]
    fn nonfailed_state_replacement_clears_failure_disposition() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let id = WorkflowId::new();
        repository
            .connection()
            .expect("connection")
            .execute(
                "INSERT INTO workflow_instances
                 (id, kind, schema_version, config_json, checkpoint_json, status,
                  current_step, created_at, updated_at, failure_disposition)
                 VALUES (?1, 'test.clear-disposition', 1, '{}', '{}', 'running',
                         'executing', 0, 0, 'permanent')",
                [id.to_string()],
            )
            .expect("insert inconsistent legacy fixture");
        let workflow = repository
            .read(id)
            .expect("read workflow")
            .expect("workflow");
        assert_eq!(
            workflow.failure_disposition,
            Some(WorkflowFailureDisposition::Permanent)
        );

        let updated = repository
            .update(
                id,
                workflow.revision,
                WorkflowState {
                    status: WorkflowStatus::Waiting,
                    current_step: Some("waiting".to_owned()),
                    checkpoint: Value::Null,
                    last_error: None,
                    result: None::<Value>,
                },
            )
            .expect("replace nonfailed state");

        assert_eq!(updated.status, WorkflowStatus::Waiting);
        assert_eq!(updated.failure_disposition, None);
    }
    fn atomic_test_workflow(marker: &str) -> NewWorkflow<Value, Value> {
        NewWorkflow {
            kind: WorkflowKind::new("test.atomic").expect("workflow kind"),
            schema_version: 1,
            config: Value::String(marker.to_owned()),
            checkpoint: Value::Null,
            current_step: None,
            parent_id: None,
        }
    }

    #[test]
    fn create_or_reuse_active_is_atomic_across_connections() {
        let directory = std::env::temp_dir().join(format!("replicant-workflow-{}", Uuid::new_v4()));
        let path = directory.join("runtime.sqlite");
        WorkflowRepository::open(&path).expect("initialize repository");
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let handles = (0..2)
            .map(|index| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    let repository = WorkflowRepository::open(&path).expect("repository");
                    barrier.wait();
                    repository
                        .create_or_reuse_active(
                            atomic_test_workflow(&index.to_string()),
                            |instance| Ok(instance.kind.as_str() == "test.atomic"),
                        )
                        .expect("create or reuse")
                })
            })
            .collect::<Vec<_>>();
        let results = handles
            .into_iter()
            .map(|handle| handle.join().expect("thread"))
            .collect::<Vec<_>>();
        let repository = WorkflowRepository::open(&path).expect("inspect repository");
        assert_eq!(repository.list().expect("workflows").len(), 1);
        assert_eq!(results[0].instance.id, results[1].instance.id);
        assert!(results.iter().filter(|result| result.created).count() == 1);
        drop(repository);
        fs::remove_dir_all(directory).expect("remove test directory");
    }

    #[test]
    fn create_or_reuse_active_keeps_incompatible_workflows_distinct() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        let first = repository
            .create_or_reuse_active(atomic_test_workflow("first"), |_| Ok(false))
            .expect("first workflow");
        let second = repository
            .create_or_reuse_active(atomic_test_workflow("second"), |_| Ok(false))
            .expect("second workflow");
        assert!(first.created);
        assert!(second.created);
        assert_ne!(first.instance.id, second.instance.id);
        assert_eq!(repository.list().expect("workflows").len(), 2);
    }

    #[test]
    fn compatibility_error_aborts_create_without_inserting() {
        let repository = WorkflowRepository::open_in_memory().expect("repository");
        repository
            .create(atomic_test_workflow("existing"))
            .expect("existing workflow");
        let error = match repository.create_or_reuse_active(atomic_test_workflow("unknown"), |_| {
            Err(RepositoryError::Compatibility(
                "unknown coverage".to_owned(),
            ))
        }) {
            Ok(_) => panic!("unknown compatibility must abort"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            RepositoryError::Compatibility(message) if message == "unknown coverage"
        ));
        assert_eq!(repository.list().expect("workflows").len(), 1);
        assert_eq!(
            RepositoryError::Compatibility("unknown coverage".to_owned()).to_string(),
            "workflow compatibility check failed: unknown coverage"
        );
    }
}
