use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

use crate::{WorkflowExecutor, WorkflowInstance, WorkflowKind};
use serde_json::Value;

/// Explicit replacement payload produced when upgrading a persisted workflow.
pub struct WorkflowMigration {
    pub(crate) config: Value,
    pub(crate) checkpoint: Value,
}

impl WorkflowMigration {
    /// Creates one complete config/checkpoint replacement.
    #[must_use]
    pub const fn new(config: Value, checkpoint: Value) -> Self {
        Self { config, checkpoint }
    }
}

/// Factory metadata required to load a persisted workflow kind.
///
pub trait WorkflowFactory: Send + Sync {
    /// Stable kind created by this factory.
    fn kind(&self) -> &WorkflowKind;

    /// Current schema version written by this factory.
    fn current_schema_version(&self) -> u32;

    /// Returns whether this factory can load a persisted schema version.
    fn supports_schema_version(&self, version: u32) -> bool {
        version == self.current_schema_version()
    }

    /// Explicitly migrates one supported old payload to the current schema.
    ///
    /// Factories that support old versions must override this method. Returning
    /// no migration for an old version fails the workflow before execution.
    fn migrate(&self, _instance: &WorkflowInstance) -> Result<Option<WorkflowMigration>, String> {
        Ok(None)
    }

    /// Constructs an executor for one persisted invocation.
    ///
    /// Metadata-only factories may retain the default until they are runnable.
    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        None
    }
}

/// Errors resolving persisted workflows through the registry.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RegistryError {
    /// The same stable kind was registered twice.
    #[error("workflow kind {0} is already registered")]
    DuplicateKind(WorkflowKind),
    /// No factory is registered for the persisted kind.
    #[error("workflow kind {0} is not registered")]
    UnknownKind(WorkflowKind),
    /// The factory cannot load the persisted payload schema.
    #[error("workflow kind {kind} does not support schema version {version}")]
    UnsupportedSchemaVersion {
        /// Workflow kind.
        kind: WorkflowKind,
        /// Persisted version.
        version: u32,
    },
    /// A supported old payload could not be migrated safely.
    #[error("workflow kind {kind} schema migration from version {from} to {to} failed: {reason}")]
    MigrationFailed {
        /// Workflow kind.
        kind: WorkflowKind,
        /// Persisted version.
        from: u32,
        /// Current version.
        to: u32,
        /// Actionable migration failure.
        reason: String,
    },
}

/// Registry of workflow factories keyed by stable kind.
#[derive(Default)]
pub struct WorkflowRegistry {
    factories: BTreeMap<WorkflowKind, Arc<dyn WorkflowFactory>>,
}

impl WorkflowRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a factory, rejecting duplicate stable kinds.
    pub fn register(&mut self, factory: Arc<dyn WorkflowFactory>) -> Result<(), RegistryError> {
        let kind = factory.kind().clone();
        match self.factories.entry(kind.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(factory);
                Ok(())
            }
            Entry::Occupied(_) => Err(RegistryError::DuplicateKind(kind)),
        }
    }

    /// Returns whether a factory is registered for `kind`.
    #[must_use]
    pub fn contains(&self, kind: &WorkflowKind) -> bool {
        self.factories.contains_key(kind)
    }

    /// Resolves a factory that can load the instance's persisted schema.
    pub fn resolve(
        &self,
        instance: &WorkflowInstance,
    ) -> Result<&dyn WorkflowFactory, RegistryError> {
        let factory = self
            .factories
            .get(&instance.kind)
            .ok_or_else(|| RegistryError::UnknownKind(instance.kind.clone()))?;
        if !factory.supports_schema_version(instance.schema_version) {
            return Err(RegistryError::UnsupportedSchemaVersion {
                kind: instance.kind.clone(),
                version: instance.schema_version,
            });
        }
        Ok(factory.as_ref())
    }

    /// Returns an explicit migration for a supported old workflow payload.
    pub fn migration(
        &self,
        instance: &WorkflowInstance,
    ) -> Result<Option<(u32, WorkflowMigration)>, RegistryError> {
        let factory = self
            .factories
            .get(&instance.kind)
            .ok_or_else(|| RegistryError::UnknownKind(instance.kind.clone()))?;
        let current = factory.current_schema_version();
        if instance.schema_version == current {
            return Ok(None);
        }
        if !factory.supports_schema_version(instance.schema_version) {
            return Err(RegistryError::UnsupportedSchemaVersion {
                kind: instance.kind.clone(),
                version: instance.schema_version,
            });
        }
        factory
            .migrate(instance)
            .map_err(|reason| RegistryError::MigrationFailed {
                kind: instance.kind.clone(),
                from: instance.schema_version,
                to: current,
                reason,
            })?
            .map(|migration| (current, migration))
            .ok_or_else(|| RegistryError::MigrationFailed {
                kind: instance.kind.clone(),
                from: instance.schema_version,
                to: current,
                reason: "factory declared this version supported but returned no migration"
                    .to_owned(),
            })
            .map(Some)
    }
}
