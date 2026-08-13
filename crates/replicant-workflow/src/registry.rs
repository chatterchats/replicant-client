use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

use crate::{WorkflowExecutor, WorkflowInstance, WorkflowKind};

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
}
