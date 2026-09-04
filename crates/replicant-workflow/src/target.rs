use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::WorkflowId;

/// One durable domain target owned or acted on by a workflow.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum WorkflowTarget {
    /// A discovered location event, with its physical context captured at association time.
    Event {
        /// Stable event designation.
        event_id: String,
        /// Containing system.
        system: String,
        /// Exact event location.
        location: String,
    },
    /// A star system designation.
    System {
        /// Stable system designation.
        system: String,
    },
    /// An exact location designation.
    Location {
        /// Stable location designation.
        location: String,
    },
    /// An owned device code.
    Device {
        /// Stable device code.
        device: String,
    },
    /// A fungible resource wire value.
    Resource {
        /// Stable resource value.
        resource: String,
    },
    /// A manufacturing blueprint key.
    Blueprint {
        /// Stable blueprint key.
        blueprint: String,
    },
    /// Application-defined target namespace for future workflow domains.
    Custom {
        /// Stable lowercase application namespace.
        namespace: String,
        /// Stable identity within the namespace.
        key: String,
        /// Optional non-secret structured labels retained with the association.
        #[serde(default)]
        metadata: BTreeMap<String, String>,
    },
}

impl WorkflowTarget {
    /// Returns the stable persisted target kind.
    #[must_use]
    pub fn kind(&self) -> &str {
        match self {
            Self::Event { .. } => "event",
            Self::System { .. } => "system",
            Self::Location { .. } => "location",
            Self::Device { .. } => "device",
            Self::Resource { .. } => "resource",
            Self::Blueprint { .. } => "blueprint",
            Self::Custom { namespace, .. } => namespace,
        }
    }

    /// Returns the stable identity within the target kind.
    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::Event { event_id, .. } => event_id,
            Self::System { system } => system,
            Self::Location { location } => location,
            Self::Device { device } => device,
            Self::Resource { resource } => resource,
            Self::Blueprint { blueprint } => blueprint,
            Self::Custom { key, .. } => key,
        }
    }
}

/// Persisted association between one workflow and one exact domain target.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowTargetRecord {
    /// Workflow that owns or acts on this target.
    pub workflow_id: WorkflowId,
    /// Exact structured target.
    pub target: WorkflowTarget,
    /// Whether this target remains in the workflow's current target set.
    pub active: bool,
    /// First association time in Unix milliseconds.
    pub created_at_ms: i64,
    /// Most recent idempotent association time in Unix milliseconds.
    pub updated_at_ms: i64,
}
