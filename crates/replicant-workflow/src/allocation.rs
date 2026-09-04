use std::{collections::BTreeMap, fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{ResourceKey, WorkItemId, WorkflowId};

/// Stable identifier for one persisted resource allocation.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AllocationId(Uuid);

impl AllocationId {
    /// Creates a unique allocation identifier.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for AllocationId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for AllocationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for AllocationId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Uuid::parse_str(value).map(Self)
    }
}

/// Geographic eligibility scope for a resource requirement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind", content = "value")]
pub enum RequirementScope {
    /// No geographic restriction.
    Anywhere,
    /// Resource must be in the named region.
    Region(String),
    /// Resource must be in the named system.
    System(String),
    /// Resource must be at the exact named location.
    Location(String),
    /// Resource must be within a maximum galactic distance.
    WithinLy {
        /// Origin location or system designation.
        origin: String,
        /// Maximum distance in light years.
        range_ly: f64,
    },
}

/// Typed resource need declared by a work item.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceRequirement {
    /// Stable requirement key within the item.
    pub key: String,
    /// Resource category understood by the broker.
    pub kind: String,
    /// Required capability names.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Geographic eligibility scope.
    pub scope: RequirementScope,
    /// Number of distinct pool members required.
    pub count: u32,
    /// Capacity required from each selected pool member.
    pub quantity: u64,
}

/// Broker-owned geographic facts for one observed candidate.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AllocationLocation {
    /// Operating region, when known.
    pub region: Option<String>,
    /// Parent system, when known.
    pub system: Option<String>,
    /// Exact current location designation, when known.
    pub designation: Option<String>,
    /// Precomputed distances from relevant requirement origins in light years.
    #[serde(default)]
    pub distances_ly: BTreeMap<String, f64>,
}

/// One resource candidate observed from authoritative managed state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AllocationCandidate {
    /// Exact resource identity.
    pub resource: ResourceKey,
    /// Resource category understood by the broker.
    pub kind: String,
    /// Available capability names.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Optional current geographic facts.
    pub location: Option<AllocationLocation>,
    /// Available capacity in this pool observation.
    pub available_quantity: u64,
    /// Monotonic managed-state observation revision.
    pub observed_revision: u64,
    /// Observation time in Unix milliseconds.
    pub observed_at_ms: i64,
}

/// Lifecycle state of one persisted allocation row.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AllocationState {
    /// Capacity and identity are actively owned by the item.
    Active,
    /// The allocated resource was proven permanently missing.
    Dead,
    /// Ownership was released normally.
    Released,
}

/// One exact identity and quantity selected for a requirement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceAllocation {
    /// Stable allocation identity.
    pub id: AllocationId,
    /// Requirement key satisfied by this allocation.
    pub requirement_key: String,
    /// Exact allocated resource.
    pub resource: ResourceKey,
    /// Quantity reserved from the resource pool.
    pub quantity: u64,
    /// Current allocation lifecycle state.
    pub state: AllocationState,
}

/// Actual identities and quantities selected for every requirement key.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AllocationSet {
    /// Allocations grouped by stable requirement key.
    pub by_requirement: BTreeMap<String, Vec<ResourceAllocation>>,
}

impl AllocationSet {
    /// Returns all allocations in deterministic requirement order.
    pub fn iter(&self) -> impl Iterator<Item = &ResourceAllocation> {
        self.by_requirement.values().flatten()
    }
}

/// Frontend/runtime-safe read projection of one active quantity reservation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ResourceReservation {
    /// Stable allocation identity.
    pub allocation_id: AllocationId,
    /// Workflow that owns the reservation.
    pub workflow_id: WorkflowId,
    /// Durable work item whose requirement created the reservation.
    pub item_id: WorkItemId,
    /// Stable requirement key within the work item.
    pub requirement_key: String,
    /// Exact reserved pool identity.
    pub resource: ResourceKey,
    /// Broker resource category, such as `material`, `device`, or `stow`.
    pub kind: String,
    /// Broker capabilities used to satisfy the requirement.
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// Geographic facts retained with the authoritative pool observation.
    pub location: Option<AllocationLocation>,
    /// Quantity reserved from the pool.
    pub quantity: u64,
    /// First reservation time in Unix milliseconds.
    pub created_at_ms: i64,
    /// Most recent reservation update time in Unix milliseconds.
    pub updated_at_ms: i64,
}

/// Result of replacing a permanently missing allocation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplacementOutcome {
    /// A replacement was allocated immediately.
    Replaced(ResourceAllocation),
    /// Eligible owned capacity exists but is temporarily unavailable.
    Waiting,
    /// No owned candidate can satisfy the original requirement.
    Unavailable,
}
