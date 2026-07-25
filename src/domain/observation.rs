use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ObservationSource {
    RestDetail,
    RestCollection,
    EventLog,
    Sse,
    CommandResponse,
    Reconciliation,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ObservationAuthority {
    EntitySnapshot,
    CollectionMember,
    CompleteCollection,
    EventDelta,
    OperationResult,
    PublicProfile,
    Discovery,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum AccessScope {
    Owned,
    SiblingShared,
    Granted,
    Public,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Reachability {
    Reachable,
    OutOfRange,
    AccessRevoked,
    Historical,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CollectionCompleteness {
    Complete,
    Filtered,
    RangeScoped,
    PublicDirectory,
    DiscoveryLimited,
    PartialPage,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceDocument {
    pub operation: String,
    pub request_id: Option<String>,
    pub document_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservationMetadata {
    pub source: ObservationSource,
    pub authority: ObservationAuthority,
    pub observed_at: String,
    pub access: AccessScope,
    pub reachability: Reachability,
    pub stale: bool,
    pub source_document: SourceDocument,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Observation<T> {
    pub value: T,
    pub metadata: ObservationMetadata,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectionObservation<T> {
    pub members: Vec<Observation<T>>,
    pub completeness: CollectionCompleteness,
    pub metadata: ObservationMetadata,
}

impl CollectionCompleteness {
    pub const fn can_reconcile_membership(&self) -> bool {
        matches!(self, Self::Complete)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum RemovalEvidence {
    ExplicitDecommission,
    ExplicitRemovalEvent,
    MutationResult,
    CompleteCollection,
    SimulationCleanup,
    NotFound,
    Absence,
}

pub fn tombstone_eligible(metadata: &ObservationMetadata, evidence: &RemovalEvidence) -> bool {
    if matches!(metadata.access, AccessScope::Public)
        || matches!(
            metadata.reachability,
            Reachability::OutOfRange | Reachability::AccessRevoked
        )
    {
        return false;
    }
    match evidence {
        RemovalEvidence::ExplicitDecommission
        | RemovalEvidence::ExplicitRemovalEvent
        | RemovalEvidence::MutationResult
        | RemovalEvidence::SimulationCleanup => true,
        RemovalEvidence::CompleteCollection => {
            matches!(metadata.authority, ObservationAuthority::CompleteCollection)
        }
        RemovalEvidence::NotFound | RemovalEvidence::Absence => false,
    }
}
