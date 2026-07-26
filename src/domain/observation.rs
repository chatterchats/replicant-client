use serde::{Deserialize, Serialize};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

/// A normalized UTC instant stored as Unix milliseconds.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ObservationTime(i64);

impl ObservationTime {
    /// Creates a timestamp from Unix milliseconds.
    pub const fn from_unix_millis(value: i64) -> Self {
        Self(value)
    }

    /// Returns the Unix-millisecond representation used by SQLite.
    pub const fn unix_millis(self) -> i64 {
        self.0
    }

    /// Captures the current local observation time in UTC.
    pub fn now() -> Self {
        Self::from(OffsetDateTime::now_utc())
    }

    /// Normalizes an RFC 3339 timestamp to Unix milliseconds.
    pub fn parse_rfc3339(value: &str) -> Result<Self, time::error::Parse> {
        Ok(Self::from(OffsetDateTime::parse(value, &Rfc3339)?))
    }
}

impl From<OffsetDateTime> for ObservationTime {
    fn from(value: OffsetDateTime) -> Self {
        Self(value.unix_timestamp_nanos().div_euclid(1_000_000) as i64)
    }
}

impl From<&str> for ObservationTime {
    fn from(value: &str) -> Self {
        if let Ok(epoch) = value.parse::<i64>() {
            return Self::from_unix_millis(if epoch.unsigned_abs() < 100_000_000_000 {
                epoch.saturating_mul(1_000)
            } else {
                epoch
            });
        }
        Self::parse_rfc3339(value).unwrap_or_default()
    }
}

impl From<String> for ObservationTime {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

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
    pub observed_at: ObservationTime,
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

#[cfg(test)]
mod tests {
    use super::ObservationTime;

    #[test]
    fn rfc3339_and_epoch_seconds_normalize_to_milliseconds() {
        let rfc = ObservationTime::parse_rfc3339("2026-07-25T00:00:00Z").expect("valid RFC 3339");
        assert_eq!(rfc, ObservationTime::from("1784937600"));
        assert_eq!(rfc.unix_millis(), 1_784_937_600_000);
    }

    #[test]
    fn numeric_order_crosses_digit_boundaries() {
        assert!(ObservationTime::from("1000000000") > ObservationTime::from("999999999"));
    }
}
