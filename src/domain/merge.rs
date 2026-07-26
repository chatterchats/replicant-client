use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MergeOutcome<T> {
    Replaced(T),
    Retained(T, MergeRejection),
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MergeRejection {
    StaleObservation,
    InsufficientAuthority,
}

fn authority_rank(authority: &ObservationAuthority) -> u8 {
    match authority {
        ObservationAuthority::OperationResult => 6,
        ObservationAuthority::EntitySnapshot => 5,
        ObservationAuthority::CompleteCollection => 4,
        ObservationAuthority::CollectionMember => 3,
        ObservationAuthority::EventDelta => 2,
        ObservationAuthority::PublicProfile | ObservationAuthority::Discovery => 1,
    }
}

fn source_rank(source: &ObservationSource) -> u8 {
    match source {
        ObservationSource::CommandResponse => 6,
        ObservationSource::RestDetail => 5,
        ObservationSource::Reconciliation => 4,
        ObservationSource::RestCollection => 3,
        ObservationSource::EventLog => 2,
        ObservationSource::Sse => 1,
    }
}

fn metadata_order(left: &ObservationMetadata, right: &ObservationMetadata) -> core::cmp::Ordering {
    left.observed_at
        .cmp(&right.observed_at)
        .then_with(|| authority_rank(&left.authority).cmp(&authority_rank(&right.authority)))
        .then_with(|| source_rank(&left.source).cmp(&source_rank(&right.source)))
}

pub fn merge_snapshot<T: Clone>(
    existing: Observation<T>,
    incoming: Observation<T>,
) -> MergeOutcome<Observation<T>> {
    if metadata_order(&incoming.metadata, &existing.metadata).is_lt() {
        return MergeOutcome::Retained(existing, MergeRejection::StaleObservation);
    }
    MergeOutcome::Replaced(incoming)
}

pub fn merge_replicant(
    existing: Observation<Replicant>,
    mut incoming: Observation<Replicant>,
) -> MergeOutcome<Observation<Replicant>> {
    if metadata_order(&incoming.metadata, &existing.metadata).is_lt() {
        return MergeOutcome::Retained(existing, MergeRejection::StaleObservation);
    }
    if matches!(incoming.metadata.access, AccessScope::Public) {
        incoming.value.private = existing.value.private.clone();
        incoming.value.access = existing.value.access.clone();
    }
    MergeOutcome::Replaced(incoming)
}

pub fn event_delta<T: Clone>(
    existing: Observation<T>,
    delta: ObservationMetadata,
    apply: impl FnOnce(&mut T),
) -> MergeOutcome<Observation<T>> {
    let mut incoming = existing.clone();
    apply(&mut incoming.value);
    incoming.metadata = delta;
    merge_snapshot(existing, incoming)
}

pub fn collection_can_tombstone<T>(collection: &CollectionObservation<T>) -> bool {
    collection.completeness.can_reconcile_membership()
        && matches!(
            collection.metadata.authority,
            ObservationAuthority::CompleteCollection
        )
        && tombstone_eligible(&collection.metadata, &RemovalEvidence::CompleteCollection)
}
