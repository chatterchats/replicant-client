use super::*;
use tracing::trace;

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
        trace!(target: "replicant_client::domain", "retaining snapshot because incoming observation is stale");
        return MergeOutcome::Retained(existing, MergeRejection::StaleObservation);
    }
    MergeOutcome::Replaced(incoming)
}

pub fn merge_replicant(
    existing: Observation<Replicant>,
    mut incoming: Observation<Replicant>,
) -> MergeOutcome<Observation<Replicant>> {
    if metadata_order(&incoming.metadata, &existing.metadata).is_lt() {
        trace!(target: "replicant_client::domain", "retaining replicant because incoming observation is stale");
        return MergeOutcome::Retained(existing, MergeRejection::StaleObservation);
    }
    if matches!(incoming.metadata.access, AccessScope::Public) {
        trace!(target: "replicant_client::domain", "preserving private replicant fields from public observation");
        incoming.value.travel = existing.value.travel.clone();
        incoming.value.private = existing.value.private.clone();
        incoming.value.access = existing.value.access.clone();
    }
    MergeOutcome::Replaced(incoming)
}

pub fn merge_device(
    existing: Observation<Device>,
    mut incoming: Observation<Device>,
) -> MergeOutcome<Observation<Device>> {
    if metadata_order(&incoming.metadata, &existing.metadata).is_lt() {
        trace!(target: "replicant_client::domain", "retaining device because incoming observation is stale");
        return MergeOutcome::Retained(existing, MergeRejection::StaleObservation);
    }
    if matches!(existing.metadata.access, AccessScope::Owned)
        && matches!(incoming.metadata.access, AccessScope::Public)
    {
        trace!(target: "replicant_client::domain", "preserving private device ownership and hosting from public observation");
        incoming.value.relationships.assigned_replicant =
            existing.value.relationships.assigned_replicant;
        incoming.value.relationships.hosting_replicant =
            existing.value.relationships.hosting_replicant;
        incoming.value.relationships.linked_device = existing.value.relationships.linked_device;
        incoming.value.relationships.stowed_in = existing.value.relationships.stowed_in;
        incoming.value.relationships.attached_devices =
            existing.value.relationships.attached_devices;
        incoming.value.relationships.controlled_devices =
            existing.value.relationships.controlled_devices;
        incoming.value.relationships.stowed_devices = existing.value.relationships.stowed_devices;
        incoming.value.attach_capacity = existing.value.attach_capacity;
        incoming.value.stow_capacity = existing.value.stow_capacity;
        incoming.value.stow_used = existing.value.stow_used;
        incoming.value.operational_capacity = existing.value.operational_capacity;
        incoming.value.active_directive = existing.value.active_directive;
        incoming.value.travel = existing.value.travel;
    }
    MergeOutcome::Replaced(incoming)
}

fn preserve<T: Clone>(incoming: &mut Option<T>, existing: &Option<T>) {
    if incoming.is_none() {
        *incoming = existing.clone();
    }
}

pub fn merge_star(
    existing: Observation<Star>,
    mut incoming: Observation<Star>,
) -> MergeOutcome<Observation<Star>> {
    if metadata_order(&incoming.metadata, &existing.metadata).is_lt() {
        return MergeOutcome::Retained(existing, MergeRejection::StaleObservation);
    }
    preserve(&mut incoming.value.name, &existing.value.name);
    preserve(
        &mut incoming.value.spectral_type,
        &existing.value.spectral_type,
    );
    preserve(&mut incoming.value.entry_point, &existing.value.entry_point);
    preserve(&mut incoming.value.position, &existing.value.position);
    preserve(&mut incoming.value.has_hub, &existing.value.has_hub);
    preserve(&mut incoming.value.region, &existing.value.region);
    MergeOutcome::Replaced(incoming)
}

pub fn merge_star_knowledge(
    existing: Observation<StarKnowledge>,
    mut incoming: Observation<StarKnowledge>,
) -> MergeOutcome<Observation<StarKnowledge>> {
    if metadata_order(&incoming.metadata, &existing.metadata).is_lt() {
        return MergeOutcome::Retained(existing, MergeRejection::StaleObservation);
    }
    preserve(&mut incoming.value.position, &existing.value.position);
    preserve(
        &mut incoming.value.spectral_type,
        &existing.value.spectral_type,
    );
    preserve(&mut incoming.value.entry_point, &existing.value.entry_point);
    preserve(&mut incoming.value.explored, &existing.value.explored);
    preserve(&mut incoming.value.has_hub, &existing.value.has_hub);
    preserve(&mut incoming.value.has_life, &existing.value.has_life);
    preserve(&mut incoming.value.region, &existing.value.region);
    preserve(
        &mut incoming.value.distance_from_replicant,
        &existing.value.distance_from_replicant,
    );
    preserve(
        &mut incoming.value.estimated_travel_time,
        &existing.value.estimated_travel_time,
    );
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
    let can_tombstone = collection.completeness.can_reconcile_membership()
        && matches!(
            collection.metadata.authority,
            ObservationAuthority::CompleteCollection
        )
        && tombstone_eligible(&collection.metadata, &RemovalEvidence::CompleteCollection);
    trace!(target: "replicant_client::domain", "collection tombstone eligibility={can_tombstone}");
    can_tombstone
}
