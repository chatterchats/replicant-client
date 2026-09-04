//! Shared physical eligibility for workers used by regional automation.

use replicant_client::domain::{Device, Replicant};

/// Capability attached to broker candidates that are physically ready for regional work.
pub(crate) const OPERATIONAL_REGIONAL_WORKER_CAPABILITY: &str = "operational_regional_worker";

/// Current reason a regional worker cannot be dispatched.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerState {
    /// Assigned, unclaimed, stationary, and physically located with its racing vessel.
    Operational,
    /// The Replicant or its racing vessel is currently travelling.
    InTransit,
    /// A durable workflow already owns the Replicant.
    Busy,
    /// The assignment does not match the requested region.
    WrongRegion,
    /// No hosted racing vessel is known.
    MissingVessel,
    /// The hosted vessel lacks an authoritative current location.
    UnknownLocation,
    /// The Replicant/vessel host relationship is inconsistent.
    LocationMismatch,
    /// Current Replicant status is not operational.
    Unavailable,
}

impl WorkerState {
    pub(crate) fn is_operational(self) -> bool {
        self == Self::Operational
    }
}

/// Classifies one Replicant/vessel pair from current managed projections.
///
/// `assigned_region` and `requested_region` are omitted by non-Director callers that need only
/// physical readiness. `physical_region` is derived by the caller from the vessel's authoritative
/// location and the current catalogue projection.
pub(crate) fn classify_regional_worker(
    replicant: &Replicant,
    vessel: Option<&Device>,
    assigned_region: Option<&str>,
    requested_region: Option<&str>,
    physical_region: Option<&str>,
    busy: bool,
) -> WorkerState {
    if let Some(requested) = requested_region
        && assigned_region != Some(requested)
    {
        return WorkerState::WrongRegion;
    }
    if busy {
        return WorkerState::Busy;
    }
    let Some(vessel) = vessel else {
        return WorkerState::MissingVessel;
    };
    let vessel_hosts_replicant = vessel
        .relationships
        .hosting_replicant
        .as_ref()
        .is_some_and(|host| host == &replicant.key);
    let replicant_hosts_in_vessel = replicant
        .hosted_device
        .as_ref()
        .is_some_and(|host| host == &vessel.key);
    if !vessel_hosts_replicant && !replicant_hosts_in_vessel {
        return WorkerState::LocationMismatch;
    }
    if replicant.travel.is_some() || vessel.travel.is_some() {
        return WorkerState::InTransit;
    }
    match replicant.status.as_ref().map(|status| status.as_str()) {
        Some(status)
            if status.eq_ignore_ascii_case("travelling")
                || status.eq_ignore_ascii_case("traveling") =>
        {
            return WorkerState::InTransit;
        }
        Some(status)
            if status.eq_ignore_ascii_case("stationary")
                || status.eq_ignore_ascii_case("active") => {}
        _ => return WorkerState::Unavailable,
    }
    if vessel.location.is_none() {
        return WorkerState::UnknownLocation;
    }
    if let Some(requested) = requested_region
        && physical_region != Some(requested)
    {
        return WorkerState::WrongRegion;
    }
    WorkerState::Operational
}

#[cfg(test)]
mod tests {
    use replicant_client::domain::{
        AccessScope, DeviceKey, DeviceRelationships, DeviceType, LocationKey, ReplicantKey,
        ReplicantStatus, TravelState,
    };

    use super::*;

    fn worker() -> Replicant {
        Replicant {
            key: ReplicantKey::live("R-1".into()),
            name: None,
            is_npc: Some(false),
            status: Some(ReplicantStatus::Stationary),
            location: Some(LocationKey::live("ALPHA-1".into())),
            hosted_device: Some(DeviceKey::live("V-1".into())),
            travel: None,
            private: None,
            access: AccessScope::Owned,
        }
    }

    fn vessel() -> Device {
        Device {
            key: DeviceKey::live("V-1".into()),
            device_type: Some(DeviceType::RacingVessel),
            status: None,
            location: Some(LocationKey::live("ALPHA-1".into())),
            deployed_at: None,
            in_control_range: None,
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            settings: Default::default(),
            relationships: DeviceRelationships {
                hosting_replicant: Some(ReplicantKey::live("R-1".into())),
                ..DeviceRelationships::default()
            },
            cargo: Default::default(),
            cargo_capacity: None,
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            runtime: Default::default(),
            access: AccessScope::Owned,
        }
    }

    fn classify(replicant: &Replicant, vessel: &Device) -> WorkerState {
        classify_regional_worker(
            replicant,
            Some(vessel),
            Some("alpha"),
            Some("alpha"),
            Some("alpha"),
            false,
        )
    }

    #[test]
    fn stationary_located_assigned_worker_is_operational() {
        assert_eq!(classify(&worker(), &vessel()), WorkerState::Operational);
    }

    #[test]
    fn travelling_replicant_is_in_transit() {
        let mut worker = worker();
        worker.travel = Some(TravelState::default());
        assert_eq!(classify(&worker, &vessel()), WorkerState::InTransit);
    }

    #[test]
    fn travelling_vessel_is_in_transit() {
        let mut vessel = vessel();
        vessel.travel = Some(TravelState::default());
        assert_eq!(classify(&worker(), &vessel), WorkerState::InTransit);
    }

    #[test]
    fn missing_vessel_location_is_unavailable() {
        let mut vessel = vessel();
        vessel.location = None;
        assert_eq!(classify(&worker(), &vessel), WorkerState::UnknownLocation);
    }

    #[test]
    fn stale_or_missing_replicant_location_defers_to_hosted_vessel() {
        let mut worker = worker();
        worker.location = None;
        assert_eq!(classify(&worker, &vessel()), WorkerState::Operational);

        worker.location = Some(LocationKey::live("OLD-SYSTEM-1".into()));
        assert_eq!(classify(&worker, &vessel()), WorkerState::Operational);
    }

    #[test]
    fn travelling_status_without_travel_detail_is_in_transit() {
        let mut worker = worker();
        worker.status = Some(ReplicantStatus::Travelling);
        assert_eq!(classify(&worker, &vessel()), WorkerState::InTransit);
    }

    #[test]
    fn non_idle_replicant_status_is_unavailable() {
        let mut worker = worker();
        worker.status = Some(ReplicantStatus::Mining);
        assert_eq!(classify(&worker, &vessel()), WorkerState::Unavailable);

        worker.status = Some(ReplicantStatus::Offline);
        assert_eq!(classify(&worker, &vessel()), WorkerState::Unavailable);
    }

    #[test]
    fn unrelated_vessel_is_not_accepted_as_the_worker_host() {
        let mut worker = worker();
        worker.hosted_device = Some(DeviceKey::live("OTHER".into()));
        let mut vessel = vessel();
        vessel.relationships.hosting_replicant = Some(ReplicantKey::live("R-2".into()));
        assert_eq!(classify(&worker, &vessel), WorkerState::LocationMismatch);
    }

    #[test]
    fn worker_assigned_to_another_region_is_not_operational() {
        assert_eq!(
            classify_regional_worker(
                &worker(),
                Some(&vessel()),
                Some("beta"),
                Some("alpha"),
                Some("alpha"),
                false,
            ),
            WorkerState::WrongRegion
        );
    }

    #[test]
    fn arrival_projection_makes_worker_operational_on_later_classification() {
        let mut worker = worker();
        let mut vessel = vessel();
        worker.travel = Some(TravelState::default());
        vessel.travel = Some(TravelState::default());
        assert_eq!(classify(&worker, &vessel), WorkerState::InTransit);

        worker.travel = None;
        vessel.travel = None;
        worker.location = Some(LocationKey::live("ALPHA-2".into()));
        vessel.location = Some(LocationKey::live("ALPHA-2".into()));
        assert_eq!(classify(&worker, &vessel), WorkerState::Operational);
    }
}
