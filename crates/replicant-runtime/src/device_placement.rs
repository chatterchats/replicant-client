//! Conservative, derived placement classification for owned devices.
//!
//! The classifier deliberately does not persist its answer.  It combines the
//! complete managed census with the workflow registry's derived evidence and
//! only calls a device stranded when a retained failed-workflow custody
//! episode proves that it needs recovery.  Absence of a relationship or
//! location is never treated as proof when the corresponding authority is
//! incomplete.

use std::collections::{BTreeMap, BTreeSet};

use replicant_client::domain::AccessScope;
use replicant_client::{Device, DeviceStatus, DeviceType};
use replicant_protocol::workflow_tag_reserved;
use replicant_workflow::{
    WorkflowPlacementEvidence, WorkflowPlacementIntentEvidence, WorkflowPlacementIntentRelation,
    WorkflowPlacementIntentSnapshot,
};
use serde::{Deserialize, Serialize};

/// The mutually exclusive result of placement classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DevicePlacementClass {
    /// A known permanent, home, or relationship-based placement.
    Intentional,
    /// A device currently explained by travel or a live workflow.
    ExplainedTransient,
    /// A device with exact retained failed-workflow custody provenance.
    Stranded,
    /// Authority, topology, outcome, or recovery mechanics are insufficient.
    Ambiguous,
}

/// Explicit reason for the selected placement classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DevicePlacementReason {
    /// The observation is not owned by the account.
    NotOwned,
    /// The owned-device census is not complete enough for absence inference.
    MissingAuthoritativeDetail,
    /// The workflow registry cannot decode a live placement projection.
    UnknownWorkflowIntent,
    /// The workflow registry cannot decode a settled outcome projection.
    UnknownTerminalOutcome,
    /// No retained failed-workflow custody episode proves an orphan.
    MissingTransientProvenance,
    /// A parent is absent or the containment graph contains a cycle.
    UnresolvedTopology,
    /// The exact physical location or its region is unavailable.
    UnknownLocation,
    /// Device type or lifecycle status is not recognized.
    UnknownDeviceSemantics,
    /// A reserved workflow tag has no exact retained workflow projection.
    ReservedTagWithoutIntent,
    /// A non-workflow tag may represent user placement intent.
    UnrecognizedPlacementTag,
    /// Authoritative relationships disagree.
    RelationshipConflict,
    /// The device is recognized as permanent infrastructure.
    PermanentInfrastructure,
    /// A succeeded workflow proves deployment at this exact location.
    SettledWorkflowPlacement,
    /// A terminal workflow retained custody without proving deployment.
    TerminalResidualCustody,
    /// The device is at a registered regional home.
    HomeInventory,
    /// The device is contained by an owned parent.
    Contained,
    /// The device is controlled by another device.
    Controlled,
    /// The device is assigned to an owned Replicant.
    ReplicantAssigned,
    /// The device hosts an owned Replicant.
    ReplicantHosted,
    /// The device has a linked device relationship.
    Linked,
    /// The device has an active AMI directive.
    ActiveDirective,
    /// The device or one of its parents is traveling.
    Traveling,
    /// A live workflow has exact code or tag evidence.
    LiveWorkflowIntent,
    /// The device type/state has no recognized permanent role or safe path.
    UnsupportedRecovery,
    /// A failed workflow durably proves unfinished transient custody.
    FailedTransientOrphan,
}

/// Inputs used by [`classify_device_placement`].
///
/// `devices` is the complete owned-device census, keyed by a stable device
/// code.  Callers must set `complete_owned_census` to false for filtered,
/// public, or otherwise partial observations; relationship absence is then
/// intentionally unusable as negative evidence.
pub struct DevicePlacementContext<'a> {
    /// Whether the census contains authoritative complete owned-device detail.
    pub complete_owned_census: bool,
    /// Complete owned-device census, including potential containment parents.
    pub devices: &'a BTreeMap<String, Device>,
    /// Registered home location IDs grouped by canonical region.
    pub registered_homes: &'a BTreeMap<String, BTreeSet<String>>,
    /// Exact location ID to system ID map.
    pub location_systems: &'a BTreeMap<String, String>,
    /// Exact system ID to canonical region map.
    pub system_regions: &'a BTreeMap<String, String>,
    /// Derived workflow placement evidence for the current reconciliation.
    pub workflow_snapshot: &'a WorkflowPlacementIntentSnapshot,
}

/// A placement result with physical and workflow evidence retained for UI and
/// Director blockers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DevicePlacementClassification {
    /// Canonical device code being classified.
    pub device_code: String,
    /// Selected four-way placement class.
    pub class: DevicePlacementClass,
    /// Precedence rule that selected the class.
    pub reason: DevicePlacementReason,
    /// Direct or inherited exact physical location, when known.
    pub effective_location: Option<String>,
    /// Canonical region for the effective location, when known.
    pub region: Option<String>,
    /// Workflow evidence matching this device's exact code or tags.
    pub workflow_evidence: WorkflowPlacementEvidence,
}

#[derive(Clone, Debug, Default)]
struct Topology {
    location: Option<String>,
    traveling: bool,
    has_parent: bool,
    invalid: bool,
}

/// Classifies one managed device using conservative precedence.
///
/// No workflow kind is inspected here.  Workflow factories own decoding and
/// expose only typed, exact code/tag placement evidence through the snapshot.
#[must_use]
pub fn classify_device_placement(
    device: &Device,
    context: &DevicePlacementContext<'_>,
) -> DevicePlacementClassification {
    let raw_code = device.key.id.as_str();
    let device_code = canonical_code(raw_code);
    let workflow_evidence = explain_device(context.workflow_snapshot, raw_code, &device.tags);
    let topology = resolve_topology(raw_code, context.devices, &mut BTreeSet::new());
    let (effective_location, region) = match topology.invalid {
        true => (None, None),
        false => {
            let location = topology.location.clone();
            let region = location
                .as_deref()
                .and_then(|value| lookup_ci(context.location_systems, value))
                .and_then(|system| lookup_ci(context.system_regions, &system))
                .filter(|value| !value.is_empty())
                .map(|value| crate::canonical_region(&value));
            (location, region)
        }
    };

    let result = |class, reason| DevicePlacementClassification {
        device_code: device_code.clone(),
        class,
        reason,
        effective_location: effective_location.clone(),
        region: region.clone(),
        workflow_evidence: workflow_evidence.clone(),
    };

    // Physical authority always wins over later positive or negative rules.
    if device.access != AccessScope::Owned {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::NotOwned,
        );
    }
    if !context.complete_owned_census {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::MissingAuthoritativeDetail,
        );
    }
    if device.device_type.as_ref().is_none_or(is_unknown_type)
        || (device.status.as_ref().is_none_or(is_unknown_status)
            && !recognized_infrastructure_status(device)
            && !recognized_ward_placement(device))
    {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::UnknownDeviceSemantics,
        );
    }
    if device.in_control_range == Some(true) {
        let controller_missing =
            device
                .relationships
                .controller
                .as_ref()
                .is_none_or(|controller| {
                    find_device(context.devices, controller.id.as_str())
                        .is_none_or(|parent| parent.access != AccessScope::Owned)
                });
        if controller_missing {
            return result(
                DevicePlacementClass::Ambiguous,
                DevicePlacementReason::RelationshipConflict,
            );
        }
    }
    if topology.invalid {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::UnresolvedTopology,
        );
    }
    let Some(location) = effective_location.as_deref() else {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::UnknownLocation,
        );
    };
    if region.is_none() {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::UnknownLocation,
        );
    }

    // Positive physical intent precedes transient and negative evidence.
    if topology.has_parent {
        return result(
            DevicePlacementClass::Intentional,
            DevicePlacementReason::Contained,
        );
    }
    if device.relationships.controller.is_some() {
        return result(
            DevicePlacementClass::Intentional,
            DevicePlacementReason::Controlled,
        );
    }
    if device.relationships.assigned_replicant.is_some() {
        return result(
            DevicePlacementClass::Intentional,
            DevicePlacementReason::ReplicantAssigned,
        );
    }
    if device.relationships.hosting_replicant.is_some() {
        return result(
            DevicePlacementClass::Intentional,
            DevicePlacementReason::ReplicantHosted,
        );
    }
    if device.relationships.linked_device.is_some() {
        return result(
            DevicePlacementClass::Intentional,
            DevicePlacementReason::Linked,
        );
    }
    if device
        .active_directive
        .as_ref()
        .is_some_and(|directive| directive.directive.is_some())
    {
        return result(
            DevicePlacementClass::Intentional,
            DevicePlacementReason::ActiveDirective,
        );
    }
    if is_registered_home(
        location,
        region.as_deref().unwrap_or_default(),
        context.registered_homes,
    ) {
        return result(
            DevicePlacementClass::Intentional,
            DevicePlacementReason::HomeInventory,
        );
    }
    if workflow_evidence
        .settled_placements
        .iter()
        .any(|item| item.intent.relation == WorkflowPlacementIntentRelation::Deployed)
        && !settled_at_location(&workflow_evidence, location)
    {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::UnknownTerminalOutcome,
        );
    }
    if settled_at_location(&workflow_evidence, location) {
        return result(
            DevicePlacementClass::Intentional,
            DevicePlacementReason::SettledWorkflowPlacement,
        );
    }
    if recognized_infrastructure(device) {
        return result(
            DevicePlacementClass::Intentional,
            DevicePlacementReason::PermanentInfrastructure,
        );
    }

    // Unknown workflow coverage is a blocker before any absence inference.
    if !workflow_evidence.unknown_live_workflows.is_empty() {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::UnknownWorkflowIntent,
        );
    }
    if !workflow_evidence.unknown_terminal_outcomes.is_empty() {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::UnknownTerminalOutcome,
        );
    }
    let failed_reserved_tags =
        failed_transient_reserved_tags(context.workflow_snapshot, &device.tags);
    if device
        .tags
        .iter()
        .filter(|tag| workflow_tag_reserved(tag))
        .any(|tag| !failed_reserved_tags.contains(tag))
    {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::ReservedTagWithoutIntent,
        );
    }
    if !workflow_evidence.live.is_empty() {
        return result(
            DevicePlacementClass::ExplainedTransient,
            DevicePlacementReason::LiveWorkflowIntent,
        );
    }
    if topology.traveling {
        return result(
            DevicePlacementClass::ExplainedTransient,
            DevicePlacementReason::Traveling,
        );
    }
    if !workflow_evidence.terminal_residuals.is_empty() {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::TerminalResidualCustody,
        );
    }
    if device.tags.iter().any(|tag| !workflow_tag_reserved(tag)) {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::UnrecognizedPlacementTag,
        );
    }
    if known_infrastructure_without_positive_placement(device) {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::UnsupportedRecovery,
        );
    }
    if !known_inactive_recoverable_status(device) {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::UnknownDeviceSemantics,
        );
    }
    if !recovery_mechanics_documented(device) {
        return result(
            DevicePlacementClass::Ambiguous,
            DevicePlacementReason::UnsupportedRecovery,
        );
    }
    if has_failed_transient(&workflow_evidence) {
        return result(
            DevicePlacementClass::Stranded,
            DevicePlacementReason::FailedTransientOrphan,
        );
    }
    // A complete free device without exact failed custody provenance is not
    // safe to recover merely because it is away from home.
    result(
        DevicePlacementClass::Ambiguous,
        DevicePlacementReason::MissingTransientProvenance,
    )
}

fn canonical_code(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

fn is_unknown_type(value: &DeviceType) -> bool {
    matches!(value, DeviceType::Unknown(_))
}

fn is_unknown_status(value: &DeviceStatus) -> bool {
    matches!(value, DeviceStatus::Unknown(_))
}

fn lookup_ci(map: &BTreeMap<String, String>, value: &str) -> Option<String> {
    map.iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(value))
        .map(|(_, mapped)| mapped.clone())
}

fn is_registered_home(
    location: &str,
    region: &str,
    homes: &BTreeMap<String, BTreeSet<String>>,
) -> bool {
    homes.iter().any(|(home_region, locations)| {
        crate::canonical_region(home_region) == crate::canonical_region(region)
            && locations
                .iter()
                .any(|home| home.eq_ignore_ascii_case(location))
    })
}

fn settled_at_location(evidence: &WorkflowPlacementEvidence, location: &str) -> bool {
    evidence.settled_placements.iter().any(|item| {
        item.intent.relation == WorkflowPlacementIntentRelation::Deployed
            && item
                .intent
                .expected_location
                .as_deref()
                .is_some_and(|expected| expected.eq_ignore_ascii_case(location))
    })
}

fn has_failed_transient(evidence: &WorkflowPlacementEvidence) -> bool {
    evidence.failed_transient.iter().any(|item| {
        matches!(
            item.intent.relation,
            WorkflowPlacementIntentRelation::Claimed
                | WorkflowPlacementIntentRelation::Staged
                | WorkflowPlacementIntentRelation::Transported
                | WorkflowPlacementIntentRelation::Awaited
        )
    })
}

fn type_is(device: &Device, value: &str) -> bool {
    device
        .device_type
        .as_ref()
        .is_some_and(|device_type| device_type.as_str().eq_ignore_ascii_case(value))
}

fn status_is(device: &Device, value: &str) -> bool {
    device
        .status
        .as_ref()
        .is_some_and(|status| status.as_str().eq_ignore_ascii_case(value))
}

/// Returns whether the lifecycle state is explicitly known to be inactive and
/// safe to consider for recovery.  Only states with a documented command path
/// are admitted; open-shaped transition states (including `offline`) are
/// deliberately rejected.
fn known_inactive_recoverable_status(device: &Device) -> bool {
    status_is(device, "idle") || status_is(device, "deactivated")
}

/// Returns the reserved tags whose exact `DeviceTag` placement evidence is
/// retained in a failed transient workflow.  Code evidence, live evidence,
/// and settled/terminal evidence intentionally do not satisfy this contract.
fn failed_transient_reserved_tags(
    snapshot: &WorkflowPlacementIntentSnapshot,
    tags: &[String],
) -> BTreeSet<String> {
    snapshot
        .failed_transient
        .iter()
        .filter_map(|item| match &item.intent.subject {
            replicant_workflow::WorkflowPlacementIntentSubject::DeviceTag(tag)
                if workflow_tag_reserved(tag) && tags.iter().any(|candidate| candidate == tag) =>
            {
                Some(tag.clone())
            }
            _ => None,
        })
        .collect()
}

fn recognized_ward_placement(device: &Device) -> bool {
    type_is(device, "system_ward")
        && !device.deployed_at.as_deref().unwrap_or_default().is_empty()
        && device.location.is_some()
}

fn recognized_infrastructure_status(device: &Device) -> bool {
    if type_is(device, "ftl_beacon") {
        return status_is(device, "active") || status_is(device, "monitoring");
    }
    (type_is(device, "ftl_relay") || type_is(device, "deep_space_relay_station"))
        && (status_is(device, "active") || status_is(device, "relaying"))
}

fn recognized_infrastructure(device: &Device) -> bool {
    if type_is(device, "system_hub") {
        // System Hub intent is only safe at a known home; this function is
        // reached after home handling, so it intentionally remains false.
        return false;
    }
    if type_is(device, "system_ward") {
        return !device.deployed_at.as_deref().unwrap_or_default().is_empty()
            && device.location.is_some();
    }
    recognized_infrastructure_status(device)
}

fn known_infrastructure_without_positive_placement(device: &Device) -> bool {
    type_is(device, "system_hub")
        || type_is(device, "system_ward")
        || type_is(device, "ftl_relay")
        || type_is(device, "deep_space_relay_station")
        || type_is(device, "ftl_beacon")
}
fn recovery_mechanics_documented(device: &Device) -> bool {
    device
        .available_commands
        .iter()
        .any(|command| command.as_str().eq_ignore_ascii_case("attach"))
}

fn resolve_topology(
    code: &str,
    devices: &BTreeMap<String, Device>,
    visiting: &mut BTreeSet<String>,
) -> Topology {
    let canonical = canonical_code(code);
    if !visiting.insert(canonical.clone()) {
        return Topology {
            invalid: true,
            ..Topology::default()
        };
    }
    let Some(device) = find_device(devices, &canonical) else {
        visiting.remove(&canonical);
        return Topology {
            invalid: true,
            ..Topology::default()
        };
    };
    if device.access != AccessScope::Owned {
        visiting.remove(&canonical);
        return Topology {
            invalid: true,
            ..Topology::default()
        };
    }
    let parents = [
        device.relationships.stowed_in.as_ref(),
        device.relationships.attached_to.as_ref(),
    ];
    let parent_count = parents.iter().filter(|parent| parent.is_some()).count();
    if parent_count > 1 {
        visiting.remove(&canonical);
        return Topology {
            invalid: true,
            ..Topology::default()
        };
    }
    let own_location = device
        .location
        .as_ref()
        .map(|location| location.id.as_str().to_owned());
    let Some(parent) = parents.into_iter().flatten().next() else {
        let result = Topology {
            location: own_location,
            traveling: device.travel.is_some(),
            has_parent: false,
            invalid: false,
        };
        visiting.remove(&canonical);
        return result;
    };
    let parent_code = canonical_code(parent.id.as_str());
    let parent_result = resolve_topology(&parent_code, devices, visiting);
    let invalid_location = own_location
        .as_deref()
        .zip(parent_result.location.as_deref())
        .is_some_and(|(own, inherited)| !own.eq_ignore_ascii_case(inherited));
    let result = Topology {
        location: own_location.or(parent_result.location),
        traveling: device.travel.is_some() || parent_result.traveling,
        has_parent: true,
        invalid: parent_result.invalid || invalid_location,
    };
    visiting.remove(&canonical);
    result
}

fn find_device<'a>(devices: &'a BTreeMap<String, Device>, code: &str) -> Option<&'a Device> {
    devices.get(code).or_else(|| {
        devices
            .iter()
            .find(|(key, device)| {
                key.eq_ignore_ascii_case(code) || device.key.id.as_str().eq_ignore_ascii_case(code)
            })
            .map(|(_, device)| device)
    })
}

fn explain_device(
    snapshot: &WorkflowPlacementIntentSnapshot,
    raw_code: &str,
    tags: &[String],
) -> WorkflowPlacementEvidence {
    let mut evidence = snapshot.explain_device(raw_code, tags);
    let canonical = canonical_code(raw_code);
    if canonical != raw_code {
        merge_evidence(&mut evidence, snapshot.explain_device(&canonical, tags));
    }
    evidence
}

fn merge_evidence(target: &mut WorkflowPlacementEvidence, source: WorkflowPlacementEvidence) {
    append_unique(&mut target.live, source.live);
    append_unique(&mut target.settled_placements, source.settled_placements);
    append_unique(&mut target.terminal_residuals, source.terminal_residuals);
    append_unique(&mut target.failed_transient, source.failed_transient);
    append_unique(&mut target.resolved_transient, source.resolved_transient);
    target
        .unknown_live_workflows
        .extend(source.unknown_live_workflows);
    target
        .unknown_terminal_outcomes
        .extend(source.unknown_terminal_outcomes);
    target.unknown_live_workflows.sort();
    target.unknown_live_workflows.dedup();
    target.unknown_terminal_outcomes.sort();
    target.unknown_terminal_outcomes.dedup();
}

fn append_unique(
    target: &mut Vec<WorkflowPlacementIntentEvidence>,
    source: Vec<WorkflowPlacementIntentEvidence>,
) {
    for item in source {
        if !target.contains(&item) {
            target.push(item);
        }
    }
    target.sort_by(|left, right| {
        left.workflow_id
            .cmp(&right.workflow_id)
            .then_with(|| left.intent.cmp(&right.intent))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_client::{DeviceCommand, DeviceId, DeviceKey, LocationKey, ReplicantKey};
    type PlacementMaps = (
        BTreeMap<String, BTreeSet<String>>,
        BTreeMap<String, String>,
        BTreeMap<String, String>,
    );

    fn device(code: &str, kind: &str, status: &str) -> Device {
        Device {
            key: DeviceKey::live(DeviceId::from(code)),
            device_type: Some(DeviceType::from(kind)),
            status: Some(DeviceStatus::from(status)),
            location: Some(LocationKey::live("REMOTE".into())),
            deployed_at: None,
            in_control_range: None,
            available_commands: vec![DeviceCommand::from("attach")],
            available_directives: Vec::new(),
            tags: Vec::new(),
            settings: Default::default(),
            relationships: Default::default(),
            cargo: BTreeMap::new(),
            cargo_capacity: None,
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            features: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            runtime: Default::default(),
            access: AccessScope::Owned,
        }
    }

    fn context<'a>(
        devices: &'a BTreeMap<String, Device>,
        snapshot: &'a WorkflowPlacementIntentSnapshot,
        homes: &'a BTreeMap<String, BTreeSet<String>>,
        location_systems: &'a BTreeMap<String, String>,
        system_regions: &'a BTreeMap<String, String>,
    ) -> DevicePlacementContext<'a> {
        DevicePlacementContext {
            complete_owned_census: true,
            devices,
            registered_homes: homes,
            location_systems,
            system_regions,
            workflow_snapshot: snapshot,
        }
    }

    fn classify_fixture(
        candidate: &Device,
        devices: &BTreeMap<String, Device>,
        snapshot: &WorkflowPlacementIntentSnapshot,
        complete_owned_census: bool,
    ) -> DevicePlacementClassification {
        let (homes, locations, regions) = maps();
        let mut placement_context = context(devices, snapshot, &homes, &locations, &regions);
        placement_context.complete_owned_census = complete_owned_census;
        classify_device_placement(candidate, &placement_context)
    }

    fn maps() -> PlacementMaps {
        (
            BTreeMap::from([("north".to_owned(), BTreeSet::from(["HOME".to_owned()]))]),
            BTreeMap::from([
                ("REMOTE".to_owned(), "SYS".to_owned()),
                ("HOME".to_owned(), "HOME-SYS".to_owned()),
            ]),
            BTreeMap::from([
                ("SYS".to_owned(), "north".to_owned()),
                ("HOME-SYS".to_owned(), "north".to_owned()),
            ]),
        )
    }

    #[test]
    fn free_device_without_failed_provenance_is_ambiguous() {
        let candidate = device("d1", "mining_drone", "idle");
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let snapshot = WorkflowPlacementIntentSnapshot::default();
        let (homes, locations, regions) = maps();
        let placement_context = context(&devices, &snapshot, &homes, &locations, &regions);
        let result = classify_device_placement(&candidate, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(
            result.reason,
            DevicePlacementReason::MissingTransientProvenance
        );
    }

    #[test]
    fn home_inventory_is_intentional() {
        let mut candidate = device("d1", "mining_drone", "idle");
        candidate.location = Some(LocationKey::live("HOME".into()));
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let snapshot = WorkflowPlacementIntentSnapshot::default();
        let (homes, locations, regions) = maps();
        let placement_context = context(&devices, &snapshot, &homes, &locations, &regions);
        let result = classify_device_placement(&candidate, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::Intentional);
        assert_eq!(result.reason, DevicePlacementReason::HomeInventory);
    }

    #[test]
    fn unknown_authority_and_topology_never_strand() {
        let mut candidate = device("d1", "mining_drone", "idle");
        candidate.access = AccessScope::Public;
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let snapshot = WorkflowPlacementIntentSnapshot::default();
        let (homes, locations, regions) = maps();
        let placement_context = context(&devices, &snapshot, &homes, &locations, &regions);
        let result = classify_device_placement(&candidate, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::NotOwned);

        let mut broken = device("d2", "mining_drone", "idle");
        broken.relationships.stowed_in = Some(DeviceKey::live("MISSING".into()));
        let devices = BTreeMap::from([("D2".to_owned(), broken.clone())]);
        let placement_context = context(&devices, &snapshot, &homes, &locations, &regions);
        let result = classify_device_placement(&broken, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::UnresolvedTopology);
    }

    #[test]
    fn live_travel_and_infrastructure_are_not_stranded() {
        let mut candidate = device("d1", "mining_drone", "idle");
        candidate.travel = Some(Default::default());
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let snapshot = WorkflowPlacementIntentSnapshot::default();
        let (homes, locations, regions) = maps();
        let placement_context = context(&devices, &snapshot, &homes, &locations, &regions);
        let result = classify_device_placement(&candidate, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::ExplainedTransient);
        assert_eq!(result.reason, DevicePlacementReason::Traveling);

        let hub = device("hub", "system_hub", "active");
        let devices = BTreeMap::from([("HUB".to_owned(), hub.clone())]);
        let placement_context = context(&devices, &snapshot, &homes, &locations, &regions);
        let result = classify_device_placement(&hub, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);

        let mut home_hub = hub.clone();
        home_hub.location = Some(LocationKey::live("HOME".into()));
        let devices = BTreeMap::from([("HUB".to_owned(), home_hub.clone())]);
        let placement_context = context(&devices, &snapshot, &homes, &locations, &regions);
        let result = classify_device_placement(&home_hub, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::Intentional);
        assert_eq!(result.reason, DevicePlacementReason::HomeInventory);
        let mut beacon = device("beacon", "ftl_beacon", "monitoring");
        beacon.location = Some(LocationKey::live("REMOTE".into()));
        let devices = BTreeMap::from([("BEACON".to_owned(), beacon.clone())]);
        let placement_context = context(&devices, &snapshot, &homes, &locations, &regions);
        let result = classify_device_placement(&beacon, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::Intentional);
        assert_eq!(
            result.reason,
            DevicePlacementReason::PermanentInfrastructure
        );

        let mut ward = device("ward", "system_ward", "idle");
        ward.deployed_at = Some("2026-08-31T00:00:00Z".to_owned());
        ward.location = Some(LocationKey::live("REMOTE".into()));
        let devices = BTreeMap::from([("WARD".to_owned(), ward.clone())]);
        let placement_context = context(&devices, &snapshot, &homes, &locations, &regions);
        let result = classify_device_placement(&ward, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::Intentional);
        assert_eq!(
            result.reason,
            DevicePlacementReason::PermanentInfrastructure
        );
        for (code, kind, status) in [
            ("RELAY", "ftl_relay", "relaying"),
            ("DSRS", "deep_space_relay_station", "active"),
        ] {
            let relay = device(code, kind, status);
            let devices = BTreeMap::from([(code.to_owned(), relay.clone())]);
            let placement_context = context(&devices, &snapshot, &homes, &locations, &regions);
            let result = classify_device_placement(&relay, &placement_context);
            assert_eq!(result.class, DevicePlacementClass::Intentional);
            assert_eq!(
                result.reason,
                DevicePlacementReason::PermanentInfrastructure
            );
        }
    }

    #[test]
    fn exact_failed_transient_provenance_is_required_for_stranded() {
        let candidate = device("d1", "mining_drone", "deactivated");
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let workflow_id = replicant_workflow::WorkflowId::new();
        let kind = replicant_workflow::WorkflowKind::new("test").expect("kind");
        let snapshot = WorkflowPlacementIntentSnapshot {
            failed_transient: vec![WorkflowPlacementIntentEvidence {
                workflow_id,
                workflow_kind: kind,
                workflow_status: replicant_workflow::WorkflowStatus::Failed,
                intent: replicant_workflow::WorkflowPlacementIntent {
                    subject: replicant_workflow::WorkflowPlacementIntentSubject::Device(
                        "D1".to_owned(),
                    ),
                    relation: WorkflowPlacementIntentRelation::Staged,
                    work_item_id: None,
                    expected_location: None,
                },
            }],
            ..WorkflowPlacementIntentSnapshot::default()
        };
        let (homes, locations, regions) = maps();
        let placement_context = context(&devices, &snapshot, &homes, &locations, &regions);
        let result = classify_device_placement(&candidate, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::Stranded);
        assert_eq!(result.reason, DevicePlacementReason::FailedTransientOrphan);
    }

    #[test]
    fn stow_only_device_is_not_recoverable() {
        let mut candidate = device("d1", "mining_drone", "idle");
        candidate.available_commands = vec![DeviceCommand::from("stow")];
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let workflow_id = replicant_workflow::WorkflowId::new();
        let kind = replicant_workflow::WorkflowKind::new("test").expect("kind");
        let snapshot = WorkflowPlacementIntentSnapshot {
            failed_transient: vec![WorkflowPlacementIntentEvidence {
                workflow_id,
                workflow_kind: kind,
                workflow_status: replicant_workflow::WorkflowStatus::Failed,
                intent: replicant_workflow::WorkflowPlacementIntent {
                    subject: replicant_workflow::WorkflowPlacementIntentSubject::Device(
                        "D1".to_owned(),
                    ),
                    relation: WorkflowPlacementIntentRelation::Staged,
                    work_item_id: None,
                    expected_location: None,
                },
            }],
            ..WorkflowPlacementIntentSnapshot::default()
        };
        let (homes, locations, regions) = maps();
        let result = classify_device_placement(
            &candidate,
            &context(&devices, &snapshot, &homes, &locations, &regions),
        );
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::UnsupportedRecovery);
    }

    #[test]
    fn matching_resolution_suppresses_only_the_resolved_episode() {
        let candidate = device("d1", "mining_drone", "idle");
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let workflow_id = replicant_workflow::WorkflowId::new();
        let kind = replicant_workflow::WorkflowKind::new("test").expect("kind");
        let failed = WorkflowPlacementIntentEvidence {
            workflow_id,
            workflow_kind: kind,
            workflow_status: replicant_workflow::WorkflowStatus::Failed,
            intent: replicant_workflow::WorkflowPlacementIntent {
                subject: replicant_workflow::WorkflowPlacementIntentSubject::Device(
                    "D1".to_owned(),
                ),
                relation: WorkflowPlacementIntentRelation::Transported,
                work_item_id: None,
                expected_location: None,
            },
        };
        let snapshot = WorkflowPlacementIntentSnapshot {
            failed_transient: vec![failed.clone()],
            ..WorkflowPlacementIntentSnapshot::default()
        };
        let resolved = WorkflowPlacementIntentSnapshot {
            resolved_transient: vec![failed],
            ..WorkflowPlacementIntentSnapshot::default()
        };
        let (homes, locations, regions) = maps();
        let placement_context = context(&devices, &resolved, &homes, &locations, &regions);
        let result = classify_device_placement(&candidate, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(
            result.reason,
            DevicePlacementReason::MissingTransientProvenance
        );

        let mut later = resolved;
        later.failed_transient = snapshot.failed_transient;
        let placement_context = context(&devices, &later, &homes, &locations, &regions);
        let result = classify_device_placement(&candidate, &placement_context);
        assert_eq!(result.class, DevicePlacementClass::Stranded);
    }
    fn placement_evidence(
        subject: replicant_workflow::WorkflowPlacementIntentSubject,
        relation: WorkflowPlacementIntentRelation,
        workflow_status: replicant_workflow::WorkflowStatus,
    ) -> WorkflowPlacementIntentEvidence {
        WorkflowPlacementIntentEvidence {
            workflow_id: replicant_workflow::WorkflowId::new(),
            workflow_kind: replicant_workflow::WorkflowKind::new("test").expect("kind"),
            workflow_status,
            intent: replicant_workflow::WorkflowPlacementIntent {
                subject,
                relation,
                work_item_id: None,
                expected_location: None,
            },
        }
    }

    #[test]
    fn reserved_tag_cannot_be_satisfied_by_unrelated_code_evidence() {
        let mut candidate = device("d1", "mining_drone", "idle");
        candidate.tags = vec!["mine-m:one".to_owned()];
        let snapshot = WorkflowPlacementIntentSnapshot {
            live: vec![placement_evidence(
                replicant_workflow::WorkflowPlacementIntentSubject::Device("D1".to_owned()),
                WorkflowPlacementIntentRelation::Staged,
                replicant_workflow::WorkflowStatus::Running,
            )],
            ..WorkflowPlacementIntentSnapshot::default()
        };
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let (homes, locations, regions) = maps();
        let result = classify_device_placement(
            &candidate,
            &context(&devices, &snapshot, &homes, &locations, &regions),
        );
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(
            result.reason,
            DevicePlacementReason::ReservedTagWithoutIntent
        );
    }

    #[test]
    fn every_reserved_tag_requires_its_own_exact_tag_evidence() {
        let mut candidate = device("d1", "mining_drone", "idle");
        candidate.tags = vec!["mine-m:one".to_owned(), "mine-m:two".to_owned()];
        let snapshot = WorkflowPlacementIntentSnapshot {
            failed_transient: vec![placement_evidence(
                replicant_workflow::WorkflowPlacementIntentSubject::DeviceTag(
                    "mine-m:one".to_owned(),
                ),
                WorkflowPlacementIntentRelation::Staged,
                replicant_workflow::WorkflowStatus::Failed,
            )],
            ..WorkflowPlacementIntentSnapshot::default()
        };
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let (homes, locations, regions) = maps();
        let result = classify_device_placement(
            &candidate,
            &context(&devices, &snapshot, &homes, &locations, &regions),
        );
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(
            result.reason,
            DevicePlacementReason::ReservedTagWithoutIntent
        );
    }

    #[test]
    fn active_portable_device_with_failed_provenance_is_not_stranded() {
        let candidate = device("d1", "mining_drone", "active");
        let snapshot = WorkflowPlacementIntentSnapshot {
            failed_transient: vec![placement_evidence(
                replicant_workflow::WorkflowPlacementIntentSubject::Device("D1".to_owned()),
                WorkflowPlacementIntentRelation::Transported,
                replicant_workflow::WorkflowStatus::Failed,
            )],
            ..WorkflowPlacementIntentSnapshot::default()
        };
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let (homes, locations, regions) = maps();
        let result = classify_device_placement(
            &candidate,
            &context(&devices, &snapshot, &homes, &locations, &regions),
        );
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::UnknownDeviceSemantics);
    }

    #[test]
    fn terminal_residual_custody_is_ambiguous() {
        let candidate = device("d1", "mining_drone", "idle");
        let snapshot = WorkflowPlacementIntentSnapshot {
            terminal_residuals: vec![placement_evidence(
                replicant_workflow::WorkflowPlacementIntentSubject::Device("D1".to_owned()),
                WorkflowPlacementIntentRelation::Staged,
                replicant_workflow::WorkflowStatus::Succeeded,
            )],
            ..WorkflowPlacementIntentSnapshot::default()
        };
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let (homes, locations, regions) = maps();
        let result = classify_device_placement(
            &candidate,
            &context(&devices, &snapshot, &homes, &locations, &regions),
        );
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(
            result.reason,
            DevicePlacementReason::TerminalResidualCustody
        );
    }

    #[test]
    fn control_range_and_controller_disagreement_is_ambiguous() {
        let mut candidate = device("d1", "mining_drone", "idle");
        candidate.in_control_range = Some(true);
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let snapshot = WorkflowPlacementIntentSnapshot::default();
        let (homes, locations, regions) = maps();
        let result = classify_device_placement(
            &candidate,
            &context(&devices, &snapshot, &homes, &locations, &regions),
        );
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::RelationshipConflict);

        let mut candidate = device("d2", "mining_drone", "idle");
        candidate.in_control_range = Some(true);
        candidate.relationships.controller = Some(DeviceKey::live("CTRL".into()));
        let mut controller = device("ctrl", "mining_controller", "active");
        controller.access = AccessScope::Public;
        let devices = BTreeMap::from([
            ("D2".to_owned(), candidate.clone()),
            ("CTRL".to_owned(), controller),
        ]);
        let result = classify_device_placement(
            &candidate,
            &context(&devices, &snapshot, &homes, &locations, &regions),
        );
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::RelationshipConflict);
        let mut candidate = device("d3", "mining_drone", "idle");
        candidate.in_control_range = Some(false);
        candidate.relationships.controller = Some(DeviceKey::live("CTRL".into()));
        let controller = device("ctrl", "mining_controller", "active");
        let devices = BTreeMap::from([
            ("D3".to_owned(), candidate.clone()),
            ("CTRL".to_owned(), controller),
        ]);
        let result = classify_device_placement(
            &candidate,
            &context(&devices, &snapshot, &homes, &locations, &regions),
        );
        assert_eq!(result.class, DevicePlacementClass::Intentional);
        assert_eq!(result.reason, DevicePlacementReason::Controlled);
    }

    #[test]
    fn both_containment_relations_are_intentional() {
        for stowed in [false, true] {
            let mut candidate = device("d1", "mining_drone", "idle");
            if stowed {
                candidate.relationships.stowed_in = Some(DeviceKey::live("CARRIER".into()));
            } else {
                candidate.relationships.attached_to = Some(DeviceKey::live("CARRIER".into()));
            }
            let carrier = device("carrier", "transport_drone", "active");
            let devices = BTreeMap::from([
                ("D1".to_owned(), candidate.clone()),
                ("CARRIER".to_owned(), carrier),
            ]);
            let result = classify_fixture(
                &candidate,
                &devices,
                &WorkflowPlacementIntentSnapshot::default(),
                true,
            );
            assert_eq!(result.class, DevicePlacementClass::Intentional);
            assert_eq!(result.reason, DevicePlacementReason::Contained);
        }
    }

    #[test]
    fn controller_replicant_and_directive_evidence_are_intentional() {
        let snapshot = WorkflowPlacementIntentSnapshot::default();

        let mut controlled = device("controlled", "mining_drone", "idle");
        controlled.in_control_range = None;
        controlled.relationships.controller = Some(DeviceKey::live("CTRL".into()));
        let controller = device("ctrl", "mining_controller", "active");
        let devices = BTreeMap::from([
            ("CONTROLLED".to_owned(), controlled.clone()),
            ("CTRL".to_owned(), controller),
        ]);
        let result = classify_fixture(&controlled, &devices, &snapshot, true);
        assert_eq!(result.class, DevicePlacementClass::Intentional);
        assert_eq!(result.reason, DevicePlacementReason::Controlled);

        let mut assigned = device("assigned", "mining_drone", "idle");
        assigned.relationships.assigned_replicant = Some(ReplicantKey::live(
            replicant_client::ReplicantId::from("R1"),
        ));
        let devices = BTreeMap::from([("ASSIGNED".to_owned(), assigned.clone())]);
        let result = classify_fixture(&assigned, &devices, &snapshot, true);
        assert_eq!(result.class, DevicePlacementClass::Intentional);
        assert_eq!(result.reason, DevicePlacementReason::ReplicantAssigned);

        let mut hosted = device("hosted", "mining_drone", "idle");
        hosted.relationships.hosting_replicant = Some(ReplicantKey::live(
            replicant_client::ReplicantId::from("R2"),
        ));
        let devices = BTreeMap::from([("HOSTED".to_owned(), hosted.clone())]);
        let result = classify_fixture(&hosted, &devices, &snapshot, true);
        assert_eq!(result.class, DevicePlacementClass::Intentional);
        assert_eq!(result.reason, DevicePlacementReason::ReplicantHosted);

        let mut linked = device("linked", "mining_drone", "idle");
        linked.relationships.linked_device = Some(DeviceKey::live("OTHER".into()));
        let devices = BTreeMap::from([("LINKED".to_owned(), linked.clone())]);
        let result = classify_fixture(&linked, &devices, &snapshot, true);
        assert_eq!(result.class, DevicePlacementClass::Intentional);
        assert_eq!(result.reason, DevicePlacementReason::Linked);

        let mut directed = device("directed", "mining_drone", "idle");
        directed.active_directive = Some(replicant_client::ActiveDeviceDirective {
            directive: Some(replicant_client::domain::DeviceDirective::from(
                "survey_system",
            )),
            ..Default::default()
        });
        let devices = BTreeMap::from([("DIRECTED".to_owned(), directed.clone())]);
        let result = classify_fixture(&directed, &devices, &snapshot, true);
        assert_eq!(result.class, DevicePlacementClass::Intentional);
        assert_eq!(result.reason, DevicePlacementReason::ActiveDirective);
    }

    #[test]
    fn settled_deployment_requires_the_exact_current_location() {
        let candidate = device("d1", "mining_drone", "idle");
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let mut deployed = placement_evidence(
            replicant_workflow::WorkflowPlacementIntentSubject::Device("D1".to_owned()),
            WorkflowPlacementIntentRelation::Deployed,
            replicant_workflow::WorkflowStatus::Succeeded,
        );
        deployed.intent.expected_location = Some("REMOTE".to_owned());
        let snapshot = WorkflowPlacementIntentSnapshot {
            settled_placements: vec![deployed.clone()],
            ..Default::default()
        };
        let result = classify_fixture(&candidate, &devices, &snapshot, true);
        assert_eq!(result.class, DevicePlacementClass::Intentional);
        assert_eq!(
            result.reason,
            DevicePlacementReason::SettledWorkflowPlacement
        );

        deployed.intent.expected_location = Some("OTHER".to_owned());
        let snapshot = WorkflowPlacementIntentSnapshot {
            settled_placements: vec![deployed],
            ..Default::default()
        };
        let result = classify_fixture(&candidate, &devices, &snapshot, true);
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::UnknownTerminalOutcome);
    }

    #[test]
    fn every_live_placement_relation_including_waiting_is_explained() {
        for relation in [
            WorkflowPlacementIntentRelation::Claimed,
            WorkflowPlacementIntentRelation::Staged,
            WorkflowPlacementIntentRelation::Transported,
            WorkflowPlacementIntentRelation::Awaited,
        ] {
            let candidate = device("d1", "mining_drone", "idle");
            let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
            let snapshot = WorkflowPlacementIntentSnapshot {
                live: vec![placement_evidence(
                    replicant_workflow::WorkflowPlacementIntentSubject::Device("D1".to_owned()),
                    relation,
                    replicant_workflow::WorkflowStatus::Waiting,
                )],
                ..Default::default()
            };
            let result = classify_fixture(&candidate, &devices, &snapshot, true);
            assert_eq!(result.class, DevicePlacementClass::ExplainedTransient);
            assert_eq!(result.reason, DevicePlacementReason::LiveWorkflowIntent);
        }
    }

    #[test]
    fn every_failed_transient_custody_relation_can_prove_strandedness() {
        for relation in [
            WorkflowPlacementIntentRelation::Claimed,
            WorkflowPlacementIntentRelation::Staged,
            WorkflowPlacementIntentRelation::Transported,
            WorkflowPlacementIntentRelation::Awaited,
        ] {
            let candidate = device("d1", "mining_drone", "deactivated");
            let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
            let snapshot = WorkflowPlacementIntentSnapshot {
                failed_transient: vec![placement_evidence(
                    replicant_workflow::WorkflowPlacementIntentSubject::Device("D1".to_owned()),
                    relation,
                    replicant_workflow::WorkflowStatus::Failed,
                )],
                ..Default::default()
            };
            let result = classify_fixture(&candidate, &devices, &snapshot, true);
            assert_eq!(result.class, DevicePlacementClass::Stranded);
            assert_eq!(result.reason, DevicePlacementReason::FailedTransientOrphan);
        }
    }

    #[test]
    fn incomplete_or_unknown_authority_never_allows_stranding() {
        let snapshot = WorkflowPlacementIntentSnapshot::default();
        let mut unknown_type = device("type", "future_device", "idle");
        let devices = BTreeMap::from([("TYPE".to_owned(), unknown_type.clone())]);
        let result = classify_fixture(&unknown_type, &devices, &snapshot, true);
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::UnknownDeviceSemantics);

        unknown_type = device("status", "mining_drone", "future_status");
        let devices = BTreeMap::from([("STATUS".to_owned(), unknown_type.clone())]);
        let result = classify_fixture(&unknown_type, &devices, &snapshot, true);
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::UnknownDeviceSemantics);

        let candidate = device("d1", "mining_drone", "deactivated");
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let result = classify_fixture(&candidate, &devices, &snapshot, false);
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(
            result.reason,
            DevicePlacementReason::MissingAuthoritativeDetail
        );
    }

    #[test]
    fn missing_or_coarse_location_is_ambiguous() {
        for location in [None, Some(LocationKey::live("UNMAPPED".into()))] {
            let mut candidate = device("d1", "mining_drone", "deactivated");
            candidate.location = location;
            let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
            let result = classify_fixture(
                &candidate,
                &devices,
                &WorkflowPlacementIntentSnapshot::default(),
                true,
            );
            assert_eq!(result.class, DevicePlacementClass::Ambiguous);
            assert_eq!(result.reason, DevicePlacementReason::UnknownLocation);
        }
    }

    #[test]
    fn unknown_workflow_coverage_blocks_absence_inference() {
        let candidate = device("d1", "mining_drone", "idle");
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let live_snapshot = WorkflowPlacementIntentSnapshot {
            unknown_live_workflows: vec![replicant_workflow::WorkflowId::new()],
            ..Default::default()
        };
        let result = classify_fixture(&candidate, &devices, &live_snapshot, true);
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::UnknownWorkflowIntent);

        let terminal_snapshot = WorkflowPlacementIntentSnapshot {
            unknown_terminal_outcomes: vec![replicant_workflow::WorkflowId::new()],
            ..Default::default()
        };
        let result = classify_fixture(&candidate, &devices, &terminal_snapshot, true);
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::UnknownTerminalOutcome);
    }

    #[test]
    fn arbitrary_tags_are_not_negative_evidence() {
        let mut candidate = device("d1", "mining_drone", "idle");
        candidate.tags = vec!["operator-note".to_owned()];
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let result = classify_fixture(
            &candidate,
            &devices,
            &WorkflowPlacementIntentSnapshot::default(),
            true,
        );
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(
            result.reason,
            DevicePlacementReason::UnrecognizedPlacementTag
        );
    }

    #[test]
    fn conflicting_containment_topology_is_ambiguous() {
        let mut candidate = device("d1", "mining_drone", "deactivated");
        candidate.relationships.attached_to = Some(DeviceKey::live("A".into()));
        candidate.relationships.stowed_in = Some(DeviceKey::live("B".into()));
        let devices = BTreeMap::from([("D1".to_owned(), candidate.clone())]);
        let result = classify_fixture(
            &candidate,
            &devices,
            &WorkflowPlacementIntentSnapshot::default(),
            true,
        );
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::UnresolvedTopology);

        let mut candidate = device("d1", "mining_drone", "deactivated");
        candidate.relationships.attached_to = Some(DeviceKey::live("CARRIER".into()));
        let mut carrier = device("carrier", "transport_drone", "active");
        carrier.location = Some(LocationKey::live("HOME".into()));
        let devices = BTreeMap::from([
            ("D1".to_owned(), candidate.clone()),
            ("CARRIER".to_owned(), carrier),
        ]);
        let result = classify_fixture(
            &candidate,
            &devices,
            &WorkflowPlacementIntentSnapshot::default(),
            true,
        );
        assert_eq!(result.class, DevicePlacementClass::Ambiguous);
        assert_eq!(result.reason, DevicePlacementReason::UnresolvedTopology);
    }
}
