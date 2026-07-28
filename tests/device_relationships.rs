//! Device ownership and matrix-hosting semantics.
#![cfg(feature = "managed")]

use replicant_client::{
    domain::{self, AccessScope, MergeOutcome, Realm},
    raw,
};
use serde_json::json;

fn device(owner: Option<&str>, hosted: Option<&str>) -> raw::devices::DeviceStatus {
    serde_json::from_value(json!({
        "device_code": "VESSEL",
        "device_type": "vessel",
        "replicant_code": owner,
        "hosting_replicant": hosted,
    }))
    .expect("valid device fixture")
}

fn observation(
    owner: Option<&str>,
    hosted: Option<&str>,
    observed_at: &str,
) -> domain::Observation<domain::Device> {
    domain::device_detail(
        &device(owner, hosted),
        Realm::Live,
        AccessScope::Owned,
        observed_at,
    )
    .expect("device normalizes")
}

#[test]
fn device_assignment_and_matrix_hosting_are_independent() {
    let vessel = observation(Some("OWNER"), Some("MATRIX"), "2026-01-01T00:00:00Z");
    assert_eq!(
        vessel
            .value
            .relationships
            .assigned_replicant
            .as_ref()
            .map(|key| key.id.as_str()),
        Some("OWNER")
    );
    assert_eq!(
        vessel
            .value
            .relationships
            .hosting_replicant
            .as_ref()
            .map(|key| key.id.as_str()),
        Some("MATRIX")
    );

    let drone = observation(Some("OWNER"), None, "2026-01-01T00:00:00Z");
    assert!(drone.value.relationships.assigned_replicant.is_some());
    assert!(drone.value.relationships.hosting_replicant.is_none());
}

#[test]
fn transfers_change_only_the_relationship_the_server_reports() {
    let initial = observation(Some("OWNER"), Some("MATRIX"), "2026-01-01T00:00:00Z");
    let owner_transfer = observation(Some("NEW-OWNER"), Some("MATRIX"), "2026-01-02T00:00:00Z");
    assert_eq!(
        owner_transfer
            .value
            .relationships
            .assigned_replicant
            .as_ref()
            .map(|key| key.id.as_str()),
        Some("NEW-OWNER")
    );
    assert_eq!(
        owner_transfer.value.relationships.hosting_replicant,
        initial.value.relationships.hosting_replicant
    );

    let matrix_transfer = observation(
        Some("NEW-OWNER"),
        Some("NEW-MATRIX"),
        "2026-01-03T00:00:00Z",
    );
    assert_eq!(
        matrix_transfer.value.relationships.assigned_replicant,
        owner_transfer.value.relationships.assigned_replicant
    );
    assert_eq!(
        matrix_transfer
            .value
            .relationships
            .hosting_replicant
            .as_ref()
            .map(|key| key.id.as_str()),
        Some("NEW-MATRIX")
    );
}

#[test]
fn public_observations_do_not_erase_owned_device_relationships() {
    let owned = observation(Some("OWNER"), Some("MATRIX"), "2026-01-01T00:00:00Z");
    let mut public = observation(None, None, "2026-01-02T00:00:00Z");
    public.metadata.access = AccessScope::Public;
    public.value.access = AccessScope::Public;
    let merged = match domain::merge_device(owned, public) {
        MergeOutcome::Replaced(value) | MergeOutcome::Retained(value, _) => value,
        _ => panic!("unknown device merge outcome"),
    };
    assert_eq!(
        merged
            .value
            .relationships
            .assigned_replicant
            .as_ref()
            .map(|key| key.id.as_str()),
        Some("OWNER")
    );
    assert_eq!(
        merged
            .value
            .relationships
            .hosting_replicant
            .as_ref()
            .map(|key| key.id.as_str()),
        Some("MATRIX")
    );
}
