#![cfg(feature = "managed")]
#![allow(missing_docs)]

use std::collections::HashMap;

use replicant_client::{domain::*, raw};
use serde_json::json;

fn raw_device(code: &str) -> raw::devices::DeviceStatus {
    serde_json::from_value(json!({
        "device_code": code, "device_type": "mining_drone", "status": "active",
        "location": "SOL", "features": ["mining", "future_feature"],
        "available_commands": ["activate", "future_command"]
    }))
    .expect("valid raw fixture")
}

fn raw_replicant() -> raw::replicants::ReplicantStatus {
    serde_json::from_value(json!({
        "replicant_code": "R-1", "name": "Syl", "description": "private",
        "pronouns": "they/them", "experience_points": 7, "status": "active"
    }))
    .expect("valid raw fixture")
}

fn raw_device_list() -> raw::devices::DeviceListResponse {
    serde_json::from_value(json!({
        "devices": [{
            "device_code": "D-1", "device_type": "mining_drone", "status": "active",
            "location": "SOL", "features": ["mining", "future_feature"],
            "available_commands": ["activate", "future_command"]
        }],
        "next_cursor": null
    }))
    .expect("valid raw list fixture")
}

fn authority_of<T>(observation: Observation<T>) -> ObservationAuthority {
    observation.metadata.authority
}

#[test]
fn endpoint_authority_is_explicit_and_table_driven() {
    let device = raw_device("D-1");
    let list = raw_device_list();
    let overview: raw::locations::LocationSystemMap =
        serde_json::from_value(json!({"locations":{"SOL":{"devices":1,"replicants":2}}})).unwrap();
    let account: raw::accounts::AccountMeResponse =
        serde_json::from_value(json!({"name":"Account"})).unwrap();
    let inventory: raw::inventory::LocationInventory =
        serde_json::from_value(json!({"location":"SOL","items":[]})).unwrap();
    let star: raw::galaxy::CatalogueStar =
        serde_json::from_value(json!({"designation":"SOL"})).unwrap();
    let event: replicant_client::events::GameEvent = serde_json::from_value(json!({
        "id":"E-1", "version":1, "category":"device", "event":"device.decommissioned",
        "created_at":"2026-01-01T00:00:00Z"
    }))
    .unwrap();
    let simulation: raw::simulations::SimulationEnterResponse = serde_json::from_value(json!({
        "simulation_id":1, "starting_location":"SOL", "starting_star":"SOL"
    }))
    .unwrap();
    let cases = [
        (
            "account",
            authority_of(account_me(&account, AccountId::from("A-1"), "2026-01-01Z")),
            false,
        ),
        (
            "device detail",
            device_detail(&device, Realm::Live, AccessScope::Owned, "2026-01-01Z")
                .unwrap()
                .metadata
                .authority,
            false,
        ),
        (
            "device list member",
            device_list_member(&device, Realm::Live, AccessScope::Owned, "2026-01-01Z")
                .unwrap()
                .metadata
                .authority,
            false,
        ),
        (
            "device full traversal",
            device_collection(&list, Realm::Live, false, true, "2026-01-01Z")
                .unwrap()
                .metadata
                .authority,
            true,
        ),
        (
            "replicant devices",
            replicant_device_collection(&list, Realm::Live, AccessScope::Owned, "2026-01-01Z")
                .unwrap()
                .metadata
                .authority,
            false,
        ),
        (
            "location overview",
            location_overview(&overview, Realm::Live, "2026-01-01Z")
                .metadata
                .authority,
            false,
        ),
        (
            "inventory",
            authority_of(
                location_inventory(
                    &inventory,
                    InventoryOwner::Account(AccountId::from("A-1")),
                    Realm::Live,
                    "2026-01-01Z",
                )
                .unwrap(),
            ),
            false,
        ),
        (
            "catalogue",
            authority_of(catalogue_star(&star, Realm::Live, "2026-01-01Z").unwrap()),
            true,
        ),
        (
            "event",
            authority_of(account_event(&event, None, "2026-01-01Z")),
            false,
        ),
        (
            "simulation",
            authority_of(simulation_start(&simulation, "2026-01-01Z").unwrap()),
            false,
        ),
    ];
    assert_eq!(cases[0].1, ObservationAuthority::EntitySnapshot);
    assert_eq!(cases[1].1, ObservationAuthority::EntitySnapshot);
    assert_eq!(cases[2].1, ObservationAuthority::CollectionMember);
    assert_eq!(cases[3].1, ObservationAuthority::CompleteCollection);
    assert!(cases[3].2);
    assert!(
        cases
            .iter()
            .filter(|case| !case.2)
            .all(|case| case.1 != ObservationAuthority::CompleteCollection)
    );
}

#[test]
fn device_lists_are_full_entities_but_only_full_unfiltered_traversals_reconcile() {
    let list = raw_device_list();
    let member = device_list_member(
        &list.devices[0],
        Realm::Live,
        AccessScope::Owned,
        "2026-01-01Z",
    )
    .unwrap();
    assert_eq!(member.value.status, Some(DeviceStatus::Active));
    assert!(matches!(
        member.value.available_commands[1],
        DeviceCommand::Unknown(_)
    ));
    assert!(!collection_can_tombstone(
        &device_collection(&list, Realm::Live, true, true, "2026-01-01Z").unwrap()
    ));
    assert!(collection_can_tombstone(
        &device_collection(&list, Realm::Live, false, true, "2026-01-01Z").unwrap()
    ));
}

#[test]
fn visibility_scoped_collections_never_tombstone() {
    let list = raw_device_list();
    let overview: raw::locations::LocationSystemMap =
        serde_json::from_value(json!({"locations":{"SOL":{}}})).unwrap();
    assert!(!collection_can_tombstone(
        &replicant_device_collection(&list, Realm::Live, AccessScope::Owned, "2026-01-01Z")
            .unwrap()
    ));
    assert!(
        !location_overview(&overview, Realm::Live, "2026-01-01Z")
            .completeness
            .can_reconcile_membership()
    );
}

#[test]
fn public_profiles_cannot_clear_owned_private_data() {
    let raw = raw_replicant();
    let owned = owned_replicant_detail(&raw, Realm::Live, "2026-01-01T00:00:00Z").unwrap();
    let public = public_replicant_detail(&raw, Realm::Live, "2026-01-02T00:00:00Z").unwrap();
    let MergeOutcome::Replaced(merged) = merge_replicant(owned, public) else {
        panic!("public profile should update public fields")
    };
    assert_eq!(
        merged.value.private.unwrap().description.as_deref(),
        Some("private")
    );
}

#[test]
fn unknown_values_round_trip() {
    let value = DeviceCommand::from("unreleased_command");
    assert_eq!(value.as_str(), "unreleased_command");
    assert_eq!(
        serde_json::to_string(&value).unwrap(),
        "\"unreleased_command\""
    );
    assert!(matches!(
        serde_json::from_str::<DeviceCommand>("\"unreleased_command\"").unwrap(),
        DeviceCommand::Unknown(_)
    ));
}

#[test]
fn realms_make_identical_server_codes_distinct() {
    let live = DeviceKey::live(DeviceId::from("D-1"));
    let simulated = DeviceKey::in_realm(
        Realm::Simulation(SimulationId::new(4)),
        DeviceId::from("D-1"),
    );
    let mut values = HashMap::new();
    values.insert(live, "live");
    values.insert(simulated, "simulation");
    assert_eq!(values.len(), 2);
}

#[test]
fn only_authoritative_removal_evidence_can_tombstone() {
    let metadata = ObservationMetadata {
        source: ObservationSource::RestDetail,
        authority: ObservationAuthority::EntitySnapshot,
        observed_at: "2026-01-01Z".into(),
        access: AccessScope::Owned,
        reachability: Reachability::Reachable,
        stale: false,
        source_document: SourceDocument {
            operation: "DELETE /v1/devices/{device_code}".into(),
            request_id: None,
            document_id: None,
        },
    };
    assert!(tombstone_eligible(
        &metadata,
        &RemovalEvidence::ExplicitDecommission
    ));
    assert!(!tombstone_eligible(&metadata, &RemovalEvidence::NotFound));
    assert!(!tombstone_eligible(&metadata, &RemovalEvidence::Absence));
}
