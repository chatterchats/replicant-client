//! Replicant Space 2.5.2 event-name contract fixtures.
#![cfg(feature = "events")]

#[cfg(feature = "managed")]
use replicant_client::domain::{
    Location as DomainLocation, ObservationTime, Realm, location_detail,
};
use replicant_client::{events::GameEvent, raw::vocab::EventName};

fn event(name: &str, payload: serde_json::Value) -> GameEvent {
    serde_json::from_value(serde_json::json!({
        "id": "252-0",
        "version": 2,
        "category": "system",
        "event": name,
        "replicant_code": null,
        "device_code": null,
        "device_type": null,
        "star": "POLIBUS",
        "location": "POLIBUS-OORT",
        "payload": payload,
        "created_at": "2026-08-25T15:34:33Z"
    }))
    .expect("2.5.2 event fixture")
}

#[test]
fn hub_and_multiplayer_event_names_and_payloads_round_trip() {
    let cases = [
        ("hub.warning", EventName::HubWarning),
        ("hub.maintained", EventName::HubMaintained),
        (
            "multiplayer.replicant_entered",
            EventName::MultiplayerReplicantEntered,
        ),
        (
            "multiplayer.replicant_left",
            EventName::MultiplayerReplicantLeft,
        ),
    ];
    for (wire_name, variant) in cases {
        let decoded: EventName =
            serde_json::from_value(serde_json::json!(wire_name)).expect("documented event name");
        assert_eq!(decoded, variant);
        assert_eq!(
            serde_json::to_value(decoded).expect("event name"),
            wire_name
        );
    }

    let warning = event(
        "hub.warning",
        serde_json::json!({"capacity": 25.0, "warning_type": "wear"}),
    )
    .hub_warning()
    .expect("hub warning payload")
    .expect("matching event");
    assert_eq!(warning.capacity, Some(25.0));
    assert_eq!(warning.warning_type.as_deref(), Some("wear"));

    let maintained = event(
        "hub.maintained",
        serde_json::json!({
            "resources_consumed": {"structural": 50, "carbon": 25},
            "capacity": 67.0
        }),
    )
    .hub_maintained()
    .expect("hub maintained payload")
    .expect("matching event");
    assert_eq!(maintained.resources_consumed["structural"], 50);

    let entered = event(
        "multiplayer.replicant_entered",
        serde_json::json!({"replicant_code": "57F0F6C8", "replicant_name": "Bob-1"}),
    )
    .multiplayer_replicant_entered()
    .expect("multiplayer entered payload")
    .expect("matching event");
    assert_eq!(entered.replicant_name.as_deref(), Some("Bob-1"));

    let left = event(
        "multiplayer.replicant_left",
        serde_json::json!({"replicant_code": "57F0F6C8", "replicant_name": "Bob-1"}),
    )
    .multiplayer_replicant_left()
    .expect("multiplayer left payload")
    .expect("matching event");
    assert_eq!(left.replicant_code.as_deref(), Some("57F0F6C8"));
}

#[test]
fn salvage_event_payloads_preserve_parent_site_resources_and_unknown_fields() {
    let discovered = event(
        "salvage.discovered",
        serde_json::json!({
            "designation": "ROOT-1-SAL-1",
            "location": "ROOT-1-L4",
            "salvage_type": "wreckage",
            "name": "Ancient Wreck",
            "resources": {"structural": 42},
            "future_field": {"nested": true}
        }),
    )
    .salvage_discovered()
    .expect("salvage discovered payload")
    .expect("matching event");
    assert_eq!(discovered.designation.as_deref(), Some("ROOT-1-SAL-1"));
    assert_eq!(discovered.location.as_deref(), Some("ROOT-1-L4"));
    assert_eq!(discovered.resources["structural"], 42);
    assert_eq!(discovered.extra["future_field"]["nested"], true);

    let depleted = event(
        "salvage.depleted",
        serde_json::json!({"site": "ROOT-1-SAL-1", "future": 7}),
    )
    .salvage_depleted()
    .expect("salvage depleted payload")
    .expect("matching event");
    assert_eq!(depleted.site.as_deref(), Some("ROOT-1-SAL-1"));
    assert_eq!(depleted.extra["future"], 7);
}

#[cfg(feature = "managed")]
#[test]
fn location_open_fields_survive_normalization() {
    let mut raw: replicant_client::raw::locations::Location =
        serde_json::from_value(serde_json::json!({
            "location": "SOL-OBJ-1",
            "location_type": "planet",
            "scanned": true,
            "system_scanned": true,
            "system_tags": ["inner"],
            "system": "SOL",
            "parent": "SOL-STAR",
            "planets_total": 4,
            "planets_scanned": 3,
            "moons_total": 2,
            "moons_scanned": 1,
            "moons_total_estimated": false,
            "object": {"designation": "SOL-OBJ-1", "kind": "asteroid"},
            "system_objects": [{"designation": "SOL-OBJ-1"}],
            "lagrange": {"designation": "SOL-L1"},
            "kuiper": {"designation": "SOL-KUIPER"},
            "oort": {"designation": "SOL-OORT"},
            "outer_system": {"designation": "SOL-OUTER"},
            "shops": [{"designation": "SOL-SHOP-1"}],
            "inventory": [{"resource_type": "carbon", "quantity": 12}],
            "star": {"designation": "SOL"},
            "planet": {
                "scanned": true,
                "atmosphere": "thin",
                "surface_gravity": 1.1,
                "future_planet": {"rings": 2}
            },
            "moon": {
                "scanned": false,
                "surface_temp_c": -120.0,
                "future_moon": ["ice", "dust"]
            },
            "future_scalar": 252,
            "future_object": {"enabled": true},
            "future_array": [1, {"nested": "value"}]
        }))
        .expect("2.5.2 location fixture");
    raw.unknown.insert(
        "star".to_owned(),
        serde_json::json!({"designation": "STALE"}),
    );
    raw.unknown
        .insert("system".to_owned(), serde_json::json!("STALE"));

    let normalized = location_detail(&raw, Realm::Live, ObservationTime::now())
        .expect("location normalization")
        .value;

    for field in [
        "object",
        "system_objects",
        "lagrange",
        "kuiper",
        "oort",
        "outer_system",
        "shops",
        "inventory",
        "star",
        "future_scalar",
        "future_object",
        "future_array",
    ] {
        assert!(
            normalized.unknown.contains_key(field),
            "open field {field} was dropped"
        );
    }
    for field in [
        "location",
        "location_type",
        "scanned",
        "system_scanned",
        "system_tags",
        "system",
        "parent",
        "planets_total",
        "planets_scanned",
        "moons_total",
        "moons_scanned",
        "moons_total_estimated",
    ] {
        assert!(
            !normalized.unknown.contains_key(field),
            "normalized field {field} was duplicated"
        );
    }
    assert_eq!(normalized.system.as_deref(), Some("SOL"));
    assert_eq!(normalized.scanned, Some(true));
    assert_eq!(normalized.unknown["star"]["designation"], "SOL");
    assert_eq!(normalized.unknown["planet"]["future_planet"]["rings"], 2);
    assert_eq!(normalized.unknown["moon"]["future_moon"][0], "ice");
    for body in ["planet", "moon"] {
        for field in [
            "scanned",
            "atmosphere",
            "in_habitable_zone",
            "life_stage",
            "magnetic_field",
            "axial_tilt_deg",
            "surface_gravity",
            "surface_temp_c",
        ] {
            assert!(
                normalized.unknown[body].get(field).is_none(),
                "typed {body}.{field} was duplicated"
            );
        }
    }

    let snapshot = serde_json::to_value(&normalized).expect("location snapshot");
    let restored: DomainLocation =
        serde_json::from_value(snapshot.clone()).expect("location snapshot round trip");
    assert_eq!(
        restored.unknown["object"]["designation"], "SOL-OBJ-1",
        "open object field must survive snapshot serialization"
    );

    let mut legacy_snapshot = snapshot;
    legacy_snapshot
        .as_object_mut()
        .expect("location snapshot object")
        .remove("unknown");
    let restored_legacy: DomainLocation =
        serde_json::from_value(legacy_snapshot).expect("legacy location snapshot");
    assert!(restored_legacy.unknown.is_empty());
}
