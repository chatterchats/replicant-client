//! Replicant Space 2.4.0 OpenAPI and rendered-document contract fixtures.
#![cfg(feature = "events")]

use replicant_client::{
    events::GameEvent,
    raw::{
        devices::{DeviceCommand, DeviceCommandResponse},
        replicants::PrintRequest,
    },
};

fn event(name: &str, payload: serde_json::Value) -> GameEvent {
    serde_json::from_value(serde_json::json!({
        "id": "100-0",
        "version": 2,
        "category": "device",
        "event": name,
        "replicant_code": null,
        "device_code": "OBS1",
        "device_type": "galactic_observatory",
        "star": "SOL",
        "location": "SOL-4-L4",
        "payload": payload,
        "created_at": "2026-08-06T15:22:00Z"
    }))
    .expect("2.4.0 event fixture")
}

#[test]
fn triangulate_request_and_documented_response_round_trip() {
    let command = DeviceCommand::Triangulate {
        signature: "a3f7c2e8b1d94f06".to_owned(),
        target: vec![5000.0, 14_000.0, 100.0],
    };
    assert_eq!(
        serde_json::to_value(command).expect("triangulate request"),
        serde_json::json!({
            "command": "triangulate",
            "signature": "a3f7c2e8b1d94f06",
            "target": [5000.0, 14000.0, 100.0]
        })
    );

    let response: DeviceCommandResponse = serde_json::from_value(serde_json::json!({
        "status": "triangulating",
        "signature": "a3f7c2e8b1d94f06",
        "target": [5000, 14000, 100],
        "started_at": "2026-08-05T10:30:00Z",
        "completes_at": "2026-08-05T11:30:00Z"
    }))
    .expect("triangulate response");
    assert_eq!(response.signature.as_deref(), Some("a3f7c2e8b1d94f06"));
    assert_eq!(response.target, Some(vec![5000.0, 14_000.0, 100.0]));
}

#[test]
fn vessel_print_preserves_explicit_flatpack_false() {
    let request = PrintRequest {
        command: Some("print".to_owned()),
        device_type: Some("galactic_observatory".to_owned()),
        flatpack: Some(false),
        notify: None,
    };
    let payload = serde_json::to_value(request).expect("vessel print request");
    assert_eq!(payload["flatpack"], false);
}

#[test]
fn new_event_payloads_decode_through_typed_helpers() {
    let print = event(
        "print.completed",
        serde_json::json!({
            "device_type": "parallax_array",
            "new_device_code": "NEW1",
            "print_mode": "autofactory",
            "consumed_device_codes": ["C1", "C2"],
            "tags": ["fleet-a"]
        }),
    )
    .print_completed()
    .expect("print payload")
    .expect("matching print event");
    assert_eq!(
        print.consumed_device_codes,
        vec!["C1".to_owned(), "C2".to_owned()]
    );
    assert_eq!(print.tags, vec!["fleet-a".to_owned()]);

    let compacting = event(
        "device.compacting",
        serde_json::json!({"completes_at": "2026-08-06T15:22:00Z"}),
    )
    .device_compacting()
    .expect("compacting payload")
    .expect("matching compacting event");
    assert_eq!(
        compacting.completes_at.as_deref(),
        Some("2026-08-06T15:22:00Z")
    );

    let completed = event(
        "triangulation.complete",
        serde_json::json!({
            "signature": "a3f7c2e8b1d94f06",
            "target": [5000, 14000, 100],
            "direction": [0.4, 0.9, 0.0]
        }),
    )
    .triangulation_complete()
    .expect("triangulation payload")
    .expect("matching triangulation event");
    assert_eq!(completed.direction, vec![0.4, 0.9, 0.0]);
}
