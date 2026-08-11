//! Replicant Space 2.5.0 OpenAPI and rendered-document contract fixtures.
#![cfg(feature = "managed")]

use replicant_client::{
    DeviceType,
    events::GameEvent,
    raw::{
        common::UpdateField,
        devices::{
            DeviceCommandResponse, DeviceConfiguration, DeviceConfigurationRequest,
            DeviceConfigurationResponse, DeviceStatus,
        },
        replicants::TeleportRequest,
        tutorials::{TutorialDetail, TutorialListResponse},
    },
};

fn event(name: &str, payload: serde_json::Value) -> GameEvent {
    serde_json::from_value(serde_json::json!({
        "id": "200-0",
        "version": 2,
        "category": "device",
        "event": name,
        "replicant_code": null,
        "device_code": "WARD1",
        "device_type": "system_ward",
        "star": "POLIBUS",
        "location": "POLIBUS-OORT",
        "payload": payload,
        "created_at": "2026-08-11T15:22:00Z"
    }))
    .expect("2.5.0 event fixture")
}

#[test]
fn tutorial_list_and_detail_follow_rendered_contract_and_keep_unknown_fields() {
    let list: TutorialListResponse = serde_json::from_value(serde_json::json!({
        "tutorials": [{
            "slug": "bootstrap",
            "name": "Bootstrap",
            "description": "Learn the basics of commanding your replicant.",
            "order": 1,
            "completed": false,
            "current_step": 3,
            "total_steps": 9,
            "future_summary_field": "kept"
        }],
        "future_list_field": 42
    }))
    .expect("tutorial list");
    assert_eq!(list.tutorials[0].slug.as_deref(), Some("bootstrap"));
    assert_eq!(list.tutorials[0].current_step, Some(3));
    assert_eq!(list.tutorials[0].extra["future_summary_field"], "kept");
    assert_eq!(list.extra["future_list_field"], 42);

    let detail: TutorialDetail = serde_json::from_value(serde_json::json!({
        "slug": "bootstrap",
        "name": "Bootstrap",
        "description": "Learn the basics of commanding your replicant.",
        "current_step": 3,
        "completed": false,
        "steps": [{
            "key": "check_vessel",
            "description": "Check your vessel and list of stowed devices.",
            "hint": "GET /replicants/:code to see your replicant information.",
            "completed": false,
            "current": true,
            "future_step_field": {"value": 1}
        }],
        "future_detail_field": true
    }))
    .expect("tutorial detail");
    assert_eq!(detail.steps[0].key.as_deref(), Some("check_vessel"));
    assert_eq!(detail.steps[0].current, Some(true));
    assert!(detail.steps[0].extra.contains_key("future_step_field"));
    assert_eq!(detail.extra["future_detail_field"], true);
}

#[test]
fn linked_device_configuration_is_tristate() {
    let omitted = serde_json::to_value(DeviceConfigurationRequest::default())
        .expect("omitted linked_device request");
    assert!(omitted["configuration"].get("linked_device").is_none());

    let linked = serde_json::to_value(DeviceConfigurationRequest {
        configuration: DeviceConfiguration::default().link_device("MATRIX1"),
    })
    .expect("linked request");
    assert_eq!(linked["configuration"]["linked_device"], "MATRIX1");

    let unlinked = serde_json::to_value(DeviceConfigurationRequest {
        configuration: DeviceConfiguration::default().unlink_device(),
    })
    .expect("unlink request");
    assert!(unlinked["configuration"]["linked_device"].is_null());

    let decoded: DeviceConfiguration = serde_json::from_value(serde_json::json!({
        "linked_device": null
    }))
    .expect("nullable linked_device");
    assert_eq!(decoded.linked_device, UpdateField::Null);
}

#[test]
fn linked_device_is_retained_in_device_and_configuration_responses() {
    let device: DeviceStatus = serde_json::from_value(serde_json::json!({
        "device_code": "SLING1",
        "device_type": "ftl_slingshot",
        "linked_device": "MATRIX1"
    }))
    .expect("device status");
    assert_eq!(device.linked_device.as_deref(), Some("MATRIX1"));

    let response: DeviceConfigurationResponse = serde_json::from_value(serde_json::json!({
        "device_code": "SLING1",
        "tags": [],
        "linked_device": "MATRIX1",
        "future_configuration_field": true
    }))
    .expect("configuration response");
    assert_eq!(response.linked_device.as_deref(), Some("MATRIX1"));
    assert_eq!(response.extra["future_configuration_field"], true);
}

#[test]
fn slingshot_uses_the_existing_teleport_request_shape() {
    let payload = serde_json::to_value(TeleportRequest {
        target: "SLING1".to_owned(),
    })
    .expect("slingshot teleport request");
    assert_eq!(payload, serde_json::json!({"target": "SLING1"}));
}

#[test]
fn system_ward_response_and_events_decode_typed_fields() {
    let response: DeviceCommandResponse = serde_json::from_value(serde_json::json!({
        "status": "activated",
        "device_code": "WARD1",
        "star": "POLIBUS",
        "location": "POLIBUS-OORT",
        "warding": true,
        "activated": "ward",
        "evicted_miners": 3
    }))
    .expect("ward activation response");
    assert_eq!(response.warding, Some(true));
    assert_eq!(response.activated.as_deref(), Some("ward"));
    assert_eq!(response.evicted_miners, Some(3));

    let activated = event("ward.activated", serde_json::json!({"future": "metadata"}))
        .ward_activated()
        .expect("ward activation payload")
        .expect("matching ward event");
    assert_eq!(activated.extra["future"], "metadata");

    let deactivated = event("ward.deactivated", serde_json::json!({}))
        .ward_deactivated()
        .expect("ward deactivation payload")
        .expect("matching ward event");
    assert!(deactivated.extra.is_empty());
}

#[test]
fn v2_5_and_related_infrastructure_device_types_are_known_values() {
    for (wire, expected) in [
        ("ftl_slingshot", DeviceType::FtlSlingshot),
        ("system_ward", DeviceType::SystemWard),
        ("galactic_observatory", DeviceType::GalacticObservatory),
        ("empty_replicant_matrix", DeviceType::EmptyReplicantMatrix),
        ("heaven_vessel", DeviceType::HeavenVessel),
    ] {
        assert_eq!(DeviceType::from(wire), expected);
        assert_eq!(expected.as_str(), wire);
    }
}
