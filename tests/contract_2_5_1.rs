//! Replicant Space 2.5.1 OpenAPI and rendered-document contract fixtures.
#![cfg(feature = "events")]

use std::time::Duration;

use replicant_client::{
    Error,
    events::GameEvent,
    raw::{
        Client, RetryPolicy, SecretString, Url,
        devices::{DeviceListQuery, DeviceStatus},
        galaxy::{CatalogueStar, StarItem},
        replicants::TravelResponse,
    },
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

fn client(server: &MockServer) -> Client {
    Client::builder()
        .base_url(Url::parse(&server.uri()).expect("mock URL"))
        .authentication_token(SecretString::from("test-token".to_owned()))
        .retry_policy(RetryPolicy {
            max_retries: 0,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            jitter: Duration::ZERO,
        })
        .build()
        .expect("raw client")
}

fn event(name: &str, payload: serde_json::Value) -> GameEvent {
    serde_json::from_value(serde_json::json!({
        "id": "251-0",
        "version": 2,
        "category": "system",
        "event": name,
        "replicant_code": null,
        "device_code": null,
        "device_type": null,
        "star": "POLIBUS",
        "location": "POLIBUS-OORT",
        "payload": payload,
        "created_at": "2026-08-22T22:43:52Z"
    }))
    .expect("2.5.1 event fixture")
}

#[test]
fn star_contract_retains_system_ward_presence() {
    let catalogue: CatalogueStar = serde_json::from_value(serde_json::json!({
        "designation": "POLIBUS",
        "has_hub": false,
        "has_ward": true
    }))
    .expect("catalogue star");
    assert_eq!(catalogue.has_ward, Some(true));

    let nearby: StarItem = serde_json::from_value(serde_json::json!({
        "designation": "POLIBUS",
        "has_ward": false,
        "explored": true
    }))
    .expect("nearby star");
    assert_eq!(nearby.has_ward, Some(false));
}

#[test]
fn travel_and_device_status_decode_new_additive_fields() {
    let travel: TravelResponse = serde_json::from_value(serde_json::json!({
        "arrival_time": "2026-08-23T00:00:00Z",
        "status": "travelling"
    }))
    .expect("travel response");
    assert_eq!(travel.arrival_time.as_deref(), Some("2026-08-23T00:00:00Z"));

    let device: DeviceStatus = serde_json::from_value(serde_json::json!({
        "device_code": "WARD1",
        "short_description": "System ward",
        "description": "Locks local mining and species interactions."
    }))
    .expect("device status");
    assert_eq!(device.short_description.as_deref(), Some("System ward"));
    assert!(
        device
            .description
            .as_deref()
            .is_some_and(|value| value.contains("mining"))
    );
}

#[tokio::test]
async fn device_list_supports_wildcard_include_and_exclude_filters() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/devices"))
        .and(query_param("tags", "squad2:*,*:miners"))
        .and(query_param("exclude_tags", "mission:*"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "devices": [], "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    client(&server)
        .devices()
        .list(&DeviceListQuery {
            tags: Some("squad2:*,*:miners".to_owned()),
            exclude_tags: Some("mission:*".to_owned()),
            ..Default::default()
        })
        .await
        .expect("pattern-filtered device list");

    let conflict = client(&server)
        .devices()
        .list(&DeviceListQuery {
            tag: Some("legacy".to_owned()),
            tags: Some("new:*".to_owned()),
            ..Default::default()
        })
        .await
        .expect_err("tag and tags conflict");
    assert!(matches!(conflict, Error::Configuration { .. }));

    let conflict = client(&server)
        .devices()
        .list(&DeviceListQuery {
            exclude_tags: Some("mission:*".to_owned()),
            untagged: Some(true),
            ..Default::default()
        })
        .await
        .expect_err("untagged and exclude_tags conflict");
    assert!(matches!(conflict, Error::Configuration { .. }));
}

#[test]
fn new_hub_and_multiplayer_events_decode_typed_payloads() {
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
