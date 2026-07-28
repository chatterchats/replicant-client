//! Replicant Space 2.3.3 OpenAPI corpus fixtures.
#![cfg(feature = "events")]

use std::time::Duration;

use replicant_client::{
    Error,
    events::GameEvent,
    raw::{
        Client, RetryPolicy, SecretString, Url,
        blueprints::Blueprint,
        devices::{DeviceListQuery, DeviceStatus},
        galaxy::{CatalogueStar, StarItem},
        leaderboards::LeaderboardEntry,
    },
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
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

#[test]
fn refreshed_openapi_dtos_decode_changed_fields_and_ignore_unknowns() {
    let catalogue: CatalogueStar = serde_json::from_value(serde_json::json!({
        "designation": "NUNKA",
        "region": "alpha",
        "has_hub": true
    }))
    .expect("catalogue star");
    assert_eq!(catalogue.region.as_deref(), Some("alpha"));
    assert_eq!(catalogue.has_hub, Some(true));

    let census: StarItem = serde_json::from_value(serde_json::json!({
        "designation": "NUNKA",
        "region": "alpha",
        "has_hub": false,
        "explored": true
    }))
    .expect("stellar census item");
    assert_eq!(census.region.as_deref(), Some("alpha"));
    assert_eq!(census.has_hub, Some(false));

    let vessel: DeviceStatus = serde_json::from_value(serde_json::json!({
        "device_code": "VESSEL",
        "replicant_code": "OWNER",
        "hosting_replicant": {"replicant_code": "MATRIX"},
        "printing": {"eta_seconds": 12},
        "scan": {"eta_seconds": 7},
        "travel": {"eta_seconds": 5, "route_eta_seconds": 9},
        "future_openapi_field": true
    }))
    .expect("device status");
    assert_eq!(vessel.replicant_code.as_deref(), Some("OWNER"));
    assert_eq!(vessel.hosting_replicant.as_deref(), Some("MATRIX"));
    assert_eq!(
        vessel.printing.and_then(|value| value.eta_seconds),
        Some(12.0)
    );
    assert_eq!(vessel.scan.and_then(|value| value.eta_seconds), Some(7.0));
    let travel = vessel.travel.expect("travel status");
    assert_eq!(travel.eta_seconds, Some(5.0));
    assert_eq!(travel.route_eta_seconds, Some(9.0));

    let entry: LeaderboardEntry = serde_json::from_value(serde_json::json!({
        "designation": "NUNKA-3",
        "future_openapi_field": true
    }))
    .expect("leaderboard entry");
    assert_eq!(entry.designation.as_deref(), Some("NUNKA-3"));
}

#[test]
fn integer_blueprint_print_time_remains_source_compatible() {
    let blueprint: Blueprint = serde_json::from_value(serde_json::json!({
        "device_type": "survey_drone",
        "print_time": 600
    }))
    .expect("blueprint");
    assert_eq!(blueprint.print_time, Some(600.0));
}

#[test]
fn typed_event_helpers_decode_2_3_3_payloads() {
    let digest: GameEvent = serde_json::from_value(serde_json::json!({
        "id": "10-0",
        "version": 1,
        "category": "ami",
        "event": "ami.survey.digest",
        "created_at": "2026-07-27T00:00:00Z",
        "payload": {
            "directive": "survey_system",
            "activity": {"event_count": 1, "counts": {"scan.completed": 1}},
            "report": {
                "progress": {"remaining": 0, "scanned": 4, "total": 4},
                "scans": [{
                    "device_code": "DRONE",
                    "scan_target": "NUNKA-3",
                    "scan_type": "planet",
                    "report": {"planet": {"designation": "NUNKA-3"}}
                }]
            }
        }
    }))
    .expect("digest envelope");
    let payload = digest
        .ami_survey_digest()
        .expect("typed digest")
        .expect("matching event");
    let report = payload.report.expect("survey report");
    assert_eq!(report.progress.expect("progress").remaining, Some(0));
    assert_eq!(report.scans[0].scan_target.as_deref(), Some("NUNKA-3"));
    assert_eq!(report.scans[0].report["planet"]["designation"], "NUNKA-3");

    let unlocked: GameEvent = serde_json::from_value(serde_json::json!({
        "id": "11-0",
        "version": 1,
        "category": "blueprint",
        "event": "blueprint.unlocked",
        "created_at": "2026-07-27T00:00:01Z",
        "payload": {
            "device_type": "ftl_relay",
            "resources": {"structural": 80},
            "components": null,
            "print_time": 600,
            "requires_autofactory": false
        }
    }))
    .expect("blueprint event envelope");
    let payload = unlocked
        .blueprint_unlocked()
        .expect("typed blueprint payload")
        .expect("matching event");
    assert_eq!(payload.print_time, Some(600.0));
    assert_eq!(payload.resources["structural"], 80);
}

#[tokio::test]
async fn device_list_sends_new_tag_filters_and_rejects_conflicts() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/devices"))
        .and(query_param("tag", "survey"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "devices": [], "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/devices"))
        .and(query_param("untagged", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "devices": [], "next_cursor": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = client(&server);
    client
        .devices()
        .list(&DeviceListQuery {
            tag: Some("survey".to_owned()),
            ..Default::default()
        })
        .await
        .expect("tagged device list");
    client
        .devices()
        .list(&DeviceListQuery {
            untagged: Some(true),
            ..Default::default()
        })
        .await
        .expect("untagged device list");

    let error = client
        .devices()
        .list(&DeviceListQuery {
            tag: Some("survey".to_owned()),
            untagged: Some(true),
            ..Default::default()
        })
        .await
        .expect_err("tag and untagged conflict");
    assert!(matches!(error, Error::Configuration { .. }));
}

#[tokio::test]
async fn colony_leaderboards_use_the_openapi_paths() {
    let server = MockServer::start().await;
    for board in ["colony_moon", "colony_planet"] {
        Mock::given(method("GET"))
            .and(path(format!("/v1/leaderboards/{board}")))
            .and(header("authorization", "Bearer test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "board": board,
                "entries": []
            })))
            .expect(1)
            .mount(&server)
            .await;
    }

    let client = client(&server);
    assert_eq!(
        client
            .leaderboards()
            .colony_moon()
            .await
            .expect("moon leaderboard")
            .value
            .board
            .as_deref(),
        Some("colony_moon")
    );
    assert_eq!(
        client
            .leaderboards()
            .colony_planet()
            .await
            .expect("planet leaderboard")
            .value
            .board
            .as_deref(),
        Some("colony_planet")
    );
}
