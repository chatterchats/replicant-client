//! Raw transport behavior and service-family contract fixtures.
#![cfg(feature = "raw")]

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use replicant_client::raw::{
    Client, RetryPolicy, SecretString, Url, devices::DeviceListQuery,
    feedback::FeedbackSubmitRequest, inventory::AccountInventoryQuery, messages::MessageListQuery,
    replicants::ReplicantListQuery,
};
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{any, header, method, path},
};

fn client(server: &MockServer, retry: RetryPolicy) -> Client {
    Client::builder()
        .base_url(Url::parse(&server.uri()).unwrap())
        .authentication_token(SecretString::from("test-token".to_string()))
        .retry_policy(retry)
        .build()
        .unwrap()
}

fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        max_retries: 2,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(2),
        jitter: Duration::ZERO,
    }
}

#[tokio::test]
async fn safe_read_retries_transient_status() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/v1/health"))
        .respond_with({
            let calls = calls.clone();
            move |_: &Request| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(503).set_body_json(serde_json::json!({"error": "busy"}))
                } else {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true}))
                }
            }
        })
        .mount(&server)
        .await;

    assert_eq!(
        client(&server, fast_retry()).health().await.unwrap().value["ok"],
        true
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn retry_after_is_accepted_for_safe_reads() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/v1/health"))
        .respond_with({
            let calls = calls.clone();
            move |_: &Request| {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(429)
                        .insert_header("Retry-After", "0")
                        .set_body_json(serde_json::json!({"error": "slow down"}))
                } else {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true}))
                }
            }
        })
        .mount(&server)
        .await;

    client(&server, fast_retry()).health().await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn unsafe_timeout_is_ambiguous_and_never_retried() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/feedback"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(100))
                .set_body_json(serde_json::json!({"status": "ok"})),
        )
        .mount(&server)
        .await;
    let client = Client::builder()
        .base_url(Url::parse(&server.uri()).unwrap())
        .authentication_token(SecretString::from("test-token".to_string()))
        .request_timeout(Duration::from_millis(20))
        .retry_policy(fast_retry())
        .build()
        .unwrap();

    let error = client
        .feedback()
        .submit(&FeedbackSubmitRequest {
            body: Some("test".into()),
            ..FeedbackSubmitRequest::default()
        })
        .await
        .unwrap_err();
    assert!(error.is_ambiguous_transport_failure());
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn authentication_errors_redact_server_echoes() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/accounts/me"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "token": "test-token",
            "message": "Authorization: Bearer test-token"
        })))
        .mount(&server)
        .await;

    let error = client(&server, fast_retry())
        .accounts()
        .me()
        .await
        .unwrap_err();
    let rendered = format!("{error:?}");
    assert!(!rendered.contains("test-token"));
    assert!(rendered.contains("<redacted>"));
}

#[tokio::test]
async fn every_raw_service_family_decodes_a_contract_fixture() {
    let server = MockServer::start().await;
    Mock::given(any())
        .respond_with(|request: &Request| {
            let body = match request.url.path() {
                "/v1/accounts/me" | "/v1/locations" | "/v1/devices/DEV/trades" => {
                    serde_json::json!({})
                }
                "/v1/devices" => serde_json::json!({"devices": [], "next_cursor": null}),
                "/v1/replicants" => serde_json::json!({"replicants": [], "next_cursor": null}),
                "/v1/achievements" => serde_json::json!({"achievements": []}),
                "/v1/blueprints" => serde_json::json!({"blueprints": []}),
                "/v1/devices/DEV/channels" => serde_json::json!({"channels": []}),
                "/v1/feedback" => serde_json::json!({"status": "ok"}),
                "/v1/stars" => serde_json::json!({"stars": []}),
                "/v1/inventory" => serde_json::json!({"locations": [], "next_cursor": null}),
                "/v1/leaderboards" => serde_json::json!({"boards": []}),
                "/v1/locations/LOC/events" => {
                    serde_json::json!({"events": [], "next_cursor": null})
                }
                "/v1/messages" => serde_json::json!({"messages": [], "next_cursor": null}),
                "/v1/replicants/REP/reputation" => serde_json::json!({"reputation": []}),
                "/v1/devices/DEV/simulate" => serde_json::json!({"scenarios": []}),
                "/v1/species" => serde_json::json!({"species": []}),
                other => panic!("missing family fixture for {other}"),
            };
            ResponseTemplate::new(200).set_body_json(body)
        })
        .mount(&server)
        .await;
    let client = client(&server, fast_retry());
    client
        .rate_limits()
        .set_policy(
            replicant_client::raw::rate_limit::RateLimitBucket::Read,
            replicant_client::raw::rate_limit::RateLimitPolicy {
                capacity: 10_000,
                refill_every: Duration::from_millis(1),
            },
        )
        .await;

    client.accounts().me().await.unwrap();
    client.achievements().list().await.unwrap();
    client.blueprints().list().await.unwrap();
    client.bobnet().channels("DEV").await.unwrap();
    client
        .devices()
        .list(&DeviceListQuery {
            cursor: Some(7),
            ..DeviceListQuery::default()
        })
        .await
        .unwrap();
    client
        .feedback()
        .submit(&FeedbackSubmitRequest::default())
        .await
        .unwrap();
    client.galaxy().catalogue().await.unwrap();
    client
        .inventory()
        .list(&AccountInventoryQuery::default())
        .await
        .unwrap();
    client.leaderboards().index().await.unwrap();
    client.location_events().list("LOC", None).await.unwrap();
    client.locations().system_map().await.unwrap();
    client
        .messages()
        .list(&MessageListQuery::default())
        .await
        .unwrap();
    client
        .replicants()
        .list(&ReplicantListQuery::default())
        .await
        .unwrap();
    client.reputation().for_replicant("REP").await.unwrap();
    client.simulations().scenarios("DEV").await.unwrap();
    client.species().list().await.unwrap();
    client.trading().list("DEV").await.unwrap();

    let requests = server.received_requests().await.unwrap();
    let device_request = requests
        .iter()
        .find(|request| request.url.path() == "/v1/devices")
        .unwrap();
    assert_eq!(device_request.url.query(), Some("cursor=7"));
}
