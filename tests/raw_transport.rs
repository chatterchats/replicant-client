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
    Client, RetryPolicy, SecretString, Url,
    accounts::AccountUpdateRequest,
    devices::{DeviceListQuery, DeviceLogsQuery, DynamicDeviceCommand},
    feedback::FeedbackSubmitRequest,
    inventory::AccountInventoryQuery,
    messages::MessageListQuery,
    replicants::ReplicantListQuery,
};
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{any, header, method, path},
};

fn limited_client(url: Url, limit: usize) -> Client {
    Client::builder()
        .base_url(url)
        .authentication_token(SecretString::from("test-token".to_string()))
        .max_response_body_bytes(limit)
        .build()
        .unwrap()
}

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

#[test]
fn unknown_device_command_round_trips_without_a_client_upgrade() {
    let payload = serde_json::json!({
        "command": "unreleased_command",
        "target": "device-42",
    });
    let command: DynamicDeviceCommand = serde_json::from_value(payload.clone()).unwrap();
    assert_eq!(command.name, "unreleased_command");
    assert_eq!(serde_json::to_value(command).unwrap(), payload);
}

#[test]
fn account_update_never_serializes_the_deprecated_message_notify_field() {
    let payload = serde_json::to_value(AccountUpdateRequest::default()).unwrap();

    assert!(payload.get("message_notify").is_none());
}

#[tokio::test]
async fn response_content_length_over_the_cap_is_rejected_early() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/health"))
        .respond_with(ResponseTemplate::new(200).set_body_string("0123456789"))
        .mount(&server)
        .await;

    let error = limited_client(Url::parse(&server.uri()).unwrap(), 4)
        .health()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("exceeds 4 bytes"));
}

#[tokio::test]
async fn oversized_chunked_response_is_rejected_while_streaming() {
    use tokio::io::AsyncWriteExt as _;
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/json\r\n\r\n4\r\n1234\r\n5\r\n56789\r\n0\r\n\r\n",
            )
            .await
            .unwrap();
    });

    let error = limited_client(Url::parse(&format!("http://{address}")).unwrap(), 4)
        .health()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("exceeds 4 bytes"));
    server.await.unwrap();
}

#[tokio::test]
async fn star_catalogue_uses_its_dedicated_larger_response_limit() {
    let server = MockServer::start().await;
    let padding = "x".repeat(1024 * 1024);
    let body = serde_json::json!({
        "generated_at": "2026-07-27T00:00:00Z",
        "stars": [],
        "padding": padding,
    })
    .to_string();
    assert!(body.len() > 1024 * 1024);

    Mock::given(method("GET"))
        .and(path("/v1/stars"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server)
        .await;

    let response = client(&server, fast_retry())
        .galaxy()
        .catalogue()
        .await
        .unwrap();
    assert!(response.value.stars.is_empty());
}

#[tokio::test]
async fn star_catalogue_response_limit_remains_configurable_and_bounded() {
    let server = MockServer::start().await;
    let body = serde_json::json!({
        "stars": [],
        "padding": "x".repeat(2048),
    })
    .to_string();

    Mock::given(method("GET"))
        .and(path("/v1/stars"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server)
        .await;

    let error = Client::builder()
        .base_url(Url::parse(&server.uri()).unwrap())
        .authentication_token(SecretString::from("test-token".to_string()))
        .max_star_catalogue_response_body_bytes(1024)
        .build()
        .unwrap()
        .galaxy()
        .catalogue()
        .await
        .unwrap_err();

    assert!(error.to_string().contains("exceeds 1024 bytes"));
}

#[tokio::test]
async fn account_wipe_uses_its_documented_typed_success_response() {
    let server = MockServer::start().await;
    Mock::given(method("DELETE"))
        .and(path("/v1/accounts/me"))
        .respond_with(
            ResponseTemplate::new(202)
                .set_body_json(serde_json::json!({"message": "wipe accepted"})),
        )
        .mount(&server)
        .await;

    let response = client(&server, fast_retry())
        .accounts()
        .request_destructive_wipe()
        .await
        .unwrap();
    assert_eq!(response.value.message.as_deref(), Some("wipe accepted"));
}

#[tokio::test]
async fn documented_page_limits_fail_locally_before_a_request() {
    let server = MockServer::start().await;
    let error = client(&server, fast_retry())
        .devices()
        .logs(
            "device-42",
            &DeviceLogsQuery {
                limit: Some(101),
                ..DeviceLogsQuery::default()
            },
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("between 1 and 100"));
    assert!(server.received_requests().await.unwrap().is_empty());
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
