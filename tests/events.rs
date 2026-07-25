//! Raw account event log and SSE contract checks.
#![cfg(feature = "events")]

use futures::StreamExt as _;
use replicant_client::{
    events::EventLogQuery,
    raw::{Client, SecretString, Url},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{header, method, path, query_param},
};

fn client(server: &MockServer) -> Client {
    Client::builder()
        .base_url(Url::parse(&server.uri()).unwrap())
        .authentication_token(SecretString::from("test-token".to_string()))
        .build()
        .unwrap()
}

#[tokio::test]
async fn event_log_uses_string_event_id_cursor_and_filtered_flag() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/events"))
        .and(query_param("cursor", "1752681600000-0"))
        .and(query_param("filtered", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "events": [], "next_cursor": "1752681620000-0"
        })))
        .mount(&server)
        .await;

    let response = client(&server)
        .events()
        .list(&EventLogQuery {
            cursor: Some("1752681600000-0".into()),
            filtered: Some(true),
            ..EventLogQuery::default()
        })
        .await
        .unwrap();
    assert_eq!(
        response.value.next_cursor.as_deref(),
        Some("1752681620000-0")
    );
}

#[tokio::test]
async fn sse_parses_frames_and_sends_reconnect_cursor() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/events/stream"))
        .and(query_param("cursor", "1752681600000-0"))
        .and(header("accept", "text/event-stream"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "text/event-stream")
                .set_body_string(
                    ": keepalive\n\nid: 1752681620000-0\nevent: future.arrived\ndata: {\"id\":\"1752681620000-0\",\"version\":1,\"category\":\"future\",\"event\":\"future.arrived\",\"replicant_code\":null,\"device_code\":null,\"device_type\":null,\"star\":null,\"location\":null,\"payload\":{\"unknown\":true},\"created_at\":\"2026-07-16T10:03:20Z\"}\n\n",
                ),
        )
        .mount(&server)
        .await;

    let mut stream = client(&server)
        .events()
        .stream(Some("1752681600000-0"))
        .await
        .unwrap();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.event, "future.arrived");
    assert_eq!(event.payload["unknown"], true);
}
