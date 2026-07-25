//! Managed-read commit-before-return integration checks.
#![cfg(feature = "managed")]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use replicant_client::{Client, SecretString, raw::Url};
use wiremock::{
    Mock, MockServer, Request, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn targeted_device_read_fetches_once_and_is_visible_before_return() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/v1/devices/D-1"))
        .respond_with({
            let calls = Arc::clone(&calls);
            move |_: &Request| {
                calls.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "device_code": "D-1", "device_type": "mining_drone", "status": "idle"
                }))
            }
        })
        .mount(&server)
        .await;

    let client = Client::builder()
        .base_url(Url::parse(&server.uri()).unwrap())
        .authentication_token(SecretString::from("test-token"))
        .in_memory()
        .startup_policy(replicant_client::StartupPolicy::RestoreOnly)
        .start()
        .await
        .unwrap();

    let handle = client.devices().get("D-1").await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(handle.snapshot().await.unwrap().key.id.as_str(), "D-1");
    assert!(client.devices().cached("D-1").is_some());
}
