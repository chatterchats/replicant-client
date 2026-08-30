//! Entity-scoped validation for mining workflows.
//!
//! Validation deliberately starts with the managed projection.  A targeted
//! refresh is only used when that projection is absent, stale, or cannot be
//! trusted because event continuity is incomplete.  Refreshes are coalesced
//! per entity, while unrelated entities remain independent.

use std::{
    collections::HashMap,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, Instant},
};

use replicant_client::{
    Client, Device, Replicant,
    domain::{Location, ObservationMetadata},
    managed::ReadinessComponent,
};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use super::AnyResult;

const VALIDATION_TTL: Duration = Duration::from_secs(60);

/// Why a mining operation needs an entity validated.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ValidationReason {
    /// A normal mutation can use a current, continuously observed projection.
    Mutation,
    /// Capacity or inventory decisions require an authoritative read.
    CapacitySensitive,
    /// An event gap means the projection cannot establish current state.
    EventGap,
    /// A conflicting state requires an authoritative read to resolve it.
    StateConflict,
}

impl ValidationReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Mutation => "mutation",
            Self::CapacitySensitive => "capacity_sensitive",
            Self::EventGap => "event_gap",
            Self::StateConflict => "state_conflict",
        }
    }

    const fn requires_refresh(self) -> bool {
        !matches!(self, Self::Mutation)
    }

    /// Event gaps and conflicting state invalidate even a recent successful
    /// validation snapshot; capacity-sensitive checks may reuse that snapshot.
    const fn bypasses_cache(self) -> bool {
        matches!(self, Self::EventGap | Self::StateConflict)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum EntityKind {
    Device,
    Replicant,
    Location,
}

impl EntityKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Device => "device",
            Self::Replicant => "replicant",
            Self::Location => "location",
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ValidationKey {
    kind: EntityKind,
    id: String,
}

impl ValidationKey {
    fn new(kind: EntityKind, id: &str) -> Self {
        Self {
            kind,
            id: id.to_owned(),
        }
    }
}

#[derive(Debug)]
struct Flight {
    /// A watch channel retains completion even when the leader finishes before
    /// a waiter gets polled, unlike a bare `Notify`.
    completion: watch::Sender<bool>,
}

impl Flight {
    fn new() -> Self {
        let (completion, _) = watch::channel(false);
        Self { completion }
    }

    fn subscribe(&self) -> watch::Receiver<bool> {
        self.completion.subscribe()
    }
}

#[derive(Default)]
struct Coordinator {
    flights: HashMap<ValidationKey, Arc<Flight>>,
    cache: HashMap<ValidationKey, Instant>,
}

static COORDINATOR: LazyLock<Mutex<Coordinator>> =
    LazyLock::new(|| Mutex::new(Coordinator::default()));

fn coordinator() -> &'static Mutex<Coordinator> {
    &COORDINATOR
}

fn lock_coordinator() -> std::sync::MutexGuard<'static, Coordinator> {
    coordinator()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Removes a flight and wakes every waiter, including when the leader task is
/// cancelled while the managed gateway is doing I/O.
struct FlightGuard {
    key: ValidationKey,
    flight: Arc<Flight>,
    finished: bool,
}

impl FlightGuard {
    fn new(key: ValidationKey, flight: Arc<Flight>) -> Self {
        Self {
            key,
            flight,
            finished: false,
        }
    }

    fn finish(mut self) {
        self.finished = true;
        let mut state = lock_coordinator();
        if state
            .flights
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.flight))
        {
            state.flights.remove(&self.key);
        }
        self.flight.completion.send_replace(true);
    }
}

impl Drop for FlightGuard {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut state = lock_coordinator();
        if state
            .flights
            .get(&self.key)
            .is_some_and(|current| Arc::ptr_eq(current, &self.flight))
        {
            state.flights.remove(&self.key);
        }
        self.flight.completion.send_replace(true);
    }
}

fn recently_validated(key: &ValidationKey) -> bool {
    let mut state = lock_coordinator();
    if state
        .cache
        .get(key)
        .is_some_and(|stored| stored.elapsed() < VALIDATION_TTL)
    {
        return true;
    }
    state.cache.remove(key);
    false
}

fn cache_success(key: ValidationKey) {
    lock_coordinator().cache.insert(key, Instant::now());
}

fn flight_for(key: &ValidationKey) -> (Arc<Flight>, bool) {
    let mut state = lock_coordinator();
    if let Some(flight) = state.flights.get(key) {
        return (Arc::clone(flight), true);
    }
    let flight = Arc::new(Flight::new());
    state.flights.insert(key.clone(), Arc::clone(&flight));
    (flight, false)
}

fn readiness_healthy(client: &Client) -> bool {
    let readiness = client.readiness();
    matches!(
        (
            readiness.local_restoration,
            readiness.store_health,
            readiness.event_catchup,
            readiness.sse_connectivity,
        ),
        (
            ReadinessComponent::Ready,
            ReadinessComponent::Ready,
            ReadinessComponent::Ready,
            ReadinessComponent::Ready,
        )
    )
}

fn metadata_stale(metadata: &ObservationMetadata) -> Option<&'static str> {
    if metadata.stale {
        return Some("stale_flag");
    }
    let now = replicant_client::domain::ObservationTime::now().unix_millis();
    let age_ms = now.saturating_sub(metadata.observed_at.unix_millis());
    if age_ms > VALIDATION_TTL.as_millis() as i64 {
        return Some("age_exceeded");
    }
    None
}

fn emit_managed(key: &ValidationKey) {
    debug!(
        target: "replicant_runtime::mining::validation",
        event = "mining.validation.managed_state_used",
        resource_kind = key.kind.as_str(),
        entity_key = %key.id,
        "used managed mining validation projection"
    );
}

fn emit_join(key: &ValidationKey) {
    debug!(
        target: "replicant_runtime::mining::validation",
        event = "mining.validation.singleflight_join",
        resource_kind = key.kind.as_str(),
        entity_key = %key.id,
        "joined in-flight mining validation refresh"
    );
}

fn emit_stale(key: &ValidationKey, reason: &str) {
    warn!(
        target: "replicant_runtime::mining::validation",
        event = "mining.validation.stale_reason",
        resource_kind = key.kind.as_str(),
        entity_key = %key.id,
        reason,
        "managed mining validation projection requires refresh"
    );
}

fn emit_refresh(key: &ValidationKey, reason: ValidationReason) {
    info!(
        target: "replicant_runtime::mining::validation",
        event = "mining.validation.targeted_refresh",
        resource_kind = key.kind.as_str(),
        entity_key = %key.id,
        reason = reason.as_str(),
        "refreshing one mining validation entity"
    );
}

async fn wait_for_flight(flight: Arc<Flight>, key: &ValidationKey) {
    emit_join(key);
    let mut completion = flight.subscribe();
    if !*completion.borrow() {
        let _ = completion.changed().await;
    }
}

fn choose_flight(key: &ValidationKey) -> (Arc<Flight>, bool) {
    flight_for(key)
}

fn projection_reason(
    client: &Client,
    metadata: Option<&ObservationMetadata>,
    continuity_override: Option<bool>,
) -> Option<&'static str> {
    if let Some(reason) = metadata.and_then(metadata_stale) {
        return Some(reason);
    }
    if !continuity_override.unwrap_or_else(|| readiness_healthy(client)) {
        return Some("event_continuity");
    }
    None
}

async fn refresh_device(client: &Client, code: &str) -> AnyResult<Device> {
    Ok(client.devices().refresh(code).await?.snapshot().await?)
}

async fn refresh_replicant(client: &Client, code: &str) -> AnyResult<Replicant> {
    Ok(client
        .replicants()
        .get_owned(code)
        .await?
        .snapshot()
        .await?)
}

async fn refresh_location(client: &Client, designation: &str) -> AnyResult<Location> {
    Ok(client.locations().refresh(designation).await?)
}

/// Validates one owned device using its projection or a keyed detail refresh.
pub(crate) async fn device(
    client: &Client,
    code: &str,
    reason: ValidationReason,
) -> AnyResult<Device> {
    device_with_continuity(client, code, reason, None).await
}

async fn device_with_continuity(
    client: &Client,
    code: &str,
    reason: ValidationReason,
    continuity_override: Option<bool>,
) -> AnyResult<Device> {
    let key = ValidationKey::new(EntityKind::Device, code);
    loop {
        if !reason.bypasses_cache()
            && recently_validated(&key)
            && let Some(handle) = client.devices().cached(code)
            && let Ok(observation) = handle.observation().await
        {
            emit_managed(&key);
            return Ok(observation.value);
        }

        if !reason.requires_refresh() {
            if let Some(handle) = client.devices().cached(code) {
                if let Ok(observation) = handle.observation().await {
                    if let Some(stale) =
                        projection_reason(client, Some(&observation.metadata), continuity_override)
                    {
                        emit_stale(&key, stale);
                    } else {
                        emit_managed(&key);
                        return Ok(observation.value);
                    }
                } else {
                    emit_stale(&key, "missing_projection");
                }
            } else {
                emit_stale(&key, "missing_projection");
            }
        } else {
            emit_stale(&key, reason.as_str());
        }

        let (flight, joined) = choose_flight(&key);
        if joined {
            wait_for_flight(flight, &key).await;
            continue;
        }
        let guard = FlightGuard::new(key.clone(), flight);
        emit_refresh(&key, reason);
        let result = refresh_device(client, code).await;
        if result.is_ok() {
            cache_success(key.clone());
        }
        guard.finish();
        return result;
    }
}

/// Validates one owned Replicant using its projection or a keyed detail refresh.
pub(crate) async fn replicant(
    client: &Client,
    code: &str,
    reason: ValidationReason,
) -> AnyResult<Replicant> {
    let key = ValidationKey::new(EntityKind::Replicant, code);
    loop {
        if !reason.bypasses_cache()
            && recently_validated(&key)
            && let Some(handle) = client.replicants().cached(code)
            && let Ok(value) = handle.snapshot().await
        {
            emit_managed(&key);
            return Ok(value);
        }

        if !reason.requires_refresh() {
            if let Some(handle) = client.replicants().cached(code) {
                if let Ok(value) = handle.snapshot().await {
                    if projection_reason(client, None, None).is_none() {
                        emit_managed(&key);
                        return Ok(value);
                    }
                    emit_stale(&key, "event_continuity");
                }
            } else {
                emit_stale(&key, "missing_projection");
            }
        } else {
            emit_stale(&key, reason.as_str());
        }

        let (flight, joined) = choose_flight(&key);
        if joined {
            wait_for_flight(flight, &key).await;
            continue;
        }
        let guard = FlightGuard::new(key.clone(), flight);
        emit_refresh(&key, reason);
        let result = refresh_replicant(client, code).await;
        if result.is_ok() {
            cache_success(key.clone());
        }
        guard.finish();
        return result;
    }
}

/// Validates one location using its projection or a keyed detail refresh.
pub(crate) async fn location(
    client: &Client,
    designation: &str,
    reason: ValidationReason,
) -> AnyResult<Location> {
    let key = ValidationKey::new(EntityKind::Location, designation);
    loop {
        if !reason.bypasses_cache()
            && recently_validated(&key)
            && let Some(value) = client.locations().cached(designation)
        {
            emit_managed(&key);
            return Ok(value);
        }

        if !reason.requires_refresh() {
            if let Some(value) = client.locations().cached(designation) {
                if projection_reason(client, None, None).is_none() {
                    emit_managed(&key);
                    return Ok(value);
                }
                emit_stale(&key, "event_continuity");
            } else {
                emit_stale(&key, "missing_projection");
            }
        } else {
            emit_stale(&key, reason.as_str());
        }

        let (flight, joined) = choose_flight(&key);
        if joined {
            wait_for_flight(flight, &key).await;
            continue;
        }
        let guard = FlightGuard::new(key.clone(), flight);
        emit_refresh(&key, reason);
        let result = refresh_location(client, designation).await;
        if result.is_ok() {
            cache_success(key.clone());
        }
        guard.finish();
        return result;
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use replicant_client::{Client, SecretString, raw::Url};
    use wiremock::{
        Mock, MockServer, Request, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;

    static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

    fn unique_id(prefix: &str) -> String {
        format!("{prefix}-{}", NEXT_ID.fetch_add(1, Ordering::Relaxed))
    }

    fn clear_cached_device(code: &str) {
        lock_coordinator()
            .cache
            .remove(&ValidationKey::new(EntityKind::Device, code));
    }

    async fn test_client(server: &MockServer) -> Client {
        Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .authentication_token(SecretString::from("test-token"))
            .in_memory()
            .startup_policy(replicant_client::StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only startup")
    }

    fn device_body(code: &str) -> serde_json::Value {
        serde_json::json!({
            "device_code": code,
            "device_type": "mining_drone",
            "status": "idle"
        })
    }

    async fn mount_device(
        server: &MockServer,
        code: &str,
        calls: Arc<AtomicUsize>,
        response_status: u16,
    ) {
        let endpoint = format!("/v1/devices/{code}");
        let code = code.to_owned();
        Mock::given(method("GET"))
            .and(path(endpoint))
            .respond_with(move |_request: &Request| {
                calls.fetch_add(1, Ordering::SeqCst);
                let response = ResponseTemplate::new(response_status);
                if response_status == 200 {
                    response.set_body_json(device_body(&code))
                } else {
                    response
                }
            })
            .mount(server)
            .await;
    }

    async fn mount_unexpected_get(
        server: &MockServer,
        endpoint: &'static str,
        calls: Arc<AtomicUsize>,
    ) {
        Mock::given(method("GET"))
            .and(path(endpoint))
            .respond_with(move |_request: &Request| {
                calls.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(500)
            })
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn validation_refreshes_only_relevant_device_ids() {
        let server = MockServer::start().await;
        let first = unique_id("device-one");
        let second = unique_id("device-two");
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let broad_calls = Arc::new(AtomicUsize::new(0));
        mount_device(&server, &first, Arc::clone(&first_calls), 200).await;
        mount_device(&server, &second, Arc::clone(&second_calls), 200).await;
        for endpoint in [
            "/v1/devices",
            "/v1/replicants",
            "/v1/stars",
            "/v1/systems",
            "/v1/bodies",
            "/v1/events",
        ] {
            mount_unexpected_get(&server, endpoint, Arc::clone(&broad_calls)).await;
        }

        let client = test_client(&server).await;
        device(&client, &first, ValidationReason::CapacitySensitive)
            .await
            .expect("first targeted device");
        device(&client, &second, ValidationReason::CapacitySensitive)
            .await
            .expect("second targeted device");

        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
        assert_eq!(broad_calls.load(Ordering::SeqCst), 0);
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn validation_targets_owned_replicant_endpoint() {
        let server = MockServer::start().await;
        let code = unique_id("replicant");
        let calls = Arc::new(AtomicUsize::new(0));
        let broad_calls = Arc::new(AtomicUsize::new(0));
        let endpoint = format!("/v1/replicants/{code}");
        let body_code = code.clone();
        let response_calls = Arc::clone(&calls);
        Mock::given(method("GET"))
            .and(path(endpoint))
            .respond_with(move |_request: &Request| {
                response_calls.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "replicant_code": body_code,
                    "status": "active"
                }))
            })
            .mount(&server)
            .await;
        mount_unexpected_get(&server, "/v1/replicants", Arc::clone(&broad_calls)).await;

        let client = test_client(&server).await;
        let value = replicant(&client, &code, ValidationReason::CapacitySensitive)
            .await
            .expect("targeted replicant");

        assert_eq!(value.key.id.as_str(), code);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(broad_calls.load(Ordering::SeqCst), 0);
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn validation_targets_location_detail_not_catalogue() {
        let server = MockServer::start().await;
        let designation = unique_id("location");
        let calls = Arc::new(AtomicUsize::new(0));
        let broad_calls = Arc::new(AtomicUsize::new(0));
        let endpoint = format!("/v1/locations/{designation}");
        let body_designation = designation.clone();
        let response_calls = Arc::clone(&calls);
        Mock::given(method("GET"))
            .and(path(endpoint))
            .respond_with(move |_request: &Request| {
                response_calls.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "location": body_designation,
                    "location_type": "planet"
                }))
            })
            .mount(&server)
            .await;
        mount_unexpected_get(&server, "/v1/locations", Arc::clone(&broad_calls)).await;

        let client = test_client(&server).await;
        let value = location(&client, &designation, ValidationReason::CapacitySensitive)
            .await
            .expect("targeted location");

        assert_eq!(value.key.id.as_str(), designation);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(broad_calls.load(Ordering::SeqCst), 0);
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn same_validation_key_joins_one_in_flight_refresh() {
        let server = MockServer::start().await;
        let code = unique_id("joined-device");
        let calls = Arc::new(AtomicUsize::new(0));
        let endpoint = format!("/v1/devices/{code}");
        let body_code = code.clone();
        let response_calls = Arc::clone(&calls);
        Mock::given(method("GET"))
            .and(path(endpoint))
            .respond_with(move |_request: &Request| {
                response_calls.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(100))
                    .set_body_json(device_body(&body_code))
            })
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        let (first, second) = tokio::join!(
            device(&client, &code, ValidationReason::CapacitySensitive),
            device(&client, &code, ValidationReason::CapacitySensitive),
        );

        assert!(first.is_ok());
        assert!(second.is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn managed_observation_respects_event_continuity() {
        let server = MockServer::start().await;
        let code = unique_id("continuity-device");
        let calls = Arc::new(AtomicUsize::new(0));
        mount_device(&server, &code, Arc::clone(&calls), 200).await;
        let client = test_client(&server).await;
        device(&client, &code, ValidationReason::CapacitySensitive)
            .await
            .expect("seed authoritative managed observation");

        clear_cached_device(&code);
        device_with_continuity(&client, &code, ValidationReason::Mutation, Some(true))
            .await
            .expect("healthy managed observation");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        clear_cached_device(&code);
        device_with_continuity(&client, &code, ValidationReason::Mutation, Some(false))
            .await
            .expect("degraded continuity targeted refresh");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn recent_success_is_reused_but_event_gap_and_conflict_refresh() {
        let server = MockServer::start().await;
        let code = unique_id("fresh-device");
        let calls = Arc::new(AtomicUsize::new(0));
        mount_device(&server, &code, Arc::clone(&calls), 200).await;

        let client = test_client(&server).await;
        device(&client, &code, ValidationReason::CapacitySensitive)
            .await
            .expect("authoritative snapshot");
        device(&client, &code, ValidationReason::Mutation)
            .await
            .expect("recent snapshot");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        device(&client, &code, ValidationReason::EventGap)
            .await
            .expect("event-gap refresh");
        device(&client, &code, ValidationReason::StateConflict)
            .await
            .expect("conflict refresh");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn failed_refresh_is_not_cached_and_can_retry() {
        let server = MockServer::start().await;
        let code = unique_id("retry-device");
        let calls = Arc::new(AtomicUsize::new(0));
        let endpoint = format!("/v1/devices/{code}");
        let body_code = code.clone();
        let response_calls = Arc::clone(&calls);
        Mock::given(method("GET"))
            .and(path(endpoint))
            .respond_with(move |_request: &Request| {
                if response_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(400)
                } else {
                    ResponseTemplate::new(200).set_body_json(device_body(&body_code))
                }
            })
            .mount(&server)
            .await;

        let client = test_client(&server).await;
        assert!(
            device(&client, &code, ValidationReason::CapacitySensitive)
                .await
                .is_err()
        );
        assert!(
            device(&client, &code, ValidationReason::CapacitySensitive)
                .await
                .is_ok()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        client.close().await.expect("close");
    }
}
