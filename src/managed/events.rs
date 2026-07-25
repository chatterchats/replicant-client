//! Durable event-journal catch-up, filtered SSE, and gap recovery.
//!
//! Three lanes cooperate here, matching the corrected game semantics:
//!
//! - the unfiltered account event log (`GET /v1/events?filtered=false`) is the
//!   durable, correctness-oriented catch-up and muted-event-recovery source;
//! - the filtered SSE stream (`GET /v1/events/stream`) is a low-latency
//!   observation channel that silently begins at the earliest retained event
//!   when a cursor is too old — this client never assumes an explicit
//!   "cursor rejected" response;
//! - REST reconciliation (via [`super::sync::SyncClient`]) is the
//!   correctness mechanism used whenever continuity cannot be proven.
//!
//! Every event is deduplicated by ID (durable journal membership), regardless
//! of whether it arrived through the log or SSE, and is stored, reduced, and
//! cursor-advanced in one atomic commit before publication.

use std::{
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::StreamExt as _;
use serde_json::Value;

use crate::domain::{self, DeviceKey, Event, Realm};
use crate::events::{EventLogQuery, GameEvent};
use crate::raw;
use crate::{Error, Result};

use super::client::{
    Client, ClientDegradation, ClientStatus, EventStreamOptions, ReconciliationPolicy,
    StartupPolicy, WeakClient,
};
use super::store::{ReconciliationWork, StoreError};

fn observed_at() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_default()
}

fn persistence_error(_: StoreError) -> Error {
    Error::Persistence {
        message: "SQLite store operation failed".into(),
    }
}

/// Subscriber registry for deduplicated managed event observation. Owned by
/// `ClientInner`; the background tasks that feed it hold only a
/// [`WeakClient`], never a strong reference to the client they publish into.
pub(crate) struct EventEngine {
    subscribers: Mutex<Vec<std::sync::mpsc::Sender<Event>>>,
}

impl EventEngine {
    pub(crate) fn new() -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn subscribe(&self) -> std::sync::mpsc::Receiver<Event> {
        let (sender, receiver) = std::sync::mpsc::channel();
        self.subscribers
            .lock()
            .expect("event subscribers lock poisoned")
            .push(sender);
        receiver
    }

    pub(crate) fn notify(&self, event: Event) {
        self.subscribers
            .lock()
            .expect("event subscribers lock poisoned")
            .retain(|sender| sender.send(event.clone()).is_ok());
    }
}

/// Managed event observation gateway returned by [`Client::events`].
#[derive(Clone, Debug)]
pub struct EventsGateway {
    client: Client,
}

impl EventsGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Subscribes to deduplicated events learned from unfiltered log
    /// catch-up and filtered SSE delivery. Local-only: it never itself
    /// issues a network request.
    pub async fn watch(&self) -> Result<EventWatch> {
        self.client.ensure_open()?;
        Ok(EventWatch {
            receiver: self.client.managed_events().subscribe(),
        })
    }
}

/// A local, deduplicated event stream. It never polls or otherwise issues
/// network requests.
pub struct EventWatch {
    receiver: std::sync::mpsc::Receiver<Event>,
}

impl EventWatch {
    /// Returns every event published since the last call, if any are
    /// available now.
    pub fn try_next(&self) -> Vec<Event> {
        self.receiver.try_iter().collect()
    }
}

/// Outcome of one unfiltered catch-up traversal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CatchUpOutcome {
    /// The traversal reached the end of currently available events.
    Complete,
    /// The traversal hit its configured page bound first — evidence that the
    /// gap since the last applied cursor may be too large to trust without
    /// REST reconciliation.
    BoundHit,
}

/// Applies one raw event: deduplicates by ID, reduces known projections,
/// commits the event and applied cursor atomically, publishes a state
/// revision, notifies subscribers, and schedules narrow reconciliation for
/// anything this client cannot yet reduce itself.
fn apply_event(client: &Client, raw_event: &GameEvent) -> Result<()> {
    if client
        .managed_state()
        .has_event(&raw_event.id)
        .map_err(persistence_error)?
    {
        // Already journaled by the other lane (log vs. SSE): apply once.
        return Ok(());
    }
    let event = domain::account_event(raw_event, Some(Realm::Live), observed_at()).value;
    let cursor = raw_event.id.clone();
    if event.name == domain::EventName::DeviceDecommissioned {
        // Device decommissioning is an explicit removal signal (unlike a
        // filtered or visibility-scoped collection page).
        let decommissioned: Vec<DeviceKey> = event.device.iter().cloned().collect();
        client
            .managed_state()
            .apply_event_with_decommission(&event, &cursor, &decommissioned)
            .map_err(persistence_error)?;
    } else {
        client
            .managed_state()
            .apply_event(&event, &cursor)
            .map_err(persistence_error)?;
        // This client does not (yet) reduce every documented event type into
        // a domain projection. Schedule the narrowest safe reconciliation
        // inferred from the envelope rather than silently trusting the event
        // payload for anything beyond what was just journaled.
        schedule_narrow_reconciliation(client, &event)?;
    }
    super::operation::resolve_awaiting_evidence(client, &event);
    schedule_trade_completion_reconciliation(client, &event)?;
    apply_simulation_lifecycle(client, &event);
    client.managed_events().notify(event);
    Ok(())
}

/// `trade.completed` cross-domain reconciliation: the buyer/seller device
/// and replicant named on the event envelope are already covered by
/// [`schedule_narrow_reconciliation`]; this additionally targets any device
/// codes the payload names directly (2.3.1's `new_device_codes`, for
/// device-typed trade rewards), which the envelope's own device/replicant
/// fields never carry.
fn schedule_trade_completion_reconciliation(client: &Client, event: &Event) -> Result<()> {
    if event.name != domain::EventName::TradeCompleted {
        return Ok(());
    }
    let Some(Value::Array(codes)) = event.payload.get("new_device_codes") else {
        return Ok(());
    };
    let realm = event.realm.clone().unwrap_or_default();
    for code in codes.iter().filter_map(Value::as_str) {
        client
            .managed_state()
            .enqueue_reconciliation(
                &format!("device:{code}"),
                &realm,
                "device",
                &serde_json::json!({ "id": code }),
            )
            .map_err(persistence_error)?;
    }
    Ok(())
}

/// A simulation ended server-side (completed, expired, or the player
/// abandoned it from another session) rather than through
/// [`super::simulations::SimulationsGateway::abandon`] on this client: clean
/// up its realm the same way either path does.
fn apply_simulation_lifecycle(client: &Client, event: &Event) {
    if !matches!(
        event.name.as_str(),
        "simulation.completed" | "simulation.expired" | "simulation.abandoned"
    ) {
        return;
    }
    if let Some(id) = event.payload.get("simulation_id").and_then(Value::as_i64) {
        super::simulations::cleanup_realm(client, crate::domain::SimulationId::new(id));
    }
}

/// Enqueues durable, coalesced reconciliation work for the narrowest entity
/// named by the event envelope. Does nothing when no entity is named: a
/// blanket account-wide reconciliation on every event would not be "narrow".
fn schedule_narrow_reconciliation(client: &Client, event: &Event) -> Result<()> {
    let (work_id, kind, payload) = if let Some(device) = &event.device {
        (
            format!("device:{}", device.id.as_str()),
            "device",
            serde_json::json!({ "id": device.id.as_str() }),
        )
    } else if let Some(replicant) = &event.replicant {
        (
            format!("replicant:{}", replicant.id.as_str()),
            "replicant",
            serde_json::json!({ "id": replicant.id.as_str() }),
        )
    } else if let Some(location) = &event.location {
        (
            format!("location:{}", location.id.as_str()),
            "location",
            serde_json::json!({ "id": location.id.as_str() }),
        )
    } else {
        return Ok(());
    };
    let realm = event.realm.clone().unwrap_or_default();
    client
        .managed_state()
        .enqueue_reconciliation(&work_id, &realm, kind, &payload)
        .map_err(persistence_error)
}

/// Fetches the latest unfiltered event ID as a first-start baseline
/// watermark, when the account has any event history at all.
async fn fetch_baseline_watermark(client: &Client) -> Result<Option<String>> {
    let query = EventLogQuery {
        limit: Some(1),
        filtered: Some(false),
        ..Default::default()
    };
    let response = client.managed_raw().events().list(&query).await?;
    Ok(response.value.events.last().map(|event| event.id.clone()))
}

/// Pages forward through the unfiltered account event log from `from_cursor`,
/// applying every event, until caught up or the page bound is hit.
async fn catch_up_unfiltered(
    client: &Client,
    from_cursor: Option<String>,
    max_pages: usize,
) -> Result<CatchUpOutcome> {
    let mut cursor = from_cursor;
    let mut pages = 0usize;
    loop {
        let query = EventLogQuery {
            cursor: cursor.clone(),
            limit: Some(100),
            filtered: Some(false),
            ..Default::default()
        };
        let response = client.managed_raw().events().list(&query).await?;
        for event in &response.value.events {
            apply_event(client, event)?;
        }
        pages += 1;
        match response.value.next_cursor {
            Some(next) => {
                cursor = Some(next);
                if pages >= max_pages.max(1) {
                    return Ok(CatchUpOutcome::BoundHit);
                }
            }
            None => return Ok(CatchUpOutcome::Complete),
        }
    }
}

/// Claims and processes one durable reconciliation work item, if any is due.
async fn process_reconciliation_work(client: &Client, work: &ReconciliationWork) -> Result<()> {
    let id = work
        .payload
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Configuration {
            message: "reconciliation work payload missing `id`".into(),
        })?;
    match work.kind.as_str() {
        "device" => {
            client.sync().device(id).await?;
        }
        "replicant" => {
            client.sync().replicant(id).await?;
        }
        "location" => {
            client.sync().location(id).await?;
        }
        "account" => {
            client.account().refresh().await?;
        }
        _ => {}
    }
    Ok(())
}

/// Drains the durable reconciliation queue for as long as the client lives.
async fn run_reconciliation_worker(weak: WeakClient, idle_interval: Duration) {
    loop {
        let Some(client) = weak.upgrade() else {
            return;
        };
        match client.managed_state().claim_reconciliation_work() {
            Ok(Some(work)) => {
                let outcome = process_reconciliation_work(&client, &work).await;
                match outcome {
                    Ok(()) => {
                        let _ = client
                            .managed_state()
                            .complete_reconciliation_work(&work.work_id);
                    }
                    Err(_) => {
                        let _ = client
                            .managed_state()
                            .retry_reconciliation_work(&work.work_id);
                    }
                }
            }
            _ => {
                drop(client);
                tokio::time::sleep(idle_interval).await;
            }
        }
    }
}

/// Periodically re-runs unfiltered log catch-up so events muted from SSE
/// still reach durable state.
async fn run_log_poll_loop(weak: WeakClient, interval: Duration, max_pages: usize) {
    loop {
        tokio::time::sleep(interval).await;
        let Some(client) = weak.upgrade() else {
            return;
        };
        let cursor = client.managed_state().event_cursor().ok().flatten();
        let _ = catch_up_unfiltered(&client, cursor, max_pages).await;
    }
}

/// The status to report when the SSE connection is not currently live.
///
/// A connection that has never once been established leaves the client
/// [`ClientStatus::Degraded`] — usable with a recoverable limitation, so
/// [`Client::ready`] does not block on it forever. A connection that *was*
/// live and was then lost is reported as [`ClientStatus::Offline`] instead.
fn disconnect_status(ever_ready: bool) -> ClientStatus {
    if ever_ready {
        ClientStatus::Offline
    } else {
        ClientStatus::Degraded(ClientDegradation::StartupIncomplete)
    }
}

/// Connects the filtered SSE stream from the last durably applied cursor,
/// reconnecting with bounded backoff. Holds only a cloned unmanaged
/// [`raw::Client`] (never the managed [`Client`]) across the long-lived
/// stream read, so an idle connection never keeps `ClientInner` alive.
async fn run_sse_loop(
    weak: WeakClient,
    raw_client: raw::Client,
    min_backoff: Duration,
    max_backoff: Duration,
) {
    let mut backoff = min_backoff;
    let mut ever_ready = false;
    loop {
        let Some(client) = weak.upgrade() else {
            return;
        };
        let cursor = client.managed_state().event_cursor().ok().flatten();
        client.set_status(ClientStatus::Connecting);
        drop(client);

        if let Ok(mut stream) = raw_client.events().stream(cursor.as_deref()).await {
            let Some(client) = weak.upgrade() else {
                return;
            };
            client.set_status(ClientStatus::Ready);
            ever_ready = true;
            drop(client);
            backoff = min_backoff;

            loop {
                let next = stream.next().await;
                let Some(client) = weak.upgrade() else {
                    return;
                };
                match next {
                    Some(Ok(event)) => {
                        let _ = apply_event(&client, &event);
                    }
                    _ => break,
                }
            }
        }

        let Some(client) = weak.upgrade() else {
            return;
        };
        client.set_status(disconnect_status(ever_ready));
        drop(client);
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2).min(max_backoff);
    }
}

/// Runs the ordered startup/restart sequence, then hands off to the
/// persistent catch-up, SSE, and reconciliation-drain tasks.
///
/// First start (no applied cursor): capture a baseline watermark, run the
/// REST baseline, then catch up forward from the watermark. Restart (an
/// applied cursor exists): catch up forward from it first; if continuity
/// cannot be proven — the traversal hit its page bound, or the cursor is
/// stale — run REST reconciliation. Neither path ever assumes an explicit
/// server cursor rejection.
async fn run_startup(
    weak: WeakClient,
    policy: StartupPolicy,
    event_options: EventStreamOptions,
    reconciliation_policy: ReconciliationPolicy,
) {
    let Some(client) = weak.upgrade() else {
        return;
    };

    // Restart recovery, network half: an operation left at `prepared` was
    // durably registered but never confirmed to have even started its one
    // automatic submission attempt, so it is retried exactly once now.
    // Every other unresolved state is left untouched (see
    // `operation::recover`).
    super::operation::recover(&client).await;

    let sync_result = if policy == StartupPolicy::Full {
        client.sync().full().await
    } else {
        client.sync().essential().await
    };
    if sync_result.is_err() {
        client.set_status(ClientStatus::Degraded(ClientDegradation::StartupIncomplete));
    }
    client.set_status(ClientStatus::CatchingUp);

    let max_pages = event_options.max_catchup_pages;
    match client.managed_state().event_cursor() {
        Ok(Some(applied)) => {
            let outcome = catch_up_unfiltered(&client, Some(applied), max_pages).await;
            let stale = client
                .managed_state()
                .event_cursor_is_stale(reconciliation_policy.staleness_threshold)
                .unwrap_or(true);
            let bound_hit = matches!(outcome, Ok(CatchUpOutcome::BoundHit));
            if stale || bound_hit || outcome.is_err() {
                let _ = client.sync().essential().await;
            }
        }
        Ok(None) => {
            if let Ok(watermark) = fetch_baseline_watermark(&client).await {
                if let Some(watermark) = &watermark {
                    let _ = client.managed_state().set_event_cursor(watermark);
                }
                let _ = catch_up_unfiltered(&client, watermark, max_pages).await;
            }
        }
        Err(_) => {}
    }

    let raw_client = client.managed_raw().clone();
    drop(client);

    let sse_task = tokio::spawn(run_sse_loop(
        weak.clone(),
        raw_client,
        event_options.reconnect_min_backoff,
        event_options.reconnect_max_backoff,
    ));
    let poll_task = tokio::spawn(run_log_poll_loop(
        weak.clone(),
        event_options.log_poll_interval,
        max_pages,
    ));
    let reconciliation_task = tokio::spawn(run_reconciliation_worker(
        weak.clone(),
        reconciliation_policy.queue_idle_interval,
    ));

    match weak.upgrade() {
        Some(client) => {
            let _ = client.register_task(sse_task);
            let _ = client.register_task(poll_task);
            let _ = client.register_task(reconciliation_task);
        }
        None => {
            sse_task.abort();
            poll_task.abort();
            reconciliation_task.abort();
        }
    }
}

/// Starts the event engine's background lifecycle task for a freshly built
/// client. Not called for [`StartupPolicy::RestoreOnly`], which makes no
/// required initial remote sweep.
pub(crate) fn spawn(
    client: &Client,
    policy: StartupPolicy,
    event_options: EventStreamOptions,
    reconciliation_policy: ReconciliationPolicy,
) {
    let weak = client.downgrade();
    let task = tokio::spawn(run_startup(
        weak,
        policy,
        event_options,
        reconciliation_policy,
    ));
    let _ = client.register_task(task);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::domain::{
        AccessScope, Device, DeviceId, DeviceKey, DeviceRelationships, DeviceStatus, DeviceType,
        Observation, ObservationAuthority, ObservationMetadata, ObservationSource, Reachability,
        SourceDocument,
    };
    use crate::raw::{SecretString, Url};

    fn device_in_realm(realm: Realm, id: &str) -> Observation<Device> {
        Observation {
            value: Device {
                key: DeviceKey::in_realm(realm, DeviceId::from(id)),
                device_type: Some(DeviceType::from("miner")),
                status: Some(DeviceStatus::from("idle")),
                location: None,
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives: Vec::new(),
                tags: Vec::new(),
                relationships: DeviceRelationships::default(),
                access: AccessScope::Owned,
            },
            metadata: ObservationMetadata {
                source: ObservationSource::RestDetail,
                authority: ObservationAuthority::EntitySnapshot,
                observed_at: "2026-07-25T00:00:00Z".into(),
                access: AccessScope::Owned,
                reachability: Reachability::Reachable,
                stale: false,
                source_document: SourceDocument {
                    operation: "GET /v1/devices/{device_code}".into(),
                    request_id: None,
                    document_id: None,
                },
            },
        }
    }

    fn device(id: &str) -> Observation<Device> {
        Observation {
            value: Device {
                key: DeviceKey::live(DeviceId::from(id)),
                device_type: Some(DeviceType::from("miner")),
                status: Some(DeviceStatus::from("idle")),
                location: None,
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives: Vec::new(),
                tags: Vec::new(),
                relationships: DeviceRelationships::default(),
                access: AccessScope::Owned,
            },
            metadata: ObservationMetadata {
                source: ObservationSource::RestDetail,
                authority: ObservationAuthority::EntitySnapshot,
                observed_at: "2026-07-25T00:00:00Z".into(),
                access: AccessScope::Owned,
                reachability: Reachability::Reachable,
                stale: false,
                source_document: SourceDocument {
                    operation: "GET /v1/devices/{device_code}".into(),
                    request_id: None,
                    document_id: None,
                },
            },
        }
    }

    fn game_event(id: &str, name: &str, device_code: Option<&str>) -> GameEvent {
        GameEvent {
            id: id.into(),
            version: 1,
            category: "device".into(),
            event: name.into(),
            replicant_code: None,
            device_code: device_code.map(Into::into),
            device_type: None,
            star: None,
            location: None,
            payload: Default::default(),
            created_at: "2026-07-25T00:00:00Z".into(),
            extra: Default::default(),
        }
    }

    async fn restore_only_client() -> Client {
        Client::builder()
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only client")
    }

    fn fast_rate_limit_policy() -> crate::raw::rate_limit::RateLimitPolicy {
        crate::raw::rate_limit::RateLimitPolicy {
            capacity: 1000,
            refill_every: Duration::from_millis(1),
        }
    }

    async fn restore_only_client_at(base_url: &str) -> Client {
        Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(base_url).expect("mock URL"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only client")
    }

    #[tokio::test]
    async fn same_event_from_log_and_sse_applies_once() {
        let client = restore_only_client().await;
        let watch = client.events().watch().await.expect("watch");
        let event = game_event("1-0", "mining.started", Some("D1"));

        apply_event(&client, &event).expect("first apply (simulated log)");
        apply_event(&client, &event).expect("second apply (simulated sse)");

        assert_eq!(
            client
                .managed_state()
                .event_cursor()
                .expect("cursor")
                .as_deref(),
            Some("1-0")
        );
        assert_eq!(watch.try_next().len(), 1, "event notified exactly once");
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn unknown_event_persists_reaches_subscribers_and_schedules_reconciliation() {
        let client = restore_only_client().await;
        let watch = client.events().watch().await.expect("watch");
        let event = game_event("2-0", "some.future.event", Some("D2"));

        apply_event(&client, &event).expect("apply forward-compatible event");

        let notified = watch.try_next();
        assert_eq!(notified.len(), 1);
        assert_eq!(notified[0].id.as_str(), "2-0");

        let work = client
            .managed_state()
            .claim_reconciliation_work()
            .expect("claim")
            .expect("narrow reconciliation was scheduled");
        assert_eq!(work.kind, "device");
        assert_eq!(work.payload["id"], "D2");
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn device_decommissioned_event_tombstones_the_device() {
        let client = restore_only_client().await;
        client
            .managed_state()
            .persist_devices(&[device("D3")])
            .expect("seed device");
        let key = DeviceKey::live(DeviceId::from("D3"));
        assert!(client.managed_state().device(&key).is_some());

        let event = game_event("3-0", "device.decommissioned", Some("D3"));
        apply_event(&client, &event).expect("apply decommission");

        assert!(client.managed_state().device(&key).is_none());
        // Decommissioning is an explicit removal signal: no narrow
        // reconciliation is needed for the device this client just removed.
        assert!(
            client
                .managed_state()
                .claim_reconciliation_work()
                .expect("claim")
                .is_none()
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn trade_completed_schedules_reconciliation_for_new_device_codes() {
        let client = restore_only_client().await;
        let mut event = game_event("4-0", "trade.completed", Some("TC1"));
        event.payload.insert(
            "new_device_codes".into(),
            serde_json::json!(["NEW1", "NEW2"]),
        );

        apply_event(&client, &event).expect("apply trade completion");

        // The controller device named on the envelope, plus both newly
        // created/transferred device codes from the payload, are all
        // scheduled: cross-domain reconciliation, not just the envelope's
        // own device.
        let mut scheduled = Vec::new();
        while let Some(work) = client
            .managed_state()
            .claim_reconciliation_work()
            .expect("claim")
        {
            scheduled.push(work.payload["id"].as_str().unwrap().to_string());
        }
        assert!(scheduled.contains(&"TC1".to_string()));
        assert!(scheduled.contains(&"NEW1".to_string()));
        assert!(scheduled.contains(&"NEW2".to_string()));
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn simulation_completed_event_purges_that_simulation_realm() {
        let client = restore_only_client().await;
        let simulation_realm = Realm::Simulation(crate::domain::SimulationId::new(9));
        client
            .managed_state()
            .persist_devices(&[device_in_realm(simulation_realm.clone(), "SIM1")])
            .expect("seed simulation device");
        client
            .managed_state()
            .persist_devices(&[device("LIVE1")])
            .expect("seed live device");

        let mut event = game_event("5-0", "simulation.completed", None);
        event
            .payload
            .insert("simulation_id".into(), serde_json::json!(9));
        apply_event(&client, &event).expect("apply simulation completion");

        assert!(
            client
                .managed_state()
                .device(&DeviceKey::in_realm(
                    simulation_realm,
                    DeviceId::from("SIM1")
                ))
                .is_none()
        );
        assert!(
            client
                .managed_state()
                .device(&DeviceKey::live(DeviceId::from("LIVE1")))
                .is_some(),
            "live devices are never touched by simulation realm cleanup"
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn catch_up_recovers_an_event_absent_from_a_hypothetically_muted_live_stream() {
        // The managed client always uses `filtered=false` for catch-up, so an
        // event a live SSE connection's mute patterns would have suppressed
        // is still recovered durably through the unfiltered log.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [
                    {"id": "1-0", "version": 1, "category": "mining", "event": "mining.started",
                     "device_code": "D1", "created_at": "2026-07-25T00:00:00Z"},
                    {"id": "2-0", "version": 1, "category": "mining", "event": "mining.muted_example",
                     "device_code": "D1", "created_at": "2026-07-25T00:00:01Z"}
                ],
                "next_cursor": null
            })))
            .mount(&server)
            .await;
        let client = restore_only_client_at(&server.uri()).await;

        let outcome = catch_up_unfiltered(&client, None, 10)
            .await
            .expect("catch-up");
        assert_eq!(outcome, CatchUpOutcome::Complete);
        assert_eq!(
            client.managed_state().event_cursor().expect("cursor"),
            Some("2-0".to_string())
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn stale_cursor_is_flagged_uncertain_without_any_server_rejection() {
        let client = restore_only_client().await;
        client
            .managed_state()
            .set_event_cursor("1-0")
            .expect("set cursor");
        assert!(
            !client
                .managed_state()
                .event_cursor_is_stale(Duration::from_secs(3600))
                .expect("freshness check")
        );
        client
            .managed_state()
            .backdate_event_cursor(7200)
            .expect("backdate");
        assert!(
            client
                .managed_state()
                .event_cursor_is_stale(Duration::from_secs(3600))
                .expect("staleness check")
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn reconnect_backoff_eventually_reaches_a_healthy_sse_connection() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/events/stream"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string("id: 1-0\nevent: mining.started\ndata: {\"version\":1,\"category\":\"mining\",\"event\":\"mining.started\",\"device_code\":\"D1\",\"created_at\":\"2026-07-25T00:00:00Z\"}\n\n"),
            )
            .mount(&server)
            .await;

        let raw_client = raw::Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .build()
            .expect("raw client");
        let client = restore_only_client().await;
        let watch = client.events().watch().await.expect("watch");
        let weak = client.downgrade();

        let task = tokio::spawn(run_sse_loop(
            weak,
            raw_client,
            Duration::from_millis(5),
            Duration::from_millis(50),
        ));

        timeout(Duration::from_secs(2), async {
            loop {
                if !watch.try_next().is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("event delivered over SSE within the timeout");

        task.abort();
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn close_joins_log_polling_and_sse_tasks_promptly() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/accounts/me"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"email": "a@b.test"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "devices": [], "next_cursor": null
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [], "next_cursor": null
            })))
            .mount(&server)
            .await;
        // No mock for /v1/events/stream: connection attempts fail and the SSE
        // loop backs off and retries, which is exactly what close() must be
        // able to cancel and join promptly rather than waiting out.

        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .in_memory()
            .startup_policy(StartupPolicy::Essential)
            .read_rate_limit_policy(fast_rate_limit_policy())
            .event_stream_options(
                EventStreamOptions::default()
                    .log_poll_interval(Duration::from_millis(20))
                    .reconnect_backoff(Duration::from_millis(20), Duration::from_millis(20)),
            )
            .start()
            .await
            .expect("start client");

        tokio::time::sleep(Duration::from_millis(50)).await;
        timeout(Duration::from_secs(2), client.close())
            .await
            .expect("close joins background tasks promptly")
            .expect("close succeeds");
        assert_eq!(client.status(), ClientStatus::Closed);
    }

    #[test]
    fn disconnect_status_distinguishes_never_ready_from_a_lost_connection() {
        // A `tokio::sync::watch` status channel only ever exposes the latest
        // value, so a connect-then-immediately-drop cycle (as a canned mock
        // response produces) can race past any observer between polls. The
        // Offline-vs-Degraded choice is exercised directly here instead of
        // through a timing-sensitive end-to-end status watch.
        assert_eq!(
            disconnect_status(false),
            ClientStatus::Degraded(ClientDegradation::StartupIncomplete)
        );
        assert_eq!(disconnect_status(true), ClientStatus::Offline);
    }

    #[tokio::test]
    async fn readiness_reaches_degraded_without_hanging_when_sse_never_connects() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/accounts/me"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"email": "a@b.test"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "devices": [], "next_cursor": null
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [], "next_cursor": null
            })))
            .mount(&server)
            .await;
        // No mock for /v1/events/stream: every connection attempt fails, so
        // the client can never reach `Ready`. `Essential` still requires the
        // live event connection, but `Degraded` is a usable, recoverable
        // state, so `ready()` must resolve rather than block forever.
        Mock::given(method("GET"))
            .and(path("/v1/events/stream"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .in_memory()
            .startup_policy(StartupPolicy::Essential)
            .read_rate_limit_policy(fast_rate_limit_policy())
            .event_stream_options(
                EventStreamOptions::default()
                    .log_poll_interval(Duration::from_secs(60))
                    .reconnect_backoff(Duration::from_millis(5), Duration::from_millis(20)),
            )
            .start()
            .await
            .expect("start client");

        timeout(Duration::from_secs(2), client.ready())
            .await
            .expect("readiness resolves instead of hanging")
            .expect("ready succeeds in a degraded state");
        assert_eq!(
            client.status(),
            ClientStatus::Degraded(ClientDegradation::StartupIncomplete)
        );

        client.close().await.expect("close");
    }
}
