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
    collections::{BTreeMap, BTreeSet},
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering as AtomicOrdering},
    },
    time::{Duration, Instant},
};

use futures::StreamExt as _;
use serde_json::Value;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::domain::{self, DeviceId, DeviceKey, Event, Realm};
use crate::events::{EventLogQuery, GameEvent};
use crate::raw;
use crate::{Error, Result};

use super::client::{
    Client, EventStreamOptions, ReadinessComponent, ReconciliationPolicy, StartupPolicy, WeakClient,
};
use super::store::{ReconciliationWork, StoreError};

/// A bounded queue deliberately applies backpressure to both input lanes: log
/// catch-up and SSE await durable application instead of growing memory or
/// acknowledging events that have not committed.
const APPLIER_QUEUE_CAPACITY: usize = 256;
/// Slow event subscribers receive a lag error once they fall this many events
/// behind; delivery never grows an unbounded queue.
const EVENT_SUBSCRIPTION_CAPACITY: usize = 256;
static RECONCILIATION_WORKER_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn observed_at() -> crate::domain::ObservationTime {
    crate::domain::ObservationTime::now()
}

fn persistence_error(error: StoreError) -> Error {
    Error::Persistence {
        message: error.to_string(),
    }
}

/// Subscriber registry for deduplicated managed event observation. Owned by
/// `ClientInner`; the background tasks that feed it hold only a
/// [`WeakClient`], never a strong reference to the client they publish into.
pub(crate) struct EventEngine {
    subscribers: broadcast::Sender<Event>,
    applier_sender: tokio::sync::mpsc::Sender<ApplyRequest>,
    applier_receiver: Mutex<Option<tokio::sync::mpsc::Receiver<ApplyRequest>>>,
}

struct ApplyRequest {
    event: GameEvent,
    completed: tokio::sync::oneshot::Sender<Result<()>>,
}

impl EventEngine {
    pub(crate) fn new() -> Self {
        let (applier_sender, applier_receiver) = tokio::sync::mpsc::channel(APPLIER_QUEUE_CAPACITY);
        let (subscribers, _) = broadcast::channel(EVENT_SUBSCRIPTION_CAPACITY);
        Self {
            subscribers,
            applier_sender,
            applier_receiver: Mutex::new(Some(applier_receiver)),
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.subscribers.subscribe()
    }

    pub(crate) fn notify(&self, event: Event) {
        let _ = self.subscribers.send(event);
    }

    async fn enqueue(&self, event: GameEvent) -> Result<()> {
        let (completed, result) = tokio::sync::oneshot::channel();
        self.applier_sender
            .send(ApplyRequest { event, completed })
            .await
            .map_err(|_| Error::Closed)?;
        result.await.map_err(|_| Error::Closed)?
    }

    fn start_applier(&self, weak: WeakClient) -> Result<tokio::task::JoinHandle<()>> {
        let receiver = self
            .applier_receiver
            .lock()
            .expect("event applier lock poisoned")
            .take()
            .ok_or_else(|| Error::Configuration {
                message: "event applier already started".into(),
            })?;
        Ok(tokio::spawn(run_applier(weak, receiver)))
    }
}

/// Result of an explicit managed event-log catch-up.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventCatchUpReport {
    /// Whether the traversal reached the current end of the event log.
    pub complete: bool,
    /// Last durably applied account event cursor after the traversal.
    pub cursor: Option<String>,
}

/// Local-only query over the durable, deduplicated account event journal.
#[derive(Clone, Debug)]
pub struct EventHistoryQuery {
    client: Client,
    after: Option<String>,
    device_code: Option<String>,
    event_name: Option<String>,
    latest: Option<usize>,
}

impl EventHistoryQuery {
    fn new(client: Client) -> Self {
        Self {
            client,
            after: None,
            device_code: None,
            event_name: None,
            latest: None,
        }
    }

    /// Returns only events whose stream ID is after `cursor`.
    #[must_use]
    pub fn after(mut self, cursor: impl Into<String>) -> Self {
        self.after = Some(cursor.into());
        self
    }

    /// Returns only events associated with this device code.
    #[must_use]
    pub fn for_device(mut self, device_code: impl Into<String>) -> Self {
        self.device_code = Some(device_code.into());
        self
    }

    /// Returns only events with this exact event name.
    #[must_use]
    pub fn named(mut self, event_name: impl Into<String>) -> Self {
        self.event_name = Some(event_name.into());
        self
    }

    /// Returns at most the newest `limit` matching events. The result remains
    /// sorted in stable event-ID order so existing consumers can reverse it
    /// when they want newest-first presentation.
    #[must_use]
    pub fn latest(mut self, limit: usize) -> Self {
        self.latest = Some(limit.max(1));
        self
    }

    /// Collects a stable event-ID-ordered view from the durable local journal.
    pub async fn collect(self) -> Result<Vec<Event>> {
        self.client.ensure_open()?;
        let matches = |event: &Event| {
            self.after
                .as_deref()
                .is_none_or(|cursor| stream_id_is_after(event.id.as_str(), cursor))
                && self.device_code.as_deref().is_none_or(|device_code| {
                    event
                        .device
                        .as_ref()
                        .is_some_and(|device| device.id.as_str() == device_code)
                })
                && self
                    .event_name
                    .as_deref()
                    .is_none_or(|event_name| event.name.as_str() == event_name)
        };
        let mut events = if let Some(limit) = self.latest {
            // Read newest rows in bounded pages rather than deserializing the
            // entire durable event journal for every Activity-page refresh.
            // Filtering is applied per page so device/name queries still
            // return the requested number of matching events when available.
            let page_size = limit.clamp(100, 1_000);
            let mut offset = 0usize;
            let mut matched = Vec::with_capacity(limit);
            loop {
                let page = self
                    .client
                    .managed_state()
                    .events_desc(page_size, offset)
                    .map_err(persistence_error)?;
                let page_len = page.len();
                matched.extend(page.into_iter().filter(&matches));
                if matched.len() >= limit || page_len < page_size {
                    break;
                }
                offset = offset.saturating_add(page_len);
            }
            matched.truncate(limit);
            matched
        } else {
            let mut all = self
                .client
                .managed_state()
                .events()
                .map_err(persistence_error)?;
            all.retain(matches);
            all
        };
        events.sort_by(|left, right| stream_id_cmp(left.id.as_str(), right.id.as_str()));
        Ok(events)
    }
}

fn stream_id_parts(value: &str) -> Option<(u64, u64)> {
    let (milliseconds, sequence) = value.split_once('-')?;
    Some((milliseconds.parse().ok()?, sequence.parse().ok()?))
}

fn stream_id_cmp(left: &str, right: &str) -> core::cmp::Ordering {
    match (stream_id_parts(left), stream_id_parts(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

fn stream_id_is_after(candidate: &str, cursor: &str) -> bool {
    stream_id_cmp(candidate, cursor).is_gt()
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

    /// Returns the last durably applied account event cursor. Local-only.
    pub fn cursor(&self) -> Result<Option<String>> {
        self.client.ensure_open()?;
        self.client
            .managed_state()
            .event_cursor()
            .map_err(persistence_error)
    }

    /// Explicitly catches up the unfiltered account event log, durably applying
    /// every page before returning. This is the managed recovery path for an
    /// SSE reconnect gap.
    pub async fn catch_up(&self, max_pages: usize) -> Result<EventCatchUpReport> {
        self.client.ensure_open()?;
        let cursor = self.cursor()?;
        let outcome = catch_up_unfiltered(&self.client, cursor, max_pages.max(1)).await?;
        Ok(EventCatchUpReport {
            complete: matches!(outcome, CatchUpOutcome::Complete),
            cursor: self.cursor()?,
        })
    }

    /// Starts a local-only query over the durable event journal.
    #[must_use]
    pub fn history(&self) -> EventHistoryQuery {
        EventHistoryQuery::new(self.client.clone())
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
    receiver: broadcast::Receiver<Event>,
}

impl EventWatch {
    /// Returns every currently buffered event. A slow watcher receives an
    /// error instead of silently losing events; use durable event history to
    /// recover the gap.
    pub fn try_next(&mut self) -> Result<Vec<Event>> {
        let mut events = Vec::new();
        loop {
            match self.receiver.try_recv() {
                Ok(event) => events.push(event),
                Err(broadcast::error::TryRecvError::Empty) => return Ok(events),
                Err(broadcast::error::TryRecvError::Closed) => return Err(Error::Closed),
                Err(broadcast::error::TryRecvError::Lagged(count)) => {
                    return Err(Error::Transport {
                        message: format!(
                            "event watcher lagged by {count} events; recover from history"
                        ),
                        source: None,
                    });
                }
            }
        }
    }

    /// Waits for one event. A lag error reports deliberate bounded-buffer
    /// loss; it never causes producer backpressure or unbounded allocation.
    pub async fn next(&mut self) -> Result<Event> {
        match self.receiver.recv().await {
            Ok(event) => Ok(event),
            Err(broadcast::error::RecvError::Closed) => Err(Error::Closed),
            Err(broadcast::error::RecvError::Lagged(count)) => Err(Error::Transport {
                message: format!("event watcher lagged by {count} events; recover from history"),
                source: None,
            }),
        }
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
fn resolve_realm(client: &Client, raw_event: &GameEvent) -> Option<Realm> {
    let simulation_id = raw_event
        .payload
        .get("simulation_id")
        .or_else(|| raw_event.extra.get("simulation_id"))
        .and_then(Value::as_i64);
    if let Some(id) = simulation_id {
        return Some(Realm::Simulation(crate::domain::SimulationId::new(id)));
    }
    if let Some(code) = raw_event.device_code.as_deref() {
        let mut realms: Vec<_> = client
            .managed_state()
            .devices()
            .into_iter()
            .filter(|device| device.value.key.id.as_str() == code)
            .map(|device| device.value.key.realm)
            .collect();
        realms.sort();
        realms.dedup();
        if realms.len() == 1 {
            return realms.pop();
        }
    }
    if let Some(code) = raw_event.replicant_code.as_deref() {
        let mut realms: Vec<_> = client
            .managed_state()
            .simulations()
            .into_iter()
            .filter(|simulation| {
                simulation.value.replicant_code.as_deref() == Some(code)
                    && !matches!(
                        simulation.value.lifecycle,
                        crate::domain::SimulationLifecycle::Ended
                    )
            })
            .map(|simulation| Realm::Simulation(simulation.value.id))
            .collect();
        realms.sort();
        realms.dedup();
        if realms.len() == 1 {
            return realms.pop();
        }
    }
    None
}

fn consumed_print_devices(event: &Event) -> Vec<DeviceKey> {
    if event.name != domain::EventName::PrintCompleted {
        return Vec::new();
    }
    let Some(realm) = event.realm.clone() else {
        return Vec::new();
    };
    event
        .payload
        .get("consumed_device_codes")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|code| DeviceKey::in_realm(realm.clone(), DeviceId::from(code)))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn apply_event(client: &Client, raw_event: &GameEvent) -> Result<()> {
    let started = Instant::now();
    debug!(
        target: "replicant_client::events",
        event = "events.apply_started",
        event_id = %raw_event.id,
        event_name = %raw_event.event,
        "applying account event"
    );
    let event =
        domain::account_event(raw_event, resolve_realm(client, raw_event), observed_at()).value;
    let cursor = raw_event.id.clone();
    let (scan_locations, scan_fallbacks) = scan_projection(&event);
    let mut decommissioned = consumed_print_devices(&event);
    if event.name == domain::EventName::DeviceDecommissioned {
        // Device decommissioning is an explicit removal signal (unlike a
        // filtered or visibility-scoped collection page).
        decommissioned.extend(event.device.iter().cloned());
    }
    decommissioned.sort();
    decommissioned.dedup();
    let inserted = if !decommissioned.is_empty() {
        client
            .managed_state()
            .apply_event_with_decommission(&event, &cursor, &decommissioned)
            .map_err(persistence_error)?
    } else {
        client
            .managed_state()
            .apply_event_with_locations(&event, &cursor, scan_locations, scan_fallbacks)
            .map_err(persistence_error)?
    };
    if !inserted {
        debug!(
            target: "replicant_client::events",
            event = "events.duplicate_skipped",
            event_id = %cursor,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "skipping duplicate account event"
        );
        // The transaction's insert-if-absent claim was lost to the other
        // lane, so no reducer, publisher, evidence, or reconciliation side
        // effect may run here.
        return Ok(());
    }
    if event.realm.is_some() && event.name != domain::EventName::DeviceDecommissioned {
        // This client does not (yet) reduce every documented event type into
        // a domain projection. Schedule the narrowest safe reconciliation
        // inferred from the envelope rather than silently trusting the event
        // payload for anything beyond what was just journaled.
        schedule_narrow_reconciliation(client, &event)?;
    }
    if event.realm.is_some() {
        super::operation::resolve_awaiting_evidence(client, &event)?;
        schedule_print_completion_reconciliation(client, &event)?;
        schedule_trade_completion_reconciliation(client, &event)?;
        apply_simulation_lifecycle(client, &event)?;
    }
    let event_name = event.name.as_str().to_owned();
    let realm = event.realm.clone();
    client.managed_events().notify(event);
    debug!(
        target: "replicant_client::events",
        event = "events.apply_completed",
        event_id = %cursor,
        event_name = %event_name,
        realm = ?realm,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "account event durably applied and published"
    );
    Ok(())
}

/// The sole ordered event-applier. Producers wait for this task's reply, so
/// queue capacity is real backpressure rather than an unbounded memory buffer.
async fn run_applier(weak: WeakClient, mut receiver: tokio::sync::mpsc::Receiver<ApplyRequest>) {
    while let Some(request) = receiver.recv().await {
        let result = weak.upgrade().ok_or(Error::Closed).and_then(|client| {
            let result = apply_event(&client, &request.event);
            if result.is_err() {
                warn!(target: "replicant_client::events", "event application failed; marking continuity degraded");
                mark_event_continuity_degraded(&client);
                if schedule_continuity_reconciliation(&client).is_err() {
                    mark_event_continuity_degraded(&client);
                }
            }
            result
        });
        if request.completed.send(result).is_err() {
            // The producer stopped awaiting while work was in flight; the
            // durable outcome has already been committed or retained for log
            // replay, so there is no caller left to notify.
        }
    }
}

fn mark_event_continuity_degraded(client: &Client) {
    client.set_readiness(|readiness| {
        readiness.event_catchup = ReadinessComponent::Degraded;
    });
}

/// The durable, coalesced work item standing for "event continuity is
/// degraded and needs reconciliation". Its successful completion is the
/// signal [`run_reconciliation_worker`] uses to restore `event_catchup`.
const EVENT_CONTINUITY_WORK_ID: &str = "account:event-continuity";

fn schedule_continuity_reconciliation(client: &Client) -> Result<()> {
    client
        .managed_state()
        .enqueue_reconciliation(
            EVENT_CONTINUITY_WORK_ID,
            &Realm::Live,
            "account",
            &serde_json::json!({ "id": "account" }),
        )
        .map_err(persistence_error)
}

/// Projects direct and controller-collated scan reports through one adapter.
/// Invalid entries retain the raw event and request only their own refresh.
fn scan_projection(
    event: &Event,
) -> (
    Vec<domain::Observation<domain::Location>>,
    Vec<(Realm, String)>,
) {
    let Some(realm) = event.realm.clone() else {
        return (Vec::new(), Vec::new());
    };
    let entries: Vec<BTreeMap<String, Value>> = match event.name.as_str() {
        "scan.completed" => vec![event.payload.clone()],
        "ami.survey.digest" => event
            .payload
            .get("report")
            .and_then(|report| report.get("scans"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_object)
            .map(|entry| {
                entry
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .collect(),
        _ => Vec::new(),
    };
    let mut locations = Vec::new();
    let mut fallbacks = BTreeSet::new();
    for entry in &entries {
        let target = entry.get("scan_target").and_then(Value::as_str);
        let result = target
            .zip(entry.get("scan_type").and_then(Value::as_str))
            .zip(entry.get("report").and_then(Value::as_object))
            .ok_or(())
            .and_then(|((target, scan_type), report)| {
                domain::scan_report_location(
                    target,
                    scan_type,
                    report,
                    realm.clone(),
                    event.occurred_at.clone(),
                    event.id.as_str(),
                )
                .map_err(|_| ())
            });
        match result {
            Ok(location) => locations.push(location),
            Err(()) => {
                if let Some(target) = target {
                    warn!(
                        target: "replicant_client::events",
                        event_id = %event.id,
                        scan_target = target,
                        "scan report was not projectable; queued targeted reconciliation"
                    );
                    fallbacks.insert(target.to_owned());
                } else {
                    warn!(
                        target: "replicant_client::events",
                        event_id = %event.id,
                        "scan report was malformed and had no target for reconciliation"
                    );
                }
            }
        }
    }
    (
        locations,
        fallbacks
            .into_iter()
            .map(|target| (realm.clone(), target))
            .collect(),
    )
}

/// A completed print names the newly created device in its payload rather
/// than the event envelope. Reconcile it explicitly so tags, ownership, and
/// placement become visible without waiting for a full device traversal.
fn schedule_print_completion_reconciliation(client: &Client, event: &Event) -> Result<()> {
    if event.name != domain::EventName::PrintCompleted {
        return Ok(());
    }
    let Some(realm) = event.realm.as_ref() else {
        return Ok(());
    };
    let Some(code) = event.payload.get("new_device_code").and_then(Value::as_str) else {
        return Ok(());
    };
    client
        .managed_state()
        .enqueue_reconciliation(
            &format!("device:{code}"),
            realm,
            "device",
            &serde_json::json!({ "id": code }),
        )
        .map_err(persistence_error)
}

/// `trade.completed` cross-domain reconciliation: the buyer/seller device
/// and replicant named on the event envelope are already covered by
/// [`schedule_narrow_reconciliation`]; this additionally targets any device
/// codes the payload names directly (the legacy `new_device_codes` field and
/// 2.3.5's role-specific `rewards_received.devices` or
/// `criteria_received.devices`), which the envelope's own device/replicant
/// fields never carry.
fn schedule_trade_completion_reconciliation(client: &Client, event: &Event) -> Result<()> {
    if event.name != domain::EventName::TradeCompleted {
        return Ok(());
    }
    let mut codes = BTreeSet::new();
    if let Some(Value::Array(values)) = event.payload.get("new_device_codes") {
        codes.extend(values.iter().filter_map(Value::as_str).map(str::to_owned));
    }
    for outcome in ["rewards_received", "criteria_received"] {
        let Some(Value::Array(values)) = event
            .payload
            .get(outcome)
            .and_then(Value::as_object)
            .and_then(|items| items.get("devices"))
        else {
            continue;
        };
        codes.extend(values.iter().filter_map(Value::as_str).map(str::to_owned));
    }
    let realm = event.realm.clone().unwrap_or_default();
    for code in codes {
        let work_id = format!("device:{code}");
        let payload = serde_json::json!({ "id": code });
        client
            .managed_state()
            .enqueue_reconciliation(&work_id, &realm, "device", &payload)
            .map_err(persistence_error)?;
    }
    Ok(())
}

/// A simulation ended server-side (completed, expired, or the player
/// abandoned it from another session) rather than through
/// [`super::simulations::SimulationsGateway::abandon`] on this client: clean
/// up its realm the same way either path does.
fn apply_simulation_lifecycle(client: &Client, event: &Event) -> Result<()> {
    if !matches!(
        event.name.as_str(),
        "simulation.completed" | "simulation.expired" | "simulation.abandoned"
    ) {
        return Ok(());
    }
    if let Some(id) = event.payload.get("simulation_id").and_then(Value::as_i64) {
        super::simulations::cleanup_realm(client, crate::domain::SimulationId::new(id))?;
    }
    Ok(())
}

/// Enqueues durable, coalesced reconciliation work for the narrowest entity
/// named by the event envelope. An event with no scoped entity falls back to
/// account reconciliation; that is the narrowest safe recovery for unknown
/// server events.
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
        (
            "account:event".to_owned(),
            "account",
            serde_json::json!({ "id": "account" }),
        )
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
    let total_started = Instant::now();
    let mut cursor = from_cursor;
    let mut pages = 0usize;
    info!(
        target: "replicant_client::events",
        event = "events.catchup_started",
        cursor_present = cursor.is_some(),
        max_pages,
        "starting unfiltered event-log catch-up"
    );
    loop {
        let page_started = Instant::now();
        debug!(
            target: "replicant_client::events",
            event = "events.catchup_page_started",
            page = pages + 1,
            cursor = cursor.as_deref().unwrap_or(""),
            "fetching event-log catch-up page"
        );
        let query = EventLogQuery {
            cursor: cursor.clone(),
            limit: Some(100),
            filtered: Some(false),
            ..Default::default()
        };
        let request_started = Instant::now();
        let response = client.managed_raw().events().list(&query).await?;
        let request_elapsed = request_started.elapsed();
        let events = response.value.events;
        let next_cursor = response.value.next_cursor;
        let event_count = events.len();
        let last_event_id = events.last().map(|event| event.id.clone());
        let apply_started = Instant::now();
        for event in events {
            client.managed_events().enqueue(event).await?;
        }
        let apply_elapsed = apply_started.elapsed();
        pages += 1;
        info!(
            target: "replicant_client::events",
            event = "events.catchup_page_completed",
            page = pages,
            events = event_count,
            has_next = next_cursor.is_some(),
            request_ms = request_elapsed.as_millis() as u64,
            apply_ms = apply_elapsed.as_millis() as u64,
            elapsed_ms = page_started.elapsed().as_millis() as u64,
            "applied event-log catch-up page"
        );
        match next_cursor {
            Some(next) => {
                if last_event_id.as_deref() == Some(next.as_str()) || cursor.as_ref() == Some(&next)
                {
                    info!(
                        target: "replicant_client::events",
                        event = "events.catchup_completed",
                        pages,
                        elapsed_ms = total_started.elapsed().as_millis() as u64,
                        reason = "terminal_cursor",
                        "event-log catch-up completed"
                    );
                    return Ok(CatchUpOutcome::Complete);
                }
                cursor = Some(next);
                if pages >= max_pages.max(1) {
                    warn!(
                        target: "replicant_client::events",
                        event = "events.catchup_bound_hit",
                        pages,
                        elapsed_ms = total_started.elapsed().as_millis() as u64,
                        "event-log catch-up reached page bound"
                    );
                    return Ok(CatchUpOutcome::BoundHit);
                }
            }
            None => {
                info!(
                    target: "replicant_client::events",
                    event = "events.catchup_completed",
                    pages,
                    elapsed_ms = total_started.elapsed().as_millis() as u64,
                    reason = "no_next_cursor",
                    "event-log catch-up completed"
                );
                return Ok(CatchUpOutcome::Complete);
            }
        }
    }
}

/// Claims and processes one durable reconciliation work item, if any is due.
async fn process_reconciliation_work(client: &Client, work: &ReconciliationWork) -> Result<()> {
    if work.kind == "simulation" {
        let id = work
            .payload
            .get("id")
            .and_then(Value::as_i64)
            .ok_or_else(|| Error::Configuration {
                message: "simulation reconciliation payload missing `id`".into(),
            })?;
        let interface_code = work
            .payload
            .get("interface_code")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Configuration {
                message: "simulation reconciliation payload missing `interface_code`".into(),
            })?;
        let active = client
            .managed_raw()
            .simulations()
            .active(interface_code)
            .await?;
        if active
            .value
            .simulations
            .iter()
            .any(|run| run.simulation_id == Some(id) && run.is_mine == Some(true))
        {
            return Err(Error::Operation {
                message: "simulation is still active".into(),
            });
        }
        return super::simulations::cleanup_realm(client, crate::domain::SimulationId::new(id));
    }
    let id = work
        .payload
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Configuration {
            message: "reconciliation work payload missing `id`".into(),
        })?;
    match work.kind.as_str() {
        "device" => {
            let response = match client.managed_raw().devices().get(id).await {
                Ok(response) => response,
                Err(error) if error.status() == Some(404) => {
                    debug!(
                        target: "replicant_client::events",
                        event = "reconciliation.device_not_found",
                        device_code = id,
                        work_id = %work.work_id,
                        "device reconciliation target no longer exists; completing work without retry"
                    );
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            let device = domain::device_detail(
                &response.value,
                work.realm.clone(),
                crate::domain::AccessScope::Owned,
                observed_at(),
            )
            .map_err(|error| Error::Decode {
                message: error.to_string(),
                status: Some(200),
                source: None,
            })?;
            client
                .managed_state()
                .persist_devices(&[device])
                .map_err(persistence_error)?;
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
    const LEASE_SECONDS: i64 = 180;
    let worker = RECONCILIATION_WORKER_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
    let owner = format!("{}:{worker}", std::process::id());

    loop {
        let Some(client) = weak.upgrade() else {
            return;
        };
        match client
            .managed_state()
            .acquire_reconciliation_leadership(&owner, LEASE_SECONDS)
        {
            Ok(true) => {}
            Ok(false) => {
                drop(client);
                tokio::time::sleep(idle_interval).await;
                continue;
            }
            Err(error) => {
                warn!(
                    target: "replicant_client::events",
                    event = "reconciliation.leader_failed",
                    error = %error,
                    "could not acquire the shared reconciliation-worker lease"
                );
                drop(client);
                tokio::time::sleep(idle_interval).await;
                continue;
            }
        }

        match client.managed_state().claim_reconciliation_work() {
            Ok(Some(work)) => {
                let outcome = process_reconciliation_work(&client, &work).await;
                match outcome {
                    Ok(()) => {
                        let _ = client
                            .managed_state()
                            .complete_reconciliation_work(&work.work_id);
                        if work.work_id == EVENT_CONTINUITY_WORK_ID {
                            client.set_readiness(|readiness| {
                                readiness.event_catchup = ReadinessComponent::Ready;
                            });
                        }
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
        let cursor = match client.managed_state().event_cursor() {
            Ok(cursor) => cursor,
            Err(_) => {
                mark_event_continuity_degraded(&client);
                None
            }
        };
        match catch_up_unfiltered(&client, cursor, max_pages).await {
            Ok(CatchUpOutcome::Complete) => {}
            Ok(CatchUpOutcome::BoundHit) | Err(_) => {
                mark_event_continuity_degraded(&client);
                if schedule_continuity_reconciliation(&client).is_err() {
                    mark_event_continuity_degraded(&client);
                }
            }
        }
    }
}

/// The status to report when the SSE connection is not currently live.
///
/// A connection that has never once been established leaves the client
/// [`ClientStatus::Degraded`] — usable with a recoverable limitation, so
/// [`Client::ready`] does not block on it forever. A connection that *was*
/// live and was then lost is reported as [`ClientStatus::Offline`] instead.
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
    loop {
        let Some(client) = weak.upgrade() else {
            return;
        };
        let cursor = match client.managed_state().event_cursor() {
            Ok(cursor) => cursor,
            Err(_) => {
                mark_event_continuity_degraded(&client);
                None
            }
        };
        if !matches!(
            client.readiness().sse_connectivity,
            ReadinessComponent::Degraded
        ) {
            client.set_readiness(|readiness| {
                readiness.sse_connectivity = ReadinessComponent::Pending;
            });
        }
        drop(client);

        let connect_started = Instant::now();
        debug!(
            target: "replicant_client::events",
            event = "events.sse_connect_started",
            cursor = cursor.as_deref().unwrap_or(""),
            backoff_ms = backoff.as_millis() as u64,
            "connecting filtered event stream"
        );
        match raw_client.events().stream(cursor.as_deref()).await {
            Ok(mut stream) => {
                let Some(client) = weak.upgrade() else {
                    return;
                };
                debug!(
                    target: "replicant_client::events",
                    event = "events.sse_connected",
                    elapsed_ms = connect_started.elapsed().as_millis() as u64,
                    "filtered event stream connected"
                );
                client.set_readiness(|readiness| {
                    readiness.sse_connectivity = ReadinessComponent::Ready;
                });
                drop(client);
                let mut received_event = false;

                loop {
                    let next = stream.next().await;
                    let Some(client) = weak.upgrade() else {
                        return;
                    };
                    match next {
                        Some(Ok(event)) => {
                            received_event = true;
                            if client.managed_events().enqueue(event).await.is_err() {
                                mark_event_continuity_degraded(&client);
                                break;
                            }
                        }
                        _ => break,
                    }
                }
                if received_event {
                    backoff = min_backoff;
                } else {
                    debug!(target: "replicant_client::events", "event stream ended without an event");
                }
            }
            Err(error) => {
                warn!(
                    target: "replicant_client::events",
                    event = "events.sse_connect_failed",
                    elapsed_ms = connect_started.elapsed().as_millis() as u64,
                    error = %error,
                    "filtered event stream connection failed"
                )
            }
        }

        let Some(client) = weak.upgrade() else {
            return;
        };
        client.set_readiness(|readiness| {
            readiness.sse_connectivity = ReadinessComponent::Degraded;
        });
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
    let startup_started = Instant::now();
    info!(
        target: "replicant_client::events",
        event = "events.startup_started",
        ?policy,
        "running managed event startup"
    );

    // Restart recovery, network half: an operation left at `prepared` was
    // durably registered but never confirmed to have even started its one
    // automatic submission attempt, so it is retried exactly once now.
    // Every other unresolved state is left untouched (see
    // `operation::recover`).
    let max_pages = event_options.max_catchup_pages;
    let recovery_started = Instant::now();
    info!(
        target: "replicant_client::events",
        event = "events.operation_recovery_started",
        "recovering durable operations"
    );
    match super::operation::recover(&client).await {
        Ok(()) => {
            info!(
                target: "replicant_client::events",
                event = "events.operation_recovery_completed",
                elapsed_ms = recovery_started.elapsed().as_millis() as u64,
                "durable operation recovery completed"
            )
        }
        Err(error) => {
            warn!(
                target: "replicant_client::events",
                event = "events.operation_recovery_failed",
                elapsed_ms = recovery_started.elapsed().as_millis() as u64,
                error = %error,
                "durable operation recovery failed"
            );
            client.set_readiness(|readiness| {
                readiness.background_reconciliation = ReadinessComponent::Degraded;
            });
        }
    }

    match client.managed_state().event_cursor() {
        Ok(Some(applied)) => {
            // Restart: the durable applied cursor is the sole continuity
            // point. Catch up before any optional REST reconciliation.
            let stale = client
                .managed_state()
                .event_cursor_is_stale(reconciliation_policy.staleness_threshold)
                .unwrap_or(true);
            let outcome = if stale {
                warn!(target: "replicant_client::events", "durable event cursor is stale; skipping replay and reconciling authoritative state");
                None
            } else {
                info!(target: "replicant_client::events", "catching up event log from durable cursor");
                Some(catch_up_unfiltered(&client, Some(applied), max_pages).await)
            };
            let complete = matches!(outcome.as_ref(), Some(Ok(CatchUpOutcome::Complete)));
            info!(
                target: "replicant_client::events",
                "event-log catch-up completed complete={} stale={stale}",
                complete
            );
            if stale || !complete {
                warn!(target: "replicant_client::events", "event continuity requires reconciliation");
                mark_event_continuity_degraded(&client);
                if schedule_continuity_reconciliation(&client).is_err() {
                    mark_event_continuity_degraded(&client);
                }
            } else {
                client.set_readiness(|readiness| {
                    readiness.event_catchup = ReadinessComponent::Ready;
                });
            }
            info!(target: "replicant_client::events", "running restart REST baseline policy={policy:?}");
            let baseline = if policy == StartupPolicy::Full {
                client.sync().full().await
            } else {
                client.sync().essential().await
            };
            if baseline.is_err() {
                warn!(target: "replicant_client::events", "restart REST baseline failed");
                client.set_readiness(|readiness| {
                    readiness.essential_rest = ReadinessComponent::Degraded;
                });
            }
        }
        Ok(None) => {
            // First start: capture an unfiltered watermark *before* the REST
            // baseline. It is intentionally not written as an applied cursor:
            // only a journaled, reduced event may advance that durable value.
            info!(target: "replicant_client::events", "fetching initial event-log watermark");
            let watermark = match fetch_baseline_watermark(&client).await {
                Ok(watermark) => {
                    info!(target: "replicant_client::events", "initial event-log watermark fetched present={}", watermark.is_some());
                    watermark
                }
                Err(error) => {
                    warn!(target: "replicant_client::events", "initial event-log watermark failed error={error}");
                    mark_event_continuity_degraded(&client);
                    None
                }
            };
            info!(target: "replicant_client::events", "running initial REST baseline policy={policy:?}");
            let baseline = if policy == StartupPolicy::Full {
                client.sync().full().await
            } else {
                client.sync().essential().await
            };
            if baseline.is_err() {
                warn!(target: "replicant_client::events", "initial REST baseline failed");
                client.set_readiness(|readiness| {
                    readiness.essential_rest = ReadinessComponent::Degraded;
                });
            }
            info!(target: "replicant_client::events", "catching up event log after initial REST baseline");
            if matches!(
                catch_up_unfiltered(&client, watermark, max_pages).await,
                Ok(CatchUpOutcome::Complete)
            ) {
                info!(target: "replicant_client::events", "initial event-log catch-up completed");
                client.set_readiness(|readiness| {
                    readiness.event_catchup = ReadinessComponent::Ready;
                });
            } else {
                warn!(target: "replicant_client::events", "initial event-log catch-up requires reconciliation");
                mark_event_continuity_degraded(&client);
                if schedule_continuity_reconciliation(&client).is_err() {
                    mark_event_continuity_degraded(&client);
                }
            }
        }
        Err(_) => {
            warn!(target: "replicant_client::events", "could not read durable event cursor");
            mark_event_continuity_degraded(&client);
            if schedule_continuity_reconciliation(&client).is_err() {
                mark_event_continuity_degraded(&client);
            }
        }
    }

    let raw_client = client.managed_raw().clone();
    info!(
        target: "replicant_client::events",
        event = "events.startup_background_workers",
        elapsed_ms = startup_started.elapsed().as_millis() as u64,
        "starting managed event background workers"
    );
    drop(client);

    let sse = run_sse_loop(
        weak.clone(),
        raw_client,
        event_options.reconnect_min_backoff,
        event_options.reconnect_max_backoff,
    );
    let poll = run_log_poll_loop(weak.clone(), event_options.log_poll_interval, max_pages);
    let reconciliation =
        run_reconciliation_worker(weak.clone(), reconciliation_policy.queue_idle_interval);
    tokio::join!(sse, poll, reconciliation);
}

/// Starts the event engine's background lifecycle task for a freshly built
/// client. Not called for [`StartupPolicy::RestoreOnly`], which makes no
/// required initial remote sweep.
pub(crate) async fn spawn(
    client: &Client,
    policy: StartupPolicy,
    event_options: EventStreamOptions,
    reconciliation_policy: ReconciliationPolicy,
) -> Result<()> {
    info!(target: "replicant_client::events", "starting managed event engine policy={policy:?}");
    let weak = client.downgrade();
    let applier = client.managed_events().start_applier(weak.clone())?;
    client.register_task(applier).await?;
    let task = tokio::spawn(run_startup(
        weak,
        policy,
        event_options,
        reconciliation_policy,
    ));
    client.register_task(task).await
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::time::timeout;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::*;
    use crate::domain::{
        AccessScope, Device, DeviceId, DeviceKey, DeviceRelationships, DeviceStatus, DeviceType,
        Observation, ObservationAuthority, ObservationMetadata, ObservationSource, Reachability,
        SourceDocument,
    };
    use crate::managed::{ClientDegradation, ClientStatus};
    use crate::raw::{SecretString, Url};

    fn scan_event(name: &str, payload: serde_json::Value) -> Event {
        scan_event_with_id("scan-test", name, payload)
    }

    fn scan_event_with_id(id: &str, name: &str, payload: serde_json::Value) -> Event {
        Event {
            id: crate::domain::EventId::from(id),
            realm: Some(Realm::Live),
            name: crate::domain::EventName::from(name),
            category: crate::domain::EventCategory::from("scan"),
            device: None,
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-27T00:00:00Z".into(),
            payload: payload
                .as_object()
                .expect("object payload")
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }
    }

    #[test]
    fn stream_ids_are_ordered_numerically_not_lexically() {
        assert!(stream_id_is_after("10-0", "9-999"));
        assert!(stream_id_is_after("10-2", "10-1"));
        assert!(!stream_id_is_after("10-1", "10-1"));
        assert!(stream_id_cmp("100-0", "99-999").is_gt());
    }

    #[tokio::test]
    async fn managed_event_history_is_durable_local_and_filterable() {
        let client = restore_only_client().await;
        let first = Event {
            id: crate::domain::EventId::from("9-999"),
            realm: Some(Realm::Live),
            name: crate::domain::EventName::from("travel.departed"),
            category: crate::domain::EventCategory::from("travel"),
            device: Some(DeviceKey::live(DeviceId::from("D1"))),
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-29T00:00:00Z".into(),
            payload: BTreeMap::new(),
        };
        let second = Event {
            id: crate::domain::EventId::from("10-0"),
            realm: Some(Realm::Live),
            name: crate::domain::EventName::from("directive.completed"),
            category: crate::domain::EventCategory::from("directive"),
            device: Some(DeviceKey::live(DeviceId::from("D1"))),
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-29T00:00:01Z".into(),
            payload: BTreeMap::new(),
        };
        let other = Event {
            id: crate::domain::EventId::from("10-1"),
            realm: Some(Realm::Live),
            name: crate::domain::EventName::from("directive.completed"),
            category: crate::domain::EventCategory::from("directive"),
            device: Some(DeviceKey::live(DeviceId::from("D2"))),
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-29T00:00:02Z".into(),
            payload: BTreeMap::new(),
        };
        client
            .managed_state()
            .apply_event(&first, first.id.as_str())
            .expect("persist first event");
        client
            .managed_state()
            .apply_event(&second, second.id.as_str())
            .expect("persist second event");
        client
            .managed_state()
            .apply_event(&other, other.id.as_str())
            .expect("persist other event");

        let events = client
            .events()
            .history()
            .after("9-999")
            .for_device("D1")
            .named("directive.completed")
            .collect()
            .await
            .expect("local managed history");
        assert_eq!(
            events
                .iter()
                .map(|event| event.id.as_str())
                .collect::<Vec<_>>(),
            ["10-0"]
        );
        assert_eq!(
            client.events().cursor().expect("cursor").as_deref(),
            Some("10-1")
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn managed_event_history_latest_reads_only_the_newest_matches() {
        let client = restore_only_client().await;
        for (id, device) in [("100-0", "D1"), ("101-0", "D2"), ("102-0", "D1")] {
            let event = Event {
                id: crate::domain::EventId::from(id),
                realm: Some(Realm::Live),
                name: crate::domain::EventName::from("ami.survey.digest"),
                category: crate::domain::EventCategory::from("directive"),
                device: Some(DeviceKey::live(DeviceId::from(device))),
                replicant: None,
                location: None,
                star: None,
                occurred_at: "2026-08-16T00:00:00Z".into(),
                payload: BTreeMap::new(),
            };
            client
                .managed_state()
                .apply_event(&event, event.id.as_str())
                .expect("persist event");
        }

        let events = client
            .events()
            .history()
            .for_device("D1")
            .latest(1)
            .collect()
            .await
            .expect("latest local history");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].id.as_str(), "102-0");
        client.close().await.expect("close");
    }

    #[test]
    fn scan_reports_project_direct_and_digest_delivery_modes() {
        let direct = scan_event(
            "scan.completed",
            serde_json::json!({
                "scan_target": "NUNKA-3",
                "scan_type": "planet",
                "report": {"planet": {"designation": "NUNKA-3", "atmosphere": "thin"}}
            }),
        );
        let (direct_locations, direct_fallbacks) = scan_projection(&direct);
        assert_eq!(direct_locations.len(), 1);
        assert!(direct_fallbacks.is_empty());
        assert_eq!(direct_locations[0].value.scanned, Some(true));
        assert_eq!(
            direct_locations[0].metadata.authority,
            ObservationAuthority::EventDelta
        );
        let future = scan_event(
            "scan.completed",
            serde_json::json!({
                "scan_target": "NUNKA-4",
                "scan_type": "planet",
                "report": {"planet": {"designation": "NUNKA-4"}, "future": {"kept": true}}
            }),
        );
        assert_eq!(
            scan_projection(&future).0[0].value.unknown["event_scan_report"]["future"]["kept"],
            true
        );

        let digest = scan_event(
            "ami.survey.digest",
            serde_json::json!({
                "report": {"scans": [
                    {"scan_target": "NUNKA-3", "scan_type": "planet", "report": {"planet": {"designation": "NUNKA-3", "atmosphere": "thin"}}},
                    {"scan_target": "NUNKA-3-MOON-1", "scan_type": "moon", "report": {"moon": {"designation": "NUNKA-3-MOON-1"}}},
                    {"scan_target": "NUNKA-3", "scan_type": "planet", "report": {"planet": {"designation": "NUNKA-3"}}},
                    {"scan_target": "BROKEN", "scan_type": "planet", "report": {"planet": {}}}
                ]}
            }),
        );
        let (digest_locations, digest_fallbacks) = scan_projection(&digest);
        assert_eq!(digest_locations.len(), 3);
        assert_eq!(digest_fallbacks, vec![(Realm::Live, "BROKEN".into())]);
        assert_eq!(direct_locations[0].value, digest_locations[0].value);

        let active_digest = scan_event(
            "ami.survey.digest",
            serde_json::json!({"report": {"scans": [{
                "scan_target": "NUNKA-3",
                "scan_type": "planet",
                "report": {"planet": {"designation": "NUNKA-3", "atmosphere": "thin"}}
            }]}}),
        );
        let direct_state = crate::managed::state::StateEngine::open_memory().expect("direct state");
        let digest_state = crate::managed::state::StateEngine::open_memory().expect("digest state");
        let (locations, fallbacks) = scan_projection(&direct);
        direct_state
            .apply_event_with_locations(&direct, direct.id.as_str(), locations, fallbacks)
            .expect("direct event");
        let (locations, fallbacks) = scan_projection(&active_digest);
        digest_state
            .apply_event_with_locations(
                &active_digest,
                active_digest.id.as_str(),
                locations,
                fallbacks,
            )
            .expect("digest event");
        assert_eq!(
            direct_state
                .locations()
                .into_iter()
                .map(|location| location.value)
                .collect::<Vec<_>>(),
            digest_state
                .locations()
                .into_iter()
                .map(|location| location.value)
                .collect::<Vec<_>>()
        );

        let empty = scan_event(
            "ami.survey.digest",
            serde_json::json!({"report": {"scans": []}}),
        );
        assert_eq!(scan_projection(&empty), (Vec::new(), Vec::new()));
    }

    #[test]
    fn inactive_direct_scans_and_active_digest_replay_to_the_same_state() {
        let direct_events = [
            scan_event_with_id(
                "1-0",
                "scan.completed",
                serde_json::json!({
                    "scan_target": "NUNKA-3", "scan_type": "planet",
                    "report": {"planet": {"designation": "NUNKA-3"}}
                }),
            ),
            scan_event_with_id(
                "2-0",
                "scan.completed",
                serde_json::json!({
                    "scan_target": "NUNKA-3-MOON-1", "scan_type": "moon",
                    "report": {"moon": {"designation": "NUNKA-3-MOON-1"}}
                }),
            ),
        ];
        let digest = scan_event_with_id(
            "1-0",
            "ami.survey.digest",
            serde_json::json!({"report": {"scans": [
                {"scan_target": "NUNKA-3", "scan_type": "planet", "report": {"planet": {"designation": "NUNKA-3"}}},
                {"scan_target": "NUNKA-3-MOON-1", "scan_type": "moon", "report": {"moon": {"designation": "NUNKA-3-MOON-1"}}}
            ]}}),
        );
        let direct_state = crate::managed::state::StateEngine::open_memory().expect("direct state");
        let digest_state = crate::managed::state::StateEngine::open_memory().expect("digest state");

        for event in &direct_events {
            let (locations, fallbacks) = scan_projection(event);
            assert!(
                direct_state
                    .apply_event_with_locations(event, event.id.as_str(), locations, fallbacks)
                    .expect("direct scan is committed")
            );
        }
        let (locations, fallbacks) = scan_projection(&digest);
        assert!(
            digest_state
                .apply_event_with_locations(&digest, digest.id.as_str(), locations, fallbacks)
                .expect("digest is committed")
        );
        assert_eq!(
            direct_state
                .locations()
                .into_iter()
                .map(|location| location.value)
                .collect::<Vec<_>>(),
            digest_state
                .locations()
                .into_iter()
                .map(|location| location.value)
                .collect::<Vec<_>>()
        );

        let replay = &direct_events[1];
        let (locations, fallbacks) = scan_projection(replay);
        assert!(
            !direct_state
                .apply_event_with_locations(replay, replay.id.as_str(), locations, fallbacks)
                .expect("replayed direct scan is ignored")
        );
        assert_eq!(
            direct_state
                .locations()
                .into_iter()
                .map(|location| location.value)
                .collect::<Vec<_>>(),
            digest_state
                .locations()
                .into_iter()
                .map(|location| location.value)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn scan_projection_is_committed_with_the_event_and_replay_is_a_noop() {
        let event = scan_event(
            "scan.completed",
            serde_json::json!({
                "scan_target": "NUNKA-3",
                "scan_type": "planet",
                "report": {"planet": {"designation": "NUNKA-3"}}
            }),
        );
        let state = crate::managed::state::StateEngine::open_memory().expect("state");
        let (locations, fallbacks) = scan_projection(&event);
        assert!(
            state
                .apply_event_with_locations(
                    &event,
                    event.id.as_str(),
                    locations.clone(),
                    fallbacks.clone()
                )
                .expect("first event is committed")
        );
        assert_eq!(state.snapshot().revision(), 1);
        assert_eq!(state.locations().len(), 1);
        assert!(
            state
                .claim_reconciliation_work()
                .expect("queue lookup")
                .is_none(),
            "a fully projectable report must not schedule fallback HTTP"
        );
        assert!(
            !state
                .apply_event_with_locations(&event, event.id.as_str(), locations, fallbacks)
                .expect("replayed event is ignored")
        );
        assert_eq!(state.snapshot().revision(), 1);
        assert_eq!(state.locations().len(), 1);

        let malformed = scan_event(
            "scan.completed",
            serde_json::json!({
                "scan_target": "BROKEN",
                "scan_type": "planet",
                "report": {"planet": {}}
            }),
        );
        let fallback_state =
            crate::managed::state::StateEngine::open_memory().expect("fallback state");
        let (locations, fallbacks) = scan_projection(&malformed);
        fallback_state
            .apply_event_with_locations(&malformed, malformed.id.as_str(), locations, fallbacks)
            .expect("malformed event is still committed");
        let work = fallback_state
            .claim_reconciliation_work()
            .expect("queue lookup")
            .expect("targeted reconciliation");
        assert_eq!(work.kind, "location");
        assert_eq!(work.payload["id"], "BROKEN");
    }

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
                attach_capacity: None,
                stow_capacity: None,
                stow_used: None,
                operational_capacity: None,
                grace_period_remaining: None,
                upkeep_requirements: Vec::new(),
                system_status: None,
                active_directive: None,
                travel: None,
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
                attach_capacity: None,
                stow_capacity: None,
                stow_used: None,
                operational_capacity: None,
                grace_period_remaining: None,
                upkeep_requirements: Vec::new(),
                system_status: None,
                active_directive: None,
                travel: None,
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

    async fn applier_client() -> Client {
        let client = restore_only_client().await;
        let task = client
            .managed_events()
            .start_applier(client.downgrade())
            .expect("start event applier");
        client
            .register_task(task)
            .await
            .expect("register event applier");
        client
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
    async fn device_reconciliation_not_found_is_terminal() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/GONE"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": "Device not found"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = restore_only_client_at(&server.uri()).await;
        let work = ReconciliationWork {
            work_id: "device:GONE".into(),
            realm: Realm::Live,
            kind: "device".into(),
            payload: serde_json::json!({"id": "GONE"}),
            attempts: 0,
        };

        process_reconciliation_work(&client, &work)
            .await
            .expect("404 is terminal for targeted device reconciliation");

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn same_event_from_log_and_sse_applies_once() {
        let client = applier_client().await;
        let mut watch = client.events().watch().await.expect("watch");
        let event = game_event("1-0", "mining.started", Some("D1"));
        let revision = client.managed_state().snapshot().revision();

        let (log, sse) = tokio::join!(
            client.managed_events().enqueue(event.clone()),
            client.managed_events().enqueue(event),
        );
        log.expect("log event applied");
        sse.expect("SSE event deduplicated");

        assert_eq!(
            client
                .managed_state()
                .event_cursor()
                .expect("cursor")
                .as_deref(),
            Some("1-0")
        );
        assert_eq!(client.managed_state().snapshot().revision(), revision + 1);
        assert_eq!(
            watch.try_next().expect("watch").len(),
            1,
            "event notified exactly once"
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn duplicate_event_producers_keep_the_cursor_monotonic() {
        let client = applier_client().await;
        let mut watch = client.events().watch().await.expect("watch");
        for sequence in 1..=32 {
            let event = game_event(&format!("{sequence}-0"), "mining.started", Some("D1"));
            let (log, sse) = tokio::join!(
                client.managed_events().enqueue(event.clone()),
                client.managed_events().enqueue(event),
            );
            log.expect("log event applied");
            sse.expect("SSE duplicate handled");
        }
        assert_eq!(
            client
                .managed_state()
                .event_cursor()
                .expect("cursor")
                .as_deref(),
            Some("32-0")
        );
        assert_eq!(watch.try_next().expect("deduplicated watch").len(), 32);
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn slow_event_subscriber_is_bounded_and_reports_lag() {
        let client = applier_client().await;
        let mut watch = client.events().watch().await.expect("watch");
        for sequence in 1..=(EVENT_SUBSCRIPTION_CAPACITY + 1) {
            client
                .managed_events()
                .enqueue(game_event(
                    &format!("{sequence}-0"),
                    "mining.started",
                    Some("D1"),
                ))
                .await
                .expect("event applied");
        }
        assert!(matches!(
            watch.try_next(),
            Err(Error::Transport { message, .. }) if message.contains("lagged")
        ));
        assert_eq!(
            client
                .managed_state()
                .event_cursor()
                .expect("cursor")
                .as_deref(),
            Some("257-0")
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn older_event_never_regresses_the_applied_cursor() {
        let client = applier_client().await;

        client
            .managed_events()
            .enqueue(game_event("10-0", "mining.started", Some("D10")))
            .await
            .expect("newer event");
        client
            .managed_events()
            .enqueue(game_event("9-999", "mining.started", Some("D9")))
            .await
            .expect("older unique event remains journaled");

        assert_eq!(
            client
                .managed_state()
                .event_cursor()
                .expect("cursor")
                .as_deref(),
            Some("10-0")
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn failed_event_commit_never_advances_cursor_or_notifies() {
        let client = applier_client().await;
        let mut watch = client.events().watch().await.expect("watch");
        client.managed_state().fail_next_commit();

        assert!(
            client
                .managed_events()
                .enqueue(game_event("3-0", "mining.started", Some("D3")))
                .await
                .is_err()
        );
        assert_eq!(client.managed_state().event_cursor().expect("cursor"), None);
        assert!(watch.try_next().expect("watch").is_empty());
        assert!(matches!(
            client.status(),
            ClientStatus::Degraded(ClientDegradation::EventContinuity)
        ));
        assert_eq!(
            client
                .managed_state()
                .claim_reconciliation_work()
                .expect("claim retained recovery work")
                .expect("continuity recovery work")
                .kind,
            "account"
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn unknown_event_persists_reaches_subscribers_and_schedules_reconciliation() {
        let client = applier_client().await;
        client
            .managed_state()
            .persist_devices(&[device("D2")])
            .expect("seed known live device");
        let mut watch = client.events().watch().await.expect("watch");
        let event = game_event("2-0", "some.future.event", Some("D2"));

        apply_event(&client, &event).expect("apply forward-compatible event");

        let notified = watch.try_next().expect("watch");
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
    async fn unscoped_unknown_event_stays_unresolved() {
        let client = restore_only_client().await;
        apply_event(&client, &game_event("2-1", "some.future.event", None))
            .expect("persist unknown event");

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
    async fn print_completed_tombstones_consumed_component_devices() {
        let client = restore_only_client().await;
        client
            .managed_state()
            .persist_devices(&[device("FACTORY"), device("C1"), device("C2")])
            .expect("seed printer and components");

        let mut event = game_event("3-1", "print.completed", Some("FACTORY"));
        event
            .payload
            .insert("new_device_code".into(), serde_json::json!("NEW1"));
        event.payload.insert(
            "consumed_device_codes".into(),
            serde_json::json!(["C1", "C2", "C1"]),
        );
        apply_event(&client, &event).expect("apply print completion");

        assert!(
            client
                .managed_state()
                .device(&DeviceKey::live(DeviceId::from("FACTORY")))
                .is_some()
        );
        assert!(
            client
                .managed_state()
                .device(&DeviceKey::live(DeviceId::from("C1")))
                .is_none()
        );
        assert!(
            client
                .managed_state()
                .device(&DeviceKey::live(DeviceId::from("C2")))
                .is_none()
        );
        let mut scheduled = Vec::new();
        while let Some(work) = client
            .managed_state()
            .claim_reconciliation_work()
            .expect("claim reconciliation")
        {
            scheduled.push(work.payload["id"].as_str().unwrap().to_owned());
        }
        assert!(scheduled.contains(&"FACTORY".to_owned()));
        assert!(scheduled.contains(&"NEW1".to_owned()));
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn trade_completed_schedules_reconciliation_for_new_device_codes() {
        let client = restore_only_client().await;
        client
            .managed_state()
            .persist_devices(&[device("TC1")])
            .expect("seed known live controller");
        let mut event = game_event("4-0", "trade.completed", Some("TC1"));
        event.payload.insert(
            "new_device_codes".into(),
            serde_json::json!(["NEW1", "NEW2"]),
        );
        event.payload.insert(
            "rewards_received".into(),
            serde_json::json!({"resources": {}, "devices": ["BUYER1", "NEW1"]}),
        );
        event.payload.insert(
            "criteria_received".into(),
            serde_json::json!({"resources": {}, "devices": ["SELLER1"]}),
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
        assert!(scheduled.contains(&"BUYER1".to_string()));
        assert!(scheduled.contains(&"SELLER1".to_string()));
        assert_eq!(scheduled.len(), 5, "duplicate device codes are coalesced");
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn explicit_simulation_id_isolated_from_a_same_code_live_device() {
        let client = restore_only_client().await;
        let simulation = Realm::Simulation(crate::domain::SimulationId::new(7));
        client
            .managed_state()
            .persist_devices(&[device("SAME"), device_in_realm(simulation.clone(), "SAME")])
            .expect("seed both realms");

        let mut event = game_event("4-9", "device.decommissioned", Some("SAME"));
        event
            .payload
            .insert("simulation_id".into(), serde_json::json!(7));
        apply_event(&client, &event).expect("apply simulation device event");

        assert!(
            client
                .managed_state()
                .device(&DeviceKey::in_realm(simulation, DeviceId::from("SAME")))
                .is_none()
        );
        assert!(
            client
                .managed_state()
                .device(&DeviceKey::live(DeviceId::from("SAME")))
                .is_some(),
            "simulation evidence must never decommission a same-code live device"
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn unresolved_realm_event_is_journaled_without_live_side_effects() {
        let client = restore_only_client().await;
        let mut watch = client.events().watch().await.expect("watch");
        apply_event(
            &client,
            &game_event("4-10", "future.event", Some("UNKNOWN")),
        )
        .expect("journal unresolved event");

        let notified = watch.try_next().expect("watch");
        assert_eq!(notified.len(), 1);
        assert_eq!(notified[0].realm, None);
        assert!(notified[0].device.is_none());
        assert!(
            client
                .managed_state()
                .claim_reconciliation_work()
                .expect("claim")
                .is_none(),
            "unresolved evidence is not allowed to reconcile Live by default"
        );
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
        let task = client
            .managed_events()
            .start_applier(client.downgrade())
            .expect("start event applier");
        client
            .register_task(task)
            .await
            .expect("register event applier");

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
    async fn catch_up_treats_a_repeated_next_cursor_as_terminal() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("cursor", "1-0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [],
                "next_cursor": "1-0"
            })))
            .mount(&server)
            .await;
        let client = restore_only_client_at(&server.uri()).await;

        let outcome = catch_up_unfiltered(&client, Some("1-0".to_owned()), 10)
            .await
            .expect("catch-up");

        assert_eq!(outcome, CatchUpOutcome::Complete);
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn catch_up_treats_the_last_event_cursor_as_terminal() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("cursor", "1-0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [{
                    "id": "2-0", "version": 1, "category": "mining", "event": "mining.started",
                    "device_code": "D1", "created_at": "2026-07-25T00:00:00Z"
                }],
                "next_cursor": "2-0"
            })))
            .mount(&server)
            .await;
        let client = restore_only_client_at(&server.uri()).await;
        let task = client
            .managed_events()
            .start_applier(client.downgrade())
            .expect("start event applier");
        client
            .register_task(task)
            .await
            .expect("register event applier");

        let outcome = catch_up_unfiltered(&client, Some("1-0".to_owned()), 10)
            .await
            .expect("catch-up");

        assert_eq!(outcome, CatchUpOutcome::Complete);
        assert_eq!(
            client.managed_state().event_cursor().expect("cursor"),
            Some("2-0".to_owned())
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn stale_cursor_is_flagged_uncertain_without_any_server_rejection() {
        let client = applier_client().await;
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
        let client = applier_client().await;
        let mut watch = client.events().watch().await.expect("watch");
        let weak = client.downgrade();

        let task = tokio::spawn(run_sse_loop(
            weak,
            raw_client,
            Duration::from_millis(5),
            Duration::from_millis(50),
        ));

        timeout(Duration::from_secs(2), async {
            loop {
                if !watch.try_next().expect("watch").is_empty() {
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
    async fn sse_connectivity_cannot_replace_a_baseline_failure() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/events/stream"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(":"),
            )
            .mount(&server)
            .await;
        let raw_client = raw::Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .build()
            .expect("raw client");
        let client = applier_client().await;
        client.set_readiness(|readiness| {
            readiness.essential_rest = ReadinessComponent::Degraded;
        });

        let task = tokio::spawn(run_sse_loop(
            client.downgrade(),
            raw_client,
            Duration::from_millis(5),
            Duration::from_millis(20),
        ));
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            client.status(),
            ClientStatus::Degraded(ClientDegradation::StartupIncomplete)
        );
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

    #[tokio::test]
    async fn first_start_captures_watermark_before_blocked_baseline_and_catches_up_once() {
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
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(75))
                    .set_body_json(serde_json::json!({"devices": [], "next_cursor": null})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("limit", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [{"id":"1-0", "version":1, "category":"mining", "event":"mining.started", "created_at":"2026-07-25T00:00:00Z"}],
                "next_cursor": null
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("cursor", "1-0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "events": [{"id":"2-0", "version":1, "category":"mining", "event":"mining.started", "device_code":"D2", "created_at":"2026-07-25T00:00:01Z"}],
                "next_cursor": null
            })))
            .mount(&server)
            .await;
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
        let mut watch = client.events().watch().await.expect("watch");

        timeout(Duration::from_secs(2), async {
            loop {
                if !watch.try_next().expect("watch").is_empty() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("post-watermark event is applied");
        assert_eq!(
            client
                .managed_state()
                .event_cursor()
                .expect("cursor")
                .as_deref(),
            Some("2-0")
        );
        client.close().await.expect("close");
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
        // live event connection. `Degraded` remains usable, but it is not
        // ready; callers use `wait_until_usable()` for that weaker barrier.
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

        timeout(Duration::from_secs(2), client.wait_until_usable())
            .await
            .expect("usable-state wait resolves instead of hanging")
            .expect("degraded client remains usable");
        let mut status = client.watch_status();
        timeout(Duration::from_secs(2), async {
            loop {
                if matches!(
                    *status.borrow(),
                    ClientStatus::Degraded(ClientDegradation::StartupIncomplete)
                ) {
                    return;
                }
                status.changed().await.expect("status channel remains open");
            }
        })
        .await
        .expect("SSE failure reaches degraded status");
        assert_eq!(
            client.status(),
            ClientStatus::Degraded(ClientDegradation::StartupIncomplete)
        );

        client.close().await.expect("close");
    }
}
