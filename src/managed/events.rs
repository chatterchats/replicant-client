//! Durable event-history catch-up, filtered SSE, and gap recovery.
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

mod projection;

use projection::*;

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

use crate::domain::{self, Event, Realm};
use crate::events::{EventLogQuery, GameEvent};
use crate::raw;
use crate::{Error, Result};

use super::client::{
    Client, EventStreamOptions, ReadinessComponent, ReconciliationPolicy, StartupPolicy, WeakClient,
};
use super::store::{EventProjectionBatch, ReconciliationWork, StoreError};

/// A bounded queue deliberately applies backpressure to both input lanes: log
/// catch-up and SSE await durable application instead of growing memory or
/// acknowledging events that have not committed.
const APPLIER_QUEUE_CAPACITY: usize = 256;
/// Slow event subscribers receive a lag error once they fall this many events
/// behind; delivery never grows an unbounded queue.
const EVENT_SUBSCRIPTION_CAPACITY: usize = 256;
static RECONCILIATION_WORKER_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Best-effort managed event-pipeline observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventTelemetrySample {
    /// Observation timestamp in Unix epoch milliseconds.
    pub observed_at_ms: i64,
    /// Stable metric name such as `sse_connect` or `event_apply`.
    pub metric: &'static str,
    /// Stable outcome associated with the metric.
    pub outcome: String,
    /// Event name when the observation concerns one account event.
    pub event_name: Option<String>,
    /// Number of account events represented by this observation.
    pub event_count: u64,
    /// Number of pages represented by a catch-up observation.
    pub page_count: u64,
    /// Optional duration associated with the observation.
    pub duration_ms: Option<u64>,
}

/// Destination for managed event/SSE telemetry.
pub trait EventTelemetrySink: Send + Sync + 'static {
    /// Records one observation without performing slow I/O inline.
    fn record(&self, sample: EventTelemetrySample);
}

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
    last_apply_lag_ms: AtomicU64,
    last_disconnect_detail: Mutex<Option<String>>,
}

struct ApplyRequest {
    event: GameEvent,
    enqueued_at: Instant,
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
            last_apply_lag_ms: AtomicU64::new(0),
            last_disconnect_detail: Mutex::new(None),
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
            .send(ApplyRequest {
                event,
                enqueued_at: Instant::now(),
                completed,
            })
            .await
            .map_err(|_| Error::Closed)?;
        result.await.map_err(|_| Error::Closed)?
    }

    fn queue_depth(&self) -> usize {
        APPLIER_QUEUE_CAPACITY.saturating_sub(self.applier_sender.capacity())
    }

    fn last_apply_lag_ms(&self) -> u64 {
        self.last_apply_lag_ms.load(AtomicOrdering::Relaxed)
    }

    fn set_disconnect_detail(&self, detail: String) {
        *self
            .last_disconnect_detail
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(detail);
    }

    fn disconnect_detail(&self) -> Option<String> {
        self.last_disconnect_detail
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
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

/// Local-only query over durable, deduplicated account event history.
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

    /// Collects a stable event-ID-ordered view from durable local history.
    pub async fn collect(self) -> Result<Vec<Event>> {
        self.client.ensure_open()?;
        self.client
            .managed_state()
            .events(self.after, self.device_code, self.event_name, self.latest)
            .map_err(persistence_error)
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

    /// Returns the last durably applied account event cursor. Local-only.
    pub fn cursor(&self) -> Result<Option<String>> {
        self.client.ensure_open()?;
        self.client
            .managed_state()
            .event_cursor()
            .map_err(persistence_error)
    }

    /// Returns the sanitized reason for the most recent managed SSE
    /// disconnect, when one has occurred during this client lifetime.
    pub fn last_disconnect_detail(&self) -> Result<Option<String>> {
        self.client.ensure_open()?;
        Ok(self.client.managed_events().disconnect_detail())
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

    /// Starts a local-only query over durable event history.
    #[must_use]
    pub fn history(&self) -> EventHistoryQuery {
        EventHistoryQuery::new(self.client.clone())
    }

    /// Fetches the complete unfiltered remote history for one exact event name.
    pub async fn full_history_named(&self, event_name: &str) -> Result<Vec<Event>> {
        self.client.ensure_open()?;
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut events = Vec::new();
        loop {
            let response = self
                .client
                .managed_raw()
                .events()
                .list(&EventLogQuery {
                    cursor: cursor.clone(),
                    limit: Some(100),
                    filtered: Some(false),
                    event: Some(event_name.to_owned()),
                    ..EventLogQuery::default()
                })
                .await?;
            events.extend(
                response
                    .value
                    .events
                    .iter()
                    .filter(|event| event.event == event_name)
                    .map(|event| {
                        domain::account_event(
                            event,
                            Some(Realm::Live),
                            domain::ObservationTime::now(),
                        )
                        .value
                    }),
            );
            let Some(next) = response.value.next_cursor else {
                break;
            };
            if next == cursor.as_deref().unwrap_or_default() || !seen_cursors.insert(next.clone()) {
                return Err(Error::Decode {
                    message: format!(
                        "remote event history cursor repeated while reading {event_name}: {next}"
                    ),
                    status: Some(200),
                    source: None,
                });
            }
            cursor = Some(next);
        }
        Ok(events)
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApplyOutcome {
    Applied,
    Duplicate,
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
    (raw_event.location.is_some() || raw_event.star.is_some()).then_some(Realm::Live)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EventProjectionKind {
    Upsert,
    Delete,
    ReconciliationOnly,
    HistoryOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EventReplayKind {
    Rebuild,
    Reconcile,
    ForwardOnly,
    NotApplicable,
}

#[derive(Clone, Copy)]
struct EventTreatment {
    name: &'static str,
    projection: EventProjectionKind,
    replay: EventReplayKind,
    reduce: fn(&Client, &Event) -> Result<EventProjectionBatch>,
}

macro_rules! event_treatments {
    ($($name:literal => ($projection:ident, $replay:ident, $function:ident, $reducer:ident)),+ $(,)?) => {
        $(
            fn $function(client: &Client, event: &Event) -> Result<EventProjectionBatch> {
                $reducer(client, event)
            }
        )+

        const EVENT_TREATMENTS: &[EventTreatment] = &[
            $(
                EventTreatment {
                    name: $name,
                    projection: EventProjectionKind::$projection,
                    replay: EventReplayKind::$replay,
                    reduce: $function,
                },
            )+
        ];
    };
}

event_treatments! {
    "ami.adopted" => (Upsert, ForwardOnly, projection_ami_adopted, projection_operational_lifecycle),
    "ami.assembled" => (ReconciliationOnly, Reconcile, projection_ami_assembled, projection_reconciliation_only),
    "ami.launched" => (ReconciliationOnly, Reconcile, projection_ami_launched, projection_reconciliation_only),
    "ami.mining.digest" => (HistoryOnly, NotApplicable, projection_ami_mining_digest, projection_history_only),
    "ami.released" => (Upsert, ForwardOnly, projection_ami_released, projection_operational_lifecycle),
    "ami.survey.digest" => (Upsert, Rebuild, projection_ami_survey_digest, projection_world_lifecycle),
    "ami.transport.digest" => (HistoryOnly, NotApplicable, projection_ami_transport_digest, projection_history_only),
    "ami.withdrawn" => (ReconciliationOnly, Reconcile, projection_ami_withdrawn, projection_reconciliation_only),
    "blueprint.unlocked" => (Upsert, ForwardOnly, projection_blueprint_unlocked, projection_account_content),
    "bobnet.new" => (HistoryOnly, NotApplicable, projection_bobnet_new, projection_history_only),
    "device.attached" => (Upsert, ForwardOnly, projection_device_attached, projection_device_movement),
    "device.changed_owner" => (Upsert, ForwardOnly, projection_device_changed_owner, projection_device_movement),
    "device.compacted" => (ReconciliationOnly, Reconcile, projection_device_compacted, projection_reconciliation_only),
    "device.compacting" => (ReconciliationOnly, Reconcile, projection_device_compacting, projection_reconciliation_only),
    "device.decommissioned" => (Delete, ForwardOnly, projection_device_decommissioned, projection_device_movement),
    "device.deployed" => (Upsert, ForwardOnly, projection_device_deployed, projection_device_movement),
    "device.detached" => (Upsert, ForwardOnly, projection_device_detached, projection_device_movement),
    "device.stowed" => (Upsert, ForwardOnly, projection_device_stowed, projection_device_movement),
    "device.unfurled" => (ReconciliationOnly, Reconcile, projection_device_unfurled, projection_reconciliation_only),
    "device.unfurling" => (ReconciliationOnly, Reconcile, projection_device_unfurling, projection_reconciliation_only),
    "directive.cleared" => (Upsert, ForwardOnly, projection_directive_cleared, projection_operational_lifecycle),
    "directive.completed" => (Upsert, ForwardOnly, projection_directive_completed, projection_operational_lifecycle),
    "directive.paused" => (Upsert, ForwardOnly, projection_directive_paused, projection_operational_lifecycle),
    "directive.resumed" => (Upsert, ForwardOnly, projection_directive_resumed, projection_operational_lifecycle),
    "directive.set" => (Upsert, ForwardOnly, projection_directive_set, projection_operational_lifecycle),
    "diversion.activated" => (Upsert, Rebuild, projection_diversion_activated, projection_automation_primitives),
    "diversion.deactivated" => (Upsert, Rebuild, projection_diversion_deactivated, projection_automation_primitives),
    "diversion.diverted" => (Upsert, Rebuild, projection_diversion_diverted, projection_automation_primitives),
    "diversion.impacted" => (Upsert, Rebuild, projection_diversion_impacted, projection_automation_primitives),
    "diversion.partial" => (Upsert, Rebuild, projection_diversion_partial, projection_automation_primitives),
    "event.completed" => (Delete, Rebuild, projection_event_completed, projection_world_lifecycle),
    "event.discovered" => (Upsert, Rebuild, projection_event_discovered, projection_world_lifecycle),
    "experience.gained" => (ReconciliationOnly, Reconcile, projection_experience_gained, projection_reconciliation_only),
    "hub.activated" => (Upsert, ForwardOnly, projection_hub_activated, projection_world_lifecycle),
    "hub.destroyed" => (Upsert, ForwardOnly, projection_hub_destroyed, projection_world_lifecycle),
    "hub.maintained" => (Upsert, ForwardOnly, projection_hub_maintained, projection_world_lifecycle),
    "hub.warning" => (Upsert, ForwardOnly, projection_hub_warning, projection_world_lifecycle),
    "megastructure.contributed" => (Delete, ForwardOnly, projection_megastructure_contributed, projection_account_content),
    "message.new" => (Upsert, ForwardOnly, projection_message_new, projection_account_content),
    "mining.relocated" => (ReconciliationOnly, Reconcile, projection_mining_relocated, projection_reconciliation_only),
    "mining.retargeted" => (ReconciliationOnly, Reconcile, projection_mining_retargeted, projection_reconciliation_only),
    "mining.started" => (ReconciliationOnly, Reconcile, projection_mining_started, projection_reconciliation_only),
    "mining.stopped" => (ReconciliationOnly, Reconcile, projection_mining_stopped, projection_reconciliation_only),
    "multiplayer.replicant_entered" => (HistoryOnly, NotApplicable, projection_multiplayer_replicant_entered, projection_history_only),
    "multiplayer.replicant_left" => (HistoryOnly, NotApplicable, projection_multiplayer_replicant_left, projection_history_only),
    "print.completed" => (Upsert, ForwardOnly, projection_print_completed, projection_operational_lifecycle),
    "print.started" => (HistoryOnly, NotApplicable, projection_print_started, projection_history_only),
    "prospect.completed" => (ReconciliationOnly, Reconcile, projection_prospect_completed, projection_reconciliation_only),
    "relay.activated" => (ReconciliationOnly, Reconcile, projection_relay_activated, projection_reconciliation_only),
    "replicant.transferred" => (Upsert, ForwardOnly, projection_replicant_transferred, projection_device_movement),
    "salvage.depleted" => (Delete, Rebuild, projection_salvage_depleted, projection_automation_primitives),
    "salvage.discovered" => (Upsert, Rebuild, projection_salvage_discovered, projection_automation_primitives),
    "scan.completed" => (Upsert, Rebuild, projection_scan_completed, projection_world_lifecycle),
    "scan.started" => (HistoryOnly, NotApplicable, projection_scan_started, projection_history_only),
    "search.completed" => (Upsert, Rebuild, projection_search_completed, projection_world_lifecycle),
    "search.started" => (HistoryOnly, NotApplicable, projection_search_started, projection_history_only),
    "simulation.abandoned" => (Delete, ForwardOnly, projection_simulation_abandoned, projection_operational_lifecycle),
    "simulation.completed" => (Delete, ForwardOnly, projection_simulation_completed, projection_operational_lifecycle),
    "simulation.expired" => (Delete, ForwardOnly, projection_simulation_expired, projection_operational_lifecycle),
    "simulation.started" => (Upsert, ForwardOnly, projection_simulation_started, projection_operational_lifecycle),
    "site.depleted" => (Delete, Rebuild, projection_site_depleted, projection_world_lifecycle),
    "story.awakened" => (ReconciliationOnly, Reconcile, projection_story_awakened, projection_reconciliation_only),
    "story.hint" => (HistoryOnly, NotApplicable, projection_story_hint, projection_history_only),
    "system.body_renamed" => (Upsert, ForwardOnly, projection_system_body_renamed, projection_world_lifecycle),
    "system.devices_halted" => (ReconciliationOnly, Reconcile, projection_system_devices_halted, projection_reconciliation_only),
    "system.entry_point_set" => (Upsert, ForwardOnly, projection_system_entry_point_set, projection_world_lifecycle),
    "system.object_detected" => (Upsert, Rebuild, projection_system_object_detected, projection_automation_primitives),
    "teleport.completed" => (Upsert, ForwardOnly, projection_teleport_completed, projection_device_movement),
    "teleport.failed" => (HistoryOnly, NotApplicable, projection_teleport_failed, projection_history_only),
    "teleport.started" => (HistoryOnly, NotApplicable, projection_teleport_started, projection_history_only),
    "trade.completed" => (Upsert, ForwardOnly, projection_trade_completed, projection_account_content),
    "trade.created" => (Upsert, ForwardOnly, projection_trade_created, projection_account_content),
    "trade.deleted" => (Delete, ForwardOnly, projection_trade_deleted, projection_account_content),
    "transport.collected" => (Upsert, ForwardOnly, projection_transport_collected, projection_operational_lifecycle),
    "transport.delivered" => (Upsert, ForwardOnly, projection_transport_delivered, projection_operational_lifecycle),
    "travel.arrived" => (Upsert, ForwardOnly, projection_travel_arrived, projection_device_movement),
    "travel.cancelled" => (Upsert, ForwardOnly, projection_travel_cancelled, projection_device_movement),
    "travel.departed" => (Upsert, ForwardOnly, projection_travel_departed, projection_device_movement),
    "triangulation.complete" => (HistoryOnly, NotApplicable, projection_triangulation_complete, projection_history_only),
    "triangulation.failed" => (HistoryOnly, NotApplicable, projection_triangulation_failed, projection_history_only),
    "triangulation.started" => (HistoryOnly, NotApplicable, projection_triangulation_started, projection_history_only),
    "ward.activated" => (Upsert, ForwardOnly, projection_ward_activated, projection_world_lifecycle),
    "ward.deactivated" => (Upsert, ForwardOnly, projection_ward_deactivated, projection_world_lifecycle),
}

fn event_treatment(name: &str) -> Option<&'static EventTreatment> {
    EVENT_TREATMENTS
        .iter()
        .find(|treatment| treatment.name == name)
}

const EVENT_PROJECTION_NAME: &str = "event_owned";
pub(crate) const EVENT_PROJECTION_VERSION: i64 = 1;
const EVENT_PROJECTION_REPLAY_PAGE_SIZE: usize = 1_000;

fn replay_owned_batch(mut batch: EventProjectionBatch) -> EventProjectionBatch {
    batch.devices.clear();
    batch.replicants.clear();
    batch.locations.clear();
    batch.stars.clear();
    batch.messages.clear();
    batch.blueprints.clear();
    batch.trades.clear();
    batch.simulations.clear();
    batch.reconciliation.clear();
    batch.deletions.retain(|deletion| {
        matches!(
            deletion.kind,
            "resource_site" | "location_event" | "incoming_object"
        )
    });
    batch
}

impl Client {
    pub(crate) fn replay_event_projections(&self) -> Result<()> {
        let replay = self
            .managed_state()
            .prepare_projection_replay(EVENT_PROJECTION_NAME, EVENT_PROJECTION_VERSION)
            .map_err(persistence_error)?;
        if replay.complete {
            return Ok(());
        }
        let mut last_rowid = replay.last_history_rowid;
        loop {
            let rows = self
                .managed_state()
                .read_projection_history(
                    last_rowid,
                    replay.high_water_rowid,
                    EVENT_PROJECTION_REPLAY_PAGE_SIZE,
                )
                .map_err(persistence_error)?;
            if rows.is_empty() {
                break;
            }
            let page_last_rowid = rows.last().map(|(rowid, _)| *rowid).unwrap_or(last_rowid);
            for (rowid, event) in rows {
                let Some(treatment) = event_treatment(event.name.as_str()) else {
                    continue;
                };
                if treatment.replay != EventReplayKind::Rebuild {
                    continue;
                }
                let batch = replay_owned_batch((treatment.reduce)(self, &event)?);
                self.managed_state()
                    .apply_replay_projection(
                        EVENT_PROJECTION_NAME,
                        EVENT_PROJECTION_VERSION,
                        rowid,
                        replay.high_water_rowid,
                        batch,
                    )
                    .map_err(persistence_error)?;
                last_rowid = rowid;
            }
            if page_last_rowid > last_rowid {
                self.managed_state()
                    .apply_replay_projection(
                        EVENT_PROJECTION_NAME,
                        EVENT_PROJECTION_VERSION,
                        page_last_rowid,
                        replay.high_water_rowid,
                        EventProjectionBatch::default(),
                    )
                    .map_err(persistence_error)?;
                last_rowid = page_last_rowid;
            }
        }
        self.managed_state()
            .complete_projection_replay(
                EVENT_PROJECTION_NAME,
                EVENT_PROJECTION_VERSION,
                replay.high_water_rowid,
            )
            .map_err(persistence_error)
    }
}

fn apply_event(client: &Client, raw_event: &GameEvent) -> Result<ApplyOutcome> {
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
    let treatment = event_treatment(event.name.as_str());
    let (batch, reconciliation_outcome) = if let Some(treatment) = treatment {
        let batch = (treatment.reduce)(client, &event)?;
        let outcome = if batch.reconciliation.is_empty() {
            matches!(
                treatment.projection,
                EventProjectionKind::Upsert | EventProjectionKind::Delete
            )
            .then_some("avoided")
        } else if treatment.projection == EventProjectionKind::ReconciliationOnly {
            Some("queued")
        } else {
            Some("fallback")
        };
        let _replay = treatment.replay;
        (batch, outcome)
    } else {
        (
            projection::projection_reconciliation_only(client, &event)?,
            Some("fallback"),
        )
    };
    let inserted = client
        .managed_state()
        .apply_event_projection(&event, &cursor, batch)
        .map_err(persistence_error)?;
    if !inserted {
        debug!(
            target: "replicant_client::events",
            event = "events.duplicate_skipped",
            event_id = %cursor,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "skipping duplicate account event"
        );
        return Ok(ApplyOutcome::Duplicate);
    }
    if let Some(outcome) = reconciliation_outcome {
        client.record_event_telemetry(EventTelemetrySample {
            observed_at_ms: telemetry_now_millis(),
            metric: "event_reconciliation",
            outcome: outcome.to_owned(),
            event_name: Some(event.name.as_str().to_owned()),
            event_count: 1,
            page_count: 0,
            duration_ms: None,
        });
    }
    if event.realm.is_some() {
        super::operation::resolve_awaiting_evidence(client, &event)?;
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
    Ok(ApplyOutcome::Applied)
}

/// The sole ordered event-applier. Producers wait for this task's reply, so
/// queue capacity is real backpressure rather than an unbounded memory buffer.
async fn run_applier(weak: WeakClient, mut receiver: tokio::sync::mpsc::Receiver<ApplyRequest>) {
    while let Some(request) = receiver.recv().await {
        let apply_started = Instant::now();
        let Some(client) = weak.upgrade() else {
            let _ = request.completed.send(Err(Error::Closed));
            continue;
        };
        let event_name = request.event.event.clone();
        let apply_client = client.clone();
        let event = request.event;
        let result =
            match tokio::task::spawn_blocking(move || apply_event(&apply_client, &event)).await {
                Ok(result) => result,
                Err(error) => {
                    warn!(
                        target: "replicant_client::events",
                        error = %error,
                        "event applier blocking task failed"
                    );
                    Err(Error::Configuration {
                        message: "event applier blocking task failed".to_owned(),
                    })
                }
            };
        let apply_lag_ms = duration_millis(request.enqueued_at.elapsed());
        client
            .managed_events()
            .last_apply_lag_ms
            .store(apply_lag_ms, AtomicOrdering::Relaxed);
        let outcome = match &result {
            Ok(ApplyOutcome::Applied) => "applied",
            Ok(ApplyOutcome::Duplicate) => "duplicate",
            Err(_) => "failed",
        };
        debug!(
            target: "replicant_client::events",
            event = "managed_sse.event_applied",
            outcome,
            apply_lag_ms,
            apply_duration_ms = duration_millis(apply_started.elapsed()),
            queue_depth = client.managed_events().queue_depth(),
            "managed event application completed"
        );
        client.record_event_telemetry(EventTelemetrySample {
            observed_at_ms: telemetry_now_millis(),
            metric: "event_apply",
            outcome: outcome.to_owned(),
            event_name: Some(event_name),
            event_count: 1,
            page_count: 0,
            duration_ms: Some(duration_millis(apply_started.elapsed())),
        });
        if result.is_err() {
            warn!(target: "replicant_client::events", "event application failed; marking continuity degraded");
            mark_event_continuity_degraded(&client);
            if schedule_continuity_reconciliation(&client).is_err() {
                mark_event_continuity_degraded(&client);
            }
        }
        if request.completed.send(result.map(|_| ())).is_err() {
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
    let mut total_events = 0u64;
    info!(
        target: "replicant_client::events",
        event = "managed_sse.catchup_started",
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
        let response = match client.managed_raw().events().list(&query).await {
            Ok(response) => response,
            Err(error) => {
                client.record_event_telemetry(EventTelemetrySample {
                    observed_at_ms: telemetry_now_millis(),
                    metric: "catchup",
                    outcome: "failed".to_owned(),
                    event_name: None,
                    event_count: total_events,
                    page_count: pages.try_into().unwrap_or(u64::MAX),
                    duration_ms: Some(duration_millis(total_started.elapsed())),
                });
                return Err(error);
            }
        };
        let request_elapsed = request_started.elapsed();
        let events = response.value.events;
        let next_cursor = response.value.next_cursor;
        let event_count = events.len();
        total_events = total_events.saturating_add(event_count.try_into().unwrap_or(u64::MAX));
        let last_event_id = events.last().map(|event| event.id.clone());
        let apply_started = Instant::now();
        for event in events {
            if let Err(error) = client.managed_events().enqueue(event).await {
                record_catchup_telemetry(
                    client,
                    "failed",
                    total_events,
                    pages,
                    total_started.elapsed(),
                );
                return Err(error);
            }
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
                        event = "managed_sse.catchup_completed",
                        pages,
                        elapsed_ms = total_started.elapsed().as_millis() as u64,
                        reason = "terminal_cursor",
                        "event-log catch-up completed"
                    );
                    record_catchup_telemetry(
                        client,
                        "complete",
                        total_events,
                        pages,
                        total_started.elapsed(),
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
                    record_catchup_telemetry(
                        client,
                        "bound_hit",
                        total_events,
                        pages,
                        total_started.elapsed(),
                    );
                    return Ok(CatchUpOutcome::BoundHit);
                }
            }
            None => {
                info!(
                    target: "replicant_client::events",
                    event = "managed_sse.catchup_completed",
                    pages,
                    elapsed_ms = total_started.elapsed().as_millis() as u64,
                    reason = "no_next_cursor",
                    "event-log catch-up completed"
                );
                record_catchup_telemetry(
                    client,
                    "complete",
                    total_events,
                    pages,
                    total_started.elapsed(),
                );
                return Ok(CatchUpOutcome::Complete);
            }
        }
    }
}

fn record_catchup_telemetry(
    client: &Client,
    outcome: &str,
    event_count: u64,
    page_count: usize,
    elapsed: Duration,
) {
    client.record_event_telemetry(EventTelemetrySample {
        observed_at_ms: telemetry_now_millis(),
        metric: "catchup",
        outcome: outcome.to_owned(),
        event_name: None,
        event_count,
        page_count: page_count.try_into().unwrap_or(u64::MAX),
        duration_ms: Some(duration_millis(elapsed)),
    });
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn telemetry_now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
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

#[derive(Clone, Copy)]
struct SseFailure {
    reason: &'static str,
    io_error_kind: &'static str,
}

fn reqwest_error_in_chain<'a>(
    source: &'a (dyn std::error::Error + 'static),
) -> Option<&'a reqwest::Error> {
    let mut current = Some(source);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<reqwest::Error>() {
            return Some(error);
        }
        current = error.source();
    }
    None
}

fn classify_sse_failure(error: &Error) -> SseFailure {
    match error {
        Error::Authentication { .. } => SseFailure {
            reason: "auth_failure",
            io_error_kind: "authentication",
        },
        Error::Contract { .. } | Error::RateLimited { .. } => SseFailure {
            reason: "upstream_http",
            io_error_kind: "http_status",
        },
        Error::Decode { .. } => SseFailure {
            reason: "parser_error",
            io_error_kind: "decode",
        },
        Error::Transport {
            source: Some(source),
            ..
        } => {
            if let Some(error) = reqwest_error_in_chain(source.as_ref()) {
                let io_error_kind = if error.is_timeout() {
                    if error.is_connect() {
                        "connect_timeout"
                    } else if error.is_body() {
                        "body_timeout"
                    } else {
                        "request_timeout"
                    }
                } else if error.is_connect() {
                    "connect"
                } else if error.is_body() {
                    "body"
                } else if error.is_request() {
                    "request"
                } else {
                    "transport"
                };
                SseFailure {
                    reason: if error.is_timeout() {
                        "local_read_timeout"
                    } else {
                        "unknown"
                    },
                    io_error_kind,
                }
            } else {
                SseFailure {
                    reason: "unknown",
                    io_error_kind: "transport",
                }
            }
        }
        Error::Transport { source: None, .. } => SseFailure {
            reason: "unknown",
            io_error_kind: "transport",
        },
        Error::Persistence { .. } => SseFailure {
            reason: "apply_error",
            io_error_kind: "persistence",
        },
        Error::Configuration { .. }
        | Error::AccountStoreMismatch { .. }
        | Error::Operation { .. }
        | Error::Closed => SseFailure {
            reason: "unknown",
            io_error_kind: "client",
        },
    }
}

fn sse_disconnect_detail(failure: SseFailure, connection_age_ms: u64, apply_lag_ms: u64) -> String {
    let cause = match failure.reason {
        "upstream_close" => format!("upstream closed after {connection_age_ms}ms"),
        "local_read_timeout" => {
            format!("client read timeout fired after {connection_age_ms}ms")
        }
        "parser_error" => format!("event parser failed after {connection_age_ms}ms"),
        "apply_error" => format!("local apply worker failed after {connection_age_ms}ms"),
        "auth_failure" => "upstream authentication failed".to_owned(),
        "upstream_http" => "upstream rejected the SSE connection".to_owned(),
        _ => format!(
            "SSE transport failed after {connection_age_ms}ms ({})",
            failure.io_error_kind
        ),
    };
    format!("{cause}; last event apply lag {apply_lag_ms}ms")
}

const HEALTHY_SSE_SESSION_MIN: Duration = Duration::from_secs(10);

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
            event = "managed_sse.connecting",
            cursor_present = cursor.is_some(),
            backoff_ms = duration_millis(backoff),
            "connecting filtered event stream"
        );
        match raw_client.events().stream(cursor.as_deref()).await {
            Ok(mut stream) => {
                let Some(client) = weak.upgrade() else {
                    return;
                };
                let connect_ms = duration_millis(connect_started.elapsed());
                info!(
                    target: "replicant_client::events",
                    event = "managed_sse.connected",
                    connect_ms,
                    cursor_present = cursor.is_some(),
                    "filtered event stream connected"
                );
                client.record_event_telemetry(EventTelemetrySample {
                    observed_at_ms: telemetry_now_millis(),
                    metric: "sse_connect",
                    outcome: "connected".to_owned(),
                    event_name: None,
                    event_count: 0,
                    page_count: 0,
                    duration_ms: Some(connect_ms),
                });
                client.set_readiness(|readiness| {
                    readiness.sse_connectivity = ReadinessComponent::Ready;
                });
                drop(client);

                let session_started = Instant::now();
                let mut last_event_at = None;
                let mut last_event_id = cursor.clone();
                let mut received_events = 0u64;
                let failure = loop {
                    let next = stream.next().await;
                    let Some(client) = weak.upgrade() else {
                        return;
                    };
                    match next {
                        Some(Ok(event)) => {
                            received_events = received_events.saturating_add(1);
                            last_event_at = Some(Instant::now());
                            last_event_id = Some(event.id.clone());
                            if let Err(error) = client.managed_events().enqueue(event).await {
                                mark_event_continuity_degraded(&client);
                                break classify_sse_failure(&error);
                            }
                        }
                        Some(Err(error)) => break classify_sse_failure(&error),
                        None => {
                            break SseFailure {
                                reason: "upstream_close",
                                io_error_kind: "eof",
                            };
                        }
                    }
                };
                let connection_age_ms = duration_millis(session_started.elapsed());
                let idle_since_last_event_ms = last_event_at
                    .map(|last_event_at| duration_millis(last_event_at.elapsed()))
                    .unwrap_or(connection_age_ms);
                if let Some(client) = weak.upgrade() {
                    let queue_depth = client.managed_events().queue_depth();
                    let apply_lag_ms = client.managed_events().last_apply_lag_ms();
                    warn!(
                        target: "replicant_client::events",
                        event = "managed_sse.disconnected",
                        connection_age_ms,
                        idle_since_last_frame_ms = ?Option::<u64>::None,
                        idle_since_last_event_ms,
                        last_event_id = last_event_id.as_deref().unwrap_or(""),
                        reason = failure.reason,
                        io_error_kind = failure.io_error_kind,
                        events_received = received_events,
                        queue_depth,
                        apply_lag_ms,
                        "managed SSE connection disconnected"
                    );
                    client.record_event_telemetry(EventTelemetrySample {
                        observed_at_ms: telemetry_now_millis(),
                        metric: "sse_disconnect",
                        outcome: failure.reason.to_owned(),
                        event_name: None,
                        event_count: received_events,
                        page_count: 0,
                        duration_ms: Some(connection_age_ms),
                    });
                    client
                        .managed_events()
                        .set_disconnect_detail(sse_disconnect_detail(
                            failure,
                            connection_age_ms,
                            apply_lag_ms,
                        ));
                }
                let healthy_interval = min_backoff.saturating_mul(10).max(HEALTHY_SSE_SESSION_MIN);
                if received_events > 0 || session_started.elapsed() >= healthy_interval {
                    backoff = min_backoff;
                }
            }
            Err(error) => {
                let connect_ms = duration_millis(connect_started.elapsed());
                let failure = classify_sse_failure(&error);
                if let Some(client) = weak.upgrade() {
                    let queue_depth = client.managed_events().queue_depth();
                    let apply_lag_ms = client.managed_events().last_apply_lag_ms();
                    client.record_event_telemetry(EventTelemetrySample {
                        observed_at_ms: telemetry_now_millis(),
                        metric: "sse_connect",
                        outcome: failure.reason.to_owned(),
                        event_name: None,
                        event_count: 0,
                        page_count: 0,
                        duration_ms: Some(connect_ms),
                    });
                    warn!(
                        target: "replicant_client::events",
                        event = "managed_sse.disconnected",
                        connection_age_ms = 0_u64,
                        idle_since_last_frame_ms = ?Option::<u64>::None,
                        idle_since_last_event_ms = 0_u64,
                        last_event_id = cursor.as_deref().unwrap_or(""),
                        reason = failure.reason,
                        io_error_kind = failure.io_error_kind,
                        events_received = 0_u64,
                        queue_depth,
                        apply_lag_ms,
                        connect_ms,
                        "managed SSE connection failed"
                    );
                    client
                        .managed_events()
                        .set_disconnect_detail(sse_disconnect_detail(failure, 0, apply_lag_ms));
                }
            }
        }

        let Some(client) = weak.upgrade() else {
            return;
        };
        client.set_readiness(|readiness| {
            readiness.sse_connectivity = ReadinessComponent::Degraded;
        });
        drop(client);
        info!(
            target: "replicant_client::events",
            event = "managed_sse.reconnecting",
            backoff_ms = duration_millis(backoff),
            "waiting before managed SSE reconnect"
        );
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
    use std::{
        fs,
        path::PathBuf,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

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

    trait ApplyScanEvent {
        fn apply_scan_event(
            &self,
            event: &Event,
            cursor: &str,
            locations: Vec<Observation<domain::Location>>,
            fallbacks: Vec<(Realm, String)>,
        ) -> std::result::Result<bool, StoreError>;
    }

    impl ApplyScanEvent for crate::managed::state::StateEngine {
        fn apply_scan_event(
            &self,
            event: &Event,
            cursor: &str,
            locations: Vec<Observation<domain::Location>>,
            fallbacks: Vec<(Realm, String)>,
        ) -> std::result::Result<bool, StoreError> {
            let reconciliation = fallbacks
                .into_iter()
                .map(|(realm, id)| super::super::store::ReconciliationTarget {
                    work_id: format!("location:{id}"),
                    realm,
                    kind: "location",
                    payload: serde_json::json!({"id": id}),
                })
                .collect();
            self.apply_event_projection(
                event,
                cursor,
                EventProjectionBatch {
                    locations,
                    reconciliation,
                    ..EventProjectionBatch::default()
                },
            )
        }
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
            .apply_event_projection(
                &first,
                first.id.as_str(),
                crate::managed::store::EventProjectionBatch::default(),
            )
            .expect("persist first event");
        client
            .managed_state()
            .apply_event_projection(
                &second,
                second.id.as_str(),
                crate::managed::store::EventProjectionBatch::default(),
            )
            .expect("persist second event");
        client
            .managed_state()
            .apply_event_projection(
                &other,
                other.id.as_str(),
                crate::managed::store::EventProjectionBatch::default(),
            )
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
                .apply_event_projection(
                    &event,
                    event.id.as_str(),
                    crate::managed::store::EventProjectionBatch::default(),
                )
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
            .apply_scan_event(&direct, direct.id.as_str(), locations, fallbacks)
            .expect("direct event");
        let (locations, fallbacks) = scan_projection(&active_digest);
        digest_state
            .apply_scan_event(
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
                    .apply_scan_event(event, event.id.as_str(), locations, fallbacks)
                    .expect("direct scan is committed")
            );
        }
        let (locations, fallbacks) = scan_projection(&digest);
        assert!(
            digest_state
                .apply_scan_event(&digest, digest.id.as_str(), locations, fallbacks)
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
                .apply_scan_event(replay, replay.id.as_str(), locations, fallbacks)
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
                .apply_scan_event(
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
                .apply_scan_event(&event, event.id.as_str(), locations, fallbacks)
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
            .apply_scan_event(&malformed, malformed.id.as_str(), locations, fallbacks)
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
                deployed_at: None,
                in_control_range: None,
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives: Vec::new(),
                tags: Vec::new(),
                settings: Default::default(),
                relationships: DeviceRelationships::default(),
                cargo: Default::default(),
                cargo_capacity: None,
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
                deployed_at: None,
                in_control_range: None,
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives: Vec::new(),
                tags: Vec::new(),
                settings: Default::default(),
                relationships: DeviceRelationships::default(),
                cargo: Default::default(),
                cargo_capacity: None,
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
    async fn automation_events_persist_resource_sites_and_incoming_objects() {
        let client = restore_only_client().await;
        let mut discovered = game_event("1-0", "salvage.discovered", None);
        discovered.location = Some("SOL-4".to_owned());
        discovered.payload = serde_json::json!({
            "designation": "SOL-4-SAL-1",
            "location": "SOL-4",
            "salvage_type": "wrecked_relay",
            "name": "Wrecked Relay",
            "resources": {"volatiles": 120}
        })
        .as_object()
        .unwrap()
        .clone();
        apply_event(&client, &discovered).expect("apply salvage discovery");
        let sites = client.locations().resource_sites().expect("resource sites");
        assert_eq!(sites.len(), 1);
        assert_eq!(sites[0].key.id.as_str(), "SOL-4-SAL-1");
        assert_eq!(sites[0].resources["volatiles"], 120);

        let mut detected = game_event("2-0", "system.object_detected", None);
        detected.star = Some("SOL".to_owned());
        detected.payload = serde_json::json!({
            "object_designation": "SOL-OBJ-2",
            "size_class": "large",
            "impact_target": "SOL-4",
            "impact_eta": "2026-08-26T09:30:00",
            "discovery_source": "hub"
        })
        .as_object()
        .unwrap()
        .clone();
        apply_event(&client, &detected).expect("apply object detection");
        let objects = client
            .locations()
            .incoming_objects()
            .expect("incoming objects");
        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].key.id.as_str(), "SOL-OBJ-2");
        assert_eq!(
            objects[0].status,
            crate::domain::IncomingObjectStatus::Detected
        );

        let mut depleted = game_event("3-0", "salvage.depleted", None);
        depleted.location = Some("SOL-4".to_owned());
        depleted
            .payload
            .insert("site".to_owned(), serde_json::json!("SOL-4-SAL-1"));
        apply_event(&client, &depleted).expect("apply salvage depletion");
        assert!(
            client
                .locations()
                .resource_sites()
                .expect("resource sites after depletion")
                .is_empty()
        );
        client.close().await.expect("close");
    }

    fn replay_test_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "replicant-event-replay-{}-{nonce}.sqlite",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn event_projection_replay_rebuilds_retained_event_owned_state() {
        let path = replay_test_path();
        let client = Client::builder()
            .sqlite(&path)
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("open replay seed client");
        client
            .managed_state()
            .persist_devices(&[device("CARRIER"), device("CHILD")])
            .expect("seed replay devices");

        let mut attached = game_event("1-0", "device.attached", Some("CARRIER"));
        attached
            .payload
            .insert("target_code".to_owned(), serde_json::json!("CHILD"));
        apply_event(&client, &attached).expect("apply forward-only device event");

        let mut salvage = game_event("2-0", "salvage.discovered", None);
        salvage.location = Some("SOL-4".to_owned());
        salvage.payload = serde_json::json!({
            "designation": "SOL-4-SAL-1",
            "location": "SOL-4",
            "salvage_type": "wreck",
            "resources": {"carbon": 10}
        })
        .as_object()
        .unwrap()
        .clone();
        apply_event(&client, &salvage).expect("apply salvage discovery");
        apply_event(&client, &salvage).expect("duplicate salvage is idempotent");

        let mut detected = game_event("3-0", "system.object_detected", None);
        detected.star = Some("SOL".to_owned());
        detected.created_at = "2026-08-26T12:00:00Z".to_owned();
        detected.payload = serde_json::json!({
            "object_designation": "SOL-OBJ-1",
            "size_class": "large",
            "impact_target": "SOL-4"
        })
        .as_object()
        .unwrap()
        .clone();
        apply_event(&client, &detected).expect("apply object detection");

        let mut partial = game_event("4-0", "diversion.partial", None);
        partial.star = Some("SOL".to_owned());
        partial.created_at = "2026-08-25T12:00:00Z".to_owned();
        partial.payload = serde_json::json!({
            "object_designation": "SOL-OBJ-1",
            "outcome": "partial"
        })
        .as_object()
        .unwrap()
        .clone();
        apply_event(&client, &partial).expect("apply partial diversion");

        let mut diverted = game_event("5-0", "diversion.diverted", None);
        diverted.star = Some("SOL".to_owned());
        diverted.payload = serde_json::json!({
            "object_designation": "SOL-OBJ-1",
            "outcome": "diverted"
        })
        .as_object()
        .unwrap()
        .clone();
        apply_event(&client, &diverted).expect("apply diverted transition");

        let mut location_event = game_event("6-0", "event.discovered", None);
        location_event.location = Some("SOL-4".to_owned());
        location_event.payload = serde_json::json!({
            "designation": "SOL-4-EVT-1",
            "location": "SOL-4",
            "event_type": "mineral_shortage",
            "tier": 1,
            "title": "Mineral Shortage",
            "description": "Deliver resources",
            "criteria": []
        })
        .as_object()
        .unwrap()
        .clone();
        apply_event(&client, &location_event).expect("apply location event discovery");

        let mut completed = game_event("7-0", "event.completed", None);
        completed.location = Some("SOL-4".to_owned());
        completed
            .payload
            .insert("designation".to_owned(), serde_json::json!("SOL-4-EVT-1"));
        apply_event(&client, &completed).expect("apply location event completion");

        let mut depleted = game_event("8-0", "salvage.depleted", None);
        depleted.location = Some("SOL-4".to_owned());
        depleted
            .payload
            .insert("site".to_owned(), serde_json::json!("SOL-4-SAL-1"));
        apply_event(&client, &depleted).expect("apply salvage depletion");

        let mut old_digest = game_event("9-0", "ami.mining.digest", Some("CARRIER"));
        old_digest.payload.insert(
            "report".to_owned(),
            serde_json::json!({"resources": {"carbon": {"actual": 99}}}),
        );
        apply_event(&client, &old_digest).expect("apply old digest history");
        client.close().await.expect("close replay seed client");

        let connection = rusqlite::Connection::open(&path).expect("open primary replay database");
        connection
            .execute_batch(
                "DELETE FROM resource_sites;
                 DELETE FROM location_events;
                 DELETE FROM discovery_data WHERE kind = 'incoming_object';
                 DELETE FROM event_projection_metadata;
                 DELETE FROM device_relationships;",
            )
            .expect("clear replay-owned projections");
        for observation in [device("CARRIER"), device("CHILD")] {
            connection
                .execute(
                    "UPDATE devices SET observation_json = ?2 WHERE device_id = ?1",
                    rusqlite::params![
                        observation.value.key.id.as_str(),
                        serde_json::to_string(&observation).expect("encode reset device")
                    ],
                )
                .expect("reset forward-only device state");
        }
        drop(connection);
        let history_path = super::super::store::history_database_path(&path);
        let history =
            rusqlite::Connection::open(&history_path).expect("open replay history database");
        history
            .execute(
                "UPDATE event_history SET appended_at = datetime('now', '-31 days') WHERE event_id = '9-0'",
                [],
            )
            .expect("age digest history");
        drop(history);

        let replayed = Client::builder()
            .sqlite(&path)
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("reopen and replay retained projections");
        assert!(
            replayed
                .locations()
                .resource_sites()
                .expect("replayed resource sites")
                .is_empty(),
            "discovery followed by depletion must remain depleted"
        );
        let objects = replayed
            .locations()
            .incoming_objects()
            .expect("replayed incoming objects");
        assert_eq!(objects.len(), 1);
        assert_eq!(
            objects[0].status,
            crate::domain::IncomingObjectStatus::Diverted,
            "history row order, not occurred_at order, owns replay"
        );
        assert!(
            replayed
                .locations()
                .location_events()
                .expect("replayed location events")
                .is_empty(),
            "event discovery followed by completion must remain completed"
        );
        let carrier = replayed
            .managed_state()
            .device(&DeviceKey::live(DeviceId::new("CARRIER")))
            .expect("restored carrier");
        assert!(
            carrier.value.relationships.attached_devices.is_empty(),
            "forward-only device deltas must not replay"
        );
        assert!(
            replayed
                .managed_state()
                .events(None, None, None, None)
                .expect("retained history")
                .iter()
                .any(|event| event.id.as_str() == "9-0"),
            "full-refresh-compatible history must retain old AMI telemetry"
        );
        replayed.close().await.expect("close replayed client");

        let connection = rusqlite::Connection::open(&path).expect("inspect replay metadata");
        let metadata = connection
            .query_row(
                "SELECT state, coverage FROM event_projection_metadata WHERE projection = 'event_owned'",
                [],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .expect("projection metadata row");
        assert_eq!(
            metadata,
            ("complete".to_owned(), "retained_only".to_owned())
        );
        drop(connection);
        fs::remove_file(&path).expect("remove replay database");
        fs::remove_file(history_path).expect("remove replay history database");
    }
    fn direct_effect_count(batch: &EventProjectionBatch) -> usize {
        batch.devices.len()
            + batch.replicants.len()
            + batch.locations.len()
            + batch.stars.len()
            + batch.resource_sites.len()
            + batch.location_events.len()
            + batch.incoming_objects.len()
            + batch.messages.len()
            + batch.blueprints.len()
            + batch.trades.len()
            + batch.simulations.len()
            + batch.deletions.len()
    }

    fn collect_payload_strings(value: &Value, strings: &mut BTreeSet<String>) {
        match value {
            Value::String(value) if !value.is_empty() => {
                strings.insert(value.clone());
            }
            Value::Array(values) => {
                for value in values {
                    collect_payload_strings(value, strings);
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    collect_payload_strings(value, strings);
                }
            }
            _ => {}
        }
    }

    fn matrix_location(id: &str) -> Observation<domain::Location> {
        Observation {
            value: domain::Location {
                key: domain::LocationKey::live(domain::LocationId::new(id)),
                location_type: None,
                scanned: None,
                system_scanned: None,
                system_tags: Vec::new(),
                system: None,
                parent: None,
                custom_name: None,
                survey_progress: Default::default(),
                environment: Default::default(),
                unknown: BTreeMap::new(),
            },
            metadata: device("metadata").metadata,
        }
    }

    fn matrix_replicant() -> Observation<domain::Replicant> {
        Observation {
            value: domain::Replicant {
                key: domain::ReplicantKey::live(domain::ReplicantId::new("R0")),
                name: Some("Matrix Replicant".to_owned()),
                is_npc: Some(false),
                status: None,
                location: Some(domain::LocationKey::live(domain::LocationId::new("LOC"))),
                hosted_device: Some(DeviceKey::live(DeviceId::new("D0"))),
                travel: None,
                private: None,
                access: AccessScope::Owned,
            },
            metadata: device("metadata").metadata,
        }
    }

    fn matrix_star(id: &str) -> Observation<domain::Star> {
        Observation {
            value: domain::Star {
                key: domain::StarKey::live(domain::StarId::new(id)),
                name: Some(id.to_owned()),
                spectral_type: None,
                entry_point: None,
                position: None,
                has_hub: Some(false),
                has_ward: Some(false),
                knowledge_observed: true,
                explored: Some(true),
                has_life: None,
                region: None,
            },
            metadata: device("metadata").metadata,
        }
    }

    fn matrix_simulation(id: domain::SimulationId) -> Observation<domain::Simulation> {
        Observation {
            value: domain::Simulation {
                id,
                scenario_code: Some("matrix".to_owned()),
                scenario_name: None,
                starting_location: None,
                starting_star: None,
                is_mine: true,
                started_at: Some("2026-08-26T00:00:00Z".to_owned()),
                completed_at: None,
                lifecycle: domain::SimulationLifecycle::Active,
                seed_failures: Vec::new(),
                replicant_code: Some("R0".to_owned()),
            },
            metadata: device("metadata").metadata,
        }
    }

    #[tokio::test]
    async fn every_projection_policy_row_reopens_and_deduplicates() {
        let client = restore_only_client().await;
        let fixture: Value =
            serde_json::from_str(include_str!("../../tests/fixtures/events-3.0.0.json"))
                .expect("event coverage fixture");
        let fixtures = fixture["events"]
            .as_array()
            .expect("event fixture rows")
            .iter()
            .map(|row| {
                (
                    row["name"].as_str().expect("fixture event name"),
                    row["payload"].clone(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let direct = EVENT_TREATMENTS
            .iter()
            .filter(|treatment| {
                matches!(
                    treatment.projection,
                    EventProjectionKind::Upsert | EventProjectionKind::Delete
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(direct.len(), 53, "policy direct-treatment total changed");

        for (index, treatment) in direct.into_iter().enumerate() {
            let mut payload = fixtures
                .get(treatment.name)
                .unwrap_or_else(|| panic!("missing fixture for {}", treatment.name))
                .as_object()
                .expect("fixture payload object")
                .clone();
            if treatment.name == "ami.survey.digest" {
                payload.insert(
                    "report".to_owned(),
                    serde_json::json!({
                        "scans": [{
                            "device_code": "D0",
                            "scan_target": "LOC",
                            "scan_type": "planet",
                            "report": {"planet": {"designation": "LOC"}}
                        }]
                    }),
                );
            }
            if treatment.name == "diversion.deactivated" {
                payload.insert("device_code".to_owned(), serde_json::json!("D0"));
            }

            let mut strings = BTreeSet::from(["D0".to_owned(), "LOC".to_owned()]);
            collect_payload_strings(&Value::Object(payload.clone()), &mut strings);
            let devices = strings.iter().map(|id| device(id)).collect::<Vec<_>>();
            client
                .managed_state()
                .persist_devices(&devices)
                .unwrap_or_else(|error| panic!("seed devices for {}: {error}", treatment.name));
            client
                .managed_state()
                .persist_replicant(matrix_replicant())
                .unwrap_or_else(|error| panic!("seed replicant for {}: {error}", treatment.name));

            let mut location_ids = BTreeSet::from(["LOC".to_owned()]);
            for field in [
                "designation",
                "location",
                "scan_target",
                "search_target",
                "impact_target",
            ] {
                if let Some(id) = payload.get(field).and_then(Value::as_str) {
                    location_ids.insert(id.to_owned());
                }
            }
            for id in location_ids {
                client
                    .managed_state()
                    .persist_location(matrix_location(&id))
                    .unwrap_or_else(|error| {
                        panic!("seed location for {}: {error}", treatment.name)
                    });
            }
            let mut star_ids = BTreeSet::from(["STAR".to_owned()]);
            if let Some(id) = payload.get("star").and_then(Value::as_str) {
                star_ids.insert(id.to_owned());
            }
            client
                .managed_state()
                .replace_catalogue(star_ids.iter().map(|id| matrix_star(id)).collect(), None)
                .unwrap_or_else(|error| panic!("seed stars for {}: {error}", treatment.name));
            let simulation_id = payload
                .get("simulation_id")
                .and_then(Value::as_i64)
                .map(domain::SimulationId::new)
                .unwrap_or_else(|| domain::SimulationId::new(17));
            client
                .managed_state()
                .persist_simulation(matrix_simulation(simulation_id))
                .unwrap_or_else(|error| panic!("seed simulation for {}: {error}", treatment.name));
            client
                .managed_state()
                .persist_devices(&[device_in_realm(Realm::Simulation(simulation_id), "SIM-D")])
                .unwrap_or_else(|error| {
                    panic!("seed simulation device for {}: {error}", treatment.name)
                });

            let cursor = format!("{}-0", index + 1);
            let raw = GameEvent {
                id: cursor.clone(),
                version: 1,
                category: "matrix".to_owned(),
                event: treatment.name.to_owned(),
                replicant_code: Some("R0".to_owned()),
                device_code: Some("D0".to_owned()),
                device_type: None,
                star: Some("STAR".to_owned()),
                location: Some("LOC".to_owned()),
                payload,
                created_at: "2026-08-26T00:00:00Z".to_owned(),
                extra: Default::default(),
            };
            let event = domain::account_event(&raw, Some(Realm::Live), observed_at()).value;
            let batch = (treatment.reduce)(&client, &event)
                .unwrap_or_else(|error| panic!("reduce {}: {error}", treatment.name));
            assert!(
                direct_effect_count(&batch) > 0,
                "{} produced no declared durable effect",
                treatment.name
            );

            let path = replay_test_path();
            let mut store =
                super::super::store::Store::open_file(&path).expect("open matrix store");
            let mut persisted_event = event.clone();
            persisted_event.id = domain::EventId::new("1-0");
            assert!(
                store
                    .apply_event_projection(&persisted_event, "1-0", &batch)
                    .unwrap_or_else(|error| panic!("persist {}: {error}", treatment.name))
            );
            let persisted_rows = store
                .projection_row_count()
                .unwrap_or_else(|error| panic!("count {}: {error}", treatment.name));
            assert!(
                persisted_rows > 0,
                "{} persisted no projection rows",
                treatment.name
            );
            drop(store);

            let mut reopened =
                super::super::store::Store::open_file(&path).expect("reopen matrix store");
            assert_eq!(
                reopened.projection_row_count().expect("reopened row count"),
                persisted_rows,
                "{} changed after reopen",
                treatment.name
            );
            assert!(
                reopened
                    .projection_batch_matches(&batch)
                    .unwrap_or_else(|error| {
                        panic!("verify reopened effect for {}: {error}", treatment.name)
                    }),
                "{} exact persisted effect differed after reopen",
                treatment.name
            );
            assert!(
                !reopened
                    .apply_event_projection(&persisted_event, "1-0", &batch)
                    .unwrap_or_else(|error| panic!("deduplicate {}: {error}", treatment.name))
            );
            assert_eq!(
                reopened
                    .projection_row_count()
                    .expect("deduplicated row count"),
                persisted_rows,
                "{} duplicate changed projection rows",
                treatment.name
            );
            assert!(
                reopened
                    .projection_batch_matches(&batch)
                    .unwrap_or_else(|error| {
                        panic!("verify deduplicated effect for {}: {error}", treatment.name)
                    }),
                "{} exact persisted effect differed after duplicate",
                treatment.name
            );
            assert_eq!(reopened.event_count().expect("deduplicated event count"), 1);
            drop(reopened);
            fs::remove_file(&path).expect("remove matrix database");
            fs::remove_file(super::super::store::history_database_path(&path))
                .expect("remove matrix history database");

            client
                .managed_state()
                .apply_event_projection(&event, &cursor, batch)
                .unwrap_or_else(|error| {
                    panic!("apply matrix context for {}: {error}", treatment.name)
                });
        }
        client.close().await.expect("close matrix client");
    }

    #[derive(Default)]
    struct RecordingTelemetry {
        samples: Mutex<Vec<EventTelemetrySample>>,
    }

    impl EventTelemetrySink for RecordingTelemetry {
        fn record(&self, sample: EventTelemetrySample) {
            self.samples
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(sample);
        }
    }

    #[tokio::test]
    async fn lifecycle_reducers_avoid_detail_gets_and_missing_state_falls_back_once() {
        let server = MockServer::start().await;
        for id in [
            "MISS_ATTACH",
            "MISS_DETACH",
            "MISS_STOW",
            "MISS_DEPLOY",
            "MISS_OWNER",
        ] {
            Mock::given(method("GET"))
                .and(path(format!("/v1/devices/{id}")))
                .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                    "error": "Device not found"
                })))
                .expect(1)
                .mount(&server)
                .await;
        }
        let telemetry = Arc::new(RecordingTelemetry::default());
        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_owned()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .event_telemetry_sink(telemetry.clone())
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only telemetry client");
        client
            .managed_state()
            .persist_devices(&[device("CARRIER"), device("CHILD")])
            .expect("seed lifecycle devices");

        let mut attached = game_event("1-0", "device.attached", Some("CARRIER"));
        attached
            .payload
            .insert("target_code".to_owned(), serde_json::json!("CHILD"));
        let mut detached = game_event("2-0", "device.detached", Some("CARRIER"));
        detached
            .payload
            .insert("target_code".to_owned(), serde_json::json!("CHILD"));
        let mut stowed = game_event("3-0", "device.stowed", Some("CHILD"));
        stowed.payload.insert(
            "stowed_in_device_code".to_owned(),
            serde_json::json!("CARRIER"),
        );
        let mut deployed = game_event("4-0", "device.deployed", Some("CHILD"));
        deployed.payload.insert(
            "deployed_from_device_code".to_owned(),
            serde_json::json!("CARRIER"),
        );
        let mut changed_owner = game_event("5-0", "device.changed_owner", Some("CHILD"));
        changed_owner
            .payload
            .insert("to_replicant".to_owned(), serde_json::json!("R2"));
        for event in [attached, detached] {
            apply_event(&client, &event).expect("apply complete lifecycle event");
            assert!(
                client
                    .managed_state()
                    .claim_reconciliation_work()
                    .expect("claim avoided work")
                    .is_none()
            );
        }
        apply_event(&client, &stowed).expect("apply complete stow event");
        let carrier = client
            .managed_state()
            .device(&DeviceKey::live("CARRIER".into()))
            .expect("carrier after stow");
        assert_eq!(carrier.value.stow_used, Some(1));
        assert_eq!(carrier.value.relationships.stowed_devices.len(), 1);

        apply_event(&client, &deployed).expect("apply complete deploy event");
        let carrier = client
            .managed_state()
            .device(&DeviceKey::live("CARRIER".into()))
            .expect("carrier after deploy");
        assert_eq!(carrier.value.stow_used, Some(0));
        assert!(carrier.value.relationships.stowed_devices.is_empty());

        apply_event(&client, &changed_owner).expect("apply complete owner event");
        assert!(
            client
                .managed_state()
                .claim_reconciliation_work()
                .expect("claim avoided work")
                .is_none()
        );

        let mut missing_attached = game_event("6-0", "device.attached", Some("CARRIER"));
        missing_attached
            .payload
            .insert("target_code".to_owned(), serde_json::json!("MISS_ATTACH"));
        let mut missing_detached = game_event("7-0", "device.detached", Some("CARRIER"));
        missing_detached
            .payload
            .insert("target_code".to_owned(), serde_json::json!("MISS_DETACH"));
        let mut missing_stowed = game_event("8-0", "device.stowed", Some("CHILD"));
        missing_stowed.payload.insert(
            "stowed_in_device_code".to_owned(),
            serde_json::json!("MISS_STOW"),
        );
        let mut missing_deployed = game_event("9-0", "device.deployed", Some("CHILD"));
        missing_deployed.payload.insert(
            "deployed_from_device_code".to_owned(),
            serde_json::json!("MISS_DEPLOY"),
        );
        let mut missing_owner = game_event("10-0", "device.changed_owner", Some("MISS_OWNER"));
        missing_owner.location = Some("LOC".to_owned());
        missing_owner
            .payload
            .insert("to_replicant".to_owned(), serde_json::json!("R2"));
        for (event, expected_id) in [
            (missing_attached, "MISS_ATTACH"),
            (missing_detached, "MISS_DETACH"),
            (missing_stowed, "MISS_STOW"),
            (missing_deployed, "MISS_DEPLOY"),
            (missing_owner, "MISS_OWNER"),
        ] {
            apply_event(&client, &event).expect("apply incomplete lifecycle event");
            let work = client
                .managed_state()
                .claim_reconciliation_work()
                .expect("claim fallback work")
                .expect("one fallback work item");
            assert_eq!(work.work_id, format!("device:{expected_id}"));
            process_reconciliation_work(&client, &work)
                .await
                .expect("404 fallback completes");
            client
                .managed_state()
                .complete_reconciliation_work(&work.work_id)
                .expect("complete fallback work");
        }
        server.verify().await;

        {
            let samples = telemetry
                .samples
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert_eq!(
                samples
                    .iter()
                    .filter(|sample| {
                        sample.metric == "event_reconciliation" && sample.outcome == "avoided"
                    })
                    .count(),
                5
            );
            let fallback_names = samples
                .iter()
                .filter(|sample| {
                    sample.metric == "event_reconciliation" && sample.outcome == "fallback"
                })
                .filter_map(|sample| sample.event_name.as_deref())
                .collect::<BTreeSet<_>>();
            assert_eq!(
                fallback_names,
                BTreeSet::from([
                    "device.attached",
                    "device.changed_owner",
                    "device.deployed",
                    "device.detached",
                    "device.stowed",
                ])
            );
        }
        client.close().await.expect("close telemetry client");
    }

    #[tokio::test]
    async fn device_relationship_events_update_both_sides_atomically() {
        let client = restore_only_client().await;
        let mut carrier = device("CARRIER");
        carrier.value.location = Some(domain::LocationKey::live(domain::LocationId::new(
            "RESTOCK",
        )));
        client
            .managed_state()
            .persist_devices(&[carrier, device("CHILD")])
            .expect("seed devices");

        let mut attached = game_event("1-0", "device.attached", Some("CARRIER"));
        attached
            .payload
            .insert("target_code".to_owned(), serde_json::json!("CHILD"));
        attached
            .payload
            .insert("target_type".to_owned(), serde_json::json!("survey_drone"));
        apply_event(&client, &attached).expect("apply attach");
        let carrier_key = DeviceKey::live(DeviceId::new("CARRIER"));
        let child_key = DeviceKey::live(DeviceId::new("CHILD"));
        let carrier = client
            .managed_state()
            .device(&carrier_key)
            .expect("carrier after attach");
        let child = client
            .managed_state()
            .device(&child_key)
            .expect("child after attach");
        assert_eq!(
            carrier.value.relationships.attached_devices.as_slice(),
            std::slice::from_ref(&child_key)
        );
        assert_eq!(
            child.value.relationships.attached_to,
            Some(carrier_key.clone())
        );
        assert_eq!(
            child.value.location,
            Some(domain::LocationKey::live(domain::LocationId::new(
                "RESTOCK"
            )))
        );

        let mut detached = game_event("2-0", "device.detached", Some("CARRIER"));
        detached
            .payload
            .insert("target_code".to_owned(), serde_json::json!("CHILD"));
        apply_event(&client, &detached).expect("apply detach");
        let carrier = client
            .managed_state()
            .device(&carrier_key)
            .expect("carrier after detach");
        let child = client
            .managed_state()
            .device(&child_key)
            .expect("child after detach");
        assert!(carrier.value.relationships.attached_devices.is_empty());
        assert_eq!(child.value.relationships.attached_to, None);
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn device_detach_propagates_known_carrier_location_and_clears_relationship() {
        let client = restore_only_client().await;
        let mut carrier = device("CARRIER");
        carrier.value.location = Some(domain::LocationKey::live(domain::LocationId::new(
            "RESTOCK",
        )));
        let carrier_key = carrier.value.key.clone();
        let child_key = DeviceKey::live(DeviceId::new("CHILD"));
        carrier
            .value
            .relationships
            .attached_devices
            .push(child_key.clone());
        let mut child = device("CHILD");
        child.value.location = Some(domain::LocationKey::live(domain::LocationId::new(
            "OLD-LOCATION",
        )));
        child.value.relationships.attached_to = Some(carrier_key.clone());
        client
            .managed_state()
            .persist_devices(&[carrier, child])
            .expect("seed attached devices");

        let mut detached = game_event("2-0", "device.detached", Some("CARRIER"));
        detached
            .payload
            .insert("target_code".to_owned(), serde_json::json!("CHILD"));
        apply_event(&client, &detached).expect("apply detach");

        let carrier = client
            .managed_state()
            .device(&carrier_key)
            .expect("carrier after detach");
        let child = client
            .managed_state()
            .device(&child_key)
            .expect("child after detach");
        assert!(carrier.value.relationships.attached_devices.is_empty());
        assert_eq!(child.value.relationships.attached_to, None);
        assert_eq!(
            child.value.location,
            Some(domain::LocationKey::live(domain::LocationId::new(
                "RESTOCK"
            )))
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn device_attach_preserves_child_location_when_carrier_location_unknown() {
        let client = restore_only_client().await;
        let mut child = device("CHILD");
        child.value.location = Some(domain::LocationKey::live(domain::LocationId::new(
            "CHILD-LOCATION",
        )));
        client
            .managed_state()
            .persist_devices(&[device("CARRIER"), child])
            .expect("seed devices");

        let mut attached = game_event("3-0", "device.attached", Some("CARRIER"));
        attached
            .payload
            .insert("target_code".to_owned(), serde_json::json!("CHILD"));
        apply_event(&client, &attached).expect("apply attach");

        let child = client
            .managed_state()
            .device(&DeviceKey::live(DeviceId::new("CHILD")))
            .expect("child after attach");
        assert_eq!(
            child.value.location,
            Some(domain::LocationKey::live(domain::LocationId::new(
                "CHILD-LOCATION"
            )))
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
    async fn sse_stream_does_not_inherit_the_ordinary_request_timeout() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/events/stream"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(75))
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string("id: 1-0\ndata: {\"id\":\"1-0\",\"version\":1,\"category\":\"mining\",\"event\":\"mining.started\",\"created_at\":\"2026-07-25T00:00:00Z\"}\n\n"),
            )
            .expect(2)
            .mount(&server)
            .await;

        // Reproduce the old shared-client policy: reqwest's total timeout
        // covers the streaming body and is classified as a local read timeout.
        let legacy_http = reqwest::Client::builder()
            .timeout(Duration::from_millis(25))
            .build()
            .expect("legacy HTTP client");
        let legacy_client = raw::Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .http_client(legacy_http)
            .build()
            .expect("legacy raw client");
        let legacy_error = match legacy_client.events().stream(None).await {
            Ok(_) => panic!("legacy total timeout should terminate SSE"),
            Err(error) => error,
        };
        assert_eq!(
            classify_sse_failure(&legacy_error).reason,
            "local_read_timeout"
        );

        let raw_client = raw::Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .request_timeout(Duration::from_millis(25))
            .build()
            .expect("raw client");

        let mut stream = timeout(Duration::from_secs(1), raw_client.events().stream(None))
            .await
            .expect("SSE headers are not governed by the ordinary request timeout")
            .expect("open SSE stream");
        let event = timeout(Duration::from_secs(1), stream.next())
            .await
            .expect("event arrives")
            .expect("stream item")
            .expect("valid event");
        assert_eq!(event.id, "1-0");
    }

    #[test]
    fn sse_failures_have_stable_reason_categories() {
        let parser = classify_sse_failure(&Error::Decode {
            message: "invalid event".to_owned(),
            status: Some(200),
            source: None,
        });
        assert_eq!(parser.reason, "parser_error");
        assert_eq!(parser.io_error_kind, "decode");

        let authentication = classify_sse_failure(&Error::Authentication {
            details: Box::default(),
        });
        assert_eq!(authentication.reason, "auth_failure");

        let upstream_http = classify_sse_failure(&Error::Contract {
            status: 503,
            details: Box::default(),
        });
        assert_eq!(upstream_http.reason, "upstream_http");
    }

    #[tokio::test]
    async fn clean_sse_close_records_an_explicit_disconnect_reason() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/events/stream"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "text/event-stream")
                    .set_body_string(": keepalive\n\n"),
            )
            .mount(&server)
            .await;
        let telemetry = Arc::new(RecordingTelemetry::default());
        let client = Client::builder()
            .event_telemetry_sink(telemetry.clone())
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only telemetry client");
        let applier = client
            .managed_events()
            .start_applier(client.downgrade())
            .expect("start event applier");
        client
            .register_task(applier)
            .await
            .expect("register event applier");
        let raw_client = raw::Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .build()
            .expect("raw client");
        let task = tokio::spawn(run_sse_loop(
            client.downgrade(),
            raw_client,
            Duration::from_millis(20),
            Duration::from_millis(20),
        ));

        timeout(Duration::from_secs(1), async {
            loop {
                if telemetry
                    .samples
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .any(|sample| {
                        sample.metric == "sse_disconnect" && sample.outcome == "upstream_close"
                    })
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("categorized disconnect telemetry");
        let detail = client
            .events()
            .last_disconnect_detail()
            .expect("disconnect detail")
            .expect("recorded disconnect");
        assert!(detail.starts_with("upstream closed after "));
        assert!(detail.contains("last event apply lag"));

        task.abort();
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
