//! Durable, resumable recovery of managed upstream projections.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::Notify;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    Error, Result,
    domain::{self, InventoryOwner, Observation, ObservationTime, Realm, Replicant},
    events::EventLogQuery,
    raw,
};

use super::{Client, ReadinessComponent, client::WeakClient, store::MessageMetadata};

const DEFAULT_READ_BUDGET: u32 = 60;
const MAX_READ_BUDGET: u32 = 60;
const LEASE_MILLIS: i64 = 300_000;

/// One durable recovery phase.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshPhase {
    /// Authenticated account profile.
    Account,
    /// Complete unfiltered owned-device census.
    Devices,
    /// Owned Replicant details discovered from the device census.
    Replicants,
    /// Complete global star catalogue.
    Stars,
    /// Account-specific explored-system knowledge.
    Systems,
    /// Fully surveyed system bodies.
    Bodies,
    /// Complete bounded account event history.
    Events,
    /// Complete available account inbox.
    Messages,
    /// Known and account-visible location details.
    Locations,
    /// Account inventory by location.
    Inventory,
    /// Account simulation history.
    Simulations,
}

impl RefreshPhase {
    /// The canonical full recovery order.
    pub const FULL: [Self; 11] = [
        Self::Account,
        Self::Devices,
        Self::Replicants,
        Self::Stars,
        Self::Systems,
        Self::Bodies,
        Self::Events,
        Self::Messages,
        Self::Locations,
        Self::Inventory,
        Self::Simulations,
    ];

    /// Stable snake-case phase name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Devices => "devices",
            Self::Replicants => "replicants",
            Self::Stars => "stars",
            Self::Systems => "systems",
            Self::Bodies => "bodies",
            Self::Events => "events",
            Self::Messages => "messages",
            Self::Locations => "locations",
            Self::Inventory => "inventory",
            Self::Simulations => "simulations",
        }
    }

    fn dependencies(self) -> &'static [Self] {
        match self {
            Self::Account => &[],
            Self::Devices => &[Self::Account],
            Self::Replicants => &[Self::Devices],
            Self::Stars => &[Self::Account],
            Self::Systems => &[Self::Replicants, Self::Stars],
            Self::Bodies => &[Self::Systems],
            Self::Events | Self::Messages | Self::Simulations => &[Self::Account],
            Self::Locations => &[Self::Devices, Self::Replicants],
            Self::Inventory => &[Self::Locations],
        }
    }
}

impl fmt::Display for RefreshPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for RefreshPhase {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::FULL
            .into_iter()
            .find(|phase| phase.as_str() == value)
            .ok_or_else(|| format!("unknown refresh phase `{value}`"))
    }
}

/// Whether a refresh applies observations or only computes a proposal.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshMode {
    /// Commit upstream observations and approved guarded removals.
    Apply,
    /// Write only refresh control, checkpoint, and staging rows.
    DryRun,
}

impl RefreshMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Apply => "apply",
            Self::DryRun => "dry_run",
        }
    }
}

/// A request to start one immutable durable refresh run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshRequest {
    /// Requested phases. Empty expands to [`RefreshPhase::FULL`].
    pub phases: BTreeSet<RefreshPhase>,
    /// Apply or non-mutating dry-run mode.
    pub mode: RefreshMode,
    /// Hard auxiliary safe-read attempt budget, from 1 through 60 per minute.
    pub read_requests_per_minute: u32,
}

impl Default for RefreshRequest {
    fn default() -> Self {
        Self {
            phases: BTreeSet::new(),
            mode: RefreshMode::Apply,
            read_requests_per_minute: DEFAULT_READ_BUDGET,
        }
    }
}

/// Opaque durable refresh-run identity.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct RefreshRunId(String);

impl RefreshRunId {
    fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Returns the stable path-safe run identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RefreshRunId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for RefreshRunId {
    type Err = String;

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let value = value.trim();
        if value.is_empty() {
            Err("refresh run ID must not be empty".to_owned())
        } else {
            Ok(Self(value.to_owned()))
        }
    }
}

/// Durable overall run state.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshRunState {
    /// Waiting for the database worker slot.
    Queued,
    /// Actively executing one phase.
    Running,
    /// Waiting for an upstream retry deadline.
    BackingOff,
    /// Guarded shrink requires an exact-digest approval.
    AwaitingApproval,
    /// A requested dependency is unavailable.
    Blocked,
    /// Applied run completed.
    Completed,
    /// Dry-run completed without authoritative mutations.
    CompletedDryRun,
    /// Operator cancellation reached a checkpoint boundary.
    Cancelled,
    /// Non-retryable failure.
    Failed,
}

impl RefreshRunState {
    pub(crate) fn parse(value: &str) -> std::result::Result<Self, String> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "backing_off" => Ok(Self::BackingOff),
            "awaiting_approval" => Ok(Self::AwaitingApproval),
            "blocked" => Ok(Self::Blocked),
            "completed" => Ok(Self::Completed),
            "completed_dry_run" => Ok(Self::CompletedDryRun),
            "cancelled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            _ => Err(format!("invalid refresh run state `{value}`")),
        }
    }

    /// Whether no worker will resume this run.
    #[must_use]
    pub const fn terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::CompletedDryRun | Self::Cancelled | Self::Failed
        )
    }
}

/// Durable state of one phase.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshPhaseState {
    /// Dependency-complete work has not started.
    Pending,
    /// The phase owns the worker lease.
    Running,
    /// Waiting for retry.
    BackingOff,
    /// Waiting for guarded shrink approval.
    AwaitingApproval,
    /// A dependency did not complete.
    Blocked,
    /// Phase completed.
    Complete,
    /// Phase stopped by cancellation.
    Cancelled,
    /// Phase failed.
    Failed,
}

impl RefreshPhaseState {
    pub(crate) fn parse(value: &str) -> std::result::Result<Self, String> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "backing_off" => Ok(Self::BackingOff),
            "awaiting_approval" => Ok(Self::AwaitingApproval),
            "blocked" => Ok(Self::Blocked),
            "complete" => Ok(Self::Complete),
            "cancelled" => Ok(Self::Cancelled),
            "failed" => Ok(Self::Failed),
            _ => Err(format!("invalid refresh phase state `{value}`")),
        }
    }
}

/// Proposed and applied semantic changes.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RefreshDelta {
    /// New normalized rows observed upstream.
    pub proposed_inserts: u64,
    /// Existing normalized values changed upstream.
    pub proposed_updates: u64,
    /// Guarded absence candidates.
    pub proposed_tombstones: u64,
    /// New rows committed by an apply run.
    pub applied_inserts: u64,
    /// Existing rows changed by an apply run.
    pub applied_updates: u64,
    /// Guarded removals committed by an apply run.
    pub applied_tombstones: u64,
}

impl std::ops::AddAssign for RefreshDelta {
    fn add_assign(&mut self, other: Self) {
        self.proposed_inserts += other.proposed_inserts;
        self.proposed_updates += other.proposed_updates;
        self.proposed_tombstones += other.proposed_tombstones;
        self.applied_inserts += other.applied_inserts;
        self.applied_updates += other.applied_updates;
        self.applied_tombstones += other.applied_tombstones;
    }
}

/// Readiness established by the requested recovery plan.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RefreshReadiness {
    /// Account or device prerequisites are incomplete.
    Unavailable,
    /// Account and devices applied, but requested work remains.
    RestBaseline,
    /// Every requested phase and dependency completed.
    Complete,
}

/// Durable report for one phase.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RefreshPhaseStatus {
    /// Phase identity.
    pub phase: RefreshPhase,
    /// Durable phase state.
    pub status: RefreshPhaseState,
    /// Completed response pages.
    pub pages: u64,
    /// Completed normalized items.
    pub items: u64,
    /// Safe-read attempts charged to this phase.
    pub request_attempts: u64,
    /// Proposed and applied deltas.
    pub delta: RefreshDelta,
    /// Retry deadline as Unix milliseconds.
    pub retry_not_before_ms: Option<i64>,
    /// Exact digest required for guarded approval.
    pub approval_digest: Option<String>,
    /// Sanitized failure category.
    pub failure_kind: Option<String>,
}

/// Durable run status and phase reports.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RefreshRunStatus {
    /// Run identity.
    pub run_id: RefreshRunId,
    /// Immutable execution mode.
    pub mode: RefreshMode,
    /// Overall durable state.
    pub status: RefreshRunState,
    /// Expanded requested phases in canonical order.
    pub requested_phases: Vec<RefreshPhase>,
    /// Current phase, when actively or resumably positioned.
    pub current_phase: Option<RefreshPhase>,
    /// Hard safe-read attempt budget.
    pub read_requests_per_minute: u32,
    /// Total safe-read attempts.
    pub request_attempts: u64,
    /// Aggregate proposed and applied deltas.
    pub delta: RefreshDelta,
    /// Recovery readiness independent of SSE health.
    pub readiness: RefreshReadiness,
    /// Fixed event-history upper bound, if captured.
    pub history_backfilled_through: Option<String>,
    /// Current managed live catch-up component.
    pub live_catchup: ReadinessComponent,
    /// Retry deadline as Unix milliseconds.
    pub retry_not_before_ms: Option<i64>,
    /// Sanitized run failure category.
    pub failure_kind: Option<String>,
    /// Creation time as Unix milliseconds.
    pub created_at_ms: i64,
    /// Last durable update as Unix milliseconds.
    pub updated_at_ms: i64,
    /// Terminal completion time as Unix milliseconds.
    pub completed_at_ms: Option<i64>,
    /// Ordered phase reports.
    pub phases: Vec<RefreshPhaseStatus>,
}

/// Managed durable-refresh entry point returned by [`Client::refresh`].
#[derive(Clone, Debug)]
pub struct RefreshClient {
    client: Client,
}

impl RefreshClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists durable runs, newest first. `limit` is clamped to `1..=100`.
    pub async fn list(&self, limit: usize) -> Result<Vec<RefreshRunStatus>> {
        let live_catchup = self.client.readiness().event_catchup;
        self.client
            .managed_store()
            .execute(move |store| store.list_refresh_runs(limit.clamp(1, 100), live_catchup))
            .await
            .map_err(super::client::store_error)
    }

    /// Creates an immutable durable run and wakes the single database worker.
    pub async fn start(&self, request: RefreshRequest) -> Result<RefreshRunStatus> {
        self.client.ensure_open()?;
        if !(1..=MAX_READ_BUDGET).contains(&request.read_requests_per_minute) {
            return Err(Error::Configuration {
                message: "refresh read budget must be between 1 and 60 requests per minute".into(),
            });
        }
        let phases = expand_phases(&request.phases);
        let run_id = RefreshRunId::new();
        let mode = request.mode;
        let budget = request.read_requests_per_minute;
        let now = unix_millis();
        let created_id = run_id.clone();
        self.client
            .managed_store()
            .execute(move |store| store.create_refresh_run(&created_id, mode, &phases, budget, now))
            .await
            .map_err(super::client::store_error)?;
        self.client.refresh_notify().notify_one();
        self.status(&run_id)
            .await?
            .ok_or_else(|| Error::Persistence {
                message: "new refresh run was not durably readable".into(),
            })
    }

    /// Reads one durable run.
    pub async fn status(&self, run_id: &RefreshRunId) -> Result<Option<RefreshRunStatus>> {
        let run_id = run_id.clone();
        let live_catchup = self.client.readiness().event_catchup;
        self.client
            .managed_store()
            .execute(move |store| store.refresh_run_status(&run_id, live_catchup))
            .await
            .map_err(super::client::store_error)
    }

    /// Approves the exact currently staged guarded shrink digest.
    pub async fn approve(
        &self,
        run_id: &RefreshRunId,
        phase: RefreshPhase,
        digest: &str,
    ) -> Result<RefreshRunStatus> {
        if digest.is_empty() || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(Error::Configuration {
                message: "refresh approval digest must be non-empty hexadecimal".into(),
            });
        }
        let run_id = run_id.clone();
        let stored_run_id = run_id.clone();
        let digest = digest.to_ascii_lowercase();
        let now = unix_millis();
        self.client
            .managed_store()
            .execute(move |store| store.approve_refresh_phase(&stored_run_id, phase, &digest, now))
            .await
            .map_err(super::client::store_error)?;
        self.client.refresh_notify().notify_one();
        self.status(&run_id)
            .await?
            .ok_or_else(|| Error::Configuration {
                message: "unknown refresh run".into(),
            })
    }

    /// Requests cancellation at the next durable page or item boundary.
    pub async fn cancel(&self, run_id: &RefreshRunId) -> Result<RefreshRunStatus> {
        let run_id = run_id.clone();
        let stored_run_id = run_id.clone();
        let now = unix_millis();
        self.client
            .managed_store()
            .execute(move |store| store.cancel_refresh_run(&stored_run_id, now))
            .await
            .map_err(super::client::store_error)?;
        self.client.refresh_notify().notify_one();
        self.status(&run_id)
            .await?
            .ok_or_else(|| Error::Configuration {
                message: "unknown refresh run".into(),
            })
    }
}

pub(crate) struct RefreshEngine {
    notify: Arc<Notify>,
}

impl RefreshEngine {
    pub(crate) fn new() -> Self {
        Self {
            notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn notify(&self) -> &Notify {
        &self.notify
    }

    pub(crate) fn notify_arc(&self) -> Arc<Notify> {
        Arc::clone(&self.notify)
    }
}

pub(crate) async fn spawn(client: &Client) -> Result<()> {
    let weak = client.downgrade();
    let notify = client.inner_refresh_notify();
    let task = tokio::spawn(async move { worker(weak, notify).await });
    client.register_task(task).await
}

async fn worker(client: WeakClient, notify: Arc<Notify>) {
    let owner = Uuid::new_v4().to_string();
    loop {
        let Some(live) = client.upgrade() else {
            return;
        };
        let now = unix_millis();
        let claim = live
            .managed_store()
            .execute({
                let owner = owner.clone();
                move |store| store.claim_refresh_run(&owner, now, now + LEASE_MILLIS)
            })
            .await;
        let run_id = match claim {
            Ok(Some(run_id)) => run_id,
            Ok(None) => {
                drop(live);
                tokio::select! {
                    () = notify.notified() => {}
                    () = tokio::time::sleep(Duration::from_secs(1)) => {}
                }
                continue;
            }
            Err(error) => {
                warn!(target: "replicant_client::refresh", %error, "refresh worker could not claim work");
                drop(live);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        info!(target: "replicant_client::refresh", run_id = %run_id, "refresh worker claimed run");
        if let Err(error) = execute_run(&live, &run_id, &owner).await {
            let attempts = live
                .managed_raw()
                .rate_limits()
                .refresh_permit_count(run_id.as_str())
                .await;
            if let Ok(Some(status)) = live.refresh().status(&run_id).await
                && let Some(phase) = status.current_phase
            {
                let _ = live
                    .managed_store()
                    .execute({
                        let run_id = run_id.clone();
                        move |store| store.update_refresh_attempts(&run_id, phase, attempts)
                    })
                    .await;
            }
            let result = if let Error::RateLimited { retry_after, .. } = &error {
                let delay = retry_after.unwrap_or(Duration::from_secs(60));
                live.managed_store()
                    .execute({
                        let run_id = run_id.clone();
                        move |store| {
                            store.backoff_refresh_run(
                                &run_id,
                                unix_millis().saturating_add(
                                    i64::try_from(delay.as_millis()).unwrap_or(i64::MAX),
                                ),
                                unix_millis(),
                            )
                        }
                    })
                    .await
            } else if matches!(
                &error,
                Error::Configuration { message } if message == "refresh cancelled"
            ) {
                live.managed_store()
                    .execute({
                        let run_id = run_id.clone();
                        move |store| store.finish_cancelled_refresh_run(&run_id, unix_millis())
                    })
                    .await
            } else {
                let failure = refresh_failure_kind(&error);
                live.managed_store()
                    .execute({
                        let run_id = run_id.clone();
                        move |store| store.fail_refresh_run(&run_id, &failure, unix_millis())
                    })
                    .await
            };
            if let Err(store_error) = result {
                warn!(target: "replicant_client::refresh", run_id = %run_id, %store_error, "could not persist refresh failure state");
            }
            if !matches!(&error, Error::RateLimited { .. }) {
                live.managed_raw()
                    .rate_limits()
                    .clear_refresh_schedule(run_id.as_str())
                    .await;
            }
            warn!(target: "replicant_client::refresh", run_id = %run_id, %error, "refresh run paused or failed");
        }
        drop(live);
    }
}

async fn execute_run(client: &Client, run_id: &RefreshRunId, owner: &str) -> Result<()> {
    let status = client
        .refresh()
        .status(run_id)
        .await?
        .ok_or_else(|| Error::Configuration {
            message: "claimed refresh run disappeared".into(),
        })?;
    let budgeted = client.with_refresh_budget(run_id.as_str(), status.read_requests_per_minute);
    for phase in status.requested_phases.clone() {
        let current =
            client
                .refresh()
                .status(run_id)
                .await?
                .ok_or_else(|| Error::Configuration {
                    message: "refresh run disappeared".into(),
                })?;
        if current.status.terminal() || current.status == RefreshRunState::AwaitingApproval {
            return Ok(());
        }
        if current
            .phases
            .iter()
            .any(|item| item.phase == phase && item.status == RefreshPhaseState::Complete)
        {
            continue;
        }
        if current.phases.iter().any(|item| {
            phase.dependencies().contains(&item.phase) && item.status != RefreshPhaseState::Complete
        }) {
            client
                .managed_store()
                .execute({
                    let run_id = run_id.clone();
                    move |store| store.block_refresh_phase(&run_id, phase, unix_millis())
                })
                .await
                .map_err(super::client::store_error)?;
            return Ok(());
        }
        let owner = owner.to_owned();
        let cancelled = client
            .managed_store()
            .execute({
                let run_id = run_id.clone();
                move |store| store.begin_refresh_phase(&run_id, phase, &owner, unix_millis())
            })
            .await
            .map_err(super::client::store_error)?;
        if cancelled {
            return Ok(());
        }
        execute_phase(&budgeted, run_id, status.mode, phase).await?;
        let attempts = budgeted
            .managed_raw()
            .rate_limits()
            .refresh_permit_count(run_id.as_str())
            .await;
        let paused = client
            .managed_store()
            .execute({
                let run_id = run_id.clone();
                move |store| {
                    store.update_refresh_attempts(&run_id, phase, attempts)?;
                    store.complete_refresh_phase(&run_id, phase, unix_millis())
                }
            })
            .await
            .map_err(super::client::store_error)?;
        if paused {
            return Ok(());
        }
    }
    client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| store.complete_refresh_run(&run_id, unix_millis())
        })
        .await
        .map_err(super::client::store_error)?;
    budgeted
        .managed_raw()
        .rate_limits()
        .clear_refresh_schedule(run_id.as_str())
        .await;
    Ok(())
}

async fn execute_phase(
    client: &Client,
    run_id: &RefreshRunId,
    mode: RefreshMode,
    phase: RefreshPhase,
) -> Result<()> {
    match phase {
        RefreshPhase::Account => refresh_account(client, run_id, mode).await,
        RefreshPhase::Devices => refresh_devices(client, run_id, mode).await,
        RefreshPhase::Replicants => refresh_replicants(client, run_id, mode).await,
        RefreshPhase::Stars => refresh_stars(client, run_id, mode).await,
        RefreshPhase::Systems => refresh_systems(client, run_id, mode).await,
        RefreshPhase::Bodies => refresh_bodies(client, run_id, mode).await,
        RefreshPhase::Events => refresh_events(client, run_id, mode).await,
        RefreshPhase::Messages => refresh_messages(client, run_id, mode).await,
        RefreshPhase::Locations => refresh_locations(client, run_id, mode).await,
        RefreshPhase::Inventory => refresh_inventory(client, run_id, mode).await,
        RefreshPhase::Simulations => refresh_simulations(client, run_id, mode).await,
    }
}

async fn refresh_account(client: &Client, run_id: &RefreshRunId, mode: RefreshMode) -> Result<()> {
    let response = client.managed_raw().accounts().me().await?;
    let id = response
        .value
        .email
        .clone()
        .filter(|email| !email.is_empty())
        .map(crate::domain::AccountId::new)
        .or_else(|| {
            client
                .managed_state()
                .account()
                .map(|account| account.value.id)
        })
        .ok_or_else(|| decode_error("account refresh response omitted account identity"))?;
    let observation = domain::account_me(&response.value, id, ObservationTime::now());
    stage_observation(
        client,
        run_id,
        RefreshPhase::Account,
        "account",
        &observation,
        mode,
        |client| client.managed_state().account().map(|value| value.value),
    )
    .await?;
    if mode == RefreshMode::Apply {
        client
            .managed_state()
            .persist_account(observation)
            .map_err(super::client::store_error)?;
    }
    checkpoint(
        client,
        run_id,
        RefreshPhase::Account,
        json!({"state":"committed"}),
        (1, 1),
        (true, true),
    )
    .await
}

async fn refresh_devices(client: &Client, run_id: &RefreshRunId, mode: RefreshMode) -> Result<()> {
    let saved = load_checkpoint(client, run_id, RefreshPhase::Devices).await?;
    let mut cursor = saved.get("next_cursor").and_then(Value::as_i64);
    let mut pages = saved.get("pages").and_then(Value::as_u64).unwrap_or(0);
    let mut seen = client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| store.refresh_stage_keys(&run_id, RefreshPhase::Devices, false)
        })
        .await
        .map_err(super::client::store_error)?
        .into_iter()
        .collect::<BTreeSet<_>>();
    loop {
        ensure_not_cancelled(client, run_id).await?;
        let query = raw::devices::DeviceListQuery {
            cursor,
            limit: Some(50),
            ..Default::default()
        };
        let response = client.managed_raw().devices().list(&query).await?;
        let next = response.value.next_cursor;
        if next.is_some() && next == cursor {
            return Err(decode_error("device refresh cursor did not advance"));
        }
        let collection = domain::device_collection(
            &response.value,
            Realm::Live,
            false,
            next.is_none(),
            ObservationTime::now(),
        )
        .map_err(|_| decode_error("device refresh response is invalid"))?;
        for observation in &collection.members {
            let key = observation.value.key.id.as_str().to_owned();
            seen.insert(key.clone());
            let existing = client
                .managed_state()
                .device(&observation.value.key)
                .map(|value| value.value);
            stage_value(
                client,
                run_id,
                RefreshPhase::Devices,
                &key,
                observation,
                existing.as_ref(),
                mode,
            )
            .await?;
        }
        if mode == RefreshMode::Apply {
            client
                .managed_state()
                .persist_devices(&collection.members)
                .map_err(super::client::store_error)?;
        }
        pages += 1;
        cursor = next;
        checkpoint(
            client,
            run_id,
            RefreshPhase::Devices,
            json!({"next_cursor":cursor,"pages":pages,"seen_count":seen.len()}),
            (pages, seen.len() as u64),
            (cursor.is_none(), true),
        )
        .await?;
        if cursor.is_none() {
            finalize_devices(client, run_id, mode, &seen).await?;
            return Ok(());
        }
    }
}

async fn refresh_replicants(
    client: &Client,
    run_id: &RefreshRunId,
    mode: RefreshMode,
) -> Result<()> {
    let saved = load_checkpoint(client, run_id, RefreshPhase::Replicants).await?;
    let mut codes = BTreeSet::new();
    for payload in
        staged_payloads::<Observation<crate::domain::Device>>(client, run_id, RefreshPhase::Devices)
            .await?
    {
        if let Some(replicant) = payload.value.relationships.assigned_replicant {
            codes.insert(replicant.id.as_str().to_owned());
        }
        if let Some(replicant) = payload.value.relationships.hosting_replicant {
            codes.insert(replicant.id.as_str().to_owned());
        }
    }
    for replicant in client.managed_state().replicants() {
        if replicant.value.private.is_some() {
            codes.insert(replicant.value.key.id.as_str().to_owned());
        }
    }
    let codes = codes.into_iter().collect::<Vec<_>>();
    let digest = membership_digest(codes.iter().map(String::as_str));
    if let Some(previous) = saved.get("codes_digest").and_then(Value::as_str)
        && previous != digest
    {
        return Err(decode_error(
            "Replicant discovery membership changed while resuming",
        ));
    }
    let mut index = saved.get("next_index").and_then(Value::as_u64).unwrap_or(0) as usize;
    while let Some(code) = codes.get(index) {
        ensure_not_cancelled(client, run_id).await?;
        let response = client.managed_raw().replicants().get(code).await?;
        let observation =
            domain::owned_replicant_detail(&response.value, Realm::Live, ObservationTime::now())
                .map_err(|_| decode_error("owned Replicant refresh response is invalid"))?;
        let existing = client
            .managed_state()
            .replicant(&observation.value.key)
            .map(|value| value.value);
        stage_value(
            client,
            run_id,
            RefreshPhase::Replicants,
            code,
            &observation,
            existing.as_ref(),
            mode,
        )
        .await?;
        if mode == RefreshMode::Apply {
            client
                .managed_state()
                .persist_replicant(observation)
                .map_err(super::client::store_error)?;
        }
        index += 1;
        checkpoint(
            client,
            run_id,
            RefreshPhase::Replicants,
            json!({"codes_digest":digest,"next_index":index}),
            (u64::from(index > 0), index as u64),
            (index == codes.len(), true),
        )
        .await?;
    }
    if codes.is_empty() {
        checkpoint(
            client,
            run_id,
            RefreshPhase::Replicants,
            json!({"codes_digest":digest,"next_index":0}),
            (0, 0),
            (true, true),
        )
        .await?;
    }
    Ok(())
}

async fn refresh_stars(client: &Client, run_id: &RefreshRunId, mode: RefreshMode) -> Result<()> {
    let response = client.managed_raw().galaxy().catalogue().await?;
    let declared = response
        .value
        .total
        .ok_or_else(|| decode_error("star catalogue omitted total"))?;
    let mut observations = BTreeMap::new();
    for star in &response.value.stars {
        let observation = domain::catalogue_star(star, Realm::Live, ObservationTime::now())
            .map_err(|_| decode_error("star catalogue contains an invalid row"))?;
        observations.insert(observation.value.key.id.as_str().to_owned(), observation);
    }
    if declared < 0 || declared as usize != observations.len() {
        return Err(decode_error(
            "star catalogue total does not match unique normalized rows",
        ));
    }
    let keys = observations.keys().map(String::as_str).collect::<Vec<_>>();
    let digest = membership_digest(keys);
    for (key, observation) in &observations {
        let existing = client
            .managed_state()
            .catalogue()
            .into_iter()
            .find(|value| value.value.key == observation.value.key)
            .map(|value| value.value);
        stage_value(
            client,
            run_id,
            RefreshPhase::Stars,
            key,
            observation,
            existing.as_ref(),
            mode,
        )
        .await?;
    }
    checkpoint(client, run_id, RefreshPhase::Stars, json!({"generated_at":response.value.generated_at,"declared_total":declared,"unique_count":observations.len(),"membership_digest":digest,"response_staged":true}), (1, observations.len() as u64), (true, true)).await?;
    finalize_stars(
        client,
        run_id,
        mode,
        observations.into_values().collect(),
        response.value.generated_at,
    )
    .await
}

async fn refresh_systems(client: &Client, run_id: &RefreshRunId, mode: RefreshMode) -> Result<()> {
    let replicants =
        staged_payloads::<Observation<Replicant>>(client, run_id, RefreshPhase::Replicants).await?;
    let mut candidates = replicants
        .into_iter()
        .map(|value| value.value.key)
        .collect::<Vec<_>>();
    candidates.sort();
    let candidate_digest =
        membership_digest(candidates.iter().map(|candidate| candidate.id.as_str()));
    let saved = load_checkpoint(client, run_id, RefreshPhase::Systems).await?;
    if saved
        .get("candidate_replicants_digest")
        .and_then(Value::as_str)
        .is_some_and(|previous| previous != candidate_digest)
    {
        return Err(decode_error(
            "Replicant census candidate membership changed while resuming",
        ));
    }
    let catalogue_count = if mode == RefreshMode::DryRun {
        staged_payloads::<Observation<crate::domain::Star>>(client, run_id, RefreshPhase::Stars)
            .await?
            .len()
    } else {
        client.managed_state().catalogue().len()
    };
    let mut census = client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| store.refresh_stage_prefix(&run_id, RefreshPhase::Systems, "census:")
        })
        .await
        .map_err(super::client::store_error)?
        .into_iter()
        .map(|(key, payload)| {
            serde_json::from_str::<raw::galaxy::StarItem>(&payload)
                .map(|item| (key.trim_start_matches("census:").to_owned(), item))
                .map_err(|_| decode_error("staged Replicant star census row is invalid"))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut total_pages = saved
        .get("total_pages")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let mut selected = saved
        .get("replicant")
        .and_then(Value::as_str)
        .and_then(|id| {
            candidates
                .iter()
                .find(|candidate| candidate.id.as_str() == id)
        })
        .cloned();
    let resume_index = saved
        .get("replicant_index")
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;
    if selected.is_none() {
        for (candidate_index, candidate) in candidates.iter().enumerate().skip(resume_index) {
            let resume_page = if candidate_index == resume_index {
                saved.get("page").and_then(Value::as_i64).unwrap_or(1)
            } else {
                1
            };
            let mut page = resume_page;
            let mut reported_pages = saved
                .get("total_pages")
                .and_then(Value::as_i64)
                .filter(|_| page > 1);
            let mut total_stars = saved
                .get("total_stars")
                .and_then(Value::as_i64)
                .filter(|_| page > 1);
            loop {
                let query = raw::galaxy::StarListQuery {
                    page: Some(page),
                    per_page: Some(50),
                };
                let response = match client
                    .managed_raw()
                    .replicants()
                    .stars(candidate.id.as_str(), &query)
                    .await
                {
                    Ok(response) => response,
                    Err(error) if page == 1 && census_capability_unavailable(&error) => {
                        census.clear();
                        client
                            .managed_store()
                            .execute({
                                let run_id = run_id.clone();
                                move |store| {
                                    store.discard_refresh_stage_prefix(
                                        &run_id,
                                        RefreshPhase::Systems,
                                        "census:",
                                    )
                                }
                            })
                            .await
                            .map_err(super::client::store_error)?;
                        checkpoint(
                            client,
                            run_id,
                            RefreshPhase::Systems,
                            json!({
                                "candidate_replicants_digest":candidate_digest,
                                "replicant_index":candidate_index + 1,
                                "capability_failure":true,
                                "page":1
                            }),
                            (0, 0),
                            (false, true),
                        )
                        .await?;
                        break;
                    }
                    Err(error) => return Err(error),
                };
                let response_page = response
                    .value
                    .page
                    .ok_or_else(|| decode_error("Replicant star census omitted page"))?;
                let response_pages = response
                    .value
                    .total_pages
                    .ok_or_else(|| decode_error("Replicant star census omitted total_pages"))?;
                let response_total = response
                    .value
                    .total_stars
                    .ok_or_else(|| decode_error("Replicant star census omitted total_stars"))?;
                if response_page != page
                    || response_pages < page
                    || reported_pages.is_some_and(|value| value != response_pages)
                    || total_stars.is_some_and(|value| value != response_total)
                {
                    return Err(decode_error(
                        "Replicant star census pagination changed while traversing",
                    ));
                }
                if response_total < 0 || response_total as usize != catalogue_count {
                    return Err(decode_error(
                        "Replicant star census total_stars does not match the completed catalogue",
                    ));
                }
                reported_pages = Some(response_pages);
                total_stars = Some(response_total);
                total_pages = response_pages;
                for item in response.value.stars {
                    let designation = item
                        .designation
                        .as_deref()
                        .ok_or_else(|| {
                            decode_error("Replicant star census row omitted designation")
                        })?
                        .to_owned();
                    if census.contains_key(&designation) {
                        return Err(decode_error(
                            "Replicant star census contains duplicate rows",
                        ));
                    }
                    stage_marker(
                        client,
                        run_id,
                        RefreshPhase::Systems,
                        &format!("census:{designation}"),
                        &item,
                    )
                    .await?;
                    census.insert(designation, item);
                }
                page += 1;
                checkpoint(
                    client,
                    run_id,
                    RefreshPhase::Systems,
                    json!({
                        "candidate_replicants_digest":candidate_digest,
                        "replicant_index":candidate_index,
                        "page":page,
                        "total_pages":response_pages,
                        "total_stars":response_total
                    }),
                    (response_page as u64, census.len() as u64),
                    (false, true),
                )
                .await?;
                if response_page >= response_pages {
                    if census.len() != response_total as usize {
                        return Err(decode_error(
                            "Replicant star census unique rows do not match total_stars",
                        ));
                    }
                    checkpoint(
                        client,
                        run_id,
                        RefreshPhase::Systems,
                        json!({
                            "candidate_replicants_digest":candidate_digest,
                            "replicant_index":candidate_index,
                            "replicant":candidate.id.as_str(),
                            "page":response_pages + 1,
                            "total_pages":response_pages,
                            "total_stars":response_total,
                            "unresolved_star_index":0
                        }),
                        (response_pages as u64, census.len() as u64),
                        (false, true),
                    )
                    .await?;
                    selected = Some(candidate.clone());
                    break;
                }
            }
            if selected.is_some() {
                break;
            }
        }
    }
    let replicant = selected.ok_or_else(|| {
        decode_error("no owned Replicant can produce a complete account star census")
    })?;
    let restored_knowledge = client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| store.refresh_stage_prefix(&run_id, RefreshPhase::Systems, "knowledge:")
        })
        .await
        .map_err(super::client::store_error)?
        .into_iter()
        .map(|(key, payload)| {
            serde_json::from_str::<Observation<crate::domain::Star>>(&payload)
                .map(|star| (key.trim_start_matches("knowledge:").to_owned(), star))
                .map_err(|_| decode_error("staged account star knowledge is invalid"))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;
    let mut explored = restored_knowledge
        .values()
        .filter(|star| star.value.explored == Some(true))
        .map(|star| star.value.key.id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    for (unresolved_index, (designation, mut item)) in census.into_iter().enumerate() {
        if restored_knowledge.contains_key(&designation) {
            continue;
        }
        if item.explored.is_none() {
            item = client
                .managed_raw()
                .replicants()
                .star(replicant.id.as_str(), &designation)
                .await?
                .value
                .star
                .ok_or_else(|| decode_error("Replicant star detail omitted star"))?;
            if item.explored.is_none() {
                return Err(decode_error(
                    "Replicant star detail left explored knowledge unresolved",
                ));
            }
        }
        let knowledge = domain::replicant_star_knowledge(
            &item,
            replicant.clone(),
            Realm::Live,
            ObservationTime::now(),
        )
        .map_err(|_| decode_error("Replicant star knowledge is invalid"))?;
        if knowledge.value.explored == Some(true) {
            explored.insert(knowledge.value.star.id.as_str().to_owned());
        }
        if mode == RefreshMode::Apply {
            client
                .managed_state()
                .persist_star_knowledge(knowledge.clone())
                .map_err(super::client::store_error)?;
        }
        let star = domain::account_star_from_knowledge(knowledge);
        stage_value(
            client,
            run_id,
            RefreshPhase::Systems,
            &format!("knowledge:{designation}"),
            &star,
            None::<&crate::domain::Star>,
            mode,
        )
        .await?;
        checkpoint(
            client,
            run_id,
            RefreshPhase::Systems,
            json!({
                "candidate_replicants_digest":candidate_digest,
                "replicant":replicant.id.as_str(),
                "replicant_index":resume_index,
                "page":total_pages + 1,
                "total_pages":total_pages,
                "total_stars":catalogue_count,
                "unresolved_star_index":unresolved_index + 1,
                "explored_digest":membership_digest(explored.iter().map(String::as_str))
            }),
            (total_pages as u64, unresolved_index as u64 + 1),
            (false, true),
        )
        .await?;
    }
    let sorted = explored.into_iter().collect::<Vec<_>>();
    let system_start = saved
        .get("system_index")
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;
    for (index, designation) in sorted.iter().enumerate().skip(system_start) {
        let response = client
            .managed_raw()
            .locations()
            .get(designation, None)
            .await?;
        let observation =
            domain::location_detail(&response.value, Realm::Live, ObservationTime::now())
                .map_err(|_| decode_error("explored system location is invalid"))?;
        let complete = matches!(
            (response.value.planets_scanned, response.value.planets_total),
            (Some(scanned), Some(total)) if scanned == total
        );
        let key = format!("system:{designation}");
        stage_value(
            client,
            run_id,
            RefreshPhase::Systems,
            &key,
            &observation,
            client
                .managed_state()
                .location(&observation.value.key)
                .map(|value| value.value)
                .as_ref(),
            mode,
        )
        .await?;
        if complete {
            stage_marker(
                client,
                run_id,
                RefreshPhase::Systems,
                &format!("body_candidate:{designation}"),
                &response.value,
            )
            .await?;
        }
        if mode == RefreshMode::Apply {
            client
                .managed_state()
                .persist_location(observation)
                .map_err(super::client::store_error)?;
        }
        checkpoint(
            client,
            run_id,
            RefreshPhase::Systems,
            json!({
                "candidate_replicants_digest":candidate_digest,
                "replicant":replicant.id.as_str(),
                "replicant_index":resume_index,
                "page":total_pages + 1,
                "total_pages":total_pages,
                "total_stars":catalogue_count,
                "system_index":index + 1,
                "candidate_digest":membership_digest(sorted.iter().map(String::as_str)),
                "fully_planet_scanned_digest":membership_digest(
                    sorted[..=index].iter().map(String::as_str)
                )
            }),
            (total_pages as u64 + index as u64 + 1, sorted.len() as u64),
            (index + 1 == sorted.len(), true),
        )
        .await?;
    }
    if sorted.is_empty() {
        checkpoint(
            client,
            run_id,
            RefreshPhase::Systems,
            json!({"system_index":0,"candidate_digest":membership_digest(std::iter::empty())}),
            (total_pages as u64, 0),
            (true, true),
        )
        .await?;
    }
    Ok(())
}

async fn refresh_bodies(client: &Client, run_id: &RefreshRunId, mode: RefreshMode) -> Result<()> {
    let candidates = client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| {
                store.refresh_stage_prefix(&run_id, RefreshPhase::Systems, "body_candidate:")
            }
        })
        .await
        .map_err(super::client::store_error)?;
    let saved = load_checkpoint(client, run_id, RefreshPhase::Bodies).await?;
    let start_system = saved
        .get("system_index")
        .and_then(Value::as_u64)
        .unwrap_or(0) as usize;
    let mut items = client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| {
                store
                    .refresh_stage_prefix_keys(&run_id, RefreshPhase::Bodies, "body:")
                    .map(|keys| keys.len() as u64)
            }
        })
        .await
        .map_err(super::client::store_error)?;
    for (system_index, (key, payload)) in candidates.into_iter().enumerate().skip(start_system) {
        let system = key.trim_start_matches("body_candidate:").to_owned();
        let root: raw::locations::Location = serde_json::from_str(&payload)
            .map_err(|_| decode_error("staged system response is invalid"))?;
        let resuming_system = saved.get("system").and_then(Value::as_str) == Some(&system);
        let mut queue = if resuming_system {
            serde_json::from_value::<VecDeque<String>>(
                saved
                    .get("remaining_queue")
                    .cloned()
                    .unwrap_or_else(|| json!([])),
            )
            .map_err(|_| decode_error("staged body queue is invalid"))?
        } else {
            VecDeque::from(body_designations(&root))
        };
        let mut scheduled = body_designations(&root)
            .into_iter()
            .collect::<BTreeSet<_>>();
        scheduled.extend(queue.iter().cloned());
        scheduled.extend(
            client
                .managed_store()
                .execute({
                    let run_id = run_id.clone();
                    let prefix = format!("body:{system}:");
                    move |store| {
                        store.refresh_stage_prefix_keys(&run_id, RefreshPhase::Bodies, &prefix)
                    }
                })
                .await
                .map_err(super::client::store_error)?
                .into_iter()
                .map(|key| {
                    key.trim_start_matches(&format!("body:{system}:"))
                        .to_owned()
                }),
        );
        let mut complete = if resuming_system {
            saved
                .get("complete")
                .and_then(Value::as_bool)
                .unwrap_or(true)
        } else {
            true
        };
        let mut body_index = if resuming_system {
            saved.get("body_index").and_then(Value::as_u64).unwrap_or(0) as usize
        } else {
            0
        };
        while let Some(designation) = queue.pop_front() {
            let response = client
                .managed_raw()
                .locations()
                .get(&designation, None)
                .await?;
            if response.value.moons_total.unwrap_or(0) > 0 && !moon_set_complete(&response.value) {
                complete = false;
            }
            for child in body_designations(&response.value) {
                if scheduled.insert(child.clone()) {
                    queue.push_back(child);
                }
            }
            let observation =
                domain::location_detail(&response.value, Realm::Live, ObservationTime::now())
                    .map_err(|_| decode_error("surveyed body location is invalid"))?;
            let stage_key = format!("body:{system}:{designation}");
            stage_value(
                client,
                run_id,
                RefreshPhase::Bodies,
                &stage_key,
                &observation,
                client
                    .managed_state()
                    .location(&observation.value.key)
                    .map(|value| value.value)
                    .as_ref(),
                RefreshMode::DryRun,
            )
            .await?;
            body_index += 1;
            items += 1;
            let queue_digest = membership_digest(scheduled.iter().map(String::as_str));
            checkpoint(
                client,
                run_id,
                RefreshPhase::Bodies,
                json!({
                    "system_index":system_index,
                    "system":system,
                    "body_index":body_index,
                    "queue_digest":queue_digest,
                    "remaining_queue":queue,
                    "complete":complete
                }),
                (system_index as u64 + 1, items),
                (false, true),
            )
            .await?;
        }
        let stage_prefix = format!("body:{system}:");
        if complete && mode == RefreshMode::Apply {
            let observations = client
                .managed_store()
                .execute({
                    let run_id = run_id.clone();
                    let stage_prefix = stage_prefix.clone();
                    move |store| {
                        store.refresh_stage_prefix(&run_id, RefreshPhase::Bodies, &stage_prefix)
                    }
                })
                .await
                .map_err(super::client::store_error)?;
            for (_, payload) in observations {
                let observation = serde_json::from_str(&payload)
                    .map_err(|_| decode_error("staged surveyed body is invalid"))?;
                client
                    .managed_state()
                    .persist_location(observation)
                    .map_err(super::client::store_error)?;
            }
            client
                .managed_store()
                .execute({
                    let run_id = run_id.clone();
                    let stage_prefix = stage_prefix.clone();
                    move |store| {
                        store.mark_refresh_stage_prefix_applied(
                            &run_id,
                            RefreshPhase::Bodies,
                            &stage_prefix,
                        )
                    }
                })
                .await
                .map_err(super::client::store_error)?;
        } else if !complete {
            client
                .managed_store()
                .execute({
                    let run_id = run_id.clone();
                    move |store| {
                        store.discard_refresh_stage_prefix(
                            &run_id,
                            RefreshPhase::Bodies,
                            &stage_prefix,
                        )
                    }
                })
                .await
                .map_err(super::client::store_error)?;
        }
        checkpoint(
            client,
            run_id,
            RefreshPhase::Bodies,
            json!({"system_index":system_index + 1,"body_index":0}),
            (system_index as u64 + 1, items),
            (false, true),
        )
        .await?;
    }
    checkpoint(
        client,
        run_id,
        RefreshPhase::Bodies,
        json!({"complete":true,"system_index":candidates_len(client, run_id).await?}),
        (candidates_len(client, run_id).await?, items),
        (true, true),
    )
    .await
}

async fn refresh_events(client: &Client, run_id: &RefreshRunId, mode: RefreshMode) -> Result<()> {
    let saved = load_checkpoint(client, run_id, RefreshPhase::Events).await?;
    let before = match saved.get("before").and_then(Value::as_str) {
        Some(value) => value.to_owned(),
        None => OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .map_err(|error| Error::Configuration {
                message: format!("could not format refresh event bound: {error}"),
            })?,
    };
    let mut cursor = saved
        .get("next_cursor")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mut pages = saved.get("pages").and_then(Value::as_u64).unwrap_or(0);
    let mut events = saved.get("events").and_then(Value::as_u64).unwrap_or(0);
    loop {
        ensure_not_cancelled(client, run_id).await?;
        let query = EventLogQuery {
            cursor: cursor.clone(),
            limit: Some(100),
            filtered: Some(false),
            after: Some("1970-01-01T00:00:00Z".to_owned()),
            before: Some(before.clone()),
            ..Default::default()
        };
        let response = client.managed_raw().events().list(&query).await?;
        let next = response.value.next_cursor.clone();
        if next.is_some() && next == cursor {
            return Err(decode_error("event refresh cursor did not advance"));
        }
        let normalized = response
            .value
            .events
            .iter()
            .map(|event| {
                domain::account_event(event, Some(Realm::Live), ObservationTime::now()).value
            })
            .collect::<Vec<_>>();
        if mode == RefreshMode::Apply {
            client
                .managed_store()
                .execute(move |store| store.append_archived_events(&normalized))
                .await
                .map_err(super::client::store_error)?;
        }
        for event in &response.value.events {
            if let Some(location) = event
                .location
                .as_deref()
                .or_else(|| event.payload.get("location").and_then(Value::as_str))
            {
                stage_marker(
                    client,
                    run_id,
                    RefreshPhase::Events,
                    &format!("location:{location}"),
                    &json!({"location":location}),
                )
                .await?;
            }
        }
        pages += 1;
        events += response.value.events.len() as u64;
        cursor = next;
        checkpoint(
            client,
            run_id,
            RefreshPhase::Events,
            json!({"before":before,"next_cursor":cursor,"pages":pages,"events":events}),
            (pages, events),
            (cursor.is_none(), true),
        )
        .await?;
        if cursor.is_none() {
            return Ok(());
        }
    }
}

async fn refresh_messages(client: &Client, run_id: &RefreshRunId, mode: RefreshMode) -> Result<()> {
    let saved = load_checkpoint(client, run_id, RefreshPhase::Messages).await?;
    let mut cursor = saved.get("next_cursor").and_then(Value::as_i64);
    let mut last_cursor = saved.get("last_cursor").and_then(Value::as_i64);
    let mut pages = saved.get("pages").and_then(Value::as_u64).unwrap_or(0);
    let mut items = saved.get("messages").and_then(Value::as_u64).unwrap_or(0);
    let mut unread = saved
        .get("unread_count")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    loop {
        let query = raw::messages::MessageListQuery {
            cursor,
            limit: Some(100),
            latest: None,
            unread_only: None,
        };
        let response = client.managed_raw().messages().list(&query).await?;
        let next = response.value.next_cursor;
        if next.is_some() && next == cursor {
            return Err(decode_error("message refresh cursor did not advance"));
        }
        let observations = response
            .value
            .messages
            .into_iter()
            .map(|message| domain::message(message, ObservationTime::now()))
            .collect::<Vec<_>>();
        for observation in &observations {
            let key = observation.value.id.map_or_else(
                || {
                    format!(
                        "anonymous:{}",
                        observation.metadata.observed_at.unix_millis()
                    )
                },
                |id| id.to_string(),
            );
            stage_value(
                client,
                run_id,
                RefreshPhase::Messages,
                &key,
                observation,
                None::<&crate::domain::Message>,
                mode,
            )
            .await?;
        }
        if let Some(page_last_id) = observations.last().and_then(|message| message.value.id) {
            last_cursor = next.or(Some(page_last_id)).or(last_cursor);
        }
        unread = response.value.unread_message_count.unwrap_or(unread);
        pages += 1;
        items += observations.len() as u64;
        cursor = next;
        checkpoint(
            client,
            run_id,
            RefreshPhase::Messages,
            json!({"next_cursor":cursor,"last_cursor":last_cursor,"pages":pages,"messages":items,"unread_count":unread}),
            (pages, items),
            (cursor.is_none(), true),
        )
        .await?;
        if cursor.is_none() {
            if mode == RefreshMode::Apply {
                let observations = staged_payloads::<
                    crate::domain::Observation<crate::domain::Message>,
                >(client, run_id, RefreshPhase::Messages)
                .await?;
                let metadata = client
                    .managed_state()
                    .messages()
                    .map_err(super::client::store_error)?
                    .1;
                client
                    .managed_state()
                    .commit_messages_and_metadata(
                        &observations,
                        MessageMetadata {
                            last_cursor,
                            unread_count: Some(unread),
                            refreshed_at: Some(ObservationTime::now()),
                            revision: metadata.revision,
                            last_error: None,
                        },
                    )
                    .map_err(super::client::store_error)?;
            }
            return Ok(());
        }
    }
}

async fn refresh_locations(
    client: &Client,
    run_id: &RefreshRunId,
    mode: RefreshMode,
) -> Result<()> {
    let saved = load_checkpoint(client, run_id, RefreshPhase::Locations).await?;
    let overview_fetched = saved.get("overview_fetched").and_then(Value::as_bool) == Some(true);
    let mut designations = if overview_fetched {
        client
            .managed_store()
            .execute({
                let run_id = run_id.clone();
                move |store| {
                    store.refresh_stage_prefix_keys(&run_id, RefreshPhase::Locations, "queue:")
                }
            })
            .await
            .map_err(super::client::store_error)?
            .into_iter()
            .map(|value| value.trim_start_matches("queue:").to_owned())
            .collect::<BTreeSet<_>>()
    } else {
        let response = client.managed_raw().locations().system_map().await?;
        response.value.locations.keys().cloned().collect()
    };
    if !overview_fetched {
        for device in staged_payloads::<Observation<crate::domain::Device>>(
            client,
            run_id,
            RefreshPhase::Devices,
        )
        .await?
        {
            if let Some(location) = device.value.location {
                designations.insert(location.id.as_str().to_owned());
            }
        }
        for replicant in
            staged_payloads::<Observation<Replicant>>(client, run_id, RefreshPhase::Replicants)
                .await?
        {
            if let Some(location) = replicant.value.location {
                designations.insert(location.id.as_str().to_owned());
            }
        }
        designations.extend(
            client
                .managed_state()
                .locations()
                .into_iter()
                .map(|value| value.value.key.id.as_str().to_owned()),
        );
        designations.extend(
            client
                .managed_store()
                .execute({
                    let run_id = run_id.clone();
                    move |store| {
                        store.refresh_stage_prefix_keys(&run_id, RefreshPhase::Events, "location:")
                    }
                })
                .await
                .map_err(super::client::store_error)?
                .into_iter()
                .map(|value| value.trim_start_matches("location:").to_owned()),
        );
        for designation in &designations {
            stage_marker(
                client,
                run_id,
                RefreshPhase::Locations,
                &format!("queue:{designation}"),
                &json!({"designation":designation}),
            )
            .await?;
        }
        let digest = membership_digest(designations.iter().map(String::as_str));
        checkpoint(
            client,
            run_id,
            RefreshPhase::Locations,
            json!({
                "overview_fetched":true,
                "designation_digest":digest,
                "next_index":0
            }),
            (1, 0),
            (designations.is_empty(), true),
        )
        .await?;
    }
    let designations = designations.into_iter().collect::<Vec<_>>();
    let digest = membership_digest(designations.iter().map(String::as_str));
    if saved
        .get("designation_digest")
        .and_then(Value::as_str)
        .is_some_and(|previous| previous != digest)
    {
        return Err(decode_error(
            "durable location refresh queue digest changed while resuming",
        ));
    }
    let mut index = saved.get("next_index").and_then(Value::as_u64).unwrap_or(0) as usize;
    while let Some(designation) = designations.get(index) {
        let response = client
            .managed_raw()
            .locations()
            .get(designation, None)
            .await?;
        let observation =
            domain::location_detail(&response.value, Realm::Live, ObservationTime::now())
                .map_err(|_| decode_error("location refresh response is invalid"))?;
        stage_value(
            client,
            run_id,
            RefreshPhase::Locations,
            &format!("detail:{designation}"),
            &observation,
            client
                .managed_state()
                .location(&observation.value.key)
                .map(|value| value.value)
                .as_ref(),
            mode,
        )
        .await?;
        if mode == RefreshMode::Apply {
            client
                .managed_state()
                .persist_location(observation)
                .map_err(super::client::store_error)?;
        }
        index += 1;
        checkpoint(
            client,
            run_id,
            RefreshPhase::Locations,
            json!({
                "overview_fetched":true,
                "designation_digest":digest,
                "next_index":index
            }),
            (1, index as u64),
            (index == designations.len(), true),
        )
        .await?;
    }
    Ok(())
}

async fn refresh_inventory(
    client: &Client,
    run_id: &RefreshRunId,
    mode: RefreshMode,
) -> Result<()> {
    let saved = load_checkpoint(client, run_id, RefreshPhase::Inventory).await?;
    let mut cursor = saved
        .get("next_cursor")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mut pages = saved.get("pages").and_then(Value::as_u64).unwrap_or(0);
    let mut items = saved.get("items").and_then(Value::as_u64).unwrap_or(0);
    loop {
        let query = raw::inventory::AccountInventoryQuery {
            location: None,
            cursor: cursor.clone(),
            limit: Some(50),
        };
        let response = client.managed_raw().inventory().list(&query).await?;
        let next = response.value.next_cursor.clone();
        if next.is_some() && next == cursor {
            return Err(decode_error("inventory refresh cursor did not advance"));
        }
        for raw_inventory in &response.value.locations {
            let designation = raw_inventory
                .location
                .as_deref()
                .ok_or_else(|| decode_error("inventory row omitted location"))?;
            let owner = InventoryOwner::Location(crate::domain::LocationKey::in_realm(
                Realm::Live,
                crate::domain::LocationId::new(designation),
            ));
            let observation = domain::location_inventory(
                raw_inventory,
                owner.clone(),
                Realm::Live,
                ObservationTime::now(),
            )
            .map_err(|_| decode_error("inventory refresh row is invalid"))?;
            stage_value(
                client,
                run_id,
                RefreshPhase::Inventory,
                designation,
                &observation,
                client
                    .managed_state()
                    .inventory(&owner)
                    .map(|value| value.value)
                    .as_ref(),
                mode,
            )
            .await?;
            if mode == RefreshMode::Apply {
                client
                    .managed_state()
                    .persist_inventory(observation)
                    .map_err(super::client::store_error)?;
            }
            items += 1;
        }
        pages += 1;
        cursor = next;
        checkpoint(
            client,
            run_id,
            RefreshPhase::Inventory,
            json!({"next_cursor":cursor,"pages":pages,"items":items}),
            (pages, items),
            (cursor.is_none(), true),
        )
        .await?;
        if cursor.is_none() {
            return Ok(());
        }
    }
}

async fn refresh_simulations(
    client: &Client,
    run_id: &RefreshRunId,
    mode: RefreshMode,
) -> Result<()> {
    let response = client.managed_raw().accounts().simulations().await?;
    let mut items = 0u64;
    for raw in &response.value.simulations {
        let observation = domain::simulation_history(raw, ObservationTime::now())
            .map_err(|_| decode_error("simulation history row is invalid"))?;
        let key = observation.value.id.get().to_string();
        stage_value(
            client,
            run_id,
            RefreshPhase::Simulations,
            &key,
            &observation,
            client
                .managed_state()
                .simulation(observation.value.id)
                .map(|value| value.value)
                .as_ref(),
            mode,
        )
        .await?;
        if mode == RefreshMode::Apply {
            client
                .managed_state()
                .persist_simulation(observation)
                .map_err(super::client::store_error)?;
        }
        items += 1;
    }
    checkpoint(
        client,
        run_id,
        RefreshPhase::Simulations,
        json!({"state":"committed"}),
        (1, items),
        (true, true),
    )
    .await
}

async fn stage_observation<T, F>(
    client: &Client,
    run_id: &RefreshRunId,
    phase: RefreshPhase,
    key: &str,
    observation: &Observation<T>,
    mode: RefreshMode,
    existing: F,
) -> Result<()>
where
    T: Clone + PartialEq + Serialize,
    F: FnOnce(&Client) -> Option<T>,
{
    let current = existing(client);
    stage_value(
        client,
        run_id,
        phase,
        key,
        observation,
        current.as_ref(),
        mode,
    )
    .await
}

async fn stage_value<T>(
    client: &Client,
    run_id: &RefreshRunId,
    phase: RefreshPhase,
    key: &str,
    observation: &Observation<T>,
    existing: Option<&T>,
    mode: RefreshMode,
) -> Result<()>
where
    T: PartialEq + Serialize,
{
    let disposition = match existing {
        None => "insert",
        Some(value) if value == &observation.value => "unchanged",
        Some(_) => "update",
    };
    let payload = serde_json::to_string(observation).map_err(|error| Error::Persistence {
        message: format!("could not stage normalized refresh value: {error}"),
    })?;
    let delta = match disposition {
        "insert" => RefreshDelta {
            proposed_inserts: 1,
            applied_inserts: u64::from(mode == RefreshMode::Apply),
            ..Default::default()
        },
        "update" => RefreshDelta {
            proposed_updates: 1,
            applied_updates: u64::from(mode == RefreshMode::Apply),
            ..Default::default()
        },
        _ => RefreshDelta::default(),
    };
    client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            let key = key.to_owned();
            let disposition = disposition.to_owned();
            move |store| {
                store.stage_refresh_item(
                    &run_id,
                    phase,
                    &key,
                    Some(&payload),
                    &disposition,
                    Some(unix_millis()),
                    delta,
                )
            }
        })
        .await
        .map_err(super::client::store_error)
}

async fn stage_marker<T: Serialize>(
    client: &Client,
    run_id: &RefreshRunId,
    phase: RefreshPhase,
    key: &str,
    value: &T,
) -> Result<()> {
    let payload = serde_json::to_string(value).map_err(|error| Error::Persistence {
        message: format!("could not stage refresh marker: {error}"),
    })?;
    client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            let key = key.to_owned();
            move |store| {
                store.stage_refresh_item(
                    &run_id,
                    phase,
                    &key,
                    Some(&payload),
                    "unchanged",
                    Some(unix_millis()),
                    RefreshDelta::default(),
                )
            }
        })
        .await
        .map_err(super::client::store_error)
}

async fn staged_payloads<T: for<'de> Deserialize<'de>>(
    client: &Client,
    run_id: &RefreshRunId,
    phase: RefreshPhase,
) -> Result<Vec<T>> {
    let payloads = client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| store.refresh_stage_payloads(&run_id, phase)
        })
        .await
        .map_err(super::client::store_error)?;
    payloads
        .into_iter()
        .map(|payload| {
            serde_json::from_str(&payload).map_err(|error| Error::Persistence {
                message: format!("invalid durable refresh staging payload: {error}"),
            })
        })
        .collect()
}

async fn load_checkpoint(
    client: &Client,
    run_id: &RefreshRunId,
    phase: RefreshPhase,
) -> Result<Value> {
    client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| store.refresh_checkpoint(&run_id, phase)
        })
        .await
        .map_err(super::client::store_error)
}

async fn checkpoint(
    client: &Client,
    run_id: &RefreshRunId,
    phase: RefreshPhase,
    value: Value,
    progress: (u64, u64),
    proof: (bool, bool),
) -> Result<()> {
    client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| {
                store.update_refresh_checkpoint(
                    &run_id,
                    phase,
                    &value,
                    progress.0,
                    progress.1,
                    proof.0,
                    proof.1,
                    unix_millis(),
                )
            }
        })
        .await
        .map_err(super::client::store_error)
}

async fn ensure_not_cancelled(client: &Client, run_id: &RefreshRunId) -> Result<()> {
    let cancelled = client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| store.refresh_cancel_requested(&run_id)
        })
        .await
        .map_err(super::client::store_error)?;
    if cancelled {
        Err(Error::Configuration {
            message: "refresh cancelled".into(),
        })
    } else {
        Ok(())
    }
}

async fn finalize_devices(
    client: &Client,
    run_id: &RefreshRunId,
    mode: RefreshMode,
    seen: &BTreeSet<String>,
) -> Result<()> {
    client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            let seen = seen.clone();
            move |store| store.finalize_refresh_devices(&run_id, mode, &seen, unix_millis())
        })
        .await
        .map_err(super::client::store_error)?;
    if mode == RefreshMode::Apply {
        client
            .managed_state()
            .reload_devices_after_refresh()
            .map_err(super::client::store_error)?;
    }
    Ok(())
}

async fn finalize_stars(
    client: &Client,
    run_id: &RefreshRunId,
    mode: RefreshMode,
    stars: Vec<Observation<crate::domain::Star>>,
    generated_at: Option<String>,
) -> Result<()> {
    client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| {
                store.finalize_refresh_stars(
                    &run_id,
                    mode,
                    &stars,
                    generated_at.as_deref(),
                    unix_millis(),
                )
            }
        })
        .await
        .map_err(super::client::store_error)?;
    if mode == RefreshMode::Apply {
        client
            .managed_state()
            .reload_catalogue_after_refresh()
            .map_err(super::client::store_error)?;
    }
    Ok(())
}
fn census_capability_unavailable(error: &Error) -> bool {
    matches!(
        error,
        Error::Authentication { .. }
            | Error::Contract {
                status: 403 | 404,
                ..
            }
    )
}

fn expand_phases(requested: &BTreeSet<RefreshPhase>) -> Vec<RefreshPhase> {
    let mut expanded = if requested.is_empty() {
        RefreshPhase::FULL.into_iter().collect()
    } else {
        requested.clone()
    };
    loop {
        let before = expanded.len();
        for phase in expanded.clone() {
            expanded.extend(phase.dependencies());
        }
        if expanded.len() == before {
            break;
        }
    }
    RefreshPhase::FULL
        .into_iter()
        .filter(|phase| expanded.contains(phase))
        .collect()
}

fn membership_digest<'a>(keys: impl IntoIterator<Item = &'a str>) -> String {
    let mut hasher = Sha256::new();
    for key in keys {
        hasher.update((key.len() as u64).to_be_bytes());
        hasher.update(key.as_bytes());
    }
    format!("{:x}", hasher.finalize())
}

fn body_designations(location: &raw::locations::Location) -> Vec<String> {
    let mut values = BTreeSet::new();
    for object in location
        .planets
        .iter()
        .flatten()
        .chain(location.moons.iter().flatten())
        .chain(location.system_objects.iter().flatten())
    {
        if let Some(value) = super::operation::object_designation(object) {
            values.insert(value);
        }
    }
    for object in [
        &location.belt,
        &location.asteroid_belt,
        &location.lagrange,
        &location.kuiper,
        &location.oort,
        &location.outer_system,
        &location.object,
    ]
    .into_iter()
    .flatten()
    {
        if let Some(value) = super::operation::object_designation(object) {
            values.insert(value);
        }
    }
    values.into_iter().collect()
}

fn moon_set_complete(location: &raw::locations::Location) -> bool {
    let unique = location
        .moons
        .iter()
        .flatten()
        .filter_map(super::operation::object_designation)
        .collect::<BTreeSet<_>>();
    location.moons_total_estimated == Some(false)
        && matches!((location.moons_scanned, location.moons_total), (Some(scanned), Some(total)) if scanned == total && total >= 0 && unique.len() == total as usize)
}

async fn candidates_len(client: &Client, run_id: &RefreshRunId) -> Result<u64> {
    client
        .managed_store()
        .execute({
            let run_id = run_id.clone();
            move |store| {
                store
                    .refresh_stage_prefix_keys(&run_id, RefreshPhase::Systems, "body_candidate:")
                    .map(|values| values.len() as u64)
            }
        })
        .await
        .map_err(super::client::store_error)
}

fn refresh_failure_kind(error: &Error) -> String {
    match error {
        Error::Authentication { .. } => "authentication",
        Error::RateLimited { .. } => "rate_limited",
        Error::Decode { .. } => "decode",
        Error::Persistence { .. } => "persistence",
        Error::Closed => "closed",
        _ => "request",
    }
    .to_owned()
}

fn decode_error(message: impl Into<String>) -> Error {
    Error::Decode {
        message: message.into(),
        status: None,
        source: None,
    }
}

fn unix_millis() -> i64 {
    let value = OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param, query_param_is_missing},
    };

    use super::*;
    use crate::raw::{SecretString, Url};

    #[test]
    fn refresh_plan_matches_executable_policy() {
        let policy: Value =
            serde_json::from_str(include_str!("../../policy/sync-domains.json")).unwrap();
        let full = policy["full_plan"].as_array().unwrap();
        assert_eq!(full.len(), RefreshPhase::FULL.len());
        for (value, phase) in full.iter().zip(RefreshPhase::FULL) {
            assert_eq!(value.as_str(), Some(phase.as_str()));
            let dependencies = policy["domains"][phase.as_str()]["depends_on"]
                .as_array()
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap())
                .collect::<BTreeSet<_>>();
            assert_eq!(
                dependencies,
                phase
                    .dependencies()
                    .iter()
                    .map(|dependency| dependency.as_str())
                    .collect()
            );
        }
        assert_eq!(policy["rate_budget"]["maximum_gets_per_minute"], 60);
        assert_eq!(policy["deletion_safety"]["shrink_approval_percent"], 20);
    }

    #[test]
    fn recursively_discovers_exact_moons_and_generic_objects() {
        let root: raw::locations::Location = serde_json::from_value(json!({
            "location": "SOL",
            "planets": [{"designation": "SOL-1"}],
            "system_objects": [{"designation": "SOL-OBJ-1"}]
        }))
        .unwrap();
        assert_eq!(
            body_designations(&root),
            vec!["SOL-1".to_owned(), "SOL-OBJ-1".to_owned()]
        );
        let planet: raw::locations::Location = serde_json::from_value(json!({
            "location": "SOL-1",
            "moons_total": 1,
            "moons_scanned": 1,
            "moons_total_estimated": false,
            "moons": [{"designation": "SOL-1-A"}]
        }))
        .unwrap();
        assert!(moon_set_complete(&planet));
        assert_eq!(body_designations(&planet), vec!["SOL-1-A".to_owned()]);
    }

    #[tokio::test]
    async fn full_refresh_rebuilds_empty_databases() {
        let server = MockServer::start().await;
        for (route, body) in [
            ("/v1/accounts/me", json!({"email":"refresh@example.test"})),
            (
                "/v1/replicants/R1",
                json!({"replicant_code":"R1","location":"SOL-1"}),
            ),
            (
                "/v1/stars",
                json!({"generated_at":"2026-08-26T00:00:00Z","total":1,
                    "stars":[{"designation":"SOL","name":"Sol"}]}),
            ),
            (
                "/v1/replicants/R1/stars",
                json!({"page":1,"per_page":50,"total":1,"total_pages":1,
                    "total_stars":1,"stars":[{"designation":"SOL","explored":true}]}),
            ),
            (
                "/v1/locations/SOL",
                json!({"location":"SOL","location_type":"star",
                    "planets_total":1,"planets_scanned":1,
                    "planets":[{"designation":"SOL-1"}],
                    "system_objects":[{"designation":"SOL-OBJ-1"}]}),
            ),
            (
                "/v1/locations/SOL-1",
                json!({"location":"SOL-1","location_type":"planet",
                    "moons_total":1,"moons_scanned":1,"moons_total_estimated":false,
                    "moons":[{"designation":"SOL-1-A"}],
                    "planet":{"scanned":true}}),
            ),
            (
                "/v1/locations/SOL-1-A",
                json!({"location":"SOL-1-A","location_type":"moon",
                    "moon":{"scanned":true}}),
            ),
            (
                "/v1/locations/SOL-OBJ-1",
                json!({"location":"SOL-OBJ-1","location_type":"object",
                    "object":{"required_strength":12,"impact_eta":"2026-08-27T00:00:00Z",
                        "progress_pct":25,"active_plates":3}}),
            ),
            (
                "/v1/locations",
                json!({"locations":{"SAL-1":{"resource_sites":1}}}),
            ),
            (
                "/v1/locations/SAL-1",
                json!({"location":"SAL-1","location_type":"salvage",
                    "resource_sites":[{"designation":"SAL-1-SITE"}]}),
            ),
            (
                "/v1/inventory",
                json!({"locations":[{"location":"SOL-1",
                    "items":[{"resource_type":"structural","quantity":3}]}],
                    "next_cursor":null}),
            ),
            (
                "/v1/accounts/simulations",
                json!({"simulations":[{"id":7,"scenario_code":"mine",
                    "started_at":"2026-01-01T00:00:00Z",
                    "completed_at":"2026-01-01T01:00:00Z"}]}),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .and(query_param_is_missing("cursor"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "devices":[{
                    "device_code":"D1",
                    "replicant_code":"R1",
                    "hosting_replicant":"R1",
                    "location":"SOL-1"
                }],
                "next_cursor":7
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .and(query_param("cursor", "7"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "devices":[{
                    "device_code":"D2",
                    "replicant_code":"R1",
                    "location":"SOL-1"
                }],
                "next_cursor":null
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param_is_missing("cursor"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "events":[{
                    "id":"9-0","version":1,"category":"system",
                    "event":"system.object_detected","location":"SOL-OBJ-1",
                    "created_at":"2026-08-24T00:00:00Z",
                    "payload":{"location":"SOL-OBJ-1"}
                }],
                "next_cursor":"10-0"
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/events"))
            .and(query_param("cursor", "10-0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "events":[{
                    "id":"10-0","version":1,"category":"system",
                    "event":"system.object_detected","location":"SOL-OBJ-1",
                    "created_at":"2026-08-25T00:00:00Z",
                    "payload":{"location":"SOL-OBJ-1"}
                }],
                "next_cursor":null
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/messages"))
            .and(query_param_is_missing("cursor"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "messages":[{"id":1,"title":"Recovered 1","is_read":false}],
                "next_cursor":2,
                "unread_message_count":2
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/messages"))
            .and(query_param("cursor", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "messages":[{"id":2,"title":"Recovered 2","is_read":false}],
                "next_cursor":null,
                "unread_message_count":2
            })))
            .expect(1)
            .mount(&server)
            .await;
        let database =
            std::env::temp_dir().join(format!("replicant-full-refresh-{}.sqlite", Uuid::new_v4()));
        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_owned()))
            .base_url(Url::parse(&server.uri()).unwrap())
            .sqlite(&database)
            .startup_policy(super::super::client::StartupPolicy::RestoreOnly)
            .start()
            .await
            .unwrap();
        spawn(&client).await.unwrap();
        let run = client
            .refresh()
            .start(RefreshRequest::default())
            .await
            .unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(40), async {
            loop {
                let status = client.refresh().status(&run.run_id).await.unwrap().unwrap();
                if status.status.terminal() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("refresh completed");
        assert_eq!(completed.status, RefreshRunState::Completed);
        assert_eq!(completed.phases.len(), RefreshPhase::FULL.len());
        assert!(completed.phases.iter().all(|phase| {
            phase.status == RefreshPhaseState::Complete
                && phase.pages >= 1
                && phase.items >= 1
                && phase.request_attempts >= 1
        }));
        for (phase, pages, items) in [
            (RefreshPhase::Devices, 2, 2),
            (RefreshPhase::Events, 2, 2),
            (RefreshPhase::Messages, 2, 2),
        ] {
            let status = completed
                .phases
                .iter()
                .find(|status| status.phase == phase)
                .unwrap();
            assert_eq!((status.pages, status.items), (pages, items));
        }
        assert!(completed.delta.proposed_inserts > 0);
        assert_eq!(
            completed.delta.proposed_inserts,
            completed.delta.applied_inserts
        );
        assert_eq!(client.managed_state().devices().len(), 2);
        assert_eq!(client.managed_state().replicants().len(), 1);
        assert_eq!(client.managed_state().catalogue().len(), 1);
        assert!(client.managed_state().locations().len() >= 5);
        assert_eq!(client.managed_state().messages().unwrap().0.len(), 2);
        assert_eq!(client.managed_state().simulations().len(), 1);
        assert_eq!(client.events().history().collect().await.unwrap().len(), 2);
        let object = client
            .managed_state()
            .locations()
            .into_iter()
            .find(|location| location.value.key.id.as_str() == "SOL-OBJ-1")
            .unwrap();
        assert_eq!(object.value.unknown["object"]["required_strength"], 12);
        assert_eq!(
            object.value.unknown["object"]["impact_eta"],
            "2026-08-27T00:00:00Z"
        );
        assert_eq!(object.value.unknown["object"]["progress_pct"], 25);
        assert_eq!(object.value.unknown["object"]["active_plates"], 3);

        client.close().await.unwrap();
        let restored = Client::builder()
            .authentication_token(SecretString::from("token".to_owned()))
            .base_url(Url::parse("http://127.0.0.1:9").unwrap())
            .sqlite(&database)
            .startup_policy(super::super::client::StartupPolicy::RestoreOnly)
            .start()
            .await
            .unwrap();
        assert_eq!(restored.managed_state().devices().len(), 2);
        assert_eq!(
            restored.events().history().collect().await.unwrap().len(),
            2
        );
        let restored_status = restored
            .refresh()
            .status(&run.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(restored_status.status, RefreshRunState::Completed);
        assert_eq!(restored_status.phases.len(), RefreshPhase::FULL.len());
        restored.close().await.unwrap();
        let _ = std::fs::remove_file(&database);
        let _ = std::fs::remove_file(super::super::store::history_database_path(&database));
        let _ = std::fs::remove_file(database.with_extension("rate-limit.sqlite"));
    }
    #[tokio::test]
    async fn truncated_device_refresh_never_tombstones() {
        use wiremock::matchers::{query_param, query_param_is_missing};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/accounts/me"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({"email":"truncate@example.test"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .and(query_param("limit", "50"))
            .and(query_param_is_missing("cursor"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "devices":[{"device_code":"D1","replicant_code":"R1"}],
                "next_cursor":1
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .and(query_param("cursor", "1"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;
        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_owned()))
            .base_url(Url::parse(&server.uri()).unwrap())
            .in_memory()
            .startup_policy(super::super::client::StartupPolicy::RestoreOnly)
            .start()
            .await
            .unwrap();
        let seeded: raw::devices::DeviceListResponse = serde_json::from_value(json!({
            "devices":[
                {"device_code":"D1","replicant_code":"R1"},
                {"device_code":"D2","replicant_code":"R1"}
            ]
        }))
        .unwrap();
        let collection =
            domain::device_collection(&seeded, Realm::Live, false, true, ObservationTime::now())
                .unwrap();
        client
            .managed_state()
            .persist_devices(&collection.members)
            .unwrap();
        spawn(&client).await.unwrap();
        let run = client
            .refresh()
            .start(RefreshRequest {
                phases: [RefreshPhase::Devices].into_iter().collect(),
                ..Default::default()
            })
            .await
            .unwrap();
        let failed = tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let status = client.refresh().status(&run.run_id).await.unwrap().unwrap();
                if status.status.terminal() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("truncated refresh terminated");
        assert_eq!(failed.status, RefreshRunState::Failed);
        assert_eq!(client.managed_state().devices().len(), 2);
        let devices = failed
            .phases
            .iter()
            .find(|phase| phase.phase == RefreshPhase::Devices)
            .unwrap();
        assert_eq!(devices.delta.applied_tombstones, 0);
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn full_refresh_dry_run_never_mutates_authoritative_state() {
        let server = MockServer::start().await;
        for (route, body) in [
            ("/v1/accounts/me", json!({"email":"dry@example.test"})),
            (
                "/v1/devices",
                json!({"devices":[{"device_code":"NEW","replicant_code":"R1"}],
                    "next_cursor":null}),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
        }
        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_owned()))
            .base_url(Url::parse(&server.uri()).unwrap())
            .in_memory()
            .startup_policy(super::super::client::StartupPolicy::RestoreOnly)
            .start()
            .await
            .unwrap();
        spawn(&client).await.unwrap();
        let run = client
            .refresh()
            .start(RefreshRequest {
                phases: [RefreshPhase::Devices].into_iter().collect(),
                mode: RefreshMode::DryRun,
                read_requests_per_minute: 60,
            })
            .await
            .unwrap();
        let completed = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let status = client.refresh().status(&run.run_id).await.unwrap().unwrap();
                if status.status.terminal() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("dry run completed");
        assert_eq!(completed.status, RefreshRunState::CompletedDryRun);
        assert!(completed.delta.proposed_inserts >= 2);
        assert_eq!(completed.delta.applied_inserts, 0);
        assert!(client.managed_state().account().is_none());
        assert!(client.managed_state().devices().is_empty());
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn refresh_resume_uses_durable_phase_cursor() {
        use wiremock::matchers::{query_param, query_param_is_missing};

        let server = MockServer::start().await;
        for (route, cursor_name, cursor, body) in [
            (
                "/v1/devices",
                "cursor",
                "42",
                json!({"devices":[{"device_code":"D2","replicant_code":"R2"}],
                    "next_cursor":null}),
            ),
            (
                "/v1/replicants/R2",
                "unused",
                "unused",
                json!({"replicant_code":"R2","location":"SOL-2"}),
            ),
            (
                "/v1/replicants/R1/stars",
                "page",
                "2",
                json!({"page":2,"per_page":50,"total_pages":2,"total_stars":2,
                    "stars":[{"designation":"STAR2","explored":false}]}),
            ),
            (
                "/v1/locations/SOL-2",
                "unused",
                "unused",
                json!({"location":"SOL-2","location_type":"planet",
                    "moons_total":0,"moons_scanned":0,"moons_total_estimated":false}),
            ),
            (
                "/v1/events",
                "cursor",
                "10-0",
                json!({"events":[],"next_cursor":null}),
            ),
            (
                "/v1/messages",
                "cursor",
                "42",
                json!({"messages":[],"next_cursor":null,"unread_message_count":0}),
            ),
            (
                "/v1/inventory",
                "cursor",
                "inventory-next",
                json!({"locations":[],"next_cursor":null}),
            ),
        ] {
            let mock = Mock::given(method("GET")).and(path(route));
            let mock = if cursor_name == "unused" {
                mock
            } else {
                mock.and(query_param(cursor_name, cursor))
            };
            mock.respond_with(ResponseTemplate::new(200).set_body_json(body))
                .expect(1)
                .mount(&server)
                .await;
        }
        for route in ["/v1/devices", "/v1/events", "/v1/messages", "/v1/inventory"] {
            Mock::given(method("GET"))
                .and(path(route))
                .and(query_param_is_missing("cursor"))
                .respond_with(ResponseTemplate::new(500))
                .expect(0)
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/v1/replicants/R1"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/replicants/R1/stars"))
            .and(query_param("page", "1"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/locations/SOL-1"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;

        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_owned()))
            .base_url(Url::parse(&server.uri()).unwrap())
            .in_memory()
            .startup_policy(super::super::client::StartupPolicy::RestoreOnly)
            .start()
            .await
            .unwrap();
        let run_id = RefreshRunId::new();
        client
            .managed_store()
            .execute({
                let run_id = run_id.clone();
                move |store| {
                    store.create_refresh_run(
                        &run_id,
                        RefreshMode::DryRun,
                        &RefreshPhase::FULL,
                        60,
                        unix_millis(),
                    )?;
                    store.begin_refresh_phase(
                        &run_id,
                        RefreshPhase::Devices,
                        "resume-test",
                        unix_millis(),
                    )?;
                    Ok(())
                }
            })
            .await
            .unwrap();

        let device_raw: raw::devices::DeviceListResponse = serde_json::from_value(json!({
            "devices":[
                {"device_code":"D1","replicant_code":"R1"},
                {"device_code":"D2","replicant_code":"R2"}
            ]
        }))
        .unwrap();
        let devices = domain::device_collection(
            &device_raw,
            Realm::Live,
            false,
            true,
            ObservationTime::now(),
        )
        .unwrap()
        .members;
        stage_marker(&client, &run_id, RefreshPhase::Devices, "D1", &devices[0])
            .await
            .unwrap();
        checkpoint(
            &client,
            &run_id,
            RefreshPhase::Devices,
            json!({"next_cursor":42,"pages":1,"seen_count":1}),
            (1, 1),
            (false, true),
        )
        .await
        .unwrap();
        refresh_devices(&client, &run_id, RefreshMode::DryRun)
            .await
            .unwrap();

        let codes = ["R1", "R2"];
        let codes_digest = membership_digest(codes);
        let r1_raw: raw::replicants::ReplicantStatus =
            serde_json::from_value(json!({"replicant_code":"R1","location":"SOL-1"})).unwrap();
        let r1 =
            domain::owned_replicant_detail(&r1_raw, Realm::Live, ObservationTime::now()).unwrap();
        stage_marker(&client, &run_id, RefreshPhase::Replicants, "R1", &r1)
            .await
            .unwrap();
        checkpoint(
            &client,
            &run_id,
            RefreshPhase::Replicants,
            json!({"codes_digest":codes_digest,"next_index":1}),
            (1, 1),
            (false, true),
        )
        .await
        .unwrap();
        refresh_replicants(&client, &run_id, RefreshMode::DryRun)
            .await
            .unwrap();

        for designation in ["STAR1", "STAR2"] {
            let raw_star: raw::galaxy::CatalogueStar =
                serde_json::from_value(json!({"designation":designation})).unwrap();
            let star =
                domain::catalogue_star(&raw_star, Realm::Live, ObservationTime::now()).unwrap();
            stage_marker(&client, &run_id, RefreshPhase::Stars, designation, &star)
                .await
                .unwrap();
        }
        let star1: raw::galaxy::StarItem =
            serde_json::from_value(json!({"designation":"STAR1","explored":false})).unwrap();
        stage_marker(
            &client,
            &run_id,
            RefreshPhase::Systems,
            "census:STAR1",
            &star1,
        )
        .await
        .unwrap();
        let candidate_digest = membership_digest(["R1", "R2"]);
        checkpoint(
            &client,
            &run_id,
            RefreshPhase::Systems,
            json!({
                "candidate_replicants_digest":candidate_digest,
                "replicant_index":0,
                "page":2,
                "total_pages":2,
                "total_stars":2
            }),
            (1, 1),
            (false, true),
        )
        .await
        .unwrap();
        refresh_systems(&client, &run_id, RefreshMode::DryRun)
            .await
            .unwrap();

        let system_root: raw::locations::Location = serde_json::from_value(json!({
            "location":"SYS","planets":[{"designation":"SOL-1"},{"designation":"SOL-2"}]
        }))
        .unwrap();
        stage_marker(
            &client,
            &run_id,
            RefreshPhase::Systems,
            "body_candidate:SYS",
            &system_root,
        )
        .await
        .unwrap();
        let sol1_raw: raw::locations::Location =
            serde_json::from_value(json!({"location":"SOL-1","location_type":"planet"})).unwrap();
        let sol1 = domain::location_detail(&sol1_raw, Realm::Live, ObservationTime::now()).unwrap();
        stage_marker(
            &client,
            &run_id,
            RefreshPhase::Bodies,
            "body:SYS:SOL-1",
            &sol1,
        )
        .await
        .unwrap();
        checkpoint(
            &client,
            &run_id,
            RefreshPhase::Bodies,
            json!({
                "system_index":0,
                "system":"SYS",
                "body_index":1,
                "remaining_queue":["SOL-2"],
                "complete":true
            }),
            (1, 1),
            (false, true),
        )
        .await
        .unwrap();
        refresh_bodies(&client, &run_id, RefreshMode::DryRun)
            .await
            .unwrap();

        checkpoint(
            &client,
            &run_id,
            RefreshPhase::Events,
            json!({"before":"2026-08-26T00:00:00Z","next_cursor":"10-0","pages":1,"events":1}),
            (1, 1),
            (false, true),
        )
        .await
        .unwrap();
        refresh_events(&client, &run_id, RefreshMode::DryRun)
            .await
            .unwrap();
        checkpoint(
            &client,
            &run_id,
            RefreshPhase::Messages,
            json!({"next_cursor":42,"pages":1,"messages":1,"unread_count":1}),
            (1, 1),
            (false, true),
        )
        .await
        .unwrap();
        refresh_messages(&client, &run_id, RefreshMode::DryRun)
            .await
            .unwrap();
        checkpoint(
            &client,
            &run_id,
            RefreshPhase::Inventory,
            json!({"next_cursor":"inventory-next","pages":1,"items":1}),
            (1, 1),
            (false, true),
        )
        .await
        .unwrap();
        refresh_inventory(&client, &run_id, RefreshMode::DryRun)
            .await
            .unwrap();
        client.close().await.unwrap();

        let database = std::env::temp_dir().join(format!(
            "replicant-refresh-resume-{}.sqlite",
            Uuid::new_v4()
        ));
        let durable = Client::builder()
            .authentication_token(SecretString::from("token".to_owned()))
            .base_url(Url::parse("http://127.0.0.1:9").unwrap())
            .sqlite(&database)
            .startup_policy(super::super::client::StartupPolicy::RestoreOnly)
            .start()
            .await
            .unwrap();
        let shutdown_run = RefreshRunId::new();
        durable
            .managed_store()
            .execute({
                let shutdown_run = shutdown_run.clone();
                move |store| {
                    store.create_refresh_run(
                        &shutdown_run,
                        RefreshMode::Apply,
                        &[RefreshPhase::Account],
                        60,
                        unix_millis(),
                    )?;
                    let _ = store.claim_refresh_run("expired-owner", 1, 2)?;
                    Ok(())
                }
            })
            .await
            .unwrap();
        durable.close().await.unwrap();
        let restored = Client::builder()
            .authentication_token(SecretString::from("token".to_owned()))
            .base_url(Url::parse("http://127.0.0.1:9").unwrap())
            .sqlite(&database)
            .startup_policy(super::super::client::StartupPolicy::RestoreOnly)
            .start()
            .await
            .unwrap();
        assert_ne!(
            restored
                .refresh()
                .status(&shutdown_run)
                .await
                .unwrap()
                .unwrap()
                .status,
            RefreshRunState::Cancelled
        );
        let recovered = restored
            .managed_store()
            .execute(move |store| store.claim_refresh_run("new-owner", 3, 100))
            .await
            .unwrap();
        assert_eq!(recovered, Some(shutdown_run));
        restored.close().await.unwrap();
        let _ = std::fs::remove_file(&database);
        let _ = std::fs::remove_file(super::super::store::history_database_path(&database));
        let _ = std::fs::remove_file(database.with_extension("rate-limit.sqlite"));
    }
    #[test]
    fn device_deletion_guards_require_exact_approval_and_preserve_newer_rows() {
        fn observations(at: i64) -> Vec<Observation<crate::domain::Device>> {
            let raw: raw::devices::DeviceListResponse = serde_json::from_value(json!({
                "devices": (0..5).map(|index| json!({
                    "device_code":format!("D{index}"),
                    "replicant_code":"R1"
                })).collect::<Vec<_>>()
            }))
            .unwrap();
            domain::device_collection(
                &raw,
                Realm::Live,
                false,
                true,
                ObservationTime::from_unix_millis(at),
            )
            .unwrap()
            .members
        }

        let mut store = super::super::store::Store::open_memory().unwrap();
        let seeded = observations(10);
        store.persist_devices(&seeded).unwrap();
        let run_id = RefreshRunId::new();
        store
            .create_refresh_run(
                &run_id,
                RefreshMode::Apply,
                &[RefreshPhase::Devices],
                60,
                100,
            )
            .unwrap();
        store
            .begin_refresh_phase(&run_id, RefreshPhase::Devices, "test", 100)
            .unwrap();
        store
            .update_refresh_checkpoint(
                &run_id,
                RefreshPhase::Devices,
                &json!({"next_cursor":null}),
                1,
                3,
                true,
                true,
                100,
            )
            .unwrap();
        let seen = ["D0", "D1", "D2"].into_iter().map(str::to_owned).collect();
        store
            .finalize_refresh_devices(&run_id, RefreshMode::Apply, &seen, 100)
            .unwrap();
        assert_eq!(store.restore_devices().unwrap().len(), 5);
        let status = store
            .refresh_run_status(&run_id, ReadinessComponent::Pending)
            .unwrap()
            .unwrap();
        let devices = status.phases.first().unwrap();
        assert_eq!(devices.status, RefreshPhaseState::AwaitingApproval);
        let digest = devices.approval_digest.clone().unwrap();
        assert!(
            store
                .approve_refresh_phase(&run_id, RefreshPhase::Devices, "deadbeef", 101)
                .is_err()
        );
        store
            .approve_refresh_phase(&run_id, RefreshPhase::Devices, &digest, 101)
            .unwrap();
        store
            .finalize_refresh_devices(&run_id, RefreshMode::Apply, &seen, 102)
            .unwrap();
        assert_eq!(store.restore_devices().unwrap().len(), 3);

        let mut store = super::super::store::Store::open_memory().unwrap();
        let mut seeded = observations(10);
        seeded[4].metadata.observed_at = ObservationTime::from_unix_millis(200);
        store.persist_devices(&seeded).unwrap();
        let run_id = RefreshRunId::new();
        store
            .create_refresh_run(
                &run_id,
                RefreshMode::Apply,
                &[RefreshPhase::Devices],
                60,
                100,
            )
            .unwrap();
        store
            .begin_refresh_phase(&run_id, RefreshPhase::Devices, "test", 100)
            .unwrap();
        store
            .update_refresh_checkpoint(
                &run_id,
                RefreshPhase::Devices,
                &json!({"next_cursor":null}),
                1,
                3,
                true,
                true,
                100,
            )
            .unwrap();
        store
            .finalize_refresh_devices(&run_id, RefreshMode::Apply, &seen, 100)
            .unwrap();
        let restored = store.restore_devices().unwrap();
        assert_eq!(restored.len(), 4);
        assert!(restored.keys().any(|key| key.id.as_str() == "D4"));

        let mut store = super::super::store::Store::open_memory().unwrap();
        store.persist_devices(&observations(10)).unwrap();
        let run_id = RefreshRunId::new();
        store
            .create_refresh_run(
                &run_id,
                RefreshMode::Apply,
                &[RefreshPhase::Devices],
                60,
                100,
            )
            .unwrap();
        store
            .begin_refresh_phase(&run_id, RefreshPhase::Devices, "test", 100)
            .unwrap();
        store
            .update_refresh_checkpoint(
                &run_id,
                RefreshPhase::Devices,
                &json!({"next_cursor":null}),
                1,
                0,
                true,
                true,
                100,
            )
            .unwrap();
        assert!(
            store
                .finalize_refresh_devices(&run_id, RefreshMode::Apply, &BTreeSet::new(), 100)
                .is_err()
        );
        assert_eq!(store.restore_devices().unwrap().len(), 5);
    }
    #[tokio::test]
    async fn star_refresh_guards_preserve_prior_catalogue() {
        for body in [
            json!({"stars":[{"designation":"NEW"}]}),
            json!({"total":2,"stars":[{"designation":"NEW"}]}),
            json!({"total":0,"stars":[]}),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path("/v1/stars"))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
            let client = Client::builder()
                .authentication_token(SecretString::from("token".to_owned()))
                .base_url(Url::parse(&server.uri()).unwrap())
                .in_memory()
                .startup_policy(super::super::client::StartupPolicy::RestoreOnly)
                .start()
                .await
                .unwrap();
            let existing_raw: raw::galaxy::CatalogueStar =
                serde_json::from_value(json!({"designation":"OLD"})).unwrap();
            let existing = domain::catalogue_star(
                &existing_raw,
                Realm::Live,
                ObservationTime::from_unix_millis(10),
            )
            .unwrap();
            client
                .managed_state()
                .replace_catalogue(vec![existing], None)
                .unwrap();
            let run_id = RefreshRunId::new();
            client
                .managed_store()
                .execute({
                    let run_id = run_id.clone();
                    move |store| {
                        store.create_refresh_run(
                            &run_id,
                            RefreshMode::Apply,
                            &[RefreshPhase::Stars],
                            60,
                            100,
                        )?;
                        store.begin_refresh_phase(&run_id, RefreshPhase::Stars, "test", 100)?;
                        Ok(())
                    }
                })
                .await
                .unwrap();
            assert!(
                refresh_stars(&client, &run_id, RefreshMode::Apply)
                    .await
                    .is_err()
            );
            assert_eq!(
                client.managed_state().catalogue()[0].value.key.id.as_str(),
                "OLD"
            );
            client.close().await.unwrap();
        }
    }
    #[test]
    fn archived_event_history_orders_numeric_ids_without_advancing_live_cursor() {
        let mut store = super::super::store::Store::open_memory().unwrap();
        let event = |id: &str| {
            domain::account_event(
                &crate::events::GameEvent {
                    id: id.to_owned(),
                    version: 1,
                    category: "system".to_owned(),
                    event: "system.object_detected".to_owned(),
                    created_at: "2026-08-26T00:00:00Z".to_owned(),
                    ..Default::default()
                },
                Some(Realm::Live),
                ObservationTime::now(),
            )
            .value
        };
        let newest = event("10-0");
        let oldest = event("9-999");
        store
            .append_archived_events(&[newest.clone(), oldest.clone()])
            .unwrap();
        assert!(store.event_cursor().unwrap().is_none());
        let events = store.read_events(None, None, None, None).unwrap();
        assert_eq!(events[0].id.as_str(), "9-999");
        assert_eq!(events[1].id.as_str(), "10-0");
        assert!(
            store
                .apply_event_projection(
                    &oldest,
                    oldest.id.as_str(),
                    &super::super::store::EventProjectionBatch::default(),
                )
                .unwrap()
        );
        assert!(
            !store
                .apply_event_projection(
                    &oldest,
                    oldest.id.as_str(),
                    &super::super::store::EventProjectionBatch::default(),
                )
                .unwrap()
        );
    }
}
