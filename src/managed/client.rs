//! The managed client lifecycle and its shared ownership graph.

use std::{
    fmt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::atomic::{AtomicU64, AtomicUsize};

use tokio::{
    sync::{Mutex as TokioMutex, watch},
    task::JoinHandle,
};
use tracing::{debug, info, warn};

use crate::raw::rate_limit::{RateLimitBucket, RateLimitCoordinator, RateLimitPolicy};
use crate::{
    AccountId, Error, Result,
    raw::{
        ApiTelemetrySink, Client as RawClient, ClientBuilder as RawClientBuilder, RequestPriority,
        SecretString, TlsBackend, Url,
    },
};

use super::{
    events::{EventTelemetrySample, EventTelemetrySink},
    state::StateEngine,
    store::{StoreError, StoreHandle},
};

fn data_directory(home: Option<&std::ffi::OsStr>) -> PathBuf {
    home.map_or_else(
        || PathBuf::from(".local/share/replicant"),
        |home| PathBuf::from(home).join(".local/share/replicant"),
    )
}

/// Returns the shared directory used for Replicant application data.
#[must_use]
pub fn default_data_directory() -> PathBuf {
    data_directory(std::env::var_os("HOME").as_deref())
}

/// Returns the default managed-client SQLite database path.
#[must_use]
pub fn default_database_path() -> PathBuf {
    default_data_directory().join("replicant-client.sqlite")
}

/// Controls how much remote work is required before [`Client::ready`] succeeds.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StartupPolicy {
    /// Restore local state only. No network request is made during startup.
    RestoreOnly,
    /// Require the account baseline, event catch-up, and a live event connection.
    #[default]
    Essential,
    /// Require all bounded account-domain baselines.
    Full,
}

/// Tuning for durable event-history catch-up and the filtered SSE engine.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventStreamOptions {
    pub(crate) log_poll_interval: Duration,
    pub(crate) reconnect_min_backoff: Duration,
    pub(crate) reconnect_max_backoff: Duration,
    pub(crate) max_catchup_pages: usize,
}

impl Default for EventStreamOptions {
    fn default() -> Self {
        Self {
            // SSE is the primary event path. This periodic unfiltered pass is
            // only a safety reconciliation for muted events, so keep it slow
            // enough that it does not compete with normal automation reads.
            log_poll_interval: Duration::from_secs(300),
            reconnect_min_backoff: Duration::from_secs(1),
            reconnect_max_backoff: Duration::from_secs(60),
            max_catchup_pages: 500,
        }
    }
}

impl EventStreamOptions {
    /// Sets how often the periodic unfiltered log poll runs, so muted events
    /// (which never arrive over SSE) still reach durable state.
    #[must_use]
    pub fn log_poll_interval(mut self, interval: Duration) -> Self {
        self.log_poll_interval = interval;
        self
    }

    /// Bounds the SSE reconnect backoff.
    #[must_use]
    pub fn reconnect_backoff(mut self, min: Duration, max: Duration) -> Self {
        self.reconnect_min_backoff = min;
        self.reconnect_max_backoff = max;
        self
    }

    /// Bounds the number of pages accepted from one log catch-up traversal
    /// before continuity is treated as uncertain.
    #[must_use]
    pub fn max_catchup_pages(mut self, pages: usize) -> Self {
        self.max_catchup_pages = pages.max(1);
        self
    }
}

/// Tuning for uncertain-continuity detection and durable reconciliation drain.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationPolicy {
    pub(crate) staleness_threshold: Duration,
    pub(crate) queue_idle_interval: Duration,
}

impl Default for ReconciliationPolicy {
    fn default() -> Self {
        Self {
            staleness_threshold: Duration::from_secs(1800),
            queue_idle_interval: Duration::from_secs(5),
        }
    }
}

impl ReconciliationPolicy {
    /// An applied cursor older than this is treated as uncertain continuity,
    /// without assuming any explicit server cursor rejection.
    #[must_use]
    pub fn staleness_threshold(mut self, threshold: Duration) -> Self {
        self.staleness_threshold = threshold;
        self
    }

    /// How often the durable reconciliation queue is polled when idle.
    #[must_use]
    pub fn queue_idle_interval(mut self, interval: Duration) -> Self {
        self.queue_idle_interval = interval;
        self
    }
}

/// A reason a client may be usable but not fully healthy.
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientDegradation {
    /// A later engine reported a recoverable startup limitation.
    StartupIncomplete,
    /// Event persistence or continuity could not be proven; durable log
    /// replay and reconciliation remain scheduled.
    EventContinuity,
}

/// The observable lifecycle state of a managed [`Client`].
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientStatus {
    /// Configuration is being assembled.
    Starting,
    /// SQLite migration and local durable restoration are in progress.
    Restoring,
    /// The event catch-up engine is still required before readiness.
    CatchingUp,
    /// The reconciliation engine is still required before readiness.
    Synchronizing,
    /// The event connection is being established.
    Connecting,
    /// The configured startup policy has completed.
    Ready,
    /// The client remains usable with a recoverable limitation.
    Degraded(ClientDegradation),
    /// The client has no currently available network connection.
    Offline,
    /// Shutdown has begun.
    Closing,
    /// All owned tasks and the durable store have been closed.
    Closed,
}

/// One independently observable part of remote readiness.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadinessComponent {
    /// Work for this component has not yet completed.
    Pending,
    /// This component satisfies its configured readiness requirement.
    Ready,
    /// This component failed or is currently unavailable.
    Degraded,
}

/// A detailed, non-blocking view of managed-client readiness.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Readiness {
    /// Durable local state has been restored.
    pub local_restoration: ReadinessComponent,
    /// The authenticated account is bound to this durable store.
    pub account_binding: ReadinessComponent,
    /// The essential REST baseline has committed.
    pub essential_rest: ReadinessComponent,
    /// The full bounded REST baseline has committed.
    pub full_rest: ReadinessComponent,
    /// Event-log continuity has been caught up.
    pub event_catchup: ReadinessComponent,
    /// The filtered SSE stream is connected.
    pub sse_connectivity: ReadinessComponent,
    /// Background reconciliation is running without a known failure.
    pub background_reconciliation: ReadinessComponent,
    /// The durable store is accepting and committing work.
    pub store_health: ReadinessComponent,
}

impl Readiness {
    fn pending() -> Self {
        Self {
            local_restoration: ReadinessComponent::Pending,
            account_binding: ReadinessComponent::Pending,
            essential_rest: ReadinessComponent::Pending,
            full_rest: ReadinessComponent::Pending,
            event_catchup: ReadinessComponent::Pending,
            sse_connectivity: ReadinessComponent::Pending,
            background_reconciliation: ReadinessComponent::Pending,
            store_health: ReadinessComponent::Pending,
        }
    }

    /// Returns whether local, durable state can be queried without network I/O.
    #[must_use]
    pub fn locally_usable(self) -> bool {
        matches!(self.local_restoration, ReadinessComponent::Ready)
            && matches!(self.store_health, ReadinessComponent::Ready)
    }

    /// Returns whether a component has reported a known limitation.
    #[must_use]
    pub fn is_degraded(self) -> bool {
        [
            self.local_restoration,
            self.account_binding,
            self.essential_rest,
            self.full_rest,
            self.event_catchup,
            self.sse_connectivity,
            self.background_reconciliation,
            self.store_health,
        ]
        .contains(&ReadinessComponent::Degraded)
    }

    fn policy_satisfied(self, policy: StartupPolicy) -> bool {
        self.locally_usable()
            && match policy {
                StartupPolicy::RestoreOnly => true,
                StartupPolicy::Essential => {
                    matches!(
                        (
                            self.account_binding,
                            self.essential_rest,
                            self.event_catchup,
                            self.sse_connectivity
                        ),
                        (
                            ReadinessComponent::Ready,
                            ReadinessComponent::Ready,
                            ReadinessComponent::Ready,
                            ReadinessComponent::Ready
                        )
                    )
                }
                StartupPolicy::Full => {
                    matches!(
                        (
                            self.account_binding,
                            self.essential_rest,
                            self.full_rest,
                            self.event_catchup,
                            self.sse_connectivity
                        ),
                        (
                            ReadinessComponent::Ready,
                            ReadinessComponent::Ready,
                            ReadinessComponent::Ready,
                            ReadinessComponent::Ready,
                            ReadinessComponent::Ready
                        )
                    )
                }
            }
    }
}

enum Storage {
    File(PathBuf),
    Memory,
}

/// Returns the sibling SQLite path used for long-lived event/telemetry history.
#[must_use]
pub fn default_history_database_path(managed_database: impl AsRef<Path>) -> PathBuf {
    super::store::history_database_path(managed_database.as_ref())
}

/// Configures and starts a managed [`Client`].
pub struct ClientBuilder {
    raw: RawClientBuilder,
    storage: Storage,
    startup_policy: StartupPolicy,
    read_rate_limit_policy: Option<RateLimitPolicy>,
    action_rate_limit_policy: Option<RateLimitPolicy>,
    event_stream_options: EventStreamOptions,
    event_telemetry_sink: Option<Arc<dyn EventTelemetrySink>>,
    reconciliation_policy: ReconciliationPolicy,
    #[cfg(feature = "shared-rate-limit")]
    shared_rate_limit_path: Option<PathBuf>,
}

impl fmt::Debug for ClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("raw", &self.raw)
            .field("storage", &"<redacted>")
            .field("startup_policy", &self.startup_policy)
            .field("read_rate_limit_policy", &self.read_rate_limit_policy)
            .field("action_rate_limit_policy", &self.action_rate_limit_policy)
            .field("event_stream_options", &self.event_stream_options)
            .field("event_telemetry", &self.event_telemetry_sink.is_some())
            .field("reconciliation_policy", &self.reconciliation_policy)
            .finish()
    }
}

impl Default for ClientBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBuilder {
    /// Creates a builder with SQLite durability enabled by default.
    #[must_use]
    pub fn new() -> Self {
        Self {
            raw: RawClientBuilder::new(),
            storage: Storage::File(default_database_path()),
            startup_policy: StartupPolicy::Essential,
            read_rate_limit_policy: None,
            action_rate_limit_policy: None,
            event_stream_options: EventStreamOptions::default(),
            event_telemetry_sink: None,
            reconciliation_policy: ReconciliationPolicy::default(),
            #[cfg(feature = "shared-rate-limit")]
            shared_rate_limit_path: None,
        }
    }

    /// Configures bearer-token authentication. The token is never persisted.
    #[must_use]
    pub fn authentication_token(mut self, token: SecretString) -> Self {
        self.raw = self.raw.authentication_token(token);
        self
    }

    /// Sets the API base URL. Validation remains owned by the raw transport.
    #[must_use]
    pub fn base_url(mut self, url: Url) -> Self {
        self.raw = self.raw.base_url(url);
        self
    }

    /// Stores durable state in this SQLite database.
    #[must_use]
    pub fn sqlite(mut self, path: impl AsRef<Path>) -> Self {
        self.storage = Storage::File(path.as_ref().to_path_buf());
        self
    }

    /// Uses an ephemeral SQLite store.
    #[must_use]
    pub fn in_memory(mut self) -> Self {
        self.storage = Storage::Memory;
        self
    }

    /// Overrides the SQLite file used to coordinate API rate limits between
    /// separate processes. Managed clients otherwise honor
    /// `REPLICANT_RATE_LIMIT_DB`, then fall back to a sibling derived from the
    /// state path (for example `replicant-client.rate-limit.sqlite`).
    #[cfg(feature = "shared-rate-limit")]
    #[must_use]
    pub fn shared_rate_limit_sqlite(mut self, path: impl AsRef<Path>) -> Self {
        self.shared_rate_limit_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the startup policy.
    #[must_use]
    pub fn startup_policy(mut self, policy: StartupPolicy) -> Self {
        self.startup_policy = policy;
        self
    }

    /// Sets the complete request timeout.
    #[must_use]
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.raw = self.raw.request_timeout(timeout);
        self
    }

    /// Sets the connection timeout.
    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.raw = self.raw.connect_timeout(timeout);
        self
    }

    /// Sets the bounded response size accepted from the complete global star
    /// catalogue. Ordinary endpoints retain their smaller default body cap.
    #[must_use]
    pub fn max_star_catalogue_response_body_bytes(mut self, bytes: usize) -> Self {
        self.raw = self.raw.max_star_catalogue_response_body_bytes(bytes);
        self
    }

    /// Selects the TLS backend. Feature validation remains owned by `raw`.
    #[must_use]
    pub fn tls_backend(mut self, backend: TlsBackend) -> Self {
        self.raw = self.raw.tls_backend(backend);
        self
    }

    /// Configures the shared authenticated-read budget.
    #[must_use]
    pub fn read_rate_limit_policy(mut self, policy: RateLimitPolicy) -> Self {
        self.read_rate_limit_policy = Some(policy);
        self
    }

    /// Configures the shared state-changing-request budget.
    #[must_use]
    pub fn action_rate_limit_policy(mut self, policy: RateLimitPolicy) -> Self {
        self.action_rate_limit_policy = Some(policy);
        self
    }

    /// Enables or disables sanitized transport tracing.
    #[must_use]
    pub fn tracing(mut self, enabled: bool) -> Self {
        self.raw = self.raw.emit_tracing(enabled);
        self
    }

    /// Installs a best-effort per-attempt HTTP telemetry sink.
    #[must_use]
    pub fn api_telemetry_sink(mut self, sink: Arc<dyn ApiTelemetrySink>) -> Self {
        self.raw = self.raw.api_telemetry_sink(sink);
        self
    }

    /// Installs a best-effort sink for managed event/SSE telemetry.
    #[must_use]
    pub fn event_telemetry_sink(mut self, sink: Arc<dyn EventTelemetrySink>) -> Self {
        self.event_telemetry_sink = Some(sink);
        self
    }

    /// Stores options used when the client starts its event engine.
    #[must_use]
    pub fn event_stream_options(mut self, options: EventStreamOptions) -> Self {
        self.event_stream_options = options;
        self
    }

    /// Stores reconciliation options for the durable synchronization engine.
    #[must_use]
    pub fn reconciliation_policy(mut self, policy: ReconciliationPolicy) -> Self {
        self.reconciliation_policy = policy;
        self
    }

    /// Opens and restores the durable client foundation.
    ///
    /// `RestoreOnly` reaches [`ClientStatus::Ready`] after local restoration.
    /// `Essential` and `Full` bind the authenticated account, then remain
    /// non-ready until their remote synchronization and event engines complete.
    pub async fn start(self) -> Result<Client> {
        let total_started = Instant::now();
        info!(
            target: "replicant_client::client",
            event = "client.start_started",
            startup_policy = ?self.startup_policy,
            "starting managed client"
        );
        let raw_started = Instant::now();
        #[cfg(feature = "shared-rate-limit")]
        let shared_rate_limit_path = self
            .shared_rate_limit_path
            .clone()
            .or_else(|| std::env::var_os("REPLICANT_RATE_LIMIT_DB").map(PathBuf::from))
            .or_else(|| match &self.storage {
                Storage::File(path) => Some(path.with_extension("rate-limit.sqlite")),
                Storage::Memory => None,
            });
        let raw = self.raw.build()?;
        #[cfg(feature = "shared-rate-limit")]
        if let Some(path) = shared_rate_limit_path {
            raw.rate_limits()
                .enable_shared_sqlite(&path)
                .await
                .map_err(|message| Error::Configuration {
                    message: format!(
                        "could not open shared rate-limit database {}: {message}",
                        path.display()
                    ),
                })?;
            info!(
                target: "replicant_client::raw::rate_limit",
                event = "rate_limit.shared_enabled",
                path = %path.display(),
                "enabled cross-process SQLite rate-limit coordination"
            );
        }
        debug!(
            target: "replicant_client::client",
            event = "client.raw_built",
            elapsed_ms = raw_started.elapsed().as_millis() as u64,
            "built raw transport"
        );
        if let Some(policy) = self.read_rate_limit_policy {
            raw.rate_limits()
                .set_policy(RateLimitBucket::Read, policy)
                .await;
        }
        if let Some(policy) = self.action_rate_limit_policy {
            raw.rate_limits()
                .set_policy(RateLimitBucket::Action, policy)
                .await;
        }

        let (status, _) = watch::channel(ClientStatus::Starting);
        let (readiness, _) = watch::channel(Readiness::pending());
        status.send_replace(ClientStatus::Restoring);
        let store_started = Instant::now();
        let store = open_store(&self.storage).await?;
        info!(
            target: "replicant_client::client",
            event = "client.store_opened",
            elapsed_ms = store_started.elapsed().as_millis() as u64,
            "opened managed SQLite store"
        );
        let restore_started = Instant::now();
        let state = match StateEngine::from_store(store.clone()) {
            Ok(state) => state,
            Err(error) => {
                close_store(store.clone()).await;
                return Err(store_error(error));
            }
        };
        info!(
            target: "replicant_client::client",
            event = "client.state_restored",
            elapsed_ms = restore_started.elapsed().as_millis() as u64,
            "restored durable managed state"
        );

        if self.startup_policy != StartupPolicy::RestoreOnly {
            let identity_started = Instant::now();
            let account = match account_identity(&raw).await {
                Ok(account) => account,
                Err(error) => {
                    close_store(store.clone()).await;
                    return Err(error);
                }
            };
            info!(
                target: "replicant_client::client",
                event = "client.account_identity_loaded",
                elapsed_ms = identity_started.elapsed().as_millis() as u64,
                "loaded authenticated account identity"
            );
            #[cfg(feature = "shared-rate-limit")]
            raw.rate_limits()
                .set_shared_scope(account.as_str().to_owned());
            let binding_started = Instant::now();
            let binding = {
                let mut store = store.lock();
                store.as_mut().ok_or(Error::Closed)?.bind_account(&account)
            };
            if let Err(error) = binding {
                close_store(store.clone()).await;
                return Err(store_error(error));
            }
            info!(
                target: "replicant_client::client",
                event = "client.account_bound",
                elapsed_ms = binding_started.elapsed().as_millis() as u64,
                "bound durable store to authenticated account"
            );
        }

        // Restart recovery, pure-local half: an operation caught mid-transmission
        // (`submitted`) cannot be distinguished from a lost response, so it is
        // promoted to `ambiguous` unconditionally, even under `RestoreOnly`
        // (no network access is required). The network half — retrying
        // operations left at `prepared`, never actually attempted — runs from
        // `events::spawn` once the account and its store binding are settled.
        let recovery_started = Instant::now();
        if let Err(error) = state.promote_crashed_submissions() {
            warn!(
                target: "replicant_client::client",
                event = "client.crashed_submission_promotion_failed",
                elapsed_ms = recovery_started.elapsed().as_millis() as u64,
                error = %error,
                "could not promote interrupted operation submissions"
            );
        } else {
            debug!(
                target: "replicant_client::client",
                event = "client.crashed_submissions_promoted",
                elapsed_ms = recovery_started.elapsed().as_millis() as u64,
                "promoted interrupted operation submissions"
            );
        }

        let scheduler = SchedulerHooks {
            rate_limits: raw.rate_limits().clone(),
        };
        let inner = Arc::new(ClientInner {
            raw,
            scheduler,
            store,
            state,
            events: super::events::EventEngine::new(),
            event_telemetry: self.event_telemetry_sink,
            sync: SyncEngine,
            refresh: super::refresh::RefreshEngine::new(),
            operations: super::operation::OperationEngine::new(),
            lifecycle: Lifecycle::new(),
            startup_policy: self.startup_policy,
            status,
            readiness,
        });
        let client = Client {
            raw: inner.raw.clone(),
            inner,
        };
        client.replay_event_projections()?;
        client.set_readiness(|readiness| {
            readiness.local_restoration = ReadinessComponent::Ready;
            readiness.store_health = ReadinessComponent::Ready;
            readiness.background_reconciliation = ReadinessComponent::Pending;
            readiness.account_binding = ReadinessComponent::Ready;
        });
        if self.startup_policy != StartupPolicy::RestoreOnly {
            let event_spawn_started = Instant::now();
            super::events::spawn(
                &client,
                self.startup_policy,
                self.event_stream_options.clone(),
                self.reconciliation_policy.clone(),
            )
            .await?;
            super::refresh::spawn(&client).await?;
            debug!(
                target: "replicant_client::client",
                event = "client.background_engines_spawned",
                elapsed_ms = event_spawn_started.elapsed().as_millis() as u64,
                "spawned managed event and reconciliation engines"
            );
        }
        info!(
            target: "replicant_client::client",
            event = "client.start_completed",
            elapsed_ms = total_started.elapsed().as_millis() as u64,
            startup_policy = ?self.startup_policy,
            "managed client started"
        );
        Ok(client)
    }
}

async fn open_store(storage: &Storage) -> Result<StoreHandle> {
    match storage {
        Storage::File(path) => StoreHandle::open_file(path.clone())
            .await
            .map_err(store_error),
        Storage::Memory => StoreHandle::open_memory().await.map_err(store_error),
    }
}

async fn account_identity(raw: &RawClient) -> Result<AccountId> {
    let response = raw.accounts().me().await?;
    // ponytail: 2.3.1 exposes no immutable account ID here; use its only
    // authenticated identity field until the server contracts one.
    response
        .value
        .email
        .filter(|email| !email.is_empty())
        .map(AccountId::from)
        .ok_or_else(|| Error::Configuration {
            message: "authenticated account identity is unavailable".into(),
        })
}

pub(super) fn store_error(error: StoreError) -> Error {
    match error {
        StoreError::AccountMismatch {
            stored_account_id,
            supplied_account_id,
        } => Error::AccountStoreMismatch {
            stored_account_id,
            supplied_account_id,
        },
        error => Error::Persistence {
            message: error.to_string(),
        },
    }
}

async fn close_store(store: StoreHandle) {
    let _ = store.close().await;
}

struct SchedulerHooks {
    #[allow(dead_code)] // Gateways and background workers share this coordinator.
    rate_limits: RateLimitCoordinator,
}
struct SyncEngine;

struct Lifecycle {
    accepting: AtomicBool,
    closing: AtomicBool,
    closed: AtomicBool,
    tasks: TokioMutex<Vec<JoinHandle<()>>>,
    completion: watch::Sender<Option<CloseCompletion>>,
    #[cfg(test)]
    close_timeout_millis: AtomicU64,
    #[cfg(test)]
    shutdown_timeouts: AtomicUsize,
}

#[derive(Clone)]
enum CloseCompletion {
    Complete,
    Failed(String),
}

impl Lifecycle {
    fn new() -> Self {
        let (completion, _) = watch::channel(None);
        Self {
            accepting: AtomicBool::new(true),
            closing: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            tasks: TokioMutex::new(Vec::new()),
            completion,
            #[cfg(test)]
            close_timeout_millis: AtomicU64::new(1_000),
            #[cfg(test)]
            shutdown_timeouts: AtomicUsize::new(0),
        }
    }

    #[allow(dead_code)] // Producers register work here for orderly shutdown.
    async fn register(&self, task: JoinHandle<()>) -> Result<()> {
        if !self.accepting.load(Ordering::Acquire) {
            task.abort();
            return Err(Error::Closed);
        }
        let mut tasks = self.tasks.lock().await;
        if !self.accepting.load(Ordering::Acquire) {
            task.abort();
            return Err(Error::Closed);
        }
        tasks.push(task);
        Ok(())
    }

    fn stop_accepting(&self) {
        self.accepting.store(false, Ordering::Release);
    }

    fn cancel(&self) {
        self.stop_accepting();
        if let Ok(tasks) = self.tasks.try_lock() {
            for task in tasks.iter() {
                task.abort();
            }
        }
    }

    async fn close(&self) -> bool {
        self.stop_accepting();
        let tasks = std::mem::take(&mut *self.tasks.lock().await);
        #[cfg(test)]
        let timeout = Duration::from_millis(self.close_timeout_millis.load(Ordering::Acquire));
        #[cfg(not(test))]
        let timeout = Duration::from_secs(1);
        let deadline = tokio::time::Instant::now() + timeout;
        let mut timed_out = false;
        for mut task in tasks {
            if tokio::time::timeout_at(deadline, &mut task).await.is_err() {
                timed_out = true;
                #[cfg(test)]
                self.shutdown_timeouts.fetch_add(1, Ordering::AcqRel);
                task.abort();
                let _ = task.await;
            }
        }
        timed_out
    }

    #[cfg(test)]
    fn set_close_timeout_for_test(&self, timeout: Duration) {
        self.close_timeout_millis.store(
            timeout.as_millis().try_into().unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }

    #[cfg(test)]
    fn shutdown_timeouts_for_test(&self) -> usize {
        self.shutdown_timeouts.load(Ordering::Acquire)
    }

    async fn completion(&self) -> Result<()> {
        let mut completion = self.completion.subscribe();
        loop {
            let current = completion.borrow().clone();
            match current {
                Some(CloseCompletion::Complete) => return Ok(()),
                Some(CloseCompletion::Failed(message)) => {
                    return Err(Error::Persistence { message });
                }
                None => completion.changed().await.map_err(|_| Error::Closed)?,
            }
        }
    }

    fn finish(&self, result: &Result<()>) {
        self.closed.store(true, Ordering::Release);
        self.completion.send_replace(Some(match result {
            Ok(()) => CloseCompletion::Complete,
            Err(error) => CloseCompletion::Failed(error.to_string()),
        }));
    }
}

struct ClientInner {
    raw: RawClient,
    #[allow(dead_code)]
    scheduler: SchedulerHooks,
    store: StoreHandle,
    #[allow(dead_code)] // Public state queries arrive in a later phase.
    state: StateEngine,
    events: super::events::EventEngine,
    event_telemetry: Option<Arc<dyn EventTelemetrySink>>,
    refresh: super::refresh::RefreshEngine,
    #[allow(dead_code)]
    sync: SyncEngine,
    operations: super::operation::OperationEngine,
    lifecycle: Lifecycle,
    startup_policy: StartupPolicy,
    status: watch::Sender<ClientStatus>,
    readiness: watch::Sender<Readiness>,
}

/// Cheaply cloneable managed client. Every clone owns the same lifecycle.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
    raw: RawClient,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("status", &self.status())
            .field("raw", &self.raw)
            .field("store", &"<redacted>")
            .finish_non_exhaustive()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if Arc::strong_count(&self.inner) == 1
            && !self.inner.lifecycle.closed.load(Ordering::Acquire)
        {
            self.inner.lifecycle.cancel();
        }
    }
}

impl Client {
    /// Starts configuring a managed client.
    #[must_use]
    pub fn builder() -> ClientBuilder {
        ClientBuilder::new()
    }

    /// Returns a clone of the unmanaged transport escape hatch.
    ///
    /// Calls made through this client never hydrate, persist, publish, or
    /// otherwise alter managed state.
    #[must_use]
    pub fn raw(&self) -> RawClient {
        self.raw.clone()
    }

    /// Returns a cheap clone whose remote requests use `priority`.
    #[must_use]
    pub fn with_priority(&self, priority: RequestPriority) -> Self {
        Self {
            inner: self.inner.clone(),
            raw: self.raw.with_priority(priority),
        }
    }

    pub(crate) fn with_refresh_budget(&self, run_id: &str, capacity: u32) -> Self {
        Self {
            inner: self.inner.clone(),
            raw: self.raw.with_refresh_budget(run_id, capacity),
        }
    }

    /// Managed account observations commit before this gateway returns.
    #[must_use]
    pub fn account(&self) -> super::gateways::AccountGateway {
        super::gateways::AccountGateway::new(self.clone())
    }

    /// Account tutorial progress. Reads are state-neutral and remain
    /// authoritative on the server rather than entering durable projections.
    #[must_use]
    pub fn tutorials(&self) -> super::gateways::TutorialsGateway {
        super::gateways::TutorialsGateway::new(self.clone())
    }

    /// Account-wide unlocked blueprint catalogue normalized behind the
    /// managed client boundary.
    #[must_use]
    pub fn blueprints(&self) -> super::gateways::BlueprintsGateway {
        super::gateways::BlueprintsGateway::new(self.clone())
    }

    /// Managed device observations commit and publish before this gateway returns.
    #[must_use]
    pub fn devices(&self) -> super::gateways::DevicesGateway {
        super::gateways::DevicesGateway::new(self.clone())
    }

    /// Managed owned-replicant observations are separate from public-directory reads.
    #[must_use]
    pub fn replicants(&self) -> super::gateways::ReplicantsGateway {
        super::gateways::ReplicantsGateway::new(self.clone())
    }

    /// Public-directory reads deliberately remain distinct from owned-replicant reads.
    #[must_use]
    pub fn directory(&self) -> super::gateways::DirectoryGateway {
        super::gateways::DirectoryGateway::new(self.clone())
    }

    /// Durable global catalogue and account-shared star-knowledge reads.
    #[must_use]
    pub fn galaxy(&self) -> super::galaxy::GalaxyGateway {
        super::galaxy::GalaxyGateway::new(self.clone())
    }

    /// Builds a local-only smart travel route selector.
    #[must_use]
    pub fn smart_travel(&self) -> super::smart_travel::SmartTravelRouter {
        super::smart_travel::SmartTravelRouter::new(self.clone())
    }

    /// Builds a synchronization request using this managed client.
    #[must_use]
    pub fn sync(&self) -> super::sync::SyncClient {
        super::sync::SyncClient::new(self.clone())
    }
    /// Starts or inspects durable authoritative recovery work.
    #[must_use]
    pub fn refresh(&self) -> super::refresh::RefreshClient {
        super::refresh::RefreshClient::new(self.clone())
    }

    /// Deduplicated managed event observation, combining unfiltered log
    /// catch-up and filtered SSE delivery. `client.raw().events().stream()`
    /// remains the unmanaged escape hatch and never mutates managed state.
    #[must_use]
    pub fn events(&self) -> super::events::EventsGateway {
        super::events::EventsGateway::new(self.clone())
    }

    /// Local managed-state revision observation for application-level waits.
    #[must_use]
    pub fn state(&self) -> super::state::StateGateway {
        super::state::StateGateway::new(self.clone())
    }

    /// Durable operations previously created through this client, most
    /// useful for recovering unresolved operations after a restart.
    #[must_use]
    pub fn operations(&self) -> super::operation::OperationsGateway {
        super::operation::OperationsGateway::new(self.clone())
    }

    /// The account-wide inbox's only mutation: marking messages read.
    #[must_use]
    pub fn messages(&self) -> super::operation::MessagesGateway {
        super::operation::MessagesGateway::new(self.clone())
    }

    /// Location-scoped mutations (megastructure/location-event contribution).
    #[must_use]
    pub fn locations(&self) -> super::operation::LocationsGateway {
        super::operation::LocationsGateway::new(self.clone())
    }

    /// Location-scoped civilisation event resolution, distinct from account
    /// events and device logs.
    #[must_use]
    pub fn location_events(&self) -> super::operation::LocationEventsGateway {
        super::operation::LocationEventsGateway::new(self.clone())
    }

    /// Resource inventory across locations and replicants.
    #[must_use]
    pub fn inventory(&self) -> super::gateways::InventoryGateway {
        super::gateways::InventoryGateway::new(self.clone())
    }

    /// BobNet channel discovery, relay history, sending, and `bobnet.new`
    /// observation.
    #[must_use]
    pub fn bobnet(&self) -> super::bobnet::BobnetGateway {
        super::bobnet::BobnetGateway::new(self.clone())
    }

    /// Trade controller listing and durable trade mutations.
    #[must_use]
    pub fn trading(&self) -> super::trading::TradingGateway {
        super::trading::TradingGateway::new(self.clone())
    }

    /// Simulation scenarios, active runs, start/abandon, and history.
    #[must_use]
    pub fn simulations(&self) -> super::simulations::SimulationsGateway {
        super::simulations::SimulationsGateway::new(self.clone())
    }

    pub(crate) fn managed_state(&self) -> &StateEngine {
        &self.inner.state
    }
    pub(crate) fn managed_store(&self) -> &StoreHandle {
        &self.inner.store
    }

    pub(crate) fn refresh_notify(&self) -> &tokio::sync::Notify {
        self.inner.refresh.notify()
    }

    pub(crate) fn inner_refresh_notify(&self) -> Arc<tokio::sync::Notify> {
        self.inner.refresh.notify_arc()
    }

    pub(crate) fn managed_events(&self) -> &super::events::EventEngine {
        &self.inner.events
    }

    pub(crate) fn record_event_telemetry(&self, sample: EventTelemetrySample) {
        if let Some(sink) = self.inner.event_telemetry.as_ref() {
            sink.record(sample);
        }
    }

    pub(crate) fn managed_operations(&self) -> &super::operation::OperationEngine {
        &self.inner.operations
    }

    pub(crate) fn managed_raw(&self) -> &RawClient {
        &self.raw
    }

    /// A non-owning reference used by background lifecycle tasks so they
    /// never keep `ClientInner` alive by themselves.
    pub(crate) fn downgrade(&self) -> WeakClient {
        WeakClient(Arc::downgrade(&self.inner))
    }

    pub(crate) fn ensure_open(&self) -> Result<()> {
        if !self.inner.lifecycle.accepting.load(Ordering::Acquire)
            || self.inner.lifecycle.closed.load(Ordering::Acquire)
        {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    /// Returns the latest lifecycle state.
    #[must_use]
    pub fn status(&self) -> ClientStatus {
        self.inner.status.borrow().clone()
    }

    /// Returns whether the configured startup policy has completed.
    #[must_use]
    pub fn startup_policy_satisfied(&self) -> bool {
        self.readiness().policy_satisfied(self.inner.startup_policy)
    }

    /// Returns whether every currently tracked health component is ready.
    #[must_use]
    pub fn is_live(&self) -> bool {
        self.startup_policy_satisfied()
            && matches!(
                self.readiness().background_reconciliation,
                ReadinessComponent::Ready
            )
    }

    /// Watches lifecycle changes shared by all clones.
    #[must_use]
    pub fn watch_status(&self) -> watch::Receiver<ClientStatus> {
        self.inner.status.subscribe()
    }

    /// Returns the components that currently make up the public status.
    #[must_use]
    pub fn readiness(&self) -> Readiness {
        *self.inner.readiness.borrow()
    }

    /// Waits until restored local state can be queried, even if remote work is degraded.
    pub async fn wait_until_usable(&self) -> Result<()> {
        let mut status = self.watch_status();
        loop {
            let current = status.borrow().clone();
            match current {
                ClientStatus::Starting | ClientStatus::Restoring => {
                    status.changed().await.map_err(|_| Error::Closed)?;
                }
                ClientStatus::Closing | ClientStatus::Closed => return Err(Error::Closed),
                _ => return Ok(()),
            }
        }
    }

    /// Waits until the configured startup policy has completed.
    pub async fn ready(&self) -> Result<()> {
        let mut status = self.watch_status();
        loop {
            let current = status.borrow().clone();
            match current {
                ClientStatus::Ready => return Ok(()),
                ClientStatus::Closing | ClientStatus::Closed => return Err(Error::Closed),
                _ => status.changed().await.map_err(|_| Error::Closed)?,
            }
        }
    }

    /// Stops new work, gives workers one second to finish, aborts stragglers,
    /// then flushes SQLite and closes the store.
    pub async fn close(&self) -> Result<()> {
        if self.inner.lifecycle.closed.load(Ordering::Acquire) {
            return self.inner.lifecycle.completion().await;
        }
        if self.inner.lifecycle.closing.swap(true, Ordering::AcqRel) {
            return self.inner.lifecycle.completion().await;
        }

        let close_started = Instant::now();
        self.set_status(ClientStatus::Closing);
        info!(
            target: "replicant_client::client",
            event = "client.close_started",
            "closing managed client"
        );
        let workers_started = Instant::now();
        let timed_out = self.inner.lifecycle.close().await;
        info!(
            target: "replicant_client::client",
            event = "client.workers_stopped",
            elapsed_ms = workers_started.elapsed().as_millis() as u64,
            timed_out,
            "managed background workers stopped"
        );
        let store_started = Instant::now();
        let result = self.inner.store.close().await.map_err(store_error);
        self.set_status(ClientStatus::Closed);
        self.inner.lifecycle.finish(&result);
        info!(
            target: "replicant_client::client",
            event = "client.close_completed",
            elapsed_ms = close_started.elapsed().as_millis() as u64,
            store_close_ms = store_started.elapsed().as_millis() as u64,
            success = result.is_ok(),
            "managed client closed"
        );
        result
    }

    pub(crate) fn set_status(&self, status: ClientStatus) {
        debug!(
            target: "replicant_client::client",
            event = "client.status_changed",
            ?status,
            "managed client status changed"
        );
        self.inner.status.send_replace(status);
    }

    pub(crate) fn set_readiness(&self, update: impl FnOnce(&mut Readiness)) {
        let had_live_connection =
            matches!(self.status(), ClientStatus::Ready | ClientStatus::Offline);
        self.inner.readiness.send_modify(update);
        if !matches!(self.status(), ClientStatus::Closing | ClientStatus::Closed) {
            self.set_status(self.derived_status(had_live_connection));
        }
    }

    fn derived_status(&self, had_live_connection: bool) -> ClientStatus {
        let readiness = self.readiness();
        if !readiness.locally_usable()
            || matches!(readiness.account_binding, ReadinessComponent::Degraded)
            || matches!(readiness.essential_rest, ReadinessComponent::Degraded)
            || matches!(readiness.full_rest, ReadinessComponent::Degraded)
        {
            return ClientStatus::Degraded(ClientDegradation::StartupIncomplete);
        }
        if matches!(readiness.event_catchup, ReadinessComponent::Degraded) {
            return ClientStatus::Degraded(ClientDegradation::EventContinuity);
        }
        if matches!(readiness.sse_connectivity, ReadinessComponent::Degraded) {
            return if had_live_connection {
                ClientStatus::Offline
            } else {
                ClientStatus::Degraded(ClientDegradation::StartupIncomplete)
            };
        }
        if readiness.policy_satisfied(self.inner.startup_policy) {
            return ClientStatus::Ready;
        }
        match self.inner.startup_policy {
            StartupPolicy::RestoreOnly => ClientStatus::Ready,
            StartupPolicy::Essential => ClientStatus::CatchingUp,
            StartupPolicy::Full => {
                if matches!(readiness.full_rest, ReadinessComponent::Pending) {
                    ClientStatus::Synchronizing
                } else if matches!(readiness.event_catchup, ReadinessComponent::Pending) {
                    ClientStatus::CatchingUp
                } else {
                    ClientStatus::Connecting
                }
            }
        }
    }

    /// Registers a background lifecycle task (event catch-up, SSE, durable
    /// reconciliation drain) so `close()` cancels and joins it.
    pub(crate) async fn register_task(&self, task: JoinHandle<()>) -> Result<()> {
        self.inner.lifecycle.register(task).await
    }
}

/// A non-owning reference to a managed client's shared state.
///
/// Long-running background tasks (event catch-up, SSE, reconciliation drain)
/// hold this instead of a [`Client`] clone. Holding a strong `Client` for the
/// task's entire lifetime would keep `ClientInner` alive even after every
/// application-visible clone is dropped, and would defeat the drop-based
/// cancellation safety net described on [`Client`].
#[derive(Clone)]
pub(crate) struct WeakClient(std::sync::Weak<ClientInner>);

impl WeakClient {
    /// Upgrades to a live [`Client`], or `None` once every real clone (and
    /// thus the managed client itself) has been dropped.
    pub(crate) fn upgrade(&self) -> Option<Client> {
        self.0.upgrade().map(|inner| Client {
            raw: inner.raw.clone(),
            inner,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use tokio::{sync::oneshot, time::timeout};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::domain::{
        AccessScope, Device, DeviceKey, DeviceRelationships, DeviceStatus, DeviceType, Observation,
        ObservationAuthority, ObservationMetadata, ObservationSource, Reachability, SourceDocument,
    };
    use crate::managed::store::Store;

    async fn restored_client() -> Client {
        Client::builder()
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only startup")
    }

    #[tokio::test]
    async fn readiness_components_prevent_sse_from_erasing_a_failed_baseline() {
        let client = restored_client().await;
        client.set_readiness(|readiness| {
            readiness.essential_rest = ReadinessComponent::Degraded;
            readiness.sse_connectivity = ReadinessComponent::Ready;
        });

        assert_eq!(
            client.status(),
            ClientStatus::Degraded(ClientDegradation::StartupIncomplete)
        );
        client.close().await.expect("close");
    }

    #[test]
    fn startup_policies_have_distinct_completion_requirements() {
        let mut readiness = Readiness::pending();
        readiness.local_restoration = ReadinessComponent::Ready;
        readiness.store_health = ReadinessComponent::Ready;
        assert!(readiness.policy_satisfied(StartupPolicy::RestoreOnly));
        assert!(!readiness.policy_satisfied(StartupPolicy::Essential));

        readiness.account_binding = ReadinessComponent::Ready;
        readiness.essential_rest = ReadinessComponent::Ready;
        readiness.event_catchup = ReadinessComponent::Ready;
        readiness.sse_connectivity = ReadinessComponent::Ready;
        assert!(readiness.policy_satisfied(StartupPolicy::Essential));
        assert!(!readiness.policy_satisfied(StartupPolicy::Full));

        readiness.full_rest = ReadinessComponent::Ready;
        assert!(readiness.policy_satisfied(StartupPolicy::Full));
    }

    fn test_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("replicant-client-client-{nonce}.sqlite"))
    }

    fn device(id: &str) -> Observation<Device> {
        Observation {
            value: Device {
                key: DeviceKey::live(id.into()),
                device_type: Some(DeviceType::from("miner")),
                status: Some(DeviceStatus::from("idle")),
                location: None,
                deployed_at: None,
                in_control_range: None,
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives: Vec::new(),
                tags: Vec::new(),
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

    async fn account_server(email: &str) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/accounts/me"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"email": email})),
            )
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn clones_share_lifecycle_state() {
        let client = restored_client().await;
        let clone = client.clone();
        assert_eq!(client.status(), ClientStatus::Ready);
        clone.close().await.expect("close clone");
        assert_eq!(client.status(), ClientStatus::Closed);
    }

    #[tokio::test]
    async fn close_is_idempotent_across_clones() {
        let client = restored_client().await;
        let clone = client.clone();
        let (first, second) = tokio::join!(client.close(), clone.close());
        first.expect("first close");
        second.expect("second close");
        client.close().await.expect("repeated close");
    }

    #[tokio::test]
    async fn concurrent_close_callers_share_one_completion() {
        for _ in 0..5 {
            let client = restored_client().await;
            let mut callers = tokio::task::JoinSet::new();
            for _ in 0..100 {
                let clone = client.clone();
                callers.spawn(async move { clone.close().await });
            }
            while let Some(result) = callers.join_next().await {
                result
                    .expect("close task")
                    .expect("shared close completion");
            }
            assert_eq!(client.status(), ClientStatus::Closed);
            client.close().await.expect("repeated close");
        }
    }

    #[tokio::test]
    async fn shutdown_timeout_aborts_only_the_stuck_task_and_closes_the_store() {
        let client = restored_client().await;
        let (graceful_sender, graceful_receiver) = oneshot::channel();
        client
            .register_task(tokio::spawn(async move {
                let _ = graceful_sender.send(());
            }))
            .await
            .expect("register graceful worker");
        client
            .register_task(tokio::spawn(pending::<()>()))
            .await
            .expect("register stuck worker");
        client
            .inner
            .lifecycle
            .set_close_timeout_for_test(Duration::ZERO);

        client.close().await.expect("timeout shutdown");
        assert_eq!(client.inner.lifecycle.shutdown_timeouts_for_test(), 1);
        graceful_receiver.await.expect("graceful worker completed");
        assert!(matches!(
            client
                .inner
                .store
                .execute(|_| Ok::<_, StoreError>(()))
                .await,
            Err(StoreError::Closed)
        ));
        assert_eq!(client.status(), ClientStatus::Closed);
    }

    #[tokio::test]
    async fn closing_rejects_new_work_before_store_shutdown() {
        let client = restored_client().await;
        client.inner.lifecycle.stop_accepting();
        assert!(matches!(client.ensure_open(), Err(Error::Closed)));
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn final_drop_cancels_registered_tasks() {
        struct Signal(Option<oneshot::Sender<()>>);
        impl Drop for Signal {
            fn drop(&mut self) {
                if let Some(sender) = self.0.take() {
                    let _ = sender.send(());
                }
            }
        }

        let client = restored_client().await;
        let (sender, receiver) = oneshot::channel();
        let signal = Signal(Some(sender));
        client
            .register_task(tokio::spawn(async move {
                let _signal = signal;
                pending::<()>().await;
            }))
            .await
            .expect("register task");
        drop(client);
        timeout(Duration::from_secs(1), receiver)
            .await
            .expect("task cancellation")
            .expect("task drop signal");
    }

    #[tokio::test]
    async fn restoration_completes_before_restore_only_readiness() {
        let path = test_path();
        let store = StoreHandle::open_file(path.clone())
            .await
            .expect("open store");
        let state = StateEngine::from_store(store.clone()).expect("restore state engine");
        state
            .persist_devices(&[device("restored")])
            .expect("persist device");
        drop(state);
        close_store(store.clone()).await;

        let client = Client::builder()
            .sqlite(&path)
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore durable client");
        assert_eq!(client.status(), ClientStatus::Ready);
        assert!(
            client
                .inner
                .state
                .snapshot()
                .devices()
                .contains_key(&DeviceKey::live("restored".into()))
        );
        client.close().await.expect("close client");
        std::fs::remove_file(path).expect("remove database");
    }

    #[tokio::test]
    async fn status_watch_observes_close() {
        let client = restored_client().await;
        let mut status = client.watch_status();
        assert_eq!(*status.borrow(), ClientStatus::Ready);
        client.close().await.expect("close client");
        status.changed().await.expect("status update");
        assert_eq!(*status.borrow(), ClientStatus::Closed);
    }

    #[tokio::test]
    async fn startup_failure_releases_the_store() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/accounts/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let path = test_path();
        let error = Client::builder()
            .authentication_token(SecretString::from("secret-token".to_string()))
            .base_url(Url::parse(&server.uri()).expect("server URL"))
            .sqlite(&path)
            .startup_policy(StartupPolicy::Essential)
            .start()
            .await
            .expect_err("missing account identity fails startup");
        assert!(matches!(error, Error::Configuration { .. }));
        drop(Store::open_file(&path).expect("store was released"));
        std::fs::remove_file(path).expect("remove database");
    }

    #[tokio::test]
    async fn account_binding_rejects_a_different_authenticated_account() {
        let path = test_path();
        let first_server = account_server("one@example.test").await;
        let first = Client::builder()
            .authentication_token(SecretString::from("first-token".to_string()))
            .base_url(Url::parse(&first_server.uri()).expect("first URL"))
            .sqlite(&path)
            .start()
            .await
            .expect("first binding");
        assert_eq!(first.status(), ClientStatus::CatchingUp);
        first.close().await.expect("close first client");

        let second_server = account_server("two@example.test").await;
        let error = Client::builder()
            .authentication_token(SecretString::from("second-token".to_string()))
            .base_url(Url::parse(&second_server.uri()).expect("second URL"))
            .sqlite(&path)
            .start()
            .await
            .expect_err("different account is rejected");
        assert!(matches!(error, Error::AccountStoreMismatch { .. }));
        std::fs::remove_file(path).expect("remove database");
    }

    #[test]
    fn database_defaults_live_under_the_user_data_directory() {
        assert_eq!(
            data_directory(Some(std::ffi::OsStr::new("/home/test"))),
            PathBuf::from("/home/test/.local/share/replicant"),
        );
        assert_eq!(
            data_directory(None),
            PathBuf::from(".local/share/replicant"),
        );
    }

    #[test]
    fn history_database_path_is_a_sibling_of_managed_state() {
        assert_eq!(
            default_history_database_path("replicant-client.sqlite"),
            PathBuf::from("replicant-history.sqlite")
        );
        assert_eq!(
            default_history_database_path("private/state.sqlite"),
            PathBuf::from("private/state.history.sqlite")
        );
    }

    #[test]
    fn debug_and_error_output_redact_credentials_and_store_details() {
        let builder = Client::builder()
            .authentication_token(SecretString::from("secret-token".to_string()))
            .sqlite("private/account.sqlite");
        let debug = format!("{builder:?}");
        assert!(!debug.contains("secret-token"));
        assert!(!debug.contains("private/account.sqlite"));

        let error = Error::AccountStoreMismatch {
            stored_account_id: "one@example.test".into(),
            supplied_account_id: "two@example.test".into(),
        };
        assert!(!format!("{error}").contains("@example.test"));
        assert!(!format!("{error:?}").contains("@example.test"));
    }

    #[tokio::test]
    async fn raw_accessor_does_not_publish_managed_state() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/health"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"ok": true})))
            .mount(&server)
            .await;
        let client = Client::builder()
            .base_url(Url::parse(&server.uri()).expect("server URL"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("start client");
        let revision = client.inner.state.snapshot().revision();
        client.raw().health().await.expect("raw health request");
        assert_eq!(client.inner.state.snapshot().revision(), revision);
        client.close().await.expect("close client");
    }
}
