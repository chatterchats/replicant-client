//! The managed client lifecycle and its shared ownership graph.

use std::{
    fmt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
};

use crate::raw::rate_limit::{RateLimitBucket, RateLimitCoordinator, RateLimitPolicy};
use crate::{
    AccountId, Error, Result,
    raw::{Client as RawClient, ClientBuilder as RawClientBuilder, SecretString, TlsBackend, Url},
};

use super::{
    state::StateEngine,
    store::{Store, StoreError, StoreHandle},
};

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

/// Tuning for the durable event-journal catch-up and filtered SSE engine.
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
            log_poll_interval: Duration::from_secs(120),
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

enum Storage {
    File(PathBuf),
    Memory,
}

/// Configures and starts a managed [`Client`].
pub struct ClientBuilder {
    raw: RawClientBuilder,
    storage: Storage,
    startup_policy: StartupPolicy,
    read_rate_limit_policy: Option<RateLimitPolicy>,
    action_rate_limit_policy: Option<RateLimitPolicy>,
    event_stream_options: EventStreamOptions,
    reconciliation_policy: ReconciliationPolicy,
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
            storage: Storage::File(PathBuf::from("replicant-client.sqlite")),
            startup_policy: StartupPolicy::Essential,
            read_rate_limit_policy: None,
            action_rate_limit_policy: None,
            event_stream_options: EventStreamOptions::default(),
            reconciliation_policy: ReconciliationPolicy::default(),
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

    /// Stores event-engine options for Phase 8; no event engine is started yet.
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
    /// non-ready until their Phase 6–8 engines exist.
    pub async fn start(self) -> Result<Client> {
        let raw = self.raw.build()?;
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
        status.send_replace(ClientStatus::Restoring);
        let store = Arc::new(Mutex::new(Some(open_store(&self.storage)?)));
        let state = match StateEngine::from_store(Arc::clone(&store)) {
            Ok(state) => state,
            Err(error) => {
                close_store(&store);
                return Err(store_error(error));
            }
        };

        if self.startup_policy != StartupPolicy::RestoreOnly {
            let account = match account_identity(&raw).await {
                Ok(account) => account,
                Err(error) => {
                    close_store(&store);
                    return Err(error);
                }
            };
            let binding = {
                let mut store = store.lock().expect("store lock poisoned");
                store.as_mut().ok_or(Error::Closed)?.bind_account(&account)
            };
            if let Err(error) = binding {
                close_store(&store);
                return Err(store_error(error));
            }
        }

        // Restart recovery, pure-local half: an operation caught mid-transmission
        // (`submitted`) cannot be distinguished from a lost response, so it is
        // promoted to `ambiguous` unconditionally, even under `RestoreOnly`
        // (no network access is required). The network half — retrying
        // operations left at `prepared`, never actually attempted — runs from
        // `events::spawn` once the account and its store binding are settled.
        let _ = state.promote_crashed_submissions();

        let scheduler = SchedulerHooks {
            rate_limits: raw.rate_limits().clone(),
        };
        let inner = Arc::new(ClientInner {
            raw,
            scheduler,
            store,
            state,
            events: super::events::EventEngine::new(),
            sync: SyncEngine,
            operations: super::operation::OperationEngine::new(),
            lifecycle: Lifecycle::new(),
            status,
        });
        let client = Client { inner };
        match self.startup_policy {
            StartupPolicy::RestoreOnly => client.set_status(ClientStatus::Ready),
            StartupPolicy::Essential => client.set_status(ClientStatus::CatchingUp),
            StartupPolicy::Full => client.set_status(ClientStatus::Synchronizing),
        }
        if self.startup_policy != StartupPolicy::RestoreOnly {
            super::events::spawn(
                &client,
                self.startup_policy,
                self.event_stream_options.clone(),
                self.reconciliation_policy.clone(),
            );
        }
        Ok(client)
    }
}

fn open_store(storage: &Storage) -> Result<Store> {
    match storage {
        Storage::File(path) => Store::open_file(path).map_err(store_error),
        Storage::Memory => Store::open_memory().map_err(store_error),
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

fn store_error(error: StoreError) -> Error {
    match error {
        StoreError::AccountMismatch {
            stored_account_id,
            supplied_account_id,
        } => Error::AccountStoreMismatch {
            stored_account_id,
            supplied_account_id,
        },
        _ => Error::Persistence {
            message: "SQLite store operation failed".into(),
        },
    }
}

fn close_store(store: &StoreHandle) {
    if let Some(mut store) = store.lock().expect("store lock poisoned").take() {
        let _ = store.flush();
    }
}

struct SchedulerHooks {
    #[allow(dead_code)] // Phase 6 gateways and Phase 7–8 workers share this coordinator.
    rate_limits: RateLimitCoordinator,
}
struct SyncEngine;

#[derive(Default)]
struct Lifecycle {
    accepting: AtomicBool,
    closing: AtomicBool,
    closed: AtomicBool,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    completed: Notify,
}

impl Lifecycle {
    fn new() -> Self {
        Self {
            accepting: AtomicBool::new(true),
            ..Self::default()
        }
    }

    #[allow(dead_code)] // Phase 6–8 producers register through this registry.
    fn register(&self, task: JoinHandle<()>) -> Result<()> {
        if !self.accepting.load(Ordering::Acquire) {
            task.abort();
            return Err(Error::Closed);
        }
        let mut tasks = self.tasks.lock().expect("lifecycle task lock poisoned");
        if !self.accepting.load(Ordering::Acquire) {
            task.abort();
            return Err(Error::Closed);
        }
        tasks.push(task);
        Ok(())
    }

    fn cancel(&self) {
        self.accepting.store(false, Ordering::Release);
        for task in self
            .tasks
            .lock()
            .expect("lifecycle task lock poisoned")
            .iter()
        {
            task.abort();
        }
    }

    async fn close(&self) {
        self.cancel();
        let tasks = std::mem::take(&mut *self.tasks.lock().expect("lifecycle task lock poisoned"));
        for task in tasks {
            task.abort();
            let _ = task.await;
        }
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
    #[allow(dead_code)]
    sync: SyncEngine,
    operations: super::operation::OperationEngine,
    lifecycle: Lifecycle,
    status: watch::Sender<ClientStatus>,
}

/// Cheaply cloneable managed client. Every clone owns the same lifecycle.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("status", &self.status())
            .field("raw", &self.inner.raw)
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
        self.inner.raw.clone()
    }

    /// Managed account observations commit before this gateway returns.
    #[must_use]
    pub fn account(&self) -> super::gateways::AccountGateway {
        super::gateways::AccountGateway::new(self.clone())
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

    /// Builds a synchronization request using this managed client.
    #[must_use]
    pub fn sync(&self) -> super::sync::SyncClient {
        super::sync::SyncClient::new(self.clone())
    }

    /// Deduplicated managed event observation, combining unfiltered log
    /// catch-up and filtered SSE delivery. `client.raw().events().stream()`
    /// remains the unmanaged escape hatch and never mutates managed state.
    #[must_use]
    pub fn events(&self) -> super::events::EventsGateway {
        super::events::EventsGateway::new(self.clone())
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

    pub(crate) fn managed_state(&self) -> &StateEngine {
        &self.inner.state
    }

    pub(crate) fn managed_events(&self) -> &super::events::EventEngine {
        &self.inner.events
    }

    pub(crate) fn managed_operations(&self) -> &super::operation::OperationEngine {
        &self.inner.operations
    }

    pub(crate) fn managed_raw(&self) -> &RawClient {
        &self.inner.raw
    }

    /// A non-owning reference used by background lifecycle tasks so they
    /// never keep `ClientInner` alive by themselves.
    pub(crate) fn downgrade(&self) -> WeakClient {
        WeakClient(Arc::downgrade(&self.inner))
    }

    pub(crate) fn ensure_open(&self) -> Result<()> {
        if self.inner.lifecycle.closed.load(Ordering::Acquire) {
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

    /// Watches lifecycle changes shared by all clones.
    #[must_use]
    pub fn watch_status(&self) -> watch::Receiver<ClientStatus> {
        self.inner.status.subscribe()
    }

    /// Waits until the configured startup policy has completed.
    ///
    /// A [`ClientStatus::Degraded`] client is usable with a recoverable
    /// limitation (for example, the live event connection has not opened
    /// yet), so `ready()` resolves successfully for it rather than blocking
    /// forever; `watch_status` remains available to observe recovery.
    pub async fn ready(&self) -> Result<()> {
        let mut status = self.watch_status();
        loop {
            let current = status.borrow().clone();
            match current {
                ClientStatus::Ready | ClientStatus::Degraded(_) => return Ok(()),
                ClientStatus::Closing | ClientStatus::Closed => return Err(Error::Closed),
                _ => status.changed().await.map_err(|_| Error::Closed)?,
            }
        }
    }

    /// Cancels lifecycle tasks, joins them, flushes SQLite, and closes the store.
    pub async fn close(&self) -> Result<()> {
        if self.inner.lifecycle.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        if self.inner.lifecycle.closing.swap(true, Ordering::AcqRel) {
            while !self.inner.lifecycle.closed.load(Ordering::Acquire) {
                self.inner.lifecycle.completed.notified().await;
            }
            return Ok(());
        }

        self.set_status(ClientStatus::Closing);
        self.inner.lifecycle.close().await;
        let result = match self.inner.store.lock().expect("store lock poisoned").take() {
            Some(mut store) => store.flush().map_err(store_error),
            None => Ok(()),
        };
        self.set_status(ClientStatus::Closed);
        self.inner.lifecycle.closed.store(true, Ordering::Release);
        self.inner.lifecycle.completed.notify_waiters();
        result
    }

    pub(crate) fn set_status(&self, status: ClientStatus) {
        self.inner.status.send_replace(status);
    }

    /// Registers a background lifecycle task (event catch-up, SSE, durable
    /// reconciliation drain) so `close()` cancels and joins it.
    pub(crate) fn register_task(&self, task: JoinHandle<()>) -> Result<()> {
        self.inner.lifecycle.register(task)
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
        self.0.upgrade().map(|inner| Client { inner })
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

    async fn restored_client() -> Client {
        Client::builder()
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only startup")
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
        let store = Arc::new(Mutex::new(Some(
            Store::open_file(&path).expect("open store"),
        )));
        let state = StateEngine::from_store(Arc::clone(&store)).expect("restore state engine");
        state
            .persist_devices(&[device("restored")])
            .expect("persist device");
        drop(state);
        close_store(&store);

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
