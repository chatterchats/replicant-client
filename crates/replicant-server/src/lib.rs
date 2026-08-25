//! HTTP query/command API for the local `replicantd` process.

mod inspector;

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        rejection::JsonRejection,
        ws::{Message, WebSocket},
    },
    http::{Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use futures_util::StreamExt;
use replicant_client::{
    ClientDegradation, ClientStatus, Error as ClientError,
    domain::{AccessScope, Device, Inventory, InventoryOwner, Realm},
    managed::{Client, OperationStatus as ManagedOperationStatus, SyncDomain},
    raw::{
        accounts::{AccountAchievementListResponse, AccountMeResponse},
        bobnet::DeviceChannelsResponse,
        devices::{DeviceAuditQuery, DeviceListQuery, DeviceStatus as RawDeviceStatus},
        events::LocationEvent,
        leaderboards::{LeaderboardIndexResponse, LeaderboardResponse},
        reputation::AccountReputationResponse,
    },
};
use replicant_event_planner::remaining_requirements;
use replicant_protocol::{
    AccountEventSummary, AccountEventsSnapshot, AccountReplicantSummary, AchievementSummary,
    ActivityLevel, AutofactoryAvailability, AutofactorySnapshot, AutofactorySummary,
    AutofactoryUtilization, AutomationControlAction, AutomationControlRequest,
    AutomationControlResponse, AutomationStatus, AutomationTrigger as ProtocolTrigger,
    BillCandidateSummary, BillDepartureSummary, BillExpansionSummary, BillFinderRequest,
    BillFinderResponse, BlueprintSummary, BlueprintsSnapshot, BobnetChannelSummary,
    BobnetMessageSummary, BobnetReplicantSummary, BobnetSnapshot, BootstrapMissionSummary,
    BootstrapSnapshot, CargoCarrierSummary, CargoResourceSummary, CargoSnapshot,
    CreateTriggerRequest, DaemonHealth, DescriptorCatalog, DeviceClaim, DeviceLogSummary,
    DeviceLogsSnapshot, DeviceSummary, DevicesSnapshot, DirectorGoalControlRequest,
    DirectorModeRequest, DirectorReplicantRegionRequest, DirectorSnapshot,
    DirectoryReplicantDetail, DirectoryReplicantDetailSnapshot, DirectoryReplicantSummary,
    DirectorySnapshot, DomainSlice, EntityId, EntityIndexSnapshot, EntityInspectorDetail,
    EntityInspectorSnapshot, EntityKind, EntityRef, EntitySummary, ErrorResponse,
    EventCriterionSummary, EventRequirementKind, EventRequirementSummary, EventRewardItem,
    EventRewardsSummary, EventSummary, EventsSnapshot, FactoryJobSummary,
    FiniteExecution as ProtocolFiniteExecution, FiniteExecutionHistoryResponse,
    FiniteExecutionStatus as ProtocolFiniteExecutionStatus, GalaxySceneSnapshot, HealthStatus,
    InboxMessageSummary, InventoryDistribution, InventoryLocationSummary, InventoryOwnerKind,
    InventoryQuantity, InventoryResourceSummary, InventorySnapshot, LeaderboardBoardSummary,
    LeaderboardEntrySummary, LeaderboardsSnapshot, LiveDelta, LiveMessage, MessagesSnapshot,
    MiningInstallationStatus, MiningInstallationSummary, MiningSnapshot, NetworkRelaySummary,
    NetworkSnapshot, Notification, NotificationLevel, OperationClass, OperationKind,
    OperationStatus, OperationUpdate, OverviewReplicant, OverviewSnapshot, OverviewTravel,
    RelayExpansionSummary, RelaySnapshot, ReportsSnapshot, ReputationSummary, RequirementSummary,
    ResultSummary, RunOperationRequest, RunOperationResponse, RuntimeSnapshot, RuntimeSyncStatus,
    SettingsSnapshot, SimulationInterfaceSummary, SimulationRunSummary, SimulationScenarioSummary,
    SimulationsSnapshot, SnapshotMetadata, StandingSnapshot, StartWorkflowRequest,
    StartWorkflowResponse, SurveyMissionSummary, SurveySnapshot, SyncPhase, SystemSceneSnapshot,
    TradeControllerSummary, TradeItemSummary, TradeSnapshot, TradeSummary,
    TriggerCondition as ProtocolTriggerCondition, TriggerId as ProtocolTriggerId,
    TriggerListResponse, TriggerTarget as ProtocolTriggerTarget, TutorialStepSummary,
    TutorialSummary, TutorialsSnapshot, UpdateTriggerRequest, Versioned, WorkflowActivity,
    WorkflowActivityResponse, WorkflowControlResponse, WorkflowDetail,
    WorkflowId as ProtocolWorkflowId, WorkflowListResponse, WorkflowStatus as ProtocolStatus,
    WorkflowStatusCount, WorkflowSummary,
};
use replicant_runtime::{
    ApplicationContext,
    automation::{
        ControllerWorkflowCheckpoint, ExplorationIntent, ExplorationWorkflowCheckpoint, ScanIntent,
        ScanTourCheckpoint, ScanTourIntent,
    },
    bootstrap::BootstrapMission,
    catalogue::{CatalogueError, OperationCatalogue},
    config::{self, RuntimeConfig},
    event::{discovered_events, normalize_event},
    galaxy_scene::galaxy_scene as build_galaxy_scene,
    intelligence::{account_profile, leaderboard, leaderboard_index, standing},
    orchestration::expanded_system_region_map,
    orchestration::{
        assign_replicant_region, cached_director_snapshot, parse_goal_kind, reconcile_director,
        set_director_mode, set_goal_enabled,
    },
    requirements::{AvailabilityKind, InfrastructureKind, RequirementScope, RequirementTarget},
    survey::summarize_plan,
    system_scene::system_scene as build_system_scene,
    telemetry::{RuntimeTelemetrySample, RuntimeTelemetrySink},
    trade::{ShopTrade, TraderSummary, shop_trades, trader_directory},
    workflows::{
        RelayWorkflowCheckpoint, RelayWorkflowConfig, RequirementWorkflowCheckpoint,
        RequirementWorkflowConfig, SurveyWorkflowCheckpoint, SurveyWorkflowConfig,
        WorkflowActivityEvent,
    },
};
use replicant_workflow::{
    AutomationPolicy, AutomationTrigger, FiniteExecution as StoredFiniteExecution,
    FiniteExecutionClass, FiniteExecutionStatus as StoredFiniteExecutionStatus, NewTrigger,
    RepositoryError, ResourceKey, SupervisorError, TriggerCondition, TriggerId, TriggerState,
    TriggerTarget, TriggerTargetClass, WorkflowId, WorkflowInstance, WorkflowKind,
    WorkflowRepository, WorkflowStatus, WorkflowSummary as StoredWorkflowSummary,
    WorkflowSupervisor, WorkflowTelemetrySink,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::{
    sync::{Mutex, Notify, broadcast, oneshot, watch},
    task::AbortHandle,
};
use tower_http::cors::CorsLayer;

/// Live broadcast capacity. Deltas are coalesced per supervisor tick, so this
/// holds many seconds of updates even during heavy fleet activity; lagging
/// subscribers recover by revision comparison rather than by reconnecting.
const LIVE_BUFFER: usize = 1024;
const DIRECTOR_RECONCILE_TIMEOUT: Duration = Duration::from_secs(45);
static DIRECTOR_RECONCILE_SEQUENCE: AtomicU64 = AtomicU64::new(1);

fn lock<T>(mutex: &StdMutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(45);
const UPSTREAM_FANOUT: usize = 8;
const DEFAULT_WORKFLOW_RETENTION_DAYS: u64 = 90;
const WORKFLOW_RETENTION_SWEEP: Duration = Duration::from_secs(60 * 60);
const BILL_REPLICANT_CODE: &str = "A8F48B26";
const BILL_DEFAULT_TRACKING_BEACON: &str = "FEB51E1B";
const BILL_MAX_CANDIDATES: usize = 12;
const BILL_HIGH_CONFIDENCE_DEG: f64 = 0.75;
const BILL_MEDIUM_CONFIDENCE_DEG: f64 = 2.0;
const BILL_AMBIGUITY_GAP_DEG: f64 = 0.35;

/// Default loopback address used by the daemon.
pub const DEFAULT_BIND: &str = "127.0.0.1:8080";

/// Environment-backed daemon configuration for one profile.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaemonConfig {
    /// Application profile name.
    pub profile: String,
    /// Managed SDK SQLite database.
    pub managed_database: PathBuf,
    /// Workflow/runtime SQLite database.
    pub runtime_database: PathBuf,
    /// Isolated API/runtime telemetry SQLite database.
    pub telemetry_database: PathBuf,
    /// Directory containing persistent daemon log files.
    pub log_directory: PathBuf,
    /// Local HTTP listen address.
    pub bind: SocketAddr,
    /// Shared secret required on every request when present.
    ///
    /// Loopback binds may run without a token because reaching the socket
    /// already implies local access. Any non-loopback bind requires one: see
    /// [`DaemonConfig::validate`].
    pub token: Option<String>,
    /// Terminal workflow retention in days, or `None` to preserve full history.
    pub workflow_retention_days: Option<u64>,
}

impl DaemonConfig {
    /// Loads configuration from the process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        let profile = env::var("REPLICANT_PROFILE").unwrap_or_else(|_| "default".to_owned());
        let managed_database = env::var_os("REPLICANT_DB")
            .map(PathBuf::from)
            .unwrap_or_else(replicant_client::default_database_path);
        let runtime_database = env::var_os("REPLICANT_RUNTIME_DB")
            .map(PathBuf::from)
            .unwrap_or_else(replicant_runtime::config::default_runtime_database_path);
        let telemetry_database = env::var_os("REPLICANT_TELEMETRY_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                replicant_runtime::telemetry::default_telemetry_database_path(&managed_database)
            });
        let log_directory = env::var_os("REPLICANT_LOG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| default_log_directory(&runtime_database));
        let bind = env::var("REPLICANTD_BIND")
            .unwrap_or_else(|_| DEFAULT_BIND.to_owned())
            .parse()
            .map_err(ConfigError::Bind)?;
        let token = env::var("REPLICANTD_TOKEN")
            .ok()
            .map(|token| token.trim().to_owned())
            .filter(|token| !token.is_empty());
        let workflow_retention_days = match env::var("REPLICANT_WORKFLOW_RETENTION_DAYS") {
            Ok(value) if matches!(value.trim(), "0" | "off" | "none") => None,
            Ok(value) => Some(
                value
                    .parse()
                    .map_err(|_| ConfigError::WorkflowRetention(value))?,
            ),
            Err(env::VarError::NotPresent) => Some(DEFAULT_WORKFLOW_RETENTION_DAYS),
            Err(env::VarError::NotUnicode(_)) => {
                return Err(ConfigError::WorkflowRetention("<non-unicode>".to_owned()));
            }
        };
        let config = Self {
            profile,
            managed_database,
            runtime_database,
            telemetry_database,
            log_directory,
            bind,
            token,
            workflow_retention_days,
        };
        config.validate()?;
        Ok(config)
    }

    /// Rejects configurations that would expose an unauthenticated daemon.
    ///
    /// Every route can start workflows, run actions, and cancel automation, so
    /// a daemon reachable from outside the machine must require a token.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.token.is_none() && !is_loopback(self.bind) {
            return Err(ConfigError::MissingToken(self.bind));
        }
        Ok(())
    }

    /// Returns whether one presented credential matches the configured token.
    #[must_use]
    pub fn authorized(&self, presented: Option<&str>) -> bool {
        let Some(expected) = self.token.as_deref() else {
            return true;
        };
        presented.is_some_and(|presented| constant_time_eq(presented, expected))
    }
}

fn default_log_directory(runtime_database: &std::path::Path) -> PathBuf {
    runtime_database
        .parent()
        .unwrap_or_else(|| std::path::Path::new(""))
        .join("logs")
}

/// Compares two secrets without leaking their common prefix length through
/// timing.
fn constant_time_eq(left: &str, right: &str) -> bool {
    let (left, right) = (left.as_bytes(), right.as_bytes());
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}

/// Invalid daemon configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The configured listen address is invalid.
    #[error("invalid REPLICANTD_BIND: {0}")]
    Bind(std::net::AddrParseError),
    /// A non-loopback bind was requested without a shared secret.
    #[error(
        "REPLICANTD_BIND={0} is reachable beyond this machine, so REPLICANTD_TOKEN must be set; \
         every daemon route can start workflows and run actions"
    )]
    MissingToken(SocketAddr),
    /// The terminal workflow retention window was malformed.
    #[error("invalid REPLICANT_WORKFLOW_RETENTION_DAYS {0:?}; use a day count or off")]
    WorkflowRetention(String),
}

/// Shared daemon services. HTTP handlers never construct managed clients.
pub struct AppState {
    context: ApplicationContext,
    repository: Arc<WorkflowRepository>,
    supervisor: WorkflowSupervisor,
    catalogue: OperationCatalogue,
    running_actions: StdMutex<BTreeMap<String, (String, AbortHandle)>>,
    live: broadcast::Sender<LiveMessage>,
    revision: AtomicU64,
    publish_lock: StdMutex<()>,
    /// Slices invalidated since the last flush, coalesced so one tick of
    /// managed churn costs one live message instead of one per slice.
    pending_slices: StdMutex<BTreeSet<DomainSlice>>,
    /// Revision each slice last reached, served with snapshots so a client can
    /// tell whether its cached projection is current.
    slice_revisions: StdMutex<BTreeMap<DomainSlice, u64>>,
    /// Device projection memoized against the managed revision it was built
    /// from. `/api/devices`, `/api/autofactories`, and the relay projection all
    /// rebuild the identical row set, previously once per request.
    device_rows: tokio::sync::Mutex<Option<(u64, Arc<Vec<DeviceSummary>>)>>,
    message_sync: Mutex<()>,
    director_reconcile: Mutex<()>,
    director_wake: Notify,
    runtime_telemetry: Option<Arc<dyn RuntimeTelemetrySink>>,
    daemon: DaemonConfig,
}

impl AppState {
    /// Builds daemon state around one managed client and one runtime repository.
    pub fn new(
        client: Client,
        runtime_config: RuntimeConfig,
        repository: Arc<WorkflowRepository>,
        daemon: DaemonConfig,
    ) -> Result<Arc<Self>, CatalogueError> {
        Self::new_with_telemetry(client, runtime_config, repository, daemon, None, None)
    }

    /// Builds daemon state with optional workflow/runtime observability sinks.
    pub fn new_with_telemetry(
        client: Client,
        runtime_config: RuntimeConfig,
        repository: Arc<WorkflowRepository>,
        daemon: DaemonConfig,
        workflow_telemetry: Option<Arc<dyn WorkflowTelemetrySink>>,
        runtime_telemetry: Option<Arc<dyn RuntimeTelemetrySink>>,
    ) -> Result<Arc<Self>, CatalogueError> {
        let catalogue = OperationCatalogue::new()?;
        let mut supervisor = WorkflowSupervisor::with_managed_client(
            repository.clone(),
            catalogue.workflow_registry(),
            client.clone(),
        );
        if let Some(sink) = workflow_telemetry {
            supervisor = supervisor.with_telemetry_sink(sink);
        }
        let revision = client.state().revision().unwrap_or_default();
        Ok(Arc::new(Self {
            context: ApplicationContext::new(client, runtime_config),
            repository,
            supervisor,
            catalogue,
            running_actions: StdMutex::new(BTreeMap::new()),
            live: broadcast::channel(LIVE_BUFFER).0,
            revision: AtomicU64::new(revision),
            publish_lock: StdMutex::new(()),
            pending_slices: StdMutex::new(BTreeSet::new()),
            slice_revisions: StdMutex::new(BTreeMap::new()),
            device_rows: tokio::sync::Mutex::new(None),
            message_sync: Mutex::new(()),
            director_reconcile: Mutex::new(()),
            director_wake: Notify::new(),
            runtime_telemetry,
            daemon,
        }))
    }

    /// Returns the daemon's single managed client.
    #[must_use]
    pub fn client(&self) -> &Client {
        self.context.client()
    }

    fn record_runtime_telemetry(
        &self,
        metric: &'static str,
        series: impl Into<String>,
        value: i64,
        duration_ms: Option<u64>,
    ) {
        let Some(sink) = self.runtime_telemetry.as_ref() else {
            return;
        };
        sink.record(RuntimeTelemetrySample {
            observed_at_ms: now_millis().unwrap_or_default(),
            metric,
            series: series.into(),
            value,
            duration_ms,
        });
    }

    /// Publishes a frontend-safe runtime notification.
    pub fn notify(&self, notification: replicant_protocol::Notification) {
        self.publish(LiveDelta::Notification(notification));
    }

    fn publish(&self, delta: LiveDelta) {
        let _guard = lock(&self.publish_lock);
        let revision = self.revision.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.live.send(LiveMessage::current(revision, delta));
    }

    /// Marks a slice dirty without publishing.
    ///
    /// Dirty slices are flushed once per supervisor tick by
    /// [`AppState::flush_invalidations`].
    fn invalidate(&self, slice: DomainSlice) {
        lock(&self.pending_slices).insert(slice);
    }

    /// Publishes one coalesced invalidation for everything marked dirty.
    fn flush_invalidations(&self) {
        let slices = std::mem::take(&mut *lock(&self.pending_slices));
        if slices.is_empty() {
            return;
        }
        let guard = lock(&self.publish_lock);
        let revision = self.revision.fetch_add(1, Ordering::Relaxed) + 1;
        let mut current = lock(&self.slice_revisions);
        let slices = slices
            .into_iter()
            .map(|slice| {
                current.insert(slice, revision);
                (slice, revision)
            })
            .collect::<BTreeMap<_, _>>();
        drop(current);
        let _ = self.live.send(LiveMessage::current(
            revision,
            LiveDelta::DomainsInvalidated { slices },
        ));
        drop(guard);
    }

    fn snapshot_metadata(&self) -> Result<SnapshotMetadata, ApiError> {
        // Deliberately lock-free: metadata is read on every GET, and taking the
        // publish lock here contended with the publisher during exactly the
        // bursts that make reads frequent.
        Ok(SnapshotMetadata {
            revision: self.revision.load(Ordering::Relaxed),
            generated_at_ms: now_millis()?,
        })
    }

    fn slice_revisions(&self) -> BTreeMap<DomainSlice, u64> {
        lock(&self.slice_revisions).clone()
    }

    fn resnapshot_message(&self) -> Result<LiveMessage, ApiError> {
        let metadata = self.snapshot_metadata()?;
        Ok(LiveMessage::current(
            metadata.revision,
            LiveDelta::Snapshot(metadata),
        ))
    }
}

/// Rejects requests that do not present the configured shared secret.
///
/// Accepts either `Authorization: Bearer <token>` or a `token` query parameter;
/// the latter exists because browser `WebSocket` construction cannot set
/// headers. `/api/health` stays open so container health checks and the
/// frontend's reachability probe work without credentials — it exposes only
/// liveness and a version string.
async fn authenticate(
    State(state): State<Arc<AppState>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if state.daemon.token.is_none() || request.uri().path() == "/api/health" {
        return next.run(request).await;
    }
    let header_token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim);
    let query_token = request.uri().query().and_then(|query| {
        query.split('&').find_map(|pair| {
            pair.strip_prefix("token=")
                .map(|token| token.trim_end_matches('#'))
        })
    });
    if state.daemon.authorized(header_token.or(query_token)) {
        next.run(request).await
    } else {
        ApiError::unauthorized().into_response()
    }
}

async fn trace_http_request(request: Request<axum::body::Body>, next: Next) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let started = Instant::now();
    tracing::debug!(method = %method, path = %path, "daemon HTTP request started");
    let response = next.run(request).await;
    let status = response.status();
    let elapsed_ms = started.elapsed().as_millis();
    if status.is_server_error() {
        tracing::error!(
            method = %method,
            path = %path,
            status = status.as_u16(),
            elapsed_ms,
            "daemon HTTP request failed"
        );
    } else if status.is_client_error() {
        tracing::warn!(
            method = %method,
            path = %path,
            status = status.as_u16(),
            elapsed_ms,
            "daemon HTTP request rejected"
        );
    } else {
        tracing::debug!(
            method = %method,
            path = %path,
            status = status.as_u16(),
            elapsed_ms,
            "daemon HTTP request completed"
        );
    }
    response
}

/// Builds the local HTTP router.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/snapshot", get(snapshot))
        .route("/api/overview", get(overview))
        .route("/api/devices", get(devices))
        .route("/api/inventory", get(inventory))
        .route("/api/autofactories", get(autofactories))
        .route("/api/cargo", get(cargo))
        .route("/api/missions/survey", get(survey_missions))
        .route("/api/missions/mining", get(mining_missions))
        .route("/api/missions/relay", get(relay_missions))
        .route("/api/missions/bootstrap", get(bootstrap_missions))
        .route("/api/events", get(events))
        .route("/api/activity", get(account_activity))
        .route("/api/devices/{code}/logs", get(device_logs))
        .route("/api/simulations", get(simulations))
        .route("/api/blueprints", get(blueprints))
        .route("/api/directory", get(directory))
        .route("/api/directory/{code}", get(directory_replicant))
        .route("/api/tutorials", get(tutorials))
        .route("/api/trade", get(trade))
        .route("/api/trade/bill/find", post(find_bill))
        .route("/api/reports", get(reports))
        .route("/api/messages", get(messages))
        .route("/api/messages/read", post(mark_messages_read))
        .route("/api/bobnet", get(bobnet))
        .route("/api/network", get(network))
        .route("/api/standing", get(standing_snapshot))
        .route("/api/leaderboards", get(leaderboards))
        .route("/api/settings", get(settings))
        .route("/api/entities", get(entity_index))
        .route("/api/entities/{kind}/{id}", get(entity_inspector))
        .route("/api/galaxy-scene", get(galaxy_scene))
        .route("/api/system-scene/{system}", get(system_scene))
        .route("/api/locations/refresh", post(refresh_locations))
        .route(
            "/api/locations/refresh/{system}",
            post(refresh_system_locations),
        )
        .route("/ws", get(websocket))
        .route("/api/descriptors", get(descriptors))
        .route("/api/reports/{kind}", post(run_report))
        .route("/api/actions/{kind}", post(run_action))
        .route("/api/action-executions/{id}/cancel", post(cancel_action))
        .route("/api/history", get(finite_execution_history))
        .route("/api/triggers", get(list_triggers).post(create_trigger))
        .route(
            "/api/triggers/{id}",
            put(update_trigger).delete(delete_trigger),
        )
        .route("/api/triggers/{id}/fire", post(fire_trigger))
        .route("/api/automation/control", post(control_automation))
        .route("/api/director", get(director_snapshot))
        .route("/api/director/reconcile", post(reconcile_director_now))
        .route("/api/director/mode", put(update_director_mode))
        .route("/api/director/goals/{kind}", put(update_director_goal))
        .route(
            "/api/director/replicants/{code}/region",
            put(update_director_replicant_region),
        )
        .route("/api/workflows", get(list_workflows).post(start_workflow))
        .route("/api/workflows/{id}", get(workflow_detail))
        .route("/api/workflows/{id}/activity", get(workflow_activity))
        .route("/api/workflows/{id}/pause", post(pause_workflow))
        .route("/api/workflows/{id}/resume", post(resume_workflow))
        .route("/api/workflows/{id}/cancel", post(cancel_workflow))
        .layer(middleware::from_fn_with_state(state.clone(), authenticate))
        .layer(middleware::from_fn(trace_http_request))
        .layer(
            CorsLayer::new()
                .allow_origin([
                    "tauri://localhost".parse().expect("valid Tauri origin"),
                    "http://tauri.localhost"
                        .parse()
                        .expect("valid Tauri origin"),
                ])
                .allow_private_network(true)
                .allow_methods([
                    axum::http::Method::GET,
                    axum::http::Method::POST,
                    axum::http::Method::PUT,
                    axum::http::Method::DELETE,
                ])
                .allow_headers([
                    axum::http::header::CONTENT_TYPE,
                    axum::http::header::AUTHORIZATION,
                ]),
        )
        .with_state(state)
}

/// Runs periodic persisted-workflow reconciliation until shutdown.
pub async fn run_supervisor(state: Arc<AppState>, mut shutdown: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    let mut telemetry_interval = tokio::time::interval(Duration::from_secs(5));
    let mut retention_interval = tokio::time::interval(WORKFLOW_RETENTION_SWEEP);
    retention_interval.tick().await;
    retention_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    telemetry_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut tick_count = 0_i64;
    let mut tick_errors = 0_i64;
    let mut tick_duration_sum_ms = 0_u64;
    let mut tick_duration_max_ms = 0_u64;
    let mut revisions = state.client().state().watch().ok();
    let mut operations = state.client().operations().watch().ok();
    let mut bobnet = state.client().bobnet().watch().await.ok();
    let mut workflows = state
        .repository
        .list_summaries()
        .unwrap_or_default()
        .into_iter()
        .map(|workflow| (workflow.id.to_string(), workflow.revision))
        .collect::<BTreeMap<_, _>>();
    let mut activity_cursor = state.repository.latest_activity_id().unwrap_or_default();
    let mut managed_phase = sync_phase(&state.client().status());
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let tick_started = Instant::now();
                let tick_result = state.supervisor.tick().await;
                let tick_duration_ms = tick_started
                    .elapsed()
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX);
                tick_count = tick_count.saturating_add(1);
                tick_duration_sum_ms = tick_duration_sum_ms.saturating_add(tick_duration_ms);
                tick_duration_max_ms = tick_duration_max_ms.max(tick_duration_ms);
                if let Err(error) = tick_result {
                    tick_errors = tick_errors.saturating_add(1);
                    tracing::error!(error = %error, "workflow supervisor tick failed");
                }
                publish_workflow_updates(&state, &mut workflows, &mut activity_cursor);
                let mut stop_bobnet_watch = false;
                if let Some(watch) = bobnet.as_mut() {
                    match watch.try_next() {
                        Ok(events) if !events.is_empty() => state.invalidate(DomainSlice::Bobnet),
                        Ok(_) => {}
                        Err(error) => {
                            tracing::warn!(error = %error, "BobNet event watch stopped");
                            state.record_runtime_telemetry("watcher_lag", "bobnet", 1, None);
                            stop_bobnet_watch = true;
                        }
                    }
                }
                if stop_bobnet_watch {
                    bobnet = None;
                }
                state.flush_invalidations();
                let status = state.client().status();
                let phase = sync_phase(&status);
                if phase != managed_phase {
                    managed_phase = phase;
                    let sync = runtime_sync_status(&state, &status);
                    state.publish(LiveDelta::DaemonStatusChanged {
                        health: DaemonHealth {
                            status: health_status(&status),
                            daemon_version: env!("CARGO_PKG_VERSION").to_owned(),
                            detail: status_detail(&status).map(str::to_owned),
                        },
                        sync: sync.clone(),
                    });
                    if matches!(phase, SyncPhase::Degraded | SyncPhase::Offline) {
                        state.notify(sync_notification(&sync));
                    }
                }
            }
            _ = telemetry_interval.tick() => {
                let average_tick_ms = if tick_count > 0 {
                    tick_duration_sum_ms / u64::try_from(tick_count).unwrap_or(1)
                } else {
                    0
                };
                state.record_runtime_telemetry(
                    "supervisor_ticks",
                    "all",
                    tick_count,
                    Some(average_tick_ms),
                );
                state.record_runtime_telemetry("supervisor_tick_errors", "all", tick_errors, None);
                state.record_runtime_telemetry(
                    "supervisor_tick_max_ms",
                    "all",
                    i64::try_from(tick_duration_max_ms).unwrap_or(i64::MAX),
                    None,
                );
                record_workflow_snapshot(&state);
                tick_count = 0;
                tick_errors = 0;
                tick_duration_sum_ms = 0;
                tick_duration_max_ms = 0;
            }
            _ = retention_interval.tick(), if state.daemon.workflow_retention_days.is_some() => {
                let days = state.daemon.workflow_retention_days.expect("guarded");
                let age_millis = days.saturating_mul(24 * 60 * 60 * 1_000);
                let cutoff = now_millis()
                    .unwrap_or_default()
                    .saturating_sub(i64::try_from(age_millis).unwrap_or(i64::MAX));
                match state.repository.prune_terminal_before(cutoff) {
                    Ok(removed) if removed > 0 => {
                        tracing::info!(removed, days, "pruned retained terminal workflows");
                    }
                    Ok(_) => {}
                    Err(error) => tracing::warn!(error = %error, "workflow retention sweep failed"),
                }
            }
            revision = async { revisions.as_mut().expect("guarded").next().await }, if revisions.is_some() => {
                match revision {
                    Ok(_) => {
                        // Marked here, flushed as one coalesced message on the
                        // next tick: a busy account bumps the managed revision
                        // continuously, and one message per slice per bump was
                        // the dominant source of live-channel churn.
                        for slice in [
                            DomainSlice::Entities,
                            DomainSlice::Universe,
                            DomainSlice::Overview,
                            DomainSlice::Devices,
                            DomainSlice::Inventory,
                            DomainSlice::Autofactories,
                            DomainSlice::Cargo,
                            DomainSlice::Missions,
                            DomainSlice::Events,
                            DomainSlice::Activity,
                            DomainSlice::Trade,
                            DomainSlice::Simulations,
                            DomainSlice::Blueprints,
                            DomainSlice::Directory,
                            DomainSlice::Tutorials,
                            DomainSlice::Messages,
                            DomainSlice::Network,
                            DomainSlice::Standing,
                            DomainSlice::Leaderboards,
                            DomainSlice::Director,
                        ] {
                            state.invalidate(slice);
                        }
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "managed state watcher stopped");
                        state.record_runtime_telemetry("watcher_lag", "managed_state", 1, None);
                        revisions = None;
                    }
                }
            }
            operation = async { operations.as_mut().expect("guarded").next().await }, if operations.is_some() => {
                match operation {
                    Ok((id, status)) => state.publish(LiveDelta::OperationUpdated(OperationUpdate {
                        id: EntityId(id.to_string()),
                        workflow_id: None,
                        status: operation_status(status),
                        message: None,
                        updated_at_ms: now_millis().unwrap_or_default(),
                    })),
                    Err(error) => {
                        tracing::warn!(error = %error, "managed operation watcher stopped");
                        state.record_runtime_telemetry("watcher_lag", "managed_operations", 1, None);
                        operations = None;
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}

fn record_workflow_snapshot(state: &AppState) {
    let Ok(workflows) = state.repository.list_summaries() else {
        return;
    };
    let now = now_millis().unwrap_or_default();
    let mut status_counts = BTreeMap::<&'static str, i64>::new();
    let mut waiting_age_by_kind = BTreeMap::<String, i64>::new();
    for workflow in workflows {
        let status = workflow_status_name(workflow.status);
        *status_counts.entry(status).or_default() += 1;
        if workflow.status == WorkflowStatus::Waiting {
            let age = now.saturating_sub(workflow.updated_at).max(0);
            waiting_age_by_kind
                .entry(workflow.kind.as_str().to_owned())
                .and_modify(|current| *current = (*current).max(age))
                .or_insert(age);
        }
    }
    for (status, count) in status_counts {
        state.record_runtime_telemetry("workflow_status", status, count, None);
    }
    for (kind, age_ms) in waiting_age_by_kind {
        state.record_runtime_telemetry("workflow_wait_age_ms", kind, age_ms, None);
    }
}

fn workflow_status_name(status: WorkflowStatus) -> &'static str {
    match status {
        WorkflowStatus::Queued => "queued",
        WorkflowStatus::Running => "running",
        WorkflowStatus::Waiting => "waiting",
        WorkflowStatus::Paused => "paused",
        WorkflowStatus::Reconciling => "reconciling",
        WorkflowStatus::Succeeded => "succeeded",
        WorkflowStatus::Failed => "failed",
        WorkflowStatus::Cancelled => "cancelled",
    }
}

/// Evaluates durable automation definitions from local managed events and projections.
pub async fn run_trigger_engine(state: Arc<AppState>, mut shutdown: watch::Receiver<bool>) {
    // Schedules need second-level checks; the full event sweep is a safety net
    // for anything the stream missed and does not need that cadence.
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut sweep = tokio::time::interval(Duration::from_secs(60));
    let mut events = state.client().events().watch().await.ok();
    let mut revisions = state.client().state().watch().ok();
    evaluate_schedules_and_parents(&state).await;
    evaluate_event_triggers_for(&state, &[]).await;
    evaluate_state_triggers(&state).await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                evaluate_schedules_and_parents(&state).await;
            }
            _ = sweep.tick() => {
                evaluate_event_triggers_for(&state, &[]).await;
            }
            event = async { events.as_mut().expect("guarded").next().await }, if events.is_some() => {
                match event {
                    // Evaluate only the triggers watching this event name.
                    Ok(event) => {
                        evaluate_event_triggers_for(&state, &[event.name.as_str().to_owned()]).await;
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "trigger event watcher lagged; recovering from durable history");
                        state.record_runtime_telemetry("watcher_lag", "trigger_events", 1, None);
                        events = state.client().events().watch().await.ok();
                        evaluate_event_triggers_for(&state, &[]).await;
                    }
                }
            }
            revision = async { revisions.as_mut().expect("guarded").next().await }, if revisions.is_some() => {
                if let Err(error) = revision {
                    tracing::warn!(error = %error, "trigger state watcher stopped");
                    state.record_runtime_telemetry("watcher_lag", "trigger_state", 1, None);
                    revisions = state.client().state().watch().ok();
                }
                evaluate_state_triggers(&state).await;
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}

async fn evaluate_schedules_and_parents(state: &Arc<AppState>) {
    let now = now_millis().unwrap_or_default();
    let Ok(triggers) = state.repository.list_triggers() else {
        return;
    };
    let workflows = state.repository.list().unwrap_or_default();
    for trigger in triggers.into_iter().filter(|trigger| trigger.enabled) {
        match &trigger.condition {
            TriggerCondition::Schedule { interval_millis } => {
                if *interval_millis <= 0 {
                    notify_trigger_error(state, &trigger, "schedule interval must be positive");
                    continue;
                }
                let Some(due) = trigger.next_run_at.filter(|due| *due <= now) else {
                    continue;
                };
                let mut next = due.saturating_add(*interval_millis);
                while next <= now {
                    next = next.saturating_add(*interval_millis);
                }
                claim_and_launch(state, trigger, format!("schedule:{due}"), Some(next), None).await;
            }
            TriggerCondition::ParentWorkflow {
                parent_kind,
                status,
            } => {
                for parent in workflows.iter().filter(|workflow| {
                    workflow.status == *status
                        && parent_kind
                            .as_ref()
                            .is_none_or(|kind| workflow.kind == *kind)
                }) {
                    claim_and_launch(
                        state,
                        trigger.clone(),
                        format!("parent:{}", parent.id),
                        None,
                        Some(parent.id),
                    )
                    .await;
                }
            }
            _ => {}
        }
    }
}

async fn evaluate_state_triggers(state: &Arc<AppState>) {
    let Ok(revision) = state.client().state().revision() else {
        return;
    };
    let Ok(triggers) = state.repository.list_triggers() else {
        return;
    };
    for trigger in triggers.into_iter().filter(|trigger| trigger.enabled) {
        if let TriggerCondition::StateCondition { minimum_revision } = trigger.condition
            && revision >= minimum_revision
        {
            claim_and_launch(
                state,
                trigger,
                format!("state:{minimum_revision}"),
                None,
                None,
            )
            .await;
        }
    }
}

/// Evaluates game-event triggers.
///
/// `names` restricts work to triggers watching an event that just arrived;
/// an empty slice evaluates all of them (startup and periodic sweeps). Without
/// it, every wake ran one durable history query per event trigger.
async fn evaluate_event_triggers_for(state: &Arc<AppState>, names: &[String]) {
    let Ok(triggers) = state.repository.list_triggers() else {
        return;
    };
    let current_cursor = state.client().events().cursor().ok().flatten();
    for trigger in triggers.into_iter().filter(|trigger| trigger.enabled) {
        let TriggerCondition::GameEvent {
            event_name,
            device_code,
        } = &trigger.condition
        else {
            continue;
        };
        if !names.is_empty() && !names.iter().any(|name| name == event_name) {
            continue;
        }
        let mut query = state.client().events().history().named(event_name);
        if let Some(cursor) = &trigger.event_cursor {
            query = query.after(cursor);
        }
        if let Some(device_code) = device_code {
            query = query.for_device(device_code);
        }
        let Ok(events) = query.collect().await else {
            notify_trigger_error(state, &trigger, "managed event history unavailable");
            continue;
        };
        for event in events {
            if state.client().state().revision().is_err() {
                notify_trigger_error(state, &trigger, "managed projections unavailable");
                break;
            }
            claim_and_launch(
                state,
                trigger.clone(),
                format!("event:{}", event.id),
                None,
                None,
            )
            .await;
            let _ = state
                .repository
                .set_trigger_cursor(trigger.id, event.id.as_str());
        }
        if let Some(cursor) = &current_cursor {
            let _ = state.repository.set_trigger_cursor(trigger.id, cursor);
        }
    }
}

async fn claim_and_launch(
    state: &Arc<AppState>,
    trigger: AutomationTrigger,
    dedupe_key: String,
    next_run_at: Option<i64>,
    parent_id: Option<WorkflowId>,
) {
    let now = now_millis().unwrap_or_default();
    match state
        .repository
        .claim_automatic_trigger_firing(trigger.id, &dedupe_key, now, next_run_at)
    {
        Ok(true) => {
            if let Err(error) = launch_trigger(state, &trigger, parent_id).await {
                notify_trigger_error(state, &trigger, &error);
            }
        }
        Ok(false) => {}
        Err(error) => {
            tracing::error!(trigger_id = %trigger.id, error = %error, "trigger claim failed");
            notify_trigger_error(state, &trigger, "trigger firing could not be claimed");
        }
    }
}

fn notify_trigger_error(state: &AppState, trigger: &AutomationTrigger, error: &str) {
    let _ = state.repository.set_trigger_error(trigger.id, Some(error));
    state.notify(Notification {
        id: EntityId(format!("trigger:{}:failed", trigger.id)),
        level: NotificationLevel::Error,
        title: "Automatic trigger failed".to_owned(),
        message: format!("{}: {error}", trigger.name),
        created_at_ms: now_millis().unwrap_or_default(),
    });
}

async fn launch_trigger(
    state: &Arc<AppState>,
    trigger: &AutomationTrigger,
    parent_id: Option<WorkflowId>,
) -> Result<(), String> {
    match trigger.target.operation_class {
        TriggerTargetClass::Workflow => state
            .catalogue
            .create_workflow_with_parent(
                &state.repository,
                &trigger.target.kind,
                trigger.target.parameters.clone(),
                parent_id,
            )
            .map(drop)
            .map_err(|error| error.to_string()),
        TriggerTargetClass::Action => {
            // Spawned, not awaited: a triggered action can run for hours, and
            // awaiting it here stalled evaluation of every other schedule and
            // event trigger until it finished.
            let started_at = now_millis().map_err(|error| error.message)?;
            let execution = state
                .repository
                .begin_finite_execution(
                    FiniteExecutionClass::Action,
                    &trigger.target.kind,
                    started_at,
                )
                .map_err(|error| error.to_string())?;
            let kind = trigger.target.kind.clone();
            let parameters = trigger.target.parameters.clone();
            spawn_action(state.clone(), execution.id, kind, parameters);
            Ok(())
        }
    }
}

fn publish_workflow_updates(
    state: &AppState,
    revisions: &mut BTreeMap<String, u64>,
    activity_cursor: &mut i64,
) {
    if let Ok(current) = state.repository.list_summaries() {
        let mut present = BTreeSet::new();
        for workflow in current {
            present.insert(workflow.id.to_string());
            let delta = match revisions.insert(workflow.id.to_string(), workflow.revision) {
                None => Some(LiveDelta::WorkflowCreated(stored_summary(&workflow))),
                Some(revision) if revision != workflow.revision => {
                    Some(LiveDelta::WorkflowUpdated(stored_summary(&workflow)))
                }
                _ => None,
            };
            if let Some(delta) = delta {
                state.publish(delta);
                for slice in [
                    DomainSlice::Entities,
                    DomainSlice::Workflows,
                    DomainSlice::Overview,
                    DomainSlice::Devices,
                    DomainSlice::Autofactories,
                    DomainSlice::Cargo,
                    DomainSlice::Missions,
                ] {
                    state.invalidate(slice);
                }
                if let Ok(Some(instance)) = state.repository.read(workflow.id)
                    && let Some(notification) = workflow_notification(&instance)
                {
                    state.notify(notification)
                }
            }
        }
        revisions.retain(|id, _| present.contains(id));
    }
    if let Ok(activity) = state.repository.activity_since(*activity_cursor) {
        for record in activity {
            *activity_cursor = record.id;
            if let Ok(id) = u64::try_from(record.id) {
                let (level, step, message) = present_activity(&record.message);
                state.publish(LiveDelta::WorkflowActivity(WorkflowActivity {
                    id,
                    workflow_id: ProtocolWorkflowId(record.workflow_id.to_string()),
                    occurred_at_ms: record.created_at,
                    level,
                    step,
                    message,
                }));
                state.invalidate(DomainSlice::Overview);
            }
        }
    }
}

fn operation_status(status: ManagedOperationStatus) -> OperationStatus {
    match status {
        ManagedOperationStatus::Prepared => OperationStatus::Pending,
        ManagedOperationStatus::Submitted
        | ManagedOperationStatus::Accepted
        | ManagedOperationStatus::InProgress
        | ManagedOperationStatus::AwaitingEvidence => OperationStatus::Running,
        ManagedOperationStatus::Completed => OperationStatus::Succeeded,
        ManagedOperationStatus::ReconciliationRequired | ManagedOperationStatus::Ambiguous => {
            OperationStatus::Ambiguous
        }
        ManagedOperationStatus::Cancelled
        | ManagedOperationStatus::Rejected
        | ManagedOperationStatus::Failed => OperationStatus::Failed,
        // Deliberately not a wildcard: the managed crate marks this enum
        // non-exhaustive, and a silent catch-all would mislabel any status
        // added upstream rather than failing the build here.
        status => {
            tracing::warn!(?status, "unmapped managed operation status");
            OperationStatus::Ambiguous
        }
    }
}

async fn websocket(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(move |socket| live_connection(socket, state))
}

async fn live_connection(mut socket: WebSocket, state: Arc<AppState>) {
    let mut updates = state.live.subscribe();
    let Ok(initial) = state.resnapshot_message() else {
        return;
    };
    let initial_revision = initial.revision;
    if send_live(&mut socket, initial).await.is_err() {
        return;
    }

    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.tick().await;
    let mut last_pong = Instant::now();
    loop {
        tokio::select! {
            update = updates.recv() => match update {
                // Messages at or below the revision the client already has
                // from its HTTP snapshot are redundant.
                Ok(update) if update.revision <= initial_revision => {}
                Ok(update) => if send_live(&mut socket, update).await.is_err() { break; },
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    // Keep the connection: the client recovers by comparing the
                    // slice revisions in a fresh snapshot against what it has
                    // loaded. Closing here forced a full reconnect during
                    // exactly the bursts that caused the lag.
                    tracing::debug!(skipped, "live subscriber lagged; sending resnapshot marker");
                    if let Ok(message) = state.resnapshot_message()
                        && send_live(&mut socket, message).await.is_err()
                    {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Pong(_))) => last_pong = Instant::now(),
                Some(Ok(Message::Ping(payload))) => {
                    if socket.send(Message::Pong(payload)).await.is_err() { break; }
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                Some(Ok(_)) => {}
            },
            _ = heartbeat.tick() => {
                if last_pong.elapsed() > HEARTBEAT_TIMEOUT
                    || socket.send(Message::Ping(Default::default())).await.is_err()
                {
                    break;
                }
            }
        }
    }
}

async fn send_live(socket: &mut WebSocket, message: LiveMessage) -> Result<(), ()> {
    let text = serde_json::to_string(&message).map_err(|_| ())?;
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ())
}

async fn health(State(state): State<Arc<AppState>>) -> Json<Versioned<DaemonHealth>> {
    let status = state.client().status();
    Json(Versioned::current(DaemonHealth {
        status: health_status(&status),
        daemon_version: env!("CARGO_PKG_VERSION").to_owned(),
        detail: status_detail(&status).map(str::to_owned),
    }))
}

async fn snapshot(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<RuntimeSnapshot>>, ApiError> {
    let revision = state
        .client()
        .state()
        .revision()
        .map_err(|_| ApiError::unavailable())?;
    let instances = state.repository.list().map_err(ApiError::repository)?;
    let requirements = instances
        .iter()
        .filter(|instance| instance.kind.as_str() == "requirement.fulfillment")
        .filter_map(requirement_summary)
        .collect();
    let workflows = instances.iter().map(summary).collect();
    let status = state.client().status();
    let triggers = state
        .repository
        .list_triggers()
        .map_err(ApiError::repository)?;
    Ok(Json(Versioned::current(RuntimeSnapshot {
        metadata: state.snapshot_metadata()?,
        sync: RuntimeSyncStatus {
            revision,
            ..runtime_sync_status(&state, &status)
        },
        automation: automation_status(
            state
                .repository
                .automation_policy()
                .map_err(ApiError::repository)?,
        ),
        workflows,
        requirements,
        notifications: operational_notifications(&instances, &triggers, &status),
        slice_revisions: state.slice_revisions(),
    })))
}

async fn overview(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<OverviewSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let status = state.client().status();
    let workflows = state.repository.list().map_err(ApiError::repository)?;
    let triggers = state
        .repository
        .list_triggers()
        .map_err(ApiError::repository)?;
    let automation = automation_status(
        state
            .repository
            .automation_policy()
            .map_err(ApiError::repository)?,
    );
    let locations = state
        .client()
        .locations()
        .find()
        .collect()
        .await
        .map_err(|_| ApiError::unavailable())?;
    let location_systems = locations
        .into_iter()
        .map(|location| (location.id().to_string(), location.system))
        .collect::<BTreeMap<_, _>>();
    let mut replicants = Vec::new();
    let mut active_travel = Vec::new();
    for handle in state
        .client()
        .replicants()
        .find()
        .owned()
        .collect()
        .await
        .map_err(|_| ApiError::unavailable())?
    {
        let replicant = handle
            .snapshot()
            .await
            .map_err(|_| ApiError::unavailable())?;
        let entity = summary_ref(EntityKind::Replicant, replicant.key.id.to_string());
        let location = replicant.location.map(|value| value.id.to_string());
        if let Some(travel) = replicant.travel {
            active_travel.push(OverviewTravel {
                entity: entity.clone(),
                from: travel.origin.map(|value| value.id.to_string()),
                to: travel
                    .final_destination
                    .or(travel.destination)
                    .map(|value| value.id.to_string()),
                arrives_at: travel.final_arrives_at.or(travel.arrives_at),
            });
        }
        replicants.push(OverviewReplicant {
            entity,
            name: replicant.name,
            system: location
                .as_ref()
                .and_then(|value| location_systems.get(value).cloned().flatten()),
            location,
            status: wire_value(replicant.status.as_ref()),
        });
    }
    replicants.sort_by(|left, right| left.entity.cmp(&right.entity));
    active_travel.sort_by(|left, right| left.entity.cmp(&right.entity));

    let workflow_rows = workflows
        .iter()
        .map(|instance| {
            (
                summary(instance),
                instance.last_error.is_some() || instance.status == WorkflowStatus::Failed,
            )
        })
        .collect();
    let mut recent_activity = state
        .repository
        .activity_since(0)
        .map_err(ApiError::repository)?
        .into_iter()
        .rev()
        .take(12)
        .map(protocol_activity)
        .collect::<Result<Vec<_>, _>>()?;
    recent_activity.sort_by_key(|activity| std::cmp::Reverse(activity.id));
    Ok(Json(Versioned::current(build_overview_snapshot(
        metadata,
        DaemonHealth {
            status: health_status(&status),
            daemon_version: env!("CARGO_PKG_VERSION").to_owned(),
            detail: status_detail(&status).map(str::to_owned),
        },
        runtime_sync_status(&state, &status),
        automation,
        replicants,
        active_travel,
        workflow_rows,
        operational_notifications(&workflows, &triggers, &status),
        recent_activity,
    ))))
}

#[allow(clippy::too_many_arguments)]
fn build_overview_snapshot(
    metadata: SnapshotMetadata,
    health: DaemonHealth,
    sync: RuntimeSyncStatus,
    automation: AutomationStatus,
    replicants: Vec<OverviewReplicant>,
    active_travel: Vec<OverviewTravel>,
    workflows: Vec<(WorkflowSummary, bool)>,
    notifications: Vec<Notification>,
    recent_activity: Vec<WorkflowActivity>,
) -> OverviewSnapshot {
    let statuses = [
        ProtocolStatus::Queued,
        ProtocolStatus::Running,
        ProtocolStatus::Waiting,
        ProtocolStatus::Paused,
        ProtocolStatus::Reconciling,
        ProtocolStatus::Succeeded,
        ProtocolStatus::Failed,
        ProtocolStatus::Cancelled,
    ];
    let workflow_counts = statuses
        .into_iter()
        .filter_map(|status| {
            let count = workflows
                .iter()
                .filter(|(workflow, _)| workflow.status == status)
                .count();
            (count > 0).then_some(WorkflowStatusCount { status, count })
        })
        .collect();
    let active_workflows = workflows
        .iter()
        .filter(|(workflow, _)| {
            !matches!(
                workflow.status,
                ProtocolStatus::Succeeded | ProtocolStatus::Failed | ProtocolStatus::Cancelled
            )
        })
        .map(|(workflow, _)| workflow.clone())
        .collect();
    let attention_workflows = workflows
        .into_iter()
        .filter_map(|(workflow, attention)| attention.then_some(workflow))
        .collect();
    OverviewSnapshot {
        metadata,
        health,
        sync,
        automation,
        replicants,
        active_travel,
        active_workflows,
        workflow_counts,
        attention_workflows,
        notifications,
        recent_activity,
    }
}

async fn devices(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<DevicesSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let devices = device_rows(&state).await?.as_ref().clone();
    Ok(Json(Versioned::current(DevicesSnapshot {
        metadata,
        devices,
    })))
}

/// Returns device rows for the current managed revision, building them only
/// when that revision has moved.
///
/// Holding the lock across the build also collapses concurrent callers into
/// one upstream pass instead of three identical ones.
async fn device_rows(state: &Arc<AppState>) -> Result<Arc<Vec<DeviceSummary>>, ApiError> {
    let revision = state
        .client()
        .state()
        .revision()
        .map_err(|_| ApiError::unavailable())?;
    let mut cached = state.device_rows.lock().await;
    if let Some((cached_revision, rows)) = cached.as_ref()
        && *cached_revision == revision
    {
        return Ok(rows.clone());
    }
    let rows = Arc::new(build_device_rows(state).await?);
    *cached = Some((revision, rows.clone()));
    Ok(rows)
}

async fn build_device_rows(state: &Arc<AppState>) -> Result<Vec<DeviceSummary>, ApiError> {
    let system_regions = expanded_system_region_map(&state.client().galaxy().catalogue());
    let location_systems = state
        .client()
        .locations()
        .find()
        .collect()
        .await
        .map_err(|_| ApiError::unavailable())?
        .into_iter()
        .map(|location| (location.id().to_string(), location.system))
        .collect::<BTreeMap<_, _>>();
    let mut replicant_names = BTreeMap::new();
    for handle in state
        .client()
        .replicants()
        .find()
        .owned()
        .collect()
        .await
        .map_err(|_| ApiError::unavailable())?
    {
        let replicant = handle
            .snapshot()
            .await
            .map_err(|_| ApiError::unavailable())?;
        if let Some(name) = replicant.name {
            replicant_names.insert(replicant.key.id.to_string(), name);
        }
    }
    let workflows = state.repository.list().map_err(ApiError::repository)?;
    let mut claims = BTreeMap::new();
    for workflow in &workflows {
        for claim in state
            .repository
            .claims(workflow.id)
            .map_err(ApiError::repository)?
        {
            if let ResourceKey::Device(code) = claim.resource {
                let workflow = summary(workflow);
                claims.insert(
                    code,
                    DeviceClaim {
                        workflow_id: workflow.id,
                        workflow_kind: workflow.kind,
                        workflow_status: workflow.status,
                    },
                );
            }
        }
    }
    let mut rows = Vec::new();
    for handle in state
        .client()
        .devices()
        .find()
        .collect()
        .await
        .map_err(|_| ApiError::unavailable())?
    {
        let device = handle
            .snapshot()
            .await
            .map_err(|_| ApiError::unavailable())?;
        rows.push(device_summary(
            device,
            &location_systems,
            &system_regions,
            &replicant_names,
            claims.remove(handle.id().as_str()),
        )?);
    }
    inherit_stowed_locations(&mut rows);
    rows.sort_by(|left, right| left.entity.cmp(&right.entity));
    Ok(rows)
}

fn inherit_stowed_locations(devices: &mut [DeviceSummary]) {
    for _ in 0..devices.len() {
        let hosts = devices
            .iter()
            .filter_map(|device| {
                Some((
                    device.entity.id.0.clone(),
                    (
                        device.location.clone()?,
                        device.system.clone(),
                        device.region.clone(),
                    ),
                ))
            })
            .collect::<BTreeMap<_, _>>();
        let mut changed = false;
        for device in devices
            .iter_mut()
            .filter(|device| device.location.is_none())
        {
            if let Some((location, system, region)) =
                device.stowed_in.as_ref().and_then(|host| hosts.get(host))
            {
                device.location = Some(location.clone());
                device.system.clone_from(system);
                device.region.clone_from(region);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
}

/// Reads full device status in paginated list calls instead of issuing one
/// upstream GET per device. Managed state intentionally stores a smaller
/// normalized device model, while cargo and factory queue details still live
/// only on the raw status payload.
async fn raw_device_details(
    client: &Client,
    device_type: Option<&str>,
) -> Result<BTreeMap<String, RawDeviceStatus>, ApiError> {
    let mut cursor = None;
    let mut details = BTreeMap::new();
    loop {
        let response = client
            .raw()
            .devices()
            .list(&DeviceListQuery {
                device_type: device_type.map(str::to_owned),
                cursor,
                limit: Some(50),
                ..DeviceListQuery::default()
            })
            .await
            .map_err(|_| ApiError::unavailable())?
            .value;
        for detail in response.devices {
            if let Some(code) = detail.device_code.clone() {
                details.insert(code, detail);
            }
        }
        let Some(next) = response.next_cursor else {
            break;
        };
        if cursor == Some(next) {
            break;
        }
        cursor = Some(next);
    }
    Ok(details)
}

async fn autofactories(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<AutofactorySnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let device_rows = device_rows(&state).await?;
    let factory_devices = device_rows
        .iter()
        .filter(|device| device.device_type.as_deref() == Some("autofactory"))
        .cloned()
        .collect::<Vec<_>>();
    if factory_devices.is_empty() {
        return Ok(Json(Versioned::current(autofactory_snapshot(
            metadata,
            Vec::new(),
        ))));
    }
    let mut details = raw_device_details(state.client(), Some("autofactory")).await?;
    let mut factories = Vec::with_capacity(factory_devices.len());
    for device in factory_devices {
        let detail = if let Some(detail) = details.remove(&device.entity.id.0) {
            detail
        } else {
            state
                .client()
                .raw()
                .devices()
                .get(&device.entity.id.0)
                .await
                .map_err(|_| ApiError::unavailable())?
                .value
        };
        let current_job = detail.printing.map(|printing| FactoryJobSummary {
            device_type: printing.device_type.unwrap_or_else(|| "unknown".to_owned()),
            quantity: 1,
            eta_seconds: printing.eta_seconds,
            tags: printing.tags,
        });
        let queued_jobs = detail
            .print_queue
            .iter()
            .map(factory_job_from_queue)
            .collect::<Vec<_>>();
        let queued_units = queued_jobs.iter().map(|job| job.quantity).sum();
        let unavailable = device.status.as_deref().is_some_and(|status| {
            matches!(
                status.to_ascii_lowercase().as_str(),
                "compacted" | "compacting" | "unfurling"
            )
        });
        let availability = if unavailable {
            AutofactoryAvailability::Unavailable
        } else if current_job.is_some() || !queued_jobs.is_empty() {
            AutofactoryAvailability::Busy
        } else {
            AutofactoryAvailability::Available
        };
        factories.push(AutofactorySummary {
            device,
            availability,
            queue_capacity: detail.queue_size,
            queued_units,
            current_job,
            queued_jobs,
        });
    }
    Ok(Json(Versioned::current(autofactory_snapshot(
        metadata, factories,
    ))))
}

fn autofactory_snapshot(
    metadata: SnapshotMetadata,
    factories: Vec<AutofactorySummary>,
) -> AutofactorySnapshot {
    let busy = factories
        .iter()
        .filter(|factory| factory.availability == AutofactoryAvailability::Busy)
        .count();
    let available = factories
        .iter()
        .filter(|factory| factory.availability == AutofactoryAvailability::Available)
        .count();
    let unavailable = factories.len().saturating_sub(busy + available);
    let printable = busy + available;
    AutofactorySnapshot {
        metadata,
        utilization: AutofactoryUtilization {
            total: factories.len(),
            busy,
            available,
            unavailable,
            queued_units: factories.iter().map(|factory| factory.queued_units).sum(),
            utilization_percent: if printable == 0 {
                0.0
            } else {
                busy as f64 * 100.0 / printable as f64
            },
        },
        factories,
    }
}

fn factory_job_from_queue(value: &Map<String, Value>) -> FactoryJobSummary {
    FactoryJobSummary {
        device_type: string_field(value, &["device_type", "type"])
            .unwrap_or("unknown")
            .to_owned(),
        quantity: integer_field(value, &["quantity", "count"])
            .unwrap_or(1)
            .max(1),
        eta_seconds: number_field(value, &["eta_seconds", "remaining_seconds"]),
        tags: value
            .get("tags")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
    }
}

fn string_field<'a>(value: &'a Map<String, Value>, names: &[&str]) -> Option<&'a str> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_str))
}

fn integer_field(value: &Map<String, Value>, names: &[&str]) -> Option<i64> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_i64))
}

fn number_field(value: &Map<String, Value>, names: &[&str]) -> Option<f64> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(Value::as_f64))
}

async fn cargo(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<CargoSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let carriers = device_rows(&state)
        .await?
        .iter()
        .filter(|device| {
            device.cargo_capacity.unwrap_or_default() > 0
                || !device.cargo.is_empty()
                || device.attach_capacity.unwrap_or_default() > 0
                || !device.attached_devices.is_empty()
        })
        .map(|device| {
            Ok(CargoCarrierSummary {
                attachment_used: i64::try_from(device.attached_devices.len())
                    .map_err(|_| ApiError::unavailable())?,
                resources: device.cargo.clone(),
                device: device.clone(),
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(Json(Versioned::current(cargo_snapshot(metadata, carriers))))
}

fn cargo_snapshot(metadata: SnapshotMetadata, carriers: Vec<CargoCarrierSummary>) -> CargoSnapshot {
    CargoSnapshot {
        metadata,
        cargo_used: carriers
            .iter()
            .filter_map(|carrier| carrier.device.cargo_used)
            .sum(),
        cargo_capacity: carriers
            .iter()
            .filter_map(|carrier| carrier.device.cargo_capacity)
            .sum(),
        attachment_used: carriers.iter().map(|carrier| carrier.attachment_used).sum(),
        attachment_capacity: carriers
            .iter()
            .filter_map(|carrier| carrier.device.attach_capacity)
            .sum(),
        carriers,
    }
}

fn active_workflow(status: WorkflowStatus) -> bool {
    matches!(
        status,
        WorkflowStatus::Queued
            | WorkflowStatus::Running
            | WorkflowStatus::Waiting
            | WorkflowStatus::Paused
            | WorkflowStatus::Reconciling
    )
}

async fn survey_missions(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<SurveySnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let devices = device_rows(&state).await?;
    let devices = devices.as_ref();
    let mut fleet_codes = std::collections::BTreeSet::new();
    let mut missions = Vec::new();
    for workflow in state
        .repository
        .list_active()
        .map_err(ApiError::repository)?
    {
        match workflow.kind.as_str() {
            "survey.route" => {
                let config = workflow
                    .config::<SurveyWorkflowConfig>()
                    .map_err(ApiError::repository)?;
                let checkpoint = workflow
                    .checkpoint::<SurveyWorkflowCheckpoint>()
                    .map_err(ApiError::repository)?;
                let plan = checkpoint.state.as_ref().map(summarize_plan);
                let controller = plan
                    .as_ref()
                    .and_then(|plan| plan.controller.clone())
                    .or_else(|| config.options.controller.clone());
                let drones = plan
                    .as_ref()
                    .map(|plan| plan.drones.clone())
                    .or_else(|| config.options.drones.clone())
                    .unwrap_or_default();
                fleet_codes.insert(config.options.vessel.clone());
                fleet_codes.extend(controller.iter().cloned());
                fleet_codes.extend(drones.iter().cloned());
                missions.push(SurveyMissionSummary {
                    workflow: summary(&workflow),
                    replicant: plan
                        .as_ref()
                        .map(|plan| plan.replicant.clone())
                        .unwrap_or_else(|| config.options.replicant.clone()),
                    vessel: plan
                        .as_ref()
                        .map(|plan| plan.vessel.clone())
                        .unwrap_or_else(|| config.options.vessel.clone()),
                    center: plan
                        .as_ref()
                        .map(|plan| plan.center.clone())
                        .unwrap_or_else(|| config.options.center.clone()),
                    phase: plan
                        .as_ref()
                        .map(|plan| plan.phase.clone())
                        .or_else(|| workflow.current_step.clone())
                        .unwrap_or_else(|| "queued".to_owned()),
                    completed_systems: plan.as_ref().map_or(0, |plan| plan.completed_stops),
                    total_systems: plan.as_ref().map_or(0, |plan| plan.total_stops),
                    next_system: plan.and_then(|plan| plan.next_system),
                    controller,
                    drones,
                });
            }
            "scan.system" | "scan.belt" => {
                let config = workflow
                    .config::<ScanIntent>()
                    .map_err(ApiError::repository)?;
                let checkpoint = workflow
                    .checkpoint::<ControllerWorkflowCheckpoint>()
                    .map_err(ApiError::repository)?;
                let controller = checkpoint.controller.or(config.controller);
                let controller_device = controller.as_ref().and_then(|code| {
                    devices
                        .iter()
                        .find(|device| device.entity.id.0.as_str() == code)
                });
                let drones = controller_device
                    .map(|device| device.controlled_devices.clone())
                    .unwrap_or_default();
                if let Some(controller) = controller.as_ref() {
                    fleet_codes.insert(controller.clone());
                }
                fleet_codes.extend(drones.iter().cloned());
                missions.push(SurveyMissionSummary {
                    workflow: summary(&workflow),
                    replicant: controller_device
                        .and_then(|device| device.owner.clone())
                        .unwrap_or_default(),
                    vessel: String::new(),
                    center: config.system.clone(),
                    phase: workflow
                        .current_step
                        .clone()
                        .unwrap_or_else(|| "queued".to_owned()),
                    completed_systems: 0,
                    total_systems: 1,
                    next_system: Some(config.system),
                    controller,
                    drones,
                });
            }
            "scan.tour" => {
                let config = workflow
                    .config::<ScanTourIntent>()
                    .map_err(ApiError::repository)?;
                let checkpoint = workflow
                    .checkpoint::<ScanTourCheckpoint>()
                    .map_err(ApiError::repository)?;
                let plan = checkpoint.state.as_ref().map(summarize_plan);
                let controller = plan.as_ref().and_then(|plan| plan.controller.clone());
                let drones = plan
                    .as_ref()
                    .map(|plan| plan.drones.clone())
                    .unwrap_or_default();
                let vessel = plan
                    .as_ref()
                    .map(|plan| plan.vessel.clone())
                    .or_else(|| checkpoint.vessel.clone())
                    .or_else(|| config.vessel.clone())
                    .unwrap_or_default();
                if !vessel.is_empty() {
                    fleet_codes.insert(vessel.clone());
                }
                fleet_codes.extend(controller.iter().cloned());
                fleet_codes.extend(drones.iter().cloned());
                missions.push(SurveyMissionSummary {
                    workflow: summary(&workflow),
                    replicant: plan
                        .as_ref()
                        .map(|plan| plan.replicant.clone())
                        .or_else(|| checkpoint.replicant.clone())
                        .or_else(|| config.replicant.clone())
                        .unwrap_or_default(),
                    vessel,
                    center: plan
                        .as_ref()
                        .map(|plan| plan.center.clone())
                        .unwrap_or(config.center),
                    phase: plan
                        .as_ref()
                        .map(|plan| plan.phase.clone())
                        .or_else(|| workflow.current_step.clone())
                        .unwrap_or_else(|| "queued".to_owned()),
                    completed_systems: plan.as_ref().map_or(0, |plan| plan.completed_stops),
                    total_systems: plan.as_ref().map_or(0, |plan| plan.total_stops),
                    next_system: plan.and_then(|plan| plan.next_system),
                    controller,
                    drones,
                });
            }
            _ => {}
        }
    }
    missions.sort_by(|left, right| left.workflow.id.cmp(&right.workflow.id));
    Ok(Json(Versioned::current(SurveySnapshot {
        metadata,
        missions,
        fleet: devices
            .iter()
            .filter(|device| fleet_codes.contains(&device.entity.id.0))
            .cloned()
            .collect(),
    })))
}

async fn mining_missions(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<MiningSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let workflows = state
        .repository
        .list_active()
        .map_err(ApiError::repository)?
        .into_iter()
        .filter(|workflow| {
            workflow.kind.as_str().contains("mining")
                || workflow.kind.as_str() == "salvage.site"
                || workflow
                    .current_step
                    .as_deref()
                    .is_some_and(|step| step.contains("mining"))
        })
        .map(|workflow| summary(&workflow))
        .collect();
    Ok(Json(Versioned::current(MiningSnapshot {
        metadata,
        installations: mining_installations(device_rows(&state).await?.as_ref().clone()),
        workflows,
    })))
}

async fn relay_missions(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<RelaySnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let workflows = state
        .repository
        .list_active()
        .map_err(ApiError::repository)?;
    let devices = device_rows(&state).await?.as_ref().clone();
    Ok(Json(Versioned::current(relay_snapshot(
        metadata,
        devices,
        state.client().galaxy().catalogue(),
        &workflows,
    )?)))
}

fn relay_snapshot(
    metadata: SnapshotMetadata,
    devices: Vec<DeviceSummary>,
    stars: Vec<replicant_client::Star>,
    workflows: &[WorkflowInstance],
) -> Result<RelaySnapshot, ApiError> {
    const RELAY_TYPES: [&str; 3] = ["ftl_relay", "system_hub", "deep_space_relay_station"];
    const RELAY_RANGE_LY: f64 = 7.499;
    let relay_capable = |device: &&DeviceSummary| {
        device
            .device_type
            .as_deref()
            .is_some_and(|kind| RELAY_TYPES.contains(&kind))
    };
    let deployed = |device: &&DeviceSummary| {
        relay_capable(device)
            && device.ownership == "owned"
            && device.attached_to.is_none()
            && device.stowed_in.is_none()
            && device.system.is_some()
            && device
                .status
                .as_deref()
                .is_some_and(|status| matches!(status, "active" | "relaying"))
    };
    let relays = devices.iter().filter(deployed).cloned().collect::<Vec<_>>();
    let connected = relays
        .iter()
        .filter_map(|device| device.system.clone())
        .collect::<BTreeSet<_>>();
    let positions = stars
        .into_iter()
        .filter_map(|star| {
            star.position
                .map(|position| (star.key.id.to_string(), position))
        })
        .collect::<BTreeMap<_, _>>();
    let relay_nodes = connected
        .iter()
        .filter(|system| positions.contains_key(*system))
        .collect::<Vec<_>>();
    let mut relay_edges = Vec::new();
    for (index, from) in relay_nodes.iter().enumerate() {
        for to in relay_nodes.iter().skip(index + 1) {
            let from_position = positions[*from];
            let to_position = positions[*to];
            let dx = from_position.x - to_position.x;
            let dy = from_position.y - to_position.y;
            let dz = from_position.z - to_position.z;
            if dx * dx + dy * dy + dz * dz <= RELAY_RANGE_LY * RELAY_RANGE_LY {
                relay_edges.push(replicant_protocol::GalaxyEdge {
                    from: (*from).clone(),
                    to: (*to).clone(),
                });
            }
        }
    }
    let deployed_codes = relays
        .iter()
        .map(|device| device.entity.id.0.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let staged_relays = devices
        .iter()
        .filter(relay_capable)
        .filter(|device| !deployed_codes.contains(device.entity.id.0.as_str()))
        .filter(|device| {
            device.tags.iter().any(|tag| tag.starts_with("relay-m:"))
                || device.claim.as_ref().is_some_and(|claim| {
                    matches!(
                        claim.workflow_kind.0.as_str(),
                        "relay.expansion" | "exploration.frontier"
                    )
                })
        })
        .cloned()
        .collect();
    let mut expansions = Vec::new();
    for workflow in workflows
        .iter()
        .filter(|workflow| active_workflow(workflow.status))
    {
        match workflow.kind.as_str() {
            "relay.expansion" => {
                let config = workflow
                    .config::<RelayWorkflowConfig>()
                    .map_err(ApiError::repository)?;
                let checkpoint = workflow
                    .checkpoint::<RelayWorkflowCheckpoint>()
                    .map_err(ApiError::repository)?;
                let status = checkpoint.state.as_ref().map(|state| state.status());
                expansions.push(RelayExpansionSummary {
                    workflow: summary(workflow),
                    replicant: config.request.replicant,
                    hub: config.request.hub,
                    targets: config.request.targets.clone(),
                    phase: status
                        .as_ref()
                        .and_then(|status| wire_value(Some(&status.phase)))
                        .or_else(|| workflow.current_step.clone())
                        .unwrap_or_else(|| "queued".to_owned()),
                    completed_stops: status.as_ref().map_or(0, |status| status.completed_stops),
                    total_stops: status.as_ref().map(|status| status.total_stops),
                    next_system: status
                        .as_ref()
                        .and_then(|status| status.next_system.clone())
                        .or_else(|| config.request.targets.first().cloned()),
                    pending_relays: status.map(|status| status.pending_relays),
                });
            }
            "exploration.frontier" => {
                let config = workflow
                    .config::<ExplorationIntent>()
                    .map_err(ApiError::repository)?;
                let checkpoint = workflow
                    .checkpoint::<ExplorationWorkflowCheckpoint>()
                    .map_err(ApiError::repository)?;
                let status = checkpoint.state.as_ref().map(|state| state.status());
                expansions.push(RelayExpansionSummary {
                    workflow: summary(workflow),
                    replicant: checkpoint
                        .replicant
                        .clone()
                        .or(config.replicant.clone())
                        .unwrap_or_default(),
                    hub: checkpoint
                        .hub
                        .clone()
                        .or(config.hub.clone())
                        .unwrap_or_default(),
                    targets: vec![config.target.clone()],
                    phase: status
                        .as_ref()
                        .and_then(|status| wire_value(Some(&status.phase)))
                        .or_else(|| workflow.current_step.clone())
                        .unwrap_or_else(|| "queued".to_owned()),
                    completed_stops: status.as_ref().map_or(0, |status| status.completed_stops),
                    total_stops: status.as_ref().map(|status| status.total_stops),
                    next_system: status
                        .as_ref()
                        .and_then(|status| status.next_system.clone())
                        .or(Some(config.target.clone())),
                    pending_relays: status.map(|status| status.pending_relays),
                });
            }
            _ => {}
        }
    }
    expansions.sort_by(|left, right| left.workflow.id.cmp(&right.workflow.id));
    Ok(RelaySnapshot {
        metadata,
        relays,
        staged_relays,
        connected_systems: connected.len(),
        relay_edges,
        expansions,
    })
}

async fn bootstrap_missions(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<BootstrapSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let missions = bootstrap_mission_summaries(
        state
            .repository
            .finite_execution_history()
            .map_err(ApiError::repository)?,
    );
    Ok(Json(Versioned::current(BootstrapSnapshot {
        metadata,
        missions,
    })))
}

async fn events(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<EventsSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let events = discovered_events(state.client())
        .await
        .map_err(|_| ApiError::unavailable())?;
    Ok(Json(Versioned::current(events_snapshot(metadata, events)?)))
}

fn events_snapshot(
    metadata: SnapshotMetadata,
    events: Vec<LocationEvent>,
) -> Result<EventsSnapshot, ApiError> {
    let mut events = events
        .into_iter()
        .map(|event| event_summary(&event))
        .collect::<Result<Vec<_>, _>>()?;
    events.sort_by(|left, right| {
        left.status
            .cmp(&right.status)
            .then_with(|| left.system.cmp(&right.system))
            .then_with(|| left.title.cmp(&right.title))
            .then_with(|| left.designation.cmp(&right.designation))
    });
    Ok(EventsSnapshot { metadata, events })
}

fn event_summary(raw: &LocationEvent) -> Result<EventSummary, ApiError> {
    let event = normalize_event(raw).map_err(|_| ApiError::unavailable())?;
    let criteria = event
        .criteria
        .iter()
        .map(|criterion| {
            let remaining = remaining_requirements(&event, &criterion.name, &BTreeMap::new(), &[])
                .map_err(|_| ApiError::unavailable())?;
            let remaining_devices = remaining
                .devices
                .iter()
                .map(|item| (item.device_type.as_str(), item.count))
                .collect::<BTreeMap<_, _>>();
            let mut requirements = criterion
                .resources
                .iter()
                .map(|(item, required)| {
                    let outstanding = *remaining.resources.get(item).unwrap_or(&0);
                    EventRequirementSummary {
                        kind: EventRequirementKind::Resource,
                        item: item.clone(),
                        required: *required,
                        completed: required.saturating_sub(outstanding),
                        remaining: outstanding,
                    }
                })
                .collect::<Vec<_>>();
            requirements.extend(criterion.devices.iter().map(|item| {
                let outstanding = *remaining_devices
                    .get(item.device_type.as_str())
                    .unwrap_or(&0);
                EventRequirementSummary {
                    kind: EventRequirementKind::Device,
                    item: item.device_type.clone(),
                    required: item.count,
                    completed: item.count.saturating_sub(outstanding),
                    remaining: outstanding,
                }
            }));
            Ok(EventCriterionSummary {
                complete: !requirements.is_empty()
                    && requirements.iter().all(|item| item.remaining == 0),
                name: criterion.name.clone(),
                requirements,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    let category = raw
        .extra
        .get("category")
        .or_else(|| raw.extra.get("event_category"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    Ok(EventSummary {
        designation: event.designation,
        title: event.title,
        event_type: event.event_type,
        category,
        tier: event.tier,
        system: event
            .location
            .split('-')
            .next()
            .unwrap_or(&event.location)
            .to_ascii_uppercase(),
        location: event.location,
        description: event.description,
        criteria,
        rewards: EventRewardsSummary {
            resources: event
                .rewards
                .resources
                .into_iter()
                .map(|(item, quantity)| EventRewardItem { item, quantity })
                .collect(),
            devices: Vec::new(),
            xp: event.rewards.xp,
            civilisation_points: event.rewards.civilisation_points,
            completion_achievement: event.rewards.completion_achievement,
        },
        status: event.status,
        discovered_at: raw.discovered_at.clone(),
        completed_at: raw.completed_at.clone(),
    })
}

#[derive(Default, Deserialize)]
struct ActivityQuery {
    device: Option<String>,
    name: Option<String>,
    ami_only: Option<bool>,
    limit: Option<usize>,
}

async fn account_activity(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ActivityQuery>,
) -> Result<Json<Versioned<AccountEventsSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let mut history = state.client().events().history();
    if let Some(device) = query
        .device
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        history = history.for_device(device.trim());
    }
    if let Some(name) = query
        .name
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        history = history.named(name.trim());
    }
    let limit = query.limit.unwrap_or(200).clamp(1, 1_000);
    // AMI digest is a prefix/suffix predicate rather than one exact event
    // name, so modestly over-read recent local history before filtering.
    let history_limit = if query.ami_only.unwrap_or(false) {
        limit.saturating_mul(20).min(5_000)
    } else {
        limit
    };
    let mut events = history
        .latest(history_limit)
        .collect()
        .await
        .map_err(|_| ApiError::unavailable())?;
    if query.ami_only.unwrap_or(false) {
        events.retain(|event| {
            event.name.as_str().starts_with("ami.") && event.name.as_str().ends_with(".digest")
        });
    }
    events.reverse();
    events.truncate(limit);
    let events = events
        .into_iter()
        .map(|event| AccountEventSummary {
            id: event.id.as_str().to_owned(),
            name: event.name.as_str().to_owned(),
            category: event.category.as_str().to_owned(),
            device: event
                .device
                .map(|key| summary_ref(EntityKind::Device, key.id.as_str())),
            replicant: event
                .replicant
                .map(|key| summary_ref(EntityKind::Replicant, key.id.as_str())),
            system: event.star.map(|key| key.id.as_str().to_owned()),
            location: event.location.map(|key| key.id.as_str().to_owned()),
            occurred_at: event.occurred_at,
            payload: Value::Object(event.payload.into_iter().collect()),
            ami_digest: event.name.as_str().starts_with("ami.")
                && event.name.as_str().ends_with(".digest"),
        })
        .collect();
    Ok(Json(Versioned::current(AccountEventsSnapshot {
        metadata,
        cursor: state
            .client()
            .events()
            .cursor()
            .map_err(|_| ApiError::unavailable())?,
        events,
    })))
}

#[derive(Default, Deserialize)]
struct DeviceLogsQueryParams {
    cursor: Option<i64>,
    limit: Option<i64>,
    latest: Option<bool>,
}

async fn device_logs(
    State(state): State<Arc<AppState>>,
    Path(code): Path<String>,
    Query(query): Query<DeviceLogsQueryParams>,
) -> Result<Json<Versioned<DeviceLogsSnapshot>>, ApiError> {
    // Device logs are an account-owned endpoint upstream. Consult the managed
    // projection first so inspecting a public/foreign device cannot waste an
    // API request only to turn a predictable 403 into a misleading daemon 503.
    let handle = state
        .client()
        .devices()
        .cached(&code)
        .ok_or(ApiError::device_not_found())?;
    let snapshot = handle
        .snapshot()
        .await
        .map_err(|_| ApiError::unavailable())?;
    if snapshot.access != AccessScope::Owned {
        return Err(ApiError::forbidden_device_logs());
    }
    let response = handle
        .logs(&replicant_client::raw::devices::DeviceLogsQuery {
            cursor: query.cursor,
            limit: query.limit.or(Some(100)),
            latest: query.latest,
        })
        .await
        .map_err(|_| ApiError::unavailable())?;
    Ok(Json(Versioned::current(DeviceLogsSnapshot {
        metadata: state.snapshot_metadata()?,
        device: summary_ref(EntityKind::Device, code),
        events: response
            .events
            .into_iter()
            .map(|event| DeviceLogSummary {
                id: event.id,
                created_at: event.created_at,
                device_code: event.device_code,
                device_type: event.device_type,
                event_type: event.event_type,
                message: event.message,
                payload: Value::Object(event.payload.unwrap_or_default()),
            })
            .collect(),
        next_cursor: response.next_cursor,
    })))
}

async fn simulations(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<SimulationsSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let devices = device_rows(&state).await?;
    let simulator_devices = devices
        .iter()
        .filter(|device| device.device_type.as_deref() == Some("replicant_interface"))
        .cloned()
        .collect::<Vec<_>>();
    let gateway = state.client().simulations();
    let mut interfaces = Vec::with_capacity(simulator_devices.len());
    for device in simulator_devices {
        let code = device.entity.id.0.clone();
        match (gateway.scenarios(&code).await, gateway.active(&code).await) {
            (Ok(scenarios), Ok(active)) => interfaces.push(SimulationInterfaceSummary {
                device,
                scenarios: scenarios
                    .scenarios
                    .into_iter()
                    .filter_map(|scenario| {
                        scenario.code.map(|code| SimulationScenarioSummary {
                            code,
                            name: scenario.name,
                            description: scenario.description,
                            long_description: scenario.long_description,
                            objective_type: scenario.objective_type,
                            objective_target: scenario.objective_target,
                            timeout_hours: scenario.timeout_hours,
                            version: scenario.version,
                            entry_cost: json_quantities(scenario.entry_cost.unwrap_or_default()),
                        })
                    })
                    .collect(),
                active: active
                    .simulations
                    .into_iter()
                    .filter_map(|run| {
                        run.simulation_id.map(|id| SimulationRunSummary {
                            id,
                            interface: Some(summary_ref(EntityKind::Device, code.clone())),
                            is_mine: run.is_mine.unwrap_or(false),
                            replicant: None,
                            replicant_name: run.replicant_name,
                            scenario_code: run.scenario_code,
                            scenario_name: run.scenario_name,
                            lifecycle: Some("active".to_owned()),
                            started_at: run.started_at,
                            completed_at: None,
                            abandoned_at: None,
                            timed_out_at: None,
                            score_seconds: None,
                            resources_mined: None,
                            devices_printed: None,
                            timeout_hours: run.timeout_hours,
                        })
                    })
                    .collect(),
                error: None,
            }),
            (scenarios, active) => interfaces.push(SimulationInterfaceSummary {
                device,
                scenarios: scenarios
                    .ok()
                    .map(|response| {
                        response
                            .scenarios
                            .into_iter()
                            .filter_map(|scenario| {
                                scenario.code.map(|code| SimulationScenarioSummary {
                                    code,
                                    name: scenario.name,
                                    description: scenario.description,
                                    long_description: scenario.long_description,
                                    objective_type: scenario.objective_type,
                                    objective_target: scenario.objective_target,
                                    timeout_hours: scenario.timeout_hours,
                                    version: scenario.version,
                                    entry_cost: json_quantities(
                                        scenario.entry_cost.unwrap_or_default(),
                                    ),
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                active: active
                    .ok()
                    .map(|response| {
                        response
                            .simulations
                            .into_iter()
                            .filter_map(|run| {
                                run.simulation_id.map(|id| SimulationRunSummary {
                                    id,
                                    interface: Some(summary_ref(EntityKind::Device, code.clone())),
                                    is_mine: run.is_mine.unwrap_or(false),
                                    replicant: None,
                                    replicant_name: run.replicant_name,
                                    scenario_code: run.scenario_code,
                                    scenario_name: run.scenario_name,
                                    lifecycle: Some("active".to_owned()),
                                    started_at: run.started_at,
                                    completed_at: None,
                                    abandoned_at: None,
                                    timed_out_at: None,
                                    score_seconds: None,
                                    resources_mined: None,
                                    devices_printed: None,
                                    timeout_hours: run.timeout_hours,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                error: Some("Some live simulation details are unavailable".to_owned()),
            }),
        }
    }
    let account_history = gateway
        .history_detailed()
        .await
        .map_err(|_| ApiError::unavailable())?;
    let managed_history = gateway
        .find()
        .collect()
        .await
        .map_err(|_| ApiError::unavailable())?;
    Ok(Json(Versioned::current(SimulationsSnapshot {
        metadata,
        interfaces,
        managed_history: managed_history
            .into_iter()
            .map(simulation_domain_summary)
            .collect(),
        account_history: account_history
            .into_iter()
            .filter_map(simulation_history_summary)
            .collect(),
    })))
}

fn simulation_history_summary(
    run: replicant_client::raw::simulations::SimulationHistoryEntry,
) -> Option<SimulationRunSummary> {
    Some(SimulationRunSummary {
        id: run.id?,
        interface: None,
        is_mine: true,
        replicant: None,
        replicant_name: None,
        scenario_code: run.scenario_code,
        scenario_name: run.scenario_name,
        lifecycle: Some(
            if run.completed_at.is_some() {
                "completed"
            } else if run.abandoned_at.is_some() {
                "abandoned"
            } else if run.timed_out_at.is_some() {
                "timed_out"
            } else {
                "archived"
            }
            .to_owned(),
        ),
        started_at: run.started_at,
        completed_at: run.completed_at,
        abandoned_at: run.abandoned_at,
        timed_out_at: run.timed_out_at,
        score_seconds: run.score_seconds,
        resources_mined: run.resources_mined,
        devices_printed: run.devices_printed,
        timeout_hours: None,
    })
}

fn simulation_domain_summary(run: replicant_client::domain::Simulation) -> SimulationRunSummary {
    SimulationRunSummary {
        id: run.id.get(),
        interface: None,
        is_mine: run.is_mine,
        replicant: run
            .replicant_code
            .map(|code| summary_ref(EntityKind::Replicant, code)),
        replicant_name: None,
        scenario_code: run.scenario_code,
        scenario_name: run.scenario_name,
        lifecycle: wire_value(Some(&run.lifecycle)),
        started_at: run.started_at,
        completed_at: run.completed_at,
        abandoned_at: None,
        timed_out_at: None,
        score_seconds: None,
        resources_mined: None,
        devices_printed: None,
        timeout_hours: None,
    }
}

fn json_quantities(values: Map<String, Value>) -> Vec<InventoryQuantity> {
    let mut rows = values
        .into_iter()
        .filter_map(|(resource, value)| {
            value
                .as_i64()
                .map(|quantity| InventoryQuantity { resource, quantity })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.resource.cmp(&right.resource));
    rows
}

async fn blueprints(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<BlueprintsSnapshot>>, ApiError> {
    let mut blueprints = state
        .client()
        .blueprints()
        .list()
        .await
        .map_err(|_| ApiError::unavailable())?
        .into_iter()
        .filter_map(|blueprint| {
            blueprint.device_type.map(|device_type| BlueprintSummary {
                device_type: device_type.as_str().to_owned(),
                short_description: blueprint.short_description,
                description: blueprint.description,
                print_time_seconds: blueprint.print_time_seconds,
                resources: blueprint
                    .resources
                    .into_iter()
                    .map(|(resource, quantity)| InventoryQuantity { resource, quantity })
                    .collect(),
                components: blueprint
                    .components
                    .into_iter()
                    .map(|(resource, quantity)| InventoryQuantity { resource, quantity })
                    .collect(),
                features: blueprint
                    .features
                    .into_iter()
                    .map(|feature| feature.as_str().to_owned())
                    .collect(),
                directives: blueprint
                    .directives
                    .into_iter()
                    .map(|directive| directive.as_str().to_owned())
                    .collect(),
                cargo_capacity: blueprint.cargo_capacity,
                attach_capacity: blueprint.attach_capacity,
                stow_capacity: blueprint.stow_capacity,
                queue_size: blueprint.queue_size,
            })
        })
        .collect::<Vec<_>>();
    blueprints.sort_by(|left, right| left.device_type.cmp(&right.device_type));
    Ok(Json(Versioned::current(BlueprintsSnapshot {
        metadata: state.snapshot_metadata()?,
        blueprints,
    })))
}

#[derive(Default, Deserialize)]
struct DirectoryQuery {
    name: Option<String>,
    limit: Option<i64>,
}

async fn directory(
    State(state): State<Arc<AppState>>,
    Query(query): Query<DirectoryQuery>,
) -> Result<Json<Versioned<DirectorySnapshot>>, ApiError> {
    let search = replicant_client::raw::replicants::ReplicantListQuery {
        cursor: None,
        limit: query.limit.or(Some(100)),
        latest: None,
        name: query.name.clone().filter(|value| !value.trim().is_empty()),
    };
    let replicants = state
        .client()
        .directory()
        .search(&search)
        .await
        .map_err(|_| ApiError::unavailable())?;
    Ok(Json(Versioned::current(DirectorySnapshot {
        metadata: state.snapshot_metadata()?,
        query: query.name,
        replicants: replicants
            .into_iter()
            .map(|profile| DirectoryReplicantSummary {
                entity: summary_ref(EntityKind::Replicant, profile.id.as_str()),
                name: profile.name,
                last_location: profile
                    .last_location
                    .map(|location| location.as_str().to_owned()),
                is_npc: profile.is_npc,
            })
            .collect(),
    })))
}

async fn directory_replicant(
    State(state): State<Arc<AppState>>,
    Path(code): Path<String>,
) -> Result<Json<Versioned<DirectoryReplicantDetailSnapshot>>, ApiError> {
    let replicant = state
        .client()
        .directory()
        .replicant(&code)
        .await
        .map_err(|_| ApiError::unavailable())?;
    let profile = DirectoryReplicantDetail {
        entity: summary_ref(EntityKind::Replicant, replicant.key.id.as_str()),
        name: replicant.name,
        is_npc: replicant.is_npc,
        status: wire_value(replicant.status.as_ref()),
        location: replicant.location.map(|location| location.id.to_string()),
        hosted_device: replicant
            .hosted_device
            .map(|device| summary_ref(EntityKind::Device, device.id.as_str())),
    };
    Ok(Json(Versioned::current(DirectoryReplicantDetailSnapshot {
        metadata: state.snapshot_metadata()?,
        replicant: profile,
    })))
}

#[derive(Default, Deserialize)]
struct TutorialsQuery {
    slug: Option<String>,
}

async fn tutorials(
    State(state): State<Arc<AppState>>,
    Query(query): Query<TutorialsQuery>,
) -> Result<Json<Versioned<TutorialsSnapshot>>, ApiError> {
    let list = state
        .client()
        .tutorials()
        .list()
        .await
        .map_err(|_| ApiError::unavailable())?;
    let tutorials = list
        .tutorials
        .into_iter()
        .filter_map(|tutorial| {
            tutorial.slug.map(|slug| TutorialSummary {
                slug,
                name: tutorial.name,
                description: tutorial.description,
                order: tutorial.order,
                completed: tutorial.completed,
                current_step: tutorial.current_step,
                total_steps: tutorial.total_steps,
                steps: Vec::new(),
            })
        })
        .collect::<Vec<_>>();
    let selected = if let Some(slug) = query
        .slug
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        let detail = state
            .client()
            .tutorials()
            .get(slug)
            .await
            .map_err(|_| ApiError::unavailable())?;
        Some(TutorialSummary {
            slug: detail.slug.unwrap_or_else(|| slug.to_owned()),
            name: detail.name,
            description: detail.description,
            order: detail.order,
            completed: detail.completed,
            current_step: detail.current_step,
            total_steps: detail.total_steps,
            steps: detail
                .steps
                .into_iter()
                .map(|step| TutorialStepSummary {
                    key: step.key,
                    description: step.description,
                    hint: step.hint,
                    completed: step.completed,
                    current: step.current,
                })
                .collect(),
        })
    } else {
        None
    };
    Ok(Json(Versioned::current(TutorialsSnapshot {
        metadata: state.snapshot_metadata()?,
        tutorials,
        selected,
    })))
}

async fn trade(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<TradeSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    // The daemon already maintains the owned replicant projection. Do not
    // force a redundant upstream replicant sync just to choose a directory
    // viewer every time the Trade page opens.
    let handles = state
        .client()
        .replicants()
        .find()
        .owned()
        .collect()
        .await
        .map_err(|_| ApiError::unavailable())?;
    let mut viewers = Vec::with_capacity(handles.len());
    for handle in handles {
        viewers.push(
            handle
                .snapshot()
                .await
                .map_err(|_| ApiError::unavailable())?,
        );
    }
    viewers.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.key.id.as_str().cmp(right.key.id.as_str()))
    });
    let Some(viewer) = viewers.into_iter().next() else {
        return Ok(Json(Versioned::current(TradeSnapshot {
            metadata,
            viewer: None,
            controllers: Vec::new(),
        })));
    };
    let viewer_code = viewer.key.id.as_str().to_owned();
    let traders = trader_directory(state.client(), &viewer_code)
        .await
        .map_err(|_| ApiError::unavailable())?;
    let devices = device_rows(&state).await?;
    let devices = devices.as_ref();
    let workflows = state
        .repository
        .list_summaries()
        .map_err(ApiError::repository)?
        .into_iter()
        .map(|workflow| (workflow.id.to_string(), stored_summary(&workflow)))
        .collect::<BTreeMap<_, _>>();
    let client = state.client().clone();
    let trade_results = futures_util::stream::iter(traders.into_iter().map(|trader| {
        let client = client.clone();
        async move {
            match shop_trades(&client, &trader.controller_code).await {
                Ok(trades) => (trader, trades, "available"),
                Err(error) => {
                    let status = trade_details_status(&error);
                    if status == "out_of_comms" {
                        tracing::debug!(
                            controller = %trader.controller_code,
                            system = ?trader.star,
                            "trade details unavailable from current comms coverage"
                        );
                    } else {
                        tracing::warn!(
                            controller = %trader.controller_code,
                            system = ?trader.star,
                            error = %error,
                            "trade detail refresh failed; returning partial trade directory"
                        );
                    }
                    (trader, Vec::new(), status)
                }
            }
        }
    }))
    .buffered(UPSTREAM_FANOUT)
    .collect::<Vec<_>>()
    .await;
    let mut controllers = Vec::with_capacity(trade_results.len());
    for (trader, trades, trade_details_status) in trade_results {
        let workflow = devices
            .iter()
            .find(|device| device.entity.id.0 == trader.controller_code)
            .and_then(|device| device.claim.as_ref())
            .and_then(|claim| workflows.get(&claim.workflow_id.0))
            .cloned();
        controllers.push(trade_controller_summary(
            trader,
            trades,
            trade_details_status,
            workflow,
        ));
    }
    Ok(Json(Versioned::current(TradeSnapshot {
        metadata,
        viewer: Some(summary_ref(EntityKind::Replicant, viewer_code)),
        controllers,
    })))
}

fn trade_details_status(error: &replicant_runtime::ApplicationError) -> &'static str {
    let Some(error) = error.downcast_ref::<ClientError>() else {
        return "unavailable";
    };
    if error.status() == Some(403)
        && error
            .details()
            .and_then(|details| details.message.as_deref())
            .is_some_and(|message| {
                message.contains("No replicant or comms device in this star system")
            })
    {
        "out_of_comms"
    } else {
        "unavailable"
    }
}

fn trade_controller_summary(
    trader: TraderSummary,
    trades: Vec<ShopTrade>,
    trade_details_status: &str,
    workflow: Option<WorkflowSummary>,
) -> TradeControllerSummary {
    TradeControllerSummary {
        entity: summary_ref(EntityKind::Device, trader.controller_code),
        shop_name: trader.shop_name,
        description: trader.description,
        is_local: trader.is_local,
        owner_name: trader.owner_name,
        owner_replicant: trader.owner_replicant_code,
        system: trader.star,
        location: trader.location,
        total_stock: trader.total_stock,
        trade_count: trader.trade_count,
        trade_details_status: trade_details_status.to_owned(),
        trades: trades
            .into_iter()
            .filter(|trade| !trade.trade_code.is_empty())
            .map(|trade| TradeSummary {
                trade_code: trade.trade_code,
                name: trade.name,
                current_stock: trade.current_stock,
                initial_stock: trade.initial_stock,
                requested: trade_items(trade.criteria.as_ref()),
                offered: trade_items(trade.rewards.as_ref()),
                created_at: trade.created_at,
            })
            .collect(),
        workflow,
    }
}

fn trade_items(value: Option<&Value>) -> Vec<TradeItemSummary> {
    let Some(Value::Object(items)) = value else {
        return Vec::new();
    };
    let mut normalized = Vec::new();
    for (kind_or_item, value) in items {
        if let Value::Object(nested) = value {
            normalized.extend(nested.iter().filter_map(|(item, quantity)| {
                quantity.as_f64().map(|quantity| TradeItemSummary {
                    kind: kind_or_item
                        .strip_suffix('s')
                        .unwrap_or(kind_or_item)
                        .to_owned(),
                    item: item.clone(),
                    quantity: Some(quantity),
                })
            }));
        } else if let Some(quantity) = value.as_f64() {
            normalized.push(TradeItemSummary {
                kind: "item".to_owned(),
                item: kind_or_item.clone(),
                quantity: Some(quantity),
            });
        }
    }
    normalized.sort_by(|left, right| {
        left.kind
            .cmp(&right.kind)
            .then_with(|| left.item.cmp(&right.item))
    });
    normalized
}

async fn find_bill(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<BillFinderRequest>, JsonRejection>,
) -> Result<Json<Versioned<BillFinderResponse>>, ApiError> {
    let Json(request) = payload.map_err(|_| ApiError::invalid("invalid Bill finder request"))?;
    let tracking_beacon =
        resolve_bill_tracking_beacon(state.client(), request.tracking_beacon.as_deref()).await?;
    let replicant_code = env::var("REPLICANT_BILL_REPLICANT_CODE")
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| BILL_REPLICANT_CODE.to_owned());

    let beacon = match state.client().devices().cached(&tracking_beacon) {
        Some(handle) => handle,
        None => state
            .client()
            .devices()
            .get(&tracking_beacon)
            .await
            .map_err(|error| {
                tracing::warn!(
                    tracking_beacon = %tracking_beacon,
                    error = %error,
                    "Bill tracking beacon could not be loaded"
                );
                ApiError::bill_unavailable()
            })?,
    };
    let audit = beacon
        .audit(&DeviceAuditQuery {
            replicant_code: Some(replicant_code.clone()),
            limit: Some(20),
            latest: Some(true),
            ..DeviceAuditQuery::default()
        })
        .await
        .map_err(|error| {
            tracing::warn!(
                tracking_beacon = %tracking_beacon,
                bill_replicant = %replicant_code,
                error = %error,
                "Bill tracking audit failed"
            );
            ApiError::bill_unavailable()
        })?;
    let observed = latest_bill_departure(&audit, &replicant_code)
        .ok_or_else(ApiError::bill_departure_not_found)?;

    let mut catalogue = state.client().galaxy().catalogue();
    let mut origin_system = resolve_catalogue_system(&observed.origin_location, &catalogue);
    if origin_system.is_none() {
        if let Err(error) = state.client().galaxy().refresh_catalogue().await {
            tracing::warn!(error = %error, "Bill finder catalogue refresh failed");
        }
        catalogue = state.client().galaxy().catalogue();
        origin_system = resolve_catalogue_system(&observed.origin_location, &catalogue);
    }
    let origin_system = origin_system.ok_or_else(ApiError::bill_catalogue_unavailable)?;
    let departure = BillDepartureSummary {
        tracking_beacon,
        replicant_code,
        vessel_code: observed.vessel_code,
        vessel_type: observed.vessel_type,
        origin_location: observed.origin_location,
        origin_system: origin_system.clone(),
        logged_at: observed.logged_at,
        vector: observed.vector,
    };
    let candidates = rank_bill_candidates(&catalogue, &origin_system, departure.vector);
    if candidates.is_empty() {
        return Err(ApiError::bill_catalogue_unavailable());
    }
    let (recommended_system, confidence, ambiguous) = bill_recommendation(&candidates);
    let expansion = bill_expansion(
        &state,
        &request,
        &candidates,
        recommended_system.as_deref(),
        ambiguous,
    )?;

    Ok(Json(Versioned::current(BillFinderResponse {
        metadata: state.snapshot_metadata()?,
        departure,
        candidates,
        recommended_system,
        confidence,
        ambiguous,
        expansion,
    })))
}

struct ObservedBillDeparture {
    vessel_code: Option<String>,
    vessel_type: Option<String>,
    origin_location: String,
    logged_at: Option<String>,
    vector: [f64; 3],
}

async fn resolve_bill_tracking_beacon(
    client: &Client,
    requested: Option<&str>,
) -> Result<String, ApiError> {
    if let Some(requested) = requested.map(str::trim).filter(|value| !value.is_empty()) {
        return Ok(requested.to_ascii_uppercase());
    }
    if let Some(configured) = env::var("REPLICANT_BILL_TRACKING_BEACON")
        .ok()
        .map(|value| value.trim().to_ascii_uppercase())
        .filter(|value| !value.is_empty())
    {
        return Ok(configured);
    }

    let monitoring = client
        .devices()
        .find()
        .owned()
        .of_type(replicant_client::domain::DeviceType::FtlBeacon)
        .with_status(replicant_client::domain::DeviceStatus::from("monitoring"))
        .in_system("SOL")
        .collect()
        .await
        .map_err(|_| ApiError::bill_unavailable())?;
    if let Some(beacon) = monitoring.first() {
        return Ok(beacon.id().as_str().to_owned());
    }

    let any_sol_beacon = client
        .devices()
        .find()
        .owned()
        .of_type(replicant_client::domain::DeviceType::FtlBeacon)
        .in_system("SOL")
        .collect()
        .await
        .map_err(|_| ApiError::bill_unavailable())?;
    Ok(any_sol_beacon
        .first()
        .map(|beacon| beacon.id().as_str().to_owned())
        .unwrap_or_else(|| BILL_DEFAULT_TRACKING_BEACON.to_owned()))
}

fn latest_bill_departure(audit: &Value, replicant_code: &str) -> Option<ObservedBillDeparture> {
    audit.get("audit")?.as_array()?.iter().find_map(|entry| {
        let entry = entry.as_object()?;
        if !entry
            .get("replicant_code")
            .and_then(Value::as_str)
            .is_some_and(|code| code.eq_ignore_ascii_case(replicant_code))
        {
            return None;
        }
        if !entry
            .get("travel_type")
            .and_then(Value::as_str)
            .is_some_and(|kind| kind.eq_ignore_ascii_case("departure"))
        {
            return None;
        }
        let vector = parse_bill_vector(entry.get("vector")?)?;
        let origin_location = entry.get("location")?.as_str()?.to_owned();
        Some(ObservedBillDeparture {
            vessel_code: entry
                .get("device_code")
                .and_then(Value::as_str)
                .map(str::to_owned),
            vessel_type: entry
                .get("device_type")
                .and_then(Value::as_str)
                .map(str::to_owned),
            origin_location,
            logged_at: entry
                .get("logged_at")
                .and_then(Value::as_str)
                .map(str::to_owned),
            vector,
        })
    })
}

fn parse_bill_vector(value: &Value) -> Option<[f64; 3]> {
    let vector = match value {
        Value::String(value) => {
            let values = value
                .split(',')
                .map(str::trim)
                .map(str::parse::<f64>)
                .collect::<Result<Vec<_>, _>>()
                .ok()?;
            (values.len() == 3).then_some([values[0], values[1], values[2]])?
        }
        Value::Array(values) if values.len() == 3 => [
            values[0].as_f64()?,
            values[1].as_f64()?,
            values[2].as_f64()?,
        ],
        _ => return None,
    };
    normalize_bill_vector(vector)
}

fn normalize_bill_vector(vector: [f64; 3]) -> Option<[f64; 3]> {
    if !vector.iter().all(|value| value.is_finite()) {
        return None;
    }
    let norm = (vector[0] * vector[0] + vector[1] * vector[1] + vector[2] * vector[2]).sqrt();
    (norm > f64::EPSILON).then_some([vector[0] / norm, vector[1] / norm, vector[2] / norm])
}

fn resolve_catalogue_system(
    location: &str,
    catalogue: &[replicant_client::domain::Star],
) -> Option<String> {
    catalogue
        .iter()
        .map(|star| star.key.id.as_str())
        .filter(|system| location == *system || location.starts_with(&format!("{system}-")))
        .max_by_key(|system| system.len())
        .map(str::to_owned)
}

fn rank_bill_candidates(
    catalogue: &[replicant_client::domain::Star],
    origin_system: &str,
    vector: [f64; 3],
) -> Vec<BillCandidateSummary> {
    let Some(origin) = catalogue
        .iter()
        .find(|star| star.key.id.as_str() == origin_system)
        .and_then(|star| star.position)
    else {
        return Vec::new();
    };
    let Some(vector) = normalize_bill_vector(vector) else {
        return Vec::new();
    };

    let mut candidates = catalogue
        .iter()
        .filter(|star| star.key.id.as_str() != origin_system)
        .filter_map(|star| {
            let position = star.position?;
            let delta = [
                position.x - origin.x,
                position.y - origin.y,
                position.z - origin.z,
            ];
            let distance = (delta[0] * delta[0] + delta[1] * delta[1] + delta[2] * delta[2]).sqrt();
            if !distance.is_finite() || distance <= f64::EPSILON {
                return None;
            }
            let projected = delta[0] * vector[0] + delta[1] * vector[1] + delta[2] * vector[2];
            if projected <= 0.0 {
                return None;
            }
            let cosine = (projected / distance).clamp(-1.0, 1.0);
            let angular_error_deg = cosine.acos().to_degrees();
            let cross_track_ly = (distance * distance - projected * projected)
                .max(0.0)
                .sqrt();
            Some(BillCandidateSummary {
                system: star.key.id.as_str().to_owned(),
                angular_error_deg,
                distance_ly: distance,
                projected_distance_ly: projected,
                cross_track_ly,
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        left.angular_error_deg
            .total_cmp(&right.angular_error_deg)
            .then_with(|| left.cross_track_ly.total_cmp(&right.cross_track_ly))
            .then_with(|| left.distance_ly.total_cmp(&right.distance_ly))
            .then_with(|| left.system.cmp(&right.system))
    });
    candidates.truncate(BILL_MAX_CANDIDATES);
    candidates
}

fn bill_recommendation(candidates: &[BillCandidateSummary]) -> (Option<String>, String, bool) {
    let Some(best) = candidates.first() else {
        return (None, "low".to_owned(), true);
    };
    let ambiguous = candidates.get(1).is_some_and(|second| {
        second.angular_error_deg <= BILL_MEDIUM_CONFIDENCE_DEG
            && second.angular_error_deg - best.angular_error_deg < BILL_AMBIGUITY_GAP_DEG
    });
    let confidence = if !ambiguous && best.angular_error_deg <= BILL_HIGH_CONFIDENCE_DEG {
        "high"
    } else if !ambiguous && best.angular_error_deg <= BILL_MEDIUM_CONFIDENCE_DEG {
        "medium"
    } else {
        "low"
    };
    let recommended = (!ambiguous && confidence != "low").then(|| best.system.clone());
    (recommended, confidence.to_owned(), ambiguous)
}

fn bill_expansion(
    state: &AppState,
    request: &BillFinderRequest,
    candidates: &[BillCandidateSummary],
    recommended_system: Option<&str>,
    ambiguous: bool,
) -> Result<BillExpansionSummary, ApiError> {
    if !request.expand {
        return Ok(BillExpansionSummary {
            status: "not_requested".to_owned(),
            target_system: None,
            workflow: None,
            message: "FTL expansion was not requested.".to_owned(),
        });
    }

    let explicit_target = request
        .target_system
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let target = if let Some(explicit_target) = explicit_target {
        candidates
            .iter()
            .find(|candidate| candidate.system.eq_ignore_ascii_case(explicit_target))
            .map(|candidate| candidate.system.clone())
            .ok_or_else(|| {
                ApiError::invalid("selected Bill candidate is not in the current result")
            })?
    } else if !ambiguous {
        if let Some(recommended_system) = recommended_system {
            recommended_system.to_owned()
        } else {
            return Ok(BillExpansionSummary {
                status: "selection_required".to_owned(),
                target_system: None,
                workflow: None,
                message: "The departure vector is not precise enough to expand automatically; select a candidate system first.".to_owned(),
            });
        }
    } else {
        return Ok(BillExpansionSummary {
            status: "selection_required".to_owned(),
            target_system: None,
            workflow: None,
            message: "Multiple systems fit Bill's departure vector; select a candidate before expanding the FTL network.".to_owned(),
        });
    };

    if let Some(existing) = existing_bill_expansion(&state.repository, &target)? {
        return Ok(BillExpansionSummary {
            status: "reused".to_owned(),
            target_system: Some(target),
            workflow: Some(summary(&existing)),
            message: "An active FTL expansion workflow already targets this system; reusing it."
                .to_owned(),
        });
    }

    let instance = state
        .catalogue
        .create_workflow(
            &state.repository,
            "exploration.frontier",
            BTreeMap::from([("target".to_owned(), Value::String(target.clone()))]),
        )
        .map_err(ApiError::catalogue)?;
    Ok(BillExpansionSummary {
        status: "queued".to_owned(),
        target_system: Some(target),
        workflow: Some(summary(&instance)),
        message: "Queued an FTL expansion workflow to the selected Bill candidate.".to_owned(),
    })
}

fn existing_bill_expansion(
    repository: &WorkflowRepository,
    target: &str,
) -> Result<Option<WorkflowInstance>, ApiError> {
    let workflows = repository.list_active().map_err(ApiError::repository)?;
    for workflow in workflows.into_iter().rev() {
        if workflow.kind.as_str() != "exploration.frontier" {
            continue;
        }
        let Ok(intent) = workflow.config::<ExplorationIntent>() else {
            continue;
        };
        if intent.target.eq_ignore_ascii_case(target) {
            return Ok(Some(workflow));
        }
    }
    Ok(None)
}

async fn reports(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<ReportsSnapshot>>, ApiError> {
    let executions = state
        .repository
        .finite_execution_history()
        .map_err(ApiError::repository)?
        .into_iter()
        .filter(|execution| execution.operation_class == FiniteExecutionClass::Report)
        .map(|execution| {
            let summary = execution
                .result
                .as_ref()
                .map_or_else(ResultSummary::default, |result| summarize_result(result).0);
            present_execution(execution, summary)
        })
        .collect();
    Ok(Json(Versioned::current(ReportsSnapshot {
        metadata: state.snapshot_metadata()?,
        reports: state.catalogue.descriptors().reports.clone(),
        executions,
    })))
}

async fn messages(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<MessagesSnapshot>>, ApiError> {
    let _guard = state.message_sync.lock().await;
    let inbox = state.client().messages().list().await.map_err(|error| {
        tracing::warn!(error = %error, "account message projection refresh failed");
        ApiError::unavailable()
    })?;
    if let Err(error) = state.repository.delete_document("messages", "inbox") {
        tracing::warn!(error = %error, "legacy runtime message cache cleanup failed");
    }
    Ok(Json(Versioned::current(messages_snapshot(&state, inbox)?)))
}

#[derive(Debug, Deserialize)]
struct MarkMessagesReadRequest {
    #[serde(default)]
    ids: Vec<i64>,
    #[serde(default)]
    mark_all: bool,
}

async fn mark_messages_read(
    State(state): State<Arc<AppState>>,
    Json(request): Json<MarkMessagesReadRequest>,
) -> Result<Json<Versioned<MessagesSnapshot>>, ApiError> {
    if !request.mark_all && request.ids.is_empty() {
        return Err(ApiError::invalid(
            "message IDs or mark_all=true are required",
        ));
    }

    let _guard = state.message_sync.lock().await;
    let ids = request.ids.into_iter().collect::<BTreeSet<_>>();
    let gateway = state.client().messages();
    let operation = gateway
        .mark_read(replicant_client::raw::messages::MessagesReadRequest {
            ids: (!request.mark_all).then(|| ids.iter().copied().collect()),
            mark_all: request.mark_all.then_some(true),
        })
        .await
        .map_err(|error| {
            tracing::warn!(error = %error, "marking inbox messages read failed");
            ApiError::unavailable()
        })?;
    let outcome = operation.outcome().await.map_err(|error| {
        tracing::warn!(error = %error, "reading message read operation outcome failed");
        ApiError::unavailable()
    })?;
    if outcome.status != ManagedOperationStatus::Completed {
        tracing::warn!(
            status = ?outcome.status,
            "message read operation did not complete successfully"
        );
        return Err(ApiError::unavailable());
    }

    let ids = ids.into_iter().collect::<Vec<_>>();
    let inbox = gateway
        .mark_cached_read(&ids, request.mark_all)
        .map_err(|error| {
            tracing::error!(error = %error, "updating managed message projection failed");
            ApiError::internal()
        })?;
    state.invalidate(DomainSlice::Messages);

    Ok(Json(Versioned::current(messages_snapshot(&state, inbox)?)))
}

fn inbox_message_summary(message: replicant_client::domain::Message) -> InboxMessageSummary {
    InboxMessageSummary {
        id: message.id,
        title: message.title,
        body: message.body,
        category: message.category,
        message_type: message.message_type,
        is_read: message.is_read,
        created_at: message.created_at,
    }
}

fn messages_snapshot(
    state: &AppState,
    inbox: replicant_client::managed::MessageInbox,
) -> Result<MessagesSnapshot, ApiError> {
    Ok(MessagesSnapshot {
        metadata: state.snapshot_metadata()?,
        inbox: inbox
            .messages
            .into_iter()
            .map(inbox_message_summary)
            .collect(),
        unread_count: inbox.unread_count,
    })
}

#[derive(Deserialize)]
struct BobnetQuery {
    source: Option<String>,
    cursor: Option<i64>,
    limit: Option<i64>,
    include_npcs: Option<bool>,
}

async fn bobnet(
    State(state): State<Arc<AppState>>,
    Query(query): Query<BobnetQuery>,
) -> Result<Json<Versioned<BobnetSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let sources = bobnet_history_source_rows(&state).await?;
    let selected_source = if let Some(source) = query.source.as_deref() {
        if !sources.iter().any(|device| device.entity.id.0 == source) {
            return Err(ApiError::invalid("unknown BobNet history source"));
        }
        Some(source.to_owned())
    } else {
        sources.first().map(|device| device.entity.id.0.clone())
    };
    let replicants = bobnet_replicants(state.client()).await?;
    let Some(source) = selected_source.as_deref() else {
        return Ok(Json(Versioned::current(BobnetSnapshot {
            metadata,
            sources,
            selected_source: None,
            channels: Vec::new(),
            messages: Vec::new(),
            replicants,
            next_cursor: None,
            total_messages: None,
            error: None,
        })));
    };

    let limit = query.limit.unwrap_or(100).clamp(1, 200);
    let include_npcs = query.include_npcs.unwrap_or(true);
    let bobnet = state.client().bobnet();
    let channels_future = tokio::time::timeout(Duration::from_secs(8), bobnet.channels(source));

    // BobNet NPCs are still replicants and can carry both a replicant code and
    // display name, so sender presence cannot classify them. Read the same
    // window with and without NPC chatter and use the differential to annotate
    // the complete history. Both reads run concurrently and the web client can
    // then toggle NPC visibility without another daemon/API request.
    let all_history = bobnet
        .history(source.to_owned())
        .include_npcs(true)
        .limit(limit);
    let player_history = bobnet
        .history(source.to_owned())
        .include_npcs(false)
        .limit(limit);
    let cursor = query.cursor;
    let all_messages_future = async move {
        if let Some(cursor) = cursor {
            all_history.cursor(cursor).list().await
        } else {
            all_history.latest(limit).await
        }
    };
    let player_messages_future = async move {
        if let Some(cursor) = cursor {
            player_history.cursor(cursor).list().await
        } else {
            player_history.latest(limit).await
        }
    };
    let (channels_result, messages_result, player_messages_result) = tokio::join!(
        channels_future,
        tokio::time::timeout(Duration::from_secs(8), all_messages_future),
        tokio::time::timeout(Duration::from_secs(8), player_messages_future),
    );

    let mut warnings = Vec::new();
    let channels = match channels_result {
        Ok(Ok(channels)) => channel_summaries(channels),
        Ok(Err(error)) => {
            warnings.push(format!("channel discovery failed: {error}"));
            Vec::new()
        }
        Err(_) => {
            warnings.push("channel discovery timed out".to_owned());
            Vec::new()
        }
    };
    let player_message_keys = match player_messages_result {
        Ok(Ok(messages)) => Some(
            messages
                .messages
                .iter()
                .map(bobnet_message_identity)
                .collect::<BTreeSet<_>>(),
        ),
        Ok(Err(error)) => {
            warnings.push(format!("player-only history read failed: {error}"));
            None
        }
        Err(_) => {
            warnings.push("player-only history read timed out".to_owned());
            None
        }
    };
    let (mut messages, next_cursor, total_messages) = match messages_result {
        Ok(Ok(messages)) => {
            let next_cursor = messages.next_cursor;
            let total_messages = messages.total_messages.or(messages.total);
            (
                messages
                    .messages
                    .into_iter()
                    .map(|message| bobnet_message_summary(message, player_message_keys.as_ref()))
                    .collect::<Vec<_>>(),
                next_cursor,
                total_messages,
            )
        }
        Ok(Err(error)) => {
            warnings.push(format!("history read failed: {error}"));
            (Vec::new(), None, None)
        }
        Err(_) => {
            warnings.push("history read timed out".to_owned());
            (Vec::new(), None, None)
        }
    };
    if !include_npcs {
        messages.retain(|message| !message.is_npc_or_system);
    }
    messages.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.id.cmp(&left.id))
    });

    Ok(Json(Versioned::current(BobnetSnapshot {
        metadata,
        sources,
        selected_source: Some(source.to_owned()),
        channels,
        messages,
        replicants,
        next_cursor,
        total_messages,
        error: (!warnings.is_empty()).then(|| warnings.join(" · ")),
    })))
}

fn bobnet_message_identity(message: &replicant_client::raw::bobnet::BobnetMessageItem) -> String {
    if let Some(id) = message.id {
        return format!("id:{id}");
    }
    format!(
        "message:{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}\u{0}{}",
        message.channel.as_deref().unwrap_or_default(),
        message.time.as_deref().unwrap_or_default(),
        message.replicant_code.as_deref().unwrap_or_default(),
        message.replicant_name.as_deref().unwrap_or_default(),
        message.current_star.as_deref().unwrap_or_default(),
        message.message.as_deref().unwrap_or_default(),
    )
}

fn bobnet_message_summary(
    message: replicant_client::raw::bobnet::BobnetMessageItem,
    player_message_keys: Option<&BTreeSet<String>>,
) -> BobnetMessageSummary {
    let is_npc_or_system = message.replicant_code.is_none()
        || player_message_keys
            .is_some_and(|keys| !keys.contains(&bobnet_message_identity(&message)));
    BobnetMessageSummary {
        id: message.id,
        channel: message.channel,
        body: message.message,
        is_npc_or_system,
        sender: message.replicant_code,
        sender_name: message.replicant_name,
        current_system: message.current_star,
        created_at: message.time,
    }
}

async fn bobnet_replicants(client: &Client) -> Result<Vec<BobnetReplicantSummary>, ApiError> {
    let handles = client
        .replicants()
        .find()
        .owned()
        .in_realm(Realm::Live)
        .collect()
        .await
        .map_err(|_| ApiError::unavailable())?;
    let mut replicants = Vec::with_capacity(handles.len());
    for handle in handles {
        let replicant = handle
            .snapshot()
            .await
            .map_err(|_| ApiError::unavailable())?;
        replicants.push(BobnetReplicantSummary {
            entity: summary_ref(EntityKind::Replicant, replicant.key.id.as_str()),
            name: replicant.name,
            status: replicant.status.map(|status| status.as_str().to_owned()),
            location: replicant
                .location
                .map(|location| location.id.as_str().to_owned()),
        });
    }
    replicants.sort_by(|left, right| {
        left.name
            .as_deref()
            .unwrap_or(left.entity.id.0.as_str())
            .cmp(right.name.as_deref().unwrap_or(right.entity.id.0.as_str()))
    });
    Ok(replicants)
}

async fn network(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<NetworkSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let relay_devices = relay_device_rows(&state).await?;
    let mut relays = relay_devices
        .into_iter()
        .map(|device| NetworkRelaySummary {
            device,
            channels: Vec::new(),
            error: None,
        })
        .collect::<Vec<_>>();

    // The game documents channel discovery as a global list obtainable from
    // any active relay-capable device. Asking every relay for the same list was
    // an N+1 read that became pathological on large relay networks. Probe one
    // active relay (or the first known relay as a fallback) and keep the rest
    // of the page entirely local.
    let source_index = relays
        .iter()
        .position(|relay| relay.device.status.as_deref() == Some("active"))
        .or_else(|| (!relays.is_empty()).then_some(0));
    let source_code = source_index.map(|index| relays[index].device.entity.id.0.clone());
    let account_future =
        tokio::time::timeout(Duration::from_secs(8), account_profile(state.client()));
    let channels_future = async {
        let relay_code = source_code.as_deref()?;
        Some(
            tokio::time::timeout(
                Duration::from_secs(6),
                state.client().bobnet().channels(relay_code),
            )
            .await,
        )
    };
    let (account_result, channel_result) = tokio::join!(account_future, channels_future);

    // Account profile and BobNet channel discovery are volatile reads. Keep
    // either one from holding the entire page hostage: managed relay state is
    // still useful when one remote endpoint is temporarily slow.
    let account = match account_result {
        Ok(Ok(account)) => account,
        _ => AccountMeResponse::default(),
    };
    if let (Some(index), Some(result)) = (source_index, channel_result) {
        match result {
            Ok(Ok(channels)) => relays[index].channels = channel_summaries(channels),
            Ok(Err(_)) => {
                relays[index].error = Some("Channel discovery unavailable".to_owned());
            }
            Err(_) => {
                relays[index].error = Some("Channel discovery timed out".to_owned());
            }
        }
    }

    Ok(Json(Versioned::current(network_snapshot(
        metadata, account, relays,
    ))))
}
fn channel_summaries(channels: DeviceChannelsResponse) -> Vec<BobnetChannelSummary> {
    channels
        .channels
        .into_iter()
        .filter_map(|channel| {
            channel.name.map(|name| BobnetChannelSummary {
                name,
                last_active: channel.last_active,
            })
        })
        .collect()
}

fn network_snapshot(
    metadata: SnapshotMetadata,
    account: AccountMeResponse,
    relays: Vec<NetworkRelaySummary>,
) -> NetworkSnapshot {
    NetworkSnapshot {
        metadata,
        account_name: account.name,
        account_status: account.status,
        subscribed_channels: account.bobnet_channels,
        replicants: account
            .replicants
            .into_iter()
            .filter_map(|replicant| {
                replicant
                    .replicant_code
                    .map(|code| AccountReplicantSummary {
                        entity: summary_ref(EntityKind::Replicant, code),
                        name: replicant.name,
                        system: replicant.current_star,
                        location: replicant.current_location,
                        hosted_device: replicant
                            .hosted_device_code
                            .map(|code| summary_ref(EntityKind::Device, code)),
                    })
            })
            .collect(),
        relays,
    }
}

fn is_bobnet_history_device(device: &DeviceSummary) -> bool {
    device.device_type.as_deref().is_some_and(|kind| {
        matches!(
            kind,
            "ftl_relay" | "system_hub" | "deep_space_relay_station"
        )
    })
}

async fn relay_device_rows(state: &Arc<AppState>) -> Result<Vec<DeviceSummary>, ApiError> {
    Ok(device_rows(state)
        .await?
        .iter()
        .filter(|device| {
            device
                .device_type
                .as_deref()
                .is_some_and(|kind| kind.to_ascii_lowercase().contains("relay"))
        })
        .cloned()
        .collect())
}

async fn bobnet_history_source_rows(state: &Arc<AppState>) -> Result<Vec<DeviceSummary>, ApiError> {
    let mut sources = device_rows(state)
        .await?
        .iter()
        .filter(|device| is_bobnet_history_device(device))
        .filter(|device| {
            device
                .status
                .as_deref()
                .is_some_and(|status| matches!(status, "active" | "relaying"))
        })
        .cloned()
        .collect::<Vec<_>>();
    sources.sort_by(|left, right| {
        left.system
            .cmp(&right.system)
            .then_with(|| left.location.cmp(&right.location))
            .then_with(|| left.entity.id.0.cmp(&right.entity.id.0))
    });
    Ok(sources)
}

async fn settings(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<SettingsSnapshot>>, ApiError> {
    Ok(Json(Versioned::current(SettingsSnapshot {
        metadata: state.snapshot_metadata()?,
        profile: state.daemon.profile.clone(),
        bind_address: state.daemon.bind.to_string(),
        managed_database_path: state.daemon.managed_database.display().to_string(),
        history_database_path: replicant_client::default_history_database_path(
            &state.daemon.managed_database,
        )
        .display()
        .to_string(),
        telemetry_database_path: state.daemon.telemetry_database.display().to_string(),
        runtime_database_path: state.daemon.runtime_database.display().to_string(),
        log_filter: config::log_filter_directive(),
        docker: config::docker_environment_detected(),
        api_token_source: config::api_token_source(),
        daemon_settings_require_restart: true,
    })))
}

async fn standing_snapshot(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<StandingSnapshot>>, ApiError> {
    let (account, achievements, reputation) = standing(state.client())
        .await
        .map_err(|_| ApiError::unavailable())?;
    Ok(Json(Versioned::current(build_standing_snapshot(
        state.snapshot_metadata()?,
        account,
        achievements,
        reputation,
    ))))
}

fn build_standing_snapshot(
    metadata: SnapshotMetadata,
    account: AccountMeResponse,
    achievements: AccountAchievementListResponse,
    reputation: AccountReputationResponse,
) -> StandingSnapshot {
    StandingSnapshot {
        metadata,
        experience_points_total: account.experience_points_total,
        civilisation_points: None,
        achievements: achievements
            .achievements
            .into_iter()
            .filter_map(|achievement| {
                achievement.achievement_key.map(|key| AchievementSummary {
                    key,
                    title: achievement.title,
                    description: achievement.description,
                    category: achievement.category,
                    xp_reward: achievement.xp_reward,
                    achieved_at: achievement.achieved_at,
                })
            })
            .collect(),
        reputation: reputation
            .reputation
            .into_iter()
            .filter_map(|standing| {
                standing.species_key.map(|species| ReputationSummary {
                    species,
                    name: standing.name,
                    value: standing.total_reputation,
                    description: standing.description,
                    trait_name: standing.r#trait,
                })
            })
            .collect(),
    }
}

#[derive(Deserialize)]
struct LeaderboardsQuery {
    board: Option<String>,
}

async fn leaderboards(
    State(state): State<Arc<AppState>>,
    Query(query): Query<LeaderboardsQuery>,
) -> Result<Json<Versioned<LeaderboardsSnapshot>>, ApiError> {
    let index = leaderboard_index(state.client())
        .await
        .map_err(|_| ApiError::unavailable())?;
    let selected = query.board.or_else(|| {
        index
            .boards
            .iter()
            .filter_map(|board| board.key.as_deref())
            .find(|key| *key == "xp")
            .or_else(|| {
                index
                    .boards
                    .iter()
                    .filter_map(|board| board.key.as_deref())
                    .next()
            })
            .map(str::to_owned)
    });
    if selected.as_deref().is_some_and(|key| {
        !index
            .boards
            .iter()
            .any(|board| board.key.as_deref() == Some(key))
    }) {
        return Err(ApiError::invalid("unknown leaderboard"));
    }
    let entries = if let Some(board) = selected.as_deref() {
        leaderboard(state.client(), board)
            .await
            .map_err(|_| ApiError::unavailable())?
    } else {
        LeaderboardResponse::default()
    };
    Ok(Json(Versioned::current(leaderboards_snapshot(
        state.snapshot_metadata()?,
        index,
        selected,
        entries,
    ))))
}

fn leaderboards_snapshot(
    metadata: SnapshotMetadata,
    index: LeaderboardIndexResponse,
    selected_board: Option<String>,
    leaderboard: LeaderboardResponse,
) -> LeaderboardsSnapshot {
    LeaderboardsSnapshot {
        metadata,
        boards: index
            .boards
            .into_iter()
            .filter_map(|board| {
                board.key.map(|key| LeaderboardBoardSummary {
                    key,
                    name: board.name,
                    description: board.description,
                    board_type: board.r#type,
                })
            })
            .collect(),
        selected_board,
        entries: leaderboard
            .entries
            .into_iter()
            .map(|entry| LeaderboardEntrySummary {
                rank: entry.rank,
                replicant: entry
                    .replicant_code
                    .map(|code| summary_ref(EntityKind::Replicant, code)),
                name: entry.name,
                designation: entry.designation,
                value: entry.value,
                contribution_count: entry.contribution_count,
            })
            .collect(),
    }
}

fn bootstrap_mission_summaries(
    executions: Vec<StoredFiniteExecution>,
) -> Vec<BootstrapMissionSummary> {
    let mut latest = BTreeMap::new();
    for execution in executions {
        if !execution.kind.starts_with("bootstrap.") {
            continue;
        }
        let Some(result) = execution.result else {
            continue;
        };
        let Ok(mission) = serde_json::from_value::<BootstrapMission>(result) else {
            continue;
        };
        latest.entry(mission.mission_id.clone()).or_insert_with(|| {
            bootstrap_mission_summary(mission, execution.id, execution.finished_at)
        });
    }
    let mut missions = latest.into_values().collect::<Vec<_>>();
    missions.sort_by_key(|mission| std::cmp::Reverse(mission.updated_at_ms));
    missions
}

fn bootstrap_mission_summary(
    mission: BootstrapMission,
    execution_id: String,
    updated_at_ms: i64,
) -> BootstrapMissionSummary {
    BootstrapMissionSummary {
        mission_id: mission.mission_id,
        execution_id,
        region: mission.region,
        source_hub: mission.source_hub,
        target_system: mission.landing_star,
        target_location: mission.landing_entry,
        phase: wire_value(Some(&mission.phase)).unwrap_or_else(|| "unknown".to_owned()),
        reserved_devices: mission.assets.values().map(Vec::len).sum(),
        loaded_devices: mission
            .carrier_loads
            .iter()
            .map(|load| load.devices.len())
            .sum(),
        capital_system: mission.capital_system,
        selected_sites: mission.selected_belts.len(),
        warnings: mission.warnings,
        completed: mission.phase.is_terminal(),
        updated_at_ms,
    }
}

fn mining_installations(devices: Vec<DeviceSummary>) -> Vec<MiningInstallationSummary> {
    const TYPES: [&str; 5] = [
        "ami_mining_controller",
        "mining_drone",
        "ami_survey_controller",
        "survey_drone",
        "maintenance_drone",
    ];
    let mut locations = BTreeMap::<String, Vec<DeviceSummary>>::new();
    for device in devices.into_iter().filter(|device| {
        device
            .device_type
            .as_deref()
            .is_some_and(|kind| TYPES.contains(&kind))
    }) {
        let key = format!(
            "{}/{}",
            device.system.as_deref().unwrap_or("unknown"),
            device.location.as_deref().unwrap_or("unknown")
        );
        locations.entry(key).or_default().push(device);
    }
    locations
        .into_iter()
        .map(|(id, devices)| mining_installation(id, devices))
        .collect()
}

fn mining_installation(id: String, devices: Vec<DeviceSummary>) -> MiningInstallationSummary {
    let find = |kind: &str| {
        devices
            .iter()
            .find(|device| device.device_type.as_deref() == Some(kind))
            .cloned()
    };
    let controller = find("ami_mining_controller");
    let survey_controller = find("ami_survey_controller");
    let adopted = |kind: &str, controller: Option<&DeviceSummary>| {
        let controller = controller.map(|device| device.entity.id.0.as_str());
        devices
            .iter()
            .filter(|device| {
                device.device_type.as_deref() == Some(kind)
                    && device.controller.as_deref() == controller
                    && controller.is_some()
            })
            .cloned()
            .collect::<Vec<_>>()
    };
    let miners = adopted("mining_drone", controller.as_ref());
    let survey_drones = adopted("survey_drone", survey_controller.as_ref());
    let maintenance_device = find("maintenance_drone");
    let mut missing = Vec::new();
    if controller.is_none() {
        missing.push("mining controller".to_owned());
    }
    if miners.len() < 4 {
        missing.push(format!("{} adopted mining drones", 4 - miners.len()));
    }
    if survey_controller.is_none() {
        missing.push("survey controller".to_owned());
    }
    if survey_drones.len() < 2 {
        missing.push(format!("{} adopted survey drones", 2 - survey_drones.len()));
    }
    if maintenance_device.is_none() {
        missing.push("maintenance drone".to_owned());
    }
    MiningInstallationSummary {
        system: devices.first().and_then(|device| device.system.clone()),
        location: devices.first().and_then(|device| device.location.clone()),
        controller,
        miners,
        survey_controller,
        survey_drones,
        maintenance_device,
        status: if missing.is_empty() {
            MiningInstallationStatus::Complete
        } else {
            MiningInstallationStatus::Partial
        },
        missing,
        id,
    }
}

async fn inventory(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<InventorySnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let location_systems = state
        .client()
        .locations()
        .find()
        .collect()
        .await
        .map_err(|_| ApiError::unavailable())?
        .into_iter()
        .map(|location| (location.id().to_string(), location.system))
        .collect();
    let inventories = state
        .client()
        .state()
        .inventories()
        .map_err(|_| ApiError::unavailable())?;
    Ok(Json(Versioned::current(inventory_snapshot(
        metadata,
        inventories,
        &location_systems,
    ))))
}

fn inventory_snapshot(
    metadata: SnapshotMetadata,
    inventories: Vec<Inventory>,
    location_systems: &BTreeMap<String, Option<String>>,
) -> InventorySnapshot {
    let mut locations = inventories
        .into_iter()
        .filter_map(|inventory| {
            let (owner_kind, owner) = match inventory.owner {
                InventoryOwner::Account(id) => {
                    (InventoryOwnerKind::Account, id.as_str().to_owned())
                }
                InventoryOwner::Replicant(key) => {
                    (InventoryOwnerKind::Replicant, key.id.as_str().to_owned())
                }
                InventoryOwner::Location(key) => {
                    (InventoryOwnerKind::Location, key.id.as_str().to_owned())
                }
                _ => return None,
            };
            let mut resources = BTreeMap::<String, i64>::new();
            for item in inventory.items.into_iter().filter(|item| item.quantity > 0) {
                *resources.entry(item.resource).or_default() += item.quantity;
            }
            if resources.is_empty() {
                return None;
            }
            let location = inventory.location.map(|key| key.id.to_string());
            let system = location
                .as_ref()
                .and_then(|value| device_system(value, location_systems));
            let resources = resources
                .into_iter()
                .map(|(resource, quantity)| InventoryQuantity { resource, quantity })
                .collect::<Vec<_>>();
            Some(InventoryLocationSummary {
                owner_kind,
                owner,
                system,
                location,
                total_quantity: resources.iter().map(|item| item.quantity).sum(),
                resources,
            })
        })
        .collect::<Vec<_>>();
    locations.sort_by(|left, right| {
        (
            &left.system,
            &left.location,
            left.owner_kind as u8,
            &left.owner,
        )
            .cmp(&(
                &right.system,
                &right.location,
                right.owner_kind as u8,
                &right.owner,
            ))
    });

    let mut resources = BTreeMap::<String, Vec<InventoryDistribution>>::new();
    for location in &locations {
        for item in &location.resources {
            resources
                .entry(item.resource.clone())
                .or_default()
                .push(InventoryDistribution {
                    owner_kind: location.owner_kind,
                    owner: location.owner.clone(),
                    system: location.system.clone(),
                    location: location.location.clone(),
                    quantity: item.quantity,
                });
        }
    }
    let resources = resources
        .into_iter()
        .map(|(resource, distribution)| InventoryResourceSummary {
            resource,
            total_quantity: distribution.iter().map(|item| item.quantity).sum(),
            distribution,
        })
        .collect::<Vec<_>>();
    InventorySnapshot {
        metadata,
        total_quantity: resources.iter().map(|item| item.total_quantity).sum(),
        locations,
        resources,
    }
}

fn device_summary(
    device: Device,
    location_systems: &BTreeMap<String, Option<String>>,
    system_regions: &BTreeMap<String, String>,
    replicant_names: &BTreeMap<String, String>,
    claim: Option<DeviceClaim>,
) -> Result<DeviceSummary, ApiError> {
    let location = device.location.map(|value| value.id.to_string());
    let owner = device
        .relationships
        .assigned_replicant
        .as_ref()
        .map(|value| value.id.to_string());
    let available_commands = device
        .available_commands
        .iter()
        .filter_map(|command| wire_value(Some(command)))
        .collect();
    let available_directives = device
        .available_directives
        .iter()
        .map(|directive| directive.as_str().to_owned())
        .collect();
    let features = device
        .features
        .iter()
        .filter_map(|feature| wire_value(Some(feature)))
        .collect();
    let system = location
        .as_ref()
        .and_then(|value| device_system(value, location_systems));
    let region = system
        .as_ref()
        .and_then(|value| system_regions.get(value).cloned());
    let cargo_capacity = device.cargo_capacity;
    let mut cargo = device
        .cargo
        .into_iter()
        .filter_map(|(resource, quantity)| {
            (quantity > 0).then_some(CargoResourceSummary { resource, quantity })
        })
        .collect::<Vec<_>>();
    cargo.sort_by(|left, right| left.resource.cmp(&right.resource));
    let cargo_used = if cargo.is_empty() && cargo_capacity.is_none() {
        None
    } else {
        Some(cargo.iter().try_fold(0_i64, |total, item| {
            total
                .checked_add(item.quantity)
                .ok_or_else(ApiError::unavailable)
        })?)
    };
    let active_directive = device
        .active_directive
        .as_ref()
        .and_then(|value| wire_value(value.directive.as_ref()));
    let directive_status = device
        .active_directive
        .as_ref()
        .and_then(|value| value.status.clone());
    Ok(DeviceSummary {
        entity: summary_ref(EntityKind::Device, device.key.id.to_string()),
        device_type: wire_value(device.device_type.as_ref()),
        status: wire_value(device.status.as_ref()),
        ownership: wire_value(Some(&device.access)).unwrap_or_else(|| "unknown".to_owned()),
        owner_name: owner
            .as_ref()
            .and_then(|value| replicant_names.get(value).cloned()),
        owner,
        system,
        region,
        location,
        available_commands,
        available_directives,
        features,
        tags: device.tags,
        attached_to: device
            .relationships
            .attached_to
            .map(|value| value.id.to_string()),
        stowed_in: device
            .relationships
            .stowed_in
            .map(|value| value.id.to_string()),
        controller: device
            .relationships
            .controller
            .map(|value| value.id.to_string()),
        linked_device: device
            .relationships
            .linked_device
            .map(|value| value.id.to_string()),
        attached_devices: device
            .relationships
            .attached_devices
            .into_iter()
            .map(|value| value.id.to_string())
            .collect(),
        controlled_devices: device
            .relationships
            .controlled_devices
            .into_iter()
            .map(|value| value.id.to_string())
            .collect(),
        stowed_devices: device
            .relationships
            .stowed_devices
            .into_iter()
            .map(|value| value.id.to_string())
            .collect(),
        attach_capacity: device.attach_capacity,
        cargo_capacity,
        cargo_used,
        cargo,
        stow_capacity: device.stow_capacity,
        stow_used: device.stow_used,
        operational_capacity_percent: device
            .operational_capacity
            .map(replicant_client::domain::OperationalCapacity::percent),
        grace_period_remaining: device.grace_period_remaining,
        upkeep_requirements: device.upkeep_requirements,
        system_status: device.system_status,
        active_directive,
        directive_status,
        travel_destination: device.travel.and_then(|value| {
            value
                .final_destination
                .or(value.destination)
                .map(|destination| destination.id.to_string())
        }),
        claim,
    })
}

fn device_system(
    location: &str,
    location_systems: &BTreeMap<String, Option<String>>,
) -> Option<String> {
    location_systems
        .get(location)
        .cloned()
        .flatten()
        .or_else(|| {
            location
                .split_once('-')
                .map(|(system, _)| system.to_owned())
        })
}

async fn entity_index(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<EntityIndexSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let workflows = state
        .repository
        .list_active()
        .map_err(ApiError::repository)?;
    let entities = build_entity_index(&state, &workflows)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "entity index projection failed");
            ApiError::unavailable()
        })?;
    Ok(Json(Versioned::current(EntityIndexSnapshot {
        metadata,

        entities,
    })))
}
async fn entity_inspector(
    State(state): State<Arc<AppState>>,
    Path((kind, id)): Path<(String, String)>,
) -> Result<Json<Versioned<EntityInspectorSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let snapshot = match kind.as_str() {
        "device" => {
            let rows = device_rows(&state).await?;
            let device = rows
                .iter()
                .find(|device| device.entity.id.0 == id)
                .cloned()
                .ok_or_else(ApiError::entity_not_found)?;
            let observation = state
                .client()
                .devices()
                .cached(&id)
                .ok_or_else(ApiError::entity_not_found)?
                .observation()
                .await
                .map_err(|_| ApiError::unavailable())?;
            EntityInspectorSnapshot {
                metadata,
                summary: EntitySummary {
                    entity: device.entity.clone(),
                    label: device.entity.id.0.clone(),
                    secondary_label: device.device_type.clone(),
                    system: device.system.clone(),
                    location: device.location.clone(),
                    entity_type: device.device_type.clone(),
                    status: device.status.clone(),
                },
                provenance: Some(inspector::provenance(&observation.metadata)),
                detail: EntityInspectorDetail::Device(device),
            }
        }
        "system" => {
            let locations = state
                .client()
                .locations()
                .find()
                .in_system(&id)
                .collect_observations()
                .await
                .map_err(|_| ApiError::unavailable())?;
            let stars = state.client().galaxy().catalogue_observations();
            let star = stars
                .iter()
                .find(|observation| observation.value.key.id.as_str() == id);
            if star.is_none() && locations.is_empty() {
                return Err(ApiError::entity_not_found());
            }
            let detail = inspector::system_detail(star, &locations).map_err(|error| {
                tracing::error!(%error, system = id, "system Inspector projection failed");
                ApiError::unavailable()
            })?;
            EntityInspectorSnapshot {
                metadata,
                summary: EntitySummary {
                    entity: summary_ref(EntityKind::System, id.clone()),
                    label: star
                        .and_then(|observation| observation.value.name.clone())
                        .unwrap_or_else(|| id.clone()),
                    secondary_label: star
                        .and_then(|observation| observation.value.spectral_type.clone()),
                    system: Some(id.clone()),
                    location: None,
                    entity_type: star
                        .and_then(|observation| observation.value.spectral_type.clone()),
                    status: star.and_then(|observation| {
                        observation.value.explored.map(|explored| {
                            if explored { "explored" } else { "unexplored" }.to_owned()
                        })
                    }),
                },
                provenance: star.map(|observation| inspector::provenance(&observation.metadata)),
                detail: EntityInspectorDetail::System(detail),
            }
        }
        "location" => {
            let mut locations = state
                .client()
                .locations()
                .find()
                .at(&id)
                .collect_observations()
                .await
                .map_err(|_| ApiError::unavailable())?;
            let observation = locations.pop().ok_or_else(ApiError::entity_not_found)?;
            let mut contents = device_rows(&state)
                .await?
                .iter()
                .filter(|device| device.location.as_deref() == Some(id.as_str()))
                .map(|device| EntitySummary {
                    entity: device.entity.clone(),
                    label: device.entity.id.0.clone(),
                    secondary_label: device.device_type.clone(),
                    system: device.system.clone(),
                    location: device.location.clone(),
                    entity_type: device.device_type.clone(),
                    status: device.status.clone(),
                })
                .collect::<Vec<_>>();
            for handle in state
                .client()
                .replicants()
                .find()
                .owned()
                .collect()
                .await
                .map_err(|_| ApiError::unavailable())?
            {
                let replicant = handle
                    .snapshot()
                    .await
                    .map_err(|_| ApiError::unavailable())?;
                if replicant
                    .location
                    .as_ref()
                    .map(|location| location.id.as_str())
                    != Some(id.as_str())
                {
                    continue;
                }
                let code = replicant.key.id.to_string();
                let name = replicant.name.clone();
                contents.push(EntitySummary {
                    entity: summary_ref(EntityKind::Replicant, code.clone()),
                    label: name.clone().unwrap_or_else(|| code.clone()),
                    secondary_label: name.map(|_| code),
                    system: observation.value.system.clone(),
                    location: Some(id.clone()),
                    entity_type: None,
                    status: wire_value(replicant.status.as_ref()),
                });
            }
            let summary = inspector::location_entity_summary(&observation.value);
            let detail =
                inspector::location_detail(&observation.value, contents).map_err(|error| {
                    tracing::error!(%error, location = id, "location Inspector projection failed");
                    ApiError::unavailable()
                })?;
            EntityInspectorSnapshot {
                metadata,
                summary,
                provenance: Some(inspector::provenance(&observation.metadata)),
                detail: EntityInspectorDetail::Location(detail),
            }
        }
        _ => {
            return Err(ApiError::invalid(
                "entity Inspector supports only device, system, and location",
            ));
        }
    };
    Ok(Json(Versioned::current(snapshot)))
}

async fn build_entity_index(
    state: &AppState,
    workflows: &[WorkflowInstance],
) -> replicant_client::Result<Vec<EntitySummary>> {
    let locations = state.client().locations().find().collect().await?;
    let location_systems = locations
        .iter()
        .map(|location| (location.id().to_string(), location.system.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut entities = locations
        .into_iter()
        .map(|location| {
            let id = location.id().to_string();
            EntitySummary {
                entity: summary_ref(EntityKind::Location, id.clone()),
                label: id.clone(),
                secondary_label: wire_value(location.location_type.as_ref()),
                system: location.system,
                location: Some(id),
                entity_type: wire_value(location.location_type.as_ref()),
                status: None,
            }
        })
        .collect::<Vec<_>>();

    for system in location_systems
        .values()
        .flatten()
        .collect::<std::collections::BTreeSet<_>>()
    {
        entities.push(EntitySummary {
            entity: summary_ref(EntityKind::System, (*system).clone()),
            label: (*system).clone(),
            secondary_label: None,
            system: Some((*system).clone()),
            location: None,
            entity_type: None,
            status: None,
        });
    }

    for handle in state.client().replicants().find().owned().collect().await? {
        let replicant = handle.snapshot().await?;
        let location = replicant.location.map(|location| location.id.to_string());
        let code = replicant.key.id.to_string();
        let name = replicant.name.clone();
        entities.push(EntitySummary {
            entity: summary_ref(EntityKind::Replicant, code.clone()),
            label: name.clone().unwrap_or_else(|| code.clone()),
            secondary_label: name.map(|_| code),
            system: location
                .as_ref()
                .and_then(|location| location_systems.get(location).cloned().flatten()),
            location,
            entity_type: None,
            status: wire_value(replicant.status.as_ref()),
        });
    }

    for handle in state.client().devices().find().collect().await? {
        let device = handle.snapshot().await?;
        let location = device.location.map(|location| location.id.to_string());
        entities.push(EntitySummary {
            entity: summary_ref(EntityKind::Device, device.key.id.to_string()),
            label: device.key.id.to_string(),
            secondary_label: wire_value(device.device_type.as_ref()),
            system: location
                .as_ref()
                .and_then(|location| location_systems.get(location).cloned().flatten()),
            location,
            entity_type: wire_value(device.device_type.as_ref()),
            status: wire_value(device.status.as_ref()),
        });
    }

    for workflow in workflows.iter().map(summary) {
        entities.push(EntitySummary {
            entity: EntityRef {
                kind: EntityKind::Workflow,
                id: EntityId(workflow.id.0.clone()),
            },
            label: workflow.kind.0,
            secondary_label: Some(workflow.id.0),
            system: None,
            location: None,
            entity_type: None,
            status: wire_value(Some(&workflow.status)),
        });
    }
    entities.sort_by(|left, right| left.entity.cmp(&right.entity));
    Ok(entities)
}

fn summary_ref(kind: EntityKind, id: impl Into<String>) -> EntityRef {
    EntityRef {
        kind,
        id: EntityId(id.into()),
    }
}

fn wire_value<T: Serialize>(value: Option<&T>) -> Option<String> {
    value
        .and_then(|value| serde_json::to_value(value).ok())
        .and_then(|value| value.as_str().map(str::to_owned))
}

async fn galaxy_scene(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<GalaxySceneSnapshot>>, ApiError> {
    let workflows = state
        .repository
        .list_active()
        .map_err(ApiError::repository)?;
    let revision = state.revision.load(Ordering::Relaxed);
    let scene = build_galaxy_scene(state.client(), &workflows, revision, now_millis()?)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, "galaxy scene projection failed");
            ApiError::unavailable()
        })?;
    Ok(Json(Versioned::current(scene)))
}

async fn system_scene(
    State(state): State<Arc<AppState>>,
    Path(system): Path<String>,
) -> Result<Json<Versioned<SystemSceneSnapshot>>, ApiError> {
    let workflows = state
        .repository
        .list_active()
        .map_err(ApiError::repository)?;
    let revision = state.revision.load(Ordering::Relaxed);
    let scene = build_system_scene(state.client(), &workflows, &system, revision, now_millis()?)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, system, "system scene projection failed");
            ApiError::unavailable()
        })?;
    Ok(Json(Versioned::current(scene)))
}

async fn refresh_locations(State(state): State<Arc<AppState>>) -> Result<StatusCode, ApiError> {
    tracing::info!("full managed location refresh requested");
    let report = state
        .client()
        .sync()
        .domain(SyncDomain::Locations)
        .await
        .map_err(|error| {
            tracing::warn!(error = %error, "full managed location refresh failed");
            ApiError::unavailable()
        })?;
    state.invalidate(DomainSlice::Universe);
    state.flush_invalidations();
    if !report.completed.contains(&SyncDomain::Locations) {
        tracing::warn!("full managed location refresh completed with failures");
        return Err(ApiError::unavailable());
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn refresh_system_locations(
    State(state): State<Arc<AppState>>,
    Path(system): Path<String>,
) -> Result<StatusCode, ApiError> {
    if system.trim().is_empty() {
        return Err(ApiError::invalid("system designation is required"));
    }
    tracing::info!(system, "targeted system location refresh requested");
    let report = state
        .client()
        .locations()
        .hydrate_system(&system)
        .planetary_bodies_only()
        .run()
        .await
        .map_err(|error| {
            tracing::warn!(error = %error, system, "targeted system location refresh failed");
            ApiError::unavailable()
        })?;
    state.invalidate(DomainSlice::Universe);
    state.flush_invalidations();
    if report.maximum_reached() || !report.failures().is_empty() {
        tracing::warn!(
            system,
            failures = report.failures().len(),
            maximum_reached = report.maximum_reached(),
            "targeted system location refresh was incomplete"
        );
        return Err(ApiError::unavailable());
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn descriptors(State(state): State<Arc<AppState>>) -> Json<Versioned<DescriptorCatalog>> {
    Json(Versioned::current(state.catalogue.descriptors().clone()))
}

async fn run_report(
    State(state): State<Arc<AppState>>,
    Path(kind): Path<String>,
    payload: Result<Json<RunOperationRequest>, JsonRejection>,
) -> Result<Json<Versioned<RunOperationResponse>>, ApiError> {
    let request = payload
        .map_err(|_| ApiError::invalid("invalid report parameters"))?
        .0;
    let started_at = now_millis()?;
    let result = state
        .catalogue
        .run_report(state.client(), &kind, request.parameters)
        .await;
    operation_response(
        &state,
        FiniteExecutionClass::Report,
        &kind,
        started_at,
        result,
    )
}

/// Starts an action and returns as soon as it is durably recorded.
///
/// Actions can run for hours (`bootstrap.deliver` travels and unloads), so the
/// work is spawned and its outcome is published over the live channel rather
/// than held open on the HTTP request. Awaiting it inline meant any proxy read
/// timeout severed the response while the action kept running, and a user who
/// retried on that apparent failure started a second copy against the same
/// devices.
async fn run_action(
    State(state): State<Arc<AppState>>,
    Path(kind): Path<String>,
    payload: Result<Json<RunOperationRequest>, JsonRejection>,
) -> Result<Json<Versioned<RunOperationResponse>>, ApiError> {
    let request = payload
        .map_err(|_| ApiError::invalid("invalid action parameters"))?
        .0;
    let started_at = now_millis()?;
    // Rejects unknown kinds and invalid parameters before anything is
    // recorded, so callers still get synchronous validation errors.
    state
        .catalogue
        .validate_action(&kind, &request.parameters)
        .map_err(ApiError::catalogue)?;

    let execution = state
        .repository
        .begin_finite_execution(FiniteExecutionClass::Action, &kind, started_at)
        .map_err(ApiError::repository)?;
    let execution = present_execution(execution, ResultSummary::default());

    spawn_action(state, execution.id.clone(), kind, request.parameters);

    Ok(Json(Versioned::current(RunOperationResponse {
        result: Value::Null,
        execution,
    })))
}

fn spawn_action(
    state: Arc<AppState>,
    execution_id: String,
    kind: String,
    parameters: BTreeMap<String, Value>,
) {
    let (registered, registration) = oneshot::channel();
    let spawned = state.clone();
    let spawned_id = execution_id.clone();
    let spawned_kind = kind.clone();
    let task = tokio::spawn(async move {
        let _ = registration.await;
        let outcome = spawned
            .catalogue
            .run_action(spawned.client(), &spawned_kind, parameters)
            .await;
        finish_action(&spawned, &spawned_id, &spawned_kind, outcome);
    });
    lock(&state.running_actions).insert(execution_id, (kind, task.abort_handle()));
    let _ = registered.send(());
}

async fn cancel_action(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    let (kind, task) = lock(&state.running_actions)
        .remove(&id)
        .ok_or_else(ApiError::action_not_running)?;
    task.abort();
    state
        .repository
        .complete_finite_execution(
            &id,
            StoredFiniteExecutionStatus::Cancelled,
            None,
            Some("cancelled by operator"),
        )
        .map_err(ApiError::repository)?;
    invalidate_action_slices(&state, &kind);
    state.invalidate(DomainSlice::History);
    state.flush_invalidations();
    Ok(StatusCode::NO_CONTENT)
}

/// Records an action's outcome and announces it to connected clients.
fn finish_action(
    state: &AppState,
    execution_id: &str,
    kind: &str,
    outcome: Result<Value, CatalogueError>,
) {
    if lock(&state.running_actions).remove(execution_id).is_none() {
        return;
    }
    let (status, result, error) = match outcome {
        Ok(result) => {
            let result = sanitize_result(result);
            let (_, status) = summarize_result(&result);
            (status, Some(result), None)
        }
        Err(error) => {
            tracing::warn!(kind, error = %error, "action execution failed");
            (
                StoredFiniteExecutionStatus::Failed,
                None,
                Some("execution failed"),
            )
        }
    };
    if let Err(repository_error) =
        state
            .repository
            .complete_finite_execution(execution_id, status, result.as_ref(), error)
    {
        tracing::error!(
            error = %repository_error,
            kind,
            "action outcome was not persisted"
        );
    }
    invalidate_action_slices(state, kind);
    state.invalidate(DomainSlice::History);
    state.flush_invalidations();
    if status == StoredFiniteExecutionStatus::Failed {
        state.notify(Notification {
            id: EntityId(format!("action:{execution_id}:failed")),
            level: NotificationLevel::Error,
            title: "Action failed".to_owned(),
            message: format!("{kind} did not complete"),
            created_at_ms: now_millis().unwrap_or_default(),
        });
    }
}

fn invalidate_action_slices(state: &AppState, kind: &str) {
    let slices: &[DomainSlice] = if kind.starts_with("bobnet.") {
        &[DomainSlice::Bobnet]
    } else if kind.starts_with("bootstrap.") {
        &[
            DomainSlice::Missions,
            DomainSlice::Devices,
            DomainSlice::Cargo,
        ]
    } else if kind.starts_with("trade.") {
        &[
            DomainSlice::Trade,
            DomainSlice::Devices,
            DomainSlice::Inventory,
            DomainSlice::Cargo,
        ]
    } else if kind.starts_with("simulation.") {
        &[
            DomainSlice::Simulations,
            DomainSlice::Devices,
            DomainSlice::Overview,
        ]
    } else if kind.starts_with("replicant.") {
        &[
            DomainSlice::Overview,
            DomainSlice::Entities,
            DomainSlice::Devices,
            DomainSlice::Universe,
        ]
    } else if kind.starts_with("device.") {
        &[
            DomainSlice::Devices,
            DomainSlice::Inventory,
            DomainSlice::Cargo,
        ]
    } else if kind.starts_with("survey.") {
        &[
            DomainSlice::Overview,
            DomainSlice::Entities,
            DomainSlice::Universe,
            DomainSlice::Activity,
        ]
    } else if kind.starts_with("observatory.") {
        &[
            DomainSlice::Devices,
            DomainSlice::Universe,
            DomainSlice::Activity,
        ]
    } else if kind.starts_with("clone.") {
        &[
            DomainSlice::Entities,
            DomainSlice::Overview,
            DomainSlice::Devices,
        ]
    } else if kind.starts_with("hub.") {
        &[DomainSlice::Devices, DomainSlice::Universe]
    } else {
        &[]
    };
    for slice in slices {
        state.invalidate(*slice);
    }
}

fn operation_response(
    state: &AppState,
    operation_class: FiniteExecutionClass,
    kind: &str,
    started_at: i64,
    result: Result<Value, CatalogueError>,
) -> Result<Json<Versioned<RunOperationResponse>>, ApiError> {
    match result {
        Ok(result) => {
            let result = sanitize_result(result);
            let (summary, status) = summarize_result(&result);
            let execution = persist_execution(
                state,
                operation_class,
                kind,
                status,
                started_at,
                Some(&result),
                None,
                summary,
            );
            invalidate_action_slices(state, kind);
            state.flush_invalidations();
            Ok(Json(Versioned::current(RunOperationResponse {
                result,
                execution,
            })))
        }
        Err(error) => {
            let _ = persist_execution(
                state,
                operation_class,
                kind,
                StoredFiniteExecutionStatus::Failed,
                started_at,
                None,
                Some("execution failed"),
                ResultSummary {
                    failed: 1,
                    ..ResultSummary::default()
                },
            );
            Err(ApiError::catalogue(error))
        }
    }
}

async fn finite_execution_history(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<FiniteExecutionHistoryResponse>>, ApiError> {
    let executions = state
        .repository
        .finite_execution_history()
        .map_err(ApiError::repository)?
        .into_iter()
        .map(|execution| {
            let summary = execution
                .result
                .as_ref()
                .map_or_else(ResultSummary::default, |result| summarize_result(result).0);
            present_execution(execution, summary)
        })
        .collect();
    Ok(Json(Versioned::current(FiniteExecutionHistoryResponse {
        executions,
    })))
}

async fn list_triggers(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<TriggerListResponse>>, ApiError> {
    let triggers = state
        .repository
        .list_triggers()
        .map_err(ApiError::repository)?
        .into_iter()
        .map(present_trigger)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(Versioned::current(TriggerListResponse { triggers })))
}

async fn create_trigger(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<CreateTriggerRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<Versioned<ProtocolTrigger>>), ApiError> {
    let Json(request) = payload.map_err(|_| ApiError::invalid("invalid trigger definition"))?;
    validate_trigger_request(&state, &request.name, &request.condition, &request.target)?;
    let condition = stored_condition(&request.condition)?;
    let now = now_millis()?;
    let next_run_at = match condition {
        TriggerCondition::Schedule { interval_millis } => Some(now.saturating_add(interval_millis)),
        _ => None,
    };
    let event_cursor = matches!(condition, TriggerCondition::GameEvent { .. })
        .then(|| state.client().events().cursor().ok().flatten())
        .flatten();
    let trigger = state
        .repository
        .create_trigger(NewTrigger {
            name: request.name.trim().to_owned(),
            condition,
            target: stored_target(request.target)?,
            enabled: request.enabled,
            next_run_at,
            event_cursor,
        })
        .map_err(ApiError::repository)?;
    Ok((
        StatusCode::CREATED,
        Json(Versioned::current(present_trigger(trigger)?)),
    ))
}

async fn update_trigger(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
    payload: Result<Json<UpdateTriggerRequest>, JsonRejection>,
) -> Result<Json<Versioned<ProtocolTrigger>>, ApiError> {
    let id = parse_trigger_id(&id)?;
    let current = state
        .repository
        .read_trigger(id)
        .map_err(ApiError::repository)?
        .ok_or_else(ApiError::not_found)?;
    let Json(request) = payload.map_err(|_| ApiError::invalid("invalid trigger definition"))?;
    validate_trigger_request(&state, &request.name, &request.condition, &request.target)?;
    let condition = stored_condition(&request.condition)?;
    let now = now_millis()?;
    let next_run_at = match condition {
        TriggerCondition::Schedule { interval_millis } => Some(now.saturating_add(interval_millis)),
        _ => None,
    };
    let event_cursor = if matches!(condition, TriggerCondition::GameEvent { .. }) {
        current
            .event_cursor
            .or_else(|| state.client().events().cursor().ok().flatten())
    } else {
        None
    };
    let trigger = state
        .repository
        .update_trigger(
            id,
            request.expected_revision,
            TriggerState {
                name: request.name.trim().to_owned(),
                condition,
                target: stored_target(request.target)?,
                enabled: request.enabled,
                next_run_at,
                event_cursor,
            },
        )
        .map_err(ApiError::repository)?;
    Ok(Json(Versioned::current(present_trigger(trigger)?)))
}

async fn delete_trigger(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    if state
        .repository
        .delete_trigger(parse_trigger_id(&id)?)
        .map_err(ApiError::repository)?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found())
    }
}

async fn fire_trigger(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Versioned<ProtocolTrigger>>, ApiError> {
    let id = parse_trigger_id(&id)?;
    let trigger = state
        .repository
        .read_trigger(id)
        .map_err(ApiError::repository)?
        .ok_or_else(ApiError::not_found)?;
    if !matches!(trigger.condition, TriggerCondition::Manual) {
        return Err(ApiError::invalid(
            "only manual triggers can be fired directly",
        ));
    }
    if !trigger.enabled {
        return Err(ApiError::invalid("trigger is disabled"));
    }
    let claimed = state
        .repository
        .claim_trigger_firing(
            id,
            &format!("manual:{}", TriggerId::new()),
            now_millis()?,
            None,
        )
        .map_err(ApiError::repository)?;
    if claimed && let Err(error) = launch_trigger(&state, &trigger, None).await {
        notify_trigger_error(&state, &trigger, &error);
        return Err(ApiError::invalid("trigger launch failed"));
    }
    let trigger = state
        .repository
        .read_trigger(id)
        .map_err(ApiError::repository)?
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(Versioned::current(present_trigger(trigger)?)))
}

fn validate_trigger_request(
    state: &AppState,
    name: &str,
    condition: &ProtocolTriggerCondition,
    target: &ProtocolTriggerTarget,
) -> Result<(), ApiError> {
    if name.trim().is_empty() || name.len() > 128 {
        return Err(ApiError::invalid(
            "trigger name must contain 1 to 128 characters",
        ));
    }
    let trigger_kind = match condition {
        ProtocolTriggerCondition::Manual => replicant_protocol::TriggerKind::Manual,
        ProtocolTriggerCondition::Schedule { interval_seconds } if *interval_seconds > 0 => {
            replicant_protocol::TriggerKind::Schedule
        }
        ProtocolTriggerCondition::Schedule { .. } => {
            return Err(ApiError::invalid(
                "schedule interval must be at least one second",
            ));
        }
        ProtocolTriggerCondition::GameEvent { event_name, .. } if !event_name.trim().is_empty() => {
            replicant_protocol::TriggerKind::GameEvent
        }
        ProtocolTriggerCondition::GameEvent { .. } => {
            return Err(ApiError::invalid("game event name cannot be empty"));
        }
        ProtocolTriggerCondition::StateCondition { .. } => {
            replicant_protocol::TriggerKind::StateCondition
        }
        ProtocolTriggerCondition::ParentWorkflow {
            status: ProtocolStatus::Succeeded | ProtocolStatus::Failed | ProtocolStatus::Cancelled,
            ..
        } => replicant_protocol::TriggerKind::ParentWorkflow,
        ProtocolTriggerCondition::ParentWorkflow { .. } => {
            return Err(ApiError::invalid("parent workflow status must be terminal"));
        }
    };
    if !matches!(
        target.operation_class,
        OperationClass::Action | OperationClass::Workflow
    ) {
        return Err(ApiError::invalid(
            "triggers can launch only actions or workflows",
        ));
    }
    state
        .catalogue
        .validate_invocation(
            target.operation_class,
            &target.kind.0,
            target.parameters.clone(),
        )
        .map_err(ApiError::catalogue)?;
    if target.operation_class == OperationClass::Workflow
        && !state
            .catalogue
            .descriptors()
            .workflows
            .iter()
            .any(|descriptor| {
                descriptor.kind == target.kind
                    && descriptor.supported_triggers.contains(&trigger_kind)
            })
    {
        return Err(ApiError::invalid(
            "workflow does not support this trigger kind",
        ));
    }
    Ok(())
}

fn stored_condition(condition: &ProtocolTriggerCondition) -> Result<TriggerCondition, ApiError> {
    Ok(match condition {
        ProtocolTriggerCondition::Manual => TriggerCondition::Manual,
        ProtocolTriggerCondition::Schedule { interval_seconds } => TriggerCondition::Schedule {
            interval_millis: i64::try_from(*interval_seconds)
                .ok()
                .and_then(|seconds| seconds.checked_mul(1_000))
                .ok_or_else(|| ApiError::invalid("schedule interval is too large"))?,
        },
        ProtocolTriggerCondition::GameEvent {
            event_name,
            device_code,
        } => TriggerCondition::GameEvent {
            event_name: event_name.clone(),
            device_code: device_code.clone(),
        },
        ProtocolTriggerCondition::StateCondition { minimum_revision } => {
            TriggerCondition::StateCondition {
                minimum_revision: *minimum_revision,
            }
        }
        ProtocolTriggerCondition::ParentWorkflow {
            parent_kind,
            status,
        } => TriggerCondition::ParentWorkflow {
            parent_kind: parent_kind
                .as_ref()
                .map(|kind| WorkflowKind::new(kind.0.clone()))
                .transpose()
                .map_err(ApiError::repository)?,
            status: stored_status(*status),
        },
    })
}

fn stored_target(target: ProtocolTriggerTarget) -> Result<TriggerTarget, ApiError> {
    let operation_class = match target.operation_class {
        OperationClass::Action => TriggerTargetClass::Action,
        OperationClass::Workflow => TriggerTargetClass::Workflow,
        OperationClass::Report => {
            return Err(ApiError::invalid("reports cannot be trigger targets"));
        }
    };
    Ok(TriggerTarget {
        operation_class,
        kind: target.kind.0,
        parameters: target.parameters,
    })
}

fn present_trigger(trigger: AutomationTrigger) -> Result<ProtocolTrigger, ApiError> {
    let condition = match trigger.condition {
        TriggerCondition::Manual => ProtocolTriggerCondition::Manual,
        TriggerCondition::Schedule { interval_millis } => ProtocolTriggerCondition::Schedule {
            interval_seconds: u64::try_from(interval_millis / 1_000)
                .map_err(|_| ApiError::invalid("invalid stored schedule interval"))?,
        },
        TriggerCondition::GameEvent {
            event_name,
            device_code,
        } => ProtocolTriggerCondition::GameEvent {
            event_name,
            device_code,
        },
        TriggerCondition::StateCondition { minimum_revision } => {
            ProtocolTriggerCondition::StateCondition { minimum_revision }
        }
        TriggerCondition::ParentWorkflow {
            parent_kind,
            status,
        } => ProtocolTriggerCondition::ParentWorkflow {
            parent_kind: parent_kind.map(|kind| OperationKind(kind.to_string())),
            status: protocol_status(status),
        },
    };
    Ok(ProtocolTrigger {
        id: ProtocolTriggerId(trigger.id.to_string()),
        name: trigger.name,
        condition,
        target: ProtocolTriggerTarget {
            operation_class: match trigger.target.operation_class {
                TriggerTargetClass::Action => OperationClass::Action,
                TriggerTargetClass::Workflow => OperationClass::Workflow,
            },
            kind: OperationKind(trigger.target.kind),
            parameters: sanitize_parameters(trigger.target.parameters),
        },
        enabled: trigger.enabled,
        created_at_ms: trigger.created_at,
        updated_at_ms: trigger.updated_at,
        last_fired_at_ms: trigger.last_fired_at,
        next_run_at_ms: trigger.next_run_at,
        last_error: trigger.last_error,
        revision: trigger.revision,
    })
}

#[allow(clippy::too_many_arguments)]
fn persist_execution(
    state: &AppState,
    operation_class: FiniteExecutionClass,
    kind: &str,
    status: StoredFiniteExecutionStatus,
    started_at: i64,
    result: Option<&Value>,
    error: Option<&str>,
    summary: ResultSummary,
) -> ProtocolFiniteExecution {
    match state.repository.record_finite_execution(
        operation_class,
        kind,
        status,
        started_at,
        result,
        error,
    ) {
        Ok(execution) => present_execution(execution, summary),
        Err(repository_error) => {
            tracing::error!(error = %repository_error, kind, "finite execution history was not persisted");
            present_execution(
                StoredFiniteExecution {
                    id: format!("unpersisted-{started_at}"),
                    operation_class,
                    kind: kind.to_owned(),
                    status,
                    started_at,
                    finished_at: now_millis().unwrap_or(started_at),
                    result: result.cloned(),
                    error: error.map(str::to_owned),
                },
                summary,
            )
        }
    }
}

fn present_execution(
    execution: StoredFiniteExecution,
    summary: ResultSummary,
) -> ProtocolFiniteExecution {
    let links = execution
        .result
        .as_ref()
        .map_or_else(Vec::new, result_links);
    ProtocolFiniteExecution {
        id: execution.id,
        operation_class: match execution.operation_class {
            FiniteExecutionClass::Report => OperationClass::Report,
            FiniteExecutionClass::Action => OperationClass::Action,
        },
        kind: OperationKind(execution.kind),
        status: match execution.status {
            StoredFiniteExecutionStatus::Running => ProtocolFiniteExecutionStatus::Running,
            StoredFiniteExecutionStatus::Succeeded => ProtocolFiniteExecutionStatus::Succeeded,
            StoredFiniteExecutionStatus::Skipped => ProtocolFiniteExecutionStatus::Skipped,
            StoredFiniteExecutionStatus::Failed => ProtocolFiniteExecutionStatus::Failed,
            StoredFiniteExecutionStatus::Cancelled => ProtocolFiniteExecutionStatus::Cancelled,
        },
        summary,
        started_at_ms: execution.started_at,
        finished_at_ms: execution.finished_at,
        result: execution.result,
        error: execution.error,
        links,
    }
}

fn summarize_result(result: &Value) -> (ResultSummary, StoredFiniteExecutionStatus) {
    fn visit(value: &Value, summary: &mut ResultSummary) {
        match value {
            Value::Array(values) => values.iter().for_each(|value| visit(value, summary)),
            Value::Object(object) => {
                if let Some(Value::String(kind)) = object.get("kind") {
                    match kind.as_str() {
                        "planned" | "succeeded" => summary.succeeded += 1,
                        "skipped" => summary.skipped += 1,
                        "failed" => summary.failed += 1,
                        _ => {}
                    }
                }
                object.values().for_each(|value| visit(value, summary));
            }
            _ => {}
        }
    }
    let mut summary = ResultSummary::default();
    visit(result, &mut summary);
    if summary == ResultSummary::default() {
        summary.succeeded = 1;
    }
    let status = if summary.failed > 0 {
        StoredFiniteExecutionStatus::Failed
    } else if summary.succeeded == 0 && summary.skipped > 0 {
        StoredFiniteExecutionStatus::Skipped
    } else {
        StoredFiniteExecutionStatus::Succeeded
    };
    (summary, status)
}

fn sanitize_result(value: Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.into_iter().map(sanitize_result).collect()),
        Value::Object(values) => Value::Object(
            values
                .into_iter()
                .map(|(key, value)| {
                    let lowered = key.to_ascii_lowercase();
                    let sensitive = [
                        "token",
                        "secret",
                        "password",
                        "credential",
                        "authorization",
                        "api_key",
                    ]
                    .iter()
                    .any(|needle| lowered.contains(needle));
                    (
                        key,
                        if sensitive {
                            Value::String("[redacted]".to_owned())
                        } else {
                            sanitize_result(value)
                        },
                    )
                })
                .collect(),
        ),
        value => value,
    }
}

fn sanitize_parameters(parameters: BTreeMap<String, Value>) -> BTreeMap<String, Value> {
    let Value::Object(parameters) =
        sanitize_result(Value::Object(parameters.into_iter().collect()))
    else {
        unreachable!("an object remains an object after sanitization")
    };
    parameters.into_iter().collect()
}

fn result_links(result: &Value) -> Vec<EntityRef> {
    fn visit(value: &Value, links: &mut Vec<EntityRef>) {
        match value {
            Value::Array(values) => values.iter().for_each(|value| visit(value, links)),
            Value::Object(object) => {
                for (key, value) in object {
                    let kind = match key.as_str() {
                        "operation_id" => Some(EntityKind::Operation),
                        "workflow_id" => Some(EntityKind::Workflow),
                        "system" => Some(EntityKind::System),
                        "location" => Some(EntityKind::Location),
                        "replicant" => Some(EntityKind::Replicant),
                        "device" => Some(EntityKind::Device),
                        _ => None,
                    };
                    if let (Some(kind), Value::String(id)) = (kind, value) {
                        let link = EntityRef {
                            kind,
                            id: EntityId(id.clone()),
                        };
                        if !links.contains(&link) {
                            links.push(link);
                        }
                    }
                    visit(value, links);
                }
            }
            _ => {}
        }
    }
    let mut links = Vec::new();
    visit(result, &mut links);
    links
}

#[derive(Default, Deserialize)]
struct WorkflowFilter {
    status: Option<ProtocolStatus>,
}

async fn list_workflows(
    State(state): State<Arc<AppState>>,
    Query(filter): Query<WorkflowFilter>,
) -> Result<Json<Versioned<WorkflowListResponse>>, ApiError> {
    let workflows = state
        .repository
        .list_summaries()
        .map_err(ApiError::repository)?
        .iter()
        .filter(|instance| {
            filter
                .status
                .is_none_or(|status| status == protocol_status(instance.status))
        })
        .map(stored_summary)
        .collect();
    Ok(Json(Versioned::current(WorkflowListResponse { workflows })))
}

async fn workflow_detail(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Versioned<WorkflowDetail>>, ApiError> {
    let instance = read_workflow(&state.repository, &id)?;
    Ok(Json(Versioned::current(detail(
        &state.repository,
        &instance,
    )?)))
}

async fn start_workflow(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<StartWorkflowRequest>, JsonRejection>,
) -> Result<(StatusCode, Json<Versioned<StartWorkflowResponse>>), ApiError> {
    let Json(request) = payload.map_err(|_| ApiError::invalid("invalid JSON request"))?;
    let instance = state
        .catalogue
        .create_workflow(&state.repository, &request.kind.0, request.parameters)
        .map_err(ApiError::catalogue)?;
    Ok((
        StatusCode::CREATED,
        Json(Versioned::current(StartWorkflowResponse {
            workflow: summary(&instance),
        })),
    ))
}

async fn director_snapshot(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<DirectorSnapshot>>, ApiError> {
    let snapshot =
        cached_director_snapshot(&state.repository, state.revision.load(Ordering::Relaxed))
            .map_err(ApiError::runtime)?;
    Ok(Json(Versioned::current(snapshot)))
}

async fn reconcile_director_now(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<DirectorSnapshot>>, ApiError> {
    tracing::info!("manual Automation Director reconciliation requested");
    let snapshot = reconcile_and_invalidate_director(&state, "manual").await?;
    state.flush_invalidations();
    Ok(Json(Versioned::current(snapshot)))
}

async fn update_director_mode(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<DirectorModeRequest>, JsonRejection>,
) -> Result<Json<Versioned<DirectorSnapshot>>, ApiError> {
    let Json(request) = payload.map_err(|_| ApiError::invalid("invalid Director mode request"))?;
    tracing::info!(mode = ?request.mode, "Automation Director mode changed");
    set_director_mode(&state.repository, request.mode).map_err(ApiError::runtime)?;
    director_control_changed(&state)
}

async fn update_director_goal(
    State(state): State<Arc<AppState>>,
    Path(kind): Path<String>,
    payload: Result<Json<DirectorGoalControlRequest>, JsonRejection>,
) -> Result<Json<Versioned<DirectorSnapshot>>, ApiError> {
    let Some(kind) = parse_goal_kind(&kind) else {
        return Err(ApiError::invalid("unknown Director goal kind"));
    };
    let Json(request) = payload.map_err(|_| ApiError::invalid("invalid Director goal request"))?;
    tracing::info!(goal = ?kind, enabled = request.enabled, "Automation Director goal changed");
    set_goal_enabled(&state.repository, kind, request.enabled).map_err(ApiError::runtime)?;
    director_control_changed(&state)
}

async fn update_director_replicant_region(
    State(state): State<Arc<AppState>>,
    Path(code): Path<String>,
    payload: Result<Json<DirectorReplicantRegionRequest>, JsonRejection>,
) -> Result<Json<Versioned<DirectorSnapshot>>, ApiError> {
    let Json(request) =
        payload.map_err(|_| ApiError::invalid("invalid regional assignment request"))?;
    tracing::info!(
        replicant = %code,
        region = ?request.region,
        role_affinity = ?request.role_affinity,
        "Automation Director regional assignment changed"
    );
    assign_replicant_region(
        &state.repository,
        &code,
        request.region.as_deref(),
        request.role_affinity.as_deref(),
    )
    .map_err(ApiError::runtime)?;
    director_control_changed(&state)
}

fn director_control_changed(
    state: &Arc<AppState>,
) -> Result<Json<Versioned<DirectorSnapshot>>, ApiError> {
    let snapshot =
        cached_director_snapshot(&state.repository, state.revision.load(Ordering::Relaxed))
            .map_err(ApiError::runtime)?;
    state.invalidate(DomainSlice::Director);
    state.flush_invalidations();
    state.director_wake.notify_one();
    Ok(Json(Versioned::current(snapshot)))
}

async fn reconcile_and_invalidate_director(
    state: &Arc<AppState>,
    trigger: &'static str,
) -> Result<DirectorSnapshot, ApiError> {
    let attempt = DIRECTOR_RECONCILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    tracing::info!(
        attempt,
        trigger,
        "Automation Director reconciliation queued"
    );
    let reconciliation = async {
        let lock_started = Instant::now();
        let _guard = state.director_reconcile.lock().await;
        tracing::info!(
            attempt,
            trigger,
            wait_ms = lock_started.elapsed().as_millis(),
            "Automation Director reconciliation lock acquired"
        );
        reconcile_director(
            state.client(),
            state.repository.clone(),
            state.revision.load(Ordering::Relaxed),
            true,
            trigger == "manual",
        )
        .await
    };
    let snapshot = match tokio::time::timeout(DIRECTOR_RECONCILE_TIMEOUT, reconciliation).await {
        Ok(Ok(snapshot)) => snapshot,
        Ok(Err(error)) => {
            state.record_runtime_telemetry(
                "director_reconcile",
                format!("{trigger}:failed"),
                1,
                Some(started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)),
            );
            return Err(ApiError::runtime(error));
        }
        Err(_) => {
            tracing::warn!(
                attempt,
                trigger,
                elapsed_ms = started.elapsed().as_millis(),
                timeout_ms = DIRECTOR_RECONCILE_TIMEOUT.as_millis(),
                "Automation Director reconciliation timed out"
            );
            state.record_runtime_telemetry(
                "director_reconcile",
                format!("{trigger}:timeout"),
                1,
                Some(started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)),
            );
            return Err(ApiError::director_timeout());
        }
    };
    state.record_runtime_telemetry(
        "director_reconcile",
        format!("{trigger}:success"),
        1,
        Some(started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)),
    );
    tracing::info!(
        attempt,
        trigger,
        elapsed_ms = started.elapsed().as_millis(),
        regions = snapshot.regions.len(),
        goals = snapshot.goals.len(),
        replicants = snapshot.replicants.len(),
        "Automation Director reconciliation completed"
    );
    state.invalidate(DomainSlice::Director);
    state.invalidate(DomainSlice::Workflows);
    Ok(snapshot)
}

async fn reconcile_director_background(state: &Arc<AppState>, trigger: &'static str) {
    match reconcile_and_invalidate_director(state, trigger).await {
        Ok(_) => state.flush_invalidations(),
        Err(error) => tracing::warn!(
            status = error.status.as_u16(),
            code = error.code,
            message = error.message,
            "Automation Director reconciliation failed"
        ),
    }
}

/// Periodically reconciles standing empire goals against managed world state.
pub async fn run_director(state: Arc<AppState>, mut shutdown: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tracing::info!(
        interval_seconds = 30,
        "Automation Director background loop started"
    );
    loop {
        tokio::select! {
            _ = interval.tick() => {
                tracing::debug!(trigger = "interval", "Automation Director reconciliation triggered");
                reconcile_director_background(&state, "interval").await;
            },
            _ = state.director_wake.notified() => {
                tracing::debug!(trigger = "control_change", "Automation Director reconciliation triggered");
                reconcile_director_background(&state, "control_change").await;
            },
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    tracing::info!("Automation Director background loop stopped");
}

async fn control_automation(
    State(state): State<Arc<AppState>>,
    payload: Result<Json<AutomationControlRequest>, JsonRejection>,
) -> Result<Json<Versioned<AutomationControlResponse>>, ApiError> {
    let Json(request) = payload.map_err(|_| ApiError::invalid("invalid automation control"))?;
    if request.action == AutomationControlAction::Cancel && !request.confirmed {
        return Err(ApiError::invalid(
            "cancellation requires explicit confirmation",
        ));
    }
    let mut policy = state
        .repository
        .automation_policy()
        .map_err(ApiError::repository)?;
    let affected_workflows = match request.action {
        AutomationControlAction::EnableTriggers => {
            policy.automatic_triggers_enabled = true;
            0
        }
        AutomationControlAction::DisableTriggers => {
            policy.automatic_triggers_enabled = false;
            0
        }
        AutomationControlAction::PauseAll => {
            policy.workflows_paused = true;
            state
                .repository
                .set_automation_policy(policy)
                .map_err(ApiError::repository)?;
            state.supervisor.pause_all().map_err(supervisor_error)?
        }
        AutomationControlAction::ResumeAll => {
            policy.workflows_paused = false;
            state
                .repository
                .set_automation_policy(policy)
                .map_err(ApiError::repository)?;
            state.supervisor.resume_all().map_err(supervisor_error)?
        }
        AutomationControlAction::Cancel => {
            let ids = request
                .workflow_ids
                .iter()
                .map(|id| parse_id(&id.0))
                .collect::<Result<Vec<_>, _>>()?;
            state
                .supervisor
                .cancel_selected(&ids)
                .map_err(supervisor_error)?
        }
    };
    policy = state
        .repository
        .set_automation_policy(policy)
        .map_err(ApiError::repository)?;
    let automation = automation_status(policy);
    state.publish(LiveDelta::AutomationChanged(automation));
    Ok(Json(Versioned::current(AutomationControlResponse {
        automation,
        affected_workflows,
    })))
}

async fn pause_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Versioned<WorkflowControlResponse>>, ApiError> {
    control_workflow(state, id, Control::Pause).await
}

async fn resume_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Versioned<WorkflowControlResponse>>, ApiError> {
    control_workflow(state, id, Control::Resume).await
}

async fn cancel_workflow(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Versioned<WorkflowControlResponse>>, ApiError> {
    control_workflow(state, id, Control::Cancel).await
}

enum Control {
    Pause,
    Resume,
    Cancel,
}

async fn control_workflow(
    state: Arc<AppState>,
    id: String,
    control: Control,
) -> Result<Json<Versioned<WorkflowControlResponse>>, ApiError> {
    let id = parse_id(&id)?;
    match control {
        Control::Pause => state.supervisor.pause(id),
        Control::Resume => state.supervisor.resume(id),
        Control::Cancel => state.supervisor.cancel(id),
    }
    .map_err(supervisor_error)?;
    let instance = state
        .repository
        .read(id)
        .map_err(ApiError::repository)?
        .ok_or_else(ApiError::not_found)?;
    Ok(Json(Versioned::current(WorkflowControlResponse {
        workflow: summary(&instance),
    })))
}

fn supervisor_error(error: SupervisorError) -> ApiError {
    match error {
        SupervisorError::Repository(error) => ApiError::repository(error),
    }
}

async fn workflow_activity(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Versioned<WorkflowActivityResponse>>, ApiError> {
    let id = parse_id(&id)?;
    if state
        .repository
        .read(id)
        .map_err(ApiError::repository)?
        .is_none()
    {
        return Err(ApiError::not_found());
    }
    let activity = state
        .repository
        .activity(id)
        .map_err(ApiError::repository)?
        .into_iter()
        .map(protocol_activity)
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(Json(Versioned::current(WorkflowActivityResponse {
        activity,
    })))
}

fn protocol_activity(
    record: replicant_workflow::WorkflowActivity,
) -> Result<WorkflowActivity, ApiError> {
    let (level, step, message) = present_activity(&record.message);
    Ok(WorkflowActivity {
        id: u64::try_from(record.id).map_err(|_| ApiError::internal())?,
        workflow_id: ProtocolWorkflowId(record.workflow_id.to_string()),
        occurred_at_ms: record.created_at,
        level,
        step,
        message,
    })
}

fn read_workflow(repository: &WorkflowRepository, id: &str) -> Result<WorkflowInstance, ApiError> {
    repository
        .read(parse_id(id)?)
        .map_err(ApiError::repository)?
        .ok_or_else(ApiError::not_found)
}

fn parse_id(id: &str) -> Result<WorkflowId, ApiError> {
    id.parse()
        .map_err(|_| ApiError::invalid("invalid workflow id"))
}

fn parse_trigger_id(id: &str) -> Result<TriggerId, ApiError> {
    id.parse()
        .map_err(|_| ApiError::invalid("invalid trigger id"))
}

fn summary(instance: &WorkflowInstance) -> WorkflowSummary {
    WorkflowSummary {
        id: ProtocolWorkflowId(instance.id.to_string()),
        kind: OperationKind(instance.kind.as_str().to_owned()),
        status: protocol_status(instance.status),
        current_step: instance.current_step.clone(),
        revision: instance.revision,
        updated_at_ms: instance.updated_at,
    }
}

fn stored_summary(instance: &StoredWorkflowSummary) -> WorkflowSummary {
    WorkflowSummary {
        id: ProtocolWorkflowId(instance.id.to_string()),
        kind: OperationKind(instance.kind.as_str().to_owned()),
        status: protocol_status(instance.status),
        current_step: instance.current_step.clone(),
        revision: instance.revision,
        updated_at_ms: instance.updated_at,
    }
}

fn requirement_summary(instance: &WorkflowInstance) -> Option<RequirementSummary> {
    let config = instance.config::<RequirementWorkflowConfig>().ok()?;
    let checkpoint = instance
        .checkpoint::<RequirementWorkflowCheckpoint>()
        .ok()?;
    let plan =
        checkpoint
            .plan
            .unwrap_or_else(|| replicant_runtime::requirements::FulfillmentPlan {
                requirement_id: config.requirement.id.clone(),
                desired: config.requirement.desired,
                actual: 0,
                in_progress: 0,
                missing: config.requirement.desired,
                step: None,
            });
    Some(RequirementSummary {
        id: config.requirement.id,
        name: config.requirement.name,
        target: requirement_target(&config.requirement.target),
        scope: requirement_scope(&config.requirement.scope),
        desired: plan.desired,
        actual: plan.actual,
        in_progress: plan.in_progress,
        missing: plan.missing,
        workflow_id: ProtocolWorkflowId(instance.id.to_string()),
        status: protocol_status(instance.status),
    })
}

fn requirement_scope(scope: &RequirementScope) -> String {
    match scope {
        RequirementScope::System(value) => format!("system {value}"),
        RequirementScope::Location(value) => format!("location {value}"),
    }
}

fn requirement_target(target: &RequirementTarget) -> String {
    match target {
        RequirementTarget::Device { device_type, .. } => device_type.clone(),
        RequirementTarget::Infrastructure { infrastructure } => match infrastructure {
            InfrastructureKind::Relay => "relay infrastructure".to_owned(),
            InfrastructureKind::Mining => "mining infrastructure".to_owned(),
        },
        RequirementTarget::Availability { asset } => match asset {
            AvailabilityKind::Device(value) => format!("available {value}"),
            AvailabilityKind::Resource(value) => format!("available {value}"),
        },
    }
}

fn detail(
    repository: &WorkflowRepository,
    instance: &WorkflowInstance,
) -> Result<WorkflowDetail, ApiError> {
    let parameters = instance
        .config::<Value>()
        .map_err(ApiError::repository)
        .and_then(config_parameters)?;
    let wait_reason = instance
        .wait_intent()
        .map_err(ApiError::repository)?
        .map(|intent| intent.description);
    let claims = repository
        .claims(instance.id)
        .map_err(ApiError::repository)?
        .into_iter()
        .map(|claim| entity_ref(claim.resource))
        .collect();
    Ok(WorkflowDetail {
        summary: summary(instance),
        schema_version: instance.schema_version,
        parameters,
        wait_reason,
        parent_id: instance
            .parent_id
            .map(|id| ProtocolWorkflowId(id.to_string())),
        claims,
        created_at_ms: instance.created_at,
        finished_at_ms: instance.status.is_terminal().then_some(instance.updated_at),
        error: if instance.status == WorkflowStatus::Failed {
            workflow_error(instance)
        } else {
            None
        },
    })
}

fn config_parameters(value: Value) -> Result<BTreeMap<String, Value>, ApiError> {
    let Value::Object(mut object) = sanitize_result(value) else {
        return Err(ApiError::internal());
    };
    if object.len() == 1
        && let Some(Value::Object(inner)) = object
            .remove("options")
            .or_else(|| object.remove("request"))
    {
        object = inner;
    }
    Ok(object.into_iter().collect())
}

fn entity_ref(resource: ResourceKey) -> EntityRef {
    let (kind, id) = match resource {
        ResourceKey::Replicant(id) => (EntityKind::Replicant, id),
        ResourceKey::Device(id) => (EntityKind::Device, id),
        ResourceKey::Autofactory(id) => (EntityKind::Autofactory, id),
        ResourceKey::Namespaced { namespace, key } => {
            (EntityKind::Workflow, format!("{namespace}:{key}"))
        }
    };
    EntityRef {
        kind,
        id: EntityId(id),
    }
}

fn protocol_status(status: WorkflowStatus) -> ProtocolStatus {
    match status {
        WorkflowStatus::Queued => ProtocolStatus::Queued,
        WorkflowStatus::Running => ProtocolStatus::Running,
        WorkflowStatus::Waiting => ProtocolStatus::Waiting,
        WorkflowStatus::Paused => ProtocolStatus::Paused,
        WorkflowStatus::Reconciling => ProtocolStatus::Reconciling,
        WorkflowStatus::Succeeded => ProtocolStatus::Succeeded,
        WorkflowStatus::Failed => ProtocolStatus::Failed,
        WorkflowStatus::Cancelled => ProtocolStatus::Cancelled,
    }
}

fn stored_status(status: ProtocolStatus) -> WorkflowStatus {
    match status {
        ProtocolStatus::Queued => WorkflowStatus::Queued,
        ProtocolStatus::Running => WorkflowStatus::Running,
        ProtocolStatus::Waiting => WorkflowStatus::Waiting,
        ProtocolStatus::Paused => WorkflowStatus::Paused,
        ProtocolStatus::Reconciling => WorkflowStatus::Reconciling,
        ProtocolStatus::Succeeded => WorkflowStatus::Succeeded,
        ProtocolStatus::Failed => WorkflowStatus::Failed,
        ProtocolStatus::Cancelled => WorkflowStatus::Cancelled,
    }
}

fn automation_status(policy: AutomationPolicy) -> AutomationStatus {
    AutomationStatus {
        automatic_triggers_enabled: policy.automatic_triggers_enabled,
        workflows_paused: policy.workflows_paused,
    }
}

fn runtime_sync_status(state: &AppState, status: &ClientStatus) -> RuntimeSyncStatus {
    RuntimeSyncStatus {
        phase: sync_phase(status),
        revision: state.client().state().revision().unwrap_or_default(),
        last_event_at_ms: None,
        detail: status_detail(status).map(str::to_owned),
    }
}

fn operational_notifications(
    workflows: &[WorkflowInstance],
    triggers: &[AutomationTrigger],
    status: &ClientStatus,
) -> Vec<Notification> {
    // A standing Director goal may retry the same blocked/failed work over time.
    // Keep the newest actionable notification for an identical condition
    // instead of accumulating one notification per superseded workflow ID.
    let mut workflow_notifications = BTreeMap::<(String, String), Notification>::new();
    for workflow in workflows {
        let Some(notification) = workflow_notification(workflow) else {
            continue;
        };
        let key = if notification.title == "Blocked resource claim" {
            // The conflicting owner may change from one scheduler pass to the
            // next. Treat that as the same actionable condition for a given
            // workflow kind while keeping the newest owner in the message.
            (notification.title.clone(), workflow.kind.to_string())
        } else {
            (notification.title.clone(), notification.message.clone())
        };
        match workflow_notifications.get(&key) {
            Some(existing) if existing.created_at_ms >= notification.created_at_ms => {}
            _ => {
                workflow_notifications.insert(key, notification);
            }
        }
    }
    let mut notifications = workflow_notifications.into_values().collect::<Vec<_>>();
    notifications.extend(triggers.iter().filter_map(|trigger| {
        trigger.last_error.as_ref().map(|_| Notification {
            id: EntityId(format!("trigger:{}:failed", trigger.id)),
            level: NotificationLevel::Error,
            title: "Automatic trigger failed".to_owned(),
            message: "An automatic trigger failed; inspect local daemon logs".to_owned(),
            created_at_ms: trigger.updated_at,
        })
    }));
    let sync = RuntimeSyncStatus {
        phase: sync_phase(status),
        revision: 0,
        last_event_at_ms: None,
        detail: status_detail(status).map(str::to_owned),
    };
    if matches!(sync.phase, SyncPhase::Degraded | SyncPhase::Offline) {
        notifications.push(sync_notification(&sync));
    }
    notifications.sort_by_key(|notification| std::cmp::Reverse(notification.created_at_ms));
    notifications
}

fn workflow_notification(workflow: &WorkflowInstance) -> Option<Notification> {
    let error = workflow_error(workflow)?;
    let blocked = error.contains("claimed by workflow") || error.contains("ClaimConflict");
    Some(Notification {
        id: EntityId(format!("workflow:{}:attention", workflow.id)),
        level: if blocked {
            NotificationLevel::Warning
        } else {
            NotificationLevel::Error
        },
        title: if blocked {
            "Blocked resource claim"
        } else {
            "Workflow needs attention"
        }
        .to_owned(),
        message: if blocked {
            format!("{} is blocked: {error}", workflow.kind)
        } else {
            format!("{} failed: {error}", workflow.kind)
        },
        created_at_ms: workflow.updated_at,
    })
}

fn workflow_error(workflow: &WorkflowInstance) -> Option<String> {
    let error = workflow.last_error.clone()?;
    let safe = workflow
        .config::<Value>()
        .is_ok_and(|config| sanitize_result(config.clone()) == config);
    Some(if safe { error } else { "[redacted]".to_owned() })
}

fn sync_notification(sync: &RuntimeSyncStatus) -> Notification {
    Notification {
        id: EntityId("managed-sync:degraded".to_owned()),
        level: NotificationLevel::Warning,
        title: "Managed synchronization degraded".to_owned(),
        message: sync
            .detail
            .clone()
            .unwrap_or_else(|| "SSE or managed state continuity needs attention".to_owned()),
        created_at_ms: now_millis().unwrap_or_default(),
    }
}

fn health_status(status: &ClientStatus) -> HealthStatus {
    match status {
        ClientStatus::Ready => HealthStatus::Healthy,
        ClientStatus::Closing | ClientStatus::Closed => HealthStatus::Unhealthy,
        _ => HealthStatus::Degraded,
    }
}

fn sync_phase(status: &ClientStatus) -> SyncPhase {
    match status {
        ClientStatus::Starting | ClientStatus::Restoring => SyncPhase::Starting,
        ClientStatus::CatchingUp | ClientStatus::Synchronizing | ClientStatus::Connecting => {
            SyncPhase::Syncing
        }
        ClientStatus::Ready => SyncPhase::Ready,
        ClientStatus::Degraded(_) => SyncPhase::Degraded,
        ClientStatus::Offline | ClientStatus::Closing | ClientStatus::Closed => SyncPhase::Offline,
        _ => SyncPhase::Degraded,
    }
}

fn status_detail(status: &ClientStatus) -> Option<&'static str> {
    match status {
        ClientStatus::Ready => None,
        ClientStatus::Starting | ClientStatus::Restoring => Some("managed state is restoring"),
        ClientStatus::CatchingUp | ClientStatus::Synchronizing | ClientStatus::Connecting => {
            Some("managed synchronization is in progress")
        }
        ClientStatus::Degraded(ClientDegradation::StartupIncomplete) => {
            Some("managed startup synchronization is incomplete")
        }
        ClientStatus::Degraded(ClientDegradation::EventContinuity) => {
            Some("managed event continuity is degraded")
        }
        ClientStatus::Degraded(_) => Some("managed synchronization is degraded"),
        ClientStatus::Offline => Some("managed client is offline"),
        ClientStatus::Closing | ClientStatus::Closed => Some("managed client is closed"),
        _ => Some("managed client status is unknown"),
    }
}

fn now_millis() -> Result<i64, ApiError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| ApiError::internal())?
        .as_millis();
    i64::try_from(millis).map_err(|_| ApiError::internal())
}

#[cfg(test)]
mod finite_result_tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn sanitizes_and_summarizes_action_results() {
        let result = sanitize_result(json!({
            "api_token": "do-not-export",
            "report": {"events": [
                {"kind": "succeeded", "device": "D-1", "operation_id": "O-1"},
                {"kind": "skipped", "device": "D-2"}
            ]}
        }));
        let (summary, status) = summarize_result(&result);

        assert_eq!(result["api_token"], "[redacted]");
        assert_eq!(summary.succeeded, 1);
        assert_eq!(summary.skipped, 1);
        assert_eq!(status, StoredFiniteExecutionStatus::Succeeded);
        assert_eq!(result_links(&result).len(), 3);
    }
}

fn present_activity(message: &str) -> (ActivityLevel, Option<String>, String) {
    match serde_json::from_str::<WorkflowActivityEvent>(message) {
        Ok(WorkflowActivityEvent::Failure { .. }) => {
            (ActivityLevel::Error, None, "workflow failed".to_owned())
        }
        Ok(WorkflowActivityEvent::StepEntered { step }) => (
            ActivityLevel::Info,
            Some(step.clone()),
            format!("entered {step}"),
        ),
        Ok(WorkflowActivityEvent::OperationSubmitted { step }) => (
            ActivityLevel::Info,
            Some(step.clone()),
            format!("submitted {step}"),
        ),
        Ok(WorkflowActivityEvent::OperationCompleted { step }) => (
            ActivityLevel::Info,
            Some(step.clone()),
            format!("completed {step}"),
        ),
        Ok(WorkflowActivityEvent::WaitReason { step, reason }) => {
            (ActivityLevel::Info, Some(step), reason)
        }
        Ok(WorkflowActivityEvent::ReconciliationDecision { step, decision }) => {
            (ActivityLevel::Info, Some(step), decision)
        }
        Ok(WorkflowActivityEvent::ResourceClaimed { resource }) => {
            (ActivityLevel::Info, None, format!("claimed {resource:?}"))
        }
        Ok(WorkflowActivityEvent::Completion) => {
            (ActivityLevel::Info, None, "workflow completed".to_owned())
        }
        Err(_) => (ActivityLevel::Info, None, message.to_owned()),
    }
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: &'static str,
}

impl ApiError {
    fn invalid(message: &'static str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "invalid_request",
            message,
        }
    }

    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "unauthorized",
            message: "a valid daemon token is required",
        }
    }

    fn forbidden_device_logs() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "device_logs_forbidden",
            message: "device logs are only available for account-owned devices",
        }
    }

    fn device_not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "device_not_found",
            message: "device is not present in managed state",
        }
    }

    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "workflow_not_found",
            message: "workflow not found",
        }
    }

    fn entity_not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "entity_not_found",
            message: "entity is not present in managed state",
        }
    }

    fn operation_not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "operation_not_found",
            message: "operation not found",
        }
    }

    fn action_not_running() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "action_not_running",
            message: "action execution is not running",
        }
    }

    fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "runtime_unavailable",
            message: "runtime is unavailable",
        }
    }

    fn bill_unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "bill_tracking_unavailable",
            message: "Bill's tracking beacon audit is currently unavailable",
        }
    }

    fn bill_departure_not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "bill_departure_not_found",
            message: "no Bill departure vector was found in the selected beacon audit",
        }
    }

    fn bill_catalogue_unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "bill_catalogue_unavailable",
            message: "the star catalogue cannot currently resolve Bill's departure vector",
        }
    }

    fn internal() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "runtime_error",
            message: "runtime request failed",
        }
    }

    fn director_timeout() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "director_reconcile_timeout",
            message: "Automation Director reconciliation timed out; the last successful snapshot remains available",
        }
    }

    fn runtime(error: replicant_runtime::ApplicationError) -> Self {
        tracing::error!(error = %error, "runtime Director request failed");
        Self::internal()
    }

    fn repository(error: RepositoryError) -> Self {
        match error {
            RepositoryError::NotFound(_) => Self::not_found(),
            RepositoryError::InvalidTransition { .. }
            | RepositoryError::ClaimConflict { .. }
            | RepositoryError::ConcurrentUpdate { .. } => Self {
                status: StatusCode::CONFLICT,
                code: "workflow_conflict",
                message: "workflow state conflict",
            },
            error => {
                tracing::error!(error = %error, "runtime repository request failed");
                Self::internal()
            }
        }
    }

    fn catalogue(error: CatalogueError) -> Self {
        match error {
            CatalogueError::UnknownKind {
                class: OperationClass::Workflow,
                ..
            } => Self::invalid("unknown workflow kind"),
            CatalogueError::UnknownKind { .. } => Self::operation_not_found(),
            CatalogueError::Invalid(_) => Self::invalid("invalid operation parameters"),
            CatalogueError::Repository(error) => Self::repository(error),
            error => {
                tracing::error!(error = %error, "runtime catalogue request failed");
                Self::internal()
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(Versioned::current(ErrorResponse {
                code: self.code.to_owned(),
                message: self.message.to_owned(),
            })),
        )
            .into_response()
    }
}

/// Returns whether an address is loopback-only.
#[must_use]
pub fn is_loopback(address: SocketAddr) -> bool {
    match address.ip() {
        IpAddr::V4(ip) => ip.is_loopback(),
        IpAddr::V6(ip) => ip.is_loopback(),
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::Body,
        http::{Request, header},
    };
    use futures_util::StreamExt;
    use http_body_util::BodyExt;
    use replicant_client::{
        StartupPolicy,
        raw::{SecretString, Url},
    };
    use replicant_protocol::{Notification, NotificationLevel};
    use replicant_workflow::{NewWorkflow, WorkflowKind, WorkflowState};
    use tokio::net::TcpListener;
    use tower::ServiceExt;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method, path},
    };

    use super::*;

    #[test]
    fn log_directory_defaults_next_to_runtime_database() {
        assert_eq!(
            default_log_directory(std::path::Path::new(
                "/var/lib/replicant/replicant-runtime.sqlite",
            )),
            PathBuf::from("/var/lib/replicant/logs"),
        );
        assert_eq!(
            default_log_directory(std::path::Path::new("replicant-runtime.sqlite")),
            PathBuf::from("logs"),
        );
    }

    fn test_daemon_config() -> DaemonConfig {
        DaemonConfig {
            profile: "test".to_owned(),
            managed_database: PathBuf::from("replicant-client.sqlite"),
            runtime_database: PathBuf::from("replicant-runtime.sqlite"),
            telemetry_database: PathBuf::from("replicant-telemetry.sqlite"),
            log_directory: PathBuf::from("logs"),
            bind: DEFAULT_BIND.parse().expect("default bind address"),
            token: None,
            workflow_retention_days: Some(DEFAULT_WORKFLOW_RETENTION_DAYS),
        }
    }

    async fn test_app() -> (Router, Client, Arc<AppState>) {
        let client = Client::builder()
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("start test client");
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("runtime database"));
        let state = AppState::new(
            client.clone(),
            RuntimeConfig::new("test"),
            repository,
            test_daemon_config(),
        )
        .expect("app state");
        (router(state.clone()), client, state)
    }

    async fn test_app_at(base_url: &str) -> (Router, Client) {
        let client = Client::builder()
            .authentication_token(SecretString::from("token".to_owned()))
            .base_url(Url::parse(base_url).expect("mock URL"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("start test client");
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("runtime database"));
        let state = AppState::new(
            client.clone(),
            RuntimeConfig::new("test"),
            repository,
            test_daemon_config(),
        )
        .expect("app state");
        (router(state), client)
    }

    #[tokio::test]
    async fn location_refresh_routes_run_full_and_targeted_traversals() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/locations/SYS-A"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "location": "SYS-A",
                "location_type": "star",
                "planets_total": 0,
                "planets_scanned": 0,
                "planets": []
            })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/locations/SYS-B"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "location": "SYS-B",
                "location_type": "star",
                "planets": []
            })))
            .expect(1)
            .mount(&server)
            .await;
        let (app, client) = test_app_at(&server.uri()).await;
        client
            .locations()
            .get("SYS-A")
            .await
            .expect("seed known system");

        let full = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/locations/refresh")
                    .body(Body::empty())
                    .expect("full refresh request"),
            )
            .await
            .expect("full refresh response");
        let targeted = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/locations/refresh/SYS-B")
                    .body(Body::empty())
                    .expect("targeted refresh request"),
            )
            .await
            .expect("targeted refresh response");

        assert_eq!(full.status(), StatusCode::NO_CONTENT);
        assert_eq!(targeted.status(), StatusCode::NO_CONTENT);
        assert!(client.locations().cached("SYS-B").is_some());
        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn running_action_can_be_cancelled() {
        let (app, client, state) = test_app().await;
        let execution = state
            .repository
            .begin_finite_execution(FiniteExecutionClass::Action, "survey.belt_search", 1)
            .expect("begin action");
        let task = tokio::spawn(std::future::pending::<()>());
        lock(&state.running_actions).insert(
            execution.id.clone(),
            ("survey.belt_search".to_owned(), task.abort_handle()),
        );

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/action-executions/{}/cancel", execution.id))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("cancel response");

        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(task.await.expect_err("task aborted").is_cancelled());
        let stored = state
            .repository
            .finite_execution_history()
            .expect("history")
            .into_iter()
            .find(|item| item.id == execution.id)
            .expect("cancelled action");
        assert_eq!(stored.status, StoredFiniteExecutionStatus::Cancelled);
        client.close().await.expect("close client");
    }

    async fn next_live(
        socket: &mut tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
    ) -> LiveMessage {
        loop {
            match socket
                .next()
                .await
                .expect("websocket message")
                .expect("websocket frame")
            {
                tokio_tungstenite::tungstenite::Message::Text(text) => {
                    return serde_json::from_str(&text).expect("live message");
                }
                tokio_tungstenite::tungstenite::Message::Ping(_) => {}
                frame => panic!("unexpected websocket frame: {frame:?}"),
            }
        }
    }

    async fn json(response: Response) -> Value {
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        serde_json::from_slice(&body).expect("JSON response")
    }

    fn projected_device(code: &str) -> DeviceSummary {
        DeviceSummary {
            entity: summary_ref(EntityKind::Device, code.to_owned()),
            device_type: None,
            status: None,
            ownership: "owned".to_owned(),
            owner: None,
            owner_name: None,
            system: None,
            region: None,
            location: None,
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            features: Vec::new(),
            tags: Vec::new(),
            attached_to: None,
            stowed_in: None,
            controller: None,
            linked_device: None,
            attached_devices: Vec::new(),
            controlled_devices: Vec::new(),
            stowed_devices: Vec::new(),
            attach_capacity: None,
            cargo_capacity: None,
            cargo_used: None,
            cargo: Vec::new(),
            stow_capacity: None,
            stow_used: None,
            operational_capacity_percent: None,
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            directive_status: None,
            travel_destination: None,
            claim: None,
        }
    }

    #[tokio::test]
    async fn entity_inspector_route_projects_compact_authoritative_details() {
        let server = MockServer::start().await;
        let devices = (0..393)
            .map(|index| {
                let mut device = serde_json::json!({
                    "device_code": format!("D-{index:03}"),
                    "device_type": "mining_drone",
                    "status": "idle",
                    "location": "SOL-BELT",
                    "cargo": [{"resource_type": "iron", "quantity": 3}],
                    "cargo_capacity": 20,
                    "stow_capacity": 10,
                    "stow_used": 1
                });
                if index == 0 {
                    device["features"] = serde_json::json!(["travel"]);
                    device["available_commands"] = serde_json::json!([
                        "enqueue_print",
                        "travel",
                        "change_owner",
                        "activate",
                        "deactivate",
                        "clear_queue",
                        "system_scan",
                        "retarget",
                        "start_mining",
                        "stellar_census"
                    ]);
                }
                device
            })
            .collect::<Vec<_>>();
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "devices": devices,
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (app, client) = test_app_at(&server.uri()).await;
        client
            .devices()
            .refresh_many()
            .collect()
            .await
            .expect("refresh devices");
        for index in 0..54 {
            let designation = if index == 0 {
                "SOL-BELT".to_owned()
            } else {
                format!("SOL-{index}")
            };
            let mut body = serde_json::json!({
                "location": designation,
                "location_type": if index == 0 { "asteroid_belt" } else { "planet" },
                "system": "SOL",
                "scanned": if index % 3 == 0 { Value::Null } else { Value::Bool(index % 2 == 0) }
            });
            if index == 1 {
                body["planet"] = serde_json::json!({
                    "scanned": false,
                    "magnetic_field": false,
                    "surface_gravity": 0.0
                });
            }
            Mock::given(method("GET"))
                .and(path(format!("/v1/locations/{designation}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .expect(1)
                .mount(&server)
                .await;
            client
                .locations()
                .get(&designation)
                .await
                .expect("refresh location");
        }

        let system = json(
            app.clone()
                .oneshot(
                    Request::get("/api/entities/system/SOL")
                        .body(Body::empty())
                        .expect("system request"),
                )
                .await
                .expect("system response"),
        )
        .await;
        assert_eq!(
            system["payload"]["detail"]["detail"]["children"]["total"],
            54
        );
        assert_eq!(
            system["payload"]["detail"]["detail"]["children"]["items"],
            serde_json::json!([])
        );
        assert!(
            system["payload"]["detail"]["detail"]["children"]["groups"]
                .as_array()
                .is_some_and(|groups| !groups.is_empty())
        );
        assert!(system["payload"]["provenance"].is_null());

        let location = json(
            app.clone()
                .oneshot(
                    Request::get("/api/entities/location/SOL-BELT")
                        .body(Body::empty())
                        .expect("location request"),
                )
                .await
                .expect("location response"),
        )
        .await;
        assert_eq!(
            location["payload"]["detail"]["detail"]["contents"]["total"],
            393
        );
        assert_eq!(
            location["payload"]["detail"]["detail"]["contents"]["items"],
            serde_json::json!([])
        );

        let planet = json(
            app.clone()
                .oneshot(
                    Request::get("/api/entities/location/SOL-1")
                        .body(Body::empty())
                        .expect("planet request"),
                )
                .await
                .expect("planet response"),
        )
        .await;
        let environment = &planet["payload"]["detail"]["detail"]["environment"];
        assert_eq!(environment["magnetic_field"], false);
        assert_eq!(environment["gravity_g"], 0.0);
        assert!(environment.get("atmosphere").is_none());
        assert_eq!(planet["payload"]["detail"]["detail"]["scanned"], false);

        let device = json(
            app.clone()
                .oneshot(
                    Request::get("/api/entities/device/D-000")
                        .body(Body::empty())
                        .expect("device request"),
                )
                .await
                .expect("device response"),
        )
        .await;
        let detail = &device["payload"]["detail"]["detail"];
        assert_eq!(detail["cargo_used"], 3);
        assert_eq!(detail["cargo_capacity"], 20);
        assert_eq!(detail["stow_used"], 1);
        assert_eq!(detail["stow_capacity"], 10);
        assert_eq!(
            detail["available_commands"].as_array().map(Vec::len),
            Some(10)
        );
        assert!(
            device["payload"]["provenance"]["observed_at_ms"]
                .as_i64()
                .is_some()
        );
        assert_eq!(
            device["payload"]["provenance"]["source_operation"],
            "GET /v1/devices"
        );

        let cargo = json(
            app.clone()
                .oneshot(
                    Request::get("/api/cargo")
                        .body(Body::empty())
                        .expect("cargo request"),
                )
                .await
                .expect("cargo response"),
        )
        .await;
        let carrier = cargo["payload"]["carriers"]
            .as_array()
            .and_then(|carriers| {
                carriers
                    .iter()
                    .find(|carrier| carrier["device"]["entity"]["id"] == "D-000")
            })
            .expect("device cargo carrier");
        assert_eq!(carrier["resources"], detail["cargo"]);
        assert_eq!(carrier["device"]["cargo_used"], detail["cargo_used"]);

        let snapshot = json(
            app.oneshot(
                Request::get("/api/snapshot")
                    .body(Body::empty())
                    .expect("snapshot request"),
            )
            .await
            .expect("snapshot response"),
        )
        .await;
        assert!(snapshot["payload"].get("entity_inspector").is_none());

        server.verify().await;
        client.close().await.expect("close client");
    }
    #[test]
    fn stowed_devices_inherit_their_host_location() {
        let mut host = projected_device("HOST");
        host.location = Some("SOL-HUB".to_owned());

        host.system = Some("SOL".to_owned());
        host.region = Some("solzone".to_owned());
        let mut child = projected_device("CHILD");
        child.stowed_in = Some("HOST".to_owned());
        let mut nested = projected_device("NESTED");
        nested.stowed_in = Some("CHILD".to_owned());
        let mut explicit = projected_device("EXPLICIT");
        explicit.stowed_in = Some("HOST".to_owned());
        explicit.location = Some("PHASYRIS-HUB".to_owned());
        explicit.system = Some("PHASYRIS".to_owned());
        let mut devices = vec![nested, child, host, explicit];

        inherit_stowed_locations(&mut devices);

        assert_eq!(devices[0].location.as_deref(), Some("SOL-HUB"));
        assert_eq!(devices[0].system.as_deref(), Some("SOL"));
        assert_eq!(devices[0].region.as_deref(), Some("solzone"));
        assert_eq!(devices[1].location.as_deref(), Some("SOL-HUB"));
        assert_eq!(devices[3].location.as_deref(), Some("PHASYRIS-HUB"));
    }

    #[test]
    fn event_projection_normalizes_progress_and_keeps_unknown_labels() {
        let event = serde_json::from_value::<LocationEvent>(serde_json::json!({
            "designation": "SOL-1-EVT-1",
            "location": "SOL-1",
            "title": "Anomaly",
            "event_type": "future_type",
            "category": "future_category",
            "status": "active",
            "criteria": [{
                "name": "supply",
                "resources": {"iron": 10},
                "devices": [{"device_type": "probe", "count": 2}]
            }],
            "progress": {
                "resources": {"iron": 4},
                "devices": {"probe": 1}
            },
            "rewards": {"resources": {"water": 3}, "xp": 5}
        }))
        .expect("event fixture");
        let snapshot = events_snapshot(
            SnapshotMetadata {
                revision: 1,
                generated_at_ms: 2,
            },
            vec![event],
        )
        .unwrap_or_else(|_| panic!("event projection"));
        let event = &snapshot.events[0];
        assert_eq!(event.event_type.as_deref(), Some("future_type"));
        assert_eq!(event.category.as_deref(), Some("future_category"));
        assert_eq!(event.system, "SOL");
        assert_eq!(
            event.criteria[0]
                .requirements
                .iter()
                .map(|item| (item.item.as_str(), item.completed, item.remaining))
                .collect::<Vec<_>>(),
            vec![("iron", 4, 6), ("probe", 1, 1)]
        );
        assert_eq!(event.rewards.resources[0].item, "water");
    }

    #[test]
    fn trade_projection_normalizes_nested_items_with_missing_optional_fields() {
        let controller = trade_controller_summary(
            TraderSummary {
                controller_code: "TC-1".to_owned(),
                ..TraderSummary::default()
            },
            vec![ShopTrade {
                trade_code: "TRD-1".to_owned(),
                criteria: Some(serde_json::json!({"resources": {"iron": 4}})),
                rewards: Some(serde_json::json!({"devices": {"probe": 1}})),
                ..ShopTrade::default()
            }],
            "available",
            None,
        );
        assert_eq!(controller.entity.id.0, "TC-1");
        assert_eq!(controller.shop_name, None);
        assert_eq!(controller.trades[0].current_stock, None);
        assert_eq!(controller.trades[0].requested[0].kind, "resource");
        assert_eq!(controller.trades[0].requested[0].quantity, Some(4.0));
        assert_eq!(controller.trades[0].offered[0].kind, "device");
    }

    fn bill_test_star(id: &str, x: f64, y: f64, z: f64) -> replicant_client::Star {
        replicant_client::Star {
            key: replicant_client::domain::StarKey::live(replicant_client::StarId::from(id)),
            name: None,
            spectral_type: None,
            entry_point: None,
            position: Some(replicant_client::domain::GalacticPosition { x, y, z }),
            has_hub: None,
            has_ward: None,
            knowledge_observed: true,
            explored: None,
            has_life: None,
            region: None,
        }
    }

    #[test]
    fn bill_audit_parser_finds_departure_vector_after_arrival_rows() {
        let audit = serde_json::json!({
            "audit": [
                {
                    "replicant_code": "A8F48B26",
                    "travel_type": "arrival",
                    "location": "SOL-5-L4",
                    "vector": "0.88,-0.04,-0.48"
                },
                {
                    "device_code": "6BE43B4B",
                    "device_type": "racing_vessel",
                    "replicant_code": "A8F48B26",
                    "travel_type": "departure",
                    "location": "SOL-5-L4",
                    "logged_at": "2026-08-21T10:57:44-04:00",
                    "vector": "0.97,0.10,-0.20"
                }
            ]
        });
        let departure = latest_bill_departure(&audit, "A8F48B26").expect("departure");
        assert_eq!(departure.origin_location, "SOL-5-L4");
        assert_eq!(departure.vessel_code.as_deref(), Some("6BE43B4B"));
        let norm = (departure.vector[0] * departure.vector[0]
            + departure.vector[1] * departure.vector[1]
            + departure.vector[2] * departure.vector[2])
            .sqrt();
        assert!((norm - 1.0).abs() < 1e-9);
    }

    #[test]
    fn bill_candidates_rank_stars_by_departure_ray() {
        let catalogue = vec![
            bill_test_star("SOL", 0.0, 0.0, 0.0),
            bill_test_star("BEST", 9.7, 1.0, -2.0),
            bill_test_star("SECOND", 19.0, 3.0, -4.0),
            bill_test_star("BEHIND", -9.7, -1.0, 2.0),
        ];
        let candidates = rank_bill_candidates(&catalogue, "SOL", [0.97, 0.10, -0.20]);
        assert_eq!(candidates[0].system, "BEST");
        assert!(
            candidates
                .iter()
                .all(|candidate| candidate.system != "BEHIND")
        );
        assert!(candidates[0].angular_error_deg < candidates[1].angular_error_deg);
    }

    #[test]
    fn bill_recommendation_requires_angular_separation() {
        let ambiguous = vec![
            BillCandidateSummary {
                system: "ONE".to_owned(),
                angular_error_deg: 0.10,
                distance_ly: 10.0,
                projected_distance_ly: 10.0,
                cross_track_ly: 0.02,
            },
            BillCandidateSummary {
                system: "TWO".to_owned(),
                angular_error_deg: 0.20,
                distance_ly: 20.0,
                projected_distance_ly: 20.0,
                cross_track_ly: 0.07,
            },
        ];
        let (recommended, confidence, is_ambiguous) = bill_recommendation(&ambiguous);
        assert!(recommended.is_none());
        assert_eq!(confidence, "low");
        assert!(is_ambiguous);

        let clear = vec![BillCandidateSummary {
            system: "ONE".to_owned(),
            angular_error_deg: 0.10,
            distance_ly: 10.0,
            projected_distance_ly: 10.0,
            cross_track_ly: 0.02,
        }];
        let (recommended, confidence, is_ambiguous) = bill_recommendation(&clear);
        assert_eq!(recommended.as_deref(), Some("ONE"));
        assert_eq!(confidence, "high");
        assert!(!is_ambiguous);
    }

    #[test]
    fn trade_details_classifies_missing_comms_as_partial_availability() {
        let mut details = replicant_client::ErrorDetails::default();
        details.message = Some("No replicant or comms device in this star system".to_owned());
        let error: replicant_runtime::ApplicationError = Box::new(ClientError::Contract {
            status: 403,
            details: Box::new(details),
        });
        assert_eq!(trade_details_status(&error), "out_of_comms");
    }

    #[tokio::test]
    async fn trade_route_returns_an_empty_typed_snapshot_without_replicants() {
        let (app, client, _) = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/trade")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let response = serde_json::from_value::<Versioned<TradeSnapshot>>(json(response).await)
            .expect("typed trade response");
        assert!(response.payload.controllers.is_empty());
        client.close().await.expect("close client");
    }

    #[test]
    fn intelligence_projections_normalize_actual_sdk_fields() {
        let metadata = SnapshotMetadata {
            revision: 1,
            generated_at_ms: 2,
        };
        let message = serde_json::from_value(serde_json::json!({
            "id": 1, "title": "Notice", "is_read": false
        }))
        .expect("message fixture");
        let message = inbox_message_summary(message);
        assert_eq!(message.is_read, Some(false));
        assert_eq!(message.title.as_deref(), Some("Notice"));

        let system_message = serde_json::from_value(serde_json::json!({
            "id": 2, "message": "hello", "replicant_code": null
        }))
        .expect("system message fixture");
        assert!(bobnet_message_summary(system_message, None).is_npc_or_system);

        // NPCs such as Riker/Bill can still have replicant codes. Classification
        // therefore comes from the include_npcs=true/false history differential.
        let npc_message = serde_json::from_value(serde_json::json!({
            "id": 3,
            "message": "Another meeting with the UN.",
            "replicant_code": "4BBA7CBE",
            "replicant_name": "Riker"
        }))
        .expect("NPC message fixture");
        let player_message = serde_json::from_value(serde_json::json!({
            "id": 4,
            "message": "hello from a player",
            "replicant_code": "PLAYER01",
            "replicant_name": "Player"
        }))
        .expect("player message fixture");
        let player_message_keys = BTreeSet::from([bobnet_message_identity(&player_message)]);
        assert!(bobnet_message_summary(npc_message, Some(&player_message_keys)).is_npc_or_system);
        assert!(
            !bobnet_message_summary(player_message, Some(&player_message_keys)).is_npc_or_system
        );

        let account: AccountMeResponse = serde_json::from_value(serde_json::json!({
            "name": "Operator",
            "bobnet_channels": ["general"],
            "replicants": [{"replicant_code": "R-1", "current_star": "SOL"}],
            "experience_points_total": 42
        }))
        .expect("account fixture");
        let network = network_snapshot(metadata.clone(), account.clone(), Vec::new());
        assert_eq!(network.replicants[0].entity.id.0, "R-1");
        let achievements = serde_json::from_value(serde_json::json!({
            "achievements": [{"achievement_key": "first-flight", "xp_reward": 5}]
        }))
        .expect("achievement fixture");
        let reputation = serde_json::from_value(serde_json::json!({
            "reputation": [{"species_key": "huwanu", "total_reputation": 3.5}]
        }))
        .expect("reputation fixture");
        let standing = build_standing_snapshot(metadata.clone(), account, achievements, reputation);
        assert_eq!(standing.experience_points_total, Some(42));
        assert_eq!(standing.civilisation_points, None);
        assert_eq!(standing.achievements[0].key, "first-flight");

        let index = serde_json::from_value(serde_json::json!({
            "boards": [{"key": "xp", "name": "XP"}]
        }))
        .expect("leaderboard index fixture");
        let board = serde_json::from_value(serde_json::json!({
            "board": "xp",
            "entries": [{"rank": 1, "replicant_code": "R-1", "value": 100}]
        }))
        .expect("leaderboard fixture");
        let leaderboards = leaderboards_snapshot(metadata, index, Some("xp".to_owned()), board);
        assert_eq!(
            leaderboards.entries[0]
                .replicant
                .as_ref()
                .map(|item| item.id.0.as_str()),
            Some("R-1")
        );
    }

    #[tokio::test]
    async fn mark_messages_read_updates_the_persisted_projection() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/messages"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "messages": [
                    {"id": 1, "title": "One", "is_read": false},
                    {"id": 2, "title": "Two", "is_read": false}
                ],
                "unread_message_count": 2
            })))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/v1/messages/read"))
            .and(body_json(serde_json::json!({"ids": [1]})))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"status": "ok"})),
            )
            .expect(1)
            .mount(&server)
            .await;

        let (app, client) = test_app_at(&server.uri()).await;
        let initial = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/messages")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("initial response");
        assert_eq!(initial.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/messages/read")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"ids":[1]}"#))
                    .expect("request"),
            )
            .await
            .expect("mark-read response");
        assert_eq!(response.status(), StatusCode::OK);
        let response = serde_json::from_value::<Versioned<MessagesSnapshot>>(json(response).await)
            .expect("typed message response");
        assert_eq!(response.payload.unread_count, Some(1));
        assert_eq!(response.payload.inbox[0].id, Some(2));
        assert_eq!(response.payload.inbox[0].is_read, Some(false));
        assert_eq!(response.payload.inbox[1].id, Some(1));
        assert_eq!(response.payload.inbox[1].is_read, Some(true));
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn intelligence_routes_are_typed_and_daemon_mediated() {
        let server = MockServer::start().await;
        for (route, body) in [
            ("/v1/messages", serde_json::json!({"messages": []})),
            (
                "/v1/accounts/me",
                serde_json::json!({"name": "Operator", "replicants": []}),
            ),
            (
                "/v1/accounts/achievements",
                serde_json::json!({"achievements": []}),
            ),
            (
                "/v1/accounts/reputation",
                serde_json::json!({"reputation": []}),
            ),
            (
                "/v1/leaderboards",
                serde_json::json!({"boards": [{"key": "xp"}]}),
            ),
            (
                "/v1/leaderboards/xp",
                serde_json::json!({"board": "xp", "entries": []}),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(route))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
        }
        let (app, client) = test_app_at(&server.uri()).await;
        for route in [
            "/api/reports",
            "/api/messages",
            "/api/bobnet",
            "/api/network",
            "/api/standing",
            "/api/leaderboards",
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(route)
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK, "{route}");
            assert_eq!(json(response).await["protocol_version"], 1);
        }
        client.close().await.expect("close client");
    }

    #[test]
    fn overview_projection_groups_active_and_attention_work() {
        let workflow = |id: &str, status| WorkflowSummary {
            id: ProtocolWorkflowId(id.to_owned()),
            kind: OperationKind("survey.route".to_owned()),
            status,
            current_step: Some("travel".to_owned()),
            revision: 1,
            updated_at_ms: 10,
        };
        let overview = build_overview_snapshot(
            SnapshotMetadata {
                revision: 7,
                generated_at_ms: 10,
            },
            DaemonHealth {
                status: HealthStatus::Healthy,
                daemon_version: "test".to_owned(),
                detail: None,
            },
            RuntimeSyncStatus {
                phase: SyncPhase::Ready,
                revision: 7,
                last_event_at_ms: None,
                detail: None,
            },
            AutomationStatus {
                automatic_triggers_enabled: true,
                workflows_paused: false,
            },
            vec![OverviewReplicant {
                entity: summary_ref(EntityKind::Replicant, "R-1".to_owned()),
                name: Some("Ada".to_owned()),
                system: Some("SOL".to_owned()),
                location: Some("EARTH".to_owned()),
                status: Some("idle".to_owned()),
            }],
            Vec::new(),
            vec![
                (workflow("running", ProtocolStatus::Running), false),
                (workflow("failed", ProtocolStatus::Failed), true),
            ],
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(overview.replicants.len(), 1);
        assert_eq!(overview.active_workflows.len(), 1);
        assert_eq!(overview.attention_workflows.len(), 1);
        assert_eq!(
            overview
                .workflow_counts
                .iter()
                .map(|item| item.count)
                .sum::<usize>(),
            2
        );
    }

    #[test]
    fn device_projection_normalizes_unknown_and_optional_fields() {
        use replicant_client::domain::{
            AccessScope, DeviceKey, DeviceRelationships, DeviceType, LocationKey,
            OperationalCapacity, ReplicantKey,
        };

        let device = Device {
            key: DeviceKey::live("D-1".into()),
            device_type: Some(DeviceType::from("future_device")),
            status: None,
            location: Some(LocationKey::live("EARTH".into())),
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: vec!["hauler".to_owned()],
            relationships: DeviceRelationships {
                assigned_replicant: Some(ReplicantKey::live("R-1".into())),
                ..DeviceRelationships::default()
            },
            cargo: Default::default(),
            cargo_capacity: None,
            attach_capacity: None,
            stow_capacity: Some(100),
            stow_used: Some(25),
            operational_capacity: OperationalCapacity::new(0.75),
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            access: AccessScope::Owned,
        };
        let row = device_summary(
            device,
            &BTreeMap::from([("EARTH".to_owned(), Some("SOL".to_owned()))]),
            &BTreeMap::from([("SOL".to_owned(), "solzone".to_owned())]),
            &BTreeMap::from([("R-1".to_owned(), "Ada".to_owned())]),
            Some(DeviceClaim {
                workflow_id: ProtocolWorkflowId("wf-1".to_owned()),
                workflow_kind: OperationKind("transport.route".to_owned()),
                workflow_status: ProtocolStatus::Running,
            }),
        )
        .expect("device projection");

        assert_eq!(row.device_type.as_deref(), Some("future_device"));
        assert_eq!(row.status, None);
        assert_eq!(row.owner.as_deref(), Some("R-1"));
        assert_eq!(row.system.as_deref(), Some("SOL"));
        assert_eq!(row.region.as_deref(), Some("solzone"));
        assert_eq!(row.owner_name.as_deref(), Some("Ada"));
        assert_eq!(row.operational_capacity_percent, Some(75.0));
        assert_eq!(row.claim.expect("claim").workflow_id.0, "wf-1");
        assert_eq!(
            device_system("THYFFAWFF-1-L4", &BTreeMap::new()).as_deref(),
            Some("THYFFAWFF")
        );
    }

    #[test]
    fn inventory_projection_aggregates_positive_resources_deterministically() {
        use replicant_client::domain::{AccountId, InventoryItem, LocationKey};

        let inventory = |location: &str, items: Vec<(&str, i64)>| Inventory {
            owner: InventoryOwner::Location(LocationKey::live(location.into())),
            location: Some(LocationKey::live(location.into())),
            items: items
                .into_iter()
                .map(|(resource, quantity)| InventoryItem {
                    resource: resource.to_owned(),
                    quantity,
                })
                .collect(),
        };
        let snapshot = inventory_snapshot(
            SnapshotMetadata {
                revision: 7,
                generated_at_ms: 10,
            },
            vec![
                Inventory {
                    owner: InventoryOwner::Account(AccountId::from("ACCOUNT")),
                    location: None,
                    items: vec![InventoryItem {
                        resource: "conductive".to_owned(),
                        quantity: 5,
                    }],
                },
                inventory("VEGA-2", vec![("silicates", 4), ("empty", 0)]),
                inventory(
                    "SOL-1",
                    vec![("conductive", 3), ("silicates", 2), ("silicates", 1)],
                ),
                inventory("EMPTY-1", Vec::new()),
            ],
            &BTreeMap::from([
                ("SOL-1".to_owned(), Some("SOL".to_owned())),
                ("VEGA-2".to_owned(), Some("VEGA".to_owned())),
            ]),
        );

        assert_eq!(snapshot.total_quantity, 15);
        assert_eq!(
            snapshot
                .locations
                .iter()
                .map(|row| row.location.as_deref().unwrap_or_default())
                .collect::<Vec<_>>(),
            ["", "SOL-1", "VEGA-2"]
        );
        assert_eq!(
            snapshot
                .resources
                .iter()
                .map(|row| (row.resource.as_str(), row.total_quantity))
                .collect::<Vec<_>>(),
            [("conductive", 8), ("silicates", 7)]
        );
        assert_eq!(snapshot.resources[1].distribution.len(), 2);
    }

    #[test]
    fn asset_projections_aggregate_factory_and_carrier_capacity() {
        let metadata = SnapshotMetadata {
            revision: 9,
            generated_at_ms: 10,
        };
        let factory = |code: &str, availability, queued_units| AutofactorySummary {
            device: projected_device(code),
            availability,
            queue_capacity: Some(4),
            queued_units,
            current_job: None,
            queued_jobs: Vec::new(),
        };
        let manufacturing = autofactory_snapshot(
            metadata.clone(),
            vec![
                factory("F-1", AutofactoryAvailability::Busy, 2),
                factory("F-2", AutofactoryAvailability::Available, 0),
                factory("F-3", AutofactoryAvailability::Unavailable, 0),
            ],
        );
        assert_eq!(manufacturing.utilization.queued_units, 2);
        assert_eq!(manufacturing.utilization.utilization_percent, 50.0);

        let mut device = projected_device("C-1");
        device.cargo_used = Some(3);
        device.cargo_capacity = Some(10);
        device.attach_capacity = Some(4);
        let cargo = cargo_snapshot(
            metadata,
            vec![CargoCarrierSummary {
                device,
                resources: vec![CargoResourceSummary {
                    resource: "silicates".to_owned(),
                    quantity: 3,
                }],
                attachment_used: 2,
            }],
        );
        assert_eq!((cargo.cargo_used, cargo.cargo_capacity), (3, 10));
        assert_eq!((cargo.attachment_used, cargo.attachment_capacity), (2, 4));
    }

    #[test]
    fn mining_projection_reports_partial_adopted_installations() {
        let mut controller = projected_device("MC-1");
        controller.device_type = Some("ami_mining_controller".to_owned());
        controller.system = Some("SOL".to_owned());
        controller.location = Some("SOL-BELT".to_owned());
        let mut miner = projected_device("MD-1");
        miner.device_type = Some("mining_drone".to_owned());
        miner.system = controller.system.clone();
        miner.location = controller.location.clone();
        miner.controller = Some("MC-1".to_owned());

        let rows = mining_installations(vec![controller, miner]);

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, MiningInstallationStatus::Partial);
        assert_eq!(rows[0].miners.len(), 1);
        assert!(
            rows[0]
                .missing
                .contains(&"3 adopted mining drones".to_owned())
        );
        assert!(rows[0].missing.contains(&"survey controller".to_owned()));
    }

    #[tokio::test]
    async fn relay_projection_reuses_coverage_and_registered_workflow_state() {
        use replicant_runtime::{
            relay::RelayExpansionRequest,
            workflows::{RelayWorkflowConfig, new_relay_workflow},
        };

        let (_, client, state) = test_app().await;
        let workflow = state
            .repository
            .create(new_relay_workflow(RelayWorkflowConfig {
                request: RelayExpansionRequest {
                    replicant: "R-1".to_owned(),
                    hub: "SOL-1".to_owned(),
                    targets: vec!["VEGA".to_owned()],
                    mission_file: PathBuf::from("relay.json"),
                    max_hop_ly: 7.499,
                    wait_timeout: Duration::from_secs(1),
                    unavailable_autofactories: Default::default(),
                },
            }))
            .expect("relay workflow");
        let mut deployed = projected_device("RELAY-1");
        deployed.device_type = Some("ftl_relay".to_owned());
        deployed.status = Some("active".to_owned());
        deployed.system = Some("SOL".to_owned());
        let mut staged = projected_device("RELAY-2");
        staged.device_type = Some("ftl_relay".to_owned());
        staged.tags = vec!["relay-m:test".to_owned()];
        let snapshot = relay_snapshot(
            SnapshotMetadata {
                revision: 4,
                generated_at_ms: 10,
            },
            vec![deployed, staged],
            vec![replicant_client::Star {
                key: replicant_client::domain::StarKey::live(replicant_client::StarId::from("SOL")),
                name: None,
                spectral_type: None,
                entry_point: None,
                position: Some(replicant_client::domain::GalacticPosition {
                    x: 0.0,
                    y: 0.0,
                    z: 0.0,
                }),
                has_hub: None,
                has_ward: None,
                knowledge_observed: false,
                explored: None,
                has_life: None,
                region: None,
            }],
            &[workflow],
        )
        .unwrap_or_else(|_| panic!("relay snapshot"));

        assert_eq!(snapshot.connected_systems, 1);
        assert_eq!(snapshot.relays[0].entity.id.0, "RELAY-1");
        assert_eq!(snapshot.staged_relays[0].entity.id.0, "RELAY-2");
        assert_eq!(snapshot.expansions[0].next_system.as_deref(), Some("VEGA"));
        client.close().await.expect("close client");
    }

    #[test]
    fn bootstrap_projection_distinguishes_active_and_completed_missions() {
        fn mission(phase: &str) -> BootstrapMission {
            serde_json::from_value(serde_json::json!({
                "version": 1,
                "mission_id": format!("BOOT-{phase}"),
                "mission_tag": "boot-m:test",
                "region_tag": "region:beta",
                "phase": phase,
                "region": "beta",
                "source_hub": "SOL-1",
                "source_system": "SOL",
                "source_entry": "SOL-ENTRY",
                "landing_star": "VEGA",
                "landing_entry": "VEGA-ENTRY",
                "operator": { "name": null },
                "explorer": { "name": null },
                "profile": {
                    "mining_setups": 5,
                    "autofactories": 3,
                    "cargo_freighters": 6,
                    "transport_controllers": 1,
                    "hub_maintenance_drones": 1,
                    "exploration_survey_drones": 2,
                    "root_relays": 1,
                    "expansion_relays": 0,
                    "ftl_beacons": 0,
                    "dedicated_surge_carriers": 0
                },
                "seed_quantity": 500,
                "quick_scout_radius_ly": 7.499,
                "survey_radius_ly": 30.0,
                "minimum_sites": 5,
                "maximum_sites": 9,
                "max_concurrency": 8,
                "print": { "requirements": {}, "submission_started": false, "queued": false },
                "assets": { "ftl_relay": ["RELAY-1"] },
                "carrier_target": 1,
                "seed_freighters": [],
                "carrier_loads": [{ "carrier": "C-1", "capacity": 10, "devices": ["RELAY-1"] }],
                "capital_system": null,
                "capital_belt": null,
                "capital_entry": null,
                "children": {
                    "quick_survey": "quick.json",
                    "initial_mining": "initial.json",
                    "survey": "survey.json",
                    "relays": "relays.json",
                    "mining": "mining.json"
                }
            }))
            .expect("bootstrap mission")
        }

        let active =
            bootstrap_mission_summary(mission("staged_at_source"), "EXEC-1".to_owned(), 10);
        let completed = bootstrap_mission_summary(mission("completed"), "EXEC-2".to_owned(), 20);
        assert!(!active.completed);
        assert_eq!((active.reserved_devices, active.loaded_devices), (1, 1));
        assert!(completed.completed);
    }

    #[tokio::test]
    async fn survey_projection_reads_registered_workflow_state() {
        use replicant_runtime::{
            survey::{SurveyMode, SurveyOptions},
            workflows::{SurveyWorkflowConfig, new_survey_workflow},
        };

        let (app, client, state) = test_app().await;
        let workflow = state
            .repository
            .create(new_survey_workflow(SurveyWorkflowConfig {
                options: SurveyOptions {
                    mode: SurveyMode::Run,
                    replicant: "R-1".to_owned(),
                    vessel: "V-1".to_owned(),
                    center: "SOL".to_owned(),
                    radius_ly: 10.0,
                    system_limit: 5,
                    target_systems: None,
                    star_detail_concurrency: 1,
                    mission_file: PathBuf::from("survey.json"),
                    controller: Some("SC-1".to_owned()),
                    drones: Some(vec!["SD-1".to_owned()]),
                    replace_plan: false,
                    include_explored: false,
                    travel_timeout: Duration::from_secs(1),
                    survey_timeout: Duration::from_secs(1),
                    maintenance_home: "SOL".to_owned(),
                    maintenance_interval: 40,
                    maintenance_threshold_pct: 25.0,
                    maintenance_resume_pct: 95.0,
                    maintenance_check_interval: Duration::from_secs(1),
                },
            }))
            .expect("survey workflow");
        let response = app
            .oneshot(
                Request::get("/api/missions/survey")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let value = json(response).await;
        assert_eq!(
            value["payload"]["missions"][0]["workflow"]["id"],
            workflow.id.to_string()
        );
        assert_eq!(value["payload"]["missions"][0]["center"], "SOL");
        assert_eq!(value["payload"]["missions"][0]["controller"], "SC-1");
        client.close().await.expect("close client");
    }

    #[test]
    fn queued_factory_jobs_accept_current_upstream_field_names() {
        let job = factory_job_from_queue(
            &[
                ("device_type".to_owned(), Value::String("relay".to_owned())),
                ("quantity".to_owned(), Value::from(2)),
                ("eta_seconds".to_owned(), Value::from(30.0)),
                ("tags".to_owned(), serde_json::json!(["network"])),
            ]
            .into_iter()
            .collect(),
        );
        assert_eq!(job.device_type, "relay");
        assert_eq!(job.quantity, 2);
        assert_eq!(job.eta_seconds, Some(30.0));
        assert_eq!(job.tags, ["network"]);
    }

    #[tokio::test]
    async fn health_snapshot_and_catalogue_are_frontend_safe() {
        let (app, client, _) = test_app().await;
        for path in [
            "/api/health",
            "/api/snapshot",
            "/api/overview",
            "/api/devices",
            "/api/inventory",
            "/api/autofactories",
            "/api/cargo",
            "/api/missions/survey",
            "/api/missions/mining",
            "/api/missions/relay",
            "/api/missions/bootstrap",
            "/api/entities",
            "/api/galaxy-scene",
            "/api/system-scene/SOL",
            "/api/descriptors",
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).expect("request"))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK);
            let value = json(response).await;
            assert_eq!(
                value["protocol_version"],
                replicant_protocol::PROTOCOL_VERSION
            );
            if path == "/api/entities" {
                assert!(value["payload"]["metadata"]["revision"].is_number());
                assert!(value["payload"]["entities"].is_array());
            }
            if path == "/api/devices" {
                assert!(value["payload"]["metadata"]["revision"].is_number());
                assert!(value["payload"]["devices"].is_array());
            }
            if path == "/api/inventory" {
                assert!(value["payload"]["metadata"]["revision"].is_number());
                assert!(value["payload"]["locations"].is_array());
                assert!(value["payload"]["resources"].is_array());
            }
            if path == "/api/descriptors" {
                assert!(
                    value["payload"]["actions"]
                        .as_array()
                        .is_some_and(|actions| actions.iter().any(|action| {
                            action["kind"] == "device.lifecycle.bulk"
                                && action["applicable_to"]
                                    .as_array()
                                    .is_some_and(Vec::is_empty)
                        }) && actions.iter().any(|action| {
                            action["kind"] == "device.refresh"
                                && action["applicable_to"]
                                    .as_array()
                                    .is_some_and(Vec::is_empty)
                        }))
                );
            }
            assert!(!value.to_string().contains("token"));
        }
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn settings_endpoint_reports_environment_without_secrets() {
        let (app, client, _) = test_app().await;
        let response = app
            .oneshot(
                Request::get("/api/settings")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let value = json(response).await;
        let payload = &value["payload"];
        assert_eq!(payload["profile"], "test");
        assert!(payload["managed_database_path"].is_string());
        assert!(payload["history_database_path"].is_string());
        assert!(payload["telemetry_database_path"].is_string());
        assert!(payload["runtime_database_path"].is_string());
        assert!(payload["bind_address"].is_string());
        assert!(payload["log_filter"].is_string());
        assert!(payload["docker"].is_boolean());
        assert!(
            ["environment", "secret_file", "unset"].contains(
                &payload["api_token_source"]
                    .as_str()
                    .expect("api token source")
            )
        );
        assert_eq!(payload["daemon_settings_require_restart"], true);
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn desktop_origin_can_reach_the_loopback_api() {
        let (app, client, _) = test_app().await;
        let response = app
            .oneshot(
                Request::builder()
                    .method("OPTIONS")
                    .uri("/api/health")
                    .header(header::ORIGIN, "tauri://localhost")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header("access-control-request-private-network", "true")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            response.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&"tauri://localhost".parse().expect("header value"))
        );
        assert_eq!(
            response
                .headers()
                .get("access-control-allow-private-network"),
            Some(&"true".parse().expect("header value"))
        );
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn repeated_blocked_workflows_collapse_to_one_notification_per_kind() {
        let (_app, client, state) = test_app().await;
        let mut failed = Vec::new();
        for owner in ["FIRST", "SECOND"] {
            let workflow = state
                .repository
                .create(NewWorkflow {
                    kind: WorkflowKind::new("scan.tour").unwrap(),
                    schema_version: 1,
                    config: Value::Null,
                    checkpoint: Value::Null,
                    current_step: None,
                    parent_id: None,
                })
                .unwrap();
            let workflow = state
                .repository
                .update(
                    workflow.id,
                    workflow.revision,
                    WorkflowState {
                        status: WorkflowStatus::Failed,
                        current_step: None,
                        checkpoint: Value::Null,
                        last_error: Some(format!(
                            "resource is already claimed by workflow {owner}"
                        )),
                        result: None::<Value>,
                    },
                )
                .unwrap();
            failed.push(workflow);
        }

        let notifications = operational_notifications(&failed, &[], &client.status());
        let blocked = notifications
            .iter()
            .filter(|notification| notification.title == "Blocked resource claim")
            .collect::<Vec<_>>();
        assert_eq!(blocked.len(), 1);
        assert!(
            blocked[0]
                .message
                .starts_with("scan.tour is blocked: resource is already claimed by workflow ")
        );
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn workflow_failure_payloads_and_notifications_redact_secrets() {
        let (app, client, state) = test_app().await;
        let workflow = state
            .repository
            .create(NewWorkflow {
                kind: WorkflowKind::new("test.secret-safety").unwrap(),
                schema_version: 1,
                config: serde_json::json!({ "api_token": "do-not-export", "system": "SOL" }),
                checkpoint: serde_json::json!({ "secret": "also-private" }),
                current_step: None,
                parent_id: None,
            })
            .unwrap();
        let failed = state
            .repository
            .update(
                workflow.id,
                workflow.revision,
                WorkflowState {
                    status: WorkflowStatus::Failed,
                    current_step: None,
                    checkpoint: serde_json::json!({ "secret": "also-private" }),
                    last_error: Some("authentication failed for do-not-export".to_owned()),
                    result: None::<Value>,
                },
            )
            .unwrap();

        let exported = serde_json::to_string(
            &detail(&state.repository, &failed).unwrap_or_else(|_| panic!("workflow detail")),
        )
        .unwrap();
        let notifications =
            serde_json::to_string(&operational_notifications(&[failed], &[], &client.status()))
                .unwrap();
        assert!(!exported.contains("do-not-export"));
        assert!(!exported.contains("also-private"));
        assert!(!notifications.contains("do-not-export"));
        assert!(exported.contains("[redacted]"));
        let entities = json(
            app.oneshot(
                Request::get("/api/entities")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response"),
        )
        .await;
        assert!(
            entities["payload"]["entities"]
                .as_array()
                .is_some_and(Vec::is_empty)
        );
        assert!(!entities.to_string().contains("do-not-export"));
        assert!(!entities.to_string().contains("also-private"));
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn workflow_routes_create_list_pause_and_report_stable_errors() {
        let (app, client, state) = test_app().await;
        let body = serde_json::json!({
            "kind": "relay.expansion",
            "parameters": {
                "replicant": "TEST-1",
                "hub": "SOL-HUB",
                "targets_csv": "ALPHA,BETA",
                "mission_file": "relay-test.json"
            }
        });
        let response = app
            .clone()
            .oneshot(
                Request::post("/api/workflows")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(body.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let created = json(response).await;
        let id = created["payload"]["workflow"]["id"]
            .as_str()
            .expect("workflow id");
        assert_eq!(
            state.repository.list().expect("persisted workflows").len(),
            1,
            "the daemon retains workflow ownership after the start request ends"
        );

        let response = app
            .clone()
            .oneshot(
                Request::post(format!("/api/workflows/{id}/pause"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            json(response).await["payload"]["workflow"]["status"],
            "paused"
        );

        for path in [
            format!("/api/workflows/{id}"),
            format!("/api/workflows/{id}/activity"),
        ] {
            let response = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).expect("request"))
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK);
        }

        for (command, status) in [("resume", "reconciling"), ("cancel", "cancelled")] {
            let response = app
                .clone()
                .oneshot(
                    Request::post(format!("/api/workflows/{id}/{command}"))
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                json(response).await["payload"]["workflow"]["status"],
                status
            );
        }

        let response = app
            .clone()
            .oneshot(
                Request::get("/api/workflows?status=cancelled")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            json(response).await["payload"]["workflows"]
                .as_array()
                .expect("list")
                .len(),
            1
        );

        let response = app
            .oneshot(
                Request::get("/api/workflows/not-a-uuid")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json(response).await["payload"]["code"], "invalid_request");
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn global_safety_controls_pause_disable_and_confirm_cancellation() {
        let (app, client, state) = test_app().await;
        let create_workflow = |kind: &str| {
            state.repository.create(NewWorkflow {
                kind: WorkflowKind::new(kind).expect("kind"),
                schema_version: 1,
                config: (),
                checkpoint: (),
                current_step: None,
                parent_id: None,
            })
        };
        let workflow = create_workflow("test.safety-one").expect("workflow");
        let other = create_workflow("test.safety-two").expect("other workflow");
        let control = |body: Value| {
            Request::post("/api/automation/control")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .expect("request")
        };

        let response = app
            .clone()
            .oneshot(control(serde_json::json!({ "action": "pause_all" })))
            .await
            .expect("pause response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            json(response).await["payload"]["automation"]["workflows_paused"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(
            state.repository.read(workflow.id).unwrap().unwrap().status,
            WorkflowStatus::Paused
        );

        let response = app
            .clone()
            .oneshot(control(serde_json::json!({ "action": "resume_all" })))
            .await
            .expect("resume response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            state.repository.read(workflow.id).unwrap().unwrap().status,
            WorkflowStatus::Reconciling
        );
        app.clone()
            .oneshot(control(serde_json::json!({ "action": "pause_all" })))
            .await
            .expect("second pause response");

        let response = app
            .clone()
            .oneshot(control(serde_json::json!({ "action": "disable_triggers" })))
            .await
            .expect("disable response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            !state
                .repository
                .automation_policy()
                .unwrap()
                .automatic_triggers_enabled
        );

        let response = app
            .clone()
            .oneshot(control(serde_json::json!({ "action": "cancel" })))
            .await
            .expect("unconfirmed response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = app
            .clone()
            .oneshot(control(serde_json::json!({
                "action": "cancel",
                "workflow_ids": [workflow.id.to_string()],
                "confirmed": true
            })))
            .await
            .expect("cancel response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            state.repository.read(workflow.id).unwrap().unwrap().status,
            WorkflowStatus::Cancelled
        );
        assert_eq!(
            state.repository.read(other.id).unwrap().unwrap().status,
            WorkflowStatus::Paused
        );
        let response = app
            .oneshot(control(serde_json::json!({
                "action": "cancel",
                "confirmed": true
            })))
            .await
            .expect("cancel all response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            state.repository.read(other.id).unwrap().unwrap().status,
            WorkflowStatus::Cancelled
        );
        assert!(!matches!(client.status(), ClientStatus::Closed));
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn trigger_routes_crud_and_manual_fire_are_durable() {
        let (app, client, state) = test_app().await;
        let target = serde_json::json!({
            "operation_class": "workflow",
            "kind": "relay.expansion",
            "parameters": {
                "replicant": "TEST-1",
                "hub": "SOL-HUB",
                "targets_csv": "ALPHA,BETA",
                "mission_file": "relay-test.json"
            }
        });
        let create = serde_json::json!({
            "name": "manual relay",
            "condition": { "kind": "manual" },
            "target": target,
            "enabled": false
        });
        let response = app
            .clone()
            .oneshot(
                Request::post("/api/triggers")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(create.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::CREATED);
        let created = json(response).await;
        let id = created["payload"]["id"].as_str().expect("trigger id");

        let update = serde_json::json!({
            "expected_revision": 0,
            "name": "manual relay",
            "condition": { "kind": "manual" },
            "target": target,
            "enabled": true
        });
        let response = app
            .clone()
            .oneshot(
                Request::put(format!("/api/triggers/{id}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(update.to_string()))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(json(response).await["payload"]["enabled"], true);

        let response = app
            .clone()
            .oneshot(
                Request::post(format!("/api/triggers/{id}/fire"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(json(response).await["payload"]["last_fired_at_ms"].is_number());
        assert_eq!(state.repository.list().expect("workflows").len(), 1);

        let response = app
            .clone()
            .oneshot(
                Request::get("/api/triggers")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            json(response).await["payload"]["triggers"]
                .as_array()
                .expect("triggers")
                .len(),
            1
        );

        let response = app
            .oneshot(
                Request::delete(format!("/api/triggers/{id}"))
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        assert!(
            state
                .repository
                .list_triggers()
                .expect("triggers")
                .is_empty()
        );
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn websocket_connects_delivers_updates_and_resnapshots_on_reconnect() {
        let (app, client, state) = test_app().await;
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve test app");
        });
        let url = format!("ws://{address}/ws");

        let (mut socket, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("connect websocket");
        let initial = next_live(&mut socket).await;
        assert!(matches!(initial.delta, LiveDelta::Snapshot(_)));

        state.notify(Notification {
            id: EntityId("test-notification".into()),
            level: NotificationLevel::Info,
            title: "Test".into(),
            message: "runtime changed".into(),
            created_at_ms: 1,
        });
        let update = next_live(&mut socket).await;
        assert!(matches!(update.delta, LiveDelta::Notification(_)));
        assert!(update.revision > initial.revision);
        drop(socket);

        let (mut socket, _) = tokio_tungstenite::connect_async(&url)
            .await
            .expect("reconnect websocket");
        let resnapshot = next_live(&mut socket).await;
        assert!(matches!(resnapshot.delta, LiveDelta::Snapshot(_)));
        assert_eq!(resnapshot.revision, update.revision);

        server.abort();
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn slow_client_is_told_to_resnapshot() {
        let (_, client, state) = test_app().await;
        for _ in 0..3 {
            let mut updates = state.live.subscribe();
            for _ in 0..=LIVE_BUFFER {
                state.publish(LiveDelta::DomainInvalidated {
                    slice: DomainSlice::Universe,
                });
            }
            assert!(matches!(
                updates.recv().await,
                Err(broadcast::error::RecvError::Lagged(_))
            ));
            let message = state
                .resnapshot_message()
                .unwrap_or_else(|_| panic!("resnapshot"));
            assert!(matches!(message.delta, LiveDelta::Snapshot(_)));
        }
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn workflow_lifecycle_and_activity_are_published() {
        let (_, client, state) = test_app().await;
        let mut updates = state.live.subscribe();
        let workflow = state
            .repository
            .create(NewWorkflow {
                kind: WorkflowKind::new("test.workflow").expect("kind"),
                schema_version: 1,
                config: (),
                checkpoint: (),
                current_step: Some("start".into()),
                parent_id: None,
            })
            .expect("create workflow");
        state
            .repository
            .append_activity(workflow.id, "started")
            .expect("append activity");
        let missing = WorkflowId::new().to_string();
        let mut revisions = BTreeMap::from([(missing.clone(), 0)]);
        publish_workflow_updates(&state, &mut revisions, &mut 0);
        assert!(!revisions.contains_key(&missing));

        assert!(matches!(
            updates.recv().await.expect("workflow update").delta,
            LiveDelta::WorkflowCreated(_)
        ));
        assert!(matches!(
            updates.recv().await.expect("activity update").delta,
            LiveDelta::WorkflowActivity(_)
        ));

        // Invalidations are coalesced: one flush carries every slice the
        // workflow change and its activity touched, each with the revision it
        // reached, instead of one message per slice.
        state.flush_invalidations();
        let LiveDelta::DomainsInvalidated { slices } =
            updates.recv().await.expect("coalesced invalidation").delta
        else {
            panic!("expected a coalesced domain invalidation");
        };
        for expected in [
            DomainSlice::Entities,
            DomainSlice::Workflows,
            DomainSlice::Overview,
            DomainSlice::Devices,
            DomainSlice::Autofactories,
            DomainSlice::Cargo,
            DomainSlice::Missions,
        ] {
            assert!(slices.contains_key(&expected), "missing slice {expected:?}");
        }
        assert!(
            slices
                .values()
                .all(|revision| *revision == state.revision.load(Ordering::Relaxed))
        );
        assert_eq!(state.slice_revisions(), slices);
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn bootstrap_action_completion_invalidates_missions() {
        let (_, client, state) = test_app().await;
        let mut updates = state.live.subscribe();
        let _ = operation_response(
            &state,
            FiniteExecutionClass::Action,
            "bootstrap.run",
            1,
            Ok(Value::Null),
        )
        .unwrap_or_else(|_| panic!("operation response"));
        let LiveDelta::DomainsInvalidated { slices } =
            updates.recv().await.expect("mission invalidation").delta
        else {
            panic!("expected a coalesced domain invalidation");
        };
        assert!(slices.contains_key(&DomainSlice::Missions));
        client.close().await.expect("close client");
    }

    #[test]
    fn non_loopback_binds_require_a_token() {
        let mut config = test_daemon_config();
        config.bind = "0.0.0.0:8080".parse().expect("wildcard bind");
        assert!(matches!(
            config.validate(),
            Err(ConfigError::MissingToken(_))
        ));
        config.token = Some("secret".to_owned());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn tokens_are_compared_in_full_and_exactly() {
        let mut config = test_daemon_config();
        assert!(
            config.authorized(None),
            "no token configured accepts anyone"
        );
        config.token = Some("secret".to_owned());
        assert!(config.authorized(Some("secret")));
        assert!(!config.authorized(Some("secre")), "prefixes are rejected");
        assert!(!config.authorized(Some("secretx")));
        assert!(!config.authorized(None));
    }

    #[tokio::test]
    async fn requests_without_a_token_are_rejected_but_health_stays_open() {
        let (_, client, state) = test_app().await;
        let mut config = state.daemon.clone();
        config.token = Some("secret".to_owned());
        let guarded = AppState::new(
            client.clone(),
            RuntimeConfig::new("test"),
            state.repository.clone(),
            config,
        )
        .expect("guarded state");
        let app = router(guarded);

        let unauthorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/snapshot")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let authorized = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/snapshot")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_ne!(authorized.status(), StatusCode::UNAUTHORIZED);

        // WebSocket clients cannot set headers, so a query token is accepted.
        let query = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/snapshot?token=secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_ne!(query.status(), StatusCode::UNAUTHORIZED);

        // Health backs container health checks and must not need credentials.
        let health = app
            .oneshot(
                Request::builder()
                    .uri("/api/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(health.status(), StatusCode::OK);
        client.close().await.expect("close client");
    }

    #[test]
    fn default_bind_is_loopback() {
        assert!(is_loopback(DEFAULT_BIND.parse().expect("default bind")));
    }

    #[test]
    fn degraded_health_identifies_the_managed_failure() {
        assert_eq!(
            status_detail(&ClientStatus::Degraded(
                ClientDegradation::StartupIncomplete
            )),
            Some("managed startup synchronization is incomplete")
        );
        assert_eq!(
            status_detail(&ClientStatus::Degraded(ClientDegradation::EventContinuity)),
            Some("managed event continuity is degraded")
        );
    }
}
