//! HTTP query/command API for the local `replicantd` process.

use std::{
    collections::BTreeMap,
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
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use replicant_client::{
    ClientDegradation, ClientStatus,
    domain::{Device, Inventory, InventoryOwner},
    managed::{Client, OperationStatus as ManagedOperationStatus},
    raw::{
        accounts::{AccountAchievementListResponse, AccountMeResponse},
        bobnet::{DeviceChannelsResponse, DeviceMessagesResponse},
        events::LocationEvent,
        leaderboards::{LeaderboardIndexResponse, LeaderboardResponse},
        messages::MessageListResponse,
        reputation::AccountReputationResponse,
    },
};
use replicant_event_planner::remaining_requirements;
use replicant_protocol::{
    AccountReplicantSummary, AchievementSummary, ActivityLevel, AutofactoryAvailability,
    AutofactorySnapshot, AutofactorySummary, AutofactoryUtilization, AutomationControlAction,
    AutomationControlRequest, AutomationControlResponse, AutomationStatus,
    AutomationTrigger as ProtocolTrigger, BobnetChannelSummary, BobnetMessageSummary,
    BootstrapMissionSummary, BootstrapSnapshot, CargoCarrierSummary, CargoResourceSummary,
    CargoSnapshot, CreateTriggerRequest, DaemonHealth, DescriptorCatalog, DeviceClaim,
    DeviceSummary, DevicesSnapshot, DomainSlice, EntityId, EntityIndexSnapshot, EntityKind,
    EntityRef, EntitySummary, ErrorResponse, EventCriterionSummary, EventRequirementKind,
    EventRequirementSummary, EventRewardItem, EventRewardsSummary, EventSummary, EventsSnapshot,
    FactoryJobSummary, FiniteExecution as ProtocolFiniteExecution, FiniteExecutionHistoryResponse,
    FiniteExecutionStatus as ProtocolFiniteExecutionStatus, GalaxySceneSnapshot, HealthStatus,
    InboxMessageSummary, InventoryDistribution, InventoryLocationSummary, InventoryOwnerKind,
    InventoryQuantity, InventoryResourceSummary, InventorySnapshot, LeaderboardBoardSummary,
    LeaderboardEntrySummary, LeaderboardsSnapshot, LiveDelta, LiveMessage, MessagesSnapshot,
    MiningInstallationStatus, MiningInstallationSummary, MiningSnapshot, NetworkRelaySummary,
    NetworkSnapshot, Notification, NotificationLevel, OperationClass, OperationKind,
    OperationStatus, OperationUpdate, OverviewReplicant, OverviewSnapshot, OverviewTravel,
    RelayExpansionSummary, RelaySnapshot, ReportsSnapshot, ReputationSummary, RequirementSummary,
    ResultSummary, RunOperationRequest, RunOperationResponse, RuntimeSnapshot, RuntimeSyncStatus,
    SnapshotMetadata, StandingSnapshot, StartWorkflowRequest, StartWorkflowResponse,
    SurveyMissionSummary, SurveySnapshot, SyncPhase, SystemSceneSnapshot, TradeControllerSummary,
    TradeItemSummary, TradeSnapshot, TradeSummary, TriggerCondition as ProtocolTriggerCondition,
    TriggerId as ProtocolTriggerId, TriggerListResponse, TriggerTarget as ProtocolTriggerTarget,
    UpdateTriggerRequest, Versioned, WorkflowActivity, WorkflowActivityResponse,
    WorkflowControlResponse, WorkflowDetail, WorkflowId as ProtocolWorkflowId,
    WorkflowListResponse, WorkflowStatus as ProtocolStatus, WorkflowStatusCount, WorkflowSummary,
};
use replicant_runtime::{
    ApplicationContext,
    bootstrap::BootstrapMission,
    catalogue::{CatalogueError, OperationCatalogue},
    config::RuntimeConfig,
    event::{discovered_events, normalize_event},
    galaxy_scene::galaxy_scene as build_galaxy_scene,
    intelligence::{
        account_profile, inbox, leaderboard, leaderboard_index, relay_history, standing,
    },
    requirements::{AvailabilityKind, InfrastructureKind, RequirementScope, RequirementTarget},
    survey::summarize_plan,
    system_scene::system_scene as build_system_scene,
    trade::{ShopTrade, TraderSummary, shop_trades, trade_viewers, trader_directory},
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
    WorkflowRepository, WorkflowStatus, WorkflowSupervisor,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::sync::{Mutex, broadcast, watch};
use tower_http::cors::CorsLayer;

const LIVE_BUFFER: usize = 32;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(45);

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
    /// Local HTTP listen address.
    pub bind: SocketAddr,
}

impl DaemonConfig {
    /// Loads configuration from the process environment.
    pub fn from_env() -> Result<Self, ConfigError> {
        let profile = env::var("REPLICANT_PROFILE").unwrap_or_else(|_| "default".to_owned());
        let managed_database = env::var_os("REPLICANT_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("replicant-client.sqlite"));
        let runtime_database = env::var_os("REPLICANT_RUNTIME_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("replicant-runtime.sqlite"));
        let bind = env::var("REPLICANTD_BIND")
            .unwrap_or_else(|_| DEFAULT_BIND.to_owned())
            .parse()
            .map_err(ConfigError::Bind)?;
        Ok(Self {
            profile,
            managed_database,
            runtime_database,
            bind,
        })
    }
}

/// Invalid daemon configuration.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    /// The configured listen address is invalid.
    #[error("invalid REPLICANTD_BIND: {0}")]
    Bind(std::net::AddrParseError),
}

/// Shared daemon services. HTTP handlers never construct managed clients.
pub struct AppState {
    context: ApplicationContext,
    repository: Arc<WorkflowRepository>,
    supervisor: Mutex<WorkflowSupervisor>,
    catalogue: OperationCatalogue,
    live: broadcast::Sender<LiveMessage>,
    revision: AtomicU64,
    publish_lock: StdMutex<()>,
}

impl AppState {
    /// Builds daemon state around one managed client and one runtime repository.
    pub fn new(
        client: Client,
        runtime_config: RuntimeConfig,
        repository: Arc<WorkflowRepository>,
    ) -> Result<Arc<Self>, CatalogueError> {
        let catalogue = OperationCatalogue::new()?;
        let supervisor = WorkflowSupervisor::with_managed_client(
            repository.clone(),
            catalogue.workflow_registry(),
            client.clone(),
        );
        let revision = client.state().revision().unwrap_or_default();
        Ok(Arc::new(Self {
            context: ApplicationContext::new(client, runtime_config),
            repository,
            supervisor: Mutex::new(supervisor),
            catalogue,
            live: broadcast::channel(LIVE_BUFFER).0,
            revision: AtomicU64::new(revision),
            publish_lock: StdMutex::new(()),
        }))
    }

    /// Returns the daemon's single managed client.
    #[must_use]
    pub fn client(&self) -> &Client {
        self.context.client()
    }

    /// Publishes a frontend-safe runtime notification.
    pub fn notify(&self, notification: replicant_protocol::Notification) {
        self.publish(LiveDelta::Notification(notification));
    }

    fn publish(&self, delta: LiveDelta) {
        let _guard = self
            .publish_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let revision = self.revision.fetch_add(1, Ordering::Relaxed) + 1;
        let _ = self.live.send(LiveMessage::current(revision, delta));
    }

    fn snapshot_metadata(&self) -> Result<SnapshotMetadata, ApiError> {
        let _guard = self
            .publish_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(SnapshotMetadata {
            revision: self.revision.load(Ordering::Relaxed),
            generated_at_ms: now_millis()?,
        })
    }

    fn resnapshot_message(&self) -> Result<LiveMessage, ApiError> {
        let metadata = self.snapshot_metadata()?;
        Ok(LiveMessage::current(
            metadata.revision,
            LiveDelta::Snapshot(metadata),
        ))
    }
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
        .route("/api/trade", get(trade))
        .route("/api/reports", get(reports))
        .route("/api/messages", get(messages))
        .route("/api/network", get(network))
        .route("/api/standing", get(standing_snapshot))
        .route("/api/leaderboards", get(leaderboards))
        .route("/api/entities", get(entity_index))
        .route("/api/galaxy-scene", get(galaxy_scene))
        .route("/api/system-scene/{system}", get(system_scene))
        .route("/ws", get(websocket))
        .route("/api/descriptors", get(descriptors))
        .route("/api/reports/{kind}", post(run_report))
        .route("/api/actions/{kind}", post(run_action))
        .route("/api/history", get(finite_execution_history))
        .route("/api/triggers", get(list_triggers).post(create_trigger))
        .route(
            "/api/triggers/{id}",
            put(update_trigger).delete(delete_trigger),
        )
        .route("/api/triggers/{id}/fire", post(fire_trigger))
        .route("/api/automation/control", post(control_automation))
        .route("/api/workflows", get(list_workflows).post(start_workflow))
        .route("/api/workflows/{id}", get(workflow_detail))
        .route("/api/workflows/{id}/activity", get(workflow_activity))
        .route("/api/workflows/{id}/pause", post(pause_workflow))
        .route("/api/workflows/{id}/resume", post(resume_workflow))
        .route("/api/workflows/{id}/cancel", post(cancel_workflow))
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
                .allow_headers([axum::http::header::CONTENT_TYPE]),
        )
        .with_state(state)
}

/// Runs periodic persisted-workflow reconciliation until shutdown.
pub async fn run_supervisor(state: Arc<AppState>, mut shutdown: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    let mut revisions = state.client().state().watch().ok();
    let mut operations = state.client().operations().watch().ok();
    let mut workflows = state
        .repository
        .list()
        .unwrap_or_default()
        .into_iter()
        .map(|workflow| (workflow.id.to_string(), workflow.revision))
        .collect::<BTreeMap<_, _>>();
    let mut activity_cursor = state.repository.latest_activity_id().unwrap_or_default();
    let mut managed_phase = sync_phase(&state.client().status());
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(error) = state.supervisor.lock().await.tick().await {
                    tracing::error!(error = %error, "workflow supervisor tick failed");
                }
                publish_workflow_updates(&state, &mut workflows, &mut activity_cursor);
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
            revision = async { revisions.as_mut().expect("guarded").next().await }, if revisions.is_some() => {
                match revision {
                    Ok(_) => {
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
                            DomainSlice::Trade,
                            DomainSlice::Messages,
                            DomainSlice::Network,
                            DomainSlice::Standing,
                            DomainSlice::Leaderboards,
                        ] {
                            state.publish(LiveDelta::DomainInvalidated { slice });
                        }
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "managed state watcher stopped");
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

/// Evaluates durable automation definitions from local managed events and projections.
pub async fn run_trigger_engine(state: Arc<AppState>, mut shutdown: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut events = state.client().events().watch().await.ok();
    let mut revisions = state.client().state().watch().ok();
    evaluate_schedules_and_parents(&state).await;
    evaluate_event_triggers(&state).await;
    evaluate_state_triggers(&state).await;
    loop {
        tokio::select! {
            _ = interval.tick() => {
                evaluate_schedules_and_parents(&state).await;
                evaluate_event_triggers(&state).await;
            }
            event = async { events.as_mut().expect("guarded").next().await }, if events.is_some() => {
                if let Err(error) = event {
                    tracing::warn!(error = %error, "trigger event watcher lagged; recovering from durable history");
                    events = state.client().events().watch().await.ok();
                }
                evaluate_event_triggers(&state).await;
            }
            revision = async { revisions.as_mut().expect("guarded").next().await }, if revisions.is_some() => {
                if let Err(error) = revision {
                    tracing::warn!(error = %error, "trigger state watcher stopped");
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

async fn evaluate_event_triggers(state: &Arc<AppState>) {
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
    state: &AppState,
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
            let started_at = now_millis().map_err(|error| error.message)?;
            match state
                .catalogue
                .run_action(
                    state.client(),
                    &trigger.target.kind,
                    trigger.target.parameters.clone(),
                )
                .await
            {
                Ok(result) => {
                    let result = sanitize_result(result);
                    let (summary, status) = summarize_result(&result);
                    persist_execution(
                        state,
                        FiniteExecutionClass::Action,
                        &trigger.target.kind,
                        status,
                        started_at,
                        Some(&result),
                        None,
                        summary,
                    );
                    Ok(())
                }
                Err(error) => {
                    persist_execution(
                        state,
                        FiniteExecutionClass::Action,
                        &trigger.target.kind,
                        StoredFiniteExecutionStatus::Failed,
                        started_at,
                        None,
                        Some("triggered action failed"),
                        ResultSummary {
                            failed: 1,
                            ..ResultSummary::default()
                        },
                    );
                    Err(error.to_string())
                }
            }
        }
    }
}

fn publish_workflow_updates(
    state: &AppState,
    revisions: &mut BTreeMap<String, u64>,
    activity_cursor: &mut i64,
) {
    if let Ok(current) = state.repository.list() {
        for workflow in current {
            let delta = match revisions.insert(workflow.id.to_string(), workflow.revision) {
                None => Some(LiveDelta::WorkflowCreated(summary(&workflow))),
                Some(revision) if revision != workflow.revision => {
                    Some(LiveDelta::WorkflowUpdated(summary(&workflow)))
                }
                _ => None,
            };
            if let Some(delta) = delta {
                state.publish(delta);
                state.publish(LiveDelta::DomainInvalidated {
                    slice: DomainSlice::Entities,
                });
                state.publish(LiveDelta::DomainInvalidated {
                    slice: DomainSlice::Workflows,
                });
                state.publish(LiveDelta::DomainInvalidated {
                    slice: DomainSlice::Overview,
                });
                state.publish(LiveDelta::DomainInvalidated {
                    slice: DomainSlice::Devices,
                });
                state.publish(LiveDelta::DomainInvalidated {
                    slice: DomainSlice::Autofactories,
                });
                state.publish(LiveDelta::DomainInvalidated {
                    slice: DomainSlice::Cargo,
                });
                state.publish(LiveDelta::DomainInvalidated {
                    slice: DomainSlice::Missions,
                });
                if let Some(notification) = workflow_notification(&workflow) {
                    state.notify(notification);
                }
            }
        }
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
                state.publish(LiveDelta::DomainInvalidated {
                    slice: DomainSlice::Overview,
                });
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
        _ => OperationStatus::Ambiguous,
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
                Ok(update) if update.revision <= initial_revision => {}
                Ok(update) => if send_live(&mut socket, update).await.is_err() { break; },
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    if let Ok(message) = state.resnapshot_message() {
                        let _ = send_live(&mut socket, message).await;
                    }
                    let _ = socket.send(Message::Close(None)).await;
                    break;
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
    recent_activity.sort_by(|left, right| right.id.cmp(&left.id));
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
    let devices = device_rows(&state).await?;
    Ok(Json(Versioned::current(DevicesSnapshot {
        metadata,
        devices,
    })))
}

async fn device_rows(state: &Arc<AppState>) -> Result<Vec<DeviceSummary>, ApiError> {
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
            &replicant_names,
            claims.remove(handle.id().as_str()),
        ));
    }
    rows.sort_by(|left, right| left.entity.cmp(&right.entity));
    Ok(rows)
}

async fn autofactories(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<AutofactorySnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let mut factories = Vec::new();
    for device in device_rows(&state)
        .await?
        .into_iter()
        .filter(|device| device.device_type.as_deref() == Some("autofactory"))
    {
        let detail = state
            .client()
            .raw()
            .devices()
            .get(&device.entity.id.0)
            .await
            .map_err(|_| ApiError::unavailable())?
            .value;
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
    let mut carriers = Vec::new();
    for device in device_rows(&state).await?.into_iter().filter(|device| {
        device.cargo_capacity.unwrap_or_default() > 0
            || device.attach_capacity.unwrap_or_default() > 0
            || !device.attached_devices.is_empty()
            || !device.stowed_devices.is_empty()
    }) {
        let detail = state
            .client()
            .raw()
            .devices()
            .get(&device.entity.id.0)
            .await
            .map_err(|_| ApiError::unavailable())?
            .value;
        let mut resources = detail
            .cargo
            .into_iter()
            .filter_map(|item| {
                let quantity = item.quantity.unwrap_or_default();
                if quantity <= 0 {
                    return None;
                }
                Some(CargoResourceSummary {
                    resource: item.resource_type?,
                    quantity,
                })
            })
            .collect::<Vec<_>>();
        resources.sort_by(|left, right| left.resource.cmp(&right.resource));
        carriers.push(CargoCarrierSummary {
            attachment_used: i64::try_from(device.attached_devices.len()).unwrap_or(i64::MAX),
            device,
            resources,
        });
    }
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
    let mut fleet_codes = std::collections::BTreeSet::new();
    let mut missions = Vec::new();
    for workflow in state.repository.list().map_err(ApiError::repository)? {
        if workflow.kind.as_str() != "survey.route" || !active_workflow(workflow.status) {
            continue;
        }
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
    missions.sort_by(|left, right| left.workflow.id.cmp(&right.workflow.id));
    Ok(Json(Versioned::current(SurveySnapshot {
        metadata,
        missions,
        fleet: devices
            .into_iter()
            .filter(|device| fleet_codes.contains(&device.entity.id.0))
            .collect(),
    })))
}

async fn mining_missions(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<MiningSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let workflows = state
        .repository
        .list()
        .map_err(ApiError::repository)?
        .into_iter()
        .filter(|workflow| {
            active_workflow(workflow.status)
                && (workflow.kind.as_str().contains("mining")
                    || workflow
                        .current_step
                        .as_deref()
                        .is_some_and(|step| step.contains("mining")))
        })
        .map(|workflow| summary(&workflow))
        .collect();
    Ok(Json(Versioned::current(MiningSnapshot {
        metadata,
        installations: mining_installations(device_rows(&state).await?),
        workflows,
    })))
}

async fn relay_missions(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<RelaySnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let workflows = state.repository.list().map_err(ApiError::repository)?;
    let scene = build_galaxy_scene(
        state.client(),
        &workflows,
        metadata.revision,
        metadata.generated_at_ms,
    )
    .await
    .map_err(|_| ApiError::unavailable())?;
    Ok(Json(Versioned::current(relay_snapshot(
        metadata,
        device_rows(&state).await?,
        scene,
        &workflows,
    )?)))
}

fn relay_snapshot(
    metadata: SnapshotMetadata,
    devices: Vec<DeviceSummary>,
    scene: GalaxySceneSnapshot,
    workflows: &[WorkflowInstance],
) -> Result<RelaySnapshot, ApiError> {
    const RELAY_TYPES: [&str; 3] = ["ftl_relay", "system_hub", "deep_space_relay_station"];
    let connected = scene
        .stars
        .iter()
        .filter(|star| star.has_relay)
        .map(|star| star.id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
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
            && device
                .system
                .as_deref()
                .is_some_and(|system| connected.contains(system))
            && device
                .status
                .as_deref()
                .is_some_and(|status| matches!(status, "active" | "relaying"))
    };
    let relays = devices.iter().filter(deployed).cloned().collect::<Vec<_>>();
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
                || device
                    .claim
                    .as_ref()
                    .is_some_and(|claim| claim.workflow_kind.0 == "relay.expansion")
        })
        .cloned()
        .collect();
    let mut expansions = Vec::new();
    for workflow in workflows.iter().filter(|workflow| {
        workflow.kind.as_str() == "relay.expansion" && active_workflow(workflow.status)
    }) {
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
    expansions.sort_by(|left, right| left.workflow.id.cmp(&right.workflow.id));
    Ok(RelaySnapshot {
        metadata,
        relays,
        staged_relays,
        connected_systems: connected.len(),
        relay_edges: scene.relay_edges,
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

async fn trade(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<TradeSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let Some(viewer) = trade_viewers(state.client())
        .await
        .map_err(|_| ApiError::unavailable())?
        .into_iter()
        .next()
    else {
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
    let workflows = state
        .repository
        .list()
        .map_err(ApiError::repository)?
        .into_iter()
        .map(|workflow| (workflow.id.to_string(), summary(&workflow)))
        .collect::<BTreeMap<_, _>>();
    let mut controllers = Vec::with_capacity(traders.len());
    for trader in traders {
        let trades = shop_trades(state.client(), &trader.controller_code)
            .await
            .map_err(|_| ApiError::unavailable())?;
        let workflow = devices
            .iter()
            .find(|device| device.entity.id.0 == trader.controller_code)
            .and_then(|device| device.claim.as_ref())
            .and_then(|claim| workflows.get(&claim.workflow_id.0))
            .cloned();
        controllers.push(trade_controller_summary(trader, trades, workflow));
    }
    Ok(Json(Versioned::current(TradeSnapshot {
        metadata,
        viewer: Some(summary_ref(EntityKind::Replicant, viewer_code)),
        controllers,
    })))
}

fn trade_controller_summary(
    trader: TraderSummary,
    trades: Vec<ShopTrade>,
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

#[derive(Deserialize)]
struct MessagesQuery {
    relay: Option<String>,
}

async fn messages(
    State(state): State<Arc<AppState>>,
    Query(query): Query<MessagesQuery>,
) -> Result<Json<Versioned<MessagesSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let relays = relay_device_rows(&state).await?;
    if let Some(relay) = query.relay.as_deref()
        && !relays.iter().any(|device| device.entity.id.0 == relay)
    {
        return Err(ApiError::invalid("unknown relay device"));
    }
    let inbox = inbox(state.client(), 100)
        .await
        .map_err(|_| ApiError::unavailable())?;
    let relay_data = if let Some(relay) = query.relay.as_deref() {
        Some(
            relay_history(state.client(), relay, 100)
                .await
                .map_err(|_| ApiError::unavailable())?,
        )
    } else {
        None
    };
    Ok(Json(Versioned::current(messages_snapshot(
        metadata,
        relays,
        query.relay,
        inbox,
        relay_data,
    ))))
}

fn messages_snapshot(
    metadata: SnapshotMetadata,
    relays: Vec<DeviceSummary>,
    selected_relay: Option<String>,
    inbox: MessageListResponse,
    relay_data: Option<(DeviceChannelsResponse, DeviceMessagesResponse)>,
) -> MessagesSnapshot {
    let (channels, relay_messages, next_cursor) = relay_data.map_or_else(
        || (Vec::new(), Vec::new(), None),
        |(channels, messages)| {
            (
                channels
                    .channels
                    .into_iter()
                    .filter_map(|channel| {
                        channel.name.map(|name| BobnetChannelSummary {
                            name,
                            last_active: channel.last_active,
                        })
                    })
                    .collect(),
                messages
                    .messages
                    .into_iter()
                    .map(|message| BobnetMessageSummary {
                        id: message.id,
                        channel: message.channel,
                        body: message.message,
                        is_npc_or_system: message.replicant_code.is_none(),
                        sender: message.replicant_code,
                        sender_name: message.replicant_name,
                        current_system: message.current_star,
                        created_at: message.time,
                    })
                    .collect(),
                messages.next_cursor,
            )
        },
    );
    MessagesSnapshot {
        metadata,
        relays: relays.into_iter().map(|device| device.entity).collect(),
        selected_relay,
        channels,
        relay_messages,
        unread_count: inbox.unread_message_count,
        inbox: inbox
            .messages
            .into_iter()
            .map(|message| InboxMessageSummary {
                id: message.id,
                title: message.title,
                body: message.body,
                category: message.category,
                message_type: message.message_type,
                is_read: message.is_read,
                created_at: message.created_at,
            })
            .collect(),
        next_cursor,
    }
}

async fn network(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Versioned<NetworkSnapshot>>, ApiError> {
    let metadata = state.snapshot_metadata()?;
    let account = account_profile(state.client())
        .await
        .map_err(|_| ApiError::unavailable())?;
    let mut relays = Vec::new();
    for device in relay_device_rows(&state).await? {
        match state.client().bobnet().channels(&device.entity.id.0).await {
            Ok(channels) => relays.push(NetworkRelaySummary {
                device,
                channels: channel_summaries(channels),
                error: None,
            }),
            Err(_) => relays.push(NetworkRelaySummary {
                device,
                channels: Vec::new(),
                error: Some("Channel status unavailable".to_owned()),
            }),
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

async fn relay_device_rows(state: &Arc<AppState>) -> Result<Vec<DeviceSummary>, ApiError> {
    Ok(device_rows(state)
        .await?
        .into_iter()
        .filter(|device| {
            device
                .device_type
                .as_deref()
                .is_some_and(|kind| kind.to_ascii_lowercase().contains("relay"))
        })
        .collect())
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
                    .find(|key| is_standard_leaderboard(key))
            })
            .map(str::to_owned)
    });
    if selected.as_deref().is_some_and(|key| {
        !is_standard_leaderboard(key)
            || !index
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

fn is_standard_leaderboard(key: &str) -> bool {
    matches!(
        key,
        "colony_moon"
            | "colony_planet"
            | "distance"
            | "fleet"
            | "megastructure"
            | "reputation"
            | "trades"
            | "xp"
    )
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
                board.key.and_then(|key| {
                    is_standard_leaderboard(&key).then(|| LeaderboardBoardSummary {
                        key,
                        name: board.name,
                        description: board.description,
                        board_type: board.r#type,
                    })
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
    missions.sort_by(|left, right| right.updated_at_ms.cmp(&left.updated_at_ms));
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
    replicant_names: &BTreeMap<String, String>,
    claim: Option<DeviceClaim>,
) -> DeviceSummary {
    let location = device.location.map(|value| value.id.to_string());
    let owner = device
        .relationships
        .assigned_replicant
        .map(|value| value.id.to_string());
    DeviceSummary {
        entity: summary_ref(EntityKind::Device, device.key.id.to_string()),
        device_type: wire_value(device.device_type.as_ref()),
        status: wire_value(device.status.as_ref()),
        ownership: wire_value(Some(&device.access)).unwrap_or_else(|| "unknown".to_owned()),
        owner_name: owner
            .as_ref()
            .and_then(|value| replicant_names.get(value).cloned()),
        owner,
        system: location
            .as_ref()
            .and_then(|value| device_system(value, location_systems)),
        location,
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
        cargo_capacity: device.stow_capacity,
        cargo_used: device.stow_used,
        operational_capacity_percent: device
            .operational_capacity
            .map(replicant_client::domain::OperationalCapacity::percent),
        active_directive: device
            .active_directive
            .as_ref()
            .and_then(|value| wire_value(value.directive.as_ref())),
        directive_status: device.active_directive.and_then(|value| value.status),
        travel_destination: device.travel.and_then(|value| {
            value
                .final_destination
                .or(value.destination)
                .map(|destination| destination.id.to_string())
        }),
        claim,
    }
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
    let workflows = state.repository.list().map_err(ApiError::repository)?;
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
        entities.push(EntitySummary {
            entity: summary_ref(EntityKind::Replicant, replicant.key.id.to_string()),
            label: replicant.key.id.to_string(),
            secondary_label: replicant.name,
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

fn summary_ref(kind: EntityKind, id: String) -> EntityRef {
    EntityRef {
        kind,
        id: EntityId(id),
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
    let workflows = state.repository.list().map_err(ApiError::repository)?;
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
    let workflows = state.repository.list().map_err(ApiError::repository)?;
    let revision = state.revision.load(Ordering::Relaxed);
    let scene = build_system_scene(state.client(), &workflows, &system, revision, now_millis()?)
        .await
        .map_err(|error| {
            tracing::error!(error = %error, system, "system scene projection failed");
            ApiError::unavailable()
        })?;
    Ok(Json(Versioned::current(scene)))
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

async fn run_action(
    State(state): State<Arc<AppState>>,
    Path(kind): Path<String>,
    payload: Result<Json<RunOperationRequest>, JsonRejection>,
) -> Result<Json<Versioned<RunOperationResponse>>, ApiError> {
    let request = payload
        .map_err(|_| ApiError::invalid("invalid action parameters"))?
        .0;
    let started_at = now_millis()?;
    let result = state
        .catalogue
        .run_action(state.client(), &kind, request.parameters)
        .await;
    operation_response(
        &state,
        FiniteExecutionClass::Action,
        &kind,
        started_at,
        result,
    )
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
            if kind.starts_with("bootstrap.") {
                state.publish(LiveDelta::DomainInvalidated {
                    slice: DomainSlice::Missions,
                });
            }
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
        ProtocolTriggerCondition::ParentWorkflow { status, .. }
            if matches!(
                status,
                ProtocolStatus::Succeeded | ProtocolStatus::Failed | ProtocolStatus::Cancelled
            ) =>
        {
            replicant_protocol::TriggerKind::ParentWorkflow
        }
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
            StoredFiniteExecutionStatus::Succeeded => ProtocolFiniteExecutionStatus::Succeeded,
            StoredFiniteExecutionStatus::Skipped => ProtocolFiniteExecutionStatus::Skipped,
            StoredFiniteExecutionStatus::Failed => ProtocolFiniteExecutionStatus::Failed,
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
        .list()
        .map_err(ApiError::repository)?
        .iter()
        .filter(|instance| {
            filter
                .status
                .is_none_or(|status| status == protocol_status(instance.status))
        })
        .map(summary)
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
    let supervisor = state.supervisor.lock().await;
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
            supervisor.pause_all().map_err(supervisor_error)?
        }
        AutomationControlAction::ResumeAll => {
            policy.workflows_paused = false;
            state
                .repository
                .set_automation_policy(policy)
                .map_err(ApiError::repository)?;
            supervisor.resume_all().map_err(supervisor_error)?
        }
        AutomationControlAction::Cancel => {
            let ids = request
                .workflow_ids
                .iter()
                .map(|id| parse_id(&id.0))
                .collect::<Result<Vec<_>, _>>()?;
            supervisor.cancel_selected(&ids).map_err(supervisor_error)?
        }
    };
    drop(supervisor);
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
    let supervisor = state.supervisor.lock().await;
    match control {
        Control::Pause => supervisor.pause(id),
        Control::Resume => supervisor.resume(id),
        Control::Cancel => supervisor.cancel(id),
    }
    .map_err(supervisor_error)?;
    drop(supervisor);
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
        error: (instance.status == WorkflowStatus::Failed).then(|| "workflow failed".to_owned()),
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
    let mut notifications = workflows
        .iter()
        .filter_map(workflow_notification)
        .chain(triggers.iter().filter_map(|trigger| {
            trigger.last_error.as_ref().map(|_| Notification {
                id: EntityId(format!("trigger:{}:failed", trigger.id)),
                level: NotificationLevel::Error,
                title: "Automatic trigger failed".to_owned(),
                message: "An automatic trigger failed; inspect local daemon logs".to_owned(),
                created_at_ms: trigger.updated_at,
            })
        }))
        .collect::<Vec<_>>();
    let sync = RuntimeSyncStatus {
        phase: sync_phase(status),
        revision: 0,
        last_event_at_ms: None,
        detail: status_detail(status).map(str::to_owned),
    };
    if matches!(sync.phase, SyncPhase::Degraded | SyncPhase::Offline) {
        notifications.push(sync_notification(&sync));
    }
    notifications
}

fn workflow_notification(workflow: &WorkflowInstance) -> Option<Notification> {
    let error = workflow.last_error.as_deref()?;
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
            "A workflow is blocked by a resource claim".to_owned()
        } else {
            "A workflow failed; inspect local daemon logs".to_owned()
        },
        created_at_ms: workflow.updated_at,
    })
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

    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "workflow_not_found",
            message: "workflow not found",
        }
    }

    fn operation_not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "operation_not_found",
            message: "operation not found",
        }
    }

    fn unavailable() -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "runtime_unavailable",
            message: "runtime is unavailable",
        }
    }

    fn internal() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "runtime_error",
            message: "runtime request failed",
        }
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
        matchers::{method, path},
    };

    use super::*;

    async fn test_app() -> (Router, Client, Arc<AppState>) {
        let client = Client::builder()
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("start test client");
        let repository = Arc::new(WorkflowRepository::open_in_memory().expect("runtime database"));
        let state = AppState::new(client.clone(), RuntimeConfig::new("test"), repository)
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
        let state = AppState::new(client.clone(), RuntimeConfig::new("test"), repository)
            .expect("app state");
        (router(state), client)
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
            location: None,
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
            operational_capacity_percent: None,
            active_directive: None,
            directive_status: None,
            travel_destination: None,
            claim: None,
        }
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
            None,
        );
        assert_eq!(controller.entity.id.0, "TC-1");
        assert_eq!(controller.shop_name, None);
        assert_eq!(controller.trades[0].current_stock, None);
        assert_eq!(controller.trades[0].requested[0].kind, "resource");
        assert_eq!(controller.trades[0].requested[0].quantity, Some(4.0));
        assert_eq!(controller.trades[0].offered[0].kind, "device");
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
        let inbox = serde_json::from_value(serde_json::json!({
            "messages": [{"id": 1, "title": "Notice", "is_read": false}],
            "unread_message_count": 1
        }))
        .expect("message fixture");
        let relay_channels = serde_json::from_value(serde_json::json!({
            "channels": [{"name": "general", "last_active": null}]
        }))
        .expect("channel fixture");
        let relay_messages = serde_json::from_value(serde_json::json!({
            "messages": [{"id": 2, "message": "hello", "replicant_code": null}]
        }))
        .expect("relay message fixture");
        let messages = messages_snapshot(
            metadata.clone(),
            vec![projected_device("RELAY-1")],
            Some("RELAY-1".to_owned()),
            inbox,
            Some((relay_channels, relay_messages)),
        );
        assert_eq!(messages.unread_count, Some(1));
        assert!(messages.relay_messages[0].is_npc_or_system);

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
            attach_capacity: None,
            stow_capacity: Some(100),
            stow_used: Some(25),
            operational_capacity: OperationalCapacity::new(0.75),
            active_directive: None,
            travel: None,
            access: AccessScope::Owned,
        };
        let row = device_summary(
            device,
            &BTreeMap::from([("EARTH".to_owned(), Some("SOL".to_owned()))]),
            &BTreeMap::from([("R-1".to_owned(), "Ada".to_owned())]),
            Some(DeviceClaim {
                workflow_id: ProtocolWorkflowId("wf-1".to_owned()),
                workflow_kind: OperationKind("transport.route".to_owned()),
                workflow_status: ProtocolStatus::Running,
            }),
        );

        assert_eq!(row.device_type.as_deref(), Some("future_device"));
        assert_eq!(row.status, None);
        assert_eq!(row.owner.as_deref(), Some("R-1"));
        assert_eq!(row.system.as_deref(), Some("SOL"));
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
            GalaxySceneSnapshot {
                revision: 4,
                generated_at_ms: 10,
                stars: vec![replicant_protocol::GalaxyStar {
                    id: "SOL".to_owned(),
                    name: None,
                    spectral_type: None,
                    position: replicant_protocol::GalaxyPoint {
                        x: 0.0,
                        y: 0.0,
                        z: 0.0,
                    },
                    exploration: replicant_protocol::GalaxyExploration::Explored,
                    current: false,
                    has_hub: false,
                    has_life: false,
                    has_relay: true,
                }],
                relay_edges: Vec::new(),
                active_travel: Vec::new(),
                signals: Vec::new(),
                highlights: Vec::new(),
                overlays: Vec::new(),
                workflow_targets: Vec::new(),
            },
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
            assert!(!value.to_string().contains("token"));
        }
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
        assert_eq!(
            entities["payload"]["entities"][0]["entity"]["kind"],
            "workflow"
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
        publish_workflow_updates(&state, &mut BTreeMap::new(), &mut 0);

        assert!(matches!(
            updates.recv().await.expect("workflow update").delta,
            LiveDelta::WorkflowCreated(_)
        ));
        for expected in [
            DomainSlice::Entities,
            DomainSlice::Workflows,
            DomainSlice::Overview,
            DomainSlice::Devices,
            DomainSlice::Autofactories,
            DomainSlice::Cargo,
            DomainSlice::Missions,
        ] {
            assert!(matches!(
                updates.recv().await.expect("domain invalidation").delta,
                LiveDelta::DomainInvalidated { slice } if slice == expected
            ));
        }
        assert!(matches!(
            updates.recv().await.expect("activity update").delta,
            LiveDelta::WorkflowActivity(_)
        ));
        assert!(matches!(
            updates.recv().await.expect("overview invalidation").delta,
            LiveDelta::DomainInvalidated {
                slice: DomainSlice::Overview
            }
        ));
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn bootstrap_action_completion_invalidates_missions() {
        let (_, client, state) = test_app().await;
        let mut updates = state.live.subscribe();
        operation_response(
            &state,
            FiniteExecutionClass::Action,
            "bootstrap.run",
            1,
            Ok(Value::Null),
        )
        .unwrap_or_else(|_| panic!("operation response"));
        assert!(matches!(
            updates.recv().await.expect("mission invalidation").delta,
            LiveDelta::DomainInvalidated {
                slice: DomainSlice::Missions
            }
        ));
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
