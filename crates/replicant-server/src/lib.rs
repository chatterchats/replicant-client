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
    managed::{Client, OperationStatus as ManagedOperationStatus},
};
use replicant_protocol::{
    ActivityLevel, AutomationTrigger as ProtocolTrigger, CreateTriggerRequest, DaemonHealth,
    DescriptorCatalog, DomainSlice, EntityId, EntityKind, EntityRef, ErrorResponse,
    FiniteExecution as ProtocolFiniteExecution, FiniteExecutionHistoryResponse,
    FiniteExecutionStatus as ProtocolFiniteExecutionStatus, GalaxySceneSnapshot, HealthStatus,
    LiveDelta, LiveMessage, OperationClass, OperationKind, OperationStatus, OperationUpdate,
    RequirementSummary, ResultSummary, RunOperationRequest, RunOperationResponse, RuntimeSnapshot,
    RuntimeSyncStatus, SnapshotMetadata, StartWorkflowRequest, StartWorkflowResponse, SyncPhase,
    SystemSceneSnapshot, TriggerCondition as ProtocolTriggerCondition,
    TriggerId as ProtocolTriggerId, TriggerListResponse, TriggerTarget as ProtocolTriggerTarget,
    UpdateTriggerRequest, Versioned, WorkflowActivity, WorkflowActivityResponse,
    WorkflowControlResponse, WorkflowDetail, WorkflowId as ProtocolWorkflowId,
    WorkflowListResponse, WorkflowStatus as ProtocolStatus, WorkflowSummary,
};
use replicant_runtime::{
    ApplicationContext,
    catalogue::{CatalogueError, OperationCatalogue},
    config::RuntimeConfig,
    galaxy_scene::galaxy_scene as build_galaxy_scene,
    requirements::{AvailabilityKind, InfrastructureKind, RequirementScope, RequirementTarget},
    system_scene::system_scene as build_system_scene,
    workflows::{RequirementWorkflowCheckpoint, RequirementWorkflowConfig, WorkflowActivityEvent},
};
use replicant_workflow::{
    AutomationTrigger, FiniteExecution as StoredFiniteExecution, FiniteExecutionClass,
    FiniteExecutionStatus as StoredFiniteExecutionStatus, NewTrigger, RepositoryError, ResourceKey,
    SupervisorError, TriggerCondition, TriggerId, TriggerState, TriggerTarget, TriggerTargetClass,
    WorkflowId, WorkflowInstance, WorkflowKind, WorkflowRepository, WorkflowStatus,
    WorkflowSupervisor,
};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{Mutex, broadcast, watch};

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
        .route("/api/workflows", get(list_workflows).post(start_workflow))
        .route("/api/workflows/{id}", get(workflow_detail))
        .route("/api/workflows/{id}/activity", get(workflow_activity))
        .route("/api/workflows/{id}/pause", post(pause_workflow))
        .route("/api/workflows/{id}/resume", post(resume_workflow))
        .route("/api/workflows/{id}/cancel", post(cancel_workflow))
        .with_state(state)
}

/// Runs periodic persisted-workflow reconciliation until shutdown.
pub async fn run_supervisor(state: Arc<AppState>, mut shutdown: watch::Receiver<bool>) {
    let mut interval = tokio::time::interval(Duration::from_millis(250));
    let mut revisions = state.client().state().watch_galaxy().ok();
    let mut operations = state.client().operations().watch().ok();
    let mut workflows = state
        .repository
        .list()
        .unwrap_or_default()
        .into_iter()
        .map(|workflow| (workflow.id.to_string(), workflow.revision))
        .collect::<BTreeMap<_, _>>();
    let mut activity_cursor = state.repository.latest_activity_id().unwrap_or_default();
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(error) = state.supervisor.lock().await.tick().await {
                    tracing::error!(error = %error, "workflow supervisor tick failed");
                }
                publish_workflow_updates(&state, &mut workflows, &mut activity_cursor);
            }
            revision = async { revisions.as_mut().expect("guarded").next().await }, if revisions.is_some() => {
                match revision {
                    Ok(_) => state.publish(LiveDelta::DomainInvalidated { slice: DomainSlice::Universe }),
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
                    let _ = state
                        .repository
                        .set_trigger_error(trigger.id, Some("schedule interval must be positive"));
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
            let _ = state
                .repository
                .set_trigger_error(trigger.id, Some("managed event history unavailable"));
            continue;
        };
        for event in events {
            if state.client().state().revision().is_err() {
                let _ = state
                    .repository
                    .set_trigger_error(trigger.id, Some("managed projections unavailable"));
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
        .claim_trigger_firing(trigger.id, &dedupe_key, now, next_run_at)
    {
        Ok(true) => {
            if let Err(error) = launch_trigger(state, &trigger, parent_id).await {
                let _ = state.repository.set_trigger_error(trigger.id, Some(&error));
            }
        }
        Ok(false) => {}
        Err(error) => {
            tracing::error!(trigger_id = %trigger.id, error = %error, "trigger claim failed")
        }
    }
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
    Ok(Json(Versioned::current(RuntimeSnapshot {
        metadata: state.snapshot_metadata()?,
        sync: RuntimeSyncStatus {
            phase: sync_phase(&status),
            revision,
            last_event_at_ms: None,
            detail: status_detail(&status).map(str::to_owned),
        },
        workflows,
        requirements,
    })))
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
        state
            .repository
            .set_trigger_error(id, Some(&error))
            .map_err(ApiError::repository)?;
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
            parameters: trigger.target.parameters,
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
    .map_err(|error| match error {
        SupervisorError::Repository(error) => ApiError::repository(error),
    })?;
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
        .map(|record| {
            let (level, step, message) = present_activity(&record.message);
            Ok(WorkflowActivity {
                id: u64::try_from(record.id).map_err(|_| ApiError::internal())?,
                workflow_id: ProtocolWorkflowId(record.workflow_id.to_string()),
                occurred_at_ms: record.created_at,
                level,
                step,
                message,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(Json(Versioned::current(WorkflowActivityResponse {
        activity,
    })))
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
    let Value::Object(mut object) = value else {
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
    use replicant_client::StartupPolicy;
    use replicant_protocol::{Notification, NotificationLevel};
    use replicant_workflow::{NewWorkflow, WorkflowKind};
    use tokio::net::TcpListener;
    use tower::ServiceExt;

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

    #[tokio::test]
    async fn health_snapshot_and_catalogue_are_frontend_safe() {
        let (app, client, _) = test_app().await;
        for path in [
            "/api/health",
            "/api/snapshot",
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
            assert!(!value.to_string().contains("token"));
        }
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
        let message = match state.resnapshot_message() {
            Ok(message) => message,
            Err(_) => panic!("resnapshot failed"),
        };
        assert!(matches!(message.delta, LiveDelta::Snapshot(_)));
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
        assert!(matches!(
            updates.recv().await.expect("activity update").delta,
            LiveDelta::WorkflowActivity(_)
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
