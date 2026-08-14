//! HTTP query/command API for the local `replicantd` process.

use std::{
    collections::BTreeMap,
    env,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Json, Router,
    extract::{Path, Query, State, rejection::JsonRejection},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use replicant_client::{ClientStatus, managed::Client};
use replicant_protocol::{
    ActivityLevel, DaemonHealth, DescriptorCatalog, EntityId, EntityKind, EntityRef, ErrorResponse,
    HealthStatus, MutationRisk, OperationKind, ParameterDescriptor, ParameterKind, ParameterOption,
    ParameterValidation, RuntimeSnapshot, RuntimeSyncStatus, SnapshotMetadata,
    StartWorkflowRequest, StartWorkflowResponse, SyncPhase, TriggerKind, Versioned,
    WorkflowActivity, WorkflowActivityResponse, WorkflowControlResponse, WorkflowDescriptor,
    WorkflowDetail, WorkflowId as ProtocolWorkflowId, WorkflowListResponse,
    WorkflowStatus as ProtocolStatus, WorkflowSummary,
};
use replicant_runtime::{
    ApplicationContext,
    config::RuntimeConfig,
    relay::RelayExpansionRequest,
    survey::{SurveyMode, SurveyOptions},
    workflows::{
        RelayWorkflowConfig, SurveyWorkflowConfig, WorkflowActivityEvent, new_relay_workflow,
        new_survey_workflow, register,
    },
};
use replicant_workflow::{
    RegistryError, RepositoryError, ResourceKey, SupervisorError, WorkflowId, WorkflowInstance,
    WorkflowRepository, WorkflowStatus, WorkflowSupervisor,
};
use serde::Deserialize;
use serde_json::{Map, Value};
use tokio::sync::{Mutex, watch};

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
    descriptors: DescriptorCatalog,
}

impl AppState {
    /// Builds daemon state around one managed client and one runtime repository.
    pub fn new(
        client: Client,
        runtime_config: RuntimeConfig,
        repository: Arc<WorkflowRepository>,
    ) -> Result<Arc<Self>, RegistryError> {
        let mut registry = replicant_workflow::WorkflowRegistry::new();
        register(&mut registry)?;
        let registry = Arc::new(registry);
        let supervisor =
            WorkflowSupervisor::with_managed_client(repository.clone(), registry, client.clone());
        Ok(Arc::new(Self {
            context: ApplicationContext::new(client, runtime_config),
            repository,
            supervisor: Mutex::new(supervisor),
            descriptors: descriptor_catalog(),
        }))
    }

    /// Returns the daemon's single managed client.
    #[must_use]
    pub fn client(&self) -> &Client {
        self.context.client()
    }
}

/// Builds the local HTTP router.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/snapshot", get(snapshot))
        .route("/api/descriptors", get(descriptors))
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
    loop {
        tokio::select! {
            _ = interval.tick() => {
                if let Err(error) = state.supervisor.lock().await.tick().await {
                    tracing::error!(error = %error, "workflow supervisor tick failed");
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
    let workflows = state
        .repository
        .list()
        .map_err(ApiError::repository)?
        .iter()
        .map(summary)
        .collect();
    let status = state.client().status();
    Ok(Json(Versioned::current(RuntimeSnapshot {
        metadata: SnapshotMetadata {
            revision,
            generated_at_ms: now_millis()?,
        },
        sync: RuntimeSyncStatus {
            phase: sync_phase(&status),
            revision,
            last_event_at_ms: None,
            detail: status_detail(&status).map(str::to_owned),
        },
        workflows,
    })))
}

async fn descriptors(State(state): State<Arc<AppState>>) -> Json<Versioned<DescriptorCatalog>> {
    Json(Versioned::current(state.descriptors.clone()))
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
    let instance = match request.kind.0.as_str() {
        "survey.route" => {
            let parameters: SurveyStart = decode_parameters(request.parameters)?;
            state
                .repository
                .create(new_survey_workflow(SurveyWorkflowConfig {
                    options: parameters.into_options(),
                }))
        }
        "relay.expansion" => {
            let parameters: RelayStart = decode_parameters(request.parameters)?;
            state
                .repository
                .create(new_relay_workflow(RelayWorkflowConfig {
                    request: parameters.into_request(),
                }))
        }
        _ => return Err(ApiError::invalid("unknown workflow kind")),
    }
    .map_err(ApiError::repository)?;
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

#[derive(Deserialize)]
struct SurveyStart {
    #[serde(default = "default_survey_mode")]
    mode: SurveyMode,
    replicant: String,
    vessel: String,
    center: String,
    #[serde(default = "default_radius")]
    radius_ly: f64,
    #[serde(default = "default_system_limit")]
    system_limit: usize,
    #[serde(default = "default_concurrency")]
    star_detail_concurrency: usize,
    mission_file: PathBuf,
    #[serde(default)]
    controller: Option<String>,
    #[serde(default)]
    drones_csv: Option<String>,
    #[serde(default)]
    replace_plan: bool,
    #[serde(default)]
    include_explored: bool,
    #[serde(default = "default_timeout")]
    travel_timeout_seconds: u64,
    #[serde(default = "default_timeout")]
    survey_timeout_seconds: u64,
    maintenance_home: String,
    #[serde(default = "default_maintenance_interval")]
    maintenance_interval: usize,
    #[serde(default = "default_maintenance_threshold")]
    maintenance_threshold_pct: f64,
    #[serde(default = "default_maintenance_resume")]
    maintenance_resume_pct: f64,
    #[serde(default = "default_maintenance_check")]
    maintenance_check_seconds: u64,
}

impl SurveyStart {
    fn into_options(self) -> SurveyOptions {
        SurveyOptions {
            mode: self.mode,
            replicant: self.replicant,
            vessel: self.vessel,
            center: self.center,
            radius_ly: self.radius_ly,
            system_limit: self.system_limit,
            star_detail_concurrency: self.star_detail_concurrency,
            mission_file: self.mission_file,
            controller: self.controller,
            drones: self.drones_csv.map(csv),
            replace_plan: self.replace_plan,
            include_explored: self.include_explored,
            travel_timeout: Duration::from_secs(self.travel_timeout_seconds),
            survey_timeout: Duration::from_secs(self.survey_timeout_seconds),
            maintenance_home: self.maintenance_home,
            maintenance_interval: self.maintenance_interval,
            maintenance_threshold_pct: self.maintenance_threshold_pct,
            maintenance_resume_pct: self.maintenance_resume_pct,
            maintenance_check_interval: Duration::from_secs(self.maintenance_check_seconds),
        }
    }
}

#[derive(Deserialize)]
struct RelayStart {
    replicant: String,
    hub: String,
    targets_csv: String,
    mission_file: PathBuf,
    #[serde(default = "default_max_hop")]
    max_hop_ly: f64,
    #[serde(default = "default_timeout")]
    wait_timeout_seconds: u64,
}

impl RelayStart {
    fn into_request(self) -> RelayExpansionRequest {
        RelayExpansionRequest {
            replicant: self.replicant,
            hub: self.hub,
            targets: csv(self.targets_csv),
            mission_file: self.mission_file,
            max_hop_ly: self.max_hop_ly,
            wait_timeout: Duration::from_secs(self.wait_timeout_seconds),
        }
    }
}

fn csv(value: String) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect()
}

fn decode_parameters<T: for<'de> Deserialize<'de>>(
    parameters: BTreeMap<String, Value>,
) -> Result<T, ApiError> {
    serde_json::from_value(Value::Object(parameters.into_iter().collect::<Map<_, _>>()))
        .map_err(|_| ApiError::invalid("invalid workflow parameters"))
}

fn default_survey_mode() -> SurveyMode {
    SurveyMode::Run
}
fn default_radius() -> f64 {
    10.0
}
fn default_system_limit() -> usize {
    80
}
fn default_concurrency() -> usize {
    8
}
fn default_timeout() -> u64 {
    21_600
}
fn default_maintenance_interval() -> usize {
    40
}
fn default_maintenance_threshold() -> f64 {
    25.0
}
fn default_maintenance_resume() -> f64 {
    95.0
}
fn default_maintenance_check() -> u64 {
    900
}
fn default_max_hop() -> f64 {
    7.499
}

fn descriptor_catalog() -> DescriptorCatalog {
    DescriptorCatalog {
        reports: Vec::new(),
        actions: Vec::new(),
        workflows: vec![
            WorkflowDescriptor {
                kind: OperationKind("survey.route".to_owned()),
                display_name: "Survey route".to_owned(),
                description: "Plan or execute a restart-safe system survey route.".to_owned(),
                category: "survey".to_owned(),
                risk: MutationRisk::Elevated,
                parameters: vec![
                    enum_parameter("mode", "Mode", &["plan", "run"], "run"),
                    required("replicant", "Replicant", ParameterKind::Replicant),
                    required("vessel", "Vessel", ParameterKind::Device),
                    required("center", "Centre system", ParameterKind::System),
                    defaulted("radius_ly", "Radius (ly)", ParameterKind::Number, 10.0),
                    defaulted("system_limit", "System limit", ParameterKind::Integer, 80),
                    defaulted(
                        "star_detail_concurrency",
                        "Catalogue concurrency",
                        ParameterKind::Integer,
                        8,
                    ),
                    required("mission_file", "Mission file", ParameterKind::String),
                    optional("controller", "Survey controller", ParameterKind::Device),
                    optional(
                        "drones_csv",
                        "Survey drones (comma-separated)",
                        ParameterKind::String,
                    ),
                    defaulted(
                        "replace_plan",
                        "Replace plan",
                        ParameterKind::Boolean,
                        false,
                    ),
                    defaulted(
                        "include_explored",
                        "Include explored",
                        ParameterKind::Boolean,
                        false,
                    ),
                    defaulted(
                        "travel_timeout_seconds",
                        "Travel timeout (seconds)",
                        ParameterKind::Integer,
                        21_600,
                    ),
                    defaulted(
                        "survey_timeout_seconds",
                        "Survey timeout (seconds)",
                        ParameterKind::Integer,
                        21_600,
                    ),
                    required(
                        "maintenance_home",
                        "Maintenance home",
                        ParameterKind::System,
                    ),
                    defaulted(
                        "maintenance_interval",
                        "Maintenance interval",
                        ParameterKind::Integer,
                        40,
                    ),
                    defaulted(
                        "maintenance_threshold_pct",
                        "Maintenance threshold (%)",
                        ParameterKind::Number,
                        25.0,
                    ),
                    defaulted(
                        "maintenance_resume_pct",
                        "Maintenance resume (%)",
                        ParameterKind::Number,
                        95.0,
                    ),
                    defaulted(
                        "maintenance_check_seconds",
                        "Maintenance check (seconds)",
                        ParameterKind::Integer,
                        900,
                    ),
                ],
                supported_triggers: vec![TriggerKind::Manual],
            },
            WorkflowDescriptor {
                kind: OperationKind("relay.expansion".to_owned()),
                display_name: "Relay expansion".to_owned(),
                description: "Build and deploy a restart-safe relay expansion.".to_owned(),
                category: "relay".to_owned(),
                risk: MutationRisk::Elevated,
                parameters: vec![
                    required("replicant", "Replicant", ParameterKind::Replicant),
                    required("hub", "Manufacturing hub", ParameterKind::Location),
                    required(
                        "targets_csv",
                        "Target systems (comma-separated)",
                        ParameterKind::String,
                    ),
                    required("mission_file", "Mission file", ParameterKind::String),
                    defaulted(
                        "max_hop_ly",
                        "Maximum hop (ly)",
                        ParameterKind::Number,
                        7.499,
                    ),
                    defaulted(
                        "wait_timeout_seconds",
                        "Wait timeout (seconds)",
                        ParameterKind::Integer,
                        21_600,
                    ),
                ],
                supported_triggers: vec![TriggerKind::Manual],
            },
        ],
    }
}

fn parameter(name: &str, label: &str, kind: ParameterKind, required: bool) -> ParameterDescriptor {
    ParameterDescriptor {
        name: name.to_owned(),
        label: label.to_owned(),
        description: label.to_owned(),
        kind,
        required,
        default: None,
        options: Vec::new(),
        validation: ParameterValidation::default(),
    }
}

fn required(name: &str, label: &str, kind: ParameterKind) -> ParameterDescriptor {
    parameter(name, label, kind, true)
}

fn optional(name: &str, label: &str, kind: ParameterKind) -> ParameterDescriptor {
    parameter(name, label, kind, false)
}

fn defaulted(
    name: &str,
    label: &str,
    kind: ParameterKind,
    value: impl Into<Value>,
) -> ParameterDescriptor {
    let mut parameter = parameter(name, label, kind, false);
    parameter.default = Some(value.into());
    parameter
}

fn enum_parameter(name: &str, label: &str, values: &[&str], default: &str) -> ParameterDescriptor {
    let mut parameter = defaulted(name, label, ParameterKind::Enum, default);
    parameter.options = values
        .iter()
        .map(|value| ParameterOption {
            value: (*value).to_owned(),
            label: (*value).to_owned(),
        })
        .collect();
    parameter
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
    use http_body_util::BodyExt;
    use replicant_client::StartupPolicy;
    use tower::ServiceExt;

    use super::*;

    async fn test_app() -> (Router, Client) {
        let client = Client::builder()
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
        let (app, client) = test_app().await;
        for path in ["/api/health", "/api/snapshot", "/api/descriptors"] {
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
        let (app, client) = test_app().await;
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

    #[test]
    fn default_bind_is_loopback() {
        assert!(is_loopback(DEFAULT_BIND.parse().expect("default bind")));
    }
}
