//! Durable workflow adapters for the application's restart-safe runtime services.

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    hash::{Hash, Hasher},
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use replicant_client::raw::RequestPriority;
use replicant_workflow::{
    AllocationCandidate, AllocationSet, BoxWorkflowFuture, ClaimAcquireOutcome, NewWorkflow,
    RegistryError, ReplacementOutcome, ResourceKey, WorkItem, WorkItemSpec, WorkItemStatus,
    WorkItemTransition, WorkflowContext, WorkflowExecutor, WorkflowFactory, WorkflowId,
    WorkflowKind, WorkflowMigration, WorkflowPlacementIntent, WorkflowPlacementIntentCoverage,
    WorkflowPlacementIntentProjection, WorkflowPlacementIntentRelation,
    WorkflowPlacementIntentSubject, WorkflowRegistry, WorkflowServiceIntentProjection,
    WorkflowServiceScope, WorkflowStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    automation::reconcile_event_connectivity,
    catalogue::OperationCatalogue,
    event::{
        EventCampaignArchive, EventPlanningRequest, event_campaign_work_item_specs,
        event_mission_preflight, plan_event_mission,
    },
    mining::{
        AmiTransportRouteIntent, MiningExpansionRequest, MiningMission, execute_mining_item,
        merge_mining_item_state, mining_item_completed, mining_work_item_specs,
        plan_expansion_from_managed_state,
    },
    relay::{
        RelayExecutionState, RelayExpansionRequest, elastic_relay_assignment, execute_relay_trip,
        merge_relay_trip_state, prepare_relay_workflow, relay_checkpoint_worker,
        relay_coverage_satisfied_stop, relay_expansion_report, relay_item_stop_index,
        relay_planned_transport_capacity, relay_stop_completed, relay_work_item_specs,
        revalidate_relay_work_items,
    },
    requirements::{
        ActiveFulfillment, FulfillmentOperation, FulfillmentOperationClass, FulfillmentPlan,
        Requirement, evaluate_requirement, managed_facts,
    },
    survey::{
        SurveyExecutionState, SurveyOptions, execute_survey_item, merge_survey_item_state,
        prepare_survey_workflow, summarize_plan, survey_checkpoint_identities,
        survey_item_completed, survey_item_route_index, survey_work_item_specs,
    },
};

const SCHEMA_VERSION: u32 = 1;

/// Stable survey-route workflow kind.
pub fn survey_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("survey.route").expect("static workflow kind is valid")
}

/// Stable relay-expansion workflow kind.
pub fn relay_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("relay.expansion").expect("static workflow kind is valid")
}

/// Stable desired-state orchestration workflow kind.
pub fn requirement_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("requirement.fulfillment").expect("static workflow kind is valid")
}

/// Stable mining-expansion workflow kind.
pub fn mining_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("mining.expansion").expect("static workflow kind is valid")
}

/// Stable persisted event-execution workflow kind.
pub fn event_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("event.fulfillment").expect("static workflow kind is valid")
}

fn requirement_action_kind() -> WorkflowKind {
    WorkflowKind::new("requirement.action").expect("static workflow kind is valid")
}

/// Registers all application workflow kinds.
pub fn register(registry: &mut WorkflowRegistry) -> Result<(), RegistryError> {
    registry.register(Arc::new(SurveyWorkflowFactory::new()))?;
    registry.register(Arc::new(RelayWorkflowFactory::new()))?;
    registry.register(Arc::new(RequirementWorkflowFactory::new()))?;
    registry.register(Arc::new(MiningWorkflowFactory::new()))?;
    registry.register(Arc::new(EventWorkflowFactory::new()))?;
    registry.register(Arc::new(RequirementActionFactory::new()))?;
    crate::automation::register(registry)
}

/// Persisted identity-free survey campaign configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SurveyWorkflowConfig {
    /// Director region constraining every worker and fleet bundle.
    pub region: String,
    /// Route centre.
    pub center: String,
    /// Search radius.
    pub radius_ly: f64,
    /// Maximum route systems.
    pub system_limit: usize,
    /// Optional exact system allowlist.
    pub target_systems: Option<Vec<String>>,
    /// Concurrent catalogue detail reads during planning.
    pub star_detail_concurrency: usize,
    /// Durable legacy plan path used by the mature route logic.
    pub mission_file: PathBuf,
    /// Replace any pre-existing plan during initial planning.
    pub replace_plan: bool,
    /// Include explored systems when planning.
    pub include_explored: bool,
    /// Travel evidence timeout.
    pub travel_timeout: Duration,
    /// Survey evidence timeout.
    pub survey_timeout: Duration,
    /// Maintenance home.
    pub maintenance_home: String,
    /// Stops between maintenance checks.
    pub maintenance_interval: usize,
    /// Capacity threshold triggering maintenance.
    pub maintenance_threshold_pct: f64,
    /// Capacity required to resume.
    pub maintenance_resume_pct: f64,
    /// Maintenance polling interval.
    pub maintenance_check_interval: Duration,
}

impl SurveyWorkflowConfig {
    /// Converts direct survey options into an identity-free regional campaign.
    #[must_use]
    pub fn from_options(options: SurveyOptions, region: String) -> Self {
        Self {
            region,
            center: options.center,
            radius_ly: options.radius_ly,
            system_limit: options.system_limit,
            target_systems: options.target_systems,
            star_detail_concurrency: options.star_detail_concurrency,
            mission_file: options.mission_file,
            replace_plan: options.replace_plan,
            include_explored: options.include_explored,
            travel_timeout: options.travel_timeout,
            survey_timeout: options.survey_timeout,
            maintenance_home: options.maintenance_home,
            maintenance_interval: options.maintenance_interval,
            maintenance_threshold_pct: options.maintenance_threshold_pct,
            maintenance_resume_pct: options.maintenance_resume_pct,
            maintenance_check_interval: options.maintenance_check_interval,
        }
    }

    fn options_for_bundle(
        &self,
        replicant: String,
        vessel: String,
        controller: Option<String>,
        drones: Option<Vec<String>>,
    ) -> SurveyOptions {
        SurveyOptions {
            mode: crate::survey::SurveyMode::Run,
            replicant,
            vessel,
            center: self.center.clone(),
            radius_ly: self.radius_ly,
            system_limit: self.system_limit,
            target_systems: self.target_systems.clone(),
            star_detail_concurrency: self.star_detail_concurrency,
            mission_file: self.mission_file.clone(),
            controller,
            drones,
            replace_plan: self.replace_plan,
            include_explored: self.include_explored,
            travel_timeout: self.travel_timeout,
            survey_timeout: self.survey_timeout,
            maintenance_home: self.maintenance_home.clone(),
            maintenance_interval: self.maintenance_interval,
            maintenance_threshold_pct: self.maintenance_threshold_pct,
            maintenance_resume_pct: self.maintenance_resume_pct,
            maintenance_check_interval: self.maintenance_check_interval,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct LegacySurveyWorkflowConfig {
    options: SurveyOptions,
}

/// Persisted survey workflow checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SurveyWorkflowCheckpoint {
    /// Last authoritative survey executor state.
    pub state: Option<SurveyExecutionState>,
    /// Completed phase names, retained for restart reconciliation activity.
    pub completed_steps: BTreeSet<String>,
    /// Historical worker identity used only to resolve schema-one region evidence.
    #[serde(default)]
    pub migration_worker: Option<String>,
}

/// Persisted identity-free relay campaign configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RelayWorkflowConfig {
    /// Manufacturing location in the regional island.
    pub hub: String,
    /// Director region evidence used to constrain every lane allocation.
    #[serde(default)]
    pub region: Option<String>,
    /// Systems that must be connected.
    pub targets: Vec<String>,
    /// Legacy mission file used only by the mature relay planner and direct adapters.
    pub mission_file: PathBuf,
    /// Maximum conventional relay hop.
    pub max_hop_ly: f64,
    /// Whole seconds in the managed-operation evidence wait.
    pub wait_timeout_seconds: u64,
    /// Nanosecond remainder in the managed-operation evidence wait.
    #[serde(default)]
    pub wait_timeout_nanoseconds: u32,
    /// Autofactories unavailable to this campaign.
    #[serde(default)]
    pub unavailable_autofactories: BTreeSet<String>,
}

impl RelayWorkflowConfig {
    /// Converts the direct relay request into an identity-free durable campaign.
    #[must_use]
    pub fn from_request(request: RelayExpansionRequest) -> Self {
        Self {
            hub: request.hub,
            region: None,
            targets: request.targets,
            mission_file: request.mission_file,
            max_hop_ly: request.max_hop_ly,
            wait_timeout_seconds: request.wait_timeout.as_secs(),
            wait_timeout_nanoseconds: request.wait_timeout.subsec_nanos(),
            unavailable_autofactories: request.unavailable_autofactories,
        }
    }

    fn request_for_worker(&self, worker: String) -> RelayExpansionRequest {
        RelayExpansionRequest {
            replicant: worker,
            hub: self.hub.clone(),
            targets: self.targets.clone(),
            mission_file: self.mission_file.clone(),
            max_hop_ly: self.max_hop_ly,
            wait_timeout: Duration::new(self.wait_timeout_seconds, self.wait_timeout_nanoseconds),
            unavailable_autofactories: self.unavailable_autofactories.clone(),
        }
    }
}

#[derive(Deserialize, Serialize)]
struct LegacyRelayWorkflowConfig {
    request: RelayExpansionRequest,
}

/// Persisted relay workflow checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RelayWorkflowCheckpoint {
    /// Last authoritative relay executor state.
    pub state: Option<RelayExecutionState>,
    /// Director region evidence resolved before item materialization.
    #[serde(default)]
    pub region: Option<String>,
    /// Completed phase names, retained for restart reconciliation activity.
    pub completed_steps: BTreeSet<String>,
}

/// Persisted desired-state workflow configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RequirementWorkflowConfig {
    /// Desired state and registered lower-level fulfillment operation.
    pub requirement: Requirement,
}

/// Restart-safe desired-state orchestration checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RequirementWorkflowCheckpoint {
    /// Most recent non-mutating evaluation.
    pub plan: Option<FulfillmentPlan>,
    /// Child workflows already created by this orchestration.
    pub children: Vec<WorkflowId>,
}

/// Identity-free persisted mining expansion inputs.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MiningWorkflowConfig {
    /// Systems that should receive mining installations.
    pub systems: Vec<String>,
    /// Director region whose broker pool may execute the campaign.
    pub region: String,
    /// Manufacturing hub used for staging and printing.
    pub hub: String,
    /// Exact AMI transport routes to provision.
    #[serde(default)]
    pub transport_routes: Vec<AmiTransportRouteIntent>,
    /// Existing mining mission file retained for direct-action interoperability.
    pub mission_file: std::path::PathBuf,
    /// Maximum duration for managed-state waits.
    pub wait_timeout_seconds: u64,
    /// Scheduler ceiling for simultaneously runnable items.
    pub max_concurrency: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LegacyMiningWorkflowConfig {
    systems: Vec<String>,
    replicant: String,
    hub: String,
    mission_file: std::path::PathBuf,
    wait_timeout_seconds: u64,
    max_concurrency: usize,
}

/// Durable workflow-owned mining campaign checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct MiningWorkflowCheckpoint {
    /// Last merged mining mission state.
    #[serde(default)]
    pub mission: Option<MiningMission>,
    /// Legacy actor used only to derive region evidence during migration.
    #[serde(default)]
    pub migration_worker: Option<String>,
    /// Whether execution has entered the pooled executor.
    pub started: bool,
}

/// Identity-free persisted event-fulfillment inputs.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventWorkflowConfig {
    /// Event designation to plan before first execution.
    #[serde(default)]
    pub event: Option<String>,
    /// Completion criterion when the event offers multiple paths.
    #[serde(default)]
    pub criterion: Option<String>,
    /// Director region whose broker pool may execute the mission.
    pub region: String,
    /// Manufacturing and staging home.
    pub home: String,
    /// Event compatibility plan file.
    pub plan_file: std::path::PathBuf,
    /// Replace an existing plan when creating a new mission.
    #[serde(default)]
    pub replace_plan: bool,
    /// Maximum duration for managed-state waits.
    pub wait_timeout_seconds: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct LegacyEventWorkflowConfig {
    event: Option<String>,
    criterion: Option<String>,
    replicant: Option<String>,
    home: Option<String>,
    plan_file: std::path::PathBuf,
    #[serde(default)]
    replace_plan: bool,
    wait_timeout_seconds: u64,
}

/// Durable event-fulfillment checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventWorkflowCheckpoint {
    /// Whether execution entered the item executor.
    pub started: bool,
    /// Legacy actor used only to resolve Director region evidence.
    #[serde(default)]
    pub migration_worker: Option<String>,
    /// Authoritative serialized mission.
    #[serde(default)]
    pub plan_json: Option<String>,
    /// Relay-expansion workflow satisfying a disconnected destination.
    #[serde(default)]
    pub connectivity_workflows: BTreeMap<String, WorkflowId>,
    /// Whether connectivity requires mission replanning.
    #[serde(default)]
    pub replan_after_connectivity: bool,
}

pub(crate) type MiningItemFuture<'a> =
    Pin<Box<dyn Future<Output = crate::mining::AnyResult<MiningMission>> + Send + 'a>>;

pub(crate) trait MiningItemExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        client: &'a replicant_client::Client,
        mission: &'a MiningMission,
        item_type: &'a str,
        index: usize,
        allocations: &'a AllocationSet,
        wait_timeout: Duration,
    ) -> MiningItemFuture<'a>;
}

#[derive(Debug, thiserror::Error)]
#[error("allocated resource for requirement {requirement} is missing")]
pub(crate) struct MiningMissingAllocationError {
    requirement: String,
    allocation_id: replicant_workflow::AllocationId,
}

pub(crate) struct ManagedMiningItemExecutor;
impl MiningItemExecutor for ManagedMiningItemExecutor {
    fn execute<'a>(
        &'a self,
        client: &'a replicant_client::Client,
        mission: &'a MiningMission,
        item_type: &'a str,
        index: usize,
        allocations: &'a AllocationSet,
        wait_timeout: Duration,
    ) -> MiningItemFuture<'a> {
        Box::pin(execute_mining_item(
            client,
            mission,
            item_type,
            index,
            allocations,
            wait_timeout,
        ))
    }
}

/// Factory for durable broker-allocated mining expansions.
pub struct MiningWorkflowFactory {
    kind: WorkflowKind,
    item_executor: Arc<dyn MiningItemExecutor>,
}

impl MiningWorkflowFactory {
    /// Creates the stable mining workflow factory.
    #[must_use]
    pub fn new() -> Self {
        Self {
            kind: mining_workflow_kind(),
            item_executor: Arc::new(ManagedMiningItemExecutor),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_item_executor(item_executor: Arc<dyn MiningItemExecutor>) -> Self {
        Self {
            kind: mining_workflow_kind(),
            item_executor,
        }
    }
}

impl Default for MiningWorkflowFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowFactory for MiningWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    fn current_schema_version(&self) -> u32 {
        2
    }

    fn supports_schema_version(&self, version: u32) -> bool {
        matches!(version, 1 | 2)
    }

    fn migrate(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
    ) -> Result<Option<WorkflowMigration>, String> {
        if instance.schema_version == 2 {
            return Ok(None);
        }
        let legacy: LegacyMiningWorkflowConfig = instance.config().map_err(string_error)?;
        let mut checkpoint: MiningWorkflowCheckpoint =
            instance.checkpoint().map_err(string_error)?;
        checkpoint.migration_worker = Some(legacy.replicant);
        if checkpoint.mission.is_none() && legacy.mission_file.exists() {
            checkpoint.mission =
                Some(crate::mining::load_expansion(&legacy.mission_file).map_err(string_error)?);
        }
        let config = MiningWorkflowConfig {
            systems: legacy.systems,
            region: String::new(),
            hub: legacy.hub,
            transport_routes: Vec::new(),
            mission_file: legacy.mission_file,
            wait_timeout_seconds: legacy.wait_timeout_seconds,
            max_concurrency: legacy.max_concurrency,
        };
        Ok(Some(WorkflowMigration::new(
            serde_json::to_value(config).map_err(string_error)?,
            serde_json::to_value(checkpoint).map_err(string_error)?,
        )))
    }

    fn placement_intents(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
        work_items: &[WorkItem],
    ) -> Result<WorkflowPlacementIntentProjection, String> {
        if instance.schema_version != self.current_schema_version() {
            return Ok(WorkflowPlacementIntentProjection::unknown());
        }
        let _: MiningWorkflowConfig = instance.config().map_err(string_error)?;
        let checkpoint: MiningWorkflowCheckpoint = instance.checkpoint().map_err(string_error)?;
        let mut intents = Vec::new();
        if let Some(mission) = checkpoint.mission {
            let value = serde_json::to_value(mission).map_err(string_error)?;
            let mission: PlacementMiningMission =
                serde_json::from_value(value).map_err(string_error)?;
            if placement_status_is_live(instance.status) {
                if !mission.mission_tag.trim().is_empty() {
                    intents.push(placement_intent(
                        WorkflowPlacementIntentSubject::DeviceTag(mission.mission_tag.clone()),
                        WorkflowPlacementIntentRelation::Awaited,
                        None,
                        None,
                    ));
                }
                for tag in &mission.legacy_mission_tags {
                    if !tag.trim().is_empty() {
                        intents.push(placement_intent(
                            WorkflowPlacementIntentSubject::DeviceTag(tag.clone()),
                            WorkflowPlacementIntentRelation::Awaited,
                            None,
                            None,
                        ));
                    }
                }
            }
            for site in mission.sites {
                let relation = match site.phase.as_deref() {
                    Some("outbound" | "adopting" | "verifying" | "configuring")
                        if !site.system.trim().is_empty() =>
                    {
                        Some(WorkflowPlacementIntentRelation::Transported)
                    }
                    Some("ready" | "deploying") if !site.system.trim().is_empty() => {
                        Some(WorkflowPlacementIntentRelation::Staged)
                    }
                    _ => None,
                };
                if placement_status_is_live(instance.status) && !site.tag.trim().is_empty() {
                    intents.push(placement_intent(
                        WorkflowPlacementIntentSubject::DeviceTag(site.tag.clone()),
                        WorkflowPlacementIntentRelation::Awaited,
                        None,
                        None,
                    ));
                }
                let mut codes = site.assets.mining_drones;
                codes.extend(site.assets.survey_drones);
                if let Some(code) = site.assets.mining_controller {
                    codes.push(code);
                }
                if let Some(code) = site.assets.survey_controller {
                    codes.push(code);
                }
                if let Some(code) = site.assets.maintenance_drone {
                    codes.push(code);
                }
                if let Some(code) = site.assets.system_ward {
                    codes.push(code);
                }
                if let Some(code) = site.carrier {
                    codes.push(code);
                }
                for code in codes {
                    if let Some(subject) = placement_subject(&code)
                        && let Some(intent) =
                            status_intent(instance.status, subject, relation, None, None)
                    {
                        intents.push(intent);
                    }
                }
            }
            for route in mission.routes {
                let relation = match route.phase.as_deref() {
                    Some("activating") => Some(WorkflowPlacementIntentRelation::Transported),
                    Some("ready") => Some(WorkflowPlacementIntentRelation::Staged),
                    _ => None,
                };
                for code in [route.controller, route.freighter].into_iter().flatten() {
                    if let Some(subject) = placement_subject(&code)
                        && let Some(intent) =
                            status_intent(instance.status, subject, relation, None, None)
                    {
                        intents.push(intent);
                    }
                }
            }
            for batch in mission.print_batches {
                for code in batch.produced_codes {
                    if let Some(subject) = placement_subject(&code)
                        && let Some(intent) = status_intent(
                            instance.status,
                            subject,
                            Some(WorkflowPlacementIntentRelation::Staged),
                            None,
                            None,
                        )
                    {
                        intents.push(intent);
                    }
                }
            }
        }
        decode_typed_work_items::<PlacementMiningItem>(work_items)?;
        Ok(complete_projection(intents))
    }

    fn service_intents(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
    ) -> Result<WorkflowServiceIntentProjection, String> {
        let config: MiningWorkflowConfig = instance.config().map_err(string_error)?;
        let checkpoint: MiningWorkflowCheckpoint = instance.checkpoint().map_err(string_error)?;
        if let Some(mission) = checkpoint.mission
            && !mission.routes.is_empty()
            && !mission.hub_location.trim().is_empty()
            && mission
                .routes
                .iter()
                .all(|route| !route.system.trim().is_empty() && !route.belt.trim().is_empty())
        {
            let destination = mission.hub_location;
            return Ok(WorkflowServiceIntentProjection::complete(
                mission
                    .routes
                    .into_iter()
                    .map(|route| {
                        crate::mining::AmiTransportRouteIntent {
                            system: route.system,
                            collect: route.belt,
                            deliver: destination.clone(),
                        }
                        .workflow_service_intent()
                    })
                    .collect(),
            ));
        }
        if !config.transport_routes.is_empty() {
            return Ok(WorkflowServiceIntentProjection::complete(
                config
                    .transport_routes
                    .iter()
                    .map(crate::mining::AmiTransportRouteIntent::workflow_service_intent)
                    .collect(),
            ));
        }
        if config.systems.iter().all(|system| system.trim().is_empty()) {
            return Err("mining workflow has no service scope".into());
        }
        Ok(WorkflowServiceIntentProjection::unknown(
            config
                .systems
                .iter()
                .filter(|system| !system.trim().is_empty())
                .map(|system| WorkflowServiceScope::System(system.trim().to_ascii_uppercase())),
        ))
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(MiningWorkflow {
            item_executor: self.item_executor.clone(),
        }))
    }
}

struct MiningWorkflow {
    item_executor: Arc<dyn MiningItemExecutor>,
}

impl WorkflowExecutor for MiningWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        let item_executor = self.item_executor.clone();
        Box::pin(async move { execute_mining_pool(context, item_executor).await })
    }
}

async fn execute_mining_pool(
    context: &mut WorkflowContext,
    item_executor: Arc<dyn MiningItemExecutor>,
) -> Result<(), String> {
    let config: MiningWorkflowConfig = context.config().map_err(string_error)?;
    let checkpoint: MiningWorkflowCheckpoint = context.checkpoint().map_err(string_error)?;
    execute_mining_pool_config(context, item_executor, config, checkpoint).await
}

fn mining_checkpoint_has_legacy_ward_gate(mission: &MiningMission) -> bool {
    mission
        .print_batches
        .iter()
        .any(|batch| batch.device_type == "system_ward")
}

pub(crate) async fn execute_mining_pool_config(
    context: &mut WorkflowContext,
    item_executor: Arc<dyn MiningItemExecutor>,
    config: MiningWorkflowConfig,
    mut checkpoint: MiningWorkflowCheckpoint,
) -> Result<(), String> {
    let client = context
        .managed_client()
        .cloned()
        .ok_or_else(|| "mining workflow requires a managed client".to_owned())?;
    let repository = context.repository_handle();
    let region = resolve_mining_region(
        repository.as_ref(),
        &config.region,
        checkpoint.migration_worker.as_deref(),
    )?
    .ok_or_else(|| "mining campaign has no Director region evidence".to_owned())?;
    let broker =
        crate::assignment::ResourceBroker::with_managed_client(repository.clone(), client.clone());
    if checkpoint
        .mission
        .as_ref()
        .is_some_and(mining_checkpoint_has_legacy_ward_gate)
    {
        return context
            .mark_failed(
                "mining campaign checkpoint uses the obsolete System-Ward-gated deployment plan; replan without ward gating",
            )
            .map_err(string_error);
    }
    if checkpoint.mission.is_none() {
        let candidates = regional_relay_candidates(
            repository.as_ref(),
            &client,
            broker.discover_candidates().map_err(string_error)?,
            &region,
        )?;
        let worker = candidate_identity(&candidates, "replicant", None)
            .ok_or_else(|| "mining planning has no regional Replicant".to_owned())?;
        checkpoint.mission = Some(
            plan_expansion_from_managed_state(
                &client,
                &MiningExpansionRequest {
                    systems: config.systems.clone(),
                    replicant: worker,
                    hub: config.hub.clone(),
                    transport_routes: config.transport_routes.clone(),
                    mission_file: config.mission_file.clone(),
                    wait_timeout: Duration::from_secs(config.wait_timeout_seconds),
                    max_concurrency: config.max_concurrency.max(1),
                },
                false,
            )
            .await
            .map_err(string_error)?,
        );
        context
            .persist_checkpoint(&checkpoint)
            .map_err(string_error)?;
    }
    let mut mission = checkpoint
        .mission
        .clone()
        .ok_or_else(|| "mining planning produced no checkpoint".to_owned())?;
    let reconciled = repository
        .reconcile_work_items(
            context.id(),
            &mining_work_item_specs(context.id(), &mission, &region).map_err(string_error)?,
            workflow_now_millis(),
        )
        .map_err(string_error)?;
    for item in reconciled {
        let completed_in_checkpoint = item.spec.payload_json["legacy_complete"]
            == Value::Bool(true)
            || (item.spec.kind.as_str() == "mining.stage"
                && mining_item_completed(&mission, "stage"));
        if completed_in_checkpoint && !item.state.status.is_terminal() {
            repository
                .transition_work_item(
                    item.id,
                    item.state.revision,
                    WorkItemTransition::Skipped {
                        reason: "completed in migrated mining checkpoint".to_owned(),
                        result_json: Some(item.spec.payload_json.clone()),
                    },
                    workflow_now_millis(),
                )
                .map_err(string_error)?;
        }
    }
    checkpoint.started = true;
    context
        .advance_to("executing", &checkpoint)
        .map_err(string_error)?;
    loop {
        let candidates = regional_relay_candidates(
            repository.as_ref(),
            &client,
            broker.discover_candidates().map_err(string_error)?,
            &region,
        )?;
        let now_ms = workflow_now_millis();
        let manufacturing_stage = repository
            .list_work_items(context.id())
            .map_err(string_error)?
            .into_iter()
            .find(|item| item.spec.kind.as_str() == "mining.stage");
        let manufacturing_incomplete = manufacturing_stage.as_ref().is_some_and(|item| {
            !matches!(
                item.state.status,
                WorkItemStatus::Succeeded | WorkItemStatus::Skipped
            )
        });
        let manufacturing_claimable =
            manufacturing_stage
                .as_ref()
                .is_none_or(|item| match item.state.status {
                    WorkItemStatus::Pending => true,
                    WorkItemStatus::Waiting => item
                        .state
                        .next_attempt_at_ms
                        .is_some_and(|retry_at_ms| retry_at_ms <= now_ms),
                    WorkItemStatus::Succeeded | WorkItemStatus::Skipped => true,
                    WorkItemStatus::Assigned
                    | WorkItemStatus::Running
                    | WorkItemStatus::Failed
                    | WorkItemStatus::Abandoned => false,
                });
        if manufacturing_incomplete && !manufacturing_claimable {
            break;
        }
        let mut running = Vec::new();
        while running.len() < config.max_concurrency.max(1) {
            let Some(assigned) = repository
                .claim_next_work_item(context.id(), workflow_now_millis())
                .map_err(string_error)?
            else {
                break;
            };
            let assigned_item_type = assigned.spec.payload_json["type"]
                .as_str()
                .unwrap_or_default();
            if manufacturing_incomplete && assigned_item_type != "stage" {
                repository
                    .transition_work_item(
                        assigned.id,
                        assigned.state.revision,
                        WorkItemTransition::Reclaimed {
                            checkpoint_json: assigned.state.checkpoint_json.clone(),
                        },
                        workflow_now_millis(),
                    )
                    .map_err(string_error)?;
                break;
            }
            let allocation_affinities = mining_allocation_affinities(assigned_item_type);
            let allocations = match broker.allocate_with_affinity(
                assigned.id,
                assigned.state.revision,
                &candidates,
                allocation_affinities,
            ) {
                Ok(allocations) => allocations,
                Err(error) => {
                    repository
                        .transition_work_item(
                            assigned.id,
                            assigned.state.revision,
                            WorkItemTransition::Waiting {
                                checkpoint_json: assigned.state.checkpoint_json.clone(),
                                reason: error.to_string(),
                                retry_at_ms: Some(workflow_now_millis().saturating_add(300_000)),
                            },
                            workflow_now_millis(),
                        )
                        .map_err(string_error)?;
                    break;
                }
            };
            let worker = survey_allocated_identity(&allocations, "worker", "replicant")?;
            let assignment_id = mining_assignment_id(assigned.id, assigned.state.revision, &worker);
            repository
                .assign_work_item(
                    assigned.id,
                    assigned.state.revision,
                    &assignment_id,
                    &ResourceKey::Replicant(worker.clone()),
                    workflow_now_millis(),
                )
                .map_err(string_error)?;
            let started = repository
                .start_work_item(
                    assigned.id,
                    assigned.state.revision,
                    &worker,
                    &assignment_id,
                    workflow_now_millis(),
                )
                .map_err(string_error)?;
            let item_type = started.spec.payload_json["type"]
                .as_str()
                .ok_or_else(|| "mining item payload omitted type".to_owned())?
                .to_owned();
            let manufacturing_barrier = item_type == "stage";
            let index = started.spec.payload_json["index"]
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| "mining item payload omitted index".to_owned())?;
            running.push(run_mining_item(
                repository.clone(),
                client.clone(),
                broker.clone(),
                item_executor.clone(),
                candidates.clone(),
                mission.clone(),
                started,
                item_type,
                index,
                allocations,
                Duration::from_secs(config.wait_timeout_seconds),
            ));
            if manufacturing_barrier {
                // Manufacturing owns the inventory/autofactory inputs that produce the
                // downstream site/route hardware. Run it as a barrier without mutating
                // persisted work-item specs, so already-active campaigns remain restartable.
                break;
            }
        }
        if running.is_empty() {
            break;
        }
        for result in futures::future::join_all(running).await {
            let (item_type, index, lane) = result?;
            merge_mining_item_state(&mut mission, &lane, &item_type, index);
            checkpoint.mission = Some(mission.clone());
            context
                .advance_to("executing", &checkpoint)
                .map_err(string_error)?;
        }
    }
    match repository
        .aggregate_campaign_result(context.id())
        .map_err(string_error)?
    {
        Some(result) if result.workflow_status() == WorkflowStatus::Succeeded => {
            emit(context, &WorkflowActivityEvent::Completion)?;
            context
                .mark_succeeded(Some(serde_json::json!({
                    "systems": mission.sites.iter().map(|site| site.system.clone()).collect::<Vec<_>>(),
                    "belts": mission.sites.iter().map(|site| site.belt.clone()).collect::<Vec<_>>(),
                    "mission": mission,
                    "progress": mission.progress(),
                })))
                .map_err(string_error)
        }
        Some(result) => context
            .mark_failed_with_result(
                "mining campaign completed without a successful item",
                result,
                replicant_workflow::WorkflowFailureDisposition::Permanent,
            )
            .map_err(string_error),
        None => context.mark_waiting().map_err(string_error),
    }
}

fn mining_assignment_id(
    item_id: replicant_workflow::WorkItemId,
    assignment_revision: u64,
    worker: &str,
) -> String {
    format!("mining:{item_id}:r{assignment_revision}:{worker}")
}

fn mining_allocation_affinities(item_type: &str) -> &'static [(&'static str, &'static str)] {
    match item_type {
        "site" => &[("stow", "carrier")],
        "route" => &[("stow", "freighter")],
        _ => &[],
    }
}

fn allocation_resource_identity(resource: &ResourceKey) -> &str {
    match resource {
        ResourceKey::Replicant(key) | ResourceKey::Device(key) | ResourceKey::Autofactory(key) => {
            key
        }
        ResourceKey::Namespaced { key, .. } => key,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_mining_item(
    repository: Arc<replicant_workflow::WorkflowRepository>,
    client: replicant_client::Client,
    broker: crate::assignment::ResourceBroker,
    item_executor: Arc<dyn MiningItemExecutor>,
    replacement_candidates: Vec<AllocationCandidate>,
    mission: MiningMission,
    item: WorkItem,
    item_type: String,
    index: usize,
    mut allocations: AllocationSet,
    wait_timeout: Duration,
) -> Result<(String, usize, MiningMission), String> {
    let revision = item.state.revision;
    loop {
        match item_executor
            .execute(
                &client,
                &mission,
                &item_type,
                index,
                &allocations,
                wait_timeout,
            )
            .await
        {
            Ok(lane) if mining_item_completed(&lane, &item_type) => {
                repository
                    .transition_work_item(
                        item.id,
                        revision,
                        WorkItemTransition::Succeeded {
                            checkpoint_json: Some(
                                serde_json::to_value(&lane).map_err(string_error)?,
                            ),
                            result_json: Some(item.spec.payload_json.clone()),
                        },
                        workflow_now_millis(),
                    )
                    .map_err(string_error)?;
                return Ok((item_type, index, lane));
            }
            Ok(lane) => {
                repository
                    .transition_work_item(
                        item.id,
                        revision,
                        WorkItemTransition::RetryableFailure {
                            checkpoint_json: Some(
                                serde_json::to_value(&lane).map_err(string_error)?,
                            ),
                            error: "mining item returned without completing".to_owned(),
                        },
                        workflow_now_millis(),
                    )
                    .map_err(string_error)?;
                return Ok((item_type, index, lane));
            }
            Err(error)
                if error
                    .downcast_ref::<MiningMissingAllocationError>()
                    .is_some()
                    || crate::failure::failure_class(error.as_ref())
                        == Some(crate::failure::FailureClass::DeviceTargetMissing) =>
            {
                let typed_missing = error
                    .downcast_ref::<MiningMissingAllocationError>()
                    .map(|error| (error.requirement.clone(), error.allocation_id));
                if let Some((requirement, allocation_id)) = match typed_missing {
                    Some(missing) => Some(missing),
                    None => missing_mining_allocation(&client, &allocations).await?,
                } {
                    match broker
                        .replace_dead_allocation_from_with_affinity(
                            item.id,
                            allocation_id,
                            &replacement_candidates,
                            mining_allocation_affinities(&item_type),
                        )
                        .map_err(string_error)?
                    {
                        ReplacementOutcome::Replaced(replacement) => {
                            let allocation = allocations
                                .by_requirement
                                .get_mut(&requirement)
                                .and_then(|values| {
                                    values
                                        .iter_mut()
                                        .find(|allocation| allocation.id == allocation_id)
                                })
                                .ok_or_else(|| {
                                    format!("mining allocation {allocation_id} disappeared")
                                })?;
                            *allocation = replacement.clone();
                            if matches!(requirement.as_str(), "carrier" | "freighter")
                                && let Some(stow) = allocations
                                    .by_requirement
                                    .get("stow")
                                    .and_then(|values| values.first())
                                    .cloned()
                                && allocation_resource_identity(&stow.resource)
                                    != allocation_resource_identity(&replacement.resource)
                            {
                                match broker
                                    .replace_dead_allocation_from_with_affinity(
                                        item.id,
                                        stow.id,
                                        &replacement_candidates,
                                        mining_allocation_affinities(&item_type),
                                    )
                                    .map_err(string_error)?
                                {
                                    ReplacementOutcome::Replaced(replacement_stow) => {
                                        allocations
                                            .by_requirement
                                            .insert("stow".to_owned(), vec![replacement_stow]);
                                    }
                                    ReplacementOutcome::Waiting => {
                                        repository
                                            .transition_work_item(
                                                item.id,
                                                revision,
                                                WorkItemTransition::Reclaimed {
                                                    checkpoint_json: Some(
                                                        serde_json::to_value(&mission)
                                                            .map_err(string_error)?,
                                                    ),
                                                },
                                                workflow_now_millis(),
                                            )
                                            .map_err(string_error)?;
                                        return Ok((item_type, index, mission));
                                    }
                                    ReplacementOutcome::Unavailable => {
                                        return Ok((item_type, index, mission));
                                    }
                                }
                            }
                            continue;
                        }
                        ReplacementOutcome::Waiting => {
                            repository
                                .transition_work_item(
                                    item.id,
                                    revision,
                                    WorkItemTransition::Waiting {
                                        checkpoint_json: Some(
                                            serde_json::to_value(&mission).map_err(string_error)?,
                                        ),
                                        reason: error.to_string(),
                                        retry_at_ms: Some(
                                            workflow_now_millis().saturating_add(300_000),
                                        ),
                                    },
                                    workflow_now_millis(),
                                )
                                .map_err(string_error)?;
                            return Ok((item_type, index, mission));
                        }
                        ReplacementOutcome::Unavailable => {
                            return Ok((item_type, index, mission));
                        }
                    }
                }
                repository
                    .transition_work_item(
                        item.id,
                        revision,
                        WorkItemTransition::RetryableFailure {
                            checkpoint_json: Some(
                                serde_json::to_value(&mission).map_err(string_error)?,
                            ),
                            error: error.to_string(),
                        },
                        workflow_now_millis(),
                    )
                    .map_err(string_error)?;
                return Ok((item_type, index, mission));
            }
            Err(error) => {
                let waiting = crate::failure::failure_class(error.as_ref()).is_some_and(|class| {
                    matches!(
                        class,
                        crate::failure::FailureClass::EventControlUnavailable
                            | crate::failure::FailureClass::ConnectivityDependency
                            | crate::failure::FailureClass::ManufacturingCapacity
                            | crate::failure::FailureClass::TransientUpstream
                    )
                });
                let transition = if waiting {
                    WorkItemTransition::Waiting {
                        checkpoint_json: Some(
                            serde_json::to_value(&mission).map_err(string_error)?,
                        ),
                        reason: error.to_string(),
                        retry_at_ms: Some(workflow_now_millis().saturating_add(300_000)),
                    }
                } else {
                    WorkItemTransition::RetryableFailure {
                        checkpoint_json: Some(
                            serde_json::to_value(&mission).map_err(string_error)?,
                        ),
                        error: error.to_string(),
                    }
                };
                repository
                    .transition_work_item(item.id, revision, transition, workflow_now_millis())
                    .map_err(string_error)?;
                return Ok((item_type, index, mission));
            }
        }
    }
}

async fn missing_mining_allocation(
    client: &replicant_client::Client,
    allocations: &AllocationSet,
) -> Result<Option<(String, replicant_workflow::AllocationId)>, String> {
    for (requirement, values) in &allocations.by_requirement {
        for allocation in values {
            let ResourceKey::Device(code) = &allocation.resource else {
                continue;
            };
            if let Err(error) = client.devices().get(code).await
                && crate::failure::device_fetch_is_missing(&error)
            {
                return Ok(Some((requirement.clone(), allocation.id)));
            }
        }
    }
    Ok(None)
}

fn resolve_mining_region(
    repository: &replicant_workflow::WorkflowRepository,
    configured: &str,
    migration_worker: Option<&str>,
) -> Result<Option<String>, String> {
    if !configured.is_empty() {
        return Ok(Some(configured.to_owned()));
    }
    if let Some(worker) = migration_worker
        && let Some((document, _)) = repository
            .read_document("director.replicant", worker)
            .map_err(string_error)?
        && let Some(region) = document.get("region").and_then(Value::as_str)
    {
        return Ok(Some(region.to_owned()));
    }
    let regions = repository
        .list_documents("director.replicant")
        .map_err(string_error)?
        .into_iter()
        .filter_map(|(_, document, _)| {
            document
                .get("region")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect::<BTreeSet<_>>();
    Ok((regions.len() == 1)
        .then(|| regions.into_iter().next())
        .flatten())
}

/// Factory for identity-free durable event fulfillment.
pub struct EventWorkflowFactory(WorkflowKind);
impl EventWorkflowFactory {
    /// Creates the stable event workflow factory.
    #[must_use]
    pub fn new() -> Self {
        Self(event_workflow_kind())
    }
}
impl Default for EventWorkflowFactory {
    fn default() -> Self {
        Self::new()
    }
}
impl WorkflowFactory for EventWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.0
    }

    fn current_schema_version(&self) -> u32 {
        2
    }

    fn supports_schema_version(&self, version: u32) -> bool {
        matches!(version, 1 | 2)
    }

    fn migrate(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
    ) -> Result<Option<WorkflowMigration>, String> {
        if instance.schema_version == 2 {
            return Ok(None);
        }
        let legacy: LegacyEventWorkflowConfig = instance.config().map_err(string_error)?;
        let mut checkpoint: EventWorkflowCheckpoint =
            instance.checkpoint().map_err(string_error)?;
        checkpoint.migration_worker = legacy.replicant;
        if checkpoint.plan_json.is_none() && legacy.plan_file.exists() {
            checkpoint.plan_json =
                Some(std::fs::read_to_string(&legacy.plan_file).map_err(string_error)?);
        }
        let config = EventWorkflowConfig {
            event: legacy.event,
            criterion: legacy.criterion,
            region: String::new(),
            home: legacy.home.unwrap_or_default(),
            plan_file: legacy.plan_file,
            replace_plan: legacy.replace_plan,
            wait_timeout_seconds: legacy.wait_timeout_seconds,
        };
        Ok(Some(WorkflowMigration::new(
            serde_json::to_value(config).map_err(string_error)?,
            serde_json::to_value(checkpoint).map_err(string_error)?,
        )))
    }

    fn placement_intents(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
        work_items: &[WorkItem],
    ) -> Result<WorkflowPlacementIntentProjection, String> {
        if instance.schema_version != self.current_schema_version() {
            return Ok(WorkflowPlacementIntentProjection::unknown());
        }
        let _: EventWorkflowConfig = instance.config().map_err(string_error)?;
        let checkpoint: EventWorkflowCheckpoint = instance.checkpoint().map_err(string_error)?;

        // The event executor's mission document is persisted as an opaque
        // compatibility payload.  EventMissionPlan is private to the event
        // adapter, so there is no actual typed schema available here to
        // decode safely.  Never claim complete coverage (and thereby prove
        // absence of placement intent) while that document is present.
        if checkpoint
            .plan_json
            .as_deref()
            .is_some_and(|mission_json| !mission_json.trim().is_empty())
        {
            return Ok(WorkflowPlacementIntentProjection::unknown());
        }

        for item in work_items {
            if item.state.checkpoint_json.is_some() {
                return Ok(WorkflowPlacementIntentProjection::unknown());
            }
            let payload = match serde_json::from_value::<PlacementEventItem>(
                item.spec.payload_json.clone(),
            ) {
                Ok(payload) => payload,
                // Unknown outer fields (including an unmodelled device
                // reference) must not be silently discarded.
                Err(_) => return Ok(WorkflowPlacementIntentProjection::unknown()),
            };
            // This is the actual event mission document, not an event-work
            // item envelope.  Without its private typed schema, an opaque
            // non-empty value cannot safely contribute placement evidence.
            if !payload.mission_json.trim().is_empty() {
                return Ok(WorkflowPlacementIntentProjection::unknown());
            }
        }

        Ok(complete_projection(Vec::new()))
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(EventWorkflow))
    }
}
struct EventWorkflow;
impl WorkflowExecutor for EventWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let config: EventWorkflowConfig =
                context.config().map_err(|error| error.to_string())?;
            let mut checkpoint: EventWorkflowCheckpoint =
                context.checkpoint().map_err(|error| error.to_string())?;
            let client = context
                .managed_client()
                .cloned()
                .ok_or_else(|| "event workflow requires a managed client".to_owned())?;
            let background_client = client.with_priority(RequestPriority::Background);
            let repository = context.repository_handle();
            let region = resolve_mining_region(
                repository.as_ref(),
                &config.region,
                checkpoint.migration_worker.as_deref(),
            )?
            .ok_or_else(|| "event fulfillment has no Director region evidence".to_owned())?;
            let broker = crate::assignment::ResourceBroker::with_managed_client(
                repository.clone(),
                client.clone(),
            );
            let candidates = regional_relay_candidates(
                repository.as_ref(),
                &client,
                broker.discover_candidates().map_err(string_error)?,
                &region,
            )?;
            let planning_worker = candidate_identity(&candidates, "replicant", None)
                .ok_or_else(|| "event planning has no regional Replicant".to_owned())?;
            if let Some(plan_json) = checkpoint.plan_json.as_deref() {
                std::fs::write(&config.plan_file, plan_json).map_err(string_error)?;
            }
            if !checkpoint.started {
                if let Some(event) = config.event.as_deref()
                    && (!config.plan_file.exists() || config.replace_plan)
                {
                    let replicant = planning_worker.clone();
                    let home = if config.home.is_empty() {
                        return Err("event workflow planning requires a manufacturing home".into());
                    } else {
                        config.home.clone()
                    };
                    plan_event_mission(
                        &background_client,
                        &EventPlanningRequest {
                            event: event.to_owned(),
                            criterion: config.criterion.clone(),
                            replicant,
                            home,
                            plan_file: config.plan_file.clone(),
                            replace_plan: config.replace_plan,
                        },
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                    checkpoint.plan_json =
                        Some(std::fs::read_to_string(&config.plan_file).map_err(string_error)?);
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(string_error)?;
                }

                let preflight = event_mission_preflight(&config.plan_file)
                    .map_err(|error| error.to_string())?;
                let targets = BTreeSet::from([preflight.target_system.clone()]);
                if !reconcile_event_connectivity(
                    context,
                    &background_client,
                    &mut checkpoint.connectivity_workflows,
                    &mut checkpoint.replan_after_connectivity,
                    &preflight.replicant,
                    &preflight.home,
                    &targets,
                )
                .await?
                {
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(|error| error.to_string())?;
                    context
                        .advance_to("awaiting_ftl_connectivity", &checkpoint)
                        .map_err(|error| error.to_string())?;
                    context.mark_waiting().map_err(|error| error.to_string())?;
                    return Ok(());
                }

                if checkpoint.replan_after_connectivity {
                    plan_event_mission(
                        &background_client,
                        &EventPlanningRequest {
                            event: preflight.event,
                            criterion: Some(preflight.criterion),
                            replicant: preflight.replicant,
                            home: preflight.home,
                            plan_file: config.plan_file.clone(),
                            replace_plan: true,
                        },
                    )
                    .await
                    .map_err(|error| error.to_string())?;
                    checkpoint.connectivity_workflows.clear();
                    checkpoint.replan_after_connectivity = false;
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(|error| error.to_string())?;
                }
            }
            checkpoint.started = true;
            checkpoint.plan_json =
                Some(std::fs::read_to_string(&config.plan_file).map_err(string_error)?);
            let archive = EventCampaignArchive {
                campaign_json: "{}".into(),
                mission_json: BTreeMap::from([(
                    config.plan_file.to_string_lossy().into_owned(),
                    checkpoint
                        .plan_json
                        .clone()
                        .ok_or_else(|| "event fulfillment omitted plan checkpoint".to_owned())?,
                )]),
            };
            let reconciled = repository
                .reconcile_work_items(
                    context.id(),
                    &event_campaign_work_item_specs(context.id(), &archive, &region)
                        .map_err(string_error)?,
                    workflow_now_millis(),
                )
                .map_err(string_error)?;
            for item in reconciled {
                if item.spec.payload_json["legacy_complete"].as_bool() == Some(true)
                    && !item.state.status.is_terminal()
                {
                    repository
                        .transition_work_item(
                            item.id,
                            item.state.revision,
                            WorkItemTransition::Skipped {
                                reason: "completed in migrated event checkpoint".into(),
                                result_json: Some(item.spec.payload_json.clone()),
                            },
                            workflow_now_millis(),
                        )
                        .map_err(string_error)?;
                }
            }
            context
                .advance_to("executing", &checkpoint)
                .map_err(string_error)?;
            loop {
                let candidates = regional_relay_candidates(
                    repository.as_ref(),
                    &client,
                    broker.discover_candidates().map_err(string_error)?,
                    &region,
                )?;
                let Some(assigned) = repository
                    .claim_next_work_item(context.id(), workflow_now_millis())
                    .map_err(string_error)?
                else {
                    break;
                };
                let allocations =
                    match broker.allocate(assigned.id, assigned.state.revision, &candidates) {
                        Ok(allocations) => allocations,
                        Err(error) => {
                            repository
                                .transition_work_item(
                                    assigned.id,
                                    assigned.state.revision,
                                    WorkItemTransition::Waiting {
                                        checkpoint_json: assigned.state.checkpoint_json.clone(),
                                        reason: error.to_string(),
                                        retry_at_ms: Some(
                                            workflow_now_millis().saturating_add(300_000),
                                        ),
                                    },
                                    workflow_now_millis(),
                                )
                                .map_err(string_error)?;
                            break;
                        }
                    };
                let worker = survey_allocated_identity(&allocations, "worker", "replicant")?;
                let assignment_id = format!("event:{}:{worker}", assigned.id);
                repository
                    .assign_work_item(
                        assigned.id,
                        assigned.state.revision,
                        &assignment_id,
                        &ResourceKey::Replicant(worker.clone()),
                        workflow_now_millis(),
                    )
                    .map_err(string_error)?;
                let started = repository
                    .start_work_item(
                        assigned.id,
                        assigned.state.revision,
                        &worker,
                        &assignment_id,
                        workflow_now_millis(),
                    )
                    .map_err(string_error)?;
                let mission_json =
                    crate::automation::event_item_input_checkpoint(repository.as_ref(), &started)?;
                let (_, updated) = crate::automation::run_event_campaign_item(
                    repository.clone(),
                    client.clone(),
                    Arc::new(crate::automation::ManagedEventItemExecutor),
                    broker.clone(),
                    crate::automation::EventItemRun {
                        replacement_candidates: candidates.clone(),
                        item: started,
                        allocations,
                        mission_json,
                    },
                )
                .await?;
                checkpoint.plan_json = Some(updated);
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            match repository
                .aggregate_campaign_result(context.id())
                .map_err(string_error)?
            {
                Some(result) if result.workflow_status() == WorkflowStatus::Succeeded => {
                    context.mark_succeeded(Some(result)).map_err(string_error)
                }
                Some(result) => context
                    .mark_failed_with_result(
                        "event fulfillment completed without a successful criterion",
                        result,
                        replicant_workflow::WorkflowFailureDisposition::Permanent,
                    )
                    .map_err(string_error),
                None => context.mark_waiting().map_err(string_error),
            }
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RequirementActionConfig {
    requirement_id: String,
    quantity: u64,
    operation: FulfillmentOperation,
}

/// Structured activity payload stored as JSON for future protocol/UI use.
#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum WorkflowActivityEvent {
    /// Executor entered a durable step.
    StepEntered { step: String },
    /// A persisted exclusive resource claim was acquired or recovered.
    ResourceClaimed { resource: ResourceKey },
    /// A logical phase operation is about to execute.
    OperationSubmitted { step: String },
    /// A prior logical phase was reconciled as complete.
    OperationCompleted { step: String },
    /// The executor is waiting for managed state/SSE evidence.
    WaitReason { step: String, reason: String },
    /// A persisted checkpoint was compared with current managed state.
    ReconciliationDecision { step: String, decision: String },
    /// Workflow completed successfully.
    Completion,
    /// Workflow stopped with an error.
    Failure { error: String },
}

fn emit(context: &WorkflowContext, event: &WorkflowActivityEvent) -> Result<(), String> {
    context
        .emit_activity(serde_json::to_string(event).map_err(|error| error.to_string())?)
        .map_err(|error| error.to_string())
}

fn claim(context: &WorkflowContext, resource: ResourceKey) -> Result<(), String> {
    let outcome = context
        .acquire_claim(resource.clone())
        .map_err(|error| error.to_string())?;
    if matches!(outcome, ClaimAcquireOutcome::Acquired(_)) {
        emit(
            context,
            &WorkflowActivityEvent::ResourceClaimed { resource },
        )?;
    }
    Ok(())
}

fn reconcile_relay_autofactory_claims(
    context: &WorkflowContext,
    required: &BTreeSet<String>,
) -> Result<(), String> {
    let stale = context
        .claims()
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter_map(|claim| match claim.resource {
            ResourceKey::Autofactory(code) if !required.contains(&code) => {
                Some(ResourceKey::Autofactory(code))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    for resource in stale {
        context
            .release_claim(&resource)
            .map_err(|error| error.to_string())?;
    }
    for code in required {
        claim(context, ResourceKey::Autofactory(code.clone()))?;
    }
    Ok(())
}

pub(crate) type SurveyItemFuture<'a> =
    Pin<Box<dyn Future<Output = crate::survey::AnyResult<SurveyExecutionState>> + Send + 'a>>;

pub(crate) trait SurveyItemExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        client: &'a replicant_client::Client,
        state: &'a SurveyExecutionState,
        route_index: usize,
        allocations: &'a AllocationSet,
        timeouts: (Duration, Duration),
        checkpoints: tokio::sync::mpsc::UnboundedSender<SurveyExecutionState>,
    ) -> SurveyItemFuture<'a>;
}

struct ManagedSurveyItemExecutor;

impl SurveyItemExecutor for ManagedSurveyItemExecutor {
    fn execute<'a>(
        &'a self,
        client: &'a replicant_client::Client,
        state: &'a SurveyExecutionState,
        route_index: usize,
        allocations: &'a AllocationSet,
        timeouts: (Duration, Duration),
        checkpoints: tokio::sync::mpsc::UnboundedSender<SurveyExecutionState>,
    ) -> SurveyItemFuture<'a> {
        Box::pin(execute_survey_item(
            client,
            state,
            route_index,
            allocations,
            timeouts.0,
            timeouts.1,
            move |checkpoint| {
                checkpoints.send(checkpoint).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "survey checkpoint receiver closed",
                    )
                    .into()
                })
            },
        ))
    }
}

/// Factory for durable survey routes.
pub struct SurveyWorkflowFactory {
    kind: WorkflowKind,
    item_executor: Arc<dyn SurveyItemExecutor>,
}

impl SurveyWorkflowFactory {
    /// Creates the factory.
    pub fn new() -> Self {
        Self {
            kind: survey_workflow_kind(),
            item_executor: Arc::new(ManagedSurveyItemExecutor),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_item_executor(item_executor: Arc<dyn SurveyItemExecutor>) -> Self {
        Self {
            kind: survey_workflow_kind(),
            item_executor,
        }
    }
}
impl Default for SurveyWorkflowFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowFactory for SurveyWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    fn current_schema_version(&self) -> u32 {
        2
    }

    fn supports_schema_version(&self, version: u32) -> bool {
        matches!(version, 1 | 2)
    }

    fn migrate(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
    ) -> Result<Option<WorkflowMigration>, String> {
        if instance.schema_version == 2 {
            return Ok(None);
        }
        let legacy: LegacySurveyWorkflowConfig = instance.config().map_err(string_error)?;
        let legacy_worker = legacy.options.replicant.clone();
        let mut checkpoint: SurveyWorkflowCheckpoint =
            instance.checkpoint().map_err(string_error)?;
        checkpoint.migration_worker = Some(legacy_worker);
        let config = serde_json::to_value(SurveyWorkflowConfig::from_options(
            legacy.options,
            String::new(),
        ))
        .map_err(string_error)?;
        let checkpoint = serde_json::to_value(checkpoint).map_err(string_error)?;
        Ok(Some(WorkflowMigration::new(config, checkpoint)))
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(SurveyWorkflow {
            item_executor: self.item_executor.clone(),
        }))
    }
    fn placement_intents(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
        work_items: &[WorkItem],
    ) -> Result<WorkflowPlacementIntentProjection, String> {
        if instance.schema_version != self.current_schema_version() {
            return Ok(WorkflowPlacementIntentProjection::unknown());
        }
        let _: SurveyWorkflowConfig = instance.config().map_err(string_error)?;
        let checkpoint: SurveyWorkflowCheckpoint = instance.checkpoint().map_err(string_error)?;
        let mut intents = Vec::new();
        if let Some(state) = checkpoint.state {
            let state: PlacementSurveyState =
                serde_json::from_value(serde_json::to_value(state).map_err(string_error)?)
                    .map_err(string_error)?;
            let relation = state.relation();
            for code in std::iter::once(Some(state.vessel))
                .chain(std::iter::once(state.controller))
                .chain(state.drones.into_iter().map(Some))
                .flatten()
            {
                if let Some(subject) = placement_subject(&code)
                    && let Some(intent) =
                        status_intent(instance.status, subject, relation, None, None)
                {
                    intents.push(intent);
                }
            }
        }
        decode_typed_work_items::<PlacementSurveyItem>(work_items)?;
        Ok(complete_projection(intents))
    }
}

struct SurveyWorkflow {
    item_executor: Arc<dyn SurveyItemExecutor>,
}

impl WorkflowExecutor for SurveyWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        let item_executor = self.item_executor.clone();
        Box::pin(async move {
            let config: SurveyWorkflowConfig = context.config().map_err(string_error)?;
            let mut checkpoint: SurveyWorkflowCheckpoint =
                context.checkpoint().map_err(string_error)?;
            let client = context
                .managed_client()
                .cloned()
                .ok_or_else(|| "survey workflow requires a managed client".to_owned())?;
            let repository = context.repository_handle();
            let region = resolve_survey_region(
                repository.as_ref(),
                checkpoint.state.as_ref(),
                checkpoint.migration_worker.as_deref(),
                &config.region,
            )?
            .ok_or_else(|| "survey campaign has no Director region evidence".to_owned())?;
            let broker = crate::assignment::ResourceBroker::with_managed_client(
                repository.clone(),
                client.clone(),
            );
            let candidates = regional_relay_candidates(
                repository.as_ref(),
                &client,
                broker.discover_candidates().map_err(string_error)?,
                &region,
            )?;
            if checkpoint.state.is_none() {
                let worker = candidate_identity(&candidates, "replicant", None)
                    .ok_or_else(|| "survey planning has no regional Replicant".to_owned())?;
                let vessel = candidate_identity(&candidates, "device", Some("racing_vessel"))
                    .ok_or_else(|| "survey planning has no regional racing vessel".to_owned())?;
                let options = config.options_for_bundle(worker, vessel, None, None);
                let state = prepare_survey_workflow(&client, &options)
                    .await
                    .map_err(|error| error.to_string())?;
                checkpoint.state = Some(state);
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            let mut state = checkpoint
                .state
                .clone()
                .ok_or_else(|| "survey planning produced no checkpoint".to_owned())?;
            let bundle_capacities = survey_bundle_drone_capacities(&client, &candidates)?;
            let desired_items = survey_capacity_work_item_specs(
                context.id(),
                &state,
                &region,
                &bundle_capacities,
                &repository
                    .list_work_items(context.id())
                    .map_err(string_error)?,
            )?;
            let reconciled = repository
                .reconcile_work_items(context.id(), &desired_items, workflow_now_millis())
                .map_err(string_error)?;
            for item in reconciled {
                if item
                    .spec
                    .payload_json
                    .get("legacy_complete")
                    .and_then(Value::as_bool)
                    == Some(true)
                    && !item.state.status.is_terminal()
                {
                    repository
                        .transition_work_item(
                            item.id,
                            item.state.revision,
                            WorkItemTransition::Skipped {
                                reason: "completed in migrated survey checkpoint".to_owned(),
                                result_json: Some(serde_json::json!({
                                    "star": item.spec.payload_json["star"]
                                })),
                            },
                            workflow_now_millis(),
                        )
                        .map_err(string_error)?;
                }
            }
            context
                .advance_to("surveying", &checkpoint)
                .map_err(string_error)?;

            loop {
                let candidates = regional_relay_candidates(
                    repository.as_ref(),
                    &client,
                    broker.discover_candidates().map_err(string_error)?,
                    &region,
                )?;
                let mut running = Vec::new();
                while let Some(assigned) = repository
                    .claim_next_work_item(context.id(), workflow_now_millis())
                    .map_err(string_error)?
                {
                    let allocations =
                        match broker.allocate(assigned.id, assigned.state.revision, &candidates) {
                            Ok(allocations) => allocations,
                            Err(error) => {
                                repository
                                    .transition_work_item(
                                        assigned.id,
                                        assigned.state.revision,
                                        WorkItemTransition::Waiting {
                                            checkpoint_json: None,
                                            reason: error.to_string(),
                                            retry_at_ms: Some(
                                                workflow_now_millis().saturating_add(300_000),
                                            ),
                                        },
                                        workflow_now_millis(),
                                    )
                                    .map_err(string_error)?;
                                break;
                            }
                        };
                    let worker = survey_allocated_identity(&allocations, "worker", "replicant")?;
                    let assignment_id = format!("survey:{}:{worker}", assigned.id);
                    repository
                        .assign_work_item(
                            assigned.id,
                            assigned.state.revision,
                            &assignment_id,
                            &ResourceKey::Replicant(worker.clone()),
                            workflow_now_millis(),
                        )
                        .map_err(string_error)?;
                    let started = repository
                        .start_work_item(
                            assigned.id,
                            assigned.state.revision,
                            &worker,
                            &assignment_id,
                            workflow_now_millis(),
                        )
                        .map_err(string_error)?;
                    let star = started
                        .spec
                        .payload_json
                        .get("star")
                        .and_then(Value::as_str)
                        .ok_or_else(|| "survey item payload omitted star".to_owned())?;
                    let route_index = survey_item_route_index(&state, star)
                        .ok_or_else(|| format!("survey route omitted {star}"))?;
                    let repository = repository.clone();
                    let client = client.clone();
                    let broker = broker.clone();
                    let replacement_candidates = candidates.clone();
                    let item_state = state.clone();
                    let travel_timeout = config.travel_timeout;
                    let survey_timeout = config.survey_timeout;
                    let item_executor = item_executor.clone();
                    running.push(async move {
                        run_survey_item(
                            repository,
                            client,
                            broker,
                            item_executor,
                            SurveyItemRun {
                                replacement_candidates,
                                state: item_state,
                                item: started,
                                route_index,
                                allocations,
                                timeouts: (travel_timeout, survey_timeout),
                            },
                        )
                        .await
                    });
                }
                if running.is_empty() {
                    break;
                }
                for result in futures::future::join_all(running).await {
                    let (route_index, lane) = result?;
                    merge_survey_item_state(&mut state, &lane, route_index);
                    checkpoint.state = Some(state.clone());
                    context
                        .advance_to(state.step_name(), &checkpoint)
                        .map_err(string_error)?;
                }
            }

            match repository
                .aggregate_campaign_result(context.id())
                .map_err(string_error)?
            {
                Some(result) if result.workflow_status() == WorkflowStatus::Succeeded => context
                    .mark_succeeded(Some(summarize_plan(&state)))
                    .map_err(string_error),
                Some(result) => context
                    .mark_failed_with_result(
                        "survey campaign completed without a successful item",
                        result,
                        replicant_workflow::WorkflowFailureDisposition::Permanent,
                    )
                    .map_err(string_error),
                None => context.mark_waiting().map_err(string_error),
            }
        })
    }
}

struct SurveyItemRun {
    replacement_candidates: Vec<AllocationCandidate>,
    state: SurveyExecutionState,
    item: WorkItem,
    route_index: usize,
    allocations: AllocationSet,
    timeouts: (Duration, Duration),
}

async fn run_survey_item(
    repository: Arc<replicant_workflow::WorkflowRepository>,
    client: replicant_client::Client,
    broker: crate::assignment::ResourceBroker,
    item_executor: Arc<dyn SurveyItemExecutor>,
    run: SurveyItemRun,
) -> Result<(usize, SurveyExecutionState), String> {
    let SurveyItemRun {
        replacement_candidates,
        mut state,
        item,
        route_index,
        mut allocations,
        timeouts,
    } = run;
    let mut revision = item.state.revision;
    loop {
        let mut last_checkpoint = state.clone();
        let execution = {
            let (checkpoint_sender, mut checkpoint_receiver) =
                tokio::sync::mpsc::unbounded_channel();
            let execution = item_executor.execute(
                &client,
                &state,
                route_index,
                &allocations,
                timeouts,
                checkpoint_sender,
            );
            tokio::pin!(execution);
            loop {
                tokio::select! {
                    result = &mut execution => {
                        while let Ok(checkpoint) = checkpoint_receiver.try_recv() {
                            let stored = repository.transition_work_item(
                                item.id,
                                revision,
                                WorkItemTransition::CheckpointCommitted {
                                    checkpoint_json: serde_json::to_value(&checkpoint)
                                        .map_err(string_error)?,
                                },
                                workflow_now_millis(),
                            )
                            .map_err(string_error)?;
                            revision = stored.state.revision;
                            last_checkpoint = checkpoint;
                        }
                        break result;
                    }
                    Some(checkpoint) = checkpoint_receiver.recv() => {
                        let stored = repository.transition_work_item(
                            item.id,
                            revision,
                            WorkItemTransition::CheckpointCommitted {
                                checkpoint_json: serde_json::to_value(&checkpoint)
                                    .map_err(string_error)?,
                            },
                            workflow_now_millis(),
                        )
                        .map_err(string_error)?;
                        revision = stored.state.revision;
                        last_checkpoint = checkpoint;
                    }
                }
            }
        };
        match execution {
            Ok(lane) if survey_item_completed(&lane) => {
                repository
                    .transition_work_item(
                        item.id,
                        revision,
                        WorkItemTransition::Succeeded {
                            checkpoint_json: Some(
                                serde_json::to_value(&lane).map_err(string_error)?,
                            ),
                            result_json: Some(serde_json::json!({
                                "star": item.spec.payload_json["star"]
                            })),
                        },
                        workflow_now_millis(),
                    )
                    .map_err(string_error)?;
                if let Some(star) = item.spec.payload_json["star"].as_str() {
                    repository
                        .put_document(
                            "automation.scheduler.survey",
                            star,
                            &serde_json::json!({
                                "scanned_bodies": true,
                                "salvage_sites": [],
                                "committed_at_ms": workflow_now_millis(),
                            }),
                        )
                        .map_err(string_error)?;
                }
                return Ok((route_index, lane));
            }
            Ok(lane) => {
                repository
                    .transition_work_item(
                        item.id,
                        revision,
                        WorkItemTransition::RetryableFailure {
                            checkpoint_json: Some(
                                serde_json::to_value(&lane).map_err(string_error)?,
                            ),
                            error: "survey stop returned without completing".to_owned(),
                        },
                        workflow_now_millis(),
                    )
                    .map_err(string_error)?;
                return Ok((route_index, lane));
            }
            Err(error)
                if crate::failure::failure_class(error.as_ref())
                    == Some(crate::failure::FailureClass::DeviceTargetMissing) =>
            {
                if let Some((requirement, allocation_id)) =
                    missing_survey_allocation(&client, &allocations).await?
                {
                    match broker
                        .replace_dead_allocation_from(
                            item.id,
                            allocation_id,
                            &replacement_candidates,
                        )
                        .map_err(string_error)?
                    {
                        ReplacementOutcome::Replaced(replacement) => {
                            let values = allocations
                                .by_requirement
                                .get_mut(&requirement)
                                .ok_or_else(|| {
                                    format!("survey allocation omitted {requirement}")
                                })?;
                            let target = values
                                .iter_mut()
                                .find(|allocation| allocation.id == allocation_id)
                                .ok_or_else(|| {
                                    format!("survey allocation {allocation_id} disappeared")
                                })?;
                            *target = replacement;
                            state = last_checkpoint;
                            continue;
                        }
                        ReplacementOutcome::Waiting => {
                            repository
                                .transition_work_item(
                                    item.id,
                                    revision,
                                    WorkItemTransition::Waiting {
                                        checkpoint_json: Some(
                                            serde_json::to_value(&last_checkpoint)
                                                .map_err(string_error)?,
                                        ),
                                        reason: error.to_string(),
                                        retry_at_ms: Some(
                                            workflow_now_millis().saturating_add(300_000),
                                        ),
                                    },
                                    workflow_now_millis(),
                                )
                                .map_err(string_error)?;
                            return Ok((route_index, last_checkpoint));
                        }
                        ReplacementOutcome::Unavailable => {
                            return Ok((route_index, last_checkpoint));
                        }
                    }
                }
                let message = error.to_string();
                repository
                    .transition_work_item(
                        item.id,
                        revision,
                        WorkItemTransition::RetryableFailure {
                            checkpoint_json: Some(
                                serde_json::to_value(&last_checkpoint).map_err(string_error)?,
                            ),
                            error: message,
                        },
                        workflow_now_millis(),
                    )
                    .map_err(string_error)?;
                return Ok((route_index, last_checkpoint));
            }
            Err(error) => {
                let message = error.to_string();
                let waiting = matches!(
                    crate::failure::failure_class(error.as_ref()),
                    Some(
                        crate::failure::FailureClass::EventControlUnavailable
                            | crate::failure::FailureClass::ConnectivityDependency
                            | crate::failure::FailureClass::ManufacturingCapacity
                            | crate::failure::FailureClass::TransientUpstream
                    )
                );
                let transition = if waiting {
                    WorkItemTransition::Waiting {
                        checkpoint_json: Some(
                            serde_json::to_value(&last_checkpoint).map_err(string_error)?,
                        ),
                        reason: message,
                        retry_at_ms: Some(workflow_now_millis().saturating_add(300_000)),
                    }
                } else {
                    WorkItemTransition::RetryableFailure {
                        checkpoint_json: Some(
                            serde_json::to_value(&last_checkpoint).map_err(string_error)?,
                        ),
                        error: message,
                    }
                };
                repository
                    .transition_work_item(item.id, revision, transition, workflow_now_millis())
                    .map_err(string_error)?;
                return Ok((route_index, last_checkpoint));
            }
        }
    }
}

async fn missing_survey_allocation(
    client: &replicant_client::Client,
    allocations: &AllocationSet,
) -> Result<Option<(String, replicant_workflow::AllocationId)>, String> {
    for (requirement, values) in &allocations.by_requirement {
        for allocation in values {
            let ResourceKey::Device(code) = &allocation.resource else {
                continue;
            };
            if let Err(error) = client.devices().get(code).await
                && crate::failure::device_fetch_is_missing(&error)
            {
                return Ok(Some((requirement.clone(), allocation.id)));
            }
        }
    }
    Ok(None)
}

fn survey_allocated_identity(
    allocations: &AllocationSet,
    requirement: &str,
    expected: &str,
) -> Result<String, String> {
    allocations
        .by_requirement
        .get(requirement)
        .and_then(|values| values.first())
        .and_then(|allocation| match (&allocation.resource, expected) {
            (ResourceKey::Replicant(code), "replicant") | (ResourceKey::Device(code), "device") => {
                Some(code.clone())
            }
            _ => None,
        })
        .ok_or_else(|| format!("survey allocation omitted {requirement}"))
}

fn candidate_identity(
    candidates: &[AllocationCandidate],
    kind: &str,
    capability: Option<&str>,
) -> Option<String> {
    candidates.iter().find_map(|candidate| {
        (candidate.kind == kind
            && capability.is_none_or(|required| {
                candidate.capabilities.iter().any(|value| value == required)
            }))
        .then(|| match &candidate.resource {
            ResourceKey::Replicant(code) | ResourceKey::Device(code) => Some(code.clone()),
            _ => None,
        })
        .flatten()
    })
}

fn resolve_survey_region(
    repository: &replicant_workflow::WorkflowRepository,
    state: Option<&SurveyExecutionState>,
    migration_worker: Option<&str>,
    configured: &str,
) -> Result<Option<String>, String> {
    if !configured.is_empty() {
        return Ok(Some(configured.to_owned()));
    }
    let worker = state
        .map(|state| survey_checkpoint_identities(state).0)
        .or(migration_worker);
    if let Some(worker) = worker
        && let Some((value, _)) = repository
            .read_document("director.replicant", worker)
            .map_err(string_error)?
    {
        return Ok(value
            .get("region")
            .and_then(Value::as_str)
            .map(str::to_owned));
    }
    Ok(None)
}

pub(crate) type RelayTripFuture<'a> =
    Pin<Box<dyn Future<Output = crate::relay::AnyResult<RelayExecutionState>> + Send + 'a>>;

pub(crate) trait RelayTripExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        client: &'a replicant_client::Client,
        state: &'a RelayExecutionState,
        stop_indices: &'a [usize],
        allocations: &'a AllocationSet,
        wait_timeout: Duration,
        checkpoints: tokio::sync::mpsc::UnboundedSender<RelayExecutionState>,
    ) -> RelayTripFuture<'a>;
}

struct ManagedRelayTripExecutor;

impl RelayTripExecutor for ManagedRelayTripExecutor {
    fn execute<'a>(
        &'a self,
        client: &'a replicant_client::Client,
        state: &'a RelayExecutionState,
        stop_indices: &'a [usize],
        allocations: &'a AllocationSet,
        wait_timeout: Duration,
        checkpoints: tokio::sync::mpsc::UnboundedSender<RelayExecutionState>,
    ) -> RelayTripFuture<'a> {
        Box::pin(execute_relay_trip(
            client,
            state,
            stop_indices,
            allocations,
            wait_timeout,
            move |checkpoint| {
                checkpoints.send(checkpoint).map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "relay checkpoint receiver closed",
                    )
                    .into()
                })
            },
        ))
    }
}

/// Factory for durable relay expansion.
pub struct RelayWorkflowFactory {
    kind: WorkflowKind,
    trip_executor: Arc<dyn RelayTripExecutor>,
}

impl RelayWorkflowFactory {
    /// Creates the factory.
    pub fn new() -> Self {
        Self {
            kind: relay_workflow_kind(),
            trip_executor: Arc::new(ManagedRelayTripExecutor),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_trip_executor(trip_executor: Arc<dyn RelayTripExecutor>) -> Self {
        Self {
            kind: relay_workflow_kind(),
            trip_executor,
        }
    }
}

impl Default for RelayWorkflowFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowFactory for RelayWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }
    fn current_schema_version(&self) -> u32 {
        2
    }

    fn supports_schema_version(&self, version: u32) -> bool {
        matches!(version, 1 | 2)
    }

    fn migrate(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
    ) -> Result<Option<WorkflowMigration>, String> {
        if instance.schema_version == 2 {
            return Ok(None);
        }
        let legacy: LegacyRelayWorkflowConfig =
            instance.config().map_err(|error| error.to_string())?;
        let checkpoint: RelayWorkflowCheckpoint =
            instance.checkpoint().map_err(|error| error.to_string())?;
        let config = serde_json::to_value(RelayWorkflowConfig::from_request(legacy.request))
            .map_err(|error| error.to_string())?;
        let checkpoint = serde_json::to_value(checkpoint).map_err(|error| error.to_string())?;
        Ok(Some(WorkflowMigration::new(config, checkpoint)))
    }

    fn placement_intents(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
        work_items: &[WorkItem],
    ) -> Result<WorkflowPlacementIntentProjection, String> {
        if instance.schema_version != self.current_schema_version() {
            return Ok(WorkflowPlacementIntentProjection::unknown());
        }
        let _: RelayWorkflowConfig = instance.config().map_err(string_error)?;
        let checkpoint: RelayWorkflowCheckpoint = instance.checkpoint().map_err(string_error)?;
        let mut intents = Vec::new();
        if let Some(state) = checkpoint.state {
            let state: PlacementRelayState =
                serde_json::from_value(serde_json::to_value(state).map_err(string_error)?)
                    .map_err(string_error)?;
            if placement_status_is_live(instance.status) {
                for tag in state.legacy_mission_tags {
                    if !tag.trim().is_empty() {
                        intents.push(placement_intent(
                            WorkflowPlacementIntentSubject::DeviceTag(tag),
                            WorkflowPlacementIntentRelation::Awaited,
                            None,
                            None,
                        ));
                    }
                }
            }
            let staged = state.stops.iter().any(|stop| stop.relay_code.is_some())
                || state.print_jobs.iter().any(|job| job.submitted)
                || state
                    .supply
                    .as_ref()
                    .is_some_and(|supply| supply.carriers.iter().any(|carrier| carrier.dispatched));
            if let Some(subject) = placement_subject(&state.vessel_code)
                && let Some(intent) = status_intent(
                    instance.status,
                    subject,
                    staged.then_some(WorkflowPlacementIntentRelation::Staged),
                    None,
                    None,
                )
            {
                intents.push(intent);
            }
            if let Some(code) = state.dsr_carrier_code
                && let Some(subject) = placement_subject(&code)
                && let Some(intent) = status_intent(
                    instance.status,
                    subject,
                    staged.then_some(WorkflowPlacementIntentRelation::Staged),
                    None,
                    None,
                )
            {
                intents.push(intent);
            }
            for stop in state.stops {
                let Some(code) = stop.relay_code else {
                    continue;
                };
                let Some(subject) = placement_subject(&code) else {
                    continue;
                };
                let deployed = stop.completed && !stop.location.trim().is_empty();
                let relation = if deployed {
                    WorkflowPlacementIntentRelation::Deployed
                } else {
                    WorkflowPlacementIntentRelation::Staged
                };
                if let Some(intent) = status_intent(
                    instance.status,
                    subject,
                    Some(relation),
                    None,
                    deployed.then_some(stop.location),
                ) {
                    intents.push(intent);
                }
            }
            for print in state.print_jobs {
                if placement_status_is_live(instance.status) {
                    for tag in [print.mission_tag.clone(), print.site_tag.clone()] {
                        if !tag.trim().is_empty() {
                            intents.push(placement_intent(
                                WorkflowPlacementIntentSubject::DeviceTag(tag),
                                WorkflowPlacementIntentRelation::Awaited,
                                None,
                                None,
                            ));
                        }
                    }
                    if let Some(tag) = print.batch_tag.clone()
                        && !tag.trim().is_empty()
                    {
                        intents.push(placement_intent(
                            WorkflowPlacementIntentSubject::DeviceTag(tag),
                            WorkflowPlacementIntentRelation::Awaited,
                            None,
                            None,
                        ));
                    }
                }
                if let Some(code) = print.relay_code
                    && print.submitted
                    && let Some(subject) = placement_subject(&code)
                    && let Some(intent) = status_intent(
                        instance.status,
                        subject,
                        Some(WorkflowPlacementIntentRelation::Staged),
                        None,
                        None,
                    )
                {
                    intents.push(intent);
                }
            }
            if let Some(supply) = state.supply {
                for carrier in supply.carriers {
                    let Some(subject) = placement_subject(&carrier.code) else {
                        continue;
                    };
                    let relation = if carrier.dispatched && !carrier.returned_home {
                        Some(WorkflowPlacementIntentRelation::Transported)
                    } else {
                        Some(WorkflowPlacementIntentRelation::Staged)
                    };
                    if let Some(intent) =
                        status_intent(instance.status, subject, relation, None, None)
                    {
                        intents.push(intent);
                    }
                }
                for restock in supply.restocks {
                    if !restock.completed {
                        if let Some(subject) = placement_subject(&restock.carrier_code)
                            && let Some(intent) = status_intent(
                                instance.status,
                                subject,
                                Some(WorkflowPlacementIntentRelation::Transported),
                                None,
                                None,
                            )
                        {
                            intents.push(intent);
                        }
                        for code in restock.confirmed_detached_relays {
                            if let Some(subject) = placement_subject(&code)
                                && let Some(intent) = status_intent(
                                    instance.status,
                                    subject,
                                    Some(WorkflowPlacementIntentRelation::Staged),
                                    None,
                                    None,
                                )
                            {
                                intents.push(intent);
                            }
                        }
                    }
                }
            }
        }
        decode_typed_work_items::<PlacementRelayItem>(work_items)?;
        Ok(complete_projection(intents))
    }
    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(RelayWorkflow {
            trip_executor: self.trip_executor.clone(),
        }))
    }
}

struct RelayWorkflow {
    trip_executor: Arc<dyn RelayTripExecutor>,
}

impl WorkflowExecutor for RelayWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        let trip_executor = self.trip_executor.clone();

        Box::pin(async move {
            let config: RelayWorkflowConfig = context.config().map_err(string_error)?;
            let mut checkpoint: RelayWorkflowCheckpoint =
                context.checkpoint().map_err(string_error)?;
            let client = context
                .managed_client()
                .cloned()
                .ok_or_else(|| "relay workflow requires a managed client".to_owned())?;
            let repository = context.repository_handle();
            let broker = crate::assignment::ResourceBroker::with_managed_client(
                repository.clone(),
                client.clone(),
            );
            let Some(region) = resolve_relay_region(
                repository.as_ref(),
                checkpoint.state.as_ref(),
                config.region.as_deref().or(checkpoint.region.as_deref()),
            )?
            else {
                return context.mark_waiting().map_err(string_error);
            };
            checkpoint.region = Some(region.clone());

            if checkpoint.state.is_none() {
                let candidates = regional_relay_candidates(
                    repository.as_ref(),
                    &client,
                    broker.discover_candidates().map_err(string_error)?,
                    &region,
                )?;
                let worker = candidates
                    .iter()
                    .filter_map(|candidate| match &candidate.resource {
                        ResourceKey::Replicant(code) => Some(code.as_str()),
                        _ => None,
                    })
                    .next()
                    .ok_or_else(|| "relay planning has no eligible Replicant".to_owned())?;
                let request = config.request_for_worker(worker.to_owned());
                let state = prepare_relay_workflow(&client, &request, worker)
                    .await
                    .map_err(|error| error.to_string())?;
                checkpoint.state = Some(state);
                context
                    .persist_checkpoint(&checkpoint)
                    .map_err(string_error)?;
            }
            let mut state = checkpoint
                .state
                .clone()
                .ok_or_else(|| "relay planning produced no checkpoint".to_owned())?;
            repository
                .reconcile_work_items(
                    context.id(),
                    &relay_work_item_specs(context.id(), &state, &region).map_err(string_error)?,
                    workflow_now_millis(),
                )
                .map_err(string_error)?;

            loop {
                revalidate_relay_work_items(
                    &client,
                    repository.as_ref(),
                    context.id(),
                    &state,
                    workflow_now_millis(),
                )
                .await
                .map_err(|error| error.to_string())?;
                let candidates = regional_relay_candidates(
                    repository.as_ref(),
                    &client,
                    broker.discover_candidates().map_err(string_error)?,
                    &region,
                )?;
                let pending = repository
                    .list_work_items(context.id())
                    .map_err(string_error)?
                    .into_iter()
                    .filter(|item| {
                        matches!(
                            item.state.status,
                            WorkItemStatus::Pending | WorkItemStatus::Waiting
                        )
                    })
                    .collect::<Vec<_>>();
                if pending.is_empty() {
                    break;
                }
                let relay_keys = pending
                    .iter()
                    .filter_map(|item| {
                        item.spec
                            .payload_json
                            .get("system")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    })
                    .collect::<Vec<_>>();
                let carrier_capacities =
                    relay_carrier_capacities(&candidates, relay_planned_transport_capacity(&state));
                let assignment = elastic_relay_assignment(&relay_keys, &carrier_capacities);
                let mut running = Vec::new();
                for lane in assignment.lanes {
                    let Some(trip) = lane.trips.first() else {
                        continue;
                    };
                    let Some(leader) = repository
                        .claim_next_work_item(context.id(), workflow_now_millis())
                        .map_err(string_error)?
                    else {
                        break;
                    };
                    let lane_candidates = relay_lane_candidates(&candidates, lane.carrier.as_str());
                    let allocations =
                        match broker.allocate(leader.id, leader.state.revision, &lane_candidates) {
                            Ok(allocations) => allocations,
                            Err(error) => {
                                repository
                                    .transition_work_item(
                                        leader.id,
                                        leader.state.revision,
                                        WorkItemTransition::Waiting {
                                            checkpoint_json: None,
                                            reason: error.to_string(),
                                            retry_at_ms: Some(
                                                workflow_now_millis().saturating_add(300_000),
                                            ),
                                        },
                                        workflow_now_millis(),
                                    )
                                    .map_err(string_error)?;
                                continue;
                            }
                        };
                    let worker = relay_allocated_worker(&allocations)
                        .ok_or_else(|| "relay allocation omitted its Replicant".to_owned())?;
                    let trip_len = trip.len().min(relay_allocated_stow(&allocations));
                    if trip_len == 0 {
                        return Err("relay allocation omitted usable stow capacity".to_owned());
                    }
                    let mut items = vec![start_relay_lane_item(
                        repository.as_ref(),
                        leader,
                        &worker,
                        true,
                    )?];
                    for _ in 1..trip_len {
                        let Some(item) = repository
                            .claim_next_work_item(context.id(), workflow_now_millis())
                            .map_err(string_error)?
                        else {
                            break;
                        };
                        items.push(start_relay_lane_item(
                            repository.as_ref(),
                            item,
                            &worker,
                            false,
                        )?);
                    }
                    let indices = items
                        .iter()
                        .map(|item| relay_item_index(&state, item))
                        .collect::<Result<Vec<_>, _>>()?;
                    let repository = repository.clone();
                    let broker = broker.clone();
                    let client = client.clone();
                    let replacement_candidates = candidates.clone();
                    let lane_state = state.clone();
                    let wait_timeout =
                        Duration::new(config.wait_timeout_seconds, config.wait_timeout_nanoseconds);
                    let trip_executor = trip_executor.clone();
                    running.push(async move {
                        run_relay_lane(
                            repository,
                            broker,
                            client,
                            trip_executor,
                            RelayLaneRun {
                                replacement_candidates,
                                state: lane_state,
                                items,
                                stop_indices: indices,
                                allocations,
                                wait_timeout,
                            },
                        )
                        .await
                    });
                }
                if running.is_empty() {
                    break;
                }
                let lane_results = futures::future::join_all(running).await;
                for result in lane_results {
                    let result = result?;
                    merge_relay_trip_state(&mut state, &result.state, &result.stop_indices);
                    finish_relay_lane(repository.as_ref(), result)?;
                    checkpoint.state = Some(state.clone());
                    context
                        .advance_to(state.step_name(), &checkpoint)
                        .map_err(string_error)?;
                }
            }

            match repository
                .aggregate_campaign_result(context.id())
                .map_err(string_error)?
            {
                Some(result) if result.workflow_status() == WorkflowStatus::Succeeded => {
                    reconcile_relay_autofactory_claims(context, &BTreeSet::new())?;
                    emit(context, &WorkflowActivityEvent::Completion)?;
                    context
                        .mark_succeeded(Some(relay_expansion_report(state)))
                        .map_err(string_error)
                }
                Some(result) => context
                    .mark_failed_with_result(
                        "relay campaign completed without a successful item",
                        result,
                        replicant_workflow::WorkflowFailureDisposition::Permanent,
                    )
                    .map_err(string_error),
                None => context.mark_waiting().map_err(string_error),
            }
        })
    }
}

#[derive(Clone, Copy)]
enum RelayLaneVerdict {
    Succeeded,
    AlreadySatisfied(usize),
    Waiting,
    RetryableFailure,
    ReplacementUnavailable,
}

struct RelayLaneResult {
    state: RelayExecutionState,
    stop_indices: Vec<usize>,
    items: Vec<(replicant_workflow::WorkItemId, usize, u64)>,
    verdict: RelayLaneVerdict,
    error: Option<String>,
}

struct RelayLaneRun {
    replacement_candidates: Vec<AllocationCandidate>,
    state: RelayExecutionState,
    items: Vec<WorkItem>,
    stop_indices: Vec<usize>,
    allocations: AllocationSet,
    wait_timeout: Duration,
}

async fn run_relay_lane(
    repository: Arc<replicant_workflow::WorkflowRepository>,
    broker: crate::assignment::ResourceBroker,
    client: replicant_client::Client,
    trip_executor: Arc<dyn RelayTripExecutor>,
    run: RelayLaneRun,
) -> Result<RelayLaneResult, String> {
    let RelayLaneRun {
        replacement_candidates,
        mut state,
        items,
        stop_indices,
        mut allocations,
        wait_timeout,
    } = run;
    let mut item_revisions = items
        .iter()
        .map(|item| {
            Ok::<_, String>((
                item.id,
                relay_item_index(&state, item)?,
                item.state.revision,
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    loop {
        let mut last_checkpoint = state.clone();
        let execution = {
            let (checkpoint_sender, mut checkpoint_receiver) =
                tokio::sync::mpsc::unbounded_channel();
            let execution = trip_executor.execute(
                &client,
                &state,
                &stop_indices,
                &allocations,
                wait_timeout,
                checkpoint_sender,
            );
            tokio::pin!(execution);
            loop {
                tokio::select! {
                    result = &mut execution => {
                        while let Ok(lane) = checkpoint_receiver.try_recv() {
                            persist_relay_lane_checkpoint(
                                repository.as_ref(),
                                &mut item_revisions,
                                &mut last_checkpoint,
                                lane,
                            )?;
                        }
                        break result;
                    }
                    Some(lane) = checkpoint_receiver.recv() => {
                        persist_relay_lane_checkpoint(
                            repository.as_ref(),
                            &mut item_revisions,
                            &mut last_checkpoint,
                            lane,
                        )?;
                    }
                }
            }
        };
        match execution {
            Ok(lane) => {
                return Ok(RelayLaneResult {
                    state: lane,
                    stop_indices,
                    items: item_revisions,
                    verdict: RelayLaneVerdict::Succeeded,
                    error: None,
                });
            }
            Err(error) if relay_coverage_satisfied_stop(error.as_ref()).is_some() => {
                let stop_index = relay_coverage_satisfied_stop(error.as_ref())
                    .ok_or_else(|| "relay coverage preflight omitted its stop".to_owned())?;
                return Ok(RelayLaneResult {
                    state: last_checkpoint,
                    stop_indices,
                    items: item_revisions,
                    verdict: RelayLaneVerdict::AlreadySatisfied(stop_index),
                    error: Some(error.to_string()),
                });
            }
            Err(error)
                if crate::failure::failure_class(error.as_ref())
                    == Some(crate::failure::FailureClass::DeviceTargetMissing) =>
            {
                let carrier = allocations
                    .by_requirement
                    .get("carrier")
                    .and_then(|values| values.first())
                    .ok_or_else(|| "relay allocation omitted its carrier".to_owned())?;
                match broker
                    .replace_dead_allocation_from(items[0].id, carrier.id, &replacement_candidates)
                    .map_err(string_error)?
                {
                    ReplacementOutcome::Replaced(replacement) => {
                        let stow = allocations
                            .by_requirement
                            .get("stow")
                            .and_then(|values| values.first())
                            .ok_or_else(|| "relay allocation omitted its stow pool".to_owned())?;
                        let replacement_stow = match broker
                            .replace_dead_allocation_from(
                                items[0].id,
                                stow.id,
                                &replacement_candidates,
                            )
                            .map_err(string_error)?
                        {
                            ReplacementOutcome::Replaced(replacement) => replacement,
                            ReplacementOutcome::Waiting => {
                                return Ok(RelayLaneResult {
                                    state: last_checkpoint,
                                    stop_indices,
                                    items: item_revisions,
                                    verdict: RelayLaneVerdict::Waiting,
                                    error: Some(error.to_string()),
                                });
                            }
                            ReplacementOutcome::Unavailable => {
                                return Ok(RelayLaneResult {
                                    state: last_checkpoint,
                                    stop_indices,
                                    items: item_revisions,
                                    verdict: RelayLaneVerdict::ReplacementUnavailable,
                                    error: Some(error.to_string()),
                                });
                            }
                        };
                        allocations
                            .by_requirement
                            .insert("carrier".to_owned(), vec![replacement]);
                        allocations
                            .by_requirement
                            .insert("stow".to_owned(), vec![replacement_stow]);
                        state = last_checkpoint;
                    }
                    ReplacementOutcome::Waiting => {
                        return Ok(RelayLaneResult {
                            state: last_checkpoint,
                            stop_indices,
                            items: item_revisions,
                            verdict: RelayLaneVerdict::Waiting,
                            error: Some(error.to_string()),
                        });
                    }
                    ReplacementOutcome::Unavailable => {
                        return Ok(RelayLaneResult {
                            state: last_checkpoint,
                            stop_indices,
                            items: item_revisions,
                            verdict: RelayLaneVerdict::ReplacementUnavailable,
                            error: Some(error.to_string()),
                        });
                    }
                }
            }
            Err(error) => {
                return Ok(RelayLaneResult {
                    state: last_checkpoint,
                    stop_indices,
                    items: item_revisions,
                    verdict: RelayLaneVerdict::RetryableFailure,
                    error: Some(error.to_string()),
                });
            }
        }
    }
}

fn persist_relay_lane_checkpoint(
    repository: &replicant_workflow::WorkflowRepository,
    item_revisions: &mut [(replicant_workflow::WorkItemId, usize, u64)],
    last_checkpoint: &mut RelayExecutionState,
    lane: RelayExecutionState,
) -> Result<(), String> {
    let checkpoint_json = serde_json::to_value(&lane).map_err(string_error)?;
    for (item_id, _, revision) in item_revisions {
        let item = repository
            .transition_work_item(
                *item_id,
                *revision,
                WorkItemTransition::CheckpointCommitted {
                    checkpoint_json: checkpoint_json.clone(),
                },
                workflow_now_millis(),
            )
            .map_err(string_error)?;
        *revision = item.state.revision;
    }
    *last_checkpoint = lane;
    Ok(())
}

fn finish_relay_lane(
    repository: &replicant_workflow::WorkflowRepository,
    result: RelayLaneResult,
) -> Result<(), String> {
    for (position, (item_id, stop_index, revision)) in result.items.iter().enumerate() {
        let completed = relay_stop_completed(&result.state, *stop_index);
        let transition = if completed {
            WorkItemTransition::Succeeded {
                checkpoint_json: Some(serde_json::to_value(&result.state).map_err(string_error)?),
                result_json: Some(serde_json::json!({ "stop_index": stop_index })),
            }
        } else {
            match result.verdict {
                RelayLaneVerdict::AlreadySatisfied(satisfied) if *stop_index == satisfied => {
                    WorkItemTransition::Skipped {
                        reason: "relay coverage became live before deployment".to_owned(),
                        result_json: Some(serde_json::json!({ "stop_index": stop_index })),
                    }
                }
                RelayLaneVerdict::AlreadySatisfied(_) => WorkItemTransition::Reclaimed {
                    checkpoint_json: Some(
                        serde_json::to_value(&result.state).map_err(string_error)?,
                    ),
                },
                RelayLaneVerdict::Succeeded => WorkItemTransition::RetryableFailure {
                    checkpoint_json: Some(
                        serde_json::to_value(&result.state).map_err(string_error)?,
                    ),
                    error: "relay trip returned without completing its stop".to_owned(),
                },
                RelayLaneVerdict::Waiting => WorkItemTransition::Waiting {
                    checkpoint_json: Some(
                        serde_json::to_value(&result.state).map_err(string_error)?,
                    ),
                    reason: result
                        .error
                        .clone()
                        .unwrap_or_else(|| "replacement busy".into()),
                    retry_at_ms: Some(workflow_now_millis().saturating_add(300_000)),
                },
                RelayLaneVerdict::RetryableFailure => WorkItemTransition::RetryableFailure {
                    checkpoint_json: Some(
                        serde_json::to_value(&result.state).map_err(string_error)?,
                    ),
                    error: result
                        .error
                        .clone()
                        .unwrap_or_else(|| "relay trip failed".into()),
                },
                RelayLaneVerdict::ReplacementUnavailable if position == 0 => {
                    continue;
                }
                RelayLaneVerdict::ReplacementUnavailable => WorkItemTransition::Reclaimed {
                    checkpoint_json: Some(
                        serde_json::to_value(&result.state).map_err(string_error)?,
                    ),
                },
            }
        };
        repository
            .transition_work_item(*item_id, *revision, transition, workflow_now_millis())
            .map_err(string_error)?;
    }
    Ok(())
}

fn start_relay_lane_item(
    repository: &replicant_workflow::WorkflowRepository,
    item: WorkItem,
    worker: &str,
    persist_assignment: bool,
) -> Result<WorkItem, String> {
    let assignment_id = format!("relay:{}:{worker}", item.id);
    if persist_assignment {
        repository
            .assign_work_item(
                item.id,
                item.state.revision,
                &assignment_id,
                &ResourceKey::Replicant(worker.to_owned()),
                workflow_now_millis(),
            )
            .map_err(string_error)?;
    }
    repository
        .start_work_item(
            item.id,
            item.state.revision,
            worker,
            &assignment_id,
            workflow_now_millis(),
        )
        .map_err(string_error)
}

fn relay_item_index(state: &RelayExecutionState, item: &WorkItem) -> Result<usize, String> {
    let system = item
        .spec
        .payload_json
        .get("system")
        .and_then(Value::as_str)
        .ok_or_else(|| "relay item payload omitted system".to_owned())?;
    let location = item
        .spec
        .payload_json
        .get("location")
        .and_then(Value::as_str)
        .ok_or_else(|| "relay item payload omitted location".to_owned())?;
    relay_item_stop_index(state, system, location)
        .ok_or_else(|| format!("relay item {system}/{location} is absent from its checkpoint"))
}

fn relay_allocated_worker(allocations: &AllocationSet) -> Option<String> {
    allocations
        .by_requirement
        .get("worker")?
        .iter()
        .find_map(|allocation| match &allocation.resource {
            ResourceKey::Replicant(code) => Some(code.clone()),
            _ => None,
        })
}

fn relay_allocated_stow(allocations: &AllocationSet) -> usize {
    allocations
        .by_requirement
        .get("stow")
        .into_iter()
        .flatten()
        .map(|allocation| usize::try_from(allocation.quantity).unwrap_or(usize::MAX))
        .sum()
}

fn resolve_relay_region(
    repository: &replicant_workflow::WorkflowRepository,
    state: Option<&RelayExecutionState>,
    configured: Option<&str>,
) -> Result<Option<String>, String> {
    if let Some(region) = configured.filter(|region| !region.is_empty()) {
        return Ok(Some(region.to_owned()));
    }
    if let Some(state) = state
        && let Some((value, _)) = repository
            .read_document("director.replicant", relay_checkpoint_worker(state))
            .map_err(string_error)?
        && let Some(region) = value.get("region").and_then(Value::as_str)
        && !region.is_empty()
    {
        return Ok(Some(region.to_owned()));
    }
    let regions = repository
        .list_documents("director.replicant")
        .map_err(string_error)?
        .into_iter()
        .filter_map(|(_, value, _)| {
            value
                .get("region")
                .and_then(Value::as_str)
                .filter(|region| !region.is_empty())
                .map(str::to_owned)
        })
        .collect::<BTreeSet<_>>();
    Ok((regions.len() == 1)
        .then(|| regions.into_iter().next())
        .flatten())
}

pub(crate) fn regional_relay_candidates(
    repository: &replicant_workflow::WorkflowRepository,
    client: &replicant_client::Client,
    mut candidates: Vec<AllocationCandidate>,
    region: &str,
) -> Result<Vec<AllocationCandidate>, String> {
    let requested_region = crate::canonical_region(region);
    let workers = repository
        .list_documents("director.replicant")
        .map_err(string_error)?
        .into_iter()
        .filter_map(|(worker, value, _)| {
            value
                .get("region")
                .and_then(Value::as_str)
                .is_some_and(|worker_region| {
                    crate::canonical_region(worker_region) == requested_region
                })
                .then_some(worker)
        })
        .collect::<BTreeSet<_>>();
    let state = client.state();
    let mut regional_devices = state
        .owned_replicants()
        .map_err(string_error)?
        .into_iter()
        .filter(|replicant| workers.contains(replicant.key.id.as_str()))
        .filter_map(|replicant| {
            replicant
                .hosted_device
                .map(|device| device.id.as_str().to_owned())
        })
        .collect::<BTreeSet<_>>();
    regional_devices.extend(
        state
            .owned_devices()
            .map_err(string_error)?
            .into_iter()
            .filter(|device| {
                device
                    .relationships
                    .assigned_replicant
                    .as_ref()
                    .is_some_and(|replicant| workers.contains(replicant.id.as_str()))
            })
            .map(|device| device.key.id.as_str().to_owned()),
    );

    let system_regions =
        crate::orchestration::expanded_system_region_map(&client.galaxy().catalogue());
    for candidate in &mut candidates {
        if let Some(location) = candidate.location.as_mut()
            && let Some(designation) = location.designation.as_deref()
            && let Some(system) = allocation_system_for_designation(designation, &system_regions)
        {
            location.region = system_regions.get(&system).cloned();
            location.system = Some(system);
        }
    }

    candidates.retain(|candidate| {
        allocation_candidate_belongs_to_region(
            candidate,
            &requested_region,
            &workers,
            &regional_devices,
        )
    });
    for candidate in &mut candidates {
        candidate.location.get_or_insert_default().region = Some(region.to_owned());
    }
    Ok(candidates)
}

fn allocation_candidate_belongs_to_region(
    candidate: &AllocationCandidate,
    requested_region: &str,
    workers: &BTreeSet<String>,
    regional_devices: &BTreeSet<String>,
) -> bool {
    let physical_region = candidate
        .location
        .as_ref()
        .and_then(|location| location.region.as_deref())
        .map(crate::canonical_region);
    match &candidate.resource {
        ResourceKey::Replicant(code) => workers.contains(code),
        ResourceKey::Device(code) | ResourceKey::Autofactory(code) => {
            physical_region.as_deref().map_or_else(
                || regional_devices.contains(code),
                |physical| physical == requested_region,
            )
        }
        ResourceKey::Namespaced { namespace, key } if namespace == "stow" => {
            physical_region.as_deref().map_or_else(
                || regional_devices.contains(key),
                |physical| physical == requested_region,
            )
        }
        ResourceKey::Namespaced { namespace, .. } if namespace == "inventory" => physical_region
            .as_deref()
            .is_none_or(|physical| physical == requested_region),
        _ => false,
    }
}

fn allocation_system_for_designation(
    designation: &str,
    system_regions: &BTreeMap<String, String>,
) -> Option<String> {
    let mut candidate = designation;
    loop {
        if system_regions.contains_key(candidate) {
            return Some(candidate.to_owned());
        }
        let (parent, _) = candidate.rsplit_once('-')?;
        candidate = parent;
    }
}

fn survey_bundle_drone_capacities(
    client: &replicant_client::Client,
    candidates: &[AllocationCandidate],
) -> Result<Vec<usize>, String> {
    let candidate_workers = candidates
        .iter()
        .filter_map(|candidate| match &candidate.resource {
            ResourceKey::Replicant(code) => Some(code.as_str()),
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    let candidate_devices = candidates
        .iter()
        .filter_map(|candidate| match &candidate.resource {
            ResourceKey::Device(code) => Some((code.as_str(), candidate.capabilities.as_slice())),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let stow_capacity = candidates
        .iter()
        .filter_map(|candidate| match &candidate.resource {
            ResourceKey::Namespaced { namespace, key } if namespace == "stow" => {
                usize::try_from(candidate.available_quantity)
                    .ok()
                    .map(|quantity| (key.as_str(), quantity))
            }
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut drones = BTreeMap::<String, usize>::new();
    let mut vessels = BTreeMap::<String, usize>::new();
    let mut controllers = BTreeSet::<String>::new();
    for device in client.state().owned_devices().map_err(string_error)? {
        let Some(worker) = device
            .relationships
            .assigned_replicant
            .as_ref()
            .map(|replicant| replicant.id.as_str())
            .filter(|worker| candidate_workers.contains(worker))
        else {
            continue;
        };
        let code = device.key.id.as_str();
        let Some(capabilities) = candidate_devices.get(code) else {
            continue;
        };
        if capabilities
            .iter()
            .any(|capability| capability == "survey_drone")
        {
            *drones.entry(worker.to_owned()).or_default() += 1;
        }
        if capabilities
            .iter()
            .any(|capability| capability == "survey_controller")
        {
            controllers.insert(worker.to_owned());
        }
        if capabilities
            .iter()
            .any(|capability| capability == "racing_vessel")
        {
            vessels
                .entry(worker.to_owned())
                .and_modify(|capacity| {
                    *capacity = (*capacity).max(stow_capacity.get(code).copied().unwrap_or(0))
                })
                .or_insert_with(|| stow_capacity.get(code).copied().unwrap_or(0));
        }
    }
    let mut capacities = candidate_workers
        .into_iter()
        .filter(|worker| controllers.contains(*worker))
        .filter_map(|worker| {
            let drone_count = drones.get(worker).copied().unwrap_or(0);
            let stow_limited = vessels.get(worker).copied().unwrap_or(0).saturating_sub(1);
            let capacity = drone_count.min(stow_limited);
            (capacity >= crate::survey::DRONE_COUNT).then_some(capacity)
        })
        .collect::<Vec<_>>();
    capacities.sort_unstable();
    capacities.dedup();
    if capacities.is_empty() {
        return Err("survey campaign has no complete regional fleet bundle".to_owned());
    }
    Ok(capacities)
}

fn survey_capacity_work_item_specs(
    workflow_id: WorkflowId,
    state: &SurveyExecutionState,
    region: &str,
    capacities: &[usize],
    existing: &[WorkItem],
) -> Result<Vec<WorkItemSpec>, String> {
    let existing = existing
        .iter()
        .map(|item| (item.spec.dedupe_key.as_str(), &item.spec))
        .collect::<BTreeMap<_, _>>();
    let mut capacity_index = 0;
    survey_work_item_specs(workflow_id, state, region)
        .map_err(string_error)?
        .into_iter()
        .map(|mut spec| {
            if let Some(stored) = existing.get(spec.dedupe_key.as_str()) {
                return Ok((*stored).clone());
            }
            if spec.payload_json["legacy_complete"] == Value::Bool(true) {
                return Ok(spec);
            }
            let capacity = capacities[capacity_index % capacities.len()];
            capacity_index += 1;
            spec.payload_json["fleet_capacity"] = serde_json::json!(capacity);
            let mut requirements: Vec<replicant_workflow::ResourceRequirement> =
                serde_json::from_value(spec.requirements_json).map_err(string_error)?;
            for requirement in &mut requirements {
                match requirement.key.as_str() {
                    "drones" => {
                        requirement.count = u32::try_from(capacity)
                            .map_err(|_| "survey drone capacity exceeds u32".to_owned())?;
                    }
                    "stow" => {
                        requirement.quantity = u64::try_from(capacity.saturating_add(1))
                            .map_err(|_| "survey stow capacity exceeds u64".to_owned())?;
                    }
                    _ => {}
                }
            }
            spec.requirements_json = serde_json::to_value(requirements).map_err(string_error)?;
            Ok(spec)
        })
        .collect()
}

fn relay_carrier_capacities(
    candidates: &[AllocationCandidate],
    planned_capacity: usize,
) -> Vec<(String, usize)> {
    let stow = candidates
        .iter()
        .filter_map(|candidate| match &candidate.resource {
            ResourceKey::Namespaced { namespace, key } if namespace == "stow" => {
                Some((key.clone(), candidate.available_quantity))
            }
            _ => None,
        })
        .collect::<BTreeMap<_, _>>();
    let mut capacities = candidates
        .iter()
        .filter_map(|candidate| match &candidate.resource {
            ResourceKey::Device(code)
                if candidate
                    .capabilities
                    .iter()
                    .any(|capability| capability == "racing_vessel") =>
            {
                usize::try_from(stow.get(code).copied().unwrap_or(0))
                    .ok()
                    .map(|capacity| (code.clone(), capacity.min(planned_capacity)))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    capacities.sort();
    capacities
}

fn relay_lane_candidates(
    candidates: &[AllocationCandidate],
    carrier: &str,
) -> Vec<AllocationCandidate> {
    candidates
        .iter()
        .filter(|candidate| match &candidate.resource {
            ResourceKey::Replicant(_) => true,
            ResourceKey::Device(code) => code == carrier,
            ResourceKey::Namespaced { namespace, key } => namespace == "stow" && key == carrier,
            _ => false,
        })
        .cloned()
        .collect()
}

fn string_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn placement_subject(code: &str) -> Option<WorkflowPlacementIntentSubject> {
    let code = code.trim();
    (!code.is_empty()).then(|| WorkflowPlacementIntentSubject::Device(code.to_ascii_uppercase()))
}

fn placement_intent(
    subject: WorkflowPlacementIntentSubject,
    relation: WorkflowPlacementIntentRelation,
    work_item_id: Option<replicant_workflow::WorkItemId>,
    expected_location: Option<String>,
) -> WorkflowPlacementIntent {
    WorkflowPlacementIntent {
        subject,
        relation,
        work_item_id,
        expected_location: (relation == WorkflowPlacementIntentRelation::Deployed)
            .then_some(expected_location)
            .flatten(),
    }
}

fn status_intent(
    status: WorkflowStatus,
    subject: WorkflowPlacementIntentSubject,
    durable_relation: Option<WorkflowPlacementIntentRelation>,
    work_item_id: Option<replicant_workflow::WorkItemId>,
    expected_location: Option<String>,
) -> Option<WorkflowPlacementIntent> {
    let relation = match status {
        WorkflowStatus::Queued
        | WorkflowStatus::Running
        | WorkflowStatus::Waiting
        | WorkflowStatus::Reconciling
        | WorkflowStatus::Paused => {
            durable_relation.or(Some(WorkflowPlacementIntentRelation::Awaited))
        }
        WorkflowStatus::Succeeded | WorkflowStatus::Failed | WorkflowStatus::Cancelled => {
            durable_relation
        }
    }?;
    Some(placement_intent(
        subject,
        relation,
        work_item_id,
        expected_location,
    ))
}

fn placement_status_is_live(status: WorkflowStatus) -> bool {
    matches!(
        status,
        WorkflowStatus::Queued
            | WorkflowStatus::Running
            | WorkflowStatus::Waiting
            | WorkflowStatus::Reconciling
            | WorkflowStatus::Paused
    )
}
fn complete_projection(intents: Vec<WorkflowPlacementIntent>) -> WorkflowPlacementIntentProjection {
    WorkflowPlacementIntentProjection {
        coverage: WorkflowPlacementIntentCoverage::Complete,
        intents,
        resolutions: Vec::new(),
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PlacementSurveyPhase {
    PreparingFleet,
    Ready,
    Traveling,
    SystemScanning,
    Surveying,
    Restowing,
    MaintenanceRecovering,
    MaintenanceReturning,
    MaintenanceRepairing,
    MaintenanceRestowing,
    Complete,
}

#[derive(Debug, Default, Deserialize)]
struct PlacementSurveyState {
    #[serde(default)]
    vessel: String,
    #[serde(default)]
    controller: Option<String>,
    #[serde(default)]
    drones: Vec<String>,
    #[serde(default)]
    fleet_prepared: bool,
    #[serde(default)]
    phase: Option<PlacementSurveyPhase>,
}

impl PlacementSurveyState {
    fn relation(&self) -> Option<WorkflowPlacementIntentRelation> {
        match self.phase {
            Some(
                PlacementSurveyPhase::Traveling
                | PlacementSurveyPhase::SystemScanning
                | PlacementSurveyPhase::Surveying
                | PlacementSurveyPhase::Restowing
                | PlacementSurveyPhase::MaintenanceRecovering
                | PlacementSurveyPhase::MaintenanceReturning
                | PlacementSurveyPhase::MaintenanceRepairing
                | PlacementSurveyPhase::MaintenanceRestowing,
            ) => Some(WorkflowPlacementIntentRelation::Transported),
            Some(PlacementSurveyPhase::Ready) if self.fleet_prepared => {
                Some(WorkflowPlacementIntentRelation::Staged)
            }
            Some(PlacementSurveyPhase::PreparingFleet) if self.fleet_prepared => {
                Some(WorkflowPlacementIntentRelation::Staged)
            }
            _ => None,
        }
    }
}

#[derive(Debug, Default, Deserialize)]
struct PlacementRelayStop {
    #[serde(default)]
    location: String,
    #[serde(default)]
    relay_code: Option<String>,
    #[serde(default)]
    completed: bool,
}

#[derive(Debug, Default, Deserialize)]
struct PlacementRelayPrint {
    #[serde(default)]
    mission_tag: String,
    #[serde(default)]
    site_tag: String,
    #[serde(default)]
    batch_tag: Option<String>,
    #[serde(default)]
    relay_code: Option<String>,
    #[serde(default)]
    submitted: bool,
}

#[derive(Debug, Default, Deserialize)]
struct PlacementRelayCarrier {
    #[serde(default)]
    code: String,
    #[serde(default)]
    dispatched: bool,
    #[serde(default)]
    returned_home: bool,
}

#[derive(Debug, Default, Deserialize)]
struct PlacementRelayRestock {
    #[serde(default)]
    carrier_code: String,
    #[serde(default)]
    confirmed_detached_relays: BTreeSet<String>,
    #[serde(default)]
    completed: bool,
}

#[derive(Debug, Default, Deserialize)]
struct PlacementRelaySupply {
    #[serde(default)]
    carriers: Vec<PlacementRelayCarrier>,
    #[serde(default)]
    restocks: Vec<PlacementRelayRestock>,
}

#[derive(Debug, Default, Deserialize)]
struct PlacementRelayState {
    #[serde(default)]
    legacy_mission_tags: Vec<String>,
    #[serde(default)]
    vessel_code: String,
    #[serde(default)]
    dsr_carrier_code: Option<String>,
    #[serde(default)]
    stops: Vec<PlacementRelayStop>,
    #[serde(default)]
    print_jobs: Vec<PlacementRelayPrint>,
    #[serde(default)]
    supply: Option<PlacementRelaySupply>,
}

#[derive(Debug, Default, Deserialize)]
struct PlacementMiningAssets {
    #[serde(default)]
    mining_controller: Option<String>,
    #[serde(default)]
    mining_drones: Vec<String>,
    #[serde(default)]
    survey_controller: Option<String>,
    #[serde(default)]
    survey_drones: Vec<String>,
    #[serde(default)]
    maintenance_drone: Option<String>,
    #[serde(default)]
    system_ward: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct PlacementMiningSite {
    #[serde(default)]
    system: String,
    #[serde(default)]
    phase: Option<String>,
    #[serde(default)]
    tag: String,
    #[serde(default)]
    assets: PlacementMiningAssets,
    #[serde(default)]
    carrier: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct PlacementMiningRoute {
    #[serde(default)]
    phase: Option<String>,
    #[serde(default)]
    controller: Option<String>,
    #[serde(default)]
    freighter: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct PlacementMiningBatch {
    #[serde(default)]
    produced_codes: Vec<String>,
}

#[derive(Debug, Default, Deserialize)]
struct PlacementMiningMission {
    #[serde(default)]
    mission_tag: String,
    #[serde(default)]
    legacy_mission_tags: Vec<String>,
    #[serde(default)]
    sites: Vec<PlacementMiningSite>,
    #[serde(default)]
    routes: Vec<PlacementMiningRoute>,
    #[serde(default)]
    print_batches: Vec<PlacementMiningBatch>,
}

trait PlacementTypedItem: serde::de::DeserializeOwned {
    fn touch(&self);
}

#[derive(Debug, Deserialize)]
struct PlacementSurveyItem {
    star: String,
    entry_point: Option<String>,
    survey_required: bool,
    legacy_complete: bool,
}

impl PlacementTypedItem for PlacementSurveyItem {
    fn touch(&self) {
        let _ = (
            &self.star,
            &self.entry_point,
            self.survey_required,
            self.legacy_complete,
        );
    }
}

#[derive(Debug, Deserialize)]
struct PlacementRelayItem {
    system: String,
}

impl PlacementTypedItem for PlacementRelayItem {
    fn touch(&self) {
        let _ = &self.system;
    }
}

#[derive(Debug, Deserialize)]
struct PlacementMiningItem {
    #[serde(rename = "type")]
    item_type: String,
    index: usize,
    system: String,
    belt: String,
    legacy_complete: bool,
}

impl PlacementTypedItem for PlacementMiningItem {
    fn touch(&self) {
        let _ = (
            &self.item_type,
            self.index,
            &self.system,
            &self.belt,
            self.legacy_complete,
        );
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PlacementEventStage {
    Stage,
    Delivery,
    Resolve,
    Return,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlacementEventItem {
    event: String,
    criterion: String,
    selected: bool,
    stage: PlacementEventStage,
    mission_path: PathBuf,
    mission_json: String,
    legacy_complete: bool,
}

impl PlacementTypedItem for PlacementEventItem {
    fn touch(&self) {
        let _ = (
            &self.event,
            &self.criterion,
            self.selected,
            self.stage,
            &self.mission_path,
            &self.mission_json,
            self.legacy_complete,
        );
    }
}

fn decode_typed_work_items<T: PlacementTypedItem>(items: &[WorkItem]) -> Result<(), String> {
    for item in items {
        if item.state.checkpoint_json.is_some() {
            return Err("work-item checkpoint has no supported typed placement schema".to_owned());
        }
        if !item.spec.payload_json.is_null() {
            let payload = serde_json::from_value::<T>(item.spec.payload_json.clone())
                .map_err(string_error)?;
            payload.touch();
        }
    }
    Ok(())
}

struct RequirementWorkflowFactory(WorkflowKind);

impl RequirementWorkflowFactory {
    fn new() -> Self {
        Self(requirement_workflow_kind())
    }
}

impl WorkflowFactory for RequirementWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.0
    }

    fn current_schema_version(&self) -> u32 {
        SCHEMA_VERSION
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(RequirementWorkflow))
    }
    fn placement_intents(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
        work_items: &[WorkItem],
    ) -> Result<WorkflowPlacementIntentProjection, String> {
        if instance.schema_version != self.current_schema_version() {
            return Ok(WorkflowPlacementIntentProjection::unknown());
        }
        let config: RequirementWorkflowConfig = instance.config().map_err(string_error)?;
        let _: RequirementWorkflowCheckpoint = instance.checkpoint().map_err(string_error)?;
        if !work_items.is_empty() {
            return Ok(WorkflowPlacementIntentProjection::unknown());
        }
        let mut intents = Vec::new();
        for claim in config.requirement.fulfillment.claims {
            let ResourceKey::Device(code) = claim else {
                continue;
            };
            let Some(subject) = placement_subject(&code) else {
                continue;
            };
            if let Some(intent) = status_intent(instance.status, subject, None, None, None) {
                intents.push(intent);
            }
        }
        Ok(complete_projection(intents))
    }
}

struct RequirementWorkflow;

impl WorkflowExecutor for RequirementWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let config: RequirementWorkflowConfig =
                context.config().map_err(|error| error.to_string())?;
            let mut checkpoint: RequirementWorkflowCheckpoint =
                context.checkpoint().map_err(|error| error.to_string())?;
            let client = context
                .managed_client()
                .cloned()
                .ok_or_else(|| "requirement workflow requires a managed client".to_owned())?;
            claim(
                context,
                ResourceKey::Namespaced {
                    namespace: "requirement".to_owned(),
                    key: config.requirement.id.clone(),
                },
            )?;

            loop {
                let children = checkpoint
                    .children
                    .iter()
                    .map(|id| context.repository().read(*id))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| error.to_string())?;
                let active = children
                    .iter()
                    .filter_map(|child| child.as_ref())
                    .map(|child| ActiveFulfillment {
                        requirement_id: config.requirement.id.clone(),
                        quantity: child
                            .config::<RequirementActionConfig>()
                            .map(|config| config.quantity)
                            .unwrap_or_else(|_| {
                                checkpoint
                                    .plan
                                    .as_ref()
                                    .and_then(|plan| plan.step.as_ref())
                                    .map_or(0, |step| step.quantity)
                            }),
                        status: child.status,
                    })
                    .collect::<Vec<_>>();
                let facts = managed_facts(&client.with_priority(RequestPriority::Background))
                    .await
                    .map_err(|error| error.to_string())?;
                let plan = evaluate_requirement(&config.requirement, &facts, &active);
                checkpoint.plan = Some(plan.clone());
                context
                    .advance_to(
                        if plan.missing == 0 && plan.in_progress == 0 {
                            "satisfied"
                        } else if plan.in_progress != 0 {
                            "awaiting_children"
                        } else {
                            "planning"
                        },
                        &checkpoint,
                    )
                    .map_err(|error| error.to_string())?;

                if plan.missing == 0 && plan.in_progress == 0 {
                    emit(context, &WorkflowActivityEvent::Completion)?;
                    return context
                        .mark_succeeded(Some(plan))
                        .map_err(|error| error.to_string());
                }
                if plan.in_progress == 0 {
                    let step = plan.step.expect("missing requirement has a step");
                    let key = fulfillment_key(&config.requirement.id, &step.operation);
                    if let Some(existing) = equivalent_active(context, &key)? {
                        checkpoint.children.push(existing);
                    } else {
                        let child = match step.operation.operation_class {
                            FulfillmentOperationClass::Action => context
                                .repository()
                                .create(NewWorkflow {
                                    kind: requirement_action_kind(),
                                    schema_version: SCHEMA_VERSION,
                                    config: RequirementActionConfig {
                                        requirement_id: config.requirement.id.clone(),
                                        quantity: step.quantity,
                                        operation: step.operation.clone(),
                                    },
                                    checkpoint: Value::Null,
                                    current_step: Some("queued".to_owned()),
                                    parent_id: Some(context.id()),
                                })
                                .map_err(|error| error.to_string())?,
                            FulfillmentOperationClass::Workflow => OperationCatalogue::new()
                                .map_err(|error| error.to_string())?
                                .create_workflow_with_parent(
                                    context.repository(),
                                    &step.operation.kind,
                                    step.operation.parameters.clone(),
                                    Some(context.id()),
                                )
                                .map_err(|error| error.to_string())?,
                        };
                        context
                            .repository()
                            .acquire_claim(
                                child.id,
                                ResourceKey::Namespaced {
                                    namespace: "fulfillment".to_owned(),
                                    key,
                                },
                            )
                            .map_err(|error| error.to_string())?;
                        for resource in &step.operation.claims {
                            context
                                .repository()
                                .acquire_claim(child.id, resource.clone())
                                .map_err(|error| error.to_string())?;
                        }
                        checkpoint.children.push(child.id);
                    }
                    context
                        .persist_checkpoint(&checkpoint)
                        .map_err(|error| error.to_string())?;
                }

                match context
                    .control_request()
                    .map_err(|error| error.to_string())?
                {
                    replicant_workflow::ControlRequest::Continue => {}
                    replicant_workflow::ControlRequest::Pause
                    | replicant_workflow::ControlRequest::Cancel => return Ok(()),
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
    }
}

struct RequirementActionFactory(WorkflowKind);

impl RequirementActionFactory {
    fn new() -> Self {
        Self(requirement_action_kind())
    }
}

impl WorkflowFactory for RequirementActionFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.0
    }

    fn current_schema_version(&self) -> u32 {
        SCHEMA_VERSION
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(RequirementActionWorkflow))
    }
    fn placement_intents(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
        work_items: &[WorkItem],
    ) -> Result<WorkflowPlacementIntentProjection, String> {
        if instance.schema_version != self.current_schema_version() {
            return Ok(WorkflowPlacementIntentProjection::unknown());
        }
        let config: RequirementActionConfig = instance.config().map_err(string_error)?;
        let checkpoint: Value = instance.checkpoint().map_err(string_error)?;
        if !checkpoint.is_null() || !work_items.is_empty() {
            return Ok(WorkflowPlacementIntentProjection::unknown());
        }
        let mut intents = Vec::new();
        for claim in config.operation.claims {
            let ResourceKey::Device(code) = claim else {
                continue;
            };
            let Some(subject) = placement_subject(&code) else {
                continue;
            };
            if let Some(intent) = status_intent(instance.status, subject, None, None, None) {
                intents.push(intent);
            }
        }
        Ok(complete_projection(intents))
    }
}

struct RequirementActionWorkflow;

impl WorkflowExecutor for RequirementActionWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let config: RequirementActionConfig =
                context.config().map_err(|error| error.to_string())?;
            let client = context
                .managed_client()
                .cloned()
                .ok_or_else(|| "fulfillment action requires a managed client".to_owned())?;
            context
                .advance_to("executing", &Value::Null)
                .map_err(|error| error.to_string())?;
            let result = OperationCatalogue::new()
                .map_err(|error| error.to_string())?
                .run_action(&client, &config.operation.kind, config.operation.parameters)
                .await
                .map_err(|error| error.to_string())?;
            context
                .mark_succeeded(Some(result))
                .map_err(|error| error.to_string())
        })
    }
}

fn fulfillment_key(requirement_id: &str, operation: &FulfillmentOperation) -> String {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    requirement_id.hash(&mut hash);
    operation.kind.hash(&mut hash);

    serde_json::to_string(&operation.parameters)
        .expect("fulfillment parameters serialize")
        .hash(&mut hash);
    format!("{requirement_id}:{:016x}", hash.finish())
}
fn workflow_now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn equivalent_active(context: &WorkflowContext, key: &str) -> Result<Option<WorkflowId>, String> {
    let resource = ResourceKey::Namespaced {
        namespace: "fulfillment".to_owned(),
        key: key.to_owned(),
    };
    for workflow in context
        .repository()
        .list_active()
        .map_err(|error| error.to_string())?
    {
        if context
            .repository()
            .claims(workflow.id)
            .map_err(|error| error.to_string())?
            .iter()
            .any(|claim| claim.resource == resource)
        {
            return Ok(Some(workflow.id));
        }
    }
    Ok(None)
}

/// Creates a queued durable survey workflow payload.
pub fn new_survey_workflow(
    config: SurveyWorkflowConfig,
) -> NewWorkflow<SurveyWorkflowConfig, SurveyWorkflowCheckpoint> {
    NewWorkflow {
        kind: survey_workflow_kind(),
        schema_version: SCHEMA_VERSION,
        config,
        checkpoint: SurveyWorkflowCheckpoint::default(),
        current_step: Some("queued".to_owned()),
        parent_id: None,
    }
}

/// Creates a queued durable relay workflow payload.
pub fn new_relay_workflow(
    config: RelayWorkflowConfig,
) -> NewWorkflow<RelayWorkflowConfig, RelayWorkflowCheckpoint> {
    NewWorkflow {
        kind: relay_workflow_kind(),
        schema_version: 2,
        config,
        checkpoint: RelayWorkflowCheckpoint::default(),
        current_step: Some("queued".to_owned()),
        parent_id: None,
    }
}

/// Creates a queued durable mining-expansion workflow.
pub fn new_mining_workflow(
    config: MiningWorkflowConfig,
) -> NewWorkflow<MiningWorkflowConfig, MiningWorkflowCheckpoint> {
    NewWorkflow {
        kind: mining_workflow_kind(),
        schema_version: SCHEMA_VERSION,
        config,
        checkpoint: MiningWorkflowCheckpoint::default(),
        current_step: Some("queued".to_owned()),
        parent_id: None,
    }
}

/// Creates a queued durable event-execution workflow.
pub fn new_event_workflow(
    config: EventWorkflowConfig,
) -> NewWorkflow<EventWorkflowConfig, EventWorkflowCheckpoint> {
    NewWorkflow {
        kind: event_workflow_kind(),
        schema_version: SCHEMA_VERSION,
        config,
        checkpoint: EventWorkflowCheckpoint::default(),
        current_step: Some("queued".to_owned()),
        parent_id: None,
    }
}

/// Creates a queued durable desired-state fulfillment workflow.
pub fn new_requirement_workflow(
    config: RequirementWorkflowConfig,
) -> NewWorkflow<RequirementWorkflowConfig, RequirementWorkflowCheckpoint> {
    NewWorkflow {
        kind: requirement_workflow_kind(),
        schema_version: SCHEMA_VERSION,
        config,
        checkpoint: RequirementWorkflowCheckpoint::default(),
        current_step: Some("queued".to_owned()),
        parent_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::requirements::RequirementScope;
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use replicant_client::{SecretString, StartupPolicy, managed::SyncDomain, raw::Url};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    #[test]
    fn mining_assignment_ids_change_between_claim_revisions() {
        let item_id = replicant_workflow::WorkItemId::new();
        assert_ne!(
            mining_assignment_id(item_id, 7, "R-1"),
            mining_assignment_id(item_id, 8, "R-1")
        );
    }

    #[test]
    fn physical_device_region_overrides_assigned_worker_fallback() {
        let workers = ["ALPHA-WORKER".to_owned()].into_iter().collect();
        let regional_devices = ["BETA-AF".to_owned()].into_iter().collect();
        let candidate = AllocationCandidate {
            resource: ResourceKey::Autofactory("BETA-AF".into()),
            kind: "autofactory".into(),
            capabilities: Vec::new(),
            location: Some(replicant_workflow::AllocationLocation {
                region: Some("beta".into()),
                system: Some("THYFFAWFF".into()),
                designation: Some("THYFFAWFF-BELT-1".into()),
                ..replicant_workflow::AllocationLocation::default()
            }),
            available_quantity: 1,
            observed_revision: 1,
            observed_at_ms: 1,
        };

        assert!(allocation_candidate_belongs_to_region(
            &candidate,
            "beta",
            &workers,
            &regional_devices,
        ));
        assert!(!allocation_candidate_belongs_to_region(
            &candidate,
            "alpha",
            &workers,
            &regional_devices,
        ));
    }

    #[test]
    fn allocation_location_resolves_against_the_longest_known_system_prefix() {
        let regions = BTreeMap::from([
            ("KHAHKUHKAK".to_owned(), "delta".to_owned()),
            ("KHAHKUHKAK-OUTER".to_owned(), "delta".to_owned()),
        ]);
        assert_eq!(
            allocation_system_for_designation("KHAHKUHKAK-OUTER-2-L4", &regions).as_deref(),
            Some("KHAHKUHKAK-OUTER")
        );
    }

    async fn mining_pool_client(server: &MockServer) -> replicant_client::Client {
        replicant_client::Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .authentication_token(SecretString::from("test-token"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("start client")
    }

    async fn seed_mining_pool_worker(
        server: &MockServer,
        client: &replicant_client::Client,
        worker: &str,
        hosted: &str,
        devices: &[(&str, &str, Option<i64>)],
    ) {
        Mock::given(method("GET"))
            .and(path(format!("/v1/replicants/{worker}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "replicant_code": worker,
                "hosted_device_code": hosted,
                "location": "ROOT-1-L4",
                "status": "active"
            })))
            .expect(1)
            .mount(server)
            .await;
        client
            .replicants()
            .get_owned(worker)
            .await
            .expect("seed worker");
        for (code, device_type, stow_capacity) in devices {
            let mut body = serde_json::json!({
                "device_code": code,
                "device_type": device_type,
                "replicant_code": worker,
                "location": "ROOT-1-L4",
                "status": "idle"
            });
            if let Some(capacity) = stow_capacity {
                body["stow_capacity"] = (*capacity).into();
                body["stow_used"] = 0.into();
            }
            Mock::given(method("GET"))
                .and(path(format!("/v1/devices/{code}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .expect(1)
                .mount(server)
                .await;
            client.devices().get(code).await.expect("seed device");
        }
    }

    struct FixtureMiningItemExecutor {
        calls: AtomicUsize,
        active: AtomicUsize,
        peak: AtomicUsize,
        first_wave: tokio::sync::Barrier,
        missing_injected: AtomicBool,
        stow_quantities: Mutex<Vec<u64>>,
        stage_allocations: Mutex<Vec<(u64, bool)>>,
    }

    impl FixtureMiningItemExecutor {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                peak: AtomicUsize::new(0),
                first_wave: tokio::sync::Barrier::new(2),
                missing_injected: AtomicBool::new(false),
                stow_quantities: Mutex::new(Vec::new()),
                stage_allocations: Mutex::new(Vec::new()),
            }
        }
    }

    impl MiningItemExecutor for FixtureMiningItemExecutor {
        fn execute<'a>(
            &'a self,
            _client: &'a replicant_client::Client,
            mission: &'a MiningMission,
            item_type: &'a str,
            index: usize,
            allocations: &'a AllocationSet,
            _wait_timeout: Duration,
        ) -> MiningItemFuture<'a> {
            let mut lane = mission.clone();
            let item_type = item_type.to_owned();
            let stow = allocations
                .by_requirement
                .get("stow")
                .into_iter()
                .flatten()
                .map(|allocation| allocation.quantity)
                .sum();
            let carrier_allocation = allocations
                .by_requirement
                .get("carrier")
                .and_then(|allocations| allocations.first())
                .map(|allocation| allocation.id);
            let material_quantity = allocations
                .by_requirement
                .get("material:structural")
                .into_iter()
                .flatten()
                .map(|allocation| allocation.quantity)
                .sum();
            let has_autofactory = allocations
                .by_requirement
                .get("autofactory")
                .is_some_and(|allocations| !allocations.is_empty());
            Box::pin(async move {
                let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
                self.stow_quantities
                    .lock()
                    .expect("stow quantities")
                    .push(stow);
                let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak.fetch_max(active, Ordering::SeqCst);
                if call <= 2 {
                    self.first_wave.wait().await;
                }
                if item_type == "site" && !self.missing_injected.swap(true, Ordering::SeqCst) {
                    self.active.fetch_sub(1, Ordering::SeqCst);
                    let allocation_id =
                        carrier_allocation.expect("site fixture carrier allocation");
                    return Err(Box::new(MiningMissingAllocationError {
                        requirement: "carrier".to_owned(),
                        allocation_id,
                    }) as crate::mining::AnyError);
                }
                match item_type.as_str() {
                    "site" => {
                        let mut site = lane.sites[index].clone();
                        site.phase = crate::mining::SitePhase::Operational;
                        lane.sites = vec![site];
                        lane.routes.clear();
                    }
                    "route" => {
                        let mut route = lane.routes[index].clone();
                        route.phase = crate::mining::RoutePhase::Active;
                        lane.routes = vec![route];
                        lane.sites.clear();
                    }
                    "stage" => {
                        self.stage_allocations
                            .lock()
                            .expect("stage allocations")
                            .push((material_quantity, has_autofactory));
                        for batch in &mut lane.print_batches {
                            batch.produced_codes.push("PRINTED-1".into());
                        }
                        lane.sites.clear();
                        lane.routes.clear();
                    }
                    other => panic!("unexpected fixture item type {other}"),
                }
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(lane)
            })
        }
    }

    fn mining_pool_mission() -> MiningMission {
        MiningMission {
            version: 1,
            mission_id: "mining-pool".into(),
            mission_tag: "mine-m:root".into(),
            legacy_mission_tags: Vec::new(),
            phase: crate::mining::MissionPhase::Planned,
            selected_replicant: "LEGACY".into(),
            hub_location: "ROOT-1-L4".into(),
            sites: vec![
                crate::mining::SiteMission {
                    system: "DONE".into(),
                    belt: "DONE-BELT-1".into(),
                    density: "high".into(),
                    tag: "mine-s:done".into(),
                    phase: crate::mining::SitePhase::Operational,
                    assets: crate::mining::SiteAssets::default(),
                    missing: BTreeMap::new(),
                    carrier: None,
                },
                crate::mining::SiteMission {
                    system: "PENDING".into(),
                    belt: "PENDING-BELT-1".into(),
                    density: "high".into(),
                    tag: "mine-s:pending".into(),
                    phase: crate::mining::SitePhase::Planned,
                    assets: crate::mining::SiteAssets::default(),
                    missing: BTreeMap::from([("ami_mining_controller".into(), 1)]),
                    carrier: None,
                },
            ],
            routes: vec![
                crate::mining::RouteMission {
                    system: "PENDING".into(),
                    belt: "PENDING-BELT-1".into(),
                    tag: "mine-s:pending".into(),
                    phase: crate::mining::RoutePhase::Planned,
                    controller: None,
                    freighter: None,
                },
                crate::mining::RouteMission {
                    system: "PENDING-2".into(),
                    belt: "PENDING-2-BELT-1".into(),
                    tag: "mine-s:pending-2".into(),
                    phase: crate::mining::RoutePhase::Planned,
                    controller: None,
                    freighter: None,
                },
            ],
            print_batches: Vec::new(),
            site_print_requirements: BTreeMap::new(),
            route_print_requirements: BTreeMap::new(),
            total_material_cost: BTreeMap::new(),
            warnings: Vec::new(),
        }
    }

    fn mining_pool_material_mission() -> MiningMission {
        let mut mission = mining_pool_mission();
        mission.sites[1].phase = crate::mining::SitePhase::Operational;
        for route in &mut mission.routes {
            route.phase = crate::mining::RoutePhase::Active;
        }
        mission
            .print_batches
            .push(crate::mining::ExecutionPrintBatch {
                purpose: crate::mining::PrintPurpose::Site,
                factory_code: "C-AF".into(),
                device_type: "mining_drone".into(),
                quantity: 1,
                projected_finish_seconds: 60.0,
                batch_tag: "mining-pool-stage".into(),
                submission_started: false,
                submitted: false,
                operation_id: None,
                produced_codes: Vec::new(),
            });
        mission.total_material_cost.insert("structural".into(), 10);
        mission
    }

    #[test]
    fn manufacturing_work_item_spec_stays_immutable_when_checkpoint_completes() {
        let repository =
            replicant_workflow::WorkflowRepository::open_in_memory().expect("repository");
        let workflow = repository
            .create(NewWorkflow {
                kind: mining_workflow_kind(),
                schema_version: SCHEMA_VERSION,
                config: Value::Null,
                checkpoint: Value::Null,
                current_step: Some("executing".into()),
                parent_id: None,
            })
            .expect("workflow");
        let mut mission = mining_pool_material_mission();
        let initial =
            mining_work_item_specs(workflow.id, &mission, "Alpha").expect("initial specs");
        repository
            .reconcile_work_items(workflow.id, &initial, 1)
            .expect("initial reconciliation");

        mission.print_batches[0]
            .produced_codes
            .push("PRINTED-1".into());
        let resumed =
            mining_work_item_specs(workflow.id, &mission, "Alpha").expect("resumed specs");
        assert_eq!(resumed, initial);
        let stage = resumed
            .iter()
            .find(|spec| spec.kind.as_str() == "mining.stage")
            .expect("manufacturing stage");
        assert_eq!(stage.payload_json["legacy_complete"], Value::Bool(false));
        repository
            .reconcile_work_items(workflow.id, &resumed, 2)
            .expect("restart reconciliation");
    }

    #[tokio::test]
    async fn mining_pool_registered_schema_one_workflow_uses_allocated_concurrent_items() {
        let server = MockServer::start().await;
        let client = mining_pool_client(&server).await;
        seed_mining_pool_worker(
            &server,
            &client,
            "REP-A",
            "A-CARRIER",
            &[
                ("A-CARRIER", "surge_carrier", Some(12)),
                ("A-MC", "ami_mining_controller", None),
                ("A-MD1", "mining_drone", None),
                ("A-MD2", "mining_drone", None),
                ("A-MD3", "mining_drone", None),
                ("A-MD4", "mining_drone", None),
                ("A-SC", "ami_survey_controller", None),
                ("A-SD1", "survey_drone", None),
                ("A-SD2", "survey_drone", None),
                ("A-MAINT", "maintenance_drone", None),
            ],
        )
        .await;
        seed_mining_pool_worker(
            &server,
            &client,
            "REP-B",
            "B-FREIGHTER",
            &[
                ("B-FREIGHTER", "cargo_freighter", Some(3)),
                ("B-TC", "ami_transport_controller", None),
            ],
        )
        .await;
        seed_mining_pool_worker(
            &server,
            &client,
            "REP-C",
            "C-FREIGHTER",
            &[
                ("C-FREIGHTER", "cargo_freighter", Some(4)),
                ("C-TC", "ami_transport_controller", None),
                ("C-AF", "autofactory", None),
                ("C-CARRIER", "surge_carrier", Some(12)),
            ],
        )
        .await;
        let repository =
            Arc::new(replicant_workflow::WorkflowRepository::open_in_memory().expect("repository"));
        for worker in ["LEGACY", "REP-A", "REP-B", "REP-C"] {
            repository
                .put_document(
                    "director.replicant",
                    worker,
                    &serde_json::json!({"region": "Alpha"}),
                )
                .expect("Director region");
        }
        let mission_path = std::env::temp_dir().join(format!(
            "mining-pool-{}-{}.json",
            std::process::id(),
            workflow_now_millis()
        ));
        std::fs::write(
            &mission_path,
            serde_json::to_vec_pretty(&mining_pool_mission()).expect("mission JSON"),
        )
        .expect("mission file");
        let workflow = repository
            .create(NewWorkflow {
                kind: mining_workflow_kind(),
                schema_version: 1,
                config: LegacyMiningWorkflowConfig {
                    systems: vec!["DONE".into(), "PENDING".into()],
                    replicant: "LEGACY".into(),
                    hub: "ROOT-1-L4".into(),
                    mission_file: mission_path.clone(),
                    wait_timeout_seconds: 1,
                    max_concurrency: 2,
                },
                checkpoint: serde_json::json!({"started": true}),
                current_step: Some("executing".into()),
                parent_id: None,
            })
            .expect("workflow");
        let executor = Arc::new(FixtureMiningItemExecutor::new());
        let mut registry = WorkflowRegistry::new();
        registry
            .register(Arc::new(MiningWorkflowFactory::with_item_executor(
                executor.clone(),
            )))
            .expect("register mining workflow");
        registry
            .register(Arc::new(
                crate::automation::MiningCampaignWorkflowFactory::with_item_executor(
                    executor.clone(),
                ),
            ))
            .expect("register mining campaign workflow");
        let supervisor = replicant_workflow::WorkflowSupervisor::with_managed_client(
            repository.clone(),
            Arc::new(registry),
            client.clone(),
        );
        for _ in 0..100 {
            supervisor.tick().await.expect("supervisor tick");
            if repository
                .read(workflow.id)
                .expect("workflow")
                .is_some_and(|workflow| workflow.status.is_terminal())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let workflow = repository
            .read(workflow.id)
            .expect("workflow")
            .expect("workflow exists");
        assert_eq!(workflow.schema_version, 2);
        let items = repository.list_work_items(workflow.id).expect("items");
        assert_eq!(
            workflow.status,
            WorkflowStatus::Succeeded,
            "error={:?}, items={items:#?}",
            workflow.last_error
        );
        let replaced_site = items
            .iter()
            .find(|item| item.spec.dedupe_key == "mining.site:PENDING-BELT-1")
            .expect("replaced site item");
        assert_eq!(replaced_site.state.status, WorkItemStatus::Succeeded);
        assert!(
            replaced_site.state.checkpoint_json.is_some(),
            "replacement must preserve and advance the site checkpoint"
        );
        assert_eq!(
            repository
                .list_work_item_attempts(replaced_site.id)
                .expect("site attempts")
                .len(),
            1,
            "replacement resumes the same durable attempt"
        );
        assert_eq!(items.len(), 3);
        assert!(
            items
                .iter()
                .all(|item| item.spec.dedupe_key != "mining.site:DONE-BELT-1"),
            "operational sites must not reopen durable work"
        );
        assert_eq!(executor.calls.load(Ordering::SeqCst), 4);
        assert_eq!(executor.peak.load(Ordering::SeqCst), 2);
        let mut stow = executor
            .stow_quantities
            .lock()
            .expect("stow quantities")
            .clone();
        stow.sort_unstable();
        assert_eq!(stow, [1, 1, 2, 2]);
        let progress = workflow
            .result::<Value>()
            .expect("mining result")
            .expect("mining result value");
        assert_eq!(progress["progress"]["sites"], serde_json::json!([2, 2]));
        assert_eq!(progress["progress"]["routes"], serde_json::json!([2, 2]));
        assert_eq!(progress["progress"]["printing"], serde_json::json!([0, 0]));

        let campaign = repository
            .create(NewWorkflow {
                kind: crate::automation::mining_campaign_workflow_kind(),
                schema_version: 1,
                config: serde_json::json!({
                    "systems": ["DONE", "PENDING"],
                    "replicant": "LEGACY",
                    "hub": "ROOT-1-L4",
                    "max_concurrency": 2
                }),
                checkpoint: serde_json::json!({
                    "replicant": "LEGACY",
                    "hub": "ROOT-1-L4",
                    "plan_json": serde_json::to_string(&mining_pool_mission())
                        .expect("campaign mission"),
                    "started": true
                }),
                current_step: Some("expanding".into()),
                parent_id: None,
            })
            .expect("legacy mining campaign");
        for _ in 0..100 {
            supervisor.tick().await.expect("campaign supervisor tick");
            if repository
                .read(campaign.id)
                .expect("campaign")
                .is_some_and(|workflow| workflow.status.is_terminal())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let campaign = repository
            .read(campaign.id)
            .expect("campaign")
            .expect("campaign exists");
        assert_eq!(campaign.schema_version, 3);
        assert_eq!(
            campaign.status,
            WorkflowStatus::Succeeded,
            "{:?}",
            campaign.last_error
        );
        assert_eq!(executor.calls.load(Ordering::SeqCst), 7);

        let shortage = repository
            .create(NewWorkflow {
                kind: mining_workflow_kind(),
                schema_version: 2,
                config: MiningWorkflowConfig {
                    systems: vec!["DONE".into(), "PENDING".into()],
                    region: "Alpha".into(),
                    hub: "ROOT-1-L4".into(),
                    transport_routes: Vec::new(),
                    mission_file: std::env::temp_dir().join("mining-shortage-unused.json"),
                    wait_timeout_seconds: 1,
                    max_concurrency: 2,
                },
                checkpoint: MiningWorkflowCheckpoint {
                    mission: Some(mining_pool_material_mission()),
                    migration_worker: None,
                    started: true,
                },
                current_step: Some("executing".into()),
                parent_id: None,
            })
            .expect("material shortage workflow");
        for _ in 0..100 {
            supervisor.tick().await.expect("shortage supervisor tick");
            if repository
                .read(shortage.id)
                .expect("shortage workflow")
                .is_some_and(|workflow| workflow.status == WorkflowStatus::Waiting)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let shortage = repository
            .read(shortage.id)
            .expect("shortage workflow")
            .expect("shortage workflow exists");
        assert_eq!(shortage.status, WorkflowStatus::Waiting);
        let shortage_items = repository
            .list_work_items(shortage.id)
            .expect("shortage items");
        let stage = shortage_items
            .iter()
            .find(|item| item.spec.kind.as_str() == "mining.stage")
            .expect("stage item");
        assert_eq!(stage.state.status, WorkItemStatus::Waiting);
        assert!(
            stage
                .state
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("material:structural")),
            "{:?}",
            stage.state.last_error
        );
        let retained_checkpoint =
            serde_json::to_value(mining_pool_material_mission()).expect("stage checkpoint");
        repository
            .transition_work_item(
                stage.id,
                stage.state.revision,
                WorkItemTransition::Waiting {
                    checkpoint_json: Some(retained_checkpoint.clone()),
                    reason: "inventory replenishment pending".into(),
                    retry_at_ms: Some(workflow_now_millis()),
                },
                workflow_now_millis(),
            )
            .expect("make waiting stage retry due");
        Mock::given(method("GET"))
            .and(path("/v1/inventory"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "locations": [{
                    "location": "ROOT-1-L4",
                    "items": [{"resource_type": "structural", "quantity": 10}]
                }],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;
        client
            .sync()
            .domain(SyncDomain::Inventory)
            .await
            .expect("refresh replenished material");
        drop(supervisor);
        tokio::time::sleep(Duration::from_secs(5)).await;
        let mut restarted_registry = WorkflowRegistry::new();
        restarted_registry
            .register(Arc::new(MiningWorkflowFactory::with_item_executor(
                executor.clone(),
            )))
            .expect("register restarted mining workflow");
        restarted_registry
            .register(Arc::new(
                crate::automation::MiningCampaignWorkflowFactory::with_item_executor(
                    executor.clone(),
                ),
            ))
            .expect("register restarted mining campaign workflow");
        let supervisor = replicant_workflow::WorkflowSupervisor::with_managed_client(
            repository.clone(),
            Arc::new(restarted_registry),
            client.clone(),
        );
        for _ in 0..100 {
            supervisor
                .tick()
                .await
                .expect("replenished supervisor tick");
            if repository
                .read_work_item(stage.id)
                .expect("read replenished stage")
                .is_some_and(|item| item.state.status == WorkItemStatus::Succeeded)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let replenished_stage = repository
            .read_work_item(stage.id)
            .expect("read replenished stage")
            .expect("replenished stage exists");
        assert_eq!(replenished_stage.id, stage.id);
        assert_eq!(
            replenished_stage.state.status,
            WorkItemStatus::Succeeded,
            "error={:?}, workflow={:?}",
            replenished_stage.state.last_error,
            repository.read(shortage.id).expect("shortage workflow")
        );
        assert!(replenished_stage.state.checkpoint_json.is_some());
        assert_ne!(
            replenished_stage.state.checkpoint_json,
            Some(retained_checkpoint),
            "successful commit advances the retained waiting checkpoint"
        );
        assert_eq!(
            repository
                .list_work_item_attempts(stage.id)
                .expect("stage attempts")
                .len(),
            1
        );
        assert_eq!(
            *executor
                .stage_allocations
                .lock()
                .expect("stage allocations"),
            [(10, true)]
        );
        let _ = std::fs::remove_file(mission_path);
        client.close().await.expect("close client");
    }
    #[test]
    fn stable_kinds_and_structured_activity_round_trip() {
        assert_eq!(survey_workflow_kind().as_str(), "survey.route");
        assert_eq!(relay_workflow_kind().as_str(), "relay.expansion");
        assert_eq!(
            requirement_workflow_kind().as_str(),
            "requirement.fulfillment"
        );
        let event = WorkflowActivityEvent::WaitReason {
            step: "surveying".to_owned(),
            reason: "managed state".to_owned(),
        };
        let json = serde_json::to_string(&event).expect("serialize activity");
        assert_eq!(
            serde_json::from_str::<WorkflowActivityEvent>(&json).expect("deserialize activity"),
            event
        );
    }

    #[test]
    fn legacy_event_checkpoint_accepts_pre_connectivity_state() {
        let checkpoint: EventWorkflowCheckpoint = serde_json::from_value(serde_json::json!({
            "started": false
        }))
        .expect("restore legacy event checkpoint");
        assert!(checkpoint.connectivity_workflows.is_empty());
        assert!(!checkpoint.replan_after_connectivity);
    }

    #[test]
    fn completed_steps_survive_representative_restarts() {
        let mut checkpoint = SurveyWorkflowCheckpoint::default();
        for step in ["preparing_fleet", "traveling", "surveying", "restowing"] {
            checkpoint.completed_steps.insert(step.to_owned());
            let stored = serde_json::to_string(&checkpoint).expect("serialize checkpoint");
            checkpoint = serde_json::from_str(&stored).expect("restore checkpoint");
        }
        assert_eq!(checkpoint.completed_steps.len(), 4);
    }

    #[test]
    fn relay_assignment_factory_migrates_schema_one_payloads() {
        let repository =
            replicant_workflow::WorkflowRepository::open_in_memory().expect("repository");
        let state: RelayExecutionState = serde_json::from_value(serde_json::json!({
            "version": 2,
            "mission_id": "legacy-mission",
            "legacy_mission_tags": [],
            "replicant_code": "R-1",
            "vessel_code": "VESSEL-1",
            "hub_location": "ROOT-1-L4",
            "start_system": "ROOT",
            "targets": ["DONE", "PENDING"],
            "max_hop_ly": 7.499,
            "network": {
                "start": "ROOT",
                "requested_targets": ["DONE", "PENDING"],
                "max_hop_ly": 7.499,
                "nodes": [],
                "edges": [],
                "new_relay_systems": ["DONE", "PENDING"],
                "activation_systems": [],
                "active_relay_systems": ["ROOT"],
                "execution_order": ["DONE", "PENDING"],
                "execution_order_optimal": true,
                "execution_hops": 2,
                "execution_distance_ly": 12.0,
                "total_edge_distance_ly": 12.0
            },
            "stops": [
                {
                    "system": "DONE",
                    "location": "DONE-1-L4",
                    "parent_system": "ROOT",
                    "action": "deploy_and_activate",
                    "relay_code": "RELAY-1",
                    "completed": true
                },
                {
                    "system": "PENDING",
                    "location": "PENDING-1-L4",
                    "parent_system": "DONE",
                    "action": "deploy_and_activate",
                    "relay_code": null,
                    "completed": false
                }
            ],
            "hub_stock_relays": [],
            "print_jobs": [],
            "planned_transport_capacity": 4,
            "supply": null,
            "dsr_carrier_code": null,
            "returned_to_hub": false
        }))
        .expect("valid legacy relay state");
        let config = LegacyRelayWorkflowConfig {
            request: RelayExpansionRequest {
                replicant: "R-1".into(),
                hub: "ROOT-1-L4".into(),
                targets: vec!["DONE".into(), "PENDING".into()],
                mission_file: std::path::PathBuf::from("legacy-relay.json"),
                max_hop_ly: 7.499,
                wait_timeout: Duration::new(60, 123),
                unavailable_autofactories: BTreeSet::new(),
            },
        };
        let legacy = repository
            .create(NewWorkflow {
                kind: relay_workflow_kind(),
                schema_version: 1,
                config,
                checkpoint: RelayWorkflowCheckpoint {
                    state: Some(state),
                    region: None,
                    completed_steps: BTreeSet::from(["planned".into()]),
                },
                current_step: Some("queued".into()),
                parent_id: None,
            })
            .expect("legacy workflow");
        let factory = RelayWorkflowFactory::new();
        let migration = factory
            .migrate(&legacy)
            .expect("migrate")
            .expect("migration exists");
        let migrated: RelayWorkflowCheckpoint =
            serde_json::from_value(migration.checkpoint().clone()).expect("checkpoint");
        let specs = relay_work_item_specs(
            legacy.id,
            migrated.state.as_ref().expect("migrated state"),
            "Alpha",
        )
        .expect("materialize incomplete stops");
        assert_eq!(specs.len(), 1);
        assert!(specs[0].dedupe_key.contains("PENDING"));
        assert!(!specs[0].dedupe_key.contains("DONE"));
        let migrated_config: RelayWorkflowConfig =
            serde_json::from_value(migration.config().clone()).expect("migrated config");
        assert_eq!(migrated_config.hub, "ROOT-1-L4");
        assert_eq!(migrated_config.wait_timeout_seconds, 60);
        assert_eq!(migrated_config.wait_timeout_nanoseconds, 123);
        assert!(migration.config().get("replicant").is_none());
        assert!(migration.config().get("request").is_none());
        assert_eq!(factory.current_schema_version(), 2);
    }
    #[test]
    fn direct_factory_projectors_cover_current_payloads_and_reject_legacy() {
        let repository =
            replicant_workflow::WorkflowRepository::open_in_memory().expect("repository");
        let survey = repository
            .create(NewWorkflow {
                kind: survey_workflow_kind(),
                schema_version: 2,
                config: SurveyWorkflowConfig {
                    region: "alpha".into(),
                    center: "HOME".into(),
                    radius_ly: 1.0,
                    system_limit: 1,
                    target_systems: None,
                    star_detail_concurrency: 1,
                    mission_file: PathBuf::from("survey.json"),
                    replace_plan: false,
                    include_explored: false,
                    travel_timeout: Duration::from_secs(1),
                    survey_timeout: Duration::from_secs(1),
                    maintenance_home: "HOME".into(),
                    maintenance_interval: 1,
                    maintenance_threshold_pct: 10.0,
                    maintenance_resume_pct: 90.0,
                    maintenance_check_interval: Duration::from_secs(1),
                },
                checkpoint: SurveyWorkflowCheckpoint::default(),
                current_step: None,
                parent_id: None,
            })
            .expect("survey workflow");
        let relay = repository
            .create(NewWorkflow {
                kind: relay_workflow_kind(),
                schema_version: 2,
                config: RelayWorkflowConfig {
                    hub: "HOME".into(),
                    region: Some("alpha".into()),
                    targets: vec!["TARGET".into()],
                    mission_file: PathBuf::from("relay.json"),
                    max_hop_ly: 1.0,
                    wait_timeout_seconds: 1,
                    wait_timeout_nanoseconds: 0,
                    unavailable_autofactories: BTreeSet::new(),
                },
                checkpoint: RelayWorkflowCheckpoint::default(),
                current_step: None,
                parent_id: None,
            })
            .expect("relay workflow");
        let mining = repository
            .create(NewWorkflow {
                kind: mining_workflow_kind(),
                schema_version: 2,
                config: MiningWorkflowConfig {
                    systems: vec!["TARGET".into()],
                    region: "alpha".into(),
                    hub: "HOME".into(),
                    transport_routes: Vec::new(),
                    mission_file: PathBuf::from("mining.json"),
                    wait_timeout_seconds: 1,
                    max_concurrency: 1,
                },
                checkpoint: MiningWorkflowCheckpoint::default(),
                current_step: None,
                parent_id: None,
            })
            .expect("mining workflow");
        let event = repository
            .create(NewWorkflow {
                kind: event_workflow_kind(),
                schema_version: 2,
                config: EventWorkflowConfig {
                    event: Some("EVENT".into()),
                    criterion: None,
                    region: "alpha".into(),
                    home: "HOME".into(),
                    plan_file: PathBuf::from("event.json"),
                    replace_plan: false,
                    wait_timeout_seconds: 1,
                },
                checkpoint: EventWorkflowCheckpoint::default(),
                current_step: None,
                parent_id: None,
            })
            .expect("event workflow");
        let requirement = repository
            .create(NewWorkflow {
                kind: requirement_workflow_kind(),
                schema_version: 1,
                config: RequirementWorkflowConfig {
                    requirement: Requirement {
                        id: "req".into(),
                        name: "requirement".into(),
                        scope: RequirementScope::Location("HOME".into()),
                        target: crate::requirements::RequirementTarget::Device {
                            device_type: "cargo_freighter".into(),
                            state: Default::default(),
                        },
                        desired: 1,
                        fulfillment: FulfillmentOperation {
                            operation_class: FulfillmentOperationClass::Action,
                            kind: "noop".into(),
                            parameters: BTreeMap::new(),
                            claims: vec![ResourceKey::Device("dev-1".into())],
                        },
                    },
                },
                checkpoint: RequirementWorkflowCheckpoint::default(),
                current_step: None,
                parent_id: None,
            })
            .expect("requirement workflow");
        let action = repository
            .create(NewWorkflow {
                kind: requirement_action_kind(),
                schema_version: 1,
                config: RequirementActionConfig {
                    requirement_id: "req".into(),
                    quantity: 1,
                    operation: FulfillmentOperation {
                        operation_class: FulfillmentOperationClass::Action,
                        kind: "noop".into(),
                        parameters: BTreeMap::new(),
                        claims: vec![ResourceKey::Device("dev-2".into())],
                    },
                },
                checkpoint: Value::Null,
                current_step: None,
                parent_id: None,
            })
            .expect("action workflow");

        assert_eq!(
            SurveyWorkflowFactory::new()
                .placement_intents(&survey, &[])
                .expect("survey projection")
                .coverage,
            WorkflowPlacementIntentCoverage::Complete
        );
        assert_eq!(
            RelayWorkflowFactory::new()
                .placement_intents(&relay, &[])
                .expect("relay projection")
                .coverage,
            WorkflowPlacementIntentCoverage::Complete
        );
        assert_eq!(
            MiningWorkflowFactory::new()
                .placement_intents(&mining, &[])
                .expect("mining projection")
                .coverage,
            WorkflowPlacementIntentCoverage::Complete
        );
        assert_eq!(
            EventWorkflowFactory::new()
                .placement_intents(&event, &[])
                .expect("event projection")
                .coverage,
            WorkflowPlacementIntentCoverage::Complete
        );
        let requirement_projection = RequirementWorkflowFactory::new()
            .placement_intents(&requirement, &[])
            .expect("requirement projection");
        assert_eq!(
            requirement_projection.intents[0].subject,
            WorkflowPlacementIntentSubject::Device("DEV-1".into())
        );
        assert_eq!(
            RequirementActionFactory::new()
                .placement_intents(&action, &[])
                .expect("action projection")
                .coverage,
            WorkflowPlacementIntentCoverage::Complete
        );

        let legacy = repository
            .create(NewWorkflow {
                kind: survey_workflow_kind(),
                schema_version: 1,
                config: Value::Null,
                checkpoint: Value::Null,
                current_step: None,
                parent_id: None,
            })
            .expect("legacy workflow");
        assert_eq!(
            SurveyWorkflowFactory::new()
                .placement_intents(&legacy, &[])
                .expect("legacy projection")
                .coverage,
            WorkflowPlacementIntentCoverage::Unknown
        );
    }
    #[test]
    fn placement_status_boundaries_keep_terminal_and_opaque_state_conservative() {
        let subject = placement_subject(" dev-9 ").expect("device subject");
        assert_eq!(
            status_intent(WorkflowStatus::Queued, subject.clone(), None, None, None)
                .expect("live intent")
                .relation,
            WorkflowPlacementIntentRelation::Awaited
        );
        assert_eq!(
            status_intent(
                WorkflowStatus::Failed,
                subject.clone(),
                Some(WorkflowPlacementIntentRelation::Transported),
                None,
                None
            )
            .expect("failed custody")
            .relation,
            WorkflowPlacementIntentRelation::Transported
        );
        assert_eq!(
            status_intent(
                WorkflowStatus::Cancelled,
                subject.clone(),
                Some(WorkflowPlacementIntentRelation::Staged),
                None,
                None
            )
            .expect("cancelled custody")
            .relation,
            WorkflowPlacementIntentRelation::Staged
        );
        assert_eq!(
            status_intent(
                WorkflowStatus::Succeeded,
                subject,
                Some(WorkflowPlacementIntentRelation::Deployed),
                None,
                Some("HOME".into())
            )
            .expect("settled placement")
            .expected_location
            .as_deref(),
            Some("HOME")
        );
        assert!(
            placement_subject(" ").is_none(),
            "blank device codes are not exact subjects"
        );
    }

    fn event_projection_instance(
        repository: &replicant_workflow::WorkflowRepository,
    ) -> replicant_workflow::WorkflowInstance {
        repository
            .create(NewWorkflow {
                kind: event_workflow_kind(),
                schema_version: 2,
                config: EventWorkflowConfig {
                    event: Some("EVENT".into()),
                    criterion: None,
                    region: "alpha".into(),
                    home: "HOME".into(),
                    plan_file: PathBuf::from("event.json"),
                    replace_plan: false,
                    wait_timeout_seconds: 1,
                },
                checkpoint: EventWorkflowCheckpoint::default(),
                current_step: None,
                parent_id: None,
            })
            .expect("event workflow")
    }

    fn event_projection_item(payload_json: Value) -> WorkItem {
        WorkItem {
            id: replicant_workflow::WorkItemId::new(),
            spec: WorkItemSpec {
                workflow_id: WorkflowId::new(),
                dedupe_key: "event:item".into(),
                kind: WorkflowKind::new("event.stage").expect("event item kind"),
                sort_key: "event:item".into(),
                payload_json,
                preconditions_json: Value::Array(Vec::new()),
                requirements_json: Value::Array(Vec::new()),
                deadline_at_ms: None,
            },
            state: replicant_workflow::WorkItemState {
                status: WorkItemStatus::Pending,
                checkpoint_json: None,
                result_json: None,
                last_error: None,
                attempt_count: 0,
                consecutive_failure_count: 0,
                next_attempt_at_ms: None,
                ever_started: false,
                created_at_ms: 0,
                updated_at_ms: 0,
                revision: 0,
            },
        }
    }

    fn event_item_payload(mission_json: &str) -> Value {
        serde_json::json!({
            "event": "EVENT",
            "criterion": "CRITERION",
            "selected": true,
            "stage": "stage",
            "mission_path": "event.json",
            "mission_json": mission_json,
            "legacy_complete": false,
        })
    }

    #[test]
    fn event_projection_requires_decoded_mission_content_for_complete_coverage() {
        let repository =
            replicant_workflow::WorkflowRepository::open_in_memory().expect("repository");
        let instance = event_projection_instance(&repository);
        let factory = EventWorkflowFactory::new();

        let empty = factory
            .placement_intents(&instance, &[event_projection_item(event_item_payload(""))])
            .expect("empty mission projection");
        assert_eq!(
            empty.coverage,
            WorkflowPlacementIntentCoverage::Complete,
            "an outer event item with no mission document is fully understood",
        );

        let opaque = factory
            .placement_intents(
                &instance,
                &[event_projection_item(event_item_payload(
                    r#"{"claimed_devices":[{"device_code":"HIDDEN-DEVICE"}]}"#,
                ))],
            )
            .expect("opaque mission projection");
        assert_eq!(
            opaque.coverage,
            WorkflowPlacementIntentCoverage::Unknown,
            "opaque mission JSON cannot prove absence of placement intent",
        );
        assert!(opaque.intents.is_empty());
    }

    #[test]
    fn event_projection_rejects_unmodelled_device_references() {
        let repository =
            replicant_workflow::WorkflowRepository::open_in_memory().expect("repository");
        let instance = event_projection_instance(&repository);
        let mut payload = event_item_payload("");
        payload["device_code"] = Value::String("SILENTLY-DROPPED".into());

        let projection = EventWorkflowFactory::new()
            .placement_intents(&instance, &[event_projection_item(payload)])
            .expect("unknown outer event projection");
        assert_eq!(
            projection.coverage,
            WorkflowPlacementIntentCoverage::Unknown,
            "new device-bearing fields must not be ignored by a complete projector",
        );
    }
}
