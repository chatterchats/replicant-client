//! Durable workflow adapters for the application's restart-safe runtime services.

use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};

use replicant_client::raw::RequestPriority;
use replicant_workflow::{
    BoxWorkflowFuture, ClaimAcquireOutcome, NewWorkflow, RegistryError, ResourceKey,
    WorkflowContext, WorkflowExecutor, WorkflowFactory, WorkflowId, WorkflowKind, WorkflowRegistry,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    automation::reconcile_event_connectivity,
    catalogue::OperationCatalogue,
    event::{
        EventExecutionRequest, EventPlanningRequest, event_mission_preflight, execute_event,
        plan_event_mission,
    },
    mining::{MiningExpansionRequest, execute_expansion},
    relay::{
        RelayExecutionState, RelayExpansionRequest, execute_relay_workflow,
        restore_relay_checkpoint,
    },
    requirements::{
        ActiveFulfillment, FulfillmentOperation, FulfillmentOperationClass, FulfillmentPlan,
        Requirement, evaluate_requirement, managed_facts,
    },
    survey::{
        SurveyExecutionState, SurveyOptions, execute_survey_workflow, restore_survey_checkpoint,
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

/// Persisted survey workflow configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SurveyWorkflowConfig {
    /// Existing runtime service options.
    pub options: SurveyOptions,
}

/// Persisted survey workflow checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct SurveyWorkflowCheckpoint {
    /// Last authoritative survey executor state.
    pub state: Option<SurveyExecutionState>,
    /// Completed phase names, retained for restart reconciliation activity.
    pub completed_steps: BTreeSet<String>,
}

/// Persisted relay workflow configuration.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RelayWorkflowConfig {
    /// Existing runtime service request.
    pub request: RelayExpansionRequest,
}

/// Persisted relay workflow checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct RelayWorkflowCheckpoint {
    /// Last authoritative relay executor state.
    pub state: Option<RelayExecutionState>,
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

/// Persisted mining expansion inputs. The child mission file remains the detailed restart checkpoint.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MiningWorkflowConfig {
    /// Systems that should receive mining installations.
    pub systems: Vec<String>,
    /// Replicant that owns and executes the expansion.
    pub replicant: String,
    /// Manufacturing hub used for staging and printing.
    pub hub: String,
    /// Existing mining mission file used for detailed restart reconciliation.
    pub mission_file: std::path::PathBuf,
    /// Maximum duration for managed-state waits.
    pub wait_timeout_seconds: u64,
    /// Maximum number of system deployments advanced concurrently.
    pub max_concurrency: usize,
}

/// Lightweight workflow checkpoint around the mining mission file.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct MiningWorkflowCheckpoint {
    /// Whether execution has entered the existing mining mission executor.
    pub started: bool,
}

/// Persisted event execution inputs. The event plan/campaign file is the detailed restart checkpoint.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventWorkflowConfig {
    /// Event designation to plan before first execution. `None` resumes an existing plan/campaign.
    #[serde(default)]
    pub event: Option<String>,
    /// Completion criterion when the selected event offers multiple paths.
    #[serde(default)]
    pub criterion: Option<String>,
    /// Replicant assigned when a new event plan is created.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Manufacturing/staging home used when a new event plan is created.
    #[serde(default)]
    pub home: Option<String>,
    /// Event plan or campaign file used for detailed restart reconciliation.
    pub plan_file: std::path::PathBuf,
    /// Replace an existing plan when creating a new mission.
    #[serde(default)]
    pub replace_plan: bool,
    /// Maximum duration for managed-state waits.
    pub wait_timeout_seconds: u64,
}

/// Lightweight workflow checkpoint around the event mission/campaign file.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventWorkflowCheckpoint {
    /// Whether execution has entered the existing event executor.
    pub started: bool,
    /// Relay-expansion workflow satisfying a disconnected event destination.
    #[serde(default)]
    pub connectivity_workflows: BTreeMap<String, WorkflowId>,
    /// Whether the event mission should be replanned after connectivity changes.
    #[serde(default)]
    pub replan_after_connectivity: bool,
}

/// Factory for durable mining expansions backed by the existing restart-safe mission file.
pub struct MiningWorkflowFactory(WorkflowKind);
impl MiningWorkflowFactory {
    /// Creates the stable mining workflow factory.
    #[must_use]
    pub fn new() -> Self {
        Self(mining_workflow_kind())
    }
}
impl Default for MiningWorkflowFactory {
    fn default() -> Self {
        Self::new()
    }
}
impl WorkflowFactory for MiningWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.0
    }

    fn current_schema_version(&self) -> u32 {
        SCHEMA_VERSION
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(MiningWorkflow))
    }
}
struct MiningWorkflow;
impl WorkflowExecutor for MiningWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let config: MiningWorkflowConfig =
                context.config().map_err(|error| error.to_string())?;
            let mut checkpoint: MiningWorkflowCheckpoint =
                context.checkpoint().map_err(|error| error.to_string())?;
            let client = context
                .managed_client()
                .cloned()
                .ok_or_else(|| "mining workflow requires a managed client".to_owned())?;
            claim(context, ResourceKey::Replicant(config.replicant.clone()))?;
            claim(
                context,
                ResourceKey::Namespaced {
                    namespace: "location".to_owned(),
                    key: config.hub.clone(),
                },
            )?;
            checkpoint.started = true;
            context
                .advance_to("executing", &checkpoint)
                .map_err(|error| error.to_string())?;
            emit(
                context,
                &WorkflowActivityEvent::StepEntered {
                    step: "executing".to_owned(),
                },
            )?;
            let request = MiningExpansionRequest {
                systems: config.systems,
                replicant: config.replicant,
                hub: config.hub,
                mission_file: config.mission_file,
                wait_timeout: Duration::from_secs(config.wait_timeout_seconds),
                max_concurrency: config.max_concurrency,
            };
            match execute_expansion(&client, &request).await {
                Ok(report) => {
                    emit(context, &WorkflowActivityEvent::Completion)?;
                    context
                        .mark_succeeded(Some(report))
                        .map_err(|error| error.to_string())
                }
                Err(error) => {
                    let message = error.to_string();
                    emit(
                        context,
                        &WorkflowActivityEvent::Failure {
                            error: message.clone(),
                        },
                    )?;
                    Err(message)
                }
            }
        })
    }
}

/// Factory for durable event mission/campaign execution backed by its persisted plan file.
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
        SCHEMA_VERSION
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
            claim(
                context,
                ResourceKey::Namespaced {
                    namespace: "event_plan".to_owned(),
                    key: config.plan_file.display().to_string(),
                },
            )?;
            if !checkpoint.started {
                if let Some(event) = config.event.as_deref()
                    && (!config.plan_file.exists() || config.replace_plan)
                {
                    let replicant = config
                        .replicant
                        .clone()
                        .ok_or_else(|| "event workflow planning requires a replicant".to_owned())?;
                    let home = config.home.clone().ok_or_else(|| {
                        "event workflow planning requires a manufacturing home".to_owned()
                    })?;
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
            context
                .advance_to("executing", &checkpoint)
                .map_err(|error| error.to_string())?;
            emit(
                context,
                &WorkflowActivityEvent::StepEntered {
                    step: "executing".to_owned(),
                },
            )?;
            let request = EventExecutionRequest::new(
                config.plan_file,
                Duration::from_secs(config.wait_timeout_seconds),
            );
            match execute_event(&client, &request).await {
                Ok(report) => {
                    emit(context, &WorkflowActivityEvent::Completion)?;
                    context
                        .mark_succeeded(Some(report))
                        .map_err(|error| error.to_string())
                }
                Err(error) => {
                    let message = error.to_string();
                    emit(
                        context,
                        &WorkflowActivityEvent::Failure {
                            error: message.clone(),
                        },
                    )?;
                    Err(message)
                }
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

/// Factory for durable survey routes.
pub struct SurveyWorkflowFactory(WorkflowKind);

impl SurveyWorkflowFactory {
    /// Creates the factory.
    pub fn new() -> Self {
        Self(survey_workflow_kind())
    }
}

impl Default for SurveyWorkflowFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowFactory for SurveyWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.0
    }

    fn current_schema_version(&self) -> u32 {
        SCHEMA_VERSION
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(SurveyWorkflow))
    }
}

struct SurveyWorkflow;

impl WorkflowExecutor for SurveyWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let config: SurveyWorkflowConfig = context.config().map_err(|e| e.to_string())?;
            let mut checkpoint: SurveyWorkflowCheckpoint =
                context.checkpoint().map_err(|e| e.to_string())?;
            let client = context
                .managed_client()
                .cloned()
                .ok_or_else(|| "survey workflow requires a managed client".to_owned())?;

            claim(
                context,
                ResourceKey::Replicant(config.options.replicant.clone()),
            )?;
            claim(context, ResourceKey::Device(config.options.vessel.clone()))?;
            if let Some(state) = &checkpoint.state {
                restore_survey_checkpoint(&config.options.mission_file, state)
                    .map_err(|e| e.to_string())?;
                emit(
                    context,
                    &WorkflowActivityEvent::ReconciliationDecision {
                        step: state.step_name(),
                        decision:
                            "restored workflow checkpoint before managed-state reconciliation"
                                .to_owned(),
                    },
                )?;
            }

            let result = execute_survey_workflow(&client, &config.options, |state| {
                let step = state.step_name();
                if let Some(previous) = checkpoint.state.as_ref().map(|state| state.step_name())
                    && previous != step
                {
                    checkpoint.completed_steps.insert(previous.clone());
                    emit(
                        context,
                        &WorkflowActivityEvent::OperationCompleted { step: previous },
                    )?;
                }
                let (replicant, vessel, devices) = state.resources();
                claim(context, ResourceKey::Replicant(replicant.to_owned()))?;
                claim(context, ResourceKey::Device(vessel.to_owned()))?;
                for device in devices {
                    claim(context, ResourceKey::Device(device.to_owned()))?;
                }
                emit(
                    context,
                    &WorkflowActivityEvent::ReconciliationDecision {
                        step: step.clone(),
                        decision: "managed state accepted checkpoint; mutation remains necessary"
                            .to_owned(),
                    },
                )?;
                emit(
                    context,
                    &WorkflowActivityEvent::StepEntered { step: step.clone() },
                )?;
                if matches!(
                    step.as_str(),
                    "traveling" | "surveying" | "maintenance_repairing"
                ) {
                    emit(
                        context,
                        &WorkflowActivityEvent::WaitReason {
                            step: step.clone(),
                            reason: "awaiting SSE or managed-state revision evidence".to_owned(),
                        },
                    )?;
                } else if step != "complete" {
                    emit(
                        context,
                        &WorkflowActivityEvent::OperationSubmitted { step: step.clone() },
                    )?;
                }
                checkpoint.state = Some(state);
                context
                    .advance_to(step, &checkpoint)
                    .map_err(|error| error.to_string().into())
            })
            .await;

            match result {
                Ok(summary) => {
                    emit(context, &WorkflowActivityEvent::Completion)?;
                    context
                        .mark_succeeded(Some(summary))
                        .map_err(|error| error.to_string())
                }
                Err(error) => {
                    let error = error.to_string();
                    emit(
                        context,
                        &WorkflowActivityEvent::Failure {
                            error: error.clone(),
                        },
                    )?;
                    Err(error)
                }
            }
        })
    }
}

/// Factory for durable relay expansion.
pub struct RelayWorkflowFactory(WorkflowKind);

impl RelayWorkflowFactory {
    /// Creates the factory.
    pub fn new() -> Self {
        Self(relay_workflow_kind())
    }
}

impl Default for RelayWorkflowFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkflowFactory for RelayWorkflowFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.0
    }

    fn current_schema_version(&self) -> u32 {
        SCHEMA_VERSION
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(RelayWorkflow))
    }
}

struct RelayWorkflow;

impl WorkflowExecutor for RelayWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let config: RelayWorkflowConfig = context.config().map_err(|e| e.to_string())?;
            let mut checkpoint: RelayWorkflowCheckpoint =
                context.checkpoint().map_err(|e| e.to_string())?;
            let client = context
                .managed_client()
                .cloned()
                .ok_or_else(|| "relay workflow requires a managed client".to_owned())?;

            claim(
                context,
                ResourceKey::Replicant(config.request.replicant.clone()),
            )?;
            if let Some(state) = &checkpoint.state {
                restore_relay_checkpoint(&config.request.mission_file, state)
                    .map_err(|e| e.to_string())?;
                emit(
                    context,
                    &WorkflowActivityEvent::ReconciliationDecision {
                        step: state.step_name().to_owned(),
                        decision:
                            "restored workflow checkpoint before managed-state reconciliation"
                                .to_owned(),
                    },
                )?;
            }

            let result = execute_relay_workflow(&client, &config.request, |state| {
                let step = state.step_name().to_owned();
                if let Some(previous) = checkpoint
                    .state
                    .as_ref()
                    .map(|state| state.step_name().to_owned())
                    && previous != step
                {
                    checkpoint.completed_steps.insert(previous.clone());
                    emit(
                        context,
                        &WorkflowActivityEvent::OperationCompleted { step: previous },
                    )?;
                }
                let (replicant, devices, factories) = state.resources();
                claim(context, ResourceKey::Replicant(replicant.to_owned()))?;
                for device in devices {
                    claim(context, ResourceKey::Device(device.to_owned()))?;
                }
                let factories = factories
                    .into_iter()
                    .map(str::to_owned)
                    .collect::<BTreeSet<_>>();
                reconcile_relay_autofactory_claims(context, &factories)?;
                emit(
                    context,
                    &WorkflowActivityEvent::ReconciliationDecision {
                        step: step.clone(),
                        decision: "managed state accepted checkpoint; mutation remains necessary"
                            .to_owned(),
                    },
                )?;
                emit(
                    context,
                    &WorkflowActivityEvent::StepEntered { step: step.clone() },
                )?;
                if step == "awaiting_relays" || step == "returning_to_hub" {
                    emit(
                        context,
                        &WorkflowActivityEvent::WaitReason {
                            step: step.clone(),
                            reason: "awaiting SSE or managed-state revision evidence".to_owned(),
                        },
                    )?;
                } else if step != "complete" {
                    emit(
                        context,
                        &WorkflowActivityEvent::OperationSubmitted { step: step.clone() },
                    )?;
                }
                checkpoint.state = Some(state);
                context
                    .advance_to(step, &checkpoint)
                    .map_err(|error| error.to_string().into())
            })
            .await;

            match result {
                Ok(report) => {
                    reconcile_relay_autofactory_claims(context, &BTreeSet::new())?;
                    emit(context, &WorkflowActivityEvent::Completion)?;
                    context
                        .mark_succeeded(Some(report))
                        .map_err(|error| error.to_string())
                }
                Err(error) => {
                    let error = error.to_string();
                    emit(
                        context,
                        &WorkflowActivityEvent::Failure {
                            error: error.clone(),
                        },
                    )?;
                    Err(error)
                }
            }
        })
    }
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
        schema_version: SCHEMA_VERSION,
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
            checkpoint = serde_json::from_str(&stored).expect("restart checkpoint");
        }
        assert_eq!(checkpoint.completed_steps.len(), 4);
    }
}
