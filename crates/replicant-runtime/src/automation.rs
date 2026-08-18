//! Intent-driven workflow layer for web/Tauri automation.
//!
//! These workflows accept player goals instead of CLI execution plumbing.
//! They compose the managed client, reusable runtime services, and durable
//! child workflows while keeping workflow checkpoints authoritative.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use replicant_client::{
    Client, DeviceType, MiningDirective, Operation, OperationStatus, SurveyDirective,
    domain::AccessScope,
};
use replicant_transport::{
    DeliveryOptions, DeliveryPlan, DeliveryRequest, DeviceRequest, ResourceMap, execute_delivery,
    plan_delivery,
};
use replicant_workflow::{
    BoxWorkflowFuture, ClaimAcquireOutcome, NewWorkflow, RegistryError, ResourceKey,
    WorkflowContext, WorkflowExecutor, WorkflowFactory, WorkflowId, WorkflowKind, WorkflowRegistry,
    WorkflowStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    event::{
        EventExecutionRequest, EventPlanningRequest, execute_event_mission, plan_event_mission,
        prestage_event_mission,
    },
    mining::{MiningExpansionRequest, execute_expansion},
    observatory::auto_prospect,
    relay::{
        RelayExecutionState, RelayExpansionRequest, execute_relay_workflow,
        restore_relay_checkpoint,
    },
    survey::{
        SurveyExecutionState, SurveyMode, SurveyOptions, execute_survey_workflow,
        restore_survey_checkpoint,
    },
};

const SCHEMA_VERSION: u32 = 1;
const DEFAULT_WAIT_SECONDS: u64 = 21_600;

/// Intent-native workflow that fully surveys one system with an AMI survey controller.
pub fn scan_system_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("scan.system").expect("static workflow kind is valid")
}

/// Intent-native workflow that searches one system's asteroid belt.
pub fn scan_belt_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("scan.belt").expect("static workflow kind is valid")
}

/// Intent-native workflow that surveys a bounded area with one racing vessel.
pub fn scan_tour_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("scan.tour").expect("static workflow kind is valid")
}

/// Intent-native workflow that salvages one site to depletion.
pub fn salvage_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("salvage.site").expect("static workflow kind is valid")
}

/// Intent-native workflow that deploys one mining installation.
pub fn mining_deploy_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("mining.deploy").expect("static workflow kind is valid")
}

/// Intent-native point-to-point logistics workflow.
pub fn logistics_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("logistics.delivery").expect("static workflow kind is valid")
}

/// Intent-native directed exploration workflow backed by the relay expansion engine.
pub fn exploration_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("exploration.frontier").expect("static workflow kind is valid")
}

/// Event workflow that prepares event payloads without claiming the assigned replicant.
pub fn event_delivery_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("event.delivery").expect("static workflow kind is valid")
}

/// Event workflow that ensures delivery is ready, then dispatches the replicant to resolve it.
pub fn event_tour_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("event.tour").expect("static workflow kind is valid")
}

/// Intent-native bounded observatory prospect workflow.
pub fn observatory_workflow_kind() -> WorkflowKind {
    WorkflowKind::new("observatory.search").expect("static workflow kind is valid")
}

/// Registers intent-native application workflows.
pub fn register(registry: &mut WorkflowRegistry) -> Result<(), RegistryError> {
    registry.register(Arc::new(ScanSystemWorkflowFactory::new()))?;
    registry.register(Arc::new(ScanBeltWorkflowFactory::new()))?;
    registry.register(Arc::new(ScanTourWorkflowFactory::new()))?;
    registry.register(Arc::new(SalvageWorkflowFactory::new()))?;
    registry.register(Arc::new(MiningDeployWorkflowFactory::new()))?;
    registry.register(Arc::new(LogisticsWorkflowFactory::new()))?;
    registry.register(Arc::new(ExplorationWorkflowFactory::new()))?;
    registry.register(Arc::new(EventDeliveryWorkflowFactory::new()))?;
    registry.register(Arc::new(EventTourWorkflowFactory::new()))?;
    registry.register(Arc::new(ObservatoryWorkflowFactory::new()))
}

/// Shared player intent for system/belt survey automation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScanIntent {
    /// System to survey or belt-search.
    pub system: String,
    /// Optional survey controller to pin. When omitted an idle owned controller in-system is used.
    #[serde(default)]
    pub controller: Option<String>,
    /// Whether the controller should recall its fleet when the directive supports it.
    #[serde(default = "default_true")]
    pub recall: bool,
}

/// Goal-level input for a bounded survey tour.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScanTourIntent {
    /// Centre system for route planning.
    pub center: String,
    /// Radius around the centre system.
    #[serde(default = "default_tour_radius")]
    pub radius_ly: f64,
    /// Maximum systems to include in one route.
    #[serde(default = "default_tour_limit")]
    pub system_limit: usize,
    /// Optional replicant to pin.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Optional racing vessel to pin.
    #[serde(default)]
    pub vessel: Option<String>,
    /// Include systems already marked explored.
    #[serde(default)]
    pub include_explored: bool,
}

/// Authoritative checkpoint for an intent-native survey tour.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ScanTourCheckpoint {
    /// Resolved replicant.
    pub replicant: Option<String>,
    /// Resolved racing vessel.
    pub vessel: Option<String>,
    /// Resolved maintenance home.
    pub maintenance_home: Option<String>,
    /// Last authoritative survey executor state.
    pub state: Option<SurveyExecutionState>,
}

/// Restart-safe controller workflow checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ControllerWorkflowCheckpoint {
    /// Controller selected by the workflow.
    pub controller: Option<String>,
    /// Whether the desired directive has been accepted.
    pub directive_set: bool,
    /// Whether the controller fleet has been launched.
    pub launched: bool,
    /// Whether managed state has observed the controller actively coordinating.
    pub observed_active: bool,
    /// Consecutive idle observations after launch, used to tolerate fast directives.
    #[serde(default)]
    pub idle_observations: u8,
}

/// Player intent for one salvage site.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SalvageIntent {
    /// Exact salvage-site location designation.
    pub location: String,
    /// Optional AMI mining controller to pin.
    #[serde(default)]
    pub controller: Option<String>,
    /// Recall the fleet after depletion.
    #[serde(default = "default_true")]
    pub recall: bool,
}

/// Goal-level input for deploying one mining installation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MiningDeployIntent {
    /// Target system.
    pub system: String,
    /// Optional replicant to pin.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Optional manufacturing hub.
    #[serde(default)]
    pub hub: Option<String>,
}

/// Restart-safe mining deployment checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct MiningDeployCheckpoint {
    /// Resolved replicant.
    pub replicant: Option<String>,
    /// Resolved manufacturing hub.
    pub hub: Option<String>,
    /// Last serialized legacy mining mission state.
    pub plan_json: Option<String>,
    /// Whether execution entered the reusable mining executor.
    pub started: bool,
}

/// Human-facing payload selector for one logistics delivery.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum LogisticsPayloadKind {
    /// Resource type and quantity.
    Resource,
    /// Device type and quantity.
    Device,
    /// Every eligible device carrying the supplied tag.
    Tag,
}

/// Player intent for point-to-point logistics.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LogisticsIntent {
    /// Origin location or system scope.
    pub origin: String,
    /// Exact destination location.
    pub destination: String,
    /// Whether `item` names a resource type, device type, or tag.
    pub payload_kind: LogisticsPayloadKind,
    /// Resource type, device type, or tag.
    pub item: String,
    /// Requested resource/device quantity. Ignored for tag payloads.
    #[serde(default = "default_quantity")]
    pub quantity: i64,
    /// Return transports after delivery.
    #[serde(default)]
    pub return_transports: bool,
}

/// Restart-safe logistics checkpoint. The concrete plan is persisted before mutation.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LogisticsWorkflowCheckpoint {
    /// Concrete transport plan selected from managed state.
    pub plan: Option<DeliveryPlan>,
    /// Whether execution entered the reusable transport executor.
    pub started: bool,
}

/// Directed frontier-expansion intent.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ExplorationIntent {
    /// System the relay network should reach.
    pub target: String,
    /// Optional replicant. When omitted the first owned replicant is selected deterministically.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Optional manufacturing hub. When omitted an owned autofactory location is selected.
    #[serde(default)]
    pub hub: Option<String>,
}

/// Authoritative exploration checkpoint. The old relay mission file is only an ephemeral adapter.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ExplorationWorkflowCheckpoint {
    /// Resolved replicant retained across restarts.
    pub replicant: Option<String>,
    /// Resolved manufacturing hub retained across restarts.
    pub hub: Option<String>,
    /// Last authoritative relay executor state.
    pub state: Option<RelayExecutionState>,
}

/// Goal-level event input shared by delivery and tour workflows.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EventIntent {
    /// Event designation.
    pub event: String,
    /// Optional completion criterion.
    #[serde(default)]
    pub criterion: Option<String>,
    /// Optional replicant to use when the event is ultimately resolved.
    #[serde(default)]
    pub replicant: Option<String>,
    /// Optional manufacturing/staging home.
    #[serde(default)]
    pub home: Option<String>,
}

/// Authoritative delivery checkpoint. `plan_json` replaces the GUI-facing mission file.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventDeliveryCheckpoint {
    /// Resolved replicant recorded by planning without reserving it for staging.
    pub replicant: Option<String>,
    /// Resolved manufacturing home.
    pub home: Option<String>,
    /// Serialized event plan at the last durable workflow boundary.
    pub plan_json: Option<String>,
    /// Whether all requirements are physically staged at the event.
    pub ready: bool,
}

/// Parent event-tour checkpoint.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct EventTourCheckpoint {
    /// Child delivery workflow created for this event.
    pub delivery_child: Option<WorkflowId>,
    /// Resolved replicant used for the final event visit.
    pub replicant: Option<String>,
    /// Final serialized plan snapshot.
    pub plan_json: Option<String>,
}

/// Optional observatory pin for automatic prospecting.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct ObservatoryIntent {
    /// Observatory device code. Omit to let the runtime choose an eligible observatory.
    #[serde(default)]
    pub observatory: Option<String>,
}

macro_rules! workflow_factory {
    ($name:ident, $executor:ident, $kind_fn:ident) => {
        /// Registered factory for this intent-native workflow.
        pub struct $name(WorkflowKind);

        impl $name {
            /// Creates the stable factory.
            #[must_use]
            pub fn new() -> Self {
                Self($kind_fn())
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl WorkflowFactory for $name {
            fn kind(&self) -> &WorkflowKind {
                &self.0
            }

            fn current_schema_version(&self) -> u32 {
                SCHEMA_VERSION
            }

            fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
                Some(Box::new($executor))
            }
        }
    };
}

workflow_factory!(ScanSystemWorkflowFactory, ScanSystemWorkflow, scan_system_workflow_kind);
workflow_factory!(ScanBeltWorkflowFactory, ScanBeltWorkflow, scan_belt_workflow_kind);
workflow_factory!(ScanTourWorkflowFactory, ScanTourWorkflow, scan_tour_workflow_kind);
workflow_factory!(SalvageWorkflowFactory, SalvageWorkflow, salvage_workflow_kind);
workflow_factory!(MiningDeployWorkflowFactory, MiningDeployWorkflow, mining_deploy_workflow_kind);
workflow_factory!(LogisticsWorkflowFactory, LogisticsWorkflow, logistics_workflow_kind);
workflow_factory!(ExplorationWorkflowFactory, ExplorationWorkflow, exploration_workflow_kind);
workflow_factory!(EventDeliveryWorkflowFactory, EventDeliveryWorkflow, event_delivery_workflow_kind);
workflow_factory!(EventTourWorkflowFactory, EventTourWorkflow, event_tour_workflow_kind);
workflow_factory!(ObservatoryWorkflowFactory, ObservatoryWorkflow, observatory_workflow_kind);

struct ScanSystemWorkflow;
impl WorkflowExecutor for ScanSystemWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: ScanIntent = context.config().map_err(string_error)?;
            run_survey_controller(context, intent, SurveyModeIntent::System).await
        })
    }
}

struct ScanBeltWorkflow;
impl WorkflowExecutor for ScanBeltWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: ScanIntent = context.config().map_err(string_error)?;
            run_survey_controller(context, intent, SurveyModeIntent::Belt).await
        })
    }
}

#[derive(Clone, Copy)]
enum SurveyModeIntent {
    System,
    Belt,
}

async fn run_survey_controller(
    context: &mut WorkflowContext,
    intent: ScanIntent,
    mode: SurveyModeIntent,
) -> Result<(), String> {
    let client = managed_client(context)?;
    let mut checkpoint: ControllerWorkflowCheckpoint = context.checkpoint().map_err(string_error)?;
    let controller = resolve_controller(
        &client,
        checkpoint.controller.as_deref().or(intent.controller.as_deref()),
        DeviceType::SurveyController,
        Some(&intent.system),
    )
    .await?;
    checkpoint.controller = Some(controller.clone());
    claim_device(context, &controller)?;
    claim_target(context, "survey-system", &intent.system)?;
    context.advance_to("configuring", &checkpoint).map_err(string_error)?;

    let survey = client
        .devices()
        .get(&controller)
        .await
        .map_err(string_error)?
        .as_survey_controller()
        .map_err(string_error)?;
    if !checkpoint.directive_set {
        let operation = match mode {
            SurveyModeIntent::System => {
                survey
                    .set_directive(SurveyDirective::SurveySystem {
                        planets: "all".to_owned(),
                        moons: "all".to_owned(),
                        recall: intent.recall,
                    })
                    .await
            }
            SurveyModeIntent::Belt => survey.set_directive(SurveyDirective::BeltSearch).await,
        }
        .map_err(string_error)?;
        await_success(&operation).await?;
        checkpoint.directive_set = true;
        context.persist_checkpoint(&checkpoint).map_err(string_error)?;
    }
    if !checkpoint.launched {
        context.advance_to("launching", &checkpoint).map_err(string_error)?;
        let operation = survey.launch().await.map_err(string_error)?;
        await_success(&operation).await?;
        checkpoint.launched = true;
        context.persist_checkpoint(&checkpoint).map_err(string_error)?;
    }
    if !wait_controller_completion(context, &client, &controller, &mut checkpoint).await? {
        return Ok(());
    }
    context
        .mark_succeeded(Some(serde_json::json!({
            "controller": controller,
            "system": intent.system,
            "directive": match mode { SurveyModeIntent::System => "survey_system", SurveyModeIntent::Belt => "belt_search" },
        })))
        .map_err(string_error)
}

struct ScanTourWorkflow;
impl WorkflowExecutor for ScanTourWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: ScanTourIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: ScanTourCheckpoint = context.checkpoint().map_err(string_error)?;
            let (replicant, vessel) = if let (Some(replicant), Some(vessel)) =
                (checkpoint.replicant.clone(), checkpoint.vessel.clone())
            {
                (replicant, vessel)
            } else {
                resolve_survey_assignment(
                    &client,
                    intent.replicant.as_deref(),
                    intent.vessel.as_deref(),
                )
                .await?
            };
            let maintenance_home = match checkpoint.maintenance_home.clone() {
                Some(value) => value,
                None => resolve_home(&client, None).await?,
            };
            checkpoint.replicant = Some(replicant.clone());
            checkpoint.vessel = Some(vessel.clone());
            checkpoint.maintenance_home = Some(maintenance_home.clone());
            claim(context, ResourceKey::Replicant(replicant.clone()))?;
            claim_device(context, &vessel)?;
            claim_target(context, "survey-tour", &intent.center)?;

            let plan_file = scratch_file(context.id(), "survey-plan.json")?;
            if let Some(state) = checkpoint.state.as_ref() {
                restore_survey_checkpoint(&plan_file, state).map_err(string_error)?;
            } else {
                clear_scratch_file(&plan_file)?;
            }
            let options = SurveyOptions {
                mode: SurveyMode::Run,
                replicant,
                vessel,
                center: intent.center,
                radius_ly: intent.radius_ly,
                system_limit: intent.system_limit.max(1),
                star_detail_concurrency: 8,
                mission_file: plan_file,
                controller: None,
                drones: None,
                replace_plan: false,
                include_explored: intent.include_explored,
                travel_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
                survey_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
                maintenance_home,
                maintenance_interval: 40,
                maintenance_threshold_pct: 25.0,
                maintenance_resume_pct: 95.0,
                maintenance_check_interval: Duration::from_secs(900),
            };
            let result = execute_survey_workflow(&client, &options, |state| {
                let (replicant, vessel, devices) = state.resources();
                claim(context, ResourceKey::Replicant(replicant.to_owned()))?;
                claim_device(context, vessel)?;
                for device in devices {
                    claim_device(context, device)?;
                }
                checkpoint.state = Some(state.clone());
                context
                    .advance_to(state.step_name(), &checkpoint)
                    .map_err(|error| error.to_string().into())
            })
            .await;
            match result {
                Ok(summary) => context.mark_succeeded(Some(summary)).map_err(string_error),
                Err(error) => Err(error.to_string()),
            }
        })
    }
}

struct SalvageWorkflow;
impl WorkflowExecutor for SalvageWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: SalvageIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: ControllerWorkflowCheckpoint =
                context.checkpoint().map_err(string_error)?;
            let system = resolve_location_system(&client, &intent.location).await?;
            let controller = resolve_controller(
                &client,
                checkpoint.controller.as_deref().or(intent.controller.as_deref()),
                DeviceType::MiningController,
                Some(&system),
            )
            .await?;
            checkpoint.controller = Some(controller.clone());
            claim_device(context, &controller)?;
            claim_target(context, "salvage-site", &intent.location)?;
            context.advance_to("configuring", &checkpoint).map_err(string_error)?;

            let mining = client
                .devices()
                .get(&controller)
                .await
                .map_err(string_error)?
                .as_mining_controller()
                .map_err(string_error)?;
            if !checkpoint.directive_set {
                let operation = mining
                    .set_directive(MiningDirective::GatherSalvage {
                        location: intent.location.clone(),
                        recall: intent.recall,
                    })
                    .await
                    .map_err(string_error)?;
                await_success(&operation).await?;
                checkpoint.directive_set = true;
                context.persist_checkpoint(&checkpoint).map_err(string_error)?;
            }
            if !checkpoint.launched {
                context.advance_to("launching", &checkpoint).map_err(string_error)?;
                let operation = mining.launch().await.map_err(string_error)?;
                await_success(&operation).await?;
                checkpoint.launched = true;
                context.persist_checkpoint(&checkpoint).map_err(string_error)?;
            }
            if !wait_controller_completion(context, &client, &controller, &mut checkpoint).await? {
                return Ok(());
            }
            context
                .mark_succeeded(Some(serde_json::json!({
                    "controller": controller,
                    "location": intent.location,
                    "directive": "gather_salvage",
                })))
                .map_err(string_error)
        })
    }
}

struct MiningDeployWorkflow;
impl WorkflowExecutor for MiningDeployWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: MiningDeployIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: MiningDeployCheckpoint = context.checkpoint().map_err(string_error)?;
            let replicant = match checkpoint.replicant.clone() {
                Some(value) => value,
                None => resolve_replicant(&client, intent.replicant.as_deref()).await?,
            };
            let hub = match checkpoint.hub.clone() {
                Some(value) => value,
                None => resolve_home(&client, intent.hub.as_deref()).await?,
            };
            checkpoint.replicant = Some(replicant.clone());
            checkpoint.hub = Some(hub.clone());
            claim(context, ResourceKey::Replicant(replicant.clone()))?;
            claim_target(context, "mining-target", &intent.system)?;
            claim_target(context, "location", &hub)?;

            let plan_file = scratch_file(context.id(), "mining-plan.json")?;
            materialize_json(&plan_file, checkpoint.plan_json.as_deref())?;
            checkpoint.started = true;
            context.advance_to("deploying", &checkpoint).map_err(string_error)?;
            let request = MiningExpansionRequest {
                systems: vec![intent.system],
                replicant,
                hub,
                mission_file: plan_file.clone(),
                wait_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
                max_concurrency: 1,
            };
            let execution = execute_expansion(&client, &request);
            tokio::pin!(execution);
            let mut checkpoint_interval = tokio::time::interval(Duration::from_secs(2));
            let result = loop {
                tokio::select! {
                    result = &mut execution => break result,
                    _ = checkpoint_interval.tick() => {
                        match context.control_request().map_err(string_error)? {
                            replicant_workflow::ControlRequest::Continue => {}
                            replicant_workflow::ControlRequest::Pause
                            | replicant_workflow::ControlRequest::Cancel => return Ok(()),
                        }
                        if plan_file.exists() {
                            checkpoint.plan_json = Some(read_json(&plan_file)?);
                            context.persist_checkpoint(&checkpoint).map_err(string_error)?;
                        }
                    }
                }
            };
            if plan_file.exists() {
                checkpoint.plan_json = Some(read_json(&plan_file)?);
                context.persist_checkpoint(&checkpoint).map_err(string_error)?;
            }
            match result {
                Ok(report) => context.mark_succeeded(Some(report)).map_err(string_error),
                Err(error) => Err(error.to_string()),
            }
        })
    }
}

struct LogisticsWorkflow;
impl WorkflowExecutor for LogisticsWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: LogisticsIntent = context.config().map_err(string_error)?;
            if intent.quantity <= 0 && !matches!(intent.payload_kind, LogisticsPayloadKind::Tag) {
                return Err("logistics quantity must be greater than zero".to_owned());
            }
            let client = managed_client(context)?;
            let mut checkpoint: LogisticsWorkflowCheckpoint =
                context.checkpoint().map_err(string_error)?;
            let request = delivery_request(&intent);
            let plan = if let Some(plan) = checkpoint.plan.clone() {
                plan
            } else {
                context.advance_to("planning", &checkpoint).map_err(string_error)?;
                let plan = plan_delivery(&client, &request).await.map_err(string_error)?;
                for code in plan
                    .cargo_transports
                    .iter()
                    .chain(plan.device_carriers.iter())
                    .chain(plan.payload_devices.iter().map(|device| &device.code))
                {
                    claim_device(context, code)?;
                }
                checkpoint.plan = Some(plan.clone());
                context.persist_checkpoint(&checkpoint).map_err(string_error)?;
                plan
            };
            checkpoint.started = true;
            context.advance_to("delivering", &checkpoint).map_err(string_error)?;
            let options = DeliveryOptions {
                return_transports: intent.return_transports,
                ..DeliveryOptions::default()
            };
            let report = execute_delivery(&client, &plan, options)
                .await
                .map_err(string_error)?;
            context.mark_succeeded(Some(report)).map_err(string_error)
        })
    }
}

struct ExplorationWorkflow;
impl WorkflowExecutor for ExplorationWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: ExplorationIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: ExplorationWorkflowCheckpoint =
                context.checkpoint().map_err(string_error)?;
            let replicant = match checkpoint.replicant.clone() {
                Some(value) => value,
                None => resolve_replicant(&client, intent.replicant.as_deref()).await?,
            };
            let hub = match checkpoint.hub.clone() {
                Some(value) => value,
                None => resolve_home(&client, intent.hub.as_deref()).await?,
            };
            checkpoint.replicant = Some(replicant.clone());
            checkpoint.hub = Some(hub.clone());
            claim(context, ResourceKey::Replicant(replicant.clone()))?;
            claim_target(context, "location", &hub)?;
            claim_target(context, "exploration-target", &intent.target)?;

            let plan_file = scratch_file(context.id(), "relay-plan.json")?;
            if let Some(state) = checkpoint.state.as_ref() {
                restore_relay_checkpoint(&plan_file, state).map_err(string_error)?;
            } else {
                clear_scratch_file(&plan_file)?;
            }
            context.advance_to("exploring", &checkpoint).map_err(string_error)?;
            let request = RelayExpansionRequest {
                replicant,
                hub,
                targets: vec![intent.target.clone()],
                mission_file: plan_file,
                max_hop_ly: 7.499,
                wait_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
            };
            let result = execute_relay_workflow(&client, &request, |state| {
                let (replicant, devices, factories) = state.resources();
                claim(context, ResourceKey::Replicant(replicant.to_owned()))?;
                for device in devices {
                    claim_device(context, device)?;
                }
                for factory in factories {
                    claim(context, ResourceKey::Autofactory(factory.to_owned()))?;
                }
                checkpoint.state = Some(state.clone());
                context
                    .advance_to(state.step_name(), &checkpoint)
                    .map_err(|error| error.to_string().into())
            })
            .await;
            match result {
                Ok(report) => context.mark_succeeded(Some(report)).map_err(string_error),
                Err(error) => Err(error.to_string()),
            }
        })
    }
}

struct EventDeliveryWorkflow;
impl WorkflowExecutor for EventDeliveryWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: EventIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: EventDeliveryCheckpoint =
                context.checkpoint().map_err(string_error)?;
            claim_target(context, "event-delivery", &intent.event)?;
            let replicant = match checkpoint.replicant.clone() {
                Some(value) => value,
                None => resolve_replicant(&client, intent.replicant.as_deref()).await?,
            };
            let home = match checkpoint.home.clone() {
                Some(value) => value,
                None => resolve_home(&client, intent.home.as_deref()).await?,
            };
            checkpoint.replicant = Some(replicant.clone());
            checkpoint.home = Some(home.clone());
            let plan_file = scratch_file(context.id(), "event-plan.json")?;
            materialize_json(&plan_file, checkpoint.plan_json.as_deref())?;
            if checkpoint.plan_json.is_none() {
                context.advance_to("planning", &checkpoint).map_err(string_error)?;
                plan_event_mission(
                    &client,
                    &EventPlanningRequest {
                        event: intent.event.clone(),
                        criterion: intent.criterion.clone(),
                        replicant,
                        home,
                        plan_file: plan_file.clone(),
                        replace_plan: true,
                    },
                )
                .await
                .map_err(string_error)?;
                checkpoint.plan_json = Some(read_json(&plan_file)?);
                context.persist_checkpoint(&checkpoint).map_err(string_error)?;
            }

            loop {
                materialize_json(&plan_file, checkpoint.plan_json.as_deref())?;
                context.advance_to("staging", &checkpoint).map_err(string_error)?;
                let result = prestage_event_mission(
                    &client,
                    &EventExecutionRequest::new(
                        plan_file.clone(),
                        Duration::from_secs(DEFAULT_WAIT_SECONDS),
                    ),
                )
                .await
                .map_err(string_error)?;
                checkpoint.plan_json = Some(read_json(&plan_file)?);
                checkpoint.ready = result.ready;
                context.persist_checkpoint(&checkpoint).map_err(string_error)?;
                if result.ready {
                    return context
                        .mark_succeeded(Some(serde_json::json!({
                            "event": intent.event,
                            "ready": true,
                            "state": result.state,
                        })))
                        .map_err(string_error);
                }
                match context.control_request().map_err(string_error)? {
                    replicant_workflow::ControlRequest::Continue => {}
                    replicant_workflow::ControlRequest::Pause => return Ok(()),
                    replicant_workflow::ControlRequest::Cancel => return Ok(()),
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        })
    }
}

struct EventTourWorkflow;
impl WorkflowExecutor for EventTourWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: EventIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            let mut checkpoint: EventTourCheckpoint = context.checkpoint().map_err(string_error)?;
            claim_target(context, "event-tour", &intent.event)?;

            let child_id = match checkpoint.delivery_child {
                Some(id) => id,
                None => {
                    let existing = find_event_delivery(context, &intent.event)?;
                    let child = match existing {
                        Some(child) => child,
                        None => context
                            .create_child(new_event_delivery_workflow(intent.clone()))
                            .map_err(string_error)?,
                    };
                    checkpoint.delivery_child = Some(child.id);
                    context.persist_checkpoint(&checkpoint).map_err(string_error)?;
                    child.id
                }
            };

            let child = loop {
                match context.control_request().map_err(string_error)? {
                    replicant_workflow::ControlRequest::Continue => {}
                    replicant_workflow::ControlRequest::Pause
                    | replicant_workflow::ControlRequest::Cancel => return Ok(()),
                }
                let Some(child) = context.repository().read(child_id).map_err(string_error)? else {
                    return Err(format!("event delivery child {child_id} disappeared"));
                };
                match child.status {
                    WorkflowStatus::Succeeded => break child,
                    WorkflowStatus::Failed | WorkflowStatus::Cancelled => {
                        return Err(format!(
                            "event delivery child {child_id} ended as {:?}: {}",
                            child.status,
                            child.last_error.unwrap_or_default()
                        ));
                    }
                    _ => {
                        context
                            .advance_to("awaiting_delivery", &checkpoint)
                            .map_err(string_error)?;
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            };
            let delivery: EventDeliveryCheckpoint = child.checkpoint().map_err(string_error)?;
            let plan_json = delivery
                .plan_json
                .ok_or_else(|| "completed event delivery child has no plan checkpoint".to_owned())?;
            let replicant = delivery
                .replicant
                .or(intent.replicant.clone())
                .ok_or_else(|| "event tour could not resolve a replicant".to_owned())?;
            checkpoint.replicant = Some(replicant.clone());
            checkpoint.plan_json = Some(plan_json.clone());
            claim(context, ResourceKey::Replicant(replicant))?;
            context.advance_to("resolving", &checkpoint).map_err(string_error)?;
            let plan_file = scratch_file(context.id(), "event-plan.json")?;
            materialize_json(&plan_file, Some(&plan_json))?;
            let state = execute_event_mission(
                &client,
                &EventExecutionRequest::new(
                    plan_file.clone(),
                    Duration::from_secs(DEFAULT_WAIT_SECONDS),
                ),
            )
            .await
            .map_err(string_error)?;
            checkpoint.plan_json = Some(read_json(&plan_file)?);
            context.persist_checkpoint(&checkpoint).map_err(string_error)?;
            context.mark_succeeded(Some(state)).map_err(string_error)
        })
    }
}

struct ObservatoryWorkflow;
impl WorkflowExecutor for ObservatoryWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let intent: ObservatoryIntent = context.config().map_err(string_error)?;
            let client = managed_client(context)?;
            if let Some(observatory) = intent.observatory.as_deref() {
                claim_device(context, observatory)?;
            }
            context.advance_to("prospecting", &Value::Null).map_err(string_error)?;
            let report = auto_prospect(&client, intent.observatory.as_deref())
                .await
                .map_err(string_error)?;
            context.mark_succeeded(Some(report)).map_err(string_error)
        })
    }
}

/// Creates a queued system-scan workflow.
pub fn new_scan_system_workflow(intent: ScanIntent) -> NewWorkflow<ScanIntent, ControllerWorkflowCheckpoint> {
    NewWorkflow::new(scan_system_workflow_kind(), SCHEMA_VERSION, intent, ControllerWorkflowCheckpoint::default())
}

/// Creates a queued belt-search workflow.
pub fn new_scan_belt_workflow(intent: ScanIntent) -> NewWorkflow<ScanIntent, ControllerWorkflowCheckpoint> {
    NewWorkflow::new(scan_belt_workflow_kind(), SCHEMA_VERSION, intent, ControllerWorkflowCheckpoint::default())
}

/// Creates a queued bounded survey-tour workflow.
pub fn new_scan_tour_workflow(intent: ScanTourIntent) -> NewWorkflow<ScanTourIntent, ScanTourCheckpoint> {
    NewWorkflow::new(scan_tour_workflow_kind(), SCHEMA_VERSION, intent, ScanTourCheckpoint::default())
}

/// Creates a queued salvage workflow.
pub fn new_salvage_workflow(intent: SalvageIntent) -> NewWorkflow<SalvageIntent, ControllerWorkflowCheckpoint> {
    NewWorkflow::new(salvage_workflow_kind(), SCHEMA_VERSION, intent, ControllerWorkflowCheckpoint::default())
}

/// Creates a queued one-system mining deployment workflow.
pub fn new_mining_deploy_workflow(intent: MiningDeployIntent) -> NewWorkflow<MiningDeployIntent, MiningDeployCheckpoint> {
    NewWorkflow::new(mining_deploy_workflow_kind(), SCHEMA_VERSION, intent, MiningDeployCheckpoint::default())
}

/// Creates a queued logistics workflow.
pub fn new_logistics_workflow(intent: LogisticsIntent) -> NewWorkflow<LogisticsIntent, LogisticsWorkflowCheckpoint> {
    NewWorkflow::new(logistics_workflow_kind(), SCHEMA_VERSION, intent, LogisticsWorkflowCheckpoint::default())
}

/// Creates a queued directed exploration workflow.
pub fn new_exploration_workflow(intent: ExplorationIntent) -> NewWorkflow<ExplorationIntent, ExplorationWorkflowCheckpoint> {
    NewWorkflow::new(exploration_workflow_kind(), SCHEMA_VERSION, intent, ExplorationWorkflowCheckpoint::default())
}

/// Creates a queued event-delivery workflow.
pub fn new_event_delivery_workflow(intent: EventIntent) -> NewWorkflow<EventIntent, EventDeliveryCheckpoint> {
    NewWorkflow::new(event_delivery_workflow_kind(), SCHEMA_VERSION, intent, EventDeliveryCheckpoint::default())
}

/// Creates a queued event-tour workflow.
pub fn new_event_tour_workflow(intent: EventIntent) -> NewWorkflow<EventIntent, EventTourCheckpoint> {
    NewWorkflow::new(event_tour_workflow_kind(), SCHEMA_VERSION, intent, EventTourCheckpoint::default())
}

/// Creates a queued observatory prospect workflow.
pub fn new_observatory_workflow(intent: ObservatoryIntent) -> NewWorkflow<ObservatoryIntent, Value> {
    NewWorkflow::new(observatory_workflow_kind(), SCHEMA_VERSION, intent, Value::Null)
}

fn default_tour_radius() -> f64 {
    30.0
}

fn default_tour_limit() -> usize {
    80
}

fn default_true() -> bool {
    true
}

fn default_quantity() -> i64 {
    1
}

fn event_delivery_matches(
    workflow: &replicant_workflow::WorkflowInstance,
    kind: &replicant_workflow::WorkflowKind,
    event: &str,
) -> bool {
    if &workflow.kind != kind
        || matches!(
            workflow.status,
            WorkflowStatus::Failed | WorkflowStatus::Cancelled
        )
    {
        return false;
    }
    workflow
        .config::<EventIntent>()
        .is_ok_and(|intent| intent.event == event)
}

fn find_event_delivery(
    context: &WorkflowContext,
    event: &str,
) -> Result<Option<replicant_workflow::WorkflowInstance>, String> {
    let kind = event_delivery_workflow_kind();

    if let Some(child) = context
        .child_workflows()
        .map_err(string_error)?
        .into_iter()
        .filter(|workflow| event_delivery_matches(workflow, &kind, event))
        .max_by_key(|workflow| workflow.created_at)
    {
        return Ok(Some(child));
    }

    let existing = context
        .repository()
        .list()
        .map_err(string_error)?
        .into_iter()
        .filter(|workflow| event_delivery_matches(workflow, &kind, event))
        .max_by_key(|workflow| workflow.created_at);
    Ok(existing)
}

fn managed_client(context: &WorkflowContext) -> Result<Client, String> {
    context
        .managed_client()
        .cloned()
        .ok_or_else(|| "automation workflow requires a managed client".to_owned())
}

fn string_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}

fn claim(context: &WorkflowContext, key: ResourceKey) -> Result<(), String> {
    match context.acquire_claim(key).map_err(string_error)? {
        ClaimAcquireOutcome::Acquired | ClaimAcquireOutcome::AlreadyOwned => Ok(()),
        ClaimAcquireOutcome::Busy { owner } => Err(format!("resource is already claimed by {owner}")),
    }
}

fn claim_device(context: &WorkflowContext, code: &str) -> Result<(), String> {
    claim(context, ResourceKey::Device(code.to_owned()))
}

fn claim_target(context: &WorkflowContext, namespace: &str, key: &str) -> Result<(), String> {
    claim(
        context,
        ResourceKey::Namespaced {
            namespace: namespace.to_owned(),
            key: key.to_owned(),
        },
    )
}

async fn resolve_survey_assignment(
    client: &Client,
    pinned_replicant: Option<&str>,
    pinned_vessel: Option<&str>,
) -> Result<(String, String), String> {
    if let Some(vessel_code) = pinned_vessel.filter(|value| !value.trim().is_empty()) {
        let handle = client.devices().get(vessel_code).await.map_err(string_error)?;
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if snapshot.access != AccessScope::Owned {
            return Err(format!("racing vessel {vessel_code} is not account-owned"));
        }
        if snapshot
            .device_type
            .as_ref()
            .is_none_or(|device_type| device_type.as_str() != DeviceType::RacingVessel.as_str())
        {
            return Err(format!("{vessel_code} is not a racing vessel"));
        }
        let hosted = snapshot
            .relationships
            .hosting_replicant
            .as_ref()
            .map(|key| key.id.as_str().to_owned())
            .ok_or_else(|| format!("racing vessel {vessel_code} is not hosting a replicant"))?;
        if let Some(expected) = pinned_replicant.filter(|value| !value.trim().is_empty())
            && expected != hosted
        {
            return Err(format!(
                "racing vessel {vessel_code} hosts {hosted}, not requested replicant {expected}"
            ));
        }
        client.replicants().get_owned(&hosted).await.map_err(string_error)?;
        return Ok((hosted, vessel_code.to_owned()));
    }

    let handles = client
        .devices()
        .find()
        .owned()
        .of_type(DeviceType::RacingVessel)
        .collect()
        .await
        .map_err(string_error)?;
    for handle in handles {
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        let Some(hosted) = snapshot.relationships.hosting_replicant.as_ref() else {
            continue;
        };
        if pinned_replicant
            .filter(|value| !value.trim().is_empty())
            .is_some_and(|expected| expected != hosted.id.as_str())
        {
            continue;
        }
        client
            .replicants()
            .get_owned(hosted.id.as_str())
            .await
            .map_err(string_error)?;
        return Ok((
            hosted.id.as_str().to_owned(),
            handle.id().as_str().to_owned(),
        ));
    }
    Err("no owned racing vessel hosting an eligible replicant is available".to_owned())
}

async fn resolve_controller(
    client: &Client,
    pinned: Option<&str>,
    device_type: DeviceType,
    system: Option<&str>,
) -> Result<String, String> {
    if let Some(code) = pinned.filter(|value| !value.trim().is_empty()) {
        let snapshot = client
            .devices()
            .get(code)
            .await
            .map_err(string_error)?
            .snapshot()
            .await
            .map_err(string_error)?;
        if snapshot.access != AccessScope::Owned {
            return Err(format!("controller {code} is not account-owned"));
        }
        if snapshot.device_type.as_ref() != Some(&device_type) {
            return Err(format!(
                "controller {code} is {}, expected {}",
                snapshot
                    .device_type
                    .as_ref()
                    .map_or("unknown", DeviceType::as_str),
                device_type.as_str(),
            ));
        }
        if snapshot.relationships.controlled_devices.is_empty() {
            return Err(format!("controller {code} has no adopted fleet"));
        }
        if let Some(system) = system.filter(|value| !value.is_empty()) {
            let location = snapshot
                .location
                .as_ref()
                .map(|location| location.id.as_str())
                .unwrap_or_default();
            if !designation_in_system(location, system) {
                return Err(format!(
                    "controller {code} is at {location}, outside requested system {system}"
                ));
            }
        }
        return Ok(code.to_owned());
    }
    let mut query = client.devices().find().owned().of_type(device_type).idle();
    if let Some(system) = system.filter(|value| !value.is_empty()) {
        query = query.in_system(system.to_owned());
    }
    let handles = query.collect().await.map_err(string_error)?;
    for handle in handles {
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if !snapshot.relationships.controlled_devices.is_empty() {
            return Ok(handle.id().as_str().to_owned());
        }
    }
    Err("no eligible idle owned controller with an adopted fleet is available in scope".to_owned())
}

async fn resolve_replicant(client: &Client, pinned: Option<&str>) -> Result<String, String> {
    if let Some(code) = pinned.filter(|value| !value.trim().is_empty()) {
        client.replicants().get_owned(code).await.map_err(string_error)?;
        return Ok(code.to_owned());
    }
    client
        .replicants()
        .find()
        .owned()
        .collect()
        .await
        .map_err(string_error)?
        .first()
        .map(|handle| handle.id().as_str().to_owned())
        .ok_or_else(|| "no owned replicant is available".to_owned())
}

async fn resolve_home(client: &Client, pinned: Option<&str>) -> Result<String, String> {
    if let Some(location) = pinned.filter(|value| !value.trim().is_empty()) {
        return Ok(location.to_owned());
    }
    let handles = client
        .devices()
        .find()
        .owned()
        .of_type(DeviceType::Autofactory)
        .collect()
        .await
        .map_err(string_error)?;
    for handle in handles {
        let snapshot = handle.snapshot().await.map_err(string_error)?;
        if let Some(location) = snapshot.location.as_ref() {
            return Ok(location.id.as_str().to_owned());
        }
    }
    Err("no owned autofactory location is available for staging".to_owned())
}

async fn wait_controller_completion(
    context: &mut WorkflowContext,
    client: &Client,
    controller: &str,
    checkpoint: &mut ControllerWorkflowCheckpoint,
) -> Result<bool, String> {
    context.advance_to("running", checkpoint).map_err(string_error)?;
    loop {
        match context.control_request().map_err(string_error)? {
            replicant_workflow::ControlRequest::Continue => {}
            replicant_workflow::ControlRequest::Pause
            | replicant_workflow::ControlRequest::Cancel => return Ok(false),
        }
        let snapshot = client
            .devices()
            .get(controller)
            .await
            .map_err(string_error)?
            .refresh()
            .await
            .map_err(string_error)?
            .snapshot()
            .await
            .map_err(string_error)?;
        let coordinating = snapshot
            .status
            .as_ref()
            .is_some_and(|status| status.as_str() == "coordinating");
        if coordinating {
            if !checkpoint.observed_active || checkpoint.idle_observations != 0 {
                checkpoint.observed_active = true;
                checkpoint.idle_observations = 0;
                context.persist_checkpoint(checkpoint).map_err(string_error)?;
            }
        } else {
            checkpoint.idle_observations = checkpoint.idle_observations.saturating_add(1);
            context.persist_checkpoint(checkpoint).map_err(string_error)?;
            if checkpoint.observed_active || checkpoint.idle_observations >= 6 {
                return Ok(true);
            }
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}

async fn await_success(operation: &Operation) -> Result<(), String> {
    loop {
        let outcome = operation
            .wait_timeout(Duration::from_secs(30))
            .await
            .map_err(string_error)?;
        match outcome.status {
            OperationStatus::Completed | OperationStatus::Accepted => return Ok(()),
            OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed => {
                return Err(format!(
                    "managed operation {} ended as {:?}: {}",
                    operation.id(),
                    outcome.status,
                    outcome.response.unwrap_or(Value::Null)
                ));
            }
            _ => {}
        }
    }
}

fn delivery_request(intent: &LogisticsIntent) -> DeliveryRequest {
    let mut resources = ResourceMap::new();
    let mut devices = Vec::new();
    let mut device_tags = Vec::new();
    match intent.payload_kind {
        LogisticsPayloadKind::Resource => {
            resources.insert(intent.item.clone(), intent.quantity);
        }
        LogisticsPayloadKind::Device => devices.push(DeviceRequest {
            quantity: intent.quantity,
            device_type: intent.item.clone(),
        }),
        LogisticsPayloadKind::Tag => device_tags.push(intent.item.clone()),
    }
    DeliveryRequest {
        origin: intent.origin.clone(),
        destination: intent.destination.clone(),
        resources,
        devices,
        device_tags,
        carrier: None,
    }
}

async fn resolve_location_system(client: &Client, location: &str) -> Result<String, String> {
    let mut catalogue = client.galaxy().catalogue();
    if catalogue.is_empty() {
        client
            .galaxy()
            .refresh_catalogue()
            .await
            .map_err(string_error)?;
        catalogue = client.galaxy().catalogue();
    }
    catalogue
        .iter()
        .map(|star| star.key.id.as_str())
        .filter(|system| designation_in_system(location, system))
        .max_by_key(|system| system.len())
        .map(str::to_owned)
        .ok_or_else(|| format!("{location} does not resolve to a known system"))
}

fn designation_in_system(location: &str, system: &str) -> bool {
    location == system || location.starts_with(&format!("{system}-"))
}

fn scratch_file(workflow_id: WorkflowId, name: &str) -> Result<PathBuf, String> {
    let directory = std::env::temp_dir()
        .join("replicant-client-automation")
        .join(workflow_id.to_string());
    fs::create_dir_all(&directory).map_err(string_error)?;
    Ok(directory.join(name))
}

fn materialize_json(path: &Path, contents: Option<&str>) -> Result<(), String> {
    let Some(contents) = contents else {
        return clear_scratch_file(path);
    };
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(string_error)?;
    }
    fs::write(path, contents).map_err(string_error)
}

fn clear_scratch_file(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn read_json(path: &Path) -> Result<String, String> {
    fs::read_to_string(path).map_err(string_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logistics_intent_builds_resource_delivery_without_executor_plumbing() {
        let request = delivery_request(&LogisticsIntent {
            origin: "SCEPTURUM".to_owned(),
            destination: "TWAFFY-OBJ-1".to_owned(),
            payload_kind: LogisticsPayloadKind::Resource,
            item: "rares".to_owned(),
            quantity: 400,
            return_transports: false,
        });
        assert_eq!(request.resources.get("rares"), Some(&400));
        assert!(request.devices.is_empty());
        assert!(request.device_tags.is_empty());
    }

    #[test]
    fn intent_workflow_kinds_are_goal_oriented() {
        assert_eq!(scan_system_workflow_kind().as_str(), "scan.system");
        assert_eq!(scan_belt_workflow_kind().as_str(), "scan.belt");
        assert_eq!(scan_tour_workflow_kind().as_str(), "scan.tour");
        assert_eq!(salvage_workflow_kind().as_str(), "salvage.site");
        assert_eq!(mining_deploy_workflow_kind().as_str(), "mining.deploy");
        assert_eq!(logistics_workflow_kind().as_str(), "logistics.delivery");
        assert_eq!(exploration_workflow_kind().as_str(), "exploration.frontier");
        assert_eq!(event_delivery_workflow_kind().as_str(), "event.delivery");
        assert_eq!(event_tour_workflow_kind().as_str(), "event.tour");
    }
}
