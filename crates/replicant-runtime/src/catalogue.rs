//! Unified discovery, validation, and invocation for application operations.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use replicant_client::{
    managed::{AutofactoryPrintOptions, Client},
    raw,
};
use replicant_protocol::{
    ActionDescriptor, DescriptorCatalog, EntityKind, MutationRisk, OperationClass, OperationKind,
    ParameterDescriptor, ParameterKind, ParameterOption, ParameterValidation, ReportDescriptor,
    TriggerKind, WorkflowDescriptor,
};
use replicant_workflow::{
    RegistryError, RepositoryError, WorkflowInstance, WorkflowRegistry, WorkflowRepository,
};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::{
    actions::{
        ClearTagsAction, ContributeDevicesAction, TagDevicesAction, clear_tags, contribute_devices,
        tag_devices,
    },
    automation::{
        EventIntent, ExplorationIntent, LogisticsIntent, MiningDeployIntent, ObservatoryIntent,
        SalvageIntent, ScanIntent, ScanTourIntent, event_delivery_workflow_kind,
        event_tour_workflow_kind, exploration_workflow_kind, logistics_workflow_kind,
        mining_deploy_workflow_kind, new_event_delivery_workflow, new_event_tour_workflow,
        new_exploration_workflow, new_logistics_workflow, new_mining_deploy_workflow,
        new_observatory_workflow, new_salvage_workflow, new_scan_belt_workflow,
        new_scan_system_workflow, new_scan_tour_workflow, observatory_workflow_kind,
        salvage_workflow_kind, scan_belt_workflow_kind, scan_system_workflow_kind,
        scan_tour_workflow_kind,
    },
    bootstrap::{BootstrapExecutionRequest, deliver_bootstrap, run_bootstrap, stage_bootstrap},
    observatory::auto_prospect,
    relay::RelayExpansionRequest,
    reports::nearby_belt_report,
    survey::{SurveyMode, SurveyOptions},
    workflows::{
        EventWorkflowConfig, MiningWorkflowConfig, RelayWorkflowConfig, RequirementWorkflowConfig,
        SurveyWorkflowConfig, event_workflow_kind, mining_workflow_kind, new_event_workflow,
        new_mining_workflow, new_relay_workflow, new_requirement_workflow, new_survey_workflow,
        register, relay_workflow_kind, requirement_workflow_kind, survey_workflow_kind,
    },
};

/// Failure while registering, validating, or invoking a catalogue entry.
#[derive(Debug, thiserror::Error)]
pub enum CatalogueError {
    /// A stable kind is not registered for the requested lifecycle class.
    #[error("unknown {class:?} kind `{kind}`")]
    UnknownKind {
        /// Requested lifecycle class.
        class: OperationClass,
        /// Requested stable kind.
        kind: String,
    },
    /// Descriptor or input validation failed.
    #[error("{0}")]
    Invalid(String),
    /// A reusable runtime operation failed.
    #[error("operation failed: {0}")]
    Runtime(String),
    /// Workflow factory registration failed.
    #[error(transparent)]
    Registry(#[from] RegistryError),
    /// Workflow persistence failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}

/// One typed catalogue for reports, finite actions, and durable workflows.
pub struct OperationCatalogue {
    descriptors: DescriptorCatalog,
    workflow_registry: Arc<WorkflowRegistry>,
}

impl OperationCatalogue {
    /// Builds the application catalogue and its connected workflow factory registry.
    pub fn new() -> Result<Self, CatalogueError> {
        let mut workflow_registry = WorkflowRegistry::new();
        register(&mut workflow_registry)?;
        let workflow_registry = Arc::new(workflow_registry);
        let descriptors = descriptors();

        let mut kinds = std::collections::BTreeSet::new();
        for (class, kind) in descriptor_kinds(&descriptors) {
            if !kinds.insert(kind) {
                return Err(CatalogueError::Invalid(format!(
                    "duplicate operation kind `{kind}`"
                )));
            }
            if class == OperationClass::Workflow {
                let workflow_kind = replicant_workflow::WorkflowKind::new(kind.to_owned())
                    .map_err(|error| CatalogueError::Invalid(error.to_string()))?;
                if !workflow_registry.contains(&workflow_kind) {
                    return Err(CatalogueError::Invalid(format!(
                        "workflow descriptor `{kind}` has no factory"
                    )));
                }
            }
        }

        let catalogue = Self {
            descriptors,
            workflow_registry,
        };
        for (class, kind) in descriptor_kinds(&catalogue.descriptors) {
            catalogue.validate(class, kind, BTreeMap::new(), true)?;
        }
        Ok(catalogue)
    }

    /// Frontend-safe descriptor catalogue.
    #[must_use]
    pub fn descriptors(&self) -> &DescriptorCatalog {
        &self.descriptors
    }

    /// Workflow factories used by the durable supervisor.
    #[must_use]
    pub fn workflow_registry(&self) -> Arc<WorkflowRegistry> {
        self.workflow_registry.clone()
    }

    /// Validates a registered invocation without executing it.
    pub fn validate_invocation(
        &self,
        class: OperationClass,
        kind: &str,
        parameters: BTreeMap<String, Value>,
    ) -> Result<(), CatalogueError> {
        let kind = self
            .resolve_kind(class, kind)
            .ok_or_else(|| unknown(class, kind))?;
        self.validate(class, kind, parameters, false).map(drop)
    }

    /// Returns whether an operation applies to an entity context.
    #[must_use]
    pub fn is_applicable(&self, class: OperationClass, kind: &str, entity: &EntityKind) -> bool {
        self.applicable_to(class, kind)
            .is_some_and(|kinds| kinds.contains(entity))
    }

    /// Validates and executes a read-only report through its reusable runtime function.
    pub async fn run_report(
        &self,
        client: &Client,
        kind: &str,
        parameters: BTreeMap<String, Value>,
    ) -> Result<Value, CatalogueError> {
        let kind = self
            .resolve_kind(OperationClass::Report, kind)
            .ok_or_else(|| unknown(OperationClass::Report, kind))?;
        let parameters = self.validate(OperationClass::Report, kind, parameters, false)?;
        match kind {
            "nearby_belts" => serialize(
                nearby_belt_report(client, &decode(parameters)?)
                    .await
                    .map_err(|error| CatalogueError::Runtime(error.to_string()))?,
            ),
            _ => Err(unknown(OperationClass::Report, kind)),
        }
    }

    /// Validates and executes a finite action through its reusable runtime function.
    pub async fn run_action(
        &self,
        client: &Client,
        kind: &str,
        parameters: BTreeMap<String, Value>,
    ) -> Result<Value, CatalogueError> {
        let kind = self
            .resolve_kind(OperationClass::Action, kind)
            .ok_or_else(|| unknown(OperationClass::Action, kind))?;
        let parameters = self.validate(OperationClass::Action, kind, parameters, false)?;
        match kind {
            "clear_tags" => serialize(
                clear_tags(client, &decode::<ClearTagsAction>(parameters)?)
                    .await
                    .map_err(|error| CatalogueError::Runtime(error.to_string()))?,
            ),
            "contribute_devices" => serialize(
                contribute_devices(client, &decode::<ContributeDevicesAction>(parameters)?)
                    .await
                    .map_err(|error| CatalogueError::Runtime(error.to_string()))?,
            ),
            "tag_devices" => serialize(
                tag_devices(client, &decode::<TagDevicesAction>(parameters)?)
                    .await
                    .map_err(|error| CatalogueError::Runtime(error.to_string()))?,
            ),
            "bobnet.send" => {
                let input: BobnetSendAction = decode(parameters)?;
                let channel = normalize_bobnet_channel(&input.channel)?;
                let text = input.text.trim();
                if text.is_empty() {
                    return Err(CatalogueError::Invalid(
                        "message text must not be empty".to_owned(),
                    ));
                }
                managed_operation_value(
                    client
                        .bobnet()
                        .send(&input.replicant, channel, text.to_owned())
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "replicant.travel" => {
                let input: ReplicantTravelAction = decode(parameters)?;
                let handle = client
                    .replicants()
                    .get_owned(&input.replicant)
                    .await
                    .map_err(runtime_error)?;
                let mut travel = handle.travel().to(input.destination);
                if input.route == "direct" {
                    travel = travel.via_direct();
                }
                managed_operation_value(travel.depart().await.map_err(runtime_error)?).await
            }
            "replicant.teleport" => {
                let input: ReplicantTeleportAction = decode(parameters)?;
                let handle = client
                    .replicants()
                    .get_owned(&input.replicant)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .teleport(raw::replicants::TeleportRequest {
                            target: input.target,
                        })
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "replicant.slingshot" => {
                let input: ReplicantSlingshotAction = decode(parameters)?;
                let handle = client
                    .replicants()
                    .get_owned(&input.replicant)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .teleport_via_slingshot(input.slingshot)
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "simulation.start" => {
                let input: SimulationStartAction = decode(parameters)?;
                managed_operation_value(
                    client
                        .simulations()
                        .start(&input.interface, &input.replicant, &input.scenario)
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "simulation.abandon" => {
                let input: SimulationAbandonAction = decode(parameters)?;
                managed_operation_value(
                    client
                        .simulations()
                        .abandon(&input.interface, input.simulation_id)
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "trade.execute" => {
                let input: TradeExecuteAction = decode(parameters)?;
                managed_operation_value(
                    client
                        .trading()
                        .execute(&input.controller, &input.trade_code)
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "trade.create" => {
                let input: TradeCreateAction = decode(parameters)?;
                let request = serde_json::json!({
                    "name": input.name,
                    "stock": input.stock,
                    "criteria": {
                        "resources": json_object(&input.criteria_resources_json)?,
                        "devices": json_object(&input.criteria_devices_json)?,
                    },
                    "rewards": {
                        "resources": json_object(&input.reward_resources_json)?,
                        "devices": json_object(&input.reward_devices_json)?,
                    },
                });
                let request = request.as_object().cloned().ok_or_else(|| {
                    CatalogueError::Invalid("trade request must be an object".to_owned())
                })?;
                managed_operation_value(
                    client
                        .trading()
                        .create(&input.controller, request)
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "trade.delete" => {
                let input: TradeDeleteAction = decode(parameters)?;
                managed_operation_value(
                    client
                        .trading()
                        .delete(&input.controller, &input.trade_code)
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "trade.configure_shop" => {
                let input: TradeShopAction = decode(parameters)?;
                let mut configuration = raw::JsonObject::new();
                configuration.insert("name".to_owned(), Value::String(input.name));
                if let Some(description) =
                    input.description.filter(|value| !value.trim().is_empty())
                {
                    configuration.insert("description".to_owned(), Value::String(description));
                }
                if let Some(announcement) =
                    input.announcement.filter(|value| !value.trim().is_empty())
                {
                    configuration.insert("announcement".to_owned(), Value::String(announcement));
                }
                let handle = client
                    .devices()
                    .get(&input.controller)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .command(raw::devices::DeviceCommand::SetDirective {
                            directive: "trade".to_owned(),
                            configuration: Some(configuration),
                            notify: None,
                        })
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "device.travel" => {
                let input: DeviceTravelAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .command(raw::devices::DeviceCommand::Travel {
                            destination: input.destination,
                            dry_run: None,
                            via: None,
                        })
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "device.lifecycle" => {
                let input: DeviceLifecycleAction = decode(parameters)?;
                let operation = if input.command == "retrieve" {
                    client.devices().retrieve(&input.device).await
                } else {
                    let handle = client
                        .devices()
                        .get(&input.device)
                        .await
                        .map_err(runtime_error)?;
                    let command = match input.command.as_str() {
                        "activate" => raw::devices::DeviceCommand::Activate,
                        "assemble" => raw::devices::DeviceCommand::Assemble,
                        "cancel" => raw::devices::DeviceCommand::Cancel,
                        "clear_queue" => raw::devices::DeviceCommand::ClearQueue,
                        "deactivate" => raw::devices::DeviceCommand::Deactivate,
                        "decommission" => raw::devices::DeviceCommand::Decommission,
                        "deploy" => raw::devices::DeviceCommand::Deploy,
                        "compact" => raw::devices::DeviceCommand::Compact,
                        "unfurl" => raw::devices::DeviceCommand::Unfurl,
                        "launch" => raw::devices::DeviceCommand::Launch,
                        "recall" => raw::devices::DeviceCommand::Recall,
                        "scan" => raw::devices::DeviceCommand::Scan,
                        "search" => raw::devices::DeviceCommand::Search,
                        "system_scan" => raw::devices::DeviceCommand::SystemScan,
                        "withdraw" => raw::devices::DeviceCommand::Withdraw,
                        _ => {
                            return Err(CatalogueError::Invalid(
                                "unsupported device lifecycle command".to_owned(),
                            ));
                        }
                    };
                    handle.command(command).await
                }
                .map_err(runtime_error)?;
                managed_operation_value(operation).await
            }
            "device.stow" => {
                let input: DeviceTargetAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .stow(Some(input.target))
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "device.attach" => {
                let input: DeviceTargetAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .attach(raw::devices::TargetsCommand {
                            target: Some(input.target),
                            ..raw::devices::TargetsCommand::default()
                        })
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "device.detach" => {
                let input: DeviceTargetAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .command(raw::devices::DeviceCommand::Detach(
                            raw::devices::TargetsCommand {
                                target: Some(input.target),
                                ..raw::devices::TargetsCommand::default()
                            },
                        ))
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "device.repair" => {
                let input: DeviceTargetAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(handle.repair(input.target).await.map_err(runtime_error)?)
                    .await
            }
            "device.change_owner" => {
                let input: DeviceTargetAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .change_owner(input.target)
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "autofactory.print" => {
                let input: AutofactoryPrintAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                let mut options = AutofactoryPrintOptions::new(input.quantity);
                if input.flatpack {
                    options = options.flatpacked();
                }
                let tags = input
                    .tags
                    .as_deref()
                    .unwrap_or_default()
                    .split(',')
                    .map(str::trim)
                    .filter(|tag| !tag.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                if !tags.is_empty() {
                    options = options.tags(tags);
                }
                managed_operation_value(
                    handle
                        .enqueue_print_configured(input.device_type, options)
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "observatory.auto_prospect" => {
                let input: ObservatoryAutoProspectAction = decode(parameters)?;
                serialize(
                    auto_prospect(client, input.device.as_deref())
                        .await
                        .map_err(|error| CatalogueError::Runtime(error.to_string()))?,
                )
            }
            "observatory.prospect" => {
                let input: ObservatoryProspectAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(handle.prospect(None).await.map_err(runtime_error)?).await
            }
            "observatory.prospect_direction" => {
                let input: ObservatoryProspectDirectionAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .prospect(Some([input.x, input.y, input.z]))
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "observatory.triangulate" => {
                let input: ObservatoryTriangulateAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .triangulate(input.signature, [input.x, input.y, input.z])
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "clone.stow_target" => {
                let input: CloneStowTargetAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.matrix)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .command(raw::devices::DeviceCommand::Stow {
                            target: Some(input.cradle),
                        })
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "clone.replicate" => {
                let input: CloneReplicateAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.source)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .command(raw::devices::DeviceCommand::Replicate {
                            target: input.target,
                            name: input.name,
                        })
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "hub.set_entry_point" => {
                let input: HubDeviceAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .command(raw::devices::DeviceCommand::SetEntryPoint)
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "hub.set_welcome_message" => {
                let input: HubWelcomeAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .command(raw::devices::DeviceCommand::SetWelcomeMessage {
                            message: input.message,
                        })
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "hub.rename" => {
                let input: HubRenameAction = decode(parameters)?;
                let handle = client
                    .devices()
                    .get(&input.device)
                    .await
                    .map_err(runtime_error)?;
                managed_operation_value(
                    handle
                        .command(raw::devices::DeviceCommand::Rename {
                            designation: input.designation,
                            name: input.name,
                        })
                        .await
                        .map_err(runtime_error)?,
                )
                .await
            }
            "bootstrap.stage" => serialize(
                stage_bootstrap(client, &decode::<BootstrapStart>(parameters)?.request())
                    .await
                    .map_err(|error| CatalogueError::Runtime(error.to_string()))?,
            ),
            "bootstrap.deliver" => serialize(
                deliver_bootstrap(client, &decode::<BootstrapStart>(parameters)?.request())
                    .await
                    .map_err(|error| CatalogueError::Runtime(error.to_string()))?,
            ),
            "bootstrap.run" => serialize(
                run_bootstrap(client, &decode::<BootstrapStart>(parameters)?.request())
                    .await
                    .map_err(|error| CatalogueError::Runtime(error.to_string()))?,
            ),
            _ => Err(unknown(OperationClass::Action, kind)),
        }
    }

    /// Validates and persists a queued durable workflow through its registered factory kind.
    pub fn create_workflow(
        &self,
        repository: &WorkflowRepository,
        kind: &str,
        parameters: BTreeMap<String, Value>,
    ) -> Result<WorkflowInstance, CatalogueError> {
        self.create_workflow_with_parent(repository, kind, parameters, None)
    }

    /// Validates and persists a workflow linked to a parent orchestration.
    pub fn create_workflow_with_parent(
        &self,
        repository: &WorkflowRepository,
        kind: &str,
        parameters: BTreeMap<String, Value>,
        parent_id: Option<replicant_workflow::WorkflowId>,
    ) -> Result<WorkflowInstance, CatalogueError> {
        let kind = self
            .resolve_kind(OperationClass::Workflow, kind)
            .ok_or_else(|| unknown(OperationClass::Workflow, kind))?;
        let parameters = self.validate(OperationClass::Workflow, kind, parameters, false)?;
        match kind {
            "scan.system" => {
                let mut workflow = new_scan_system_workflow(decode::<ScanIntent>(parameters)?);
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "scan.belt" => {
                let mut workflow = new_scan_belt_workflow(decode::<ScanIntent>(parameters)?);
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "scan.tour" => {
                let mut workflow = new_scan_tour_workflow(decode::<ScanTourIntent>(parameters)?);
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "salvage.site" => {
                let mut workflow = new_salvage_workflow(decode::<SalvageIntent>(parameters)?);
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "mining.deploy" => {
                let mut workflow =
                    new_mining_deploy_workflow(decode::<MiningDeployIntent>(parameters)?);
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "logistics.delivery" => {
                let mut workflow = new_logistics_workflow(decode::<LogisticsIntent>(parameters)?);
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "exploration.frontier" => {
                let mut workflow =
                    new_exploration_workflow(decode::<ExplorationIntent>(parameters)?);
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "event.delivery" => {
                let mut workflow = new_event_delivery_workflow(decode::<EventIntent>(parameters)?);
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "event.tour" => {
                let mut workflow = new_event_tour_workflow(decode::<EventIntent>(parameters)?);
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "observatory.search" => {
                let mut workflow =
                    new_observatory_workflow(decode::<ObservatoryIntent>(parameters)?);
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "survey.route" => {
                let parameters: SurveyStart = decode(parameters)?;
                let mut workflow = new_survey_workflow(SurveyWorkflowConfig {
                    options: parameters.into_options(),
                });
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "relay.expansion" => {
                let parameters: RelayStart = decode(parameters)?;
                let mut workflow = new_relay_workflow(RelayWorkflowConfig {
                    request: parameters.into_request(),
                });
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "mining.expansion" => {
                let parameters: MiningStart = decode(parameters)?;
                let mut workflow = new_mining_workflow(MiningWorkflowConfig {
                    systems: csv(parameters.systems_csv),
                    replicant: parameters.replicant,
                    hub: parameters.hub,
                    mission_file: parameters.mission_file,
                    wait_timeout_seconds: parameters.wait_timeout_seconds,
                    max_concurrency: parameters.max_concurrency,
                });
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "event.fulfillment" => {
                let parameters: EventStart = decode(parameters)?;
                let mut workflow = new_event_workflow(EventWorkflowConfig {
                    event: parameters.event,
                    criterion: parameters.criterion,
                    replicant: parameters.replicant,
                    home: parameters.home,
                    plan_file: parameters.plan_file,
                    replace_plan: parameters.replace_plan,
                    wait_timeout_seconds: parameters.wait_timeout_seconds,
                });
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            "requirement.fulfillment" => {
                let parameters: RequirementStart = decode(parameters)?;
                let requirement = serde_json::from_str(&parameters.requirement_json)
                    .map_err(|error| CatalogueError::Invalid(error.to_string()))?;
                let mut workflow =
                    new_requirement_workflow(RequirementWorkflowConfig { requirement });
                workflow.parent_id = parent_id;
                Ok(repository.create(workflow)?)
            }
            _ => Err(unknown(OperationClass::Workflow, kind)),
        }
    }

    fn resolve_kind<'a>(&'a self, class: OperationClass, kind: &str) -> Option<&'a str> {
        match class {
            OperationClass::Report => self
                .descriptors
                .reports
                .iter()
                .find(|item| item.kind.0 == kind || item.aliases.iter().any(|alias| alias == kind))
                .map(|item| item.kind.0.as_str()),
            OperationClass::Action => self
                .descriptors
                .actions
                .iter()
                .find(|item| item.kind.0 == kind || item.aliases.iter().any(|alias| alias == kind))
                .map(|item| item.kind.0.as_str()),
            OperationClass::Workflow => self
                .descriptors
                .workflows
                .iter()
                .find(|item| item.kind.0 == kind || item.aliases.iter().any(|alias| alias == kind))
                .map(|item| item.kind.0.as_str()),
        }
    }

    fn applicable_to(&self, class: OperationClass, kind: &str) -> Option<&[EntityKind]> {
        match class {
            OperationClass::Report => self
                .descriptors
                .reports
                .iter()
                .find(|item| item.kind.0 == kind)
                .map(|item| item.applicable_to.as_slice()),
            OperationClass::Action => self
                .descriptors
                .actions
                .iter()
                .find(|item| item.kind.0 == kind)
                .map(|item| item.applicable_to.as_slice()),
            OperationClass::Workflow => self
                .descriptors
                .workflows
                .iter()
                .find(|item| item.kind.0 == kind)
                .map(|item| item.applicable_to.as_slice()),
        }
    }

    fn parameters(&self, class: OperationClass, kind: &str) -> Option<&[ParameterDescriptor]> {
        match class {
            OperationClass::Report => self
                .descriptors
                .reports
                .iter()
                .find(|item| item.kind.0 == kind)
                .map(|item| item.parameters.as_slice()),
            OperationClass::Action => self
                .descriptors
                .actions
                .iter()
                .find(|item| item.kind.0 == kind)
                .map(|item| item.parameters.as_slice()),
            OperationClass::Workflow => self
                .descriptors
                .workflows
                .iter()
                .find(|item| item.kind.0 == kind)
                .map(|item| item.parameters.as_slice()),
        }
    }

    /// Checks that an action kind exists and its parameters are well formed,
    /// without running it.
    ///
    /// Lets callers that execute actions asynchronously still reject bad
    /// requests synchronously.
    pub fn validate_action(
        &self,
        kind: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<(), CatalogueError> {
        let kind = self
            .resolve_kind(OperationClass::Action, kind)
            .ok_or_else(|| unknown(OperationClass::Action, kind))?;
        self.validate(OperationClass::Action, kind, parameters.clone(), false)
            .map(drop)
    }

    fn validate(
        &self,
        class: OperationClass,
        kind: &str,
        mut values: BTreeMap<String, Value>,
        defaults_only: bool,
    ) -> Result<BTreeMap<String, Value>, CatalogueError> {
        let parameters = self
            .parameters(class, kind)
            .ok_or_else(|| unknown(class, kind))?;
        if class == OperationClass::Workflow && kind == logistics_workflow_kind().as_str() {
            return validate_logistics_workflow_parameters(parameters, values, defaults_only);
        }
        if let Some(name) = values
            .keys()
            .find(|name| !parameters.iter().any(|item| item.name == name.as_str()))
        {
            return Err(CatalogueError::Invalid(format!(
                "unknown parameter `{name}` for `{kind}`"
            )));
        }
        for parameter in parameters {
            if !values.contains_key(&parameter.name)
                && let Some(default) = &parameter.default
            {
                values.insert(parameter.name.clone(), default.clone());
            }
            let Some(value) = values.get(&parameter.name) else {
                if parameter.required && !defaults_only {
                    return Err(CatalogueError::Invalid(format!(
                        "missing required parameter `{}`",
                        parameter.name
                    )));
                }
                continue;
            };
            validate_parameter(parameter, value)?;
        }
        Ok(values)
    }
}

fn validate_logistics_workflow_parameters(
    parameters: &[ParameterDescriptor],
    mut values: BTreeMap<String, Value>,
    defaults_only: bool,
) -> Result<BTreeMap<String, Value>, CatalogueError> {
    const MANIFEST_FIELDS: [&str; 3] = ["resources", "devices", "device_tags"];
    if let Some(name) = values.keys().find(|name| {
        !parameters.iter().any(|item| item.name == name.as_str())
            && !MANIFEST_FIELDS.contains(&name.as_str())
    }) {
        return Err(CatalogueError::Invalid(format!(
            "unknown parameter `{name}` for `logistics.delivery`"
        )));
    }
    for parameter in parameters {
        if !values.contains_key(&parameter.name)
            && let Some(default) = &parameter.default
        {
            values.insert(parameter.name.clone(), default.clone());
        }
        let Some(value) = values.get(&parameter.name) else {
            if parameter.required && !defaults_only {
                return Err(CatalogueError::Invalid(format!(
                    "missing required parameter `{}`",
                    parameter.name
                )));
            }
            continue;
        };
        validate_parameter(parameter, value)?;
    }
    if let Some(resources) = values.get("resources") {
        let resources = resources.as_object().ok_or_else(|| {
            CatalogueError::Invalid("parameter `resources` must be an object".to_owned())
        })?;
        if resources.values().any(|quantity| {
            quantity
                .as_i64()
                .or_else(|| {
                    quantity
                        .as_u64()
                        .and_then(|value| i64::try_from(value).ok())
                })
                .is_none_or(|quantity| quantity <= 0)
        }) {
            return Err(CatalogueError::Invalid(
                "resource quantities must be positive integers".to_owned(),
            ));
        }
    }
    if let Some(devices) = values.get("devices") {
        let devices = devices.as_array().ok_or_else(|| {
            CatalogueError::Invalid("parameter `devices` must be an array".to_owned())
        })?;
        for device in devices {
            let device = device.as_object().ok_or_else(|| {
                CatalogueError::Invalid("each device payload must be an object".to_owned())
            })?;
            if device
                .get("device_type")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
                || device
                    .get("quantity")
                    .and_then(Value::as_i64)
                    .is_none_or(|quantity| quantity <= 0)
            {
                return Err(CatalogueError::Invalid(
                    "device payloads require device_type and positive quantity".to_owned(),
                ));
            }
        }
    }
    if let Some(tags) = values.get("device_tags") {
        let tags = tags.as_array().ok_or_else(|| {
            CatalogueError::Invalid("parameter `device_tags` must be an array".to_owned())
        })?;
        if tags
            .iter()
            .any(|tag| tag.as_str().is_none_or(str::is_empty))
        {
            return Err(CatalogueError::Invalid(
                "device tags must be non-empty strings".to_owned(),
            ));
        }
    }
    let has_manifest = values
        .get("resources")
        .and_then(Value::as_object)
        .is_some_and(|items| !items.is_empty())
        || values
            .get("devices")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty())
        || values
            .get("device_tags")
            .and_then(Value::as_array)
            .is_some_and(|items| !items.is_empty());
    let has_legacy = values
        .get("item")
        .and_then(Value::as_str)
        .is_some_and(|item| !item.is_empty());
    if !defaults_only && !has_manifest && !has_legacy {
        return Err(CatalogueError::Invalid(
            "logistics delivery requires at least one payload".to_owned(),
        ));
    }
    Ok(values)
}

fn unknown(class: OperationClass, kind: &str) -> CatalogueError {
    CatalogueError::UnknownKind {
        class,
        kind: kind.to_owned(),
    }
}

fn decode<T: for<'de> Deserialize<'de>>(
    parameters: BTreeMap<String, Value>,
) -> Result<T, CatalogueError> {
    serde_json::from_value(Value::Object(parameters.into_iter().collect::<Map<_, _>>()))
        .map_err(|error| CatalogueError::Invalid(error.to_string()))
}

fn serialize(value: impl serde::Serialize) -> Result<Value, CatalogueError> {
    serde_json::to_value(value).map_err(|error| CatalogueError::Runtime(error.to_string()))
}

fn validate_parameter(
    parameter: &ParameterDescriptor,
    value: &Value,
) -> Result<(), CatalogueError> {
    let valid_type = match parameter.kind {
        ParameterKind::Integer => value.as_i64().is_some() || value.as_u64().is_some(),
        ParameterKind::Number => value.as_f64().is_some_and(f64::is_finite),
        ParameterKind::Boolean => value.is_boolean(),
        ParameterKind::String
        | ParameterKind::Enum
        | ParameterKind::System
        | ParameterKind::Location
        | ParameterKind::Replicant
        | ParameterKind::Device
        | ParameterKind::DeviceType
        | ParameterKind::Tag
        | ParameterKind::Entity { .. } => value.is_string(),
    };
    if !valid_type {
        return Err(CatalogueError::Invalid(format!(
            "parameter `{}` has the wrong type",
            parameter.name
        )));
    }
    if parameter.required && value.as_str().is_some_and(str::is_empty) {
        return Err(CatalogueError::Invalid(format!(
            "parameter `{}` must not be empty",
            parameter.name
        )));
    }
    if matches!(parameter.kind, ParameterKind::Enum)
        && !parameter
            .options
            .iter()
            .any(|option| value.as_str() == Some(&option.value))
    {
        return Err(CatalogueError::Invalid(format!(
            "parameter `{}` is not an allowed value",
            parameter.name
        )));
    }
    if let Some(number) = value.as_f64()
        && (parameter
            .validation
            .minimum
            .is_some_and(|minimum| number < minimum)
            || parameter
                .validation
                .maximum
                .is_some_and(|maximum| number > maximum))
    {
        return Err(CatalogueError::Invalid(format!(
            "parameter `{}` is outside the allowed range",
            parameter.name
        )));
    }
    if let Some(text) = value.as_str() {
        let length = u32::try_from(text.chars().count()).unwrap_or(u32::MAX);
        if parameter
            .validation
            .min_length
            .is_some_and(|minimum| length < minimum)
            || parameter
                .validation
                .max_length
                .is_some_and(|maximum| length > maximum)
        {
            return Err(CatalogueError::Invalid(format!(
                "parameter `{}` has an invalid length",
                parameter.name
            )));
        }
    }
    Ok(())
}

fn descriptors() -> DescriptorCatalog {
    DescriptorCatalog {
        reports: vec![ReportDescriptor {
            kind: operation_kind("nearby_belts"),
            display_name: "Nearby belt report".to_owned(),
            aliases: strings(&["nearby_belt_report", "belts"]),
            description: "Find asteroid belts in explored systems near an origin star.".to_owned(),
            category: "reports".to_owned(),
            operation_class: OperationClass::Report,
            risk: MutationRisk::None,
            applicable_to: vec![EntityKind::System],
            parameters: vec![
                required("origin", "Origin system", ParameterKind::System),
                bounded(
                    defaulted(
                        "radius_ly",
                        "Radius (light years)",
                        ParameterKind::Number,
                        10.0,
                    ),
                    Some(0.0),
                    None,
                ),
                bounded(
                    defaulted(
                        "concurrency",
                        "Refresh concurrency",
                        ParameterKind::Integer,
                        4,
                    ),
                    Some(1.0),
                    Some(16.0),
                ),
            ],
        }],
        actions: vec![
            ActionDescriptor {
                kind: operation_kind("clear_tags"),
                display_name: "Clear device tags".to_owned(),
                aliases: strings(&["clear_tags"]),
                description: "Remove matching tags from owned devices.".to_owned(),
                category: "devices".to_owned(),
                operation_class: OperationClass::Action,
                risk: MutationRisk::Elevated,
                applicable_to: vec![EntityKind::Device],
                parameters: vec![
                    required("tag_prefix", "Tag prefix", ParameterKind::Tag),
                    defaulted("dry_run", "Dry run", ParameterKind::Boolean, false),
                ],
            },
            ActionDescriptor {
                kind: operation_kind("contribute_devices"),
                display_name: "Contribute devices".to_owned(),
                aliases: strings(&["contribute_twaffy_injectors"]),
                description: "Contribute selected owned devices at a destination.".to_owned(),
                category: "devices".to_owned(),
                operation_class: OperationClass::Action,
                risk: MutationRisk::Elevated,
                applicable_to: vec![
                    EntityKind::Device,
                    EntityKind::Location,
                    EntityKind::Replicant,
                ],
                parameters: vec![
                    required("destination", "Destination", ParameterKind::Location),
                    required("device_type", "Device type", ParameterKind::DeviceType),
                    required("owner", "Owner", ParameterKind::Replicant),
                    optional("tag", "Tag", ParameterKind::Tag),
                    bounded(
                        optional("count", "Maximum devices", ParameterKind::Integer),
                        Some(1.0),
                        None,
                    ),
                    defaulted("dry_run", "Dry run", ParameterKind::Boolean, false),
                ],
            },
            ActionDescriptor {
                kind: operation_kind("tag_devices"),
                display_name: "Tag devices".to_owned(),
                aliases: strings(&["tag_twaffy_ring_injectors"]),
                description: "Add one tag to every owned device of a type.".to_owned(),
                category: "devices".to_owned(),
                operation_class: OperationClass::Action,
                risk: MutationRisk::Elevated,
                applicable_to: vec![EntityKind::Device],
                parameters: vec![
                    required("device_type", "Device type", ParameterKind::DeviceType),
                    required("tag", "Tag", ParameterKind::Tag),
                    defaulted("dry_run", "Dry run", ParameterKind::Boolean, false),
                ],
            },
            simple_action(
                "bobnet.send",
                "Send BobNet message",
                "Broadcast a message from an owned replicant to a BobNet channel.",
                "communications",
                MutationRisk::Low,
                vec![EntityKind::Replicant],
                vec![
                    required("replicant", "Replicant", ParameterKind::Replicant),
                    required("channel", "Channel", ParameterKind::String),
                    required("text", "Message", ParameterKind::String),
                ],
            ),
            simple_action(
                "replicant.travel",
                "Travel replicant",
                "Start normal replicant travel to a system or location.",
                "navigation",
                MutationRisk::Elevated,
                vec![
                    EntityKind::Replicant,
                    EntityKind::System,
                    EntityKind::Location,
                ],
                vec![
                    required("replicant", "Replicant", ParameterKind::Replicant),
                    required("destination", "Destination", ParameterKind::Location),
                    enum_parameter("route", "Route", &["auto", "direct"], "auto"),
                ],
            ),
            simple_action(
                "replicant.teleport",
                "Teleport replicant",
                "Teleport a replicant to a target replicant matrix.",
                "navigation",
                MutationRisk::Elevated,
                vec![EntityKind::Replicant, EntityKind::Device],
                vec![
                    required("replicant", "Replicant", ParameterKind::Replicant),
                    required("target", "Target matrix", ParameterKind::Device),
                ],
            ),
            simple_action(
                "replicant.slingshot",
                "Teleport via slingshot",
                "Teleport through an FTL slingshot's configured linked matrix.",
                "navigation",
                MutationRisk::Elevated,
                vec![EntityKind::Replicant, EntityKind::Device],
                vec![
                    required("replicant", "Replicant", ParameterKind::Replicant),
                    required("slingshot", "FTL slingshot", ParameterKind::Device),
                ],
            ),
            simple_action(
                "simulation.start",
                "Start simulation",
                "Enter a scenario through a datacentre replicant interface.",
                "simulations",
                MutationRisk::Elevated,
                vec![EntityKind::Device, EntityKind::Replicant],
                vec![
                    required("interface", "Replicant interface", ParameterKind::Device),
                    required("replicant", "Replicant", ParameterKind::Replicant),
                    required("scenario", "Scenario code", ParameterKind::String),
                ],
            ),
            simple_action(
                "simulation.abandon",
                "Abandon simulation",
                "Abandon a running simulation.",
                "simulations",
                MutationRisk::Elevated,
                vec![EntityKind::Device, EntityKind::Replicant],
                vec![
                    required("interface", "Replicant interface", ParameterKind::Device),
                    bounded(
                        required("simulation_id", "Simulation ID", ParameterKind::Integer),
                        Some(1.0),
                        None,
                    ),
                ],
            ),
            simple_action(
                "trade.execute",
                "Execute trade",
                "Fulfill one unit of a listed trade.",
                "trade",
                MutationRisk::Elevated,
                vec![EntityKind::Device, EntityKind::Replicant],
                vec![
                    required("controller", "Trade controller", ParameterKind::Device),
                    required("trade_code", "Trade code", ParameterKind::String),
                ],
            ),
            simple_action(
                "trade.create",
                "Create trade",
                "Create a stocked trade on an owned trade controller.",
                "trade",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("controller", "Trade controller", ParameterKind::Device),
                    required("name", "Trade name", ParameterKind::String),
                    bounded(
                        required("stock", "Stock", ParameterKind::Integer),
                        Some(1.0),
                        None,
                    ),
                    defaulted(
                        "criteria_resources_json",
                        "Buyer resources JSON",
                        ParameterKind::String,
                        "{}",
                    ),
                    defaulted(
                        "criteria_devices_json",
                        "Buyer devices JSON",
                        ParameterKind::String,
                        "{}",
                    ),
                    defaulted(
                        "reward_resources_json",
                        "Reward resources JSON",
                        ParameterKind::String,
                        "{}",
                    ),
                    defaulted(
                        "reward_devices_json",
                        "Reward devices JSON",
                        ParameterKind::String,
                        "{}",
                    ),
                ],
            ),
            simple_action(
                "trade.delete",
                "Delete trade",
                "Delete an owned trade and release its escrow.",
                "trade",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("controller", "Trade controller", ParameterKind::Device),
                    required("trade_code", "Trade code", ParameterKind::String),
                ],
            ),
            simple_action(
                "trade.configure_shop",
                "Configure shop",
                "Set the public name, description, and BobNet announcement for an owned trade controller.",
                "trade",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("controller", "Trade controller", ParameterKind::Device),
                    required("name", "Shop name", ParameterKind::String),
                    optional("description", "Description", ParameterKind::String),
                    optional("announcement", "BobNet announcement", ParameterKind::String),
                ],
            ),
            simple_action(
                "device.travel",
                "Travel device",
                "Send one travel-capable device to a location or system.",
                "devices",
                MutationRisk::Elevated,
                vec![EntityKind::Device, EntityKind::Location, EntityKind::System],
                vec![
                    required("device", "Device", ParameterKind::Device),
                    required("destination", "Destination", ParameterKind::Location),
                ],
            ),
            simple_action(
                "device.lifecycle",
                "Control device",
                "Run a standard managed lifecycle command on one device.",
                "devices",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("device", "Device", ParameterKind::Device),
                    enum_parameter(
                        "command",
                        "Command",
                        &[
                            "activate",
                            "assemble",
                            "cancel",
                            "clear_queue",
                            "deactivate",
                            "decommission",
                            "deploy",
                            "compact",
                            "unfurl",
                            "launch",
                            "recall",
                            "scan",
                            "search",
                            "system_scan",
                            "withdraw",
                            "retrieve",
                        ],
                        "activate",
                    ),
                ],
            ),
            simple_action(
                "autofactory.print",
                "Print devices",
                "Queue one or more devices on a selected owned Autofactory.",
                "manufacturing",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("device", "Autofactory", ParameterKind::Device),
                    required("device_type", "Device type", ParameterKind::DeviceType),
                    bounded(
                        defaulted("quantity", "Quantity", ParameterKind::Integer, 1),
                        Some(1.0),
                        None,
                    ),
                    optional("tags", "Tags (comma separated)", ParameterKind::String),
                    defaulted("flatpack", "Print compacted", ParameterKind::Boolean, false),
                ],
            ),
            simple_action(
                "device.stow",
                "Stow device",
                "Stow the selected device inside another compatible device.",
                "devices",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("device", "Device", ParameterKind::Device),
                    required("target", "Stow inside", ParameterKind::Device),
                ],
            ),
            simple_action(
                "device.attach",
                "Attach device",
                "Attach another compatible device to the selected host device.",
                "devices",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("device", "Host device", ParameterKind::Device),
                    required("target", "Device to attach", ParameterKind::Device),
                ],
            ),
            simple_action(
                "device.detach",
                "Detach device",
                "Detach one attached device from the selected host device.",
                "devices",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("device", "Host device", ParameterKind::Device),
                    required("target", "Device to detach", ParameterKind::Device),
                ],
            ),
            simple_action(
                "device.repair",
                "Repair device",
                "Use the selected maintenance-capable device to repair a target device.",
                "devices",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("device", "Repair device", ParameterKind::Device),
                    required("target", "Repair target", ParameterKind::Device),
                ],
            ),
            simple_action(
                "device.change_owner",
                "Change device owner",
                "Transfer the selected device to another account or replicant.",
                "devices",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("device", "Device", ParameterKind::Device),
                    required("target", "New owner", ParameterKind::String),
                ],
            ),
            simple_action(
                "observatory.auto_prospect",
                "Auto prospect sparse space",
                "Use the runtime catalogue-density planner to choose an observatory and retry sparse directions.",
                "observatory",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![optional("device", "Observatory", ParameterKind::Device)],
            ),
            simple_action(
                "observatory.prospect",
                "Prospect outward",
                "Start the game server's default outward Galactic Observatory prospect.",
                "observatory",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![required("device", "Observatory", ParameterKind::Device)],
            ),
            simple_action(
                "observatory.prospect_direction",
                "Prospect direction",
                "Start a Galactic Observatory prospect along an explicit direction vector.",
                "observatory",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("device", "Observatory", ParameterKind::Device),
                    required("x", "Direction X", ParameterKind::Number),
                    required("y", "Direction Y", ParameterKind::Number),
                    required("z", "Direction Z", ParameterKind::Number),
                ],
            ),
            simple_action(
                "observatory.triangulate",
                "Triangulate discovery",
                "Triangulate a spectral signature from a target coordinate.",
                "observatory",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("device", "Observatory", ParameterKind::Device),
                    required("signature", "Signature", ParameterKind::String),
                    required("x", "Target X", ParameterKind::Number),
                    required("y", "Target Y", ParameterKind::Number),
                    required("z", "Target Z", ParameterKind::Number),
                ],
            ),
            simple_action(
                "clone.stow_target",
                "Stow clone target",
                "Stow an empty replicant matrix in a cradle device before replication.",
                "replicants",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![
                    required("matrix", "Empty target matrix", ParameterKind::Device),
                    required("cradle", "Cradle device", ParameterKind::Device),
                ],
            ),
            simple_action(
                "clone.replicate",
                "Create replicant",
                "Replicate from the source replicant matrix into a prepared empty target matrix.",
                "replicants",
                MutationRisk::Elevated,
                vec![EntityKind::Device, EntityKind::Replicant],
                vec![
                    required("source", "Source replicant matrix", ParameterKind::Device),
                    required("target", "Empty target matrix", ParameterKind::Device),
                    optional("name", "Replicant name", ParameterKind::String),
                ],
            ),
            simple_action(
                "hub.set_entry_point",
                "Set system entry point",
                "Designate an owned system hub as the interstellar entry point.",
                "claims",
                MutationRisk::Elevated,
                vec![EntityKind::Device],
                vec![required("device", "System hub", ParameterKind::Device)],
            ),
            simple_action(
                "hub.set_welcome_message",
                "Set hub welcome message",
                "Update the welcome message on an owned system hub.",
                "claims",
                MutationRisk::Low,
                vec![EntityKind::Device],
                vec![
                    required("device", "System hub", ParameterKind::Device),
                    optional("message", "Welcome message", ParameterKind::String),
                ],
            ),
            simple_action(
                "hub.rename",
                "Rename system hub",
                "Exercise an owned system hub's naming rights by updating its designation and display name.",
                "claims",
                MutationRisk::Low,
                vec![EntityKind::Device],
                vec![
                    required("device", "System hub", ParameterKind::Device),
                    required("designation", "Designation", ParameterKind::String),
                    required("name", "Display name", ParameterKind::String),
                ],
            ),
            bootstrap_action(
                "bootstrap.stage",
                "Stage bootstrap ark",
                "Resume manufacturing, loading, and source staging from a bootstrap mission file.",
            ),
            bootstrap_action(
                "bootstrap.deliver",
                "Deliver bootstrap ark",
                "Deliver a staged bootstrap ark to its planned landing star.",
            ),
            bootstrap_action(
                "bootstrap.run",
                "Run regional bootstrap",
                "Resume the complete regional bootstrap from its durable mission file.",
            ),
        ],
        workflows: workflow_descriptors(),
    }
}

fn simple_action(
    kind: &str,
    display_name: &str,
    description: &str,
    category: &str,
    risk: MutationRisk,
    applicable_to: Vec<EntityKind>,
    parameters: Vec<ParameterDescriptor>,
) -> ActionDescriptor {
    ActionDescriptor {
        kind: operation_kind(kind),
        display_name: display_name.to_owned(),
        aliases: Vec::new(),
        description: description.to_owned(),
        category: category.to_owned(),
        operation_class: OperationClass::Action,
        risk,
        applicable_to,
        parameters,
    }
}

fn runtime_error(error: replicant_client::Error) -> CatalogueError {
    CatalogueError::Runtime(error.to_string())
}

async fn managed_operation_value(
    operation: replicant_client::managed::Operation,
) -> Result<Value, CatalogueError> {
    let outcome = operation.outcome().await.map_err(runtime_error)?;
    Ok(serde_json::json!({
        "operation_id": operation.id().as_str(),
        "status": format!("{:?}", outcome.status).to_ascii_lowercase(),
        "response": outcome.response,
    }))
}

fn json_object(input: &str) -> Result<raw::JsonObject, CatalogueError> {
    serde_json::from_str::<raw::JsonObject>(input)
        .map_err(|error| CatalogueError::Invalid(format!("invalid JSON object: {error}")))
}

fn workflow_descriptors() -> Vec<WorkflowDescriptor> {
    vec![
        WorkflowDescriptor {
            kind: operation_kind(scan_system_workflow_kind().as_str()),
            display_name: "Survey system".to_owned(),
            aliases: strings(&["system_scanner"]),
            description: "Survey every planet and moon in a system using an automatically selected AMI survey controller.".to_owned(),
            category: "survey".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::System, EntityKind::Location, EntityKind::Device],
            parameters: vec![
                required("system", "System", ParameterKind::System),
                optional("controller", "Survey controller", ParameterKind::Device),
                defaulted("recall", "Recall when complete", ParameterKind::Boolean, true),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(scan_belt_workflow_kind().as_str()),
            display_name: "Search asteroid belt".to_owned(),
            aliases: strings(&["belt_searcher"]),
            description: "Search a system's asteroid belt for resource sites with an automatically selected AMI survey controller.".to_owned(),
            category: "survey".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::System, EntityKind::Location, EntityKind::Device],
            parameters: vec![
                required("system", "System", ParameterKind::System),
                optional("controller", "Survey controller", ParameterKind::Device),
                defaulted("recall", "Recall when complete", ParameterKind::Boolean, true),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(scan_tour_workflow_kind().as_str()),
            display_name: "Survey area".to_owned(),
            aliases: strings(&["survey_area"]),
            description: "Plan and run a bounded survey route with an automatically resolved racing vessel and maintenance home.".to_owned(),
            category: "survey".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::System, EntityKind::Replicant, EntityKind::Device],
            parameters: vec![
                required("center", "Centre system", ParameterKind::System),
                bounded(
                    defaulted("radius_ly", "Radius (ly)", ParameterKind::Number, 30.0),
                    Some(0.0),
                    None,
                ),
                bounded(
                    defaulted("system_limit", "System limit", ParameterKind::Integer, 80),
                    Some(1.0),
                    None,
                ),
                optional("replicant", "Replicant", ParameterKind::Replicant),
                optional("vessel", "Racing vessel", ParameterKind::Device),
                defaulted("include_explored", "Include explored", ParameterKind::Boolean, false),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(salvage_workflow_kind().as_str()),
            display_name: "Salvage site".to_owned(),
            aliases: strings(&["salvage"]),
            description: "Assign an AMI mining controller to deplete a salvage site.".to_owned(),
            category: "mining".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::Location, EntityKind::Device],
            parameters: vec![
                required("location", "Salvage site", ParameterKind::Location),
                optional("controller", "Mining controller", ParameterKind::Device),
                defaulted("recall", "Recall when complete", ParameterKind::Boolean, true),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(mining_deploy_workflow_kind().as_str()),
            display_name: "Deploy mining operation".to_owned(),
            aliases: strings(&["mining_deploy"]),
            description: "Stage and deploy one mining installation while automatically resolving the replicant and manufacturing hub when omitted.".to_owned(),
            category: "mining".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::System, EntityKind::Location, EntityKind::Replicant],
            parameters: vec![
                required("system", "Target system", ParameterKind::System),
                optional("replicant", "Replicant", ParameterKind::Replicant),
                optional("hub", "Manufacturing hub", ParameterKind::Location),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(logistics_workflow_kind().as_str()),
            display_name: "Deliver cargo or devices".to_owned(),
            aliases: strings(&["transport", "cargo", "taxi"]),
            description: "Move one or more resources, device types, or tagged device groups between two locations without exposing a transport plan file.".to_owned(),
            category: "logistics".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::System, EntityKind::Location, EntityKind::Device],
            parameters: vec![
                required("origin", "Origin", ParameterKind::Location),
                required("destination", "Destination", ParameterKind::Location),
                enum_parameter("payload_kind", "Payload", &["resource", "device", "tag"], "resource"),
                optional("item", "Resource, device type, or tag", ParameterKind::String),
                bounded(
                    defaulted("quantity", "Quantity", ParameterKind::Integer, 1),
                    Some(1.0),
                    None,
                ),
                defaulted("return_transports", "Return transports", ParameterKind::Boolean, false),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(exploration_workflow_kind().as_str()),
            display_name: "Explore toward system".to_owned(),
            aliases: strings(&["explore_system"]),
            description: "Extend the relay frontier toward one target system, automatically selecting a replicant and manufacturing hub when they are not pinned.".to_owned(),
            category: "exploration".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::System, EntityKind::Replicant, EntityKind::Location],
            parameters: vec![
                required("target", "Target system", ParameterKind::System),
                optional("replicant", "Replicant", ParameterKind::Replicant),
                optional("hub", "Manufacturing hub", ParameterKind::Location),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(event_delivery_workflow_kind().as_str()),
            display_name: "Prepare event".to_owned(),
            aliases: strings(&["event_delivery"]),
            description: "Manufacture and stage event requirements while leaving the selected replicant free.".to_owned(),
            category: "events".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::System, EntityKind::Location],
            parameters: vec![
                required("event", "Event designation", ParameterKind::String),
                optional("criterion", "Completion criterion", ParameterKind::String),
                optional("replicant", "Preferred replicant", ParameterKind::Replicant),
                optional("home", "Manufacturing home", ParameterKind::Location),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(event_tour_workflow_kind().as_str()),
            display_name: "Fulfill event".to_owned(),
            aliases: strings(&["event_tour"]),
            description: "Ensure event requirements are staged, then dispatch the selected replicant to resolve the event.".to_owned(),
            category: "events".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::System, EntityKind::Location, EntityKind::Replicant],
            parameters: vec![
                required("event", "Event designation", ParameterKind::String),
                optional("criterion", "Completion criterion", ParameterKind::String),
                optional("replicant", "Preferred replicant", ParameterKind::Replicant),
                optional("home", "Manufacturing home", ParameterKind::Location),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(observatory_workflow_kind().as_str()),
            display_name: "Search with observatory".to_owned(),
            aliases: strings(&["signal_search"]),
            description: "Run bounded automatic prospecting with an eligible Galactic Observatory.".to_owned(),
            category: "observatory".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::Device, EntityKind::System],
            parameters: vec![optional("observatory", "Galactic Observatory", ParameterKind::Device)],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(survey_workflow_kind().as_str()),
            display_name: "Survey route".to_owned(),
            aliases: strings(&["survey"]),
            description: "Plan or execute a restart-safe system survey route.".to_owned(),
            category: "compatibility".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![
                EntityKind::System,
                EntityKind::Replicant,
                EntityKind::Device,
            ],
            parameters: vec![
                enum_parameter("mode", "Mode", &["plan", "run"], "run"),
                required("replicant", "Replicant", ParameterKind::Replicant),
                required("vessel", "Vessel", ParameterKind::Device),
                required("center", "Centre system", ParameterKind::System),
                bounded(
                    defaulted("radius_ly", "Radius (ly)", ParameterKind::Number, 10.0),
                    Some(0.0),
                    None,
                ),
                bounded(
                    defaulted("system_limit", "System limit", ParameterKind::Integer, 80),
                    Some(1.0),
                    None,
                ),
                bounded(
                    defaulted(
                        "star_detail_concurrency",
                        "Catalogue concurrency",
                        ParameterKind::Integer,
                        8,
                    ),
                    Some(1.0),
                    None,
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
                bounded(
                    defaulted(
                        "travel_timeout_seconds",
                        "Travel timeout (seconds)",
                        ParameterKind::Integer,
                        21_600,
                    ),
                    Some(1.0),
                    None,
                ),
                bounded(
                    defaulted(
                        "survey_timeout_seconds",
                        "Survey timeout (seconds)",
                        ParameterKind::Integer,
                        21_600,
                    ),
                    Some(1.0),
                    None,
                ),
                required(
                    "maintenance_home",
                    "Maintenance home",
                    ParameterKind::System,
                ),
                bounded(
                    defaulted(
                        "maintenance_interval",
                        "Maintenance interval",
                        ParameterKind::Integer,
                        40,
                    ),
                    Some(1.0),
                    None,
                ),
                bounded(
                    defaulted(
                        "maintenance_threshold_pct",
                        "Maintenance threshold (%)",
                        ParameterKind::Number,
                        25.0,
                    ),
                    Some(0.0),
                    Some(100.0),
                ),
                bounded(
                    defaulted(
                        "maintenance_resume_pct",
                        "Maintenance resume (%)",
                        ParameterKind::Number,
                        95.0,
                    ),
                    Some(0.0),
                    Some(100.0),
                ),
                bounded(
                    defaulted(
                        "maintenance_check_seconds",
                        "Maintenance check (seconds)",
                        ParameterKind::Integer,
                        900,
                    ),
                    Some(1.0),
                    None,
                ),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(relay_workflow_kind().as_str()),
            display_name: "Relay expansion".to_owned(),
            aliases: strings(&["relay"]),
            description: "Build and deploy a restart-safe relay expansion.".to_owned(),
            category: "compatibility".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![
                EntityKind::System,
                EntityKind::Location,
                EntityKind::Replicant,
            ],
            parameters: vec![
                required("replicant", "Replicant", ParameterKind::Replicant),
                required("hub", "Manufacturing hub", ParameterKind::Location),
                required(
                    "targets_csv",
                    "Target systems (comma-separated)",
                    ParameterKind::System,
                ),
                required("mission_file", "Mission file", ParameterKind::String),
                bounded(
                    defaulted(
                        "max_hop_ly",
                        "Maximum hop (ly)",
                        ParameterKind::Number,
                        7.499,
                    ),
                    Some(0.0),
                    None,
                ),
                bounded(
                    defaulted(
                        "wait_timeout_seconds",
                        "Wait timeout (seconds)",
                        ParameterKind::Integer,
                        21_600,
                    ),
                    Some(1.0),
                    None,
                ),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(mining_workflow_kind().as_str()),
            display_name: "Mining expansion".to_owned(),
            aliases: strings(&["mining"]),
            description: "Stage and deploy restart-safe mining installations to one or more systems."
                .to_owned(),
            category: "compatibility".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![
                EntityKind::System,
                EntityKind::Location,
                EntityKind::Replicant,
            ],
            parameters: vec![
                required("replicant", "Replicant", ParameterKind::Replicant),
                required("hub", "Manufacturing hub", ParameterKind::Location),
                required(
                    "systems_csv",
                    "Target systems (comma-separated)",
                    ParameterKind::System,
                ),
                required("mission_file", "Mission file", ParameterKind::String),
                bounded(
                    defaulted(
                        "wait_timeout_seconds",
                        "Wait timeout (seconds)",
                        ParameterKind::Integer,
                        21_600,
                    ),
                    Some(1.0),
                    None,
                ),
                bounded(
                    defaulted(
                        "max_concurrency",
                        "Maximum concurrency",
                        ParameterKind::Integer,
                        4,
                    ),
                    Some(1.0),
                    None,
                ),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(event_workflow_kind().as_str()),
            display_name: "Event fulfillment".to_owned(),
            aliases: strings(&["event"]),
            description:
                "Plan and execute a discovered event, or resume an existing event mission/campaign, as a durable workflow."
                    .to_owned(),
            category: "compatibility".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::System, EntityKind::Location],
            parameters: vec![
                optional("event", "Event designation", ParameterKind::String),
                optional("criterion", "Completion criterion", ParameterKind::String),
                optional("replicant", "Replicant", ParameterKind::Replicant),
                optional("home", "Manufacturing home", ParameterKind::Location),
                defaulted(
                    "plan_file",
                    "Event plan file",
                    ParameterKind::String,
                    "event-mission.json",
                ),
                defaulted(
                    "replace_plan",
                    "Replace existing plan",
                    ParameterKind::Boolean,
                    false,
                ),
                bounded(
                    defaulted(
                        "wait_timeout_seconds",
                        "Wait timeout (seconds)",
                        ParameterKind::Integer,
                        21_600,
                    ),
                    Some(1.0),
                    None,
                ),
            ],
            supported_triggers: all_trigger_kinds(),
        },
        WorkflowDescriptor {
            kind: operation_kind(requirement_workflow_kind().as_str()),
            display_name: "Fulfill requirement".to_owned(),
            aliases: strings(&["requirement"]),
            description: "Evaluate desired state and expose its lower-level child work.".to_owned(),
            category: "compatibility".to_owned(),
            operation_class: OperationClass::Workflow,
            risk: MutationRisk::Elevated,
            applicable_to: vec![EntityKind::System, EntityKind::Location],
            parameters: vec![required(
                "requirement_json",
                "Typed requirement JSON",
                ParameterKind::String,
            )],
            supported_triggers: all_trigger_kinds(),
        },
    ]
}

fn all_trigger_kinds() -> Vec<TriggerKind> {
    vec![
        TriggerKind::Manual,
        TriggerKind::Schedule,
        TriggerKind::GameEvent,
        TriggerKind::StateCondition,
        TriggerKind::ParentWorkflow,
    ]
}

fn descriptor_kinds(catalogue: &DescriptorCatalog) -> impl Iterator<Item = (OperationClass, &str)> {
    catalogue
        .reports
        .iter()
        .map(|item| (item.operation_class, item.kind.0.as_str()))
        .chain(
            catalogue
                .actions
                .iter()
                .map(|item| (item.operation_class, item.kind.0.as_str())),
        )
        .chain(
            catalogue
                .workflows
                .iter()
                .map(|item| (item.operation_class, item.kind.0.as_str())),
        )
}

fn operation_kind(value: &str) -> OperationKind {
    OperationKind(value.to_owned())
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
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

fn bounded(
    mut parameter: ParameterDescriptor,
    minimum: Option<f64>,
    maximum: Option<f64>,
) -> ParameterDescriptor {
    parameter.validation.minimum = minimum;
    parameter.validation.maximum = maximum;
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

fn bootstrap_action(kind: &str, display_name: &str, description: &str) -> ActionDescriptor {
    ActionDescriptor {
        kind: operation_kind(kind),
        display_name: display_name.to_owned(),
        aliases: Vec::new(),
        description: description.to_owned(),
        category: "bootstrap".to_owned(),
        operation_class: OperationClass::Action,
        risk: MutationRisk::Elevated,
        applicable_to: vec![EntityKind::System, EntityKind::Location],
        parameters: vec![
            required("mission_file", "Mission file", ParameterKind::String),
            bounded(
                defaulted(
                    "wait_timeout_seconds",
                    "Wait timeout (seconds)",
                    ParameterKind::Integer,
                    21_600,
                ),
                Some(1.0),
                None,
            ),
        ],
    }
}

fn normalize_bobnet_channel(channel: &str) -> Result<String, CatalogueError> {
    let channel = channel.trim();
    if channel.is_empty() {
        return Err(CatalogueError::Invalid(
            "BobNet channel must not be empty".to_owned(),
        ));
    }
    Ok(if channel.starts_with('#') {
        channel.to_owned()
    } else {
        format!("#{channel}")
    })
}

#[derive(Debug, Deserialize)]
struct BobnetSendAction {
    replicant: String,
    channel: String,
    text: String,
}
#[derive(Debug, Deserialize)]
struct ReplicantTravelAction {
    replicant: String,
    destination: String,
    route: String,
}
#[derive(Debug, Deserialize)]
struct ReplicantTeleportAction {
    replicant: String,
    target: String,
}
#[derive(Debug, Deserialize)]
struct ReplicantSlingshotAction {
    replicant: String,
    slingshot: String,
}
#[derive(Debug, Deserialize)]
struct SimulationStartAction {
    interface: String,
    replicant: String,
    scenario: String,
}
#[derive(Debug, Deserialize)]
struct SimulationAbandonAction {
    interface: String,
    simulation_id: i64,
}
#[derive(Debug, Deserialize)]
struct TradeExecuteAction {
    controller: String,
    trade_code: String,
}
#[derive(Debug, Deserialize)]
struct TradeCreateAction {
    controller: String,
    name: String,
    stock: i64,
    criteria_resources_json: String,
    criteria_devices_json: String,
    reward_resources_json: String,
    reward_devices_json: String,
}
#[derive(Debug, Deserialize)]
struct TradeDeleteAction {
    controller: String,
    trade_code: String,
}
#[derive(Debug, Deserialize)]
struct TradeShopAction {
    controller: String,
    name: String,
    description: Option<String>,
    announcement: Option<String>,
}
#[derive(Debug, Deserialize)]
struct DeviceTravelAction {
    device: String,
    destination: String,
}
#[derive(Debug, Deserialize)]
struct DeviceLifecycleAction {
    device: String,
    command: String,
}
#[derive(Debug, Deserialize)]
struct DeviceTargetAction {
    device: String,
    target: String,
}
#[derive(Debug, Deserialize)]
struct AutofactoryPrintAction {
    device: String,
    device_type: String,
    quantity: i64,
    #[serde(default)]
    tags: Option<String>,
    #[serde(default)]
    flatpack: bool,
}
#[derive(Debug, Deserialize)]
struct ObservatoryAutoProspectAction {
    device: Option<String>,
}
#[derive(Debug, Deserialize)]
struct ObservatoryProspectAction {
    device: String,
}
#[derive(Debug, Deserialize)]
struct ObservatoryProspectDirectionAction {
    device: String,
    x: f64,
    y: f64,
    z: f64,
}
#[derive(Debug, Deserialize)]
struct ObservatoryTriangulateAction {
    device: String,
    signature: String,
    x: f64,
    y: f64,
    z: f64,
}
#[derive(Debug, Deserialize)]
struct CloneStowTargetAction {
    matrix: String,
    cradle: String,
}
#[derive(Debug, Deserialize)]
struct CloneReplicateAction {
    source: String,
    target: String,
    name: Option<String>,
}
#[derive(Debug, Deserialize)]
struct HubDeviceAction {
    device: String,
}
#[derive(Debug, Deserialize)]
struct HubWelcomeAction {
    device: String,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HubRenameAction {
    device: String,
    designation: String,
    name: String,
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
            target_systems: None,
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

#[derive(Deserialize)]
struct MiningStart {
    replicant: String,
    hub: String,
    systems_csv: String,
    mission_file: PathBuf,
    #[serde(default = "default_timeout")]
    wait_timeout_seconds: u64,
    #[serde(default = "default_mining_concurrency")]
    max_concurrency: usize,
}

#[derive(Deserialize)]
struct EventStart {
    #[serde(default)]
    event: Option<String>,
    #[serde(default)]
    criterion: Option<String>,
    #[serde(default)]
    replicant: Option<String>,
    #[serde(default)]
    home: Option<String>,
    plan_file: PathBuf,
    #[serde(default)]
    replace_plan: bool,
    #[serde(default = "default_timeout")]
    wait_timeout_seconds: u64,
}

#[derive(Deserialize)]
struct BootstrapStart {
    mission_file: PathBuf,
    #[serde(default = "default_timeout")]
    wait_timeout_seconds: u64,
}

impl BootstrapStart {
    fn request(self) -> BootstrapExecutionRequest {
        BootstrapExecutionRequest::new(
            self.mission_file,
            Duration::from_secs(self.wait_timeout_seconds),
        )
    }
}

#[derive(Deserialize)]
struct RequirementStart {
    requirement_json: String,
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
fn default_mining_concurrency() -> usize {
    4
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kinds_are_globally_unique_and_workflows_have_factories() {
        let catalogue = OperationCatalogue::new().expect("catalogue");
        let kinds = descriptor_kinds(catalogue.descriptors())
            .map(|(_, kind)| kind)
            .collect::<Vec<_>>();
        assert_eq!(
            kinds.len(),
            kinds
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
        );
        assert!(
            catalogue
                .workflow_registry()
                .contains(&survey_workflow_kind())
        );
        assert!(
            catalogue
                .workflow_registry()
                .contains(&relay_workflow_kind())
        );
        assert!(
            catalogue
                .workflow_registry()
                .contains(&mining_workflow_kind())
        );
        assert!(
            catalogue
                .workflow_registry()
                .contains(&event_workflow_kind())
        );
    }

    #[test]
    fn intent_workflows_hide_executor_plumbing_from_frontend_descriptors() {
        let catalogue = OperationCatalogue::new().expect("catalogue");
        for kind in [
            "scan.system",
            "scan.belt",
            "scan.tour",
            "salvage.site",
            "mining.deploy",
            "logistics.delivery",
            "exploration.frontier",
            "event.delivery",
            "event.tour",
            "observatory.search",
        ] {
            let descriptor = catalogue
                .descriptors()
                .workflows
                .iter()
                .find(|descriptor| descriptor.kind.0 == kind)
                .expect("intent descriptor");
            assert_ne!(descriptor.category, "compatibility");
            assert!(descriptor.parameters.iter().all(|parameter| {
                !parameter.name.ends_with("_file")
                    && !parameter.name.contains("timeout")
                    && !parameter.name.contains("concurrency")
                    && !parameter.name.ends_with("_json")
            }));
        }

        for kind in [
            "survey.route",
            "relay.expansion",
            "mining.expansion",
            "event.fulfillment",
            "requirement.fulfillment",
        ] {
            let descriptor = catalogue
                .descriptors()
                .workflows
                .iter()
                .find(|descriptor| descriptor.kind.0 == kind)
                .expect("compatibility descriptor");
            assert_eq!(descriptor.category, "compatibility");
        }
    }

    #[test]
    fn logistics_delivery_accepts_mixed_manifest_parameters() {
        let catalogue = OperationCatalogue::new().expect("catalogue");
        let parameters = BTreeMap::from([
            ("origin".to_owned(), Value::String("SCEPTURUM".to_owned())),
            (
                "destination".to_owned(),
                Value::String("TWAFFY-OBJ-1".to_owned()),
            ),
            (
                "resources".to_owned(),
                serde_json::json!({"rares": 400, "volatiles": 100}),
            ),
            (
                "devices".to_owned(),
                serde_json::json!([{"device_type": "exotic_matter_injector", "quantity": 36}]),
            ),
            (
                "device_tags".to_owned(),
                serde_json::json!(["twaffy-obj-1"]),
            ),
        ]);
        let validated = catalogue
            .validate(
                OperationClass::Workflow,
                logistics_workflow_kind().as_str(),
                parameters,
                false,
            )
            .expect("mixed logistics manifest should validate");
        assert!(validated.contains_key("resources"));
        assert!(validated.contains_key("devices"));
        assert!(validated.contains_key("device_tags"));
    }

    #[test]
    fn intent_workflow_factories_are_registered() {
        let catalogue = OperationCatalogue::new().expect("catalogue");
        for kind in [
            scan_system_workflow_kind(),
            scan_belt_workflow_kind(),
            scan_tour_workflow_kind(),
            salvage_workflow_kind(),
            mining_deploy_workflow_kind(),
            logistics_workflow_kind(),
            exploration_workflow_kind(),
            event_delivery_workflow_kind(),
            event_tour_workflow_kind(),
            observatory_workflow_kind(),
        ] {
            assert!(catalogue.workflow_registry().contains(&kind), "{kind}");
        }
    }

    #[test]
    fn bobnet_channels_are_normalized_to_irc_form() {
        assert_eq!(
            normalize_bobnet_channel("general").expect("channel"),
            "#general"
        );
        assert_eq!(
            normalize_bobnet_channel(" #trade ").expect("channel"),
            "#trade"
        );
        assert!(normalize_bobnet_channel("   ").is_err());
    }

    #[test]
    fn validation_applies_defaults_and_rejects_invalid_values() {
        let catalogue = OperationCatalogue::new().expect("catalogue");
        let valid = catalogue
            .validate(
                OperationClass::Report,
                "nearby_belts",
                BTreeMap::from([("origin".to_owned(), Value::String("SOL".to_owned()))]),
                false,
            )
            .expect("valid report");
        assert_eq!(valid["radius_ly"], 10.0);
        assert!(
            catalogue
                .validate(
                    OperationClass::Report,
                    "nearby_belts",
                    BTreeMap::from([
                        ("origin".to_owned(), Value::String("SOL".to_owned())),
                        ("concurrency".to_owned(), Value::from(0)),
                    ]),
                    false,
                )
                .is_err()
        );
    }

    #[test]
    fn legacy_script_names_resolve_to_registered_capabilities() {
        let catalogue = OperationCatalogue::new().expect("catalogue");
        for (class, alias, canonical) in [
            (OperationClass::Action, "clear_tags", "clear_tags"),
            (
                OperationClass::Action,
                "contribute_twaffy_injectors",
                "contribute_devices",
            ),
            (
                OperationClass::Action,
                "tag_twaffy_ring_injectors",
                "tag_devices",
            ),
            (OperationClass::Report, "nearby_belt_report", "nearby_belts"),
        ] {
            assert_eq!(catalogue.resolve_kind(class, alias), Some(canonical));
        }
    }

    #[test]
    fn applicability_uses_explicit_entity_contexts() {
        let catalogue = OperationCatalogue::new().expect("catalogue");
        assert!(catalogue.is_applicable(
            OperationClass::Report,
            "nearby_belts",
            &EntityKind::System
        ));
        assert!(!catalogue.is_applicable(
            OperationClass::Report,
            "nearby_belts",
            &EntityKind::Device
        ));
        assert!(catalogue.is_applicable(OperationClass::Action, "clear_tags", &EntityKind::Device));
        assert!(catalogue.is_applicable(
            OperationClass::Action,
            "bootstrap.run",
            &EntityKind::System
        ));
    }

    #[test]
    fn bootstrap_actions_validate_persisted_mission_inputs() {
        let catalogue = OperationCatalogue::new().expect("catalogue");
        for kind in ["bootstrap.stage", "bootstrap.deliver", "bootstrap.run"] {
            let parameters = catalogue
                .validate(
                    OperationClass::Action,
                    kind,
                    BTreeMap::from([(
                        "mission_file".to_owned(),
                        Value::String("regional-bootstrap.json".to_owned()),
                    )]),
                    false,
                )
                .expect("valid bootstrap action");
            assert_eq!(parameters["wait_timeout_seconds"], 21_600);
        }
    }
}
