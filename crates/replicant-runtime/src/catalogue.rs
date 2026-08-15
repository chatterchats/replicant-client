//! Unified discovery, validation, and invocation for application operations.

use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use replicant_client::managed::Client;
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
    relay::RelayExpansionRequest,
    reports::nearby_belt_report,
    survey::{SurveyMode, SurveyOptions},
    workflows::{
        RelayWorkflowConfig, RequirementWorkflowConfig, SurveyWorkflowConfig, new_relay_workflow,
        new_requirement_workflow, new_survey_workflow, register, relay_workflow_kind,
        requirement_workflow_kind, survey_workflow_kind,
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
        ],
        workflows: workflow_descriptors(),
    }
}

fn workflow_descriptors() -> Vec<WorkflowDescriptor> {
    vec![
        WorkflowDescriptor {
            kind: operation_kind(survey_workflow_kind().as_str()),
            display_name: "Survey route".to_owned(),
            aliases: strings(&["survey"]),
            description: "Plan or execute a restart-safe system survey route.".to_owned(),
            category: "survey".to_owned(),
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
            category: "relay".to_owned(),
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
            kind: operation_kind(requirement_workflow_kind().as_str()),
            display_name: "Fulfill requirement".to_owned(),
            aliases: strings(&["requirement"]),
            description: "Evaluate desired state and expose its lower-level child work.".to_owned(),
            category: "automation".to_owned(),
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
    }
}
