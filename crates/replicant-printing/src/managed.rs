//! Managed-client Autofactory discovery and durable queue submission.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

use replicant_client::{
    AutofactoryPrintOptions, Client, DeviceType, Operation, OperationStatus, domain::Device, raw,
};
use serde::Serialize;
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::time::sleep;
use tracing::info;

use crate::{
    Blueprint, FactoryWorkload, PrintRequest, PrintTime, QuantityMap, ScheduleError,
    normalize_requests, plan_print_dependencies, schedule_prints,
};

const AUTOFACTORY: &str = "autofactory";

/// One live Autofactory and its current queue capacity.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FactoryState {
    /// Autofactory device code.
    pub code: String,
    /// Maximum number of queued print units.
    pub queue_size: usize,
    /// Number of queued print units currently consuming queue capacity.
    pub queued_units: usize,
    /// Whether one print is actively running.
    pub printing: bool,
    /// Whether the factory's current head job is blocked on missing inputs.
    pub waiting_for_resources: bool,
    /// Estimated seconds until work newly appended now would finish waiting.
    pub remaining_seconds: f64,
}

impl FactoryState {
    /// Number of print units that can be submitted immediately.
    #[must_use]
    pub fn available_slots(&self) -> usize {
        if self.waiting_for_resources {
            0
        } else {
            self.queue_size.saturating_sub(self.queued_units)
        }
    }

    /// Converts this live state into pure scheduler input.
    #[must_use]
    pub fn workload(&self) -> FactoryWorkload {
        FactoryWorkload {
            code: self.code.clone(),
            remaining_seconds: self.remaining_seconds,
        }
    }
}

/// Options for continuously filling live Autofactory queues.
#[derive(Clone, Debug)]
pub struct QueueOptions {
    /// Location containing eligible Autofactories.
    pub hub: String,
    /// Tags applied to every printed device.
    pub tags: Vec<String>,
    /// Print requested modular devices in their compacted transport state.
    pub flatpack: bool,
    /// Delay between queue-capacity checks when more work remains.
    pub poll_interval: Duration,
    /// Maximum time allowed to queue every requested unit.
    pub wait_timeout: Duration,
    /// Optional explicit Autofactory set. When present, queueing is confined to
    /// these factories so higher-level workflows can build dependency-safe lanes.
    pub factory_codes: Option<BTreeSet<String>>,
}

impl QueueOptions {
    /// Creates queue options for one manufacturing hub.
    #[must_use]
    pub fn at(hub: impl Into<String>) -> Self {
        Self {
            hub: hub.into(),
            tags: Vec::new(),
            flatpack: false,
            poll_interval: Duration::from_secs(5),
            wait_timeout: Duration::from_secs(21_600),
            factory_codes: None,
        }
    }
}

/// Summary of successfully queued manufacturing work.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct QueueReport {
    /// Canonical combined request quantities.
    pub requested: QuantityMap,
    /// Quantities accepted by Autofactories during this call.
    pub queued: QuantityMap,
    /// Missing prerequisite devices printed before the requested outputs.
    pub components_queued: QuantityMap,
    /// Existing free component stock reserved instead of reprinting it.
    pub components_reused: QuantityMap,
    /// Physical completion waves used for recursive component ordering.
    pub component_waves: Vec<QuantityMap>,
    /// Accepted quantities keyed by Autofactory device code.
    pub by_factory: BTreeMap<String, i64>,
    /// Durable operation identifiers for each accepted unit submission.
    pub operation_ids: Vec<String>,
    /// Whether the accepted jobs requested compacted modular output.
    pub flatpack: bool,
}

/// One non-blocking prerequisite queue pass.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct PrerequisiteQueueReport {
    /// Work accepted during this pass and the dependency waves still being tracked.
    pub queue: QueueReport,
    /// True when every prerequisite for the requested parent is either already
    /// present/in-flight under the supplied tags or was accepted during this pass.
    pub ready_for_parent: bool,
}

/// Result of clearing queued work and, unless preserved, active work from one Autofactory.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct ClearedFactory {
    /// Autofactory device code.
    pub code: String,
    /// Last observed location.
    pub location: Option<String>,
    /// Whether a `clear_queue` command was submitted.
    pub queue_cleared: bool,
    /// Whether `deactivate` was submitted to stop an active print.
    pub active_print_stopped: bool,
    /// Whether an active print was deliberately left running for this factory.
    pub active_print_preserved: bool,
}

/// Summary of a system-wide Autofactory clear operation.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct ClearReport {
    /// Canonical target star system.
    pub system: String,
    /// Factories processed in stable device-code order.
    pub factories: Vec<ClearedFactory>,
    /// Durable operation identifiers for clear/deactivate commands.
    pub operation_ids: Vec<String>,
}

/// Options controlling a system-wide Autofactory clear operation.
#[derive(Clone, Debug)]
pub struct ClearOptions {
    /// Delay between live-state checks while queued work is settling.
    pub poll_interval: Duration,
    /// Maximum time allowed for each Autofactory to reach its requested clear state.
    pub wait_timeout: Duration,
    /// Autofactory codes whose currently active print must not be deactivated.
    /// Queued work on these factories is still cleared normally.
    pub preserve_active_factory_codes: BTreeSet<String>,
}

impl Default for ClearOptions {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            wait_timeout: Duration::from_secs(21_600),
            preserve_active_factory_codes: BTreeSet::new(),
        }
    }
}

/// One device-type inventory summary within a system.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct DeviceInventoryStatus {
    /// Canonical device type.
    pub device_type: String,
    /// Matching account-owned devices currently in the system.
    pub total: i64,
    /// Free-standing devices that can satisfy a component requirement.
    pub free: i64,
    /// Counts grouped by the device's current status string.
    pub by_status: BTreeMap<String, i64>,
}

/// One requested output or prerequisite quantity summary.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct ManufacturingStatusLine {
    /// Canonical device type.
    pub device_type: String,
    /// Quantity required for the requested production target.
    pub required: i64,
    /// Completed matching devices available in the system.
    pub available: i64,
    /// Matching devices currently being printed.
    pub active: i64,
    /// Matching devices waiting in Autofactory queues.
    pub queued: i64,
    /// Quantity that is neither available nor in flight.
    pub missing: i64,
    /// Quantity available or in flight beyond the requirement.
    pub surplus: i64,
}

/// One active or queued Autofactory job.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct FactoryPrintJobStatus {
    /// Canonical device type, when supplied by the server.
    pub device_type: String,
    /// Number of units represented by this job.
    pub quantity: i64,
    /// Estimated seconds remaining for an active print.
    pub eta_seconds: Option<f64>,
    /// Tags that will be applied to the completed device.
    pub tags: Vec<String>,
    /// Whether the job matches the status command's tag filter.
    pub matches_filter: bool,
}

/// Live work reported by one Autofactory in the system.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct FactoryPrintStatus {
    /// Autofactory device code.
    pub code: String,
    /// Current location.
    pub location: Option<String>,
    /// Current factory status string.
    pub status: Option<String>,
    /// Current active print, when present.
    pub active: Option<FactoryPrintJobStatus>,
    /// Jobs waiting behind the active print.
    pub queued: Vec<FactoryPrintJobStatus>,
}

/// Read-only manufacturing and inventory status for one star system.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct SystemPrintingStatus {
    /// Canonical target star system.
    pub system: String,
    /// Tags required on completed and in-flight devices for gap calculations.
    pub tags: Vec<String>,
    /// Matching device inventory grouped by type and current status.
    pub inventory: Vec<DeviceInventoryStatus>,
    /// Explicit requested output status.
    pub requested: Vec<ManufacturingStatusLine>,
    /// Recursive prerequisite status for outputs that are still missing.
    pub prerequisites: Vec<ManufacturingStatusLine>,
    /// Missing prerequisite print waves in leaf-first completion order.
    pub missing_component_waves: Vec<QuantityMap>,
    /// Explicit output quantities that still need to be queued.
    pub remaining_requests: QuantityMap,
    /// Live Autofactory work in stable device-code order.
    pub factories: Vec<FactoryPrintStatus>,
}

/// Live discovery or durable queueing failure.
#[derive(Debug, Error)]
pub enum PrintingError {
    /// Pure schedule validation failed.
    #[error(transparent)]
    Schedule(#[from] ScheduleError),
    /// The managed Replicant client failed.
    #[error(transparent)]
    Client(#[from] replicant_client::Error),
    /// No owned Autofactory was found at the requested hub.
    #[error("no account-owned Autofactory is available at `{0}`")]
    NoFactoryAtHub(String),
    /// Flatpack output was requested for a non-modular device blueprint.
    #[error("flatpack output requires the `modular` feature, but `{0}` is not modular")]
    FlatpackRequiresModular(String),
    /// A queue payload contained an invalid quantity.
    #[error("Autofactory {factory_code} reported invalid queued quantity {quantity}")]
    InvalidQueuedQuantity {
        /// Autofactory reporting the invalid quantity.
        factory_code: String,
        /// Invalid queued quantity.
        quantity: i64,
    },
    /// The server definitively rejected a managed printing/factory operation.
    #[error("managed operation {operation_id} ended as {status:?}: {response:?}")]
    SubmissionRejected {
        /// Durable managed operation identifier.
        operation_id: String,
        /// Terminal rejection status.
        status: OperationStatus,
        /// Sanitized server response or error.
        response: Option<Value>,
    },
    /// A managed printing/factory operation is not yet safely classified.
    #[error(
        "managed operation {operation_id} has unresolved status {status:?}; it was not retried"
    )]
    SubmissionUnresolved {
        /// Durable managed operation identifier.
        operation_id: String,
        /// Current unresolved operation status.
        status: OperationStatus,
    },
    /// Queue capacity did not become available before the configured timeout.
    #[error("timed out waiting for Autofactory queue capacity; remaining: {remaining:?}")]
    TimedOut {
        /// Quantities not yet queued.
        remaining: QuantityMap,
    },
    /// A prerequisite wave was queued but did not physically finish in time.
    #[error(
        "timed out waiting for component print wave {wave} to finish on Autofactories {factories:?}"
    )]
    ComponentWaveTimedOut {
        /// One-based dependency-wave index.
        wave: usize,
        /// Autofactories that still had active or queued work.
        factories: Vec<String>,
    },
    /// Existing prerequisite work did not finish before restart reconciliation timed out.
    #[error(
        "timed out waiting for existing prerequisite work {device_types:?} on Autofactories {factories:?}"
    )]
    ExistingComponentWorkTimedOut {
        /// Relevant component types that were still active or queued.
        device_types: Vec<String>,
        /// Autofactories still carrying relevant work.
        factories: Vec<String>,
    },
    /// No account-owned Autofactory exists in the requested system.
    #[error("no account-owned Autofactory was found in system `{0}`")]
    NoFactoryInSystem(String),
    /// A requested active-print exclusion did not match an Autofactory in the target system.
    #[error(
        "cannot preserve active print on Autofactory {factory_code}: no such account-owned Autofactory was found in system `{system}`"
    )]
    PreservedFactoryNotFound {
        /// Requested Autofactory device code.
        factory_code: String,
        /// Canonical target system.
        system: String,
    },
    /// A factory cannot perform the requested queue-clear or print-stop transition.
    #[error("Autofactory {factory_code} does not advertise `{command}` while status is {status:?}")]
    FactoryCommandUnavailable {
        /// Autofactory device code.
        factory_code: String,
        /// Required command name.
        command: String,
        /// Last observed device status.
        status: Option<String>,
    },
    /// A cleared factory did not reach an empty-work state in time.
    #[error("timed out waiting for Autofactory {factory_code} to clear queued and active work")]
    FactoryClearTimedOut {
        /// Autofactory device code.
        factory_code: String,
    },
}

/// Fetches unlocked blueprints and their print durations.
pub async fn fetch_blueprints(
    client: &Client,
) -> Result<BTreeMap<String, Blueprint>, PrintingError> {
    Ok(client
        .raw()
        .blueprints()
        .list()
        .await?
        .value
        .blueprints
        .into_iter()
        .filter_map(|blueprint| {
            let device_type = blueprint.device_type?;
            Some((
                device_type.clone(),
                Blueprint {
                    device_type,
                    print_time_seconds: blueprint.print_time.unwrap_or(0.0),
                    features: blueprint.features.unwrap_or_default(),
                    components: numeric_map(blueprint.components.as_ref()),
                },
            ))
        })
        .collect())
}

/// Discovers account-owned Autofactories at `hub` and reads their live queues.
pub async fn discover_factories<B: PrintTime>(
    client: &Client,
    hub: &str,
    blueprints: &BTreeMap<String, B>,
) -> Result<Vec<FactoryState>, PrintingError> {
    let handles = client
        .devices()
        .find()
        .owned()
        .of_type(DeviceType::from(AUTOFACTORY))
        .at(hub)
        .collect()
        .await?;
    let mut factory_codes = Vec::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if device_type(&snapshot) == Some(AUTOFACTORY) && device_location(&snapshot) == Some(hub) {
            // The local projection is used only to identify candidate factory
            // codes. `inspect_factory` below is authoritative for current
            // queue/status, so a stale cached `available_commands` list cannot
            // hide a factory that has just become usable again.
            factory_codes.push(handle.id().as_str().to_owned());
        }
    }
    factory_codes.sort();

    let mut factories = Vec::with_capacity(factory_codes.len());
    for code in factory_codes {
        factories.push(inspect_factory(client, &code, blueprints).await?);
    }
    Ok(factories)
}

/// Reads one Autofactory's queue capacity and projected workload.
pub async fn inspect_factory<B: PrintTime>(
    client: &Client,
    factory_code: &str,
    blueprints: &BTreeMap<String, B>,
) -> Result<FactoryState, PrintingError> {
    let detail = client.raw().devices().get(factory_code).await?.value;
    let queue_size = usize::try_from(detail.queue_size.unwrap_or(1).max(1)).map_err(|_| {
        PrintingError::InvalidQueuedQuantity {
            factory_code: factory_code.to_owned(),
            quantity: detail.queue_size.unwrap_or(1),
        }
    })?;
    let queued_units = queued_print_units(factory_code, &detail.print_queue)?;
    let active_seconds = detail
        .printing
        .as_ref()
        .and_then(|printing| printing.eta_seconds)
        .unwrap_or(0.0)
        .max(0.0);
    let queued_seconds = detail
        .print_queue
        .iter()
        .map(|job| {
            let quantity = integer_field(job, &["quantity", "count"])
                .unwrap_or(1)
                .max(1);
            string_field(job, &["device_type", "type"])
                .and_then(|device_type| blueprints.get(device_type))
                .map_or(0.0, |blueprint| {
                    blueprint.print_time_seconds().max(0.0) * quantity as f64
                })
        })
        .sum::<f64>();
    Ok(FactoryState {
        code: factory_code.to_owned(),
        queue_size,
        queued_units,
        printing: detail.printing.is_some(),
        waiting_for_resources: detail.status.as_deref() == Some("waiting_for_resources"),
        remaining_seconds: active_seconds + queued_seconds,
    })
}

/// Returns the live number of available queue slots on one Autofactory.
pub async fn factory_queue_slots(
    client: &Client,
    factory_code: &str,
) -> Result<usize, PrintingError> {
    let detail = client.raw().devices().get(factory_code).await?.value;
    if detail.status.as_deref() == Some("waiting_for_resources") {
        return Ok(0);
    }
    let queue_size = usize::try_from(detail.queue_size.unwrap_or(1).max(1)).map_err(|_| {
        PrintingError::InvalidQueuedQuantity {
            factory_code: factory_code.to_owned(),
            quantity: detail.queue_size.unwrap_or(1),
        }
    })?;
    Ok(queue_size.saturating_sub(queued_print_units(factory_code, &detail.print_queue)?))
}

/// Submits one durable print operation and verifies its immediate outcome.
///
/// This waits for the HTTP submission classification, not for the physical
/// print to finish. Ambiguous operations are returned as errors and are never
/// automatically retried.
pub async fn enqueue_print(
    client: &Client,
    factory_code: &str,
    device_type: &str,
    quantity: i64,
    tags: &[String],
) -> Result<Operation, PrintingError> {
    enqueue_print_configured(client, factory_code, device_type, quantity, tags, false).await
}

/// Submits one durable flatpack print operation for a modular device.
pub async fn enqueue_print_flatpacked(
    client: &Client,
    factory_code: &str,
    device_type: &str,
    quantity: i64,
    tags: &[String],
) -> Result<Operation, PrintingError> {
    enqueue_print_configured(client, factory_code, device_type, quantity, tags, true).await
}

async fn enqueue_print_configured(
    client: &Client,
    factory_code: &str,
    device_type: &str,
    quantity: i64,
    tags: &[String],
    flatpack: bool,
) -> Result<Operation, PrintingError> {
    let handle = client.devices().get(factory_code).await?;
    let mut options = AutofactoryPrintOptions::new(quantity).tags(tags.iter().cloned());
    if flatpack {
        options = options.flatpacked();
    }
    let operation = handle
        .enqueue_print_configured(device_type, options)
        .await?;
    ensure_submission_accepted(&operation).await?;
    Ok(operation)
}

/// Verifies the immediate server classification of a durable operation.
pub async fn ensure_submission_accepted(operation: &Operation) -> Result<(), PrintingError> {
    let outcome = operation.outcome().await?;
    let operation_id = operation.id().as_str().to_owned();
    if matches!(
        outcome.status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(PrintingError::SubmissionRejected {
            operation_id,
            status: outcome.status,
            response: outcome.response,
        });
    }
    if matches!(
        outcome.status,
        OperationStatus::Prepared | OperationStatus::Submitted | OperationStatus::Ambiguous
    ) {
        return Err(PrintingError::SubmissionUnresolved {
            operation_id,
            status: outcome.status,
        });
    }
    Ok(())
}

/// Continuously fills all eligible Autofactory queues until every direct
/// request has been accepted.
///
/// This preserves the original low-level behavior for automation crates that
/// already own their dependency lifecycle. Use [`queue_prints_with_components`]
/// for recursive blueprint-component handling.
pub async fn queue_prints(
    client: &Client,
    requests: &[PrintRequest],
    options: &QueueOptions,
) -> Result<QueueReport, PrintingError> {
    let requested = normalize_requests(requests)?;
    let blueprints = fetch_blueprints(client).await?;
    for device_type in requested.keys() {
        let blueprint = blueprints
            .get(device_type)
            .ok_or_else(|| ScheduleError::MissingBlueprint(device_type.clone()))?;
        if options.flatpack && !blueprint.is_modular() {
            return Err(PrintingError::FlatpackRequiresModular(device_type.clone()));
        }
    }
    queue_print_batch(client, &requested, options, &blueprints, options.flatpack).await
}

/// Queues prerequisite devices ahead of a higher-level parent without waiting
/// for physical completion.
///
/// Only completed and in-flight component devices carrying `options.tags` are
/// credited to this bundle. This lets callers give every parent a unique
/// component-bundle tag, making the operation restart-safe without allowing
/// another event's queued components to satisfy the same dependency twice.
///
/// The function is deliberately non-blocking with respect to queue capacity: it
/// makes one live scheduling pass and returns `ready_for_parent = false` when a
/// dependency wave cannot be fully accepted yet. Later waves are never queued
/// until every unit of the preceding wave is already present/in-flight or has
/// been accepted, preserving a deadlock-free queue topology.
pub async fn queue_print_prerequisites_ahead(
    client: &Client,
    requests: &[PrintRequest],
    options: &QueueOptions,
) -> Result<PrerequisiteQueueReport, PrintingError> {
    let requested = normalize_requests(requests)?;
    let blueprints = fetch_blueprints(client).await?;
    let status = printing_status_in_system(client, &options.hub, requests, &options.tags).await?;
    let mut report = QueueReport {
        requested,
        component_waves: status.missing_component_waves.clone(),
        ..QueueReport::default()
    };

    if status.missing_component_waves.is_empty() {
        return Ok(PrerequisiteQueueReport {
            queue: report,
            ready_for_parent: true,
        });
    }

    for wave in &status.missing_component_waves {
        let (wave_report, complete) =
            queue_print_batch_once(client, wave, options, &blueprints, false).await?;
        merge_quantities(&mut report.components_queued, &wave_report.queued);
        merge_quantities(&mut report.by_factory, &wave_report.by_factory);
        report.operation_ids.extend(wave_report.operation_ids);
        if !complete {
            return Ok(PrerequisiteQueueReport {
                queue: report,
                ready_for_parent: false,
            });
        }
    }

    Ok(PrerequisiteQueueReport {
        queue: report,
        ready_for_parent: true,
    })
}

/// Manufactures only the recursive prerequisite devices needed by `requests`.
///
/// This is useful for higher-level workflows that need to retain ownership of
/// how the requested parent devices themselves are tagged, assigned to
/// factories, reconciled after restart, or otherwise submitted. Existing free
/// component stock at the hub is reserved first. Missing components are queued
/// leaf-first and every wave is allowed to physically finish before this
/// function returns.
pub async fn queue_print_prerequisites(
    client: &Client,
    requests: &[PrintRequest],
    options: &QueueOptions,
) -> Result<QueueReport, PrintingError> {
    let blueprints = fetch_blueprints(client).await?;
    let requested = normalize_requests(requests)?;
    let dependency_plan =
        prepare_dependency_plan(client, requests, options, &blueprints, true).await?;
    let mut report = queue_dependency_waves(client, options, &blueprints, &dependency_plan).await?;
    report.requested = requested;
    Ok(report)
}

/// Queues requested devices after recursively manufacturing their subdevices.
///
/// Printable subdevices declared by blueprint `components` are expanded
/// recursively. Missing components are queued in leaf-first waves, and each
/// wave is allowed to physically finish before its dependent wave or the
/// caller's requested devices are submitted. Existing free component stock at
/// the hub is reserved before new component prints are scheduled.
pub async fn queue_prints_with_components(
    client: &Client,
    requests: &[PrintRequest],
    options: &QueueOptions,
) -> Result<QueueReport, PrintingError> {
    let blueprints = fetch_blueprints(client).await?;
    let requested = normalize_requests(requests)?;
    let dependency_plan =
        prepare_dependency_plan(client, requests, options, &blueprints, false).await?;

    for device_type in requested.keys() {
        let blueprint = blueprints
            .get(device_type)
            .ok_or_else(|| ScheduleError::MissingBlueprint(device_type.clone()))?;
        if options.flatpack && !blueprint.is_modular() {
            return Err(PrintingError::FlatpackRequiresModular(device_type.clone()));
        }
    }

    let mut report = queue_dependency_waves(client, options, &blueprints, &dependency_plan).await?;
    report.requested = requested.clone();
    report.flatpack = options.flatpack;

    let requested_report =
        queue_print_batch(client, &requested, options, &blueprints, options.flatpack).await?;
    report.queued = requested_report.queued;
    merge_quantities(&mut report.by_factory, &requested_report.by_factory);
    report.operation_ids.extend(requested_report.operation_ids);
    Ok(report)
}

async fn prepare_dependency_plan(
    client: &Client,
    requests: &[PrintRequest],
    options: &QueueOptions,
    blueprints: &BTreeMap<String, Blueprint>,
    include_waiting_parents: bool,
) -> Result<crate::PrintDependencyPlan, PrintingError> {
    let mut prerequisite_requests = requests.to_vec();
    if include_waiting_parents {
        let blocked = waiting_parent_requests(client, &options.hub).await?;
        if !blocked.is_empty() {
            info!(
                requests = ?blocked,
                "including prerequisites for Autofactory jobs already waiting for resources"
            );
            prerequisite_requests.extend(blocked);
        }
    }
    if prerequisite_requests.is_empty() {
        return Ok(crate::PrintDependencyPlan::default());
    }
    let prerequisite_requested = normalize_requests(&prerequisite_requests)?;
    let component_types = component_dependency_types(&prerequisite_requested, blueprints)?;
    wait_for_existing_component_work(client, &options.hub, &component_types, options).await?;
    let available_components = discover_component_stock(client, &options.hub).await?;
    Ok(plan_print_dependencies(
        &prerequisite_requests,
        blueprints,
        &available_components,
    )?)
}

async fn queue_dependency_waves(
    client: &Client,
    options: &QueueOptions,
    blueprints: &BTreeMap<String, Blueprint>,
    dependency_plan: &crate::PrintDependencyPlan,
) -> Result<QueueReport, PrintingError> {
    let mut report = QueueReport {
        requested: dependency_plan.requested.clone(),
        components_reused: dependency_plan.reused_components.clone(),
        component_waves: dependency_plan.component_waves.clone(),
        ..QueueReport::default()
    };

    for (index, wave) in dependency_plan.component_waves.iter().enumerate() {
        let wave_report = queue_print_batch(client, wave, options, blueprints, false).await?;
        merge_quantities(&mut report.components_queued, &wave_report.queued);
        merge_quantities(&mut report.by_factory, &wave_report.by_factory);
        report.operation_ids.extend(wave_report.operation_ids);
        wait_for_component_wave(
            client,
            blueprints,
            wave_report.by_factory.keys().cloned().collect(),
            options,
            index.saturating_add(1),
        )
        .await?;
    }
    Ok(report)
}

async fn queue_print_batch_once(
    client: &Client,
    requested: &QuantityMap,
    options: &QueueOptions,
    blueprints: &BTreeMap<String, Blueprint>,
    flatpack: bool,
) -> Result<(QueueReport, bool), PrintingError> {
    let mut remaining = requested.clone();
    remaining.retain(|_, quantity| *quantity > 0);
    let mut report = QueueReport {
        requested: requested.clone(),
        flatpack,
        ..QueueReport::default()
    };
    if remaining.is_empty() {
        return Ok((report, true));
    }

    let factories = discover_factories(client, &options.hub, blueprints).await?;
    if factories.is_empty() {
        return Err(PrintingError::NoFactoryAtHub(options.hub.clone()));
    }
    let available_factories = factories
        .iter()
        .filter(|factory| factory.available_slots() > 0)
        .filter(|factory| {
            options
                .factory_codes
                .as_ref()
                .is_none_or(|codes| codes.contains(&factory.code))
        })
        .collect::<Vec<_>>();
    if available_factories.is_empty() {
        return Ok((report, false));
    }

    let workloads = available_factories
        .iter()
        .map(|factory| factory.workload())
        .collect::<Vec<_>>();
    let schedule = schedule_prints(&remaining, blueprints, &workloads)?;
    let mut slots = available_factories
        .iter()
        .map(|factory| (factory.code.clone(), factory.available_slots()))
        .collect::<BTreeMap<_, _>>();

    for batch in schedule.batches {
        let available = slots.get(&batch.factory_code).copied().unwrap_or(0);
        let quantity =
            usize::try_from(batch.quantity).map_err(|_| ScheduleError::InvalidQuantity {
                device_type: batch.device_type.clone(),
                quantity: batch.quantity,
            })?;
        let to_submit = available.min(quantity);
        for _ in 0..to_submit {
            let operation = if flatpack {
                enqueue_print_flatpacked(
                    client,
                    &batch.factory_code,
                    &batch.device_type,
                    1,
                    &options.tags,
                )
                .await?
            } else {
                enqueue_print(
                    client,
                    &batch.factory_code,
                    &batch.device_type,
                    1,
                    &options.tags,
                )
                .await?
            };
            *remaining.entry(batch.device_type.clone()).or_default() -= 1;
            *report.queued.entry(batch.device_type.clone()).or_default() += 1;
            *report
                .by_factory
                .entry(batch.factory_code.clone())
                .or_default() += 1;
            report
                .operation_ids
                .push(operation.id().as_str().to_owned());
            info!(
                factory = %batch.factory_code,
                device_type = %batch.device_type,
                flatpacked = flatpack,
                "queued dependency-safe print unit"
            );
        }
        slots.insert(batch.factory_code, available.saturating_sub(to_submit));
    }

    remaining.retain(|_, quantity| *quantity > 0);
    Ok((report, remaining.is_empty()))
}

async fn queue_print_batch(
    client: &Client,
    requested: &QuantityMap,
    options: &QueueOptions,
    blueprints: &BTreeMap<String, Blueprint>,
    flatpack: bool,
) -> Result<QueueReport, PrintingError> {
    let mut remaining = requested.clone();
    let mut report = QueueReport {
        requested: requested.clone(),
        flatpack,
        ..QueueReport::default()
    };
    if remaining.is_empty() {
        return Ok(report);
    }

    let deadline = Instant::now() + options.wait_timeout;
    loop {
        remaining.retain(|_, quantity| *quantity > 0);
        if remaining.is_empty() {
            return Ok(report);
        }

        let factories = discover_factories(client, &options.hub, blueprints).await?;
        if factories.is_empty() {
            return Err(PrintingError::NoFactoryAtHub(options.hub.clone()));
        }
        let available_factories = factories
            .iter()
            .filter(|factory| factory.available_slots() > 0)
            .filter(|factory| {
                options
                    .factory_codes
                    .as_ref()
                    .is_none_or(|codes| codes.contains(&factory.code))
            })
            .collect::<Vec<_>>();
        if available_factories.is_empty() {
            if Instant::now() >= deadline {
                return Err(PrintingError::TimedOut { remaining });
            }
            info!("waiting for Autofactory queue capacity");
            sleep(options.poll_interval).await;
            continue;
        }
        let workloads = available_factories
            .iter()
            .map(|factory| factory.workload())
            .collect::<Vec<_>>();
        let schedule = schedule_prints(&remaining, blueprints, &workloads)?;
        let mut slots = available_factories
            .iter()
            .map(|factory| (factory.code.clone(), factory.available_slots()))
            .collect::<BTreeMap<_, _>>();
        let mut submitted = 0usize;

        for batch in schedule.batches {
            let available = slots.get(&batch.factory_code).copied().unwrap_or(0);
            let quantity =
                usize::try_from(batch.quantity).map_err(|_| ScheduleError::InvalidQuantity {
                    device_type: batch.device_type.clone(),
                    quantity: batch.quantity,
                })?;
            let to_submit = available.min(quantity);
            for _ in 0..to_submit {
                let operation = if flatpack {
                    enqueue_print_flatpacked(
                        client,
                        &batch.factory_code,
                        &batch.device_type,
                        1,
                        &options.tags,
                    )
                    .await?
                } else {
                    enqueue_print(
                        client,
                        &batch.factory_code,
                        &batch.device_type,
                        1,
                        &options.tags,
                    )
                    .await?
                };
                *remaining.entry(batch.device_type.clone()).or_default() -= 1;
                *report.queued.entry(batch.device_type.clone()).or_default() += 1;
                *report
                    .by_factory
                    .entry(batch.factory_code.clone())
                    .or_default() += 1;
                report
                    .operation_ids
                    .push(operation.id().as_str().to_owned());
                submitted += 1;
                info!(
                    factory = %batch.factory_code,
                    device_type = %batch.device_type,
                    flatpack,
                    "queued distributed print unit"
                );
            }
            slots.insert(batch.factory_code, available.saturating_sub(to_submit));
        }

        remaining.retain(|_, quantity| *quantity > 0);
        if remaining.is_empty() {
            return Ok(report);
        }
        if Instant::now() >= deadline {
            return Err(PrintingError::TimedOut { remaining });
        }
        if submitted == 0 {
            info!("waiting for Autofactory queue capacity");
        }
        sleep(options.poll_interval).await;
    }
}

fn component_dependency_types(
    requested: &QuantityMap,
    blueprints: &BTreeMap<String, Blueprint>,
) -> Result<BTreeSet<String>, ScheduleError> {
    let mut components = BTreeSet::new();
    let mut expanded = BTreeSet::new();
    for device_type in requested.keys() {
        let blueprint = blueprints
            .get(device_type)
            .ok_or_else(|| ScheduleError::MissingBlueprint(device_type.clone()))?;
        let mut visiting = vec![device_type.clone()];
        collect_component_types(
            blueprint,
            blueprints,
            &mut components,
            &mut expanded,
            &mut visiting,
        )?;
    }
    Ok(components)
}

fn collect_component_types(
    blueprint: &Blueprint,
    blueprints: &BTreeMap<String, Blueprint>,
    components: &mut BTreeSet<String>,
    expanded: &mut BTreeSet<String>,
    visiting: &mut Vec<String>,
) -> Result<(), ScheduleError> {
    for component in blueprint.components.keys() {
        if visiting.iter().any(|item| item == component) {
            return Err(ScheduleError::ComponentCycle(component.clone()));
        }
        components.insert(component.clone());
        if !expanded.insert(component.clone()) {
            continue;
        }
        let Some(component_blueprint) = blueprints.get(component) else {
            continue;
        };
        visiting.push(component.clone());
        collect_component_types(
            component_blueprint,
            blueprints,
            components,
            expanded,
            visiting,
        )?;
        visiting.pop();
    }
    Ok(())
}

async fn waiting_parent_requests(
    client: &Client,
    hub: &str,
) -> Result<Vec<PrintRequest>, PrintingError> {
    let handles = client
        .devices()
        .find()
        .owned()
        .of_type(DeviceType::from(AUTOFACTORY))
        .at(hub)
        .collect()
        .await?;
    let mut factory_codes = Vec::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if device_type(&snapshot) == Some(AUTOFACTORY) && device_location(&snapshot) == Some(hub) {
            factory_codes.push(handle.id().as_str().to_owned());
        }
    }
    factory_codes.sort();

    let mut requests = Vec::new();
    for factory_code in factory_codes {
        let detail = client.raw().devices().get(&factory_code).await?.value;
        if detail.status.as_deref() != Some("waiting_for_resources") {
            continue;
        }
        if let Some(device_type) = detail
            .printing
            .as_ref()
            .and_then(|printing| printing.device_type.as_deref())
        {
            requests.push(PrintRequest::new(device_type, 1));
            continue;
        }
        let Some(head) = detail.print_queue.first() else {
            continue;
        };
        let Some(device_type) = string_field(head, &["device_type", "type"]) else {
            continue;
        };
        let quantity = integer_field(head, &["quantity", "count"])
            .unwrap_or(1)
            .max(1);
        requests.push(PrintRequest::new(device_type, quantity));
    }
    Ok(requests)
}

async fn wait_for_existing_component_work(
    client: &Client,
    hub: &str,
    component_types: &BTreeSet<String>,
    options: &QueueOptions,
) -> Result<(), PrintingError> {
    if component_types.is_empty() {
        return Ok(());
    }
    let deadline = Instant::now() + options.wait_timeout;
    let mut saw_pending = false;
    let mut clear_after_pending = false;
    loop {
        let handles = client
            .devices()
            .find()
            .owned()
            .of_type(DeviceType::from(AUTOFACTORY))
            .at(hub)
            .collect()
            .await?;
        let mut factory_codes = Vec::new();
        for handle in handles {
            let snapshot = handle.snapshot().await?;
            if device_type(&snapshot) == Some(AUTOFACTORY)
                && device_location(&snapshot) == Some(hub)
            {
                factory_codes.push(handle.id().as_str().to_owned());
            }
        }
        factory_codes.sort();

        let mut pending_factories = BTreeSet::new();
        let mut pending_types = BTreeSet::new();
        for factory_code in factory_codes {
            let detail = client.raw().devices().get(&factory_code).await?.value;
            if detail.status.as_deref() == Some("waiting_for_resources") {
                continue;
            }
            if let Some(device_type) = detail
                .printing
                .as_ref()
                .and_then(|printing| printing.device_type.as_deref())
                && component_types.contains(device_type)
            {
                pending_factories.insert(factory_code.clone());
                pending_types.insert(device_type.to_owned());
            }
            for job in &detail.print_queue {
                let Some(device_type) = string_field(job, &["device_type", "type"]) else {
                    continue;
                };
                if component_types.contains(device_type) {
                    pending_factories.insert(factory_code.clone());
                    pending_types.insert(device_type.to_owned());
                }
            }
        }
        if pending_factories.is_empty() {
            if !saw_pending || clear_after_pending {
                return Ok(());
            }
            clear_after_pending = true;
            info!("existing prerequisite work cleared; confirming completed inventory projection");
            sleep(options.poll_interval).await;
            continue;
        }
        saw_pending = true;
        clear_after_pending = false;
        if Instant::now() >= deadline {
            return Err(PrintingError::ExistingComponentWorkTimedOut {
                device_types: pending_types.into_iter().collect(),
                factories: pending_factories.into_iter().collect(),
            });
        }
        info!(
            device_types = ?pending_types,
            factories = ?pending_factories,
            "waiting for existing prerequisite work before replanning"
        );
        sleep(options.poll_interval).await;
    }
}

async fn discover_component_stock(
    client: &Client,
    hub: &str,
) -> Result<QuantityMap, PrintingError> {
    let handles = client
        .devices()
        .refresh_many()
        .at(hub)
        .page_size(50)
        .collect()
        .await?;
    let mut stock = QuantityMap::new();
    for handle in handles {
        let device = handle.snapshot().await?;
        let Some(device_type) = device_type(&device) else {
            continue;
        };
        if device_location(&device) != Some(hub) || !reusable_component_device(&device) {
            continue;
        }
        *stock.entry(device_type.to_owned()).or_default() += 1;
    }
    Ok(stock)
}

fn component_stock_status(status: Option<&str>) -> bool {
    status.is_none_or(|status| {
        matches!(
            status,
            "idle" | "inactive" | "deactivated" | "compacted" | "complete"
        )
    })
}

async fn wait_for_component_wave(
    client: &Client,
    blueprints: &BTreeMap<String, Blueprint>,
    factories: BTreeSet<String>,
    options: &QueueOptions,
    wave: usize,
) -> Result<(), PrintingError> {
    if factories.is_empty() {
        return Ok(());
    }
    let deadline = Instant::now() + options.wait_timeout;
    loop {
        let mut pending = Vec::new();
        for factory in &factories {
            let state = inspect_factory(client, factory, blueprints).await?;
            if state.printing || state.queued_units > 0 {
                pending.push(factory.clone());
            }
        }
        if pending.is_empty() {
            info!(wave, "component print wave physically completed");
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(PrintingError::ComponentWaveTimedOut {
                wave,
                factories: pending,
            });
        }
        info!(wave, factories = ?pending, "waiting for component print wave");
        sleep(options.poll_interval).await;
    }
}

/// Reads completed devices and live Autofactory work in one system.
///
/// When `requests` are supplied, completed and in-flight requested outputs are
/// subtracted first. Recursive component requirements are then calculated only
/// for the outputs that remain missing. Optional tags are required on both
/// completed devices and print jobs used by the gap calculation.
pub async fn printing_status_in_system(
    client: &Client,
    system_or_location: &str,
    requests: &[PrintRequest],
    tags: &[String],
) -> Result<SystemPrintingStatus, PrintingError> {
    let system = system_from_location(system_or_location);
    let handles = client
        .devices()
        .find()
        .owned()
        .in_system(system.clone())
        .collect()
        .await?;
    let mut inventory_quantities = QuantityMap::new();
    let mut free_quantities = QuantityMap::new();
    let mut inventory = BTreeMap::<String, DeviceInventoryStatus>::new();
    let mut factories = Vec::<(String, Option<String>)>::new();

    for handle in handles {
        let snapshot = handle.snapshot().await?;
        let Some(location) = device_location(&snapshot) else {
            continue;
        };
        if !designation_in_system(location, &system) {
            continue;
        }
        let Some(device_type) = device_type(&snapshot) else {
            continue;
        };
        if device_type == AUTOFACTORY {
            factories.push((handle.id().as_str().to_owned(), Some(location.to_owned())));
        }
        if !matches_required_tags(&snapshot.tags, tags) {
            continue;
        }

        let status = snapshot
            .status
            .as_ref()
            .map_or_else(|| "unknown".to_owned(), |value| value.as_str().to_owned());
        let free = reusable_component_device(&snapshot);
        *inventory_quantities
            .entry(device_type.to_owned())
            .or_default() += 1;
        if free {
            *free_quantities.entry(device_type.to_owned()).or_default() += 1;
        }
        let entry =
            inventory
                .entry(device_type.to_owned())
                .or_insert_with(|| DeviceInventoryStatus {
                    device_type: device_type.to_owned(),
                    ..DeviceInventoryStatus::default()
                });
        entry.total += 1;
        if free {
            entry.free += 1;
        }
        *entry.by_status.entry(status).or_default() += 1;
    }
    factories.sort_by(|left, right| left.0.cmp(&right.0));

    let mut active_prints = QuantityMap::new();
    let mut queued_prints = QuantityMap::new();
    let mut factory_statuses = Vec::with_capacity(factories.len());
    for (factory_code, location) in factories {
        let detail = client.raw().devices().get(&factory_code).await?.value;
        let active = detail.printing.as_ref().map(|printing| {
            let job = FactoryPrintJobStatus {
                device_type: printing
                    .device_type
                    .clone()
                    .unwrap_or_else(|| "unknown".into()),
                quantity: 1,
                eta_seconds: printing.eta_seconds,
                tags: printing.tags.clone(),
                matches_filter: matches_required_tags(&printing.tags, tags),
            };
            if job.matches_filter && job.device_type != "unknown" {
                *active_prints.entry(job.device_type.clone()).or_default() += 1;
            }
            job
        });
        let queued = detail
            .print_queue
            .iter()
            .map(|value| {
                let job = queue_job_status(value, tags);
                if job.matches_filter && job.device_type != "unknown" {
                    *queued_prints.entry(job.device_type.clone()).or_default() += job.quantity;
                }
                job
            })
            .collect();
        factory_statuses.push(FactoryPrintStatus {
            code: factory_code,
            location,
            status: detail.status,
            active,
            queued,
        });
    }

    let requested_quantities = normalize_requests(requests)?;
    let (requested, prerequisites, missing_component_waves, remaining_requests) =
        if requested_quantities.is_empty() {
            (Vec::new(), Vec::new(), Vec::new(), QuantityMap::new())
        } else {
            let blueprints = fetch_blueprints(client).await?;
            calculate_manufacturing_status(
                &requested_quantities,
                &inventory_quantities,
                &free_quantities,
                &active_prints,
                &queued_prints,
                &blueprints,
            )?
        };

    Ok(SystemPrintingStatus {
        system,
        tags: tags.to_vec(),
        inventory: inventory.into_values().collect(),
        requested,
        prerequisites,
        missing_component_waves,
        remaining_requests,
        factories: factory_statuses,
    })
}

type StatusCalculation = (
    Vec<ManufacturingStatusLine>,
    Vec<ManufacturingStatusLine>,
    Vec<QuantityMap>,
    QuantityMap,
);

fn calculate_manufacturing_status(
    requested: &QuantityMap,
    inventory: &QuantityMap,
    free_inventory: &QuantityMap,
    active_prints: &QuantityMap,
    queued_prints: &QuantityMap,
    blueprints: &BTreeMap<String, Blueprint>,
) -> Result<StatusCalculation, ScheduleError> {
    let mut component_free = free_inventory.clone();
    let mut component_active = active_prints.clone();
    let mut component_queued = queued_prints.clone();
    let mut requested_status = Vec::with_capacity(requested.len());
    let mut remaining_requests = QuantityMap::new();

    for (device_type, required) in requested {
        let available = quantity(inventory, device_type);
        let free = quantity(free_inventory, device_type).min(available);
        let non_free = available.saturating_sub(free);
        let active = quantity(active_prints, device_type);
        let queued = quantity(queued_prints, device_type);
        let mut remaining = (*required).max(0);

        remaining = remaining.saturating_sub(non_free.min(remaining));
        let reserved_free = free.min(remaining);
        remaining = remaining.saturating_sub(reserved_free);
        subtract_quantity(&mut component_free, device_type, reserved_free);
        let reserved_active = active.min(remaining);
        remaining = remaining.saturating_sub(reserved_active);
        subtract_quantity(&mut component_active, device_type, reserved_active);
        let reserved_queued = queued.min(remaining);
        remaining = remaining.saturating_sub(reserved_queued);
        subtract_quantity(&mut component_queued, device_type, reserved_queued);

        if remaining > 0 {
            remaining_requests.insert(device_type.clone(), remaining);
        }
        let (_, surplus) = manufacturing_gap(*required, available, active, queued);
        requested_status.push(ManufacturingStatusLine {
            device_type: device_type.clone(),
            required: *required,
            available,
            active,
            queued,
            missing: remaining,
            surplus,
        });
    }

    if remaining_requests.is_empty() {
        return Ok((requested_status, Vec::new(), Vec::new(), remaining_requests));
    }

    let mut component_supply = component_free.clone();
    merge_quantities(&mut component_supply, &component_active);
    merge_quantities(&mut component_supply, &component_queued);
    let missing_requests = remaining_requests
        .iter()
        .map(|(device_type, quantity)| PrintRequest::new(device_type.clone(), *quantity))
        .collect::<Vec<_>>();
    let plan = plan_print_dependencies(&missing_requests, blueprints, &component_supply)?;
    let mut component_required = plan.reused_components;
    for wave in &plan.component_waves {
        merge_quantities(&mut component_required, wave);
    }

    let prerequisites = component_required
        .into_iter()
        .map(|(device_type, required)| {
            let available = quantity(&component_free, &device_type);
            let active = quantity(&component_active, &device_type);
            let queued = quantity(&component_queued, &device_type);
            let (missing, surplus) = manufacturing_gap(required, available, active, queued);
            ManufacturingStatusLine {
                device_type,
                required,
                available,
                active,
                queued,
                missing,
                surplus,
            }
        })
        .collect();

    Ok((
        requested_status,
        prerequisites,
        plan.component_waves,
        remaining_requests,
    ))
}

fn queue_job_status(value: &Map<String, Value>, required_tags: &[String]) -> FactoryPrintJobStatus {
    let tags = string_array_field(value, "tags");
    FactoryPrintJobStatus {
        device_type: string_field(value, &["device_type", "type"])
            .unwrap_or("unknown")
            .to_owned(),
        quantity: integer_field(value, &["quantity", "count"])
            .unwrap_or(1)
            .max(1),
        eta_seconds: numeric_field(value, &["eta_seconds", "remaining_seconds"]),
        matches_filter: matches_required_tags(&tags, required_tags),
        tags,
    }
}

fn reusable_component_device(device: &Device) -> bool {
    device.relationships.attached_to.is_none()
        && device.relationships.stowed_in.is_none()
        && device.relationships.controller.is_none()
        && !device.is_traveling()
        && component_stock_status(device.status.as_ref().map(|status| status.as_str()))
}

fn matches_required_tags(actual: &[String], required: &[String]) -> bool {
    required.iter().all(|tag| actual.contains(tag))
}

fn subtract_quantity(quantities: &mut QuantityMap, device_type: &str, amount: i64) {
    if amount <= 0 {
        return;
    }
    let remaining = quantity(quantities, device_type).saturating_sub(amount);
    if remaining == 0 {
        quantities.remove(device_type);
    } else {
        quantities.insert(device_type.to_owned(), remaining);
    }
}

fn quantity(quantities: &QuantityMap, device_type: &str) -> i64 {
    quantities.get(device_type).copied().unwrap_or(0).max(0)
}

fn manufacturing_gap(required: i64, available: i64, active: i64, queued: i64) -> (i64, i64) {
    let required = required.max(0);
    let covered = available
        .max(0)
        .saturating_add(active.max(0))
        .saturating_add(queued.max(0));
    (
        required.saturating_sub(covered).max(0),
        covered.saturating_sub(required).max(0),
    )
}

fn canonical_factory_codes(codes: &BTreeSet<String>) -> BTreeSet<String> {
    codes
        .iter()
        .map(|code| code.trim().to_ascii_uppercase())
        .collect()
}

/// Clears queued and active work from every account-owned Autofactory in one
/// system.
///
/// Factories are processed in stable device-code order. A non-empty queue is
/// cleared first. The function then waits for the queue projection to settle
/// and submits `deactivate` only when an active print still exists. An idle
/// factory is a successful terminal state; the command does not require the
/// Autofactory itself to remain deactivated.
pub async fn clear_factories_in_system(
    client: &Client,
    system_or_location: &str,
    poll_interval: Duration,
    wait_timeout: Duration,
) -> Result<ClearReport, PrintingError> {
    clear_factories_in_system_with_options(
        client,
        system_or_location,
        &ClearOptions {
            poll_interval,
            wait_timeout,
            ..ClearOptions::default()
        },
    )
    .await
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ActivePrintClearAction {
    None,
    Preserve,
    Deactivate,
}

fn active_print_clear_action(printing: bool, preserve_active: bool) -> ActivePrintClearAction {
    match (printing, preserve_active) {
        (false, _) => ActivePrintClearAction::None,
        (true, true) => ActivePrintClearAction::Preserve,
        (true, false) => ActivePrintClearAction::Deactivate,
    }
}

/// Clears queued work from every account-owned Autofactory in one system and
/// stops active prints except on explicitly preserved factories.
///
/// Excluded factories still receive `clear_queue`; only their currently active
/// print is protected from the subsequent `deactivate`. All requested factory
/// codes are validated before any mutation is submitted so a typo cannot cause
/// an intended-to-be-preserved print to be stopped accidentally.
pub async fn clear_factories_in_system_with_options(
    client: &Client,
    system_or_location: &str,
    options: &ClearOptions,
) -> Result<ClearReport, PrintingError> {
    let system = system_from_location(system_or_location);
    let handles = client
        .devices()
        .find()
        .owned()
        .of_type(DeviceType::from(AUTOFACTORY))
        .in_system(system.clone())
        .collect()
        .await?;
    let mut factories = Vec::<(String, Option<String>)>::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if device_type(&snapshot) != Some(AUTOFACTORY) {
            continue;
        }
        let location = device_location(&snapshot).map(str::to_owned);
        if location
            .as_deref()
            .is_some_and(|location| designation_in_system(location, &system))
        {
            factories.push((handle.id().as_str().to_owned(), location));
        }
    }
    factories.sort_by(|left, right| left.0.cmp(&right.0));
    if factories.is_empty() {
        return Err(PrintingError::NoFactoryInSystem(system));
    }

    let preserve_active_factory_codes =
        canonical_factory_codes(&options.preserve_active_factory_codes);
    let factory_codes = factories
        .iter()
        .map(|(code, _)| code.to_ascii_uppercase())
        .collect::<BTreeSet<_>>();
    if let Some(factory_code) = preserve_active_factory_codes
        .iter()
        .find(|code| !factory_codes.contains(*code))
    {
        return Err(PrintingError::PreservedFactoryNotFound {
            factory_code: factory_code.clone(),
            system: system.clone(),
        });
    }

    let mut report = ClearReport {
        system,
        ..ClearReport::default()
    };
    for (factory_code, location) in factories {
        let handle = client.devices().get(&factory_code).await?;
        let deadline = Instant::now() + options.wait_timeout;
        let preserve_active =
            preserve_active_factory_codes.contains(&factory_code.to_ascii_uppercase());
        let mut detail = client.raw().devices().get(&factory_code).await?.value;
        let mut cleared = ClearedFactory {
            code: factory_code.clone(),
            location,
            ..ClearedFactory::default()
        };

        if !detail.print_queue.is_empty() {
            if !has_command(&detail.available_commands, "clear_queue") {
                return Err(PrintingError::FactoryCommandUnavailable {
                    factory_code,
                    command: "clear_queue".into(),
                    status: detail.status,
                });
            }
            let operation = handle
                .command(raw::devices::DeviceCommand::ClearQueue)
                .await?;
            ensure_submission_accepted(&operation).await?;
            report
                .operation_ids
                .push(operation.id().as_str().to_owned());
            cleared.queue_cleared = true;
            info!(factory = %factory_code, "cleared Autofactory print queue");
        }

        // `clear_queue` removes queued jobs but leaves the active print alone.
        // Wait for the queue projection to settle before deciding whether the
        // active print should be deactivated or explicitly preserved.
        loop {
            detail = client.raw().devices().get(&factory_code).await?.value;
            if detail.print_queue.is_empty() {
                break;
            }
            if Instant::now() >= deadline {
                return Err(PrintingError::FactoryClearTimedOut { factory_code });
            }
            sleep(options.poll_interval).await;
        }

        match active_print_clear_action(detail.printing.is_some(), preserve_active) {
            ActivePrintClearAction::None => {}
            ActivePrintClearAction::Preserve => {
                cleared.active_print_preserved = true;
                info!(factory = %factory_code, "preserved active Autofactory print");
            }
            ActivePrintClearAction::Deactivate => {
                if !has_command(&detail.available_commands, "deactivate") {
                    return Err(PrintingError::FactoryCommandUnavailable {
                        factory_code,
                        command: "deactivate".into(),
                        status: detail.status,
                    });
                }
                let operation = client
                    .devices()
                    .get(&factory_code)
                    .await?
                    .deactivate()
                    .await?;
                match ensure_submission_accepted(&operation).await {
                    Ok(()) => {
                        report
                            .operation_ids
                            .push(operation.id().as_str().to_owned());
                        cleared.active_print_stopped = true;
                        info!(factory = %factory_code, "stopped active Autofactory print");
                    }
                    Err(error) => {
                        let already_stopped = matches!(
                            &error,
                            PrintingError::SubmissionRejected { response, .. }
                                if nothing_to_deactivate(response.as_ref())
                        );
                        if !already_stopped {
                            return Err(error);
                        }
                        info!(
                            factory = %factory_code,
                            operation_id = %operation.id().as_str(),
                            "active print ended before deactivate reached the server"
                        );
                    }
                }
            }
        }

        loop {
            detail = client.raw().devices().get(&factory_code).await?.value;
            if detail.print_queue.is_empty() && (preserve_active || detail.printing.is_none()) {
                break;
            }
            if Instant::now() >= deadline {
                return Err(PrintingError::FactoryClearTimedOut { factory_code });
            }
            sleep(options.poll_interval).await;
        }
        report.factories.push(cleared);
    }
    Ok(report)
}

fn nothing_to_deactivate(response: Option<&Value>) -> bool {
    response
        .and_then(Value::as_object)
        .and_then(|response| response.get("message"))
        .and_then(Value::as_str)
        .is_some_and(|message| {
            message
                .to_ascii_lowercase()
                .contains("nothing to deactivate")
        })
}

fn merge_quantities(target: &mut QuantityMap, source: &QuantityMap) {
    for (name, quantity) in source {
        *target.entry(name.clone()).or_default() += quantity;
    }
}

fn has_command(commands: &[String], expected: &str) -> bool {
    commands.iter().any(|command| command == expected)
}

fn system_from_location(location: &str) -> String {
    location
        .split('-')
        .next()
        .filter(|system| !system.is_empty())
        .unwrap_or(location)
        .to_ascii_uppercase()
}

fn designation_in_system(location: &str, system: &str) -> bool {
    location.eq_ignore_ascii_case(system)
        || location
            .get(..system.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(system))
            && location.as_bytes().get(system.len()) == Some(&b'-')
}

fn queued_print_units(
    factory_code: &str,
    jobs: &[Map<String, Value>],
) -> Result<usize, PrintingError> {
    let mut queued_units = 0usize;
    for job in jobs {
        let quantity = integer_field(job, &["quantity", "count"])
            .unwrap_or(1)
            .max(1);
        queued_units = queued_units.saturating_add(usize::try_from(quantity).map_err(|_| {
            PrintingError::InvalidQueuedQuantity {
                factory_code: factory_code.to_owned(),
                quantity,
            }
        })?);
    }
    Ok(queued_units)
}

fn string_field<'a>(object: &'a Map<String, Value>, names: &[&str]) -> Option<&'a str> {
    names
        .iter()
        .find_map(|name| object.get(*name).and_then(Value::as_str))
}

fn integer_field(object: &Map<String, Value>, names: &[&str]) -> Option<i64> {
    names.iter().find_map(|name| {
        object.get(*name).and_then(|value| {
            value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
                .or_else(|| value.as_f64().map(|value| value.ceil() as i64))
        })
    })
}

fn numeric_field(object: &Map<String, Value>, names: &[&str]) -> Option<f64> {
    names.iter().find_map(|name| {
        object.get(*name).and_then(|value| {
            value
                .as_f64()
                .or_else(|| value.as_i64().map(|value| value as f64))
                .or_else(|| value.as_u64().map(|value| value as f64))
        })
    })
}

fn string_array_field(object: &Map<String, Value>, name: &str) -> Vec<String> {
    object
        .get(name)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn numeric_map(object: Option<&Map<String, Value>>) -> QuantityMap {
    object
        .into_iter()
        .flat_map(|object| object.iter())
        .filter_map(|(name, value)| {
            let quantity = value
                .as_i64()
                .or_else(|| value.as_u64().and_then(|value| i64::try_from(value).ok()))
                .or_else(|| value.as_f64().map(|value| value.ceil() as i64))
                .unwrap_or(0);
            (quantity > 0).then_some((name.clone(), quantity))
        })
        .collect()
}

fn device_type(device: &Device) -> Option<&str> {
    device.device_type.as_ref().map(|value| value.as_str())
}

fn device_location(device: &Device) -> Option<&str> {
    device
        .location
        .as_ref()
        .map(|location| location.id.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_occupancy_counts_device_quantities() {
        let jobs = vec![
            [("quantity".into(), Value::from(2))].into_iter().collect(),
            [("count".into(), Value::from(3))].into_iter().collect(),
            Map::new(),
        ];
        assert_eq!(queued_print_units("AF1", &jobs).unwrap(), 6);
    }

    #[test]
    fn available_slots_saturate_at_zero() {
        let factory = FactoryState {
            code: "AF1".into(),
            queue_size: 3,
            queued_units: 5,
            printing: true,
            waiting_for_resources: false,
            remaining_seconds: 100.0,
        };
        assert_eq!(factory.available_slots(), 0);
    }

    #[test]
    fn waiting_for_resources_factory_has_no_available_slots() {
        let factory = FactoryState {
            code: "AF1".into(),
            queue_size: 4,
            queued_units: 1,
            printing: false,
            waiting_for_resources: true,
            remaining_seconds: 100.0,
        };
        assert_eq!(factory.available_slots(), 0);
    }

    #[test]
    fn blueprint_component_map_accepts_integer_like_numbers() {
        let values = [
            ("whole".into(), Value::from(2)),
            ("float".into(), Value::from(1.2)),
            ("ignored".into(), Value::from(0)),
        ]
        .into_iter()
        .collect();
        let components = numeric_map(Some(&values));
        assert_eq!(components["whole"], 2);
        assert_eq!(components["float"], 2);
        assert!(!components.contains_key("ignored"));
    }

    #[test]
    fn system_clear_matches_every_child_location() {
        assert_eq!(system_from_location("SCEPTURUM-BELT-1"), "SCEPTURUM");
        assert!(designation_in_system("SCEPTURUM-7-L4", "SCEPTURUM"));
        assert!(designation_in_system("SCEPTURUM-BELT-1", "SCEPTURUM"));
        assert!(!designation_in_system("SCEPTURUMX-1", "SCEPTURUM"));
    }

    #[test]
    fn preserved_active_print_never_selects_deactivate() {
        assert_eq!(
            active_print_clear_action(true, true),
            ActivePrintClearAction::Preserve
        );
        assert_eq!(
            active_print_clear_action(true, false),
            ActivePrintClearAction::Deactivate
        );
        assert_eq!(
            active_print_clear_action(false, true),
            ActivePrintClearAction::None
        );
        assert_eq!(
            active_print_clear_action(false, false),
            ActivePrintClearAction::None
        );
    }

    #[test]
    fn nothing_to_deactivate_rejection_is_reconciled_as_empty_work() {
        let response = serde_json::json!({
            "message": "unexpected HTTP status 400: Nothing to deactivate",
            "status": 400
        });
        assert!(nothing_to_deactivate(Some(&response)));
        assert!(!nothing_to_deactivate(Some(&serde_json::json!({
            "message": "Device is out of comms range"
        }))));
    }

    #[test]
    fn status_counts_completed_and_in_flight_outputs_before_components() {
        let blueprints = [
            (
                "exotic_matter_injector".into(),
                Blueprint {
                    device_type: "exotic_matter_injector".into(),
                    components: [
                        ("casimir_array".into(), 1),
                        ("exotic_particle_trap".into(), 2),
                        ("negative_energy_conduit".into(), 1),
                    ]
                    .into_iter()
                    .collect(),
                    ..Blueprint::default()
                },
            ),
            (
                "casimir_array".into(),
                Blueprint {
                    device_type: "casimir_array".into(),
                    ..Blueprint::default()
                },
            ),
            (
                "exotic_particle_trap".into(),
                Blueprint {
                    device_type: "exotic_particle_trap".into(),
                    ..Blueprint::default()
                },
            ),
            (
                "negative_energy_conduit".into(),
                Blueprint {
                    device_type: "negative_energy_conduit".into(),
                    ..Blueprint::default()
                },
            ),
        ]
        .into_iter()
        .collect();
        let requested = [("exotic_matter_injector".into(), 5)].into_iter().collect();
        let inventory: QuantityMap = [
            ("exotic_matter_injector".into(), 2),
            ("casimir_array".into(), 1),
            ("exotic_particle_trap".into(), 1),
        ]
        .into_iter()
        .collect();
        let free = inventory.clone();
        let active = [
            ("exotic_matter_injector".into(), 1),
            ("exotic_particle_trap".into(), 1),
        ]
        .into_iter()
        .collect();
        let queued = [("exotic_matter_injector".into(), 1)].into_iter().collect();

        let (targets, components, waves, remaining) = calculate_manufacturing_status(
            &requested,
            &inventory,
            &free,
            &active,
            &queued,
            &blueprints,
        )
        .unwrap();

        assert_eq!(targets[0].missing, 1);
        assert_eq!(remaining["exotic_matter_injector"], 1);
        let by_type = components
            .iter()
            .map(|line| (line.device_type.as_str(), line))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(by_type["casimir_array"].missing, 0);
        assert_eq!(by_type["exotic_particle_trap"].missing, 0);
        assert_eq!(by_type["negative_energy_conduit"].missing, 1);
        assert_eq!(waves.len(), 1);
        assert_eq!(waves[0]["negative_energy_conduit"], 1);
    }

    #[test]
    fn status_clamps_missing_and_reports_surplus() {
        assert_eq!(manufacturing_gap(24, 23, 1, 1), (0, 1));
        assert_eq!(manufacturing_gap(48, 0, 10, 86), (0, 48));
        assert_eq!(manufacturing_gap(24, 9, 0, 23), (0, 8));
    }

    #[test]
    fn dependency_type_closure_includes_recursive_components() {
        let blueprints = [
            (
                "injector".into(),
                Blueprint {
                    device_type: "injector".into(),
                    components: [("middle".into(), 1)].into_iter().collect(),
                    ..Blueprint::default()
                },
            ),
            (
                "middle".into(),
                Blueprint {
                    device_type: "middle".into(),
                    components: [("leaf".into(), 2)].into_iter().collect(),
                    ..Blueprint::default()
                },
            ),
            (
                "leaf".into(),
                Blueprint {
                    device_type: "leaf".into(),
                    ..Blueprint::default()
                },
            ),
        ]
        .into_iter()
        .collect();
        let requested = [("injector".into(), 1)].into_iter().collect();
        assert_eq!(
            component_dependency_types(&requested, &blueprints).unwrap(),
            ["leaf".into(), "middle".into()].into_iter().collect()
        );
    }

    #[test]
    fn status_tag_filter_requires_every_requested_tag() {
        let actual = vec!["event-stock".into(), "season-two".into()];
        assert!(matches_required_tags(
            &actual,
            &["event-stock".into(), "season-two".into()]
        ));
        assert!(!matches_required_tags(
            &actual,
            &["event-stock".into(), "different".into()]
        ));
    }

    #[test]
    fn queued_job_status_reads_quantity_eta_and_tags() {
        let value = serde_json::json!({
            "device_type": "exotic_matter_injector",
            "quantity": 2,
            "eta_seconds": 123,
            "tags": ["event-stock"]
        });
        let job = queue_job_status(
            value.as_object().expect("queue object"),
            &["event-stock".into()],
        );
        assert_eq!(job.device_type, "exotic_matter_injector");
        assert_eq!(job.quantity, 2);
        assert_eq!(job.eta_seconds, Some(123.0));
        assert!(job.matches_filter);
    }

    #[test]
    fn preserved_factory_codes_are_canonicalized_before_clear() {
        let codes = BTreeSet::from([
            " ff259175 ".to_owned(),
            "E71BC14B".to_owned(),
            "e71bc14b".to_owned(),
        ]);

        assert_eq!(
            canonical_factory_codes(&codes),
            BTreeSet::from(["E71BC14B".to_owned(), "FF259175".to_owned()])
        );
    }
}
