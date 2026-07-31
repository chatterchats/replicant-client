use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::Duration,
};

use replicant_client::{Client, Device, Operation, OperationId, OperationStatus, raw};
use replicant_event_planner::{
    BeaconAction, DeviceRequirement, DeviceStock, ResourceMap, blueprint_resource_cost,
    role_tag,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::time::{Instant, sleep, timeout};
use tracing::{info, warn};

use super::{
    AnyResult, ClaimedDevice, Config, EventMissionPlan, MissionPhase, app_error,
    fetch_blueprints, fetch_devices, fetch_inventory, normalize_event, save_plan,
};

const CARGO_FREIGHTER: &str = "cargo_freighter";
const FTL_BEACON: &str = "ftl_beacon";
const POLL_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct ExecutionState {
    #[serde(default)]
    pub(crate) print_batches: Vec<ExecutionPrintBatch>,
    #[serde(default)]
    pub(crate) payload_devices: Vec<PayloadDevice>,
    #[serde(default)]
    pub(crate) reward_home_baseline: Option<ResourceMap>,
    #[serde(default)]
    pub(crate) event_resolved: bool,
    #[serde(default)]
    pub(crate) beacon_completed: bool,
    #[serde(default)]
    pub(crate) warnings: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ExecutionPrintBatch {
    pub(crate) factory_code: String,
    pub(crate) device_type: String,
    pub(crate) quantity: i64,
    pub(crate) role: String,
    pub(crate) batch_tag: String,
    #[serde(default)]
    pub(crate) submission_started: bool,
    #[serde(default)]
    pub(crate) submitted: bool,
    #[serde(default)]
    pub(crate) operation_id: Option<String>,
    #[serde(default)]
    pub(crate) produced_codes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct PayloadDevice {
    pub(crate) code: String,
    pub(crate) device_type: String,
    pub(crate) role: String,
    #[serde(default)]
    pub(crate) delivered: bool,
}

pub(crate) async fn execute_saved_plan(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    if !config.execute {
        return Err(app_error(
            io::ErrorKind::PermissionDenied,
            "event mission execution requires --execute",
        ));
    }
    if plan.phase.is_terminal() {
        println!(
            "Mission {} is already terminal ({:?}).",
            plan.mission_id, plan.phase
        );
        return Ok(());
    }

    initialize_execution(plan);
    save_plan(&config.plan_path, plan)?;

    if phase_rank(plan.phase) <= phase_rank(MissionPhase::Manufacturing) {
        set_phase(config, plan, MissionPhase::Manufacturing)?;
        reconcile_print_batches(client, plan).await?;
        save_plan(&config.plan_path, plan)?;
        if !plan.execution.print_batches.iter().any(|batch| batch.submitted) {
            ensure_home_resources(client, config, plan).await?;
        }
        submit_print_batches(client, config, plan).await?;
        wait_for_print_outputs(client, config, plan).await?;
        assign_printed_outputs(client, plan).await?;
        save_plan(&config.plan_path, plan)?;
    }

    if phase_rank(plan.phase) <= phase_rank(MissionPhase::ClaimingTransports) {
        set_phase(config, plan, MissionPhase::ClaimingTransports)?;
        claim_mission_assets(client, config, plan).await?;
    }

    if phase_rank(plan.phase) <= phase_rank(MissionPhase::PreparingFleet) {
        set_phase(config, plan, MissionPhase::PreparingFleet)?;
        prepare_initial_fleet(client, config, plan).await?;
    }

    if phase_rank(plan.phase) <= phase_rank(MissionPhase::Outbound) {
        set_phase(config, plan, MissionPhase::Outbound)?;
        deliver_event_resources(client, config, plan).await?;
    }

    if phase_rank(plan.phase) <= phase_rank(MissionPhase::Staging) {
        set_phase(config, plan, MissionPhase::Staging)?;
        stage_event_devices(client, config, plan).await?;
    }

    if phase_rank(plan.phase) <= phase_rank(MissionPhase::InstallingBeacon) {
        set_phase(config, plan, MissionPhase::InstallingBeacon)?;
        if let Err(error) = install_beacon(client, config, plan).await {
            let warning = format!("FTL beacon objective failed: {error}");
            warn!(warning = %warning, "continuing event mission without beacon");
            if !plan.execution.warnings.contains(&warning) {
                plan.execution.warnings.push(warning);
            }
            save_plan(&config.plan_path, plan)?;
        }
    }

    if phase_rank(plan.phase) <= phase_rank(MissionPhase::ReadyToResolve) {
        set_phase(config, plan, MissionPhase::ReadyToResolve)?;
        verify_event_requirements(client, plan).await?;
    }

    if phase_rank(plan.phase) <= phase_rank(MissionPhase::Resolving) {
        set_phase(config, plan, MissionPhase::Resolving)?;
        resolve_event(client, config, plan).await?;
    }

    if phase_rank(plan.phase) <= phase_rank(MissionPhase::CollectingRewards) {
        set_phase(config, plan, MissionPhase::CollectingRewards)?;
        recover_rewards(client, config, plan).await?;
    }

    if phase_rank(plan.phase) <= phase_rank(MissionPhase::Returning) {
        set_phase(config, plan, MissionPhase::Returning)?;
        return_mission_assets(client, config, plan).await?;
    }

    if phase_rank(plan.phase) <= phase_rank(MissionPhase::CleaningUp) {
        set_phase(config, plan, MissionPhase::CleaningUp)?;
        cleanup_claims(client, config, plan).await?;
    }

    plan.phase = if plan.execution.warnings.is_empty() {
        MissionPhase::Completed
    } else {
        MissionPhase::CompletedWithWarnings
    };
    save_plan(&config.plan_path, plan)?;
    println!(
        "Mission {} completed{}.",
        plan.mission_id,
        if plan.execution.warnings.is_empty() {
            ""
        } else {
            " with warnings"
        }
    );
    for warning in &plan.execution.warnings {
        println!("Warning: {warning}");
    }
    Ok(())
}

fn phase_rank(phase: MissionPhase) -> u8 {
    match phase {
        MissionPhase::Planned => 0,
        MissionPhase::Manufacturing => 1,
        MissionPhase::ClaimingTransports => 2,
        MissionPhase::PreparingFleet => 3,
        MissionPhase::Outbound => 4,
        MissionPhase::Staging => 5,
        MissionPhase::InstallingBeacon => 6,
        MissionPhase::ReadyToResolve => 7,
        MissionPhase::Resolving => 8,
        MissionPhase::CollectingRewards => 9,
        MissionPhase::Returning => 10,
        MissionPhase::CleaningUp => 11,
        MissionPhase::Completed | MissionPhase::CompletedWithWarnings | MissionPhase::Cancelled => {
            12
        }
    }
}

fn set_phase(
    config: &Config,
    plan: &mut EventMissionPlan,
    phase: MissionPhase,
) -> AnyResult<()> {
    if phase_rank(plan.phase) <= phase_rank(phase) {
        info!(
            mission_id = %plan.mission_id,
            phase = ?phase,
            "event mission phase"
        );
        plan.phase = phase;
        save_plan(&config.plan_path, plan)?;
    }
    Ok(())
}

fn initialize_execution(plan: &mut EventMissionPlan) {
    if plan.execution.print_batches.is_empty() {
        plan.execution.print_batches = plan
            .selected_criterion
            .print_schedule
            .batches
            .iter()
            .map(|batch| {
                let role = role_for_device_type(&batch.device_type).to_owned();
                ExecutionPrintBatch {
                    factory_code: batch.factory_code.clone(),
                    device_type: batch.device_type.clone(),
                    quantity: batch.quantity,
                    role,
                    batch_tag: print_batch_tag(
                        &plan.mission_id,
                        &batch.factory_code,
                        batch.sequence,
                        &batch.device_type,
                    ),
                    submission_started: false,
                    submitted: false,
                    operation_id: None,
                    produced_codes: Vec::new(),
                }
            })
            .collect();
    }
    plan.version = plan.version.max(2);
}

fn role_for_device_type(device_type: &str) -> &'static str {
    match device_type {
        CARGO_FREIGHTER => "cargo",
        FTL_BEACON => "beacon",
        "surge_plate" | "surge_platform" | "surge_carrier" | "mobile_fleet" => "carrier",
        _ => "payload",
    }
}

fn print_batch_tag(
    mission_id: &str,
    factory_code: &str,
    sequence: usize,
    device_type: &str,
) -> String {
    format!(
        "evt-b:{:016x}",
        stable_hash(&format!(
            "{mission_id}:{factory_code}:{sequence}:{device_type}"
        ))
    )
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}


async fn reconcile_print_batches(client: &Client, plan: &mut EventMissionPlan) -> AnyResult<()> {
    let batch_tags = plan
        .execution
        .print_batches
        .iter()
        .map(|batch| batch.batch_tag.clone())
        .collect::<BTreeSet<_>>();
    let handles = client
        .devices()
        .refresh_many()
        .with_tag(plan.mission_tag.clone())
        .page_size(50)
        .collect()
        .await?;
    let mut produced = BTreeMap::<String, Vec<String>>::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        let matching = snapshot
            .tags
            .iter()
            .filter(|tag| batch_tags.contains(*tag))
            .cloned()
            .collect::<BTreeSet<_>>();
        if matching.len() > 1 {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "mission-tagged device {} matches multiple print batches",
                    handle.id().as_str()
                ),
            ));
        }
        if let Some(batch_tag) = matching.into_iter().next() {
            produced
                .entry(batch_tag)
                .or_default()
                .push(handle.id().as_str().to_owned());
        }
    }

    let factory_codes = plan
        .execution
        .print_batches
        .iter()
        .map(|batch| batch.factory_code.clone())
        .collect::<BTreeSet<_>>();
    let mut factory_jobs = BTreeMap::<String, Vec<BTreeSet<String>>>::new();
    for code in factory_codes {
        factory_jobs.insert(code.clone(), factory_job_tags(client, &code).await?);
    }

    for batch in &mut plan.execution.print_batches {
        let mut codes = produced.remove(&batch.batch_tag).unwrap_or_default();
        codes.sort();
        codes.dedup();
        if i64::try_from(codes.len())? > batch.quantity {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "print batch {} produced {} devices for quantity {}",
                    batch.batch_tag,
                    codes.len(),
                    batch.quantity
                ),
            ));
        }
        batch.produced_codes = codes;
        let queued = factory_jobs
            .get(&batch.factory_code)
            .is_some_and(|jobs| {
                jobs.iter().any(|tags| {
                    tags.contains(&plan.mission_tag) && tags.contains(&batch.batch_tag)
                })
            });
        if queued || i64::try_from(batch.produced_codes.len())? == batch.quantity {
            batch.submission_started = true;
            batch.submitted = true;
            continue;
        }

        if let Some(operation_id) = batch.operation_id.clone() {
            let operation = client.operations().get(OperationId::from(operation_id));
            let outcome = operation.outcome().await?;
            if matches!(
                outcome.status,
                OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
            ) {
                batch.submission_started = false;
                batch.submitted = false;
                batch.operation_id = None;
            } else {
                batch.submission_started = true;
                batch.submitted = true;
            }
        }
    }

    if let Some(batch) = plan.execution.print_batches.iter().find(|batch| {
        batch.submission_started
            && !batch.submitted
            && batch.operation_id.is_none()
            && batch.produced_codes.is_empty()
    }) {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "print submission for batch {} began before interruption, but no durable operation, queue entry, or output is visible; refusing automatic resubmission",
                batch.batch_tag
            ),
        ));
    }
    Ok(())
}

async fn factory_job_tags(
    client: &Client,
    factory_code: &str,
) -> AnyResult<Vec<BTreeSet<String>>> {
    let detail = client.raw().devices().get(factory_code).await?.value;
    let mut jobs = Vec::new();
    if let Some(printing) = detail.printing {
        jobs.push(printing.tags.into_iter().collect());
    }
    for queued in detail.print_queue {
        let mut tags = BTreeSet::new();
        collect_tags(&Value::Object(queued), &mut tags);
        jobs.push(tags);
    }
    Ok(jobs)
}

fn collect_tags(value: &Value, tags: &mut BTreeSet<String>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_tags(item, tags);
            }
        }
        Value::Object(object) => {
            if let Some(Value::Array(values)) = object.get("tags") {
                for value in values {
                    if let Some(tag) = value.as_str() {
                        tags.insert(tag.to_owned());
                    }
                }
            }
            for value in object.values() {
                collect_tags(value, tags);
            }
        }
        _ => {}
    }
}

async fn ensure_home_resources(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    let inventory = fetch_inventory(client, &plan.home_location).await?;
    let live_resources = live_remaining_requirements(client, plan).await?.resources;
    let mut required = live_resources.clone();
    merge_resources(
        &mut required,
        &plan.selected_criterion.manufacturing_resources,
    );
    let shortages = resource_shortages(&inventory, &required);
    if shortages.is_empty() {
        return Ok(());
    }

    if plan.selected_criterion.beacon.transport_slots > 0 {
        let blueprints = fetch_blueprints(client).await?;
        let mut optional_prints = BTreeMap::new();
        if matches!(
            plan.selected_criterion.beacon.action,
            BeaconAction::PrintAndTransport
        ) {
            optional_prints.insert(FTL_BEACON.to_owned(), 1_i64);
        }
        if device_requirement_total(&plan.selected_criterion.remaining_devices) == 0 {
            for transport in plan
                .selected_criterion
                .carriers
                .transports
                .iter()
                .filter(|transport| transport.must_print)
            {
                *optional_prints
                    .entry(transport.device_type.clone())
                    .or_default() += 1;
            }
        }

        let mut optional_resources = ResourceMap::new();
        for (device_type, quantity) in &optional_prints {
            merge_resources(
                &mut optional_resources,
                &blueprint_resource_cost(device_type, *quantity, &blueprints)?,
            );
        }
        let mut critical_manufacturing = plan.selected_criterion.manufacturing_resources.clone();
        subtract_resources(&mut critical_manufacturing, &optional_resources);
        let mut critical_required = live_resources;
        merge_resources(&mut critical_required, &critical_manufacturing);
        if resource_shortages(&inventory, &critical_required).is_empty() {
            let warning = format!(
                "FTL beacon objective skipped because home inventory is short: {}",
                format_resource_map(&shortages)
            );
            disable_optional_beacon(plan, &optional_prints, &optional_resources, warning);
            save_plan(&config.plan_path, plan)?;
            return Ok(());
        }
    }

    Err(app_error(
        io::ErrorKind::Other,
        format!(
            "home inventory at {} cannot fund the mission: {}",
            plan.home_location,
            format_resource_map(&shortages)
        ),
    ))
}

fn disable_optional_beacon(
    plan: &mut EventMissionPlan,
    optional_prints: &BTreeMap<String, i64>,
    optional_resources: &ResourceMap,
    warning: String,
) {
    plan.selected_criterion.beacon = replicant_event_planner::BeaconPlan {
        action: BeaconAction::Unavailable,
        device_code: None,
        transport_slots: 0,
        warning: Some(warning.clone()),
    };
    if !plan.selected_criterion.warnings.contains(&warning) {
        plan.selected_criterion.warnings.push(warning.clone());
    }
    if !plan.execution.warnings.contains(&warning) {
        plan.execution.warnings.push(warning);
    }
    for (device_type, quantity) in optional_prints {
        decrement_requirement(
            &mut plan.selected_criterion.print_devices,
            device_type,
            *quantity,
        );
        decrement_print_batches(
            &mut plan.selected_criterion.print_schedule.batches,
            device_type,
            *quantity,
        );
        decrement_execution_batches(
            &mut plan.execution.print_batches,
            device_type,
            *quantity,
        );
    }
    subtract_resources(
        &mut plan.selected_criterion.manufacturing_resources,
        optional_resources,
    );
    plan.selected_criterion.print_schedule.makespan_seconds = plan
        .selected_criterion
        .print_schedule
        .batches
        .iter()
        .map(|batch| batch.projected_finish_seconds)
        .fold(0.0, f64::max);
    if device_requirement_total(&plan.selected_criterion.remaining_devices) == 0 {
        plan.selected_criterion.carriers = replicant_event_planner::TransportPlan::default();
    }
}

fn decrement_requirement(
    requirements: &mut Vec<DeviceRequirement>,
    device_type: &str,
    mut quantity: i64,
) {
    for requirement in requirements
        .iter_mut()
        .filter(|requirement| requirement.device_type == device_type)
    {
        let removed = requirement.count.min(quantity);
        requirement.count -= removed;
        quantity -= removed;
        if quantity == 0 {
            break;
        }
    }
    requirements.retain(|requirement| requirement.count > 0);
}

fn decrement_print_batches(
    batches: &mut Vec<replicant_event_planner::PrintBatch>,
    device_type: &str,
    mut quantity: i64,
) {
    for batch in batches
        .iter_mut()
        .rev()
        .filter(|batch| batch.device_type == device_type)
    {
        let removed = batch.quantity.min(quantity);
        batch.quantity -= removed;
        quantity -= removed;
        if quantity == 0 {
            break;
        }
    }
    batches.retain(|batch| batch.quantity > 0);
}

fn decrement_execution_batches(
    batches: &mut Vec<ExecutionPrintBatch>,
    device_type: &str,
    mut quantity: i64,
) {
    for batch in batches
        .iter_mut()
        .rev()
        .filter(|batch| batch.device_type == device_type && !batch.submission_started)
    {
        let removed = batch.quantity.min(quantity);
        batch.quantity -= removed;
        quantity -= removed;
        if quantity == 0 {
            break;
        }
    }
    batches.retain(|batch| batch.quantity > 0);
}

fn resource_shortages(inventory: &ResourceMap, required: &ResourceMap) -> ResourceMap {
    required
        .iter()
        .filter_map(|(resource, quantity)| {
            let available = *inventory.get(resource).unwrap_or(&0);
            let shortage = quantity.saturating_sub(available);
            (shortage > 0).then_some((resource.clone(), shortage))
        })
        .collect()
}

fn subtract_resources(target: &mut ResourceMap, source: &ResourceMap) {
    for (resource, quantity) in source {
        let remaining = target
            .get(resource)
            .copied()
            .unwrap_or(0)
            .saturating_sub(*quantity);
        if remaining == 0 {
            target.remove(resource);
        } else {
            target.insert(resource.clone(), remaining);
        }
    }
}

async fn submit_print_batches(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    reconcile_print_batches(client, plan).await?;
    save_plan(&config.plan_path, plan)?;
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;

    loop {
        let pending = plan
            .execution
            .print_batches
            .iter()
            .enumerate()
            .filter_map(|(index, batch)| (!batch.submitted).then_some(index))
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }

        let factory_codes = pending
            .iter()
            .map(|index| plan.execution.print_batches[*index].factory_code.clone())
            .collect::<BTreeSet<_>>();
        let mut queue_slots = BTreeMap::new();
        for factory_code in factory_codes {
            queue_slots.insert(
                factory_code.clone(),
                factory_queue_slots(client, &factory_code).await?,
            );
        }

        let mut submitted_any = false;
        for index in pending {
            let factory_code = plan.execution.print_batches[index].factory_code.clone();
            let slots = queue_slots.get(&factory_code).copied().unwrap_or(0);
            if slots == 0 {
                continue;
            }
            {
                let batch = &mut plan.execution.print_batches[index];
                batch.submission_started = true;
            }
            save_plan(&config.plan_path, plan)?;

            let batch = plan.execution.print_batches[index].clone();
            let factory = client.devices().get(&batch.factory_code).await?;
            let operation = factory
                .enqueue_print_with_tags(
                    batch.device_type.clone(),
                    batch.quantity,
                    [
                        plan.mission_tag.clone(),
                        role_tag(&batch.role),
                        batch.batch_tag.clone(),
                    ],
                )
                .await?;
            {
                let current = &mut plan.execution.print_batches[index];
                current.operation_id = Some(operation.id().as_str().to_owned());
                current.submitted = true;
            }
            save_plan(&config.plan_path, plan)?;
            ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
            queue_slots.insert(factory_code, slots - 1);
            submitted_any = true;
        }

        if submitted_any {
            reconcile_print_batches(client, plan).await?;
            save_plan(&config.plan_path, plan)?;
            continue;
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                "timed out waiting for autofactory queue capacity",
            ));
        }
        wait_for_relevant_event(
            &mut watch,
            deadline,
            &["print.completed", "device.print_completed"],
        )
        .await?;
        reconcile_print_batches(client, plan).await?;
        save_plan(&config.plan_path, plan)?;
    }
}

async fn factory_queue_slots(client: &Client, factory_code: &str) -> AnyResult<usize> {
    let detail = client.raw().devices().get(factory_code).await?.value;
    let queue_size = usize::try_from(detail.queue_size.unwrap_or(1).max(1))?;
    Ok(queue_size.saturating_sub(detail.print_queue.len()))
}

async fn wait_for_print_outputs(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    if plan.execution.print_batches.is_empty() {
        return Ok(());
    }
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        reconcile_print_batches(client, plan).await?;
        save_plan(&config.plan_path, plan)?;
        if plan.execution.print_batches.iter().all(|batch| {
            i64::try_from(batch.produced_codes.len()).ok() == Some(batch.quantity)
        }) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let incomplete = plan
                .execution
                .print_batches
                .iter()
                .filter(|batch| {
                    i64::try_from(batch.produced_codes.len()).ok() != Some(batch.quantity)
                })
                .map(|batch| {
                    format!(
                        "{} {}: {}/{}",
                        batch.factory_code,
                        batch.device_type,
                        batch.produced_codes.len(),
                        batch.quantity
                    )
                })
                .collect::<Vec<_>>();
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out waiting for printed mission devices: {}",
                    incomplete.join("; ")
                ),
            ));
        }
        wait_for_relevant_event(
            &mut watch,
            deadline,
            &["print.completed", "device.print_completed"],
        )
        .await?;
    }
}

async fn assign_printed_outputs(
    client: &Client,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    let mut by_type = BTreeMap::<String, Vec<String>>::new();
    for batch in &plan.execution.print_batches {
        by_type
            .entry(batch.device_type.clone())
            .or_default()
            .extend(batch.produced_codes.clone());
    }
    for codes in by_type.values_mut() {
        codes.sort();
        codes.dedup();
    }
    let mut used = BTreeSet::<String>::new();

    assign_transport_placeholders(
        &mut plan.selected_criterion.cargo.transports,
        &by_type,
        &mut used,
    )?;
    assign_transport_placeholders(
        &mut plan.selected_criterion.carriers.transports,
        &by_type,
        &mut used,
    )?;

    if matches!(
        plan.selected_criterion.beacon.action,
        BeaconAction::PrintAndTransport
    ) && plan.selected_criterion.beacon.device_code.is_none()
    {
        let code = next_unused(&by_type, FTL_BEACON, &used).ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                "printed FTL beacon output was not found",
            )
        })?;
        used.insert(code.clone());
        plan.selected_criterion.beacon.device_code = Some(code);
    } else if let Some(code) = &plan.selected_criterion.beacon.device_code {
        used.insert(code.clone());
    }

    if !plan.execution.payload_devices.is_empty() {
        return Ok(());
    }

    let live_devices = fetch_devices(client, &fetch_blueprints(client).await?).await?;
    let types = live_devices
        .into_iter()
        .map(|device| (device.stock.code, device.stock.device_type))
        .collect::<BTreeMap<_, _>>();

    let mut reusable_by_type = BTreeMap::<String, Vec<String>>::new();
    for code in &plan.selected_criterion.reused_devices {
        let device_type = types.get(code).ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("reused event device {code} is no longer visible"),
            )
        })?;
        reusable_by_type
            .entry(device_type.clone())
            .or_default()
            .push(code.clone());
    }
    for codes in reusable_by_type.values_mut() {
        codes.sort();
    }

    for requirement in &plan.selected_criterion.remaining_devices {
        let mut remaining = requirement.count;
        for code in reusable_by_type
            .get(&requirement.device_type)
            .into_iter()
            .flatten()
        {
            if remaining == 0 {
                break;
            }
            used.insert(code.clone());
            plan.execution.payload_devices.push(PayloadDevice {
                code: code.clone(),
                device_type: requirement.device_type.clone(),
                role: "payload".into(),
                delivered: false,
            });
            remaining -= 1;
        }
        while remaining > 0 {
            let code = next_unused(&by_type, &requirement.device_type, &used).ok_or_else(|| {
                app_error(
                    io::ErrorKind::NotFound,
                    format!(
                        "printed {} output is missing for selected criterion",
                        requirement.device_type
                    ),
                )
            })?;
            used.insert(code.clone());
            plan.execution.payload_devices.push(PayloadDevice {
                code,
                device_type: requirement.device_type.clone(),
                role: "payload".into(),
                delivered: false,
            });
            remaining -= 1;
        }
    }

    if let Some(code) = plan.selected_criterion.beacon.device_code.clone()
        && matches!(
            plan.selected_criterion.beacon.action,
            BeaconAction::TransportExisting | BeaconAction::PrintAndTransport
        )
        && !plan
            .execution
            .payload_devices
            .iter()
            .any(|item| item.code == code)
    {
        plan.execution.payload_devices.push(PayloadDevice {
            code,
            device_type: FTL_BEACON.into(),
            role: "beacon".into(),
            delivered: false,
        });
    }
    plan.execution
        .payload_devices
        .sort_by(|left, right| left.code.cmp(&right.code));
    Ok(())
}

fn assign_transport_placeholders(
    transports: &mut [replicant_event_planner::SelectedTransport],
    by_type: &BTreeMap<String, Vec<String>>,
    used: &mut BTreeSet<String>,
) -> AnyResult<()> {
    for transport in transports.iter_mut().filter(|item| item.must_print) {
        if !transport.code.starts_with("<print:") {
            used.insert(transport.code.clone());
            continue;
        }
        let code = next_unused(by_type, &transport.device_type, used).ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("printed transport {} was not found", transport.device_type),
            )
        })?;
        used.insert(code.clone());
        transport.code = code;
    }
    Ok(())
}

fn next_unused(
    by_type: &BTreeMap<String, Vec<String>>,
    device_type: &str,
    used: &BTreeSet<String>,
) -> Option<String> {
    by_type
        .get(device_type)?
        .iter()
        .find(|code| !used.contains(*code))
        .cloned()
}


async fn claim_mission_assets(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    let mut assets = Vec::<(String, String)>::new();
    assets.extend(
        plan.selected_criterion
            .cargo
            .transports
            .iter()
            .map(|transport| (transport.code.clone(), "cargo".into())),
    );
    assets.extend(
        plan.selected_criterion
            .carriers
            .transports
            .iter()
            .map(|transport| (transport.code.clone(), "carrier".into())),
    );
    assets.extend(
        plan.execution
            .payload_devices
            .iter()
            .map(|device| (device.code.clone(), device.role.clone())),
    );
    if !matches!(
        plan.selected_criterion.beacon.action,
        BeaconAction::AlreadyActive | BeaconAction::Unavailable
    ) && let Some(code) = plan.selected_criterion.beacon.device_code.clone()
        && !assets.iter().any(|(existing, _)| existing == &code)
    {
        assets.push((code, "beacon".into()));
    }
    assets.sort();
    assets.dedup();

    for (code, role) in assets {
        claim_device(client, config, plan, &code, &role).await?;
    }
    Ok(())
}

async fn claim_device(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
    code: &str,
    role: &str,
) -> AnyResult<()> {
    let detail = client.raw().devices().get(code).await?.value;
    if role == "cargo" && detail.controller_device_code.is_some() {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("cargo freighter {code} is controlled by an AMI and cannot be claimed"),
        ));
    }
    if role == "carrier" && !detail.attached_devices.is_empty() {
        let attached = detail
            .attached_devices
            .iter()
            .filter_map(reference_code)
            .collect::<Vec<_>>();
        let mission_payload = plan
            .execution
            .payload_devices
            .iter()
            .map(|device| device.code.as_str())
            .collect::<BTreeSet<_>>();
        if attached
            .iter()
            .any(|attached_code| !mission_payload.contains(attached_code.as_str()))
        {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "carrier {code} already has non-mission devices attached: {}",
                    attached.join(", ")
                ),
            ));
        }
    }

    let other_mission = detail
        .tags
        .iter()
        .find(|tag| tag.starts_with("evt-m:") && *tag != &plan.mission_tag)
        .cloned();
    if let Some(tag) = other_mission {
        return Err(app_error(
            io::ErrorKind::AlreadyExists,
            format!("device {code} is already claimed by {tag}"),
        ));
    }

    let desired_tags = [plan.mission_tag.clone(), role_tag(role)];
    let missing_tags = desired_tags
        .iter()
        .filter(|tag| !detail.tags.contains(tag))
        .cloned()
        .collect::<Vec<_>>();
    if let Some(claim) = plan
        .claimed_devices
        .iter_mut()
        .find(|claim| claim.device_code == code)
    {
        claim.role = role.to_owned();
        for tag in &missing_tags {
            if !claim.mission_tags.contains(tag) {
                claim.mission_tags.push(tag.clone());
            }
        }
    } else {
        plan.claimed_devices.push(ClaimedDevice {
            device_code: code.to_owned(),
            role: role.to_owned(),
            original_tags: detail.tags.clone(),
            mission_tags: missing_tags.clone(),
            released: false,
        });
    }
    save_plan(&config.plan_path, plan)?;

    if !missing_tags.is_empty() {
        let operation = client
            .devices()
            .get(code)
            .await?
            .configure(raw::devices::DeviceConfiguration {
                add_tags: Some(missing_tags),
                remove_tags: None,
                tags: None,
            })
            .await?;
        ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
        wait_for_device_snapshot(client, config, code, |device| {
            desired_tags
                .iter()
                .all(|tag| device.tags.iter().any(|existing| existing == tag))
        })
        .await?;
    }

    let snapshot = client.devices().get(code).await?.snapshot().await?;
    let assigned = snapshot
        .relationships
        .assigned_replicant
        .as_ref()
        .map(|replicant| replicant.id.as_str());
    if assigned != Some(plan.selected_replicant.as_str()) {
        let operation = client
            .devices()
            .get(code)
            .await?
            .change_owner(plan.selected_replicant.clone())
            .await?;
        ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
        wait_for_device_snapshot(client, config, code, |device| {
            device
                .relationships
                .assigned_replicant
                .as_ref()
                .is_some_and(|replicant| {
                    replicant.id.as_str() == plan.selected_replicant.as_str()
                })
        })
        .await?;
    }
    Ok(())
}

fn reference_code(value: &Map<String, Value>) -> Option<String> {
    ["device_code", "code", "device"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(Value::as_str))
        .map(str::to_owned)
}

async fn prepare_initial_fleet(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    for code in cargo_codes(plan) {
        ensure_device_at(client, config, &code, &plan.home_location).await?;
        let detail = client.raw().devices().get(&code).await?.value;
        ensure_uncontrolled_cargo(&detail, &code)?;
        if !cargo_map(&detail).is_empty() {
            deposit_all(client, config, &code).await?;
        }
    }

    gather_remote_payload(client, config, plan).await?;

    for code in carrier_codes(plan) {
        ensure_device_at(client, config, &code, &plan.home_location).await?;
        let detail = client.raw().devices().get(&code).await?.value;
        let attached = detail
            .attached_devices
            .iter()
            .filter_map(reference_code)
            .collect::<Vec<_>>();
        if !attached.is_empty() {
            let mission_payload = plan
                .execution
                .payload_devices
                .iter()
                .map(|device| device.code.as_str())
                .collect::<BTreeSet<_>>();
            if attached
                .iter()
                .any(|code| !mission_payload.contains(code.as_str()))
            {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    format!(
                        "carrier {code} contains non-mission attachments: {}",
                        attached.join(", ")
                    ),
                ));
            }
            detach_devices(client, config, &code, &attached).await?;
        }
    }
    Ok(())
}

async fn gather_remote_payload(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    let carriers = carrier_codes(plan);
    for index in 0..plan.execution.payload_devices.len() {
        let code = plan.execution.payload_devices[index].code.clone();
        let detail = client.raw().devices().get(&code).await?.value;
        if let Some(carrier) = detail.attached_to_device_code.clone() {
            if !carriers.contains(&carrier) {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    format!("payload device {code} is attached to non-mission carrier {carrier}"),
                ));
            }
            ensure_device_at(client, config, &carrier, &plan.home_location).await?;
            detach_devices(client, config, &carrier, std::slice::from_ref(&code)).await?;
            continue;
        }
        if detail.location.as_deref() == Some(plan.home_location.as_str())
            || detail.location.as_deref() == Some(plan.event.location.as_str())
        {
            continue;
        }
        let source = detail.location.clone().ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("payload device {code} has no current location"),
            )
        })?;
        let carrier = carriers.first().ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("no surge carrier is available to retrieve {code} from {source}"),
            )
        })?;
        ensure_device_at(client, config, carrier, &source).await?;
        ensure_free_standing(client, config, &code).await?;
        attach_devices(client, config, carrier, std::slice::from_ref(&code)).await?;
        ensure_device_at(client, config, carrier, &plan.home_location).await?;
        detach_devices(client, config, carrier, std::slice::from_ref(&code)).await?;
        wait_for_raw_device(client, config, &code, |device| {
            device.location.as_deref() == Some(plan.home_location.as_str())
                && device.attached_to_device_code.is_none()
        })
        .await?;
    }
    Ok(())
}

async fn ensure_free_standing(
    client: &Client,
    config: &Config,
    code: &str,
) -> AnyResult<()> {
    let detail = client.raw().devices().get(code).await?.value;
    if let Some(attached_to) = detail.attached_to_device_code {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("device {code} is already attached to {attached_to}"),
        ));
    }
    if detail.stowed_in_device_code.is_some() {
        let operation = client.devices().get(code).await?.deploy().await?;
        ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
        wait_for_raw_device(client, config, code, |device| {
            device.stowed_in_device_code.is_none()
        })
        .await?;
    }
    Ok(())
}


async fn deliver_event_resources(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    let cargo = cargo_codes(plan);
    if cargo.is_empty() {
        let remaining = live_remaining_requirements(client, plan).await?;
        if remaining.resources.is_empty() {
            return Ok(());
        }
        return Err(app_error(
            io::ErrorKind::NotFound,
            "event still requires resources but no Cargo Freighter is available",
        ));
    }

    let deadline = Instant::now() + config.wait_timeout;
    loop {
        let mut remaining = live_remaining_requirements(client, plan).await?.resources;
        if remaining.is_empty() {
            return Ok(());
        }
        let before = sum_resources(&remaining);

        for code in &cargo {
            let mut detail = client.raw().devices().get(code).await?.value;
            ensure_uncontrolled_cargo(&detail, code)?;
            if detail.travel.is_some() {
                let destination = planned_device_destination(&detail).ok_or_else(|| {
                    app_error(
                        io::ErrorKind::InvalidData,
                        format!("cargo transport {code} is travelling without a destination"),
                    )
                })?;
                if destination != plan.home_location && destination != plan.event.location {
                    return Err(app_error(
                        io::ErrorKind::Other,
                        format!("cargo transport {code} is travelling to unexpected destination {destination}"),
                    ));
                }
                ensure_device_at(client, config, code, &destination).await?;
                detail = client.raw().devices().get(code).await?.value;
                ensure_uncontrolled_cargo(&detail, code)?;
            }
            if detail.location.as_deref() == Some(plan.event.location.as_str())
                && !cargo_map(&detail).is_empty()
            {
                deposit_all(client, config, code).await?;
                remaining = live_remaining_requirements(client, plan).await?.resources;
                if remaining.is_empty() {
                    return Ok(());
                }
                detail = client.raw().devices().get(code).await?.value;
            }

            if detail.location.as_deref() != Some(plan.home_location.as_str()) {
                ensure_device_at(client, config, code, &plan.home_location).await?;
                detail = client.raw().devices().get(code).await?.value;
            }
            if !cargo_map(&detail).is_empty() {
                deposit_all(client, config, code).await?;
                detail = client.raw().devices().get(code).await?.value;
            }

            let capacity = detail.cargo_capacity.unwrap_or(0);
            if capacity <= 0 {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    format!("cargo transport {code} has no usable cargo capacity"),
                ));
            }
            let manifest = take_manifest(&remaining, capacity);
            if manifest.is_empty() {
                continue;
            }
            info!(
                mission_id = %plan.mission_id,
                transport = %code,
                manifest = %format_resource_map(&manifest),
                "collecting event material manifest"
            );
            collect_resources(client, config, code, &manifest).await?;
            ensure_device_at(client, config, code, &plan.event.location).await?;
            deposit_resources(client, config, code, Some(&manifest)).await?;
            remaining = live_remaining_requirements(client, plan).await?.resources;
            if remaining.is_empty() {
                return Ok(());
            }
            ensure_device_at(client, config, code, &plan.home_location).await?;
        }

        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out delivering event materials; still needed: {}",
                    format_resource_map(&remaining)
                ),
            ));
        }
        if sum_resources(&remaining) >= before {
            sleep(POLL_INTERVAL).await;
        }
    }
}

async fn stage_event_devices(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    let carriers = carrier_codes(plan);
    let deadline = Instant::now() + config.wait_timeout;

    loop {
        let remaining = live_remaining_requirements(client, plan).await?.devices;
        if remaining.is_empty() {
            mark_delivered_payload(client, plan).await?;
            save_plan(&config.plan_path, plan)?;
            return Ok(());
        }
        if carriers.is_empty() {
            return Err(app_error(
                io::ErrorKind::NotFound,
                format!(
                    "event still requires devices ({}) but no Surge Carrier is available",
                    format_device_requirements(&remaining)
                ),
            ));
        }

        let needed = remaining
            .iter()
            .map(|item| (item.device_type.clone(), item.count))
            .collect::<BTreeMap<_, _>>();
        let mut made_progress = false;

        for carrier in &carriers {
            let mut detail = client.raw().devices().get(carrier).await?.value;
            if detail.travel.is_some() {
                let destination = planned_device_destination(&detail).ok_or_else(|| {
                    app_error(
                        io::ErrorKind::InvalidData,
                        format!("carrier {carrier} is travelling without a destination"),
                    )
                })?;
                if destination != plan.home_location && destination != plan.event.location {
                    return Err(app_error(
                        io::ErrorKind::Other,
                        format!("carrier {carrier} is travelling to unexpected destination {destination}"),
                    ));
                }
                ensure_device_at(client, config, carrier, &destination).await?;
                detail = client.raw().devices().get(carrier).await?.value;
            }
            if detail.location.as_deref() == Some(plan.event.location.as_str())
                && !detail.attached_devices.is_empty()
            {
                let attached = detail
                    .attached_devices
                    .iter()
                    .filter_map(reference_code)
                    .collect::<Vec<_>>();
                detach_devices(client, config, carrier, &attached).await?;
                made_progress = true;
            }

            let remaining = live_remaining_requirements(client, plan).await?.devices;
            if remaining.is_empty() {
                mark_delivered_payload(client, plan).await?;
                save_plan(&config.plan_path, plan)?;
                return Ok(());
            }

            ensure_device_at(client, config, carrier, &plan.home_location).await?;
            let detail = client.raw().devices().get(carrier).await?.value;
            if !detail.attached_devices.is_empty() {
                let attached = detail
                    .attached_devices
                    .iter()
                    .filter_map(reference_code)
                    .collect::<Vec<_>>();
                detach_devices(client, config, carrier, &attached).await?;
            }
            let capacity = detail.attach_capacity.unwrap_or(0);
            if capacity <= 0 {
                continue;
            }

            let current_needed = remaining
                .iter()
                .map(|item| (item.device_type.clone(), item.count))
                .collect::<BTreeMap<_, _>>();
            let selected = select_payload_for_trip(plan, &current_needed, capacity);
            if selected.is_empty() {
                continue;
            }
            for code in &selected {
                ensure_free_standing(client, config, code).await?;
            }
            attach_devices(client, config, carrier, &selected).await?;
            ensure_device_at(client, config, carrier, &plan.event.location).await?;
            detach_devices(client, config, carrier, &selected).await?;
            for payload in &mut plan.execution.payload_devices {
                if selected.contains(&payload.code) {
                    payload.delivered = true;
                }
            }
            save_plan(&config.plan_path, plan)?;
            made_progress = true;

            let remaining = live_remaining_requirements(client, plan).await?.devices;
            if remaining.is_empty() {
                return Ok(());
            }
            ensure_device_at(client, config, carrier, &plan.home_location).await?;
        }

        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out staging event devices; still needed: {}",
                    format_device_requirements(
                        &live_remaining_requirements(client, plan).await?.devices
                    )
                ),
            ));
        }
        if !made_progress {
            let available = plan
                .execution
                .payload_devices
                .iter()
                .filter(|device| !device.delivered)
                .map(|device| format!("{} ({})", device.code, device.device_type))
                .collect::<Vec<_>>();
            return Err(app_error(
                io::ErrorKind::NotFound,
                format!(
                    "no planned payload devices can satisfy {}; available payload: {}",
                    format_device_requirements(&remaining),
                    available.join(", ")
                ),
            ));
        }

        let new_remaining = live_remaining_requirements(client, plan).await?.devices;
        if device_requirement_total(&new_remaining) >= device_requirement_total(&remaining)
            && needed == new_remaining
                .iter()
                .map(|item| (item.device_type.clone(), item.count))
                .collect::<BTreeMap<_, _>>()
        {
            sleep(POLL_INTERVAL).await;
        }
    }
}

fn select_payload_for_trip(
    plan: &EventMissionPlan,
    needed: &BTreeMap<String, i64>,
    capacity: i64,
) -> Vec<String> {
    let mut remaining = needed.clone();
    let mut selected = Vec::new();
    let capacity = usize::try_from(capacity.max(0)).unwrap_or(usize::MAX);
    for payload in &plan.execution.payload_devices {
        if payload.role != "payload" || payload.delivered {
            continue;
        }
        let Some(needed_count) = remaining.get_mut(&payload.device_type) else {
            continue;
        };
        if *needed_count <= 0 {
            continue;
        }
        if selected.len() >= capacity {
            break;
        }
        selected.push(payload.code.clone());
        *needed_count -= 1;
    }
    selected
}

async fn mark_delivered_payload(
    client: &Client,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    for payload in &mut plan.execution.payload_devices {
        let detail = match client.raw().devices().get(&payload.code).await {
            Ok(response) => response.value,
            Err(_) => continue,
        };
        if detail.location.as_deref() == Some(plan.event.location.as_str())
            && detail.attached_to_device_code.is_none()
            && detail.stowed_in_device_code.is_none()
        {
            payload.delivered = true;
        }
    }
    Ok(())
}

async fn install_beacon(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    if plan.execution.beacon_completed {
        return Ok(());
    }
    match plan.selected_criterion.beacon.action {
        BeaconAction::Unavailable => {
            let warning = plan
                .selected_criterion
                .beacon
                .warning
                .clone()
                .unwrap_or_else(|| "FTL beacon objective is unavailable".into());
            if !plan.execution.warnings.contains(&warning) {
                plan.execution.warnings.push(warning);
            }
            return Ok(());
        }
        BeaconAction::AlreadyActive => {
            plan.execution.beacon_completed = true;
            save_plan(&config.plan_path, plan)?;
            return Ok(());
        }
        BeaconAction::DeployExisting
        | BeaconAction::TransportExisting
        | BeaconAction::PrintAndTransport => {}
    }

    let code = plan
        .selected_criterion
        .beacon
        .device_code
        .clone()
        .ok_or_else(|| app_error(io::ErrorKind::NotFound, "beacon device code is missing"))?;
    let mut detail = client.raw().devices().get(&code).await?.value;
    if detail.location.as_deref() != Some(plan.event.location.as_str()) {
        let carriers = carrier_codes(plan);
        if let Some(carrier) = detail.attached_to_device_code.clone() {
            if !carriers.contains(&carrier) {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    format!("beacon {code} is attached to non-mission carrier {carrier}"),
                ));
            }
            ensure_device_at(client, config, &carrier, &plan.event.location).await?;
            detach_devices(client, config, &carrier, std::slice::from_ref(&code)).await?;
        } else {
            let carrier = carriers.into_iter().next().ok_or_else(|| {
                app_error(
                    io::ErrorKind::NotFound,
                    "beacon needs transport but no Surge Carrier is available",
                )
            })?;
            let source = detail.location.clone().ok_or_else(|| {
                app_error(
                    io::ErrorKind::InvalidData,
                    format!("beacon {code} has no current location"),
                )
            })?;
            ensure_device_at(client, config, &carrier, &source).await?;
            ensure_free_standing(client, config, &code).await?;
            attach_devices(client, config, &carrier, std::slice::from_ref(&code)).await?;
            ensure_device_at(client, config, &carrier, &plan.event.location).await?;
            detach_devices(client, config, &carrier, std::slice::from_ref(&code)).await?;
        }
        detail = client.raw().devices().get(&code).await?.value;
    }

    if detail.location.as_deref() != Some(plan.event.location.as_str()) {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "beacon {code} is at {:?}, not {}",
                detail.location, plan.event.location
            ),
        ));
    }
    if detail.attached_to_device_code.is_some() || detail.stowed_in_device_code.is_some() {
        ensure_free_standing(client, config, &code).await?;
        detail = client.raw().devices().get(&code).await?.value;
    }
    let deployed = detail.status.as_deref().is_some_and(|status| {
        matches!(
            status.to_ascii_lowercase().as_str(),
            "active" | "beaconing" | "deployed" | "monitoring"
        )
    });
    if !deployed {
        if !detail.available_commands.is_empty()
            && !detail.available_commands.iter().any(|command| command == "deploy")
        {
            return Err(app_error(
                io::ErrorKind::Other,
                format!("beacon {code} does not currently advertise the deploy command"),
            ));
        }
        let operation = client.devices().get(&code).await?.deploy().await?;
        ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
        wait_for_raw_device(client, config, &code, |device| {
            device.location.as_deref() == Some(plan.event.location.as_str())
                && device.attached_to_device_code.is_none()
                && device.stowed_in_device_code.is_none()
                && device.status.as_deref().is_some_and(|status| {
                    matches!(
                        status.to_ascii_lowercase().as_str(),
                        "active" | "beaconing" | "deployed" | "monitoring"
                    )
                })
        })
        .await?;
    }
    plan.execution.beacon_completed = true;
    save_plan(&config.plan_path, plan)?;
    Ok(())
}


async fn live_remaining_requirements(
    client: &Client,
    plan: &EventMissionPlan,
) -> AnyResult<replicant_event_planner::RemainingRequirements> {
    let Some(event) = fetch_event_definition(client, &plan.event.designation, "active").await? else {
        if fetch_event_definition(client, &plan.event.designation, "completed")
            .await?
            .is_some()
        {
            return Ok(replicant_event_planner::RemainingRequirements::default());
        }
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!("event {} is no longer visible", plan.event.designation),
        ));
    };
    let inventory = fetch_inventory(client, &plan.event.location).await?;
    let devices = fetch_location_device_stock(client, &plan.event.location).await?;
    Ok(replicant_event_planner::remaining_requirements(
        &event,
        &plan.selected_criterion.criterion_name,
        &inventory,
        &devices,
    )?)
}

async fn fetch_event_definition(
    client: &Client,
    designation: &str,
    status: &str,
) -> AnyResult<Option<replicant_event_planner::EventDefinition>> {
    let mut cursor = None;
    for _ in 0..100 {
        let response = client
            .raw()
            .accounts()
            .events(&raw::accounts::AccountEventsQuery {
                status: Some(status.to_owned()),
                cursor,
                limit: Some(100),
            })
            .await?
            .value;
        if let Some(event) = response.events.iter().find(|event| {
            event
                .designation
                .as_deref()
                .is_some_and(|value| value == designation)
        }) {
            return Ok(Some(normalize_event(event)?));
        }
        let Some(next) = response.next_cursor else {
            return Ok(None);
        };
        cursor = Some(next);
    }
    Err(app_error(
        io::ErrorKind::InvalidData,
        "event lookup exceeded the 100-page safety bound",
    ))
}

async fn fetch_location_device_stock(
    client: &Client,
    location: &str,
) -> AnyResult<Vec<DeviceStock>> {
    let blueprints = fetch_blueprints(client).await?;
    let mut cursor = None;
    let mut result = Vec::new();
    for _ in 0..100 {
        let response = client
            .raw()
            .devices()
            .list(&raw::devices::DeviceListQuery {
                replicant_code: None,
                device_type: None,
                tag: None,
                untagged: None,
                location: Some(location.to_owned()),
                cursor,
                limit: Some(50),
            })
            .await?
            .value;
        for device in response.devices {
            let Some(code) = device.device_code else {
                continue;
            };
            let Some(device_type) = device.device_type else {
                continue;
            };
            let blueprint = blueprints.get(&device_type);
            result.push(DeviceStock {
                code,
                device_type,
                status: device.status,
                location: device.location,
                assigned_replicant: device.replicant_code,
                tags: device.tags.into_iter().collect(),
                cargo_capacity: device
                    .cargo_capacity
                    .or_else(|| blueprint.map(|item| item.cargo_capacity))
                    .unwrap_or(0),
                attach_capacity: device
                    .attach_capacity
                    .or_else(|| blueprint.map(|item| item.attach_capacity))
                    .unwrap_or(0),
                attach_used: i64::try_from(device.attached_devices.len())?,
                controlled_by_ami: device.controller_device_code.is_some(),
                travelling: device.travel.is_some(),
            });
        }
        let Some(next) = response.next_cursor else {
            return Ok(result);
        };
        cursor = Some(next);
    }
    Err(app_error(
        io::ErrorKind::InvalidData,
        "location device listing exceeded the 100-page safety bound",
    ))
}

async fn verify_event_requirements(
    client: &Client,
    plan: &EventMissionPlan,
) -> AnyResult<()> {
    let remaining = live_remaining_requirements(client, plan).await?;
    if remaining.resources.is_empty() && remaining.devices.is_empty() {
        Ok(())
    } else {
        Err(app_error(
            io::ErrorKind::Other,
            format!(
                "event requirements are not staged: materials {}; devices {}",
                format_resource_map(&remaining.resources),
                format_device_requirements(&remaining.devices)
            ),
        ))
    }
}

async fn resolve_event(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    if plan.execution.event_resolved
        || fetch_event_definition(client, &plan.event.designation, "completed")
            .await?
            .is_some()
    {
        plan.execution.event_resolved = true;
        save_plan(&config.plan_path, plan)?;
        return Ok(());
    }

    verify_event_requirements(client, plan).await?;
    travel_replicant_to(
        client,
        config,
        &plan.selected_replicant,
        &plan.event.location,
    )
    .await?;

    if plan.execution.reward_home_baseline.is_none() {
        plan.execution.reward_home_baseline =
            Some(fetch_inventory(client, &plan.home_location).await?);
        save_plan(&config.plan_path, plan)?;
    }

    let operation = client
        .location_events()
        .resolve(&plan.event.location, &plan.event.designation)
        .await?;
    ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
    wait_for_event_completion(client, config, &plan.event.designation).await?;
    plan.execution.event_resolved = true;
    save_plan(&config.plan_path, plan)?;
    Ok(())
}

async fn wait_for_event_completion(
    client: &Client,
    config: &Config,
    designation: &str,
) -> AnyResult<()> {
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        if fetch_event_definition(client, designation, "completed")
            .await?
            .is_some()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for event {designation} to complete"),
            ));
        }
        wait_for_relevant_event(&mut watch, deadline, &["event.completed"]).await?;
    }
}

async fn recover_rewards(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    if plan.event.rewards.resources.is_empty() {
        return Ok(());
    }
    let cargo = cargo_codes(plan);
    if cargo.is_empty() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            "resource rewards exist but no Cargo Freighter is available",
        ));
    }
    if plan.execution.reward_home_baseline.is_none() {
        plan.execution.reward_home_baseline =
            Some(fetch_inventory(client, &plan.home_location).await?);
        save_plan(&config.plan_path, plan)?;
    }

    start_replicant_travel_to(
        client,
        &plan.selected_replicant,
        &plan.home_location,
    )
    .await?;

    let deadline = Instant::now() + config.wait_timeout;
    loop {
        for code in &cargo {
            let mut detail = client.raw().devices().get(code).await?.value;
            ensure_uncontrolled_cargo(&detail, code)?;
            if detail.travel.is_some() {
                let destination = planned_device_destination(&detail).ok_or_else(|| {
                    app_error(
                        io::ErrorKind::InvalidData,
                        format!("cargo transport {code} is travelling without a destination"),
                    )
                })?;
                if destination != plan.home_location && destination != plan.event.location {
                    return Err(app_error(
                        io::ErrorKind::Other,
                        format!("cargo transport {code} is travelling to unexpected destination {destination}"),
                    ));
                }
                ensure_device_at(client, config, code, &destination).await?;
                detail = client.raw().devices().get(code).await?.value;
                ensure_uncontrolled_cargo(&detail, code)?;
            }
            let carried = cargo_map(&detail);
            if detail.location.as_deref() == Some(plan.home_location.as_str())
                && !carried.is_empty()
            {
                deposit_all(client, config, code).await?;
            } else if !carried.is_empty() {
                ensure_device_at(client, config, code, &plan.home_location).await?;
                deposit_all(client, config, code).await?;
            }
        }

        let remaining = reward_remaining_at_home(client, plan).await?;
        if remaining.is_empty() {
            info!(
                mission_id = %plan.mission_id,
                "all event rewards recovered at home"
            );
            return Ok(());
        }

        info!(
            mission_id = %plan.mission_id,
            remaining = %format_resource_map(&remaining),
            "recovering event rewards"
        );
        let before = sum_resources(&remaining);
        for code in &cargo {
            ensure_device_at(client, config, code, &plan.event.location).await?;
            let detail = client.raw().devices().get(code).await?.value;
            ensure_uncontrolled_cargo(&detail, code)?;
            if !cargo_map(&detail).is_empty() {
                ensure_device_at(client, config, code, &plan.home_location).await?;
                deposit_all(client, config, code).await?;
                continue;
            }
            let capacity = detail.cargo_capacity.unwrap_or(0);
            let current_remaining = reward_remaining_at_home(client, plan).await?;
            let manifest = take_manifest(&current_remaining, capacity);
            if manifest.is_empty() {
                continue;
            }
            info!(
                mission_id = %plan.mission_id,
                transport = %code,
                manifest = %format_resource_map(&manifest),
                "collecting reward manifest"
            );
            collect_resources(client, config, code, &manifest).await?;
            ensure_device_at(client, config, code, &plan.home_location).await?;
            deposit_all(client, config, code).await?;
            if reward_remaining_at_home(client, plan).await?.is_empty() {
                return Ok(());
            }
        }

        let remaining = reward_remaining_at_home(client, plan).await?;
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out recovering event rewards; still missing at home: {}",
                    format_resource_map(&remaining)
                ),
            ));
        }
        if sum_resources(&remaining) >= before {
            sleep(POLL_INTERVAL).await;
        }
    }
}

async fn reward_remaining_at_home(
    client: &Client,
    plan: &EventMissionPlan,
) -> AnyResult<ResourceMap> {
    let baseline = plan
        .execution
        .reward_home_baseline
        .as_ref()
        .ok_or_else(|| app_error(io::ErrorKind::InvalidData, "reward baseline is missing"))?;
    let current = fetch_inventory(client, &plan.home_location).await?;
    Ok(plan
        .event
        .rewards
        .resources
        .iter()
        .filter_map(|(resource, reward)| {
            let baseline_quantity = *baseline.get(resource).unwrap_or(&0);
            let current_quantity = *current.get(resource).unwrap_or(&0);
            let recovered = current_quantity.saturating_sub(baseline_quantity);
            let remaining = reward.saturating_sub(recovered);
            (remaining > 0).then_some((resource.clone(), remaining))
        })
        .collect())
}


async fn return_mission_assets(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    recover_failed_beacon(client, config, plan).await?;

    for code in cargo_codes(plan) {
        ensure_device_at(client, config, &code, &plan.home_location).await?;
        let detail = client.raw().devices().get(&code).await?.value;
        ensure_uncontrolled_cargo(&detail, &code)?;
        if !cargo_map(&detail).is_empty() {
            deposit_all(client, config, &code).await?;
        }
    }

    let mission_payload = plan
        .execution
        .payload_devices
        .iter()
        .map(|device| device.code.as_str())
        .collect::<BTreeSet<_>>();
    for code in carrier_codes(plan) {
        let detail = client.raw().devices().get(&code).await?.value;
        if !detail.attached_devices.is_empty() {
            let attached = detail
                .attached_devices
                .iter()
                .filter_map(reference_code)
                .filter(|attached| mission_payload.contains(attached.as_str()))
                .collect::<Vec<_>>();
            if !attached.is_empty() {
                detach_devices(client, config, &code, &attached).await?;
            }
        }
        ensure_device_at(client, config, &code, &plan.home_location).await?;
    }

    travel_replicant_to(
        client,
        config,
        &plan.selected_replicant,
        &plan.home_location,
    )
    .await?;
    Ok(())
}

async fn recover_failed_beacon(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    if plan.execution.beacon_completed {
        return Ok(());
    }
    let Some(code) = plan.selected_criterion.beacon.device_code.clone() else {
        return Ok(());
    };
    let detail = client.raw().devices().get(&code).await?.value;
    let deployed = detail.status.as_deref().is_some_and(|status| {
        matches!(
            status.to_ascii_lowercase().as_str(),
            "active" | "beaconing" | "deployed" | "monitoring"
        )
    });
    if deployed && detail.location.as_deref() == Some(plan.event.location.as_str()) {
        plan.execution.beacon_completed = true;
        save_plan(&config.plan_path, plan)?;
        return Ok(());
    }
    if detail.location.as_deref() == Some(plan.home_location.as_str())
        && detail.attached_to_device_code.is_none()
        && detail.stowed_in_device_code.is_none()
    {
        return Ok(());
    }

    let carriers = carrier_codes(plan);
    if let Some(carrier) = detail.attached_to_device_code.clone() {
        if !carriers.contains(&carrier) {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("failed beacon {code} is attached to non-mission carrier {carrier}"),
            ));
        }
        ensure_device_at(client, config, &carrier, &plan.home_location).await?;
        detach_devices(client, config, &carrier, std::slice::from_ref(&code)).await?;
    } else {
        let carrier = carriers.into_iter().next().ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("failed beacon {code} is away from home and no Surge Carrier is available"),
            )
        })?;
        let source = detail.location.clone().ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("failed beacon {code} has no current location"),
            )
        })?;
        ensure_device_at(client, config, &carrier, &source).await?;
        ensure_free_standing(client, config, &code).await?;
        attach_devices(client, config, &carrier, std::slice::from_ref(&code)).await?;
        ensure_device_at(client, config, &carrier, &plan.home_location).await?;
        detach_devices(client, config, &carrier, std::slice::from_ref(&code)).await?;
    }
    wait_for_raw_device(client, config, &code, |device| {
        device.location.as_deref() == Some(plan.home_location.as_str())
            && device.attached_to_device_code.is_none()
            && device.stowed_in_device_code.is_none()
    })
    .await
}

async fn cleanup_claims(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    for index in 0..plan.claimed_devices.len() {
        if plan.claimed_devices[index].released {
            continue;
        }
        let claim = plan.claimed_devices[index].clone();
        let detail = match client.raw().devices().get(&claim.device_code).await {
            Ok(response) => response.value,
            Err(error) if claim.role == "payload" && error.status() == Some(404) => {
                plan.claimed_devices[index].released = true;
                save_plan(&config.plan_path, plan)?;
                continue;
            }
            Err(error) => return Err(error.into()),
        };

        match claim.role.as_str() {
            "cargo" => {
                if detail.location.as_deref() != Some(plan.home_location.as_str())
                    || !cargo_map(&detail).is_empty()
                {
                    return Err(app_error(
                        io::ErrorKind::Other,
                        format!(
                            "cargo transport {} is not safely returned and empty",
                            claim.device_code
                        ),
                    ));
                }
            }
            "carrier" => {
                if detail.location.as_deref() != Some(plan.home_location.as_str())
                    || !detail.attached_devices.is_empty()
                {
                    return Err(app_error(
                        io::ErrorKind::Other,
                        format!(
                            "carrier {} is not safely returned and empty",
                            claim.device_code
                        ),
                    ));
                }
            }
            "beacon" => {
                if !plan.execution.beacon_completed
                    && (detail.location.as_deref() != Some(plan.home_location.as_str())
                        || detail.attached_to_device_code.is_some()
                        || detail.stowed_in_device_code.is_some())
                {
                    return Err(app_error(
                        io::ErrorKind::Other,
                        format!(
                            "failed beacon {} has not been recovered to the home hub",
                            claim.device_code
                        ),
                    ));
                }
            }
            "payload" => {}
            _ => {}
        }

        let removable = claim
            .mission_tags
            .iter()
            .filter(|tag| !claim.original_tags.contains(*tag) && detail.tags.contains(*tag))
            .cloned()
            .collect::<Vec<_>>();
        if !removable.is_empty() {
            let operation = client
                .devices()
                .get(&claim.device_code)
                .await?
                .configure(raw::devices::DeviceConfiguration {
                    add_tags: None,
                    remove_tags: Some(removable.clone()),
                    tags: None,
                })
                .await?;
            ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
            wait_for_device_snapshot(client, config, &claim.device_code, |device| {
                removable
                    .iter()
                    .all(|tag| !device.tags.iter().any(|existing| existing == tag))
            })
            .await?;
        }
        plan.claimed_devices[index].released = true;
        save_plan(&config.plan_path, plan)?;
    }
    Ok(())
}

fn cargo_codes(plan: &EventMissionPlan) -> Vec<String> {
    let mut codes = plan
        .selected_criterion
        .cargo
        .transports
        .iter()
        .map(|transport| transport.code.clone())
        .filter(|code| !code.starts_with("<print:"))
        .collect::<Vec<_>>();
    codes.sort();
    codes.dedup();
    codes
}

fn carrier_codes(plan: &EventMissionPlan) -> Vec<String> {
    let mut codes = plan
        .selected_criterion
        .carriers
        .transports
        .iter()
        .map(|transport| transport.code.clone())
        .filter(|code| !code.starts_with("<print:"))
        .collect::<Vec<_>>();
    codes.sort();
    codes.dedup();
    codes
}

fn planned_device_destination(device: &raw::devices::DeviceStatus) -> Option<String> {
    let travel = device.travel.as_ref()?;
    travel
        .final_destination
        .as_ref()
        .or(travel.destination.as_ref())
        .cloned()
}

async fn ensure_device_at(
    client: &Client,
    config: &Config,
    code: &str,
    destination: &str,
) -> AnyResult<()> {
    let detail = client.raw().devices().get(code).await?.value;
    if detail.travel.is_none() && detail.location.as_deref() == Some(destination) {
        return Ok(());
    }
    if let Some(travel) = &detail.travel {
        let planned = travel
            .final_destination
            .as_deref()
            .or(travel.destination.as_deref());
        if planned != Some(destination) {
            return Err(app_error(
                io::ErrorKind::Other,
                format!(
                    "device {code} is already travelling to {:?}, not {destination}",
                    planned
                ),
            ));
        }
    } else {
        let operation = client
            .devices()
            .get(code)
            .await?
            .command(raw::devices::DeviceCommand::Travel {
                destination: destination.to_owned(),
                dry_run: None,
                via: None,
            })
            .await?;
        ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
    }

    wait_for_raw_device(client, config, code, |device| {
        device.travel.is_none() && device.location.as_deref() == Some(destination)
    })
    .await
}

async fn start_replicant_travel_to(
    client: &Client,
    replicant_code: &str,
    destination: &str,
) -> AnyResult<()> {
    let handle = client.replicants().get_owned(replicant_code).await?;
    let snapshot = handle.snapshot().await?;
    if snapshot.travel.is_none()
        && snapshot
            .location
            .as_ref()
            .is_some_and(|location| location.id.as_str() == destination)
    {
        return Ok(());
    }
    if let Some(travel) = &snapshot.travel {
        let planned = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        if planned != Some(destination) {
            return Err(app_error(
                io::ErrorKind::Other,
                format!(
                    "replicant {replicant_code} is already travelling to {:?}, not {destination}",
                    planned
                ),
            ));
        }
        return Ok(());
    }

    info!(
        replicant = %replicant_code,
        destination = %destination,
        "dispatching replicant travel"
    );
    let operation = handle.travel().to(destination).depart().await?;
    ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
    Ok(())
}

async fn travel_replicant_to(
    client: &Client,
    config: &Config,
    replicant_code: &str,
    destination: &str,
) -> AnyResult<()> {
    start_replicant_travel_to(client, replicant_code, destination).await?;

    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        let snapshot = client
            .replicants()
            .get_owned(replicant_code)
            .await?
            .snapshot()
            .await?;
        if snapshot.travel.is_none()
            && snapshot
                .location
                .as_ref()
                .is_some_and(|location| location.id.as_str() == destination)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out travelling replicant to {destination}"),
            ));
        }
        wait_for_relevant_event(&mut watch, deadline, &["travel.arrived"]).await?;
    }
}

async fn collect_resources(
    client: &Client,
    config: &Config,
    code: &str,
    resources: &ResourceMap,
) -> AnyResult<()> {
    if resources.is_empty() {
        return Ok(());
    }
    let before = cargo_map(&client.raw().devices().get(code).await?.value);
    let operation = client
        .devices()
        .get(code)
        .await?
        .command(raw::devices::DeviceCommand::CollectResources {
            resources: resource_json(resources),
        })
        .await?;
    ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
    wait_for_raw_device(client, config, code, |device| {
        let cargo = cargo_map(device);
        resources.iter().all(|(resource, quantity)| {
            cargo.get(resource).copied().unwrap_or(0)
                >= before
                    .get(resource)
                    .copied()
                    .unwrap_or(0)
                    .saturating_add(*quantity)
        })
    })
    .await
}

async fn deposit_all(client: &Client, config: &Config, code: &str) -> AnyResult<()> {
    deposit_resources(client, config, code, None).await
}

async fn deposit_resources(
    client: &Client,
    config: &Config,
    code: &str,
    resources: Option<&ResourceMap>,
) -> AnyResult<()> {
    let before = cargo_map(&client.raw().devices().get(code).await?.value);
    if before.is_empty() {
        return Ok(());
    }
    let requested = resources.cloned().unwrap_or_else(|| before.clone());
    let operation = client
        .devices()
        .get(code)
        .await?
        .command(raw::devices::DeviceCommand::DepositResources {
            resources: resources.map(resource_json),
        })
        .await?;
    ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
    wait_for_raw_device(client, config, code, |device| {
        let cargo = cargo_map(device);
        requested.iter().all(|(resource, quantity)| {
            cargo.get(resource).copied().unwrap_or(0)
                <= before
                    .get(resource)
                    .copied()
                    .unwrap_or(0)
                    .saturating_sub(*quantity)
        })
    })
    .await
}

async fn attach_devices(
    client: &Client,
    config: &Config,
    carrier: &str,
    devices: &[String],
) -> AnyResult<()> {
    if devices.is_empty() {
        return Ok(());
    }
    let operation = client
        .devices()
        .get(carrier)
        .await?
        .attach(raw::devices::TargetsCommand {
            device: None,
            devices: Some(Value::Array(
                devices.iter().cloned().map(Value::String).collect(),
            )),
            target: None,
            targets: None,
        })
        .await?;
    ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
    for code in devices {
        wait_for_raw_device(client, config, code, |device| {
            device.attached_to_device_code.as_deref() == Some(carrier)
        })
        .await?;
    }
    Ok(())
}

async fn detach_devices(
    client: &Client,
    config: &Config,
    carrier: &str,
    devices: &[String],
) -> AnyResult<()> {
    if devices.is_empty() {
        return Ok(());
    }
    let operation = client
        .devices()
        .get(carrier)
        .await?
        .command(raw::devices::DeviceCommand::Detach(
            raw::devices::TargetsCommand {
                device: None,
                devices: Some(Value::Array(
                    devices.iter().cloned().map(Value::String).collect(),
                )),
                target: None,
                targets: None,
            },
        ))
        .await?;
    ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
    for code in devices {
        wait_for_raw_device(client, config, code, |device| {
            device.attached_to_device_code.is_none()
        })
        .await?;
    }
    Ok(())
}

async fn wait_for_device_snapshot(
    client: &Client,
    config: &Config,
    code: &str,
    predicate: impl Fn(&Device) -> bool,
) -> AnyResult<()> {
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        let snapshot = client.devices().get(code).await?.snapshot().await?;
        if predicate(&snapshot) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for device {code}"),
            ));
        }
        wait_for_relevant_event(&mut watch, deadline, &[]).await?;
    }
}

async fn wait_for_raw_device(
    client: &Client,
    config: &Config,
    code: &str,
    predicate: impl Fn(&raw::devices::DeviceStatus) -> bool,
) -> AnyResult<()> {
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        let detail = client.raw().devices().get(code).await?.value;
        if predicate(&detail) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for device {code}"),
            ));
        }
        wait_for_relevant_event(&mut watch, deadline, &[]).await?;
    }
}

async fn wait_for_relevant_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    names: &[&str],
) -> AnyResult<()> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let interval = remaining.min(POLL_INTERVAL);
    match timeout(interval, watch.next()).await {
        Ok(Ok(event)) if names.is_empty() || names.contains(&event.name.as_str()) => Ok(()),
        Ok(Ok(_)) | Err(_) => Ok(()),
        Ok(Err(error)) => {
            warn!(error = %error, "event watcher gap; falling back to authoritative refresh");
            sleep(Duration::from_millis(250)).await;
            Ok(())
        }
    }
}

async fn ensure_operation_accepted(
    operation: &Operation,
    wait: Duration,
) -> AnyResult<()> {
    let outcome = operation.wait_timeout(wait).await?;
    if matches!(
        outcome.status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "operation {} ended as {:?}: {:?}",
                operation.id().as_str(),
                outcome.status,
                outcome.response
            ),
        ));
    }
    Ok(())
}

fn ensure_uncontrolled_cargo(
    device: &raw::devices::DeviceStatus,
    code: &str,
) -> AnyResult<()> {
    if device.controller_device_code.is_some() {
        Err(app_error(
            io::ErrorKind::InvalidData,
            format!("cargo freighter {code} became controlled by an AMI during the mission"),
        ))
    } else {
        Ok(())
    }
}

fn cargo_map(device: &raw::devices::DeviceStatus) -> ResourceMap {
    device
        .cargo
        .iter()
        .filter_map(|item| {
            let resource = item.resource_type.clone()?;
            let quantity = item.quantity.unwrap_or(0);
            (quantity > 0).then_some((resource, quantity))
        })
        .collect()
}

fn resource_json(resources: &ResourceMap) -> raw::JsonObject {
    resources
        .iter()
        .map(|(resource, quantity)| (resource.clone(), Value::from(*quantity)))
        .collect()
}

fn take_manifest(resources: &ResourceMap, capacity: i64) -> ResourceMap {
    let mut free = capacity.max(0);
    let mut result = ResourceMap::new();
    for (resource, quantity) in resources {
        if free == 0 {
            break;
        }
        let amount = (*quantity).min(free);
        if amount > 0 {
            result.insert(resource.clone(), amount);
            free -= amount;
        }
    }
    result
}

fn merge_resources(target: &mut ResourceMap, source: &ResourceMap) {
    for (resource, quantity) in source {
        *target.entry(resource.clone()).or_default() += quantity;
    }
}

fn sum_resources(resources: &ResourceMap) -> i64 {
    resources.values().copied().sum()
}

fn device_requirement_total(requirements: &[DeviceRequirement]) -> i64 {
    requirements.iter().map(|item| item.count).sum()
}

fn format_resource_map(resources: &ResourceMap) -> String {
    if resources.is_empty() {
        return "none".into();
    }
    resources
        .iter()
        .map(|(resource, quantity)| format!("{quantity} {resource}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_device_requirements(requirements: &[DeviceRequirement]) -> String {
    if requirements.is_empty() {
        return "none".into();
    }
    requirements
        .iter()
        .map(|item| format!("{} {}", item.count, item.device_type))
        .collect::<Vec<_>>()
        .join(", ")
}
