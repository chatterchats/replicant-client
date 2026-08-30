use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::future::{join_all, try_join_all};
use replicant_client::{
    AutofactoryPrintOptions, Client, Device, DeviceType, Operation, OperationId, OperationStatus,
    domain::AccessScope, raw,
};
use replicant_event_planner::{
    BeaconAction, DeviceRequirement, DeviceStock, ResourceMap, blueprint_resource_cost,
    mission_tag, plan_event, role_tag,
};
use replicant_printing::{
    PrintRequest,
    managed::{
        QueueOptions, discover_factories, factory_queue_slots,
        fetch_blueprints as fetch_print_blueprints, invalidate_factory_detail_cache,
        queue_print_prerequisites, queue_print_prerequisites_ahead,
    },
};
use replicant_transport::{DeliveryOptions, PayloadDevice as TransportPayloadDevice};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tokio::time::{Instant, sleep, timeout};
use tracing::{info, warn};

use crate::failure::{
    FailureClass, classified_error, device_fetch_is_missing, device_operation_is_missing,
    permanent_classified_error,
};

use super::{
    AnyResult, ClaimedDevice, Config, EVENT_MISSION_TAG_PREFIX, EventMissionPlan, MissionPhase,
    app_error, build_context, fetch_blueprints, fetch_devices, fetch_earned_achievements,
    fetch_inventory, normalize_event, save_plan,
};

const CARGO_FREIGHTER: &str = "cargo_freighter";
const FTL_BEACON: &str = "ftl_beacon";
const POLL_INTERVAL: Duration = Duration::from_secs(5);
const AUTHORITATIVE_POLL_INTERVAL: Duration = Duration::from_secs(30);
const BLOCKED_PREREQUISITE_RECHECK_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Debug, Default)]
pub(crate) struct CampaignReplanReservations {
    pub(crate) device_codes: BTreeSet<String>,
    pub(crate) home_resources: ResourceMap,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct ExecutionState {
    #[serde(default)]
    pub(crate) print_batches: Vec<ExecutionPrintBatch>,
    #[serde(default)]
    pub(crate) payload_devices: Vec<PayloadDevice>,
    #[serde(default)]
    pub(crate) reward_home_baseline: Option<ResourceMap>,
    #[serde(default)]
    pub(crate) reward_recovered: ResourceMap,
    #[serde(default)]
    pub(crate) reward_pending_deposits: BTreeMap<String, ResourceMap>,
    #[serde(default)]
    pub(crate) reward_accounting_initialized: bool,
    #[serde(default)]
    pub(crate) resources_staged: bool,
    #[serde(default)]
    pub(crate) devices_staged: bool,
    #[serde(default)]
    pub(crate) prestage_complete: bool,
    #[serde(default)]
    pub(crate) printer_lanes: Vec<String>,
    #[serde(default)]
    pub(crate) queue_adoption_checked: bool,
    #[serde(default)]
    pub(crate) last_blocked_prerequisite_check_at_ms: Option<i64>,
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
    pub(crate) prerequisites_queued: bool,
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct PrintKickoff {
    pub(crate) submitted: usize,
    pub(crate) pending: usize,
}

pub(crate) async fn kickoff_printing(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
    submission_limit: usize,
) -> AnyResult<PrintKickoff> {
    if plan.phase.is_terminal() || phase_rank(plan.phase) > phase_rank(MissionPhase::Manufacturing)
    {
        return Ok(PrintKickoff::default());
    }
    initialize_execution(plan);
    split_pending_print_batches(plan);
    save_plan(&config.plan_path, plan)?;
    if plan.execution.print_batches.is_empty() {
        return Ok(PrintKickoff::default());
    }

    set_phase(config, plan, MissionPhase::Manufacturing)?;
    reconcile_print_batches(client, plan, false).await?;
    save_plan(&config.plan_path, plan)?;
    if !plan
        .execution
        .print_batches
        .iter()
        .any(|batch| batch.submitted)
    {
        ensure_home_resources(client, config, plan).await?;
    }
    let submitted = submit_available_print_batches(client, config, plan, submission_limit).await?;
    let pending = plan
        .execution
        .print_batches
        .iter()
        .filter(|batch| !batch.submitted)
        .count();
    Ok(PrintKickoff { submitted, pending })
}

/// Prepares an event's existing Cargo Freighters for independent resource staging.
///
/// This performs the durable claim/checkpoint work before a background transport
/// task starts. Once it returns `true`, the transport can run without mutating
/// the mission JSON, allowing the print feeder to keep checkpointing the same
/// mission concurrently.
pub(crate) async fn prepare_campaign_resource_stage(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<bool> {
    if plan.phase.is_terminal() || plan.execution.event_resolved {
        return Ok(false);
    }
    let remaining = live_remaining_requirements(client, plan).await?.resources;
    if remaining.is_empty() {
        plan.execution.resources_staged = true;
        save_plan(&config.plan_path, plan)?;
        return Ok(false);
    }
    if plan
        .selected_criterion
        .cargo
        .transports
        .iter()
        .any(|transport| transport.code.starts_with("<print:"))
    {
        return Ok(false);
    }

    for code in cargo_codes(plan) {
        claim_device(client, config, plan, &code, "cargo").await?;
        ensure_device_at(client, config, &code, &plan.home_location).await?;
        let detail = fetch_raw_device(client, &code).await?;
        ensure_uncontrolled_cargo(&detail, &code)?;
        if !cargo_map(&detail).is_empty() {
            deposit_all(client, config, &code).await?;
        }
    }
    Ok(true)
}

/// Performs only the physical outbound resource delivery. This deliberately
/// does not save the mission file; the campaign scheduler checkpoints the
/// result after the worker joins.
pub(crate) async fn deliver_campaign_resources(
    client: &Client,
    config: &Config,
    plan: &EventMissionPlan,
) -> AnyResult<()> {
    deliver_event_resources(client, config, plan).await
}

/// Verifies and checkpoints completion of a background resource feeder.
pub(crate) async fn confirm_campaign_resources_staged(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    let remaining = live_remaining_requirements(client, plan).await?.resources;
    if !remaining.is_empty() {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "background transport finished but event {} still needs materials: {}",
                plan.event.designation,
                format_resource_map(&remaining)
            ),
        ));
    }
    plan.execution.resources_staged = true;
    save_plan(&config.plan_path, plan)?;
    info!(
        mission = %plan.mission_id,
        event = %plan.event.designation,
        "event resources prestaged independently"
    );
    Ok(())
}

/// Sends an event's resource payload independently of device manufacturing.
///
/// This combined form is used when a mission is being prestaged synchronously.
/// Campaign background workers use the split prepare/deliver/confirm functions
/// above so print checkpointing can continue while the freighter is travelling.
pub(crate) async fn stage_campaign_resources(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<bool> {
    if plan.execution.resources_staged {
        if live_remaining_requirements(client, plan)
            .await?
            .resources
            .is_empty()
        {
            return Ok(true);
        }
        plan.execution.resources_staged = false;
        save_plan(&config.plan_path, plan)?;
    }
    if !prepare_campaign_resource_stage(client, config, plan).await? {
        return Ok(plan.execution.resources_staged);
    }
    deliver_campaign_resources(client, config, plan).await?;
    confirm_campaign_resources_staged(client, config, plan).await?;
    Ok(true)
}

/// Advances one campaign mission through manufacturing reconciliation and
/// independent device logistics, but deliberately leaves the selected
/// replicant free.
///
/// Returns `true` once all event materials/devices (and a transported beacon,
/// when applicable) are physically staged at the event location. A `false`
/// result means printing or a printed transport is still in progress.
pub(crate) async fn prestage_campaign_mission(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
    reservations: &CampaignReplanReservations,
) -> AnyResult<bool> {
    if plan.phase.is_terminal() || plan.execution.event_resolved {
        return Ok(true);
    }
    if reconcile_remote_event_completion(client, config, plan).await? {
        return Ok(true);
    }
    if plan.execution.prestage_complete {
        let remaining = live_remaining_requirements(client, plan).await?;
        if remaining.resources.is_empty() && remaining.devices.is_empty() {
            return Ok(true);
        }
        plan.execution.prestage_complete = false;
        save_plan(&config.plan_path, plan)?;
    }

    let resources_ready = stage_campaign_resources(client, config, plan).await?;

    initialize_execution(plan);
    split_pending_print_batches(plan);
    reconcile_print_batches(client, plan, false).await?;
    save_plan(&config.plan_path, plan)?;

    if plan
        .execution
        .print_batches
        .iter()
        .any(|batch| i64::try_from(batch.produced_codes.len()).ok() != Some(batch.quantity))
    {
        return Ok(false);
    }

    if !campaign_device_staging_started(plan)
        && replan_nonlocal_assets_with_reservations(client, config, plan, reservations).await?
    {
        return Ok(false);
    }

    assign_printed_outputs(client, plan).await?;
    save_plan(&config.plan_path, plan)?;

    if !plan.execution.devices_staged {
        claim_mission_assets(client, config, plan).await?;
        prepare_device_fleet(client, config, plan).await?;
        stage_event_devices(client, config, plan).await?;
        plan.execution.devices_staged = true;
        save_plan(&config.plan_path, plan)?;
    }

    let resources_ready = resources_ready || stage_campaign_resources(client, config, plan).await?;
    if !resources_ready {
        return Ok(false);
    }
    verify_event_requirements(client, plan).await?;
    plan.execution.resources_staged = true;
    plan.execution.devices_staged = true;
    plan.execution.prestage_complete = true;
    save_plan(&config.plan_path, plan)?;
    info!(
        mission = %plan.mission_id,
        event = %plan.event.designation,
        "event payload prestaged; replicant may resolve when selected"
    );
    Ok(true)
}

/// Resolves a campaign event whose logistics have already been prestaged.
/// Reward recovery and asset return are intentionally left to an independent
/// campaign worker so the replicant can immediately continue to another event.
pub(crate) async fn resolve_prestaged_campaign_mission(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
    reservations: &CampaignReplanReservations,
) -> AnyResult<()> {
    if plan.phase.is_terminal() || plan.execution.event_resolved {
        return Ok(());
    }
    if reconcile_remote_event_completion(client, config, plan).await? {
        return Ok(());
    }
    if !prestage_campaign_mission(client, config, plan, reservations).await? {
        return Err(app_error(
            io::ErrorKind::WouldBlock,
            format!(
                "event {} is not ready for the replicant yet",
                plan.event.designation
            ),
        ));
    }

    set_phase(config, plan, MissionPhase::Outbound)?;
    dispatch_replicant_outbound(client, plan).await?;

    set_phase(config, plan, MissionPhase::InstallingBeacon)?;
    if let Err(error) = install_beacon(client, config, plan).await {
        let warning = format!("FTL beacon objective failed: {error}");
        warn!(warning = %warning, "continuing event mission without beacon");
        if !plan.execution.warnings.contains(&warning) {
            plan.execution.warnings.push(warning);
        }
        save_plan(&config.plan_path, plan)?;
    }

    set_phase(config, plan, MissionPhase::ReadyToResolve)?;
    verify_event_requirements(client, plan).await?;
    set_phase(config, plan, MissionPhase::Resolving)?;
    resolve_event(client, config, plan).await?;
    set_phase(config, plan, MissionPhase::CollectingRewards)?;
    Ok(())
}

/// Finishes a resolved campaign event without moving the campaign replicant.
/// Cargo Freighters recover rewards independently, carriers return after their
/// payload has been consumed, and mission claims are then released.
pub(crate) async fn finish_resolved_campaign_mission(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    if plan.phase.is_terminal() {
        return Ok(());
    }
    if !plan.execution.event_resolved
        && fetch_event_definition(
            client,
            &plan.event.location,
            &plan.event.designation,
            "completed",
        )
        .await?
        .is_none()
    {
        return Err(app_error(
            io::ErrorKind::WouldBlock,
            format!("event {} has not resolved yet", plan.event.designation),
        ));
    }
    plan.execution.event_resolved = true;
    save_plan(&config.plan_path, plan)?;

    set_phase(config, plan, MissionPhase::CollectingRewards)?;
    recover_rewards(client, config, plan).await?;
    set_phase(config, plan, MissionPhase::Returning)?;
    return_mission_assets_internal(client, config, plan, false).await?;
    set_phase(config, plan, MissionPhase::CleaningUp)?;
    cleanup_claims(client, config, plan).await?;

    plan.phase = if plan.execution.warnings.is_empty() {
        MissionPhase::Completed
    } else {
        MissionPhase::CompletedWithWarnings
    };
    save_plan(&config.plan_path, plan)?;
    info!(
        mission = %plan.mission_id,
        event = %plan.event.designation,
        "campaign event logistics finished independently of replicant"
    );
    info!(mission = %plan.mission_id, event = %plan.event.designation, "event mission completed");
    Ok(())
}

/// Returns the campaign replicant home once no unresolved event needs it.
pub(crate) async fn return_campaign_replicant_home(
    client: &Client,
    config: &Config,
    replicant: &str,
    home: &str,
) -> AnyResult<()> {
    travel_replicant_to(client, config, replicant, home).await
}

pub(crate) async fn execute_saved_plan(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    if plan.phase.is_terminal() {
        info!(mission = %plan.mission_id, phase = ?plan.phase, "event mission is already terminal");
        return Ok(());
    }
    let _ = reconcile_remote_event_completion(client, config, plan).await?;

    let mut home_scope_replans = 0usize;
    loop {
        initialize_execution(plan);
        split_pending_print_batches(plan);
        save_plan(&config.plan_path, plan)?;

        if phase_rank(plan.phase) <= phase_rank(MissionPhase::Manufacturing) {
            set_phase(config, plan, MissionPhase::Manufacturing)?;
            reconcile_print_batches(client, plan, false).await?;
            save_plan(&config.plan_path, plan)?;
            if !plan
                .execution
                .print_batches
                .iter()
                .any(|batch| batch.submitted)
            {
                ensure_home_resources(client, config, plan).await?;
            }
            submit_print_batches(client, config, plan).await?;
            wait_for_print_outputs(client, config, plan).await?;
            assign_printed_outputs(client, plan).await?;
            save_plan(&config.plan_path, plan)?;
        }

        if phase_rank(plan.phase) <= phase_rank(MissionPhase::PreparingFleet)
            && replan_nonlocal_assets(client, config, plan).await?
        {
            home_scope_replans += 1;
            if home_scope_replans > 1 {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    "event mission still selected nonlocal or reserved assets after replanning",
                ));
            }
            continue;
        }
        break;
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
        dispatch_replicant_outbound(client, plan).await?;
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
    info!(mission = %plan.mission_id, phase = ?plan.phase, "event mission completed");
    for warning in &plan.execution.warnings {
        warn!(mission = %plan.mission_id, warning, "event mission completed with warning");
    }
    Ok(())
}

pub(crate) async fn reconcile_campaign_asset_plan(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
    reservations: &CampaignReplanReservations,
) -> AnyResult<bool> {
    if plan.phase.is_terminal() {
        return Ok(false);
    }
    if reconcile_remote_event_completion(client, config, plan).await? {
        return Ok(false);
    }
    if campaign_device_staging_started(plan) {
        return Ok(false);
    }

    if !plan.execution.resources_staged
        && live_remaining_requirements(client, plan)
            .await?
            .resources
            .is_empty()
    {
        plan.execution.resources_staged = true;
        save_plan(&config.plan_path, plan)?;
        info!(
            mission = %plan.mission_id,
            event = %plan.event.designation,
            "reconciled already-prestaged event resources before asset replanning"
        );
    }
    if !plan.execution.resources_staged
        && plan
            .claimed_devices
            .iter()
            .any(|claim| !claim.released && claim.role == "cargo")
    {
        // An interrupted resource feeder owns its Cargo Freighter already. Let
        // prestaging reconcile/finish that delivery before judging whether the
        // mission's device reservations need to be replanned.
        return Ok(false);
    }

    initialize_execution(plan);
    split_pending_print_batches(plan);
    reconcile_print_batches(client, plan, false).await?;
    save_plan(&config.plan_path, plan)?;
    if plan
        .execution
        .print_batches
        .iter()
        .any(|batch| i64::try_from(batch.produced_codes.len()).ok() != Some(batch.quantity))
    {
        return Ok(false);
    }

    replan_nonlocal_assets_with_reservations(client, config, plan, reservations).await
}

fn campaign_device_staging_started(plan: &EventMissionPlan) -> bool {
    !plan.execution.payload_devices.is_empty()
        || plan.claimed_devices.iter().any(|claim| {
            !claim.released && matches!(claim.role.as_str(), "payload" | "carrier" | "beacon")
        })
}

async fn reconcile_remote_event_completion(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<bool> {
    if plan.phase.is_terminal() {
        return Ok(true);
    }
    if plan.execution.event_resolved {
        if phase_rank(plan.phase) < phase_rank(MissionPhase::CollectingRewards) {
            plan.phase = MissionPhase::CollectingRewards;
            save_plan(&config.plan_path, plan)?;
        }
        return Ok(true);
    }
    if phase_rank(plan.phase) < phase_rank(MissionPhase::Resolving) {
        return Ok(false);
    }
    if fetch_event_definition(
        client,
        &plan.event.location,
        &plan.event.designation,
        "completed",
    )
    .await?
    .is_none()
    {
        return Ok(false);
    }

    plan.execution.event_resolved = true;
    if phase_rank(plan.phase) < phase_rank(MissionPhase::CollectingRewards) {
        plan.phase = MissionPhase::CollectingRewards;
    }
    save_plan(&config.plan_path, plan)?;
    info!(
        mission = %plan.mission_id,
        event = %plan.event.designation,
        "reconciled remotely completed event before validating consumed payload"
    );
    Ok(true)
}

fn subtract_reserved_resources(inventory: &mut ResourceMap, reserved: &ResourceMap) {
    for (resource, quantity) in reserved {
        let remaining = inventory
            .get(resource)
            .copied()
            .unwrap_or(0)
            .saturating_sub((*quantity).max(0));
        if remaining == 0 {
            inventory.remove(resource);
        } else {
            inventory.insert(resource.clone(), remaining);
        }
    }
}

async fn replan_nonlocal_assets(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<bool> {
    replan_nonlocal_assets_with_reservations(
        client,
        config,
        plan,
        &CampaignReplanReservations::default(),
    )
    .await
}

async fn replan_nonlocal_assets_with_reservations(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
    reservations: &CampaignReplanReservations,
) -> AnyResult<bool> {
    let violations = nonlocal_asset_violations(client, plan).await?;
    if violations.is_empty() {
        return Ok(false);
    }

    warn!(
        mission = %plan.mission_id,
        violations = %violations.join("; "),
        "replanning event mission to keep assets at the home hub"
    );

    let event = fetch_event_definition(
        client,
        &plan.event.location,
        &plan.event.designation,
        "active",
    )
    .await?
    .unwrap_or_else(|| plan.event.clone());
    let earned = fetch_earned_achievements(client).await?;
    let mut context = build_context(client, &event, &earned, &plan.home_location).await?;
    for device in &mut context.devices {
        device.tags.remove(&plan.mission_tag);
    }
    context
        .devices
        .retain(|device| !reservations.device_codes.contains(&device.code));
    subtract_reserved_resources(&mut context.home_inventory, &reservations.home_resources);
    if !reservations.device_codes.is_empty() || !reservations.home_resources.is_empty() {
        info!(
            mission = %plan.mission_id,
            protected_devices = reservations.device_codes.len(),
            protected_resources = %format_resource_map(&reservations.home_resources),
            "protecting sibling campaign reservations during event replan"
        );
    }
    let event_plan = plan_event(event, &context)?;
    let criterion_name = plan.selected_criterion.criterion_name.clone();
    let selected_criterion = event_plan
        .criteria
        .iter()
        .find(|criterion| {
            criterion
                .criterion_name
                .eq_ignore_ascii_case(&criterion_name)
        })
        .cloned()
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "event {} no longer exposes criterion {criterion_name:?}",
                    event_plan.event.designation
                ),
            )
        })?;
    if !selected_criterion.feasible {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!(
                "criterion {criterion_name:?} cannot be replanned with home-scoped assets: {}",
                selected_criterion.blockers.join("; ")
            ),
        ));
    }

    release_preflight_claims(client, config, plan).await?;
    let previous_mission_id = plan.mission_id.clone();
    let replanned_mission_id = uuid::Uuid::new_v4().simple().to_string();
    plan.mission_id = replanned_mission_id.clone();
    plan.mission_tag = mission_tag(&event_plan.event.designation);
    plan.event = event_plan.event;
    plan.selected_criterion = selected_criterion;
    plan.grants_unearned_achievement = event_plan.grants_unearned_achievement;
    plan.execution = ExecutionState::default();
    plan.phase = MissionPhase::Planned;
    save_plan(&config.plan_path, plan)?;
    info!(
        previous_mission = %previous_mission_id,
        mission = %plan.mission_id,
        home = %plan.home_location,
        "replanned event mission using home-scoped free stock"
    );
    Ok(true)
}

async fn nonlocal_asset_violations(
    client: &Client,
    plan: &EventMissionPlan,
) -> AnyResult<Vec<String>> {
    let blueprints = fetch_blueprints(client).await?;
    let devices = fetch_devices(client, &blueprints, &plan.home_location).await?;
    let stocks = devices
        .into_iter()
        .map(|device| (device.stock.code.clone(), device.stock))
        .collect::<BTreeMap<_, _>>();
    let mut violations = Vec::new();

    for code in &plan.selected_criterion.reused_devices {
        match stocks.get(code) {
            Some(device) if home_payload_eligible(device, plan) => {}
            Some(device) => violations.push(format!(
                "payload {code} is at {:?}, nested in {:?}/{:?}, or reserved by another workflow",
                device.location, device.attached_to_device_code, device.stowed_in_device_code
            )),
            None => violations.push(format!("payload {code} is no longer visible")),
        }
    }

    for transport in &plan.selected_criterion.cargo.transports {
        if transport.code.starts_with("<print:") {
            // Campaign prestaging validates existing assets before printed
            // transport placeholders are assigned to their completed outputs.
            continue;
        }
        if plan.execution.resources_staged {
            // Resource feeders intentionally leave Cargo Freighters at the
            // event so they can collect rewards later. `stocks` is deliberately
            // home-scoped now, so verify continued ownership from the global
            // local projection instead of mistaking "away from home" for
            // "missing" and triggering a destructive replan.
            let visible = match client.devices().cached(&transport.code) {
                Some(handle) => handle
                    .snapshot()
                    .await
                    .is_ok_and(|device| matches!(device.access, AccessScope::Owned)),
                None => false,
            };
            if !visible {
                violations.push(format!(
                    "cargo transport {} is no longer visible",
                    transport.code
                ));
            }
            continue;
        }
        match stocks.get(&transport.code) {
            Some(device)
                if home_transport_eligible(device, plan)
                    && !device.controlled_by_ami
                    && device.cargo_capacity >= transport.capacity => {}
            Some(device) => violations.push(format!(
                "cargo transport {} is not a free eligible transport in the home system (location {:?})",
                transport.code, device.location
            )),
            None => violations.push(format!(
                "cargo transport {} is no longer visible",
                transport.code
            )),
        }
    }

    for transport in &plan.selected_criterion.carriers.transports {
        if transport.code.starts_with("<print:") {
            // See the Cargo Freighter case above. The completed print is
            // assigned immediately after this preflight validation.
            continue;
        }
        let Some(device) = stocks.get(&transport.code) else {
            violations.push(format!(
                "device carrier {} is no longer visible",
                transport.code
            ));
            continue;
        };
        if !home_transport_eligible(device, plan) || device.attach_capacity < transport.capacity {
            violations.push(format!(
                "device carrier {} is not empty and eligible in the home system (location {:?}, used {})",
                transport.code, device.location, device.attach_used
            ));
            continue;
        }
        if device.attach_used > 0 {
            let detail = fetch_raw_device(client, &transport.code).await?;
            let mission_payload = plan
                .execution
                .payload_devices
                .iter()
                .map(|payload| payload.code.as_str())
                .collect::<BTreeSet<_>>();
            let foreign = detail
                .attached_devices
                .iter()
                .filter_map(reference_code)
                .filter(|code| !mission_payload.contains(code.as_str()))
                .collect::<Vec<_>>();
            if !foreign.is_empty() {
                violations.push(format!(
                    "device carrier {} contains non-mission attachments: {}",
                    transport.code,
                    foreign.join(", ")
                ));
            }
        }
    }

    if let Some(code) = plan.selected_criterion.beacon.device_code.as_deref() {
        let valid = match plan.selected_criterion.beacon.action {
            BeaconAction::AlreadyActive => true,
            BeaconAction::DeployExisting => stocks.get(code).is_some_and(|device| {
                device.location.as_deref() == Some(plan.event.location.as_str())
                    && device.is_inactive()
                    && device.is_free_standing()
                    && !device.is_reserved_for_workflow(
                        EVENT_MISSION_TAG_PREFIX,
                        Some(plan.mission_tag.as_str()),
                    )
            }),
            BeaconAction::TransportExisting | BeaconAction::PrintAndTransport => stocks
                .get(code)
                .is_some_and(|device| home_payload_eligible(device, plan)),
            BeaconAction::Unavailable => true,
        };
        if !valid {
            violations.push(format!(
                "beacon {code} is not eligible at the event or home hub"
            ));
        }
    }

    for payload in &plan.execution.payload_devices {
        if payload.delivered {
            continue;
        }
        match stocks.get(&payload.code) {
            Some(device) if home_payload_eligible(device, plan) => {}
            Some(device) => violations.push(format!(
                "prepared payload {} is not free stock at {} (location {:?})",
                payload.code, plan.home_location, device.location
            )),
            None => violations.push(format!(
                "prepared payload {} is no longer visible",
                payload.code
            )),
        }
    }

    violations.sort();
    violations.dedup();
    Ok(violations)
}

fn home_payload_eligible(device: &DeviceStock, plan: &EventMissionPlan) -> bool {
    let mission_carriers = plan
        .selected_criterion
        .carriers
        .transports
        .iter()
        .map(|transport| transport.code.as_str())
        .collect::<BTreeSet<_>>();
    let attached_to_mission_carrier = device
        .attached_to_device_code
        .as_deref()
        .is_some_and(|carrier| mission_carriers.contains(carrier));
    let position_eligible = if attached_to_mission_carrier {
        device.is_in_same_system_as(&plan.home_location) && device.stowed_in_device_code.is_none()
    } else {
        device.location.as_deref() == Some(plan.home_location.as_str()) && device.is_free_standing()
    };
    position_eligible
        && device.is_inactive()
        && !device
            .is_reserved_for_workflow(EVENT_MISSION_TAG_PREFIX, Some(plan.mission_tag.as_str()))
}

fn home_transport_eligible(device: &DeviceStock, plan: &EventMissionPlan) -> bool {
    device.is_in_same_system_as(&plan.home_location)
        && !device.travelling
        && device.is_free_standing()
        && !device
            .is_reserved_for_workflow(EVENT_MISSION_TAG_PREFIX, Some(plan.mission_tag.as_str()))
}

async fn release_preflight_claims(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    for index in 0..plan.claimed_devices.len() {
        if plan.claimed_devices[index].released {
            continue;
        }
        let claim = plan.claimed_devices[index].clone();
        if claim.role == "payload" && plan.execution.event_resolved {
            // Event requirement devices are consumed by successful resolution.
            // Their disappearance is terminal evidence, not a reason to issue
            // one guaranteed-404 detail request per payload during cleanup.
            plan.claimed_devices[index].released = true;
            save_plan(&config.plan_path, plan)?;
            continue;
        }
        let detail = match client.raw().devices().get(&claim.device_code).await {
            Ok(response) => response.value,
            Err(error) if error.status() == Some(404) => {
                plan.claimed_devices[index].released = true;
                save_plan(&config.plan_path, plan)?;
                continue;
            }
            Err(error) => return Err(error.into()),
        };
        let removable = claim
            .mission_tags
            .iter()
            .filter(|tag| !claim.original_tags.contains(*tag) && detail.tags.contains(*tag))
            .cloned()
            .collect::<Vec<_>>();
        if !removable.is_empty() {
            let handle = match client.devices().cached(&claim.device_code) {
                Some(handle) => handle,
                None => client.devices().get(&claim.device_code).await?,
            };
            let operation = handle
                .configure(raw::devices::DeviceConfiguration {
                    add_tags: None,
                    remove_tags: Some(removable.clone()),
                    tags: None,
                    ..Default::default()
                })
                .await?;
            ensure_operation_accepted(&operation).await?;
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

fn set_phase(config: &Config, plan: &mut EventMissionPlan, phase: MissionPhase) -> AnyResult<()> {
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
        let mut device_ordinals = BTreeMap::<String, usize>::new();
        let mut execution_batches = Vec::new();
        for batch in &plan.selected_criterion.print_schedule.batches {
            let ordinal = device_ordinals
                .entry(batch.device_type.clone())
                .or_default();
            for _ in 0..batch.quantity.max(0) {
                let role = role_for_device_type(&batch.device_type).to_owned();
                execution_batches.push(ExecutionPrintBatch {
                    factory_code: batch.factory_code.clone(),
                    device_type: batch.device_type.clone(),
                    quantity: 1,
                    role,
                    batch_tag: print_batch_tag(&plan.mission_tag, &batch.device_type, *ordinal),
                    prerequisites_queued: false,
                    submission_started: false,
                    submitted: false,
                    operation_id: None,
                    produced_codes: Vec::new(),
                });
                *ordinal += 1;
            }
        }
        plan.execution.print_batches = execution_batches;
    }
    plan.version = plan.version.max(2);
}

fn split_pending_print_batches(plan: &mut EventMissionPlan) -> usize {
    let batches = std::mem::take(&mut plan.execution.print_batches);
    let mut normalized = Vec::with_capacity(batches.len());
    let mut split = 0usize;
    for batch in batches {
        let can_split = batch.quantity > 1
            && !batch.submission_started
            && !batch.submitted
            && batch.operation_id.is_none()
            && batch.produced_codes.is_empty();
        if !can_split {
            normalized.push(batch);
            continue;
        }
        split += 1;
        normalized.extend((0..batch.quantity).map(|unit_index| {
            let mut unit = batch.clone();
            unit.quantity = 1;
            if unit_index > 0 {
                unit.batch_tag = format!(
                    "evt-b:{:016x}",
                    stable_hash(&format!("{}:{unit_index}", batch.batch_tag))
                );
            }
            unit
        }));
    }
    plan.execution.print_batches = normalized;
    split
}

fn role_for_device_type(device_type: &str) -> &'static str {
    match device_type {
        CARGO_FREIGHTER => "cargo",
        FTL_BEACON => "beacon",
        "surge_plate" | "surge_platform" | "surge_carrier" | "mobile_fleet" => "carrier",
        _ => "payload",
    }
}

fn print_batch_tag(mission_tag: &str, device_type: &str, ordinal: usize) -> String {
    format!(
        "evt-b:{:016x}",
        stable_hash(&format!("{mission_tag}:{device_type}:{ordinal}"))
    )
}

fn component_bundle_tag(batch_tag: &str) -> String {
    format!("evt-p:{:016x}", stable_hash(batch_tag))
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

async fn reconcile_print_batches(
    client: &Client,
    plan: &mut EventMissionPlan,
    authoritative: bool,
) -> AnyResult<()> {
    let batch_tags = plan
        .execution
        .print_batches
        .iter()
        .map(|batch| batch.batch_tag.clone())
        .collect::<BTreeSet<_>>();
    let handles = if authoritative {
        client
            .devices()
            .refresh_many()
            .with_tag(plan.mission_tag.clone())
            .page_size(50)
            .collect()
            .await?
    } else {
        client
            .devices()
            .find()
            .owned()
            .with_tag(plan.mission_tag.clone())
            .collect()
            .await?
    };
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

    // A freshly recreated mission uses deterministic event/batch tags. Inspect
    // the home Autofactories once so queued work from a failed predecessor can
    // be adopted instead of submitted again. After that one adoption pass,
    // queue inspection is only needed for the tiny crash window where a
    // submission began but no durable operation ID/submitted checkpoint made
    // it to disk.
    let adopt_predecessor_queue = !plan.execution.queue_adoption_checked;
    let mut factory_codes = plan
        .execution
        .print_batches
        .iter()
        .filter(|batch| {
            batch.submission_started
                && !batch.submitted
                && batch.operation_id.is_none()
                && i64::try_from(batch.produced_codes.len()).ok() != Some(batch.quantity)
        })
        .map(|batch| batch.factory_code.clone())
        .collect::<BTreeSet<_>>();
    if adopt_predecessor_queue && !plan.execution.print_batches.is_empty() {
        let blueprints = fetch_print_blueprints(client).await?;
        factory_codes.extend(
            discover_factories(client, &plan.home_location, &blueprints)
                .await?
                .into_iter()
                .map(|factory| factory.code),
        );
    }
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
        let queued_factory = factory_jobs.iter().find_map(|(factory_code, jobs)| {
            jobs.iter()
                .any(|tags| tags.contains(&plan.mission_tag) && tags.contains(&batch.batch_tag))
                .then(|| factory_code.clone())
        });
        let queued = queued_factory.is_some();
        if let Some(factory_code) = queued_factory {
            batch.factory_code = factory_code;
        }
        let produced = i64::try_from(batch.produced_codes.len())? == batch.quantity;
        if queued || produced {
            if produced {
                batch.prerequisites_queued = true;
            }
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
    if adopt_predecessor_queue {
        plan.execution.queue_adoption_checked = true;
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

async fn factory_job_tags(client: &Client, factory_code: &str) -> AnyResult<Vec<BTreeSet<String>>> {
    let detail = fetch_raw_device(client, factory_code).await?;
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
        decrement_execution_batches(&mut plan.execution.print_batches, device_type, *quantity);
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
    reconcile_print_batches(client, plan, false).await?;
    save_plan(&config.plan_path, plan)?;
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;

    loop {
        let submitted = submit_available_print_batches(client, config, plan, usize::MAX).await?;
        if plan
            .execution
            .print_batches
            .iter()
            .all(|batch| batch.submitted)
        {
            return Ok(());
        }
        if submitted > 0 {
            reconcile_print_batches(client, plan, false).await?;
            save_plan(&config.plan_path, plan)?;
            continue;
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                "timed out waiting for autofactory queue capacity",
            ));
        }
        let wake = wait_for_relevant_event(
            &mut watch,
            deadline,
            &["print.completed", "device.print_completed"],
        )
        .await?;
        if matches!(wake, WaitWake::Poll | WaitWake::Gap) {
            reconcile_print_batches(client, plan, true).await?;
            save_plan(&config.plan_path, plan)?;
        }
        reconcile_print_batches(client, plan, false).await?;
        save_plan(&config.plan_path, plan)?;
    }
}

async fn submit_available_print_batches(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
    submission_limit: usize,
) -> AnyResult<usize> {
    reconcile_print_batches(client, plan, false).await?;
    save_plan(&config.plan_path, plan)?;
    let pending = plan
        .execution
        .print_batches
        .iter()
        .enumerate()
        .filter_map(|(index, batch)| (!batch.submitted).then_some(index))
        .collect::<Vec<_>>();
    if submission_limit == 0 {
        return Ok(0);
    }
    if pending.is_empty() {
        let legacy_or_unknown_topology = plan.execution.print_batches.iter().any(|batch| {
            i64::try_from(batch.produced_codes.len()).ok() != Some(batch.quantity)
                && !batch.prerequisites_queued
        });
        if legacy_or_unknown_topology {
            let _ = prepare_print_prerequisites(client, config, plan, &[]).await?;
        }
        return Ok(0);
    }

    let printing_blueprints = fetch_print_blueprints(client).await?;
    let allowed_lanes = plan
        .execution
        .printer_lanes
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let factories = discover_factories(client, &plan.home_location, &printing_blueprints)
        .await?
        .into_iter()
        .filter(|factory| allowed_lanes.is_empty() || allowed_lanes.contains(&factory.code))
        .collect::<Vec<_>>();
    let mut preparation_slots = factories
        .iter()
        .map(|factory| (factory.code.clone(), factory.available_slots()))
        .collect::<BTreeMap<_, _>>();
    let mut projected_work = factories
        .iter()
        .map(|factory| (factory.code.clone(), factory.remaining_seconds.max(0.0)))
        .collect::<BTreeMap<_, _>>();
    let mut prepared = Vec::new();
    for index in pending {
        if prepared.len() >= submission_limit {
            break;
        }
        let Some(factory_code) = projected_work
            .iter()
            .filter_map(|(factory_code, work)| {
                (preparation_slots.get(factory_code).copied().unwrap_or(0) > 0)
                    .then_some((factory_code, *work))
            })
            .min_by(|(left_code, left_work), (right_code, right_work)| {
                left_work
                    .total_cmp(right_work)
                    .then_with(|| left_code.cmp(right_code))
            })
            .map(|(factory_code, _)| factory_code.clone())
        else {
            break;
        };
        let slots = preparation_slots.get(&factory_code).copied().unwrap_or(0);
        let batch_device_type = plan.execution.print_batches[index].device_type.clone();
        let previous_factory = plan.execution.print_batches[index].factory_code.clone();
        let batch_tag = plan.execution.print_batches[index].batch_tag.clone();
        let duration = printing_blueprints
            .get(&batch_device_type)
            .map_or(0.0, |blueprint| blueprint.print_time_seconds.max(0.0));
        if previous_factory != factory_code {
            info!(
                mission = %plan.mission_id,
                batch = %batch_tag,
                from_factory = %previous_factory,
                to_factory = %factory_code,
                "reassigning unsubmitted event print batch to live Autofactory"
            );
            plan.execution.print_batches[index].factory_code = factory_code.clone();
        }
        prepared.push(index);
        preparation_slots.insert(factory_code.clone(), slots - 1);
        *projected_work.entry(factory_code).or_default() += duration;
    }
    if prepared.is_empty() {
        return Ok(0);
    }
    save_plan(&config.plan_path, plan)?;

    let parent_ready = prepare_print_prerequisites(client, config, plan, &prepared).await?;

    let prepared_factories = prepared
        .iter()
        .filter(|index| parent_ready.contains(index))
        .map(|index| plan.execution.print_batches[*index].factory_code.clone())
        .collect::<BTreeSet<_>>();
    let mut queue_slots = BTreeMap::new();
    for factory_code in prepared_factories {
        queue_slots.insert(
            factory_code.clone(),
            factory_queue_slots(client, &factory_code).await?,
        );
    }

    let modular_device_types = printing_blueprints
        .into_iter()
        .filter_map(|(device_type, blueprint)| blueprint.is_modular().then_some(device_type))
        .collect::<BTreeSet<_>>();

    let mut submitted = 0usize;
    for index in prepared {
        if !parent_ready.contains(&index) {
            continue;
        }
        let factory_code = plan.execution.print_batches[index].factory_code.clone();
        let slots = queue_slots.get(&factory_code).copied().unwrap_or(0);
        if slots == 0 {
            continue;
        }
        let batch = plan.execution.print_batches[index].clone();
        if batch.quantity != 1 {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "pending print batch {} has queue-unsafe quantity {}; recreate the plan or clear its unsubmitted execution batches",
                    batch.batch_tag, batch.quantity
                ),
            ));
        }
        plan.execution.print_batches[index].submission_started = true;
        save_plan(&config.plan_path, plan)?;

        let flatpacked = modular_device_types.contains(&batch.device_type);
        let mut options = AutofactoryPrintOptions::new(1).tags([
            plan.mission_tag.clone(),
            role_tag(&batch.role),
            batch.batch_tag.clone(),
        ]);
        if flatpacked {
            options = options.flatpacked();
        }
        info!(
            factory = %batch.factory_code,
            device_type = %batch.device_type,
            flatpacked,
            "submitting event print batch"
        );
        // Factory planning already populated the selected factory projection.
        let factory = match client.devices().cached(&batch.factory_code) {
            Some(handle) => handle,
            None => client.devices().get(&batch.factory_code).await?,
        };
        let operation = factory
            .enqueue_print_configured(batch.device_type.clone(), options)
            .await?;
        plan.execution.print_batches[index].operation_id = Some(operation.id().as_str().to_owned());
        plan.execution.print_batches[index].submitted = true;
        save_plan(&config.plan_path, plan)?;
        ensure_operation_accepted(&operation).await?;
        invalidate_factory_detail_cache(&batch.factory_code);
        queue_slots.insert(factory_code, slots - 1);
        submitted += 1;
    }
    Ok(submitted)
}

async fn prepare_print_prerequisites(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
    batch_indexes: &[usize],
) -> AnyResult<BTreeSet<usize>> {
    if batch_indexes.is_empty() {
        let now = event_now_millis();
        if plan
            .execution
            .last_blocked_prerequisite_check_at_ms
            .is_some_and(|last| {
                now.saturating_sub(last)
                    < i64::try_from(BLOCKED_PREREQUISITE_RECHECK_INTERVAL.as_millis())
                        .unwrap_or(i64::MAX)
            })
        {
            return Ok(BTreeSet::new());
        }
        plan.execution.last_blocked_prerequisite_check_at_ms = Some(now);
        save_plan(&config.plan_path, plan)?;
        let mut options = QueueOptions::at(plan.home_location.clone());
        options.tags = vec![plan.mission_tag.clone(), role_tag("component")];
        options.poll_interval = POLL_INTERVAL;
        options.wait_timeout = config.wait_timeout;
        info!(
            mission = %plan.mission_id,
            "checking blocked Autofactory prerequisites before waiting for event outputs"
        );
        let report = queue_print_prerequisites(client, &[], &options).await?;
        if !report.components_queued.is_empty() {
            info!(
                mission = %plan.mission_id,
                components = ?report.components_queued,
                reused = ?report.components_reused,
                "recovered blocked event print prerequisites"
            );
        }
        return Ok(BTreeSet::new());
    }

    let printing_blueprints = fetch_print_blueprints(client).await?;
    let factories = discover_factories(client, &plan.home_location, &printing_blueprints).await?;
    let mut ready = BTreeSet::new();

    for index in batch_indexes {
        if plan.execution.print_batches[*index].prerequisites_queued {
            ready.insert(*index);
            continue;
        }
        let batch = plan.execution.print_batches[*index].clone();
        let request = PrintRequest::new(batch.device_type.clone(), batch.quantity);
        let bundle_tag = component_bundle_tag(&batch.batch_tag);
        let mut options = QueueOptions::at(plan.home_location.clone());
        options.tags = vec![
            plan.mission_tag.clone(),
            role_tag("component"),
            bundle_tag.clone(),
        ];
        options.poll_interval = POLL_INTERVAL;
        options.wait_timeout = config.wait_timeout;

        let mut lanes = plan
            .execution
            .printer_lanes
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>();
        if lanes.is_empty() {
            lanes.insert(batch.factory_code.clone());
            if let Some(partner) = factories
                .iter()
                .filter(|factory| factory.code != batch.factory_code)
                .filter(|factory| factory.available_slots() > 0)
                .min_by(|left, right| {
                    left.remaining_seconds
                        .total_cmp(&right.remaining_seconds)
                        .then_with(|| left.code.cmp(&right.code))
                })
            {
                lanes.insert(partner.code.clone());
            }
        }
        options.factory_codes = Some(lanes.clone());

        info!(
            mission = %plan.mission_id,
            batch = %batch.batch_tag,
            device_type = %batch.device_type,
            lanes = ?lanes,
            "queueing event prerequisite bundle ahead of parent"
        );
        let report = queue_print_prerequisites_ahead(client, &[request], &options).await?;
        if !report.queue.components_queued.is_empty() {
            info!(
                mission = %plan.mission_id,
                batch = %batch.batch_tag,
                components = ?report.queue.components_queued,
                "queued event prerequisite bundle work"
            );
        }
        if report.ready_for_parent {
            plan.execution.print_batches[*index].prerequisites_queued = true;
            save_plan(&config.plan_path, plan)?;
            ready.insert(*index);
        } else {
            info!(
                mission = %plan.mission_id,
                batch = %batch.batch_tag,
                "prerequisite bundle is only partially queued; parent remains pending"
            );
        }
    }

    Ok(ready)
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
        reconcile_print_batches(client, plan, false).await?;
        save_plan(&config.plan_path, plan)?;
        if plan
            .execution
            .print_batches
            .iter()
            .all(|batch| i64::try_from(batch.produced_codes.len()).ok() == Some(batch.quantity))
        {
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
        let wake = wait_for_mission_print_event(&mut watch, deadline, &plan.mission_tag).await?;
        if wake == WaitWake::Event {
            // A completed print may satisfy a blocked parent's recursive
            // prerequisite chain. Recheck immediately on evidence; otherwise
            // the persisted fallback probe is limited to once every five minutes.
            plan.execution.last_blocked_prerequisite_check_at_ms = None;
            let _ = prepare_print_prerequisites(client, config, plan, &[]).await?;
        }
        if matches!(wake, WaitWake::Poll | WaitWake::Gap) {
            reconcile_print_batches(client, plan, true).await?;
            let _ = prepare_print_prerequisites(client, config, plan, &[]).await?;
            save_plan(&config.plan_path, plan)?;
        }
    }
}

async fn assign_printed_outputs(client: &Client, plan: &mut EventMissionPlan) -> AnyResult<()> {
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

    let live_devices = fetch_devices(
        client,
        &fetch_blueprints(client).await?,
        &plan.home_location,
    )
    .await?;
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
    let known_owned = client.devices().find().owned().collect().await?;
    if !known_owned
        .iter()
        .any(|handle| handle.id().as_str().eq_ignore_ascii_case(code))
    {
        return Err(classified_error(
            FailureClass::EventAssetStale,
            io::ErrorKind::NotFound,
            format!(
                "event asset {code} is not present in the account-owned device projection; replan required"
            ),
        ));
    }
    let detail = fetch_raw_device(client, code).await?;
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
        claim.released = false;
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

    // The owned-device projection and raw preflight already established this asset.
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    if !missing_tags.is_empty() {
        let operation = handle
            .configure(raw::devices::DeviceConfiguration {
                add_tags: Some(missing_tags),
                remove_tags: None,
                tags: None,
                ..Default::default()
            })
            .await?;
        ensure_operation_accepted(&operation).await?;
        wait_for_device_snapshot(client, config, code, |device| {
            desired_tags
                .iter()
                .all(|tag| device.tags.iter().any(|existing| existing == tag))
        })
        .await?;
    }

    let snapshot = handle.snapshot().await?;
    let assigned = snapshot
        .relationships
        .assigned_replicant
        .as_ref()
        .map(|replicant| replicant.id.as_str());
    if assigned != Some(plan.selected_replicant.as_str()) {
        let operation = handle.change_owner(plan.selected_replicant.clone()).await?;
        ensure_operation_accepted(&operation).await?;
        wait_for_device_snapshot(client, config, code, |device| {
            device
                .relationships
                .assigned_replicant
                .as_ref()
                .is_some_and(|replicant| replicant.id.as_str() == plan.selected_replicant.as_str())
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
    prepare_resource_cargo(client, config, plan).await?;
    prepare_device_fleet(client, config, plan).await
}

async fn prepare_resource_cargo(
    client: &Client,
    config: &Config,
    plan: &EventMissionPlan,
) -> AnyResult<()> {
    for code in cargo_codes(plan) {
        ensure_device_at(client, config, &code, &plan.home_location).await?;
        let detail = fetch_raw_device(client, &code).await?;
        ensure_uncontrolled_cargo(&detail, &code)?;
        if !cargo_map(&detail).is_empty() {
            deposit_all(client, config, &code).await?;
        }
    }
    Ok(())
}

async fn prepare_device_fleet(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    gather_remote_payload(client, config, plan).await?;
    prepare_home_payload_for_attachment(client, config, plan).await?;

    for code in carrier_codes(plan) {
        ensure_device_at(client, config, &code, &plan.home_location).await?;
        let detail = fetch_raw_device(client, &code).await?;
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

async fn prepare_home_payload_for_attachment(
    client: &Client,
    config: &Config,
    plan: &EventMissionPlan,
) -> AnyResult<()> {
    for payload in &plan.execution.payload_devices {
        if payload.delivered {
            continue;
        }
        let detail = fetch_raw_device(client, &payload.code).await?;
        if detail.location.as_deref() == Some(plan.home_location.as_str())
            && detail.attached_to_device_code.is_none()
        {
            ensure_attachable_device(client, config, &payload.code).await?;
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
        let detail = fetch_raw_device(client, &code).await?;
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
        ensure_attachable_device(client, config, &code).await?;
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

async fn ensure_free_standing(client: &Client, config: &Config, code: &str) -> AnyResult<()> {
    let detail = fetch_raw_device(client, code).await?;
    if let Some(attached_to) = detail.attached_to_device_code {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("device {code} is already attached to {attached_to}"),
        ));
    }
    if detail.stowed_in_device_code.is_some() {
        // The free-standing preflight reads projection-backed placement fields.
        let handle = match client.devices().cached(code) {
            Some(handle) => handle,
            None => client.devices().get(code).await?,
        };
        let operation = handle.deploy().await?;
        ensure_operation_accepted(&operation).await?;
        wait_for_raw_device(client, config, code, |device| {
            device.stowed_in_device_code.is_none()
        })
        .await?;
    }
    Ok(())
}

fn is_modular_device(detail: &raw::devices::DeviceStatus) -> bool {
    detail
        .features
        .iter()
        .any(|feature| feature.eq_ignore_ascii_case("modular"))
        || detail
            .available_commands
            .iter()
            .chain(detail.commands.iter())
            .any(|command| matches!(command.as_str(), "compact" | "unfurl"))
        || detail.status.as_deref().is_some_and(|status| {
            matches!(
                status.to_ascii_lowercase().as_str(),
                "compacting" | "compacted" | "unfurling"
            )
        })
}

fn status_is(detail: &raw::devices::DeviceStatus, expected: &str) -> bool {
    detail
        .status
        .as_deref()
        .is_some_and(|status| status.eq_ignore_ascii_case(expected))
}

fn command_available(detail: &raw::devices::DeviceStatus, expected: &str) -> bool {
    detail
        .available_commands
        .iter()
        .chain(detail.commands.iter())
        .any(|command| command.eq_ignore_ascii_case(expected))
}

async fn ensure_attachable_device(client: &Client, config: &Config, code: &str) -> AnyResult<()> {
    ensure_free_standing(client, config, code).await?;
    let mut detail = fetch_raw_device(client, code).await?;
    if !is_modular_device(&detail) || status_is(&detail, "compacted") {
        return Ok(());
    }

    if status_is(&detail, "compacting") {
        wait_for_raw_device(client, config, code, |device| {
            status_is(device, "compacted")
        })
        .await?;
        return Ok(());
    }

    if status_is(&detail, "unfurling") {
        wait_for_raw_device(client, config, code, |device| {
            !status_is(device, "unfurling")
        })
        .await?;
        detail = fetch_raw_device(client, code).await?;
        if status_is(&detail, "compacted") {
            return Ok(());
        }
    }

    if detail.printing.is_some() || !detail.print_queue.is_empty() {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "modular payload {code} must finish its Autofactory work before it can be compacted for event transport"
            ),
        ));
    }
    if !command_available(&detail, "compact") {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "modular payload {code} is {:?} and does not currently advertise compact; it cannot be attached until it is compacted",
                detail.status
            ),
        ));
    }

    info!(device = %code, "compacting modular event payload for carrier attachment");
    // The attachability preflight already established the managed device.
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    let operation = handle.compact().await?;
    ensure_operation_accepted(&operation).await?;
    wait_for_raw_device(client, config, code, |device| {
        status_is(device, "compacted")
            && device.attached_to_device_code.is_none()
            && device.stowed_in_device_code.is_none()
    })
    .await
}

async fn deliver_event_resources(
    client: &Client,
    config: &Config,
    plan: &EventMissionPlan,
) -> AnyResult<()> {
    let remaining = live_remaining_requirements(client, plan).await?.resources;
    if remaining.is_empty() {
        return Ok(());
    }
    let cargo = cargo_codes(plan);
    replicant_transport::deliver_resources_with(
        client,
        &plan.home_location,
        &plan.event.location,
        &remaining,
        &cargo,
        transport_options(config),
    )
    .await?;

    let remaining = live_remaining_requirements(client, plan).await?.resources;
    if !remaining.is_empty() {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "transport delivery finished but the event still needs: {}",
                format_resource_map(&remaining)
            ),
        ));
    }
    Ok(())
}

async fn dispatch_replicant_outbound(client: &Client, plan: &EventMissionPlan) -> AnyResult<()> {
    info!(
        mission_id = %plan.mission_id,
        replicant = %plan.selected_replicant,
        destination = %plan.event.location,
        "dispatching replicant alongside outbound event logistics"
    );
    start_replicant_travel_to(client, &plan.selected_replicant, &plan.event.location).await?;
    Ok(())
}

async fn stage_event_devices(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    let remaining = live_remaining_requirements(client, plan).await?.devices;
    let beacon_needs_transport = if let Some(beacon_code) =
        plan.selected_criterion.beacon.device_code.as_ref()
        && matches!(
            plan.selected_criterion.beacon.action,
            BeaconAction::TransportExisting | BeaconAction::PrintAndTransport
        ) {
        fetch_raw_device(client, beacon_code)
            .await?
            .location
            .as_deref()
            != Some(plan.event.location.as_str())
    } else {
        false
    };
    if remaining.is_empty() && !beacon_needs_transport {
        mark_delivered_payload(client, plan).await?;
        save_plan(&config.plan_path, plan)?;
        return Ok(());
    }

    let needed = remaining
        .iter()
        .map(|item| (item.device_type.clone(), item.count))
        .collect::<BTreeMap<_, _>>();
    let mut selected = select_payload_for_trip(plan, &needed, i64::MAX);
    if let Some(beacon_code) = plan.selected_criterion.beacon.device_code.as_ref()
        && matches!(
            plan.selected_criterion.beacon.action,
            BeaconAction::TransportExisting | BeaconAction::PrintAndTransport
        )
        && !selected.contains(beacon_code)
        && beacon_needs_transport
    {
        selected.push(beacon_code.clone());
    }
    if selected.is_empty() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!(
                "no planned payload devices can satisfy {}",
                format_device_requirements(&remaining)
            ),
        ));
    }

    let payloads = selected
        .iter()
        .filter_map(|code| {
            plan.execution
                .payload_devices
                .iter()
                .find(|payload| payload.code == *code)
                .map(|payload| TransportPayloadDevice {
                    code: payload.code.clone(),
                    device_type: payload.device_type.clone(),
                    origin: plan.home_location.clone(),
                })
        })
        .collect::<Vec<_>>();

    replicant_transport::deliver_devices_with(
        client,
        &plan.event.location,
        &payloads,
        &carrier_codes(plan),
        transport_options(config),
    )
    .await?;

    mark_delivered_payload(client, plan).await?;
    save_plan(&config.plan_path, plan)?;
    let remaining = live_remaining_requirements(client, plan).await?.devices;
    if !remaining.is_empty() {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "transport delivery finished but the event still needs devices: {}",
                format_device_requirements(&remaining)
            ),
        ));
    }
    Ok(())
}

fn transport_options(config: &Config) -> DeliveryOptions {
    DeliveryOptions {
        wait_timeout: config.wait_timeout,
        poll_interval: POLL_INTERVAL,
        unfurl_modular_payload: true,
        return_transports: false,
        ..DeliveryOptions::default()
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

async fn mark_delivered_payload(client: &Client, plan: &mut EventMissionPlan) -> AnyResult<()> {
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
    let mut detail = fetch_raw_device(client, &code).await?;
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
            ensure_attachable_device(client, config, &code).await?;
            attach_devices(client, config, &carrier, std::slice::from_ref(&code)).await?;
            ensure_device_at(client, config, &carrier, &plan.event.location).await?;
            detach_devices(client, config, &carrier, std::slice::from_ref(&code)).await?;
        }
        detail = fetch_raw_device(client, &code).await?;
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
        detail = fetch_raw_device(client, &code).await?;
    }
    let deployed = detail.status.as_deref().is_some_and(|status| {
        matches!(
            status.to_ascii_lowercase().as_str(),
            "active" | "beaconing" | "deployed" | "monitoring"
        )
    });
    if !deployed {
        if !detail.available_commands.is_empty()
            && !detail
                .available_commands
                .iter()
                .any(|command| command == "deploy")
        {
            return Err(app_error(
                io::ErrorKind::Other,
                format!("beacon {code} does not currently advertise the deploy command"),
            ));
        }
        // Event travel and detach waits keep beacon placement current locally.
        let handle = match client.devices().cached(&code) {
            Some(handle) => handle,
            None => client.devices().get(&code).await?,
        };
        let operation = handle.deploy().await?;
        ensure_operation_accepted(&operation).await?;
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
    let Some(event) = fetch_event_definition(
        client,
        &plan.event.location,
        &plan.event.designation,
        "active",
    )
    .await?
    else {
        if fetch_event_definition(
            client,
            &plan.event.location,
            &plan.event.designation,
            "completed",
        )
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
    location: &str,
    designation: &str,
    status: &str,
) -> AnyResult<Option<replicant_event_planner::EventDefinition>> {
    let events = client
        .location_events()
        .list(location, Some(status))
        .await?;
    events
        .iter()
        .find(|event| {
            event
                .designation
                .as_deref()
                .is_some_and(|value| value == designation)
        })
        .map(normalize_event)
        .transpose()
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
                tags: None,
                exclude_tags: None,
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
                attached_to_device_code: device.attached_to_device_code,
                stowed_in_device_code: device.stowed_in_device_code,
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

async fn verify_event_requirements(client: &Client, plan: &EventMissionPlan) -> AnyResult<()> {
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
        || fetch_event_definition(
            client,
            &plan.event.location,
            &plan.event.designation,
            "completed",
        )
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
    if !plan.execution.reward_accounting_initialized {
        plan.execution.reward_accounting_initialized = true;
        save_plan(&config.plan_path, plan)?;
    }

    let operation = client
        .location_events()
        .resolve(&plan.event.location, &plan.event.designation)
        .await?;
    ensure_operation_accepted(&operation).await?;
    wait_for_event_completion(
        client,
        config,
        &plan.event.location,
        &plan.event.designation,
    )
    .await?;
    plan.execution.event_resolved = true;
    save_plan(&config.plan_path, plan)?;
    Ok(())
}

async fn wait_for_event_completion(
    client: &Client,
    config: &Config,
    location: &str,
    designation: &str,
) -> AnyResult<()> {
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        if fetch_event_definition(client, location, designation, "completed")
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
        let poll_deadline = Instant::now() + AUTHORITATIVE_POLL_INTERVAL;
        loop {
            let wake_deadline = deadline.min(poll_deadline);
            let remaining = wake_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, watch.next()).await {
                Ok(Ok(event))
                    if event.name.as_str() == "event.completed"
                        && event
                            .location
                            .as_ref()
                            .is_some_and(|value| value.id.as_str() == location) =>
                {
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(error)) => {
                    warn!(error = %error, "event watcher gap; checking completion authoritatively");
                    break;
                }
                Err(_) => break,
            }
        }
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
    initialize_reward_accounting(client, config, plan).await?;
    reconcile_pending_reward_deposits(client, config, plan).await?;

    let deadline = Instant::now() + config.wait_timeout;
    loop {
        try_join_all(
            cargo
                .iter()
                .map(|code| settle_reward_transport(client, config, plan, code)),
        )
        .await?;

        let carrying_rewards = reward_cargo_is_loaded(client, &plan.mission_tag, &cargo).await?;
        if carrying_rewards {
            return_reward_cargo_home(client, config, plan, &cargo).await?;
            checkpoint_and_deposit_rewards(client, config, plan, &cargo).await?;
        }

        let remaining = reward_remaining(plan);
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
        travel_devices_to(client, config, &cargo, &plan.event.location).await?;

        let mut capacities = Vec::with_capacity(cargo.len());
        for code in &cargo {
            let detail = fetch_raw_device(client, code).await?;
            ensure_uncontrolled_cargo(&detail, code)?;
            if !cargo_map(&detail).is_empty() {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    format!("cargo transport {code} is not empty before reward collection"),
                ));
            }
            let capacity = detail.cargo_capacity.unwrap_or(0);
            if capacity <= 0 {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    format!("cargo transport {code} has no usable cargo capacity"),
                ));
            }
            capacities.push((code.clone(), capacity));
        }

        let available = fetch_inventory(client, &plan.event.location).await?;
        let collectable = resources_available_from(&remaining, &available);
        if collectable.is_empty() {
            return Err(app_error(
                io::ErrorKind::NotFound,
                format!(
                    "event location no longer holds advertised rewards still unaccounted for: {}",
                    format_resource_map(&remaining)
                ),
            ));
        }
        let manifests = allocate_manifests(&collectable, &capacities);
        if manifests.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                "no Cargo Freighter can carry the remaining event rewards",
            ));
        }
        for (code, manifest) in &manifests {
            info!(
                mission_id = %plan.mission_id,
                transport = %code,
                manifest = %format_resource_map(manifest),
                "collecting reward manifest"
            );
        }
        finish_all(
            join_all(
                manifests
                    .iter()
                    .map(|(code, manifest)| collect_resources(client, config, code, manifest)),
            )
            .await,
        )?;

        return_reward_cargo_home(client, config, plan, &cargo).await?;
        checkpoint_and_deposit_rewards(client, config, plan, &cargo).await?;
        if reward_remaining(plan).is_empty() {
            return Ok(());
        }

        let remaining = reward_remaining(plan);
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

async fn reward_cargo_is_loaded(
    client: &Client,
    mission_tag: &str,
    cargo: &[String],
) -> AnyResult<bool> {
    let requested = cargo.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let handles = client
        .devices()
        .refresh_many()
        .with_tag(mission_tag.to_owned())
        .of_type(DeviceType::from(CARGO_FREIGHTER))
        .page_size(50)
        .collect()
        .await?;
    let mut found = BTreeSet::new();
    let mut carrying_rewards = false;
    for handle in handles {
        let code = handle.id().as_str();
        if !requested.contains(code) {
            continue;
        }
        let detail = handle.snapshot().await?;
        if detail.relationships.controller.is_some() {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("cargo freighter {code} became controlled by an AMI during the mission"),
            ));
        }
        carrying_rewards |= !detail.cargo.is_empty();
        found.insert(code.to_owned());
    }
    if found.len() != requested.len() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!(
                "event cargo missing from managed projection: {}",
                requested
                    .into_iter()
                    .filter(|code| !found.contains(*code))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
        ));
    }
    Ok(carrying_rewards)
}

async fn initialize_reward_accounting(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    if plan.execution.reward_accounting_initialized {
        return Ok(());
    }
    let baseline = plan
        .execution
        .reward_home_baseline
        .as_ref()
        .ok_or_else(|| app_error(io::ErrorKind::InvalidData, "reward baseline is missing"))?;
    let current = fetch_inventory(client, &plan.home_location).await?;
    plan.execution.reward_recovered =
        legacy_recovered_rewards(&plan.event.rewards.resources, baseline, &current);
    plan.execution.reward_accounting_initialized = true;
    save_plan(&config.plan_path, plan)?;
    info!(
        mission_id = %plan.mission_id,
        recovered = %format_resource_map(&plan.execution.reward_recovered),
        "initialized durable reward accounting for an existing mission"
    );
    Ok(())
}

async fn checkpoint_and_deposit_rewards(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
    codes: &[String],
) -> AnyResult<()> {
    let mut remaining = reward_remaining(plan);
    for code in codes {
        if plan.execution.reward_pending_deposits.contains_key(code) {
            continue;
        }
        let detail = fetch_raw_device(client, code).await?;
        let manifest = resources_available_from(&remaining, &cargo_map(&detail));
        if manifest.is_empty() {
            continue;
        }
        subtract_resources(&mut remaining, &manifest);
        plan.execution
            .reward_pending_deposits
            .insert(code.clone(), manifest);
    }
    save_plan(&config.plan_path, plan)?;
    reconcile_pending_reward_deposits(client, config, plan).await
}

async fn reconcile_pending_reward_deposits(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    let pending = plan.execution.reward_pending_deposits.clone();
    for (code, manifest) in pending {
        let mut detail = fetch_raw_device(client, &code).await?;
        if !cargo_map(&detail).is_empty()
            && detail.location.as_deref() != Some(plan.home_location.as_str())
        {
            ensure_device_at(client, config, &code, &plan.home_location).await?;
            detail = fetch_raw_device(client, &code).await?;
        }
        if !cargo_map(&detail).is_empty() {
            deposit_all(client, config, &code).await?;
        }
        merge_recovered_rewards(
            &mut plan.execution.reward_recovered,
            &plan.event.rewards.resources,
            &manifest,
        );
        plan.execution.reward_pending_deposits.remove(&code);
        save_plan(&config.plan_path, plan)?;
        info!(
            mission_id = %plan.mission_id,
            transport = %code,
            deposited = %format_resource_map(&manifest),
            recovered = %format_resource_map(&plan.execution.reward_recovered),
            "checkpointed recovered event rewards at home"
        );
    }
    Ok(())
}

async fn settle_reward_transport(
    client: &Client,
    config: &Config,
    plan: &EventMissionPlan,
    code: &str,
) -> AnyResult<()> {
    let detail = fetch_raw_device(client, code).await?;
    ensure_uncontrolled_cargo(&detail, code)?;
    if detail.travel.is_none() {
        return Ok(());
    }
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
    // The raw detail above already proved that this transport is actively
    // travelling to an allowed destination. Do not re-enter ensure_device_at(),
    // which would issue another authoritative device read before waiting.
    wait_for_device_at(client, config, code, &destination).await
}

async fn deposit_all_devices(client: &Client, config: &Config, codes: &[String]) -> AnyResult<()> {
    finish_all(join_all(codes.iter().map(|code| deposit_all(client, config, code))).await)
}

async fn return_reward_cargo_home(
    client: &Client,
    config: &Config,
    plan: &EventMissionPlan,
    cargo: &[String],
) -> AnyResult<()> {
    travel_fleet_to(client, config, cargo, None, &plan.home_location).await
}

fn reward_remaining(plan: &EventMissionPlan) -> ResourceMap {
    plan.event
        .rewards
        .resources
        .iter()
        .filter_map(|(resource, reward)| {
            let recovered = plan
                .execution
                .reward_recovered
                .get(resource)
                .copied()
                .unwrap_or(0)
                .max(0);
            let remaining = reward.saturating_sub(recovered);
            (remaining > 0).then_some((resource.clone(), remaining))
        })
        .collect()
}

async fn return_mission_assets(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
) -> AnyResult<()> {
    return_mission_assets_internal(client, config, plan, true).await
}

async fn return_mission_assets_internal(
    client: &Client,
    config: &Config,
    plan: &mut EventMissionPlan,
    return_replicant: bool,
) -> AnyResult<()> {
    let cargo = cargo_codes(plan);
    for code in &cargo {
        let detail = fetch_raw_device(client, code).await?;
        ensure_uncontrolled_cargo(&detail, code)?;
    }

    let mission_payload = plan
        .execution
        .payload_devices
        .iter()
        .map(|device| device.code.as_str())
        .collect::<BTreeSet<_>>();
    let carriers = carrier_codes(plan);
    for code in &carriers {
        let detail = fetch_raw_device(client, code).await?;
        if !detail.attached_devices.is_empty() {
            let attached = detail
                .attached_devices
                .iter()
                .filter_map(reference_code)
                .filter(|attached| mission_payload.contains(attached.as_str()))
                .collect::<Vec<_>>();
            if !attached.is_empty() {
                detach_devices(client, config, code, &attached).await?;
            }
        }
    }

    let mut devices = cargo.clone();
    devices.extend(carriers);
    devices.sort();
    devices.dedup();
    travel_fleet_to(
        client,
        config,
        &devices,
        return_replicant.then_some(plan.selected_replicant.as_str()),
        &plan.home_location,
    )
    .await?;

    deposit_all_devices(client, config, &cargo).await?;
    recover_failed_beacon(client, config, plan).await?;
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
    let detail = fetch_raw_device(client, &code).await?;
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
        ensure_attachable_device(client, config, &code).await?;
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
        if claim.role == "payload" && plan.execution.event_resolved {
            // Event requirement devices are consumed by successful resolution.
            // Their disappearance is terminal evidence, not a reason to issue
            // one guaranteed-404 detail request per payload during cleanup.
            plan.claimed_devices[index].released = true;
            save_plan(&config.plan_path, plan)?;
            continue;
        }
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
            let handle = match client.devices().cached(&claim.device_code) {
                Some(handle) => handle,
                None => client.devices().get(&claim.device_code).await?,
            };
            let operation = handle
                .configure(raw::devices::DeviceConfiguration {
                    add_tags: None,
                    remove_tags: Some(removable.clone()),
                    tags: None,
                    ..Default::default()
                })
                .await?;
            ensure_operation_accepted(&operation).await?;
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
    cleanup_component_tags(client, config, plan).await?;
    Ok(())
}

async fn cleanup_component_tags(
    client: &Client,
    config: &Config,
    plan: &EventMissionPlan,
) -> AnyResult<()> {
    let component_tag = role_tag("component");
    let handles = client
        .devices()
        .find()
        .owned()
        .with_tag(plan.mission_tag.clone())
        .collect()
        .await?;
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if !snapshot.tags.iter().any(|tag| tag == &component_tag) {
            continue;
        }
        let mut removable = [plan.mission_tag.clone(), component_tag.clone()]
            .into_iter()
            .filter(|tag| snapshot.tags.contains(tag))
            .collect::<Vec<_>>();
        removable.extend(
            snapshot
                .tags
                .iter()
                .filter(|tag| tag.starts_with("evt-p:"))
                .cloned(),
        );
        removable.sort();
        removable.dedup();
        if removable.is_empty() {
            continue;
        }
        let code = handle.id().as_str().to_owned();
        let operation = handle
            .configure(raw::devices::DeviceConfiguration {
                add_tags: None,
                remove_tags: Some(removable.clone()),
                tags: None,
                ..Default::default()
            })
            .await?;
        ensure_operation_accepted(&operation).await?;
        wait_for_device_snapshot(client, config, &code, |device| {
            removable
                .iter()
                .all(|tag| !device.tags.iter().any(|existing| existing == tag))
        })
        .await?;
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
    start_device_travel_to(client, code, destination).await?;
    wait_for_device_at(client, config, code, destination).await
}

async fn start_device_travel_to(client: &Client, code: &str, destination: &str) -> AnyResult<()> {
    // Travel events keep mission-device location and travel state current.
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    let detail = handle.snapshot().await?;
    if detail.travel.is_none()
        && detail
            .location
            .as_ref()
            .is_some_and(|location| location.id.as_str() == destination)
    {
        return Ok(());
    }
    if let Some(travel) = &detail.travel {
        let planned = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
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
        info!(
            device = %code,
            destination = %destination,
            "dispatching device travel"
        );
        let via = client
            .smart_travel()
            .route_for_device(code, destination)
            .await?
            .filter(|plan| !plan.is_direct && !plan.intermediate_systems.is_empty())
            .map(|plan| {
                Value::Array(
                    plan.explicit_waypoints_for(destination)
                        .into_iter()
                        .map(Value::String)
                        .collect(),
                )
            });
        let operation = handle
            .command(raw::devices::DeviceCommand::Travel {
                destination: destination.to_owned(),
                dry_run: None,
                via,
            })
            .await?;
        ensure_operation_accepted(&operation).await?;
    }
    Ok(())
}

async fn wait_for_device_at(
    client: &Client,
    config: &Config,
    code: &str,
    destination: &str,
) -> AnyResult<()> {
    wait_for_devices_at(client, config, &[code.to_owned()], destination).await
}

async fn dispatch_devices_to(
    client: &Client,
    codes: &[String],
    destination: &str,
) -> AnyResult<()> {
    finish_all(
        join_all(
            codes
                .iter()
                .map(|code| start_device_travel_to(client, code, destination)),
        )
        .await,
    )
}

async fn wait_for_devices_at(
    client: &Client,
    config: &Config,
    codes: &[String],
    destination: &str,
) -> AnyResult<()> {
    if codes.is_empty() {
        return Ok(());
    }
    let mut pending = codes.iter().cloned().collect::<BTreeSet<_>>();
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;

    loop {
        let mut eta_seconds = None::<i64>;
        let current = pending.iter().cloned().collect::<Vec<_>>();
        for code in current {
            let handle = match client.devices().cached(&code) {
                Some(handle) => handle,
                None => client.devices().get(&code).await?,
            };
            let snapshot = handle.snapshot().await?;
            if snapshot.travel.is_none()
                && snapshot
                    .location
                    .as_ref()
                    .is_some_and(|location| location.id.as_str() == destination)
            {
                pending.remove(&code);
                continue;
            }
            if let Some(eta) = snapshot
                .travel
                .as_ref()
                .and_then(|travel| travel.eta_seconds)
            {
                eta_seconds = Some(eta_seconds.map_or(eta, |current| current.min(eta)));
            }
        }
        if pending.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out waiting for devices at {destination}: {}",
                    pending.iter().cloned().collect::<Vec<_>>().join(",")
                ),
            ));
        }

        let wake = wait_for_device_travel_event(
            &mut watch,
            deadline,
            &pending,
            travel_poll_interval(eta_seconds),
        )
        .await?;
        if matches!(wake, WaitWake::Poll | WaitWake::Gap) {
            // Target only the devices still in flight. The previous
            // implementation spawned one independent polling loop per device,
            // multiplying GETs by fleet size.
            let refreshes = pending
                .iter()
                .cloned()
                .map(|code| {
                    let client = client.clone();
                    async move { client.devices().get(&code).await.map(|_| ()) }
                })
                .collect::<Vec<_>>();
            for result in join_all(refreshes).await {
                result?;
            }
        }
    }
}

async fn travel_devices_to(
    client: &Client,
    config: &Config,
    codes: &[String],
    destination: &str,
) -> AnyResult<()> {
    if codes.is_empty() {
        return Ok(());
    }
    info!(
        count = codes.len(),
        devices = %codes.join(","),
        destination = %destination,
        "dispatching device travel batch"
    );
    dispatch_devices_to(client, codes, destination).await?;
    wait_for_devices_at(client, config, codes, destination).await
}

async fn start_replicant_travel_to(
    client: &Client,
    replicant_code: &str,
    destination: &str,
) -> AnyResult<Option<String>> {
    let handle = client.replicants().get_owned(replicant_code).await?;
    let snapshot = handle.snapshot().await?;
    let origin = snapshot
        .location
        .as_ref()
        .map(|location| location.id.as_str().to_owned());
    if snapshot.travel.is_none()
        && snapshot
            .location
            .as_ref()
            .is_some_and(|location| location.id.as_str() == destination)
    {
        return Ok(origin);
    }
    if let Some(travel) = &snapshot.travel {
        let planned = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        if planned != Some(destination) {
            info!(
                replicant = %replicant_code,
                in_flight_destination = ?planned,
                requested_destination = %destination,
                "replicant is already in flight; waiting for that travel to finish before continuing event route"
            );
        }
        return Ok(origin);
    }

    info!(
        replicant = %replicant_code,
        destination = %destination,
        "dispatching replicant travel"
    );
    let operation = handle.travel().to(destination).depart().await?;
    ensure_operation_accepted(&operation).await?;
    Ok(origin)
}

async fn wait_for_replicant_at(
    client: &Client,
    config: &Config,
    replicant_code: &str,
    destination: &str,
    mut departure_origin: Option<String>,
) -> AnyResult<()> {
    let mut handle = match client.replicants().cached(replicant_code) {
        Some(handle) => handle,
        None => client.replicants().get_owned(replicant_code).await?,
    };
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        let snapshot = handle.snapshot().await?;
        let location = snapshot
            .location
            .as_ref()
            .map(|location| location.id.as_str());
        match replicant_travel_decision(
            location,
            snapshot.travel.is_some(),
            destination,
            departure_origin.as_deref(),
        ) {
            ReplicantTravelDecision::Arrived => return Ok(()),
            ReplicantTravelDecision::Continue => {
                let Some(intermediate) = location else {
                    continue;
                };
                info!(
                    replicant = %replicant_code,
                    intermediate = %intermediate,
                    destination = %destination,
                    "continuing replicant travel from intermediate waypoint"
                );
                departure_origin =
                    start_replicant_travel_to(client, replicant_code, destination).await?;
                handle = client.replicants().cached(replicant_code).unwrap_or(handle);
                continue;
            }
            ReplicantTravelDecision::Wait => {}
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out travelling replicant to {destination}"),
            ));
        }
        let eta = snapshot
            .travel
            .as_ref()
            .and_then(|travel| travel.eta_seconds);
        let wake = wait_for_replicant_travel_event(
            &mut watch,
            deadline,
            replicant_code,
            travel_poll_interval(eta),
        )
        .await?;
        if matches!(wake, WaitWake::Poll | WaitWake::Gap) {
            handle = handle.refresh().await?;
        }
    }
}

async fn travel_replicant_to(
    client: &Client,
    config: &Config,
    replicant_code: &str,
    destination: &str,
) -> AnyResult<()> {
    let departure_origin = start_replicant_travel_to(client, replicant_code, destination).await?;
    wait_for_replicant_at(
        client,
        config,
        replicant_code,
        destination,
        departure_origin,
    )
    .await
}

pub(crate) async fn travel_fleet_to(
    client: &Client,
    config: &Config,
    devices: &[String],
    replicant: Option<&str>,
    destination: &str,
) -> AnyResult<()> {
    info!(
        device_count = devices.len(),
        devices = %devices.join(","),
        replicant = replicant.unwrap_or("none"),
        destination = %destination,
        "dispatching mission fleet"
    );

    match replicant {
        Some(replicant) => {
            let (devices_result, replicant_result) = tokio::join!(
                dispatch_devices_to(client, devices, destination),
                start_replicant_travel_to(client, replicant, destination),
            );
            devices_result?;
            let departure_origin = replicant_result?;
            tokio::try_join!(
                wait_for_devices_at(client, config, devices, destination),
                wait_for_replicant_at(client, config, replicant, destination, departure_origin,),
            )?;
        }
        None => {
            dispatch_devices_to(client, devices, destination).await?;
            wait_for_devices_at(client, config, devices, destination).await?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReplicantTravelDecision {
    Arrived,
    Continue,
    Wait,
}

fn replicant_travel_decision(
    location: Option<&str>,
    travelling: bool,
    destination: &str,
    departure_origin: Option<&str>,
) -> ReplicantTravelDecision {
    if travelling {
        return ReplicantTravelDecision::Wait;
    }
    if location == Some(destination) {
        return ReplicantTravelDecision::Arrived;
    }
    if let (Some(location), Some(departure_origin)) = (location, departure_origin)
        && location != departure_origin
    {
        return ReplicantTravelDecision::Continue;
    }
    ReplicantTravelDecision::Wait
}

fn finish_all(results: Vec<AnyResult<()>>) -> AnyResult<()> {
    for result in results {
        result?;
    }
    Ok(())
}

pub(crate) async fn collect_resources(
    client: &Client,
    config: &Config,
    code: &str,
    resources: &ResourceMap,
) -> AnyResult<()> {
    if resources.is_empty() {
        return Ok(());
    }
    let before = cargo_map(&fetch_raw_device(client, code).await?);
    // Mission cargo is already tracked by the managed projection.
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    let operation = handle
        .command(raw::devices::DeviceCommand::CollectResources {
            resources: resource_json(resources),
        })
        .await?;
    ensure_operation_accepted(&operation).await?;
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

pub(crate) async fn deposit_resources(
    client: &Client,
    config: &Config,
    code: &str,
    resources: Option<&ResourceMap>,
) -> AnyResult<()> {
    let before = cargo_map(&fetch_raw_device(client, code).await?);
    if before.is_empty() {
        return Ok(());
    }
    let requested = resources.cloned().unwrap_or_else(|| before.clone());
    // Mission cargo is already tracked by the managed projection.
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    let operation = handle
        .command(raw::devices::DeviceCommand::DepositResources {
            resources: resources.map(resource_json),
        })
        .await?;
    ensure_operation_accepted(&operation).await?;
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
    // Mission carriers are maintained by attachment and travel events.
    let handle = match client.devices().cached(carrier) {
        Some(handle) => handle,
        None => client.devices().get(carrier).await?,
    };
    let operation = handle
        .attach(raw::devices::TargetsCommand {
            device: None,
            devices: Some(Value::Array(
                devices.iter().cloned().map(Value::String).collect(),
            )),
            target: None,
            targets: None,
        })
        .await?;
    ensure_operation_accepted(&operation).await?;
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
    // Attachment waits keep the mission carrier projection current.
    let handle = match client.devices().cached(carrier) {
        Some(handle) => handle,
        None => client.devices().get(carrier).await?,
    };
    let snapshot = handle.snapshot().await?;
    if !snapshot.available_commands.is_empty()
        && !snapshot
            .available_commands
            .iter()
            .any(|command| command.as_str() == "detach")
    {
        return Err(classified_error(
            FailureClass::EventControlUnavailable,
            io::ErrorKind::WouldBlock,
            format!(
                "carrier {carrier} cannot currently detach payload; it may be out of control range"
            ),
        ));
    }
    let operation = handle
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
    ensure_operation_accepted(&operation).await?;
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
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    let mut watch = handle.watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        let snapshot = handle.snapshot().await?;
        if predicate(&snapshot) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for device {code}"),
            ));
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        match timeout(remaining.min(AUTHORITATIVE_POLL_INTERVAL), watch.next()).await {
            Ok(Some(snapshot)) if predicate(&snapshot) => return Ok(()),
            Ok(Some(_)) => {}
            Ok(None) => {
                warn!(device = %code, "device projection watcher closed; falling back to refresh");
                let _ = handle.refresh().await?;
                watch = handle.watch().await?;
            }
            Err(_) => {
                // SSE/projection updates are the fast path. A sparse
                // authoritative refresh is only a fallback against a missed
                // or delayed event.
                let _ = handle.refresh().await?;
            }
        }
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
        let detail = fetch_raw_device(client, code).await?;
        if predicate(&detail) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for device {code}"),
            ));
        }

        // Cargo, queue, and other raw-only fields do not necessarily change
        // the normalized Device value. Use the local account event stream as
        // the fast signal, but only for this exact device. Unrelated events are
        // consumed without another GET; a sparse authoritative fallback still
        // protects against muted or missed events.
        let poll_deadline = Instant::now() + AUTHORITATIVE_POLL_INTERVAL;
        loop {
            let wake_deadline = deadline.min(poll_deadline);
            let remaining = wake_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, watch.next()).await {
                Ok(Ok(event))
                    if event
                        .device
                        .as_ref()
                        .is_some_and(|device| device.id.as_str() == code) =>
                {
                    break;
                }
                Ok(Ok(_)) => continue,
                Ok(Err(error)) => {
                    warn!(
                        error = %error,
                        device = %code,
                        "event watcher gap; refreshing raw device"
                    );
                    break;
                }
                Err(_) => break,
            }
        }
    }
}

async fn wait_for_device_travel_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    pending: &BTreeSet<String>,
    poll_interval: Duration,
) -> AnyResult<WaitWake> {
    let poll_deadline = Instant::now() + poll_interval;
    loop {
        let wake_deadline = deadline.min(poll_deadline);
        let remaining = wake_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(WaitWake::Poll);
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event))
                if event.name.as_str() == "travel.arrived"
                    && event
                        .device
                        .as_ref()
                        .is_some_and(|device| pending.contains(device.id.as_str())) =>
            {
                return Ok(WaitWake::Event);
            }
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(WaitWake::Poll),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; refreshing pending device travel");
                return Ok(WaitWake::Gap);
            }
        }
    }
}

async fn wait_for_replicant_travel_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    replicant_code: &str,
    poll_interval: Duration,
) -> AnyResult<WaitWake> {
    let poll_deadline = Instant::now() + poll_interval;
    loop {
        let wake_deadline = deadline.min(poll_deadline);
        let remaining = wake_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(WaitWake::Poll);
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event))
                if event.name.as_str() == "travel.arrived"
                    && event
                        .replicant
                        .as_ref()
                        .is_some_and(|replicant| replicant.id.as_str() == replicant_code) =>
            {
                return Ok(WaitWake::Event);
            }
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(WaitWake::Poll),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; refreshing replicant travel");
                return Ok(WaitWake::Gap);
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WaitWake {
    Event,
    Poll,
    Gap,
}

async fn wait_for_mission_print_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    mission_tag: &str,
) -> AnyResult<WaitWake> {
    let poll_deadline = Instant::now() + AUTHORITATIVE_POLL_INTERVAL;
    loop {
        let wake_deadline = deadline.min(poll_deadline);
        let remaining = wake_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(WaitWake::Poll);
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event))
                if matches!(
                    event.name.as_str(),
                    "print.completed" | "device.print_completed"
                ) && event.payload.get("tags").is_some_and(|tags| {
                    tags.as_array().is_some_and(|tags| {
                        tags.iter().any(|tag| tag.as_str() == Some(mission_tag))
                    })
                }) =>
            {
                return Ok(WaitWake::Event);
            }
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(WaitWake::Poll),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; falling back to authoritative refresh");
                sleep(Duration::from_millis(250)).await;
                return Ok(WaitWake::Gap);
            }
        }
    }
}

async fn wait_for_relevant_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    names: &[&str],
) -> AnyResult<WaitWake> {
    wait_for_relevant_event_with_interval(watch, deadline, names, AUTHORITATIVE_POLL_INTERVAL).await
}

async fn wait_for_relevant_event_with_interval(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    names: &[&str],
    poll_interval: Duration,
) -> AnyResult<WaitWake> {
    let poll_deadline = Instant::now() + poll_interval;
    loop {
        let wake_deadline = deadline.min(poll_deadline);
        let remaining = wake_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(WaitWake::Poll);
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event)) if names.is_empty() || names.contains(&event.name.as_str()) => {
                return Ok(WaitWake::Event);
            }
            // The previous implementation returned here, so every unrelated
            // SSE event triggered another authoritative GET. Consume it and
            // keep waiting for something relevant instead.
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(WaitWake::Poll),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; falling back to authoritative refresh");
                sleep(Duration::from_millis(250)).await;
                return Ok(WaitWake::Gap);
            }
        }
    }
}

fn event_now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_millis()).ok())
        .unwrap_or(i64::MAX)
}

fn travel_poll_interval(eta_seconds: Option<i64>) -> Duration {
    match eta_seconds.unwrap_or(0) {
        eta if eta >= 300 => Duration::from_secs(60),
        eta if eta >= 60 => Duration::from_secs(30),
        eta if eta > 0 => Duration::from_secs(10),
        _ => AUTHORITATIVE_POLL_INTERVAL,
    }
}

async fn fetch_raw_device(client: &Client, code: &str) -> AnyResult<raw::devices::DeviceStatus> {
    match client.raw().devices().get(code).await {
        Ok(response) => Ok(response.value),
        Err(error) if device_fetch_is_missing(&error) => Err(permanent_classified_error(
            FailureClass::DeviceTargetMissing,
            io::ErrorKind::NotFound,
            format!("device {code} no longer exists"),
        )),
        Err(error) => Err(Box::new(error)),
    }
}

/// Verifies the immediate durable classification of a submitted command.
///
/// Managed mutation construction has already completed the one durable HTTP
/// submission and persisted its classification. Successful device mutations
/// normally stay in `AwaitingEvidence`/`ReconciliationRequired` until the event
/// engine reconciles them, so blocking for a terminal status here stalled every
/// campaign command for the full timeout without adding any safety. Each call
/// site below performs its own state-specific verification (the
/// `wait_for_device_snapshot` / `wait_for_raw_device` calls that follow), which
/// is what actually establishes ordering.
async fn ensure_operation_accepted(operation: &Operation) -> AnyResult<()> {
    let outcome = operation.outcome().await?;
    if device_operation_is_missing(&outcome) {
        return Err(permanent_classified_error(
            FailureClass::DeviceTargetMissing,
            io::ErrorKind::NotFound,
            format!(
                "operation {} targeted a missing device",
                operation.id().as_str()
            ),
        ));
    }
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

fn ensure_uncontrolled_cargo(device: &raw::devices::DeviceStatus, code: &str) -> AnyResult<()> {
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

fn resource_json(resources: &ResourceMap) -> BTreeMap<String, f64> {
    resources
        .iter()
        .map(|(resource, quantity)| (resource.clone(), *quantity as f64))
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

fn allocate_manifests(
    resources: &ResourceMap,
    capacities: &[(String, i64)],
) -> Vec<(String, ResourceMap)> {
    let mut remaining = resources.clone();
    let mut manifests = Vec::new();
    for (code, capacity) in capacities {
        let manifest = take_manifest(&remaining, *capacity);
        if manifest.is_empty() {
            continue;
        }
        for (resource, quantity) in &manifest {
            let remove = if let Some(remaining_quantity) = remaining.get_mut(resource) {
                *remaining_quantity = remaining_quantity.saturating_sub(*quantity);
                *remaining_quantity == 0
            } else {
                false
            };
            if remove {
                remaining.remove(resource);
            }
        }
        manifests.push((code.clone(), manifest));
        if remaining.is_empty() {
            break;
        }
    }
    manifests
}

fn merge_resources(target: &mut ResourceMap, source: &ResourceMap) {
    for (resource, quantity) in source {
        *target.entry(resource.clone()).or_default() += quantity;
    }
}

fn resources_available_from(requested: &ResourceMap, available: &ResourceMap) -> ResourceMap {
    requested
        .iter()
        .filter_map(|(resource, requested_quantity)| {
            let available_quantity = available.get(resource).copied().unwrap_or(0).max(0);
            let quantity = (*requested_quantity).max(0).min(available_quantity);
            (quantity > 0).then_some((resource.clone(), quantity))
        })
        .collect()
}

fn legacy_recovered_rewards(
    rewards: &ResourceMap,
    baseline: &ResourceMap,
    current: &ResourceMap,
) -> ResourceMap {
    rewards
        .iter()
        .filter_map(|(resource, reward)| {
            let baseline_quantity = baseline.get(resource).copied().unwrap_or(0);
            let current_quantity = current.get(resource).copied().unwrap_or(0);
            let recovered = current_quantity
                .saturating_sub(baseline_quantity)
                .max(0)
                .min((*reward).max(0));
            (recovered > 0).then_some((resource.clone(), recovered))
        })
        .collect()
}

fn merge_recovered_rewards(
    recovered: &mut ResourceMap,
    rewards: &ResourceMap,
    deposited: &ResourceMap,
) {
    for (resource, quantity) in deposited {
        let maximum = rewards.get(resource).copied().unwrap_or(0).max(0);
        if maximum == 0 {
            continue;
        }
        let current = recovered.get(resource).copied().unwrap_or(0).max(0);
        recovered.insert(
            resource.clone(),
            current.saturating_add((*quantity).max(0)).min(maximum),
        );
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

#[cfg(test)]
mod tests {
    use replicant_client::{Client, SecretString, StartupPolicy, raw, raw::Url};
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path, query_param},
    };

    use super::{
        ReplicantTravelDecision, ResourceMap, allocate_manifests, command_available,
        is_modular_device, legacy_recovered_rewards, merge_recovered_rewards, merge_resources,
        print_batch_tag, replicant_travel_decision, resources_available_from,
        reward_cargo_is_loaded, status_is,
    };

    async fn test_client_at(server: &MockServer) -> Client {
        Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .authentication_token(SecretString::from("test-token"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("start test client")
    }

    #[tokio::test]
    async fn reward_cargo_inspection_uses_one_bulk_refresh() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .and(query_param("device_type", "cargo_freighter"))
            .and(query_param("tag", "evt-m:test"))
            .and(query_param("limit", "50"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "devices": [
                    {
                        "device_code": "CARGO-1",
                        "device_type": "cargo_freighter",
                        "cargo": [{"resource_type": "conductive", "quantity": 12}]
                    },
                    {"device_code": "CARGO-2", "device_type": "cargo_freighter"},
                    {"device_code": "CARGO-3", "device_type": "cargo_freighter"}
                ],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        let cargo = ["CARGO-1", "CARGO-2", "CARGO-3"].map(str::to_owned);

        assert!(
            reward_cargo_is_loaded(&client, "evt-m:test", &cargo)
                .await
                .expect("inspect cargo")
        );

        server.verify().await;
        client.close().await.expect("close client");
    }

    #[test]
    fn event_print_batch_tags_survive_workflow_recreation() {
        let mission_tag = "evt-m:khuxkrixx-3-evt-008";
        assert_eq!(
            print_batch_tag(mission_tag, "sensor_array", 0),
            print_batch_tag(mission_tag, "sensor_array", 0)
        );
        assert_ne!(
            print_batch_tag(mission_tag, "sensor_array", 0),
            print_batch_tag(mission_tag, "sensor_array", 1)
        );
        assert_ne!(
            print_batch_tag(mission_tag, "sensor_array", 0),
            print_batch_tag(mission_tag, "ftl_beacon", 0)
        );
    }

    #[test]
    fn recognizes_modular_transport_states_and_commands() {
        let mut device = raw::devices::DeviceStatus::default();
        device.features = vec!["modular".into()];
        device.available_commands = vec!["compact".into()];

        assert!(is_modular_device(&device));
        assert!(command_available(&device, "compact"));
        assert!(!status_is(&device, "compacted"));

        device.features.clear();
        device.available_commands = vec!["unfurl".into()];
        device.status = Some("compacted".into());

        assert!(is_modular_device(&device));
        assert!(command_available(&device, "unfurl"));
        assert!(status_is(&device, "compacted"));
    }

    #[test]
    fn allocates_reward_manifests_across_the_fleet() {
        let resources =
            ResourceMap::from([("conductive".to_owned(), 300), ("rares".to_owned(), 600)]);
        let capacities = [("CF-1".to_owned(), 500), ("CF-2".to_owned(), 400)];

        let manifests = allocate_manifests(&resources, &capacities);

        assert_eq!(manifests.len(), 2);
        assert_eq!(manifests[0].0, "CF-1");
        assert_eq!(manifests[0].1.values().sum::<i64>(), 500);
        assert_eq!(manifests[1].0, "CF-2");
        assert_eq!(manifests[1].1.values().sum::<i64>(), 400);

        let mut allocated = ResourceMap::new();
        for (_, manifest) in manifests {
            merge_resources(&mut allocated, &manifest);
        }
        assert_eq!(allocated, resources);
    }

    #[test]
    fn does_not_dispatch_unused_reward_capacity() {
        let resources = ResourceMap::from([("rares".to_owned(), 250)]);
        let capacities = [("CF-1".to_owned(), 500), ("CF-2".to_owned(), 500)];

        let manifests = allocate_manifests(&resources, &capacities);

        assert_eq!(
            manifests,
            vec![(
                "CF-1".to_owned(),
                ResourceMap::from([("rares".to_owned(), 250)])
            )]
        );
    }

    #[test]
    fn legacy_home_inventory_consumption_does_not_inflate_rewards() {
        let rewards =
            ResourceMap::from([("conductive".to_owned(), 300), ("rares".to_owned(), 600)]);
        let baseline = ResourceMap::from([
            ("conductive".to_owned(), 157_246),
            ("rares".to_owned(), 16_971),
        ]);
        let current = ResourceMap::from([
            ("conductive".to_owned(), 156_620),
            ("rares".to_owned(), 16_737),
        ]);

        assert!(legacy_recovered_rewards(&rewards, &baseline, &current).is_empty());
        assert_eq!(
            resources_available_from(
                &rewards,
                &ResourceMap::from([("conductive".to_owned(), 300)])
            ),
            ResourceMap::from([("conductive".to_owned(), 300)])
        );
    }

    #[test]
    fn recovered_reward_ledger_is_capped_at_the_advertised_reward() {
        let rewards = ResourceMap::from([("conductive".to_owned(), 300)]);
        let mut recovered = ResourceMap::from([("conductive".to_owned(), 200)]);

        merge_recovered_rewards(
            &mut recovered,
            &rewards,
            &ResourceMap::from([("conductive".to_owned(), 500)]),
        );

        assert_eq!(recovered, rewards);
    }

    #[test]
    fn continues_only_after_reaching_an_idle_intermediate_waypoint() {
        assert_eq!(
            replicant_travel_decision(
                Some("SCEPTURUM-BELT-1"),
                false,
                "KHUXKRIXX-3",
                Some("SCEPTURUM-BELT-1"),
            ),
            ReplicantTravelDecision::Wait,
        );
        assert_eq!(
            replicant_travel_decision(
                Some("SCEPTURUM-7-L4"),
                true,
                "KHUXKRIXX-3",
                Some("SCEPTURUM-BELT-1"),
            ),
            ReplicantTravelDecision::Wait,
        );
        assert_eq!(
            replicant_travel_decision(
                Some("SCEPTURUM-7-L4"),
                false,
                "KHUXKRIXX-3",
                Some("SCEPTURUM-BELT-1"),
            ),
            ReplicantTravelDecision::Continue,
        );
        assert_eq!(
            replicant_travel_decision(Some("SCEPTURUM-7-L4"), false, "KHUXKRIXX-3", None,),
            ReplicantTravelDecision::Wait,
        );
        assert_eq!(
            replicant_travel_decision(
                Some("KHUXKRIXX-3"),
                false,
                "KHUXKRIXX-3",
                Some("SCEPTURUM-7-L4"),
            ),
            ReplicantTravelDecision::Arrived,
        );
    }
}
