use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::{Duration, Instant},
};

use futures::future::join_all;
use replicant_client::{
    Client, MiningDirective, OperationId, OperationStatus, SurveyDirective, TransportDirective,
    domain::Device, managed::Operation, raw as api_raw,
};
use replicant_mining_planner::{
    CARGO_FREIGHTER, MAINTENANCE_DRONE, MINING_CONTROLLER, MINING_DRONE, QuantityMap,
    SURGE_CARRIER, SURVEY_CONTROLLER, SURVEY_DRONE, SYSTEM_WARD, TRANSPORT_CONTROLLER, role_tag,
};
use replicant_printing::managed::{
    QueueOptions, TrackedPrintRequest, TrackedPrintUpdate,
    fetch_blueprints as fetch_print_blueprints, queue_tracked_prints_once,
};
use replicant_transport::{DeliveryOptions, PayloadDevice, deliver_devices_with};
use serde_json::Value;
use tokio::time::timeout;
use tracing::{info, warn};

use super::validation::{self, ValidationReason};
use super::{
    AnyResult, Config, EvidenceState, ExecutionPrintBatch, MiningMission, MissionPhase,
    PrintPurpose, RoutePhase, SiteAssets, SitePhase, app_error, audit_site, controller_code,
    device_is_in_system, device_location, device_snapshots, device_type, fetch_blueprints,
    find_device, has_reservation_tag, is_opaque_mining_mission_tag, save_plan, site_shortages,
    stable_hash, transport_service_present,
};

const POLL_INTERVAL: Duration = Duration::from_secs(5);
fn format_quantities(quantities: &QuantityMap) -> String {
    quantities
        .iter()
        .map(|(resource, quantity)| format!("{resource}={quantity}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Upper bound between authoritative refreshes while waiting on the event
/// stream. Waits wake immediately on a relevant event; this only bounds how
/// long a missed or filtered-out event can delay progress.
const AUTHORITATIVE_POLL_INTERVAL: Duration = Duration::from_secs(60);
const VERIFY_TIMEOUT: Duration = Duration::from_secs(300);

/// Why a mission wait loop woke up.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MissionWake {
    /// A relevant upstream event arrived.
    Event,
    /// The authoritative poll interval elapsed.
    Poll,
    /// The event watcher reported a gap and state must be re-read.
    Gap,
}

/// Waits for an event that could advance the mission, bounded by the
/// authoritative poll interval.
///
/// Mission progress is driven by device and print events. Sleeping a flat
/// interval between selected-resource refreshes still delayed event handling;
/// waking on the event stream keeps the reaction time while bounding fallback
/// validation to the mission resources involved in the current phase.
async fn wait_for_mission_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    names: &[&str],
) -> AnyResult<MissionWake> {
    let poll_deadline = (Instant::now() + AUTHORITATIVE_POLL_INTERVAL).min(deadline);
    loop {
        let remaining = poll_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(MissionWake::Poll);
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event)) if names.is_empty() || names.contains(&event.name.as_str()) => {
                return Ok(MissionWake::Event);
            }
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(MissionWake::Poll),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; refreshing mining mission state");
                return Ok(MissionWake::Gap);
            }
        }
    }
}
async fn refresh_after_fallback_wake(
    client: &Client,
    wake: MissionWake,
    codes: &[String],
) -> AnyResult<()> {
    if matches!(wake, MissionWake::Poll | MissionWake::Gap) {
        info!(
            wake = ?wake,
            resources = codes.len(),
            "refreshing selected mining resources after event-stream fallback"
        );
        for code in codes {
            validation::device(client, code, ValidationReason::EventGap).await?;
        }
    }
    Ok(())
}

fn mission_resource_codes(mission: &MiningMission) -> Vec<String> {
    let mut codes = mission
        .sites
        .iter()
        .flat_map(|site| {
            site.assets
                .codes()
                .into_iter()
                .chain(site.carrier.iter().cloned())
        })
        .chain(
            mission
                .routes
                .iter()
                .flat_map(|route| route.controller.iter().cloned().chain(route.freighters())),
        )
        .chain(mission.print_batches.iter().flat_map(|batch| {
            std::iter::once(batch.factory_code.clone()).chain(batch.produced_codes.clone())
        }))
        .collect::<Vec<_>>();
    codes.sort();
    codes.dedup();
    codes
}

fn site_resource_codes(site: &super::SiteMission) -> Vec<String> {
    let mut codes = site
        .assets
        .codes()
        .into_iter()
        .chain(site.carrier.iter().cloned())
        .collect::<Vec<_>>();
    codes.sort();
    codes.dedup();
    codes
}

fn route_resource_codes(route: &super::RouteMission) -> Vec<String> {
    route
        .controller
        .iter()
        .cloned()
        .chain(route.freighters())
        .collect()
}

async fn validate_codes(
    client: &Client,
    codes: &[String],
    reason: ValidationReason,
) -> AnyResult<Vec<Device>> {
    let mut snapshots = Vec::with_capacity(codes.len());
    for code in codes {
        snapshots.push(validation::device(client, code, reason).await?);
    }
    Ok(snapshots)
}

pub(crate) async fn execute(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    if mission.phase.is_terminal() {
        return Ok(());
    }
    let replicant = validation::replicant(
        client,
        &mission.selected_replicant,
        ValidationReason::StateConflict,
    )
    .await?;
    if let Some(claims) = &config.claims {
        let vessel = replicant.hosted_device.as_ref().ok_or_else(|| {
            app_error(
                io::ErrorKind::WouldBlock,
                "mining worker has no authoritative hosted vessel",
            )
        })?;
        let mut resources = mission_resource_codes(mission)
            .into_iter()
            .map(replicant_workflow::ResourceKey::Device)
            .collect::<Vec<_>>();
        resources.push(replicant_workflow::ResourceKey::Replicant(
            mission.selected_replicant.clone(),
        ));
        resources.push(replicant_workflow::ResourceKey::Device(
            vessel.id.as_str().to_owned(),
        ));
        claims
            .repository
            .acquire_claims(claims.workflow_id, &resources)?;
    }
    migrate_legacy_mission_devices(client, mission, config.claims.as_ref()).await?;
    save_plan(&config.plan_path, mission)?;
    reconcile(client, config, mission).await?;
    config.claim_devices(&mission_resource_codes(mission))?;
    tag_existing_automation(client, mission).await?;

    set_phase(config, mission, MissionPhase::ManufacturingSites)?;
    execute_print_phase(client, config, mission, PrintPurpose::Site).await?;
    allocate_site_assets(client, config, mission).await?;

    set_phase(config, mission, MissionPhase::DeployingSites)?;
    deploy_sites(client, config, mission).await?;

    set_phase(config, mission, MissionPhase::ManufacturingRoutes)?;
    execute_print_phase(client, config, mission, PrintPurpose::Route).await?;
    allocate_route_assets(client, config, mission).await?;

    set_phase(config, mission, MissionPhase::ActivatingRoutes)?;
    activate_routes(client, config, mission).await?;

    set_phase(config, mission, MissionPhase::ReturningCarriers)?;
    return_and_release_carriers(client, config, mission).await?;
    cleanup_transient_tags(client, mission).await?;

    mission.phase = if mission.warnings.is_empty() {
        MissionPhase::Completed
    } else {
        MissionPhase::CompletedWithWarnings
    };
    save_plan(&config.plan_path, mission)?;
    Ok(())
}

async fn tag_existing_automation(client: &Client, mission: &MiningMission) -> AnyResult<()> {
    for site in &mission.sites {
        if site.phase == SitePhase::Operational {
            tag_site_assets(client, site).await?;
        }
    }
    for route in &mission.routes {
        if route.phase == RoutePhase::Active {
            tag_route_assets(client, route).await?;
        }
    }
    Ok(())
}

fn set_phase(config: &Config, mission: &mut MiningMission, phase: MissionPhase) -> AnyResult<()> {
    info!(
        mission_id = %mission.mission_id,
        phase = ?phase,
        "mining mission phase"
    );
    mission.phase = mission.phase.advance_to(phase);
    save_plan(&config.plan_path, mission)
}

async fn reconcile(client: &Client, config: &Config, mission: &mut MiningMission) -> AnyResult<()> {
    reconcile_print_batches(client, mission, config.claims.as_ref()).await?;
    let devices = device_snapshots(client).await?;
    for site in &mut mission.sites {
        let audit = audit_site(&devices, &site.system, &site.belt);
        if !audit.mutation_safe {
            return Err(app_error(
                io::ErrorKind::WouldBlock,
                format!(
                    "mining site {} requires authoritative evidence before provisioning: {:?}",
                    site.belt, audit.issues
                ),
            ));
        }
        if audit.operational {
            site.assets = audit.assets;
            site.missing.clear();
            site.phase = SitePhase::Operational;
        } else if site.phase == SitePhase::Operational {
            site.missing = site_shortages(&audit);
            site.assets = audit.assets;
            if audit.mutation_safe && audit.normal_repair_needed {
                site.phase = SitePhase::Planned;
            } else {
                site.phase = SitePhase::Verifying;
            }
        } else if site.phase == SitePhase::Planned {
            site.missing = site_shortages(&audit);
            site.assets = audit.assets;
        }
    }
    reconcile_carrier_claims(client, mission).await?;
    save_plan(&config.plan_path, mission)?;
    Ok(())
}

async fn reconcile_carrier_claims(client: &Client, mission: &mut MiningMission) -> AnyResult<()> {
    for site in &mut mission.sites {
        let Some(carrier) = site.carrier.clone() else {
            continue;
        };
        let snapshot =
            validation::device(client, &carrier, ValidationReason::StateConflict).await?;
        if device_location(&snapshot) == Some(mission.hub_location.as_str())
            && snapshot.travel.is_none()
            && snapshot.relationships.attached_devices.is_empty()
            && site.phase == SitePhase::Operational
        {
            remove_tags(
                client,
                &carrier,
                &[mission.mission_tag.clone(), role_tag("carrier")],
            )
            .await?;
            site.carrier = None;
        }
    }
    Ok(())
}

async fn execute_print_phase(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
    purpose: PrintPurpose,
) -> AnyResult<()> {
    reconcile_print_batches(client, mission, config.claims.as_ref()).await?;
    let migrated = split_pending_print_batches(mission);
    if migrated > 0 {
        info!(
            grouped_batches = migrated,
            unit_batches = mission.print_batches.len(),
            "split pending print quantities into queue-safe unit batches"
        );
    }
    save_plan(&config.plan_path, mission)?;
    if phase_batches(mission, purpose).is_empty() {
        return Ok(());
    }
    submit_print_batches(client, config, mission, purpose).await?;
    wait_for_print_outputs(client, config, mission, purpose).await
}

fn split_pending_print_batches(mission: &mut MiningMission) -> usize {
    let batches = std::mem::take(&mut mission.print_batches);
    let mut normalized = Vec::with_capacity(batches.len());
    let mut migrated = 0usize;
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
        migrated += 1;
        normalized.extend(unit_print_batches(batch));
    }
    mission.print_batches = normalized;
    migrated
}

fn unit_print_batches(batch: ExecutionPrintBatch) -> Vec<ExecutionPrintBatch> {
    (0..batch.quantity)
        .map(|unit_index| {
            let mut unit = batch.clone();
            unit.quantity = 1;
            if unit_index > 0 {
                unit.batch_tag = format!(
                    "mine-b:{:016x}",
                    stable_hash(&format!("{}:{unit_index}", batch.batch_tag))
                );
            }
            unit
        })
        .collect()
}

fn phase_batches(mission: &MiningMission, purpose: PrintPurpose) -> Vec<usize> {
    mission
        .print_batches
        .iter()
        .enumerate()
        .filter_map(|(index, batch)| (batch.purpose == purpose).then_some(index))
        .collect()
}

async fn reconcile_print_batches(
    client: &Client,
    mission: &mut MiningMission,
    claims: Option<&super::MiningWorkflowClaims>,
) -> AnyResult<()> {
    let batch_tags = mission
        .print_batches
        .iter()
        .map(|batch| batch.batch_tag.clone())
        .collect::<BTreeSet<_>>();
    migrate_legacy_mission_devices(client, mission, claims).await?;
    let aliases = mission_tag_aliases(mission);
    let devices = device_snapshots(client).await?;
    let mut produced = BTreeMap::<String, Vec<String>>::new();
    for snapshot in devices
        .iter()
        .filter(|device| device.tags.iter().any(|tag| aliases.contains(tag)))
    {
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
                    snapshot.key.id.as_str()
                ),
            ));
        }
        if let Some(batch_tag) = matching.into_iter().next() {
            produced
                .entry(batch_tag)
                .or_default()
                .push(snapshot.key.id.as_str().to_owned());
        }
    }

    let factory_codes = mission
        .print_batches
        .iter()
        .map(|batch| batch.factory_code.clone())
        .collect::<BTreeSet<_>>();
    let mut factory_jobs = BTreeMap::new();
    for factory in factory_codes {
        factory_jobs.insert(factory.clone(), factory_job_tags(client, &factory).await?);
    }

    for batch in &mut mission.print_batches {
        let mut codes = produced.remove(&batch.batch_tag).unwrap_or_default();
        codes.extend(batch.produced_codes.clone());
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
        if let Some(claims) = claims {
            claims.acquire_devices(&codes)?;
        }
        batch.produced_codes = codes;
        let queued =
            factory_jobs
                .get(&batch.factory_code)
                .is_some_and(|jobs: &Vec<BTreeSet<String>>| {
                    jobs.iter().any(|tags| {
                        aliases.iter().any(|tag| tags.contains(tag))
                            && tags.contains(&batch.batch_tag)
                    })
                });
        // Partial output from a legacy multi-unit batch also proves acceptance.
        // Reprinting its full quantity would duplicate the devices already produced.
        if queued || !batch.produced_codes.is_empty() {
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
    if let Some(batch) = mission.print_batches.iter().find(|batch| {
        batch.submission_started
            && !batch.submitted
            && batch.operation_id.is_none()
            && batch.produced_codes.is_empty()
    }) {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "print submission {} was interrupted before its durable operation could be recorded; queue/output evidence is absent, so automatic resubmission is unsafe",
                batch.batch_tag
            ),
        ));
    }
    Ok(())
}

async fn factory_job_tags(client: &Client, factory_code: &str) -> AnyResult<Vec<BTreeSet<String>>> {
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
        Value::String(value) => {
            if value.starts_with("mine-") {
                tags.insert(value.clone());
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_tags(value, tags);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_tags(value, tags);
            }
        }
        _ => {}
    }
}

async fn submit_print_batches(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
    purpose: PrintPurpose,
) -> AnyResult<()> {
    let blueprints = fetch_blueprints(client).await?;
    let printing_blueprints = fetch_print_blueprints(client).await?;
    let deadline = Instant::now() + config.wait_timeout;
    let mut watch = client.events().watch().await?;
    loop {
        reconcile_print_batches(client, mission, config.claims.as_ref()).await?;
        if purpose == PrintPurpose::Site {
            progress_site_pipeline(client, config, mission).await?;
        }
        let pending = phase_batches(mission, purpose)
            .into_iter()
            .filter(|index| !mission.print_batches[*index].submitted)
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }

        let options = QueueOptions::at(mission.hub_location.clone());
        let mut unaffordable = None;
        // Select an inventory-safe request subset before managed assignment.
        // Every mining batch is one print unit, so any assigned subset remains
        // affordable even when live queue capacity is smaller than this list.
        let mut available = hub_inventory(client, &mission.hub_location).await?;
        let mut schedulable = Vec::new();
        for index in pending {
            let batch = &mission.print_batches[index];
            let cost = replicant_mining_planner::blueprint_resource_cost(
                &batch.device_type,
                batch.quantity,
                &blueprints,
            )?;
            if !contains_quantities(&available, &cost) {
                unaffordable = Some((cost, available.clone()));
                continue;
            }
            deduct_quantities(&mut available, &cost);
            schedulable.push(index);
        }
        let requests = schedulable
            .iter()
            .map(|index| {
                let batch = &mission.print_batches[*index];
                let mut request =
                    TrackedPrintRequest::new(batch.device_type.clone(), batch.quantity)
                        .authoritative_factory_check();
                request.operation_id = Some(OperationId::from(format!(
                    "mining:{}:print:{}",
                    mission.mission_id, batch.batch_tag
                )));
                request
            })
            .collect::<Vec<_>>();
        let report = queue_tracked_prints_once(
            client,
            &requests,
            &options,
            &printing_blueprints,
            |update| match update {
                TrackedPrintUpdate::Preparing(assignment) => {
                    config
                        .claim_devices(std::slice::from_ref(&assignment.factory_code))
                        .map_err(|error| error.to_string())?;
                    let index = schedulable[assignment.request_index];
                    let batch = &mut mission.print_batches[index];
                    batch.factory_code.clone_from(&assignment.factory_code);
                    batch.submission_started = true;
                    let tags = vec![
                        mission.mission_tag.clone(),
                        role_tag(role_for_type(&batch.device_type)),
                        batch.batch_tag.clone(),
                    ];
                    save_plan(&config.plan_path, mission).map_err(|error| error.to_string())?;
                    Ok(Some(tags))
                }
                TrackedPrintUpdate::OperationRecorded {
                    assignment,
                    operation_id,
                } => {
                    let batch = &mut mission.print_batches[schedulable[assignment.request_index]];
                    batch.operation_id = Some(operation_id);
                    batch.submitted = true;
                    save_plan(&config.plan_path, mission).map_err(|error| error.to_string())?;
                    Ok(None)
                }
            },
        )
        .await?;
        for submission in &report.submissions {
            info!(
                purpose = ?purpose,
                factory = %submission.assignment.factory_code,
                device_type = %submission.assignment.device_type,
                quantity = submission.assignment.quantity,
                "queued mining expansion print batch through managed printing"
            );
        }
        if !report.submissions.is_empty() {
            continue;
        }
        if Instant::now() >= deadline {
            if let Some((needed, available)) = unaffordable {
                return Err(app_error(
                    io::ErrorKind::TimedOut,
                    format!(
                        "timed out waiting for hub materials; next batch needs {}, available {}",
                        format_quantities(&needed),
                        format_quantities(&available)
                    ),
                ));
            }
            return Err(app_error(
                io::ErrorKind::TimedOut,
                "timed out waiting for Autofactory queue capacity",
            ));
        }
        if let Some((needed, available)) = unaffordable {
            info!(
                needed = %format_quantities(&needed),
                available = %format_quantities(&available),
                "waiting for hub inventory before submitting next print batch"
            );
        }
        wait_for_mission_event(&mut watch, deadline, &[]).await?;
    }
}

/// Reserves one candidate batch's cost in the pass-local inventory snapshot.
fn deduct_quantities(available: &mut QuantityMap, cost: &QuantityMap) {
    for (resource, quantity) in cost {
        let remaining = available.entry(resource.clone()).or_default();
        *remaining = remaining.saturating_sub(*quantity);
    }
}

async fn wait_for_print_outputs(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
    purpose: PrintPurpose,
) -> AnyResult<()> {
    let deadline = Instant::now() + config.wait_timeout;
    let mut watch = client.events().watch().await?;
    loop {
        reconcile_print_batches(client, mission, config.claims.as_ref()).await?;
        if purpose == PrintPurpose::Site {
            progress_site_pipeline(client, config, mission).await?;
        }
        save_plan(&config.plan_path, mission)?;
        let incomplete = phase_batches(mission, purpose)
            .into_iter()
            .filter(|index| {
                i64::try_from(mission.print_batches[*index].produced_codes.len()).ok()
                    != Some(mission.print_batches[*index].quantity)
            })
            .collect::<Vec<_>>();
        if incomplete.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out waiting for {:?} print outputs: {} batch(es) incomplete",
                    purpose,
                    incomplete.len()
                ),
            ));
        }
        let wake = wait_for_mission_event(
            &mut watch,
            deadline,
            &["print.completed", "device.print_completed"],
        )
        .await?;
        let codes = mission_resource_codes(mission);
        refresh_after_fallback_wake(client, wake, &codes).await?;
    }
}

async fn hub_inventory(client: &Client, hub: &str) -> AnyResult<QuantityMap> {
    let (inventories, _) = client
        .inventory()
        .list(&api_raw::inventory::AccountInventoryQuery {
            location: Some(hub.to_owned()),
            cursor: None,
            limit: Some(50),
        })
        .await?;
    Ok(inventories
        .into_iter()
        .find(|inventory| {
            inventory
                .location
                .as_ref()
                .is_some_and(|location| location.id.as_str() == hub)
        })
        .map(|inventory| {
            inventory
                .items
                .into_iter()
                .map(|item| (item.resource, item.quantity))
                .collect()
        })
        .unwrap_or_default())
}

fn contains_quantities(available: &QuantityMap, required: &QuantityMap) -> bool {
    required
        .iter()
        .all(|(name, quantity)| available.get(name).copied().unwrap_or(0) >= *quantity)
}

async fn allocate_site_assets(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    allocate_available_site_assets(client, config, mission).await?;
    let devices = device_snapshots(client).await?;
    let incomplete_evidence = mission
        .sites
        .iter()
        .filter(|site| site.phase == SitePhase::Planned)
        .filter_map(|site| {
            let audit = audit_site(&devices, &site.system, &site.belt);
            (!audit.mutation_safe).then_some(format!("{}:{:?}", site.system, audit.issues))
        })
        .collect::<Vec<_>>();
    if !incomplete_evidence.is_empty() {
        return Err(app_error(
            io::ErrorKind::WouldBlock,
            format!(
                "mining site evidence is incomplete; waiting before mutation: {}",
                incomplete_evidence.join(", ")
            ),
        ));
    }
    let incomplete = mission
        .sites
        .iter()
        .filter(|site| site.phase == SitePhase::Planned)
        .map(|site| site.system.as_str())
        .collect::<Vec<_>>();
    if !incomplete.is_empty() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!(
                "site printing completed without enough hub devices for: {}",
                incomplete.join(", ")
            ),
        ));
    }
    Ok(())
}

async fn progress_site_pipeline(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    let allocated = allocate_available_site_assets(client, config, mission).await?;
    reconcile_carrier_claims(client, mission).await?;
    dispatch_ready_sites(client, config, mission).await?;
    if allocated > 0 {
        info!(
            allocated,
            "allocated complete mining setups while manufacturing continues"
        );
    }
    Ok(())
}

async fn allocate_available_site_assets(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<usize> {
    let devices = device_snapshots(client).await?;
    let mut used = mission
        .sites
        .iter()
        .flat_map(|site| site.assets.codes())
        .chain(
            mission
                .routes
                .iter()
                .flat_map(|route| route.controller.iter().cloned().chain(route.freighters())),
        )
        .collect::<BTreeSet<_>>();
    let mut pool = reusable_pool(&devices, &mission.hub_location, &mission.mission_tag, &used);
    let mut allocated = 0usize;
    for index in 0..mission.sites.len() {
        if mission.sites[index].phase == SitePhase::Planned {
            let audit = audit_site(
                &devices,
                &mission.sites[index].system,
                &mission.sites[index].belt,
            );
            let missing = site_shortages(&audit);
            mission.sites[index].assets = audit.assets;
            mission.sites[index].missing = missing;
            if !audit.mutation_safe {
                continue;
            }
        }
        let missing = mission.sites[index].missing.clone();
        let initial_deployment = mission.sites[index].phase == SitePhase::Planned
            && pool_can_complete_initial_deployment(&pool, &missing);
        let ward_addition = mission.sites[index].phase == SitePhase::Configuring
            && missing.len() == 1
            && missing.contains_key(SYSTEM_WARD)
            && (mission.sites[index].assets.system_ward.is_some()
                || pool_can_complete_site(&pool, &missing));
        if !initial_deployment && !ward_addition {
            continue;
        }
        fill_site_asset(
            &mut mission.sites[index].assets.mining_controller,
            MINING_CONTROLLER,
            &mut pool,
            &mut used,
        )?;
        fill_site_devices(
            &mut mission.sites[index].assets.mining_drones,
            MINING_DRONE,
            4,
            &mut pool,
            &mut used,
        )?;
        fill_site_asset(
            &mut mission.sites[index].assets.survey_controller,
            SURVEY_CONTROLLER,
            &mut pool,
            &mut used,
        )?;
        fill_site_devices(
            &mut mission.sites[index].assets.survey_drones,
            SURVEY_DRONE,
            2,
            &mut pool,
            &mut used,
        )?;
        fill_site_asset(
            &mut mission.sites[index].assets.maintenance_drone,
            MAINTENANCE_DRONE,
            &mut pool,
            &mut used,
        )?;
        if ward_addition {
            fill_site_asset(
                &mut mission.sites[index].assets.system_ward,
                SYSTEM_WARD,
                &mut pool,
                &mut used,
            )?;
            mission.sites[index].missing.remove(SYSTEM_WARD);
        } else {
            mission.sites[index]
                .missing
                .retain(|device_type, _| device_type == SYSTEM_WARD);
        }
        config.claim_devices(&site_resource_codes(&mission.sites[index]))?;
        mission.sites[index].phase = SitePhase::Ready;
        allocated += 1;
        save_plan(&config.plan_path, mission)?;
        ensure_asset_ownership(
            client,
            &mission.sites[index].assets.codes(),
            &mission.selected_replicant,
        )
        .await?;
        tag_site_assets(client, &mission.sites[index]).await?;
    }
    Ok(allocated)
}

fn pool_can_complete_initial_deployment(
    pool: &BTreeMap<String, Vec<String>>,
    missing: &QuantityMap,
) -> bool {
    missing
        .iter()
        .filter(|(device_type, _)| device_type.as_str() != SYSTEM_WARD)
        .all(|(device_type, quantity)| {
            i64::try_from(pool.get(device_type).map_or(0, Vec::len))
                .is_ok_and(|available| available >= *quantity)
        })
}

fn pool_can_complete_site(pool: &BTreeMap<String, Vec<String>>, missing: &QuantityMap) -> bool {
    missing.iter().all(|(device_type, quantity)| {
        i64::try_from(pool.get(device_type).map_or(0, Vec::len))
            .is_ok_and(|available| available >= *quantity)
    })
}

fn reusable_pool(
    devices: &[Device],
    hub: &str,
    mission_tag: &str,
    used: &BTreeSet<String>,
) -> BTreeMap<String, Vec<String>> {
    let mut pool = BTreeMap::<String, Vec<String>>::new();
    for device in devices
        .iter()
        .filter(|device| is_reusable_device(device, hub, mission_tag, used))
    {
        if let Some(device_type) = &device.device_type {
            pool.entry(device_type.as_str().to_owned())
                .or_default()
                .push(device.key.id.as_str().to_owned());
        }
    }
    for codes in pool.values_mut() {
        codes.sort();
        codes.reverse();
    }
    pool
}

fn is_reusable_device(
    device: &Device,
    hub: &str,
    mission_tag: &str,
    used: &BTreeSet<String>,
) -> bool {
    let mission_output = device.tags.iter().any(|tag| tag == mission_tag);
    device_location(device) == Some(hub)
        && device
            .status
            .as_ref()
            .is_some_and(|status| status.as_str() == "idle")
        && device.relationships.controller.is_none()
        && device.relationships.attached_to.is_none()
        && device.relationships.stowed_in.is_none()
        && device.travel.is_none()
        && (mission_output || !has_reservation_tag(device))
        && !used.contains(device.key.id.as_str())
}

fn fill_site_asset(
    target: &mut Option<String>,
    device_type: &str,
    pool: &mut BTreeMap<String, Vec<String>>,
    used: &mut BTreeSet<String>,
) -> AnyResult<()> {
    if target.is_some() {
        return Ok(());
    }
    let code = pool
        .get_mut(device_type)
        .and_then(Vec::pop)
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("no reserved {device_type} is available at the hub"),
            )
        })?;
    used.insert(code.clone());
    *target = Some(code);
    Ok(())
}

fn fill_site_devices(
    target: &mut Vec<String>,
    device_type: &str,
    required: usize,
    pool: &mut BTreeMap<String, Vec<String>>,
    used: &mut BTreeSet<String>,
) -> AnyResult<()> {
    while target.len() < required {
        let code = pool
            .get_mut(device_type)
            .and_then(Vec::pop)
            .ok_or_else(|| {
                app_error(
                    io::ErrorKind::NotFound,
                    format!("not enough reserved {device_type} devices are available at the hub"),
                )
            })?;
        used.insert(code.clone());
        target.push(code);
    }
    target.sort();
    target.dedup();
    Ok(())
}

async fn tag_site_assets(client: &Client, site: &super::SiteMission) -> AnyResult<()> {
    let roles = site_roles(&site.assets);
    for (code, role) in roles {
        reconcile_site_asset_tags(client, &code, &site.tag, role).await?;
    }
    Ok(())
}

async fn reconcile_site_asset_tags(
    client: &Client,
    code: &str,
    expected_site_tag: &str,
    role: &str,
) -> AnyResult<()> {
    let snapshot = validation::device(client, code, ValidationReason::Mutation).await?;
    let expected_role_tag = role_tag(role);
    let desired = [expected_site_tag.to_owned(), expected_role_tag.clone()];
    let add_tags = desired
        .iter()
        .filter(|tag| !snapshot.tags.contains(*tag))
        .cloned()
        .collect::<Vec<_>>();
    let remove_tags = snapshot
        .tags
        .iter()
        .filter(|tag| {
            (tag.starts_with("mine-s:") && tag.as_str() != expected_site_tag)
                || (tag.starts_with("mine-r:") && tag.as_str() != expected_role_tag.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    if add_tags.is_empty() && remove_tags.is_empty() {
        return Ok(());
    }
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    ensure_operation_accepted(
        &handle
            .configure(api_raw::devices::DeviceConfiguration {
                add_tags: (!add_tags.is_empty()).then_some(add_tags),
                remove_tags: (!remove_tags.is_empty()).then_some(remove_tags),
                ..Default::default()
            })
            .await?,
    )
    .await
}

fn site_roles(assets: &SiteAssets) -> Vec<(String, &'static str)> {
    let mut roles = Vec::new();
    if let Some(code) = &assets.mining_controller {
        roles.push((code.clone(), "mining-controller"));
    }
    roles.extend(
        assets
            .mining_drones
            .iter()
            .cloned()
            .map(|code| (code, "mining-drone")),
    );
    if let Some(code) = &assets.survey_controller {
        roles.push((code.clone(), "survey-controller"));
    }
    roles.extend(
        assets
            .survey_drones
            .iter()
            .cloned()
            .map(|code| (code, "survey-drone")),
    );
    if let Some(code) = &assets.maintenance_drone {
        roles.push((code.clone(), "maintenance"));
    }
    if let Some(code) = &assets.system_ward {
        roles.push((code.clone(), "system-ward"));
    }
    roles
}

async fn deploy_sites(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    let deadline = Instant::now() + config.wait_timeout;
    let mut watch = client.events().watch().await?;
    loop {
        reconcile(client, config, mission).await?;
        allocate_available_site_assets(client, config, mission).await?;
        if mission
            .sites
            .iter()
            .all(|site| site.phase == SitePhase::Operational)
        {
            return Ok(());
        }

        resume_site_configuration(client, config, mission).await?;
        dispatch_ready_sites(client, config, mission).await?;
        if mission
            .sites
            .iter()
            .all(|site| site.phase == SitePhase::Operational)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let pending = mission
                .sites
                .iter()
                .filter(|site| site.phase != SitePhase::Operational)
                .map(|site| format!("{}:{:?}", site.system, site.phase))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out deploying mining sites: {pending}"),
            ));
        }
        let wake = wait_for_mission_event(&mut watch, deadline, &[]).await?;
        let codes = mission_resource_codes(mission);
        refresh_after_fallback_wake(client, wake, &codes).await?;
    }
}

fn site_delivery_namespace(mission: &MiningMission, index: usize) -> uuid::Uuid {
    use sha2::{Digest, Sha256};

    let mut digest = Sha256::new();
    let site = &mission.sites[index];
    for value in std::iter::once(&mission.mission_id)
        .chain(std::iter::once(&site.belt))
        .chain(site.assets.codes().iter())
    {
        digest.update((value.len() as u64).to_be_bytes());
        digest.update(value.as_bytes());
    }
    let hash = digest.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash[..16]);
    uuid::Uuid::from_bytes(bytes)
}

async fn resume_site_delivery(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
    index: usize,
) -> AnyResult<()> {
    let site = &mission.sites[index];
    let carrier = site.carrier.as_ref().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            "outbound mining site lost its carrier",
        )
    })?;
    let mut payload = Vec::new();
    for code in site.assets.codes() {
        if site.missing.contains_key(SYSTEM_WARD)
            && site.assets.system_ward.as_deref() == Some(code.as_str())
        {
            continue;
        }
        let snapshot = validation::device(client, &code, ValidationReason::Mutation).await?;
        payload.push(PayloadDevice {
            code,
            device_type: device_type(&snapshot).unwrap_or_default().to_owned(),
            origin: mission.hub_location.clone(),
        });
    }
    deliver_devices_with(
        client,
        &site.belt,
        &payload,
        std::slice::from_ref(carrier),
        DeliveryOptions {
            wait_timeout: config.wait_timeout,
            poll_interval: POLL_INTERVAL,
            unfurl_modular_payload: false,
            return_transports: true,
            operation_namespace: Some(site_delivery_namespace(mission, index)),
            ..DeliveryOptions::default()
        },
    )
    .await?;
    mission.sites[index].phase = SitePhase::Deploying;
    save_plan(&config.plan_path, mission)
}

async fn dispatch_ready_sites(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    // An Outbound checkpoint owns the exact payload and carrier even if delivery
    // completed before the phase write. Transport reconciles arrived/attached
    // devices and resumes the same managed operations, never a new shipment.
    for index in 0..mission.sites.len() {
        if mission.sites[index].phase != SitePhase::Outbound {
            continue;
        }
        resume_site_delivery(client, config, mission, index).await?;
        configure_site(client, config, mission, index).await?;
    }
    let devices = device_snapshots(client).await?;
    let in_flight = mission
        .sites
        .iter()
        .filter(|site| {
            site.carrier.is_some()
                && matches!(site.phase, SitePhase::Outbound | SitePhase::Deploying)
        })
        .count();
    let available_slots = config.max_concurrency.saturating_sub(in_flight);
    if available_slots == 0 {
        return Ok(());
    }
    let claimed = mission
        .sites
        .iter()
        .filter_map(|site| site.carrier.clone())
        .collect::<BTreeSet<_>>();
    let mut carriers = devices
        .iter()
        .filter(|device| {
            device_type(device) == Some(SURGE_CARRIER)
                && device_location(device) == Some(mission.hub_location.as_str())
                && device
                    .status
                    .as_ref()
                    .is_some_and(|status| status.as_str() == "idle")
                && device.relationships.attached_to.is_none()
                && device.relationships.stowed_in.is_none()
                && device.relationships.attached_devices.is_empty()
                && device.travel.is_none()
                && !claimed.contains(device.key.id.as_str())
                && !has_reservation_tag(device)
        })
        .map(|device| device.key.id.as_str().to_owned())
        .collect::<Vec<_>>();
    carriers.sort();
    carriers.reverse();

    let ready = mission
        .sites
        .iter()
        .enumerate()
        .filter_map(|(index, site)| (site.phase == SitePhase::Ready).then_some(index))
        .take(available_slots)
        .collect::<Vec<_>>();
    let mut deliveries = Vec::new();
    for index in ready {
        let defer_ward = mission.sites[index].missing.contains_key(SYSTEM_WARD);
        let ward = mission.sites[index].assets.system_ward.as_deref();
        let payload = mission.sites[index]
            .assets
            .codes()
            .into_iter()
            .filter(|code| !defer_ward || ward != Some(code.as_str()))
            .filter_map(|code| {
                let device = find_device(&devices, &code)?;
                (device_location(device) == Some(mission.hub_location.as_str())).then(|| {
                    PayloadDevice {
                        code,
                        device_type: device_type(device).unwrap_or_default().to_owned(),
                        origin: mission.hub_location.clone(),
                    }
                })
            })
            .collect::<Vec<_>>();
        if payload.is_empty() {
            mission.sites[index].carrier = None;
            mission.sites[index].phase = SitePhase::Deploying;
            save_plan(&config.plan_path, mission)?;
            configure_site(client, config, mission, index).await?;
            continue;
        }
        let carrier = if let Some(carrier) = mission.sites[index].carrier.clone() {
            carrier
        } else {
            let Some(carrier) = carriers.pop() else {
                break;
            };
            carrier
        };
        validation::device(client, &carrier, ValidationReason::CapacitySensitive).await?;
        for item in &payload {
            validation::device(client, &item.code, ValidationReason::CapacitySensitive).await?;
        }
        mission.sites[index].carrier = Some(carrier.clone());
        config.claim_devices(std::slice::from_ref(&carrier))?;
        mission.sites[index].phase = SitePhase::Outbound;
        save_plan(&config.plan_path, mission)?;
        add_tags(
            client,
            &carrier,
            &[mission.mission_tag.clone(), role_tag("carrier")],
        )
        .await?;
        ensure_asset_ownership(
            client,
            std::slice::from_ref(&carrier),
            &mission.selected_replicant,
        )
        .await?;
        deliveries.push((index, carrier, payload, mission.sites[index].belt.clone()));
    }
    let results = join_all(deliveries.iter().map(|(index, carrier, payload, belt)| {
        deliver_devices_with(
            client,
            belt,
            payload,
            std::slice::from_ref(carrier),
            DeliveryOptions {
                wait_timeout: config.wait_timeout,
                poll_interval: POLL_INTERVAL,
                unfurl_modular_payload: false,
                return_transports: true,
                operation_namespace: Some(site_delivery_namespace(mission, *index)),
                ..DeliveryOptions::default()
            },
        )
    }))
    .await;
    for ((index, carrier, payload, _), result) in deliveries.into_iter().zip(results) {
        result?;
        mission.sites[index].phase = SitePhase::Deploying;
        save_plan(&config.plan_path, mission)?;
        configure_site(client, config, mission, index).await?;
        info!(
            system = %mission.sites[index].system,
            carrier = %carrier,
            devices = payload.len(),
            destination = %mission.sites[index].belt,
            "delivered and configured mining site"
        );
    }
    Ok(())
}

async fn resume_site_configuration(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    let pending = mission
        .sites
        .iter()
        .enumerate()
        .filter_map(|(index, site)| {
            matches!(
                site.phase,
                SitePhase::Deploying
                    | SitePhase::Adopting
                    | SitePhase::Verifying
                    | SitePhase::Configuring
            )
            .then_some(index)
        })
        .collect::<Vec<_>>();
    for index in pending {
        configure_site(client, config, mission, index).await?;
    }
    Ok(())
}

async fn configure_site(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
    index: usize,
) -> AnyResult<()> {
    let mut site = mission.sites[index].clone();
    validate_codes(
        client,
        &site_resource_codes(&site),
        ValidationReason::StateConflict,
    )
    .await?;
    let preflight = audit_site(&device_snapshots(client).await?, &site.system, &site.belt);
    if !preflight.mutation_safe {
        let missing = site_shortages(&preflight);
        mission.sites[index].assets = preflight.assets;
        mission.sites[index].missing = missing;
        mission.sites[index].phase = SitePhase::Verifying;
        save_plan(&config.plan_path, mission)?;
        return Err(app_error(
            io::ErrorKind::WouldBlock,
            format!(
                "mining site {} has incomplete authoritative evidence: {:?}",
                site.belt, preflight.issues
            ),
        ));
    }
    let missing = site_shortages(&preflight);
    site.assets = preflight.assets;
    site.missing = missing;
    config.claim_devices(&site_resource_codes(&site))?;
    mission.sites[index].assets = site.assets.clone();
    mission.sites[index].missing = site.missing.clone();
    save_plan(&config.plan_path, mission)?;
    let ward_assigned = site.assets.system_ward.is_some();
    let mut ward_pending = false;
    for code in site.assets.codes() {
        let snapshot = validation::device(client, &code, ValidationReason::StateConflict).await?;
        let is_ward = site.assets.system_ward.as_deref() == Some(code.as_str());
        let in_place = if is_ward {
            device_is_in_system(&snapshot, &site.system)
        } else {
            device_location(&snapshot) == Some(site.belt.as_str())
        };
        if is_ward && !in_place {
            ward_pending = true;
            continue;
        }
        if !in_place {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("site asset {code} has not arrived at {}", site.belt),
            ));
        }
    }
    tag_site_assets(client, &site).await?;
    if ward_assigned && !ward_pending {
        ensure_site_protection(client, &site).await?;
    }
    mission.sites[index].phase = SitePhase::Adopting;
    save_plan(&config.plan_path, mission)?;
    let mining_controller =
        site.assets.mining_controller.as_deref().ok_or_else(|| {
            app_error(io::ErrorKind::InvalidData, "site has no mining controller")
        })?;
    ensure_adoption(
        client,
        &mission.mission_id,
        mining_controller,
        &site.assets.mining_drones,
    )
    .await?;
    let survey_controller =
        site.assets.survey_controller.as_deref().ok_or_else(|| {
            app_error(io::ErrorKind::InvalidData, "site has no survey controller")
        })?;
    ensure_adoption(
        client,
        &mission.mission_id,
        survey_controller,
        &site.assets.survey_drones,
    )
    .await?;
    mission.sites[index].phase = SitePhase::Verifying;
    save_plan(&config.plan_path, mission)?;
    let mining_controller = site.assets.mining_controller.as_deref().unwrap_or_default();
    let mining_snapshot =
        validation::device(client, mining_controller, ValidationReason::StateConflict).await?;
    let mining_handle = match client.devices().cached(mining_controller) {
        Some(handle) => handle,
        None => client.devices().get(mining_controller).await?,
    };
    let mining = mining_handle.as_mining_controller()?;
    if site_directive_missing(&mining_snapshot, "deplete_smallest")? {
        let operation = mining
            .set_directive(MiningDirective::DepleteSmallest)
            .await?;
        ensure_operation_accepted(&operation).await?;
    }
    let mining_snapshot =
        validation::device(client, mining_controller, ValidationReason::StateConflict).await?;
    if mining_snapshot.status.is_none() {
        return Err(app_error(
            io::ErrorKind::WouldBlock,
            "mining controller status is unknown before launch",
        ));
    }
    if mining_snapshot
        .status
        .as_ref()
        .is_none_or(|status| status.as_str() != "coordinating")
    {
        ensure_operation_accepted(&mining.launch().await?).await?;
    }

    let survey_controller = site.assets.survey_controller.as_deref().unwrap_or_default();
    let survey_snapshot =
        validation::device(client, survey_controller, ValidationReason::StateConflict).await?;
    let survey_handle = match client.devices().cached(survey_controller) {
        Some(handle) => handle,
        None => client.devices().get(survey_controller).await?,
    };
    let survey = survey_handle.as_survey_controller()?;
    if site_directive_missing(&survey_snapshot, "belt_search")? {
        ensure_operation_accepted(&survey.set_directive(SurveyDirective::BeltSearch).await?)
            .await?;
    }
    let survey_snapshot =
        validation::device(client, survey_controller, ValidationReason::StateConflict).await?;
    if survey_snapshot.status.is_none() {
        return Err(app_error(
            io::ErrorKind::WouldBlock,
            "survey controller status is unknown before launch",
        ));
    }
    if survey_snapshot
        .status
        .as_ref()
        .is_none_or(|status| status.as_str() != "coordinating")
    {
        ensure_operation_accepted(&survey.launch().await?).await?;
    }

    let maintenance =
        site.assets.maintenance_drone.as_deref().ok_or_else(|| {
            app_error(io::ErrorKind::InvalidData, "site has no maintenance drone")
        })?;
    let maintenance_snapshot =
        validation::device(client, maintenance, ValidationReason::StateConflict).await?;
    let maintenance_handle = match client.devices().cached(maintenance) {
        Some(handle) => handle,
        None => client.devices().get(maintenance).await?,
    };
    if site_directive_missing(&maintenance_snapshot, "patrol")? {
        ensure_operation_accepted(
            &maintenance_handle
                .command(api_raw::devices::DeviceCommand::SetDirective {
                    directive: "patrol".into(),
                    configuration: None,
                    notify: None,
                })
                .await?,
        )
        .await?;
    }

    if ward_pending {
        mission.sites[index].phase = SitePhase::Configuring;
        save_plan(&config.plan_path, mission)?;
        return Ok(());
    }

    let deadline = Instant::now() + VERIFY_TIMEOUT;
    let mut watch = client.events().watch().await?;
    loop {
        let devices = validate_codes(
            client,
            &site_resource_codes(&site),
            ValidationReason::StateConflict,
        )
        .await?;
        if audit_site(&devices, &site.system, &site.belt).operational {
            break;
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "mining setup at {} did not verify as operational",
                    site.belt
                ),
            ));
        }
        let wake = wait_for_mission_event(&mut watch, deadline, &[]).await?;
        let codes = site_resource_codes(&site);
        refresh_after_fallback_wake(client, wake, &codes).await?;
    }
    info!(system = %site.system, belt = %site.belt, "mining site operational");
    mission.sites[index].phase = SitePhase::Operational;
    save_plan(&config.plan_path, mission)?;
    Ok(())
}

async fn ensure_site_protection(client: &Client, site: &super::SiteMission) -> AnyResult<()> {
    let catalogue = client.galaxy().catalogue();
    let hub_protection = catalogue
        .iter()
        .any(|star| star.key.id.as_str() == site.system && star.has_hub == Some(true));
    let Some(ward) = site.assets.system_ward.as_deref() else {
        if hub_protection {
            return Ok(());
        }
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("mining site {} has no System Ward", site.system),
        ));
    };
    let snapshot = validation::device(client, ward, ValidationReason::StateConflict).await?;
    if !device_is_in_system(&snapshot, &site.system) {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("System Ward {ward} is not deployed in {}", site.system),
        ));
    }
    let handle = match client.devices().cached(ward) {
        Some(handle) => handle,
        None => client.devices().get(ward).await?,
    };
    ensure_operation_accepted(
        &handle
            .command(api_raw::devices::DeviceCommand::Activate)
            .await?,
    )
    .await?;
    Ok(())
}

async fn ensure_adoption(
    client: &Client,
    mission_id: &str,
    controller: &str,
    devices: &[String],
) -> AnyResult<()> {
    let snapshots = device_snapshots(client).await?;
    let missing = devices
        .iter()
        .filter(|code| {
            find_device(&snapshots, code)
                .is_none_or(|device| controller_code(device) != Some(controller))
        })
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let controller_state =
        validation::device(client, controller, ValidationReason::StateConflict).await?;
    if !super::site_asset_usable(&controller_state)
        || controller_state.location.is_none()
        || controller_state.status.is_none()
    {
        return Err(app_error(
            io::ErrorKind::WouldBlock,
            "controller authority changed before adoption",
        ));
    }
    let mut missing = Vec::new();
    for code in devices {
        let device = validation::device(client, code, ValidationReason::StateConflict).await?;
        if controller_code(&device) == Some(controller) {
            continue;
        }
        if !super::site_asset_usable(&device)
            || device.location != controller_state.location
            || device.status.is_none()
            || find_device(&snapshots, code).is_none_or(|before| {
                before.relationships.controller != device.relationships.controller
            })
        {
            return Err(app_error(
                io::ErrorKind::WouldBlock,
                format!("device {code} is no longer free at controller {controller}"),
            ));
        }
        missing.push(code.clone());
    }
    let handle = match client.devices().cached(controller) {
        Some(handle) => handle,
        None => client.devices().get(controller).await?,
    };
    for code in &missing {
        let operation = handle
            .command_with_id(
                OperationId::from(format!("mining:{mission_id}:adopt:{controller}:{code}")),
                api_raw::devices::DeviceCommand::Adopt(targets(std::slice::from_ref(code))),
            )
            .await?;
        ensure_operation_accepted(&operation).await?;
    }
    Ok(())
}

async fn allocate_route_assets(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    let devices = device_snapshots(client).await?;
    let mut used = mission
        .sites
        .iter()
        .flat_map(|site| site.assets.codes())
        .chain(
            mission
                .routes
                .iter()
                .flat_map(|route| route.controller.iter().cloned().chain(route.freighters())),
        )
        .collect::<BTreeSet<_>>();
    let mut pool = reusable_pool(&devices, &mission.hub_location, &mission.mission_tag, &used);
    for index in 0..mission.routes.len() {
        if mission.routes[index].phase == RoutePhase::Active {
            tag_route_assets(client, &mission.routes[index]).await?;
            continue;
        }
        fill_site_asset(
            &mut mission.routes[index].controller,
            TRANSPORT_CONTROLLER,
            &mut pool,
            &mut used,
        )?;
        if mission.routes[index].freighter.is_none() {
            fill_site_asset(
                &mut mission.routes[index].freighter,
                CARGO_FREIGHTER,
                &mut pool,
                &mut used,
            )?;
        }
        while mission.routes[index].freighters().len() < mission.routes[index].desired_freighters {
            let mut next = None;
            fill_site_asset(&mut next, CARGO_FREIGHTER, &mut pool, &mut used)?;
            if let Some(code) = next {
                mission.routes[index].additional_freighters.push(code);
            }
        }
        mission.routes[index].additional_freighters.sort();
        mission.routes[index].additional_freighters.dedup();
        let codes = mission.routes[index]
            .controller
            .iter()
            .cloned()
            .chain(mission.routes[index].freighters())
            .collect::<Vec<_>>();
        config.claim_devices(&codes)?;
        mission.routes[index].phase = RoutePhase::Ready;
        save_plan(&config.plan_path, mission)?;
        ensure_asset_ownership(client, &codes, &mission.selected_replicant).await?;
        tag_route_assets(client, &mission.routes[index]).await?;
    }
    Ok(())
}

async fn tag_route_assets(client: &Client, route: &super::RouteMission) -> AnyResult<()> {
    if let Some(controller) = &route.controller {
        add_tags(
            client,
            controller,
            &[route.tag.clone(), role_tag("transport-controller")],
        )
        .await?;
    }
    for freighter in route.freighters() {
        add_tags(
            client,
            &freighter,
            &[route.tag.clone(), role_tag("cargo-freighter")],
        )
        .await?;
    }
    Ok(())
}

async fn activate_routes(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    for index in 0..mission.routes.len() {
        if mission.routes[index].phase == RoutePhase::Active {
            tag_route_assets(client, &mission.routes[index]).await?;
            continue;
        }
        mission.routes[index].phase = RoutePhase::Activating;
        save_plan(&config.plan_path, mission)?;
        configure_route(
            client,
            &mission.mission_id,
            &mission.routes[index],
            &mission.hub_location,
        )
        .await?;
        mission.routes[index].phase = RoutePhase::Active;
        save_plan(&config.plan_path, mission)?;
    }
    Ok(())
}

fn site_directive_missing(device: &Device, expected: &str) -> AnyResult<bool> {
    match super::site_directive_evidence(device, expected) {
        EvidenceState::Present => Ok(false),
        EvidenceState::Absent => Ok(true),
        EvidenceState::Unknown => Err(app_error(
            io::ErrorKind::WouldBlock,
            format!(
                "device {} has incomplete active directive evidence",
                device.key.id
            ),
        )),
    }
}

async fn configure_route(
    client: &Client,
    mission_id: &str,
    route: &super::RouteMission,
    hub: &str,
) -> AnyResult<()> {
    let controller = route.controller.as_deref().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            "route has no transport controller",
        )
    })?;
    let freighters = route.freighters();
    if freighters.len() < route.desired_freighters {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "route has {} Cargo Freighter(s), but {} are required",
                freighters.len(),
                route.desired_freighters
            ),
        ));
    }
    let initial_devices = validate_codes(
        client,
        &route_resource_codes(route),
        ValidationReason::StateConflict,
    )
    .await?;
    let initial_audit =
        transport_service_present(&initial_devices, &route.system, &route.belt, hub);
    if initial_audit.controller_operational == EvidenceState::Unknown
        || initial_audit.controller_directive == EvidenceState::Unknown
        || initial_audit.controller_configuration == EvidenceState::Unknown
    {
        return Err(app_error(
            io::ErrorKind::WouldBlock,
            "transport controller evidence is incomplete before mutation",
        ));
    }
    ensure_adoption(client, mission_id, controller, &freighters).await?;
    let handle = match client.devices().cached(controller) {
        Some(handle) => handle,
        None => client.devices().get(controller).await?,
    };
    let transport = handle.as_transport_controller()?;
    if initial_audit.controller_directive == EvidenceState::Absent
        || initial_audit.controller_configuration == EvidenceState::Absent
    {
        let directive = if super::location_is_in_system(hub, &route.system) {
            TransportDirective::Shuttle {
                collect: route.belt.clone(),
                deliver: hub.to_owned(),
                priority: vec!["rares".into(), "volatiles".into()],
            }
        } else {
            TransportDirective::Ferry {
                collect: route.belt.clone(),
                deliver: hub.to_owned(),
                priority: vec!["rares".into(), "volatiles".into()],
            }
        };
        ensure_operation_accepted(&transport.set_directive(directive).await?).await?;
    }
    let snapshot = validation::device(client, controller, ValidationReason::StateConflict).await?;
    if super::transport_controller_status_state(&snapshot) == EvidenceState::Unknown {
        return Err(app_error(
            io::ErrorKind::WouldBlock,
            "transport controller status is incomplete before launch",
        ));
    }
    if snapshot
        .status
        .as_ref()
        .is_none_or(|status| status.as_str() != "coordinating")
    {
        ensure_operation_accepted(&transport.launch().await?).await?;
    }
    let deadline = Instant::now() + VERIFY_TIMEOUT;
    let mut watch = client.events().watch().await?;
    loop {
        let devices = validate_codes(
            client,
            &route_resource_codes(route),
            ValidationReason::StateConflict,
        )
        .await?;
        let audit = transport_service_present(&devices, &route.system, &route.belt, hub);
        if audit.state == EvidenceState::Present
            && audit.usable_freighter_count() >= route.desired_freighters
        {
            break;
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "transport route for {} did not verify as active",
                    route.system
                ),
            ));
        }
        let wake = wait_for_mission_event(&mut watch, deadline, &[]).await?;
        let codes = route_resource_codes(route);
        refresh_after_fallback_wake(client, wake, &codes).await?;
    }
    info!(
        system = %route.system,
        collect = %route.belt,
        deliver = %hub,
        "mining transport route active"
    );
    Ok(())
}

async fn return_and_release_carriers(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    let deadline = Instant::now() + config.wait_timeout;
    let mut watch = client.events().watch().await?;
    loop {
        let mut pending = 0usize;
        for site in &mut mission.sites {
            let Some(carrier) = site.carrier.clone() else {
                continue;
            };
            let snapshot =
                validation::device(client, &carrier, ValidationReason::StateConflict).await?;
            if snapshot.travel.is_none()
                && device_location(&snapshot) == Some(mission.hub_location.as_str())
                && snapshot.relationships.attached_devices.is_empty()
            {
                remove_tags(
                    client,
                    &carrier,
                    &[mission.mission_tag.clone(), role_tag("carrier")],
                )
                .await?;
                site.carrier = None;
                continue;
            }
            pending += 1;
            if snapshot.travel.is_none()
                && device_location(&snapshot) == Some(site.belt.as_str())
                && snapshot.relationships.attached_devices.is_empty()
            {
                start_travel(client, &carrier, &mission.hub_location).await?;
            }
        }
        save_plan(&config.plan_path, mission)?;
        if pending == 0 {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for {pending} Surge Carrier(s) to return"),
            ));
        }
        let wake = wait_for_mission_event(&mut watch, deadline, &["travel.arrived"]).await?;
        let codes = mission
            .sites
            .iter()
            .filter_map(|site| site.carrier.clone())
            .collect::<Vec<_>>();
        refresh_after_fallback_wake(client, wake, &codes).await?;
    }
}

fn mission_tag_aliases(mission: &MiningMission) -> Vec<String> {
    std::iter::once(mission.mission_tag.clone())
        .chain(mission.legacy_mission_tags.iter().cloned())
        .collect()
}

async fn migrate_legacy_mission_devices(
    client: &Client,
    mission: &MiningMission,
    claims: Option<&super::MiningWorkflowClaims>,
) -> AnyResult<usize> {
    if mission.legacy_mission_tags.is_empty() {
        return Ok(0);
    }
    let devices = device_snapshots(client).await?;
    let mut migrated = 0usize;
    for device in devices {
        let removable = device
            .tags
            .iter()
            .filter(|tag| {
                mission.legacy_mission_tags.contains(*tag)
                    && is_opaque_mining_mission_tag(tag)
                    && *tag != &mission.mission_tag
            })
            .cloned()
            .collect::<Vec<_>>();
        if removable.is_empty() {
            continue;
        }
        let current =
            validation::device(client, device.key.id.as_str(), ValidationReason::Mutation).await?;
        if let Some(claims) = claims {
            claims.acquire_devices(&[device.key.id.as_str().to_owned()])?;
        }
        let handle = match client.devices().cached(device.key.id.as_str()) {
            Some(handle) => handle,
            None => client.devices().get(device.key.id.as_str()).await?,
        };
        let add_tags = (!current.tags.contains(&mission.mission_tag))
            .then_some(vec![mission.mission_tag.clone()]);
        ensure_operation_accepted(
            &handle
                .configure(api_raw::devices::DeviceConfiguration {
                    add_tags,
                    remove_tags: Some(removable.clone()),
                    ..Default::default()
                })
                .await?,
        )
        .await?;
        migrated += 1;
        info!(
            device = %device.key.id.as_str(),
            new_tag = %mission.mission_tag,
            old_tags = ?removable,
            "migrated legacy mining mission tag"
        );
    }
    Ok(migrated)
}

async fn cleanup_transient_tags(client: &Client, mission: &MiningMission) -> AnyResult<()> {
    let batch_tags = mission
        .print_batches
        .iter()
        .map(|batch| batch.batch_tag.clone())
        .collect::<BTreeSet<_>>();
    for code in mission_resource_codes(mission) {
        let snapshot = validation::device(client, &code, ValidationReason::Mutation).await?;
        let mut removable = vec![mission.mission_tag.clone()];
        removable.extend(
            snapshot
                .tags
                .iter()
                .filter(|tag| batch_tags.contains(*tag))
                .cloned(),
        );
        remove_tags(client, &code, &removable).await?;
    }
    Ok(())
}

async fn ensure_asset_ownership(
    client: &Client,
    codes: &[String],
    selected_replicant: &str,
) -> AnyResult<()> {
    for code in codes {
        let snapshot = validation::device(client, code, ValidationReason::Mutation).await?;
        if snapshot
            .relationships
            .assigned_replicant
            .as_ref()
            .is_none_or(|replicant| replicant.id.as_str() != selected_replicant)
        {
            let handle = match client.devices().cached(code) {
                Some(handle) => handle,
                None => client.devices().get(code).await?,
            };
            ensure_operation_accepted(&handle.change_owner(selected_replicant).await?).await?;
        }
    }
    Ok(())
}

async fn add_tags(client: &Client, code: &str, desired: &[String]) -> AnyResult<()> {
    let snapshot = validation::device(client, code, ValidationReason::Mutation).await?;
    let missing = desired
        .iter()
        .filter(|tag| !snapshot.tags.contains(*tag))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    ensure_operation_accepted(
        &handle
            .configure(api_raw::devices::DeviceConfiguration {
                add_tags: Some(missing),
                ..Default::default()
            })
            .await?,
    )
    .await
}

async fn remove_tags(client: &Client, code: &str, removable: &[String]) -> AnyResult<()> {
    let snapshot = validation::device(client, code, ValidationReason::Mutation).await?;
    let present = removable
        .iter()
        .filter(|tag| snapshot.tags.contains(*tag))
        .cloned()
        .collect::<Vec<_>>();
    if present.is_empty() {
        return Ok(());
    }
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    ensure_operation_accepted(
        &handle
            .configure(api_raw::devices::DeviceConfiguration {
                remove_tags: Some(present),
                ..Default::default()
            })
            .await?,
    )
    .await
}

fn targets(devices: &[String]) -> api_raw::devices::TargetsCommand {
    api_raw::devices::TargetsCommand {
        device: None,
        devices: Some(Value::Array(
            devices.iter().cloned().map(Value::String).collect(),
        )),
        target: None,
        targets: None,
    }
}

async fn start_travel(client: &Client, code: &str, destination: &str) -> AnyResult<()> {
    let snapshot = validation::device(client, code, ValidationReason::StateConflict).await?;
    if snapshot.travel.is_none() && device_location(&snapshot) == Some(destination) {
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
                format!("device {code} is travelling to {planned:?}, not {destination}"),
            ));
        }
        return Ok(());
    }
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
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    let operation = handle
        .command(api_raw::devices::DeviceCommand::Travel {
            destination: destination.to_owned(),
            dry_run: None,
            via,
        })
        .await?;
    ensure_operation_accepted(&operation).await
}

async fn ensure_operation_accepted(operation: &Operation) -> AnyResult<()> {
    // Managed mutation construction has already completed the one durable HTTP
    // submission attempt. Read that classification immediately; waiting for a
    // terminal evidence state here serializes independent queue submissions.
    let outcome = operation.outcome().await?;
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

fn role_for_type(device_type_name: &str) -> &'static str {
    match device_type_name {
        MINING_CONTROLLER => "mining-controller",
        MINING_DRONE => "mining-drone",
        SURVEY_CONTROLLER => "survey-controller",
        SURVEY_DRONE => "survey-drone",
        MAINTENANCE_DRONE => "maintenance",
        TRANSPORT_CONTROLLER => "transport-controller",
        CARGO_FREIGHTER => "cargo-freighter",
        SURGE_CARRIER => "carrier",
        _ => "device",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_client::domain::{
        AccessScope, DeviceId, DeviceKey, DeviceRelationships, DeviceStatus, LocationKey,
    };

    fn device(code: &str) -> Device {
        Device {
            key: DeviceKey::live(DeviceId::from(code)),
            device_type: Some(replicant_client::domain::DeviceType::from(MINING_DRONE)),
            status: Some(DeviceStatus::from("idle")),
            location: Some(LocationKey::live("HUB-BELT-1".into())),
            deployed_at: None,
            in_control_range: None,
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            settings: Default::default(),
            relationships: DeviceRelationships::default(),
            cargo: Default::default(),
            cargo_capacity: None,
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            runtime: Default::default(),
            access: AccessScope::Owned,
        }
    }

    #[test]
    fn grouped_pending_batch_splits_into_distinct_units() {
        let original_tag = "mine-b:0000000000000001";
        let batch = ExecutionPrintBatch {
            purpose: PrintPurpose::Site,
            factory_code: "AF1".into(),
            device_type: MINING_DRONE.into(),
            quantity: 3,
            projected_finish_seconds: 300.0,
            batch_tag: original_tag.into(),
            submission_started: false,
            submitted: false,
            operation_id: None,
            produced_codes: Vec::new(),
        };
        let units = unit_print_batches(batch);
        assert_eq!(units.len(), 3);
        assert!(units.iter().all(|unit| unit.quantity == 1));
        assert_eq!(units[0].batch_tag, original_tag);
        assert_eq!(
            units
                .iter()
                .map(|unit| unit.batch_tag.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );
    }

    #[test]
    fn site_pipeline_waits_for_one_complete_setup() {
        let missing = replicant_mining_planner::mining_site_requirements();
        let mut pool = BTreeMap::<String, Vec<String>>::new();
        pool.insert(MINING_CONTROLLER.into(), vec!["mc".into()]);
        pool.insert(
            MINING_DRONE.into(),
            ["m1", "m2", "m3", "m4"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        );
        pool.insert(SURVEY_CONTROLLER.into(), vec!["sc".into()]);
        pool.insert(
            SURVEY_DRONE.into(),
            ["s1", "s2"].into_iter().map(str::to_owned).collect(),
        );

        assert!(!pool_can_complete_site(&pool, &missing));
        pool.insert(MAINTENANCE_DRONE.into(), vec!["md".into()]);
        assert!(pool_can_complete_site(&pool, &missing));
    }

    #[test]
    fn initial_site_deployment_does_not_wait_for_system_ward() {
        let mut missing = replicant_mining_planner::mining_site_requirements();
        missing.insert(SYSTEM_WARD.into(), 1);
        let mut pool = BTreeMap::<String, Vec<String>>::new();
        pool.insert(MINING_CONTROLLER.into(), vec!["mc".into()]);
        pool.insert(
            MINING_DRONE.into(),
            ["m1", "m2", "m3", "m4"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
        );
        pool.insert(SURVEY_CONTROLLER.into(), vec!["sc".into()]);
        pool.insert(
            SURVEY_DRONE.into(),
            ["s1", "s2"].into_iter().map(str::to_owned).collect(),
        );
        pool.insert(MAINTENANCE_DRONE.into(), vec!["md".into()]);

        assert!(pool_can_complete_initial_deployment(&pool, &missing));
        assert!(!pool_can_complete_site(&pool, &missing));
    }

    #[test]
    fn site_validation_scope_contains_only_selected_assets_and_carrier() {
        let site = super::super::SiteMission {
            system: "SOL".into(),
            belt: "SOL-BELT-1".into(),
            density: "dense".into(),
            tag: "mine-s:sol".into(),
            phase: SitePhase::Ready,
            assets: SiteAssets {
                mining_controller: Some("MC".into()),
                mining_drones: ["M1", "M2", "M3", "M4"]
                    .into_iter()
                    .map(str::to_owned)
                    .collect(),
                survey_controller: Some("SC".into()),
                survey_drones: ["S1", "S2"].into_iter().map(str::to_owned).collect(),
                maintenance_drone: Some("MD".into()),
                system_ward: Some("WARD".into()),
            },
            missing: QuantityMap::new(),
            carrier: Some("CARRIER".into()),
        };

        assert_eq!(
            site_resource_codes(&site),
            [
                "CARRIER", "M1", "M2", "M3", "M4", "MC", "MD", "S1", "S2", "SC", "WARD"
            ]
        );
    }

    #[test]
    fn reusable_device_rejects_attached_reserved_and_used_assets() {
        let used = BTreeSet::new();
        let plain = device("plain");
        assert!(is_reusable_device(&plain, "HUB-BELT-1", "mission", &used));

        let mut attached = device("attached");
        attached.relationships.attached_to = Some(DeviceKey::live(DeviceId::from("carrier")));
        assert!(!is_reusable_device(
            &attached,
            "HUB-BELT-1",
            "mission",
            &used
        ));

        let mut reserved = device("reserved");
        reserved.tags.push("mine-m:other".into());
        assert!(!is_reusable_device(
            &reserved,
            "HUB-BELT-1",
            "mission",
            &used
        ));
        reserved.tags.push("mission".into());
        assert!(is_reusable_device(
            &reserved,
            "HUB-BELT-1",
            "mission",
            &used
        ));

        assert!(!is_reusable_device(
            &plain,
            "HUB-BELT-1",
            "mission",
            &["plain".to_owned()].into_iter().collect()
        ));
    }

    #[tokio::test]
    async fn mining_restart_print_acceptance_uses_queue_or_partial_output_without_resubmission() {
        use replicant_client::{SecretString, StartupPolicy, raw::Url};
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        for evidence in ["queue", "partial_output", "ambiguous"] {
            let server = MockServer::start().await;
            let queue = if evidence == "queue" {
                serde_json::json!([{"tags": ["mine-m:sol", "mine-b:one"]}])
            } else {
                serde_json::json!([])
            };
            Mock::given(method("GET"))
                .and(path("/v1/devices/AF"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "device_code": "AF", "device_type": "autofactory",
                    "location": "SOL-BELT-1", "status": "idle", "print_queue": queue
                })))
                .mount(&server)
                .await;
            let client = Client::builder()
                .base_url(Url::parse(&server.uri()).expect("URL"))
                .authentication_token(SecretString::from("fixture"))
                .in_memory()
                .startup_policy(StartupPolicy::RestoreOnly)
                .start()
                .await
                .expect("client");
            let mut mission: MiningMission = serde_json::from_value(serde_json::json!({
                "version": 1, "mission_id": "legacy", "mission_tag": "mine-m:sol",
                "phase": "deploying_sites", "selected_replicant": "WORKER",
                "hub_location": "SOL-BELT-1", "sites": [], "routes": [],
                "print_batches": [{
                    "purpose": "site", "factory_code": "AF", "device_type": "mining_drone",
                    "quantity": 2, "projected_finish_seconds": 10.0,
                    "batch_tag": "mine-b:one", "submission_started": true, "submitted": false,
                    "operation_id": null,
                    "produced_codes": if evidence == "partial_output" { vec!["OUTPUT"] } else { vec![] }
                }],
                "site_print_requirements": {}, "route_print_requirements": {},
                "total_material_cost": {}, "warnings": []
            })).expect("legacy print checkpoint");
            for _ in 0..2 {
                let result = reconcile_print_batches(&client, &mut mission, None).await;
                if evidence == "ambiguous" {
                    assert!(result.is_err());
                    assert!(!mission.print_batches[0].submitted);
                } else {
                    result.expect("reconcile accepted print");
                    assert!(mission.print_batches[0].submitted);
                }
            }
            assert!(
                server
                    .received_requests()
                    .await
                    .expect("requests")
                    .iter()
                    .all(|request| request.method == "GET")
            );
            client.close().await.expect("close");
        }
    }

    #[tokio::test]
    async fn mining_print_output_cannot_be_allocated_before_or_after_owner_reconciliation() {
        use replicant_client::{SecretString, StartupPolicy, raw::Url};
        use replicant_workflow::{
            NewWorkflow, RequirementScope, ResourceRequirement, WorkItemSpec, WorkflowKind,
            WorkflowRepository,
        };
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };
        let server = MockServer::start().await;
        for (code, body) in [
            (
                "AF",
                serde_json::json!({"device_code": "AF", "device_type": "autofactory", "location": "SOL-BELT-1", "status": "idle", "print_queue": []}),
            ),
            (
                "OUTPUT",
                serde_json::json!({"device_code": "OUTPUT", "device_type": "mining_drone", "location": "SOL-BELT-1", "status": "idle", "tags": ["mine-m:sol", "mine-b:one"]}),
            ),
        ] {
            Mock::given(method("GET"))
                .and(path(format!("/v1/devices/{code}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(&server)
                .await;
        }
        let client = Client::builder()
            .base_url(Url::parse(&server.uri()).expect("URL"))
            .authentication_token(SecretString::from("fixture"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("client");
        client
            .devices()
            .refresh("OUTPUT")
            .await
            .expect("new print output");
        let repository =
            std::sync::Arc::new(WorkflowRepository::open_in_memory().expect("repository"));
        let create = || {
            repository
                .create(NewWorkflow {
                    kind: WorkflowKind::new("mining.campaign").expect("kind"),
                    schema_version: 1,
                    config: serde_json::json!({}),
                    checkpoint: serde_json::json!({}),
                    current_step: None,
                    parent_id: None,
                })
                .expect("workflow")
        };
        let owner = create();
        let rival = create();
        let item_for = |workflow_id| {
            repository
                .reconcile_work_items(
                    workflow_id,
                    &[WorkItemSpec {
                        workflow_id,
                        dedupe_key: "output".into(),
                        kind: WorkflowKind::new("mining.site").expect("kind"),
                        sort_key: "output".into(),
                        payload_json: serde_json::json!({}),
                        preconditions_json: serde_json::json!([]),
                        deadline_at_ms: None,
                        requirements_json: serde_json::to_value(vec![ResourceRequirement {
                            key: "drone".into(),
                            kind: "device".into(),
                            capabilities: vec!["mining_drone".into()],
                            scope: RequirementScope::Anywhere,
                            count: 1,
                            quantity: 1,
                        }])
                        .expect("requirements"),
                    }],
                    0,
                )
                .expect("item")
                .remove(0)
        };
        let owner_item = item_for(owner.id);
        let rival_item = item_for(rival.id);
        let broker = crate::assignment::ResourceBroker::with_managed_client(
            repository.clone(),
            client.clone(),
        );
        let before = broker.discover_candidates().expect("new output discovery");
        assert!(
            broker
                .allocate(rival_item.id, rival_item.state.revision, &before)
                .is_err()
        );
        let mut mission: MiningMission = serde_json::from_value(serde_json::json!({
            "version": 1, "mission_id": "owner", "mission_tag": "mine-m:sol",
            "phase": "manufacturing_sites", "selected_replicant": "WORKER",
            "hub_location": "SOL-BELT-1", "sites": [], "routes": [],
            "print_batches": [{
                "purpose": "site", "factory_code": "AF", "device_type": "mining_drone",
                "quantity": 1, "projected_finish_seconds": 10.0, "batch_tag": "mine-b:one",
                "submission_started": true, "submitted": false, "produced_codes": []
            }],
            "site_print_requirements": {}, "route_print_requirements": {}, "total_material_cost": {}, "warnings": []
        })).expect("mission");
        let claims = super::super::MiningWorkflowClaims {
            repository: repository.clone(),
            workflow_id: owner.id,
        };
        for _ in 0..2 {
            reconcile_print_batches(&client, &mut mission, Some(&claims))
                .await
                .expect("claim output on restart");
        }
        let after = broker
            .discover_candidates()
            .expect("claimed output discovery");
        assert!(
            broker
                .allocate(rival_item.id, rival_item.state.revision, &after)
                .is_err()
        );
        let allocation = broker
            .allocate(owner_item.id, owner_item.state.revision, &after)
            .expect("owner uses output");
        assert_eq!(
            allocation.by_requirement["drone"][0].resource,
            replicant_workflow::ResourceKey::Device("OUTPUT".into())
        );
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn mining_restart_outbound_delivery_already_arrived_advances_without_shipping_again() {
        use replicant_client::{SecretString, StartupPolicy, raw::Url};
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        let server = MockServer::start().await;
        for (code, kind, location) in [
            ("MD", "maintenance_drone", "REMOTE-BELT-1"),
            ("CARRIER", "surge_carrier", "HUB-BELT-1"),
        ] {
            Mock::given(method("GET"))
                .and(path(format!("/v1/devices/{code}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "device_code": code, "device_type": kind, "location": location, "status": "idle"
                })))
                .mount(&server)
                .await;
        }
        let client = Client::builder()
            .base_url(Url::parse(&server.uri()).expect("URL"))
            .authentication_token(SecretString::from("fixture"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("client");
        let mut mission: MiningMission = serde_json::from_value(serde_json::json!({
            "version": 1, "mission_id": "legacy", "mission_tag": "mine-m:remote",
            "phase": "deploying_sites", "selected_replicant": "WORKER",
            "hub_location": "HUB-BELT-1", "routes": [], "print_batches": [],
            "sites": [{
                "system": "REMOTE", "belt": "REMOTE-BELT-1", "density": "dense",
                "tag": "mine-s:remote", "phase": "outbound", "carrier": "CARRIER",
                "assets": {"mining_drones": [], "survey_drones": [], "maintenance_drone": "MD"},
                "missing": {}
            }],
            "site_print_requirements": {}, "route_print_requirements": {},
            "total_material_cost": {}, "warnings": []
        }))
        .expect("outbound checkpoint");
        let plan_path =
            std::env::temp_dir().join(format!("mining-outbound-{}.json", uuid::Uuid::new_v4()));
        let config = Config {
            systems: vec!["REMOTE".into()],
            replicant: Some("WORKER".into()),
            hub: "HUB-BELT-1".into(),
            transport_routes: Vec::new(),
            plan_path: plan_path.clone(),
            replace_plan: false,
            wait_timeout: Duration::from_secs(1),
            max_concurrency: 1,
            claims: None,
        };
        save_plan(&plan_path, &mission).expect("persist outbound intent");
        mission = serde_json::from_slice(&std::fs::read(&plan_path).expect("checkpoint"))
            .expect("restart");
        resume_site_delivery(&client, &config, &mut mission, 0)
            .await
            .expect("reconcile delivery");
        let restored: MiningMission =
            serde_json::from_slice(&std::fs::read(&plan_path).expect("checkpoint"))
                .expect("persisted arrival");
        assert_eq!(restored.sites[0].phase, SitePhase::Deploying);
        assert_eq!(
            restored.sites[0].assets.maintenance_drone.as_deref(),
            Some("MD")
        );
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .iter()
                .all(|request| request.method == "GET")
        );
        client.close().await.expect("close");
        std::fs::remove_file(plan_path).expect("remove checkpoint");
    }

    #[tokio::test]
    async fn mining_route_unknown_controller_evidence_never_rewrites_or_launches() {
        use replicant_client::{SecretString, StartupPolicy, raw::Url};
        use replicant_mining_planner::site_tag;
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };
        for missing in [
            "status",
            "device_type",
            "ami_directive",
            "ami_directive_status",
            "config",
            "collect",
            "deliver",
        ] {
            let server = MockServer::start().await;
            let mut controller = serde_json::json!({
                "device_code": "AUTH-TC", "device_type": "ami_transport_controller",
                "location": "REMOTE-BELT-1", "status": "coordinating",
                "tags": [site_tag("REMOTE"), role_tag("transport-controller")],
                "ami_directive": {"directive": "ferry", "config": {
                    "collect": "REMOTE-BELT-1", "deliver": "HUB-BELT-1"
                }},
                "ami_directive_status": "active",
                "controlled_devices": [{"device_code": "AUTH-CF"}]
            });
            if missing == "config" {
                controller["ami_directive"]
                    .as_object_mut()
                    .expect("directive")
                    .remove(missing);
            } else if matches!(missing, "collect" | "deliver") {
                controller["ami_directive"]["config"]
                    .as_object_mut()
                    .expect("configuration")
                    .remove(missing);
            } else {
                controller
                    .as_object_mut()
                    .expect("controller")
                    .remove(missing);
            }
            let freighter = serde_json::json!({
                "device_code": "AUTH-CF", "device_type": "cargo_freighter",
                "location": "REMOTE-BELT-1", "status": "idle", "controller_device_code": "AUTH-TC"
            });
            for (code, value) in [("AUTH-TC", controller), ("AUTH-CF", freighter)] {
                Mock::given(method("GET"))
                    .and(path(format!("/v1/devices/{code}")))
                    .respond_with(ResponseTemplate::new(200).set_body_json(value))
                    .mount(&server)
                    .await;
            }
            let client = Client::builder()
                .base_url(Url::parse(&server.uri()).expect("URL"))
                .authentication_token(SecretString::from("fixture"))
                .in_memory()
                .startup_policy(StartupPolicy::RestoreOnly)
                .start()
                .await
                .expect("client");
            let route: super::super::RouteMission = serde_json::from_value(serde_json::json!({
                "system": "REMOTE", "belt": "REMOTE-BELT-1", "tag": site_tag("REMOTE"),
                "phase": "ready", "controller": "AUTH-TC", "freighter": "AUTH-CF",
                "desired_freighters": 1
            }))
            .expect("route");
            assert!(
                configure_route(&client, "authority-test", &route, "HUB-BELT-1")
                    .await
                    .is_err()
            );
            assert_eq!(
                transport_service_present(
                    &device_snapshots(&client).await.expect("devices"),
                    "REMOTE",
                    "REMOTE-BELT-1",
                    "HUB-BELT-1"
                )
                .state,
                EvidenceState::Unknown,
                "{missing}",
            );
            assert!(
                server
                    .received_requests()
                    .await
                    .expect("requests")
                    .iter()
                    .all(|request| request.method == "GET"),
                "{missing}"
            );
            client.close().await.expect("close");
        }
    }

    #[tokio::test]
    async fn mining_site_missing_active_directives_never_issue_commands() {
        use replicant_client::{SecretString, StartupPolicy, raw::Url};
        use replicant_mining_planner::site_tag;
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };
        for missing in ["SITE-MC", "SITE-SC", "SITE-MD"] {
            let server = MockServer::start().await;
            let client = Client::builder()
                .base_url(Url::parse(&server.uri()).expect("URL"))
                .authentication_token(SecretString::from("fixture"))
                .in_memory()
                .startup_policy(StartupPolicy::RestoreOnly)
                .start()
                .await
                .expect("client");
            for (code, kind, role, directive, status) in [
                (
                    "SITE-MC",
                    MINING_CONTROLLER,
                    "mining-controller",
                    "deplete_smallest",
                    "coordinating",
                ),
                (
                    "SITE-SC",
                    SURVEY_CONTROLLER,
                    "survey-controller",
                    "belt_search",
                    "coordinating",
                ),
                (
                    "SITE-MD",
                    MAINTENANCE_DRONE,
                    "maintenance",
                    "patrol",
                    "patrolling",
                ),
            ] {
                let mut body = serde_json::json!({
                    "device_code": code, "device_type": kind, "location": "AUTH-BELT-1",
                    "status": status, "operational_capacity": 100,
                    "tags": [site_tag("AUTH"), role_tag(role)],
                    "available_directives": [directive],
                    "ami_directive": {"directive": directive}, "ami_directive_status": "active"
                });
                if code == missing {
                    let fields = body.as_object_mut().expect("device");
                    fields.remove("ami_directive");
                    fields.remove("ami_directive_status");
                }
                Mock::given(method("GET"))
                    .and(path(format!("/v1/devices/{code}")))
                    .respond_with(ResponseTemplate::new(200).set_body_json(body))
                    .mount(&server)
                    .await;
            }
            let mut mission: MiningMission = serde_json::from_value(serde_json::json!({
                "version": 1, "mission_id": "site-directive-authority", "mission_tag": "mine-m:authority",
                "phase": "deploying_sites", "selected_replicant": "WORKER", "hub_location": "HUB-BELT-1",
                "sites": [{
                    "system": "AUTH", "belt": "AUTH-BELT-1", "density": "dense", "tag": site_tag("AUTH"),
                    "phase": "configuring", "missing": {},
                    "assets": {"mining_controller": "SITE-MC", "survey_controller": "SITE-SC",
                        "maintenance_drone": "SITE-MD", "mining_drones": [], "survey_drones": []}
                }],
                "routes": [], "print_batches": [], "site_print_requirements": {},
                "route_print_requirements": {}, "total_material_cost": {}, "warnings": []
            })).expect("mission");
            let plan_path = std::env::temp_dir()
                .join(format!("mining-directive-{}.json", uuid::Uuid::new_v4()));
            let config = Config {
                systems: vec!["AUTH".into()],
                replicant: None,
                hub: "HUB-BELT-1".into(),
                transport_routes: Vec::new(),
                plan_path: plan_path.clone(),
                replace_plan: false,
                wait_timeout: Duration::from_secs(1),
                max_concurrency: 1,
                claims: None,
            };
            assert!(
                configure_site(&client, &config, &mut mission, 0)
                    .await
                    .is_err()
            );
            let devices = device_snapshots(&client).await.expect("devices");
            let audit = audit_site(&devices, "AUTH", "AUTH-BELT-1");
            assert!(!audit.mutation_safe);
            assert!(audit.issues.iter().any(|issue| matches!(
                issue, super::super::MiningSiteIssue::EvidenceIncomplete { device, field: "active_directive" }
                    if device == missing
            )));
            let mut idle = find_device(&devices, missing)
                .expect("selected device")
                .clone();
            idle.status = Some(DeviceStatus::from("idle"));
            assert_eq!(
                super::super::site_directive_evidence(&idle, "patrol"),
                EvidenceState::Absent
            );
            assert!(
                server
                    .received_requests()
                    .await
                    .expect("requests")
                    .iter()
                    .all(|request| request.method == "GET"),
                "{missing}"
            );
            client.close().await.expect("close");
            std::fs::remove_file(plan_path).expect("remove checkpoint");
        }
    }
}
