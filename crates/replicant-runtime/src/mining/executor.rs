use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    time::{Duration, Instant},
};

use futures::future::join_all;
use replicant_client::{
    Client, MiningDirective, OperationId, OperationStatus, SurveyDirective, TransportDirective,
    domain::Device, managed::Operation, raw as api_raw,
};
use replicant_mining_planner::{
    BlueprintSpec, CARGO_FREIGHTER, FactoryWorkload, MAINTENANCE_DRONE, MINING_CONTROLLER,
    MINING_DRONE, QuantityMap, SURGE_CARRIER, SURVEY_CONTROLLER, SURVEY_DRONE, SYSTEM_WARD,
    TRANSPORT_CONTROLLER, role_tag,
};
use replicant_printing::managed::{enqueue_print, factory_queue_slots};
use replicant_printing::schedule_prints;
use replicant_transport::{DeliveryOptions, PayloadDevice, deliver_devices_with};
use serde_json::Value;
use tokio::time::timeout;
use tracing::{info, warn};

use super::{
    AnyResult, Config, ExecutionPrintBatch, MiningMission, MissionPhase, PrintPurpose, RoutePhase,
    SiteAssets, SitePhase, app_error, audit_route, audit_site, controller_code,
    device_is_in_system, device_location, device_type, factory_workloads, fetch_blueprints,
    find_device, format_quantities, has_directive, has_reservation_tag,
    is_opaque_mining_mission_tag, protected_systems, refresh_device_snapshots, save_plan,
    site_shortages, stable_hash,
};

const POLL_INTERVAL: Duration = Duration::from_secs(5);
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
/// interval between full-account refreshes meant every wait re-read the entire
/// device census every few seconds regardless of whether anything had changed;
/// waking on the event stream keeps the reaction time while cutting the idle
/// refreshes by an order of magnitude.
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

pub(crate) async fn execute(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    if mission.phase.is_terminal() {
        return Ok(());
    }

    let sync = client.sync().full().await?;
    info!(readiness = ?sync.readiness, "full managed synchronization completed");
    migrate_legacy_mission_devices(client, mission).await?;
    save_plan(&config.plan_path, mission)?;
    reconcile(client, config, mission).await?;
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
    reconcile_print_batches(client, mission).await?;
    let devices = refresh_device_snapshots(client).await?;
    let protection = protected_systems(&devices, &client.galaxy().catalogue());
    for site in &mut mission.sites {
        let audit = audit_site(
            &devices,
            &site.system,
            &site.belt,
            protection.contains(&site.system),
        );
        if audit.operational {
            site.assets = audit.assets;
            site.missing.clear();
            site.phase = SitePhase::Operational;
        } else if site.phase == SitePhase::Operational {
            site.phase = SitePhase::Planned;
            site.missing = site_shortages(&audit);
            site.assets = audit.assets;
        } else if site.phase == SitePhase::Planned {
            site.missing = site_shortages(&audit);
            site.assets = audit.assets;
        }
    }
    for route in &mut mission.routes {
        let audit = audit_route(&devices, &route.system, &route.belt, &mission.hub_location);
        if audit.active {
            route.controller = audit.controller;
            route.freighter = audit.freighter;
            route.phase = RoutePhase::Active;
        } else if route.phase == RoutePhase::Active {
            route.phase = RoutePhase::Planned;
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
        // Reconciliation just refreshed the owned-device projection in bulk.
        let handle = match client.devices().cached(&carrier) {
            Some(handle) => handle,
            None => client.devices().get(&carrier).await?,
        };
        let snapshot = handle.snapshot().await?;
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
    reconcile_print_batches(client, mission).await?;
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

async fn reconcile_print_batches(client: &Client, mission: &mut MiningMission) -> AnyResult<()> {
    let batch_tags = mission
        .print_batches
        .iter()
        .map(|batch| batch.batch_tag.clone())
        .collect::<BTreeSet<_>>();
    migrate_legacy_mission_devices(client, mission).await?;
    let aliases = mission_tag_aliases(mission);
    let devices = refresh_device_snapshots(client).await?;
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
    let deadline = Instant::now() + config.wait_timeout;
    let mut watch = client.events().watch().await?;
    let mut reported_factories = false;
    loop {
        reconcile_print_batches(client, mission).await?;
        if purpose == PrintPurpose::Site {
            progress_site_pipeline(client, config, mission).await?;
        }
        let factories = factory_workloads(client, &blueprints, &mission.hub_location).await?;
        let reassigned =
            rebalance_pending_print_batches(mission, purpose, &blueprints, &factories)?;
        if !reported_factories {
            info!(
                purpose = ?purpose,
                factories = %format_factory_workloads(&factories),
                assignments = %format_factory_assignments(mission, purpose),
                "discovered Autofactories and balanced pending prints"
            );
            reported_factories = true;
        }
        if reassigned > 0 {
            info!(
                purpose = ?purpose,
                reassigned,
                assignments = %format_factory_assignments(mission, purpose),
                "rebalanced pending prints across available Autofactories"
            );
        }
        save_plan(&config.plan_path, mission)?;
        let pending = phase_batches(mission, purpose)
            .into_iter()
            .filter(|index| !mission.print_batches[*index].submitted)
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }

        let factories = pending
            .iter()
            .map(|index| mission.print_batches[*index].factory_code.clone())
            .collect::<BTreeSet<_>>();
        let mut slots = BTreeMap::new();
        for factory in factories {
            slots.insert(
                factory.clone(),
                factory_queue_slots(client, &factory).await?,
            );
        }

        let mut submitted_any = false;
        let mut unaffordable = None;
        // One inventory read per pass, not per candidate batch. Submitted costs
        // are deducted locally so several batches can still be funded from one
        // read without over-committing the same materials.
        let mut available = hub_inventory(client, &mission.hub_location).await?;
        for index in pending {
            let batch = mission.print_batches[index].clone();
            let available_slots = slots.get(&batch.factory_code).copied().unwrap_or(0);
            if available_slots == 0 {
                continue;
            }
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

            mission.print_batches[index].submission_started = true;
            save_plan(&config.plan_path, mission)?;
            let operation = enqueue_print(
                client,
                &batch.factory_code,
                &batch.device_type,
                batch.quantity,
                &[
                    mission.mission_tag.clone(),
                    role_tag(role_for_type(&batch.device_type)),
                    batch.batch_tag.clone(),
                ],
            )
            .await?;
            mission.print_batches[index].operation_id = Some(operation.id().as_str().to_owned());
            mission.print_batches[index].submitted = true;
            save_plan(&config.plan_path, mission)?;
            ensure_operation_accepted(&operation).await?;
            slots.insert(batch.factory_code.clone(), available_slots - 1);
            submitted_any = true;
            info!(
                purpose = ?purpose,
                factory = %batch.factory_code,
                device_type = %batch.device_type,
                quantity = batch.quantity,
                "queued mining expansion print batch"
            );
        }
        if submitted_any {
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

/// Subtracts submitted batch costs from a locally held inventory snapshot.
fn deduct_quantities(available: &mut QuantityMap, cost: &QuantityMap) {
    for (resource, quantity) in cost {
        let remaining = available.entry(resource.clone()).or_default();
        *remaining = remaining.saturating_sub(*quantity);
    }
}

fn rebalance_pending_print_batches(
    mission: &mut MiningMission,
    purpose: PrintPurpose,
    blueprints: &BTreeMap<String, BlueprintSpec>,
    factories: &[FactoryWorkload],
) -> AnyResult<usize> {
    let mut pending = mission
        .print_batches
        .iter()
        .enumerate()
        .filter_map(|(index, batch)| {
            (batch.purpose == purpose
                && !batch.submission_started
                && !batch.submitted
                && batch.operation_id.is_none()
                && batch.produced_codes.is_empty())
            .then_some(index)
        })
        .collect::<Vec<_>>();
    if pending.is_empty() {
        return Ok(0);
    }
    let required = pending
        .iter()
        .fold(QuantityMap::new(), |mut required, index| {
            *required
                .entry(mission.print_batches[*index].device_type.clone())
                .or_default() += 1;
            required
        });
    let schedule = schedule_prints(&required, blueprints, factories)?;
    let mut assignments = BTreeMap::<String, VecDeque<(String, f64)>>::new();
    for batch in schedule.batches {
        for _ in 0..batch.quantity {
            assignments
                .entry(batch.device_type.clone())
                .or_default()
                .push_back((batch.factory_code.clone(), batch.projected_finish_seconds));
        }
    }
    pending.sort_by(|left, right| {
        mission.print_batches[*left]
            .device_type
            .cmp(&mission.print_batches[*right].device_type)
            .then_with(|| {
                mission.print_batches[*left]
                    .batch_tag
                    .cmp(&mission.print_batches[*right].batch_tag)
            })
    });
    let mut reassigned = 0usize;
    for index in pending {
        let (factory_code, projected_finish_seconds) = assignments
            .get_mut(&mission.print_batches[index].device_type)
            .and_then(VecDeque::pop_front)
            .ok_or_else(|| {
                app_error(
                    io::ErrorKind::InvalidData,
                    "distributed print schedule omitted a pending unit",
                )
            })?;
        let batch = &mut mission.print_batches[index];
        if batch.factory_code != factory_code {
            batch.factory_code = factory_code;
            reassigned += 1;
        }
        batch.projected_finish_seconds = projected_finish_seconds;
    }
    Ok(reassigned)
}

fn format_factory_workloads(factories: &[FactoryWorkload]) -> String {
    factories
        .iter()
        .map(|factory| format!("{}:{:.0}s", factory.code, factory.remaining_seconds))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_factory_assignments(mission: &MiningMission, purpose: PrintPurpose) -> String {
    let mut counts = BTreeMap::<&str, usize>::new();
    for batch in mission.print_batches.iter().filter(|batch| {
        batch.purpose == purpose
            && !batch.submitted
            && batch.operation_id.is_none()
            && batch.produced_codes.is_empty()
    }) {
        *counts.entry(&batch.factory_code).or_default() += 1;
    }
    counts
        .into_iter()
        .map(|(factory, count)| format!("{factory}:{count}"))
        .collect::<Vec<_>>()
        .join(",")
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
        reconcile_print_batches(client, mission).await?;
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
        wait_for_mission_event(
            &mut watch,
            deadline,
            &["print.completed", "device.print_completed"],
        )
        .await?;
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
    let devices = refresh_device_snapshots(client).await?;
    let mut used = mission
        .sites
        .iter()
        .flat_map(|site| site.assets.codes())
        .chain(
            mission
                .routes
                .iter()
                .flat_map(|route| route.controller.iter().chain(&route.freighter).cloned()),
        )
        .collect::<BTreeSet<_>>();
    let mut pool = reusable_pool(&devices, &mission.hub_location, &mission.mission_tag, &used);
    let mut allocated = 0usize;
    for index in 0..mission.sites.len() {
        let missing = mission.sites[index].missing.clone();
        if mission.sites[index].phase != SitePhase::Planned
            || !pool_can_complete_site(&pool, &missing)
        {
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
        if missing.contains_key(SYSTEM_WARD) {
            fill_site_asset(
                &mut mission.sites[index].assets.system_ward,
                SYSTEM_WARD,
                &mut pool,
                &mut used,
            )?;
        }
        mission.sites[index].missing.clear();
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
        add_tags(client, &code, &[site.tag.clone(), role_tag(role)]).await?;
    }
    Ok(())
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
        wait_for_mission_event(&mut watch, deadline, &[]).await?;
    }
}

async fn dispatch_ready_sites(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    let devices = refresh_device_snapshots(client).await?;
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
        let payload = mission.sites[index]
            .assets
            .codes()
            .into_iter()
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
        mission.sites[index].carrier = Some(carrier.clone());
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
    let results = join_all(deliveries.iter().map(|(_, carrier, payload, belt)| {
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
    let site = mission.sites[index].clone();
    for code in site.assets.codes() {
        // Arrival events keep each site asset current in the projection.
        let handle = match client.devices().cached(&code) {
            Some(handle) => handle,
            None => client.devices().get(&code).await?,
        };
        let snapshot = handle.snapshot().await?;
        let is_ward = site.assets.system_ward.as_deref() == Some(code.as_str());
        let in_place = if is_ward {
            device_is_in_system(&snapshot, &site.system)
        } else {
            device_location(&snapshot) == Some(site.belt.as_str())
        };
        if !in_place {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                if is_ward {
                    format!(
                        "system ward {code} is not deployed anywhere in {}",
                        site.system
                    )
                } else {
                    format!("site asset {code} has not arrived at {}", site.belt)
                },
            ));
        }
    }
    tag_site_assets(client, &site).await?;
    ensure_site_protection(client, &site).await?;
    mission.sites[index].phase = SitePhase::Adopting;
    save_plan(&config.plan_path, mission)?;
    let mining_controller =
        site.assets.mining_controller.as_deref().ok_or_else(|| {
            app_error(io::ErrorKind::InvalidData, "site has no mining controller")
        })?;
    ensure_adoption(client, mining_controller, &site.assets.mining_drones).await?;
    let survey_controller =
        site.assets.survey_controller.as_deref().ok_or_else(|| {
            app_error(io::ErrorKind::InvalidData, "site has no survey controller")
        })?;
    ensure_adoption(client, survey_controller, &site.assets.survey_drones).await?;
    mission.sites[index].phase = SitePhase::Verifying;
    save_plan(&config.plan_path, mission)?;
    let mining_controller = site.assets.mining_controller.as_deref().unwrap_or_default();
    // Fleet reconciliation already populated the selected controller.
    let mining_handle = match client.devices().cached(mining_controller) {
        Some(handle) => handle,
        None => client.devices().get(mining_controller).await?,
    };
    let mining = mining_handle.as_mining_controller()?;
    let mining_snapshot = mining.device().snapshot().await?;
    if !has_directive(&mining_snapshot, "deplete_smallest") {
        let operation = mining
            .set_directive(MiningDirective::DepleteSmallest)
            .await?;
        ensure_operation_accepted(&operation).await?;
    }
    let mining_snapshot = mining.device().refresh().await?.snapshot().await?;
    if mining_snapshot
        .status
        .as_ref()
        .is_none_or(|status| status.as_str() != "coordinating")
    {
        ensure_operation_accepted(&mining.launch().await?).await?;
    }

    let survey_controller = site.assets.survey_controller.as_deref().unwrap_or_default();
    // Fleet reconciliation already populated the selected controller.
    let survey_handle = match client.devices().cached(survey_controller) {
        Some(handle) => handle,
        None => client.devices().get(survey_controller).await?,
    };
    let survey = survey_handle.as_survey_controller()?;
    let survey_snapshot = survey.device().snapshot().await?;
    if !has_directive(&survey_snapshot, "belt_search") {
        ensure_operation_accepted(&survey.set_directive(SurveyDirective::BeltSearch).await?)
            .await?;
    }
    let survey_snapshot = survey.device().refresh().await?.snapshot().await?;
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
    // Fleet reconciliation already populated the maintenance drone.
    let maintenance_handle = match client.devices().cached(maintenance) {
        Some(handle) => handle,
        None => client.devices().get(maintenance).await?,
    };
    let maintenance_snapshot = maintenance_handle.snapshot().await?;
    if !has_directive(&maintenance_snapshot, "patrol") {
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

    let deadline = Instant::now() + VERIFY_TIMEOUT;
    let mut watch = client.events().watch().await?;
    loop {
        let devices = refresh_device_snapshots(client).await?;
        let protection = protected_systems(&devices, &client.galaxy().catalogue());
        if audit_site(
            &devices,
            &site.system,
            &site.belt,
            protection.contains(&site.system),
        )
        .operational
        {
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
        wait_for_mission_event(&mut watch, deadline, &[]).await?;
    }
    info!(system = %site.system, belt = %site.belt, "mining site operational");
    mission.sites[index].phase = SitePhase::Operational;
    save_plan(&config.plan_path, mission)?;
    Ok(())
}

async fn ensure_site_protection(client: &Client, site: &super::SiteMission) -> AnyResult<()> {
    let devices = refresh_device_snapshots(client).await?;
    let protection = protected_systems(&devices, &client.galaxy().catalogue());
    if protection.contains(&site.system) {
        return Ok(());
    }

    let ward = site.assets.system_ward.as_deref().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            format!("mining site {} has no System Ward", site.system),
        )
    })?;
    let snapshot = find_device(&devices, ward).ok_or_else(|| {
        app_error(
            io::ErrorKind::NotFound,
            format!("System Ward {ward} is absent from the owned-device projection"),
        )
    })?;
    if !device_is_in_system(snapshot, &site.system) {
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
    client.galaxy().refresh_catalogue().await?;
    Ok(())
}

async fn ensure_adoption(client: &Client, controller: &str, devices: &[String]) -> AnyResult<()> {
    let snapshots = refresh_device_snapshots(client).await?;
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
    // Adoption checks read the controller and drone projection immediately above.
    let handle = match client.devices().cached(controller) {
        Some(handle) => handle,
        None => client.devices().get(controller).await?,
    };
    let operation = handle
        .command(api_raw::devices::DeviceCommand::Adopt(targets(&missing)))
        .await?;
    ensure_operation_accepted(&operation).await
}

async fn allocate_route_assets(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    let devices = refresh_device_snapshots(client).await?;
    let mut used = mission
        .sites
        .iter()
        .flat_map(|site| site.assets.codes())
        .chain(
            mission
                .routes
                .iter()
                .flat_map(|route| route.controller.iter().chain(&route.freighter).cloned()),
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
        fill_site_asset(
            &mut mission.routes[index].freighter,
            CARGO_FREIGHTER,
            &mut pool,
            &mut used,
        )?;
        let codes = mission.routes[index]
            .controller
            .iter()
            .chain(&mission.routes[index].freighter)
            .cloned()
            .collect::<Vec<_>>();
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
    if let Some(freighter) = &route.freighter {
        add_tags(
            client,
            freighter,
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
        configure_route(client, &mission.routes[index], &mission.hub_location).await?;
        mission.routes[index].phase = RoutePhase::Active;
        save_plan(&config.plan_path, mission)?;
    }
    Ok(())
}

async fn configure_route(client: &Client, route: &super::RouteMission, hub: &str) -> AnyResult<()> {
    let controller = route.controller.as_deref().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            "route has no transport controller",
        )
    })?;
    let freighter = route
        .freighter
        .as_deref()
        .ok_or_else(|| app_error(io::ErrorKind::InvalidData, "route has no Cargo Freighter"))?;
    ensure_adoption(client, controller, &[freighter.to_owned()]).await?;
    // Adoption and fleet reconciliation already populated the controller.
    let handle = match client.devices().cached(controller) {
        Some(handle) => handle,
        None => client.devices().get(controller).await?,
    };
    let transport = handle.as_transport_controller()?;
    let snapshot = transport.device().snapshot().await?;
    if !super::ferry_route_matches(&snapshot, &route.belt, hub) {
        ensure_operation_accepted(
            &transport
                .set_directive(TransportDirective::Ferry {
                    collect: route.belt.clone(),
                    deliver: hub.to_owned(),
                    priority: vec!["rares".into(), "volatiles".into()],
                })
                .await?,
        )
        .await?;
    }
    let snapshot = transport.device().refresh().await?.snapshot().await?;
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
        let devices = refresh_device_snapshots(client).await?;
        if audit_route(&devices, &route.system, &route.belt, hub).active {
            break;
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("ferry route for {} did not verify as active", route.system),
            ));
        }
        wait_for_mission_event(&mut watch, deadline, &[]).await?;
    }
    info!(
        system = %route.system,
        collect = %route.belt,
        deliver = %hub,
        "mining ferry route active"
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
            // Travel events keep returning carriers current between loop passes.
            let handle = match client.devices().cached(&carrier) {
                Some(handle) => handle,
                None => client.devices().get(&carrier).await?,
            };
            let snapshot = handle.snapshot().await?;
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
        wait_for_mission_event(&mut watch, deadline, &["travel.arrived"]).await?;
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
) -> AnyResult<usize> {
    if mission.legacy_mission_tags.is_empty() {
        return Ok(0);
    }
    let devices = refresh_device_snapshots(client).await?;
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
        // The bulk refresh that produced `device` also populated its handle.
        let handle = match client.devices().cached(device.key.id.as_str()) {
            Some(handle) => handle,
            None => client.devices().get(device.key.id.as_str()).await?,
        };
        let add_tags = (!device.tags.contains(&mission.mission_tag))
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
    let handles = client
        .devices()
        .refresh_many()
        .with_tag(mission.mission_tag.clone())
        .page_size(50)
        .collect()
        .await?;
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        let mut removable = vec![mission.mission_tag.clone()];
        removable.extend(
            snapshot
                .tags
                .iter()
                .filter(|tag| batch_tags.contains(*tag))
                .cloned(),
        );
        remove_tags(client, handle.id().as_str(), &removable).await?;
    }
    Ok(())
}

async fn ensure_asset_ownership(
    client: &Client,
    codes: &[String],
    selected_replicant: &str,
) -> AnyResult<()> {
    for code in codes {
        // Selected mission assets are maintained by the SSE projection.
        let handle = match client.devices().cached(code) {
            Some(handle) => handle,
            None => client.devices().get(code).await?,
        };
        let snapshot = handle.snapshot().await?;
        if snapshot
            .relationships
            .assigned_replicant
            .as_ref()
            .is_none_or(|replicant| replicant.id.as_str() != selected_replicant)
        {
            ensure_operation_accepted(&handle.change_owner(selected_replicant).await?).await?;
        }
    }
    Ok(())
}

async fn add_tags(client: &Client, code: &str, desired: &[String]) -> AnyResult<()> {
    // Mission tag mutations operate on projection-backed selected assets.
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    let snapshot = handle.snapshot().await?;
    let missing = desired
        .iter()
        .filter(|tag| !snapshot.tags.contains(*tag))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
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
    // Mission tag mutations operate on projection-backed selected assets.
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    let snapshot = handle.snapshot().await?;
    let present = removable
        .iter()
        .filter(|tag| snapshot.tags.contains(*tag))
        .cloned()
        .collect::<Vec<_>>();
    if present.is_empty() {
        return Ok(());
    }
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
    // Travel events keep mission-device location and travel state current.
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    let snapshot = handle.snapshot().await?;
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
                plan.intermediate_systems
                    .into_iter()
                    .map(Value::String)
                    .collect(),
            )
        });
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
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
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
            access: AccessScope::Owned,
        }
    }

    fn print_batch(factory: &str, tag: &str) -> ExecutionPrintBatch {
        ExecutionPrintBatch {
            purpose: PrintPurpose::Site,
            factory_code: factory.into(),
            device_type: MINING_DRONE.into(),
            quantity: 1,
            projected_finish_seconds: 0.0,
            batch_tag: tag.into(),
            submission_started: false,
            submitted: false,
            operation_id: None,
            produced_codes: Vec::new(),
        }
    }

    fn mission_with_batches(print_batches: Vec<ExecutionPrintBatch>) -> MiningMission {
        MiningMission {
            version: 1,
            mission_id: "mission".into(),
            mission_tag: "mine-m:hub".into(),
            legacy_mission_tags: Vec::new(),
            phase: MissionPhase::ManufacturingSites,
            selected_replicant: "replicant".into(),
            hub_location: "HUB-BELT-1".into(),
            sites: Vec::new(),
            routes: Vec::new(),
            print_batches,
            site_print_requirements: QuantityMap::new(),
            route_print_requirements: QuantityMap::new(),
            total_material_cost: QuantityMap::new(),
            warnings: Vec::new(),
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
    fn pending_prints_rebalance_across_live_factories() {
        let mut submitted = print_batch("AF1", "submitted");
        submitted.submission_started = true;
        submitted.submitted = true;
        submitted.operation_id = Some("operation".into());
        let mut mission = mission_with_batches(vec![
            submitted,
            print_batch("AF1", "one"),
            print_batch("AF1", "two"),
            print_batch("AF1", "three"),
            print_batch("AF1", "four"),
        ]);
        let blueprints = [(
            MINING_DRONE.into(),
            BlueprintSpec {
                device_type: MINING_DRONE.into(),
                print_time_seconds: 100.0,
                resources: QuantityMap::new(),
                components: QuantityMap::new(),
            },
        )]
        .into_iter()
        .collect();
        let factories = vec![
            FactoryWorkload {
                code: "AF1".into(),
                remaining_seconds: 300.0,
            },
            FactoryWorkload {
                code: "AF2".into(),
                remaining_seconds: 0.0,
            },
        ];

        let reassigned = rebalance_pending_print_batches(
            &mut mission,
            PrintPurpose::Site,
            &blueprints,
            &factories,
        )
        .unwrap();

        assert_eq!(reassigned, 3);
        assert_eq!(mission.print_batches[0].factory_code, "AF1");
        let pending_counts = mission.print_batches[1..].iter().fold(
            BTreeMap::<&str, usize>::new(),
            |mut counts, batch| {
                *counts.entry(&batch.factory_code).or_default() += 1;
                counts
            },
        );
        assert_eq!(pending_counts["AF1"], 1);
        assert_eq!(pending_counts["AF2"], 3);
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
}
