use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::{Duration, Instant},
};

use replicant_client::{
    Client, MiningDirective, OperationId, OperationStatus, SurveyDirective, TransportDirective,
    domain::Device, managed::Operation, raw as api_raw,
};
use replicant_mining_planner::{
    CARGO_FREIGHTER, MAINTENANCE_DRONE, MINING_CONTROLLER, MINING_DRONE, QuantityMap,
    SURGE_CARRIER, SURVEY_CONTROLLER, SURVEY_DRONE, TRANSPORT_CONTROLLER, role_tag,
};
use serde_json::Value;
use tokio::time::sleep;
use tracing::info;

use super::{
    AnyResult, Config, MiningMission, MissionPhase, PrintPurpose, RoutePhase, SiteAssets,
    SitePhase, app_error, audit_route, audit_site, controller_code, device_location, device_type,
    fetch_blueprints, find_device, format_quantities, has_directive, has_reservation_tag,
    refresh_device_snapshots, save_plan,
};

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const OPERATION_WAIT: Duration = Duration::from_secs(30);
const VERIFY_TIMEOUT: Duration = Duration::from_secs(300);

pub(crate) async fn execute(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    if mission.phase.is_terminal() {
        println!(
            "Mining mission {} is already {:?}.",
            mission.mission_id, mission.phase
        );
        return Ok(());
    }

    let sync = client.sync().full().await?;
    info!(readiness = ?sync.readiness, "full managed synchronization completed");
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
    println!(
        "Mining mission {} completed: {} sites operational and {} ferry routes active.",
        mission.mission_id,
        mission.sites.len(),
        mission.routes.len()
    );
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
    mission.phase = phase;
    save_plan(&config.plan_path, mission)
}

async fn reconcile(client: &Client, config: &Config, mission: &mut MiningMission) -> AnyResult<()> {
    reconcile_print_batches(client, mission).await?;
    let devices = refresh_device_snapshots(client).await?;
    for site in &mut mission.sites {
        let audit = audit_site(&devices, &site.belt);
        if audit.operational {
            site.assets = audit.assets;
            site.missing.clear();
            site.phase = SitePhase::Operational;
        } else if site.phase == SitePhase::Operational {
            site.phase = SitePhase::Planned;
            site.missing = replicant_mining_planner::shortages(
                &replicant_mining_planner::mining_site_requirements(),
                &audit.assets.counts(),
            );
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
        let snapshot = client.devices().get(&carrier).await?.snapshot().await?;
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
    save_plan(&config.plan_path, mission)?;
    if phase_batches(mission, purpose).is_empty() {
        return Ok(());
    }
    submit_print_batches(client, config, mission, purpose).await?;
    wait_for_print_outputs(client, config, mission, purpose).await
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
    let handles = client
        .devices()
        .refresh_many()
        .with_tag(mission.mission_tag.clone())
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
                        tags.contains(&mission.mission_tag) && tags.contains(&batch.batch_tag)
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
    loop {
        reconcile_print_batches(client, mission).await?;
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
            let available = hub_inventory(client, &mission.hub_location).await?;
            if !contains_quantities(&available, &cost) {
                unaffordable = Some((cost, available));
                continue;
            }

            mission.print_batches[index].submission_started = true;
            save_plan(&config.plan_path, mission)?;
            let operation = client
                .devices()
                .get(&batch.factory_code)
                .await?
                .enqueue_print_with_tags(
                    batch.device_type.clone(),
                    batch.quantity,
                    [
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
        sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_print_outputs(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
    purpose: PrintPurpose,
) -> AnyResult<()> {
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        reconcile_print_batches(client, mission).await?;
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
        sleep(POLL_INTERVAL).await;
    }
}

async fn factory_queue_slots(client: &Client, factory_code: &str) -> AnyResult<usize> {
    let detail = client.raw().devices().get(factory_code).await?.value;
    let queue_size = usize::try_from(detail.queue_size.unwrap_or(1).max(1))?;
    Ok(queue_size.saturating_sub(detail.print_queue.len()))
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
    for index in 0..mission.sites.len() {
        if mission.sites[index].phase == SitePhase::Operational {
            tag_site_assets(client, &mission.sites[index]).await?;
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
        mission.sites[index].missing.clear();
        mission.sites[index].phase = SitePhase::Ready;
        save_plan(&config.plan_path, mission)?;
        ensure_asset_ownership(
            client,
            &mission.sites[index].assets.codes(),
            &mission.selected_replicant,
        )
        .await?;
        tag_site_assets(client, &mission.sites[index]).await?;
    }
    Ok(())
}

fn reusable_pool(
    devices: &[Device],
    hub: &str,
    mission_tag: &str,
    used: &BTreeSet<String>,
) -> BTreeMap<String, Vec<String>> {
    let mut pool = BTreeMap::<String, Vec<String>>::new();
    for device in devices.iter().filter(|device| {
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
    }) {
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
    roles
}

async fn deploy_sites(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        reconcile(client, config, mission).await?;
        if mission
            .sites
            .iter()
            .all(|site| site.phase == SitePhase::Operational)
        {
            return Ok(());
        }

        finish_arrived_sites(client, config, mission).await?;
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
        sleep(POLL_INTERVAL).await;
    }
}

async fn dispatch_ready_sites(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    let devices = refresh_device_snapshots(client).await?;
    let claimed_count = mission
        .sites
        .iter()
        .filter(|site| site.carrier.is_some())
        .count();
    let available_slots = config.max_concurrency.saturating_sub(claimed_count);
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
        .filter_map(|(index, site)| {
            (site.phase == SitePhase::Ready && site.carrier.is_none()).then_some(index)
        })
        .take(available_slots)
        .collect::<Vec<_>>();
    for index in ready {
        let payload = mission.sites[index]
            .assets
            .codes()
            .into_iter()
            .filter(|code| {
                find_device(&devices, code).is_some_and(|device| {
                    device_location(device) == Some(mission.hub_location.as_str())
                })
            })
            .collect::<Vec<_>>();
        if payload.is_empty() {
            mission.sites[index].phase = SitePhase::Configuring;
            configure_site(client, &mission.sites[index]).await?;
            mission.sites[index].phase = SitePhase::Operational;
            save_plan(&config.plan_path, mission)?;
            continue;
        }
        let Some(carrier) = carriers.pop() else {
            break;
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
        attach_devices(client, &carrier, &payload).await?;
        start_travel(client, &carrier, &mission.sites[index].belt).await?;
        info!(
            system = %mission.sites[index].system,
            carrier = %carrier,
            devices = payload.len(),
            destination = %mission.sites[index].belt,
            "dispatched mining site concurrently"
        );
    }
    Ok(())
}

async fn finish_arrived_sites(
    client: &Client,
    config: &Config,
    mission: &mut MiningMission,
) -> AnyResult<()> {
    let candidates = mission
        .sites
        .iter()
        .enumerate()
        .filter_map(|(index, site)| {
            matches!(site.phase, SitePhase::Outbound | SitePhase::Configuring).then_some(index)
        })
        .collect::<Vec<_>>();
    for index in candidates {
        let arrived = if let Some(carrier) = &mission.sites[index].carrier {
            let snapshot = client.devices().get(carrier).await?.snapshot().await?;
            if mission.sites[index].phase == SitePhase::Outbound
                && snapshot.travel.is_none()
                && device_location(&snapshot) == Some(mission.hub_location.as_str())
            {
                add_tags(
                    client,
                    carrier,
                    &[mission.mission_tag.clone(), role_tag("carrier")],
                )
                .await?;
                ensure_asset_ownership(
                    client,
                    std::slice::from_ref(carrier),
                    &mission.selected_replicant,
                )
                .await?;
                let attached = snapshot
                    .relationships
                    .attached_devices
                    .iter()
                    .map(|device| device.id.as_str().to_owned())
                    .collect::<BTreeSet<_>>();
                let devices = refresh_device_snapshots(client).await?;
                let payload = mission.sites[index]
                    .assets
                    .codes()
                    .into_iter()
                    .filter(|code| {
                        find_device(&devices, code).is_some_and(|device| {
                            device_location(device) == Some(mission.hub_location.as_str())
                                && !attached.contains(code)
                        })
                    })
                    .collect::<Vec<_>>();
                attach_devices(client, carrier, &payload).await?;
                start_travel(client, carrier, &mission.sites[index].belt).await?;
                false
            } else {
                snapshot.travel.is_none()
                    && device_location(&snapshot) == Some(mission.sites[index].belt.as_str())
            }
        } else {
            true
        };
        if !arrived {
            continue;
        }
        mission.sites[index].phase = SitePhase::Configuring;
        save_plan(&config.plan_path, mission)?;
        if let Some(carrier) = mission.sites[index].carrier.clone() {
            let snapshot = client.devices().get(&carrier).await?.snapshot().await?;
            let attached = snapshot
                .relationships
                .attached_devices
                .iter()
                .map(|device| device.id.as_str().to_owned())
                .collect::<Vec<_>>();
            detach_devices(client, &carrier, &attached).await?;
        }
        configure_site(client, &mission.sites[index]).await?;
        mission.sites[index].phase = SitePhase::Operational;
        save_plan(&config.plan_path, mission)?;
        if let Some(carrier) = mission.sites[index].carrier.clone() {
            start_travel(client, &carrier, &mission.hub_location).await?;
        }
    }
    Ok(())
}

async fn configure_site(client: &Client, site: &super::SiteMission) -> AnyResult<()> {
    for code in site.assets.codes() {
        let snapshot = client.devices().get(&code).await?.snapshot().await?;
        if device_location(&snapshot) != Some(site.belt.as_str()) {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("site asset {code} has not arrived at {}", site.belt),
            ));
        }
    }
    tag_site_assets(client, site).await?;
    let mining_controller =
        site.assets.mining_controller.as_deref().ok_or_else(|| {
            app_error(io::ErrorKind::InvalidData, "site has no mining controller")
        })?;
    ensure_adoption(client, mining_controller, &site.assets.mining_drones).await?;
    let mining = client
        .devices()
        .get(mining_controller)
        .await?
        .as_mining_controller()?;
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

    let survey_controller =
        site.assets.survey_controller.as_deref().ok_or_else(|| {
            app_error(io::ErrorKind::InvalidData, "site has no survey controller")
        })?;
    ensure_adoption(client, survey_controller, &site.assets.survey_drones).await?;
    let survey = client
        .devices()
        .get(survey_controller)
        .await?
        .as_survey_controller()?;
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
    let maintenance_handle = client.devices().get(maintenance).await?;
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
    loop {
        let devices = refresh_device_snapshots(client).await?;
        if audit_site(&devices, &site.belt).operational {
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
        sleep(POLL_INTERVAL).await;
    }
    info!(system = %site.system, belt = %site.belt, "mining site operational");
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
    let operation = client
        .devices()
        .get(controller)
        .await?
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
    let transport = client
        .devices()
        .get(controller)
        .await?
        .as_transport_controller()?;
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
        sleep(POLL_INTERVAL).await;
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
    loop {
        let mut pending = 0usize;
        for site in &mut mission.sites {
            let Some(carrier) = site.carrier.clone() else {
                continue;
            };
            let snapshot = client.devices().get(&carrier).await?.snapshot().await?;
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
        sleep(POLL_INTERVAL).await;
    }
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
        let handle = client.devices().get(code).await?;
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
    let handle = client.devices().get(code).await?;
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
    let handle = client.devices().get(code).await?;
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

async fn attach_devices(client: &Client, carrier: &str, devices: &[String]) -> AnyResult<()> {
    if devices.is_empty() {
        return Ok(());
    }
    let operation = client
        .devices()
        .get(carrier)
        .await?
        .attach(targets(devices))
        .await?;
    ensure_operation_accepted(&operation).await?;
    let deadline = Instant::now() + VERIFY_TIMEOUT;
    loop {
        let snapshot = client.devices().get(carrier).await?.snapshot().await?;
        let attached = snapshot
            .relationships
            .attached_devices
            .iter()
            .map(|device| device.id.as_str())
            .collect::<BTreeSet<_>>();
        if devices
            .iter()
            .all(|device| attached.contains(device.as_str()))
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("carrier {carrier} did not report all attached devices"),
            ));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn detach_devices(client: &Client, carrier: &str, devices: &[String]) -> AnyResult<()> {
    if devices.is_empty() {
        return Ok(());
    }
    let operation = client
        .devices()
        .get(carrier)
        .await?
        .command(api_raw::devices::DeviceCommand::Detach(targets(devices)))
        .await?;
    ensure_operation_accepted(&operation).await?;
    let deadline = Instant::now() + VERIFY_TIMEOUT;
    loop {
        let mut detached = true;
        for code in devices {
            let snapshot = client.devices().get(code).await?.snapshot().await?;
            if snapshot.relationships.attached_to.is_some() {
                detached = false;
                break;
            }
        }
        if detached {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("carrier {carrier} did not finish detaching its payload"),
            ));
        }
        sleep(POLL_INTERVAL).await;
    }
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
    let handle = client.devices().get(code).await?;
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
    let operation = handle
        .command(api_raw::devices::DeviceCommand::Travel {
            destination: destination.to_owned(),
            dry_run: None,
            via: None,
        })
        .await?;
    ensure_operation_accepted(&operation).await
}

async fn ensure_operation_accepted(operation: &Operation) -> AnyResult<()> {
    let outcome = operation.wait_timeout(OPERATION_WAIT).await?;
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
