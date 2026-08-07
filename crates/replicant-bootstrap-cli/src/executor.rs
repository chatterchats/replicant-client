use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::{Duration, Instant},
};

use futures::future::join_all;
use replicant_bootstrap_planner::{
    AUTOFACTORY, BeltCandidate, FTL_RELAY, SEED_RESOURCES, SURGE_CARRIER, ark_device_requirements,
    attachment_slots, carrier_provisioning, select_dense_belts,
};
use replicant_client::{
    Client, Operation, OperationStatus, Replicant,
    domain::{GalacticPosition, Location},
    raw,
};
use replicant_mining_cli::{MiningExpansionRequest, execute_expansion as execute_mining};
use replicant_mining_planner::{
    CARGO_FREIGHTER, MAINTENANCE_DRONE, SURVEY_CONTROLLER, SURVEY_DRONE,
};
use replicant_printing::{
    PrintRequest,
    managed::{QueueOptions, fetch_blueprints, queue_prints},
};
use replicant_relay_cli::{RelayExpansionRequest, execute_expansion as execute_relays};
use replicant_survey_cli::{SurveyRequest, execute_survey};
use serde_json::{Map, Value};
use tokio::time::sleep;
use tracing::{info, warn};

use crate::{
    AnyResult, Config, app_error,
    model::{
        BootstrapMission, CarrierLoad, MissionPhase, PrintState, ReplicantIdentity, SeedFreighter,
    },
    reservation_tag, save_mission, unique,
};

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const QUICK_SCOUT_SYSTEM_LIMIT: usize = 12;
const MOBILE_FLEET: &str = "mobile_fleet";

pub async fn resolve_replicant(
    client: &Client,
    query: &str,
) -> AnyResult<Option<ReplicantIdentity>> {
    let handles = client.replicants().find().owned().collect().await?;
    let mut matches = Vec::<Replicant>::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if snapshot.key.id.as_str().eq_ignore_ascii_case(query)
            || snapshot
                .name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(query))
        {
            matches.push(snapshot);
        }
    }
    match matches.len() {
        1 => return Ok(identity_from_replicant(query, matches.remove(0))),
        0 => {}
        _ => {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("replicant name {query:?} is ambiguous; use its code"),
            ));
        }
    }

    let profiles = client
        .directory()
        .search(&raw::replicants::ReplicantListQuery {
            cursor: None,
            limit: Some(100),
            latest: None,
            name: Some(query.to_owned()),
        })
        .await?;
    let exact = profiles
        .into_iter()
        .filter(|profile| {
            profile.id.as_str().eq_ignore_ascii_case(query)
                || profile
                    .name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(query))
        })
        .collect::<Vec<_>>();
    let mut discovered = Vec::new();
    for profile in exact {
        let public = client.directory().replicant(profile.id.as_str()).await?;
        if public.hosted_device.is_none() {
            continue;
        }
        let handle = client.replicants().get_owned(profile.id.as_str()).await?;
        let owned = handle.snapshot().await?;
        if let Some(identity) = identity_from_replicant(query, owned) {
            discovered.push(identity);
        }
    }
    match discovered.len() {
        0 => Ok(None),
        1 => Ok(discovered.pop()),
        _ => Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("multiple hosted replicants match {query:?}; use a replicant code"),
        )),
    }
}

fn identity_from_replicant(query: &str, replicant: Replicant) -> Option<ReplicantIdentity> {
    let vessel = replicant.hosted_device.as_ref()?;
    Some(ReplicantIdentity {
        requested: query.to_owned(),
        code: replicant.key.id.as_str().to_owned(),
        name: replicant.name,
        vessel: vessel.id.as_str().to_owned(),
    })
}

pub async fn resolve_star(client: &Client, designation: &str) -> AnyResult<(String, String)> {
    let catalogue = client.galaxy().catalogue();
    let star = catalogue
        .iter()
        .find(|star| star.key.id.as_str().eq_ignore_ascii_case(designation))
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("star {designation} is not in the catalogue"),
            )
        })?;
    let entry = star.entry_point.as_ref().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            format!("star {designation} has no known entry point"),
        )
    })?;
    Ok((
        entry.id.as_str().to_owned(),
        star.region.clone().unwrap_or_else(|| "unknown".into()),
    ))
}

pub async fn execute(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if mission.phase.is_terminal() {
        info!(phase=?mission.phase, "regional bootstrap is already complete");
        return Ok(());
    }
    let sync = client.sync().full().await?;
    info!(readiness=?sync.readiness, phase=?mission.phase, "reconciled managed state");
    client.galaxy().refresh_catalogue().await?;
    ensure_source_entry(client, config, mission).await?;
    resolve_required_replicants(client, config, mission).await?;
    claim_staged_ark_for_operator(client, mission).await?;

    manufacture_ark(client, config, mission).await?;
    load_ark(client, config, mission).await?;
    if matches!(
        mission.phase,
        MissionPhase::StagingAtSource | MissionPhase::StagedAtSource
    ) {
        stage_at_source_entry(client, config, mission).await?;
    }
    dispatch_to_landing(client, config, mission).await?;
    quick_scout(client, config, mission).await?;
    establish_capital(client, config, mission).await?;
    establish_initial_mine(client, config, mission).await?;
    survey_region(client, config, mission).await?;
    expand_relays(client, config, mission).await?;
    expand_mining(client, config, mission).await?;
    cleanup(client, config, mission).await
}

pub async fn stage(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::StagedAtSource) {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "the mission has already departed the source staging point; use `run` to continue it",
        ));
    }
    let sync = client.sync().full().await?;
    info!(readiness=?sync.readiness, phase=?mission.phase, "reconciled managed state for source staging");
    client.galaxy().refresh_catalogue().await?;
    ensure_source_entry(client, config, mission).await?;
    manufacture_ark(client, config, mission).await?;
    load_ark(client, config, mission).await?;
    stage_at_source_entry(client, config, mission).await
}

pub(crate) async fn ensure_source_entry(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if !mission.source_system.is_empty() && !mission.source_entry.is_empty() {
        return Ok(());
    }
    let catalogue = client.galaxy().catalogue();
    let source = catalogue
        .iter()
        .filter(|star| {
            let system = star.key.id.as_str();
            mission.source_hub == system || mission.source_hub.starts_with(&format!("{system}-"))
        })
        .max_by_key(|star| star.key.id.as_str().len())
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "source hub {} does not resolve to a catalogue system",
                    mission.source_hub
                ),
            )
        })?;
    mission.source_system = source.key.id.as_str().to_owned();
    mission.source_entry = source
        .entry_point
        .as_ref()
        .map(|entry| entry.id.as_str().to_owned())
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "source system {} has no known entry point",
                    mission.source_system
                ),
            )
        })?;
    save_mission(&config.mission_file, mission)
}

async fn resolve_required_replicants(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    let operator_query = mission.operator.query().to_owned();
    mission.operator = resolve_replicant(client, &operator_query).await?
        .ok_or_else(|| app_error(io::ErrorKind::NotFound,
            format!("planned operator {operator_query:?} does not exist with a hosted vessel yet; use `stage` to prepare the ark without it")))?;
    let explorer_query = mission.explorer.query().to_owned();
    mission.explorer = resolve_replicant(client, &explorer_query).await?
        .ok_or_else(|| app_error(io::ErrorKind::NotFound,
            format!("planned explorer {explorer_query:?} does not exist with a hosted vessel yet; use `stage` to prepare the ark without it")))?;
    if mission.operator.code == mission.explorer.code {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "operator and explorer resolved to the same replicant",
        ));
    }
    save_mission(&config.mission_file, mission)
}

async fn set_phase(
    config: &Config,
    mission: &mut BootstrapMission,
    phase: MissionPhase,
) -> AnyResult<()> {
    if mission.phase != phase {
        mission.phase = phase;
        save_mission(&config.mission_file, mission)?;
        info!(phase=?phase, "regional bootstrap phase changed");
    }
    Ok(())
}

async fn manufacture_ark(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::StagedAtSource) {
        return Ok(());
    }
    if !phase_after(mission.phase, MissionPhase::ManufacturingArk) {
        set_phase(config, mission, MissionPhase::ManufacturingArk).await?;
    }
    let blueprints = fetch_blueprints(client).await?;
    let carrier_capacity = client
        .raw()
        .blueprints()
        .list()
        .await?
        .value
        .blueprints
        .into_iter()
        .find(|item| item.device_type.as_deref() == Some(SURGE_CARRIER))
        .and_then(|item| item.attach_capacity)
        .unwrap_or(0);
    if carrier_capacity <= 0 {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            "Surge Carrier blueprint has no positive attach capacity",
        ));
    }

    if remove_system_hub_request(&mut mission.print) {
        info!("removed the obsolete System Hub from the regional ark print plan");
        save_mission(&config.mission_file, mission)?;
    }
    let desired = ark_device_requirements(&mission.profile);
    let devices = list_devices(client, Some(&mission.source_hub), None).await?;
    let mut used = mission
        .assets
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>();

    // Reconcile ordinary idle stock on every staging pass. Autofactories remain
    // print-only so the source hub never donates its production infrastructure.
    for (device_type, quantity) in &desired {
        if device_type == AUTOFACTORY {
            continue;
        }
        let current = mission.assets.entry(device_type.clone()).or_default();
        let missing = usize::try_from(quantity.saturating_sub(i64::try_from(current.len())?))?;
        let selected = devices
            .iter()
            .filter(|device| eligible_idle(device, &mission.mission_tag))
            .filter(|device| device.device_type.as_deref() == Some(device_type))
            .filter_map(|device| device.device_code.clone())
            .filter(|code| !used.contains(code))
            .take(missing)
            .collect::<Vec<_>>();
        for code in selected {
            used.insert(code.clone());
            current.push(code);
        }
    }

    let current_carriers = i64::try_from(mission.assets.get(SURGE_CARRIER).map_or(0, Vec::len))?;
    if mission.carrier_target == 0 {
        if current_carriers > 0 || !mission.carrier_loads.is_empty() {
            // Migration for an ark assembled by the older implementation: keep
            // its recorded convoy and add the new three-carrier expansion reserve.
            mission.carrier_target =
                current_carriers.saturating_add(mission.profile.dedicated_surge_carriers);
            mission.reused_carrier_target = mission.carrier_target;
            info!(
                existing = current_carriers,
                target = mission.carrier_target,
                "upgraded the staged ark with dedicated relay and beacon carriers"
            );
        } else {
            let mut candidates = devices
                .iter()
                .filter(|device| eligible_idle(device, &mission.mission_tag))
                .filter(|device| is_attachment_carrier(device.device_type.as_deref()))
                .filter(|device| device.attached_devices.is_empty())
                .filter_map(|device| {
                    Some((
                        device.device_code.clone()?,
                        device.attach_capacity.unwrap_or(carrier_capacity),
                    ))
                })
                .filter(|(code, _)| !used.contains(code))
                .collect::<Vec<_>>();
            candidates
                .sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
            let capacities = candidates
                .iter()
                .map(|(_, capacity)| *capacity)
                .collect::<Vec<_>>();
            let (reuse_count, print_count) = carrier_provisioning(
                attachment_slots(&desired),
                &capacities,
                carrier_capacity,
                mission.profile.dedicated_surge_carriers,
            )?;
            let carriers = mission.assets.entry(SURGE_CARRIER.into()).or_default();
            for (code, _) in candidates.into_iter().take(reuse_count) {
                used.insert(code.clone());
                carriers.push(code);
            }
            mission.reused_carrier_target = i64::try_from(carriers.len())?;
            mission.carrier_target = mission.reused_carrier_target.saturating_add(print_count);
            info!(
                reused = mission.reused_carrier_target,
                printing = print_count,
                target = mission.carrier_target,
                "planned minimum attachment-carrier fleet"
            );
        }
    }

    // Older staged missions may be satisfied with manually printed carriers.
    // New missions cap reuse at the exact count chosen above, so later resumes
    // cannot silently consume every idle carrier at the source hub.
    let current_carriers = i64::try_from(mission.assets.get(SURGE_CARRIER).map_or(0, Vec::len))?;
    let reusable_missing = usize::try_from(
        mission
            .reused_carrier_target
            .saturating_sub(current_carriers),
    )?;
    let carriers = mission.assets.entry(SURGE_CARRIER.into()).or_default();
    let selected = devices
        .iter()
        .filter(|device| eligible_idle(device, &mission.mission_tag))
        .filter(|device| is_attachment_carrier(device.device_type.as_deref()))
        .filter(|device| device.attached_devices.is_empty())
        .filter_map(|device| device.device_code.clone())
        .filter(|code| !used.contains(code))
        .take(reusable_missing)
        .collect::<Vec<_>>();
    for code in selected {
        used.insert(code.clone());
        carriers.push(code);
    }

    mission.print.targets = desired.clone();
    mission
        .print
        .targets
        .insert(SURGE_CARRIER.into(), mission.carrier_target);
    save_mission(&config.mission_file, mission)?;
    claim_recorded_assets(client, mission).await?;
    reconcile_interrupted_print_submission(client, config, mission).await?;

    if !mission.print.queued && !mission.print.requirements.is_empty() {
        mission.print.submission_started = true;
        save_mission(&config.mission_file, mission)?;
        let (modular, standard): (Vec<_>, Vec<_>) = mission
            .print
            .requirements
            .iter()
            .filter(|(_, quantity)| **quantity > 0)
            .map(|(device_type, quantity)| PrintRequest::new(device_type.clone(), *quantity))
            .partition(|request| {
                blueprints
                    .get(&request.device_type)
                    .is_some_and(|blueprint| blueprint.is_modular())
            });
        let tags = vec![mission.mission_tag.clone(), mission.region_tag.clone()];
        if !standard.is_empty() {
            let mut options = QueueOptions::at(&mission.source_hub);
            options.tags = tags.clone();
            options.wait_timeout = config.wait_timeout;
            let report = match queue_prints(client, &standard, &options).await {
                Ok(report) => report,
                Err(error) => {
                    reconcile_interrupted_print_submission(client, config, mission).await?;
                    return Err(error.into());
                }
            };
            mission.print.operation_ids.extend(report.operation_ids);
        }
        if !modular.is_empty() {
            let mut options = QueueOptions::at(&mission.source_hub);
            options.tags = tags;
            options.flatpack = true;
            options.wait_timeout = config.wait_timeout;
            let report = match queue_prints(client, &modular, &options).await {
                Ok(report) => report,
                Err(error) => {
                    reconcile_interrupted_print_submission(client, config, mission).await?;
                    return Err(error.into());
                }
            };
            mission.print.operation_ids.extend(report.operation_ids);
        }
        mission.print.queued = true;
        save_mission(&config.mission_file, mission)?;
    }

    wait_for_printed_assets(client, config, mission, &desired).await?;
    allocate_printed_assets(client, config, mission, &desired).await?;
    claim_recorded_assets(client, mission).await?;
    Ok(())
}

async fn reconcile_interrupted_print_submission(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    let tagged = list_devices(
        client,
        Some(&mission.source_hub),
        Some(&mission.mission_tag),
    )
    .await?;
    let recorded_codes = mission
        .assets
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut completed = BTreeMap::<String, i64>::new();
    for device in &tagged {
        let Some(code) = device.device_code.as_ref() else {
            continue;
        };
        let Some(device_type) = device.device_type.as_ref() else {
            continue;
        };
        if !recorded_codes.contains(code) {
            *completed.entry(device_type.clone()).or_default() += 1;
        }
    }
    let pending = pending_tagged_prints(client, &mission.source_hub, &mission.mission_tag).await?;
    let recorded = mission
        .assets
        .iter()
        .map(|(device_type, codes)| {
            (
                device_type.clone(),
                i64::try_from(codes.len()).unwrap_or(i64::MAX),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let previous = mission.print.requirements.clone();
    let was_submission_started = mission.print.submission_started;
    mission.print.requirements =
        remaining_print_requirements(&mission.print.targets, &recorded, &completed, &pending);
    mission.print.submission_started = false;
    mission.print.queued = mission.print.requirements.is_empty();
    if mission.print.requirements != previous || was_submission_started {
        info!(remaining=?mission.print.requirements, completed=?completed, pending=?pending,
            "reconciled regional ark print manifest");
    }
    save_mission(&config.mission_file, mission)
}

async fn pending_tagged_prints(
    client: &Client,
    hub: &str,
    mission_tag: &str,
) -> AnyResult<BTreeMap<String, i64>> {
    let factories = list_devices(client, Some(hub), None).await?;
    let mut pending = BTreeMap::<String, i64>::new();
    for factory in factories
        .iter()
        .filter(|device| device.device_type.as_deref() == Some(AUTOFACTORY))
    {
        if let Some(printing) = &factory.printing
            && printing.tags.iter().any(|tag| tag == mission_tag)
            && let Some(device_type) = &printing.device_type
        {
            *pending.entry(device_type.clone()).or_default() += 1;
        }
        for job in &factory.print_queue {
            let tagged = job
                .get("tags")
                .and_then(Value::as_array)
                .is_some_and(|tags| tags.iter().any(|tag| tag.as_str() == Some(mission_tag)));
            if !tagged {
                continue;
            }
            let Some(device_type) = ["device_type", "type"]
                .into_iter()
                .find_map(|key| job.get(key).and_then(Value::as_str))
            else {
                continue;
            };
            let quantity = ["quantity", "count"]
                .into_iter()
                .find_map(|key| job.get(key).and_then(Value::as_i64))
                .unwrap_or(1)
                .max(1);
            *pending.entry(device_type.to_owned()).or_default() += quantity;
        }
    }
    Ok(pending)
}

fn remaining_print_requirements(
    targets: &BTreeMap<String, i64>,
    recorded: &BTreeMap<String, i64>,
    completed: &BTreeMap<String, i64>,
    pending: &BTreeMap<String, i64>,
) -> BTreeMap<String, i64> {
    targets
        .iter()
        .filter_map(|(device_type, target)| {
            let accounted = recorded
                .get(device_type)
                .copied()
                .unwrap_or(0)
                .saturating_add(completed.get(device_type).copied().unwrap_or(0))
                .saturating_add(pending.get(device_type).copied().unwrap_or(0));
            let remaining = target.saturating_sub(accounted);
            (remaining > 0).then_some((device_type.clone(), remaining))
        })
        .collect()
}

fn remove_system_hub_request(print: &mut PrintState) -> bool {
    let removed_target = print.targets.remove("system_hub").is_some();
    let removed_requirement = print.requirements.remove("system_hub").is_some();
    removed_target || removed_requirement
}

async fn wait_for_printed_assets(
    client: &Client,
    config: &Config,
    mission: &BootstrapMission,
    desired: &BTreeMap<String, i64>,
) -> AnyResult<()> {
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        let tagged = list_devices(
            client,
            Some(&mission.source_hub),
            Some(&mission.mission_tag),
        )
        .await?;
        let complete = desired.iter().all(|(device_type, quantity)| {
            let recorded = mission.assets.get(device_type).map_or(0, Vec::len);
            let printed = tagged
                .iter()
                .filter(|device| {
                    device.device_type.as_deref() == Some(device_type)
                        && device.device_code.as_ref().is_some_and(|code| {
                            !mission
                                .assets
                                .values()
                                .flatten()
                                .any(|existing| existing == code)
                        })
                })
                .count();
            recorded.saturating_add(printed) >= usize::try_from(*quantity).unwrap_or(usize::MAX)
        });
        let loaded_carriers = mission
            .carrier_loads
            .iter()
            .map(|load| load.carrier.clone())
            .collect::<BTreeSet<_>>();
        let loaded_capacity = mission
            .carrier_loads
            .iter()
            .map(|load| load.capacity.max(0))
            .sum::<i64>();
        let hub_carriers = tagged
            .iter()
            .filter(|device| is_attachment_carrier(device.device_type.as_deref()))
            .filter(|device| {
                device
                    .device_code
                    .as_ref()
                    .is_some_and(|code| !loaded_carriers.contains(code))
            })
            .collect::<Vec<_>>();
        let carrier_capacity = loaded_capacity.saturating_add(
            hub_carriers
                .iter()
                .map(|device| device.attach_capacity.unwrap_or(0).max(0))
                .sum::<i64>(),
        );
        let carrier_count = loaded_carriers.len().saturating_add(hub_carriers.len());
        let carriers_ready = carrier_capacity >= attachment_slots(desired)
            && carrier_count >= usize::try_from(mission.carrier_target).unwrap_or(usize::MAX);
        if complete && carriers_ready {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                "timed out waiting for the regional ark to finish printing",
            ));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn allocate_printed_assets(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
    desired: &BTreeMap<String, i64>,
) -> AnyResult<()> {
    let tagged = list_devices(
        client,
        Some(&mission.source_hub),
        Some(&mission.mission_tag),
    )
    .await?;
    let mut used = mission
        .assets
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>();
    for (device_type, quantity) in desired {
        let current = mission.assets.entry(device_type.clone()).or_default();
        let missing = usize::try_from(quantity.saturating_sub(i64::try_from(current.len())?))?;
        let selected = tagged
            .iter()
            .filter(|device| device.device_type.as_deref() == Some(device_type))
            .filter_map(|device| device.device_code.clone())
            .filter(|code| !used.contains(code))
            .take(missing)
            .collect::<Vec<_>>();
        for code in selected {
            used.insert(code.clone());
            current.push(code);
        }
        if current.len() < usize::try_from(*quantity)? {
            return Err(app_error(
                io::ErrorKind::NotFound,
                format!("printed {device_type} outputs are incomplete"),
            ));
        }
    }
    let loaded_carriers = mission
        .carrier_loads
        .iter()
        .map(|load| load.carrier.clone())
        .collect::<BTreeSet<_>>();
    let mut capacity = mission
        .carrier_loads
        .iter()
        .map(|load| load.capacity.max(0))
        .sum::<i64>();
    let carriers = mission.assets.entry(SURGE_CARRIER.into()).or_default();
    capacity = capacity.saturating_add(
        tagged
            .iter()
            .filter(|device| {
                device
                    .device_code
                    .as_ref()
                    .is_some_and(|code| carriers.contains(code) && !loaded_carriers.contains(code))
            })
            .map(|device| device.attach_capacity.unwrap_or(0).max(0))
            .sum::<i64>(),
    );
    for device in tagged
        .iter()
        .filter(|device| is_attachment_carrier(device.device_type.as_deref()))
    {
        if capacity >= attachment_slots(desired)
            && carriers.len() >= usize::try_from(mission.carrier_target)?
        {
            break;
        }
        let Some(code) = device.device_code.clone() else {
            continue;
        };
        if used.insert(code.clone()) {
            capacity += device.attach_capacity.unwrap_or(0).max(0);
            carriers.push(code);
        }
    }
    if capacity < attachment_slots(desired)
        || carriers.len() < usize::try_from(mission.carrier_target)?
    {
        return Err(app_error(
            io::ErrorKind::NotFound,
            "available Surge Carrier count or attachment capacity is incomplete",
        ));
    }
    save_mission(&config.mission_file, mission)
}

async fn claim_recorded_assets(client: &Client, mission: &BootstrapMission) -> AnyResult<()> {
    let owner = mission
        .operator
        .is_resolved()
        .then_some(mission.operator.code.as_str());
    for code in mission.assets.values().flatten() {
        ensure_claim(
            client,
            code,
            owner,
            &[mission.mission_tag.clone(), mission.region_tag.clone()],
        )
        .await?;
    }
    Ok(())
}

async fn claim_staged_ark_for_operator(
    client: &Client,
    mission: &BootstrapMission,
) -> AnyResult<()> {
    let assembled_at_source = !mission.carrier_loads.is_empty()
        && matches!(
            mission.phase,
            MissionPhase::LoadingArk | MissionPhase::StagingAtSource | MissionPhase::StagedAtSource
        );
    if assembled_at_source {
        for load in &mission.carrier_loads {
            detach_devices(client, &load.carrier, &load.devices).await?;
        }
    }
    claim_recorded_assets(client, mission).await?;
    if assembled_at_source {
        for load in &mission.carrier_loads {
            attach_devices(client, &load.carrier, &load.devices).await?;
        }
    }
    Ok(())
}

async fn load_ark(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::StagedAtSource) {
        return Ok(());
    }
    if !phase_after(mission.phase, MissionPhase::LoadingArk) {
        set_phase(config, mission, MissionPhase::LoadingArk).await?;
    }
    if mission.seed_freighters.is_empty() {
        let freighters = mission
            .assets
            .get(CARGO_FREIGHTER)
            .cloned()
            .unwrap_or_default();
        if freighters.len() < SEED_RESOURCES.len() {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                "ark has fewer than six Cargo Freighters",
            ));
        }
        ensure_seed_inventory(client, mission).await?;
        mission.seed_freighters = freighters
            .into_iter()
            .zip(SEED_RESOURCES)
            .map(|(code, resource)| SeedFreighter {
                code,
                resource: resource.into(),
                quantity: mission.seed_quantity,
            })
            .collect();
        save_mission(&config.mission_file, mission)?;
    }
    let collect_results = join_all(
        mission
            .seed_freighters
            .iter()
            .map(|seed| collect_resource(client, &seed.code, &seed.resource, seed.quantity)),
    )
    .await;
    finish_all(collect_results)?;

    append_missing_carrier_loads(client, config, mission).await?;
    let attach_results = join_all(
        mission
            .carrier_loads
            .iter()
            .map(|load| attach_devices(client, &load.carrier, &load.devices)),
    )
    .await;
    finish_all(attach_results)
}

async fn append_missing_carrier_loads(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    let assigned = mission
        .carrier_loads
        .iter()
        .flat_map(|load| load.devices.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut unassigned = mission
        .assets
        .iter()
        .filter(|(device_type, _)| !matches!(device_type.as_str(), CARGO_FREIGHTER | SURGE_CARRIER))
        .flat_map(|(_, codes)| codes.iter().cloned())
        .filter(|code| !assigned.contains(code))
        .collect::<BTreeSet<_>>();
    if unassigned.is_empty() {
        return Ok(());
    }

    let used_carriers = mission
        .carrier_loads
        .iter()
        .map(|load| load.carrier.clone())
        .collect::<BTreeSet<_>>();
    let mut carriers = Vec::new();
    for carrier in mission
        .assets
        .get(SURGE_CARRIER)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|carrier| !used_carriers.contains(carrier))
    {
        let capacity = client
            .raw()
            .devices()
            .get(&carrier)
            .await?
            .value
            .attach_capacity
            .unwrap_or(0);
        carriers.push((carrier, capacity));
    }
    carriers.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));

    // Keep the 18 expansion relays and nine beacons in three self-contained
    // carrier loads. This makes the regional-network reserve easy to inspect
    // and deploy without disturbing the rest of the ark.
    let root_relay = first_asset(mission, FTL_RELAY)?;
    let expansion_relays = mission
        .assets
        .get(FTL_RELAY)
        .into_iter()
        .flatten()
        .filter(|code| code.as_str() != root_relay && unassigned.contains(*code))
        .cloned()
        .collect::<Vec<_>>();
    let beacons = mission
        .assets
        .get(replicant_bootstrap_planner::FTL_BEACON)
        .into_iter()
        .flatten()
        .filter(|code| unassigned.contains(*code))
        .cloned()
        .collect::<Vec<_>>();
    let mut reserved_groups = expansion_relays
        .chunks(9)
        .map(|chunk| chunk.to_vec())
        .chain(beacons.chunks(9).map(|chunk| chunk.to_vec()))
        .collect::<Vec<_>>();
    reserved_groups.retain(|group| !group.is_empty());
    for group in reserved_groups {
        let required = i64::try_from(group.len())?;
        let Some(index) = carriers
            .iter()
            .position(|(_, capacity)| *capacity >= required)
        else {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "no unused carrier can hold a reserved {required}-device relay/beacon load"
                ),
            ));
        };
        let (carrier, capacity) = carriers.remove(index);
        for code in &group {
            unassigned.remove(code);
        }
        mission.carrier_loads.push(CarrierLoad {
            carrier,
            capacity,
            devices: group,
        });
    }

    let mut payload = unassigned.into_iter().collect::<Vec<_>>();
    payload.sort();
    if let Some(index) = payload.iter().position(|code| code == &root_relay) {
        payload.swap(0, index);
    }
    let mut cursor = 0_usize;
    for (carrier, capacity) in carriers {
        let take = usize::try_from(capacity.max(0))?.min(payload.len().saturating_sub(cursor));
        if take == 0 {
            continue;
        }
        let devices = payload[cursor..cursor + take].to_vec();
        cursor += take;
        mission.carrier_loads.push(CarrierLoad {
            carrier,
            capacity,
            devices,
        });
        if cursor == payload.len() {
            break;
        }
    }
    if cursor != payload.len() {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "carrier capacity covers {cursor} of {} remaining payload devices",
                payload.len()
            ),
        ));
    }
    save_mission(&config.mission_file, mission)
}

async fn stage_at_source_entry(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if mission.phase != MissionPhase::StagedAtSource {
        set_phase(config, mission, MissionPhase::StagingAtSource).await?;
    } else {
        info!(source_entry=%mission.source_entry,
            "checking the staged ark for newly manufactured catch-up loads");
    }
    let devices = mission
        .carrier_loads
        .iter()
        .map(|load| load.carrier.clone())
        .chain(
            mission
                .assets
                .get(CARGO_FREIGHTER)
                .into_iter()
                .flatten()
                .cloned(),
        )
        .collect::<Vec<_>>();
    info!(devices=devices.len(), destination=%mission.source_entry,
        "moving the assembled ark to the source system entry point concurrently");
    finish_all(
        join_all(
            devices
                .iter()
                .map(|code| start_device_travel(client, code, &mission.source_entry)),
        )
        .await,
    )?;
    finish_all(
        join_all(
            devices.iter().map(|code| {
                wait_device_at(client, code, &mission.source_entry, config.wait_timeout)
            }),
        )
        .await,
    )?;
    set_phase(config, mission, MissionPhase::StagedAtSource).await
}

async fn dispatch_to_landing(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::Outbound) {
        return Ok(());
    }
    set_phase(config, mission, MissionPhase::Outbound).await?;
    let devices = mission
        .carrier_loads
        .iter()
        .map(|load| load.carrier.clone())
        .chain(
            mission
                .assets
                .get(CARGO_FREIGHTER)
                .into_iter()
                .flatten()
                .cloned(),
        )
        .collect::<Vec<_>>();
    let departures = devices
        .iter()
        .map(|code| (code.clone(), mission.landing_entry.clone()))
        .collect::<Vec<_>>();
    ensure_operator_in_comms_for_departures(
        client,
        &mission.operator.code,
        &departures,
        config.wait_timeout,
    )
    .await?;
    info!(devices=devices.len(), destination=%mission.landing_entry,
        "submitting complete ark travel before the controlling replicant departs");
    finish_all(
        join_all(
            devices
                .iter()
                .map(|code| start_device_travel(client, code, &mission.landing_entry)),
        )
        .await,
    )?;
    let (operator_start, explorer_start) = tokio::join!(
        start_replicant_travel(client, &mission.operator.code, &mission.landing_entry),
        start_replicant_travel(client, &mission.explorer.code, &mission.landing_entry),
    );
    operator_start?;
    explorer_start?;
    let (device_waits, operator_wait, explorer_wait) = tokio::join!(
        async {
            finish_all(
                join_all(devices.iter().map(|code| {
                    wait_device_at(client, code, &mission.landing_entry, config.wait_timeout)
                }))
                .await,
            )
        },
        wait_replicant_at(
            client,
            &mission.operator.code,
            &mission.landing_entry,
            config.wait_timeout
        ),
        wait_replicant_at(
            client,
            &mission.explorer.code,
            &mission.landing_entry,
            config.wait_timeout
        ),
    );
    device_waits?;
    operator_wait?;
    explorer_wait
}

async fn quick_scout(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::QuickScouting) {
        return Ok(());
    }
    if mission.capital_system.is_some()
        && mission.capital_belt.is_some()
        && mission.capital_entry.is_some()
    {
        return Ok(());
    }

    set_phase(config, mission, MissionPhase::QuickScouting).await?;
    recover_legacy_quick_survey_fleet(client, config, mission).await?;
    let route = quick_scout_route(
        client,
        &mission.landing_star,
        mission.quick_scout_radius_ly,
        QUICK_SCOUT_SYSTEM_LIMIT,
    )?;
    let completed = mission
        .quick_scouted_systems
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();

    for system in route {
        if completed.contains(&system) {
            continue;
        }

        travel_replicant_to_system(client, &mission.explorer.code, &system, config.wait_timeout)
            .await?;
        scan_system(client, &mission.explorer.code, &system).await?;

        mission.quick_scouted_systems.push(system.clone());
        let completed_systems = std::mem::take(&mut mission.quick_scouted_systems);
        mission.quick_scouted_systems = unique(completed_systems);
        save_mission(&config.mission_file, mission)?;
        info!(
            system,
            completed = mission.quick_scouted_systems.len(),
            "quick scout system scan completed"
        );
    }

    let mut candidates = belt_candidates(
        client,
        &mission.landing_star,
        &mission.quick_scouted_systems,
    )
    .await?;
    candidates.sort_by(|left, right| {
        density_rank(&right.density)
            .cmp(&density_rank(&left.density))
            .then_with(|| {
                left.distance_from_capital_ly
                    .total_cmp(&right.distance_from_capital_ly)
            })
            .then_with(|| left.system.cmp(&right.system))
            .then_with(|| left.designation.cmp(&right.designation))
    });
    let capital = candidates
        .into_iter()
        .find(|candidate| candidate.density.eq_ignore_ascii_case("dense"))
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!(
                    "quick scout found no dense belt within {} ly of {}",
                    mission.quick_scout_radius_ly, mission.landing_star
                ),
            )
        })?;
    let (entry, _) = resolve_star(client, &capital.system).await?;
    mission.capital_system = Some(capital.system);
    mission.capital_belt = Some(capital.designation);
    mission.capital_entry = Some(entry);
    save_mission(&config.mission_file, mission)
}

fn quick_scout_route(
    client: &Client,
    center: &str,
    radius_ly: f64,
    system_limit: usize,
) -> AnyResult<Vec<String>> {
    let catalogue = client.galaxy().catalogue();
    let center_position = catalogue
        .iter()
        .find(|star| star.key.id.as_str() == center)
        .and_then(|star| star.position)
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("{center} has no catalogue position"),
            )
        })?;

    let mut nearby = catalogue
        .iter()
        .filter_map(|star| {
            let system = star.key.id.as_str();
            if system == center {
                return None;
            }
            let position = star.position?;
            (distance(center_position, position) <= radius_ly)
                .then(|| (system.to_owned(), position))
        })
        .collect::<Vec<_>>();
    nearby.sort_by(|left, right| left.0.cmp(&right.0));

    Ok(nearest_neighbor_system_route(
        center.to_owned(),
        center_position,
        nearby,
        system_limit.max(1),
    ))
}

fn nearest_neighbor_system_route(
    center: String,
    center_position: GalacticPosition,
    mut remaining: Vec<(String, GalacticPosition)>,
    system_limit: usize,
) -> Vec<String> {
    let mut route = vec![center];
    let mut current = center_position;

    while !remaining.is_empty() && route.len() < system_limit {
        let Some((index, _)) = remaining
            .iter()
            .enumerate()
            .map(|(index, (_, position))| (index, distance(current, *position)))
            .min_by(
                |(left_index, left_distance), (right_index, right_distance)| {
                    left_distance
                        .total_cmp(right_distance)
                        .then_with(|| remaining[*left_index].0.cmp(&remaining[*right_index].0))
                },
            )
        else {
            break;
        };
        let (system, position) = remaining.remove(index);
        route.push(system);
        current = position;
    }

    route
}

async fn travel_replicant_to_system(
    client: &Client,
    code: &str,
    destination: &str,
    timeout: Duration,
) -> AnyResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let handle = client.replicants().get_owned(code).await?;
        let snapshot = handle.snapshot().await?;
        if snapshot.travel.is_none()
            && snapshot
                .location
                .as_ref()
                .is_some_and(|location| designation_in_system(location.id.as_str(), destination))
        {
            return Ok(());
        }

        if let Some(travel) = &snapshot.travel {
            let planned = travel
                .final_destination
                .as_ref()
                .or(travel.destination.as_ref())
                .map(|location| location.id.as_str());
            if !planned.is_some_and(|location| designation_in_system(location, destination)) {
                return Err(app_error(
                    io::ErrorKind::Other,
                    format!(
                        "replicant {code} is travelling to {planned:?}, not system {destination}"
                    ),
                ));
            }
        } else {
            info!(
                replicant = code,
                destination, "quick scout departing for system"
            );
            ensure_operation(&handle.travel().to(destination).depart().await?).await?;
        }

        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for replicant {code} in system {destination}"),
            ));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn scan_system(client: &Client, replicant_code: &str, system: &str) -> AnyResult<()> {
    let locally_explored = client
        .galaxy()
        .replicant_star_knowledge(replicant_code)
        .into_iter()
        .any(|knowledge| knowledge.star.id.as_str() == system && knowledge.explored == Some(true));
    let explored = if locally_explored {
        true
    } else {
        client
            .galaxy()
            .refresh_replicant_star(replicant_code, system)
            .await?
            .explored
            == Some(true)
    };

    if explored {
        info!(
            replicant = replicant_code,
            system, "quick scout system was already scanned"
        );
    } else {
        info!(
            replicant = replicant_code,
            system,
            endpoint = "POST /v1/replicants/{code}/scan",
            "quick scout scanning system"
        );
        let handle = client.replicants().get_owned(replicant_code).await?;
        let operation = handle.scan().await?;
        let outcome = operation.outcome().await?;
        if !matches!(
            outcome.status,
            OperationStatus::ReconciliationRequired | OperationStatus::Completed
        ) {
            if matches!(
                outcome.status,
                OperationStatus::Rejected | OperationStatus::Cancelled | OperationStatus::Failed
            ) {
                return Err(app_error(
                    io::ErrorKind::Other,
                    format!(
                        "quick-scout system scan for {system} ended as {:?}: {:?}",
                        outcome.status, outcome.response
                    ),
                ));
            }

            let knowledge = client
                .galaxy()
                .refresh_replicant_star(replicant_code, system)
                .await?;
            if knowledge.explored != Some(true) {
                return Err(app_error(
                    io::ErrorKind::Other,
                    format!(
                        "quick-scout system scan operation {} for {system} is {:?}, and authoritative star knowledge does not confirm completion; rerun to reconcile without submitting a blind duplicate",
                        operation.id(),
                        outcome.status
                    ),
                ));
            }
        } else if let Err(error) = client
            .galaxy()
            .refresh_replicant_star(replicant_code, system)
            .await
        {
            warn!(
                replicant = replicant_code,
                system,
                operation_id = %operation.id(),
                operation_status = ?outcome.status,
                error = %error,
                "system scan succeeded but the star-knowledge refresh failed"
            );
        }
    }

    let location = client.locations().get(system).await?;
    let belts = belts_from_location(&location);
    info!(
        replicant = replicant_code,
        system,
        belts = ?belts,
        "quick scout recorded system belt details"
    );
    Ok(())
}

async fn recover_legacy_quick_survey_fleet(
    client: &Client,
    config: &Config,
    mission: &BootstrapMission,
) -> AnyResult<()> {
    let Some(controller) = mission
        .assets
        .get(SURVEY_CONTROLLER)
        .and_then(|codes| codes.last())
        .cloned()
    else {
        return Ok(());
    };
    let Some(drones) = mission
        .assets
        .get(SURVEY_DRONE)
        .map(|codes| codes.iter().rev().take(3).cloned().collect::<Vec<_>>())
    else {
        return Ok(());
    };
    if drones.len() != 3 {
        return Ok(());
    }

    let vessel = client
        .devices()
        .get(&mission.explorer.vessel)
        .await?
        .snapshot()
        .await?;
    let vessel_location = vessel
        .location
        .as_ref()
        .map(|location| location.id.as_str().to_owned())
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "explorer vessel {} has no current location",
                    mission.explorer.vessel
                ),
            )
        })?;
    if !designation_in_system(&vessel_location, &mission.landing_star) {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!(
                "explorer vessel {} is at {vessel_location}, outside the landing system {}; legacy quick-scout devices cannot be recalled across systems",
                mission.explorer.vessel, mission.landing_star
            ),
        ));
    }

    let mut selected = vec![controller];
    selected.extend(drones);
    let mut recoverable = Vec::new();
    for code in selected {
        let snapshot = client.devices().get(&code).await?.snapshot().await?;
        if snapshot.relationships.attached_to.is_some()
            || snapshot
                .relationships
                .stowed_in
                .as_ref()
                .is_some_and(|vessel| vessel.id.as_str() == mission.explorer.vessel.as_str())
        {
            continue;
        }
        if let Some(other_vessel) = snapshot.relationships.stowed_in.as_ref() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "legacy quick-scout device {code} is stowed in {}, not explorer vessel {}",
                    other_vessel.id.as_str(),
                    mission.explorer.vessel
                ),
            ));
        }
        let assigned = snapshot
            .relationships
            .assigned_replicant
            .as_ref()
            .map(|replicant| replicant.id.as_str());
        if assigned != Some(mission.explorer.code.as_str()) {
            continue;
        }
        if let Some(location) = snapshot.location.as_ref()
            && !designation_in_system(location.id.as_str(), &mission.landing_star)
        {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "legacy quick-scout device {code} is at {}, outside the landing system {}",
                    location.id.as_str(),
                    mission.landing_star
                ),
            ));
        }
        recoverable.push(code);
    }

    if recoverable.is_empty() {
        return Ok(());
    }
    if let Some(capacity) = vessel.stow_capacity {
        let used = vessel.stow_used.unwrap_or(0);
        let required = i64::try_from(recoverable.len())?;
        if used + required > capacity {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "explorer vessel {} has stow capacity {capacity}, uses {used}, and needs {required} slots to recover the legacy quick-scout fleet",
                    mission.explorer.vessel
                ),
            ));
        }
    }

    info!(
        devices = ?recoverable,
        vessel = %mission.explorer.vessel,
        "recovering survey devices launched by the legacy quick-scout workflow"
    );
    for code in &recoverable {
        let snapshot = client.devices().get(code).await?.snapshot().await?;
        let already_returned = snapshot.travel.is_none()
            && snapshot
                .location
                .as_ref()
                .is_some_and(|location| location.id.as_str() == vessel_location.as_str());
        let can_recall = snapshot
            .available_commands
            .iter()
            .any(|command| command.as_str() == "recall");
        let recall_in_progress = snapshot.travel.is_some()
            || snapshot
                .status
                .as_ref()
                .is_some_and(|status| status.as_str() == "recalling")
            || (snapshot.location.is_none() && snapshot.relationships.stowed_in.is_none());
        if can_recall {
            ensure_operation(&client.devices().get(code).await?.recall().await?).await?;
        } else if !already_returned && !recall_in_progress {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "legacy quick-scout device {code} cannot be recalled to vessel {}",
                    mission.explorer.vessel
                ),
            ));
        }
    }

    wait_devices_returned_to_vessel(
        client,
        &recoverable,
        &mission.explorer.vessel,
        &vessel_location,
        config.wait_timeout,
    )
    .await?;

    for code in &recoverable {
        let snapshot = client.devices().get(code).await?.snapshot().await?;
        if snapshot
            .relationships
            .stowed_in
            .as_ref()
            .is_some_and(|vessel| vessel.id.as_str() == mission.explorer.vessel.as_str())
        {
            continue;
        }
        ensure_operation(
            &client
                .devices()
                .get(code)
                .await?
                .stow(Some(mission.explorer.vessel.clone()))
                .await?,
        )
        .await?;
    }

    wait_devices_stowed_in_vessel(
        client,
        &recoverable,
        &mission.explorer.vessel,
        config.wait_timeout,
    )
    .await?;
    info!(
        devices = ?recoverable,
        vessel = %mission.explorer.vessel,
        "legacy quick-scout survey fleet recovered and stowed"
    );
    Ok(())
}

async fn wait_devices_returned_to_vessel(
    client: &Client,
    devices: &[String],
    vessel: &str,
    vessel_location: &str,
    timeout: Duration,
) -> AnyResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut pending = Vec::new();
        for code in devices {
            let snapshot = client.devices().get(code).await?.snapshot().await?;
            let stowed = snapshot
                .relationships
                .stowed_in
                .as_ref()
                .is_some_and(|target| target.id.as_str() == vessel);
            let colocated = snapshot.travel.is_none()
                && snapshot
                    .location
                    .as_ref()
                    .is_some_and(|location| location.id.as_str() == vessel_location);
            if !stowed && !colocated {
                pending.push(code.clone());
            }
        }
        if pending.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out waiting for legacy quick-scout devices at vessel {vessel}: {pending:?}"
                ),
            ));
        }
        info!(
            vessel,
            pending = ?pending,
            "waiting for legacy quick-scout devices to return"
        );
        sleep(POLL_INTERVAL).await;
    }
}

async fn wait_devices_stowed_in_vessel(
    client: &Client,
    devices: &[String],
    vessel: &str,
    timeout: Duration,
) -> AnyResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut pending = Vec::new();
        for code in devices {
            let snapshot = client.devices().get(code).await?.snapshot().await?;
            if snapshot
                .relationships
                .stowed_in
                .as_ref()
                .is_none_or(|target| target.id.as_str() != vessel)
            {
                pending.push(code.clone());
            }
        }
        if pending.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "timed out waiting for legacy quick-scout devices to stow in {vessel}: {pending:?}"
                ),
            ));
        }
        info!(
            vessel,
            pending = ?pending,
            "waiting for legacy quick-scout devices to stow"
        );
        sleep(POLL_INTERVAL).await;
    }
}

fn designation_in_system(designation: &str, system: &str) -> bool {
    designation == system
        || designation
            .strip_prefix(system)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

async fn establish_capital(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::EstablishingCapital) {
        return Ok(());
    }
    set_phase(config, mission, MissionPhase::EstablishingCapital).await?;
    let capital_belt = required_field(&mission.capital_belt, "capital belt")?.to_owned();
    let capital_entry = required_field(&mission.capital_entry, "capital entry")?.to_owned();
    let relay = first_asset(mission, FTL_RELAY)?;
    let maintenance = mission
        .assets
        .get(MAINTENANCE_DRONE)
        .map(|codes| {
            codes
                .iter()
                .rev()
                .take(usize::try_from(mission.profile.hub_maintenance_drones).unwrap_or(0))
                .cloned()
                .collect::<Vec<_>>()
        })
        .filter(|codes| {
            codes.len()
                == usize::try_from(mission.profile.hub_maintenance_drones).unwrap_or(usize::MAX)
        })
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                "ark has too few capital maintenance drones",
            )
        })?;
    let infrastructure = vec![relay.clone()];
    let infrastructure_carrier = mission
        .carrier_loads
        .iter()
        .find(|load| load.devices.contains(&relay))
        .map(|load| load.carrier.clone())
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                "FTL Relay is not assigned to a carrier",
            )
        })?;
    let other_carriers = mission
        .carrier_loads
        .iter()
        .filter(|load| load.carrier != infrastructure_carrier)
        .map(|load| load.carrier.clone())
        .collect::<Vec<_>>();
    let freighters = mission
        .assets
        .get(CARGO_FREIGHTER)
        .cloned()
        .unwrap_or_default();
    let departures = std::iter::once((infrastructure_carrier.clone(), capital_entry.clone()))
        .chain(
            other_carriers
                .iter()
                .cloned()
                .map(|code| (code, capital_belt.clone())),
        )
        .chain(
            freighters
                .iter()
                .cloned()
                .map(|code| (code, capital_belt.clone())),
        )
        .collect::<Vec<_>>();
    ensure_operator_in_comms_for_departures(
        client,
        &mission.operator.code,
        &departures,
        config.wait_timeout,
    )
    .await?;
    info!(devices=departures.len(), capital=%capital_belt,
        "submitting capital-bound ark travel before the controlling replicant departs");
    let (infra_start, carrier_starts, freighter_starts) = tokio::join!(
        start_device_travel(client, &infrastructure_carrier, &capital_entry),
        async {
            finish_all(
                join_all(
                    other_carriers
                        .iter()
                        .map(|code| start_device_travel(client, code, &capital_belt)),
                )
                .await,
            )
        },
        async {
            finish_all(
                join_all(
                    freighters
                        .iter()
                        .map(|code| start_device_travel(client, code, &capital_belt)),
                )
                .await,
            )
        },
    );
    infra_start?;
    carrier_starts?;
    freighter_starts?;
    let (operator_start, explorer_start) = tokio::join!(
        start_replicant_travel(client, &mission.operator.code, &capital_belt),
        start_replicant_travel(client, &mission.explorer.code, &capital_belt),
    );
    operator_start?;
    explorer_start?;
    let (infra_wait, carriers_wait, freighters_wait, operator_wait, explorer_wait) =
        tokio::join!(
            wait_device_at(
                client,
                &infrastructure_carrier,
                &capital_entry,
                config.wait_timeout
            ),
            async {
                finish_all(
                    join_all(other_carriers.iter().map(|code| {
                        wait_device_at(client, code, &capital_belt, config.wait_timeout)
                    }))
                    .await,
                )
            },
            async {
                finish_all(
                    join_all(freighters.iter().map(|code| {
                        wait_device_at(client, code, &capital_belt, config.wait_timeout)
                    }))
                    .await,
                )
            },
            wait_replicant_at(
                client,
                &mission.operator.code,
                &capital_belt,
                config.wait_timeout
            ),
            wait_replicant_at(
                client,
                &mission.explorer.code,
                &capital_belt,
                config.wait_timeout
            ),
        );
    infra_wait?;
    carriers_wait?;
    freighters_wait?;
    operator_wait?;
    explorer_wait?;
    detach_devices(client, &infrastructure_carrier, &infrastructure).await?;
    configure_structure(client, &relay, false).await?;
    start_device_travel(client, &infrastructure_carrier, &capital_belt).await?;
    wait_device_at(
        client,
        &infrastructure_carrier,
        &capital_belt,
        config.wait_timeout,
    )
    .await?;
    let detach_results = join_all(mission.carrier_loads.iter().map(|load| async {
        let detail = client.raw().devices().get(&load.carrier).await?.value;
        let attached = attached_codes(&detail)
            .into_iter()
            .filter(|code| load.devices.contains(code))
            .collect::<Vec<_>>();
        detach_devices(client, &load.carrier, &attached).await
    }))
    .await;
    finish_all(detach_results)?;
    let unfold_results = join_all(
        mission
            .assets
            .get(AUTOFACTORY)
            .into_iter()
            .flatten()
            .map(|code| configure_structure(client, code, false)),
    )
    .await;
    finish_all(unfold_results)?;
    for code in &maintenance {
        set_patrol(client, code).await?;
    }
    let deposit_results = join_all(freighters.iter().map(|code| deposit_all(client, code))).await;
    finish_all(deposit_results)
}

async fn establish_initial_mine(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::InitialMining) {
        return Ok(());
    }
    set_phase(config, mission, MissionPhase::InitialMining).await?;
    execute_mining(
        client,
        &MiningExpansionRequest {
            systems: vec![required_field(&mission.capital_system, "capital system")?.to_owned()],
            replicant: mission.operator.code.clone(),
            hub: required_field(&mission.capital_belt, "capital belt")?.to_owned(),
            mission_file: mission.children.initial_mining.clone(),
            wait_timeout: config.wait_timeout,
            max_concurrency: mission.max_concurrency,
        },
    )
    .await?;
    Ok(())
}

async fn survey_region(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::SurveyingRegion) {
        return Ok(());
    }
    set_phase(config, mission, MissionPhase::SurveyingRegion).await?;
    let controller = mission
        .assets
        .get(SURVEY_CONTROLLER)
        .and_then(|codes| codes.last())
        .cloned();
    let drones = mission
        .assets
        .get(SURVEY_DRONE)
        .map(|codes| codes.iter().rev().take(3).cloned().collect::<Vec<_>>());
    let capital_system = required_field(&mission.capital_system, "capital system")?.to_owned();
    let report = execute_survey(
        client,
        &SurveyRequest {
            replicant: mission.explorer.code.clone(),
            vessel: mission.explorer.vessel.clone(),
            center: capital_system.clone(),
            radius_ly: mission.survey_radius_ly,
            system_limit: 100,
            star_detail_concurrency: 16,
            mission_file: mission.children.survey.clone(),
            controller,
            drones,
            include_explored: false,
            travel_timeout: config.wait_timeout,
            survey_timeout: config.wait_timeout,
        },
    )
    .await?;
    mission.survey_systems = unique(report.systems);
    let candidates = belt_candidates(client, &capital_system, &mission.survey_systems).await?;
    mission.selected_belts = select_dense_belts(
        &candidates,
        required_field(&mission.capital_belt, "capital belt")?,
        mission.minimum_sites,
        mission.maximum_sites,
    )?;
    save_mission(&config.mission_file, mission)
}

async fn expand_relays(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::ExpandingRelays) {
        return Ok(());
    }
    set_phase(config, mission, MissionPhase::ExpandingRelays).await?;
    let capital = required_field(&mission.capital_system, "capital system")?;
    let targets = mission
        .selected_belts
        .iter()
        .filter(|belt| belt.system != capital)
        .map(|belt| belt.system.clone())
        .collect::<Vec<_>>();
    if !targets.is_empty() {
        execute_relays(
            client,
            &RelayExpansionRequest {
                replicant: mission.operator.code.clone(),
                hub: required_field(&mission.capital_belt, "capital belt")?.to_owned(),
                targets,
                mission_file: mission.children.relays.clone(),
                max_hop_ly: 7.499,
                wait_timeout: config.wait_timeout,
            },
        )
        .await?;
    }
    Ok(())
}

async fn expand_mining(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::ExpandingMining) {
        return Ok(());
    }
    set_phase(config, mission, MissionPhase::ExpandingMining).await?;
    let capital = required_field(&mission.capital_system, "capital system")?;
    let systems = mission
        .selected_belts
        .iter()
        .filter(|belt| belt.system != capital)
        .map(|belt| belt.system.clone())
        .collect::<Vec<_>>();
    if !systems.is_empty() {
        execute_mining(
            client,
            &MiningExpansionRequest {
                systems,
                replicant: mission.operator.code.clone(),
                hub: required_field(&mission.capital_belt, "capital belt")?.to_owned(),
                mission_file: mission.children.mining.clone(),
                wait_timeout: config.wait_timeout,
                max_concurrency: mission.max_concurrency,
            },
        )
        .await?;
    }
    Ok(())
}

async fn cleanup(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    set_phase(config, mission, MissionPhase::CleaningUp).await?;
    let codes = mission
        .assets
        .values()
        .flatten()
        .cloned()
        .collect::<Vec<_>>();
    let results = join_all(
        codes
            .iter()
            .map(|code| remove_tag(client, code, &mission.mission_tag)),
    )
    .await;
    for result in results {
        if let Err(error) = result {
            warn!(%error, "could not remove one bootstrap reservation tag");
            mission.warnings.push(error.to_string());
        }
    }
    mission.phase = if mission.warnings.is_empty() {
        MissionPhase::Completed
    } else {
        MissionPhase::CompletedWithWarnings
    };
    save_mission(&config.mission_file, mission)?;
    info!(phase=?mission.phase, capital=?mission.capital_belt, sites=mission.selected_belts.len(), "regional island bootstrap complete");
    Ok(())
}

fn phase_after(current: MissionPhase, completed: MissionPhase) -> bool {
    phase_rank(current) > phase_rank(completed)
}

const fn phase_rank(phase: MissionPhase) -> u8 {
    match phase {
        MissionPhase::Planned => 0,
        MissionPhase::ManufacturingArk => 1,
        MissionPhase::LoadingArk => 2,
        MissionPhase::StagingAtSource => 3,
        MissionPhase::StagedAtSource => 4,
        MissionPhase::Outbound => 5,
        MissionPhase::QuickScouting => 6,
        MissionPhase::EstablishingCapital => 7,
        MissionPhase::InitialMining => 8,
        MissionPhase::SurveyingRegion => 9,
        MissionPhase::ExpandingRelays => 10,
        MissionPhase::ExpandingMining => 11,
        MissionPhase::CleaningUp => 12,
        MissionPhase::Completed | MissionPhase::CompletedWithWarnings => 13,
    }
}

async fn list_devices(
    client: &Client,
    location: Option<&str>,
    tag: Option<&str>,
) -> AnyResult<Vec<raw::devices::DeviceStatus>> {
    let mut cursor = None;
    let mut devices = Vec::new();
    for _ in 0..200 {
        let response = client
            .raw()
            .devices()
            .list(&raw::devices::DeviceListQuery {
                replicant_code: None,
                device_type: None,
                tag: tag.map(str::to_owned),
                untagged: None,
                location: location.map(str::to_owned),
                cursor,
                limit: Some(50),
            })
            .await?
            .value;
        devices.extend(response.devices);
        let Some(next) = response.next_cursor else {
            return Ok(devices);
        };
        cursor = Some(next);
    }
    Err(app_error(
        io::ErrorKind::InvalidData,
        "device listing exceeded 200 pages",
    ))
}

fn eligible_idle(device: &raw::devices::DeviceStatus, mission_tag: &str) -> bool {
    device.travel.is_none()
        && device.controller_device_code.is_none()
        && device.attached_to_device_code.is_none()
        && device.stowed_in_device_code.is_none()
        && device.hosting_replicant.is_none()
        && device
            .tags
            .iter()
            .all(|tag| tag == mission_tag || !reservation_tag(tag))
        && device.status.as_deref().is_none_or(|status| {
            matches!(status, "idle" | "inactive" | "deactivated" | "compacted")
        })
}

fn is_attachment_carrier(device_type: Option<&str>) -> bool {
    matches!(
        device_type,
        Some(SURGE_CARRIER | "surge_platform" | MOBILE_FLEET)
    )
}

async fn ensure_claim(
    client: &Client,
    code: &str,
    owner: Option<&str>,
    tags: &[String],
) -> AnyResult<()> {
    let handle = client.devices().get(code).await?;
    let snapshot = handle.snapshot().await?;
    let missing = tags
        .iter()
        .filter(|tag| !snapshot.tags.iter().any(|existing| existing == *tag))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        ensure_operation(
            &handle
                .configure(raw::devices::DeviceConfiguration {
                    add_tags: Some(missing),
                    remove_tags: None,
                    tags: None,
                })
                .await?,
        )
        .await?;
    }
    let snapshot = handle.refresh().await?.snapshot().await?;
    if let Some(owner) = owner
        && snapshot
            .relationships
            .assigned_replicant
            .as_ref()
            .map(|value| value.id.as_str())
            != Some(owner)
    {
        ensure_operation(&handle.change_owner(owner.to_owned()).await?).await?;
    }
    Ok(())
}

async fn remove_tag(client: &Client, code: &str, tag: &str) -> AnyResult<()> {
    let handle = client.devices().get(code).await?;
    let snapshot = handle.snapshot().await?;
    if snapshot.tags.iter().any(|existing| existing == tag) {
        ensure_operation(
            &handle
                .configure(raw::devices::DeviceConfiguration {
                    add_tags: None,
                    remove_tags: Some(vec![tag.to_owned()]),
                    tags: None,
                })
                .await?,
        )
        .await?;
    }
    Ok(())
}

async fn ensure_seed_inventory(client: &Client, mission: &BootstrapMission) -> AnyResult<()> {
    let response = client
        .raw()
        .inventory()
        .list(&raw::inventory::AccountInventoryQuery {
            location: Some(mission.source_hub.clone()),
            cursor: None,
            limit: Some(50),
        })
        .await?
        .value;
    let inventory = response
        .locations
        .into_iter()
        .find(|location| location.location.as_deref() == Some(&mission.source_hub))
        .map(|location| {
            location
                .items
                .into_iter()
                .filter_map(|item| Some((item.resource_type?, item.quantity.unwrap_or(0))))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    for resource in SEED_RESOURCES {
        let available = inventory.get(resource).copied().unwrap_or(0);
        if available < mission.seed_quantity {
            return Err(app_error(
                io::ErrorKind::Other,
                format!(
                    "source hub needs {} {resource} for seed cargo but has {available}",
                    mission.seed_quantity
                ),
            ));
        }
    }
    Ok(())
}

async fn collect_resource(
    client: &Client,
    code: &str,
    resource: &str,
    quantity: i64,
) -> AnyResult<()> {
    let detail = client.raw().devices().get(code).await?.value;
    let cargo = cargo_map(&detail);
    let have = cargo.get(resource).copied().unwrap_or(0);
    if have >= quantity {
        return Ok(());
    }
    if !cargo.is_empty() {
        deposit_all(client, code).await?;
    }
    let resources = [(resource.to_owned(), Value::from(quantity))]
        .into_iter()
        .collect();
    ensure_operation(
        &client
            .devices()
            .get(code)
            .await?
            .command(raw::devices::DeviceCommand::CollectResources { resources })
            .await?,
    )
    .await
}

async fn deposit_all(client: &Client, code: &str) -> AnyResult<()> {
    let detail = client.raw().devices().get(code).await?.value;
    if cargo_map(&detail).is_empty() {
        return Ok(());
    }
    ensure_operation(
        &client
            .devices()
            .get(code)
            .await?
            .command(raw::devices::DeviceCommand::DepositResources { resources: None })
            .await?,
    )
    .await
}

fn cargo_map(device: &raw::devices::DeviceStatus) -> BTreeMap<String, i64> {
    device
        .cargo
        .iter()
        .filter_map(|item| Some((item.resource_type.clone()?, item.quantity.unwrap_or(0))))
        .collect()
}

async fn attach_devices(client: &Client, carrier: &str, devices: &[String]) -> AnyResult<()> {
    if devices.is_empty() {
        return Ok(());
    }
    let detail = client.raw().devices().get(carrier).await?.value;
    let attached = attached_codes(&detail);
    let missing = devices
        .iter()
        .filter(|code| !attached.contains(*code))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    ensure_operation(
        &client
            .devices()
            .get(carrier)
            .await?
            .attach(targets(&missing))
            .await?,
    )
    .await
}

async fn detach_devices(client: &Client, carrier: &str, devices: &[String]) -> AnyResult<()> {
    if devices.is_empty() {
        return Ok(());
    }
    let detail = client.raw().devices().get(carrier).await?.value;
    let attached = attached_codes(&detail);
    let present = devices
        .iter()
        .filter(|code| attached.contains(*code))
        .cloned()
        .collect::<Vec<_>>();
    if present.is_empty() {
        return Ok(());
    }
    ensure_operation(
        &client
            .devices()
            .get(carrier)
            .await?
            .command(raw::devices::DeviceCommand::Detach(targets(&present)))
            .await?,
    )
    .await
}

fn targets(devices: &[String]) -> raw::devices::TargetsCommand {
    raw::devices::TargetsCommand {
        device: None,
        devices: Some(Value::Array(
            devices.iter().cloned().map(Value::String).collect(),
        )),
        target: None,
        targets: None,
    }
}

fn attached_codes(device: &raw::devices::DeviceStatus) -> BTreeSet<String> {
    device
        .attached_devices
        .iter()
        .filter_map(reference_code)
        .collect()
}

fn reference_code(value: &Map<String, Value>) -> Option<String> {
    ["device_code", "code", "device"]
        .into_iter()
        .find_map(|key| value.get(key).and_then(Value::as_str))
        .map(str::to_owned)
}

async fn ensure_operator_in_comms_for_departures(
    client: &Client,
    operator: &str,
    departures: &[(String, String)],
    timeout: Duration,
) -> AnyResult<()> {
    let mut pending = Vec::new();
    let mut origins = BTreeSet::new();
    for (code, destination) in departures {
        let detail = client.raw().devices().get(code).await?.value;
        if detail.travel.is_none() && detail.location.as_deref() == Some(destination) {
            continue;
        }
        if let Some(travel) = &detail.travel {
            let planned = travel
                .final_destination
                .as_deref()
                .or(travel.destination.as_deref());
            if planned == Some(destination) {
                continue;
            }
            return Err(app_error(
                io::ErrorKind::Other,
                format!("device {code} is travelling to {planned:?}, not {destination}"),
            ));
        }
        let origin = detail.location.ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("device {code} needs to depart for {destination} but has no location"),
            )
        })?;
        origins.insert(origin);
        pending.push(code.clone());
    }
    if pending.is_empty() {
        return Ok(());
    }
    if origins.len() != 1 {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "ark devices awaiting departure are split across locations {origins:?}: {pending:?}"
            ),
        ));
    }
    let origin = origins.into_iter().next().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            "ark departure has pending devices but no origin",
        )
    })?;
    info!(
        operator,
        origin = %origin,
        devices = pending.len(),
        "positioning the controlling replicant before submitting ark travel"
    );
    wait_replicant_at(client, operator, &origin, timeout).await
}

async fn start_device_travel(client: &Client, code: &str, destination: &str) -> AnyResult<()> {
    let detail = client.raw().devices().get(code).await?.value;
    if detail.travel.is_none() && detail.location.as_deref() == Some(destination) {
        return Ok(());
    }
    if let Some(travel) = &detail.travel {
        let planned = travel
            .final_destination
            .as_deref()
            .or(travel.destination.as_deref());
        if planned == Some(destination) {
            return Ok(());
        }
        return Err(app_error(
            io::ErrorKind::Other,
            format!("device {code} is travelling to {planned:?}, not {destination}"),
        ));
    }
    ensure_operation(
        &client
            .devices()
            .get(code)
            .await?
            .command(raw::devices::DeviceCommand::Travel {
                destination: destination.into(),
                dry_run: None,
                via: None,
            })
            .await?,
    )
    .await
}

async fn wait_device_at(
    client: &Client,
    code: &str,
    destination: &str,
    timeout: Duration,
) -> AnyResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let detail = client.raw().devices().get(code).await?.value;
        if detail.travel.is_none() && detail.location.as_deref() == Some(destination) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for device {code} at {destination}"),
            ));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn start_replicant_travel(client: &Client, code: &str, destination: &str) -> AnyResult<()> {
    let handle = client.replicants().get_owned(code).await?;
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
        if planned == Some(destination) {
            return Ok(());
        }
        return Err(app_error(
            io::ErrorKind::Other,
            format!("replicant {code} is travelling to {planned:?}, not {destination}"),
        ));
    }
    ensure_operation(&handle.travel().to(destination).depart().await?).await
}

async fn wait_replicant_at(
    client: &Client,
    code: &str,
    destination: &str,
    timeout: Duration,
) -> AnyResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let snapshot = client
            .replicants()
            .get_owned(code)
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
        if snapshot.travel.is_none() {
            start_replicant_travel(client, code, destination).await?;
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for replicant {code} at {destination}"),
            ));
        }
        sleep(POLL_INTERVAL).await;
    }
}

async fn configure_structure(client: &Client, code: &str, set_entry: bool) -> AnyResult<()> {
    let mut detail = client.raw().devices().get(code).await?.value;
    if detail
        .available_commands
        .iter()
        .any(|command| command == "unfurl")
    {
        ensure_operation(&client.devices().get(code).await?.unfurl().await?).await?;
        detail = client.raw().devices().get(code).await?.value;
    }
    if detail.status.as_deref() != Some("active")
        && detail
            .available_commands
            .iter()
            .any(|command| command == "activate")
    {
        ensure_operation(&client.devices().get(code).await?.activate().await?).await?;
        detail = client.raw().devices().get(code).await?.value;
    }
    if set_entry
        && detail
            .available_commands
            .iter()
            .any(|command| command == "set_entry_point")
    {
        ensure_operation(
            &client
                .devices()
                .get(code)
                .await?
                .command(raw::devices::DeviceCommand::SetEntryPoint)
                .await?,
        )
        .await?;
    }
    Ok(())
}

async fn set_patrol(client: &Client, code: &str) -> AnyResult<()> {
    let detail = client.raw().devices().get(code).await?.value;
    let current = detail
        .ami_directive
        .as_ref()
        .and_then(|value| value.get("directive"))
        .and_then(Value::as_str);
    if current != Some("patrol") {
        ensure_operation(
            &client
                .devices()
                .get(code)
                .await?
                .command(raw::devices::DeviceCommand::SetDirective {
                    directive: "patrol".into(),
                    configuration: None,
                    notify: None,
                })
                .await?,
        )
        .await?;
    }
    Ok(())
}

async fn belt_candidates(
    client: &Client,
    center: &str,
    systems: &[String],
) -> AnyResult<Vec<BeltCandidate>> {
    let catalogue = client.galaxy().catalogue();
    let center_position = catalogue
        .iter()
        .find(|star| star.key.id.as_str() == center)
        .and_then(|star| star.position)
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("{center} has no catalogue position"),
            )
        })?;
    let positions = catalogue
        .iter()
        .filter_map(|star| Some((star.key.id.as_str().to_owned(), star.position?)))
        .collect::<BTreeMap<_, _>>();
    let mut result = Vec::new();
    for system in systems {
        let location = client.locations().get(system).await?;
        let position = positions.get(system).copied().ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("{system} has no catalogue position"),
            )
        })?;
        let distance = distance(center_position, position);
        result.extend(
            belts_from_location(&location)
                .into_iter()
                .map(|(designation, density)| BeltCandidate {
                    system: system.clone(),
                    designation,
                    density,
                    distance_from_capital_ly: distance,
                }),
        );
    }
    Ok(result)
}

fn belts_from_location(location: &Location) -> Vec<(String, String)> {
    let Some(asteroid_belt) = location.unknown.get("asteroid_belt") else {
        return Vec::new();
    };
    asteroid_belt
        .get("belts")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(asteroid_belt))
        .iter()
        .filter_map(|value| {
            let object = value.as_object()?;
            Some((
                object.get("designation")?.as_str()?.to_owned(),
                object
                    .get("density")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
            ))
        })
        .collect()
}

fn density_rank(value: &str) -> u8 {
    match value.to_ascii_lowercase().as_str() {
        "dense" => 3,
        "moderate" => 2,
        "sparse" => 1,
        _ => 0,
    }
}

fn distance(left: GalacticPosition, right: GalacticPosition) -> f64 {
    ((left.x - right.x).powi(2) + (left.y - right.y).powi(2) + (left.z - right.z).powi(2)).sqrt()
}

async fn ensure_operation(operation: &Operation) -> AnyResult<()> {
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

fn finish_all(results: Vec<AnyResult<()>>) -> AnyResult<()> {
    for result in results {
        result?;
    }
    Ok(())
}

fn first_asset(mission: &BootstrapMission, device_type: &str) -> AnyResult<String> {
    mission
        .assets
        .get(device_type)
        .and_then(|codes| codes.first())
        .cloned()
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("ark has no {device_type}"),
            )
        })
}

fn required_field<'a>(value: &'a Option<String>, name: &str) -> AnyResult<&'a str> {
    value
        .as_deref()
        .ok_or_else(|| app_error(io::ErrorKind::InvalidData, format!("mission has no {name}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn phase_order_is_monotonic() {
        assert!(phase_after(
            MissionPhase::SurveyingRegion,
            MissionPhase::InitialMining
        ));
        assert!(phase_after(
            MissionPhase::StagedAtSource,
            MissionPhase::LoadingArk
        ));
        assert!(!phase_after(
            MissionPhase::StagedAtSource,
            MissionPhase::Outbound
        ));
        assert!(!phase_after(MissionPhase::Outbound, MissionPhase::Outbound));
    }
    #[test]
    fn density_order_prefers_dense() {
        assert!(density_rank("dense") > density_rank("moderate"));
    }

    #[test]
    fn quick_scout_route_starts_at_center_and_uses_nearest_neighbors() {
        let route = nearest_neighbor_system_route(
            "CENTER".into(),
            GalacticPosition {
                x: 0.0,
                y: 0.0,
                z: 0.0,
            },
            vec![
                (
                    "FAR".into(),
                    GalacticPosition {
                        x: 3.0,
                        y: 0.0,
                        z: 0.0,
                    },
                ),
                (
                    "NEAR".into(),
                    GalacticPosition {
                        x: 1.0,
                        y: 0.0,
                        z: 0.0,
                    },
                ),
                (
                    "MIDDLE".into(),
                    GalacticPosition {
                        x: 2.0,
                        y: 0.0,
                        z: 0.0,
                    },
                ),
            ],
            4,
        );

        assert_eq!(
            route,
            vec![
                "CENTER".to_owned(),
                "NEAR".to_owned(),
                "MIDDLE".to_owned(),
                "FAR".to_owned(),
            ]
        );
    }

    #[test]
    fn quick_scout_system_matching_accepts_child_locations() {
        assert!(designation_in_system("RHWYRHYR", "RHWYRHYR"));
        assert!(designation_in_system("RHWYRHYR-5-L4", "RHWYRHYR"));
        assert!(!designation_in_system("RHWYRHYRA-5-L4", "RHWYRHYR"));
    }
    #[test]
    fn mobile_fleets_are_attachment_carriers() {
        assert!(is_attachment_carrier(Some("mobile_fleet")));
        assert!(is_attachment_carrier(Some("surge_carrier")));
        assert!(!is_attachment_carrier(Some("cargo_freighter")));
    }
    #[test]
    fn interrupted_print_reconciliation_does_not_duplicate_accounted_work() {
        let targets = [("autofactory".into(), 1), ("survey_drone".into(), 4)]
            .into_iter()
            .collect();
        let recorded = [("survey_drone".into(), 1)].into_iter().collect();
        let completed = [("survey_drone".into(), 1)].into_iter().collect();
        let pending = [("survey_drone".into(), 2)].into_iter().collect();
        let remaining = remaining_print_requirements(&targets, &recorded, &completed, &pending);
        assert_eq!(remaining, [("autofactory".into(), 1)].into_iter().collect());
    }

    #[test]
    fn staged_manifest_upgrade_only_queues_the_new_reserve() {
        let targets = [
            ("ftl_relay".into(), 19),
            ("ftl_beacon".into(), 9),
            ("surge_carrier".into(), 12),
        ]
        .into_iter()
        .collect();
        let recorded = [("ftl_relay".into(), 1), ("surge_carrier".into(), 9)]
            .into_iter()
            .collect();

        let remaining =
            remaining_print_requirements(&targets, &recorded, &BTreeMap::new(), &BTreeMap::new());

        assert_eq!(remaining.get("ftl_relay"), Some(&18));
        assert_eq!(remaining.get("ftl_beacon"), Some(&9));
        assert_eq!(remaining.get("surge_carrier"), Some(&3));
    }

    #[test]
    fn old_missions_drop_system_hub_printing() {
        let mut print = PrintState {
            targets: [("system_hub".into(), 1), ("autofactory".into(), 6)]
                .into_iter()
                .collect(),
            requirements: [("system_hub".into(), 1), ("autofactory".into(), 6)]
                .into_iter()
                .collect(),
            ..PrintState::default()
        };

        assert!(remove_system_hub_request(&mut print));
        assert_eq!(
            print.targets,
            [("autofactory".into(), 6)].into_iter().collect()
        );
        assert_eq!(
            print.requirements,
            [("autofactory".into(), 6)].into_iter().collect()
        );
        assert!(!remove_system_hub_request(&mut print));
    }
}
