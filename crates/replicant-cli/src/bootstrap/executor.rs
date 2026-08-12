use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::{Duration, Instant},
};

use crate::mining::{MiningExpansionRequest, execute_expansion as execute_mining};
use crate::relay::{RelayExpansionRequest, execute_expansion as execute_relays};
use crate::survey::{SurveyRequest, execute_survey};
use futures::future::join_all;
use replicant_bootstrap_planner::{
    AUTOFACTORY, BeltCandidate, FTL_BEACON, FTL_RELAY, SEED_RESOURCES, SURGE_CARRIER,
    ark_device_requirements, attachment_slots, required_role_carriers, select_dense_belts,
};
use replicant_client::{
    Client, Device, Operation, OperationStatus, Replicant, SyncDomain,
    domain::{GalacticPosition, Location},
    raw,
};
use replicant_mining_planner::{
    CARGO_FREIGHTER, MAINTENANCE_DRONE, MINING_CONTROLLER, MINING_DRONE, SURVEY_CONTROLLER,
    SURVEY_DRONE,
};
use replicant_printing::{
    PrintRequest,
    managed::{QueueOptions, queue_prints},
};
use serde_json::{Map, Value};
use tokio::time::timeout;
use tracing::{info, warn};

use super::{
    AnyResult, Config, app_error,
    model::{
        BootstrapMission, CarrierLoad, MissionPhase, PrintState, ReplicantIdentity, SeedFreighter,
    },
    reservation_tag, save_mission, unique,
};

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const AUTHORITATIVE_POLL_INTERVAL: Duration = Duration::from_secs(60);
const QUICK_SCOUT_SYSTEM_LIMIT: usize = 12;

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
    let entry_point = star.entry_point.as_ref().map(|entry| entry.id.as_str());
    let destination = preferred_star_destination(star.key.id.as_str(), entry_point);
    if entry_point.is_none() {
        info!(
            star = %star.key.id.as_str(),
            destination = %destination,
            "landing star has no catalogue entry point; using the star designation so the server can select the default arrival zone"
        );
    }
    Ok((
        destination,
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
    client.ready().await?;
    let sync = client.sync().domain(SyncDomain::Replicants).await?;
    info!(readiness=?sync.readiness, phase=?mission.phase, "refreshed owned replicants for bootstrap execution");
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

pub async fn deliver(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::Outbound) {
        info!(phase=?mission.phase, "bootstrap ark has already reached the landing star");
        return Ok(());
    }
    client.ready().await?;
    let sync = client.sync().domain(SyncDomain::Replicants).await?;
    info!(readiness=?sync.readiness, phase=?mission.phase, "refreshed owned replicants for landing delivery");
    client.galaxy().refresh_catalogue().await?;
    ensure_source_entry(client, config, mission).await?;
    resolve_operator(client, config, mission).await?;
    claim_staged_ark_for_operator(client, mission).await?;

    manufacture_ark(client, config, mission).await?;
    load_ark(client, config, mission).await?;
    if matches!(
        mission.phase,
        MissionPhase::StagingAtSource | MissionPhase::StagedAtSource
    ) {
        stage_at_source_entry(client, config, mission).await?;
    }
    dispatch_devices_to_landing(client, config, mission).await?;
    info!(
        landing_star=%mission.landing_star,
        landing_entry=%mission.landing_entry,
        "bootstrap ark delivery complete; regional deployment was not started"
    );
    Ok(())
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
    client.ready().await?;
    info!(readiness=?client.readiness(), phase=?mission.phase, "managed essential startup ready for source staging");
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
    resolve_operator(client, config, mission).await?;
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

async fn resolve_operator(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    let operator_query = mission.operator.query().to_owned();
    mission.operator = resolve_replicant(client, &operator_query).await?
        .ok_or_else(|| app_error(io::ErrorKind::NotFound,
            format!("planned operator {operator_query:?} does not exist with a hosted vessel yet; use `stage` to prepare the ark without it")))?;
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
    // Bootstrap previously fetched the unlocked blueprint catalogue twice:
    // once through replicant-printing and once again just for carrier capacity.
    // Keep the raw catalogue from the single authoritative request and derive
    // both carrier capacity and modular-print classification from it.
    let blueprint_catalogue = client.raw().blueprints().list().await?.value.blueprints;
    let carrier_capacity = blueprint_catalogue
        .iter()
        .find(|item| item.device_type.as_deref() == Some(SURGE_CARRIER))
        .and_then(|item| item.attach_capacity)
        .unwrap_or(0);
    let modular_blueprints = blueprint_catalogue
        .iter()
        .filter_map(|blueprint| {
            let device_type = blueprint.device_type.as_ref()?;
            let modular = blueprint
                .features
                .as_ref()
                .is_some_and(|features| features.iter().any(|feature| feature == "modular"))
                || matches!(
                    device_type.as_str(),
                    "autofactory" | "system_hub" | "exotic_matter_injector"
                );
            modular.then_some(device_type.clone())
        })
        .collect::<BTreeSet<_>>();
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

    let required_carriers = required_role_carriers(&mission.profile, &desired, carrier_capacity)?;
    let current_carriers = i64::try_from(mission.assets.get(SURGE_CARRIER).map_or(0, Vec::len))?;
    let additional_reuse_needed = usize::try_from(required_carriers.saturating_sub(current_carriers))?;
    let mut candidates = devices
        .iter()
        .filter(|device| eligible_idle(device, &mission.mission_tag))
        .filter(|device| device.device_type.as_deref() == Some(SURGE_CARRIER))
        .filter(|device| device.attached_devices.is_empty())
        .filter(|device| device.attach_capacity.unwrap_or(carrier_capacity) >= carrier_capacity)
        .filter_map(|device| device.device_code.clone())
        .filter(|code| !used.contains(code))
        .collect::<Vec<_>>();
    candidates.sort();

    let carriers = mission.assets.entry(SURGE_CARRIER.into()).or_default();
    let reused_before = mission.reused_carrier_target.max(0);
    let mut newly_reused = 0_i64;
    for code in candidates.into_iter().take(additional_reuse_needed) {
        used.insert(code.clone());
        carriers.push(code);
        newly_reused = newly_reused.saturating_add(1);
    }
    mission.reused_carrier_target = reused_before
        .saturating_add(newly_reused)
        .min(i64::try_from(carriers.len())?);
    mission.carrier_target = required_carriers;
    let printing = required_carriers.saturating_sub(i64::try_from(carriers.len())?);
    info!(
        reused = mission.reused_carrier_target,
        printing,
        target = mission.carrier_target,
        mining_carriers = mission.profile.mining_setups,
        "derived attachment-carrier fleet from role-based ark payload"
    );

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
            .partition(|request| modular_blueprints.contains(&request.device_type));
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
    let hub_devices = list_devices(client, Some(&mission.source_hub), None).await?;
    let tagged = hub_devices
        .iter()
        .filter(|device| device.tags.iter().any(|tag| tag == &mission.mission_tag))
        .collect::<Vec<_>>();
    let recorded_codes = mission
        .assets
        .values()
        .flatten()
        .cloned()
        .collect::<BTreeSet<_>>();
    let mut completed = BTreeMap::<String, i64>::new();
    for device in tagged {
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
    let pending = pending_tagged_prints_from_devices(&hub_devices, &mission.mission_tag);
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

fn pending_tagged_prints_from_devices(
    devices: &[raw::devices::DeviceStatus],
    mission_tag: &str,
) -> BTreeMap<String, i64> {
    let mut pending = BTreeMap::<String, i64>::new();
    for factory in devices
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
    pending
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
    let mut watch = client.events().watch().await?;
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
            .filter(|device| device.device_type.as_deref() == Some(SURGE_CARRIER))
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

        // Printing can take hours. Use the account event stream as the normal
        // wake-up path and only re-list the hub on a relevant completion or a
        // sparse 60-second authoritative fallback. The old five-second list
        // loop multiplied paginated /devices calls for the entire print window.
        let poll_deadline = (Instant::now() + AUTHORITATIVE_POLL_INTERVAL).min(deadline);
        loop {
            let remaining = poll_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(remaining, watch.next()).await {
                Ok(Ok(event)) if event.name.as_str() == "print.completed" => {
                    if event
                        .payload
                        .get("tags")
                        .and_then(serde_json::Value::as_array)
                        .is_some_and(|tags| {
                            tags.iter().any(|tag| {
                                tag.as_str() == Some(mission.mission_tag.as_str())
                            })
                        })
                    {
                        break;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(error)) => {
                    warn!(error = %error, "event watcher gap; refreshing bootstrap print state");
                    break;
                }
                Err(_) => break,
            }
        }
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
        .filter(|device| device.device_type.as_deref() == Some(SURGE_CARRIER))
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
    let hub_devices = list_devices(client, Some(&mission.source_hub), None).await?;
    let cargo_by_device = hub_devices
        .iter()
        .filter_map(|device| Some((device.device_code.clone()?, cargo_map(device))))
        .collect::<BTreeMap<_, _>>();
    let collect_results = join_all(mission.seed_freighters.iter().map(|seed| {
        let cargo = cargo_by_device.get(&seed.code).cloned().unwrap_or_default();
        collect_resource_with_cargo(
            client,
            &seed.code,
            &seed.resource,
            seed.quantity,
            cargo,
        )
    }))
    .await;
    finish_all(collect_results)?;

    append_missing_carrier_loads(client, config, mission).await?;
    attach_carrier_loads_at(client, &mission.source_hub, &mission.carrier_loads).await?;
    queue_borrowed_carrier_replacements(client, config, mission).await
}

fn carrier_replacement_tag(mission_tag: &str) -> String {
    let suffix = mission_tag.strip_prefix("boot-m:").unwrap_or(mission_tag);
    format!("boot-repl:{suffix}")
}

async fn queue_borrowed_carrier_replacements(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    let target = mission.reused_carrier_target.max(0);
    if target == 0 {
        return Ok(());
    }
    let replacement_tag = carrier_replacement_tag(&mission.mission_tag);
    let hub_devices = list_devices(client, Some(&mission.source_hub), None).await?;
    let completed = hub_devices
        .iter()
        .filter(|device| device.device_type.as_deref() == Some(SURGE_CARRIER))
        .filter(|device| device.tags.iter().any(|tag| tag == &replacement_tag))
        .count();
    let pending = pending_tagged_prints_from_devices(&hub_devices, &replacement_tag)
        .get(SURGE_CARRIER)
        .copied()
        .unwrap_or(0);
    let accounted = i64::try_from(completed)?.saturating_add(pending);
    let remaining = target.saturating_sub(accounted);

    mission.carrier_replacement_print.targets =
        [(SURGE_CARRIER.to_owned(), target)].into_iter().collect();
    mission.carrier_replacement_print.requirements = if remaining > 0 {
        [(SURGE_CARRIER.to_owned(), remaining)].into_iter().collect()
    } else {
        BTreeMap::new()
    };
    mission.carrier_replacement_print.submission_started = false;
    mission.carrier_replacement_print.queued = remaining == 0;
    save_mission(&config.mission_file, mission)?;

    if remaining == 0 {
        info!(target, completed, pending, "source-hub carrier replacements already accounted for");
        return Ok(());
    }

    mission.carrier_replacement_print.submission_started = true;
    save_mission(&config.mission_file, mission)?;
    let mut options = QueueOptions::at(&mission.source_hub);
    options.tags = vec![replacement_tag];
    options.wait_timeout = config.wait_timeout;
    let request = [PrintRequest::new(SURGE_CARRIER, remaining)];
    match queue_prints(client, &request, &options).await {
        Ok(report) => {
            mission
                .carrier_replacement_print
                .operation_ids
                .extend(report.operation_ids);
            mission.carrier_replacement_print.submission_started = false;
            mission.carrier_replacement_print.queued = true;
            mission.carrier_replacement_print.requirements.clear();
            save_mission(&config.mission_file, mission)?;
            info!(
                borrowed = target,
                queued = remaining,
                "queued non-blocking Surge Carrier replacements for the source hub"
            );
            Ok(())
        }
        Err(error) => {
            // Persist the uncertain submission marker. A rerun reconciles the
            // dedicated replacement tag before attempting any additional work.
            save_mission(&config.mission_file, mission)?;
            Err(error.into())
        }
    }
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
    let unassigned = mission
        .assets
        .iter()
        .filter(|(device_type, _)| !matches!(device_type.as_str(), CARGO_FREIGHTER | SURGE_CARRIER))
        .flat_map(|(_, codes)| codes.iter().cloned())
        .filter(|code| !assigned.contains(code))
        .collect::<BTreeSet<_>>();
    if unassigned.is_empty() {
        return Ok(());
    }

    // One authoritative list replaces the previous N+1 GET /devices/{carrier}
    // loop while preserving the live attach capacity for every selected carrier.
    let hub_devices = list_devices(client, Some(&mission.source_hub), None).await?;
    let carrier_capacities = hub_devices
        .iter()
        .filter_map(|device| {
            Some((
                device.device_code.clone()?,
                device.attach_capacity.unwrap_or(0).max(0),
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let used_carriers = mission
        .carrier_loads
        .iter()
        .map(|load| load.carrier.clone())
        .collect::<BTreeSet<_>>();
    let mut carriers = mission
        .assets
        .get(SURGE_CARRIER)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|carrier| !used_carriers.contains(carrier))
        .map(|carrier| {
            let capacity = carrier_capacities.get(&carrier).copied().unwrap_or(0);
            (carrier, capacity)
        })
        .collect::<Vec<_>>();
    carriers.sort_by(|left, right| left.1.cmp(&right.1).then_with(|| left.0.cmp(&right.0)));

    if mission.carrier_loads.is_empty() {
        let role_capacity = carriers.iter().map(|(_, capacity)| *capacity).max().unwrap_or(9);
        let (reserved, general) = fresh_role_payloads(
            &mission.profile,
            &mission.assets,
            &unassigned,
            role_capacity,
        )?;
        for (role, devices) in reserved {
            let required = i64::try_from(devices.len())?;
            let Some(index) = carriers
                .iter()
                .position(|(_, capacity)| *capacity >= required)
            else {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    format!("no unused Surge Carrier can hold role {role} ({required} devices)"),
                ));
            };
            let (carrier, capacity) = carriers.remove(index);
            mission.carrier_loads.push(CarrierLoad {
                carrier,
                capacity,
                role: Some(role),
                devices,
            });
        }
        append_general_carrier_loads(mission, carriers, general)?;
    } else {
        // Legacy or interrupted missions may already have persisted attachment
        // loads. Preserve those exact assignments and pack only the remainder.
        append_general_carrier_loads(
            mission,
            carriers,
            unassigned.into_iter().collect::<Vec<_>>(),
        )?;
    }

    save_mission(&config.mission_file, mission)
}

fn fresh_role_payloads(
    profile: &replicant_bootstrap_planner::BootstrapProfile,
    assets: &BTreeMap<String, Vec<String>>,
    unassigned: &BTreeSet<String>,
    carrier_capacity: i64,
) -> AnyResult<(Vec<(String, Vec<String>)>, Vec<String>)> {
    let mut pools = assets
        .iter()
        .filter(|(device_type, _)| !matches!(device_type.as_str(), CARGO_FREIGHTER | SURGE_CARRIER))
        .map(|(device_type, codes)| {
            let mut codes = codes
                .iter()
                .filter(|code| unassigned.contains(*code))
                .cloned()
                .collect::<Vec<_>>();
            codes.sort();
            (device_type.clone(), codes)
        })
        .collect::<BTreeMap<_, _>>();
    let mut reserved = Vec::<(String, Vec<String>)>::new();
    let mut reserved_general = Vec::<String>::new();

    // Preserve the exact tail assets used later by the explorer/capital roles.
    // Mining carriers must not consume those devices just because their type
    // also appears in each nine-device mining setup.
    reserve_tail_assets(
        &mut pools,
        assets,
        SURVEY_CONTROLLER,
        1,
        &mut reserved_general,
    );
    reserve_tail_assets(
        &mut pools,
        assets,
        SURVEY_DRONE,
        usize::try_from(profile.exploration_survey_drones.max(0))?,
        &mut reserved_general,
    );
    reserve_tail_assets(
        &mut pools,
        assets,
        MAINTENANCE_DRONE,
        usize::try_from(profile.hub_maintenance_drones.max(0))?,
        &mut reserved_general,
    );

    for index in 0..usize::try_from(profile.mining_setups.max(0))? {
        let mut devices = Vec::with_capacity(9);
        devices.extend(take_role_devices(&mut pools, MINING_CONTROLLER, 1)?);
        devices.extend(take_role_devices(&mut pools, MINING_DRONE, 4)?);
        devices.extend(take_role_devices(&mut pools, SURVEY_CONTROLLER, 1)?);
        devices.extend(take_role_devices(&mut pools, SURVEY_DRONE, 2)?);
        devices.extend(take_role_devices(&mut pools, MAINTENANCE_DRONE, 1)?);
        reserved.push((format!("mining-{}", index + 1), devices));
    }

    // Root relays remain in the general ark payload. Only the expansion reserve
    // is kept in dedicated relay carriers.
    let mut relays = pools.remove(FTL_RELAY).unwrap_or_default();
    relays.sort();
    let root_count = usize::try_from(profile.root_relays.max(0))?.min(relays.len());
    let expansion_relays = relays.split_off(root_count);
    pools.insert(FTL_RELAY.to_owned(), relays);

    let role_capacity = usize::try_from(carrier_capacity.max(1))?;
    for (index, chunk) in expansion_relays.chunks(role_capacity).enumerate() {
        if !chunk.is_empty() {
            reserved.push((format!("relays-{}", index + 1), chunk.to_vec()));
        }
    }
    let beacons = pools.remove(FTL_BEACON).unwrap_or_default();
    for (index, chunk) in beacons.chunks(role_capacity).enumerate() {
        if !chunk.is_empty() {
            reserved.push((format!("beacons-{}", index + 1), chunk.to_vec()));
        }
    }

    let mut general = reserved_general;
    general.extend(pools.into_values().flatten());
    general.sort();
    Ok((reserved, general))
}

fn reserve_tail_assets(
    pools: &mut BTreeMap<String, Vec<String>>,
    assets: &BTreeMap<String, Vec<String>>,
    device_type: &str,
    count: usize,
    reserved_general: &mut Vec<String>,
) {
    let reserved = assets
        .get(device_type)
        .into_iter()
        .flatten()
        .rev()
        .take(count)
        .cloned()
        .collect::<BTreeSet<_>>();
    if let Some(pool) = pools.get_mut(device_type) {
        pool.retain(|code| !reserved.contains(code));
    }
    reserved_general.extend(reserved);
}

fn take_role_devices(
    pools: &mut BTreeMap<String, Vec<String>>,
    device_type: &str,
    count: usize,
) -> AnyResult<Vec<String>> {
    let pool = pools.entry(device_type.to_owned()).or_default();
    if pool.len() < count {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "cannot build complete mining carrier: need {count} {device_type}, have {}",
                pool.len()
            ),
        ));
    }
    Ok(pool.drain(..count).collect())
}

fn append_general_carrier_loads(
    mission: &mut BootstrapMission,
    carriers: Vec<(String, i64)>,
    mut payload: Vec<String>,
) -> AnyResult<()> {
    payload.sort();
    let mut cursor = 0_usize;
    let existing_general = mission
        .carrier_loads
        .iter()
        .filter(|load| load.role.as_deref().is_some_and(|role| role.starts_with("general-")))
        .count();
    for (offset, (carrier, capacity)) in carriers.into_iter().enumerate() {
        let take = usize::try_from(capacity.max(0))?.min(payload.len().saturating_sub(cursor));
        if take == 0 {
            continue;
        }
        let devices = payload[cursor..cursor + take].to_vec();
        cursor += take;
        mission.carrier_loads.push(CarrierLoad {
            carrier,
            capacity,
            role: Some(format!("general-{}", existing_general + offset + 1)),
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
                "Surge Carrier capacity covers {cursor} of {} remaining payload devices",
                payload.len()
            ),
        ));
    }
    Ok(())
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
    let devices = ark_transport_devices(mission);
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
    wait_devices_at_matching(
        client,
        &devices,
        &mission.source_entry,
        None,
        config.wait_timeout,
    )
    .await?;
    set_phase(config, mission, MissionPhase::StagedAtSource).await
}

async fn dispatch_devices_to_landing(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::Outbound) {
        return Ok(());
    }
    set_phase(config, mission, MissionPhase::Outbound).await?;
    let devices = ark_transport_devices(mission);
    let departures = devices
        .iter()
        .map(|code| (code.clone(), mission.landing_entry.clone()))
        .collect::<Vec<_>>();
    let landing_system_fallback = landing_system_fallback(mission);
    ensure_operator_in_comms_for_departures(
        client,
        &mission.operator.code,
        &departures,
        landing_system_fallback,
        config.wait_timeout,
    )
    .await?;
    info!(devices=devices.len(), destination=%mission.landing_entry,
        "submitting ark device travel to the landing star without regional deployment");
    finish_all(
        join_all(devices.iter().map(|code| {
            start_device_travel_matching(
                client,
                code,
                &mission.landing_entry,
                landing_system_fallback,
            )
        }))
        .await,
    )?;
    wait_devices_at_matching(
        client,
        &devices,
        &mission.landing_entry,
        landing_system_fallback,
        config.wait_timeout,
    )
    .await
}

fn ark_transport_devices(mission: &BootstrapMission) -> Vec<String> {
    mission
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
        .collect()
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
    let devices = ark_transport_devices(mission);
    let departures = devices
        .iter()
        .map(|code| (code.clone(), mission.landing_entry.clone()))
        .collect::<Vec<_>>();
    let landing_system_fallback = landing_system_fallback(mission);
    ensure_operator_in_comms_for_departures(
        client,
        &mission.operator.code,
        &departures,
        landing_system_fallback,
        config.wait_timeout,
    )
    .await?;
    info!(devices=devices.len(), destination=%mission.landing_entry,
        "submitting complete ark travel before the controlling replicant departs");
    finish_all(
        join_all(devices.iter().map(|code| {
            start_device_travel_matching(
                client,
                code,
                &mission.landing_entry,
                landing_system_fallback,
            )
        }))
        .await,
    )?;
    let (operator_start, explorer_start) = tokio::join!(
        start_replicant_travel_matching(
            client,
            &mission.operator.code,
            &mission.landing_entry,
            landing_system_fallback,
        ),
        start_replicant_travel_matching(
            client,
            &mission.explorer.code,
            &mission.landing_entry,
            landing_system_fallback,
        ),
    );
    operator_start?;
    explorer_start?;
    let (device_waits, operator_wait, explorer_wait) = tokio::join!(
        wait_devices_at_matching(
            client,
            &devices,
            &mission.landing_entry,
            landing_system_fallback,
            config.wait_timeout,
        ),
        wait_replicant_at_matching(
            client,
            &mission.operator.code,
            &mission.landing_entry,
            landing_system_fallback,
            config.wait_timeout
        ),
        wait_replicant_at_matching(
            client,
            &mission.explorer.code,
            &mission.landing_entry,
            landing_system_fallback,
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
    start_replicant_travel_matching(client, code, destination, Some(destination)).await?;
    wait_replicant_at_matching(client, code, destination, Some(destination), timeout).await
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
    wait_timeout: Duration,
) -> AnyResult<()> {
    let deadline = Instant::now() + wait_timeout;
    let mut watch = client.events().watch().await?;
    loop {
        let mut pending = BTreeSet::new();
        for code in devices {
            let handle = match client.devices().cached(code) {
                Some(handle) => handle,
                None => client.devices().get(code).await?,
            };
            let snapshot = handle.snapshot().await?;
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
                pending.insert(code.clone());
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
        let wake = wait_for_pending_device_event(&mut watch, deadline, &pending).await?;
        if matches!(wake, TravelWake::Poll | TravelWake::Gap) {
            refresh_pending_devices(client, &pending).await?;
        }
    }
}

async fn wait_devices_stowed_in_vessel(
    client: &Client,
    devices: &[String],
    vessel: &str,
    wait_timeout: Duration,
) -> AnyResult<()> {
    let deadline = Instant::now() + wait_timeout;
    let mut watch = client.events().watch().await?;
    loop {
        let mut pending = BTreeSet::new();
        for code in devices {
            let handle = match client.devices().cached(code) {
                Some(handle) => handle,
                None => client.devices().get(code).await?,
            };
            let snapshot = handle.snapshot().await?;
            if snapshot
                .relationships
                .stowed_in
                .as_ref()
                .is_none_or(|target| target.id.as_str() != vessel)
            {
                pending.insert(code.clone());
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
        let wake = wait_for_pending_device_event(&mut watch, deadline, &pending).await?;
        if matches!(wake, TravelWake::Poll | TravelWake::Gap) {
            refresh_pending_devices(client, &pending).await?;
        }
    }
}

async fn wait_for_pending_device_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    pending: &BTreeSet<String>,
) -> AnyResult<TravelWake> {
    let poll_deadline = (Instant::now() + AUTHORITATIVE_POLL_INTERVAL).min(deadline);
    loop {
        let remaining = poll_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(TravelWake::Poll);
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event))
                if event
                    .device
                    .as_ref()
                    .is_some_and(|device| pending.contains(device.id.as_str())) =>
            {
                return Ok(TravelWake::Event);
            }
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(TravelWake::Poll),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; refreshing bootstrap device state");
                return Ok(TravelWake::Gap);
            }
        }
    }
}

async fn refresh_pending_devices(client: &Client, pending: &BTreeSet<String>) -> AnyResult<()> {
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
    Ok(())
}

fn preferred_star_destination(system: &str, entry_point: Option<&str>) -> String {
    entry_point.unwrap_or(system).to_owned()
}

fn designation_in_system(designation: &str, system: &str) -> bool {
    designation == system
        || designation
            .strip_prefix(system)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

fn landing_system_fallback(mission: &BootstrapMission) -> Option<&str> {
    mission
        .landing_entry
        .eq_ignore_ascii_case(&mission.landing_star)
        .then_some(mission.landing_star.as_str())
}

fn destination_matches(actual: &str, requested: &str, destination_system: Option<&str>) -> bool {
    actual == requested
        || destination_system.is_some_and(|system| designation_in_system(actual, system))
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
        None,
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
    let mut capital_devices = other_carriers.clone();
    capital_devices.extend(freighters.iter().cloned());
    let (infra_wait, capital_wait, operator_wait, explorer_wait) = tokio::join!(
        wait_device_at(
            client,
            &infrastructure_carrier,
            &capital_entry,
            config.wait_timeout
        ),
        wait_devices_at_matching(
            client,
            &capital_devices,
            &capital_belt,
            None,
            config.wait_timeout,
        ),
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
    capital_wait?;
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
    detach_carrier_loads_at(client, &capital_belt, &mission.carrier_loads).await?;
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

async fn ensure_claim(
    client: &Client,
    code: &str,
    owner: Option<&str>,
    tags: &[String],
) -> AnyResult<()> {
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
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
                    ..Default::default()
                })
                .await?,
        )
        .await?;
    }
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
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    let snapshot = handle.snapshot().await?;
    if snapshot.tags.iter().any(|existing| existing == tag) {
        ensure_operation(
            &handle
                .configure(raw::devices::DeviceConfiguration {
                    add_tags: None,
                    remove_tags: Some(vec![tag.to_owned()]),
                    tags: None,
                    ..Default::default()
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

async fn collect_resource_with_cargo(
    client: &Client,
    code: &str,
    resource: &str,
    quantity: i64,
    cargo: BTreeMap<String, i64>,
) -> AnyResult<()> {
    let have = cargo.get(resource).copied().unwrap_or(0);
    if have >= quantity {
        return Ok(());
    }
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    if !cargo.is_empty() {
        ensure_operation(
            &handle
                .command(raw::devices::DeviceCommand::DepositResources { resources: None })
                .await?,
        )
        .await?;
    }
    let resources = [(resource.to_owned(), Value::from(quantity))]
        .into_iter()
        .collect();
    ensure_operation(
        &handle
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

async fn attach_carrier_loads_at(
    client: &Client,
    location: &str,
    loads: &[CarrierLoad],
) -> AnyResult<()> {
    let statuses = list_devices(client, Some(location), None).await?;
    let attached_by_carrier = statuses
        .iter()
        .filter_map(|device| Some((device.device_code.clone()?, attached_codes(device))))
        .collect::<BTreeMap<_, _>>();
    finish_all(
        join_all(loads.iter().map(|load| async {
            let attached = attached_by_carrier.get(&load.carrier);
            let missing = load
                .devices
                .iter()
                .filter(|code| attached.is_none_or(|codes| !codes.contains(*code)))
                .cloned()
                .collect::<Vec<_>>();
            if missing.is_empty() {
                return Ok(());
            }
            let handle = match client.devices().cached(&load.carrier) {
                Some(handle) => handle,
                None => client.devices().get(&load.carrier).await?,
            };
            ensure_operation(&handle.attach(targets(&missing)).await?).await
        }))
        .await,
    )
}

async fn detach_carrier_loads_at(
    client: &Client,
    location: &str,
    loads: &[CarrierLoad],
) -> AnyResult<()> {
    let statuses = list_devices(client, Some(location), None).await?;
    let attached_by_carrier = statuses
        .iter()
        .filter_map(|device| Some((device.device_code.clone()?, attached_codes(device))))
        .collect::<BTreeMap<_, _>>();
    finish_all(
        join_all(loads.iter().map(|load| async {
            let present = load
                .devices
                .iter()
                .filter(|code| {
                    attached_by_carrier
                        .get(&load.carrier)
                        .is_some_and(|codes| codes.contains(*code))
                })
                .cloned()
                .collect::<Vec<_>>();
            if present.is_empty() {
                return Ok(());
            }
            let handle = match client.devices().cached(&load.carrier) {
                Some(handle) => handle,
                None => client.devices().get(&load.carrier).await?,
            };
            ensure_operation(
                &handle
                    .command(raw::devices::DeviceCommand::Detach(targets(&present)))
                    .await?,
            )
            .await
        }))
        .await,
    )
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
    destination_system: Option<&str>,
    timeout: Duration,
) -> AnyResult<()> {
    let mut pending = Vec::new();
    let mut origins = BTreeSet::new();
    for (code, destination) in departures {
        // Essential startup already populated the managed device projection.
        // Reuse it here instead of issuing a raw GET for every ark device.
        let handle = match client.devices().cached(code) {
            Some(handle) => handle,
            None => client.devices().get(code).await?,
        };
        let detail = handle.snapshot().await?;
        if managed_device_at(&detail, destination, destination_system) {
            continue;
        }
        if let Some(planned) = managed_travel_destination(&detail) {
            if destination_matches(planned, destination, destination_system) {
                continue;
            }
            return Err(app_error(
                io::ErrorKind::Other,
                format!("device {code} is travelling to {planned:?}, not {destination}"),
            ));
        }
        let origin = detail
            .location
            .as_ref()
            .map(|location| location.id.as_str().to_owned())
            .ok_or_else(|| {
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
    start_device_travel_matching(client, code, destination, None).await
}

async fn start_device_travel_matching(
    client: &Client,
    code: &str,
    destination: &str,
    destination_system: Option<&str>,
) -> AnyResult<()> {
    // Match the event/relay executors: one authoritative managed read is
    // enough to inspect current travel state and obtain the durable handle.
    // The old path did a raw detail GET and then another managed read before
    // every departure.
    let handle = match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    };
    let detail = handle.snapshot().await?;
    if managed_device_at(&detail, destination, destination_system) {
        return Ok(());
    }
    if let Some(planned) = managed_travel_destination(&detail) {
        if destination_matches(planned, destination, destination_system) {
            return Ok(());
        }
        return Err(app_error(
            io::ErrorKind::Other,
            format!("device {code} is travelling to {planned:?}, not {destination}"),
        ));
    }
    ensure_operation(
        &handle
            .command(raw::devices::DeviceCommand::Travel {
                destination: destination.into(),
                dry_run: None,
                via: None,
            })
            .await?,
    )
    .await
}

fn managed_device_at(
    device: &Device,
    destination: &str,
    destination_system: Option<&str>,
) -> bool {
    device.travel.is_none()
        && device.location.as_ref().is_some_and(|location| {
            destination_matches(location.id.as_str(), destination, destination_system)
        })
}

fn managed_travel_destination(device: &Device) -> Option<&str> {
    device.travel.as_ref().and_then(|travel| {
        travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str())
    })
}

async fn wait_device_at(
    client: &Client,
    code: &str,
    destination: &str,
    timeout: Duration,
) -> AnyResult<()> {
    wait_devices_at_matching(client, &[code.to_owned()], destination, None, timeout).await
}

async fn wait_devices_at_matching(
    client: &Client,
    codes: &[String],
    destination: &str,
    destination_system: Option<&str>,
    wait_timeout: Duration,
) -> AnyResult<()> {
    if codes.is_empty() {
        return Ok(());
    }
    let mut pending = codes.iter().cloned().collect::<BTreeSet<_>>();
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + wait_timeout;

    loop {
        let mut eta_seconds = None::<i64>;
        let current = pending.iter().cloned().collect::<Vec<_>>();
        for code in current {
            let handle = match client.devices().cached(&code) {
                Some(handle) => handle,
                None => client.devices().get(&code).await?,
            };
            let snapshot = handle.snapshot().await?;
            if managed_device_at(&snapshot, destination, destination_system) {
                pending.remove(&code);
                continue;
            }
            if let Some(planned) = managed_travel_destination(&snapshot)
                && !destination_matches(planned, destination, destination_system)
            {
                return Err(app_error(
                    io::ErrorKind::Other,
                    format!("device {code} is travelling to {planned:?}, not {destination}"),
                ));
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
        if matches!(wake, TravelWake::Poll | TravelWake::Gap) {
            // SSE/projection updates are the fast path. On a sparse fallback,
            // authoritatively refresh only the devices still in flight rather
            // than polling every ark device every five seconds.
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TravelWake {
    Event,
    Poll,
    Gap,
}

async fn wait_for_device_travel_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    pending: &BTreeSet<String>,
    poll_interval: Duration,
) -> AnyResult<TravelWake> {
    let poll_deadline = Instant::now() + poll_interval;
    loop {
        let wake_deadline = deadline.min(poll_deadline);
        let remaining = wake_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(TravelWake::Poll);
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event))
                if event.name.as_str() == "travel.arrived"
                    && event
                        .device
                        .as_ref()
                        .is_some_and(|device| pending.contains(device.id.as_str())) =>
            {
                return Ok(TravelWake::Event);
            }
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(TravelWake::Poll),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; refreshing bootstrap device travel");
                return Ok(TravelWake::Gap);
            }
        }
    }
}

fn travel_poll_interval(eta_seconds: Option<i64>) -> Duration {
    match eta_seconds.unwrap_or(0) {
        eta if eta >= 300 => Duration::from_secs(60),
        eta if eta >= 60 => Duration::from_secs(30),
        eta if eta > 0 => Duration::from_secs(10),
        _ => POLL_INTERVAL,
    }
}

async fn start_replicant_travel(client: &Client, code: &str, destination: &str) -> AnyResult<()> {
    start_replicant_travel_matching(client, code, destination, None).await
}

async fn start_replicant_travel_matching(
    client: &Client,
    code: &str,
    destination: &str,
    destination_system: Option<&str>,
) -> AnyResult<()> {
    let handle = client.replicants().get_owned(code).await?;
    let snapshot = handle.snapshot().await?;
    if snapshot.travel.is_none()
        && snapshot.location.as_ref().is_some_and(|location| {
            destination_matches(location.id.as_str(), destination, destination_system)
        })
    {
        return Ok(());
    }
    if let Some(travel) = &snapshot.travel {
        let planned = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        if planned
            .is_some_and(|planned| destination_matches(planned, destination, destination_system))
        {
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
    wait_replicant_at_matching(client, code, destination, None, timeout).await
}

async fn wait_replicant_at_matching(
    client: &Client,
    code: &str,
    destination: &str,
    destination_system: Option<&str>,
    wait_timeout: Duration,
) -> AnyResult<()> {
    let mut handle = client.replicants().get_owned(code).await?;
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + wait_timeout;
    loop {
        let snapshot = handle.snapshot().await?;
        if snapshot.travel.is_none()
            && snapshot.location.as_ref().is_some_and(|location| {
                destination_matches(location.id.as_str(), destination, destination_system)
            })
        {
            return Ok(());
        }
        if let Some(travel) = &snapshot.travel {
            let planned = travel
                .final_destination
                .as_ref()
                .or(travel.destination.as_ref())
                .map(|location| location.id.as_str());
            if !planned.is_some_and(|planned| {
                destination_matches(planned, destination, destination_system)
            }) {
                return Err(app_error(
                    io::ErrorKind::Other,
                    format!("replicant {code} is travelling to {planned:?}, not {destination}"),
                ));
            }
        } else {
            let operation = handle.travel().to(destination).depart().await?;
            ensure_operation(&operation).await?;
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for replicant {code} at {destination}"),
            ));
        }

        let eta_seconds = snapshot
            .travel
            .as_ref()
            .and_then(|travel| travel.eta_seconds);
        match wait_for_replicant_travel_event(
            &mut watch,
            deadline,
            code,
            travel_poll_interval(eta_seconds),
        )
        .await?
        {
            TravelWake::Event => {}
            TravelWake::Poll | TravelWake::Gap => {
                handle = handle.refresh().await?;
            }
        }
    }
}

async fn wait_for_replicant_travel_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    code: &str,
    poll_interval: Duration,
) -> AnyResult<TravelWake> {
    let poll_deadline = Instant::now() + poll_interval;
    loop {
        let wake_deadline = deadline.min(poll_deadline);
        let remaining = wake_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(TravelWake::Poll);
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event))
                if event.name.as_str() == "travel.arrived"
                    && event
                        .replicant
                        .as_ref()
                        .is_some_and(|replicant| replicant.id.as_str() == code) =>
            {
                return Ok(TravelWake::Event);
            }
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(TravelWake::Poll),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; refreshing bootstrap replicant travel");
                return Ok(TravelWake::Gap);
            }
        }
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
    fn landing_destination_prefers_known_entry_point() {
        assert_eq!(
            preferred_star_destination("DELTA", Some("DELTA-4-L4")),
            "DELTA-4-L4"
        );
    }

    #[test]
    fn landing_destination_falls_back_to_star_designation() {
        assert_eq!(preferred_star_destination("DELTA", None), "DELTA");
    }

    #[test]
    fn system_level_destination_accepts_default_arrival_zone() {
        assert!(destination_matches("DELTA-OORT", "DELTA", Some("DELTA")));
        assert!(destination_matches("DELTA-KUIPER", "DELTA", Some("DELTA")));
        assert!(destination_matches("DELTA", "DELTA", Some("DELTA")));
        assert!(!destination_matches("DELTAE-OORT", "DELTA", Some("DELTA")));
    }

    #[test]
    fn exact_destination_does_not_accept_another_location_in_system() {
        assert!(destination_matches("DELTA-4-L4", "DELTA-4-L4", None));
        assert!(!destination_matches("DELTA-OORT", "DELTA-4-L4", None));
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
            ("surge_carrier".into(), 14),
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
        assert_eq!(remaining.get("surge_carrier"), Some(&5));
    }

    #[test]
    fn fresh_ark_role_packing_keeps_mining_relay_and_beacon_sets_together() {
        let profile = replicant_bootstrap_planner::BootstrapProfile::default();
        let requirements = ark_device_requirements(&profile);
        let mut assets = BTreeMap::<String, Vec<String>>::new();
        for (device_type, quantity) in requirements {
            if device_type == CARGO_FREIGHTER {
                continue;
            }
            assets.insert(
                device_type.clone(),
                (0..quantity)
                    .map(|index| format!("{device_type}-{index:02}"))
                    .collect(),
            );
        }
        let unassigned = assets.values().flatten().cloned().collect::<BTreeSet<_>>();
        let (reserved, general) =
            fresh_role_payloads(&profile, &assets, &unassigned, 9).expect("role payloads");

        let mining = reserved
            .iter()
            .filter(|(role, _)| role.starts_with("mining-"))
            .collect::<Vec<_>>();
        assert_eq!(mining.len(), 8);
        for (_, devices) in mining {
            let counts = devices
                .iter()
                .filter_map(|code| code.rsplit_once('-').map(|(device_type, _)| device_type))
                .fold(BTreeMap::<&str, usize>::new(), |mut counts, device_type| {
                    *counts.entry(device_type).or_default() += 1;
                    counts
                });
            assert_eq!(counts.get(MINING_CONTROLLER), Some(&1));
            assert_eq!(counts.get(MINING_DRONE), Some(&4));
            assert_eq!(counts.get(SURVEY_CONTROLLER), Some(&1));
            assert_eq!(counts.get(SURVEY_DRONE), Some(&2));
            assert_eq!(counts.get(MAINTENANCE_DRONE), Some(&1));
        }
        assert_eq!(
            reserved
                .iter()
                .filter(|(role, _)| role.starts_with("relays-"))
                .count(),
            2
        );
        assert_eq!(
            reserved
                .iter()
                .filter(|(role, _)| role.starts_with("beacons-"))
                .count(),
            1
        );
        assert!(reserved.iter().all(|(_, devices)| devices.len() == 9));
        assert_eq!(general.len(), 19);
    }

    #[test]
    fn replacement_tag_is_distinct_and_fits_server_limit() {
        let tag = carrier_replacement_tag("boot-m:0123456789abcdef");
        assert_eq!(tag, "boot-repl:0123456789abcdef");
        assert!(tag.len() <= 32);
        assert!(!reservation_tag(&tag));
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
