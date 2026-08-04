use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    time::{Duration, Instant},
};

use futures::future::join_all;
use replicant_bootstrap_planner::{
    AUTOFACTORY, BeltCandidate, FTL_RELAY, SEED_RESOURCES, SURGE_CARRIER, SYSTEM_HUB,
    ark_device_requirements, attachment_slots, missing_carriers, select_dense_belts,
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
    model::{BootstrapMission, CarrierLoad, MissionPhase, ReplicantIdentity, SeedFreighter},
    reservation_tag, save_mission, unique,
};

const POLL_INTERVAL: Duration = Duration::from_secs(5);

pub async fn resolve_replicant(client: &Client, query: &str) -> AnyResult<ReplicantIdentity> {
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
    let replicant = match matches.len() {
        1 => matches.remove(0),
        0 => {
            return Err(app_error(
                io::ErrorKind::NotFound,
                format!("no owned replicant matches {query:?}"),
            ));
        }
        _ => {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("replicant name {query:?} is ambiguous; use its code"),
            ));
        }
    };
    let vessel = replicant.hosted_device.as_ref().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!(
                "replicant {} has no hosted vessel",
                replicant.key.id.as_str()
            ),
        )
    })?;
    Ok(ReplicantIdentity {
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

    manufacture_ark(client, config, mission).await?;
    load_ark(client, config, mission).await?;
    dispatch_to_landing(client, config, mission).await?;
    quick_scout(client, config, mission).await?;
    establish_capital(client, config, mission).await?;
    establish_initial_mine(client, config, mission).await?;
    survey_region(client, config, mission).await?;
    expand_relays(client, config, mission).await?;
    expand_mining(client, config, mission).await?;
    cleanup(client, config, mission).await
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
    if phase_after(mission.phase, MissionPhase::ManufacturingArk) {
        return Ok(());
    }
    set_phase(config, mission, MissionPhase::ManufacturingArk).await?;
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

    let desired = ark_device_requirements(&mission.profile);
    if mission.assets.is_empty() {
        let devices = list_devices(client, Some(&mission.source_hub), None).await?;
        for (device_type, quantity) in &desired {
            if device_type == AUTOFACTORY {
                continue;
            }
            let selected = devices
                .iter()
                .filter(|device| {
                    eligible_idle(device, &mission.mission_tag)
                        && device.device_type.as_deref() == Some(device_type)
                })
                .take(usize::try_from(*quantity).unwrap_or(0))
                .filter_map(|device| device.device_code.clone())
                .collect::<Vec<_>>();
            if !selected.is_empty() {
                mission.assets.insert(device_type.clone(), selected);
            }
        }
        let existing_carriers = devices
            .iter()
            .filter(|device| {
                eligible_idle(device, &mission.mission_tag)
                    && matches!(
                        device.device_type.as_deref(),
                        Some("surge_carrier" | "surge_platform")
                    )
            })
            .filter_map(|device| device.device_code.clone())
            .collect::<Vec<_>>();
        let existing_capacity = existing_carriers
            .iter()
            .map(|code| {
                devices
                    .iter()
                    .find(|device| device.device_code.as_deref() == Some(code))
                    .and_then(|device| device.attach_capacity)
                    .unwrap_or(carrier_capacity)
            })
            .sum();
        let required_carriers = missing_carriers(
            attachment_slots(&desired),
            existing_capacity,
            carrier_capacity,
        )?;
        mission
            .assets
            .insert(SURGE_CARRIER.into(), existing_carriers);
        let mut shortages = BTreeMap::new();
        for (device_type, quantity) in &desired {
            let have = i64::try_from(mission.assets.get(device_type).map_or(0, Vec::len))?;
            if quantity > &have {
                shortages.insert(device_type.clone(), quantity - have);
            }
        }
        if required_carriers > 0 {
            shortages.insert(SURGE_CARRIER.into(), required_carriers);
        }
        mission.print.requirements = shortages;
        save_mission(&config.mission_file, mission)?;
        claim_recorded_assets(client, mission).await?;
    }

    if !mission.print.queued && !mission.print.requirements.is_empty() {
        if mission.print.submission_started {
            return Err(app_error(
                io::ErrorKind::Other,
                "ark print submission was interrupted after it began; inspect Autofactory queues for the boot-m tag before clearing print.submission_started in the mission",
            ));
        }
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
            let report = queue_prints(client, &standard, &options).await?;
            mission.print.operation_ids.extend(report.operation_ids);
        }
        if !modular.is_empty() {
            let mut options = QueueOptions::at(&mission.source_hub);
            options.tags = tags;
            options.flatpack = true;
            options.wait_timeout = config.wait_timeout;
            let report = queue_prints(client, &modular, &options).await?;
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
        let carrier_capacity = tagged
            .iter()
            .filter(|device| {
                matches!(
                    device.device_type.as_deref(),
                    Some("surge_carrier" | "surge_platform")
                )
            })
            .map(|device| device.attach_capacity.unwrap_or(0).max(0))
            .sum::<i64>();
        let carriers_ready = carrier_capacity >= attachment_slots(desired);
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
    let carriers = mission.assets.entry(SURGE_CARRIER.into()).or_default();
    let mut capacity = tagged
        .iter()
        .filter(|device| {
            device
                .device_code
                .as_ref()
                .is_some_and(|code| carriers.contains(code))
        })
        .map(|device| device.attach_capacity.unwrap_or(0).max(0))
        .sum::<i64>();
    for device in tagged.iter().filter(|device| {
        matches!(
            device.device_type.as_deref(),
            Some("surge_carrier" | "surge_platform")
        )
    }) {
        if capacity >= attachment_slots(desired) {
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
    if capacity < attachment_slots(desired) {
        return Err(app_error(
            io::ErrorKind::NotFound,
            "available Surge Carrier capacity is incomplete",
        ));
    }
    save_mission(&config.mission_file, mission)
}

async fn claim_recorded_assets(client: &Client, mission: &BootstrapMission) -> AnyResult<()> {
    for code in mission.assets.values().flatten() {
        ensure_claim(
            client,
            code,
            &mission.operator.code,
            &[mission.mission_tag.clone(), mission.region_tag.clone()],
        )
        .await?;
    }
    Ok(())
}

async fn load_ark(
    client: &Client,
    config: &Config,
    mission: &mut BootstrapMission,
) -> AnyResult<()> {
    if phase_after(mission.phase, MissionPhase::LoadingArk) {
        return Ok(());
    }
    set_phase(config, mission, MissionPhase::LoadingArk).await?;
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

    if mission.carrier_loads.is_empty() {
        let mut payload = mission
            .assets
            .iter()
            .filter(|(device_type, _)| {
                !matches!(device_type.as_str(), CARGO_FREIGHTER | SURGE_CARRIER)
            })
            .flat_map(|(_, codes)| codes.iter().cloned())
            .collect::<Vec<_>>();
        payload.sort();
        let mut infrastructure = vec![
            first_asset(mission, SYSTEM_HUB)?,
            first_asset(mission, FTL_RELAY)?,
        ];
        infrastructure.extend(
            mission
                .assets
                .get(MAINTENANCE_DRONE)
                .into_iter()
                .flatten()
                .rev()
                .take(usize::try_from(mission.profile.hub_maintenance_drones)?)
                .cloned(),
        );
        let infrastructure_count = infrastructure.len();
        payload.retain(|code| !infrastructure.contains(code));
        infrastructure.append(&mut payload);
        payload = infrastructure;
        let mut carriers = Vec::new();
        for carrier in mission
            .assets
            .get(SURGE_CARRIER)
            .cloned()
            .unwrap_or_default()
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
        let mut cursor = 0usize;
        for (carrier, capacity) in carriers {
            if cursor == 0 && usize::try_from(capacity.max(0))? < infrastructure_count {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    format!(
                        "first Surge Carrier capacity {capacity} cannot hold the {infrastructure_count}-device capital infrastructure group"
                    ),
                ));
            }
            let take = usize::try_from(capacity.max(0))?.min(payload.len().saturating_sub(cursor));
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
                    "carrier capacity covers {cursor} of {} payload devices",
                    payload.len()
                ),
            ));
        }
        save_mission(&config.mission_file, mission)?;
    }
    let attach_results = join_all(
        mission
            .carrier_loads
            .iter()
            .map(|load| attach_devices(client, &load.carrier, &load.devices)),
    )
    .await;
    finish_all(attach_results)
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
    info!(devices=devices.len(), replicants=2, destination=%mission.landing_entry, "dispatching the complete ark concurrently");
    let (device_starts, operator_start, explorer_start) = tokio::join!(
        async {
            finish_all(
                join_all(
                    devices
                        .iter()
                        .map(|code| start_device_travel(client, code, &mission.landing_entry)),
                )
                .await,
            )
        },
        start_replicant_travel(client, &mission.operator.code, &mission.landing_entry),
        start_replicant_travel(client, &mission.explorer.code, &mission.landing_entry),
    );
    device_starts?;
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
    set_phase(config, mission, MissionPhase::QuickScouting).await?;
    let controller = mission
        .assets
        .get(SURVEY_CONTROLLER)
        .and_then(|codes| codes.last())
        .cloned()
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                "ark has no exploration survey controller",
            )
        })?;
    let drones = mission
        .assets
        .get(SURVEY_DRONE)
        .map(|codes| codes.iter().rev().take(3).cloned().collect::<Vec<_>>())
        .filter(|codes| codes.len() == 3)
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                "ark has fewer than three exploration survey drones",
            )
        })?;
    detach_selected_from_carriers(
        client,
        mission,
        &std::iter::once(controller.clone())
            .chain(drones.iter().cloned())
            .collect::<Vec<_>>(),
    )
    .await?;
    ensure_claim(
        client,
        &controller,
        &mission.explorer.code,
        &[mission.mission_tag.clone(), mission.region_tag.clone()],
    )
    .await?;
    for drone in &drones {
        ensure_claim(
            client,
            drone,
            &mission.explorer.code,
            &[mission.mission_tag.clone(), mission.region_tag.clone()],
        )
        .await?;
    }
    let report = execute_survey(
        client,
        &SurveyRequest {
            replicant: mission.explorer.code.clone(),
            vessel: mission.explorer.vessel.clone(),
            center: mission.landing_star.clone(),
            radius_ly: mission.quick_scout_radius_ly,
            system_limit: 12,
            star_detail_concurrency: 8,
            mission_file: mission.children.quick_survey.clone(),
            controller: Some(controller),
            drones: Some(drones),
            include_explored: false,
            travel_timeout: config.wait_timeout,
            survey_timeout: config.wait_timeout,
        },
    )
    .await?;
    mission.quick_scouted_systems = unique(report.systems);
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
    let hub = first_asset(mission, SYSTEM_HUB)?;
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
                "ark has too few hub maintenance drones",
            )
        })?;
    let infrastructure = std::iter::once(hub.clone())
        .chain(std::iter::once(relay.clone()))
        .chain(maintenance.iter().cloned())
        .collect::<Vec<_>>();
    let infrastructure_carrier = mission
        .carrier_loads
        .iter()
        .find(|load| load.devices.contains(&hub))
        .map(|load| load.carrier.clone())
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                "System Hub is not assigned to a carrier",
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
    let (infra_start, carrier_starts, freighter_starts, operator_start, explorer_start) = tokio::join!(
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
        start_replicant_travel(client, &mission.operator.code, &capital_belt),
        start_replicant_travel(client, &mission.explorer.code, &capital_belt),
    );
    infra_start?;
    carrier_starts?;
    freighter_starts?;
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
    configure_structure(client, &hub, true).await?;
    configure_structure(client, &relay, false).await?;
    for code in &maintenance {
        set_patrol(client, code).await?;
    }
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
        MissionPhase::Outbound => 3,
        MissionPhase::QuickScouting => 4,
        MissionPhase::EstablishingCapital => 5,
        MissionPhase::InitialMining => 6,
        MissionPhase::SurveyingRegion => 7,
        MissionPhase::ExpandingRelays => 8,
        MissionPhase::ExpandingMining => 9,
        MissionPhase::CleaningUp => 10,
        MissionPhase::Completed | MissionPhase::CompletedWithWarnings => 11,
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

async fn ensure_claim(client: &Client, code: &str, owner: &str, tags: &[String]) -> AnyResult<()> {
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
    if snapshot
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

async fn detach_selected_from_carriers(
    client: &Client,
    mission: &BootstrapMission,
    selected: &[String],
) -> AnyResult<()> {
    for load in &mission.carrier_loads {
        let group = selected
            .iter()
            .filter(|code| load.devices.contains(code))
            .cloned()
            .collect::<Vec<_>>();
        detach_devices(client, &load.carrier, &group).await?;
    }
    Ok(())
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
        assert!(!phase_after(MissionPhase::Outbound, MissionPhase::Outbound));
    }
    #[test]
    fn density_order_prefers_dense() {
        assert!(density_rank("dense") > density_rank("moderate"));
    }
}
