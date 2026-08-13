//! Lightweight asteroid-belt scouting with a Replicant only.
//!
//! The command visits the requested systems in order, performs the Replicant's
//! instant system scan when the system is not already explored, records the
//! asteroid-belt summary, and immediately moves on. No survey controller or
//! survey drones are required.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{self, OpenOptions},
    io,
    path::PathBuf,
    time::Duration,
};

use replicant_client::{
    Client, Operation, OperationStatus, Replicant, Star, SyncDomain,
    domain::{GalacticPosition, Location},
};
use replicant_runtime::{config::ManagedClientConfig, start_managed_client};
use serde_json::Value;
use tokio::time::{Instant, timeout};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, prelude::*};

const DEFAULT_REPLICANT: &str = "Chats-4";
const DEFAULT_DATABASE: &str = "replicant-client.sqlite";
const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(6 * 60 * 60);
const DEFAULT_SYSTEM_LIMIT: usize = 80;
const POLL_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug)]
struct Config {
    database: PathBuf,
    replicant: String,
    systems: Vec<String>,
    route_start: Option<String>,
    radius_ly: Option<f64>,
    system_limit: usize,
    include_explored: bool,
    plan_only: bool,
    wait_timeout: Duration,
    log_file: Option<PathBuf>,
    verbose: bool,
}

impl Config {
    fn from_args_and_env(arguments: impl IntoIterator<Item = String>) -> crate::AnyResult<Self> {
        let mut arguments = arguments.into_iter();
        let mut database = env::var_os("REPLICANT_DB")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_DATABASE));
        let mut replicant =
            env::var("RS_BELT_SEARCH_REPLICANT").unwrap_or_else(|_| DEFAULT_REPLICANT.to_owned());
        let mut systems = Vec::new();
        let mut route_start = env::var("RS_BELT_SEARCH_START")
            .ok()
            .map(|value| normalize_system(&value))
            .transpose()?;
        let mut radius_ly = env::var("RS_BELT_SEARCH_RADIUS_LY")
            .ok()
            .map(|value| parse_positive_f64("RS_BELT_SEARCH_RADIUS_LY", &value))
            .transpose()?;
        let mut system_limit = env::var("RS_BELT_SEARCH_SYSTEM_LIMIT")
            .ok()
            .map(|value| parse_positive_usize("RS_BELT_SEARCH_SYSTEM_LIMIT", &value))
            .transpose()?
            .unwrap_or(DEFAULT_SYSTEM_LIMIT);
        let mut include_explored = env_bool("RS_BELT_SEARCH_INCLUDE_EXPLORED", false)?;
        let mut plan_only = env_bool("RS_BELT_SEARCH_PLAN_ONLY", false)?;
        let mut wait_timeout = Duration::from_secs(
            env::var("RS_BELT_SEARCH_WAIT_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|value| *value > 0)
                .unwrap_or(DEFAULT_WAIT_TIMEOUT.as_secs()),
        );
        let mut log_file = env::var_os("RS_BELT_SEARCH_LOG_FILE").map(PathBuf::from);
        let mut verbose = env_bool("RS_BELT_SEARCH_VERBOSE", false)?;

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "-h" | "--help" | "help" => {
                    print_help();
                    std::process::exit(0);
                }
                "--replicant" => replicant = next_value(&mut arguments, "--replicant")?,
                "--start" | "--center" | "--centre" => {
                    route_start = Some(normalize_system(&next_value(&mut arguments, &argument)?)?);
                }
                "--range" | "--radius" | "--radius-ly" => {
                    let value = next_value(&mut arguments, &argument)?;
                    radius_ly = Some(parse_positive_f64(&argument, &value)?);
                }
                "--system-limit" => {
                    let value = next_value(&mut arguments, "--system-limit")?;
                    system_limit = parse_positive_usize("--system-limit", &value)?;
                }
                "--include-explored" => include_explored = true,
                "--plan-only" | "--plan" => plan_only = true,
                "--systems-file" => {
                    let path = PathBuf::from(next_value(&mut arguments, "--systems-file")?);
                    let contents = fs::read_to_string(&path).map_err(|error| {
                        app_error(format!(
                            "failed to read systems file {}: {error}",
                            path.display()
                        ))
                    })?;
                    systems.extend(parse_systems_text(&contents)?);
                }
                "--database" | "--db" => {
                    database = PathBuf::from(next_value(&mut arguments, &argument)?);
                }
                "--wait-timeout-secs" => {
                    let value = next_value(&mut arguments, "--wait-timeout-secs")?;
                    let seconds = value.parse::<u64>().map_err(|_| {
                        app_error(format!(
                            "--wait-timeout-secs must be an integer, got {value:?}"
                        ))
                    })?;
                    if seconds == 0 {
                        return Err(app_error("--wait-timeout-secs must be greater than zero"));
                    }
                    wait_timeout = Duration::from_secs(seconds);
                }
                "--log-file" => {
                    log_file = Some(PathBuf::from(next_value(&mut arguments, "--log-file")?));
                }
                "--verbose" => verbose = true,
                other if other.starts_with('-') => {
                    return Err(app_error(format!(
                        "unknown belt-search option {other:?}; run `replicant-cli belt-search --help`"
                    )));
                }
                system => systems.extend(parse_systems_text(system)?),
            }
        }

        systems = unique_preserving_order(systems);
        let auto_route_requested = route_start.is_some() || radius_ly.is_some();
        if auto_route_requested {
            if route_start.is_none() || radius_ly.is_none() {
                return Err(app_error(
                    "automatic belt-search routing requires both --start LOCATION|SYSTEM and --range LY",
                ));
            }
            if !systems.is_empty() {
                return Err(app_error(
                    "explicit SYSTEM arguments/--systems-file cannot be combined with --start/--range automatic routing",
                ));
            }
        } else if systems.is_empty() {
            return Err(app_error(
                "belt-search requires SYSTEM..., --systems-file PATH, or --start LOCATION|SYSTEM --range LY",
            ));
        }

        Ok(Self {
            database,
            replicant,
            systems,
            route_start,
            radius_ly,
            system_limit,
            include_explored,
            plan_only,
            wait_timeout,
            log_file,
            verbose,
        })
    }
}

#[derive(Clone, Debug)]
struct BeltReport {
    system: String,
    designation: String,
    density: String,
    inner_radius_au: Option<f64>,
    outer_radius_au: Option<f64>,
    resources: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
struct RouteCandidate {
    system: String,
    position: GalacticPosition,
    distance_from_start_ly: f64,
    explored: bool,
}

#[derive(Clone, Debug)]
struct PlannedStop {
    system: String,
    distance_from_start_ly: f64,
    leg_distance_ly: f64,
    explored: bool,
}

#[derive(Clone, Debug)]
struct BeltRoutePlan {
    requested_start: String,
    start_system: String,
    radius_ly: f64,
    stops: Vec<PlannedStop>,
    nearest_neighbor_distance_ly: f64,
    optimized_distance_ly: f64,
    two_opt_swaps: usize,
}

impl BeltRoutePlan {
    fn systems(&self) -> Vec<String> {
        self.stops.iter().map(|stop| stop.system.clone()).collect()
    }
}

impl BeltReport {
    fn density_rank(&self) -> u8 {
        density_rank(&self.density)
    }

    fn radii(&self) -> String {
        match (self.inner_radius_au, self.outer_radius_au) {
            (Some(inner), Some(outer)) => format!("{inner:.2}-{outer:.2} AU"),
            (Some(inner), None) => format!("{inner:.2}-? AU"),
            (None, Some(outer)) => format!("?-{outer:.2} AU"),
            (None, None) => "?".to_owned(),
        }
    }

    fn resources(&self) -> String {
        if self.resources.is_empty() {
            return "unknown resources".to_owned();
        }
        self.resources
            .iter()
            .map(|(resource, scarcity)| format!("{resource}={scarcity}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// Runs the standalone belt-search command-line interface.
pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let config = Config::from_args_and_env(arguments)?;
    init_logging(&config)?;

    let client = start_managed_client(ManagedClientConfig::from_env(&config.database)?).await?;

    let result = run(&client, &config).await;
    let close_result = client.close().await;
    close_result?;
    result
}

async fn run(client: &Client, config: &Config) -> crate::AnyResult<()> {
    client.ready().await?;
    client.sync().domain(SyncDomain::Replicants).await?;
    let replicant = resolve_owned_replicant(client, &config.replicant).await?;
    let replicant_code = replicant.key.id.as_str().to_owned();
    let replicant_name = replicant
        .name
        .as_deref()
        .unwrap_or(replicant_code.as_str())
        .to_owned();

    // Populate durable explored-star knowledge once. Individual systems are
    // still targeted-refreshed before a scan when local knowledge is missing.
    if let Err(error) = client.galaxy().sync_replicant_stars(&replicant_code).await {
        warn!(
            replicant = %replicant_code,
            error = %error,
            "belt-search could not refresh the complete explored-system list; falling back to targeted checks"
        );
    }

    let route_plan =
        if let (Some(start), Some(radius_ly)) = (config.route_start.as_deref(), config.radius_ly) {
            Some(
                plan_belt_route(
                    client,
                    &replicant_code,
                    start,
                    radius_ly,
                    config.system_limit,
                    config.include_explored,
                )
                .await?,
            )
        } else {
            None
        };
    let systems = route_plan
        .as_ref()
        .map(BeltRoutePlan::systems)
        .unwrap_or_else(|| config.systems.clone());

    println!("Belt search");
    println!("Replicant: {replicant_name} ({replicant_code})");
    if let Some(plan) = &route_plan {
        print_route_plan(plan, config.include_explored);
    } else {
        println!("Systems: {}", systems.join(" -> "));
    }
    println!();

    if config.plan_only {
        if route_plan.is_none() {
            println!("Plan only: {} explicit system(s)", systems.len());
            for (index, system) in systems.iter().enumerate() {
                println!("  {:>3}. {system}", index + 1);
            }
        }
        return Ok(());
    }

    let auto_route = route_plan.is_some();
    let mut all_belts = Vec::new();
    for (index, system) in systems.iter().enumerate() {
        println!("[{}/{}] {system}", index + 1, systems.len());
        let already_explored = system_is_explored(client, &replicant_code, system).await?;
        let scanned_now = if already_explored {
            if auto_route {
                // Auto routes are anchored at the requested start and must stay
                // physically faithful to the planned order. Known systems are
                // therefore visited when present in the route, but never rescanned.
                travel_to_system(client, &replicant_code, system, config.wait_timeout).await?;
                info!(
                    replicant = %replicant_code,
                    system,
                    "belt-search route stop is already explored; visited without duplicate scan"
                );
            } else {
                info!(
                    replicant = %replicant_code,
                    system,
                    "belt-search system is already explored; skipping travel and duplicate scan"
                );
            }
            false
        } else {
            travel_to_system(client, &replicant_code, system, config.wait_timeout).await?;
            scan_system(client, &replicant_code, system).await?;
            true
        };
        let location = client.locations().get(system).await?;
        let mut belts = belts_from_location(system, &location);
        belts.sort_by(|left, right| {
            right
                .density_rank()
                .cmp(&left.density_rank())
                .then_with(|| left.designation.cmp(&right.designation))
        });

        println!(
            "  scan: {}",
            if scanned_now {
                "completed"
            } else {
                "already known"
            }
        );
        if belts.is_empty() {
            println!("  belts: none");
        } else {
            for belt in &belts {
                println!(
                    "  belt: {:<8} {:<24} {:<15} {}",
                    belt.density,
                    belt.designation,
                    belt.radii(),
                    belt.resources()
                );
            }
        }
        println!();
        all_belts.extend(belts);
    }

    print_summary(&systems, &all_belts);
    Ok(())
}

async fn plan_belt_route(
    client: &Client,
    replicant_code: &str,
    requested_start: &str,
    radius_ly: f64,
    system_limit: usize,
    include_explored: bool,
) -> crate::AnyResult<BeltRoutePlan> {
    let mut catalogue = client.galaxy().catalogue();
    if catalogue.is_empty() {
        info!("belt-search star catalogue is empty; refreshing it for route planning");
        client.galaxy().refresh_catalogue().await?;
        catalogue = client.galaxy().catalogue();
    }

    let start = resolve_route_start(&catalogue, requested_start)?;
    let start_system = start.key.id.as_str().to_owned();
    let start_position = start.position.ok_or_else(|| {
        app_error(format!(
            "belt-search start system {start_system} has no catalogue position"
        ))
    })?;

    let explored_by_system = client
        .galaxy()
        .replicant_star_knowledge(replicant_code)
        .into_iter()
        .filter_map(|knowledge| {
            knowledge
                .explored
                .map(|explored| (knowledge.star.id.as_str().to_owned(), explored))
        })
        .collect::<BTreeMap<_, _>>();

    let mut candidates = catalogue
        .into_iter()
        .filter_map(|star| {
            let system = star.key.id.as_str().to_owned();
            if system == start_system {
                return None;
            }
            let position = star.position?;
            let distance = position_distance(start_position, position);
            if distance > radius_ly {
                return None;
            }
            let explored = explored_by_system.get(&system).copied().unwrap_or(false);
            if explored && !include_explored {
                return None;
            }
            Some(RouteCandidate {
                system,
                position,
                distance_from_start_ly: distance,
                explored,
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.system.cmp(&right.system));

    let mut remaining = candidates;
    let mut ordered = Vec::new();
    let mut current = start_position;
    while !remaining.is_empty() && ordered.len() + 1 < system_limit {
        let (index, _) = remaining
            .iter()
            .enumerate()
            .map(|(index, candidate)| (index, position_distance(current, candidate.position)))
            .min_by(
                |(left_index, left_distance), (right_index, right_distance)| {
                    left_distance
                        .partial_cmp(right_distance)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| {
                            remaining[*left_index]
                                .system
                                .cmp(&remaining[*right_index].system)
                        })
                },
            )
            .expect("belt-search route candidate list is non-empty");
        let candidate = remaining.remove(index);
        current = candidate.position;
        ordered.push(candidate);
    }

    let nearest_neighbor_distance_ly = candidate_route_distance(start_position, &ordered);
    let two_opt_swaps = improve_candidate_route_2opt(start_position, &mut ordered, 8);
    let optimized_distance_ly = candidate_route_distance(start_position, &ordered);

    let mut stops = Vec::with_capacity(ordered.len() + 1);
    stops.push(PlannedStop {
        system: start_system.clone(),
        distance_from_start_ly: 0.0,
        leg_distance_ly: 0.0,
        explored: explored_by_system
            .get(&start_system)
            .copied()
            .unwrap_or(false),
    });
    let mut previous = start_position;
    for candidate in ordered {
        let leg_distance_ly = position_distance(previous, candidate.position);
        previous = candidate.position;
        stops.push(PlannedStop {
            system: candidate.system,
            distance_from_start_ly: candidate.distance_from_start_ly,
            leg_distance_ly,
            explored: candidate.explored,
        });
    }

    info!(
        replicant = %replicant_code,
        requested_start = %requested_start,
        start_system = %start_system,
        radius_ly = radius_ly,
        stops = stops.len(),
        system_limit = system_limit,
        include_explored = include_explored,
        nearest_neighbor_distance_ly = nearest_neighbor_distance_ly,
        optimized_distance_ly = optimized_distance_ly,
        two_opt_swaps = two_opt_swaps,
        "planned belt-search route"
    );

    Ok(BeltRoutePlan {
        requested_start: requested_start.to_owned(),
        start_system,
        radius_ly,
        stops,
        nearest_neighbor_distance_ly,
        optimized_distance_ly,
        two_opt_swaps,
    })
}

fn resolve_route_start<'a>(
    catalogue: &'a [Star],
    requested_start: &str,
) -> crate::AnyResult<&'a Star> {
    if let Some(star) = catalogue
        .iter()
        .find(|star| star.key.id.as_str().eq_ignore_ascii_case(requested_start))
    {
        return Ok(star);
    }

    catalogue
        .iter()
        .filter(|star| designation_in_system(requested_start, star.key.id.as_str()))
        .max_by_key(|star| star.key.id.as_str().len())
        .ok_or_else(|| {
            app_error(format!(
                "belt-search start {requested_start:?} does not resolve to a star in the catalogue"
            ))
        })
}

fn candidate_route_distance(start: GalacticPosition, route: &[RouteCandidate]) -> f64 {
    let mut previous = start;
    let mut total = 0.0;
    for candidate in route {
        total += position_distance(previous, candidate.position);
        previous = candidate.position;
    }
    total
}

fn improve_candidate_route_2opt(
    start: GalacticPosition,
    route: &mut [RouteCandidate],
    max_passes: usize,
) -> usize {
    if route.len() < 3 {
        return 0;
    }

    let mut swaps = 0;
    for _ in 0..max_passes {
        let mut improved = false;
        for left in 0..route.len() - 1 {
            for right in left + 1..route.len() {
                let previous = if left == 0 {
                    start
                } else {
                    route[left - 1].position
                };
                let old_before = position_distance(previous, route[left].position);
                let new_before = position_distance(previous, route[right].position);
                let (old_after, new_after) = if right + 1 < route.len() {
                    let next = route[right + 1].position;
                    (
                        position_distance(route[right].position, next),
                        position_distance(route[left].position, next),
                    )
                } else {
                    (0.0, 0.0)
                };

                if new_before + new_after + 1e-9 < old_before + old_after {
                    route[left..=right].reverse();
                    swaps += 1;
                    improved = true;
                }
            }
        }
        if !improved {
            break;
        }
    }
    swaps
}

fn position_distance(left: GalacticPosition, right: GalacticPosition) -> f64 {
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let dz = left.z - right.z;
    (dx * dx + dy * dy + dz * dz).sqrt()
}

fn print_route_plan(plan: &BeltRoutePlan, include_explored: bool) {
    let unexplored = plan.stops.iter().filter(|stop| !stop.explored).count();
    let explored = plan.stops.len().saturating_sub(unexplored);
    println!("Route plan:");
    if plan.requested_start == plan.start_system {
        println!("  Start: {}", plan.start_system);
    } else {
        println!(
            "  Start: {} (system {})",
            plan.requested_start, plan.start_system
        );
    }
    println!("  Radius: {:.2} ly", plan.radius_ly);
    println!("  Stops: {}", plan.stops.len());
    println!("  New scans: {unexplored}");
    if explored > 0 {
        println!(
            "  Known visits: {explored}{}",
            if include_explored {
                " (included by request)"
            } else {
                " (route anchor)"
            }
        );
    }
    println!("  Route distance: {:.2} ly", plan.optimized_distance_ly);
    println!(
        "  Optimization: {:.2} -> {:.2} ly ({} 2-opt swap(s))",
        plan.nearest_neighbor_distance_ly, plan.optimized_distance_ly, plan.two_opt_swaps
    );
    println!("  Route:");
    for (index, stop) in plan.stops.iter().enumerate() {
        println!(
            "    {:>3}. {:<18} leg {:>7.2} ly  radius {:>7.2} ly  {}",
            index + 1,
            stop.system,
            stop.leg_distance_ly,
            stop.distance_from_start_ly,
            if stop.explored { "known" } else { "scan" }
        );
    }
}

async fn resolve_owned_replicant(client: &Client, requested: &str) -> crate::AnyResult<Replicant> {
    let handles = client.replicants().find().owned().collect().await?;
    let mut matches = Vec::new();
    for handle in handles {
        let replicant = handle.snapshot().await?;
        if replicant.key.id.as_str().eq_ignore_ascii_case(requested)
            || replicant
                .name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(requested))
        {
            matches.push(replicant);
        }
    }
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(app_error(format!(
            "owned replicant {requested:?} was not found"
        ))),
        _ => Err(app_error(format!(
            "owned replicant name {requested:?} is ambiguous; use its code"
        ))),
    }
}

async fn travel_to_system(
    client: &Client,
    replicant_code: &str,
    destination: &str,
    wait_timeout: Duration,
) -> crate::AnyResult<()> {
    let mut handle = client.replicants().get_owned(replicant_code).await?;
    let mut snapshot = handle.snapshot().await?;
    let mut departure_origin = snapshot
        .location
        .as_ref()
        .map(|location| location.id.as_str().to_owned());

    if snapshot.travel.is_none()
        && snapshot
            .location
            .as_ref()
            .is_some_and(|location| designation_in_system(location.id.as_str(), destination))
    {
        info!(
            replicant = %replicant_code,
            destination,
            "belt-search replicant is already in target system"
        );
        return Ok(());
    }

    if let Some(travel) = &snapshot.travel {
        let planned_destination = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        if !planned_destination.is_some_and(|planned| designation_in_system(planned, destination)) {
            return Err(app_error(format!(
                "replicant {replicant_code} is already traveling to {planned_destination:?}, not system {destination}"
            )));
        }
        info!(
            replicant = %replicant_code,
            destination,
            "resuming existing belt-search travel"
        );
    } else {
        info!(
            replicant = %replicant_code,
            destination,
            "dispatching belt-search travel"
        );
        let operation = handle.travel().to(destination).depart().await?;
        ensure_operation_accepted(&operation).await?;
    }

    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + wait_timeout;
    loop {
        snapshot = handle.snapshot().await?;
        let location = snapshot
            .location
            .as_ref()
            .map(|location| location.id.as_str());
        if snapshot.travel.is_none()
            && location.is_some_and(|location| designation_in_system(location, destination))
        {
            info!(
                replicant = %replicant_code,
                destination,
                location = ?location,
                "belt-search travel arrived"
            );
            return Ok(());
        }

        if snapshot.travel.is_none()
            && let Some(location) = location
            && departure_origin.as_deref() != Some(location)
        {
            info!(
                replicant = %replicant_code,
                intermediate = %location,
                destination,
                "continuing belt-search travel from intermediate waypoint"
            );
            departure_origin = Some(location.to_owned());
            let operation = handle.travel().to(destination).depart().await?;
            ensure_operation_accepted(&operation).await?;
            continue;
        }

        if Instant::now() >= deadline {
            return Err(app_error(format!(
                "timed out waiting for replicant {replicant_code} in system {destination}"
            )));
        }

        let eta_seconds = snapshot
            .travel
            .as_ref()
            .and_then(|travel| travel.eta_seconds);
        match wait_for_replicant_travel_event(
            &mut watch,
            deadline,
            replicant_code,
            travel_poll_interval(eta_seconds),
        )
        .await?
        {
            TravelWake::Event => {}
            TravelWake::Poll | TravelWake::Gap => {
                handle = handle.refresh().await?;
                let refreshed = handle.snapshot().await?;
                info!(
                    replicant = %replicant_code,
                    destination,
                    location = ?refreshed.location.as_ref().map(|location| location.id.as_str()),
                    traveling = refreshed.travel.is_some(),
                    eta_seconds = ?refreshed.travel.as_ref().and_then(|travel| travel.eta_seconds),
                    "authoritatively refreshed belt-search travel"
                );
            }
        }
    }
}

async fn system_is_explored(
    client: &Client,
    replicant_code: &str,
    system: &str,
) -> crate::AnyResult<bool> {
    let locally_explored = client
        .galaxy()
        .replicant_star_knowledge(replicant_code)
        .into_iter()
        .any(|knowledge| knowledge.star.id.as_str() == system && knowledge.explored == Some(true));
    if locally_explored {
        return Ok(true);
    }
    Ok(client
        .galaxy()
        .refresh_replicant_star(replicant_code, system)
        .await?
        .explored
        == Some(true))
}

async fn scan_system(client: &Client, replicant_code: &str, system: &str) -> crate::AnyResult<()> {
    info!(
        replicant = %replicant_code,
        system,
        endpoint = "POST /v1/replicants/{code}/scan",
        "belt-search scanning system"
    );
    let handle = client.replicants().get_owned(replicant_code).await?;
    let operation = handle.scan().await?;
    let outcome = operation.outcome().await?;
    if matches!(
        outcome.status,
        OperationStatus::Rejected | OperationStatus::Cancelled | OperationStatus::Failed
    ) {
        return Err(app_error(format!(
            "belt-search system scan for {system} ended as {:?}: {:?}",
            outcome.status, outcome.response
        )));
    }

    if !matches!(
        outcome.status,
        OperationStatus::ReconciliationRequired | OperationStatus::Completed
    ) {
        let knowledge = client
            .galaxy()
            .refresh_replicant_star(replicant_code, system)
            .await?;
        if knowledge.explored != Some(true) {
            return Err(app_error(format!(
                "belt-search scan operation {} for {system} is {:?}, and targeted star knowledge does not confirm completion; rerun to reconcile without submitting a blind duplicate",
                operation.id(),
                outcome.status
            )));
        }
    } else if let Err(error) = client
        .galaxy()
        .refresh_replicant_star(replicant_code, system)
        .await
    {
        warn!(
            replicant = %replicant_code,
            system,
            operation_id = %operation.id(),
            error = %error,
            "belt-search scan succeeded but the star-knowledge refresh failed"
        );
    }

    Ok(())
}

fn belts_from_location(system: &str, location: &Location) -> Vec<BeltReport> {
    let Some(asteroid_belt) = location.unknown.get("asteroid_belt") else {
        return Vec::new();
    };
    asteroid_belt
        .get("belts")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(asteroid_belt))
        .iter()
        .filter_map(|value| parse_belt(system, value))
        .collect()
}

fn parse_belt(system: &str, value: &Value) -> Option<BeltReport> {
    let object = value.as_object()?;
    let designation = object.get("designation")?.as_str()?.to_owned();
    let density = object
        .get("density")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let resources = object
        .get("resources")
        .and_then(Value::as_object)
        .map(|resources| {
            resources
                .iter()
                .map(|(resource, scarcity)| {
                    let scarcity = scarcity
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| scarcity.to_string());
                    (resource.clone(), scarcity)
                })
                .collect()
        })
        .unwrap_or_default();

    Some(BeltReport {
        system: system.to_owned(),
        designation,
        density,
        inner_radius_au: object.get("inner_radius_au").and_then(Value::as_f64),
        outer_radius_au: object.get("outer_radius_au").and_then(Value::as_f64),
        resources,
    })
}

fn print_summary(systems: &[String], belts: &[BeltReport]) {
    let systems_with_belts = belts
        .iter()
        .map(|belt| belt.system.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    let dense = belts
        .iter()
        .filter(|belt| belt.density.eq_ignore_ascii_case("dense"))
        .count();
    let moderate = belts
        .iter()
        .filter(|belt| belt.density.eq_ignore_ascii_case("moderate"))
        .count();
    let sparse = belts
        .iter()
        .filter(|belt| belt.density.eq_ignore_ascii_case("sparse"))
        .count();

    println!(
        "Belt search complete: {} system(s), {} belt(s) in {} system(s) [dense {}, moderate {}, sparse {}]",
        systems.len(),
        belts.len(),
        systems_with_belts,
        dense,
        moderate,
        sparse
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TravelWake {
    Event,
    Poll,
    Gap,
}

async fn wait_for_replicant_travel_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    replicant_code: &str,
    poll_interval: Duration,
) -> crate::AnyResult<TravelWake> {
    let poll_deadline = Instant::now() + poll_interval;
    loop {
        let now = Instant::now();
        let remaining = deadline
            .saturating_duration_since(now)
            .min(poll_deadline.saturating_duration_since(now));
        if remaining.is_zero() {
            return Ok(TravelWake::Poll);
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event))
                if event.name.as_str() == "travel.arrived"
                    && event
                        .replicant
                        .as_ref()
                        .is_some_and(|replicant| replicant.id.as_str() == replicant_code) =>
            {
                return Ok(TravelWake::Event);
            }
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(TravelWake::Poll),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; refreshing belt-search travel");
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

async fn ensure_operation_accepted(operation: &Operation) -> crate::AnyResult<()> {
    let outcome = operation.outcome().await?;
    if matches!(
        outcome.status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(app_error(format!(
            "operation {} ended as {:?}: {:?}",
            operation.id(),
            outcome.status,
            outcome.response
        )));
    }
    Ok(())
}

fn designation_in_system(designation: &str, system: &str) -> bool {
    designation == system
        || designation
            .strip_prefix(system)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

fn density_rank(value: &str) -> u8 {
    match value.to_ascii_lowercase().as_str() {
        "dense" => 3,
        "moderate" => 2,
        "sparse" => 1,
        _ => 0,
    }
}

fn normalize_system(value: &str) -> crate::AnyResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(app_error("system designation must not be empty"));
    }
    if value.chars().any(char::is_whitespace) {
        return Err(app_error(format!(
            "invalid system designation {value:?}: whitespace is not allowed"
        )));
    }
    Ok(value.to_ascii_uppercase())
}

fn parse_positive_f64(option: &str, value: &str) -> crate::AnyResult<f64> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| app_error(format!("{option} must be a positive number, got {value:?}")))?;
    if !parsed.is_finite() || parsed <= 0.0 {
        return Err(app_error(format!(
            "{option} must be a finite number greater than zero"
        )));
    }
    Ok(parsed)
}

fn parse_positive_usize(option: &str, value: &str) -> crate::AnyResult<usize> {
    let parsed = value.parse::<usize>().map_err(|_| {
        app_error(format!(
            "{option} must be a positive integer, got {value:?}"
        ))
    })?;
    if parsed == 0 {
        return Err(app_error(format!("{option} must be greater than zero")));
    }
    Ok(parsed)
}

fn parse_systems_text(value: &str) -> crate::AnyResult<Vec<String>> {
    value
        .split(|character: char| character.is_whitespace() || character == ',')
        .filter(|value| !value.is_empty())
        .map(normalize_system)
        .collect()
}

fn unique_preserving_order(values: Vec<String>) -> Vec<String> {
    let mut seen = BTreeSet::new();
    values
        .into_iter()
        .filter(|value| seen.insert(value.clone()))
        .collect()
}

fn next_value(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> crate::AnyResult<String> {
    arguments
        .next()
        .ok_or_else(|| app_error(format!("{option} requires a value")))
}

fn env_bool(name: &str, default: bool) -> crate::AnyResult<bool> {
    let Ok(value) = env::var(name) else {
        return Ok(default);
    };
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(app_error(format!(
            "{name} must be one of 1/0, true/false, yes/no, or on/off"
        ))),
    }
}

fn init_logging(config: &Config) -> crate::AnyResult<()> {
    if !config.verbose && config.log_file.is_none() {
        return Ok(());
    }
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(
            "warn,replicant_cli::belt_search=info,replicant_client::ops=info,replicant_client::raw::http=warn",
        )
    });
    match (&config.log_file, config.verbose) {
        (None, true) => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
            .try_init()
            .map_err(|error| app_error(error.to_string()))?,
        (Some(path), verbose) => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new().create(true).append(true).open(path)?;
            let registry = tracing_subscriber::registry().with(filter);
            if verbose {
                registry
                    .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_ansi(false)
                            .with_writer(std::sync::Mutex::new(file)),
                    )
                    .try_init()
                    .map_err(|error| app_error(error.to_string()))?;
            } else {
                registry
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_ansi(false)
                            .with_writer(std::sync::Mutex::new(file)),
                    )
                    .try_init()
                    .map_err(|error| app_error(error.to_string()))?;
            }
        }
        (None, false) => {}
    }
    Ok(())
}

fn print_help() {
    println!(
        "Asteroid belt search\n\n\
Usage:\n  replicant-cli belt-search [OPTIONS] SYSTEM...\n  replicant-cli belt-search [OPTIONS] --systems-file PATH\n  replicant-cli belt-search [OPTIONS] --start LOCATION|SYSTEM --range LY\n\n\
Fast Replicant-only asteroid-belt scouting. Explicit mode visits the supplied systems in order. Automatic route mode selects catalogue systems inside the requested radius, skips already-explored systems by default, builds the same nearest-neighbor + bounded 2-opt style route used by survey planning, anchors it at --start, and then quick-scans each route stop. No survey controller or drones are used.\n\n\
Route planning:\n  --start LOCATION|SYSTEM    Route anchor and radius centre\n  --range LY                 Radius around --start in light years\n  --radius LY                Alias for --range\n  --system-limit N           Maximum route stops including start (default: 80)\n  --include-explored         Include known systems in an automatic route (never rescans them)\n  --plan-only, --plan        Print the planned route without travelling or scanning\n\n\
General options:\n  --replicant NAME_OR_CODE  Scout replicant (default: Chats-4)\n  --systems-file PATH       Read whitespace/comma-separated explicit systems; repeatable\n  --database PATH           Managed SQLite database [env: REPLICANT_DB]\n  --wait-timeout-secs N     Per-travel timeout (default: 21600)\n  --verbose                 Show tracing logs in the terminal\n  --log-file PATH           Append detailed logs to a file\n  -h, --help                Show this help\n\n\
Examples:\n  replicant-cli belt-search SOL YINU MENKUNT\n\n  replicant-cli belt-search --start SCEPTURUM --range 30\n\n  replicant-cli belt-search --start SCEPTURUM-BELT-1 --range 30 \\\n    --system-limit 50 --plan-only\n\n  replicant-cli belt-search --start THYFFAWFF --range 20 \\\n    --replicant Chats-4 --log-file logs/belt-search.log"
    );
}

fn app_error(message: impl Into<String>) -> crate::AnyError {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_client::domain::{StarId, StarKey};

    fn star(name: &str, position: [f64; 3]) -> Star {
        Star {
            key: StarKey::live(StarId::from(name)),
            name: None,
            spectral_type: None,
            entry_point: None,
            position: Some(GalacticPosition {
                x: position[0],
                y: position[1],
                z: position[2],
            }),
            has_hub: None,
            region: None,
        }
    }

    #[test]
    fn parses_and_deduplicates_system_lists() {
        let parsed = parse_systems_text("sol, yinu\nMENKUNT sol").unwrap();
        assert_eq!(
            unique_preserving_order(parsed),
            vec!["SOL", "YINU", "MENKUNT"]
        );
    }

    #[test]
    fn child_locations_match_their_system() {
        assert!(designation_in_system("SOL", "SOL"));
        assert!(designation_in_system("SOL-5-L4", "SOL"));
        assert!(!designation_in_system("SOLA-1", "SOL"));
    }

    #[test]
    fn parses_belt_details() {
        let location = Location {
            key: replicant_client::domain::LocationKey::live("SOL".into()),
            location_type: None,
            scanned: None,
            system_scanned: Some(true),
            system_tags: Vec::new(),
            system: Some("SOL".into()),
            parent: None,
            survey_progress: Default::default(),
            environment: Default::default(),
            unknown: BTreeMap::from([(
                "asteroid_belt".into(),
                serde_json::json!({
                    "present": true,
                    "belts": [{
                        "density": "dense",
                        "designation": "SOL-BELT-1",
                        "inner_radius_au": 0.6,
                        "outer_radius_au": 0.9,
                        "resources": {"carbon": "rich"}
                    }]
                }),
            )]),
        };

        let belts = belts_from_location("SOL", &location);
        assert_eq!(belts.len(), 1);
        assert_eq!(belts[0].designation, "SOL-BELT-1");
        assert_eq!(belts[0].density_rank(), 3);
        assert_eq!(belts[0].resources["carbon"], "rich");
    }
    #[test]
    fn route_start_accepts_child_location_designations() {
        let catalogue = vec![
            star("SCEPTURUM", [0.0, 0.0, 0.0]),
            star("SOL", [1.0, 0.0, 0.0]),
        ];
        let resolved = resolve_route_start(&catalogue, "SCEPTURUM-BELT-1").unwrap();
        assert_eq!(resolved.key.id.as_str(), "SCEPTURUM");
    }

    #[test]
    fn two_opt_shortens_crossing_belt_route() {
        let start = GalacticPosition {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        };
        let mut route = vec![
            RouteCandidate {
                system: "A".into(),
                position: GalacticPosition {
                    x: 10.0,
                    y: 0.0,
                    z: 0.0,
                },
                distance_from_start_ly: 10.0,
                explored: false,
            },
            RouteCandidate {
                system: "B".into(),
                position: GalacticPosition {
                    x: 0.0,
                    y: 10.0,
                    z: 0.0,
                },
                distance_from_start_ly: 10.0,
                explored: false,
            },
            RouteCandidate {
                system: "C".into(),
                position: GalacticPosition {
                    x: 10.0,
                    y: 10.0,
                    z: 0.0,
                },
                distance_from_start_ly: 14.142_135_623_7,
                explored: false,
            },
        ];
        let before = candidate_route_distance(start, &route);
        assert!(improve_candidate_route_2opt(start, &mut route, 8) > 0);
        assert!(candidate_route_distance(start, &route) < before);
    }

    #[test]
    fn positive_route_parameters_reject_zero_and_non_finite_values() {
        assert_eq!(parse_positive_usize("--system-limit", "80").unwrap(), 80);
        assert!(parse_positive_usize("--system-limit", "0").is_err());
        assert_eq!(parse_positive_f64("--range", "30").unwrap(), 30.0);
        assert!(parse_positive_f64("--range", "0").is_err());
        assert!(parse_positive_f64("--range", "NaN").is_err());
    }
}
