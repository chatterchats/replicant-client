//! Lightweight asteroid-belt scouting with a Replicant only.
//!
//! The command visits the requested systems in order, performs the Replicant's
//! instant system scan when the system is not already explored, records the
//! asteroid-belt summary, and immediately moves on. No survey controller or
//! survey drones are required.

use std::{cmp::Ordering, collections::BTreeMap, io, time::Duration};

use replicant_client::{
    Client, Operation, OperationStatus, Replicant, Star,
    domain::{GalacticPosition, Location},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::{Instant, timeout};
use tracing::{info, warn};
const POLL_INTERVAL: Duration = Duration::from_secs(15);

/// One asteroid belt discovered by a system scan.
#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BeltReport {
    pub system: String,
    pub designation: String,
    pub density: String,
    pub inner_radius_au: Option<f64>,
    pub outer_radius_au: Option<f64>,
    pub resources: BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
struct RouteCandidate {
    system: String,
    position: GalacticPosition,
    distance_from_start_ly: f64,
    explored: bool,
}

/// One stop in an automatically planned belt-search route.
#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct PlannedStop {
    pub system: String,
    pub distance_from_start_ly: f64,
    pub leg_distance_ly: f64,
    pub explored: bool,
}

/// Automatically planned belt-search route.
#[allow(missing_docs)]
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BeltRoutePlan {
    pub requested_start: String,
    pub start_system: String,
    pub radius_ly: f64,
    pub stops: Vec<PlannedStop>,
    pub nearest_neighbor_distance_ly: f64,
    pub optimized_distance_ly: f64,
    pub two_opt_swaps: usize,
}

impl BeltRoutePlan {
    /// Returns the route systems in visit order.
    pub fn systems(&self) -> Vec<String> {
        self.stops.iter().map(|stop| stop.system.clone()).collect()
    }
}

impl BeltReport {
    /// Returns the stable display ordering for this belt's density.
    pub fn density_rank(&self) -> u8 {
        density_rank(&self.density)
    }

    /// Formats the known inner and outer radii.
    pub fn radii(&self) -> String {
        match (self.inner_radius_au, self.outer_radius_au) {
            (Some(inner), Some(outer)) => format!("{inner:.2}-{outer:.2} AU"),
            (Some(inner), None) => format!("{inner:.2}-? AU"),
            (None, Some(outer)) => format!("?-{outer:.2} AU"),
            (None, None) => "?".to_owned(),
        }
    }

    /// Formats the belt resources and scarcity values.
    pub fn resources(&self) -> String {
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

/// Inputs for a Replicant-only belt-search action.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BeltSearchRequest {
    /// Owned Replicant name or code.
    pub replicant: String,
    /// Explicit systems to visit when automatic routing is disabled.
    pub systems: Vec<String>,
    /// Optional automatic-route start location or system.
    pub route_start: Option<String>,
    /// Radius for automatic routing.
    pub radius_ly: Option<f64>,
    /// Maximum automatic-route stops.
    pub system_limit: usize,
    /// Whether automatic routes may include already explored systems.
    pub include_explored: bool,
    /// Return the route without traveling or scanning.
    pub plan_only: bool,
    /// Maximum time to wait for each travel leg.
    pub wait_timeout: Duration,
}

/// One completed belt-search stop.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BeltSearchStop {
    /// System inspected.
    pub system: String,
    /// Whether this invocation submitted a system scan.
    pub scanned: bool,
    /// Belts discovered in the system.
    pub belts: Vec<BeltReport>,
}

/// Typed belt-search plan and execution result.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct BeltSearchResult {
    /// Resolved Replicant code.
    pub replicant_code: String,
    /// Resolved Replicant display name.
    pub replicant_name: String,
    /// Automatic route, when requested.
    pub route: Option<BeltRoutePlan>,
    /// Systems in visit order.
    pub systems: Vec<String>,
    /// Per-system execution results; empty for plan-only requests.
    pub stops: Vec<BeltSearchStop>,
}

/// Plans and optionally executes a belt search using an existing managed client.
pub async fn execute_belt_search(
    client: &Client,
    request: &BeltSearchRequest,
) -> crate::ActionResult<BeltSearchResult> {
    validate_request(request)?;
    client.ready().await?;
    let replicant = resolve_owned_replicant(client, &request.replicant).await?;
    let replicant_code = replicant.key.id.as_str().to_owned();
    let replicant_name = replicant
        .name
        .as_deref()
        .unwrap_or(replicant_code.as_str())
        .to_owned();

    // Automatic routes that exclude explored systems need a complete
    // explored-star set before filtering candidates. Inclusive routes and
    // explicit system lists do not: they use the managed projection and
    // targeted checks at each visited system instead of traversing hundreds
    // of Replicant-star catalogue pages up front.
    if needs_complete_explored_catalogue(request)
        && let Err(error) = client.galaxy().sync_replicant_stars(&replicant_code).await
    {
        warn!(
            replicant = %replicant_code,
            error = %error,
            "belt-search could not refresh the explored-system list required for route filtering; falling back to managed knowledge"
        );
    }

    let route_plan = if let (Some(start), Some(radius_ly)) =
        (request.route_start.as_deref(), request.radius_ly)
    {
        Some(
            plan_belt_route(
                client,
                &replicant_code,
                start,
                radius_ly,
                request.system_limit,
                request.include_explored,
            )
            .await?,
        )
    } else {
        None
    };
    let systems = route_plan
        .as_ref()
        .map(BeltRoutePlan::systems)
        .unwrap_or_else(|| request.systems.clone());

    if request.plan_only {
        return Ok(BeltSearchResult {
            replicant_code,
            replicant_name,
            route: route_plan,
            systems,
            stops: Vec::new(),
        });
    }

    let auto_route = route_plan.is_some();
    let mut stops = Vec::new();
    for system in &systems {
        let already_explored = system_is_explored(client, &replicant_code, system).await?;
        let scanned_now = if already_explored {
            if auto_route {
                // Auto routes are anchored at the requested start and must stay
                // physically faithful to the planned order. Known systems are
                // therefore visited when present in the route, but never rescanned.
                travel_to_system(client, &replicant_code, system, request.wait_timeout).await?;
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
            travel_to_system(client, &replicant_code, system, request.wait_timeout).await?;
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

        stops.push(BeltSearchStop {
            system: system.clone(),
            scanned: scanned_now,
            belts,
        });
    }
    Ok(BeltSearchResult {
        replicant_code,
        replicant_name,
        route: route_plan,
        systems,
        stops,
    })
}

fn needs_complete_explored_catalogue(request: &BeltSearchRequest) -> bool {
    request.route_start.is_some() && !request.include_explored
}

fn validate_request(request: &BeltSearchRequest) -> crate::ActionResult<()> {
    if request.route_start.is_some() != request.radius_ly.is_some() {
        return Err(app_error(
            "automatic belt-search routing requires both a start and radius",
        ));
    }
    if request.route_start.is_some() && !request.systems.is_empty() {
        return Err(app_error(
            "explicit systems cannot be combined with automatic routing",
        ));
    }
    if request.route_start.is_none() && request.systems.is_empty() {
        return Err(app_error(
            "belt search requires systems or an automatic route",
        ));
    }
    if request.system_limit == 0
        || request
            .radius_ly
            .is_some_and(|radius| !radius.is_finite() || radius <= 0.0)
        || request.wait_timeout.is_zero()
    {
        return Err(app_error(
            "belt-search limits, radius, and timeout must be positive",
        ));
    }
    Ok(())
}

async fn plan_belt_route(
    client: &Client,
    replicant_code: &str,
    requested_start: &str,
    radius_ly: f64,
    system_limit: usize,
    include_explored: bool,
) -> crate::ActionResult<BeltRoutePlan> {
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
) -> crate::ActionResult<&'a Star> {
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

async fn resolve_owned_replicant(
    client: &Client,
    requested: &str,
) -> crate::ActionResult<Replicant> {
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
) -> crate::ActionResult<()> {
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
) -> crate::ActionResult<bool> {
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

async fn scan_system(
    client: &Client,
    replicant_code: &str,
    system: &str,
) -> crate::ActionResult<()> {
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
) -> crate::ActionResult<TravelWake> {
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

async fn ensure_operation_accepted(operation: &Operation) -> crate::ActionResult<()> {
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

fn app_error(message: impl Into<String>) -> crate::ApplicationError {
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
            has_ward: None,
            knowledge_observed: false,
            explored: None,
            has_life: None,
            region: None,
        }
    }

    #[test]
    fn child_locations_match_their_system() {
        assert!(designation_in_system("SOL", "SOL"));
        assert!(designation_in_system("SOL-5-L4", "SOL"));
        assert!(!designation_in_system("SOLA-1", "SOL"));
    }

    #[test]
    fn full_replicant_star_sync_is_only_needed_for_excluding_explored_auto_routes() {
        let mut request = BeltSearchRequest {
            replicant: "R-1".to_owned(),
            systems: Vec::new(),
            route_start: Some("SOL".to_owned()),
            radius_ly: Some(30.0),
            system_limit: 80,
            include_explored: false,
            plan_only: false,
            wait_timeout: Duration::from_secs(60),
        };
        assert!(needs_complete_explored_catalogue(&request));

        request.include_explored = true;
        assert!(!needs_complete_explored_catalogue(&request));

        request.route_start = None;
        request.radius_ly = None;
        request.systems = vec!["SOL".to_owned(), "VEGA".to_owned()];
        request.include_explored = false;
        assert!(!needs_complete_explored_catalogue(&request));
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
            custom_name: None,
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
    fn request_validation_rejects_incomplete_and_invalid_routes() {
        let mut request = BeltSearchRequest {
            replicant: "TEST".into(),
            systems: vec!["SOL".into()],
            route_start: None,
            radius_ly: None,
            system_limit: 80,
            include_explored: false,
            plan_only: false,
            wait_timeout: Duration::from_secs(1),
        };
        assert!(validate_request(&request).is_ok());
        request.route_start = Some("SOL".into());
        assert!(validate_request(&request).is_err());
        request.systems.clear();
        request.radius_ly = Some(f64::NAN);
        assert!(validate_request(&request).is_err());
    }
}
