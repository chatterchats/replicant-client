//! Pure, deterministic travel timing and smart route planning.
//!
//! The planner intentionally accepts only catalogue [`Star`] values.  In
//! particular, hub eligibility is a global catalogue fact (`has_hub`) and is
//! never inferred from ownership or account-relative star knowledge.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::{DeviceType, Star, StarKey};

const MAX_INTERMEDIATE_HUBS: usize = 8;
const MIN_SAVING_SECONDS: u64 = 2;

/// The measured propulsion profile used for a travel estimate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TravelProfile {
    /// Standard propulsion, measured at 30 seconds per light-year.
    Standard,
    /// Cargo Vessel propulsion. Its documented 40% speed reduction is a
    /// `0.6×` speed multiplier, equivalent to 50 seconds per light-year.
    Cargo,
    /// Racing Vessel propulsion, measured at `30 / 1.35` seconds per
    /// light-year.
    Racing,
}

impl TravelProfile {
    /// Returns the standard propulsion profile.
    #[must_use]
    pub const fn standard() -> Self {
        Self::Standard
    }

    /// Returns the Cargo Vessel propulsion profile.
    #[must_use]
    pub const fn cargo() -> Self {
        Self::Cargo
    }

    /// Returns the Racing Vessel propulsion profile.
    #[must_use]
    pub const fn racing() -> Self {
        Self::Racing
    }

    /// Selects a profile from an optional device type.
    ///
    /// Racing and Cargo Vessels use their documented propulsion modifiers.
    /// Cargo Freighters and every other known or unknown device type use the
    /// experimentally measured standard profile.
    #[must_use]
    pub fn for_device_type(device_type: Option<&DeviceType>) -> Self {
        match device_type {
            Some(DeviceType::CargoVessel) => Self::Cargo,
            Some(DeviceType::RacingVessel) => Self::Racing,
            _ => Self::Standard,
        }
    }

    /// Computes measured travel time for one directed leg.
    ///
    /// `distance_ly` is first converted to the profile's pre-surge integer
    /// time, then a destination hub applies the integer `3 / 4` reduction.
    /// Returning `None` means the distance is invalid or cannot be represented
    /// as a finite, non-negative number of seconds.
    #[must_use]
    pub fn surge_seconds(self, distance_ly: f64, destination_has_hub: bool) -> Option<u64> {
        surge_seconds(distance_ly, self, destination_has_hub)
    }
}

impl From<DeviceType> for TravelProfile {
    fn from(device_type: DeviceType) -> Self {
        Self::for_device_type(Some(&device_type))
    }
}

impl From<&DeviceType> for TravelProfile {
    fn from(device_type: &DeviceType) -> Self {
        Self::for_device_type(Some(device_type))
    }
}

/// Computes the measured, destination-directed duration of one travel leg.
///
/// The baseline is `floor(distance * 30)`. Cargo Vessel timing uses its
/// documented `0.6×` speed multiplier, or `30 / 0.6` seconds per light-year.
/// Racing Vessel timing uses the measured `1.35×` speed multiplier. Each
/// profile is truncated before a destination hub applies
/// `floor(pre_surge * 0.75)`, represented exactly as integer `3 / 4`.
/// Invalid, negative, or non-finite distances return `None`.
#[must_use]
pub fn surge_seconds(
    distance_ly: f64,
    profile: TravelProfile,
    destination_has_hub: bool,
) -> Option<u64> {
    if !distance_ly.is_finite() || distance_ly < 0.0 {
        return None;
    }

    let scaled = match profile {
        TravelProfile::Standard => distance_ly * 30.0,
        TravelProfile::Cargo => distance_ly * 30.0 / 0.6,
        TravelProfile::Racing => distance_ly * 30.0 / 1.35,
    };
    if !scaled.is_finite() {
        return None;
    }
    let pre_surge = scaled.floor();
    if pre_surge < 0.0 || pre_surge > u64::MAX as f64 {
        return None;
    }
    let pre_surge = pre_surge as u64;
    if destination_has_hub {
        // Avoid overflow from `pre_surge * 3`; this is exactly floor(n * 3/4).
        Some((pre_surge / 4) * 3 + ((pre_surge % 4) * 3) / 4)
    } else {
        Some(pre_surge)
    }
}

/// A deterministic local smart-travel selection.
#[derive(Clone, Debug, PartialEq)]
pub struct SmartTravelPlan {
    /// Full route designations, including origin and destination.
    pub systems: Vec<String>,
    /// Route designations between the origin and destination.
    pub intermediate_systems: Vec<String>,
    /// Direct interstellar surge duration. Local departure and final-arrival
    /// legs are common to every candidate for the same request and therefore
    /// do not affect route ordering.
    pub direct_seconds: u64,
    /// Interstellar surge duration of the selected route.
    pub estimated_seconds: u64,
    /// Sum of geometric leg distances in light-years.
    pub total_distance_ly: f64,
    /// Seconds saved against the direct route (zero for a direct selection).
    pub saved_seconds: u64,
    /// Whether the selected route has no intermediate systems.
    pub is_direct: bool,
}

impl SmartTravelPlan {
    /// Converts the selected system route into the explicit waypoint sequence
    /// accepted by the travel API for `destination`.
    ///
    /// The route planner intentionally keeps the destination system out of
    /// `intermediate_systems`. That is sufficient when the final destination
    /// is the star designation itself, but an explicit `via` route ending at
    /// a planet/belt/Lagrange point in another system must include that final
    /// star as the last interstellar waypoint. Otherwise the server is asked
    /// to travel directly from the last hub to a body in a different system.
    #[must_use]
    pub fn explicit_waypoints_for(&self, destination: &str) -> Vec<String> {
        let mut waypoints = self.intermediate_systems.clone();
        let Some(origin_system) = self.systems.first() else {
            return waypoints;
        };
        let Some(destination_system) = self.systems.last() else {
            return waypoints;
        };
        if origin_system.eq_ignore_ascii_case(destination_system)
            || destination.eq_ignore_ascii_case(destination_system)
        {
            return waypoints;
        }

        let is_local_destination = destination
            .strip_prefix(destination_system)
            .is_some_and(|suffix| suffix.starts_with('-'));
        if is_local_destination
            && !waypoints
                .last()
                .is_some_and(|waypoint| waypoint.eq_ignore_ascii_case(destination_system))
        {
            waypoints.push(destination_system.clone());
        }
        waypoints
    }
}

/// Pure bounded route planner over global catalogue hubs.
#[derive(Clone, Copy, Debug, Default)]
pub struct SmartTravelPlanner;

impl SmartTravelPlanner {
    /// Creates the deterministic default planner.
    #[must_use]
    pub const fn default() -> Self {
        Self
    }

    /// Selects a route from `origin` to `destination` using catalogue hubs.
    ///
    /// Only stars with `has_hub == Some(true)` in `catalogue` are eligible as
    /// intermediate systems.  Positions must be finite and present.  At most
    /// eight intermediate hubs are considered, and routes never revisit a
    /// system.  An alternate route is selected only when it saves at least
    /// two seconds; otherwise the valid direct plan is returned.
    ///
    /// Catalogue `entry_point` values intentionally do not enter this
    /// comparison. A request has the same local departure leg and final local
    /// arrival leg for every candidate, while each native system waypoint
    /// arrives at and departs from that system's entry point. Those fixed
    /// local terms cancel when comparing routes. The only existing local-time
    /// values are request-relative server response fields in the raw galaxy,
    /// location, and travel DTOs; there is no deterministic AU-to-seconds
    /// estimator to reuse, so this planner does not invent one.
    #[must_use]
    pub fn plan(
        &self,
        origin: &Star,
        destination: &Star,
        catalogue: &[Star],
        profile: TravelProfile,
    ) -> Option<SmartTravelPlan> {
        let origin_position = valid_position(origin)?;
        let destination_position = valid_position(destination)?;

        let direct_distance = distance(origin_position, destination_position)?;
        let direct_seconds =
            surge_seconds(direct_distance, profile, destination.has_hub == Some(true))?;

        if origin.key == destination.key {
            return Some(make_plan(
                std::slice::from_ref(&origin.key),
                direct_distance,
                direct_seconds,
                direct_seconds,
            ));
        }

        let mut hubs: Vec<&Star> = catalogue
            .iter()
            .filter(|star| {
                star.key != origin.key
                    && star.key != destination.key
                    && star.has_hub == Some(true)
                    && valid_position(star).is_some()
            })
            .collect();
        hubs.sort_by(|left, right| left.key.cmp(&right.key));
        hubs.dedup_by(|left, right| left.key == right.key);

        let mut nodes = Vec::with_capacity(hubs.len() + 2);
        nodes.push(Node {
            key: origin.key.clone(),
            position: origin_position,
            has_hub: origin.has_hub == Some(true),
        });
        for star in hubs {
            // `valid_position` was checked above, so this cannot fail.
            nodes.push(Node {
                key: star.key.clone(),
                position: valid_position(star).expect("validated catalogue position"),
                has_hub: true,
            });
        }
        let destination_index = nodes.len();
        nodes.push(Node {
            key: destination.key.clone(),
            position: destination_position,
            has_hub: destination.has_hub == Some(true),
        });

        let mut best: Vec<Vec<Option<Route>>> = (0..nodes.len())
            .map(|_| (0..=MAX_INTERMEDIATE_HUBS).map(|_| None).collect())
            .collect();
        let initial = Route {
            node: 0,
            hubs: 0,
            seconds: 0,
            distance: 0.0,
            path: vec![0],
        };
        best[0][0] = Some(initial.clone());
        let mut queue = BinaryHeap::new();
        queue.push(QueueEntry(initial));
        let mut destination_route: Option<Route> = None;

        while let Some(QueueEntry(route)) = queue.pop() {
            let Some(known) = best[route.node][route.hubs].as_ref() else {
                continue;
            };
            if known.quality_cmp(&route, &nodes) != Ordering::Equal {
                continue;
            }
            if route.node == destination_index {
                if destination_route
                    .as_ref()
                    .is_none_or(|current| route.quality_cmp(current, &nodes) == Ordering::Less)
                {
                    destination_route = Some(route);
                }
                continue;
            }

            for target in 1..nodes.len() {
                if route.path.contains(&target) {
                    continue;
                }
                let target_is_destination = target == destination_index;
                if !target_is_destination && route.hubs == MAX_INTERMEDIATE_HUBS {
                    continue;
                }
                let leg_distance = distance(nodes[route.node].position, nodes[target].position)?;
                let leg_seconds = surge_seconds(leg_distance, profile, nodes[target].has_hub)?;
                let next_hubs = route.hubs + usize::from(!target_is_destination);
                let candidate = Route {
                    node: target,
                    hubs: next_hubs,
                    seconds: route.seconds.checked_add(leg_seconds)?,
                    distance: route.distance + leg_distance,
                    path: {
                        let mut path = route.path.clone();
                        path.push(target);
                        path
                    },
                };
                let replace = best[target][next_hubs]
                    .as_ref()
                    .is_none_or(|known| candidate.quality_cmp(known, &nodes) == Ordering::Less);
                if replace {
                    best[target][next_hubs] = Some(candidate.clone());
                    queue.push(QueueEntry(candidate));
                }
            }
        }

        let selected = destination_route.unwrap_or(Route {
            node: destination_index,
            hubs: 0,
            seconds: direct_seconds,
            distance: direct_distance,
            path: vec![0, destination_index],
        });
        let saving = direct_seconds.saturating_sub(selected.seconds);
        let use_selected = selected.hubs > 0 && saving >= MIN_SAVING_SECONDS;
        if use_selected {
            Some(make_plan(
                &selected
                    .path
                    .iter()
                    .map(|index| nodes[*index].key.clone())
                    .collect::<Vec<_>>(),
                selected.distance,
                direct_seconds,
                selected.seconds,
            ))
        } else {
            Some(make_plan(
                &[origin.key.clone(), destination.key.clone()],
                direct_distance,
                direct_seconds,
                direct_seconds,
            ))
        }
    }
}

#[derive(Clone)]
struct Node {
    key: StarKey,
    position: [f64; 3],
    has_hub: bool,
}

#[derive(Clone)]
struct Route {
    node: usize,
    hubs: usize,
    seconds: u64,
    distance: f64,
    path: Vec<usize>,
}

impl Route {
    fn quality_cmp(&self, other: &Self, nodes: &[Node]) -> Ordering {
        self.seconds
            .cmp(&other.seconds)
            .then_with(|| self.hubs.cmp(&other.hubs))
            .then_with(|| self.distance.total_cmp(&other.distance))
            .then_with(|| {
                self.path
                    .iter()
                    .map(|index| &nodes[*index].key)
                    .cmp(other.path.iter().map(|index| &nodes[*index].key))
            })
    }
}

struct QueueEntry(Route);

impl PartialEq for QueueEntry {
    fn eq(&self, other: &Self) -> bool {
        self.0.seconds == other.0.seconds
            && self.0.hubs == other.0.hubs
            && self.0.distance.total_cmp(&other.0.distance) == Ordering::Equal
            && self.0.path == other.0.path
    }
}
impl Eq for QueueEntry {}
impl Ord for QueueEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; reverse quality for a min-heap.
        other
            .0
            .seconds
            .cmp(&self.0.seconds)
            .then_with(|| other.0.hubs.cmp(&self.0.hubs))
            .then_with(|| other.0.distance.total_cmp(&self.0.distance))
            .then_with(|| other.0.path.cmp(&self.0.path))
    }
}
impl PartialOrd for QueueEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn valid_position(star: &Star) -> Option<[f64; 3]> {
    let position = star.position.as_ref()?;
    let values = [position.x, position.y, position.z];
    values
        .iter()
        .all(|value| value.is_finite())
        .then_some(values)
}

fn distance(left: [f64; 3], right: [f64; 3]) -> Option<f64> {
    let dx = left[0] - right[0];
    let dy = left[1] - right[1];
    let dz = left[2] - right[2];
    let distance = dx.hypot(dy).hypot(dz);
    distance.is_finite().then_some(distance)
}

fn make_plan(
    keys: &[StarKey],
    total_distance_ly: f64,
    direct_seconds: u64,
    estimated_seconds: u64,
) -> SmartTravelPlan {
    let systems: Vec<String> = keys.iter().map(|key| key.id.as_str().to_owned()).collect();
    let intermediate_systems: Vec<String> = systems
        .iter()
        .skip(1)
        .take(systems.len().saturating_sub(2))
        .cloned()
        .collect();
    SmartTravelPlan {
        is_direct: intermediate_systems.is_empty(),
        systems,
        intermediate_systems,
        direct_seconds,
        estimated_seconds,
        total_distance_ly,
        saved_seconds: direct_seconds.saturating_sub(estimated_seconds),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{GalacticPosition, Realm, StarId};

    fn star(id: &str, x: f64, hub: bool) -> Star {
        Star {
            key: StarKey::in_realm(Realm::Live, StarId::from(id)),
            name: None,
            spectral_type: None,
            entry_point: None,
            position: Some(GalacticPosition { x, y: 0.0, z: 0.0 }),
            has_hub: Some(hub),
            has_ward: None,
            knowledge_observed: false,
            explored: None,
            has_life: None,
            region: None,
        }
    }

    #[test]
    fn timing_truncates_before_destination_hub_surge() {
        assert_eq!(
            surge_seconds(1.0, TravelProfile::standard(), false),
            Some(30)
        );
        assert_eq!(
            surge_seconds(1.0, TravelProfile::standard(), true),
            Some(22)
        );
        assert_eq!(
            surge_seconds(0.01, TravelProfile::standard(), false),
            Some(0)
        );
        assert_eq!(
            surge_seconds(0.01, TravelProfile::standard(), true),
            Some(0)
        );
        assert_eq!(surge_seconds(2.0, TravelProfile::racing(), false), Some(44));
        assert_eq!(surge_seconds(2.0, TravelProfile::racing(), true), Some(33));
    }

    #[test]
    fn measured_regression_fixtures_match_observed_seconds() {
        assert_eq!(
            surge_seconds(180.997, TravelProfile::standard(), true),
            Some(4_071)
        );
        assert_eq!(
            surge_seconds(178.9404, TravelProfile::standard(), true),
            Some(4_026)
        );
        assert_eq!(
            surge_seconds(340.5487, TravelProfile::racing(), true),
            Some(5_675)
        );
    }

    #[test]
    fn timing_is_destination_directed() {
        assert_eq!(
            surge_seconds(10.0, TravelProfile::standard(), true),
            Some(225)
        );
        assert_eq!(
            surge_seconds(10.0, TravelProfile::standard(), false),
            Some(300)
        );
    }

    #[test]
    fn cargo_vessel_uses_documented_speed_reduction_before_hub_assistance() {
        assert_eq!(
            surge_seconds(10.0, TravelProfile::cargo(), false),
            Some(500)
        );
        assert_eq!(surge_seconds(10.0, TravelProfile::cargo(), true), Some(375));
        assert_eq!(surge_seconds(0.03, TravelProfile::cargo(), true), Some(0));
    }

    #[test]
    fn device_profiles_distinguish_cargo_vessels_from_freighters() {
        assert_eq!(
            TravelProfile::for_device_type(Some(&DeviceType::RacingVessel)),
            TravelProfile::Racing
        );
        assert_eq!(
            TravelProfile::for_device_type(Some(&DeviceType::CargoVessel)),
            TravelProfile::Cargo
        );
        assert_eq!(
            TravelProfile::for_device_type(Some(&DeviceType::CargoFreighter)),
            TravelProfile::Standard
        );
        assert_eq!(
            TravelProfile::for_device_type(None),
            TravelProfile::Standard
        );
    }

    #[test]
    fn direct_and_one_hub_routes_are_selected() {
        let origin = star("A", 0.0, false);
        let destination = star("D", 10.0, false);
        let hub = star("B", 2.0, true);
        let plan = SmartTravelPlanner::default()
            .plan(&origin, &destination, &[hub], TravelProfile::standard())
            .expect("valid plan");
        assert_eq!(plan.systems, ["A", "B", "D"]);
        assert_eq!(plan.intermediate_systems, ["B"]);
        assert_eq!(plan.direct_seconds, 300);
        assert_eq!(plan.estimated_seconds, 285);
        assert_eq!(plan.saved_seconds, 15);
        assert!(!plan.is_direct);
        let racing = SmartTravelPlanner::default()
            .plan(
                &origin,
                &destination,
                &[star("B", 2.0, true)],
                TravelProfile::racing(),
            )
            .expect("valid racing plan");
        assert_eq!(racing.systems, ["A", "B", "D"]);
        assert!(racing.saved_seconds >= MIN_SAVING_SECONDS);
        let cargo = SmartTravelPlanner::default()
            .plan(
                &origin,
                &destination,
                &[star("B", 2.0, true)],
                TravelProfile::cargo(),
            )
            .expect("valid cargo plan");
        assert_eq!(cargo.systems, ["A", "B", "D"]);
        assert_eq!(cargo.direct_seconds, 500);
        assert_eq!(cargo.estimated_seconds, 475);

        let direct = SmartTravelPlanner::default()
            .plan(&origin, &destination, &[], TravelProfile::standard())
            .expect("direct plan");
        assert!(direct.is_direct);
        assert_eq!(direct.systems, ["A", "D"]);
    }

    #[test]
    fn explicit_waypoints_append_terminal_star_for_remote_local_destination() {
        let origin = star("A", 0.0, false);
        let destination = star("D", 10.0, false);
        let hub = star("B", 2.0, true);
        let plan = SmartTravelPlanner::default()
            .plan(&origin, &destination, &[hub], TravelProfile::standard())
            .expect("valid plan");

        assert_eq!(plan.explicit_waypoints_for("D"), ["B"]);
        assert_eq!(
            plan.explicit_waypoints_for("D-2-L4"),
            ["B", "D"],
            "an explicit hub route to a body must enter the destination system first"
        );

        let direct = SmartTravelPlanner::default()
            .plan(&origin, &destination, &[], TravelProfile::standard())
            .expect("direct plan");
        assert_eq!(direct.explicit_waypoints_for("D-2-L4"), ["D"]);
    }

    #[test]
    fn equal_multi_hub_cost_prefers_fewer_intermediates() {
        let origin = star("A", 0.0, false);
        let destination = star("D", 20.0, false);
        let hubs = [star("B", 5.0, true), star("C", 15.0, true)];
        let standard = SmartTravelPlanner::default()
            .plan(&origin, &destination, &hubs, TravelProfile::standard())
            .expect("standard plan");
        assert_eq!(standard.systems, ["A", "C", "D"]);
        let racing = SmartTravelPlanner::default()
            .plan(&origin, &destination, &hubs, TravelProfile::racing())
            .expect("racing plan");
        assert!(racing.estimated_seconds <= standard.estimated_seconds);
    }

    #[test]
    fn racing_rounding_can_keep_a_marginal_detour_direct() {
        let origin = star("A", 0.0, false);
        let destination = star("D", 1.1, false);
        let hub = star("B", 0.2, true);
        let standard = SmartTravelPlanner::default()
            .plan(
                &origin,
                &destination,
                std::slice::from_ref(&hub),
                TravelProfile::standard(),
            )
            .expect("standard plan");
        let racing = SmartTravelPlanner::default()
            .plan(&origin, &destination, &[hub], TravelProfile::racing())
            .expect("racing plan");
        assert!(!standard.is_direct);
        assert!(racing.is_direct);
    }

    #[test]
    fn equal_ties_are_lexical_and_not_ownership_dependent() {
        let origin = star("A", 0.0, false);
        let destination = star("D", 10.0, false);
        let hubs = [star("C", 2.0, true), star("B", 2.0, true)];
        let plan = SmartTravelPlanner::default()
            .plan(&origin, &destination, &hubs, TravelProfile::standard())
            .expect("tie plan");
        assert_eq!(plan.systems, ["A", "B", "D"]);
    }

    #[test]
    fn slower_hub_detours_remain_direct() {
        let origin = star("A", 0.0, false);
        let destination = star("D", 10.0, false);
        let barely_slower = SmartTravelPlanner::default()
            .plan(
                &origin,
                &destination,
                &[star("B", -0.04, true)],
                TravelProfile::standard(),
            )
            .expect("barely slower route");
        let clearly_slower = SmartTravelPlanner::default()
            .plan(
                &origin,
                &destination,
                &[star("C", -1.0, true)],
                TravelProfile::standard(),
            )
            .expect("clearly slower route");
        assert!(barely_slower.is_direct);
        assert!(clearly_slower.is_direct);
        assert_eq!(barely_slower.estimated_seconds, 300);
        assert_eq!(clearly_slower.estimated_seconds, 300);
    }

    #[test]
    fn entry_points_do_not_change_route_ordering() {
        let origin = star("A", 0.0, false);
        let destination = star("D", 10.0, false);
        let mut hub = star("B", 2.0, true);
        hub.entry_point = Some(crate::domain::LocationKey::live("B-9-L5".into()));
        let with_entry_point = SmartTravelPlanner::default()
            .plan(
                &origin,
                &destination,
                &[hub.clone()],
                TravelProfile::standard(),
            )
            .expect("route with entry point");
        hub.entry_point = None;
        let without_entry_point = SmartTravelPlanner::default()
            .plan(&origin, &destination, &[hub], TravelProfile::standard())
            .expect("route without entry point");
        assert_eq!(with_entry_point, without_entry_point);
    }

    #[test]
    fn routes_have_no_cycles_and_are_capped_at_eight_hubs() {
        let origin = star("A", 0.0, false);
        let destination = star("D", 100.0, false);
        let hubs: Vec<_> = (0..12)
            .map(|index| star(&format!("H{index:02}"), index as f64 * 8.0, true))
            .collect();
        let plan = SmartTravelPlanner::default()
            .plan(&origin, &destination, &hubs, TravelProfile::standard())
            .expect("bounded plan");
        assert!(plan.intermediate_systems.len() <= MAX_INTERMEDIATE_HUBS);
        let mut unique = plan.systems.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), plan.systems.len());
    }

    #[test]
    fn invalid_and_non_finite_inputs_are_unplannable() {
        assert_eq!(
            surge_seconds(f64::NAN, TravelProfile::standard(), false),
            None
        );
        assert_eq!(surge_seconds(-1.0, TravelProfile::standard(), false), None);
        let origin = star("A", 0.0, false);
        let mut destination = star("D", 1.0, false);
        destination.position.as_mut().expect("position").x = f64::INFINITY;
        assert!(
            SmartTravelPlanner::default()
                .plan(&origin, &destination, &[], TravelProfile::standard())
                .is_none()
        );
    }

    #[test]
    fn catalogue_scale_considers_only_hub_stars() {
        let origin = star("A", 0.0, false);
        let destination = star("D", 10.0, false);
        let mut catalogue: Vec<_> = (0..500)
            .map(|index| star(&format!("N{index:03}"), 5.0, false))
            .collect();
        catalogue.push(star("H", 2.0, true));
        let plan = SmartTravelPlanner::default()
            .plan(&origin, &destination, &catalogue, TravelProfile::standard())
            .expect("catalogue plan");
        assert_eq!(plan.systems, ["A", "H", "D"]);
    }
}
