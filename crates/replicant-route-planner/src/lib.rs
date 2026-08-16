//! Pure relay-network planning for Replicant Space.
//!
//! The crate intentionally knows nothing about HTTP, SQLite, devices, or
//! managed-client state. Callers provide a star catalogue plus the systems
//! that already contain account-owned relay-capable devices. The exact solver minimizes
//! newly manufactured relay sites first and total graph hops second.

use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const INF_COMPONENT: u32 = u32::MAX / 8;
const EPSILON: f64 = 1e-9;
const EXACT_TERMINAL_LIMIT: usize = 20;
const EXACT_EXECUTION_STOP_LIMIT: usize = 16;
/// Upper bound on `2^terminals * candidate_nodes` DP states. Each state costs
/// 16 bytes across the `dp` and `parent` tables, so this caps exact-solver
/// memory at roughly 1 GiB even after corridor pruning.
const EXACT_STATE_LIMIT: usize = 64_000_000;

/// A position in the galaxy, measured in light-years.
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Position {
    /// X coordinate.
    pub x: f64,
    /// Y coordinate.
    pub y: f64,
    /// Z coordinate.
    pub z: f64,
}

impl Position {
    /// Straight-line distance to another position.
    #[must_use]
    pub fn distance(self, other: Self) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        let dz = self.z - other.z;
        (dx * dx + dy * dy + dz * dz).sqrt()
    }
}

/// One star available to the planner.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Star {
    /// Stable system designation.
    pub designation: String,
    /// Catalogue position.
    pub position: Position,
    /// Preferred arrival location, normally an L4 entry point.
    pub entry_point: Option<String>,
}

/// Whether a selected network node already has an account-owned relay.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayAvailability {
    /// The relay is already active and provides coverage before execution.
    Active,
    /// A deployed account-owned relay exists but must be activated.
    ActivationRequired,
    /// No relay exists at the selected system.
    New,
}

/// One oriented network node.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NetworkNode {
    /// System designation.
    pub system: String,
    /// Preferred L4/L5 arrival location, when known.
    pub entry_point: Option<String>,
    /// Tree depth from the start system.
    pub depth: usize,
    /// Whether this is the start system.
    pub is_start: bool,
    /// Whether this is a requested terminal.
    pub is_target: bool,
    /// Relay state at planning time.
    pub relay: RelayAvailability,
    /// Parent system in the oriented tree.
    pub parent: Option<String>,
}

/// One oriented edge in the relay tree.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NetworkEdge {
    /// Upstream system.
    pub parent: String,
    /// Downstream system.
    pub child: String,
    /// Straight-line distance.
    pub distance_ly: f64,
}

/// Exact relay-network result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RelayNetworkPlan {
    /// Starting system.
    pub start: String,
    /// Requested terminal systems.
    pub requested_targets: Vec<String>,
    /// Uniform maximum relay hop.
    pub max_hop_ly: f64,
    /// Selected nodes in dependency-safe breadth-first order.
    pub nodes: Vec<NetworkNode>,
    /// Oriented relay-tree edges.
    pub edges: Vec<NetworkEdge>,
    /// Systems that require a newly manufactured relay.
    pub new_relay_systems: Vec<String>,
    /// Systems with an existing inactive relay that must be activated.
    pub activation_systems: Vec<String>,
    /// Systems already providing active coverage.
    pub active_relay_systems: Vec<String>,
    /// Dependency-safe deployment/activation order, optimized for a continuous
    /// trip that starts and ends at the network start.
    pub execution_order: Vec<String>,
    /// Whether the execution order is proven optimal for its hop-first objective.
    pub execution_order_optimal: bool,
    /// Minimum graph hops for the complete deployment trip, including return
    /// when `execution_order_optimal` is true; otherwise the deterministic
    /// precedence-safe heuristic's hop count.
    pub execution_hops: usize,
    /// Minimum routed light-year distance among equal-hop deployment trips,
    /// including return to the start system.
    pub execution_distance_ly: f64,
    /// Sum of selected tree-edge distances.
    pub total_edge_distance_ly: f64,
    /// Whether the relay tree itself is proven minimum-new-relay.
    ///
    /// `true` for the exact Dreyfus-Wagner solution. `false` when the search
    /// was too large even after corridor pruning and a feasible greedy tree
    /// was returned instead; the plan is still valid and dependency-safe, but
    /// may use more new relays than strictly necessary.
    ///
    /// Defaults to `true` when absent so mission plans persisted before this
    /// field existed still load: every such plan came from the exact solver.
    #[serde(default = "exact_by_default")]
    pub relay_tree_optimal: bool,
}

fn exact_by_default() -> bool {
    true
}

/// Planner input.
#[derive(Clone, Debug)]
pub struct RelayNetworkRequest {
    /// System where the network is already anchored.
    pub start: String,
    /// Systems that must be connected.
    pub targets: Vec<String>,
    /// Systems with account-owned relay-capable devices already providing coverage.
    pub active_relay_systems: BTreeSet<String>,
    /// Systems with account-owned relay-capable devices that can be restored by activation.
    pub inactive_relay_systems: BTreeSet<String>,
    /// Maximum straight-line hop.
    pub max_hop_ly: f64,
}

/// One unsupported bridge considered while diagnosing a disconnected relay graph.
#[derive(Clone, Debug, PartialEq)]
pub struct DisconnectedBridge {
    /// Boundary system on the upstream connected component.
    pub from: String,
    /// Boundary system on the downstream connected component.
    pub to: String,
    /// Straight-line distance between the boundary systems.
    pub distance_ly: f64,
    /// Maximum relay range currently available across the two boundary systems.
    pub available_range_ly: f64,
    /// Additional range required to make this bridge usable.
    pub shortfall_ly: f64,
}

/// Detailed route-around analysis for a disconnected relay graph.
#[derive(Debug, Error, PartialEq)]
#[error(
    "no relay network connects {start} to {target}; closest start-to-target component gap is {direct_from} -> {direct_to} at {direct_distance_ly:.3} ly (range {direct_available_range_ly:.3} ly, short by {direct_shortfall_ly:.3} ly). A route-around through disconnected catalogue islands is closer to viable: {route_summary}; worst remaining shortfall {bottleneck_shortfall_ly:.3} ly. No fully valid detour exists at the current relay ranges"
)]
pub struct DisconnectedRouteAroundDetails {
    /// Requested network start.
    pub start: String,
    /// Requested target that is unreachable from the start component.
    pub target: String,
    /// Closest boundary system in the start component to the target component.
    pub direct_from: String,
    /// Closest boundary system in the target component to the start component.
    pub direct_to: String,
    /// Distance across the closest start-to-target component gap.
    pub direct_distance_ly: f64,
    /// Available range across the closest start-to-target component gap.
    pub direct_available_range_ly: f64,
    /// Shortfall across the closest start-to-target component gap.
    pub direct_shortfall_ly: f64,
    /// Worst shortfall on the best route-around candidate.
    pub bottleneck_shortfall_ly: f64,
    /// Human-readable boundary sequence for the best route-around candidate.
    pub route_summary: String,
    /// Structured unsupported bridges on the route-around candidate.
    pub bridges: Vec<DisconnectedBridge>,
}

/// Planning failure.
#[derive(Debug, Error, PartialEq)]
pub enum PlannerError {
    /// The catalogue has duplicate designations.
    #[error("duplicate star designation: {0}")]
    DuplicateStar(String),
    /// A requested system is absent from the catalogue.
    #[error("unknown system: {0}")]
    UnknownSystem(String),
    /// The request contains no targets.
    #[error("at least one target system is required")]
    NoTargets,
    /// The maximum hop is invalid.
    #[error("maximum hop must be finite and greater than zero")]
    InvalidMaximumHop,
    /// Exact planning would exceed the supported terminal mask size.
    #[error("exact planning supports at most {limit} terminals, received {actual}")]
    TooManyTerminals {
        /// Number of start-plus-target terminals requested.
        actual: usize,
        /// Maximum exact-solver terminal count.
        limit: usize,
    },
    /// Even after corridor pruning, the exact search table would not fit in
    /// memory. Requesting fewer targets per plan resolves this.
    #[error(
        "exact planning over {nodes} candidate systems with {terminals} terminals exceeds the \
         supported search size; plan fewer targets at once"
    )]
    ExactSearchTooLarge {
        /// Candidate systems remaining after corridor pruning.
        nodes: usize,
        /// Number of start-plus-target terminals requested.
        terminals: usize,
    },
    /// No connected network exists in the supplied graph.
    #[error("no relay network connects every requested system")]
    Disconnected,
    /// The start and a requested target are separated by an unbridgeable gap.
    #[error(
        "no relay network connects {start} to {target}; closest gap is {from} -> {to} at {distance_ly:.3} ly, but the available relay range is {available_range_ly:.3} ly (short by {shortfall_ly:.3} ly); alternate catalogue routes were checked and none reduce the required range"
    )]
    DisconnectedGap {
        /// Requested network start.
        start: String,
        /// Requested target that is unreachable from the start component.
        target: String,
        /// Closest star on the start-side connected component.
        from: String,
        /// Closest star on the target-side connected component.
        to: String,
        /// Straight-line distance between `from` and `to`.
        distance_ly: f64,
        /// Maximum relay range available across the two boundary systems.
        available_range_ly: f64,
        /// Additional range required to bridge the closest gap.
        shortfall_ly: f64,
    },
    /// No valid route exists, but traversing other disconnected catalogue
    /// islands reduces the largest unsupported gap compared with bridging the
    /// start and target components directly.
    #[error(transparent)]
    DisconnectedRouteAround(Box<DisconnectedRouteAroundDetails>),
    /// Internal reconstruction failed.
    #[error("failed to reconstruct exact relay network")]
    Reconstruction,
}

/// Immutable star graph with a conventional relay range plus optional
/// per-system extended relay ranges supplied by already deployed devices.
#[derive(Clone, Debug)]
pub struct StarGraph {
    stars: Vec<Star>,
    index: BTreeMap<String, usize>,
    relay_ranges_ly: Vec<f64>,
    adjacency: Vec<Vec<(usize, f64)>>,
}

#[derive(Clone, Copy, Debug)]
struct BridgeCandidate {
    from: usize,
    to: usize,
    distance_ly: f64,
    available_range_ly: f64,
    shortfall_ly: f64,
}

#[derive(Clone, Copy, Debug)]
struct DetourCost {
    bottleneck_shortfall_ly: f64,
    total_shortfall_ly: f64,
    total_distance_ly: f64,
    bridges: usize,
}

impl DetourCost {
    const INF: Self = Self {
        bottleneck_shortfall_ly: f64::INFINITY,
        total_shortfall_ly: f64::INFINITY,
        total_distance_ly: f64::INFINITY,
        bridges: usize::MAX,
    };

    const ZERO: Self = Self {
        bottleneck_shortfall_ly: 0.0,
        total_shortfall_ly: 0.0,
        total_distance_ly: 0.0,
        bridges: 0,
    };

    fn extend(self, bridge: BridgeCandidate) -> Self {
        Self {
            bottleneck_shortfall_ly: self.bottleneck_shortfall_ly.max(bridge.shortfall_ly),
            total_shortfall_ly: self.total_shortfall_ly + bridge.shortfall_ly,
            total_distance_ly: self.total_distance_ly + bridge.distance_ly,
            bridges: self.bridges.saturating_add(1),
        }
    }

    fn better_than(self, other: Self) -> bool {
        self.bottleneck_shortfall_ly
            .total_cmp(&other.bottleneck_shortfall_ly)
            .then_with(|| self.total_shortfall_ly.total_cmp(&other.total_shortfall_ly))
            .then_with(|| self.bridges.cmp(&other.bridges))
            .then_with(|| self.total_distance_ly.total_cmp(&other.total_distance_ly))
            .is_lt()
    }
}

impl StarGraph {
    /// Builds a deterministic graph using one uniform relay range. Stars are
    /// sorted by designation before edges are generated, so equal-cost exact
    /// solutions are repeatable.
    pub fn new(stars: Vec<Star>, max_hop_ly: f64) -> Result<Self, PlannerError> {
        Self::with_relay_ranges(stars, max_hop_ly, &BTreeMap::new())
    }

    /// Builds a deterministic graph while allowing already deployed relay-
    /// capable systems to advertise a longer range than a conventional relay.
    ///
    /// An edge is usable when either endpoint can span the separation. This
    /// models relay-capable infrastructure such as a 15 ly System Hub or a
    /// 10 ly Deep Space Relay Station bridging to an ordinary FTL relay.
    pub fn with_relay_ranges(
        mut stars: Vec<Star>,
        max_hop_ly: f64,
        relay_ranges_ly: &BTreeMap<String, f64>,
    ) -> Result<Self, PlannerError> {
        if !max_hop_ly.is_finite() || max_hop_ly <= 0.0 {
            return Err(PlannerError::InvalidMaximumHop);
        }
        if relay_ranges_ly
            .values()
            .any(|range| !range.is_finite() || *range <= 0.0)
        {
            return Err(PlannerError::InvalidMaximumHop);
        }
        stars.sort_by(|left, right| left.designation.cmp(&right.designation));
        let mut index = BTreeMap::new();
        for (star_index, star) in stars.iter().enumerate() {
            if index.insert(star.designation.clone(), star_index).is_some() {
                return Err(PlannerError::DuplicateStar(star.designation.clone()));
            }
        }

        // Every catalogue star can host a newly manufactured conventional
        // relay, so the baseline range remains `max_hop_ly`. Existing devices
        // only enlarge that range; they never make a candidate site worse.
        let node_ranges = stars
            .iter()
            .map(|star| {
                relay_ranges_ly
                    .get(&star.designation)
                    .copied()
                    .unwrap_or(max_hop_ly)
                    .max(max_hop_ly)
            })
            .collect::<Vec<_>>();
        let bucket_size = node_ranges.iter().copied().fold(max_hop_ly, f64::max);
        let cell = |position: Position| {
            (
                (position.x / bucket_size).floor() as i64,
                (position.y / bucket_size).floor() as i64,
                (position.z / bucket_size).floor() as i64,
            )
        };
        let mut buckets = BTreeMap::<(i64, i64, i64), Vec<usize>>::new();
        for (star_index, star) in stars.iter().enumerate() {
            buckets
                .entry(cell(star.position))
                .or_default()
                .push(star_index);
        }

        let mut adjacency = vec![Vec::new(); stars.len()];
        for left in 0..stars.len() {
            let (cell_x, cell_y, cell_z) = cell(stars[left].position);
            for offset_x in -1..=1 {
                for offset_y in -1..=1 {
                    for offset_z in -1..=1 {
                        let neighbor_cell =
                            (cell_x + offset_x, cell_y + offset_y, cell_z + offset_z);
                        for right in buckets.get(&neighbor_cell).into_iter().flatten() {
                            if *right <= left {
                                continue;
                            }
                            let distance = stars[left].position.distance(stars[*right].position);
                            let available_range = node_ranges[left].max(node_ranges[*right]);
                            if distance <= available_range + EPSILON {
                                adjacency[left].push((*right, distance));
                                adjacency[*right].push((left, distance));
                            }
                        }
                    }
                }
            }
        }
        for neighbors in &mut adjacency {
            neighbors.sort_by_key(|(neighbor, _)| *neighbor);
        }
        Ok(Self {
            stars,
            index,
            relay_ranges_ly: node_ranges,
            adjacency,
        })
    }

    /// Returns the graph's stars in deterministic designation order.
    #[must_use]
    pub fn stars(&self) -> &[Star] {
        &self.stars
    }

    fn relay_range(&self, index: usize) -> f64 {
        self.relay_ranges_ly[index]
    }

    fn disconnected_gap_error(&self, start: usize, targets: &[usize]) -> PlannerError {
        let (component_of, components) = self.connected_components();
        let start_component = component_of[start];
        let Some(target) = targets
            .iter()
            .copied()
            .find(|target| component_of[*target] != start_component)
        else {
            return PlannerError::Disconnected;
        };
        let target_component = component_of[target];

        let Some(direct) =
            self.best_component_bridge(&components[start_component], &components[target_component])
        else {
            return PlannerError::Disconnected;
        };

        if let Some((detour_cost, bridges)) =
            self.best_component_detour(start_component, target_component, &components)
            && bridges.len() > 1
            && detour_cost.bottleneck_shortfall_ly + EPSILON < direct.shortfall_ly
        {
            let route_summary = bridges
                .iter()
                .map(|bridge| {
                    format!(
                        "{} -> {} {:.3} ly (range {:.3}, short {:.3})",
                        self.stars[bridge.from].designation,
                        self.stars[bridge.to].designation,
                        bridge.distance_ly,
                        bridge.available_range_ly,
                        bridge.shortfall_ly,
                    )
                })
                .collect::<Vec<_>>()
                .join("; then ");
            return PlannerError::DisconnectedRouteAround(Box::new(
                DisconnectedRouteAroundDetails {
                    start: self.stars[start].designation.clone(),
                    target: self.stars[target].designation.clone(),
                    direct_from: self.stars[direct.from].designation.clone(),
                    direct_to: self.stars[direct.to].designation.clone(),
                    direct_distance_ly: direct.distance_ly,
                    direct_available_range_ly: direct.available_range_ly,
                    direct_shortfall_ly: direct.shortfall_ly,
                    bottleneck_shortfall_ly: detour_cost.bottleneck_shortfall_ly,
                    route_summary,
                    bridges: bridges
                        .into_iter()
                        .map(|bridge| DisconnectedBridge {
                            from: self.stars[bridge.from].designation.clone(),
                            to: self.stars[bridge.to].designation.clone(),
                            distance_ly: bridge.distance_ly,
                            available_range_ly: bridge.available_range_ly,
                            shortfall_ly: bridge.shortfall_ly,
                        })
                        .collect(),
                },
            ));
        }

        PlannerError::DisconnectedGap {
            start: self.stars[start].designation.clone(),
            target: self.stars[target].designation.clone(),
            from: self.stars[direct.from].designation.clone(),
            to: self.stars[direct.to].designation.clone(),
            distance_ly: direct.distance_ly,
            available_range_ly: direct.available_range_ly,
            shortfall_ly: direct.shortfall_ly,
        }
    }

    fn connected_components(&self) -> (Vec<usize>, Vec<Vec<usize>>) {
        let mut component_of = vec![usize::MAX; self.stars.len()];
        let mut components = Vec::<Vec<usize>>::new();
        for seed in 0..self.stars.len() {
            if component_of[seed] != usize::MAX {
                continue;
            }
            let component_index = components.len();
            let mut members = Vec::new();
            let mut queue = VecDeque::from([seed]);
            component_of[seed] = component_index;
            while let Some(current) = queue.pop_front() {
                members.push(current);
                for (neighbor, _) in &self.adjacency[current] {
                    if component_of[*neighbor] == usize::MAX {
                        component_of[*neighbor] = component_index;
                        queue.push_back(*neighbor);
                    }
                }
            }
            members.sort_unstable();
            components.push(members);
        }
        (component_of, components)
    }

    fn best_component_bridge(&self, left: &[usize], right: &[usize]) -> Option<BridgeCandidate> {
        let mut best = None::<BridgeCandidate>;
        for from in left {
            for to in right {
                let distance_ly = self.stars[*from]
                    .position
                    .distance(self.stars[*to].position);
                let available_range_ly = self.relay_range(*from).max(self.relay_range(*to));
                let candidate = BridgeCandidate {
                    from: *from,
                    to: *to,
                    distance_ly,
                    available_range_ly,
                    shortfall_ly: (distance_ly - available_range_ly).max(0.0),
                };
                if best.is_none_or(|current| self.bridge_better(candidate, current)) {
                    best = Some(candidate);
                }
            }
        }
        best
    }

    fn bridge_better(&self, candidate: BridgeCandidate, current: BridgeCandidate) -> bool {
        candidate.shortfall_ly + EPSILON < current.shortfall_ly
            || ((candidate.shortfall_ly - current.shortfall_ly).abs() <= EPSILON
                && (candidate.distance_ly + EPSILON < current.distance_ly
                    || ((candidate.distance_ly - current.distance_ly).abs() <= EPSILON
                        && (
                            self.stars[candidate.from].designation.as_str(),
                            self.stars[candidate.to].designation.as_str(),
                        ) < (
                            self.stars[current.from].designation.as_str(),
                            self.stars[current.to].designation.as_str(),
                        ))))
    }

    fn best_component_detour(
        &self,
        start_component: usize,
        target_component: usize,
        components: &[Vec<usize>],
    ) -> Option<(DetourCost, Vec<BridgeCandidate>)> {
        let count = components.len();
        let mut costs = vec![DetourCost::INF; count];
        let mut previous = vec![None::<(usize, BridgeCandidate)>; count];
        let mut visited = vec![false; count];
        costs[start_component] = DetourCost::ZERO;

        loop {
            let current = (0..count)
                .filter(|index| {
                    !visited[*index] && costs[*index].bottleneck_shortfall_ly.is_finite()
                })
                .min_by(|left, right| {
                    if costs[*left].better_than(costs[*right]) {
                        std::cmp::Ordering::Less
                    } else if costs[*right].better_than(costs[*left]) {
                        std::cmp::Ordering::Greater
                    } else {
                        left.cmp(right)
                    }
                })?;
            if current == target_component {
                break;
            }
            visited[current] = true;

            for next in 0..count {
                if next == current || visited[next] {
                    continue;
                }
                let Some(bridge) =
                    self.best_component_bridge(&components[current], &components[next])
                else {
                    continue;
                };
                let candidate = costs[current].extend(bridge);
                if candidate.better_than(costs[next]) {
                    costs[next] = candidate;
                    previous[next] = Some((current, bridge));
                }
            }
        }

        if !costs[target_component].bottleneck_shortfall_ly.is_finite() {
            return None;
        }
        let mut current = target_component;
        let mut reversed = Vec::new();
        while current != start_component {
            let (prior, bridge) = previous[current]?;
            reversed.push(bridge);
            current = prior;
        }
        reversed.reverse();
        Some((costs[target_component], reversed))
    }

    fn resolve(&self, designation: &str) -> Result<usize, PlannerError> {
        self.index
            .get(designation)
            .copied()
            .ok_or_else(|| PlannerError::UnknownSystem(designation.to_owned()))
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Cost {
    new_relays: u32,
    hops: u32,
}

impl Cost {
    const INF: Self = Self {
        new_relays: INF_COMPONENT,
        hops: INF_COMPONENT,
    };

    fn node(new_relay: bool) -> Self {
        Self {
            new_relays: if new_relay { 1 } else { 0 },
            hops: 0,
        }
    }

    fn add(self, other: Self) -> Self {
        if self == Self::INF || other == Self::INF {
            return Self::INF;
        }
        Self {
            new_relays: self.new_relays.saturating_add(other.new_relays),
            hops: self.hops.saturating_add(other.hops),
        }
    }

    fn subtract_node(self, node: Self) -> Self {
        if self == Self::INF {
            return Self::INF;
        }
        Self {
            new_relays: self.new_relays.saturating_sub(node.new_relays),
            hops: self.hops,
        }
    }
}

/// DP back-pointer. Payloads are `u32` so one entry costs 8 bytes instead of
/// 16: vertex indices are bounded by the star catalogue and submasks by
/// `2^EXACT_TERMINAL_LIMIT`, both far below `u32::MAX`.
#[derive(Clone, Copy, Debug, Default)]
enum Parent {
    #[default]
    None,
    Move(u32),
    Split(u32),
}

/// Solves the exact minimum-new-relay Steiner tree.
///
/// Inactive account-owned relays cost zero new units, but are surfaced as
/// activation stops. The secondary objective is minimum selected graph hops.
pub fn plan_relay_network(
    stars: Vec<Star>,
    request: RelayNetworkRequest,
) -> Result<RelayNetworkPlan, PlannerError> {
    plan_relay_network_with_ranges(stars, request, BTreeMap::new())
}

/// Solves the relay network while honoring the advertised ranges of existing
/// relay-capable systems. Values in `relay_ranges_ly` only extend the
/// conventional `request.max_hop_ly`; candidate systems can always receive a
/// newly manufactured conventional relay.
pub fn plan_relay_network_with_ranges(
    stars: Vec<Star>,
    request: RelayNetworkRequest,
    relay_ranges_ly: BTreeMap<String, f64>,
) -> Result<RelayNetworkPlan, PlannerError> {
    plan_relay_network_within(stars, request, relay_ranges_ly, EXACT_STATE_LIMIT)
}

/// Solver body with an explicit exact-search budget, so the fallback path is
/// reachable in tests without allocating a catalogue that actually exhausts
/// memory.
fn plan_relay_network_within(
    stars: Vec<Star>,
    request: RelayNetworkRequest,
    relay_ranges_ly: BTreeMap<String, f64>,
    state_limit: usize,
) -> Result<RelayNetworkPlan, PlannerError> {
    if request.targets.is_empty() {
        return Err(PlannerError::NoTargets);
    }
    let full_graph = StarGraph::with_relay_ranges(stars, request.max_hop_ly, &relay_ranges_ly)?;

    let mut target_names = request.targets.clone();
    target_names.sort();
    target_names.dedup();
    target_names.retain(|target| target != &request.start);
    if target_names.is_empty() {
        return Err(PlannerError::NoTargets);
    }

    let full_context = SolverContext::resolve(&full_graph, &request, &target_names)?;
    let terminal_count = full_context.targets.len() + 1;
    if terminal_count > EXACT_TERMINAL_LIMIT {
        return Err(PlannerError::TooManyTerminals {
            actual: terminal_count,
            limit: EXACT_TERMINAL_LIMIT,
        });
    }

    // The Dreyfus-Wagner tables are `2^terminals x nodes`, which is hopeless
    // on a full star catalogue. Restrict the exact search to the corridor of
    // systems that can still participate in an optimal tree; the reduction is
    // provably lossless (see `prune_to_relay_corridor`). Existing account
    // relays cost zero new units, so with `--reuse-account-relays` the
    // corridor automatically hugs the already-covered systems and shrinks
    // further. Execution-route metrics still use the full graph below, so
    // travel legs may cut through pruned systems.
    let (graph, context) = match prune_to_relay_corridor(&full_graph, &full_context) {
        Some(kept_stars) => {
            let graph =
                StarGraph::with_relay_ranges(kept_stars, request.max_hop_ly, &relay_ranges_ly)?;
            let context = SolverContext::resolve(&graph, &request, &target_names)?;
            (graph, context)
        }
        None => (full_graph.clone(), full_context),
    };
    let terminals = std::iter::once(context.start)
        .chain(context.targets.iter().copied())
        .collect::<Vec<_>>();
    let mask_count = 1usize << terminal_count;
    let node_count = graph.stars.len();
    if mask_count.saturating_mul(node_count) > state_limit {
        // The exact tables would not fit in memory even after pruning. Rather
        // than refuse the request, fall back to the same feasible tree that
        // anchors the pruning bound and mark the plan as non-optimal, so the
        // caller still gets a deployable network with an honest quality flag.
        let tree = greedy_terminal_tree(&graph, &context, &terminals).ok_or(
            PlannerError::ExactSearchTooLarge {
                nodes: node_count,
                terminals: terminal_count,
            },
        )?;
        let terminal_set = terminals.iter().copied().collect::<BTreeSet<_>>();
        let tree = deterministic_spanning_tree(context.start, &tree.edges);
        let tree = prune_nonterminal_leaves(tree, &terminal_set);
        return orient(
            &graph,
            &full_graph,
            context.start,
            &context.targets,
            &context.active,
            &context.inactive,
            request.max_hop_ly,
            tree,
            false,
        );
    }

    let SolverContext {
        start,
        targets,
        active,
        inactive,
        node_cost,
    } = context;
    let mut dp = vec![vec![Cost::INF; node_count]; mask_count];
    let mut parent = vec![vec![Parent::None; node_count]; mask_count];

    for (bit, terminal) in terminals.iter().copied().enumerate() {
        let mask = 1usize << bit;
        dp[mask][terminal] = node_cost[terminal];
        metric_closure(&graph, &node_cost, &mut dp[mask], &mut parent[mask]);
    }

    for mask in 1..mask_count {
        if mask.is_power_of_two() {
            continue;
        }
        let mut submask = (mask - 1) & mask;
        while submask != 0 {
            let other = mask ^ submask;
            if submask < other {
                for vertex in 0..node_count {
                    let candidate = dp[submask][vertex]
                        .add(dp[other][vertex])
                        .subtract_node(node_cost[vertex]);
                    if candidate < dp[mask][vertex] {
                        dp[mask][vertex] = candidate;
                        parent[mask][vertex] = Parent::Split(submask as u32);
                    }
                }
            }
            submask = (submask - 1) & mask;
        }
        metric_closure(&graph, &node_cost, &mut dp[mask], &mut parent[mask]);
    }

    let full_mask = mask_count - 1;
    if dp[full_mask][start] == Cost::INF {
        return Err(graph.disconnected_gap_error(start, &targets));
    }

    let mut reconstructed = BTreeSet::new();
    let mut visited_states = BTreeSet::new();
    reconstruct(
        full_mask,
        start,
        &parent,
        &mut reconstructed,
        &mut visited_states,
    )?;

    let terminal_set = terminals.iter().copied().collect::<BTreeSet<_>>();
    let tree = deterministic_spanning_tree(start, &reconstructed);
    let tree = prune_nonterminal_leaves(tree, &terminal_set);
    orient(
        &graph,
        &full_graph,
        start,
        &targets,
        &active,
        &inactive,
        request.max_hop_ly,
        tree,
        true,
    )
}

/// Resolved solver inputs for one specific [`StarGraph`]. Vertex indices are
/// graph-local, so corridor pruning requires re-resolution on the new graph.
struct SolverContext {
    start: usize,
    targets: Vec<usize>,
    active: BTreeSet<usize>,
    inactive: BTreeSet<usize>,
    node_cost: Vec<Cost>,
}

impl SolverContext {
    fn resolve(
        graph: &StarGraph,
        request: &RelayNetworkRequest,
        target_names: &[String],
    ) -> Result<Self, PlannerError> {
        let start = graph.resolve(&request.start)?;
        let targets = target_names
            .iter()
            .map(|target| graph.resolve(target))
            .collect::<Result<Vec<_>, _>>()?;
        let active = request
            .active_relay_systems
            .iter()
            .filter_map(|system| graph.index.get(system).copied())
            .collect::<BTreeSet<_>>();
        let inactive = request
            .inactive_relay_systems
            .iter()
            .filter_map(|system| graph.index.get(system).copied())
            .collect::<BTreeSet<_>>();
        let node_cost = (0..graph.stars.len())
            .map(|index| {
                Cost::node(index != start && !active.contains(&index) && !inactive.contains(&index))
            })
            .collect::<Vec<_>>();
        Ok(Self {
            start,
            targets,
            active,
            inactive,
            node_cost,
        })
    }
}

/// Catalogues at or below this size are solved without pruning; the rebuild
/// would cost more than the DP saves.
const PRUNE_MINIMUM_NODES: usize = 64;

/// Restricts the exact search to systems that can still appear in an optimal
/// relay tree.
///
/// After non-terminal leaves are pruned, every vertex of an optimal Steiner
/// tree lies on a tree path between two terminals. The new-relay count along
/// that path cannot exceed the whole tree's count, which in turn cannot exceed
/// any feasible solution's count. A system is therefore kept only when routing
/// between its two cheapest terminals *through it* stays within the greedy
/// feasible bound. Only the primary (new-relay) objective is tested, which
/// keeps the reduction lossless under the lexicographic `(relays, hops)` cost.
/// Systems carrying existing account relays cost zero new units, so corridors
/// naturally widen to hug already-covered space and shrink elsewhere.
///
/// Returns `None` when pruning should be skipped: the catalogue is already
/// small, a terminal is unreachable (disconnected diagnostics need the full
/// graph), or nothing would be removed.
fn prune_to_relay_corridor(graph: &StarGraph, context: &SolverContext) -> Option<Vec<Star>> {
    let node_count = graph.stars.len();
    if node_count <= PRUNE_MINIMUM_NODES {
        return None;
    }
    let terminals = std::iter::once(context.start)
        .chain(context.targets.iter().copied())
        .collect::<Vec<_>>();

    let mut from_terminal = Vec::with_capacity(terminals.len());
    for terminal in &terminals {
        let mut values = vec![Cost::INF; node_count];
        let mut parents = vec![Parent::None; node_count];
        values[*terminal] = context.node_cost[*terminal];
        metric_closure(graph, &context.node_cost, &mut values, &mut parents);
        if terminals.iter().any(|other| values[*other] == Cost::INF) {
            return None;
        }
        from_terminal.push(values);
    }
    let bound = greedy_tree_relay_bound(graph, context, &terminals)?;

    let terminal_set = terminals.iter().copied().collect::<BTreeSet<_>>();
    let mut kept = Vec::with_capacity(node_count);
    for vertex in 0..node_count {
        if terminal_set.contains(&vertex) {
            kept.push(graph.stars[vertex].clone());
            continue;
        }
        // The two cheapest terminal approaches; their sum counts this vertex's
        // own relay cost twice, so it is subtracted once.
        let mut cheapest = INF_COMPONENT;
        let mut second_cheapest = INF_COMPONENT;
        for values in &from_terminal {
            let relays = values[vertex].new_relays;
            if relays < cheapest {
                second_cheapest = cheapest;
                cheapest = relays;
            } else if relays < second_cheapest {
                second_cheapest = relays;
            }
        }
        if second_cheapest >= INF_COMPONENT {
            continue;
        }
        let through = cheapest
            .saturating_add(second_cheapest)
            .saturating_sub(context.node_cost[vertex].new_relays);
        if through <= bound {
            kept.push(graph.stars[vertex].clone());
        }
    }
    (kept.len() < node_count).then_some(kept)
}

/// New-relay count of a feasible tree built by nearest-terminal insertion.
/// This is the upper bound that anchors [`prune_to_relay_corridor`].
fn greedy_tree_relay_bound(
    graph: &StarGraph,
    context: &SolverContext,
    terminals: &[usize],
) -> Option<u32> {
    greedy_terminal_tree(graph, context, terminals).map(|tree| tree.new_relays)
}

/// A feasible relay tree built by nearest-terminal insertion.
struct GreedyTree {
    edges: BTreeSet<(usize, usize)>,
    new_relays: u32,
}

/// Builds a feasible tree by repeatedly attaching the cheapest remaining
/// terminal to the tree built so far.
///
/// This both anchors the corridor-pruning bound and serves as the fallback
/// plan when the exact search would not fit in memory.
fn greedy_terminal_tree(
    graph: &StarGraph,
    context: &SolverContext,
    terminals: &[usize],
) -> Option<GreedyTree> {
    let node_count = graph.stars.len();
    let mut in_tree = vec![false; node_count];
    in_tree[context.start] = true;
    let mut edges = BTreeSet::new();
    let mut remaining = terminals
        .iter()
        .copied()
        .filter(|terminal| *terminal != context.start)
        .collect::<BTreeSet<_>>();
    let mut total = 0u32;
    while !remaining.is_empty() {
        let mut values = vec![Cost::INF; node_count];
        let mut parents = vec![Parent::None; node_count];
        for (vertex, member) in in_tree.iter().enumerate() {
            if *member {
                values[vertex] = Cost {
                    new_relays: 0,
                    hops: 0,
                };
            }
        }
        metric_closure(graph, &context.node_cost, &mut values, &mut parents);
        let next = remaining
            .iter()
            .copied()
            .min_by_key(|terminal| (values[*terminal], *terminal))?;
        if values[next] == Cost::INF {
            return None;
        }
        total = total.saturating_add(values[next].new_relays);
        let mut current = next;
        while !in_tree[current] {
            in_tree[current] = true;
            match parents[current] {
                Parent::Move(previous) => {
                    let previous = previous as usize;
                    edges.insert(ordered_edge(current, previous));
                    current = previous;
                }
                _ => break,
            }
        }
        remaining.remove(&next);
    }
    Some(GreedyTree {
        edges,
        new_relays: total,
    })
}

fn metric_closure(
    graph: &StarGraph,
    node_cost: &[Cost],
    values: &mut [Cost],
    parents: &mut [Parent],
) {
    let mut heap = BinaryHeap::new();
    for (index, value) in values.iter().copied().enumerate() {
        if value != Cost::INF {
            heap.push((Reverse(value), Reverse(index)));
        }
    }
    while let Some((Reverse(cost), Reverse(current))) = heap.pop() {
        if cost != values[current] {
            continue;
        }
        for (neighbor, _) in &graph.adjacency[current] {
            let candidate = cost.add(node_cost[*neighbor]).add(Cost {
                new_relays: 0,
                hops: 1,
            });
            if candidate < values[*neighbor] {
                values[*neighbor] = candidate;
                parents[*neighbor] = Parent::Move(current as u32);
                heap.push((Reverse(candidate), Reverse(*neighbor)));
            }
        }
    }
}

fn reconstruct(
    mask: usize,
    vertex: usize,
    parent: &[Vec<Parent>],
    edges: &mut BTreeSet<(usize, usize)>,
    visited: &mut BTreeSet<(usize, usize)>,
) -> Result<(), PlannerError> {
    if !visited.insert((mask, vertex)) {
        return Ok(());
    }
    match parent[mask][vertex] {
        Parent::Move(previous) => {
            let previous = previous as usize;
            edges.insert(ordered_edge(vertex, previous));
            reconstruct(mask, previous, parent, edges, visited)
        }
        Parent::Split(submask) => {
            let submask = submask as usize;
            if submask == 0 || submask == mask {
                return Err(PlannerError::Reconstruction);
            }
            reconstruct(submask, vertex, parent, edges, visited)?;
            reconstruct(mask ^ submask, vertex, parent, edges, visited)
        }
        Parent::None if mask.is_power_of_two() => Ok(()),
        Parent::None => Err(PlannerError::Reconstruction),
    }
}

fn ordered_edge(left: usize, right: usize) -> (usize, usize) {
    if left < right {
        (left, right)
    } else {
        (right, left)
    }
}

fn deterministic_spanning_tree(
    start: usize,
    edges: &BTreeSet<(usize, usize)>,
) -> BTreeSet<(usize, usize)> {
    let mut adjacency = BTreeMap::<usize, BTreeSet<usize>>::new();
    for &(left, right) in edges {
        adjacency.entry(left).or_default().insert(right);
        adjacency.entry(right).or_default().insert(left);
    }
    let mut result = BTreeSet::new();
    let mut seen = BTreeSet::from([start]);
    let mut queue = VecDeque::from([start]);
    while let Some(current) = queue.pop_front() {
        for neighbor in adjacency.get(&current).into_iter().flatten() {
            if seen.insert(*neighbor) {
                result.insert(ordered_edge(current, *neighbor));
                queue.push_back(*neighbor);
            }
        }
    }
    result
}

fn prune_nonterminal_leaves(
    mut edges: BTreeSet<(usize, usize)>,
    terminals: &BTreeSet<usize>,
) -> BTreeSet<(usize, usize)> {
    loop {
        let mut degree = BTreeMap::<usize, usize>::new();
        for &(left, right) in &edges {
            *degree.entry(left).or_default() += 1;
            *degree.entry(right).or_default() += 1;
        }
        let removable = degree
            .iter()
            .filter_map(|(node, count)| (!terminals.contains(node) && *count <= 1).then_some(*node))
            .collect::<BTreeSet<_>>();
        if removable.is_empty() {
            break;
        }
        edges.retain(|(left, right)| !removable.contains(left) && !removable.contains(right));
    }
    edges
}

#[expect(
    clippy::too_many_arguments,
    reason = "internal orientation step; grouping these into a struct would only move the noise"
)]
fn orient(
    graph: &StarGraph,
    execution_graph: &StarGraph,
    start: usize,
    targets: &[usize],
    active: &BTreeSet<usize>,
    inactive: &BTreeSet<usize>,
    max_hop_ly: f64,
    edges: BTreeSet<(usize, usize)>,
    relay_tree_optimal: bool,
) -> Result<RelayNetworkPlan, PlannerError> {
    let mut adjacency = BTreeMap::<usize, Vec<usize>>::new();
    for &(left, right) in &edges {
        adjacency.entry(left).or_default().push(right);
        adjacency.entry(right).or_default().push(left);
    }
    for neighbors in adjacency.values_mut() {
        neighbors.sort_by(|left, right| {
            graph.stars[*left]
                .designation
                .cmp(&graph.stars[*right].designation)
        });
    }

    let target_set = targets.iter().copied().collect::<BTreeSet<_>>();
    let mut parents = BTreeMap::from([(start, None)]);
    let mut depths = BTreeMap::from([(start, 0usize)]);
    let mut traversal = Vec::new();
    let mut queue = VecDeque::from([start]);
    while let Some(current) = queue.pop_front() {
        traversal.push(current);
        for neighbor in adjacency.get(&current).into_iter().flatten() {
            if parents.contains_key(neighbor) {
                continue;
            }
            parents.insert(*neighbor, Some(current));
            depths.insert(*neighbor, depths[&current] + 1);
            queue.push_back(*neighbor);
        }
    }
    if targets.iter().any(|target| !parents.contains_key(target)) {
        return Err(graph.disconnected_gap_error(start, targets));
    }

    let availability = |index: usize| {
        if active.contains(&index) {
            RelayAvailability::Active
        } else if inactive.contains(&index) {
            RelayAvailability::ActivationRequired
        } else {
            RelayAvailability::New
        }
    };

    let nodes = traversal
        .iter()
        .map(|index| NetworkNode {
            system: graph.stars[*index].designation.clone(),
            entry_point: graph.stars[*index].entry_point.clone(),
            depth: depths[index],
            is_start: *index == start,
            is_target: target_set.contains(index),
            relay: if *index == start {
                RelayAvailability::Active
            } else {
                availability(*index)
            },
            parent: parents[index].map(|parent| graph.stars[parent].designation.clone()),
        })
        .collect::<Vec<_>>();

    let mut oriented_edges = Vec::new();
    for child in traversal.iter().copied().filter(|index| *index != start) {
        let parent = parents[&child].ok_or(PlannerError::Reconstruction)?;
        oriented_edges.push(NetworkEdge {
            parent: graph.stars[parent].designation.clone(),
            child: graph.stars[child].designation.clone(),
            distance_ly: graph.stars[parent]
                .position
                .distance(graph.stars[child].position),
        });
    }

    let mut new_relay_systems = Vec::new();
    let mut activation_systems = Vec::new();
    let mut active_relay_systems = Vec::new();
    for node in nodes.iter().filter(|node| !node.is_start) {
        match node.relay {
            RelayAvailability::Active => active_relay_systems.push(node.system.clone()),
            RelayAvailability::ActivationRequired => {
                activation_systems.push(node.system.clone());
            }
            RelayAvailability::New => {
                new_relay_systems.push(node.system.clone());
            }
        }
    }
    // Deployment travel is not constrained to the pruned relay corridor, so
    // the trip metrics run on the full catalogue graph. The start index is
    // re-resolved because pruning renumbers vertices.
    let execution_start = execution_graph.resolve(&graph.stars[start].designation)?;
    let (execution_order, execution_order_optimal, execution_hops, execution_distance_ly) =
        optimize_execution_order(execution_graph, execution_start, &nodes)?;

    Ok(RelayNetworkPlan {
        start: graph.stars[start].designation.clone(),
        requested_targets: targets
            .iter()
            .map(|index| graph.stars[*index].designation.clone())
            .collect(),
        max_hop_ly,
        total_edge_distance_ly: oriented_edges.iter().map(|edge| edge.distance_ly).sum(),
        nodes,
        edges: oriented_edges,
        new_relay_systems,
        activation_systems,
        active_relay_systems,
        execution_order,
        execution_order_optimal,
        execution_hops,
        execution_distance_ly,
        relay_tree_optimal,
    })
}

#[derive(Clone, Copy, Debug)]
struct TravelCost {
    hops: u32,
    distance_ly: f64,
}

impl TravelCost {
    const INF: Self = Self {
        hops: u32::MAX,
        distance_ly: f64::INFINITY,
    };

    fn add(self, other: Self) -> Self {
        if self.hops == u32::MAX || other.hops == u32::MAX {
            return Self::INF;
        }
        Self {
            hops: self.hops.saturating_add(other.hops),
            distance_ly: self.distance_ly + other.distance_ly,
        }
    }

    fn better_than(self, other: Self) -> bool {
        self.hops < other.hops
            || (self.hops == other.hops && self.distance_ly + EPSILON < other.distance_ly)
    }
}

fn shortest_hop_metrics(graph: &StarGraph, source: usize) -> Vec<TravelCost> {
    let mut hops = vec![u32::MAX; graph.stars.len()];
    hops[source] = 0;
    let mut queue = VecDeque::from([source]);
    while let Some(current) = queue.pop_front() {
        let next_hop = hops[current].saturating_add(1);
        for (neighbor, _) in &graph.adjacency[current] {
            if hops[*neighbor] == u32::MAX {
                hops[*neighbor] = next_hop;
                queue.push_back(*neighbor);
            }
        }
    }

    let mut order = (0..graph.stars.len()).collect::<Vec<_>>();
    order.sort_by_key(|index| (hops[*index], *index));
    let mut distance = vec![f64::INFINITY; graph.stars.len()];
    distance[source] = 0.0;
    for current in order {
        if !distance[current].is_finite() {
            continue;
        }
        for (neighbor, edge_distance) in &graph.adjacency[current] {
            if hops[*neighbor] != hops[current].saturating_add(1) {
                continue;
            }
            let candidate = distance[current] + edge_distance;
            if candidate + EPSILON < distance[*neighbor] {
                distance[*neighbor] = candidate;
            }
        }
    }

    hops.into_iter()
        .zip(distance)
        .map(|(hops, distance_ly)| TravelCost { hops, distance_ly })
        .collect()
}

fn optimize_execution_order(
    graph: &StarGraph,
    start: usize,
    nodes: &[NetworkNode],
) -> Result<(Vec<String>, bool, usize, f64), PlannerError> {
    let mut stops = nodes
        .iter()
        .filter(|node| !node.is_start && node.relay != RelayAvailability::Active)
        .collect::<Vec<_>>();
    stops.sort_by(|left, right| left.system.cmp(&right.system));
    if stops.is_empty() {
        return Ok((Vec::new(), true, 0, 0.0));
    }

    let stop_indices = stops
        .iter()
        .map(|node| graph.resolve(&node.system))
        .collect::<Result<Vec<_>, _>>()?;
    let stop_bits = stops
        .iter()
        .enumerate()
        .map(|(bit, node)| (node.system.as_str(), bit))
        .collect::<BTreeMap<_, _>>();
    let prerequisite_indices = stops
        .iter()
        .map(|node| {
            node.parent
                .as_deref()
                .and_then(|parent| stop_bits.get(parent).copied())
        })
        .collect::<Vec<_>>();

    let points = std::iter::once(start)
        .chain(stop_indices.iter().copied())
        .collect::<Vec<_>>();
    let mut pair_costs = vec![vec![TravelCost::INF; points.len()]; points.len()];
    for (source_position, source) in points.iter().copied().enumerate() {
        let metrics = shortest_hop_metrics(graph, source);
        for (target_position, target) in points.iter().copied().enumerate() {
            pair_costs[source_position][target_position] = metrics[target];
        }
    }

    let stop_count = stops.len();
    if stop_count > EXACT_EXECUTION_STOP_LIMIT {
        let mut completed = vec![false; stop_count];
        let mut current = 0usize;
        let mut total = TravelCost {
            hops: 0,
            distance_ly: 0.0,
        };
        let mut order = Vec::with_capacity(stop_count);
        while order.len() < stop_count {
            let next = (0..stop_count)
                .filter(|next| {
                    !completed[*next]
                        && prerequisite_indices[*next].is_none_or(|parent| completed[parent])
                })
                .min_by(|left, right| {
                    let left_cost = pair_costs[current][left + 1];
                    let right_cost = pair_costs[current][right + 1];
                    left_cost
                        .hops
                        .cmp(&right_cost.hops)
                        .then_with(|| left_cost.distance_ly.total_cmp(&right_cost.distance_ly))
                        .then_with(|| stops[*left].system.cmp(&stops[*right].system))
                })
                .ok_or(PlannerError::Reconstruction)?;
            let leg = pair_costs[current][next + 1];
            if leg.hops == u32::MAX {
                return Err(PlannerError::Disconnected);
            }
            total = total.add(leg);
            completed[next] = true;
            current = next + 1;
            order.push(stops[next].system.clone());
        }
        total = total.add(pair_costs[current][0]);
        if total.hops == u32::MAX {
            return Err(PlannerError::Disconnected);
        }
        return Ok((
            order,
            false,
            usize::try_from(total.hops).map_err(|_| PlannerError::Reconstruction)?,
            total.distance_ly,
        ));
    }

    let prerequisites = prerequisite_indices
        .iter()
        .map(|parent| parent.map_or(0usize, |bit| 1usize << bit))
        .collect::<Vec<_>>();
    let mask_count = 1usize << stop_count;
    let mut dp = vec![vec![TravelCost::INF; stop_count]; mask_count];
    let mut previous = vec![vec![None; stop_count]; mask_count];
    for next in 0..stop_count {
        if prerequisites[next] == 0 {
            dp[1usize << next][next] = pair_costs[0][next + 1];
        }
    }

    for mask in 1usize..mask_count {
        for end in 0..stop_count {
            let current = dp[mask][end];
            if current.hops == u32::MAX || mask & (1usize << end) == 0 {
                continue;
            }
            for next in 0..stop_count {
                let bit = 1usize << next;
                if mask & bit != 0 || prerequisites[next] & mask != prerequisites[next] {
                    continue;
                }
                let candidate = current.add(pair_costs[end + 1][next + 1]);
                let next_mask = mask | bit;
                if candidate.better_than(dp[next_mask][next]) {
                    dp[next_mask][next] = candidate;
                    previous[next_mask][next] = Some(end);
                }
            }
        }
    }

    let full_mask = mask_count - 1;
    let mut best = TravelCost::INF;
    let mut best_end = None;
    for end in 0..stop_count {
        let candidate = dp[full_mask][end].add(pair_costs[end + 1][0]);
        if candidate.better_than(best) {
            best = candidate;
            best_end = Some(end);
        }
    }
    let mut end = best_end.ok_or(PlannerError::Disconnected)?;
    let mut mask = full_mask;
    let mut reversed = Vec::with_capacity(stop_count);
    loop {
        reversed.push(stops[end].system.clone());
        let prior = previous[mask][end];
        mask ^= 1usize << end;
        let Some(prior) = prior else {
            break;
        };
        end = prior;
    }
    if mask != 0 {
        return Err(PlannerError::Reconstruction);
    }
    reversed.reverse();
    Ok((
        reversed,
        true,
        usize::try_from(best.hops).map_err(|_| PlannerError::Reconstruction)?,
        best.distance_ly,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn star(name: &str, x: f64) -> Star {
        star_at(name, x, 0.0, 0.0)
    }

    fn star_at(name: &str, x: f64, y: f64, z: f64) -> Star {
        Star {
            designation: name.to_owned(),
            position: Position { x, y, z },
            entry_point: Some(format!("{name}-1-L4")),
        }
    }

    #[test]
    fn exact_solver_reuses_active_and_inactive_relays() {
        let stars = vec![
            star("A", 0.0),
            star("B", 6.0),
            star("C", 12.0),
            star("D", 18.0),
        ];
        let plan = plan_relay_network(
            stars,
            RelayNetworkRequest {
                start: "A".into(),
                targets: vec!["D".into()],
                active_relay_systems: BTreeSet::from(["B".into()]),
                inactive_relay_systems: BTreeSet::from(["C".into()]),
                max_hop_ly: 7.499,
            },
        )
        .expect("plan");
        assert!(plan.new_relay_systems.contains(&"D".to_owned()));
        assert_eq!(plan.activation_systems, vec!["C".to_owned()]);
        assert_eq!(plan.execution_order, vec!["C".to_owned(), "D".to_owned()]);
        assert!(plan.execution_order_optimal);
    }

    #[test]
    fn exact_solver_prefers_fewer_new_relays_before_hops() {
        let stars = vec![
            star("A", 0.0),
            star("B", 5.0),
            star("C", 10.0),
            Star {
                designation: "X".into(),
                position: Position {
                    x: 5.0,
                    y: 4.0,
                    z: 0.0,
                },
                entry_point: None,
            },
        ];
        let plan = plan_relay_network(
            stars,
            RelayNetworkRequest {
                start: "A".into(),
                targets: vec!["C".into()],
                active_relay_systems: BTreeSet::from(["B".into()]),
                inactive_relay_systems: BTreeSet::new(),
                max_hop_ly: 7.499,
            },
        )
        .expect("plan");
        assert!(plan.active_relay_systems.contains(&"B".to_owned()));
        assert_eq!(plan.new_relay_systems, vec!["C".to_owned()]);
    }

    #[test]
    fn execution_order_respects_dependencies_and_optimizes_return_trip() {
        let stars = vec![
            Star {
                designation: "A".into(),
                position: Position {
                    x: 0.0,
                    y: 0.0,
                    z: 0.0,
                },
                entry_point: Some("A-1-L4".into()),
            },
            Star {
                designation: "B".into(),
                position: Position {
                    x: 5.0,
                    y: 0.0,
                    z: 0.0,
                },
                entry_point: Some("B-1-L4".into()),
            },
            Star {
                designation: "C".into(),
                position: Position {
                    x: 10.0,
                    y: 2.0,
                    z: 0.0,
                },
                entry_point: Some("C-1-L4".into()),
            },
            Star {
                designation: "D".into(),
                position: Position {
                    x: 10.0,
                    y: -2.0,
                    z: 0.0,
                },
                entry_point: Some("D-1-L4".into()),
            },
        ];
        let plan = plan_relay_network(
            stars,
            RelayNetworkRequest {
                start: "A".into(),
                targets: vec!["C".into(), "D".into()],
                active_relay_systems: BTreeSet::new(),
                inactive_relay_systems: BTreeSet::new(),
                max_hop_ly: 7.499,
            },
        )
        .expect("plan");
        let b = plan
            .execution_order
            .iter()
            .position(|system| system == "B")
            .expect("B stop");
        let c = plan
            .execution_order
            .iter()
            .position(|system| system == "C")
            .expect("C stop");
        let d = plan
            .execution_order
            .iter()
            .position(|system| system == "D")
            .expect("D stop");
        assert!(b < c && b < d);
        assert!(plan.execution_hops > 0);
        assert!(plan.execution_distance_ly.is_finite());
    }

    #[test]
    fn long_execution_route_uses_precedence_safe_heuristic() {
        let stars = (0..=17)
            .map(|index| star(&format!("S{index:02}"), index as f64 * 6.0))
            .collect::<Vec<_>>();
        let plan = plan_relay_network(
            stars,
            RelayNetworkRequest {
                start: "S00".into(),
                targets: vec!["S17".into()],
                active_relay_systems: BTreeSet::new(),
                inactive_relay_systems: BTreeSet::new(),
                max_hop_ly: 7.499,
            },
        )
        .expect("plan");
        assert!(!plan.execution_order_optimal);
        assert_eq!(
            plan.execution_order.first().map(String::as_str),
            Some("S01")
        );
        assert_eq!(plan.execution_order.last().map(String::as_str), Some("S17"));
    }

    #[test]
    fn disconnected_graph_reports_the_closest_gap() {
        let error = plan_relay_network(
            vec![star("A", 0.0), star("B", 20.0)],
            RelayNetworkRequest {
                start: "A".into(),
                targets: vec!["B".into()],
                active_relay_systems: BTreeSet::new(),
                inactive_relay_systems: BTreeSet::new(),
                max_hop_ly: 7.499,
            },
        )
        .expect_err("disconnected");
        let PlannerError::DisconnectedGap {
            start,
            target,
            from,
            to,
            distance_ly,
            available_range_ly,
            shortfall_ly,
        } = error
        else {
            panic!("expected detailed disconnected gap");
        };
        assert_eq!(start, "A");
        assert_eq!(target, "B");
        assert_eq!(from, "A");
        assert_eq!(to, "B");
        assert!((distance_ly - 20.0).abs() < EPSILON);
        assert!((available_range_ly - 7.499).abs() < EPSILON);
        assert!((shortfall_ly - 12.501).abs() < EPSILON);
    }

    #[test]
    fn planner_automatically_routes_around_an_oversized_direct_hop() {
        let plan = plan_relay_network(
            vec![
                star_at("A", 0.0, 0.0, 0.0),
                star_at("B", 5.0, 5.0, 0.0),
                star_at("C", 10.0, 0.0, 0.0),
            ],
            RelayNetworkRequest {
                start: "A".into(),
                targets: vec!["C".into()],
                active_relay_systems: BTreeSet::new(),
                inactive_relay_systems: BTreeSet::new(),
                max_hop_ly: 7.499,
            },
        )
        .expect("a valid off-axis detour should be selected automatically");

        assert_eq!(plan.edges.len(), 2);
        assert!(
            plan.edges.iter().any(|edge| {
                edge.parent == "A" && edge.child == "B" && edge.distance_ly < 7.499
            })
        );
        assert!(
            plan.edges.iter().any(|edge| {
                edge.parent == "B" && edge.child == "C" && edge.distance_ly < 7.499
            })
        );
        assert!(
            !plan
                .edges
                .iter()
                .any(|edge| edge.parent == "A" && edge.child == "C")
        );
    }

    #[test]
    fn disconnected_graph_reports_a_better_route_around_through_other_islands() {
        let error = plan_relay_network(
            vec![
                star("A", 0.0),
                star("B", 8.0),
                star("C", 16.0),
                star("D", 20.0),
            ],
            RelayNetworkRequest {
                start: "A".into(),
                targets: vec!["D".into()],
                active_relay_systems: BTreeSet::new(),
                inactive_relay_systems: BTreeSet::new(),
                max_hop_ly: 7.499,
            },
        )
        .expect_err("the two 8 ly gaps are still too large for standard relays");

        let PlannerError::DisconnectedRouteAround(details) = error else {
            panic!("expected route-around diagnostics");
        };
        assert_eq!(details.direct_from, "A");
        assert_eq!(details.direct_to, "C");
        assert!((details.direct_shortfall_ly - 8.501).abs() < EPSILON);
        assert!((details.bottleneck_shortfall_ly - 0.501).abs() < EPSILON);
        assert_eq!(details.bridges.len(), 2);
        assert_eq!(details.bridges[0].from, "A");
        assert_eq!(details.bridges[0].to, "B");
        assert_eq!(details.bridges[1].from, "B");
        assert_eq!(details.bridges[1].to, "C");
        assert!(details.route_summary.contains("A -> B"));
        assert!(details.route_summary.contains("B -> C"));
    }

    #[test]
    fn extended_existing_relay_range_bridges_a_standard_relay_gap() {
        let plan = plan_relay_network_with_ranges(
            vec![star("HUB", 0.0), star("REMOTE", 12.0)],
            RelayNetworkRequest {
                start: "HUB".into(),
                targets: vec!["REMOTE".into()],
                active_relay_systems: BTreeSet::from(["HUB".to_owned(), "REMOTE".to_owned()]),
                inactive_relay_systems: BTreeSet::new(),
                max_hop_ly: 7.499,
            },
            BTreeMap::from([("HUB".to_owned(), 15.0)]),
        )
        .expect("15 ly hub should bridge a 12 ly gap");

        assert!(plan.new_relay_systems.is_empty());
        assert_eq!(plan.edges.len(), 1);
        assert_eq!(plan.edges[0].parent, "HUB");
        assert_eq!(plan.edges[0].child, "REMOTE");
        assert_eq!(plan.edges[0].distance_ly, 12.0);
    }

    #[test]
    fn oversized_searches_fall_back_to_a_feasible_plan_instead_of_failing() {
        // Same chain as the pruning test, but with a search budget too small
        // for the exact tables. The plan must still be produced, connect the
        // target, and honestly report that its relay tree is not proven
        // minimal.
        let mut stars = (0..10)
            .map(|index| star(&format!("CHAIN{index:02}"), index as f64 * 6.0))
            .collect::<Vec<_>>();
        for index in 0..200 {
            stars.push(star_at(
                &format!("CLOUD{index:03}"),
                (index % 20) as f64 * 6.0,
                500.0 + (index / 20) as f64 * 6.0,
                0.0,
            ));
        }
        let request = RelayNetworkRequest {
            start: "CHAIN00".into(),
            targets: vec!["CHAIN09".into()],
            active_relay_systems: BTreeSet::new(),
            inactive_relay_systems: BTreeSet::new(),
            max_hop_ly: 7.499,
        };

        let fallback =
            plan_relay_network_within(stars.clone(), request.clone(), BTreeMap::new(), 1)
                .expect("fallback plan");
        assert!(!fallback.relay_tree_optimal);
        assert!(fallback.nodes.iter().any(|node| node.system == "CHAIN09"));
        assert!(
            fallback
                .nodes
                .iter()
                .all(|node| node.system.starts_with("CHAIN"))
        );

        // On this geometry the only route is the chain itself, so the feasible
        // plan matches the exact one; the flag is the difference.
        let exact = plan_relay_network(stars, request).expect("exact plan");
        assert!(exact.relay_tree_optimal);
        assert_eq!(fallback.new_relay_systems, exact.new_relay_systems);
    }

    #[test]
    fn corridor_pruning_preserves_the_exact_plan_on_a_large_catalogue() {
        // A 10-system chain surrounded by a large far-away cloud that can
        // never be part of an optimal tree. The catalogue exceeds
        // PRUNE_MINIMUM_NODES so the pruned code path is exercised.
        let mut stars = (0..10)
            .map(|index| star(&format!("CHAIN{index:02}"), index as f64 * 6.0))
            .collect::<Vec<_>>();
        for index in 0..200 {
            stars.push(star_at(
                &format!("CLOUD{index:03}"),
                (index % 20) as f64 * 6.0,
                500.0 + (index / 20) as f64 * 6.0,
                0.0,
            ));
        }
        let request = |targets: Vec<String>| RelayNetworkRequest {
            start: "CHAIN00".into(),
            targets,
            active_relay_systems: BTreeSet::new(),
            inactive_relay_systems: BTreeSet::new(),
            max_hop_ly: 7.499,
        };
        let plan = plan_relay_network(stars.clone(), request(vec!["CHAIN09".into()]))
            .expect("pruned plan");
        assert_eq!(plan.new_relay_systems.len(), 9);
        assert_eq!(plan.execution_order.len(), 9);
        assert!(plan.execution_order_optimal);
        assert!(
            plan.nodes
                .iter()
                .all(|node| node.system.starts_with("CHAIN"))
        );

        // The same plan on the corridor-only catalogue must be identical:
        // pruning is lossless.
        let corridor_only = stars
            .iter()
            .filter(|star| star.designation.starts_with("CHAIN"))
            .cloned()
            .collect::<Vec<_>>();
        let unpruned = plan_relay_network(corridor_only, request(vec!["CHAIN09".into()]))
            .expect("small-catalogue plan");
        assert_eq!(plan.nodes, unpruned.nodes);
        assert_eq!(plan.edges, unpruned.edges);
        assert_eq!(plan.execution_order, unpruned.execution_order);
    }

    #[test]
    fn corridor_pruning_keeps_detours_through_account_relays() {
        // The direct axis has a hole that only an off-axis chain of existing
        // account relays can bridge; pruning must keep that chain even though
        // it is geometrically off the corridor.
        let mut stars = vec![
            star_at("A", 0.0, 0.0, 0.0),
            star_at("GOAL", 28.0, 0.0, 0.0),
            star_at("R1", 5.0, 5.0, 0.0),
            star_at("R2", 11.0, 7.0, 0.0),
            star_at("R3", 17.0, 5.0, 0.0),
            star_at("EDGE", 23.0, 3.0, 0.0),
        ];
        for index in 0..120 {
            stars.push(star_at(
                &format!("CLOUD{index:03}"),
                (index % 12) as f64 * 6.0,
                -400.0 - (index / 12) as f64 * 6.0,
                0.0,
            ));
        }
        let plan = plan_relay_network(
            stars,
            RelayNetworkRequest {
                start: "A".into(),
                targets: vec!["GOAL".into()],
                active_relay_systems: BTreeSet::from(["R1".into(), "R2".into(), "R3".into()]),
                inactive_relay_systems: BTreeSet::new(),
                max_hop_ly: 7.499,
            },
        )
        .expect("detour through account relays");
        let systems = plan
            .nodes
            .iter()
            .map(|node| node.system.as_str())
            .collect::<Vec<_>>();
        assert!(systems.contains(&"R1"));
        assert!(systems.contains(&"R2"));
        assert!(systems.contains(&"R3"));
        assert_eq!(plan.new_relay_systems, vec!["EDGE", "GOAL"]);
    }

    #[test]
    fn extended_range_can_reach_a_new_standard_relay_site() {
        let plan = plan_relay_network_with_ranges(
            vec![star("HUB", 0.0), star("BRIDGE", 12.0), star("TARGET", 18.0)],
            RelayNetworkRequest {
                start: "HUB".into(),
                targets: vec!["TARGET".into()],
                active_relay_systems: BTreeSet::from(["HUB".to_owned()]),
                inactive_relay_systems: BTreeSet::new(),
                max_hop_ly: 7.499,
            },
            BTreeMap::from([("HUB".to_owned(), 15.0)]),
        )
        .expect("extended hub should reach the first conventional relay site");

        assert_eq!(
            plan.new_relay_systems,
            vec!["BRIDGE".to_owned(), "TARGET".to_owned()]
        );
        assert_eq!(plan.edges.len(), 2);
    }
}
