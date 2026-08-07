//! Pure planning primitives for Replicant Space civilisation events.
//!
//! The crate normalizes open-shaped event criteria and rewards, subtracts
//! confirmed progress and destination stock, expands manufacturing costs,
//! balances print work across autofactories, and plans repeated cargo/device
//! transport trips. It deliberately performs no HTTP, persistence, or gameplay
//! mutations.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use thiserror::Error;

/// Maximum user-tag length accepted by the game API.
pub const MAX_TAG_CHARACTERS: usize = 32;
/// Default cargo-freighter device type.
pub const CARGO_FREIGHTER: &str = "cargo_freighter";
/// Preferred attached-device transport.
pub const SURGE_CARRIER: &str = "surge_carrier";
/// Persistent beacon installed at an event's main body.
pub const FTL_BEACON: &str = "ftl_beacon";

/// Resource quantities keyed by open resource type.
pub type ResourceMap = BTreeMap<String, i64>;

/// A required consumable device type and count.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeviceRequirement {
    /// Open device-type key.
    pub device_type: String,
    /// Number of devices required.
    pub count: i64,
}

/// One valid method for resolving a location event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventCriterion {
    /// Stable criterion name supplied by the event.
    pub name: String,
    /// Resource quantities consumed by this criterion.
    pub resources: ResourceMap,
    /// Device quantities consumed by this criterion.
    pub devices: Vec<DeviceRequirement>,
}

/// Rewards granted when an event resolves.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventRewards {
    /// Resource quantities deposited at the event location.
    pub resources: ResourceMap,
    /// Experience reward.
    pub xp: Option<i64>,
    /// Civilisation-point reward.
    pub civilisation_points: Option<i64>,
    /// Completion achievement key.
    pub completion_achievement: Option<String>,
}

/// A normalized, selectable location event.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EventDefinition {
    /// Event designation.
    pub designation: String,
    /// Main body where requirements and rewards are stored.
    pub location: String,
    /// Display title.
    pub title: String,
    /// Display description.
    pub description: Option<String>,
    /// Open event type.
    pub event_type: Option<String>,
    /// Event tier.
    pub tier: Option<i64>,
    /// Event status.
    pub status: Option<String>,
    /// Alternative completion methods.
    pub criteria: Vec<EventCriterion>,
    /// Current open-shaped progress document.
    pub progress: Option<Value>,
    /// Resolution rewards.
    pub rewards: EventRewards,
}

/// Borrowed and owned fields used to normalize one open-shaped API event.
pub struct OpenEventFields<'a> {
    /// Event designation.
    pub designation: String,
    /// Main-body location where requirements and rewards are stored.
    pub location: String,
    /// Display title.
    pub title: String,
    /// Display description.
    pub description: Option<String>,
    /// Open event type.
    pub event_type: Option<String>,
    /// Event tier.
    pub tier: Option<i64>,
    /// Current event status.
    pub status: Option<String>,
    /// One or more alternative criterion objects.
    pub criteria: &'a [Map<String, Value>],
    /// Current progress object.
    pub progress: Option<&'a Map<String, Value>>,
    /// Reward object.
    pub rewards: Option<&'a Map<String, Value>>,
}

impl EventDefinition {
    /// Normalizes open-shaped criteria, progress, and rewards from the API.
    pub fn from_open_fields(fields: OpenEventFields<'_>) -> Result<Self, PlannerError> {
        let OpenEventFields {
            designation,
            location,
            title,
            description,
            event_type,
            tier,
            status,
            criteria,
            progress,
            rewards,
        } = fields;
        if designation.trim().is_empty() {
            return Err(PlannerError::InvalidEvent(
                "event designation is empty".into(),
            ));
        }
        if location.trim().is_empty() {
            return Err(PlannerError::InvalidEvent("event location is empty".into()));
        }
        let criteria = criteria
            .iter()
            .enumerate()
            .map(|(index, criterion)| parse_criterion(criterion, index))
            .collect::<Result<Vec<_>, _>>()?;
        if criteria.is_empty() {
            return Err(PlannerError::InvalidEvent(format!(
                "event {designation} has no completion criteria"
            )));
        }
        let rewards = rewards.map(parse_rewards).unwrap_or_default();
        Ok(Self {
            designation,
            location,
            title,
            description,
            event_type,
            tier,
            status,
            criteria,
            progress: progress.cloned().map(Value::Object),
            rewards,
        })
    }
}

/// An unlocked device blueprint used for costs, durations, and capacities.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BlueprintSpec {
    /// Device type printed by this blueprint.
    pub device_type: String,
    /// Print duration in seconds.
    pub print_time_seconds: f64,
    /// Cargo capacity, when applicable.
    pub cargo_capacity: i64,
    /// Attachment capacity, when applicable.
    pub attach_capacity: i64,
    /// Stow capacity, when applicable.
    pub stow_capacity: i64,
    /// Raw resource costs.
    pub resources: ResourceMap,
    /// Printable component costs.
    pub components: BTreeMap<String, i64>,
    /// Open feature flags.
    pub features: BTreeSet<String>,
}

/// Account-owned device stock relevant to an event mission.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeviceStock {
    /// Stable device code.
    pub code: String,
    /// Open device type.
    pub device_type: String,
    /// Current status.
    pub status: Option<String>,
    /// Current location, when known.
    pub location: Option<String>,
    /// Assigned replicant, when known.
    pub assigned_replicant: Option<String>,
    /// User tags.
    pub tags: BTreeSet<String>,
    /// Cargo capacity.
    pub cargo_capacity: i64,
    /// Attached-device capacity.
    pub attach_capacity: i64,
    /// Attachment slots currently occupied.
    #[serde(default)]
    pub attach_used: i64,
    /// Carrier this device is currently attached to, when any.
    #[serde(default)]
    pub attached_to_device_code: Option<String>,
    /// Vessel this device is currently stowed in, when any.
    #[serde(default)]
    pub stowed_in_device_code: Option<String>,
    /// Whether an AMI controller currently controls this device.
    pub controlled_by_ami: bool,
    /// Whether the device is travelling.
    pub travelling: bool,
}

impl DeviceStock {
    /// Whether the device is eligible to be consumed or transported as
    /// inactive stock.
    #[must_use]
    pub fn is_inactive(&self) -> bool {
        self.status.as_deref().is_some_and(|status| {
            matches!(
                status.to_ascii_lowercase().as_str(),
                "inactive" | "deactivated" | "idle" | "stowed" | "recalled" | "compacted"
            )
        }) && !self.travelling
    }

    /// Whether the device is not nested inside another device.
    #[must_use]
    pub fn is_free_standing(&self) -> bool {
        self.attached_to_device_code.is_none() && self.stowed_in_device_code.is_none()
    }

    /// Whether the device is in the same star system as `location`.
    #[must_use]
    pub fn is_in_same_system_as(&self, location: &str) -> bool {
        self.location
            .as_deref()
            .is_some_and(|device_location| same_system(device_location, location))
    }

    /// Whether another automation workflow has reserved this device.
    ///
    /// `allowed_event_mission` permits a currently executing event mission to
    /// retain its own claim while still rejecting every other mission and the
    /// bootstrap, mining, and relay namespaces.
    #[must_use]
    pub fn is_reserved_for_workflow(
        &self,
        mission_tag_prefix: &str,
        allowed_event_mission: Option<&str>,
    ) -> bool {
        self.tags.iter().any(|tag| {
            if tag.starts_with(mission_tag_prefix) {
                return allowed_event_mission != Some(tag.as_str());
            }
            [
                "boot-m:", "boot-r:", "region:", "mine-m:", "mine-b:", "mine-r:", "mine-s:",
                "relay-m:", "relay-b:", "relay-s:", "infra-r:", "infra-s:",
            ]
            .iter()
            .any(|prefix| tag.starts_with(*prefix))
        })
    }

    /// Whether the device can be claimed as a mission transport.
    ///
    /// AMI-controlled, nested, travelling, and workflow-reserved transports
    /// are deliberately ineligible.
    #[must_use]
    pub fn is_transport_eligible(&self, mission_tag_prefix: &str) -> bool {
        !self.controlled_by_ami
            && !self.travelling
            && self.is_free_standing()
            && !self.is_reserved_for_workflow(mission_tag_prefix, None)
    }
}

/// Existing autofactory workload.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FactoryWorkload {
    /// Autofactory device code.
    pub code: String,
    /// Remaining active-plus-queued work in seconds.
    pub remaining_seconds: f64,
}

/// One unit of printing work.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PrintUnit {
    /// Device type to print.
    pub device_type: String,
    /// Print duration in seconds.
    pub duration_seconds: f64,
}

/// A quantity-batched assignment to one autofactory.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PrintBatch {
    /// Autofactory device code.
    pub factory_code: String,
    /// Device type.
    pub device_type: String,
    /// Quantity assigned.
    pub quantity: i64,
    /// Zero-based queue order within this factory's newly scheduled work.
    pub sequence: usize,
    /// Projected factory workload after this batch.
    pub projected_finish_seconds: f64,
}

/// Balanced print schedule.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PrintSchedule {
    /// Quantity batches grouped by factory and device type.
    pub batches: Vec<PrintBatch>,
    /// Projected time until all newly scheduled work completes.
    pub makespan_seconds: f64,
}

/// A selected transport and its usable mission capacity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SelectedTransport {
    /// Device code, or a synthetic print placeholder.
    pub code: String,
    /// Device type.
    pub device_type: String,
    /// Mission capacity.
    pub capacity: i64,
    /// Whether this transport must be printed.
    pub must_print: bool,
}

/// Repeated-trip transport calculation.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct TransportPlan {
    /// Selected existing or to-be-printed transports.
    pub transports: Vec<SelectedTransport>,
    /// Combined capacity per trip.
    pub capacity_per_trip: i64,
    /// Inbound trips required.
    pub inbound_trips: i64,
    /// Return/reward trips required.
    pub outbound_trips: i64,
}

/// How the event body's persistent FTL beacon will be satisfied.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BeaconAction {
    /// An active account-owned beacon already exists at the event body.
    AlreadyActive,
    /// Deploy an inactive beacon already at the event body.
    #[serde(alias = "activate_existing")]
    DeployExisting,
    /// Carry an inactive account-owned beacon to the event body.
    TransportExisting,
    /// Print and transport a new beacon.
    PrintAndTransport,
    /// The secondary beacon objective cannot currently be satisfied.
    Unavailable,
}

/// FTL-beacon objective for future event discovery.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BeaconPlan {
    /// Planned action.
    pub action: BeaconAction,
    /// Existing beacon code, when applicable.
    pub device_code: Option<String>,
    /// Whether a carrier slot is required.
    pub transport_slots: i64,
    /// Non-fatal reason the beacon objective is unavailable.
    pub warning: Option<String>,
}

/// One recommendation badge assigned to a criterion.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Recommendation {
    /// Lowest projected preparation time.
    Fastest,
    /// Lowest expanded manufacturing resource cost.
    LowestManufacturingCost,
    /// Lowest rare-resource opportunity cost.
    LowestRareResourceUse,
    /// Fewest new devices printed.
    FewestPrints,
    /// Fewest total transport trips.
    FewestTrips,
    /// Reuses the most inactive stock.
    UsesExistingStockBest,
}

/// Fully assessed completion criterion.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CriterionAssessment {
    /// Criterion name.
    pub criterion_name: String,
    /// Resource quantities still needed at the event body.
    pub remaining_resources: ResourceMap,
    /// Device quantities still needed at the event body.
    pub remaining_devices: Vec<DeviceRequirement>,
    /// Existing inactive devices selected for consumption.
    pub reused_devices: Vec<String>,
    /// Device quantities that must be printed.
    pub print_devices: Vec<DeviceRequirement>,
    /// Expanded resource cost for all required printing.
    pub manufacturing_resources: ResourceMap,
    /// Balanced factory schedule.
    pub print_schedule: PrintSchedule,
    /// Resource transport plan.
    pub cargo: TransportPlan,
    /// Event-device and beacon transport plan.
    pub carriers: TransportPlan,
    /// Persistent beacon objective.
    pub beacon: BeaconPlan,
    /// Whether this criterion is currently executable from known state.
    pub feasible: bool,
    /// Hard blockers that must be resolved before execution.
    pub blockers: Vec<String>,
    /// Recommendation badges, assigned after all criteria are assessed.
    pub recommendations: BTreeSet<Recommendation>,
    /// Planner warnings.
    pub warnings: Vec<String>,
}

impl CriterionAssessment {
    /// Total number of new devices printed, including prerequisite transports
    /// and a printed beacon.
    #[must_use]
    pub fn print_count(&self) -> i64 {
        self.print_devices.iter().map(|item| item.count).sum()
    }

    /// Total number of inbound and reward-recovery trips.
    #[must_use]
    pub fn total_trips(&self) -> i64 {
        self.cargo.inbound_trips
            + self.cargo.outbound_trips
            + self.carriers.inbound_trips
            + self.carriers.outbound_trips
    }
}

/// Event-wide planning result.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EventPlan {
    /// Event definition.
    pub event: EventDefinition,
    /// Whether the completion achievement is not yet earned.
    pub grants_unearned_achievement: bool,
    /// Assessments for all valid criteria.
    pub criteria: Vec<CriterionAssessment>,
}

/// Requirements still missing for one selected criterion at the event body.
///
/// Progress and current event-body stock are treated as two observations of
/// the same satisfaction state; the larger observed quantity is used rather
/// than summing them, which avoids double-counting mirrored progress fields.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemainingRequirements {
    /// Resource quantities still required.
    pub resources: ResourceMap,
    /// Device quantities still required.
    pub devices: Vec<DeviceRequirement>,
}

/// Recalculates the live remaining requirements for a selected criterion.
pub fn remaining_requirements(
    event: &EventDefinition,
    criterion_name: &str,
    event_inventory: &ResourceMap,
    event_devices: &[DeviceStock],
) -> Result<RemainingRequirements, PlannerError> {
    let criterion = event
        .criteria
        .iter()
        .find(|criterion| criterion.name.eq_ignore_ascii_case(criterion_name))
        .ok_or_else(|| {
            PlannerError::InvalidEvent(format!(
                "criterion {criterion_name:?} is not present on event {}",
                event.designation
            ))
        })?;
    let progress = extract_progress(event.progress.as_ref()).for_criterion(&criterion.name);
    let mut event_stock = BTreeMap::new();
    for device in event_devices.iter().filter(|device| {
        device.location.as_deref() == Some(event.location.as_str()) && device.is_inactive()
    }) {
        *event_stock.entry(device.device_type.clone()).or_default() += 1;
    }
    Ok(RemainingRequirements {
        resources: subtract_resources(&criterion.resources, &progress.resources, event_inventory),
        devices: subtract_device_requirements(&criterion.devices, &progress.devices, &event_stock),
    })
}

/// All state required to assess one event.
#[derive(Clone, Debug, Default)]
pub struct PlanningContext {
    /// Resources currently at the home/manufacturing hub.
    pub home_inventory: ResourceMap,
    /// Resources already at the event body.
    pub event_inventory: ResourceMap,
    /// Unlocked blueprints keyed by device type.
    pub blueprints: BTreeMap<String, BlueprintSpec>,
    /// Relevant account-owned devices.
    pub devices: Vec<DeviceStock>,
    /// Current autofactory workloads.
    pub factories: Vec<FactoryWorkload>,
    /// Earned completion-achievement keys.
    pub earned_achievements: BTreeSet<String>,
    /// Home/manufacturing location.
    pub home_location: String,
    /// Prefix used by event-mission claim tags.
    pub mission_tag_prefix: String,
}

/// Planning failure.
#[derive(Debug, Error, PartialEq)]
pub enum PlannerError {
    /// Invalid or incomplete event payload.
    #[error("invalid event: {0}")]
    InvalidEvent(String),
    /// A required blueprint is unavailable.
    #[error("blueprint is not unlocked: {0}")]
    MissingBlueprint(String),
    /// Component dependency cycle.
    #[error("blueprint component cycle: {0}")]
    ComponentCycle(String),
    /// No autofactory is available for required printing.
    #[error("printing is required but no autofactory is available")]
    NoAutofactory,
    /// A required transport blueprint has no useful capacity.
    #[error("transport blueprint {device_type} has no {capacity_kind} capacity")]
    InvalidTransportBlueprint {
        /// Device type.
        device_type: String,
        /// Capacity being validated.
        capacity_kind: &'static str,
    },
}

/// Plans every criterion and assigns comparison badges.
pub fn plan_event(
    event: EventDefinition,
    context: &PlanningContext,
) -> Result<EventPlan, PlannerError> {
    let beacon = plan_beacon(&event.location, context);
    let progress = extract_progress(event.progress.as_ref());
    let mut criteria = event
        .criteria
        .iter()
        .map(|criterion| {
            assess_criterion(&event, criterion, &progress, &beacon, context).unwrap_or_else(
                |error| blocked_assessment(&event, criterion, &progress, &beacon, context, error),
            )
        })
        .collect::<Vec<_>>();
    assign_recommendations(&mut criteria);
    let grants_unearned_achievement = event
        .rewards
        .completion_achievement
        .as_ref()
        .is_some_and(|key| !context.earned_achievements.contains(key));
    Ok(EventPlan {
        event,
        grants_unearned_achievement,
        criteria,
    })
}

/// Schedules print units using longest-processing-time-first list scheduling.
pub fn schedule_print_units(
    factories: &[FactoryWorkload],
    mut units: Vec<PrintUnit>,
) -> Result<PrintSchedule, PlannerError> {
    if units.is_empty() {
        return Ok(PrintSchedule::default());
    }
    if factories.is_empty() {
        return Err(PlannerError::NoAutofactory);
    }
    units.sort_by(|left, right| {
        right
            .duration_seconds
            .partial_cmp(&left.duration_seconds)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.device_type.cmp(&right.device_type))
    });
    let mut loads = factories
        .iter()
        .map(|factory| (factory.code.clone(), factory.remaining_seconds.max(0.0)))
        .collect::<BTreeMap<_, _>>();
    let mut per_factory = BTreeMap::<String, Vec<(String, i64, f64)>>::new();
    for unit in units {
        let factory = loads
            .iter()
            .min_by(|(left_code, left), (right_code, right)| {
                left.partial_cmp(right)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| left_code.cmp(right_code))
            })
            .map(|(code, _)| code.clone())
            .ok_or(PlannerError::NoAutofactory)?;
        let finish = loads.entry(factory.clone()).or_default();
        *finish += unit.duration_seconds.max(0.0);
        let queue = per_factory.entry(factory).or_default();
        if let Some((last_type, quantity, projected_finish)) = queue.last_mut()
            && *last_type == unit.device_type
        {
            *quantity += 1;
            *projected_finish = *finish;
        } else {
            queue.push((unit.device_type, 1, *finish));
        }
    }
    let mut batches = Vec::new();
    for (factory_code, queue) in per_factory {
        for (sequence, (device_type, quantity, projected_finish_seconds)) in
            queue.into_iter().enumerate()
        {
            batches.push(PrintBatch {
                factory_code: factory_code.clone(),
                device_type,
                quantity,
                sequence,
                projected_finish_seconds,
            });
        }
    }
    let makespan_seconds = batches
        .iter()
        .map(|batch| batch.projected_finish_seconds)
        .fold(0.0, f64::max);
    Ok(PrintSchedule {
        batches,
        makespan_seconds,
    })
}

/// Produces a bounded deterministic mission tag.
#[must_use]
pub fn mission_tag(mission_id: &str) -> String {
    format!("evt-m:{}", short_hash(mission_id))
}

/// Produces a bounded role tag.
pub fn role_tag(role: &str) -> String {
    let normalized = normalize_tag_component(role);
    let direct = format!("evt-role:{normalized}");
    if direct.chars().count() <= MAX_TAG_CHARACTERS {
        direct
    } else {
        format!("evt-role:{}", short_hash(role))
    }
}

fn assess_criterion(
    event: &EventDefinition,
    criterion: &EventCriterion,
    progress: &ProgressSnapshot,
    beacon: &BeaconPlan,
    context: &PlanningContext,
) -> Result<CriterionAssessment, PlannerError> {
    let criterion_progress = progress.for_criterion(&criterion.name);
    let remaining_resources = subtract_resources(
        &criterion.resources,
        &criterion_progress.resources,
        &context.event_inventory,
    );
    let event_device_stock = inactive_devices_at(
        &context.devices,
        &event.location,
        &context.mission_tag_prefix,
    );
    let mut remaining_devices = subtract_device_requirements(
        &criterion.devices,
        &criterion_progress.devices,
        &event_device_stock,
    );
    remaining_devices.sort_by(|left, right| left.device_type.cmp(&right.device_type));

    let mut reusable_pool = context
        .devices
        .iter()
        .filter(|device| {
            device.is_inactive()
                && device.is_free_standing()
                && device.location.as_deref() == Some(context.home_location.as_str())
                && device.location.as_deref() != Some(&event.location)
                && !device.is_reserved_for_workflow(&context.mission_tag_prefix, None)
        })
        .collect::<Vec<_>>();
    reusable_pool.sort_by(|left, right| {
        location_rank(left.location.as_deref(), &context.home_location)
            .cmp(&location_rank(
                right.location.as_deref(),
                &context.home_location,
            ))
            .then_with(|| left.code.cmp(&right.code))
    });
    let mut reused_devices = Vec::new();
    let mut print_devices = Vec::new();
    for requirement in &remaining_devices {
        let mut needed = requirement.count;
        for &device in &reusable_pool {
            if needed == 0 {
                break;
            }
            if device.device_type != requirement.device_type
                || reused_devices.contains(&device.code)
                || device
                    .tags
                    .iter()
                    .any(|tag| tag.starts_with(&context.mission_tag_prefix))
            {
                continue;
            }
            reused_devices.push(device.code.clone());
            needed -= 1;
        }
        if needed > 0 {
            print_devices.push(DeviceRequirement {
                device_type: requirement.device_type.clone(),
                count: needed,
            });
        }
    }

    let mut beacon = beacon.clone();
    if matches!(beacon.action, BeaconAction::PrintAndTransport) {
        increment_requirement(&mut print_devices, FTL_BEACON, 1);
    }

    let mut cargo_print = Vec::new();
    let cargo = plan_cargo_transport(event, &remaining_resources, context, &mut cargo_print)?;
    for requirement in cargo_print {
        increment_requirement(
            &mut print_devices,
            &requirement.device_type,
            requirement.count,
        );
    }

    let event_device_slots = remaining_devices.iter().map(|item| item.count).sum::<i64>();
    let mut carrier_print = Vec::new();
    let carriers = match plan_device_transport(
        event_device_slots + beacon.transport_slots,
        context,
        &mut carrier_print,
    ) {
        Ok(plan) => plan,
        Err(error) if event_device_slots == 0 && beacon.transport_slots > 0 => {
            beacon = BeaconPlan {
                action: BeaconAction::Unavailable,
                device_code: beacon.device_code.clone(),
                transport_slots: 0,
                warning: Some(format!("beacon objective skipped: {error}")),
            };
            remove_requirement(&mut print_devices, FTL_BEACON, 1);
            TransportPlan::default()
        }
        Err(error) => return Err(error),
    };
    for requirement in carrier_print {
        increment_requirement(
            &mut print_devices,
            &requirement.device_type,
            requirement.count,
        );
    }

    let mut print_units = Vec::new();
    let mut manufacturing_resources = ResourceMap::new();
    for requirement in &print_devices {
        let blueprint = context
            .blueprints
            .get(&requirement.device_type)
            .ok_or_else(|| PlannerError::MissingBlueprint(requirement.device_type.clone()))?;
        for _ in 0..requirement.count {
            print_units.push(PrintUnit {
                device_type: requirement.device_type.clone(),
                duration_seconds: blueprint.print_time_seconds,
            });
        }
        merge_resources(
            &mut manufacturing_resources,
            &expand_blueprint_resources(
                &requirement.device_type,
                requirement.count,
                &context.blueprints,
            )?,
        );
    }
    print_devices.sort_by(|left, right| left.device_type.cmp(&right.device_type));
    let print_schedule = match schedule_print_units(&context.factories, print_units) {
        Ok(schedule) => schedule,
        Err(PlannerError::NoAutofactory)
            if print_devices.len() == 1
                && print_devices[0].device_type == FTL_BEACON
                && print_devices[0].count == 1 =>
        {
            beacon = BeaconPlan {
                action: BeaconAction::Unavailable,
                device_code: None,
                transport_slots: 0,
                warning: Some("beacon objective skipped: no autofactory is available".into()),
            };
            print_devices.clear();
            manufacturing_resources.clear();
            PrintSchedule::default()
        }
        Err(error) => return Err(error),
    };

    let mut warnings = Vec::new();
    if let Some(warning) = &beacon.warning {
        warnings.push(warning.clone());
    }
    let optional_beacon_resources = if matches!(beacon.action, BeaconAction::PrintAndTransport) {
        expand_blueprint_resources(FTL_BEACON, 1, &context.blueprints)?
    } else {
        ResourceMap::new()
    };
    let critical_manufacturing =
        subtract_resource_map(&manufacturing_resources, &optional_beacon_resources);
    let critical_shortages = resource_shortages(
        &context.home_inventory,
        &remaining_resources,
        &critical_manufacturing,
    );
    let total_shortages = resource_shortages(
        &context.home_inventory,
        &remaining_resources,
        &manufacturing_resources,
    );
    let mut blockers = Vec::new();
    if !critical_shortages.is_empty() {
        blockers.push(format!(
            "home inventory is short: {}",
            format_resources(&critical_shortages)
        ));
    }
    if critical_shortages.is_empty() && !total_shortages.is_empty() {
        warnings.push(format!(
            "event can proceed, but beacon resources are short: {}",
            format_resources(&total_shortages)
        ));
    }

    Ok(CriterionAssessment {
        criterion_name: criterion.name.clone(),
        remaining_resources,
        remaining_devices,
        reused_devices,
        print_devices,
        manufacturing_resources,
        print_schedule,
        cargo,
        carriers,
        beacon,
        feasible: blockers.is_empty(),
        blockers,
        recommendations: BTreeSet::new(),
        warnings,
    })
}

fn blocked_assessment(
    event: &EventDefinition,
    criterion: &EventCriterion,
    progress: &ProgressSnapshot,
    beacon: &BeaconPlan,
    context: &PlanningContext,
    error: PlannerError,
) -> CriterionAssessment {
    let criterion_progress = progress.for_criterion(&criterion.name);
    let remaining_resources = subtract_resources(
        &criterion.resources,
        &criterion_progress.resources,
        &context.event_inventory,
    );
    let event_device_stock = inactive_devices_at(
        &context.devices,
        &event.location,
        &context.mission_tag_prefix,
    );
    let remaining_devices = subtract_device_requirements(
        &criterion.devices,
        &criterion_progress.devices,
        &event_device_stock,
    );
    let mut warnings = Vec::new();
    if let Some(warning) = &beacon.warning {
        warnings.push(warning.clone());
    }
    CriterionAssessment {
        criterion_name: criterion.name.clone(),
        remaining_resources,
        remaining_devices,
        reused_devices: Vec::new(),
        print_devices: Vec::new(),
        manufacturing_resources: ResourceMap::new(),
        print_schedule: PrintSchedule::default(),
        cargo: TransportPlan::default(),
        carriers: TransportPlan::default(),
        beacon: beacon.clone(),
        feasible: false,
        blockers: vec![error.to_string()],
        recommendations: BTreeSet::new(),
        warnings,
    }
}

fn plan_cargo_transport(
    event: &EventDefinition,
    remaining_resources: &ResourceMap,
    context: &PlanningContext,
    prints: &mut Vec<DeviceRequirement>,
) -> Result<TransportPlan, PlannerError> {
    let inbound = sum_resources(remaining_resources);
    let outbound = sum_resources(&event.rewards.resources);
    if inbound == 0 && outbound == 0 {
        return Ok(TransportPlan::default());
    }
    let mut candidates = context
        .devices
        .iter()
        .filter(|device| {
            device.device_type == CARGO_FREIGHTER
                && device.cargo_capacity > 0
                && device.is_in_same_system_as(&context.home_location)
                && device.is_transport_eligible(&context.mission_tag_prefix)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        location_rank(left.location.as_deref(), &context.home_location)
            .cmp(&location_rank(
                right.location.as_deref(),
                &context.home_location,
            ))
            .then_with(|| right.cargo_capacity.cmp(&left.cargo_capacity))
            .then_with(|| left.code.cmp(&right.code))
    });
    let target_capacity = inbound.max(outbound);
    let mut selected = Vec::new();
    let mut capacity = 0;
    for device in candidates {
        selected.push(SelectedTransport {
            code: device.code.clone(),
            device_type: device.device_type.clone(),
            capacity: device.cargo_capacity,
            must_print: false,
        });
        capacity += device.cargo_capacity;
        if capacity >= target_capacity {
            break;
        }
    }
    if selected.is_empty() {
        let blueprint = context
            .blueprints
            .get(CARGO_FREIGHTER)
            .ok_or_else(|| PlannerError::MissingBlueprint(CARGO_FREIGHTER.into()))?;
        if blueprint.cargo_capacity <= 0 {
            return Err(PlannerError::InvalidTransportBlueprint {
                device_type: CARGO_FREIGHTER.into(),
                capacity_kind: "cargo",
            });
        }
        prints.push(DeviceRequirement {
            device_type: CARGO_FREIGHTER.into(),
            count: 1,
        });
        selected.push(SelectedTransport {
            code: "<print:cargo_freighter>".into(),
            device_type: CARGO_FREIGHTER.into(),
            capacity: blueprint.cargo_capacity,
            must_print: true,
        });
        capacity = blueprint.cargo_capacity;
    }
    Ok(TransportPlan {
        transports: selected,
        capacity_per_trip: capacity,
        inbound_trips: trips(inbound, capacity),
        outbound_trips: trips(outbound, capacity),
    })
}

fn plan_device_transport(
    slots: i64,
    context: &PlanningContext,
    prints: &mut Vec<DeviceRequirement>,
) -> Result<TransportPlan, PlannerError> {
    if slots <= 0 {
        return Ok(TransportPlan::default());
    }
    let mut candidates = context
        .devices
        .iter()
        .filter(|device| {
            device.attach_capacity > 0
                && device.attach_used == 0
                && device.is_in_same_system_as(&context.home_location)
                && device.is_transport_eligible(&context.mission_tag_prefix)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| {
        carrier_rank(&left.device_type)
            .cmp(&carrier_rank(&right.device_type))
            .then_with(|| {
                location_rank(left.location.as_deref(), &context.home_location).cmp(&location_rank(
                    right.location.as_deref(),
                    &context.home_location,
                ))
            })
            .then_with(|| right.attach_capacity.cmp(&left.attach_capacity))
            .then_with(|| left.code.cmp(&right.code))
    });
    let mut selected = Vec::new();
    let mut capacity = 0;
    for device in candidates {
        selected.push(SelectedTransport {
            code: device.code.clone(),
            device_type: device.device_type.clone(),
            capacity: device.attach_capacity,
            must_print: false,
        });
        capacity += device.attach_capacity;
        if capacity >= slots {
            break;
        }
    }
    if selected.is_empty() {
        let blueprint = context
            .blueprints
            .get(SURGE_CARRIER)
            .ok_or_else(|| PlannerError::MissingBlueprint(SURGE_CARRIER.into()))?;
        if blueprint.attach_capacity <= 0 {
            return Err(PlannerError::InvalidTransportBlueprint {
                device_type: SURGE_CARRIER.into(),
                capacity_kind: "attachment",
            });
        }
        prints.push(DeviceRequirement {
            device_type: SURGE_CARRIER.into(),
            count: 1,
        });
        selected.push(SelectedTransport {
            code: "<print:surge_carrier>".into(),
            device_type: SURGE_CARRIER.into(),
            capacity: blueprint.attach_capacity,
            must_print: true,
        });
        capacity = blueprint.attach_capacity;
    }
    Ok(TransportPlan {
        transports: selected,
        capacity_per_trip: capacity,
        inbound_trips: trips(slots, capacity),
        outbound_trips: 0,
    })
}

fn plan_beacon(event_location: &str, context: &PlanningContext) -> BeaconPlan {
    let mut beacons = context
        .devices
        .iter()
        .filter(|device| device.device_type == FTL_BEACON)
        .collect::<Vec<_>>();
    beacons.sort_by(|left, right| left.code.cmp(&right.code));
    if let Some(beacon) = beacons.iter().find(|device| {
        device.location.as_deref() == Some(event_location)
            && device.status.as_deref().is_some_and(|status| {
                matches!(
                    status.to_ascii_lowercase().as_str(),
                    "active" | "beaconing" | "monitoring"
                )
            })
    }) {
        return BeaconPlan {
            action: BeaconAction::AlreadyActive,
            device_code: Some(beacon.code.clone()),
            transport_slots: 0,
            warning: None,
        };
    }
    if let Some(beacon) = beacons.iter().find(|device| {
        device.location.as_deref() == Some(event_location)
            && device.is_inactive()
            && device.is_free_standing()
            && !device.is_reserved_for_workflow(&context.mission_tag_prefix, None)
    }) {
        return BeaconPlan {
            action: BeaconAction::DeployExisting,
            device_code: Some(beacon.code.clone()),
            transport_slots: 0,
            warning: None,
        };
    }
    if let Some(beacon) = beacons.iter().find(|device| {
        device.is_inactive()
            && device.is_free_standing()
            && device.location.as_deref() == Some(context.home_location.as_str())
            && !device.is_reserved_for_workflow(&context.mission_tag_prefix, None)
    }) {
        return BeaconPlan {
            action: BeaconAction::TransportExisting,
            device_code: Some(beacon.code.clone()),
            transport_slots: 1,
            warning: None,
        };
    }
    if !context.blueprints.contains_key(FTL_BEACON) {
        return BeaconPlan {
            action: BeaconAction::Unavailable,
            device_code: None,
            transport_slots: 0,
            warning: Some("beacon objective skipped: FTL beacon blueprint is not unlocked".into()),
        };
    }
    BeaconPlan {
        action: BeaconAction::PrintAndTransport,
        device_code: None,
        transport_slots: 1,
        warning: None,
    }
}

fn assign_recommendations(criteria: &mut [CriterionAssessment]) {
    if !criteria.iter().any(|criterion| criterion.feasible) {
        return;
    }
    let fastest = min_indices(criteria, |item| {
        FloatKey(item.print_schedule.makespan_seconds)
    });
    let lowest_cost = min_indices(criteria, |item| {
        sum_resources(&item.manufacturing_resources)
    });
    let lowest_rare = min_indices(criteria, rare_cost_key);
    let fewest_prints = min_indices(criteria, CriterionAssessment::print_count);
    let fewest_trips = min_indices(criteria, CriterionAssessment::total_trips);
    let best_stock = max_indices(criteria, |item| item.reused_devices.len());
    add_badge(criteria, fastest, Recommendation::Fastest);
    add_badge(
        criteria,
        lowest_cost,
        Recommendation::LowestManufacturingCost,
    );
    add_badge(criteria, lowest_rare, Recommendation::LowestRareResourceUse);
    add_badge(criteria, fewest_prints, Recommendation::FewestPrints);
    add_badge(criteria, fewest_trips, Recommendation::FewestTrips);
    add_badge(criteria, best_stock, Recommendation::UsesExistingStockBest);
}

fn add_badge(
    criteria: &mut [CriterionAssessment],
    indices: Vec<usize>,
    recommendation: Recommendation,
) {
    for index in indices {
        criteria[index].recommendations.insert(recommendation);
    }
}

fn min_indices<T: Ord>(
    criteria: &[CriterionAssessment],
    key: impl Fn(&CriterionAssessment) -> T,
) -> Vec<usize> {
    let keys = criteria
        .iter()
        .enumerate()
        .filter(|(_, criterion)| criterion.feasible)
        .map(|(index, criterion)| (index, key(criterion)))
        .collect::<Vec<_>>();
    let Some(minimum) = keys.iter().map(|(_, value)| value).min() else {
        return Vec::new();
    };
    keys.iter()
        .filter_map(|(index, value)| (value == minimum).then_some(*index))
        .collect()
}

fn max_indices<T: Ord>(
    criteria: &[CriterionAssessment],
    key: impl Fn(&CriterionAssessment) -> T,
) -> Vec<usize> {
    let keys = criteria
        .iter()
        .enumerate()
        .filter(|(_, criterion)| criterion.feasible)
        .map(|(index, criterion)| (index, key(criterion)))
        .collect::<Vec<_>>();
    let Some(maximum) = keys.iter().map(|(_, value)| value).max() else {
        return Vec::new();
    };
    keys.iter()
        .filter_map(|(index, value)| (value == maximum).then_some(*index))
        .collect()
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct FloatKey(f64);

impl Eq for FloatKey {}

impl Ord for FloatKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialOrd for FloatKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn rare_cost_key(item: &CriterionAssessment) -> (i64, i64, i64, i64) {
    let mut consumed = item.manufacturing_resources.clone();
    merge_resources(&mut consumed, &item.remaining_resources);
    (
        *consumed.get("rares").unwrap_or(&0),
        *consumed.get("volatiles").unwrap_or(&0),
        *consumed.get("conductive").unwrap_or(&0),
        sum_resources(&consumed),
    )
}

#[derive(Clone, Debug, Default)]
struct ProgressSnapshot {
    global: ProgressValues,
    criteria: BTreeMap<String, ProgressValues>,
}

impl ProgressSnapshot {
    fn for_criterion(&self, name: &str) -> ProgressValues {
        let mut result = self.global.clone();
        if let Some(specific) = self.criteria.get(name) {
            merge_resources(&mut result.resources, &specific.resources);
            merge_device_counts(&mut result.devices, &specific.devices);
        }
        result
    }
}

#[derive(Clone, Debug, Default)]
struct ProgressValues {
    resources: ResourceMap,
    devices: BTreeMap<String, i64>,
}

fn extract_progress(progress: Option<&Value>) -> ProgressSnapshot {
    let Some(Value::Object(object)) = progress else {
        return ProgressSnapshot::default();
    };
    let mut snapshot = ProgressSnapshot {
        global: parse_progress_values(object),
        ..ProgressSnapshot::default()
    };
    for (key, value) in object {
        if let Value::Object(specific) = value {
            let parsed = parse_progress_values(specific);
            if !parsed.resources.is_empty() || !parsed.devices.is_empty() {
                snapshot.criteria.insert(key.clone(), parsed);
            }
        }
    }
    for key in ["criteria", "options"] {
        if let Some(Value::Array(criteria)) = object.get(key) {
            for criterion in criteria {
                let Some(criterion) = criterion.as_object() else {
                    continue;
                };
                let Some(name) = criterion.get("name").and_then(Value::as_str) else {
                    continue;
                };
                let parsed = parse_progress_values(criterion);
                if !parsed.resources.is_empty() || !parsed.devices.is_empty() {
                    snapshot.criteria.insert(name.to_owned(), parsed);
                }
            }
        }
    }
    snapshot
}

fn parse_progress_values(object: &Map<String, Value>) -> ProgressValues {
    let mut result = ProgressValues::default();
    for key in [
        "resources",
        "contributed_resources",
        "consumed_resources",
        "delivered_resources",
    ] {
        if let Some(value) = object.get(key) {
            merge_resources(&mut result.resources, &progress_resource_map(value));
        }
    }
    for key in [
        "devices",
        "contributed_devices",
        "consumed_devices",
        "delivered_devices",
    ] {
        if let Some(value) = object.get(key) {
            merge_device_counts(&mut result.devices, &progress_device_map(value));
        }
    }
    result
}

fn progress_resource_map(value: &Value) -> ResourceMap {
    match value {
        Value::Object(_) => numeric_map(Some(value)),
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_object)
            .filter_map(|item| {
                let resource = ["resource_type", "resource", "type"]
                    .into_iter()
                    .find_map(|key| item.get(key).and_then(Value::as_str))?;
                let quantity = ["current", "quantity", "count", "delivered"]
                    .into_iter()
                    .find_map(|key| item.get(key).and_then(value_to_i64))
                    .unwrap_or(0);
                (quantity > 0).then_some((resource.to_owned(), quantity))
            })
            .collect(),
        _ => ResourceMap::new(),
    }
}

fn progress_device_map(value: &Value) -> BTreeMap<String, i64> {
    match value {
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_object)
            .filter_map(|item| {
                let device_type = ["device_type", "type"]
                    .into_iter()
                    .find_map(|key| item.get(key).and_then(Value::as_str))?;
                let count = ["current", "count", "quantity", "delivered"]
                    .into_iter()
                    .find_map(|key| item.get(key).and_then(value_to_i64))
                    .unwrap_or(0);
                (count > 0).then_some((device_type.to_owned(), count))
            })
            .collect(),
        _ => device_count_map(value),
    }
}

fn parse_criterion(
    object: &Map<String, Value>,
    index: usize,
) -> Result<EventCriterion, PlannerError> {
    let name = object
        .get("name")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| format!("option_{}", index + 1));
    let resources = numeric_map(object.get("resources"));
    let devices = object
        .get("devices")
        .map(parse_device_requirements)
        .transpose()?
        .unwrap_or_default();
    Ok(EventCriterion {
        name,
        resources,
        devices,
    })
}

fn parse_rewards(object: &Map<String, Value>) -> EventRewards {
    EventRewards {
        resources: numeric_map(object.get("resources")),
        xp: integer_field(object, "xp"),
        civilisation_points: integer_field(object, "civilisation_points"),
        completion_achievement: object
            .get("completion_achievement")
            .and_then(Value::as_str)
            .map(str::to_owned),
    }
}

fn parse_device_requirements(value: &Value) -> Result<Vec<DeviceRequirement>, PlannerError> {
    let Value::Array(items) = value else {
        return Err(PlannerError::InvalidEvent(
            "criterion devices must be an array".into(),
        ));
    };
    let mut counts = BTreeMap::<String, i64>::new();
    for item in items {
        let Value::Object(object) = item else {
            return Err(PlannerError::InvalidEvent(
                "criterion device entry must be an object".into(),
            ));
        };
        let device_type = object
            .get("device_type")
            .and_then(Value::as_str)
            .ok_or_else(|| PlannerError::InvalidEvent("device_type is missing".into()))?;
        let count = integer_field(object, "count").unwrap_or(1);
        if count > 0 {
            *counts.entry(device_type.to_owned()).or_default() += count;
        }
    }
    Ok(counts
        .into_iter()
        .map(|(device_type, count)| DeviceRequirement { device_type, count })
        .collect())
}

fn device_count_map(value: &Value) -> BTreeMap<String, i64> {
    match value {
        Value::Array(_) => parse_device_requirements(value)
            .unwrap_or_default()
            .into_iter()
            .map(|item| (item.device_type, item.count))
            .collect(),
        Value::Object(object) => object
            .iter()
            .filter_map(|(key, value)| value_to_i64(value).map(|count| (key.clone(), count)))
            .collect(),
        _ => BTreeMap::new(),
    }
}

fn numeric_map(value: Option<&Value>) -> ResourceMap {
    let Some(Value::Object(object)) = value else {
        return ResourceMap::new();
    };
    object
        .iter()
        .filter_map(|(key, value)| value_to_i64(value).map(|quantity| (key.clone(), quantity)))
        .filter(|(_, quantity)| *quantity > 0)
        .collect()
}

fn integer_field(object: &Map<String, Value>, key: &str) -> Option<i64> {
    object.get(key).and_then(value_to_i64)
}

fn value_to_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_u64()
            .and_then(|number| i64::try_from(number).ok())
            .or_else(|| value.as_f64().map(|number| number.round() as i64))
    })
}

fn inactive_devices_at(
    devices: &[DeviceStock],
    location: &str,
    mission_tag_prefix: &str,
) -> BTreeMap<String, i64> {
    let mut counts = BTreeMap::new();
    for device in devices.iter().filter(|device| {
        device.location.as_deref() == Some(location)
            && device.is_inactive()
            && device.is_free_standing()
            && !device.is_reserved_for_workflow(mission_tag_prefix, None)
    }) {
        *counts.entry(device.device_type.clone()).or_default() += 1;
    }
    counts
}

fn subtract_resources(
    required: &ResourceMap,
    progress: &ResourceMap,
    inventory: &ResourceMap,
) -> ResourceMap {
    required
        .iter()
        .filter_map(|(resource, quantity)| {
            let satisfied =
                (*progress.get(resource).unwrap_or(&0)).max(*inventory.get(resource).unwrap_or(&0));
            let remaining = quantity.saturating_sub(satisfied);
            (remaining > 0).then_some((resource.clone(), remaining))
        })
        .collect()
}

fn subtract_device_requirements(
    required: &[DeviceRequirement],
    progress: &BTreeMap<String, i64>,
    event_stock: &BTreeMap<String, i64>,
) -> Vec<DeviceRequirement> {
    required
        .iter()
        .filter_map(|item| {
            let satisfied = (*progress.get(&item.device_type).unwrap_or(&0))
                .max(*event_stock.get(&item.device_type).unwrap_or(&0));
            let remaining = item.count.saturating_sub(satisfied);
            (remaining > 0).then_some(DeviceRequirement {
                device_type: item.device_type.clone(),
                count: remaining,
            })
        })
        .collect()
}

/// Expands one blueprint quantity into its total raw-resource cost.
pub fn blueprint_resource_cost(
    device_type: &str,
    quantity: i64,
    blueprints: &BTreeMap<String, BlueprintSpec>,
) -> Result<ResourceMap, PlannerError> {
    let mut visiting = BTreeSet::new();
    expand_blueprint_resources_inner(device_type, quantity, blueprints, &mut visiting)
}

fn expand_blueprint_resources(
    device_type: &str,
    quantity: i64,
    blueprints: &BTreeMap<String, BlueprintSpec>,
) -> Result<ResourceMap, PlannerError> {
    blueprint_resource_cost(device_type, quantity, blueprints)
}

fn expand_blueprint_resources_inner(
    device_type: &str,
    quantity: i64,
    blueprints: &BTreeMap<String, BlueprintSpec>,
    visiting: &mut BTreeSet<String>,
) -> Result<ResourceMap, PlannerError> {
    let blueprint = blueprints
        .get(device_type)
        .ok_or_else(|| PlannerError::MissingBlueprint(device_type.to_owned()))?;
    if !visiting.insert(device_type.to_owned()) {
        return Err(PlannerError::ComponentCycle(device_type.to_owned()));
    }
    let mut result = blueprint
        .resources
        .iter()
        .map(|(resource, amount)| (resource.clone(), amount.saturating_mul(quantity)))
        .collect::<ResourceMap>();
    for (component, count) in &blueprint.components {
        let required = count.saturating_mul(quantity);
        if blueprints.contains_key(component) {
            let component_resources =
                expand_blueprint_resources_inner(component, required, blueprints, visiting)?;
            merge_resources(&mut result, &component_resources);
        } else {
            *result.entry(component.clone()).or_default() += required;
        }
    }
    visiting.remove(device_type);
    Ok(result)
}

fn subtract_resource_map(total: &ResourceMap, subtraction: &ResourceMap) -> ResourceMap {
    total
        .iter()
        .filter_map(|(resource, quantity)| {
            let remaining = quantity.saturating_sub(*subtraction.get(resource).unwrap_or(&0));
            (remaining > 0).then_some((resource.clone(), remaining))
        })
        .collect()
}

fn resource_shortages(
    available: &ResourceMap,
    event_resources: &ResourceMap,
    manufacturing_resources: &ResourceMap,
) -> ResourceMap {
    let mut required = event_resources.clone();
    merge_resources(&mut required, manufacturing_resources);
    required
        .into_iter()
        .filter_map(|(resource, quantity)| {
            let shortage = quantity.saturating_sub(*available.get(&resource).unwrap_or(&0));
            (shortage > 0).then_some((resource, shortage))
        })
        .collect()
}

fn merge_resources(target: &mut ResourceMap, addition: &ResourceMap) {
    for (resource, amount) in addition {
        *target.entry(resource.clone()).or_default() += amount;
    }
}

fn merge_device_counts(target: &mut BTreeMap<String, i64>, addition: &BTreeMap<String, i64>) {
    for (device_type, count) in addition {
        *target.entry(device_type.clone()).or_default() += count;
    }
}

fn increment_requirement(requirements: &mut Vec<DeviceRequirement>, device_type: &str, count: i64) {
    if let Some(existing) = requirements
        .iter_mut()
        .find(|item| item.device_type == device_type)
    {
        existing.count += count;
    } else {
        requirements.push(DeviceRequirement {
            device_type: device_type.to_owned(),
            count,
        });
    }
}

fn remove_requirement(requirements: &mut Vec<DeviceRequirement>, device_type: &str, count: i64) {
    if let Some(existing) = requirements
        .iter_mut()
        .find(|item| item.device_type == device_type)
    {
        existing.count = existing.count.saturating_sub(count);
    }
    requirements.retain(|item| item.count > 0);
}

fn sum_resources(resources: &ResourceMap) -> i64 {
    resources.values().copied().sum()
}

fn trips(quantity: i64, capacity: i64) -> i64 {
    if quantity <= 0 {
        return 0;
    }

    let quantity = u64::try_from(quantity).expect("positive quantity fits in u64");
    let capacity = u64::try_from(capacity).expect("transport capacity must be positive");
    i64::try_from(quantity.div_ceil(capacity)).expect("trip count fits in i64")
}

fn location_rank(location: Option<&str>, home: &str) -> u8 {
    match location {
        Some(location) if location == home => 0,
        Some(_) => 1,
        None => 2,
    }
}

fn same_system(left: &str, right: &str) -> bool {
    system_designation(left).eq_ignore_ascii_case(system_designation(right))
}

fn system_designation(location: &str) -> &str {
    location
        .split('-')
        .next()
        .filter(|system| !system.is_empty())
        .unwrap_or(location)
}

fn carrier_rank(device_type: &str) -> u8 {
    if device_type == SURGE_CARRIER { 0 } else { 1 }
}

fn format_resources(resources: &ResourceMap) -> String {
    resources
        .iter()
        .map(|(resource, quantity)| format!("{quantity} {resource}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn normalize_tag_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect()
}

fn short_hash(value: &str) -> String {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn blueprint(
        device_type: &str,
        print_time_seconds: f64,
        cargo_capacity: i64,
        attach_capacity: i64,
    ) -> BlueprintSpec {
        BlueprintSpec {
            device_type: device_type.into(),
            print_time_seconds,
            cargo_capacity,
            attach_capacity,
            resources: [("structural".into(), 10)].into_iter().collect(),
            ..BlueprintSpec::default()
        }
    }

    fn event() -> EventDefinition {
        let value = json!({
            "criteria": [
                {
                    "name": "sensor_shield_containment",
                    "resources": {"conductive": 150},
                    "devices": [
                        {"count": 1, "device_type": "sensor_array"},
                        {"count": 1, "device_type": "shield_generator"},
                        {"count": 1, "device_type": "exotic_particle_trap"}
                    ]
                },
                {
                    "name": "deep_scan_containment",
                    "resources": {"silicates": 200},
                    "devices": [
                        {"count": 2, "device_type": "sensor_array"},
                        {"count": 1, "device_type": "negative_energy_conduit"}
                    ]
                }
            ],
            "rewards": {
                "resources": {"conductive": 300, "rares": 600},
                "xp": 2500,
                "civilisation_points": 3,
                "completion_achievement": "exotic_storm_contained"
            }
        });
        let object = value.as_object().expect("object");
        let criteria = object["criteria"]
            .as_array()
            .expect("criteria")
            .iter()
            .map(|value| value.as_object().expect("criterion").clone())
            .collect::<Vec<_>>();
        EventDefinition::from_open_fields(OpenEventFields {
            designation: "WIXUKHHU-4-EVT-002".into(),
            location: "WIXUKHHU-4".into(),
            title: "Exotic Particle Storm".into(),
            description: None,
            event_type: Some("exotic_particle_storm".into()),
            tier: Some(1),
            status: Some("active".into()),
            criteria: &criteria,
            progress: None,
            rewards: object["rewards"].as_object(),
        })
        .expect("event")
    }

    fn context() -> PlanningContext {
        let mut blueprints = BTreeMap::new();
        for device_type in [
            "sensor_array",
            "shield_generator",
            "exotic_particle_trap",
            "negative_energy_conduit",
            FTL_BEACON,
        ] {
            blueprints.insert(device_type.into(), blueprint(device_type, 800.0, 0, 0));
        }
        blueprints.insert(
            CARGO_FREIGHTER.into(),
            blueprint(CARGO_FREIGHTER, 800.0, 500, 0),
        );
        blueprints.insert(SURGE_CARRIER.into(), blueprint(SURGE_CARRIER, 800.0, 0, 9));
        PlanningContext {
            home_inventory: [
                ("conductive".into(), 100_000),
                ("silicates".into(), 100_000),
                ("structural".into(), 100_000),
            ]
            .into_iter()
            .collect(),
            event_inventory: ResourceMap::new(),
            blueprints,
            devices: vec![
                DeviceStock {
                    code: "CF-AMI".into(),
                    device_type: CARGO_FREIGHTER.into(),
                    status: Some("idle".into()),
                    location: Some("SCEPTURUM-BELT-1".into()),
                    assigned_replicant: Some("Chats-1".into()),
                    tags: BTreeSet::new(),
                    cargo_capacity: 500,
                    attach_capacity: 0,
                    attach_used: 0,
                    attached_to_device_code: None,
                    stowed_in_device_code: None,
                    controlled_by_ami: true,
                    travelling: false,
                },
                DeviceStock {
                    code: "CF-FREE-1".into(),
                    device_type: CARGO_FREIGHTER.into(),
                    status: Some("idle".into()),
                    location: Some("SCEPTURUM-BELT-1".into()),
                    assigned_replicant: Some("Chats-1".into()),
                    tags: BTreeSet::new(),
                    cargo_capacity: 500,
                    attach_capacity: 0,
                    attach_used: 0,
                    attached_to_device_code: None,
                    stowed_in_device_code: None,
                    controlled_by_ami: false,
                    travelling: false,
                },
                DeviceStock {
                    code: "CF-FREE-2".into(),
                    device_type: CARGO_FREIGHTER.into(),
                    status: Some("idle".into()),
                    location: Some("SCEPTURUM-BELT-1".into()),
                    assigned_replicant: Some("Chats-1".into()),
                    tags: BTreeSet::new(),
                    cargo_capacity: 500,
                    attach_capacity: 0,
                    attach_used: 0,
                    attached_to_device_code: None,
                    stowed_in_device_code: None,
                    controlled_by_ami: false,
                    travelling: false,
                },
                DeviceStock {
                    code: "SC-1".into(),
                    device_type: SURGE_CARRIER.into(),
                    status: Some("idle".into()),
                    location: Some("SCEPTURUM-BELT-1".into()),
                    assigned_replicant: Some("Chats-1".into()),
                    tags: BTreeSet::new(),
                    cargo_capacity: 0,
                    attach_capacity: 9,
                    attach_used: 0,
                    attached_to_device_code: None,
                    stowed_in_device_code: None,
                    controlled_by_ami: false,
                    travelling: false,
                },
            ],
            factories: vec![
                FactoryWorkload {
                    code: "AF-1".into(),
                    remaining_seconds: 800.0,
                },
                FactoryWorkload {
                    code: "AF-2".into(),
                    remaining_seconds: 0.0,
                },
            ],
            earned_achievements: BTreeSet::new(),
            home_location: "SCEPTURUM-BELT-1".into(),
            mission_tag_prefix: "evt-m:".into(),
        }
    }

    #[test]
    fn parses_multiple_event_criteria_and_rewards() {
        let event = event();
        assert_eq!(event.criteria.len(), 2);
        assert_eq!(event.rewards.resources["rares"], 600);
        assert_eq!(
            event.rewards.completion_achievement.as_deref(),
            Some("exotic_storm_contained")
        );
    }

    #[test]
    fn excludes_ami_controlled_freighters() {
        let plan = plan_event(event(), &context()).expect("plan");
        let cargo = &plan.criteria[0].cargo;
        assert_eq!(cargo.capacity_per_trip, 1_000);
        assert!(cargo.transports.iter().all(|item| item.code != "CF-AMI"));
    }

    #[test]
    fn calculates_two_freighters_for_nine_hundred_reward_units() {
        let plan = plan_event(event(), &context()).expect("plan");
        assert_eq!(plan.criteria[0].cargo.transports.len(), 2);
        assert_eq!(plan.criteria[0].cargo.outbound_trips, 1);
    }

    #[test]
    fn uses_multiple_trips_instead_of_printing_extra_freighters() {
        let mut context = context();
        context.devices.retain(|device| device.code != "CF-FREE-2");
        let plan = plan_event(event(), &context).expect("plan");
        let cargo = &plan.criteria[0].cargo;
        assert_eq!(cargo.transports.len(), 1);
        assert_eq!(cargo.outbound_trips, 2);
        assert!(!cargo.transports[0].must_print);
    }

    #[test]
    fn subtracts_destination_inventory_and_progress_without_double_counting() {
        let mut event = event();
        event.progress = Some(json!({
            "met": false,
            "options": [{
                "name": "deep_scan_containment",
                "resources": [{
                    "resource_type": "silicates",
                    "current": 25,
                    "required": 200,
                    "met": false
                }],
                "devices": [{
                    "device_type": "sensor_array",
                    "current": 1,
                    "required": 2,
                    "met": false
                }]
            }]
        }));
        let mut context = context();
        context.event_inventory.insert("silicates".into(), 50);
        context.devices.push(DeviceStock {
            code: "NEC-AT-EVENT".into(),
            device_type: "negative_energy_conduit".into(),
            status: Some("inactive".into()),
            location: Some("WIXUKHHU-4".into()),
            assigned_replicant: Some("Chats-1".into()),
            tags: BTreeSet::new(),
            cargo_capacity: 0,
            attach_capacity: 0,
            attach_used: 0,
            attached_to_device_code: None,
            stowed_in_device_code: None,
            controlled_by_ami: false,
            travelling: false,
        });
        let plan = plan_event(event, &context).expect("plan");
        let deep = plan
            .criteria
            .iter()
            .find(|item| item.criterion_name == "deep_scan_containment")
            .expect("criterion");
        assert_eq!(deep.remaining_resources["silicates"], 150);
        assert_eq!(
            deep.remaining_devices,
            vec![DeviceRequirement {
                device_type: "sensor_array".into(),
                count: 1
            }]
        );
    }

    #[test]
    fn occupied_carrier_is_not_selected() {
        let mut context = context();
        let carrier = context
            .devices
            .iter_mut()
            .find(|device| device.code == "SC-1")
            .expect("carrier");
        carrier.attach_used = 1;
        let plan = plan_event(event(), &context).expect("plan");
        assert!(plan.criteria.iter().all(|criterion| {
            criterion
                .carriers
                .transports
                .iter()
                .all(|transport| transport.code != "SC-1")
        }));
        assert!(plan.criteria.iter().all(|criterion| {
            criterion
                .carriers
                .transports
                .iter()
                .any(|transport| transport.must_print && transport.device_type == SURGE_CARRIER)
        }));
    }

    #[test]
    fn compacted_modular_device_is_inactive_stock() {
        let device = DeviceStock {
            code: "AF-1".into(),
            device_type: "autofactory".into(),
            status: Some("compacted".into()),
            location: Some("SCEPTURUM-BELT-1".into()),
            assigned_replicant: Some("Chats-1".into()),
            tags: BTreeSet::new(),
            cargo_capacity: 0,
            attach_capacity: 0,
            attach_used: 0,
            attached_to_device_code: None,
            stowed_in_device_code: None,
            controlled_by_ami: false,
            travelling: false,
        };

        assert!(device.is_inactive());
        assert!(device.is_free_standing());
    }

    #[test]
    fn payload_reuse_is_limited_to_free_unreserved_stock_at_the_home_hub() {
        let mut context = context();
        for (code, location, tags, attached_to_device_code) in [
            ("SENSOR-REMOTE", "RHWYRHYR-5-L4", BTreeSet::new(), None),
            (
                "SENSOR-SAME-SYSTEM",
                "SCEPTURUM-7-L4",
                BTreeSet::new(),
                None,
            ),
            (
                "SENSOR-BOOTSTRAP",
                "SCEPTURUM-BELT-1",
                ["boot-m:regional-beta".into()].into_iter().collect(),
                None,
            ),
            (
                "SENSOR-ATTACHED",
                "SCEPTURUM-BELT-1",
                BTreeSet::new(),
                Some("OTHER-CARRIER".into()),
            ),
            ("SENSOR-HOME", "SCEPTURUM-BELT-1", BTreeSet::new(), None),
        ] {
            context.devices.push(DeviceStock {
                code: code.into(),
                device_type: "sensor_array".into(),
                status: Some("inactive".into()),
                location: Some(location.into()),
                assigned_replicant: Some("Chats-1".into()),
                tags,
                cargo_capacity: 0,
                attach_capacity: 0,
                attach_used: 0,
                attached_to_device_code,
                stowed_in_device_code: None,
                controlled_by_ami: false,
                travelling: false,
            });
        }

        let plan = plan_event(event(), &context).expect("plan");
        let criterion = plan
            .criteria
            .iter()
            .find(|criterion| criterion.criterion_name == "sensor_shield_containment")
            .expect("criterion");
        assert_eq!(criterion.reused_devices, vec!["SENSOR-HOME".to_owned()]);
    }

    #[test]
    fn remote_transports_and_bootstrap_carriers_are_not_selected() {
        let mut context = context();
        context.devices.push(DeviceStock {
            code: "CF-BETA".into(),
            device_type: CARGO_FREIGHTER.into(),
            status: Some("idle".into()),
            location: Some("RHWYRHYR-5-L4".into()),
            assigned_replicant: Some("Chats-3".into()),
            tags: BTreeSet::new(),
            cargo_capacity: 5_000,
            attach_capacity: 0,
            attach_used: 0,
            attached_to_device_code: None,
            stowed_in_device_code: None,
            controlled_by_ami: false,
            travelling: false,
        });
        let carrier = context
            .devices
            .iter_mut()
            .find(|device| device.code == "SC-1")
            .expect("carrier");
        carrier.tags.insert("boot-m:regional-beta".into());

        let plan = plan_event(event(), &context).expect("plan");
        assert!(plan.criteria.iter().all(|criterion| {
            criterion
                .cargo
                .transports
                .iter()
                .all(|transport| transport.code != "CF-BETA")
        }));
        assert!(plan.criteria.iter().all(|criterion| {
            criterion
                .carriers
                .transports
                .iter()
                .all(|transport| transport.code != "SC-1")
        }));
    }

    #[test]
    fn monitoring_beacon_satisfies_secondary_objective() {
        let mut context = context();
        context.devices.push(DeviceStock {
            code: "BEACON-1".into(),
            device_type: FTL_BEACON.into(),
            status: Some("monitoring".into()),
            location: Some("WIXUKHHU-4".into()),
            assigned_replicant: Some("Chats-1".into()),
            tags: BTreeSet::new(),
            cargo_capacity: 0,
            attach_capacity: 0,
            attach_used: 0,
            attached_to_device_code: None,
            stowed_in_device_code: None,
            controlled_by_ami: false,
            travelling: false,
        });
        let plan = plan_event(event(), &context).expect("plan");
        assert!(plan.criteria.iter().all(|criterion| {
            criterion.beacon.action == BeaconAction::AlreadyActive
                && criterion.beacon.device_code.as_deref() == Some("BEACON-1")
        }));
    }

    #[test]
    fn one_surge_carrier_handles_three_devices_and_beacon() {
        let plan = plan_event(event(), &context()).expect("plan");
        assert_eq!(plan.criteria[0].carriers.transports.len(), 1);
        assert_eq!(plan.criteria[0].carriers.capacity_per_trip, 9);
        assert_eq!(plan.criteria[0].carriers.inbound_trips, 1);
    }

    #[test]
    fn marks_unearned_achievement() {
        let plan = plan_event(event(), &context()).expect("plan");
        assert!(plan.grants_unearned_achievement);
    }

    #[test]
    fn balances_equal_prints_against_existing_workload() {
        let factories = vec![
            FactoryWorkload {
                code: "AF-1".into(),
                remaining_seconds: 800.0,
            },
            FactoryWorkload {
                code: "AF-2".into(),
                remaining_seconds: 0.0,
            },
        ];
        let units = (0..9)
            .map(|_| PrintUnit {
                device_type: "ftl_relay".into(),
                duration_seconds: 800.0,
            })
            .collect();
        let schedule = schedule_print_units(&factories, units).expect("schedule");
        let quantities = schedule
            .batches
            .iter()
            .map(|batch| (batch.factory_code.as_str(), batch.quantity))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(quantities["AF-1"], 4);
        assert_eq!(quantities["AF-2"], 5);
        assert_eq!(schedule.makespan_seconds, 4_000.0);
    }

    #[test]
    fn one_blocked_criterion_does_not_hide_feasible_alternatives() {
        let mut event = event();
        event.criteria[0].devices.push(DeviceRequirement {
            device_type: "missing_blueprint_device".into(),
            count: 1,
        });
        let plan = plan_event(event, &context()).expect("plan");
        let blocked = plan
            .criteria
            .iter()
            .find(|criterion| criterion.criterion_name == "sensor_shield_containment")
            .expect("blocked criterion");
        let feasible = plan
            .criteria
            .iter()
            .find(|criterion| criterion.criterion_name == "deep_scan_containment")
            .expect("feasible criterion");
        assert!(!blocked.feasible);
        assert!(blocked.recommendations.is_empty());
        assert!(
            blocked
                .blockers
                .iter()
                .any(|blocker| blocker.contains("missing_blueprint_device"))
        );
        assert!(feasible.feasible);
        assert!(!feasible.recommendations.is_empty());
    }

    #[test]
    fn missing_event_materials_block_only_the_affected_criterion() {
        let mut context = context();
        context.home_inventory.remove("conductive");
        let plan = plan_event(event(), &context).expect("plan");
        let blocked = plan
            .criteria
            .iter()
            .find(|criterion| criterion.criterion_name == "sensor_shield_containment")
            .expect("blocked criterion");
        let feasible = plan
            .criteria
            .iter()
            .find(|criterion| criterion.criterion_name == "deep_scan_containment")
            .expect("feasible criterion");
        assert!(!blocked.feasible);
        assert!(
            blocked
                .blockers
                .iter()
                .any(|blocker| blocker.contains("conductive"))
        );
        assert!(feasible.feasible);
    }

    #[test]
    fn missing_beacon_blueprint_is_a_non_blocking_warning() {
        let mut context = context();
        context.blueprints.remove(FTL_BEACON);
        let plan = plan_event(event(), &context).expect("plan");
        assert!(plan.criteria.iter().all(|criterion| criterion.feasible));
        assert!(plan.criteria.iter().all(|criterion| {
            criterion.beacon.action == BeaconAction::Unavailable
                && criterion
                    .warnings
                    .iter()
                    .any(|warning| warning.contains("beacon"))
        }));
    }

    #[test]
    fn generated_tags_fit_api_limit() {
        assert!(mission_tag("a very long mission identifier").len() <= MAX_TAG_CHARACTERS);
        assert!(
            role_tag("a very long role name that would otherwise overflow").len()
                <= MAX_TAG_CHARACTERS
        );
    }
}
