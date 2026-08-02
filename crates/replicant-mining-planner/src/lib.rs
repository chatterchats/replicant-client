//! Pure planning primitives for repeatable mining-network expansion.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// AMI mining controller blueprint identifier.
pub const MINING_CONTROLLER: &str = "ami_mining_controller";
/// Mining drone blueprint identifier.
pub const MINING_DRONE: &str = "mining_drone";
/// AMI survey controller blueprint identifier.
pub const SURVEY_CONTROLLER: &str = "ami_survey_controller";
/// Survey drone blueprint identifier.
pub const SURVEY_DRONE: &str = "survey_drone";
/// Maintenance drone blueprint identifier.
pub const MAINTENANCE_DRONE: &str = "maintenance_drone";
/// AMI transport controller blueprint identifier.
pub const TRANSPORT_CONTROLLER: &str = "ami_transport_controller";
/// Cargo Freighter blueprint identifier.
pub const CARGO_FREIGHTER: &str = "cargo_freighter";
/// Surge Carrier blueprint identifier.
pub const SURGE_CARRIER: &str = "surge_carrier";

/// Integer quantities keyed by canonical resource or device type.
pub type QuantityMap = BTreeMap<String, i64>;

/// One printable blueprint's planning data.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BlueprintSpec {
    /// Canonical device type.
    pub device_type: String,
    /// Seconds required for one unit.
    pub print_time_seconds: f64,
    /// Raw resource inputs per unit.
    pub resources: QuantityMap,
    /// Printable component inputs per unit.
    pub components: QuantityMap,
}

/// Existing queued work on an Autofactory.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FactoryWorkload {
    /// Autofactory device code.
    pub code: String,
    /// Estimated seconds until newly appended work can begin.
    pub remaining_seconds: f64,
}

/// A quantity-batched print assignment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PrintBatch {
    /// Autofactory device code.
    pub factory_code: String,
    /// Canonical device type.
    pub device_type: String,
    /// Quantity submitted in one operation.
    pub quantity: i64,
    /// New-work sequence within this factory.
    pub sequence: usize,
    /// Projected total factory workload after the batch.
    pub projected_finish_seconds: f64,
}

/// Complete distributed manufacturing schedule.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PrintSchedule {
    /// Quantity batches grouped by the scheduler.
    pub batches: Vec<PrintBatch>,
    /// Maximum projected finish time across factories.
    pub projected_finish_seconds: f64,
}

/// Planner validation failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PlannerError {
    /// A required blueprint was not supplied.
    #[error("missing blueprint for required device type `{0}`")]
    MissingBlueprint(String),
    /// Manufacturing is required but no Autofactory is available.
    #[error("printing is required but no Autofactory is available")]
    NoAutofactory,
    /// A component dependency cycle was encountered.
    #[error("blueprint component cycle includes `{0}`")]
    ComponentCycle(String),
}

/// The canonical nine-device mining site requirement.
#[must_use]
pub fn mining_site_requirements() -> QuantityMap {
    [
        (MINING_CONTROLLER.to_owned(), 1),
        (MINING_DRONE.to_owned(), 4),
        (SURVEY_CONTROLLER.to_owned(), 1),
        (SURVEY_DRONE.to_owned(), 2),
        (MAINTENANCE_DRONE.to_owned(), 1),
    ]
    .into_iter()
    .collect()
}

/// Returns positive shortages after subtracting reusable stock.
#[must_use]
pub fn shortages(required: &QuantityMap, reusable: &QuantityMap) -> QuantityMap {
    required
        .iter()
        .filter_map(|(device_type, quantity)| {
            let missing = quantity.saturating_sub(*reusable.get(device_type).unwrap_or(&0));
            (missing > 0).then_some((device_type.clone(), missing))
        })
        .collect()
}

/// Multiplies a requirement map by a non-negative site count.
#[must_use]
pub fn multiply(requirements: &QuantityMap, count: i64) -> QuantityMap {
    requirements
        .iter()
        .map(|(device_type, quantity)| (device_type.clone(), quantity.saturating_mul(count.max(0))))
        .collect()
}

/// Balances individual print units against existing factory workloads and
/// combines adjacent equal-type units on the same factory into quantities.
pub fn schedule_prints(
    required: &QuantityMap,
    blueprints: &BTreeMap<String, BlueprintSpec>,
    factories: &[FactoryWorkload],
) -> Result<PrintSchedule, PlannerError> {
    if required.values().all(|quantity| *quantity <= 0) {
        return Ok(PrintSchedule::default());
    }
    if factories.is_empty() {
        return Err(PlannerError::NoAutofactory);
    }

    let mut units = Vec::new();
    for (device_type, quantity) in required {
        if *quantity <= 0 {
            continue;
        }
        let blueprint = blueprints
            .get(device_type)
            .ok_or_else(|| PlannerError::MissingBlueprint(device_type.clone()))?;
        for _ in 0..*quantity {
            units.push((device_type.clone(), blueprint.print_time_seconds.max(0.0)));
        }
    }
    units.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });

    let mut loads = factories
        .iter()
        .map(|factory| (factory.code.clone(), factory.remaining_seconds.max(0.0)))
        .collect::<BTreeMap<_, _>>();
    let mut assigned = BTreeMap::<String, Vec<(String, i64, f64)>>::new();
    for (device_type, seconds) in units {
        let factory = loads
            .iter()
            .min_by(|left, right| left.1.total_cmp(right.1).then_with(|| left.0.cmp(right.0)))
            .map(|(code, _)| code.clone())
            .ok_or(PlannerError::NoAutofactory)?;
        let finish = loads.entry(factory.clone()).or_default();
        *finish += seconds;
        let queue = assigned.entry(factory).or_default();
        if let Some((last_type, quantity, projected)) = queue.last_mut()
            && *last_type == device_type
        {
            *quantity += 1;
            *projected = *finish;
        } else {
            queue.push((device_type, 1, *finish));
        }
    }

    let mut batches = Vec::new();
    for (factory_code, queue) in assigned {
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
    batches.sort_by(|left, right| {
        left.factory_code
            .cmp(&right.factory_code)
            .then_with(|| left.sequence.cmp(&right.sequence))
    });
    Ok(PrintSchedule {
        projected_finish_seconds: loads.values().copied().fold(0.0, f64::max),
        batches,
    })
}

/// Expands blueprint resource and component costs recursively.
pub fn blueprint_resource_cost(
    device_type: &str,
    quantity: i64,
    blueprints: &BTreeMap<String, BlueprintSpec>,
) -> Result<QuantityMap, PlannerError> {
    let mut visiting = Vec::new();
    expand_cost(device_type, quantity.max(0), blueprints, &mut visiting)
}

fn expand_cost(
    device_type: &str,
    quantity: i64,
    blueprints: &BTreeMap<String, BlueprintSpec>,
    visiting: &mut Vec<String>,
) -> Result<QuantityMap, PlannerError> {
    if visiting.iter().any(|item| item == device_type) {
        return Err(PlannerError::ComponentCycle(device_type.to_owned()));
    }
    let blueprint = blueprints
        .get(device_type)
        .ok_or_else(|| PlannerError::MissingBlueprint(device_type.to_owned()))?;
    visiting.push(device_type.to_owned());
    let mut result = blueprint
        .resources
        .iter()
        .map(|(resource, amount)| (resource.clone(), amount.saturating_mul(quantity)))
        .collect::<QuantityMap>();
    for (component, count) in &blueprint.components {
        let nested = expand_cost(
            component,
            count.saturating_mul(quantity),
            blueprints,
            visiting,
        )?;
        add_quantities(&mut result, &nested);
    }
    visiting.pop();
    Ok(result)
}

/// Adds every positive source quantity to a target map.
pub fn add_quantities(target: &mut QuantityMap, source: &QuantityMap) {
    for (name, quantity) in source {
        *target.entry(name.clone()).or_default() += quantity;
    }
}

/// Stable, permanent site reservation tag.
#[must_use]
pub fn site_tag(system: &str) -> String {
    format!("mine-s:{}", system.to_ascii_lowercase())
}

/// Stable role tag for mining automation assets.
#[must_use]
pub fn role_tag(role: &str) -> String {
    format!("mine-r:{role}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blueprint(device_type: &str, seconds: f64) -> BlueprintSpec {
        BlueprintSpec {
            device_type: device_type.into(),
            print_time_seconds: seconds,
            resources: [("structural".into(), 10)].into_iter().collect(),
            components: QuantityMap::new(),
        }
    }

    #[test]
    fn canonical_site_fits_one_surge_carrier() {
        assert_eq!(mining_site_requirements().values().sum::<i64>(), 9);
    }

    #[test]
    fn shortages_only_include_positive_missing_counts() {
        let required = multiply(&mining_site_requirements(), 2);
        let reusable = [
            (MINING_CONTROLLER.into(), 1),
            (MINING_DRONE.into(), 8),
            (SURVEY_CONTROLLER.into(), 5),
        ]
        .into_iter()
        .collect();
        let missing = shortages(&required, &reusable);
        assert_eq!(missing[MINING_CONTROLLER], 1);
        assert!(!missing.contains_key(MINING_DRONE));
        assert!(!missing.contains_key(SURVEY_CONTROLLER));
        assert_eq!(missing[SURVEY_DRONE], 4);
        assert_eq!(missing[MAINTENANCE_DRONE], 2);
    }

    #[test]
    fn balances_long_prints_against_existing_work() {
        let blueprints = [("device".into(), blueprint("device", 100.0))]
            .into_iter()
            .collect();
        let required = [("device".into(), 5)].into_iter().collect();
        let factories = vec![
            FactoryWorkload {
                code: "A".into(),
                remaining_seconds: 200.0,
            },
            FactoryWorkload {
                code: "B".into(),
                remaining_seconds: 0.0,
            },
        ];
        let schedule = schedule_prints(&required, &blueprints, &factories).unwrap();
        let quantities = schedule
            .batches
            .iter()
            .map(|batch| (batch.factory_code.as_str(), batch.quantity))
            .collect::<BTreeMap<_, _>>();
        let a_quantity = quantities["A"];
        let b_quantity = quantities["B"];
        let a_finish = 200 + 100 * a_quantity;
        let b_finish = 100 * b_quantity;
        assert_eq!(a_quantity + b_quantity, 5);
        assert!(b_quantity > a_quantity);
        assert_eq!(a_finish.max(b_finish), 400);
        assert_eq!((a_finish - b_finish).abs(), 100);
        assert_eq!(schedule.projected_finish_seconds, 400.0);
    }

    #[test]
    fn expands_component_costs() {
        let mut blueprints = BTreeMap::new();
        blueprints.insert("part".into(), blueprint("part", 1.0));
        blueprints.insert(
            "device".into(),
            BlueprintSpec {
                device_type: "device".into(),
                print_time_seconds: 1.0,
                resources: [("conductive".into(), 2)].into_iter().collect(),
                components: [("part".into(), 3)].into_iter().collect(),
            },
        );
        let cost = blueprint_resource_cost("device", 2, &blueprints).unwrap();
        assert_eq!(cost["conductive"], 4);
        assert_eq!(cost["structural"], 60);
    }

    #[test]
    fn generated_tags_fit_the_api_limit() {
        assert!(site_tag("XHAKKWUKKXHU").chars().count() <= 32);
        assert!(role_tag("survey-controller").chars().count() <= 32);
    }
}
