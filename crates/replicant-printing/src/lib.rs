//! Reusable distributed Autofactory scheduling and queueing.
//!
//! The pure planning API is always available. Enable the `managed` feature to
//! discover live Autofactories and submit durable print operations through
//! [`replicant_client::Client`].

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[cfg(feature = "managed")]
pub mod managed;

/// Integer quantities keyed by canonical device type.
pub type QuantityMap = BTreeMap<String, i64>;

/// Supplies the duration required to print one device.
pub trait PrintTime {
    /// Returns the duration of one print in seconds.
    fn print_time_seconds(&self) -> f64;
}

/// Minimal printable-blueprint timing information.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Blueprint {
    /// Canonical device type.
    pub device_type: String,
    /// Duration of one print in seconds.
    pub print_time_seconds: f64,
    /// Open device feature flags supplied by the blueprint catalogue.
    #[serde(default)]
    pub features: Vec<String>,
    /// Printable device components consumed by one completed unit.
    #[serde(default)]
    pub components: QuantityMap,
}

impl PrintTime for Blueprint {
    fn print_time_seconds(&self) -> f64 {
        self.print_time_seconds
    }
}

impl Blueprint {
    /// Whether this device can be printed in a compacted flatpack state.
    #[must_use]
    pub fn is_modular(&self) -> bool {
        self.features.iter().any(|feature| feature == "modular")
            || matches!(
                self.device_type.as_str(),
                "autofactory" | "system_hub" | "exotic_matter_injector"
            )
    }
}

/// One requested device quantity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrintRequest {
    /// Canonical device type.
    pub device_type: String,
    /// Number of devices to queue.
    pub quantity: i64,
}

impl PrintRequest {
    /// Creates one print request.
    #[must_use]
    pub fn new(device_type: impl Into<String>, quantity: i64) -> Self {
        Self {
            device_type: device_type.into(),
            quantity,
        }
    }
}

/// Existing work assigned to one Autofactory.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct FactoryWorkload {
    /// Autofactory device code.
    pub code: String,
    /// Estimated seconds before newly appended work finishes waiting.
    pub remaining_seconds: f64,
}

/// A quantity-batched print assignment.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PrintBatch {
    /// Autofactory device code.
    pub factory_code: String,
    /// Canonical device type.
    pub device_type: String,
    /// Quantity assigned to this adjacent batch.
    pub quantity: i64,
    /// New-work sequence within this Autofactory.
    pub sequence: usize,
    /// Projected total Autofactory workload after this batch.
    pub projected_finish_seconds: f64,
}

/// Complete distributed manufacturing schedule.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct PrintSchedule {
    /// Quantity batches grouped by Autofactory and adjacent device type.
    pub batches: Vec<PrintBatch>,
    /// Maximum projected finish time across all Autofactories.
    pub projected_finish_seconds: f64,
}

/// Recursive manufacturing prerequisites for one user request.
///
/// Component waves are ordered from deepest leaf components to the devices
/// consumed directly by the requested outputs. Each wave must physically
/// finish before the next wave can be queued.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrintDependencyPlan {
    /// Canonical quantities explicitly requested by the caller.
    pub requested: QuantityMap,
    /// Missing component quantities grouped into completion-ordered waves.
    pub component_waves: Vec<QuantityMap>,
    /// Existing free component stock reserved instead of reprinting it.
    pub reused_components: QuantityMap,
}

/// Pure scheduling validation failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ScheduleError {
    /// A request used zero or a negative quantity.
    #[error("print quantity for `{device_type}` must be greater than zero, got {quantity}")]
    InvalidQuantity {
        /// Canonical requested device type.
        device_type: String,
        /// Invalid requested quantity.
        quantity: i64,
    },
    /// A required blueprint was not supplied.
    #[error("missing blueprint for requested device type `{0}`")]
    MissingBlueprint(String),
    /// A missing component cannot be printed because its blueprint is locked
    /// or otherwise absent from the account catalogue.
    #[error("component `{device_type}` is short by {missing}, but its blueprint is not unlocked")]
    MissingComponentBlueprint {
        /// Component device type.
        device_type: String,
        /// Quantity still missing after reserving local stock.
        missing: i64,
    },
    /// Recursive component declarations contain a dependency cycle.
    #[error("blueprint component cycle includes `{0}`")]
    ComponentCycle(String),
    /// Manufacturing was requested without an Autofactory.
    #[error("printing is required but no Autofactory is available")]
    NoAutofactory,
}

/// Combines repeated requests into canonical positive quantities.
pub fn normalize_requests(requests: &[PrintRequest]) -> Result<QuantityMap, ScheduleError> {
    let mut quantities = QuantityMap::new();
    for request in requests {
        if request.quantity <= 0 {
            return Err(ScheduleError::InvalidQuantity {
                device_type: request.device_type.clone(),
                quantity: request.quantity,
            });
        }
        *quantities.entry(request.device_type.clone()).or_default() += request.quantity;
    }
    Ok(quantities)
}

/// Expands printable component requirements into dependency-ordered waves.
///
/// Explicit requests are always printed in full. Existing free stock is only
/// applied to component requirements, because consuming a device the caller
/// explicitly requested would change the requested final quantity.
pub fn plan_print_dependencies(
    requests: &[PrintRequest],
    blueprints: &BTreeMap<String, Blueprint>,
    available_components: &QuantityMap,
) -> Result<PrintDependencyPlan, ScheduleError> {
    let requested = normalize_requests(requests)?;
    for device_type in requested.keys() {
        if !blueprints.contains_key(device_type) {
            return Err(ScheduleError::MissingBlueprint(device_type.clone()));
        }
    }

    let mut available = available_components.clone();
    let mut reused_components = QuantityMap::new();
    let mut waves = BTreeMap::<usize, QuantityMap>::new();

    for (device_type, quantity) in &requested {
        let blueprint = blueprints
            .get(device_type)
            .ok_or_else(|| ScheduleError::MissingBlueprint(device_type.clone()))?;
        let mut visiting = vec![device_type.clone()];
        for (component, component_quantity) in &blueprint.components {
            schedule_component_requirement(
                component,
                component_quantity.saturating_mul(*quantity),
                blueprints,
                &mut available,
                &mut reused_components,
                &mut waves,
                &mut visiting,
            )?;
        }
    }

    Ok(PrintDependencyPlan {
        requested,
        component_waves: waves.into_values().collect(),
        reused_components,
    })
}

fn schedule_component_requirement(
    device_type: &str,
    quantity: i64,
    blueprints: &BTreeMap<String, Blueprint>,
    available: &mut QuantityMap,
    reused: &mut QuantityMap,
    waves: &mut BTreeMap<usize, QuantityMap>,
    visiting: &mut Vec<String>,
) -> Result<usize, ScheduleError> {
    if quantity <= 0 {
        return Ok(0);
    }
    if visiting.iter().any(|item| item == device_type) {
        return Err(ScheduleError::ComponentCycle(device_type.to_owned()));
    }

    let available_quantity = available.get(device_type).copied().unwrap_or(0).max(0);
    let reused_quantity = quantity.min(available_quantity);
    if reused_quantity > 0 {
        available.insert(device_type.to_owned(), available_quantity - reused_quantity);
        *reused.entry(device_type.to_owned()).or_default() += reused_quantity;
    }
    let missing = quantity - reused_quantity;
    if missing == 0 {
        return Ok(0);
    }

    let blueprint =
        blueprints
            .get(device_type)
            .ok_or_else(|| ScheduleError::MissingComponentBlueprint {
                device_type: device_type.to_owned(),
                missing,
            })?;
    visiting.push(device_type.to_owned());
    let mut deepest_child = 0usize;
    for (component, component_quantity) in &blueprint.components {
        deepest_child = deepest_child.max(schedule_component_requirement(
            component,
            component_quantity.saturating_mul(missing),
            blueprints,
            available,
            reused,
            waves,
            visiting,
        )?);
    }
    visiting.pop();

    let depth = deepest_child.saturating_add(1);
    *waves
        .entry(depth)
        .or_default()
        .entry(device_type.to_owned())
        .or_default() += missing;
    Ok(depth)
}

/// Balances individual print units against existing Autofactory workloads.
///
/// Longer prints are assigned first. Each unit is placed on the factory with
/// the earliest projected finish time, then adjacent same-type assignments are
/// grouped into quantity batches for display or submission.
pub fn schedule_prints<B: PrintTime>(
    required: &QuantityMap,
    blueprints: &BTreeMap<String, B>,
    factories: &[FactoryWorkload],
) -> Result<PrintSchedule, ScheduleError> {
    if required.values().all(|quantity| *quantity <= 0) {
        return Ok(PrintSchedule::default());
    }
    if factories.is_empty() {
        return Err(ScheduleError::NoAutofactory);
    }

    let mut units = Vec::new();
    for (device_type, quantity) in required {
        if *quantity <= 0 {
            continue;
        }
        let blueprint = blueprints
            .get(device_type)
            .ok_or_else(|| ScheduleError::MissingBlueprint(device_type.clone()))?;
        for _ in 0..*quantity {
            units.push((device_type.clone(), blueprint.print_time_seconds().max(0.0)));
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
            .ok_or(ScheduleError::NoAutofactory)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_requests_are_combined() {
        let requests = vec![
            PrintRequest::new("cargo_freighter", 2),
            PrintRequest::new("autofactory", 6),
            PrintRequest::new("cargo_freighter", 4),
        ];
        let quantities = normalize_requests(&requests).unwrap();
        assert_eq!(quantities["autofactory"], 6);
        assert_eq!(quantities["cargo_freighter"], 6);
    }

    #[test]
    fn balances_against_existing_work() {
        let blueprints = [(
            "device".into(),
            Blueprint {
                device_type: "device".into(),
                print_time_seconds: 100.0,
                features: Vec::new(),
                components: QuantityMap::new(),
            },
        )]
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
        assert_eq!(quantities["A"], 2);
        assert_eq!(quantities["B"], 3);
        assert_eq!(schedule.projected_finish_seconds, 400.0);
    }

    #[test]
    fn rejects_non_positive_quantities() {
        let error = normalize_requests(&[PrintRequest::new("autofactory", 0)]).unwrap_err();
        assert!(matches!(error, ScheduleError::InvalidQuantity { .. }));
    }

    #[test]
    fn modular_feature_enables_flatpack_output() {
        let modular = Blueprint {
            device_type: "autofactory".into(),
            print_time_seconds: 1.0,
            features: vec!["modular".into()],
            components: QuantityMap::new(),
        };
        let ordinary = Blueprint {
            device_type: "mining_drone".into(),
            print_time_seconds: 1.0,
            features: Vec::new(),
            components: QuantityMap::new(),
        };
        assert!(modular.is_modular());
        assert!(!ordinary.is_modular());
    }

    #[test]
    fn documented_modular_infrastructure_survives_missing_feature_flags() {
        let hub = Blueprint {
            device_type: "system_hub".into(),
            ..Blueprint::default()
        };
        let autofactory = Blueprint {
            device_type: "autofactory".into(),
            ..Blueprint::default()
        };
        assert!(hub.is_modular());
        assert!(autofactory.is_modular());
    }

    fn dependency_blueprint(device_type: &str, components: &[(&str, i64)]) -> Blueprint {
        Blueprint {
            device_type: device_type.into(),
            print_time_seconds: 1.0,
            features: Vec::new(),
            components: components
                .iter()
                .map(|(name, quantity)| ((*name).to_owned(), *quantity))
                .collect(),
        }
    }

    #[test]
    fn plans_exotic_injector_components_before_parent() {
        let blueprints = [
            (
                "exotic_matter_injector".into(),
                dependency_blueprint(
                    "exotic_matter_injector",
                    &[
                        ("casimir_array", 1),
                        ("exotic_particle_trap", 2),
                        ("negative_energy_conduit", 1),
                    ],
                ),
            ),
            (
                "exotic_particle_trap".into(),
                dependency_blueprint("exotic_particle_trap", &[]),
            ),
            (
                "negative_energy_conduit".into(),
                dependency_blueprint("negative_energy_conduit", &[]),
            ),
        ]
        .into_iter()
        .collect();
        let stock = [("casimir_array".into(), 1)].into_iter().collect();
        let plan = plan_print_dependencies(
            &[PrintRequest::new("exotic_matter_injector", 1)],
            &blueprints,
            &stock,
        )
        .unwrap();
        assert_eq!(plan.component_waves.len(), 1);
        assert_eq!(plan.component_waves[0]["exotic_particle_trap"], 2);
        assert_eq!(plan.component_waves[0]["negative_energy_conduit"], 1);
        assert_eq!(plan.reused_components["casimir_array"], 1);
        assert_eq!(plan.requested["exotic_matter_injector"], 1);
    }

    #[test]
    fn nested_components_are_ordered_leaf_first() {
        let blueprints = [
            (
                "parent".into(),
                dependency_blueprint("parent", &[("middle", 1)]),
            ),
            (
                "middle".into(),
                dependency_blueprint("middle", &[("leaf", 2)]),
            ),
            ("leaf".into(), dependency_blueprint("leaf", &[])),
        ]
        .into_iter()
        .collect();
        let plan = plan_print_dependencies(
            &[PrintRequest::new("parent", 1)],
            &blueprints,
            &QuantityMap::new(),
        )
        .unwrap();
        assert_eq!(plan.component_waves.len(), 2);
        assert_eq!(plan.component_waves[0]["leaf"], 2);
        assert_eq!(plan.component_waves[1]["middle"], 1);
    }

    #[test]
    fn locked_component_is_allowed_when_local_stock_covers_it() {
        let blueprints = [(
            "parent".into(),
            dependency_blueprint("parent", &[("event_component", 1)]),
        )]
        .into_iter()
        .collect();
        let stock = [("event_component".into(), 1)].into_iter().collect();
        let plan = plan_print_dependencies(&[PrintRequest::new("parent", 1)], &blueprints, &stock)
            .unwrap();
        assert!(plan.component_waves.is_empty());
        assert_eq!(plan.reused_components["event_component"], 1);
    }

    #[test]
    fn rejects_component_cycles() {
        let blueprints = [
            (
                "parent".into(),
                dependency_blueprint("parent", &[("part", 1)]),
            ),
            (
                "part".into(),
                dependency_blueprint("part", &[("parent", 1)]),
            ),
        ]
        .into_iter()
        .collect();
        let error = plan_print_dependencies(
            &[PrintRequest::new("parent", 1)],
            &blueprints,
            &QuantityMap::new(),
        )
        .unwrap_err();
        assert!(matches!(error, ScheduleError::ComponentCycle(_)));
    }
}
