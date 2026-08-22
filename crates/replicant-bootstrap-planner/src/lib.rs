//! Pure planning primitives for an autonomous regional bootstrap mission.

use std::{cmp::Ordering, collections::BTreeMap};

use replicant_mining_planner::{
    CARGO_FREIGHTER, MAINTENANCE_DRONE, QuantityMap, SURVEY_CONTROLLER, SURVEY_DRONE,
    TRANSPORT_CONTROLLER, add_quantities, mining_site_requirements, multiply,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Autofactory blueprint identifier.
pub const AUTOFACTORY: &str = "autofactory";
/// Conventional FTL relay blueprint identifier.
pub const FTL_RELAY: &str = "ftl_relay";
/// Cheap monitoring beacon blueprint identifier.
pub const FTL_BEACON: &str = "ftl_beacon";
/// Surge Carrier blueprint identifier.
pub const SURGE_CARRIER: &str = "surge_carrier";
/// Canonical resource keys loaded into the six seed freighters.
pub const SEED_RESOURCES: [&str; 6] = [
    "carbon",
    "conductive",
    "rares",
    "silicates",
    "structural",
    "volatiles",
];

/// Tunable composition of one regional ark.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BootstrapProfile {
    /// Complete mining setups staged for the regional expansion.
    pub mining_setups: i64,
    /// Autofactories delivered to the first dense belt.
    pub autofactories: i64,
    /// Cargo Freighters initially loaded with seed resources.
    pub cargo_freighters: i64,
    /// AMI transport controllers staged for belt routes.
    pub transport_controllers: i64,
    /// Dedicated maintenance drones for the regional capital.
    pub hub_maintenance_drones: i64,
    /// Extra survey drones accompanying the exploration controller.
    pub exploration_survey_drones: i64,
    /// Conventional relays carried to establish the island root.
    pub root_relays: i64,
    /// Additional conventional relays carried for the first expansion wave.
    #[serde(default = "default_expansion_relays")]
    pub expansion_relays: i64,
    /// Monitoring beacons carried for intelligent-life worlds.
    #[serde(default = "default_ftl_beacons")]
    pub ftl_beacons: i64,
    /// Legacy fixed carrier reserve. Retained for mission/profile compatibility;
    /// current planning derives the required Surge Carrier count from payload roles.
    #[serde(default)]
    pub dedicated_surge_carriers: i64,
}

const fn default_ftl_beacons() -> i64 {
    9
}

const fn default_expansion_relays() -> i64 {
    18
}

impl Default for BootstrapProfile {
    fn default() -> Self {
        Self {
            mining_setups: 8,
            autofactories: 6,
            cargo_freighters: 6,
            transport_controllers: 6,
            hub_maintenance_drones: 2,
            exploration_survey_drones: 3,
            root_relays: 1,
            expansion_relays: default_expansion_relays(),
            ftl_beacons: default_ftl_beacons(),
            dedicated_surge_carriers: 0,
        }
    }
}

/// Pure bootstrap-planning failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PlannerError {
    /// A profile quantity falls outside its supported range.
    #[error("{field} must be between {minimum} and {maximum}; got {actual}")]
    InvalidCount {
        /// Field being validated.
        field: &'static str,
        /// Inclusive minimum.
        minimum: i64,
        /// Inclusive maximum.
        maximum: i64,
        /// Supplied value.
        actual: i64,
    },
    /// Carrier capacity cannot be derived from the unlocked blueprint.
    #[error("Surge Carrier attach capacity must be positive")]
    MissingCarrierCapacity,
    /// A Surge Carrier is too small to keep one complete mining setup together.
    #[error(
        "Surge Carrier attach capacity {actual} cannot hold a complete {required}-device mining setup"
    )]
    CarrierCapacityTooSmall {
        /// Required capacity for one mining setup.
        required: i64,
        /// Available carrier capacity.
        actual: i64,
    },
    /// The survey did not find enough dense belts.
    #[error("survey found {found} dense belts, but at least {required} are required")]
    InsufficientDenseBelts {
        /// Required site count.
        required: usize,
        /// Discovered dense-belt count.
        found: usize,
    },
}

/// Validates the supported regional-ark profile bounds.
pub fn validate_profile(profile: &BootstrapProfile) -> Result<(), PlannerError> {
    validate_count("mining_setups", profile.mining_setups, 5, 10)?;
    validate_count("autofactories", profile.autofactories, 3, 6)?;
    validate_count("cargo_freighters", profile.cargo_freighters, 6, 12)?;
    validate_count(
        "transport_controllers",
        profile.transport_controllers,
        1,
        12,
    )?;
    validate_count(
        "hub_maintenance_drones",
        profile.hub_maintenance_drones,
        1,
        4,
    )?;
    validate_count(
        "exploration_survey_drones",
        profile.exploration_survey_drones,
        3,
        3,
    )?;
    validate_count("root_relays", profile.root_relays, 1, 4)?;
    validate_count("expansion_relays", profile.expansion_relays, 0, 36)?;
    validate_count("ftl_beacons", profile.ftl_beacons, 0, 18)
}

fn validate_count(
    field: &'static str,
    actual: i64,
    minimum: i64,
    maximum: i64,
) -> Result<(), PlannerError> {
    if (minimum..=maximum).contains(&actual) {
        Ok(())
    } else {
        Err(PlannerError::InvalidCount {
            field,
            minimum,
            maximum,
            actual,
        })
    }
}

/// Returns all devices that should be available at departure, excluding carriers.
#[must_use]
pub fn ark_device_requirements(profile: &BootstrapProfile) -> QuantityMap {
    let mut requirements = multiply(&mining_site_requirements(), profile.mining_setups);
    add_quantities(
        &mut requirements,
        &[
            (AUTOFACTORY.to_owned(), profile.autofactories),
            (CARGO_FREIGHTER.to_owned(), profile.cargo_freighters),
            (
                TRANSPORT_CONTROLLER.to_owned(),
                profile.transport_controllers,
            ),
            (
                FTL_RELAY.to_owned(),
                profile.root_relays.saturating_add(profile.expansion_relays),
            ),
            (FTL_BEACON.to_owned(), profile.ftl_beacons),
            (MAINTENANCE_DRONE.to_owned(), profile.hub_maintenance_drones),
            (SURVEY_CONTROLLER.to_owned(), 1),
            (SURVEY_DRONE.to_owned(), profile.exploration_survey_drones),
        ]
        .into_iter()
        .collect(),
    );
    requirements
}

/// Number of ark devices that need attachment transport.
#[must_use]
pub fn attachment_slots(requirements: &QuantityMap) -> i64 {
    requirements
        .iter()
        .filter(|(device_type, _)| device_type.as_str() != CARGO_FREIGHTER)
        .map(|(_, quantity)| (*quantity).max(0))
        .sum()
}

/// Number of Surge Carriers required when payload roles are kept together.
///
/// Every mining setup receives its own carrier, expansion relays and beacons
/// are packed into dedicated carrier-sized groups, and the remaining payload
/// is packed normally. This intentionally derives the convoy size from the
/// profile instead of requiring callers to maintain a separate carrier count.
pub fn required_role_carriers(
    profile: &BootstrapProfile,
    requirements: &QuantityMap,
    carrier_capacity: i64,
) -> Result<i64, PlannerError> {
    if carrier_capacity <= 0 {
        return Err(PlannerError::MissingCarrierCapacity);
    }
    let mining_slots_per_setup = mining_site_requirements().values().copied().sum::<i64>();
    if carrier_capacity < mining_slots_per_setup {
        return Err(PlannerError::CarrierCapacityTooSmall {
            required: mining_slots_per_setup,
            actual: carrier_capacity,
        });
    }
    let mining_slots = mining_slots_per_setup.saturating_mul(profile.mining_setups.max(0));
    let expansion_relays = profile.expansion_relays.max(0);
    let beacons = profile.ftl_beacons.max(0);
    let general_slots = attachment_slots(requirements)
        .saturating_sub(mining_slots)
        .saturating_sub(expansion_relays)
        .saturating_sub(beacons);

    Ok(profile
        .mining_setups
        .max(0)
        .saturating_add(div_ceil(expansion_relays, carrier_capacity))
        .saturating_add(div_ceil(beacons, carrier_capacity))
        .saturating_add(div_ceil(general_slots, carrier_capacity)))
}

const fn div_ceil(value: i64, divisor: i64) -> i64 {
    if value <= 0 {
        0
    } else {
        (value + divisor - 1) / divisor
    }
}

/// Number of additional Surge Carriers required for the attachment payload.
pub fn missing_carriers(
    payload_slots: i64,
    existing_capacity: i64,
    carrier_capacity: i64,
) -> Result<i64, PlannerError> {
    if carrier_capacity <= 0 {
        return Err(PlannerError::MissingCarrierCapacity);
    }
    Ok(div_ceil(
        payload_slots.saturating_sub(existing_capacity),
        carrier_capacity,
    ))
}

/// Split a payload between the minimum newly printed carrier reserve and only
/// as many existing carriers as are still needed.
pub fn carrier_provisioning(
    payload_slots: i64,
    existing_capacities: &[i64],
    carrier_capacity: i64,
    minimum_printed: i64,
) -> Result<(usize, i64), PlannerError> {
    if carrier_capacity <= 0 {
        return Err(PlannerError::MissingCarrierCapacity);
    }
    let minimum_printed = minimum_printed.max(0);
    let capacity_after_prints = minimum_printed.saturating_mul(carrier_capacity);
    let required_existing_capacity = payload_slots.saturating_sub(capacity_after_prints);
    let mut selected_capacity = 0_i64;
    let mut selected_count = 0_usize;
    for capacity in existing_capacities {
        if selected_capacity >= required_existing_capacity {
            break;
        }
        selected_capacity = selected_capacity.saturating_add((*capacity).max(0));
        selected_count += 1;
    }
    let printed =
        missing_carriers(payload_slots, selected_capacity, carrier_capacity)?.max(minimum_printed);
    Ok((selected_count, printed))
}

/// One dense belt learned during the regional survey.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BeltCandidate {
    /// Parent system.
    pub system: String,
    /// Belt designation.
    pub designation: String,
    /// Open density value from the location response.
    pub density: String,
    /// Straight-line distance from the regional capital star.
    pub distance_from_capital_ly: f64,
}

/// Selects the closest distinct dense-belt systems, always retaining the capital belt.
pub fn select_dense_belts(
    candidates: &[BeltCandidate],
    capital_belt: &str,
    minimum: usize,
    maximum: usize,
) -> Result<Vec<BeltCandidate>, PlannerError> {
    let maximum = maximum.max(minimum);
    let mut by_system = BTreeMap::<String, BeltCandidate>::new();
    for candidate in candidates.iter().filter(|candidate| {
        candidate.density.eq_ignore_ascii_case("dense") || candidate.designation == capital_belt
    }) {
        // Per system, the capital belt always wins; otherwise the closer belt
        // wins, with the designation as a deterministic tie-break.
        let replace = match by_system.get(&candidate.system) {
            None => true,
            Some(_) if candidate.designation == capital_belt => true,
            Some(current) if current.designation == capital_belt => false,
            Some(current) => {
                candidate
                    .distance_from_capital_ly
                    .partial_cmp(&current.distance_from_capital_ly)
                    .unwrap_or(Ordering::Equal)
                    .then_with(|| candidate.designation.cmp(&current.designation))
                    == Ordering::Less
            }
        };
        if replace {
            by_system.insert(candidate.system.clone(), candidate.clone());
        }
    }
    let mut selected = by_system.into_values().collect::<Vec<_>>();
    selected.sort_by(|left, right| {
        (left.designation != capital_belt)
            .cmp(&(right.designation != capital_belt))
            .then_with(|| {
                left.distance_from_capital_ly
                    .partial_cmp(&right.distance_from_capital_ly)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.system.cmp(&right.system))
    });
    if selected.len() < minimum {
        return Err(PlannerError::InsufficientDenseBelts {
            required: minimum,
            found: selected.len(),
        });
    }
    selected.truncate(maximum);
    Ok(selected)
}

/// Stable, readable mission reservation tag scoped to a landing system.
///
/// Normal system names remain readable. Overlong names retain a readable prefix
/// plus a deterministic hash so the result always fits the server's
/// 32-character device-tag limit.
#[must_use]
pub fn mission_tag(system: &str) -> String {
    const PREFIX: &str = "boot-m:";
    const HASH_CHARS: usize = 12;

    let normalized = system
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let normalized = normalized.trim_matches('-');
    let direct = format!("{PREFIX}{normalized}");
    if direct.chars().count() <= 32 {
        return direct;
    }

    let fixed = PREFIX.chars().count() + 1 + HASH_CHARS;
    let head_budget = 32usize.saturating_sub(fixed).max(1);
    let mut head = normalized.chars().take(head_budget).collect::<String>();
    head = head.trim_end_matches('-').to_owned();
    if head.is_empty() {
        head.push('s');
    }
    let hash = stable_hash(normalized) & 0x0000_ffff_ffff_ffff;
    format!("{PREFIX}{head}-{hash:012x}")
}

/// Stable role tag that fits the server's 32-character limit.
#[must_use]
pub fn role_tag(role: &str) -> String {
    let normalized = role
        .chars()
        .filter(|character| character.is_ascii_alphanumeric() || *character == '-')
        .take(22)
        .collect::<String>()
        .to_ascii_lowercase();
    format!("boot-r:{normalized}")
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_mining_planner::{MINING_CONTROLLER, MINING_DRONE};

    #[test]
    fn default_ark_contains_eight_complete_sites() {
        let requirements = ark_device_requirements(&BootstrapProfile::default());
        assert_eq!(requirements[MINING_CONTROLLER], 8);
        assert_eq!(requirements[MINING_DRONE], 32);
        assert_eq!(requirements[AUTOFACTORY], 6);
        assert_eq!(requirements[CARGO_FREIGHTER], 6);
        assert_eq!(requirements[FTL_RELAY], 19);
        assert_eq!(requirements[FTL_BEACON], 9);
        assert!(!requirements.contains_key("system_hub"));
    }

    #[test]
    fn carrier_count_uses_existing_capacity_first() {
        assert_eq!(missing_carriers(91, 50, 10), Ok(5));
        assert_eq!(missing_carriers(50, 50, 10), Ok(0));
    }

    #[test]
    fn legacy_carrier_provisioning_helper_remains_stable() {
        assert_eq!(carrier_provisioning(45, &[9, 9, 9, 9], 9, 3), Ok((2, 3)));
        assert_eq!(carrier_provisioning(27, &[9, 9], 9, 3), Ok((0, 3)));
    }

    #[test]
    fn default_role_packing_derives_fourteen_carriers() {
        let profile = BootstrapProfile::default();
        let requirements = ark_device_requirements(&profile);
        assert_eq!(required_role_carriers(&profile, &requirements, 9), Ok(14));
    }

    #[test]
    fn role_packing_rejects_a_carrier_that_cannot_hold_one_mining_setup() {
        let profile = BootstrapProfile::default();
        let requirements = ark_device_requirements(&profile);
        assert_eq!(
            required_role_carriers(&profile, &requirements, 8),
            Err(PlannerError::CarrierCapacityTooSmall {
                required: 9,
                actual: 8,
            })
        );
    }

    #[test]
    fn carrier_count_tracks_profile_changes_without_a_manual_reserve() {
        let mut profile = BootstrapProfile::default();
        profile.mining_setups = 10;
        profile.expansion_relays = 9;
        profile.ftl_beacons = 0;
        let requirements = ark_device_requirements(&profile);
        assert_eq!(required_role_carriers(&profile, &requirements, 9), Ok(14));
    }

    #[test]
    fn legacy_profiles_gain_the_regional_network_reserve() {
        let profile: BootstrapProfile = serde_json::from_str(
            r#"{
                "mining_setups": 8,
                "autofactories": 6,
                "cargo_freighters": 6,
                "transport_controllers": 6,
                "hub_maintenance_drones": 2,
                "exploration_survey_drones": 3,
                "root_relays": 1
            }"#,
        )
        .expect("legacy profile");

        assert_eq!(profile.expansion_relays, 18);
        assert_eq!(profile.ftl_beacons, 9);
        assert_eq!(profile.dedicated_surge_carriers, 0);
    }

    #[test]
    fn dense_selection_keeps_capital_and_orders_by_distance() {
        let candidates = [
            BeltCandidate {
                system: "CAP".into(),
                designation: "CAP-BELT-1".into(),
                density: "dense".into(),
                distance_from_capital_ly: 0.0,
            },
            BeltCandidate {
                system: "NEAR".into(),
                designation: "NEAR-BELT-1".into(),
                density: "dense".into(),
                distance_from_capital_ly: 4.0,
            },
            BeltCandidate {
                system: "FAR".into(),
                designation: "FAR-BELT-1".into(),
                density: "dense".into(),
                distance_from_capital_ly: 12.0,
            },
        ];
        let selected =
            select_dense_belts(&candidates, "CAP-BELT-1", 2, 2).expect("two dense belts");
        assert_eq!(selected[0].system, "CAP");
        assert_eq!(selected[1].system, "NEAR");
    }

    #[test]
    fn generated_tags_fit_the_api_limit() {
        assert_eq!(mission_tag("SCEPTURUM"), "boot-m:scepturum");
        assert!(mission_tag(&"x".repeat(200)).len() <= 32);
        assert_eq!(mission_tag(&"x".repeat(200)), mission_tag(&"X".repeat(200)));
        assert!(role_tag(&"role".repeat(20)).len() <= 32);
    }
}
