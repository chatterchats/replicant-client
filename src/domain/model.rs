use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::*;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Account {
    pub id: AccountId,
    pub name: Option<String>,
    pub email: Option<String>,
    pub timezone: Option<String>,
    pub status: Option<String>,
    pub experience_points_total: Option<i64>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct DeviceRelationships {
    pub attached_to: Option<DeviceKey>,
    pub controller: Option<DeviceKey>,
    /// Replicant currently assigned as this device's owner or operator.
    pub assigned_replicant: Option<ReplicantKey>,
    /// Replicant matrix physically hosted by this device.
    pub hosting_replicant: Option<ReplicantKey>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Device {
    pub key: DeviceKey,
    pub device_type: Option<DeviceType>,
    pub status: Option<DeviceStatus>,
    pub location: Option<LocationKey>,
    pub features: Vec<DeviceFeature>,
    pub available_commands: Vec<DeviceCommand>,
    pub available_directives: Vec<DeviceDirective>,
    pub tags: Vec<String>,
    pub relationships: DeviceRelationships,
    pub access: AccessScope,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct OwnedReplicantData {
    pub description: Option<String>,
    pub pronouns: Option<String>,
    pub experience_points: Option<i64>,
    pub plan: Option<String>,
    pub cohort_permission: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Replicant {
    pub key: ReplicantKey,
    pub name: Option<String>,
    pub is_npc: Option<bool>,
    pub status: Option<ReplicantStatus>,
    pub location: Option<LocationKey>,
    pub hosted_device: Option<DeviceKey>,
    pub private: Option<OwnedReplicantData>,
    pub access: AccessScope,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DirectoryProfile {
    pub id: ReplicantId,
    pub name: Option<String>,
    pub last_location: Option<LocationId>,
    pub is_npc: Option<bool>,
}

/// A field's knowledge state. `Absent` is materially different from an
/// unobserved field and survives persistence.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum Knowledge<T> {
    #[default]
    Unknown,
    Absent,
    Present(T),
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct LocationEnvironment {
    pub atmosphere: Knowledge<Atmosphere>,
    pub magnetic_field: Knowledge<bool>,
    /// Earth gravities (`g`).
    pub gravity_g: Knowledge<f64>,
    /// Degrees Celsius.
    pub surface_temp_c: Knowledge<f64>,
    pub in_habitable_zone: Knowledge<bool>,
    pub life_stage: Knowledge<LifeStage>,
    /// Axial tilt in degrees.
    pub axial_tilt_deg: Knowledge<f64>,
    /// Forward-compatible observed rotation classification, when supplied.
    pub rotation_state: Knowledge<String>,
    /// Forward-compatible host-star spectral classification, when supplied.
    pub star_spectral_type: Knowledge<String>,
    /// Forward-compatible nearby-belt richness, when supplied.
    pub nearby_belt_richness: Knowledge<String>,
    /// Light years from SOL, when the durable star catalogue can supply it.
    pub distance_from_sol_ly: Knowledge<f64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Location {
    pub key: LocationKey,
    pub location_type: Option<LocationType>,
    pub scanned: Option<bool>,
    pub system_scanned: Option<bool>,
    pub system_tags: Vec<String>,
    pub system: Option<String>,
    pub parent: Option<LocationKey>,
    pub environment: LocationEnvironment,
    /// Sanitized, untyped response fields retained for a later contract update.
    #[serde(default)]
    pub unknown: BTreeMap<String, Value>,
}

impl Location {
    #[must_use]
    pub fn id(&self) -> &LocationId {
        &self.key.id
    }
    #[must_use]
    pub fn atmosphere(&self) -> &Knowledge<Atmosphere> {
        &self.environment.atmosphere
    }
    #[must_use]
    pub fn gravity_g(&self) -> &Knowledge<f64> {
        &self.environment.gravity_g
    }
    #[must_use]
    pub fn surface_temp_c(&self) -> &Knowledge<f64> {
        &self.environment.surface_temp_c
    }
    #[must_use]
    pub fn magnetic_field_present(&self) -> &Knowledge<bool> {
        &self.environment.magnetic_field
    }
    #[must_use]
    pub fn in_habitable_zone(&self) -> &Knowledge<bool> {
        &self.environment.in_habitable_zone
    }
    #[must_use]
    pub fn life_stage(&self) -> &Knowledge<LifeStage> {
        &self.environment.life_stage
    }
    #[must_use]
    pub fn axial_tilt_deg(&self) -> &Knowledge<f64> {
        &self.environment.axial_tilt_deg
    }
    #[must_use]
    pub fn rotation_state(&self) -> &Knowledge<String> {
        &self.environment.rotation_state
    }
    #[must_use]
    pub fn star_spectral_type(&self) -> &Knowledge<String> {
        &self.environment.star_spectral_type
    }
    #[must_use]
    pub fn nearby_belt_richness(&self) -> &Knowledge<String> {
        &self.environment.nearby_belt_richness
    }
    #[must_use]
    pub fn distance_from_sol_ly(&self) -> &Knowledge<f64> {
        &self.environment.distance_from_sol_ly
    }

    /// Returns whether survey-only environmental evidence has been observed.
    ///
    /// Some location-detail responses omit the top-level `scanned` flag even
    /// though they contain atmosphere, magnetic-field, or axial-tilt results.
    /// Those fields are emitted by survey-drone detail and provide conservative
    /// evidence that the body has been surveyed. Habitable-zone, gravity,
    /// temperature, and life fields are intentionally excluded because less
    /// detailed system observations may provide them before a survey.
    #[must_use]
    pub fn has_survey_environment_evidence(&self) -> bool {
        !matches!(&self.environment.atmosphere, Knowledge::Unknown)
            || !matches!(&self.environment.magnetic_field, Knowledge::Unknown)
            || !matches!(&self.environment.axial_tilt_deg, Knowledge::Unknown)
    }

    pub(crate) fn merge_from(&mut self, newer: &Self) {
        self.location_type = newer
            .location_type
            .clone()
            .or_else(|| self.location_type.clone());
        self.scanned = newer.scanned.or(self.scanned);
        self.system_scanned = newer.system_scanned.or(self.system_scanned);
        if !newer.system_tags.is_empty() {
            self.system_tags = newer.system_tags.clone();
        }
        self.system = newer.system.clone().or_else(|| self.system.clone());
        self.parent = newer.parent.clone().or_else(|| self.parent.clone());
        merge_knowledge(
            &mut self.environment.atmosphere,
            &newer.environment.atmosphere,
        );
        merge_knowledge(
            &mut self.environment.magnetic_field,
            &newer.environment.magnetic_field,
        );
        merge_knowledge(
            &mut self.environment.gravity_g,
            &newer.environment.gravity_g,
        );
        merge_knowledge(
            &mut self.environment.surface_temp_c,
            &newer.environment.surface_temp_c,
        );
        merge_knowledge(
            &mut self.environment.in_habitable_zone,
            &newer.environment.in_habitable_zone,
        );
        merge_knowledge(
            &mut self.environment.life_stage,
            &newer.environment.life_stage,
        );
        merge_knowledge(
            &mut self.environment.axial_tilt_deg,
            &newer.environment.axial_tilt_deg,
        );
        merge_knowledge(
            &mut self.environment.rotation_state,
            &newer.environment.rotation_state,
        );
        merge_knowledge(
            &mut self.environment.star_spectral_type,
            &newer.environment.star_spectral_type,
        );
        merge_knowledge(
            &mut self.environment.nearby_belt_richness,
            &newer.environment.nearby_belt_richness,
        );
        merge_knowledge(
            &mut self.environment.distance_from_sol_ly,
            &newer.environment.distance_from_sol_ly,
        );
        self.unknown.extend(newer.unknown.clone());
    }
}

fn merge_knowledge<T: Clone>(current: &mut Knowledge<T>, newer: &Knowledge<T>) {
    if !matches!(newer, Knowledge::Unknown) {
        *current = newer.clone();
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocationOverview {
    pub key: LocationKey,
    pub device_count: i64,
    pub replicant_count: i64,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[non_exhaustive]
pub enum InventoryOwner {
    Account(AccountId),
    Replicant(ReplicantKey),
    Location(LocationKey),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InventoryItem {
    pub resource: String,
    pub quantity: i64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Inventory {
    pub owner: InventoryOwner,
    pub location: Option<LocationKey>,
    pub items: Vec<InventoryItem>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub id: EventId,
    pub realm: Option<Realm>,
    pub name: EventName,
    pub category: EventCategory,
    pub device: Option<DeviceKey>,
    pub replicant: Option<ReplicantKey>,
    pub location: Option<LocationKey>,
    pub star: Option<StarKey>,
    pub occurred_at: String,
    pub payload: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    pub key: TradeKey,
    pub controller: DeviceKey,
    pub status: Option<TradeStatus>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Simulation {
    pub id: SimulationId,
    pub scenario_code: Option<String>,
    pub scenario_name: Option<String>,
    pub starting_location: Option<LocationKey>,
    pub starting_star: Option<StarKey>,
    pub is_mine: bool,
    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    #[serde(default)]
    pub lifecycle: SimulationLifecycle,
    #[serde(default)]
    pub seed_failures: Vec<String>,
    #[serde(default)]
    pub replicant_code: Option<String>,
}

/// Durable local lifecycle for an owned simulation realm.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimulationLifecycle {
    #[default]
    Synchronizing,
    Active,
    AbandonPending,
    AbandonAmbiguous,
    Ended,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Blueprint {
    pub id: BlueprintId,
    pub device_type: Option<DeviceType>,
    pub description: Option<String>,
    pub features: Vec<DeviceFeature>,
    pub directives: Vec<DeviceDirective>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Achievement {
    pub id: AchievementId,
    pub title: Option<String>,
    pub category: Option<String>,
    pub description: Option<String>,
    pub xp_reward: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Reputation {
    pub species: SpeciesId,
    pub value: f64,
    pub description: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Species {
    pub id: SpeciesId,
    pub name: Option<String>,
    pub kind: Option<SpeciesKind>,
    pub description: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct GalacticPosition {
    pub x: f64,
    pub y: f64,
    pub z: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Star {
    pub key: StarKey,
    pub name: Option<String>,
    pub spectral_type: Option<String>,
    pub entry_point: Option<LocationKey>,
    pub position: Option<GalacticPosition>,
    pub has_hub: Option<bool>,
    pub region: Option<String>,
}

/// A star observation from one owned replicant's perspective.  It is not a
/// catalogue replacement: different replicants can know different facts.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StarKnowledge {
    pub replicant: ReplicantKey,
    pub star: StarKey,
    pub position: Option<GalacticPosition>,
    pub spectral_type: Option<String>,
    pub entry_point: Option<LocationKey>,
    pub explored: Option<bool>,
    pub has_hub: Option<bool>,
    pub has_life: Option<bool>,
    pub region: Option<String>,
    pub distance_from_replicant: Option<f64>,
    pub estimated_travel_time: Option<i64>,
}
