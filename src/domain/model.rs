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
    #[serde(default)]
    pub stowed_in: Option<DeviceKey>,
    pub controller: Option<DeviceKey>,
    /// Device linked through configuration, such as an FTL slingshot's
    /// destination empty replicant matrix.
    #[serde(default)]
    pub linked_device: Option<DeviceKey>,
    #[serde(default)]
    pub attached_devices: Vec<DeviceKey>,
    #[serde(default)]
    pub controlled_devices: Vec<DeviceKey>,
    #[serde(default)]
    pub stowed_devices: Vec<DeviceKey>,
    /// Replicant currently assigned as this device's owner or operator.
    pub assigned_replicant: Option<ReplicantKey>,
    /// Replicant matrix physically hosted by this device.
    pub hosting_replicant: Option<ReplicantKey>,
}

/// Normalized in-progress travel shared by devices and replicants.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct TravelState {
    pub arrives_at: Option<String>,
    pub departed_at: Option<String>,
    pub destination: Option<LocationKey>,
    pub eta_seconds: Option<i64>,
    pub final_arrives_at: Option<String>,
    pub final_destination: Option<LocationKey>,
    pub origin: Option<LocationKey>,
    pub route_eta_seconds: Option<i64>,
    pub stage: Option<String>,
    pub travel_type: Option<String>,
}

/// Operational-capacity value reported by the game API.
///
/// Current responses normally use percentage points (`0.0..=100.0`), while
/// some historical fixtures used a `0.0..=1.0` fraction. The wrapper preserves
/// the original wire value and provides [`Self::percent`] for safe comparisons.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperationalCapacity(f64);

impl OperationalCapacity {
    /// Creates a finite, non-negative operational-capacity value.
    #[must_use]
    pub fn new(value: f64) -> Option<Self> {
        (value.is_finite() && value >= 0.0).then_some(Self(value))
    }

    /// Returns the unmodified value received from the API.
    #[must_use]
    pub const fn raw(self) -> f64 {
        self.0
    }

    /// Returns a percentage-point interpretation suitable for thresholds.
    #[must_use]
    pub fn percent(self) -> f64 {
        if self.0 <= 1.0 {
            self.0 * 100.0
        } else {
            self.0
        }
    }
}

impl PartialEq for OperationalCapacity {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OperationalCapacity {}

/// Current AMI directive and its forward-compatible detail fields.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct ActiveDeviceDirective {
    pub directive: Option<DeviceDirective>,
    pub status: Option<String>,
    #[serde(default)]
    pub details: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Device {
    pub key: DeviceKey,
    pub device_type: Option<DeviceType>,
    pub status: Option<DeviceStatus>,
    pub location: Option<LocationKey>,
    /// Timestamp at which the device was deployed, when the authoritative
    /// device status includes one.
    ///
    /// `None` means the deployment timestamp was not reported; it must not be
    /// interpreted as evidence that the device was never deployed.
    #[serde(default)]
    pub deployed_at: Option<String>,
    /// Whether the device is currently within range of its controller.
    ///
    /// `None` means the control-range fact was not reported. This is retained
    /// as unknown rather than inferred from the device's relationships.
    #[serde(default)]
    pub in_control_range: Option<bool>,
    pub features: Vec<DeviceFeature>,
    pub available_commands: Vec<DeviceCommand>,
    pub available_directives: Vec<DeviceDirective>,
    pub tags: Vec<String>,
    pub relationships: DeviceRelationships,
    /// Resource quantities currently carried by this device.
    #[serde(default)]
    pub cargo: BTreeMap<String, i64>,
    /// Maximum resource quantity this device can carry.
    #[serde(default)]
    pub cargo_capacity: Option<i64>,
    #[serde(default)]
    pub attach_capacity: Option<i64>,
    #[serde(default)]
    pub stow_capacity: Option<i64>,
    #[serde(default)]
    pub stow_used: Option<i64>,
    /// Current operational capacity reported by the server. Current game
    /// responses use percentage points (0-100), while older fixtures may use
    /// a 0-1 fraction; callers should normalize before comparing thresholds.
    #[serde(default)]
    pub operational_capacity: Option<OperationalCapacity>,
    /// Remaining grace period, in seconds, before the device's upkeep model
    /// is enforced. This is retained without interpreting the associated
    /// upkeep payload until the live System Hub shape is captured.
    #[serde(default)]
    pub grace_period_remaining: Option<i64>,
    /// Forward-compatible System Hub/device upkeep requirements. The upstream
    /// API intentionally leaves these objects open-shaped, so the managed
    /// domain preserves them verbatim instead of guessing a schema.
    #[serde(default)]
    pub upkeep_requirements: Vec<BTreeMap<String, Value>>,
    /// Forward-compatible system-specific status detail. Kept open-shaped for
    /// the same reason as `upkeep_requirements`.
    #[serde(default)]
    pub system_status: Option<BTreeMap<String, Value>>,
    #[serde(default)]
    pub active_directive: Option<ActiveDeviceDirective>,
    #[serde(default)]
    pub travel: Option<TravelState>,
    pub access: AccessScope,
}

impl Device {
    #[must_use]
    pub fn is_stowed_in(&self, vessel: &DeviceKey) -> bool {
        self.relationships.stowed_in.as_ref() == Some(vessel)
    }

    /// Returns stow usage without trusting a stale scalar projection over the
    /// explicitly projected stowed-device relationships.
    ///
    /// SSE lifecycle events can update the relationship list between full
    /// device refreshes. Taking the larger value prevents automation from
    /// overestimating free capacity during that window.
    #[must_use]
    pub fn effective_stow_used(&self) -> i64 {
        let related = i64::try_from(self.relationships.stowed_devices.len()).unwrap_or(i64::MAX);
        self.stow_used.unwrap_or_default().max(related)
    }

    /// Returns conservatively available device-stow slots.
    #[must_use]
    pub fn free_stow_capacity(&self) -> i64 {
        self.stow_capacity
            .unwrap_or_default()
            .saturating_sub(self.effective_stow_used())
            .max(0)
    }

    #[must_use]
    pub fn is_traveling(&self) -> bool {
        self.travel.is_some()
    }
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
    #[serde(default)]
    pub travel: Option<TravelState>,
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

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocationSurveyProgress {
    pub planets_total: Option<i64>,
    pub planets_scanned: Option<i64>,
    pub moons_total: Option<i64>,
    pub moons_scanned: Option<i64>,
    pub moons_total_estimated: Option<bool>,
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
    #[serde(default)]
    pub custom_name: Option<String>,
    #[serde(default)]
    pub survey_progress: LocationSurveyProgress,
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
        self.custom_name = newer
            .custom_name
            .clone()
            .or_else(|| self.custom_name.clone());
        self.survey_progress.planets_total = newer
            .survey_progress
            .planets_total
            .or(self.survey_progress.planets_total);
        self.survey_progress.planets_scanned = newer
            .survey_progress
            .planets_scanned
            .or(self.survey_progress.planets_scanned);
        self.survey_progress.moons_total = newer
            .survey_progress
            .moons_total
            .or(self.survey_progress.moons_total);
        self.survey_progress.moons_scanned = newer
            .survey_progress
            .moons_scanned
            .or(self.survey_progress.moons_scanned);
        self.survey_progress.moons_total_estimated = newer
            .survey_progress
            .moons_total_estimated
            .or(self.survey_progress.moons_total_estimated);
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
pub struct ResourceSite {
    pub key: ResourceSiteKey,
    pub location: Option<LocationKey>,
    pub site_type: Option<String>,
    pub name: Option<String>,
    #[serde(default)]
    pub resources: BTreeMap<String, Value>,
    #[serde(default)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocationEvent {
    pub key: LocationEventKey,
    pub location: Option<LocationKey>,
    pub event_type: Option<String>,
    pub tier: Option<i64>,
    pub title: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub criteria: Vec<BTreeMap<String, Value>>,
    #[serde(default)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IncomingObjectStatus {
    #[default]
    Detected,
    DiversionActive,
    Partial,
    Diverted,
    Impacted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IncomingObject {
    pub key: IncomingObjectKey,
    pub star: Option<StarKey>,
    pub size_class: Option<String>,
    pub impact_target: Option<LocationKey>,
    pub impact_eta: Option<String>,
    pub discovery_source: Option<String>,
    pub status: IncomingObjectStatus,
    pub propulsor: Option<DeviceKey>,
    #[serde(default)]
    pub extra: BTreeMap<String, Value>,
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
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub remaining_stock: Option<i64>,
    #[serde(default)]
    pub extra: BTreeMap<String, Value>,
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Blueprint {
    pub id: BlueprintId,
    pub device_type: Option<DeviceType>,
    #[serde(default)]
    pub short_description: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub print_time_seconds: Option<f64>,
    #[serde(default)]
    pub resources: BTreeMap<String, i64>,
    #[serde(default)]
    pub components: BTreeMap<String, i64>,
    pub features: Vec<DeviceFeature>,
    pub directives: Vec<DeviceDirective>,
    #[serde(default)]
    pub cargo_capacity: Option<i64>,
    #[serde(default)]
    pub attach_capacity: Option<i64>,
    #[serde(default)]
    pub stow_capacity: Option<i64>,
    #[serde(default)]
    pub queue_size: Option<i64>,
    /// Sanitized forward-compatible blueprint fields not yet modeled.
    #[serde(default)]
    pub unknown: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub id: Option<i64>,
    pub title: Option<String>,
    pub body: Option<String>,
    pub category: Option<String>,
    #[serde(default)]
    pub subcategory: Option<String>,
    pub message_type: Option<String>,
    pub is_read: Option<bool>,
    pub created_at: Option<String>,
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
    /// Whether the catalogue or account star knowledge reports an active system ward.
    #[serde(default)]
    pub has_ward: Option<bool>,
    /// Whether an owned Replicant has supplied account star knowledge for this system.
    #[serde(default)]
    pub knowledge_observed: bool,
    /// Whether the account has explored this system.
    #[serde(default)]
    pub explored: Option<bool>,
    /// Whether account-wide star knowledge has detected life in this system.
    #[serde(default)]
    pub has_life: Option<bool>,
    pub region: Option<String>,
}

/// Compatibility view of account-shared star knowledge from one Replicant.
///
/// Canonical facts are persisted once on [`Star`]. Replicant-relative distance
/// and travel estimates are intentionally ephemeral and are not durably stored.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StarKnowledge {
    pub replicant: ReplicantKey,
    pub star: StarKey,
    pub position: Option<GalacticPosition>,
    pub spectral_type: Option<String>,
    pub entry_point: Option<LocationKey>,
    pub explored: Option<bool>,
    pub has_hub: Option<bool>,
    #[serde(default)]
    pub has_ward: Option<bool>,
    pub has_life: Option<bool>,
    pub region: Option<String>,
    pub distance_from_replicant: Option<f64>,
    pub estimated_travel_time: Option<i64>,
}
