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
    pub hosted_by: Option<ReplicantKey>,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Location {
    pub key: LocationKey,
    pub location_type: Option<LocationType>,
    pub scanned: Option<bool>,
    pub system_scanned: Option<bool>,
    pub system_tags: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LocationOverview {
    pub key: LocationKey,
    pub device_count: i64,
    pub replicant_count: i64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Star {
    pub key: StarKey,
    pub name: Option<String>,
    pub spectral_type: Option<String>,
    pub entry_point: Option<LocationKey>,
}
