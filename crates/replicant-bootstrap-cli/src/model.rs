use std::{collections::BTreeMap, path::PathBuf};

use replicant_bootstrap_planner::{BeltCandidate, BootstrapProfile};
use serde::{Deserialize, Serialize};

pub const PLAN_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionPhase {
    Planned,
    ManufacturingArk,
    LoadingArk,
    Outbound,
    QuickScouting,
    EstablishingCapital,
    InitialMining,
    SurveyingRegion,
    ExpandingRelays,
    ExpandingMining,
    CleaningUp,
    Completed,
    CompletedWithWarnings,
}

impl MissionPhase {
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::CompletedWithWarnings)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicantIdentity {
    pub code: String,
    pub name: Option<String>,
    pub vessel: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PrintState {
    pub requirements: BTreeMap<String, i64>,
    pub submission_started: bool,
    pub queued: bool,
    #[serde(default)]
    pub operation_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SeedFreighter {
    pub code: String,
    pub resource: String,
    pub quantity: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CarrierLoad {
    pub carrier: String,
    pub capacity: i64,
    #[serde(default)]
    pub devices: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildMissions {
    pub quick_survey: PathBuf,
    pub initial_mining: PathBuf,
    pub survey: PathBuf,
    pub relays: PathBuf,
    pub mining: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BootstrapMission {
    pub version: u32,
    pub mission_id: String,
    pub mission_tag: String,
    pub region_tag: String,
    pub phase: MissionPhase,
    pub region: String,
    pub source_hub: String,
    pub landing_star: String,
    pub landing_entry: String,
    pub operator: ReplicantIdentity,
    pub explorer: ReplicantIdentity,
    pub profile: BootstrapProfile,
    pub seed_quantity: i64,
    pub quick_scout_radius_ly: f64,
    pub survey_radius_ly: f64,
    pub minimum_sites: usize,
    pub maximum_sites: usize,
    pub max_concurrency: usize,
    pub print: PrintState,
    #[serde(default)]
    pub assets: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub seed_freighters: Vec<SeedFreighter>,
    #[serde(default)]
    pub carrier_loads: Vec<CarrierLoad>,
    #[serde(default)]
    pub quick_scouted_systems: Vec<String>,
    pub capital_system: Option<String>,
    pub capital_belt: Option<String>,
    pub capital_entry: Option<String>,
    #[serde(default)]
    pub survey_systems: Vec<String>,
    #[serde(default)]
    pub selected_belts: Vec<BeltCandidate>,
    pub children: ChildMissions,
    #[serde(default)]
    pub warnings: Vec<String>,
}
