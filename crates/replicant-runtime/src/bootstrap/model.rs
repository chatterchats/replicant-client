#![allow(missing_docs)]

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
    StagingAtSource,
    StagedAtSource,
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
    #[serde(default)]
    pub requested: String,
    #[serde(default)]
    pub code: String,
    pub name: Option<String>,
    #[serde(default)]
    pub vessel: String,
}

impl ReplicantIdentity {
    pub fn pending(requested: impl Into<String>) -> Self {
        let requested = requested.into();
        Self {
            requested: requested.clone(),
            code: String::new(),
            name: Some(requested),
            vessel: String::new(),
        }
    }

    pub fn is_resolved(&self) -> bool {
        !self.code.is_empty() && !self.vessel.is_empty()
    }

    pub fn query(&self) -> &str {
        if self.requested.is_empty() {
            self.name.as_deref().unwrap_or(&self.code)
        } else {
            &self.requested
        }
    }

    pub fn migrate(&mut self) {
        if self.requested.is_empty() {
            self.requested = self
                .name
                .clone()
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| self.code.clone());
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PrintState {
    #[serde(default)]
    pub targets: BTreeMap<String, i64>,
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
    /// Stable payload role, e.g. `mining-1`, `relays-1`, `beacons-1`, or `general-1`.
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub devices: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChildMissions {
    /// Legacy child path retained for mission-file compatibility; quick scouting is parent-checkpointed.
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
    #[serde(default)]
    pub source_system: String,
    #[serde(default)]
    pub source_entry: String,
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
    /// Fire-and-forget replacement prints for source-hub Surge Carriers borrowed by this ark.
    #[serde(default)]
    pub carrier_replacement_print: PrintState,
    #[serde(default)]
    pub assets: BTreeMap<String, Vec<String>>,
    /// Total attachment-carrier count reserved for this ark.
    #[serde(default)]
    pub carrier_target: i64,
    /// Number of source-hub Surge Carriers borrowed from idle stock and later replaced.
    #[serde(default)]
    pub reused_carrier_target: i64,
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

#[cfg(test)]
mod tests {
    use super::ReplicantIdentity;

    #[test]
    fn future_replicant_keeps_the_requested_name() {
        let identity = ReplicantIdentity::pending("Chats-3");
        assert_eq!(identity.query(), "Chats-3");
        assert!(!identity.is_resolved());
    }

    #[test]
    fn legacy_identity_migrates_to_a_stable_query() {
        let mut identity = ReplicantIdentity {
            requested: String::new(),
            code: "96593446".into(),
            name: Some("Chats-1".into()),
            vessel: "F7CD8684".into(),
        };
        identity.migrate();
        assert_eq!(identity.query(), "Chats-1");
        assert!(identity.is_resolved());
    }
}
