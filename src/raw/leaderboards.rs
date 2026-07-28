//! Leaderboards (`/v1/leaderboards*`).
//!
//! The two simulation-scenario leaderboard endpoints are unauthenticated
//! (public high-score tables); every other leaderboard requires a token.

use reqwest::Method;
use serde::Deserialize;

use crate::error::Error;
use crate::raw::common::encode_path_segment;
use crate::raw::{Client, RawResponse, RequestSafety};

/// One published leaderboard's metadata.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct LeaderboardBoard {
    /// Flavor description.
    pub description: Option<String>,
    /// Stable leaderboard key, e.g. `"xp"`, `"distance"`.
    pub key: Option<String>,
    /// Display name.
    pub name: Option<String>,
    /// Leaderboard type.
    pub r#type: Option<String>,
}

/// Response body for `GET /v1/leaderboards`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct LeaderboardIndexResponse {
    /// All published leaderboards.
    #[serde(default)]
    pub boards: Vec<LeaderboardBoard>,
}

/// One ranked entry on a leaderboard.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct LeaderboardEntry {
    /// Contributions counted toward this entry, for contribution-based
    /// boards (e.g. megastructure).
    pub contribution_count: Option<i64>,
    /// Colony designation, when this is a colony leaderboard entry.
    pub designation: Option<String>,
    /// Replicant display name.
    pub name: Option<String>,
    /// Rank on this board.
    pub rank: Option<i64>,
    /// Replicant code.
    pub replicant_code: Option<String>,
    /// Replicant ID.
    pub replicant_id: Option<i64>,
    /// The ranked value itself (distance, trades, XP, or similar).
    pub value: Option<f64>,
}

/// Response body for standard ranked leaderboards, including colony boards.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct LeaderboardResponse {
    /// The board key this response is for.
    pub board: Option<String>,
    /// Ranked entries.
    #[serde(default)]
    pub entries: Vec<LeaderboardEntry>,
}

/// One simulation scenario's leaderboard summary.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimLeaderboardScenario {
    /// Fastest recorded completion time, in seconds.
    pub best_time_seconds: Option<i64>,
    /// Total completions recorded for this scenario.
    pub completions: Option<i64>,
    /// Scenario code.
    pub scenario_code: Option<String>,
    /// Scenario display name.
    pub scenario_name: Option<String>,
}

/// Response body for `GET /v1/leaderboards/simulations`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimLeaderboardIndexResponse {
    /// Every simulation scenario with recorded runs.
    #[serde(default)]
    pub scenarios: Vec<SimLeaderboardScenario>,
}

/// One ranked personal-best entry for a simulation scenario.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimLeaderboardEntry {
    /// When this run was completed, RFC3339.
    pub completed_at: Option<String>,
    /// Devices printed during the run.
    pub devices_printed: Option<i64>,
    /// Replicant display name.
    pub name: Option<String>,
    /// Rank on this scenario's leaderboard.
    pub rank: Option<i64>,
    /// Replicant code.
    pub replicant_code: Option<String>,
    /// Resources mined during the run.
    pub resources_mined: Option<i64>,
    /// Completion time, in seconds (lower is better).
    pub score_seconds: Option<i64>,
}

/// Response body for `GET /v1/leaderboards/simulations/{scenario_code}`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimLeaderboardResponse {
    /// Top personal-best entries for this scenario.
    #[serde(default)]
    pub entries: Vec<SimLeaderboardEntry>,
    /// Scenario code.
    pub scenario_code: Option<String>,
    /// Scenario display name.
    pub scenario_name: Option<String>,
}

/// Typed client for `/v1/leaderboards*`.
#[derive(Clone, Debug)]
pub struct LeaderboardsClient {
    client: Client,
}

impl LeaderboardsClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists all published leaderboards.
    pub async fn index(&self) -> Result<RawResponse<LeaderboardIndexResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/leaderboards",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Fetches the colony-moon suitability leaderboard.
    pub async fn colony_moon(&self) -> Result<RawResponse<LeaderboardResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/leaderboards/colony_moon",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Fetches the colony-planet suitability leaderboard.
    pub async fn colony_planet(&self) -> Result<RawResponse<LeaderboardResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/leaderboards/colony_planet",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Fetches the distance leaderboard.
    pub async fn distance(&self) -> Result<RawResponse<LeaderboardResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/leaderboards/distance",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Fetches the fleet-size leaderboard.
    pub async fn fleet(&self) -> Result<RawResponse<LeaderboardResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/leaderboards/fleet",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Fetches the megastructure-contribution leaderboard.
    pub async fn megastructure(&self) -> Result<RawResponse<LeaderboardResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/leaderboards/megastructure",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Fetches the species-reputation leaderboard.
    pub async fn reputation(&self) -> Result<RawResponse<LeaderboardResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/leaderboards/reputation",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Lists simulation scenarios with completion stats. Unauthenticated.
    pub async fn simulations(&self) -> Result<RawResponse<SimLeaderboardIndexResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/leaderboards/simulations",
                false,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Fetches the top personal bests for a simulation scenario.
    /// Unauthenticated.
    pub async fn simulation_scenario(
        &self,
        scenario_code: &str,
    ) -> Result<RawResponse<SimLeaderboardResponse>, Error> {
        let path = format!(
            "v1/leaderboards/simulations/{}",
            encode_path_segment(scenario_code)
        );
        self.client
            .execute(Method::GET, &path, false, RequestSafety::SafeRead)
            .await
    }

    /// Fetches the trade-volume leaderboard.
    pub async fn trades(&self) -> Result<RawResponse<LeaderboardResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/leaderboards/trades",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }

    /// Fetches the XP leaderboard.
    pub async fn xp(&self) -> Result<RawResponse<LeaderboardResponse>, Error> {
        self.client
            .execute(
                Method::GET,
                "v1/leaderboards/xp",
                true,
                RequestSafety::SafeRead,
            )
            .await
    }
}
