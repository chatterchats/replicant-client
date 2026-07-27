//! Device-hosted simulation scenarios (`/v1/devices/{device_code}/simulate*`).
//!
//! Entering a simulation is done through a simulator device, so every
//! operation here is scoped by `device_code`. The authenticated account's
//! own simulation run history (`GET /v1/accounts/simulations`) is a sibling
//! schema but lives on [`crate::raw::accounts::AccountsClient::simulations`],
//! matching the server's own route grouping.

use reqwest::Method;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::raw::common::encode_path_segment;
use crate::raw::{Client, JsonObject, RawResponse, RequestSafety};

/// One simulation scenario available on a simulator device.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScenarioSummary {
    /// Scenario code.
    pub code: Option<String>,
    /// Flavor description.
    pub description: Option<String>,
    /// Resources consumed to enter this scenario.
    pub entry_cost: Option<JsonObject>,
    /// Extended rules description.
    pub long_description: Option<String>,
    /// Display name.
    pub name: Option<String>,
    /// Target value for the scenario's objective.
    pub objective_target: Option<i64>,
    /// Objective type, e.g. `"survive"`, `"collect"`.
    pub objective_type: Option<String>,
    /// Timeout, in hours, before the run is abandoned.
    pub timeout_hours: Option<f64>,
    /// Scenario definition version.
    pub version: Option<i64>,
}

/// Response body for `GET /v1/devices/{device_code}/simulate`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct ScenarioListResponse {
    /// Scenarios available on this device.
    #[serde(default)]
    pub scenarios: Vec<ScenarioSummary>,
}

/// Request body for `POST /v1/devices/{device_code}/simulate`.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct SimulationEnterRequest {
    /// The replicant entering the simulation.
    pub replicant_code: String,
    /// The scenario code to run.
    pub scenario: String,
}

/// Response body for `POST /v1/devices/{device_code}/simulate`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct SimulationEnterResponse {
    /// Devices spawned into the simulation's starting scene.
    #[serde(default)]
    pub devices: Vec<JsonObject>,
    /// Scenario code that was entered.
    pub scenario_code: Option<String>,
    /// Scenario display name.
    pub scenario_name: Option<String>,
    /// Random seed for this run.
    pub seed: Option<i64>,
    /// Simulation run ID, used to cancel it later.
    pub simulation_id: Option<i64>,
    /// Starting location designation within the simulation.
    pub starting_location: Option<String>,
    /// Starting star designation within the simulation.
    pub starting_star: Option<String>,
    /// Timeout, in hours, before the run is abandoned.
    pub timeout_hours: Option<f64>,
}

/// One currently running simulation on a device.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimulationActiveSummary {
    /// Whether the authenticated account owns this run.
    pub is_mine: Option<bool>,
    /// The running replicant's display name.
    pub replicant_name: Option<String>,
    /// Scenario code.
    pub scenario_code: Option<String>,
    /// Scenario display name.
    pub scenario_name: Option<String>,
    /// Simulation run ID.
    pub simulation_id: Option<i64>,
    /// When the run started, RFC3339.
    pub started_at: Option<String>,
    /// Timeout, in hours, before the run is abandoned.
    pub timeout_hours: Option<f64>,
}

/// Response body for `GET /v1/devices/{device_code}/simulate/active`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimulationActiveResponse {
    /// Every simulation currently running on this device.
    #[serde(default)]
    pub simulations: Vec<SimulationActiveSummary>,
}

/// One completed, abandoned, or timed-out simulation run in an account's
/// history.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimulationHistoryEntry {
    /// When the run was abandoned, RFC3339, if it was.
    pub abandoned_at: Option<String>,
    /// When the run completed successfully, RFC3339, if it did.
    pub completed_at: Option<String>,
    /// Devices printed during the run.
    pub devices_printed: Option<i64>,
    /// Simulation run ID.
    pub id: Option<i64>,
    /// Resources mined during the run.
    pub resources_mined: Option<i64>,
    /// Scenario code.
    pub scenario_code: Option<String>,
    /// Scenario display name.
    pub scenario_name: Option<String>,
    /// Completion time, in seconds, if the run completed.
    pub score_seconds: Option<i64>,
    /// When the run started, RFC3339.
    pub started_at: Option<String>,
    /// When the run timed out, RFC3339, if it did.
    pub timed_out_at: Option<String>,
}

/// Response body for `GET /v1/accounts/simulations`, exposed via
/// [`crate::raw::accounts::AccountsClient::simulations`].
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SimulationHistoryResponse {
    /// The account's simulation run history.
    #[serde(default)]
    pub simulations: Vec<SimulationHistoryEntry>,
}

/// Typed client for device-hosted simulation scenarios.
#[derive(Clone, Debug)]
pub struct SimulationsClient {
    client: Client,
}

impl SimulationsClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists simulation scenarios available on a simulator device.
    pub async fn scenarios(
        &self,
        device_code: &str,
    ) -> Result<RawResponse<ScenarioListResponse>, Error> {
        let path = format!("v1/devices/{}/simulate", encode_path_segment(device_code));
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Enters a simulation scenario via a simulator device.
    pub async fn enter(
        &self,
        device_code: &str,
        request: &SimulationEnterRequest,
    ) -> Result<RawResponse<SimulationEnterResponse>, Error> {
        let path = format!("v1/devices/{}/simulate", encode_path_segment(device_code));
        self.client
            .execute_json(Method::POST, &path, true, RequestSafety::Mutating, request)
            .await
    }

    /// Lists all currently running simulations on a device.
    pub async fn active(
        &self,
        device_code: &str,
    ) -> Result<RawResponse<SimulationActiveResponse>, Error> {
        let path = format!(
            "v1/devices/{}/simulate/active",
            encode_path_segment(device_code)
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Cancels a running simulation. The contract documents no response
    /// schema for this endpoint, so the raw body is returned as-is.
    pub async fn cancel(
        &self,
        device_code: &str,
        sim_id: i64,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!(
            "v1/devices/{}/simulate/{}",
            encode_path_segment(device_code),
            sim_id
        );
        self.client
            .execute(Method::DELETE, &path, true, RequestSafety::Mutating)
            .await
    }
}
