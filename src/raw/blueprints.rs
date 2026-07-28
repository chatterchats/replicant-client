//! Unlocked account blueprint catalogue (`GET /v1/blueprints`).

use reqwest::Method;
use serde::Deserialize;

use crate::error::Error;
use crate::raw::{Client, JsonObject, RawResponse, RequestSafety};

/// One unlocked device blueprint.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct Blueprint {
    /// Device type this blueprint prints.
    pub device_type: Option<String>,
    /// Short catalogue description.
    pub short_description: Option<String>,
    /// Full description.
    pub description: Option<String>,
    /// Base print time in seconds. Replicant Space 2.3.3 emits whole seconds;
    /// `f64` is retained for source compatibility and accepts integer JSON.
    pub print_time: Option<f64>,
    /// Base structural strength.
    pub strength: Option<f64>,
    /// Cargo capacity, if the device can carry cargo.
    pub cargo_capacity: Option<i64>,
    /// Attach capacity, if the device can host attached devices.
    pub attach_capacity: Option<i64>,
    /// Stow capacity, if the device can stow other devices.
    pub stow_capacity: Option<i64>,
    /// Print queue size, if the device can print.
    pub queue_size: Option<i64>,
    /// Current number of BobNet relay hubs this blueprint contributes to.
    pub current_hubs: Option<i64>,
    /// Feature flags this device type exposes.
    pub features: Option<Vec<String>>,
    /// AMI directives this device type supports.
    pub directives: Option<Vec<String>>,
    /// Resource cost to print, keyed by resource type.
    pub resources: Option<JsonObject>,
    /// Component cost to print, keyed by component type.
    pub components: Option<JsonObject>,
    /// Any other fields the server returns.
    #[serde(flatten)]
    pub extra: JsonObject,
}

/// Response body for `GET /v1/blueprints`.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct BlueprintListResponse {
    /// Unlocked blueprints.
    #[serde(default)]
    pub blueprints: Vec<Blueprint>,
}

/// Typed client for `/v1/blueprints`.
#[derive(Clone, Debug)]
pub struct BlueprintsClient {
    client: Client,
}

impl BlueprintsClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists unlocked blueprints for this account.
    pub async fn list(&self) -> Result<RawResponse<BlueprintListResponse>, Error> {
        self.client
            .execute(Method::GET, "v1/blueprints", true, RequestSafety::SafeRead)
            .await
    }
}
