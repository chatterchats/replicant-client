//! Resource inventory (`GET /v1/inventory`,
//! `GET /v1/replicants/{replicant_code}/inventory`).

use reqwest::Method;
use serde::Deserialize;

use crate::error::Error;
use crate::raw::common::{encode_path_segment, with_query};
use crate::raw::{Client, RawResponse, RequestSafety};

/// One resource stack at a location.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct InventoryItem {
    /// Stack quantity.
    pub quantity: Option<i64>,
    /// Resource type key.
    pub resource_type: Option<String>,
}

/// Inventory held at a single location.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct LocationInventory {
    /// Resource stacks at this location.
    #[serde(default)]
    pub items: Vec<InventoryItem>,
    /// Location designation.
    pub location: Option<String>,
    /// Human-readable location name.
    pub location_name: Option<String>,
}

/// Query parameters for `GET /v1/inventory`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AccountInventoryQuery {
    /// Restrict to a single location designation.
    pub location: Option<String>,
    /// Opaque string cursor from a previous page's `next_cursor`.
    pub cursor: Option<String>,
    /// Maximum number of locations to return.
    pub limit: Option<i64>,
}

/// Response body for `GET /v1/inventory`, addressed by an opaque string
/// cursor (unlike the integer cursors used elsewhere in the contract).
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct AccountInventoryResponse {
    /// Inventory grouped by location.
    #[serde(default)]
    pub locations: Vec<LocationInventory>,
    /// Cursor for the next page, or `None` if this is the last page.
    pub next_cursor: Option<String>,
}

/// Response body for `GET /v1/replicants/{replicant_code}/inventory`: the
/// replicant's current-location inventory, plus a breakdown across every
/// location in its current star system.
#[non_exhaustive]
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct SystemInventoryResponse {
    /// Resource stacks at the replicant's current location.
    #[serde(default)]
    pub items: Vec<InventoryItem>,
    /// The replicant's current location designation.
    pub location: Option<String>,
    /// Human-readable current location name.
    pub location_name: Option<String>,
    /// Inventory at every location in the current star system.
    #[serde(default)]
    pub locations: Vec<LocationInventory>,
    /// The replicant's current star designation.
    pub star: Option<String>,
    /// Human-readable current star name.
    pub star_name: Option<String>,
}

/// Typed client for inventory operations.
#[derive(Clone, Debug)]
pub struct InventoryClient {
    client: Client,
}

impl InventoryClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists inventory across every system where the account has devices.
    pub async fn list(
        &self,
        query: &AccountInventoryQuery,
    ) -> Result<RawResponse<AccountInventoryResponse>, Error> {
        let path = with_query(
            "v1/inventory",
            &[
                ("location", query.location.clone()),
                ("cursor", query.cursor.clone()),
                ("limit", query.limit.map(|value| value.to_string())),
            ],
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Fetches a single replicant's current-system inventory.
    pub async fn for_replicant(
        &self,
        replicant_code: &str,
        location: Option<&str>,
    ) -> Result<RawResponse<SystemInventoryResponse>, Error> {
        let base = format!(
            "v1/replicants/{}/inventory",
            encode_path_segment(replicant_code)
        );
        let path = with_query(&base, &[("location", location.map(str::to_string))]);
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }
}
