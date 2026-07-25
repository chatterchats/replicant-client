//! Device-hosted trade controllers (`/v1/devices/{device_code}/trades*`,
//! `GET /v1/replicants/{replicant_code}/traders`).
//!
//! The contract documents no request or response schema for any trading
//! operation (every response is the shared default-error shape only), so
//! this client is deliberately untyped: requests take a caller-supplied
//! [`JsonObject`] and responses are returned as raw [`serde_json::Value`].
//! Do not invent a typed shape the docs/OpenAPI do not define.

use reqwest::Method;

use crate::error::Error;
use crate::raw::common::encode_path_segment;
use crate::raw::{Client, JsonObject, RawResponse, RequestSafety};

/// Typed client for trade controller operations.
#[derive(Clone, Debug)]
pub struct TradingClient {
    client: Client,
}

impl TradingClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists all trades on a trade controller device. Requires the caller to
    /// have a replicant in the same star system.
    pub async fn list(&self, device_code: &str) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!("v1/devices/{}/trades", encode_path_segment(device_code));
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }

    /// Creates a new trade on a trade controller device.
    pub async fn create(
        &self,
        device_code: &str,
        request: &JsonObject,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!("v1/devices/{}/trades", encode_path_segment(device_code));
        self.client
            .execute_json(Method::POST, &path, true, RequestSafety::Mutating, request)
            .await
    }

    /// Deletes a trade and returns its escrowed rewards to the owner.
    pub async fn delete(
        &self,
        device_code: &str,
        trade_code: &str,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!(
            "v1/devices/{}/trades/{}",
            encode_path_segment(device_code),
            encode_path_segment(trade_code)
        );
        self.client
            .execute(Method::DELETE, &path, true, RequestSafety::Mutating)
            .await
    }

    /// Fulfills one unit of a trade as a buyer. The buyer replicant is
    /// server-selected (the caller's replicant at the controller's location
    /// with the most recent arrival).
    pub async fn fulfill(
        &self,
        device_code: &str,
        trade_code: &str,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!(
            "v1/devices/{}/trades/{}",
            encode_path_segment(device_code),
            encode_path_segment(trade_code)
        );
        self.client
            .execute(Method::POST, &path, true, RequestSafety::Mutating)
            .await
    }

    /// Lists trade controllers visible to a replicant: local system trades,
    /// plus galaxy-wide relay-network trades if the replicant has an FTL
    /// relay.
    pub async fn visible_to_replicant(
        &self,
        replicant_code: &str,
    ) -> Result<RawResponse<serde_json::Value>, Error> {
        let path = format!(
            "v1/replicants/{}/traders",
            encode_path_segment(replicant_code)
        );
        self.client
            .execute(Method::GET, &path, true, RequestSafety::SafeRead)
            .await
    }
}
