//! Trade controller gateways and handles.
//!
//! `reference/replicant-space-2-5-1/trading/*` and the corrected OpenAPI corpus
//! document no request or response schema for any trading operation — every
//! trading response is the shared default-error shape only
//! (`src/raw/trading.rs` is deliberately untyped for the same reason). This
//! gateway stays equally untyped rather than inventing a schema; the
//! durable-operation coverage (`device_create_trade`, `device_fulfill_trade`,
//! `device_delete_trade`, already registered as durable operations) is what makes
//! `create`/`execute`/`delete` safe managed mutations.

use serde_json::Value;

use crate::raw::JsonObject;
use crate::{Client, Result};

use super::operation::{self, Operation};

/// Gateway returned by [`crate::Client::trading`].
#[derive(Clone, Debug)]
pub struct TradingGateway {
    client: Client,
}

impl TradingGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists trade controllers visible to a replicant: local system trades,
    /// plus galaxy-wide relay-network trades if the replicant has an FTL
    /// relay.
    pub async fn visible_to(&self, replicant_code: &str) -> Result<Value> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .trading()
            .visible_to_replicant(replicant_code)
            .await?
            .value)
    }

    /// Scopes further calls to one trade controller device.
    #[must_use]
    pub fn for_controller(&self, controller_code: impl Into<String>) -> TradeControllerHandle {
        TradeControllerHandle {
            client: self.client.clone(),
            controller_code: controller_code.into(),
        }
    }

    /// Creates a new trade on a controller.
    pub async fn create(&self, controller_code: &str, request: JsonObject) -> Result<Operation> {
        self.for_controller(controller_code).create(request).await
    }

    /// Executes (fulfills) one unit of a trade as a buyer.
    pub async fn execute(&self, controller_code: &str, trade_code: &str) -> Result<Operation> {
        self.for_controller(controller_code)
            .execute(trade_code)
            .await
    }

    /// Deletes a trade, returning its escrowed rewards to the owner.
    pub async fn delete(&self, controller_code: &str, trade_code: &str) -> Result<Operation> {
        self.for_controller(controller_code)
            .delete(trade_code)
            .await
    }
}

/// A trade controller device, scoped for trade listing and mutation.
#[derive(Clone, Debug)]
pub struct TradeControllerHandle {
    client: Client,
    controller_code: String,
}

impl TradeControllerHandle {
    /// Lists this controller's current trades. A successful call is a
    /// complete traversal of this controller's trade set (the endpoint is
    /// unpaginated), so it may reconcile the caller's view of which trades
    /// still exist.
    pub async fn trades(&self) -> Result<Vec<Value>> {
        self.client.ensure_open()?;
        let response = self
            .client
            .managed_raw()
            .trading()
            .list(&self.controller_code)
            .await?;
        Ok(response
            .value
            .get("trades")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    /// Re-fetches this controller's trade list. An alias for [`Self::trades`]
    /// matching this client's `sync` vocabulary for explicit reconciliation
    /// (for example, after a `trade.completed` event).
    pub async fn sync(&self) -> Result<Vec<Value>> {
        self.trades().await
    }

    /// Creates a new trade on this controller.
    pub async fn create(&self, request: JsonObject) -> Result<Operation> {
        operation::device_create_trade(&self.client, &self.controller_code, request).await
    }

    /// Executes (fulfills) one unit of a trade as a buyer. The buyer
    /// replicant is server-selected.
    pub async fn execute(&self, trade_code: &str) -> Result<Operation> {
        operation::device_fulfill_trade(&self.client, &self.controller_code, trade_code).await
    }

    /// Deletes a trade, returning its escrowed rewards to the owner.
    pub async fn delete(&self, trade_code: &str) -> Result<Operation> {
        operation::device_delete_trade(&self.client, &self.controller_code, trade_code).await
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use crate::managed::test_client_at as client_at;

    #[tokio::test]
    async fn trades_extracts_the_trades_array_from_the_untyped_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/TC1/trades"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "trades": [{"trade_code": "TRD-1"}, {"trade_code": "TRD-2"}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let trades = client
            .trading()
            .for_controller("TC1")
            .trades()
            .await
            .expect("trades");
        assert_eq!(trades.len(), 2);

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn execute_dispatches_a_bare_post_with_no_invented_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/TC1/trades/TRD-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let operation = client
            .trading()
            .execute("TC1", "TRD-1")
            .await
            .expect("execute");
        assert_eq!(
            operation.status().await.expect("status"),
            crate::managed::OperationStatus::AwaitingEvidence
        );

        let requests = server.received_requests().await.expect("recorded requests");
        assert_eq!(requests.len(), 1);
        assert!(requests[0].body.is_empty());
        client.close().await.expect("close");
    }
}
