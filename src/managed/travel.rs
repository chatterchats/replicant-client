//! First-class replicant travel: route preview and durable departure.
//!
//! `destination`, `dry_run`, `notify`, and `via` are exactly the fields the
//! corrected `app_schemas_travel_TravelRequestSchema` OpenAPI schema defines
//! for `POST /v1/replicants/{code}/travel` (`reference/replicant-space-2-5-1/openapi.json`);
//! `via` accepts `"auto"` (the server default), `"direct"`, or an explicit
//! waypoint list, so [`TravelVia`] models exactly those three shapes and
//! nothing invented beyond the schema.

use serde_json::Value;

use crate::raw;
use crate::{Client, Error, Result};

use super::operation::{self, Operation};

/// A route preview returned by [`TravelBuilder::preview`]. Identical to the
/// server's normal travel response shape; `preview` only sets `dry_run` on
/// the request and never registers a durable operation.
pub type TravelPreview = raw::replicants::TravelResponse;

/// Route-mode hint accepted by the current travel request's `via` field.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum TravelVia {
    /// Server-selected routing (the default when `via` is omitted).
    Auto,
    /// Force a direct route, skipping relay/hub optimization.
    Direct,
    /// An explicit sequence of waypoint location codes.
    Waypoints(Vec<String>),
}

impl TravelVia {
    fn into_value(self) -> Value {
        match self {
            Self::Auto => Value::String("auto".into()),
            Self::Direct => Value::String("direct".into()),
            Self::Waypoints(points) => {
                Value::Array(points.into_iter().map(Value::String).collect())
            }
        }
    }
}

/// Builds one replicant travel request. [`TravelBuilder::preview`] never
/// mutates state; [`TravelBuilder::depart`] registers a durable operation
/// like every other unsafe mutation this client exposes.
#[derive(Clone, Debug)]
pub struct TravelBuilder {
    client: Client,
    replicant_code: String,
    destination: Option<String>,
    via: Option<TravelVia>,
    notify_device: Option<String>,
}

impl TravelBuilder {
    pub(crate) fn new(client: Client, replicant_code: String) -> Self {
        Self {
            client,
            replicant_code,
            destination: None,
            via: None,
            notify_device: None,
        }
    }

    /// Sets the destination: a star, planet, moon, belt, or Lagrange point
    /// designation.
    #[must_use]
    pub fn to(mut self, destination: impl Into<String>) -> Self {
        self.destination = Some(destination.into());
        self
    }

    /// Sets the route mode.
    #[must_use]
    pub fn via(mut self, via: TravelVia) -> Self {
        self.via = Some(via);
        self
    }

    /// Forces a direct route, skipping relay/hub optimization.
    #[must_use]
    pub fn via_direct(self) -> Self {
        self.via(TravelVia::Direct)
    }

    /// Routes through an explicit sequence of waypoint location codes.
    #[must_use]
    pub fn via_waypoints(self, waypoints: impl IntoIterator<Item = String>) -> Self {
        self.via(TravelVia::Waypoints(waypoints.into_iter().collect()))
    }

    /// Requests a notification to a device (e.g. a BobNet relay) on arrival.
    #[must_use]
    pub fn notify_device(mut self, device_code: impl Into<String>) -> Self {
        self.notify_device = Some(device_code.into());
        self
    }

    fn notify(&self) -> Option<raw::JsonObject> {
        self.notify_device.clone().map(|device| {
            let mut object = raw::JsonObject::new();
            object.insert("device".into(), Value::String(device));
            object
        })
    }

    fn destination(&self) -> Result<String> {
        self.destination
            .clone()
            .ok_or_else(|| Error::Configuration {
                message: "travel requires a destination".into(),
            })
    }

    /// Computes and returns the route the nav system would take, without
    /// departing. Local-only against the network in the sense that it never
    /// registers a durable operation; it does perform the one preview
    /// request.
    pub async fn preview(&self) -> Result<TravelPreview> {
        self.client.ensure_open()?;
        let request = raw::replicants::TravelRequest {
            destination: Some(self.destination()?),
            dry_run: Some(true),
            notify: self.notify(),
            via: self.via.clone().map(TravelVia::into_value),
        };
        let response = self
            .client
            .managed_raw()
            .replicants()
            .travel(&self.replicant_code, &request)
            .await?;
        Ok(response.value)
    }

    /// Departs for the configured destination as a durable operation.
    pub async fn depart(&self) -> Result<Operation> {
        let request = raw::replicants::TravelRequest {
            destination: Some(self.destination()?),
            dry_run: None,
            notify: self.notify(),
            via: self.via.clone().map(TravelVia::into_value),
        };
        operation::replicant_travel(&self.client, &self.replicant_code, request).await
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method, path},
    };

    use super::*;
    use crate::managed::test_client_at as client_at;

    #[tokio::test]
    async fn preview_sends_dry_run_and_never_creates_a_durable_operation() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/replicants/R1/travel"))
            .and(body_json(serde_json::json!({
                "destination": "SOL", "dry_run": true, "via": "direct"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "preview", "total_time_seconds": 28.0
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let plan = TravelBuilder::new(client.clone(), "R1".to_string())
            .to("SOL")
            .via_direct();
        let preview = plan.preview().await.expect("preview");
        assert_eq!(preview.total_time_seconds, Some(28.0));
        assert!(
            client
                .operations()
                .list_unresolved()
                .await
                .expect("list unresolved")
                .is_empty(),
            "preview must never register a durable operation"
        );

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn depart_omits_dry_run_and_registers_a_durable_operation() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/replicants/R1/travel"))
            .and(body_json(serde_json::json!({ "destination": "SOL" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "travel_initiated"
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let operation = TravelBuilder::new(client.clone(), "R1".to_string())
            .to("SOL")
            .depart()
            .await
            .expect("depart");
        // Travel is a `device_command`/`replicant_travel`-shaped operation
        // that expects further event/reconciliation evidence, not an
        // immediately-terminal one.
        assert_eq!(
            operation.status().await.expect("status"),
            crate::managed::OperationStatus::AwaitingEvidence
        );

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn missing_destination_is_rejected_before_any_request() {
        let client = client_at(&MockServer::start().await.uri()).await;
        let plan = TravelBuilder::new(client.clone(), "R1".to_string());
        assert!(matches!(
            plan.preview().await.expect_err("destination required"),
            Error::Configuration { .. }
        ));
        assert!(matches!(
            plan.depart().await.expect_err("destination required"),
            Error::Configuration { .. }
        ));
        client.close().await.expect("close");
    }
}
