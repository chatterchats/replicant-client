//! First-class replicant travel: route preview and durable departure.
//!
//! `destination`, `dry_run`, `notify`, and `via` are exactly the fields the
//! corrected `app_schemas_travel_TravelRequestSchema` OpenAPI schema defines
//! for `POST /v1/replicants/{code}/travel` (`reference/replicant-space-2-5-2/openapi.json`);
//! `via` accepts `"auto"` (the server default), `"direct"`, or an explicit
//! waypoint list, so [`TravelVia`] models exactly those three shapes and
//! nothing invented beyond the schema.

use serde_json::Value;

use crate::raw;
use crate::{Client, Error, OperationId, Result};

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

    async fn effective_via(&self, destination: &str) -> Result<Option<Value>> {
        let fallback = self.via.clone().map(TravelVia::into_value);
        if matches!(
            self.via.as_ref(),
            Some(TravelVia::Direct | TravelVia::Waypoints(_))
        ) {
            return Ok(fallback);
        }

        let plan = self
            .client
            .smart_travel()
            .route_for_replicant(&self.replicant_code, destination)
            .await?;
        Ok(plan
            .filter(|plan| !plan.intermediate_systems.is_empty())
            .map(|plan| {
                Value::Array(
                    plan.explicit_waypoints_for(destination)
                        .into_iter()
                        .map(Value::String)
                        .collect(),
                )
            })
            .or(fallback))
    }

    /// Computes and returns the route the nav system would take, without
    /// departing. Local-only against the network in the sense that it never
    /// registers a durable operation; it does perform the one preview
    /// request.
    pub async fn preview(&self) -> Result<TravelPreview> {
        self.client.ensure_open()?;
        let destination = self.destination()?;
        let request = raw::replicants::TravelRequest {
            destination: Some(destination.clone()),
            dry_run: Some(true),
            notify: self.notify(),
            via: self.effective_via(&destination).await?,
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
        let destination = self.destination()?;
        let request = raw::replicants::TravelRequest {
            destination: Some(destination.clone()),
            dry_run: None,
            notify: self.notify(),
            via: self.effective_via(&destination).await?,
        };
        operation::replicant_travel(&self.client, &self.replicant_code, request).await
    }

    /// Departs under a caller-supplied durable operation identity.
    ///
    /// This is intended for restart-safe workflows that must never submit a
    /// second travel mutation after a process restart. Reusing an operation
    /// identity is safe only when the complete travel intent is unchanged.
    pub async fn depart_with_id(&self, operation_id: OperationId) -> Result<Operation> {
        let destination = self.destination()?;
        let request = raw::replicants::TravelRequest {
            destination: Some(destination.clone()),
            dry_run: None,
            notify: self.notify(),
            via: self.effective_via(&destination).await?,
        };
        operation::replicant_travel_with_id(
            &self.client,
            &self.replicant_code,
            request,
            operation_id,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_json, method, path},
    };

    use super::*;
    use crate::domain::{
        AccessScope, GalacticPosition, LocationKey, Observation, ObservationAuthority,
        ObservationMetadata, ObservationSource, Reachability, Realm, Replicant, ReplicantKey,
        SourceDocument, Star, StarKey,
    };
    use crate::managed::test_client_at as client_at;

    fn observation<T>(value: T) -> Observation<T> {
        Observation {
            value,
            metadata: ObservationMetadata {
                source: ObservationSource::RestDetail,
                authority: ObservationAuthority::EntitySnapshot,
                observed_at: "2026-08-29T00:00:00Z".into(),
                access: AccessScope::Owned,
                reachability: Reachability::Reachable,
                stale: false,
                source_document: SourceDocument {
                    operation: "smart travel test".into(),
                    request_id: None,
                    document_id: None,
                },
            },
        }
    }

    fn star(designation: &str, x: f64, has_hub: bool) -> Observation<Star> {
        observation(Star {
            key: StarKey::in_realm(Realm::Live, designation.into()),
            name: None,
            spectral_type: None,
            entry_point: None,
            position: Some(GalacticPosition { x, y: 0.0, z: 0.0 }),
            has_hub: Some(has_hub),
            has_ward: None,
            knowledge_observed: false,
            explored: Some(true),
            has_life: None,
            region: None,
        })
    }

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
    async fn preview_preserves_explicit_waypoints_without_smart_routing() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/replicants/R1/travel"))
            .and(body_json(serde_json::json!({
                "destination": "SOL",
                "dry_run": true,
                "via": ["ALPHA", "BETA"]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "preview", "total_time_seconds": 42.0
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        let preview = TravelBuilder::new(client.clone(), "R1".to_string())
            .to("SOL")
            .via_waypoints(["ALPHA".to_string(), "BETA".to_string()])
            .preview()
            .await
            .expect("preview");
        assert_eq!(preview.total_time_seconds, Some(42.0));

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn default_preview_inherits_smart_hub_waypoints() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/replicants/R1/travel"))
            .and(body_json(serde_json::json!({
                "destination": "D",
                "dry_run": true,
                "via": ["B"]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "preview", "total_time_seconds": 285.0
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        client
            .managed_state()
            .replace_catalogue(
                vec![
                    star("A", 0.0, false),
                    star("B", 2.0, true),
                    star("D", 10.0, false),
                ],
                Some("2026-08-29T00:00:00Z".into()),
            )
            .expect("seed catalogue");
        client
            .managed_state()
            .persist_replicant(observation(Replicant {
                key: ReplicantKey::live("R1".into()),
                name: None,
                is_npc: Some(false),
                status: None,
                location: Some(LocationKey::live("A-1".into())),
                hosted_device: None,
                travel: None,
                private: None,
                access: AccessScope::Owned,
            }))
            .expect("seed replicant");

        TravelBuilder::new(client.clone(), "R1".to_owned())
            .to("D")
            .preview()
            .await
            .expect("smart preview");

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn smart_hub_route_to_remote_body_appends_destination_star_waypoint() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/replicants/R1/travel"))
            .and(body_json(serde_json::json!({
                "destination": "D-2-L4",
                "dry_run": true,
                "via": ["B", "D"]
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "preview", "total_time_seconds": 285.0
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        client
            .managed_state()
            .replace_catalogue(
                vec![
                    star("A", 0.0, false),
                    star("B", 2.0, true),
                    star("D", 10.0, false),
                ],
                Some("2026-08-29T00:00:00Z".into()),
            )
            .expect("seed catalogue");
        client
            .managed_state()
            .persist_replicant(observation(Replicant {
                key: ReplicantKey::live("R1".into()),
                name: None,
                is_npc: Some(false),
                status: None,
                location: Some(LocationKey::live("A-1".into())),
                hosted_device: None,
                travel: None,
                private: None,
                access: AccessScope::Owned,
            }))
            .expect("seed replicant");

        TravelBuilder::new(client.clone(), "R1".to_owned())
            .to("D-2-L4")
            .preview()
            .await
            .expect("smart preview");

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
