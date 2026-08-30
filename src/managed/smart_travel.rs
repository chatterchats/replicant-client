//! Local-only smart travel route selection.
//!
//! This gateway deliberately reads only durable managed projections.  It never
//! refreshes the galaxy catalogue or performs a route preview: callers can use
//! the returned waypoint list in the one durable travel operation defined by
//! the OpenAPI contract, while `None` leaves the server's `auto` routing mode
//! untouched.

use crate::domain::{
    DeviceId, DeviceKey, ReplicantId, ReplicantKey, SmartTravelPlan, SmartTravelPlanner, Star,
    TravelProfile,
};
use crate::{Client, Result};
use tracing::{debug, info};

/// A local managed router for replicant and device travel.
#[derive(Clone, Debug)]
pub struct SmartTravelRouter {
    client: Client,
}

impl SmartTravelRouter {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Selects a locally computed route for an owned or visible Replicant.
    ///
    /// No request is made.  If the Replicant, its location, the catalogue, or
    /// any required observation is unavailable or stale, this returns `Ok(None)`.
    pub async fn route_for_replicant(
        &self,
        code: &str,
        destination: &str,
    ) -> Result<Option<SmartTravelPlan>> {
        self.client.ensure_open()?;
        let key = ReplicantKey::live(ReplicantId::from(code));
        let Some(observation) = self.client.managed_state().replicant(&key) else {
            return self.unavailable("replicant_not_cached", code, destination);
        };
        if observation.metadata.stale {
            return self.unavailable("replicant_observation_stale", code, destination);
        }
        let replicant = observation.value;
        let Some(origin) = replicant.location.as_ref() else {
            return self.unavailable("replicant_location_missing", code, destination);
        };

        let profile = match replicant.hosted_device.as_ref() {
            Some(device_key) => match self.client.managed_state().device(device_key) {
                Some(device_observation) => {
                    if device_observation.metadata.stale {
                        return self.unavailable(
                            "hosted_device_observation_stale",
                            code,
                            destination,
                        );
                    }
                    TravelProfile::for_device_type(device_observation.value.device_type.as_ref())
                }
                None => {
                    return self.unavailable("hosted_device_not_cached", code, destination);
                }
            },
            None => TravelProfile::standard(),
        };
        self.plan(code, origin.id.as_str(), destination, profile)
    }

    /// Selects a locally computed route for a cached device.
    ///
    /// No request is made.  Missing or stale device/location/catalogue data
    /// returns `Ok(None)`, allowing the caller to preserve server `auto`.
    pub async fn route_for_device(
        &self,
        code: &str,
        destination: &str,
    ) -> Result<Option<SmartTravelPlan>> {
        self.client.ensure_open()?;
        let key = DeviceKey::live(DeviceId::from(code));
        let Some(observation) = self.client.managed_state().device(&key) else {
            return self.unavailable("device_not_cached", code, destination);
        };
        if observation.metadata.stale {
            return self.unavailable("device_observation_stale", code, destination);
        }
        let device = observation.value;
        let Some(origin) = device.location.as_ref() else {
            return self.unavailable("device_location_missing", code, destination);
        };
        let profile = TravelProfile::for_device_type(device.device_type.as_ref());
        self.plan(code, origin.id.as_str(), destination, profile)
    }

    fn plan(
        &self,
        source: &str,
        origin_code: &str,
        destination_code: &str,
        profile: TravelProfile,
    ) -> Result<Option<SmartTravelPlan>> {
        let observations = self.client.galaxy().catalogue_observations();
        if observations.is_empty() {
            return self.unavailable("catalogue_missing", source, destination_code);
        }
        if observations
            .iter()
            .any(|observation| observation.metadata.stale)
        {
            return self.unavailable("catalogue_stale", source, destination_code);
        }
        let stars: Vec<Star> = observations
            .into_iter()
            .map(|observation| observation.value)
            .collect();
        let Some(origin) = resolve_system(origin_code, &stars) else {
            return self.unavailable("origin_not_in_catalogue", source, destination_code);
        };
        let Some(destination) = resolve_system(destination_code, &stars) else {
            return self.unavailable("destination_not_in_catalogue", source, destination_code);
        };
        let Some(plan) = SmartTravelPlanner::default().plan(&origin, &destination, &stars, profile)
        else {
            return self.unavailable("planner_no_route", source, destination_code);
        };

        let route = if plan.is_direct { "direct" } else { "hub" };
        let direct_seconds = plan.direct_seconds;
        let smart_seconds = plan.estimated_seconds;
        let saved_seconds = plan.saved_seconds;
        let saved_pct = if direct_seconds > 0 {
            saved_seconds as f64 * 100.0 / direct_seconds as f64
        } else {
            0.0
        };
        info!(
            target: "replicant_client::travel",
            event = "travel.smart_route_selected",
            origin = origin_code,
            destination = destination_code,
            route,
            direct_seconds,
            smart_seconds,
            saved_seconds,
            saved_pct,
            "selected local smart travel route"
        );
        Ok(Some(plan))
    }

    fn unavailable<T>(
        &self,
        reason: &'static str,
        origin: &str,
        destination: &str,
    ) -> Result<Option<T>> {
        debug!(
            target: "replicant_client::travel",
            event = "travel.smart_route_unavailable",
            reason,
            origin,
            destination,
            "smart travel unavailable; preserving server auto route"
        );
        Ok(None)
    }
}

/// Resolves a system designation exactly, or by the longest designation
/// prefix for a local body/location code.
fn resolve_system(code: &str, stars: &[Star]) -> Option<Star> {
    stars
        .iter()
        .filter(|star| {
            let designation = star.key.id.as_str();
            code.strip_prefix(designation)
                .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with('-'))
        })
        .max_by_key(|star| star.key.id.as_str().len())
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{
        AccessScope, GalacticPosition, LocationId, LocationKey, Observation, ObservationAuthority,
        ObservationMetadata, ObservationSource, Reachability, Replicant, SourceDocument, StarKey,
    };

    fn star(designation: &str) -> Star {
        Star {
            key: StarKey::live(designation.into()),
            name: None,
            spectral_type: None,
            entry_point: Some(crate::domain::LocationKey::live(LocationId::from(
                designation,
            ))),
            position: Some(GalacticPosition::default()),
            has_hub: Some(false),
            has_ward: None,
            knowledge_observed: false,
            explored: Some(true),
            has_life: None,
            region: None,
        }
    }

    fn observation<T>(value: T, stale: bool) -> Observation<T> {
        Observation {
            value,
            metadata: ObservationMetadata {
                source: ObservationSource::RestDetail,
                authority: ObservationAuthority::EntitySnapshot,
                observed_at: "2026-08-29T00:00:00Z".into(),
                access: AccessScope::Owned,
                reachability: Reachability::Reachable,
                stale,
                source_document: SourceDocument {
                    operation: "smart travel test".into(),
                    request_id: None,
                    document_id: None,
                },
            },
        }
    }

    #[test]
    fn resolves_exact_and_delimited_longest_local_prefix() {
        let stars = vec![star("SOL"), star("SOLAR")];
        assert_eq!(
            resolve_system("SOL", &stars).unwrap().key.id.as_str(),
            "SOL"
        );
        assert_eq!(
            resolve_system("SOLAR-1", &stars).unwrap().key.id.as_str(),
            "SOLAR"
        );
        assert_eq!(
            resolve_system("SOL-1-L4", &stars).unwrap().key.id.as_str(),
            "SOL"
        );
        assert!(resolve_system("SOLARITY-1", &stars).is_none());
        assert!(resolve_system("VEGA", &stars).is_none());
    }

    #[tokio::test]
    async fn empty_local_state_preserves_server_auto() {
        let client = crate::managed::test_client_at("http://127.0.0.1:1").await;
        let router = SmartTravelRouter::new(client.clone());
        assert!(
            router
                .route_for_device("D1", "SOL")
                .await
                .unwrap()
                .is_none()
        );
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn missing_hosted_device_preserves_server_auto() {
        let client = crate::managed::test_client_at("http://127.0.0.1:1").await;
        client
            .managed_state()
            .persist_replicant(observation(
                Replicant {
                    key: ReplicantKey::live("R1".into()),
                    name: None,
                    is_npc: Some(false),
                    status: None,
                    location: Some(LocationKey::live("SOL-1".into())),
                    hosted_device: Some(DeviceKey::live("MISSING".into())),
                    travel: None,
                    private: None,
                    access: AccessScope::Owned,
                },
                false,
            ))
            .expect("seed replicant");
        let router = SmartTravelRouter::new(client.clone());

        assert!(
            router
                .route_for_replicant("R1", "VEGA")
                .await
                .unwrap()
                .is_none()
        );
        client.close().await.unwrap();
    }

    #[tokio::test]
    async fn any_stale_catalogue_row_preserves_server_auto() {
        let client = crate::managed::test_client_at("http://127.0.0.1:1").await;
        client
            .managed_state()
            .replace_catalogue(
                vec![
                    observation(star("A"), false),
                    observation(star("D"), false),
                    observation(star("H"), true),
                ],
                Some("2026-08-29T00:00:00Z".into()),
            )
            .expect("seed catalogue");
        let router = SmartTravelRouter::new(client.clone());

        assert!(
            router
                .plan("D1", "A", "D", TravelProfile::standard())
                .unwrap()
                .is_none()
        );
        client.close().await.unwrap();
    }
}
