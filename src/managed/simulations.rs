//! Simulations: isolated virtual worlds entered through a `replicant_interface`
//! simulator device (`reference/replicant-space/simulations/*`).
//!
//! Starting a simulation creates `Realm::Simulation(id)` and seeds it with
//! the server's starting device loadout; ending one (abandonment,
//! completion, or expiry) removes that realm's ephemeral device projections
//! without ever touching live-world records — the two realms never collide
//! (`crate::domain::Realm`).

use serde_json::Value;

use crate::domain::{self, AccessScope, Realm, SimulationId};
use crate::raw;
use crate::{Client, Error, Result};

use super::operation::{self, Operation};
use super::store::StoreError;

fn observed_at() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_default()
}

fn persistence_error(_: StoreError) -> Error {
    Error::Persistence {
        message: "SQLite store operation failed".into(),
    }
}

fn normalization(error: domain::NormalizeError) -> Error {
    Error::Decode {
        message: error.to_string(),
        status: None,
    }
}

/// Gateway returned by [`crate::Client::simulations`].
#[derive(Clone, Debug)]
pub struct SimulationsGateway {
    client: Client,
}

impl SimulationsGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Lists simulation scenarios available on a simulator interface device.
    pub async fn scenarios(
        &self,
        interface_code: &str,
    ) -> Result<raw::simulations::ScenarioListResponse> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .simulations()
            .scenarios(interface_code)
            .await?
            .value)
    }

    /// Lists simulations currently running on a simulator interface device.
    /// Includes other players' runs; only entries with `is_mine` establish
    /// this account's owned simulation state.
    pub async fn active(
        &self,
        interface_code: &str,
    ) -> Result<raw::simulations::SimulationActiveResponse> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .simulations()
            .active(interface_code)
            .await?
            .value)
    }

    /// Starts a simulation: plugs `replicant_code` into `interface_code`
    /// running `scenario`. A durable operation like every other unsafe
    /// mutation; once it resolves, this also creates `Realm::Simulation(id)`
    /// and seeds it with the server's starting device loadout. The live
    /// vessel and live devices are left untouched but out of range for the
    /// plugged-in replicant.
    pub async fn start(
        &self,
        interface_code: &str,
        replicant_code: &str,
        scenario: &str,
    ) -> Result<Operation> {
        let operation = operation::device_enter_simulation(
            &self.client,
            interface_code,
            raw::simulations::SimulationEnterRequest {
                replicant_code: replicant_code.to_string(),
                scenario: scenario.to_string(),
            },
        )
        .await?;
        if let Ok(outcome) = operation.outcome().await
            && let Some(response) = &outcome.response
            && let Ok(entered) = serde_json::from_value::<raw::simulations::SimulationEnterResponse>(
                response.clone(),
            )
        {
            let _ = self.seed_realm(&entered).await;
        }
        Ok(operation)
    }

    async fn seed_realm(&self, entered: &raw::simulations::SimulationEnterResponse) -> Result<()> {
        let observation =
            domain::simulation_start(entered, observed_at()).map_err(normalization)?;
        let realm = Realm::Simulation(observation.value.id);
        self.client
            .managed_state()
            .persist_simulation(observation)
            .map_err(persistence_error)?;
        for summary in &entered.devices {
            let Some(code) = summary.get("device_code").and_then(Value::as_str) else {
                continue;
            };
            let Ok(response) = self.client.managed_raw().devices().get(code).await else {
                continue;
            };
            if let Ok(device) = domain::device_detail(
                &response.value,
                realm.clone(),
                AccessScope::Owned,
                observed_at(),
            ) {
                let _ = self.client.managed_state().persist_devices(&[device]);
            }
        }
        Ok(())
    }

    /// Abandons a running simulation. A durable operation; once it resolves,
    /// this also removes that simulation realm's ephemeral device
    /// projections. The simulation's own history row is archived, not
    /// deleted (`client.simulations().history()`), and live-world state is
    /// never touched.
    pub async fn abandon(&self, interface_code: &str, simulation_id: i64) -> Result<Operation> {
        let operation =
            operation::device_abandon_simulation(&self.client, interface_code, simulation_id)
                .await?;
        cleanup_realm(&self.client, SimulationId::new(simulation_id));
        Ok(operation)
    }

    /// The account's simulation run history: completed, abandoned, and
    /// expired runs. Distinct from the live realm's current state.
    pub async fn history(&self) -> Result<raw::simulations::SimulationHistoryResponse> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_raw()
            .accounts()
            .simulations()
            .await?
            .value)
    }
}

/// Removes a simulation realm's ephemeral device projections. Called after
/// an explicit abandonment resolves, and reactively when this client
/// observes `simulation.completed`/`simulation.expired`/`simulation.abandoned`
/// account events (`super::events`), since a timed-out or otherwise
/// server-ended run is not something this client itself requested.
pub(crate) fn cleanup_realm(client: &Client, id: SimulationId) {
    let _ = client
        .managed_state()
        .purge_realm_devices(&Realm::Simulation(id));
}

#[cfg(test)]
mod tests {
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::domain::{
        AccessScope, Device, DeviceId, DeviceKey, DeviceRelationships,
        DeviceStatus as DomainDeviceStatus, Observation, ObservationAuthority, ObservationMetadata,
        ObservationSource, Reachability, SourceDocument,
    };
    use crate::managed::client::StartupPolicy;
    use crate::raw::{SecretString, Url};

    async fn client_at(base_url: &str) -> Client {
        Client::builder()
            .authentication_token(SecretString::from("token".to_string()))
            .base_url(Url::parse(base_url).expect("mock URL"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only client")
    }

    fn device_observation(key: DeviceKey) -> Observation<Device> {
        Observation {
            value: Device {
                key,
                device_type: Some(crate::domain::DeviceType::from("heaven_vessel")),
                status: Some(DomainDeviceStatus::from("active")),
                location: None,
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives: Vec::new(),
                tags: Vec::new(),
                relationships: DeviceRelationships::default(),
                access: AccessScope::Owned,
            },
            metadata: ObservationMetadata {
                source: ObservationSource::RestDetail,
                authority: ObservationAuthority::EntitySnapshot,
                observed_at: "2026-07-25T00:00:00Z".into(),
                access: AccessScope::Owned,
                reachability: Reachability::Reachable,
                stale: false,
                source_document: SourceDocument {
                    operation: "GET /v1/devices/{device_code}".into(),
                    request_id: None,
                    document_id: None,
                },
            },
        }
    }

    fn seed_live_device(client: &Client, code: &str) {
        client
            .managed_state()
            .persist_devices(&[device_observation(DeviceKey::live(DeviceId::from(code)))])
            .expect("seed live device");
    }

    #[tokio::test]
    async fn start_creates_a_separate_realm_and_leaves_the_live_device_untouched() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/SIMDEV1/simulate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "simulation_id": 42,
                "scenario_code": "mining_rush",
                "scenario_name": "Mining Rush",
                "starting_star": "VIRTUHOMAM",
                "starting_location": "VIRTUHOMAM-3-L4",
                "seed": 7,
                "devices": [
                    { "device_code": "SIM1", "device_type": "heaven_vessel" },
                    { "device_code": "SIM2", "device_type": "mining_drone" }
                ],
                "timeout_hours": 24
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/SIM1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "SIM1", "device_type": "heaven_vessel", "status": "active"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/SIM2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "SIM2", "device_type": "mining_drone", "status": "active"
            })))
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        // A live device with the same code family, to prove realm isolation.
        seed_live_device(&client, "LIVE1");

        let operation = client
            .simulations()
            .start("SIMDEV1", "R1", "mining_rush")
            .await
            .expect("start");
        operation.wait().await.expect("wait");

        let realm = Realm::Simulation(SimulationId::new(42));
        assert!(
            client
                .managed_state()
                .device(&DeviceKey::in_realm(realm.clone(), DeviceId::from("SIM1")))
                .is_some()
        );
        assert!(
            client
                .managed_state()
                .device(&DeviceKey::in_realm(realm, DeviceId::from("SIM2")))
                .is_some()
        );
        assert!(
            client
                .managed_state()
                .device(&DeviceKey::live(DeviceId::from("LIVE1")))
                .is_some()
        );
        assert!(
            client
                .managed_state()
                .simulation(SimulationId::new(42))
                .is_some()
        );

        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn abandon_purges_only_that_simulation_realm_leaving_live_devices_intact() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/devices/SIMDEV1/simulate/7"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        seed_live_device(&client, "LIVE1");
        let realm = Realm::Simulation(SimulationId::new(7));
        client
            .managed_state()
            .persist_devices(&[device_observation(DeviceKey::in_realm(
                realm.clone(),
                DeviceId::from("SIM1"),
            ))])
            .expect("seed simulation device");

        client
            .simulations()
            .abandon("SIMDEV1", 7)
            .await
            .expect("abandon");

        assert!(
            client
                .managed_state()
                .device(&DeviceKey::in_realm(realm, DeviceId::from("SIM1")))
                .is_none(),
            "abandoned realm's device is purged"
        );
        assert!(
            client
                .managed_state()
                .device(&DeviceKey::live(DeviceId::from("LIVE1")))
                .is_some(),
            "live device is never touched by simulation cleanup"
        );

        client.close().await.expect("close");
    }
}
