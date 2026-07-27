//! Simulations: isolated virtual worlds entered through a `replicant_interface`
//! simulator device (`reference/replicant-space/simulations/*`).
//!
//! Starting a simulation creates `Realm::Simulation(id)` and seeds it with
//! the server's starting device loadout; ending one (abandonment,
//! completion, or expiry) removes that realm's ephemeral device projections
//! without ever touching live-world records — the two realms never collide
//! (`crate::domain::Realm`).

use serde_json::Value;

use crate::domain::{self, AccessScope, Realm, SimulationId, SimulationLifecycle};
use crate::raw;
use crate::{Client, Error, Result};

use super::operation::{self, Operation};
use super::store::StoreError;

fn observed_at() -> crate::domain::ObservationTime {
    crate::domain::ObservationTime::now()
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
        source: None,
    }
}

/// Gateway returned by [`crate::Client::simulations`].
#[derive(Clone, Debug)]
pub struct SimulationsGateway {
    client: Client,
}

/// Local-only query over committed simulation history.
#[derive(Clone, Debug)]
pub struct SimulationQuery {
    client: Client,
    mine: Option<bool>,
    scenario: Option<String>,
    completed: Option<bool>,
}

impl SimulationQuery {
    fn new(client: Client) -> Self {
        Self {
            client,
            mine: None,
            scenario: None,
            completed: None,
        }
    }
    /// Restricts results to this account's simulations.
    #[must_use]
    pub fn mine(mut self) -> Self {
        self.mine = Some(true);
        self
    }
    /// Restricts results to a scenario code.
    #[must_use]
    pub fn scenario(mut self, code: impl Into<String>) -> Self {
        self.scenario = Some(code.into());
        self
    }
    /// Restricts results to archived, completed simulations.
    #[must_use]
    pub fn completed(mut self) -> Self {
        self.completed = Some(true);
        self
    }
    /// Restricts results to simulations without a completion time.
    #[must_use]
    pub fn active(mut self) -> Self {
        self.completed = Some(false);
        self
    }
    /// Collects a stable, ID-sorted view from the current committed snapshot.
    pub async fn collect(self) -> Result<Vec<domain::Simulation>> {
        self.client.ensure_open()?;
        Ok(self
            .client
            .managed_state()
            .simulations()
            .into_iter()
            .filter(|entry| self.mine.is_none_or(|mine| entry.value.is_mine == mine))
            .filter(|entry| {
                self.scenario
                    .as_ref()
                    .is_none_or(|scenario| entry.value.scenario_code.as_ref() == Some(scenario))
            })
            .filter(|entry| {
                self.completed
                    .is_none_or(|completed| entry.value.completed_at.is_some() == completed)
            })
            .map(|entry| entry.value)
            .collect())
    }
}

impl SimulationsGateway {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    /// Starts a local query over committed simulation history.
    #[must_use]
    pub fn find(&self) -> SimulationQuery {
        SimulationQuery::new(self.client.clone())
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
        let outcome = operation.outcome().await?;
        let response = outcome.response.ok_or_else(|| Error::Operation {
            message: "simulation enter did not return an authoritative response".into(),
        })?;
        let entered = serde_json::from_value::<raw::simulations::SimulationEnterResponse>(response)
            .map_err(|error| Error::Decode {
                message: format!("simulation enter response decode failed: {error}"),
                status: Some(200),
                source: Some(Box::new(error)),
            })?;
        self.seed_realm(&entered, replicant_code).await?;
        Ok(operation)
    }

    async fn seed_realm(
        &self,
        entered: &raw::simulations::SimulationEnterResponse,
        replicant_code: &str,
    ) -> Result<()> {
        let mut observation =
            domain::simulation_start(entered, observed_at()).map_err(normalization)?;
        let realm = Realm::Simulation(observation.value.id);
        observation.value.replicant_code = Some(replicant_code.to_owned());
        let mut devices = Vec::with_capacity(entered.devices.len());
        let mut failures = Vec::new();
        for summary in &entered.devices {
            let Some(code) = summary.get("device_code").and_then(Value::as_str) else {
                failures.push("missing device_code".to_owned());
                continue;
            };
            match self.client.managed_raw().devices().get(code).await {
                Ok(response) => match domain::device_detail(
                    &response.value,
                    realm.clone(),
                    AccessScope::Owned,
                    observed_at(),
                ) {
                    Ok(device) => devices.push(device),
                    Err(_) => failures.push(code.to_owned()),
                },
                Err(_) => failures.push(code.to_owned()),
            }
        }
        observation.value.seed_failures = failures.clone();
        observation.value.lifecycle = if failures.is_empty() {
            SimulationLifecycle::Active
        } else {
            SimulationLifecycle::Synchronizing
        };
        self.client
            .managed_state()
            .persist_simulation_and_devices(observation, &devices)
            .map_err(persistence_error)?;
        for code in &failures {
            if code == "missing device_code" {
                continue;
            }
            self.client
                .managed_state()
                .enqueue_reconciliation(
                    &format!("simulation:{}:device:{code}", realm_id(&realm)),
                    &realm,
                    "device",
                    &serde_json::json!({ "id": code }),
                )
                .map_err(persistence_error)?;
        }
        if !failures.is_empty() {
            return Err(Error::Operation {
                message: "simulation seed is incomplete; durable reconciliation was queued".into(),
            });
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
        if let Some(mut simulation) = self
            .client
            .managed_state()
            .simulation(SimulationId::new(simulation_id))
        {
            simulation.value.lifecycle = match operation.status().await? {
                operation::OperationStatus::Ambiguous => SimulationLifecycle::AbandonAmbiguous,
                operation::OperationStatus::Rejected => return Ok(operation),
                _ => SimulationLifecycle::AbandonPending,
            };
            self.client
                .managed_state()
                .persist_simulation(simulation)
                .map_err(persistence_error)?;
        }
        Ok(operation)
    }

    /// Refreshes the account's simulation run history. Each returned run is
    /// committed before this method returns; history is additive and never
    /// reconciles absence into deletion.
    pub async fn history(&self) -> Result<Vec<domain::Simulation>> {
        self.client.ensure_open()?;
        let response = self
            .client
            .managed_raw()
            .accounts()
            .simulations()
            .await?
            .value;
        let mut simulations = Vec::with_capacity(response.simulations.len());
        for entry in &response.simulations {
            let observation =
                domain::simulation_history(entry, observed_at()).map_err(normalization)?;
            let value = observation.value.clone();
            self.client
                .managed_state()
                .persist_simulation(observation)
                .map_err(persistence_error)?;
            simulations.push(value);
        }
        Ok(simulations)
    }
}

/// Removes a simulation realm's ephemeral device projections. Called after
/// an explicit abandonment resolves, and reactively when this client
/// observes `simulation.completed`/`simulation.expired`/`simulation.abandoned`
/// account events (`super::events`), since a timed-out or otherwise
/// server-ended run is not something this client itself requested.
pub(crate) fn cleanup_realm(client: &Client, id: SimulationId) -> Result<()> {
    if let Some(mut simulation) = client.managed_state().simulation(id) {
        if !simulation.value.is_mine {
            return Ok(());
        }
        simulation.value.lifecycle = SimulationLifecycle::Ended;
        simulation.value.seed_failures.clear();
        client
            .managed_state()
            .persist_simulation(simulation)
            .map_err(persistence_error)?;
    }
    client
        .managed_state()
        .purge_realm_devices(&Realm::Simulation(id))
        .map_err(persistence_error)
}

fn realm_id(realm: &Realm) -> String {
    match realm {
        Realm::Live => "live".into(),
        Realm::Simulation(id) => format!("simulation:{}", id.get()),
    }
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
    async fn missing_seed_detail_is_persisted_as_synchronizing_work() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/SIMDEV1/simulate"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "simulation_id": 43,
                "devices": [{"device_code": "MISSING"}]
            })))
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        assert!(
            client
                .simulations()
                .start("SIMDEV1", "R1", "mining_rush")
                .await
                .is_err(),
            "the accepted operation cannot report a silently partial seed"
        );
        let simulation = client
            .managed_state()
            .simulation(SimulationId::new(43))
            .expect("persisted synchronizing run");
        assert_eq!(
            simulation.value.lifecycle,
            SimulationLifecycle::Synchronizing
        );
        assert_eq!(simulation.value.seed_failures, vec!["MISSING"]);

        let mut queued = false;
        while let Some(work) = client
            .managed_state()
            .claim_reconciliation_work()
            .expect("claim durable work")
        {
            if work.kind == "device" && work.realm == Realm::Simulation(SimulationId::new(43)) {
                queued = true;
            }
        }
        assert!(queued, "missing detail has realm-qualified durable work");
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn abandon_retains_the_realm_until_authoritative_end_evidence() {
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
                .is_some(),
            "a successful DELETE alone is not proof the run ended"
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

    #[tokio::test]
    async fn active_other_player_run_does_not_create_an_owned_realm() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/SIMDEV1/simulate/active"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "simulations": [{"simulation_id": 69, "is_mine": false}]
            })))
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;

        client
            .simulations()
            .active("SIMDEV1")
            .await
            .expect("active simulations");

        assert!(client.managed_state().simulations().is_empty());
        client.close().await.expect("close");
    }
}
