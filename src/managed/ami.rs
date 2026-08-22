//! Typed AMI controller handles.
//!
//! Every AMI controller (mining, survey, transport, fleet) shares one common
//! command set — `adopt`, `release`, `launch`, `withdraw`, `assemble`,
//! `activate`, `deactivate`, `clear_directive` — documented on
//! `reference/replicant-space-2-5-1/ami/index.md`. Each controller type then adds
//! its own `set_directive` catalogue, documented on its own
//! `reference/replicant-space-2-5-1/ami/<kind>-controller/index.md` page. AMI
//! digest events (`ami.*.digest`) are periodic operational reports, not
//! complete fleet snapshots, so they are exposed only through
//! [`crate::managed::EventsGateway`] like any other account event, never
//! folded into [`crate::Device`].

use std::collections::BTreeMap;

use serde_json::Value;

use crate::domain::{DeviceDirective, DeviceType};
use crate::raw::{
    JsonObject,
    devices::{DeviceCommand, TargetsCommand},
};
use crate::{Error, Result};

use super::gateways::DeviceHandle;
use super::operation::Operation;

fn targets(codes: impl IntoIterator<Item = impl Into<String>>) -> TargetsCommand {
    let mut codes = codes.into_iter().map(Into::into);
    let first = codes.next();
    let rest: Vec<String> = codes.collect();
    if rest.is_empty() {
        TargetsCommand {
            device: first,
            ..Default::default()
        }
    } else {
        let mut all = first.into_iter().collect::<Vec<_>>();
        all.extend(rest);
        TargetsCommand {
            devices: Some(Value::Array(all.into_iter().map(Value::String).collect())),
            ..Default::default()
        }
    }
}

/// Rejects a directive submission the controller's latest cached snapshot
/// does not currently advertise. A stale or absent capability list never
/// blocks the call; the server remains the authoritative validator either
/// way — this only improves local diagnostics, matching the same leniency
/// [`crate::managed::operation`]'s device-command capability check uses.
fn ensure_directive_available(handle: &DeviceHandle, directive: &DeviceDirective) -> Result<()> {
    let Some(observation) = handle.client().managed_state().device(handle.key()) else {
        return Ok(());
    };
    let known = &observation.value.available_directives;
    if !known.is_empty() && !known.iter().any(|available| available == directive) {
        return Err(Error::Operation {
            message: format!(
                "device `{}` does not currently advertise directive `{}`",
                handle.id().as_str(),
                directive.as_str()
            ),
        });
    }
    Ok(())
}

/// Rejects construction of a typed controller handle when the device's
/// latest cached snapshot names a different, known device type. An absent
/// or uncached device type never blocks construction; the server remains
/// authoritative for the actual command dispatch.
fn ensure_device_type(handle: &DeviceHandle, expected: DeviceType, kind: &str) -> Result<()> {
    let Some(observation) = handle.client().managed_state().device(handle.key()) else {
        return Ok(());
    };
    if let Some(actual) = &observation.value.device_type
        && *actual != expected
    {
        return Err(Error::Configuration {
            message: format!(
                "device `{}` is not a {kind} controller (cached device_type is `{}`)",
                handle.id().as_str(),
                actual.as_str()
            ),
        });
    }
    Ok(())
}

fn resources_i64(resources: &BTreeMap<String, i64>) -> JsonObject {
    resources
        .iter()
        .map(|(key, value)| (key.clone(), Value::from(*value)))
        .collect()
}

fn resources_f64(resources: &BTreeMap<String, f64>) -> JsonObject {
    resources
        .iter()
        .map(|(key, value)| (key.clone(), Value::from(*value)))
        .collect()
}

macro_rules! common_controller_methods {
    () => {
        /// This controller's underlying device handle.
        #[must_use]
        pub fn device(&self) -> &DeviceHandle {
            &self.device
        }

        /// Brings one or more ownerless (or directly-controlled) devices
        /// into this controller's fleet.
        pub async fn adopt(
            &self,
            devices: impl IntoIterator<Item = impl Into<String>>,
        ) -> Result<Operation> {
            self.device
                .command(DeviceCommand::Adopt(targets(devices)))
                .await
        }

        /// Hands one or more adopted devices back to direct control.
        pub async fn release(
            &self,
            devices: impl IntoIterator<Item = impl Into<String>>,
        ) -> Result<Operation> {
            self.device
                .command(DeviceCommand::Release(targets(devices)))
                .await
        }

        /// Deploys the fleet and starts executing the current directive.
        pub async fn launch(&self) -> Result<Operation> {
            self.device.command(DeviceCommand::Launch).await
        }

        /// Requests that the controller recall its fleet.
        pub async fn withdraw(&self) -> Result<Operation> {
            self.device.command(DeviceCommand::Withdraw).await
        }

        /// Brings the fleet home to this controller's current location
        /// without ending the directive.
        pub async fn assemble(&self) -> Result<Operation> {
            self.device.command(DeviceCommand::Assemble).await
        }

        /// Resumes a stopped directive from where it left off.
        pub async fn activate(&self) -> Result<Operation> {
            self.device.command(DeviceCommand::Activate).await
        }

        /// Pauses the current directive without clearing its configuration.
        pub async fn deactivate(&self) -> Result<Operation> {
            self.device.command(DeviceCommand::Deactivate).await
        }

        /// Drops the current directive entirely.
        pub async fn clear_directive(&self) -> Result<Operation> {
            self.device.command(DeviceCommand::ClearDirective).await
        }
    };
}

async fn set_directive(
    device: &DeviceHandle,
    directive: &'static str,
    configuration: Option<JsonObject>,
) -> Result<Operation> {
    device
        .command(DeviceCommand::SetDirective {
            directive: directive.to_string(),
            configuration,
            notify: None,
        })
        .await
}

/// Directives available on an AMI mining controller
/// (`reference/replicant-space-2-5-1/ami/mining-controller/index.md`).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum MiningDirective {
    /// Mine a specific amount of each resource, then deactivate.
    GatherResources {
        /// Resource type to target quantity.
        resources: BTreeMap<String, i64>,
    },
    /// Assign available drones evenly across available resources.
    GatherEvenly,
    /// Maintain a per-resource ratio across stockpiles.
    MaintainRatios {
        /// Resource type to target ratio.
        ratios: BTreeMap<String, f64>,
    },
    /// Gather whatever resource there is least of first.
    DepleteSmallest,
    /// Mine a salvage site to depletion, then optionally recall the fleet.
    GatherSalvage {
        /// The salvage site's location designation.
        location: String,
        /// Whether to recall the fleet once the site is depleted.
        recall: bool,
    },
}

impl MiningDirective {
    fn wire(&self) -> (&'static str, Option<JsonObject>) {
        match self {
            Self::GatherResources { resources } => {
                ("gather_resources", Some(resources_i64(resources)))
            }
            Self::GatherEvenly => ("gather_evenly", None),
            Self::MaintainRatios { ratios } => ("maintain_ratios", Some(resources_f64(ratios))),
            Self::DepleteSmallest => ("deplete_smallest", None),
            Self::GatherSalvage { location, recall } => {
                let mut configuration = JsonObject::new();
                configuration.insert("location".into(), Value::String(location.clone()));
                configuration.insert("recall".into(), Value::Bool(*recall));
                ("gather_salvage", Some(configuration))
            }
        }
    }

    fn domain(&self) -> DeviceDirective {
        DeviceDirective::from(self.wire().0)
    }
}

/// A typed handle to an AMI mining controller device.
#[derive(Clone, Debug)]
pub struct MiningController {
    device: DeviceHandle,
}

impl MiningController {
    pub(crate) fn new(device: DeviceHandle) -> Result<Self> {
        ensure_device_type(&device, DeviceType::MiningController, "mining")?;
        Ok(Self { device })
    }

    common_controller_methods!();

    /// Configures this controller's mining directive.
    pub async fn set_directive(&self, directive: MiningDirective) -> Result<Operation> {
        ensure_directive_available(&self.device, &directive.domain())?;
        let (name, configuration) = directive.wire();
        set_directive(&self.device, name, configuration).await
    }
}

/// Directives available on an AMI survey controller
/// (`reference/replicant-space-2-5-1/ami/survey-controller/index.md`).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum SurveyDirective {
    /// Sweep every body in the current system.
    SurveySystem {
        /// `"all"`, `"none"`, or a server-defined subset selector.
        planets: String,
        /// `"all"`, `"none"`, or a server-defined subset selector.
        moons: String,
        /// Whether to bring the fleet home once the sweep completes.
        recall: bool,
    },
    /// Sweep the belt for additional resource sites to track.
    BeltSearch,
}

impl SurveyDirective {
    fn wire(&self) -> (&'static str, Option<JsonObject>) {
        match self {
            Self::SurveySystem {
                planets,
                moons,
                recall,
            } => {
                let mut configuration = JsonObject::new();
                configuration.insert("planets".into(), Value::String(planets.clone()));
                configuration.insert("moons".into(), Value::String(moons.clone()));
                configuration.insert("recall".into(), Value::Bool(*recall));
                ("survey_system", Some(configuration))
            }
            Self::BeltSearch => ("belt_search", None),
        }
    }

    fn domain(&self) -> DeviceDirective {
        DeviceDirective::from(self.wire().0)
    }
}

/// A typed handle to an AMI survey controller device.
#[derive(Clone, Debug)]
pub struct SurveyController {
    device: DeviceHandle,
}

impl SurveyController {
    pub(crate) fn new(device: DeviceHandle) -> Result<Self> {
        ensure_device_type(&device, DeviceType::SurveyController, "survey")?;
        Ok(Self { device })
    }

    common_controller_methods!();

    /// Configures this controller's survey directive.
    pub async fn set_directive(&self, directive: SurveyDirective) -> Result<Operation> {
        ensure_directive_available(&self.device, &directive.domain())?;
        let (name, configuration) = directive.wire();
        set_directive(&self.device, name, configuration).await
    }
}

/// Directives available on an AMI transport controller
/// (`reference/replicant-space-2-5-1/ami/transport-controller/index.md`).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum TransportDirective {
    /// Move a specific quantity of resources from one location to another,
    /// then stop.
    Delivery {
        /// Collection location.
        collect: String,
        /// Delivery location.
        deliver: String,
        /// Resource type to required quantity.
        requirement: BTreeMap<String, i64>,
    },
    /// Continuously move all resources between two in-system locations.
    Shuttle {
        /// Collection location.
        collect: String,
        /// Delivery location.
        deliver: String,
        /// Resource types to move first, if present.
        priority: Vec<String>,
    },
    /// The interstellar variant of [`Self::Shuttle`].
    Ferry {
        /// Collection location, in the origin system.
        collect: String,
        /// Delivery location, in the destination system.
        deliver: String,
        /// Resource types to move first, if present.
        priority: Vec<String>,
    },
    /// Sweep every location with available resources to a single delivery
    /// location.
    Consolidate {
        /// Delivery location.
        deliver: String,
        /// Resource types to move first, if present.
        priority: Vec<String>,
    },
}

fn priority_value(priority: &[String]) -> Value {
    Value::Array(priority.iter().cloned().map(Value::String).collect())
}

impl TransportDirective {
    fn wire(&self) -> (&'static str, Option<JsonObject>) {
        match self {
            Self::Delivery {
                collect,
                deliver,
                requirement,
            } => {
                let mut route = JsonObject::new();
                route.insert("collect".into(), Value::String(collect.clone()));
                route.insert("deliver".into(), Value::String(deliver.clone()));
                let mut configuration = JsonObject::new();
                configuration.insert("route".into(), Value::Object(route));
                configuration.insert(
                    "requirement".into(),
                    Value::Object(resources_i64(requirement)),
                );
                ("delivery", Some(configuration))
            }
            Self::Shuttle {
                collect,
                deliver,
                priority,
            } => {
                let mut configuration = JsonObject::new();
                configuration.insert("collect".into(), Value::String(collect.clone()));
                configuration.insert("deliver".into(), Value::String(deliver.clone()));
                configuration.insert("priority".into(), priority_value(priority));
                ("shuttle", Some(configuration))
            }
            Self::Ferry {
                collect,
                deliver,
                priority,
            } => {
                let mut configuration = JsonObject::new();
                configuration.insert("collect".into(), Value::String(collect.clone()));
                configuration.insert("deliver".into(), Value::String(deliver.clone()));
                configuration.insert("priority".into(), priority_value(priority));
                ("ferry", Some(configuration))
            }
            Self::Consolidate { deliver, priority } => {
                let mut configuration = JsonObject::new();
                configuration.insert("deliver".into(), Value::String(deliver.clone()));
                configuration.insert("priority".into(), priority_value(priority));
                ("consolidate", Some(configuration))
            }
        }
    }

    fn domain(&self) -> DeviceDirective {
        DeviceDirective::from(self.wire().0)
    }
}

/// A typed handle to an AMI transport controller device.
#[derive(Clone, Debug)]
pub struct TransportController {
    device: DeviceHandle,
}

impl TransportController {
    pub(crate) fn new(device: DeviceHandle) -> Result<Self> {
        ensure_device_type(&device, DeviceType::TransportController, "transport")?;
        Ok(Self { device })
    }

    common_controller_methods!();

    /// Configures this controller's transport directive.
    pub async fn set_directive(&self, directive: TransportDirective) -> Result<Operation> {
        ensure_directive_available(&self.device, &directive.domain())?;
        let (name, configuration) = directive.wire();
        set_directive(&self.device, name, configuration).await
    }
}

/// A typed handle to an AMI fleet controller device. Fleet controllers have
/// no directives: their only job is relaying `travel` commands to every
/// adopted device (`reference/replicant-space-2-5-1/ami/fleet-controller/index.md`).
#[derive(Clone, Debug)]
pub struct FleetController {
    device: DeviceHandle,
}

impl FleetController {
    pub(crate) fn new(device: DeviceHandle) -> Result<Self> {
        ensure_device_type(&device, DeviceType::FleetController, "fleet")?;
        Ok(Self { device })
    }

    common_controller_methods!();

    /// Issues one travel command that cascades to every adopted device (and,
    /// transitively, every device adopted by an adopted fleet controller).
    pub async fn travel(&self, destination: impl Into<String>) -> Result<Operation> {
        self.device
            .command(DeviceCommand::Travel {
                destination: destination.into(),
                dry_run: None,
                via: None,
            })
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
        AccessScope, Device, DeviceId, DeviceKey, DeviceRelationships,
        DeviceStatus as DomainDeviceStatus, LocationId, LocationKey, Observation,
        ObservationAuthority, ObservationMetadata, ObservationSource, Reachability, SourceDocument,
    };
    use crate::{Client, Error};

    use crate::managed::test_client_at as client_at;

    fn seed_device(
        client: &Client,
        code: &str,
        device_type: DeviceType,
        available_directives: Vec<DeviceDirective>,
    ) {
        let observation = Observation {
            value: Device {
                key: DeviceKey::live(DeviceId::from(code)),
                device_type: Some(device_type),
                status: Some(DomainDeviceStatus::from("active")),
                location: Some(LocationKey::live(LocationId::from("SOL-3-L4"))),
                features: Vec::new(),
                available_commands: Vec::new(),
                available_directives,
                tags: Vec::new(),
                relationships: DeviceRelationships::default(),
                attach_capacity: None,
                stow_capacity: None,
                stow_used: None,
                operational_capacity: None,
                grace_period_remaining: None,
                upkeep_requirements: Vec::new(),
                system_status: None,
                active_directive: None,
                travel: None,
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
        };
        client
            .managed_state()
            .persist_devices(&[observation])
            .expect("seed device");
    }

    fn handle(client: &Client, code: &str) -> DeviceHandle {
        client.devices().cached(code).expect("device is cached")
    }

    #[tokio::test]
    async fn as_mining_controller_rejects_a_known_mismatched_cached_device_type() {
        let client = client_at(&MockServer::start().await.uri()).await;
        seed_device(&client, "D1", DeviceType::MiningDrone, Vec::new());
        let error = handle(&client, "D1")
            .as_mining_controller()
            .expect_err("mining drone is not a mining controller");
        assert!(matches!(error, Error::Configuration { .. }));
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn as_mining_controller_allows_an_uncached_device() {
        let client = client_at(&MockServer::start().await.uri()).await;
        // Not cached at all: the server remains the authoritative validator.
        let uncached =
            DeviceHandle::for_test(client.clone(), DeviceKey::live(DeviceId::from("D2")));
        assert!(MiningController::new(uncached).is_ok());
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn set_directive_rejects_a_directive_the_snapshot_does_not_advertise() {
        let client = client_at(&MockServer::start().await.uri()).await;
        seed_device(
            &client,
            "D1",
            DeviceType::MiningController,
            vec![DeviceDirective::GatherEvenly],
        );
        let controller = MiningController::new(handle(&client, "D1")).expect("controller");
        let error = controller
            .set_directive(MiningDirective::DepleteSmallest)
            .await
            .expect_err("directive not advertised by the cached snapshot");
        assert!(matches!(error, Error::Operation { .. }));
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn set_directive_dispatches_the_documented_gather_salvage_shape() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/D1"))
            .and(body_json(serde_json::json!({
                "command": "set_directive",
                "directive": "gather_salvage",
                "configuration": { "location": "SOL-3-L4", "recall": true }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        seed_device(&client, "D1", DeviceType::MiningController, Vec::new());
        let controller = MiningController::new(handle(&client, "D1")).expect("controller");

        controller
            .set_directive(MiningDirective::GatherSalvage {
                location: "SOL-3-L4".into(),
                recall: true,
            })
            .await
            .expect("set_directive");

        server.verify().await;
        client.close().await.expect("close");
    }

    #[tokio::test]
    async fn common_commands_use_the_documented_wire_names() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/devices/D1"))
            .and(body_json(serde_json::json!({ "command": "launch" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .expect(1)
            .mount(&server)
            .await;
        let client = client_at(&server.uri()).await;
        seed_device(&client, "D1", DeviceType::SurveyController, Vec::new());
        let controller = SurveyController::new(handle(&client, "D1")).expect("controller");

        controller.launch().await.expect("launch");

        server.verify().await;
        client.close().await.expect("close");
    }
}
