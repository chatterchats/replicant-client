use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use replicant_client::{
    Client, Star,
    domain::{Device, DeviceStatus, DeviceType, Inventory, InventoryOwner, Location, LocationType},
};
use replicant_mining_planner::{
    BlueprintSpec, CARGO_FREIGHTER, FactoryWorkload, MAINTENANCE_DRONE, MINING_CONTROLLER,
    MINING_DRONE, PrintBatch, QuantityMap, SURGE_CARRIER, SURVEY_CONTROLLER, SURVEY_DRONE,
    SYSTEM_WARD, TRANSPORT_CONTROLLER, add_quantities, blueprint_resource_cost,
    mining_site_requirements, schedule_prints, shortages, site_tag,
};
use replicant_printing::managed::discover_factories;
use replicant_workflow::{
    AllocationSet, RequirementScope, ResourceKey, ResourceRequirement, WorkItemSpec, WorkflowId,
    WorkflowKind, WorkflowServiceIntent,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

use crate::worker_state::OPERATIONAL_REGIONAL_WORKER_CAPABILITY;

mod executor;
mod validation;

const PLAN_VERSION: u32 = 1;

/// Error type returned by the reusable mining workflow.
pub type AnyError = Box<dyn StdError + Send + Sync + 'static>;
/// Result type returned by the reusable mining workflow.
pub type AnyResult<T> = Result<T, AnyError>;

fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> AnyError {
    io::Error::new(kind, message.into()).into()
}

struct Config {
    systems: Vec<String>,
    replicant: Option<String>,
    hub: String,
    transport_routes: Vec<AmiTransportRouteIntent>,
    plan_path: PathBuf,
    replace_plan: bool,
    wait_timeout: Duration,
    max_concurrency: usize,
}

impl Config {
    fn requested_systems(&self) -> AnyResult<Vec<String>> {
        let systems = self
            .systems
            .iter()
            .map(|value| value.trim().to_ascii_uppercase())
            .filter(|value| !value.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if systems.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "planning requires at least one system",
            ));
        }
        Ok(systems)
    }
}

/// Durable top-level mining expansion phase.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissionPhase {
    Planned,
    ManufacturingSites,
    DeployingSites,
    ManufacturingRoutes,
    ActivatingRoutes,
    ReturningCarriers,
    Completed,
    CompletedWithWarnings,
}

impl MissionPhase {
    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::CompletedWithWarnings)
    }

    fn advance_to(self, next: Self) -> Self {
        if self.is_terminal() || self.rank() > next.rank() {
            self
        } else {
            next
        }
    }

    const fn rank(self) -> u8 {
        match self {
            Self::Planned => 0,
            Self::ManufacturingSites => 1,
            Self::DeployingSites => 2,
            Self::ManufacturingRoutes => 3,
            Self::ActivatingRoutes => 4,
            Self::ReturningCarriers => 5,
            Self::Completed | Self::CompletedWithWarnings => 6,
        }
    }
}

/// Durable deployment phase for one mining site.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SitePhase {
    Planned,
    Ready,
    Outbound,
    Deploying,
    Adopting,
    Verifying,
    /// Legacy checkpoint read from version-one missions as deployment resumes.
    Configuring,
    Operational,
}

/// Exact AMI transport route requested by a mining campaign.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct AmiTransportRouteIntent {
    /// Origin system containing the collection belt.
    pub system: String,
    /// Exact discovered belt location to collect from.
    pub collect: String,
    /// Exact System Hub location to deliver to.
    pub deliver: String,
}

impl AmiTransportRouteIntent {
    /// Projects this route into the generic durable service-intent contract.
    #[must_use]
    pub fn workflow_service_intent(&self) -> WorkflowServiceIntent {
        WorkflowServiceIntent {
            service: "ami_transport".to_owned(),
            dimensions: [
                ("collect".to_owned(), self.collect.clone()),
                ("deliver".to_owned(), self.deliver.clone()),
            ]
            .into_iter()
            .collect(),
        }
    }
}

/// Tri-state evidence used by resource and transport reconciliation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EvidenceState {
    /// Complete positive evidence exists.
    Present,
    /// Complete evidence proves the predicate false.
    Absent,
    /// Authority or required fields are incomplete.
    Unknown,
}

/// Strict health audit of an AMI transport route.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TransportServiceAudit {
    pub(crate) state: EvidenceState,
    pub(crate) controller: Option<String>,
    pub(crate) freighter: Option<String>,
}

/// Durable activation phase for one mining transport route.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoutePhase {
    Planned,
    Ready,
    Activating,
    Active,
}

/// Manufacturing purpose recorded on one print batch.
#[allow(missing_docs)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrintPurpose {
    Site,
    Route,
}

/// Device assignments checkpointed for one mining site.
#[allow(missing_docs)]
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SiteAssets {
    pub mining_controller: Option<String>,
    pub mining_drones: Vec<String>,
    pub survey_controller: Option<String>,
    pub survey_drones: Vec<String>,
    pub maintenance_drone: Option<String>,
    /// Owned System Ward assigned to protect this mining system. A System Hub
    /// may satisfy protection without populating this field because wards and
    /// hubs cannot be active in the same system.
    #[serde(default)]
    pub system_ward: Option<String>,
}

impl SiteAssets {
    fn codes(&self) -> Vec<String> {
        self.mining_controller
            .iter()
            .chain(&self.mining_drones)
            .chain(self.survey_controller.iter())
            .chain(&self.survey_drones)
            .chain(self.maintenance_drone.iter())
            .chain(self.system_ward.iter())
            .cloned()
            .collect()
    }

    fn counts(&self) -> QuantityMap {
        let mut counts = QuantityMap::new();
        counts.insert(
            MINING_CONTROLLER.into(),
            if self.mining_controller.is_some() {
                1
            } else {
                0
            },
        );
        counts.insert(
            MINING_DRONE.into(),
            i64::try_from(self.mining_drones.len()).unwrap_or(i64::MAX),
        );
        counts.insert(
            SURVEY_CONTROLLER.into(),
            if self.survey_controller.is_some() {
                1
            } else {
                0
            },
        );
        counts.insert(
            SURVEY_DRONE.into(),
            i64::try_from(self.survey_drones.len()).unwrap_or(i64::MAX),
        );
        counts.insert(
            MAINTENANCE_DRONE.into(),
            if self.maintenance_drone.is_some() {
                1
            } else {
                0
            },
        );
        counts.insert(
            SYSTEM_WARD.into(),
            if self.system_ward.is_some() { 1 } else { 0 },
        );
        counts
    }
}

/// Durable checkpoint for one mining-site deployment.
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SiteMission {
    pub system: String,
    pub belt: String,
    pub density: String,
    pub tag: String,
    pub phase: SitePhase,
    pub assets: SiteAssets,
    pub missing: QuantityMap,
    pub carrier: Option<String>,
}

/// Durable checkpoint for one mining ferry route.
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RouteMission {
    pub system: String,
    pub belt: String,
    pub tag: String,
    pub phase: RoutePhase,
    pub controller: Option<String>,
    pub freighter: Option<String>,
}

/// Durable manufacturing checkpoint for mining execution.
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExecutionPrintBatch {
    pub purpose: PrintPurpose,
    pub factory_code: String,
    pub device_type: String,
    pub quantity: i64,
    pub projected_finish_seconds: f64,
    pub batch_tag: String,
    pub submission_started: bool,
    pub submitted: bool,
    pub operation_id: Option<String>,
    pub produced_codes: Vec<String>,
}

/// Complete durable mining expansion state suitable for workflow checkpointing.
#[allow(missing_docs)]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MiningMission {
    pub version: u32,
    pub mission_id: String,
    pub mission_tag: String,
    /// Historical UUID-derived mission tags still recognized while queued
    /// prints and already-produced stock are migrated to the system tag.
    #[serde(default)]
    pub legacy_mission_tags: Vec<String>,
    pub phase: MissionPhase,
    pub selected_replicant: String,
    pub hub_location: String,
    pub sites: Vec<SiteMission>,
    pub routes: Vec<RouteMission>,
    pub print_batches: Vec<ExecutionPrintBatch>,
    pub site_print_requirements: QuantityMap,
    pub route_print_requirements: QuantityMap,
    pub total_material_cost: QuantityMap,
    pub warnings: Vec<String>,
}

/// Compact structured progress derived from a durable mining checkpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MiningProgress {
    /// Current top-level execution phase.
    pub phase: MissionPhase,
    /// Operational sites and total planned sites.
    pub sites: (usize, usize),
    /// Active routes and total planned routes.
    pub routes: (usize, usize),
    /// Produced devices and total planned print quantity.
    pub printing: (usize, usize),
}

impl MiningMission {
    /// Returns progress without consulting live managed state.
    #[must_use]
    pub fn progress(&self) -> MiningProgress {
        MiningProgress {
            phase: self.phase,
            sites: (
                self.sites
                    .iter()
                    .filter(|site| site.phase == SitePhase::Operational)
                    .count(),
                self.sites.len(),
            ),
            routes: (
                self.routes
                    .iter()
                    .filter(|route| route.phase == RoutePhase::Active)
                    .count(),
                self.routes.len(),
            ),
            printing: (
                self.print_batches
                    .iter()
                    .map(|batch| batch.produced_codes.len())
                    .sum(),
                self.print_batches
                    .iter()
                    .filter_map(|batch| usize::try_from(batch.quantity).ok())
                    .sum(),
            ),
        }
    }
}

/// Materializes mining sites, routes, and shared manufacturing as durable work items.
pub fn mining_work_item_specs(
    workflow_id: WorkflowId,
    mission: &MiningMission,
    region: &str,
) -> Result<Vec<WorkItemSpec>, replicant_workflow::RepositoryError> {
    let mut specs = Vec::new();
    let site_kind = WorkflowKind::new("mining.site")?;
    for (index, site) in mission.sites.iter().enumerate() {
        if site.phase == SitePhase::Operational {
            continue;
        }
        specs.push(WorkItemSpec {
            workflow_id,
            dedupe_key: format!("mining.site:{}", site.belt),
            kind: site_kind.clone(),
            sort_key: format!("1:{index:08}:{}", site.belt),
            payload_json: serde_json::json!({
                "type": "site",
                "index": index,
                "system": site.system,
                "belt": site.belt,
                "legacy_complete": site.phase == SitePhase::Operational,
            }),
            preconditions_json: serde_json::json!([{
                "kind": "mining.site_incomplete",
                "parameters": { "belt": site.belt }
            }]),
            requirements_json: serde_json::to_value(mining_site_item_requirements(region, site))?,
            deadline_at_ms: None,
        });
    }
    let route_kind = WorkflowKind::new("mining.route")?;
    for (index, route) in mission.routes.iter().enumerate() {
        specs.push(WorkItemSpec {
            workflow_id,
            dedupe_key: format!("mining.route:{}:{}", route.belt, mission.hub_location),
            kind: route_kind.clone(),
            sort_key: format!("2:{index:08}:{}", route.belt),
            payload_json: serde_json::json!({
                "type": "route",
                "index": index,
                "system": route.system,
                "belt": route.belt,
                "legacy_complete": route.phase == RoutePhase::Active,
            }),
            preconditions_json: serde_json::json!([{
                "kind": "mining.route_inactive",
                "parameters": { "belt": route.belt, "hub": mission.hub_location }
            }]),
            requirements_json: serde_json::to_value(mining_route_item_requirements(region))?,
            deadline_at_ms: None,
        });
    }
    if !mission.print_batches.is_empty() {
        let stage_kind = WorkflowKind::new("mining.stage")?;
        let mut requirements = vec![
            ResourceRequirement {
                key: "worker".into(),
                kind: "replicant".into(),
                capabilities: vec![OPERATIONAL_REGIONAL_WORKER_CAPABILITY.into()],
                scope: RequirementScope::Region(region.to_owned()),
                count: 1,
                quantity: 1,
            },
            ResourceRequirement {
                key: "autofactory".into(),
                kind: "autofactory".into(),
                capabilities: Vec::new(),
                scope: RequirementScope::Location(mission.hub_location.clone()),
                count: 1,
                quantity: 1,
            },
        ];
        requirements.extend(mission.total_material_cost.iter().filter_map(
            |(resource, quantity)| {
                let quantity = u64::try_from(*quantity).ok()?;
                (quantity != 0).then(|| ResourceRequirement {
                    key: format!("material:{resource}"),
                    kind: "material".into(),
                    capabilities: vec![resource.clone()],
                    scope: RequirementScope::Location(mission.hub_location.clone()),
                    count: 1,
                    quantity,
                })
            },
        ));
        specs.push(WorkItemSpec {
            workflow_id,
            dedupe_key: "mining.stage:manufacturing".into(),
            kind: stage_kind,
            sort_key: "0:manufacturing".into(),
            payload_json: serde_json::json!({
                "type": "stage",
                "index": 0,
                // This is part of the immutable work-item specification. Manufacturing
                // completion is derived from the checkpoint after reconciliation.
                "legacy_complete": false,
            }),
            preconditions_json: serde_json::json!([]),
            requirements_json: serde_json::to_value(requirements)?,
            deadline_at_ms: None,
        });
    }
    Ok(specs)
}

fn mining_site_positive_count(quantity: i64) -> Option<u32> {
    u32::try_from(quantity).ok().filter(|count| *count > 0)
}

fn mining_site_has_missing(missing: &QuantityMap, device_type: &str) -> bool {
    missing
        .get(device_type)
        .and_then(|quantity| mining_site_positive_count(*quantity))
        .is_some()
}

fn mining_site_transport_quantity(missing: &QuantityMap) -> u64 {
    missing
        .iter()
        .filter(|(device_type, _)| {
            matches!(
                device_type.as_str(),
                MINING_CONTROLLER
                    | MINING_DRONE
                    | SURVEY_CONTROLLER
                    | SURVEY_DRONE
                    | MAINTENANCE_DRONE
            )
        })
        .filter_map(|(_, quantity)| mining_site_positive_count(*quantity).map(u64::from))
        .fold(0, u64::saturating_add)
}

fn mining_site_item_requirements(region: &str, site: &SiteMission) -> Vec<ResourceRequirement> {
    let scope = || RequirementScope::Region(region.to_owned());
    let mut requirements = vec![ResourceRequirement {
        key: "worker".into(),
        kind: "replicant".into(),
        capabilities: vec![OPERATIONAL_REGIONAL_WORKER_CAPABILITY.into()],
        scope: scope(),
        count: 1,
        quantity: 1,
    }];
    for (device_type, quantity) in &site.missing {
        let Some(count) = mining_site_positive_count(*quantity) else {
            continue;
        };
        let key = match device_type.as_str() {
            MINING_CONTROLLER => "mining_controller",
            MINING_DRONE => "mining_drones",
            SURVEY_CONTROLLER => "survey_controller",
            SURVEY_DRONE => "survey_drones",
            MAINTENANCE_DRONE => "maintenance_drone",
            SYSTEM_WARD => "system_ward",
            _ => continue,
        };
        requirements.push(ResourceRequirement {
            key: key.into(),
            kind: "device".into(),
            capabilities: vec![device_type.clone()],
            scope: scope(),
            count,
            quantity: 1,
        });
    }
    let missing_devices = mining_site_transport_quantity(&site.missing);
    if missing_devices > 0 {
        requirements.push(ResourceRequirement {
            key: "carrier".into(),
            kind: "device".into(),
            capabilities: vec![SURGE_CARRIER.into()],
            scope: scope(),
            count: 1,
            quantity: 1,
        });
        requirements.push(ResourceRequirement {
            key: "attach".into(),
            kind: "attach".into(),
            capabilities: Vec::new(),
            scope: scope(),
            count: 1,
            quantity: missing_devices,
        });
    }
    requirements
}

fn mining_route_item_requirements(region: &str) -> Vec<ResourceRequirement> {
    let scope = || RequirementScope::Region(region.to_owned());
    vec![
        ResourceRequirement {
            key: "worker".into(),
            kind: "replicant".into(),
            capabilities: vec![OPERATIONAL_REGIONAL_WORKER_CAPABILITY.into()],
            scope: scope(),
            count: 1,
            quantity: 1,
        },
        ResourceRequirement {
            key: "transport_controller".into(),
            kind: "device".into(),
            capabilities: vec![TRANSPORT_CONTROLLER.into()],
            scope: scope(),
            count: 1,
            quantity: 1,
        },
        ResourceRequirement {
            key: "freighter".into(),
            kind: "device".into(),
            capabilities: vec![CARGO_FREIGHTER.into()],
            scope: scope(),
            count: 1,
            quantity: 1,
        },
    ]
}

/// Executes one isolated mining site, route, or manufacturing stage.
pub async fn execute_mining_item(
    client: &Client,
    mission: &MiningMission,
    item_type: &str,
    index: usize,
    allocations: &AllocationSet,
    wait_timeout: Duration,
) -> AnyResult<MiningMission> {
    let worker = mining_allocated_identity(allocations, "worker", "replicant")?;
    let mut lane = mission.clone();
    lane.selected_replicant.clone_from(&worker);
    match item_type {
        "site" => {
            let mut site = lane.sites.get(index).cloned().ok_or_else(|| {
                app_error(io::ErrorKind::InvalidInput, "mining site index is invalid")
            })?;
            let missing = site.missing.clone();
            if mining_site_has_missing(&missing, MINING_CONTROLLER) {
                site.assets.mining_controller = Some(mining_allocated_identity(
                    allocations,
                    "mining_controller",
                    "device",
                )?);
            }
            if mining_site_has_missing(&missing, MINING_DRONE) {
                site.assets
                    .mining_drones
                    .extend(mining_allocated_identities(
                        allocations,
                        "mining_drones",
                        "device",
                    )?);
                site.assets.mining_drones.sort();
                site.assets.mining_drones.dedup();
            }
            if mining_site_has_missing(&missing, SURVEY_CONTROLLER) {
                site.assets.survey_controller = Some(mining_allocated_identity(
                    allocations,
                    "survey_controller",
                    "device",
                )?);
            }
            if mining_site_has_missing(&missing, SURVEY_DRONE) {
                site.assets
                    .survey_drones
                    .extend(mining_allocated_identities(
                        allocations,
                        "survey_drones",
                        "device",
                    )?);
                site.assets.survey_drones.sort();
                site.assets.survey_drones.dedup();
            }
            if mining_site_has_missing(&missing, MAINTENANCE_DRONE) {
                site.assets.maintenance_drone = Some(mining_allocated_identity(
                    allocations,
                    "maintenance_drone",
                    "device",
                )?);
            }
            if mining_site_has_missing(&missing, SYSTEM_WARD) {
                // Keep the allocated ward reserved for the follow-up delivery.
                // The nine mining devices can deploy and begin configuring first.
                site.assets.system_ward = Some(mining_allocated_identity(
                    allocations,
                    "system_ward",
                    "device",
                )?);
            }
            if mining_site_transport_quantity(&missing) > 0 {
                site.carrier = Some(mining_allocated_identity(allocations, "carrier", "device")?);
                mining_validate_carrier_capacity_owner(allocations)?;
            }
            site.missing
                .retain(|device_type, _| device_type == SYSTEM_WARD);
            lane.sites = vec![site];
            lane.routes.clear();
            lane.print_batches
                .retain(|batch| batch.purpose == PrintPurpose::Site);
        }
        "route" => {
            let mut route = lane.routes.get(index).cloned().ok_or_else(|| {
                app_error(io::ErrorKind::InvalidInput, "mining route index is invalid")
            })?;
            route.controller = Some(mining_allocated_identity(
                allocations,
                "transport_controller",
                "device",
            )?);
            route.freighter = Some(mining_allocated_identity(
                allocations,
                "freighter",
                "device",
            )?);
            lane.routes = vec![route];
            lane.sites.clear();
            lane.print_batches
                .retain(|batch| batch.purpose == PrintPurpose::Route);
        }
        "stage" => {
            let _ = mining_allocated_identity(allocations, "autofactory", "autofactory")?;
            for (resource, quantity) in &mission.total_material_cost {
                if *quantity > 0 {
                    let key = format!("material:{resource}");
                    let allocated = allocations
                        .by_requirement
                        .get(&key)
                        .into_iter()
                        .flatten()
                        .map(|allocation| allocation.quantity)
                        .sum::<u64>();
                    if allocated < u64::try_from(*quantity).unwrap_or(u64::MAX) {
                        return Err(app_error(
                            io::ErrorKind::WouldBlock,
                            format!("mining stage allocation omitted material {resource}"),
                        ));
                    }
                }
            }
            lane.sites.clear();
            lane.routes.clear();
        }
        _ => {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("unknown mining item type {item_type}"),
            ));
        }
    }
    let plan_path = std::env::temp_dir().join(format!(
        "replicant-mining-item-{}-{item_type}-{index}.json",
        lane.mission_id
    ));
    save_plan(&plan_path, &lane)?;
    let request = MiningExpansionRequest {
        systems: lane
            .sites
            .iter()
            .map(|site| site.system.clone())
            .chain(lane.routes.iter().map(|route| route.system.clone()))
            .collect(),
        replicant: worker,
        hub: lane.hub_location.clone(),
        transport_routes: Vec::new(),
        mission_file: plan_path.clone(),
        wait_timeout,
        max_concurrency: 1,
    };
    let result = execute_expansion(client, &request).await;
    let final_state = load_expansion(&plan_path);
    let _ = fs::remove_file(plan_path);
    result?;
    final_state
}

fn mining_allocated_identity(
    allocations: &AllocationSet,
    requirement: &str,
    expected: &str,
) -> AnyResult<String> {
    mining_allocated_identities(allocations, requirement, expected)?
        .into_iter()
        .next()
        .ok_or_else(|| app_error(io::ErrorKind::InvalidData, "empty mining allocation"))
}

fn mining_allocated_identities(
    allocations: &AllocationSet,
    requirement: &str,
    expected: &str,
) -> AnyResult<Vec<String>> {
    allocations
        .by_requirement
        .get(requirement)
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("mining item allocation omitted {requirement}"),
            )
        })?
        .iter()
        .map(|allocation| match (&allocation.resource, expected) {
            (ResourceKey::Replicant(code), "replicant")
            | (ResourceKey::Device(code), "device")
            | (ResourceKey::Autofactory(code), "autofactory") => Ok(code.clone()),
            _ => Err(app_error(
                io::ErrorKind::InvalidData,
                format!("mining allocation {requirement} has the wrong resource kind"),
            )),
        })
        .collect()
}

fn mining_validate_carrier_capacity_owner(allocations: &AllocationSet) -> AnyResult<()> {
    let carrier = mining_allocated_identity(allocations, "carrier", "device")?;
    let matching = ["attach", "stow"].into_iter().any(|requirement| {
        allocations
            .by_requirement
            .get(requirement)
            .into_iter()
            .flatten()
            .any(|allocation| {
                matches!(
                    &allocation.resource,
                    ResourceKey::Namespaced { namespace, key }
                        if matches!(
                            (requirement, namespace.as_str()),
                            ("attach", "attach") | ("stow", "attach" | "stow")
                        ) && key == &carrier
                )
            })
    });
    if !matching {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("mining item attachment-capacity allocation does not belong to {carrier}"),
        ));
    }
    Ok(())
}

/// Merges one terminal mining item checkpoint into campaign state.
pub fn merge_mining_item_state(
    mission: &mut MiningMission,
    lane: &MiningMission,
    item_type: &str,
    index: usize,
) {
    match item_type {
        "site" => {
            if let (Some(target), Some(source)) = (mission.sites.get_mut(index), lane.sites.first())
            {
                target.clone_from(source);
            }
        }
        "route" => {
            if let (Some(target), Some(source)) =
                (mission.routes.get_mut(index), lane.routes.first())
            {
                target.clone_from(source);
            }
        }
        "stage" => mission.print_batches.clone_from(&lane.print_batches),
        _ => {}
    }
    if mission
        .sites
        .iter()
        .all(|site| site.phase == SitePhase::Operational)
        && mission
            .routes
            .iter()
            .all(|route| route.phase == RoutePhase::Active)
    {
        mission.phase = MissionPhase::Completed;
    }
}

/// Returns whether an isolated mining item reached its domain terminal state.
#[must_use]
pub fn mining_item_completed(mission: &MiningMission, item_type: &str) -> bool {
    match item_type {
        "site" => mission
            .sites
            .first()
            .is_some_and(|site| site.phase == SitePhase::Operational),
        "route" => mission
            .routes
            .first()
            .is_some_and(|route| route.phase == RoutePhase::Active),
        "stage" => mission.print_batches.iter().all(|batch| {
            usize::try_from(batch.quantity)
                .is_ok_and(|quantity| batch.produced_codes.len() >= quantity)
        }),
        _ => false,
    }
}

struct MissionLock {
    path: PathBuf,
}

impl MissionLock {
    fn acquire(mission_path: &Path) -> AnyResult<Self> {
        let lock_path = mission_path.with_extension("lock");
        if let Some(parent) = lock_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)?;
        }
        for attempt in 0..2 {
            match OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&lock_path)
            {
                Ok(mut file) => {
                    writeln!(file, "{}", std::process::id())?;
                    file.sync_all()?;
                    return Ok(Self { path: lock_path });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists && attempt == 0 => {
                    let owner = fs::read_to_string(&lock_path)
                        .ok()
                        .and_then(|value| value.trim().parse::<u32>().ok());
                    let owner_is_running =
                        owner.is_some_and(|pid| PathBuf::from(format!("/proc/{pid}")).exists());
                    if owner_is_running {
                        return Err(app_error(
                            io::ErrorKind::WouldBlock,
                            format!(
                                "another mining executor holds {} (pid {})",
                                lock_path.display(),
                                owner.unwrap_or_default()
                            ),
                        ));
                    }
                    fs::remove_file(&lock_path)?;
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(app_error(
            io::ErrorKind::WouldBlock,
            format!("could not acquire {}", lock_path.display()),
        ))
    }
}

impl Drop for MissionLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

async fn create_plan(client: &Client, config: &Config) -> AnyResult<MiningMission> {
    if config.plan_path.exists() && !config.replace_plan {
        let existing = load_expansion(&config.plan_path)?;
        if !existing.phase.is_terminal() {
            return Err(app_error(
                io::ErrorKind::AlreadyExists,
                format!(
                    "incomplete mission {} exists at {}; use `run`, `status`, or plan --replace-plan",
                    existing.mission_id,
                    config.plan_path.display()
                ),
            ));
        }
    }

    info!("planning mining expansion from committed managed state");
    let selected_replicant = select_replicant(client, config.replicant.as_deref()).await?;
    let mut systems = config.requested_systems()?;
    let transport_routes =
        validate_transport_routes(client, &systems, &config.hub, &config.transport_routes).await?;
    let explicit_routes = transport_routes
        .iter()
        .map(|route| (route.system.as_str(), route))
        .collect::<BTreeMap<_, _>>();
    let devices = device_snapshots(client).await?;
    let catalogue = client.galaxy().catalogue();
    sort_systems_by_hub_distance(&mut systems, &config.hub, &catalogue);
    let blueprints = fetch_blueprints(client).await?;
    let factories = factory_workloads(client, &blueprints, &config.hub).await?;

    let mut sites = Vec::new();
    for system in systems {
        let belt = match explicit_routes.get(system.as_str()) {
            Some(route) => selected_belt_for_route(client, route).await?,
            None => select_belt(client, &system, &devices).await?,
        };
        let audit = audit_site(&devices, &system, &belt.designation);
        let missing = site_shortages(&audit);
        sites.push(SiteMission {
            system: system.clone(),
            belt: belt.designation,
            density: belt.density,
            tag: site_tag(&system),
            phase: if audit.operational {
                SitePhase::Operational
            } else {
                SitePhase::Planned
            },
            assets: audit.assets,
            missing,
            carrier: None,
        });
    }

    let mission_tag = mining_mission_tag(&config.hub);
    let mut site_required = QuantityMap::new();
    for site in &sites {
        add_quantities(&mut site_required, &site.missing);
    }
    let reusable_site = reusable_counts(&devices, &config.hub, &mission_tag, true);
    let site_print_requirements = shortages(&site_required, &reusable_site);

    let mut routes = Vec::new();
    for site in &sites {
        if site.belt == config.hub {
            continue;
        }
        let audit = transport_service_present(&devices, &site.system, &site.belt, &config.hub);
        routes.push(RouteMission {
            system: site.system.clone(),
            belt: site.belt.clone(),
            tag: site.tag.clone(),
            phase: if audit.state == EvidenceState::Present {
                RoutePhase::Active
            } else {
                RoutePhase::Planned
            },
            controller: audit.controller,
            freighter: audit.freighter,
        });
    }
    let missing_routes = i64::try_from(
        routes
            .iter()
            .filter(|route| route.phase != RoutePhase::Active)
            .count(),
    )?;
    let route_required = [
        (TRANSPORT_CONTROLLER.to_owned(), missing_routes),
        (CARGO_FREIGHTER.to_owned(), missing_routes),
    ]
    .into_iter()
    .collect();
    let reusable_route = reusable_counts(&devices, &config.hub, &mission_tag, false);
    let route_print_requirements = shortages(&route_required, &reusable_route);

    let site_schedule = schedule_prints(&site_print_requirements, &blueprints, &factories)?;
    let route_factories =
        site_schedule
            .batches
            .iter()
            .fold(factories.clone(), |mut factories, batch| {
                if let Some(factory) = factories
                    .iter_mut()
                    .find(|factory| factory.code == batch.factory_code)
                {
                    factory.remaining_seconds = batch.projected_finish_seconds;
                }
                factories
            });
    let route_schedule = schedule_prints(&route_print_requirements, &blueprints, &route_factories)?;
    let mission_id = uuid::Uuid::new_v4().simple().to_string();
    let mut print_batches =
        execution_batches(&mission_id, PrintPurpose::Site, &site_schedule.batches);
    print_batches.extend(execution_batches(
        &mission_id,
        PrintPurpose::Route,
        &route_schedule.batches,
    ));
    let mut total_material_cost = QuantityMap::new();
    for (device_type, quantity) in site_print_requirements
        .iter()
        .chain(&route_print_requirements)
    {
        add_quantities(
            &mut total_material_cost,
            &blueprint_resource_cost(device_type, *quantity, &blueprints)?,
        );
    }

    let mission = MiningMission {
        version: PLAN_VERSION,
        mission_id,
        mission_tag,
        legacy_mission_tags: Vec::new(),
        phase: MissionPhase::Planned,
        selected_replicant,
        hub_location: config.hub.clone(),
        sites,
        routes,
        print_batches,
        site_print_requirements,
        route_print_requirements,
        total_material_cost,
        warnings: Vec::new(),
    };
    save_plan(&config.plan_path, &mission)?;
    Ok(mission)
}

async fn validate_transport_routes(
    client: &Client,
    systems: &[String],
    hub: &str,
    routes: &[AmiTransportRouteIntent],
) -> AnyResult<Vec<AmiTransportRouteIntent>> {
    let systems = systems.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let mut normalized = Vec::with_capacity(routes.len());
    let mut pairs = BTreeSet::new();
    let mut systems_seen = BTreeSet::new();
    for route in routes {
        let route = AmiTransportRouteIntent {
            system: route.system.trim().to_ascii_uppercase(),
            collect: route.collect.trim().to_ascii_uppercase(),
            deliver: route.deliver.trim().to_ascii_uppercase(),
        };
        if route.system.is_empty() || route.collect.is_empty() || route.deliver.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "transport route fields must be nonblank",
            ));
        }
        if !systems.contains(route.system.as_str()) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("transport route system {} is not in systems", route.system),
            ));
        }
        if route.deliver != hub {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("transport route delivery must equal hub {hub}"),
            ));
        }
        if !pairs.insert((route.collect.clone(), route.deliver.clone())) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "duplicate transport route {} -> {}",
                    route.collect, route.deliver
                ),
            ));
        }
        if !systems_seen.insert(route.system.clone()) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "at most one explicit transport route is allowed for {}",
                    route.system
                ),
            ));
        }
        let location = match client.locations().cached(&route.system) {
            Some(location) => location,
            None => {
                validation::location(
                    client,
                    &route.system,
                    validation::ValidationReason::Mutation,
                )
                .await?
            }
        };
        if !belts_from_location(&location)
            .iter()
            .any(|belt| belt.designation == route.collect)
        {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "transport route collect location {} is not a discovered belt in {}",
                    route.collect, route.system
                ),
            ));
        }
        normalized.push(route);
    }
    Ok(normalized)
}

fn sort_systems_by_hub_distance(systems: &mut [String], hub: &str, catalogue: &[Star]) {
    let positions = catalogue
        .iter()
        .filter_map(|star| {
            star.position
                .map(|position| (star.key.id.as_str(), position))
        })
        .collect::<BTreeMap<_, _>>();
    let hub_position = catalogue
        .iter()
        .filter(|star| location_is_in_system(hub, star.key.id.as_str()))
        .filter_map(|star| star.position)
        .next();
    systems.sort_by(|left, right| {
        let distance = |system: &str| {
            let position = positions.get(system)?;
            let hub = hub_position?;
            Some(
                (position.x - hub.x).powi(2)
                    + (position.y - hub.y).powi(2)
                    + (position.z - hub.z).powi(2),
            )
        };
        match (distance(left), distance(right)) {
            (Some(left_distance), Some(right_distance)) => left_distance
                .total_cmp(&right_distance)
                .then_with(|| left.cmp(right)),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => left.cmp(right),
        }
    });
}

fn location_is_in_system(location: &str, system: &str) -> bool {
    location == system
        || location
            .strip_prefix(system)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

fn execution_batches(
    mission_id: &str,
    purpose: PrintPurpose,
    batches: &[PrintBatch],
) -> Vec<ExecutionPrintBatch> {
    batches
        .iter()
        .flat_map(|batch| {
            (0..batch.quantity).map(move |unit_index| ExecutionPrintBatch {
                purpose,
                factory_code: batch.factory_code.clone(),
                device_type: batch.device_type.clone(),
                quantity: 1,
                projected_finish_seconds: batch.projected_finish_seconds,
                batch_tag: format!(
                    "mine-b:{:016x}",
                    stable_hash(&format!(
                        "{mission_id}:{purpose:?}:{}:{}:{}:{unit_index}",
                        batch.factory_code, batch.sequence, batch.device_type
                    ))
                ),
                submission_started: false,
                submitted: false,
                operation_id: None,
                produced_codes: Vec::new(),
            })
        })
        .collect()
}

const MINING_MISSION_TAG_PREFIX: &str = "mine-m:";
const MAX_DEVICE_TAG_CHARS: usize = 32;

/// Returns the stable mining reservation tag for a manufacturing hub system.
pub(crate) fn mining_mission_tag(hub_location: &str) -> String {
    let system = hub_location
        .split('-')
        .next()
        .filter(|system| !system.is_empty())
        .unwrap_or(hub_location);
    bounded_system_tag(MINING_MISSION_TAG_PREFIX, system)
}

/// Upgrades an in-memory legacy mining checkpoint while retaining old aliases.
pub(crate) fn migrate_mission_tag_metadata(mission: &mut MiningMission) -> bool {
    let desired = mining_mission_tag(&mission.hub_location);
    let mut changed = false;
    if mission.mission_tag != desired {
        let previous = std::mem::replace(&mut mission.mission_tag, desired.clone());
        if previous.starts_with(MINING_MISSION_TAG_PREFIX)
            && !mission.legacy_mission_tags.contains(&previous)
        {
            mission.legacy_mission_tags.push(previous);
        }
        changed = true;
    }
    let before = mission.legacy_mission_tags.len();
    mission
        .legacy_mission_tags
        .retain(|tag| tag.starts_with(MINING_MISSION_TAG_PREFIX) && tag != &desired);
    mission.legacy_mission_tags.sort();
    mission.legacy_mission_tags.dedup();
    changed || mission.legacy_mission_tags.len() != before
}

/// Returns whether a mining mission tag uses the old 16-hex hash identity.
pub(crate) fn is_opaque_mining_mission_tag(tag: &str) -> bool {
    tag.strip_prefix(MINING_MISSION_TAG_PREFIX)
        .is_some_and(|suffix| {
            suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

fn bounded_system_tag(prefix: &str, system: &str) -> String {
    const HASH_CHARS: usize = 12;
    let normalized = system
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    let normalized = normalized.trim_matches('-');
    let direct = format!("{prefix}{normalized}");
    if direct.chars().count() <= MAX_DEVICE_TAG_CHARS {
        return direct;
    }

    let fixed = prefix.chars().count() + 1 + HASH_CHARS;
    let head_budget = MAX_DEVICE_TAG_CHARS.saturating_sub(fixed).max(1);
    let mut head = normalized.chars().take(head_budget).collect::<String>();
    head = head.trim_end_matches('-').to_owned();
    if head.is_empty() {
        head.push('s');
    }
    let hash = stable_hash(normalized) & 0x0000_ffff_ffff_ffff;
    format!("{prefix}{head}-{hash:012x}")
}

fn stable_hash(value: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

pub(crate) async fn device_snapshots(client: &Client) -> AnyResult<Vec<Device>> {
    let handles = client.devices().find().owned().collect().await?;
    let mut devices = Vec::with_capacity(handles.len());
    for handle in handles {
        devices.push(handle.snapshot().await?);
    }
    devices.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(devices)
}

async fn select_replicant(client: &Client, requested: Option<&str>) -> AnyResult<String> {
    let requested = requested.ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidInput,
            "mining expansion requires a replicant name or code",
        )
    })?;
    let mut matches = client
        .state()
        .owned_replicants()?
        .into_iter()
        .filter(|replicant| {
            replicant.key.id.as_str().eq_ignore_ascii_case(requested)
                || replicant
                    .name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(requested))
        })
        .collect::<Vec<_>>();
    match matches.len() {
        1 => Ok(matches.remove(0).key.id.as_str().to_owned()),
        0 => Err(app_error(
            io::ErrorKind::NotFound,
            format!("no owned replicant matches {requested:?}"),
        )),
        _ => Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("replicant name {requested:?} is ambiguous; use its code"),
        )),
    }
}

#[derive(Clone, Debug)]
struct SelectedBelt {
    designation: String,
    density: String,
}

async fn select_belt(client: &Client, system: &str, devices: &[Device]) -> AnyResult<SelectedBelt> {
    let location = match client.locations().cached(system) {
        Some(location) => location,
        None => {
            validation::location(client, system, validation::ValidationReason::Mutation).await?
        }
    };
    preferred_belt(belts_from_location(&location), devices, system)
}

fn preferred_belt(
    mut belts: Vec<SelectedBelt>,
    devices: &[Device],
    system: &str,
) -> AnyResult<SelectedBelt> {
    belts.sort_by(|left, right| {
        managed_belt_asset_count(devices, &right.designation)
            .cmp(&managed_belt_asset_count(devices, &left.designation))
            .then_with(|| density_rank(&right.density).cmp(&density_rank(&left.density)))
            .then_with(|| left.designation.cmp(&right.designation))
    });
    belts.into_iter().next().ok_or_else(|| {
        app_error(
            io::ErrorKind::NotFound,
            format!("system {system} has no discovered asteroid belt"),
        )
    })
}

async fn selected_belt_for_route(
    client: &Client,
    route: &AmiTransportRouteIntent,
) -> AnyResult<SelectedBelt> {
    let location = match client.locations().cached(&route.system) {
        Some(location) => location,
        None => {
            validation::location(
                client,
                &route.system,
                validation::ValidationReason::Mutation,
            )
            .await?
        }
    };
    belts_from_location(&location)
        .into_iter()
        .find(|belt| belt.designation == route.collect)
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "transport route collect location {} is not a discovered belt in {}",
                    route.collect, route.system
                ),
            )
        })
}

/// Determines whether positive belt output is currently serviceable.
///
/// Positive location stock alone is insufficient: the location must be an
/// exact discovered belt with a complete system mapping and operational mining
/// installation.
fn positive_location_stock(inventories: &[Inventory], location: &str) -> bool {
    inventories.iter().any(|inventory| {
        matches!(&inventory.owner, InventoryOwner::Location(key) if key.id.as_str() == location)
            && inventory.items.iter().any(|item| item.quantity > 0)
    })
}

pub(crate) fn resource_present(
    devices: &[Device],
    locations: &[Location],
    inventories: &[Inventory],
    location: &str,
) -> EvidenceState {
    let positive_stock = positive_location_stock(inventories, location);
    if !positive_stock {
        return EvidenceState::Absent;
    }
    let Some(location_record) = locations
        .iter()
        .find(|record| record.key.id.as_str() == location)
    else {
        return EvidenceState::Unknown;
    };
    match location_record.location_type.as_ref() {
        None => return EvidenceState::Unknown,
        Some(LocationType::Belt) => {}
        Some(_) => return EvidenceState::Absent,
    }
    let Some(system) = location_record.system.as_deref() else {
        return EvidenceState::Unknown;
    };
    mining_site_evidence_state(devices, system, location)
}

fn mining_site_evidence_state(devices: &[Device], system: &str, belt: &str) -> EvidenceState {
    let audit = audit_site(devices, system, belt);
    if audit.operational {
        return EvidenceState::Present;
    }
    if devices
        .iter()
        .any(|device| device_location(device) == Some(belt) && device.device_type.is_none())
    {
        return EvidenceState::Unknown;
    }
    for code in [
        audit.assets.mining_controller.as_deref(),
        audit.assets.survey_controller.as_deref(),
        audit.assets.maintenance_drone.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        let Some(device) = find_device(devices, code) else {
            return EvidenceState::Unknown;
        };
        let Some(status) = device.status.as_ref().map(DeviceStatus::as_str) else {
            return EvidenceState::Unknown;
        };
        if !documented_transport_status(status)
            && !matches!(status, "active" | "deactivated" | "offline")
        {
            return EvidenceState::Unknown;
        }
        if device
            .active_directive
            .as_ref()
            .is_some_and(|active| active.directive.is_none() || active.status.as_deref().is_none())
        {
            return EvidenceState::Unknown;
        }
    }
    EvidenceState::Absent
}

/// Applies the same resource predicate when callers can report census
/// completeness separately. Positive stock with incomplete authority is never
/// treated as absent.
pub(crate) fn resource_present_with_authority(
    devices: &[Device],
    locations: &[Location],
    inventories: &[Inventory],
    location: &str,
    complete_authority: bool,
) -> EvidenceState {
    let positive_stock = positive_location_stock(inventories, location);
    if positive_stock && !complete_authority {
        EvidenceState::Unknown
    } else {
        resource_present(devices, locations, inventories, location)
    }
}

fn managed_belt_asset_count(devices: &[Device], belt: &str) -> usize {
    devices
        .iter()
        .filter(|device| device_location(device) == Some(belt))
        .filter(|device| {
            matches!(
                device_type(device),
                Some(
                    MINING_CONTROLLER
                        | MINING_DRONE
                        | SURVEY_CONTROLLER
                        | SURVEY_DRONE
                        | MAINTENANCE_DRONE
                )
            )
        })
        .count()
}

fn belts_from_location(location: &Location) -> Vec<SelectedBelt> {
    let Some(asteroid_belt) = location.unknown.get("asteroid_belt") else {
        return Vec::new();
    };
    asteroid_belt
        .get("belts")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(asteroid_belt))
        .iter()
        .filter_map(|value| {
            let object = value.as_object()?;
            Some(SelectedBelt {
                designation: object.get("designation")?.as_str()?.to_owned(),
                density: object
                    .get("density")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_owned(),
            })
        })
        .collect()
}

fn density_rank(density: &str) -> u8 {
    match density.to_ascii_lowercase().as_str() {
        "dense" => 3,
        "moderate" => 2,
        "sparse" => 1,
        _ => 0,
    }
}

pub(crate) struct SiteAudit {
    pub(crate) assets: SiteAssets,
    pub(crate) operational: bool,
}

pub(crate) fn audit_site(devices: &[Device], system: &str, belt: &str) -> SiteAudit {
    let at_belt = devices
        .iter()
        .filter(|device| device_location(device) == Some(belt))
        .collect::<Vec<_>>();
    let mining_controller = select_controller(&at_belt, MINING_CONTROLLER, MINING_DRONE);
    let survey_controller = select_controller(&at_belt, SURVEY_CONTROLLER, SURVEY_DRONE);
    let mut mining_drones = children_of(devices, mining_controller.as_deref(), MINING_DRONE);
    let mut survey_drones = children_of(devices, survey_controller.as_deref(), SURVEY_DRONE);
    fill_free_devices(&at_belt, MINING_DRONE, 4, &mut mining_drones);
    fill_free_devices(&at_belt, SURVEY_DRONE, 2, &mut survey_drones);
    let maintenance = at_belt
        .iter()
        .filter(|device| device_type(device) == Some(MAINTENANCE_DRONE))
        .min_by(|left, right| {
            (!has_directive(left, "patrol"))
                .cmp(&!has_directive(right, "patrol"))
                .then_with(|| left.key.id.as_str().cmp(right.key.id.as_str()))
        })
        .map(|device| device.key.id.as_str().to_owned());
    let system_ward = devices
        .iter()
        .filter(|device| device_type(device) == Some(SYSTEM_WARD))
        .filter(|device| device_is_in_system(device, system))
        .min_by_key(|device| device.key.id.as_str())
        .map(|device| device.key.id.as_str().to_owned());
    let assets = SiteAssets {
        mining_controller,
        mining_drones,
        survey_controller,
        survey_drones,
        maintenance_drone: maintenance,
        system_ward,
    };
    // Protection is intentionally outside operational readiness: the Director
    // backfills or relocates System Wards after the productive stack is online.
    let operational = assets
        .mining_controller
        .as_deref()
        .and_then(|code| find_device(devices, code))
        .is_some_and(|device| {
            has_directive(device, "deplete_smallest")
                && device
                    .status
                    .as_ref()
                    .is_some_and(|status| status.as_str() == "coordinating")
        })
        && assets.mining_drones.len() >= 4
        && assets
            .survey_controller
            .as_deref()
            .and_then(|code| find_device(devices, code))
            .is_some_and(|device| {
                has_directive(device, "belt_search")
                    && device
                        .status
                        .as_ref()
                        .is_some_and(|status| status.as_str() == "coordinating")
            })
        && assets.survey_drones.len() >= 2
        && assets
            .maintenance_drone
            .as_deref()
            .and_then(|code| find_device(devices, code))
            .is_some_and(|device| has_directive(device, "patrol"))
        && adopted_count(devices, assets.mining_controller.as_deref(), MINING_DRONE) >= 4
        && adopted_count(devices, assets.survey_controller.as_deref(), SURVEY_DRONE) >= 2;
    SiteAudit {
        assets,
        operational,
    }
}

fn site_shortages(audit: &SiteAudit) -> QuantityMap {
    // System Wards harden a mining system, but they are deliberately not part
    // of the initial mining payload. Getting the controller/drone stack online
    // produces resources sooner; the Director can backfill or relocate a ward
    // after the site is operational when protection is available.
    shortages(&mining_site_requirements(), &audit.assets.counts())
}

fn device_is_in_system(device: &Device, system: &str) -> bool {
    device_location(device).is_some_and(|location| location_is_in_system(location, system))
}

fn select_controller(
    devices: &[&Device],
    controller_type: &str,
    child_type: &str,
) -> Option<String> {
    devices
        .iter()
        .filter(|device| device_type(device) == Some(controller_type))
        .max_by(|left, right| {
            let left_count = devices
                .iter()
                .filter(|device| {
                    device_type(device) == Some(child_type)
                        && controller_code(device) == Some(left.key.id.as_str())
                })
                .count();
            let right_count = devices
                .iter()
                .filter(|device| {
                    device_type(device) == Some(child_type)
                        && controller_code(device) == Some(right.key.id.as_str())
                })
                .count();
            left_count
                .cmp(&right_count)
                .then_with(|| right.key.id.as_str().cmp(left.key.id.as_str()))
        })
        .map(|device| device.key.id.as_str().to_owned())
}

fn children_of(devices: &[Device], controller: Option<&str>, child_type: &str) -> Vec<String> {
    let Some(controller) = controller else {
        return Vec::new();
    };
    devices
        .iter()
        .filter(|device| {
            device_type(device) == Some(child_type) && controller_code(device) == Some(controller)
        })
        .map(|device| device.key.id.as_str().to_owned())
        .collect()
}

fn fill_free_devices(
    devices: &[&Device],
    device_type_name: &str,
    minimum: usize,
    selected: &mut Vec<String>,
) {
    for device in devices {
        if selected.len() >= minimum {
            break;
        }
        let code = device.key.id.as_str();
        if device_type(device) == Some(device_type_name)
            && controller_code(device).is_none()
            && !selected.iter().any(|selected| selected == code)
        {
            selected.push(code.to_owned());
        }
    }
    selected.sort();
    selected.dedup();
}

fn adopted_count(devices: &[Device], controller: Option<&str>, child_type: &str) -> usize {
    children_of(devices, controller, child_type).len()
}

fn documented_transport_status(status: &str) -> bool {
    const DOCUMENTED: [&str; 27] = [
        "stowed",
        "idle",
        "travelling",
        "cruising",
        "surging",
        "recalling",
        "recall_waiting",
        "decommissioning",
        "collecting",
        "depositing",
        "waiting_for_surge_plate",
        "prospecting",
        "tracking",
        "scanning",
        "monitoring",
        "printing",
        "waiting_for_resources",
        "repairing",
        "diverting",
        "patrolling",
        "coordinating",
        "relaying",
        "inactive",
        "compacting",
        "compacted",
        "unfurling",
        "mining",
    ];
    DOCUMENTED.iter().any(|documented| {
        status == *documented
            || status
                .strip_prefix(documented)
                .is_some_and(|suffix| suffix.starts_with(" ("))
    })
}

fn transport_status_state(device: &Device) -> EvidenceState {
    const USABLE: [&str; 9] = [
        "idle",
        "travelling",
        "cruising",
        "surging",
        "recalling",
        "recall_waiting",
        "collecting",
        "depositing",
        "waiting_for_surge_plate",
    ];
    match device.status.as_ref().map(DeviceStatus::as_str) {
        None => EvidenceState::Unknown,
        Some(status) if USABLE.contains(&status) => EvidenceState::Present,
        Some(status) if documented_transport_status(status) => EvidenceState::Absent,
        Some(_) => EvidenceState::Unknown,
    }
}

fn transport_controller_status_state(device: &Device) -> EvidenceState {
    match device.status.as_ref().map(DeviceStatus::as_str) {
        None => EvidenceState::Unknown,
        Some("coordinating") => EvidenceState::Present,
        Some(status) if documented_transport_status(status) => EvidenceState::Absent,
        Some(_) => EvidenceState::Unknown,
    }
}

/// Audits exact owned AMI transport coverage, retaining reusable identities.
pub(crate) fn transport_service_present(
    devices: &[Device],
    system: &str,
    collect: &str,
    deliver: &str,
) -> TransportServiceAudit {
    if collect == deliver {
        return TransportServiceAudit {
            state: EvidenceState::Present,
            controller: None,
            freighter: None,
        };
    }
    let expected = if location_is_in_system(deliver, system) {
        "shuttle"
    } else {
        "ferry"
    };
    let mut saw_unknown = false;
    let mut saw_controller = None;
    for controller in devices.iter().filter(|device| {
        device.access == replicant_client::domain::AccessScope::Owned
            && device_type(device) == Some(TRANSPORT_CONTROLLER)
    }) {
        let controller_status = transport_controller_status_state(controller);
        if controller_status == EvidenceState::Unknown {
            saw_unknown = true;
            continue;
        }
        if controller_status != EvidenceState::Present {
            continue;
        }
        let Some(active) = controller.active_directive.as_ref() else {
            continue;
        };
        let Some(directive) = active.directive.as_ref().map(|value| value.as_str()) else {
            saw_unknown = true;
            continue;
        };
        if directive != expected {
            continue;
        }
        match active.status.as_deref() {
            Some("active") => {}
            Some("paused" | "completed" | "cleared") => continue,
            Some(_) | None => {
                saw_unknown = true;
                continue;
            }
        }
        let Some(config) = active.details.get("config").and_then(Value::as_object) else {
            saw_unknown = true;
            continue;
        };
        if config.get("collect").and_then(Value::as_str) != Some(collect)
            || config.get("deliver").and_then(Value::as_str) != Some(deliver)
        {
            continue;
        }
        saw_controller = Some(controller.key.id.as_str().to_owned());
        let mut matching_freighter = false;
        for freighter in devices.iter().filter(|device| {
            device.access == replicant_client::domain::AccessScope::Owned
                && device_type(device) == Some(CARGO_FREIGHTER)
                && controller_code(device) == Some(controller.key.id.as_str())
        }) {
            matching_freighter = true;
            match transport_status_state(freighter) {
                EvidenceState::Present => {
                    return TransportServiceAudit {
                        state: EvidenceState::Present,
                        controller: saw_controller,
                        freighter: Some(freighter.key.id.as_str().to_owned()),
                    };
                }
                EvidenceState::Unknown => saw_unknown = true,
                EvidenceState::Absent => {}
            }
        }
        if !matching_freighter {
            continue;
        }
    }
    TransportServiceAudit {
        state: if saw_unknown {
            EvidenceState::Unknown
        } else {
            EvidenceState::Absent
        },
        controller: saw_controller,
        freighter: None,
    }
}

fn reusable_counts(
    devices: &[Device],
    hub: &str,
    mission_tag: &str,
    site_devices: bool,
) -> QuantityMap {
    let allowed: BTreeSet<&str> = if site_devices {
        [
            MINING_CONTROLLER,
            MINING_DRONE,
            SURVEY_CONTROLLER,
            SURVEY_DRONE,
            MAINTENANCE_DRONE,
            SYSTEM_WARD,
        ]
        .into_iter()
        .collect()
    } else {
        [TRANSPORT_CONTROLLER, CARGO_FREIGHTER]
            .into_iter()
            .collect()
    };
    let mut counts = QuantityMap::new();
    for device in devices.iter().filter(|device| {
        device_location(device) == Some(hub)
            && device
                .device_type
                .as_ref()
                .is_some_and(|value| allowed.contains(value.as_str()))
            && device
                .status
                .as_ref()
                .is_some_and(|value| value.as_str() == "idle")
            && device.relationships.controller.is_none()
            && device.relationships.attached_to.is_none()
            && device.relationships.stowed_in.is_none()
            && device.travel.is_none()
            && (device.tags.iter().any(|tag| tag == mission_tag) || !has_reservation_tag(device))
    }) {
        if let Some(device_type) = &device.device_type {
            *counts.entry(device_type.as_str().to_owned()).or_default() += 1;
        }
    }
    counts
}

fn has_reservation_tag(device: &Device) -> bool {
    device.tags.iter().any(|tag| {
        tag.starts_with("evt-")
            || tag.starts_with("evt_")
            || tag.starts_with("mine-")
            || tag.starts_with("relay-")
    })
}

fn device_type(device: &Device) -> Option<&str> {
    device.device_type.as_ref().map(DeviceType::as_str)
}

fn device_location(device: &Device) -> Option<&str> {
    device
        .location
        .as_ref()
        .map(|location| location.id.as_str())
}

fn controller_code(device: &Device) -> Option<&str> {
    device
        .relationships
        .controller
        .as_ref()
        .map(|controller| controller.id.as_str())
}

fn has_directive(device: &Device, directive_name: &str) -> bool {
    device
        .active_directive
        .as_ref()
        .and_then(|active| active.directive.as_ref())
        .is_some_and(|directive| directive.as_str() == directive_name)
}

fn find_device<'a>(devices: &'a [Device], code: &str) -> Option<&'a Device> {
    devices.iter().find(|device| device.key.id.as_str() == code)
}

async fn fetch_blueprints(client: &Client) -> AnyResult<BTreeMap<String, BlueprintSpec>> {
    Ok(client
        .raw()
        .blueprints()
        .list()
        .await?
        .value
        .blueprints
        .into_iter()
        .filter_map(|blueprint| {
            let device_type = blueprint.device_type?;
            Some((
                device_type.clone(),
                BlueprintSpec {
                    device_type,
                    print_time_seconds: blueprint.print_time.unwrap_or(0.0),
                    resources: blueprint.resources.unwrap_or_default(),
                    components: blueprint.components.unwrap_or_default(),
                },
            ))
        })
        .collect())
}

async fn factory_workloads(
    client: &Client,
    blueprints: &BTreeMap<String, BlueprintSpec>,
    hub: &str,
) -> AnyResult<Vec<FactoryWorkload>> {
    let mut factories = discover_factories(client, hub, blueprints)
        .await?
        .into_iter()
        .map(|factory| factory.workload())
        .collect::<Vec<_>>();
    factories.sort_by(|left, right| left.code.cmp(&right.code));
    Ok(factories)
}

fn save_plan(path: &Path, mission: &MiningMission) -> AnyResult<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    let file = File::create(&temporary)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, mission)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::rename(&temporary, path)?;
    Ok(())
}

/// Loads a durable mining expansion checkpoint from disk.
pub fn load_expansion(path: &Path) -> AnyResult<MiningMission> {
    let mut mission: MiningMission = serde_json::from_slice(&fs::read(path)?)?;
    if mission.version != PLAN_VERSION {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "mission version {} is unsupported; expected {PLAN_VERSION}",
                mission.version
            ),
        ));
    }
    migrate_mission_tag_metadata(&mut mission);
    Ok(mission)
}

/// Inputs for invoking the durable mining workflow from another automation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MiningExpansionRequest {
    /// Systems whose best discovered belts should receive mining setups.
    pub systems: Vec<String>,
    /// Owned replicant name or code responsible for the assets.
    pub replicant: String,
    /// Manufacturing hub and route delivery location.
    pub hub: String,
    /// Exact AMI routes to provision. Systems without an explicit route use
    /// the deterministic preferred-belt selector.
    #[serde(default)]
    pub transport_routes: Vec<AmiTransportRouteIntent>,
    /// Child mission file used for restart-safe reconciliation.
    pub mission_file: PathBuf,
    /// Maximum wait for one manufacturing or travel stage.
    pub wait_timeout: Duration,
    /// Maximum number of carrier deployments in flight at once.
    pub max_concurrency: usize,
}

/// Result of a reusable mining expansion run.
#[derive(Clone, Debug, Serialize)]
pub struct MiningExpansionReport {
    /// Systems represented by the completed child mission.
    pub systems: Vec<String>,
    /// Belt locations made operational.
    pub belts: Vec<String>,
    /// Final durable state, including per-site and per-route checkpoints.
    pub mission: MiningMission,
}

/// Creates and persists a mining expansion plan without executing it.
pub async fn plan_expansion(
    client: &Client,
    request: &MiningExpansionRequest,
    replace_existing: bool,
) -> AnyResult<MiningMission> {
    validate_request(request)?;
    let config = Config {
        systems: request.systems.clone(),
        replicant: Some(request.replicant.clone()),
        hub: request.hub.to_ascii_uppercase(),
        transport_routes: request.transport_routes.clone(),
        plan_path: request.mission_file.clone(),
        replace_plan: replace_existing,
        wait_timeout: request.wait_timeout,
        max_concurrency: request.max_concurrency,
    };
    create_plan(client, &config).await
}

/// Creates a mining expansion plan from the daemon's committed managed state.
///
/// Durable Director workflows use this path so both planning entry points
/// consume the same projection-first planner.
pub(crate) async fn plan_expansion_from_managed_state(
    client: &Client,
    request: &MiningExpansionRequest,
    replace_existing: bool,
) -> AnyResult<MiningMission> {
    validate_request(request)?;
    let config = Config {
        systems: request.systems.clone(),
        replicant: Some(request.replicant.clone()),
        hub: request.hub.to_ascii_uppercase(),
        transport_routes: request.transport_routes.clone(),
        plan_path: request.mission_file.clone(),
        replace_plan: replace_existing,
        wait_timeout: request.wait_timeout,
        max_concurrency: request.max_concurrency,
    };
    create_plan(client, &config).await
}

/// Creates or resumes a mining expansion using an already-running managed client.
pub async fn execute_expansion(
    client: &Client,
    request: &MiningExpansionRequest,
) -> AnyResult<MiningExpansionReport> {
    validate_request(request)?;
    if request.systems.is_empty() && !request.mission_file.exists() {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "a new mining child mission requires at least one system",
        ));
    }
    if !request.mission_file.exists() {
        plan_expansion(client, request, false).await?;
    }

    let execution = Config {
        systems: Vec::new(),
        replicant: None,
        hub: request.hub.to_ascii_uppercase(),
        transport_routes: request.transport_routes.clone(),
        plan_path: request.mission_file.clone(),
        replace_plan: false,
        wait_timeout: request.wait_timeout,
        max_concurrency: request.max_concurrency,
    };
    let _lock = MissionLock::acquire(&request.mission_file)?;
    let mut mission = load_expansion(&request.mission_file)?;
    executor::execute(client, &execution, &mut mission).await?;
    let report = MiningExpansionReport {
        systems: mission
            .sites
            .iter()
            .map(|site| site.system.clone())
            .collect(),
        belts: mission.sites.iter().map(|site| site.belt.clone()).collect(),
        mission,
    };
    Ok(report)
}

fn validate_request(request: &MiningExpansionRequest) -> AnyResult<()> {
    if request.replicant.trim().is_empty() || request.hub.trim().is_empty() {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "mining expansion requires a replicant and hub",
        ));
    }
    if !(1..=32).contains(&request.max_concurrency) {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "max concurrency must be between 1 and 32",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_client::{
        SecretString,
        domain::{
            AccessScope, ActiveDeviceDirective, DeviceDirective, DeviceId, DeviceKey,
            DeviceRelationships, DeviceStatus, GalacticPosition, StarKey,
        },
        raw::Url,
    };
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    #[test]
    fn mining_mission_tags_are_system_scoped_and_bounded() {
        assert_eq!(mining_mission_tag("SCEPTURUM-BELT-1"), "mine-m:scepturum");
        let long =
            mining_mission_tag("A-SYSTEM-NAME-THAT-IS-WELL-PAST-THE-DEVICE-TAG-LIMIT-BELT-1");
        assert!(long.chars().count() <= MAX_DEVICE_TAG_CHARS);
    }

    #[test]
    fn mining_site_delivery_requires_attachment_capacity() {
        let site = SiteMission {
            system: "PHASYRIS".into(),
            belt: "PHASYRIS-BELT-1".into(),
            density: "dense".into(),
            tag: "mine-site:phasyris".into(),
            phase: SitePhase::Planned,
            assets: SiteAssets::default(),
            missing: [
                (MINING_CONTROLLER.into(), 1),
                (MINING_DRONE.into(), 8),
                (SYSTEM_WARD.into(), 1),
            ]
            .into_iter()
            .collect(),
            carrier: None,
        };
        let requirements = mining_site_item_requirements("delta", &site);
        let attachment = requirements
            .iter()
            .find(|requirement| requirement.key == "attach")
            .expect("attachment-capacity requirement");

        assert_eq!(attachment.kind, "attach");
        assert_eq!(attachment.quantity, 9);
        assert!(
            requirements
                .iter()
                .all(|requirement| requirement.key != "stow")
        );

        let mut ward_only = site;
        ward_only.missing = [
            (SYSTEM_WARD.into(), 1),
            (MINING_DRONE.into(), 0),
            ("unknown_device".into(), 3),
        ]
        .into_iter()
        .collect();
        let ward_requirements = mining_site_item_requirements("delta", &ward_only);
        assert!(
            ward_requirements
                .iter()
                .all(|requirement| requirement.key != "carrier")
        );
        assert!(
            ward_requirements
                .iter()
                .all(|requirement| requirement.key != "attach")
        );
    }

    #[test]
    fn mining_transport_routes_do_not_require_stow_capacity() {
        let requirements = mining_route_item_requirements("delta");
        let keys = requirements
            .iter()
            .map(|requirement| requirement.key.as_str())
            .collect::<Vec<_>>();

        assert_eq!(keys, ["worker", "transport_controller", "freighter"]);
        assert!(
            requirements
                .iter()
                .all(|requirement| requirement.kind != "stow")
        );
    }

    #[tokio::test]
    async fn planning_uses_projection_then_exact_location_without_full_sync() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/replicants/WORKER"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "replicant_code": "WORKER",
                "status": "active"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/blueprints"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"blueprints": []})),
            )
            .mount(&server)
            .await;
        let client = Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .authentication_token(SecretString::from("test-token"))
            .in_memory()
            .startup_policy(replicant_client::StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("restore-only startup");
        client
            .replicants()
            .get_owned("WORKER")
            .await
            .expect("seed committed worker projection");
        let mission_file = std::env::temp_dir().join(format!(
            "replicant-mining-no-full-sync-{}.json",
            uuid::Uuid::new_v4()
        ));
        let request = MiningExpansionRequest {
            systems: vec!["SOL".into()],
            replicant: "WORKER".into(),
            hub: "SOL-L4".into(),
            transport_routes: Vec::new(),
            mission_file,
            wait_timeout: Duration::from_secs(1),
            max_concurrency: 1,
        };

        assert!(plan_expansion(&client, &request, false).await.is_err());
        let requests = server.received_requests().await.expect("received requests");
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].url.path(), "/v1/replicants/WORKER");
        assert_eq!(requests[1].url.path(), "/v1/blueprints");
        assert_eq!(requests[2].url.path(), "/v1/locations/SOL");
        client.close().await.expect("close");
    }

    fn device(code: &str, device_type_name: &str, location: &str) -> Device {
        Device {
            key: DeviceKey::live(DeviceId::from(code)),
            device_type: Some(DeviceType::from(device_type_name)),
            status: Some(DeviceStatus::from("idle")),
            location: Some(replicant_client::domain::LocationKey::live(location.into())),
            deployed_at: None,
            in_control_range: None,
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            settings: Default::default(),
            relationships: DeviceRelationships::default(),
            cargo: Default::default(),
            cargo_capacity: None,
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
            grace_period_remaining: None,
            upkeep_requirements: Vec::new(),
            system_status: None,
            active_directive: None,
            travel: None,
            runtime: Default::default(),
            access: AccessScope::Owned,
        }
    }

    fn directive(device: &mut Device, name: &str) {
        device.active_directive = Some(ActiveDeviceDirective {
            directive: Some(DeviceDirective::from(name)),
            status: Some("active".into()),
            details: BTreeMap::new(),
        });
    }

    fn star(name: &str, x: f64, y: f64, z: f64) -> Star {
        Star {
            key: StarKey::live(replicant_client::StarId::from(name)),
            name: None,
            spectral_type: None,
            entry_point: None,
            position: Some(GalacticPosition { x, y, z }),
            has_hub: None,
            has_ward: None,
            knowledge_observed: true,
            explored: None,
            has_life: None,
            region: None,
        }
    }

    #[test]
    fn mining_sites_are_ordered_nearest_to_the_hub() {
        let catalogue = vec![
            star("HUB", 0.0, 0.0, 0.0),
            star("NEAR", 1.0, 1.0, 0.0),
            star("FAR", 8.0, 0.0, 0.0),
        ];
        let mut systems = vec!["UNKNOWN".into(), "FAR".into(), "NEAR".into()];

        sort_systems_by_hub_distance(&mut systems, "HUB-BELT-1", &catalogue);

        assert_eq!(systems, ["NEAR", "FAR", "UNKNOWN"]);
    }

    #[test]
    fn same_location_needs_no_transport_service() {
        assert_eq!(
            transport_service_present(&[], "BETA", "BETA-BELT-1", "BETA-BELT-1").state,
            EvidenceState::Present
        );
        assert_eq!(
            transport_service_present(&[], "BETA", "BETA-BELT-2", "BETA-BELT-1").state,
            EvidenceState::Absent
        );
    }

    #[test]
    fn complete_site_is_recognized_from_child_relationships() {
        let belt = "SOL-BELT-1";
        let mut devices = vec![
            device("MC", MINING_CONTROLLER, belt),
            device("SC", SURVEY_CONTROLLER, belt),
            device("MD", MAINTENANCE_DRONE, belt),
        ];
        directive(&mut devices[0], "deplete_smallest");
        directive(&mut devices[1], "belt_search");
        directive(&mut devices[2], "patrol");
        devices[0].status = Some(DeviceStatus::from("coordinating"));
        devices[1].status = Some(DeviceStatus::from("coordinating"));
        for index in 0..4 {
            let mut drone = device(&format!("M{index}"), MINING_DRONE, belt);
            drone.relationships.controller = Some(DeviceKey::live(DeviceId::from("MC")));
            devices.push(drone);
        }
        for index in 0..2 {
            let mut drone = device(&format!("S{index}"), SURVEY_DRONE, belt);
            drone.relationships.controller = Some(DeviceKey::live(DeviceId::from("SC")));
            devices.push(drone);
        }
        let audit = audit_site(&devices, "SOL", belt);
        assert!(audit.operational);
        assert!(site_shortages(&audit).is_empty());
    }

    #[test]
    fn deployed_inactive_ward_is_a_configuration_repair_not_a_print_shortage() {
        let belt = "SOL-BELT-1";
        let devices = vec![device("WARD", SYSTEM_WARD, "SOL-OORT")];
        let audit = audit_site(&devices, "SOL", belt);
        assert_eq!(audit.assets.system_ward.as_deref(), Some("WARD"));
        assert!(!audit.operational);
        assert!(!site_shortages(&audit).contains_key(SYSTEM_WARD));
    }

    #[test]
    fn tags_are_normalized_for_uppercase_systems() {
        assert_eq!(site_tag("ILPHARD"), "mine-s:ilphard");
    }

    #[test]
    fn execution_batches_use_single_queue_units() {
        let scheduled = PrintBatch {
            factory_code: "AF1".into(),
            device_type: MINING_DRONE.into(),
            quantity: 3,
            sequence: 0,
            projected_finish_seconds: 300.0,
        };
        let batches = execution_batches("mission", PrintPurpose::Site, &[scheduled]);
        assert_eq!(batches.len(), 3);
        assert!(batches.iter().all(|batch| batch.quantity == 1));
        assert_eq!(
            batches
                .iter()
                .map(|batch| batch.batch_tag.as_str())
                .collect::<BTreeSet<_>>()
                .len(),
            3
        );
    }

    #[test]
    fn planner_reuses_output_reserved_for_the_same_stable_mining_mission() {
        let hub = "ROOT-1-L4";
        let mission_tag = mining_mission_tag(hub);
        let mut same_mission = device("WARD-SAME", SYSTEM_WARD, hub);
        same_mission.tags = vec![mission_tag.clone(), "mine-b:old-batch".into()];
        let mut other_mission = device("WARD-OTHER", SYSTEM_WARD, hub);
        other_mission.tags = vec!["mine-m:other".into(), "mine-b:other-batch".into()];

        let same_counts = reusable_counts(&[same_mission], hub, &mission_tag, true);
        assert_eq!(same_counts.get(SYSTEM_WARD), Some(&1));
        let other_counts = reusable_counts(&[other_mission], hub, &mission_tag, true);
        assert_eq!(
            other_counts.get(SYSTEM_WARD).copied().unwrap_or_default(),
            0
        );
    }
    #[test]
    fn traveling_freighter_relationship_completes_existing_route() {
        let hub = "SCEPTURUM-BELT-1";
        let belt = "ILPHARD-BELT-1";
        let mut controller = device("TC", TRANSPORT_CONTROLLER, hub);
        controller.active_directive = Some(ActiveDeviceDirective {
            directive: Some(DeviceDirective::from("ferry")),
            status: Some("active".into()),
            details: [(
                "config".into(),
                serde_json::json!({
                    "collect": belt,
                    "deliver": hub,
                    "priority": ["rares", "volatiles"]
                }),
            )]
            .into_iter()
            .collect(),
        });
        let mut freighter = device("CF", CARGO_FREIGHTER, hub);
        freighter.location = None;
        freighter.relationships.controller = Some(DeviceKey::live(DeviceId::from("TC")));
        let idle_audit = transport_service_present(
            &[controller.clone(), freighter.clone()],
            "ILPHARD",
            belt,
            hub,
        );
        assert_eq!(idle_audit.state, EvidenceState::Absent);

        controller.status = Some(DeviceStatus::from("coordinating"));

        let audit = transport_service_present(&[controller, freighter], "ILPHARD", belt, hub);
        assert_eq!(audit.state, EvidenceState::Present);
        assert_eq!(audit.controller.as_deref(), Some("TC"));
        assert_eq!(audit.freighter.as_deref(), Some("CF"));
    }

    #[test]
    fn mission_phase_advances_without_regressing_after_resume() {
        assert_eq!(
            MissionPhase::Planned.advance_to(MissionPhase::ManufacturingSites),
            MissionPhase::ManufacturingSites
        );
        assert_eq!(
            MissionPhase::ActivatingRoutes.advance_to(MissionPhase::ManufacturingSites),
            MissionPhase::ActivatingRoutes
        );
        assert_eq!(
            MissionPhase::Completed.advance_to(MissionPhase::ReturningCarriers),
            MissionPhase::Completed
        );
    }
}
