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
    domain::{Device, DeviceType, Location},
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
    WorkflowKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::info;

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

#[derive(Debug)]
struct Config {
    systems: Vec<String>,
    replicant: Option<String>,
    hub: String,
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
                capabilities: Vec::new(),
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
                "legacy_complete": mission.print_batches.iter().all(|batch| {
                    usize::try_from(batch.quantity)
                        .is_ok_and(|quantity| batch.produced_codes.len() >= quantity)
                }),
            }),
            preconditions_json: serde_json::json!([]),
            requirements_json: serde_json::to_value(requirements)?,
            deadline_at_ms: None,
        });
    }
    Ok(specs)
}

fn mining_site_item_requirements(region: &str, site: &SiteMission) -> Vec<ResourceRequirement> {
    let scope = || RequirementScope::Region(region.to_owned());
    let mut requirements = vec![ResourceRequirement {
        key: "worker".into(),
        kind: "replicant".into(),
        capabilities: Vec::new(),
        scope: scope(),
        count: 1,
        quantity: 1,
    }];
    for (device_type, quantity) in &site.missing {
        let Ok(count) = u32::try_from(*quantity) else {
            continue;
        };
        if count == 0 {
            continue;
        }
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
    let missing_devices = site
        .missing
        .values()
        .filter_map(|quantity| u64::try_from(*quantity).ok())
        .sum::<u64>();
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
            key: "stow".into(),
            kind: "stow".into(),
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
            capabilities: Vec::new(),
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
        ResourceRequirement {
            key: "stow".into(),
            kind: "stow".into(),
            capabilities: Vec::new(),
            scope: scope(),
            count: 1,
            quantity: 2,
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
            if missing.contains_key(MINING_CONTROLLER) {
                site.assets.mining_controller = Some(mining_allocated_identity(
                    allocations,
                    "mining_controller",
                    "device",
                )?);
            }
            if missing.contains_key(MINING_DRONE) {
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
            if missing.contains_key(SURVEY_CONTROLLER) {
                site.assets.survey_controller = Some(mining_allocated_identity(
                    allocations,
                    "survey_controller",
                    "device",
                )?);
            }
            if missing.contains_key(SURVEY_DRONE) {
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
            if missing.contains_key(MAINTENANCE_DRONE) {
                site.assets.maintenance_drone = Some(mining_allocated_identity(
                    allocations,
                    "maintenance_drone",
                    "device",
                )?);
            }
            if missing.contains_key(SYSTEM_WARD) {
                // Keep the allocated ward reserved for the follow-up delivery.
                // The nine mining devices can deploy and begin configuring first.
                site.assets.system_ward = Some(mining_allocated_identity(
                    allocations,
                    "system_ward",
                    "device",
                )?);
            }
            if !missing.is_empty() {
                site.carrier = Some(mining_allocated_identity(allocations, "carrier", "device")?);
                mining_validate_stow_owner(allocations, "carrier")?;
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
            mining_validate_stow_owner(allocations, "freighter")?;
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

fn mining_validate_stow_owner(
    allocations: &AllocationSet,
    device_requirement: &str,
) -> AnyResult<()> {
    let device = mining_allocated_identity(allocations, device_requirement, "device")?;
    let matching = allocations
        .by_requirement
        .get("stow")
        .into_iter()
        .flatten()
        .any(|allocation| {
            matches!(
                &allocation.resource,
                ResourceKey::Namespaced { namespace, key }
                    if namespace == "stow" && key == &device
            )
        });
    if !matching {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("mining item stow allocation does not belong to {device}"),
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
    let devices = device_snapshots(client).await?;
    let catalogue = client.galaxy().catalogue();
    sort_systems_by_hub_distance(&mut systems, &config.hub, &catalogue);
    let protection = protected_systems(&devices, &catalogue);
    let blueprints = fetch_blueprints(client).await?;
    let factories = factory_workloads(client, &blueprints, &config.hub).await?;

    let mut sites = Vec::new();
    for system in systems {
        let belt = select_belt(client, &system, &devices).await?;
        let audit = audit_site(
            &devices,
            &system,
            &belt.designation,
            protection.contains(&system),
        );
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

    let mut site_required = QuantityMap::new();
    for site in &sites {
        add_quantities(&mut site_required, &site.missing);
    }
    let reusable_site = reusable_counts(&devices, &config.hub, true);
    let site_print_requirements = shortages(&site_required, &reusable_site);

    let mut routes = Vec::new();
    for site in &sites {
        if !requires_ferry(&site.belt, &config.hub) {
            continue;
        }
        let audit = audit_route(&devices, &site.system, &site.belt, &config.hub);
        routes.push(RouteMission {
            system: site.system.clone(),
            belt: site.belt.clone(),
            tag: site.tag.clone(),
            phase: if audit.active {
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
    let reusable_route = reusable_counts(&devices, &config.hub, false);
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
    let mission_tag = mining_mission_tag(&config.hub);
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

fn requires_ferry(belt: &str, hub: &str) -> bool {
    belt != hub
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
    let mut belts = belts_from_location(&location);
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
    pub(crate) protection_active: bool,
    pub(crate) operational: bool,
}

pub(crate) fn audit_site(
    devices: &[Device],
    system: &str,
    belt: &str,
    protection_active: bool,
) -> SiteAudit {
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
        && adopted_count(devices, assets.survey_controller.as_deref(), SURVEY_DRONE) >= 2
        && protection_active;
    SiteAudit {
        assets,
        protection_active,
        operational,
    }
}

fn site_shortages(audit: &SiteAudit) -> QuantityMap {
    let mut missing = shortages(&mining_site_requirements(), &audit.assets.counts());
    if !audit.protection_active && audit.assets.system_ward.is_none() {
        missing.insert(SYSTEM_WARD.to_owned(), 1);
    }
    missing
}

fn device_is_in_system(device: &Device, system: &str) -> bool {
    device_location(device).is_some_and(|location| location_is_in_system(location, system))
}

pub(crate) fn active_owned_ward_systems(
    devices: &[Device],
    catalogue: &[Star],
) -> BTreeSet<String> {
    catalogue
        .iter()
        .filter(|star| star.has_ward == Some(true))
        .filter_map(|star| {
            let system = star.key.id.as_str();
            devices
                .iter()
                .any(|device| {
                    device.device_type.as_ref() == Some(&DeviceType::SystemWard)
                        && device_is_in_system(device, system)
                })
                .then_some(system.to_owned())
        })
        .collect()
}

pub(crate) fn protected_systems(devices: &[Device], catalogue: &[Star]) -> BTreeSet<String> {
    let active_wards = active_owned_ward_systems(devices, catalogue);
    catalogue
        .iter()
        .filter_map(|star| {
            let system = star.key.id.as_str();
            let owned_hub = star.has_hub == Some(true)
                && devices.iter().any(|device| {
                    device.device_type.as_ref() == Some(&DeviceType::SystemHub)
                        && device_is_in_system(device, system)
                });
            (owned_hub || active_wards.contains(system)).then_some(system.to_owned())
        })
        .collect()
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

struct RouteAudit {
    controller: Option<String>,
    freighter: Option<String>,
    active: bool,
}

fn audit_route(devices: &[Device], _system: &str, belt: &str, hub: &str) -> RouteAudit {
    let controller = devices
        .iter()
        .filter(|device| {
            device_type(device) == Some(TRANSPORT_CONTROLLER)
                && ferry_route_matches(device, belt, hub)
        })
        .min_by(|left, right| left.key.id.as_str().cmp(right.key.id.as_str()));
    let freighter = controller.and_then(|controller| {
        devices
            .iter()
            .filter(|device| {
                device_type(device) == Some(CARGO_FREIGHTER)
                    && controller_code(device) == Some(controller.key.id.as_str())
            })
            .min_by(|left, right| left.key.id.as_str().cmp(right.key.id.as_str()))
    });
    RouteAudit {
        controller: controller.map(|device| device.key.id.as_str().to_owned()),
        freighter: freighter.map(|device| device.key.id.as_str().to_owned()),
        active: controller.is_some() && freighter.is_some(),
    }
}

fn ferry_route_matches(device: &Device, collect: &str, deliver: &str) -> bool {
    let Some(active) = &device.active_directive else {
        return false;
    };
    if active
        .directive
        .as_ref()
        .is_none_or(|directive| directive.as_str() != "ferry")
    {
        return false;
    }
    let config = active.details.get("config").and_then(Value::as_object);
    config.is_some_and(|config| {
        config.get("collect").and_then(Value::as_str) == Some(collect)
            && config.get("deliver").and_then(Value::as_str) == Some(deliver)
    })
}

fn reusable_counts(devices: &[Device], hub: &str, site_devices: bool) -> QuantityMap {
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
            && !has_reservation_tag(device)
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

fn format_quantities(quantities: &QuantityMap) -> String {
    if quantities.is_empty() {
        return "none".into();
    }
    quantities
        .iter()
        .filter(|(_, quantity)| **quantity > 0)
        .map(|(name, quantity)| format!("{quantity} {name}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Inputs for invoking the durable mining workflow from another automation.
#[derive(Clone, Debug)]
pub struct MiningExpansionRequest {
    /// Systems whose best discovered belts should receive mining setups.
    pub systems: Vec<String>,
    /// Owned replicant name or code responsible for the assets.
    pub replicant: String,
    /// Manufacturing hub and route delivery location.
    pub hub: String,
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
            features: Vec::new(),
            available_commands: Vec::new(),
            available_directives: Vec::new(),
            tags: Vec::new(),
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
    fn hub_belt_needs_no_ferry_route() {
        assert!(!requires_ferry("BETA-BELT-1", "BETA-BELT-1"));
        assert!(requires_ferry("BETA-BELT-2", "BETA-BELT-1"));
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
        let audit = audit_site(&devices, "SOL", belt, true);
        assert!(audit.operational);
        assert!(shortages(&mining_site_requirements(), &audit.assets.counts()).is_empty());
    }

    #[test]
    fn deployed_inactive_ward_is_a_configuration_repair_not_a_print_shortage() {
        let belt = "SOL-BELT-1";
        let devices = vec![device("WARD", SYSTEM_WARD, "SOL-OORT")];
        let audit = audit_site(&devices, "SOL", belt, false);
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

        let audit = audit_route(&[controller, freighter], "ILPHARD", belt, hub);
        assert!(audit.active);
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
