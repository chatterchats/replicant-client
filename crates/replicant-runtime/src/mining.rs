use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error as StdError,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use replicant_client::{
    Client, Replicant,
    domain::{Device, DeviceType, Location},
};
use replicant_mining_planner::{
    BlueprintSpec, CARGO_FREIGHTER, FactoryWorkload, MAINTENANCE_DRONE, MINING_CONTROLLER,
    MINING_DRONE, PrintBatch, QuantityMap, SURVEY_CONTROLLER, SURVEY_DRONE, TRANSPORT_CONTROLLER,
    add_quantities, blueprint_resource_cost, mining_site_requirements, schedule_prints, shortages,
    site_tag,
};
use replicant_printing::managed::discover_factories;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::info;

mod executor;

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
}

impl SiteAssets {
    fn codes(&self) -> Vec<String> {
        self.mining_controller
            .iter()
            .chain(&self.mining_drones)
            .chain(self.survey_controller.iter())
            .chain(&self.survey_drones)
            .chain(self.maintenance_drone.iter())
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

    let sync = client.sync().full().await?;
    info!(readiness = ?sync.readiness, "full managed synchronization completed");
    let selected_replicant = select_replicant(client, config.replicant.as_deref()).await?;
    let systems = config.requested_systems()?;
    let devices = refresh_device_snapshots(client).await?;
    let blueprints = fetch_blueprints(client).await?;
    let factories = factory_workloads(client, &blueprints, &config.hub).await?;

    let mut sites = Vec::new();
    for system in systems {
        let belt = select_belt(client, &system).await?;
        let audit = audit_site(&devices, &belt.designation);
        let missing = shortages(&mining_site_requirements(), &audit.assets.counts());
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

async fn refresh_device_snapshots(client: &Client) -> AnyResult<Vec<Device>> {
    let handles = client
        .devices()
        .refresh_many()
        .page_size(50)
        .collect()
        .await?;
    let mut devices = Vec::with_capacity(handles.len());
    for handle in handles {
        devices.push(handle.snapshot().await?);
    }
    devices.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(devices)
}

async fn select_replicant(client: &Client, requested: Option<&str>) -> AnyResult<String> {
    let handles = client.replicants().find().owned().collect().await?;
    let mut replicants = Vec::new();
    for handle in handles {
        replicants.push(handle.snapshot().await?);
    }
    let requested = requested.ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidInput,
            "mining expansion requires a replicant name or code",
        )
    })?;
    let mut matches = replicants
        .into_iter()
        .filter(|replicant| {
            replicant.key.id.as_str().eq_ignore_ascii_case(requested)
                || replicant
                    .name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(requested))
        })
        .collect::<Vec<Replicant>>();
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

async fn select_belt(client: &Client, system: &str) -> AnyResult<SelectedBelt> {
    let location = client.locations().get(system).await?;
    let mut belts = belts_from_location(&location);
    belts.sort_by(|left, right| {
        density_rank(&right.density)
            .cmp(&density_rank(&left.density))
            .then_with(|| left.designation.cmp(&right.designation))
    });
    belts.into_iter().next().ok_or_else(|| {
        app_error(
            io::ErrorKind::NotFound,
            format!("system {system} has no discovered asteroid belt"),
        )
    })
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

struct SiteAudit {
    assets: SiteAssets,
    operational: bool,
}

fn audit_site(devices: &[Device], belt: &str) -> SiteAudit {
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
    let assets = SiteAssets {
        mining_controller,
        mining_drones,
        survey_controller,
        survey_drones,
        maintenance_drone: maintenance,
    };
    let operational = assets
        .mining_controller
        .as_deref()
        .and_then(|code| find_device(devices, code))
        .is_some_and(|device| has_directive(device, "deplete_smallest"))
        && assets.mining_drones.len() >= 4
        && assets
            .survey_controller
            .as_deref()
            .and_then(|code| find_device(devices, code))
            .is_some_and(|device| has_directive(device, "belt_search"))
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
                    resources: numeric_map(blueprint.resources.as_ref()),
                    components: numeric_map(blueprint.components.as_ref()),
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

fn numeric_map(object: Option<&Map<String, Value>>) -> QuantityMap {
    object
        .map(|object| {
            object
                .iter()
                .filter_map(|(name, value)| {
                    value_to_i64(value).map(|quantity| (name.clone(), quantity))
                })
                .filter(|(_, quantity)| *quantity > 0)
                .collect()
        })
        .unwrap_or_default()
}

fn value_to_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_u64()
            .and_then(|number| i64::try_from(number).ok())
            .or_else(|| value.as_f64().map(|number| number.round() as i64))
    })
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
    use replicant_client::domain::{
        AccessScope, ActiveDeviceDirective, DeviceDirective, DeviceId, DeviceKey,
        DeviceRelationships, DeviceStatus,
    };

    #[test]
    fn mining_mission_tags_are_system_scoped_and_bounded() {
        assert_eq!(mining_mission_tag("SCEPTURUM-BELT-1"), "mine-m:scepturum");
        let long =
            mining_mission_tag("A-SYSTEM-NAME-THAT-IS-WELL-PAST-THE-DEVICE-TAG-LIMIT-BELT-1");
        assert!(long.chars().count() <= MAX_DEVICE_TAG_CHARS);
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
        let audit = audit_site(&devices, belt);
        assert!(audit.operational);
        assert!(shortages(&mining_site_requirements(), &audit.assets.counts()).is_empty());
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
