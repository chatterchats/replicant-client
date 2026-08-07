use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error as StdError,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use replicant_client::{
    Client, Replicant, SecretString, StartupPolicy,
    domain::{Device, DeviceType, Location},
};
use replicant_mining_planner::{
    BlueprintSpec, CARGO_FREIGHTER, FactoryWorkload, MAINTENANCE_DRONE, MINING_CONTROLLER,
    MINING_DRONE, PrintBatch, QuantityMap, SURVEY_CONTROLLER, SURVEY_DRONE, TRANSPORT_CONTROLLER,
    add_quantities, blueprint_resource_cost, mining_site_requirements, schedule_prints, shortages,
    site_tag,
};
use replicant_printing::managed::inspect_factory;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::info;
use tracing_subscriber::{EnvFilter, prelude::*};

mod executor;

const PLAN_VERSION: u32 = 1;
const DEFAULT_REPLICANT: &str = "Chats-1";
const DEFAULT_HUB: &str = "SCEPTURUM-BELT-1";
const DEFAULT_PLAN_PATH: &str = "mining-expansion.json";
const AUTOFACTORY: &str = "autofactory";
const DEFAULT_WAIT_SECONDS: u64 = 21_600;

/// Error type returned by the reusable mining workflow.
pub type AnyError = Box<dyn StdError + Send + Sync + 'static>;
/// Result type returned by the reusable mining workflow.
pub type AnyResult<T> = Result<T, AnyError>;

fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> AnyError {
    io::Error::new(kind, message.into()).into()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Plan,
    Run,
    Status,
}

#[derive(Debug)]
struct Config {
    command: Command,
    systems: Vec<String>,
    systems_file: Option<PathBuf>,
    replicant: Option<String>,
    hub: String,
    database: PathBuf,
    plan_path: PathBuf,
    replace_plan: bool,
    wait_timeout: Duration,
    max_concurrency: usize,
    verbose: bool,
    log_file: Option<PathBuf>,
    json: bool,
}

impl Config {
    fn from_args_and_env(arguments: impl IntoIterator<Item = String>) -> AnyResult<Self> {
        let mut arguments = arguments.into_iter().peekable();
        let command = match arguments.next().as_deref() {
            Some("plan") => Command::Plan,
            Some("run") => Command::Run,
            Some("status") => Command::Status,
            Some("-h" | "--help") | None => {
                print_help();
                std::process::exit(0);
            }
            Some(other) => {
                return Err(app_error(
                    io::ErrorKind::InvalidInput,
                    format!("unknown command: {other}"),
                ));
            }
        };

        let mut systems = Vec::new();
        let mut systems_file = None;
        let mut replicant = env::var("RS_MINING_REPLICANT").ok();
        let mut hub = env::var("RS_MINING_HUB").unwrap_or_else(|_| DEFAULT_HUB.into());
        let mut database = PathBuf::from(
            env::var("REPLICANT_DB").unwrap_or_else(|_| "replicant-client.sqlite".into()),
        );
        let mut plan_path =
            PathBuf::from(env::var("RS_MINING_PLAN").unwrap_or_else(|_| DEFAULT_PLAN_PATH.into()));
        let mut replace_plan = false;
        let mut wait_timeout = Duration::from_secs(
            env::var("RS_MINING_WAIT_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_WAIT_SECONDS),
        );
        let mut max_concurrency = env::var("RS_MINING_MAX_CONCURRENCY")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(8usize);
        let mut verbose = env_flag("RS_MINING_VERBOSE");
        let mut log_file = env::var("RS_MINING_LOG_FILE").ok().map(PathBuf::from);
        let mut json = false;

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--system" => systems.push(required_argument(&mut arguments, "--system")?),
                "--systems-file" => {
                    systems_file = Some(PathBuf::from(required_argument(
                        &mut arguments,
                        "--systems-file",
                    )?))
                }
                "--replicant" => {
                    replicant = Some(required_argument(&mut arguments, "--replicant")?)
                }
                "--hub" => hub = required_argument(&mut arguments, "--hub")?,
                "--database" => {
                    database = PathBuf::from(required_argument(&mut arguments, "--database")?)
                }
                "--mission-file" | "--plan-file" => {
                    plan_path = PathBuf::from(required_argument(&mut arguments, &argument)?)
                }
                "--replace-plan" => replace_plan = true,
                "--wait-timeout-secs" => {
                    wait_timeout = Duration::from_secs(
                        required_argument(&mut arguments, "--wait-timeout-secs")?
                            .parse()
                            .map_err(|_| {
                                app_error(
                                    io::ErrorKind::InvalidInput,
                                    "--wait-timeout-secs must be an integer",
                                )
                            })?,
                    );
                }
                "--max-concurrency" => {
                    max_concurrency = required_argument(&mut arguments, "--max-concurrency")?
                        .parse()
                        .map_err(|_| {
                            app_error(
                                io::ErrorKind::InvalidInput,
                                "--max-concurrency must be an integer",
                            )
                        })?;
                }
                "--verbose" => verbose = true,
                "--log-file" => {
                    log_file = Some(PathBuf::from(required_argument(
                        &mut arguments,
                        "--log-file",
                    )?))
                }
                "--json" => json = true,
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                value if value.starts_with('-') => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        format!("unknown option: {value}"),
                    ));
                }
                value if command == Command::Plan => systems.push(value.to_owned()),
                value => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        format!("unexpected argument: {value}"),
                    ));
                }
            }
        }

        if max_concurrency == 0 || max_concurrency > 32 {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--max-concurrency must be between 1 and 32",
            ));
        }
        if command != Command::Plan && (!systems.is_empty() || systems_file.is_some()) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "system inputs belong on the plan command; run loads the persisted mission",
            ));
        }

        Ok(Self {
            command,
            systems,
            systems_file,
            replicant,
            hub: hub.to_ascii_uppercase(),
            database,
            plan_path,
            replace_plan,
            wait_timeout,
            max_concurrency,
            verbose,
            log_file,
            json,
        })
    }

    fn requested_systems(&self) -> AnyResult<Vec<String>> {
        let mut systems = self.systems.clone();
        if let Some(path) = &self.systems_file {
            let contents = fs::read_to_string(path)?;
            systems.extend(contents.lines().flat_map(|line| {
                line.split_once('#')
                    .map_or(line, |(before, _)| before)
                    .split_whitespace()
                    .map(str::to_owned)
            }));
        }
        let systems = systems
            .into_iter()
            .map(|value| value.trim().to_ascii_uppercase())
            .filter(|value| !value.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if systems.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "plan requires at least one SYSTEM, --system, or --systems-file",
            ));
        }
        Ok(systems)
    }
}

fn required_argument(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> AnyResult<String> {
    arguments.next().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} requires a value"),
        )
    })
}

fn env_flag(name: &str) -> bool {
    env::var(name).is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

fn print_help() {
    println!(
        "Replicant mining expansion\n\n\
Usage:\n  replicant-cli mining --plan [SYSTEM ...] [OPTIONS]\n  replicant-cli mining --run [OPTIONS]\n  replicant-cli mining --status [OPTIONS]\n\n\
Options:\n  --system SYSTEM           Add one target system (repeatable)\n  --systems-file PATH       Read whitespace/newline separated systems\n  --replicant NAME_OR_CODE  Defaults to Chats-1\n  --hub LOCATION            Manufacturing and delivery hub\n  --database PATH           Managed SQLite database\n  --mission-file PATH       Persisted mission (default: mining-expansion.json)\n  --replace-plan            Replace an existing incomplete mission\n  --wait-timeout-secs N     Per-stage wait timeout (default: 21600)\n  --max-concurrency N       Maximum simultaneous carrier deployments (default: 8)\n  --verbose                 Show tracing logs in the terminal\n  --log-file PATH           Append tracing logs to a file\n  --json                    Emit machine-readable output\n  -h, --help                Show this help\n\n\
The plan command is read-only. Run always reconciles the saved mission against\n\
live state and then starts or continues its first incomplete stage."
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MissionPhase {
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SitePhase {
    Planned,
    Ready,
    Outbound,
    Configuring,
    Operational,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RoutePhase {
    Planned,
    Ready,
    Activating,
    Active,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum PrintPurpose {
    Site,
    Route,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct SiteAssets {
    mining_controller: Option<String>,
    mining_drones: Vec<String>,
    survey_controller: Option<String>,
    survey_drones: Vec<String>,
    maintenance_drone: Option<String>,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SiteMission {
    system: String,
    belt: String,
    density: String,
    tag: String,
    phase: SitePhase,
    assets: SiteAssets,
    missing: QuantityMap,
    carrier: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RouteMission {
    system: String,
    belt: String,
    tag: String,
    phase: RoutePhase,
    controller: Option<String>,
    freighter: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ExecutionPrintBatch {
    purpose: PrintPurpose,
    factory_code: String,
    device_type: String,
    quantity: i64,
    projected_finish_seconds: f64,
    batch_tag: String,
    submission_started: bool,
    submitted: bool,
    operation_id: Option<String>,
    produced_codes: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MiningMission {
    version: u32,
    mission_id: String,
    mission_tag: String,
    phase: MissionPhase,
    selected_replicant: String,
    hub_location: String,
    sites: Vec<SiteMission>,
    routes: Vec<RouteMission>,
    print_batches: Vec<ExecutionPrintBatch>,
    site_print_requirements: QuantityMap,
    route_print_requirements: QuantityMap,
    total_material_cost: QuantityMap,
    warnings: Vec<String>,
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

/// Runs the standalone mining-expansion command-line interface.
pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let config = Config::from_args_and_env(arguments)?;
    init_logging(&config)?;
    if config.command == Command::Status {
        return show_status(&config);
    }
    if config.command == Command::Run && !config.plan_path.exists() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!(
                "no mining mission exists at {}; create one with `replicant-cli mining --plan ...`",
                config.plan_path.display()
            ),
        ));
    }

    let token = env::var("RS_API_TOKEN")
        .map(SecretString::from)
        .map_err(|_| app_error(io::ErrorKind::NotFound, "RS_API_TOKEN is not set"))?;
    let client = Client::builder()
        .authentication_token(token)
        .sqlite(&config.database)
        .startup_policy(StartupPolicy::Essential)
        .start()
        .await?;
    let result = match config.command {
        Command::Plan => create_plan(&client, &config).await,
        Command::Run => {
            let _lock = MissionLock::acquire(&config.plan_path)?;
            let mut mission = load_plan(&config.plan_path)?;
            executor::execute(&client, &config, &mut mission).await
        }
        Command::Status => unreachable!("status returned before client startup"),
    };
    let close_result = client.close().await;
    result?;
    close_result?;
    Ok(())
}

fn init_logging(config: &Config) -> AnyResult<()> {
    if !config.verbose && config.log_file.is_none() {
        return Ok(());
    }
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("warn,replicant_cli::mining=info,replicant_client::ops=info")
    });
    match (&config.log_file, config.verbose) {
        (None, true) => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
            .try_init()
            .map_err(|error| app_error(io::ErrorKind::Other, error.to_string()))?,
        (Some(path), verbose) => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)?;
            }
            let file = OpenOptions::new().create(true).append(true).open(path)?;
            let registry = tracing_subscriber::registry().with(filter);
            if verbose {
                registry
                    .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_ansi(false)
                            .with_writer(std::sync::Mutex::new(file)),
                    )
                    .try_init()
                    .map_err(|error| app_error(io::ErrorKind::Other, error.to_string()))?;
            } else {
                registry
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_ansi(false)
                            .with_writer(std::sync::Mutex::new(file)),
                    )
                    .try_init()
                    .map_err(|error| app_error(io::ErrorKind::Other, error.to_string()))?;
            }
        }
        (None, false) => {}
    }
    Ok(())
}

async fn create_plan(client: &Client, config: &Config) -> AnyResult<()> {
    if config.plan_path.exists() && !config.replace_plan {
        let existing = load_plan(&config.plan_path)?;
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
    let factories = factory_workloads(client, &devices, &blueprints, &config.hub).await?;

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
    let mission_tag = format!("mine-m:{:016x}", stable_hash(&mission_id));
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
    print_plan(&mission, &config.plan_path, config.json)?;
    Ok(())
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
    let requested = requested.unwrap_or(DEFAULT_REPLICANT);
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
    devices: &[Device],
    blueprints: &BTreeMap<String, BlueprintSpec>,
    hub: &str,
) -> AnyResult<Vec<FactoryWorkload>> {
    let mut factories = Vec::new();
    for device in devices.iter().filter(|device| {
        device_type(device) == Some(AUTOFACTORY) && device_location(device) == Some(hub)
    }) {
        factories.push(
            inspect_factory(client, device.key.id.as_str(), blueprints)
                .await?
                .workload(),
        );
    }
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

fn load_plan(path: &Path) -> AnyResult<MiningMission> {
    let mission: MiningMission = serde_json::from_slice(&fs::read(path)?)?;
    if mission.version != PLAN_VERSION {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "mission version {} is unsupported; expected {PLAN_VERSION}",
                mission.version
            ),
        ));
    }
    Ok(mission)
}

fn print_plan(mission: &MiningMission, plan_path: &Path, json: bool) -> AnyResult<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(mission)?);
        return Ok(());
    }
    println!("Mining expansion plan {}", mission.mission_id);
    println!("Hub:        {}", mission.hub_location);
    println!("Replicant:  {}", mission.selected_replicant);
    println!("Systems:    {}", mission.sites.len());
    println!(
        "Established: {}",
        mission
            .sites
            .iter()
            .filter(|site| site.phase == SitePhase::Operational)
            .count()
    );
    println!("\nSites:");
    for site in &mission.sites {
        println!(
            "  {:<14} {:<24} {:<9} {:?}{}",
            site.system,
            site.belt,
            site.density,
            site.phase,
            if site.missing.is_empty() {
                String::new()
            } else {
                format!("  missing {}", format_quantities(&site.missing))
            }
        );
    }
    println!(
        "\nSite prints:  {}",
        format_quantities(&mission.site_print_requirements)
    );
    println!(
        "Route prints: {}",
        format_quantities(&mission.route_print_requirements)
    );
    println!(
        "Materials:    {}",
        format_quantities(&mission.total_material_cost)
    );
    println!(
        "Print finish: {:.0}s projected",
        mission
            .print_batches
            .iter()
            .map(|batch| batch.projected_finish_seconds)
            .fold(0.0, f64::max)
    );
    println!("Saved plan:   {}", plan_path.display());
    println!("Start or continue with: replicant-cli mining --run");
    Ok(())
}

fn show_status(config: &Config) -> AnyResult<()> {
    if !config.plan_path.exists() {
        println!(
            "No mining mission exists at {}.",
            config.plan_path.display()
        );
        return Ok(());
    }
    let mission = load_plan(&config.plan_path)?;
    if config.json {
        println!("{}", serde_json::to_string_pretty(&mission)?);
        return Ok(());
    }
    println!("Mission:    {}", mission.mission_id);
    println!("Phase:      {:?}", mission.phase);
    println!("Hub:        {}", mission.hub_location);
    println!("Replicant:  {}", mission.selected_replicant);
    println!(
        "Sites:      {}/{} operational",
        mission
            .sites
            .iter()
            .filter(|site| site.phase == SitePhase::Operational)
            .count(),
        mission.sites.len()
    );
    println!(
        "Routes:     {}/{} active",
        mission
            .routes
            .iter()
            .filter(|route| route.phase == RoutePhase::Active)
            .count(),
        mission.routes.len()
    );
    println!(
        "Printing:   {}/{} produced",
        mission
            .print_batches
            .iter()
            .map(|batch| batch.produced_codes.len())
            .sum::<usize>(),
        mission
            .print_batches
            .iter()
            .filter_map(|batch| usize::try_from(batch.quantity).ok())
            .sum::<usize>()
    );
    for site in &mission.sites {
        println!(
            "  {:<14} site={:?} carrier={}",
            site.system,
            site.phase,
            site.carrier.as_deref().unwrap_or("-")
        );
    }
    if !mission.warnings.is_empty() {
        println!("Warnings:");
        for warning in &mission.warnings {
            println!("  - {warning}");
        }
    }
    Ok(())
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
}

/// Creates or resumes a mining expansion using an already-running managed client.
pub async fn execute_expansion(
    client: &Client,
    request: &MiningExpansionRequest,
) -> AnyResult<MiningExpansionReport> {
    if request.systems.is_empty() && !request.mission_file.exists() {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "a new mining child mission requires at least one system",
        ));
    }
    let planning = Config {
        command: Command::Plan,
        systems: request.systems.clone(),
        systems_file: None,
        replicant: Some(request.replicant.clone()),
        hub: request.hub.to_ascii_uppercase(),
        database: PathBuf::new(),
        plan_path: request.mission_file.clone(),
        replace_plan: false,
        wait_timeout: request.wait_timeout,
        max_concurrency: request.max_concurrency,
        verbose: false,
        log_file: None,
        json: false,
    };
    if !request.mission_file.exists() {
        create_plan(client, &planning).await?;
    }

    let execution = Config {
        command: Command::Run,
        systems: Vec::new(),
        systems_file: None,
        replicant: None,
        hub: request.hub.to_ascii_uppercase(),
        database: PathBuf::new(),
        plan_path: request.mission_file.clone(),
        replace_plan: false,
        wait_timeout: request.wait_timeout,
        max_concurrency: request.max_concurrency,
        verbose: false,
        log_file: None,
        json: false,
    };
    let _lock = MissionLock::acquire(&request.mission_file)?;
    let mut mission = load_plan(&request.mission_file)?;
    executor::execute(client, &execution, &mut mission).await?;
    Ok(MiningExpansionReport {
        systems: mission
            .sites
            .iter()
            .map(|site| site.system.clone())
            .collect(),
        belts: mission.sites.iter().map(|site| site.belt.clone()).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_client::domain::{
        AccessScope, ActiveDeviceDirective, DeviceDirective, DeviceId, DeviceKey,
        DeviceRelationships, DeviceStatus,
    };

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
            attach_capacity: None,
            stow_capacity: None,
            stow_used: None,
            operational_capacity: None,
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
}
