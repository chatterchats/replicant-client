mod executor;
mod model;

use std::{
    collections::BTreeSet,
    env,
    error::Error as StdError,
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{config::ManagedClientConfig, start_managed_client};
pub use model::{BootstrapMission, MissionPhase};
use model::{ChildMissions, PLAN_VERSION, PrintState};
use replicant_bootstrap_planner::{
    BootstrapProfile, ark_device_requirements, mission_tag, validate_profile,
};
use replicant_client::{Client, SyncDomain};
use tracing::info;
use tracing_subscriber::{EnvFilter, prelude::*};

type AnyError = Box<dyn StdError + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

const DEFAULT_WAIT_SECONDS: u64 = 21_600;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Plan,
    Stage,
    Deliver,
    Run,
    Status,
}

#[derive(Debug)]
struct Config {
    command: Command,
    region: Option<String>,
    landing_star: Option<String>,
    source_hub: String,
    operator: String,
    explorer: String,
    mission_file: PathBuf,
    database: PathBuf,
    profile: BootstrapProfile,
    seed_quantity: i64,
    quick_scout_radius_ly: f64,
    survey_radius_ly: f64,
    minimum_sites: usize,
    maximum_sites: usize,
    max_concurrency: usize,
    wait_timeout: Duration,
    replace_plan: bool,
    verbose: bool,
    log_file: Option<PathBuf>,
    json: bool,
}

impl Config {
    fn from_args(arguments: impl IntoIterator<Item = String>) -> AnyResult<Self> {
        let mut args = arguments.into_iter();
        let command = match args.next().as_deref() {
            Some("plan") => Command::Plan,
            Some("stage") => Command::Stage,
            Some("deliver") => Command::Deliver,
            Some("run") => Command::Run,
            Some("status") => Command::Status,
            Some("-h" | "--help") | None => {
                print_help();
                std::process::exit(0);
            }
            Some(value) => {
                return Err(app_error(
                    io::ErrorKind::InvalidInput,
                    format!("unknown command: {value}"),
                ));
            }
        };
        let mut config = Self {
            command,
            region: None,
            landing_star: None,
            source_hub: "SCEPTURUM-BELT-1".into(),
            operator: "Chats-1".into(),
            explorer: "Chats-2".into(),
            mission_file: PathBuf::from("regional-bootstrap.json"),
            database: env::var_os("REPLICANT_DB")
                .map(PathBuf::from)
                .unwrap_or_else(replicant_client::default_database_path),
            profile: BootstrapProfile::default(),
            seed_quantity: 500,
            quick_scout_radius_ly: 7.499,
            survey_radius_ly: 30.0,
            minimum_sites: 5,
            maximum_sites: 9,
            max_concurrency: 8,
            wait_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
            replace_plan: false,
            verbose: false,
            log_file: None,
            json: false,
        };
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--region" => config.region = Some(required(&mut args, &arg)?.to_ascii_lowercase()),
                "--landing-star" => {
                    config.landing_star = Some(required(&mut args, &arg)?.to_ascii_uppercase())
                }
                "--source-hub" | "--hub" => {
                    config.source_hub = required(&mut args, &arg)?.to_ascii_uppercase()
                }
                "--operator" => config.operator = required(&mut args, &arg)?,
                "--explorer" => config.explorer = required(&mut args, &arg)?,
                "--mission-file" => config.mission_file = PathBuf::from(required(&mut args, &arg)?),
                "--database" => config.database = PathBuf::from(required(&mut args, &arg)?),
                "--mining-setups" => config.profile.mining_setups = parse(&mut args, &arg)?,
                "--autofactories" => config.profile.autofactories = parse(&mut args, &arg)?,
                "--freighters" => config.profile.cargo_freighters = parse(&mut args, &arg)?,
                "--transport-controllers" => {
                    config.profile.transport_controllers = parse(&mut args, &arg)?
                }
                "--hub-maintenance-drones" => {
                    config.profile.hub_maintenance_drones = parse(&mut args, &arg)?
                }
                "--root-relays" => config.profile.root_relays = parse(&mut args, &arg)?,
                "--expansion-relays" => config.profile.expansion_relays = parse(&mut args, &arg)?,
                "--ftl-beacons" => config.profile.ftl_beacons = parse(&mut args, &arg)?,
                "--seed-quantity" => config.seed_quantity = parse(&mut args, &arg)?,
                "--quick-scout-radius" => config.quick_scout_radius_ly = parse(&mut args, &arg)?,
                "--survey-radius" => config.survey_radius_ly = parse(&mut args, &arg)?,
                "--min-sites" => config.minimum_sites = parse(&mut args, &arg)?,
                "--max-sites" => config.maximum_sites = parse(&mut args, &arg)?,
                "--max-concurrency" => config.max_concurrency = parse(&mut args, &arg)?,
                "--wait-timeout-secs" => {
                    config.wait_timeout = Duration::from_secs(parse(&mut args, &arg)?)
                }
                "--replace-plan" => config.replace_plan = true,
                "--verbose" => config.verbose = true,
                "--log-file" => config.log_file = Some(PathBuf::from(required(&mut args, &arg)?)),
                "--json" => config.json = true,
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                value => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        format!("unknown option: {value}"),
                    ));
                }
            }
        }
        if config
            .region
            .as_deref()
            .is_some_and(|region| !matches!(region, "beta" | "gamma"))
        {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--region must be beta or gamma",
            ));
        }
        if config.operator.eq_ignore_ascii_case(&config.explorer) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "operator and explorer must be different replicants",
            ));
        }
        if config.seed_quantity <= 0
            || config.minimum_sites == 0
            || config.maximum_sites < config.minimum_sites
        {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "seed quantity and site bounds must be positive and ordered",
            ));
        }
        if !(1..=32).contains(&config.max_concurrency) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--max-concurrency must be between 1 and 32",
            ));
        }
        validate_profile(&config.profile)?;
        Ok(config)
    }

    fn landing_star(&self) -> String {
        self.landing_star.clone().unwrap_or_else(|| {
            if self.region.as_deref() == Some("gamma") {
                "OWLOAEI".into()
            } else {
                "RHWYRHYR".into()
            }
        })
    }
}

fn required(args: &mut impl Iterator<Item = String>, option: &str) -> AnyResult<String> {
    args.next().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} requires a value"),
        )
    })
}

fn parse<T: std::str::FromStr>(
    args: &mut impl Iterator<Item = String>,
    option: &str,
) -> AnyResult<T> {
    required(args, option)?.parse().map_err(|_| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("invalid value for {option}"),
        )
    })
}

fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> AnyError {
    io::Error::new(kind, message.into()).into()
}

/// Goal-level inputs for creating a regional bootstrap plan without exposing CLI plumbing.
#[derive(Clone, Debug)]
pub struct BootstrapPlanningRequest {
    /// Known star in the target region used as the ark rendezvous.
    pub landing_star: String,
    /// Existing manufacturing location from which the ark is built.
    pub source_hub: String,
    /// Replicant that becomes the regional capital operator.
    pub operator: String,
    /// Replicant that performs regional scouting and survey work.
    pub explorer: String,
    /// Workflow-owned compatibility mission path.
    pub mission_file: PathBuf,
    /// Whether an abandoned scratch plan may be replaced.
    pub replace_plan: bool,
}

/// Creates a bootstrap mission from goal-level inputs and returns the durable plan.
pub async fn plan_bootstrap(
    client: &Client,
    request: &BootstrapPlanningRequest,
) -> AnyResult<BootstrapMission> {
    let config = Config {
        command: Command::Plan,
        region: None,
        landing_star: Some(request.landing_star.trim().to_ascii_uppercase()),
        source_hub: request.source_hub.trim().to_ascii_uppercase(),
        operator: request.operator.clone(),
        explorer: request.explorer.clone(),
        mission_file: request.mission_file.clone(),
        database: PathBuf::new(),
        profile: BootstrapProfile::default(),
        seed_quantity: 500,
        quick_scout_radius_ly: 7.499,
        survey_radius_ly: 30.0,
        minimum_sites: 5,
        maximum_sites: 9,
        max_concurrency: 8,
        wait_timeout: Duration::from_secs(DEFAULT_WAIT_SECONDS),
        replace_plan: request.replace_plan,
        verbose: false,
        log_file: None,
        json: false,
    };
    create_plan(client, &config).await?;
    load_mission(&request.mission_file)
}

/// Inputs shared by the reusable bootstrap stage, deliver, and run operations.
#[derive(Clone, Debug)]
pub struct BootstrapExecutionRequest {
    /// Durable bootstrap mission file.
    pub mission_file: PathBuf,
    /// Maximum wait for one manufacturing, travel, or SSE-backed state transition.
    pub wait_timeout: Duration,
}

impl BootstrapExecutionRequest {
    /// Creates an execution request for a persisted bootstrap mission.
    #[must_use]
    pub fn new(mission_file: impl Into<PathBuf>, wait_timeout: Duration) -> Self {
        Self {
            mission_file: mission_file.into(),
            wait_timeout,
        }
    }
}

fn print_help() {
    println!(
        "Regional bootstrap automation\n\nUsage:\n  replicant-cli bootstrap --plan [OPTIONS]\n  replicant-cli bootstrap --stage [OPTIONS]\n  replicant-cli bootstrap --deliver [OPTIONS]\n  replicant-cli bootstrap --run [OPTIONS]\n  replicant-cli bootstrap --status [OPTIONS]\n\nCore options:\n  --landing-star STAR       Ark rendezvous; its region is inferred automatically\n  --region beta|gamma       Optional region constraint/default landing selector\n  --operator NAME_OR_CODE   Existing or future capital replicant (default: Chats-1)\n  --explorer NAME_OR_CODE   Existing or future explorer (default: Chats-2)\n  --source-hub LOCATION     Source manufacturing hub\n  --mission-file PATH       Durable parent mission\n  --mining-setups N         Initial complete setups (5-10, default: 8)\n  --autofactories N         Regional factories (3-6, default: 6)\n  --freighters N            Seed/route freighters (6-12, default: 6)\n  --transport-controllers N Initial AMI transport controllers\n  --hub-maintenance-drones N Capital maintenance reserve (1-4, default: 2)\n  --root-relays N            Relays reserved for the regional root (1-4, default: 1)\n  --expansion-relays N       Expansion relays (0-36, default: 18)\n  --ftl-beacons N            Monitoring beacons (0-18, default: 9)\n  --seed-quantity N         Resource units in each seed freighter (default: 500)\n  --quick-scout-radius LY   Visit-and-scan dense-belt search radius\n  --survey-radius LY        Regional survey radius (default: 30)\n  --min-sites N             Minimum dense mining systems (default: 5)\n  --max-sites N             Maximum dense mining systems (default: 9)\n  --max-concurrency N       Concurrent dispatch limit (default: 8)\n  --wait-timeout-secs N     Per-stage timeout\n  --replace-plan            Replace an incomplete plan\n  --database PATH           Managed SQLite database\n  --verbose                 Log to stderr\n  --log-file PATH           Append detailed logs to a file\n  --json                    Machine-readable plan/status\n\n`stage` prints, loads, assembles, and moves the ark to the source system entry point without requiring the planned replicants. `deliver` creates a plan when needed, manufactures and loads the ark, sends only the ark devices to the landing star entry point (or the star itself when no entry point is known), and stops before regional deployment. `run` resolves the planned replicants and continues the full regional workflow. There is no --execute or resume command."
    );
    println!(
        "\nDefault ark reserve: 8 complete mining setups, 1 root relay, 18 expansion relays, and 9 monitoring beacons. Surge Carrier count is derived automatically from the payload; idle source-hub carriers are reused first and replacement carriers are queued after loading."
    );
}

/// Runs the legacy bootstrap CLI frontend against the reusable bootstrap services.
pub async fn run_cli(arguments: Vec<String>) -> AnyResult<()> {
    let config = Config::from_args(arguments)?;
    init_logging(&config)?;
    if config.command == Command::Status {
        return show_status(&config);
    }
    if matches!(config.command, Command::Stage | Command::Run) && !config.mission_file.exists() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!(
                "no mission at {}; create it with `replicant-cli bootstrap --plan`",
                config.mission_file.display()
            ),
        ));
    }
    let client = start_managed_client(ManagedClientConfig::from_env(&config.database)?).await?;
    let result = match config.command {
        Command::Plan => create_plan(&client, &config).await,
        Command::Stage => {
            let _lock = MissionLock::acquire(&config.mission_file)?;
            let mut mission = load_mission(&config.mission_file)?;
            validate_mission_constraints(&config, &mission)?;
            executor::stage(&client, &config, &mut mission).await
        }
        Command::Deliver => deliver_command(&client, &config).await,
        Command::Run => {
            let _lock = MissionLock::acquire(&config.mission_file)?;
            let mut mission = load_mission(&config.mission_file)?;
            validate_mission_constraints(&config, &mission)?;
            executor::execute(&client, &config, &mut mission).await
        }
        Command::Status => unreachable!(),
    };
    let close = client.close().await;
    result?;
    close?;
    Ok(())
}

async fn deliver_command(client: &Client, config: &Config) -> AnyResult<()> {
    if !config.mission_file.exists() {
        create_plan(client, config).await?;
    }
    let _lock = MissionLock::acquire(&config.mission_file)?;
    let mut mission = load_mission(&config.mission_file)?;
    validate_mission_constraints(config, &mission)?;
    executor::deliver(client, config, &mut mission).await
}

fn validate_mission_constraints(config: &Config, mission: &BootstrapMission) -> AnyResult<()> {
    if let Some(landing_star) = config.landing_star.as_deref()
        && !mission.landing_star.eq_ignore_ascii_case(landing_star)
    {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!(
                "saved mission lands at {}, not requested {landing_star}; use --replace-plan to create a new mission",
                mission.landing_star
            ),
        ));
    }
    if let Some(region) = config.region.as_deref()
        && !mission.region.eq_ignore_ascii_case(region)
    {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!(
                "saved mission is for region {}, not requested {region}; use --replace-plan to create a new mission",
                mission.region
            ),
        ));
    }
    Ok(())
}

async fn create_plan(client: &Client, config: &Config) -> AnyResult<()> {
    if config.mission_file.exists() && !config.replace_plan {
        let existing = load_mission(&config.mission_file)?;
        if !existing.phase.is_terminal() {
            return Err(app_error(
                io::ErrorKind::AlreadyExists,
                format!(
                    "incomplete mission {} exists; use deliver, run, status, or --replace-plan",
                    existing.mission_id
                ),
            ));
        }
    }
    client.ready().await?;
    let sync = client.sync().domain(SyncDomain::Replicants).await?;
    info!(readiness=?sync.readiness, "refreshed owned replicants for bootstrap planning");
    client.galaxy().refresh_catalogue().await?;
    let operator = executor::resolve_replicant(client, &config.operator)
        .await?
        .unwrap_or_else(|| model::ReplicantIdentity::pending(&config.operator));
    let explorer = executor::resolve_replicant(client, &config.explorer)
        .await?
        .unwrap_or_else(|| model::ReplicantIdentity::pending(&config.explorer));
    if operator.is_resolved() && explorer.is_resolved() && operator.code == explorer.code {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "operator and explorer resolved to the same replicant",
        ));
    }
    let landing_star = config.landing_star();
    let (landing_entry, resolved_region) = executor::resolve_star(client, &landing_star).await?;
    if let Some(region) = config.region.as_deref()
        && !resolved_region.eq_ignore_ascii_case(region)
    {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("{landing_star} is in region {resolved_region}, not {region}"),
        ));
    }
    let region = config
        .region
        .clone()
        .unwrap_or_else(|| resolved_region.to_ascii_lowercase());
    let mission_id = uuid::Uuid::new_v4().simple().to_string();
    let parent = config
        .mission_file
        .parent()
        .unwrap_or_else(|| Path::new("."));
    let stem = config
        .mission_file
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("regional-bootstrap");
    let child_dir = parent.join(format!("{stem}.d")).join(&mission_id);
    let requirements = ark_device_requirements(&config.profile);
    let mut mission = BootstrapMission {
        version: PLAN_VERSION,
        mission_tag: mission_tag(&landing_star),
        legacy_mission_tags: Vec::new(),
        region_tag: format!("region:{region}"),
        mission_id,
        phase: MissionPhase::Planned,
        region,
        source_hub: config.source_hub.clone(),
        source_system: String::new(),
        source_entry: String::new(),
        landing_star,
        landing_entry,
        operator,
        explorer,
        profile: config.profile.clone(),
        seed_quantity: config.seed_quantity,
        quick_scout_radius_ly: config.quick_scout_radius_ly,
        survey_radius_ly: config.survey_radius_ly,
        minimum_sites: config.minimum_sites,
        maximum_sites: config.maximum_sites,
        max_concurrency: config.max_concurrency,
        print: PrintState {
            requirements,
            ..PrintState::default()
        },
        carrier_replacement_print: PrintState::default(),
        assets: Default::default(),
        carrier_target: 0,
        reused_carrier_target: 0,
        seed_freighters: Vec::new(),
        carrier_loads: Vec::new(),
        quick_scouted_systems: Vec::new(),
        capital_system: None,
        capital_belt: None,
        capital_entry: None,
        survey_systems: Vec::new(),
        selected_belts: Vec::new(),
        children: ChildMissions {
            quick_survey: child_dir.join("quick-survey.json"),
            initial_mining: child_dir.join("initial-mining.json"),
            survey: child_dir.join("survey.json"),
            relays: child_dir.join("relays.json"),
            mining: child_dir.join("mining.json"),
        },
        warnings: Vec::new(),
    };
    executor::ensure_source_entry(client, config, &mut mission).await?;
    save_mission(&config.mission_file, &mission)?;
    print_mission(&mission, &config.mission_file, config.json)
}

fn show_status(config: &Config) -> AnyResult<()> {
    let mission = load_mission(&config.mission_file)?;
    print_mission(&mission, &config.mission_file, config.json)
}

fn print_mission(mission: &BootstrapMission, path: &Path, json: bool) -> AnyResult<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(mission)?);
        return Ok(());
    }
    println!(
        "Regional bootstrap {} ({})",
        mission.mission_id, mission.region
    );
    let operator = if mission.operator.is_resolved() {
        mission.operator.code.as_str()
    } else {
        mission.operator.query()
    };
    let explorer = if mission.explorer.is_resolved() {
        mission.explorer.code.as_str()
    } else {
        mission.explorer.query()
    };
    println!(
        "Phase: {:?}\nSource: {} (entry: {})\nLanding: {} ({})\nOperator: {}{}  Explorer: {}{}",
        mission.phase,
        mission.source_hub,
        mission.source_entry,
        mission.landing_star,
        mission.landing_entry,
        operator,
        if mission.operator.is_resolved() {
            ""
        } else {
            " (pending)"
        },
        explorer,
        if mission.explorer.is_resolved() {
            ""
        } else {
            " (pending)"
        }
    );
    if let (Some(system), Some(belt)) = (&mission.capital_system, &mission.capital_belt) {
        println!("Capital: {system} / {belt}");
    }
    println!(
        "Selected mining systems: {}\nMission file: {}",
        mission.selected_belts.len(),
        path.display()
    );
    if mission.carrier_target > 0 {
        println!(
            "Ark Surge Carriers: {} total ({} reused from source); replacement prints: {}",
            mission.carrier_target,
            mission.reused_carrier_target,
            if mission.carrier_replacement_print.queued {
                "queued"
            } else if mission.carrier_replacement_print.submission_started {
                "submission pending reconciliation"
            } else {
                "not queued yet"
            }
        );
    }
    if !mission.warnings.is_empty() {
        println!("Warnings:\n  - {}", mission.warnings.join("\n  - "));
    }
    Ok(())
}

/// Loads and migrates a durable bootstrap mission.
pub fn load_mission(path: &Path) -> AnyResult<BootstrapMission> {
    let mut mission: BootstrapMission = serde_json::from_slice(&fs::read(path)?)?;
    if mission.version != PLAN_VERSION {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("unsupported mission version {}", mission.version),
        ));
    }
    mission.operator.migrate();
    mission.explorer.migrate();
    migrate_mission_tag_metadata(&mut mission);
    Ok(mission)
}

/// Upgrades an in-memory legacy bootstrap checkpoint while retaining old aliases.
pub(crate) fn migrate_mission_tag_metadata(mission: &mut BootstrapMission) -> bool {
    let desired = mission_tag(&mission.landing_star);
    let mut changed = false;
    if mission.mission_tag != desired {
        let previous = std::mem::replace(&mut mission.mission_tag, desired.clone());
        if previous.starts_with("boot-m:") && !mission.legacy_mission_tags.contains(&previous) {
            mission.legacy_mission_tags.push(previous);
        }
        changed = true;
    }
    let before = mission.legacy_mission_tags.len();
    mission
        .legacy_mission_tags
        .retain(|tag| tag.starts_with("boot-m:") && tag != &desired);
    mission.legacy_mission_tags.sort();
    mission.legacy_mission_tags.dedup();
    changed || mission.legacy_mission_tags.len() != before
}

pub(crate) fn save_mission(path: &Path, mission: &BootstrapMission) -> AnyResult<()> {
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(mission)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn init_logging(config: &Config) -> AnyResult<()> {
    if !config.verbose && config.log_file.is_none() {
        return Ok(());
    }
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("warn,replicant_cli::bootstrap=info,replicant_client::ops=info")
    });
    match (&config.log_file, config.verbose) {
        (None, true) => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
            .try_init()?,
        (Some(path), verbose) => {
            if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
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
                    .try_init()?;
            } else {
                registry
                    .with(
                        tracing_subscriber::fmt::layer()
                            .with_ansi(false)
                            .with_writer(std::sync::Mutex::new(file)),
                    )
                    .try_init()?;
            }
        }
        (None, false) => {}
    }
    Ok(())
}

struct MissionLock {
    path: PathBuf,
}
impl MissionLock {
    fn acquire(path: &Path) -> AnyResult<Self> {
        let lock = PathBuf::from(format!("{}.lock", path.display()));
        if let Some(parent) = lock.parent().filter(|value| !value.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        for attempt in 0..2 {
            match OpenOptions::new().write(true).create_new(true).open(&lock) {
                Ok(mut file) => {
                    writeln!(file, "{}", std::process::id())?;
                    file.sync_all()?;
                    return Ok(Self { path: lock });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists && attempt == 0 => {
                    let owner = fs::read_to_string(&lock)
                        .ok()
                        .and_then(|value| value.trim().parse::<u32>().ok());
                    if owner.is_some_and(|pid| PathBuf::from(format!("/proc/{pid}")).exists()) {
                        return Err(app_error(
                            io::ErrorKind::WouldBlock,
                            format!(
                                "another bootstrap executor holds {} (pid {})",
                                lock.display(),
                                owner.unwrap_or_default()
                            ),
                        ));
                    }
                    fs::remove_file(&lock)?;
                }
                Err(error) => {
                    return Err(app_error(
                        error.kind(),
                        format!("could not acquire mission lock {}: {error}", lock.display()),
                    ));
                }
            }
        }
        Err(app_error(
            io::ErrorKind::WouldBlock,
            format!("could not acquire mission lock {}", lock.display()),
        ))
    }
}
impl Drop for MissionLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(crate) fn reservation_tag(tag: &str) -> bool {
    ["evt-m:", "mine-m:", "relay-m:", "boot-m:"]
        .iter()
        .any(|prefix| tag.starts_with(prefix))
}

pub(crate) fn unique(values: impl IntoIterator<Item = String>) -> Vec<String> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn execution_config(
    request: &BootstrapExecutionRequest,
    mission: &BootstrapMission,
    command: Command,
) -> Config {
    Config {
        command,
        region: Some(mission.region.clone()),
        landing_star: Some(mission.landing_star.clone()),
        source_hub: mission.source_hub.clone(),
        operator: mission.operator.query().to_owned(),
        explorer: mission.explorer.query().to_owned(),
        mission_file: request.mission_file.clone(),
        database: PathBuf::new(),
        profile: mission.profile.clone(),
        seed_quantity: mission.seed_quantity,
        quick_scout_radius_ly: mission.quick_scout_radius_ly,
        survey_radius_ly: mission.survey_radius_ly,
        minimum_sites: mission.minimum_sites,
        maximum_sites: mission.maximum_sites,
        max_concurrency: mission.max_concurrency,
        wait_timeout: request.wait_timeout,
        replace_plan: false,
        verbose: false,
        log_file: None,
        json: false,
    }
}

/// Resumes manufacturing, loading, and source-entry staging for a persisted ark.
pub async fn stage_bootstrap(
    client: &Client,
    request: &BootstrapExecutionRequest,
) -> AnyResult<BootstrapMission> {
    let _lock = MissionLock::acquire(&request.mission_file)?;
    let mut mission = load_mission(&request.mission_file)?;
    let config = execution_config(request, &mission, Command::Stage);
    executor::stage(client, &config, &mut mission).await?;
    Ok(mission)
}

/// Resumes manufacturing and delivers the ark payload to the persisted landing star.
pub async fn deliver_bootstrap(
    client: &Client,
    request: &BootstrapExecutionRequest,
) -> AnyResult<BootstrapMission> {
    let _lock = MissionLock::acquire(&request.mission_file)?;
    let mut mission = load_mission(&request.mission_file)?;
    let config = execution_config(request, &mission, Command::Deliver);
    executor::deliver(client, &config, &mut mission).await?;
    Ok(mission)
}

/// Resumes the complete regional bootstrap from its durable checkpoint.
pub async fn run_bootstrap(
    client: &Client,
    request: &BootstrapExecutionRequest,
) -> AnyResult<BootstrapMission> {
    let _lock = MissionLock::acquire(&request.mission_file)?;
    let mut mission = load_mission(&request.mission_file)?;
    let config = execution_config(request, &mission, Command::Run);
    executor::execute(client, &config, &mut mission).await?;
    Ok(mission)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(args: &[&str]) -> Config {
        Config::from_args(args.iter().map(|value| (*value).to_owned())).expect("valid config")
    }

    #[test]
    fn landing_star_does_not_require_a_region() {
        let config = config(&["plan", "--landing-star", "lumbunga"]);
        assert_eq!(config.landing_star.as_deref(), Some("LUMBUNGA"));
        assert_eq!(config.region.as_deref(), None);
        assert_eq!(config.landing_star(), "LUMBUNGA");
    }

    #[test]
    fn region_still_selects_the_legacy_default_landing() {
        let config = config(&["plan", "--region", "gamma"]);
        assert_eq!(config.region.as_deref(), Some("gamma"));
        assert_eq!(config.landing_star(), "OWLOAEI");
    }

    #[test]
    fn deliver_is_a_first_class_bootstrap_operation() {
        let config = config(&["deliver", "--landing-star", "waneame"]);
        assert_eq!(config.command, Command::Deliver);
        assert_eq!(config.landing_star(), "WANEAME");
    }

    #[test]
    fn payload_counts_do_not_require_manual_carrier_configuration() {
        let config = config(&[
            "plan",
            "--mining-setups",
            "10",
            "--hub-maintenance-drones",
            "4",
            "--root-relays",
            "2",
            "--expansion-relays",
            "27",
            "--ftl-beacons",
            "18",
        ]);
        assert_eq!(config.profile.mining_setups, 10);
        assert_eq!(config.profile.hub_maintenance_drones, 4);
        assert_eq!(config.profile.root_relays, 2);
        assert_eq!(config.profile.expansion_relays, 27);
        assert_eq!(config.profile.ftl_beacons, 18);
        assert_eq!(config.profile.dedicated_surge_carriers, 0);
    }

    #[test]
    fn explicit_regions_remain_limited_to_supported_names() {
        let error = Config::from_args(["plan", "--region", "alpha"].into_iter().map(str::to_owned))
            .expect_err("alpha is not an explicit regional-bootstrap selector");
        assert!(error.to_string().contains("--region must be beta or gamma"));
    }

    #[test]
    fn reusable_execution_uses_persisted_mission_scope() {
        let mission: BootstrapMission = serde_json::from_value(serde_json::json!({
            "version": 1,
            "mission_id": "mission",
            "mission_tag": "boot-m:mission",
            "region_tag": "region:beta",
            "phase": "planned",
            "region": "beta",
            "source_hub": "SOURCE-BELT-1",
            "source_system": "SOURCE",
            "source_entry": "SOURCE-STAR",
            "landing_star": "LANDING",
            "landing_entry": "LANDING-STAR",
            "operator": {"requested":"Operator","code":"1","name":"Operator","vessel":"V1"},
            "explorer": {"requested":"Explorer","code":"2","name":"Explorer","vessel":"V2"},
            "profile": BootstrapProfile::default(),
            "seed_quantity": 500,
            "quick_scout_radius_ly": 7.499,
            "survey_radius_ly": 30.0,
            "minimum_sites": 5,
            "maximum_sites": 9,
            "max_concurrency": 8,
            "print": {"requirements":{},"submission_started":false,"queued":false},
            "children": {"quick_survey":"q.json","initial_mining":"i.json","survey":"s.json","relays":"r.json","mining":"m.json"}
        })).expect("mission");
        let request = BootstrapExecutionRequest::new("mission.json", Duration::from_secs(9));
        let config = execution_config(&request, &mission, Command::Run);
        assert_eq!(config.region.as_deref(), Some("beta"));
        assert_eq!(config.landing_star.as_deref(), Some("LANDING"));
        assert_eq!(config.wait_timeout, Duration::from_secs(9));
    }
}
