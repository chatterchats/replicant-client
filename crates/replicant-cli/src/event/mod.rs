use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error as StdError,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use replicant_client::{Client, Replicant, Star, SyncDomain, raw};
use replicant_event_planner::{
    BlueprintSpec, CriterionAssessment, DeviceStock, EventDefinition, EventPlan, FactoryWorkload,
    OpenEventFields, PlanningContext, Recommendation, ResourceMap, mission_tag, plan_event,
    role_tag,
};
use replicant_runtime::{config::ManagedClientConfig, start_managed_client};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, prelude::*};

mod campaign;
mod executor;

const PLAN_VERSION: u32 = 2;
const DEFAULT_REPLICANT: &str = "Chats-1";
const DEFAULT_HOME: &str = "SCEPTURUM-BELT-1";
const DEFAULT_PLAN_PATH: &str = "event-mission.json";
const EVENT_MISSION_TAG_PREFIX: &str = "evt-m:";
const AUTOFACTORY: &str = "autofactory";

type AnyError = Box<dyn StdError + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> AnyError {
    io::Error::new(kind, message.into()).into()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Interactive,
    List,
    Plan,
    Run,
    Status,
}

#[derive(Clone, Debug)]
struct Config {
    command: Command,
    event: Option<String>,
    criterion: Option<String>,
    replicant: Option<String>,
    home: String,
    database: PathBuf,
    plan_path: PathBuf,
    replace_plan: bool,
    all_events: bool,
    region: Option<String>,
    center: Option<String>,
    radius_ly: Option<f64>,
    wait_timeout: Duration,
    verbose: bool,
    log_file: Option<PathBuf>,
    json: bool,
}

impl Config {
    fn from_args_and_env(arguments: impl IntoIterator<Item = String>) -> AnyResult<Self> {
        let mut command = Command::Interactive;
        let mut event = None;
        let mut criterion = None;
        let mut replicant = env::var("RS_EVENT_REPLICANT").ok();
        let mut home = env::var("RS_EVENT_HOME").unwrap_or_else(|_| DEFAULT_HOME.into());
        let mut database = PathBuf::from(
            env::var("REPLICANT_DB").unwrap_or_else(|_| "replicant-client.sqlite".into()),
        );
        let mut plan_path =
            PathBuf::from(env::var("RS_EVENT_PLAN").unwrap_or_else(|_| DEFAULT_PLAN_PATH.into()));
        let mut replace_plan = false;
        let mut all_events = false;
        let mut region = env::var("RS_EVENT_REGION").ok();
        let mut center = env::var("RS_EVENT_CENTER").ok();
        let mut radius_ly = env::var("RS_EVENT_RADIUS_LY")
            .ok()
            .map(|value| parse_radius(&value, "RS_EVENT_RADIUS_LY"))
            .transpose()?;
        let mut scope_cli_seen = false;
        let mut wait_timeout = Duration::from_secs(
            env::var("RS_EVENT_WAIT_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(21_600),
        );
        let mut verbose = env_flag("RS_EVENT_VERBOSE");
        let mut log_file = env::var("RS_EVENT_LOG_FILE").ok().map(PathBuf::from);
        let mut json = false;

        let mut arguments = arguments.into_iter().peekable();
        if let Some(first) = arguments.peek().map(String::as_str) {
            command = match first {
                "list" => {
                    arguments.next();
                    Command::List
                }
                "plan" => {
                    arguments.next();
                    Command::Plan
                }
                "run" => {
                    arguments.next();
                    Command::Run
                }
                "status" => {
                    arguments.next();
                    Command::Status
                }
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                _ => Command::Interactive,
            };
        }

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--event" => event = Some(required_argument(&mut arguments, "--event")?),
                "--criterion" => {
                    criterion = Some(required_argument(&mut arguments, "--criterion")?)
                }
                "--replicant" => {
                    replicant = Some(required_argument(&mut arguments, "--replicant")?)
                }
                "--home" => home = required_argument(&mut arguments, "--home")?,
                "--database" => {
                    database = PathBuf::from(required_argument(&mut arguments, "--database")?)
                }
                "--plan-file" => {
                    plan_path = PathBuf::from(required_argument(&mut arguments, "--plan-file")?)
                }
                "--replace-plan" => replace_plan = true,
                "--all" => all_events = true,
                "--region" => {
                    reset_environment_scope(
                        &mut scope_cli_seen,
                        &mut region,
                        &mut center,
                        &mut radius_ly,
                    );
                    region = Some(required_argument(&mut arguments, "--region")?);
                }
                "--center" => {
                    reset_environment_scope(
                        &mut scope_cli_seen,
                        &mut region,
                        &mut center,
                        &mut radius_ly,
                    );
                    center = Some(required_argument(&mut arguments, "--center")?);
                }
                "--radius" | "--radius-ly" => {
                    reset_environment_scope(
                        &mut scope_cli_seen,
                        &mut region,
                        &mut center,
                        &mut radius_ly,
                    );
                    let option = argument.as_str();
                    let value = required_argument(&mut arguments, option)?;
                    radius_ly = Some(parse_radius(&value, option)?);
                }
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
                value if event.is_none() && command == Command::Plan => {
                    event = Some(value.to_owned());
                }
                value => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        format!("unexpected argument: {value}"),
                    ));
                }
            }
        }

        if all_events && command != Command::Plan {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--all belongs on the plan command",
            ));
        }
        if all_events && (event.is_some() || criterion.is_some()) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--all cannot be combined with --event, a positional event, or --criterion",
            ));
        }
        region = region.map(|value| value.trim().to_ascii_lowercase());
        center = center.map(|value| value.trim().to_ascii_uppercase());
        if region.as_deref().is_some_and(str::is_empty) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--region cannot be empty",
            ));
        }
        if center.as_deref().is_some_and(str::is_empty) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--center cannot be empty",
            ));
        }
        if region.is_some() && (center.is_some() || radius_ly.is_some()) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--region cannot be combined with --center or --radius",
            ));
        }
        if center.is_some() && radius_ly.is_none() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--center requires --radius",
            ));
        }

        Ok(Self {
            command,
            event,
            criterion,
            replicant,
            home: home.to_uppercase(),
            database,
            plan_path,
            replace_plan,
            all_events,
            region,
            center,
            radius_ly,
            wait_timeout,
            verbose,
            log_file,
            json,
        })
    }

    fn event_scope(&self) -> EventScope {
        if let Some(region) = &self.region {
            return EventScope::Region {
                region: region.clone(),
            };
        }
        if let Some(radius_ly) = self.radius_ly {
            return EventScope::Radius {
                center_system: system_from_location(
                    self.center.as_deref().unwrap_or(self.home.as_str()),
                ),
                radius_ly,
            };
        }
        EventScope::All
    }
}

fn reset_environment_scope(
    scope_cli_seen: &mut bool,
    region: &mut Option<String>,
    center: &mut Option<String>,
    radius_ly: &mut Option<f64>,
) {
    if *scope_cli_seen {
        return;
    }
    *scope_cli_seen = true;
    *region = None;
    *center = None;
    *radius_ly = None;
}

fn parse_radius(value: &str, option: &str) -> AnyResult<f64> {
    let radius = value.parse::<f64>().map_err(|_| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be a positive number"),
        )
    })?;
    if !radius.is_finite() || radius <= 0.0 {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be a positive finite number"),
        ));
    }
    Ok(radius)
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
        "Replicant event logistics\n\n\
Usage:\n  replicant-cli event\n  replicant-cli event --list [OPTIONS]\n  replicant-cli event --plan [EVENT] [OPTIONS]\n  replicant-cli event --plan --all [OPTIONS]\n  replicant-cli event --run [OPTIONS]\n  replicant-cli event --status [OPTIONS]\n\n\
Options:\n  --event DESIGNATION       Event to plan\n  --criterion NAME          Completion option to select\n  --all                     Plan every active discovered event\n  --region REGION           Limit discovery to a catalogue region\n  --center LOCATION         Radius centre; accepts a star, system, or location\n  --radius LY               Limit discovery to LY around --center or --home\n  --replicant NAME_OR_CODE  Defaults to Chats-1; interactive mode permits selection\n  --home LOCATION           Home/manufacturing hub (default: SCEPTURUM-BELT-1)\n  --database PATH           Managed SQLite database\n  --plan-file PATH          Saved mission or campaign (default: event-mission.json)\n  --replace-plan            Replace an existing active plan\n  --wait-timeout-secs N     Per-phase wait timeout (default: 21600)\n  --verbose                 Show tracing logs in the terminal\n  --log-file PATH           Append tracing logs to a file\n  --json                    Emit machine-readable JSON\n  -h, --help                Show this help\n\n\
Planning performs no gameplay mutations. Run always reconciles and continues\n\
the persisted mission or all-events campaign; there is no separate resume\n\
command or --execute flag. Campaigns start print queues before completing\n\
material-only events while manufacturing continues. Region and radius scopes\n\
are persisted in mission files; --region is mutually exclusive with\n\
--center/--radius."
    );
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum EventScope {
    #[default]
    All,
    Region {
        region: String,
    },
    Radius {
        center_system: String,
        radius_ly: f64,
    },
}

impl EventScope {
    fn description(&self) -> String {
        match self {
            Self::All => "all discovered systems".into(),
            Self::Region { region } => format!("catalogue region {region}"),
            Self::Radius {
                center_system,
                radius_ly,
            } => format!("within {radius_ly:.2} ly of {center_system}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MissionPhase {
    Planned,
    ClaimingTransports,
    Manufacturing,
    PreparingFleet,
    Outbound,
    Staging,
    InstallingBeacon,
    ReadyToResolve,
    Resolving,
    CollectingRewards,
    Returning,
    CleaningUp,
    Completed,
    CompletedWithWarnings,
    #[allow(dead_code)] // Reserved for a future explicit cancellation command.
    Cancelled,
}

impl MissionPhase {
    fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::CompletedWithWarnings | Self::Cancelled
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClaimedDevice {
    device_code: String,
    role: String,
    original_tags: Vec<String>,
    mission_tags: Vec<String>,
    released: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EventMissionPlan {
    version: u32,
    mission_id: String,
    mission_tag: String,
    phase: MissionPhase,
    selected_replicant: String,
    home_location: String,
    #[serde(default)]
    event_scope: EventScope,
    event: EventDefinition,
    selected_criterion: CriterionAssessment,
    grants_unearned_achievement: bool,
    #[serde(default)]
    claimed_devices: Vec<ClaimedDevice>,
    #[serde(default)]
    execution: executor::ExecutionState,
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
                                "another event executor holds {} (pid {})",
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
                "no event mission exists at {}; create one with `replicant-cli event --plan ...`",
                config.plan_path.display()
            ),
        ));
    }

    let _mission_lock = if config.command == Command::Run {
        Some(MissionLock::acquire(&config.plan_path)?)
    } else {
        None
    };
    let client = start_managed_client(ManagedClientConfig::from_env(&config.database)?).await?;
    let result = run(&client, &config).await;
    let close_result = client.close().await;
    close_result?;
    result
}

fn init_logging(config: &Config) -> AnyResult<()> {
    if !config.verbose && config.log_file.is_none() {
        return Ok(());
    }
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("replicant_client=info,replicant_cli::event=info,info"));

    match (&config.log_file, config.verbose) {
        (None, true) => {
            tracing_subscriber::registry()
                .with(filter)
                .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
                .try_init()
                .map_err(|error| app_error(io::ErrorKind::Other, error.to_string()))?;
        }
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

async fn run(client: &Client, config: &Config) -> AnyResult<()> {
    // `Essential` startup already performs the authoritative Account + Devices
    // baseline and event-log catch-up used by every event command. Avoid a
    // second full sync: walking every known Location is unrelated to event
    // execution and can collide with survey scans in another workflow.
    client.ready().await?;
    info!(
        readiness = ?client.readiness(),
        "managed essential startup completed for event command"
    );

    if config.command == Command::Run {
        if campaign::is_campaign_file(&config.plan_path)? {
            let mut plan = campaign::load_campaign(&config.plan_path)?;
            campaign::execute_campaign(client, config, &mut plan).await?;
            return Ok(());
        }
        let mut plan = load_plan(&config.plan_path)?;
        executor::execute_saved_plan(client, config, &mut plan).await?;
        return Ok(());
    }

    // Planning needs the owned replicant roster, but event discovery,
    // inventories, blueprints, and scoped star-catalogue data are already read
    // through their targeted APIs below. Listing does not need replicants at
    // all, so it can stay on the Essential baseline alone.
    if config.command != Command::List {
        let sync = client.sync().domain(SyncDomain::Replicants).await?;
        info!(
            readiness = ?sync.readiness,
            "refreshed owned replicants for event planning"
        );
    }

    let event_scope = config.event_scope();
    let events = fetch_active_events_in_scope(client, &event_scope).await?;
    let earned = fetch_earned_achievements(client).await?;
    if events.is_empty() {
        println!(
            "No active discovered location events were found in {}.",
            event_scope.description()
        );
        return Ok(());
    }

    if config.command == Command::List {
        let definitions = events
            .iter()
            .map(normalize_event)
            .collect::<Result<Vec<_>, _>>()?;
        if config.json {
            println!("{}", serde_json::to_string_pretty(&definitions)?);
        } else {
            println!("Scope: {}\n", event_scope.description());
            print_event_table(&definitions, &earned);
        }
        return Ok(());
    }

    if config.all_events {
        let definitions = events
            .iter()
            .map(normalize_event)
            .collect::<Result<Vec<_>, _>>()?;
        campaign::create_campaign(client, config, definitions, &earned).await?;
        return Ok(());
    }

    if config.command == Command::Interactive && !config.json {
        println!("Scope: {}\n", event_scope.description());
    }
    let selected_event = select_event(&events, config.event.as_deref(), config.command, &earned)?;
    let event = normalize_event(selected_event)?;
    let replicant = select_replicant(client, config.replicant.as_deref(), config.command).await?;
    let replicant_code = replicant.key.id.as_str().to_owned();

    if config.plan_path.exists() && !config.replace_plan {
        if campaign::is_campaign_file(&config.plan_path)? {
            return Err(app_error(
                io::ErrorKind::AlreadyExists,
                format!(
                    "an all-events campaign already exists at {}; use run, status, or plan --replace-plan",
                    config.plan_path.display()
                ),
            ));
        }
        let existing = load_plan(&config.plan_path)?;
        if !existing.phase.is_terminal() {
            return Err(app_error(
                io::ErrorKind::AlreadyExists,
                format!(
                    "active mission {} already exists at {}; use status or --replace-plan",
                    existing.mission_id,
                    config.plan_path.display()
                ),
            ));
        }
    }

    let context = build_context(client, &event, &earned, &config.home).await?;
    let event_plan = plan_event(event, &context)?;
    let selected_criterion = select_criterion(
        &event_plan,
        config.criterion.as_deref(),
        config.command == Command::Interactive || config.criterion.is_none(),
    )?;
    let mission_id = uuid::Uuid::new_v4().simple().to_string();
    let plan = EventMissionPlan {
        version: PLAN_VERSION,
        mission_id: mission_id.clone(),
        mission_tag: mission_tag(&mission_id),
        phase: MissionPhase::Planned,
        selected_replicant: replicant_code,
        home_location: config.home.clone(),
        event_scope: event_scope.clone(),
        event: event_plan.event.clone(),
        selected_criterion,
        grants_unearned_achievement: event_plan.grants_unearned_achievement,
        claimed_devices: Vec::new(),
        execution: executor::ExecutionState::default(),
    };
    save_plan(&config.plan_path, &plan)?;

    if config.json {
        println!("{}", serde_json::to_string_pretty(&plan)?);
    } else {
        print_event_plan(&event_plan);
        println!(
            "\nSelected criterion: {}",
            plan.selected_criterion.criterion_name
        );
        println!("Replicant: {}", plan.selected_replicant);
        println!("Scope: {}", plan.event_scope.description());
        println!("Saved plan: {}", config.plan_path.display());
        println!(
            "Mission tags reserved for execution: {}, {}, {}",
            plan.mission_tag,
            role_tag("cargo"),
            role_tag("carrier")
        );
        println!("Execute with: replicant-cli event --run");
    }
    Ok(())
}

async fn fetch_active_events(client: &Client) -> AnyResult<Vec<raw::events::LocationEvent>> {
    let mut cursor = None;
    let mut events = Vec::new();
    for _ in 0..100 {
        let response = client
            .raw()
            .accounts()
            .events(&raw::accounts::AccountEventsQuery {
                status: Some("active".into()),
                cursor,
                limit: Some(100),
            })
            .await?
            .value;
        events.extend(response.events);
        let Some(next) = response.next_cursor else {
            events.sort_by(|left, right| {
                left.location
                    .cmp(&right.location)
                    .then_with(|| left.title.cmp(&right.title))
                    .then_with(|| left.designation.cmp(&right.designation))
            });
            return Ok(events);
        };
        cursor = Some(next);
    }
    Err(app_error(
        io::ErrorKind::InvalidData,
        "event listing exceeded the 100-page safety bound",
    ))
}

async fn fetch_active_events_in_scope(
    client: &Client,
    scope: &EventScope,
) -> AnyResult<Vec<raw::events::LocationEvent>> {
    let events = fetch_active_events(client).await?;
    if !matches!(scope, EventScope::All) {
        client.galaxy().refresh_catalogue().await?;
    }
    filter_events_to_scope(events, client.galaxy().catalogue(), scope)
}

#[derive(Clone, Debug)]
struct ScopeStar {
    region: Option<String>,
    position: Option<(f64, f64, f64)>,
}

fn filter_events_to_scope(
    events: Vec<raw::events::LocationEvent>,
    stars: Vec<Star>,
    scope: &EventScope,
) -> AnyResult<Vec<raw::events::LocationEvent>> {
    if matches!(scope, EventScope::All) {
        return Ok(events);
    }

    let catalogue = stars
        .into_iter()
        .map(|star| {
            let position = star
                .position
                .map(|position| (position.x, position.y, position.z));
            (
                star.key.id.as_str().to_ascii_uppercase(),
                ScopeStar {
                    region: star.region,
                    position,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    let center = match scope {
        EventScope::Radius { center_system, .. } => {
            let star = catalogue.get(center_system).ok_or_else(|| {
                app_error(
                    io::ErrorKind::NotFound,
                    format!(
                        "event scope centre {center_system:?} is not present in the star catalogue"
                    ),
                )
            })?;
            Some(star.position.ok_or_else(|| {
                app_error(
                    io::ErrorKind::InvalidData,
                    format!("event scope centre {center_system:?} has no catalogue coordinates"),
                )
            })?)
        }
        EventScope::All | EventScope::Region { .. } => None,
    };

    let mut included = Vec::new();
    let mut outside = 0usize;
    let mut unresolved = 0usize;
    for event in events {
        let Some(location) = event.location.as_deref() else {
            unresolved += 1;
            continue;
        };
        let system = system_from_location(location);
        let Some(star) = catalogue.get(&system) else {
            unresolved += 1;
            continue;
        };
        let matches = match scope {
            EventScope::All => true,
            EventScope::Region { region } => star
                .region
                .as_deref()
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(region)),
            EventScope::Radius { radius_ly, .. } => {
                let Some(position) = star.position else {
                    unresolved += 1;
                    continue;
                };
                let Some(center) = center else {
                    return Err(app_error(
                        io::ErrorKind::InvalidData,
                        "radius event scope did not resolve its centre coordinates",
                    ));
                };
                euclidean_distance_ly(center, position) <= *radius_ly
            }
        };
        if matches {
            included.push(event);
        } else {
            outside += 1;
        }
    }

    if unresolved > 0 {
        warn!(
            scope = %scope.description(),
            unresolved,
            "excluded active events whose system catalogue data was incomplete"
        );
    }
    info!(
        scope = %scope.description(),
        included = included.len(),
        outside,
        unresolved,
        "filtered active events to the configured operating scope"
    );
    Ok(included)
}

fn euclidean_distance_ly(left: (f64, f64, f64), right: (f64, f64, f64)) -> f64 {
    let dx = left.0 - right.0;
    let dy = left.1 - right.1;
    let dz = left.2 - right.2;
    (dx.mul_add(dx, dy.mul_add(dy, dz * dz))).sqrt()
}

async fn fetch_earned_achievements(client: &Client) -> AnyResult<BTreeSet<String>> {
    Ok(client
        .raw()
        .accounts()
        .achievements()
        .await?
        .value
        .achievements
        .into_iter()
        .filter_map(|achievement| achievement.achievement_key)
        .collect())
}

fn normalize_event(raw_event: &raw::events::LocationEvent) -> AnyResult<EventDefinition> {
    let designation = raw_event.designation.clone().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            "location event omitted designation",
        )
    })?;
    let location = raw_event.location.clone().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            "location event omitted location",
        )
    })?;
    let title = raw_event
        .title
        .clone()
        .unwrap_or_else(|| designation.clone());
    Ok(EventDefinition::from_open_fields(OpenEventFields {
        designation,
        location,
        title,
        description: raw_event.description.clone(),
        event_type: raw_event.event_type.clone(),
        tier: raw_event.tier,
        status: raw_event.status.clone(),
        criteria: raw_event.criteria.as_deref().unwrap_or_default(),
        progress: raw_event.progress.as_ref(),
        rewards: raw_event.rewards.as_ref(),
    })?)
}

async fn select_replicant(
    client: &Client,
    requested: Option<&str>,
    command: Command,
) -> AnyResult<Replicant> {
    let handles = client.replicants().find().owned().collect().await?;
    let mut replicants = Vec::new();
    for handle in handles {
        replicants.push(handle.snapshot().await?);
    }
    replicants.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.key.id.as_str().cmp(right.key.id.as_str()))
    });
    if let Some(requested) = requested {
        return resolve_replicant(&replicants, requested);
    }
    if command != Command::Interactive {
        return resolve_replicant(&replicants, DEFAULT_REPLICANT);
    }
    println!("\nReplicants:");
    let default_index = replicants
        .iter()
        .position(|replicant| {
            replicant
                .name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(DEFAULT_REPLICANT))
        })
        .unwrap_or(0);
    for (index, replicant) in replicants.iter().enumerate() {
        println!(
            "  {:>2}. {:<20} {}{}",
            index + 1,
            replicant.name.as_deref().unwrap_or("<unnamed>"),
            replicant.key.id.as_str(),
            if index == default_index {
                " [default]"
            } else {
                ""
            }
        );
    }
    let selected = prompt_index("Select replicant", replicants.len(), default_index + 1)?;
    Ok(replicants.remove(selected - 1))
}

fn resolve_replicant(replicants: &[Replicant], requested: &str) -> AnyResult<Replicant> {
    let mut matches = replicants
        .iter()
        .filter(|replicant| {
            replicant.key.id.as_str().eq_ignore_ascii_case(requested)
                || replicant
                    .name
                    .as_deref()
                    .is_some_and(|name| name.eq_ignore_ascii_case(requested))
        })
        .cloned()
        .collect::<Vec<_>>();
    match matches.len() {
        1 => Ok(matches.remove(0)),
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

fn select_event<'a>(
    events: &'a [raw::events::LocationEvent],
    requested: Option<&str>,
    command: Command,
    earned: &BTreeSet<String>,
) -> AnyResult<&'a raw::events::LocationEvent> {
    if let Some(requested) = requested {
        return events
            .iter()
            .find(|event| {
                event
                    .designation
                    .as_deref()
                    .is_some_and(|value| value.eq_ignore_ascii_case(requested))
            })
            .ok_or_else(|| {
                app_error(
                    io::ErrorKind::NotFound,
                    format!("active event {requested:?} was not found"),
                )
            });
    }
    if command != Command::Interactive {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "plan requires an event designation when not interactive",
        ));
    }
    let definitions = events
        .iter()
        .map(normalize_event)
        .collect::<Result<Vec<_>, _>>()?;
    print_event_table(&definitions, earned);
    let selected = prompt_index("Select event", events.len(), 1)?;
    Ok(&events[selected - 1])
}

fn select_criterion(
    plan: &EventPlan,
    requested: Option<&str>,
    interactive: bool,
) -> AnyResult<CriterionAssessment> {
    if let Some(requested) = requested {
        let criterion = plan
            .criteria
            .iter()
            .find(|criterion| criterion.criterion_name.eq_ignore_ascii_case(requested))
            .cloned()
            .ok_or_else(|| {
                app_error(
                    io::ErrorKind::NotFound,
                    format!("criterion {requested:?} was not found"),
                )
            })?;
        return require_feasible_criterion(criterion);
    }
    if plan.criteria.len() == 1 {
        return require_feasible_criterion(plan.criteria[0].clone());
    }
    if !interactive {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "event has multiple criteria; use --criterion",
        ));
    }
    print_event_plan(plan);
    loop {
        let selected = prompt_index("Select completion option", plan.criteria.len(), 1)?;
        let criterion = plan.criteria[selected - 1].clone();
        if criterion.feasible {
            return Ok(criterion);
        }
        println!("That option is blocked: {}", criterion.blockers.join("; "));
    }
}

fn require_feasible_criterion(criterion: CriterionAssessment) -> AnyResult<CriterionAssessment> {
    if criterion.feasible {
        Ok(criterion)
    } else {
        Err(app_error(
            io::ErrorKind::InvalidInput,
            format!(
                "criterion {:?} is blocked: {}",
                criterion.criterion_name,
                criterion.blockers.join("; ")
            ),
        ))
    }
}

async fn build_context(
    client: &Client,
    event: &EventDefinition,
    earned: &BTreeSet<String>,
    home: &str,
) -> AnyResult<PlanningContext> {
    let blueprints = fetch_blueprints(client).await?;
    let mut devices = fetch_devices(client, &blueprints, home).await?;
    hydrate_factory_workloads(client, &mut devices, home).await?;
    let home_inventory = fetch_inventory(client, home).await?;
    let event_inventory = fetch_inventory(client, &event.location).await?;
    let factories = build_factory_workloads(&devices, &blueprints, home);
    Ok(PlanningContext {
        home_inventory,
        event_inventory,
        blueprints,
        devices: devices.into_iter().map(|item| item.stock).collect(),
        factories,
        earned_achievements: earned.clone(),
        home_location: home.to_owned(),
        mission_tag_prefix: EVENT_MISSION_TAG_PREFIX.into(),
    })
}

#[derive(Clone, Debug)]
struct LiveDevice {
    stock: DeviceStock,
    printing_eta_seconds: f64,
    print_queue: Vec<Map<String, Value>>,
}

async fn fetch_devices(
    client: &Client,
    blueprints: &BTreeMap<String, BlueprintSpec>,
    home: &str,
) -> AnyResult<Vec<LiveDevice>> {
    // Event planning and runtime replanning are home-system scoped by policy:
    // payload stock must be at the exact hub, while eligible transports may
    // start elsewhere in that same system. The Essential managed startup has
    // already refreshed owned devices, so use the committed projection rather
    // than paging the entire account again.
    let handles = client
        .devices()
        .find()
        .owned()
        .in_system(system_from_location(home))
        .collect()
        .await?;
    let mut devices = Vec::with_capacity(handles.len());
    for handle in handles {
        let device = handle.snapshot().await?;
        let Some(device_type) = device
            .device_type
            .as_ref()
            .map(|value| value.as_str().to_owned())
        else {
            continue;
        };
        let blueprint = blueprints.get(&device_type);
        devices.push(LiveDevice {
            stock: DeviceStock {
                code: handle.id().as_str().to_owned(),
                device_type,
                status: device
                    .status
                    .as_ref()
                    .map(|value| value.as_str().to_owned()),
                location: device
                    .location
                    .as_ref()
                    .map(|value| value.id.as_str().to_owned()),
                assigned_replicant: device
                    .relationships
                    .assigned_replicant
                    .as_ref()
                    .map(|value| value.id.as_str().to_owned()),
                tags: device.tags.into_iter().collect(),
                cargo_capacity: blueprint.map_or(0, |item| item.cargo_capacity),
                attach_capacity: device
                    .attach_capacity
                    .or_else(|| blueprint.map(|item| item.attach_capacity))
                    .unwrap_or(0),
                attach_used: i64::try_from(device.relationships.attached_devices.len())?,
                attached_to_device_code: device
                    .relationships
                    .attached_to
                    .as_ref()
                    .map(|value| value.id.as_str().to_owned()),
                stowed_in_device_code: device
                    .relationships
                    .stowed_in
                    .as_ref()
                    .map(|value| value.id.as_str().to_owned()),
                controlled_by_ami: device.relationships.controller.is_some(),
                travelling: device.travel.is_some(),
            },
            printing_eta_seconds: 0.0,
            print_queue: Vec::new(),
        });
    }
    devices.sort_by(|left, right| left.stock.code.cmp(&right.stock.code));
    Ok(devices)
}

async fn hydrate_factory_workloads(
    client: &Client,
    devices: &mut [LiveDevice],
    home: &str,
) -> AnyResult<()> {
    for device in devices.iter_mut().filter(|device| {
        device.stock.device_type == AUTOFACTORY && device.stock.location.as_deref() == Some(home)
    }) {
        let detail = client.raw().devices().get(&device.stock.code).await?.value;
        if detail.status.is_some() {
            device.stock.status = detail.status.clone();
        }
        device.printing_eta_seconds = detail
            .printing
            .and_then(|printing| printing.eta_seconds)
            .unwrap_or(0.0);
        device.print_queue = detail.print_queue;
    }
    Ok(())
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
                    cargo_capacity: blueprint.cargo_capacity.unwrap_or(0),
                    attach_capacity: blueprint.attach_capacity.unwrap_or(0),
                    stow_capacity: blueprint.stow_capacity.unwrap_or(0),
                    resources: numeric_map(blueprint.resources.as_ref()),
                    components: numeric_map(blueprint.components.as_ref()),
                    features: blueprint.features.unwrap_or_default().into_iter().collect(),
                },
            ))
        })
        .collect())
}

async fn fetch_inventory(client: &Client, location: &str) -> AnyResult<ResourceMap> {
    let (inventories, _) = client
        .inventory()
        .list(&raw::inventory::AccountInventoryQuery {
            location: Some(location.to_owned()),
            cursor: None,
            limit: Some(50),
        })
        .await?;
    Ok(inventories
        .into_iter()
        .find(|inventory| {
            inventory
                .location
                .as_ref()
                .is_some_and(|item| item.id.as_str() == location)
        })
        .map(|inventory| {
            inventory
                .items
                .into_iter()
                .map(|item| (item.resource, item.quantity))
                .collect()
        })
        .unwrap_or_default())
}

fn build_factory_workloads(
    devices: &[LiveDevice],
    blueprints: &BTreeMap<String, BlueprintSpec>,
    home: &str,
) -> Vec<FactoryWorkload> {
    let mut workloads = devices
        .iter()
        .filter(|device| {
            device.stock.device_type == AUTOFACTORY
                && device.stock.location.as_deref() == Some(home)
                && !factory_status_blocks_printing(device.stock.status.as_deref())
        })
        .map(|device| {
            let queued = device
                .print_queue
                .iter()
                .map(|job| {
                    let device_type = string_field(job, &["device_type", "type"]);
                    let quantity = integer_field(job, &["quantity", "count"])
                        .unwrap_or(1)
                        .max(1);
                    device_type
                        .and_then(|device_type| blueprints.get(device_type))
                        .map(|blueprint| blueprint.print_time_seconds * quantity as f64)
                        .unwrap_or(0.0)
                })
                .sum::<f64>();
            FactoryWorkload {
                code: device.stock.code.clone(),
                remaining_seconds: device.printing_eta_seconds + queued,
            }
        })
        .collect::<Vec<_>>();
    workloads.sort_by(|left, right| left.code.cmp(&right.code));
    workloads
}

fn numeric_map(object: Option<&Map<String, Value>>) -> BTreeMap<String, i64> {
    let Some(object) = object else {
        return BTreeMap::new();
    };
    object
        .iter()
        .filter_map(|(key, value)| value_to_i64(value).map(|amount| (key.clone(), amount)))
        .filter(|(_, amount)| *amount > 0)
        .collect()
}

fn value_to_i64(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| {
        value
            .as_u64()
            .and_then(|number| i64::try_from(number).ok())
            .or_else(|| value.as_f64().map(|number| number.round() as i64))
    })
}

fn string_field<'a>(object: &'a Map<String, Value>, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(Value::as_str))
}

fn integer_field(object: &Map<String, Value>, keys: &[&str]) -> Option<i64> {
    keys.iter()
        .find_map(|key| object.get(*key).and_then(value_to_i64))
}

fn print_event_table(events: &[EventDefinition], earned: &BTreeSet<String>) {
    println!(
        "{:<3} {:<13} {:<30} {:<5} {:<22} {:<10} {:<22} FLAGS",
        "#", "SYS", "TITLE", "WAYS", "MATERIALS", "DEVICES", "REWARD"
    );
    println!("{}", "-".repeat(122));
    for (index, event) in events.iter().enumerate() {
        let materials = event
            .criteria
            .iter()
            .map(|criterion| format_compact_resources(&criterion.resources))
            .collect::<Vec<_>>()
            .join(" | ");
        let devices = event
            .criteria
            .iter()
            .map(|criterion| {
                criterion
                    .devices
                    .iter()
                    .map(|item| item.count)
                    .sum::<i64>()
                    .to_string()
            })
            .collect::<Vec<_>>()
            .join(" | ");
        let reward = format_compact_resources(&event.rewards.resources);
        let flags = event
            .rewards
            .completion_achievement
            .as_ref()
            .filter(|achievement| !earned.contains(*achievement))
            .map(|_| "NEW ACH")
            .unwrap_or("");
        println!(
            "{:<3} {:<13} {:<30} {:<5} {:<22} {:<10} {:<22} {}",
            index + 1,
            truncate(&system_from_location(&event.location), 13),
            truncate(&event.title, 30),
            event.criteria.len(),
            truncate(&materials, 22),
            truncate(&devices, 10),
            truncate(&reward, 22),
            flags
        );
    }
    println!("\nReward rarity note: Rares > Volatiles > Conductive.");
}

fn print_event_plan(plan: &EventPlan) {
    println!("\n{}", plan.event.title);
    println!("{} · {}", plan.event.location, plan.event.designation);
    if plan.grants_unearned_achievement {
        println!("Achievement: NOT YET EARNED");
    }
    println!(
        "Reward: {}",
        format_resources(&plan.event.rewards.resources)
    );
    if let Some(xp) = plan.event.rewards.xp {
        println!("XP: {xp}");
    }
    println!("\nCompletion options:");
    for (index, criterion) in plan.criteria.iter().enumerate() {
        println!("\n  {}. {}", index + 1, criterion.criterion_name);
        println!(
            "     Status:               {}",
            if criterion.feasible {
                "FEASIBLE"
            } else {
                "BLOCKED"
            }
        );
        println!(
            "     Remaining materials: {}",
            format_resources(&criterion.remaining_resources)
        );
        println!(
            "     Remaining devices:   {}",
            format_devices(&criterion.remaining_devices)
        );
        println!(
            "     Reused / printed:     {} / {}",
            criterion.reused_devices.len(),
            criterion.print_count()
        );
        println!(
            "     Cargo:                {} transport(s), {} inbound trip(s), {} reward trip(s)",
            criterion.cargo.transports.len(),
            criterion.cargo.inbound_trips,
            criterion.cargo.outbound_trips
        );
        println!(
            "     Device carrier:       {} transport(s), {} trip(s)",
            criterion.carriers.transports.len(),
            criterion.carriers.inbound_trips
        );
        println!("     Beacon:               {:?}", criterion.beacon.action);
        println!(
            "     Print ready estimate: {}",
            format_duration(criterion.print_schedule.makespan_seconds)
        );
        if !criterion.recommendations.is_empty() {
            println!(
                "     Badges:               {}",
                criterion
                    .recommendations
                    .iter()
                    .map(recommendation_label)
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        for blocker in &criterion.blockers {
            println!("     Blocker:              {blocker}");
        }
        for warning in &criterion.warnings {
            println!("     Warning:              {warning}");
        }
        for batch in &criterion.print_schedule.batches {
            println!(
                "     Print:                {} × {} on {} (factory finish {})",
                batch.quantity,
                batch.device_type,
                batch.factory_code,
                format_duration(batch.projected_finish_seconds)
            );
        }
    }
}

fn recommendation_label(recommendation: &Recommendation) -> &'static str {
    match recommendation {
        Recommendation::Fastest => "FASTEST",
        Recommendation::LowestManufacturingCost => "LOWEST COST",
        Recommendation::LowestRareResourceUse => "LOWEST RARE USE",
        Recommendation::FewestPrints => "FEWEST PRINTS",
        Recommendation::FewestTrips => "FEWEST TRIPS",
        Recommendation::UsesExistingStockBest => "USES STOCK",
    }
}

fn format_compact_resources(resources: &ResourceMap) -> String {
    if resources.is_empty() {
        return "—".into();
    }
    resources
        .iter()
        .map(|(resource, quantity)| format!("{quantity} {}", resource_abbreviation(resource)))
        .collect::<Vec<_>>()
        .join(" + ")
}

fn resource_abbreviation(resource: &str) -> String {
    match resource.to_ascii_lowercase().as_str() {
        "carbon" => "Car".into(),
        "conductive" => "Con".into(),
        "silicate" | "silicates" => "Sil".into(),
        "structural" => "Str".into(),
        "volatile" | "volatiles" => "Vol".into(),
        "rare" | "rares" => "Rar".into(),
        _ => display_name(resource).chars().take(3).collect(),
    }
}

fn format_resources(resources: &ResourceMap) -> String {
    if resources.is_empty() {
        return "—".into();
    }
    resources
        .iter()
        .map(|(resource, quantity)| format!("{quantity} {}", display_name(resource)))
        .collect::<Vec<_>>()
        .join(" + ")
}

fn format_devices(devices: &[replicant_event_planner::DeviceRequirement]) -> String {
    if devices.is_empty() {
        return "—".into();
    }
    devices
        .iter()
        .map(|item| format!("{} × {}", item.count, display_name(&item.device_type)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn display_name(value: &str) -> String {
    value
        .split('_')
        .map(|word| {
            let mut characters = word.chars();
            match characters.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + characters.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn factory_status_blocks_printing(status: Option<&str>) -> bool {
    status.is_some_and(|status| {
        status.eq_ignore_ascii_case("compacted")
            || status.eq_ignore_ascii_case("compacting")
            || status.eq_ignore_ascii_case("unfurling")
    })
}

fn system_from_location(location: &str) -> String {
    location
        .split('-')
        .next()
        .filter(|system| !system.is_empty())
        .unwrap_or(location)
        .to_ascii_uppercase()
}

fn truncate(value: &str, width: usize) -> String {
    if value.chars().count() <= width {
        return value.to_owned();
    }
    value
        .chars()
        .take(width.saturating_sub(1))
        .collect::<String>()
        + "…"
}

fn format_duration(seconds: f64) -> String {
    let total = seconds.max(0.0).round() as u64;
    let hours = total / 3_600;
    let minutes = (total % 3_600) / 60;
    let seconds = total % 60;
    if hours > 0 {
        format!("{hours}h {minutes:02}m {seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m {seconds:02}s")
    } else {
        format!("{seconds}s")
    }
}

fn prompt_index(prompt: &str, maximum: usize, default: usize) -> AnyResult<usize> {
    let stdin = io::stdin();
    let mut line = String::new();
    loop {
        print!("{prompt} [{default}]: ");
        io::stdout().flush()?;
        line.clear();
        stdin.lock().read_line(&mut line)?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(default);
        }
        if let Ok(value) = trimmed.parse::<usize>()
            && (1..=maximum).contains(&value)
        {
            return Ok(value);
        }
        println!("Enter a number from 1 to {maximum}.");
    }
}

fn save_plan(path: &Path, plan: &EventMissionPlan) -> AnyResult<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    let file = File::create(&temporary)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, plan)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    writer.get_ref().sync_all()?;
    fs::rename(&temporary, path)?;
    Ok(())
}

fn load_plan(path: &Path) -> AnyResult<EventMissionPlan> {
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn show_status(config: &Config) -> AnyResult<()> {
    if !config.plan_path.exists() {
        println!("No mission plan exists at {}.", config.plan_path.display());
        return Ok(());
    }
    if campaign::is_campaign_file(&config.plan_path)? {
        let campaign = campaign::load_campaign(&config.plan_path)?;
        return campaign::show_campaign_status(config, &campaign);
    }
    let plan = load_plan(&config.plan_path)?;
    if config.json {
        println!("{}", serde_json::to_string_pretty(&plan)?);
        return Ok(());
    }
    println!("Mission:    {}", plan.mission_id);
    println!("Phase:      {:?}", plan.phase);
    println!("Event:      {}", plan.event.designation);
    println!("Criterion:  {}", plan.selected_criterion.criterion_name);
    println!("Replicant:  {}", plan.selected_replicant);
    println!("Home:       {}", plan.home_location);
    println!("Scope:      {}", plan.event_scope.description());
    println!("Mission tag: {}", plan.mission_tag);
    println!(
        "Prints:     {}/{} produced",
        plan.execution
            .print_batches
            .iter()
            .map(|batch| batch.produced_codes.len())
            .sum::<usize>(),
        plan.execution
            .print_batches
            .iter()
            .filter_map(|batch| usize::try_from(batch.quantity).ok())
            .sum::<usize>()
    );
    println!(
        "Claims:     {}/{} released",
        plan.claimed_devices
            .iter()
            .filter(|claim| claim.released)
            .count(),
        plan.claimed_devices.len()
    );
    if !plan.execution.warnings.is_empty() {
        println!("Warnings:");
        for warning in &plan.execution.warnings {
            println!("  - {warning}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalogue_star(designation: &str, region: Option<&str>, position: (f64, f64, f64)) -> Star {
        serde_json::from_value(serde_json::json!({
            "key": {"realm": "Live", "id": designation},
            "name": null,
            "spectral_type": null,
            "entry_point": null,
            "position": {"x": position.0, "y": position.1, "z": position.2},
            "has_hub": null,
            "region": region,
        }))
        .expect("catalogue star")
    }

    fn location_event(designation: &str, location: &str) -> raw::events::LocationEvent {
        serde_json::from_value(serde_json::json!({
            "designation": designation,
            "location": location,
        }))
        .expect("location event")
    }

    #[test]
    fn compacted_autofactories_are_not_event_print_workloads() {
        assert!(factory_status_blocks_printing(Some("compacted")));
        assert!(factory_status_blocks_printing(Some("compacting")));
        assert!(factory_status_blocks_printing(Some("unfurling")));
        assert!(!factory_status_blocks_printing(Some("idle")));
        assert!(!factory_status_blocks_printing(Some(
            "waiting_for_resources"
        )));
    }

    #[test]
    fn location_designations_resolve_to_their_star_system() {
        assert_eq!(system_from_location("SCEPTURUM"), "SCEPTURUM");
        assert_eq!(system_from_location("SCEPTURUM-7"), "SCEPTURUM");
        assert_eq!(system_from_location("SCEPTURUM-7-L4"), "SCEPTURUM");
        assert_eq!(system_from_location("SCEPTURUM-BELT-1"), "SCEPTURUM");
    }

    #[test]
    fn region_scope_uses_catalogue_membership() {
        let events = vec![
            location_event("near-hub", "SCEPTURUM-7"),
            location_event("alpha", "WIHAX-3"),
            location_event("beta", "RHWYRHYR-5"),
        ];
        let stars = vec![
            catalogue_star("SCEPTURUM", None, (0.0, 0.0, 0.0)),
            catalogue_star("WIHAX", Some("alpha"), (20.0, 0.0, 0.0)),
            catalogue_star("RHWYRHYR", Some("beta"), (300.0, 0.0, 0.0)),
        ];
        let filtered = filter_events_to_scope(
            events,
            stars,
            &EventScope::Region {
                region: "alpha".into(),
            },
        )
        .expect("region filter");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].designation.as_deref(), Some("alpha"));
    }

    #[test]
    fn radius_scope_can_include_unregioned_hub_neighbors() {
        let events = vec![
            location_event("hub", "SCEPTURUM-7"),
            location_event("near", "ILPHARD-3"),
            location_event("far", "RHWYRHYR-5"),
        ];
        let stars = vec![
            catalogue_star("SCEPTURUM", None, (0.0, 0.0, 0.0)),
            catalogue_star("ILPHARD", None, (18.0, 0.0, 0.0)),
            catalogue_star("RHWYRHYR", Some("beta"), (290.0, 0.0, 0.0)),
        ];
        let filtered = filter_events_to_scope(
            events,
            stars,
            &EventScope::Radius {
                center_system: "SCEPTURUM".into(),
                radius_ly: 35.0,
            },
        )
        .expect("radius filter");
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].designation.as_deref(), Some("hub"));
        assert_eq!(filtered[1].designation.as_deref(), Some("near"));
    }
}
