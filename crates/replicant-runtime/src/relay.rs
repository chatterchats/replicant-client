//! Plans and runs an account-owned FTL relay expansion.
//!
//! This service uses the pure `replicant-route-planner` workspace crate for an
//! exact minimum-new-relay Steiner tree. Newly manufactured FTL relays use the
//! configured conventional range (7.499 ly by default). Already deployed
//! relay-capable devices can extend individual systems using their advertised
//! `/network` range, and disconnected 7.499-10 ly catalogue gaps can be bridged
//! with Deep Space Relay Stations when reusable stock exists or that blueprint
//! is unlocked. Managed client operations perform all mutations, and relay
//! manufacturing, including recursive prerequisite devices, is delegated to the
//! shared `replicant-printing` crate.
//!
//! ```text
//! cargo run --quiet -p replicant-cli -- relay plan \
//!   --replicant Chats-1 \
//!   --hub SCEPTURUM-BELT-1 \
//!   WIHAX ILPHARD KRAKHUX XHAKKWUKKXHU XIHAKHXA XHAKHKHU
//!
//! cargo run --quiet -p replicant-cli -- relay run
//! ```
//!
//! Environment:
//!
//! - `RS_API_TOKEN` (required)
//! - `REPLICANT_DB=~/.local/share/replicant/replicant-client.sqlite`
//! - `RS_RELAY_REPLICANT=Chats-1`
//! - `RS_RELAY_HUB=SCEPTURUM-BELT-1`
//! - `RS_RELAY_PLAN=ftl-relay-expansion.json`
//! - `RS_RELAY_REPLACE_PLAN=1`
//! - `RS_RELAY_REUSE_ACCOUNT_RELAYS=1`
//! - `RS_RELAY_IGNORE_PRINTERS=FF259175,E71BC14B`
//! - `RS_RELAY_SUPPLY_STRATEGY=auto`
//! - `RS_RELAY_WAIT_TIMEOUT_SECS=21600`

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error as StdError,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use crate::{
    config::ManagedClientConfig,
    failure::{FailureClass, classified_error},
    orchestration::REGION_GATEWAY_HUB_RANGE_LY,
    start_managed_client,
};
use replicant_client::{
    Client, Device, DeviceHandle, DeviceType, Operation, OperationId, OperationStatus, Replicant,
    raw,
};
// Workflow device claims are shared vocabulary; see
// `replicant_protocol::RESERVED_WORKFLOW_TAG_PREFIXES`.
use replicant_printing::{
    Blueprint as PrintingBlueprint, FactoryWorkload, PrintRequest, QuantityMap,
    managed::{
        FactoryState, PrintingError, QueueOptions, discover_factories as discover_print_factories,
        discover_factory_codes as discover_print_factory_codes,
        enqueue_print as enqueue_shared_print,
        enqueue_print_flatpacked as enqueue_shared_print_flatpacked,
        fetch_blueprints as fetch_print_blueprints, printing_status_in_system,
        queue_print_prerequisites_ahead,
    },
    schedule_prints,
};
use replicant_protocol::{workflow_reserved, workflow_tag_reserved};
use replicant_route_planner::{
    NetworkNode, PlannerError, Position, RelayAvailability, RelayNetworkPlan, RelayNetworkRequest,
    Star as PlannerStar, plan_relay_network_with_ranges,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::time::{Instant, sleep, timeout};
use tracing::{debug, info, warn};
use tracing_subscriber::{EnvFilter, prelude::*};

tokio::task_local! {
    static WORKFLOW_CHECKPOINTS: mpsc::UnboundedSender<Box<RelayExecutionState>>;
}

const PLAN_VERSION: u32 = 2;
const DEFAULT_MAX_HOP_LY: f64 = 7.499;
const RELAY_DISTANCE_EPSILON: f64 = 1e-9;
const FTL_RELAY: &str = "ftl_relay";
const DEEP_SPACE_RELAY: &str = "deep_space_relay_station";
const DEEP_SPACE_RELAY_RANGE_LY: f64 = 10.0;
const SYSTEM_HUB: &str = "system_hub";
const RELAYING: &str = "relaying";
const POLL_INTERVAL: Duration = Duration::from_secs(15);
/// Upper bound between authoritative refreshes while waiting on the event
/// stream. Event-driven waits wake immediately on a relevant event; this only
/// bounds how long a missed event can delay progress.
const AUTHORITATIVE_POLL_INTERVAL: Duration = Duration::from_secs(60);
const MAX_DEVICE_TAG_CHARS: usize = 32;
const RELAY_MISSION_TAG_PREFIX: &str = "relay-m:";
const RELAY_SITE_TAG_PREFIX: &str = "relay-s:";
const RELAY_BATCH_TAG_PREFIX: &str = "relay-b:";
const RELAY_PREREQUISITE_TAG_PREFIX: &str = "relay-p:";

/// Error type returned by the reusable relay workflow.
pub type AnyError = Box<dyn StdError + Send + Sync + 'static>;
/// Result type returned by the reusable relay workflow.
pub type AnyResult<T> = Result<T, AnyError>;

fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> AnyError {
    io::Error::new(kind, message.into()).into()
}

async fn projected_device(client: &Client, code: &str) -> AnyResult<DeviceHandle> {
    Ok(match client.devices().cached(code) {
        Some(handle) => handle,
        None => client.devices().get(code).await?,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Plan,
    Run,
    Status,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum RequestedSupplyStrategy {
    #[default]
    Auto,
    Staged,
    Minimal,
    HubReturns,
}

impl RequestedSupplyStrategy {
    fn parse(value: &str) -> AnyResult<Self> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "staged" => Ok(Self::Staged),
            "minimal" => Ok(Self::Minimal),
            "hub" | "hub-returns" | "hub_returns" => Ok(Self::HubReturns),
            _ => Err(app_error(
                io::ErrorKind::InvalidInput,
                "--supply-strategy must be auto, staged, minimal, or hub",
            )),
        }
    }
}

#[derive(Debug)]
struct Config {
    command: Command,
    database: PathBuf,
    replicant: String,
    hub: String,
    plan_path: PathBuf,
    max_hop_ly: f64,
    replace_plan: bool,
    reuse_account_relays: bool,
    ignore_printers: BTreeSet<String>,
    supply_strategy: RequestedSupplyStrategy,
    wait_timeout: Duration,
    targets: Vec<String>,
    verbose: bool,
    log_file: Option<PathBuf>,
}

impl Config {
    fn from_args_and_env(arguments: impl IntoIterator<Item = String>) -> AnyResult<Self> {
        let mut arguments = arguments.into_iter();
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
        let mut database = env::var_os("REPLICANT_DB")
            .map(PathBuf::from)
            .unwrap_or_else(replicant_client::default_database_path);
        let mut replicant = env::var("RS_RELAY_REPLICANT").unwrap_or_else(|_| "Chats-1".into());
        let mut hub = env::var("RS_RELAY_HUB").unwrap_or_else(|_| "SCEPTURUM-BELT-1".into());
        let mut plan_path = PathBuf::from(
            env::var("RS_RELAY_PLAN").unwrap_or_else(|_| "ftl-relay-expansion.json".into()),
        );
        let mut max_hop_ly = DEFAULT_MAX_HOP_LY;
        let mut replace_plan =
            env_flag("RS_RELAY_REPLACE_PLAN") || env_flag("RS_RELAY_REBUILD_PLAN");
        let mut reuse_account_relays = env_flag("RS_RELAY_REUSE_ACCOUNT_RELAYS");
        let mut ignore_printers = if command == Command::Plan {
            env::var("RS_RELAY_IGNORE_PRINTERS")
                .ok()
                .map(|value| parse_code_list(&value))
                .unwrap_or_default()
        } else {
            BTreeSet::new()
        };
        let mut supply_strategy = env::var("RS_RELAY_SUPPLY_STRATEGY")
            .ok()
            .map(|value| RequestedSupplyStrategy::parse(&value))
            .transpose()?
            .unwrap_or_default();
        let mut wait_timeout = Duration::from_secs(
            env::var("RS_RELAY_WAIT_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(21_600),
        );
        let mut targets = Vec::new();
        let mut verbose = env_flag("RS_RELAY_VERBOSE");
        let mut log_file = env::var("RS_RELAY_LOG_FILE").ok().map(PathBuf::from);

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--replace-plan" | "--rebuild-plan" => replace_plan = true,
                "--reuse-account-relays" => reuse_account_relays = true,
                "--ignore-printer" | "--exclude-printer" => {
                    let value = required_argument(&mut arguments, &argument)?;
                    ignore_printers.extend(parse_code_list(&value));
                }
                "--supply-strategy" => {
                    supply_strategy = RequestedSupplyStrategy::parse(&required_argument(
                        &mut arguments,
                        "--supply-strategy",
                    )?)?;
                }
                "--replicant" => replicant = required_argument(&mut arguments, "--replicant")?,
                "--hub" => hub = required_argument(&mut arguments, "--hub")?,
                "--plan" => plan_path = PathBuf::from(required_argument(&mut arguments, "--plan")?),
                "--database" => {
                    database = PathBuf::from(required_argument(&mut arguments, "--database")?)
                }
                "--max-hop" => {
                    max_hop_ly = required_argument(&mut arguments, "--max-hop")?
                        .parse()
                        .map_err(|_| {
                            app_error(io::ErrorKind::InvalidInput, "--max-hop must be numeric")
                        })?;
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
                    )?));
                }
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
                value if command == Command::Plan => targets.extend(
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_uppercase),
                ),
                value => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        format!("unexpected argument for {command:?}: {value}"),
                    ));
                }
            }
        }

        if !max_hop_ly.is_finite() || max_hop_ly <= 0.0 {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--max-hop must be finite and greater than zero",
            ));
        }
        targets.sort();
        targets.dedup();
        if command != Command::Plan && replace_plan {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--replace-plan belongs on the plan command",
            ));
        }
        if command != Command::Plan && reuse_account_relays {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--reuse-account-relays belongs on the plan command",
            ));
        }
        if command != Command::Plan && !ignore_printers.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--ignore-printer belongs on the plan command",
            ));
        }
        Ok(Self {
            command,
            database,
            replicant,
            hub: hub.to_uppercase(),
            plan_path,
            max_hop_ly,
            replace_plan,
            reuse_account_relays,
            ignore_printers,
            supply_strategy,
            wait_timeout,
            targets,
            verbose,
            log_file,
        })
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

fn parse_code_list(value: &str) -> BTreeSet<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_ascii_uppercase)
        .collect()
}

fn print_help() {
    println!(
        "FTL relay expansion\n\n\
Usage:\n  replicant-cli relay plan [OPTIONS] SYSTEM ...\n  replicant-cli relay run [OPTIONS]\n  replicant-cli relay status [OPTIONS]\n\n\
Options:\n  --replace-plan             Replace the saved plan\n  --replicant NAME_OR_CODE   Transport replicant (default: Chats-1)\n  --hub LOCATION             Manufacturing hub (default: SCEPTURUM-BELT-1)\n  --plan PATH                Saved mission plan\n  --database PATH            Managed SQLite database\n  --max-hop LY               New FTL relay range (default: 7.499); existing relay-capable devices use advertised ranges\n  --reuse-account-relays     Reuse disconnected account relay islands too\n  --ignore-printer CODE      Exclude an Autofactory from relay print assignment; repeatable and comma-separated\n  --supply-strategy MODE     auto, staged, minimal, or hub (default: auto)\n  --wait-timeout-secs N      Per-phase timeout\n  --direct                   Run locally instead of submitting run to replicantd\n  --verbose                  Show tracing logs in the terminal\n  --log-file PATH            Append tracing logs to a file\n  -h, --help                 Show this help\n\n\
Targets are system designations, not planet locations. Plan is read-only. Run\n\
always reconciles and continues the persisted mission; there is no separate\n\
resume command or --execute confirmation."
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StopAction {
    DeployAndActivate,
    ActivateExisting,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RelayStop {
    system: String,
    location: String,
    parent_system: String,
    action: StopAction,
    #[serde(default = "default_relay_device_type")]
    device_type: String,
    relay_code: Option<String>,
    completed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PrintJob {
    system: String,
    #[serde(default = "default_relay_device_type")]
    device_type: String,
    factory_code: String,
    mission_tag: String,
    site_tag: String,
    #[serde(default)]
    batch_tag: Option<String>,
    #[serde(default)]
    flatpack: bool,
    submission_started: bool,
    operation_id: Option<String>,
    submitted: bool,
    relay_code: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SupplyStrategy {
    Staged,
    Minimal,
}

fn default_relay_device_type() -> String {
    FTL_RELAY.to_owned()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RelayRestock {
    boundary_stop_index: usize,
    location: String,
    relay_stop_indices: Vec<usize>,
    carrier_code: String,
    #[serde(default)]
    completed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RelaySupplyCarrier {
    code: String,
    device_type: String,
    attach_capacity: i64,
    restock_indices: Vec<usize>,
    #[serde(default)]
    dispatched: bool,
    #[serde(default)]
    returned_home: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RelaySupplyPlan {
    strategy: SupplyStrategy,
    initial_relay_stop_indices: Vec<usize>,
    restocks: Vec<RelayRestock>,
    carriers: Vec<RelaySupplyCarrier>,
}

#[derive(Clone, Debug)]
struct SupplyCarrierCandidate {
    code: String,
    device_type: String,
    attach_capacity: i64,
}

/// Serializable relay checkpoint reconciled before execution resumes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RelayExecutionState {
    version: u32,
    mission_id: String,
    /// Historical UUID-derived mission tags still recognized while an old
    /// checkpoint or queued print is migrated to the system-scoped tag.
    #[serde(default)]
    legacy_mission_tags: Vec<String>,
    replicant_code: String,
    vessel_code: String,
    hub_location: String,
    start_system: String,
    targets: Vec<String>,
    max_hop_ly: f64,
    network: RelayNetworkPlan,
    stops: Vec<RelayStop>,
    hub_stock_relays: Vec<String>,
    print_jobs: Vec<PrintJob>,
    #[serde(default)]
    planned_transport_capacity: i64,
    #[serde(default)]
    supply: Option<RelaySupplyPlan>,
    /// Dedicated attachment carrier used for modular Deep Space Relay Stations.
    /// DSRs cannot be stowed in the deployment vessel; they remain attached to
    /// this carrier until the carrier and deployment replicant reach the target.
    #[serde(default)]
    dsr_carrier_code: Option<String>,
    returned_to_hub: bool,
}

type MissionPlan = RelayExecutionState;

/// Current high-level relay execution phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RelayExecutionPhase {
    /// Relays still need to be manufactured or discovered.
    AwaitingRelays,
    /// Deployment or activation stops remain.
    Deploying,
    /// All stops are complete and the vessel is returning to the hub.
    ReturningToHub,
    /// Every stop and the final return are complete.
    Succeeded,
}

/// Frontend-neutral progress derived from a relay checkpoint.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RelayExecutionStatus {
    /// Stable mission identifier.
    pub mission_id: String,
    /// Current execution phase.
    pub phase: RelayExecutionPhase,
    /// Number of completed deployment or activation stops.
    pub completed_stops: usize,
    /// Total deployment or activation stops.
    pub total_stops: usize,
    /// Next incomplete system, if any.
    pub next_system: Option<String>,
    /// Number of relays still awaiting manufacture or discovery.
    pub pending_relays: usize,
}

impl RelayExecutionState {
    /// Returns the stable tag plus UUID-derived aliases recorded by this checkpoint.
    pub(crate) fn mission_tag_migration(&self) -> (String, Vec<String>) {
        let desired = relay_system_mission_tag(&self.start_system);
        let mut legacy = self.legacy_mission_tags.clone();
        legacy.extend(
            self.print_jobs
                .iter()
                .map(|job| job.mission_tag.clone())
                .filter(|tag| tag != &desired && is_opaque_relay_mission_tag(tag)),
        );
        legacy.sort();
        legacy.dedup();
        (desired, legacy)
    }

    /// Returns structured progress without consulting live API state.
    #[must_use]
    pub fn status(&self) -> RelayExecutionStatus {
        let completed_stops = self.stops.iter().filter(|stop| stop.completed).count();
        let pending_relays = self
            .print_jobs
            .iter()
            .filter(|job| job.relay_code.is_none())
            .count();
        let phase = if completed_stops == self.stops.len() && self.returned_to_hub {
            RelayExecutionPhase::Succeeded
        } else if completed_stops == self.stops.len() {
            RelayExecutionPhase::ReturningToHub
        } else if pending_relays != 0 {
            RelayExecutionPhase::AwaitingRelays
        } else {
            RelayExecutionPhase::Deploying
        };
        RelayExecutionStatus {
            mission_id: self.mission_id.clone(),
            phase,
            completed_stops,
            total_stops: self.stops.len(),
            next_system: self
                .stops
                .iter()
                .find(|stop| !stop.completed)
                .map(|stop| stop.system.clone()),
            pending_relays,
        }
    }

    pub(crate) fn step_name(&self) -> &'static str {
        match self.status().phase {
            RelayExecutionPhase::AwaitingRelays => "awaiting_relays",
            RelayExecutionPhase::Deploying => "deploying",
            RelayExecutionPhase::ReturningToHub => "returning_to_hub",
            RelayExecutionPhase::Succeeded => "complete",
        }
    }

    pub(crate) fn resources(&self) -> (&str, Vec<&str>, Vec<&str>) {
        let devices = std::iter::once(self.vessel_code.as_str())
            .chain(self.hub_stock_relays.iter().map(String::as_str))
            .chain(
                self.stops
                    .iter()
                    .filter_map(|stop| stop.relay_code.as_deref()),
            )
            .chain(
                self.supply
                    .iter()
                    .flat_map(|supply| supply.carriers.iter().map(|carrier| carrier.code.as_str())),
            )
            .chain(self.dsr_carrier_code.iter().map(String::as_str))
            .collect();
        let factories = self
            .print_jobs
            .iter()
            .filter(|job| job.relay_code.is_none())
            .map(|job| job.factory_code.as_str())
            .collect();
        (&self.replicant_code, devices, factories)
    }
}

#[derive(Clone, Debug)]
struct DeviceCensus {
    devices: BTreeMap<String, Device>,
    active_relay_codes: BTreeMap<String, Vec<String>>,
    inactive_relay_codes: BTreeMap<String, Vec<String>>,
    recoverable_dsr_codes: BTreeMap<String, Vec<String>>,
    compacted_dsr_codes: BTreeMap<String, Vec<String>>,
    relay_ranges_ly: BTreeMap<String, f64>,
    hub_stock: Vec<String>,
    hub_stock_dsr: Vec<String>,
    factories: Vec<FactoryState>,
    supply_carriers: Vec<SupplyCarrierCandidate>,
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
                                "another relay executor holds {} (pid {})",
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

/// Runs the standalone relay-expansion command-line interface.
pub async fn run_cli(arguments: Vec<String>) -> AnyResult<()> {
    let config = Config::from_args_and_env(arguments)?;
    init_logging(&config)?;
    if config.command == Command::Status {
        let plan = load_plan(&config.plan_path)?;
        print_plan(&plan);
        return Ok(());
    }
    if config.command == Command::Run && !config.plan_path.exists() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!(
                "no relay mission exists at {}; create one with `replicant-cli relay --plan ...`",
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
    if let Ok(plan) = &result {
        print_plan(plan);
    }
    let close_result = client.close().await;
    close_result?;
    result.map(drop)
}

fn init_logging(config: &Config) -> AnyResult<()> {
    if !config.verbose && config.log_file.is_none() {
        return Ok(());
    }
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("warn,replicant_cli::relay=info,replicant_client::ops=info")
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

async fn run(client: &Client, config: &Config) -> AnyResult<MissionPlan> {
    let resuming_plan = config.plan_path.exists() && !config.replace_plan;
    if client.galaxy().catalogue().is_empty() {
        info!(
            resuming_plan,
            "relay catalogue projection is empty; performing one targeted catalogue refresh"
        );
        client.galaxy().refresh_catalogue().await?;
    } else {
        debug!(
            resuming_plan,
            catalogue_stars = client.galaxy().catalogue().len(),
            "relay workflow is using the managed galaxy projection"
        );
    }

    let requested_replicant = if config.command == Command::Run && config.plan_path.exists() {
        load_plan(&config.plan_path)?.replicant_code
    } else {
        config.replicant.clone()
    };
    let replicant = resolve_owned_replicant(client, &requested_replicant).await?;
    let replicant_code = replicant.key.id.as_str().to_owned();
    let vessel_code = replicant
        .hosted_device
        .as_ref()
        .map(|device| device.id.as_str().to_owned())
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("replicant {replicant_code} is not hosted in a vessel"),
            )
        })?;

    let printing_blueprints = fetch_print_blueprints(client).await?;

    let mut plan = if config.plan_path.exists() && !config.replace_plan {
        let mut plan = load_plan(&config.plan_path)?;
        validate_loaded_plan(&plan, config, &replicant_code, &vessel_code)?;
        if !config.ignore_printers.is_empty() {
            let all_factory_codes = discover_print_factory_codes(client, &config.hub)
                .await?
                .into_iter()
                .collect::<BTreeSet<_>>();
            let factories =
                discover_print_factories(client, &config.hub, &printing_blueprints).await?;
            let mission_id = plan.mission_id.clone();
            let reassigned = reassign_ignored_print_jobs(
                &mut plan.print_jobs,
                &mission_id,
                &factories,
                &all_factory_codes,
                &config.ignore_printers,
                &printing_blueprints,
                &config.hub,
            )?;
            if reassigned != 0 {
                info!(
                    reassigned,
                    "reassigned relay print jobs away from ignored Autofactories"
                );
            }
        }
        plan
    } else {
        if config.targets.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "at least one system target is required when creating a plan",
            ));
        }
        let plan = create_plan(
            client,
            config,
            &replicant_code,
            &vessel_code,
            &printing_blueprints,
        )
        .await?;
        save_plan(&config.plan_path, &plan)?;
        plan
    };

    reconcile_plan(client, &mut plan).await?;
    let mission_id = plan.mission_id.clone();
    let hub_location = plan.hub_location.clone();
    let live_factories =
        discover_print_factories(client, &hub_location, &printing_blueprints).await?;
    let reassigned = reassign_unavailable_print_jobs(
        &mut plan.print_jobs,
        &mission_id,
        &live_factories,
        &printing_blueprints,
        &hub_location,
    )?;
    if reassigned != 0 {
        info!(reassigned, hub = %hub_location, "reassigned relay print jobs from unavailable Autofactories");
    }
    save_plan(&config.plan_path, &plan)?;
    if config.command == Command::Plan {
        return Ok(plan);
    }

    execute_plan(client, config, &mut plan).await?;
    save_plan(&config.plan_path, &plan)?;
    Ok(plan)
}

async fn resolve_owned_replicant(client: &Client, query: &str) -> AnyResult<Replicant> {
    let handles = client.replicants().find().owned().collect().await?;
    let mut matches = Vec::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if snapshot.key.id.as_str().eq_ignore_ascii_case(query)
            || snapshot
                .name
                .as_deref()
                .is_some_and(|name| name.eq_ignore_ascii_case(query))
        {
            matches.push(snapshot);
        }
    }
    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(app_error(
            io::ErrorKind::NotFound,
            format!("no owned replicant matches {query:?}"),
        )),
        _ => Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("owned replicant name {query:?} is ambiguous; use its code"),
        )),
    }
}

fn existing_relay_activation_stop(
    census: &DeviceCensus,
    system: &str,
    parent_system: &str,
    relay_code: String,
) -> AnyResult<RelayStop> {
    let relay = census.devices.get(&relay_code).ok_or_else(|| {
        app_error(
            io::ErrorKind::NotFound,
            format!("selected relay {relay_code} omitted from census"),
        )
    })?;
    let location = device_location(relay).ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            format!("inactive relay {relay_code} has no location"),
        )
    })?;
    Ok(RelayStop {
        system: system.to_owned(),
        location: location.to_owned(),
        parent_system: parent_system.to_owned(),
        action: StopAction::ActivateExisting,
        device_type: device_type(relay).unwrap_or(FTL_RELAY).to_owned(),
        relay_code: Some(relay_code),
        completed: false,
    })
}

async fn create_plan(
    client: &Client,
    config: &Config,
    replicant_code: &str,
    vessel_code: &str,
    printing_blueprints: &BTreeMap<String, PrintingBlueprint>,
) -> AnyResult<MissionPlan> {
    let catalogue = client.galaxy().catalogue();
    let system_names = catalogue
        .iter()
        .map(|star| star.key.id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let start_system = resolve_system(&config.hub, &system_names).ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("hub {} does not resolve to a catalogue system", config.hub),
        )
    })?;
    for target in &config.targets {
        if !system_names.contains(target) {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("target {target} is not a catalogue system designation"),
            ));
        }
    }

    let factory_codes = discover_print_factory_codes(client, &config.hub)
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let census = refresh_device_census(
        client,
        &config.hub,
        vessel_code,
        &system_names,
        printing_blueprints,
    )
    .await?;
    if factory_codes.is_empty() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!(
                "no account-owned autofactories are present at {}",
                config.hub
            ),
        ));
    }
    validate_ignored_printers(&factory_codes, &config.ignore_printers, &config.hub)?;
    if !config.ignore_printers.is_empty() {
        info!(
            ignored_printers = ?config.ignore_printers,
            hub = %config.hub,
            "excluding autofactories from relay print assignment"
        );
    }
    if !census.active_relay_codes.contains_key(&start_system) {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "starting system {start_system} does not currently have an active account-owned relay-capable device"
            ),
        ));
    }

    let stars = catalogue
        .into_iter()
        .filter_map(|star| {
            Some(PlannerStar {
                designation: star.key.id.as_str().to_owned(),
                position: star.position.map(|position| Position {
                    x: position.x,
                    y: position.y,
                    z: position.z,
                })?,
                entry_point: star
                    .entry_point
                    .as_ref()
                    .map(|location| location.id.as_str().to_owned()),
            })
        })
        .collect::<Vec<_>>();
    let account_active_relays = census
        .active_relay_codes
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let account_inactive_relays = census
        .inactive_relay_codes
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    let (active_relay_systems, inactive_relay_systems) = if config.reuse_account_relays {
        (
            account_active_relays.clone(),
            account_inactive_relays.clone(),
        )
    } else {
        relay_reuse_scope(
            &stars,
            &start_system,
            &config.targets,
            &account_active_relays,
            &account_inactive_relays,
            &census.relay_ranges_ly,
            config.max_hop_ly,
        )
    };
    let ignored_active = account_active_relays
        .len()
        .saturating_sub(active_relay_systems.len());
    let ignored_inactive = account_inactive_relays
        .len()
        .saturating_sub(inactive_relay_systems.len());
    let excluded_relay_systems = account_active_relays
        .union(&account_inactive_relays)
        .filter(|system| {
            !active_relay_systems.contains(system.as_str())
                && !inactive_relay_systems.contains(system.as_str())
        })
        .cloned()
        .collect::<BTreeSet<_>>();
    let planning_stars = stars
        .into_iter()
        .filter(|star| !excluded_relay_systems.contains(&star.designation))
        .collect::<Vec<_>>();
    let planning_relay_ranges = census
        .relay_ranges_ly
        .iter()
        .filter(|(system, _)| {
            active_relay_systems.contains(system.as_str())
                || inactive_relay_systems.contains(system.as_str())
        })
        .map(|(system, range)| (system.clone(), *range))
        .collect::<BTreeMap<_, _>>();
    for (system, range_ly) in planning_relay_ranges
        .iter()
        .filter(|(_, range)| **range > config.max_hop_ly + RELAY_DISTANCE_EPSILON)
    {
        info!(
            system = %system,
            range_ly = *range_ly,
            conventional_range_ly = config.max_hop_ly,
            "using extended relay-capable device range"
        );
    }
    info!(
        start = %start_system,
        reusable_active = active_relay_systems.len(),
        reusable_inactive = inactive_relay_systems.len(),
        ignored_active,
        ignored_inactive,
        account_wide = config.reuse_account_relays,
        "selected existing relay reuse scope"
    );
    let dsr_available =
        printing_blueprints.contains_key(DEEP_SPACE_RELAY) || !census.hub_stock_dsr.is_empty();
    let (network, dsr_systems) = plan_relay_network_with_dsr_fallback(
        &planning_stars,
        RelayNetworkRequest {
            start: start_system.clone(),
            targets: config.targets.clone(),
            active_relay_systems,
            inactive_relay_systems,
            max_hop_ly: config.max_hop_ly,
        },
        planning_relay_ranges,
        dsr_available,
    )?;
    if !dsr_systems.is_empty() {
        info!(
            systems = ?dsr_systems,
            range_ly = DEEP_SPACE_RELAY_RANGE_LY,
            "relay expansion requires Deep Space Relay Station bridge sites"
        );
    }

    let mission_id = format!("{}-{}", start_system.to_lowercase(), uuid::Uuid::new_v4());
    let mission_tag = relay_system_mission_tag(&start_system);
    let mut hub_stock = census.hub_stock.clone();
    let mut hub_stock_dsr = census.hub_stock_dsr.clone();
    // Assign relays that are already aboard the transport to the earliest new
    // deployment stops. That keeps the first multi-trip load executable even
    // when some of the vessel's stow capacity is already occupied by hub stock.
    hub_stock.sort_by_key(|code| {
        let stowed_in_transport = census.devices.get(code).is_some_and(|device| {
            device
                .relationships
                .stowed_in
                .as_ref()
                .is_some_and(|container| container.id.as_str() == vessel_code)
        });
        (stowed_in_transport, code.clone())
    });
    hub_stock_dsr.sort_by_key(|code| {
        let stowed_in_transport = census.devices.get(code).is_some_and(|device| {
            device
                .relationships
                .stowed_in
                .as_ref()
                .is_some_and(|container| container.id.as_str() == vessel_code)
        });
        (stowed_in_transport, code.clone())
    });
    let nodes = network
        .nodes
        .iter()
        .map(|node| (node.system.as_str(), node))
        .collect::<BTreeMap<_, _>>();
    let mut stops = Vec::new();
    let mut print_jobs = Vec::new();
    let mut used_hub_stock = Vec::new();

    // Existing active relay systems are normally omitted from execution_order.
    // DSR fallback can require adding a 10 ly booster at one of those systems,
    // so fold those booster actions into a dependency-safe order. Sorting by
    // tree depth keeps every parent available before a child is verified; the
    // original optimized execution rank remains the secondary ordering.
    let execution_rank = network
        .execution_order
        .iter()
        .enumerate()
        .map(|(index, system)| (system.as_str(), index))
        .collect::<BTreeMap<_, _>>();
    let mut action_order = network.execution_order.clone();
    action_order.extend(
        dsr_systems
            .iter()
            .filter(|system| {
                nodes
                    .get(system.as_str())
                    .is_some_and(|node| node.relay == RelayAvailability::Active)
            })
            .cloned(),
    );
    // A deployed DSR can be compacted or otherwise inactive while an ordinary
    // relay in the same system keeps the network node classified as Active.
    // Include those systems explicitly so expansion repairs the DSR instead of
    // silently leaving reusable 10 ly infrastructure folded up.
    action_order.extend(
        census
            .recoverable_dsr_codes
            .keys()
            .filter(|system| {
                nodes
                    .get(system.as_str())
                    .is_some_and(|node| !node.is_start)
            })
            .cloned(),
    );
    action_order.sort_by(|left, right| {
        let left_node = nodes.get(left.as_str()).expect("planned node exists");
        let right_node = nodes.get(right.as_str()).expect("planned node exists");
        left_node
            .depth
            .cmp(&right_node.depth)
            .then_with(|| {
                execution_rank
                    .get(left.as_str())
                    .copied()
                    .unwrap_or(usize::MAX)
                    .cmp(
                        &execution_rank
                            .get(right.as_str())
                            .copied()
                            .unwrap_or(usize::MAX),
                    )
            })
            .then_with(|| left.cmp(right))
    });
    action_order.dedup();

    for system in &action_order {
        let node = nodes.get(system.as_str()).copied().ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("execution order references missing network node {system}"),
            )
        })?;
        let parent_system = node.parent.clone().ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("planned relay system {system} has no parent"),
            )
        })?;
        let requires_dsr = dsr_systems.contains(system);
        let recoverable_dsr = census
            .recoverable_dsr_codes
            .get(system)
            .and_then(|codes| codes.first())
            .cloned();
        let mut scheduled_existing = BTreeSet::new();

        if requires_dsr {
            if let Some(relay_code) = recoverable_dsr {
                info!(
                    system = %system,
                    relay = %relay_code,
                    "reusing deployed Deep Space Relay Station for required 10 ly coverage"
                );
                scheduled_existing.insert(relay_code.clone());
                stops.push(existing_relay_activation_stop(
                    &census,
                    system,
                    &parent_system,
                    relay_code,
                )?);
            } else {
                let location = choose_l4_location(client, node).await?;
                let relay_code = hub_stock_dsr.pop().inspect(|code| {
                    used_hub_stock.push(code.clone());
                });
                if relay_code.is_none() {
                    let site_tag = relay_site_tag(system);
                    print_jobs.push(PrintJob {
                        system: system.clone(),
                        device_type: DEEP_SPACE_RELAY.to_owned(),
                        factory_code: String::new(),
                        mission_tag: mission_tag.clone(),
                        site_tag,
                        batch_tag: None,
                        flatpack: true,
                        submission_started: false,
                        operation_id: None,
                        submitted: false,
                        relay_code: None,
                    });
                }
                stops.push(RelayStop {
                    system: system.clone(),
                    location,
                    parent_system: parent_system.clone(),
                    action: StopAction::DeployAndActivate,
                    device_type: DEEP_SPACE_RELAY.to_owned(),
                    relay_code,
                    completed: false,
                });
            }
        } else {
            match node.relay {
                RelayAvailability::ActivationRequired => {
                    let relay_code = census
                        .inactive_relay_codes
                        .get(system)
                        .and_then(|codes| codes.first())
                        .cloned()
                        .ok_or_else(|| {
                            app_error(
                                io::ErrorKind::NotFound,
                                format!(
                                    "inactive relay selected at {system} but no device was found"
                                ),
                            )
                        })?;
                    scheduled_existing.insert(relay_code.clone());
                    stops.push(existing_relay_activation_stop(
                        &census,
                        system,
                        &parent_system,
                        relay_code,
                    )?);
                }
                RelayAvailability::New => {
                    let location = choose_l4_location(client, node).await?;
                    let relay_code = hub_stock.pop().inspect(|code| {
                        used_hub_stock.push(code.clone());
                    });
                    if relay_code.is_none() {
                        let site_tag = relay_site_tag(system);
                        print_jobs.push(PrintJob {
                            system: system.clone(),
                            device_type: FTL_RELAY.to_owned(),
                            factory_code: String::new(),
                            mission_tag: mission_tag.clone(),
                            site_tag,
                            batch_tag: None,
                            flatpack: false,
                            submission_started: false,
                            operation_id: None,
                            submitted: false,
                            relay_code: None,
                        });
                    }
                    stops.push(RelayStop {
                        system: system.clone(),
                        location,
                        parent_system: parent_system.clone(),
                        action: StopAction::DeployAndActivate,
                        device_type: FTL_RELAY.to_owned(),
                        relay_code,
                        completed: false,
                    });
                }
                RelayAvailability::Active => {
                    if let Some(relay_code) = recoverable_dsr {
                        info!(
                            system = %system,
                            relay = %relay_code,
                            "repairing deployed inactive Deep Space Relay Station during expansion"
                        );
                        scheduled_existing.insert(relay_code.clone());
                        stops.push(existing_relay_activation_stop(
                            &census,
                            system,
                            &parent_system,
                            relay_code,
                        )?);
                    }
                }
            }
        }

        // If several deployed DSRs in one selected network system are folded
        // up, restore all of them. One may already be the primary relay stop
        // above; the remainder are explicit repair stops and require no cargo.
        if let Some(compacted_codes) = census.compacted_dsr_codes.get(system) {
            for relay_code in compacted_codes {
                if scheduled_existing.contains(relay_code) {
                    continue;
                }
                info!(
                    system = %system,
                    relay = %relay_code,
                    "adding repair stop for deployed compacted Deep Space Relay Station"
                );
                stops.push(existing_relay_activation_stop(
                    &census,
                    system,
                    &parent_system,
                    relay_code.clone(),
                )?);
            }
        }
    }

    let dsr_required = stops.iter().any(stop_requires_attachment_carrier);
    let dsr_carrier_code = if dsr_required {
        Some(
            census
                .supply_carriers
                .first()
                .map(|carrier| carrier.code.clone())
                .ok_or_else(|| {
                    classified_error(
                        FailureClass::ConnectivityDependency,
                        io::ErrorKind::NotFound,
                        format!(
                            "Deep Space Relay Station deployment from {} requires an idle attachment carrier in system {}",
                            config.hub, start_system
                        ),
                    )
                })?,
        )
    } else {
        None
    };
    let ordinary_supply_carriers = census
        .supply_carriers
        .iter()
        .filter(|carrier| dsr_carrier_code.as_deref() != Some(carrier.code.as_str()))
        .cloned()
        .collect::<Vec<_>>();

    // Device census populated the selected vessel projection.
    let vessel = projected_device(client, vessel_code)
        .await?
        .snapshot()
        .await?;
    let free_slots = vessel
        .stow_capacity
        .unwrap_or(0)
        .saturating_sub(vessel.stow_used.unwrap_or(0));
    let already_stowed = used_hub_stock
        .iter()
        .filter(|code| {
            census.devices.get(*code).is_some_and(|device| {
                device_type(device) != Some(DEEP_SPACE_RELAY)
                    && device
                        .relationships
                        .stowed_in
                        .as_ref()
                        .is_some_and(|container| container.id.as_str() == vessel_code)
            })
        })
        .count();
    let transport_capacity = free_slots.saturating_add(i64::try_from(already_stowed)?);
    let transport_required = i64::try_from(
        stops
            .iter()
            .filter(|stop| stop_uses_vessel_stow(stop))
            .count(),
    )?;
    if transport_required > 0 && transport_capacity <= 0 {
        return Err(classified_error(
            FailureClass::ConnectivityDependency,
            io::ErrorKind::Other,
            format!(
                "vessel {vessel_code} has no usable stow capacity for the {transport_required} ordinary mission relay(s)"
            ),
        ));
    }
    let supply = if transport_required > transport_capacity {
        let trips = (transport_required + transport_capacity - 1) / transport_capacity;
        let supply = build_supply_plan(
            config.supply_strategy,
            &stops,
            usize::try_from(transport_capacity)?,
            &ordinary_supply_carriers,
        )?;
        if let Some(supply) = &supply {
            info!(
                vessel = %vessel_code,
                transport_capacity,
                transport_required,
                restocks = supply.restocks.len(),
                carriers = supply.carriers.len(),
                strategy = ?supply.strategy,
                "relay expansion will use rolling carrier restocks"
            );
        } else {
            info!(
                vessel = %vessel_code,
                transport_capacity,
                transport_required,
                trips,
                "relay expansion will use multiple hub-return deployment trips"
            );
        }
        supply
    } else {
        None
    };

    assign_unsubmitted_print_jobs(
        &mut print_jobs,
        &census.factories,
        &config.ignore_printers,
        printing_blueprints,
        &BTreeMap::new(),
        &config.hub,
    )?;
    assign_new_plan_print_batches(&mission_id, &mut print_jobs);

    Ok(MissionPlan {
        version: PLAN_VERSION,
        mission_id,
        legacy_mission_tags: Vec::new(),
        replicant_code: replicant_code.to_owned(),
        vessel_code: vessel_code.to_owned(),
        hub_location: config.hub.clone(),
        start_system,
        targets: config.targets.clone(),
        max_hop_ly: config.max_hop_ly,
        network,
        stops,
        hub_stock_relays: used_hub_stock,
        print_jobs,
        planned_transport_capacity: transport_capacity,
        supply,
        dsr_carrier_code,
        returned_to_hub: false,
    })
}

fn plan_relay_network_with_dsr_fallback(
    stars: &[PlannerStar],
    request: RelayNetworkRequest,
    relay_ranges_ly: BTreeMap<String, f64>,
    dsr_available: bool,
) -> Result<(RelayNetworkPlan, BTreeSet<String>), PlannerError> {
    let mut planning_ranges = relay_ranges_ly;
    let mut dsr_systems = BTreeSet::new();
    let mut result =
        plan_relay_network_with_ranges(stars.to_vec(), request.clone(), planning_ranges.clone());
    if result.is_ok() || !dsr_available {
        return result.map(|network| (network, dsr_systems));
    }

    // Conventional 7.499 ly relays cannot bridge every catalogue gap. When the
    // account can manufacture a Deep Space Relay Station, incrementally grant
    // a 10 ly range to one boundary system at a time and re-run the exact
    // planner. The selected systems are later materialized as DSR deployment
    // stops, so this is not a fictitious range increase.
    for _ in 0..stars.len().min(64) {
        let candidate = match result.as_ref().expect_err("checked error above") {
            PlannerError::DisconnectedGap {
                from,
                to,
                distance_ly,
                ..
            } if *distance_ly <= DEEP_SPACE_RELAY_RANGE_LY + RELAY_DISTANCE_EPSILON => {
                [from.as_str(), to.as_str()].into_iter().find_map(|system| {
                    (planning_ranges
                        .get(system)
                        .copied()
                        .unwrap_or(request.max_hop_ly)
                        + RELAY_DISTANCE_EPSILON
                        < DEEP_SPACE_RELAY_RANGE_LY)
                        .then(|| system.to_owned())
                })
            }
            PlannerError::DisconnectedRouteAround(details) => details
                .bridges
                .iter()
                .find(|bridge| {
                    bridge.distance_ly <= DEEP_SPACE_RELAY_RANGE_LY + RELAY_DISTANCE_EPSILON
                        && planning_ranges
                            .get(&bridge.from)
                            .copied()
                            .unwrap_or(request.max_hop_ly)
                            + RELAY_DISTANCE_EPSILON
                            < DEEP_SPACE_RELAY_RANGE_LY
                })
                .map(|bridge| bridge.from.clone())
                .or_else(|| {
                    details.bridges.iter().find_map(|bridge| {
                        (bridge.distance_ly <= DEEP_SPACE_RELAY_RANGE_LY + RELAY_DISTANCE_EPSILON
                            && planning_ranges
                                .get(&bridge.to)
                                .copied()
                                .unwrap_or(request.max_hop_ly)
                                + RELAY_DISTANCE_EPSILON
                                < DEEP_SPACE_RELAY_RANGE_LY)
                            .then(|| bridge.to.clone())
                    })
                }),
            _ => None,
        };
        let Some(system) = candidate else {
            break;
        };
        planning_ranges.insert(system.clone(), DEEP_SPACE_RELAY_RANGE_LY);
        dsr_systems.insert(system.clone());
        info!(
            system = %system,
            range_ly = DEEP_SPACE_RELAY_RANGE_LY,
            "retrying disconnected relay plan with a Deep Space Relay Station bridge"
        );
        result = plan_relay_network_with_ranges(
            stars.to_vec(),
            request.clone(),
            planning_ranges.clone(),
        );
        if let Ok(network) = result.as_ref() {
            let selected = network
                .nodes
                .iter()
                .map(|node| node.system.as_str())
                .collect::<BTreeSet<_>>();
            dsr_systems.retain(|system| selected.contains(system.as_str()));
            return Ok((network.clone(), dsr_systems));
        }
    }

    result.map(|network| (network, dsr_systems))
}

fn relay_reuse_scope(
    stars: &[PlannerStar],
    start: &str,
    targets: &[String],
    active: &BTreeSet<String>,
    inactive: &BTreeSet<String>,
    relay_ranges_ly: &BTreeMap<String, f64>,
    max_hop_ly: f64,
) -> (BTreeSet<String>, BTreeSet<String>) {
    let positions = stars
        .iter()
        .map(|star| (star.designation.clone(), star.position))
        .collect::<BTreeMap<_, _>>();
    let existing = active
        .union(inactive)
        .filter(|system| positions.contains_key(system.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>();

    let mut reachable = BTreeSet::new();
    let mut pending = std::collections::VecDeque::new();
    if existing.contains(start) && positions.contains_key(start) {
        reachable.insert(start.to_owned());
        pending.push_back(start.to_owned());
    }

    while let Some(current) = pending.pop_front() {
        let Some(current_position) = positions.get(current.as_str()).copied() else {
            continue;
        };
        for candidate in &existing {
            if reachable.contains(candidate) {
                continue;
            }
            let Some(candidate_position) = positions.get(candidate.as_str()).copied() else {
                continue;
            };
            let available_range = relay_ranges_ly
                .get(current.as_str())
                .copied()
                .unwrap_or(max_hop_ly)
                .max(
                    relay_ranges_ly
                        .get(candidate.as_str())
                        .copied()
                        .unwrap_or(max_hop_ly),
                )
                .max(max_hop_ly);
            if current_position.distance(candidate_position)
                <= available_range + RELAY_DISTANCE_EPSILON
            {
                reachable.insert(candidate.clone());
                pending.push_back(candidate.clone());
            }
        }
    }

    // An explicit target that already contains a relay remains reusable even if
    // it belongs to another disconnected island. This lets callers deliberately
    // bridge to that relay without making every relay in the remote island a
    // zero-cost waypoint.
    for target in targets {
        if existing.contains(target) {
            reachable.insert(target.clone());
        }
    }

    (
        active.intersection(&reachable).cloned().collect(),
        inactive.intersection(&reachable).cloned().collect(),
    )
}

fn validate_ignored_printers(
    factory_codes: &BTreeSet<String>,
    ignored: &BTreeSet<String>,
    hub: &str,
) -> AnyResult<()> {
    if ignored.is_empty() {
        return Ok(());
    }
    let unknown = ignored
        .iter()
        .filter(|code| !factory_codes.contains(code.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if unknown.is_empty() {
        return Ok(());
    }
    Err(app_error(
        io::ErrorKind::InvalidInput,
        format!(
            "ignored printer(s) {} are not account-owned Autofactories at {hub}",
            unknown.join(", ")
        ),
    ))
}

fn stop_requires_attachment_carrier(stop: &RelayStop) -> bool {
    stop.action == StopAction::DeployAndActivate && stop.device_type == DEEP_SPACE_RELAY
}

fn stop_uses_vessel_stow(stop: &RelayStop) -> bool {
    stop.action == StopAction::DeployAndActivate && !stop_requires_attachment_carrier(stop)
}

fn deployment_batches(
    stops: &[RelayStop],
    transport_capacity: usize,
) -> (Vec<usize>, Vec<(usize, Vec<usize>)>) {
    if transport_capacity == 0 {
        return (Vec::new(), Vec::new());
    }
    let deploy_indices = stops
        .iter()
        .enumerate()
        .filter_map(|(index, stop)| stop_uses_vessel_stow(stop).then_some(index))
        .collect::<Vec<_>>();
    let initial = deploy_indices
        .iter()
        .copied()
        .take(transport_capacity)
        .collect::<Vec<_>>();
    let mut restocks = Vec::new();
    let mut offset = initial.len();
    while offset < deploy_indices.len() {
        let boundary_stop_index = deploy_indices[offset - 1];
        let refill = deploy_indices
            .iter()
            .copied()
            .skip(offset)
            .take(transport_capacity)
            .collect::<Vec<_>>();
        offset += refill.len();
        restocks.push((boundary_stop_index, refill));
    }
    (initial, restocks)
}

fn assign_batches_to_candidates(
    quantities: &[usize],
    candidates: &[SupplyCarrierCandidate],
) -> Option<Vec<usize>> {
    fn search(
        quantities: &[usize],
        candidates: &[SupplyCarrierCandidate],
        batch_index: usize,
        remaining: &mut [i64],
        assignment_counts: &mut [usize],
        assignments: &mut [usize],
    ) -> bool {
        if batch_index == quantities.len() {
            return true;
        }
        let Ok(quantity) = i64::try_from(quantities[batch_index]) else {
            return false;
        };
        let mut choices = (0..candidates.len())
            .filter(|index| remaining[*index] >= quantity)
            .collect::<Vec<_>>();
        choices.sort_by(|left, right| {
            assignment_counts[*left]
                .cmp(&assignment_counts[*right])
                .then_with(|| {
                    remaining[*left]
                        .saturating_sub(quantity)
                        .cmp(&remaining[*right].saturating_sub(quantity))
                })
                .then_with(|| candidates[*left].code.cmp(&candidates[*right].code))
        });
        for carrier_index in choices {
            remaining[carrier_index] -= quantity;
            assignment_counts[carrier_index] += 1;
            assignments[batch_index] = carrier_index;
            if search(
                quantities,
                candidates,
                batch_index + 1,
                remaining,
                assignment_counts,
                assignments,
            ) {
                return true;
            }
            assignment_counts[carrier_index] -= 1;
            remaining[carrier_index] += quantity;
        }
        false
    }

    if quantities.is_empty() {
        return Some(Vec::new());
    }
    let mut remaining = candidates
        .iter()
        .map(|candidate| candidate.attach_capacity)
        .collect::<Vec<_>>();
    let mut assignment_counts = vec![0usize; candidates.len()];
    let mut assignments = vec![0usize; quantities.len()];
    search(
        quantities,
        candidates,
        0,
        &mut remaining,
        &mut assignment_counts,
        &mut assignments,
    )
    .then_some(assignments)
}

fn staged_supply_assignment(
    quantities: &[usize],
    candidates: &[SupplyCarrierCandidate],
) -> Option<(Vec<SupplyCarrierCandidate>, Vec<usize>)> {
    if candidates.len() < quantities.len() {
        return None;
    }
    let mut unused = candidates.to_vec();
    let mut assignments = vec![usize::MAX; quantities.len()];
    let mut chosen = Vec::<SupplyCarrierCandidate>::new();
    let mut batches = quantities.iter().copied().enumerate().collect::<Vec<_>>();
    batches.sort_by(
        |(left_index, left_quantity), (right_index, right_quantity)| {
            right_quantity
                .cmp(left_quantity)
                .then_with(|| left_index.cmp(right_index))
        },
    );
    for (batch_index, quantity) in batches {
        let needed = i64::try_from(quantity).ok()?;
        let candidate_index = unused
            .iter()
            .enumerate()
            .filter(|(_, candidate)| candidate.attach_capacity >= needed)
            .min_by(|(_, left), (_, right)| {
                left.attach_capacity
                    .cmp(&right.attach_capacity)
                    .then_with(|| left.code.cmp(&right.code))
            })
            .map(|(index, _)| index)?;
        let candidate = unused.remove(candidate_index);
        let selected_index = chosen.len();
        chosen.push(candidate);
        assignments[batch_index] = selected_index;
    }
    Some((chosen, assignments))
}

fn minimal_supply_assignment(
    quantities: &[usize],
    candidates: &[SupplyCarrierCandidate],
) -> Option<(Vec<SupplyCarrierCandidate>, Vec<usize>)> {
    let mut ranked = candidates.to_vec();
    ranked.sort_by(|left, right| {
        right
            .attach_capacity
            .cmp(&left.attach_capacity)
            .then_with(|| left.code.cmp(&right.code))
    });
    for count in 1..=ranked.len().min(quantities.len()) {
        let selected = ranked.iter().take(count).cloned().collect::<Vec<_>>();
        if let Some(assignments) = assign_batches_to_candidates(quantities, &selected) {
            return Some((selected, assignments));
        }
    }
    None
}

fn build_supply_plan(
    requested: RequestedSupplyStrategy,
    stops: &[RelayStop],
    transport_capacity: usize,
    candidates: &[SupplyCarrierCandidate],
) -> AnyResult<Option<RelaySupplyPlan>> {
    let (initial_relay_stop_indices, batches) = deployment_batches(stops, transport_capacity);
    if batches.is_empty() || requested == RequestedSupplyStrategy::HubReturns {
        return Ok(None);
    }
    let quantities = batches
        .iter()
        .map(|(_, indices)| indices.len())
        .collect::<Vec<_>>();
    let assignment = match requested {
        RequestedSupplyStrategy::Staged => staged_supply_assignment(&quantities, candidates)
            .map(|value| (SupplyStrategy::Staged, value)),
        RequestedSupplyStrategy::Minimal => minimal_supply_assignment(&quantities, candidates)
            .map(|value| (SupplyStrategy::Minimal, value)),
        RequestedSupplyStrategy::Auto => staged_supply_assignment(&quantities, candidates)
            .map(|value| (SupplyStrategy::Staged, value))
            .or_else(|| {
                minimal_supply_assignment(&quantities, candidates)
                    .map(|value| (SupplyStrategy::Minimal, value))
            }),
        RequestedSupplyStrategy::HubReturns => None,
    };

    let Some((strategy, (selected, assignments))) = assignment else {
        if requested == RequestedSupplyStrategy::Auto {
            warn!(
                restocks = batches.len(),
                carriers = candidates.len(),
                "no carrier set can preload every relay restock; falling back to hub-return deployment trips"
            );
            return Ok(None);
        }
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!(
                "supply strategy {requested:?} cannot cover {} restock batch(es) with the available idle attachment carriers",
                batches.len()
            ),
        ));
    };

    let mut restocks = Vec::with_capacity(batches.len());
    let mut carrier_restock_indices = vec![Vec::<usize>::new(); selected.len()];
    for (restock_index, ((boundary_stop_index, relay_stop_indices), carrier_index)) in
        batches.into_iter().zip(assignments).enumerate()
    {
        carrier_restock_indices[carrier_index].push(restock_index);
        restocks.push(RelayRestock {
            boundary_stop_index,
            location: stops[boundary_stop_index].location.clone(),
            relay_stop_indices,
            carrier_code: selected[carrier_index].code.clone(),
            completed: false,
        });
    }
    let carriers = selected
        .into_iter()
        .enumerate()
        .map(|(index, candidate)| RelaySupplyCarrier {
            code: candidate.code,
            device_type: candidate.device_type,
            attach_capacity: candidate.attach_capacity,
            restock_indices: carrier_restock_indices[index].clone(),
            dispatched: false,
            returned_home: false,
        })
        .collect::<Vec<_>>();

    Ok(Some(RelaySupplyPlan {
        strategy,
        initial_relay_stop_indices,
        restocks,
        carriers,
    }))
}

async fn choose_l4_location(client: &Client, node: &NetworkNode) -> AnyResult<String> {
    if let Some(entry_point) = node.entry_point.as_deref()
        && entry_point.ends_with("-L4")
    {
        return Ok(entry_point.to_owned());
    }
    let mut locations = client
        .locations()
        .find()
        .in_system(&node.system)
        .collect()
        .await?;
    locations.sort_by(|left, right| left.key.id.as_str().cmp(right.key.id.as_str()));
    if let Some(location) = locations
        .iter()
        .find(|location| location.key.id.as_str().ends_with("-L4"))
    {
        return Ok(location.key.id.as_str().to_owned());
    }
    if let Some(entry_point) = node.entry_point.as_deref()
        && entry_point.ends_with("-L5")
    {
        return Ok(entry_point.to_owned());
    }
    if let Some(location) = locations
        .iter()
        .find(|location| location.key.id.as_str().ends_with("-L5"))
    {
        return Ok(location.key.id.as_str().to_owned());
    }

    // Newly discovered catalogue stars can legitimately have no projected
    // locations or entry point yet. Travelling to the system designation lets
    // the server select its default arrival zone (typically Oort/Kuiper), from
    // which the relay can be deployed and the system can be explored.
    warn!(
        system = %node.system,
        "relay target has no known L4/L5; using system-level arrival fallback"
    );
    Ok(node.system.clone())
}

async fn refresh_device_census(
    client: &Client,
    hub: &str,
    vessel_code: &str,
    systems: &BTreeSet<String>,
    printing_blueprints: &BTreeMap<String, PrintingBlueprint>,
) -> AnyResult<DeviceCensus> {
    // Relay planning consumes the daemon's SSE-backed managed projection. A
    // full account device traversal here was especially expensive when several
    // durable frontiers resumed together after restart. If one cached handle is
    // stale/missing, refresh only that device rather than the whole fleet.
    let handles = client.devices().find().owned().collect().await?;
    let mut devices = BTreeMap::new();
    for handle in handles {
        let code = handle.id().as_str().to_owned();
        let snapshot = match handle.snapshot().await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                warn!(
                    device = %code,
                    error = %error,
                    "relay census found an incomplete managed device snapshot; refreshing only that device"
                );
                client.devices().refresh(&code).await?.snapshot().await?
            }
        };
        devices.insert(code, snapshot);
    }

    let mut active_relay_codes = BTreeMap::<String, Vec<String>>::new();
    let mut inactive_relay_codes = BTreeMap::<String, Vec<String>>::new();
    let mut recoverable_dsr_codes = BTreeMap::<String, Vec<String>>::new();
    let mut compacted_dsr_codes = BTreeMap::<String, Vec<String>>::new();
    let mut relay_range_devices = Vec::<(String, String, String)>::new();
    let mut relay_code_ranges_ly = BTreeMap::<String, f64>::new();
    let mut hub_stock = Vec::new();
    let mut hub_stock_dsr = Vec::new();
    let mut supply_carriers = Vec::new();
    let hub_system = resolve_system(hub, systems).ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("hub {hub} does not resolve to a catalogue system"),
        )
    })?;
    for (code, device) in &devices {
        if code != vessel_code
            && device.attach_capacity.unwrap_or(0) > 0
            && device.travel.is_none()
            && device.relationships.attached_to.is_none()
            && device.relationships.stowed_in.is_none()
            && device.relationships.controller.is_none()
            && device.relationships.hosting_replicant.is_none()
            && device.relationships.attached_devices.is_empty()
            && !workflow_reserved(&device.tags)
            && device_has_command(device, "attach")
            && device_has_command(device, "travel")
            && device_location(device)
                .is_some_and(|location| designation_in_system(location, &hub_system))
        {
            supply_carriers.push(SupplyCarrierCandidate {
                code: code.clone(),
                device_type: device_type(device).unwrap_or("unknown").to_owned(),
                attach_capacity: device.attach_capacity.unwrap_or(0),
            });
        }

        let kind = device_type(device);
        let relay_capable = device_has_feature(device, "relay")
            || kind == Some(FTL_RELAY)
            || kind == Some(SYSTEM_HUB)
            || kind == Some(DEEP_SPACE_RELAY);
        if relay_capable {
            let stowed_in_transport = device
                .relationships
                .stowed_in
                .as_ref()
                .is_some_and(|container| container.id.as_str() == vessel_code);

            // Relays already aboard the selected transport remain usable
            // mission stock even though a stowed device deliberately has no
            // direct location. Count them before requiring `device.location`;
            // otherwise a full vessel carrying exactly the relays it needs is
            // incorrectly reported as having zero usable stow capacity.
            if kind == Some(FTL_RELAY) && stowed_in_transport {
                hub_stock.push(code.clone());
                continue;
            }

            let Some(location) = device_location(device) else {
                continue;
            };

            // Free ordinary relays and DSRs at the manufacturing hub are
            // fungible stock for expansion. System Hubs remain fixed network
            // infrastructure and are never silently repurposed as cargo.
            if (kind == Some(FTL_RELAY) || kind == Some(DEEP_SPACE_RELAY))
                && location == hub
                && device.relationships.stowed_in.is_none()
                && device.relationships.attached_to.is_none()
                && !relay_device_active(device)
                && (kind == Some(DEEP_SPACE_RELAY) || device_has_command(device, "stow"))
            {
                if kind == Some(DEEP_SPACE_RELAY) {
                    hub_stock_dsr.push(code.clone());
                } else {
                    hub_stock.push(code.clone());
                }
                continue;
            }
            if device.relationships.stowed_in.is_some()
                || device.relationships.attached_to.is_some()
            {
                continue;
            }
            let Some(system) = resolve_system(location, systems) else {
                continue;
            };
            let recoverable_dsr =
                kind == Some(DEEP_SPACE_RELAY) && relay_device_recoverable(device);
            if recoverable_dsr {
                recoverable_dsr_codes
                    .entry(system.clone())
                    .or_default()
                    .push(code.clone());
                if relay_device_needs_unfurl(device) {
                    compacted_dsr_codes
                        .entry(system.clone())
                        .or_default()
                        .push(code.clone());
                }
            }
            let usable = if relay_device_active(device) {
                active_relay_codes
                    .entry(system.clone())
                    .or_default()
                    .push(code.clone());
                true
            } else if device_has_command(device, "activate") || recoverable_dsr {
                inactive_relay_codes
                    .entry(system.clone())
                    .or_default()
                    .push(code.clone());
                true
            } else {
                false
            };
            if usable && kind != Some(FTL_RELAY) {
                relay_range_devices.push((
                    code.clone(),
                    system,
                    kind.unwrap_or("unknown").to_owned(),
                ));
            }
            continue;
        }
    }
    // Standard FTL relays continue to use the configured conventional
    // `--max-hop`. Known hardware uses its documented range without remote
    // I/O; future relay types retain an authoritative network lookup.
    let mut discovered_ranges_ly = BTreeMap::<String, Option<f64>>::new();
    for (code, system, relay_type) in relay_range_devices {
        let range = if let Some(range) = documented_relay_range_ly(&relay_type) {
            Some(range)
        } else if let Some(range) = discovered_ranges_ly.get(&relay_type) {
            *range
        } else {
            let handle = if let Some(handle) = client.devices().cached(&code) {
                Ok(handle)
            } else {
                client.devices().get(&code).await
            };
            let range = match handle {
                Ok(handle) => match handle.network().await {
                    Ok(network) => network
                        .range_ly
                        .filter(|range| range.is_finite() && *range > 0.0),
                    Err(error) => {
                        warn!(
                            device = %code,
                            system = %system,
                            device_type = %relay_type,
                            %error,
                            "could not read relay-capable device network range; using documented fallback when available"
                        );
                        None
                    }
                },
                Err(error) => {
                    warn!(
                        device = %code,
                        system = %system,
                        device_type = %relay_type,
                        %error,
                        "could not open relay-capable device for network range lookup; using documented fallback when available"
                    );
                    None
                }
            };
            discovered_ranges_ly.insert(relay_type, range);
            range
        };

        if let Some(range_ly) = range {
            relay_code_ranges_ly.insert(code, range_ly);
        }
    }

    for codes in active_relay_codes.values_mut() {
        codes.sort();
    }
    for codes in inactive_relay_codes.values_mut() {
        // If activation is required, prefer the device with the greatest known
        // relay range so the executed stop matches the range the planner may
        // rely on. Ordinary FTL relays have no override and therefore sort
        // behind an extended-range System Hub/other relay-capable device.
        codes.sort_by(|left, right| {
            let left_range = relay_code_ranges_ly.get(left).copied().unwrap_or(0.0);
            let right_range = relay_code_ranges_ly.get(right).copied().unwrap_or(0.0);
            right_range
                .total_cmp(&left_range)
                .then_with(|| left.cmp(right))
        });
    }
    for codes in recoverable_dsr_codes.values_mut() {
        codes.sort();
    }
    for codes in compacted_dsr_codes.values_mut() {
        codes.sort();
    }

    // A system's extended planning range must be backed by a device that is
    // actually usable at execution time. A recoverable deployed DSR counts
    // here even when an ordinary relay is already active in the same system,
    // because create_plan schedules that DSR for unfurl/activation before its
    // extended range can be relied upon by downstream stops.
    let mut relay_ranges_ly = BTreeMap::<String, f64>::new();
    for (system, codes) in &active_relay_codes {
        if let Some(range_ly) = codes
            .iter()
            .filter_map(|code| relay_code_ranges_ly.get(code).copied())
            .max_by(f64::total_cmp)
        {
            relay_ranges_ly.insert(system.clone(), range_ly);
        }
    }
    for (system, codes) in &inactive_relay_codes {
        if active_relay_codes.contains_key(system) {
            continue;
        }
        if let Some(range_ly) = codes
            .iter()
            .filter_map(|code| relay_code_ranges_ly.get(code).copied())
            .max_by(f64::total_cmp)
        {
            relay_ranges_ly.insert(system.clone(), range_ly);
        }
    }
    for (system, codes) in &recoverable_dsr_codes {
        // The starting system must already have live coverage before a mission
        // can execute. Do not let a folded-up DSR there silently extend the
        // root range because start-system repair cannot be topology-verified
        // until another network node exists.
        if system == &hub_system {
            continue;
        }
        if let Some(range_ly) = codes
            .iter()
            .filter_map(|code| relay_code_ranges_ly.get(code).copied())
            .max_by(f64::total_cmp)
        {
            relay_ranges_ly
                .entry(system.clone())
                .and_modify(|existing| *existing = existing.max(range_ly))
                .or_insert(range_ly);
        }
    }
    hub_stock.sort();
    hub_stock_dsr.sort();
    supply_carriers.sort_by(|left, right| {
        left.attach_capacity
            .cmp(&right.attach_capacity)
            .then_with(|| left.code.cmp(&right.code))
    });

    let factories = discover_print_factories(client, hub, printing_blueprints).await?;
    Ok(DeviceCensus {
        devices,
        active_relay_codes,
        inactive_relay_codes,
        recoverable_dsr_codes,
        compacted_dsr_codes,
        relay_ranges_ly,
        hub_stock,
        hub_stock_dsr,
        factories,
        supply_carriers,
    })
}

fn relay_print_workloads(
    factories: &[FactoryState],
    ignored: &BTreeSet<String>,
    reserved_seconds: &BTreeMap<String, f64>,
) -> Vec<FactoryWorkload> {
    factories
        .iter()
        .filter(|factory| !ignored.contains(&factory.code))
        .map(|factory| FactoryWorkload {
            code: factory.code.clone(),
            remaining_seconds: factory.remaining_seconds
                + reserved_seconds
                    .get(&factory.code)
                    .copied()
                    .unwrap_or_default(),
        })
        .collect()
}

fn reserved_print_seconds(
    jobs: &[PrintJob],
    factories: &BTreeSet<&str>,
    blueprints: &BTreeMap<String, PrintingBlueprint>,
) -> AnyResult<BTreeMap<String, f64>> {
    let mut reserved = BTreeMap::<String, f64>::new();
    for job in jobs.iter().filter(|job| {
        job.relay_code.is_none()
            && !job.submission_started
            && job.operation_id.is_none()
            && !job.submitted
            && factories.contains(job.factory_code.as_str())
    }) {
        let seconds = blueprints
            .get(&job.device_type)
            .ok_or_else(|| {
                classified_error(
                    FailureClass::ConnectivityDependency,
                    io::ErrorKind::NotFound,
                    format!("{} blueprint is not unlocked", job.device_type),
                )
            })?
            .print_time_seconds
            .max(0.0);
        *reserved.entry(job.factory_code.clone()).or_default() += seconds;
    }
    Ok(reserved)
}

fn assign_job_indices_with_shared_scheduler(
    print_jobs: &mut [PrintJob],
    indices: &[usize],
    factories: &[FactoryState],
    ignored: &BTreeSet<String>,
    blueprints: &BTreeMap<String, PrintingBlueprint>,
    reserved_seconds: &BTreeMap<String, f64>,
    hub: &str,
) -> AnyResult<()> {
    if indices.is_empty() {
        return Ok(());
    }
    let workloads = relay_print_workloads(factories, ignored, reserved_seconds);
    if workloads.is_empty() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!("no eligible Autofactory is available at {hub}"),
        ));
    }
    let mut required = QuantityMap::new();
    let mut by_type = BTreeMap::<String, std::collections::VecDeque<usize>>::new();
    for index in indices {
        let device_type = print_jobs[*index].device_type.clone();
        *required.entry(device_type.clone()).or_default() += 1;
        by_type.entry(device_type).or_default().push_back(*index);
    }
    let schedule = schedule_prints(&required, blueprints, &workloads)?;
    let mut assigned = 0usize;
    for batch in schedule.batches {
        let queue = by_type.get_mut(&batch.device_type).ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "shared print scheduler returned unexpected device type {}",
                    batch.device_type
                ),
            )
        })?;
        for _ in 0..batch.quantity {
            let index = queue.pop_front().ok_or_else(|| {
                app_error(
                    io::ErrorKind::InvalidData,
                    format!(
                        "shared print scheduler over-assigned device type {}",
                        batch.device_type
                    ),
                )
            })?;
            print_jobs[index].factory_code = batch.factory_code.clone();
            assigned += 1;
        }
    }
    if assigned != indices.len() || by_type.values().any(|queue| !queue.is_empty()) {
        return Err(classified_error(
            FailureClass::RelayPlanStale,
            io::ErrorKind::InvalidData,
            format!(
                "shared print scheduler assigned {assigned} of {} relay print units",
                indices.len()
            ),
        ));
    }
    Ok(())
}

fn assign_unsubmitted_print_jobs(
    print_jobs: &mut [PrintJob],
    factories: &[FactoryState],
    ignored: &BTreeSet<String>,
    blueprints: &BTreeMap<String, PrintingBlueprint>,
    reserved_seconds: &BTreeMap<String, f64>,
    hub: &str,
) -> AnyResult<()> {
    let indices = print_jobs
        .iter()
        .enumerate()
        .filter(|(_, job)| {
            job.relay_code.is_none()
                && !job.submission_started
                && job.operation_id.is_none()
                && !job.submitted
                && job.factory_code.is_empty()
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    assign_job_indices_with_shared_scheduler(
        print_jobs,
        &indices,
        factories,
        ignored,
        blueprints,
        reserved_seconds,
        hub,
    )
}

fn reassign_ignored_print_jobs(
    print_jobs: &mut [PrintJob],
    mission_id: &str,
    factories: &[FactoryState],
    all_factory_codes: &BTreeSet<String>,
    ignored: &BTreeSet<String>,
    blueprints: &BTreeMap<String, PrintingBlueprint>,
    hub: &str,
) -> AnyResult<usize> {
    validate_ignored_printers(all_factory_codes, ignored, hub)?;
    if ignored.is_empty() {
        return Ok(0);
    }

    let mut committed = Vec::new();
    for job in print_jobs.iter() {
        if !ignored.contains(&job.factory_code) || job.relay_code.is_some() {
            continue;
        }
        if job.submission_started || job.operation_id.is_some() || job.submitted {
            committed.push(format!("{} via {}", job.system, job.factory_code));
        }
    }
    if !committed.is_empty() {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!(
                "cannot ignore printer(s) {} for the saved plan because relay print job(s) have already been submitted or may be in flight: {}; let those jobs finish or recreate the plan with --replace-plan",
                ignored.iter().cloned().collect::<Vec<_>>().join(", "),
                committed.join(", ")
            ),
        ));
    }

    let moving = print_jobs
        .iter()
        .enumerate()
        .filter(|(_, job)| {
            job.relay_code.is_none()
                && ignored.contains(&job.factory_code)
                && !job.submission_started
                && job.operation_id.is_none()
                && !job.submitted
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if moving.is_empty() {
        return Ok(0);
    }
    let available = factories
        .iter()
        .filter(|factory| !ignored.contains(&factory.code))
        .map(|factory| factory.code.as_str())
        .collect::<BTreeSet<_>>();
    let reserved = reserved_print_seconds(print_jobs, &available, blueprints)?;
    assign_job_indices_with_shared_scheduler(
        print_jobs, &moving, factories, ignored, blueprints, &reserved, hub,
    )?;
    for index in &moving {
        if print_jobs[*index].batch_tag.is_some() {
            print_jobs[*index].batch_tag = Some(relay_batch_tag(
                mission_id,
                &print_jobs[*index].factory_code,
                &print_jobs[*index].device_type,
                print_jobs[*index].flatpack,
            ));
        }
    }
    Ok(moving.len())
}

fn reassign_unavailable_print_jobs(
    print_jobs: &mut [PrintJob],
    mission_id: &str,
    factories: &[FactoryState],
    blueprints: &BTreeMap<String, PrintingBlueprint>,
    hub: &str,
) -> AnyResult<usize> {
    let available = factories
        .iter()
        .map(|factory| factory.code.as_str())
        .collect::<BTreeSet<_>>();
    let moving = print_jobs
        .iter()
        .enumerate()
        .filter(|(_, job)| {
            job.relay_code.is_none()
                && !job.submission_started
                && job.operation_id.is_none()
                && !job.submitted
                && !available.contains(job.factory_code.as_str())
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if moving.is_empty() {
        return Ok(0);
    }
    let reserved = reserved_print_seconds(print_jobs, &available, blueprints)?;
    assign_job_indices_with_shared_scheduler(
        print_jobs,
        &moving,
        factories,
        &BTreeSet::new(),
        blueprints,
        &reserved,
        hub,
    )?;
    for index in &moving {
        if print_jobs[*index].batch_tag.is_some() {
            print_jobs[*index].batch_tag = Some(relay_batch_tag(
                mission_id,
                &print_jobs[*index].factory_code,
                &print_jobs[*index].device_type,
                print_jobs[*index].flatpack,
            ));
        }
    }
    Ok(moving.len())
}

fn stable_tag_hash(value: &str) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    value
        .as_bytes()
        .iter()
        .fold(FNV_OFFSET_BASIS, |hash, byte| {
            (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
        })
}

/// Returns the stable relay reservation tag for a start/hub system.
pub(crate) fn relay_system_mission_tag(system: &str) -> String {
    bounded_relay_tag(RELAY_MISSION_TAG_PREFIX, system)
}

fn legacy_relay_mission_tag(mission_id: &str) -> String {
    format!(
        "{RELAY_MISSION_TAG_PREFIX}{:016x}",
        stable_tag_hash(mission_id)
    )
}

fn bounded_relay_tag(prefix: &str, value: &str) -> String {
    const HASH_CHARS: usize = 12;
    let normalized = value
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
    let hash = stable_tag_hash(normalized) & 0x0000_ffff_ffff_ffff;
    format!("{prefix}{head}-{hash:012x}")
}

/// Returns whether a relay mission tag uses the old 16-hex hash identity.
pub(crate) fn is_opaque_relay_mission_tag(tag: &str) -> bool {
    tag.strip_prefix(RELAY_MISSION_TAG_PREFIX)
        .is_some_and(|suffix| {
            suffix.len() == 16 && suffix.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

fn migrate_relay_mission_tag_metadata(plan: &mut MissionPlan) -> bool {
    let desired = relay_system_mission_tag(&plan.start_system);
    let discovered_legacy = plan
        .print_jobs
        .iter()
        .map(|job| job.mission_tag.clone())
        .filter(|tag| tag != &desired && tag.starts_with(RELAY_MISSION_TAG_PREFIX))
        .collect::<Vec<_>>();
    let mut changed = false;
    for tag in discovered_legacy {
        if !plan.legacy_mission_tags.contains(&tag) {
            plan.legacy_mission_tags.push(tag);
            changed = true;
        }
    }
    let historical = legacy_relay_mission_tag(&plan.mission_id);
    if historical != desired && !plan.legacy_mission_tags.contains(&historical) {
        // Only retain the computed historical alias when this is a legacy plan.
        // New plans already create every print job with the readable system tag.
        if plan
            .print_jobs
            .iter()
            .any(|job| job.mission_tag == historical)
        {
            plan.legacy_mission_tags.push(historical);
            changed = true;
        }
    }
    let before = plan.legacy_mission_tags.len();
    plan.legacy_mission_tags
        .retain(|tag| tag.starts_with(RELAY_MISSION_TAG_PREFIX) && tag != &desired);
    plan.legacy_mission_tags.sort();
    plan.legacy_mission_tags.dedup();
    changed || plan.legacy_mission_tags.len() != before
}

fn relay_mission_tag_aliases(plan: &MissionPlan) -> Vec<String> {
    std::iter::once(relay_system_mission_tag(&plan.start_system))
        .chain(plan.legacy_mission_tags.iter().cloned())
        .collect()
}

fn relay_site_tag(system: &str) -> String {
    let direct = format!("{RELAY_SITE_TAG_PREFIX}{system}");
    if direct.chars().count() <= MAX_DEVICE_TAG_CHARS {
        return direct;
    }

    let prefix_budget = MAX_DEVICE_TAG_CHARS
        .saturating_sub(RELAY_SITE_TAG_PREFIX.chars().count())
        .saturating_sub(13);
    let prefix = system.chars().take(prefix_budget).collect::<String>();
    let hash = stable_tag_hash(system) & 0x0000_ffff_ffff_ffff;
    format!("{RELAY_SITE_TAG_PREFIX}{prefix}-{hash:012x}")
}

fn relay_batch_tag(
    mission_id: &str,
    factory_code: &str,
    device_type: &str,
    flatpack: bool,
) -> String {
    let mode = if flatpack { "flatpack" } else { "assembled" };
    format!(
        "{RELAY_BATCH_TAG_PREFIX}{:016x}",
        stable_tag_hash(&format!("{mission_id}:{factory_code}:{device_type}:{mode}"))
    )
}

fn relay_prerequisite_tag(mission_id: &str) -> String {
    format!(
        "{RELAY_PREREQUISITE_TAG_PREFIX}{:016x}",
        stable_tag_hash(mission_id)
    )
}

fn print_job_correlation_tag(job: &PrintJob) -> &str {
    job.batch_tag.as_deref().unwrap_or(&job.site_tag)
}

fn normalize_relay_print_jobs(jobs: &mut [PrintJob]) {
    // Ordinary FTL relays are directly attachable and should be printed
    // assembled. Deep Space Relay Stations are modular and must be printed as
    // flatpacks so the deployment vessel/carrier can transport them.
    for job in jobs {
        job.flatpack = job.device_type == DEEP_SPACE_RELAY;
    }
}

fn assign_new_plan_print_batches(mission_id: &str, jobs: &mut [PrintJob]) {
    normalize_relay_print_jobs(jobs);
    for job in jobs {
        job.batch_tag = Some(relay_batch_tag(
            mission_id,
            &job.factory_code,
            &job.device_type,
            job.flatpack,
        ));
    }
}

fn assign_safe_legacy_print_batches(plan: &mut MissionPlan) {
    let mut groups = BTreeSet::new();
    for job in &plan.print_jobs {
        if job.batch_tag.is_none()
            && !job.submission_started
            && job.operation_id.is_none()
            && !job.submitted
            && job.relay_code.is_none()
        {
            groups.insert((
                job.factory_code.clone(),
                job.device_type.clone(),
                job.flatpack,
            ));
        }
    }
    for (factory_code, device_type, flatpack) in groups {
        if plan.print_jobs.iter().any(|job| {
            job.factory_code == factory_code
                && job.device_type == device_type
                && job.flatpack == flatpack
                && job.batch_tag.is_some()
        }) {
            continue;
        }
        let batch_tag = relay_batch_tag(&plan.mission_id, &factory_code, &device_type, flatpack);
        for job in &mut plan.print_jobs {
            if job.factory_code == factory_code
                && job.device_type == device_type
                && job.flatpack == flatpack
                && job.batch_tag.is_none()
                && !job.submission_started
                && job.operation_id.is_none()
                && !job.submitted
                && job.relay_code.is_none()
            {
                job.batch_tag = Some(batch_tag.clone());
            }
        }
    }
}

fn pending_print_groups(jobs: &[PrintJob], factory_code: &str) -> Vec<Vec<usize>> {
    let mut groups = BTreeMap::<String, Vec<usize>>::new();
    for (index, job) in jobs.iter().enumerate() {
        if job.factory_code == factory_code && !job.submitted && !job.submission_started {
            groups
                .entry(print_job_correlation_tag(job).to_owned())
                .or_default()
                .push(index);
        }
    }
    groups.into_values().collect()
}

fn normalize_print_job_tags(plan: &mut MissionPlan) -> AnyResult<()> {
    normalize_relay_print_jobs(&mut plan.print_jobs);
    migrate_relay_mission_tag_metadata(plan);
    let mission_tag = relay_system_mission_tag(&plan.start_system);
    let mut site_tags = BTreeMap::<String, String>::new();
    let mut batch_factories = BTreeMap::<String, String>::new();
    for job in &mut plan.print_jobs {
        job.mission_tag.clone_from(&mission_tag);
        job.site_tag = relay_site_tag(&job.system);
        if let Some(previous) = site_tags.insert(job.site_tag.clone(), job.system.clone())
            && previous != job.system
        {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "relay tag collision between planned sites {previous} and {}",
                    job.system
                ),
            ));
        }
        if let Some(batch_tag) = job.batch_tag.as_ref() {
            if batch_tag.chars().count() > MAX_DEVICE_TAG_CHARS {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    format!("saved print batch tag exceeds {MAX_DEVICE_TAG_CHARS} characters"),
                ));
            }
            if let Some(previous) =
                batch_factories.insert(batch_tag.clone(), job.factory_code.clone())
                && previous != job.factory_code
            {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    format!(
                        "print batch tag {batch_tag} is shared by factories {previous} and {}",
                        job.factory_code
                    ),
                ));
            }
        }
    }
    Ok(())
}

async fn ensure_planned_active_coverage(client: &Client, plan: &MissionPlan) -> AnyResult<()> {
    let mut required = plan
        .network
        .active_relay_systems
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    required.insert(plan.start_system.clone());

    let handles = client.devices().find().owned().collect().await?;
    let mut active = BTreeSet::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        let kind = device_type(&snapshot);
        let relay_capable = device_has_feature(&snapshot, "relay")
            || kind == Some(FTL_RELAY)
            || kind == Some(SYSTEM_HUB)
            || kind == Some(DEEP_SPACE_RELAY);
        if !relay_capable
            || device_status(&snapshot) != Some(RELAYING)
            || snapshot.relationships.stowed_in.is_some()
            || snapshot.relationships.attached_to.is_some()
        {
            continue;
        }
        if let Some(location) = device_location(&snapshot)
            && let Some(system) = resolve_system(location, &required)
        {
            active.insert(system);
        }
    }

    let missing = required.difference(&active).cloned().collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "planned account-owned relay coverage is no longer relaying at: {}; restore those relays or use --rebuild-plan before continuing",
                missing.join(", ")
            ),
        ));
    }
    Ok(())
}

async fn reconcile_supply_plan(client: &Client, plan: &mut MissionPlan) -> AnyResult<()> {
    if plan.supply.is_none() {
        return Ok(());
    }
    let stop_states = plan
        .stops
        .iter()
        .map(|stop| (stop.completed, stop.relay_code.clone(), stop.system.clone()))
        .collect::<Vec<_>>();
    let handles = client.devices().find().collect().await?;
    let mut devices = BTreeMap::<String, Device>::new();
    for handle in handles {
        if let Ok(snapshot) = handle.snapshot().await {
            devices.insert(handle.id().as_str().to_owned(), snapshot);
        }
    }
    let vessel_code = plan.vessel_code.clone();
    let hub_location = plan.hub_location.clone();
    let supply = plan.supply.as_mut().expect("supply checked above");

    for restock in &mut supply.restocks {
        restock.completed = restock.relay_stop_indices.iter().all(|stop_index| {
            let (stop_completed, relay_code, system) = &stop_states[*stop_index];
            if *stop_completed {
                return true;
            }
            let Some(code) = relay_code.as_deref() else {
                return false;
            };
            devices.get(code).is_some_and(|device| {
                device
                    .relationships
                    .stowed_in
                    .as_ref()
                    .is_some_and(|container| container.id.as_str() == vessel_code.as_str())
                    || (device.relationships.attached_to.is_none()
                        && device_location(device)
                            .is_some_and(|location| designation_in_system(location, system)))
            })
        });
    }

    let carrier_duties = supply
        .carriers
        .iter()
        .map(|carrier| {
            let next_restock = carrier.restock_indices.iter().find_map(|index| {
                (!supply.restocks[*index].completed)
                    .then(|| supply.restocks[*index].location.clone())
            });
            let has_pending_restock = next_restock.is_some();
            let destination = next_restock.unwrap_or_else(|| hub_location.clone());
            (carrier.code.clone(), (destination, has_pending_restock))
        })
        .collect::<BTreeMap<_, _>>();
    for carrier in &mut supply.carriers {
        if let Some(device) = devices.get(&carrier.code) {
            let (destination, has_pending_restock) = carrier_duties
                .get(&carrier.code)
                .expect("every supply carrier has a reconciliation duty");
            let observed_dispatched = device_at(device, destination)
                || (device.travel.is_some()
                    && travel_destination(device) == Some(destination.as_str()));
            // A staged carrier can leave the currently relayed network before
            // its rendezvous becomes reachable. In that case the managed device
            // snapshot may remain at its last known pre-dispatch state. Never
            // downgrade a durable dispatch checkpoint merely because current
            // observation cannot prove the carrier is still en route.
            carrier.dispatched |= observed_dispatched;
            carrier.returned_home = !*has_pending_restock && device_at(device, &hub_location);
        }
    }
    Ok(())
}

async fn migrate_legacy_relay_devices(client: &Client, plan: &MissionPlan) -> AnyResult<usize> {
    if plan.legacy_mission_tags.is_empty() {
        return Ok(0);
    }
    let desired = relay_system_mission_tag(&plan.start_system);
    let handles = client
        .devices()
        .refresh_many()
        .page_size(50)
        .collect()
        .await?;
    let mut migrated = 0usize;
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        let removable = snapshot
            .tags
            .iter()
            .filter(|tag| {
                plan.legacy_mission_tags.contains(*tag)
                    && is_opaque_relay_mission_tag(tag)
                    && *tag != &desired
            })
            .cloned()
            .collect::<Vec<_>>();
        if removable.is_empty() {
            continue;
        }
        let add_tags = (!snapshot.tags.contains(&desired)).then_some(vec![desired.clone()]);
        let operation = handle
            .configure(raw::devices::DeviceConfiguration {
                add_tags,
                remove_tags: Some(removable.clone()),
                tags: None,
                ..Default::default()
            })
            .await?;
        ensure_operation_accepted(&operation).await?;
        migrated += 1;
        info!(
            device = %handle.id().as_str(),
            new_tag = %desired,
            old_tags = ?removable,
            "migrated legacy relay mission tag"
        );
    }
    Ok(migrated)
}

async fn reconcile_plan(client: &Client, plan: &mut MissionPlan) -> AnyResult<()> {
    normalize_print_job_tags(plan)?;
    migrate_legacy_relay_devices(client, plan).await?;
    ensure_planned_active_coverage(client, plan).await?;
    let mission_tag = relay_system_mission_tag(&plan.start_system);
    let mission_tag_aliases = relay_mission_tag_aliases(plan);
    let site_tag_systems = plan
        .print_jobs
        .iter()
        .filter(|job| job.batch_tag.is_none())
        .map(|job| (job.site_tag.clone(), job.system.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut batch_tag_indices = BTreeMap::<String, Vec<usize>>::new();
    for (index, job) in plan.print_jobs.iter().enumerate() {
        if let Some(batch_tag) = job.batch_tag.as_ref() {
            batch_tag_indices
                .entry(batch_tag.clone())
                .or_default()
                .push(index);
        }
    }
    let handles = client
        .devices()
        .refresh_many()
        .with_tag(mission_tag.clone())
        .page_size(50)
        .collect()
        .await?;
    let mut tagged_sites = BTreeMap::new();
    let mut tagged_batches = BTreeMap::<String, Vec<String>>::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if !snapshot.tags.iter().any(|tag| tag == &mission_tag) {
            continue;
        }
        let matching_batches = snapshot
            .tags
            .iter()
            .filter(|tag| batch_tag_indices.contains_key(*tag))
            .cloned()
            .collect::<BTreeSet<_>>();
        let matching_systems = snapshot
            .tags
            .iter()
            .filter_map(|tag| site_tag_systems.get(tag))
            .cloned()
            .collect::<BTreeSet<_>>();
        if matching_batches.len() + matching_systems.len() > 1 {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "mission-tagged relay {} matches multiple print correlations",
                    handle.id().as_str()
                ),
            ));
        }
        let code = handle.id().as_str().to_owned();
        if let Some(batch_tag) = matching_batches.into_iter().next() {
            tagged_batches.entry(batch_tag).or_default().push(code);
        } else if let Some(system) = matching_systems.into_iter().next()
            && let Some(previous) = tagged_sites.insert(system.clone(), code.clone())
        {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("multiple mission-tagged relays exist for {system}: {previous}, {code}"),
            ));
        }
    }

    for job in &mut plan.print_jobs {
        if let Some(code) = tagged_sites.get(&job.system) {
            job.relay_code = Some(code.clone());
            job.submission_started = true;
            job.submitted = true;
        }
    }

    let mut assigned_codes = plan
        .print_jobs
        .iter()
        .filter_map(|job| job.relay_code.clone())
        .collect::<BTreeSet<_>>();
    for (batch_tag, indices) in &batch_tag_indices {
        let mut codes = tagged_batches.remove(batch_tag).unwrap_or_default();
        codes.sort();
        codes.dedup();
        if codes.len() > indices.len() {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "print batch {batch_tag} produced {} relays for {} planned sites",
                    codes.len(),
                    indices.len()
                ),
            ));
        }
        codes.retain(|code| !assigned_codes.contains(code));
        let unassigned = indices
            .iter()
            .copied()
            .filter(|index| plan.print_jobs[*index].relay_code.is_none())
            .collect::<Vec<_>>();
        for (index, code) in unassigned.into_iter().zip(codes) {
            assigned_codes.insert(code.clone());
            let job = &mut plan.print_jobs[index];
            job.relay_code = Some(code);
            job.submission_started = true;
            job.submitted = true;
        }
    }

    let printing_status = printing_status_in_system(client, &plan.hub_location, &[], &[]).await?;
    let factory_job_tags = printing_status
        .factories
        .into_iter()
        .map(|factory| {
            let mut jobs = Vec::<BTreeSet<String>>::new();
            if let Some(active) = factory.active {
                jobs.push(active.tags.into_iter().collect());
            }
            jobs.extend(
                factory
                    .queued
                    .into_iter()
                    .map(|job| job.tags.into_iter().collect()),
            );
            (factory.code, jobs)
        })
        .collect::<BTreeMap<_, _>>();

    for index in 0..plan.print_jobs.len() {
        if plan.print_jobs[index].relay_code.is_some() {
            plan.print_jobs[index].submission_started = true;
            plan.print_jobs[index].submitted = true;
            continue;
        }
        let correlation_tag = print_job_correlation_tag(&plan.print_jobs[index]);
        let queued = factory_job_tags
            .get(&plan.print_jobs[index].factory_code)
            .is_some_and(|jobs| {
                jobs.iter().any(|tags| {
                    mission_tag_aliases.iter().any(|tag| tags.contains(tag))
                        && tags.contains(correlation_tag)
                })
            });
        if queued {
            plan.print_jobs[index].submission_started = true;
            plan.print_jobs[index].submitted = true;
            continue;
        }
        if let Some(operation_id) = plan.print_jobs[index].operation_id.clone() {
            let operation = client.operations().get(OperationId::from(operation_id));
            let outcome = operation.outcome().await?;
            if matches!(
                outcome.status,
                OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
            ) {
                let job = &mut plan.print_jobs[index];
                job.submission_started = false;
                job.operation_id = None;
                job.submitted = false;
            } else {
                plan.print_jobs[index].submitted = true;
            }
        }
    }

    assign_safe_legacy_print_batches(plan);
    normalize_print_job_tags(plan)?;

    if let Some(job) = plan
        .print_jobs
        .iter()
        .find(|job| job.submission_started && job.operation_id.is_none() && !job.submitted)
    {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "print submission for {} began before the previous process stopped, but no durable operation ID or tagged queue entry is visible; refusing to resubmit automatically",
                job.system
            ),
        ));
    }

    for index in 0..plan.stops.len() {
        if plan.stops[index].action == StopAction::DeployAndActivate
            && plan.stops[index].relay_code.is_none()
        {
            let system = plan.stops[index].system.clone();
            plan.stops[index].relay_code = plan
                .print_jobs
                .iter()
                .find(|job| job.system == system)
                .and_then(|job| job.relay_code.clone());
        }
        plan.stops[index].completed = false;
        let Some(code) = plan.stops[index].relay_code.clone() else {
            continue;
        };
        // Reconciliation consumes the census-backed relay projection.
        let Ok(handle) = projected_device(client, &code).await else {
            continue;
        };
        let snapshot = handle.snapshot().await?;
        let actual_location = device_location(&snapshot).map(str::to_owned);
        let correctly_placed = actual_location
            .as_deref()
            .is_some_and(|location| designation_in_system(location, &plan.stops[index].system));
        if plan.stops[index].action == StopAction::DeployAndActivate
            && snapshot.relationships.stowed_in.is_none()
            && correctly_placed
            && let Some(location) = actual_location
        {
            plan.stops[index].location = location;
        }
        if device_status(&snapshot) == Some(RELAYING) && correctly_placed {
            let network = handle.network().await?;
            let upstream = expected_upstream_systems(plan, index);
            plan.stops[index].completed = network.connections.iter().any(|connection| {
                connection
                    .star
                    .as_deref()
                    .is_some_and(|system| upstream.contains(system))
            });
        }
    }

    reconcile_supply_plan(client, plan).await?;

    let replicant = client
        .replicants()
        .get_owned(&plan.replicant_code)
        .await?
        .snapshot()
        .await?;
    let supply_home = plan
        .supply
        .as_ref()
        .is_none_or(|supply| supply.carriers.iter().all(|carrier| carrier.returned_home));
    let dsr_carrier_home = if let Some(code) = plan.dsr_carrier_code.as_deref() {
        // Reconciliation consumes the census-backed carrier projection.
        match projected_device(client, code).await {
            Ok(handle) => match handle.snapshot().await {
                Ok(carrier) => device_at(&carrier, &plan.hub_location),
                Err(_) => false,
            },
            Err(_) => false,
        }
    } else {
        true
    };
    plan.returned_to_hub = plan.stops.iter().all(|stop| stop.completed)
        && replicant.travel.is_none()
        && replicant
            .location
            .as_ref()
            .is_some_and(|location| location.id.as_str() == plan.hub_location.as_str())
        && supply_home
        && dsr_carrier_home;
    Ok(())
}

fn next_trip_stop_indices(stops: &[RelayStop], transport_capacity: usize) -> Vec<usize> {
    let mut indices = Vec::new();
    let mut deploy_count = 0usize;

    for (index, stop) in stops.iter().enumerate() {
        if stop.completed {
            continue;
        }
        if stop_uses_vessel_stow(stop) {
            if deploy_count >= transport_capacity {
                break;
            }
            deploy_count += 1;
        }
        indices.push(index);
    }

    indices
}

fn trip_relays_ready(stops: &[RelayStop], indices: &[usize]) -> bool {
    indices.iter().all(|index| {
        let stop = &stops[*index];
        stop.action != StopAction::DeployAndActivate || stop.relay_code.is_some()
    })
}

fn trip_deploy_count(stops: &[RelayStop], indices: &[usize]) -> usize {
    indices
        .iter()
        .filter(|index| stops[**index].action == StopAction::DeployAndActivate)
        .count()
}

async fn current_transport_capacity(client: &Client, plan: &MissionPlan) -> AnyResult<usize> {
    // Capacity planning reads the live mission-vessel projection.
    let vessel = projected_device(client, &plan.vessel_code)
        .await?
        .snapshot()
        .await?;
    let free = vessel
        .stow_capacity
        .unwrap_or(0)
        .saturating_sub(vessel.stow_used.unwrap_or(0));

    let mut stowed_mission_relays = 0usize;
    let mission_codes = plan
        .stops
        .iter()
        .filter(|stop| stop_uses_vessel_stow(stop) && !stop.completed)
        .filter_map(|stop| stop.relay_code.as_deref())
        .collect::<BTreeSet<_>>();
    let handles = if mission_codes
        .iter()
        .all(|code| client.devices().cached(code).is_some())
    {
        mission_codes
            .iter()
            .filter_map(|code| client.devices().cached(code))
            .collect()
    } else {
        client
            .devices()
            .refresh_many()
            .with_tag(relay_system_mission_tag(&plan.start_system))
            .page_size(50)
            .collect()
            .await?
    };
    for handle in handles {
        if !mission_codes.contains(handle.id().as_str()) {
            continue;
        }
        let snapshot = handle.snapshot().await?;
        if snapshot
            .relationships
            .stowed_in
            .as_ref()
            .is_some_and(|container| container.id.as_str() == plan.vessel_code.as_str())
        {
            stowed_mission_relays += 1;
        }
    }

    let total = free.saturating_add(i64::try_from(stowed_mission_relays)?);
    Ok(usize::try_from(total)?)
}

async fn wait_for_trip_relays(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
    indices: &[usize],
) -> AnyResult<()> {
    if trip_relays_ready(&plan.stops, indices) {
        return Ok(());
    }

    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        reconcile_plan(client, plan).await?;
        save_plan(&config.plan_path, plan)?;
        if trip_relays_ready(&plan.stops, indices) {
            return Ok(());
        }

        if plan.print_jobs.iter().any(|job| !job.submitted) {
            submit_print_jobs(client, config, plan).await?;
            continue;
        }

        // Plans created by older builds may already have submitted a parent
        // relay/DSR that is now waiting for component devices. Reconcile the
        // same recursive prerequisite bundle even when there is no remaining
        // parent submission, so upgrading cannot leave that Autofactory job
        // permanently blocked.
        if !prepare_relay_print_prerequisites(client, config, plan).await? {
            wait_for_relevant_event(&mut watch, deadline, &["print.completed"]).await?;
            continue;
        }

        if Instant::now() >= deadline {
            let waiting = indices
                .iter()
                .filter_map(|index| {
                    let stop = &plan.stops[*index];
                    (stop.action == StopAction::DeployAndActivate && stop.relay_code.is_none())
                        .then_some(stop.system.as_str())
                })
                .collect::<Vec<_>>()
                .join(", ");
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for next relay deployment load: {waiting}"),
            ));
        }
        wait_for_relevant_event(&mut watch, deadline, &["print.completed"]).await?;
    }
}

fn restock_relay_indices_for_carrier(plan: &MissionPlan, carrier_code: &str) -> Vec<usize> {
    let Some(supply) = plan.supply.as_ref() else {
        return Vec::new();
    };
    let Some(carrier) = supply
        .carriers
        .iter()
        .find(|carrier| carrier.code.as_str() == carrier_code)
    else {
        return Vec::new();
    };
    carrier
        .restock_indices
        .iter()
        .filter(|restock_index| !supply.restocks[**restock_index].completed)
        .flat_map(|restock_index| {
            supply.restocks[*restock_index]
                .relay_stop_indices
                .iter()
                .copied()
        })
        .filter(|stop_index| !plan.stops[*stop_index].completed)
        .collect()
}

fn next_restock_for_carrier(
    plan: &MissionPlan,
    carrier_code: &str,
) -> Option<(usize, RelayRestock)> {
    let supply = plan.supply.as_ref()?;
    let carrier = supply
        .carriers
        .iter()
        .find(|carrier| carrier.code.as_str() == carrier_code)?;
    carrier.restock_indices.iter().find_map(|restock_index| {
        (!supply.restocks[*restock_index].completed)
            .then(|| (*restock_index, supply.restocks[*restock_index].clone()))
    })
}

fn due_restock(plan: &MissionPlan) -> Option<usize> {
    let supply = plan.supply.as_ref()?;
    supply
        .restocks
        .iter()
        .enumerate()
        .find_map(|(index, restock)| {
            (!restock.completed && plan.stops[restock.boundary_stop_index].completed)
                .then_some(index)
        })
}

fn carrier_all_assigned_relay_codes(plan: &MissionPlan, carrier_code: &str) -> BTreeSet<String> {
    let Some(supply) = plan.supply.as_ref() else {
        return BTreeSet::new();
    };
    supply
        .restocks
        .iter()
        .filter(|restock| restock.carrier_code.as_str() == carrier_code)
        .flat_map(|restock| restock.relay_stop_indices.iter())
        .filter_map(|stop_index| plan.stops[*stop_index].relay_code.clone())
        .collect()
}

async fn ensure_carrier_claim(client: &Client, plan: &MissionPlan, code: &str) -> AnyResult<()> {
    let mission_tag = relay_system_mission_tag(&plan.start_system);
    let aliases = relay_mission_tag_aliases(plan);
    // Supply planning already selected this projection-backed carrier.
    let handle = projected_device(client, code).await?;
    let snapshot = handle.snapshot().await?;
    let conflicting = snapshot
        .tags
        .iter()
        .filter(|tag| !aliases.contains(*tag) && workflow_tag_reserved(tag.as_str()))
        .cloned()
        .collect::<Vec<_>>();
    if !conflicting.is_empty() {
        return Err(app_error(
            io::ErrorKind::WouldBlock,
            format!(
                "planned relay supply carrier {code} is now reserved by {}",
                conflicting.join(", ")
            ),
        ));
    }
    if !snapshot.tags.iter().any(|tag| tag == &mission_tag) {
        let operation = handle
            .configure(raw::devices::DeviceConfiguration {
                add_tags: Some(vec![mission_tag]),
                remove_tags: None,
                tags: None,
                ..Default::default()
            })
            .await?;
        ensure_operation_accepted(&operation).await?;
    }
    Ok(())
}

async fn release_carrier_claim(client: &Client, plan: &MissionPlan, code: &str) -> AnyResult<()> {
    let aliases = relay_mission_tag_aliases(plan);
    // Claim cleanup uses the same live carrier projection.
    let handle = projected_device(client, code).await?;
    let snapshot = handle.snapshot().await?;
    let removable = snapshot
        .tags
        .iter()
        .filter(|tag| aliases.contains(*tag))
        .cloned()
        .collect::<Vec<_>>();
    if !removable.is_empty() {
        let operation = handle
            .configure(raw::devices::DeviceConfiguration {
                add_tags: None,
                remove_tags: Some(removable),
                tags: None,
                ..Default::default()
            })
            .await?;
        ensure_operation_accepted(&operation).await?;
    }
    Ok(())
}

fn plan_has_pending_dsr(plan: &MissionPlan) -> bool {
    plan.stops
        .iter()
        .any(|stop| stop_requires_attachment_carrier(stop) && !stop.completed)
}

async fn ensure_dsr_carrier_assignment(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
) -> AnyResult<()> {
    if !plan_has_pending_dsr(plan) {
        return Ok(());
    }
    if let Some(code) = plan.dsr_carrier_code.clone() {
        ensure_carrier_claim(client, plan, &code).await?;
        return Ok(());
    }

    let mission_tag_aliases = relay_mission_tag_aliases(plan);
    let supply_codes = plan
        .supply
        .iter()
        .flat_map(|supply| supply.carriers.iter().map(|carrier| carrier.code.clone()))
        .collect::<BTreeSet<_>>();
    let handles = client.devices().find().owned().collect().await?;
    let mut candidates = Vec::<(i64, String)>::new();
    for handle in handles {
        let code = handle.id().as_str();
        if code == plan.vessel_code.as_str() || supply_codes.contains(code) {
            continue;
        }
        let snapshot = handle.snapshot().await?;
        let conflicting = snapshot
            .tags
            .iter()
            .any(|tag| !mission_tag_aliases.contains(tag) && workflow_tag_reserved(tag.as_str()));
        if conflicting
            || snapshot.attach_capacity.unwrap_or(0) <= 0
            || snapshot.travel.is_some()
            || snapshot.relationships.attached_to.is_some()
            || snapshot.relationships.stowed_in.is_some()
            || snapshot.relationships.controller.is_some()
            || snapshot.relationships.hosting_replicant.is_some()
            || !snapshot.relationships.attached_devices.is_empty()
            || !device_has_command(&snapshot, "attach")
            || !device_has_command(&snapshot, "travel")
            || !device_location(&snapshot)
                .is_some_and(|location| designation_in_system(location, &plan.start_system))
        {
            continue;
        }
        candidates.push((snapshot.attach_capacity.unwrap_or(0), code.to_owned()));
    }
    candidates.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    let Some((capacity, code)) = candidates.into_iter().next() else {
        return Err(classified_error(
            FailureClass::ConnectivityDependency,
            io::ErrorKind::NotFound,
            format!(
                "Deep Space Relay Station deployment from {} requires an idle attachment carrier in system {}",
                plan.hub_location, plan.start_system
            ),
        ));
    };

    plan.dsr_carrier_code = Some(code.clone());
    ensure_carrier_claim(client, plan, &code).await?;
    save_plan(&config.plan_path, plan)?;
    info!(
        carrier = %code,
        attach_capacity = capacity,
        hub = %plan.hub_location,
        "assigned dedicated attachment carrier for Deep Space Relay Station deployment"
    );
    Ok(())
}

async fn ensure_dsr_carrier_at_hub(
    client: &Client,
    config: &Config,
    plan: &MissionPlan,
    carrier_code: &str,
) -> AnyResult<()> {
    // Travel events maintain the DSR carrier projection.
    let carrier = projected_device(client, carrier_code)
        .await?
        .snapshot()
        .await?;
    if device_at(&carrier, &plan.hub_location) {
        return Ok(());
    }
    if carrier.travel.is_some() && travel_destination(&carrier) != Some(plan.hub_location.as_str())
    {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "DSR carrier {carrier_code} is travelling to {:?}, not hub {}",
                travel_destination(&carrier),
                plan.hub_location
            ),
        ));
    }
    start_device_travel(client, carrier_code, &plan.hub_location).await?;
    wait_device_at_location(client, config, carrier_code, &plan.hub_location).await
}

async fn ensure_dsr_carrier_dispatched(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
    index: usize,
) -> AnyResult<String> {
    ensure_dsr_carrier_assignment(client, config, plan).await?;
    let carrier_code = plan.dsr_carrier_code.clone().ok_or_else(|| {
        app_error(
            io::ErrorKind::NotFound,
            "Deep Space Relay Station stop has no assigned attachment carrier",
        )
    })?;
    let stop = plan.stops[index].clone();
    let relay_code = stop.relay_code.as_deref().ok_or_else(|| {
        app_error(
            io::ErrorKind::NotFound,
            format!(
                "stop {} has no assigned Deep Space Relay Station",
                stop.system
            ),
        )
    })?;

    transfer_trip_relays(client, config, plan, &[index]).await?;
    ensure_carrier_claim(client, plan, &carrier_code).await?;

    // Relay assignment and transfer steps keep this stop projection current.
    let relay = projected_device(client, relay_code)
        .await?
        .snapshot()
        .await?;
    if relay.relationships.attached_to.is_none()
        && relay.relationships.stowed_in.is_none()
        && device_location(&relay)
            .is_some_and(|location| designation_in_system(location, &stop.system))
    {
        return Ok(carrier_code);
    }
    if let Some(container) = relay.relationships.stowed_in.as_ref() {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "Deep Space Relay Station {relay_code} is stowed in {}; DSRs must be transported attached to carrier {carrier_code}",
                container.id.as_str()
            ),
        ));
    }
    if let Some(attached_to) = relay.relationships.attached_to.as_ref() {
        if attached_to.id.as_str() != carrier_code {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "Deep Space Relay Station {relay_code} is attached to {}, not planned carrier {carrier_code}",
                    attached_to.id.as_str()
                ),
            ));
        }
        // Carrier travel waits update the projection before this check.
        let carrier = projected_device(client, &carrier_code)
            .await?
            .snapshot()
            .await?;
        if device_at(&carrier, &stop.location)
            || (carrier.travel.is_some()
                && travel_destination(&carrier) == Some(stop.location.as_str()))
        {
            return Ok(carrier_code);
        }
        if !device_at(&carrier, &plan.hub_location) {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "DSR carrier {carrier_code} is at {:?} with {relay_code} attached; expected hub {} or destination {}",
                    device_location(&carrier),
                    plan.hub_location,
                    stop.location
                ),
            ));
        }
        start_device_travel(client, &carrier_code, &stop.location).await?;
        return Ok(carrier_code);
    }

    if device_location(&relay) != Some(plan.hub_location.as_str()) {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "Deep Space Relay Station {relay_code} is at {:?}; expected hub {} before carrier loading",
                device_location(&relay),
                plan.hub_location
            ),
        ));
    }

    ensure_dsr_carrier_at_hub(client, config, plan, &carrier_code).await?;
    ensure_relay_attachable(client, config, relay_code).await?;
    // Hub travel and attachability checks keep both devices current.
    let carrier = projected_device(client, &carrier_code).await?;
    let snapshot = carrier.snapshot().await?;
    if snapshot.attach_capacity.unwrap_or(0) <= 0 {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("DSR carrier {carrier_code} has no attachment capacity"),
        ));
    }
    let operation = carrier
        .attach(raw::devices::TargetsCommand {
            device: Some(relay_code.to_owned()),
            devices: None,
            target: None,
            targets: None,
        })
        .await?;
    ensure_operation_accepted(&operation).await?;
    wait_for_device(client, config, relay_code, |device| {
        device
            .relationships
            .attached_to
            .as_ref()
            .is_some_and(|attached_to| attached_to.id.as_str() == carrier_code)
    })
    .await?;
    info!(
        relay = %relay_code,
        carrier = %carrier_code,
        destination = %stop.location,
        "loaded Deep Space Relay Station onto attachment carrier"
    );
    start_device_travel(client, &carrier_code, &stop.location).await?;
    Ok(carrier_code)
}

async fn detach_dsr_at_stop(
    client: &Client,
    config: &Config,
    stop: &RelayStop,
    relay_code: &str,
    carrier_code: &str,
) -> AnyResult<()> {
    // Stop execution starts from the live assigned-relay projection.
    let relay = projected_device(client, relay_code)
        .await?
        .snapshot()
        .await?;
    if relay.relationships.attached_to.is_none()
        && relay.relationships.stowed_in.is_none()
        && device_location(&relay)
            .is_some_and(|location| designation_in_system(location, &stop.system))
    {
        return Ok(());
    }
    wait_device_at_location(client, config, carrier_code, &stop.location).await?;
    // The carrier arrival wait keeps attached relay state current via SSE.
    let relay = projected_device(client, relay_code)
        .await?
        .snapshot()
        .await?;
    if relay
        .relationships
        .attached_to
        .as_ref()
        .is_none_or(|carrier| carrier.id.as_str() != carrier_code)
    {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "Deep Space Relay Station {relay_code} is not attached to carrier {carrier_code} at {}",
                stop.location
            ),
        ));
    }
    // Carrier and payload projections were verified immediately above.
    let operation = projected_device(client, carrier_code)
        .await?
        .command(raw::devices::DeviceCommand::Detach(
            raw::devices::TargetsCommand {
                device: Some(relay_code.to_owned()),
                devices: None,
                target: None,
                targets: None,
            },
        ))
        .await?;
    ensure_operation_accepted(&operation).await?;
    wait_for_device(client, config, relay_code, |device| {
        device.relationships.attached_to.is_none()
            && device.relationships.stowed_in.is_none()
            && device_location(device)
                .is_some_and(|location| designation_in_system(location, &stop.system))
    })
    .await?;
    Ok(())
}

async fn send_dsr_carrier_home(
    client: &Client,
    plan: &MissionPlan,
    carrier_code: &str,
) -> AnyResult<()> {
    match start_device_travel(client, carrier_code, &plan.hub_location).await {
        Ok(()) => Ok(()),
        Err(error) if is_out_of_comms_error(&error) => {
            warn!(
                carrier = %carrier_code,
                hub = %plan.hub_location,
                error = %error,
                "DSR carrier return command is temporarily out of comms; final reconciliation will retry"
            );
            Ok(())
        }
        Err(error) => Err(error),
    }
}

async fn finish_dsr_carrier(client: &Client, config: &Config, plan: &MissionPlan) -> AnyResult<()> {
    let Some(carrier_code) = plan.dsr_carrier_code.as_deref() else {
        return Ok(());
    };
    start_device_travel(client, carrier_code, &plan.hub_location).await?;
    wait_device_at_location(client, config, carrier_code, &plan.hub_location).await?;
    release_carrier_claim(client, plan, carrier_code).await
}

fn relay_destination_matches(actual: &str, destination: &str) -> bool {
    actual.eq_ignore_ascii_case(destination)
        || (!destination.contains('-') && designation_in_system(actual, destination))
}

fn device_at(device: &Device, destination: &str) -> bool {
    device.travel.is_none()
        && device_location(device)
            .is_some_and(|actual| relay_destination_matches(actual, destination))
}

fn travel_destination(device: &Device) -> Option<&str> {
    device.travel.as_ref().and_then(|travel| {
        travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str())
    })
}

async fn start_device_travel(client: &Client, code: &str, destination: &str) -> AnyResult<()> {
    // Travel events maintain device location and route state locally.
    let handle = projected_device(client, code).await?;
    let snapshot = handle.snapshot().await?;
    if device_at(&snapshot, destination) {
        return Ok(());
    }
    if snapshot.travel.is_some() {
        if travel_destination(&snapshot)
            .is_some_and(|planned| relay_destination_matches(planned, destination))
        {
            return Ok(());
        }
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "supply carrier {code} is already travelling to {:?}, not {destination}",
                travel_destination(&snapshot)
            ),
        ));
    }
    info!(carrier = %code, destination = %destination, "dispatching relay supply carrier");
    let operation = handle
        .command(raw::devices::DeviceCommand::Travel {
            destination: destination.to_owned(),
            dry_run: None,
            via: None,
        })
        .await?;
    ensure_operation_accepted(&operation).await
}

async fn wait_device_at_location(
    client: &Client,
    config: &Config,
    code: &str,
    destination: &str,
) -> AnyResult<()> {
    // The event-backed wait begins from the current projection.
    let handle = projected_device(client, code).await?;
    let snapshot = handle.snapshot().await?;
    if device_at(&snapshot, destination) {
        return Ok(());
    }
    if snapshot.travel.is_some()
        && !travel_destination(&snapshot)
            .is_some_and(|planned| relay_destination_matches(planned, destination))
    {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "supply carrier {code} is travelling to {:?}, not {destination}",
                travel_destination(&snapshot)
            ),
        ));
    }
    let mut watch = handle.watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for supply carrier {code} at {destination}"),
            ));
        }
        match timeout(remaining.min(Duration::from_secs(60)), watch.next()).await {
            Ok(Some(device)) if device_at(&device, destination) => return Ok(()),
            Ok(Some(device))
                if device.travel.is_some()
                    && !travel_destination(&device)
                        .is_some_and(|planned| relay_destination_matches(planned, destination)) =>
            {
                return Err(app_error(
                    io::ErrorKind::Other,
                    format!(
                        "supply carrier {code} changed destination to {:?} while waiting for {destination}",
                        travel_destination(&device)
                    ),
                ));
            }
            Ok(Some(_)) => {}
            Ok(None) | Err(_) => {
                let refreshed = handle.refresh().await?.snapshot().await?;
                if device_at(&refreshed, destination) {
                    return Ok(());
                }
            }
        }
    }
}

async fn wait_for_carrier_payload(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
    carrier_code: &str,
) -> AnyResult<Vec<usize>> {
    let indices = restock_relay_indices_for_carrier(plan, carrier_code);
    if indices.is_empty() {
        return Ok(indices);
    }
    wait_for_trip_relays(client, config, plan, &indices).await?;
    Ok(indices)
}

async fn ensure_relay_attachable(client: &Client, config: &Config, code: &str) -> AnyResult<()> {
    // Mission selection and travel keep the relay projection current.
    let handle = projected_device(client, code).await?;
    let mut snapshot = handle.snapshot().await?;
    let modular = snapshot
        .features
        .iter()
        .any(|feature| feature.as_str() == "modular")
        || snapshot
            .available_commands
            .iter()
            .any(|command| matches!(command.as_str(), "compact" | "unfurl"))
        || device_status(&snapshot)
            .is_some_and(|status| matches!(status, "compacting" | "compacted" | "unfurling"));
    if !modular || device_status(&snapshot) == Some("compacted") {
        return Ok(());
    }
    if device_status(&snapshot) == Some("compacting") {
        return wait_for_device(client, config, code, |device| {
            device_status(device) == Some("compacted")
        })
        .await;
    }
    if device_status(&snapshot) == Some("unfurling") {
        wait_for_device(client, config, code, |device| {
            device_status(device) != Some("unfurling")
        })
        .await?;
        snapshot = handle.refresh().await?.snapshot().await?;
        if device_status(&snapshot) == Some("compacted") {
            return Ok(());
        }
    }
    if !device_has_command(&snapshot, "compact") {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "relay {code} is {:?} and cannot currently be compacted for carrier attachment",
                device_status(&snapshot)
            ),
        ));
    }
    info!(relay = %code, "compacting relay for transport");
    let operation = handle.compact().await?;
    ensure_operation_accepted(&operation).await?;
    wait_for_device(client, config, code, |device| {
        device_status(device) == Some("compacted")
    })
    .await
}

async fn ensure_relay_unfurled(client: &Client, config: &Config, code: &str) -> AnyResult<()> {
    // Mission selection and travel keep the relay projection current.
    let handle = projected_device(client, code).await?;
    let mut snapshot = handle.snapshot().await?;
    if device_status(&snapshot) == Some(RELAYING) {
        return Ok(());
    }
    if device_status(&snapshot) == Some("compacting") {
        wait_for_device(client, config, code, |device| {
            device_status(device) == Some("compacted")
        })
        .await?;
        snapshot = handle.refresh().await?.snapshot().await?;
    }
    if device_status(&snapshot) == Some("compacted") {
        if !device_has_command(&snapshot, "unfurl") {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("relay {code} is compacted after deployment and does not advertise unfurl"),
            ));
        }
        info!(relay = %code, "unfurling transported relay before activation");
        let operation = handle.unfurl().await?;
        ensure_operation_accepted(&operation).await?;
    } else if device_status(&snapshot) != Some("unfurling") {
        return Ok(());
    }
    wait_for_device(client, config, code, |device| {
        device_status(device) != Some("compacted")
            && device_status(device) != Some("compacting")
            && device_status(device) != Some("unfurling")
    })
    .await
}

async fn attach_carrier_payload(
    client: &Client,
    config: &Config,
    plan: &MissionPlan,
    carrier_code: &str,
    indices: &[usize],
) -> AnyResult<()> {
    if indices.is_empty() {
        return Ok(());
    }
    transfer_trip_relays(client, config, plan, indices).await?;
    ensure_carrier_claim(client, plan, carrier_code).await?;
    // Carrier travel and claim steps maintain the supply projection.
    let handle = projected_device(client, carrier_code).await?;
    let carrier = handle.snapshot().await?;
    if !device_at(&carrier, &plan.hub_location) {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "supply carrier {carrier_code} must be at {} before loading, but is at {:?}",
                plan.hub_location,
                device_location(&carrier)
            ),
        ));
    }
    let allowed = carrier_all_assigned_relay_codes(plan, carrier_code);
    let attached = carrier
        .relationships
        .attached_devices
        .iter()
        .map(|device| device.id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let unexpected = attached.difference(&allowed).cloned().collect::<Vec<_>>();
    if !unexpected.is_empty() {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "supply carrier {carrier_code} contains non-mission attachments: {}",
                unexpected.join(", ")
            ),
        ));
    }

    let mut missing = Vec::new();
    for index in indices {
        if plan.stops[*index].completed {
            continue;
        }
        let code = plan.stops[*index].relay_code.as_deref().ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("stop {} has no assigned relay", plan.stops[*index].system),
            )
        })?;
        if attached.contains(code) {
            continue;
        }
        // Assigned payload devices are maintained by attachment events.
        let device = projected_device(client, code).await?.snapshot().await?;
        if device
            .relationships
            .attached_to
            .as_ref()
            .is_some_and(|attached_to| attached_to.id.as_str() == carrier_code)
        {
            continue;
        }
        if let Some(other) = device.relationships.attached_to.as_ref() {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "relay {code} is attached to {}, not planned supply carrier {carrier_code}",
                    other.id.as_str()
                ),
            ));
        }
        if let Some(other) = device.relationships.stowed_in.as_ref() {
            if other.id.as_str() == plan.vessel_code.as_str() {
                continue;
            }
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "relay {code} is stowed in {}, so it cannot be loaded onto supply carrier {carrier_code}",
                    other.id.as_str()
                ),
            ));
        }
        if device.relationships.attached_to.is_none()
            && device.travel.is_none()
            && device_location(&device)
                .is_some_and(|location| designation_in_system(location, &plan.stops[*index].system))
        {
            continue;
        }
        if device_location(&device) != Some(plan.hub_location.as_str()) {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "relay {code} is at {:?}; expected hub {} before supply-carrier loading",
                    device_location(&device),
                    plan.hub_location
                ),
            ));
        }
        ensure_relay_attachable(client, config, code).await?;
        missing.push(code.to_owned());
    }

    let capacity = carrier.attach_capacity.unwrap_or(0).max(0);
    if i64::try_from(attached.len().saturating_add(missing.len()))? > capacity {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "supply carrier {carrier_code} has attachment capacity {capacity}, but its planned preloaded payload needs {} slots",
                attached.len().saturating_add(missing.len())
            ),
        ));
    }
    if missing.is_empty() {
        return Ok(());
    }
    let operation = handle
        .attach(raw::devices::TargetsCommand {
            device: None,
            devices: Some(Value::Array(
                missing.iter().cloned().map(Value::String).collect(),
            )),
            target: None,
            targets: None,
        })
        .await?;
    ensure_operation_accepted(&operation).await?;
    for code in missing {
        wait_for_device(client, config, &code, |device| {
            device
                .relationships
                .attached_to
                .as_ref()
                .is_some_and(|attached_to| attached_to.id.as_str() == carrier_code)
        })
        .await?;
    }
    Ok(())
}

fn set_carrier_dispatched(plan: &mut MissionPlan, carrier_code: &str, dispatched: bool) {
    if let Some(supply) = plan.supply.as_mut()
        && let Some(carrier) = supply
            .carriers
            .iter_mut()
            .find(|carrier| carrier.code.as_str() == carrier_code)
    {
        carrier.dispatched = dispatched;
    }
}

fn checkpoint_carrier_dispatched(
    config: &Config,
    plan: &mut MissionPlan,
    carrier_code: &str,
) -> AnyResult<()> {
    set_carrier_dispatched(plan, carrier_code, true);
    save_plan(&config.plan_path, plan)
}

fn is_out_of_comms_error(error: &AnyError) -> bool {
    error
        .to_string()
        .to_ascii_lowercase()
        .contains("out of comms range")
}

async fn carrier_payload_satisfied(
    client: &Client,
    plan: &MissionPlan,
    carrier_code: &str,
    restock: &RelayRestock,
    indices: &[usize],
    attached: &BTreeSet<String>,
) -> AnyResult<bool> {
    for index in indices {
        let stop = &plan.stops[*index];
        if stop.completed {
            continue;
        }
        let Some(code) = stop.relay_code.as_deref() else {
            return Ok(false);
        };
        if attached.contains(code) {
            continue;
        }
        let Some(handle) = client.devices().cached(code) else {
            return Ok(false);
        };
        let device = handle.snapshot().await?;
        let attached_to_carrier = device
            .relationships
            .attached_to
            .as_ref()
            .is_some_and(|carrier| carrier.id.as_str() == carrier_code);
        if attached_to_carrier {
            continue;
        }
        let stowed_in_vessel = device
            .relationships
            .stowed_in
            .as_ref()
            .is_some_and(|container| container.id.as_str() == plan.vessel_code.as_str());
        let free_standing = device.travel.is_none()
            && device.relationships.attached_to.is_none()
            && device.relationships.stowed_in.is_none();
        let at_final_target = free_standing
            && device_location(&device)
                .is_some_and(|location| designation_in_system(location, &stop.system));
        let current_batch_at_restock = restock.relay_stop_indices.contains(index)
            && free_standing
            && device_location(&device) == Some(restock.location.as_str());
        if !stowed_in_vessel && !at_final_target && !current_batch_at_restock {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn ensure_carrier_dispatched(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
    carrier_code: &str,
    wait_for_payload: bool,
) -> AnyResult<bool> {
    let Some((_, restock)) = next_restock_for_carrier(plan, carrier_code) else {
        start_device_travel(client, carrier_code, &plan.hub_location).await?;
        checkpoint_carrier_dispatched(config, plan, carrier_code)?;
        return Ok(true);
    };

    let pending_indices = restock_relay_indices_for_carrier(plan, carrier_code);
    if !wait_for_payload && !trip_relays_ready(&plan.stops, &pending_indices) {
        return Ok(false);
    }
    // Dispatch reconciliation reads the live supply-carrier projection.
    let mut carrier = projected_device(client, carrier_code)
        .await?
        .snapshot()
        .await?;
    let attached = carrier
        .relationships
        .attached_devices
        .iter()
        .map(|device| device.id.as_str().to_owned())
        .collect::<BTreeSet<_>>();
    let payload_loaded = !pending_indices.is_empty()
        && carrier_payload_satisfied(
            client,
            plan,
            carrier_code,
            &restock,
            &pending_indices,
            &attached,
        )
        .await?;
    if device_at(&carrier, &restock.location)
        || (carrier.travel.is_some()
            && travel_destination(&carrier) == Some(restock.location.as_str()))
    {
        if !payload_loaded {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "supply carrier {carrier_code} is already dispatched to {} without its complete planned payload",
                    restock.location
                ),
            ));
        }
        checkpoint_carrier_dispatched(config, plan, carrier_code)?;
        return Ok(true);
    }

    if !payload_loaded && !device_at(&carrier, &plan.hub_location) {
        if carrier.travel.is_some()
            && travel_destination(&carrier) != Some(plan.hub_location.as_str())
        {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "supply carrier {carrier_code} is travelling to {:?} without its complete planned payload; expected it to return to {}",
                    travel_destination(&carrier),
                    plan.hub_location
                ),
            ));
        }
        start_device_travel(client, carrier_code, &plan.hub_location).await?;
        if !wait_for_payload {
            return Ok(false);
        }
        wait_device_at_location(client, config, carrier_code, &plan.hub_location).await?;
        // The carrier arrival wait updated this projection via SSE.
        carrier = projected_device(client, carrier_code)
            .await?
            .snapshot()
            .await?;
    }

    if device_at(&carrier, &plan.hub_location) && !payload_loaded {
        let indices = if trip_relays_ready(&plan.stops, &pending_indices) {
            pending_indices
        } else if wait_for_payload {
            wait_for_carrier_payload(client, config, plan, carrier_code).await?
        } else {
            return Ok(false);
        };
        attach_carrier_payload(client, config, plan, carrier_code, &indices).await?;
    } else if !payload_loaded {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "supply carrier {carrier_code} does not contain all remaining planned relay payload"
            ),
        ));
    }

    match start_device_travel(client, carrier_code, &restock.location).await {
        Ok(()) => {}
        Err(error) if is_out_of_comms_error(&error) => {
            // Recovery for a crash/SIGINT after the carrier departed but before
            // its dispatch checkpoint reached disk. Once a preloaded staged
            // carrier has left the current relay network, reissuing the same
            // command is rejected as out of comms. Adopt that carrier as already
            // dispatched and verify it authoritatively when the expanding relay
            // chain reaches its rendezvous.
            warn!(
                carrier = %carrier_code,
                destination = %restock.location,
                error = %error,
                "relay supply carrier is already out of comms; adopting its prior dispatch"
            );
        }
        Err(error) => return Err(error),
    }
    checkpoint_carrier_dispatched(config, plan, carrier_code)?;
    Ok(true)
}

async fn dispatch_ready_supply_carriers(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
) -> AnyResult<()> {
    let carrier_codes = plan
        .supply
        .as_ref()
        .map(|supply| {
            supply
                .carriers
                .iter()
                .filter(|carrier| !carrier.dispatched && !carrier.returned_home)
                .map(|carrier| carrier.code.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for code in carrier_codes {
        let _ = ensure_carrier_dispatched(client, config, plan, &code, false).await?;
    }
    Ok(())
}

async fn detach_restock_payload(
    client: &Client,
    config: &Config,
    plan: &MissionPlan,
    restock: &RelayRestock,
) -> AnyResult<()> {
    let mut attached = Vec::new();
    for index in &restock.relay_stop_indices {
        if plan.stops[*index].completed {
            continue;
        }
        let code = plan.stops[*index].relay_code.as_deref().ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("stop {} has no assigned relay", plan.stops[*index].system),
            )
        })?;
        // Restock payload membership is maintained by attachment events.
        let device = projected_device(client, code).await?.snapshot().await?;
        if device
            .relationships
            .attached_to
            .as_ref()
            .is_some_and(|carrier| carrier.id.as_str() == restock.carrier_code.as_str())
        {
            attached.push(code.to_owned());
            continue;
        }
        if device
            .relationships
            .stowed_in
            .as_ref()
            .is_some_and(|vessel| vessel.id.as_str() == plan.vessel_code.as_str())
        {
            continue;
        }
        if device.relationships.attached_to.is_none()
            && device.relationships.stowed_in.is_none()
            && device.travel.is_none()
            && device_location(&device).is_some_and(|location| {
                location == restock.location
                    || designation_in_system(location, &plan.stops[*index].system)
            })
        {
            continue;
        }
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "relay {code} is not attached to supply carrier {} at restock {}",
                restock.carrier_code, restock.location
            ),
        ));
    }
    if attached.is_empty() {
        return Ok(());
    }
    // Payload inspection above established the live restock carrier.
    let operation = projected_device(client, &restock.carrier_code)
        .await?
        .command(raw::devices::DeviceCommand::Detach(
            raw::devices::TargetsCommand {
                device: None,
                devices: Some(Value::Array(
                    attached.iter().cloned().map(Value::String).collect(),
                )),
                target: None,
                targets: None,
            },
        ))
        .await?;
    ensure_operation_accepted(&operation).await?;
    for code in attached {
        wait_for_device(client, config, &code, |device| {
            device.relationships.attached_to.is_none()
        })
        .await?;
    }
    Ok(())
}

async fn stow_restock_payload(
    client: &Client,
    config: &Config,
    plan: &MissionPlan,
    restock: &RelayRestock,
) -> AnyResult<()> {
    let mut to_stow = Vec::new();
    let mut already_stowed = 0usize;
    for index in &restock.relay_stop_indices {
        if plan.stops[*index].completed || stop_requires_attachment_carrier(&plan.stops[*index]) {
            continue;
        }
        let code = plan.stops[*index].relay_code.as_deref().ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("stop {} has no assigned relay", plan.stops[*index].system),
            )
        })?;
        // Restock relay placement is maintained by stow events.
        let device = projected_device(client, code).await?.snapshot().await?;
        if device
            .relationships
            .stowed_in
            .as_ref()
            .is_some_and(|container| container.id.as_str() == plan.vessel_code.as_str())
        {
            already_stowed += 1;
            continue;
        }
        if device.relationships.attached_to.is_some() || device.relationships.stowed_in.is_some() {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("relay {code} is still contained by another device during restock"),
            ));
        }
        if device.travel.is_none()
            && device_location(&device)
                .is_some_and(|location| designation_in_system(location, &plan.stops[*index].system))
        {
            continue;
        }
        if device_location(&device) != Some(restock.location.as_str()) {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "relay {code} is at {:?}; expected restock location {}",
                    device_location(&device),
                    restock.location
                ),
            ));
        }
        if !device_has_command(&device, "stow") {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("relay {code} does not currently advertise stow"),
            ));
        }
        to_stow.push(code.to_owned());
    }
    // Vessel capacity is part of the managed mission projection.
    let vessel = projected_device(client, &plan.vessel_code)
        .await?
        .snapshot()
        .await?;
    let free = vessel
        .stow_capacity
        .unwrap_or(0)
        .saturating_sub(vessel.stow_used.unwrap_or(0));
    if i64::try_from(to_stow.len())? > free {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "vessel {} has {free} free stow slots at {} but restock needs {} additional relay(s) ({} already stowed)",
                plan.vessel_code,
                restock.location,
                to_stow.len(),
                already_stowed
            ),
        ));
    }
    for code in to_stow {
        // The placement pass above populated each relay handle.
        let operation = projected_device(client, &code)
            .await?
            .stow(Some(plan.vessel_code.clone()))
            .await?;
        ensure_operation_accepted(&operation).await?;
        wait_for_device(client, config, &code, |device| {
            device
                .relationships
                .stowed_in
                .as_ref()
                .is_some_and(|container| container.id.as_str() == plan.vessel_code.as_str())
        })
        .await?;
    }
    Ok(())
}

async fn perform_restock(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
    restock_index: usize,
) -> AnyResult<()> {
    let restock = plan
        .supply
        .as_ref()
        .and_then(|supply| supply.restocks.get(restock_index))
        .cloned()
        .ok_or_else(|| app_error(io::ErrorKind::InvalidData, "missing planned relay restock"))?;
    if restock.completed {
        return Ok(());
    }
    wait_for_trip_relays(client, config, plan, &restock.relay_stop_indices).await?;
    ensure_carrier_dispatched(client, config, plan, &restock.carrier_code, true).await?;
    travel_to(client, config, &plan.replicant_code, &restock.location).await?;
    wait_device_at_location(client, config, &restock.carrier_code, &restock.location).await?;

    info!(
        restock = restock_index + 1,
        carrier = %restock.carrier_code,
        location = %restock.location,
        quantity = restock.relay_stop_indices.len(),
        "performing rolling relay restock"
    );
    detach_restock_payload(client, config, plan, &restock).await?;
    stow_restock_payload(client, config, plan, &restock).await?;
    if let Some(supply) = plan.supply.as_mut() {
        supply.restocks[restock_index].completed = true;
    }
    // Completing this restock changes the carrier's current duty. Persist that
    // transition before dispatching it onward so a crash cannot leave a stale
    // `dispatched=true` referring to the just-completed rendezvous.
    set_carrier_dispatched(plan, &restock.carrier_code, false);
    save_plan(&config.plan_path, plan)?;

    if let Some((_, next)) = next_restock_for_carrier(plan, &restock.carrier_code) {
        start_device_travel(client, &restock.carrier_code, &next.location).await?;
        checkpoint_carrier_dispatched(config, plan, &restock.carrier_code)?;
        info!(
            carrier = %restock.carrier_code,
            next_restock = %next.location,
            "sent relay supply carrier ahead to its next restock"
        );
    } else {
        start_device_travel(client, &restock.carrier_code, &plan.hub_location).await?;
        checkpoint_carrier_dispatched(config, plan, &restock.carrier_code)?;
        info!(
            carrier = %restock.carrier_code,
            hub = %plan.hub_location,
            "relay supply carrier completed its last restock and started home"
        );
    }
    Ok(())
}

async fn prepare_carrier_supply(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
) -> AnyResult<()> {
    let initial = plan
        .supply
        .as_ref()
        .map(|supply| supply.initial_relay_stop_indices.clone())
        .unwrap_or_default();
    let pending_initial = initial
        .into_iter()
        .filter(|index| !plan.stops[*index].completed)
        .collect::<Vec<_>>();
    wait_for_trip_relays(client, config, plan, &pending_initial).await?;
    transfer_trip_relays(client, config, plan, &pending_initial).await?;
    stow_trip_relays(client, config, plan, &pending_initial).await?;

    // Do not hold the deployment vessel at the hub waiting for the first
    // resupply batch. Any carrier whose full preloaded payload is already
    // printed can depart now; the execution loop keeps checking the others
    // while the vessel works through its initial load. A due restock remains
    // an authoritative barrier if manufacturing ultimately runs behind.
    dispatch_ready_supply_carriers(client, config, plan).await?;
    save_plan(&config.plan_path, plan)?;
    Ok(())
}

async fn finish_supply_carriers(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
) -> AnyResult<()> {
    let carriers = plan
        .supply
        .as_ref()
        .map(|supply| {
            supply
                .carriers
                .iter()
                .map(|carrier| carrier.code.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for code in &carriers {
        start_device_travel(client, code, &plan.hub_location).await?;
    }
    for code in &carriers {
        wait_device_at_location(client, config, code, &plan.hub_location).await?;
        release_carrier_claim(client, plan, code).await?;
        if let Some(supply) = plan.supply.as_mut()
            && let Some(carrier) = supply
                .carriers
                .iter_mut()
                .find(|carrier| carrier.code.as_str() == code.as_str())
        {
            carrier.returned_home = true;
        }
        save_plan(&config.plan_path, plan)?;
    }
    Ok(())
}

fn supply_payload_assignments_incomplete(plan: &MissionPlan) -> bool {
    plan.supply.as_ref().is_some_and(|supply| {
        supply.restocks.iter().any(|restock| {
            !restock.completed
                && restock
                    .relay_stop_indices
                    .iter()
                    .any(|index| plan.stops[*index].relay_code.is_none())
        })
    })
}

async fn execute_carrier_supply_plan(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
) -> AnyResult<()> {
    submit_print_jobs(client, config, plan).await?;
    reconcile_plan(client, plan).await?;
    prepare_carrier_supply(client, config, plan).await?;

    let total_deploys = plan
        .stops
        .iter()
        .filter(|stop| stop.action == StopAction::DeployAndActivate)
        .count();
    while plan.stops.iter().any(|stop| !stop.completed) {
        while let Some(restock_index) = due_restock(plan) {
            perform_restock(client, config, plan, restock_index).await?;
            save_plan(&config.plan_path, plan)?;
        }
        dispatch_ready_supply_carriers(client, config, plan).await?;
        let Some(index) = plan.stops.iter().position(|stop| !stop.completed) else {
            break;
        };
        let deployed_stop = plan.stops[index].action == StopAction::DeployAndActivate;
        execute_stop(client, config, plan, index).await?;
        let completed_deploys = plan
            .stops
            .iter()
            .filter(|stop| stop.action == StopAction::DeployAndActivate && stop.completed)
            .count();
        info!(
            completed_deploys,
            total_deploys,
            stop = %plan.stops[index].system,
            "completed relay stop in continuous carrier-supplied traversal"
        );
        save_plan(&config.plan_path, plan)?;
        while let Some(restock_index) = due_restock(plan) {
            perform_restock(client, config, plan, restock_index).await?;
        }
        if deployed_stop
            && completed_deploys % 3 == 0
            && supply_payload_assignments_incomplete(plan)
        {
            reconcile_plan(client, plan).await?;
        }
        dispatch_ready_supply_carriers(client, config, plan).await?;
        save_plan(&config.plan_path, plan)?;
    }

    travel_to(client, config, &plan.replicant_code, &plan.hub_location).await?;
    finish_supply_carriers(client, config, plan).await?;
    finish_dsr_carrier(client, config, plan).await?;
    plan.returned_to_hub = true;
    save_plan(&config.plan_path, plan)?;
    info!(
        hub = %plan.hub_location,
        carriers = plan.supply.as_ref().map_or(0, |supply| supply.carriers.len()),
        "relay expansion completed; deployment vessel and supply carriers returned"
    );
    Ok(())
}

async fn execute_plan(client: &Client, config: &Config, plan: &mut MissionPlan) -> AnyResult<()> {
    ensure_dsr_carrier_assignment(client, config, plan).await?;
    if plan.supply.is_some() {
        execute_carrier_supply_plan(client, config, plan).await
    } else {
        execute_hub_return_plan(client, config, plan).await
    }
}

async fn execute_hub_return_plan(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
) -> AnyResult<()> {
    // Queue every planned relay as early as the Autofactory queues permit. Once
    // accepted, those jobs continue manufacturing while the transport is away.
    submit_print_jobs(client, config, plan).await?;

    let total_deploys = plan
        .stops
        .iter()
        .filter(|stop| stop.action == StopAction::DeployAndActivate)
        .count();
    let mut trip_number = 0usize;

    while plan.stops.iter().any(|stop| !stop.completed) {
        reconcile_plan(client, plan).await?;
        save_plan(&config.plan_path, plan)?;
        if plan.stops.iter().all(|stop| stop.completed) {
            break;
        }

        // A restart in the middle of a trip intentionally returns to the hub
        // before rebuilding the next load. This is slower than reconstructing a
        // partial route in place, but keeps recovery deterministic and safe.
        travel_to(client, config, &plan.replicant_code, &plan.hub_location).await?;
        let transport_capacity = current_transport_capacity(client, plan).await?;
        let pending_deploys = plan
            .stops
            .iter()
            .filter(|stop| stop_uses_vessel_stow(stop) && !stop.completed)
            .count();
        if pending_deploys > 0 && transport_capacity == 0 {
            return Err(classified_error(
                FailureClass::ConnectivityDependency,
                io::ErrorKind::Other,
                format!(
                    "vessel {} has no usable stow capacity for {pending_deploys} remaining mission relay(s)",
                    plan.vessel_code
                ),
            ));
        }

        let trip_indices = next_trip_stop_indices(&plan.stops, transport_capacity);
        if trip_indices.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                "relay mission has incomplete stops but no executable next trip",
            ));
        }

        wait_for_trip_relays(client, config, plan, &trip_indices).await?;
        transfer_trip_relays(client, config, plan, &trip_indices).await?;
        stow_trip_relays(client, config, plan, &trip_indices).await?;

        trip_number += 1;
        let carried = trip_deploy_count(&plan.stops, &trip_indices);
        let completed_deploys = plan
            .stops
            .iter()
            .filter(|stop| stop.action == StopAction::DeployAndActivate && stop.completed)
            .count();
        info!(
            trip = trip_number,
            carried,
            capacity = transport_capacity,
            completed_deploys,
            total_deploys,
            stops = trip_indices.len(),
            "departing relay deployment trip"
        );
        plan.returned_to_hub = false;
        save_plan(&config.plan_path, plan)?;

        for index in trip_indices {
            if plan.stops[index].completed {
                continue;
            }
            execute_stop(client, config, plan, index).await?;
            save_plan(&config.plan_path, plan)?;
        }

        if plan.stops.iter().any(|stop| !stop.completed) {
            travel_to(client, config, &plan.replicant_code, &plan.hub_location).await?;
            save_plan(&config.plan_path, plan)?;
            info!(
                trip = trip_number,
                hub = %plan.hub_location,
                "relay transport returned for the next load"
            );
        }
    }

    travel_to(client, config, &plan.replicant_code, &plan.hub_location).await?;
    finish_dsr_carrier(client, config, plan).await?;
    plan.returned_to_hub = true;
    save_plan(&config.plan_path, plan)?;
    info!(
        trips = trip_number,
        hub = %plan.hub_location,
        "relay expansion completed and transport returned"
    );
    Ok(())
}

async fn submit_print_jobs(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
) -> AnyResult<()> {
    reconcile_plan(client, plan).await?;
    let blueprints = fetch_print_blueprints(client).await?;
    let initial_factories =
        discover_print_factories(client, &plan.hub_location, &blueprints).await?;
    let mission_id = plan.mission_id.clone();
    let reassigned = reassign_unavailable_print_jobs(
        &mut plan.print_jobs,
        &mission_id,
        &initial_factories,
        &blueprints,
        &plan.hub_location,
    )?;
    if reassigned != 0 {
        info!(reassigned, hub = %plan.hub_location, "reassigned relay print jobs before submission");
    }
    save_plan(&config.plan_path, plan)?;
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    while plan.print_jobs.iter().any(|job| !job.submitted) {
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                "timed out waiting for autofactory queue capacity",
            ));
        }

        if !prepare_relay_print_prerequisites(client, config, plan).await? {
            wait_for_relevant_event(&mut watch, deadline, &["print.completed"]).await?;
            reconcile_plan(client, plan).await?;
            save_plan(&config.plan_path, plan)?;
            continue;
        }

        let states = discover_print_factories(client, &plan.hub_location, &blueprints).await?;
        let mission_id = plan.mission_id.clone();
        let reassigned = reassign_unavailable_print_jobs(
            &mut plan.print_jobs,
            &mission_id,
            &states,
            &blueprints,
            &plan.hub_location,
        )?;
        if reassigned != 0 {
            save_plan(&config.plan_path, plan)?;
        }
        let mut submitted_any = false;
        let mut refresh_factories = false;
        for state in states {
            let mut slots = state.available_slots();
            for job_indices in pending_print_groups(&plan.print_jobs, &state.code) {
                if slots == 0 {
                    break;
                }
                let selected = job_indices.into_iter().take(slots).collect::<Vec<_>>();
                if selected.is_empty() {
                    continue;
                }
                let first_index = *selected
                    .first()
                    .ok_or_else(|| app_error(io::ErrorKind::InvalidData, "empty print batch"))?;
                let mission_tag = plan.print_jobs[first_index].mission_tag.clone();
                let correlation_tag =
                    print_job_correlation_tag(&plan.print_jobs[first_index]).to_owned();
                let device_type = plan.print_jobs[first_index].device_type.clone();
                let flatpack = plan.print_jobs[first_index].flatpack;
                if selected.iter().any(|index| {
                    plan.print_jobs[*index].device_type != device_type
                        || plan.print_jobs[*index].flatpack != flatpack
                }) {
                    return Err(app_error(
                        io::ErrorKind::InvalidData,
                        format!(
                            "relay print batch {correlation_tag} mixes device types or output modes"
                        ),
                    ));
                }
                let quantity = i64::try_from(selected.len())?;
                for index in &selected {
                    plan.print_jobs[*index].submission_started = true;
                }
                save_plan(&config.plan_path, plan)?;

                let tags = [mission_tag, correlation_tag];
                let submission = if flatpack {
                    enqueue_shared_print_flatpacked(
                        client,
                        &state.code,
                        &device_type,
                        quantity,
                        &tags,
                    )
                    .await
                } else {
                    enqueue_shared_print(client, &state.code, &device_type, quantity, &tags).await
                };
                match submission {
                    Ok(operation) => {
                        let operation_id = operation.id().as_str().to_owned();
                        for index in &selected {
                            plan.print_jobs[*index].operation_id = Some(operation_id.clone());
                            plan.print_jobs[*index].submitted = true;
                        }
                    }
                    Err(error) => {
                        let operation_id = match &error {
                            PrintingError::SubmissionRejected { operation_id, .. }
                            | PrintingError::SubmissionUnresolved { operation_id, .. } => {
                                Some(operation_id.clone())
                            }
                            _ => None,
                        };
                        if let Some(operation_id) = operation_id {
                            for index in &selected {
                                plan.print_jobs[*index].operation_id = Some(operation_id.clone());
                            }
                        }
                        if error.is_factory_unavailable_rejection() {
                            for index in &selected {
                                plan.print_jobs[*index].submission_started = false;
                                plan.print_jobs[*index].operation_id = None;
                                plan.print_jobs[*index].submitted = false;
                            }
                            save_plan(&config.plan_path, plan)?;
                            warn!(factory = %state.code, %error, "Autofactory became unavailable; refreshing relay print assignments");
                            refresh_factories = true;
                            break;
                        }
                        save_plan(&config.plan_path, plan)?;
                        return Err(error.into());
                    }
                }
                save_plan(&config.plan_path, plan)?;
                info!(
                    factory = %state.code,
                    quantity,
                    systems = %selected
                        .iter()
                        .map(|index| plan.print_jobs[*index].system.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    device_type = %device_type,
                    flatpack,
                    "queued relay print batch through shared printing crate"
                );
                slots = slots.saturating_sub(selected.len());
                submitted_any = true;
            }
            if refresh_factories {
                break;
            }
        }
        if refresh_factories {
            continue;
        }
        if !submitted_any {
            wait_for_relevant_event(&mut watch, deadline, &["print.completed"]).await?;
            reconcile_plan(client, plan).await?;
            save_plan(&config.plan_path, plan)?;
        }
    }
    Ok(())
}

async fn prepare_relay_print_prerequisites(
    client: &Client,
    config: &Config,
    plan: &MissionPlan,
) -> AnyResult<bool> {
    let mut quantities = BTreeMap::<String, i64>::new();
    for job in plan
        .print_jobs
        .iter()
        .filter(|job| job.relay_code.is_none())
    {
        *quantities.entry(job.device_type.clone()).or_default() += 1;
    }
    if quantities.is_empty() {
        return Ok(true);
    }

    let requests = quantities
        .into_iter()
        .map(|(device_type, quantity)| PrintRequest::new(device_type, quantity))
        .collect::<Vec<_>>();
    let factory_codes = discover_print_factory_codes(client, &plan.hub_location)
        .await?
        .into_iter()
        .filter(|code| !config.ignore_printers.contains(code))
        .collect::<BTreeSet<_>>();
    if factory_codes.is_empty() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!(
                "no eligible Autofactory is available at {}",
                plan.hub_location
            ),
        ));
    }

    let prerequisite_tag = relay_prerequisite_tag(&plan.mission_id);
    let mut options = QueueOptions::at(plan.hub_location.clone());
    options.tags = vec![
        relay_system_mission_tag(&plan.start_system),
        prerequisite_tag,
    ];
    options.poll_interval = POLL_INTERVAL;
    options.wait_timeout = config.wait_timeout;
    options.factory_codes = Some(factory_codes);

    let report = queue_print_prerequisites_ahead(client, &requests, &options).await?;
    if !report.queue.components_queued.is_empty() || !report.queue.components_reused.is_empty() {
        info!(
            mission = %plan.mission_id,
            components_queued = ?report.queue.components_queued,
            components_reused = ?report.queue.components_reused,
            ready_for_parent = report.ready_for_parent,
            "prepared relay manufacturing prerequisites through shared printing pipeline"
        );
    }
    Ok(report.ready_for_parent)
}

async fn transfer_trip_relays(
    client: &Client,
    config: &Config,
    plan: &MissionPlan,
    indices: &[usize],
) -> AnyResult<()> {
    let codes = indices
        .iter()
        .filter(|index| plan.stops[**index].action == StopAction::DeployAndActivate)
        .filter_map(|index| plan.stops[*index].relay_code.clone())
        .collect::<BTreeSet<_>>();
    for code in codes {
        // Trip relays are selected from the managed mission projection.
        let handle = projected_device(client, &code).await?;
        let snapshot = handle.snapshot().await?;
        if assigned_replicant(&snapshot) != Some(plan.replicant_code.as_str()) {
            if !device_has_command(&snapshot, "change_owner") {
                return Err(app_error(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "relay {code} is assigned to {:?} and does not advertise change_owner",
                        assigned_replicant(&snapshot)
                    ),
                ));
            }
            let operation = handle.change_owner(&plan.replicant_code).await?;
            ensure_operation_accepted(&operation).await?;
            wait_for_device(client, config, &code, |device| {
                assigned_replicant(device) == Some(plan.replicant_code.as_str())
            })
            .await?;
        }
    }
    Ok(())
}

async fn prepare_relay_stow(
    client: &Client,
    config: &Config,
    code: &str,
) -> AnyResult<DeviceHandle> {
    let handle = projected_device(client, code).await?;
    let mut snapshot = handle.snapshot().await?;
    if !device_has_command(&snapshot, "stow") {
        ensure_relay_attachable(client, config, code).await?;
        snapshot = handle.refresh().await?.snapshot().await?;
    }
    if !device_has_command(&snapshot, "stow") {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("relay {code} does not currently advertise stow"),
        ));
    }
    Ok(handle)
}

async fn stow_trip_relays(
    client: &Client,
    config: &Config,
    plan: &MissionPlan,
    indices: &[usize],
) -> AnyResult<()> {
    let mut to_stow = Vec::new();
    for index in indices {
        let stop = &plan.stops[*index];
        if stop.completed || !stop_uses_vessel_stow(stop) {
            continue;
        }
        let code = stop.relay_code.as_deref().ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("stop {} has no assigned relay", stop.system),
            )
        })?;
        // Trip relay placement is maintained by managed stow events.
        let snapshot = projected_device(client, code).await?.snapshot().await?;
        if snapshot
            .relationships
            .stowed_in
            .as_ref()
            .is_some_and(|container| container.id.as_str() == plan.vessel_code.as_str())
        {
            continue;
        }
        if let Some(container) = snapshot.relationships.stowed_in.as_ref() {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "relay {code} is stowed in {}, not transport vessel {}",
                    container.id.as_str(),
                    plan.vessel_code
                ),
            ));
        }
        let location = device_location(&snapshot).ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                format!("relay {code} has no current location"),
            )
        })?;
        if designation_in_system(location, &stop.system) {
            continue;
        }
        if location != plan.hub_location {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "relay {code} is at {location}; expected hub {} or target system {}",
                    plan.hub_location, stop.system
                ),
            ));
        }
        to_stow.push(code.to_owned());
    }

    if to_stow.is_empty() {
        return Ok(());
    }
    travel_to(client, config, &plan.replicant_code, &plan.hub_location).await?;
    // Mission-vessel capacity is maintained by managed stow events.
    let vessel = projected_device(client, &plan.vessel_code)
        .await?
        .snapshot()
        .await?;
    let free = vessel
        .stow_capacity
        .unwrap_or(0)
        .saturating_sub(vessel.stow_used.unwrap_or(0));
    if i64::try_from(to_stow.len())? > free {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "vessel {} has {free} free stow slots but the next trip needs {} additional relay(s)",
                plan.vessel_code,
                to_stow.len()
            ),
        ));
    }

    for code in to_stow {
        let handle = prepare_relay_stow(client, config, &code).await?;
        let operation = handle.stow(Some(plan.vessel_code.clone())).await?;
        ensure_operation_accepted(&operation).await?;
        wait_for_device(client, config, &code, |device| {
            device
                .relationships
                .stowed_in
                .as_ref()
                .is_some_and(|container| container.id.as_str() == plan.vessel_code.as_str())
        })
        .await?;
    }
    Ok(())
}

async fn execute_stop(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
    index: usize,
) -> AnyResult<()> {
    let stop = plan.stops[index].clone();
    let relay_code = stop.relay_code.as_deref().ok_or_else(|| {
        app_error(
            io::ErrorKind::NotFound,
            format!("stop {} has no assigned relay", stop.system),
        )
    })?;
    let dsr_carrier = if stop_requires_attachment_carrier(&stop) {
        Some(ensure_dsr_carrier_dispatched(client, config, plan, index).await?)
    } else {
        None
    };

    info!(
        system = %stop.system,
        location = %stop.location,
        relay = %relay_code,
        device_type = %stop.device_type,
        carrier = ?dsr_carrier,
        "starting relay deployment stop"
    );
    travel_to(client, config, &plan.replicant_code, &stop.location).await?;
    info!(
        system = %stop.system,
        location = %stop.location,
        relay = %relay_code,
        "deployment replicant is in position for relay stop"
    );
    if let Some(carrier_code) = dsr_carrier.as_deref() {
        info!(
            relay = %relay_code,
            carrier = %carrier_code,
            location = %stop.location,
            "waiting for DSR attachment carrier at deployment stop"
        );
        detach_dsr_at_stop(client, config, &stop, relay_code, carrier_code).await?;
        info!(
            relay = %relay_code,
            carrier = %carrier_code,
            system = %stop.system,
            "DSR detached from carrier at deployment stop"
        );
    } else {
        // Stop execution uses the live assigned-relay projection.
        let relay = projected_device(client, relay_code).await?;
        let snapshot = relay.snapshot().await?;
        if stop.action == StopAction::DeployAndActivate
            && snapshot.relationships.stowed_in.is_some()
        {
            if !device_has_command(&snapshot, "deploy") {
                return Err(app_error(
                    io::ErrorKind::InvalidData,
                    format!("relay {relay_code} does not currently advertise deploy"),
                ));
            }
            let operation = relay.deploy().await?;
            ensure_operation_accepted(&operation).await?;
            wait_for_device(client, config, relay_code, |device| {
                device.relationships.stowed_in.is_none()
                    && device_location(device)
                        .is_some_and(|location| designation_in_system(location, &stop.system))
            })
            .await?;
        }
    }

    ensure_relay_unfurled(client, config, relay_code).await?;
    // This authoritative read confirms unfurling before activation ordering.
    let snapshot = client.devices().get(relay_code).await?.snapshot().await?;
    if device_status(&snapshot) != Some(RELAYING) {
        info!(
            relay = %relay_code,
            system = %stop.system,
            "activating relay at deployment stop"
        );
        if !device_has_command(&snapshot, "activate") {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("relay {relay_code} does not currently advertise activate"),
            ));
        }
        // The authoritative activation preflight just populated this handle.
        let operation = projected_device(client, relay_code)
            .await?
            .activate()
            .await?;
        ensure_operation_accepted(&operation).await?;
        wait_for_device(client, config, relay_code, |device| {
            device_status(device) == Some(RELAYING)
        })
        .await?;
    }
    info!(
        relay = %relay_code,
        system = %stop.system,
        preferred_parent = %stop.parent_system,
        "waiting for relay topology evidence"
    );
    wait_for_parent_connection(client, config, plan, index, relay_code).await?;
    plan.stops[index].completed = true;
    if let Some(carrier_code) = dsr_carrier.as_deref() {
        send_dsr_carrier_home(client, plan, carrier_code).await?;
    }
    info!(system = %stop.system, relay = relay_code, "relay stop verified");
    Ok(())
}

async fn travel_to(
    client: &Client,
    config: &Config,
    replicant_code: &str,
    destination: &str,
) -> AnyResult<()> {
    let mut handle = client.replicants().get_owned(replicant_code).await?;
    let mut snapshot = handle.snapshot().await?;
    let mut departure_origin = snapshot
        .location
        .as_ref()
        .map(|location| location.id.as_str().to_owned());
    if snapshot.travel.is_none()
        && snapshot
            .location
            .as_ref()
            .is_some_and(|location| relay_destination_matches(location.id.as_str(), destination))
    {
        return Ok(());
    }
    if let Some(travel) = &snapshot.travel {
        let planned_destination = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        if !planned_destination
            .is_some_and(|planned| relay_destination_matches(planned, destination))
        {
            info!(
                replicant = %replicant_code,
                in_flight_destination = ?planned_destination,
                requested_destination = %destination,
                "replicant is already in flight; waiting for that travel to finish before continuing relay route"
            );
        }
    } else {
        info!(
            replicant = %replicant_code,
            destination = %destination,
            "dispatching relay deployment travel"
        );
        let operation = handle.travel().to(destination).depart().await?;
        ensure_operation_accepted(&operation).await?;
    }

    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        snapshot = handle.snapshot().await?;
        let location = snapshot
            .location
            .as_ref()
            .map(|location| location.id.as_str());
        if snapshot.travel.is_none()
            && location.is_some_and(|actual| relay_destination_matches(actual, destination))
        {
            info!(
                replicant = %replicant_code,
                destination = %destination,
                "relay deployment travel arrived"
            );
            return Ok(());
        }

        if snapshot.travel.is_none()
            && let (Some(location), Some(origin)) = (location, departure_origin.as_deref())
            && location != origin
        {
            info!(
                replicant = %replicant_code,
                intermediate = %location,
                destination = %destination,
                "continuing relay deployment travel from intermediate waypoint"
            );
            departure_origin = Some(location.to_owned());
            let operation = handle.travel().to(destination).depart().await?;
            ensure_operation_accepted(&operation).await?;
            continue;
        }

        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out traveling to {destination}"),
            ));
        }

        let eta_seconds = snapshot
            .travel
            .as_ref()
            .and_then(|travel| travel.eta_seconds);
        match wait_for_replicant_travel_event(
            &mut watch,
            deadline,
            replicant_code,
            travel_poll_interval(eta_seconds),
        )
        .await?
        {
            TravelWake::Event => {}
            TravelWake::Poll | TravelWake::Gap => {
                handle = handle.refresh().await?;
                let refreshed = handle.snapshot().await?;
                info!(
                    replicant = %replicant_code,
                    destination = %destination,
                    location = ?refreshed.location.as_ref().map(|location| location.id.as_str()),
                    traveling = refreshed.travel.is_some(),
                    eta_seconds = ?refreshed.travel.as_ref().and_then(|travel| travel.eta_seconds),
                    "authoritatively refreshed relay deployment travel"
                );
            }
        }
    }
}

/// Waits until one device's authoritative state satisfies `predicate`.
///
/// The wait wakes on events for this specific device and otherwise refreshes
/// on the authoritative poll interval. An earlier version woke on *any*
/// account event and issued a remote device read per wake, so a busy account
/// (a mining or survey mission streaming events in parallel) turned every
/// relay wait into a per-event fetch storm.
async fn wait_for_device(
    client: &Client,
    config: &Config,
    code: &str,
    predicate: impl Fn(&Device) -> bool,
) -> AnyResult<()> {
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    let mut authoritative_refresh_due = client.devices().cached(code).is_none();
    loop {
        if let Some(handle) = client.devices().cached(code) {
            match handle.snapshot().await {
                Ok(device) if predicate(&device) => return Ok(()),
                Ok(_) => {}
                Err(error) => {
                    debug!(
                        device = %code,
                        error = %error,
                        "relay wait could not read the managed device projection"
                    );
                    authoritative_refresh_due = true;
                }
            }
        } else {
            authoritative_refresh_due = true;
        }

        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for device {code}"),
            ));
        }

        if authoritative_refresh_due {
            debug!(
                device = %code,
                "relay wait performing bounded authoritative device refresh"
            );
            let device = client.devices().refresh(code).await?.snapshot().await?;
            if predicate(&device) {
                return Ok(());
            }
            authoritative_refresh_due = false;
        }

        match wait_for_device_event(&mut watch, deadline, code).await? {
            DeviceWaitWake::Event => {
                // The managed event reducer is the primary evidence path. The
                // next loop reads the committed projection without another GET.
            }
            DeviceWaitWake::Poll | DeviceWaitWake::Gap => {
                authoritative_refresh_due = true;
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DeviceWaitWake {
    Event,
    Poll,
    Gap,
}

/// Waits for an event naming `code`, bounded by the authoritative poll
/// interval so a missed or filtered event cannot stall the wait.
async fn wait_for_device_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    code: &str,
) -> AnyResult<DeviceWaitWake> {
    let poll_deadline = (Instant::now() + AUTHORITATIVE_POLL_INTERVAL).min(deadline);
    loop {
        let remaining = poll_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(DeviceWaitWake::Poll);
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event))
                if event
                    .device
                    .as_ref()
                    .is_some_and(|device| device.id.as_str() == code) =>
            {
                return Ok(DeviceWaitWake::Event);
            }
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(DeviceWaitWake::Poll),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; falling back to authoritative refresh");
                sleep(Duration::from_millis(250)).await;
                return Ok(DeviceWaitWake::Gap);
            }
        }
    }
}

fn expected_upstream_systems(plan: &MissionPlan, index: usize) -> BTreeSet<String> {
    let mut upstream = plan
        .network
        .active_relay_systems
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    upstream.insert(plan.start_system.clone());
    upstream.insert(plan.stops[index].parent_system.clone());
    upstream.extend(
        plan.stops
            .iter()
            .enumerate()
            .filter(|(stop_index, stop)| *stop_index != index && stop.completed)
            .map(|(_, stop)| stop.system.clone()),
    );
    upstream
}

async fn wait_for_parent_connection(
    client: &Client,
    config: &Config,
    plan: &MissionPlan,
    index: usize,
    relay_code: &str,
) -> AnyResult<()> {
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    let upstream = expected_upstream_systems(plan, index);
    let expected_parent = plan.stops[index].parent_system.as_str();
    // Each pass costs a remote `/network` read, so wake on events naming this
    // relay rather than on any account event. The optimized relay tree names a
    // preferred parent, but the live network can legitimately connect through
    // another already-reachable relay-capable system. Either proves the new
    // relay has joined the expanding mesh.
    loop {
        // Each pass performs one authoritative `/network` read; the handle is cached.
        let network = projected_device(client, relay_code)
            .await?
            .network()
            .await?;
        if let Some(connected_via) = network.connections.iter().find_map(|connection| {
            connection
                .star
                .as_deref()
                .filter(|system| upstream.contains(*system))
        }) {
            if connected_via != expected_parent {
                info!(
                    relay = %relay_code,
                    expected_parent = %expected_parent,
                    connected_via = %connected_via,
                    "relay joined the reachable mesh through an alternate upstream system"
                );
            }
            return Ok(());
        }
        if Instant::now() >= deadline {
            let observed = network
                .connections
                .iter()
                .filter_map(|connection| connection.star.as_deref())
                .collect::<Vec<_>>();
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "relay {relay_code} never joined the reachable mesh (preferred parent {expected_parent}; observed connections: {})",
                    if observed.is_empty() {
                        "none".to_owned()
                    } else {
                        observed.join(", ")
                    }
                ),
            ));
        }
        let _ = wait_for_device_event(&mut watch, deadline, relay_code).await?;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TravelWake {
    Event,
    Poll,
    Gap,
}

async fn wait_for_replicant_travel_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    replicant_code: &str,
    poll_interval: Duration,
) -> AnyResult<TravelWake> {
    let poll_deadline = Instant::now() + poll_interval;
    loop {
        let now = Instant::now();
        let remaining = deadline
            .saturating_duration_since(now)
            .min(poll_deadline.saturating_duration_since(now));
        if remaining.is_zero() {
            return Ok(TravelWake::Poll);
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event))
                if event.name.as_str() == "travel.arrived"
                    && event
                        .replicant
                        .as_ref()
                        .is_some_and(|replicant| replicant.id.as_str() == replicant_code) =>
            {
                return Ok(TravelWake::Event);
            }
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(TravelWake::Poll),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; refreshing relay deployment travel");
                return Ok(TravelWake::Gap);
            }
        }
    }
}

fn travel_poll_interval(eta_seconds: Option<i64>) -> Duration {
    match eta_seconds.unwrap_or(0) {
        eta if eta >= 300 => Duration::from_secs(60),
        eta if eta >= 60 => Duration::from_secs(30),
        eta if eta > 0 => Duration::from_secs(10),
        _ => POLL_INTERVAL,
    }
}

async fn wait_for_relevant_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    names: &[&str],
) -> AnyResult<()> {
    let poll_deadline = Instant::now() + POLL_INTERVAL;
    loop {
        let now = Instant::now();
        let remaining = deadline
            .saturating_duration_since(now)
            .min(poll_deadline.saturating_duration_since(now));
        if remaining.is_zero() {
            return Ok(());
        }
        match timeout(remaining, watch.next()).await {
            Ok(Ok(event)) if names.is_empty() || names.contains(&event.name.as_str()) => {
                return Ok(());
            }
            Ok(Ok(_)) => continue,
            Err(_) => return Ok(()),
            Ok(Err(error)) => {
                warn!(error = %error, "event watcher gap; falling back to authoritative refresh");
                sleep(Duration::from_millis(250)).await;
                return Ok(());
            }
        }
    }
}

async fn ensure_operation_accepted(operation: &Operation) -> AnyResult<()> {
    // Operation creation has already completed the HTTP submission and persisted
    // the resulting durable state. Do not wait for asynchronous evidence here:
    // most successful device mutations remain AwaitingEvidence/ReconciliationRequired
    // until the event engine or an authoritative state check reconciles them. Each
    // relay workflow call site performs its own state-specific verification when
    // ordering actually depends on the mutation being visible.
    let outcome = operation.outcome().await?;
    if matches!(
        outcome.status,
        OperationStatus::Cancelled | OperationStatus::Rejected | OperationStatus::Failed
    ) {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "operation {} ended as {:?}: {:?}",
                operation.id().as_str(),
                outcome.status,
                outcome.response
            ),
        ));
    }
    Ok(())
}

fn validate_loaded_plan(
    plan: &MissionPlan,
    config: &Config,
    replicant_code: &str,
    vessel_code: &str,
) -> AnyResult<()> {
    if plan.version != PLAN_VERSION {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "plan version {} is unsupported; create a replacement with `plan --replace-plan`",
                plan.version
            ),
        ));
    }
    if plan.replicant_code != replicant_code || plan.vessel_code != vessel_code {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "saved plan does not match the selected replicant or its current vessel",
        ));
    }
    if config.command == Command::Plan && plan.hub_location != config.hub {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "saved plan hub differs from --hub; use plan --replace-plan",
        ));
    }
    if config.command == Command::Plan
        && !config.targets.is_empty()
        && plan.targets != config.targets
    {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "saved plan targets differ from the command line; use plan --replace-plan",
        ));
    }
    if config.command == Command::Plan && (plan.max_hop_ly - config.max_hop_ly).abs() > f64::EPSILON
    {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "saved plan maximum hop differs from --max-hop; use plan --replace-plan",
        ));
    }
    Ok(())
}

fn load_plan(path: &Path) -> AnyResult<MissionPlan> {
    let mut plan: MissionPlan = serde_json::from_reader(File::open(path)?)?;
    normalize_relay_print_jobs(&mut plan.print_jobs);
    migrate_relay_mission_tag_metadata(&mut plan);
    Ok(plan)
}

fn save_plan(path: &Path, plan: &MissionPlan) -> AnyResult<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    {
        let file = File::create(&temporary)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, plan)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        writer.get_ref().sync_all()?;
    }
    fs::rename(temporary, path)?;
    let _ = WORKFLOW_CHECKPOINTS.try_with(|sender| sender.send(Box::new(plan.clone())));
    Ok(())
}

fn print_plan(plan: &MissionPlan) {
    println!("FTL relay expansion plan");
    println!("  Mission: {}", plan.mission_id);
    println!("  Start/hub: {}", plan.hub_location);
    println!("  Replicant: {}", plan.replicant_code);
    println!("  Vessel: {}", plan.vessel_code);
    if let Some(carrier) = plan.dsr_carrier_code.as_deref() {
        println!("  DSR attachment carrier: {carrier}");
    }
    println!("  Targets: {}", plan.targets.join(", "));
    println!("  Conventional/new-relay hop: {:.3} ly", plan.max_hop_ly);
    println!(
        "  Network sites after start: {}",
        plan.network.nodes.len().saturating_sub(1)
    );
    let deployment_count = plan
        .stops
        .iter()
        .filter(|stop| stop.action == StopAction::DeployAndActivate)
        .count();
    println!("  New placements: {deployment_count}");
    println!(
        "  Existing inactive activation stops: {}",
        plan.network.activation_systems.len()
    );
    println!(
        "  Reusable inactive relays at hub: {}",
        plan.hub_stock_relays.len()
    );
    println!("  Relays to print: {}", plan.print_jobs.len());
    if plan.planned_transport_capacity > 0 {
        let required = i64::try_from(deployment_count).unwrap_or(i64::MAX);
        let trips = if required == 0 {
            0
        } else {
            (required + plan.planned_transport_capacity - 1) / plan.planned_transport_capacity
        };
        if let Some(supply) = &plan.supply {
            println!(
                "  Planned vessel capacity: {} mission relay(s); one continuous deployment run",
                plan.planned_transport_capacity
            );
            println!(
                "  Supply strategy: {:?}; {} carrier(s), {} rolling restock(s)",
                supply.strategy,
                supply.carriers.len(),
                supply.restocks.len()
            );
        } else {
            println!(
                "  Planned vessel capacity: {} mission relay(s); deployment trips: {trips}",
                plan.planned_transport_capacity
            );
        }
    }
    println!(
        "  Total tree distance: {:.4} ly",
        plan.network.total_edge_distance_ly
    );
    let extended_links = plan
        .network
        .edges
        .iter()
        .filter(|edge| edge.distance_ly > plan.max_hop_ly + RELAY_DISTANCE_EPSILON)
        .collect::<Vec<_>>();
    if !extended_links.is_empty() {
        println!(
            "  Extended-range links: {} (existing infrastructure and/or planned DSR bridges)",
            extended_links.len()
        );
        for edge in extended_links {
            println!(
                "    {} -> {}: {:.3} ly",
                edge.parent, edge.child, edge.distance_ly
            );
        }
    }
    if !plan.network.relay_tree_optimal {
        println!(
            "  Note: the exact minimum-relay search was too large for this target set, so this \
             is a feasible plan that may use more new relays than strictly necessary. Planning \
             fewer targets at once will restore the exact solver."
        );
    }
    println!(
        "  Single-pass planner traversal: {} hops, {:.4} routed ly including one return ({})",
        plan.network.execution_hops,
        plan.network.execution_distance_ly,
        if plan.network.execution_order_optimal {
            "proven optimal"
        } else {
            "precedence-safe heuristic"
        }
    );
    if plan.supply.is_none()
        && plan.planned_transport_capacity > 0
        && i64::try_from(deployment_count).unwrap_or(i64::MAX) > plan.planned_transport_capacity
    {
        println!("  Multi-trip execution adds a hub return between vessel loads.");
    }
    println!();
    println!("Execution order:");
    for (index, stop) in plan.stops.iter().enumerate() {
        let restock = plan.supply.as_ref().and_then(|supply| {
            supply
                .restocks
                .iter()
                .enumerate()
                .find(|(_, restock)| restock.boundary_stop_index == index)
        });
        let restock_note = restock.map_or_else(String::new, |(restock_index, restock)| {
            format!(
                " [RESTOCK {}: +{} via {}]",
                restock_index + 1,
                restock.relay_stop_indices.len(),
                restock.carrier_code
            )
        });
        println!(
            "  {:>2}. {:<18} {:<24} {:?} type={} relay={} parent={}{}{}",
            index + 1,
            stop.system,
            stop.location,
            stop.action,
            stop.device_type,
            stop.relay_code.as_deref().unwrap_or("pending print"),
            stop.parent_system,
            if stop.completed { " [complete]" } else { "" },
            restock_note
        );
    }
    if let Some(supply) = &plan.supply {
        println!();
        println!("Relay logistics:");
        println!(
            "  Initial load: {} relay(s) at {}",
            supply.initial_relay_stop_indices.len(),
            plan.hub_location
        );
        for (index, restock) in supply.restocks.iter().enumerate() {
            let systems = restock
                .relay_stop_indices
                .iter()
                .map(|stop_index| plan.stops[*stop_index].system.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            println!(
                "  RESTOCK {:>2}: {:<24} +{} via {} -> {}{}",
                index + 1,
                restock.location,
                restock.relay_stop_indices.len(),
                restock.carrier_code,
                systems,
                if restock.completed { " [complete]" } else { "" }
            );
        }
        println!("  Supply carriers:");
        for carrier in &supply.carriers {
            let assignments = carrier
                .restock_indices
                .iter()
                .map(|index| (index + 1).to_string())
                .collect::<Vec<_>>()
                .join(",");
            println!(
                "    {} ({}, capacity={}): restock(s) {}{}",
                carrier.code,
                carrier.device_type,
                carrier.attach_capacity,
                assignments,
                if carrier.returned_home { " [home]" } else { "" }
            );
        }
    }
    if !plan.print_jobs.is_empty() {
        println!();
        println!("Manufacturing batches:");
        let mut batches = BTreeMap::<(String, String), Vec<&PrintJob>>::new();
        for job in &plan.print_jobs {
            batches
                .entry((
                    job.factory_code.clone(),
                    print_job_correlation_tag(job).to_owned(),
                ))
                .or_default()
                .push(job);
        }
        for ((factory_code, _), jobs) in batches {
            let systems = jobs
                .iter()
                .map(|job| job.system.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let printed = jobs.iter().filter(|job| job.relay_code.is_some()).count();
            let submitted = jobs.iter().all(|job| job.submitted);
            let flatpack = jobs.first().is_some_and(|job| job.flatpack);
            println!(
                "  {factory_code}: quantity={} -> {systems} ({printed} printed){}{}",
                jobs.len(),
                if flatpack { " [flatpack]" } else { "" },
                if submitted { " [submitted]" } else { "" }
            );
        }
    }
    println!();
    println!("Return destination: {}", plan.hub_location);
}

fn device_has_command(device: &Device, command: &str) -> bool {
    device
        .available_commands
        .iter()
        .any(|available| available.as_str() == command)
}

fn device_has_feature(device: &Device, feature: &str) -> bool {
    device
        .features
        .iter()
        .any(|available| available.as_str() == feature)
}

fn relay_device_active(device: &Device) -> bool {
    // `deactivate` is advertised by inactive, compacted, travelling, and even
    // stowed relay-capable devices, so command availability is not evidence of
    // live network participation. Only an explicitly active status may
    // contribute coverage.
    device_status(device).is_some_and(|status| status == RELAYING || status == "active")
}

fn relay_device_needs_unfurl(device: &Device) -> bool {
    device_type(device) == Some(DEEP_SPACE_RELAY)
        && device_status(device)
            .is_some_and(|status| matches!(status, "compacting" | "compacted" | "unfurling"))
}

fn relay_device_recoverable(device: &Device) -> bool {
    if device_type(device) != Some(DEEP_SPACE_RELAY) || relay_device_active(device) {
        return false;
    }
    match device_status(device) {
        Some("compacted") => device_has_command(device, "unfurl"),
        Some("compacting") | Some("unfurling") => true,
        _ => device_has_command(device, "activate"),
    }
}

fn documented_relay_range_ly(device_type: &str) -> Option<f64> {
    // Replicant Space 2.5.1 reference: `ftl-relays/index.md`,
    // `system-hubs/index.md`, and the DSR note in `changelog/index.md`.
    match device_type {
        FTL_RELAY => Some(DEFAULT_MAX_HOP_LY),
        SYSTEM_HUB => Some(REGION_GATEWAY_HUB_RANGE_LY),
        DEEP_SPACE_RELAY => Some(DEEP_SPACE_RELAY_RANGE_LY),
        _ => None,
    }
}

fn device_type(device: &Device) -> Option<&str> {
    device.device_type.as_ref().map(DeviceType::as_str)
}

fn device_status(device: &Device) -> Option<&str> {
    device.status.as_ref().map(|status| status.as_str())
}

fn device_location(device: &Device) -> Option<&str> {
    device
        .location
        .as_ref()
        .map(|location| location.id.as_str())
}

fn assigned_replicant(device: &Device) -> Option<&str> {
    device
        .relationships
        .assigned_replicant
        .as_ref()
        .map(|replicant| replicant.id.as_str())
}

fn resolve_system(location: &str, systems: &BTreeSet<String>) -> Option<String> {
    if systems.contains(location) {
        return Some(location.to_owned());
    }
    systems
        .iter()
        .filter(|system| location.starts_with(&format!("{system}-")))
        .max_by_key(|system| system.len())
        .cloned()
}

fn designation_in_system(location: &str, system: &str) -> bool {
    location == system || location.starts_with(&format!("{system}-"))
}

/// Returns the requested systems currently connected to `start` through the
/// active account-owned relay mesh. This is a projection-only preflight: it
/// reads the managed device state and local star catalogue without forcing an
/// account-wide refresh. Inactive relays intentionally do not count because a
/// relay expansion may need to activate them before travel is actually usable.
pub async fn ftl_network_reachable_systems(
    client: &Client,
    start: &str,
    targets: &BTreeSet<String>,
    conventional_range_ly: f64,
) -> AnyResult<BTreeSet<String>> {
    let catalogue = client.galaxy().catalogue();
    let positions = catalogue
        .iter()
        .filter_map(|star| {
            star.position.map(|position| {
                (
                    star.key.id.as_str().to_owned(),
                    Position {
                        x: position.x,
                        y: position.y,
                        z: position.z,
                    },
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    if !positions.contains_key(start) {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!("FTL connectivity origin {start} is not present in the star catalogue"),
        ));
    }
    if let Some(target) = targets
        .iter()
        .find(|target| !positions.contains_key(*target))
    {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!("FTL connectivity target {target} is not present in the star catalogue"),
        ));
    }
    let systems = positions.keys().cloned().collect::<BTreeSet<_>>();
    let handles = client.devices().find().owned().collect().await?;
    let mut relay_ranges = BTreeMap::<String, f64>::new();
    for handle in handles {
        let device = handle.snapshot().await?;
        let kind = device_type(&device);
        let relay_capable = device_has_feature(&device, "relay")
            || kind == Some(FTL_RELAY)
            || kind == Some(SYSTEM_HUB)
            || kind == Some(DEEP_SPACE_RELAY);
        if !relay_capable
            || !relay_device_active(&device)
            || device.relationships.stowed_in.is_some()
            || device.relationships.attached_to.is_some()
        {
            continue;
        }
        let Some(location) = device_location(&device) else {
            continue;
        };
        let Some(system) = resolve_system(location, &systems) else {
            continue;
        };
        let range = kind
            .and_then(documented_relay_range_ly)
            .unwrap_or(conventional_range_ly)
            .max(conventional_range_ly);
        relay_ranges
            .entry(system)
            .and_modify(|current| *current = current.max(range))
            .or_insert(range);
    }

    let mut reachable =
        relay_mesh_reachable_systems(&positions, &relay_ranges, start, conventional_range_ly);
    // Same-system work never needs FTL even if the home system lacks a relay.
    reachable.insert(start.to_owned());
    reachable.retain(|system| targets.contains(system));
    Ok(reachable)
}

/// Convenience predicate for one target system.
pub async fn ftl_network_reaches_system(
    client: &Client,
    start: &str,
    target: &str,
    conventional_range_ly: f64,
) -> AnyResult<bool> {
    let targets = BTreeSet::from([target.to_owned()]);
    Ok(
        ftl_network_reachable_systems(client, start, &targets, conventional_range_ly)
            .await?
            .contains(target),
    )
}

fn relay_mesh_reachable_systems(
    positions: &BTreeMap<String, Position>,
    relay_ranges: &BTreeMap<String, f64>,
    start: &str,
    conventional_range_ly: f64,
) -> BTreeSet<String> {
    if !relay_ranges.contains_key(start) {
        return BTreeSet::new();
    }
    let mut reachable = BTreeSet::from([start.to_owned()]);
    let mut pending = std::collections::VecDeque::from([start.to_owned()]);
    while let Some(current) = pending.pop_front() {
        let Some(current_position) = positions.get(&current) else {
            continue;
        };
        let current_range = relay_ranges
            .get(&current)
            .copied()
            .unwrap_or(conventional_range_ly);
        for (candidate, candidate_range) in relay_ranges {
            if reachable.contains(candidate) {
                continue;
            }
            let Some(candidate_position) = positions.get(candidate) else {
                continue;
            };
            let available_range = current_range
                .max(*candidate_range)
                .max(conventional_range_ly);
            if current_position.distance(*candidate_position)
                <= available_range + RELAY_DISTANCE_EPSILON
            {
                reachable.insert(candidate.clone());
                pending.push_back(candidate.clone());
            }
        }
    }
    reachable
}

#[cfg(test)]
fn relay_mesh_reaches(
    positions: &BTreeMap<String, Position>,
    relay_ranges: &BTreeMap<String, f64>,
    start: &str,
    target: &str,
    conventional_range_ly: f64,
) -> bool {
    start == target
        || relay_mesh_reachable_systems(positions, relay_ranges, start, conventional_range_ly)
            .contains(target)
}

/// Inputs for invoking the durable relay-expansion workflow from another automation.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RelayExpansionRequest {
    /// Replicant name or code that carries and deploys the relays.
    pub replicant: String,
    /// Manufacturing location in the autonomous regional island.
    pub hub: String,
    /// Systems that must be connected to the island's relay network.
    pub targets: Vec<String>,
    /// Child mission file used for restart-safe reconciliation.
    pub mission_file: PathBuf,
    /// Maximum conventional relay hop in light years.
    pub max_hop_ly: f64,
    /// Maximum wait for printing, travel, or activation evidence.
    pub wait_timeout: Duration,
    /// Autofactories reserved by other workflows and unavailable for new print work.
    #[serde(default)]
    pub unavailable_autofactories: BTreeSet<String>,
}

/// Parses the existing relay CLI contract into a daemon workflow request.
pub fn relay_workflow_request(arguments: Vec<String>) -> AnyResult<RelayExpansionRequest> {
    let config = Config::from_args_and_env(arguments)?;
    if config.command != Command::Run {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "only relay run is a durable daemon workflow",
        ));
    }
    Ok(RelayExpansionRequest {
        replicant: config.replicant,
        hub: config.hub,
        targets: config.targets,
        mission_file: config.plan_path,
        max_hop_ly: config.max_hop_ly,
        wait_timeout: config.wait_timeout,
        unavailable_autofactories: BTreeSet::new(),
    })
}

/// Summary returned after a reusable relay expansion completes.
#[derive(Clone, Debug, Serialize)]
pub struct RelayExpansionReport {
    /// Requested target systems.
    pub targets: Vec<String>,
    /// Number of deployment or activation stops in the persisted plan.
    pub stops: usize,
    /// Final persisted checkpoint, ready for a workflow persistence layer.
    pub state: RelayExecutionState,
}

/// Loads a persisted relay checkpoint without starting a managed client.
pub fn relay_state(path: impl AsRef<Path>) -> AnyResult<RelayExecutionState> {
    load_plan(path.as_ref())
}

/// Returns frontend-neutral progress from a persisted relay checkpoint.
pub fn relay_status(path: impl AsRef<Path>) -> AnyResult<RelayExecutionStatus> {
    Ok(relay_state(path)?.status())
}

/// Creates or resumes a relay expansion using an already-running managed client.
pub async fn execute_expansion(
    client: &Client,
    request: &RelayExpansionRequest,
) -> AnyResult<RelayExpansionReport> {
    if request.targets.is_empty() && !request.mission_file.exists() {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "a new relay child mission requires at least one target",
        ));
    }
    let config = Config {
        command: Command::Run,
        database: PathBuf::new(),
        replicant: request.replicant.clone(),
        hub: request.hub.to_ascii_uppercase(),
        plan_path: request.mission_file.clone(),
        max_hop_ly: request.max_hop_ly,
        replace_plan: false,
        reuse_account_relays: false,
        ignore_printers: request.unavailable_autofactories.clone(),
        supply_strategy: RequestedSupplyStrategy::Auto,
        wait_timeout: request.wait_timeout,
        targets: request
            .targets
            .iter()
            .map(|target| target.to_ascii_uppercase())
            .collect(),
        verbose: false,
        log_file: None,
    };
    let _lock = MissionLock::acquire(&request.mission_file)?;
    let plan = run(client, &config).await?;
    Ok(RelayExpansionReport {
        targets: plan.targets.clone(),
        stops: plan.stops.len(),
        state: plan,
    })
}

/// Executes relay expansion while reporting every durable phase checkpoint.
pub async fn execute_relay_workflow<F>(
    client: &Client,
    request: &RelayExpansionRequest,
    mut checkpoint: F,
) -> AnyResult<RelayExpansionReport>
where
    F: FnMut(RelayExecutionState) -> AnyResult<()>,
{
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let execution = WORKFLOW_CHECKPOINTS.scope(sender, execute_expansion(client, request));
    tokio::pin!(execution);
    loop {
        tokio::select! {
            result = &mut execution => {
                while let Ok(state) = receiver.try_recv() {
                    checkpoint(*state)?;
                }
                return result;
            }
            Some(state) = receiver.recv() => checkpoint(*state)?,
        }
    }
}

/// Restores the workflow-authoritative checkpoint into the legacy mission file.
pub fn restore_relay_checkpoint(path: &Path, checkpoint: &RelayExecutionState) -> AnyResult<()> {
    save_plan(path, checkpoint)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use replicant_client::{SecretString, StartupPolicy, raw::Url};
    use wiremock::{
        Mock, MockServer, Request, ResponseTemplate,
        matchers::{method, path, path_regex},
    };

    use super::*;

    fn test_relay_device(device_type_name: &str, status: &str, commands: &[&str]) -> Device {
        Device {
            key: replicant_client::domain::DeviceKey::live(
                replicant_client::domain::DeviceId::from("RELAY-TEST"),
            ),
            device_type: Some(DeviceType::from(device_type_name)),
            status: Some(replicant_client::domain::DeviceStatus::from(status)),
            location: Some(replicant_client::domain::LocationKey::live(
                "ANTAR-1-L4".into(),
            )),
            features: Vec::new(),
            available_commands: commands
                .iter()
                .map(|command| replicant_client::domain::DeviceCommand::from(*command))
                .collect(),
            available_directives: Vec::new(),
            tags: Vec::new(),
            relationships: replicant_client::domain::DeviceRelationships::default(),
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
            access: replicant_client::domain::AccessScope::Owned,
        }
    }

    async fn test_client_at(server: &MockServer) -> Client {
        Client::builder()
            .base_url(Url::parse(&server.uri()).expect("mock URL"))
            .authentication_token(SecretString::from("test-token"))
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .expect("start test client")
    }

    fn test_config() -> Config {
        Config {
            command: Command::Run,
            database: PathBuf::new(),
            replicant: "REP-1".to_owned(),
            hub: "ROOT-1-L4".to_owned(),
            plan_path: PathBuf::new(),
            max_hop_ly: DEFAULT_MAX_HOP_LY,
            replace_plan: false,
            reuse_account_relays: false,
            ignore_printers: BTreeSet::new(),
            supply_strategy: RequestedSupplyStrategy::Auto,
            wait_timeout: Duration::from_secs(1),
            targets: vec!["TARGET".to_owned()],
            verbose: false,
            log_file: None,
        }
    }

    async fn seed_relay_device(
        server: &MockServer,
        client: &Client,
        code: &str,
        device_type: &str,
        features: &[&str],
    ) {
        Mock::given(method("GET"))
            .and(path(format!("/v1/devices/{code}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": code,
                "device_type": device_type,
                "features": features,
                "location": "ANTAR-1-L4",
                "status": "active"
            })))
            .expect(1)
            .mount(server)
            .await;
        client.devices().get(code).await.expect("seed device");
    }

    #[tokio::test]
    async fn projected_device_reuses_cached_handle_without_another_request() {
        let server = MockServer::start().await;
        let client = test_client_at(&server).await;
        seed_relay_device(&server, &client, "RELAY-1", FTL_RELAY, &["relay"]).await;

        projected_device(&client, "RELAY-1")
            .await
            .expect("cached relay handle");

        let requests = server.received_requests().await.expect("recorded requests");
        assert_eq!(
            requests.len(),
            1,
            "cached access must not issue a second GET"
        );
        server.verify().await;
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn transport_capacity_bulk_refreshes_missing_relay_projections_once() {
        let server = MockServer::start().await;
        let client = test_client_at(&server).await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/VESSEL-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "VESSEL-1",
                "device_type": "ftl_transport",
                "location": "ROOT-1-L4",
                "status": "active",
                "stow_capacity": 2,
                "stow_used": 0
            })))
            .expect(1)
            .mount(&server)
            .await;
        client.devices().get("VESSEL-1").await.expect("seed vessel");

        let mission_tag = relay_system_mission_tag("ROOT");
        Mock::given(method("GET"))
            .and(path("/v1/devices"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "devices": [
                    {"device_code": "RELAY-1", "device_type": FTL_RELAY, "tags": [mission_tag], "stowed_in_device_code": "VESSEL-1"},
                    {"device_code": "RELAY-2", "device_type": FTL_RELAY, "tags": [mission_tag]},
                    {"device_code": "RELAY-3", "device_type": FTL_RELAY, "tags": [mission_tag]}
                ],
                "next_cursor": null
            })))
            .expect(1)
            .mount(&server)
            .await;

        let mut plan = execution_state();
        plan.stops[0].relay_code = Some("RELAY-1".to_owned());
        for (system, code) in [("TARGET-2", "RELAY-2"), ("TARGET-3", "RELAY-3")] {
            let mut stop = plan.stops[0].clone();
            stop.system = system.to_owned();
            stop.relay_code = Some(code.to_owned());
            plan.stops.push(stop);
        }

        assert_eq!(
            current_transport_capacity(&client, &plan)
                .await
                .expect("transport capacity"),
            3
        );
        let requests = server.received_requests().await.expect("recorded requests");
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path() == "/v1/devices")
                .count(),
            1
        );
        assert!(
            requests
                .iter()
                .all(|request| { !request.url.path().starts_with("/v1/devices/RELAY-") })
        );
        server.verify().await;
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn parent_connection_pass_only_reads_network_for_cached_relay() {
        let server = MockServer::start().await;
        let client = test_client_at(&server).await;
        seed_relay_device(&server, &client, "RELAY-1", FTL_RELAY, &["relay"]).await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/RELAY-1/network"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "connections": [{"star": "ROOT"}]
            })))
            .expect(1)
            .mount(&server)
            .await;

        wait_for_parent_connection(&client, &test_config(), &execution_state(), 0, "RELAY-1")
            .await
            .expect("connected relay");

        let requests = server.received_requests().await.expect("recorded requests");
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.url.path() == "/v1/devices/RELAY-1")
                .count(),
            1,
            "the wait must reuse the seeded handle"
        );
        server.verify().await;
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn advertised_stow_command_avoids_redundant_device_refresh() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/devices/RELAY-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "device_code": "RELAY-1",
                "device_type": FTL_RELAY,
                "location": "ROOT-1-L4",
                "status": "active",
                "available_commands": ["stow"]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        client.devices().get("RELAY-1").await.expect("seed relay");

        prepare_relay_stow(&client, &test_config(), "RELAY-1")
            .await
            .expect("stow-ready relay");

        server.verify().await;
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn census_uses_documented_hub_and_dsr_ranges_without_network_requests() {
        let server = MockServer::start().await;
        let network_calls = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .and(path_regex(r"^/v1/devices/[^/]+/network$"))
            .respond_with({
                let network_calls = Arc::clone(&network_calls);
                move |_: &Request| {
                    network_calls.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({"range_ly": 99.0}))
                }
            })
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        seed_relay_device(&server, &client, "HUB-1", SYSTEM_HUB, &[]).await;
        seed_relay_device(&server, &client, "DSR-1", DEEP_SPACE_RELAY, &[]).await;

        let census = refresh_device_census(
            &client,
            "ANTAR-1-L4",
            "VESSEL-1",
            &BTreeSet::from(["ANTAR".to_owned()]),
            &BTreeMap::new(),
        )
        .await
        .expect("refresh census");

        assert_eq!(network_calls.load(Ordering::SeqCst), 0);
        assert_eq!(census.relay_ranges_ly["ANTAR"], REGION_GATEWAY_HUB_RANGE_LY);
        server.verify().await;
        client.close().await.expect("close client");
    }

    #[tokio::test]
    async fn census_reads_unknown_relay_range_once_per_device_type() {
        let server = MockServer::start().await;
        let network_calls = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .and(path_regex(r"^/v1/devices/UNKNOWN-[12]/network$"))
            .respond_with({
                let network_calls = Arc::clone(&network_calls);
                move |_: &Request| {
                    network_calls.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({"range_ly": 12.5}))
                }
            })
            .mount(&server)
            .await;
        let client = test_client_at(&server).await;
        seed_relay_device(&server, &client, "UNKNOWN-1", "future_relay", &["relay"]).await;
        seed_relay_device(&server, &client, "UNKNOWN-2", "future_relay", &["relay"]).await;

        let census = refresh_device_census(
            &client,
            "ANTAR-1-L4",
            "VESSEL-1",
            &BTreeSet::from(["ANTAR".to_owned()]),
            &BTreeMap::new(),
        )
        .await
        .expect("refresh census");

        assert_eq!(network_calls.load(Ordering::SeqCst), 1);
        assert_eq!(census.relay_ranges_ly["ANTAR"], 12.5);
        server.verify().await;
        client.close().await.expect("close client");
    }

    #[test]
    fn system_level_relay_destination_accepts_server_selected_arrival_zone() {
        assert!(relay_destination_matches("LYRDANIA-OORT", "LYRDANIA"));
        assert!(relay_destination_matches("LYRDANIA-KUIPER", "LYRDANIA"));
        assert!(relay_destination_matches("LYRDANIA-2-L4", "LYRDANIA"));
        assert!(relay_destination_matches("LYRDANIA-2-L4", "LYRDANIA-2-L4"));
        assert!(!relay_destination_matches("OTHER-2-L4", "LYRDANIA"));
        assert!(!relay_destination_matches("LYRDANIA-2-L5", "LYRDANIA-2-L4"));
    }

    #[test]
    fn compacted_dsr_is_recoverable_when_it_can_unfurl() {
        let device = test_relay_device(DEEP_SPACE_RELAY, "compacted", &["unfurl"]);

        assert!(relay_device_needs_unfurl(&device));
        assert!(relay_device_recoverable(&device));
    }

    #[test]
    fn compacted_dsr_without_unfurl_is_not_planned_as_recoverable() {
        let device = test_relay_device(DEEP_SPACE_RELAY, "compacted", &[]);

        assert!(relay_device_needs_unfurl(&device));
        assert!(!relay_device_recoverable(&device));
    }

    #[test]
    fn inactive_deployed_dsr_is_reusable_without_compaction_repair() {
        let device = test_relay_device(DEEP_SPACE_RELAY, "inactive", &["activate"]);

        assert!(!relay_device_needs_unfurl(&device));
        assert!(relay_device_recoverable(&device));
    }

    #[test]
    fn compacted_ordinary_relay_is_not_mistaken_for_recoverable_dsr() {
        let device = test_relay_device(FTL_RELAY, "compacted", &["unfurl"]);

        assert!(!relay_device_needs_unfurl(&device));
        assert!(!relay_device_recoverable(&device));
    }

    fn execution_state() -> RelayExecutionState {
        serde_json::from_value(serde_json::json!({
            "version": PLAN_VERSION,
            "mission_id": "relay-test",
            "replicant_code": "REP-1",
            "vessel_code": "VESSEL-1",
            "hub_location": "ROOT-1-L4",
            "start_system": "ROOT",
            "targets": ["TARGET"],
            "max_hop_ly": DEFAULT_MAX_HOP_LY,
            "network": {
                "start": "ROOT",
                "requested_targets": ["TARGET"],
                "max_hop_ly": DEFAULT_MAX_HOP_LY,
                "nodes": [],
                "edges": [],
                "new_relay_systems": ["TARGET"],
                "activation_systems": [],
                "active_relay_systems": ["ROOT"],
                "execution_order": ["TARGET"],
                "execution_order_optimal": true,
                "execution_hops": 2,
                "execution_distance_ly": 12.0,
                "total_edge_distance_ly": 6.0
            },
            "stops": [{
                "system": "TARGET",
                "location": "TARGET-1-L4",
                "parent_system": "ROOT",
                "action": "deploy_and_activate",
                "relay_code": null,
                "completed": false
            }],
            "hub_stock_relays": [],
            "print_jobs": [{
                "system": "TARGET",
                "factory_code": "FACTORY-1",
                "mission_tag": "relay-m:test",
                "site_tag": "relay-s:TARGET",
                "batch_tag": "relay-b:test",
                "flatpack": false,
                "submission_started": false,
                "operation_id": null,
                "submitted": false,
                "relay_code": null
            }],
            "planned_transport_capacity": 1,
            "supply": null,
            "returned_to_hub": false
        }))
        .expect("valid relay checkpoint")
    }

    #[test]
    fn active_relay_mesh_connectivity_requires_one_connected_component() {
        let positions = BTreeMap::from([
            (
                "ROOT".to_owned(),
                Position {
                    x: 0.0,
                    y: 0.0,
                    z: 0.0,
                },
            ),
            (
                "MID".to_owned(),
                Position {
                    x: 12.0,
                    y: 0.0,
                    z: 0.0,
                },
            ),
            (
                "TARGET".to_owned(),
                Position {
                    x: 19.0,
                    y: 0.0,
                    z: 0.0,
                },
            ),
            (
                "ISLAND".to_owned(),
                Position {
                    x: 40.0,
                    y: 0.0,
                    z: 0.0,
                },
            ),
        ]);
        let ranges = BTreeMap::from([
            ("ROOT".to_owned(), 15.0),
            ("MID".to_owned(), DEFAULT_MAX_HOP_LY),
            ("TARGET".to_owned(), DEFAULT_MAX_HOP_LY),
            ("ISLAND".to_owned(), 10.0),
        ]);

        assert!(relay_mesh_reaches(
            &positions,
            &ranges,
            "ROOT",
            "TARGET",
            DEFAULT_MAX_HOP_LY,
        ));
        assert!(!relay_mesh_reaches(
            &positions,
            &ranges,
            "ROOT",
            "ISLAND",
            DEFAULT_MAX_HOP_LY,
        ));
    }

    #[test]
    fn target_without_active_relay_is_not_yet_ftl_connected() {
        let positions = BTreeMap::from([
            ("ROOT".to_owned(), Position::default()),
            (
                "TARGET".to_owned(),
                Position {
                    x: 5.0,
                    y: 0.0,
                    z: 0.0,
                },
            ),
        ]);
        let ranges = BTreeMap::from([("ROOT".to_owned(), 15.0)]);

        assert!(!relay_mesh_reaches(
            &positions,
            &ranges,
            "ROOT",
            "TARGET",
            DEFAULT_MAX_HOP_LY,
        ));
    }

    #[test]
    fn known_relay_types_have_documented_ranges() {
        assert_eq!(
            documented_relay_range_ly(FTL_RELAY),
            Some(DEFAULT_MAX_HOP_LY)
        );
        assert_eq!(
            documented_relay_range_ly(SYSTEM_HUB),
            Some(REGION_GATEWAY_HUB_RANGE_LY)
        );
        assert_eq!(
            documented_relay_range_ly(DEEP_SPACE_RELAY),
            Some(DEEP_SPACE_RELAY_RANGE_LY)
        );
    }

    #[test]
    fn dsr_fallback_bridges_gap_above_conventional_relay_range() {
        let stars = vec![
            PlannerStar {
                designation: "ROOT".to_owned(),
                position: Position::default(),
                entry_point: None,
            },
            PlannerStar {
                designation: "ANTAR".to_owned(),
                position: Position {
                    x: 7.0,
                    y: 0.0,
                    z: 0.0,
                },
                entry_point: None,
            },
            PlannerStar {
                designation: "ALIPHERATZ".to_owned(),
                position: Position {
                    x: 15.403,
                    y: 0.0,
                    z: 0.0,
                },
                entry_point: None,
            },
        ];
        let request = RelayNetworkRequest {
            start: "ROOT".to_owned(),
            targets: vec!["ALIPHERATZ".to_owned()],
            active_relay_systems: BTreeSet::from(["ROOT".to_owned()]),
            inactive_relay_systems: BTreeSet::new(),
            max_hop_ly: DEFAULT_MAX_HOP_LY,
        };

        let conventional =
            plan_relay_network_with_dsr_fallback(&stars, request.clone(), BTreeMap::new(), false);
        assert!(matches!(
            conventional,
            Err(PlannerError::DisconnectedGap { .. })
        ));

        let (network, dsr_systems) =
            plan_relay_network_with_dsr_fallback(&stars, request, BTreeMap::new(), true)
                .expect("10 ly DSR should bridge the 8.403 ly catalogue gap");
        assert!(dsr_systems.contains("ANTAR"));
        assert!(
            network
                .edges
                .iter()
                .any(|edge| edge.distance_ly > DEFAULT_MAX_HOP_LY)
        );
    }

    #[test]
    fn dsr_fallback_does_not_claim_to_bridge_more_than_ten_ly() {
        let stars = vec![
            PlannerStar {
                designation: "ROOT".to_owned(),
                position: Position::default(),
                entry_point: None,
            },
            PlannerStar {
                designation: "TARGET".to_owned(),
                position: Position {
                    x: 10.5,
                    y: 0.0,
                    z: 0.0,
                },
                entry_point: None,
            },
        ];
        let request = RelayNetworkRequest {
            start: "ROOT".to_owned(),
            targets: vec!["TARGET".to_owned()],
            active_relay_systems: BTreeSet::from(["ROOT".to_owned()]),
            inactive_relay_systems: BTreeSet::new(),
            max_hop_ly: DEFAULT_MAX_HOP_LY,
        };

        assert!(
            plan_relay_network_with_dsr_fallback(&stars, request, BTreeMap::new(), true,).is_err()
        );
    }

    #[test]
    fn checkpoint_status_is_structured_and_restart_stable() {
        let mut state = execution_state();
        assert_eq!(state.status().phase, RelayExecutionPhase::AwaitingRelays);

        state.print_jobs[0].relay_code = Some("RELAY-1".to_owned());
        state.stops[0].relay_code = Some("RELAY-1".to_owned());
        assert_eq!(state.status().phase, RelayExecutionPhase::Deploying);

        state.stops[0].completed = true;
        assert_eq!(state.status().phase, RelayExecutionPhase::ReturningToHub);
        state.returned_to_hub = true;
        let expected = state.status();
        assert_eq!(expected.phase, RelayExecutionPhase::Succeeded);

        let restored: RelayExecutionState =
            serde_json::from_value(serde_json::to_value(&state).expect("serialize checkpoint"))
                .expect("restore checkpoint");
        assert_eq!(restored.status(), expected);
        assert_eq!(restored.status(), restored.status());
    }

    #[test]
    fn resource_factories_are_held_only_while_print_work_is_pending() {
        let mut state = execution_state();
        assert_eq!(state.resources().2, vec!["FACTORY-1"]);

        state.print_jobs[0].relay_code = Some("RELAY-1".to_owned());
        assert!(state.resources().2.is_empty());
    }

    fn print_job(system: &str, factory: &str, batch_tag: Option<&str>) -> PrintJob {
        PrintJob {
            system: system.to_owned(),
            device_type: FTL_RELAY.to_owned(),
            factory_code: factory.to_owned(),
            mission_tag: "relay-m:test".to_owned(),
            site_tag: relay_site_tag(system),
            batch_tag: batch_tag.map(str::to_owned),
            flatpack: false,
            submission_started: false,
            operation_id: None,
            submitted: false,
            relay_code: None,
        }
    }

    fn relay_stop(system: &str, action: StopAction, completed: bool) -> RelayStop {
        RelayStop {
            system: system.to_owned(),
            location: format!("{system}-1-L4"),
            parent_system: "ROOT".to_owned(),
            action,
            device_type: FTL_RELAY.to_owned(),
            relay_code: (action == StopAction::ActivateExisting)
                .then_some(format!("relay-{system}")),
            completed,
        }
    }

    fn dsr_stop(system: &str, completed: bool) -> RelayStop {
        let mut stop = relay_stop(system, StopAction::DeployAndActivate, completed);
        stop.device_type = DEEP_SPACE_RELAY.to_owned();
        stop
    }

    fn factory_state(code: &str, queued_units: usize, printing: bool) -> FactoryState {
        FactoryState {
            code: code.to_owned(),
            queue_size: 5,
            queued_units,
            printing,
            waiting_for_resources: false,
            remaining_seconds: queued_units as f64 * 10.0,
        }
    }

    fn relay_blueprints() -> BTreeMap<String, PrintingBlueprint> {
        [
            (
                FTL_RELAY.to_owned(),
                PrintingBlueprint {
                    device_type: FTL_RELAY.to_owned(),
                    print_time_seconds: 10.0,
                    ..PrintingBlueprint::default()
                },
            ),
            (
                DEEP_SPACE_RELAY.to_owned(),
                PrintingBlueprint {
                    device_type: DEEP_SPACE_RELAY.to_owned(),
                    print_time_seconds: 8.0,
                    ..PrintingBlueprint::default()
                },
            ),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn ignored_printers_are_excluded_from_relay_workloads() {
        let factories = vec![
            factory_state("KEEPBUSY", 0, false),
            factory_state("AVAILABLE", 2, true),
        ];
        let ignored = BTreeSet::from(["KEEPBUSY".to_owned()]);
        let workloads = relay_print_workloads(&factories, &ignored, &BTreeMap::new());
        assert_eq!(workloads.len(), 1);
        assert_eq!(workloads[0].code, "AVAILABLE");
    }

    #[test]
    fn shared_scheduler_keeps_ftl_and_dsr_print_assignments_typed() {
        let factories = vec![factory_state("A", 0, false), factory_state("B", 0, false)];
        let mut jobs = vec![print_job("ONE", "", None), print_job("TWO", "", None)];
        jobs[1].device_type = DEEP_SPACE_RELAY.to_owned();
        let indices = vec![0, 1];

        assign_job_indices_with_shared_scheduler(
            &mut jobs,
            &indices,
            &factories,
            &BTreeSet::new(),
            &relay_blueprints(),
            &BTreeMap::new(),
            "SCEPTURUM-BELT-1",
        )
        .expect("mixed relay types should schedule");

        assert!(!jobs[0].factory_code.is_empty());
        assert!(!jobs[1].factory_code.is_empty());
        assert_eq!(jobs[0].device_type, FTL_RELAY);
        assert_eq!(jobs[1].device_type, DEEP_SPACE_RELAY);
    }

    #[test]
    fn saved_plan_unsubmitted_jobs_are_reassigned_away_from_ignored_printers() {
        let factories = vec![
            factory_state("IGNORED", 0, false),
            factory_state("A", 0, false),
            factory_state("B", 0, false),
        ];
        let all_factory_codes = factories
            .iter()
            .map(|factory| factory.code.clone())
            .collect();
        let ignored = BTreeSet::from(["IGNORED".to_owned()]);
        let mut jobs = vec![
            print_job("ONE", "IGNORED", Some("relay-b:ignored")),
            print_job("TWO", "A", Some("relay-b:a")),
            print_job("THREE", "IGNORED", Some("relay-b:ignored")),
        ];

        let moved = reassign_ignored_print_jobs(
            &mut jobs,
            "mission-test",
            &factories,
            &all_factory_codes,
            &ignored,
            &relay_blueprints(),
            "SCEPTURUM-BELT-1",
        )
        .expect("unsubmitted jobs should be safely reassigned");

        assert_eq!(moved, 2);
        assert!(jobs.iter().all(|job| job.factory_code != "IGNORED"));
        assert_eq!(jobs.iter().filter(|job| job.factory_code == "A").count(), 2);
        assert_eq!(jobs.iter().filter(|job| job.factory_code == "B").count(), 1);
        assert!(jobs.iter().filter(|job| job.system != "TWO").all(|job| {
            let expected =
                relay_batch_tag("mission-test", &job.factory_code, &job.device_type, false);
            job.batch_tag.as_deref() == Some(expected.as_str())
        }));
    }

    #[test]
    fn saved_plan_does_not_reassign_in_flight_jobs_from_ignored_printers() {
        let factories = vec![
            factory_state("IGNORED", 0, false),
            factory_state("AVAILABLE", 0, false),
        ];
        let all_factory_codes = factories
            .iter()
            .map(|factory| factory.code.clone())
            .collect();
        let ignored = BTreeSet::from(["IGNORED".to_owned()]);
        let mut job = print_job("ONE", "IGNORED", None);
        job.submission_started = true;
        let mut jobs = vec![job];

        let error = reassign_ignored_print_jobs(
            &mut jobs,
            "mission-test",
            &factories,
            &all_factory_codes,
            &ignored,
            &relay_blueprints(),
            "SCEPTURUM-BELT-1",
        )
        .expect_err("in-flight work must not be reassigned");

        let message = error.to_string();
        assert!(message.contains("already been submitted or may be in flight"));
        assert!(message.contains("--replace-plan"));
        assert_eq!(jobs[0].factory_code, "IGNORED");
    }

    #[test]
    fn ignored_printer_validation_rejects_codes_outside_the_hub_factory_set() {
        let factory_codes = BTreeSet::from(["KNOWN".to_owned()]);
        let ignored = BTreeSet::from(["TYPO".to_owned()]);

        let error = validate_ignored_printers(&factory_codes, &ignored, "SCEPTURUM-BELT-1")
            .expect_err("unknown ignored printer should fail validation");

        let message = error.to_string();
        assert!(message.contains("TYPO"));
        assert!(message.contains("SCEPTURUM-BELT-1"));
    }

    #[test]
    fn printer_code_lists_are_canonicalized_and_deduplicated() {
        assert_eq!(
            parse_code_list("ff259175, E71bc14b,ff259175"),
            BTreeSet::from(["E71BC14B".to_owned(), "FF259175".to_owned()])
        );
    }

    #[test]
    fn generated_relay_tags_fit_the_api_limit() {
        let mission = relay_system_mission_tag("SCEPTURUM");
        let direct_site = relay_site_tag("XHAKKWUKKXHU");
        let shortened_site =
            relay_site_tag("A-SYSTEM-DESIGNATION-THAT-IS-LONGER-THAN-THE-TAG-LIMIT");
        let batch = relay_batch_tag(&mission, "6523AC61", FTL_RELAY, false);

        for tag in [&mission, &direct_site, &shortened_site, &batch] {
            assert!(tag.chars().count() <= MAX_DEVICE_TAG_CHARS, "{tag}");
        }
        assert_eq!(mission, "relay-m:scepturum");
        assert_eq!(direct_site, "relay-s:XHAKKWUKKXHU");
        assert_ne!(
            shortened_site,
            relay_site_tag("A-SYSTEM-DESIGNATION-THAT-IS-LONGER-THAN-THE-TAG-LIMIU")
        );
    }

    fn planner_star(name: &str, x: f64, y: f64) -> PlannerStar {
        PlannerStar {
            designation: name.to_owned(),
            position: Position { x, y, z: 0.0 },
            entry_point: Some(format!("{name}-1-L4")),
        }
    }

    #[test]
    fn relay_reuse_scope_excludes_disconnected_account_islands() {
        let stars = vec![
            planner_star("BETA-ROOT", 0.0, 0.0),
            planner_star("BETA-OLD", 6.0, 0.0),
            planner_star("ALPHA-ONE", 30.0, 0.0),
            planner_star("ALPHA-TWO", 36.0, 0.0),
        ];
        let active = BTreeSet::from([
            "BETA-ROOT".to_owned(),
            "ALPHA-ONE".to_owned(),
            "ALPHA-TWO".to_owned(),
        ]);
        let inactive = BTreeSet::from(["BETA-OLD".to_owned()]);

        let (scoped_active, scoped_inactive) = relay_reuse_scope(
            &stars,
            "BETA-ROOT",
            &[],
            &active,
            &inactive,
            &BTreeMap::new(),
            DEFAULT_MAX_HOP_LY,
        );

        assert_eq!(scoped_active, BTreeSet::from(["BETA-ROOT".to_owned()]));
        assert_eq!(scoped_inactive, BTreeSet::from(["BETA-OLD".to_owned()]));
    }

    #[test]
    fn relay_reuse_scope_keeps_an_explicit_remote_relay_target() {
        let stars = vec![
            planner_star("BETA-ROOT", 0.0, 0.0),
            planner_star("ALPHA-TARGET", 30.0, 0.0),
            planner_star("ALPHA-OTHER", 36.0, 0.0),
        ];
        let active = BTreeSet::from([
            "BETA-ROOT".to_owned(),
            "ALPHA-TARGET".to_owned(),
            "ALPHA-OTHER".to_owned(),
        ]);

        let (scoped_active, scoped_inactive) = relay_reuse_scope(
            &stars,
            "BETA-ROOT",
            &["ALPHA-TARGET".to_owned()],
            &active,
            &BTreeSet::new(),
            &BTreeMap::new(),
            DEFAULT_MAX_HOP_LY,
        );

        assert_eq!(
            scoped_active,
            BTreeSet::from(["ALPHA-TARGET".to_owned(), "BETA-ROOT".to_owned(),])
        );
        assert!(scoped_inactive.is_empty());
        assert!(!scoped_active.contains("ALPHA-OTHER"));
    }

    #[test]
    fn relay_reuse_scope_honors_extended_system_hub_range() {
        let stars = vec![
            planner_star("HUB", 0.0, 0.0),
            planner_star("REMOTE", 12.0, 0.0),
        ];
        let active = BTreeSet::from(["HUB".to_owned(), "REMOTE".to_owned()]);
        let ranges = BTreeMap::from([("HUB".to_owned(), 15.0)]);

        let (scoped_active, scoped_inactive) = relay_reuse_scope(
            &stars,
            "HUB",
            &[],
            &active,
            &BTreeSet::new(),
            &ranges,
            DEFAULT_MAX_HOP_LY,
        );

        assert_eq!(scoped_active, active);
        assert!(scoped_inactive.is_empty());
    }

    #[test]
    fn trip_batch_caps_carried_relays_and_keeps_interleaved_activation_stops() {
        let stops = vec![
            relay_stop("A", StopAction::DeployAndActivate, false),
            relay_stop("B", StopAction::ActivateExisting, false),
            relay_stop("C", StopAction::DeployAndActivate, false),
            relay_stop("D", StopAction::ActivateExisting, false),
            relay_stop("E", StopAction::DeployAndActivate, false),
        ];

        assert_eq!(next_trip_stop_indices(&stops, 2), vec![0, 1, 2, 3]);
    }

    #[test]
    fn dsr_stops_use_attachment_transport_without_consuming_vessel_stow_slots() {
        let stops = vec![
            dsr_stop("DSR-A", false),
            relay_stop("A", StopAction::DeployAndActivate, false),
            dsr_stop("DSR-B", false),
            relay_stop("B", StopAction::DeployAndActivate, false),
        ];

        assert_eq!(next_trip_stop_indices(&stops, 1), vec![0, 1, 2]);
        let (initial, restocks) = deployment_batches(&stops, 1);
        assert_eq!(initial, vec![1]);
        assert_eq!(restocks, vec![(1, vec![3])]);
    }

    #[test]
    fn trip_batch_skips_completed_stops_before_filling_the_next_load() {
        let stops = vec![
            relay_stop("A", StopAction::DeployAndActivate, true),
            relay_stop("B", StopAction::DeployAndActivate, false),
            relay_stop("C", StopAction::DeployAndActivate, false),
            relay_stop("D", StopAction::DeployAndActivate, false),
        ];

        assert_eq!(next_trip_stop_indices(&stops, 2), vec![1, 2]);
    }

    #[test]
    fn forty_relay_route_places_restocks_after_each_nine_deployments() {
        let systems = [
            "ATHEBIYNE",
            "MESURTHIM",
            "MPUNGO",
            "ALIOTHON",
            "SOGIN",
            "BRACHIOM",
            "PARADYSON",
            "KOMONDORIS",
            "OLRESCHA",
            "HASOSALEH",
            "MAZAR",
            "ALULABORA",
            "BAJAMUR",
            "NUSHOGAK",
            "MIZIR",
            "AMEKBUDA",
            "MAHASIMAR",
            "TIANIGUAN",
            "XIH",
            "TERIBELLUM",
            "FUYIE",
            "EDELTOTON",
            "CIDALA",
            "ACOBENS",
            "RANUGIFER",
            "UMONTUNO",
            "NEMBUS",
            "NEKKARON",
            "ENTARES",
            "GORUMIUN",
            "PAREMLEO",
            "AREKABPRI",
            "HATYSU",
            "ALKARABON",
            "TIMAR",
            "ELKES",
            "MENKUNT",
            "NASTO",
            "SOL",
            "YINU",
        ];
        let stops = systems
            .iter()
            .map(|system| relay_stop(system, StopAction::DeployAndActivate, false))
            .collect::<Vec<_>>();

        let (initial, restocks) = deployment_batches(&stops, 9);

        assert_eq!(initial.len(), 9);
        assert_eq!(restocks.len(), 4);
        assert_eq!(
            restocks
                .iter()
                .map(|(boundary, refill)| (stops[*boundary].system.as_str(), refill.len()))
                .collect::<Vec<_>>(),
            vec![
                ("OLRESCHA", 9),
                ("TIANIGUAN", 9),
                ("NEMBUS", 9),
                ("ELKES", 4),
            ]
        );
    }

    #[test]
    fn staged_supply_uses_one_nine_slot_carrier_per_restock() {
        let quantities = vec![9, 9, 9, 4];
        let carriers = (0..4)
            .map(|index| SupplyCarrierCandidate {
                code: format!("SURGE-{index}"),
                device_type: "surge_carrier".to_owned(),
                attach_capacity: 9,
            })
            .collect::<Vec<_>>();

        let (selected, assignments) =
            staged_supply_assignment(&quantities, &carriers).expect("staged assignment");

        assert_eq!(selected.len(), 4);
        assert_eq!(
            assignments.iter().copied().collect::<BTreeSet<_>>().len(),
            4
        );
    }

    #[test]
    fn minimal_supply_preloads_one_large_carrier_for_every_restock() {
        let quantities = vec![9, 9, 9, 4];
        let carriers = vec![SupplyCarrierCandidate {
            code: "MOBILE-1".to_owned(),
            device_type: "mobile_fleet".to_owned(),
            attach_capacity: 31,
        }];

        let (selected, assignments) =
            minimal_supply_assignment(&quantities, &carriers).expect("minimal assignment");

        assert_eq!(selected.len(), 1);
        assert_eq!(assignments, vec![0, 0, 0, 0]);
    }

    #[test]
    fn relay_print_jobs_normalize_dsr_to_flatpack_and_ftl_to_assembled() {
        let mut jobs = vec![
            print_job("ATHEBIYNE", "6523AC61", None),
            print_job("HASOSALEH", "6523AC61", None),
        ];
        jobs[0].flatpack = true;
        jobs[1].device_type = DEEP_SPACE_RELAY.to_owned();

        assign_new_plan_print_batches("relay-m:test", &mut jobs);

        assert!(!jobs[0].flatpack);
        assert!(jobs[1].flatpack);
        assert_ne!(jobs[0].batch_tag, jobs[1].batch_tag);
        let groups = pending_print_groups(&jobs, "6523AC61");
        assert_eq!(groups.len(), 2);
        assert!(groups.contains(&vec![0]));
        assert!(groups.contains(&vec![1]));
    }

    #[test]
    fn relay_prerequisite_tag_is_stable_and_within_api_limit() {
        let first = relay_prerequisite_tag("mission-test");
        let second = relay_prerequisite_tag("mission-test");
        assert_eq!(first, second);
        assert!(first.starts_with(RELAY_PREREQUISITE_TAG_PREFIX));
        assert!(first.chars().count() <= MAX_DEVICE_TAG_CHARS);
    }

    #[test]
    fn pending_jobs_with_one_batch_tag_share_one_quantity_submission() {
        let jobs = vec![
            print_job("WIHAX", "6523AC61", Some("relay-b:one")),
            print_job("KRAKHUX", "6523AC61", Some("relay-b:one")),
            print_job("XHAKHKHU", "850547EE", Some("relay-b:two")),
        ];

        let groups = pending_print_groups(&jobs, "6523AC61");

        assert_eq!(groups, vec![vec![0, 1]]);
    }

    #[test]
    fn legacy_site_tags_remain_individual_submissions() {
        let jobs = vec![
            print_job("WIHAX", "6523AC61", None),
            print_job("KRAKHUX", "6523AC61", None),
        ];

        let groups = pending_print_groups(&jobs, "6523AC61");

        assert_eq!(groups, vec![vec![1], vec![0]]);
    }
}
