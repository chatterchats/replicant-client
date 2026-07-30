//! Plans and executes an account-owned FTL relay expansion.
//!
//! The example uses the pure `replicant-route-planner` workspace crate for an
//! exact minimum-new-relay Steiner tree over a uniform 7.499 ly graph. Managed
//! client operations perform all mutations. The raw escape hatch is used only
//! for blueprint costs and autofactory queue details that are not represented
//! by the normalized managed projection.
//!
//! Dry-run planning is the default. Add `--execute` to manufacture, transport,
//! deploy, activate, verify, and return to the manufacturing hub.
//!
//! ```text
//! cargo run --example expand_ftl_relay_network -- \
//!   --replicant Chats-1 \
//!   --hub SCEPTURUM-BELT-1 \
//!   WIHAX ILPHARD KRAKHUX XHAKKWUKKXHU XIHAKHXA XHAKHKHU
//!
//! cargo run --example expand_ftl_relay_network -- \
//!   --execute \
//!   --replicant Chats-1 \
//!   --hub SCEPTURUM-BELT-1 \
//!   WIHAX ILPHARD KRAKHUX XHAKKWUKKXHU XIHAKHXA XHAKHKHU
//! ```
//!
//! Environment:
//!
//! - `RS_API_TOKEN` (required)
//! - `REPLICANT_DB=replicant-client.sqlite`
//! - `RS_RELAY_REPLICANT=Chats-1`
//! - `RS_RELAY_HUB=SCEPTURUM-BELT-1`
//! - `RS_RELAY_PLAN=ftl-relay-expansion.json`
//! - `RS_RELAY_EXECUTE=1`
//! - `RS_RELAY_REBUILD_PLAN=1`
//! - `RS_RELAY_WAIT_TIMEOUT_SECS=21600`

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error as StdError,
    fs::{self, File},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use replicant_client::{
    Client, Device, DeviceType, Operation, OperationId, OperationStatus, Replicant, SecretString,
    StartupPolicy, raw,
};
use replicant_route_planner::{
    NetworkNode, Position, RelayAvailability, RelayNetworkPlan, RelayNetworkRequest,
    Star as PlannerStar, plan_relay_network,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::{Instant, sleep, timeout};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const PLAN_VERSION: u32 = 1;
const DEFAULT_MAX_HOP_LY: f64 = 7.499;
const FTL_RELAY: &str = "ftl_relay";
const AUTOFACTORY: &str = "autofactory";
const RELAYING: &str = "relaying";
const POLL_INTERVAL: Duration = Duration::from_secs(15);
const MAX_DEVICE_TAG_CHARS: usize = 32;
const RELAY_MISSION_TAG_PREFIX: &str = "relay-m:";
const RELAY_SITE_TAG_PREFIX: &str = "relay-s:";
const RELAY_BATCH_TAG_PREFIX: &str = "relay-b:";
const LEGACY_RELAY_MISSION_TAG_PREFIX: &str = "relay-expansion:";
const LEGACY_RELAY_SITE_TAG_PREFIX: &str = "relay-site:";

type AnyError = Box<dyn StdError + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> AnyError {
    io::Error::new(kind, message.into()).into()
}

#[derive(Debug)]
struct Config {
    token: SecretString,
    database: PathBuf,
    replicant: String,
    hub: String,
    plan_path: PathBuf,
    max_hop_ly: f64,
    execute: bool,
    rebuild_plan: bool,
    wait_timeout: Duration,
    targets: Vec<String>,
}

impl Config {
    fn from_args_and_env() -> AnyResult<Self> {
        let token = env::var("RS_API_TOKEN")
            .map(SecretString::from)
            .map_err(|_| app_error(io::ErrorKind::NotFound, "RS_API_TOKEN is not set"))?;
        let mut database = PathBuf::from(
            env::var("REPLICANT_DB").unwrap_or_else(|_| "replicant-client.sqlite".into()),
        );
        let mut replicant = env::var("RS_RELAY_REPLICANT").unwrap_or_else(|_| "Chats-1".into());
        let mut hub = env::var("RS_RELAY_HUB").unwrap_or_else(|_| "SCEPTURUM-BELT-1".into());
        let mut plan_path = PathBuf::from(
            env::var("RS_RELAY_PLAN").unwrap_or_else(|_| "ftl-relay-expansion.json".into()),
        );
        let mut max_hop_ly = DEFAULT_MAX_HOP_LY;
        let mut execute = env_flag("RS_RELAY_EXECUTE");
        let mut rebuild_plan = env_flag("RS_RELAY_REBUILD_PLAN");
        let mut wait_timeout = Duration::from_secs(
            env::var("RS_RELAY_WAIT_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(21_600),
        );
        let mut targets = Vec::new();

        let mut arguments = env::args().skip(1);
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--execute" => execute = true,
                "--rebuild-plan" => rebuild_plan = true,
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
                value => targets.extend(
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(str::to_uppercase),
                ),
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
        Ok(Self {
            token,
            database,
            replicant,
            hub: hub.to_uppercase(),
            plan_path,
            max_hop_ly,
            execute,
            rebuild_plan,
            wait_timeout,
            targets,
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

fn print_help() {
    println!(
        "FTL relay expansion\n\n\
Usage:\n  cargo run --example expand_ftl_relay_network -- [OPTIONS] SYSTEM...\n\n\
Options:\n  --execute                  Permit gameplay mutations\n  --rebuild-plan             Replace the saved plan\n  --replicant NAME_OR_CODE   Transport replicant (default: Chats-1)\n  --hub LOCATION             Manufacturing hub (default: SCEPTURUM-BELT-1)\n  --plan PATH                Saved mission plan\n  --database PATH            Managed SQLite database\n  --max-hop LY               Uniform relay range (default: 7.499)\n  --wait-timeout-secs N      Per-phase timeout\n  -h, --help                 Show this help\n\n\
Targets are system designations, not planet locations."
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
    relay_code: Option<String>,
    completed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PrintJob {
    system: String,
    factory_code: String,
    mission_tag: String,
    site_tag: String,
    #[serde(default)]
    batch_tag: Option<String>,
    submission_started: bool,
    operation_id: Option<String>,
    submitted: bool,
    relay_code: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MissionPlan {
    version: u32,
    mission_id: String,
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
    returned_to_hub: bool,
}

#[derive(Clone, Debug)]
struct DeviceCensus {
    devices: BTreeMap<String, Device>,
    active_relay_codes: BTreeMap<String, Vec<String>>,
    inactive_relay_codes: BTreeMap<String, Vec<String>>,
    hub_stock: Vec<String>,
    factories: Vec<FactoryState>,
}

#[derive(Clone, Debug)]
struct FactoryState {
    code: String,
    queue_size: usize,
    queue_depth: usize,
    printing: bool,
    observed_job_tags: Vec<BTreeSet<String>>,
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("replicant_client=info,info")),
        )
        .try_init()
        .map_err(|error| app_error(io::ErrorKind::Other, error.to_string()))?;

    let config = Config::from_args_and_env()?;
    let client = Client::builder()
        .authentication_token(config.token.clone())
        .sqlite(&config.database)
        .startup_policy(StartupPolicy::Essential)
        .start()
        .await?;

    let result = run(&client, &config).await;
    let close_result = client.close().await;
    close_result?;
    result
}

async fn run(client: &Client, config: &Config) -> AnyResult<()> {
    let sync = client.sync().full().await?;
    info!(readiness = ?sync.readiness, "full managed synchronization completed");
    client.galaxy().refresh_catalogue().await?;

    let replicant = resolve_owned_replicant(client, &config.replicant).await?;
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

    let mut plan = if config.plan_path.exists() && !config.rebuild_plan {
        let plan = load_plan(&config.plan_path)?;
        validate_loaded_plan(&plan, config, &replicant_code, &vessel_code)?;
        plan
    } else {
        if config.targets.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "at least one system target is required when creating a plan",
            ));
        }
        let plan = create_plan(client, config, &replicant_code, &vessel_code).await?;
        save_plan(&config.plan_path, &plan)?;
        plan
    };

    reconcile_plan(client, &mut plan).await?;
    save_plan(&config.plan_path, &plan)?;
    print_plan(&plan);

    if !config.execute {
        warn!(
            plan = %config.plan_path.display(),
            "plan only; rerun with --execute to permit manufacturing and deployment"
        );
        return Ok(());
    }

    execute_plan(client, config, &mut plan).await?;
    save_plan(&config.plan_path, &plan)?;
    Ok(())
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

async fn create_plan(
    client: &Client,
    config: &Config,
    replicant_code: &str,
    vessel_code: &str,
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

    let census = refresh_device_census(client, &config.hub, vessel_code, &system_names).await?;
    if census.factories.is_empty() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!(
                "no account-owned autofactories are present at {}",
                config.hub
            ),
        ));
    }
    if !census.active_relay_codes.contains_key(&start_system) {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "starting system {start_system} does not currently have an account-owned FTL relay in relaying status"
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
    let network = plan_relay_network(
        stars,
        RelayNetworkRequest {
            start: start_system.clone(),
            targets: config.targets.clone(),
            active_relay_systems: census.active_relay_codes.keys().cloned().collect(),
            inactive_relay_systems: census.inactive_relay_codes.keys().cloned().collect(),
            max_hop_ly: config.max_hop_ly,
        },
    )?;

    let mission_id = format!("{}-{}", start_system.to_lowercase(), uuid::Uuid::new_v4());
    let mut hub_stock = census.hub_stock.clone();
    hub_stock.sort();
    let mut factory_load = census
        .factories
        .iter()
        .map(|factory| {
            (
                factory.code.clone(),
                factory.queue_depth + if factory.printing { 1 } else { 0 },
            )
        })
        .collect::<BTreeMap<_, _>>();

    let nodes = network
        .nodes
        .iter()
        .map(|node| (node.system.as_str(), node))
        .collect::<BTreeMap<_, _>>();
    let mut stops = Vec::new();
    let mut print_jobs = Vec::new();
    let mut used_hub_stock = Vec::new();

    for system in &network.execution_order {
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
                            format!("inactive relay selected at {system} but no device was found"),
                        )
                    })?;
                let relay = census.devices.get(&relay_code).ok_or_else(|| {
                    app_error(
                        io::ErrorKind::NotFound,
                        "selected relay omitted from census",
                    )
                })?;
                let location = device_location(relay).ok_or_else(|| {
                    app_error(
                        io::ErrorKind::InvalidData,
                        format!("inactive relay {relay_code} has no location"),
                    )
                })?;
                stops.push(RelayStop {
                    system: system.clone(),
                    location: location.to_owned(),
                    parent_system,
                    action: StopAction::ActivateExisting,
                    relay_code: Some(relay_code),
                    completed: false,
                });
            }
            RelayAvailability::New => {
                let location = choose_l4_location(client, node).await?;
                let relay_code = hub_stock.pop().inspect(|code| {
                    used_hub_stock.push(code.clone());
                });
                if relay_code.is_none() {
                    let factory_code = least_loaded_factory(&mut factory_load)?;
                    let mission_tag = relay_mission_tag(&mission_id);
                    let site_tag = relay_site_tag(system);
                    print_jobs.push(PrintJob {
                        system: system.clone(),
                        factory_code,
                        mission_tag,
                        site_tag,
                        batch_tag: None,
                        submission_started: false,
                        operation_id: None,
                        submitted: false,
                        relay_code: None,
                    });
                }
                stops.push(RelayStop {
                    system: system.clone(),
                    location,
                    parent_system,
                    action: StopAction::DeployAndActivate,
                    relay_code,
                    completed: false,
                });
            }
            RelayAvailability::Active => {}
        }
    }

    assign_new_plan_print_batches(&mission_id, &mut print_jobs);

    let vessel = client.devices().get(vessel_code).await?.snapshot().await?;
    let free_slots = vessel
        .stow_capacity
        .unwrap_or(0)
        .saturating_sub(vessel.stow_used.unwrap_or(0));
    let already_stowed = used_hub_stock
        .iter()
        .filter(|code| {
            census.devices.get(*code).is_some_and(|device| {
                device
                    .relationships
                    .stowed_in
                    .as_ref()
                    .is_some_and(|container| container.id.as_str() == vessel_code)
            })
        })
        .count();
    let transport_capacity = free_slots.saturating_add(i64::try_from(already_stowed)?);
    let transport_required = i64::try_from(network.new_relay_systems.len())?;
    if transport_required > transport_capacity {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "vessel {vessel_code} can carry {transport_capacity} mission relays in its current configuration, but {transport_required} are required; multi-trip deployment is not implemented"
            ),
        ));
    }

    ensure_manufacturing_resources(client, &config.hub, print_jobs.len()).await?;
    Ok(MissionPlan {
        version: PLAN_VERSION,
        mission_id,
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
        returned_to_hub: false,
    })
}

fn least_loaded_factory(loads: &mut BTreeMap<String, usize>) -> AnyResult<String> {
    let code = loads
        .iter()
        .min_by(|(left_code, left_load), (right_code, right_load)| {
            left_load
                .cmp(right_load)
                .then_with(|| left_code.cmp(right_code))
        })
        .map(|(code, _)| code.clone())
        .ok_or_else(|| app_error(io::ErrorKind::NotFound, "no autofactory is available"))?;
    *loads.entry(code.clone()).or_default() += 1;
    Ok(code)
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
    locations
        .iter()
        .find(|location| location.key.id.as_str().ends_with("-L5"))
        .map(|location| location.key.id.as_str().to_owned())
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("{} has no known L4 or L5 deployment location", node.system),
            )
        })
}

async fn refresh_device_census(
    client: &Client,
    hub: &str,
    vessel_code: &str,
    systems: &BTreeSet<String>,
) -> AnyResult<DeviceCensus> {
    let handles = client
        .devices()
        .refresh_many()
        .page_size(50)
        .max_pages(200)
        .collect()
        .await?;
    let mut devices = BTreeMap::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        devices.insert(handle.id().as_str().to_owned(), snapshot);
    }

    let mut active_relay_codes = BTreeMap::<String, Vec<String>>::new();
    let mut inactive_relay_codes = BTreeMap::<String, Vec<String>>::new();
    let mut hub_stock = Vec::new();
    let mut factory_codes = Vec::new();
    for (code, device) in &devices {
        match device_type(device) {
            Some(FTL_RELAY) => {
                let Some(location) = device_location(device) else {
                    continue;
                };
                let stowed_in_transport = device
                    .relationships
                    .stowed_in
                    .as_ref()
                    .is_some_and(|container| container.id.as_str() == vessel_code);
                if location == hub
                    && (device.relationships.stowed_in.is_none() || stowed_in_transport)
                    && device.relationships.attached_to.is_none()
                    && device_status(device) != Some(RELAYING)
                    && (stowed_in_transport || device_has_command(device, "stow"))
                {
                    hub_stock.push(code.clone());
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
                if device_status(device) == Some(RELAYING) {
                    active_relay_codes
                        .entry(system)
                        .or_default()
                        .push(code.clone());
                } else if device_has_command(device, "activate") {
                    inactive_relay_codes
                        .entry(system)
                        .or_default()
                        .push(code.clone());
                }
            }
            Some(AUTOFACTORY) if device_location(device) == Some(hub) => {
                factory_codes.push(code.clone());
            }
            _ => {}
        }
    }
    for codes in active_relay_codes.values_mut() {
        codes.sort();
    }
    for (system, codes) in &mut inactive_relay_codes {
        codes.sort();
        if active_relay_codes.contains_key(system) {
            codes.clear();
        }
    }
    inactive_relay_codes.retain(|_, codes| !codes.is_empty());
    hub_stock.sort();

    let mut factories = Vec::new();
    for code in factory_codes {
        factories.push(fetch_factory_state(client, &code).await?);
    }
    factories.sort_by_key(|factory| (factory.printing, factory.queue_depth, factory.code.clone()));
    Ok(DeviceCensus {
        devices,
        active_relay_codes,
        inactive_relay_codes,
        hub_stock,
        factories,
    })
}

async fn fetch_factory_state(client: &Client, code: &str) -> AnyResult<FactoryState> {
    let detail = client.raw().devices().get(code).await?.value;
    let mut observed_job_tags = Vec::new();
    if let Some(printing) = &detail.printing {
        observed_job_tags.push(printing.tags.iter().cloned().collect());
    }
    for queued in &detail.print_queue {
        let mut tags = BTreeSet::new();
        collect_relay_tags(&Value::Object(queued.clone()), &mut tags);
        observed_job_tags.push(tags);
    }
    Ok(FactoryState {
        code: code.to_owned(),
        queue_size: usize::try_from(detail.queue_size.unwrap_or(1).max(1)).unwrap_or(1),
        queue_depth: detail.print_queue.len(),
        printing: detail.printing.is_some(),
        observed_job_tags,
    })
}

fn collect_relay_tags(value: &Value, tags: &mut BTreeSet<String>) {
    match value {
        Value::String(value) if is_relay_job_tag(value) => {
            tags.insert(value.clone());
        }
        Value::Array(values) => {
            for value in values {
                collect_relay_tags(value, tags);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                collect_relay_tags(value, tags);
            }
        }
        _ => {}
    }
}

fn is_relay_job_tag(tag: &str) -> bool {
    tag.starts_with(RELAY_MISSION_TAG_PREFIX)
        || tag.starts_with(RELAY_SITE_TAG_PREFIX)
        || tag.starts_with(RELAY_BATCH_TAG_PREFIX)
        || tag.starts_with(LEGACY_RELAY_MISSION_TAG_PREFIX)
        || tag.starts_with(LEGACY_RELAY_SITE_TAG_PREFIX)
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

fn relay_mission_tag(mission_id: &str) -> String {
    format!(
        "{RELAY_MISSION_TAG_PREFIX}{:016x}",
        stable_tag_hash(mission_id)
    )
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

fn relay_batch_tag(mission_id: &str, factory_code: &str) -> String {
    format!(
        "{RELAY_BATCH_TAG_PREFIX}{:016x}",
        stable_tag_hash(&format!("{mission_id}:{factory_code}"))
    )
}

fn print_job_correlation_tag(job: &PrintJob) -> &str {
    job.batch_tag.as_deref().unwrap_or(&job.site_tag)
}

fn assign_new_plan_print_batches(mission_id: &str, jobs: &mut [PrintJob]) {
    for job in jobs {
        job.batch_tag = Some(relay_batch_tag(mission_id, &job.factory_code));
    }
}

fn assign_safe_legacy_print_batches(plan: &mut MissionPlan) {
    let mut factories = BTreeSet::new();
    for job in &plan.print_jobs {
        if job.batch_tag.is_none()
            && !job.submission_started
            && job.operation_id.is_none()
            && !job.submitted
            && job.relay_code.is_none()
        {
            factories.insert(job.factory_code.clone());
        }
    }
    for factory_code in factories {
        if plan
            .print_jobs
            .iter()
            .any(|job| job.factory_code == factory_code && job.batch_tag.is_some())
        {
            continue;
        }
        let batch_tag = relay_batch_tag(&plan.mission_id, &factory_code);
        for job in &mut plan.print_jobs {
            if job.factory_code == factory_code
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
    let mission_tag = relay_mission_tag(&plan.mission_id);
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

async fn ensure_manufacturing_resources(
    client: &Client,
    hub: &str,
    quantity: usize,
) -> AnyResult<()> {
    if quantity == 0 {
        return Ok(());
    }
    let blueprints = client.raw().blueprints().list().await?.value.blueprints;
    let blueprint = blueprints
        .iter()
        .find(|blueprint| blueprint.device_type.as_deref() == Some(FTL_RELAY))
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                "FTL relay blueprint is not unlocked",
            )
        })?;
    let mut requirements = numeric_requirements(blueprint.resources.as_ref());
    for (component, amount) in numeric_requirements(blueprint.components.as_ref()) {
        *requirements.entry(component).or_default() += amount;
    }
    for required in requirements.values_mut() {
        *required = required.saturating_mul(i64::try_from(quantity)?);
    }

    let (inventories, _) = client
        .inventory()
        .list(&raw::inventory::AccountInventoryQuery {
            location: Some(hub.to_owned()),
            cursor: None,
            limit: Some(50),
        })
        .await?;
    let inventory = inventories
        .into_iter()
        .find(|inventory| {
            inventory
                .location
                .as_ref()
                .is_some_and(|location| location.id.as_str() == hub)
        })
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("inventory response omitted manufacturing hub {hub}"),
            )
        })?;
    let available = inventory
        .items
        .into_iter()
        .map(|item| (item.resource, item.quantity))
        .collect::<BTreeMap<_, _>>();
    let shortages = requirements
        .iter()
        .filter_map(|(resource, required)| {
            let available = available.get(resource).copied().unwrap_or(0);
            (available < *required).then_some(format!(
                "{resource}: need {required}, available {available}"
            ))
        })
        .collect::<Vec<_>>();
    if !shortages.is_empty() {
        return Err(app_error(
            io::ErrorKind::Other,
            format!(
                "insufficient manufacturing inventory at {hub}: {}",
                shortages.join("; ")
            ),
        ));
    }
    Ok(())
}

fn numeric_requirements(object: Option<&raw::JsonObject>) -> BTreeMap<String, i64> {
    object
        .into_iter()
        .flat_map(|object| object.iter())
        .filter_map(|(name, value)| {
            value
                .as_i64()
                .or_else(|| value.as_f64().map(|value| value.ceil() as i64))
                .map(|quantity| (name.clone(), quantity.max(0)))
        })
        .collect()
}

async fn ensure_planned_active_coverage(client: &Client, plan: &MissionPlan) -> AnyResult<()> {
    let mut required = plan
        .network
        .active_relay_systems
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    required.insert(plan.start_system.clone());

    let handles = client
        .devices()
        .refresh_many()
        .of_type(DeviceType::FtlRelay)
        .page_size(50)
        .max_pages(200)
        .collect()
        .await?;
    let mut active = BTreeSet::new();
    for handle in handles {
        let snapshot = handle.snapshot().await?;
        if device_status(&snapshot) != Some(RELAYING)
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

async fn reconcile_plan(client: &Client, plan: &mut MissionPlan) -> AnyResult<()> {
    normalize_print_job_tags(plan)?;
    ensure_planned_active_coverage(client, plan).await?;
    let mission_tag = relay_mission_tag(&plan.mission_id);
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

    let factory_codes = plan
        .print_jobs
        .iter()
        .map(|job| job.factory_code.clone())
        .collect::<BTreeSet<_>>();
    let mut factory_job_tags = BTreeMap::<String, Vec<BTreeSet<String>>>::new();
    for code in factory_codes {
        let state = fetch_factory_state(client, &code).await?;
        factory_job_tags.insert(code, state.observed_job_tags);
    }

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
                    tags.contains(&plan.print_jobs[index].mission_tag)
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
        let Ok(handle) = client.devices().get(&code).await else {
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
            plan.stops[index].completed = network.connections.iter().any(|connection| {
                connection.star.as_deref() == Some(plan.stops[index].parent_system.as_str())
            });
        }
    }

    let replicant = client
        .replicants()
        .get_owned(&plan.replicant_code)
        .await?
        .snapshot()
        .await?;
    plan.returned_to_hub = plan.stops.iter().all(|stop| stop.completed)
        && replicant.travel.is_none()
        && replicant
            .location
            .as_ref()
            .is_some_and(|location| location.id.as_str() == plan.hub_location.as_str());
    Ok(())
}

async fn execute_plan(client: &Client, config: &Config, plan: &mut MissionPlan) -> AnyResult<()> {
    ensure_manufacturing_resources(
        client,
        &plan.hub_location,
        plan.print_jobs.iter().filter(|job| !job.submitted).count(),
    )
    .await?;
    submit_print_jobs(client, config, plan).await?;
    wait_for_printed_relays(client, config, plan).await?;
    assign_printed_relays(plan)?;
    save_plan(&config.plan_path, plan)?;

    transfer_mission_relays(client, config, plan).await?;
    stow_transport_relays(client, config, plan).await?;

    for index in 0..plan.stops.len() {
        if plan.stops[index].completed {
            continue;
        }
        execute_stop(client, config, plan, index).await?;
        save_plan(&config.plan_path, plan)?;
    }

    travel_to(client, config, &plan.replicant_code, &plan.hub_location).await?;
    plan.returned_to_hub = true;
    save_plan(&config.plan_path, plan)?;
    info!(hub = %plan.hub_location, "relay expansion completed and transport returned");
    Ok(())
}

async fn submit_print_jobs(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
) -> AnyResult<()> {
    reconcile_plan(client, plan).await?;
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
        let factory_codes = plan
            .print_jobs
            .iter()
            .map(|job| job.factory_code.clone())
            .collect::<BTreeSet<_>>();
        let mut states = Vec::new();
        for code in factory_codes {
            states.push(fetch_factory_state(client, &code).await?);
        }
        states.sort_by_key(|state| (state.printing, state.queue_depth, state.code.clone()));
        let mut submitted_any = false;
        for state in states {
            let slots = state.queue_size.saturating_sub(state.queue_depth);
            for job_indices in pending_print_groups(&plan.print_jobs, &state.code)
                .into_iter()
                .take(slots)
            {
                let first_index = *job_indices
                    .first()
                    .ok_or_else(|| app_error(io::ErrorKind::InvalidData, "empty print batch"))?;
                let mission_tag = plan.print_jobs[first_index].mission_tag.clone();
                let correlation_tag =
                    print_job_correlation_tag(&plan.print_jobs[first_index]).to_owned();
                let quantity = i64::try_from(job_indices.len())?;
                for index in &job_indices {
                    plan.print_jobs[*index].submission_started = true;
                }
                save_plan(&config.plan_path, plan)?;

                let factory = client.devices().get(&state.code).await?;
                let operation = factory
                    .enqueue_print_with_tags(FTL_RELAY, quantity, [mission_tag, correlation_tag])
                    .await?;
                let operation_id = operation.id().as_str().to_owned();
                for index in &job_indices {
                    plan.print_jobs[*index].operation_id = Some(operation_id.clone());
                }
                save_plan(&config.plan_path, plan)?;
                ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
                for index in &job_indices {
                    plan.print_jobs[*index].submitted = true;
                }
                save_plan(&config.plan_path, plan)?;
                info!(
                    factory = %state.code,
                    quantity,
                    systems = %job_indices
                        .iter()
                        .map(|index| plan.print_jobs[*index].system.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    "queued FTL relay print batch"
                );
                submitted_any = true;
            }
        }
        if !submitted_any {
            wait_for_relevant_event(&mut watch, deadline, &["print.completed"]).await?;
            reconcile_plan(client, plan).await?;
            save_plan(&config.plan_path, plan)?;
        }
    }
    Ok(())
}

async fn wait_for_printed_relays(
    client: &Client,
    config: &Config,
    plan: &mut MissionPlan,
) -> AnyResult<()> {
    if plan.print_jobs.is_empty() {
        return Ok(());
    }
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        reconcile_plan(client, plan).await?;
        save_plan(&config.plan_path, plan)?;
        if plan.print_jobs.iter().all(|job| job.relay_code.is_some()) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                "timed out waiting for FTL relay printing",
            ));
        }
        wait_for_relevant_event(&mut watch, deadline, &["print.completed"]).await?;
    }
}

fn assign_printed_relays(plan: &mut MissionPlan) -> AnyResult<()> {
    for stop in &mut plan.stops {
        if stop.action != StopAction::DeployAndActivate || stop.relay_code.is_some() {
            continue;
        }
        stop.relay_code = plan
            .print_jobs
            .iter()
            .find(|job| job.system == stop.system)
            .and_then(|job| job.relay_code.clone());
        if stop.relay_code.is_none() {
            return Err(app_error(
                io::ErrorKind::NotFound,
                format!("no printed relay was identified for {}", stop.system),
            ));
        }
    }
    Ok(())
}

async fn transfer_mission_relays(
    client: &Client,
    config: &Config,
    plan: &MissionPlan,
) -> AnyResult<()> {
    let codes = plan
        .stops
        .iter()
        .filter_map(|stop| stop.relay_code.clone())
        .collect::<BTreeSet<_>>();
    for code in codes {
        let handle = client.devices().get(&code).await?;
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
            ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
            wait_for_device(client, config, &code, |device| {
                assigned_replicant(device) == Some(plan.replicant_code.as_str())
            })
            .await?;
        }
    }
    Ok(())
}

async fn stow_transport_relays(
    client: &Client,
    config: &Config,
    plan: &MissionPlan,
) -> AnyResult<()> {
    let pending = plan
        .stops
        .iter()
        .filter(|stop| stop.action == StopAction::DeployAndActivate && !stop.completed)
        .collect::<Vec<_>>();
    let mut to_stow = Vec::new();
    for stop in pending {
        let code = stop.relay_code.as_deref().ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("stop {} has no assigned relay", stop.system),
            )
        })?;
        let snapshot = client.devices().get(code).await?.snapshot().await?;
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
    let vessel = client
        .devices()
        .get(&plan.vessel_code)
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
                "vessel {} has {free} free stow slots but {} are required",
                plan.vessel_code,
                to_stow.len()
            ),
        ));
    }

    for code in to_stow {
        let handle = client.devices().get(&code).await?;
        let snapshot = handle.snapshot().await?;
        if !device_has_command(&snapshot, "stow") {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("relay {code} does not currently advertise stow"),
            ));
        }
        let operation = handle.stow(Some(plan.vessel_code.clone())).await?;
        ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
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
    travel_to(client, config, &plan.replicant_code, &stop.location).await?;
    let relay = client.devices().get(relay_code).await?;
    let snapshot = relay.snapshot().await?;
    if stop.action == StopAction::DeployAndActivate && snapshot.relationships.stowed_in.is_some() {
        if !device_has_command(&snapshot, "deploy") {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("relay {relay_code} does not currently advertise deploy"),
            ));
        }
        let operation = relay.deploy().await?;
        ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
        wait_for_device(client, config, relay_code, |device| {
            device.relationships.stowed_in.is_none()
                && device_location(device)
                    .is_some_and(|location| designation_in_system(location, &stop.system))
        })
        .await?;
    }

    let snapshot = client.devices().get(relay_code).await?.snapshot().await?;
    if device_status(&snapshot) != Some(RELAYING) {
        if !device_has_command(&snapshot, "activate") {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("relay {relay_code} does not currently advertise activate"),
            ));
        }
        let operation = client.devices().get(relay_code).await?.activate().await?;
        ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
        wait_for_device(client, config, relay_code, |device| {
            device_status(device) == Some(RELAYING)
        })
        .await?;
    }
    wait_for_parent_connection(client, config, relay_code, &stop.parent_system).await?;
    plan.stops[index].completed = true;
    info!(system = %stop.system, relay = relay_code, "relay stop verified");
    Ok(())
}

async fn travel_to(
    client: &Client,
    config: &Config,
    replicant_code: &str,
    destination: &str,
) -> AnyResult<()> {
    let handle = client.replicants().get_owned(replicant_code).await?;
    let snapshot = handle.snapshot().await?;
    if snapshot.travel.is_none()
        && snapshot
            .location
            .as_ref()
            .is_some_and(|location| location.id.as_str() == destination)
    {
        return Ok(());
    }
    if let Some(travel) = &snapshot.travel {
        let planned_destination = travel
            .final_destination
            .as_ref()
            .or(travel.destination.as_ref())
            .map(|location| location.id.as_str());
        if planned_destination != Some(destination) {
            return Err(app_error(
                io::ErrorKind::Other,
                format!(
                    "replicant {replicant_code} is already traveling to {:?}, not {destination}",
                    planned_destination
                ),
            ));
        }
    } else {
        let operation = handle.travel().to(destination).depart().await?;
        ensure_operation_accepted(&operation, Duration::from_secs(30)).await?;
    }

    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        let snapshot = client
            .replicants()
            .get_owned(replicant_code)
            .await?
            .snapshot()
            .await?;
        if snapshot.travel.is_none()
            && snapshot
                .location
                .as_ref()
                .is_some_and(|location| location.id.as_str() == destination)
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out traveling to {destination}"),
            ));
        }
        wait_for_relevant_event(&mut watch, deadline, &["travel.arrived"]).await?;
    }
}

async fn wait_for_device(
    client: &Client,
    config: &Config,
    code: &str,
    predicate: impl Fn(&Device) -> bool,
) -> AnyResult<()> {
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        let device = client.devices().get(code).await?.snapshot().await?;
        if predicate(&device) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for device {code}"),
            ));
        }
        wait_for_relevant_event(&mut watch, deadline, &[]).await?;
    }
}

async fn wait_for_parent_connection(
    client: &Client,
    config: &Config,
    relay_code: &str,
    parent_system: &str,
) -> AnyResult<()> {
    let mut watch = client.events().watch().await?;
    let deadline = Instant::now() + config.wait_timeout;
    loop {
        let network = client.devices().get(relay_code).await?.network().await?;
        if network
            .connections
            .iter()
            .any(|connection| connection.star.as_deref() == Some(parent_system))
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "relay {relay_code} never connected to expected parent system {parent_system}"
                ),
            ));
        }
        wait_for_relevant_event(&mut watch, deadline, &[]).await?;
    }
}

async fn wait_for_relevant_event(
    watch: &mut replicant_client::EventWatch,
    deadline: Instant,
    names: &[&str],
) -> AnyResult<()> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    let interval = remaining.min(POLL_INTERVAL);
    match timeout(interval, watch.next()).await {
        Ok(Ok(event)) if names.is_empty() || names.contains(&event.name.as_str()) => Ok(()),
        Ok(Ok(_)) | Err(_) => Ok(()),
        Ok(Err(error)) => {
            warn!(error = %error, "event watcher gap; falling back to authoritative refresh");
            sleep(Duration::from_millis(250)).await;
            Ok(())
        }
    }
}

async fn ensure_operation_accepted(operation: &Operation, wait: Duration) -> AnyResult<()> {
    let outcome = operation.wait_timeout(wait).await?;
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
                "plan version {} is unsupported; use --rebuild-plan",
                plan.version
            ),
        ));
    }
    if plan.replicant_code != replicant_code
        || plan.vessel_code != vessel_code
        || plan.hub_location != config.hub
    {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "saved plan does not match the selected replicant, vessel, or hub; use --rebuild-plan",
        ));
    }
    if !config.targets.is_empty() && plan.targets != config.targets {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "saved plan targets differ from the command line; use --rebuild-plan",
        ));
    }
    if (plan.max_hop_ly - config.max_hop_ly).abs() > f64::EPSILON {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "saved plan maximum hop differs from --max-hop; use --rebuild-plan",
        ));
    }
    Ok(())
}

fn load_plan(path: &Path) -> AnyResult<MissionPlan> {
    Ok(serde_json::from_reader(File::open(path)?)?)
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
    Ok(())
}

fn print_plan(plan: &MissionPlan) {
    println!("FTL relay expansion plan");
    println!("  Mission: {}", plan.mission_id);
    println!("  Start/hub: {}", plan.hub_location);
    println!("  Replicant: {}", plan.replicant_code);
    println!("  Vessel: {}", plan.vessel_code);
    println!("  Targets: {}", plan.targets.join(", "));
    println!("  Maximum hop: {:.3} ly", plan.max_hop_ly);
    println!(
        "  Network sites after start: {}",
        plan.network.nodes.len().saturating_sub(1)
    );
    println!("  New placements: {}", plan.network.new_relay_systems.len());
    println!(
        "  Existing inactive activation stops: {}",
        plan.network.activation_systems.len()
    );
    println!(
        "  Reusable inactive relays at hub: {}",
        plan.hub_stock_relays.len()
    );
    println!("  Relays to print: {}", plan.print_jobs.len());
    println!(
        "  Total tree distance: {:.4} ly",
        plan.network.total_edge_distance_ly
    );
    println!(
        "  Deployment trip: {} hops, {:.4} routed ly including return ({})",
        plan.network.execution_hops,
        plan.network.execution_distance_ly,
        if plan.network.execution_order_optimal {
            "proven optimal"
        } else {
            "precedence-safe heuristic"
        }
    );
    println!();
    println!("Execution order:");
    for (index, stop) in plan.stops.iter().enumerate() {
        println!(
            "  {:>2}. {:<18} {:<24} {:?} relay={} parent={}{}",
            index + 1,
            stop.system,
            stop.location,
            stop.action,
            stop.relay_code.as_deref().unwrap_or("pending print"),
            stop.parent_system,
            if stop.completed { " [complete]" } else { "" }
        );
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
            println!(
                "  {factory_code}: quantity={} -> {systems} ({printed} printed){}",
                jobs.len(),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn print_job(system: &str, factory: &str, batch_tag: Option<&str>) -> PrintJob {
        PrintJob {
            system: system.to_owned(),
            factory_code: factory.to_owned(),
            mission_tag: "relay-m:test".to_owned(),
            site_tag: relay_site_tag(system),
            batch_tag: batch_tag.map(str::to_owned),
            submission_started: false,
            operation_id: None,
            submitted: false,
            relay_code: None,
        }
    }

    #[test]
    fn generated_relay_tags_fit_the_api_limit() {
        let mission = relay_mission_tag("scepturum-12345678-1234-1234-1234-123456789abc");
        let direct_site = relay_site_tag("XHAKKWUKKXHU");
        let shortened_site =
            relay_site_tag("A-SYSTEM-DESIGNATION-THAT-IS-LONGER-THAN-THE-TAG-LIMIT");
        let batch = relay_batch_tag(&mission, "6523AC61");

        for tag in [&mission, &direct_site, &shortened_site, &batch] {
            assert!(tag.chars().count() <= MAX_DEVICE_TAG_CHARS, "{tag}");
        }
        assert_eq!(direct_site, "relay-s:XHAKKWUKKXHU");
        assert_ne!(
            shortened_site,
            relay_site_tag("A-SYSTEM-DESIGNATION-THAT-IS-LONGER-THAN-THE-TAG-LIMIU")
        );
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
