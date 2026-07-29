//! Resumable, intentionally simple survey-route automation.
//!
//! This example:
//!
//! 1. synchronizes managed state and star knowledge;
//! 2. selects one idle AMI survey controller and the configured survey drones;
//! 3. adopts the drones, configures `survey_system`, and stows the fleet in
//!    the racing vessel;
//! 4. pre-plans a deterministic nearest-neighbor route around a centre star;
//! 5. travels one system at a time;
//! 6. runs the replicant's instant `POST /v1/replicants/{code}/scan`;
//! 7. launches the survey controller and waits for either a terminal
//!    `ami.survey.digest` or `directive.completed`;
//! 8. on startup, rechecks the current system scan and planet/moon survey completeness;
//! 9. recalls and restows the fleet before the next hop.
//!
//! The plan is written atomically after every phase. Stop the process at any
//! point and rerun it with the same plan path to resume.
//!
//! # Safety
//!
//! By default this example only synchronizes and creates the route plan. Set
//! `RS_EXPLORE_EXECUTE=1` to permit mutations.
//!
//! # Environment
//!
//! Required:
//!
//! - `RS_API_TOKEN`
//!
//! Defaults matching the requested fleet:
//!
//! - `RS_EXPLORE_REPLICANT=B6BA399E`
//! - `RS_EXPLORE_VESSEL=FD5EA802`
//! - `RS_EXPLORE_CENTER=SCEPTURUM`
//! - `RS_EXPLORE_RADIUS_LY=10`
//! - `RS_EXPLORE_SYSTEM_LIMIT=80`
//! - `RS_EXPLORE_STAR_DETAIL_CONCURRENCY=8`
//! - `RS_EXPLORE_PLAN=explore-survey-route.json`
//! - `RS_EXPLORE_LOG=logs/explore-survey-route.log`
//! - `REPLICANT_DB=replicant-client.sqlite`
//!
//! Optional fleet overrides:
//!
//! - `RS_EXPLORE_CONTROLLER=<device code>`
//! - `RS_EXPLORE_DRONES=<code>,<code>,<code>,<code>`
//!
//! Other controls:
//!
//! - `RS_EXPLORE_EXECUTE=1` — permit gameplay mutations.
//! - `RS_EXPLORE_REBUILD_PLAN=1` — replace an existing route plan.
//! - `RS_EXPLORE_INCLUDE_EXPLORED=1` — include already explored systems.
//! - `RS_EXPLORE_TRAVEL_TIMEOUT_SECS=21600`
//! - `RS_EXPLORE_SURVEY_TIMEOUT_SECS=21600`
//! - `RUST_LOG=replicant_client=info,replicant_client::explore=debug`
//!
//! Example:
//!
//! ```text
//! RS_EXPLORE_EXECUTE=1 \
//! cargo run --example explore_survey_route
//! ```
//!
//! A nearest-neighbor seed followed by bounded 2-opt improvement is used rather
//! than Dijkstra. Dijkstra solves the shortest path between two graph nodes;
//! this problem is choosing the order in which to visit many stars. The server's
//! travel endpoint still chooses the actual route for each individual hop.

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error as StdError,
    fs::{self, File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use futures::{StreamExt, stream};
use replicant_client::{
    Client, Device, DeviceType, Error as ClientError, Event, Operation,
    OperationStatus, Realm, SecretString, StartupPolicy, SurveyDirective,
    domain::GalacticPosition,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{
    EnvFilter,
    fmt::{MakeWriter, format::FmtSpan, time::SystemTime as TraceSystemTime},
};

type AnyError = Box<dyn StdError + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> AnyError {
    io::Error::new(kind, message.into()).into()
}

async fn refresh_device_snapshot(client: &Client, code: &str) -> AnyResult<Device> {
    let handle = client.devices().get(code).await?;
    Ok(handle.snapshot().await?)
}

async fn refresh_assigned_device_snapshots(
    client: &Client,
    replicant_code: &str,
) -> Result<BTreeMap<String, Device>, ClientError> {
    let handles = client
        .devices()
        .refresh_many()
        .assigned_to(replicant_code)
        .page_size(50)
        .collect()
        .await?;
    let mut devices = BTreeMap::new();
    for handle in handles {
        let device = handle.snapshot().await?;
        devices.insert(handle.id().as_str().to_owned(), device);
    }
    Ok(devices)
}

fn device_type_name(device: &Device) -> Option<&str> {
    device.device_type.as_ref().map(|value| value.as_str())
}

fn device_status_name(device: &Device) -> Option<&str> {
    device.status.as_ref().map(|value| value.as_str())
}

fn device_location(device: &Device) -> Option<&str> {
    device.location.as_ref().map(|value| value.id.as_str())
}

fn device_replicant(device: &Device) -> Option<&str> {
    device
        .relationships
        .assigned_replicant
        .as_ref()
        .map(|value| value.id.as_str())
}

fn device_stowed_in(device: &Device) -> Option<&str> {
    device
        .relationships
        .stowed_in
        .as_ref()
        .map(|value| value.id.as_str())
}

fn device_controller(device: &Device) -> Option<&str> {
    device
        .relationships
        .controller
        .as_ref()
        .map(|value| value.id.as_str())
}

fn device_has_command(device: &Device, command: &str) -> bool {
    device
        .available_commands
        .iter()
        .any(|value| value.as_str() == command)
}

fn active_directive_status(device: &Device) -> Option<&str> {
    device
        .active_directive
        .as_ref()
        .and_then(|directive| directive.status.as_deref())
}

fn required_device<'a>(
    devices: &'a BTreeMap<String, Device>,
    code: &str,
    context: &str,
) -> AnyResult<&'a Device> {
    devices.get(code).ok_or_else(|| {
        app_error(
            io::ErrorKind::NotFound,
            format!("managed device refresh omitted {context} {code}"),
        )
    })
}


const PLAN_VERSION: u32 = 2;
const LEGACY_PLAN_VERSION: u32 = 1;
const DRONE_COUNT: usize = 3;
const DEFAULT_FILTER: &str = concat!(
    "replicant_client=info,",
    "replicant_client::explore=debug,",
    "replicant_client::raw::http=info,",
    "replicant_client::raw::rate_limit=info,",
    "replicant_client::events=info,",
    "replicant_client::ops=info"
);

#[derive(Clone)]
struct TeeMakeWriter {
    file: Arc<Mutex<File>>,
}

struct TeeWriter {
    file: Arc<Mutex<File>>,
}

impl<'a> MakeWriter<'a> for TeeMakeWriter {
    type Writer = TeeWriter;

    fn make_writer(&'a self) -> Self::Writer {
        TeeWriter {
            file: Arc::clone(&self.file),
        }
    }
}

impl Write for TeeWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        io::stderr().lock().write_all(buffer)?;
        self.file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .write_all(buffer)?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        io::stderr().lock().flush()?;
        self.file
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .flush()
    }
}

#[derive(Debug)]
struct Config {
    token: SecretString,
    database: PathBuf,
    replicant: String,
    vessel: String,
    center: String,
    radius_ly: f64,
    system_limit: usize,
    star_detail_concurrency: usize,
    plan_path: PathBuf,
    log_path: PathBuf,
    controller_override: Option<String>,
    drone_overrides: Option<Vec<String>>,
    execute: bool,
    rebuild_plan: bool,
    include_explored: bool,
    travel_timeout: Duration,
    survey_timeout: Duration,
}

impl Config {
    fn from_env() -> AnyResult<Self> {
        let token = SecretString::from(
            env::var("RS_API_TOKEN").map_err(|_| {
                app_error(io::ErrorKind::InvalidInput, "RS_API_TOKEN is required")
            })?,
        );

        let drone_overrides = env::var("RS_EXPLORE_DRONES").ok().map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        });

        if let Some(drones) = &drone_overrides {
            if drones.len() != DRONE_COUNT {
                return Err(app_error(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "RS_EXPLORE_DRONES must contain exactly {DRONE_COUNT} comma-separated codes"
                    ),
                ));
            }
            if drones.iter().collect::<BTreeSet<_>>().len() != DRONE_COUNT {
                return Err(app_error(
                    io::ErrorKind::InvalidInput,
                    format!("RS_EXPLORE_DRONES must contain {DRONE_COUNT} distinct device codes"),
                ));
            }
        }

        Ok(Self {
            token,
            database: env::var_os("REPLICANT_DB")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("replicant-client.sqlite")),
            replicant: env_string("RS_EXPLORE_REPLICANT", "B6BA399E"),
            vessel: env_string("RS_EXPLORE_VESSEL", "FD5EA802"),
            center: env_string("RS_EXPLORE_CENTER", "SCEPTURUM").to_ascii_uppercase(),
            radius_ly: env_f64("RS_EXPLORE_RADIUS_LY", 30.0)?,
            system_limit: env_usize("RS_EXPLORE_SYSTEM_LIMIT", 80)?.max(1),
            star_detail_concurrency: env_usize("RS_EXPLORE_STAR_DETAIL_CONCURRENCY", 8)?
                .clamp(1, 16),
            plan_path: env::var_os("RS_EXPLORE_PLAN")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("explore-survey-route.json")),
            log_path: env::var_os("RS_EXPLORE_LOG")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("logs/explore-survey-route.log")),
            controller_override: env::var("RS_EXPLORE_CONTROLLER").ok(),
            drone_overrides,
            execute: env_bool("RS_EXPLORE_EXECUTE", false)?,
            rebuild_plan: env_bool("RS_EXPLORE_REBUILD_PLAN", false)?,
            include_explored: env_bool("RS_EXPLORE_INCLUDE_EXPLORED", false)?,
            travel_timeout: Duration::from_secs(env_u64(
                "RS_EXPLORE_TRAVEL_TIMEOUT_SECS",
                6 * 60 * 60,
            )?),
            survey_timeout: Duration::from_secs(env_u64(
                "RS_EXPLORE_SURVEY_TIMEOUT_SECS",
                6 * 60 * 60,
            )?),
        })
    }
}

fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_bool(name: &str, default: bool) -> AnyResult<bool> {
    match env::var(name) {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("{name} must be 1/0, true/false, yes/no, or on/off"),
            )),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_usize(name: &str, default: usize) -> AnyResult<usize> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_u64(name: &str, default: u64) -> AnyResult<u64> {
    match env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_f64(name: &str, default: f64) -> AnyResult<f64> {
    match env::var(name) {
        Ok(value) => {
            let parsed: f64 = value.parse()?;
            if !parsed.is_finite() || parsed <= 0.0 {
                return Err(app_error(
                    io::ErrorKind::InvalidInput,
                    format!("{name} must be a positive finite number"),
                ));
            }
            Ok(parsed)
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn install_tracing(log_path: &Path) -> AnyResult<()> {
    if let Some(parent) = log_path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }

    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)?;
    let writer = TeeMakeWriter {
        file: Arc::new(Mutex::new(file)),
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(writer)
        .with_timer(TraceSystemTime)
        .with_target(true)
        .with_thread_ids(true)
        .with_ansi(false)
        .with_span_events(FmtSpan::CLOSE)
        .try_init()
        .map_err(|error| app_error(io::ErrorKind::Other, error.to_string()))?;

    Ok(())
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RunPhase {
    PreparingFleet,
    Ready,
    Traveling,
    SystemScanning,
    Surveying,
    Restowing,
    Complete,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RouteStop {
    star: String,
    entry_point: Option<String>,
    distance_from_center_ly: f64,
    leg_distance_ly: f64,
    survey_required: bool,
    system_scan_done: bool,
    survey_done: bool,
}

impl RouteStop {
    fn can_advance_without_restow(&self) -> bool {
        !self.survey_required && self.system_scan_done && self.survey_done
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct RoutePlan {
    version: u32,
    created_unix_seconds: u64,
    replicant: String,
    vessel: String,
    center: String,
    radius_ly: f64,
    system_limit: usize,
    include_explored: bool,
    controller: Option<String>,
    drones: Vec<String>,
    fleet_prepared: bool,
    route: Vec<RouteStop>,
    next_index: usize,
    phase: RunPhase,
}

impl RoutePlan {
    fn migrate(&mut self) -> AnyResult<bool> {
        match self.version {
            PLAN_VERSION => Ok(false),
            LEGACY_PLAN_VERSION => {
                // Version 1 stored a per-stop `completed` flag. Version 2
                // uses `next_index` as the authoritative finalized-stop
                // boundary, which preserves the Restowing safety phase.
                // Serde safely ignores the legacy JSON field.
                self.version = PLAN_VERSION;
                Ok(true)
            }
            version => Err(app_error(
                io::ErrorKind::InvalidData,
                format!("unsupported route plan version {version}; expected {PLAN_VERSION}"),
            )),
        }
    }

    fn validate(&self, config: &Config) -> AnyResult<()> {
        if self.version != PLAN_VERSION {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported route plan version {}; expected {PLAN_VERSION}",
                    self.version
                ),
            ));
        }
        if self.replicant != config.replicant
            || self.vessel != config.vessel
            || self.center != config.center
        {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                "existing route plan targets a different replicant, vessel, or centre; set RS_EXPLORE_REBUILD_PLAN=1 to replace it",
            ));
        }
        if self.next_index > self.route.len() {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                "route plan next_index exceeds route length",
            ));
        }
        Ok(())
    }

    fn stop_is_finalized(&self, index: usize) -> bool {
        index < self.next_index
    }

    fn normalize_progress(&mut self) -> bool {
        let mut changed = false;

        // Only automatically advance stops that never require fleet launch or
        // restow. A surveyed stop with `survey_required=true` remains current
        // until the Restowing phase safely returns the fleet.
        while self.next_index < self.route.len()
            && self.route[self.next_index].can_advance_without_restow()
        {
            self.next_index += 1;
            changed = true;
        }

        if self.next_index == self.route.len() && self.phase != RunPhase::Complete {
            self.phase = RunPhase::Complete;
            changed = true;
        } else if self.next_index < self.route.len() && self.phase == RunPhase::Complete {
            self.phase = RunPhase::Ready;
            changed = true;
        }

        changed
    }
}


#[derive(Clone)]
struct CandidateStar {
    star: String,
    entry_point: Option<String>,
    position: GalacticPosition,
    distance_from_center_ly: f64,
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let config = Config::from_env()?;
    install_tracing(&config.log_path)?;

    info!(
        target: "replicant_client::explore",
        event = "explore.started",
        replicant = %config.replicant,
        vessel = %config.vessel,
        center = %config.center,
        radius_ly = config.radius_ly,
        system_limit = config.system_limit,
        star_detail_concurrency = config.star_detail_concurrency,
        plan = %config.plan_path.display(),
        log = %config.log_path.display(),
        execute = config.execute,
        "starting survey-route automation"
    );

    let client = Client::builder()
        .authentication_token(config.token.clone())
        .sqlite(&config.database)
        .startup_policy(StartupPolicy::Essential)
        .start()
        .await?;

    let result = run(&client, &config).await;
    let close_result = client.close().await;

    if let Err(error) = &result {
        error!(
            target: "replicant_client::explore",
            event = "explore.failed",
            error = %error,
            "survey-route automation failed; rerun to resume from the saved plan"
        );
    }
    close_result?;
    result
}

async fn run(client: &Client, config: &Config) -> AnyResult<()> {
    let sync_started = Instant::now();
    let sync = client.sync().full().await?;
    info!(
        target: "replicant_client::explore",
        event = "explore.full_sync_completed",
        readiness = ?sync.readiness,
        elapsed_ms = sync_started.elapsed().as_millis() as u64,
        "full synchronization completed"
    );

    let mut plan = load_or_create_plan(client, config).await?;
    if plan.next_index >= plan.route.len() || plan.phase == RunPhase::Complete {
        log_route(&plan);
        info!(
            target: "replicant_client::explore",
            event = "route.already_completed",
            plan = %config.plan_path.display(),
            "route plan is already complete"
        );
        return Ok(());
    }

    reconcile_current_system_scan_on_startup(client, config, &mut plan).await?;
    log_route(&plan);

    if !config.execute {
        warn!(
            target: "replicant_client::explore",
            event = "explore.plan_only",
            "route plan created/loaded; set RS_EXPLORE_EXECUTE=1 to permit mutations"
        );
        return Ok(());
    }

    if !plan.fleet_prepared {
        prepare_fleet(client, config, &mut plan).await?;
        save_plan(&config.plan_path, &plan)?;
    } else if plan.phase == RunPhase::Traveling {
        // A vessel can legitimately report no location during interstellar
        // travel. Verify only that the complete fleet is onboard; `travel_to`
        // owns the authoritative resume path from this phase.
        verify_fleet(client, config, &plan, FleetVerification::travel()).await?;
    } else {
        if phase_requires_stowed_fleet(plan.phase) {
            let controller = plan
                .controller
                .as_deref()
                .ok_or_else(|| app_error(io::ErrorKind::Other, "route plan has no survey controller"))?;
            ensure_replicant_owns_device(client, controller, &config.replicant).await?;
            for code in &plan.drones {
                ensure_replicant_owns_device(client, code, &config.replicant).await?;
            }
            stow_fleet(client, config, &plan).await?;
        }
        verify_fleet(client, config, &plan, FleetVerification::for_plan(&plan)).await?;
    }

    execute_route(client, config, &mut plan).await
}

#[derive(Clone, Debug)]
struct CurrentSystemSurveyCheck {
    complete: Option<bool>,
    planets_total: Option<i64>,
    planets_scanned: Option<i64>,
    moons_total: Option<i64>,
    moons_scanned: Option<i64>,
    moons_total_estimated: Option<bool>,
}

async fn reconcile_current_system_scan_on_startup(
    client: &Client,
    config: &Config,
    plan: &mut RoutePlan,
) -> AnyResult<()> {
    let started = Instant::now();
    let Some(current_star) = current_star(client, &config.replicant).await? else {
        warn!(
            target: "replicant_client::explore",
            event = "startup.current_system_check_skipped",
            replicant = %config.replicant,
            reason = "current_star_unknown",
            "could not determine the current star during startup reconciliation"
        );
        return Ok(());
    };

    let Some(route_index) = plan.route.iter().position(|stop| stop.star == current_star) else {
        info!(
            target: "replicant_client::explore",
            event = "startup.current_system_check_skipped",
            star = %current_star,
            reason = "current_star_not_in_route",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "current star is not part of the saved route"
        );
        return Ok(());
    };

    if plan.stop_is_finalized(route_index) {
        info!(
            target: "replicant_client::explore",
            event = "startup.current_system_check_skipped",
            star = %current_star,
            route_index,
            reason = "route_stop_complete",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "current route stop is already complete; skipping startup system recheck"
        );
        return Ok(());
    }

    let local_explored = client
        .galaxy()
        .replicant_star_knowledge(&config.replicant)
        .into_iter()
        .find(|knowledge| knowledge.star.id.as_str() == current_star.as_str())
        .and_then(|knowledge| knowledge.explored);

    info!(
        target: "replicant_client::explore",
        event = "startup.current_system_check_started",
        star = %current_star,
        route_index,
        next_index = plan.next_index,
        phase = ?plan.phase,
        saved_system_scan_done = plan.route[route_index].system_scan_done,
        saved_survey_done = plan.route[route_index].survey_done,
        local_explored,
        "refreshing current-star and planetary survey knowledge before route execution"
    );

    let refreshed_explored = match client
        .galaxy()
        .refresh_replicant_star(&config.replicant, &current_star)
        .await
    {
        Ok(knowledge) => knowledge.explored,
        Err(refresh_error) => {
            warn!(
                target: "replicant_client::explore",
                event = "startup.system_scan_refresh_failed",
                star = %current_star,
                route_index,
                next_index = plan.next_index,
                local_explored,
                error = %refresh_error,
                "targeted current-star refresh failed"
            );

            if local_explored == Some(true) {
                Some(true)
            } else if route_index == plan.next_index && !plan.route[route_index].system_scan_done {
                return Err(app_error(io::ErrorKind::Other, format!(
                    "unable to verify whether the current system {current_star} was scanned; refusing to risk a duplicate system_scan command: {refresh_error}"
                )));
            } else {
                local_explored
            }
        }
    };

    let survey_check = if refreshed_explored == Some(true) {
        Some(inspect_current_system_surveys(client, config, &current_star).await?)
    } else {
        None
    };

    let changed = apply_startup_current_system_completion(
        plan,
        &current_star,
        refreshed_explored == Some(true),
        survey_check.as_ref().and_then(|check| check.complete),
    );

    if changed {
        save_plan(&config.plan_path, plan)?;
    }

    if let Some(check) = &survey_check {
        info!(
            target: "replicant_client::explore",
            event = "startup.planetary_survey_check_completed",
            star = %current_star,
            complete = check.complete,
            planets_total = check.planets_total,
            planets_scanned = check.planets_scanned,
            moons_total = check.moons_total,
            moons_scanned = check.moons_scanned,
            moons_total_estimated = check.moons_total_estimated,
            "completed current-system survey-counter check"
        );
    }

    info!(
        target: "replicant_client::explore",
        event = "startup.current_system_check_completed",
        star = %current_star,
        route_index,
        next_index = plan.next_index,
        phase = ?plan.phase,
        local_explored,
        refreshed_explored,
        planetary_surveys_complete = survey_check.as_ref().and_then(|check| check.complete),
        plan_changed = changed,
        system_scan_done = plan.route[route_index].system_scan_done,
        survey_required = plan.route[route_index].survey_required,
        survey_done = plan.route[route_index].survey_done,
        finalized = plan.stop_is_finalized(route_index),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "completed current-system startup reconciliation"
    );

    Ok(())
}

async fn inspect_current_system_surveys(
    client: &Client,
    config: &Config,
    current_star: &str,
) -> AnyResult<CurrentSystemSurveyCheck> {
    let started = Instant::now();

    // The system response already contains the authoritative aggregate survey
    // progress. No planet/moon hydration or individual body requests are
    // needed to decide whether the system survey is complete.
    let root = client
        .locations()
        .get_for_replicant(current_star, &config.replicant)
        .await?;
    let progress = &root.survey_progress;

    let planets_complete = exact_count_complete(progress.planets_total, progress.planets_scanned);
    let moons_complete = match progress.moons_total_estimated {
        Some(false) => exact_count_complete(progress.moons_total, progress.moons_scanned),
        Some(true) => Some(false),
        None => None,
    };
    let complete = aggregate_survey_counts_complete(planets_complete, moons_complete);

    info!(
        target: "replicant_client::explore",
        event = "startup.planetary_survey_inspected",
        star = current_star,
        complete,
        planets_total = progress.planets_total,
        planets_scanned = progress.planets_scanned,
        planets_complete,
        moons_total = progress.moons_total,
        moons_scanned = progress.moons_scanned,
        moons_total_estimated = progress.moons_total_estimated,
        moons_complete,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "inspected current-system survey completeness from aggregate counters"
    );

    Ok(CurrentSystemSurveyCheck {
        complete,
        planets_total: progress.planets_total,
        planets_scanned: progress.planets_scanned,
        moons_total: progress.moons_total,
        moons_scanned: progress.moons_scanned,
        moons_total_estimated: progress.moons_total_estimated,
    })
}

fn exact_count_complete(total: Option<i64>, scanned: Option<i64>) -> Option<bool> {
    match (total, scanned) {
        (Some(total), Some(scanned)) => Some(scanned == total),
        _ => None,
    }
}

fn aggregate_survey_counts_complete(
    planets_complete: Option<bool>,
    moons_complete: Option<bool>,
) -> Option<bool> {
    match (planets_complete, moons_complete) {
        (Some(true), Some(true)) => Some(true),
        (Some(false), _) | (_, Some(false)) => Some(false),
        _ => None,
    }
}

fn apply_startup_current_system_completion(
    plan: &mut RoutePlan,
    current_star: &str,
    system_scan_complete: bool,
    planetary_surveys_complete: Option<bool>,
) -> bool {
    let Some(route_index) = plan.route.iter().position(|stop| stop.star == current_star) else {
        return false;
    };

    let mut changed = false;
    {
        let stop = &mut plan.route[route_index];

        if system_scan_complete && !stop.system_scan_done {
            stop.system_scan_done = true;
            changed = true;
        }

        match planetary_surveys_complete {
            Some(true) => {
                if !stop.survey_done {
                    stop.survey_done = true;
                    changed = true;
                }
            }
            Some(false) => {
                if !stop.survey_required {
                    stop.survey_required = true;
                    changed = true;
                }
                if stop.survey_done {
                    stop.survey_done = false;
                    changed = true;
                }
            }
            None => {}
        }
    }

    if planetary_surveys_complete == Some(false) && route_index < plan.next_index {
        plan.next_index = route_index;
        changed = true;
    }

    if route_index == plan.next_index {
        if plan.route[route_index].can_advance_without_restow() {
            changed |= plan.normalize_progress();
        } else if plan.fleet_prepared
            && !matches!(plan.phase, RunPhase::Restowing | RunPhase::Complete)
        {
            let desired_phase = if !plan.route[route_index].system_scan_done {
                RunPhase::SystemScanning
            } else if plan.route[route_index].survey_done {
                RunPhase::Restowing
            } else {
                RunPhase::Surveying
            };
            if plan.phase != desired_phase {
                plan.phase = desired_phase;
                changed = true;
            }
        } else if !plan.fleet_prepared && plan.phase != RunPhase::PreparingFleet {
            plan.phase = RunPhase::PreparingFleet;
            changed = true;
        }
    }

    changed
}

async fn load_or_create_plan(client: &Client, config: &Config) -> AnyResult<RoutePlan> {
    if config.rebuild_plan && config.plan_path.exists() {
        info!(
            target: "replicant_client::explore",
            event = "route.plan_removed",
            path = %config.plan_path.display(),
            "removing existing route plan"
        );
        fs::remove_file(&config.plan_path)?;
    }

    if config.plan_path.exists() {
        let bytes = fs::read(&config.plan_path)?;
        let mut plan: RoutePlan = serde_json::from_slice(&bytes)?;
        let migrated = plan.migrate()?;
        plan.validate(config)?;
        let normalized = plan.normalize_progress();
        if migrated || normalized {
            save_plan(&config.plan_path, &plan)?;
            info!(
                target: "replicant_client::explore",
                event = "route.plan_normalized",
                migrated,
                next_index = plan.next_index,
                phase = ?plan.phase,
                "migrated and normalized route progress"
            );
        }
        info!(
            target: "replicant_client::explore",
            event = "route.plan_loaded",
            stops = plan.route.len(),
            next_index = plan.next_index,
            phase = ?plan.phase,
            "loaded resumable route plan"
        );
        return Ok(plan);
    }

    create_plan(client, config).await
}

async fn create_plan(client: &Client, config: &Config) -> AnyResult<RoutePlan> {
    let planning_started = Instant::now();

    let mut catalogue = client.galaxy().catalogue();
    if catalogue.is_empty() {
        info!(
            target: "replicant_client::explore",
            event = "route.catalogue_refresh_required",
            "no durable star catalogue is available; refreshing it"
        );
        client.galaxy().refresh_catalogue().await?;
        catalogue = client.galaxy().catalogue();
    }
    let center = catalogue
        .iter()
        .find(|star| star.key.id.as_str() == config.center)
        .cloned()
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!(
                    "centre star `{}` is absent from the catalogue",
                    config.center
                ),
            )
        })?;
    let center_position = center.position.ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            format!("centre star `{}` has no catalogue position", config.center),
        )
    })?;

    // Filter the complete catalogue locally first.  The earlier implementation
    // downloaded every page of the replicant's ~14k-star census merely to learn
    // whether a few nearby candidates were explored.  Resolve only this bounded
    // shortlist through the single-star detail endpoint instead.
    let mut candidates = catalogue
        .into_iter()
        .filter_map(|star| {
            let position = star.position?;
            let star_code = star.key.id.as_str().to_owned();
            if star_code == config.center {
                return None;
            }
            let distance = position_distance(center_position, position);
            if distance > config.radius_ly {
                return None;
            }
            Some(CandidateStar {
                star: star_code,
                entry_point: star.entry_point.map(|entry| entry.id.as_str().to_owned()),
                position,
                distance_from_center_ly: distance,
            })
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| left.star.cmp(&right.star));

    let knowledge_started = Instant::now();
    let mut explored_by_star = client
        .galaxy()
        .replicant_star_knowledge(&config.replicant)
        .into_iter()
        .filter_map(|knowledge| {
            knowledge
                .explored
                .map(|explored| (knowledge.star.id.as_str().to_owned(), explored))
        })
        .collect::<BTreeMap<_, _>>();

    let mut required_stars = candidates
        .iter()
        .map(|candidate| candidate.star.clone())
        .collect::<Vec<_>>();
    required_stars.push(config.center.clone());
    required_stars.sort();
    required_stars.dedup();

    let missing = required_stars
        .iter()
        .filter(|star| !explored_by_star.contains_key(*star))
        .cloned()
        .collect::<Vec<_>>();
    let local_hits = required_stars.len() - missing.len();

    let refreshed = stream::iter(missing.into_iter().map(|star| {
        let galaxy = client.galaxy();
        let replicant = config.replicant.clone();
        async move {
            let started = Instant::now();
            let result = galaxy.refresh_replicant_star(&replicant, &star).await;
            (star, started.elapsed(), result)
        }
    }))
    .buffer_unordered(config.star_detail_concurrency)
    .collect::<Vec<_>>()
    .await;

    let mut refreshed_count = 0_usize;
    for (star, elapsed, result) in refreshed {
        let knowledge = result?;
        explored_by_star.insert(star.clone(), knowledge.explored.unwrap_or(false));
        refreshed_count += 1;
        debug!(
            target: "replicant_client::explore",
            event = "route.star_detail_resolved",
            star,
            explored = knowledge.explored,
            elapsed_ms = elapsed.as_millis() as u64,
            "resolved targeted star knowledge"
        );
    }

    info!(
        target: "replicant_client::explore",
        event = "route.star_knowledge_resolved",
        candidates = required_stars.len(),
        local_hits,
        refreshed = refreshed_count,
        concurrency = config.star_detail_concurrency,
        elapsed_ms = knowledge_started.elapsed().as_millis() as u64,
        "resolved only the star knowledge needed for route planning"
    );

    candidates.retain(|candidate| {
        config.include_explored
            || !explored_by_star
                .get(&candidate.star)
                .copied()
                .unwrap_or(false)
    });

    // Build a deterministic nearest-neighbor seed, then run a bounded 2-opt
    // improvement pass.  This remains cheap for the small route limit while
    // avoiding obvious crossings and backtracking in the initial greedy tour.
    let mut remaining = candidates;
    let mut ordered = Vec::new();
    let mut current = center_position;
    while !remaining.is_empty() && ordered.len() + 1 < config.system_limit {
        let (index, _) = remaining
            .iter()
            .enumerate()
            .map(|(index, candidate)| (index, position_distance(current, candidate.position)))
            .min_by(
                |(left_index, left_distance), (right_index, right_distance)| {
                    left_distance
                        .partial_cmp(right_distance)
                        .unwrap_or(Ordering::Equal)
                        .then_with(|| {
                            remaining[*left_index]
                                .star
                                .cmp(&remaining[*right_index].star)
                        })
                },
            )
            .expect("candidate list is non-empty");
        let candidate = remaining.remove(index);
        current = candidate.position;
        ordered.push(candidate);
    }

    let nearest_neighbor_distance = candidate_route_distance(center_position, &ordered);
    let two_opt_swaps = improve_candidate_route_2opt(center_position, &mut ordered, 8);
    let optimized_distance = candidate_route_distance(center_position, &ordered);

    let center_already_explored = explored_by_star
        .get(&config.center)
        .copied()
        .unwrap_or(false);
    let center_survey_required = config.include_explored || !center_already_explored;
    let mut route = vec![RouteStop {
        star: config.center.clone(),
        entry_point: center
            .entry_point
            .as_ref()
            .map(|entry| entry.id.as_str().to_owned()),
        distance_from_center_ly: 0.0,
        leg_distance_ly: 0.0,
        survey_required: center_survey_required,
        system_scan_done: center_already_explored,
        survey_done: !center_survey_required,
    }];

    let mut previous = center_position;
    for candidate in ordered {
        let already_explored = explored_by_star
            .get(&candidate.star)
            .copied()
            .unwrap_or(false);
        let leg_distance = position_distance(previous, candidate.position);
        previous = candidate.position;
        let survey_required = config.include_explored || !already_explored;
        route.push(RouteStop {
            star: candidate.star,
            entry_point: candidate.entry_point,
            distance_from_center_ly: candidate.distance_from_center_ly,
            leg_distance_ly: leg_distance,
            survey_required,
            system_scan_done: already_explored,
            survey_done: !survey_required,
        });
    }

    let next_index = route
        .iter()
        .position(|stop| !stop.can_advance_without_restow())
        .unwrap_or(route.len());
    let phase = if next_index == route.len() {
        RunPhase::Complete
    } else {
        RunPhase::PreparingFleet
    };

    let plan = RoutePlan {
        version: PLAN_VERSION,
        created_unix_seconds: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        replicant: config.replicant.clone(),
        vessel: config.vessel.clone(),
        center: config.center.clone(),
        radius_ly: config.radius_ly,
        system_limit: config.system_limit,
        include_explored: config.include_explored,
        controller: None,
        drones: Vec::new(),
        fleet_prepared: false,
        route,
        next_index,
        phase,
    };
    save_plan(&config.plan_path, &plan)?;

    info!(
        target: "replicant_client::explore",
        event = "route.plan_created",
        stops = plan.route.len(),
        explored_known = explored_by_star.values().filter(|value| **value).count(),
        nearest_neighbor_distance_ly = nearest_neighbor_distance,
        optimized_distance_ly = optimized_distance,
        distance_saved_ly = nearest_neighbor_distance - optimized_distance,
        two_opt_swaps,
        elapsed_ms = planning_started.elapsed().as_millis() as u64,
        "created and saved optimized nearest-neighbor route"
    );
    Ok(plan)
}

fn log_route(plan: &RoutePlan) {
    let total_distance: f64 = plan.route.iter().map(|stop| stop.leg_distance_ly).sum();
    info!(
        target: "replicant_client::explore",
        event = "route.summary",
        stops = plan.route.len(),
        next_index = plan.next_index,
        phase = ?plan.phase,
        total_distance_ly = total_distance,
        "route summary"
    );
    for (index, stop) in plan.route.iter().enumerate() {
        info!(
            target: "replicant_client::explore",
            event = "route.stop",
            index,
            star = %stop.star,
            entry_point = stop.entry_point.as_deref().unwrap_or(""),
            distance_from_center_ly = stop.distance_from_center_ly,
            leg_distance_ly = stop.leg_distance_ly,
            survey_required = stop.survey_required,
            finalized = plan.stop_is_finalized(index),
            "planned route stop"
        );
    }
}

async fn prepare_fleet(client: &Client, config: &Config, plan: &mut RoutePlan) -> AnyResult<()> {
    let started = Instant::now();
    plan.phase = RunPhase::PreparingFleet;
    save_plan(&config.plan_path, plan)?;

    let replicant = client.replicants().get_owned(&config.replicant).await?;
    let replicant_snapshot = replicant.snapshot().await?;
    let hosted_vessel = replicant_snapshot
        .hosted_device
        .as_ref()
        .map(|device| device.id.as_str());
    if hosted_vessel != Some(config.vessel.as_str()) {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!(
                "replicant {} is hosted by {:?}, not vessel {}",
                config.replicant, hosted_vessel, config.vessel
            ),
        ));
    }

    let vessel_snapshot = refresh_device_snapshot(client, &config.vessel).await?;
    ensure_device_type(&vessel_snapshot, &config.vessel, "racing_vessel")?;
    ensure_device_replicant(&vessel_snapshot, &config.vessel, &config.replicant)?;
    let location_id = device_location(&vessel_snapshot)
        .ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidData,
                "racing vessel has no current location",
            )
        })?
        .to_owned();

    let account_owned_devices = client
        .devices()
        .find()
        .in_realm(Realm::Live)
        .owned()
        .collect()
        .await?
        .into_iter()
        .map(|device| device.id().as_str().to_owned())
        .collect::<BTreeSet<_>>();

    if plan.controller.is_none() {
        let controller_code = if let Some(code) = &config.controller_override {
            ensure_account_owned(&account_owned_devices, code)?;
            let device = refresh_device_snapshot(client, code).await?;
            validate_controller_candidate(&device, code, &location_id, &config.vessel)?;
            code.clone()
        } else {
            let controllers = client
                .devices()
                .controllers(DeviceType::SurveyController)
                .owned()
                .idle()
                .at(&location_id)
                .without_adopted_devices()
                .collect()
                .await?;

            let mut eligible = Vec::new();
            for controller in controllers {
                let code = controller.id().as_str().to_owned();
                let device = controller.snapshot().await?;
                match validate_controller_candidate(&device, &code, &location_id, &config.vessel) {
                    Ok(()) => eligible.push((
                        device_replicant(&device) == Some(config.replicant.as_str()),
                        code,
                    )),
                    Err(reason) => {
                        debug!(
                            target: "replicant_client::explore",
                            event = "fleet.controller_rejected",
                            device = code,
                            reason = %reason,
                            "survey controller is not eligible for the target vessel"
                        );
                    }
                }
            }
            eligible.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
            eligible
                .into_iter()
                .next()
                .map(|(_, code)| code)
                .ok_or_else(|| {
                    app_error(
                        io::ErrorKind::NotFound,
                        format!(
                            "no account-owned idle survey controller is available at {} or already stowed in vessel {}",
                            location_id, config.vessel
                        ),
                    )
                })?
        };
        plan.controller = Some(controller_code);
        save_plan(&config.plan_path, plan)?;
    }

    let controller_code = plan
        .controller
        .clone()
        .ok_or_else(|| app_error(io::ErrorKind::Other, "route plan has no selected controller"))?;

    if plan.drones.is_empty() {
        let drones = if let Some(codes) = &config.drone_overrides {
            if codes.iter().any(|code| code == &controller_code) {
                return Err(app_error(
                    io::ErrorKind::InvalidInput,
                    "the survey controller cannot also be selected as a survey drone",
                ));
            }
            for code in codes {
                ensure_account_owned(&account_owned_devices, code)?;
                let device = refresh_device_snapshot(client, code).await?;
                validate_drone_candidate(
                    &device,
                    code,
                    &controller_code,
                    &location_id,
                    &config.vessel,
                )?;
            }
            codes.clone()
        } else {
            let available = client
                .devices()
                .find()
                .in_realm(Realm::Live)
                .owned()
                .of_type(DeviceType::from("survey_drone"))
                .idle()
                .at(&location_id)
                .without_controller()
                .collect()
                .await?;

            let mut eligible = Vec::new();
            for drone in available {
                let code = drone.id().as_str().to_owned();
                let device = drone.snapshot().await?;
                match validate_drone_candidate(
                    &device,
                    &code,
                    &controller_code,
                    &location_id,
                    &config.vessel,
                ) {
                    Ok(()) => eligible.push((
                        device_replicant(&device) == Some(config.replicant.as_str()),
                        code,
                    )),
                    Err(reason) => {
                        debug!(
                            target: "replicant_client::explore",
                            event = "fleet.drone_rejected",
                            device = code,
                            reason = %reason,
                            "survey drone is not eligible for the target vessel"
                        );
                    }
                }
            }
            eligible.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.cmp(&right.1)));
            let selected = eligible
                .into_iter()
                .take(DRONE_COUNT)
                .map(|(_, code)| code)
                .collect::<Vec<_>>();
            if selected.len() < DRONE_COUNT {
                return Err(app_error(
                    io::ErrorKind::NotFound,
                    format!(
                        "only {} eligible account-owned survey drones are available at {} or already stowed in vessel {}; need {DRONE_COUNT}",
                        selected.len(), location_id, config.vessel
                    ),
                ));
            }
            selected
        };
        plan.drones = drones;
        save_plan(&config.plan_path, plan)?;
    }

    ensure_replicant_owns_device(client, &controller_code, &config.replicant).await?;
    for code in &plan.drones {
        ensure_replicant_owns_device(client, code, &config.replicant).await?;
    }

    verify_fleet(client, config, plan, FleetVerification::for_plan(plan)).await?;

    let controller_handle = client.devices().get(&controller_code).await?;
    let controller = controller_handle.as_survey_controller()?;

    let mut needs_adoption = Vec::new();
    for code in &plan.drones {
        let device = refresh_device_snapshot(client, code).await?;
        match device_controller(&device) {
            Some(actual) if actual == controller_code.as_str() => {}
            Some(actual) => {
                return Err(app_error(
                    io::ErrorKind::InvalidInput,
                    format!("drone {code} is already controlled by {actual}"),
                ));
            }
            None => needs_adoption.push(code.clone()),
        }
    }

    if !needs_adoption.is_empty() {
        info!(
            target: "replicant_client::explore",
            event = "fleet.adoption_started",
            controller = controller_code,
            drones = ?needs_adoption,
            "adopting survey drones"
        );
        let operation = controller.adopt(needs_adoption.clone()).await?;
        wait_immediate_operation("adopt survey drones", &operation).await?;
        for code in &needs_adoption {
            let refreshed = refresh_device_snapshot(client, code).await?;
            if device_controller(&refreshed) != Some(controller_code.as_str()) {
                return Err(app_error(
                    io::ErrorKind::Other,
                    format!(
                        "drone {code} did not report controller {controller_code} after adoption"
                    ),
                ));
            }
        }
    }

    let controller_snapshot = refresh_device_snapshot(client, &controller_code).await?;
    if !has_survey_system_directive(&controller_snapshot) {
        info!(
            target: "replicant_client::explore",
            event = "fleet.directive_started",
            controller = controller_code,
            "configuring survey_system directive"
        );
        let operation = controller
            .set_directive(SurveyDirective::SurveySystem {
                planets: "all".to_owned(),
                moons: "all".to_owned(),
                recall: true,
            })
            .await?;
        wait_immediate_operation("configure survey_system", &operation).await?;
    }

    stow_fleet(client, config, plan).await?;

    plan.fleet_prepared = true;
    plan.phase = RunPhase::Ready;
    save_plan(&config.plan_path, plan)?;
    info!(
        target: "replicant_client::explore",
        event = "fleet.prepared",
        controller = controller_code,
        drones = ?plan.drones,
        location = location_id,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "survey fleet is adopted, configured, and stowed"
    );
    Ok(())
}

#[derive(Clone, Copy, Debug)]
struct FleetVerification {
    context: &'static str,
    require_stowed: bool,
    require_adoption: bool,
    require_directive: bool,
}

impl FleetVerification {
    fn for_plan(plan: &RoutePlan) -> Self {
        Self {
            context: "plan",
            require_stowed: phase_requires_stowed_fleet(plan.phase),
            require_adoption: plan.fleet_prepared,
            require_directive: plan.fleet_prepared,
        }
    }

    const fn travel() -> Self {
        Self {
            context: "travel",
            require_stowed: true,
            require_adoption: true,
            require_directive: true,
        }
    }
}

async fn verify_fleet(
    client: &Client,
    config: &Config,
    plan: &RoutePlan,
    requirements: FleetVerification,
) -> AnyResult<()> {
    let controller = plan
        .controller
        .as_deref()
        .ok_or_else(|| app_error(io::ErrorKind::Other, "route plan has no survey controller"))?;
    if plan.drones.len() != DRONE_COUNT {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "route plan must contain exactly {DRONE_COUNT} survey drones; found {}",
                plan.drones.len()
            ),
        ));
    }

    let devices = refresh_assigned_device_snapshots(client, &config.replicant).await?;
    let vessel = required_device(&devices, &config.vessel, "racing vessel")?;
    ensure_device_type(vessel, &config.vessel, "racing_vessel")?;
    ensure_device_replicant(vessel, &config.vessel, &config.replicant)?;

    let controller_device = required_device(&devices, controller, "survey controller")?;
    ensure_device_type(controller_device, controller, "ami_survey_controller")?;
    ensure_device_replicant(controller_device, controller, &config.replicant)?;
    if requirements.require_stowed {
        ensure_stowed_in_vessel(controller_device, controller, &config.vessel)?;
    }

    for code in &plan.drones {
        let device = required_device(&devices, code, "survey drone")?;
        ensure_device_type(device, code, "survey_drone")?;
        ensure_device_replicant(device, code, &config.replicant)?;
        if requirements.require_stowed {
            ensure_stowed_in_vessel(device, code, &config.vessel)?;
        }
        if requirements.require_adoption && device_controller(device) != Some(controller) {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!("survey drone {code} is not adopted by controller {controller}"),
            ));
        }
    }

    if requirements.require_directive && !has_survey_system_directive(controller_device) {
        return Err(app_error(
            io::ErrorKind::InvalidData,
            format!("controller {controller} no longer has survey_system configured"),
        ));
    }

    if requirements.context == "travel" {
        info!(
            target: "replicant_client::explore",
            event = "travel.fleet_onboard_verified",
            replicant = %config.replicant,
            vessel = %config.vessel,
            controller,
            drones = ?plan.drones,
            "verified the complete survey fleet is onboard before travel"
        );
    } else {
        debug!(
            target: "replicant_client::explore",
            event = "fleet.verification_completed",
            context = requirements.context,
            require_stowed = requirements.require_stowed,
            require_adoption = requirements.require_adoption,
            require_directive = requirements.require_directive,
            controller,
            drones = ?plan.drones,
            "verified survey-fleet invariants"
        );
    }

    Ok(())
}

fn phase_requires_stowed_fleet(phase: RunPhase) -> bool {
    matches!(
        phase,
        RunPhase::Ready | RunPhase::SystemScanning
    )
}

fn ensure_account_owned(account_owned: &BTreeSet<String>, code: &str) -> AnyResult<()> {
    if account_owned.contains(code) {
        return Ok(());
    }
    Err(app_error(
        io::ErrorKind::PermissionDenied,
        format!("device {code} is not present in the authenticated account's owned-device set"),
    ))
}

fn validate_controller_candidate(
    device: &Device,
    code: &str,
    vessel_location: &str,
    vessel: &str,
) -> AnyResult<()> {
    ensure_device_type(device, code, "ami_survey_controller")?;
    if device_status_name(device) != Some("idle") {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("survey controller {code} is not idle"),
        ));
    }
    if !device.relationships.controlled_devices.is_empty() {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("survey controller {code} already controls one or more devices"),
        ));
    }
    validate_device_vessel_placement(device, code, vessel_location, vessel)
}

fn validate_drone_candidate(
    device: &Device,
    code: &str,
    controller: &str,
    vessel_location: &str,
    vessel: &str,
) -> AnyResult<()> {
    ensure_device_type(device, code, "survey_drone")?;
    if device_status_name(device) != Some("idle") {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("survey drone {code} is not idle"),
        ));
    }
    if let Some(actual) = device_controller(device)
        && actual != controller
    {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("survey drone {code} is already controlled by {actual}"),
        ));
    }
    validate_device_vessel_placement(device, code, vessel_location, vessel)
}

async fn ensure_replicant_owns_device(
    client: &Client,
    code: &str,
    replicant: &str,
) -> AnyResult<()> {
    let device = refresh_device_snapshot(client, code).await?;
    if device_replicant(&device) == Some(replicant) {
        debug!(
            target: "replicant_client::explore",
            event = "fleet.owner_verified",
            device = code,
            replicant,
            "device is already assigned to the target replicant"
        );
        return Ok(());
    }

    if !device_has_command(&device, "change_owner") {
        return Err(app_error(
            io::ErrorKind::PermissionDenied,
            format!(
                "device {code} is assigned to {:?} and does not advertise change_owner",
                device_replicant(&device)
            ),
        ));
    }

    info!(
        target: "replicant_client::explore",
        event = "fleet.owner_change_started",
        device = code,
        previous_replicant = device_replicant(&device).unwrap_or("unassigned"),
        target_replicant = replicant,
        "transferring device to the target replicant"
    );
    let handle = client.devices().get(code).await?;
    let operation = handle.change_owner(replicant.to_owned()).await?;
    wait_for_device_owner(client, code, replicant, &operation).await
}

async fn wait_for_device_owner(
    client: &Client,
    code: &str,
    replicant: &str,
    operation: &Operation,
) -> AnyResult<()> {
    const VERIFY_TIMEOUT: Duration = Duration::from_secs(30);
    const INITIAL_DELAY: Duration = Duration::from_millis(250);
    const MAX_DELAY: Duration = Duration::from_secs(2);

    let started = Instant::now();
    let mut delay = INITIAL_DELAY;
    let mut attempts = 0_u32;
    let mut last_replicant = None;

    loop {
        attempts += 1;
        let refreshed = refresh_device_snapshot(client, code).await?;
        last_replicant = device_replicant(&refreshed).map(str::to_owned);

        if last_replicant.as_deref() == Some(replicant) {
            let operation_status = match operation.reconcile().await {
                Ok(outcome) => outcome.status,
                Err(reconcile_error) => {
                    warn!(
                        target: "replicant_client::explore",
                        event = "fleet.owner_change_reconcile_failed",
                        device = code,
                        operation_id = %operation.id(),
                        error = %reconcile_error,
                        "ownership is authoritative, but the durable operation could not be reconciled"
                    );
                    operation.status().await?
                }
            };
            info!(
                target: "replicant_client::explore",
                event = "fleet.owner_change_completed",
                device = code,
                replicant,
                attempts,
                operation_id = %operation.id(),
                operation_status = ?operation_status,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "verified device ownership transfer from authoritative managed state"
            );
            return Ok(());
        }

        let operation_status = operation.status().await?;
        match operation_status {
            OperationStatus::Rejected | OperationStatus::Cancelled | OperationStatus::Failed => {
                return Err(app_error(
                    io::ErrorKind::Other,
                    format!(
                        "change_owner for device {code} ended with {operation_status:?}; device still reports replicant={last_replicant:?}"
                    ),
                ));
            }
            _ => {}
        }

        if started.elapsed() >= VERIFY_TIMEOUT {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "device {code} did not report replicant={replicant} within {VERIFY_TIMEOUT:?}; last_replicant={last_replicant:?}, operation_status={operation_status:?}, operation_id={} (rerun is safe)",
                    operation.id()
                ),
            ));
        }

        debug!(
            target: "replicant_client::explore",
            event = "fleet.owner_change_pending",
            device = code,
            target_replicant = replicant,
            current_replicant = last_replicant.as_deref().unwrap_or("unassigned"),
            attempts,
            operation_id = %operation.id(),
            operation_status = ?operation_status,
            next_poll_ms = delay.as_millis() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "ownership transfer is accepted but not yet visible in authoritative managed state"
        );

        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(MAX_DELAY);
    }
}

fn ensure_device_type(device: &Device, code: &str, expected_type: &str) -> AnyResult<()> {
    if device_type_name(device) == Some(expected_type) {
        return Ok(());
    }
    Err(app_error(
        io::ErrorKind::InvalidData,
        format!(
            "device {code} reports device_type={:?}, expected {expected_type}",
            device_type_name(device)
        ),
    ))
}

fn ensure_device_replicant(
    device: &Device,
    code: &str,
    expected_replicant: &str,
) -> AnyResult<()> {
    match device_replicant(device) {
        Some(actual) if actual == expected_replicant => Ok(()),
        Some(actual) => Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("device {code} reports replicant={actual}, expected {expected_replicant}"),
        )),
        None => Err(app_error(
            io::ErrorKind::InvalidData,
            format!(
                "device {code} does not report replicant ownership; expected {expected_replicant}"
            ),
        )),
    }
}

fn validate_device_vessel_placement(
    device: &Device,
    code: &str,
    vessel_location: &str,
    vessel: &str,
) -> AnyResult<()> {
    match device_stowed_in(device) {
        Some(actual) if actual == vessel => Ok(()),
        Some(actual) => Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("device {code} is stowed in vessel {actual}, not required vessel {vessel}"),
        )),
        None if device_location(device) == Some(vessel_location) => Ok(()),
        None => Err(app_error(
            io::ErrorKind::InvalidInput,
            format!(
                "device {code} is not stowed in vessel {vessel} and is at {:?}, while the vessel is at {vessel_location}",
                device_location(device)
            ),
        )),
    }
}

fn ensure_stowed_in_vessel(device: &Device, code: &str, vessel: &str) -> AnyResult<()> {
    if device_stowed_in(device) == Some(vessel) {
        return Ok(());
    }
    Err(app_error(
        io::ErrorKind::InvalidData,
        format!(
            "device {code} is not stowed in required vessel {vessel}; reported stowed_in={:?}",
            device_stowed_in(device)
        ),
    ))
}

fn has_survey_system_directive(device: &Device) -> bool {
    device
        .active_directive
        .as_ref()
        .and_then(|directive| directive.directive.as_ref())
        .is_some_and(|directive| directive.as_str() == "survey_system")
}

async fn stow_fleet(client: &Client, config: &Config, plan: &RoutePlan) -> AnyResult<()> {
    let controller = plan
        .controller
        .as_deref()
        .ok_or_else(|| app_error(io::ErrorKind::Other, "route plan has no survey controller"))?;

    let mut codes = plan.drones.clone();
    codes.push(controller.to_owned());

    let devices = refresh_assigned_device_snapshots(client, &config.replicant).await?;
    let vessel = required_device(&devices, &config.vessel, "racing vessel")?;
    ensure_device_replicant(vessel, &config.vessel, &config.replicant)?;
    let vessel_location = device_location(vessel).ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            format!("vessel {} has no current location", config.vessel),
        )
    })?;
    let capacity = vessel.stow_capacity;
    let used = vessel.stow_used.unwrap_or(0);
    let mut missing = Vec::new();

    for code in &codes {
        let device = required_device(&devices, code, "survey-fleet device")?;
        ensure_device_replicant(device, code, &config.replicant)?;
        validate_device_vessel_placement(device, code, vessel_location, &config.vessel)?;
        if device_stowed_in(device) != Some(config.vessel.as_str()) {
            missing.push(code.clone());
        }
    }

    if let Some(capacity) = capacity {
        if used + i64::try_from(missing.len())? > capacity {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!(
                    "vessel {} has stow capacity {capacity}, currently uses {used}, and needs {} more slots",
                    config.vessel,
                    missing.len()
                ),
            ));
        }
    } else if !missing.is_empty() {
        warn!(
            target: "replicant_client::explore",
            event = "fleet.stow_capacity_unknown",
            vessel = %config.vessel,
            used,
            additional_devices = missing.len(),
            "managed vessel state did not report stow capacity; letting the server enforce it"
        );
    }

    for code in missing {
        ensure_device_stowed_idempotently(client, config, &code).await?;
    }

    let verified = refresh_assigned_device_snapshots(client, &config.replicant).await?;
    for code in &codes {
        let device = required_device(&verified, code, "survey-fleet device")?;
        ensure_device_replicant(device, code, &config.replicant)?;
        ensure_stowed_in_vessel(device, code, &config.vessel)?;
    }

    info!(
        target: "replicant_client::explore",
        event = "fleet.stow_verified",
        replicant = %config.replicant,
        vessel = %config.vessel,
        devices = ?codes,
        "verified from managed state that the complete survey fleet belongs to the target replicant and is stowed in the correct vessel"
    );

    Ok(())
}

async fn ensure_device_stowed_idempotently(
    client: &Client,
    config: &Config,
    code: &str,
) -> AnyResult<()> {
    let started = Instant::now();
    let before = refresh_device_snapshot(client, code).await?;
    ensure_device_replicant(&before, code, &config.replicant)?;

    if device_stowed_in(&before) == Some(config.vessel.as_str()) {
        info!(
            target: "replicant_client::explore",
            event = "fleet.stow_already_completed",
            device = code,
            vessel = %config.vessel,
            source = "managed_preflight_refresh",
            elapsed_ms = started.elapsed().as_millis() as u64,
            "device was auto-stowed before the explicit stow request"
        );
        return Ok(());
    }

    let vessel = refresh_device_snapshot(client, &config.vessel).await?;
    ensure_device_replicant(&vessel, &config.vessel, &config.replicant)?;
    let vessel_location = device_location(&vessel).ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            format!("vessel {} has no current location", config.vessel),
        )
    })?;
    validate_device_vessel_placement(&before, code, vessel_location, &config.vessel)?;

    info!(
        target: "replicant_client::explore",
        event = "fleet.stow_started",
        device = code,
        vessel = %config.vessel,
        "stowing survey-fleet device"
    );

    let handle = client.devices().get(code).await?;
    let operation = handle.stow(Some(config.vessel.clone())).await?;
    wait_for_authoritative_stow(client, config, code, &operation).await
}

async fn wait_for_authoritative_stow(
    client: &Client,
    config: &Config,
    code: &str,
    operation: &Operation,
) -> AnyResult<()> {
    const VERIFY_TIMEOUT: Duration = Duration::from_secs(30);
    const TERMINAL_PROPAGATION_GRACE: Duration = Duration::from_secs(5);
    const INITIAL_DELAY: Duration = Duration::from_millis(150);
    const MAX_DELAY: Duration = Duration::from_secs(1);

    let started = Instant::now();
    let mut delay = INITIAL_DELAY;
    let mut attempts = 0_u32;

    loop {
        attempts += 1;

        let device = refresh_device_snapshot(client, code).await?;
        ensure_device_replicant(&device, code, &config.replicant)?;

        if device_stowed_in(&device) == Some(config.vessel.as_str()) {
            let outcome = operation.outcome().await?;
            if outcome.status != OperationStatus::Completed {
                debug!(
                    target: "replicant_client::explore",
                    event = "fleet.stow_operation_overridden_by_state",
                    device = code,
                    vessel = %config.vessel,
                    operation_id = %operation.id(),
                    operation_status = ?outcome.status,
                    operation_response = ?outcome.response,
                    "authoritative managed device state proves stow success despite the durable operation classification"
                );
            }

            info!(
                target: "replicant_client::explore",
                event = "fleet.stow_completed",
                device = code,
                vessel = %config.vessel,
                operation_id = %operation.id(),
                operation_status = ?outcome.status,
                attempts,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "managed device state confirms the device is stowed"
            );
            return Ok(());
        }

        if let Some(other_vessel) = device_stowed_in(&device) {
            return Err(app_error(
                io::ErrorKind::InvalidData,
                format!(
                    "device {code} became stowed in vessel {other_vessel}, expected {}",
                    config.vessel
                ),
            ));
        }

        let outcome = operation.outcome().await?;
        let terminal_failure = stow_operation_is_terminal_failure(outcome.status);

        if terminal_failure && started.elapsed() >= TERMINAL_PROPAGATION_GRACE {
            return Err(app_error(
                io::ErrorKind::Other,
                format!(
                    "stow operation {} for device {code} ended with {:?}, and managed device state still reports location={:?}, stowed_in={:?}; response={:?}",
                    operation.id(),
                    outcome.status,
                    device_location(&device),
                    device_stowed_in(&device),
                    outcome.response
                ),
            ));
        }

        if started.elapsed() >= VERIFY_TIMEOUT {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "device {code} did not report stowed_in={} within {VERIFY_TIMEOUT:?}; operation_id={}, operation_status={:?}, location={:?}, response={:?}",
                    config.vessel,
                    operation.id(),
                    outcome.status,
                    device_location(&device),
                    outcome.response
                ),
            ));
        }

        debug!(
            target: "replicant_client::explore",
            event = "fleet.stow_pending",
            device = code,
            vessel = %config.vessel,
            operation_id = %operation.id(),
            operation_status = ?outcome.status,
            location = device_location(&device).unwrap_or("none"),
            attempts,
            next_poll_ms = delay.as_millis() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "waiting for managed device state to confirm stow completion"
        );

        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(MAX_DELAY);
    }
}

fn stow_operation_is_terminal_failure(status: OperationStatus) -> bool {
    matches!(
        status,
        OperationStatus::Rejected | OperationStatus::Cancelled | OperationStatus::Failed
    )
}

async fn wait_immediate_operation(label: &str, operation: &Operation) -> AnyResult<()> {
    let started = Instant::now();
    let outcome = operation.wait_timeout(Duration::from_secs(60)).await?;
    info!(
        target: "replicant_client::explore",
        event = "operation.observed",
        label,
        operation_id = %operation.id(),
        status = ?outcome.status,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "observed operation outcome"
    );

    match outcome.status {
        OperationStatus::Completed => Ok(()),
        OperationStatus::Rejected | OperationStatus::Cancelled | OperationStatus::Failed => {
            Err(app_error(io::ErrorKind::Other, format!("{label} ended with {:?}", outcome.status)))
        }
        _ => {
            warn!(
                target: "replicant_client::explore",
                event = "operation.non_terminal",
                label,
                status = ?outcome.status,
                "operation remains non-terminal; subsequent authoritative verification will decide whether to continue"
            );
            Ok(())
        }
    }
}

async fn execute_route(client: &Client, config: &Config, plan: &mut RoutePlan) -> AnyResult<()> {
    while plan.next_index < plan.route.len() {
        let index = plan.next_index;
        let target = plan.route[index].star.clone();

        info!(
            target: "replicant_client::explore",
            event = "route.stop_started",
            index,
            total = plan.route.len(),
            star = %target,
            phase = ?plan.phase,
            "processing route stop"
        );

        match plan.phase {
            RunPhase::PreparingFleet => {
                prepare_fleet(client, config, plan).await?;
            }
            RunPhase::Ready => {
                let current = current_star(client, &config.replicant).await?;
                if current.as_deref() != Some(target.as_str()) {
                    // `travel_to` owns the departure invariant and will recall,
                    // stow, and authoritatively verify the complete fleet.
                    plan.phase = RunPhase::Traveling;
                    save_plan(&config.plan_path, plan)?;
                    travel_to(client, config, plan, &target).await?;
                }

                if !plan.route[index].survey_required {
                    plan.next_index += 1;
                    plan.phase = if plan.next_index >= plan.route.len() {
                        RunPhase::Complete
                    } else {
                        RunPhase::Ready
                    };
                    save_plan(&config.plan_path, plan)?;
                    continue;
                }

                if !plan.route[index].system_scan_done {
                    plan.phase = RunPhase::SystemScanning;
                    save_plan(&config.plan_path, plan)?;
                } else if !plan.route[index].survey_done {
                    plan.phase = RunPhase::Surveying;
                    save_plan(&config.plan_path, plan)?;
                } else {
                    plan.phase = RunPhase::Restowing;
                    save_plan(&config.plan_path, plan)?;
                }
            }
            RunPhase::Traveling => {
                travel_to(client, config, plan, &target).await?;
                plan.phase = if plan.route[index].system_scan_done {
                    RunPhase::Surveying
                } else {
                    RunPhase::SystemScanning
                };
                save_plan(&config.plan_path, plan)?;
            }
            RunPhase::SystemScanning => {
                run_system_scan(client, config, &target).await?;
                plan.route[index].system_scan_done = true;
                plan.phase = RunPhase::Surveying;
                save_plan(&config.plan_path, plan)?;
            }
            RunPhase::Surveying => {
                run_survey(client, config, plan, &target).await?;
                plan.route[index].survey_done = true;
                plan.phase = RunPhase::Restowing;
                save_plan(&config.plan_path, plan)?;
            }
            RunPhase::Restowing => {
                recall_and_stow(client, config, plan).await?;
                plan.next_index += 1;
                plan.phase = if plan.next_index >= plan.route.len() {
                    RunPhase::Complete
                } else {
                    RunPhase::Ready
                };
                save_plan(&config.plan_path, plan)?;
                info!(
                    target: "replicant_client::explore",
                    event = "route.stop_completed",
                    index,
                    star = %target,
                    next_index = plan.next_index,
                    "route stop completed and saved"
                );
            }
            RunPhase::Complete => break,
        }
    }

    plan.phase = RunPhase::Complete;
    save_plan(&config.plan_path, plan)?;
    info!(
        target: "replicant_client::explore",
        event = "route.completed",
        stops = plan.route.len(),
        plan = %config.plan_path.display(),
        "survey route completed"
    );
    Ok(())
}

async fn travel_to(
    client: &Client,
    config: &Config,
    plan: &RoutePlan,
    target: &str,
) -> AnyResult<()> {
    if current_star(client, &config.replicant).await?.as_deref() == Some(target) {
        info!(
            target: "replicant_client::explore",
            event = "travel.already_arrived",
            star = target,
            "replicant is already at target star"
        );
        return Ok(());
    }

    let replicant = client.replicants().get_owned(&config.replicant).await?;
    let replicant_snapshot = replicant.snapshot().await?;
    let already_traveling_to_target = replicant_snapshot.travel.as_ref().is_some_and(|travel| {
        travel
            .destination
            .as_ref()
            .is_some_and(|destination| designation_in_star(destination.id.as_str(), target))
    });

    if already_traveling_to_target {
        // A resumed trip cannot safely recall or stow devices mid-flight.
        // Refuse to continue unless authoritative device state proves the
        // controller and every configured drone were onboard when travel began.
        verify_fleet(client, config, plan, FleetVerification::travel()).await?;
    } else {
        info!(
            target: "replicant_client::explore",
            event = "travel.fleet_preflight_started",
            destination = target,
            vessel = %config.vessel,
            "recalling, stowing, and verifying the survey fleet before departure"
        );
        recall_and_stow(client, config, plan).await?;
        verify_fleet(client, config, plan, FleetVerification::travel()).await?;
    }

    let mut watch = client.events().watch().await?;
    if !already_traveling_to_target {
        info!(
            target: "replicant_client::explore",
            event = "travel.departure_requested",
            replicant = %config.replicant,
            vessel = %config.vessel,
            destination = target,
            "requesting interstellar travel"
        );
        let replicant = client.replicants().get_owned(&config.replicant).await?;
        let operation = replicant.travel().to(target).depart().await?;
        debug!(
            target: "replicant_client::explore",
            event = "travel.operation_registered",
            operation_id = %operation.id(),
            destination = target,
            "durable travel operation registered"
        );
    } else {
        info!(
            target: "replicant_client::explore",
            event = "travel.resumed",
            destination = target,
            "existing in-progress travel matches the saved route"
        );
    }

    let started = Instant::now();
    loop {
        if started.elapsed() >= config.travel_timeout {
            if current_star(client, &config.replicant).await?.as_deref() == Some(target) {
                return Ok(());
            }
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!("travel to {target} exceeded {:?}", config.travel_timeout),
            ));
        }

        let wait_for = (config.travel_timeout - started.elapsed()).min(Duration::from_secs(30));
        match tokio::time::timeout(wait_for, watch.next()).await {
            Ok(Ok(event)) => {
                if is_travel_event_for(&event, config, target) {
                    info!(
                        target: "replicant_client::explore",
                        event = "travel.event",
                        event_id = %event.id,
                        event_name = event.name.as_str(),
                        destination = target,
                        payload = ?event.payload,
                        "observed relevant travel event"
                    );
                    if event.name.as_str() == "travel.arrived"
                        && current_star(client, &config.replicant).await?.as_deref()
                            == Some(target)
                    {
                        info!(
                            target: "replicant_client::explore",
                            event = "travel.arrived",
                            destination = target,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "arrived at target star"
                        );
                        return Ok(());
                    }
                }
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                let current = current_star(client, &config.replicant).await?;
                info!(
                    target: "replicant_client::explore",
                    event = "travel.waiting",
                    destination = target,
                    current_star = current.as_deref().unwrap_or("unknown"),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "still waiting for travel arrival"
                );
                if current.as_deref() == Some(target) {
                    return Ok(());
                }
            }
        }
    }
}

async fn run_system_scan(client: &Client, config: &Config, target: &str) -> AnyResult<()> {
    let started = Instant::now();

    let locally_explored = client
        .galaxy()
        .replicant_star_knowledge(&config.replicant)
        .into_iter()
        .any(|knowledge| knowledge.star.id.as_str() == target && knowledge.explored == Some(true));
    if locally_explored {
        info!(
            target: "replicant_client::explore",
            event = "system_scan.already_completed",
            replicant = %config.replicant,
            star = target,
            source = "local_star_knowledge",
            "star is already explored; skipping duplicate system scan"
        );
        return Ok(());
    }

    let refreshed = client
        .galaxy()
        .refresh_replicant_star(&config.replicant, target)
        .await?;
    if refreshed.explored == Some(true) {
        info!(
            target: "replicant_client::explore",
            event = "system_scan.already_completed",
            replicant = %config.replicant,
            star = target,
            source = "targeted_refresh",
            "authoritative star knowledge confirms the system scan already completed"
        );
        return Ok(());
    }

    info!(
        target: "replicant_client::explore",
        event = "system_scan.started",
        replicant = %config.replicant,
        star = target,
        endpoint = "POST /v1/replicants/{code}/scan",
        "starting instant replicant system scan"
    );

    let replicant = client.replicants().get_owned(&config.replicant).await?;
    let operation = replicant.scan().await?;
    let outcome = operation.outcome().await?;

    // Replicant scans do not expect later event evidence. In the current
    // durable-operation engine, a successfully decoded 2xx response therefore
    // lands in ReconciliationRequired; the HTTP response itself is the scan's
    // completion signal.
    if system_scan_response_was_ok(outcome.status) {
        info!(
            target: "replicant_client::explore",
            event = "system_scan.response_ok",
            replicant = %config.replicant,
            star = target,
            operation_id = %operation.id(),
            operation_status = ?outcome.status,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "instant system scan returned a successful response"
        );

        // Best-effort refresh for durable startup/resume knowledge. A stale or
        // failed read does not invalidate the successful scan response.
        match client
            .galaxy()
            .refresh_replicant_star(&config.replicant, target)
            .await
        {
            Ok(knowledge) => {
                debug!(
                    target: "replicant_client::explore",
                    event = "system_scan.star_knowledge_refreshed",
                    replicant = %config.replicant,
                    star = target,
                    explored = knowledge.explored,
                    "refreshed star knowledge after the instant system scan"
                );
            }
            Err(refresh_error) => {
                warn!(
                    target: "replicant_client::explore",
                    event = "system_scan.post_refresh_failed",
                    replicant = %config.replicant,
                    star = target,
                    operation_id = %operation.id(),
                    error = %refresh_error,
                    "system scan succeeded, but the follow-up star refresh failed"
                );
            }
        }

        return Ok(());
    }

    match outcome.status {
        OperationStatus::Rejected | OperationStatus::Cancelled | OperationStatus::Failed => {
            return Err(app_error(io::ErrorKind::Other, format!(
                "instant system scan for {target} ended with {:?}: {:?}",
                outcome.status, outcome.response
            )));
        }
        _ => {}
    }

    // An ambiguous or otherwise unexpected non-terminal state may mean the
    // request reached the server. Reconcile through authoritative star
    // knowledge; never wait for an individual-body scan event and never blindly resubmit.
    warn!(
        target: "replicant_client::explore",
        event = "system_scan.outcome_ambiguous",
        replicant = %config.replicant,
        star = target,
        operation_id = %operation.id(),
        operation_status = ?outcome.status,
        "system scan response was not safely classified; checking star knowledge"
    );

    let knowledge = client
        .galaxy()
        .refresh_replicant_star(&config.replicant, target)
        .await?;
    if knowledge.explored == Some(true) {
        info!(
            target: "replicant_client::explore",
            event = "system_scan.reconciled",
            replicant = %config.replicant,
            star = target,
            operation_id = %operation.id(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "targeted star knowledge confirms the system scan completed"
        );
        return Ok(());
    }

    Err(app_error(io::ErrorKind::Other, format!(
        "system scan operation {} for {target} is {:?}, and targeted star knowledge does not confirm completion; rerun to reconcile without submitting a blind duplicate",
        operation.id(),
        outcome.status
    )))
}

fn system_scan_response_was_ok(status: OperationStatus) -> bool {
    matches!(
        status,
        OperationStatus::ReconciliationRequired | OperationStatus::Completed
    )
}

async fn latest_event_history_cursor(client: &Client) -> AnyResult<Option<String>> {
    const MAX_PAGES: usize = 500;

    let report = client.events().catch_up(MAX_PAGES).await?;
    if !report.complete {
        return Err(app_error(
            io::ErrorKind::TimedOut,
            format!(
                "managed event catch-up did not reach the terminal cursor within {MAX_PAGES} pages; refusing to start a survey without a trustworthy history baseline"
            ),
        ));
    }
    Ok(report.cursor)
}

async fn poll_survey_completion_history(
    client: &Client,
    cursor: &mut Option<String>,
    controller: &str,
    target: &str,
) -> AnyResult<Option<(String, String, SurveyCompletionProof)>> {
    const MAX_PAGES: usize = 10;

    let requested_cursor = cursor.clone();
    let report = client.events().catch_up(MAX_PAGES).await?;
    let mut query = client.events().history().for_device(controller);
    if let Some(after) = requested_cursor.as_deref() {
        query = query.after(after);
    }
    let events = query.collect().await?;

    debug!(
        target: "replicant_client::explore",
        event = "survey.history_checked",
        controller,
        star = target,
        events = events.len(),
        complete = report.complete,
        cursor = requested_cursor.as_deref().unwrap_or(""),
        applied_cursor = report.cursor.as_deref().unwrap_or(""),
        "checked managed durable event history for survey completion"
    );

    for event in &events {
        if let Some(proof) = survey_completion_proof(event, controller, target) {
            *cursor = Some(event.id.as_str().to_owned());
            return Ok(Some((
                event.id.as_str().to_owned(),
                event.name.as_str().to_owned(),
                proof,
            )));
        }
    }

    if let Some(last) = events.last() {
        *cursor = Some(last.id.as_str().to_owned());
    } else if report.cursor.is_some() {
        *cursor = report.cursor;
    }

    if !report.complete {
        warn!(
            target: "replicant_client::explore",
            event = "survey.history_page_bound_hit",
            controller,
            star = target,
            max_pages = MAX_PAGES,
            cursor = cursor.as_deref().unwrap_or(""),
            "managed survey-history catch-up reached its page bound"
        );
    }
    Ok(None)
}

async fn run_survey(
    client: &Client,
    config: &Config,
    plan: &RoutePlan,
    target: &str,
) -> AnyResult<()> {
    let controller_code = plan
        .controller
        .as_deref()
        .ok_or_else(|| app_error(io::ErrorKind::Other, "route plan has no survey controller"))?;
    let controller_handle = client.devices().get(controller_code).await?;
    let controller = controller_handle.as_survey_controller()?;

    // Capture a durable history watermark before opening the local live watch.
    // If a completion lands during an SSE disconnect or between this request
    // and watch subscription, the unfiltered-history fallback can still find it.
    let mut history_cursor = latest_event_history_cursor(client).await?;
    let mut watch = client.events().watch().await?;
    debug!(
        target: "replicant_client::explore",
        event = "survey.history_baseline_captured",
        controller = controller_code,
        star = target,
        cursor = history_cursor.as_deref().unwrap_or(""),
        "captured survey event-history watermark"
    );

    let controller_snapshot = refresh_device_snapshot(client, controller_code).await?;
    if survey_directive_needs_launch(active_directive_status(&controller_snapshot)) {
        info!(
            target: "replicant_client::explore",
            event = "survey.launch_started",
            controller = controller_code,
            star = target,
            drones = ?plan.drones,
            "launching survey controller"
        );
        let operation = controller.launch().await?;
        wait_immediate_operation("launch survey controller", &operation).await?;
    } else {
        info!(
            target: "replicant_client::explore",
            event = "survey.resumed",
            controller = controller_code,
            star = target,
            directive_status = active_directive_status(&controller_snapshot).unwrap_or("unknown"),
            controller_status = device_status_name(&controller_snapshot).unwrap_or("unknown"),
            "controller already has a launched survey directive"
        );
    }

    let started = Instant::now();
    loop {
        if started.elapsed() >= config.survey_timeout {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "survey at {target} exceeded {:?}; plan remains resumable",
                    config.survey_timeout
                ),
            ));
        }

        let wait_for = (config.survey_timeout - started.elapsed()).min(Duration::from_secs(30));
        match tokio::time::timeout(wait_for, watch.next()).await {
            Ok(Ok(event)) => {
                if is_survey_digest_for(&event, controller_code, target) {
                    let progress = survey_progress(&event.payload);
                    info!(
                        target: "replicant_client::explore",
                        event = "survey.digest",
                        event_id = %event.id,
                        controller = controller_code,
                        star = target,
                        progress_known = progress.is_some(),
                        scanned = progress.map_or(0, |value| value.0),
                        remaining = progress.map_or(0, |value| value.1),
                        total = progress.map_or(0, |value| value.2),
                        devices = ?event.payload.get("devices"),
                        "observed survey digest"
                    );
                }

                if let Some(proof) = survey_completion_proof(&event, controller_code, target) {
                    info!(
                        target: "replicant_client::explore",
                        event = "survey.completion_observed",
                        event_id = %event.id,
                        event_name = event.name.as_str(),
                        proof = ?proof,
                        controller = controller_code,
                        star = target,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "survey completion has event evidence; reconciling planet and moon completeness"
                    );
                    confirm_survey_completion(client, config, target).await?;
                    return Ok(());
                }
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                match poll_survey_completion_history(
                    client,
                    &mut history_cursor,
                    controller_code,
                    target,
                )
                .await
                {
                    Ok(Some((event_id, event_name, proof))) => {
                        info!(
                            target: "replicant_client::explore",
                            event = "survey.completion_history_observed",
                            event_id,
                            event_name,
                            proof = ?proof,
                            controller = controller_code,
                            star = target,
                            elapsed_ms = started.elapsed().as_millis() as u64,
                            "found missed survey completion in unfiltered event history"
                        );
                        confirm_survey_completion(client, config, target).await?;
                        return Ok(());
                    }
                    Ok(None) => {}
                    Err(history_error) => {
                        warn!(
                            target: "replicant_client::explore",
                            event = "survey.history_poll_failed",
                            controller = controller_code,
                            star = target,
                            error = %history_error,
                            "could not check unfiltered event history; continuing the live wait"
                        );
                    }
                }

                info!(
                    target: "replicant_client::explore",
                    event = "survey.waiting",
                    controller = controller_code,
                    star = target,
                    history_cursor = history_cursor.as_deref().unwrap_or(""),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "still waiting for survey completion"
                );
            }
        }
    }
}

#[derive(Debug)]
struct FleetReturnCheck {
    vessel_location: String,
    pending: Vec<String>,
    recall_in_progress: bool,
    controller_status: Option<String>,
    controller_directive_status: Option<String>,
    controller_withdraw_available: bool,
}

#[derive(Debug)]
enum FleetReturnInspection {
    Available(FleetReturnCheck),
    RateLimited { retry_after: Duration },
}

fn fleet_return_rate_limit_delay(retry_after: Option<Duration>) -> Duration {
    const MINIMUM_DELAY: Duration = Duration::from_secs(15);

    match retry_after {
        Some(delay) if delay >= MINIMUM_DELAY => delay,
        Some(_) | None => MINIMUM_DELAY,
    }
}

async fn recall_and_stow(client: &Client, config: &Config, plan: &RoutePlan) -> AnyResult<()> {
    let controller_code = plan
        .controller
        .as_deref()
        .ok_or_else(|| app_error(io::ErrorKind::Other, "route plan has no survey controller"))?;

    let initial = loop {
        match inspect_fleet_return_state(client, config, plan).await? {
            FleetReturnInspection::Available(check) => break check,
            FleetReturnInspection::RateLimited { retry_after } => {
                warn!(
                    target: "replicant_client::explore",
                    event = "survey.recall_inspection_rate_limited",
                    controller = controller_code,
                    retry_after_ms = retry_after.as_millis() as u64,
                    "rate limited while refreshing managed survey-fleet state; backing off"
                );
                tokio::time::sleep(retry_after).await;
            }
        }
    };

    if initial.pending.is_empty() {
        info!(
            target: "replicant_client::explore",
            event = "survey.recall_already_completed",
            controller = controller_code,
            vessel = %config.vessel,
            vessel_location = %initial.vessel_location,
            "the complete survey fleet has already returned to the vessel location"
        );
        return stow_fleet(client, config, plan).await;
    }

    let mut withdraw_operation = None;
    if initial.recall_in_progress {
        info!(
            target: "replicant_client::explore",
            event = "survey.recall_already_in_progress",
            controller = controller_code,
            vessel = %config.vessel,
            vessel_location = %initial.vessel_location,
            pending = ?initial.pending,
            "survey completion already triggered automatic fleet recall"
        );
    } else if survey_directive_needs_recall(initial.controller_directive_status.as_deref())
        && initial.controller_withdraw_available
    {
        info!(
            target: "replicant_client::explore",
            event = "survey.withdraw_started",
            controller = controller_code,
            pending = ?initial.pending,
            "requesting survey-fleet recall before stowing"
        );
        let handle = client.devices().cached(controller_code).ok_or_else(|| {
            app_error(
                io::ErrorKind::NotFound,
                format!("managed state omitted survey controller {controller_code}"),
            )
        })?;
        let controller = handle.as_survey_controller()?;
        let operation = controller.withdraw().await?;
        debug!(
            target: "replicant_client::explore",
            event = "survey.withdraw_registered",
            controller = controller_code,
            operation_id = %operation.id(),
            "registered survey-fleet withdraw operation"
        );
        withdraw_operation = Some(operation);
    } else {
        warn!(
            target: "replicant_client::explore",
            event = "survey.withdraw_unavailable",
            controller = controller_code,
            controller_status = initial.controller_status.as_deref().unwrap_or("unknown"),
            directive_status = initial
                .controller_directive_status
                .as_deref()
                .unwrap_or("unknown"),
            pending = ?initial.pending,
            "fleet has not returned, but withdraw is not currently advertised; waiting for managed placement to converge"
        );
    }

    wait_for_fleet_return(client, config, plan, initial, withdraw_operation.as_ref()).await?;
    stow_fleet(client, config, plan).await
}

async fn wait_for_fleet_return(
    client: &Client,
    config: &Config,
    plan: &RoutePlan,
    initial: FleetReturnCheck,
    withdraw_operation: Option<&Operation>,
) -> AnyResult<()> {
    const RETURN_TIMEOUT: Duration = Duration::from_secs(30 * 60);
    const INITIAL_DELAY: Duration = Duration::from_secs(5);
    const MAX_DELAY: Duration = Duration::from_secs(15);

    let started = Instant::now();
    let mut delay = INITIAL_DELAY;
    let mut attempts = 0_u32;
    let mut check = initial;

    loop {
        attempts += 1;

        if check.pending.is_empty() {
            if let Some(operation) = withdraw_operation
                && let Err(reconcile_error) = operation.reconcile().await
            {
                warn!(
                    target: "replicant_client::explore",
                    event = "survey.withdraw_reconcile_failed",
                    operation_id = %operation.id(),
                    error = %reconcile_error,
                    "fleet return is authoritative, but the durable operation could not be reconciled"
                );
            }

            info!(
                target: "replicant_client::explore",
                event = "survey.recall_completed",
                vessel = %config.vessel,
                vessel_location = %check.vessel_location,
                attempts,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "all survey-fleet devices have returned and are eligible to be stowed"
            );
            return Ok(());
        }

        let operation_status = if let Some(operation) = withdraw_operation {
            let status = operation.status().await?;
            match status {
                OperationStatus::Rejected
                | OperationStatus::Cancelled
                | OperationStatus::Failed => {
                    return Err(app_error(
                        io::ErrorKind::Other,
                        format!(
                            "withdraw operation {} ended with {status:?} while devices were still returning: {:?}",
                            operation.id(),
                            check.pending
                        ),
                    ));
                }
                _ => {}
            }
            Some(status)
        } else {
            None
        };

        if started.elapsed() >= RETURN_TIMEOUT {
            return Err(app_error(
                io::ErrorKind::TimedOut,
                format!(
                    "survey fleet did not return to vessel {} at {} within {RETURN_TIMEOUT:?}; pending={:?}, withdraw_status={operation_status:?}",
                    config.vessel,
                    check.vessel_location,
                    check.pending
                ),
            ));
        }

        info!(
            target: "replicant_client::explore",
            event = "survey.recall_waiting",
            vessel = %config.vessel,
            vessel_location = %check.vessel_location,
            pending = ?check.pending,
            recall_in_progress = check.recall_in_progress,
            withdraw_status = ?operation_status,
            attempts,
            next_poll_ms = delay.as_millis() as u64,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "waiting for managed survey-fleet state to report physical return before stowing"
        );

        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(MAX_DELAY);

        loop {
            match inspect_fleet_return_state(client, config, plan).await? {
                FleetReturnInspection::Available(next) => {
                    check = next;
                    break;
                }
                FleetReturnInspection::RateLimited { retry_after } => {
                    if started.elapsed() >= RETURN_TIMEOUT {
                        return Err(app_error(
                            io::ErrorKind::TimedOut,
                            format!(
                                "survey fleet return inspection remained rate limited for {RETURN_TIMEOUT:?}; last known pending={:?}",
                                check.pending
                            ),
                        ));
                    }
                    warn!(
                        target: "replicant_client::explore",
                        event = "survey.recall_poll_rate_limited",
                        vessel = %config.vessel,
                        pending = ?check.pending,
                        retry_after_ms = retry_after.as_millis() as u64,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "managed fleet refresh was rate limited; backing off without failing the route"
                    );
                    tokio::time::sleep(retry_after).await;
                    delay = MAX_DELAY;
                }
            }
        }
    }
}

async fn inspect_fleet_return_state(
    client: &Client,
    config: &Config,
    plan: &RoutePlan,
) -> AnyResult<FleetReturnInspection> {
    let controller = plan
        .controller
        .as_deref()
        .ok_or_else(|| app_error(io::ErrorKind::Other, "route plan has no survey controller"))?;

    let devices = match refresh_assigned_device_snapshots(client, &config.replicant).await {
        Ok(devices) => devices,
        Err(error) if error.status() == Some(429) => {
            return Ok(FleetReturnInspection::RateLimited {
                retry_after: fleet_return_rate_limit_delay(error.retry_after()),
            });
        }
        Err(error) => return Err(error.into()),
    };

    let vessel = required_device(&devices, &config.vessel, "racing vessel")?;
    ensure_device_replicant(vessel, &config.vessel, &config.replicant)?;
    let vessel_location = device_location(vessel).ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidData,
            format!("vessel {} has no current location", config.vessel),
        )
    })?;

    let controller_device = required_device(&devices, controller, "survey controller")?;
    ensure_device_replicant(controller_device, controller, &config.replicant)?;

    let mut pending = Vec::new();
    let mut recall_in_progress = false;

    for code in std::iter::once(controller).chain(plan.drones.iter().map(String::as_str)) {
        let device = required_device(&devices, code, "survey-fleet device")?;
        ensure_device_replicant(device, code, &config.replicant)?;

        if device_has_returned_to_vessel(
            device_stowed_in(device),
            device_location(device),
            &config.vessel,
            vessel_location,
        ) {
            continue;
        }

        let travel_active = device.is_traveling();
        let device_recalling = device_state_indicates_recall(
            device_status_name(device),
            device_location(device),
            device_stowed_in(device),
            travel_active,
        );
        recall_in_progress |= device_recalling;
        pending.push(format!(
            "{code}: status={}, location={}, stowed_in={}, travel_active={travel_active}, recalling={device_recalling}",
            device_status_name(device).unwrap_or("unknown"),
            device_location(device).unwrap_or("none"),
            device_stowed_in(device).unwrap_or("none")
        ));
    }

    Ok(FleetReturnInspection::Available(FleetReturnCheck {
        vessel_location: vessel_location.to_owned(),
        pending,
        recall_in_progress,
        controller_status: device_status_name(controller_device).map(str::to_owned),
        controller_directive_status: active_directive_status(controller_device).map(str::to_owned),
        controller_withdraw_available: device_has_command(controller_device, "withdraw"),
    }))
}

fn device_has_returned_to_vessel(
    stowed_in: Option<&str>,
    location: Option<&str>,
    vessel: &str,
    vessel_location: &str,
) -> bool {
    stowed_in == Some(vessel) || location == Some(vessel_location)
}

fn device_state_indicates_recall(
    status: Option<&str>,
    location: Option<&str>,
    stowed_in: Option<&str>,
    travel_active: bool,
) -> bool {
    travel_active
        || matches!(
            status,
            Some("recalling" | "returning" | "traveling" | "travelling" | "in_transit")
        )
        || (location.is_none() && stowed_in.is_none())
}

async fn current_star(client: &Client, replicant_code: &str) -> AnyResult<Option<String>> {
    let handle = client.replicants().get_owned(replicant_code).await?;
    let replicant = handle.snapshot().await?;
    Ok(replicant
        .location
        .as_ref()
        .map(|location| star_from_designation(location.id.as_str()).to_owned()))
}

fn star_from_designation(designation: &str) -> &str {
    designation.split('-').next().unwrap_or(designation)
}

fn designation_in_star(designation: &str, star: &str) -> bool {
    designation == star
        || designation
            .strip_prefix(star)
            .is_some_and(|suffix| suffix.starts_with('-'))
}

fn candidate_route_distance(start: GalacticPosition, route: &[CandidateStar]) -> f64 {
    let mut total = 0.0;
    let mut previous = start;
    for candidate in route {
        total += position_distance(previous, candidate.position);
        previous = candidate.position;
    }
    total
}

fn improve_candidate_route_2opt(
    start: GalacticPosition,
    route: &mut [CandidateStar],
    max_passes: usize,
) -> usize {
    if route.len() < 3 {
        return 0;
    }

    let mut swaps = 0_usize;
    for _ in 0..max_passes {
        let mut improved = false;
        for left in 0..route.len() - 1 {
            for right in left + 1..route.len() {
                let previous = if left == 0 {
                    start
                } else {
                    route[left - 1].position
                };
                let old_before = position_distance(previous, route[left].position);
                let new_before = position_distance(previous, route[right].position);

                let (old_after, new_after) = if right + 1 < route.len() {
                    let next = route[right + 1].position;
                    (
                        position_distance(route[right].position, next),
                        position_distance(route[left].position, next),
                    )
                } else {
                    (0.0, 0.0)
                };

                if new_before + new_after + 1e-9 < old_before + old_after {
                    route[left..=right].reverse();
                    swaps += 1;
                    improved = true;
                }
            }
        }
        if !improved {
            break;
        }
    }
    swaps
}

fn position_distance(left: GalacticPosition, right: GalacticPosition) -> f64 {
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let dz = left.z - right.z;
    (dx * dx + dy * dy + dz * dz).sqrt()
}

fn is_travel_event_for(event: &Event, config: &Config, target: &str) -> bool {
    if !matches!(event.name.as_str(), "travel.departed" | "travel.arrived") {
        return false;
    }
    let actor_matches = event
        .replicant
        .as_ref()
        .is_some_and(|replicant| replicant.id.as_str() == config.replicant)
        || event
            .device
            .as_ref()
            .is_some_and(|device| device.id.as_str() == config.vessel);
    actor_matches && event_in_star(event, target)
}

fn is_survey_directive_completion_for(event: &Event, controller: &str, target: &str) -> bool {
    if !matches!(event.name.as_str(), "directive.completed") {
        return false;
    }

    let controller_matches = event
        .device
        .as_ref()
        .is_some_and(|device| device.id.as_str() == controller)
        || json_reference_matches(event.payload.get("device"), controller)
        || json_reference_matches(event.payload.get("controller"), controller)
        || json_reference_matches(event.payload.get("device_code"), controller)
        || json_reference_matches(event.payload.get("controller_device_code"), controller);

    if !controller_matches {
        return false;
    }

    let directive_matches = event
        .payload
        .get("directive")
        .and_then(Value::as_str)
        .is_none_or(|directive| directive == "survey_system");

    if !directive_matches {
        return false;
    }

    // Some live directive-completion events omit normalized star/location
    // references. When they do provide location context, require it to match
    // the active route stop; otherwise the uniquely matched controller is
    // sufficient because it runs only one directive at a time.
    !event_has_location_context(event) || event_in_star(event, target)
}

#[derive(Debug, PartialEq, Eq)]
enum SurveyCompletionProof {
    TerminalDigest,
    DirectiveCompleted,
}

fn survey_completion_proof(
    event: &Event,
    controller: &str,
    target: &str,
) -> Option<SurveyCompletionProof> {
    if is_survey_digest_for(event, controller, target)
        && survey_progress(&event.payload).is_some_and(|(_, remaining, _)| remaining == 0)
    {
        Some(SurveyCompletionProof::TerminalDigest)
    } else if is_survey_directive_completion_for(event, controller, target) {
        Some(SurveyCompletionProof::DirectiveCompleted)
    } else {
        None
    }
}

fn survey_directive_needs_launch(status: Option<&str>) -> bool {
    matches!(
        status,
        Some("idle" | "inactive" | "completed" | "paused" | "stowed")
    )
}

fn survey_directive_needs_recall(status: Option<&str>) -> bool {
    status.is_some_and(|status| !survey_directive_needs_launch(Some(status)))
}

async fn confirm_survey_completion(
    client: &Client,
    config: &Config,
    target: &str,
) -> AnyResult<()> {
    let check = inspect_current_system_surveys(client, config, target).await?;
    info!(
        target: "replicant_client::explore",
        event = "survey.completion_reconciled",
        star = target,
        complete = check.complete,
        planets_total = check.planets_total,
        planets_scanned = check.planets_scanned,
        moons_total = check.moons_total,
        moons_scanned = check.moons_scanned,
        moons_total_estimated = check.moons_total_estimated,
        "authoritatively reconciled planet and moon survey completeness"
    );
    match check.complete {
        Some(true) => Ok(()),
        Some(false) => Err(app_error(io::ErrorKind::Other, format!(
            "survey completion evidence for {target} conflicts with authoritative planet/moon state"
        ))),
        None => Err(app_error(io::ErrorKind::Other, format!(
            "survey completion evidence for {target} needs a complete planet/moon reconciliation"
        ))),
    }
}

fn event_has_location_context(event: &Event) -> bool {
    event.star.is_some()
        || event.location.is_some()
        || event.payload.contains_key("destination")
        || event.payload.contains_key("star")
        || event.payload.contains_key("location")
}

fn json_reference_matches(value: Option<&Value>, expected: &str) -> bool {
    let Some(value) = value else {
        return false;
    };

    match value {
        Value::String(value) => value == expected,
        Value::Object(object) => ["designation", "code", "id", "device_code"]
            .into_iter()
            .filter_map(|key| object.get(key))
            .filter_map(Value::as_str)
            .any(|value| value == expected),
        _ => false,
    }
}

fn is_survey_digest_for(event: &Event, controller: &str, target: &str) -> bool {
    event.name.as_str() == "ami.survey.digest"
        && event
            .device
            .as_ref()
            .is_some_and(|device| device.id.as_str() == controller)
        && event.payload.get("directive").and_then(Value::as_str) == Some("survey_system")
        && event_in_star(event, target)
}

fn event_in_star(event: &Event, target: &str) -> bool {
    event
        .star
        .as_ref()
        .is_some_and(|star| star.id.as_str() == target)
        || event
            .location
            .as_ref()
            .is_some_and(|location| designation_in_star(location.id.as_str(), target))
        || payload_mentions_star(&event.payload, target)
}

fn payload_mentions_star(payload: &BTreeMap<String, Value>, target: &str) -> bool {
    payload_values_mention_star(payload.get("destination"), payload.get("star"), target)
}

fn payload_values_mention_star(
    destination: Option<&Value>,
    star: Option<&Value>,
    target: &str,
) -> bool {
    destination
        .and_then(Value::as_str)
        .is_some_and(|destination| designation_in_star(destination, target))
        || star.and_then(Value::as_str) == Some(target)
}

fn survey_progress(payload: &BTreeMap<String, Value>) -> Option<(u64, u64, u64)> {
    survey_progress_from_report(payload.get("report"))
}

fn survey_progress_from_report(report: Option<&Value>) -> Option<(u64, u64, u64)> {
    let progress = report?.get("progress")?.as_object()?;
    Some((
        progress.get("scanned")?.as_u64()?,
        progress.get("remaining")?.as_u64()?,
        progress.get("total")?.as_u64()?,
    ))
}

fn save_plan(path: &Path, plan: &RoutePlan) -> AnyResult<()> {
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
    fs::rename(&temporary, path)?;
    debug!(
        target: "replicant_client::explore",
        event = "route.plan_saved",
        path = %path.display(),
        next_index = plan.next_index,
        phase = ?plan.phase,
        "saved route plan"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_client::{DeviceId, DeviceKey, StarId, domain::StarKey};

    #[test]
    fn designation_matching_accepts_star_and_child_locations() {
        assert!(designation_in_star("SCEPTURUM", "SCEPTURUM"));
        assert!(designation_in_star("SCEPTURUM-2-L4", "SCEPTURUM"));
        assert!(!designation_in_star("SCEPTURUMA-2", "SCEPTURUM"));
    }

    #[test]
    fn distance_is_euclidean_in_three_dimensions() {
        let left = GalacticPosition {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        };
        let right = GalacticPosition {
            x: 3.0,
            y: 4.0,
            z: 12.0,
        };
        assert!((position_distance(left, right) - 13.0).abs() < f64::EPSILON);
    }

    #[test]
    fn two_opt_shortens_a_crossing_route() {
        let start = GalacticPosition {
            x: 0.0,
            y: 0.0,
            z: 0.0,
        };
        let mut route = vec![
            CandidateStar {
                star: "A".into(),
                entry_point: None,
                position: GalacticPosition {
                    x: 10.0,
                    y: 0.0,
                    z: 0.0,
                },
                distance_from_center_ly: 10.0,
            },
            CandidateStar {
                star: "B".into(),
                entry_point: None,
                position: GalacticPosition {
                    x: 0.0,
                    y: 10.0,
                    z: 0.0,
                },
                distance_from_center_ly: 10.0,
            },
            CandidateStar {
                star: "C".into(),
                entry_point: None,
                position: GalacticPosition {
                    x: 10.0,
                    y: 10.0,
                    z: 0.0,
                },
                distance_from_center_ly: 14.142_135_623_7,
            },
        ];
        let before = candidate_route_distance(start, &route);
        assert!(improve_candidate_route_2opt(start, &mut route, 8) > 0);
        assert!(candidate_route_distance(start, &route) < before);
    }

    #[test]
    fn already_explored_stop_can_advance_without_restow() {
        let stop = RouteStop {
            star: "SCEPTURUM".into(),
            entry_point: Some("SCEPTURUM-7-L4".into()),
            distance_from_center_ly: 0.0,
            leg_distance_ly: 0.0,
            survey_required: false,
            system_scan_done: true,
            survey_done: true,
        };
        assert!(stop.can_advance_without_restow());
    }

    #[test]
    fn normalization_advances_skipped_stops_without_restow() {
        let mut plan = RoutePlan {
            version: PLAN_VERSION,
            created_unix_seconds: 0,
            replicant: "B6BA399E".into(),
            vessel: "FD5EA802".into(),
            center: "SCEPTURUM".into(),
            radius_ly: 10.0,
            system_limit: 2,
            include_explored: false,
            controller: None,
            drones: Vec::new(),
            fleet_prepared: false,
            route: vec![
                RouteStop {
                    star: "SCEPTURUM".into(),
                    entry_point: Some("SCEPTURUM-7-L4".into()),
                    distance_from_center_ly: 0.0,
                    leg_distance_ly: 0.0,
                    survey_required: false,
                    system_scan_done: true,
                    survey_done: true,
                },
                RouteStop {
                    star: "NEXT".into(),
                    entry_point: None,
                    distance_from_center_ly: 1.0,
                    leg_distance_ly: 1.0,
                    survey_required: true,
                    system_scan_done: false,
                    survey_done: false,
                },
            ],
            next_index: 0,
            phase: RunPhase::PreparingFleet,
        };

        assert!(plan.normalize_progress());
        assert_eq!(plan.next_index, 1);
        assert_eq!(plan.phase, RunPhase::PreparingFleet);
    }

    #[test]
    fn startup_reconciliation_advances_scanned_unsurveyed_stop_to_surveying() {
        let mut plan = RoutePlan {
            version: PLAN_VERSION,
            created_unix_seconds: 0,
            replicant: "B6BA399E".into(),
            vessel: "FD5EA802".into(),
            center: "TEJUT".into(),
            radius_ly: 10.0,
            system_limit: 1,
            include_explored: false,
            controller: Some("CONTROLLER".into()),
            drones: vec!["D1".into(), "D2".into(), "D3".into(), "D4".into()],
            fleet_prepared: true,
            route: vec![RouteStop {
                star: "TEJUT".into(),
                entry_point: None,
                distance_from_center_ly: 0.0,
                leg_distance_ly: 0.0,
                survey_required: false,
                system_scan_done: false,
                survey_done: true,
            }],
            next_index: 0,
            phase: RunPhase::Ready,
        };

        assert!(apply_startup_current_system_completion(
            &mut plan,
            "TEJUT",
            true,
            Some(false)
        ));
        assert!(plan.route[0].system_scan_done);
        assert!(plan.route[0].survey_required);
        assert!(!plan.route[0].survey_done);
        assert!(!plan.stop_is_finalized(0));
        assert_eq!(plan.phase, RunPhase::Surveying);
    }

    #[test]
    fn startup_reconciliation_advances_fully_surveyed_stop_to_restowing() {
        let mut plan = RoutePlan {
            version: PLAN_VERSION,
            created_unix_seconds: 0,
            replicant: "B6BA399E".into(),
            vessel: "FD5EA802".into(),
            center: "TEJUT".into(),
            radius_ly: 10.0,
            system_limit: 1,
            include_explored: false,
            controller: Some("CONTROLLER".into()),
            drones: vec!["D1".into(), "D2".into(), "D3".into(), "D4".into()],
            fleet_prepared: true,
            route: vec![RouteStop {
                star: "TEJUT".into(),
                entry_point: None,
                distance_from_center_ly: 0.0,
                leg_distance_ly: 0.0,
                survey_required: true,
                system_scan_done: true,
                survey_done: false,
            }],
            next_index: 0,
            phase: RunPhase::Surveying,
        };

        assert!(apply_startup_current_system_completion(
            &mut plan,
            "TEJUT",
            true,
            Some(true)
        ));
        assert!(plan.route[0].system_scan_done);
        assert!(plan.route[0].survey_done);
        assert_eq!(plan.phase, RunPhase::Restowing);
    }

    #[test]
    fn startup_reconciliation_reopens_an_earlier_incomplete_survey_stop() {
        let mut plan = RoutePlan {
            version: PLAN_VERSION,
            created_unix_seconds: 0,
            replicant: "B6BA399E".into(),
            vessel: "FD5EA802".into(),
            center: "TEJUT".into(),
            radius_ly: 10.0,
            system_limit: 2,
            include_explored: false,
            controller: Some("CONTROLLER".into()),
            drones: vec!["D1".into(), "D2".into(), "D3".into(), "D4".into()],
            fleet_prepared: true,
            route: vec![
                RouteStop {
                    star: "FIRST".into(),
                    entry_point: None,
                    distance_from_center_ly: 1.0,
                    leg_distance_ly: 1.0,
                    survey_required: false,
                    system_scan_done: true,
                    survey_done: true,
                },
                RouteStop {
                    star: "SECOND".into(),
                    entry_point: None,
                    distance_from_center_ly: 2.0,
                    leg_distance_ly: 1.0,
                    survey_required: true,
                    system_scan_done: false,
                    survey_done: false,
                },
            ],
            next_index: 1,
            phase: RunPhase::Ready,
        };

        assert!(apply_startup_current_system_completion(
            &mut plan,
            "FIRST",
            true,
            Some(false)
        ));
        assert_eq!(plan.next_index, 0);
        assert!(plan.route[0].survey_required);
        assert!(!plan.route[0].survey_done);
        assert!(!plan.stop_is_finalized(0));
        assert_eq!(plan.phase, RunPhase::Surveying);
    }

    #[test]
    fn legacy_plan_migrates_using_next_index_as_finalized_boundary() {
        let json = r#"{
            "version": 1,
            "created_unix_seconds": 0,
            "replicant": "B6BA399E",
            "vessel": "FD5EA802",
            "center": "TEJUT",
            "radius_ly": 10.0,
            "system_limit": 2,
            "include_explored": false,
            "controller": null,
            "drones": [],
            "fleet_prepared": false,
            "route": [
                {
                    "star": "FIRST",
                    "entry_point": null,
                    "distance_from_center_ly": 0.0,
                    "leg_distance_ly": 0.0,
                    "survey_required": false,
                    "system_scan_done": true,
                    "survey_done": true,
                    "completed": true
                },
                {
                    "star": "SECOND",
                    "entry_point": null,
                    "distance_from_center_ly": 1.0,
                    "leg_distance_ly": 1.0,
                    "survey_required": true,
                    "system_scan_done": true,
                    "survey_done": true,
                    "completed": false
                }
            ],
            "next_index": 1,
            "phase": "restowing"
        }"#;

        let mut plan: RoutePlan = serde_json::from_str(json).expect("legacy plan should decode");
        assert!(plan.migrate().expect("legacy plan should migrate"));
        assert_eq!(plan.version, PLAN_VERSION);
        assert!(plan.stop_is_finalized(0));
        assert!(!plan.stop_is_finalized(1));
        assert_eq!(plan.phase, RunPhase::Restowing);
        assert!(!serde_json::to_string(&plan)
            .expect("plan should serialize")
            .contains("completed"));
    }

    #[test]
    fn normalization_does_not_skip_a_surveyed_stop_waiting_for_restow() {
        let mut plan = RoutePlan {
            version: PLAN_VERSION,
            created_unix_seconds: 0,
            replicant: "B6BA399E".into(),
            vessel: "FD5EA802".into(),
            center: "TEJUT".into(),
            radius_ly: 10.0,
            system_limit: 1,
            include_explored: false,
            controller: Some("CONTROLLER".into()),
            drones: vec!["D1".into(), "D2".into(), "D3".into()],
            fleet_prepared: true,
            route: vec![RouteStop {
                star: "TEJUT".into(),
                entry_point: None,
                distance_from_center_ly: 0.0,
                leg_distance_ly: 0.0,
                survey_required: true,
                system_scan_done: true,
                survey_done: true,
            }],
            next_index: 0,
            phase: RunPhase::Restowing,
        };

        assert!(!plan.normalize_progress());
        assert_eq!(plan.next_index, 0);
        assert_eq!(plan.phase, RunPhase::Restowing);
    }

    #[test]
    fn survey_counter_completion_requires_exact_authoritative_counts() {
        assert_eq!(exact_count_complete(Some(4), Some(4)), Some(true));
        assert_eq!(exact_count_complete(Some(4), Some(3)), Some(false));
        assert_eq!(exact_count_complete(Some(4), Some(5)), Some(false));
        assert_eq!(exact_count_complete(Some(0), Some(0)), Some(true));
        assert_eq!(exact_count_complete(Some(0), None), None);
        assert_eq!(exact_count_complete(None, Some(3)), None);
    }

    #[test]
    fn aggregate_survey_completion_requires_planets_and_exact_moons() {
        assert_eq!(
            aggregate_survey_counts_complete(Some(true), Some(true)),
            Some(true)
        );
        assert_eq!(
            aggregate_survey_counts_complete(Some(false), Some(true)),
            Some(false)
        );
        assert_eq!(
            aggregate_survey_counts_complete(Some(true), Some(false)),
            Some(false)
        );
        assert_eq!(aggregate_survey_counts_complete(Some(true), None), None);
    }

    #[test]
    fn instant_system_scan_accepts_only_decoded_success_states() {
        assert!(system_scan_response_was_ok(
            OperationStatus::ReconciliationRequired
        ));
        assert!(system_scan_response_was_ok(OperationStatus::Completed));
        assert!(!system_scan_response_was_ok(OperationStatus::Ambiguous));
        assert!(!system_scan_response_was_ok(OperationStatus::Rejected));
    }

    #[test]
    fn managed_history_directive_completion_is_terminal() {
        let event = Event {
            id: replicant_client::EventId::from("1785276160433-0"),
            realm: Some(Realm::Live),
            name: replicant_client::domain::EventName::from("directive.completed"),
            category: replicant_client::domain::EventCategory::from("directive"),
            device: Some(DeviceKey::in_realm(
                Realm::Live,
                DeviceId::from("76C57506"),
            )),
            replicant: None,
            location: Some(replicant_client::LocationKey::in_realm(
                Realm::Live,
                replicant_client::LocationId::from("KRUKKRAK-1-L4"),
            )),
            star: Some(StarKey::in_realm(Realm::Live, StarId::from("KRUKKRAK"))),
            occurred_at: "2026-07-28T22:02:40Z".into(),
            payload: [("directive".into(), serde_json::json!("survey_system"))]
                .into_iter()
                .collect(),
        };

        assert_eq!(
            survey_completion_proof(&event, "76C57506", "KRUKKRAK"),
            Some(SurveyCompletionProof::DirectiveCompleted)
        );
        assert_eq!(
            survey_completion_proof(&event, "76C57506", "OTHER"),
            None
        );
    }

    #[test]
    fn managed_history_terminal_digest_is_supported() {
        let event = Event {
            id: replicant_client::EventId::from("1785276160434-0"),
            realm: Some(Realm::Live),
            name: replicant_client::domain::EventName::from("ami.survey.digest"),
            category: replicant_client::domain::EventCategory::from("ami"),
            device: Some(DeviceKey::in_realm(
                Realm::Live,
                DeviceId::from("76C57506"),
            )),
            replicant: None,
            location: Some(replicant_client::LocationKey::in_realm(
                Realm::Live,
                replicant_client::LocationId::from("KRUKKRAK-1-L4"),
            )),
            star: Some(StarKey::in_realm(Realm::Live, StarId::from("KRUKKRAK"))),
            occurred_at: "2026-07-28T22:02:41Z".into(),
            payload: [
                ("directive".into(), serde_json::json!("survey_system")),
                (
                    "report".into(),
                    serde_json::json!({
                        "progress": {
                            "scanned": 5,
                            "remaining": 0,
                            "total": 5
                        }
                    }),
                ),
            ]
            .into_iter()
            .collect(),
        };

        assert_eq!(
            survey_completion_proof(&event, "76C57506", "KRUKKRAK"),
            Some(SurveyCompletionProof::TerminalDigest)
        );
    }

    #[test]
    fn directive_completed_is_the_terminal_survey_event() {
        let event = Event {
            id: replicant_client::EventId::from("2-0"),
            realm: Some(Realm::Live),
            name: replicant_client::domain::EventName::from("directive.completed"),
            category: replicant_client::domain::EventCategory::from("device"),
            device: Some(DeviceKey::in_realm(
                Realm::Live,
                DeviceId::from("CONTROLLER"),
            )),
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-27T00:00:00Z".into(),
            payload: [("directive".into(), serde_json::json!("survey_system"))]
                .into_iter()
                .collect(),
        };

        assert!(is_survey_directive_completion_for(
            &event,
            "CONTROLLER",
            "TEJUT"
        ));
        assert_eq!(
            survey_completion_proof(&event, "CONTROLLER", "TEJUT"),
            Some(SurveyCompletionProof::DirectiveCompleted)
        );
    }

    #[test]
    fn directive_completed_payload_reference_is_supported() {
        let event = Event {
            id: replicant_client::EventId::from("3-0"),
            realm: Some(Realm::Live),
            name: replicant_client::domain::EventName::from("directive.completed"),
            category: replicant_client::domain::EventCategory::from("device"),
            device: None,
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-27T00:00:00Z".into(),
            payload: [
                (
                    "controller".into(),
                    serde_json::json!({"code": "CONTROLLER"}),
                ),
                ("directive".into(), serde_json::json!("survey_system")),
                ("star".into(), serde_json::json!("TEJUT")),
            ]
            .into_iter()
            .collect(),
        };

        assert!(is_survey_directive_completion_for(
            &event,
            "CONTROLLER",
            "TEJUT"
        ));
        assert!(!is_survey_directive_completion_for(
            &event, "OTHER", "TEJUT"
        ));
    }

    #[test]
    fn survey_completion_proofs_are_order_independent() {
        let directive_completed = Event {
            id: replicant_client::EventId::from("directive-1"),
            realm: Some(Realm::Live),
            name: replicant_client::domain::EventName::from("directive.completed"),
            category: replicant_client::domain::EventCategory::from("device"),
            device: Some(DeviceKey::in_realm(
                Realm::Live,
                DeviceId::from("CONTROLLER"),
            )),
            replicant: None,
            location: None,
            star: Some(StarKey::in_realm(Realm::Live, StarId::from("TEJUT"))),
            occurred_at: "2026-07-27T00:00:00Z".into(),
            payload: [("directive".into(), serde_json::json!("survey_system"))]
                .into_iter()
                .collect(),
        };
        let terminal_digest = Event {
            id: replicant_client::EventId::from("digest-1"),
            realm: Some(Realm::Live),
            name: replicant_client::domain::EventName::from("ami.survey.digest"),
            category: replicant_client::domain::EventCategory::from("ami"),
            device: Some(DeviceKey::in_realm(
                Realm::Live,
                DeviceId::from("CONTROLLER"),
            )),
            replicant: None,
            location: None,
            star: Some(StarKey::in_realm(Realm::Live, StarId::from("TEJUT"))),
            occurred_at: "2026-07-27T00:00:01Z".into(),
            payload: [
                ("directive".into(), serde_json::json!("survey_system")),
                (
                    "report".into(),
                    serde_json::json!({"progress": {"scanned": 4, "remaining": 0, "total": 4}}),
                ),
            ]
            .into_iter()
            .collect(),
        };
        let completion_before_digest = [directive_completed.clone(), terminal_digest.clone()]
            .iter()
            .find_map(|event| survey_completion_proof(event, "CONTROLLER", "TEJUT"));
        let digest_before_completion = [terminal_digest.clone(), directive_completed.clone()]
            .iter()
            .find_map(|event| survey_completion_proof(event, "CONTROLLER", "TEJUT"));

        assert_eq!(
            completion_before_digest,
            Some(SurveyCompletionProof::DirectiveCompleted)
        );
        assert_eq!(
            digest_before_completion,
            Some(SurveyCompletionProof::TerminalDigest)
        );
        let scan_event = Event {
            name: replicant_client::domain::EventName::from("scan.completed"),
            ..terminal_digest
        };
        assert_eq!(
            survey_completion_proof(&scan_event, "CONTROLLER", "TEJUT"),
            None,
            "individual body scans are never system-scan completion evidence"
        );
    }

    #[test]
    fn survey_progress_reads_digest_shape() {
        let event = Event {
            id: replicant_client::EventId::from("1-0"),
            realm: Some(Realm::Live),
            name: replicant_client::domain::EventName::from("ami.survey.digest"),
            category: replicant_client::domain::EventCategory::from("device"),
            device: None,
            replicant: None,
            location: None,
            star: None,
            occurred_at: "2026-07-27T00:00:00Z".into(),
            payload: [(
                "report".into(),
                serde_json::json!({
                    "progress": {"scanned": 28, "remaining": 8, "total": 36}
                }),
            )]
            .into_iter()
            .collect(),
        };
        assert_eq!(survey_progress(&event.payload), Some((28, 8, 36)));
    }

    #[test]
    fn fleet_return_rate_limit_delay_never_retries_immediately() {
        assert_eq!(
            fleet_return_rate_limit_delay(None),
            Duration::from_secs(15)
        );
        assert_eq!(
            fleet_return_rate_limit_delay(Some(Duration::ZERO)),
            Duration::from_secs(15)
        );
        assert_eq!(
            fleet_return_rate_limit_delay(Some(Duration::from_secs(5))),
            Duration::from_secs(15)
        );
        assert_eq!(
            fleet_return_rate_limit_delay(Some(Duration::from_secs(30))),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn rejected_stow_requires_authoritative_state_verification() {
        assert!(stow_operation_is_terminal_failure(
            OperationStatus::Rejected
        ));
        assert!(stow_operation_is_terminal_failure(
            OperationStatus::Cancelled
        ));
        assert!(stow_operation_is_terminal_failure(OperationStatus::Failed));
        assert!(!stow_operation_is_terminal_failure(
            OperationStatus::ReconciliationRequired
        ));
        assert!(!stow_operation_is_terminal_failure(
            OperationStatus::Completed
        ));
    }

    #[test]
    fn fleet_device_is_returned_when_at_or_inside_vessel() {
        assert!(device_has_returned_to_vessel(
            Some("VESSEL"),
            None,
            "VESSEL",
            "STAR-1-L4"
        ));
        assert!(device_has_returned_to_vessel(
            None,
            Some("STAR-1-L4"),
            "VESSEL",
            "STAR-1-L4"
        ));
        assert!(!device_has_returned_to_vessel(
            None,
            None,
            "VESSEL",
            "STAR-1-L4"
        ));
        assert!(!device_has_returned_to_vessel(
            None,
            Some("STAR-2"),
            "VESSEL",
            "STAR-1-L4"
        ));
    }

    #[test]
    fn missing_location_during_recall_is_pending_not_a_stow_error() {
        assert!(device_state_indicates_recall(
            Some("recalling"),
            None,
            None,
            false
        ));
        assert!(device_state_indicates_recall(
            Some("active"),
            None,
            None,
            false
        ));
        assert!(device_state_indicates_recall(
            Some("active"),
            Some("STAR-2"),
            None,
            true
        ));
        assert!(!device_state_indicates_recall(
            Some("idle"),
            Some("STAR-1-L4"),
            None,
            false
        ));
    }

    #[test]
    fn active_controller_state_does_not_block_launch_or_recall_decisions() {
        assert!(survey_directive_needs_launch(Some("inactive")));
        assert!(!survey_directive_needs_launch(Some("active")));
        assert!(survey_directive_needs_recall(Some("active")));
        assert!(!survey_directive_needs_recall(Some("inactive")));
    }

    #[test]
    fn restart_while_restowing_does_not_resubmit_withdraw() {
        let statuses = ["active", "inactive"];
        let unsafe_withdraws = statuses
            .into_iter()
            .filter(|status| survey_directive_needs_recall(Some(status)))
            .count();
        assert_eq!(unsafe_withdraws, 1);
    }
}
