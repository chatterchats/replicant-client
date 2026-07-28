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
    Client, DeviceType, Event, Operation, OperationStatus, Realm, SecretString, StartupPolicy,
    SurveyDirective, domain::GalacticPosition, raw::devices::DeviceCommand,
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

const PLAN_VERSION: u32 = 1;
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
    token: String,
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
        let token = env::var("RS_API_TOKEN")
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "RS_API_TOKEN is required"))?;

        let drone_overrides = env::var("RS_EXPLORE_DRONES").ok().map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        });

        if let Some(drones) = &drone_overrides
            && drones.len() != DRONE_COUNT
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "RS_EXPLORE_DRONES must contain exactly {DRONE_COUNT} comma-separated codes"
                ),
            )
            .into());
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
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be 1/0, true/false, yes/no, or on/off"),
            )
            .into()),
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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("{name} must be a positive finite number"),
                )
                .into());
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
        .map_err(|error| io::Error::other(error.to_string()))?;

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
    completed: bool,
}

impl RouteStop {
    fn is_already_complete(&self) -> bool {
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
    last_event_id: Option<String>,
}

impl RoutePlan {
    fn validate(&self, config: &Config) -> AnyResult<()> {
        if self.version != PLAN_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "unsupported route plan version {}; expected {PLAN_VERSION}",
                    self.version
                ),
            )
            .into());
        }
        if self.replicant != config.replicant
            || self.vessel != config.vessel
            || self.center != config.center
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "existing route plan targets a different replicant, vessel, or centre; set RS_EXPLORE_REBUILD_PLAN=1 to replace it",
            )
            .into());
        }
        if self.next_index > self.route.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "route plan next_index exceeds route length",
            )
            .into());
        }
        Ok(())
    }

    fn normalize_progress(&mut self) -> bool {
        let mut changed = false;

        for stop in &mut self.route {
            if stop.is_already_complete() && !stop.completed {
                stop.completed = true;
                changed = true;
            }
        }

        let first_incomplete = self
            .route
            .iter()
            .position(|stop| !stop.completed)
            .unwrap_or(self.route.len());
        if self.next_index != first_incomplete {
            self.next_index = first_incomplete;
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
        .authentication_token(SecretString::from(config.token.clone()))
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
    } else {
        if phase_requires_stowed_fleet(plan.phase) {
            let controller = plan
                .controller
                .as_deref()
                .ok_or_else(|| io::Error::other("route plan has no survey controller"))?;
            ensure_replicant_owns_device(client, controller, &config.replicant).await?;
            for code in &plan.drones {
                ensure_replicant_owns_device(client, code, &config.replicant).await?;
            }
            stow_fleet(client, config, &plan).await?;
        }
        verify_fleet_plan(client, config, &plan).await?;
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

    if plan.route[route_index].completed {
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
                return Err(io::Error::other(format!(
                    "unable to verify whether the current system {current_star} was scanned; refusing to risk a duplicate system_scan command: {refresh_error}"
                ))
                .into());
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
        completed = plan.route[route_index].completed,
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
        .raw()
        .locations()
        .get(current_star, Some(&config.replicant))
        .await?
        .value;

    let planets_complete = exact_count_complete(root.planets_total, root.planets_scanned);
    let moons_complete = match root.moons_total_estimated {
        Some(false) => exact_count_complete(root.moons_total, root.moons_scanned),
        Some(true) => Some(false),
        None => None,
    };
    let complete = aggregate_survey_counts_complete(planets_complete, moons_complete);

    info!(
        target: "replicant_client::explore",
        event = "startup.planetary_survey_inspected",
        star = current_star,
        complete,
        planets_total = root.planets_total,
        planets_scanned = root.planets_scanned,
        planets_complete,
        moons_total = root.moons_total,
        moons_scanned = root.moons_scanned,
        moons_total_estimated = root.moons_total_estimated,
        moons_complete,
        elapsed_ms = started.elapsed().as_millis() as u64,
        "inspected current-system survey completeness from aggregate counters"
    );

    Ok(CurrentSystemSurveyCheck {
        complete,
        planets_total: root.planets_total,
        planets_scanned: root.planets_scanned,
        moons_total: root.moons_total,
        moons_scanned: root.moons_scanned,
        moons_total_estimated: root.moons_total_estimated,
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
                if stop.completed {
                    stop.completed = false;
                    changed = true;
                }
            }
            None => {}
        }

        if !stop.survey_required && stop.system_scan_done && stop.survey_done && !stop.completed {
            stop.completed = true;
            changed = true;
        }
    }

    if planetary_surveys_complete == Some(false) && route_index < plan.next_index {
        plan.next_index = route_index;
        changed = true;
    }

    if route_index == plan.next_index {
        if plan.route[route_index].completed {
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
        plan.validate(config)?;
        if plan.normalize_progress() {
            save_plan(&config.plan_path, &plan)?;
            info!(
                target: "replicant_client::explore",
                event = "route.plan_normalized",
                next_index = plan.next_index,
                phase = ?plan.phase,
                "normalized legacy or internally inconsistent route progress"
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

    if client.galaxy().catalogue().is_empty() {
        info!(
            target: "replicant_client::explore",
            event = "route.catalogue_refresh_required",
            "no durable star catalogue is available; refreshing it"
        );
        client.galaxy().refresh_catalogue().await?;
    }

    let catalogue = client.galaxy().catalogue();
    let center = catalogue
        .iter()
        .find(|star| star.key.id.as_str() == config.center)
        .cloned()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "centre star `{}` is absent from the catalogue",
                    config.center
                ),
            )
        })?;
    let center_position = center.position.ok_or_else(|| {
        io::Error::new(
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
        completed: !center_survey_required,
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
            completed: !survey_required,
        });
    }

    let next_index = route
        .iter()
        .position(|stop| !stop.completed)
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
        last_event_id: None,
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
            completed = stop.completed,
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
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "replicant {} is hosted by {:?}, not vessel {}",
                config.replicant, hosted_vessel, config.vessel
            ),
        )
        .into());
    }

    let vessel = client.devices().get(&config.vessel).await?;
    let vessel_snapshot = vessel.snapshot().await?;
    if vessel_snapshot
        .device_type
        .as_ref()
        .map(|value| value.as_str())
        != Some("racing_vessel")
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("device {} is not a racing_vessel", config.vessel),
        )
        .into());
    }
    let location = vessel_snapshot.location.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "racing vessel has no current location",
        )
    })?;
    let location_id = location.id.as_str().to_owned();

    let vessel_status = client.raw().devices().get(&config.vessel).await?.value;
    ensure_device_replicant(&vessel_status, &config.vessel, &config.replicant)?;

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
            let status = client.raw().devices().get(code).await?.value;
            validate_controller_candidate(&status, code, &location_id, &config.vessel)?;
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
                let status = client.raw().devices().get(&code).await?.value;
                match validate_controller_candidate(&status, &code, &location_id, &config.vessel) {
                    Ok(()) => eligible.push((
                        status.replicant_code.as_deref() == Some(config.replicant.as_str()),
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
                    io::Error::new(
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

    if plan.drones.is_empty() {
        let controller_code = plan
            .controller
            .as_deref()
            .ok_or_else(|| io::Error::other("route plan has no selected controller"))?;
        let drones = if let Some(codes) = &config.drone_overrides {
            let unique = codes.iter().collect::<BTreeSet<_>>();
            if unique.len() != DRONE_COUNT {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("RS_EXPLORE_DRONES must contain {DRONE_COUNT} distinct device codes"),
                )
                .into());
            }
            if codes.iter().any(|code| code == controller_code) {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "the survey controller cannot also be selected as a survey drone",
                )
                .into());
            }
            for code in codes {
                ensure_account_owned(&account_owned_devices, code)?;
                let status = client.raw().devices().get(code).await?.value;
                validate_drone_candidate(
                    &status,
                    code,
                    controller_code,
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
                let status = client.raw().devices().get(&code).await?.value;
                match validate_drone_candidate(
                    &status,
                    &code,
                    controller_code,
                    &location_id,
                    &config.vessel,
                ) {
                    Ok(()) => eligible.push((
                        status.replicant_code.as_deref() == Some(config.replicant.as_str()),
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
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "only {} eligible account-owned survey drones are available at {} or already stowed in vessel {}; need {DRONE_COUNT}",
                        selected.len(), location_id, config.vessel
                    ),
                )
                .into());
            }
            selected
        };
        plan.drones = drones;
        save_plan(&config.plan_path, plan)?;
    }

    let controller_code = plan
        .controller
        .as_deref()
        .ok_or_else(|| io::Error::other("route plan has no selected controller"))?;
    ensure_replicant_owns_device(client, controller_code, &config.replicant).await?;
    for code in &plan.drones {
        ensure_replicant_owns_device(client, code, &config.replicant).await?;
    }

    verify_fleet_plan(client, config, plan).await?;

    let controller_code = plan
        .controller
        .as_deref()
        .ok_or_else(|| io::Error::other("route plan has no selected controller"))?;
    let controller_handle = client.devices().get(controller_code).await?;
    let controller = controller_handle.as_survey_controller()?;

    let mut needs_adoption = Vec::new();
    for code in &plan.drones {
        let status = client.raw().devices().get(code).await?.value;
        match status.controller_device_code.as_deref() {
            Some(actual) if actual == controller_code => {}
            Some(actual) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("drone {code} is already controlled by {actual}"),
                )
                .into());
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
            let refreshed = client.raw().devices().get(code).await?.value;
            if refreshed.controller_device_code.as_deref() != Some(controller_code) {
                return Err(io::Error::other(format!(
                    "drone {code} did not report controller {controller_code} after adoption"
                ))
                .into());
            }
        }
    }

    let controller_status = client.raw().devices().get(controller_code).await?.value;
    if !has_survey_system_directive(&controller_status.ami_directive) {
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

async fn verify_fleet_plan(client: &Client, config: &Config, plan: &RoutePlan) -> AnyResult<()> {
    let controller = plan
        .controller
        .as_deref()
        .ok_or_else(|| io::Error::other("route plan has no survey controller"))?;
    if plan.drones.len() != DRONE_COUNT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "route plan must contain exactly {DRONE_COUNT} survey drones; found {}",
                plan.drones.len()
            ),
        )
        .into());
    }

    let controller_status = client.raw().devices().get(controller).await?.value;
    if controller_status.device_type.as_deref() != Some("ami_survey_controller") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("planned controller {controller} is not an ami_survey_controller"),
        )
        .into());
    }

    let vessel_status = client.raw().devices().get(&config.vessel).await?.value;
    if vessel_status.device_type.as_deref() != Some("racing_vessel") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("planned vessel {} is not a racing_vessel", config.vessel),
        )
        .into());
    }
    ensure_device_replicant(&vessel_status, &config.vessel, &config.replicant)?;
    ensure_device_replicant(&controller_status, controller, &config.replicant)?;

    let fleet_must_be_stowed = phase_requires_stowed_fleet(plan.phase);
    if fleet_must_be_stowed {
        ensure_stowed_in_vessel(&controller_status, controller, &config.vessel)?;
    }

    for code in &plan.drones {
        let status = client.raw().devices().get(code).await?.value;
        if status.device_type.as_deref() != Some("survey_drone") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("planned drone {code} is not a survey_drone"),
            )
            .into());
        }
        ensure_device_replicant(&status, code, &config.replicant)?;
        if fleet_must_be_stowed {
            ensure_stowed_in_vessel(&status, code, &config.vessel)?;
        }
        if plan.fleet_prepared && status.controller_device_code.as_deref() != Some(controller) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("prepared drone {code} is no longer adopted by controller {controller}"),
            )
            .into());
        }
    }

    if plan.fleet_prepared && !has_survey_system_directive(&controller_status.ami_directive) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("prepared controller {controller} no longer has survey_system configured"),
        )
        .into());
    }

    Ok(())
}

fn phase_requires_stowed_fleet(phase: RunPhase) -> bool {
    matches!(
        phase,
        RunPhase::Ready | RunPhase::Traveling | RunPhase::SystemScanning | RunPhase::Complete
    )
}

fn ensure_account_owned(account_owned: &BTreeSet<String>, code: &str) -> AnyResult<()> {
    if account_owned.contains(code) {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::PermissionDenied,
        format!("device {code} is not present in the authenticated account's owned-device set"),
    )
    .into())
}

fn validate_controller_candidate(
    status: &replicant_client::raw::devices::DeviceStatus,
    code: &str,
    vessel_location: &str,
    vessel: &str,
) -> AnyResult<()> {
    if status.device_type.as_deref() != Some("ami_survey_controller") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("device {code} is not an ami_survey_controller"),
        )
        .into());
    }
    if status.status.as_deref() != Some("idle") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("survey controller {code} is not idle"),
        )
        .into());
    }
    if !status.controlled_devices.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("survey controller {code} already controls one or more devices"),
        )
        .into());
    }
    validate_device_vessel_placement(status, code, vessel_location, vessel)
}

fn validate_drone_candidate(
    status: &replicant_client::raw::devices::DeviceStatus,
    code: &str,
    controller: &str,
    vessel_location: &str,
    vessel: &str,
) -> AnyResult<()> {
    if status.device_type.as_deref() != Some("survey_drone") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("device {code} is not a survey_drone"),
        )
        .into());
    }
    if status.status.as_deref() != Some("idle") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("survey drone {code} is not idle"),
        )
        .into());
    }
    if let Some(actual) = status.controller_device_code.as_deref()
        && actual != controller
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("survey drone {code} is already controlled by {actual}"),
        )
        .into());
    }
    validate_device_vessel_placement(status, code, vessel_location, vessel)
}

async fn ensure_replicant_owns_device(
    client: &Client,
    code: &str,
    replicant: &str,
) -> AnyResult<()> {
    let status = client.raw().devices().get(code).await?.value;
    if status.replicant_code.as_deref() == Some(replicant) {
        debug!(
            target: "replicant_client::explore",
            event = "fleet.owner_verified",
            device = code,
            replicant,
            "device is already assigned to the target replicant"
        );
        return Ok(());
    }

    if !status
        .available_commands
        .iter()
        .any(|command| command == "change_owner")
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "device {code} is assigned to {:?} and does not advertise change_owner",
                status.replicant_code.as_deref()
            ),
        )
        .into());
    }

    info!(
        target: "replicant_client::explore",
        event = "fleet.owner_change_started",
        device = code,
        previous_replicant = status.replicant_code.as_deref().unwrap_or("unassigned"),
        target_replicant = replicant,
        "transferring device to the target replicant"
    );
    let handle = client.devices().get(code).await?;
    let operation = handle
        .command(DeviceCommand::ChangeOwner {
            target: replicant.to_owned(),
        })
        .await?;
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
        let refreshed = client.raw().devices().get(code).await?.value;
        last_replicant.clone_from(&refreshed.replicant_code);

        if refreshed.replicant_code.as_deref() == Some(replicant) {
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
                "verified device ownership transfer from authoritative device state"
            );
            return Ok(());
        }

        let operation_status = operation.status().await?;
        match operation_status {
            OperationStatus::Rejected | OperationStatus::Cancelled | OperationStatus::Failed => {
                return Err(io::Error::other(format!(
                    "change_owner for device {code} ended with {operation_status:?}; device still reports replicant_code={last_replicant:?}"
                ))
                .into());
            }
            _ => {}
        }

        if started.elapsed() >= VERIFY_TIMEOUT {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "device {code} did not report replicant_code={replicant} within {VERIFY_TIMEOUT:?}; last_replicant={last_replicant:?}, operation_status={operation_status:?}, operation_id={} (rerun is safe)",
                    operation.id()
                ),
            )
            .into());
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
            "ownership transfer is accepted but not yet visible in authoritative device state"
        );

        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(MAX_DELAY);
    }
}

fn ensure_device_replicant(
    status: &replicant_client::raw::devices::DeviceStatus,
    code: &str,
    expected_replicant: &str,
) -> AnyResult<()> {
    match status.replicant_code.as_deref() {
        Some(actual) if actual == expected_replicant => Ok(()),
        Some(actual) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "device {code} reports replicant_code={actual}, expected {expected_replicant}"
            ),
        )
        .into()),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "device {code} does not report replicant ownership/hosting; expected {expected_replicant}"
            ),
        )
        .into()),
    }
}

fn validate_device_vessel_placement(
    status: &replicant_client::raw::devices::DeviceStatus,
    code: &str,
    vessel_location: &str,
    vessel: &str,
) -> AnyResult<()> {
    match status.stowed_in_device_code.as_deref() {
        Some(actual) if actual == vessel => Ok(()),
        Some(actual) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "device {code} is stowed in vessel {actual}, not required vessel {vessel}"
            ),
        )
        .into()),
        None if status.location.as_deref() == Some(vessel_location) => Ok(()),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "device {code} is not stowed in vessel {vessel} and is at {:?}, while the vessel is at {vessel_location}",
                status.location.as_deref()
            ),
        )
        .into()),
    }
}

fn ensure_stowed_in_vessel(
    status: &replicant_client::raw::devices::DeviceStatus,
    code: &str,
    vessel: &str,
) -> AnyResult<()> {
    if status.stowed_in_device_code.as_deref() == Some(vessel) {
        return Ok(());
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        format!(
            "device {code} is not stowed in required vessel {vessel}; reported stowed_in_device_code={:?}",
            status.stowed_in_device_code.as_deref()
        ),
    )
    .into())
}

fn has_survey_system_directive(directive: &Option<replicant_client::raw::JsonObject>) -> bool {
    directive.as_ref().is_some_and(|directive| {
        directive
            .get("directive")
            .or_else(|| directive.get("name"))
            .and_then(Value::as_str)
            == Some("survey_system")
    })
}

async fn stow_fleet(client: &Client, config: &Config, plan: &RoutePlan) -> AnyResult<()> {
    let controller = plan
        .controller
        .as_deref()
        .ok_or_else(|| io::Error::other("route plan has no survey controller"))?;

    let mut codes = plan.drones.clone();
    codes.push(controller.to_owned());

    let vessel = client.raw().devices().get(&config.vessel).await?.value;
    ensure_device_replicant(&vessel, &config.vessel, &config.replicant)?;
    let vessel_location = vessel.location.as_deref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("vessel {} has no current location", config.vessel),
        )
    })?;
    let capacity = vessel.stow_capacity.unwrap_or(5);
    let used = vessel.stow_used.unwrap_or(0);
    let mut missing = Vec::new();

    for code in &codes {
        let status = client.raw().devices().get(code).await?.value;
        ensure_device_replicant(&status, code, &config.replicant)?;
        validate_device_vessel_placement(&status, code, vessel_location, &config.vessel)?;
        if status.stowed_in_device_code.as_deref() != Some(config.vessel.as_str()) {
            missing.push(code.clone());
        }
    }

    if used + i64::try_from(missing.len())? > capacity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "vessel {} has stow capacity {capacity}, currently uses {used}, and needs {} more slots",
                config.vessel,
                missing.len()
            ),
        )
        .into());
    }

    for code in missing {
        info!(
            target: "replicant_client::explore",
            event = "fleet.stow_started",
            device = code,
            vessel = %config.vessel,
            "stowing survey-fleet device"
        );
        let handle = client.devices().get(&code).await?;
        let operation = handle.stow(Some(config.vessel.clone())).await?;
        wait_immediate_operation("stow fleet device", &operation).await?;

        let status = client.raw().devices().get(&code).await?.value;
        ensure_device_replicant(&status, &code, &config.replicant)?;
        ensure_stowed_in_vessel(&status, &code, &config.vessel)?;
    }

    for code in &codes {
        let status = client.raw().devices().get(code).await?.value;
        ensure_device_replicant(&status, code, &config.replicant)?;
        ensure_stowed_in_vessel(&status, code, &config.vessel)?;
    }

    info!(
        target: "replicant_client::explore",
        event = "fleet.stow_verified",
        replicant = %config.replicant,
        vessel = %config.vessel,
        devices = ?codes,
        "verified that the complete survey fleet belongs to the target replicant and is stowed in the correct vessel"
    );

    Ok(())
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
            Err(io::Error::other(format!("{label} ended with {:?}", outcome.status)).into())
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
                    plan.route[index].completed = true;
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
                plan.route[index].completed = true;
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

async fn verify_fleet_onboard_for_travel(
    client: &Client,
    config: &Config,
    plan: &RoutePlan,
) -> AnyResult<()> {
    let controller = plan
        .controller
        .as_deref()
        .ok_or_else(|| io::Error::other("route plan has no survey controller"))?;
    if plan.drones.len() != DRONE_COUNT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "route plan must contain exactly {DRONE_COUNT} survey drones before travel; found {}",
                plan.drones.len()
            ),
        )
        .into());
    }

    let vessel = client.raw().devices().get(&config.vessel).await?.value;
    if vessel.device_type.as_deref() != Some("racing_vessel") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("configured vessel {} is not a racing_vessel", config.vessel),
        )
        .into());
    }
    ensure_device_replicant(&vessel, &config.vessel, &config.replicant)?;

    let controller_status = client.raw().devices().get(controller).await?.value;
    if controller_status.device_type.as_deref() != Some("ami_survey_controller") {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("planned controller {controller} is not an ami_survey_controller"),
        )
        .into());
    }
    ensure_device_replicant(&controller_status, controller, &config.replicant)?;
    ensure_stowed_in_vessel(&controller_status, controller, &config.vessel)?;

    for code in &plan.drones {
        let status = client.raw().devices().get(code).await?.value;
        if status.device_type.as_deref() != Some("survey_drone") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("planned drone {code} is not a survey_drone"),
            )
            .into());
        }
        ensure_device_replicant(&status, code, &config.replicant)?;
        ensure_stowed_in_vessel(&status, code, &config.vessel)?;
        if status.controller_device_code.as_deref() != Some(controller) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "survey drone {code} is stowed in vessel {} but is not adopted by controller {controller}",
                    config.vessel
                ),
            )
            .into());
        }
    }

    info!(
        target: "replicant_client::explore",
        event = "travel.fleet_onboard_verified",
        replicant = %config.replicant,
        vessel = %config.vessel,
        controller,
        drones = ?plan.drones,
        "verified the complete survey fleet is onboard before travel"
    );

    Ok(())
}

async fn travel_to(
    client: &Client,
    config: &Config,
    plan: &mut RoutePlan,
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

    let raw_replicant = client
        .raw()
        .replicants()
        .get(&config.replicant)
        .await?
        .value;
    let already_traveling_to_target = raw_replicant.travel.as_ref().is_some_and(|travel| {
        travel
            .destination
            .as_deref()
            .is_some_and(|destination| designation_in_star(destination, target))
    });

    if already_traveling_to_target {
        // A resumed trip cannot safely recall or stow devices mid-flight.
        // Refuse to continue unless authoritative device state proves the
        // controller and every configured drone were onboard when travel began.
        verify_fleet_onboard_for_travel(client, config, plan).await?;
    } else {
        info!(
            target: "replicant_client::explore",
            event = "travel.fleet_preflight_started",
            destination = target,
            vessel = %config.vessel,
            "recalling, stowing, and verifying the survey fleet before departure"
        );
        recall_and_stow(client, config, plan).await?;
        verify_fleet_onboard_for_travel(client, config, plan).await?;
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
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("travel to {target} exceeded {:?}", config.travel_timeout),
            )
            .into());
        }

        let wait_for = (config.travel_timeout - started.elapsed()).min(Duration::from_secs(30));
        match tokio::time::timeout(wait_for, watch.next()).await {
            Ok(Ok(event)) => {
                plan.last_event_id = Some(event.id.as_str().to_owned());
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
                    if event.name.as_str() == "travel.arrived" {
                        save_plan(&config.plan_path, plan)?;
                        if current_star(client, &config.replicant).await?.as_deref() == Some(target)
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
            return Err(io::Error::other(format!(
                "instant system scan for {target} ended with {:?}: {:?}",
                outcome.status, outcome.response
            ))
            .into());
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

    Err(io::Error::other(format!(
        "system scan operation {} for {target} is {:?}, and targeted star knowledge does not confirm completion; rerun to reconcile without submitting a blind duplicate",
        operation.id(),
        outcome.status
    ))
    .into())
}

fn system_scan_response_was_ok(status: OperationStatus) -> bool {
    matches!(
        status,
        OperationStatus::ReconciliationRequired | OperationStatus::Completed
    )
}

async fn run_survey(
    client: &Client,
    config: &Config,
    plan: &mut RoutePlan,
    target: &str,
) -> AnyResult<()> {
    let controller_code = plan
        .controller
        .as_deref()
        .ok_or_else(|| io::Error::other("route plan has no survey controller"))?;
    let controller_handle = client.devices().get(controller_code).await?;
    let controller = controller_handle.as_survey_controller()?;
    let mut watch = client.events().watch().await?;

    let status = client.raw().devices().get(controller_code).await?.value;
    if survey_directive_needs_launch(status.ami_directive_status.as_deref()) {
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
            directive_status = status.ami_directive_status.as_deref().unwrap_or("unknown"),
            controller_status = status.status.as_deref().unwrap_or("unknown"),
            "controller already has a launched survey directive"
        );
    }

    let started = Instant::now();
    loop {
        if started.elapsed() >= config.survey_timeout {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "survey at {target} exceeded {:?}; plan remains resumable",
                    config.survey_timeout
                ),
            )
            .into());
        }

        let wait_for = (config.survey_timeout - started.elapsed()).min(Duration::from_secs(30));
        match tokio::time::timeout(wait_for, watch.next()).await {
            Ok(Ok(event)) => {
                plan.last_event_id = Some(event.id.as_str().to_owned());

                let mut completion = None;
                if is_survey_digest_for(&event, controller_code, target) {
                    let progress = survey_progress(&event);
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
                    save_plan(&config.plan_path, plan)?;

                    completion = survey_completion_proof(&event, controller_code, target);
                }

                completion =
                    completion.or_else(|| survey_completion_proof(&event, controller_code, target));
                if let Some(proof) = completion {
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
                    save_plan(&config.plan_path, plan)?;
                    return Ok(());
                }
            }
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => {
                info!(
                    target: "replicant_client::explore",
                    event = "survey.waiting",
                    controller = controller_code,
                    star = target,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "still waiting for survey completion"
                );
            }
        }
    }
}

async fn recall_and_stow(client: &Client, config: &Config, plan: &RoutePlan) -> AnyResult<()> {
    let controller_code = plan
        .controller
        .as_deref()
        .ok_or_else(|| io::Error::other("route plan has no survey controller"))?;
    let controller = client
        .devices()
        .get(controller_code)
        .await?
        .as_survey_controller()?;

    let status = client.raw().devices().get(controller_code).await?.value;
    if status.stowed_in_device_code.as_deref() != Some(config.vessel.as_str()) {
        if survey_directive_needs_recall(status.ami_directive_status.as_deref())
            && status
                .available_commands
                .iter()
                .any(|command| command == "withdraw")
        {
            info!(
                target: "replicant_client::explore",
                event = "survey.withdraw_started",
                controller = controller_code,
                "withdrawing survey fleet before travel"
            );
            let operation = controller.withdraw().await?;
            wait_immediate_operation("withdraw survey controller", &operation).await?;
        } else {
            warn!(
                target: "replicant_client::explore",
                event = "survey.withdraw_unavailable",
                controller = controller_code,
                controller_status = status.status.as_deref().unwrap_or("unknown"),
                directive_status = status.ami_directive_status.as_deref().unwrap_or("unknown"),
                "withdraw is not needed or not advertised; attempting authoritative stow verification"
            );
        }
    }
    stow_fleet(client, config, plan).await
}

async fn current_star(client: &Client, replicant_code: &str) -> AnyResult<Option<String>> {
    let status = client.raw().replicants().get(replicant_code).await?.value;
    Ok(status
        .location
        .as_deref()
        .map(star_from_designation)
        .map(str::to_owned))
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
        && survey_progress(event).is_some_and(|(_, remaining, _)| remaining == 0)
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
        Some(false) => Err(io::Error::other(format!(
            "survey completion evidence for {target} conflicts with authoritative planet/moon state"
        ))
        .into()),
        None => Err(io::Error::other(format!(
            "survey completion evidence for {target} needs a complete planet/moon reconciliation"
        ))
        .into()),
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
        || event
            .payload
            .get("destination")
            .and_then(Value::as_str)
            .is_some_and(|destination| designation_in_star(destination, target))
        || event.payload.get("star").and_then(Value::as_str) == Some(target)
}

fn survey_progress(event: &Event) -> Option<(u64, u64, u64)> {
    let progress = event.payload.get("report")?.get("progress")?.as_object()?;
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
    fn already_explored_stop_is_complete_without_execution() {
        let stop = RouteStop {
            star: "SCEPTURUM".into(),
            entry_point: Some("SCEPTURUM-7-L4".into()),
            distance_from_center_ly: 0.0,
            leg_distance_ly: 0.0,
            survey_required: false,
            system_scan_done: true,
            survey_done: true,
            completed: false,
        };
        assert!(stop.is_already_complete());
    }

    #[test]
    fn normalization_marks_skipped_stops_complete_and_advances_index() {
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
                    completed: false,
                },
                RouteStop {
                    star: "NEXT".into(),
                    entry_point: None,
                    distance_from_center_ly: 1.0,
                    leg_distance_ly: 1.0,
                    survey_required: true,
                    system_scan_done: false,
                    survey_done: false,
                    completed: false,
                },
            ],
            next_index: 0,
            phase: RunPhase::PreparingFleet,
            last_event_id: None,
        };

        assert!(plan.normalize_progress());
        assert!(plan.route[0].completed);
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
                completed: true,
            }],
            next_index: 0,
            phase: RunPhase::Ready,
            last_event_id: None,
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
        assert!(!plan.route[0].completed);
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
                completed: false,
            }],
            next_index: 0,
            phase: RunPhase::Surveying,
            last_event_id: None,
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
                    completed: true,
                },
                RouteStop {
                    star: "SECOND".into(),
                    entry_point: None,
                    distance_from_center_ly: 2.0,
                    leg_distance_ly: 1.0,
                    survey_required: true,
                    system_scan_done: false,
                    survey_done: false,
                    completed: false,
                },
            ],
            next_index: 1,
            phase: RunPhase::Ready,
            last_event_id: None,
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
        assert!(!plan.route[0].completed);
        assert_eq!(plan.phase, RunPhase::Surveying);
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
        assert_eq!(survey_progress(&event), Some((28, 8, 36)));
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
