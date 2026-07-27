//! Fully hydrates the durable database used by the Riker colony-candidate example.
//!
//! This initializer performs only safe reads. It does not run system scans,
//! dispatch survey drones, submit candidates, or send BobNet messages.
//! Consequently, it can persist only information the account has already
//! discovered.
//!
//! Environment variables:
//!
//! - `RS_API_TOKEN` or `REPLICANT_TOKEN` — required bearer token.
//! - `REPLICANT_DB` — SQLite path; defaults to `replicant-client.sqlite`.
//! - `REPLICANT_INIT_SYSTEM_LIMIT` — optional maximum explored systems to hydrate.
//! - `REPLICANT_INIT_OBJECT_LIMIT` — maximum known locations per system;
//!   defaults to `14096`.
//! - `REPLICANT_INIT_CONCURRENCY` — bounded location-detail concurrency;
//!   defaults to `4`.
//! - `REPLICANT_INIT_STAR_CATALOGUE_LIMIT_BYTES` — bounded response size for
//!   the unpaginated global star catalogue; defaults to `33554432` (32 MiB).
//! - `RUST_LOG` — tracing filter. The default enables detailed request and
//!   synchronization timing without exposing credentials.
//!
//! Example:
//!
//! ```text
//! RUST_LOG=replicant_client=info,replicant_client::raw::http=debug,replicant_client::sync=debug \
//! cargo run --example initialize_colony_database
//! ```

use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error,
    io,
    path::PathBuf,
    time::Instant,
};

use replicant_client::{Client, LifeStage, Realm, SecretString, StartupPolicy};
use tracing::{Instrument as _, error, info, info_span, warn};
use tracing_subscriber::{
    EnvFilter,
    fmt::{format::FmtSpan, time::SystemTime},
};

type AnyError = Box<dyn Error + Send + Sync + 'static>;

const DEFAULT_LOG_FILTER: &str = concat!(
    "replicant_client=info,",
    "replicant_client::initializer=debug,",
    "replicant_client::raw::http=debug,",
    "replicant_client::raw::rate_limit=debug,",
    "replicant_client::sync=debug,",
    "replicant_client::galaxy=debug,",
    "replicant_client::locations=debug,",
    "replicant_client::events=info,",
    "replicant_client::ops=info,",
    "replicant_client::state=debug,",
    "replicant_client::store=debug"
);

fn install_tracing() -> Result<(), AnyError> {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_LOG_FILTER));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_timer(SystemTime)
        .with_target(true)
        .with_thread_ids(true)
        .with_span_events(FmtSpan::CLOSE)
        .try_init()
        .map_err(|error| io::Error::other(error.to_string()))?;
    Ok(())
}

struct Config {
    token: String,
    database: PathBuf,
    system_limit: Option<usize>,
    object_limit: usize,
    concurrency: usize,
    star_catalogue_response_limit_bytes: usize,
}

impl Config {
    fn from_env() -> Result<Self, AnyError> {
        let token = env::var("RS_API_TOKEN")
            .or_else(|_| env::var("REPLICANT_TOKEN"))
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "RS_API_TOKEN or REPLICANT_TOKEN is required",
                )
            })?;

        Ok(Self {
            token,
            database: env::var_os("REPLICANT_DB")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("replicant-client.sqlite")),
            system_limit: optional_usize("REPLICANT_INIT_SYSTEM_LIMIT")?,
            object_limit: env_usize("REPLICANT_INIT_OBJECT_LIMIT", 14096)?,
            concurrency: env_usize("REPLICANT_INIT_CONCURRENCY", 4)?.max(1),
            star_catalogue_response_limit_bytes: env_usize(
                "REPLICANT_INIT_STAR_CATALOGUE_LIMIT_BYTES",
                32 * 1024 * 1024,
            )?,
        })
    }
}

fn optional_usize(name: &str) -> Result<Option<usize>, AnyError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value.parse::<usize>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{name} must be a non-negative integer: {error}"),
            )
        })?)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(Box::new(error)),
    }
}

fn env_usize(name: &str, default: usize) -> Result<usize, AnyError> {
    Ok(optional_usize(name)?.unwrap_or(default))
}

#[tokio::main]
async fn main() -> Result<(), AnyError> {
    install_tracing()?;
    let total_started = Instant::now();
    let config = Config::from_env()?;

    info!(
        target: "replicant_client::initializer",
        event = "initializer.started",
        database = %config.database.display(),
        system_limit = ?config.system_limit,
        object_limit = config.object_limit,
        concurrency = config.concurrency,
        star_catalogue_response_limit_bytes = config.star_catalogue_response_limit_bytes,
        "starting colony database initializer"
    );

    let client_started = Instant::now();
    let client = Client::builder()
        .authentication_token(SecretString::from(config.token.clone()))
        .sqlite(&config.database)
        .max_star_catalogue_response_body_bytes(config.star_catalogue_response_limit_bytes)
        .startup_policy(StartupPolicy::Essential)
        .start()
        .instrument(info_span!(
            target: "replicant_client::initializer",
            "client.start",
            database = %config.database.display()
        ))
        .await?;
    info!(
        target: "replicant_client::initializer",
        event = "initializer.client_started",
        elapsed_ms = client_started.elapsed().as_millis() as u64,
        "managed client started"
    );

    let ready_started = Instant::now();
    client.ready().await?;
    info!(
        target: "replicant_client::initializer",
        event = "initializer.client_ready",
        elapsed_ms = ready_started.elapsed().as_millis() as u64,
        readiness = ?client.readiness(),
        "managed client reached readiness"
    );

    let result = initialize(&client, &config).await;
    let close_started = Instant::now();
    let close_result = client.close().await;
    info!(
        target: "replicant_client::initializer",
        event = "initializer.client_closed",
        elapsed_ms = close_started.elapsed().as_millis() as u64,
        success = close_result.is_ok(),
        "managed client closed"
    );

    if let Err(error) = &result {
        error!(
            target: "replicant_client::initializer",
            event = "initializer.failed",
            elapsed_ms = total_started.elapsed().as_millis() as u64,
            error = %error,
            "colony database initializer failed"
        );
    } else {
        info!(
            target: "replicant_client::initializer",
            event = "initializer.completed",
            elapsed_ms = total_started.elapsed().as_millis() as u64,
            "colony database initializer completed"
        );
    }

    result?;
    close_result?;
    Ok(())
}

async fn initialize(client: &Client, config: &Config) -> Result<(), AnyError> {
    let initialize_started = Instant::now();

    let sync_started = Instant::now();
    info!(
        target: "replicant_client::initializer",
        event = "initializer.full_sync_started",
        "synchronizing all bounded managed account domains"
    );
    let core_report = client
        .sync()
        .full()
        .instrument(info_span!(target: "replicant_client::initializer", "initializer.full_sync"))
        .await?;
    let sync_elapsed = sync_started.elapsed();
    for diagnostic in &core_report.diagnostics {
        info!(
            target: "replicant_client::initializer",
            event = "initializer.sync_domain_result",
            domain = ?diagnostic.domain,
            progress = ?diagnostic.progress,
            pages = diagnostic.pages,
            items = diagnostic.items,
            revisions = diagnostic.revisions,
            complete = diagnostic.complete,
            reconciliation_queued = diagnostic.reconciliation_queued,
            failure = ?diagnostic.failure,
            "managed synchronization domain result"
        );
    }
    info!(
        target: "replicant_client::initializer",
        event = "initializer.full_sync_completed",
        elapsed_ms = sync_elapsed.as_millis() as u64,
        readiness = ?core_report.readiness,
        completed_domains = core_report.completed.len(),
        "full managed synchronization completed"
    );
    if core_report
        .diagnostics
        .iter()
        .any(|diagnostic| !diagnostic.complete)
    {
        return Err(io::Error::other(format!(
            "full managed synchronization was incomplete: {core_report:#?}"
        ))
        .into());
    }

    let query_started = Instant::now();
    let owned_replicants = client
        .replicants()
        .find()
        .in_realm(Realm::Live)
        .owned()
        .collect()
        .await?;
    info!(
        target: "replicant_client::initializer",
        event = "initializer.owned_replicants_loaded",
        elapsed_ms = query_started.elapsed().as_millis() as u64,
        replicants = owned_replicants.len(),
        "loaded owned replicants from committed local state"
    );

    if owned_replicants.is_empty() {
        return Err(io::Error::other("full synchronization produced no owned replicants").into());
    }

    let catalogue_started = Instant::now();
    info!(
        target: "replicant_client::initializer",
        event = "initializer.catalogue_refresh_started",
        "refreshing complete global star catalogue"
    );
    let catalogue_report = client
        .galaxy()
        .refresh_catalogue()
        .instrument(
            info_span!(target: "replicant_client::initializer", "initializer.catalogue_refresh"),
        )
        .await?;
    info!(
        target: "replicant_client::initializer",
        event = "initializer.catalogue_refresh_completed",
        elapsed_ms = catalogue_started.elapsed().as_millis() as u64,
        stars = catalogue_report.stars(),
        generated_at = catalogue_report.generated_at().unwrap_or(""),
        "global star catalogue committed"
    );

    let star_sync_started = Instant::now();
    let mut explored_by = BTreeMap::<String, BTreeSet<String>>::new();
    for (index, replicant) in owned_replicants.iter().enumerate() {
        let code = replicant.id().as_str();
        let item_started = Instant::now();
        info!(
            target: "replicant_client::initializer",
            event = "initializer.replicant_stars_started",
            replicant = code,
            index = index + 1,
            total = owned_replicants.len(),
            "synchronizing replicant star knowledge"
        );

        let report = client
            .galaxy()
            .sync_replicant_stars(code)
            .instrument(info_span!(
                target: "replicant_client::initializer",
                "initializer.replicant_stars",
                replicant = code
            ))
            .await?;

        for designation in report.explored_designations() {
            explored_by
                .entry(designation.as_str().to_owned())
                .or_default()
                .insert(code.to_owned());
        }

        info!(
            target: "replicant_client::initializer",
            event = "initializer.replicant_stars_completed",
            replicant = code,
            elapsed_ms = item_started.elapsed().as_millis() as u64,
            pages = report.pages(),
            stars = report.stars_seen(),
            explored = report.explored_designations().len(),
            "replicant star knowledge committed"
        );
    }

    let mut explored_systems = explored_by.into_iter().collect::<Vec<_>>();
    if let Some(limit) = config.system_limit {
        explored_systems.truncate(limit);
    }
    let star_sync_elapsed = star_sync_started.elapsed();
    info!(
        target: "replicant_client::initializer",
        event = "initializer.replicant_star_phase_completed",
        elapsed_ms = star_sync_elapsed.as_millis() as u64,
        explored_systems = explored_systems.len(),
        "completed all replicant star-list traversals"
    );

    let hydration_started = Instant::now();
    let mut failures = Vec::new();
    let mut systems_completed = 0usize;
    let mut locations_committed = 0usize;

    for (index, (star, discovered_by)) in explored_systems.iter().enumerate() {
        let system_started = Instant::now();
        info!(
            target: "replicant_client::initializer",
            event = "initializer.system_hydration_started",
            star = %star,
            index = index + 1,
            total = explored_systems.len(),
            discovered_by = %discovered_by.iter().cloned().collect::<Vec<_>>().join(","),
            object_limit = config.object_limit,
            concurrency = config.concurrency,
            "hydrating known objects in explored system"
        );

        let result = client
            .locations()
            .hydrate_system(star)
            .all_known_objects()
            .max_locations(config.object_limit)
            .concurrency(config.concurrency)
            .run()
            .instrument(info_span!(
                target: "replicant_client::initializer",
                "initializer.system_hydration",
                star = %star
            ))
            .await;

        match result {
            Ok(report) => {
                systems_completed += 1;
                locations_committed += report.locations_committed();
                if !report.failures().is_empty() {
                    failures.push((
                        star.clone(),
                        report
                            .failures()
                            .iter()
                            .map(|failure| format!("{}: {}", failure.designation, failure.message))
                            .collect::<Vec<_>>()
                            .join("; "),
                    ));
                }
                info!(
                    target: "replicant_client::initializer",
                    event = "initializer.system_hydration_completed",
                    star = %star,
                    elapsed_ms = system_started.elapsed().as_millis() as u64,
                    locations_committed = report.locations_committed(),
                    failures = report.failures().len(),
                    unknown_designations = report.unknown_designations().len(),
                    maximum_reached = report.maximum_reached(),
                    "explored system hydration completed"
                );
            }
            Err(error) => {
                warn!(
                    target: "replicant_client::initializer",
                    event = "initializer.system_hydration_failed",
                    star = %star,
                    elapsed_ms = system_started.elapsed().as_millis() as u64,
                    error = %error,
                    "explored system hydration failed"
                );
                failures.push((star.clone(), error.to_string()));
            }
        }
    }

    let hydration_elapsed = hydration_started.elapsed();
    info!(
        target: "replicant_client::initializer",
        event = "initializer.hydration_phase_completed",
        elapsed_ms = hydration_elapsed.as_millis() as u64,
        systems = explored_systems.len(),
        systems_completed,
        locations_committed,
        failures = failures.len(),
        "completed explored-system hydration phase"
    );

    let candidate_query_started = Instant::now();
    let candidates = client
        .locations()
        .find()
        .in_realm(Realm::Live)
        .planetary_bodies()
        .surveyed()
        .breathable_atmosphere()
        .without_advanced_civilisation()
        .life_stage_below(LifeStage::Intelligent)
        .gravity_g_between(0.8..=1.3)
        .surface_temp_c_between(10.0..=25.0)
        .collect()
        .await?;
    info!(
        target: "replicant_client::initializer",
        event = "initializer.candidate_query_completed",
        elapsed_ms = candidate_query_started.elapsed().as_millis() as u64,
        candidates = candidates.len(),
        "local colony candidate validation query completed"
    );

    info!(
        target: "replicant_client::initializer",
        event = "initializer.summary",
        elapsed_ms = initialize_started.elapsed().as_millis() as u64,
        full_sync_ms = sync_elapsed.as_millis() as u64,
        star_sync_ms = star_sync_elapsed.as_millis() as u64,
        hydration_ms = hydration_elapsed.as_millis() as u64,
        owned_replicants = owned_replicants.len(),
        explored_systems = explored_systems.len(),
        systems_completed,
        locations_committed,
        eligible_candidates = candidates.len(),
        failed_systems = failures.len(),
        "colony database initialization summary"
    );

    if !failures.is_empty() {
        for (star, failure) in &failures {
            warn!(
                target: "replicant_client::initializer",
                event = "initializer.system_failure_summary",
                star = %star,
                error = %failure,
                "system can be retried safely by rerunning the initializer"
            );
        }
        return Err(io::Error::other(format!(
            "{} explored system(s) failed to hydrate",
            failures.len()
        ))
        .into());
    }

    Ok(())
}
