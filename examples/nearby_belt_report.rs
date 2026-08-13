//! Reports asteroid belts in explored systems near a catalogue star.
//!
//! Required: `RS_API_TOKEN` or `REPLICANT_TOKEN`.
//! Optional: `REPLICANT_DB` and `RS_BELT_REPORT_CONCURRENCY`.
//!
//! ```text
//! cargo run --example nearby_belt_report -- SCEPTURUM 25
//! ```

use std::{collections::BTreeSet, env, error::Error, io, path::PathBuf};

use replicant_client::{Client, SecretString, StartupPolicy};
use replicant_runtime::reports::{
    DEFAULT_BELT_REPORT_CONCURRENCY, MAX_BELT_REPORT_CONCURRENCY, NearbyBelt, NearbyBeltReport,
    NearbyBeltReportRequest, nearby_belt_report,
};

type AnyError = Box<dyn Error + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

const STAR_CATALOGUE_RESPONSE_LIMIT_BYTES: usize = 32 * 1024 * 1024;

struct Config {
    token: SecretString,
    database: PathBuf,
    request: NearbyBeltReportRequest,
}

impl Config {
    fn from_args() -> AnyResult<Self> {
        let mut args = env::args().skip(1);
        let origin = args.next().ok_or_else(|| {
            input_error("usage: cargo run --example nearby_belt_report -- SYSTEM RADIUS_LY")
        })?;
        let radius = args.next().ok_or_else(|| {
            input_error("usage: cargo run --example nearby_belt_report -- SYSTEM RADIUS_LY")
        })?;
        if args.next().is_some() {
            return Err(input_error(
                "expected exactly two arguments: SYSTEM and RADIUS_LY",
            ));
        }

        let radius_ly = radius
            .parse::<f64>()
            .map_err(|error| input_error(format!("RADIUS_LY must be a number: {error}")))?;
        let concurrency = match env::var("RS_BELT_REPORT_CONCURRENCY") {
            Ok(value) => value.parse::<usize>().map_err(|error| {
                input_error(format!(
                    "RS_BELT_REPORT_CONCURRENCY must be an integer: {error}"
                ))
            })?,
            Err(env::VarError::NotPresent) => DEFAULT_BELT_REPORT_CONCURRENCY,
            Err(error) => return Err(error.into()),
        };
        if concurrency == 0 || concurrency > MAX_BELT_REPORT_CONCURRENCY {
            return Err(input_error(format!(
                "RS_BELT_REPORT_CONCURRENCY must be between 1 and {MAX_BELT_REPORT_CONCURRENCY}"
            )));
        }

        let mut request = NearbyBeltReportRequest::new(origin, radius_ly);
        request.concurrency = concurrency;
        Ok(Self {
            token: SecretString::from(
                env::var("RS_API_TOKEN")
                    .or_else(|_| env::var("REPLICANT_TOKEN"))
                    .map_err(|_| input_error("RS_API_TOKEN or REPLICANT_TOKEN is required"))?,
            ),
            database: env::var_os("REPLICANT_DB")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("replicant-client.sqlite")),
            request,
        })
    }
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let config = Config::from_args()?;
    let client = Client::builder()
        .authentication_token(config.token)
        .sqlite(config.database)
        .max_star_catalogue_response_body_bytes(STAR_CATALOGUE_RESPONSE_LIMIT_BYTES)
        .startup_policy(StartupPolicy::Essential)
        .start()
        .await?;

    let result = nearby_belt_report(&client, &config.request).await;
    let close_result = client.close().await;
    let report = result?;
    print_report(&report);
    if !report.failures.is_empty() {
        eprintln!();
        eprintln!("Failed to refresh {} system(s):", report.failures.len());
        for failure in &report.failures {
            eprintln!("  {}: {}", failure.system, failure.error);
        }
        return Err(io::Error::other(format!(
            "belt report is incomplete because {} system refresh(es) failed",
            report.failures.len()
        ))
        .into());
    }
    close_result?;
    Ok(())
}

fn print_report(report: &NearbyBeltReport) {
    let systems_with_belts = report
        .belts
        .iter()
        .map(|belt| belt.system.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    println!();
    println!(
        "Belts within {:.2} ly of {}: {} belt(s) in {} of {} explored system(s)",
        report.radius_ly,
        report.origin,
        report.belts.len(),
        systems_with_belts,
        report.examined_systems
    );
    if report.belts.is_empty() {
        return;
    }

    let density_width = width(&report.belts, "DENSITY", |belt| &belt.density);
    let system_width = width(&report.belts, "SYSTEM", |belt| &belt.system);
    let belt_width = width(&report.belts, "BELT", |belt| &belt.designation);
    println!();
    println!(
        "{:<density_width$}  {:<system_width$}  {:>8}  {:<belt_width$}  {:>11}  {:>9}  RESOURCES",
        "DENSITY", "SYSTEM", "DIST LY", "BELT", "RADII AU", "WIDTH AU"
    );
    println!(
        "{}",
        "-".repeat(density_width + system_width + belt_width + 59)
    );
    for belt in &report.belts {
        println!(
            "{:<density_width$}  {:<system_width$}  {:>8.2}  {:<belt_width$}  {:>11}  {:>9}  {}",
            belt.density,
            belt.system,
            belt.distance_ly,
            belt.designation,
            radii(belt),
            width_au(belt),
            resources(belt)
        );
    }
}

fn width<'a>(
    belts: &'a [NearbyBelt],
    heading: &str,
    value: impl Fn(&'a NearbyBelt) -> &'a str,
) -> usize {
    belts
        .iter()
        .map(|belt| value(belt).len())
        .max()
        .unwrap_or(0)
        .max(heading.len())
}

fn radii(belt: &NearbyBelt) -> String {
    match (belt.inner_radius_au, belt.outer_radius_au) {
        (Some(inner), Some(outer)) => format!("{inner:.2}-{outer:.2}"),
        (Some(inner), None) => format!("{inner:.2}-?"),
        (None, Some(outer)) => format!("?-{outer:.2}"),
        (None, None) => "?".into(),
    }
}

fn width_au(belt: &NearbyBelt) -> String {
    match (belt.inner_radius_au, belt.outer_radius_au) {
        (Some(inner), Some(outer)) => format!("{:.2}", (outer - inner).max(0.0)),
        _ => "?".into(),
    }
}

fn resources(belt: &NearbyBelt) -> String {
    if belt.resources.is_empty() {
        return "?".into();
    }
    belt.resources
        .iter()
        .map(|(resource, scarcity)| format!("{}={scarcity}", abbreviation(resource)))
        .collect::<Vec<_>>()
        .join(" ")
}

fn abbreviation(resource: &str) -> &str {
    match resource {
        "carbon" => "Car",
        "conductive" => "Con",
        "rares" => "Rar",
        "silicates" => "Sil",
        "volatiles" => "Vol",
        other => other,
    }
}

fn input_error(message: impl Into<String>) -> AnyError {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}
