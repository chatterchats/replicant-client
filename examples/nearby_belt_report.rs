//! Reports asteroid belts in explored systems near a catalogue star.
//!
//! The example performs safe reads only. It refreshes star knowledge for every
//! owned replicant, unions the explored systems, filters those systems by
//! straight-line catalogue distance, then refreshes each selected system's
//! managed location snapshot. The final report is ordered from dense belts to
//! sparse belts.
//!
//! Required:
//!
//! - `RS_API_TOKEN` or `REPLICANT_TOKEN`
//!
//! Optional:
//!
//! - `REPLICANT_DB` (defaults to `replicant-client.sqlite`)
//! - `RS_BELT_REPORT_CONCURRENCY` (defaults to `4`, maximum `16`)
//!
//! Usage:
//!
//! ```text
//! cargo run --example nearby_belt_report -- SCEPTURUM 25
//! ```

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error as StdError,
    io,
    path::PathBuf,
};

use futures::{StreamExt, stream};
use replicant_client::{
    Client, Realm, SecretString, StartupPolicy,
    domain::{GalacticPosition, Location},
};
use serde_json::Value;

type AnyError = Box<dyn StdError + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

const DEFAULT_CONCURRENCY: usize = 4;
const MAX_CONCURRENCY: usize = 16;
const STAR_CATALOGUE_RESPONSE_LIMIT_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug)]
struct Config {
    token: SecretString,
    database: PathBuf,
    origin: String,
    radius_ly: f64,
    concurrency: usize,
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

        let radius_ly = radius.parse::<f64>().map_err(|error| {
            input_error(format!("RADIUS_LY must be a number: {error}"))
        })?;
        if !radius_ly.is_finite() || radius_ly < 0.0 {
            return Err(input_error(
                "RADIUS_LY must be a non-negative finite number",
            ));
        }

        let token = env::var("RS_API_TOKEN")
            .or_else(|_| env::var("REPLICANT_TOKEN"))
            .map_err(|_| input_error("RS_API_TOKEN or REPLICANT_TOKEN is required"))?;
        let concurrency = match env::var("RS_BELT_REPORT_CONCURRENCY") {
            Ok(value) => value.parse::<usize>().map_err(|error| {
                input_error(format!(
                    "RS_BELT_REPORT_CONCURRENCY must be an integer: {error}"
                ))
            })?,
            Err(env::VarError::NotPresent) => DEFAULT_CONCURRENCY,
            Err(error) => return Err(error.into()),
        };
        if concurrency == 0 || concurrency > MAX_CONCURRENCY {
            return Err(input_error(format!(
                "RS_BELT_REPORT_CONCURRENCY must be between 1 and {MAX_CONCURRENCY}"
            )));
        }

        Ok(Self {
            token: SecretString::from(token),
            database: env::var_os("REPLICANT_DB")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("replicant-client.sqlite")),
            origin: origin.to_ascii_uppercase(),
            radius_ly,
            concurrency,
        })
    }
}

#[derive(Clone, Debug)]
struct NearbySystem {
    designation: String,
    distance_ly: f64,
}

#[derive(Clone, Debug)]
struct BeltReport {
    system: String,
    designation: String,
    distance_ly: f64,
    density: String,
    inner_radius_au: Option<f64>,
    outer_radius_au: Option<f64>,
    resources: BTreeMap<String, String>,
}

impl BeltReport {
    fn density_rank(&self) -> u8 {
        match self.density.to_ascii_lowercase().as_str() {
            "dense" => 3,
            "moderate" => 2,
            "sparse" => 1,
            _ => 0,
        }
    }

    fn radii(&self) -> String {
        match (self.inner_radius_au, self.outer_radius_au) {
            (Some(inner), Some(outer)) => format!("{inner:.2}-{outer:.2}"),
            (Some(inner), None) => format!("{inner:.2}-?"),
            (None, Some(outer)) => format!("?-{outer:.2}"),
            (None, None) => "?".into(),
        }
    }

    fn width_au(&self) -> String {
        match (self.inner_radius_au, self.outer_radius_au) {
            (Some(inner), Some(outer)) => format!("{:.2}", (outer - inner).max(0.0)),
            _ => "?".into(),
        }
    }

    fn resources(&self) -> String {
        if self.resources.is_empty() {
            return "?".into();
        }
        self.resources
            .iter()
            .map(|(resource, scarcity)| {
                format!("{}={scarcity}", resource_abbreviation(resource))
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let config = Config::from_args()?;
    let client = Client::builder()
        .authentication_token(config.token.clone())
        .sqlite(&config.database)
        .max_star_catalogue_response_body_bytes(STAR_CATALOGUE_RESPONSE_LIMIT_BYTES)
        .startup_policy(StartupPolicy::Essential)
        .start()
        .await?;

    let result = run(&client, &config).await;
    let close_result = client.close().await;
    result?;
    close_result?;
    Ok(())
}

async fn run(client: &Client, config: &Config) -> AnyResult<()> {
    client.ready().await?;

    let owned_replicants = client
        .replicants()
        .find()
        .in_realm(Realm::Live)
        .owned()
        .collect()
        .await?;
    if owned_replicants.is_empty() {
        return Err(input_error("the account has no owned replicants"));
    }

    eprintln!(
        "Refreshing explored-system knowledge for {} replicant(s)...",
        owned_replicants.len()
    );
    let mut explored = BTreeSet::new();
    for replicant in &owned_replicants {
        let report = client
            .galaxy()
            .sync_replicant_stars(replicant.id().as_str())
            .await?;
        explored.extend(
            report
                .explored_designations()
                .iter()
                .map(|designation| designation.as_str().to_owned()),
        );
    }

    let mut catalogue = client.galaxy().catalogue();
    if catalogue.is_empty() {
        eprintln!("Refreshing the star catalogue...");
        client.galaxy().refresh_catalogue().await?;
        catalogue = client.galaxy().catalogue();
    }
    let origin_position = catalogue
        .iter()
        .find(|star| star.key.id.as_str() == config.origin)
        .ok_or_else(|| {
            input_error(format!(
                "origin system `{}` is absent from the star catalogue",
                config.origin
            ))
        })?
        .position
        .ok_or_else(|| {
            input_error(format!(
                "origin system `{}` has no catalogue position",
                config.origin
            ))
        })?;

    let mut nearby = catalogue
        .iter()
        .filter_map(|star| {
            let designation = star.key.id.as_str();
            if !explored.contains(designation) {
                return None;
            }
            let position = star.position?;
            let distance_ly = position_distance(origin_position, position);
            if distance_ly <= config.radius_ly {
                Some(NearbySystem {
                    designation: designation.to_owned(),
                    distance_ly,
                })
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    nearby.sort_by(|left, right| left.designation.cmp(&right.designation));

    eprintln!(
        "Refreshing {} explored system snapshot(s) within {:.2} ly of {}...",
        nearby.len(),
        config.radius_ly,
        config.origin
    );
    let fetched = stream::iter(nearby.into_iter().map(|system| {
        let locations = client.locations();
        async move {
            let result = locations.get(&system.designation).await;
            (system, result)
        }
    }))
    .buffer_unordered(config.concurrency)
    .collect::<Vec<_>>()
    .await;

    let examined_systems = fetched.len();
    let mut failed_systems = Vec::new();
    let mut belts = Vec::new();
    for (system, result) in fetched {
        match result {
            Ok(location) => belts.extend(belts_from_location(&system, &location)),
            Err(error) => failed_systems.push((system.designation, error.to_string())),
        }
    }

    belts.sort_by(|left, right| {
        right
            .density_rank()
            .cmp(&left.density_rank())
            .then_with(|| {
                left.distance_ly
                    .partial_cmp(&right.distance_ly)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| left.designation.cmp(&right.designation))
    });
    print_report(config, examined_systems, &belts);

    if !failed_systems.is_empty() {
        eprintln!();
        eprintln!("Failed to refresh {} system(s):", failed_systems.len());
        for (system, error) in &failed_systems {
            eprintln!("  {system}: {error}");
        }
        return Err(io::Error::other(format!(
            "belt report is incomplete because {} system refresh(es) failed",
            failed_systems.len()
        ))
        .into());
    }

    Ok(())
}

fn position_distance(left: GalacticPosition, right: GalacticPosition) -> f64 {
    let dx = left.x - right.x;
    let dy = left.y - right.y;
    let dz = left.z - right.z;
    (dx * dx + dy * dy + dz * dz).sqrt()
}

fn belts_from_location(system: &NearbySystem, location: &Location) -> Vec<BeltReport> {
    let Some(asteroid_belt) = location.unknown.get("asteroid_belt") else {
        return Vec::new();
    };

    let values = asteroid_belt
        .get("belts")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_else(|| std::slice::from_ref(asteroid_belt));

    values
        .iter()
        .filter_map(|value| parse_belt(system, value))
        .collect()
}

fn parse_belt(system: &NearbySystem, value: &Value) -> Option<BeltReport> {
    let object = value.as_object()?;
    let designation = object.get("designation")?.as_str()?.to_owned();
    let density = object
        .get("density")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let resources = object
        .get("resources")
        .and_then(Value::as_object)
        .map(|resources| {
            resources
                .iter()
                .map(|(resource, scarcity)| {
                    let scarcity = scarcity
                        .as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| scarcity.to_string());
                    (resource.clone(), scarcity)
                })
                .collect()
        })
        .unwrap_or_default();

    Some(BeltReport {
        system: system.designation.clone(),
        designation,
        distance_ly: system.distance_ly,
        density,
        inner_radius_au: object.get("inner_radius_au").and_then(Value::as_f64),
        outer_radius_au: object.get("outer_radius_au").and_then(Value::as_f64),
        resources,
    })
}

fn resource_abbreviation(resource: &str) -> &str {
    match resource {
        "carbon" => "Car",
        "conductive" => "Con",
        "rares" => "Rar",
        "silicates" => "Sil",
        "structural" => "Str",
        "volatiles" => "Vol",
        other => other,
    }
}

fn print_report(config: &Config, examined_systems: usize, belts: &[BeltReport]) {
    let systems_with_belts = belts
        .iter()
        .map(|belt| belt.system.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    println!();
    println!(
        "Belts within {:.2} ly of {}: {} belt(s) in {} of {} explored system(s)",
        config.radius_ly,
        config.origin,
        belts.len(),
        systems_with_belts,
        examined_systems
    );
    if belts.is_empty() {
        return;
    }

    let density_width = belts
        .iter()
        .map(|belt| belt.density.len())
        .max()
        .unwrap_or(0)
        .max("DENSITY".len());
    let system_width = belts
        .iter()
        .map(|belt| belt.system.len())
        .max()
        .unwrap_or(0)
        .max("SYSTEM".len());
    let belt_width = belts
        .iter()
        .map(|belt| belt.designation.len())
        .max()
        .unwrap_or(0)
        .max("BELT".len());

    println!();
    println!(
        "{:<density_width$}  {:<system_width$}  {:>8}  {:<belt_width$}  {:>11}  {:>9}  RESOURCES",
        "DENSITY", "SYSTEM", "DIST LY", "BELT", "RADII AU", "WIDTH AU"
    );
    println!(
        "{}",
        "-".repeat(density_width + system_width + belt_width + 59)
    );
    for belt in belts {
        println!(
            "{:<density_width$}  {:<system_width$}  {:>8.2}  {:<belt_width$}  {:>11}  {:>9}  {}",
            belt.density,
            belt.system,
            belt.distance_ly,
            belt.designation,
            belt.radii(),
            belt.width_au(),
            belt.resources()
        );
    }
    println!();
    println!("Resources: Car=carbon Con=conductive Rar=rares Sil=silicates Str=structural Vol=volatiles");
}

fn input_error(message: impl Into<String>) -> AnyError {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_documented_system_belt_summary() {
        let system = NearbySystem {
            designation: "TARAZEDAR".into(),
            distance_ly: 4.5,
        };
        let location = Location {
            key: replicant_client::domain::LocationKey::live("TARAZEDAR".into()),
            location_type: None,
            scanned: None,
            system_scanned: Some(true),
            system_tags: Vec::new(),
            system: Some("TARAZEDAR".into()),
            parent: None,
            survey_progress: Default::default(),
            environment: Default::default(),
            unknown: BTreeMap::from([(
                "asteroid_belt".into(),
                serde_json::json!({
                    "present": true,
                    "belts": [{
                        "density": "dense",
                        "designation": "TARAZEDAR-BELT-1",
                        "inner_radius_au": 0.6,
                        "outer_radius_au": 0.9,
                        "resources": {"carbon": "rich"}
                    }]
                }),
            )]),
        };

        let belts = belts_from_location(&system, &location);
        assert_eq!(belts.len(), 1);
        assert_eq!(belts[0].designation, "TARAZEDAR-BELT-1");
        assert_eq!(belts[0].density_rank(), 3);
        assert_eq!(belts[0].resources["carbon"], "rich");
    }

    #[test]
    fn density_order_is_dense_then_moderate_then_sparse() {
        let rank = |density: &str| BeltReport {
            system: "SOL".into(),
            designation: "SOL-BELT-1".into(),
            distance_ly: 0.0,
            density: density.into(),
            inner_radius_au: None,
            outer_radius_au: None,
            resources: BTreeMap::new(),
        }
        .density_rank();

        assert!(rank("dense") > rank("moderate"));
        assert!(rank("moderate") > rank("sparse"));
    }
}
