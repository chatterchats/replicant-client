//! Galactic Observatory prospecting and spectral triangulation workflows.
//!
//! The CLI deliberately keeps the gameplay mutation itself inside the managed
//! operation journal. Catalogue geometry and target selection are local-only;
//! server responses remain authoritative for whether a prospect direction is
//! actually sparse enough to use.

use std::{
    cmp::Ordering,
    env,
    f64::consts::PI,
    io::{self, ErrorKind},
    path::{Path, PathBuf},
};

use crate::{config::ManagedClientConfig, start_managed_client};
use replicant_client::{Client, DeviceType, OperationOutcome, OperationStatus, Star, raw};
use serde_json::Value;

const DEFAULT_TRIANGULATION_SIGNATURE: &str = "934d3ac4dcc918ad";
const MIN_TRIANGULATION_RADIUS_LY: f64 = 15_000.0;
const TRIANGULATION_FRINGE_MARGIN_LY: f64 = 5_000.0;
const DEFAULT_ANALYSIS_RADIUS_LY: f64 = 20.0;
const DEFAULT_AUTO_SAMPLES: usize = 64;
const DEFAULT_AUTO_ATTEMPTS: usize = 4;
const GOLDEN_ANGLE: f64 = 2.399_963_229_728_653;

#[derive(Clone, Copy, Debug, PartialEq)]
struct Vec3 {
    x: f64,
    y: f64,
    z: f64,
}

impl Vec3 {
    const ZERO: Self = Self {
        x: 0.0,
        y: 0.0,
        z: 0.0,
    };

    fn from_array(value: [f64; 3]) -> Self {
        Self {
            x: value[0],
            y: value[1],
            z: value[2],
        }
    }

    fn to_array(self) -> [f64; 3] {
        [self.x, self.y, self.z]
    }

    fn add(self, other: Self) -> Self {
        Self {
            x: self.x + other.x,
            y: self.y + other.y,
            z: self.z + other.z,
        }
    }

    fn sub(self, other: Self) -> Self {
        Self {
            x: self.x - other.x,
            y: self.y - other.y,
            z: self.z - other.z,
        }
    }

    fn scale(self, factor: f64) -> Self {
        Self {
            x: self.x * factor,
            y: self.y * factor,
            z: self.z * factor,
        }
    }

    fn dot(self, other: Self) -> f64 {
        self.x * other.x + self.y * other.y + self.z * other.z
    }

    fn norm(self) -> f64 {
        self.dot(self).sqrt()
    }

    fn normalized(self) -> Option<Self> {
        let norm = self.norm();
        (norm > f64::EPSILON).then(|| self.scale(1.0 / norm))
    }
}

impl From<replicant_client::domain::GalacticPosition> for Vec3 {
    fn from(value: replicant_client::domain::GalacticPosition) -> Self {
        Self {
            x: value.x,
            y: value.y,
            z: value.z,
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Selector {
    observatories: Vec<String>,
    all: bool,
    tag: Option<String>,
}

#[derive(Clone, Debug)]
enum ProspectDirection {
    Auto,
    Outward,
    AwaySol,
    TowardSol,
    TowardStar(String),
    AwayStar(String),
    Vector(Vec3),
    Axis(Vec3),
}

#[derive(Clone, Debug)]
struct ProspectConfig {
    selector: Selector,
    database: PathBuf,
    direction: ProspectDirection,
    analysis_radius_ly: f64,
    samples: usize,
    attempts: usize,
    dry_run: bool,
}

#[derive(Clone, Debug)]
struct TriangulateConfig {
    selector: Selector,
    database: PathBuf,
    signature: String,
    target: Option<Vec3>,
    radius_ly: Option<f64>,
    seed: Option<String>,
    dry_run: bool,
}

#[derive(Clone, Debug)]
struct StatusConfig {
    selector: Selector,
    database: PathBuf,
}

#[derive(Clone, Debug)]
enum Command {
    Status(StatusConfig),
    Prospect(ProspectConfig),
    Triangulate(TriangulateConfig),
}

#[derive(Clone, Debug)]
struct ObservatoryInfo {
    code: String,
    status: raw::devices::DeviceStatus,
    star: Option<String>,
    position: Option<Vec3>,
    distance_from_sol_ly: Option<f64>,
}

impl ObservatoryInfo {
    fn location(&self) -> &str {
        self.status.location.as_deref().unwrap_or("<unknown>")
    }

    fn advertised(&self, command: &str) -> bool {
        self.status.available_commands.is_empty()
            || self
                .status
                .available_commands
                .iter()
                .any(|candidate| candidate == command)
    }
}

#[derive(Clone, Debug)]
struct DirectionCandidate {
    direction: Vec3,
    forward_neighbours: usize,
    weighted_density: f64,
    outward_alignment: f64,
}

#[derive(Clone, Debug, Default, PartialEq)]
struct ProspectBlocked {
    message: Option<String>,
    neighbours: Option<f64>,
    outward_neighbours: Option<f64>,
    expected: Option<f64>,
    ratio: Option<f64>,
    outward_ratio: Option<f64>,
}

/// Result of one automatic sparse-direction prospect submission.
#[derive(Clone, Debug, serde::Serialize)]
pub struct AutoProspectReport {
    /// Observatory chosen for the attempt.
    pub observatory: String,
    /// Catalogue star containing the observatory, when known.
    pub star: Option<String>,
    /// Sparse direction submitted to the game server.
    pub direction: Option<[f64; 3]>,
    /// Number of candidate directions tried before a prospect started.
    pub attempts: usize,
    /// Durable managed operation identifier.
    pub operation_id: String,
    /// Final managed operation status.
    pub status: String,
}

/// Uses the same catalogue-density ranking as the observatory CLI to choose a
/// deployed observatory and retry sparse directions until one prospect starts.
/// Supplying `observatory_code` pins selection to that device; `None` chooses
/// the sparsest eligible owned observatory automatically.
pub async fn auto_prospect(
    client: &Client,
    observatory_code: Option<&str>,
) -> crate::AnyResult<AutoProspectReport> {
    let selector = Selector {
        observatories: observatory_code
            .map(|code| vec![code.to_owned()])
            .unwrap_or_default(),
        all: false,
        tag: None,
    };
    let config = ProspectConfig {
        selector,
        database: PathBuf::new(),
        direction: ProspectDirection::Auto,
        analysis_radius_ly: DEFAULT_ANALYSIS_RADIUS_LY,
        samples: DEFAULT_AUTO_SAMPLES,
        attempts: DEFAULT_AUTO_ATTEMPTS,
        dry_run: false,
    };
    let catalogue = refresh_catalogue(client).await?;
    let sol = sol_position(&catalogue);
    let mut observatories = load_observatories(client, &config.selector, &catalogue, sol).await?;
    if observatory_code.is_none() {
        let selected = choose_best_prospect_observatory(
            &observatories,
            &catalogue,
            sol,
            config.analysis_radius_ly,
        )
        .ok_or_else(|| {
            app_error("no deployed observatory with usable catalogue coordinates was found")
        })?
        .clone();
        observatories = vec![selected];
    }
    let observatory = observatories
        .first()
        .ok_or_else(|| app_error("no Galactic Observatory device matched the selection"))?;
    if !observatory.advertised("prospect") {
        return Err(app_error(format!(
            "{} does not currently advertise the prospect command",
            observatory.code
        )));
    }
    let candidates = prospect_directions(&config, observatory, &catalogue, sol)?;
    for (index, candidate) in candidates.into_iter().take(config.attempts).enumerate() {
        let handle = client.devices().get(&observatory.code).await?;
        let operation = handle.prospect(candidate.direction).await?;
        let operation_id = operation.id().to_string();
        let outcome = operation.outcome().await?;
        match classify_prospect_outcome(&outcome) {
            ProspectSubmission::Started => {
                return Ok(AutoProspectReport {
                    observatory: observatory.code.clone(),
                    star: observatory.star.clone(),
                    direction: candidate.direction,
                    attempts: index + 1,
                    operation_id,
                    status: format!("{:?}", outcome.status).to_ascii_lowercase(),
                });
            }
            ProspectSubmission::Blocked(_) => continue,
            ProspectSubmission::Rejected => {
                return Err(app_error(format!(
                    "{} rejected automatic prospect attempt {}",
                    observatory.code,
                    index + 1
                )));
            }
            ProspectSubmission::Ambiguous => {
                return Ok(AutoProspectReport {
                    observatory: observatory.code.clone(),
                    star: observatory.star.clone(),
                    direction: candidate.direction,
                    attempts: index + 1,
                    operation_id,
                    status: "ambiguous".to_owned(),
                });
            }
        }
    }
    Err(app_error(format!(
        "{} had no sparse prospect direction accepted after {} attempts",
        observatory.code, config.attempts
    )))
}

/// Runs the compatibility CLI adapter for observatory reports, plans, and actions.
pub async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let command = parse_command(arguments)?;
    match command {
        Command::Status(config) => run_status(config).await,
        Command::Prospect(config) => run_prospect(config).await,
        Command::Triangulate(config) => run_triangulate(config).await,
    }
}

fn parse_command(arguments: Vec<String>) -> crate::AnyResult<Command> {
    let mut arguments = arguments.into_iter();
    let Some(command) = arguments.next() else {
        print_help();
        return Err(app_error(
            "observatory requires an operation: status, prospect, or triangulate",
        ));
    };
    let rest = arguments.collect::<Vec<_>>();
    match command.as_str() {
        "-h" | "--help" | "help" => {
            print_help();
            std::process::exit(0);
        }
        "status" | "list" | "ls" => Ok(Command::Status(parse_status(rest)?)),
        "prospect" => Ok(Command::Prospect(parse_prospect(rest)?)),
        "triangulate" | "triangulation" => Ok(Command::Triangulate(parse_triangulate(rest)?)),
        other => Err(app_error(format!(
            "unknown observatory operation {other:?}; run `replicant-cli observatory --help`"
        ))),
    }
}

fn parse_status(arguments: Vec<String>) -> crate::AnyResult<StatusConfig> {
    let mut database = default_database();
    let mut selector = Selector::default();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "-h" | "--help" | "help" => {
                print_status_help();
                std::process::exit(0);
            }
            "--database" | "--db" => {
                database = PathBuf::from(required(&mut arguments, "--database")?);
            }
            "--observatory" => selector
                .observatories
                .push(required(&mut arguments, "--observatory")?),
            "--all" => selector.all = true,
            "--tag" => selector.tag = Some(required(&mut arguments, "--tag")?),
            other => return Err(unknown_option("status", other)),
        }
    }
    validate_selector(&selector)?;
    Ok(StatusConfig { selector, database })
}

fn parse_prospect(arguments: Vec<String>) -> crate::AnyResult<ProspectConfig> {
    let mut database = default_database();
    let mut selector = Selector::default();
    let mut direction_name = "auto".to_owned();
    let mut star = None;
    let mut vector = None;
    let mut analysis_radius_ly = DEFAULT_ANALYSIS_RADIUS_LY;
    let mut samples = DEFAULT_AUTO_SAMPLES;
    let mut attempts = DEFAULT_AUTO_ATTEMPTS;
    let mut dry_run = false;
    let mut arguments = arguments.into_iter();

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "-h" | "--help" | "help" => {
                print_prospect_help();
                std::process::exit(0);
            }
            "--database" | "--db" => {
                database = PathBuf::from(required(&mut arguments, "--database")?);
            }
            "--observatory" => selector
                .observatories
                .push(required(&mut arguments, "--observatory")?),
            "--all" => selector.all = true,
            "--tag" => selector.tag = Some(required(&mut arguments, "--tag")?),
            "--direction" => direction_name = required(&mut arguments, "--direction")?,
            "--star" => star = Some(required(&mut arguments, "--star")?),
            "--vector" => vector = Some(parse_vec3(&required(&mut arguments, "--vector")?)?),
            "--analysis-radius" => {
                analysis_radius_ly = parse_positive_f64(
                    &required(&mut arguments, "--analysis-radius")?,
                    "--analysis-radius",
                )?;
            }
            "--samples" => {
                samples =
                    parse_positive_usize(&required(&mut arguments, "--samples")?, "--samples")?;
            }
            "--attempts" => {
                attempts =
                    parse_positive_usize(&required(&mut arguments, "--attempts")?, "--attempts")?;
            }
            "--dry-run" => dry_run = true,
            other => return Err(unknown_option("prospect", other)),
        }
    }

    validate_selector(&selector)?;
    let direction = if let Some(vector) = vector {
        if direction_name != "auto" {
            return Err(app_error(
                "--vector and an explicit --direction are mutually exclusive",
            ));
        }
        if star.is_some() {
            return Err(app_error("--star cannot be combined with --vector"));
        }
        if vector.norm() <= f64::EPSILON {
            return Err(app_error("--vector must be non-zero"));
        }
        ProspectDirection::Vector(vector)
    } else {
        parse_direction(&direction_name, star)?
    };
    if !matches!(&direction, ProspectDirection::Auto) {
        attempts = 1;
    }

    Ok(ProspectConfig {
        selector,
        database,
        direction,
        analysis_radius_ly,
        samples,
        attempts,
        dry_run,
    })
}

fn parse_triangulate(arguments: Vec<String>) -> crate::AnyResult<TriangulateConfig> {
    let mut database = default_database();
    let mut selector = Selector::default();
    let mut signature = env::var("RS_OBSERVATORY_SIGNATURE")
        .unwrap_or_else(|_| DEFAULT_TRIANGULATION_SIGNATURE.to_owned());
    let mut target = None;
    let mut radius_ly = None;
    let mut seed = None;
    let mut dry_run = false;
    let mut arguments = arguments.into_iter();

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "-h" | "--help" | "help" => {
                print_triangulate_help();
                std::process::exit(0);
            }
            "--database" | "--db" => {
                database = PathBuf::from(required(&mut arguments, "--database")?);
            }
            "--observatory" => selector
                .observatories
                .push(required(&mut arguments, "--observatory")?),
            "--all" => selector.all = true,
            "--tag" => selector.tag = Some(required(&mut arguments, "--tag")?),
            "--signature" => signature = required(&mut arguments, "--signature")?,
            "--target" => target = Some(parse_vec3(&required(&mut arguments, "--target")?)?),
            "--radius" => {
                radius_ly = Some(parse_positive_f64(
                    &required(&mut arguments, "--radius")?,
                    "--radius",
                )?);
            }
            "--seed" => seed = Some(required(&mut arguments, "--seed")?),
            "--dry-run" => dry_run = true,
            other => return Err(unknown_option("triangulate", other)),
        }
    }

    validate_selector(&selector)?;
    if signature.trim().is_empty() {
        return Err(app_error("--signature must not be empty"));
    }
    Ok(TriangulateConfig {
        selector,
        database,
        signature,
        target,
        radius_ly,
        seed,
        dry_run,
    })
}

fn parse_direction(value: &str, star: Option<String>) -> crate::AnyResult<ProspectDirection> {
    let normalized = value.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "auto" | "sparse" => {
            if star.is_some() {
                return Err(app_error(
                    "--star is only valid with toward-star or away-star",
                ));
            }
            Ok(ProspectDirection::Auto)
        }
        "outward" | "default" => {
            if star.is_some() {
                return Err(app_error(
                    "--star is only valid with toward-star or away-star",
                ));
            }
            Ok(ProspectDirection::Outward)
        }
        "away-sol" | "from-sol" => {
            reject_unused_star(&star)?;
            Ok(ProspectDirection::AwaySol)
        }
        "toward-sol" | "to-sol" | "inward" => {
            reject_unused_star(&star)?;
            Ok(ProspectDirection::TowardSol)
        }
        "toward-star" | "to-star" => {
            Ok(ProspectDirection::TowardStar(star.ok_or_else(|| {
                app_error("--direction toward-star requires --star STAR")
            })?))
        }
        "away-star" | "from-star" => {
            Ok(ProspectDirection::AwayStar(star.ok_or_else(|| {
                app_error("--direction away-star requires --star STAR")
            })?))
        }
        "+x" | "x+" => axis_direction(&star, [1.0, 0.0, 0.0]),
        "-x" | "x-" => axis_direction(&star, [-1.0, 0.0, 0.0]),
        "+y" | "y+" | "sideways" => axis_direction(&star, [0.0, 1.0, 0.0]),
        "-y" | "y-" => axis_direction(&star, [0.0, -1.0, 0.0]),
        "+z" | "z+" => axis_direction(&star, [0.0, 0.0, 1.0]),
        "-z" | "z-" => axis_direction(&star, [0.0, 0.0, -1.0]),
        other => Err(app_error(format!(
            "unknown prospect direction {other:?}; expected auto, outward, away-sol, \
             toward-sol, toward-star, away-star, +x/-x/+y/-y/+z/-z"
        ))),
    }
}

fn reject_unused_star(star: &Option<String>) -> crate::AnyResult<()> {
    if star.is_some() {
        return Err(app_error(
            "--star is only valid with toward-star or away-star",
        ));
    }
    Ok(())
}

fn axis_direction(star: &Option<String>, vector: [f64; 3]) -> crate::AnyResult<ProspectDirection> {
    reject_unused_star(star)?;
    Ok(ProspectDirection::Axis(Vec3::from_array(vector)))
}

fn default_database() -> PathBuf {
    env::var_os("REPLICANT_DB")
        .map(PathBuf::from)
        .unwrap_or_else(replicant_client::default_database_path)
}

fn validate_selector(selector: &Selector) -> crate::AnyResult<()> {
    if selector.all && !selector.observatories.is_empty() {
        return Err(app_error("--all cannot be combined with --observatory"));
    }
    Ok(())
}

fn required(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> crate::AnyResult<String> {
    arguments
        .next()
        .ok_or_else(|| app_error(format!("{option} requires a value")))
}

fn unknown_option(operation: &str, option: &str) -> crate::AnyError {
    app_error(format!(
        "unknown observatory {operation} option {option:?}; \
         run `replicant-cli observatory {operation} --help`"
    ))
}

fn app_error(message: impl Into<String>) -> crate::AnyError {
    io::Error::new(ErrorKind::InvalidInput, message.into()).into()
}

fn parse_positive_f64(value: &str, option: &str) -> crate::AnyResult<f64> {
    let parsed = value
        .parse::<f64>()
        .map_err(|_| app_error(format!("{option} must be a number")))?;
    if !parsed.is_finite() || parsed <= 0.0 {
        return Err(app_error(format!("{option} must be greater than zero")));
    }
    Ok(parsed)
}

fn parse_positive_usize(value: &str, option: &str) -> crate::AnyResult<usize> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| app_error(format!("{option} must be an integer")))?;
    if parsed == 0 {
        return Err(app_error(format!("{option} must be greater than zero")));
    }
    Ok(parsed)
}

fn parse_vec3(value: &str) -> crate::AnyResult<Vec3> {
    let parts = value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(app_error("coordinate/vector values must be X,Y,Z"));
    }
    let mut coordinates = [0.0_f64; 3];
    for (index, part) in parts.into_iter().enumerate() {
        coordinates[index] = part
            .parse::<f64>()
            .map_err(|_| app_error(format!("invalid coordinate {part:?}")))?;
        if !coordinates[index].is_finite() {
            return Err(app_error("coordinates must be finite numbers"));
        }
    }
    Ok(Vec3::from_array(coordinates))
}

async fn start_client(database: &Path) -> crate::AnyResult<Client> {
    Ok(start_managed_client(ManagedClientConfig::from_env(database)?).await?)
}

async fn run_status(config: StatusConfig) -> crate::AnyResult<()> {
    let client = start_client(&config.database).await?;
    let catalogue = refresh_catalogue(&client).await?;
    let sol = sol_position(&catalogue);
    let mut observatories = load_observatories(&client, &config.selector, &catalogue, sol).await?;
    observatories.sort_by(|left, right| left.code.cmp(&right.code));

    println!("Galactic observatories: {}", observatories.len());
    for observatory in observatories {
        let distance = observatory
            .distance_from_sol_ly
            .map(|value| format!("{value:.1} ly from SOL"))
            .unwrap_or_else(|| "distance unknown".to_owned());
        println!(
            "  {}  {}  {}  {}",
            observatory.code,
            observatory.location(),
            observatory.status.status.as_deref().unwrap_or("unknown"),
            distance
        );
        if let Some(prospect) = &observatory.status.prospect {
            println!(
                "    prospect: {:.1}%  eta={}s  direction={}",
                prospect.progress_percent.unwrap_or(0.0),
                format_optional_number(prospect.eta_seconds),
                format_vector_slice(prospect.direction.as_deref())
            );
        } else if observatory.status.is_prospecting == Some(true) {
            println!("    prospect: active");
        }
        if let Some(value) = observatory.status.extra.get("triangulation") {
            println!("    triangulation: {}", compact_json(value));
        }
        if !observatory.status.available_commands.is_empty() {
            println!(
                "    commands: {}",
                observatory.status.available_commands.join(", ")
            );
        }
    }

    client.close().await?;
    Ok(())
}

async fn run_prospect(config: ProspectConfig) -> crate::AnyResult<()> {
    let client = start_client(&config.database).await?;
    let catalogue = refresh_catalogue(&client).await?;
    let sol = sol_position(&catalogue);
    let mut observatories = load_observatories(&client, &config.selector, &catalogue, sol).await?;

    if config.selector.observatories.is_empty() && !config.selector.all {
        let chosen = choose_best_prospect_observatory(
            &observatories,
            &catalogue,
            sol,
            config.analysis_radius_ly,
        )
        .ok_or_else(|| {
            app_error("no deployed observatory with usable catalogue coordinates was found")
        })?;
        println!(
            "auto-selected observatory {} at {} ({})",
            chosen.code,
            chosen.star.as_deref().unwrap_or(chosen.location()),
            chosen
                .distance_from_sol_ly
                .map(|distance| format!("{distance:.1} ly from SOL"))
                .unwrap_or_else(|| "distance unknown".to_owned())
        );
        observatories = vec![chosen.clone()];
    }

    let mut any_started = false;
    for observatory in observatories {
        if !observatory.advertised("prospect") {
            eprintln!(
                "{}: prospect is not currently advertised (status={}); skipping",
                observatory.code,
                observatory.status.status.as_deref().unwrap_or("unknown")
            );
            continue;
        }
        let attempts = prospect_directions(&config, &observatory, &catalogue, sol)?;
        if attempts.is_empty() {
            return Err(app_error(format!(
                "{}: no usable prospect direction could be calculated",
                observatory.code
            )));
        }

        println!(
            "{} at {}: prospect mode {}",
            observatory.code,
            observatory
                .star
                .as_deref()
                .unwrap_or(observatory.location()),
            direction_label(&config.direction)
        );

        let mut started = false;
        for (index, candidate) in attempts.into_iter().take(config.attempts).enumerate() {
            print_direction_plan(index + 1, &candidate, &config.direction);
            if config.dry_run {
                continue;
            }

            let handle = client.devices().get(&observatory.code).await?;
            let operation = handle.prospect(candidate.direction).await?;
            let outcome = operation.outcome().await?;
            match classify_prospect_outcome(&outcome) {
                ProspectSubmission::Started => {
                    print_started("prospect", &observatory.code, &outcome);
                    started = true;
                    any_started = true;
                    break;
                }
                ProspectSubmission::Blocked(blocked) => {
                    print_blocked(&observatory.code, index + 1, &blocked);
                    if !matches!(&config.direction, ProspectDirection::Auto) {
                        break;
                    }
                }
                ProspectSubmission::Rejected => {
                    print_rejected("prospect", &observatory.code, &outcome);
                    break;
                }
                ProspectSubmission::Ambiguous => {
                    print_ambiguous("prospect", &observatory.code, &operation.id().to_string());
                    break;
                }
            }
        }
        if config.dry_run {
            any_started = true;
        } else if !started {
            eprintln!("{}: no prospect was started", observatory.code);
        }
    }

    client.close().await?;
    if any_started {
        Ok(())
    } else {
        Err(app_error("no observatory prospect was started"))
    }
}

async fn run_triangulate(config: TriangulateConfig) -> crate::AnyResult<()> {
    let client = start_client(&config.database).await?;
    let catalogue = refresh_catalogue(&client).await?;
    let sol = sol_position(&catalogue).unwrap_or(Vec3::ZERO);
    let mut observatories =
        load_observatories(&client, &config.selector, &catalogue, Some(sol)).await?;

    if config.selector.observatories.is_empty() && !config.selector.all {
        observatories.sort_by(fringe_order);
        if observatories.len() > 1 {
            let chosen = observatories.remove(0);
            println!(
                "auto-selected fringe observatory {} at {} ({})",
                chosen.code,
                chosen.star.as_deref().unwrap_or(chosen.location()),
                chosen
                    .distance_from_sol_ly
                    .map(|distance| format!("{distance:.1} ly from SOL"))
                    .unwrap_or_else(|| "distance unknown".to_owned())
            );
            observatories = vec![chosen];
        }
    }

    if observatories.is_empty() {
        return Err(app_error(
            "no Galactic Observatory devices matched the selection",
        ));
    }

    let target_radius_ly = config
        .radius_ly
        .unwrap_or_else(|| automatic_triangulation_radius(&catalogue, sol));
    let auto_targets = if config.target.is_none() {
        let seed = config
            .seed
            .clone()
            .unwrap_or_else(|| auto_target_seed(&config.signature, &observatories));
        Some(spread_targets(
            sol,
            target_radius_ly,
            observatories.len(),
            &seed,
        ))
    } else {
        None
    };

    println!("signature: {}", config.signature);
    if config.target.is_none() {
        println!(
            "automatic target sphere: {:.0} ly from SOL; {} reading(s) spread across the sphere",
            target_radius_ly,
            observatories.len()
        );
    } else if observatories.len() > 1 {
        eprintln!("note: explicit --target is being reused for every selected observatory");
    }

    let mut submitted = 0usize;
    for (index, observatory) in observatories.iter().enumerate() {
        if !observatory.advertised("triangulate") {
            eprintln!(
                "{}: triangulate is not currently advertised (status={}); skipping",
                observatory.code,
                observatory.status.status.as_deref().unwrap_or("unknown")
            );
            continue;
        }
        let target = config
            .target
            .or_else(|| {
                auto_targets
                    .as_ref()
                    .and_then(|targets| targets.get(index).copied())
            })
            .ok_or_else(|| app_error("failed to calculate triangulation target"))?;
        println!(
            "{} at {} -> target {} ({:.0} ly from SOL)",
            observatory.code,
            observatory
                .star
                .as_deref()
                .unwrap_or(observatory.location()),
            format_vec3(target),
            target.sub(sol).norm()
        );
        if config.dry_run {
            submitted += 1;
            continue;
        }

        let handle = client.devices().get(&observatory.code).await?;
        let operation = handle
            .triangulate(config.signature.clone(), target.to_array())
            .await?;
        let outcome = operation.outcome().await?;
        match outcome.status {
            OperationStatus::Rejected | OperationStatus::Failed | OperationStatus::Cancelled => {
                print_rejected("triangulation", &observatory.code, &outcome);
            }
            OperationStatus::Ambiguous => {
                print_ambiguous(
                    "triangulation",
                    &observatory.code,
                    &operation.id().to_string(),
                );
            }
            _ => {
                print_started("triangulation", &observatory.code, &outcome);
                submitted += 1;
            }
        }
    }

    client.close().await?;
    if submitted == 0 {
        Err(app_error("no triangulation was submitted"))
    } else {
        println!(
            "submitted {submitted} triangulation reading(s); completion is asynchronous \
             (documented runtime: about one hour)"
        );
        Ok(())
    }
}

async fn refresh_catalogue(client: &Client) -> crate::AnyResult<Vec<Star>> {
    client.galaxy().refresh_catalogue().await?;
    Ok(client.galaxy().catalogue())
}

async fn load_observatories(
    client: &Client,
    selector: &Selector,
    catalogue: &[Star],
    sol: Option<Vec3>,
) -> crate::AnyResult<Vec<ObservatoryInfo>> {
    let mut codes = Vec::new();
    if !selector.observatories.is_empty() {
        for code in &selector.observatories {
            let handle = client.devices().get(code).await?;
            let snapshot = handle.snapshot().await?;
            if snapshot.device_type.as_ref() != Some(&DeviceType::GalacticObservatory) {
                return Err(app_error(format!(
                    "device {code} is not a galactic_observatory"
                )));
            }
            codes.push(code.clone());
        }
    } else {
        let mut query = client
            .devices()
            .refresh_many()
            .of_type(DeviceType::GalacticObservatory);
        if let Some(tag) = &selector.tag {
            query = query.with_tag(tag.clone());
        }
        codes = query
            .collect()
            .await?
            .into_iter()
            .map(|handle| handle.id().as_str().to_owned())
            .collect();
    }

    codes.sort();
    codes.dedup();
    if codes.is_empty() {
        return Err(app_error(
            "no Galactic Observatory devices matched the selection",
        ));
    }
    let raw_client = client.raw();
    let mut observatories = Vec::with_capacity(codes.len());
    for code in codes {
        let status = raw_client.devices().get(&code).await?.value;
        let star = status
            .location
            .as_deref()
            .and_then(|location| star_for_location(catalogue, location))
            .map(ToOwned::to_owned);
        let position = star
            .as_deref()
            .and_then(|designation| find_star(catalogue, designation))
            .and_then(|star| star.position)
            .map(Vec3::from);
        let distance_from_sol_ly = position
            .zip(sol)
            .map(|(position, sol)| position.sub(sol).norm());
        observatories.push(ObservatoryInfo {
            code,
            status,
            star,
            position,
            distance_from_sol_ly,
        });
    }
    Ok(observatories)
}

fn choose_best_prospect_observatory<'a>(
    observatories: &'a [ObservatoryInfo],
    catalogue: &[Star],
    sol: Option<Vec3>,
    analysis_radius_ly: f64,
) -> Option<&'a ObservatoryInfo> {
    observatories
        .iter()
        .filter(|observatory| observatory.advertised("prospect"))
        .filter_map(|observatory| {
            let position = observatory.position?;
            let local = local_neighbours(catalogue, position, analysis_radius_ly).len();
            let distance = sol.map_or(0.0, |sol| position.sub(sol).norm());
            Some((observatory, local, distance))
        })
        .min_by(|left, right| {
            left.1
                .cmp(&right.1)
                .then_with(|| right.2.partial_cmp(&left.2).unwrap_or(Ordering::Equal))
                .then_with(|| left.0.code.cmp(&right.0.code))
        })
        .map(|(observatory, _, _)| observatory)
}

fn prospect_directions(
    config: &ProspectConfig,
    observatory: &ObservatoryInfo,
    catalogue: &[Star],
    sol: Option<Vec3>,
) -> crate::AnyResult<Vec<ProspectAttempt>> {
    let position = observatory.position;
    let one = |direction: Option<Vec3>, note: String| {
        Ok(vec![ProspectAttempt {
            direction: direction.map(Vec3::to_array),
            note,
        }])
    };

    match &config.direction {
        ProspectDirection::Outward => one(None, "server default: outward from SOL".to_owned()),
        ProspectDirection::Vector(direction) | ProspectDirection::Axis(direction) => one(
            Some(*direction),
            format!("explicit vector {}", format_vec3(*direction)),
        ),
        ProspectDirection::TowardSol => {
            let current = require_position(observatory, position)?;
            let sol =
                sol.ok_or_else(|| app_error("SOL coordinates are missing from the catalogue"))?;
            one(
                Some(non_zero_direction(
                    sol.sub(current),
                    "cannot prospect toward SOL from SOL itself",
                )?),
                format!(
                    "toward SOL from {}",
                    observatory.star.as_deref().unwrap_or("current star")
                ),
            )
        }
        ProspectDirection::AwaySol => {
            let current = require_position(observatory, position)?;
            let sol =
                sol.ok_or_else(|| app_error("SOL coordinates are missing from the catalogue"))?;
            one(
                Some(non_zero_direction(
                    current.sub(sol),
                    "cannot derive an away-from-SOL vector while located at SOL",
                )?),
                format!(
                    "away from SOL from {}",
                    observatory.star.as_deref().unwrap_or("current star")
                ),
            )
        }
        ProspectDirection::TowardStar(target) | ProspectDirection::AwayStar(target) => {
            let current = require_position(observatory, position)?;
            let target_star = find_star(catalogue, target).ok_or_else(|| {
                app_error(format!("star {target:?} is not in the current catalogue"))
            })?;
            let target_position = target_star.position.map(Vec3::from).ok_or_else(|| {
                app_error(format!("star {target:?} has no catalogue coordinates"))
            })?;
            let toward = target_position.sub(current);
            let direction = if matches!(&config.direction, ProspectDirection::AwayStar(_)) {
                toward.scale(-1.0)
            } else {
                toward
            };
            let direction = non_zero_direction(
                direction,
                "target star is the observatory's current star; choose another star",
            )?;
            one(
                Some(direction),
                format!(
                    "{} {target}",
                    if matches!(&config.direction, ProspectDirection::AwayStar(_)) {
                        "away from"
                    } else {
                        "toward"
                    }
                ),
            )
        }
        ProspectDirection::Auto => {
            let current = require_position(observatory, position)?;
            let candidates = rank_sparse_directions(
                catalogue,
                current,
                sol,
                config.analysis_radius_ly,
                config.samples,
            );
            Ok(candidates
                .into_iter()
                .map(|candidate| ProspectAttempt {
                    direction: Some(candidate.direction.to_array()),
                    note: format!(
                        "local estimate: {} forward neighbours within {:.1} ly, \
                         weighted density {:.3}, outward alignment {:.3}",
                        candidate.forward_neighbours,
                        config.analysis_radius_ly,
                        candidate.weighted_density,
                        candidate.outward_alignment
                    ),
                })
                .collect())
        }
    }
}

fn non_zero_direction(direction: Vec3, message: &str) -> crate::AnyResult<Vec3> {
    if direction.norm() <= f64::EPSILON {
        return Err(app_error(message));
    }
    Ok(direction)
}

fn require_position(
    observatory: &ObservatoryInfo,
    position: Option<Vec3>,
) -> crate::AnyResult<Vec3> {
    position.ok_or_else(|| {
        app_error(format!(
            "{} at {} could not be mapped to catalogue coordinates",
            observatory.code,
            observatory.location()
        ))
    })
}

#[derive(Clone, Debug)]
struct ProspectAttempt {
    direction: Option<[f64; 3]>,
    note: String,
}

fn rank_sparse_directions(
    catalogue: &[Star],
    origin: Vec3,
    sol: Option<Vec3>,
    radius_ly: f64,
    samples: usize,
) -> Vec<DirectionCandidate> {
    let neighbours = local_neighbours(catalogue, origin, radius_ly);
    let outward = sol
        .and_then(|sol| origin.sub(sol).normalized())
        .unwrap_or(Vec3::from_array([1.0, 0.0, 0.0]));

    let mut directions = vec![
        outward,
        Vec3::from_array([1.0, 0.0, 0.0]),
        Vec3::from_array([-1.0, 0.0, 0.0]),
        Vec3::from_array([0.0, 1.0, 0.0]),
        Vec3::from_array([0.0, -1.0, 0.0]),
        Vec3::from_array([0.0, 0.0, 1.0]),
        Vec3::from_array([0.0, 0.0, -1.0]),
    ];
    directions.extend(fibonacci_unit_sphere(samples, 0.0));

    let mut unique: Vec<Vec3> = Vec::new();
    for direction in directions {
        let Some(direction) = direction.normalized() else {
            continue;
        };
        if unique
            .iter()
            .any(|existing| existing.dot(direction) > 0.999_95)
        {
            continue;
        }
        unique.push(direction);
    }

    let mut ranked = unique
        .into_iter()
        .map(|direction| {
            let mut forward_neighbours = 0usize;
            let mut weighted_density = 0.0;
            for delta in &neighbours {
                let distance = delta.norm();
                let Some(unit) = delta.normalized() else {
                    continue;
                };
                let alignment = direction.dot(unit);
                if alignment > 0.0 {
                    forward_neighbours += 1;
                    weighted_density += alignment / distance.max(0.25);
                }
            }
            DirectionCandidate {
                direction,
                forward_neighbours,
                weighted_density,
                outward_alignment: direction.dot(outward),
            }
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| {
        left.forward_neighbours
            .cmp(&right.forward_neighbours)
            .then_with(|| {
                left.weighted_density
                    .partial_cmp(&right.weighted_density)
                    .unwrap_or(Ordering::Equal)
            })
            .then_with(|| {
                right
                    .outward_alignment
                    .partial_cmp(&left.outward_alignment)
                    .unwrap_or(Ordering::Equal)
            })
    });
    ranked
}

fn local_neighbours(catalogue: &[Star], origin: Vec3, radius_ly: f64) -> Vec<Vec3> {
    catalogue
        .iter()
        .filter_map(|star| star.position.map(Vec3::from))
        .map(|position| position.sub(origin))
        .filter(|delta| {
            let distance = delta.norm();
            distance > 1e-6 && distance <= radius_ly
        })
        .collect()
}

fn automatic_triangulation_radius(catalogue: &[Star], sol: Vec3) -> f64 {
    let farthest_catalogued = catalogue
        .iter()
        .filter_map(|star| star.position.map(Vec3::from))
        .map(|position| position.sub(sol).norm())
        .filter(|distance| distance.is_finite())
        .fold(0.0_f64, f64::max);
    MIN_TRIANGULATION_RADIUS_LY.max(farthest_catalogued + TRIANGULATION_FRINGE_MARGIN_LY)
}

fn auto_target_seed(signature: &str, observatories: &[ObservatoryInfo]) -> String {
    let mut codes = observatories
        .iter()
        .map(|observatory| observatory.code.as_str())
        .collect::<Vec<_>>();
    codes.sort_unstable();
    format!("{signature}:{}", codes.join(","))
}

fn spread_targets(origin: Vec3, radius_ly: f64, count: usize, seed: &str) -> Vec<Vec3> {
    let count = count.max(1);
    let phase = stable_phase(seed);
    fibonacci_unit_sphere(count, phase)
        .into_iter()
        .map(|direction| origin.add(direction.scale(radius_ly)))
        .collect()
}

fn fibonacci_unit_sphere(count: usize, phase: f64) -> Vec<Vec3> {
    if count == 0 {
        return Vec::new();
    }
    (0..count)
        .map(|index| {
            let z = 1.0 - 2.0 * ((index as f64 + 0.5) / count as f64);
            let radial = (1.0 - z * z).max(0.0).sqrt();
            let theta = phase + GOLDEN_ANGLE * index as f64;
            Vec3 {
                x: radial * theta.cos(),
                y: radial * theta.sin(),
                z,
            }
        })
        .collect()
}

fn stable_phase(seed: &str) -> f64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in seed.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let fraction = hash as f64 / u64::MAX as f64;
    fraction * 2.0 * PI
}

fn find_star<'a>(catalogue: &'a [Star], designation: &str) -> Option<&'a Star> {
    catalogue
        .iter()
        .find(|star| star.key.id.as_str().eq_ignore_ascii_case(designation))
}

fn sol_position(catalogue: &[Star]) -> Option<Vec3> {
    find_star(catalogue, "SOL")
        .and_then(|star| star.position)
        .map(Vec3::from)
}

fn star_for_location<'a>(catalogue: &'a [Star], location: &str) -> Option<&'a str> {
    catalogue
        .iter()
        .filter_map(|star| {
            let designation = star.key.id.as_str();
            let prefix_matches = location
                .get(..designation.len())
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case(designation));
            let boundary_matches = location.len() == designation.len()
                || location
                    .get(designation.len()..)
                    .is_some_and(|suffix| suffix.starts_with('-'));
            (prefix_matches && boundary_matches).then_some(designation)
        })
        .max_by_key(|designation| designation.len())
}

fn fringe_order(left: &ObservatoryInfo, right: &ObservatoryInfo) -> Ordering {
    right
        .distance_from_sol_ly
        .partial_cmp(&left.distance_from_sol_ly)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left.code.cmp(&right.code))
}

#[derive(Clone, Debug, PartialEq)]
enum ProspectSubmission {
    Started,
    Blocked(ProspectBlocked),
    Rejected,
    Ambiguous,
}

fn classify_prospect_outcome(outcome: &OperationOutcome) -> ProspectSubmission {
    if outcome.status == OperationStatus::Ambiguous {
        return ProspectSubmission::Ambiguous;
    }
    if matches!(
        outcome.status,
        OperationStatus::Rejected | OperationStatus::Failed | OperationStatus::Cancelled
    ) {
        if let Some(blocked) = prospect_blocked(outcome.response.as_ref())
            && blocked.message.as_deref().is_some_and(|message| {
                message
                    .to_ascii_lowercase()
                    .contains("no new stars visible")
            })
        {
            return ProspectSubmission::Blocked(blocked);
        }
        return ProspectSubmission::Rejected;
    }
    ProspectSubmission::Started
}

fn prospect_blocked(response: Option<&Value>) -> Option<ProspectBlocked> {
    let server = response?.get("server")?;
    let message = server
        .get("error")
        .or_else(|| server.get("message"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let detail = server.get("detail");
    Some(ProspectBlocked {
        message,
        neighbours: detail
            .and_then(|value| value.get("neighbours"))
            .and_then(Value::as_f64),
        outward_neighbours: detail
            .and_then(|value| value.get("outward_neighbours"))
            .and_then(Value::as_f64),
        expected: detail
            .and_then(|value| value.get("expected"))
            .and_then(Value::as_f64),
        ratio: detail
            .and_then(|value| value.get("ratio"))
            .and_then(Value::as_f64),
        outward_ratio: detail
            .and_then(|value| value.get("outward_ratio"))
            .and_then(Value::as_f64),
    })
}

fn print_direction_plan(attempt: usize, candidate: &ProspectAttempt, mode: &ProspectDirection) {
    let direction = candidate
        .direction
        .map(|value| format_vec3(Vec3::from_array(value)))
        .unwrap_or_else(|| "<omitted>".to_owned());
    if matches!(mode, ProspectDirection::Auto) {
        println!("  attempt {attempt}: direction {direction}");
    } else {
        println!("  direction: {direction}");
    }
    println!("    {}", candidate.note);
}

fn print_started(kind: &str, code: &str, outcome: &OperationOutcome) {
    let completes = outcome
        .response
        .as_ref()
        .and_then(|response| response.get("completes_at"))
        .and_then(Value::as_str);
    match completes {
        Some(completes) => println!("{code}: {kind} accepted; completes at {completes}"),
        None => println!("{code}: {kind} accepted ({:?})", outcome.status),
    }
}

fn print_rejected(kind: &str, code: &str, outcome: &OperationOutcome) {
    eprintln!("{code}: {kind} rejected ({:?})", outcome.status);
    if let Some(response) = &outcome.response {
        if let Some(server) = response.get("server") {
            eprintln!("  server: {}", compact_json(server));
        } else if let Some(message) = response.get("message").and_then(Value::as_str) {
            eprintln!("  {message}");
        }
    }
}

fn print_ambiguous(kind: &str, code: &str, operation_id: &str) {
    eprintln!(
        "{code}: {kind} submission is ambiguous; it was NOT retried. Operation: {operation_id}"
    );
}

fn print_blocked(code: &str, attempt: usize, blocked: &ProspectBlocked) {
    eprintln!(
        "{code}: prospect attempt {attempt} blocked: {}",
        blocked
            .message
            .as_deref()
            .unwrap_or("server rejected fringe prospect")
    );
    if blocked.neighbours.is_some()
        || blocked.outward_neighbours.is_some()
        || blocked.expected.is_some()
        || blocked.ratio.is_some()
        || blocked.outward_ratio.is_some()
    {
        eprintln!(
            "  neighbours={}  directional={}  expected={}  ratio={}  directional_ratio={}",
            format_optional(blocked.neighbours),
            format_optional(blocked.outward_neighbours),
            format_optional(blocked.expected),
            format_optional(blocked.ratio),
            format_optional(blocked.outward_ratio)
        );
    }
}

fn direction_label(direction: &ProspectDirection) -> &'static str {
    match direction {
        ProspectDirection::Auto => "auto",
        ProspectDirection::Outward => "outward",
        ProspectDirection::AwaySol => "away-sol",
        ProspectDirection::TowardSol => "toward-sol",
        ProspectDirection::TowardStar(_) => "toward-star",
        ProspectDirection::AwayStar(_) => "away-star",
        ProspectDirection::Vector(_) => "vector",
        ProspectDirection::Axis(_) => "axis",
    }
}

fn format_optional(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.3}"))
        .unwrap_or_else(|| "?".to_owned())
}

fn format_optional_number(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.0}"))
        .unwrap_or_else(|| "?".to_owned())
}

fn format_vector_slice(value: Option<&[f64]>) -> String {
    let Some(value) = value else {
        return "?".to_owned();
    };
    if value.len() != 3 {
        return compact_json(&serde_json::json!(value));
    }
    format!("[{:.4}, {:.4}, {:.4}]", value[0], value[1], value[2])
}

fn format_vec3(value: Vec3) -> String {
    format!("[{:.3}, {:.3}, {:.3}]", value.x, value.y, value.z)
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unprintable>".to_owned())
}

fn print_help() {
    println!(
        "Galactic Observatory operations\n\n\
Usage:\n  replicant-cli observatory OPERATION [OPTIONS]\n\n\
Operations:\n  status       List observatories, locations, current prospect state, and commands\n  prospect     Start an automated or explicitly-directed fringe prospect\n  triangulate  Start spectral triangulation from automatic deep-space targets\n\n\
Selection:\n  --observatory CODE   Select one observatory; may be repeated\n  --all                Select every matching observatory\n  --tag TAG            Restrict discovery to observatories carrying TAG\n\n\
Examples:\n  replicant-cli observatory prospect\n  replicant-cli observatory prospect --direction toward-sol\n  replicant-cli observatory prospect --direction toward-star --star SCEPTURUM\n  replicant-cli observatory prospect --vector 0,-1,0\n\n  replicant-cli observatory triangulate --all\n  replicant-cli observatory triangulate --all --radius 20000\n  replicant-cli observatory triangulate --target 5000,14000,100\n\n\
The current default event signature is 934d3ac4dcc918ad. Override it with\n\
--signature or RS_OBSERVATORY_SIGNATURE when Bill publishes a new signature."
    );
}

fn print_status_help() {
    println!(
        "Usage:\n  replicant-cli observatory status [OPTIONS]\n\n\
Options:\n  --observatory CODE   Restrict to a device code; may be repeated\n  --all                Explicitly select all observatories\n  --tag TAG            Restrict to a device tag\n  --database PATH      Managed SQLite database\n  -h, --help           Show this help"
    );
}

fn print_prospect_help() {
    println!(
        "Usage:\n  replicant-cli observatory prospect [OPTIONS]\n\n\
Direction modes:\n  auto          Score many hemispheres from the current catalogue and prefer sparse/outward (default)\n  outward       Omit direction and use the server's outward-from-SOL default\n  away-sol      Explicit vector away from SOL\n  toward-sol    Explicit vector toward SOL\n  toward-star   Vector toward --star STAR\n  away-star     Vector away from --star STAR\n  +x/-x/+y/-y/+z/-z  Fixed galactic axis\n\n\
Options:\n  --direction MODE     Prospect direction mode (default: auto)\n  --star STAR          Named star for toward-star/away-star\n  --vector X,Y,Z       Explicit non-zero direction vector\n  --analysis-radius LY Local density radius used by auto mode (default: 20)\n  --samples N          Candidate sphere samples used by auto mode (default: 64)\n  --attempts N         Auto directions to try if the server reports a blocked prospect (default: 4)\n  --observatory CODE   Select one observatory; may be repeated\n  --all                Prospect from every matching observatory\n  --tag TAG            Restrict discovery to a device tag\n  --dry-run            Print selected directions without issuing commands\n  --database PATH      Managed SQLite database\n  -h, --help           Show this help\n\n\
With no device selector, the CLI chooses the locally sparsest usable observatory,\n\
tie-breaking toward the one furthest from SOL. Server blocked diagnostics are\n\
printed verbatim enough to show neighbours, expected density, and ratios."
    );
}

fn print_triangulate_help() {
    println!(
        "Usage:\n  replicant-cli observatory triangulate [OPTIONS]\n\n\
Options:\n  --signature HASH     Spectral signature (default: 934d3ac4dcc918ad or RS_OBSERVATORY_SIGNATURE)\n  --target X,Y,Z       Use an explicit reference point instead of automatic spreading\n  --radius LY          Exact automatic target distance; default is dynamic deep space\n  --seed TEXT          Override the deterministic target-distribution seed\n  --observatory CODE   Select one observatory; may be repeated\n  --all                Use every matching observatory, spreading targets across the sphere\n  --tag TAG            Restrict discovery to a device tag\n  --dry-run            Print targets without issuing commands\n  --database PATH      Managed SQLite database\n  -h, --help           Show this help\n\n\
With no device selector, the observatory furthest from SOL is selected. Automatic\n\
default radius is at least 15000 ly, or 5000 ly beyond the known fringe if farther.\n\
Targets are deterministic for the signature plus selected observatory codes. Use --seed\n\
to deliberately choose a different spread."
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use replicant_client::domain::{StarId, StarKey};

    fn star(name: &str, position: [f64; 3]) -> Star {
        Star {
            key: StarKey::live(StarId::from(name)),
            name: None,
            spectral_type: None,
            entry_point: None,
            position: Some(replicant_client::domain::GalacticPosition {
                x: position[0],
                y: position[1],
                z: position[2],
            }),
            has_hub: None,
            has_ward: None,
            knowledge_observed: false,
            explored: None,
            has_life: None,
            region: None,
        }
    }

    #[test]
    fn vectors_parse_as_three_finite_coordinates() {
        assert_eq!(
            parse_vec3("5000, 14000, 100").expect("vector"),
            Vec3::from_array([5000.0, 14_000.0, 100.0])
        );
        assert!(parse_vec3("1,2").is_err());
        assert!(parse_vec3("1,NaN,3").is_err());
    }

    #[test]
    fn longest_catalogue_prefix_resolves_location_star() {
        let catalogue = vec![star("A", [0.0, 0.0, 0.0]), star("A-B", [1.0, 0.0, 0.0])];
        assert_eq!(star_for_location(&catalogue, "A-B-BELT-1"), Some("A-B"));
    }

    #[test]
    fn automatic_triangulation_radius_stays_beyond_the_known_fringe() {
        let nearby = vec![
            star("SOL", [0.0, 0.0, 0.0]),
            star("EDGE", [5_000.0, 0.0, 0.0]),
        ];
        assert_eq!(
            automatic_triangulation_radius(&nearby, Vec3::ZERO),
            MIN_TRIANGULATION_RADIUS_LY
        );

        let distant = vec![
            star("SOL", [0.0, 0.0, 0.0]),
            star("EDGE", [18_000.0, 0.0, 0.0]),
        ];
        assert_eq!(
            automatic_triangulation_radius(&distant, Vec3::ZERO),
            23_000.0
        );
    }

    #[test]
    fn automatic_targets_are_far_from_sol_and_spread_apart() {
        let targets = spread_targets(Vec3::ZERO, 15_000.0, 4, "934d3ac4dcc918ad");
        assert_eq!(targets.len(), 4);
        for target in &targets {
            assert!((target.norm() - 15_000.0).abs() < 1e-6);
        }
        for left in 0..targets.len() {
            for right in left + 1..targets.len() {
                let separation = targets[left].sub(targets[right]).norm();
                assert!(separation > 5_000.0, "targets should be widely separated");
            }
        }
    }

    #[test]
    fn sparse_direction_ranking_prefers_the_empty_hemisphere() {
        let catalogue = vec![
            star("SOL", [0.0, 0.0, 0.0]),
            star("OBS", [10.0, 0.0, 0.0]),
            star("DENSE1", [12.0, 0.0, 0.0]),
            star("DENSE2", [12.0, 1.0, 0.0]),
            star("DENSE3", [13.0, -1.0, 0.0]),
        ];
        let ranked = rank_sparse_directions(
            &catalogue,
            Vec3::from_array([10.0, 0.0, 0.0]),
            Some(Vec3::ZERO),
            10.0,
            32,
        );
        assert!(!ranked.is_empty());
        assert_eq!(ranked[0].forward_neighbours, 0);
        assert!(ranked[0].direction.x < 0.5);
    }

    #[test]
    fn blocked_projection_extracts_server_fringe_diagnostics() {
        let response = serde_json::json!({
            "message": "unexpected HTTP status 400: No new stars visible from this location",
            "status": 400,
            "server": {
                "error": "No new stars visible from this location",
                "detail": {
                    "neighbours": 22,
                    "outward_neighbours": 14,
                    "expected": 16.8,
                    "ratio": 1.31,
                    "outward_ratio": 1.667
                }
            }
        });
        assert_eq!(
            prospect_blocked(Some(&response)),
            Some(ProspectBlocked {
                message: Some("No new stars visible from this location".to_owned()),
                neighbours: Some(22.0),
                outward_neighbours: Some(14.0),
                expected: Some(16.8),
                ratio: Some(1.31),
                outward_ratio: Some(1.667),
            })
        );
    }

    #[test]
    fn current_event_signature_constant_matches_bill_update() {
        assert_eq!(DEFAULT_TRIANGULATION_SIGNATURE, "934d3ac4dcc918ad");
    }
}
