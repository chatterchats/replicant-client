use std::{
    collections::BTreeSet,
    env, fs,
    io::{self},
    path::PathBuf,
    time::Duration,
};

use replicant_mining_planner::QuantityMap;
use replicant_runtime::{
    config::ManagedClientConfig,
    mining::{MiningMission, RoutePhase, SitePhase, load_expansion, plan_expansion},
    start_managed_client,
};
use tracing_subscriber::{EnvFilter, prelude::*};

pub(crate) use replicant_runtime::mining::{MiningExpansionRequest, execute_expansion};

const DEFAULT_REPLICANT: &str = "Chats-1";
const DEFAULT_HUB: &str = "SCEPTURUM-BELT-1";
const DEFAULT_PLAN_PATH: &str = "mining-expansion.json";
const DEFAULT_WAIT_SECONDS: u64 = 21_600;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Plan,
    Run,
    Status,
}

struct Config {
    command: Command,
    systems: Vec<String>,
    systems_file: Option<PathBuf>,
    replicant: String,
    hub: String,
    database: PathBuf,
    mission_file: PathBuf,
    replace_plan: bool,
    wait_timeout: Duration,
    max_concurrency: usize,
    verbose: bool,
    log_file: Option<PathBuf>,
    json: bool,
}

impl Config {
    fn parse(arguments: impl IntoIterator<Item = String>) -> crate::AnyResult<Self> {
        let mut arguments = arguments.into_iter().peekable();
        let command = match arguments.next().as_deref() {
            Some("plan") => Command::Plan,
            Some("run") => Command::Run,
            Some("status") => Command::Status,
            Some("-h" | "--help") | None => {
                print_help();
                std::process::exit(0);
            }
            Some(other) => return Err(input_error(format!("unknown command: {other}"))),
        };
        let mut config = Self {
            command,
            systems: Vec::new(),
            systems_file: None,
            replicant: env::var("RS_MINING_REPLICANT").unwrap_or_else(|_| DEFAULT_REPLICANT.into()),
            hub: env::var("RS_MINING_HUB").unwrap_or_else(|_| DEFAULT_HUB.into()),
            database: env::var("REPLICANT_DB")
                .unwrap_or_else(|_| "replicant-client.sqlite".into())
                .into(),
            mission_file: env::var("RS_MINING_PLAN")
                .unwrap_or_else(|_| DEFAULT_PLAN_PATH.into())
                .into(),
            replace_plan: false,
            wait_timeout: Duration::from_secs(
                env::var("RS_MINING_WAIT_TIMEOUT_SECS")
                    .ok()
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(DEFAULT_WAIT_SECONDS),
            ),
            max_concurrency: env::var("RS_MINING_MAX_CONCURRENCY")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(8),
            verbose: env_flag("RS_MINING_VERBOSE"),
            log_file: env::var("RS_MINING_LOG_FILE").ok().map(PathBuf::from),
            json: false,
        };

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--system" => config.systems.push(required(&mut arguments, "--system")?),
                "--systems-file" => {
                    config.systems_file = Some(required(&mut arguments, "--systems-file")?.into())
                }
                "--replicant" => config.replicant = required(&mut arguments, "--replicant")?,
                "--hub" => config.hub = required(&mut arguments, "--hub")?,
                "--database" => config.database = required(&mut arguments, "--database")?.into(),
                "--mission-file" | "--plan-file" => {
                    config.mission_file = required(&mut arguments, &argument)?.into()
                }
                "--replace-plan" => config.replace_plan = true,
                "--wait-timeout-secs" => {
                    config.wait_timeout = Duration::from_secs(
                        required(&mut arguments, "--wait-timeout-secs")?
                            .parse()
                            .map_err(|_| input_error("--wait-timeout-secs must be an integer"))?,
                    )
                }
                "--max-concurrency" => {
                    config.max_concurrency = required(&mut arguments, "--max-concurrency")?
                        .parse()
                        .map_err(|_| input_error("--max-concurrency must be an integer"))?
                }
                "--verbose" => config.verbose = true,
                "--log-file" => {
                    config.log_file = Some(required(&mut arguments, "--log-file")?.into())
                }
                "--json" => config.json = true,
                "-h" | "--help" => {
                    print_help();
                    std::process::exit(0);
                }
                value if value.starts_with('-') => {
                    return Err(input_error(format!("unknown option: {value}")));
                }
                value if command == Command::Plan => config.systems.push(value.to_owned()),
                value => return Err(input_error(format!("unexpected argument: {value}"))),
            }
        }
        if !(1..=32).contains(&config.max_concurrency) {
            return Err(input_error("--max-concurrency must be between 1 and 32"));
        }
        if command != Command::Plan && (!config.systems.is_empty() || config.systems_file.is_some())
        {
            return Err(input_error(
                "system inputs belong on the plan command; run loads the persisted mission",
            ));
        }
        config.hub.make_ascii_uppercase();
        Ok(config)
    }

    fn request(&self) -> crate::AnyResult<MiningExpansionRequest> {
        let mut systems = self.systems.clone();
        if let Some(path) = &self.systems_file {
            let contents = fs::read_to_string(path)?;
            systems.extend(contents.lines().flat_map(|line| {
                line.split_once('#')
                    .map_or(line, |(before, _)| before)
                    .split_whitespace()
                    .map(str::to_owned)
            }));
        }
        let systems = systems
            .into_iter()
            .map(|system| system.trim().to_ascii_uppercase())
            .filter(|system| !system.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        Ok(MiningExpansionRequest {
            systems,
            replicant: self.replicant.clone(),
            hub: self.hub.clone(),
            mission_file: self.mission_file.clone(),
            wait_timeout: self.wait_timeout,
            max_concurrency: self.max_concurrency,
        })
    }
}

pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let config = Config::parse(arguments)?;
    init_logging(&config)?;
    if config.command == Command::Status {
        if !config.mission_file.exists() {
            println!(
                "No mining mission exists at {}.",
                config.mission_file.display()
            );
            return Ok(());
        }
        return render_status(&load_expansion(&config.mission_file)?, config.json);
    }
    if config.command == Command::Run && !config.mission_file.exists() {
        return Err(input_error(format!(
            "no mining mission exists at {}; create one with `replicant-cli mining --plan ...`",
            config.mission_file.display()
        )));
    }

    let client = start_managed_client(ManagedClientConfig::from_env(&config.database)?).await?;
    let request = config.request()?;
    let result = match config.command {
        Command::Plan => {
            let mission = plan_expansion(&client, &request, config.replace_plan).await?;
            render_plan(&mission, &config)
        }
        Command::Run => {
            let report = execute_expansion(&client, &request).await?;
            if config.json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!(
                    "Mining mission {} completed: {} sites operational and {} ferry routes active.",
                    report.mission.mission_id,
                    report.mission.sites.len(),
                    report.mission.routes.len()
                );
            }
            Ok(())
        }
        Command::Status => unreachable!(),
    };
    let close = client.close().await;
    result?;
    close?;
    Ok(())
}

fn render_plan(mission: &MiningMission, config: &Config) -> crate::AnyResult<()> {
    if config.json {
        println!("{}", serde_json::to_string_pretty(mission)?);
        return Ok(());
    }
    println!("Mining expansion plan {}", mission.mission_id);
    println!("Hub:        {}", mission.hub_location);
    println!("Replicant:  {}", mission.selected_replicant);
    println!("Systems:    {}", mission.sites.len());
    println!(
        "Established: {}",
        mission
            .sites
            .iter()
            .filter(|site| site.phase == SitePhase::Operational)
            .count()
    );
    println!("\nSites:");
    for site in &mission.sites {
        println!(
            "  {:<14} {:<24} {:<9} {:?}{}",
            site.system,
            site.belt,
            site.density,
            site.phase,
            if site.missing.is_empty() {
                String::new()
            } else {
                format!("  missing {}", format_quantities(&site.missing))
            }
        );
    }
    println!(
        "\nSite prints:  {}",
        format_quantities(&mission.site_print_requirements)
    );
    println!(
        "Route prints: {}",
        format_quantities(&mission.route_print_requirements)
    );
    println!(
        "Materials:    {}",
        format_quantities(&mission.total_material_cost)
    );
    println!(
        "Print finish: {:.0}s projected",
        mission
            .print_batches
            .iter()
            .map(|batch| batch.projected_finish_seconds)
            .fold(0.0, f64::max)
    );
    println!("Saved plan:   {}", config.mission_file.display());
    println!("Start or continue with: replicant-cli mining --run");
    Ok(())
}

fn render_status(mission: &MiningMission, json: bool) -> crate::AnyResult<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(mission)?);
        return Ok(());
    }
    println!("Mission:    {}", mission.mission_id);
    println!("Phase:      {:?}", mission.phase);
    println!("Hub:        {}", mission.hub_location);
    println!("Replicant:  {}", mission.selected_replicant);
    println!(
        "Sites:      {}/{} operational",
        mission
            .sites
            .iter()
            .filter(|site| site.phase == SitePhase::Operational)
            .count(),
        mission.sites.len()
    );
    println!(
        "Routes:     {}/{} active",
        mission
            .routes
            .iter()
            .filter(|route| route.phase == RoutePhase::Active)
            .count(),
        mission.routes.len()
    );
    println!(
        "Printing:   {}/{} produced",
        mission
            .print_batches
            .iter()
            .map(|batch| batch.produced_codes.len())
            .sum::<usize>(),
        mission
            .print_batches
            .iter()
            .filter_map(|batch| usize::try_from(batch.quantity).ok())
            .sum::<usize>()
    );
    for site in &mission.sites {
        println!(
            "  {:<14} site={:?} carrier={}",
            site.system,
            site.phase,
            site.carrier.as_deref().unwrap_or("-")
        );
    }
    if !mission.warnings.is_empty() {
        println!("Warnings:");
        for warning in &mission.warnings {
            println!("  - {warning}");
        }
    }
    Ok(())
}

fn init_logging(config: &Config) -> crate::AnyResult<()> {
    if !config.verbose && config.log_file.is_none() {
        return Ok(());
    }
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("warn,replicant_runtime::mining=info,replicant_client::ops=info")
    });
    match (&config.log_file, config.verbose) {
        (None, true) => tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
            .try_init()?,
        (Some(path), verbose) => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)?;
            }
            let file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
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

fn format_quantities(quantities: &QuantityMap) -> String {
    if quantities.is_empty() {
        return "none".into();
    }
    quantities
        .iter()
        .filter(|(_, quantity)| **quantity > 0)
        .map(|(name, quantity)| format!("{quantity} {name}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn required(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> crate::AnyResult<String> {
    arguments
        .next()
        .ok_or_else(|| input_error(format!("{option} requires a value")))
}

fn input_error(message: impl Into<String>) -> crate::AnyError {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

fn env_flag(name: &str) -> bool {
    env::var(name).is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes" | "on"))
}

fn print_help() {
    println!(
        "Replicant mining expansion\n\n\
Usage:\n  replicant-cli mining --plan [SYSTEM ...] [OPTIONS]\n  replicant-cli mining --run [OPTIONS]\n  replicant-cli mining --status [OPTIONS]\n\n\
Options:\n  --system SYSTEM           Add one target system (repeatable)\n  --systems-file PATH       Read whitespace/newline separated systems\n  --replicant NAME_OR_CODE  Defaults to Chats-1\n  --hub LOCATION            Manufacturing and delivery hub\n  --database PATH           Managed SQLite database\n  --mission-file PATH       Persisted mission (default: mining-expansion.json)\n  --replace-plan            Replace an existing incomplete mission\n  --wait-timeout-secs N     Per-stage wait timeout (default: 21600)\n  --max-concurrency N       Maximum simultaneous carrier deployments (default: 8)\n  --verbose                 Show tracing logs in the terminal\n  --log-file PATH           Append tracing logs to a file\n  --json                    Emit machine-readable output\n  -h, --help                Show this help\n\n\
The plan command is read-only. Run always reconciles the saved mission against\n\
live state and then starts or continues its first incomplete stage."
    );
}
