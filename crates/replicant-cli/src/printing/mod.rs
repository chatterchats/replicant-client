//! CLI adapter for `replicant-printing` planner, report, and action APIs.

use std::{
    collections::BTreeSet,
    env,
    error::Error as StdError,
    fs::{self, OpenOptions},
    io,
    path::PathBuf,
    time::Duration,
};

use replicant_printing::{
    PrintRequest,
    managed::{
        ClearOptions, ClearReport, FactoryPrintJobStatus, ManufacturingStatusLine, QueueOptions,
        QueueReport, SystemPrintingStatus, clear_factories_in_system_with_options,
        printing_status_in_system, queue_prints_with_components,
    },
};
use replicant_runtime::{config::ManagedClientConfig, start_managed_client};
use tracing_subscriber::{EnvFilter, prelude::*};

const DEFAULT_HUB: &str = "SCEPTURUM-BELT-1";
const DEFAULT_WAIT_SECONDS: u64 = 21_600;
const DEFAULT_POLL_SECONDS: u64 = 5;

type AnyError = Box<dyn StdError + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> AnyError {
    io::Error::new(kind, message.into()).into()
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Command {
    Queue,
    Clear,
    Status,
}

#[derive(Debug)]
struct Config {
    command: Command,
    hub: String,
    system: String,
    database: PathBuf,
    requests: Vec<PrintRequest>,
    tags: Vec<String>,
    preserve_active_factory_codes: BTreeSet<String>,
    flatpack: bool,
    wait_timeout: Duration,
    poll_interval: Duration,
    verbose: bool,
    log_file: Option<PathBuf>,
    json: bool,
}

impl Config {
    fn from_args_and_env(arguments: impl IntoIterator<Item = String>) -> AnyResult<Self> {
        let mut raw_arguments = arguments.into_iter().collect::<Vec<_>>();
        let command = match raw_arguments.first().map(String::as_str) {
            Some("queue") => {
                raw_arguments.remove(0);
                Command::Queue
            }
            Some("clear") => {
                raw_arguments.remove(0);
                Command::Clear
            }
            Some("status") => {
                raw_arguments.remove(0);
                Command::Status
            }
            _ => Command::Queue,
        };
        let mut arguments = raw_arguments.into_iter();

        let mut hub = env::var("RS_PRINTING_HUB").unwrap_or_else(|_| DEFAULT_HUB.into());
        let mut system = env::var("RS_PRINTING_SYSTEM").ok();
        let mut database = PathBuf::from(
            env::var("REPLICANT_DB").unwrap_or_else(|_| "replicant-client.sqlite".into()),
        );
        let mut requests = Vec::new();
        let mut tags = Vec::new();
        let mut preserve_active_factory_codes = BTreeSet::new();
        let mut flatpack = env_flag("RS_PRINTING_FLATPACK");
        let mut wait_timeout = Duration::from_secs(
            env::var("RS_PRINTING_WAIT_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_WAIT_SECONDS),
        );
        let mut poll_interval = Duration::from_secs(
            env::var("RS_PRINTING_POLL_SECONDS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(DEFAULT_POLL_SECONDS),
        );
        let mut verbose = env_flag("RS_PRINTING_VERBOSE");
        let mut log_file = env::var("RS_PRINTING_LOG_FILE").ok().map(PathBuf::from);
        let mut json = false;

        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--hub" => hub = required_argument(&mut arguments, "--hub")?,
                "--system" if matches!(command, Command::Clear | Command::Status) => {
                    system = Some(required_argument(&mut arguments, "--system")?)
                }
                "--system" => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        "--system is only valid with the clear or status command",
                    ));
                }
                "--database" => {
                    database = PathBuf::from(required_argument(&mut arguments, "--database")?)
                }
                "--print" if matches!(command, Command::Queue | Command::Status) => {
                    let quantity_text = required_argument(&mut arguments, "--print QUANTITY")?;
                    let quantity = quantity_text.parse::<i64>().map_err(|_| {
                        app_error(
                            io::ErrorKind::InvalidInput,
                            format!("--print quantity must be an integer, got {quantity_text:?}"),
                        )
                    })?;
                    let device_type = required_argument(&mut arguments, "--print DEVICE_TYPE")?;
                    requests.push(PrintRequest::new(device_type, quantity));
                }
                "--print" => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        "--print is only valid with the queue or status command",
                    ));
                }
                "--tag" if matches!(command, Command::Queue | Command::Status) => {
                    tags.push(required_argument(&mut arguments, "--tag")?)
                }
                "--tag" => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        "--tag is only valid with the queue or status command",
                    ));
                }
                "--exclude-active" | "--keep-active" if command == Command::Clear => {
                    preserve_active_factory_codes.insert(
                        required_argument(&mut arguments, argument.as_str())?
                            .trim()
                            .to_ascii_uppercase(),
                    );
                }
                "--exclude-active" | "--keep-active" => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        format!("{argument} is only valid with the clear command"),
                    ));
                }
                "--flatpack" if command == Command::Queue => flatpack = true,
                "--flatpack" => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        "--flatpack is only valid with the queue command",
                    ));
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
                    )
                }
                "--poll-seconds" => {
                    poll_interval = Duration::from_secs(
                        required_argument(&mut arguments, "--poll-seconds")?
                            .parse()
                            .map_err(|_| {
                                app_error(
                                    io::ErrorKind::InvalidInput,
                                    "--poll-seconds must be an integer",
                                )
                            })?,
                    )
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
                value => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        format!("unexpected argument {value:?}"),
                    ));
                }
            }
        }

        if command == Command::Queue && requests.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "at least one --print QUANTITY DEVICE_TYPE request is required",
            ));
        }
        if command != Command::Status && poll_interval.is_zero() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--poll-seconds must be greater than zero",
            ));
        }
        if command != Command::Status && wait_timeout.is_zero() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--wait-timeout-secs must be greater than zero",
            ));
        }

        let hub = hub.to_ascii_uppercase();
        let system = system
            .map(|value| system_from_location(&value))
            .unwrap_or_else(|| system_from_location(&hub));
        Ok(Self {
            command,
            hub,
            system,
            database,
            requests,
            tags,
            preserve_active_factory_codes,
            flatpack,
            wait_timeout,
            poll_interval,
            verbose,
            log_file,
            json,
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

fn system_from_location(location: &str) -> String {
    location
        .split('-')
        .next()
        .filter(|system| !system.is_empty())
        .unwrap_or(location)
        .to_ascii_uppercase()
}

fn print_help() {
    println!(
        "Replicant distributed printing\n\n\
Usage:\n  replicant-cli print [--queue] --print QUANTITY DEVICE_TYPE [OPTIONS]\n  replicant-cli print --clear [--system SYSTEM] [OPTIONS]\n  replicant-cli print --status [--system SYSTEM] [--print N DEVICE_TYPE] [OPTIONS]\n\n\
Queue options:\n  --print N DEVICE_TYPE    Queue N devices (repeatable)\n  --hub LOCATION           Autofactory location (default: SCEPTURUM-BELT-1)\n  --tag TAG                Tag every printed device and prerequisite (repeatable)\n  --flatpack               Print requested modular devices compacted for transport\n\n\
Clear options:\n  --system SYSTEM          Clear all Autofactories in this system\n  --hub LOCATION           Derive the clear system from this location\n  --exclude-active CODE   Clear this factory's queue but preserve its active print (repeatable)\n  --keep-active CODE      Alias for --exclude-active\n\n\
Status options:\n  --system SYSTEM          Inspect devices and Autofactories in this system\n  --hub LOCATION           Derive the status system from this location\n  --print N DEVICE_TYPE    Compare live state with a desired quantity (repeatable)\n  --tag TAG                Count only matching completed/in-flight devices (repeatable)\n\n\
Shared options:\n  --database PATH          Managed SQLite database\n  --wait-timeout-secs N    Capacity/completion wait timeout (default: 21600)\n  --poll-seconds N          State poll interval (default: 5)\n  --verbose                 Show tracing logs in the terminal\n  --log-file PATH           Append tracing logs to a file\n  --json                    Emit the final report as JSON\n  -h, --help                Show this help\n\n\
Queueing recursively prints blueprint subdevices in leaf-first waves and waits\n\
for each prerequisite wave to physically finish before queueing its parent.\n\
The final requested devices return after their submissions are accepted. The\n\
clear command removes queued work and stops active prints unless their factory\n\
code is protected with --exclude-active.\n\
The status command is read-only and reconstructs missing outputs and components\n\
from live inventory and Autofactory queues."
    );
}

pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let config = Config::from_args_and_env(arguments)?;
    init_logging(&config)?;
    let client = start_managed_client(ManagedClientConfig::from_env(&config.database)?).await?;
    client.ready().await?;

    match config.command {
        Command::Queue => {
            let options = QueueOptions {
                hub: config.hub.clone(),
                tags: config.tags.clone(),
                flatpack: config.flatpack,
                poll_interval: config.poll_interval,
                wait_timeout: config.wait_timeout,
                factory_codes: None,
            };
            let result = queue_prints_with_components(&client, &config.requests, &options).await;
            let close_result = client.close().await;
            let report = result?;
            close_result?;
            print_queue_report(&config, &report)?;
        }
        Command::Clear => {
            let result = clear_factories_in_system_with_options(
                &client,
                &config.system,
                &ClearOptions {
                    poll_interval: config.poll_interval,
                    wait_timeout: config.wait_timeout,
                    preserve_active_factory_codes: config.preserve_active_factory_codes.clone(),
                },
            )
            .await;
            let close_result = client.close().await;
            let report = result?;
            close_result?;
            print_clear_report(&config, &report)?;
        }
        Command::Status => {
            let result =
                printing_status_in_system(&client, &config.system, &config.requests, &config.tags)
                    .await;
            let close_result = client.close().await;
            let report = result?;
            close_result?;
            print_status_report(&config, &report)?;
        }
    }
    Ok(())
}

fn print_queue_report(config: &Config, report: &QueueReport) -> AnyResult<()> {
    if config.json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    let total = report.queued.values().sum::<i64>();
    let output = if report.flatpack { " flatpacked" } else { "" };
    println!(
        "Queued {total}{output} requested device(s) from {}:",
        config.hub
    );
    for (device_type, quantity) in &report.queued {
        println!("  {quantity:>4}  {device_type}");
    }
    if !report.components_reused.is_empty() {
        println!("Existing prerequisite components reserved:");
        for (device_type, quantity) in &report.components_reused {
            println!("  {quantity:>4}  {device_type}");
        }
    }
    if !report.components_queued.is_empty() {
        println!("Prerequisite components queued and completed:");
        for (device_type, quantity) in &report.components_queued {
            println!("  {quantity:>4}  {device_type}");
        }
    }
    println!("Autofactories:");
    for (factory, quantity) in &report.by_factory {
        println!("  {factory}: {quantity}");
    }
    Ok(())
}

fn print_clear_report(config: &Config, report: &ClearReport) -> AnyResult<()> {
    if config.json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    println!(
        "Cleared {} Autofactor{} in {}:",
        report.factories.len(),
        if report.factories.len() == 1 {
            "y"
        } else {
            "ies"
        },
        report.system
    );
    for factory in &report.factories {
        let location = factory.location.as_deref().unwrap_or("unknown location");
        let cleared = if factory.queue_cleared {
            "queue cleared"
        } else {
            "queue already unavailable/empty"
        };
        let active = if factory.active_print_preserved {
            "active print preserved"
        } else if factory.active_print_stopped {
            "active print stopped"
        } else {
            "no active print"
        };
        println!("  {}  {}  {cleared}, {active}", factory.code, location);
    }
    Ok(())
}

fn print_status_report(config: &Config, report: &SystemPrintingStatus) -> AnyResult<()> {
    if config.json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    println!("Printing status for {}:", report.system);
    if !report.tags.is_empty() {
        println!("  tag filter: {}", report.tags.join(", "));
    }

    if !report.requested.is_empty() {
        println!();
        println!("Requested outputs:");
        print_status_lines(&report.requested);
        if report.remaining_requests.is_empty() {
            println!("  All requested outputs are completed or in flight.");
        } else {
            println!("  Still needs queueing:");
            for (device_type, quantity) in &report.remaining_requests {
                println!("    --print {quantity} {device_type}");
            }
        }
    }

    if !report.prerequisites.is_empty() {
        println!();
        println!("Prerequisites for the still-missing outputs:");
        print_status_lines(&report.prerequisites);
    }
    if !report.missing_component_waves.is_empty() {
        println!("  Missing component waves, leaf first:");
        for (index, wave) in report.missing_component_waves.iter().enumerate() {
            let values = wave
                .iter()
                .map(|(device_type, quantity)| format!("{quantity} {device_type}"))
                .collect::<Vec<_>>()
                .join(", ");
            println!("    {}: {values}", index + 1);
        }
    }

    println!();
    println!("Matching device inventory:");
    if report.inventory.is_empty() {
        println!("  none");
    } else {
        for item in &report.inventory {
            let statuses = item
                .by_status
                .iter()
                .map(|(status, quantity)| format!("{status}={quantity}"))
                .collect::<Vec<_>>()
                .join(", ");
            println!(
                "  {:>4} total  {:>4} free  {:<28}  {}",
                item.total, item.free, item.device_type, statuses
            );
        }
    }

    println!();
    println!("Autofactory work:");
    if report.factories.is_empty() {
        println!("  none in system");
    } else {
        for factory in &report.factories {
            let location = factory.location.as_deref().unwrap_or("unknown location");
            let status = factory.status.as_deref().unwrap_or("unknown");
            println!("  {}  {}  status={status}", factory.code, location);
            if let Some(active) = &factory.active {
                print_factory_job("active", active);
            }
            for queued in &factory.queued {
                print_factory_job("queued", queued);
            }
            if factory.active.is_none() && factory.queued.is_empty() {
                println!("    idle queue");
            }
        }
    }
    Ok(())
}

fn print_status_lines(lines: &[ManufacturingStatusLine]) {
    println!("  NEED  HAVE  ACTIVE  QUEUED  MISS  EXTRA  DEVICE");
    for line in lines {
        println!(
            "  {:>4}  {:>4}  {:>6}  {:>6}  {:>4}  {:>5}  {}",
            line.required,
            line.available,
            line.active,
            line.queued,
            line.missing,
            line.surplus,
            line.device_type
        );
    }
}

fn print_factory_job(kind: &str, job: &FactoryPrintJobStatus) {
    let eta = job
        .eta_seconds
        .map(|seconds| format!(", eta={}s", seconds.round() as i64))
        .unwrap_or_default();
    let tags = if job.tags.is_empty() {
        String::new()
    } else {
        format!(", tags={}", job.tags.join(","))
    };
    let filtered = if job.matches_filter {
        ""
    } else {
        ", outside filter"
    };
    println!(
        "    {kind}: {} x {}{eta}{tags}{filtered}",
        job.quantity, job.device_type
    );
}

fn init_logging(config: &Config) -> AnyResult<()> {
    if !config.verbose && config.log_file.is_none() {
        return Ok(());
    }
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("warn,replicant_printing=info,replicant_client::ops=info")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_print_arguments_are_retained() {
        let requests = [
            PrintRequest::new("autofactory", 6),
            PrintRequest::new("cargo_freighter", 6),
        ];
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].quantity, 6);
        assert_eq!(requests[1].device_type, "cargo_freighter");
    }

    #[test]
    fn clear_and_status_accept_system_or_child_location() {
        assert_eq!(system_from_location("SCEPTURUM"), "SCEPTURUM");
        assert_eq!(system_from_location("SCEPTURUM-BELT-1"), "SCEPTURUM");
        assert_eq!(system_from_location("THYFFAWFF-1-L4"), "THYFFAWFF");
    }

    #[test]
    fn clear_accepts_repeatable_active_print_exclusions() {
        let config = Config::from_args_and_env([
            "clear".to_owned(),
            "--system".to_owned(),
            "SCEPTURUM".to_owned(),
            "--exclude-active".to_owned(),
            "abc123".to_owned(),
            "--keep-active".to_owned(),
            "DEF456".to_owned(),
        ])
        .unwrap();

        assert_eq!(
            config.preserve_active_factory_codes,
            BTreeSet::from(["ABC123".to_owned(), "DEF456".to_owned()])
        );
    }
}
