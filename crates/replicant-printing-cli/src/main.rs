use std::{
    env,
    error::Error as StdError,
    fs::{self, OpenOptions},
    io,
    path::PathBuf,
    time::Duration,
};

use replicant_client::{Client, SecretString, StartupPolicy};
use replicant_printing::{
    PrintRequest,
    managed::{QueueOptions, QueueReport, queue_prints},
};
use tracing_subscriber::{EnvFilter, prelude::*};

const DEFAULT_HUB: &str = "SCEPTURUM-BELT-1";
const DEFAULT_WAIT_SECONDS: u64 = 21_600;
const DEFAULT_POLL_SECONDS: u64 = 5;

type AnyError = Box<dyn StdError + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> AnyError {
    io::Error::new(kind, message.into()).into()
}

#[derive(Debug)]
struct Config {
    hub: String,
    database: PathBuf,
    requests: Vec<PrintRequest>,
    tags: Vec<String>,
    flatpack: bool,
    wait_timeout: Duration,
    poll_interval: Duration,
    verbose: bool,
    log_file: Option<PathBuf>,
    json: bool,
}

impl Config {
    fn from_args_and_env() -> AnyResult<Self> {
        let mut arguments = env::args().skip(1).peekable();
        if arguments.peek().is_some_and(|argument| argument == "queue") {
            arguments.next();
        }

        let mut hub = env::var("RS_PRINTING_HUB").unwrap_or_else(|_| DEFAULT_HUB.into());
        let mut database = PathBuf::from(
            env::var("REPLICANT_DB").unwrap_or_else(|_| "replicant-client.sqlite".into()),
        );
        let mut requests = Vec::new();
        let mut tags = Vec::new();
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
                "--database" => {
                    database = PathBuf::from(required_argument(&mut arguments, "--database")?)
                }
                "--print" => {
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
                "--tag" => tags.push(required_argument(&mut arguments, "--tag")?),
                "--flatpack" => flatpack = true,
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
                        format!("unexpected argument {value:?}; devices must follow --print"),
                    ));
                }
            }
        }

        if requests.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "at least one --print QUANTITY DEVICE_TYPE request is required",
            ));
        }
        if poll_interval.is_zero() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "--poll-seconds must be greater than zero",
            ));
        }

        Ok(Self {
            hub: hub.to_ascii_uppercase(),
            database,
            requests,
            tags,
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

fn print_help() {
    println!(
        "Replicant distributed printing\n\n\
Usage:\n  replicant-printing [queue] --print QUANTITY DEVICE_TYPE [OPTIONS]\n\n\
Options:\n  --print N DEVICE_TYPE    Queue N devices (repeatable)\n  --hub LOCATION           Autofactory location (default: SCEPTURUM-BELT-1)\n  --tag TAG                Tag every printed device (repeatable)\n  --flatpack               Print modular devices compacted for transport\n  --database PATH          Managed SQLite database\n  --wait-timeout-secs N    Maximum queue-capacity wait (default: 21600)\n  --poll-seconds N          Queue-capacity poll interval (default: 5)\n  --verbose                 Show tracing logs in the terminal\n  --log-file PATH           Append tracing logs to a file\n  --json                    Emit the final report as JSON\n  -h, --help                Show this help\n\n\
The command distributes work by projected finish time, submits one device per\n\
queue slot, and returns after all requested work is queued. It does not wait\n\
for the physical devices to finish printing."
    );
}

#[tokio::main]
async fn main() -> AnyResult<()> {
    let config = Config::from_args_and_env()?;
    init_logging(&config)?;
    let token = env::var("RS_API_TOKEN")
        .map(SecretString::from)
        .map_err(|_| app_error(io::ErrorKind::NotFound, "RS_API_TOKEN is not set"))?;
    let client = Client::builder()
        .authentication_token(token)
        .sqlite(&config.database)
        .startup_policy(StartupPolicy::Essential)
        .start()
        .await?;
    client.ready().await?;
    let options = QueueOptions {
        hub: config.hub.clone(),
        tags: config.tags.clone(),
        flatpack: config.flatpack,
        poll_interval: config.poll_interval,
        wait_timeout: config.wait_timeout,
    };
    let result = queue_prints(&client, &config.requests, &options).await;
    let close_result = client.close().await;
    let report = result?;
    close_result?;
    print_report(&config, &report)?;
    Ok(())
}

fn print_report(config: &Config, report: &QueueReport) -> AnyResult<()> {
    if config.json {
        println!("{}", serde_json::to_string_pretty(report)?);
        return Ok(());
    }
    let total = report.queued.values().sum::<i64>();
    let output = if report.flatpack { " flatpacked" } else { "" };
    println!("Queued {total}{output} device(s) from {}:", config.hub);
    for (device_type, quantity) in &report.queued {
        println!("  {quantity:>4}  {device_type}");
    }
    println!("Autofactories:");
    for (factory, quantity) in &report.by_factory {
        println!("  {factory}: {quantity}");
    }
    Ok(())
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
}
