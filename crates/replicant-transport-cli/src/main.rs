use std::{
    env,
    error::Error as StdError,
    fs::{self, OpenOptions},
    io,
    path::PathBuf,
    time::Duration,
};

use replicant_client::{Client, SecretString, StartupPolicy};
use replicant_transport::{
    CarrierPreference, DeliveryOptions, DeliveryRequest, DeviceRequest, ResourceMap,
    execute_delivery, plan_delivery,
};
use tracing_subscriber::{EnvFilter, prelude::*};

type AnyError = Box<dyn StdError + Send + Sync + 'static>;
type AnyResult<T> = Result<T, AnyError>;

fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> AnyError {
    io::Error::new(kind, message.into()).into()
}

#[derive(Clone, Debug)]
struct Config {
    request: DeliveryRequest,
    database: PathBuf,
    wait_timeout: Duration,
    poll_interval: Duration,
    return_carriers: bool,
    unfurl_modular_payload: bool,
    dry_run: bool,
    verbose: bool,
    log_file: Option<PathBuf>,
    json: bool,
}

impl Config {
    fn from_args_and_env() -> AnyResult<Self> {
        let mut origin = env::var("RS_TRANSPORT_ORIGIN").ok();
        let mut destination = None;
        let mut resources = ResourceMap::new();
        let mut devices = Vec::new();
        let mut carrier = None;
        let mut database = PathBuf::from(
            env::var("REPLICANT_DB").unwrap_or_else(|_| "replicant-client.sqlite".into()),
        );
        let mut wait_timeout = Duration::from_secs(
            env::var("RS_TRANSPORT_WAIT_TIMEOUT_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(21_600),
        );
        let mut poll_interval = Duration::from_secs(
            env::var("RS_TRANSPORT_POLL_SECS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(5),
        );
        let mut return_carriers = false;
        let mut unfurl_modular_payload = true;
        let mut dry_run = false;
        let mut verbose = env_flag("RS_TRANSPORT_VERBOSE");
        let mut log_file = env::var("RS_TRANSPORT_LOG_FILE").ok().map(PathBuf::from);
        let mut json = false;

        let mut arguments = env::args().skip(1).peekable();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--origin" => origin = Some(required_argument(&mut arguments, "--origin")?),
                "--destination" => {
                    destination = Some(required_argument(&mut arguments, "--destination")?)
                }
                "--devices" | "--device" => {
                    let quantity = parse_positive_i64(
                        &required_argument(&mut arguments, argument.as_str())?,
                        argument.as_str(),
                    )?;
                    let device_type = required_argument(&mut arguments, argument.as_str())?;
                    devices.push(DeviceRequest {
                        quantity,
                        device_type: normalize_key(&device_type),
                    });
                }
                "--resource" => {
                    let quantity = parse_positive_i64(
                        &required_argument(&mut arguments, "--resource")?,
                        "--resource",
                    )?;
                    let resource = required_argument(&mut arguments, "--resource")?;
                    add_resource(&mut resources, &normalize_resource(&resource), quantity);
                }
                "--carbon" | "--conductive" | "--rares" | "--rare" | "--silicates"
                | "--silicate" | "--structural" | "--volatiles" | "--volatile" => {
                    let quantity = parse_positive_i64(
                        &required_argument(&mut arguments, argument.as_str())?,
                        argument.as_str(),
                    )?;
                    let resource = normalize_resource(argument.trim_start_matches('-'));
                    add_resource(&mut resources, &resource, quantity);
                }
                "--carrier" => {
                    if carrier.is_some() {
                        return Err(app_error(
                            io::ErrorKind::InvalidInput,
                            "--carrier may only be supplied once",
                        ));
                    }
                    let first = required_argument(&mut arguments, "--carrier")?;
                    carrier = Some(if let Ok(quantity) = first.parse::<usize>() {
                        if quantity == 0 {
                            return Err(app_error(
                                io::ErrorKind::InvalidInput,
                                "--carrier count must be positive",
                            ));
                        }
                        CarrierPreference {
                            quantity,
                            device_type: normalize_key(&required_argument(
                                &mut arguments,
                                "--carrier COUNT TYPE",
                            )?),
                        }
                    } else {
                        CarrierPreference {
                            quantity: 1,
                            device_type: normalize_key(&first),
                        }
                    });
                }
                "--database" => {
                    database = PathBuf::from(required_argument(&mut arguments, "--database")?)
                }
                "--wait-timeout-secs" => {
                    wait_timeout = Duration::from_secs(parse_positive_u64(
                        &required_argument(&mut arguments, "--wait-timeout-secs")?,
                        "--wait-timeout-secs",
                    )?);
                }
                "--poll-seconds" => {
                    poll_interval = Duration::from_secs(parse_positive_u64(
                        &required_argument(&mut arguments, "--poll-seconds")?,
                        "--poll-seconds",
                    )?);
                }
                "--return-carriers" => return_carriers = true,
                "--no-unfurl" => unfurl_modular_payload = false,
                "--dry-run" | "--plan" => dry_run = true,
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
                value if value.starts_with('-') => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        format!("unknown option: {value}"),
                    ));
                }
                value => {
                    return Err(app_error(
                        io::ErrorKind::InvalidInput,
                        format!("unexpected argument: {value}"),
                    ));
                }
            }
        }

        let origin = origin.ok_or_else(|| {
            app_error(
                io::ErrorKind::InvalidInput,
                "--origin is required (or set RS_TRANSPORT_ORIGIN)",
            )
        })?;
        let destination = destination
            .ok_or_else(|| app_error(io::ErrorKind::InvalidInput, "--destination is required"))?;
        if resources.is_empty() && devices.is_empty() {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                "specify at least one resource flag, --resource, or --devices",
            ));
        }

        Ok(Self {
            request: DeliveryRequest {
                origin: origin.trim().to_ascii_uppercase(),
                destination: destination.trim().to_ascii_uppercase(),
                resources,
                devices,
                carrier,
            },
            database,
            wait_timeout,
            poll_interval,
            return_carriers,
            unfurl_modular_payload,
            dry_run,
            verbose,
            log_file,
            json,
        })
    }
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

    let plan_result = plan_delivery(&client, &config.request).await;
    let plan = match plan_result {
        Ok(plan) => plan,
        Err(error) => {
            let _ = client.close().await;
            return Err(error.into());
        }
    };

    if config.dry_run {
        let close_result = client.close().await;
        print_plan(&config, &plan)?;
        close_result?;
        return Ok(());
    }

    let result = execute_delivery(
        &client,
        &plan,
        DeliveryOptions {
            wait_timeout: config.wait_timeout,
            poll_interval: config.poll_interval,
            unfurl_modular_payload: config.unfurl_modular_payload,
            return_transports: config.return_carriers,
        },
    )
    .await;
    let close_result = client.close().await;
    let report = result?;
    close_result?;

    if config.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!(
            "Delivered payload from {} to {}.",
            config.request.origin, config.request.destination
        );
        if !report.resources_delivered.is_empty() {
            println!("Resources:");
            for (resource, quantity) in &report.resources_delivered {
                println!("  {quantity:>6}  {resource}");
            }
        }
        if !report.devices_delivered.is_empty() {
            println!("Devices: {}", report.devices_delivered.len());
            for code in &report.devices_delivered {
                println!("  {code}");
            }
        }
        if !report.cargo_transports.is_empty() {
            println!("Cargo transports: {}", report.cargo_transports.join(", "));
        }
        if !report.device_carriers.is_empty() {
            println!("Device carriers: {}", report.device_carriers.join(", "));
        }
    }
    Ok(())
}

fn print_plan(config: &Config, plan: &replicant_transport::DeliveryPlan) -> AnyResult<()> {
    if config.json {
        println!("{}", serde_json::to_string_pretty(plan)?);
        return Ok(());
    }
    println!("Transport plan: {} -> {}", plan.origin, plan.destination);
    if !plan.resource_pickups.is_empty() {
        println!("Resource pickups:");
        for pickup in &plan.resource_pickups {
            let manifest = pickup
                .resources
                .iter()
                .map(|(resource, quantity)| format!("{quantity} {resource}"))
                .collect::<Vec<_>>()
                .join(", ");
            println!("  {}: {manifest}", pickup.location);
        }
        println!("Cargo transports: {}", plan.cargo_transports.join(", "));
    }
    if !plan.payload_devices.is_empty() {
        println!("Device payload:");
        for payload in &plan.payload_devices {
            println!(
                "  {}  {}  from {}",
                payload.code, payload.device_type, payload.origin
            );
        }
        println!("Device carriers: {}", plan.device_carriers.join(", "));
    }
    Ok(())
}

fn required_argument(
    arguments: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    option: &str,
) -> AnyResult<String> {
    arguments.next().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} requires a value"),
        )
    })
}

fn parse_positive_i64(value: &str, option: &str) -> AnyResult<i64> {
    let quantity = value.parse::<i64>().map_err(|_| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} quantity must be an integer"),
        )
    })?;
    if quantity <= 0 {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} quantity must be positive"),
        ));
    }
    Ok(quantity)
}

fn parse_positive_u64(value: &str, option: &str) -> AnyResult<u64> {
    let quantity = value.parse::<u64>().map_err(|_| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be a positive integer"),
        )
    })?;
    if quantity == 0 {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be positive"),
        ));
    }
    Ok(quantity)
}

fn add_resource(resources: &mut ResourceMap, resource: &str, quantity: i64) {
    *resources.entry(resource.to_owned()).or_default() += quantity;
}

fn normalize_key(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('-', "_")
}

fn normalize_resource(value: &str) -> String {
    match normalize_key(value).as_str() {
        "rare" => "rares".into(),
        "silicate" => "silicates".into(),
        "volatile" => "volatiles".into(),
        other => other.to_owned(),
    }
}

fn env_flag(name: &str) -> bool {
    env::var(name).ok().is_some_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

fn init_logging(config: &Config) -> AnyResult<()> {
    if !config.verbose && config.log_file.is_none() {
        return Ok(());
    }
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("warn,replicant_transport=info,replicant_client::ops=info")
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

fn print_help() {
    println!(
        "Replicant point-to-point transport\n\n\
Usage:\n  replicant-transport --origin ORIGIN --destination LOCATION [PAYLOAD] [OPTIONS]\n\n\
Payload:\n  --devices N DEVICE_TYPE   Move N free inactive devices (repeatable)\n  --carbon N                Move Carbon\n  --conductive N            Move Conductive\n  --rares N                 Move Rares\n  --silicates N             Move Silicates\n  --structural N            Move Structural\n  --volatiles N             Move Volatiles\n  --resource N TYPE         Move any open resource type (repeatable)\n\n\
Routing and carrier selection:\n  --origin LOCATION|SYSTEM  Exact source location, or search the whole source system\n  --destination LOCATION    Exact delivery location\n  --carrier TYPE            Require one transport of TYPE\n  --carrier N TYPE          Require N transports of TYPE\n  --return-carriers         Return transports after delivery (exact origin only)\n  --no-unfurl               Leave compacted modular payload compacted after delivery\n\n\
Shared options:\n  --database PATH           Managed SQLite database\n  --wait-timeout-secs N     Travel/state wait timeout (default: 21600)\n  --poll-seconds N          State poll interval (default: 5)\n  --dry-run, --plan         Resolve payload/carriers without moving anything\n  --verbose                 Show transport tracing in the terminal\n  --log-file PATH           Append tracing logs to a file\n  --json                    Emit plan/report as JSON\n  -h, --help                Show this help\n\n\
Examples:\n  replicant-transport --origin SCEPTURUM --devices 36 exotic_matter_injector \\\n    --carrier 1 mobile_fleet --destination TWAFFY-OBJ-1\n\n\
  replicant-transport --origin SCEPTURUM-BELT-1 --rares 400 --volatiles 100 \\\n    --carrier cargo_freighter --destination TWAFFY-OBJ-1\n\n\
If --carrier is omitted, the planner chooses the smallest free transport that\n\
can make the delivery in one trip; if none can, it chooses the largest usable\n\
transport and makes repeated trips. System origins can source payload from\n\
multiple locations inside that system."
    );
}
