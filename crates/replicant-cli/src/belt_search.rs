use std::{collections::BTreeSet, env, fs, io, path::PathBuf, time::Duration};

use replicant_runtime::{
    belt_search::{BeltRoutePlan, BeltSearchRequest, BeltSearchResult, execute_belt_search},
    config::ManagedClientConfig,
    start_managed_client,
};
use tracing_subscriber::{EnvFilter, prelude::*};

struct Config {
    database: PathBuf,
    request: BeltSearchRequest,
    log_file: Option<PathBuf>,
    verbose: bool,
}

pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let config = parse(arguments)?;
    init_logging(&config)?;
    let client = start_managed_client(ManagedClientConfig::from_env(&config.database)?).await?;
    let result = execute_belt_search(&client, &config.request).await;
    let close = client.close().await;
    let result = result?;
    close?;
    print_result(
        &result,
        config.request.include_explored,
        config.request.plan_only,
    );
    Ok(())
}

fn parse(arguments: Vec<String>) -> crate::AnyResult<Config> {
    let mut arguments = arguments.into_iter();
    let mut database = env::var_os("REPLICANT_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("replicant-client.sqlite"));
    let mut replicant = env::var("RS_BELT_SEARCH_REPLICANT").unwrap_or_else(|_| "Chats-4".into());
    let mut systems = Vec::new();
    let mut route_start = env::var("RS_BELT_SEARCH_START")
        .ok()
        .map(|v| normalize(&v))
        .transpose()?;
    let mut radius_ly = env::var("RS_BELT_SEARCH_RADIUS_LY")
        .ok()
        .map(|v| positive_f64("RS_BELT_SEARCH_RADIUS_LY", &v))
        .transpose()?;
    let mut system_limit = env::var("RS_BELT_SEARCH_SYSTEM_LIMIT")
        .ok()
        .map(|v| positive_usize("RS_BELT_SEARCH_SYSTEM_LIMIT", &v))
        .transpose()?
        .unwrap_or(80);
    let mut include_explored = env_bool("RS_BELT_SEARCH_INCLUDE_EXPLORED", false)?;
    let mut plan_only = env_bool("RS_BELT_SEARCH_PLAN_ONLY", false)?;
    let mut wait_timeout = Duration::from_secs(
        env::var("RS_BELT_SEARCH_WAIT_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|v| *v > 0)
            .unwrap_or(6 * 60 * 60),
    );
    let mut log_file = env::var_os("RS_BELT_SEARCH_LOG_FILE").map(PathBuf::from);
    let mut verbose = env_bool("RS_BELT_SEARCH_VERBOSE", false)?;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "-h" | "--help" | "help" => {
                print_help();
                std::process::exit(0);
            }
            "--replicant" => replicant = next(&mut arguments, "--replicant")?,
            "--start" | "--center" | "--centre" => {
                route_start = Some(normalize(&next(&mut arguments, &argument)?)?)
            }
            "--range" | "--radius" | "--radius-ly" => {
                radius_ly = Some(positive_f64(&argument, &next(&mut arguments, &argument)?)?)
            }
            "--system-limit" => {
                system_limit =
                    positive_usize("--system-limit", &next(&mut arguments, "--system-limit")?)?
            }
            "--include-explored" => include_explored = true,
            "--plan-only" | "--plan" => plan_only = true,
            "--systems-file" => systems.extend(parse_systems(&fs::read_to_string(next(
                &mut arguments,
                "--systems-file",
            )?)?)?),
            "--database" | "--db" => database = PathBuf::from(next(&mut arguments, &argument)?),
            "--wait-timeout-secs" => {
                wait_timeout = Duration::from_secs(positive_u64(
                    "--wait-timeout-secs",
                    &next(&mut arguments, "--wait-timeout-secs")?,
                )?)
            }
            "--log-file" => log_file = Some(PathBuf::from(next(&mut arguments, "--log-file")?)),
            "--verbose" => verbose = true,
            other if other.starts_with('-') => {
                return Err(error(format!(
                    "unknown belt-search option {other:?}; run `replicant-cli belt-search --help`"
                )));
            }
            system => systems.extend(parse_systems(system)?),
        }
    }
    let mut seen = BTreeSet::new();
    systems.retain(|system| seen.insert(system.clone()));
    if route_start.is_some() != radius_ly.is_some() {
        return Err(error(
            "automatic belt-search routing requires both --start LOCATION|SYSTEM and --range LY",
        ));
    }
    if route_start.is_some() && !systems.is_empty() {
        return Err(error(
            "explicit SYSTEM arguments/--systems-file cannot be combined with --start/--range automatic routing",
        ));
    }
    if route_start.is_none() && systems.is_empty() {
        return Err(error(
            "belt-search requires SYSTEM..., --systems-file PATH, or --start LOCATION|SYSTEM --range LY",
        ));
    }
    Ok(Config {
        database,
        request: BeltSearchRequest {
            replicant,
            systems,
            route_start,
            radius_ly,
            system_limit,
            include_explored,
            plan_only,
            wait_timeout,
        },
        log_file,
        verbose,
    })
}

fn print_result(result: &BeltSearchResult, include_explored: bool, plan_only: bool) {
    println!("Belt search");
    println!(
        "Replicant: {} ({})",
        result.replicant_name, result.replicant_code
    );
    if let Some(plan) = &result.route {
        print_route(plan, include_explored);
    } else {
        println!("Systems: {}", result.systems.join(" -> "));
    }
    println!();
    if plan_only {
        if result.route.is_none() {
            println!("Plan only: {} explicit system(s)", result.systems.len());
            for (i, system) in result.systems.iter().enumerate() {
                println!("  {:>3}. {system}", i + 1);
            }
        }
        return;
    }
    for (i, stop) in result.stops.iter().enumerate() {
        println!("[{}/{}] {}", i + 1, result.stops.len(), stop.system);
        println!(
            "  scan: {}",
            if stop.scanned {
                "completed"
            } else {
                "already known"
            }
        );
        if stop.belts.is_empty() {
            println!("  belts: none");
        } else {
            for belt in &stop.belts {
                println!(
                    "  belt: {:<8} {:<24} {:<15} {}",
                    belt.density,
                    belt.designation,
                    belt.radii(),
                    belt.resources()
                );
            }
        }
        println!();
    }
    let belts = result
        .stops
        .iter()
        .flat_map(|stop| &stop.belts)
        .collect::<Vec<_>>();
    let systems_with_belts = belts
        .iter()
        .map(|belt| belt.system.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    let count = |density: &str| {
        belts
            .iter()
            .filter(|belt| belt.density.eq_ignore_ascii_case(density))
            .count()
    };
    println!(
        "Belt search complete: {} system(s), {} belt(s) in {} system(s) [dense {}, moderate {}, sparse {}]",
        result.systems.len(),
        belts.len(),
        systems_with_belts,
        count("dense"),
        count("moderate"),
        count("sparse")
    );
}

fn print_route(plan: &BeltRoutePlan, include_explored: bool) {
    let unexplored = plan.stops.iter().filter(|stop| !stop.explored).count();
    let explored = plan.stops.len() - unexplored;
    println!("Route plan:");
    if plan.requested_start == plan.start_system {
        println!("  Start: {}", plan.start_system);
    } else {
        println!(
            "  Start: {} (system {})",
            plan.requested_start, plan.start_system
        );
    }
    println!(
        "  Radius: {:.2} ly\n  Stops: {}\n  New scans: {unexplored}",
        plan.radius_ly,
        plan.stops.len()
    );
    if explored > 0 {
        println!(
            "  Known visits: {explored}{}",
            if include_explored {
                " (included by request)"
            } else {
                " (route anchor)"
            }
        );
    }
    println!("  Route distance: {:.2} ly", plan.optimized_distance_ly);
    println!(
        "  Optimization: {:.2} -> {:.2} ly ({} 2-opt swap(s))",
        plan.nearest_neighbor_distance_ly, plan.optimized_distance_ly, plan.two_opt_swaps
    );
    println!("  Route:");
    for (i, stop) in plan.stops.iter().enumerate() {
        println!(
            "    {:>3}. {:<18} leg {:>7.2} ly  radius {:>7.2} ly  {}",
            i + 1,
            stop.system,
            stop.leg_distance_ly,
            stop.distance_from_start_ly,
            if stop.explored { "known" } else { "scan" }
        );
    }
}

fn init_logging(config: &Config) -> crate::AnyResult<()> {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("replicant_client=info"));
    match (&config.log_file, config.verbose) {
        (None, false) => Ok(()),
        (None, true) => Ok(tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
            .try_init()?),
        (Some(path), verbose) => {
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                fs::create_dir_all(parent)?;
            }
            let file = fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?;
            let registry = tracing_subscriber::registry().with(filter).with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(file),
            );
            if verbose {
                Ok(registry
                    .with(tracing_subscriber::fmt::layer().with_writer(io::stderr))
                    .try_init()?)
            } else {
                Ok(registry.try_init()?)
            }
        }
    }
}

fn parse_systems(value: &str) -> crate::AnyResult<Vec<String>> {
    value
        .split([',', '\n', '\r'])
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(normalize)
        .collect()
}
fn normalize(value: &str) -> crate::AnyResult<String> {
    let value = value.trim();
    if value.is_empty() || value.chars().any(char::is_whitespace) {
        Err(error(
            "system designation must not be empty or contain whitespace",
        ))
    } else {
        Ok(value.to_ascii_uppercase())
    }
}
fn next(args: &mut impl Iterator<Item = String>, option: &str) -> crate::AnyResult<String> {
    args.next()
        .ok_or_else(|| error(format!("{option} requires a value")))
}
fn positive_f64(name: &str, value: &str) -> crate::AnyResult<f64> {
    let parsed: f64 = value
        .parse()
        .map_err(|_| error(format!("{name} must be a positive number")))?;
    if parsed.is_finite() && parsed > 0.0 {
        Ok(parsed)
    } else {
        Err(error(format!("{name} must be a positive number")))
    }
}
fn positive_usize(name: &str, value: &str) -> crate::AnyResult<usize> {
    let parsed = value
        .parse()
        .map_err(|_| error(format!("{name} must be a positive integer")))?;
    if parsed > 0 {
        Ok(parsed)
    } else {
        Err(error(format!("{name} must be greater than zero")))
    }
}
fn positive_u64(name: &str, value: &str) -> crate::AnyResult<u64> {
    let parsed = value
        .parse()
        .map_err(|_| error(format!("{name} must be an integer")))?;
    if parsed > 0 {
        Ok(parsed)
    } else {
        Err(error(format!("{name} must be greater than zero")))
    }
}
fn env_bool(name: &str, default: bool) -> crate::AnyResult<bool> {
    match env::var(name) {
        Ok(v) => match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(error(format!("{name} must be boolean"))),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(e.into()),
    }
}
fn error(message: impl Into<String>) -> crate::AnyError {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}
fn print_help() {
    println!(
        "Fast asteroid-belt search\n\nUsage:\n  replicant-cli belt-search SYSTEM... [OPTIONS]\n  replicant-cli belt-search --systems-file PATH [OPTIONS]\n  replicant-cli belt-search --start LOCATION|SYSTEM --range LY [OPTIONS]"
    );
}
