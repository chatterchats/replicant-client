use std::{collections::BTreeMap, env, fs, io, path::PathBuf, time::Duration};

use replicant_protocol::{OperationKind, StartWorkflowRequest};
pub use replicant_runtime::survey::{SurveyRequest, execute_survey};
use replicant_runtime::{
    config::ManagedClientConfig,
    start_managed_client,
    survey::{SurveyMode, SurveyOptions, SurveyPlanSummary, execute_survey_route, survey_status},
};
use tracing::error;
use tracing_subscriber::{EnvFilter, prelude::*};

const DEFAULT_MAINTENANCE_INTERVAL: usize = 40;
const DEFAULT_MAINTENANCE_THRESHOLD_PCT: f64 = 25.0;
const DEFAULT_MAINTENANCE_RESUME_PCT: f64 = 95.0;

#[derive(Clone, Copy)]
enum Command {
    Plan,
    Run,
    Status,
}

struct Config {
    command: Command,
    database: PathBuf,
    options: SurveyOptions,
    log_path: Option<PathBuf>,
    verbose: bool,
    direct: bool,
}

pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let standalone_option = arguments
        .iter()
        .find(|argument| matches!(argument.as_str(), "--database" | "--verbose" | "--log-file"))
        .cloned();
    let config = parse(arguments)?;
    if matches!(config.command, Command::Status) {
        print_plan(&survey_status(&config.options.mission_file)?);
        return Ok(());
    }
    if !config.direct {
        if let Some(option) = standalone_option {
            return Err(crate::app_error(format!(
                "{option} configures standalone execution; use --direct or configure replicantd"
            )));
        }
        return crate::workflow::submit(workflow_request(&config.options)?).await;
    }
    if matches!(config.command, Command::Run) && !config.options.mission_file.exists() {
        return Err(app_error(
            io::ErrorKind::NotFound,
            format!(
                "no survey mission exists at {}; create one with `replicant-cli survey --plan`",
                config.options.mission_file.display()
            ),
        ));
    }
    install_tracing(&config)?;
    let client = start_managed_client(ManagedClientConfig::from_env(&config.database)?).await?;
    let result = execute_survey_route(&client, &config.options).await;
    let close = client.close().await;
    if let Err(error) = &result {
        error!(target: "replicant_client::explore", error = %error, "survey-route automation failed; run it again to reconcile and continue the saved mission");
    }
    let summary = result?;
    close?;
    print_plan(&summary);
    Ok(())
}

fn parse(arguments: Vec<String>) -> crate::AnyResult<Config> {
    let mut arguments = arguments.into_iter();
    let command = match arguments.next().as_deref() {
        Some("plan") => Command::Plan,
        Some("run") => Command::Run,
        Some("status") => Command::Status,
        Some("-h" | "--help") | None => {
            print_help();
            std::process::exit(0);
        }
        Some(other) => {
            return Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("unknown command: {other}"),
            ));
        }
    };
    let mut database = env::var_os("REPLICANT_DB")
        .map(PathBuf::from)
        .unwrap_or_else(|| "replicant-client.sqlite".into());
    let mut replicant = env_string("RS_EXPLORE_REPLICANT", "B6BA399E");
    let mut vessel = env_string("RS_EXPLORE_VESSEL", "FD5EA802");
    let mut center = env_string("RS_EXPLORE_CENTER", "SCEPTURUM").to_ascii_uppercase();
    let mut radius_ly = env_f64("RS_EXPLORE_RADIUS_LY", 30.0)?;
    let mut system_limit = env_usize("RS_EXPLORE_SYSTEM_LIMIT", 80)?.max(1);
    let mut star_detail_concurrency =
        env_usize("RS_EXPLORE_STAR_DETAIL_CONCURRENCY", 8)?.clamp(1, 16);
    let mut mission_file = env::var_os("RS_EXPLORE_PLAN")
        .map(PathBuf::from)
        .unwrap_or_else(|| "explore-survey-route.json".into());
    let mut log_path = env::var_os("RS_EXPLORE_LOG_FILE")
        .or_else(|| env::var_os("RS_EXPLORE_LOG"))
        .map(PathBuf::from);
    let mut controller = env::var("RS_EXPLORE_CONTROLLER").ok();
    let mut drones = env::var("RS_EXPLORE_DRONES")
        .ok()
        .map(|value| drone_codes(&value));
    let mut replace_plan =
        env_bool("RS_EXPLORE_REPLACE_PLAN", false)? || env_bool("RS_EXPLORE_REBUILD_PLAN", false)?;
    let mut include_explored = env_bool("RS_EXPLORE_INCLUDE_EXPLORED", false)?;
    let mut travel_timeout =
        Duration::from_secs(env_u64("RS_EXPLORE_TRAVEL_TIMEOUT_SECS", 6 * 60 * 60)?);
    let mut survey_timeout =
        Duration::from_secs(env_u64("RS_EXPLORE_SURVEY_TIMEOUT_SECS", 6 * 60 * 60)?);
    let mut maintenance_home = env::var("RS_EXPLORE_MAINTENANCE_HOME")
        .ok()
        .map(|value| value.to_ascii_uppercase());
    let mut maintenance_interval = env_usize(
        "RS_EXPLORE_MAINTENANCE_INTERVAL_SYSTEMS",
        DEFAULT_MAINTENANCE_INTERVAL,
    )?;
    let mut maintenance_threshold_pct = env_f64(
        "RS_EXPLORE_MAINTENANCE_THRESHOLD_PCT",
        DEFAULT_MAINTENANCE_THRESHOLD_PCT,
    )?;
    let mut maintenance_resume_pct = env_f64(
        "RS_EXPLORE_MAINTENANCE_RESUME_PCT",
        DEFAULT_MAINTENANCE_RESUME_PCT,
    )?;
    let mut maintenance_check_interval =
        Duration::from_secs(env_u64("RS_EXPLORE_MAINTENANCE_CHECK_SECS", 900)?);
    let mut verbose = env_bool("RS_EXPLORE_VERBOSE", false)?;
    let mut direct = false;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--direct" => direct = true,
            "--database" => database = required_path(&mut arguments, "--database")?,
            "--replicant" => replicant = required(&mut arguments, "--replicant")?,
            "--vessel" => vessel = required(&mut arguments, "--vessel")?,
            "--center" => center = required(&mut arguments, "--center")?.to_ascii_uppercase(),
            "--radius" => {
                radius_ly = positive_f64(&required(&mut arguments, "--radius")?, "--radius")?
            }
            "--system-limit" => {
                system_limit = positive_usize(
                    &required(&mut arguments, "--system-limit")?,
                    "--system-limit",
                )?
            }
            "--star-detail-concurrency" => {
                star_detail_concurrency = positive_usize(
                    &required(&mut arguments, "--star-detail-concurrency")?,
                    "--star-detail-concurrency",
                )?
                .clamp(1, 16)
            }
            "--mission-file" | "--plan-file" => {
                mission_file = required_path(&mut arguments, &argument)?
            }
            "--controller" => controller = Some(required(&mut arguments, "--controller")?),
            "--drones" => drones = Some(drone_codes(&required(&mut arguments, "--drones")?)),
            "--replace-plan" | "--rebuild-plan" => replace_plan = true,
            "--include-explored" => include_explored = true,
            "--travel-timeout-secs" => {
                travel_timeout = Duration::from_secs(positive_u64(
                    &required(&mut arguments, "--travel-timeout-secs")?,
                    "--travel-timeout-secs",
                )?)
            }
            "--survey-timeout-secs" => {
                survey_timeout = Duration::from_secs(positive_u64(
                    &required(&mut arguments, "--survey-timeout-secs")?,
                    "--survey-timeout-secs",
                )?)
            }
            "--maintenance-home" => {
                maintenance_home =
                    Some(required(&mut arguments, "--maintenance-home")?.to_ascii_uppercase())
            }
            "--maintenance-interval-systems" => {
                maintenance_interval = positive_usize(
                    &required(&mut arguments, "--maintenance-interval-systems")?,
                    "--maintenance-interval-systems",
                )?
            }
            "--maintenance-threshold-pct" => {
                maintenance_threshold_pct = percentage(
                    &required(&mut arguments, "--maintenance-threshold-pct")?,
                    "--maintenance-threshold-pct",
                )?
            }
            "--maintenance-resume-pct" => {
                maintenance_resume_pct = percentage(
                    &required(&mut arguments, "--maintenance-resume-pct")?,
                    "--maintenance-resume-pct",
                )?
            }
            "--maintenance-check-secs" => {
                maintenance_check_interval = Duration::from_secs(positive_u64(
                    &required(&mut arguments, "--maintenance-check-secs")?,
                    "--maintenance-check-secs",
                )?)
            }
            "--verbose" => verbose = true,
            "--log-file" => log_path = Some(required_path(&mut arguments, "--log-file")?),
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                return Err(app_error(
                    io::ErrorKind::InvalidInput,
                    format!("unexpected argument: {other}"),
                ));
            }
        }
    }
    if drones.as_ref().is_some_and(|values| {
        values.len() != 3
            || values
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != 3
    }) {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "--drones must contain exactly 3 distinct comma-separated codes",
        ));
    }
    if maintenance_resume_pct < maintenance_threshold_pct {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "--maintenance-resume-pct must be greater than or equal to --maintenance-threshold-pct",
        ));
    }
    if !matches!(command, Command::Plan) && replace_plan {
        return Err(app_error(
            io::ErrorKind::InvalidInput,
            "--replace-plan belongs on the plan command",
        ));
    }
    let maintenance_home = maintenance_home.unwrap_or_else(|| center.clone());
    Ok(Config {
        command,
        database,
        options: SurveyOptions {
            mode: if matches!(command, Command::Plan) {
                SurveyMode::Plan
            } else {
                SurveyMode::Run
            },
            replicant,
            vessel,
            center,
            radius_ly,
            system_limit,
            star_detail_concurrency,
            mission_file,
            controller,
            drones,
            replace_plan,
            include_explored,
            travel_timeout,
            survey_timeout,
            maintenance_home,
            maintenance_interval,
            maintenance_threshold_pct,
            maintenance_resume_pct,
            maintenance_check_interval,
        },
        log_path,
        verbose,
        direct,
    })
}

fn workflow_request(options: &SurveyOptions) -> crate::AnyResult<StartWorkflowRequest> {
    let mut parameters = BTreeMap::new();
    parameters.insert("mode".into(), serde_json::to_value(options.mode)?);
    parameters.insert("replicant".into(), options.replicant.clone().into());
    parameters.insert("vessel".into(), options.vessel.clone().into());
    parameters.insert("center".into(), options.center.clone().into());
    parameters.insert("radius_ly".into(), options.radius_ly.into());
    parameters.insert("system_limit".into(), options.system_limit.into());
    parameters.insert(
        "star_detail_concurrency".into(),
        options.star_detail_concurrency.into(),
    );
    parameters.insert(
        "mission_file".into(),
        options.mission_file.to_string_lossy().into_owned().into(),
    );
    if let Some(controller) = &options.controller {
        parameters.insert("controller".into(), controller.clone().into());
    }
    if let Some(drones) = &options.drones {
        parameters.insert("drones_csv".into(), drones.join(",").into());
    }
    parameters.insert("replace_plan".into(), options.replace_plan.into());
    parameters.insert("include_explored".into(), options.include_explored.into());
    parameters.insert(
        "travel_timeout_seconds".into(),
        options.travel_timeout.as_secs().into(),
    );
    parameters.insert(
        "survey_timeout_seconds".into(),
        options.survey_timeout.as_secs().into(),
    );
    parameters.insert(
        "maintenance_home".into(),
        options.maintenance_home.clone().into(),
    );
    parameters.insert(
        "maintenance_interval".into(),
        options.maintenance_interval.into(),
    );
    parameters.insert(
        "maintenance_threshold_pct".into(),
        options.maintenance_threshold_pct.into(),
    );
    parameters.insert(
        "maintenance_resume_pct".into(),
        options.maintenance_resume_pct.into(),
    );
    parameters.insert(
        "maintenance_check_seconds".into(),
        options.maintenance_check_interval.as_secs().into(),
    );
    Ok(StartWorkflowRequest {
        kind: OperationKind("survey.route".to_owned()),
        parameters,
    })
}

fn print_plan(plan: &SurveyPlanSummary) {
    println!("Survey route mission");
    println!(
        "  Replicant: {}\n  Vessel: {}\n  Centre: {}\n  Radius: {:.2} ly",
        plan.replicant, plan.vessel, plan.center, plan.radius_ly
    );
    println!(
        "  Progress: {}/{} stops\n  Phase: {}\n  Route distance: {:.2} ly",
        plan.completed_stops, plan.total_stops, plan.phase, plan.route_distance_ly
    );
    println!(
        "  Maintenance: {} every {} stops at <= {:.1}% (resume >= {:.1}%, check {}s)",
        plan.maintenance_home,
        plan.maintenance_interval,
        plan.maintenance_threshold_pct,
        plan.maintenance_resume_pct,
        plan.maintenance_check_seconds
    );
    if let Some(next) = &plan.next_system {
        println!("  Next system: {next}");
    }
    if let Some(controller) = &plan.controller {
        println!("  Survey controller: {controller}");
    }
    if !plan.drones.is_empty() {
        println!("  Survey drones: {}", plan.drones.join(", "));
    }
}

fn install_tracing(config: &Config) -> crate::AnyResult<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("replicant_client=info,replicant_client::explore=debug")
    });
    match (&config.log_path, config.verbose) {
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

fn required(args: &mut impl Iterator<Item = String>, option: &str) -> crate::AnyResult<String> {
    args.next().ok_or_else(|| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} requires a value"),
        )
    })
}
fn required_path(
    args: &mut impl Iterator<Item = String>,
    option: &str,
) -> crate::AnyResult<PathBuf> {
    Ok(required(args, option)?.into())
}
fn drone_codes(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
        .collect()
}
fn positive_usize(value: &str, option: &str) -> crate::AnyResult<usize> {
    let parsed = value.parse().map_err(|_| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be an integer"),
        )
    })?;
    if parsed > 0 {
        Ok(parsed)
    } else {
        Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be greater than zero"),
        ))
    }
}
fn positive_u64(value: &str, option: &str) -> crate::AnyResult<u64> {
    let parsed = value.parse().map_err(|_| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be an integer"),
        )
    })?;
    if parsed > 0 {
        Ok(parsed)
    } else {
        Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be greater than zero"),
        ))
    }
}
fn positive_f64(value: &str, option: &str) -> crate::AnyResult<f64> {
    let parsed: f64 = value.parse().map_err(|_| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be numeric"),
        )
    })?;
    if parsed > 0.0 && parsed.is_finite() {
        Ok(parsed)
    } else {
        Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be finite and greater than zero"),
        ))
    }
}
fn percentage(value: &str, option: &str) -> crate::AnyResult<f64> {
    let parsed: f64 = value.parse().map_err(|_| {
        app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be numeric"),
        )
    })?;
    if (0.0..=100.0).contains(&parsed) {
        Ok(parsed)
    } else {
        Err(app_error(
            io::ErrorKind::InvalidInput,
            format!("{option} must be between 0 and 100"),
        ))
    }
}
fn env_string(name: &str, default: &str) -> String {
    env::var(name).unwrap_or_else(|_| default.into())
}
fn env_bool(name: &str, default: bool) -> crate::AnyResult<bool> {
    match env::var(name) {
        Ok(v) => match v.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            _ => Err(app_error(
                io::ErrorKind::InvalidInput,
                format!("{name} must be boolean"),
            )),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(e.into()),
    }
}
fn env_usize(name: &str, default: usize) -> crate::AnyResult<usize> {
    Ok(env::var(name)
        .ok()
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(default))
}
fn env_u64(name: &str, default: u64) -> crate::AnyResult<u64> {
    Ok(env::var(name)
        .ok()
        .map(|v| v.parse())
        .transpose()?
        .unwrap_or(default))
}
fn env_f64(name: &str, default: f64) -> crate::AnyResult<f64> {
    match env::var(name) {
        Ok(v) => positive_f64(&v, name),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(e.into()),
    }
}
fn app_error(kind: io::ErrorKind, message: impl Into<String>) -> crate::AnyError {
    io::Error::new(kind, message.into()).into()
}
fn print_help() {
    println!(
        "Replicant survey route\n\nUsage:\n  replicant-cli survey --plan [OPTIONS]\n  replicant-cli survey --run [OPTIONS]\n  replicant-cli survey --status [OPTIONS]\n\nPlan and run submit durable replicantd workflows by default. Use --direct for\ndiagnostic standalone execution with a local managed client."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn daemon_request_contains_typed_survey_options() {
        let config = parse(vec![
            "run".into(),
            "--replicant".into(),
            "TEST-1".into(),
            "--system-limit".into(),
            "12".into(),
        ])
        .expect("config");
        let request = workflow_request(&config.options).expect("request");
        assert_eq!(request.kind.0, "survey.route");
        assert_eq!(request.parameters["replicant"], "TEST-1");
        assert_eq!(request.parameters["system_limit"], 12);
    }
}
