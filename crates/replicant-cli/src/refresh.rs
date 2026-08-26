use replicant_protocol::{ApproveRefreshRequest, RefreshPhase, StartRefreshRequest};

use crate::{AnyResult, app_error, daemon::DaemonClient};

pub(crate) async fn run_cli(arguments: Vec<String>) -> AnyResult<()> {
    let Some(command) = arguments.first().map(String::as_str) else {
        print_help();
        return Ok(());
    };
    if matches!(command, "-h" | "--help" | "help") {
        print_help();
        return Ok(());
    }
    let client = DaemonClient::from_env();
    match command {
        "start" => start(&client, &arguments[1..]).await,
        "status" => status(&client, &arguments[1..]).await,
        "approve" => approve(&client, &arguments[1..]).await,
        "cancel" => cancel(&client, &arguments[1..]).await,
        other => Err(app_error(format!(
            "unknown refresh command {other:?}; expected start, status, approve, or cancel"
        ))),
    }
}

async fn start(client: &DaemonClient, arguments: &[String]) -> AnyResult<()> {
    let mut phases = Vec::new();
    let mut dry_run = false;
    let mut read_requests_per_minute = None;
    let mut index = 0;
    while index < arguments.len() {
        match arguments[index].as_str() {
            "--phase" => {
                index += 1;
                let value = arguments
                    .get(index)
                    .ok_or_else(|| app_error("--phase requires a phase name"))?;
                phases.push(parse_phase(value)?);
            }
            "--dry-run" => dry_run = true,
            "--read-budget-per-minute" => {
                index += 1;
                let value = arguments
                    .get(index)
                    .ok_or_else(|| app_error("--read-budget-per-minute requires an integer"))?
                    .parse::<u32>()
                    .map_err(|_| app_error("refresh read budget must be an integer"))?;
                if !(1..=60).contains(&value) {
                    return Err(app_error(
                        "refresh read budget must be between 1 and 60 requests per minute",
                    ));
                }
                read_requests_per_minute = Some(value);
            }
            "-h" | "--help" => {
                print_help();
                return Ok(());
            }
            value => return Err(app_error(format!("unknown refresh start option {value:?}"))),
        }
        index += 1;
    }
    let run = client
        .start_refresh(&StartRefreshRequest {
            phases,
            dry_run,
            read_requests_per_minute,
        })
        .await?;
    println!("{}", run.run_id);
    println!("status: replicant-cli refresh status {}", run.run_id);
    Ok(())
}

async fn status(client: &DaemonClient, arguments: &[String]) -> AnyResult<()> {
    match arguments {
        [] => println!(
            "{}",
            serde_json::to_string_pretty(&client.refreshes().await?)?
        ),
        [run_id] => println!(
            "{}",
            serde_json::to_string_pretty(&client.refresh(run_id).await?)?
        ),
        _ => return Err(app_error("usage: replicant-cli refresh status [RUN_ID]")),
    }
    Ok(())
}

async fn approve(client: &DaemonClient, arguments: &[String]) -> AnyResult<()> {
    let [run_id, phase, digest] = arguments else {
        return Err(app_error(
            "usage: replicant-cli refresh approve RUN_ID PHASE DIGEST",
        ));
    };
    let run = client
        .approve_refresh(
            run_id,
            &ApproveRefreshRequest {
                phase: parse_phase(phase)?,
                digest: digest.clone(),
            },
        )
        .await?;
    println!("{}", serde_json::to_string_pretty(&run)?);
    Ok(())
}

async fn cancel(client: &DaemonClient, arguments: &[String]) -> AnyResult<()> {
    let [run_id] = arguments else {
        return Err(app_error("usage: replicant-cli refresh cancel RUN_ID"));
    };
    let run = client.cancel_refresh(run_id).await?;
    println!("{}", serde_json::to_string_pretty(&run)?);
    Ok(())
}

fn parse_phase(value: &str) -> AnyResult<RefreshPhase> {
    match value {
        "account" => Ok(RefreshPhase::Account),
        "devices" => Ok(RefreshPhase::Devices),
        "replicants" => Ok(RefreshPhase::Replicants),
        "stars" => Ok(RefreshPhase::Stars),
        "systems" => Ok(RefreshPhase::Systems),
        "bodies" => Ok(RefreshPhase::Bodies),
        "events" => Ok(RefreshPhase::Events),
        "messages" => Ok(RefreshPhase::Messages),
        "locations" => Ok(RefreshPhase::Locations),
        "inventory" => Ok(RefreshPhase::Inventory),
        "simulations" => Ok(RefreshPhase::Simulations),
        _ => Err(app_error(format!("unknown refresh phase {value:?}"))),
    }
}

pub(crate) fn print_help() {
    println!(
        "Durable managed-state recovery\n\n\
Usage:\n\
  replicant-cli refresh start [--phase PHASE]... [--dry-run] [--read-budget-per-minute N]\n\
  replicant-cli refresh status [RUN_ID]\n\
  replicant-cli refresh approve RUN_ID PHASE DIGEST\n\
  replicant-cli refresh cancel RUN_ID\n\n\
Phases:\n\
  account devices replicants stars systems bodies events messages locations inventory simulations\n\n\
An empty start phase list runs the complete recovery plan through replicantd."
    );
}
