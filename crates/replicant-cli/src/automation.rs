//! CLI entry points for global automation safety and fleet reset operations.

use replicant_protocol::AutomationResetRequest;

use crate::daemon::DaemonClient;

pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let mut arguments = arguments.into_iter();
    match arguments.next().as_deref() {
        Some("reset") => {
            let mut confirmed = false;
            for argument in arguments {
                match argument.as_str() {
                    "--confirm" | "--yes" => confirmed = true,
                    "-h" | "--help" => {
                        print_help();
                        return Ok(());
                    }
                    other => {
                        return Err(crate::app_error(format!(
                            "unknown automation reset option {other:?}"
                        )));
                    }
                }
            }
            if !confirmed {
                return Err(crate::app_error(
                    "automation reset is destructive; rerun with --confirm",
                ));
            }
            let response = DaemonClient::from_env()
                .automation_reset(&AutomationResetRequest { confirmed: true })
                .await?;
            println!(
                "Automation reset started as workflow {}.\nDirector: off\nAutomatic triggers: disabled\nCancelled workflows: {}\nReplicants returning home: {}",
                response.reset_workflow.id.0, response.affected_workflows, response.replicants,
            );
        }
        Some("-h" | "--help") | None => print_help(),
        Some(other) => {
            return Err(crate::app_error(format!(
                "unknown automation command {other:?}"
            )));
        }
    }
    Ok(())
}

fn print_help() {
    println!(
        "Automation control\n\n\
Usage:\n  replicant-cli reset automation --confirm\n  replicant-cli automation reset --confirm\n\n\
reset:\n  Stops the Automation Director, disables automatic triggers, cancels all\n  active workflows, and starts a durable fleet reset. Replicants already\n  travelling are allowed to arrive first; each Replicant then returns to its\n  Director-assigned regional hub system. At home, all stowed devices except\n  replicant_matrix and empty_replicant_matrix are deployed and their tags are\n  cleared.\n\n\
Options:\n  --confirm, --yes   Required destructive-operation confirmation\n  -h, --help         Show this help"
    );
}
