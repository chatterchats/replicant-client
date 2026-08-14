use std::collections::BTreeMap;

use replicant_protocol::{
    DaemonHealth, DescriptorCatalog, HealthStatus, OperationKind, RunOperationRequest,
    StartWorkflowRequest, WorkflowDetail, WorkflowStatus, WorkflowSummary,
};
use serde_json::Value;

use crate::daemon::DaemonClient;

pub(crate) async fn run_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let mut arguments = arguments.into_iter();
    match arguments.next().as_deref() {
        Some("list") => {
            reject_extra(arguments)?;
            let response = DaemonClient::from_env().workflows().await?;
            print!("{}", render_list(&response.workflows));
        }
        Some("catalogue" | "catalog") => {
            reject_extra(arguments)?;
            print!(
                "{}",
                render_catalogue(&DaemonClient::from_env().descriptors().await?)
            );
        }
        Some("inspect") => {
            let id = required(&mut arguments, "workflow inspect ID")?;
            reject_extra(arguments)?;
            print!(
                "{}",
                render_detail(&DaemonClient::from_env().workflow(&id).await?)
            );
        }
        Some("start") => {
            let request = start_request(arguments)?;
            let response = DaemonClient::from_env().start(&request).await?;
            println!(
                "Started {} workflow {} ({})\nThe workflow is owned by replicantd and continues after this CLI exits.",
                response.workflow.kind.0,
                response.workflow.id.0,
                status(response.workflow.status)
            );
        }
        Some(command @ ("pause" | "resume" | "cancel")) => {
            let id = required(&mut arguments, &format!("workflow {command} ID"))?;
            reject_extra(arguments)?;
            let response = DaemonClient::from_env().control(&id, command).await?;
            println!(
                "{} {}",
                response.workflow.id.0,
                status(response.workflow.status)
            );
        }
        Some("-h" | "--help") | None => print_help(),
        Some(other) => {
            return Err(crate::app_error(format!(
                "unknown workflow command {other:?}"
            )));
        }
    }
    Ok(())
}

fn render_catalogue(catalogue: &DescriptorCatalog) -> String {
    let mut output = String::new();
    for (class, entries) in [
        (
            "Reports",
            catalogue
                .reports
                .iter()
                .map(|item| {
                    (
                        &item.kind.0,
                        &item.aliases,
                        &item.display_name,
                        &item.description,
                    )
                })
                .collect::<Vec<_>>(),
        ),
        (
            "Actions",
            catalogue
                .actions
                .iter()
                .map(|item| {
                    (
                        &item.kind.0,
                        &item.aliases,
                        &item.display_name,
                        &item.description,
                    )
                })
                .collect(),
        ),
        (
            "Workflows",
            catalogue
                .workflows
                .iter()
                .map(|item| {
                    (
                        &item.kind.0,
                        &item.aliases,
                        &item.display_name,
                        &item.description,
                    )
                })
                .collect(),
        ),
    ] {
        output.push_str(class);
        output.push('\n');
        for (kind, aliases, name, description) in entries {
            output.push_str(&format!("  {kind:<20} {name} — {description}\n"));
            if !aliases.is_empty() {
                output.push_str(&format!("    aliases: {}\n", aliases.join(", ")));
            }
        }
    }
    output
}

pub(crate) async fn run_operation_cli(arguments: Vec<String>) -> crate::AnyResult<()> {
    let mut arguments = arguments.into_iter();
    match arguments.next().as_deref() {
        Some("catalogue" | "catalog") => {
            reject_extra(arguments)?;
            print!(
                "{}",
                render_catalogue(&DaemonClient::from_env().descriptors().await?)
            );
        }
        Some(class @ ("report" | "action")) => {
            let kind = required(
                &mut arguments,
                "operation report|action KIND [NAME=VALUE ...]",
            )?;
            let request = RunOperationRequest {
                parameters: parse_parameters(arguments)?,
            };
            let response = DaemonClient::from_env()
                .run_operation(class, &kind, &request)
                .await?;
            println!("{}", serde_json::to_string_pretty(&response.result)?);
        }
        Some("-h" | "--help") | None => print_operation_help(),
        Some(other) => {
            return Err(crate::app_error(format!(
                "unknown operation command {other:?}"
            )));
        }
    }
    Ok(())
}

pub(crate) async fn daemon_status(arguments: Vec<String>) -> crate::AnyResult<()> {
    if arguments.as_slice() == ["-h"] || arguments.as_slice() == ["--help"] {
        println!(
            "replicant-cli daemon\n\nShow replicantd health. Set REPLICANTD_URL to override http://127.0.0.1:8080."
        );
        return Ok(());
    }
    reject_extra(arguments.into_iter())?;
    print!(
        "{}",
        render_health(&DaemonClient::from_env().health().await?)
    );
    Ok(())
}

pub(crate) fn start_request(
    mut arguments: impl Iterator<Item = String>,
) -> crate::AnyResult<StartWorkflowRequest> {
    let kind = required(&mut arguments, "workflow start KIND [NAME=VALUE ...]")?;
    Ok(StartWorkflowRequest {
        kind: OperationKind(kind),
        parameters: parse_parameters(arguments)?,
    })
}

fn parse_parameters(
    mut arguments: impl Iterator<Item = String>,
) -> crate::AnyResult<BTreeMap<String, Value>> {
    let mut parameters = BTreeMap::new();
    while let Some(argument) = arguments.next() {
        let assignment = if argument == "--param" {
            required(&mut arguments, "--param NAME=VALUE")?
        } else {
            argument
        };
        let (name, value) = assignment
            .split_once('=')
            .filter(|(name, _)| !name.is_empty())
            .ok_or_else(|| crate::app_error("workflow parameters must use NAME=VALUE"))?;
        let value = serde_json::from_str(value).unwrap_or_else(|_| Value::String(value.to_owned()));
        parameters.insert(name.to_owned(), value);
    }
    Ok(parameters)
}

pub(crate) async fn submit(request: StartWorkflowRequest) -> crate::AnyResult<()> {
    let response = DaemonClient::from_env().start(&request).await?;
    println!(
        "Started {} workflow {} ({}); it continues under replicantd after this CLI exits.",
        response.workflow.kind.0,
        response.workflow.id.0,
        status(response.workflow.status)
    );
    Ok(())
}

fn required(arguments: &mut impl Iterator<Item = String>, usage: &str) -> crate::AnyResult<String> {
    arguments
        .next()
        .ok_or_else(|| crate::app_error(format!("usage: replicant-cli {usage}")))
}

fn reject_extra(mut arguments: impl Iterator<Item = String>) -> crate::AnyResult<()> {
    match arguments.next() {
        Some(argument) => Err(crate::app_error(format!(
            "unexpected argument {argument:?}"
        ))),
        None => Ok(()),
    }
}

fn status(value: WorkflowStatus) -> &'static str {
    match value {
        WorkflowStatus::Queued => "queued",
        WorkflowStatus::Running => "running",
        WorkflowStatus::Waiting => "waiting",
        WorkflowStatus::Paused => "paused",
        WorkflowStatus::Reconciling => "reconciling",
        WorkflowStatus::Succeeded => "succeeded",
        WorkflowStatus::Failed => "failed",
        WorkflowStatus::Cancelled => "cancelled",
    }
}

fn render_health(health: &DaemonHealth) -> String {
    let status = match health.status {
        HealthStatus::Healthy => "healthy",
        HealthStatus::Degraded => "degraded",
        HealthStatus::Unhealthy => "unhealthy",
    };
    format!(
        "replicantd {}: {}{}\n",
        health.daemon_version,
        status,
        health
            .detail
            .as_deref()
            .map(|detail| format!(" ({detail})"))
            .unwrap_or_default()
    )
}

fn render_list(workflows: &[WorkflowSummary]) -> String {
    if workflows.is_empty() {
        return "No workflows.\n".to_owned();
    }
    workflows
        .iter()
        .map(|workflow| {
            format!(
                "{}  {:<16} {:<12} {}\n",
                workflow.id.0,
                workflow.kind.0,
                status(workflow.status),
                workflow.current_step.as_deref().unwrap_or("-")
            )
        })
        .collect()
}

fn render_detail(workflow: &WorkflowDetail) -> String {
    format!(
        "Workflow {}\n  Kind: {}\n  Status: {}\n  Step: {}\n  Wait: {}\n  Claims: {}\n  Error: {}\n  Parameters: {}\n",
        workflow.summary.id.0,
        workflow.summary.kind.0,
        status(workflow.summary.status),
        workflow.summary.current_step.as_deref().unwrap_or("-"),
        workflow.wait_reason.as_deref().unwrap_or("-"),
        workflow.claims.len(),
        workflow.error.as_deref().unwrap_or("-"),
        serde_json::to_string(&workflow.parameters).unwrap_or_else(|_| "{}".to_owned())
    )
}

fn print_help() {
    println!(
        "Daemon workflow control\n\n\
Usage:\n  replicant-cli workflow catalogue\n  replicant-cli workflow list\n  replicant-cli workflow inspect ID\n  replicant-cli workflow start KIND [--param] NAME=VALUE ...\n  replicant-cli workflow pause ID\n  replicant-cli workflow resume ID\n  replicant-cli workflow cancel ID\n\n\
Values accept JSON scalars, arrays, and objects; unquoted values are strings.\n\
Set REPLICANTD_URL to override http://127.0.0.1:8080."
    );
}

fn print_operation_help() {
    println!(
        "Daemon operation catalogue\n\n\
Usage:\n  replicant-cli operation catalogue\n  replicant-cli operation report KIND [NAME=VALUE ...]\n  replicant-cli operation action KIND [NAME=VALUE ...]\n\n\
Kinds may use catalogue aliases. Values accept JSON scalars, arrays, and objects; unquoted values are strings.\n\
Set REPLICANTD_URL to override http://127.0.0.1:8080."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructs_typed_start_requests() {
        let request = start_request(
            [
                "survey.route",
                "replicant=Chats-1",
                "system_limit=12",
                "replace_plan=true",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .expect("request");
        assert_eq!(request.kind.0, "survey.route");
        assert_eq!(request.parameters["replicant"], "Chats-1");
        assert_eq!(request.parameters["system_limit"], 12);
        assert_eq!(request.parameters["replace_plan"], true);
    }

    #[test]
    fn renders_empty_and_populated_workflow_responses() {
        assert_eq!(render_list(&[]), "No workflows.\n");
        let workflow = WorkflowSummary {
            id: replicant_protocol::WorkflowId("workflow-1".to_owned()),
            kind: OperationKind("survey.route".to_owned()),
            status: WorkflowStatus::Waiting,
            current_step: Some("surveying".to_owned()),
            revision: 2,
            updated_at_ms: 3,
        };
        let rendered = render_list(&[workflow]);
        assert!(rendered.contains("workflow-1"));
        assert!(rendered.contains("waiting"));
        assert!(rendered.contains("surveying"));
    }

    #[test]
    fn renders_every_catalogue_lifecycle_class() {
        let catalogue =
            replicant_runtime::catalogue::OperationCatalogue::new().expect("operation catalogue");
        let rendered = render_catalogue(catalogue.descriptors());
        for expected in [
            "Reports",
            "nearby_belts",
            "Actions",
            "clear_tags",
            "contribute_twaffy_injectors",
            "tag_twaffy_ring_injectors",
            "Workflows",
            "survey.route",
        ] {
            assert!(rendered.contains(expected));
        }
    }
}
