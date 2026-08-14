use std::collections::BTreeMap;

use replicant_protocol::{
    DaemonHealth, HealthStatus, OperationKind, StartWorkflowRequest, WorkflowDetail,
    WorkflowStatus, WorkflowSummary,
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
    Ok(StartWorkflowRequest {
        kind: OperationKind(kind),
        parameters,
    })
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
Usage:\n  replicant-cli workflow list\n  replicant-cli workflow inspect ID\n  replicant-cli workflow start KIND [--param] NAME=VALUE ...\n  replicant-cli workflow pause ID\n  replicant-cli workflow resume ID\n  replicant-cli workflow cancel ID\n\n\
Values accept JSON scalars, arrays, and objects; unquoted values are strings.\n\
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
}
