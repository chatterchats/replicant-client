//! Read-only reporting for durable managed-client recovery runs.

use replicant_client::{Client, managed::RefreshRunId};
use serde_json::{Value, json};

use crate::ReportResult;

/// Returns one durable refresh run or the newest runs when `run_id` is absent.
pub async fn managed_refresh_status(client: &Client, run_id: Option<&str>) -> ReportResult<Value> {
    if let Some(run_id) = run_id {
        let run_id = run_id
            .parse::<RefreshRunId>()
            .map_err(|message| std::io::Error::new(std::io::ErrorKind::InvalidInput, message))?;
        let status = client.refresh().status(&run_id).await?;
        return Ok(status.map_or(Value::Null, |status| run_value(&status)));
    }
    let runs = client.refresh().list(20).await?;
    Ok(Value::Array(runs.iter().map(run_value).collect()))
}

fn run_value(status: &replicant_client::managed::RefreshRunStatus) -> Value {
    json!({
        "run_id": status.run_id.as_str(),
        "mode": refresh_mode(status.mode),
        "status": refresh_run_state(status.status),
        "readiness": refresh_readiness(status.readiness),
        "current_phase": status.current_phase.map(|phase| phase.as_str()),
        "read_requests_per_minute": status.read_requests_per_minute,
        "request_attempts": status.request_attempts,
        "history_backfilled_through": status.history_backfilled_through,
        "live_catchup": readiness_component(status.live_catchup),
        "delta": delta_value(status.delta),
        "phases": status.phases.iter().map(|phase| json!({
            "phase": phase.phase.as_str(),
            "status": refresh_phase_state(phase.status),
            "pages": phase.pages,
            "items": phase.items,
            "request_attempts": phase.request_attempts,
            "retry_not_before": phase.retry_not_before_ms,
            "approval_digest": phase.approval_digest,
            "failure_kind": phase.failure_kind,
            "delta": delta_value(phase.delta),
        })).collect::<Vec<_>>()
    })
}

fn delta_value(delta: replicant_client::managed::RefreshDelta) -> Value {
    json!({
        "proposed_inserts": delta.proposed_inserts,
        "proposed_updates": delta.proposed_updates,
        "proposed_tombstones": delta.proposed_tombstones,
        "applied_inserts": delta.applied_inserts,
        "applied_updates": delta.applied_updates,
        "applied_tombstones": delta.applied_tombstones,
    })
}

fn refresh_mode(value: replicant_client::managed::RefreshMode) -> &'static str {
    match value {
        replicant_client::managed::RefreshMode::Apply => "apply",
        replicant_client::managed::RefreshMode::DryRun => "dry_run",
    }
}

fn refresh_run_state(value: replicant_client::managed::RefreshRunState) -> &'static str {
    use replicant_client::managed::RefreshRunState;
    match value {
        RefreshRunState::Queued => "queued",
        RefreshRunState::Running => "running",
        RefreshRunState::BackingOff => "backing_off",
        RefreshRunState::AwaitingApproval => "awaiting_approval",
        RefreshRunState::Blocked => "blocked",
        RefreshRunState::Completed => "completed",
        RefreshRunState::CompletedDryRun => "completed_dry_run",
        RefreshRunState::Cancelled => "cancelled",
        RefreshRunState::Failed => "failed",
    }
}

fn refresh_phase_state(value: replicant_client::managed::RefreshPhaseState) -> &'static str {
    use replicant_client::managed::RefreshPhaseState;
    match value {
        RefreshPhaseState::Pending => "pending",
        RefreshPhaseState::Running => "running",
        RefreshPhaseState::BackingOff => "backing_off",
        RefreshPhaseState::AwaitingApproval => "awaiting_approval",
        RefreshPhaseState::Blocked => "blocked",
        RefreshPhaseState::Complete => "complete",
        RefreshPhaseState::Cancelled => "cancelled",
        RefreshPhaseState::Failed => "failed",
    }
}

fn refresh_readiness(value: replicant_client::managed::RefreshReadiness) -> &'static str {
    match value {
        replicant_client::managed::RefreshReadiness::Unavailable => "unavailable",
        replicant_client::managed::RefreshReadiness::RestBaseline => "rest_baseline",
        replicant_client::managed::RefreshReadiness::Complete => "complete",
    }
}

fn readiness_component(value: replicant_client::managed::ReadinessComponent) -> &'static str {
    match value {
        replicant_client::managed::ReadinessComponent::Pending => "pending",
        replicant_client::managed::ReadinessComponent::Ready => "ready",
        replicant_client::managed::ReadinessComponent::Degraded => "degraded",
        _ => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use replicant_client::{
        StartupPolicy,
        managed::{RefreshMode, RefreshPhase, RefreshRequest},
    };

    use super::*;

    #[tokio::test]
    async fn managed_refresh_status_reports_durable_run() {
        let client = Client::builder()
            .in_memory()
            .startup_policy(StartupPolicy::RestoreOnly)
            .start()
            .await
            .unwrap();
        let run = client
            .refresh()
            .start(RefreshRequest {
                phases: BTreeSet::from([RefreshPhase::Account]),
                mode: RefreshMode::DryRun,
                read_requests_per_minute: 30,
            })
            .await
            .unwrap();
        let report = managed_refresh_status(&client, Some(run.run_id.as_str()))
            .await
            .unwrap();
        assert_eq!(report["run_id"], run.run_id.as_str());
        assert_eq!(report["mode"], "dry_run");
        assert_eq!(report["status"], "queued");
        assert_eq!(report["phases"][0]["phase"], "account");
        client.close().await.unwrap();
    }
}
