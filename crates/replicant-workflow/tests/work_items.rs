//! Durable work-item persistence and lifecycle acceptance tests.

use std::{fs, sync::Arc};

use replicant_workflow::{
    BoxWorkflowFuture, CampaignCounts, CampaignOutcome, CampaignResult, NewWorkflow,
    RepositoryError, WorkItemAttemptOutcome, WorkItemSpec, WorkItemStatus, WorkItemTransition,
    WorkflowContext, WorkflowExecutor, WorkflowFactory, WorkflowFailureDisposition, WorkflowKind,
    WorkflowRegistry, WorkflowRepository, WorkflowState, WorkflowStatus, WorkflowSupervisor,
};
use serde_json::json;

fn create_campaign(repository: &WorkflowRepository) -> replicant_workflow::WorkflowInstance {
    repository
        .create(NewWorkflow {
            kind: WorkflowKind::new("test.campaign").expect("valid campaign kind"),
            schema_version: 1,
            config: json!({}),
            checkpoint: json!({}),
            current_step: None,
            parent_id: None,
        })
        .expect("create campaign")
}

fn spec(workflow_id: replicant_workflow::WorkflowId, key: &str) -> WorkItemSpec {
    WorkItemSpec {
        workflow_id,
        dedupe_key: key.to_owned(),
        kind: WorkflowKind::new("test.item").expect("valid item kind"),
        sort_key: key.to_owned(),
        payload_json: json!({ "key": key }),
        preconditions_json: json!([]),
        requirements_json: json!([]),
        deadline_at_ms: None,
    }
}

#[test]
fn work_item_reconciliation_and_claim_are_idempotent() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let campaign = create_campaign(&repository);
    let first = spec(campaign.id, "a");
    let second = spec(campaign.id, "b");

    let initial = repository
        .reconcile_work_items(campaign.id, &[first.clone(), first.clone(), second], 100)
        .expect("reconcile desired items");
    assert_eq!(initial.len(), 2);
    assert!(
        initial
            .iter()
            .all(|item| item.state.status == WorkItemStatus::Pending)
    );

    let repeated = repository
        .reconcile_work_items(campaign.id, std::slice::from_ref(&first), 200)
        .expect("repeat reconciliation");
    assert_eq!(repeated.len(), 2, "omitted historical items remain");
    assert_eq!(repeated[0].id, initial[0].id);

    let mut conflicting = first;
    conflicting.payload_json = json!({ "different": true });
    assert!(matches!(
        repository.reconcile_work_items(campaign.id, &[conflicting], 300),
        Err(RepositoryError::WorkItemSpecConflict { .. })
    ));

    let claimed = repository
        .claim_next_work_item(campaign.id, 400)
        .expect("claim item")
        .expect("eligible item");
    assert_eq!(claimed.spec.dedupe_key, "a");
    assert_eq!(claimed.state.status, WorkItemStatus::Assigned);
    assert_eq!(
        repository
            .read_work_item(claimed.id)
            .expect("read item")
            .expect("item exists"),
        claimed
    );
}

#[test]
fn work_item_transitions_close_attempts_and_reject_stale_revisions() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let campaign = create_campaign(&repository);
    repository
        .reconcile_work_items(campaign.id, &[spec(campaign.id, "a")], 10)
        .expect("reconcile item");
    let assigned = repository
        .claim_next_work_item(campaign.id, 20)
        .expect("claim")
        .expect("assigned item");
    let running = repository
        .start_work_item(assigned.id, assigned.state.revision, "R-1", "grant-1", 30)
        .expect("start first attempt");
    assert_eq!(running.state.attempt_count, 1);
    assert!(running.state.ever_started);
    assert!(matches!(
        repository.transition_work_item(
            running.id,
            assigned.state.revision,
            WorkItemTransition::CheckpointCommitted {
                checkpoint_json: json!({ "step": 1 }),
            },
            40,
        ),
        Err(RepositoryError::ConcurrentWorkItemUpdate { .. })
    ));
    let checkpointed = repository
        .transition_work_item(
            running.id,
            running.state.revision,
            WorkItemTransition::CheckpointCommitted {
                checkpoint_json: json!({ "step": 1 }),
            },
            40,
        )
        .expect("commit checkpoint");
    assert_eq!(checkpointed.state.status, WorkItemStatus::Running);
    let waiting = repository
        .transition_work_item(
            checkpointed.id,
            checkpointed.state.revision,
            WorkItemTransition::Waiting {
                checkpoint_json: None,
                reason: "blocked".into(),
                retry_at_ms: Some(60),
            },
            50,
        )
        .expect("wait");
    assert_eq!(waiting.state.status, WorkItemStatus::Waiting);
    assert_eq!(waiting.state.checkpoint_json, Some(json!({ "step": 1 })));
    assert!(
        repository
            .claim_next_work_item(campaign.id, 59)
            .expect("check early eligibility")
            .is_none()
    );
    let assigned = repository
        .claim_next_work_item(campaign.id, 60)
        .expect("claim due item")
        .expect("due item");
    let running = repository
        .start_work_item(assigned.id, assigned.state.revision, "R-2", "grant-2", 70)
        .expect("start second attempt");
    let retrying = repository
        .transition_work_item(
            running.id,
            running.state.revision,
            WorkItemTransition::RetryableFailure {
                checkpoint_json: None,
                error: "temporary".into(),
            },
            80,
        )
        .expect("schedule retry");
    assert_eq!(retrying.state.status, WorkItemStatus::Pending);
    assert_eq!(retrying.state.consecutive_failure_count, 1);
    let retry_at = retrying.state.next_attempt_at_ms.expect("retry time");
    assert!(retry_at > 80);
    let assigned = repository
        .claim_next_work_item(campaign.id, retry_at)
        .expect("claim retry")
        .expect("retry eligible");
    let running = repository
        .start_work_item(
            assigned.id,
            assigned.state.revision,
            "R-3",
            "grant-3",
            retry_at + 1,
        )
        .expect("start third attempt");
    let succeeded = repository
        .transition_work_item(
            running.id,
            running.state.revision,
            WorkItemTransition::Succeeded {
                checkpoint_json: None,
                result_json: Some(json!({ "ok": true })),
            },
            retry_at + 2,
        )
        .expect("succeed item");
    assert_eq!(succeeded.state.status, WorkItemStatus::Succeeded);
    assert_eq!(succeeded.state.consecutive_failure_count, 0);
    assert!(matches!(
        repository.transition_work_item(
            succeeded.id,
            succeeded.state.revision,
            WorkItemTransition::Abandoned {
                reason: "too late".into(),
            },
            retry_at + 3,
        ),
        Err(RepositoryError::InvalidWorkItemTransition { .. })
    ));

    let attempts = repository
        .list_work_item_attempts(succeeded.id)
        .expect("list attempts");
    assert_eq!(attempts.len(), 3);
    assert_eq!(attempts[0].outcome, Some(WorkItemAttemptOutcome::Reclaimed));
    assert_eq!(attempts[1].outcome, Some(WorkItemAttemptOutcome::Failed));
    assert_eq!(attempts[2].outcome, Some(WorkItemAttemptOutcome::Succeeded));
    assert!(attempts.iter().all(|attempt| attempt.ended_at_ms.is_some()));
}

fn fail_next_attempt(
    repository: &WorkflowRepository,
    campaign_id: replicant_workflow::WorkflowId,
    eligible_at: i64,
) -> replicant_workflow::WorkItem {
    let assigned = repository
        .claim_next_work_item(campaign_id, eligible_at)
        .expect("claim retry item")
        .expect("item is eligible");
    let running = repository
        .start_work_item(
            assigned.id,
            assigned.state.revision,
            "R-1",
            "grant",
            eligible_at + 1,
        )
        .expect("start retry attempt");
    repository
        .transition_work_item(
            running.id,
            running.state.revision,
            WorkItemTransition::RetryableFailure {
                checkpoint_json: None,
                error: "transient".into(),
            },
            eligible_at + 2,
        )
        .expect("record retryable failure")
}

#[test]
fn work_item_retry_backoff_is_deterministic_and_capped() {
    let first_path = std::env::temp_dir().join(format!(
        "replicant-work-item-backoff-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let second_path = first_path.with_extension("copy.sqlite");
    let repository = WorkflowRepository::open(&first_path).expect("open first repository");
    let campaign = create_campaign(&repository);
    repository
        .reconcile_work_items(campaign.id, &[spec(campaign.id, "a")], 0)
        .expect("reconcile retry item");
    drop(repository);
    fs::copy(&first_path, &second_path).expect("copy deterministic fixture");

    let first_repository = WorkflowRepository::open(&first_path).expect("reopen first repository");
    let second_repository = WorkflowRepository::open(&second_path).expect("open copied repository");
    let first_failure = fail_next_attempt(&first_repository, campaign.id, 10);
    let copied_failure = fail_next_attempt(&second_repository, campaign.id, 10);
    assert_eq!(
        first_failure.state.next_attempt_at_ms, copied_failure.state.next_attempt_at_ms,
        "the same item and attempt receive the same jitter"
    );

    let first_delay = first_failure.state.next_attempt_at_ms.expect("retry time") - 12;
    assert!((270_000..=330_000).contains(&first_delay));
    let mut item = first_failure;
    for failure_count in 2..=10_u32 {
        let eligible_at = item.state.next_attempt_at_ms.expect("next retry");
        item = fail_next_attempt(&first_repository, campaign.id, eligible_at);
        assert_eq!(item.state.consecutive_failure_count, failure_count);
        let delay = item.state.next_attempt_at_ms.expect("retry time") - (eligible_at + 2);
        let exponent = failure_count.saturating_sub(1);
        let unjittered = if exponent >= 7 {
            21_600_000_i64
        } else {
            (300_000_i64 * (1_i64 << exponent)).min(21_600_000)
        };
        assert!(delay >= unjittered * 9 / 10);
        assert!(delay <= (unjittered * 11 / 10).min(21_600_000));
    }
    assert_eq!(item.state.consecutive_failure_count, 10);

    drop(first_repository);
    drop(second_repository);
    fs::remove_file(first_path).expect("remove first fixture");
    fs::remove_file(second_path).expect("remove copied fixture");
}

#[test]
fn work_item_campaign_aggregation_preserves_partial_success() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");

    let empty = create_campaign(&repository);
    let result = repository
        .aggregate_campaign_result(empty.id)
        .expect("aggregate empty campaign")
        .expect("empty campaign is terminal");
    assert_eq!(result.outcome, CampaignOutcome::NothingCouldStart);
    assert_eq!(result.workflow_status(), WorkflowStatus::Succeeded);

    let skipped_campaign = create_campaign(&repository);
    let skipped = repository
        .reconcile_work_items(
            skipped_campaign.id,
            &[spec(skipped_campaign.id, "skipped")],
            10,
        )
        .expect("reconcile skipped item")
        .remove(0);
    repository
        .transition_work_item(
            skipped.id,
            skipped.state.revision,
            WorkItemTransition::Skipped {
                reason: "already satisfied".into(),
                result_json: None,
            },
            11,
        )
        .expect("skip item");
    let result = repository
        .aggregate_campaign_result(skipped_campaign.id)
        .expect("aggregate skipped campaign")
        .expect("skipped campaign is terminal");
    assert_eq!(result.outcome, CampaignOutcome::NothingCouldStart);
    assert_eq!(result.workflow_status(), WorkflowStatus::Succeeded);

    let partial_campaign = create_campaign(&repository);
    repository
        .reconcile_work_items(
            partial_campaign.id,
            &[
                spec(partial_campaign.id, "a"),
                spec(partial_campaign.id, "b"),
            ],
            20,
        )
        .expect("reconcile partial campaign");
    let assigned = repository
        .claim_next_work_item(partial_campaign.id, 21)
        .expect("claim success")
        .expect("success item");
    let running = repository
        .start_work_item(assigned.id, assigned.state.revision, "R-1", "a", 22)
        .expect("start success");
    repository
        .transition_work_item(
            running.id,
            running.state.revision,
            WorkItemTransition::Succeeded {
                checkpoint_json: None,
                result_json: Some(json!({ "done": true })),
            },
            23,
        )
        .expect("succeed first item");
    let assigned = repository
        .claim_next_work_item(partial_campaign.id, 24)
        .expect("claim failure")
        .expect("failure item");
    repository
        .transition_work_item(
            assigned.id,
            assigned.state.revision,
            WorkItemTransition::Failed {
                error: "permanent".into(),
                result_json: None,
            },
            25,
        )
        .expect("fail second item");
    let result = repository
        .aggregate_campaign_result(partial_campaign.id)
        .expect("aggregate partial campaign")
        .expect("partial campaign is terminal");
    assert_eq!(result.outcome, CampaignOutcome::PartialSuccess);
    assert_eq!(result.workflow_status(), WorkflowStatus::Succeeded);
    assert_eq!(result.counts.succeeded, 1);
    assert_eq!(result.counts.failed, 1);

    let blocked_campaign = create_campaign(&repository);
    repository
        .reconcile_work_items(
            blocked_campaign.id,
            &[spec(blocked_campaign.id, "blocked")],
            30,
        )
        .expect("reconcile blocked item");
    assert!(
        repository
            .aggregate_campaign_result(blocked_campaign.id)
            .expect("aggregate nonterminal campaign")
            .is_none()
    );

    let infeasible_campaign = create_campaign(&repository);
    let infeasible = repository
        .reconcile_work_items(
            infeasible_campaign.id,
            &[spec(infeasible_campaign.id, "infeasible")],
            40,
        )
        .expect("reconcile infeasible item")
        .remove(0);
    repository
        .transition_work_item(
            infeasible.id,
            infeasible.state.revision,
            WorkItemTransition::Failed {
                error: "permanently infeasible".into(),
                result_json: None,
            },
            41,
        )
        .expect("fail before start");
    let result = repository
        .aggregate_campaign_result(infeasible_campaign.id)
        .expect("aggregate infeasible campaign")
        .expect("infeasible campaign is terminal");
    assert_eq!(result.outcome, CampaignOutcome::NothingCouldStart);
    assert_eq!(result.workflow_status(), WorkflowStatus::Failed);

    let no_success_campaign = create_campaign(&repository);
    repository
        .reconcile_work_items(
            no_success_campaign.id,
            &[spec(no_success_campaign.id, "failed")],
            50,
        )
        .expect("reconcile unsuccessful item");
    let assigned = repository
        .claim_next_work_item(no_success_campaign.id, 51)
        .expect("claim unsuccessful item")
        .expect("unsuccessful item");
    let running = repository
        .start_work_item(assigned.id, assigned.state.revision, "R-1", "failed", 52)
        .expect("start unsuccessful item");
    repository
        .transition_work_item(
            running.id,
            running.state.revision,
            WorkItemTransition::Failed {
                error: "terminal failure".into(),
                result_json: None,
            },
            53,
        )
        .expect("fail started item");
    let result = repository
        .aggregate_campaign_result(no_success_campaign.id)
        .expect("aggregate unsuccessful campaign")
        .expect("unsuccessful campaign is terminal");
    assert_eq!(result.outcome, CampaignOutcome::NoSuccess);
    assert_eq!(result.workflow_status(), WorkflowStatus::Failed);
}

#[test]
fn work_item_terminal_workflow_update_abandons_open_work_atomically() {
    for terminal_status in [
        WorkflowStatus::Succeeded,
        WorkflowStatus::Failed,
        WorkflowStatus::Cancelled,
    ] {
        let repository = WorkflowRepository::open_in_memory().expect("open repository");
        let campaign = create_campaign(&repository);
        repository
            .reconcile_work_items(
                campaign.id,
                &[spec(campaign.id, "running"), spec(campaign.id, "pending")],
                10,
            )
            .expect("reconcile work");
        let assigned = repository
            .claim_next_work_item(campaign.id, 20)
            .expect("claim")
            .expect("assigned item");
        let running = repository
            .start_work_item(assigned.id, assigned.state.revision, "R-1", "grant", 30)
            .expect("start attempt");
        let active_campaign = repository
            .update(
                campaign.id,
                campaign.revision,
                WorkflowState::<_, ()> {
                    status: WorkflowStatus::Running,
                    current_step: None,
                    checkpoint: json!({}),
                    last_error: None,
                    result: None,
                },
            )
            .expect("start campaign");
        repository
            .update(
                active_campaign.id,
                active_campaign.revision,
                WorkflowState::<_, ()> {
                    status: terminal_status,
                    current_step: None,
                    checkpoint: json!({}),
                    last_error: (terminal_status == WorkflowStatus::Failed)
                        .then(|| "campaign failed".to_owned()),
                    result: None,
                },
            )
            .expect("finish campaign");

        let items = repository
            .list_work_items(campaign.id)
            .expect("list abandoned items");
        assert_eq!(items.len(), 2);
        assert!(
            items
                .iter()
                .all(|item| item.state.status == WorkItemStatus::Abandoned)
        );
        assert!(
            items
                .iter()
                .all(|item| item.state.next_attempt_at_ms.is_none())
        );
        let attempts = repository
            .list_work_item_attempts(running.id)
            .expect("list cancelled attempt");
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].outcome, Some(WorkItemAttemptOutcome::Cancelled));
        assert!(attempts[0].ended_at_ms.is_some());
    }
}

#[test]
fn work_item_pause_retains_and_resume_reclaims_attempt() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let campaign = create_campaign(&repository);
    let campaign = repository
        .update(
            campaign.id,
            campaign.revision,
            WorkflowState::<_, ()> {
                status: WorkflowStatus::Running,
                current_step: None,
                checkpoint: json!({}),
                last_error: None,
                result: None,
            },
        )
        .expect("start campaign");
    repository
        .reconcile_work_items(campaign.id, &[spec(campaign.id, "running")], 10)
        .expect("reconcile item");
    let assigned = repository
        .claim_next_work_item(campaign.id, 20)
        .expect("claim item")
        .expect("assigned item");
    let running = repository
        .start_work_item(assigned.id, assigned.state.revision, "R-1", "grant", 30)
        .expect("start item");
    let supervisor = WorkflowSupervisor::new(repository.clone(), Arc::new(WorkflowRegistry::new()));

    supervisor.pause(campaign.id).expect("pause campaign");
    assert_eq!(
        repository
            .read_work_item(running.id)
            .expect("read paused item")
            .expect("item exists")
            .state
            .status,
        WorkItemStatus::Running,
        "pause retains the running item and attempt"
    );
    supervisor.resume(campaign.id).expect("resume campaign");
    assert_eq!(
        repository
            .read_work_item(running.id)
            .expect("read resumed item")
            .expect("item exists")
            .state
            .status,
        WorkItemStatus::Pending
    );
    let attempts = repository
        .list_work_item_attempts(running.id)
        .expect("list reclaimed attempt");
    assert_eq!(attempts[0].outcome, Some(WorkItemAttemptOutcome::Reclaimed));
    assert_eq!(
        repository
            .read(campaign.id)
            .expect("read resumed campaign")
            .expect("campaign exists")
            .status,
        WorkflowStatus::Reconciling
    );
}

#[tokio::test]
async fn work_item_supervisor_startup_reclaims_open_attempt_before_execution() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let campaign = create_campaign(&repository);
    let campaign = repository
        .update(
            campaign.id,
            campaign.revision,
            WorkflowState::<_, ()> {
                status: WorkflowStatus::Running,
                current_step: None,
                checkpoint: json!({}),
                last_error: None,
                result: None,
            },
        )
        .expect("start campaign");
    repository
        .reconcile_work_items(campaign.id, &[spec(campaign.id, "running")], 10)
        .expect("reconcile item");
    let assigned = repository
        .claim_next_work_item(campaign.id, 20)
        .expect("claim item")
        .expect("assigned item");
    let running = repository
        .start_work_item(assigned.id, assigned.state.revision, "R-1", "grant", 30)
        .expect("start item");

    let supervisor = WorkflowSupervisor::new(repository.clone(), Arc::new(WorkflowRegistry::new()));
    supervisor.tick().await.expect("run startup reconciliation");
    let attempts = repository
        .list_work_item_attempts(running.id)
        .expect("list startup-reclaimed attempt");
    assert_eq!(attempts[0].outcome, Some(WorkItemAttemptOutcome::Reclaimed));
}

#[test]
fn work_item_schema_eleven_migrates_without_losing_workflows() {
    const MIGRATIONS: [&str; 11] = [
        include_str!("../migrations/0001_initial.sql"),
        include_str!("../migrations/0002_activity.sql"),
        include_str!("../migrations/0003_resource_claims.sql"),
        include_str!("../migrations/0004_wait_intent.sql"),
        include_str!("../migrations/0005_finite_execution_history.sql"),
        include_str!("../migrations/0006_automation_triggers.sql"),
        include_str!("../migrations/0007_automation_policy.sql"),
        include_str!("../migrations/0008_runtime_documents.sql"),
        include_str!("../migrations/0009_finite_execution_running.sql"),
        include_str!("../migrations/0010_finite_execution_cancelled.sql"),
        include_str!("../migrations/0011_workflow_failure_disposition.sql"),
    ];
    let path = std::env::temp_dir().join(format!(
        "replicant-work-item-schema-11-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let workflow_id = uuid::Uuid::new_v4();
    {
        let connection = rusqlite::Connection::open(&path).expect("open schema-11 fixture");
        connection
            .execute(
                "CREATE TABLE runtime_schema_migrations (version INTEGER PRIMARY KEY NOT NULL)",
                [],
            )
            .expect("create migration table");
        for (index, migration) in MIGRATIONS.iter().enumerate() {
            connection
                .execute_batch(migration)
                .expect("apply old migration");
            connection
                .execute(
                    "INSERT INTO runtime_schema_migrations (version) VALUES (?1)",
                    [i64::try_from(index + 1).expect("migration index fits")],
                )
                .expect("record old migration");
        }
        connection
            .execute(
                "INSERT INTO workflow_instances (
                    id, kind, schema_version, config_json, checkpoint_json, status,
                    created_at, updated_at
                 ) VALUES (?1, 'test.campaign', 1, '{}', '{}', 'queued', 1, 1)",
                [workflow_id.to_string()],
            )
            .expect("insert existing workflow");
    }

    let repository = WorkflowRepository::open(&path).expect("migrate schema 11 to 12");
    assert!(
        repository
            .read(workflow_id.to_string().parse().expect("parse workflow id"))
            .expect("read preserved workflow")
            .is_some()
    );
    let connection = rusqlite::Connection::open(&path).expect("inspect migrated schema");
    for object in [
        "workflow_work_items",
        "workflow_work_item_attempts",
        "workflow_work_items_eligibility_idx",
        "workflow_work_item_attempts_open_idx",
    ] {
        let found: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = ?1",
                [object],
                |row| row.get(0),
            )
            .expect("query migrated object");
        assert_eq!(found, 1, "missing migrated object {object}");
    }
    drop(connection);
    drop(repository);
    fs::remove_file(path).expect("remove migration fixture");
}

#[test]
fn work_item_file_backed_claim_has_one_winner() {
    let path = std::env::temp_dir().join(format!(
        "replicant-work-item-race-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let repository = WorkflowRepository::open(&path).expect("open repository");
    let campaign = create_campaign(&repository);
    repository
        .reconcile_work_items(campaign.id, &[spec(campaign.id, "only")], 10)
        .expect("reconcile item");
    drop(repository);

    let first = WorkflowRepository::open(&path).expect("open first handle");
    let second = WorkflowRepository::open(&path).expect("open second handle");
    let first_claim = first
        .claim_next_work_item(campaign.id, 20)
        .expect("first claim");
    let second_claim = second
        .claim_next_work_item(campaign.id, 20)
        .expect("second claim");
    assert_eq!(
        usize::from(first_claim.is_some()) + usize::from(second_claim.is_some()),
        1
    );
    drop(first);
    drop(second);
    fs::remove_file(path).expect("remove race fixture");
}

struct FailureFactory {
    kind: WorkflowKind,
}

impl WorkflowFactory for FailureFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    fn current_schema_version(&self) -> u32 {
        1
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(FailureExecutor))
    }
}

struct FailureExecutor;

impl WorkflowExecutor for FailureExecutor {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let structured: bool = context.config().map_err(|error| error.to_string())?;
            if structured {
                context
                    .mark_failed_with_result(
                        "campaign failed",
                        CampaignResult {
                            outcome: CampaignOutcome::NoSuccess,
                            counts: CampaignCounts {
                                total: 1,
                                failed: 1,
                                ..CampaignCounts::default()
                            },
                            items: Vec::new(),
                        },
                        WorkflowFailureDisposition::Permanent,
                    )
                    .map_err(|error| error.to_string())
            } else {
                context
                    .mark_failed("legacy failure")
                    .map_err(|error| error.to_string())
            }
        })
    }
}

#[tokio::test]
async fn work_item_failure_result_persists_and_legacy_null_remains_sql_null() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let kind = WorkflowKind::new("test.failure-result").expect("valid kind");
    let structured = repository
        .create(NewWorkflow {
            kind: kind.clone(),
            schema_version: 1,
            config: true,
            checkpoint: json!({}),
            current_step: None,
            parent_id: None,
        })
        .expect("create structured failure");
    let legacy = repository
        .create(NewWorkflow {
            kind: kind.clone(),
            schema_version: 1,
            config: false,
            checkpoint: json!({}),
            current_step: None,
            parent_id: None,
        })
        .expect("create legacy failure");
    let mut registry = WorkflowRegistry::new();
    registry
        .register(Arc::new(FailureFactory { kind }))
        .expect("register failure workflow");
    let supervisor = WorkflowSupervisor::new(repository.clone(), Arc::new(registry));
    supervisor.tick().await.expect("start failure workflows");
    for _ in 0..100 {
        let structured_status = repository
            .read(structured.id)
            .expect("read structured workflow")
            .expect("structured workflow exists")
            .status;
        let legacy_status = repository
            .read(legacy.id)
            .expect("read legacy workflow")
            .expect("legacy workflow exists")
            .status;
        if structured_status == WorkflowStatus::Failed && legacy_status == WorkflowStatus::Failed {
            break;
        }
        tokio::task::yield_now().await;
    }
    let structured = repository
        .read(structured.id)
        .expect("read structured result")
        .expect("structured workflow exists");
    assert_eq!(
        structured.failure_disposition,
        Some(WorkflowFailureDisposition::Permanent)
    );
    assert_eq!(
        structured
            .result::<CampaignResult>()
            .expect("decode campaign result")
            .expect("campaign result exists")
            .outcome,
        CampaignOutcome::NoSuccess
    );
    let legacy = repository
        .read(legacy.id)
        .expect("read legacy result")
        .expect("legacy workflow exists");
    assert_eq!(
        legacy.failure_disposition,
        Some(WorkflowFailureDisposition::Retryable)
    );
    assert_eq!(
        legacy
            .result::<serde_json::Value>()
            .expect("decode legacy result"),
        None
    );
}

fn prepared_edge_item(
    source: WorkItemStatus,
    key: &str,
) -> (
    WorkflowRepository,
    replicant_workflow::WorkflowId,
    replicant_workflow::WorkItem,
) {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let campaign = create_campaign(&repository);
    let pending = repository
        .reconcile_work_items(campaign.id, &[spec(campaign.id, key)], 1)
        .expect("reconcile item")
        .remove(0);
    let item = match source {
        WorkItemStatus::Pending => pending,
        WorkItemStatus::Assigned | WorkItemStatus::Running => {
            let assigned = repository
                .claim_next_work_item(campaign.id, 2)
                .expect("claim item")
                .expect("assigned item");
            if source == WorkItemStatus::Running {
                repository
                    .start_work_item(
                        assigned.id,
                        assigned.state.revision,
                        "R-EDGE",
                        &format!("edge-{key}"),
                        3,
                    )
                    .expect("start item")
            } else {
                assigned
            }
        }
        WorkItemStatus::Waiting => repository
            .transition_work_item(
                pending.id,
                pending.state.revision,
                WorkItemTransition::Waiting {
                    checkpoint_json: None,
                    reason: "fixture wait".into(),
                    retry_at_ms: None,
                },
                2,
            )
            .expect("wait item"),
        terminal => panic!("cannot prepare terminal source {terminal:?}"),
    };
    (repository, campaign.id, item)
}

fn named_edge_transition(name: &str) -> (WorkItemTransition, WorkItemStatus) {
    match name {
        "waiting" => (
            WorkItemTransition::Waiting {
                checkpoint_json: Some(json!({ "edge": "waiting" })),
                reason: "blocked".into(),
                retry_at_ms: Some(100),
            },
            WorkItemStatus::Waiting,
        ),
        "reclaimed" => (
            WorkItemTransition::Reclaimed {
                checkpoint_json: Some(json!({ "edge": "reclaimed" })),
            },
            WorkItemStatus::Pending,
        ),
        "skipped" => (
            WorkItemTransition::Skipped {
                reason: "already satisfied".into(),
                result_json: Some(json!({ "edge": "skipped" })),
            },
            WorkItemStatus::Skipped,
        ),
        "failed" => (
            WorkItemTransition::Failed {
                error: "permanent".into(),
                result_json: Some(json!({ "edge": "failed" })),
            },
            WorkItemStatus::Failed,
        ),
        "abandoned" => (
            WorkItemTransition::Abandoned {
                reason: "cancelled".into(),
            },
            WorkItemStatus::Abandoned,
        ),
        name => panic!("unknown edge transition {name}"),
    }
}

#[test]
fn work_item_every_allowed_status_edge_is_persisted() {
    let cases = [
        (
            WorkItemStatus::Pending,
            &["waiting", "skipped", "failed", "abandoned"][..],
        ),
        (
            WorkItemStatus::Assigned,
            &["reclaimed", "waiting", "skipped", "failed", "abandoned"][..],
        ),
        (
            WorkItemStatus::Running,
            &["reclaimed", "waiting", "skipped", "failed", "abandoned"][..],
        ),
        (
            WorkItemStatus::Waiting,
            &["waiting", "skipped", "failed", "abandoned"][..],
        ),
    ];
    for (source, transitions) in cases {
        for transition_name in transitions {
            let key = format!("{source:?}-{transition_name}");
            let (repository, _, item) = prepared_edge_item(source, &key);
            let (transition, expected) = named_edge_transition(transition_name);
            let updated = repository
                .transition_work_item(item.id, item.state.revision, transition, 10)
                .unwrap_or_else(|error| {
                    panic!("{source:?}->{transition_name} must be allowed: {error}")
                });
            assert_eq!(updated.state.status, expected);
            if source == WorkItemStatus::Running {
                assert!(
                    repository
                        .list_work_item_attempts(item.id)
                        .expect("attempts")[0]
                        .ended_at_ms
                        .is_some()
                );
            }
        }
    }

    let (repository, campaign_id, waiting) =
        prepared_edge_item(WorkItemStatus::Waiting, "waiting-assigned");
    let waiting = repository
        .transition_work_item(
            waiting.id,
            waiting.state.revision,
            WorkItemTransition::Waiting {
                checkpoint_json: None,
                reason: "due".into(),
                retry_at_ms: Some(20),
            },
            11,
        )
        .expect("make waiting item due");
    let assigned = repository
        .claim_next_work_item(campaign_id, 20)
        .expect("claim due waiting item")
        .expect("waiting item becomes assigned");
    assert_eq!(assigned.id, waiting.id);
    assert_eq!(assigned.state.status, WorkItemStatus::Assigned);
}
