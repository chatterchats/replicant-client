use replicant_workflow::{FiniteExecutionClass, FiniteExecutionStatus, WorkflowRepository};
use serde_json::json;

#[test]
fn finite_execution_history_survives_repository_reopen() {
    let path = std::env::temp_dir().join(format!(
        "replicant-finite-history-{}.sqlite",
        std::process::id()
    ));
    {
        let repository = WorkflowRepository::open(&path).expect("open repository");
        repository
            .record_finite_execution(
                FiniteExecutionClass::Action,
                "tag_devices",
                FiniteExecutionStatus::Succeeded,
                10,
                Some(&json!({"changed_devices": 1})),
                None,
            )
            .expect("record execution");
    }
    let history = WorkflowRepository::open(&path)
        .expect("reopen repository")
        .finite_execution_history()
        .expect("read history");
    std::fs::remove_file(path).expect("remove test database");

    assert_eq!(history.len(), 1);
    assert_eq!(history[0].operation_class, FiniteExecutionClass::Action);
    assert_eq!(history[0].kind, "tag_devices");
    assert_eq!(history[0].result, Some(json!({"changed_devices": 1})));
}

#[test]
fn running_finite_execution_can_be_recorded_then_completed() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let running = repository
        .begin_finite_execution(FiniteExecutionClass::Action, "clone.replicate", 42)
        .expect("begin execution");
    assert_eq!(running.status, FiniteExecutionStatus::Running);

    let history = repository
        .finite_execution_history()
        .expect("read running history");
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].status, FiniteExecutionStatus::Running);

    repository
        .complete_finite_execution(
            &running.id,
            FiniteExecutionStatus::Succeeded,
            Some(&json!({"replicant": "R-1"})),
            None,
        )
        .expect("complete execution");
    let history = repository
        .finite_execution_history()
        .expect("read completed history");
    assert_eq!(history[0].status, FiniteExecutionStatus::Succeeded);
}

#[test]
fn migrates_v8_finite_execution_constraint_to_allow_running() {
    let path = std::env::temp_dir().join(format!(
        "replicant-finite-history-v8-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    {
        let connection = rusqlite::Connection::open(&path).expect("open v8 database");
        connection
            .execute_batch(concat!(
                include_str!("../migrations/0001_initial.sql"),
                include_str!("../migrations/0002_activity.sql"),
                include_str!("../migrations/0003_resource_claims.sql"),
                include_str!("../migrations/0004_wait_intent.sql"),
                include_str!("../migrations/0005_finite_execution_history.sql"),
                include_str!("../migrations/0006_automation_triggers.sql"),
                include_str!("../migrations/0007_automation_policy.sql"),
                include_str!("../migrations/0008_runtime_documents.sql"),
                "CREATE TABLE runtime_schema_migrations (version INTEGER PRIMARY KEY NOT NULL);",
                "INSERT INTO runtime_schema_migrations VALUES (1), (2), (3), (4), (5), (6), (7), (8);"
            ))
            .expect("install v8 schema");
        connection
            .execute(
                "INSERT INTO finite_executions
                 (id, operation_class, kind, status, started_at, finished_at)
                 VALUES ('old', 'action', 'clone.stow_target', 'succeeded', 1, 2)",
                [],
            )
            .expect("insert existing execution");
    }

    let repository = WorkflowRepository::open(&path).expect("migrate v8 database");
    let running = repository
        .begin_finite_execution(FiniteExecutionClass::Action, "clone.replicate", 3)
        .expect("running action is accepted after migration");
    assert_eq!(running.status, FiniteExecutionStatus::Running);
    let history = repository
        .finite_execution_history()
        .expect("read migrated history");
    assert_eq!(history.len(), 2);
    assert!(history.iter().any(|execution| execution.id == "old"));

    drop(repository);
    std::fs::remove_file(path).expect("remove test database");
}
