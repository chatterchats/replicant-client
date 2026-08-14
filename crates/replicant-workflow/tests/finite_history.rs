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
