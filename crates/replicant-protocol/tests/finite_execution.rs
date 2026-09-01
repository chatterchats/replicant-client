//! Finite execution protocol serialization coverage.

use replicant_protocol::{
    EntityId, EntityKind, EntityRef, FiniteExecution, FiniteExecutionStatus, OperationClass,
    OperationKind, ResultSummary,
};
use serde_json::json;

#[test]
fn finite_execution_serializes_for_frontend_history() {
    let execution = FiniteExecution {
        id: "execution-1".to_owned(),
        operation_class: OperationClass::Action,
        kind: OperationKind("tag_devices".to_owned()),
        status: FiniteExecutionStatus::Succeeded,
        summary: ResultSummary {
            succeeded: 1,
            skipped: 2,
            failed: 0,
        },
        started_at_ms: 10,
        finished_at_ms: 20,
        result: Some(json!({"changed_devices": 1})),
        error: None,
        links: vec![EntityRef {
            kind: EntityKind::Operation,
            id: EntityId("operation-1".to_owned()),
        }],
    };

    let value = serde_json::to_value(&execution).expect("serialize execution");
    assert_eq!(value["operation_class"], "action");
    assert_eq!(value["status"], "succeeded");
    assert_eq!(value["summary"]["skipped"], 2);
    assert_eq!(
        serde_json::from_value::<FiniteExecution>(value).expect("deserialize execution"),
        execution
    );
}
