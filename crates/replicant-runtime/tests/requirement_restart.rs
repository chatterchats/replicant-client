//! Integration coverage for requirement-plan restart persistence.

use std::{collections::BTreeMap, fs};

use replicant_runtime::{
    requirements::{
        FulfillmentOperation, FulfillmentOperationClass, InfrastructureKind, Requirement,
        RequirementScope, RequirementTarget,
    },
    workflows::{
        RequirementWorkflowCheckpoint, RequirementWorkflowConfig, new_requirement_workflow,
    },
};
use replicant_workflow::{
    NewWorkflow, ResourceKey, WorkflowKind, WorkflowRepository, WorkflowState, WorkflowStatus,
};
use serde_json::Value;
use uuid::Uuid;

#[test]
fn requirement_children_and_claims_survive_restart() {
    let path = std::env::temp_dir().join(format!("requirement-{0}.sqlite", Uuid::new_v4()));
    let repository = WorkflowRepository::open(&path).expect("open repository");
    let parent = repository
        .create(new_requirement_workflow(RequirementWorkflowConfig {
            requirement: Requirement {
                id: "relay-sol".to_owned(),
                name: "SOL relay coverage".to_owned(),
                scope: RequirementScope::System("SOL".to_owned()),
                target: RequirementTarget::Infrastructure {
                    infrastructure: InfrastructureKind::Relay,
                },
                desired: 1,
                fulfillment: FulfillmentOperation {
                    operation_class: FulfillmentOperationClass::Workflow,
                    kind: "relay.expansion".to_owned(),
                    parameters: BTreeMap::new(),
                    claims: Vec::new(),
                },
            },
        }))
        .expect("create parent");
    let child = repository
        .create(NewWorkflow {
            kind: WorkflowKind::new("test.child").expect("kind"),
            schema_version: 1,
            config: Value::Null,
            checkpoint: Value::Null,
            current_step: Some("queued".to_owned()),
            parent_id: Some(parent.id),
        })
        .expect("create child");
    repository
        .acquire_claim(
            child.id,
            ResourceKey::Namespaced {
                namespace: "fulfillment".to_owned(),
                key: "relay-sol:1".to_owned(),
            },
        )
        .expect("claim child work");
    repository
        .update(
            parent.id,
            parent.revision,
            WorkflowState::<_, ()> {
                status: WorkflowStatus::Running,
                current_step: Some("awaiting_children".to_owned()),
                checkpoint: RequirementWorkflowCheckpoint {
                    plan: None,
                    children: vec![child.id],
                },
                last_error: None,
                result: None,
            },
        )
        .expect("checkpoint parent");
    drop(repository);

    let repository = WorkflowRepository::open(&path).expect("reopen repository");
    let resumed: RequirementWorkflowCheckpoint = repository
        .read(parent.id)
        .expect("read parent")
        .expect("parent exists")
        .checkpoint()
        .expect("decode checkpoint");
    assert_eq!(resumed.children, vec![child.id]);
    assert_eq!(
        repository
            .read(child.id)
            .expect("read child")
            .unwrap()
            .parent_id,
        Some(parent.id)
    );
    assert_eq!(repository.claims(child.id).expect("read claims").len(), 1);
    drop(repository);
    fs::remove_file(path).expect("remove database");
}
