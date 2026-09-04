//! Durable reservation and structured-target intelligence acceptance tests.

use replicant_workflow::{
    AllocationCandidate, AllocationLocation, NewWorkflow, RequirementScope, ResourceKey,
    ResourceRequirement, WorkItemSpec, WorkItemTransition, WorkflowKind, WorkflowRepository,
    WorkflowTarget,
};
use serde_json::json;

fn workflow(repository: &WorkflowRepository) -> replicant_workflow::WorkflowInstance {
    repository
        .create(NewWorkflow {
            kind: WorkflowKind::new("test.intelligence").expect("valid workflow kind"),
            schema_version: 1,
            config: json!({}),
            checkpoint: json!({}),
            current_step: None,
            parent_id: None,
        })
        .expect("create workflow")
}

#[test]
fn active_reservation_projection_tracks_quantity_and_releases_on_terminal_item() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let workflow = workflow(&repository);
    let initial_intelligence_revision = repository
        .workflow_intelligence_revision()
        .expect("read intelligence revision");
    let requirements = vec![ResourceRequirement {
        key: "material:structural".into(),
        kind: "material".into(),
        capabilities: vec!["structural".into()],
        scope: RequirementScope::Location("HUB-1".into()),
        count: 1,
        quantity: 400,
    }];
    let item = WorkItemSpec {
        workflow_id: workflow.id,
        dedupe_key: "stage".into(),
        kind: WorkflowKind::new("test.stage").expect("valid work-item kind"),
        sort_key: "stage".into(),
        payload_json: json!({}),
        preconditions_json: json!([]),
        requirements_json: serde_json::to_value(&requirements).expect("serialize requirements"),
        deadline_at_ms: None,
    };
    repository
        .reconcile_work_items(workflow.id, &[item], 10)
        .expect("reconcile work item");
    let assigned = repository
        .claim_next_work_item(workflow.id, 20)
        .expect("claim work item")
        .expect("assigned work item");
    let candidate = AllocationCandidate {
        resource: ResourceKey::Namespaced {
            namespace: "inventory".into(),
            key: "location:HUB-1:structural".into(),
        },
        kind: "material".into(),
        capabilities: vec!["structural".into()],
        location: Some(AllocationLocation {
            region: Some("Alpha".into()),
            system: Some("HUB".into()),
            designation: Some("HUB-1".into()),
            distances_ly: Default::default(),
        }),
        available_quantity: 1_000,
        observed_revision: 7,
        observed_at_ms: 21,
    };
    repository
        .allocate_requirements(assigned.id, assigned.state.revision, &[candidate])
        .expect("reserve material");
    let reserved_revision = repository
        .workflow_intelligence_revision()
        .expect("read reservation revision");
    assert!(reserved_revision > initial_intelligence_revision);

    let reservations = repository
        .active_resource_reservations()
        .expect("list active reservations");
    assert_eq!(reservations.len(), 1);
    let reservation = &reservations[0];
    assert_eq!(reservation.workflow_id, workflow.id);
    assert_eq!(reservation.item_id, assigned.id);
    assert_eq!(reservation.requirement_key, "material:structural");
    assert_eq!(reservation.kind, "material");
    assert_eq!(reservation.capabilities, vec!["structural"]);
    assert_eq!(reservation.quantity, 400);
    assert_eq!(
        reservation
            .location
            .as_ref()
            .and_then(|location| location.designation.as_deref()),
        Some("HUB-1")
    );

    let running = repository
        .start_work_item(
            assigned.id,
            assigned.state.revision,
            "worker-1",
            "assignment-1",
            30,
        )
        .expect("start work item");
    repository
        .transition_work_item(
            running.id,
            running.state.revision,
            WorkItemTransition::Succeeded {
                checkpoint_json: None,
                result_json: None,
            },
            40,
        )
        .expect("complete work item");

    assert!(
        repository
            .active_resource_reservations()
            .expect("list released reservations")
            .is_empty()
    );
    assert!(
        repository
            .workflow_intelligence_revision()
            .expect("read release revision")
            > reserved_revision
    );
}

#[test]
fn workflow_targets_are_idempotent_and_support_reverse_lookup() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let workflow = workflow(&repository);
    let initial_intelligence_revision = repository
        .workflow_intelligence_revision()
        .expect("read intelligence revision");
    let target = WorkflowTarget::Event {
        event_id: "EVT-42".into(),
        system: "THYFFAWFF".into(),
        location: "THYFFAWFF-3-L4".into(),
    };

    let first = repository
        .record_workflow_targets(workflow.id, std::slice::from_ref(&target), 100)
        .expect("record target");
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].target, target);
    assert!(first[0].active);
    assert_eq!(first[0].created_at_ms, 100);
    assert_eq!(first[0].updated_at_ms, 100);
    let first_target_revision = repository
        .workflow_intelligence_revision()
        .expect("read target revision");
    assert!(first_target_revision > initial_intelligence_revision);

    let second = repository
        .record_workflow_targets(workflow.id, std::slice::from_ref(&target), 200)
        .expect("record target idempotently");
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].created_at_ms, 100);
    assert_eq!(second[0].updated_at_ms, 100);
    assert!(second[0].active);
    assert_eq!(
        repository
            .workflow_intelligence_revision()
            .expect("read idempotent target revision"),
        first_target_revision
    );

    let reverse = repository
        .workflows_targeting("event", "EVT-42")
        .expect("reverse lookup target");
    assert_eq!(reverse.len(), 1);
    assert_eq!(reverse[0].workflow_id, workflow.id);
    assert_eq!(reverse[0].target, target);
    assert!(reverse[0].active);

    let replacement = WorkflowTarget::Event {
        event_id: "EVT-43".into(),
        system: "INASTI".into(),
        location: "INASTI-2-L4".into(),
    };
    let replaced = repository
        .replace_workflow_targets(workflow.id, std::slice::from_ref(&replacement), 300)
        .expect("replace target set");
    assert_eq!(replaced.len(), 2);
    let released = replaced
        .iter()
        .find(|record| record.target == target)
        .expect("released historical target");
    assert!(!released.active);
    let current = replaced
        .iter()
        .find(|record| record.target == replacement)
        .expect("replacement target");
    assert!(current.active);
    let replacement_revision = repository
        .workflow_intelligence_revision()
        .expect("read replacement revision");
    let unchanged = repository
        .replace_workflow_targets(workflow.id, std::slice::from_ref(&replacement), 400)
        .expect("replace identical target set");
    assert_eq!(
        unchanged
            .iter()
            .find(|record| record.target == replacement)
            .expect("unchanged target")
            .updated_at_ms,
        300
    );
    assert_eq!(
        repository
            .workflow_intelligence_revision()
            .expect("read unchanged replacement revision"),
        replacement_revision
    );

    let active = repository
        .active_workflow_targets()
        .expect("list active targets");
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].workflow_id, workflow.id);
    assert_eq!(active[0].target, replacement);
    assert!(active[0].active);

    let old_reverse = repository
        .workflows_targeting("event", "EVT-42")
        .expect("reverse lookup released target");
    assert_eq!(old_reverse.len(), 1);
    assert!(!old_reverse[0].active);
}
