//! Durable automation trigger persistence and deduplication tests.

use std::{collections::BTreeMap, fs};

use replicant_workflow::{
    AutomationPolicy, NewTrigger, TriggerCondition, TriggerTarget, TriggerTargetClass,
    WorkflowRepository,
};

fn schedule(next_run_at: i64) -> NewTrigger {
    NewTrigger {
        name: "hourly survey".into(),
        condition: TriggerCondition::Schedule {
            interval_millis: 3_600_000,
        },
        target: TriggerTarget {
            operation_class: TriggerTargetClass::Workflow,
            kind: "survey.route".into(),
            parameters: BTreeMap::new(),
        },
        enabled: true,
        next_run_at: Some(next_run_at),
        event_cursor: None,
    }
}

#[test]
fn disabled_automatic_triggers_do_not_claim_and_policy_survives_restart() {
    let path = std::env::temp_dir().join(format!(
        "replicant-trigger-policy-{}.sqlite",
        replicant_workflow::TriggerId::new()
    ));
    let trigger_id = {
        let repository = WorkflowRepository::open(&path).expect("open repository");
        let trigger = repository
            .create_trigger(schedule(10_000))
            .expect("create schedule");
        repository
            .set_automation_policy(AutomationPolicy {
                automatic_triggers_enabled: false,
                workflows_paused: false,
            })
            .expect("disable automatic triggers");
        assert!(
            !repository
                .claim_automatic_trigger_firing(
                    trigger.id,
                    "schedule:10000",
                    10_000,
                    Some(3_610_000),
                )
                .expect("automatic firing is suppressed")
        );
        assert!(
            repository
                .claim_trigger_firing(trigger.id, "manual:test", 10_000, None)
                .expect("manual firing remains available")
        );
        trigger.id
    };

    let repository = WorkflowRepository::open(&path).expect("reopen repository");
    assert!(
        !repository
            .automation_policy()
            .expect("policy")
            .automatic_triggers_enabled
    );
    assert!(
        !repository
            .claim_automatic_trigger_firing(trigger_id, "schedule:10000", 10_000, Some(3_610_000),)
            .expect("automatic firing remains suppressed")
    );
    drop(repository);
    fs::remove_file(path).expect("remove test database");
}

#[test]
fn schedule_claim_survives_restart_without_duplicate_firing() {
    let path = std::env::temp_dir().join(format!(
        "replicant-trigger-{}.sqlite",
        replicant_workflow::TriggerId::new()
    ));
    let trigger_id = {
        let repository = WorkflowRepository::open(&path).expect("open repository");
        let trigger = repository
            .create_trigger(schedule(10_000))
            .expect("create schedule");
        assert!(
            repository
                .claim_trigger_firing(trigger.id, "schedule:10000", 10_000, Some(3_610_000))
                .expect("claim firing")
        );
        assert!(
            !repository
                .claim_trigger_firing(trigger.id, "schedule:10000", 10_000, Some(3_610_000))
                .expect("deduplicate firing")
        );
        trigger.id
    };

    let repository = WorkflowRepository::open(&path).expect("reopen repository");
    let trigger = repository
        .read_trigger(trigger_id)
        .expect("read trigger")
        .expect("trigger exists");
    assert_eq!(trigger.last_fired_at, Some(10_000));
    assert_eq!(trigger.next_run_at, Some(3_610_000));
    assert!(
        !repository
            .claim_trigger_firing(trigger.id, "schedule:10000", 10_000, Some(3_610_000))
            .expect("deduplicate after restart")
    );

    drop(repository);
    fs::remove_file(path).expect("remove test database");
}
