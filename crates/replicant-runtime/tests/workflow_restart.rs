//! Restart coverage for persisted runtime workflow checkpoints.

use std::{collections::BTreeSet, fs, path::PathBuf};

use replicant_runtime::relay::RelayExpansionRequest;
use replicant_runtime::workflows::{
    RelayWorkflowCheckpoint, RelayWorkflowConfig, SurveyWorkflowCheckpoint, SurveyWorkflowConfig,
    new_relay_workflow, new_survey_workflow,
};
use replicant_workflow::{WorkflowRepository, WorkflowState, WorkflowStatus};
use uuid::Uuid;

fn database_path() -> PathBuf {
    std::env::temp_dir().join(format!(
        "replicant-workflow-restart-{}.sqlite",
        Uuid::new_v4()
    ))
}

#[test]
fn survey_and_relay_checkpoints_resume_without_repeating_completed_steps() {
    let path = database_path();
    let repository = WorkflowRepository::open(&path).expect("open workflow repository");
    let survey = repository
        .create(new_survey_workflow(SurveyWorkflowConfig {
            region: "Alpha".to_owned(),
            center: "ROOT".to_owned(),
            radius_ly: 1.0,
            system_limit: 1,
            target_systems: None,
            star_detail_concurrency: 1,
            mission_file: path.with_extension("survey.json"),
            replace_plan: false,
            include_explored: false,
            travel_timeout: std::time::Duration::from_secs(1),
            survey_timeout: std::time::Duration::from_secs(1),
            maintenance_home: "ROOT".to_owned(),
            maintenance_interval: 1,
            maintenance_threshold_pct: 25.0,
            maintenance_resume_pct: 95.0,
            maintenance_check_interval: std::time::Duration::from_secs(1),
        }))
        .expect("create survey workflow");
    let relay = repository
        .create(new_relay_workflow(RelayWorkflowConfig::from_request(
            RelayExpansionRequest {
                replicant: "REP-2".to_owned(),
                hub: "ROOT-L1".to_owned(),
                targets: vec!["TARGET".to_owned()],
                mission_file: path.with_extension("relay.json"),
                max_hop_ly: 7.499,
                wait_timeout: std::time::Duration::from_secs(1),
                unavailable_autofactories: Default::default(),
            },
        )))
        .expect("create relay workflow");

    let survey_checkpoint = SurveyWorkflowCheckpoint {
        state: None,
        migration_worker: None,
        completed_steps: BTreeSet::from(["preparing_fleet".to_owned(), "traveling".to_owned()]),
    };
    repository
        .update(
            survey.id,
            survey.revision,
            WorkflowState::<_, ()> {
                status: WorkflowStatus::Running,
                current_step: Some("surveying".to_owned()),
                checkpoint: survey_checkpoint,
                last_error: None,
                result: None,
            },
        )
        .expect("checkpoint survey");
    let relay_checkpoint = RelayWorkflowCheckpoint {
        state: None,
        region: None,
        completed_steps: BTreeSet::from(["awaiting_relays".to_owned()]),
    };
    repository
        .update(
            relay.id,
            relay.revision,
            WorkflowState::<_, ()> {
                status: WorkflowStatus::Running,
                current_step: Some("deploying".to_owned()),
                checkpoint: relay_checkpoint,
                last_error: None,
                result: None,
            },
        )
        .expect("checkpoint relay");
    drop(repository);

    let repository = WorkflowRepository::open(&path).expect("reopen after process restart");
    let survey_instance = repository
        .read(survey.id)
        .expect("read survey")
        .expect("survey exists");
    let mut survey_checkpoint: SurveyWorkflowCheckpoint = survey_instance
        .checkpoint()
        .expect("decode survey checkpoint");
    assert!(
        survey_checkpoint
            .completed_steps
            .insert("surveying".to_owned())
    );
    assert!(
        !survey_checkpoint
            .completed_steps
            .insert("traveling".to_owned())
    );
    repository
        .update(
            survey.id,
            survey_instance.revision,
            WorkflowState::<_, ()> {
                status: WorkflowStatus::Running,
                current_step: Some("restowing".to_owned()),
                checkpoint: survey_checkpoint,
                last_error: None,
                result: None,
            },
        )
        .expect("continue survey after restart");

    let relay_instance = repository
        .read(relay.id)
        .expect("read relay")
        .expect("relay exists");
    let mut relay_checkpoint: RelayWorkflowCheckpoint = relay_instance
        .checkpoint()
        .expect("decode relay checkpoint");
    assert!(
        relay_checkpoint
            .completed_steps
            .insert("deploying".to_owned())
    );
    assert!(
        !relay_checkpoint
            .completed_steps
            .insert("awaiting_relays".to_owned())
    );
    repository
        .update(
            relay.id,
            relay_instance.revision,
            WorkflowState::<_, ()> {
                status: WorkflowStatus::Running,
                current_step: Some("returning_to_hub".to_owned()),
                checkpoint: relay_checkpoint,
                last_error: None,
                result: None,
            },
        )
        .expect("continue relay after restart");
    drop(repository);

    let repository = WorkflowRepository::open(&path).expect("reopen second checkpoint");
    let resumed: SurveyWorkflowCheckpoint = repository
        .read(survey.id)
        .expect("read resumed survey")
        .expect("survey exists")
        .checkpoint()
        .expect("decode resumed survey");
    assert_eq!(resumed.completed_steps.len(), 3);
    let resumed: RelayWorkflowCheckpoint = repository
        .read(relay.id)
        .expect("read resumed relay")
        .expect("relay exists")
        .checkpoint()
        .expect("decode resumed relay");
    assert_eq!(resumed.completed_steps.len(), 2);
    drop(repository);
    fs::remove_file(path).expect("remove temporary workflow database");
}
