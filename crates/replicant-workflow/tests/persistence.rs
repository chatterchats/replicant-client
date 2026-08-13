//! SQLite persistence and registry acceptance tests.

use std::{fs, sync::Arc};

use replicant_workflow::{
    NewWorkflow, RegistryError, RepositoryError, WorkflowFactory, WorkflowKind, WorkflowRegistry,
    WorkflowRepository, WorkflowState, WorkflowStatus,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct Config {
    system: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct Checkpoint {
    visits: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
struct ResultMetadata {
    surveyed: u32,
}

fn kind() -> WorkflowKind {
    WorkflowKind::new("survey.route").expect("valid kind")
}

fn create(
    repository: &WorkflowRepository,
    parent_id: Option<replicant_workflow::WorkflowId>,
) -> replicant_workflow::WorkflowInstance {
    repository
        .create(NewWorkflow {
            kind: kind(),
            schema_version: 2,
            config: Config {
                system: "SOL".into(),
            },
            checkpoint: Checkpoint { visits: 0 },
            current_step: Some("plan".into()),
            parent_id,
        })
        .expect("create workflow")
}

#[test]
fn creates_reads_and_lists_typed_workflows() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let parent = create(&repository, None);
    let child = create(&repository, Some(parent.id));

    let stored = repository
        .read(child.id)
        .expect("read workflow")
        .expect("workflow exists");
    assert_eq!(stored.kind, kind());
    assert_eq!(stored.schema_version, 2);
    assert_eq!(stored.status, WorkflowStatus::Queued);
    assert_eq!(stored.parent_id, Some(parent.id));
    assert_eq!(
        stored.config::<Config>().expect("decode config"),
        Config {
            system: "SOL".into()
        }
    );
    assert_eq!(
        stored
            .checkpoint::<Checkpoint>()
            .expect("decode checkpoint"),
        Checkpoint { visits: 0 }
    );
    assert_eq!(repository.list().expect("list workflows").len(), 2);
}

#[test]
fn updates_state_and_rejects_invalid_or_stale_transitions() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let queued = create(&repository, None);
    let running = repository
        .update(
            queued.id,
            queued.revision,
            WorkflowState::<_, ResultMetadata> {
                status: WorkflowStatus::Running,
                current_step: Some("travel".into()),
                checkpoint: Checkpoint { visits: 1 },
                last_error: None,
                result: None,
            },
        )
        .expect("start workflow");
    assert_eq!(running.revision, 1);
    assert_eq!(
        running.checkpoint::<Checkpoint>().expect("checkpoint"),
        Checkpoint { visits: 1 }
    );

    let stale = repository.update(
        running.id,
        0,
        WorkflowState::<_, ResultMetadata> {
            status: WorkflowStatus::Waiting,
            current_step: Some("wait".into()),
            checkpoint: Checkpoint { visits: 1 },
            last_error: None,
            result: None,
        },
    );
    assert!(matches!(
        stale,
        Err(RepositoryError::ConcurrentUpdate { .. })
    ));

    let succeeded = repository
        .update(
            running.id,
            running.revision,
            WorkflowState {
                status: WorkflowStatus::Succeeded,
                current_step: None,
                checkpoint: Checkpoint { visits: 2 },
                last_error: None,
                result: Some(ResultMetadata { surveyed: 2 }),
            },
        )
        .expect("complete workflow");
    assert_eq!(
        succeeded.result::<ResultMetadata>().expect("result"),
        Some(ResultMetadata { surveyed: 2 })
    );

    let invalid = repository.update(
        succeeded.id,
        succeeded.revision,
        WorkflowState::<_, ResultMetadata> {
            status: WorkflowStatus::Running,
            current_step: None,
            checkpoint: Checkpoint { visits: 2 },
            last_error: None,
            result: None,
        },
    );
    assert!(matches!(
        invalid,
        Err(RepositoryError::InvalidTransition { .. })
    ));
}

#[test]
fn persists_across_reopen() {
    let path = std::env::temp_dir().join(format!(
        "replicant-workflow-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let id = {
        let repository = WorkflowRepository::open(&path).expect("open repository");
        create(&repository, None).id
    };
    let reopened = WorkflowRepository::open(&path).expect("reopen repository");
    assert!(reopened.read(id).expect("read workflow").is_some());
    fs::remove_file(path).expect("remove test database");
}

struct Factory {
    kind: WorkflowKind,
}

impl WorkflowFactory for Factory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    fn current_schema_version(&self) -> u32 {
        2
    }
}

#[test]
fn registry_resolves_kind_and_schema_version() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let instance = create(&repository, None);
    let mut registry = WorkflowRegistry::new();
    registry
        .register(Arc::new(Factory { kind: kind() }))
        .expect("register factory");
    assert_eq!(
        registry
            .resolve(&instance)
            .expect("resolve")
            .current_schema_version(),
        2
    );

    let duplicate = registry.register(Arc::new(Factory { kind: kind() }));
    assert!(matches!(duplicate, Err(RegistryError::DuplicateKind(_))));

    let version_three = repository
        .create(NewWorkflow {
            kind: kind(),
            schema_version: 3,
            config: Config {
                system: "SOL".into(),
            },
            checkpoint: Checkpoint { visits: 0 },
            current_step: None,
            parent_id: None,
        })
        .expect("create newer workflow");
    assert!(matches!(
        registry.resolve(&version_three),
        Err(RegistryError::UnsupportedSchemaVersion { version: 3, .. })
    ));
}

#[test]
fn rejects_newer_database_schema_and_zero_workflow_schema() {
    let path = std::env::temp_dir().join(format!(
        "replicant-workflow-schema-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    {
        let connection = rusqlite::Connection::open(&path).expect("open sqlite");
        connection
            .execute(
                "CREATE TABLE runtime_schema_migrations (version INTEGER PRIMARY KEY NOT NULL)",
                [],
            )
            .expect("create migrations");
        connection
            .execute(
                "INSERT INTO runtime_schema_migrations (version) VALUES (99)",
                [],
            )
            .expect("insert future migration");
    }
    assert!(matches!(
        WorkflowRepository::open(&path),
        Err(RepositoryError::UnsupportedDatabaseSchema {
            found: 99,
            supported: 2
        })
    ));
    fs::remove_file(path).expect("remove test database");

    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let invalid = repository.create(NewWorkflow {
        kind: kind(),
        schema_version: 0,
        config: Config {
            system: "SOL".into(),
        },
        checkpoint: Checkpoint { visits: 0 },
        current_step: None,
        parent_id: None,
    });
    assert!(matches!(
        invalid,
        Err(RepositoryError::InvalidWorkflowSchemaVersion)
    ));
}
