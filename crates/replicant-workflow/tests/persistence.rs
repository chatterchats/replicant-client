//! SQLite persistence and registry acceptance tests.

use std::{
    fs,
    sync::{Arc, Barrier},
};

use replicant_workflow::{
    ClaimAcquireOutcome, NewWorkflow, RegistryError, RepositoryError, ResourceKey, WorkflowFactory,
    WorkflowKind, WorkflowMigration, WorkflowRegistry, WorkflowRepository, WorkflowState,
    WorkflowStatus, WorkflowSupervisor,
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
fn persists_namespaced_runtime_documents_with_revisions() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let first = serde_json::json!({"mode": "advisory", "enabled": true});
    let second = serde_json::json!({"mode": "automatic", "enabled": true});

    assert_eq!(
        repository
            .put_document("director.settings", "singleton", &first)
            .expect("insert document"),
        0
    );
    let (stored, revision) = repository
        .read_document("director.settings", "singleton")
        .expect("read document")
        .expect("document exists");
    assert_eq!(stored, first);
    assert_eq!(revision, 0);

    assert_eq!(
        repository
            .put_document("director.settings", "singleton", &second)
            .expect("update document"),
        1
    );
    assert_eq!(
        repository
            .list_documents("director.settings")
            .expect("list documents"),
        vec![("singleton".to_owned(), second, 1)]
    );
    assert!(
        repository
            .delete_document("director.settings", "singleton")
            .expect("delete document")
    );
    assert!(
        repository
            .read_document("director.settings", "singleton")
            .expect("read deleted document")
            .is_none()
    );
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

#[test]
fn claims_are_typed_idempotent_and_exclusive() {
    let repository = WorkflowRepository::open_in_memory().expect("open repository");
    let owner = create(&repository, None);
    let contender = create(&repository, None);
    let resource = ResourceKey::Device("VESSEL-1".into());

    let first = repository
        .acquire_claim(owner.id, resource.clone())
        .expect("acquire claim");
    let acquired_at = match first {
        ClaimAcquireOutcome::Acquired(claim) => claim.acquired_at,
        ClaimAcquireOutcome::AlreadyOwned(_) => panic!("new claim was already owned"),
    };
    assert!(matches!(
        repository
            .acquire_claim(owner.id, resource.clone())
            .expect("reacquire claim"),
        ClaimAcquireOutcome::AlreadyOwned(claim) if claim.acquired_at == acquired_at
    ));
    assert!(matches!(
        repository.acquire_claim(contender.id, resource.clone()),
        Err(RepositoryError::ClaimConflict { owner: id, .. }) if id == owner.id
    ));
    assert!(
        !repository
            .release_claim(contender.id, &resource)
            .expect("non-owner release")
    );
    assert!(
        repository
            .release_claim(owner.id, &resource)
            .expect("owner release")
    );

    for resource in [
        ResourceKey::Replicant("ADA".into()),
        ResourceKey::Autofactory("FACTORY-1".into()),
        ResourceKey::Namespaced {
            namespace: "survey.site".into(),
            key: "SOL:A1".into(),
        },
    ] {
        repository
            .acquire_claim(owner.id, resource)
            .expect("acquire typed claim");
    }
    assert_eq!(repository.claims(owner.id).expect("list claims").len(), 3);
}

#[test]
fn concurrent_workflows_cannot_claim_the_same_resource() {
    let path = std::env::temp_dir().join(format!(
        "replicant-workflow-claims-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let first_repository = WorkflowRepository::open(&path).expect("open first repository");
    let first = create(&first_repository, None).id;
    let second_repository = WorkflowRepository::open(&path).expect("open second repository");
    let second = create(&second_repository, None).id;
    let barrier = Arc::new(Barrier::new(3));

    let run = |repository: WorkflowRepository, workflow_id, barrier: Arc<Barrier>| {
        std::thread::spawn(move || {
            barrier.wait();
            repository.acquire_claim(workflow_id, ResourceKey::Replicant("ADA".into()))
        })
    };
    let first_thread = run(first_repository, first, barrier.clone());
    let second_thread = run(second_repository, second, barrier.clone());
    barrier.wait();
    let results = [
        first_thread.join().expect("first claimant thread"),
        second_thread.join().expect("second claimant thread"),
    ];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(RepositoryError::ClaimConflict { .. })))
            .count(),
        1
    );
    fs::remove_file(path).expect("remove test database");
}

#[tokio::test]
async fn startup_reconciles_terminal_and_missing_claim_owners() {
    let path = std::env::temp_dir().join(format!(
        "replicant-workflow-claim-reconcile-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let repository = WorkflowRepository::open(&path).expect("open repository");
    let terminal = create(&repository, None);
    let paused = create(&repository, None);
    repository
        .acquire_claim(terminal.id, ResourceKey::Device("DONE".into()))
        .expect("claim terminal resource");
    repository
        .acquire_claim(paused.id, ResourceKey::Device("PAUSED".into()))
        .expect("claim paused resource");
    let terminal = repository
        .update(
            terminal.id,
            terminal.revision,
            WorkflowState::<_, ()> {
                status: WorkflowStatus::Running,
                current_step: None,
                checkpoint: Checkpoint { visits: 0 },
                last_error: None,
                result: None,
            },
        )
        .expect("start owner");
    repository
        .update(
            terminal.id,
            terminal.revision,
            WorkflowState::<_, ()> {
                status: WorkflowStatus::Succeeded,
                current_step: None,
                checkpoint: Checkpoint { visits: 0 },
                last_error: None,
                result: None,
            },
        )
        .expect("complete owner");
    repository
        .update(
            paused.id,
            paused.revision,
            WorkflowState::<_, ()> {
                status: WorkflowStatus::Paused,
                current_step: None,
                checkpoint: Checkpoint { visits: 0 },
                last_error: None,
                result: None,
            },
        )
        .expect("pause owner");
    drop(repository);
    let raw = rusqlite::Connection::open(&path).expect("open raw sqlite");
    raw.pragma_update(None, "foreign_keys", "OFF")
        .expect("disable foreign keys for orphan fixture");
    raw.execute(
        "INSERT INTO workflow_resource_claims
             (resource_namespace, resource_key, workflow_id, acquired_at, updated_at)
             VALUES ('device', 'MISSING', '00000000-0000-0000-0000-000000000000', 1, 1)",
        [],
    )
    .expect("insert missing owner claim");

    let repository = Arc::new(WorkflowRepository::open(&path).expect("reopen repository"));
    let mut supervisor =
        WorkflowSupervisor::new(repository.clone(), Arc::new(WorkflowRegistry::new()));
    supervisor.tick().await.expect("startup reconciliation");
    assert!(
        repository
            .claims(terminal.id)
            .expect("terminal claims")
            .is_empty()
    );
    assert_eq!(
        repository.claims(paused.id).expect("paused claims").len(),
        1
    );
    drop(supervisor);
    drop(repository);
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

    fn supports_schema_version(&self, version: u32) -> bool {
        matches!(version, 1 | 2)
    }

    fn migrate(
        &self,
        instance: &replicant_workflow::WorkflowInstance,
    ) -> Result<Option<WorkflowMigration>, String> {
        let config = instance
            .config::<serde_json::Value>()
            .map_err(|error| error.to_string())?;
        let checkpoint = instance
            .checkpoint::<serde_json::Value>()
            .map_err(|error| error.to_string())?;
        Ok(Some(WorkflowMigration::new(
            serde_json::json!({ "system": config["system_code"] }),
            serde_json::json!({ "visits": checkpoint["seen"] }),
        )))
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
            supported: 8
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

#[tokio::test]
async fn explicitly_migrates_old_checkpoint_before_executor_resolution() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let workflow = repository
        .create(NewWorkflow {
            kind: kind(),
            schema_version: 1,
            config: serde_json::json!({ "system_code": "SOL" }),
            checkpoint: serde_json::json!({ "seen": 4 }),
            current_step: None,
            parent_id: None,
        })
        .expect("create old workflow");
    let mut registry = WorkflowRegistry::new();
    registry
        .register(Arc::new(Factory { kind: kind() }))
        .expect("register factory");
    let mut supervisor = WorkflowSupervisor::new(repository.clone(), Arc::new(registry));

    supervisor.tick().await.expect("migrate workflow");

    let migrated = repository
        .read(workflow.id)
        .expect("read workflow")
        .expect("workflow exists");
    assert_eq!(migrated.schema_version, 2);
    assert_eq!(migrated.config::<Config>().unwrap().system, "SOL");
    assert_eq!(migrated.checkpoint::<Checkpoint>().unwrap().visits, 4);
    assert_eq!(migrated.status, WorkflowStatus::Failed);
    assert_eq!(
        migrated.revision, 3,
        "migration and failure are separate durable writes"
    );
}

#[tokio::test]
async fn unsupported_checkpoint_fails_without_running_or_losing_claims_history() {
    struct UnsupportedFactory(WorkflowKind);
    impl WorkflowFactory for UnsupportedFactory {
        fn kind(&self) -> &WorkflowKind {
            &self.0
        }

        fn current_schema_version(&self) -> u32 {
            2
        }
    }

    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let workflow = repository
        .create(NewWorkflow {
            kind: kind(),
            schema_version: 1,
            config: serde_json::json!({ "incompatible": true }),
            checkpoint: serde_json::json!(["not", "reinterpreted"]),
            current_step: None,
            parent_id: None,
        })
        .expect("create incompatible workflow");
    repository
        .acquire_claim(workflow.id, ResourceKey::Device("SAFE".into()))
        .expect("claim resource");
    let mut registry = WorkflowRegistry::new();
    registry
        .register(Arc::new(UnsupportedFactory(kind())))
        .expect("register factory");
    let mut supervisor = WorkflowSupervisor::new(repository.clone(), Arc::new(registry));

    supervisor
        .tick()
        .await
        .expect("reject incompatible workflow");

    let failed = repository.read(workflow.id).unwrap().unwrap();
    assert_eq!(failed.status, WorkflowStatus::Failed);
    assert!(
        failed
            .last_error
            .unwrap()
            .contains("does not support schema version 1")
    );
    assert!(repository.claims(workflow.id).unwrap().is_empty());
    assert_eq!(
        repository.list().unwrap().len(),
        1,
        "terminal history is retained"
    );
}

#[test]
fn migrates_an_existing_runtime_database_without_losing_workflows() {
    let path = std::env::temp_dir().join(format!(
        "replicant-workflow-database-migration-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let id = uuid::Uuid::new_v4();
    {
        let connection = rusqlite::Connection::open(&path).expect("open old database");
        connection
            .execute_batch(concat!(
                include_str!("../migrations/0001_initial.sql"),
                include_str!("../migrations/0002_activity.sql"),
                include_str!("../migrations/0003_resource_claims.sql"),
                "CREATE TABLE runtime_schema_migrations (version INTEGER PRIMARY KEY NOT NULL);",
                "INSERT INTO runtime_schema_migrations VALUES (1), (2), (3);"
            ))
            .expect("install old schema");
        connection
            .execute(
                "INSERT INTO workflow_instances
                 (id, kind, schema_version, config_json, checkpoint_json, status,
                  created_at, updated_at)
                 VALUES (?1, 'survey.route', 2, '{\"system\":\"SOL\"}',
                         '{\"visits\":7}', 'paused', 1, 1)",
                [id.to_string()],
            )
            .expect("insert old workflow");
    }

    let repository = WorkflowRepository::open(&path).expect("migrate database");
    let workflows = repository.list().expect("read migrated workflows");
    assert_eq!(workflows.len(), 1);
    assert_eq!(workflows[0].checkpoint::<Checkpoint>().unwrap().visits, 7);
    assert_eq!(repository.automation_policy().unwrap(), Default::default());
    drop(repository);
    fs::remove_file(path).expect("remove test database");
}

#[test]
fn rejects_gapped_database_migration_history() {
    let path = std::env::temp_dir().join(format!(
        "replicant-workflow-gapped-migration-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    {
        let connection = rusqlite::Connection::open(&path).expect("open database");
        connection
            .execute_batch(
                "CREATE TABLE runtime_schema_migrations (version INTEGER PRIMARY KEY NOT NULL);
                 INSERT INTO runtime_schema_migrations VALUES (1), (3);",
            )
            .expect("create invalid history");
    }
    assert!(matches!(
        WorkflowRepository::open(&path),
        Err(RepositoryError::InvalidMigrationHistory(versions)) if versions == vec![1, 3]
    ));
    fs::remove_file(path).expect("remove test database");
}

#[test]
fn rejects_corrupt_runtime_database_without_recreating_it() {
    let path = std::env::temp_dir().join(format!(
        "replicant-workflow-corrupt-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    fs::write(&path, b"not a sqlite database").expect("write corrupt fixture");
    assert!(WorkflowRepository::open(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), b"not a sqlite database");
    fs::remove_file(path).expect("remove test database");
}
