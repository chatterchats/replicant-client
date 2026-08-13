//! Durable supervisor lifecycle integration tests.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use replicant_workflow::{
    BoxWorkflowFuture, ControlRequest, NewWorkflow, ResourceKey, WorkflowContext, WorkflowExecutor,
    WorkflowFactory, WorkflowId, WorkflowKind, WorkflowRegistry, WorkflowRepository,
    WorkflowStatus, WorkflowSupervisor,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

#[derive(Clone, Deserialize, Serialize)]
struct Config {
    steps: usize,
    panic_after_first_step: bool,
}

#[derive(Clone, Deserialize, Serialize)]
struct Checkpoint {
    completed: usize,
}

struct Harness {
    reached: Semaphore,
    proceed: Semaphore,
    executions: AtomicUsize,
}

impl Default for Harness {
    fn default() -> Self {
        Self {
            reached: Semaphore::new(0),
            proceed: Semaphore::new(0),
            executions: AtomicUsize::new(0),
        }
    }
}

struct Factory {
    kind: WorkflowKind,
    harness: Arc<Harness>,
}

impl WorkflowFactory for Factory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    fn current_schema_version(&self) -> u32 {
        1
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(TestWorkflow {
            harness: self.harness.clone(),
        }))
    }
}

struct TestWorkflow {
    harness: Arc<Harness>,
}

impl WorkflowExecutor for TestWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            self.harness.executions.fetch_add(1, Ordering::SeqCst);
            let config: Config = context.config().map_err(|error| error.to_string())?;
            let mut checkpoint: Checkpoint =
                context.checkpoint().map_err(|error| error.to_string())?;
            while checkpoint.completed < config.steps {
                if context
                    .control_request()
                    .map_err(|error| error.to_string())?
                    != ControlRequest::Continue
                {
                    return Ok(());
                }
                checkpoint.completed += 1;
                context
                    .advance_to(format!("step-{}", checkpoint.completed), &checkpoint)
                    .map_err(|error| error.to_string())?;
                context
                    .emit_activity(format!("completed step {}", checkpoint.completed))
                    .map_err(|error| error.to_string())?;
                self.harness.reached.add_permits(1);
                self.harness
                    .proceed
                    .acquire()
                    .await
                    .expect("test harness remains open")
                    .forget();
                assert!(
                    !(config.panic_after_first_step && checkpoint.completed == 1),
                    "intentional test panic"
                );
            }
            context
                .mark_succeeded::<()>(None)
                .map_err(|error| error.to_string())
        })
    }
}

fn setup(
    repository: Arc<WorkflowRepository>,
    panic_after_first_step: bool,
) -> (
    WorkflowId,
    Arc<Harness>,
    Arc<WorkflowRegistry>,
    WorkflowSupervisor,
) {
    let kind = WorkflowKind::new("test.checkpoint").expect("valid kind");
    let instance = repository
        .create(NewWorkflow {
            kind: kind.clone(),
            schema_version: 1,
            config: Config {
                steps: 3,
                panic_after_first_step,
            },
            checkpoint: Checkpoint { completed: 0 },
            current_step: None,
            parent_id: None,
        })
        .expect("create workflow");
    let harness = Arc::new(Harness::default());
    let mut registry = WorkflowRegistry::new();
    registry
        .register(Arc::new(Factory {
            kind,
            harness: harness.clone(),
        }))
        .expect("register workflow");
    let registry = Arc::new(registry);
    let supervisor = WorkflowSupervisor::new(repository, registry.clone());
    (instance.id, harness, registry, supervisor)
}

async fn reach_step(harness: &Harness) {
    harness
        .reached
        .acquire()
        .await
        .expect("test harness remains open")
        .forget();
}

async fn wait_for_status(
    supervisor: &mut WorkflowSupervisor,
    repository: &WorkflowRepository,
    id: WorkflowId,
    expected: WorkflowStatus,
) {
    for _ in 0..100 {
        supervisor.tick().await.expect("tick supervisor");
        if repository.read(id).expect("read workflow").unwrap().status == expected {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("workflow did not reach {expected:?}");
}

async fn finish_remaining(harness: &Harness, completed: usize) {
    for _ in completed..3 {
        reach_step(harness).await;
        harness.proceed.add_permits(1);
    }
}

#[tokio::test]
async fn completes_and_checkpoints_each_step() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let (id, harness, _, mut supervisor) = setup(repository.clone(), false);
    repository
        .acquire_claim(id, ResourceKey::Replicant("ADA".into()))
        .expect("claim resource");
    supervisor.tick().await.expect("start workflow");
    finish_remaining(&harness, 0).await;
    wait_for_status(&mut supervisor, &repository, id, WorkflowStatus::Succeeded).await;

    let checkpoint: Checkpoint = repository
        .read(id)
        .expect("read workflow")
        .unwrap()
        .checkpoint()
        .expect("decode checkpoint");
    assert_eq!(checkpoint.completed, 3);
    assert_eq!(repository.activity(id).expect("read activity").len(), 3);
    assert!(repository.claims(id).expect("read claims").is_empty());
}

#[tokio::test]
async fn pauses_and_resumes_cooperatively() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let (id, harness, _, mut supervisor) = setup(repository.clone(), false);
    repository
        .acquire_claim(id, ResourceKey::Device("VESSEL-1".into()))
        .expect("claim resource");
    supervisor.tick().await.expect("start workflow");
    reach_step(&harness).await;
    supervisor.pause(id).expect("request pause");
    harness.proceed.add_permits(1);
    wait_for_status(&mut supervisor, &repository, id, WorkflowStatus::Paused).await;
    while supervisor.has_executor(id) {
        tokio::task::yield_now().await;
        supervisor.tick().await.expect("reap paused executor");
    }
    assert_eq!(repository.claims(id).expect("read claims").len(), 1);

    supervisor.resume(id).expect("resume workflow");
    supervisor.tick().await.expect("restart workflow");
    finish_remaining(&harness, 1).await;
    wait_for_status(&mut supervisor, &repository, id, WorkflowStatus::Succeeded).await;
    assert_eq!(harness.executions.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancels_cooperatively() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let (id, harness, _, mut supervisor) = setup(repository.clone(), false);
    repository
        .acquire_claim(id, ResourceKey::Autofactory("FACTORY-1".into()))
        .expect("claim resource");
    supervisor.tick().await.expect("start workflow");
    reach_step(&harness).await;
    supervisor.cancel(id).expect("request cancellation");
    assert_eq!(repository.claims(id).expect("read claims").len(), 1);
    harness.proceed.add_permits(1);
    while supervisor.has_executor(id) {
        tokio::task::yield_now().await;
        supervisor.tick().await.expect("reap cancelled executor");
    }
    assert!(repository.claims(id).expect("read claims").is_empty());
    assert_eq!(repository.activity(id).expect("read activity").len(), 1);
}

#[tokio::test]
async fn reopens_and_resumes_without_repeating_completed_steps() {
    let path = std::env::temp_dir().join(format!(
        "replicant-workflow-supervisor-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let repository = Arc::new(WorkflowRepository::open(&path).expect("open repository"));
    let (id, harness, registry, mut supervisor) = setup(repository.clone(), false);
    supervisor.tick().await.expect("start workflow");
    reach_step(&harness).await;
    drop(supervisor);
    drop(repository);
    tokio::task::yield_now().await;

    let repository = Arc::new(WorkflowRepository::open(&path).expect("reopen repository"));
    let mut supervisor = WorkflowSupervisor::new(repository.clone(), registry);
    supervisor.tick().await.expect("reconcile workflow");
    finish_remaining(&harness, 1).await;
    wait_for_status(&mut supervisor, &repository, id, WorkflowStatus::Succeeded).await;

    let activity = repository.activity(id).expect("read activity");
    assert_eq!(activity.len(), 3);
    assert_eq!(activity[0].message, "completed step 1");
    assert_eq!(activity[1].message, "completed step 2");
    assert_eq!(activity[2].message, "completed step 3");
    drop(supervisor);
    drop(repository);
    std::fs::remove_file(path).expect("remove test database");
}

#[tokio::test]
async fn records_executor_panics_as_failures() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let (id, harness, _, mut supervisor) = setup(repository.clone(), true);
    supervisor.tick().await.expect("start workflow");
    reach_step(&harness).await;
    harness.proceed.add_permits(1);
    wait_for_status(&mut supervisor, &repository, id, WorkflowStatus::Failed).await;
    assert!(
        repository
            .read(id)
            .expect("read workflow")
            .unwrap()
            .last_error
            .unwrap()
            .contains("task failed")
    );
}
