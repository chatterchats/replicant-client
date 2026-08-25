//! Durable supervisor lifecycle integration tests.

use std::sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
};

use replicant_workflow::{
    AutomationPolicy, BoxWorkflowFuture, ControlRequest, NewWorkflow, ResourceKey, WaitIntent,
    WaitOutcome, WaitSignal, WorkflowContext, WorkflowExecutor, WorkflowFactory, WorkflowId,
    WorkflowKind, WorkflowRegistry, WorkflowRepository, WorkflowStatus, WorkflowSupervisor,
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

struct BlockingFactory {
    inner: Factory,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl WorkflowFactory for BlockingFactory {
    fn kind(&self) -> &WorkflowKind {
        self.inner.kind()
    }

    fn current_schema_version(&self) -> u32 {
        1
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        self.entered.wait();
        self.release.wait();
        self.inner.create_executor()
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

struct NonCooperativeFactory {
    kind: WorkflowKind,
    harness: Arc<Harness>,
}

impl WorkflowFactory for NonCooperativeFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    fn current_schema_version(&self) -> u32 {
        1
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(NonCooperativeWorkflow {
            harness: self.harness.clone(),
        }))
    }
}

struct NonCooperativeWorkflow {
    harness: Arc<Harness>,
}

impl WorkflowExecutor for NonCooperativeWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            self.harness.executions.fetch_add(1, Ordering::SeqCst);
            self.harness.reached.add_permits(1);
            self.harness
                .proceed
                .acquire()
                .await
                .expect("test harness remains open")
                .forget();
            context
                .emit_activity("mutation after wait")
                .map_err(|error| error.to_string())?;
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
    supervisor: &WorkflowSupervisor,
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

#[test]
fn controls_remain_available_while_tick_starts_executor() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let kind = WorkflowKind::new("test.concurrent-control").expect("valid kind");
    let instance = repository
        .create(NewWorkflow {
            kind: kind.clone(),
            schema_version: 1,
            config: Config {
                steps: 1,
                panic_after_first_step: false,
            },
            checkpoint: Checkpoint { completed: 0 },
            current_step: None,
            parent_id: None,
        })
        .expect("create workflow");
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let mut registry = WorkflowRegistry::new();
    registry
        .register(Arc::new(BlockingFactory {
            inner: Factory {
                kind,
                harness: Arc::new(Harness::default()),
            },
            entered: entered.clone(),
            release: release.clone(),
        }))
        .expect("register workflow");
    let supervisor = Arc::new(WorkflowSupervisor::new(
        repository.clone(),
        Arc::new(registry),
    ));
    let ticking = supervisor.clone();
    let tick = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build runtime")
            .block_on(ticking.tick())
    });

    entered.wait();
    let (paused, pause_result) = std::sync::mpsc::channel();
    let controlling = supervisor.clone();
    let control = std::thread::spawn(move || {
        paused
            .send(controlling.pause(instance.id))
            .expect("send pause result");
    });
    let pause_result = pause_result.recv_timeout(std::time::Duration::from_secs(2));
    release.wait();
    tick.join().expect("tick thread").expect("tick supervisor");
    control.join().expect("control thread");
    pause_result
        .expect("pause is not blocked by tick")
        .expect("pause during tick");
    assert_eq!(
        repository.read(instance.id).unwrap().unwrap().status,
        WorkflowStatus::Paused
    );
}

#[tokio::test]
async fn completes_and_checkpoints_each_step() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let (id, harness, _, supervisor) = setup(repository.clone(), false);
    repository
        .acquire_claim(id, ResourceKey::Replicant("ADA".into()))
        .expect("claim resource");
    supervisor.tick().await.expect("start workflow");
    finish_remaining(&harness, 0).await;
    wait_for_status(&supervisor, &repository, id, WorkflowStatus::Succeeded).await;

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
    let (id, harness, _, supervisor) = setup(repository.clone(), false);
    repository
        .acquire_claim(id, ResourceKey::Device("VESSEL-1".into()))
        .expect("claim resource");
    supervisor.tick().await.expect("start workflow");
    reach_step(&harness).await;
    supervisor.pause(id).expect("request pause");
    harness.proceed.add_permits(1);
    wait_for_status(&supervisor, &repository, id, WorkflowStatus::Paused).await;
    while supervisor.has_executor(id) {
        tokio::task::yield_now().await;
        supervisor.tick().await.expect("reap paused executor");
    }
    assert_eq!(repository.claims(id).expect("read claims").len(), 1);

    supervisor.resume(id).expect("resume workflow");
    supervisor.tick().await.expect("restart workflow");
    finish_remaining(&harness, 1).await;
    wait_for_status(&supervisor, &repository, id, WorkflowStatus::Succeeded).await;
    assert_eq!(harness.executions.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn pause_stops_an_executor_that_does_not_poll_control() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let kind = WorkflowKind::new("test.non-cooperative").expect("valid kind");
    let instance = repository
        .create(NewWorkflow {
            kind: kind.clone(),
            schema_version: 1,
            config: Config {
                steps: 1,
                panic_after_first_step: false,
            },
            checkpoint: Checkpoint { completed: 0 },
            current_step: None,
            parent_id: None,
        })
        .expect("create workflow");
    let harness = Arc::new(Harness::default());
    let mut registry = WorkflowRegistry::new();
    registry
        .register(Arc::new(NonCooperativeFactory {
            kind,
            harness: harness.clone(),
        }))
        .expect("register workflow");
    let supervisor = WorkflowSupervisor::new(repository.clone(), Arc::new(registry));

    supervisor.tick().await.expect("start workflow");
    reach_step(&harness).await;
    supervisor.pause(instance.id).expect("pause workflow");
    harness.proceed.add_permits(1);
    while supervisor.has_executor(instance.id) {
        tokio::task::yield_now().await;
        supervisor.tick().await.expect("reap paused executor");
    }

    assert_eq!(
        repository.read(instance.id).unwrap().unwrap().status,
        WorkflowStatus::Paused
    );
    assert!(
        repository
            .activity(instance.id)
            .expect("read activity")
            .is_empty()
    );
}

#[tokio::test]
async fn pause_all_survives_restart_and_blocks_executor_start() {
    let path = std::env::temp_dir().join(format!(
        "replicant-workflow-pause-all-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let repository = Arc::new(WorkflowRepository::open(&path).expect("open repository"));
    let (id, harness, registry, supervisor) = setup(repository.clone(), false);
    repository
        .set_automation_policy(AutomationPolicy {
            automatic_triggers_enabled: true,
            workflows_paused: true,
        })
        .expect("persist global pause");
    assert_eq!(supervisor.pause_all().expect("pause all"), 1);
    drop(supervisor);
    drop(repository);

    let repository = Arc::new(WorkflowRepository::open(&path).expect("reopen repository"));
    let supervisor = WorkflowSupervisor::new(repository.clone(), registry);
    supervisor.tick().await.expect("reconcile while paused");
    assert!(
        repository
            .automation_policy()
            .expect("policy")
            .workflows_paused
    );
    assert_eq!(
        repository.read(id).expect("workflow").unwrap().status,
        WorkflowStatus::Paused
    );
    assert_eq!(harness.executions.load(Ordering::SeqCst), 0);

    drop(supervisor);
    drop(repository);
    std::fs::remove_file(path).expect("remove test database");
}

#[tokio::test]
async fn cancels_cooperatively() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let (id, harness, _, supervisor) = setup(repository.clone(), false);
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
    let mut repository = Arc::new(WorkflowRepository::open(&path).expect("open repository"));
    let (id, harness, registry, mut supervisor) = setup(repository.clone(), false);
    repository
        .acquire_claim(id, ResourceKey::Device("RESTART-VESSEL".into()))
        .expect("claim resource");

    for expected_completed in 1..=3 {
        supervisor.tick().await.expect("start workflow");
        reach_step(&harness).await;
        assert_eq!(
            repository
                .read(id)
                .unwrap()
                .unwrap()
                .checkpoint::<Checkpoint>()
                .unwrap()
                .completed,
            expected_completed
        );
        assert_eq!(repository.claims(id).unwrap().len(), 1);
        drop(supervisor);
        drop(repository);
        tokio::task::yield_now().await;
        repository = Arc::new(WorkflowRepository::open(&path).expect("reopen repository"));
        supervisor = WorkflowSupervisor::new(repository.clone(), registry.clone());
    }

    wait_for_status(&supervisor, &repository, id, WorkflowStatus::Succeeded).await;

    let activity = repository.activity(id).expect("read activity");
    assert_eq!(activity.len(), 3);
    assert_eq!(activity[0].message, "completed step 1");
    assert_eq!(activity[1].message, "completed step 2");
    assert_eq!(activity[2].message, "completed step 3");
    assert!(repository.claims(id).expect("terminal claims").is_empty());
    drop(supervisor);
    drop(repository);

    for _ in 0..3 {
        let repository = WorkflowRepository::open(&path).expect("reopen terminal history");
        assert_eq!(repository.list().unwrap().len(), 1);
        assert_eq!(repository.activity(id).unwrap().len(), 3);
    }
    std::fs::remove_file(path).expect("remove test database");
}

struct MutationHarness {
    submitted: AtomicUsize,
    evidence: std::sync::atomic::AtomicBool,
    submitted_before_checkpoint: Semaphore,
    checkpoint_allowed: Semaphore,
}

impl Default for MutationHarness {
    fn default() -> Self {
        Self {
            submitted: AtomicUsize::new(0),
            evidence: std::sync::atomic::AtomicBool::new(false),
            submitted_before_checkpoint: Semaphore::new(0),
            checkpoint_allowed: Semaphore::new(0),
        }
    }
}

struct MutationFactory {
    kind: WorkflowKind,
    harness: Arc<MutationHarness>,
}

impl WorkflowFactory for MutationFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    fn current_schema_version(&self) -> u32 {
        1
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(MutationWorkflow(self.harness.clone())))
    }
}

struct MutationWorkflow(Arc<MutationHarness>);

impl WorkflowExecutor for MutationWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            if !self.0.evidence.swap(true, Ordering::SeqCst) {
                self.0.submitted.fetch_add(1, Ordering::SeqCst);
                self.0.submitted_before_checkpoint.add_permits(1);
                self.0
                    .checkpoint_allowed
                    .acquire()
                    .await
                    .expect("test harness remains open")
                    .forget();
            }
            context
                .persist_checkpoint(&serde_json::json!({ "evidence_reconciled": true }))
                .map_err(|error| error.to_string())?;
            context
                .mark_succeeded(Some(true))
                .map_err(|error| error.to_string())
        })
    }
}

#[tokio::test]
async fn restart_after_mutation_submission_reconciles_evidence_without_resubmitting() {
    let path = std::env::temp_dir().join(format!(
        "replicant-workflow-mutation-gap-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let repository = Arc::new(WorkflowRepository::open(&path).expect("open repository"));
    let kind = WorkflowKind::new("test.mutation-gap").unwrap();
    let workflow = repository
        .create(NewWorkflow {
            kind: kind.clone(),
            schema_version: 1,
            config: (),
            checkpoint: serde_json::json!({ "evidence_reconciled": false }),
            current_step: Some("submit".into()),
            parent_id: None,
        })
        .unwrap();
    let harness = Arc::new(MutationHarness::default());
    let mut registry = WorkflowRegistry::new();
    registry
        .register(Arc::new(MutationFactory {
            kind,
            harness: harness.clone(),
        }))
        .unwrap();
    let registry = Arc::new(registry);
    let supervisor = WorkflowSupervisor::new(repository.clone(), registry.clone());
    supervisor.tick().await.unwrap();
    harness
        .submitted_before_checkpoint
        .acquire()
        .await
        .unwrap()
        .forget();
    drop(supervisor);
    drop(repository);
    tokio::task::yield_now().await;

    let repository = Arc::new(WorkflowRepository::open(&path).expect("reopen repository"));
    let supervisor = WorkflowSupervisor::new(repository.clone(), registry);
    wait_for_status(
        &supervisor,
        &repository,
        workflow.id,
        WorkflowStatus::Succeeded,
    )
    .await;
    assert_eq!(harness.submitted.load(Ordering::SeqCst), 1);
    assert_eq!(
        repository
            .read(workflow.id)
            .unwrap()
            .unwrap()
            .checkpoint::<serde_json::Value>()
            .unwrap()["evidence_reconciled"],
        true
    );
    drop(supervisor);
    drop(repository);
    std::fs::remove_file(path).expect("remove test database");
}

#[tokio::test]
async fn records_executor_panics_as_failures() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let (id, harness, _, supervisor) = setup(repository.clone(), true);
    supervisor.tick().await.expect("start workflow");
    reach_step(&harness).await;
    harness.proceed.add_permits(1);
    wait_for_status(&supervisor, &repository, id, WorkflowStatus::Failed).await;
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

struct PollingWaitingFactory {
    kind: WorkflowKind,
    executions: Arc<AtomicUsize>,
}

impl WorkflowFactory for PollingWaitingFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    fn current_schema_version(&self) -> u32 {
        1
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(PollingWaitingWorkflow {
            executions: self.executions.clone(),
        }))
    }
}

struct PollingWaitingWorkflow {
    executions: Arc<AtomicUsize>,
}

impl WorkflowExecutor for PollingWaitingWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            self.executions.fetch_add(1, Ordering::SeqCst);
            context.mark_waiting().map_err(|error| error.to_string())
        })
    }
}

#[tokio::test]
async fn polling_wait_is_not_restarted_on_every_supervisor_tick() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let kind = WorkflowKind::new("test.polling-wait").expect("valid kind");
    let workflow = repository
        .create(NewWorkflow {
            kind: kind.clone(),
            schema_version: 1,
            config: (),
            checkpoint: (),
            current_step: Some("waiting".into()),
            parent_id: None,
        })
        .expect("create workflow");
    let executions = Arc::new(AtomicUsize::new(0));
    let mut registry = WorkflowRegistry::new();
    registry
        .register(Arc::new(PollingWaitingFactory {
            kind,
            executions: executions.clone(),
        }))
        .expect("register workflow");
    let supervisor = WorkflowSupervisor::new(repository.clone(), Arc::new(registry));

    wait_for_status(
        &supervisor,
        &repository,
        workflow.id,
        WorkflowStatus::Waiting,
    )
    .await;
    for _ in 0..5 {
        supervisor
            .tick()
            .await
            .expect("reap or retry waiting workflow");
        tokio::task::yield_now().await;
    }

    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert_eq!(
        repository
            .read(workflow.id)
            .expect("read workflow")
            .unwrap()
            .status,
        WorkflowStatus::Waiting
    );
}

struct WaitingFactory {
    kind: WorkflowKind,
    satisfied: Arc<std::sync::atomic::AtomicBool>,
    deadline_millis: i64,
    satisfy_on_poll: bool,
}

impl WorkflowFactory for WaitingFactory {
    fn kind(&self) -> &WorkflowKind {
        &self.kind
    }

    fn current_schema_version(&self) -> u32 {
        1
    }

    fn create_executor(&self) -> Option<Box<dyn WorkflowExecutor>> {
        Some(Box::new(WaitingWorkflow {
            satisfied: self.satisfied.clone(),
            deadline_millis: self.deadline_millis,
            satisfy_on_poll: self.satisfy_on_poll,
        }))
    }
}

struct WaitingWorkflow {
    satisfied: Arc<std::sync::atomic::AtomicBool>,
    deadline_millis: i64,
    satisfy_on_poll: bool,
}

impl WorkflowExecutor for WaitingWorkflow {
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a> {
        Box::pin(async move {
            let poll_interval = if self.satisfy_on_poll {
                std::time::Duration::ZERO
            } else {
                std::time::Duration::from_millis(10)
            };
            let outcome = context
                .wait_until(
                    WaitIntent::state("test state is ready")
                        .for_event("test.changed")
                        .polling_every(poll_interval)
                        .until(self.deadline_millis),
                    |_, signal| {
                        std::future::ready(Ok(self.satisfied.load(Ordering::SeqCst)
                            || self.satisfy_on_poll && signal == WaitSignal::Poll))
                    },
                )
                .await
                .map_err(|error| error.to_string())?;
            if matches!(outcome, WaitOutcome::Satisfied | WaitOutcome::Deadline) {
                context
                    .mark_succeeded(Some(outcome == WaitOutcome::Satisfied))
                    .map_err(|error| error.to_string())?;
            }
            Ok(())
        })
    }
}

async fn managed_client() -> replicant_client::managed::Client {
    replicant_client::managed::Client::builder()
        .authentication_token(replicant_client::raw::SecretString::from("test".to_owned()))
        .in_memory()
        .startup_policy(replicant_client::managed::StartupPolicy::RestoreOnly)
        .start()
        .await
        .expect("start managed client")
}

fn waiting_setup(
    repository: Arc<WorkflowRepository>,
    client: replicant_client::managed::Client,
    satisfied: Arc<std::sync::atomic::AtomicBool>,
    deadline_millis: i64,
) -> (WorkflowId, Arc<WorkflowRegistry>, WorkflowSupervisor) {
    let kind = WorkflowKind::new("test.wait").expect("valid kind");
    let id = repository
        .create(NewWorkflow {
            kind: kind.clone(),
            schema_version: 1,
            config: (),
            checkpoint: (),
            current_step: Some("wait".into()),
            parent_id: None,
        })
        .expect("create workflow")
        .id;
    let mut registry = WorkflowRegistry::new();
    registry
        .register(Arc::new(WaitingFactory {
            kind,
            satisfied,
            deadline_millis,
            satisfy_on_poll: false,
        }))
        .expect("register workflow");
    let registry = Arc::new(registry);
    let supervisor = WorkflowSupervisor::with_managed_client(repository, registry.clone(), client);
    (id, registry, supervisor)
}

#[tokio::test]
async fn wait_uses_authoritative_poll_fallback() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let kind = WorkflowKind::new("test.poll-wait").expect("valid kind");
    let id = repository
        .create(NewWorkflow {
            kind: kind.clone(),
            schema_version: 1,
            config: (),
            checkpoint: (),
            current_step: Some("wait".into()),
            parent_id: None,
        })
        .expect("create workflow")
        .id;
    let mut registry = WorkflowRegistry::new();
    registry
        .register(Arc::new(WaitingFactory {
            kind,
            satisfied: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            deadline_millis: unix_millis() + 60_000,
            satisfy_on_poll: true,
        }))
        .expect("register workflow");
    let supervisor = WorkflowSupervisor::with_managed_client(
        repository.clone(),
        Arc::new(registry),
        managed_client().await,
    );

    supervisor.tick().await.expect("start workflow");
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    wait_for_status(&supervisor, &repository, id, WorkflowStatus::Succeeded).await;
}

fn unix_millis() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("current time")
            .as_millis(),
    )
    .expect("timestamp fits")
}

#[tokio::test]
async fn wait_persists_intent_and_wakes_on_pause() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let (id, _, supervisor) = waiting_setup(
        repository.clone(),
        managed_client().await,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
        unix_millis() + 60_000,
    );
    supervisor.tick().await.expect("start workflow");
    wait_for_status(&supervisor, &repository, id, WorkflowStatus::Waiting).await;
    assert_eq!(
        repository
            .read(id)
            .expect("read workflow")
            .unwrap()
            .wait_intent()
            .expect("decode wait")
            .unwrap()
            .event_name
            .as_deref(),
        Some("test.changed")
    );

    supervisor.pause(id).expect("pause wait");
    wait_for_status(&supervisor, &repository, id, WorkflowStatus::Paused).await;
    while supervisor.has_executor(id) {
        tokio::task::yield_now().await;
        supervisor.tick().await.expect("reap paused wait");
    }
}

#[tokio::test]
async fn wait_wakes_on_cancel() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let (id, _, supervisor) = waiting_setup(
        repository.clone(),
        managed_client().await,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
        unix_millis() + 60_000,
    );
    supervisor.tick().await.expect("start workflow");
    wait_for_status(&supervisor, &repository, id, WorkflowStatus::Waiting).await;
    supervisor.cancel(id).expect("cancel wait");
    while supervisor.has_executor(id) {
        tokio::task::yield_now().await;
        supervisor.tick().await.expect("reap cancelled wait");
    }
    assert_eq!(
        repository.read(id).expect("read workflow").unwrap().status,
        WorkflowStatus::Cancelled
    );
}

#[tokio::test]
async fn restart_reconciles_wait_and_rechecks_managed_state() {
    let path = std::env::temp_dir().join(format!(
        "replicant-workflow-wait-{}.sqlite",
        uuid::Uuid::new_v4()
    ));
    let mut repository = Arc::new(WorkflowRepository::open(&path).expect("open repository"));
    let satisfied = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let client = managed_client().await;
    let (id, registry, mut supervisor) = waiting_setup(
        repository.clone(),
        client.clone(),
        satisfied.clone(),
        unix_millis() + 60_000,
    );
    for _ in 0..3 {
        supervisor.tick().await.expect("start workflow");
        wait_for_status(&supervisor, &repository, id, WorkflowStatus::Waiting).await;
        drop(supervisor);
        drop(repository);
        tokio::task::yield_now().await;
        repository = Arc::new(WorkflowRepository::open(&path).expect("reopen repository"));
        supervisor = WorkflowSupervisor::with_managed_client(
            repository.clone(),
            registry.clone(),
            client.clone(),
        );
    }

    satisfied.store(true, Ordering::SeqCst);
    wait_for_status(&supervisor, &repository, id, WorkflowStatus::Succeeded).await;
    assert_eq!(
        repository
            .read(id)
            .expect("read workflow")
            .unwrap()
            .result::<bool>()
            .expect("decode result"),
        Some(true)
    );
    drop(supervisor);
    drop(repository);
    std::fs::remove_file(path).expect("remove test database");
}

#[tokio::test]
async fn wait_wakes_at_persisted_deadline() {
    let repository = Arc::new(WorkflowRepository::open_in_memory().expect("open repository"));
    let (id, _, supervisor) = waiting_setup(
        repository.clone(),
        managed_client().await,
        Arc::new(std::sync::atomic::AtomicBool::new(false)),
        unix_millis(),
    );
    wait_for_status(&supervisor, &repository, id, WorkflowStatus::Succeeded).await;
    assert_eq!(
        repository
            .read(id)
            .expect("read workflow")
            .unwrap()
            .result::<bool>()
            .expect("decode result"),
        Some(false)
    );
}
