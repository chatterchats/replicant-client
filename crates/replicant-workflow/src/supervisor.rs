use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use replicant_client::managed::Client;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    ClaimAcquireOutcome, NewWorkflow, RepositoryError, ResourceClaim, ResourceKey, WaitIntent,
    WaitOutcome, WaitSignal, WorkflowFailureDisposition, WorkflowId, WorkflowInstance,
    WorkflowRegistry, WorkflowRepository, WorkflowState, WorkflowStatus, WorkflowTelemetrySample,
    WorkflowTelemetrySink,
};

// Workflows that explicitly return `Waiting` without a durable `WaitIntent` are
// polling/reconciliation waits. Throttle them so the supervisor cannot hot-loop
// a finished executor several times per second while its prerequisite is unchanged.
const POLLING_WAIT_RETRY_INTERVAL: Duration = Duration::from_secs(5);
// Survey tours already perform authoritative survey-state checks on a much
// slower cadence. Re-entering the whole durable executor every five seconds
// while a tour is merely waiting adds churn without improving responsiveness.
const SCAN_TOUR_POLLING_WAIT_RETRY_INTERVAL: Duration = Duration::from_secs(30);
// Event workflows can remain blocked on manufacturing, staging, or relay
// expansion for many minutes. The prerequisite workflow itself reacts to
// managed evidence; parents only need an occasional reconciliation pass to
// observe completion.
const EVENT_DEPENDENCY_POLLING_WAIT_RETRY_INTERVAL: Duration = Duration::from_secs(60);
// Frontier expansion can be blocked for minutes or hours on a blueprint,
// prerequisite manufacturing, or temporary upstream availability. Re-running
// the full relay planner every five seconds only burns API budget while the
// prerequisite is unchanged.
const EXPLORATION_PREREQUISITE_POLLING_WAIT_RETRY_INTERVAL: Duration = Duration::from_secs(300);

/// Boxed future returned by a workflow executor.
pub type BoxWorkflowFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// Reconstructible workflow execution contract.
pub trait WorkflowExecutor: Send {
    /// Runs from the checkpoint loaded through `context`.
    fn execute<'a>(&'a mut self, context: &'a mut WorkflowContext) -> BoxWorkflowFuture<'a>;
}

/// Durable control request observed by an executor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlRequest {
    /// Continue executing.
    Continue,
    /// Stop after the current safe boundary and await resume.
    Pause,
    /// Stop permanently after the current safe boundary.
    Cancel,
}

/// Persisted context for one executor invocation.
pub struct WorkflowContext {
    repository: Arc<WorkflowRepository>,
    instance: WorkflowInstance,
    client: Option<Client>,
    control: watch::Receiver<ControlRequest>,
    telemetry: Option<Arc<dyn WorkflowTelemetrySink>>,
}

impl WorkflowContext {
    fn new(
        repository: Arc<WorkflowRepository>,
        instance: WorkflowInstance,
        client: Option<Client>,
        control: watch::Receiver<ControlRequest>,
        telemetry: Option<Arc<dyn WorkflowTelemetrySink>>,
    ) -> Self {
        Self {
            repository,
            instance,
            client,
            control,
            telemetry,
        }
    }

    /// Returns the workflow identifier.
    #[must_use]
    pub fn id(&self) -> WorkflowId {
        self.instance.id
    }

    /// Loads typed configuration from the authoritative row.
    pub fn config<C: DeserializeOwned>(&self) -> Result<C, RepositoryError> {
        self.instance.config()
    }

    /// Loads the current typed checkpoint.
    pub fn checkpoint<P: DeserializeOwned>(&self) -> Result<P, RepositoryError> {
        self.instance.checkpoint()
    }

    /// Returns the daemon-owned managed client for gameplay reconciliation.
    #[must_use]
    pub fn managed_client(&self) -> Option<&Client> {
        self.client.as_ref()
    }

    /// Returns the durable repository for composing child workflows.
    #[must_use]
    pub fn repository(&self) -> &WorkflowRepository {
        &self.repository
    }

    /// Clones the authoritative repository handle for concurrent item workers.
    #[must_use]
    pub fn repository_handle(&self) -> Arc<WorkflowRepository> {
        self.repository.clone()
    }

    /// Persists a child workflow owned by this orchestration.
    ///
    /// The parent link is always derived from the current workflow so callers
    /// cannot accidentally create an unparented task while composing durable
    /// child work.
    pub fn create_child<C: Serialize, P: Serialize>(
        &self,
        mut workflow: NewWorkflow<C, P>,
    ) -> Result<WorkflowInstance, RepositoryError> {
        workflow.parent_id = Some(self.instance.id);
        self.repository.create(workflow)
    }

    /// Lists durable child workflows owned by this orchestration.
    ///
    /// This lets restart reconciliation reattach to child work that was
    /// created immediately before a parent checkpoint write was interrupted.
    pub fn child_workflows(&self) -> Result<Vec<WorkflowInstance>, RepositoryError> {
        self.repository.list_children(self.instance.id)
    }

    /// Atomically advances to a named logical step and persists its checkpoint.
    pub fn advance_to<P: Serialize>(
        &mut self,
        step: impl Into<String>,
        checkpoint: &P,
    ) -> Result<(), RepositoryError> {
        self.replace(
            WorkflowStatus::Running,
            Some(step.into()),
            serde_json::to_value(checkpoint)?,
            None,
        )
    }

    /// Persists a typed checkpoint atomically.
    pub fn persist_checkpoint<P: Serialize>(
        &mut self,
        checkpoint: &P,
    ) -> Result<(), RepositoryError> {
        self.replace(
            WorkflowStatus::Running,
            self.instance.current_step.clone(),
            serde_json::to_value(checkpoint)?,
            None,
        )
    }

    /// Appends durable, non-secret activity.
    pub fn emit_activity(&self, message: impl Into<String>) -> Result<(), RepositoryError> {
        self.repository
            .append_activity(self.instance.id, message)
            .map(|_| ())
    }

    /// Atomically acquires an exclusive gameplay resource for this workflow.
    pub fn acquire_claim(
        &self,
        resource: ResourceKey,
    ) -> Result<ClaimAcquireOutcome, RepositoryError> {
        let resource_kind = resource_kind(&resource).to_owned();
        match self.repository.acquire_claim(self.instance.id, resource) {
            Ok(outcome) => Ok(outcome),
            Err(error @ RepositoryError::ClaimConflict { .. }) => {
                self.record_telemetry("claim_conflict", "conflict", Some(resource_kind), None);
                Err(error)
            }
            Err(error) => Err(error),
        }
    }

    /// Releases one claim only if this workflow owns it.
    pub fn release_claim(&self, resource: &ResourceKey) -> Result<bool, RepositoryError> {
        self.repository.release_claim(self.instance.id, resource)
    }

    /// Lists this workflow's persisted resource claims.
    pub fn claims(&self) -> Result<Vec<ResourceClaim>, RepositoryError> {
        self.repository.claims(self.instance.id)
    }

    /// Explicitly releases all claims at a workflow-defined safe boundary.
    pub fn release_all_claims(&self) -> Result<usize, RepositoryError> {
        self.repository.release_claims(self.instance.id)
    }

    /// Marks this invocation as durably waiting.
    pub fn mark_waiting(&mut self) -> Result<(), RepositoryError> {
        self.replace(
            WorkflowStatus::Waiting,
            self.instance.current_step.clone(),
            self.checkpoint_value()?,
            None,
        )
    }

    /// Waits for SSE-backed event or managed-state revision signals, then
    /// verifies `predicate` against managed durable state.
    pub async fn wait_until<F, Fut>(
        &mut self,
        intent: WaitIntent,
        mut predicate: F,
    ) -> Result<WaitOutcome, WorkflowWaitError>
    where
        F: FnMut(&Client, WaitSignal) -> Fut,
        Fut: Future<Output = Result<bool, String>>,
    {
        let client = self
            .client
            .clone()
            .ok_or(WorkflowWaitError::NoManagedClient)?;
        let mut intent = self.instance.wait_intent()?.unwrap_or(intent);
        if intent.cursor.is_none() {
            intent.cursor = client.events().cursor()?;
        }
        self.persist_wait(&intent)?;
        let mut events = client.events().watch().await?;
        let mut revisions = client.state().watch()?;
        let poll_interval = Duration::from_millis(intent.poll_interval_millis.unwrap_or(30_000));
        let mut poll_deadline = tokio::time::Instant::now() + poll_interval;

        if predicate(&client, WaitSignal::Initial)
            .await
            .map_err(WorkflowWaitError::Predicate)?
            || self.recover_history(&client, &mut intent).await?
                && predicate(&client, WaitSignal::History)
                    .await
                    .map_err(WorkflowWaitError::Predicate)?
        {
            self.clear_wait()?;
            return Ok(WaitOutcome::Satisfied);
        }

        loop {
            let mut signal = None;
            let deadline = deadline_delay(intent.deadline_millis)?;
            tokio::select! {
                control = self.control.changed() => {
                    if control.is_err() {
                        return Err(WorkflowWaitError::ControlClosed);
                    }
                    match *self.control.borrow_and_update() {
                        ControlRequest::Continue => {}
                        ControlRequest::Pause => return Ok(WaitOutcome::Paused),
                        ControlRequest::Cancel => return Ok(WaitOutcome::Cancelled),
                    }
                }
                _ = tokio::time::sleep(deadline) => {
                    self.clear_wait()?;
                    return Ok(WaitOutcome::Deadline);
                }
                _ = tokio::time::sleep_until(poll_deadline) => {
                    poll_deadline = tokio::time::Instant::now() + poll_interval;
                    signal = Some(WaitSignal::Poll);
                }
                revision = revisions.next() => {
                    revision?;
                    signal = Some(WaitSignal::StateRevision);
                }
                event = events.next() => {
                    match event {
                        Ok(event) => {
                            let relevant = wait_event_matches(&intent, &event);
                            intent.cursor = Some(event.id.to_string());
                            self.persist_wait(&intent)?;
                            if relevant {
                                signal = Some(WaitSignal::Event);
                            }
                        }
                        Err(replicant_client::Error::Transport { message, .. })
                            if message.contains("lagged") => {
                            self.recover_history(&client, &mut intent).await?;
                            signal = Some(WaitSignal::WatcherGap);
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
            }
            if let Some(signal) = signal
                && predicate(&client, signal)
                    .await
                    .map_err(WorkflowWaitError::Predicate)?
            {
                self.clear_wait()?;
                return Ok(WaitOutcome::Satisfied);
            }
        }
    }

    async fn recover_history(
        &mut self,
        client: &Client,
        intent: &mut WaitIntent,
    ) -> Result<bool, WorkflowWaitError> {
        let mut query = client.events().history();
        if let Some(cursor) = &intent.cursor {
            query = query.after(cursor.clone());
        }
        if let Some(name) = &intent.event_name {
            query = query.named(name.clone());
        }
        if let Some(device) = &intent.device_code {
            query = query.for_device(device.clone());
        }
        let events = query.collect().await?;
        if let Some(event) = events.last() {
            intent.cursor = Some(event.id.to_string());
            self.persist_wait(intent)?;
        }
        Ok(events.iter().any(|event| wait_event_matches(intent, event)))
    }

    fn persist_wait(&mut self, intent: &WaitIntent) -> Result<(), RepositoryError> {
        self.instance = self.repository.update_with_wait(
            self.instance.id,
            self.instance.revision,
            WorkflowState {
                status: WorkflowStatus::Waiting,
                current_step: self.instance.current_step.clone(),
                checkpoint: self.checkpoint_value()?,
                last_error: None,
                result: self.result_value()?,
            },
            Some(intent),
        )?;
        Ok(())
    }

    fn clear_wait(&mut self) -> Result<(), RepositoryError> {
        self.replace(
            WorkflowStatus::Running,
            self.instance.current_step.clone(),
            self.checkpoint_value()?,
            self.result_value()?,
        )
    }

    /// Marks this invocation as successfully completed and releases its claims.
    pub fn mark_succeeded<R: Serialize>(
        &mut self,
        result: Option<R>,
    ) -> Result<(), RepositoryError> {
        self.replace(
            WorkflowStatus::Succeeded,
            self.instance.current_step.clone(),
            self.checkpoint_value()?,
            result.map(serde_json::to_value).transpose()?,
        )?;
        self.release_all_claims()?;
        Ok(())
    }

    /// Marks this invocation as failed, retaining its checkpoint and releasing its claims.
    pub fn mark_failed(&mut self, error: impl Into<String>) -> Result<(), RepositoryError> {
        let result = self.result_value()?;
        self.mark_failed_with_result(error, result, WorkflowFailureDisposition::Retryable)
    }

    /// Marks this invocation as failed with a structured terminal result.
    pub fn mark_failed_with_result<R: Serialize>(
        &mut self,
        error: impl Into<String>,
        result: R,
        disposition: WorkflowFailureDisposition,
    ) -> Result<(), RepositoryError> {
        let result = serde_json::to_value(result)?;
        let result = (!result.is_null()).then_some(result);
        let checkpoint = self.checkpoint_value()?;
        self.instance = self.repository.update_with_failure_disposition(
            self.instance.id,
            self.instance.revision,
            WorkflowState {
                status: WorkflowStatus::Failed,
                current_step: self.instance.current_step.clone(),
                checkpoint,
                last_error: Some(error.into()),
                result,
            },
            disposition,
        )?;
        self.release_all_claims()?;
        Ok(())
    }

    /// Marks this invocation as permanently failed and releases its claims.
    ///
    /// Directors must not launch equivalent work until its identity changes.
    pub fn mark_failed_permanently(
        &mut self,
        error: impl Into<String>,
    ) -> Result<(), RepositoryError> {
        let result = self.result_value()?;
        self.mark_failed_with_result(error, result, WorkflowFailureDisposition::Permanent)
    }

    /// Refreshes state and reports a cooperative pause or cancel request.
    pub fn control_request(&mut self) -> Result<ControlRequest, RepositoryError> {
        self.instance = self
            .repository
            .read(self.instance.id)?
            .ok_or(RepositoryError::NotFound(self.instance.id))?;
        Ok(match self.instance.status {
            WorkflowStatus::Paused => ControlRequest::Pause,
            WorkflowStatus::Cancelled => ControlRequest::Cancel,
            _ => ControlRequest::Continue,
        })
    }

    fn record_telemetry(
        &self,
        metric: &'static str,
        outcome: impl Into<String>,
        detail: Option<String>,
        duration_ms: Option<u64>,
    ) {
        let Some(sink) = self.telemetry.as_ref() else {
            return;
        };
        sink.record(WorkflowTelemetrySample {
            observed_at_ms: now_millis(),
            workflow_id: self.instance.id.to_string(),
            workflow_kind: self.instance.kind.as_str().to_owned(),
            metric,
            outcome: outcome.into(),
            detail,
            duration_ms,
        });
    }

    fn checkpoint_value(&self) -> Result<Value, RepositoryError> {
        self.instance.checkpoint()
    }

    fn result_value(&self) -> Result<Option<Value>, RepositoryError> {
        self.instance.result()
    }

    fn replace(
        &mut self,
        status: WorkflowStatus,
        current_step: Option<String>,
        checkpoint: Value,
        result: Option<Value>,
    ) -> Result<(), RepositoryError> {
        self.instance = self.repository.update(
            self.instance.id,
            self.instance.revision,
            WorkflowState {
                status,
                current_step,
                checkpoint,
                last_error: None,
                result,
            },
        )?;
        Ok(())
    }
}

/// Workflow supervision failures.
#[derive(Debug, thiserror::Error)]
pub enum SupervisorError {
    /// Workflow persistence failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}

/// Failures while awaiting managed state.
#[derive(Debug, thiserror::Error)]
pub enum WorkflowWaitError {
    /// Workflow persistence failed.
    #[error(transparent)]
    Repository(#[from] RepositoryError),
    /// Managed client state or event observation failed.
    #[error(transparent)]
    Managed(#[from] replicant_client::Error),
    /// The workflow supervisor was created without a managed client.
    #[error("workflow wait requires a managed client")]
    NoManagedClient,
    /// The state predicate could not be evaluated.
    #[error("managed state predicate failed: {0}")]
    Predicate(String),
    /// The supervisor control channel closed unexpectedly.
    #[error("workflow control channel closed")]
    ControlClosed,
    /// The system clock cannot produce a Unix timestamp.
    #[error("system clock is before the Unix epoch")]
    Clock,
}

/// Owns at most one in-process executor for each persisted workflow.
pub struct WorkflowSupervisor {
    repository: Arc<WorkflowRepository>,
    registry: Arc<WorkflowRegistry>,
    executors: Mutex<Executors>,
    tick_lock: tokio::sync::Mutex<()>,
    client: Option<Client>,
    telemetry: Option<Arc<dyn WorkflowTelemetrySink>>,
    startup_reconciled: AtomicBool,
}

#[derive(Default)]
struct Executors {
    tasks: HashMap<WorkflowId, JoinHandle<()>>,
    controls: HashMap<WorkflowId, watch::Sender<ControlRequest>>,
}

impl WorkflowSupervisor {
    /// Creates a supervisor. Call [`Self::tick`] to reconcile and start work.
    #[must_use]
    pub fn new(repository: Arc<WorkflowRepository>, registry: Arc<WorkflowRegistry>) -> Self {
        Self {
            repository,
            registry,
            executors: Mutex::new(Executors::default()),
            tick_lock: tokio::sync::Mutex::new(()),
            client: None,
            telemetry: None,
            startup_reconciled: AtomicBool::new(false),
        }
    }

    /// Creates a supervisor integrated with the daemon's one managed client.
    #[must_use]
    pub fn with_managed_client(
        repository: Arc<WorkflowRepository>,
        registry: Arc<WorkflowRegistry>,
        client: Client,
    ) -> Self {
        let mut supervisor = Self::new(repository, registry);
        supervisor.client = Some(client);
        supervisor
    }

    /// Installs a best-effort telemetry sink for executor lifecycle observations.
    #[must_use]
    pub fn with_telemetry_sink(mut self, sink: Arc<dyn WorkflowTelemetrySink>) -> Self {
        self.telemetry = Some(sink);
        self
    }

    /// Reconciles stale durable state once, reaps tasks, and starts runnable instances.
    pub async fn tick(&self) -> Result<(), SupervisorError> {
        let _tick = self.tick_lock.lock().await;
        let instances = if !self.startup_reconciled.load(Ordering::Acquire) {
            let reclaimed_items = self
                .repository
                .reconcile_orphaned_work_items(None, now_millis())?;
            let released_claims = self.repository.reconcile_claims()?;
            let instances = self.repository.list()?;
            let resumable = instances
                .iter()
                .filter(|workflow| {
                    matches!(
                        workflow.status,
                        WorkflowStatus::Queued
                            | WorkflowStatus::Running
                            | WorkflowStatus::Waiting
                            | WorkflowStatus::Reconciling
                    )
                })
                .count();
            let paused = instances
                .iter()
                .filter(|workflow| workflow.status == WorkflowStatus::Paused)
                .count();
            let terminal = instances
                .iter()
                .filter(|workflow| workflow.status.is_terminal())
                .count();
            tracing::info!(
                workflows = instances.len(),
                resumable,
                paused,
                terminal,
                reclaimed_items,
                released_claims,
                "workflow startup reconciliation complete"
            );
            self.startup_reconciled.store(true, Ordering::Release);
            Some(instances)
        } else {
            None
        };
        self.reap_finished().await?;
        if self.repository.automation_policy()?.workflows_paused {
            return Ok(());
        }
        for instance in match instances {
            Some(instances) => instances,
            None => self.repository.list_active()?,
        } {
            if self.executors().controls.contains_key(&instance.id) {
                continue;
            }
            if instance.status == WorkflowStatus::Waiting
                && instance.wait_intent()?.is_none()
                && !polling_wait_retry_due(&instance)
            {
                continue;
            }
            let instance = if matches!(
                instance.status,
                WorkflowStatus::Running | WorkflowStatus::Waiting
            ) {
                self.transition(instance, WorkflowStatus::Reconciling)?
            } else {
                instance
            };
            if matches!(
                instance.status,
                WorkflowStatus::Queued | WorkflowStatus::Reconciling
            ) {
                self.start(instance)?;
            }
        }
        Ok(())
    }

    /// Durably pauses a workflow and immediately stops its in-process executor.
    ///
    /// The executor future is dropped after the paused state is persisted. Any
    /// already-submitted managed operation remains durable, but the workflow
    /// cannot issue another command until it is resumed.
    pub fn pause(&self, id: WorkflowId) -> Result<(), SupervisorError> {
        self.pause_instance(self.read(id)?)
    }

    fn pause_instance(&self, instance: WorkflowInstance) -> Result<(), SupervisorError> {
        let id = instance.id;
        tracing::info!(
            workflow_id = %id,
            kind = %instance.kind,
            "workflow pause requested"
        );
        self.transition(instance, WorkflowStatus::Paused)?;
        let executors = self.executors();
        if let Some(control) = executors.controls.get(&id) {
            control.send_replace(ControlRequest::Pause);
        }
        if let Some(task) = executors.tasks.get(&id) {
            task.abort();
        }
        Ok(())
    }

    /// Durably resumes a paused workflow through reconciliation.
    pub fn resume(&self, id: WorkflowId) -> Result<(), SupervisorError> {
        self.resume_instance(self.read(id)?)
    }

    fn resume_instance(&self, instance: WorkflowInstance) -> Result<(), SupervisorError> {
        let id = instance.id;
        tracing::info!(
            workflow_id = %id,
            kind = %instance.kind,
            "workflow resume requested"
        );
        self.repository
            .reconcile_orphaned_work_items(Some(id), now_millis())?;
        self.transition(instance, WorkflowStatus::Reconciling)?;
        Ok(())
    }

    /// Durably requests cooperative cancellation.
    ///
    /// Claims remain held until a running executor reaches its safe boundary;
    /// a workflow without an executor releases them immediately.
    pub fn cancel(&self, id: WorkflowId) -> Result<(), SupervisorError> {
        self.cancel_instance(self.read(id)?)
    }

    fn cancel_instance(&self, instance: WorkflowInstance) -> Result<(), SupervisorError> {
        let id = instance.id;
        tracing::info!(
            workflow_id = %id,
            kind = %instance.kind,
            "workflow cancellation requested"
        );
        self.transition(instance, WorkflowStatus::Cancelled)?;
        let executors = self.executors();
        if let Some(control) = executors.controls.get(&id) {
            control.send_replace(ControlRequest::Cancel);
        }
        if !executors.controls.contains_key(&id) {
            drop(executors);
            self.repository.release_claims(id)?;
        }
        Ok(())
    }

    /// Durably pauses every eligible workflow and requests cooperative stops.
    pub fn pause_all(&self) -> Result<usize, SupervisorError> {
        let mut paused = 0;
        for instance in self.repository.list_active()? {
            if instance.status.can_transition_to(WorkflowStatus::Paused)
                && instance.status != WorkflowStatus::Paused
            {
                self.pause_instance(instance)?;
                paused += 1;
            }
        }
        Ok(paused)
    }

    /// Durably resumes every paused workflow through reconciliation.
    pub fn resume_all(&self) -> Result<usize, SupervisorError> {
        let mut resumed = 0;
        for instance in self.repository.list_active()? {
            if instance.status == WorkflowStatus::Paused {
                self.resume_instance(instance)?;
                resumed += 1;
            }
        }
        Ok(resumed)
    }

    /// Durably cancels the selected workflows, or every eligible workflow when empty.
    pub fn cancel_selected(&self, ids: &[WorkflowId]) -> Result<usize, SupervisorError> {
        let instances = self.repository.list_active()?;
        let mut cancelled = 0;
        for instance in instances {
            if !instance.status.is_terminal() && (ids.is_empty() || ids.contains(&instance.id)) {
                self.cancel_instance(instance)?;
                cancelled += 1;
            }
        }
        Ok(cancelled)
    }

    /// Returns whether this supervisor currently owns the instance executor.
    #[must_use]
    pub fn has_executor(&self, id: WorkflowId) -> bool {
        self.executors().controls.contains_key(&id)
    }

    fn read(&self, id: WorkflowId) -> Result<WorkflowInstance, RepositoryError> {
        self.repository
            .read(id)?
            .ok_or(RepositoryError::NotFound(id))
    }

    fn transition(
        &self,
        instance: WorkflowInstance,
        status: WorkflowStatus,
    ) -> Result<WorkflowInstance, RepositoryError> {
        let wait_intent = instance.wait_intent()?;
        self.repository.update_with_wait(
            instance.id,
            instance.revision,
            WorkflowState {
                status,
                current_step: instance.current_step.clone(),
                checkpoint: instance.checkpoint::<Value>()?,
                last_error: instance.last_error.clone(),
                result: instance.result::<Value>()?,
            },
            wait_intent.as_ref(),
        )
    }

    fn start(&self, instance: WorkflowInstance) -> Result<(), SupervisorError> {
        let instance = match self.registry.migration(&instance) {
            Ok(Some((target_version, migration))) => {
                let migrated =
                    self.repository
                        .migrate_workflow(&instance, target_version, migration)?;
                tracing::info!(
                    workflow_id = %migrated.id,
                    kind = %migrated.kind,
                    from_version = instance.schema_version,
                    to_version = target_version,
                    "workflow checkpoint migrated"
                );
                migrated
            }
            Ok(None) => instance,
            Err(error) => {
                self.fail_without_executor(instance, error.to_string())?;
                return Ok(());
            }
        };
        let id = instance.id;
        let kind = instance.kind.clone();
        let (control_sender, control) = watch::channel(ControlRequest::Continue);
        self.executors().controls.insert(id, control_sender);
        let instance = match self.transition(instance, WorkflowStatus::Running) {
            Ok(instance) => instance,
            Err(error) => {
                self.clear_start(id)?;
                return Err(error.into());
            }
        };
        let mut executor = match self.registry.resolve(&instance) {
            Ok(factory) => match factory.create_executor() {
                Some(executor) => executor,
                None => {
                    let result = WorkflowContext::new(
                        self.repository.clone(),
                        instance,
                        self.client.clone(),
                        control,
                        self.telemetry.clone(),
                    )
                    .mark_failed("workflow kind has no executor");
                    self.clear_start(id)?;
                    result?;
                    return Ok(());
                }
            },
            Err(error) => {
                let result = WorkflowContext::new(
                    self.repository.clone(),
                    instance,
                    self.client.clone(),
                    control,
                    self.telemetry.clone(),
                )
                .mark_failed(error.to_string());
                self.clear_start(id)?;
                result?;
                return Ok(());
            }
        };
        // A control request can race with factory construction. Do not let an
        // executor start after its durable row was paused or cancelled.
        if *control.borrow() != ControlRequest::Continue {
            self.clear_start(id)?;
            return Ok(());
        }
        tracing::info!(workflow_id = %id, kind = %kind, "workflow executor starting");
        if let Some(sink) = self.telemetry.as_ref() {
            sink.record(WorkflowTelemetrySample {
                observed_at_ms: now_millis(),
                workflow_id: id.to_string(),
                workflow_kind: kind.as_str().to_owned(),
                metric: "executor_started",
                outcome: "started".to_owned(),
                detail: None,
                duration_ms: None,
            });
        }
        let repository = self.repository.clone();
        let client = self.client.clone();
        let telemetry = self.telemetry.clone();
        let task = tokio::spawn(async move {
            let executor_started = std::time::Instant::now();
            let mut context =
                WorkflowContext::new(repository, instance, client, control, telemetry.clone());
            if let Err(error) = executor.execute(&mut context).await {
                tracing::error!(
                    workflow_id = %id,
                    kind = %kind,
                    error = %error,
                    "workflow executor failed"
                );
                if let Err(record_error) = context.mark_failed(error) {
                    tracing::error!(workflow_id = %id, error = %record_error, "failed to record workflow executor error");
                }
            } else if let Err(error) = fail_if_still_running(&mut context) {
                tracing::error!(
                    workflow_id = %id,
                    kind = %kind,
                    error = %error,
                    "failed to finalize workflow executor"
                );
            } else {
                tracing::info!(workflow_id = %id, kind = %kind, "workflow executor finished");
            }
            if let Some(sink) = telemetry.as_ref() {
                sink.record(WorkflowTelemetrySample {
                    observed_at_ms: now_millis(),
                    workflow_id: id.to_string(),
                    workflow_kind: kind.as_str().to_owned(),
                    metric: "executor_finished",
                    outcome: context.instance.status.as_str().to_owned(),
                    detail: context.instance.current_step.clone(),
                    duration_ms: Some(
                        executor_started
                            .elapsed()
                            .as_millis()
                            .try_into()
                            .unwrap_or(u64::MAX),
                    ),
                });
            }
        });
        self.executors().tasks.insert(id, task);
        Ok(())
    }

    fn fail_without_executor(
        &self,
        instance: WorkflowInstance,
        error: String,
    ) -> Result<(), SupervisorError> {
        tracing::error!(
            workflow_id = %instance.id,
            kind = %instance.kind,
            schema_version = instance.schema_version,
            "workflow startup reconciliation failed"
        );
        let (_, control) = watch::channel(ControlRequest::Continue);
        WorkflowContext::new(
            self.repository.clone(),
            instance,
            self.client.clone(),
            control,
            self.telemetry.clone(),
        )
        .mark_failed(error)?;
        Ok(())
    }

    async fn reap_finished(&self) -> Result<(), SupervisorError> {
        let finished = {
            let mut executors = self.executors();
            let ids = executors
                .tasks
                .iter()
                .filter_map(|(id, task)| task.is_finished().then_some(*id))
                .collect::<Vec<_>>();
            ids.into_iter()
                .map(|id| {
                    executors.controls.remove(&id);
                    (
                        id,
                        executors.tasks.remove(&id).expect("finished task exists"),
                    )
                })
                .collect::<Vec<_>>()
        };
        for (id, task) in finished {
            if let Err(error) = task.await {
                let instance = self.read(id)?;
                if !matches!(
                    instance.status,
                    WorkflowStatus::Paused
                        | WorkflowStatus::Succeeded
                        | WorkflowStatus::Failed
                        | WorkflowStatus::Cancelled
                ) {
                    let (_, control) = watch::channel(ControlRequest::Continue);
                    let mut context = WorkflowContext::new(
                        self.repository.clone(),
                        instance,
                        self.client.clone(),
                        control,
                        self.telemetry.clone(),
                    );
                    context.mark_failed(format!("workflow executor task failed: {error}"))?;
                }
            }
            if self.read(id)?.status == WorkflowStatus::Cancelled {
                self.repository.release_claims(id)?;
            }
        }
        Ok(())
    }

    fn executors(&self) -> MutexGuard<'_, Executors> {
        self.executors
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn clear_start(&self, id: WorkflowId) -> Result<(), RepositoryError> {
        self.executors().controls.remove(&id);
        if self.read(id)?.status == WorkflowStatus::Cancelled {
            self.repository.release_claims(id)?;
        }
        Ok(())
    }
}

fn resource_kind(resource: &ResourceKey) -> &'static str {
    match resource {
        ResourceKey::Replicant(_) => "replicant",
        ResourceKey::Device(_) => "device",
        ResourceKey::Autofactory(_) => "autofactory",
        ResourceKey::Namespaced { .. } => "namespaced",
    }
}

fn wait_event_matches(intent: &WaitIntent, event: &replicant_client::domain::Event) -> bool {
    intent
        .event_name
        .as_deref()
        .is_none_or(|name| event.name.as_str() == name)
        && if let Some(device) = intent.device_code.as_deref() {
            event
                .device
                .as_ref()
                .is_some_and(|event_device| event_device.id.as_str() == device)
        } else {
            intent.device_codes.is_empty()
                || event.device.as_ref().is_some_and(|event_device| {
                    intent
                        .device_codes
                        .iter()
                        .any(|device| event_device.id.as_str() == device)
                })
        }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(i64::MAX)
}

fn polling_wait_retry_due(instance: &WorkflowInstance) -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(u128::MAX, |duration| duration.as_millis());
    let updated_at = u128::try_from(instance.updated_at).unwrap_or_default();
    let interval = if instance.kind.as_str() == "scan.tour" {
        SCAN_TOUR_POLLING_WAIT_RETRY_INTERVAL
    } else if instance.kind.as_str() == "exploration.frontier"
        && instance.current_step.as_deref() == Some("awaiting_relay_prerequisites")
    {
        EXPLORATION_PREREQUISITE_POLLING_WAIT_RETRY_INTERVAL
    } else if matches!(
        (instance.kind.as_str(), instance.current_step.as_deref()),
        (
            "event.campaign" | "event.delivery" | "event.fulfillment",
            Some("awaiting_ftl_connectivity"),
        ) | ("event.tour", Some("awaiting_delivery"))
    ) {
        EVENT_DEPENDENCY_POLLING_WAIT_RETRY_INTERVAL
    } else {
        POLLING_WAIT_RETRY_INTERVAL
    };
    now.saturating_sub(updated_at) >= interval.as_millis()
}

fn deadline_delay(deadline: Option<i64>) -> Result<Duration, WorkflowWaitError> {
    let Some(deadline) = deadline else {
        return Ok(Duration::MAX);
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| WorkflowWaitError::Clock)?
        .as_millis();
    Ok(Duration::from_millis(
        u64::try_from(i128::from(deadline) - i128::try_from(now).unwrap_or(i128::MAX)).unwrap_or(0),
    ))
}

impl Drop for WorkflowSupervisor {
    fn drop(&mut self) {
        let executors = self
            .executors
            .get_mut()
            .unwrap_or_else(|error| error.into_inner());
        for task in executors.tasks.values() {
            task.abort();
        }
    }
}

fn fail_if_still_running(context: &mut WorkflowContext) -> Result<(), RepositoryError> {
    if context.control_request()? == ControlRequest::Continue
        && context.instance.status == WorkflowStatus::Running
    {
        context.mark_failed("workflow executor returned without a terminal or waiting state")?;
    }
    Ok(())
}
