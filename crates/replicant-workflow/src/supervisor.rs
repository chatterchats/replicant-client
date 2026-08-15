use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use replicant_client::managed::Client;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{sync::watch, task::JoinHandle};

use crate::{
    ClaimAcquireOutcome, RepositoryError, ResourceClaim, ResourceKey, WaitIntent, WaitOutcome,
    WorkflowId, WorkflowInstance, WorkflowRegistry, WorkflowRepository, WorkflowState,
    WorkflowStatus,
};

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
}

impl WorkflowContext {
    fn new(
        repository: Arc<WorkflowRepository>,
        instance: WorkflowInstance,
        client: Option<Client>,
        control: watch::Receiver<ControlRequest>,
    ) -> Self {
        Self {
            repository,
            instance,
            client,
            control,
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
        self.repository.acquire_claim(self.instance.id, resource)
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
    pub async fn wait_until<F>(
        &mut self,
        intent: WaitIntent,
        mut predicate: F,
    ) -> Result<WaitOutcome, WorkflowWaitError>
    where
        F: FnMut(&Client) -> Result<bool, String>,
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

        loop {
            if predicate(&client).map_err(WorkflowWaitError::Predicate)? {
                self.clear_wait()?;
                return Ok(WaitOutcome::Satisfied);
            }
            if self.recover_history(&client, &mut intent).await?
                && predicate(&client).map_err(WorkflowWaitError::Predicate)?
            {
                self.clear_wait()?;
                return Ok(WaitOutcome::Satisfied);
            }
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
                revision = revisions.next() => {
                    revision?;
                }
                event = events.next() => {
                    match event {
                        Ok(event) => {
                            intent.cursor = Some(event.id.to_string());
                            self.persist_wait(&intent)?;
                        }
                        Err(replicant_client::Error::Transport { message, .. })
                            if message.contains("lagged") => {
                            // A bounded local watcher can lag. Durable history and
                            // state verification below recover without trusting it.
                            self.recover_history(&client, &mut intent).await?;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
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
        Ok(!events.is_empty())
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
        let error = error.into();
        let checkpoint = self.checkpoint_value()?;
        let result = self.result_value()?;
        self.instance = self.repository.update(
            self.instance.id,
            self.instance.revision,
            WorkflowState {
                status: WorkflowStatus::Failed,
                current_step: self.instance.current_step.clone(),
                checkpoint,
                last_error: Some(error),
                result,
            },
        )?;
        self.release_all_claims()?;
        Ok(())
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
    tasks: HashMap<WorkflowId, JoinHandle<()>>,
    controls: HashMap<WorkflowId, watch::Sender<ControlRequest>>,
    client: Option<Client>,
    claims_reconciled: bool,
}

impl WorkflowSupervisor {
    /// Creates a supervisor. Call [`Self::tick`] to reconcile and start work.
    #[must_use]
    pub fn new(repository: Arc<WorkflowRepository>, registry: Arc<WorkflowRegistry>) -> Self {
        Self {
            repository,
            registry,
            tasks: HashMap::new(),
            controls: HashMap::new(),
            client: None,
            claims_reconciled: false,
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

    /// Reconciles stale claims once, reaps tasks, and starts runnable instances.
    pub async fn tick(&mut self) -> Result<(), SupervisorError> {
        let instances = if !self.claims_reconciled {
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
                released_claims,
                "workflow startup reconciliation complete"
            );
            self.claims_reconciled = true;
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
            None => self.repository.list()?,
        } {
            if self.tasks.contains_key(&instance.id) {
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

    /// Durably requests a cooperative pause while retaining resource claims.
    pub fn pause(&self, id: WorkflowId) -> Result<(), SupervisorError> {
        let instance = self.read(id)?;
        self.transition(instance, WorkflowStatus::Paused)?;
        if let Some(control) = self.controls.get(&id) {
            control.send_replace(ControlRequest::Pause);
        }
        Ok(())
    }

    /// Durably resumes a paused workflow through reconciliation.
    pub fn resume(&self, id: WorkflowId) -> Result<(), SupervisorError> {
        let instance = self.read(id)?;
        self.transition(instance, WorkflowStatus::Reconciling)?;
        Ok(())
    }

    /// Durably requests cooperative cancellation.
    ///
    /// Claims remain held until a running executor reaches its safe boundary;
    /// a workflow without an executor releases them immediately.
    pub fn cancel(&self, id: WorkflowId) -> Result<(), SupervisorError> {
        let instance = self.read(id)?;
        self.transition(instance, WorkflowStatus::Cancelled)?;
        if let Some(control) = self.controls.get(&id) {
            control.send_replace(ControlRequest::Cancel);
        }
        if !self.tasks.contains_key(&id) {
            self.repository.release_claims(id)?;
        }
        Ok(())
    }

    /// Durably pauses every eligible workflow and requests cooperative stops.
    pub fn pause_all(&self) -> Result<usize, SupervisorError> {
        let mut paused = 0;
        for instance in self.repository.list()? {
            if instance.status.can_transition_to(WorkflowStatus::Paused)
                && instance.status != WorkflowStatus::Paused
            {
                self.pause(instance.id)?;
                paused += 1;
            }
        }
        Ok(paused)
    }

    /// Durably resumes every paused workflow through reconciliation.
    pub fn resume_all(&self) -> Result<usize, SupervisorError> {
        let mut resumed = 0;
        for instance in self.repository.list()? {
            if instance.status == WorkflowStatus::Paused {
                self.resume(instance.id)?;
                resumed += 1;
            }
        }
        Ok(resumed)
    }

    /// Durably cancels the selected workflows, or every eligible workflow when empty.
    pub fn cancel_selected(&self, ids: &[WorkflowId]) -> Result<usize, SupervisorError> {
        let instances = self.repository.list()?;
        let mut cancelled = 0;
        for instance in instances {
            if !instance.status.is_terminal() && (ids.is_empty() || ids.contains(&instance.id)) {
                self.cancel(instance.id)?;
                cancelled += 1;
            }
        }
        Ok(cancelled)
    }

    /// Returns whether this supervisor currently owns the instance executor.
    #[must_use]
    pub fn has_executor(&self, id: WorkflowId) -> bool {
        self.tasks.contains_key(&id)
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

    fn start(&mut self, instance: WorkflowInstance) -> Result<(), SupervisorError> {
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
        let instance = self.transition(instance, WorkflowStatus::Running)?;
        let mut executor = match self
            .registry
            .resolve(&instance)
            .map(|factory| factory.create_executor())
        {
            Ok(Some(executor)) => executor,
            Ok(None) => {
                let (_, control) = watch::channel(ControlRequest::Continue);
                let mut context = WorkflowContext::new(
                    self.repository.clone(),
                    instance,
                    self.client.clone(),
                    control,
                );
                context.mark_failed("workflow kind has no executor")?;
                return Ok(());
            }
            Err(error) => {
                let (_, control) = watch::channel(ControlRequest::Continue);
                let mut context = WorkflowContext::new(
                    self.repository.clone(),
                    instance,
                    self.client.clone(),
                    control,
                );
                context.mark_failed(error.to_string())?;
                return Ok(());
            }
        };
        let id = instance.id;
        let repository = self.repository.clone();
        let client = self.client.clone();
        let (control_sender, control) = watch::channel(ControlRequest::Continue);
        let task = tokio::spawn(async move {
            let mut context = WorkflowContext::new(repository, instance, client, control);
            if let Err(error) = executor.execute(&mut context).await {
                if let Err(record_error) = context.mark_failed(error) {
                    tracing::error!(workflow_id = %id, error = %record_error, "failed to record workflow executor error");
                }
            } else if let Err(error) = fail_if_still_running(&mut context) {
                tracing::error!(workflow_id = %id, error = %error, "failed to finalize workflow executor");
            }
        });
        self.tasks.insert(id, task);
        self.controls.insert(id, control_sender);
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
        )
        .mark_failed(error)?;
        Ok(())
    }

    async fn reap_finished(&mut self) -> Result<(), SupervisorError> {
        let finished: Vec<_> = self
            .tasks
            .iter()
            .filter_map(|(id, task)| task.is_finished().then_some(*id))
            .collect();
        for id in finished {
            let task = self.tasks.remove(&id).expect("finished task exists");
            self.controls.remove(&id);
            if let Err(error) = task.await {
                let instance = self.read(id)?;
                if !matches!(
                    instance.status,
                    WorkflowStatus::Succeeded | WorkflowStatus::Failed | WorkflowStatus::Cancelled
                ) {
                    let (_, control) = watch::channel(ControlRequest::Continue);
                    let mut context = WorkflowContext::new(
                        self.repository.clone(),
                        instance,
                        self.client.clone(),
                        control,
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
        for task in self.tasks.values() {
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
