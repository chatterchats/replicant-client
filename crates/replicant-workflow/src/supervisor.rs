use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::task::JoinHandle;

use crate::{
    RepositoryError, WorkflowId, WorkflowInstance, WorkflowRegistry, WorkflowRepository,
    WorkflowState, WorkflowStatus,
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
}

impl WorkflowContext {
    fn new(repository: Arc<WorkflowRepository>, instance: WorkflowInstance) -> Self {
        Self {
            repository,
            instance,
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

    /// Marks this invocation as durably waiting.
    pub fn mark_waiting(&mut self) -> Result<(), RepositoryError> {
        self.replace(
            WorkflowStatus::Waiting,
            self.instance.current_step.clone(),
            self.checkpoint_value()?,
            None,
        )
    }

    /// Marks this invocation as successfully completed.
    pub fn mark_succeeded<R: Serialize>(
        &mut self,
        result: Option<R>,
    ) -> Result<(), RepositoryError> {
        self.replace(
            WorkflowStatus::Succeeded,
            self.instance.current_step.clone(),
            self.checkpoint_value()?,
            result.map(serde_json::to_value).transpose()?,
        )
    }

    /// Marks this invocation as failed while retaining its checkpoint.
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

/// Owns at most one in-process executor for each persisted workflow.
pub struct WorkflowSupervisor {
    repository: Arc<WorkflowRepository>,
    registry: Arc<WorkflowRegistry>,
    tasks: HashMap<WorkflowId, JoinHandle<()>>,
}

impl WorkflowSupervisor {
    /// Creates a supervisor. Call [`Self::tick`] to reconcile and start work.
    #[must_use]
    pub fn new(repository: Arc<WorkflowRepository>, registry: Arc<WorkflowRegistry>) -> Self {
        Self {
            repository,
            registry,
            tasks: HashMap::new(),
        }
    }

    /// Reaps completed tasks, reconciles interrupted work, and starts runnable instances.
    pub async fn tick(&mut self) -> Result<(), SupervisorError> {
        self.reap_finished().await?;
        for instance in self.repository.list()? {
            if self.tasks.contains_key(&instance.id) {
                continue;
            }
            let instance = if instance.status == WorkflowStatus::Running {
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

    /// Durably requests a cooperative pause.
    pub fn pause(&self, id: WorkflowId) -> Result<(), SupervisorError> {
        let instance = self.read(id)?;
        self.transition(instance, WorkflowStatus::Paused)?;
        Ok(())
    }

    /// Durably resumes a paused workflow through reconciliation.
    pub fn resume(&self, id: WorkflowId) -> Result<(), SupervisorError> {
        let instance = self.read(id)?;
        self.transition(instance, WorkflowStatus::Reconciling)?;
        Ok(())
    }

    /// Durably requests cooperative cancellation.
    pub fn cancel(&self, id: WorkflowId) -> Result<(), SupervisorError> {
        let instance = self.read(id)?;
        self.transition(instance, WorkflowStatus::Cancelled)?;
        Ok(())
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
        self.repository.update(
            instance.id,
            instance.revision,
            WorkflowState {
                status,
                current_step: instance.current_step.clone(),
                checkpoint: instance.checkpoint::<Value>()?,
                last_error: instance.last_error.clone(),
                result: instance.result::<Value>()?,
            },
        )
    }

    fn start(&mut self, instance: WorkflowInstance) -> Result<(), SupervisorError> {
        let instance = self.transition(instance, WorkflowStatus::Running)?;
        let mut executor = match self
            .registry
            .resolve(&instance)
            .map(|factory| factory.create_executor())
        {
            Ok(Some(executor)) => executor,
            Ok(None) => {
                let mut context = WorkflowContext::new(self.repository.clone(), instance);
                context.mark_failed("workflow kind has no executor")?;
                return Ok(());
            }
            Err(error) => {
                let mut context = WorkflowContext::new(self.repository.clone(), instance);
                context.mark_failed(error.to_string())?;
                return Ok(());
            }
        };
        let id = instance.id;
        let repository = self.repository.clone();
        let task = tokio::spawn(async move {
            let mut context = WorkflowContext::new(repository, instance);
            if let Err(error) = executor.execute(&mut context).await {
                if let Err(record_error) = context.mark_failed(error) {
                    tracing::error!(workflow_id = %id, error = %record_error, "failed to record workflow executor error");
                }
            } else if let Err(error) = fail_if_still_running(&mut context) {
                tracing::error!(workflow_id = %id, error = %error, "failed to finalize workflow executor");
            }
        });
        self.tasks.insert(id, task);
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
            if let Err(error) = task.await {
                let instance = self.read(id)?;
                if !matches!(
                    instance.status,
                    WorkflowStatus::Succeeded | WorkflowStatus::Failed | WorkflowStatus::Cancelled
                ) {
                    let mut context = WorkflowContext::new(self.repository.clone(), instance);
                    context.mark_failed(format!("workflow executor task failed: {error}"))?;
                }
            }
        }
        Ok(())
    }
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
