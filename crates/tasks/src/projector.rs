use crate::TaskRuntimeResult;
use crate::task_boundary::task_fresh_task;
use pioneer_crud::{AppendedTaskEvent, CrudStore};
use pioneer_protocol::TaskEventPayload;
use std::future::Future;
use std::sync::Arc;

async fn task_projector_fresh_task<F, T>(future: F) -> TaskRuntimeResult<T>
where
    F: Future<Output = TaskRuntimeResult<T>> + Send + 'static,
    T: Send + 'static,
{
    task_fresh_task(future, "task projector worker failed").await
}

#[derive(Clone)]
pub struct TaskProjector {
    store: Arc<CrudStore>,
}

impl TaskProjector {
    pub fn new(store: Arc<CrudStore>) -> Self {
        Self { store }
    }

    pub async fn append_event(
        &self,
        event: TaskEventPayload,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<AppendedTaskEvent> {
        let store = self.store.clone();
        task_projector_fresh_task(async move {
            Ok(store.append_task_event(event, event_timestamp_secs).await?)
        })
        .await
    }

    pub async fn append_events(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<Vec<AppendedTaskEvent>> {
        let store = self.store.clone();
        task_projector_fresh_task(async move {
            Ok(store
                .append_task_events(events, event_timestamp_secs)
                .await?)
        })
        .await
    }

    pub async fn append_events_with_agent_action(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        agent_action: pioneer_crud::AgentCommitInput,
    ) -> TaskRuntimeResult<Vec<AppendedTaskEvent>> {
        let store = self.store.clone();
        task_projector_fresh_task(async move {
            Ok(store
                .append_task_events_with_agent_action(events, event_timestamp_secs, agent_action)
                .await?)
        })
        .await
    }

    pub async fn append_events_with_execution_admission(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        admission: pioneer_crud::NewTaskExecutionAdmission,
    ) -> TaskRuntimeResult<Vec<AppendedTaskEvent>> {
        let store = self.store.clone();
        task_projector_fresh_task(async move {
            Ok(store
                .append_task_events_with_execution_admission(
                    events,
                    event_timestamp_secs,
                    admission,
                )
                .await?)
        })
        .await
    }

    pub async fn append_events_with_execution_readmission(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        admission: pioneer_crud::NewTaskExecutionAdmission,
    ) -> TaskRuntimeResult<Vec<AppendedTaskEvent>> {
        let store = self.store.clone();
        task_projector_fresh_task(async move {
            Ok(store
                .append_task_events_with_execution_readmission(
                    events,
                    event_timestamp_secs,
                    admission,
                )
                .await?)
        })
        .await
    }

    pub async fn append_events_with_execution_readmission_and_agent_action(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
        admission: pioneer_crud::NewTaskExecutionAdmission,
        agent_action: pioneer_crud::AgentCommitInput,
    ) -> TaskRuntimeResult<Vec<AppendedTaskEvent>> {
        let store = self.store.clone();
        task_projector_fresh_task(async move {
            Ok(store
                .append_task_events_with_execution_readmission_and_agent_action(
                    events,
                    event_timestamp_secs,
                    admission,
                    agent_action,
                )
                .await?)
        })
        .await
    }

    pub async fn commit_task_creation(
        &self,
        input: pioneer_crud::TaskCreationCommitInput,
    ) -> TaskRuntimeResult<Vec<AppendedTaskEvent>> {
        let store = self.store.clone();
        task_projector_fresh_task(async move { Ok(store.commit_task_creation(input).await?) }).await
    }
}
