use crate::TaskRuntimeResult;
use anyhow::Context;
use pioneer_crud::{AppendedTaskEvent, CrudStore};
use pioneer_protocol::TaskEventPayload;
use std::future::Future;
use std::sync::Arc;
use tokio::task::JoinHandle;

struct AbortOnDropTask<T> {
    handle: Option<JoinHandle<T>>,
}

impl<T> AbortOnDropTask<T> {
    fn new(handle: JoinHandle<T>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    async fn join(mut self) -> Result<T, tokio::task::JoinError> {
        let result = self
            .handle
            .as_mut()
            .expect("join handle should be present")
            .await;
        self.handle = None;
        result
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take()
            && !handle.is_finished()
        {
            handle.abort();
        }
    }
}

async fn task_projector_fresh_task<F, T>(future: F) -> TaskRuntimeResult<T>
where
    F: Future<Output = TaskRuntimeResult<T>> + Send + 'static,
    T: Send + 'static,
{
    AbortOnDropTask::new(tokio::spawn(future))
        .join()
        .await
        .context("task projector worker failed")?
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
}
