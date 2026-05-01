use crate::TaskRuntimeResult;
use pioneer_crud::{AppendedTaskEvent, CrudStore};
use pioneer_protocol::TaskEventPayload;
use std::sync::Arc;

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
        Ok(self
            .store
            .append_task_event(event, event_timestamp_secs)
            .await?)
    }

    pub async fn append_events(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> TaskRuntimeResult<Vec<AppendedTaskEvent>> {
        Ok(self
            .store
            .append_task_events(events, event_timestamp_secs)
            .await?)
    }
}
