use crate::CrudStore;
use anyhow::Result;
use pioneer_crud_tasks::task_event_context::TaskEventContextRepository;
pub use pioneer_crud_tasks::task_event_context::{
    TaskAnchorAgent, TaskAnchorContext, TaskEventContext,
};
use pioneer_protocol::{Task, TaskEventPayload, TaskRun, TaskTrigger};

impl CrudStore {
    pub async fn task_run_has_pending_thread_delivery(
        &self,
        workspace_id: &str,
        task_id: &str,
        run_id: &str,
        occurrence_thread_id: Option<&str>,
    ) -> Result<bool> {
        TaskEventContextRepository::new(&self.connection)
            .task_run_has_pending_thread_delivery(
                workspace_id,
                task_id,
                run_id,
                occurrence_thread_id,
            )
            .await
    }

    pub async fn get_task_event_context(
        &self,
        payload: &TaskEventPayload,
    ) -> Result<Option<TaskEventContext>> {
        TaskEventContextRepository::new(&self.connection)
            .get_task_event_context(payload)
            .await
    }

    pub async fn latest_task_run_uses_creation_anchor(&self, task: &Task) -> Result<bool> {
        TaskEventContextRepository::new(&self.connection)
            .latest_task_run_uses_creation_anchor(task)
            .await
    }

    pub async fn get_task_creation_anchor_context(&self, task: &Task) -> Result<TaskAnchorContext> {
        TaskEventContextRepository::new(&self.connection)
            .get_task_creation_anchor_context(task)
            .await
    }

    pub async fn get_task_run_anchor_context(
        &self,
        task: &Task,
        run: Option<&TaskRun>,
        trigger: Option<&TaskTrigger>,
    ) -> Result<TaskAnchorContext> {
        TaskEventContextRepository::new(&self.connection)
            .get_task_run_anchor_context(task, run, trigger)
            .await
    }
}
