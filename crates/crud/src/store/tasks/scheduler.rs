use crate::CrudStore;
use anyhow::Result;
use pioneer_crud_tasks::task_scheduler::TaskScheduler;
pub use pioneer_crud_tasks::task_scheduler::{TaskSchedulerPage, TaskSchedulerRowFailure};
use pioneer_protocol::{TaskRun, TaskRunStatus, TaskTrigger};
use sea_orm::prelude::DateTimeWithTimeZone;

impl CrudStore {
    pub async fn list_due_active_task_triggers_page(
        &self,
        now: i64,
        after: Option<(i64, &str)>,
        limit: u64,
    ) -> Result<Vec<TaskTrigger>> {
        TaskScheduler::new(&self.connection)
            .list_due_active_task_triggers_page(now, after, limit)
            .await
    }

    pub async fn list_due_active_task_triggers_page_partial(
        &self,
        now: i64,
        after: Option<(i64, &str)>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskTrigger, (i64, String)>> {
        TaskScheduler::new(&self.connection)
            .list_due_active_task_triggers_page_partial(now, after, limit)
            .await
    }

    pub async fn list_dormant_active_task_triggers(
        &self,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<TaskTrigger>> {
        TaskScheduler::new(&self.connection)
            .list_dormant_active_task_triggers(after_id, limit)
            .await
    }

    pub async fn list_dormant_active_task_triggers_partial(
        &self,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskTrigger, String>> {
        TaskScheduler::new(&self.connection)
            .list_dormant_active_task_triggers_partial(after_id, limit)
            .await
    }

    pub async fn list_task_recovery_runs(
        &self,
        status: TaskRunStatus,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        TaskScheduler::new(&self.connection)
            .list_task_recovery_runs(status, after_id, limit)
            .await
    }

    pub async fn list_task_recovery_runs_partial(
        &self,
        status: TaskRunStatus,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        TaskScheduler::new(&self.connection)
            .list_task_recovery_runs_partial(status, after_id, limit)
            .await
    }

    pub async fn list_task_recovery_runs_for_task(
        &self,
        task_id: &str,
        status: TaskRunStatus,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        TaskScheduler::new(&self.connection)
            .list_task_recovery_runs_for_task(task_id, status, limit)
            .await
    }

    pub async fn list_task_recovery_runs_for_task_partial(
        &self,
        task_id: &str,
        status: TaskRunStatus,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        TaskScheduler::new(&self.connection)
            .list_task_recovery_runs_for_task_partial(task_id, status, limit)
            .await
    }

    pub async fn list_queued_task_schedule(&self, limit: u64) -> Result<Vec<TaskRun>> {
        TaskScheduler::new(&self.connection)
            .list_queued_task_schedule(limit)
            .await
    }

    pub async fn list_queued_task_schedule_partial(
        &self,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        TaskScheduler::new(&self.connection)
            .list_queued_task_schedule_partial(limit)
            .await
    }

    pub async fn list_scheduler_due_task_runs(
        &self,
        now: i64,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<Vec<TaskRun>> {
        TaskScheduler::new(&self.connection)
            .list_scheduler_due_task_runs(now, after_id, limit)
            .await
    }

    pub async fn list_scheduler_due_task_runs_partial(
        &self,
        now: i64,
        after_id: Option<&str>,
        limit: u64,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        TaskScheduler::new(&self.connection)
            .list_scheduler_due_task_runs_partial(now, after_id, limit)
            .await
    }

    /// Only active triggers are scheduling inputs; malformed inactive history is irrelevant.

    pub async fn task_scheduler_inputs(
        &self,
        task_id: &str,
    ) -> Result<(Vec<TaskTrigger>, Vec<TaskRun>)> {
        TaskScheduler::new(&self.connection)
            .task_scheduler_inputs(task_id)
            .await
    }

    pub async fn task_scheduler_inputs_partial(
        &self,
        task_id: &str,
    ) -> Result<(
        TaskSchedulerPage<TaskTrigger, String>,
        TaskSchedulerPage<TaskRun, String>,
    )> {
        TaskScheduler::new(&self.connection)
            .task_scheduler_inputs_partial(task_id)
            .await
    }

    pub async fn list_active_task_triggers_partial(
        &self,
    ) -> Result<TaskSchedulerPage<TaskTrigger, String>> {
        TaskScheduler::new(&self.connection)
            .list_active_task_triggers_partial()
            .await
    }

    pub async fn task_scheduler_run_partial(
        &self,
        id: &str,
    ) -> Result<TaskSchedulerPage<TaskRun, String>> {
        TaskScheduler::new(&self.connection)
            .task_scheduler_run_partial(id)
            .await
    }

    pub async fn task_scheduler_trigger_partial(
        &self,
        id: &str,
    ) -> Result<TaskSchedulerPage<TaskTrigger, String>> {
        TaskScheduler::new(&self.connection)
            .task_scheduler_trigger_partial(id)
            .await
    }

    /// Keep the exact stored timestamp in finite keys; foreign/legacy rows may

    /// have subsecond deadlines even though protocol TaskTrigger uses seconds.

    pub async fn list_due_active_task_triggers_page_finite(
        &self,
        now: i64,
        after: Option<(DateTimeWithTimeZone, &str)>,
        upper: Option<(DateTimeWithTimeZone, &str)>,
        limit: u64,
    ) -> Result<(
        TaskSchedulerPage<TaskTrigger, (DateTimeWithTimeZone, String)>,
        Option<(DateTimeWithTimeZone, String)>,
    )> {
        TaskScheduler::new(&self.connection)
            .list_due_active_task_triggers_page_finite(now, after, upper, limit)
            .await
    }

    /// Finite advisory pass: capture a raw upper once, then constrain every page.

    pub async fn list_dormant_active_task_triggers_finite(
        &self,
        after: Option<&str>,
        upper: Option<&str>,
        limit: u64,
    ) -> Result<(TaskSchedulerPage<TaskTrigger, String>, Option<String>)> {
        TaskScheduler::new(&self.connection)
            .list_dormant_active_task_triggers_finite(after, upper, limit)
            .await
    }

    /// Finite advisory pass: capture a raw upper once, then constrain every page.

    pub async fn list_task_recovery_runs_finite(
        &self,
        status: TaskRunStatus,
        after: Option<&str>,
        upper: Option<&str>,
        limit: u64,
    ) -> Result<(TaskSchedulerPage<TaskRun, String>, Option<String>)> {
        TaskScheduler::new(&self.connection)
            .list_task_recovery_runs_finite(status, after, upper, limit)
            .await
    }

    /// Finite advisory pass: capture a raw upper once, then constrain every page.

    pub async fn list_scheduler_due_task_runs_finite(
        &self,
        now: i64,
        after: Option<&str>,
        upper: Option<&str>,
        limit: u64,
    ) -> Result<(TaskSchedulerPage<TaskRun, String>, Option<String>)> {
        TaskScheduler::new(&self.connection)
            .list_scheduler_due_task_runs_finite(now, after, upper, limit)
            .await
    }
}

impl CrudStore {
    pub async fn next_queued_task_ready_deadline(&self, now: i64) -> anyhow::Result<Option<i64>> {
        pioneer_crud_tasks::task_run::next_ready_deadline(&self.connection, now).await
    }
}
