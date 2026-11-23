//! Task event reads and publication receipts.

use crate::{
    AppendedTaskEvent, CrudStore, TaskEventFanoutClaim, TaskEventFanoutOutcome, TaskEventFanoutPage,
};
use anyhow::Result;
use pioneer_protocol::TaskEventsResponse;

impl CrudStore {
    pub async fn get_task_events(
        &self,
        task_id: &str,
        after_sequence: Option<i64>,
    ) -> Result<TaskEventsResponse> {
        self.task_event_store()
            .get_task_events(task_id, after_sequence)
            .await
    }

    pub async fn get_task_events_page(
        &self,
        task_id: &str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> Result<TaskEventsResponse> {
        self.task_event_store()
            .get_task_events_page(task_id, after_sequence, limit)
            .await
    }

    pub async fn list_task_events_after(
        &self,
        task_id: &str,
        after_sequence: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        self.task_event_store()
            .list_task_events_after(task_id, after_sequence)
            .await
    }

    pub async fn list_task_event_task_ids(&self) -> Result<Vec<String>> {
        self.task_event_store().list_task_event_task_ids().await
    }

    pub async fn due_task_event_fanout(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<pioneer_entity::task_event_fanout_pending::Model>> {
        self.task_event_store()
            .due_task_event_fanout(now, limit)
            .await
    }

    pub async fn has_pending_task_event_fanout(&self) -> Result<bool> {
        self.task_event_store()
            .has_pending_task_event_fanout()
            .await
    }

    pub async fn claim_task_event_fanout(
        &self,
        row: &pioneer_entity::task_event_fanout_pending::Model,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<Option<TaskEventFanoutClaim>> {
        self.task_event_store()
            .claim_task_event_fanout(row, clock)
            .await
    }

    pub async fn renew_task_event_fanout(
        &self,
        claim: &TaskEventFanoutClaim,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<bool> {
        self.task_event_store()
            .renew_task_event_fanout(claim, clock)
            .await
    }

    pub async fn ack_task_event_fanout(
        &self,
        claim: &TaskEventFanoutClaim,
        sequence: i64,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<()> {
        self.task_event_store()
            .ack_task_event_fanout(claim, sequence, clock)
            .await
    }

    pub async fn release_task_event_fanout(
        &self,
        claim: &TaskEventFanoutClaim,
        outcome: TaskEventFanoutOutcome,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<()> {
        self.task_event_store()
            .release_task_event_fanout(claim, outcome, clock)
            .await
    }

    pub async fn task_event_fanout_page(
        &self,
        task_id: &str,
        after: i64,
        limit: usize,
        bytes_left: &mut usize,
        allow_oversized: bool,
    ) -> Result<TaskEventFanoutPage<Result<AppendedTaskEvent>>> {
        self.task_event_store()
            .task_event_fanout_page(task_id, after, limit, bytes_left, allow_oversized)
            .await
    }

    pub async fn get_task_event_fanout_cursor(&self, task_id: &str) -> Result<Option<i64>> {
        self.task_event_store()
            .get_task_event_fanout_cursor(task_id)
            .await
    }

    pub async fn advance_task_event_fanout_cursor(
        &self,
        task_id: &str,
        sequence: i64,
    ) -> Result<()> {
        self.task_event_store()
            .advance_task_event_fanout_cursor(task_id, sequence)
            .await
    }

    pub async fn list_task_events_for_thread_turn(
        &self,
        thread_id: &str,
        turn_id: Option<&str>,
    ) -> Result<Vec<pioneer_protocol::TaskEvent>> {
        self.task_event_store()
            .list_task_events_for_thread_turn(thread_id, turn_id)
            .await
    }
}

impl CrudStore {
    pub async fn next_task_event_fanout_deadline(&self) -> anyhow::Result<Option<i64>> {
        pioneer_crud_tasks::event_fanout::next_deadline(&self.connection).await
    }
}
