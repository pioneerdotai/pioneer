//! Scoped Task event ingestion, fanout claims, and read cursors.
use crate::event_fanout::{
    self, TaskEventFanoutClaim, TaskEventFanoutOutcome, TaskEventFanoutPage,
};
use crate::event_pipeline::{
    MAX_ATOMIC_TASK_EVENT_BATCH_SIZE, append_task_events_in_connection_with_delivery_policy,
    load_task_event_metadata, prepare_task_events_for_write,
};
use crate::event_records::{
    AppendedTaskEvent, DeferredAppendedTaskEvent, TaskEventAppendStatus, materialize_task_events,
};
use crate::task_event;
use crate::task_projector::TaskOutputBinder;
use anyhow::{Context, Result};
use pioneer_postgres_pools::{
    DEFAULT_LOCK_RETRY_ATTEMPTS, DEFAULT_LOCK_RETRY_BASE_DELAY_MS, PostgresDatabase,
    retry_with_backoff,
};
use pioneer_protocol::{TaskEventPayload, TaskEventsResponse};
use sea_orm::TransactionTrait;
use std::{future::Future, time::Duration};

#[derive(Clone)]
pub struct TaskEventStore<B> {
    pub(crate) connection: PostgresDatabase,
    pub(crate) outputs: B,
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) delivery_commit_test_hooks: crate::delivery_transitions::DeliveryCommitTestHooks,
}

impl<B: TaskOutputBinder + Clone> TaskEventStore<B> {
    pub fn new(connection: PostgresDatabase, outputs: B) -> Self {
        Self {
            connection,
            outputs,
            #[cfg(any(test, feature = "test-support"))]
            delivery_commit_test_hooks: Default::default(),
        }
    }

    pub(crate) fn with_maintenance_access(&self) -> Self {
        Self {
            connection: self.connection.maintenance(),
            ..self.clone()
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn with_delivery_commit_test_hooks(
        mut self,
        hooks: &crate::delivery_transitions::DeliveryCommitTestHooks,
    ) -> Self {
        self.delivery_commit_test_hooks = hooks.clone();
        self
    }

    pub(crate) async fn retry_write_on_conflict<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry_with_backoff(
            operation,
            crate::retry::is_retryable,
            DEFAULT_LOCK_RETRY_ATTEMPTS,
            Duration::from_millis(DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }
    pub async fn append_task_event(
        &self,
        event: TaskEventPayload,
        event_timestamp_secs: i64,
    ) -> Result<AppendedTaskEvent> {
        let change = crate::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            [event.task_id().to_owned()],
        );
        let result = async {
            self.retry_write_on_conflict(|| {
                self.append_task_event_once(event.clone(), event_timestamp_secs)
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(value.append_status.is_inserted());
        }
        result
    }

    pub async fn append_task_events(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let change = crate::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            events.iter().map(|event| event.task_id().to_owned()),
        );
        let result = async {
            self.retry_write_on_conflict(|| {
                self.append_task_events_once(events.clone(), event_timestamp_secs)
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(crate::work_change::inserted_task_events(value));
        }
        result
    }

    pub async fn get_task_events(
        &self,
        task_id: &str,
        after_sequence: Option<i64>,
    ) -> Result<TaskEventsResponse> {
        let rows =
            task_event::list_events_for_task(&self.connection, task_id, after_sequence, None)
                .await?;
        let mut events = Vec::with_capacity(rows.len());
        let mut last_sequence = after_sequence.unwrap_or(0);
        for row in rows {
            last_sequence = row.sequence;
            events.push(task_event::task_event_from_model(row)?);
        }
        Ok(TaskEventsResponse {
            task_id: task_id.to_owned(),
            events,
            last_sequence,
            has_more: false,
        })
    }

    pub async fn get_task_events_page(
        &self,
        task_id: &str,
        after_sequence: Option<i64>,
        limit: usize,
    ) -> Result<TaskEventsResponse> {
        let mut rows = task_event::list_events_for_task(
            &self.connection,
            task_id,
            after_sequence,
            Some(limit.saturating_add(1) as u64),
        )
        .await?;
        let has_more = rows.len() > limit;
        rows.truncate(limit);
        let mut events = Vec::with_capacity(rows.len());
        let mut last_sequence = after_sequence.unwrap_or(0);

        for row in rows {
            last_sequence = row.sequence;
            events.push(task_event::task_event_from_model(row)?);
        }

        Ok(TaskEventsResponse {
            task_id: task_id.to_owned(),
            events,
            last_sequence,
            has_more,
        })
    }

    pub async fn list_task_events_after(
        &self,
        task_id: &str,
        after_sequence: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let rows =
            task_event::list_events_for_task(&self.connection, task_id, Some(after_sequence), None)
                .await?;
        let mut events = Vec::with_capacity(rows.len());

        for row in rows {
            let mut event =
                task_event::appended_task_event_from_model(row, TaskEventAppendStatus::Inserted)?;
            (event.workspace_id, event.root_task_id, event.parent_task_id) =
                load_task_event_metadata(&self.connection, &event.task_id).await?;
            events.push(event);
        }

        Ok(events)
    }

    pub async fn list_task_event_task_ids(&self) -> Result<Vec<String>> {
        task_event::list_event_task_ids(&self.connection).await
    }

    pub async fn due_task_event_fanout(
        &self,
        now: i64,
        limit: u64,
    ) -> Result<Vec<pioneer_entity::task_event_fanout_pending::Model>> {
        event_fanout::due(&self.with_maintenance_access().connection, now, limit).await
    }

    pub async fn has_pending_task_event_fanout(&self) -> Result<bool> {
        event_fanout::has_pending(&self.with_maintenance_access().connection).await
    }

    pub async fn claim_task_event_fanout(
        &self,
        row: &pioneer_entity::task_event_fanout_pending::Model,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<Option<TaskEventFanoutClaim>> {
        event_fanout::claim(
            &self.with_maintenance_access().connection,
            row,
            pioneer_protocol::generate_id(21),
            clock,
        )
        .await
    }

    pub async fn renew_task_event_fanout(
        &self,
        claim: &TaskEventFanoutClaim,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<bool> {
        event_fanout::renew(&self.with_maintenance_access().connection, claim, clock).await
    }

    pub async fn ack_task_event_fanout(
        &self,
        claim: &TaskEventFanoutClaim,
        sequence: i64,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<()> {
        event_fanout::ack(&self.connection, claim, sequence, clock).await
    }

    pub async fn release_task_event_fanout(
        &self,
        claim: &TaskEventFanoutClaim,
        outcome: TaskEventFanoutOutcome,
        clock: &(dyn Fn() -> i64 + Send + Sync),
    ) -> Result<()> {
        event_fanout::release(
            &self.with_maintenance_access().connection,
            claim,
            outcome,
            clock,
        )
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
        let page = task_event::fanout_page(
            &self.connection,
            task_id,
            after,
            limit,
            bytes_left,
            allow_oversized,
        )
        .await?;
        let TaskEventFanoutPage::Prefix {
            events: rows,
            bytes,
        } = page
        else {
            return Ok(TaskEventFanoutPage::BudgetDeferred);
        };
        // All reader resources have been returned before JSON decoding.
        let events = rows
            .into_iter()
            .map(|row| {
                task_event::appended_task_event_from_model(row, TaskEventAppendStatus::Inserted)
            })
            .collect::<Vec<_>>();
        Ok(TaskEventFanoutPage::Prefix { events, bytes })
    }

    pub async fn get_task_event_fanout_cursor(&self, task_id: &str) -> Result<Option<i64>> {
        task_event::find_fanout_cursor(&self.connection, task_id).await
    }

    pub async fn advance_task_event_fanout_cursor(
        &self,
        task_id: &str,
        sequence: i64,
    ) -> Result<()> {
        task_event::advance_fanout_cursor(
            &self.connection,
            task_id,
            sequence,
            chrono::Utc::now().fixed_offset(),
        )
        .await
    }

    pub async fn list_task_events_for_thread_turn(
        &self,
        thread_id: &str,
        turn_id: Option<&str>,
    ) -> Result<Vec<pioneer_protocol::TaskEvent>> {
        task_event::list_events_for_thread_turn(&self.connection, thread_id, turn_id)
            .await?
            .into_iter()
            .map(task_event::task_event_from_model)
            .collect()
    }

    async fn append_task_event_once(
        &self,
        event: TaskEventPayload,
        event_timestamp_secs: i64,
    ) -> Result<AppendedTaskEvent> {
        let events = prepare_task_events_for_write(&self.connection, vec![event]).await?;
        let transaction = self
            .connection
            .begin()
            .await
            .context("failed to begin task event materialization transaction")?;

        let mut appended_events = match append_task_events_in_connection_with_delivery_policy(
            &transaction,
            events,
            event_timestamp_secs,
            false,
            &self.outputs,
        )
        .await
        {
            Ok(events) => events,
            Err(error) => {
                let _ = transaction.rollback().await;
                return Err(error);
            }
        };

        transaction
            .commit()
            .await
            .context("failed to commit task event materialization transaction")?;

        appended_events
            .pop()
            .map(DeferredAppendedTaskEvent::into_owned)
            .context("single Task event materialization returned no event")
    }

    pub async fn append_task_events_once(
        &self,
        events: Vec<TaskEventPayload>,
        event_timestamp_secs: i64,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let mut appended_events = Vec::with_capacity(events.len());
        // This general ingestion API is deliberately unbounded at its public
        // boundary. Materialize it as bounded, ordered durable quanta so a
        // large import cannot monopolize the single physical writer. Event
        // identities make a retry after a partially committed call idempotent.
        for chunk in events.chunks(MAX_ATOMIC_TASK_EVENT_BATCH_SIZE) {
            let prepared = prepare_task_events_for_write(&self.connection, chunk.to_vec()).await?;
            let transaction = self
                .connection
                .begin()
                .await
                .context("failed to begin task event batch materialization transaction")?;

            let chunk_events = match append_task_events_in_connection_with_delivery_policy(
                &transaction,
                prepared,
                event_timestamp_secs,
                false,
                &self.outputs,
            )
            .await
            {
                Ok(appended_events) => appended_events,
                Err(error) => {
                    let _ = transaction.rollback().await;
                    return Err(error);
                }
            };

            transaction
                .commit()
                .await
                .context("failed to commit task event batch materialization transaction")?;
            appended_events.extend(materialize_task_events(chunk_events));
        }

        Ok(appended_events)
    }
}

#[cfg(test)]
#[path = "event_store_tests.rs"]
mod tests;
