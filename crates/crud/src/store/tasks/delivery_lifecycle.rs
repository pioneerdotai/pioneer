//! Short writer boundaries for worker ownership and delivery cancellation.
//! Payloads/projections are prepared outside writer admission. Only indexed
//! durable-state reads and the bounded atomic event write set run inside it.
use crate::{AppendedTaskEvent, CrudStore, TaskEventPayload};
use anyhow::Result;
pub use pioneer_crud_tasks::cancellation::TaskCancellationRunsChanged;
use pioneer_crud_tasks::delivery_event_checks::CancellationSnapshotChanged;
#[cfg(any(test, feature = "test-support"))]
pub use pioneer_crud_tasks::delivery_transitions::TaskDeliveryCommitTestKind;
pub use pioneer_crud_tasks::delivery_transitions::{
    TaskDeliveryTransition, TaskDeliveryTransitionOutcome,
};
use pioneer_crud_tasks::event_records::materialize_task_events;
use pioneer_protocol::TaskDeliveryAttempt;
use sea_orm::TransactionTrait;

impl CrudStore {
    pub async fn get_task_delivery_attempt(
        &self,
        delivery_id: &str,
        number: u32,
    ) -> Result<Option<TaskDeliveryAttempt>> {
        self.task_event_store()
            .get_task_delivery_attempt(delivery_id, number)
            .await
    }

    pub async fn transition_task_delivery(
        &self,
        event: TaskEventPayload,
        transition: TaskDeliveryTransition,
        at: i64,
    ) -> Result<TaskDeliveryTransitionOutcome<AppendedTaskEvent>> {
        self.task_event_store()
            .transition_task_delivery(event, transition, at)
            .await
    }

    /// Recovery preparation reads a selected delivery, exact attempt and retry.
    /// Revalidate these raw facts in the same transaction as the domain event.
    pub async fn recover_task_delivery(
        &self,
        event: TaskEventPayload,
        snapshot: &crate::DeliveryRecoverySnapshot,
        cutoff: i64,
        at: i64,
    ) -> Result<TaskDeliveryTransitionOutcome<AppendedTaskEvent>> {
        self.task_event_store()
            .recover_task_delivery(event, snapshot, cutoff, at)
            .await
    }

    /// Keeps delivery cancellation inside the caller's existing atomic Task
    /// batch (including its optional Agent action). Refreshes only delivery
    /// snapshots after a typed preflight conflict, outside the writer.
    pub async fn append_task_cancellation_events(
        &self,
        events: Vec<TaskEventPayload>,
        at: i64,
        action: Option<crate::AgentCommitInput>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        self.append_task_cancellation_events_as(events, at, action, None)
            .await
    }

    pub async fn append_task_cancellation_events_as(
        &self,
        events: Vec<TaskEventPayload>,
        at: i64,
        action: Option<crate::AgentCommitInput>,
        actor_id: Option<String>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let change = pioneer_crud_tasks::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            events.iter().map(|event| event.task_id().to_owned()),
        );
        let result = async {
            let mut appended = Vec::new();
            if action.is_some() {
                return self
                    .append_task_cancellation_quantum(events, at, action, actor_id)
                    .await;
            }
            for chunk in
                events.chunks(pioneer_crud_tasks::event_pipeline::MAX_ATOMIC_TASK_EVENT_BATCH_SIZE)
            {
                appended.extend(
                    self.append_task_cancellation_quantum(
                        chunk.to_vec(),
                        at,
                        None,
                        actor_id.clone(),
                    )
                    .await?,
                );
            }
            Ok(appended)
        }
        .await;
        if let Ok(value) = &result {
            change.completed(pioneer_crud_tasks::work_change::inserted_task_events(value));
        }
        result
    }

    async fn append_task_cancellation_quantum(
        &self,
        events: Vec<TaskEventPayload>,
        at: i64,
        action: Option<crate::AgentCommitInput>,
        actor_id: Option<String>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        pioneer_sqlite::retry_with_backoff(
            || {
                self.run_serialized_write(|| {
                    self.append_task_cancellation_events_once(
                        events.clone(),
                        at,
                        action.clone(),
                        actor_id.clone(),
                    )
                })
            },
            |error| {
                error.chain().any(|cause| {
                    cause
                        .downcast_ref::<CancellationSnapshotChanged>()
                        .is_some()
                })
            },
            pioneer_sqlite::DEFAULT_LOCK_RETRY_ATTEMPTS,
            std::time::Duration::from_millis(pioneer_sqlite::DEFAULT_LOCK_RETRY_BASE_DELAY_MS),
        )
        .await
    }

    async fn append_task_cancellation_events_once(
        &self,
        events: Vec<TaskEventPayload>,
        at: i64,
        action: Option<crate::AgentCommitInput>,
        actor_id: Option<String>,
    ) -> Result<Vec<AppendedTaskEvent>> {
        let tasks = self.task_event_store();
        let prepared = tasks.prepare_cancellation(events, at).await?;
        #[cfg(any(test, feature = "test-support"))]
        self.pause_delivery_commit_for_test(TaskDeliveryCommitTestKind::Cancellation)
            .await;
        let control_change = self.control_work_change(
            "gateway.control",
            &[
                crate::GatewayControlWake::Resources,
                crate::GatewayControlWake::Actions,
            ],
        );
        let mut action_changed = false;
        let tx = self.connection.begin().await?;
        let result = async {
            let appended = tasks
                .append_cancellation_in_transaction(&tx, prepared, at, actor_id.as_deref())
                .await?;
            if let Some(action) = &action {
                action_changed =
                    !pioneer_crud_execution::agent_action_commit::agent_action_commit_exists(
                        &tx, action,
                    )
                    .await?;
                crate::store::execution::agent_transactions::commit_agent_action(&tx, action)
                    .await?;
            }
            Ok(appended)
        }
        .await;
        match result {
            Ok(appended) => {
                tx.commit().await?;
                control_change.completed(action_changed);
                Ok(materialize_task_events(appended))
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }
}

#[cfg(any(test, feature = "test-support"))]
impl CrudStore {
    #[doc(hidden)]
    pub fn set_delivery_commit_gate_for_test(
        &self,
        kind: TaskDeliveryCommitTestKind,
        entered: std::sync::Arc<tokio::sync::Notify>,
        release: std::sync::Arc<tokio::sync::Notify>,
    ) {
        self.delivery_commit_test_hooks
            .set_delivery_commit_gate_for_test(kind, entered, release);
    }

    pub(crate) async fn pause_delivery_commit_for_test(&self, kind: TaskDeliveryCommitTestKind) {
        self.delivery_commit_test_hooks
            .pause_delivery_commit_for_test(kind)
            .await;
    }
}
