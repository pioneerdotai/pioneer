//! Admits queued runs under the task row lock using the ordinary event pipeline.
use crate::event_pipeline::{
    append_task_events_in_connection_with_delivery_policy, prepare_task_events_for_write,
    refresh_task_event_preflight,
};
use crate::event_records::{AppendedTaskEvent, DeferredAppendedTaskEvent, materialize_task_events};
use crate::event_store::TaskEventStore;
use crate::task_event::PreparedTaskEvent;
use crate::{event_log, task_projector::TaskOutputBinder, webhook_admission};
use anyhow::{Context, Result};
use pioneer_entity::task_occurrence_contract;
use pioneer_protocol::{Task, TaskEventPayload, TaskOccurrenceContract, TaskRun, TaskTrigger};
use sea_orm::{DatabaseTransaction, TransactionTrait};

/// Facts captured before writer admission; refreshed retries preserve this fence.
pub struct PreparedQueueAdmission<'a> {
    expected_task: &'a Task,
    expected_trigger: &'a TaskTrigger,
    run: &'a TaskRun,
    expected_spec: serde_json::Value,
    expected_generation: u64,
    manual: bool,
    occurrence: task_occurrence_contract::ActiveModel,
    events: Vec<PreparedTaskEvent>,
}

impl PreparedQueueAdmission<'_> {
    pub async fn lock_task(&self, transaction: &DatabaseTransaction) -> Result<()> {
        event_log::lock_task(transaction, &self.expected_task.id).await?;
        Ok(())
    }
}

impl<B: TaskOutputBinder + Clone> TaskEventStore<B> {
    pub async fn prepare_queue_admission<'a>(
        &self,
        expected_task: &'a Task,
        expected_trigger: &'a TaskTrigger,
        run: &'a TaskRun,
        occurrence: TaskOccurrenceContract,
        events: Vec<TaskEventPayload>,
        manual: bool,
    ) -> Result<PreparedQueueAdmission<'a>> {
        anyhow::ensure!(
            run.task_id == expected_task.id
                && occurrence.run_id == run.id
                && occurrence.task_id == run.task_id
                && if manual {
                    run.trigger_id.is_none()
                } else {
                    run.trigger_id.as_deref() == Some(expected_trigger.id.as_str())
                },
            "invalid webhook occurrence identity"
        );
        let expected_spec = serde_json::to_value(&expected_trigger.spec)?;
        let expected_generation = occurrence.execution_generation;
        let occurrence = webhook_admission::prepare_occurrence(occurrence, run.created_at)?;
        let events = prepare_task_events_for_write(&self.connection, events).await?;
        Ok(PreparedQueueAdmission {
            expected_task,
            expected_trigger,
            run,
            expected_spec,
            expected_generation,
            manual,
            occurrence,
            events,
        })
    }

    pub async fn refresh_queue_admission<'a>(
        &self,
        prepared: &PreparedQueueAdmission<'a>,
    ) -> Result<PreparedQueueAdmission<'a>> {
        Ok(PreparedQueueAdmission {
            expected_task: prepared.expected_task,
            expected_trigger: prepared.expected_trigger,
            run: prepared.run,
            expected_spec: prepared.expected_spec.clone(),
            expected_generation: prepared.expected_generation,
            manual: prepared.manual,
            occurrence: prepared.occurrence.clone(),
            events: refresh_task_event_preflight(&self.connection, prepared.events.clone()).await?,
        })
    }

    /// The caller must lock the task in this transaction before applying its events.
    pub async fn admit_locked_queue_occurrence(
        &self,
        transaction: &DatabaseTransaction,
        prepared: PreparedQueueAdmission<'_>,
    ) -> Result<Option<Vec<DeferredAppendedTaskEvent>>> {
        if !webhook_admission::admission_still_current(
            transaction,
            prepared.expected_task,
            prepared.expected_trigger,
            &prepared.expected_spec,
            prepared.run.run_number,
            prepared.expected_generation,
            prepared.manual,
        )
        .await?
        {
            return Ok(None);
        }
        let appended = append_task_events_in_connection_with_delivery_policy(
            transaction,
            prepared.events,
            prepared.run.created_at,
            false,
            &self.outputs,
        )
        .await?;
        webhook_admission::insert_occurrence(transaction, &prepared.run.id, prepared.occurrence)
            .await?;
        Ok(Some(appended))
    }

    pub async fn accept_task_webhook(
        &self,
        expected_task: &Task,
        expected_trigger: &TaskTrigger,
        run: &TaskRun,
        occurrence: TaskOccurrenceContract,
        events: Vec<TaskEventPayload>,
    ) -> Result<Option<Vec<AppendedTaskEvent>>> {
        let change = crate::work_change::TaskWorkChange::for_tasks(
            &self.connection,
            [expected_task.id.clone()],
        );
        let result = async {
            let prepared = self
                .prepare_queue_admission(
                    expected_task,
                    expected_trigger,
                    run,
                    occurrence,
                    events,
                    false,
                )
                .await?;
            let prepared = &prepared;
            self.retry_write_on_conflict(|| async move {
                let prepared = self.refresh_queue_admission(prepared).await?;
                let transaction = self.connection.begin().await?;
                let outcome = async {
                    prepared.lock_task(&transaction).await?;
                    self.admit_locked_queue_occurrence(&transaction, prepared)
                        .await
                }
                .await;
                match outcome {
                    Ok(value) => {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit webhook admission")?;
                        Ok(value.map(materialize_task_events))
                    }
                    Err(error) => {
                        let _ = transaction.rollback().await;
                        Err(error)
                    }
                }
            })
            .await
        }
        .await;
        if let Ok(value) = &result {
            change.completed(
                value
                    .as_ref()
                    .is_some_and(|events| crate::work_change::inserted_task_events(events)),
            );
        }
        result
    }
}

#[cfg(test)]
#[path = "queued_admission_tests.rs"]
mod tests;
