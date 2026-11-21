//! Webhook acceptance uses the ordinary run/event queue. Preparation reads only
//! immutable actor facts and the task revision/trigger/next run number; mutable
//! facts are compared again under the task row lock before any projection.
use crate::{AgentCommitInput, AppendedTaskEvent, CrudStore};
use anyhow::{Context, Result};
use pioneer_crud_tasks::event_records::materialize_task_events;
use pioneer_crud_tasks::webhook_admission;
pub use pioneer_crud_tasks::webhook_admission::TaskWebhookAdmissionSnapshot;
use pioneer_crud_tasks::work_change::{TaskWorkChange, inserted_task_events};
use pioneer_protocol::{Task, TaskEventPayload, TaskOccurrenceContract, TaskRun, TaskTrigger};
use sea_orm::{DatabaseTransaction, TransactionTrait};

impl CrudStore {
    pub async fn get_webhook_task_trigger(&self, id: &str) -> Result<Option<TaskTrigger>> {
        webhook_admission::get_webhook_task_trigger(&self.connection, id).await
    }

    pub async fn get_task_queue_admission_snapshot(
        &self,
        task_id: &str,
    ) -> Result<Option<TaskWebhookAdmissionSnapshot>> {
        webhook_admission::get_task_queue_admission_snapshot(&self.connection, task_id).await
    }

    pub async fn get_task_webhook_admission_snapshot(
        &self,
        trigger_id: &str,
    ) -> Result<Option<TaskWebhookAdmissionSnapshot>> {
        webhook_admission::get_task_webhook_admission_snapshot(&self.connection, trigger_id).await
    }

    /// None means a concurrent mutation won; the caller must reload and repeat
    /// admission (or reject). The input is never committed partially.
    pub async fn accept_task_webhook(
        &self,
        expected_task: &Task,
        expected_trigger: &TaskTrigger,
        run: &TaskRun,
        occurrence: TaskOccurrenceContract,
        events: Vec<TaskEventPayload>,
    ) -> Result<Option<Vec<AppendedTaskEvent>>> {
        self.task_event_store()
            .accept_task_webhook(expected_task, expected_trigger, run, occurrence, events)
            .await
    }

    /// Shared ordinary queue admission. Manual requests carry the existing
    /// Agent action ledger commit; webhook POSTs retain their endpoint identity.
    pub async fn accept_task_queued_occurrence(
        &self,
        expected_task: &Task,
        expected_trigger: &TaskTrigger,
        run: &TaskRun,
        occurrence: TaskOccurrenceContract,
        events: Vec<TaskEventPayload>,
        action: Option<AgentCommitInput>,
    ) -> Result<Option<(String, Vec<AppendedTaskEvent>)>> {
        let Some(action) = action.as_ref() else {
            return Ok(self
                .accept_task_webhook(expected_task, expected_trigger, run, occurrence, events)
                .await?
                .map(|events| (run.id.clone(), events)));
        };
        let change = TaskWorkChange::for_tasks(&self.connection, [expected_task.id.clone()]);
        let tasks = self.task_event_store();
        let result = async {
            let prepared = tasks
                .prepare_queue_admission(
                    expected_task,
                    expected_trigger,
                    run,
                    occurrence,
                    events,
                    true,
                )
                .await?;
            let prepared = &prepared;
            let tasks = &tasks;
            self.run_serialized_write(|| async move {
                let prepared = tasks.refresh_queue_admission(prepared).await?;
                let transaction = self.connection.begin().await?;
                let outcome = async {
                    prepared.lock_task(&transaction).await?;
                    if let Some(run_id) =
                        replay_manual_run(&transaction, &expected_task.id, action).await?
                    {
                        return Ok(Some((run_id, Vec::new())));
                    }
                    let Some(events) = tasks
                        .admit_locked_queue_occurrence(&transaction, prepared)
                        .await?
                    else {
                        return Ok(None);
                    };
                    crate::store::execution::agent_transactions::commit_agent_action(
                        &transaction,
                        action,
                    )
                    .await?;
                    Ok(Some((run.id.clone(), events)))
                }
                .await;
                match outcome {
                    Ok(value) => {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit webhook admission")?;
                        Ok(value.map(|(id, events)| (id, materialize_task_events(events))))
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
                    .is_some_and(|(_, events)| inserted_task_events(events)),
            );
        }
        result
    }
}

async fn replay_manual_run(
    transaction: &DatabaseTransaction,
    task_id: &str,
    action: &AgentCommitInput,
) -> Result<Option<String>> {
    let existing =
        pioneer_crud_execution::agent_actions::load_agent_action(transaction, &action.action.id)
            .await?;
    let Some(existing) = existing.filter(|record| record.status == "committed") else {
        return Ok(None);
    };
    anyhow::ensure!(
        existing.request_fingerprint == action.request_fingerprint
            && existing.execution_id == action.action.execution_id,
        "manual run action identity conflict"
    );
    let response: serde_json::Value = serde_json::from_str(
        existing
            .response_json
            .as_deref()
            .context("manual run receipt missing")?,
    )?;
    let run_id = response["taskRunNow"]["runId"]
        .as_str()
        .context("manual run reference missing")?;
    let existing_run = pioneer_crud_tasks::task_run::find_run_by_id(transaction, run_id)
        .await?
        .context("manual run missing")?;
    anyhow::ensure!(existing_run.task_id == task_id, "manual run task mismatch");
    Ok(Some(existing_run.id))
}
