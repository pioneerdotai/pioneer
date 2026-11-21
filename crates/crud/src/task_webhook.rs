//! Webhook acceptance uses the ordinary run/event queue. Preparation reads only
//! immutable actor facts and the task revision/trigger/next run number; mutable
//! facts are compared again under the task row lock before any projection.
use crate::repositories::task_run;
use crate::task_events::materialize_task_events;
use crate::{AppendedTaskEvent, CrudStore};
use anyhow::{Context, Result};
use pioneer_crud_tasks::webhook_admission;
use pioneer_protocol::{Task, TaskEventPayload, TaskOccurrenceContract, TaskRun, TaskTrigger};
use sea_orm::TransactionTrait;

pub use pioneer_crud_tasks::webhook_admission::TaskWebhookAdmissionSnapshot;

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
        Ok(self
            .accept_task_queued_occurrence(
                expected_task,
                expected_trigger,
                run,
                occurrence,
                events,
                None,
            )
            .await?
            .map(|(_, events)| events))
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
        action: Option<crate::AgentCommitInput>,
    ) -> Result<Option<(String, Vec<AppendedTaskEvent>)>> {
        let change = crate::task_events::TaskWorkChange::for_tasks(
            &self.connection,
            [expected_task.id.clone()],
        );
        let result = async {
            let manual = action.is_some();
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
            let prepared_occurrence =
                webhook_admission::prepare_occurrence(occurrence, run.created_at)?;
            let prepared = self.prepare_task_events_for_write(events).await?;
            self.run_serialized_write(|| async {
                let prepared = self.refresh_task_event_preflight(prepared.clone()).await?;
                let transaction = self.connection.begin().await?;
                let outcome = async {
                    pioneer_crud_tasks::event_log::lock_task(&transaction, &expected_task.id)
                        .await?;
                    if let Some(action) = action.as_ref()
                        && let Some(existing) =
                            crate::repositories::agent_domain::load_agent_action(
                                &transaction,
                                &action.action.id,
                            )
                            .await?
                        && existing.status == "committed"
                    {
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
                        let existing_run = task_run::find_run_by_id(&transaction, run_id)
                            .await?
                            .context("manual run missing")?;
                        anyhow::ensure!(
                            existing_run.task_id == expected_task.id,
                            "manual run task mismatch"
                        );
                        return Ok(Some((existing_run.id, Vec::new())));
                    }
                    if !webhook_admission::admission_still_current(
                        &transaction,
                        expected_task,
                        expected_trigger,
                        &expected_spec,
                        run.run_number,
                        expected_generation,
                        manual,
                    )
                    .await?
                    {
                        return Ok(None);
                    }
                    let appended = self
                        .append_task_events_in_connection(&transaction, prepared, run.created_at)
                        .await?;
                    webhook_admission::insert_occurrence(
                        &transaction,
                        &run.id,
                        prepared_occurrence.clone(),
                    )
                    .await?;
                    if let Some(action) = action.as_ref() {
                        crate::repositories::agent_domain::commit_agent_action(
                            &transaction,
                            action,
                        )
                        .await?;
                    }
                    Ok(Some((run.id.clone(), appended)))
                }
                .await;
                match outcome {
                    Ok(value) => {
                        transaction
                            .commit()
                            .await
                            .context("failed to commit webhook admission")?;
                        Ok(value.map(|(run, events)| (run, materialize_task_events(events))))
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
                    .is_some_and(|(_, events)| crate::task_events::inserted_task_events(events)),
            );
        }
        result
    }
}
