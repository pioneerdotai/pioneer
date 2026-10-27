//! Webhook acceptance uses the ordinary run/event queue. Preparation reads only
//! immutable actor facts and the task revision/trigger/next run number; mutable
//! facts are compared again inside the serialized writer before any projection.
use crate::repositories::{task, task_actor_contract, task_agent_spec, task_run, task_trigger};
use crate::{
    AppendedTaskEvent, CrudStore, task_agent_spec_from_db_model, task_from_db_model,
    task_trigger_from_db_model,
};
use anyhow::{Context, Result};
use pioneer_entity::task_occurrence_contract;
use pioneer_protocol::{Task, TaskEventPayload, TaskOccurrenceContract, TaskRun, TaskTrigger};
use sea_orm::{EntityTrait, Set, TransactionTrait};

/// Bounded reader facts for a new POST. No past run payload, run-specific spec,
/// binding, turn, candidate or review history belongs in admission preparation.
#[derive(Debug, Clone)]
pub struct TaskWebhookAdmissionSnapshot {
    pub task: Task,
    pub trigger: TaskTrigger,
    pub actor: pioneer_protocol::TaskActorContract,
    pub agent_spec: Option<pioneer_protocol::TaskAgentSpec>,
    pub next_run_number: i64,
    pub execution_generation: u64,
}

impl CrudStore {
    pub async fn get_webhook_task_trigger(&self, id: &str) -> Result<Option<TaskTrigger>> {
        task_trigger::find_trigger_by_id(&self.connection, id)
            .await?
            .map(task_trigger_from_db_model)
            .transpose()
    }

    pub async fn get_task_webhook_admission_snapshot(
        &self,
        trigger_id: &str,
    ) -> Result<Option<TaskWebhookAdmissionSnapshot>> {
        let Some(trigger) = self.get_webhook_task_trigger(trigger_id).await? else {
            return Ok(None);
        };
        let Some(task) = task::find_task_by_id(&self.connection, &trigger.task_id).await? else {
            return Ok(None);
        };
        let task = task_from_db_model(task)?;
        let actor = task_actor_contract::find_task_actor_contract(&self.connection, &task.id)
            .await?
            .context("webhook actor contract unavailable")?;
        let agent_spec = task_agent_spec::find_latest_base_agent_spec(&self.connection, &task.id)
            .await?
            .map(task_agent_spec_from_db_model)
            .transpose()?;
        let next_run_number = task_run::next_run_number(&self.connection, &task.id).await?;
        let execution_generation = task_actor_contract::next_task_occurrence_execution_generation(
            &self.connection,
            &task.id,
        )
        .await?;
        // These separate bounded reads need no long reader snapshot. The short
        // admission writer rechecks revision/trigger/number/generation before
        // projecting anything; the actor contract is immutable by construction.
        Ok(Some(TaskWebhookAdmissionSnapshot {
            task,
            trigger,
            actor,
            agent_spec,
            next_run_number,
            execution_generation,
        }))
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
        anyhow::ensure!(
            run.task_id == expected_task.id
                && occurrence.run_id == run.id
                && occurrence.task_id == run.task_id
                && run.trigger_id.as_deref() == Some(expected_trigger.id.as_str()),
            "invalid webhook occurrence identity"
        );
        occurrence
            .validate()
            .map_err(|_| anyhow::anyhow!("invalid webhook occurrence"))?;
        let expected_spec = serde_json::to_string(&expected_trigger.spec)?;
        // This fresh occurrence is insert-only. Serialize before writer admission.
        let expected_generation = occurrence.execution_generation;
        let prepared_occurrence = task_occurrence_contract::ActiveModel {
            occurrence_id: Set(occurrence.occurrence_id),
            task_id: Set(occurrence.task_id),
            run_id: Set(occurrence.run_id),
            trigger_id: Set(occurrence.trigger_id),
            occurrence_key: Set(occurrence.occurrence_key),
            execution_generation: Set(i64::try_from(occurrence.execution_generation)?),
            agent_execution_id: Set(occurrence.agent_execution_id),
            work_graph_root_execution_id: Set(occurrence.work_graph_root_execution_id),
            root_resource_scope_id: Set(occurrence.root_resource_scope_id),
            status: Set(serde_json::to_string(&occurrence.status)?
                .trim_matches('"')
                .to_owned()),
            queue_position: Set(occurrence.queue_position.map(i64::try_from).transpose()?),
            retry_attempt: Set(i64::from(occurrence.retry_attempt)),
            action_idempotency_key: Set(occurrence.action_idempotency_key),
            route_id: Set(occurrence.route_id),
            result_return_route_id: Set(occurrence.result_return_route_id),
            delivery_plan_json: Set(crate::util::optional_typed_json_to_db(
                &occurrence.delivery_plan,
            )?),
            terminal_reason: Set(occurrence.terminal_reason),
            created_at: Set(crate::util::unix_to_datetime(run.created_at)),
            updated_at: Set(crate::util::unix_to_datetime(run.created_at)),
        };
        let prepared = self.prepare_task_events_for_write(events).await?;
        self.run_serialized_write(|| async {
            let prepared = self.refresh_task_event_preflight(prepared.clone()).await?;
            let transaction = self.connection.begin().await?;
            let outcome = async {
                let Some(current_trigger) =
                    task_trigger::find_trigger_by_id(&transaction, &expected_trigger.id).await?
                else {
                    return Ok(None);
                };
                if current_trigger.task_id != expected_task.id
                    || current_trigger.status != "active"
                    || current_trigger.spec_json != expected_spec
                    || current_trigger.webhook_secret_ref != expected_trigger.webhook_secret_ref
                    || current_trigger.updated_at.timestamp() != expected_trigger.updated_at
                {
                    return Ok(None);
                }
                let Some(current_task) =
                    task::find_task_by_id(&transaction, &expected_task.id).await?
                else {
                    return Ok(None);
                };
                if current_task.revision != expected_task.revision
                    || !matches!(
                        current_task.status.as_str(),
                        "waiting" | "scheduled" | "queued" | "running" | "waiting_review"
                    )
                    || task::finish_requested(&transaction, &expected_task.id).await?
                {
                    return Ok(None);
                }
                if task_run::next_run_number(&transaction, &expected_task.id).await?
                    != run.run_number
                    || task_actor_contract::next_task_occurrence_execution_generation(
                        &transaction,
                        &expected_task.id,
                    )
                    .await?
                        != expected_generation
                    || task_actor_contract::find_task_actor_contract_row(
                        &transaction,
                        &expected_task.id,
                    )
                    .await?
                    .is_none()
                {
                    return Ok(None);
                }
                let appended = self
                    .append_task_events_in_connection(&transaction, prepared, run.created_at)
                    .await?;
                task_occurrence_contract::Entity::insert(prepared_occurrence.clone())
                    .exec(&transaction)
                    .await?;
                Ok(Some(appended))
            }
            .await;
            match outcome {
                Ok(value) => {
                    transaction
                        .commit()
                        .await
                        .context("failed to commit webhook admission")?;
                    Ok(value)
                }
                Err(error) => {
                    let _ = transaction.rollback().await;
                    Err(error)
                }
            }
        })
        .await
    }
}
