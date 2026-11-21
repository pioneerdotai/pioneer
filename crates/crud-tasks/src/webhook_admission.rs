//! Task facts used before and during queue admission.

use crate::task::task_from_db_model;
use crate::task_agent_spec::task_agent_spec_from_db_model;
use crate::task_trigger::task_trigger_from_db_model;
use crate::{task, task_actor_contract, task_agent_spec, task_run, task_trigger};
use anyhow::{Context, Result};
use pioneer_entity::task_occurrence_contract;
use pioneer_protocol::{Task, TaskOccurrenceContract, TaskTrigger};
use sea_orm::{ConnectionTrait, EntityTrait, Set};

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

pub async fn get_webhook_task_trigger(
    db: &impl ConnectionTrait,
    id: &str,
) -> Result<Option<TaskTrigger>> {
    task_trigger::find_trigger_by_id(db, id)
        .await?
        .map(task_trigger_from_db_model)
        .transpose()
}

pub async fn get_task_queue_admission_snapshot(
    db: &impl ConnectionTrait,
    task_id: &str,
) -> Result<Option<TaskWebhookAdmissionSnapshot>> {
    let Some(trigger) = task_trigger::find_latest_trigger_by_task(db, task_id).await? else {
        return Ok(None);
    };
    get_task_webhook_admission_snapshot(db, &trigger.id).await
}

pub async fn get_task_webhook_admission_snapshot(
    db: &impl ConnectionTrait,
    trigger_id: &str,
) -> Result<Option<TaskWebhookAdmissionSnapshot>> {
    let Some(trigger) = get_webhook_task_trigger(db, trigger_id).await? else {
        return Ok(None);
    };
    let Some(task) = task::find_task_by_id(db, &trigger.task_id).await? else {
        return Ok(None);
    };
    let task = task_from_db_model(task)?;
    let actor = task_actor_contract::find_task_actor_contract(db, &task.id)
        .await?
        .context("webhook actor contract unavailable")?;
    let agent_spec = task_agent_spec::find_latest_base_agent_spec(db, &task.id)
        .await?
        .map(task_agent_spec_from_db_model)
        .transpose()?;
    let next_run_number = task_run::next_run_number(db, &task.id).await?;
    let execution_generation =
        task_actor_contract::next_task_occurrence_execution_generation(db, &task.id).await?;
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

pub fn prepare_occurrence(
    occurrence: TaskOccurrenceContract,
    created_at: i64,
) -> Result<task_occurrence_contract::ActiveModel> {
    occurrence
        .validate()
        .map_err(|_| anyhow::anyhow!("invalid webhook occurrence"))?;
    // This fresh occurrence is insert-only. Serialize before writer admission.
    Ok(task_occurrence_contract::ActiveModel {
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
        delivery_plan_json: Set(crate::values::optional_typed_json_to_db(
            &occurrence.delivery_plan,
        )?),
        terminal_reason: Set(occurrence.terminal_reason),
        created_at: Set(crate::values::unix_to_datetime(created_at)),
        updated_at: Set(crate::values::unix_to_datetime(created_at)),
    })
}

pub async fn admission_still_current(
    db: &impl ConnectionTrait,
    expected_task: &Task,
    expected_trigger: &TaskTrigger,
    expected_spec: &serde_json::Value,
    run_number: i64,
    expected_generation: u64,
    manual: bool,
) -> Result<bool> {
    let Some(current_trigger) = task_trigger::find_trigger_by_id(db, &expected_trigger.id).await?
    else {
        return Ok(false);
    };
    if current_trigger.task_id != expected_task.id
        || !(current_trigger.status == "active"
            || (manual && current_trigger.status == "exhausted"))
        || (manual
            && current_trigger.status
                != crate::status::task_trigger_status_to_db(expected_trigger.status))
        || current_trigger.next_fire_at.map(|at| at.timestamp()) != expected_trigger.next_fire_at
        || current_trigger.last_fire_at.map(|at| at.timestamp()) != expected_trigger.last_fire_at
        || current_trigger.spec_json != *expected_spec
        || current_trigger.webhook_secret_ref != expected_trigger.webhook_secret_ref
        || current_trigger.updated_at.timestamp() != expected_trigger.updated_at
    {
        return Ok(false);
    }
    let Some(current_task) = task::find_task_by_id(db, &expected_task.id).await? else {
        return Ok(false);
    };
    if current_task.revision != expected_task.revision
        || !matches!(
            current_task.status.as_str(),
            "waiting" | "scheduled" | "queued" | "running" | "waiting_review"
        )
        || task::finish_requested(db, &expected_task.id).await?
    {
        return Ok(false);
    }
    if task_run::next_run_number(db, &expected_task.id).await? != run_number
        || task_actor_contract::next_task_occurrence_execution_generation(db, &expected_task.id)
            .await?
            != expected_generation
        || task_actor_contract::find_task_actor_contract_row(db, &expected_task.id)
            .await?
            .is_none()
    {
        return Ok(false);
    }
    Ok(true)
}

pub async fn insert_occurrence(
    db: &impl ConnectionTrait,
    run_id: &str,
    prepared_occurrence: task_occurrence_contract::ActiveModel,
) -> Result<()> {
    task_occurrence_contract::Entity::insert(prepared_occurrence)
        .exec(db)
        .await?;
    let occurrence_row = task_actor_contract::find_task_occurrence_row_by_run(db, run_id)
        .await?
        .context("webhook occurrence disappeared after insert")?;
    crate::task_occurrence_producers::contract_change(db, None, Some(&occurrence_row)).await?;
    crate::task_child_producers::occurrence_identity(db, run_id).await?;
    Ok(())
}
