//! Canonical execution action identities and receipts.

use anyhow::{Context, Result, bail};
use pioneer_entity::{agent_action, agent_action_outbox, agent_action_receipt};
use pioneer_protocol::{AgentRouteAction, SafeRouteProvenance};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, Set};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentActionInput {
    pub id: String,
    pub execution_id: String,
    pub action_kind: String,
    pub idempotency_key: String,
    pub request_fingerprint: String,
    pub now: DateTimeWithTimeZone,
}

pub async fn insert_agent_action_idempotent<C: ConnectionTrait>(
    db: &C,
    input: &AgentActionInput,
) -> Result<agent_action::Model> {
    if let Some(existing) = agent_action::Entity::find()
        .filter(agent_action::Column::ExecutionId.eq(input.execution_id.clone()))
        .filter(agent_action::Column::IdempotencyKey.eq(input.idempotency_key.clone()))
        .one(db)
        .await
        .context("failed to load idempotent agent action")?
    {
        if existing.id != input.id
            || existing.action_kind != input.action_kind
            || existing.request_fingerprint != input.request_fingerprint
        {
            bail!("idempotency key was reused with different immutable action facts");
        }
        return Ok(existing);
    }
    agent_action::Entity::insert(agent_action::ActiveModel {
        id: Set(input.id.clone()),
        execution_id: Set(input.execution_id.clone()),
        action_kind: Set(input.action_kind.clone()),
        idempotency_key: Set(input.idempotency_key.clone()),
        request_fingerprint: Set(input.request_fingerprint.clone()),
        status: Set("prepared".to_owned()),
        created_at: Set(input.now),
        committed_at: Set(None),
        response_json: Set(None),
    })
    .on_conflict(
        OnConflict::columns([
            agent_action::Column::ExecutionId,
            agent_action::Column::IdempotencyKey,
        ])
        .do_nothing()
        .to_owned(),
    )
    .exec_without_returning(db)
    .await
    .context("failed to persist idempotent agent action")?;
    let action = agent_action::Entity::find()
        .filter(agent_action::Column::ExecutionId.eq(input.execution_id.clone()))
        .filter(agent_action::Column::IdempotencyKey.eq(input.idempotency_key.clone()))
        .one(db)
        .await
        .context("failed to reload agent action")?
        .context("agent action disappeared after idempotent insert")?;
    if action.id != input.id
        || action.action_kind != input.action_kind
        || action.request_fingerprint != input.request_fingerprint
    {
        bail!("idempotency key was reused with different immutable action facts");
    }
    Ok(action)
}

pub async fn load_agent_action<C: ConnectionTrait>(
    db: &C,
    action_id: &str,
) -> Result<Option<agent_action::Model>> {
    agent_action::Entity::find_by_id(action_id.to_owned())
        .one(db)
        .await
        .context("failed to load agent action")
}

pub async fn load_agent_action_receipt<C: ConnectionTrait>(
    db: &C,
    action_id: &str,
) -> Result<Option<agent_action_receipt::Model>> {
    agent_action_receipt::Entity::find()
        .filter(agent_action_receipt::Column::ActionId.eq(action_id.to_owned()))
        .one(db)
        .await
        .context("failed to load agent action receipt")
}

pub async fn load_agent_action_outbox<C: ConnectionTrait>(
    db: &C,
    action_id: &str,
) -> Result<Option<agent_action_outbox::Model>> {
    agent_action_outbox::Entity::find()
        .filter(agent_action_outbox::Column::ActionId.eq(action_id.to_owned()))
        .one(db)
        .await
        .context("failed to load agent action outbox")
}

pub fn safe_route_provenance_from_receipt(
    json: &str,
    expected_action_kind: &str,
) -> Result<SafeRouteProvenance> {
    let value: serde_json::Value =
        serde_json::from_str(json).context("persisted agent action route receipt is invalid")?;
    let route_id = value
        .get("routeId")
        .and_then(serde_json::Value::as_str)
        .context("persisted agent action route receipt has no route id")?;
    if route_id.trim().is_empty() {
        bail!("persisted agent action route receipt has an empty route id");
    }
    for generation in [
        "routeGeneration",
        "sourcePolicyGeneration",
        "destinationPolicyGeneration",
    ] {
        if value
            .get(generation)
            .and_then(serde_json::Value::as_u64)
            .is_none_or(|generation| generation == 0)
        {
            bail!("persisted agent action route receipt has invalid `{generation}`");
        }
    }
    let action_name = value
        .get("action")
        .and_then(serde_json::Value::as_str)
        .context("persisted agent action route receipt has no action")?;
    if action_name != expected_action_kind {
        bail!("persisted agent action route receipt differs from its action");
    }
    let action = match action_name {
        "send_message" => AgentRouteAction::SendMessage,
        "start_agent" => AgentRouteAction::StartAgent,
        "create_task" => AgentRouteAction::CreateTask,
        "schedule_task" => AgentRouteAction::ScheduleTask,
        "review_task_result" => AgentRouteAction::ReviewTaskResult,
        "deliver_result" => AgentRouteAction::DeliverResult,
        action => bail!("persisted agent action route receipt has unsupported action `{action}`"),
    };
    Ok(SafeRouteProvenance::delegated_action(action))
}

#[cfg(test)]
mod tests;
