use anyhow::{Context, Result};
use pioneer_entity::agent_memory_policy_decision;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::DB_ID_LEN;
use crate::memory::{NewAgentMemoryPolicyDecision, actor_id_to_db, actor_kind_to_db};
use crate::util::unix_to_datetime;

pub async fn insert_policy_decision<C: ConnectionTrait>(
    db: &C,
    decision: NewAgentMemoryPolicyDecision,
) -> Result<agent_memory_policy_decision::Model> {
    let id = pioneer_protocol::generate_id(DB_ID_LEN);
    agent_memory_policy_decision::Entity::insert(agent_memory_policy_decision::ActiveModel {
        id: Set(id.clone()),
        memory_id: Set(decision.memory_id),
        candidate_id: Set(decision.candidate_id),
        workspace_id: Set(decision.workspace_id),
        action: Set(decision.action.clone()),
        decision: Set(decision.decision),
        reason_code: Set(decision.reason_code),
        reason: Set(decision.reason),
        policy_version: Set(decision.policy_version),
        actor_kind: Set(actor_kind_to_db(&decision.actor)),
        actor_id: Set(actor_id_to_db(&decision.actor)),
        thread_id: Set(decision.thread_id),
        turn_id: Set(decision.turn_id),
        item_id: Set(decision.item_id),
        details_json: Set(decision.details_json),
        created_at: Set(unix_to_datetime(decision.created_at_unix)),
    })
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to insert memory policy decision `{}`",
            decision.action
        )
    })?;

    agent_memory_policy_decision::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload memory policy decision")?
        .context("inserted memory policy decision missing")
}

pub async fn list_policy_decisions_for_memory<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    limit: u64,
) -> Result<Vec<agent_memory_policy_decision::Model>> {
    agent_memory_policy_decision::Entity::find()
        .filter(agent_memory_policy_decision::Column::MemoryId.eq(memory_id.to_owned()))
        .order_by_desc(agent_memory_policy_decision::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to list memory policy decisions for `{memory_id}`"))
}

pub async fn list_policy_decisions_for_candidate<C: ConnectionTrait>(
    db: &C,
    candidate_id: &str,
    limit: u64,
) -> Result<Vec<agent_memory_policy_decision::Model>> {
    agent_memory_policy_decision::Entity::find()
        .filter(agent_memory_policy_decision::Column::CandidateId.eq(candidate_id.to_owned()))
        .order_by_desc(agent_memory_policy_decision::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to list memory policy decisions for `{candidate_id}`"))
}

pub async fn list_policy_decisions_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<agent_memory_policy_decision::Model>> {
    agent_memory_policy_decision::Entity::find()
        .filter(agent_memory_policy_decision::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_desc(agent_memory_policy_decision::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to list memory policy decisions for thread `{thread_id}`"))
}
