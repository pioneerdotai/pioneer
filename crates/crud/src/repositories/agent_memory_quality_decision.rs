use anyhow::{Context, Result};
use pioneer_entity::agent_memory_quality_decision;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    DB_ID_LEN, memory_evidence_class_to_db, memory_fact_class_to_db, memory_lifetime_class_to_db,
    memory_ownership_class_to_db, memory_quality_action_to_db, memory_source_context_kind_to_db,
    memory_write_relation_to_db,
};
use crate::memory::NewAgentMemoryQualityDecision;
use crate::util::unix_to_datetime;

pub async fn insert_quality_decision<C: ConnectionTrait>(
    db: &C,
    decision: NewAgentMemoryQualityDecision,
) -> Result<agent_memory_quality_decision::Model> {
    let id = pioneer_protocol::generate_id(DB_ID_LEN);
    agent_memory_quality_decision::Entity::insert(agent_memory_quality_decision::ActiveModel {
        id: Set(id.clone()),
        workspace_id: Set(decision.workspace_id),
        thread_id: Set(decision.thread_id),
        turn_id: Set(decision.turn_id),
        item_id: Set(decision.item_id),
        task_id: Set(decision.task_id),
        memory_id: Set(decision.memory_id),
        candidate_id: Set(decision.candidate_id),
        canonical_key: Set(decision.canonical_key),
        action: Set(memory_quality_action_to_db(decision.action)),
        target_ownership: Set(memory_ownership_class_to_db(decision.target_ownership)),
        source_context_kind: Set(
            memory_source_context_kind_to_db(decision.source_context_kind).to_owned(),
        ),
        fact_class: Set(memory_fact_class_to_db(decision.fact_class)),
        lifetime_class: Set(memory_lifetime_class_to_db(decision.lifetime_class)),
        ownership_class: Set(memory_ownership_class_to_db(decision.ownership_class)),
        evidence_class: Set(memory_evidence_class_to_db(decision.evidence_class)),
        relation: Set(memory_write_relation_to_db(decision.relation)),
        reason_codes_json: Set(serde_json::to_string(&decision.reason_codes)?),
        input_snapshot_json: Set(decision.input_snapshot_json),
        created_at: Set(unix_to_datetime(decision.created_at_unix)),
        updated_at: Set(unix_to_datetime(decision.updated_at_unix)),
    })
    .exec(db)
    .await
    .with_context(|| format!("failed to insert memory quality decision `{}`", id.as_str()))?;

    agent_memory_quality_decision::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload memory quality decision")?
        .context("inserted memory quality decision missing")
}

pub async fn list_quality_decisions_for_memory<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    limit: u64,
) -> Result<Vec<agent_memory_quality_decision::Model>> {
    agent_memory_quality_decision::Entity::find()
        .filter(agent_memory_quality_decision::Column::MemoryId.eq(memory_id.to_owned()))
        .order_by_desc(agent_memory_quality_decision::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to list memory quality decisions for `{memory_id}`"))
}

pub async fn list_quality_decisions_for_candidate<C: ConnectionTrait>(
    db: &C,
    candidate_id: &str,
    limit: u64,
) -> Result<Vec<agent_memory_quality_decision::Model>> {
    agent_memory_quality_decision::Entity::find()
        .filter(agent_memory_quality_decision::Column::CandidateId.eq(candidate_id.to_owned()))
        .order_by_desc(agent_memory_quality_decision::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to list memory quality decisions for `{candidate_id}`"))
}

pub async fn list_quality_decisions_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<agent_memory_quality_decision::Model>> {
    agent_memory_quality_decision::Entity::find()
        .filter(agent_memory_quality_decision::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_desc(agent_memory_quality_decision::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list memory quality decisions for thread `{thread_id}`")
        })
}
