use anyhow::{Context, Result};
use pioneer_entity::agent_memory_quarantine;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::DB_ID_LEN;
use crate::memory::{
    NewAgentMemoryQuarantine, ResolveAgentMemoryQuarantine, lifecycle_actor_id_to_db,
    lifecycle_actor_kind_to_db,
};
use crate::{convention::memory_lifecycle_reason_code_to_db, util::unix_to_datetime};

const QUARANTINE_DETAILS_JSON_MAX_CHARS: usize = 4096;

pub async fn create_active_quarantine<C: ConnectionTrait>(
    db: &C,
    quarantine: NewAgentMemoryQuarantine,
) -> Result<agent_memory_quarantine::Model> {
    if let Some(existing) =
        find_active_quarantine_by_memory(db, quarantine.memory_id.as_str()).await?
    {
        return Ok(existing);
    }

    let id = quarantine
        .id
        .clone()
        .unwrap_or_else(|| pioneer_protocol::generate_id(DB_ID_LEN));
    agent_memory_quarantine::Entity::insert(agent_memory_quarantine::ActiveModel {
        id: Set(id.clone()),
        memory_id: Set(quarantine.memory_id.clone()),
        workspace_id: Set(quarantine.workspace_id),
        reason_code: Set(memory_lifecycle_reason_code_to_db(quarantine.reason_code)),
        actor_kind: Set(lifecycle_actor_kind_to_db(&quarantine.actor)),
        actor_id: Set(lifecycle_actor_id_to_db(&quarantine.actor)),
        created_at: Set(Some(unix_to_datetime(quarantine.created_at_unix))),
        resolved_at: Set(None),
        resolved_reason_code: Set(None),
        resolved_actor_kind: Set(None),
        resolved_actor_id: Set(None),
        details_json: Set(bounded_details_json(quarantine.details_json)),
    })
    .exec(db)
    .await
    .with_context(|| format!("failed to create quarantine marker `{id}`"))?;

    agent_memory_quarantine::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload quarantine marker")?
        .context("inserted quarantine marker missing")
}

fn bounded_details_json(details_json: Option<String>) -> Option<String> {
    let details_json = details_json?;
    if details_json.chars().count() <= QUARANTINE_DETAILS_JSON_MAX_CHARS {
        return Some(details_json);
    }
    Some(serde_json::json!({ "truncated": true }).to_string())
}

pub async fn find_active_quarantine_by_memory<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
) -> Result<Option<agent_memory_quarantine::Model>> {
    agent_memory_quarantine::Entity::find()
        .filter(agent_memory_quarantine::Column::MemoryId.eq(memory_id.to_owned()))
        .filter(agent_memory_quarantine::Column::ResolvedAt.is_null())
        .order_by_desc(agent_memory_quarantine::Column::CreatedAt)
        .one(db)
        .await
        .with_context(|| format!("failed to find active quarantine for memory `{memory_id}`"))
}

pub async fn list_active_quarantines_by_memory_ids<C: ConnectionTrait>(
    db: &C,
    memory_ids: &[String],
) -> Result<Vec<agent_memory_quarantine::Model>> {
    if memory_ids.is_empty() {
        return Ok(Vec::new());
    }
    agent_memory_quarantine::Entity::find()
        .filter(agent_memory_quarantine::Column::MemoryId.is_in(memory_ids.to_vec()))
        .filter(agent_memory_quarantine::Column::ResolvedAt.is_null())
        .all(db)
        .await
        .context("failed to list active memory quarantines")
}

pub async fn resolve_active_quarantine<C: ConnectionTrait>(
    db: &C,
    resolution: ResolveAgentMemoryQuarantine,
) -> Result<Option<agent_memory_quarantine::Model>> {
    let resolved_at: DateTimeWithTimeZone = unix_to_datetime(resolution.resolved_at_unix);
    let affected = agent_memory_quarantine::Entity::update_many()
        .col_expr(
            agent_memory_quarantine::Column::ResolvedAt,
            Expr::value(Some(resolved_at)),
        )
        .col_expr(
            agent_memory_quarantine::Column::ResolvedReasonCode,
            Expr::value(Some(memory_lifecycle_reason_code_to_db(
                resolution.reason_code,
            ))),
        )
        .col_expr(
            agent_memory_quarantine::Column::ResolvedActorKind,
            Expr::value(Some(lifecycle_actor_kind_to_db(&resolution.actor))),
        )
        .col_expr(
            agent_memory_quarantine::Column::ResolvedActorId,
            Expr::value(lifecycle_actor_id_to_db(&resolution.actor)),
        )
        .filter(agent_memory_quarantine::Column::MemoryId.eq(resolution.memory_id.clone()))
        .filter(agent_memory_quarantine::Column::ResolvedAt.is_null())
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to resolve active quarantine for memory `{}`",
                resolution.memory_id
            )
        })?
        .rows_affected;

    if affected == 0 {
        return Ok(None);
    }

    list_quarantine_history_for_memory(db, resolution.memory_id.as_str(), 1)
        .await
        .map(|mut rows| rows.pop())
}

pub async fn list_quarantine_history_for_memory<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    limit: u64,
) -> Result<Vec<agent_memory_quarantine::Model>> {
    agent_memory_quarantine::Entity::find()
        .filter(agent_memory_quarantine::Column::MemoryId.eq(memory_id.to_owned()))
        .order_by_desc(agent_memory_quarantine::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to list quarantine history for memory `{memory_id}`"))
}
