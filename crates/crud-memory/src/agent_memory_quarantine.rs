use anyhow::{Context, Result};
use pioneer_entity::agent_memory_quarantine;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::Expr;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::DB_ID_LEN;
use crate::memory::{
    NewAgentMemoryEvent, NewAgentMemoryQuarantine, ResolveAgentMemoryQuarantine,
    lifecycle_actor_id_to_db, lifecycle_actor_kind_to_db,
};
use crate::{convention::memory_lifecycle_reason_code_to_db, util::unix_to_datetime};

const QUARANTINE_DETAILS_JSON_MAX_CHARS: usize = 4096;

#[derive(Clone, Debug)]
pub struct PreparedAgentMemoryQuarantine {
    memory_id: String,
    id: String,
    active_model: agent_memory_quarantine::ActiveModel,
    event: NewAgentMemoryEvent,
}

#[derive(Clone, Debug)]
pub struct PreparedResolveAgentMemoryQuarantine {
    memory_id: String,
    expected_quarantine_id: Option<String>,
    resolved_at: DateTimeWithTimeZone,
    reason_code: String,
    actor_kind: String,
    actor_id: Option<String>,
}

pub fn prepare_active_quarantine(
    quarantine: NewAgentMemoryQuarantine,
) -> PreparedAgentMemoryQuarantine {
    let id = quarantine
        .id
        .clone()
        .unwrap_or_else(|| pioneer_protocol::generate_id(DB_ID_LEN));
    let memory_id = quarantine.memory_id.clone();
    let reason_code = memory_lifecycle_reason_code_to_db(quarantine.reason_code);
    let actor_kind = lifecycle_actor_kind_to_db(&quarantine.actor);
    let actor_id = lifecycle_actor_id_to_db(&quarantine.actor);
    let event = NewAgentMemoryEvent {
        memory_id: Some(memory_id.clone()),
        candidate_id: None,
        workspace_id: quarantine.workspace_id.clone(),
        event_kind: crate::convention::MEMORY_EVENT_QUARANTINED.to_owned(),
        actor: None,
        thread_id: None,
        turn_id: None,
        item_id: None,
        details_json: Some(
            serde_json::json!({
                "quarantine_id": id.clone(),
                "reason_code": reason_code.clone(),
                "actor": {
                    "kind": actor_kind.clone(),
                    "id": actor_id.clone(),
                }
            })
            .to_string(),
        ),
        created_at_unix: quarantine.created_at_unix,
    };
    let active_model = agent_memory_quarantine::ActiveModel {
        id: Set(id.clone()),
        memory_id: Set(memory_id.clone()),
        workspace_id: Set(quarantine.workspace_id),
        reason_code: Set(reason_code),
        actor_kind: Set(actor_kind),
        actor_id: Set(actor_id),
        created_at: Set(Some(unix_to_datetime(quarantine.created_at_unix))),
        resolved_at: Set(None),
        resolved_reason_code: Set(None),
        resolved_actor_kind: Set(None),
        resolved_actor_id: Set(None),
        details_json: Set(bounded_details_json(quarantine.details_json)),
    };
    PreparedAgentMemoryQuarantine {
        memory_id,
        id,
        active_model,
        event,
    }
}

impl PreparedAgentMemoryQuarantine {
    pub(crate) fn event(&self) -> NewAgentMemoryEvent {
        self.event.clone()
    }
}

pub(crate) async fn prepare_quarantine_resolution<C: ConnectionTrait>(
    db: &C,
    resolution: ResolveAgentMemoryQuarantine,
) -> Result<(
    PreparedResolveAgentMemoryQuarantine,
    Option<NewAgentMemoryEvent>,
)> {
    let current = find_active_quarantine_by_memory(db, resolution.memory_id.as_str()).await?;
    let prepared = PreparedResolveAgentMemoryQuarantine {
        memory_id: resolution.memory_id,
        expected_quarantine_id: current.as_ref().map(|row| row.id.clone()),
        resolved_at: unix_to_datetime(resolution.resolved_at_unix),
        reason_code: memory_lifecycle_reason_code_to_db(resolution.reason_code),
        actor_kind: lifecycle_actor_kind_to_db(&resolution.actor),
        actor_id: lifecycle_actor_id_to_db(&resolution.actor),
    };
    let event = current.map(|row| NewAgentMemoryEvent {
        memory_id: Some(row.memory_id),
        candidate_id: None,
        workspace_id: row.workspace_id,
        event_kind: crate::convention::MEMORY_EVENT_RESTORED.to_owned(),
        actor: None,
        thread_id: None,
        turn_id: None,
        item_id: None,
        details_json: Some(
            serde_json::json!({
                "quarantine_id": row.id,
                "reason_code": prepared.reason_code.clone(),
                "actor": {
                    "kind": prepared.actor_kind.clone(),
                    "id": prepared.actor_id.clone(),
                }
            })
            .to_string(),
        ),
        created_at_unix: resolution.resolved_at_unix,
    });
    Ok((prepared, event))
}

pub async fn create_active_quarantine<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedAgentMemoryQuarantine,
) -> Result<agent_memory_quarantine::Model> {
    if let Some(existing) =
        find_active_quarantine_by_memory(db, prepared.memory_id.as_str()).await?
    {
        return Ok(existing);
    }

    let id = prepared.id;
    agent_memory_quarantine::Entity::insert(prepared.active_model)
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
    prepared: PreparedResolveAgentMemoryQuarantine,
) -> Result<Option<agent_memory_quarantine::Model>> {
    let Some(expected_quarantine_id) = prepared.expected_quarantine_id.clone() else {
        return Ok(None);
    };
    let affected = agent_memory_quarantine::Entity::update_many()
        .col_expr(
            agent_memory_quarantine::Column::ResolvedAt,
            Expr::value(Some(prepared.resolved_at)),
        )
        .col_expr(
            agent_memory_quarantine::Column::ResolvedReasonCode,
            Expr::value(Some(prepared.reason_code)),
        )
        .col_expr(
            agent_memory_quarantine::Column::ResolvedActorKind,
            Expr::value(Some(prepared.actor_kind)),
        )
        .col_expr(
            agent_memory_quarantine::Column::ResolvedActorId,
            Expr::value(prepared.actor_id),
        )
        .filter(agent_memory_quarantine::Column::MemoryId.eq(prepared.memory_id.clone()))
        .filter(agent_memory_quarantine::Column::Id.eq(expected_quarantine_id))
        .filter(agent_memory_quarantine::Column::ResolvedAt.is_null())
        .exec(db)
        .await
        .with_context(|| {
            format!(
                "failed to resolve active quarantine for memory `{}`",
                prepared.memory_id
            )
        })?
        .rows_affected;

    if affected == 0 {
        return Ok(None);
    }

    list_quarantine_history_for_memory(db, prepared.memory_id.as_str(), 1)
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
