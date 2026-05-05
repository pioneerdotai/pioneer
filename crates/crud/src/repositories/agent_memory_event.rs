use anyhow::{Context, Result};
use pioneer_entity::agent_memory_event;
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::DB_ID_LEN;
use crate::memory::{NewAgentMemoryEvent, actor_id_to_db, actor_kind_to_db};
use crate::util::unix_to_datetime;

pub async fn append_memory_event<C: ConnectionTrait>(
    db: &C,
    event: NewAgentMemoryEvent,
) -> Result<agent_memory_event::Model> {
    let id = pioneer_protocol::generate_id(DB_ID_LEN);
    agent_memory_event::Entity::insert(agent_memory_event::ActiveModel {
        id: Set(id.clone()),
        memory_id: Set(event.memory_id),
        candidate_id: Set(event.candidate_id),
        workspace_id: Set(event.workspace_id),
        event_kind: Set(event.event_kind.clone()),
        actor_kind: Set(actor_kind_to_db(&event.actor)),
        actor_id: Set(actor_id_to_db(&event.actor)),
        thread_id: Set(event.thread_id),
        turn_id: Set(event.turn_id),
        item_id: Set(event.item_id),
        details_json: Set(event.details_json),
        created_at: Set(unix_to_datetime(event.created_at_unix)),
    })
    .exec(db)
    .await
    .with_context(|| format!("failed to append memory event `{}`", event.event_kind))?;

    agent_memory_event::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload appended memory event")?
        .context("appended memory event row missing")
}

pub async fn list_memory_events<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    limit: u64,
) -> Result<Vec<agent_memory_event::Model>> {
    agent_memory_event::Entity::find()
        .filter(agent_memory_event::Column::MemoryId.eq(memory_id.to_owned()))
        .order_by_desc(agent_memory_event::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to list memory events for `{memory_id}`"))
}

pub async fn list_candidate_events<C: ConnectionTrait>(
    db: &C,
    candidate_id: &str,
    limit: u64,
) -> Result<Vec<agent_memory_event::Model>> {
    agent_memory_event::Entity::find()
        .filter(agent_memory_event::Column::CandidateId.eq(candidate_id.to_owned()))
        .order_by_desc(agent_memory_event::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to list memory candidate events for `{candidate_id}`"))
}

pub async fn list_workspace_memory_events<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    limit: u64,
) -> Result<Vec<agent_memory_event::Model>> {
    agent_memory_event::Entity::find()
        .filter(agent_memory_event::Column::WorkspaceId.eq(workspace_id.to_owned()))
        .order_by_desc(agent_memory_event::Column::CreatedAt)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| format!("failed to list workspace memory events for `{workspace_id}`"))
}
