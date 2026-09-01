use anyhow::{Context, Result};
use pioneer_entity::agent_memory_capsule;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, Set,
};

use crate::convention::{
    DB_ID_LEN, MEMORY_CAPSULE_STATUS_MISSING, MEMORY_CAPSULE_STATUS_REPAIR_NEEDED,
    MEMORY_REPAIR_STATUS_FAILED, MEMORY_REPAIR_STATUS_REPAIR_NEEDED, MEMORY_SCOPE_SLOT_PRIMARY,
    memory_scope_kind_to_db,
};
use crate::memory::{AgentMemoryCapsuleRecord, MemoryScopeResolution};

pub async fn upsert_capsule<C: ConnectionTrait>(
    db: &C,
    capsule: AgentMemoryCapsuleRecord,
    resolved_scope: MemoryScopeResolution,
    now: DateTimeWithTimeZone,
) -> Result<agent_memory_capsule::Model> {
    let id = capsule
        .id
        .clone()
        .unwrap_or_else(|| pioneer_protocol::generate_id(DB_ID_LEN));
    let scope_slot = capsule
        .scope_slot
        .clone()
        .unwrap_or_else(|| MEMORY_SCOPE_SLOT_PRIMARY.to_owned());

    agent_memory_capsule::Entity::insert(agent_memory_capsule::ActiveModel {
        id: Set(id.clone()),
        scope_kind: Set(memory_scope_kind_to_db(resolved_scope.scope.kind).to_owned()),
        scope_key: Set(resolved_scope.scope.key.clone()),
        scope_key_hash: Set(resolved_scope.scope_key_hash.clone()),
        workspace_id: Set(resolved_scope.workspace_id.clone()),
        scope_slot: Set(scope_slot.clone()),
        capsule_ref: Set(capsule.capsule_ref.clone()),
        storage_uri: Set(capsule.storage_uri),
        backend: Set(capsule.backend),
        format: Set(capsule.format),
        encrypted: Set(capsule.encrypted),
        status: Set(capsule.status),
        repair_status: Set(capsule.repair_status),
        content_hash: Set(capsule.content_hash),
        active_record_count: Set(capsule.active_record_count),
        last_compacted_at: Set(None),
        last_reindexed_at: Set(None),
        last_verified_at: Set(None),
        last_error: Set(capsule.last_error),
        metadata_json: Set(capsule.metadata_json),
        created_at: Set(now),
        updated_at: Set(now),
    })
    .on_conflict(
        OnConflict::columns([
            agent_memory_capsule::Column::ScopeKind,
            agent_memory_capsule::Column::ScopeKeyHash,
            agent_memory_capsule::Column::ScopeSlot,
        ])
        .update_columns([
            agent_memory_capsule::Column::WorkspaceId,
            agent_memory_capsule::Column::CapsuleRef,
            agent_memory_capsule::Column::StorageUri,
            agent_memory_capsule::Column::Backend,
            agent_memory_capsule::Column::Format,
            agent_memory_capsule::Column::Encrypted,
            agent_memory_capsule::Column::Status,
            agent_memory_capsule::Column::RepairStatus,
            agent_memory_capsule::Column::ContentHash,
            agent_memory_capsule::Column::ActiveRecordCount,
            agent_memory_capsule::Column::LastError,
            agent_memory_capsule::Column::MetadataJson,
            agent_memory_capsule::Column::UpdatedAt,
        ])
        .to_owned(),
    )
    .exec(db)
    .await
    .with_context(|| format!("failed to upsert memory capsule `{}`", capsule.capsule_ref))?;

    find_capsule_by_scope_slot(db, &resolved_scope, scope_slot.as_str())
        .await?
        .context("upserted memory capsule missing")
}

pub async fn find_primary_capsule<C: ConnectionTrait>(
    db: &C,
    resolved_scope: &MemoryScopeResolution,
) -> Result<Option<agent_memory_capsule::Model>> {
    find_capsule_by_scope_slot(db, resolved_scope, MEMORY_SCOPE_SLOT_PRIMARY).await
}

pub async fn find_capsule_by_ref<C: ConnectionTrait>(
    db: &C,
    capsule_ref: &str,
) -> Result<Option<agent_memory_capsule::Model>> {
    agent_memory_capsule::Entity::find()
        .filter(agent_memory_capsule::Column::CapsuleRef.eq(capsule_ref.to_owned()))
        .one(db)
        .await
        .with_context(|| format!("failed to find memory capsule by ref `{capsule_ref}`"))
}

pub async fn find_capsule_by_id<C: ConnectionTrait>(
    db: &C,
    capsule_id: &str,
) -> Result<Option<agent_memory_capsule::Model>> {
    agent_memory_capsule::Entity::find_by_id(capsule_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to find memory capsule `{capsule_id}`"))
}

pub async fn mark_capsule_repair_status<C: ConnectionTrait>(
    db: &C,
    capsule_id: &str,
    status: &str,
    last_error: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<Option<agent_memory_capsule::Model>> {
    agent_memory_capsule::Entity::update_many()
        .col_expr(
            agent_memory_capsule::Column::RepairStatus,
            Expr::value(status.to_owned()),
        )
        .col_expr(
            agent_memory_capsule::Column::LastError,
            Expr::value(last_error),
        )
        .col_expr(agent_memory_capsule::Column::UpdatedAt, Expr::value(now))
        .filter(agent_memory_capsule::Column::Id.eq(capsule_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark memory capsule `{capsule_id}` repair status"))?;

    agent_memory_capsule::Entity::find_by_id(capsule_id.to_owned())
        .one(db)
        .await
        .context("failed to reload memory capsule after repair status update")
}

pub async fn list_capsules_needing_repair<C: ConnectionTrait>(
    db: &C,
    workspace_id: Option<&str>,
    limit: u64,
) -> Result<Vec<agent_memory_capsule::Model>> {
    let mut query = agent_memory_capsule::Entity::find().filter(
        Condition::any()
            .add(agent_memory_capsule::Column::RepairStatus.eq(MEMORY_REPAIR_STATUS_REPAIR_NEEDED))
            .add(agent_memory_capsule::Column::RepairStatus.eq(MEMORY_REPAIR_STATUS_FAILED))
            .add(agent_memory_capsule::Column::Status.eq(MEMORY_CAPSULE_STATUS_MISSING))
            .add(agent_memory_capsule::Column::Status.eq(MEMORY_CAPSULE_STATUS_REPAIR_NEEDED)),
    );

    if let Some(workspace_id) = workspace_id {
        query = query.filter(agent_memory_capsule::Column::WorkspaceId.eq(workspace_id.to_owned()));
    }

    query
        .order_by_asc(agent_memory_capsule::Column::UpdatedAt)
        .limit(limit)
        .all(db)
        .await
        .context("failed to list memory capsules needing repair")
}

async fn find_capsule_by_scope_slot<C: ConnectionTrait>(
    db: &C,
    resolved_scope: &MemoryScopeResolution,
    scope_slot: &str,
) -> Result<Option<agent_memory_capsule::Model>> {
    agent_memory_capsule::Entity::find()
        .filter(
            agent_memory_capsule::Column::ScopeKind
                .eq(memory_scope_kind_to_db(resolved_scope.scope.kind)),
        )
        .filter(
            agent_memory_capsule::Column::ScopeKeyHash.eq(resolved_scope.scope_key_hash.clone()),
        )
        .filter(agent_memory_capsule::Column::ScopeSlot.eq(scope_slot.to_owned()))
        .one(db)
        .await
        .context("failed to find memory capsule by scope slot")
}
