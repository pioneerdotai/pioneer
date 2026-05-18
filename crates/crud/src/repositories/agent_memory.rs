use anyhow::{Context, Result};
use pioneer_entity::agent_memory;
use pioneer_protocol::{MemoryScopeKind, MemoryStatus};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Expr, OnConflict};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, ExprTrait, QueryFilter, QueryOrder,
    QuerySelect, Set,
};

use crate::convention::{
    DB_ID_LEN, MEMORY_REPAIR_STATUS_OK, memory_category_to_db, memory_scope_kind_to_db,
    memory_sensitivity_to_db, memory_source_context_kind_to_db, memory_source_kind_to_db,
    memory_status_to_db,
};
use crate::memory::{
    AgentMemoryListFilter, MemoryScopeResolution, MemoryWorkspaceGuard,
    NewAgentMemoryControlRecord, actor_id_to_db, actor_kind_to_db, normalized_memory_key,
    normalized_memory_namespace, normalized_optional_memory_key,
};
use crate::util::unix_to_datetime;

pub async fn insert_memory_record<C: ConnectionTrait>(
    db: &C,
    record: NewAgentMemoryControlRecord,
    resolved_scope: MemoryScopeResolution,
    now: DateTimeWithTimeZone,
) -> Result<agent_memory::Model> {
    let id = record
        .id
        .clone()
        .unwrap_or_else(|| pioneer_protocol::generate_id(DB_ID_LEN));
    let namespace = normalized_memory_namespace(record.namespace.as_deref())?;
    let key = normalized_optional_memory_key(record.key.clone())?;
    let active_key = key.clone();

    agent_memory::Entity::insert(agent_memory::ActiveModel {
        id: Set(id.clone()),
        scope_kind: Set(memory_scope_kind_to_db(resolved_scope.scope.kind).to_owned()),
        scope_key: Set(resolved_scope.scope.key),
        scope_key_hash: Set(resolved_scope.scope_key_hash),
        workspace_id: Set(resolved_scope.workspace_id),
        namespace: Set(namespace),
        category: Set(memory_category_to_db(record.category).to_owned()),
        key: Set(key),
        active_key: Set(active_key),
        status: Set(memory_status_to_db(MemoryStatus::Active).to_owned()),
        sensitivity: Set(memory_sensitivity_to_db(record.sensitivity).to_owned()),
        confidence: Set(record.confidence),
        importance: Set(record.importance),
        content_preview: Set(record.content_preview),
        capsule_id: Set(record.capsule_id),
        capsule_ref: Set(record.capsule_ref),
        frame_id: Set(record.frame_id),
        frame_uri: Set(record.frame_uri),
        frame_version: Set(record.frame_version),
        source_kind: Set(memory_source_kind_to_db(record.source_kind).to_owned()),
        source_context_kind: Set(record
            .source_context_kind
            .map(memory_source_context_kind_to_db)
            .map(str::to_owned)),
        source_thread_id: Set(record.source_thread_id),
        source_turn_id: Set(record.source_turn_id),
        source_item_id: Set(record.source_item_id),
        created_by_kind: Set(actor_kind_to_db(&record.created_by)),
        created_by_id: Set(actor_id_to_db(&record.created_by)),
        created_at: Set(now),
        updated_at: Set(now),
        last_accessed_at: Set(None),
        access_count: Set(0),
        expires_at: Set(record.expires_at_unix.map(unix_to_datetime)),
        superseded_by: Set(None),
        deleted_at: Set(None),
        deleted_by_kind: Set(None),
        deleted_by_id: Set(None),
        delete_reason: Set(None),
        policy_version: Set(record.policy_version),
        repair_status: Set(MEMORY_REPAIR_STATUS_OK.to_owned()),
        metadata_json: Set(record.metadata_json),
    })
    .exec(db)
    .await
    .with_context(|| format!("failed to insert agent memory record `{id}`"))?;

    agent_memory::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload inserted agent memory record")?
        .context("inserted agent memory record missing")
}

pub async fn upsert_active_memory_record<C: ConnectionTrait>(
    db: &C,
    record: NewAgentMemoryControlRecord,
    resolved_scope: MemoryScopeResolution,
    now: DateTimeWithTimeZone,
) -> Result<agent_memory::Model> {
    let id = record
        .id
        .clone()
        .unwrap_or_else(|| pioneer_protocol::generate_id(DB_ID_LEN));
    let namespace = normalized_memory_namespace(record.namespace.as_deref())?;
    let key = normalized_optional_memory_key(record.key.clone())?;
    let active_key = key.clone();
    let scope_kind = memory_scope_kind_to_db(resolved_scope.scope.kind).to_owned();
    let scope_key_hash = resolved_scope.scope_key_hash.clone();

    agent_memory::Entity::insert(agent_memory::ActiveModel {
        id: Set(id.clone()),
        scope_kind: Set(scope_kind.clone()),
        scope_key: Set(resolved_scope.scope.key),
        scope_key_hash: Set(scope_key_hash.clone()),
        workspace_id: Set(resolved_scope.workspace_id),
        namespace: Set(namespace.clone()),
        category: Set(memory_category_to_db(record.category).to_owned()),
        key: Set(key.clone()),
        active_key: Set(active_key),
        status: Set(memory_status_to_db(MemoryStatus::Active).to_owned()),
        sensitivity: Set(memory_sensitivity_to_db(record.sensitivity).to_owned()),
        confidence: Set(record.confidence),
        importance: Set(record.importance),
        content_preview: Set(record.content_preview),
        capsule_id: Set(record.capsule_id),
        capsule_ref: Set(record.capsule_ref),
        frame_id: Set(record.frame_id),
        frame_uri: Set(record.frame_uri),
        frame_version: Set(record.frame_version),
        source_kind: Set(memory_source_kind_to_db(record.source_kind).to_owned()),
        source_context_kind: Set(record
            .source_context_kind
            .map(memory_source_context_kind_to_db)
            .map(str::to_owned)),
        source_thread_id: Set(record.source_thread_id),
        source_turn_id: Set(record.source_turn_id),
        source_item_id: Set(record.source_item_id),
        created_by_kind: Set(actor_kind_to_db(&record.created_by)),
        created_by_id: Set(actor_id_to_db(&record.created_by)),
        created_at: Set(now),
        updated_at: Set(now),
        last_accessed_at: Set(None),
        access_count: Set(0),
        expires_at: Set(record.expires_at_unix.map(unix_to_datetime)),
        superseded_by: Set(None),
        deleted_at: Set(None),
        deleted_by_kind: Set(None),
        deleted_by_id: Set(None),
        delete_reason: Set(None),
        policy_version: Set(record.policy_version),
        repair_status: Set(MEMORY_REPAIR_STATUS_OK.to_owned()),
        metadata_json: Set(record.metadata_json),
    })
    .on_conflict(
        OnConflict::columns([
            agent_memory::Column::ScopeKind,
            agent_memory::Column::ScopeKeyHash,
            agent_memory::Column::Namespace,
            agent_memory::Column::ActiveKey,
        ])
        .update_columns([
            agent_memory::Column::WorkspaceId,
            agent_memory::Column::Category,
            agent_memory::Column::Key,
            agent_memory::Column::Status,
            agent_memory::Column::Sensitivity,
            agent_memory::Column::Confidence,
            agent_memory::Column::Importance,
            agent_memory::Column::ContentPreview,
            agent_memory::Column::CapsuleId,
            agent_memory::Column::CapsuleRef,
            agent_memory::Column::FrameId,
            agent_memory::Column::FrameUri,
            agent_memory::Column::FrameVersion,
            agent_memory::Column::SourceKind,
            agent_memory::Column::SourceContextKind,
            agent_memory::Column::SourceThreadId,
            agent_memory::Column::SourceTurnId,
            agent_memory::Column::SourceItemId,
            agent_memory::Column::CreatedByKind,
            agent_memory::Column::CreatedById,
            agent_memory::Column::UpdatedAt,
            agent_memory::Column::ExpiresAt,
            agent_memory::Column::PolicyVersion,
            agent_memory::Column::RepairStatus,
            agent_memory::Column::MetadataJson,
        ])
        .to_owned(),
    )
    .exec(db)
    .await
    .with_context(|| {
        format!(
            "failed to upsert agent memory record for `{}/{}`",
            scope_kind, scope_key_hash
        )
    })?;

    if let Some(key) = key {
        return agent_memory::Entity::find()
            .filter(agent_memory::Column::ScopeKind.eq(scope_kind))
            .filter(agent_memory::Column::ScopeKeyHash.eq(scope_key_hash))
            .filter(agent_memory::Column::Namespace.eq(namespace))
            .filter(agent_memory::Column::ActiveKey.eq(key))
            .one(db)
            .await
            .context("failed to reload upserted active agent memory record")?
            .context("upserted active agent memory record missing");
    }

    agent_memory::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload upserted keyless agent memory record")?
        .context("upserted keyless agent memory record missing")
}

pub async fn find_memory_by_id<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    include_non_active: bool,
) -> Result<Option<agent_memory::Model>> {
    let mut query = agent_memory::Entity::find().filter(agent_memory::Column::Id.eq(memory_id));
    if !include_non_active {
        query = query
            .filter(agent_memory::Column::Status.eq(memory_status_to_db(MemoryStatus::Active)));
    }
    query
        .one(db)
        .await
        .with_context(|| format!("failed to find agent memory `{memory_id}`"))
}

pub async fn find_active_memory_by_scoped_key<C: ConnectionTrait>(
    db: &C,
    resolved_scope: &MemoryScopeResolution,
    namespace: &str,
    key: &str,
) -> Result<Option<agent_memory::Model>> {
    let namespace = normalized_memory_namespace(Some(namespace))?;
    let key = normalized_memory_key(key)?;
    agent_memory::Entity::find()
        .filter(
            agent_memory::Column::ScopeKind.eq(memory_scope_kind_to_db(resolved_scope.scope.kind)),
        )
        .filter(agent_memory::Column::ScopeKeyHash.eq(resolved_scope.scope_key_hash.clone()))
        .filter(agent_memory::Column::Namespace.eq(namespace))
        .filter(agent_memory::Column::ActiveKey.eq(key.clone()))
        .filter(agent_memory::Column::Status.eq(memory_status_to_db(MemoryStatus::Active)))
        .one(db)
        .await
        .with_context(|| format!("failed to find active memory by key `{key}`"))
}

pub async fn update_memory_metadata<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    metadata_json: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<Option<agent_memory::Model>> {
    let affected = agent_memory::Entity::update_many()
        .col_expr(
            agent_memory::Column::MetadataJson,
            Expr::value(metadata_json),
        )
        .col_expr(agent_memory::Column::UpdatedAt, Expr::value(now))
        .filter(agent_memory::Column::Id.eq(memory_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to update agent memory `{memory_id}` metadata"))?
        .rows_affected;
    if affected == 0 {
        return Ok(None);
    }
    find_memory_by_id(db, memory_id, true).await
}

pub async fn list_memory_records<C: ConnectionTrait>(
    db: &C,
    filter: AgentMemoryListFilter,
    resolved_scopes: Vec<MemoryScopeResolution>,
    now: DateTimeWithTimeZone,
) -> Result<Vec<agent_memory::Model>> {
    let mut query = agent_memory::Entity::find();

    if !resolved_scopes.is_empty() {
        query = query.filter(scope_condition(&resolved_scopes));
    }

    if let Some(guard) = &filter.workspace_guard {
        query = query.filter(workspace_guard_condition(guard));
    }

    let statuses = if filter.statuses.is_empty() {
        vec![memory_status_to_db(MemoryStatus::Active).to_owned()]
    } else {
        filter
            .statuses
            .iter()
            .map(|status| memory_status_to_db(*status).to_owned())
            .collect::<Vec<_>>()
    };
    query = query.filter(agent_memory::Column::Status.is_in(statuses));

    if let Some(namespace) = filter.namespace {
        let namespace = normalized_memory_namespace(Some(namespace.as_str()))?;
        query = query.filter(agent_memory::Column::Namespace.eq(namespace));
    }

    if !filter.categories.is_empty() {
        query = query.filter(
            agent_memory::Column::Category.is_in(
                filter
                    .categories
                    .iter()
                    .map(|category| memory_category_to_db(*category).to_owned()),
            ),
        );
    }

    if !filter.include_deleted {
        query = query
            .filter(agent_memory::Column::Status.ne(memory_status_to_db(MemoryStatus::Deleted)));
    }
    if !filter.include_superseded {
        query = query
            .filter(agent_memory::Column::Status.ne(memory_status_to_db(MemoryStatus::Superseded)));
    }
    if !filter.include_expired {
        query = query
            .filter(agent_memory::Column::Status.ne(memory_status_to_db(MemoryStatus::Expired)))
            .filter(
                Condition::any()
                    .add(agent_memory::Column::ExpiresAt.is_null())
                    .add(agent_memory::Column::ExpiresAt.gt(now)),
            );
    }

    query = query.filter(agent_memory::Column::RepairStatus.eq(MEMORY_REPAIR_STATUS_OK));

    if let Some(limit) = filter.limit {
        query = query.limit(limit);
    }

    query
        .order_by_desc(agent_memory::Column::UpdatedAt)
        .all(db)
        .await
        .context("failed to list agent memory records")
}

pub async fn mark_memory_deleted<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    actor: Option<crate::memory::MemoryActorRecord>,
    reason: Option<String>,
    now: DateTimeWithTimeZone,
) -> Result<Option<agent_memory::Model>> {
    agent_memory::Entity::update_many()
        .col_expr(
            agent_memory::Column::Status,
            Expr::value(memory_status_to_db(MemoryStatus::Deleted).to_owned()),
        )
        .col_expr(
            agent_memory::Column::ActiveKey,
            Expr::value(Option::<String>::None),
        )
        .col_expr(agent_memory::Column::DeletedAt, Expr::value(Some(now)))
        .col_expr(
            agent_memory::Column::DeletedByKind,
            Expr::value(crate::memory::actor_kind_to_db(&actor)),
        )
        .col_expr(
            agent_memory::Column::DeletedById,
            Expr::value(crate::memory::actor_id_to_db(&actor)),
        )
        .col_expr(agent_memory::Column::DeleteReason, Expr::value(reason))
        .col_expr(agent_memory::Column::UpdatedAt, Expr::value(now))
        .filter(agent_memory::Column::Id.eq(memory_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark agent memory `{memory_id}` deleted"))?;
    find_memory_by_id(db, memory_id, true).await
}

pub async fn mark_memory_superseded<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    superseded_by: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<agent_memory::Model>> {
    agent_memory::Entity::update_many()
        .col_expr(
            agent_memory::Column::Status,
            Expr::value(memory_status_to_db(MemoryStatus::Superseded).to_owned()),
        )
        .col_expr(
            agent_memory::Column::ActiveKey,
            Expr::value(Option::<String>::None),
        )
        .col_expr(
            agent_memory::Column::SupersededBy,
            Expr::value(Some(superseded_by.to_owned())),
        )
        .col_expr(agent_memory::Column::UpdatedAt, Expr::value(now))
        .filter(agent_memory::Column::Id.eq(memory_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark agent memory `{memory_id}` superseded"))?;
    find_memory_by_id(db, memory_id, true).await
}

pub async fn mark_memory_expired<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<agent_memory::Model>> {
    agent_memory::Entity::update_many()
        .col_expr(
            agent_memory::Column::Status,
            Expr::value(memory_status_to_db(MemoryStatus::Expired).to_owned()),
        )
        .col_expr(
            agent_memory::Column::ActiveKey,
            Expr::value(Option::<String>::None),
        )
        .col_expr(agent_memory::Column::UpdatedAt, Expr::value(now))
        .filter(agent_memory::Column::Id.eq(memory_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark agent memory `{memory_id}` expired"))?;
    find_memory_by_id(db, memory_id, true).await
}

pub async fn increment_memory_access<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    now: DateTimeWithTimeZone,
) -> Result<bool> {
    let updated = agent_memory::Entity::update_many()
        .col_expr(
            agent_memory::Column::AccessCount,
            Expr::col(agent_memory::Column::AccessCount).add(1),
        )
        .col_expr(agent_memory::Column::LastAccessedAt, Expr::value(Some(now)))
        .col_expr(agent_memory::Column::UpdatedAt, Expr::value(now))
        .filter(agent_memory::Column::Id.eq(memory_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to increment access for agent memory `{memory_id}`"))?
        .rows_affected
        > 0;
    Ok(updated)
}

pub async fn mark_memory_repair_status<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    repair_status: &str,
    now: DateTimeWithTimeZone,
) -> Result<Option<agent_memory::Model>> {
    agent_memory::Entity::update_many()
        .col_expr(
            agent_memory::Column::RepairStatus,
            Expr::value(repair_status.to_owned()),
        )
        .col_expr(agent_memory::Column::UpdatedAt, Expr::value(now))
        .filter(agent_memory::Column::Id.eq(memory_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to mark agent memory `{memory_id}` repair status"))?;
    find_memory_by_id(db, memory_id, true).await
}

fn scope_condition(scopes: &[MemoryScopeResolution]) -> Condition {
    let mut condition = Condition::any();
    for scope in scopes {
        condition = condition.add(
            Condition::all()
                .add(agent_memory::Column::ScopeKind.eq(memory_scope_kind_to_db(scope.scope.kind)))
                .add(agent_memory::Column::ScopeKeyHash.eq(scope.scope_key_hash.clone())),
        );
    }
    condition
}

fn workspace_guard_condition(guard: &MemoryWorkspaceGuard) -> Condition {
    Condition::any()
        .add(agent_memory::Column::WorkspaceId.eq(guard.workspace_id.clone()))
        .add_option(guard.allow_global_user.then(|| {
            Condition::all()
                .add(agent_memory::Column::WorkspaceId.is_null())
                .add(
                    agent_memory::Column::ScopeKind
                        .eq(memory_scope_kind_to_db(MemoryScopeKind::User)),
                )
        }))
        .add_option(guard.allow_global_agent.then(|| {
            Condition::all()
                .add(agent_memory::Column::WorkspaceId.is_null())
                .add(
                    agent_memory::Column::ScopeKind
                        .eq(memory_scope_kind_to_db(MemoryScopeKind::Agent)),
                )
        }))
}
