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
    memory_sensitivity_to_db, memory_source_context_kind_to_db, memory_status_to_db,
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
        payload_version: Set(record.payload_version),
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
        payload_version: Set(record.payload_version),
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
            agent_memory::Column::PayloadVersion,
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

enum MemoryListOrder {
    RecentUpdates,
    Inventory { before: Option<(i64, String)> },
}

pub async fn list_memory_records<C: ConnectionTrait>(
    db: &C,
    filter: AgentMemoryListFilter,
    resolved_scopes: Vec<MemoryScopeResolution>,
    now: DateTimeWithTimeZone,
) -> Result<Vec<agent_memory::Model>> {
    list_memory_records_ordered(
        db,
        filter,
        resolved_scopes,
        now,
        MemoryListOrder::RecentUpdates,
    )
    .await
}

/// Inventory uses immutable creation order, so updating a record while paging
/// does not move it across the continuation boundary.
pub async fn list_memory_record_page<C: ConnectionTrait>(
    db: &C,
    filter: AgentMemoryListFilter,
    resolved_scopes: Vec<MemoryScopeResolution>,
    now: DateTimeWithTimeZone,
    before: Option<(i64, String)>,
) -> Result<Vec<agent_memory::Model>> {
    list_memory_records_ordered(
        db,
        filter,
        resolved_scopes,
        now,
        MemoryListOrder::Inventory { before },
    )
    .await
}

fn memory_records_query(
    filter: AgentMemoryListFilter,
    resolved_scopes: Vec<MemoryScopeResolution>,
    now: DateTimeWithTimeZone,
) -> Result<sea_orm::Select<agent_memory::Entity>> {
    let mut query = agent_memory::Entity::find();

    if !resolved_scopes.is_empty() {
        query = query.filter(scope_condition(&resolved_scopes));
    }

    if let Some(guard) = &filter.workspace_guard {
        query = query.filter(workspace_guard_condition(guard));
    }
    if let Some(thread_ids) = filter.allowed_source_thread_ids {
        let mut access =
            Condition::any().add(agent_memory::Column::SourceThreadId.is_in(thread_ids));
        for scope in filter.owned_scopes {
            if matches!(scope.kind, MemoryScopeKind::Thread | MemoryScopeKind::Task) {
                access = access.add(
                    Condition::all()
                        .add(
                            agent_memory::Column::ScopeKind.eq(memory_scope_kind_to_db(scope.kind)),
                        )
                        .add(agent_memory::Column::ScopeKey.eq(scope.key)),
                );
            }
        }
        query = query.filter(access);
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

    if let Some(key) = filter.key {
        let key = normalized_memory_key(key.as_str())?;
        query = query.filter(agent_memory::Column::ActiveKey.eq(key));
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

    Ok(query)
}

async fn list_memory_records_ordered<C: ConnectionTrait>(
    db: &C,
    filter: AgentMemoryListFilter,
    resolved_scopes: Vec<MemoryScopeResolution>,
    now: DateTimeWithTimeZone,
    order: MemoryListOrder,
) -> Result<Vec<agent_memory::Model>> {
    let mut query = memory_records_query(filter, resolved_scopes, now)?;

    let query = match order {
        MemoryListOrder::Inventory { before } => {
            if let Some((created_at, id)) = before {
                let created_at = unix_to_datetime(created_at);
                query = query.filter(
                    Condition::any()
                        .add(agent_memory::Column::CreatedAt.lt(created_at))
                        .add(
                            Condition::all()
                                .add(agent_memory::Column::CreatedAt.eq(created_at))
                                .add(agent_memory::Column::Id.lt(id)),
                        ),
                );
            }
            query
                .order_by_desc(agent_memory::Column::CreatedAt)
                .order_by_desc(agent_memory::Column::Id)
        }
        MemoryListOrder::RecentUpdates => query.order_by_desc(agent_memory::Column::UpdatedAt),
    };
    query
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

/// The payload is storage owned by MemoryService's existing backend protocol.
/// It is deliberately separate from control rows: put precedes policy/control
/// publication, and uncommitted payloads must never appear in product search.
pub async fn put_payload<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    scope_kind: &str,
    scope_key_hash: &str,
    content: &str,
) -> Result<i64> {
    use pioneer_entity::agent_memory_payloads as payload;
    if let Some(previous) = payload::Entity::find_by_id(memory_id).one(db).await? {
        anyhow::ensure!(
            previous.scope_kind == scope_kind && previous.scope_key_hash == scope_key_hash,
            "memory payload ownership mismatch"
        );
    }
    payload::Entity::insert(payload::ActiveModel {
        memory_id: Set(memory_id.to_owned()),
        scope_kind: Set(scope_kind.to_owned()),
        scope_key_hash: Set(scope_key_hash.to_owned()),
        content: Set(content.to_owned()),
        revision: Set(1),
    })
    .on_conflict(
        OnConflict::column(payload::Column::MemoryId)
            .update_column(payload::Column::Content)
            .value(
                payload::Column::Revision,
                Expr::col(payload::Column::Revision).add(1),
            )
            .to_owned(),
    )
    .exec(db)
    .await?;
    Ok(payload::Entity::find_by_id(memory_id)
        .one(db)
        .await?
        .context("written memory payload missing")?
        .revision)
}

pub async fn get_payload<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    scope_kind: &str,
    scope_key_hash: &str,
) -> Result<Option<pioneer_entity::agent_memory_payloads::Model>> {
    use pioneer_entity::agent_memory_payloads as payload;
    Ok(payload::Entity::find_by_id(memory_id)
        .filter(payload::Column::ScopeKind.eq(scope_kind))
        .filter(payload::Column::ScopeKeyHash.eq(scope_key_hash))
        .one(db)
        .await?)
}

pub async fn delete_payload<C: ConnectionTrait>(
    db: &C,
    memory_id: &str,
    scope_kind: &str,
    scope_key_hash: &str,
) -> Result<bool> {
    use pioneer_entity::agent_memory_payloads as payload;
    Ok(payload::Entity::delete_many()
        .filter(payload::Column::MemoryId.eq(memory_id))
        .filter(payload::Column::ScopeKind.eq(scope_kind))
        .filter(payload::Column::ScopeKeyHash.eq(scope_key_hash))
        .exec(db)
        .await?
        .rows_affected
        != 0)
}

pub async fn search_payloads<C: ConnectionTrait>(
    db: &C,
    query: &str,
    mut filter: AgentMemoryListFilter,
    resolved_scopes: Vec<MemoryScopeResolution>,
    sensitivities: &[pioneer_protocol::MemorySensitivity],
    now_unix: i64,
    limit: u32,
) -> Result<Vec<(String, String)>> {
    use sea_orm::{DbBackend, QueryTrait, Statement, Value};
    if resolved_scopes.is_empty() || limit == 0 || sensitivities.is_empty() {
        return Ok(Vec::new());
    }
    // Eligibility is the existing control-plane filter, evaluated before rank
    // and LIMIT, with the same ownership and source-provenance rules as listing.
    filter.limit = None;
    let eligible = memory_records_query(filter, resolved_scopes, unix_to_datetime(now_unix))?
        .select_only()
        .column(agent_memory::Column::Id)
        .filter(
            agent_memory::Column::Sensitivity.is_in(
                sensitivities
                    .iter()
                    .map(|value| memory_sensitivity_to_db(*value)),
            ),
        )
        .filter(
            agent_memory::Column::Id.not_in_subquery(
                pioneer_entity::agent_memory_quarantine::Entity::find()
                    .select_only()
                    .column(pioneer_entity::agent_memory_quarantine::Column::MemoryId)
                    .filter(pioneer_entity::agent_memory_quarantine::Column::ResolvedAt.is_null())
                    .into_query(),
            ),
        );
    let eligible = eligible.build(DbBackend::Sqlite);
    let mut values: Vec<Value> = vec![query.to_owned().into()];
    values.extend(eligible.values.map(|values| values.0).unwrap_or_default());
    values.push(i64::from(std::cmp::min(limit, 1000)).into());
    values.push(query.to_owned().into());
    // MATCH and rank have no Entity/SeaQuery search representation. The ordinary
    // eligibility subquery stays typed; only this FTS operation uses raw SQL.
    let sql = format!(
        "WITH ranked_payloads AS MATERIALIZED (
        SELECT p.memory_id, p.rowid AS payload_rowid, bm25(agent_memory_fts) AS rank FROM agent_memory_fts
        JOIN agent_memory_payloads p ON p.rowid = agent_memory_fts.rowid
        JOIN agent_memory m ON m.id = p.memory_id
        WHERE agent_memory_fts MATCH pioneer_fts_literal(?) AND m.id IN ({})
        AND m.scope_kind = p.scope_kind AND m.scope_key_hash = p.scope_key_hash
        ORDER BY rank, p.memory_id LIMIT ?)
        SELECT h.memory_id, substr((SELECT snippet(agent_memory_fts,0,'','','…',8)
            FROM agent_memory_fts WHERE rowid=h.payload_rowid
            AND agent_memory_fts MATCH pioneer_fts_literal(?)),1,240) AS content
        FROM ranked_payloads h ORDER BY h.rank,h.memory_id",
        eligible.sql
    );
    db.query_all_raw(Statement::from_sql_and_values(
        DbBackend::Sqlite,
        sql,
        values,
    ))
    .await?
    .into_iter()
    .map(|row| Ok((row.try_get("", "memory_id")?, row.try_get("", "content")?)))
    .collect()
}

/// Discovery for the one-time pre-SQLite payload reset. The migration marker's
/// timestamp bounds the old contour; a concurrent new SQLite payload excludes
/// its control row again at the commit boundary.
pub async fn legacy_payloadless_ids<C: ConnectionTrait>(
    db: &C,
    through: DateTimeWithTimeZone,
    after: Option<&str>,
) -> Result<Vec<String>> {
    use sea_orm::QueryTrait;
    let mut query = agent_memory::Entity::find()
        .select_only()
        .column(agent_memory::Column::Id)
        .filter(agent_memory::Column::CreatedAt.lte(through))
        .filter(
            agent_memory::Column::Id.not_in_subquery(
                pioneer_entity::agent_memory_payloads::Entity::find()
                    .select_only()
                    .column(pioneer_entity::agent_memory_payloads::Column::MemoryId)
                    .into_query(),
            ),
        );
    if let Some(after) = after {
        query = query.filter(agent_memory::Column::Id.gt(after));
    }
    Ok(query
        .order_by_asc(agent_memory::Column::Id)
        .limit(64)
        .into_tuple::<String>()
        .all(db)
        .await?)
}

pub async fn retire_legacy_payloadless_record<C: ConnectionTrait>(
    db: &C,
    id: &str,
    through: DateTimeWithTimeZone,
    now: DateTimeWithTimeZone,
) -> Result<()> {
    let Some(row) = agent_memory::Entity::find_by_id(id).one(db).await? else {
        return Ok(());
    };
    if row.created_at > through
        || pioneer_entity::agent_memory_payloads::Entity::find_by_id(id)
            .one(db)
            .await?
            .is_some()
    {
        return Ok(());
    }
    // FK enforcement is disabled: release the obsolete canonical-key binding
    // explicitly, without touching sources, quality history or canonical turns.
    pioneer_entity::agent_memory_identity::Entity::delete_many()
        .filter(pioneer_entity::agent_memory_identity::Column::MemoryId.eq(id))
        .exec(db)
        .await?;
    mark_memory_deleted(
        db,
        id,
        None,
        Some("derived_memory_storage_reset".to_owned()),
        now,
    )
    .await?;
    agent_memory::Entity::update_many()
        .col_expr(
            agent_memory::Column::ContentPreview,
            Expr::value(None::<String>),
        )
        .filter(agent_memory::Column::Id.eq(id))
        .exec(db)
        .await?;
    Ok(())
}
