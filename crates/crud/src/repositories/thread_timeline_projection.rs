use anyhow::{Context, Result};
use pioneer_entity::{
    thread_timeline_block, thread_timeline_projection_meta, turn_work_item_projection,
    turn_work_projection,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, PaginatorTrait, QueryFilter, QueryOrder,
    QuerySelect, Set, Statement,
};
use std::collections::HashMap;

pub const SEMANTIC_TIMELINE_PROJECTION_KEY: &str = "semantic_timeline";
pub const SEMANTIC_TIMELINE_PROJECTION_VERSION: i64 = 8;

pub const PROJECTION_META_STATUS_PENDING: &str = "pending";
pub const PROJECTION_META_STATUS_BACKFILLING: &str = "backfilling";
pub const PROJECTION_META_STATUS_COMPLETE: &str = "complete";
pub const PROJECTION_META_STATUS_FAILED: &str = "failed";

pub const BLOCK_KIND_USER_MESSAGE: &str = "user_message";
pub const BLOCK_KIND_TURN_WORK: &str = "turn_work";
pub const BLOCK_KIND_DETACHED_TASK_RUN: &str = "detached_task_run";
pub const BLOCK_KIND_ASSISTANT_MESSAGE: &str = "assistant_message";
pub const BLOCK_KIND_RUNNING: &str = "running";
pub const BLOCK_KIND_APPROVAL: &str = "approval";
pub const BLOCK_KIND_SYSTEM: &str = "system";

pub const WORK_VISIBILITY_VISIBLE: &str = "visible";
pub const WORK_VISIBILITY_HIDDEN: &str = "hidden";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionPageAnchor<'a> {
    Start,
    End,
    Before(&'a str),
    After(&'a str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionMetaRecord {
    pub projection_key: String,
    pub projection_version: i64,
    pub status: String,
    pub source_thread_count: i64,
    pub source_turn_count: i64,
    pub source_turn_item_count: i64,
    pub source_turn_event_count: i64,
    pub last_error: Option<String>,
    pub backfill_started_at: Option<DateTimeWithTimeZone>,
    pub backfilled_at: Option<DateTimeWithTimeZone>,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectionMetaConfigRecord {
    pub projection_config_hash: Option<String>,
    pub projection_config_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadTimelineBlockRecord {
    pub block_id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: Option<String>,
    pub block_kind: String,
    pub sort_key: String,
    pub source_kind: Option<String>,
    pub source_key: Option<String>,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub metadata_json: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

/// Server-resolved presentation grant for approval payloads embedded in a
/// shared thread timeline.
///
/// The grant is action-based, not initiator-based: every current collaborator
/// whose role grants `AgentRequestObserve` sees every pending interaction in
/// the root thread capsule. A narrower future role can still read the ordinary
/// timeline while approval rows remain filtered before ordering/pagination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadTimelineApprovalScope {
    pub can_observe_agent_requests: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnWorkProjectionRecord {
    pub turn_id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub presentation: String,
    pub state: String,
    pub work_count: i64,
    pub visible_work_count: i64,
    pub hidden_work_count: i64,
    pub first_work_item_id: Option<String>,
    pub last_work_item_id: Option<String>,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub elapsed_ms: Option<i64>,
    pub source_high_watermark: i64,
    pub metadata_json: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnWorkItemProjectionRecord {
    pub work_item_id: String,
    pub workspace_id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub source_event_id: Option<String>,
    pub source_sequence: i64,
    pub order_key: String,
    pub item_type: String,
    pub visibility: String,
    pub classification: String,
    pub status: String,
    pub started_at: Option<DateTimeWithTimeZone>,
    pub completed_at: Option<DateTimeWithTimeZone>,
    pub metadata_json: String,
    pub created_at: DateTimeWithTimeZone,
    pub updated_at: DateTimeWithTimeZone,
}

pub async fn find_projection_meta<C: ConnectionTrait>(
    db: &C,
    projection_key: &str,
) -> Result<Option<thread_timeline_projection_meta::Model>> {
    thread_timeline_projection_meta::Entity::find_by_id(projection_key.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to query timeline projection meta `{projection_key}`"))
}

pub async fn list_projection_meta_by_key_prefix<C: ConnectionTrait>(
    db: &C,
    projection_key_prefix: &str,
) -> Result<Vec<thread_timeline_projection_meta::Model>> {
    let rows = thread_timeline_projection_meta::Entity::find()
        .order_by_asc(thread_timeline_projection_meta::Column::ProjectionKey)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to list timeline projection meta rows with key prefix `{projection_key_prefix}`"
            )
        })?;
    Ok(rows
        .into_iter()
        .filter(|row| row.projection_key.starts_with(projection_key_prefix))
        .collect())
}

pub async fn upsert_projection_meta<C: ConnectionTrait>(
    db: &C,
    record: ProjectionMetaRecord,
) -> Result<()> {
    upsert_projection_meta_with_config(db, record, ProjectionMetaConfigRecord::default()).await
}

pub async fn upsert_projection_meta_with_config<C: ConnectionTrait>(
    db: &C,
    record: ProjectionMetaRecord,
    config: ProjectionMetaConfigRecord,
) -> Result<()> {
    thread_timeline_projection_meta::Entity::insert(thread_timeline_projection_meta::ActiveModel {
        projection_key: Set(record.projection_key),
        projection_version: Set(record.projection_version),
        status: Set(record.status),
        source_thread_count: Set(record.source_thread_count),
        source_turn_count: Set(record.source_turn_count),
        source_turn_item_count: Set(record.source_turn_item_count),
        source_turn_event_count: Set(record.source_turn_event_count),
        projection_config_hash: Set(config.projection_config_hash),
        projection_config_json: Set(config.projection_config_json),
        last_error: Set(record.last_error),
        backfill_started_at: Set(record.backfill_started_at),
        backfilled_at: Set(record.backfilled_at),
        created_at: Set(record.created_at),
        updated_at: Set(record.updated_at),
    })
    .on_conflict(
        OnConflict::column(thread_timeline_projection_meta::Column::ProjectionKey)
            .update_columns([
                thread_timeline_projection_meta::Column::ProjectionVersion,
                thread_timeline_projection_meta::Column::Status,
                thread_timeline_projection_meta::Column::SourceThreadCount,
                thread_timeline_projection_meta::Column::SourceTurnCount,
                thread_timeline_projection_meta::Column::SourceTurnItemCount,
                thread_timeline_projection_meta::Column::SourceTurnEventCount,
                thread_timeline_projection_meta::Column::ProjectionConfigHash,
                thread_timeline_projection_meta::Column::ProjectionConfigJson,
                thread_timeline_projection_meta::Column::LastError,
                thread_timeline_projection_meta::Column::BackfillStartedAt,
                thread_timeline_projection_meta::Column::BackfilledAt,
                thread_timeline_projection_meta::Column::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to upsert timeline projection meta")?;

    Ok(())
}

pub async fn update_projection_meta_status<C: ConnectionTrait>(
    db: &C,
    projection_key: &str,
    status: &str,
    last_error: Option<&str>,
    updated_at: DateTimeWithTimeZone,
) -> Result<bool> {
    let result = thread_timeline_projection_meta::Entity::update_many()
        .filter(
            thread_timeline_projection_meta::Column::ProjectionKey.eq(projection_key.to_owned()),
        )
        .col_expr(
            thread_timeline_projection_meta::Column::Status,
            sea_orm::sea_query::Expr::value(status.to_owned()),
        )
        .col_expr(
            thread_timeline_projection_meta::Column::LastError,
            sea_orm::sea_query::Expr::value(last_error.map(str::to_owned)),
        )
        .col_expr(
            thread_timeline_projection_meta::Column::UpdatedAt,
            sea_orm::sea_query::Expr::value(updated_at),
        )
        .exec(db)
        .await
        .with_context(|| format!("failed to update timeline projection meta `{projection_key}`"))?;

    Ok(result.rows_affected > 0)
}

pub async fn upsert_thread_timeline_block<C: ConnectionTrait>(
    db: &C,
    record: ThreadTimelineBlockRecord,
) -> Result<()> {
    thread_timeline_block::Entity::insert(thread_timeline_block::ActiveModel {
        block_id: Set(record.block_id),
        workspace_id: Set(record.workspace_id),
        thread_id: Set(record.thread_id),
        turn_id: Set(record.turn_id),
        block_kind: Set(record.block_kind),
        sort_key: Set(record.sort_key),
        source_kind: Set(record.source_kind),
        source_key: Set(record.source_key),
        started_at: Set(record.started_at),
        completed_at: Set(record.completed_at),
        metadata_json: Set(record.metadata_json),
        created_at: Set(record.created_at),
        updated_at: Set(record.updated_at),
    })
    .on_conflict(
        OnConflict::column(thread_timeline_block::Column::BlockId)
            .update_columns([
                thread_timeline_block::Column::WorkspaceId,
                thread_timeline_block::Column::ThreadId,
                thread_timeline_block::Column::TurnId,
                thread_timeline_block::Column::BlockKind,
                thread_timeline_block::Column::SortKey,
                thread_timeline_block::Column::SourceKind,
                thread_timeline_block::Column::SourceKey,
                thread_timeline_block::Column::StartedAt,
                thread_timeline_block::Column::CompletedAt,
                thread_timeline_block::Column::MetadataJson,
                thread_timeline_block::Column::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to upsert thread timeline block")?;

    Ok(())
}

pub async fn delete_thread_timeline_blocks_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<u64> {
    let result = thread_timeline_block::Entity::delete_many()
        .filter(thread_timeline_block::Column::ThreadId.eq(thread_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to delete timeline blocks for thread `{thread_id}`"))?;
    Ok(result.rows_affected)
}

pub async fn delete_thread_timeline_blocks_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<u64> {
    let result = thread_timeline_block::Entity::delete_many()
        .filter(thread_timeline_block::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to delete timeline blocks for turn `{turn_id}`"))?;
    Ok(result.rows_affected)
}

pub async fn delete_thread_timeline_block<C: ConnectionTrait>(
    db: &C,
    block_id: &str,
) -> Result<u64> {
    let result = thread_timeline_block::Entity::delete_many()
        .filter(thread_timeline_block::Column::BlockId.eq(block_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to delete timeline block `{block_id}`"))?;
    Ok(result.rows_affected)
}

pub async fn list_thread_timeline_blocks_page<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    approval_scope: Option<&ThreadTimelineApprovalScope>,
    anchor: ProjectionPageAnchor<'_>,
    limit: u64,
) -> Result<Vec<thread_timeline_block::Model>> {
    let mut query = thread_timeline_block::Entity::find()
        .filter(thread_timeline_block::Column::ThreadId.eq(thread_id.to_owned()));
    if let Some(approval_scope) = approval_scope {
        query = query.filter(thread_timeline_approval_visibility(approval_scope));
    }

    let reverse_after_load = matches!(
        anchor,
        ProjectionPageAnchor::End | ProjectionPageAnchor::Before(_)
    );

    match anchor {
        ProjectionPageAnchor::Start => {
            query = query.order_by_asc(thread_timeline_block::Column::SortKey);
        }
        ProjectionPageAnchor::End => {
            query = query.order_by_desc(thread_timeline_block::Column::SortKey);
        }
        ProjectionPageAnchor::Before(sort_key) => {
            query = query
                .filter(thread_timeline_block::Column::SortKey.lt(sort_key.to_owned()))
                .order_by_desc(thread_timeline_block::Column::SortKey);
        }
        ProjectionPageAnchor::After(sort_key) => {
            query = query
                .filter(thread_timeline_block::Column::SortKey.gt(sort_key.to_owned()))
                .order_by_asc(thread_timeline_block::Column::SortKey);
        }
    }

    let mut rows =
        query.limit(limit).all(db).await.with_context(|| {
            format!("failed to list timeline blocks page for thread `{thread_id}`")
        })?;

    if reverse_after_load {
        rows.reverse();
    }

    Ok(rows)
}

pub async fn find_thread_timeline_block_by_sort_key<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    approval_scope: Option<&ThreadTimelineApprovalScope>,
    sort_key: &str,
) -> Result<Option<thread_timeline_block::Model>> {
    let mut query = thread_timeline_block::Entity::find()
        .filter(thread_timeline_block::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(thread_timeline_block::Column::SortKey.eq(sort_key.to_owned()));
    if let Some(approval_scope) = approval_scope {
        query = query.filter(thread_timeline_approval_visibility(approval_scope));
    }
    query.one(db).await.with_context(|| {
        format!("failed to query timeline block for thread `{thread_id}` sort key `{sort_key}`")
    })
}

pub async fn find_user_message_block_by_turn_id<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<thread_timeline_block::Model>> {
    thread_timeline_block::Entity::find()
        .filter(thread_timeline_block::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(thread_timeline_block::Column::TurnId.eq(turn_id.to_owned()))
        .filter(thread_timeline_block::Column::BlockKind.eq(BLOCK_KIND_USER_MESSAGE))
        .one(db)
        .await
        .with_context(|| {
            format!(
                "failed to query user-message timeline block for turn `{turn_id}` in thread `{thread_id}`"
            )
        })
}

pub async fn count_unread_user_message_blocks<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    principal_id: &str,
    after_sort_key: &str,
) -> Result<u64> {
    let row = db
        .query_one_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            r#"
                SELECT COUNT(*) AS unread_count
                FROM thread_timeline_block AS block
                INNER JOIN turn
                    ON turn.id = block.turn_id
                    AND turn.thread_id = block.thread_id
                WHERE block.thread_id = ?
                    AND block.block_kind = 'user_message'
                    AND block.sort_key > ?
                    AND turn.turn_kind = 'conversation'
                    AND turn.origin = 'user'
                    AND turn.message_deleted_at IS NULL
                    AND turn.initiated_by_actor_kind = 'principal'
                    AND turn.initiated_by_actor_id <> ?
            "#,
            vec![
                thread_id.to_owned().into(),
                after_sort_key.to_owned().into(),
                principal_id.to_owned().into(),
            ],
        ))
        .await
        .context("failed to count unread user-message timeline blocks")?
        .context("unread count query returned no row")?;
    let count = row
        .try_get::<i64>("", "unread_count")
        .context("failed to decode unread user-message count")?;
    u64::try_from(count).context("unread user-message count is negative")
}

pub async fn count_unread_user_message_blocks_for_threads<C: ConnectionTrait>(
    db: &C,
    thread_ids: &[String],
    principal_id: &str,
) -> Result<HashMap<String, u64>> {
    if thread_ids.is_empty() {
        return Ok(HashMap::new());
    }
    let placeholders = std::iter::repeat("?")
        .take(thread_ids.len())
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        r#"
            SELECT block.thread_id, COUNT(*) AS unread_count
            FROM thread_timeline_block AS block
            INNER JOIN turn
                ON turn.id = block.turn_id
                AND turn.thread_id = block.thread_id
            LEFT JOIN thread_read_cursor AS cursor
                ON cursor.thread_id = block.thread_id
                AND cursor.principal_id = ?
            WHERE block.thread_id IN ({placeholders})
                AND block.block_kind = 'user_message'
                AND block.sort_key > COALESCE(cursor.last_read_sort_key, '')
                AND turn.turn_kind = 'conversation'
                AND turn.origin = 'user'
                AND turn.message_deleted_at IS NULL
                AND turn.initiated_by_actor_kind = 'principal'
                AND turn.initiated_by_actor_id <> ?
            GROUP BY block.thread_id
        "#
    );
    let mut values = Vec::with_capacity(thread_ids.len() + 2);
    values.push(principal_id.to_owned().into());
    values.extend(thread_ids.iter().cloned().map(Into::into));
    values.push(principal_id.to_owned().into());
    let rows = db
        .query_all_raw(Statement::from_sql_and_values(
            db.get_database_backend(),
            sql,
            values,
        ))
        .await
        .context("failed to count unread user-message blocks for thread batch")?;
    rows.into_iter()
        .map(|row| {
            let thread_id = row
                .try_get::<String>("", "thread_id")
                .context("failed to decode unread thread id")?;
            let count = row
                .try_get::<i64>("", "unread_count")
                .context("failed to decode unread thread count")?;
            Ok((
                thread_id,
                u64::try_from(count).context("unread thread count is negative")?,
            ))
        })
        .collect()
}

fn thread_timeline_approval_visibility(approval_scope: &ThreadTimelineApprovalScope) -> Condition {
    if approval_scope.can_observe_agent_requests {
        Condition::all()
    } else {
        Condition::all().add(thread_timeline_block::Column::BlockKind.ne(BLOCK_KIND_APPROVAL))
    }
}

pub async fn count_thread_timeline_blocks<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<u64> {
    thread_timeline_block::Entity::find()
        .filter(thread_timeline_block::Column::ThreadId.eq(thread_id.to_owned()))
        .count(db)
        .await
        .with_context(|| format!("failed to count timeline blocks for thread `{thread_id}`"))
}

pub async fn count_thread_timeline_blocks_for_turn_kind<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    block_kind: &str,
) -> Result<u64> {
    thread_timeline_block::Entity::find()
        .filter(thread_timeline_block::Column::TurnId.eq(turn_id.to_owned()))
        .filter(thread_timeline_block::Column::BlockKind.eq(block_kind.to_owned()))
        .count(db)
        .await
        .with_context(|| {
            format!("failed to count `{block_kind}` timeline blocks for turn `{turn_id}`")
        })
}

pub async fn upsert_turn_work_projection<C: ConnectionTrait>(
    db: &C,
    record: TurnWorkProjectionRecord,
) -> Result<()> {
    turn_work_projection::Entity::insert(turn_work_projection::ActiveModel {
        turn_id: Set(record.turn_id),
        workspace_id: Set(record.workspace_id),
        thread_id: Set(record.thread_id),
        presentation: Set(record.presentation),
        state: Set(record.state),
        work_count: Set(record.work_count),
        visible_work_count: Set(record.visible_work_count),
        hidden_work_count: Set(record.hidden_work_count),
        first_work_item_id: Set(record.first_work_item_id),
        last_work_item_id: Set(record.last_work_item_id),
        started_at: Set(record.started_at),
        completed_at: Set(record.completed_at),
        elapsed_ms: Set(record.elapsed_ms),
        source_high_watermark: Set(record.source_high_watermark),
        metadata_json: Set(record.metadata_json),
        created_at: Set(record.created_at),
        updated_at: Set(record.updated_at),
    })
    .on_conflict(
        OnConflict::column(turn_work_projection::Column::TurnId)
            .update_columns([
                turn_work_projection::Column::WorkspaceId,
                turn_work_projection::Column::ThreadId,
                turn_work_projection::Column::Presentation,
                turn_work_projection::Column::State,
                turn_work_projection::Column::WorkCount,
                turn_work_projection::Column::VisibleWorkCount,
                turn_work_projection::Column::HiddenWorkCount,
                turn_work_projection::Column::FirstWorkItemId,
                turn_work_projection::Column::LastWorkItemId,
                turn_work_projection::Column::StartedAt,
                turn_work_projection::Column::CompletedAt,
                turn_work_projection::Column::ElapsedMs,
                turn_work_projection::Column::SourceHighWatermark,
                turn_work_projection::Column::MetadataJson,
                turn_work_projection::Column::UpdatedAt,
            ])
            .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to upsert turn work projection")?;

    Ok(())
}

pub async fn find_turn_work_projection<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<turn_work_projection::Model>> {
    turn_work_projection::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to query turn work projection for turn `{turn_id}`"))
}

pub async fn find_turn_work_item_projection<C: ConnectionTrait>(
    db: &C,
    work_item_id: &str,
) -> Result<Option<turn_work_item_projection::Model>> {
    turn_work_item_projection::Entity::find_by_id(work_item_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to query turn work item projection `{work_item_id}`"))
}

pub async fn list_turn_work_item_projections_by_ids<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    work_item_ids: &[String],
    visibility: Option<&str>,
) -> Result<Vec<turn_work_item_projection::Model>> {
    if work_item_ids.is_empty() {
        return Ok(Vec::new());
    }

    let mut query = turn_work_item_projection::Entity::find()
        .filter(turn_work_item_projection::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_work_item_projection::Column::WorkItemId.is_in(work_item_ids.iter().cloned()));
    if let Some(visibility) = visibility {
        query =
            query.filter(turn_work_item_projection::Column::Visibility.eq(visibility.to_owned()));
    }

    query
        .order_by_asc(turn_work_item_projection::Column::OrderKey)
        .all(db)
        .await
        .with_context(|| {
            format!(
                "failed to query {} work item projections for turn `{turn_id}`",
                work_item_ids.len()
            )
        })
}

pub async fn find_turn_work_item_projection_by_order_key<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    order_key: &str,
    visibility: Option<&str>,
) -> Result<Option<turn_work_item_projection::Model>> {
    let mut query = turn_work_item_projection::Entity::find()
        .filter(turn_work_item_projection::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_work_item_projection::Column::OrderKey.eq(order_key.to_owned()));

    if let Some(visibility) = visibility {
        query =
            query.filter(turn_work_item_projection::Column::Visibility.eq(visibility.to_owned()));
    }

    query.one(db).await.with_context(|| {
        format!("failed to query work item projection for turn `{turn_id}` order key `{order_key}`")
    })
}

pub async fn delete_turn_work_projection<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<u64> {
    let result = turn_work_projection::Entity::delete_many()
        .filter(turn_work_projection::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to delete work projection for turn `{turn_id}`"))?;
    Ok(result.rows_affected)
}

pub async fn upsert_turn_work_item_projection<C: ConnectionTrait>(
    db: &C,
    record: TurnWorkItemProjectionRecord,
) -> Result<()> {
    turn_work_item_projection::Entity::insert(turn_work_item_projection::ActiveModel {
        work_item_id: Set(record.work_item_id),
        workspace_id: Set(record.workspace_id),
        thread_id: Set(record.thread_id),
        turn_id: Set(record.turn_id),
        item_id: Set(record.item_id),
        source_event_id: Set(record.source_event_id),
        source_sequence: Set(record.source_sequence),
        order_key: Set(record.order_key),
        item_type: Set(record.item_type),
        visibility: Set(record.visibility),
        classification: Set(record.classification),
        status: Set(record.status),
        started_at: Set(record.started_at),
        completed_at: Set(record.completed_at),
        metadata_json: Set(record.metadata_json),
        created_at: Set(record.created_at),
        updated_at: Set(record.updated_at),
    })
    .on_conflict(
        OnConflict::columns([
            turn_work_item_projection::Column::TurnId,
            turn_work_item_projection::Column::ItemId,
        ])
        .update_columns([
            turn_work_item_projection::Column::WorkspaceId,
            turn_work_item_projection::Column::ThreadId,
            turn_work_item_projection::Column::SourceEventId,
            turn_work_item_projection::Column::SourceSequence,
            turn_work_item_projection::Column::OrderKey,
            turn_work_item_projection::Column::ItemType,
            turn_work_item_projection::Column::Visibility,
            turn_work_item_projection::Column::Classification,
            turn_work_item_projection::Column::Status,
            turn_work_item_projection::Column::StartedAt,
            turn_work_item_projection::Column::CompletedAt,
            turn_work_item_projection::Column::MetadataJson,
            turn_work_item_projection::Column::UpdatedAt,
        ])
        .to_owned(),
    )
    .exec(db)
    .await
    .context("failed to upsert turn work item projection")?;

    Ok(())
}

pub async fn delete_turn_work_items_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<u64> {
    let result = turn_work_item_projection::Entity::delete_many()
        .filter(turn_work_item_projection::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| format!("failed to delete work item projections for turn `{turn_id}`"))?;
    Ok(result.rows_affected)
}

pub async fn delete_turn_work_item_projection_for_item<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<u64> {
    let result = turn_work_item_projection::Entity::delete_many()
        .filter(turn_work_item_projection::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_work_item_projection::Column::ItemId.eq(item_id.to_owned()))
        .exec(db)
        .await
        .with_context(|| {
            format!("failed to delete work item projection for turn `{turn_id}` item `{item_id}`")
        })?;
    Ok(result.rows_affected)
}

pub async fn list_turn_work_items_page<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    visibility: Option<&str>,
    anchor: ProjectionPageAnchor<'_>,
    limit: u64,
) -> Result<Vec<turn_work_item_projection::Model>> {
    let mut query = turn_work_item_projection::Entity::find()
        .filter(turn_work_item_projection::Column::TurnId.eq(turn_id.to_owned()));

    if let Some(visibility) = visibility {
        query =
            query.filter(turn_work_item_projection::Column::Visibility.eq(visibility.to_owned()));
    }

    let reverse_after_load = matches!(
        anchor,
        ProjectionPageAnchor::End | ProjectionPageAnchor::Before(_)
    );

    match anchor {
        ProjectionPageAnchor::Start => {
            query = query.order_by_asc(turn_work_item_projection::Column::OrderKey);
        }
        ProjectionPageAnchor::End => {
            query = query.order_by_desc(turn_work_item_projection::Column::OrderKey);
        }
        ProjectionPageAnchor::Before(order_key) => {
            query = query
                .filter(turn_work_item_projection::Column::OrderKey.lt(order_key.to_owned()))
                .order_by_desc(turn_work_item_projection::Column::OrderKey);
        }
        ProjectionPageAnchor::After(order_key) => {
            query = query
                .filter(turn_work_item_projection::Column::OrderKey.gt(order_key.to_owned()))
                .order_by_asc(turn_work_item_projection::Column::OrderKey);
        }
    }

    let mut rows = query.limit(limit).all(db).await.with_context(|| {
        format!("failed to list work item projection page for turn `{turn_id}`")
    })?;

    if reverse_after_load {
        rows.reverse();
    }

    Ok(rows)
}

pub async fn list_turn_work_items_by_status_page<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    status: &str,
    after_order_key: Option<&str>,
    limit: u64,
) -> Result<Vec<turn_work_item_projection::Model>> {
    let mut query = turn_work_item_projection::Entity::find()
        .filter(turn_work_item_projection::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_work_item_projection::Column::Status.eq(status.to_owned()));

    if let Some(after_order_key) = after_order_key {
        query = query
            .filter(turn_work_item_projection::Column::OrderKey.gt(after_order_key.to_owned()));
    }

    query
        .order_by_asc(turn_work_item_projection::Column::OrderKey)
        .limit(limit)
        .all(db)
        .await
        .with_context(|| {
            format!("failed to list `{status}` work item projection page for turn `{turn_id}`")
        })
}

pub async fn count_turn_work_items<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    visibility: Option<&str>,
) -> Result<u64> {
    let mut query = turn_work_item_projection::Entity::find()
        .filter(turn_work_item_projection::Column::TurnId.eq(turn_id.to_owned()));

    if let Some(visibility) = visibility {
        query =
            query.filter(turn_work_item_projection::Column::Visibility.eq(visibility.to_owned()));
    }

    query
        .count(db)
        .await
        .with_context(|| format!("failed to count work item projections for turn `{turn_id}`"))
}
