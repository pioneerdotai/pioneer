use anyhow::{Context, Result};
use pioneer_entity::{
    thread_timeline_block, thread_timeline_projection_meta, turn_work_item_projection,
    turn_work_projection,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseBackend, EntityTrait, PaginatorTrait,
    QueryFilter, QueryOrder, QuerySelect, Set, Statement,
};
use std::collections::HashMap;

pub const SEMANTIC_TIMELINE_PROJECTION_KEY: &str = "semantic_timeline";
pub const SEMANTIC_TIMELINE_PROJECTION_VERSION: i64 = 9;

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

pub async fn find_thread_timeline_block_by_id<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    block_id: &str,
) -> Result<Option<thread_timeline_block::Model>> {
    thread_timeline_block::Entity::find_by_id(block_id.to_owned())
        .filter(thread_timeline_block::Column::ThreadId.eq(thread_id.to_owned()))
        .one(db)
        .await
        .context("failed to find message read position")
}

/// Called inside the canonical projection transaction, only when a message
/// first becomes readable. In particular, a late final answer must sort after
/// cursors that could already have passed the beginning of its Turn.
/// Existing rows retain their position on edits, snapshots and redelivery.
pub(crate) async fn new_message_sort_key<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    block_id: &str,
    turn_id: &str,
    candidate: String,
    published_at: i64,
) -> Result<String> {
    super::unread_start::publish_message_position(
        db,
        thread_id,
        block_id,
        turn_id,
        candidate,
        published_at,
    )
    .await
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

// Assistant blocks already identify canonical final responses. EXISTS checks
// completion without loading payloads or multiplying a response by its HTML publications.
const UNREAD_MESSAGE_PREDICATE: &str = r#"AND (
                        (block.block_kind = 'user_message'
                            AND turn.turn_kind = 'conversation'
                            AND turn.origin = 'user'
                            AND turn.message_deleted_at IS NULL
                            AND turn.initiated_by_actor_kind = 'principal'
                            AND turn.initiated_by_actor_id <> ?)
                        OR (block.block_kind = 'assistant_message'
                            AND block.source_kind = 'turn_item'
                            AND EXISTS (
                                SELECT 1 FROM turn_item AS item
                                WHERE item.turn_id = block.turn_id
                                    AND item.item_id = block.source_key
                                    AND item.item_type = 'agent_message'
                                    AND item.status = 'completed'
                            ))
                    )"#;

pub async fn count_unread_message_blocks<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    principal_id: &str,
    after_sort_key: &str,
) -> Result<u64> {
    let id = thread_id.to_owned();
    let row = db
        .query_one_raw(unread_thread_batch_statement(
            &[&id],
            principal_id,
            Some(after_sort_key),
        ))
        .await
        .context("failed to count unread message timeline blocks")?;
    let Some(row) = row else {
        return Ok(0);
    };
    u64::try_from(row.try_get::<i64>("", "unread_count")?)
        .context("unread message count is negative")
}

// Bound bind parameters even for callers outside the workspace tree RPC. Dedup
// before batching: the old IN query counted every input thread only once.
const UNREAD_THREAD_BATCH_SIZE: usize = 128;

fn unread_thread_batch_statement(
    thread_ids: &[&String],
    principal_id: &str,
    after_override: Option<&str>,
) -> Statement {
    use sea_orm::sea_query::{
        Alias, Asterisk, CommonTableExpression, Expr, ExprTrait, Func, JoinType, Query, SimpleExpr,
        SqliteQueryBuilder, WithClause,
    };

    // Rollout/access are second-resolution facts. Exclude the boundary second
    // deliberately, as agreed; retain exact, monotonic confirmed read positions.
    let count = |after: Expr| {
        let mut query = Query::select();
        query
            .expr(Expr::col(Asterisk).count())
            .from_as("thread_timeline_block", "block")
            .join(
                JoinType::InnerJoin,
                "turn",
                Condition::all()
                    .add(Expr::col(("turn", "id")).eq(Expr::col(("block", "turn_id"))))
                    .add(Expr::col(("turn", "thread_id")).eq(Expr::col(("block", "thread_id")))),
            )
            .and_where(Expr::col(("block", "thread_id")).eq(Expr::col(("bounds", "thread_id"))))
            .and_where(Expr::col(("block", "sort_key")).gt(after))
            // Validate parents only for messages in the indexed range. An
            // already-read owner directory must not touch each potentially
            // large Thread row just to discover an empty range.
            .and_where(Expr::exists(
                Query::select()
                    .expr(1)
                    .from_as("thread", "readable_thread")
                    .join_as(
                        JoinType::InnerJoin,
                        "workspace",
                        "readable_workspace",
                        Expr::col(("readable_workspace", "id"))
                            .eq(Expr::col(("readable_thread", "workspace_id"))),
                    )
                    .and_where(
                        Expr::col(("readable_thread", "id")).eq(Expr::col(("block", "thread_id"))),
                    )
                    .and_where(
                        Expr::col(("readable_thread", "workspace_id"))
                            .eq(Expr::col(("block", "workspace_id"))),
                    )
                    .and_where(Expr::col(("readable_thread", "sidebar_visibility")).eq("visible"))
                    .to_owned(),
            ))
            .and_where(Expr::exists(
                Query::select()
                    .expr(1)
                    .from_as("unread_message_position", "publication")
                    .and_where(
                        Expr::col(("publication", "thread_id"))
                            .eq(Expr::col(("block", "thread_id"))),
                    )
                    .and_where(
                        Expr::col(("publication", "block_id")).eq(Expr::col(("block", "block_id"))),
                    )
                    .and_where(
                        Expr::col(("publication", "turn_id")).eq(Expr::col(("block", "turn_id"))),
                    )
                    .and_where(
                        Expr::col(("publication", "published_at"))
                            .gt(Expr::col(("bounds", "started_at"))),
                    )
                    .to_owned(),
            ))
            .and_where(Expr::cust_with_values(
                UNREAD_MESSAGE_PREDICATE.trim_start_matches("AND "),
                [principal_id],
            ));
        Expr::from(query)
    };
    let scalar = |name: &str, args: Vec<SimpleExpr>| -> SimpleExpr {
        Func::cust(Alias::new(name)).args(args).into()
    };
    let timestamp = |alias: &'static str| -> SimpleExpr {
        Expr::from(scalar(
            "strftime",
            vec![
                Expr::val("%s").into(),
                Expr::col((alias, "created_at")).into(),
            ],
        ))
        .cast_as(Alias::new("INTEGER"))
        .if_null(0)
        .into()
    };
    let public_start: SimpleExpr = Expr::col(("thread", "workspace_unread_started_at")).into();
    let public_or_private = Expr::case(
        Expr::col(("tm", "principal_id")).is_null(),
        public_start.clone(),
    )
    .finally(scalar("MIN", vec![public_start, timestamp("tm")]));
    let thread_access = Expr::case(
        Expr::col(("thread", "access_class")).eq("workspace"),
        public_or_private,
    )
    .finally(timestamp("tm"));
    let access = Expr::case(
        Expr::col(("principal", "kind")).eq("superuser"),
        Expr::val(0),
    )
    .finally(scalar("MAX", vec![timestamp("wm"), thread_access.into()]));
    let position = |start: SimpleExpr| {
        scalar(
            "printf",
            vec![
                Expr::val("%020d:").into(),
                Expr::from(start).add(1).mul(1000).into(),
            ],
        )
    };
    let base_start = scalar(
        "MAX",
        vec![
            Expr::col(("gateway", "unread_started_at")).into(),
            timestamp("identity"),
        ],
    );
    // This single current principal row belongs to this SELECT's snapshot.
    // Materialize only its common boundary, so strftime/printf are not repeated
    // for every owner thread. This is neither a cache nor an initialization write.
    let principal = Query::select()
        .column(("identity", "kind"))
        .expr_as(base_start.clone(), "started_at")
        .expr_as(position(base_start), "position")
        .from_as("gateway_principal", "identity")
        .join_as(
            JoinType::InnerJoin,
            "gateway_identity",
            "gateway",
            Expr::col(("gateway", "id")).eq(Expr::col(("identity", "gateway_id"))),
        )
        .and_where(Expr::col(("identity", "id")).eq(principal_id))
        .and_where(Expr::col(("identity", "status")).eq("active"))
        .and_where(Expr::col(("identity", "removed_at")).is_null())
        .to_owned();
    let with = WithClause::new()
        .cte(
            CommonTableExpression::new()
                .table_name("unread_principal")
                .query(principal)
                .materialized(true)
                .to_owned(),
        )
        .to_owned();
    let started_at = scalar(
        "MAX",
        vec![Expr::col(("principal", "started_at")).into(), access.into()],
    );
    let position: SimpleExpr = Expr::case(
        Expr::col(("principal", "kind")).eq("superuser"),
        Expr::col(("principal", "position")),
    )
    .finally(position(started_at.clone()))
    .into();
    let lower = scalar(
        "MAX",
        vec![
            position,
            Expr::col(("cursor", "last_read_sort_key"))
                .if_null("")
                .into(),
            Expr::val(after_override.unwrap_or("")).into(),
        ],
    );
    let mut bounds = Query::select();
    bounds
        .expr_as(Expr::col(("selected", "column1")), "thread_id")
        .expr_as(lower, "lower")
        .expr_as(started_at, "started_at")
        .from_values(thread_ids.iter().map(|id| id.as_str()), "selected")
        .join_as(
            JoinType::InnerJoin,
            "unread_principal",
            "principal",
            Expr::val(true),
        )
        // Owners need no membership-derived boundary. Keep the current ACL
        // joins for users, without reading every Thread row for owners.
        .left_join(
            "thread",
            Expr::col(("thread", "id")).eq(Expr::case(
                Expr::col(("principal", "kind")).eq("user"),
                Expr::col(("selected", "column1")),
            )
            .finally(Expr::val(None::<String>))),
        )
        .join_as(
            JoinType::LeftJoin,
            "thread_read_cursor",
            "cursor",
            Condition::all()
                .add(Expr::col(("cursor", "thread_id")).eq(Expr::col(("selected", "column1"))))
                .add(Expr::col(("cursor", "principal_id")).eq(principal_id)),
        )
        .join_as(
            JoinType::LeftJoin,
            "workspace_membership",
            "wm",
            Condition::all()
                .add(Expr::col(("wm", "workspace_id")).eq(Expr::col(("thread", "workspace_id"))))
                .add(Expr::col(("wm", "principal_id")).eq(principal_id)),
        )
        .join_as(
            JoinType::LeftJoin,
            "thread_membership",
            "tm",
            Condition::all()
                .add(Expr::col(("tm", "thread_id")).eq(Expr::col(("thread", "id"))))
                .add(Expr::col(("tm", "principal_id")).eq(principal_id)),
        )
        .cond_where(
            Condition::any()
                .add(Expr::col(("principal", "kind")).eq("superuser"))
                .add(
                    Condition::all()
                        .add(Expr::col(("principal", "kind")).eq("user"))
                        .add(Expr::col(("wm", "principal_id")).is_not_null())
                        .add(
                            Condition::any()
                                .add(Expr::col(("thread", "access_class")).eq("workspace"))
                                .add(
                                    Condition::all()
                                        .add(Expr::col(("thread", "access_class")).eq("private"))
                                        .add(Expr::col(("tm", "principal_id")).is_not_null()),
                                ),
                        ),
                ),
        );
    let (sql, values) = Query::select()
        .column(("bounds", "thread_id"))
        .expr_as(count(Expr::col(("bounds", "lower"))), "unread_count")
        .from_subquery(bounds.to_owned(), "bounds")
        .with_cte(with)
        .build(SqliteQueryBuilder);
    Statement::from_sql_and_values(DatabaseBackend::Sqlite, sql, values)
}

pub async fn count_unread_message_blocks_for_threads<C: ConnectionTrait>(
    db: &C,
    thread_ids: &[String],
    principal_id: &str,
) -> Result<HashMap<String, u64>> {
    let selected = thread_ids.iter().collect::<std::collections::BTreeSet<_>>();
    let selected = selected.into_iter().collect::<Vec<_>>();
    let mut counts = HashMap::new();
    for batch in selected.chunks(UNREAD_THREAD_BATCH_SIZE) {
        let statement = unread_thread_batch_statement(batch, principal_id, None);
        let rows = db
            .query_all_raw(statement)
            .await
            .context("failed to count unread message blocks for thread batch")?;
        for row in rows {
            let count = row
                .try_get::<i64>("", "unread_count")
                .context("failed to decode unread thread count")?;
            let count = u64::try_from(count).context("unread thread count is negative")?;
            // Preserve the sparse-map contract of the former GROUP BY query.
            if count > 0 {
                let id = row
                    .try_get::<String>("", "thread_id")
                    .context("failed to decode unread thread id")?;
                counts.insert(id, count);
            }
        }
    }
    Ok(counts)
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

/// Refreshes every stale running work-item projection after its Turn has
/// become terminal. The authoritative `turn_item` rows have already been
/// terminalized in the caller's transaction, so this can be expressed as one
/// indexed SQLite update instead of an application-side query/update loop.
///
/// Keeping this operation set-based is important: terminal Turn projection is
/// atomic, but the number of historical work items in a Turn is not a valid
/// correctness limit. A large valid Turn must not fail merely to bound an
/// application loop while the writer is reserved.
pub async fn refresh_terminal_running_turn_work_item_projections<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    source_event_id: &str,
    source_sequence: i64,
    updated_at: DateTimeWithTimeZone,
) -> Result<u64> {
    if db.get_database_backend() != DatabaseBackend::Sqlite {
        anyhow::bail!("terminal work-item projection refresh requires the SQLite store");
    }

    // A work item and an assistant block are mutually exclusive placements.
    // Preserve the existing projector's cleanup invariant without issuing one
    // DELETE for every running row.
    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        r#"
            DELETE FROM thread_timeline_block
            WHERE turn_id = ?
              AND block_kind = 'assistant_message'
              AND source_kind = 'turn_item'
              AND source_key IN (
                  SELECT projection.item_id
                  FROM turn_work_item_projection AS projection
                  WHERE projection.turn_id = ?
                    AND projection.status = 'running'
              )
        "#,
        vec![turn_id.to_owned().into(), turn_id.to_owned().into()],
    ))
    .await
    .with_context(|| {
        format!("failed to remove conflicting assistant blocks for terminal Turn `{turn_id}`")
    })?;

    let result = db
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            r#"
                UPDATE turn_work_item_projection AS projection
                SET source_event_id = ?,
                    source_sequence = ?,
                    status = CASE (
                        SELECT item.status
                        FROM turn_item AS item
                        WHERE item.turn_id = projection.turn_id
                          AND item.item_id = projection.item_id
                    )
                        WHEN 'in_progress' THEN 'running'
                        WHEN 'running' THEN 'running'
                        WHEN 'failed' THEN 'failed'
                        WHEN 'timed_out' THEN 'failed'
                        WHEN 'cancelled' THEN 'cancelled'
                        WHEN 'canceled' THEN 'cancelled'
                        ELSE 'completed'
                    END,
                    completed_at = (
                        SELECT item.updated_at
                        FROM turn_item AS item
                        WHERE item.turn_id = projection.turn_id
                          AND item.item_id = projection.item_id
                    ),
                    updated_at = ?
                WHERE projection.turn_id = ?
                  AND projection.status = 'running'
                  AND EXISTS (
                      SELECT 1
                      FROM turn_item AS item
                      WHERE item.turn_id = projection.turn_id
                        AND item.item_id = projection.item_id
                  )
            "#,
            vec![
                source_event_id.to_owned().into(),
                source_sequence.into(),
                updated_at.into(),
                turn_id.to_owned().into(),
            ],
        ))
        .await
        .with_context(|| {
            format!("failed to refresh running work-item projections for terminal Turn `{turn_id}`")
        })?;

    Ok(result.rows_affected())
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
