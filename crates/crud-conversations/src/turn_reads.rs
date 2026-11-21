//! Exact Turn, input, item and history-window reads on the caller's connection.
use anyhow::{Context, Result};
use pioneer_entity::{turn, turn_input, turn_item};
use pioneer_protocol::{Turn, TurnItemType, TurnKind, TurnStatus, TurnWorkOwner, UserInput};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, FromQueryResult, JoinType,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
};

use std::collections::HashMap;

use crate::protocol_projection::{decode_turn_input_model, thread_snapshot_turn_from_db_model};
use crate::turn_item::turn_item_type_from_db;
use crate::values::{turn_kind_to_db, turn_status_to_db};

pub const TURN_MESSAGE_GUARD_PAGE_ROWS: usize = 128;
pub const TURN_MESSAGE_GUARD_PAGE_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, FromQueryResult)]
pub struct TurnMessageGuard {
    pub id: String,
    pub message_revision: i64,
    pub message_deleted_at: Option<DateTimeWithTimeZone>,
}

pub async fn find_turn_by_id<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<turn::Model>> {
    let existing = turn::Entity::find_by_id(turn_id.to_owned())
        .one(db)
        .await
        .context("failed to query turn by id")?;
    Ok(existing)
}

pub async fn find_turn_by_thread_and_id<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<turn::Model>> {
    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Id.eq(turn_id.to_owned()))
        .one(db)
        .await
        .context("failed to query turn by thread and id")
}

/// Immutable Work ownership, with authoritative Turn/thread/workspace scope.
pub async fn find_turn_work_owner<C: ConnectionTrait>(
    db: &C,
    workspace_id: &str,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<TurnWorkOwner>> {
    let owner = turn::Entity::find_by_id(turn_id.to_owned())
        .select_only()
        .column(turn::Column::WorkOwner)
        .filter(turn::Column::ThreadId.eq(thread_id))
        .filter(
            turn::Column::ThreadId.in_subquery(
                pioneer_entity::thread::Entity::find_by_id(thread_id.to_owned())
                    .select_only()
                    .column(pioneer_entity::thread::Column::Id)
                    .filter(pioneer_entity::thread::Column::WorkspaceId.eq(workspace_id))
                    .into_query(),
            ),
        )
        .into_tuple::<String>()
        .one(db)
        .await?;
    owner
        .map(|owner| match owner.as_str() {
            "turn" => Ok(TurnWorkOwner::Turn),
            "detached_task" => Ok(TurnWorkOwner::DetachedTask),
            _ => anyhow::bail!("unknown Turn Work owner"),
        })
        .transpose()
}

pub async fn find_turns_by_thread_and_ids<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_ids: &[String],
) -> Result<Vec<turn::Model>> {
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }

    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Id.is_in(turn_ids.iter().cloned()))
        .all(db)
        .await
        .context("failed to query turns by thread and ids")
}

pub async fn find_turn_message_guards_by_thread_and_ids<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_ids: &[String],
) -> Result<Vec<TurnMessageGuard>> {
    anyhow::ensure!(
        turn_ids.len() <= TURN_MESSAGE_GUARD_PAGE_ROWS,
        "turn message guard page exceeds row bound"
    );
    let encoded_bytes = turn_ids
        .iter()
        .try_fold(2_usize, |bytes, id| -> Result<usize> {
            anyhow::ensure!(
                id.len().saturating_add(3) <= TURN_MESSAGE_GUARD_PAGE_BYTES,
                "one turn message guard id exceeds byte bound"
            );
            Ok(bytes.saturating_add(id.len()).saturating_add(1))
        })?;
    anyhow::ensure!(
        encoded_bytes <= TURN_MESSAGE_GUARD_PAGE_BYTES,
        "turn message guard page exceeds byte bound"
    );
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }

    turn::Entity::find()
        .select_only()
        .column(turn::Column::Id)
        .column(turn::Column::MessageRevision)
        .column(turn::Column::MessageDeletedAt)
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Id.is_in(turn_ids.iter().cloned()))
        .into_model::<TurnMessageGuard>()
        .all(db)
        .await
        .context("failed to query bounded turn message guards")
}

pub async fn has_in_progress_conversation_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<bool> {
    let count = turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Status.eq(turn_status_to_db(TurnStatus::InProgress)))
        .filter(turn::Column::TurnKind.eq(turn_kind_to_db(TurnKind::Conversation)))
        .count(db)
        .await
        .context("failed to count in-progress conversation turns")?;
    Ok(count > 0)
}

pub async fn find_turn_item_type<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<String>> {
    let row = turn_item::Entity::find()
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemId.eq(item_id.to_owned()))
        .one(db)
        .await
        .context("failed to query turn item type")?;
    Ok(row.map(|row| row.item_type))
}

pub use crate::turn_item::find_turn_item_metadata;

pub async fn find_turn_item(
    db: &pioneer_sqlite::SqliteDatabase,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<turn_item::Model>> {
    Ok(crate::turn_payload::items(
        db,
        turn_item::Entity::find()
            .filter(turn_item::Column::TurnId.eq(turn_id))
            .filter(turn_item::Column::ItemId.eq(item_id))
            .limit(1),
    )
    .await?
    .pop())
}

pub async fn is_completed_agent_message<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<bool> {
    // The canonical assistant block supplies final-answer placement. Check
    // lifecycle state without fetching/parsing its potentially large payload.
    Ok(turn_item::Entity::find()
        .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_item::Column::ItemId.eq(item_id.to_owned()))
        .filter(turn_item::Column::ItemType.eq("agent_message"))
        .filter(turn_item::Column::Status.eq("completed"))
        .count(db)
        .await
        .context("failed to validate completed response position")?
        > 0)
}

/// Exact page bodies plus a metadata-only final-answer decision from the same
/// SQL snapshot. No non-page body participates in the final-answer lookup.
#[derive(Debug, FromQueryResult)]
pub struct TimelineAssistantItemRow {
    pub payload_payload_id: Option<String>,
    pub turn_id: String,
    pub item_id: String,
    pub item_type: String,
    inline_payload: Option<String>,
    #[sea_orm(skip)]
    pub payload: String,
    pub status: Option<String>,
    pub is_last_final: bool,
}

pub async fn timeline_assistant_items(
    db: &pioneer_sqlite::SqliteDatabase,
    workspace_id: &str,
    thread_id: &str,
    targets: &[(String, String)],
) -> Result<Vec<TimelineAssistantItemRow>> {
    use pioneer_entity::{thread, thread_timeline_block as block};
    let reader = db.begin_read().await?;
    let mut result = Vec::with_capacity(targets.len());
    for targets in targets.chunks(64) {
        let mut exact = Condition::any();
        for (turn, item) in targets {
            exact = exact.add(
                Condition::all()
                    .add(turn_item::Column::TurnId.eq(turn.clone()))
                    .add(turn_item::Column::ItemId.eq(item.clone())),
            );
        }
        // The projection classifies final answers, including legacy messages
        // whose omitted phase defaults to FinalAnswer. Canonical item order is
        // (created_at,item_id), not publication/sort_key or completion time.
        let newer = Query::select()
            .expr(Expr::val(1_i64))
            .from_as(turn_item::Entity, Alias::new("later_item"))
            .join(
                JoinType::InnerJoin,
                block::Entity,
                Condition::all()
                    .add(
                        Expr::col((block::Entity, block::Column::TurnId)).eq(Expr::col((
                            Alias::new("later_item"),
                            turn_item::Column::TurnId,
                        ))),
                    )
                    .add(
                        Expr::col((block::Entity, block::Column::SourceKey)).eq(Expr::col((
                            Alias::new("later_item"),
                            turn_item::Column::ItemId,
                        ))),
                    ),
            )
            .and_where(
                Expr::col((Alias::new("later_item"), turn_item::Column::TurnId))
                    .eq(Expr::col((turn_item::Entity, turn_item::Column::TurnId))),
            )
            .and_where(block::Column::WorkspaceId.eq(workspace_id))
            .and_where(block::Column::ThreadId.eq(thread_id))
            .and_where(
                block::Column::BlockKind
                    .eq(crate::thread_timeline_projection::BLOCK_KIND_ASSISTANT_MESSAGE),
            )
            .and_where(
                Expr::col((Alias::new("later_item"), turn_item::Column::ItemType))
                    .eq("agent_message"),
            )
            .cond_where(
                Condition::any()
                    .add(
                        Expr::col((Alias::new("later_item"), turn_item::Column::CreatedAt))
                            .gt(Expr::col((turn_item::Entity, turn_item::Column::CreatedAt))),
                    )
                    .add(
                        Condition::all()
                            .add(
                                Expr::col((Alias::new("later_item"), turn_item::Column::CreatedAt))
                                    .eq(Expr::col((
                                        turn_item::Entity,
                                        turn_item::Column::CreatedAt,
                                    ))),
                            )
                            .add(
                                Expr::col((Alias::new("later_item"), turn_item::Column::ItemId))
                                    .gt(Expr::col((turn_item::Entity, turn_item::Column::ItemId))),
                            ),
                    ),
            )
            .to_owned();
        let rows = turn_item::Entity::find()
            .select_only()
            .columns([
                turn_item::Column::TurnId,
                turn_item::Column::ItemId,
                turn_item::Column::ItemType,
                turn_item::Column::PayloadPayloadId,
                turn_item::Column::Status,
            ])
            .expr_as(
                Expr::col(turn_item::Column::Payload).cast_as("text"),
                "inline_payload",
            )
            .column_as(Expr::exists(newer).not(), "is_last_final")
            .join(
                JoinType::InnerJoin,
                turn_item::Entity::belongs_to(turn::Entity)
                    .from(turn_item::Column::TurnId)
                    .to(turn::Column::Id)
                    .into(),
            )
            .join(
                JoinType::InnerJoin,
                turn::Entity::belongs_to(thread::Entity)
                    .from(turn::Column::ThreadId)
                    .to(thread::Column::Id)
                    .into(),
            )
            .filter(thread::Column::WorkspaceId.eq(workspace_id))
            .filter(turn::Column::ThreadId.eq(thread_id))
            .filter(exact)
            .into_model::<TimelineAssistantItemRow>()
            .all(&reader)
            .await
            .context("failed to load exact timeline assistant items")?;
        result.extend(rows);
    }
    let mut captured = Vec::with_capacity(result.len());
    for row in &result {
        captured.push(capture_item_text(&reader, row.payload_payload_id.as_deref()).await?);
    }
    reader.commit().await?;
    for (row, body) in result.iter_mut().zip(captured) {
        row.payload = match body {
            Some(body) => body.into_text()?,
            None => row
                .inline_payload
                .take()
                .context("missing inline turn item")?,
        };
    }
    Ok(result)
}

/// Only exact source messages, never all bodies in a source Turn.
#[derive(FromQueryResult)]
pub struct ExactAgentMessageRow {
    pub payload_payload_id: Option<String>,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    inline_payload: Option<String>,
    #[sea_orm(skip)]
    pub payload: String,
}

pub async fn exact_agent_messages(
    db: &pioneer_sqlite::SqliteDatabase,
    workspace: &str,
    targets: &[(String, String, String)],
) -> Result<Vec<ExactAgentMessageRow>> {
    use pioneer_entity::thread;
    let reader = db.begin_read().await?;
    let mut rows = Vec::new();
    for targets in targets.chunks(64) {
        let mut exact = Condition::any();
        for (thread, turn, item) in targets {
            exact = exact.add(
                Condition::all()
                    .add(turn::Column::ThreadId.eq(thread.clone()))
                    .add(turn_item::Column::TurnId.eq(turn.clone()))
                    .add(turn_item::Column::ItemId.eq(item.clone())),
            );
        }
        rows.extend(
            turn_item::Entity::find()
                .select_only()
                .column_as(turn::Column::ThreadId, "thread_id")
                .columns([
                    turn_item::Column::TurnId,
                    turn_item::Column::ItemId,
                    turn_item::Column::PayloadPayloadId,
                ])
                .expr_as(
                    Expr::col(turn_item::Column::Payload).cast_as("text"),
                    "inline_payload",
                )
                .join(
                    JoinType::InnerJoin,
                    turn_item::Entity::belongs_to(turn::Entity)
                        .from(turn_item::Column::TurnId)
                        .to(turn::Column::Id)
                        .into(),
                )
                .join(
                    JoinType::InnerJoin,
                    turn::Entity::belongs_to(thread::Entity)
                        .from(turn::Column::ThreadId)
                        .to(thread::Column::Id)
                        .into(),
                )
                .filter(thread::Column::WorkspaceId.eq(workspace))
                .filter(turn_item::Column::ItemType.eq("agent_message"))
                .filter(exact)
                .into_model::<ExactAgentMessageRow>()
                .all(&reader)
                .await
                .context("failed to load exact source messages")?,
        );
    }
    let mut captured = Vec::with_capacity(rows.len());
    for row in &rows {
        captured.push(capture_item_text(&reader, row.payload_payload_id.as_deref()).await?);
    }
    reader.commit().await?;
    for (row, body) in rows.iter_mut().zip(captured) {
        row.payload = match body {
            Some(body) => body.into_text()?,
            None => row
                .inline_payload
                .take()
                .context("missing inline turn item")?,
        };
    }
    Ok(rows)
}

async fn capture_item_text<C: ConnectionTrait>(
    db: &C,
    id: Option<&str>,
) -> Result<Option<pioneer_crud_payloads::storage::CapturedPayload>> {
    use pioneer_crud_payloads::storage::{self, Selection};
    match id {
        Some(id) => Ok(Some(
            storage::capture(db, id, Selection::Full)
                .await?
                .context("turn item payload disappeared")?,
        )),
        None => Ok(None),
    }
}

pub async fn list_turn_items_by_ids(
    db: &pioneer_sqlite::SqliteDatabase,
    turn_id: &str,
    item_ids: &[String],
) -> Result<Vec<turn_item::Model>> {
    if item_ids.is_empty() {
        return Ok(Vec::new());
    }

    crate::turn_payload::items(
        db,
        turn_item::Entity::find()
            .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
            .filter(turn_item::Column::ItemId.is_in(item_ids.iter().cloned())),
    )
    .await
    .context("failed to query turn item rows by id")
}

pub async fn list_turn_items_by_type(
    db: &pioneer_sqlite::SqliteDatabase,
    turn_id: &str,
    item_type: &str,
) -> Result<Vec<turn_item::Model>> {
    crate::turn_payload::items(
        db,
        turn_item::Entity::find()
            .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
            .filter(turn_item::Column::ItemType.eq(item_type.to_owned()))
            .order_by_asc(turn_item::Column::CreatedAt)
            .order_by_asc(turn_item::Column::ItemId),
    )
    .await
    .context("failed to query turn item rows by type")
}

pub async fn list_turn_items_by_type_for_turns(
    db: &pioneer_sqlite::SqliteDatabase,
    turn_ids: &[String],
    item_type: &str,
) -> Result<Vec<turn_item::Model>> {
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }

    crate::turn_payload::items(
        db,
        turn_item::Entity::find()
            .filter(turn_item::Column::TurnId.is_in(turn_ids.iter().cloned()))
            .filter(turn_item::Column::ItemType.eq(item_type.to_owned()))
            .order_by_asc(turn_item::Column::TurnId)
            .order_by_asc(turn_item::Column::CreatedAt)
            .order_by_asc(turn_item::Column::ItemId),
    )
    .await
    .context("failed to query turn item rows by type for turns")
}

pub async fn find_terminal_turns_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    limit: u64,
) -> Result<Vec<turn::Model>> {
    if limit == 0 {
        return Ok(Vec::new());
    }

    let terminal = turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id))
        .filter(turn::Column::Status.is_in(["completed", "failed", "interrupted"]));

    // Resolve the cutoff and include its full timestamp bucket in one
    // statement, so concurrent commits cannot change the chosen boundary.
    let window = terminal
        .clone()
        .select_only()
        .column(turn::Column::CreatedAt)
        .order_by_desc(turn::Column::CreatedAt)
        .limit(limit)
        .into_query();
    let boundary = Query::select()
        .expr(Expr::col(turn::Column::CreatedAt).min())
        .from_subquery(window, "terminal_window")
        .to_owned();
    terminal
        .filter(
            Expr::col(turn::Column::CreatedAt).gte(Expr::SubQuery(None, Box::new(boundary.into()))),
        )
        .order_by_asc(turn::Column::CreatedAt)
        .order_by_asc(Expr::cust_with_expr(
            r#"$1 COLLATE "C""#,
            Expr::col(turn::Column::Id),
        ))
        .all(db)
        .await
        .context("failed to query terminal turns for thread")
}

pub async fn find_latest_turn_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<turn::Model>> {
    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_desc(turn::Column::CreatedAt)
        .order_by_desc(turn::Column::Id)
        .one(db)
        .await
        .context("failed to query latest turn for thread")
}

pub async fn find_latest_conversation_turn_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<turn::Model>> {
    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::TurnKind.eq(turn_kind_to_db(TurnKind::Conversation)))
        .order_by_desc(turn::Column::CreatedAt)
        .order_by_desc(turn::Column::Id)
        .one(db)
        .await
        .context("failed to query latest conversation turn for thread")
}

/// A Composer TaskRun occurrence can sort before its source in the same
/// timestamp bucket. It is not the first user message used for an automatic name.
pub async fn find_oldest_user_conversation_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Option<turn::Model>> {
    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::TurnKind.eq("conversation"))
        .filter(turn::Column::Origin.eq("user"))
        .order_by_asc(turn::Column::CreatedAt)
        .order_by_asc(turn::Column::Id)
        .one(db)
        .await
        .context("failed to query first user conversation turn")
}

pub async fn find_turn_inputs<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<turn_input::Model>> {
    turn_input::Entity::find()
        .filter(turn_input::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(turn_input::Column::InputIndex)
        .all(db)
        .await
        .context("failed to query turn inputs")
}

pub async fn find_turn_inputs_for_turns<C: ConnectionTrait>(
    db: &C,
    turn_ids: &[String],
) -> Result<Vec<turn_input::Model>> {
    if turn_ids.is_empty() {
        return Ok(Vec::new());
    }

    turn_input::Entity::find()
        .filter(turn_input::Column::TurnId.is_in(turn_ids.iter().cloned()))
        .order_by_asc(turn_input::Column::TurnId)
        .order_by_asc(turn_input::Column::InputIndex)
        .all(db)
        .await
        .context("failed to query turn inputs for turns")
}

pub async fn find_completed_turn_items(
    db: &pioneer_sqlite::SqliteDatabase,
    turn_id: &str,
) -> Result<Vec<turn_item::Model>> {
    crate::turn_payload::items(
        db,
        turn_item::Entity::find()
            .filter(turn_item::Column::TurnId.eq(turn_id.to_owned()))
            .filter(turn_item::Column::ItemType.eq("agent_message"))
            .filter(turn_item::Column::Status.eq("completed"))
            .order_by_asc(turn_item::Column::CreatedAt)
            .order_by_asc(turn_item::Column::ItemId),
    )
    .await
    .context("failed to query completed turn items")
}

pub async fn count_completed_turns_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<u64> {
    let count = turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Status.eq("completed"))
        .count(db)
        .await
        .context("failed to count completed turns for thread")?;
    Ok(count)
}

pub async fn find_completed_turns_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
) -> Result<Vec<turn::Model>> {
    turn::Entity::find()
        .filter(turn::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn::Column::Status.eq("completed"))
        .order_by_asc(turn::Column::CreatedAt)
        .order_by_asc(turn::Column::Id)
        .all(db)
        .await
        .context("failed to query completed turns for thread")
}

pub async fn get_turn_location(
    db: &impl ConnectionTrait,
    turn_id: &str,
) -> Result<Option<(String, String)>> {
    let Some(turn_model) = find_turn_by_id(db, turn_id).await? else {
        return Ok(None);
    };
    let Some(thread_model) =
        crate::thread_metadata::find_thread_by_id(db, turn_model.thread_id.as_str()).await?
    else {
        return Ok(None);
    };
    Ok(Some((turn_model.thread_id, thread_model.workspace_id)))
}

pub async fn get_turn_for_thread(
    db: &pioneer_sqlite::SqliteDatabase,
    thread_id: &str,
    turn_id: &str,
) -> Result<Option<(String, pioneer_protocol::Turn)>> {
    let Some(thread_model) = crate::thread_metadata::find_thread_by_id(db, thread_id).await? else {
        return Ok(None);
    };

    let Some(turn_model) = find_turn_by_thread_and_id(db, thread_id, turn_id).await? else {
        return Ok(None);
    };

    Ok(crate::protocol_projection::turn_from_db_model(turn_model)?
        .map(|turn| (thread_model.workspace_id, turn)))
}

pub async fn get_turns_by_thread_and_ids(
    db: &impl ConnectionTrait,
    thread_id: &str,
    turn_ids: &[String],
) -> Result<HashMap<String, Turn>> {
    let rows = find_turns_by_thread_and_ids(db, thread_id, turn_ids).await?;
    rows.into_iter()
        .filter_map(|row| match thread_snapshot_turn_from_db_model(row) {
            Ok(Some(turn)) => Some(Ok((turn.id.clone(), turn))),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

pub async fn get_turn_inputs(db: &impl ConnectionTrait, turn_id: &str) -> Result<Vec<UserInput>> {
    let rows = find_turn_inputs(db, turn_id).await?;
    let mut inputs = Vec::with_capacity(rows.len());

    for row in rows {
        inputs.push(decode_turn_input_model(row)?);
    }

    Ok(inputs)
}

pub async fn get_turn_inputs_for_turns(
    db: &impl ConnectionTrait,
    turn_ids: &[String],
) -> Result<HashMap<String, Vec<UserInput>>> {
    let rows = find_turn_inputs_for_turns(db, turn_ids).await?;
    let mut inputs_by_turn = turn_ids
        .iter()
        .cloned()
        .map(|turn_id| (turn_id, Vec::new()))
        .collect::<HashMap<_, _>>();
    for row in rows {
        let turn_id = row.turn_id.clone();
        inputs_by_turn
            .entry(turn_id)
            .or_default()
            .push(decode_turn_input_model(row)?);
    }
    Ok(inputs_by_turn)
}

pub async fn get_turn_item_type(
    db: &impl ConnectionTrait,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<TurnItemType>> {
    let value = find_turn_item_type(db, turn_id, item_id).await?;
    Ok(match value {
        Some(value) => {
            Some(turn_item_type_from_db(value.as_str()).unwrap_or(TurnItemType::DynamicToolCall))
        }
        None => None,
    })
}

pub async fn get_turn_item(
    db: &pioneer_sqlite::SqliteDatabase,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<pioneer_protocol::TurnItem>> {
    let Some(model) = find_turn_item(db, turn_id, item_id).await? else {
        return Ok(None);
    };
    let parsed = crate::turn_payload::decode_item(&model).with_context(|| {
        format!("failed to decode turn_item payload for turn `{turn_id}` item `{item_id}`")
    })?;
    Ok(Some(parsed))
}

pub async fn list_decoded_turn_items_by_type(
    db: &pioneer_sqlite::SqliteDatabase,
    turn_id: &str,
    item_type: &str,
) -> Result<Vec<pioneer_protocol::TurnItem>> {
    let rows = list_turn_items_by_type(db, turn_id, item_type).await?;
    rows.into_iter()
        .map(|model| {
            crate::turn_payload::decode_item(&model).with_context(|| {
                format!(
                    "failed to decode turn item payload for turn `{turn_id}` item `{}`",
                    model.item_id
                )
            })
        })
        .collect()
}
