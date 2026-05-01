use anyhow::{Context, Result};
use pioneer_entity::{turn, turn_llm_context};
use pioneer_protocol::{TurnStatus, generate_id};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{ColumnTrait, Condition, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

use crate::convention::turn_status_to_db;

const DB_ID_LEN: usize = 21;

#[derive(Debug, Clone)]
pub struct TurnLlmContextEntry {
    pub id: String,
    pub turn_id: String,
    pub item_id: Option<String>,
    pub attempt_id: Option<String>,
    pub sequence: i64,
    pub source: String,
    pub tool_name: Option<String>,
    pub payload: String,
    pub output_policy_snapshot: String,
    pub created_at: DateTimeWithTimeZone,
    pub expires_at: Option<DateTimeWithTimeZone>,
}

#[derive(Debug, Clone)]
pub struct NewTurnLlmContextEntry {
    pub turn_id: String,
    pub item_id: Option<String>,
    pub attempt_id: Option<String>,
    pub sequence: i64,
    pub source: String,
    pub tool_name: Option<String>,
    pub payload: String,
    pub output_policy_snapshot: String,
    pub created_at: DateTimeWithTimeZone,
    pub expires_at: Option<DateTimeWithTimeZone>,
}

pub async fn insert_turn_llm_context<C: ConnectionTrait>(
    db: &C,
    entry: NewTurnLlmContextEntry,
) -> Result<turn_llm_context::Model> {
    let id = generate_id(DB_ID_LEN);
    turn_llm_context::Entity::insert(turn_llm_context::ActiveModel {
        id: Set(id.clone()),
        turn_id: Set(entry.turn_id),
        item_id: Set(entry.item_id),
        attempt_id: Set(entry.attempt_id),
        sequence: Set(entry.sequence),
        source: Set(entry.source),
        tool_name: Set(entry.tool_name),
        payload: Set(entry.payload),
        output_policy_snapshot: Set(entry.output_policy_snapshot),
        created_at: Set(entry.created_at),
        expires_at: Set(entry.expires_at),
    })
    .exec(db)
    .await
    .context("failed to insert turn_llm_context row")?;

    turn_llm_context::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload inserted turn_llm_context row")?
        .context("inserted turn_llm_context row is missing")
}

pub async fn list_turn_llm_context<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<TurnLlmContextEntry>> {
    let rows = turn_llm_context::Entity::find()
        .filter(turn_llm_context::Column::TurnId.eq(turn_id.to_owned()))
        .filter(
            Condition::any()
                .add(turn_llm_context::Column::ExpiresAt.is_null())
                .add(turn_llm_context::Column::ExpiresAt.gt(chrono::Utc::now().fixed_offset())),
        )
        .order_by_asc(turn_llm_context::Column::Sequence)
        .all(db)
        .await
        .context("failed to list turn_llm_context rows")?;

    Ok(rows.into_iter().map(entry_from_model).collect())
}

pub async fn delete_turn_llm_context_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<u64> {
    let deleted = turn_llm_context::Entity::delete_many()
        .filter(turn_llm_context::Column::TurnId.eq(turn_id.to_owned()))
        .exec(db)
        .await
        .context("failed to delete turn_llm_context rows for turn")?;
    Ok(deleted.rows_affected)
}

pub async fn delete_expired_turn_llm_context<C: ConnectionTrait>(db: &C) -> Result<u64> {
    let deleted = turn_llm_context::Entity::delete_many()
        .filter(turn_llm_context::Column::ExpiresAt.is_not_null())
        .filter(turn_llm_context::Column::ExpiresAt.lte(chrono::Utc::now().fixed_offset()))
        .exec(db)
        .await
        .context("failed to delete expired turn_llm_context rows")?;
    Ok(deleted.rows_affected)
}

pub async fn delete_turn_llm_context_for_terminal_turns<C: ConnectionTrait>(db: &C) -> Result<u64> {
    let terminal_statuses = [
        turn_status_to_db(TurnStatus::Completed).to_owned(),
        turn_status_to_db(TurnStatus::Failed).to_owned(),
        turn_status_to_db(TurnStatus::Interrupted).to_owned(),
    ];

    let terminal_turn_ids = turn::Entity::find()
        .filter(turn::Column::Status.is_in(terminal_statuses))
        .all(db)
        .await
        .context("failed to list terminal turns for turn_llm_context cleanup")?
        .into_iter()
        .map(|turn| turn.id)
        .collect::<Vec<_>>();

    if terminal_turn_ids.is_empty() {
        return Ok(0);
    }

    let deleted = turn_llm_context::Entity::delete_many()
        .filter(turn_llm_context::Column::TurnId.is_in(terminal_turn_ids))
        .exec(db)
        .await
        .context("failed to delete terminal turn_llm_context rows")?;
    Ok(deleted.rows_affected)
}

fn entry_from_model(model: turn_llm_context::Model) -> TurnLlmContextEntry {
    TurnLlmContextEntry {
        id: model.id,
        turn_id: model.turn_id,
        item_id: model.item_id,
        attempt_id: model.attempt_id,
        sequence: model.sequence,
        source: model.source,
        tool_name: model.tool_name,
        payload: model.payload,
        output_policy_snapshot: model.output_policy_snapshot,
        created_at: model.created_at,
        expires_at: model.expires_at,
    }
}
