use anyhow::{Context, Result};
use pioneer_entity::{turn, turn_llm_context};
use pioneer_protocol::{TurnStatus, generate_id};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, Set};

use crate::convention::turn_status_to_db;

const DB_ID_LEN: usize = 21;

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnLlmContextAppendOutcome {
    pub entry: TurnLlmContextEntry,
    pub already_committed: bool,
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
        delivery_key: Set(None),
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

pub async fn append_turn_llm_context<C: ConnectionTrait>(
    db: &C,
    mut entry: NewTurnLlmContextEntry,
    delivery_key: &str,
) -> Result<TurnLlmContextAppendOutcome> {
    let delivery_key = delivery_key.trim();
    if delivery_key.is_empty() {
        anyhow::bail!("turn_llm_context delivery key must not be empty");
    }
    if let Some(existing) = turn_llm_context::Entity::find()
        .filter(turn_llm_context::Column::TurnId.eq(entry.turn_id.clone()))
        .filter(turn_llm_context::Column::DeliveryKey.eq(delivery_key.to_owned()))
        .one(db)
        .await
        .context("failed to query idempotent turn_llm_context delivery")?
    {
        if existing.item_id != entry.item_id
            || existing.attempt_id != entry.attempt_id
            || existing.source != entry.source
            || existing.tool_name != entry.tool_name
            || existing.payload != entry.payload
            || existing.output_policy_snapshot != entry.output_policy_snapshot
            || existing.expires_at != entry.expires_at
        {
            anyhow::bail!(
                "turn_llm_context delivery key collision for turn `{}`",
                entry.turn_id
            );
        }
        return Ok(TurnLlmContextAppendOutcome {
            entry: entry_from_model(existing),
            already_committed: true,
        });
    }

    entry.sequence = next_sequence_for_turn(db, entry.turn_id.as_str()).await?;
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
        delivery_key: Set(Some(delivery_key.to_owned())),
        created_at: Set(entry.created_at),
        expires_at: Set(entry.expires_at),
    })
    .exec(db)
    .await
    .context("failed to append turn_llm_context row")?;

    let model = turn_llm_context::Entity::find_by_id(id)
        .one(db)
        .await
        .context("failed to reload appended turn_llm_context row")?
        .context("appended turn_llm_context row is missing")?;
    Ok(TurnLlmContextAppendOutcome {
        entry: entry_from_model(model),
        already_committed: false,
    })
}

async fn next_sequence_for_turn<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<i64> {
    let max_sequence = db
        .query_one(
            &Query::select()
                .expr_as(
                    Expr::cust("COALESCE(MAX(sequence), 0)"),
                    Alias::new("max_sequence"),
                )
                .from(Alias::new("turn_llm_context"))
                .and_where(Expr::col(Alias::new("turn_id")).eq(turn_id.to_owned()))
                .to_owned(),
        )
        .await
        .context("failed to query max turn_llm_context sequence")?
        .and_then(|row| {
            row.try_get::<Option<i64>>("", "max_sequence")
                .ok()
                .flatten()
        })
        .unwrap_or(0);
    Ok(max_sequence.saturating_add(1))
}

pub async fn list_turn_llm_context<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Vec<TurnLlmContextEntry>> {
    let rows = turn_llm_context::Entity::find()
        .filter(turn_llm_context::Column::TurnId.eq(turn_id.to_owned()))
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
        turn_status_to_db(TurnStatus::Blocked).to_owned(),
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
