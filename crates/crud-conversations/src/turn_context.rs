use anyhow::{Context, Result};
use pioneer_entity::turn_llm_context;
use pioneer_protocol::generate_id;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, DbBackend, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set, Statement,
};

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

const CONTEXT_STREAM_LOCK_SEED: i64 = 0x7069_6f6e_6374_7874;

/// Serialize writes to one context stream, including sources published before
/// their Turn or task link exists. Take this before any task-capsule locks.
pub async fn lock_context_stream(db: &DatabaseTransaction, turn_id: &str) -> Result<()> {
    // PostgreSQL exposes advisory transaction locks as SQL functions.
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "SELECT pg_advisory_xact_lock(hashtextextended($1, $2))",
        [turn_id.into(), CONTEXT_STREAM_LOCK_SEED.into()],
    ))
    .await
    .context("lock turn context stream")?;
    Ok(())
}

pub async fn insert(
    db: &DatabaseTransaction,
    entry: NewTurnLlmContextEntry,
) -> Result<turn_llm_context::Model> {
    lock_context_stream(db, &entry.turn_id).await?;
    store(db, entry, None)
        .await
        .context("failed to insert turn_llm_context row")
}

async fn store(
    db: &DatabaseTransaction,
    entry: NewTurnLlmContextEntry,
    delivery_key: Option<String>,
) -> Result<turn_llm_context::Model> {
    Ok(
        turn_llm_context::Entity::insert(turn_llm_context::ActiveModel {
            id: Set(generate_id(DB_ID_LEN)),
            turn_id: Set(entry.turn_id),
            item_id: Set(entry.item_id),
            attempt_id: Set(entry.attempt_id),
            sequence: Set(entry.sequence),
            source: Set(entry.source),
            tool_name: Set(entry.tool_name),
            payload: Set(entry.payload),
            output_policy_snapshot: Set(entry.output_policy_snapshot),
            delivery_key: Set(delivery_key),
            created_at: Set(entry.created_at),
            expires_at: Set(entry.expires_at),
        })
        .exec_with_returning(db)
        .await?,
    )
}

pub async fn append(
    db: &DatabaseTransaction,
    mut entry: NewTurnLlmContextEntry,
    delivery_key: &str,
) -> Result<TurnLlmContextAppendOutcome> {
    let delivery_key = delivery_key.trim();
    if delivery_key.is_empty() {
        anyhow::bail!("turn_llm_context delivery key must not be empty");
    }
    lock_context_stream(db, &entry.turn_id).await?;
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
    let model = store(db, entry, Some(delivery_key.to_owned()))
        .await
        .context("failed to append turn_llm_context row")?;
    Ok(TurnLlmContextAppendOutcome {
        entry: entry_from_model(model),
        already_committed: false,
    })
}

async fn next_sequence_for_turn(db: &DatabaseTransaction, turn_id: &str) -> Result<i64> {
    let max_sequence = turn_llm_context::Entity::find()
        .select_only()
        .column(turn_llm_context::Column::Sequence)
        .filter(turn_llm_context::Column::TurnId.eq(turn_id))
        .order_by_desc(turn_llm_context::Column::Sequence)
        .into_tuple::<i64>()
        .one(db)
        .await
        .context("failed to query max turn_llm_context sequence")?
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

pub fn entry_from_model(model: turn_llm_context::Model) -> TurnLlmContextEntry {
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
