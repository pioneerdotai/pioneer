use anyhow::{Context, Result};
use pioneer_entity::turn_event;
use pioneer_protocol::TurnItem;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
use sea_orm::{ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect};

use crate::events::{AppendedTurnEvent, TurnEventPayload};
use pioneer_protocol::generate_id;

const DB_ID_LEN: usize = 21;

pub async fn append_event<C: ConnectionTrait>(
    db: &C,
    payload: &TurnEventPayload,
    created_at: DateTimeWithTimeZone,
) -> Result<AppendedTurnEvent> {
    let thread_id = payload.thread_id().to_owned();
    let turn_id = payload.turn_id().to_owned();

    let event_type = payload.event_type().to_owned();

    let payload_json =
        serde_json::to_string(payload).context("failed to serialize turn event payload")?;

    let idempotency_key = payload
        .idempotency_key()
        .context("failed to derive turn event idempotency key")?;
    if let Some(existing) = turn_event::Entity::find()
        .filter(turn_event::Column::TurnId.eq(turn_id.clone()))
        .filter(turn_event::Column::IdempotencyKey.eq(idempotency_key.clone()))
        .one(db)
        .await
        .context("failed to query idempotent turn event")?
    {
        let mut existing = appended_event_from_model(existing)?;
        if existing.payload != *payload {
            anyhow::bail!("turn event idempotency key collision for turn `{turn_id}`");
        }
        existing.was_inserted = false;
        return Ok(existing);
    }

    let sequence = next_sequence_for_turn(db, turn_id.as_str()).await?;

    let id = generate_id(DB_ID_LEN);

    let mut insert = Query::insert();

    insert
        .into_table(Alias::new("turn_event"))
        .columns([
            Alias::new("id"),
            Alias::new("thread_id"),
            Alias::new("turn_id"),
            Alias::new("sequence"),
            Alias::new("event_type"),
            Alias::new("payload"),
            Alias::new("idempotency_key"),
            Alias::new("created_at"),
        ])
        .values_panic([
            id.clone().into(),
            thread_id.clone().into(),
            turn_id.clone().into(),
            sequence.into(),
            event_type.clone().into(),
            payload_json.into(),
            idempotency_key.clone().into(),
            created_at.into(),
        ]);

    db.execute(&insert)
        .await
        .context("failed to append turn event")?;

    Ok(AppendedTurnEvent {
        id,
        thread_id,
        turn_id,
        sequence,
        payload: payload.clone(),
        idempotency_key: Some(idempotency_key),
        was_inserted: true,
        created_at,
    })
}

pub fn appended_event_from_model(model: turn_event::Model) -> Result<AppendedTurnEvent> {
    let payload: TurnEventPayload = serde_json::from_str(model.payload.as_str())
        .with_context(|| format!("failed to deserialize turn_event `{}` payload", model.id))?;

    Ok(AppendedTurnEvent {
        id: model.id,
        thread_id: model.thread_id,
        turn_id: model.turn_id,
        sequence: model.sequence,
        payload,
        idempotency_key: model.idempotency_key,
        was_inserted: false,
        created_at: model.created_at,
    })
}

pub async fn find_event_by_id<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
) -> Result<Option<AppendedTurnEvent>> {
    turn_event::Entity::find_by_id(event_id.to_owned())
        .one(db)
        .await
        .context("failed to query turn_event by id")?
        .map(appended_event_from_model)
        .transpose()
}

pub async fn find_event_by_idempotency_key<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    idempotency_key: &str,
) -> Result<Option<AppendedTurnEvent>> {
    turn_event::Entity::find()
        .filter(turn_event::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_event::Column::IdempotencyKey.eq(idempotency_key.to_owned()))
        .one(db)
        .await
        .context("failed to query turn_event by idempotency key")?
        .map(appended_event_from_model)
        .transpose()
}

/// Repairs a legacy final agent-diff snapshot whose denormalized thread owner
/// came from the reusable CLI session instead of the Turn binding. The raw
/// event column, canonical payload, and payload-derived idempotency key must
/// move together or subsequent idempotent appends would no longer recognize
/// the repaired event.
pub async fn repair_legacy_agent_diff_thread_owner<C: ConnectionTrait>(
    db: &C,
    event_id: &str,
    turn_id: &str,
    legacy_thread_id: &str,
    canonical_thread_id: &str,
) -> Result<bool> {
    let model = turn_event::Entity::find_by_id(event_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to load legacy agent diff event `{event_id}`"))?
        .with_context(|| format!("legacy agent diff event `{event_id}` is missing"))?;

    if model.turn_id != turn_id {
        anyhow::bail!(
            "legacy agent diff event `{event_id}` belongs to Turn `{}`, not `{turn_id}`",
            model.turn_id
        );
    }
    if model.thread_id == canonical_thread_id {
        let payload = appended_event_from_model(model)?;
        if payload.payload.thread_id() != canonical_thread_id {
            anyhow::bail!(
                "legacy agent diff event `{event_id}` has a canonical row owner but a mismatched payload owner"
            );
        }
        return Ok(false);
    }
    if model.thread_id != legacy_thread_id {
        anyhow::bail!(
            "legacy agent diff event `{event_id}` belongs to thread `{}`, not expected legacy thread `{legacy_thread_id}`",
            model.thread_id
        );
    }
    if model.event_type != pioneer_protocol::constants::events::ITEM_COMPLETED {
        anyhow::bail!(
            "cross-thread event `{event_id}` has unsupported type `{}`",
            model.event_type
        );
    }

    let mut payload = serde_json::from_str::<TurnEventPayload>(model.payload.as_str())
        .with_context(|| format!("failed to decode legacy agent diff event `{event_id}`"))?;
    let TurnEventPayload::ItemCompleted(notification) = &mut payload else {
        anyhow::bail!("cross-thread event `{event_id}` is not an item/completed payload");
    };
    if notification.turn_id != turn_id || notification.thread_id != legacy_thread_id {
        anyhow::bail!(
            "cross-thread agent diff event `{event_id}` payload ownership does not match its persisted row"
        );
    }
    if !matches!(
        &notification.item,
        TurnItem::SystemEvent {
            code: Some(code),
            ..
        } if code == "agent_diff_updated"
    ) {
        anyhow::bail!(
            "cross-thread event `{event_id}` is not a repairable final agent diff snapshot"
        );
    }
    notification.thread_id = canonical_thread_id.to_owned();

    let repaired_payload = serde_json::to_string(&payload)
        .with_context(|| format!("failed to encode repaired agent diff event `{event_id}`"))?;
    let repaired_idempotency_key = payload.idempotency_key().with_context(|| {
        format!("failed to derive repaired agent diff event `{event_id}` identity")
    })?;
    if let Some(conflict) = turn_event::Entity::find()
        .filter(turn_event::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_event::Column::IdempotencyKey.eq(repaired_idempotency_key.clone()))
        .filter(turn_event::Column::Id.ne(event_id.to_owned()))
        .one(db)
        .await
        .with_context(|| {
            format!("failed to check repaired agent diff event `{event_id}` identity")
        })?
    {
        anyhow::bail!(
            "cannot repair agent diff event `{event_id}` because event `{}` already owns its canonical identity",
            conflict.id
        );
    }

    let original_payload = model.payload;
    let original_idempotency_key = model.idempotency_key;
    let mut update = turn_event::Entity::update_many()
        .col_expr(
            turn_event::Column::ThreadId,
            Expr::value(canonical_thread_id.to_owned()),
        )
        .col_expr(
            turn_event::Column::Payload,
            Expr::value(repaired_payload.clone()),
        )
        .col_expr(
            turn_event::Column::IdempotencyKey,
            Expr::value(Some(repaired_idempotency_key.clone())),
        )
        .filter(turn_event::Column::Id.eq(event_id.to_owned()))
        .filter(turn_event::Column::TurnId.eq(turn_id.to_owned()))
        .filter(turn_event::Column::ThreadId.eq(legacy_thread_id.to_owned()))
        .filter(turn_event::Column::Payload.eq(original_payload));
    update = match original_idempotency_key {
        Some(idempotency_key) => {
            update.filter(turn_event::Column::IdempotencyKey.eq(idempotency_key))
        }
        None => update.filter(turn_event::Column::IdempotencyKey.is_null()),
    };

    let changed = update
        .exec(db)
        .await
        .with_context(|| format!("failed to repair legacy agent diff event `{event_id}`"))?
        .rows_affected;
    if changed > 1 {
        anyhow::bail!(
            "legacy agent diff event `{event_id}` ownership repair changed {changed} rows"
        );
    }

    // SQLite reports zero affected rows for a successful UPDATE routed through
    // the INSTEAD OF trigger on the transparent Zstd view. Verify the durable
    // result instead of interpreting that driver count as a failed compare-and-set.
    let repaired = turn_event::Entity::find_by_id(event_id.to_owned())
        .one(db)
        .await
        .with_context(|| format!("failed to verify repaired agent diff event `{event_id}`"))?
        .with_context(|| format!("repaired agent diff event `{event_id}` disappeared"))?;
    if repaired.thread_id != canonical_thread_id
        || repaired.payload != repaired_payload
        || repaired.idempotency_key.as_deref() != Some(repaired_idempotency_key.as_str())
    {
        anyhow::bail!("legacy agent diff event `{event_id}` changed during ownership repair");
    }
    Ok(true)
}

pub async fn latest_event_for_turn<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
) -> Result<Option<AppendedTurnEvent>> {
    turn_event::Entity::find()
        .filter(turn_event::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_desc(turn_event::Column::Sequence)
        .one(db)
        .await
        .context("failed to query latest turn_event")?
        .map(appended_event_from_model)
        .transpose()
}

async fn next_sequence_for_turn<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<i64> {
    let max_sequence = db
        .query_one(
            &Query::select()
                .expr_as(
                    Expr::cust("COALESCE(MAX(sequence), 0)"),
                    Alias::new("max_sequence"),
                )
                .from(Alias::new("turn_event"))
                .and_where(Expr::col(Alias::new("turn_id")).eq(turn_id.to_owned()))
                .to_owned(),
        )
        .await
        .context("failed to query max turn_event sequence")?
        .and_then(|row| {
            row.try_get::<Option<i64>>("", "max_sequence")
                .ok()
                .flatten()
        })
        .unwrap_or(0);

    Ok(max_sequence + 1)
}

pub async fn list_events_for_turn<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    turn_id: &str,
    after_sequence: Option<i64>,
    limit: Option<u64>,
) -> Result<Vec<turn_event::Model>> {
    let mut query = turn_event::Entity::find()
        .filter(turn_event::Column::ThreadId.eq(thread_id.to_owned()))
        .filter(turn_event::Column::TurnId.eq(turn_id.to_owned()))
        .order_by_asc(turn_event::Column::Sequence);
    if let Some(after_sequence) = after_sequence {
        query = query.filter(turn_event::Column::Sequence.gt(after_sequence));
    }
    if let Some(limit) = limit {
        query = query.limit(limit);
    }
    query.all(db).await.context("failed to query turn events")
}

pub async fn list_events_for_thread<C: ConnectionTrait>(
    db: &C,
    thread_id: &str,
    limit: Option<u64>,
) -> Result<Vec<turn_event::Model>> {
    let mut query = turn_event::Entity::find()
        .filter(turn_event::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_desc(turn_event::Column::CreatedAt)
        .order_by_desc(turn_event::Column::TurnId)
        .order_by_desc(turn_event::Column::Sequence);

    if let Some(limit) = limit {
        query = query.limit(limit);
    }

    query
        .all(db)
        .await
        .map(|mut rows| {
            rows.reverse();
            rows
        })
        .context("failed to query thread history events")
}
