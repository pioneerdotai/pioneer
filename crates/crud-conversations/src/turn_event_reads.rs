//! Canonical event reads and bounded portable payload hydration.
use crate::events::{AppendedTurnEvent, TurnEventPayload};
use anyhow::{Context, Result};
use pioneer_entity::{compaction_event_revision, turn_event};
use sea_orm::{
    ColumnTrait, ConnectionTrait, EntityTrait, QueryFilter, QueryOrder, QuerySelect, QueryTrait,
};

/// Read the stored event row for an exact idempotency check.
pub async fn find_event_model_by_idempotency_key<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    idempotency_key: &str,
) -> Result<Option<turn_event::Model>> {
    Ok(turn_event::Entity::find()
        .filter(turn_event::Column::TurnId.eq(turn_id))
        .filter(turn_event::Column::IdempotencyKey.eq(idempotency_key))
        .one(db)
        .await?)
}

/// The existing canonical item index is written in the acceptance transaction,
/// independently of the projection watermark. No payload decoding is needed.
fn accepted_item_completions(
    turn_id: &str,
    item_id: &str,
    before_sequence: Option<i64>,
) -> sea_orm::Select<turn_event::Entity> {
    let ids = compaction_event_revision::Entity::find()
        .select_only()
        .column(compaction_event_revision::Column::SourceId)
        .filter(compaction_event_revision::Column::TurnId.eq(turn_id.to_owned()))
        .filter(compaction_event_revision::Column::ItemId.eq(item_id.to_owned()))
        .filter(compaction_event_revision::Column::Present.eq(1))
        .into_query();
    let mut query = turn_event::Entity::find()
        .filter(turn_event::Column::TurnId.eq(turn_id.to_owned()))
        .filter(
            turn_event::Column::EventType.eq(pioneer_protocol::constants::events::ITEM_COMPLETED),
        )
        .filter(turn_event::Column::Id.in_subquery(ids));
    if let Some(sequence) = before_sequence {
        query = query.filter(turn_event::Column::Sequence.lt(sequence));
    }
    query
}

pub async fn has_accepted_item_completion<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
    before_sequence: Option<i64>,
) -> Result<bool> {
    Ok(accepted_item_completions(turn_id, item_id, before_sequence)
        .select_only()
        .column(turn_event::Column::Id)
        .into_tuple::<String>()
        .one(db)
        .await?
        .is_some())
}

pub async fn find_accepted_item_completion<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<turn_event::Model>> {
    Ok(accepted_item_completions(turn_id, item_id, None)
        .order_by_desc(turn_event::Column::Sequence)
        .one(db)
        .await?)
}

pub fn appended_event_from_model(model: turn_event::Model) -> Result<AppendedTurnEvent> {
    let payload: TurnEventPayload =
        serde_json::from_value(model.payload.context("turn event payload was not loaded")?)
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

pub async fn find_event_model_by_id(
    db: &impl ConnectionTrait,
    event_id: &str,
) -> Result<Option<turn_event::Model>> {
    Ok(turn_event::Entity::find_by_id(event_id.to_owned())
        .one(db)
        .await?)
}

pub async fn find_event_by_id(
    db: &pioneer_sqlite::SqliteDatabase,
    event_id: &str,
) -> Result<Option<AppendedTurnEvent>> {
    crate::turn_payload::events(
        db,
        turn_event::Entity::find_by_id(event_id.to_owned()).limit(1),
    )
    .await?
    .pop()
    .map(appended_event_from_model)
    .transpose()
}

/// Read finite rows after the claim commit. The caller decodes each body
/// independently so one corrupt legacy body cannot suppress healthy deliveries.
pub async fn find_event_models_by_ids(
    db: &pioneer_sqlite::SqliteDatabase,
    ids: &[String],
) -> Result<Vec<(String, Result<turn_event::Model>)>> {
    anyhow::ensure!(
        ids.len() <= crate::turn_event_delivery::DELIVERY_BATCH_LIMIT,
        "event body batch exceeds bound"
    );
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut result = Vec::with_capacity(ids.len());
    for id in ids {
        // Keep per-event failures independent: one damaged portable header or
        // block must not prevent healthy claimed deliveries from progressing.
        let row =
            crate::turn_payload::events(db, turn_event::Entity::find_by_id(id.clone()).limit(1))
                .await
                .and_then(|mut rows| rows.pop().context("delivery event disappeared"));
        result.push((id.clone(), row));
    }
    Ok(result)
}

pub async fn latest_event_for_turn(
    db: &pioneer_sqlite::SqliteDatabase,
    turn_id: &str,
) -> Result<Option<AppendedTurnEvent>> {
    crate::turn_payload::events(
        db,
        turn_event::Entity::find()
            .filter(turn_event::Column::TurnId.eq(turn_id.to_owned()))
            .order_by_desc(turn_event::Column::Sequence)
            .limit(1),
    )
    .await?
    .pop()
    .map(appended_event_from_model)
    .transpose()
}

pub async fn list_events_for_turn(
    db: &pioneer_sqlite::SqliteDatabase,
    thread_id: &str,
    turn_id: &str,
    after_sequence: Option<i64>,
    limit: Option<u64>,
) -> Result<Vec<turn_event::Model>> {
    let query = turn_event_page_query(thread_id, turn_id, after_sequence, limit);
    crate::turn_payload::events(db, query)
        .await
        .context("failed to query turn events")
}

pub(crate) fn turn_event_page_query(
    thread_id: &str,
    turn_id: &str,
    after_sequence: Option<i64>,
    limit: Option<u64>,
) -> sea_orm::Select<turn_event::Entity> {
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
    query
}

pub async fn list_events_for_thread(
    db: &pioneer_sqlite::SqliteDatabase,
    thread_id: &str,
    limit: Option<u64>,
) -> Result<Vec<turn_event::Model>> {
    let query = thread_event_query(thread_id, limit);
    crate::turn_payload::events(db, query)
        .await
        .map(|mut rows| {
            rows.reverse();
            rows
        })
        .context("failed to query thread history events")
}

pub(crate) fn thread_event_query(
    thread_id: &str,
    limit: Option<u64>,
) -> sea_orm::Select<turn_event::Entity> {
    let mut query = turn_event::Entity::find()
        .filter(turn_event::Column::ThreadId.eq(thread_id.to_owned()))
        .order_by_desc(turn_event::Column::CreatedAt)
        .order_by_desc(turn_event::Column::TurnId)
        .order_by_desc(turn_event::Column::Sequence);

    if let Some(limit) = limit {
        query = query.limit(limit);
    }

    query
}
