use anyhow::{Context, Result};
use pioneer_crud_conversations::turn_event as event_store;
use pioneer_crud_payloads::storage::{self, PreparedPayload};
use pioneer_entity::{compaction_event_revision, turn_event};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QueryOrder,
    QuerySelect, QueryTrait, TransactionTrait,
};

use crate::events::{AppendedTurnEvent, TurnEventPayload};
use pioneer_protocol::generate_id;

const DB_ID_LEN: usize = 21;

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

pub(crate) async fn has_accepted_item_completion<C: ConnectionTrait>(
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

pub(crate) async fn find_accepted_item_completion<C: ConnectionTrait>(
    db: &C,
    turn_id: &str,
    item_id: &str,
) -> Result<Option<turn_event::Model>> {
    Ok(accepted_item_completions(turn_id, item_id, None)
        .order_by_desc(turn_event::Column::Sequence)
        .one(db)
        .await?)
}

/// CPU-only representation of an append. Serialization, idempotency-key
/// derivation and ID generation happen before a caller begins a write
/// transaction. The PostgreSQL write uses only the prepared storage representation.
#[derive(Clone, Debug)]
pub struct PreparedTurnEvent {
    id: String,
    thread_id: String,
    turn_id: String,
    event_type: String,
    body: PreparedPayload,
    inline_payload: serde_json::Value,
    context_item_id: Option<String>,
    context_projection_kind: &'static str,
    idempotency_key: String,
    legacy_idempotency_key: String,
    payload: TurnEventPayload,
    semantically_matching_existing_id: Option<(String, String, i64)>,
    execution_observation: Option<super::execution_observability::PreparedExecutionObservation>,
}

impl PreparedTurnEvent {
    pub fn prepare(mut payload: TurnEventPayload) -> Result<Self> {
        let thread_id = payload.thread_id().to_owned();
        let turn_id = payload.turn_id().to_owned();
        let event_type = payload.event_type().to_owned();
        let legacy_idempotency_key = payload.idempotency_key()?;
        let execution_observation = super::execution_observability::prepare_event(&payload)?;
        if let Some(observation) = &execution_observation {
            super::execution_observability::reference_event_tariff(&mut payload, observation);
        }
        let payload_json =
            serde_json::to_string(&payload).context("failed to serialize turn event payload")?;
        let idempotency_key = payload
            .idempotency_key()
            .context("failed to derive turn event idempotency key")?;
        let (context_item_id, context_projection_kind) =
            crate::compaction::history::event_projection_metadata(&payload);
        Ok(Self {
            execution_observation,
            context_item_id,
            context_projection_kind,
            id: generate_id(DB_ID_LEN),
            thread_id,
            turn_id,
            event_type,
            body: PreparedPayload::new(&payload_json)?,
            inline_payload: serde_json::from_str(&payload_json)?,
            idempotency_key,
            legacy_idempotency_key,
            payload,
            semantically_matching_existing_id: None,
        })
    }

    pub fn payload(&self) -> &TurnEventPayload {
        &self.payload
    }

    pub fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Performs the potentially expensive legacy-payload decode before a
    /// caller acquires the writer. Newly written rows use canonical JSON, but
    /// older rows may be semantically identical without being byte-identical.
    /// Row ID, incarnation and logical revision enter the write transaction as
    /// the compare fence, preserving idempotent replay without parsing JSON
    /// while the writer is held.
    pub async fn preflight_idempotency(
        mut self,
        db: &pioneer_sqlite::SqliteDatabase,
    ) -> Result<Self> {
        if let Some(existing) = super::turn_payload::events(
            db,
            turn_event::Entity::find()
                .filter(turn_event::Column::TurnId.eq(self.turn_id.clone()))
                .filter(
                    sea_orm::Condition::any()
                        .add(turn_event::Column::IdempotencyKey.eq(self.idempotency_key.clone()))
                        .add(
                            turn_event::Column::IdempotencyKey
                                .eq(self.legacy_idempotency_key.clone()),
                        ),
                )
                .limit(1),
        )
        .await?
        .pop()
        {
            let existing_id = (
                existing.id.clone(),
                existing.portable_identity.clone(),
                existing.portable_revision,
            );
            let mut existing = appended_event_from_model(existing)?;
            if let Some(observation) =
                super::execution_observability::prepare_event(&existing.payload)?
            {
                super::execution_observability::reference_event_tariff(
                    &mut existing.payload,
                    &observation,
                );
            }
            if existing.payload != self.payload {
                anyhow::bail!(
                    "turn event idempotency key collision for turn `{}`",
                    self.turn_id
                );
            }
            self.semantically_matching_existing_id = Some(existing_id);
        }
        Ok(self)
    }
}

async fn validate_existing_prepared_event<C: ConnectionTrait>(
    db: &C,
    existing: &turn_event::Model,
    prepared: &PreparedTurnEvent,
) -> Result<()> {
    let canonical_payload_matches = if let Some(id) = &existing.payload_payload_id {
        storage::metadata(db, id).await? == Some(prepared.body.metadata())
    } else {
        existing.payload.as_ref() == Some(&prepared.inline_payload)
    };
    let preflight_fence_matches = prepared
        .semantically_matching_existing_id
        .as_ref()
        .is_some_and(|(id, identity, revision)| {
            id == &existing.id
                && identity == &existing.portable_identity
                && revision == &existing.portable_revision
        });
    if !canonical_payload_matches && !preflight_fence_matches {
        anyhow::bail!(
            "turn event idempotency key collision for turn `{}`",
            prepared.turn_id
        );
    }
    Ok(())
}

/// Checks the durable replay fence without decoding JSON. The semantic legacy
/// check, when required, was completed by `preflight_idempotency` before the
/// writer transaction began.
pub async fn prepared_event_already_exists<C: ConnectionTrait>(
    db: &C,
    prepared: &PreparedTurnEvent,
) -> Result<bool> {
    let existing = turn_event::Entity::find()
        .filter(turn_event::Column::TurnId.eq(prepared.turn_id.clone()))
        .filter(
            sea_orm::Condition::any()
                .add(turn_event::Column::IdempotencyKey.eq(prepared.idempotency_key.clone()))
                .add(
                    turn_event::Column::IdempotencyKey.eq(prepared.legacy_idempotency_key.clone()),
                ),
        )
        .one(db)
        .await
        .context("failed to query idempotent turn event")?;
    let Some(existing) = existing else {
        return Ok(false);
    };
    validate_existing_prepared_event(db, &existing, prepared).await?;
    Ok(true)
}

#[cfg(test)]
pub async fn append_event(
    db: &pioneer_sqlite::SqliteDatabase,
    payload: &TurnEventPayload,
    created_at: DateTimeWithTimeZone,
) -> Result<AppendedTurnEvent> {
    let prepared = PreparedTurnEvent::prepare(payload.clone())?
        .preflight_idempotency(db)
        .await?;
    let tx = db.begin().await?;
    let appended = append_prepared_event(&tx, prepared, created_at).await?;
    tx.commit().await?;
    Ok(appended)
}

pub async fn append_prepared_event(
    db: &DatabaseTransaction,
    prepared: PreparedTurnEvent,
    created_at: DateTimeWithTimeZone,
) -> Result<AppendedTurnEvent> {
    event_store::lock_stream(db, &prepared.turn_id).await?;
    if let Some(existing) = turn_event::Entity::find()
        .filter(turn_event::Column::TurnId.eq(prepared.turn_id.clone()))
        .filter(
            sea_orm::Condition::any()
                .add(turn_event::Column::IdempotencyKey.eq(prepared.idempotency_key.clone()))
                .add(
                    turn_event::Column::IdempotencyKey.eq(prepared.legacy_idempotency_key.clone()),
                ),
        )
        .one(db)
        .await
        .context("failed to query idempotent turn event")?
    {
        validate_existing_prepared_event(db, &existing, &prepared).await?;
        return Ok(AppendedTurnEvent {
            id: existing.id,
            thread_id: existing.thread_id,
            turn_id: existing.turn_id,
            sequence: existing.sequence,
            payload: prepared.payload,
            idempotency_key: existing.idempotency_key,
            was_inserted: false,
            created_at: existing.created_at,
        });
    }

    let sequence = event_store::next_sequence(db, &prepared.turn_id).await?;
    event_store::insert(
        db,
        event_store::NewEvent {
            id: &prepared.id,
            thread_id: &prepared.thread_id,
            turn_id: &prepared.turn_id,
            sequence,
            event_type: &prepared.event_type,
            idempotency_key: &prepared.idempotency_key,
            created_at,
        },
        &prepared.body,
    )
    .await?;

    // Prepared metadata reads only the immutable typed payload inserted above.
    // Production callers hold the same serialized append transaction, so an
    // event edit cannot intervene. Replays do not overwrite metadata; stale or
    // legacy metadata is detected by its revision and reprojected by readers.
    db.execute(
        &Query::update()
            .table(Alias::new("compaction_event_revision"))
            .value("projection_revision", Expr::col("revision"))
            .value("item_id", prepared.context_item_id)
            .value("projection_kind", prepared.context_projection_kind)
            .and_where(Expr::col("source_id").eq(prepared.id.clone()))
            .and_where(Expr::col("turn_id").eq(prepared.turn_id.clone()))
            .and_where(Expr::col("present").eq(1))
            .to_owned(),
    )
    .await
    .context("failed to record canonical event projection metadata")?;

    if let Some(observation) = &prepared.execution_observation {
        super::execution_observability::commit_event(db, observation, sequence).await?;
    }
    Ok(AppendedTurnEvent {
        id: prepared.id,
        thread_id: prepared.thread_id,
        turn_id: prepared.turn_id,
        sequence,
        payload: prepared.payload,
        idempotency_key: Some(prepared.idempotency_key),
        was_inserted: true,
        created_at,
    })
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

pub async fn find_event_by_id(
    db: &pioneer_sqlite::SqliteDatabase,
    event_id: &str,
) -> Result<Option<AppendedTurnEvent>> {
    super::turn_payload::events(
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
        ids.len() <= super::turn_event_delivery::DELIVERY_BATCH_LIMIT,
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
            super::turn_payload::events(db, turn_event::Entity::find_by_id(id.clone()).limit(1))
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
    super::turn_payload::events(
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

pub async fn max_sequence_for_turn<C: ConnectionTrait>(db: &C, turn_id: &str) -> Result<i64> {
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

    Ok(max_sequence)
}

pub async fn list_events_for_turn(
    db: &pioneer_sqlite::SqliteDatabase,
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
    super::turn_payload::events(db, query)
        .await
        .context("failed to query turn events")
}

pub async fn list_events_for_thread(
    db: &pioneer_sqlite::SqliteDatabase,
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

    super::turn_payload::events(db, query)
        .await
        .map(|mut rows| {
            rows.reverse();
            rows
        })
        .context("failed to query thread history events")
}
