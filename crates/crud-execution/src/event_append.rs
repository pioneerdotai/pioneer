use anyhow::{Context, Result};
use pioneer_crud_conversations::turn_event as event_store;
use pioneer_crud_payloads::storage::{self, PreparedPayload};
use pioneer_entity::turn_event;
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use sea_orm::sea_query::{Alias, Expr, ExprTrait, Query};
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseTransaction, EntityTrait, QueryFilter, QuerySelect,
};

use pioneer_crud_conversations::events::{AppendedTurnEvent, TurnEventPayload};
use pioneer_protocol::generate_id;

const DB_ID_LEN: usize = 21;

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
    execution_observation: Option<crate::execution_observability::PreparedExecutionObservation>,
}

impl PreparedTurnEvent {
    pub fn prepare_for_storage(payload: TurnEventPayload) -> Result<Self> {
        crate::event_storage_validation::validate_turn_event_for_permanent_storage(&payload)?;
        Self::prepare(payload)
    }

    pub fn prepare(mut payload: TurnEventPayload) -> Result<Self> {
        let thread_id = payload.thread_id().to_owned();
        let turn_id = payload.turn_id().to_owned();
        let event_type = payload.event_type().to_owned();
        let legacy_idempotency_key = payload.idempotency_key()?;
        let execution_observation = crate::execution_observability::prepare_event(&payload)?;
        if let Some(observation) = &execution_observation {
            crate::execution_observability::reference_event_tariff(&mut payload, observation);
        }
        let payload_json =
            serde_json::to_string(&payload).context("failed to serialize turn event payload")?;
        let idempotency_key = payload
            .idempotency_key()
            .context("failed to derive turn event idempotency key")?;
        let (context_item_id, context_projection_kind) =
            pioneer_crud_conversations::model_history::event_projection_metadata(&payload);
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
        db: &pioneer_postgres_pools::PostgresDatabase,
    ) -> Result<Self> {
        if let Some(existing) = pioneer_crud_conversations::turn_payload::events(
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
                crate::execution_observability::prepare_event(&existing.payload)?
            {
                crate::execution_observability::reference_event_tariff(
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

#[cfg(any(test, feature = "test-support"))]
pub async fn append_event(
    db: &pioneer_postgres_pools::PostgresDatabase,
    payload: &TurnEventPayload,
    created_at: DateTimeWithTimeZone,
) -> Result<AppendedTurnEvent> {
    use sea_orm::TransactionTrait;

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
        crate::execution_observability::commit_event(db, observation, sequence).await?;
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

pub use pioneer_crud_conversations::turn_event::max_sequence as max_sequence_for_turn;
pub use pioneer_crud_conversations::turn_event_reads::{
    appended_event_from_model, find_accepted_item_completion, find_event_by_id,
    find_event_models_by_ids, has_accepted_item_completion, latest_event_for_turn,
    list_events_for_thread, list_events_for_turn,
};
