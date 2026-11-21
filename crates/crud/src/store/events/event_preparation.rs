use crate::DB_ID_LEN;
use crate::events::TurnEventPayload;
use pioneer_crud_execution::turn_execution::terminal_turn_execution_status_for_event;
use crate::store::events::turn_projector::{PreparedTurnProjection, TurnProjector};
use crate::store::execution::native_cancellation::PreparedNativeCancellation;
use crate::{TurnExecutionRecord, TurnItemAttemptDeadlines};
use anyhow::{Context, Result};
use pioneer_crud_execution::{event_append as turn_event, native_terminal_effect_outbox};
use pioneer_protocol::generate_id;
use pioneer_sqlite::SqliteDatabase;
use sea_orm::ConnectionTrait;
use sea_orm::entity::prelude::DateTimeWithTimeZone;

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub(crate) struct TurnEventProjectionContext {
    #[serde(default)]
    pub(crate) item_started_deadlines: Option<TurnItemAttemptDeadlines>,
    #[serde(default)]
    pub(crate) enqueue_optional_deliveries: bool,
    #[serde(skip)]
    pub(crate) native_cancellation: Option<PreparedNativeCancellation>,
    #[serde(skip)]
    pub(crate) unactivated_terminal: Option<TurnExecutionRecord>,
    #[serde(skip)]
    pub(crate) cancellation_actor_id: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct PreparedProjectedTurnEvent {
    pub(crate) event: turn_event::PreparedTurnEvent,
    pub(crate) projection: PreparedTurnProjection,
    pub(crate) projection_context_json: String,
    pub(crate) claim_token: String,
    pub(crate) terminal_effect_activation:
        Option<native_terminal_effect_outbox::PreparedNativeTerminalEffectActivation>,
}

/// State-independent work for a durable Turn event. This envelope is built
/// before the writer is reserved; projection preparation is deliberately not
/// part of it because an atomic batch must observe the effects of every
/// preceding event in that same transaction.
#[derive(Clone, Debug)]
pub(crate) struct PreparedTurnEventEnvelope {
    pub(crate) event: turn_event::PreparedTurnEvent,
    pub(crate) projection_context_json: String,
    pub(crate) claim_token: String,
}

pub(crate) const TURN_EVENT_PROJECTION_LEASE_SECS: i64 = 120;

pub(crate) fn serialize_turn_event_projection_context(
    context: &TurnEventProjectionContext,
    event_id: &str,
) -> Result<String> {
    serde_json::to_string(context).with_context(|| {
        format!("failed to serialize turn event projection context for event `{event_id}`")
    })
}

pub(crate) fn deserialize_turn_event_projection_context(
    context_json: &str,
    event_id: &str,
) -> Result<TurnEventProjectionContext> {
    serde_json::from_str(context_json).with_context(|| {
        format!("failed to deserialize turn event projection context for event `{event_id}`")
    })
}

pub(crate) async fn prepare_turn_event_for_permanent_storage(
    db: &pioneer_sqlite::SqliteDatabase,
    event: TurnEventPayload,
) -> Result<turn_event::PreparedTurnEvent> {
    turn_event::PreparedTurnEvent::prepare_for_storage(event)?
        .preflight_idempotency(db)
        .await
}

pub(crate) async fn prepare_projected_turn_event_for_permanent_storage(
    db: &pioneer_sqlite::SqliteDatabase,
    event: TurnEventPayload,
    created_at: DateTimeWithTimeZone,
) -> Result<PreparedProjectedTurnEvent> {
    let event = prepare_turn_event_envelope_for_permanent_storage(db, event).await?;
    prepare_projected_turn_event_from_envelope(db, event, created_at).await
}

pub(crate) async fn prepare_turn_event_envelope_for_permanent_storage(
    db: &pioneer_sqlite::SqliteDatabase,
    event: TurnEventPayload,
) -> Result<PreparedTurnEventEnvelope> {
    let event = prepare_turn_event_for_permanent_storage(db, event).await?;
    let projection_context_json = serialize_turn_event_projection_context(
        &TurnEventProjectionContext::default(),
        event.id(),
    )?;
    Ok(PreparedTurnEventEnvelope {
        event,
        projection_context_json,
        claim_token: generate_id(DB_ID_LEN),
    })
}

pub(crate) async fn prepare_projected_turn_event_from_envelope<C: ConnectionTrait>(
    db: &C,
    prepared: PreparedTurnEventEnvelope,
    created_at: DateTimeWithTimeZone,
) -> Result<PreparedProjectedTurnEvent> {
    let PreparedTurnEventEnvelope {
        event,
        projection_context_json,
        claim_token,
    } = prepared;
    let mut projection = TurnProjector::new()
        .prepare(db, event.id(), event.payload(), created_at)
        .await?;
    let terminal_effect_activation =
        if terminal_turn_execution_status_for_event(event.payload()).is_some() {
            Some(
                native_terminal_effect_outbox::prepare_activation_for_terminal(
                    db,
                    event.payload().turn_id(),
                )
                .await?,
            )
        } else {
            None
        };
    Ok(PreparedProjectedTurnEvent {
        event,
        projection,
        projection_context_json,
        claim_token,
        terminal_effect_activation,
    })
}
