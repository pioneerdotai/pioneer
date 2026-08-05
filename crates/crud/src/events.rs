use pioneer_protocol::constants::events;
use pioneer_protocol::{
    ItemCompletedNotification, ItemRecoveryAttachedNotification, ItemRecoveryExhaustedNotification,
    ItemRecoveryOpenedNotification, ItemRecoverySucceededNotification,
    ItemRetryAttemptStartedNotification, ItemRetryScheduledNotification, ItemStartedNotification,
    ItemTimeoutDetectedNotification, ItemToolRetryExhaustedNotification,
    ItemToolRetryResolvedNotification, ItemToolRetryScheduledNotification, ItemUpdatedNotification,
    PersistedActorRef, SandboxMode, Thread, Turn, TurnBlockedNotification,
    TurnCompletedNotification, TurnExecutionWindowBlockedNotification,
    TurnExecutionWindowCheckpointedNotification, TurnExecutionWindowContinuedNotification,
    TurnExecutionWindowExhaustedNotification, TurnExecutionWindowStartedNotification,
    TurnFailedNotification, TurnMessageDeletedEvent, TurnMessageEditedEvent,
    TurnPermissionAuditEvent, TurnToolLoopBudgetExceededNotification, UserInput,
};
use sea_orm::entity::prelude::DateTimeWithTimeZone;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CanonicalTurnStartedEventPayload {
    pub thread: Thread,
    pub sandbox_mode: SandboxMode,
    pub turn: Turn,
    pub input: Vec<UserInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actor: Option<PersistedActorRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum CanonicalTurnEventPayload {
    TurnStarted(CanonicalTurnStartedEventPayload),
    ItemStarted(ItemStartedNotification),
    ItemCompleted(ItemCompletedNotification),
    ItemUpdated(ItemUpdatedNotification),
    ItemTimeoutDetected(ItemTimeoutDetectedNotification),
    ItemRecoveryOpened(ItemRecoveryOpenedNotification),
    ItemRecoveryAttached(ItemRecoveryAttachedNotification),
    ItemRetryScheduled(ItemRetryScheduledNotification),
    ItemRetryAttemptStarted(ItemRetryAttemptStartedNotification),
    ItemRecoverySucceeded(ItemRecoverySucceededNotification),
    ItemRecoveryExhausted(ItemRecoveryExhaustedNotification),
    ItemToolRetryScheduled(ItemToolRetryScheduledNotification),
    ItemToolRetryResolved(ItemToolRetryResolvedNotification),
    ItemToolRetryExhausted(ItemToolRetryExhaustedNotification),
    TurnToolLoopBudgetExceeded(TurnToolLoopBudgetExceededNotification),
    TurnExecutionWindowStarted(TurnExecutionWindowStartedNotification),
    TurnExecutionWindowExhausted(TurnExecutionWindowExhaustedNotification),
    TurnExecutionWindowCheckpointed(TurnExecutionWindowCheckpointedNotification),
    TurnExecutionWindowContinued(TurnExecutionWindowContinuedNotification),
    TurnExecutionWindowBlocked(TurnExecutionWindowBlockedNotification),
    TurnPermissionAudit(TurnPermissionAuditEvent),
    TurnMessageEdited(TurnMessageEditedEvent),
    TurnMessageDeleted(TurnMessageDeletedEvent),
    TurnCompleted(TurnCompletedNotification),
    TurnFailed(TurnFailedNotification),
    TurnBlocked(TurnBlockedNotification),
}

impl CanonicalTurnEventPayload {
    /// Stable identity for retrying the same logical native delivery after an
    /// unknown commit result. It is deliberately derived from the canonical
    /// structured payload, not from process-local sequence or wall clock.
    pub fn idempotency_key(&self) -> Result<String, serde_json::Error> {
        let payload = serde_json::to_vec(self)?;
        let mut hasher = Sha256::new();
        hasher.update(self.event_type().as_bytes());
        hasher.update([0]);
        hasher.update(payload);
        Ok(hex::encode(hasher.finalize()))
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            Self::TurnStarted(_) => events::TURN_STARTED,
            Self::ItemStarted(_) => events::ITEM_STARTED,
            Self::ItemCompleted(_) => events::ITEM_COMPLETED,
            Self::ItemUpdated(_) => events::ITEM_UPDATED,
            Self::ItemTimeoutDetected(_) => events::ITEM_TIMEOUT_DETECTED,
            Self::ItemRecoveryOpened(_) => events::ITEM_RECOVERY_OPENED,
            Self::ItemRecoveryAttached(_) => events::ITEM_RECOVERY_ATTACHED,
            Self::ItemRetryScheduled(_) => events::ITEM_RETRY_SCHEDULED,
            Self::ItemRetryAttemptStarted(_) => events::ITEM_RETRY_ATTEMPT_STARTED,
            Self::ItemRecoverySucceeded(_) => events::ITEM_RECOVERY_SUCCEEDED,
            Self::ItemRecoveryExhausted(_) => events::ITEM_RECOVERY_EXHAUSTED,
            Self::ItemToolRetryScheduled(_) => events::ITEM_TOOL_RETRY_SCHEDULED,
            Self::ItemToolRetryResolved(_) => events::ITEM_TOOL_RETRY_RESOLVED,
            Self::ItemToolRetryExhausted(_) => events::ITEM_TOOL_RETRY_EXHAUSTED,
            Self::TurnToolLoopBudgetExceeded(_) => events::TURN_TOOL_LOOP_BUDGET_EXCEEDED,
            Self::TurnExecutionWindowStarted(_) => events::TURN_EXECUTION_WINDOW_STARTED,
            Self::TurnExecutionWindowExhausted(_) => events::TURN_EXECUTION_WINDOW_EXHAUSTED,
            Self::TurnExecutionWindowCheckpointed(_) => events::TURN_EXECUTION_WINDOW_CHECKPOINTED,
            Self::TurnExecutionWindowContinued(_) => events::TURN_EXECUTION_WINDOW_CONTINUED,
            Self::TurnExecutionWindowBlocked(_) => events::TURN_EXECUTION_WINDOW_BLOCKED,
            Self::TurnPermissionAudit(_) => events::TURN_PERMISSION_AUDIT,
            Self::TurnMessageEdited(_) => events::TURN_MESSAGE_EDITED,
            Self::TurnMessageDeleted(_) => events::TURN_MESSAGE_DELETED,
            Self::TurnCompleted(_) => events::TURN_COMPLETED,
            Self::TurnFailed(_) => events::TURN_FAILED,
            Self::TurnBlocked(_) => events::TURN_BLOCKED,
        }
    }

    pub fn thread_id(&self) -> &str {
        match self {
            Self::TurnStarted(payload) => payload.thread.id.as_str(),
            Self::ItemStarted(payload) => payload.thread_id.as_str(),
            Self::ItemCompleted(payload) => payload.thread_id.as_str(),
            Self::ItemUpdated(payload) => payload.thread_id.as_str(),
            Self::ItemTimeoutDetected(payload) => payload.thread_id.as_str(),
            Self::ItemRecoveryOpened(payload) => payload.thread_id.as_str(),
            Self::ItemRecoveryAttached(payload) => payload.thread_id.as_str(),
            Self::ItemRetryScheduled(payload) => payload.thread_id.as_str(),
            Self::ItemRetryAttemptStarted(payload) => payload.thread_id.as_str(),
            Self::ItemRecoverySucceeded(payload) => payload.thread_id.as_str(),
            Self::ItemRecoveryExhausted(payload) => payload.thread_id.as_str(),
            Self::ItemToolRetryScheduled(payload) => payload.thread_id.as_str(),
            Self::ItemToolRetryResolved(payload) => payload.thread_id.as_str(),
            Self::ItemToolRetryExhausted(payload) => payload.thread_id.as_str(),
            Self::TurnToolLoopBudgetExceeded(payload) => payload.thread_id.as_str(),
            Self::TurnExecutionWindowStarted(payload) => payload.thread_id.as_str(),
            Self::TurnExecutionWindowExhausted(payload) => payload.thread_id.as_str(),
            Self::TurnExecutionWindowCheckpointed(payload) => payload.thread_id.as_str(),
            Self::TurnExecutionWindowContinued(payload) => payload.thread_id.as_str(),
            Self::TurnExecutionWindowBlocked(payload) => payload.thread_id.as_str(),
            Self::TurnPermissionAudit(payload) => payload.thread_id.as_str(),
            Self::TurnMessageEdited(payload) => payload.thread_id.as_str(),
            Self::TurnMessageDeleted(payload) => payload.thread_id.as_str(),
            Self::TurnCompleted(payload) => payload.thread_id.as_str(),
            Self::TurnFailed(payload) => payload.thread_id.as_str(),
            Self::TurnBlocked(payload) => payload.thread_id.as_str(),
        }
    }

    pub fn workspace_id(&self) -> &str {
        match self {
            Self::TurnStarted(payload) => payload.thread.workspace_id.as_str(),
            Self::ItemStarted(payload) => payload.workspace_id.as_str(),
            Self::ItemCompleted(payload) => payload.workspace_id.as_str(),
            Self::ItemUpdated(payload) => payload.workspace_id.as_str(),
            Self::ItemTimeoutDetected(payload) => payload.workspace_id.as_str(),
            Self::ItemRecoveryOpened(payload) => payload.workspace_id.as_str(),
            Self::ItemRecoveryAttached(payload) => payload.workspace_id.as_str(),
            Self::ItemRetryScheduled(payload) => payload.workspace_id.as_str(),
            Self::ItemRetryAttemptStarted(payload) => payload.workspace_id.as_str(),
            Self::ItemRecoverySucceeded(payload) => payload.workspace_id.as_str(),
            Self::ItemRecoveryExhausted(payload) => payload.workspace_id.as_str(),
            Self::ItemToolRetryScheduled(payload) => payload.workspace_id.as_str(),
            Self::ItemToolRetryResolved(payload) => payload.workspace_id.as_str(),
            Self::ItemToolRetryExhausted(payload) => payload.workspace_id.as_str(),
            Self::TurnToolLoopBudgetExceeded(payload) => payload.workspace_id.as_str(),
            Self::TurnExecutionWindowStarted(payload) => payload.workspace_id.as_str(),
            Self::TurnExecutionWindowExhausted(payload) => payload.workspace_id.as_str(),
            Self::TurnExecutionWindowCheckpointed(payload) => payload.workspace_id.as_str(),
            Self::TurnExecutionWindowContinued(payload) => payload.workspace_id.as_str(),
            Self::TurnExecutionWindowBlocked(payload) => payload.workspace_id.as_str(),
            Self::TurnPermissionAudit(payload) => payload.workspace_id.as_str(),
            Self::TurnMessageEdited(payload) => payload.workspace_id.as_str(),
            Self::TurnMessageDeleted(payload) => payload.workspace_id.as_str(),
            Self::TurnCompleted(payload) => payload.workspace_id.as_str(),
            Self::TurnFailed(payload) => payload.workspace_id.as_str(),
            Self::TurnBlocked(payload) => payload.workspace_id.as_str(),
        }
    }

    pub fn turn_id(&self) -> &str {
        match self {
            Self::TurnStarted(payload) => payload.turn.id.as_str(),
            Self::ItemStarted(payload) => payload.turn_id.as_str(),
            Self::ItemCompleted(payload) => payload.turn_id.as_str(),
            Self::ItemUpdated(payload) => payload.turn_id.as_str(),
            Self::ItemTimeoutDetected(payload) => payload.turn_id.as_str(),
            Self::ItemRecoveryOpened(payload) => payload.turn_id.as_str(),
            Self::ItemRecoveryAttached(payload) => payload.turn_id.as_str(),
            Self::ItemRetryScheduled(payload) => payload.turn_id.as_str(),
            Self::ItemRetryAttemptStarted(payload) => payload.turn_id.as_str(),
            Self::ItemRecoverySucceeded(payload) => payload.turn_id.as_str(),
            Self::ItemRecoveryExhausted(payload) => payload.turn_id.as_str(),
            Self::ItemToolRetryScheduled(payload) => payload.turn_id.as_str(),
            Self::ItemToolRetryResolved(payload) => payload.turn_id.as_str(),
            Self::ItemToolRetryExhausted(payload) => payload.turn_id.as_str(),
            Self::TurnToolLoopBudgetExceeded(payload) => payload.turn_id.as_str(),
            Self::TurnExecutionWindowStarted(payload) => payload.turn_id.as_str(),
            Self::TurnExecutionWindowExhausted(payload) => payload.turn_id.as_str(),
            Self::TurnExecutionWindowCheckpointed(payload) => payload.turn_id.as_str(),
            Self::TurnExecutionWindowContinued(payload) => payload.turn_id.as_str(),
            Self::TurnExecutionWindowBlocked(payload) => payload.turn_id.as_str(),
            Self::TurnPermissionAudit(payload) => payload.turn_id.as_str(),
            Self::TurnMessageEdited(payload) => payload.turn.id.as_str(),
            Self::TurnMessageDeleted(payload) => payload.turn.id.as_str(),
            Self::TurnCompleted(payload) => payload.turn.id.as_str(),
            Self::TurnFailed(payload) => payload.turn.id.as_str(),
            Self::TurnBlocked(payload) => payload.turn.id.as_str(),
        }
    }
}

pub(crate) type TurnEventPayload = CanonicalTurnEventPayload;
pub(crate) type TurnStartedEventPayload = CanonicalTurnStartedEventPayload;

#[derive(Debug, Clone)]
pub struct AppendedTurnEvent {
    pub id: String,
    pub thread_id: String,
    pub turn_id: String,
    pub sequence: i64,
    pub payload: TurnEventPayload,
    pub idempotency_key: Option<String>,
    pub was_inserted: bool,
    pub created_at: DateTimeWithTimeZone,
}

#[cfg(test)]
mod tests {
    use pioneer_protocol::{
        PersistedActorRef, PrincipalId, SandboxMode, Thread, ThreadMode, ThreadOriginKind,
        ThreadSidebarVisibility, ThreadStatus, Turn, TurnKind, TurnOrigin, TurnPermissionMode,
        TurnPermissionProfileSnapshot, TurnPermissionProfileSource, TurnStatus, UserInput,
    };
    use serde_json::json;

    use super::{CanonicalTurnEventPayload, CanonicalTurnStartedEventPayload};

    const PRINCIPAL_ID: &str = "P00000000000000000001";

    fn started_payload(actor: Option<PersistedActorRef>) -> CanonicalTurnStartedEventPayload {
        CanonicalTurnStartedEventPayload {
            thread: Thread {
                workspace_id: "workspace".to_owned(),
                id: "thread".to_owned(),
                name: Some("Thread".to_owned()),
                preview: String::new(),
                mode: ThreadMode::Agent,
                model: "model".to_owned(),
                model_provider: "provider".to_owned(),
                reasoning_effort: None,
                created_at: 1,
                updated_at: 2,
                status: ThreadStatus::Active,
                origin_kind: ThreadOriginKind::User,
                sidebar_visibility: ThreadSidebarVisibility::Visible,
                agent_nickname: None,
                agent_role: None,
                visibility: None,
                turns: Vec::new(),
            },
            sandbox_mode: SandboxMode::FullAccess,
            turn: Turn {
                id: "turn".to_owned(),
                status: TurnStatus::InProgress,
                turn_kind: TurnKind::Conversation,
                origin: TurnOrigin::User,
                mode: Default::default(),
                author: None,
                reply_to_turn_id: None,
                mentions: Vec::new(),
                message_revision: 0,
                message_deleted: false,
                error: None,
                prompt_manifest: None,
                permission_profile: TurnPermissionProfileSnapshot::from_mode(
                    TurnPermissionMode::FullAccess,
                    TurnPermissionProfileSource::Composer,
                ),
            },
            input: vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            actor,
            reasoning_effort: Some("high".to_owned()),
        }
    }

    #[test]
    fn historical_turn_started_payload_without_actor_remains_readable() {
        let historical = serde_json::to_value(CanonicalTurnEventPayload::TurnStarted(
            started_payload(None),
        ))
        .expect("historical fixture should serialize");
        assert!(
            historical
                .get("payload")
                .and_then(serde_json::Value::as_object)
                .is_some_and(|payload| !payload.contains_key("actor"))
        );

        let decoded: CanonicalTurnEventPayload =
            serde_json::from_value(historical).expect("historical fixture should remain readable");
        let CanonicalTurnEventPayload::TurnStarted(decoded) = decoded else {
            panic!("expected turn_started event");
        };
        assert_eq!(decoded.actor, None);
    }

    #[test]
    fn turn_started_actor_round_trips_through_the_canonical_event_envelope() {
        for actor in [
            PersistedActorRef::Principal(
                PrincipalId::new(PRINCIPAL_ID).expect("valid principal id"),
            ),
            PersistedActorRef::System,
        ] {
            let canonical =
                CanonicalTurnEventPayload::TurnStarted(started_payload(Some(actor.clone())));
            let encoded = serde_json::to_value(&canonical).expect("event should serialize");
            let decoded: CanonicalTurnEventPayload =
                serde_json::from_value(encoded).expect("event should deserialize");

            let CanonicalTurnEventPayload::TurnStarted(decoded) = decoded else {
                panic!("expected turn_started event");
            };
            assert_eq!(decoded.actor, Some(actor));
        }
    }

    #[test]
    fn turn_started_actor_rejects_malformed_ids_and_unknown_variants() {
        for actor in [
            json!({"kind": "principal", "id": "superuser"}),
            json!({"kind": "unknown"}),
        ] {
            let mut fixture = serde_json::to_value(CanonicalTurnEventPayload::TurnStarted(
                started_payload(None),
            ))
            .expect("fixture should serialize");
            fixture["payload"]["actor"] = actor;

            assert!(
                serde_json::from_value::<CanonicalTurnEventPayload>(fixture).is_err(),
                "invalid actor must fail canonical event decoding"
            );
        }
    }
}
