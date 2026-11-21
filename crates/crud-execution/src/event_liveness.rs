//! Select canonical events that count as execution activity.
#[cfg(test)]
use crate::time::unix_to_datetime;
#[cfg(test)]
use pioneer_crud_conversations::events::{AppendedTurnEvent, TurnStartedEventPayload};
use pioneer_crud_conversations::{
    events::TurnEventPayload, turn_item::turn_item_type_to_db, turn_liveness,
};
use pioneer_protocol::ThreadMode;
#[cfg(test)]
use pioneer_protocol::{PersistedActorRef, ThreadStatus, UserInput};

pub fn liveness_observation_from_payload(
    payload: &TurnEventPayload,
    created_at: sea_orm::entity::prelude::DateTimeWithTimeZone,
) -> Option<turn_liveness::TurnLivenessObservation> {
    let mut item_id = None;
    let mut item_type = None;
    let meaningful = match payload {
        TurnEventPayload::TurnStarted(payload) => payload.turn.mode != ThreadMode::Message,
        TurnEventPayload::ItemStarted(payload) => {
            item_id = Some(payload.item.item_id().to_owned());
            item_type = Some(turn_item_type_to_db(payload.item.item_type()).to_owned());
            true
        }
        TurnEventPayload::ItemCompleted(payload) => {
            item_id = Some(payload.item.item_id().to_owned());
            item_type = Some(turn_item_type_to_db(payload.item.item_type()).to_owned());
            true
        }
        TurnEventPayload::ItemUpdated(payload) => {
            item_id = Some(payload.item.item_id().to_owned());
            item_type = Some(turn_item_type_to_db(payload.item.item_type()).to_owned());
            true
        }
        TurnEventPayload::TurnExecutionWindowStarted(_)
        | TurnEventPayload::TurnExecutionWindowCheckpointed(_)
        | TurnEventPayload::TurnExecutionWindowContinued(_) => true,
        TurnEventPayload::ItemTimeoutDetected(_)
        | TurnEventPayload::ItemRecoveryOpened(_)
        | TurnEventPayload::ItemRecoveryAttached(_)
        | TurnEventPayload::ItemRetryScheduled(_)
        | TurnEventPayload::ItemRetryAttemptStarted(_)
        | TurnEventPayload::ItemRecoverySucceeded(_)
        | TurnEventPayload::ItemRecoveryExhausted(_)
        | TurnEventPayload::ItemToolRetryScheduled(_)
        | TurnEventPayload::ItemToolRetryResolved(_)
        | TurnEventPayload::ItemToolRetryExhausted(_)
        | TurnEventPayload::TurnToolLoopBudgetExceeded(_)
        | TurnEventPayload::TurnExecutionWindowExhausted(_)
        | TurnEventPayload::TurnExecutionWindowBlocked(_)
        | TurnEventPayload::TurnPermissionAudit(_)
        | TurnEventPayload::TurnMessageEdited(_)
        | TurnEventPayload::TurnMessageDeleted(_)
        | TurnEventPayload::TurnCompleted(_)
        | TurnEventPayload::TurnFailed(_)
        | TurnEventPayload::TurnBlocked(_) => false,
    };

    meaningful.then(|| turn_liveness::TurnLivenessObservation {
        turn_id: payload.turn_id().to_owned(),
        thread_id: payload.thread_id().to_owned(),
        activity_sequence: 0,
        activity_kind: payload.event_type().to_owned(),
        item_id,
        item_type,
        observed_at: created_at,
    })
}

#[cfg(test)]
mod epic6_tests {
    use super::*;
    use pioneer_protocol::{
        Turn, TurnMessageDeletedEvent, TurnMessageEditedEvent, TurnStatus,
        default_turn_permission_profile_snapshot,
    };

    fn completed_turn() -> Turn {
        Turn {
            work_owner: Default::default(),
            id: "turn-message".to_owned(),
            status: TurnStatus::Completed,
            turn_kind: Default::default(),
            origin: Default::default(),
            mode: pioneer_protocol::ThreadMode::Message,
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 1,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: default_turn_permission_profile_snapshot(),
        }
    }

    fn event(payload: TurnEventPayload) -> AppendedTurnEvent {
        AppendedTurnEvent {
            id: "event-message-mutation".to_owned(),
            thread_id: "thread-message".to_owned(),
            turn_id: "turn-message".to_owned(),
            sequence: 9,
            payload,
            idempotency_key: None,
            was_inserted: true,
            created_at: unix_to_datetime(1_700_000_000),
        }
    }

    #[test]
    fn message_mutations_are_not_execution_liveness_events() {
        let edit = event(TurnEventPayload::TurnMessageEdited(
            TurnMessageEditedEvent {
                workspace_id: "workspace".to_owned(),
                thread_id: "thread-message".to_owned(),
                turn: completed_turn(),
                input: Vec::new(),
                changed_by: PersistedActorRef::System,
                changed_at: 1_700_000_000,
            },
        ));
        let mut deleted_turn = completed_turn();
        deleted_turn.message_deleted = true;
        let delete = event(TurnEventPayload::TurnMessageDeleted(
            TurnMessageDeletedEvent {
                workspace_id: "workspace".to_owned(),
                thread_id: "thread-message".to_owned(),
                turn: deleted_turn,
                deleted_by: PersistedActorRef::System,
                deleted_at: 1_700_000_001,
            },
        ));

        assert!(liveness_observation_from_payload(&edit.payload, edit.created_at).is_none());
        assert!(liveness_observation_from_payload(&delete.payload, delete.created_at).is_none());
    }

    #[test]
    fn message_start_is_not_an_execution_liveness_event() {
        let mut turn = completed_turn();
        turn.status = TurnStatus::InProgress;
        turn.message_revision = 0;
        let started = event(TurnEventPayload::TurnStarted(TurnStartedEventPayload {
            thread: pioneer_protocol::Thread {
                context_window: None,
                service_mode: None,
                context_authority: None,
                workspace_id: "workspace".to_owned(),
                id: "thread-message".to_owned(),
                name: None,
                preview: "hello".to_owned(),
                preview_author: None,
                mode: ThreadMode::Message,
                model: "unused".to_owned(),
                model_provider: "unused".to_owned(),
                reasoning_effort: None,
                created_at: 1_700_000_000,
                updated_at: 1_700_000_000,
                status: ThreadStatus::Idle,
                origin_kind: pioneer_protocol::ThreadOriginKind::User,
                sidebar_visibility: pioneer_protocol::ThreadSidebarVisibility::Visible,
                agent_nickname: None,
                agent_role: None,
                visibility: None,
                turns: vec![turn.clone()],
            },
            sandbox_mode: pioneer_protocol::SandboxMode::FullAccess,
            turn,
            input: vec![UserInput::Text {
                text: "hello".to_owned(),
                text_elements: Vec::new(),
            }],
            actor: Some(PersistedActorRef::System),
            reasoning_effort: None,
            work_owner: Default::default(),
        }));

        assert!(liveness_observation_from_payload(&started.payload, started.created_at).is_none());
    }
}
