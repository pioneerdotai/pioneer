//! Turn cancel orchestration.

use crate::conversation::events::ConversationEvent;
use pioneer_protocol::{TurnCancelParams, TurnCancelResponse, TurnStatus};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnCancelPlan {
    pub thread_id: String,
    pub turn_id: String,
    pub reason: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TurnCancelRequest {
    pub requested_event: ConversationEvent,
    pub params: TurnCancelParams,
}

pub fn plan_turn_cancel_request(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    already_cancelling: bool,
    reason: Option<String>,
) -> Option<TurnCancelRequest> {
    if already_cancelling {
        return None;
    }

    let thread_id = thread_id.into();
    let turn_id = turn_id.into();
    let plan = TurnCancelPlan {
        thread_id: thread_id.clone(),
        turn_id: turn_id.clone(),
        reason,
    };

    Some(TurnCancelRequest {
        requested_event: local_turn_cancel_requested_event(thread_id, turn_id),
        params: turn_cancel_params_from_plan(plan),
    })
}

pub fn turn_cancel_params_from_plan(plan: TurnCancelPlan) -> TurnCancelParams {
    TurnCancelParams {
        thread_id: plan.thread_id,
        turn_id: plan.turn_id,
        reason: plan.reason,
    }
}

pub fn local_turn_cancel_requested_event(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
) -> ConversationEvent {
    ConversationEvent::LocalTurnCancelRequested {
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
    }
}

pub fn local_turn_cancel_rejected_event(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    error: impl Into<String>,
) -> ConversationEvent {
    ConversationEvent::LocalTurnCancelRejected {
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
        error: error.into(),
    }
}

pub fn turn_cancel_response_event(response: TurnCancelResponse) -> Option<ConversationEvent> {
    let TurnCancelResponse {
        thread_id, turn, ..
    } = response;
    match turn.status {
        TurnStatus::InProgress => None,
        TurnStatus::Completed => Some(ConversationEvent::TurnCompleted { thread_id, turn }),
        TurnStatus::Failed | TurnStatus::Interrupted => {
            Some(ConversationEvent::TurnFailed { thread_id, turn })
        }
        TurnStatus::Blocked => Some(ConversationEvent::TurnBlocked {
            thread_id,
            turn,
            resume: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plan_turn_cancel_request_builds_requested_event_and_params() {
        let request = plan_turn_cancel_request(
            "thread",
            "turn",
            false,
            Some("user requested stop".to_owned()),
        )
        .expect("cancel request");

        assert!(matches!(
            request.requested_event,
            ConversationEvent::LocalTurnCancelRequested {
                ref thread_id,
                ref turn_id,
            } if thread_id == "thread" && turn_id == "turn"
        ));
        assert_eq!(request.params.thread_id, "thread");
        assert_eq!(request.params.turn_id, "turn");
        assert_eq!(
            request.params.reason.as_deref(),
            Some("user requested stop")
        );
    }

    #[test]
    fn plan_turn_cancel_request_returns_none_when_already_cancelling() {
        assert!(plan_turn_cancel_request("thread", "turn", true, None).is_none());
    }

    #[test]
    fn turn_cancel_params_from_plan_preserves_optional_reason() {
        let params = turn_cancel_params_from_plan(TurnCancelPlan {
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            reason: None,
        });

        assert_eq!(params.thread_id, "thread");
        assert_eq!(params.turn_id, "turn");
        assert_eq!(params.reason, None);
    }

    #[test]
    fn local_turn_cancel_rejected_event_preserves_error() {
        let event = local_turn_cancel_rejected_event("thread", "turn", "boom");

        assert!(matches!(
            event,
            ConversationEvent::LocalTurnCancelRejected {
                ref thread_id,
                ref turn_id,
                ref error,
            } if thread_id == "thread" && turn_id == "turn" && error == "boom"
        ));
    }

    #[test]
    fn turn_cancel_response_event_terminalizes_interrupted_turn_immediately() {
        let event = turn_cancel_response_event(TurnCancelResponse {
            thread_id: "thread".to_owned(),
            workspace_id: "workspace".to_owned(),
            turn: pioneer_protocol::Turn {
                id: "turn".to_owned(),
                status: TurnStatus::Interrupted,
                turn_kind: Default::default(),
                origin: Default::default(),
                error: Some("stopped".to_owned()),
                prompt_manifest: None,
                permission_profile: pioneer_protocol::system_turn_permission_profile_snapshot(
                    pioneer_protocol::TurnPermissionMode::FullAccess,
                ),
            },
        })
        .expect("interrupted cancel response should produce an event");

        assert!(matches!(
            event,
            ConversationEvent::TurnFailed {
                ref thread_id,
                ref turn,
            } if thread_id == "thread"
                && turn.id == "turn"
                && turn.status == TurnStatus::Interrupted
        ));
    }
}
