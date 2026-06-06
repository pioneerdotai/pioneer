//! Turn start orchestration.

use crate::conversation::events::ConversationEvent;
use pioneer_protocol::{
    REQUEST_ID_LEN, ThreadMode, TurnCapability, TurnStartParams, UserInput, UserMessageAttachment,
    generate_id,
};

pub const TURN_ID_LEN: usize = 21;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnStartIds {
    pub turn_id: String,
    pub pending_request_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextTurnPreparation {
    pub input: Vec<UserInput>,
    pub user_text: String,
    pub user_message_text: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnStartParamsPlan {
    pub thread_id: String,
    pub turn_id: String,
    pub input: Vec<UserInput>,
    pub capabilities: Vec<TurnCapability>,
    pub model: Option<String>,
    pub model_provider: Option<String>,
    pub mode: Option<ThreadMode>,
}

pub fn plan_turn_start_ids() -> TurnStartIds {
    TurnStartIds {
        turn_id: generate_id(TURN_ID_LEN),
        pending_request_id: generate_id(REQUEST_ID_LEN),
    }
}

pub fn prepare_text_turn(text: &str) -> TextTurnPreparation {
    let user_message_text = text.trim().to_owned();
    let input = text_user_input(text).into_iter().collect::<Vec<_>>();

    TextTurnPreparation {
        input,
        user_text: user_message_text.clone(),
        user_message_text,
    }
}

pub fn text_user_input(text: &str) -> Option<UserInput> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }

    Some(UserInput::Text {
        text: text.to_owned(),
        text_elements: Vec::new(),
    })
}

pub fn turn_start_params_from_plan(plan: TurnStartParamsPlan) -> TurnStartParams {
    TurnStartParams {
        thread_id: plan.thread_id,
        turn_id: plan.turn_id,
        input: plan.input,
        capabilities: plan.capabilities,
        model: plan.model,
        model_provider: plan.model_provider,
        sandbox_policy: None,
        mode: plan.mode,
    }
}

pub fn local_turn_start_requested_event(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    pending_request_id: impl Into<String>,
    user_text: impl Into<String>,
    attachments: Vec<UserMessageAttachment>,
) -> ConversationEvent {
    ConversationEvent::LocalTurnStartRequested {
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
        pending_request_id: pending_request_id.into(),
        user_text: user_text.into(),
        attachments,
    }
}

pub fn local_turn_start_accepted_event(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    pending_request_id: impl Into<String>,
) -> ConversationEvent {
    ConversationEvent::LocalTurnStartAccepted {
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
        pending_request_id: pending_request_id.into(),
    }
}

pub fn local_turn_start_rejected_event(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    pending_request_id: impl Into<String>,
    error: impl Into<String>,
) -> ConversationEvent {
    ConversationEvent::LocalTurnStartRejected {
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
        pending_request_id: pending_request_id.into(),
        error: error.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{McpScopeKind, TurnCapabilityKind, UserMessageAttachment};

    fn skill_capability() -> TurnCapability {
        TurnCapability {
            id: "skill:user:docs".to_owned(),
            label: Some("Docs".to_owned()),
            kind: TurnCapabilityKind::Skill {
                slug: "docs".to_owned(),
                source_kind: "user".to_owned(),
            },
        }
    }

    #[test]
    fn plan_turn_start_ids_uses_protocol_lengths() {
        let ids = plan_turn_start_ids();

        assert_eq!(ids.turn_id.len(), TURN_ID_LEN);
        assert_eq!(ids.pending_request_id.len(), REQUEST_ID_LEN);
        assert_ne!(ids.turn_id, ids.pending_request_id);
    }

    #[test]
    fn prepare_text_turn_trims_text_and_keeps_text_input_only_when_present() {
        let prepared = prepare_text_turn("  hello world  ");

        assert_eq!(prepared.user_text, "hello world");
        assert_eq!(prepared.user_message_text, "hello world");
        assert_eq!(prepared.input.len(), 1);
        assert!(matches!(
            prepared.input[0],
            UserInput::Text { ref text, .. } if text == "hello world"
        ));

        let blank = prepare_text_turn("   ");
        assert!(blank.input.is_empty());
        assert!(blank.user_text.is_empty());
    }

    #[test]
    fn turn_start_params_from_plan_preserves_capabilities_model_and_mode() {
        let params = turn_start_params_from_plan(TurnStartParamsPlan {
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            input: vec![text_user_input("hello").expect("text input")],
            capabilities: vec![skill_capability()],
            model: Some("gpt-5.4".to_owned()),
            model_provider: Some("openai".to_owned()),
            mode: Some(ThreadMode::Agent),
        });

        assert_eq!(params.thread_id, "thread");
        assert_eq!(params.turn_id, "turn");
        assert_eq!(params.input.len(), 1);
        assert_eq!(params.capabilities.len(), 1);
        assert_eq!(params.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(params.model_provider.as_deref(), Some("openai"));
        assert_eq!(params.mode, Some(ThreadMode::Agent));
        assert_eq!(params.sandbox_policy, None);
    }

    #[test]
    fn local_turn_start_events_preserve_ids_and_payloads() {
        let attachment = UserMessageAttachment::McpServer {
            capability: pioneer_protocol::TurnMcpServerCapabilitySummary {
                id: "mcp-server:workspace:browser".to_owned(),
                label: "browser".to_owned(),
                name: "browser".to_owned(),
                scope_kind: McpScopeKind::Workspace,
            },
        };

        let requested = local_turn_start_requested_event(
            "thread",
            "turn",
            "pending",
            "hello",
            vec![attachment],
        );
        assert!(matches!(
            requested,
            ConversationEvent::LocalTurnStartRequested {
                ref thread_id,
                ref turn_id,
                ref pending_request_id,
                ref user_text,
                ref attachments,
            } if thread_id == "thread"
                && turn_id == "turn"
                && pending_request_id == "pending"
                && user_text == "hello"
                && attachments.len() == 1
        ));

        let accepted = local_turn_start_accepted_event("thread", "turn", "pending");
        assert!(matches!(
            accepted,
            ConversationEvent::LocalTurnStartAccepted {
                ref thread_id,
                ref turn_id,
                ref pending_request_id,
            } if thread_id == "thread" && turn_id == "turn" && pending_request_id == "pending"
        ));

        let rejected = local_turn_start_rejected_event("thread", "turn", "pending", "boom");
        assert!(matches!(
            rejected,
            ConversationEvent::LocalTurnStartRejected {
                ref thread_id,
                ref turn_id,
                ref pending_request_id,
                ref error,
            } if thread_id == "thread"
                && turn_id == "turn"
                && pending_request_id == "pending"
                && error == "boom"
        ));
    }
}
