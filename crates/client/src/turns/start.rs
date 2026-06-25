//! Turn start orchestration.

use crate::conversation::events::ConversationEvent;
use pioneer_protocol::{
    AgentExecutionBackend, REQUEST_ID_LEN, Thread, ThreadMode, TurnCLIRuntimeOptions,
    TurnCapability, TurnReasoningSelection, TurnStartParams, TurnStartResponse, UserInput,
    UserMessageAttachment, generate_id,
};
use std::time::{SystemTime, UNIX_EPOCH};

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

#[derive(Clone, Debug, PartialEq)]
pub struct TurnStartParamsPlan {
    pub thread_id: String,
    pub turn_id: String,
    pub input: Vec<UserInput>,
    pub capabilities: Vec<TurnCapability>,
    pub model: Option<String>,
    pub model_provider: Option<String>,
    pub mode: Option<ThreadMode>,
    pub execution_backend: Option<AgentExecutionBackend>,
    pub reasoning: Option<TurnReasoningSelection>,
    pub cli_runtime_options: Option<TurnCLIRuntimeOptions>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnStartSendContext {
    pub thread_id: String,
    pub turn_id: String,
    pub pending_request_id: String,
}

#[derive(Clone, Debug)]
pub enum TurnStartSendReduction {
    Accepted { events: Vec<ConversationEvent> },
    Rejected { event: ConversationEvent },
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
        execution_backend: plan.execution_backend,
        reasoning: plan.reasoning,
        cli_runtime_options: plan.cli_runtime_options,
    }
}

pub fn turn_reasoning_selection_from_effort(
    effort: Option<String>,
) -> Option<TurnReasoningSelection> {
    let effort = effort?.trim().to_owned();
    (!effort.is_empty()).then_some(TurnReasoningSelection { effort })
}

pub fn now_unix_seconds() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        Err(_) => 0,
    }
}

pub fn reduce_turn_start_send_success(
    context: TurnStartSendContext,
    response: TurnStartResponse,
) -> TurnStartSendReduction {
    TurnStartSendReduction::Accepted {
        events: vec![
            local_turn_start_accepted_event(
                context.thread_id.clone(),
                context.turn_id,
                context.pending_request_id,
            ),
            ConversationEvent::TurnStarted {
                thread_id: context.thread_id,
                turn: response.turn,
            },
        ],
    }
}

pub fn reduce_turn_start_send_failure(
    context: TurnStartSendContext,
    error: impl Into<String>,
) -> TurnStartSendReduction {
    TurnStartSendReduction::Rejected {
        event: local_turn_start_rejected_event(
            context.thread_id,
            context.turn_id,
            context.pending_request_id,
            error,
        ),
    }
}

pub fn apply_prepared_turn_to_thread_snapshot(
    thread: &mut Thread,
    selected_model: Option<&str>,
    selected_provider: Option<&str>,
    selected_reasoning_effort: Option<&str>,
    user_text: &str,
    updated_at_unix: i64,
) {
    if let (Some(model), Some(provider)) = (selected_model, selected_provider) {
        thread.model = model.to_owned();
        thread.model_provider = provider.to_owned();
    }
    thread.reasoning_effort = selected_reasoning_effort
        .map(str::trim)
        .filter(|effort| !effort.is_empty())
        .map(str::to_owned);
    if thread.preview.trim().is_empty() {
        thread.preview = user_text.to_owned();
    }
    thread.updated_at = updated_at_unix;
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
    use pioneer_protocol::{
        McpScopeKind, Turn, TurnCapabilityKind, TurnKind, TurnOrigin, TurnStartResponse,
        TurnStatus, UserMessageAttachment,
    };

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
            execution_backend: None,
            reasoning: None,
            cli_runtime_options: None,
        });

        assert_eq!(params.thread_id, "thread");
        assert_eq!(params.turn_id, "turn");
        assert_eq!(params.input.len(), 1);
        assert_eq!(params.capabilities.len(), 1);
        assert_eq!(params.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(params.model_provider.as_deref(), Some("openai"));
        assert_eq!(params.mode, Some(ThreadMode::Agent));
        assert_eq!(params.sandbox_policy, None);
        assert_eq!(params.execution_backend, None);
        assert_eq!(params.reasoning, None);
        assert_eq!(params.cli_runtime_options, None);
    }

    #[test]
    fn turn_start_params_from_plan_preserves_reasoning_selection() {
        let params = turn_start_params_from_plan(TurnStartParamsPlan {
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            input: Vec::new(),
            capabilities: Vec::new(),
            model: Some("gpt-5.4".to_owned()),
            model_provider: Some("openai".to_owned()),
            mode: None,
            execution_backend: None,
            reasoning: turn_reasoning_selection_from_effort(Some(" high ".to_owned())),
            cli_runtime_options: None,
        });

        assert_eq!(
            params
                .reasoning
                .as_ref()
                .map(|selection| selection.effort.as_str()),
            Some("high")
        );
    }

    #[test]
    fn prepared_turn_snapshot_update_sets_model_preview_and_updated_at() {
        let mut thread = Thread {
            workspace_id: "ws_1".to_owned(),
            id: "thr_1".to_owned(),
            name: None,
            preview: "   ".to_owned(),
            mode: ThreadMode::Chat,
            model: "old-model".to_owned(),
            model_provider: "old-provider".to_owned(),
            reasoning_effort: None,
            created_at: 10,
            updated_at: 10,
            status: pioneer_protocol::ThreadStatus::Idle,
            origin_kind: pioneer_protocol::ThreadOriginKind::User,
            sidebar_visibility: pioneer_protocol::ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };

        apply_prepared_turn_to_thread_snapshot(
            &mut thread,
            Some("new-model"),
            Some("new-provider"),
            Some(" high "),
            "hello",
            42,
        );

        assert_eq!(thread.model, "new-model");
        assert_eq!(thread.model_provider, "new-provider");
        assert_eq!(thread.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(thread.preview, "hello");
        assert_eq!(thread.updated_at, 42);
    }

    #[test]
    fn prepared_turn_snapshot_update_preserves_existing_preview_and_partial_model_selection() {
        let mut thread = Thread {
            workspace_id: "ws_1".to_owned(),
            id: "thr_1".to_owned(),
            name: None,
            preview: "existing".to_owned(),
            mode: ThreadMode::Chat,
            model: "old-model".to_owned(),
            model_provider: "old-provider".to_owned(),
            reasoning_effort: None,
            created_at: 10,
            updated_at: 10,
            status: pioneer_protocol::ThreadStatus::Idle,
            origin_kind: pioneer_protocol::ThreadOriginKind::User,
            sidebar_visibility: pioneer_protocol::ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            turns: Vec::new(),
        };

        apply_prepared_turn_to_thread_snapshot(
            &mut thread,
            Some("new-model"),
            None,
            None,
            "hello",
            42,
        );

        assert_eq!(thread.model, "old-model");
        assert_eq!(thread.model_provider, "old-provider");
        assert!(thread.reasoning_effort.is_none());
        assert_eq!(thread.preview, "existing");
        assert_eq!(thread.updated_at, 42);
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

    #[test]
    fn turn_start_send_success_reduction_accepts_local_turn_and_applies_protocol_turn() {
        let turn = Turn {
            id: "turn".to_owned(),
            status: TurnStatus::InProgress,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            error: None,
            prompt_manifest: None,
        };

        let reduction = reduce_turn_start_send_success(
            TurnStartSendContext {
                thread_id: "thread".to_owned(),
                turn_id: "turn".to_owned(),
                pending_request_id: "pending".to_owned(),
            },
            TurnStartResponse { turn: turn.clone() },
        );

        let TurnStartSendReduction::Accepted { events } = reduction else {
            panic!("success should accept the local turn");
        };
        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            ConversationEvent::LocalTurnStartAccepted {
                thread_id,
                turn_id,
                pending_request_id,
            } if thread_id == "thread" && turn_id == "turn" && pending_request_id == "pending"
        ));
        assert!(matches!(
            &events[1],
            ConversationEvent::TurnStarted { thread_id, turn: started }
                if thread_id == "thread" && started == &turn
        ));
    }

    #[test]
    fn turn_start_send_failure_reduction_rejects_local_turn() {
        let reduction = reduce_turn_start_send_failure(
            TurnStartSendContext {
                thread_id: "thread".to_owned(),
                turn_id: "turn".to_owned(),
                pending_request_id: "pending".to_owned(),
            },
            "boom",
        );

        let TurnStartSendReduction::Rejected { event } = reduction else {
            panic!("failure should reject the local turn");
        };
        assert!(matches!(
            event,
            ConversationEvent::LocalTurnStartRejected {
                thread_id,
                turn_id,
                pending_request_id,
                error,
            } if thread_id == "thread"
                && turn_id == "turn"
                && pending_request_id == "pending"
                && error == "boom"
        ));
    }
}
