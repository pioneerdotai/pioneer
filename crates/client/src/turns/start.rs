//! Turn start orchestration.

use crate::conversation::events::ConversationEvent;
use pioneer_protocol::{
    AgentExecutionBackend, PrincipalId, REQUEST_ID_LEN, Thread, ThreadMode, TurnCLIRuntimeOptions,
    TurnCapability, TurnPermissionProfileSelection, TurnReasoningSelection, TurnStartParams,
    TurnStartResponse, UserInput, UserMessageAttachment, generate_id,
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
    pub agent_launch: Option<pioneer_protocol::AgentLaunchSelection>,
    pub reply_to_turn_id: Option<String>,
    pub mentioned_principal_ids: Vec<PrincipalId>,
    pub execution_backend: Option<AgentExecutionBackend>,
    pub reasoning: Option<TurnReasoningSelection>,
    pub permission_profile: TurnPermissionProfileSelection,
    pub cli_runtime_options: Option<TurnCLIRuntimeOptions>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TurnStartSendContext {
    pub thread_id: String,
    pub turn_id: String,
    pub pending_request_id: String,
    pub mode: ThreadMode,
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
    let is_message = plan.mode == Some(ThreadMode::Message);
    TurnStartParams {
        agent_delegation_routes: Vec::new(),
        thread_id: plan.thread_id,
        turn_id: plan.turn_id,
        input: plan.input,
        capabilities: if is_message {
            Vec::new()
        } else {
            plan.capabilities
        },
        model: if is_message { None } else { plan.model },
        model_provider: if is_message {
            None
        } else {
            plan.model_provider
        },
        sandbox_policy: None,
        mode: plan.mode,
        agent_launch: if is_message { None } else { plan.agent_launch },
        reply_to_turn_id: plan.reply_to_turn_id,
        mentioned_principal_ids: plan.mentioned_principal_ids,
        execution_backend: if is_message {
            None
        } else {
            plan.execution_backend
        },
        reasoning: if is_message { None } else { plan.reasoning },
        permission_profile: (!is_message).then_some(plan.permission_profile),
        cli_runtime_options: if is_message {
            None
        } else {
            plan.cli_runtime_options
        },
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
    let turn_event = match response.turn.status {
        pioneer_protocol::TurnStatus::InProgress => ConversationEvent::TurnStarted {
            thread_id: context.thread_id.clone(),
            turn: response.turn,
        },
        pioneer_protocol::TurnStatus::Completed => ConversationEvent::TurnCompleted {
            thread_id: context.thread_id.clone(),
            turn: response.turn,
        },
        pioneer_protocol::TurnStatus::Failed | pioneer_protocol::TurnStatus::Interrupted => {
            ConversationEvent::TurnFailed {
                thread_id: context.thread_id.clone(),
                turn: response.turn,
            }
        }
        pioneer_protocol::TurnStatus::Blocked => ConversationEvent::TurnBlocked {
            thread_id: context.thread_id.clone(),
            turn: response.turn,
            resume: None,
        },
    };
    TurnStartSendReduction::Accepted {
        events: vec![
            local_turn_start_accepted_event(
                context.thread_id.clone(),
                context.turn_id,
                context.pending_request_id,
                context.mode,
            ),
            turn_event,
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
            context.mode,
            error,
        ),
    }
}

pub fn apply_prepared_turn_to_thread_snapshot(
    thread: &mut Thread,
    mode: ThreadMode,
    selected_model: Option<&str>,
    selected_provider: Option<&str>,
    selected_reasoning_effort: Option<&str>,
    user_text: &str,
    updated_at_unix: i64,
) {
    if mode != ThreadMode::Message {
        if let (Some(model), Some(provider)) = (selected_model, selected_provider) {
            thread.model = model.to_owned();
            thread.model_provider = provider.to_owned();
        }
        thread.reasoning_effort = selected_reasoning_effort
            .map(str::trim)
            .filter(|effort| !effort.is_empty())
            .map(str::to_owned);
    }
    if thread.preview.trim().is_empty() {
        thread.preview = user_text.to_owned();
    }
    thread.updated_at = updated_at_unix;
}

pub fn local_turn_start_requested_event(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    pending_request_id: impl Into<String>,
    mode: ThreadMode,
    user_text: impl Into<String>,
    attachments: Vec<UserMessageAttachment>,
) -> ConversationEvent {
    ConversationEvent::LocalTurnStartRequested {
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
        pending_request_id: pending_request_id.into(),
        mode,
        user_text: user_text.into(),
        attachments,
    }
}

pub fn local_turn_start_accepted_event(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    pending_request_id: impl Into<String>,
    mode: ThreadMode,
) -> ConversationEvent {
    ConversationEvent::LocalTurnStartAccepted {
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
        pending_request_id: pending_request_id.into(),
        mode,
    }
}

pub fn local_turn_start_rejected_event(
    thread_id: impl Into<String>,
    turn_id: impl Into<String>,
    pending_request_id: impl Into<String>,
    mode: ThreadMode,
    error: impl Into<String>,
) -> ConversationEvent {
    ConversationEvent::LocalTurnStartRejected {
        thread_id: thread_id.into(),
        turn_id: turn_id.into(),
        pending_request_id: pending_request_id.into(),
        mode,
        error: error.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pioneer_protocol::{
        SkillId, Turn, TurnCapabilityKind, TurnKind, TurnOrigin, TurnPermissionMode,
        TurnSkillCapabilitySummary, TurnStartResponse, TurnStatus, UserMessageAttachment,
    };

    fn skill_capability() -> TurnCapability {
        let skill_id = SkillId::new("D".repeat(21)).expect("valid skill id");
        TurnCapability {
            id: pioneer_protocol::skill_capability_key(&skill_id),
            label: Some("Docs".to_owned()),
            kind: TurnCapabilityKind::Skill {
                skill_id,
                pack_id: None,
            },
        }
    }

    fn permission_profile_selection(mode: TurnPermissionMode) -> TurnPermissionProfileSelection {
        TurnPermissionProfileSelection { mode }
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
            agent_launch: None,
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: None,
            permission_profile: permission_profile_selection(TurnPermissionMode::FullAccess),
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
        assert_eq!(
            params.permission_profile,
            Some(permission_profile_selection(TurnPermissionMode::FullAccess))
        );
        assert_eq!(params.cli_runtime_options, None);
    }

    #[test]
    fn turn_start_params_from_plan_keeps_message_collaboration_and_omits_execution_fields() {
        let mentioned_principal_id =
            PrincipalId::new("P00000000000000000001").expect("principal id");
        let params = turn_start_params_from_plan(TurnStartParamsPlan {
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            input: vec![text_user_input("hello").expect("text input")],
            capabilities: vec![skill_capability()],
            model: Some("must-not-cross".to_owned()),
            model_provider: Some("must-not-cross".to_owned()),
            mode: Some(ThreadMode::Message),
            agent_launch: None,
            reply_to_turn_id: Some("parent-turn".to_owned()),
            mentioned_principal_ids: vec![mentioned_principal_id.clone()],
            execution_backend: None,
            reasoning: turn_reasoning_selection_from_effort(Some("high".to_owned())),
            permission_profile: permission_profile_selection(TurnPermissionMode::FullAccess),
            cli_runtime_options: None,
        });

        assert_eq!(params.mode, Some(ThreadMode::Message));
        assert_eq!(params.reply_to_turn_id.as_deref(), Some("parent-turn"));
        assert_eq!(params.mentioned_principal_ids, vec![mentioned_principal_id]);
        assert!(params.capabilities.is_empty());
        assert!(params.model.is_none());
        assert!(params.model_provider.is_none());
        assert!(params.execution_backend.is_none());
        assert!(params.reasoning.is_none());
        assert!(params.permission_profile.is_none());
        assert!(params.cli_runtime_options.is_none());
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
            agent_launch: None,
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: turn_reasoning_selection_from_effort(Some(" high ".to_owned())),
            permission_profile: permission_profile_selection(TurnPermissionMode::FullAccess),
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
    fn turn_start_params_from_plan_preserves_permission_profile_selection() {
        let params = turn_start_params_from_plan(TurnStartParamsPlan {
            thread_id: "thread".to_owned(),
            turn_id: "turn".to_owned(),
            input: Vec::new(),
            capabilities: Vec::new(),
            model: None,
            model_provider: None,
            mode: None,
            agent_launch: None,
            reply_to_turn_id: None,
            mentioned_principal_ids: Vec::new(),
            execution_backend: None,
            reasoning: None,
            permission_profile: permission_profile_selection(TurnPermissionMode::Supervised),
            cli_runtime_options: None,
        });

        assert_eq!(
            params.permission_profile,
            Some(permission_profile_selection(TurnPermissionMode::Supervised))
        );
    }

    #[test]
    fn prepared_turn_snapshot_update_sets_model_preview_and_updated_at() {
        let mut thread = Thread {
            workspace_id: "ws_1".to_owned(),
            id: "thr_1".to_owned(),
            name: None,
            preview: "   ".to_owned(),
            preview_author: None,
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
            visibility: None,
            turns: Vec::new(),
        };

        apply_prepared_turn_to_thread_snapshot(
            &mut thread,
            ThreadMode::Agent,
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
            preview_author: None,
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
            visibility: None,
            turns: Vec::new(),
        };

        apply_prepared_turn_to_thread_snapshot(
            &mut thread,
            ThreadMode::Agent,
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
    fn message_snapshot_update_preserves_execution_selection() {
        let mut thread = Thread {
            workspace_id: "ws_1".to_owned(),
            id: "thr_1".to_owned(),
            name: None,
            preview: String::new(),
            preview_author: None,
            mode: ThreadMode::Message,
            model: "old-model".to_owned(),
            model_provider: "old-provider".to_owned(),
            reasoning_effort: Some("high".to_owned()),
            created_at: 10,
            updated_at: 10,
            status: pioneer_protocol::ThreadStatus::Idle,
            origin_kind: pioneer_protocol::ThreadOriginKind::User,
            sidebar_visibility: pioneer_protocol::ThreadSidebarVisibility::Visible,
            agent_nickname: None,
            agent_role: None,
            visibility: None,
            turns: Vec::new(),
        };

        apply_prepared_turn_to_thread_snapshot(
            &mut thread,
            ThreadMode::Message,
            None,
            None,
            None,
            "ordinary message",
            42,
        );

        assert_eq!(thread.model, "old-model");
        assert_eq!(thread.model_provider, "old-provider");
        assert_eq!(thread.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(thread.preview, "ordinary message");
        assert_eq!(thread.updated_at, 42);
    }

    #[test]
    fn local_turn_start_events_preserve_ids_and_payloads() {
        let skill_id = SkillId::new("E".repeat(21)).expect("valid skill id");
        let attachment = UserMessageAttachment::Skill {
            capability: TurnSkillCapabilitySummary {
                skill_id: skill_id.clone(),
                label: "pioneer/browser".to_owned(),
                owner: Some("pioneer".to_owned()),
                slug: "browser".to_owned(),
                source_kind: "system".to_owned(),
                pack: None,
            },
        };

        let requested = local_turn_start_requested_event(
            "thread",
            "turn",
            "pending",
            ThreadMode::Agent,
            "hello",
            vec![attachment],
        );
        assert!(matches!(
            requested,
            ConversationEvent::LocalTurnStartRequested {
                ref thread_id,
                ref turn_id,
                ref pending_request_id,
                mode,
                ref user_text,
                ref attachments,
            } if thread_id == "thread"
                && turn_id == "turn"
                && pending_request_id == "pending"
                && mode == ThreadMode::Agent
                && user_text == "hello"
                && matches!(
                    attachments.as_slice(),
                    [UserMessageAttachment::Skill { capability }]
                        if capability.skill_id == skill_id
                            && capability.label == "pioneer/browser"
                )
        ));

        let accepted =
            local_turn_start_accepted_event("thread", "turn", "pending", ThreadMode::Agent);
        assert!(matches!(
            accepted,
            ConversationEvent::LocalTurnStartAccepted {
                ref thread_id,
                ref turn_id,
                ref pending_request_id,
                mode,
                ..
            } if thread_id == "thread"
                && turn_id == "turn"
                && pending_request_id == "pending"
                && mode == ThreadMode::Agent
        ));

        let rejected =
            local_turn_start_rejected_event("thread", "turn", "pending", ThreadMode::Agent, "boom");
        assert!(matches!(
            rejected,
            ConversationEvent::LocalTurnStartRejected {
                ref thread_id,
                ref turn_id,
                ref pending_request_id,
                mode,
                ref error,
            } if thread_id == "thread"
                && turn_id == "turn"
                && pending_request_id == "pending"
                && mode == ThreadMode::Agent
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
            mode: Default::default(),
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };

        let reduction = reduce_turn_start_send_success(
            TurnStartSendContext {
                thread_id: "thread".to_owned(),
                turn_id: "turn".to_owned(),
                pending_request_id: "pending".to_owned(),
                mode: ThreadMode::Agent,
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
                mode,
                ..
            } if thread_id == "thread"
                && turn_id == "turn"
                && pending_request_id == "pending"
                && *mode == ThreadMode::Agent
        ));
        assert!(matches!(
            &events[1],
            ConversationEvent::TurnStarted {
                thread_id,
                turn: started,
                ..
            }
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
                mode: ThreadMode::Agent,
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
                mode,
                error,
            } if thread_id == "thread"
                && turn_id == "turn"
                && pending_request_id == "pending"
                && mode == ThreadMode::Agent
                && error == "boom"
        ));
    }

    #[test]
    fn completed_turn_start_response_reduces_to_completed_event() {
        let turn = Turn {
            id: "turn".to_owned(),
            status: TurnStatus::Completed,
            turn_kind: TurnKind::Conversation,
            origin: TurnOrigin::User,
            mode: ThreadMode::Message,
            author: None,
            reply_to_turn_id: None,
            mentions: Vec::new(),
            message_revision: 0,
            message_deleted: false,
            error: None,
            prompt_manifest: None,
            permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
        };
        let reduction = reduce_turn_start_send_success(
            TurnStartSendContext {
                thread_id: "thread".to_owned(),
                turn_id: "turn".to_owned(),
                pending_request_id: "pending".to_owned(),
                mode: ThreadMode::Message,
            },
            TurnStartResponse { turn: turn.clone() },
        );

        let TurnStartSendReduction::Accepted { events } = reduction else {
            panic!("success should accept the completed Message");
        };
        assert!(matches!(
            &events[1],
            ConversationEvent::TurnCompleted { thread_id, turn: completed }
                if thread_id == "thread" && completed == &turn
        ));
    }
}
