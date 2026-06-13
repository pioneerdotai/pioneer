use super::{ActiveTurnRequest, RecoveryAttemptRequest};
use pioneer_protocol::{TurnCapabilityKind, UserInput};
use pioneer_provider::{ChatMessage, InputContentType, Role};

pub(super) fn apply_recovery_adjustments(
    turn_request: &mut ActiveTurnRequest,
    request: &RecoveryAttemptRequest,
) {
    if let Some(model_override) = request.model_override.clone()
        && !model_override.trim().is_empty()
    {
        turn_request.model = model_override;
    }
    if request.compact_history {
        turn_request.history =
            compact_history_for_recovery(std::mem::take(&mut turn_request.history));
    }
    if request.disable_tool_calling {
        turn_request.capabilities.retain(|capability| {
            !matches!(
                capability.kind,
                TurnCapabilityKind::McpServer { .. } | TurnCapabilityKind::McpTool { .. }
            )
        });
    }
    if request.disable_image_input {
        turn_request.input.retain(|input| {
            !matches!(
                input,
                UserInput::Image { .. } | UserInput::LocalImage { .. }
            )
        });
        turn_request
            .resolved_artifacts
            .retain(|artifact| artifact.content_type != InputContentType::Image);
    }
    turn_request.execution_options.force_non_stream = request.force_non_stream;
    turn_request.execution_options.disable_tool_calling = request.disable_tool_calling;
    turn_request.execution_options.continue_generation_hint = request.continue_generation;
    turn_request.retained_llm_context = request.retained_llm_context.clone();
    turn_request.execution_checkpoint_context = request.execution_checkpoint_context.clone();
    if let Some(context) = request.execution_checkpoint_context.as_ref() {
        turn_request.execution_window_index = turn_request
            .execution_window_index
            .max(context.next_window_index());
        turn_request
            .execution_usage
            .observe_checkpoint_payload(&context.payload);
    }
}

fn compact_history_for_recovery(history: Vec<ChatMessage>) -> Vec<ChatMessage> {
    if history.len() <= 12 {
        return history;
    }

    let keep = (history.len() / 2).max(12);
    let start = history.len().saturating_sub(keep);
    let mut compacted = history[start..].to_vec();

    let has_system = compacted
        .iter()
        .any(|message| matches!(message.role, Role::System));
    if !has_system
        && let Some(system_message) = history
            .iter()
            .find(|message| matches!(message.role, Role::System))
            .cloned()
    {
        compacted.insert(0, system_message);
    }

    compacted
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ResolvedArtifactInput, TurnExecutionOptions, TurnExecutionUsageCounters,
        WorkspaceSkillPolicy,
    };
    use pioneer_protocol::{
        ExecutionCheckpointOriginalRequestSummary, ExecutionCheckpointProviderBudgetSummary,
        ExecutionCheckpointToolSummary, ExecutionCheckpointWindowSummary, McpScopeKind, ThreadMode,
        TurnCapability, TurnCapabilityKind, TurnItemType, UserInput,
        build_execution_checkpoint_payload,
    };
    use pioneer_provider::MessageAttachment;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn recovery_adjustments_preserve_turn_request_inputs() {
        let mut turn_request = ActiveTurnRequest {
            turn_id: "turn_recovery".to_owned(),
            execution_window_index: 3,
            mode: ThreadMode::Agent,
            hook_runtime_context: crate::AgentTurnHookRuntimeContext::default(),
            model: "test-model".to_owned(),
            provider_name: "test-provider".to_owned(),
            workspace_skill_policies:
                HashMap::<pioneer_skills::SkillPolicyKey, WorkspaceSkillPolicy>::new(),
            input: vec![UserInput::Text {
                text: "retry".to_owned(),
                text_elements: Vec::new(),
            }],
            capabilities: Vec::new(),
            resolved_artifacts: Vec::<ResolvedArtifactInput>::new(),
            runtime_environment: HashMap::new(),
            history: vec![ChatMessage::user("retry")],
            retained_llm_context: Vec::new(),
            execution_checkpoint_context: None,
            execution_usage: TurnExecutionUsageCounters::default(),
            execution_options: TurnExecutionOptions::default(),
        };
        let request = RecoveryAttemptRequest {
            recovery_job_id: "job_agents_md".to_owned(),
            recovery_attempt_id: "attempt_agents_md".to_owned(),
            turn_id: turn_request.turn_id.clone(),
            item_id: "item_agents_md".to_owned(),
            item_type: TurnItemType::AgentMessage,
            force_non_stream: true,
            disable_tool_calling: false,
            disable_image_input: false,
            refresh_provider_auth: false,
            compact_history: true,
            continue_generation: true,
            model_override: Some("recovery-model".to_owned()),
            retained_llm_context: vec![crate::RetainedToolLlmContext {
                item_id: "tool_1".to_owned(),
                tool_name: "read_file".to_owned(),
                arguments: "{}".to_owned(),
                sequence: 1,
                payload: json!({"kind": "empty"}),
            }],
            execution_checkpoint_context: None,
        };

        apply_recovery_adjustments(&mut turn_request, &request);

        assert_eq!(
            turn_request.input,
            vec![UserInput::Text {
                text: "retry".to_owned(),
                text_elements: Vec::new(),
            }]
        );
        assert_eq!(turn_request.model, "recovery-model");
        assert_eq!(turn_request.execution_window_index, 3);
        assert!(turn_request.execution_options.force_non_stream);
        assert!(turn_request.execution_options.continue_generation_hint);
    }

    #[test]
    fn recovery_adjustments_apply_execution_checkpoint_context() {
        let mut turn_request = ActiveTurnRequest {
            turn_id: "turn_checkpoint_recovery".to_owned(),
            execution_window_index: 1,
            mode: ThreadMode::Agent,
            hook_runtime_context: crate::AgentTurnHookRuntimeContext::default(),
            model: "test-model".to_owned(),
            provider_name: "test-provider".to_owned(),
            workspace_skill_policies:
                HashMap::<pioneer_skills::SkillPolicyKey, WorkspaceSkillPolicy>::new(),
            input: vec![UserInput::Text {
                text: "continue".to_owned(),
                text_elements: Vec::new(),
            }],
            capabilities: Vec::new(),
            resolved_artifacts: Vec::<ResolvedArtifactInput>::new(),
            runtime_environment: HashMap::new(),
            history: Vec::new(),
            retained_llm_context: Vec::new(),
            execution_checkpoint_context: None,
            execution_usage: TurnExecutionUsageCounters::default(),
            execution_options: TurnExecutionOptions::default(),
        };
        let payload = build_execution_checkpoint_payload(
            "ws_checkpoint".to_owned(),
            "thr_checkpoint".to_owned(),
            turn_request.turn_id.clone(),
            ExecutionCheckpointOriginalRequestSummary {
                input_count: 1,
                text_preview: Some("continue".to_owned()),
                text_truncated: false,
                attachment_count: 0,
                attachment_kinds: Vec::new(),
            },
            ExecutionCheckpointWindowSummary {
                window_id: Some("window_4".to_owned()),
                window_index: 4,
                started_at_unix_ms: Some(1),
                completed_at_unix_ms: Some(2),
                agent_round_count: 1,
                tool_call_count: 2,
                provider_token_count: Some(3),
                exhaustion_reason: None,
            },
            ExecutionCheckpointProviderBudgetSummary {
                model: Some("test-model".to_owned()),
                model_provider: Some("test-provider".to_owned()),
                agent_round_count: 1,
                tool_call_count: 2,
                provider_token_count: Some(3),
                provider_usage_available: true,
                exhaustion_reason: None,
                exhausted_limit: None,
                exhausted_observed: None,
            },
            ExecutionCheckpointToolSummary {
                requested_count: 2,
                executed_count: 2,
                unexecuted_count: 0,
                total_count: 2,
                succeeded_count: 2,
                failed_count: 0,
                in_progress_count: 0,
                detail_limit: 0,
                details_truncated: false,
                details: Vec::new(),
            },
            Vec::new(),
        );
        let context = crate::ExecutionCheckpointContext {
            window_id: "window_4".to_owned(),
            window_index: 4,
            checkpoint_id: "checkpoint_4".to_owned(),
            checkpoint_kind: "window_exhausted".to_owned(),
            payload,
        };
        let request = RecoveryAttemptRequest {
            recovery_job_id: "job_checkpoint".to_owned(),
            recovery_attempt_id: "attempt_checkpoint".to_owned(),
            turn_id: turn_request.turn_id.clone(),
            item_id: "item_checkpoint".to_owned(),
            item_type: TurnItemType::AgentMessage,
            force_non_stream: false,
            disable_tool_calling: false,
            disable_image_input: false,
            refresh_provider_auth: false,
            compact_history: false,
            continue_generation: true,
            model_override: None,
            retained_llm_context: Vec::new(),
            execution_checkpoint_context: Some(context),
        };

        apply_recovery_adjustments(&mut turn_request, &request);

        assert_eq!(turn_request.execution_window_index, 5);
        assert_eq!(
            turn_request.execution_usage,
            TurnExecutionUsageCounters {
                total_windows: 4,
                total_tool_calls: 2,
                total_wall_clock_ms: 1,
                total_provider_tokens: 3,
                provider_token_usage_unknown: false,
                consecutive_failed_windows: 0,
            }
        );
        assert!(turn_request.execution_options.continue_generation_hint);
        assert_eq!(
            turn_request
                .execution_checkpoint_context
                .as_ref()
                .map(|context| context.checkpoint_id.as_str()),
            Some("checkpoint_4")
        );
    }

    #[test]
    fn recovery_adjustments_downgrade_unsupported_tools_and_images() {
        let mut turn_request = ActiveTurnRequest {
            turn_id: "turn_downgrade".to_owned(),
            execution_window_index: 1,
            mode: ThreadMode::Agent,
            hook_runtime_context: crate::AgentTurnHookRuntimeContext::default(),
            model: "test-model".to_owned(),
            provider_name: "test-provider".to_owned(),
            workspace_skill_policies:
                HashMap::<pioneer_skills::SkillPolicyKey, WorkspaceSkillPolicy>::new(),
            input: vec![
                UserInput::Text {
                    text: "describe this".to_owned(),
                    text_elements: Vec::new(),
                },
                UserInput::Image {
                    url: "https://example.test/image.png".to_owned(),
                },
                UserInput::LocalImage {
                    path: "/tmp/image.png".to_owned(),
                },
            ],
            capabilities: vec![
                TurnCapability {
                    id: "skill:workspace:docs".to_owned(),
                    kind: TurnCapabilityKind::Skill {
                        slug: "docs".to_owned(),
                        source_kind: "workspace".to_owned(),
                    },
                    label: None,
                },
                TurnCapability {
                    id: "mcp-server:workspace:browser".to_owned(),
                    kind: TurnCapabilityKind::McpServer {
                        name: "browser".to_owned(),
                        scope_kind: McpScopeKind::Workspace,
                    },
                    label: None,
                },
                TurnCapability {
                    id: "mcp-tool:workspace:browser:open".to_owned(),
                    kind: TurnCapabilityKind::McpTool {
                        server_name: "browser".to_owned(),
                        raw_tool_name: "open".to_owned(),
                        scope_kind: McpScopeKind::Workspace,
                    },
                    label: None,
                },
            ],
            resolved_artifacts: vec![
                ResolvedArtifactInput {
                    artifact_id: "art_image".to_owned(),
                    version_id: None,
                    content_type: InputContentType::Image,
                    attachment: MessageAttachment::from_url(
                        "https://example.test/image.png",
                        "image/png",
                    ),
                },
                ResolvedArtifactInput {
                    artifact_id: "art_file".to_owned(),
                    version_id: None,
                    content_type: InputContentType::File,
                    attachment: MessageAttachment::from_url(
                        "https://example.test/file.txt",
                        "text/plain",
                    ),
                },
            ],
            runtime_environment: HashMap::new(),
            history: vec![ChatMessage::user("describe this")],
            retained_llm_context: Vec::new(),
            execution_checkpoint_context: None,
            execution_usage: TurnExecutionUsageCounters::default(),
            execution_options: TurnExecutionOptions::default(),
        };
        let request = RecoveryAttemptRequest {
            recovery_job_id: "job_downgrade".to_owned(),
            recovery_attempt_id: "attempt_downgrade".to_owned(),
            turn_id: turn_request.turn_id.clone(),
            item_id: "item_downgrade".to_owned(),
            item_type: TurnItemType::AgentMessage,
            force_non_stream: false,
            disable_tool_calling: true,
            disable_image_input: true,
            refresh_provider_auth: false,
            compact_history: false,
            continue_generation: false,
            model_override: None,
            retained_llm_context: Vec::new(),
            execution_checkpoint_context: None,
        };

        apply_recovery_adjustments(&mut turn_request, &request);

        assert!(turn_request.execution_options.disable_tool_calling);
        assert_eq!(turn_request.input.len(), 1);
        assert_eq!(turn_request.capabilities.len(), 1);
        assert!(matches!(
            turn_request.capabilities[0].kind,
            TurnCapabilityKind::Skill { .. }
        ));
        assert_eq!(turn_request.resolved_artifacts.len(), 1);
        assert_eq!(
            turn_request.resolved_artifacts[0].content_type,
            InputContentType::File
        );
    }
}
