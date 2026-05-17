use super::{ActiveTurnRequest, RecoveryAttemptRequest};
use pioneer_provider::{ChatMessage, Role};

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
    turn_request.execution_options.force_non_stream = request.force_non_stream;
    turn_request.execution_options.continue_generation_hint = request.continue_generation;
    turn_request.retained_llm_context = request.retained_llm_context.clone();
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
    use crate::{ResolvedArtifactInput, TurnExecutionOptions, WorkspaceSkillPolicy};
    use pioneer_protocol::{ThreadMode, TurnItemType, UserInput};
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn recovery_adjustments_preserve_turn_request_inputs() {
        let mut turn_request = ActiveTurnRequest {
            turn_id: "turn_recovery".to_owned(),
            mode: ThreadMode::Agent,
            model: "test-model".to_owned(),
            provider_name: "test-provider".to_owned(),
            workspace_skill_policies:
                HashMap::<pioneer_skills::SkillPolicyKey, WorkspaceSkillPolicy>::new(),
            input: vec![UserInput::Text {
                text: "retry".to_owned(),
                text_elements: Vec::new(),
            }],
            resolved_artifacts: Vec::<ResolvedArtifactInput>::new(),
            runtime_environment: HashMap::new(),
            history: vec![ChatMessage::user("retry")],
            retained_llm_context: Vec::new(),
            execution_options: TurnExecutionOptions::default(),
        };
        let request = RecoveryAttemptRequest {
            recovery_job_id: "job_agents_md".to_owned(),
            recovery_attempt_id: "attempt_agents_md".to_owned(),
            turn_id: turn_request.turn_id.clone(),
            item_id: "item_agents_md".to_owned(),
            item_type: TurnItemType::AgentMessage,
            force_non_stream: true,
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
        assert!(turn_request.execution_options.force_non_stream);
        assert!(turn_request.execution_options.continue_generation_hint);
    }
}
