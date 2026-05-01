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
