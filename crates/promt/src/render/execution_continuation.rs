use crate::content;
use crate::section::{PromptSection, PromptSectionId, PromptStability};
use pioneer_protocol::{
    ExecutionCheckpointPayload, ExecutionCheckpointStrictObligation,
    ExecutionCheckpointToolCallSummary, ExecutionWindowExhaustionReason,
};
use serde::Serialize;

pub const PRIOR_VISIBLE_ASSISTANT_TEXT_MAX_CHARS: usize = 2_000;
const TOOL_METADATA_MAX_CHARS: usize = 512;
const STRICT_OBLIGATION_DESCRIPTION_MAX_CHARS: usize = 512;
const STRICT_OBLIGATION_REFS_MAX_CHARS: usize = 512;

#[derive(Debug, Clone, Copy)]
pub struct ExecutionContinuationRuntimeFactsInput<'a> {
    pub checkpoint: &'a ExecutionCheckpointPayload,
    pub prior_visible_assistant_text: Option<&'a str>,
}

pub fn render_execution_continuation_prompt() -> &'static str {
    content::EXECUTION_CONTINUATION_PROMPT
}

pub fn render_execution_continuation_runtime_facts(
    input: &ExecutionContinuationRuntimeFactsInput<'_>,
) -> String {
    let checkpoint = input.checkpoint;
    let mut lines = Vec::new();

    lines.push(format!(
        "Checkpoint: schema_version={}, workspace_id={}, thread_id={}, turn_id={}",
        checkpoint.schema_version,
        checkpoint.workspace_id,
        checkpoint.thread_id,
        checkpoint.turn_id
    ));

    match checkpoint.original_request.text_preview.as_deref() {
        Some(preview) if !preview.trim().is_empty() => {
            let suffix = if checkpoint.original_request.text_truncated {
                " (truncated)"
            } else {
                ""
            };
            lines.push(format!(
                "Original request preview{suffix}: {}",
                preview.trim()
            ));
        }
        _ => lines.push(format!(
            "Original request: {} input(s), {} attachment(s)",
            checkpoint.original_request.input_count, checkpoint.original_request.attachment_count
        )),
    }
    if !checkpoint.original_request.attachment_kinds.is_empty() {
        lines.push(format!(
            "Original attachment kinds: {}",
            checkpoint.original_request.attachment_kinds.join(", ")
        ));
    }

    lines.push(format!(
        "Completed window: index={}, agent_rounds={}, tool_calls={}, provider_tokens={}",
        checkpoint.window.window_index,
        checkpoint.window.agent_round_count,
        checkpoint.window.tool_call_count,
        checkpoint
            .window
            .provider_token_count
            .map(|count| count.to_string())
            .unwrap_or_else(|| "unknown".to_owned())
    ));
    if let Some(reason) = checkpoint.window.exhaustion_reason {
        lines.push(format!(
            "Window exhaustion reason: {}",
            exhaustion_reason_label(reason)
        ));
    }
    if let (Some(limit), Some(observed)) = (
        checkpoint.provider_budget.exhausted_limit,
        checkpoint.provider_budget.exhausted_observed,
    ) {
        lines.push(format!(
            "Observed exhausted budget: limit={limit}, observed={observed}"
        ));
    }

    lines.push(format!(
        "Tool summary: requested={}, executed={}, succeeded={}, failed={}, in_progress={}, unexecuted={}",
        checkpoint.tools.requested_count,
        checkpoint.tools.executed_count,
        checkpoint.tools.succeeded_count,
        checkpoint.tools.failed_count,
        checkpoint.tools.in_progress_count,
        checkpoint.tools.unexecuted_count
    ));
    if checkpoint.tools.details_truncated {
        lines.push(format!(
            "Tool detail list was truncated to {} item(s).",
            checkpoint.tools.detail_limit
        ));
    }
    for detail in &checkpoint.tools.details {
        lines.push(render_tool_detail(detail));
    }

    if checkpoint.strict_obligations.is_empty() {
        lines
            .push("Strict unresolved obligations: none reported by runtime validators.".to_owned());
    } else {
        lines.push("Strict unresolved obligations reported by runtime validators:".to_owned());
        for obligation in &checkpoint.strict_obligations {
            lines.push(render_strict_obligation(obligation));
        }
    }

    if let Some(text) = input.prior_visible_assistant_text.map(str::trim)
        && !text.is_empty()
    {
        if text.chars().count() <= PRIOR_VISIBLE_ASSISTANT_TEXT_MAX_CHARS {
            lines.push(format!("Prior visible assistant text: {text}"));
        } else {
            lines.push(format!(
                "Prior visible assistant text omitted because it exceeded {} chars.",
                PRIOR_VISIBLE_ASSISTANT_TEXT_MAX_CHARS
            ));
        }
    }

    lines.join("\n")
}

pub fn execution_continuation_section() -> PromptSection {
    PromptSection {
        id: PromptSectionId::ExecutionContinuation,
        stability: PromptStability::Dynamic,
        title: content::SECTION_TITLE_EXECUTION_CONTINUATION.to_owned(),
        content: render_execution_continuation_prompt().to_owned(),
        sources: Vec::new(),
    }
}

pub fn execution_continuation_section_with_runtime_facts(
    input: &ExecutionContinuationRuntimeFactsInput<'_>,
) -> PromptSection {
    let content = format!(
        "{}\n\n{}",
        render_execution_continuation_prompt(),
        render_execution_continuation_runtime_facts(input)
    );
    PromptSection {
        id: PromptSectionId::ExecutionContinuation,
        stability: PromptStability::Dynamic,
        title: content::SECTION_TITLE_EXECUTION_CONTINUATION.to_owned(),
        content,
        sources: Vec::new(),
    }
}

fn render_tool_detail(detail: &ExecutionCheckpointToolCallSummary) -> String {
    let metadata = bounded_json(&detail.metadata.to_json(), TOOL_METADATA_MAX_CHARS);
    let success = detail
        .success
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    let mut line = format!(
        "Tool detail: item_id={}, tool={}, status={}, success={}, metadata={}",
        detail.item_id,
        detail.tool_name,
        snake_label(detail.status),
        success,
        metadata
    );
    if let Some(error_class) = detail.error_class {
        line.push_str(format!(", error_class={}", snake_label(error_class)).as_str());
    }
    if let Some(retry_error_class) = detail.retry_error_class.as_deref()
        && !retry_error_class.is_empty()
    {
        line.push_str(format!(", retry_error_class={retry_error_class}").as_str());
    }
    line
}

fn render_strict_obligation(obligation: &ExecutionCheckpointStrictObligation) -> String {
    let description = bounded_text(
        obligation.description.as_str(),
        STRICT_OBLIGATION_DESCRIPTION_MAX_CHARS,
    );
    let refs = if obligation.refs.is_empty() {
        String::new()
    } else {
        let refs_json = serde_json::to_value(&obligation.refs)
            .unwrap_or_else(|_| serde_json::Value::Object(Default::default()));
        format!(
            ", refs={}",
            bounded_json(&refs_json, STRICT_OBLIGATION_REFS_MAX_CHARS)
        )
    };
    format!(
        "Strict obligation: id={}, kind={}, description={}{}",
        obligation.obligation_id, obligation.kind, description, refs
    )
}

fn exhaustion_reason_label(reason: ExecutionWindowExhaustionReason) -> String {
    snake_label(reason)
}

fn snake_label<T: Serialize>(value: T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_else(|| "unknown".to_owned())
}

fn bounded_json(value: &serde_json::Value, max_chars: usize) -> String {
    let serialized = serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned());
    let char_count = serialized.chars().count();
    if char_count <= max_chars {
        return serialized;
    }

    let mut truncated = serialized.chars().take(max_chars).collect::<String>();
    truncated.push_str("...<truncated>");
    truncated
}

fn bounded_text(value: &str, max_chars: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= max_chars {
        return value.to_owned();
    }

    let mut truncated = value.chars().take(max_chars).collect::<String>();
    truncated.push_str("...<truncated>");
    truncated
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn execution_continuation_prompt_is_operational() {
        let prompt = render_execution_continuation_prompt();

        assert!(prompt.contains("same user turn"));
        assert!(prompt.contains("new execution window"));
        assert!(prompt.contains("without restarting"));
        assert!(prompt.contains("Do not replay prior failed tool calls verbatim"));
        assert!(!prompt.contains("completed"));
        assert!(!prompt.contains("successful"));
    }

    #[test]
    fn execution_continuation_section_is_independent() {
        let section = execution_continuation_section();

        assert_eq!(section.id, PromptSectionId::ExecutionContinuation);
        assert_eq!(section.title, "Execution Continuation");
        assert_eq!(section.stability, PromptStability::Dynamic);
    }

    #[test]
    fn runtime_facts_render_bounded_checkpoint_details() {
        let mut checkpoint = pioneer_protocol::build_execution_checkpoint_payload(
            "ws".to_owned(),
            "thr".to_owned(),
            "turn".to_owned(),
            pioneer_protocol::ExecutionCheckpointOriginalRequestSummary {
                input_count: 1,
                text_preview: Some("Create the report".to_owned()),
                text_truncated: false,
                attachment_count: 0,
                attachment_kinds: Vec::new(),
            },
            pioneer_protocol::ExecutionCheckpointWindowSummary {
                window_id: Some("window_1".to_owned()),
                window_index: 1,
                started_at_unix_ms: Some(1),
                completed_at_unix_ms: Some(2),
                agent_round_count: 3,
                tool_call_count: 4,
                provider_token_count: Some(500),
                exhaustion_reason: Some(
                    pioneer_protocol::ExecutionWindowExhaustionReason::MaxToolCallsPerWindow,
                ),
            },
            pioneer_protocol::ExecutionCheckpointProviderBudgetSummary {
                model: Some("model".to_owned()),
                model_provider: Some("provider".to_owned()),
                agent_round_count: 3,
                tool_call_count: 4,
                provider_token_count: Some(500),
                provider_usage_available: true,
                exhaustion_reason: Some(
                    pioneer_protocol::ExecutionWindowExhaustionReason::MaxToolCallsPerWindow,
                ),
                exhausted_limit: Some(4),
                exhausted_observed: Some(5),
            },
            pioneer_protocol::ExecutionCheckpointToolSummary {
                requested_count: 4,
                executed_count: 4,
                unexecuted_count: 0,
                total_count: 4,
                succeeded_count: 3,
                failed_count: 1,
                in_progress_count: 0,
                detail_limit: 1,
                details_truncated: false,
                details: Vec::new(),
            },
            Vec::new(),
        );
        checkpoint
            .tools
            .details
            .push(pioneer_protocol::ExecutionCheckpointToolCallSummary {
                item_id: "tool_1".to_owned(),
                tool_name: "write_file".to_owned(),
                item_type: pioneer_protocol::TurnItemType::FileChange,
                status: pioneer_protocol::ToolCallStatus::Failed,
                success: Some(false),
                error_class: Some(pioneer_protocol::ToolErrorClass::InvalidArguments),
                retry_error_class: None,
                metadata: pioneer_protocol::ToolMetadata::from_json(
                    serde_json::json!({"error": "x".repeat(4_000)}),
                ),
            });

        let rendered =
            render_execution_continuation_runtime_facts(&ExecutionContinuationRuntimeFactsInput {
                checkpoint: &checkpoint,
                prior_visible_assistant_text: Some("short visible text"),
            });

        assert!(rendered.contains("Original request preview: Create the report"));
        assert!(rendered.contains("Completed window: index=1"));
        assert!(rendered.contains("Window exhaustion reason: max_tool_calls_per_window"));
        assert!(rendered.contains("Tool summary: requested=4"));
        assert!(rendered.contains("tool=write_file"));
        assert!(
            rendered
                .contains("Strict unresolved obligations: none reported by runtime validators.")
        );
        assert!(rendered.contains("Prior visible assistant text: short visible text"));
        assert!(rendered.contains("<truncated>"));
        assert!(!rendered.contains(&"x".repeat(1_000)));
    }

    #[test]
    fn runtime_facts_render_reported_strict_obligations_only() {
        let mut refs = BTreeMap::new();
        refs.insert("artifact_id".to_owned(), "artifact_1".to_owned());
        let checkpoint = pioneer_protocol::build_execution_checkpoint_payload(
            "ws",
            "thr",
            "turn",
            pioneer_protocol::ExecutionCheckpointOriginalRequestSummary {
                input_count: 1,
                text_preview: None,
                text_truncated: false,
                attachment_count: 0,
                attachment_kinds: Vec::new(),
            },
            pioneer_protocol::ExecutionCheckpointWindowSummary {
                window_id: Some("window_1".to_owned()),
                window_index: 1,
                started_at_unix_ms: None,
                completed_at_unix_ms: None,
                agent_round_count: 1,
                tool_call_count: 0,
                provider_token_count: None,
                exhaustion_reason: None,
            },
            pioneer_protocol::ExecutionCheckpointProviderBudgetSummary {
                model: None,
                model_provider: None,
                agent_round_count: 1,
                tool_call_count: 0,
                provider_token_count: None,
                provider_usage_available: false,
                exhaustion_reason: None,
                exhausted_limit: None,
                exhausted_observed: None,
            },
            pioneer_protocol::ExecutionCheckpointToolSummary {
                requested_count: 0,
                executed_count: 0,
                unexecuted_count: 0,
                total_count: 0,
                succeeded_count: 0,
                failed_count: 0,
                in_progress_count: 0,
                detail_limit: 0,
                details_truncated: false,
                details: Vec::new(),
            },
            vec![pioneer_protocol::ExecutionCheckpointStrictObligation {
                obligation_id: "obligation_1".to_owned(),
                kind: "artifact_not_registered".to_owned(),
                description: "artifact was prepared but not finalized".to_owned(),
                refs,
            }],
        );

        let rendered =
            render_execution_continuation_runtime_facts(&ExecutionContinuationRuntimeFactsInput {
                checkpoint: &checkpoint,
                prior_visible_assistant_text: None,
            });

        assert!(rendered.contains("Strict unresolved obligations reported by runtime validators:"));
        assert!(
            rendered.contains("Strict obligation: id=obligation_1, kind=artifact_not_registered")
        );
        assert!(rendered.contains("refs={\"artifact_id\":\"artifact_1\"}"));
        assert!(!rendered.contains("none reported"));
    }

    #[test]
    fn runtime_facts_render_core_snapshot() {
        let mut checkpoint = pioneer_protocol::build_execution_checkpoint_payload(
            "ws_snapshot",
            "thr_snapshot",
            "turn_snapshot",
            pioneer_protocol::ExecutionCheckpointOriginalRequestSummary {
                input_count: 1,
                text_preview: Some("Create the proposal".to_owned()),
                text_truncated: false,
                attachment_count: 1,
                attachment_kinds: vec!["local_file".to_owned()],
            },
            pioneer_protocol::ExecutionCheckpointWindowSummary {
                window_id: Some("window_snapshot_1".to_owned()),
                window_index: 2,
                started_at_unix_ms: Some(100),
                completed_at_unix_ms: Some(200),
                agent_round_count: 5,
                tool_call_count: 6,
                provider_token_count: None,
                exhaustion_reason: Some(
                    pioneer_protocol::ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow,
                ),
            },
            pioneer_protocol::ExecutionCheckpointProviderBudgetSummary {
                model: Some("model".to_owned()),
                model_provider: Some("provider".to_owned()),
                agent_round_count: 5,
                tool_call_count: 6,
                provider_token_count: None,
                provider_usage_available: false,
                exhaustion_reason: Some(
                    pioneer_protocol::ExecutionWindowExhaustionReason::MaxAgentRoundsPerWindow,
                ),
                exhausted_limit: Some(5),
                exhausted_observed: Some(5),
            },
            pioneer_protocol::ExecutionCheckpointToolSummary {
                requested_count: 6,
                executed_count: 5,
                unexecuted_count: 1,
                total_count: 6,
                succeeded_count: 4,
                failed_count: 1,
                in_progress_count: 0,
                detail_limit: 2,
                details_truncated: true,
                details: Vec::new(),
            },
            Vec::new(),
        );
        checkpoint
            .tools
            .details
            .push(pioneer_protocol::ExecutionCheckpointToolCallSummary {
                item_id: "tool_snapshot".to_owned(),
                tool_name: "read_file".to_owned(),
                item_type: pioneer_protocol::TurnItemType::DynamicToolCall,
                status: pioneer_protocol::ToolCallStatus::Completed,
                success: Some(true),
                error_class: None,
                retry_error_class: None,
                metadata: pioneer_protocol::ToolMetadata::from_json(
                    serde_json::json!({"path": "/tmp/input.md"}),
                ),
            });

        let rendered =
            render_execution_continuation_runtime_facts(&ExecutionContinuationRuntimeFactsInput {
                checkpoint: &checkpoint,
                prior_visible_assistant_text: None,
            });

        insta::assert_snapshot!(rendered, @r###"
Checkpoint: schema_version=1, workspace_id=ws_snapshot, thread_id=thr_snapshot, turn_id=turn_snapshot
Original request preview: Create the proposal
Original attachment kinds: local_file
Completed window: index=2, agent_rounds=5, tool_calls=6, provider_tokens=unknown
Window exhaustion reason: max_agent_rounds_per_window
Observed exhausted budget: limit=5, observed=5
Tool summary: requested=6, executed=5, succeeded=4, failed=1, in_progress=0, unexecuted=1
Tool detail list was truncated to 2 item(s).
Tool detail: item_id=tool_snapshot, tool=read_file, status=completed, success=true, metadata={"path":"/tmp/input.md"}
Strict unresolved obligations: none reported by runtime validators.
"###);
    }
}
