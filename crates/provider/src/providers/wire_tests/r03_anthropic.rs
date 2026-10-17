use super::*;
use crate::attachments::prepare_messages_for_provider_model;
use crate::traits::Provider;
use crate::types::ChatMessage;
#[test]
fn typed_failed_result_survives_cold_history_and_legacy_is_unknown() {
    use crate::{ToolResultOutcome, ToolResultStatus};
    let provider = AnthropicProvider::new("fixture");
    let call = crate::ProviderToolCall {
        id: "call".into(),
        name: "read".into(),
        arguments: "{}".into(),
    };
    for (outcome, expected) in [
        (
            Some(ToolResultOutcome::new(ToolResultStatus::Ok)),
            Some(false),
        ),
        (
            Some(ToolResultOutcome::new(ToolResultStatus::RecoverableError)),
            Some(true),
        ),
        (
            Some(ToolResultOutcome::new(ToolResultStatus::FatalError)),
            Some(true),
        ),
        (
            Some(ToolResultOutcome::new(ToolResultStatus::PartialSuccess)),
            Some(true),
        ),
        (
            Some(ToolResultOutcome {
                schema_version: 2,
                status: ToolResultStatus::Ok,
            }),
            None,
        ),
        (None, None),
    ] {
        // Payload intentionally contradicts typed fact. It must never classify status.
        let mut result = ChatMessage::tool_result(
            &call.id,
            &call.name,
            "{\"success\":true,\"tool_outcome\":\"ok\"}",
        );
        result.tool_outcome = outcome;
        let cold: ChatMessage =
            serde_json::from_value(serde_json::to_value(&result).unwrap()).unwrap();
        for result in [result, cold] {
            let history = vec![
                ChatMessage::user("read"),
                ChatMessage::assistant_tool_calls(None::<String>, vec![call.clone()]),
                result,
            ];
            let prepared = prepare_messages_for_provider_model(
                provider.name(),
                "claude-sonnet-4-5",
                &provider.capabilities(),
                &history,
            )
            .unwrap();
            let (_, messages) = AnthropicProvider::prepare_messages(&prepared).unwrap();
            let wire = serde_json::to_value(messages).unwrap();
            assert_eq!(
                wire[2]["content"][0]
                    .get("is_error")
                    .and_then(|v| v.as_bool()),
                expected
            );
        }
    }
}
