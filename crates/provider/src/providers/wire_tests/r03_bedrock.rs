use super::*;
use crate::attachments::prepare_messages_for_provider_model;
use crate::traits::Provider;
use crate::types::ChatMessage;
#[test]
fn typed_result_status_is_cold_stable_and_only_sent_on_supported_model_profiles() {
    use crate::{ToolResultOutcome, ToolResultStatus};
    let provider = BedrockProvider::new("fixture", "fixture", "us-east-1");
    let call = crate::ProviderToolCall {
        id: "call".into(),
        name: "read".into(),
        arguments: "{}".into(),
    };
    for (outcome, expected) in [
        (
            Some(ToolResultOutcome::new(ToolResultStatus::Ok)),
            Some("success"),
        ),
        (
            Some(ToolResultOutcome::new(ToolResultStatus::RecoverableError)),
            Some("error"),
        ),
        (
            Some(ToolResultOutcome::new(ToolResultStatus::PartialSuccess)),
            Some("error"),
        ),
        (None, None),
    ] {
        let mut result = ChatMessage::tool_result(&call.id, &call.name, "{\"success\":true}");
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
                "anthropic.claude-sonnet-4-5",
                &provider.capabilities(),
                &history,
            )
            .unwrap();
            for (model, expected) in [
                ("anthropic.claude-sonnet-4-5", expected),
                ("us.amazon.nova-pro-v1:0", expected),
                ("opaque-deployment", None),
            ] {
                let (messages, _) =
                    BedrockProvider::convert_messages_for_model(&prepared, model).unwrap();
                let wire = serde_json::to_value(messages).unwrap();
                assert_eq!(
                    wire[2]["content"][0]["toolResult"]
                        .get("status")
                        .and_then(|v| v.as_str()),
                    expected
                );
            }
        }
    }
}
