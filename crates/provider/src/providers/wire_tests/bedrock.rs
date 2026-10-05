use super::*;
use crate::attachments::{prepare_messages_for_provider, prepare_messages_for_provider_model};
use crate::traits::Provider;
use crate::types::{AttachmentDataSource, ChatMessage, MessageAttachment, MessageContentPart};

fn request(messages: Vec<ChatMessage>) -> ChatRequest {
    ChatRequest {
        model: "anthropic.claude-sonnet-4-5".into(),
        messages,
        temperature: None,
        max_tokens: Some(128),
        tools: None,
        tool_choice: None,
        parallel_tool_calls: None,
        reasoning: None,
        compiled_prompt: None,
    }
}

#[test]
fn canonical_tool_rounds_parse_and_replay_native_unions() {
    let provider = BedrockProvider::new("fixture", "fixture", "us-east-1");
    let mut history = vec![
        ChatMessage::system("Use tools"),
        ChatMessage::user("Forecast and time"),
    ];
    for (round, fixture) in [
        include_str!("../../../tests/fixtures/wire/bedrock-round-1.json"),
        include_str!("../../../tests/fixtures/wire/bedrock-round-2.json"),
    ]
    .iter()
    .enumerate()
    {
        // The stream entry point also uses chat/Converse and this normalization.
        let response =
            BedrockProvider::parse_response(serde_json::from_str(fixture).unwrap()).unwrap();
        assert_eq!(response.tool_calls.len(), 1);
        let call = &response.tool_calls[0];
        assert_eq!(call.id, ["forecast_1", "clock_2"][round]);
        assert_eq!(call.name, ["forecast", "clock"][round]);
        let args: serde_json::Value = serde_json::from_str(&call.arguments).unwrap();
        if round == 0 {
            assert_eq!(args["location"]["city"], "Moscow");
            assert_eq!(args["days"], 2);
            assert_eq!(args["metric"], true);
        } else {
            assert_eq!(args, serde_json::json!({}));
        }
        history.push(ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            response.reasoning_content,
            response.tool_calls.clone(),
            response.provider_replay_state.map(|mut state| {
                state.model = Some("anthropic.claude-sonnet-4-5".into());
                state
            }),
        ));
        history.push(ChatMessage::tool_result(
            &call.id,
            &call.name,
            "{\"ok\":true}",
        ));
        let request = request(history.clone());
        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            &request.model,
            &provider.capabilities(),
            &request.messages,
        )
        .unwrap();
        let wire =
            serde_json::to_value(BedrockProvider::build_request(&request, &prepared).unwrap())
                .unwrap();
        assert_eq!(wire["system"][0]["text"], "Use tools");
        for previous in 0..=round {
            let assistant = &wire["messages"][1 + previous * 2];
            let result = &wire["messages"][2 + previous * 2];
            assert_eq!(assistant["role"], "assistant");
            assert_eq!(result["role"], "user");
            assert_eq!(
                assistant["content"][1]["toolUse"]["toolUseId"],
                ["forecast_1", "clock_2"][previous]
            );
            assert_eq!(
                result["content"][0]["toolResult"]["toolUseId"],
                assistant["content"][1]["toolUse"]["toolUseId"]
            );
            assert!(assistant["content"][1]["toolUse"]["input"].is_object());
            assert_eq!(
                result["content"][0]["toolResult"]["content"][0]["text"],
                "{\"ok\":true}"
            );
            let reasoning = &assistant["content"][0]["reasoningContent"];
            if previous == 0 {
                assert_eq!(reasoning["reasoningText"]["signature"], "opaque-signature");
                assert!(reasoning.get("redactedContent").is_none());
            } else {
                assert_eq!(reasoning["redactedContent"], "AQIDBA==");
                assert!(reasoning.get("reasoningText").is_none());
            }
            for block in assistant["content"].as_array().unwrap() {
                assert_eq!(block.as_object().unwrap().len(), 1);
                assert!(
                    block.get("tool_use").is_none() && block.get("reasoning_content").is_none()
                );
            }
            assert!(result["content"][0].get("tool_result").is_none());
        }
    }
}

#[test]
fn prepared_image_uses_binary_source_in_real_request() {
    let provider = BedrockProvider::new("fixture", "fixture", "us-east-1");
    let message = ChatMessage::user_parts(vec![MessageContentPart::image(MessageAttachment {
        mime_type: "image/png".into(), name: None, size_bytes: None, sha256: None,
        source: AttachmentDataSource::Bytes { base64_data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+VrWQAAAAASUVORK5CYII=".into() }, artifact: None,
    })]);
    let request = request(vec![message]);
    let prepared =
        prepare_messages_for_provider(provider.name(), &provider.capabilities(), &request.messages)
            .unwrap();
    let wire =
        serde_json::to_value(BedrockProvider::build_request(&request, &prepared).unwrap()).unwrap();
    assert_eq!(wire["messages"][0]["content"][0]["image"]["format"], "png");
    let bytes = wire["messages"][0]["content"][0]["image"]["source"]["bytes"]
        .as_str()
        .unwrap();
    assert_eq!(&BASE64.decode(bytes).unwrap()[..8], b"\x89PNG\r\n\x1a\n");
}

#[test]
fn ordinary_and_unknown_response_blocks_parse_independently() {
    let response = BedrockProvider::parse_response(
        serde_json::from_value(serde_json::json!({
            "output": {"message": {"role":"assistant", "content":[
                {"text":"answer"}, {"citationsContent":{"content":[{"text":"citation"}]}}
            ]}}, "stopReason":"end_turn"
        }))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(response.text, "answer");
    assert!(response.tool_calls.is_empty());
    assert!(response.provider_replay_state.is_none());
}
