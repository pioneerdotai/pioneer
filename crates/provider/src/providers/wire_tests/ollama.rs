use super::*;
use crate::attachments::prepare_messages_for_provider;
use crate::traits::Provider;
use crate::types::ChatMessage;

#[test]
fn canonical_tool_rounds_keep_object_arguments_and_result_names() {
    tool_rounds(false);
}

#[test]
fn explicit_native_ids_survive_two_rounds_through_preparation_and_converter() {
    tool_rounds(true);
}

fn tool_rounds(native_ids: bool) {
    let provider = OllamaProvider::new();
    let mut history = vec![
        ChatMessage::system("Use tools"),
        ChatMessage::user("Forecast and time"),
    ];
    let fixtures = if native_ids {
        [
            include_str!("../../../tests/fixtures/wire/ollama-id-round-1.json"),
            include_str!("../../../tests/fixtures/wire/ollama-id-round-2.json"),
        ]
    } else {
        [
            include_str!("../../../tests/fixtures/wire/ollama-round-1.json"),
            include_str!("../../../tests/fixtures/wire/ollama-round-2.json"),
        ]
    };
    for (round, fixture) in fixtures.iter().enumerate() {
        let response: OllamaChatResponse = serde_json::from_str(fixture).unwrap();
        let stream: OllamaStreamChunk = serde_json::from_str(fixture).unwrap();
        let calls = OllamaProvider::convert_tool_calls(response.message.tool_calls.unwrap());
        assert_eq!(
            calls,
            OllamaProvider::convert_tool_calls(stream.message.tool_calls.unwrap())
        );
        assert_eq!(calls[0].name, ["forecast", "clock"][round]);
        assert_eq!(
            calls[0].id,
            if native_ids {
                ["forecast_native_1", "clock_native_2"][round]
            } else {
                "call_1"
            }
        );
        let args: serde_json::Value = serde_json::from_str(&calls[0].arguments).unwrap();
        if round == 0 {
            assert_eq!(args["location"]["city"], "Moscow");
            assert_eq!(args["days"], 2);
            assert_eq!(args["metric"], true);
        } else {
            assert_eq!(args, serde_json::json!({}));
        }
        history.push(ChatMessage::assistant_tool_calls(
            None::<String>,
            calls.clone(),
        ));
        history.push(ChatMessage::tool_result(&calls[0].id, &calls[0].name, "ok"));
        let prepared =
            prepare_messages_for_provider(provider.name(), &provider.capabilities(), &history)
                .unwrap();
        let wire = serde_json::to_value(OllamaChatRequest {
            model: "qwen3".into(),
            messages: OllamaProvider::convert_messages(&prepared).unwrap(),
            stream: false,
            tools: None,
            options: None,
        })
        .unwrap();
        for previous in 0..=round {
            assert_eq!(wire["messages"][2 + previous * 2]["role"], "assistant");
            assert!(
                wire["messages"][2 + previous * 2]["tool_calls"][0]["function"]["arguments"]
                    .is_object()
            );
            if previous == round {
                assert_eq!(
                    wire["messages"][2 + previous * 2]["tool_calls"][0]["function"]["arguments"],
                    args
                );
            }
            assert_eq!(wire["messages"][3 + previous * 2]["role"], "tool");
            assert_eq!(
                wire["messages"][3 + previous * 2]["tool_name"],
                ["forecast", "clock"][previous]
            );
            assert_eq!(wire["messages"][3 + previous * 2]["content"], "ok");
            let expected_id = if native_ids {
                ["forecast_native_1", "clock_native_2"][previous]
            } else {
                "call_1"
            };
            assert_eq!(
                wire["messages"][2 + previous * 2]["tool_calls"][0]["id"],
                expected_id
            );
            assert_eq!(
                wire["messages"][3 + previous * 2]["tool_call_id"],
                expected_id
            );
        }
        for message in wire["messages"].as_array().unwrap() {
            if message["role"] != "tool" {
                assert!(message.get("tool_name").is_none());
                assert!(message.get("tool_call_id").is_none());
            }
        }
    }
}

#[test]
fn invalid_arguments_fail_on_actual_prepared_outgoing_converter() {
    let provider = OllamaProvider::new();
    for arguments in ["{secret", "[]", "42", "true", "null", "\"secret\""] {
        let history = vec![ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: "call_1".into(),
                name: "clock".into(),
                arguments: arguments.into(),
            }],
        )];
        let prepared =
            prepare_messages_for_provider(provider.name(), &provider.capabilities(), &history)
                .unwrap();
        let error = OllamaProvider::convert_messages(&prepared)
            .unwrap_err()
            .to_string();
        assert!(error.contains("JSON"));
        assert!(!error.contains("secret"));
    }
}

#[test]
fn ordinary_and_ndjson_reject_invalid_missing_or_null_arguments_without_defaults() {
    for arguments in [
        serde_json::json!([]),
        serde_json::json!(42),
        serde_json::json!(true),
        serde_json::Value::Null,
        serde_json::json!("secret"),
    ] {
        let fixture = serde_json::json!({"message":{"tool_calls":[{"id":"native_1", "function":{"name":"clock", "arguments":arguments}}]}, "done":true}).to_string();
        let ordinary = serde_json::from_str::<OllamaChatResponse>(&fixture).map(|response| {
            OllamaProvider::convert_tool_calls(response.message.tool_calls.unwrap())
        });
        let stream = serde_json::from_str::<OllamaStreamChunk>(&fixture).map(|response| {
            OllamaProvider::convert_tool_calls(response.message.tool_calls.unwrap())
        });
        assert!(
            ordinary
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("Ollama function arguments must be a JSON object")
        );
        assert!(
            stream
                .as_ref()
                .unwrap_err()
                .to_string()
                .contains("Ollama function arguments must be a JSON object")
        );
        assert!(!ordinary.unwrap_err().to_string().contains("secret"));
    }
    for raw in [
        r#"{"message":{"tool_calls":[{"function":{"name":"clock"}}]}}"#,
        r#"{"message":{"tool_calls":[{"function":{"name":"clock","arguments":{secret}}}]}}"#,
    ] {
        assert!(
            serde_json::from_str::<OllamaChatResponse>(raw)
                .map(|response| OllamaProvider::convert_tool_calls(
                    response.message.tool_calls.unwrap()
                ))
                .is_err()
        );
        assert!(
            serde_json::from_str::<OllamaStreamChunk>(raw)
                .map(|response| OllamaProvider::convert_tool_calls(
                    response.message.tool_calls.unwrap()
                ))
                .is_err()
        );
    }
}

#[test]
fn result_metadata_is_optional_and_confined_to_tool_role() {
    let provider = OllamaProvider::new();
    let mut prepared = prepare_messages_for_provider(
        provider.name(),
        &provider.capabilities(),
        &[
            ChatMessage::system("system"),
            ChatMessage::user("user"),
            ChatMessage::assistant("answer"),
            ChatMessage::tool_result("native_1", "clock", "ok"),
        ],
    )
    .unwrap();
    for message in &mut prepared.messages[..3] {
        message.name = Some("irrelevant".into());
        message.tool_call_id = Some("irrelevant".into());
    }
    prepared.messages[3].tool_call_id = None;
    let wire = serde_json::to_value(OllamaProvider::convert_messages(&prepared).unwrap()).unwrap();
    for message in &wire.as_array().unwrap()[..3] {
        assert!(message.get("tool_call_id").is_none());
        assert!(message.get("tool_name").is_none());
    }
    assert!(wire[3].get("tool_call_id").is_none());
    assert_eq!(wire[3]["tool_name"], "clock");
    assert_eq!(wire[3]["content"], "ok");
}
