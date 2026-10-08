use super::*;
use crate::attachments::{prepare_messages_for_provider, prepare_messages_for_provider_model};
use crate::traits::Provider;
use crate::types::ChatMessage;

#[test]
fn canonical_tool_rounds_keep_native_block_ids_and_object_inputs() {
    let provider = AnthropicProvider::new("fixture");
    let mut history = vec![
        ChatMessage::system("Use tools"),
        ChatMessage::user("Forecast and time"),
    ];
    for (round, fixture) in [
        r#"{"content":[{"type":"tool_use","id":"forecast_1","name":"forecast","input":{"location":{"city":"Moscow"},"days":2,"metric":true}}],"stop_reason":"tool_use"}"#,
        r#"{"content":[{"type":"tool_use","id":"clock_2","name":"clock","input":{}}],"stop_reason":"tool_use"}"#,
    ].iter().enumerate() {
        let response = AnthropicProvider::parse_response(serde_json::from_str(fixture).unwrap()).unwrap();
        assert_eq!(response.termination, ProviderTermination::ToolCalls);
        assert_eq!(response.tool_calls.len(), 1);
        let call = &response.tool_calls[0];
        assert_eq!(call.id, ["forecast_1", "clock_2"][round]);
        assert_eq!(call.name, ["forecast", "clock"][round]);
        let input: serde_json::Value = serde_json::from_str(&call.arguments).unwrap();
        if round == 0 {
            assert_eq!(input["location"]["city"], "Moscow");
            assert_eq!(input["days"], 2);
            assert_eq!(input["metric"], true);
        } else { assert_eq!(input, serde_json::json!({})); }
        history.push(ChatMessage::assistant_tool_calls(None::<String>, response.tool_calls.clone()));
        history.push(ChatMessage::tool_result(&call.id, &call.name, "ok"));
        let prepared = prepare_messages_for_provider_model(provider.name(), "claude-sonnet-4-5", &provider.capabilities(), &history).unwrap();
        let (system, messages) = AnthropicProvider::prepare_messages(&prepared).unwrap();
        let wire = serde_json::to_value(ApiChatRequest {
            generation: Default::default(),
            model: "claude-sonnet-4-5".into(), messages, max_tokens: 128, temperature: None,
            system, tools: None, tool_choice: None, output_config: None, stream: false,
            cache_control: None,
        }).unwrap();
        assert_eq!(wire["system"], "Use tools");
        for previous in 0..=round {
            let assistant = &wire["messages"][1 + previous * 2];
            let result = &wire["messages"][2 + previous * 2];
            assert_eq!(assistant["role"], "assistant");
            assert_eq!(assistant["content"][0]["type"], "tool_use");
            assert_eq!(assistant["content"][0]["id"], ["forecast_1", "clock_2"][previous]);
            assert!(assistant["content"][0]["input"].is_object());
            if previous == round { assert_eq!(assistant["content"][0]["input"], input); }
            assert_eq!(result["role"], "user");
            assert_eq!(result["content"][0]["type"], "tool_result");
            assert_eq!(result["content"][0]["tool_use_id"], assistant["content"][0]["id"]);
        }
    }
}

#[test]
fn native_replay_keeps_two_parallel_tool_rounds_separate_after_storage() {
    let provider = AnthropicProvider::new("fixture");
    let model = "claude-sonnet-4-5";
    let mut history = vec![ChatMessage::user("inspect two things")];
    for round in 0..2 {
        let blocks: Vec<_> = (0..2)
            .map(|index| {
                serde_json::json!({
                    "type":"tool_use", "id":format!("call_{round}_{index}"),
                    "name":"inspect", "input":{"round":round, "index":index}
                })
            })
            .collect();
        let response = AnthropicProvider::parse_response(
            serde_json::from_value(serde_json::json!({
                "content":blocks, "stop_reason":"tool_use"
            }))
            .unwrap(),
        )
        .unwrap();
        let calls = response.tool_calls.clone();
        let mut state = response.provider_replay_state.unwrap();
        state.model = Some(model.into());
        let assistant = ChatMessage::assistant_tool_calls_with_provider_state(
            Some(response.text),
            response.reasoning_content,
            calls.clone(),
            Some(state),
        );
        let assistant: ChatMessage =
            serde_json::from_value(serde_json::to_value(assistant).unwrap()).unwrap();
        history.push(assistant);
        history.extend(
            calls
                .iter()
                .map(|call| ChatMessage::tool_result(&call.id, &call.name, "ok")),
        );
    }
    let prepared = prepare_messages_for_provider_model(
        provider.name(),
        model,
        &provider.capabilities(),
        &history,
    )
    .unwrap();
    let (_, messages) = AnthropicProvider::prepare_messages(&prepared).unwrap();
    let wire = serde_json::to_value(messages).unwrap();
    assert_eq!(wire.as_array().unwrap().len(), 5);
    for round in 0..2 {
        let assistant = &wire[1 + 2 * round];
        let results = &wire[2 + 2 * round];
        assert_eq!(assistant["role"], "assistant");
        assert_eq!(results["role"], "user");
        assert_eq!(assistant["content"].as_array().unwrap().len(), 2);
        assert_eq!(results["content"].as_array().unwrap().len(), 2);
        for index in 0..2 {
            assert_eq!(assistant["content"][index]["type"], "tool_use");
            assert_eq!(results["content"][index]["type"], "tool_result");
            assert_eq!(
                results["content"][index]["tool_use_id"],
                assistant["content"][index]["id"]
            );
        }
    }
}

#[test]
fn canonical_stream_start_and_json_delta_use_same_tool_schema() {
    let event: StreamEvent = serde_json::from_str(r#"{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"forecast_1","name":"forecast","input":{}}}"#).unwrap();
    let block = event.content_block.unwrap();
    assert_eq!(block.block_type, "tool_use");
    assert_eq!(block.id.as_deref(), Some("forecast_1"));
    assert_eq!(block.name.as_deref(), Some("forecast"));
    let event: StreamEvent = serde_json::from_str(r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"days\":2}"}}"#).unwrap();
    let args: serde_json::Value =
        serde_json::from_str(event.delta.unwrap().partial_json.as_deref().unwrap()).unwrap();
    assert_eq!(args["days"], 2);
}

#[test]
fn ordinary_text_reasoning_usage_and_replay_keep_existing_normalization() {
    let response = AnthropicProvider::parse_response(serde_json::from_value(serde_json::json!({
        "content":[
            {"type":"thinking","thinking":"consider", "signature":"opaque"},
            {"type":"redacted_thinking","data":"AQIDBA=="},
            {"type":"text","text":"answer"},
            {"type":"future_block"}
        ], "stop_reason":"end_turn",
        "usage":{"input_tokens":10,"output_tokens":2,"cache_creation_input_tokens":3,"cache_read_input_tokens":4}
    })).unwrap()).unwrap();
    assert_eq!(response.text, "answer");
    assert_eq!(response.termination, ProviderTermination::Complete);
    assert!(response.tool_calls.is_empty());
    assert_eq!(response.reasoning_content.as_deref(), Some("consider"));
    let usage = response.usage.unwrap();
    assert_eq!(usage.input_tokens, Some(17));
    assert_eq!(usage.output_tokens, Some(2));
    let mut replay = response.provider_replay_state.unwrap();
    assert_eq!(replay.provider, "anthropic");
    assert_eq!(replay.payload["blocks"][0]["signature"], "opaque");
    assert_eq!(replay.payload["blocks"][1]["data"], "AQIDBA==");
    replay.model = Some("claude-sonnet-4-5".into());
    let provider = AnthropicProvider::new("fixture");
    let mut assistant = ChatMessage::assistant("answer");
    assistant.reasoning_content = Some("consider".into());
    assistant.provider_replay_state = Some(replay);
    let history = vec![ChatMessage::user("question"), assistant];
    let prepared = prepare_messages_for_provider_model(
        provider.name(),
        "claude-sonnet-4-5",
        &provider.capabilities(),
        &history,
    )
    .unwrap();
    let (_, messages) = AnthropicProvider::prepare_messages(&prepared).unwrap();
    let wire = serde_json::to_value(messages).unwrap();
    assert_eq!(wire[1]["content"][0]["signature"], "opaque");
    assert_eq!(wire[1]["content"][1]["data"], "AQIDBA==");
    assert_eq!(wire[1]["content"][2]["text"], "answer");
}

#[test]
fn invalid_outgoing_inputs_fail_on_prepared_history_without_payload_in_diagnostic() {
    let provider = AnthropicProvider::new("fixture");
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
        let error = AnthropicProvider::prepare_messages(&prepared)
            .unwrap_err()
            .to_string();
        assert!(error.contains("JSON"));
        assert!(!error.contains("secret"));
    }
}

#[test]
fn invalid_ordinary_inputs_and_missing_input_fail_in_production_normalization() {
    for input in [
        serde_json::json!([]),
        serde_json::json!(42),
        serde_json::json!(true),
        serde_json::Value::Null,
        serde_json::json!("secret"),
    ] {
        let raw = serde_json::json!({"content":[{"type":"tool_use", "id":"call_1", "name":"clock", "input":input}],"stop_reason":"tool_use"});
        let error = AnthropicProvider::parse_response(serde_json::from_value(raw).unwrap())
            .unwrap_err()
            .to_string();
        assert_eq!(error, "Anthropic tool_use input must be a JSON object");
    }
    let missing = r#"{"content":[{"type":"text","text":"do not hide invalid call"},{"type":"tool_use","id":"call_1","name":"clock"}]}"#;
    assert!(AnthropicProvider::parse_response(serde_json::from_str(missing).unwrap()).is_err());
    let malformed =
        r#"{"content":[{"type":"tool_use","id":"call_1","name":"clock","input":{secret}]}"#;
    let response = serde_json::from_str::<ApiChatResponse>(malformed)
        .map_err(anyhow::Error::from)
        .and_then(AnthropicProvider::parse_response);
    assert!(response.is_err());
}

#[test]
fn completed_stream_arguments_follow_object_contract_without_validating_fragments() {
    for raw in [
        "{}",
        r#"{"nested":{"days":2,"metric":true,"items":[null,"text"]}}"#,
    ] {
        let pending = PendingToolUse {
            id: "call_1".into(),
            name: "clock".into(),
            arguments: raw.into(),
            has_partial_json: true,
        };
        let call = pending.finalize().unwrap();
        assert_eq!(call.id, "call_1");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&call.arguments).unwrap(),
            serde_json::from_str::<serde_json::Value>(raw).unwrap()
        );
    }
    for raw in ["{secret", "[]", "42", "true", "null", "\"secret\""] {
        let error = PendingToolUse {
            id: "call_1".into(),
            name: "clock".into(),
            arguments: raw.into(),
            has_partial_json: true,
        }
        .finalize()
        .unwrap_err()
        .to_string();
        assert!(error.contains("JSON"));
        assert!(!error.contains("secret"));
    }
    // A temporary fragment is accepted by the event schema; only the final buffer is validated.
    let event: StreamEvent = serde_json::from_str(r#"{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"nested\":"}}"#).unwrap();
    assert_eq!(
        event.delta.unwrap().partial_json.as_deref(),
        Some("{\"nested\":")
    );
}

#[test]
fn replay_tool_use_inputs_use_the_same_object_guard_on_outgoing_preparation() {
    let provider = AnthropicProvider::new("fixture");
    for input in [
        serde_json::json!({"nested":{"values":[null,true,2,"text"]}}),
        serde_json::json!([]),
        serde_json::json!(42),
        serde_json::Value::Null,
        serde_json::json!("secret"),
    ] {
        let mut assistant = ChatMessage::assistant("");
        assistant.provider_replay_state = Some(ProviderReplayState::for_model(
            "anthropic",
            "claude-sonnet-4-5",
            serde_json::json!({"blocks":[{"type":"tool_use", "id":"call_1", "name":"clock", "input":input}]}),
        ));
        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            "claude-sonnet-4-5",
            &provider.capabilities(),
            &[assistant],
        )
        .unwrap();
        let converted = AnthropicProvider::prepare_messages(&prepared);
        if input.is_object() {
            let (_, messages) = converted.unwrap();
            assert_eq!(
                serde_json::to_value(messages).unwrap()[0]["content"][0]["input"],
                input
            );
        } else {
            let error = converted.unwrap_err().to_string();
            assert!(error.contains("Anthropic tool_use input must be a JSON object"));
            assert!(!error.contains("secret"));
        }
    }
}
