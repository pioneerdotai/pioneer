use super::*;
use crate::attachments::prepare_messages_for_provider;
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
        let response: ApiChatResponse = serde_json::from_str(fixture).unwrap();
        let block = &response.content[0];
        assert_eq!(block.block_type, "tool_use");
        let input = block.input.as_ref().unwrap();
        if round == 0 {
            assert_eq!(input["location"]["city"], "Moscow");
            assert_eq!(input["days"], 2);
            assert_eq!(input["metric"], true);
        } else { assert_eq!(input, &serde_json::json!({})); }
        let call = ProviderToolCall {
            id: block.id.clone().unwrap(), name: block.name.clone().unwrap(),
            arguments: serde_json::to_string(input).unwrap(),
        };
        history.push(ChatMessage::assistant_tool_calls(None::<String>, vec![call.clone()]));
        history.push(ChatMessage::tool_result(&call.id, &call.name, "ok"));
        let prepared = prepare_messages_for_provider(provider.name(), &provider.capabilities(), &history).unwrap();
        let (system, messages) = AnthropicProvider::prepare_messages(&prepared).unwrap();
        let wire = serde_json::to_value(ApiChatRequest {
            model: "claude-sonnet-4-5".into(), messages, max_tokens: 128, temperature: None,
            system, tools: None, tool_choice: None, output_config: None, stream: false,
        }).unwrap();
        assert_eq!(wire["system"], "Use tools");
        for previous in 0..=round {
            let assistant = &wire["messages"][1 + previous * 2];
            let result = &wire["messages"][2 + previous * 2];
            assert_eq!(assistant["role"], "assistant");
            assert_eq!(assistant["content"][0]["type"], "tool_use");
            assert_eq!(assistant["content"][0]["id"], ["forecast_1", "clock_2"][previous]);
            assert!(assistant["content"][0]["input"].is_object());
            assert_eq!(result["role"], "user");
            assert_eq!(result["content"][0]["type"], "tool_result");
            assert_eq!(result["content"][0]["tool_use_id"], assistant["content"][0]["id"]);
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
