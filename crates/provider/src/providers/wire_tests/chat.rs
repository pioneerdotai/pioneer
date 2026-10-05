use crate::attachments::prepare_messages_for_provider;
use crate::traits::Provider;
use crate::types::ChatMessage;

#[test]
fn canonical_chat_rounds_parse_and_render_native_messages() {
    let provider = wire_provider();
    let mut history = vec![
        ChatMessage::system("Use tools"),
        ChatMessage::user("Forecast and time"),
    ];
    for (round, fixture) in [
        include_str!("../../../tests/fixtures/wire/chat-round-1.json"),
        include_str!("../../../tests/fixtures/wire/chat-round-2.json"),
    ]
    .iter()
    .enumerate()
    {
        let response: ApiChatResponse = serde_json::from_str(fixture).unwrap();
        let message = &response.choices[0].message;
        let calls =
            parse_tool_calls(message.tool_calls.as_ref(), message.function_call.as_ref()).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].id, ["forecast_1", "clock_2"][round]);
        assert_eq!(calls[0].name, ["forecast", "clock"][round]);
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
        let wire =
            serde_json::to_value(WireProvider::convert_messages(&prepared).unwrap()).unwrap();
        assert_eq!(wire[0]["role"], "system");
        for previous in 0..=round {
            let assistant = &wire[2 + previous * 2];
            let result = &wire[3 + previous * 2];
            assert_eq!(assistant["role"], "assistant");
            assert!(assistant["tool_calls"][0]["function"]["arguments"].is_string());
            assert_eq!(
                assistant["tool_calls"][0]["id"],
                ["forecast_1", "clock_2"][previous]
            );
            assert_eq!(result["role"], "tool");
            assert_eq!(result["tool_call_id"], assistant["tool_calls"][0]["id"]);
            assert_eq!(result["content"], "ok");
        }
    }
}

#[test]
fn canonical_stream_parts_match_non_stream_arguments() {
    let response: ApiChatResponse = serde_json::from_str(include_str!(
        "../../../tests/fixtures/wire/chat-round-1.json"
    ))
    .unwrap();
    let message = &response.choices[0].message;
    let expected = parse_tool_calls(message.tool_calls.as_ref(), None).unwrap();
    let event: StreamResponse = serde_json::from_value(serde_json::json!({
        "choices": [{"delta": {"tool_calls": [{"index":0, "id":"forecast_1", "type":"function", "function":{
            "name":"forecast", "arguments":expected[0].arguments
        }}]}, "finish_reason":"tool_calls"}]
    })).unwrap();
    let mut accumulator = StreamToolCallAccumulator::default();
    accumulator.ingest(
        event
            .choices
            .into_iter()
            .next()
            .unwrap()
            .delta
            .tool_calls
            .unwrap(),
    );
    assert_eq!(accumulator.take_tool_calls().unwrap(), expected);
}

#[test]
fn canonical_error_accepts_numeric_and_string_codes() {
    for code in [
        serde_json::json!(429),
        serde_json::json!("rate_limit_exceeded"),
    ] {
        let event: StreamResponse = serde_json::from_value(serde_json::json!({
            "error":{"message":"Request rejected", "type":"rate_limit_error", "code":code}
        }))
        .unwrap();
        let description = event.error.unwrap().description();
        assert!(description.contains("Request rejected"));
        assert!(description.contains("rate_limit_error"));
        assert!(description.contains("code="));
    }
}
