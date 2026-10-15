use super::*;

#[test]
fn canonical_chat_rounds_keep_nested_arguments_and_ids_for_each_profile() {
    // This covers the actual shared builder for each registered compatible profile.
    // Acceptance by each vendor/model still needs its own contract/recorded response.
    for definition in crate::definition::provider_definitions().filter(|definition| {
        definition.retirement_reason().is_none()
            && !matches!(
                definition.name,
                "openai"
                    | "anthropic"
                    | "openrouter"
                    | "deepseek"
                    | "gemini"
                    | "ollama"
                    | "telnyx"
                    | "copilot"
                    | "glm"
                    | "zai"
                    | "glm-coding"
                    | "zai-coding"
                    | "local"
                    | "bedrock"
                    | "azure-openai"
            )
    }) {
        let provider = OpenAiCompatibleProvider::new(
            definition.name,
            definition.default_base_url.unwrap(),
            "fixture",
            AuthStyle::Bearer,
        );
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
                parse_tool_calls(message.tool_calls.as_ref(), message.function_call.as_ref())
                    .unwrap();
            assert_eq!(calls.len(), 1, "{}", definition.name);
            assert_eq!(calls[0].id, ["forecast_1", "clock_2"][round]);
            assert_eq!(calls[0].name, ["forecast", "clock"][round]);
            let arguments: serde_json::Value = serde_json::from_str(&calls[0].arguments).unwrap();
            if round == 0 {
                assert_eq!(arguments["location"]["city"], "Moscow");
                assert_eq!(arguments["days"], 2);
                assert_eq!(arguments["metric"], true);
            } else {
                assert_eq!(arguments, serde_json::json!({}));
            }
            history.push(ChatMessage::assistant_tool_calls(
                None::<String>,
                calls.clone(),
            ));
            history.push(ChatMessage::tool_result(&calls[0].id, &calls[0].name, "ok"));
            let request = ChatRequest {
                model: "fixture-model".into(),
                messages: history.clone(),
                temperature: None,
                max_tokens: Some(128),
                tools: None,
                tool_choice: None,
                parallel_tool_calls: None,
                reasoning: None,
                compiled_prompt: None,
            };
            for stream in [false, true] {
                // An unknown model keeps the ordinary cap and server defaults.
                // SiliconFlow's separate thinking reserve requires catalog
                // evidence, covered by the model-aware generation regressions.
                let wire = serde_json::to_value(
                    provider
                        .build_chat_request(request.clone(), stream)
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(wire["stream"], stream);
                assert_eq!(wire["max_tokens"], 128, "{}", definition.name);
                assert!(wire.get("thinking_budget").is_none());
                assert!(wire.get("temperature").is_none());
                assert_eq!(wire["messages"][0]["role"], "system");
                for previous in 0..=round {
                    let call = &wire["messages"][2 + previous * 2]["tool_calls"][0];
                    let result = &wire["messages"][3 + previous * 2];
                    assert_eq!(call["type"], "function");
                    assert!(call["function"]["arguments"].is_string());
                    let original_id = ["forecast_1", "clock_2"][previous];
                    if definition.name == "mistral" {
                        let wire_id = call["id"].as_str().unwrap();
                        assert_eq!(wire_id.len(), 9);
                        assert!(wire_id.bytes().all(|b| b.is_ascii_alphanumeric()));
                        assert_ne!(wire_id, original_id);
                    } else {
                        assert_eq!(call["id"], original_id);
                    }
                    assert_eq!(result["role"], "tool");
                    assert_eq!(result["tool_call_id"], call["id"]);
                }
            }
            assert_eq!(request.messages, history, "canonical IDs must be unchanged");
        }
    }
}

#[test]
fn canonical_chat_delta_agrees_with_ordinary_tool_call() {
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
