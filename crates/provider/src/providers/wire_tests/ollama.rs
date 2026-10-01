use super::*;
use crate::attachments::prepare_messages_for_provider;
use crate::traits::Provider;
use crate::types::ChatMessage;

#[test]
fn canonical_tool_rounds_keep_object_arguments_and_result_names() {
    let provider = OllamaProvider::new();
    let mut history = vec![
        ChatMessage::system("Use tools"),
        ChatMessage::user("Forecast and time"),
    ];
    for (round, fixture) in [
        include_str!("../../../tests/fixtures/wire/ollama-round-1.json"),
        include_str!("../../../tests/fixtures/wire/ollama-round-2.json"),
    ]
    .iter()
    .enumerate()
    {
        let response: OllamaChatResponse = serde_json::from_str(fixture).unwrap();
        let stream: OllamaStreamChunk = serde_json::from_str(fixture).unwrap();
        let calls = OllamaProvider::convert_tool_calls(response.message.tool_calls.unwrap());
        assert_eq!(
            calls,
            OllamaProvider::convert_tool_calls(stream.message.tool_calls.unwrap())
        );
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
            assert_eq!(wire["messages"][3 + previous * 2]["role"], "tool");
            assert_eq!(
                wire["messages"][3 + previous * 2]["tool_name"],
                ["forecast", "clock"][previous]
            );
            assert_eq!(wire["messages"][3 + previous * 2]["content"], "ok");
        }
        assert!(wire["messages"][0].get("tool_name").is_none());
    }
}
