use super::*;
use crate::traits::Provider;
use crate::types::{AttachmentDataSource, ChatMessage, MessageAttachment, MessageContentPart};

fn request(messages: Vec<ChatMessage>) -> ChatRequest {
    ChatRequest {
        model: "gemini-2.5-flash".into(),
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
fn canonical_tool_rounds_parse_and_replay_native_parts() {
    let mut history = vec![
        ChatMessage::system("Use tools"),
        ChatMessage::user("Forecast and time"),
    ];
    for (round, fixture) in [
        include_str!("../../../tests/fixtures/wire/gemini-round-1.json"),
        include_str!("../../../tests/fixtures/wire/gemini-round-2.json"),
    ]
    .iter()
    .enumerate()
    {
        // Both generateContent and each streamGenerateContent data event use this parser.
        let response: ApiGenerateResponse = serde_json::from_str(fixture).unwrap();
        let calls = GeminiProvider::extract_tool_calls(&response);
        assert_eq!(calls.len(), 1);
        let call = &calls[0];
        assert_eq!(call.id, ["forecast_1", "clock_2"][round]);
        assert_eq!(call.name, ["forecast", "clock"][round]);
        let arguments: serde_json::Value = serde_json::from_str(&call.arguments).unwrap();
        if round == 0 {
            assert_eq!(arguments["location"]["city"], "Moscow");
            assert_eq!(arguments["days"], 2);
            assert_eq!(arguments["metric"], true);
            assert_eq!(
                GeminiProvider::extract_reasoning(&response).as_deref(),
                Some("Look up the forecast.")
            );
            assert!(GeminiProvider::extract_text(&response).is_none());
        } else {
            assert_eq!(arguments, serde_json::json!({}));
        }
        let state = GeminiProvider::extract_provider_replay_state(&response).map(|mut state| {
            // The agent binds replay state to the selected model before storing it.
            state.model = Some("gemini-2.5-flash".into());
            state
        });
        history.push(ChatMessage::assistant_tool_calls_with_provider_state(
            None::<String>,
            None::<String>,
            calls.clone(),
            state,
        ));
        history.push(ChatMessage::tool_result(
            &call.id,
            &call.name,
            if round == 0 { "[1,2]" } else { "42" },
        ));
        let request = request(history.clone());
        let provider = GeminiProvider::new("fixture");
        let prepared = prepare_messages_for_provider_model(
            provider.name(),
            &request.model,
            &provider.capabilities(),
            &request.messages,
        )
        .unwrap();
        let wire = serde_json::to_value(
            GeminiProvider::build_request_from_prepared(&request, &prepared).unwrap(),
        )
        .unwrap();
        assert_eq!(wire["systemInstruction"]["parts"][0]["text"], "Use tools");
        for previous in 0..=round {
            let assistant = &wire["contents"][1 + previous * 2];
            let result = &wire["contents"][2 + previous * 2];
            assert_eq!(assistant["role"], "model");
            assert_eq!(result["role"], "user");
            assert_eq!(
                assistant["parts"][0]["functionCall"]["id"],
                ["forecast_1", "clock_2"][previous]
            );
            assert_eq!(
                result["parts"][0]["functionResponse"]["id"],
                assistant["parts"][0]["functionCall"]["id"]
            );
            assert!(assistant["parts"][0]["functionCall"]["args"].is_object());
            assert!(result["parts"][0]["functionResponse"]["response"].is_object());
            assert_eq!(
                assistant["parts"][0]["thoughtSignature"],
                ["AQIDBA==", "BQYHCA=="][previous]
            );
            assert!(assistant["parts"][0].get("function_call").is_none());
        }
        if round == 0 {
            assert_eq!(
                wire["contents"][2]["parts"][0]["functionResponse"]["response"]["content"],
                serde_json::json!([1, 2])
            );
        } else {
            assert_eq!(
                wire["contents"][4]["parts"][0]["functionResponse"]["response"]["content"],
                42
            );
        }
    }
}

#[test]
fn canonical_optional_and_unknown_parts_do_not_hide_calls() {
    let response: ApiGenerateResponse = serde_json::from_value(serde_json::json!({
        "candidates": [{"content": {"role":null, "parts": [
            {"executableCode": {"language":"PYTHON", "code":"print(1)"}},
            {"text": "answer", "thought": null, "thoughtSignature": null},
            {"fileData": {"fileUri":"https://example.invalid/file", "mimeType":null}},
            {"inlineData": {"mimeType":"image/png", "data":"AQID"}},
            {"functionCall": {"name":"clock"}},
            {"functionResponse": {"name":"clock", "response":{}}}
        ]}}]
    }))
    .unwrap();
    assert_eq!(
        GeminiProvider::extract_text(&response).as_deref(),
        Some("answer")
    );
    let calls = GeminiProvider::extract_tool_calls(&response);
    assert_eq!(calls[0].name, "clock");
    assert_eq!(calls[0].arguments, "{}");
    assert!(GeminiProvider::extract_provider_replay_state(&response).is_none());
    let parts = &response.candidates[0].content.as_ref().unwrap().parts;
    assert_eq!(
        parts[2].file_data.as_ref().unwrap().file_uri,
        "https://example.invalid/file"
    );
    assert_eq!(
        parts[3].inline_data.as_ref().unwrap().mime_type,
        "image/png"
    );
    assert_eq!(parts[5].function_response.as_ref().unwrap().name, "clock");
}

#[test]
fn prepared_media_uses_canonical_part_and_nested_names() {
    let provider = GeminiProvider::new("fixture");
    let mut message = ChatMessage::user("Inspect");
    message.content_parts = vec![
        MessageContentPart::image(MessageAttachment {
            mime_type: "image/png".into(), name: None, size_bytes: None, sha256: None,
            source: AttachmentDataSource::Bytes { base64_data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+VrWQAAAAASUVORK5CYII=".into() }, artifact: None,
        }),
        MessageContentPart::file(MessageAttachment {
            mime_type: "application/pdf".into(), name: None, size_bytes: None, sha256: None,
            source: AttachmentDataSource::Reference { reference: "https://generativelanguage.googleapis.com/v1beta/files/fixture".into() }, artifact: None,
        }),
    ];
    let request = request(vec![message]);
    let prepared =
        prepare_messages_for_provider(provider.name(), &provider.capabilities(), &request.messages)
            .unwrap();
    assert_eq!(prepared.attachments.len(), 2);
    let wire = serde_json::to_value(
        GeminiProvider::build_request_from_prepared(&request, &prepared).unwrap(),
    )
    .unwrap();
    let parts = wire["contents"][0]["parts"].as_array().unwrap();
    let inline = parts
        .iter()
        .find_map(|part| part.get("inlineData"))
        .unwrap();
    assert_eq!(inline["mimeType"], "image/png");
    assert!(inline["data"].is_string());
    let file = parts.iter().find_map(|part| part.get("fileData")).unwrap();
    assert_eq!(file["mimeType"], "application/pdf");
    assert_eq!(
        file["fileUri"],
        "https://generativelanguage.googleapis.com/v1beta/files/fixture"
    );
    assert!(
        parts
            .iter()
            .all(|part| part.get("inline_data").is_none() && part.get("file_data").is_none())
    );
}

#[test]
fn invalid_function_arguments_fail_before_transport() {
    for arguments in ["invalid", "[]", "42", "null", "\"text\""] {
        let request = request(vec![ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![ProviderToolCall {
                id: "call_1".into(),
                name: "clock".into(),
                arguments: arguments.into(),
            }],
        )]);
        assert!(GeminiProvider::build_request_result(&request).is_err());
    }
}

#[test]
fn optional_null_args_normalize_to_empty_object_but_wrong_types_fail() {
    let call: ApiFunctionCall =
        serde_json::from_value(serde_json::json!({"name":"clock", "args":null})).unwrap();
    assert_eq!(call.args, serde_json::json!({}));
    for args in [
        serde_json::json!([]),
        serde_json::json!(42),
        serde_json::json!("{}"),
    ] {
        assert!(
            serde_json::from_value::<ApiFunctionCall>(
                serde_json::json!({"name":"clock", "args":args})
            )
            .is_err()
        );
    }
}
