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
    let mut native_parts = Vec::new();
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
        native_parts.push(state.as_ref().unwrap().payload["parts"].clone());
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
                assistant["parts"], native_parts[previous],
                "replay preserves all native parts in their original order"
            );
            let call_part = assistant["parts"]
                .as_array()
                .unwrap()
                .iter()
                .find(|part| part.get("functionCall").is_some())
                .unwrap();
            assert_eq!(
                call_part["functionCall"]["id"],
                ["forecast_1", "clock_2"][previous]
            );
            assert_eq!(
                result["parts"][0]["functionResponse"]["id"],
                call_part["functionCall"]["id"]
            );
            assert!(call_part["functionCall"]["args"].is_object());
            assert!(result["parts"][0]["functionResponse"]["response"].is_object());
            assert_eq!(
                call_part["thoughtSignature"],
                ["AQIDBA==", "BQYHCA=="][previous]
            );
            assert!(call_part.get("function_call").is_none());
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
fn unsigned_native_tool_rounds_survive_storage_for_older_models_and_aliases() {
    for model in ["gemini-2.0-flash", "gemini-2.5-flash", "deployment-alias"] {
        let provider = GeminiProvider::new("fixture");
        let mut history = vec![ChatMessage::user("Use both tools")];
        let mut originals = Vec::new();
        for round in 0..2 {
            let response: ApiGenerateResponse = serde_json::from_value(serde_json::json!({
                "candidates": [{"content": {"role":"model", "parts":[{
                    "functionCall":{"id":format!("call_{round}"),"name":"clock","args":{}}
                }]}, "finishReason":"STOP"}]
            }))
            .unwrap();
            let calls = GeminiProvider::extract_tool_calls(&response);
            let mut state = GeminiProvider::extract_provider_replay_state(&response).unwrap();
            state.model = Some(model.into());
            originals.push(state.payload["parts"].clone());
            assert_ne!(
                crate::continuation::retention(&state),
                crate::continuation::Retention::Unsupported
            );
            history.push(ChatMessage::assistant_tool_calls_with_provider_state(
                None::<String>,
                None::<String>,
                calls.clone(),
                Some(state),
            ));
            history.push(ChatMessage::tool_result(&calls[0].id, &calls[0].name, "{}"));
            // Exercise the persisted message representation before request projection.
            history = serde_json::from_value(serde_json::to_value(&history).unwrap()).unwrap();
            let mut request = request(history.clone());
            request.model = model.into();
            request.max_tokens = None;
            let prepared = prepare_messages_for_provider_model(
                provider.name(),
                model,
                &provider.capabilities(),
                &request.messages,
            )
            .unwrap();
            let wire = serde_json::to_value(
                GeminiProvider::build_request_from_prepared(&request, &prepared).unwrap(),
            )
            .unwrap();
            for previous in 0..=round {
                assert_eq!(
                    wire["contents"][1 + previous * 2]["parts"],
                    originals[previous]
                );
                assert!(
                    wire["contents"][1 + previous * 2]["parts"][0]
                        .get("thoughtSignature")
                        .is_none()
                );
                assert_eq!(
                    wire["contents"][2 + previous * 2]["parts"][0]["functionResponse"]["id"],
                    format!("call_{previous}")
                );
            }
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
    let parts = &response.candidates[0].content.as_ref().unwrap().parts;
    let mut state = GeminiProvider::extract_provider_replay_state(&response).unwrap();
    assert_eq!(state.payload["schema_version"], 2);
    assert_eq!(state.payload["parts"], serde_json::to_value(parts).unwrap());
    assert_eq!(
        state.payload["parts"][0]["executableCode"]["code"],
        "print(1)"
    );
    assert_eq!(
        parts[2].file_data.as_ref().unwrap().file_uri,
        "https://example.invalid/file"
    );
    assert_eq!(
        parts[3].inline_data.as_ref().unwrap().mime_type,
        "image/png"
    );
    assert_eq!(parts[5].function_response.as_ref().unwrap().name, "clock");
    // Opaque/unknown parts remain canonical data, without acquiring a proven
    // continuation contract merely because the same model is selected.
    state.model = Some("gemini-2.5-flash".into());
    let mut messages = vec![ChatMessage::assistant_tool_calls_with_provider_state(
        Some("answer"),
        None::<String>,
        calls.clone(),
        Some(state.clone()),
    )];
    for call in calls {
        messages.push(ChatMessage::tool_result(call.id, call.name, "{}"));
    }
    let request = request(messages);
    let provider = GeminiProvider::new("fixture");
    let prepared = prepare_messages_for_provider_model(
        provider.name(),
        &request.model,
        &provider.capabilities(),
        &request.messages,
    )
    .unwrap();
    assert!(GeminiProvider::build_request_from_prepared(&request, &prepared).is_err());
    assert_eq!(
        request.messages[0].provider_replay_state.as_ref(),
        Some(&state)
    );
}

#[tokio::test]
async fn prepared_media_uses_canonical_part_and_nested_names() {
    use crate::attachments::regression as fixture;
    use std::sync::Arc;

    let provider = GeminiProvider::new("fixture");
    let state = Arc::new(fixture::state("gemini", "media", serde_json::json!({})));
    let image = fixture::image(image::ImageFormat::Png, 1, 1);
    let pdf = fixture::pdf(1);
    let mut message = ChatMessage::user("Inspect");
    message.content_parts = vec![
        fixture::part(crate::InputContentType::Image, "image/png", &image),
        fixture::part(crate::InputContentType::File, "application/pdf", &pdf),
    ];
    let mut request = request(vec![message]);
    request.model = "media".into();
    let mut external_reference = request.clone();
    external_reference.messages[0].content_parts[1] = MessageContentPart::file(MessageAttachment {
        mime_type: "application/pdf".into(),
        name: None,
        size_bytes: None,
        sha256: None,
        source: AttachmentDataSource::Reference {
            reference: "https://generativelanguage.googleapis.com/v1beta/files/fixture".into(),
        },
        artifact: None,
    });
    assert!(
        fixture::scoped(
            state.clone(),
            provider.prepare_input_budget(external_reference)
        )
        .await
        .is_err()
    );

    let budget = fixture::scoped(state.clone(), provider.prepare_input_budget(request))
        .await
        .unwrap();
    let mut prepared = fixture::scoped(
        state,
        prepare_messages_for_provider_async(
            provider.name(),
            "media",
            &provider.capabilities(),
            &budget.request.messages,
        ),
    )
    .await
    .unwrap();
    assert_eq!(prepared.attachments.len(), 2);
    let wire = serde_json::to_value(
        GeminiProvider::build_request_from_prepared(&budget.request, &prepared).unwrap(),
    )
    .unwrap();
    let parts = wire["contents"][0]["parts"].as_array().unwrap();
    let inline_image = parts
        .iter()
        .find_map(|part| {
            part.get("inlineData")
                .filter(|data| data["mimeType"] == "image/png")
        })
        .unwrap();
    assert_eq!(inline_image["data"], BASE64.encode(&image));
    let inline_pdf = parts
        .iter()
        .find_map(|part| {
            part.get("inlineData")
                .filter(|data| data["mimeType"] == "application/pdf")
        })
        .unwrap();
    assert_eq!(inline_pdf["data"], BASE64.encode(&pdf));

    // Wire-only projection of an internal upload result after validating the
    // original PDF bytes. This does not admit a caller-supplied file reference.
    prepared.attachments[1].source = PreparedAttachmentSource::Reference {
        reference: "https://generativelanguage.googleapis.com/v1beta/files/fixture".into(),
    };
    prepared.attachments[1].transport_plan.kind = crate::AttachmentTransportKind::Upload;
    prepared.attachments[1].bytes = None;
    let wire = serde_json::to_value(
        GeminiProvider::build_request_from_prepared(&budget.request, &prepared).unwrap(),
    )
    .unwrap();
    let parts = wire["contents"][0]["parts"].as_array().unwrap();
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
