//! Synthetic Responses fixtures; these are target protocol examples, not captures.
use super::*;
use crate::{
    ChatMessage, ProviderMessageCollector, ProviderReplayState, ProviderTermination, ToolChoice,
    ToolDefinition,
};
use serde_json::json;

fn request() -> ChatRequest {
    ChatRequest {
        model: "gpt-6.1-sol".into(),
        messages: vec![ChatMessage::user("test")],
        temperature: None,
        max_tokens: Some(100),
        tools: Some(vec![ToolDefinition {
            name: "lookup".into(),
            description: "Lookup".into(),
            parameters: json!({"type":"object","properties":{"optional":{"type":"string"}}}),
        }]),
        tool_choice: Some(ToolChoice::Auto),
        parallel_tool_calls: Some(false),
        reasoning: None,
        compiled_prompt: None,
    }
}
fn message(id: &str, phase: Value, text: &str) -> Value {
    json!({"type":"message","id":id,"role":"assistant","phase":phase,"status":"completed","content":[{"type":"output_text","text":text,"annotations":[]}]})
}
fn snapshot(output: Vec<Value>) -> Value {
    json!({"id":"resp_synthetic","status":"completed","model":"gpt-6.1-sol","output":output,"usage":{"input_tokens":100,"output_tokens":20,"input_tokens_details":{"cached_tokens":80},"output_tokens_details":{"reasoning_tokens":12},"total_tokens":120}})
}
fn reducer() -> Reducer {
    reducer_for("openai")
}
fn reducer_for(provider: &str) -> Reducer {
    Reducer::new(
        provider,
        "gpt-6.1-sol",
        "authority",
        Some(false),
        Some("req-synthetic"),
    )
    .with_provisional_messages(provider == "vercel-ai-gateway")
}
fn event(reducer: &mut Reducer, value: Value) -> Vec<StreamChunk> {
    reducer.event(None, &value).unwrap()
}
fn call(id: Value, args: &str) -> Value {
    json!({"type":"function_call","id":"fc_synthetic","call_id":id,"name":"lookup","arguments":args,"status":"completed"})
}

#[test]
fn json_and_terminal_only_sse_have_identical_semantics_and_multiple_parts() {
    let mut first = message("m1", json!("commentary"), "working");
    first["content"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"output_text","text":" now"}));
    let value = snapshot(vec![
        first,
        message("m2", json!("final_answer"), "one"),
        message("m3", Value::Null, "two"),
    ]);
    let mut json = reducer();
    json.snapshot(&value).unwrap();
    let json = json.response().unwrap();
    let mut sse = reducer();
    let chunks = event(
        &mut sse,
        json!({"type":"response.completed","response":value}),
    );
    let mut collector = ProviderMessageCollector::default();
    for chunk in &chunks {
        collector.push(chunk).unwrap();
    }
    collector.finish().unwrap();
    let sse = sse.response().unwrap();
    assert_eq!(json.text, "one\n\ntwo");
    assert_eq!(json.messages[0].text, "working now");
    assert_eq!(json.messages, sse.messages);
    assert_eq!(json.text, sse.text);
    assert_eq!(json.tool_calls, sse.tool_calls);
    assert_eq!(json.usage, sse.usage);
    assert_eq!(json.termination, sse.termination);
    assert_eq!(json.provider_replay_state, sse.provider_replay_state);
    assert_eq!(collector.text(), json.text);
}
#[test]
fn early_commentary_is_visible_before_terminal_and_done_snapshots_do_not_duplicate() {
    let mut r = reducer();
    let chunks = event(
        &mut r,
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"m","type":"message","role":"assistant","phase":"commentary","content":[]}}),
    );
    assert!(chunks.iter().any(|c| c.message.is_some()));
    let chunks = event(
        &mut r,
        json!({"type":"response.output_text.delta","output_index":0,"item_id":"m","content_index":0,"delta":"hello"}),
    );
    assert_eq!(
        chunks.iter().map(|c| c.delta.as_str()).collect::<String>(),
        "hello"
    );
    assert!(!r.is_terminal());
    event(
        &mut r,
        json!({"type":"response.output_text.done","output_index":0,"item_id":"m","content_index":0,"text":"hello"}),
    );
    event(
        &mut r,
        json!({"type":"response.output_item.done","output_index":0,"item":message("m",json!("commentary"),"hello")}),
    );
    let chunks = r
        .snapshot(&snapshot(vec![message("m", json!("commentary"), "hello")]))
        .unwrap();
    assert!(chunks.iter().all(|c| c.delta.is_empty()));
    assert!(r.response().unwrap().text.is_empty());
}
#[test]
fn absent_late_unknown_and_conflicting_phase_are_explicit() {
    for provider in ["openai", "vercel-ai-gateway"] {
        for initial_null in [false, true] {
            for phase in [Value::Null, json!("final_answer"), json!("commentary")] {
                let mut r = reducer_for(provider);
                let mut consumer = ProviderMessageCollector::default();
                let mut added = message("m", Value::Null, "");
                if !initial_null {
                    added.as_object_mut().unwrap().remove("phase");
                }
                let start = event(
                    &mut r,
                    json!({"type":"response.output_item.added","output_index":0,"item":added}),
                );
                let early = event(
                    &mut r,
                    json!({"type":"response.output_text.delta","output_index":0,"item_id":"m","content_index":0,"delta":"held"}),
                );
                if provider == "openai" {
                    assert!(start.is_empty());
                    assert!(early.is_empty());
                } else {
                    assert_eq!(
                        start[0].message.as_ref().unwrap().boundary,
                        crate::ProviderMessageBoundary::StartProvisional
                    );
                    assert_eq!(
                        early.iter().map(|c| c.delta.as_str()).collect::<String>(),
                        "held"
                    );
                    assert!(!r.is_terminal());
                }
                for chunk in start.iter().chain(&early) {
                    consumer.push(chunk).unwrap();
                }
                assert_eq!(consumer.text(), "", "unknown phase is not accepted final");
                let chunks = r
                    .snapshot(&snapshot(vec![message("m", phase.clone(), "held")]))
                    .unwrap();
                assert_eq!(
                    chunks.iter().map(|c| c.delta.as_str()).collect::<String>(),
                    if provider == "openai" { "held" } else { "" }
                );
                for chunk in &chunks {
                    consumer.push(chunk).unwrap();
                }
                consumer.finish().unwrap();
                let response = r.response().unwrap();
                assert_eq!(response.messages.as_slice(), consumer.messages());
                assert_eq!(response.messages[0].text, "held");
                assert_eq!(
                    response.messages[0].phase,
                    match phase.as_str() {
                        Some("commentary") => Some(pioneer_protocol::AgentMessagePhase::Commentary),
                        Some("final_answer") =>
                            Some(pioneer_protocol::AgentMessagePhase::FinalAnswer),
                        _ => None,
                    }
                );
                assert_eq!(
                    response.text,
                    if phase == "commentary" { "" } else { "held" }
                );
                assert_eq!(response.text, consumer.text());
            }
        }
        for changed in [Value::Null, json!("final_answer"), json!("unknown")] {
            let mut r = reducer_for(provider);
            event(
                &mut r,
                json!({"type":"response.output_item.added","output_index":0,"item":message("m",json!("commentary"),"")}),
            );
            assert!(
                r.snapshot(&snapshot(vec![message("m", changed, "changed")]))
                    .is_err()
            );
        }
    }
}

#[test]
fn wire_completed_provisional_item_does_not_block_the_next_live_message() {
    for provider in ["openai", "vercel-ai-gateway"] {
        let mut r = reducer_for(provider);
        let mut consumer = ProviderMessageCollector::default();
        for (index, id, text) in [(0, "one", "first"), (1, "two", "second")] {
            let added = event(
                &mut r,
                json!({"type":"response.output_item.added","output_index":index,"item":message(id,Value::Null,"")}),
            );
            let delta = event(
                &mut r,
                json!({"type":"response.output_text.delta","output_index":index,"item_id":id,"content_index":0,"delta":text}),
            );
            assert_eq!(
                delta
                    .iter()
                    .map(|chunk| chunk.delta.as_str())
                    .collect::<String>(),
                if provider == "openai" { "" } else { text }
            );
            assert!(!r.is_terminal());
            let done = event(
                &mut r,
                json!({"type":"response.output_item.done","output_index":index,"item":message(id,Value::Null,text)}),
            );
            for chunk in added.iter().chain(&delta).chain(&done) {
                consumer.push(chunk).unwrap();
            }
        }
        assert!(consumer.text().is_empty());
        let terminal = r
            .snapshot(&snapshot(vec![
                message("one", json!("commentary"), "first"),
                message("two", Value::Null, "second"),
            ]))
            .unwrap();
        for chunk in &terminal {
            consumer.push(chunk).unwrap();
        }
        consumer.finish().unwrap();
        assert_eq!(consumer.text(), "second");
        assert_eq!(
            consumer
                .messages()
                .iter()
                .map(|message| message.text.as_str())
                .collect::<Vec<_>>(),
            vec!["first", "second"]
        );
        assert_eq!(r.response().unwrap().text, "second");
    }
}

#[test]
fn successful_terminal_is_the_only_executable_call_authority() {
    let mut r = reducer();
    let mut added = call(json!("call_real"), "");
    added["status"] = json!("in_progress");
    assert!(
        event(
            &mut r,
            json!({"type":"response.output_item.added","output_index":0,"item":added})
        )
        .iter()
        .all(|c| c.tool_calls.is_empty())
    );
    assert!(event(&mut r,json!({"type":"response.function_call_arguments.done","output_index":0,"item_id":"fc_synthetic","arguments":"{}"})).iter().all(|c|c.tool_calls.is_empty()));
    assert!(event(&mut r,json!({"type":"response.output_item.done","output_index":0,"item":call(json!("call_real"),"{}")})).iter().all(|c|c.tool_calls.is_empty()));
    let chunks = r
        .snapshot(&snapshot(vec![call(json!("call_real"), "{}")]))
        .unwrap();
    assert_eq!(
        chunks.iter().flat_map(|c| &c.tool_calls).next().unwrap().id,
        "call_real"
    );
    assert_eq!(
        r.response().unwrap().termination,
        ProviderTermination::ToolCalls
    );
}
#[test]
fn invalid_ids_arguments_duplicates_and_parallel_are_rejected() {
    for item in [
        call(Value::Null, "{}"),
        call(json!(""), "{}"),
        call(json!("c"), "[]"),
        call(json!("c"), "{"),
    ] {
        assert!(reducer().snapshot(&snapshot(vec![item])).is_err());
    }
    let mut other = call(json!("same"), "{}");
    other["id"] = json!("fc_other");
    assert!(
        reducer()
            .snapshot(&snapshot(vec![call(json!("same"), "{}"), other.clone()]))
            .is_err()
    );
    other["call_id"] = json!("other");
    assert!(
        reducer()
            .snapshot(&snapshot(vec![call(json!("same"), "{}"), other]))
            .is_err()
    );
    for (field, value) in [
        ("async", json!(true)),
        ("caller", json!({"type":"program","caller_id":"program"})),
        ("namespace", json!("custom")),
    ] {
        let mut item = call(json!("c"), "{}");
        item[field] = value;
        assert!(reducer().snapshot(&snapshot(vec![item])).is_err());
    }
}
#[test]
fn incomplete_truncated_arguments_preserves_outcome_without_calls_or_replay() {
    for (reason, outcome) in [
        ("max_output_tokens", ProviderTermination::Length),
        ("content_filter", ProviderTermination::ContentFiltered),
        ("unknown", ProviderTermination::Unknown("incomplete".into())),
    ] {
        let mut value = snapshot(vec![
            call(json!("c"), "{\"truncated\":"),
            message("m", json!("final_answer"), "partial"),
        ]);
        value["status"] = json!("incomplete");
        value["incomplete_details"] = json!({"reason":reason});
        let mut r = reducer();
        r.snapshot(&value).unwrap();
        let response = r.response().unwrap();
        assert_eq!(response.termination, outcome);
        assert!(response.tool_calls.is_empty());
        assert!(response.provider_replay_state.is_none());
        assert_eq!(response.text, "partial");
    }
}
#[test]
fn refusal_failed_and_top_level_error_are_safe() {
    let value = snapshot(vec![
        json!({"type":"message","id":"m","role":"assistant","content":[{"type":"refusal","refusal":"Cannot help"}]}),
    ]);
    let mut r = reducer();
    r.snapshot(&value).unwrap();
    let response = r.response().unwrap();
    assert_eq!(response.text, "Cannot help");
    assert_eq!(response.termination, ProviderTermination::Safety);
    for value in [
        json!({"type":"error","code":"rate_limit_exceeded","message":"SECRET ciphertext private-host"}),
        json!({"type":"response.failed","response":{"status":"failed","usage":{"input_tokens":7},"error":{"code":"invalid_request_error","message":"SECRET"}}}),
    ] {
        let mut r = reducer();
        let error = r.event(None, &value).unwrap_err();
        assert!(!format!("{error:?} {error}").contains("SECRET"));
        assert!(super::classify_failure(&error).is_some());
        if value["type"] == "response.failed" {
            assert_eq!(r.usage.input_tokens, Some(7));
        }
    }
}
#[test]
fn unknown_content_events_snapshots_and_outer_type_conflicts_fail_closed() {
    assert!(
        reducer()
            .event(
                Some("response.completed"),
                &json!({"type":"response.created","response":{}})
            )
            .is_err()
    );
    for item in [
        json!({"type":"web_search_call","id":"x"}),
        json!({"type":"message","id":"m","role":"assistant","content":[{"type":"unknown","text":"lost"}]}),
    ] {
        assert!(reducer().snapshot(&snapshot(vec![item])).is_err());
    }
    assert!(
        reducer()
            .event(
                None,
                &json!({"type":"response.done","response":snapshot(vec![])})
            )
            .is_err()
    );
    let mut r = reducer();
    event(
        &mut r,
        json!({"type":"response.output_item.added","output_index":0,"item":message("m",json!("final_answer"),"prefix")}),
    );
    assert!(
        r.snapshot(&snapshot(vec![message(
            "m",
            json!("final_answer"),
            "different"
        )]))
        .is_err()
    );
}
#[tokio::test]
async fn truncated_streams_and_every_byte_split_use_existing_sse_framing() {
    for wire in [
        "",
        "data: [DONE]\n\n",
        "data: {\"type\":\"response.created\",\"response\":{}}\n\n",
    ] {
        let stream =
            futures_util::stream::iter(vec![Ok(bytes::Bytes::from(wire.to_owned()))]).boxed();
        let chunks = ResponsesApiCompatibleProvider::decode(stream, reducer())
            .collect::<Vec<_>>()
            .await;
        assert!(chunks.last().unwrap().is_err());
        assert!(!chunks.iter().any(|c| c.as_ref().is_ok_and(|c| c.is_final)));
    }
    let value = json!({"type":"response.completed","response":snapshot(vec![message("m",json!("final_answer"),"Привет")])});
    let wire = format!("\u{feff}: synthetic\r\nevent: response.completed\r\ndata: {value}\r\n\r\n");
    for split in 0..=wire.len() {
        let b = wire.as_bytes();
        let stream = futures_util::stream::iter(vec![
            Ok(bytes::Bytes::copy_from_slice(&b[..split])),
            Ok(bytes::Bytes::copy_from_slice(&b[split..])),
        ])
        .boxed();
        let chunks = ResponsesApiCompatibleProvider::decode(stream, reducer())
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let mut c = ProviderMessageCollector::default();
        for chunk in &chunks {
            c.push(chunk).unwrap();
        }
        c.finish().unwrap();
        assert_eq!(c.text(), "Привет");
    }
}
#[test]
fn replay_retention_authority_and_readable_portability() {
    for (items, retention) in [
        (
            vec![message("m", json!("commentary"), "readable")],
            crate::continuation::Retention::Ordinary,
        ),
        (
            vec![
                json!({"id":"rs","type":"reasoning","summary":[{"type":"summary_text","text":"why"}],"encrypted_content":"SECRET"}),
                message("m", json!("final_answer"), "answer"),
            ],
            crate::continuation::Retention::ActiveTurn,
        ),
    ] {
        let mut r = reducer();
        r.snapshot(&snapshot(items)).unwrap();
        let response = r.response().unwrap();
        let state = response.provider_replay_state.unwrap();
        assert_eq!(crate::continuation::retention(&state), retention);
        assert!(replay::compatible(
            &state,
            "openai",
            "gpt-6.1-sol",
            "authority"
        ));
        assert!(!format!("{state:?}").contains("SECRET"));
        assert!(!replay::compatible(
            &state,
            "openai",
            "gpt-6.1-sol",
            "other"
        ));
        let mut message = ChatMessage::assistant("aggregate");
        message.provider_replay_state = Some(state);
        let portable = crate::history::portable_history_message(&message);
        assert!(!format!("{portable:?}").contains("SECRET"));
        assert!(!portable.content.contains("aggregate"));
    }
}

#[tokio::test]
async fn generic_root_auth_profiles_and_openai_facade_dispatch_real_http() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for profile in 0..3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let root = format!("http://{}/prefix/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (offset, size) = loop {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&bytes[..i]).to_lowercase();
                    let size = head
                        .lines()
                        .find_map(|s| s.strip_prefix("content-length:"))
                        .unwrap()
                        .trim()
                        .parse::<usize>()
                        .unwrap();
                    break (i + 4, size);
                }
            };
            while bytes.len() < offset + size {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
            }
            let headers = String::from_utf8_lossy(&bytes[..offset]).to_lowercase();
            assert!(headers.starts_with("post /prefix/v1/responses "));
            assert!(headers.contains(if profile == 1 {
                "x-api-key: fixture"
            } else {
                "authorization: bearer fixture"
            }));
            let body: Value = serde_json::from_slice(&bytes[offset..offset + size]).unwrap();
            assert_eq!(body["store"], false);
            assert_eq!(body["truncation"], "disabled");
            assert_eq!(body["include"], json!(["reasoning.encrypted_content"]));
            assert_eq!(body["max_output_tokens"], 100);
            assert_eq!(body["tools"][0]["strict"], false);
            assert!(body["tools"][0].get("function").is_none());
            assert!(body.get("previous_response_id").is_none());
            let body = snapshot(vec![message("m", json!("final_answer"), "answer")]).to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nX-Request-Id: req-synthetic\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        });
        let provider: std::sync::Arc<dyn Provider> = if profile == 2 {
            let registry =
                crate::ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
                    |_, _| Ok("fixture".into()),
                    |_, _| Ok(None),
                    move |_, _| Ok(Some(root.clone())),
                    ProviderTimeoutPolicy::default(),
                );
            registry
                .get_or_create_for_workspace("synthetic-workspace", "openai")
                .unwrap()
        } else {
            std::sync::Arc::new(ResponsesApiCompatibleProvider::new(
                "openai",
                root,
                "fixture",
                if profile == 1 {
                    AuthStyle::XApiKey
                } else {
                    AuthStyle::Bearer
                },
            ))
        };
        assert_eq!(
            provider.request_usage_profile(&request()).unwrap().api,
            "responses"
        );
        let response = crate::attachments::runtime::with_async_authority_scope(
            "synthetic".into(),
            provider.chat(request()),
        )
        .await
        .unwrap();
        assert_eq!(response.text, "answer");
        if profile == 2 {
            let usage = response.usage.as_ref().unwrap();
            assert_eq!(usage.api.as_deref(), Some("responses"));
            assert!(usage.route.as_ref().unwrap().ends_with(";path=/responses"));
            assert!(usage.physical_attempt_id.is_some());
        }
        assert_eq!(
            response.usage.unwrap().request_id.as_deref(),
            Some("req-synthetic")
        );
        server.await.unwrap();
    }
}

#[tokio::test]
async fn provisional_delivery_is_explicit_and_independent_of_provider_name() {
    for (name, provisional) in [
        ("neutral-responses", true),
        ("neutral-responses", false),
        ("openai", false),
    ] {
        let events = [
            json!({"type":"response.output_item.added","output_index":0,"item":message("m",Value::Null,"")}),
            json!({"type":"response.output_text.delta","output_index":0,"item_id":"m","content_index":0,"delta":"visible"}),
            json!({"type":"response.completed","response":snapshot(vec![message("m",Value::Null,"visible")])}),
        ];
        let body = events
            .iter()
            .map(|e| format!("data: {e}\n\n"))
            .collect::<String>();
        let (root, server) =
            super::super::usage_tests::fixture_with_headers(body, String::new(), "200 OK").await;
        let provider =
            ResponsesApiCompatibleProvider::new(name, root, "fixture", AuthStyle::Bearer);
        // Leave defaults untouched in the control rows, including OpenAI.
        let provider = if provisional {
            provider.with_provisional_messages(true)
        } else {
            provider
        };
        // Neutral providers have no verified model generation contract; leave
        // generation controls omitted so this fixture exercises delivery only.
        let mut req = request();
        req.max_tokens = None;
        let chunks = crate::attachments::runtime::with_async_authority_scope(
            "delivery-fixture".into(),
            async {
                let stream = provider.stream_chat_with_diagnostics(req).await.unwrap();
                assert_eq!(
                    stream.message_delivery,
                    if provisional {
                        crate::ProviderMessageDelivery::Provisional
                    } else {
                        crate::ProviderMessageDelivery::Incremental
                    }
                );
                stream.stream.collect::<Vec<_>>().await
            },
        )
        .await;
        let chunks = chunks.into_iter().collect::<Result<Vec<_>>>().unwrap();
        let mut collector = ProviderMessageCollector::default();
        let mut unresolved = false;
        let mut provisional_text = String::new();
        for chunk in &chunks {
            if let Some(event) = &chunk.message {
                match event.boundary {
                    crate::ProviderMessageBoundary::StartProvisional => unresolved = true,
                    crate::ProviderMessageBoundary::Resolve => unresolved = false,
                    _ => {}
                }
            }
            if unresolved {
                provisional_text.push_str(&chunk.delta);
            }
            collector.push(chunk).unwrap();
        }
        collector.finish().unwrap();
        assert_eq!(provisional_text, if provisional { "visible" } else { "" });
        assert_eq!(collector.text(), "visible");
        assert_eq!(collector.messages().len(), 1);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn compiled_prompt_tool_controls_and_responses_generation_policy() {
    let state = std::sync::Arc::new(crate::attachments::admission::AdmissionState::for_test(
        std::sync::Arc::new(compatible_catalog(
            "openai",
            "gpt-6.1-sol",
            json!({
                "compat":{"supportsReasoningEffort":true,"supportsTemperature":false},
                "thinkingLevelMap":{"max":"max","off":null}
            }),
        )),
    ));
    crate::attachments::regression::scoped(
        state,
        crate::attachments::runtime::with_async_authority_scope(
            "responses-generation-fixture".into(),
            async {
                let provider = ResponsesApiCompatibleProvider::new(
                    "openai",
                    "https://synthetic.invalid/prefix",
                    "fixture",
                    AuthStyle::Bearer,
                );
                for choice in [
                    ToolChoice::Auto,
                    ToolChoice::None,
                    ToolChoice::Required,
                    ToolChoice::Tool {
                        name: "lookup".into(),
                    },
                ] {
                    let mut req = request();
                    req.tool_choice = Some(choice.clone());
                    req.compiled_prompt = Some(crate::CompiledPromptPayload {
                        stable_system_text: "compiled".into(),
                        dynamic_system_text: String::new(),
                        boundary_marker: String::new(),
                        full_system_text: "compiled".into(),
                    });
                    req.messages
                        .insert(0, ChatMessage::system("must not duplicate"));
                    let prepared = provider
                        .prepare(req, &provider.capabilities())
                        .await
                        .unwrap();
                    let body = provider.render(&prepared, true).unwrap();
                    assert_eq!(body["instructions"], "compiled");
                    assert!(!body["input"].to_string().contains("must not duplicate"));
                    assert_eq!(body["max_output_tokens"], 100);
                    assert_eq!(body["parallel_tool_calls"], json!(false));
                    assert!(body["tools"][0]["parameters"].get("required").is_none());
                    match choice {
                        ToolChoice::Auto => assert_eq!(body["tool_choice"], "auto"),
                        ToolChoice::None => assert_eq!(body["tool_choice"], "none"),
                        ToolChoice::Required => assert_eq!(body["tool_choice"], "required"),
                        ToolChoice::Tool { name } => {
                            assert_eq!(body["tool_choice"], json!({"type":"function","name":name}))
                        }
                    }
                }
                let mut req = request();
                req.temperature = Some(0.7);
                assert!(
                    provider
                        .prepare(req.clone(), &provider.capabilities())
                        .await
                        .is_err()
                );
                req.temperature = None;
                req.reasoning = Some(crate::ReasoningConfig::Disabled);
                assert!(
                    provider
                        .prepare(req.clone(), &provider.capabilities())
                        .await
                        .is_err()
                );
                req.reasoning = Some(crate::ReasoningConfig::Effort(crate::ReasoningEffort::Max));
                let prepared = provider
                    .prepare(req.clone(), &provider.capabilities())
                    .await
                    .unwrap();
                assert_eq!(
                    provider.render(&prepared, false).unwrap()["reasoning"]["effort"],
                    "max"
                );
                assert!(crate::generation::chat_fields("azure-openai", &req).is_err());
                assert!(crate::tools::policy::prepare_request("azure-openai", req).is_err());
                let mut req = request();
                req.tool_choice = Some(ToolChoice::Tool {
                    name: "missing".into(),
                });
                assert!(
                    provider
                        .prepare(req, &provider.capabilities())
                        .await
                        .is_err()
                );
            },
        ),
    )
    .await;
}
#[tokio::test]
async fn tool_media_and_pdf_use_the_owned_prepared_positions_and_no_second_preparation() {
    use crate::attachments::regression as fixture;
    use crate::{InputContentType, InputTypeSupport, ProviderInputCapabilities};
    let capabilities = ProviderInputCapabilities {
        text: true,
        image: InputTypeSupport::data_url_inline_only(),
        file: InputTypeSupport::native_inline_only(),
        audio: InputTypeSupport::disabled(),
        video: InputTypeSupport::disabled(),
    };
    let provider = ResponsesApiCompatibleProvider::new(
        "openai",
        "https://synthetic.invalid",
        "fixture",
        AuthStyle::Bearer,
    )
    .with_input_capabilities(capabilities);
    let (state, req) = fixture::html_preview_fixture("openai", "openai-responses");
    let prepared = fixture::scoped(
        state.clone(),
        provider.prepare(req, &provider.capabilities()),
    )
    .await
    .unwrap();
    let body = provider.render(&prepared, false).unwrap();
    let input = body["input"].as_array().unwrap();
    assert_eq!(input[0]["type"], "function_call");
    assert_eq!(input[1]["type"], "function_call");
    assert_eq!(input[2]["type"], "function_call_output");
    assert_eq!(input[3]["type"], "function_call_output");
    assert_eq!(input[4]["role"], "user");
    assert_eq!(
        input[4]["content"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|p| p["type"] == "input_image")
            .count(),
        6
    );
    let pdf = fixture::pdf(1);
    let req = fixture::request(
        "media",
        vec![fixture::part(
            InputContentType::File,
            "application/pdf",
            &pdf,
        )],
    );
    let state = std::sync::Arc::new(fixture::state("openai", "media", json!({})));
    let mut prepared = fixture::scoped(state, provider.prepare(req, &provider.capabilities()))
        .await
        .unwrap();
    let inline = provider.render(&prepared, false).unwrap();
    assert!(
        inline["input"][0]["content"][1]["file_data"]
            .as_str()
            .unwrap()
            .starts_with("data:application/pdf;base64,")
    );
    prepared.messages.attachments[0].source = crate::PreparedAttachmentSource::Reference {
        reference: "file-owned-after-upload".into(),
    };
    prepared.messages.attachments[0].bytes = None;
    let owned = provider.render(&prepared, false).unwrap();
    assert_eq!(
        owned["input"][0]["content"][1]["file_id"],
        "file-owned-after-upload"
    );
    assert!(owned["input"][0]["content"][1].get("file_data").is_none());
    // The same real prepared request owns bytes, positions and authority; execution never prepares it again.
}
#[tokio::test]
async fn audio_selector_is_stable_for_text_budget_and_continuation_and_regular_models_reject_audio()
{
    let provider = super::super::OpenAiProvider::new("fixture");
    for model in [
        "gpt-audio",
        "gpt-audio-mini-2025-12-15",
        "gpt-4o-audio-preview",
        "gpt-4o-mini-audio-preview-2024-12-17",
    ] {
        let mut req = request();
        req.model = model.into();
        req.tools = None;
        req.tool_choice = None;
        let profile = provider.request_usage_profile(&req).unwrap();
        assert_eq!(profile.api, "chat_completions");
        req.messages.push(ChatMessage::assistant("previous"));
        req.messages.push(ChatMessage::user("continue as text"));
        assert_eq!(provider.request_usage_profile(&req).unwrap(), profile);
        let budget = provider.prepare_input_budget(req.clone()).await.unwrap();
        assert_eq!(
            provider.request_usage_profile(&budget.request).unwrap(),
            profile
        );
    }
    assert!(!super::super::openai::chat_only_audio_model(
        "gpt-audio-unverified"
    ));
    let mut req = request();
    req.model = "gpt-4o".into();
    assert_eq!(
        provider.request_usage_profile(&req).unwrap().api,
        "responses"
    );
    let state = std::sync::Arc::new(crate::attachments::regression::state(
        "openai",
        "media",
        json!({}),
    ));
    req = crate::attachments::regression::request(
        "media",
        vec![crate::attachments::regression::part(
            crate::InputContentType::Audio,
            "audio/wav",
            &crate::attachments::regression::wav(),
        )],
    );
    assert_eq!(
        provider.request_usage_profile(&req).unwrap().api,
        "responses"
    );
    assert!(
        crate::attachments::regression::scoped(state, provider.prepare_input_budget(req))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn completed_authority_model_dialect_switches_are_portable_active_are_closed() {
    let provider = ResponsesApiCompatibleProvider::new(
        "openai",
        "https://synthetic.invalid",
        "fixture",
        AuthStyle::Bearer,
    );
    let mut r = reducer();
    r.snapshot(&snapshot(vec![
        message("m", json!("commentary"), "historical commentary"),
        message("f", json!("final_answer"), "answer"),
    ]))
    .unwrap();
    let response = r.response().unwrap();
    let mut msg = ChatMessage::assistant("common aggregate");
    msg.provider_replay_state = response.provider_replay_state;
    let mut req = request();
    req.messages = vec![msg.clone()];
    assert!(
        provider.project(req.clone()).is_err(),
        "active same-model different-authority replay must fail closed"
    );
    msg.provenance = Some(crate::MessageProvenance {
        logical_turn_id: None,
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        context_thread: None,
        unit_id: "completed-round".into(),
        sources: vec![crate::MessageSourceRef {
            scope: "context:turn".into(),
            id: "request".into(),
            version: "revision:1".into(),
        }],
        source_aliases: vec![],
        ambiguous_input_aliases: vec![],
        complete: true,
        protected_input: false,
        inherited: false,
    });
    req.messages = vec![msg.clone()];
    let projected = provider.project(req.clone()).unwrap();
    assert!(projected.messages[0].provider_replay_state.is_none());
    assert!(
        projected.messages[0]
            .content
            .contains("historical commentary")
    );
    assert!(!projected.messages[0].content.contains("common aggregate"));
    for model in ["gpt-6.1-sol", "other-model"] {
        let mut request = req.clone();
        request.model = model.into();
        assert!(
            provider.project(request).unwrap().messages[0]
                .provider_replay_state
                .is_none()
        );
    }
    let state = msg.provider_replay_state.as_mut().unwrap();
    state.payload["schema_version"] = json!(99);
    assert_eq!(
        crate::continuation::retention(state),
        crate::continuation::Retention::Unsupported
    );
    req.messages = vec![msg];
    assert!(
        provider.project(req).unwrap().messages[0]
            .provider_replay_state
            .is_none()
    );
}
#[test]
fn resource_limits_cover_message_records_snapshots_reasoning_and_calls() {
    assert!(
        reducer()
            .snapshot(&snapshot(
                (0..129)
                    .map(|i| message(&format!("m{i}"), json!("commentary"), ""))
                    .collect()
            ))
            .is_err()
    );
    let mut r = reducer();
    let oversized = "x".repeat(ProviderResponseLimits::default().max_text_bytes + 1);
    assert!(
        r.snapshot(&snapshot(vec![message(
            "m",
            json!("commentary"),
            &oversized
        )]))
        .is_err()
    );
    let mut r = reducer();
    assert!(r.snapshot(&snapshot(vec![json!({"id":"rs","type":"reasoning","summary":[{"type":"summary_text","text":oversized}]})])).is_err());
    let mut r = reducer();
    let output=(0..65).map(|i|json!({"id":format!("fc{i}"),"type":"function_call","call_id":format!("c{i}"),"name":"lookup","arguments":"{}"})).collect();
    assert!(r.snapshot(&snapshot(output)).is_err());
}
#[tokio::test]
async fn stream_drop_aborts_owned_network_future_and_keeps_queue_bounded() {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = Dropped(dropped.clone());
    let stream = futures_util::stream::unfold(guard, |guard| async move {
        futures_util::future::pending::<()>().await;
        Some((Ok(bytes::Bytes::new()), guard))
    })
    .boxed();
    let output = ResponsesApiCompatibleProvider::decode(stream, reducer());
    drop(output);
    for _ in 0..16 {
        tokio::task::yield_now().await;
        if dropped.load(Ordering::SeqCst) {
            break;
        }
    }
    assert!(dropped.load(Ordering::SeqCst));
}

#[tokio::test]
async fn failed_stream_preserves_header_usage_route_and_safe_classification_through_registry() {
    let body = format!(
        "data: {}\n\ndata: {}\n\n",
        json!({"type":"response.created","response":{"id":"resp_synthetic","model":"gpt-6.1-sol","usage":{"input_tokens":12}}}),
        json!({"type":"error","code":"rate_limit_exceeded","message":"SECRET_BODY CIPHERTEXT http://private/credential"})
    );
    let (root, server) = super::super::usage_tests::fixture_with_headers(
        body,
        "X-Request-Id: req_openai_native\r\n".into(),
        "200 OK",
    )
    .await;
    let registry = crate::ProviderRegistry::with_provider(
        "openai",
        std::sync::Arc::new(super::super::OpenAiProvider::with_base_url(
            "synthetic-key",
            root,
        )),
    );
    let provider = registry.get_or_create("openai").unwrap();
    let response = provider
        .stream_chat_with_diagnostics(request())
        .await
        .unwrap();
    assert!(response.diagnostics.request_id().is_some());
    let mut stream = response.stream;
    let error = stream.next().await.unwrap().unwrap_err();
    let facts = crate::usage::error_usage(&error).unwrap();
    assert_eq!(facts.input_tokens, Some(12));
    assert_eq!(facts.output_tokens, None);
    assert_eq!(facts.api.as_deref(), Some("responses"));
    assert_eq!(facts.request_id.as_deref(), Some("req_openai_native"));
    assert_eq!(facts.generation_id.as_deref(), Some("resp_synthetic"));
    assert!(facts.physical_attempt_id.is_some());
    assert!(facts.route.as_ref().unwrap().ends_with(";path=/responses"));
    let failure = provider.classify_failure(&error).unwrap();
    assert_eq!(
        failure.class,
        pioneer_protocol::ProviderFailureClass::RateLimit
    );
    assert_eq!(failure.http_status, None, "native code is not HTTP status");
    let display = format!("{error:#} {error:?} {facts:?}");
    for secret in [
        "SECRET_BODY",
        "CIPHERTEXT",
        "private/credential",
        "synthetic-key",
    ] {
        assert!(!display.contains(secret));
    }
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}
#[test]
fn terminal_only_valid_call_never_uses_optional_item_id_as_call_identity() {
    let mut item = call(json!("real_call"), "{}");
    item.as_object_mut().unwrap().remove("id");
    let mut r = reducer();
    r.snapshot(&snapshot(vec![item])).unwrap();
    assert_eq!(r.response().unwrap().tool_calls[0].id, "real_call");
}

#[test]
fn responses_tool_controls_and_ids_do_not_inherit_a_services_chat_codec() {
    let mut history = vec![
        ChatMessage::assistant_tool_calls(
            None::<String>,
            vec![crate::ProviderToolCall {
                id: "native_call_id".into(),
                name: "lookup".into(),
                arguments: "{}".into(),
            }],
        ),
        ChatMessage::tool_result("native_call_id", "lookup", "ok"),
    ];
    crate::tools::policy::prepare_responses_history("mistral", &mut history).unwrap();
    assert_eq!(
        history[0].tool_calls.as_ref().unwrap()[0].id,
        "native_call_id"
    );
    assert_eq!(history[1].tool_call_id.as_deref(), Some("native_call_id"));
    for name in ["synthetic", "anthropic", "cohere", "bedrock"] {
        let mut req = request();
        req.tool_choice = Some(ToolChoice::None);
        let prepared = crate::tools::policy::prepare_responses_request(name, req).unwrap();
        assert!(matches!(prepared.tool_choice, Some(ToolChoice::None)));
        assert_eq!(prepared.parallel_tool_calls, Some(false));
        assert!(prepared.tools.is_some());
    }
    let mut req = request();
    req.tools.as_mut().unwrap()[0].name = "not.a.responses.name".into();
    assert!(crate::tools::policy::prepare_responses_request("gemini", req).is_err());
}

#[test]
fn interleaved_items_keep_native_order_with_local_message_ordinals_and_json_parity() {
    let mut r = reducer();
    let later = message("later", json!("final_answer"), "two");
    assert!(
        event(
            &mut r,
            json!({"type":"response.output_item.added","output_index":2,"item":later})
        )
        .is_empty()
    );
    event(
        &mut r,
        json!({"type":"response.output_item.added","output_index":0,"item":{"id":"rs","type":"reasoning","summary":[]}}),
    );
    event(
        &mut r,
        json!({"type":"response.reasoning_summary_text.delta","output_index":0,"item_id":"rs","summary_index":0,"delta":"why"}),
    );
    let first = message("first", json!("commentary"), "one");
    event(
        &mut r,
        json!({"type":"response.output_item.added","output_index":1,"item":first.clone()}),
    );
    event(
        &mut r,
        json!({"type":"response.output_item.done","output_index":1,"item":first.clone()}),
    );
    let value = snapshot(vec![
        json!({"id":"rs","type":"reasoning","summary":[{"type":"summary_text","text":"why"}],"encrypted_content":"synthetic"}),
        first,
        later,
    ]);
    r.snapshot(&value).unwrap();
    let stream = r.response().unwrap();
    let mut json = reducer();
    json.snapshot(&value).unwrap();
    let json = json.response().unwrap();
    assert_eq!(stream.messages, json.messages);
    assert_eq!(stream.reasoning_content, json.reasoning_content);
    assert_eq!(stream.provider_replay_state, json.provider_replay_state);
    assert_eq!(stream.messages[0].id, crate::ProviderMessageId(0));
    assert_eq!(stream.messages[1].id, crate::ProviderMessageId(1));
}

#[tokio::test]
async fn calls_followed_by_failed_incomplete_or_eof_never_leave_decoder_as_executable() {
    for provider in ["openai", "vercel-ai-gateway"] {
        for status in ["failed", "incomplete", "eof"] {
            let text = message("m", Value::Null, "partial");
            let mut terminal = snapshot(vec![text.clone(), call(json!("call_real"), "{")]);
            terminal["status"] = json!(status);
            terminal["incomplete_details"] = json!({"reason":"max_output_tokens"});
            terminal["error"] = json!({"code":"invalid_request_error","message":"SECRET"});
            let mut wire = format!(
                "data: {}\n\ndata: {}\n\ndata: {}\n\ndata: {}\n\n",
                json!({"type":"response.output_item.added","output_index":0,"item":message("m", Value::Null, "")}),
                json!({"type":"response.output_text.delta","output_index":0,"item_id":"m","content_index":0,"delta":"partial"}),
                json!({"type":"response.output_item.added","output_index":1,"item":call(json!("call_real"),"")}),
                json!({"type":"response.function_call_arguments.done","output_index":1,"item_id":"fc_synthetic","arguments":"{"})
            );
            if status != "eof" {
                wire.push_str(&format!(
                    "data: {}\n\n",
                    json!({"type":format!("response.{status}"),"response":terminal})
                ));
            }
            let bytes = futures_util::stream::iter(vec![Ok(bytes::Bytes::from(wire))]).boxed();
            let chunks = ResponsesApiCompatibleProvider::decode(bytes, reducer_for(provider))
                .collect::<Vec<_>>()
                .await;
            assert!(
                chunks
                    .iter()
                    .all(|c| c.as_ref().map_or(true, |c| c.tool_calls.is_empty()
                        && c.provider_replay_state.is_none()))
            );
            if provider == "vercel-ai-gateway" {
                assert_eq!(
                    chunks
                        .iter()
                        .filter_map(|c| c.as_ref().ok())
                        .map(|c| c.delta.as_str())
                        .collect::<String>(),
                    "partial"
                );
            }
            if status == "incomplete" {
                assert_eq!(
                    chunks.last().unwrap().as_ref().unwrap().termination,
                    Some(ProviderTermination::Length)
                );
            } else {
                assert!(chunks.last().unwrap().is_err());
            }
        }
    }
}

#[test]
fn private_json_tree_budget_counts_nodes_and_rejects_duplicate_keys() {
    assert!(super::wire::parse(br#"{"status":"completed","status":"failed"}"#, 4096).is_err());
    let nodes = json!({"unknown_metadata":(0..128).map(|_|json!({"small":1})).collect::<Vec<_>>()})
        .to_string();
    assert!(nodes.len() < 4096);
    let error = super::wire::parse(nodes.as_bytes(), 4096).unwrap_err();
    assert!(
        error
            .downcast_ref::<crate::ProviderResponseTooLarge>()
            .is_some()
    );
    let (value, bytes) =
        super::wire::parse(br#"{"type":"response.created","response":{}}"#, 4096).unwrap();
    assert_eq!(bytes, super::wire::retained_bytes(&value));
}

// Synthetic HTTP fixture exercises the real facade's prepare -> Files -> execute
// sequence. Artifact cache/receipt authority is covered by attachments::registry.
#[tokio::test]
async fn openai_large_pdf_upload_is_materialized_before_responses_dispatch() {
    use crate::attachments::regression as fixture;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root = format!("http://{}/owned/v1", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for upload in [true, false] {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let (offset, size) = loop {
                let mut buf = [0; 8192];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let head = String::from_utf8_lossy(&bytes[..i]).to_lowercase();
                    let size = head
                        .lines()
                        .find_map(|s| s.strip_prefix("content-length:"))
                        .unwrap()
                        .trim()
                        .parse::<usize>()
                        .unwrap();
                    break (i + 4, size);
                }
            };
            while bytes.len() < offset + size {
                let mut buf = [0; 8192];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
            }
            let head = String::from_utf8_lossy(&bytes[..offset]).to_lowercase();
            assert!(head.contains("authorization: bearer fixture"));
            let response = if upload {
                assert!(head.starts_with("post /owned/v1/files "));
                assert!(head.contains("idempotency-key: openai-upload:"));
                let multipart = String::from_utf8_lossy(&bytes[offset..offset + size]);
                assert!(multipart.contains("user_data"));
                assert!(multipart.contains("application/pdf"));
                json!({"id":"file-owned-synthetic"})
            } else {
                assert!(head.starts_with("post /owned/v1/responses "));
                let body: Value = serde_json::from_slice(&bytes[offset..offset + size]).unwrap();
                assert_eq!(
                    body["input"][0]["content"][1]["file_id"],
                    "file-owned-synthetic"
                );
                assert!(body["input"][0]["content"][1].get("file_data").is_none());
                snapshot(vec![message("m", json!("final_answer"), "PDF read")])
            }
            .to_string();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
        }
    });
    let provider = super::super::OpenAiProvider::with_base_url("fixture", root);
    let mut document = lopdf::Document::load_mem(&fixture::pdf(1)).unwrap();
    document.add_object(lopdf::Stream::new(
        lopdf::dictionary! {},
        vec![b'x'; 600 * 1024],
    ));
    let mut pdf = Vec::new();
    document.save_to(&mut pdf).unwrap();
    let req = fixture::request(
        "media",
        vec![fixture::part(
            crate::InputContentType::File,
            "application/pdf",
            &pdf,
        )],
    );
    let state = std::sync::Arc::new(fixture::state("openai", "media", json!({})));
    let response = fixture::scoped(state, provider.chat(req)).await.unwrap();
    assert_eq!(response.text, "PDF read");
    server.await.unwrap();
}

#[tokio::test]
async fn registry_audio_text_continuation_records_actual_chat_route() {
    for continuation in [false, true] {
        let body = json!({"id":"chatcmpl-synthetic","model":"gpt-audio","choices":[{"message":{"role":"assistant","content":"heard"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2}}).to_string();
        let (root, server) =
            super::super::usage_tests::fixture_with_headers(body, String::new(), "200 OK").await;
        let registry =
            crate::ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
                |_, _| Ok("fixture".into()),
                |_, _| Ok(None),
                move |_, _| Ok(Some(root.clone())),
                ProviderTimeoutPolicy::default(),
            );
        let provider = registry
            .get_or_create_for_workspace("audio-synthetic", "openai")
            .unwrap();
        let mut req = request();
        req.model = "gpt-audio".into();
        req.tools = None;
        req.tool_choice = None;
        req.parallel_tool_calls = None;
        if continuation {
            req.messages.push(ChatMessage::assistant("previous text"));
            req.messages.push(ChatMessage::user("continue as text"));
        }
        let response = provider.chat(req).await.unwrap();
        assert_eq!(response.text, "heard");
        let usage = response.usage.unwrap();
        assert_eq!(usage.input_tokens, Some(5));
        assert_eq!(usage.output_tokens, Some(2));
        assert_eq!(usage.api.as_deref(), Some("chat_completions"));
        assert!(usage.route.unwrap().ends_with(";path=/chat/completions"));
        assert!(usage.physical_attempt_id.is_some());
        let body = server.await.unwrap();
        assert_eq!(body["model"], "gpt-audio");
        assert!(body.get("input").is_none());
    }
}

#[test]
fn replay_uses_input_supported_message_fields_and_preserves_native_phase() {
    for phase in [Value::Null, json!("commentary"), json!("final_answer")] {
        let mut item = message("msg-original", phase.clone(), "readable");
        item["content"][0]["annotations"] = json!([{"type":"synthetic_annotation"}]);
        let input = replay::input_items(&[item.clone()]).unwrap();
        assert_eq!(input[0]["phase"], phase);
        assert_eq!(
            input[0]["content"][0],
            json!({"type":"input_text","text":"readable"})
        );
        assert!(input[0].get("id").is_none());
        assert!(input[0].get("status").is_none());
        item.as_object_mut().unwrap().remove("phase");
        assert!(
            replay::input_items(&[item]).unwrap()[0]
                .get("phase")
                .is_none()
        );
    }
}

#[test]
fn malformed_private_authority_is_unsupported_and_unknown_readable_projection_keeps_common_text() {
    let state = ProviderReplayState::for_model(
        "openai",
        "gpt-6.1-sol",
        json!({"schema_version":1,"wire_api":"responses","output":[]}),
    );
    assert_eq!(
        crate::continuation::retention(&state),
        crate::continuation::Retention::Unsupported
    );
    let mut message = ChatMessage::assistant("known portable text");
    message.provider_replay_state = Some(state);
    let portable = crate::history::portable_history_message(&message);
    assert_eq!(portable.content, "known portable text");
    assert!(portable.provider_replay_state.is_none());
}

// Synthetic compatible service/model contract, independent of OpenAI IDs and
// of the factory's production services. Uses the existing scoped catalog seam.
fn compatible_catalog(
    provider: &str,
    model: &str,
    overrides: Value,
) -> crate::catalog::ModelCatalog {
    let mut entry = json!({"id":model,"name":model,"provider":provider,
        "api":"openai-responses","baseUrl":"https://synthetic.invalid/v1",
        "contextWindow":1000000,"maxTokens":16000,"reasoning":true,
        "input":["text","image"],"inputOrigin":{"kind":"source","expression":"synthetic Responses contract"},"cost":{},
        "compat":{"supportsReasoningEffort":true},
        "thinkingLevelMap":{"high":"high","off":"none"}});
    for (key, value) in overrides.as_object().unwrap() {
        entry[key] = value.clone();
    }
    crate::catalog::ModelCatalog::parse(
        &json!({provider:{model:entry}}).to_string(),
        &json!({provider:{model:{"contextWindow":{"kind":"source","expression":"synthetic"},"maxTokens":{"kind":"source","expression":"synthetic"}}}}).to_string(),
    ).unwrap()
}

#[tokio::test]
async fn generic_responses_controls_use_other_provider_model_contract_before_network() {
    use crate::attachments::{admission::AdmissionState, regression::scoped};
    use crate::{ReasoningConfig, ReasoningEffort};
    use std::sync::Arc;
    let provider = ResponsesApiCompatibleProvider::new(
        "synthetic-responses",
        "https://synthetic.invalid/v1",
        "fixture",
        AuthStyle::Bearer,
    );
    let mut req = request();
    req.model = "vendor-reasoner".into();
    req.tools = None;
    req.tool_choice = None;
    req.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
    let state = Arc::new(AdmissionState::for_test(Arc::new(compatible_catalog(
        provider.name(),
        &req.model,
        json!({}),
    ))));
    scoped(state, async {
        let budget = provider.prepare_input_budget(req.clone()).await.unwrap();
        assert_eq!(budget.request.max_tokens, Some(100));
        let prepared = provider
            .prepare(budget.request, &provider.capabilities())
            .await
            .unwrap();
        for stream in [false, true] {
            let body = provider.render(&prepared, stream).unwrap();
            assert_eq!(body["model"], "vendor-reasoner");
            assert_eq!(body["reasoning"]["effort"], "high");
            assert_eq!(body["max_output_tokens"], 100);
            assert!(body.get("reasoning_effort").is_none());
            assert!(body.get("temperature").is_none());
        }
        req.reasoning = Some(ReasoningConfig::Disabled);
        let prepared = provider
            .prepare(req.clone(), &provider.capabilities())
            .await
            .unwrap();
        assert_eq!(
            provider.render(&prepared, false).unwrap()["reasoning"]["effort"],
            "none"
        );
        req.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::Low));
        assert!(
            provider
                .prepare(req.clone(), &provider.capabilities())
                .await
                .is_err()
        );
    })
    .await;
    req.reasoning = Some(ReasoningConfig::Effort(ReasoningEffort::High));
    for overrides in [
        json!({"api":"openai-completions"}),
        json!({"reasoning":false}),
        json!({"compat":{"supportsReasoningEffort":false}}),
        json!({"thinkingLevelMap":{}}),
        json!({"thinkingLevelMap":{"high":null}}),
        json!({"thinkingLevelMap":{"high":"arbitrary-native-value"}}),
    ] {
        let state = Arc::new(AdmissionState::for_test(Arc::new(compatible_catalog(
            provider.name(),
            &req.model,
            overrides,
        ))));
        scoped(state, async {
            assert!(provider.prepare_input_budget(req.clone()).await.is_err());
            assert!(
                provider
                    .prepare(req.clone(), &provider.capabilities())
                    .await
                    .is_err()
            );
        })
        .await;
    }
    // A name (even an OpenAI-looking ID) is not proof for another service.
    let state = Arc::new(AdmissionState::for_test(Arc::new(compatible_catalog(
        provider.name(),
        "different-model",
        json!({}),
    ))));
    scoped(state, async {
        assert!(
            provider
                .prepare(req.clone(), &provider.capabilities())
                .await
                .is_err()
        );
        req.model = "gpt-5.4".into();
        assert!(
            provider
                .prepare(req.clone(), &provider.capabilities())
                .await
                .is_err()
        );
    })
    .await;
}

#[tokio::test]
async fn generic_responses_sampling_preserves_unknown_and_explicit_denials() {
    use crate::ReasoningConfig;
    use crate::attachments::{admission::AdmissionState, regression::scoped};
    use std::sync::Arc;
    let provider = ResponsesApiCompatibleProvider::new(
        "synthetic-responses",
        "https://synthetic.invalid",
        "fixture",
        AuthStyle::Bearer,
    );
    let mut req = request();
    req.model = "vendor-reasoner".into();
    req.tools = None;
    req.tool_choice = None;
    req.reasoning = Some(ReasoningConfig::Disabled);
    req.temperature = Some(0.5);
    for (overrides, allowed) in [
        (json!({}), true),
        (json!({"compat":{"supportsTemperature":true}}), true),
        (json!({"sourceGeneration":{"temperature":true}}), true),
        (
            json!({"compat":{"supportsTemperature":false},"sourceGeneration":{"temperature":true}}),
            false,
        ),
        (
            json!({"compat":{"supportsTemperature":true},"sourceGeneration":{"temperature":false}}),
            false,
        ),
        (
            json!({"thinkingLevelMap":{"off":"low"},"compat":{"supportsTemperature":true}}),
            false,
        ),
        (
            json!({"thinkingLevelMap":{},"compat":{"supportsTemperature":true}}),
            false,
        ),
    ] {
        let state = Arc::new(AdmissionState::for_test(Arc::new(compatible_catalog(
            provider.name(),
            &req.model,
            overrides,
        ))));
        scoped(state, async {
            assert_eq!(
                provider.prepare_input_budget(req.clone()).await.is_ok(),
                allowed
            );
            let prepared = provider
                .prepare(req.clone(), &provider.capabilities())
                .await;
            assert_eq!(prepared.is_ok(), allowed);
            if let Ok(prepared) = prepared {
                let body = provider.render(&prepared, false).unwrap();
                assert_eq!(body["temperature"], json!(0.5));
                assert_eq!(body["reasoning"]["effort"], "none");
            }
        })
        .await;
    }
}

fn completed_provenance(item: &str) -> crate::MessageProvenance {
    crate::MessageProvenance {
        logical_turn_id: Some("turn".into()),
        workspace_id: "ws".into(),
        thread_id: "thread".into(),
        context_thread: None,
        unit_id: "completed-round".into(),
        sources: vec![crate::MessageSourceRef {
            scope: "context:turn".into(),
            id: item.into(),
            version: "revision:1".into(),
        }],
        source_aliases: vec![],
        ambiguous_input_aliases: vec![],
        complete: true,
        protected_input: false,
        inherited: false,
    }
}

fn compatible_history_request(
    provider: &ResponsesApiCompatibleProvider,
    with_media: bool,
) -> ChatRequest {
    use crate::{ProviderReplayState, ProviderToolCall};
    let mut req = request();
    req.model = "deepseek-v4-responses-fixture".into();
    req.reasoning = Some(crate::ReasoningConfig::Effort(crate::ReasoningEffort::High));
    let native = vec![
        json!({"type":"reasoning","id":"rs_complete","summary":[{"type":"summary_text","text":"readable reasoning"}],"encrypted_content":"SYNTHETIC_COMPLETED_CIPHERTEXT"}),
        message("m_commentary", json!("commentary"), "native commentary"),
        call(json!("call_binding"), "{}"),
    ];
    let mut assistant = ChatMessage::assistant_tool_calls_with_provider_state(
        Some("[Assistant commentary]\nnative commentary\n\ncommon aggregate must not be appended"),
        Some("readable reasoning"),
        vec![ProviderToolCall {
            id: "call_binding".into(),
            name: "lookup".into(),
            arguments: "{}".into(),
        }],
        Some(ProviderReplayState::for_model(
            provider.name(),
            &req.model,
            json!({"schema_version":1,"wire_api":"responses","authority_scope":provider.authority,"output":native}),
        )),
    );
    assistant.provenance = Some(completed_provenance("assistant"));
    let mut result = ChatMessage::tool_result("call_binding", "lookup", "actual tool result");
    result.provenance = Some(completed_provenance("result"));
    if with_media {
        let png = crate::attachments::regression::image(image::ImageFormat::Png, 1, 1);
        result
            .content_parts
            .push(crate::attachments::regression::part(
                crate::InputContentType::Image,
                "image/png",
                &png,
            ));
    }
    req.messages = vec![assistant, result, ChatMessage::user("continue")];
    req
}

#[tokio::test]
async fn generic_responses_replay_survives_budget_media_and_render_for_chat_policy_service_name() {
    use crate::attachments::{admission::AdmissionState, regression::scoped};
    use std::sync::Arc;
    // Synthetic only: this does not claim DeepSeek serves the Responses API.
    let provider = ResponsesApiCompatibleProvider::new(
        "deepseek",
        "https://synthetic.invalid",
        "fixture",
        AuthStyle::Bearer,
    )
    .with_input_capabilities(crate::ProviderInputCapabilities {
        text: true,
        image: crate::InputTypeSupport::data_url_inline_only(),
        file: crate::InputTypeSupport::disabled(),
        audio: crate::InputTypeSupport::disabled(),
        video: crate::InputTypeSupport::disabled(),
    });
    for with_media in [false, true] {
        let req = compatible_history_request(&provider, with_media);
        let original = req.messages[0].provider_replay_state.clone();
        let state = Arc::new(AdmissionState::for_test(Arc::new(compatible_catalog(
            provider.name(),
            &req.model,
            json!({}),
        ))));
        scoped(
            state,
            crate::tools::policy::with_discovery_tools(
                "deepseek",
                false,
                std::collections::BTreeMap::from([(req.model.clone(), true)]),
                async {
                    let budget = provider.prepare_input_budget(req.clone()).await.unwrap();
                    assert_eq!(budget.request.messages[0].provider_replay_state, original);
                    assert_eq!(
                        budget.request.messages[0].tool_calls.as_ref().unwrap()[0].id,
                        "call_binding"
                    );
                    assert_eq!(
                        budget.request.messages[1].tool_call_id.as_deref(),
                        Some("call_binding")
                    );
                    assert_eq!(budget.media.len(), usize::from(with_media));
                    if with_media {
                        assert_eq!((budget.media[0].message, budget.media[0].part), (2, 0));
                    }
                    let prepared = provider
                        .prepare(budget.request, &provider.capabilities())
                        .await
                        .unwrap();
                    assert_eq!(prepared.request.messages[0].provider_replay_state, original);
                    assert_eq!(
                        prepared.messages.messages[0].provider_replay_state,
                        original
                    );
                    assert_eq!(prepared.messages.messages[1].role, crate::Role::Tool);
                    for stream in [false, true] {
                        let body = provider.render(&prepared, stream).unwrap();
                        let input = body["input"].as_array().unwrap();
                        assert_eq!(input.iter().filter(|i| i["type"] == "reasoning").count(), 1);
                        assert_eq!(
                            input[0]["encrypted_content"],
                            "SYNTHETIC_COMPLETED_CIPHERTEXT"
                        );
                        assert_eq!(input[1]["phase"], "commentary");
                        assert_eq!(input[1]["content"][0]["text"], "native commentary");
                        assert_eq!(
                            input
                                .iter()
                                .filter(|i| i["type"] == "function_call")
                                .count(),
                            1
                        );
                        assert_eq!(input[2]["call_id"], "call_binding");
                        assert_eq!(
                            input
                                .iter()
                                .filter(|i| i["type"] == "function_call_output")
                                .count(),
                            1
                        );
                        assert_eq!(input[3]["call_id"], "call_binding");
                        assert_eq!(input[3]["output"][0]["text"], "actual tool result");
                        if with_media {
                            assert_eq!(input[4]["role"], "user");
                            assert_eq!(input[4]["content"][1]["type"], "input_image");
                            assert!(
                                input[4]["content"][1]["image_url"]
                                    .as_str()
                                    .unwrap()
                                    .starts_with("data:image/png;base64,")
                            );
                        }
                        let serialized = body.to_string();
                        assert!(!serialized.contains("common aggregate"));
                        assert_eq!(serialized.matches("native commentary").count(), 1);
                        assert_eq!(serialized.matches("readable reasoning").count(), 1);
                    }
                    for (field, wrong) in [
                        ("authority_scope", json!("other-authority")),
                        ("wire_api", json!("other-dialect")),
                        ("schema_version", json!(99)),
                        ("model", json!("different-model")),
                        ("provider", json!("different-provider")),
                    ] {
                        let mut switched = req.clone();
                        let state = switched.messages[0].provider_replay_state.as_mut().unwrap();
                        match field {
                            "model" => state.model = Some(wrong.as_str().unwrap().into()),
                            "provider" => state.provider = wrong.as_str().unwrap().into(),
                            _ => state.payload[field] = wrong,
                        }
                        let budget = provider
                            .prepare_input_budget(switched.clone())
                            .await
                            .unwrap();
                        assert!(budget.request.messages[0].provider_replay_state.is_none());
                        let prepared = provider
                            .prepare(budget.request, &provider.capabilities())
                            .await
                            .unwrap();
                        let body = provider.render(&prepared, false).unwrap().to_string();
                        assert!(body.contains("native commentary"));
                        assert!(!body.contains("SYNTHETIC_COMPLETED_CIPHERTEXT"));
                        switched.messages[0].provenance = None;
                        assert!(
                            provider
                                .prepare_input_budget(switched.clone())
                                .await
                                .is_err()
                        );
                        assert!(
                            provider
                                .prepare(switched, &provider.capabilities())
                                .await
                                .is_err()
                        );
                    }
                },
            ),
        )
        .await;
    }
}

fn reasoning_ciphertext(ciphertext: Value) -> Value {
    json!({"type":"reasoning","id":"rs_sealed","summary":[{"type":"summary_text","text":"readable reasoning"}],"encrypted_content":ciphertext})
}

fn completed_ciphertext_events() -> Vec<Value> {
    vec![
        json!({"type":"response.output_item.added","output_index":0,"item":reasoning_ciphertext(json!("SYNTHETIC_PARTIAL"))}),
        json!({"type":"response.output_item.done","output_index":0,"item":reasoning_ciphertext(json!("SYNTHETIC_COMPLETE_A"))}),
        json!({"type":"response.output_item.done","output_index":1,"item":call(json!("real_call"), "{}")}),
    ]
}

#[test]
fn completed_reasoning_ciphertext_allows_partial_completion_and_equal_or_omitted_terminal() {
    for omit in [false, true] {
        let mut r = reducer();
        let mut chunks = Vec::new();
        for e in completed_ciphertext_events() {
            let emitted = event(&mut r, e);
            assert!(
                emitted
                    .iter()
                    .all(|c| c.tool_calls.is_empty() && c.provider_replay_state.is_none())
            );
            chunks.extend(emitted);
        }
        let mut terminal = reasoning_ciphertext(json!("SYNTHETIC_COMPLETE_A"));
        if omit {
            terminal
                .as_object_mut()
                .unwrap()
                .remove("encrypted_content");
        }
        chunks.extend(event(&mut r, json!({"type":"response.completed","response":snapshot(vec![terminal, call(json!("real_call"), "{}")])})));
        let response = r.response().unwrap();
        assert_eq!(response.tool_calls.len(), 1);
        assert_eq!(response.tool_calls[0].id, "real_call");
        assert_eq!(
            response.reasoning_content.as_deref(),
            Some("readable reasoning")
        );
        let state = response.provider_replay_state.unwrap();
        assert_eq!(
            state.payload["output"][0]["encrypted_content"],
            "SYNTHETIC_COMPLETE_A"
        );
        assert!(!state.payload.to_string().contains("SYNTHETIC_PARTIAL"));
        assert_eq!(
            chunks
                .iter()
                .filter_map(|c| c.reasoning_delta.as_deref())
                .collect::<String>(),
            "readable reasoning"
        );
        assert_eq!(chunks.iter().map(|c| c.tool_calls.len()).sum::<usize>(), 1);
        assert_eq!(
            chunks
                .iter()
                .filter(|c| c.provider_replay_state.is_some())
                .count(),
            1
        );
    }
}

#[test]
fn partial_ciphertext_is_not_promoted_when_first_completed_snapshot_omits_it() {
    let mut r = reducer();
    event(
        &mut r,
        json!({"type":"response.output_item.added","output_index":0,"item":reasoning_ciphertext(json!("SYNTHETIC_PARTIAL"))}),
    );
    let mut completed = reasoning_ciphertext(Value::Null);
    completed
        .as_object_mut()
        .unwrap()
        .remove("encrypted_content");
    event(
        &mut r,
        json!({"type":"response.output_item.done","output_index":0,"item":completed}),
    );
    r.snapshot(&snapshot(vec![reasoning_ciphertext(json!(
        "SYNTHETIC_COMPLETE_A"
    ))]))
    .unwrap();
    assert_eq!(
        r.response().unwrap().provider_replay_state.unwrap().payload["output"][0]["encrypted_content"],
        "SYNTHETIC_COMPLETE_A"
    );
}

#[tokio::test]
async fn conflicting_completed_reasoning_is_safe_protocol_failure_with_usage_and_no_executable_state()
 {
    for conflicting in [json!("SYNTHETIC_COMPLETE_B"), Value::Null] {
        let terminal = snapshot(vec![
            reasoning_ciphertext(conflicting),
            call(json!("real_call"), "{}"),
        ]);
        let mut events = completed_ciphertext_events();
        events.push(json!({"type":"response.completed","response":terminal}));
        let mut r = reducer();
        for e in &events[..events.len() - 1] {
            event(&mut r, e.clone());
        }
        let error = r.event(None, events.last().unwrap()).unwrap_err();
        assert_eq!(r.usage.input_tokens, Some(100));
        assert_eq!(r.usage.output_tokens, Some(20));
        assert!(r.response().is_err());
        assert!(!format!("{error:#} {error:?}").contains("SYNTHETIC_COMPLETE"));
        // Exercise the production decoder, registry usage enrichment and error
        // redaction, rather than only the private merge helper. Synthetic SSE.
        let body = events
            .iter()
            .map(|e| format!("data: {e}\n\n"))
            .collect::<String>();
        let (root, server) = super::super::usage_tests::fixture_with_headers(
            body,
            "X-Request-Id: req_openai_native\r\n".into(),
            "200 OK",
        )
        .await;
        let registry = crate::ProviderRegistry::with_provider(
            "openai",
            std::sync::Arc::new(super::super::OpenAiProvider::with_base_url(
                "synthetic-key",
                root,
            )),
        );
        let provider = registry.get_or_create("openai").unwrap();
        let mut stream = provider
            .stream_chat_with_diagnostics(request())
            .await
            .unwrap()
            .stream;
        let mut failure = None;
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(chunk) => {
                    assert!(chunk.tool_calls.is_empty());
                    assert!(chunk.provider_replay_state.is_none());
                }
                Err(error) => {
                    assert!(failure.replace(error).is_none());
                }
            }
        }
        let error = failure.expect("contradictory completed state must fail");
        let facts = crate::usage::error_usage(&error).unwrap();
        assert_eq!(facts.input_tokens, Some(100));
        assert_eq!(facts.output_tokens, Some(20));
        assert_eq!(facts.api.as_deref(), Some("responses"));
        assert_eq!(facts.request_id.as_deref(), Some("req_openai_native"));
        assert_eq!(facts.generation_id.as_deref(), Some("resp_synthetic"));
        assert!(facts.physical_attempt_id.is_some());
        assert!(facts.route.as_ref().unwrap().ends_with(";path=/responses"));
        let classification = provider.classify_failure(&error).unwrap();
        assert_eq!(
            classification.class,
            pioneer_protocol::ProviderFailureClass::MalformedProviderRequest
        );
        assert_eq!(classification.http_status, None);
        let safe = format!("{error:#} {error:?} {facts:?}");
        for secret in [
            "SYNTHETIC_COMPLETE_A",
            "SYNTHETIC_COMPLETE_B",
            "SYNTHETIC_PARTIAL",
            "encrypted_content",
            "synthetic-key",
        ] {
            assert!(!safe.contains(secret));
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn generic_responses_native_negative_facts_veto_catalog_effort_and_off() {
    use crate::attachments::{admission::AdmissionState, regression::scoped};
    let provider = ResponsesApiCompatibleProvider::new(
        "synthetic-responses",
        "https://synthetic.invalid",
        "fixture",
        AuthStyle::Bearer,
    );
    let mut req = request();
    req.model = "vendor-reasoner".into();
    req.tools = None;
    req.tool_choice = None;
    for (selection, key, value) in [
        (
            crate::ReasoningConfig::Effort(crate::ReasoningEffort::High),
            "reasoning.supported",
            false,
        ),
        (
            crate::ReasoningConfig::Effort(crate::ReasoningEffort::High),
            "effort.supported",
            false,
        ),
        (
            crate::ReasoningConfig::Effort(crate::ReasoningEffort::High),
            "effort.enum",
            false,
        ),
        (
            crate::ReasoningConfig::Effort(crate::ReasoningEffort::High),
            "effort.high",
            false,
        ),
        (crate::ReasoningConfig::Disabled, "effort.none", false),
        (crate::ReasoningConfig::Disabled, "mandatory", true),
    ] {
        req.reasoning = Some(selection);
        let state = std::sync::Arc::new(AdmissionState::for_test(std::sync::Arc::new(
            compatible_catalog(provider.name(), &req.model, json!({})),
        )));
        let facts = std::collections::BTreeMap::from([(
            req.model.clone(),
            std::collections::BTreeMap::from([(key.into(), Some(value))]),
        )]);
        scoped(
            state,
            crate::generation::with_native_reasoning(provider.name(), true, facts, async {
                assert!(provider.prepare_input_budget(req.clone()).await.is_err());
                assert!(
                    provider
                        .prepare(req.clone(), &provider.capabilities())
                        .await
                        .is_err()
                );
            }),
        )
        .await;
    }
}

#[tokio::test]
async fn responses_media_admission_keeps_catalog_role_source_and_adapter_negatives() {
    use crate::attachments::{admission::AdmissionState, regression::scoped};
    // Neither registration nor a service's old Chat contract proves media.
    for name in ["deepseek", "synthetic-responses"] {
        let provider = ResponsesApiCompatibleProvider::new(
            name,
            "https://synthetic.invalid",
            "fixture",
            AuthStyle::Bearer,
        )
        .with_input_capabilities(crate::ProviderInputCapabilities {
            text: true,
            image: crate::InputTypeSupport::data_url_inline_only(),
            file: crate::InputTypeSupport::disabled(),
            audio: crate::InputTypeSupport::disabled(),
            video: crate::InputTypeSupport::disabled(),
        });
        let mut req = request();
        req.model = "vendor-vision".into();
        req.tools = None;
        req.tool_choice = None;
        req.max_tokens = None;
        let png = crate::attachments::regression::image(image::ImageFormat::Png, 1, 1);
        req.messages[0]
            .content_parts
            .push(crate::attachments::regression::part(
                crate::InputContentType::Image,
                "image/png",
                &png,
            ));
        for (overrides, allowed) in [
            (json!({}), true),
            (json!({"api":"openai-completions"}), false),
            (json!({"input":["text"]}), false),
            (
                json!({"inputConstraints":{"image":{"roles":["tool"]}}}),
                false,
            ),
            (
                json!({"inputConstraints":{"image":{"mimeTypes":["image/jpeg"]}}}),
                false,
            ),
            (json!({"inputConstraints":{"image":{"maxBytes":1}}}), false),
        ] {
            let state = std::sync::Arc::new(AdmissionState::for_test(std::sync::Arc::new(
                compatible_catalog(name, &req.model, overrides),
            )));
            scoped(state, async {
                assert_eq!(
                    provider.prepare_input_budget(req.clone()).await.is_ok(),
                    allowed
                );
                assert_eq!(
                    provider
                        .prepare(req.clone(), &provider.capabilities())
                        .await
                        .is_ok(),
                    allowed
                );
            })
            .await;
        }
        let state = std::sync::Arc::new(AdmissionState::for_test(std::sync::Arc::new(
            compatible_catalog(name, &req.model, json!({})),
        )));
        scoped(state, async {
            let mut wrong_role = req.clone();
            wrong_role.messages[0].role = crate::Role::Assistant;
            assert!(
                provider
                    .prepare_input_budget(wrong_role.clone())
                    .await
                    .is_err()
            );
            assert!(
                provider
                    .prepare(wrong_role, &provider.capabilities())
                    .await
                    .is_err()
            );
            let mut reference = req.clone();
            if let crate::MessageContentPart::Image { image } =
                &mut reference.messages[0].content_parts[0]
            {
                image.source = crate::AttachmentDataSource::Reference {
                    reference: "unowned".into(),
                };
            }
            assert!(
                provider
                    .prepare_input_budget(reference.clone())
                    .await
                    .is_err()
            );
            assert!(
                provider
                    .prepare(reference, &provider.capabilities())
                    .await
                    .is_err()
            );
            let disabled = ResponsesApiCompatibleProvider::new(
                name,
                "https://synthetic.invalid",
                "fixture",
                AuthStyle::Bearer,
            );
            assert!(disabled.prepare_input_budget(req.clone()).await.is_err());
            assert!(
                disabled
                    .prepare(req.clone(), &disabled.capabilities())
                    .await
                    .is_err()
            );
        })
        .await;
    }
}

#[tokio::test]
async fn responses_anthropic_named_consumer_requires_model_pdf_support_through_preparation() {
    use crate::attachments::{admission::AdmissionState, regression as fixture};
    use std::sync::Arc;
    // Synthetic API contract; no Anthropic Responses integration is asserted.
    let provider = ResponsesApiCompatibleProvider::new(
        "anthropic",
        "https://synthetic.invalid",
        "fixture",
        AuthStyle::Bearer,
    )
    .with_input_capabilities(crate::ProviderInputCapabilities {
        text: true,
        image: crate::InputTypeSupport::data_url_inline_only(),
        file: crate::InputTypeSupport::native_inline_only(),
        audio: crate::InputTypeSupport::disabled(),
        video: crate::InputTypeSupport::disabled(),
    });
    let req = fixture::request(
        "vendor-vision",
        vec![fixture::part(
            crate::InputContentType::File,
            "application/pdf",
            &fixture::pdf(1),
        )],
    );
    for (overrides, expected) in [
        (
            json!({"sourceMetadata":{"attachment":false}}),
            crate::catalog::InputCapabilityState::Unsupported,
        ),
        (
            json!({"inputOrigin":null}),
            crate::catalog::InputCapabilityState::Unknown,
        ),
        (
            json!({"input":["text","image","pdf"],"sourceMetadata":{"attachment":false}}),
            crate::catalog::InputCapabilityState::Supported,
        ),
    ] {
        let catalog = compatible_catalog(provider.name(), &req.model, overrides);
        let entry = catalog.model(provider.name(), &req.model).unwrap();
        assert_eq!(entry.api, "openai-responses");
        assert_eq!(
            entry.input_capability(crate::InputContentType::File),
            expected
        );
        let supported = expected == crate::catalog::InputCapabilityState::Supported;
        let state = Arc::new(AdmissionState::for_test(Arc::new(catalog)));
        fixture::scoped(state, async {
            // Both production entry paths must apply admission, before I/O.
            let budget = provider.prepare_input_budget(req.clone()).await;
            let direct = provider
                .prepare(req.clone(), &provider.capabilities())
                .await;
            if !supported {
                for error in [budget.err().unwrap(), direct.err().unwrap()] {
                    assert!(
                        error
                            .downcast_ref::<crate::attachments::MediaInputRejection>()
                            .is_some()
                    );
                    let safe = format!("{error:#}");
                    assert!(
                        safe.contains("does not support this media input")
                            || safe.contains("unknown input capabilities")
                    );
                }
                return;
            }
            let budget = budget.unwrap();
            assert_eq!(budget.media.len(), 1);
            assert_eq!((budget.media[0].message, budget.media[0].part), (0, 0));
            // Repeated materialization of the budget's pinned bytes stays valid.
            let repeated = provider
                .prepare(budget.request, &provider.capabilities())
                .await
                .unwrap();
            for prepared in [direct.unwrap(), repeated] {
                assert_eq!(prepared.messages.attachments.len(), 1);
                assert_eq!(
                    prepared.messages.attachments[0].mime_type,
                    "application/pdf"
                );
                for stream in [false, true] {
                    let body = provider.render(&prepared, stream).unwrap();
                    let input = body["input"].as_array().unwrap();
                    assert_eq!(input.len(), 1);
                    assert_eq!(input[0]["role"], "user");
                    let files = input[0]["content"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|p| p["type"] == "input_file")
                        .collect::<Vec<_>>();
                    assert_eq!(files.len(), 1);
                    assert!(
                        files[0]["file_data"]
                            .as_str()
                            .unwrap()
                            .starts_with("data:application/pdf;base64,")
                    );
                }
            }
        })
        .await;
    }
}

#[tokio::test]
async fn responses_gemini_named_consumer_does_not_inherit_native_live_endpoint_guard() {
    use crate::attachments::{admission::AdmissionState, regression as fixture};
    use std::sync::Arc;
    // Synthetic only: Gemini's native API is still covered by ServiceDefault.
    let provider = ResponsesApiCompatibleProvider::new(
        "gemini",
        "https://synthetic.invalid",
        "fixture",
        AuthStyle::Bearer,
    )
    .with_input_capabilities(crate::ProviderInputCapabilities {
        text: true,
        image: crate::InputTypeSupport::data_url_inline_only(),
        file: crate::InputTypeSupport::disabled(),
        audio: crate::InputTypeSupport::disabled(),
        video: crate::InputTypeSupport::disabled(),
    });
    for model in ["vendor-live-fixture", "deep-research-vendor-fixture"] {
        let req = fixture::request(
            model,
            vec![fixture::part(
                crate::InputContentType::Image,
                "image/png",
                &fixture::image(image::ImageFormat::Png, 1, 1),
            )],
        );
        // Production catalog lookup canonicalizes the consumer gemini to google.
        let catalog = compatible_catalog("google", model, json!({}));
        assert_eq!(
            catalog.model("gemini", model).unwrap().api,
            "openai-responses"
        );
        let state = Arc::new(AdmissionState::for_test(Arc::new(catalog)));
        fixture::scoped(state, async {
            let budget = provider.prepare_input_budget(req.clone()).await.unwrap();
            assert_eq!(budget.media.len(), 1);
            assert_eq!((budget.media[0].message, budget.media[0].part), (0, 0));
            let direct = provider
                .prepare(req, &provider.capabilities())
                .await
                .unwrap();
            let repeated = provider
                .prepare(budget.request, &provider.capabilities())
                .await
                .unwrap();
            for prepared in [direct, repeated] {
                assert_eq!(prepared.messages.attachments.len(), 1);
                for stream in [false, true] {
                    let body = provider.render(&prepared, stream).unwrap();
                    assert_eq!(body["model"], model);
                    assert_eq!(body["input"].as_array().unwrap().len(), 1);
                    let images = body["input"][0]["content"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .filter(|p| p["type"] == "input_image")
                        .collect::<Vec<_>>();
                    assert_eq!(images.len(), 1);
                    assert!(
                        images[0]["image_url"]
                            .as_str()
                            .unwrap()
                            .starts_with("data:image/png;base64,")
                    );
                }
            }
        })
        .await;
    }
}

#[tokio::test]
async fn vercel_authority_models_drive_controls_and_media_on_the_responses_wire() {
    use crate::attachments::regression::{image, part};
    use crate::{InputContentType as Kind, ReasoningConfig as Config, ReasoningEffort as Effort};
    // Independent, hand-authored contracts. Names intentionally resemble
    // vendors without authorizing any family-based control inference.
    for (raw, selection, expected, forbidden) in [
        (
            json!({"id":"private/gpt-6-astra","type":"language","tags":["tool-use","reasoning"],"modalities":{"input":["text","image","pdf"],"output":["text"]},"reasoning_options":[{"type":"effort","values":["low","high"]}],"temperature":false,"max_tokens":800}),
            Config::Effort(Effort::High),
            "high",
            Effort::Max,
        ),
        (
            json!({"id":"private/claude","type":"language","tags":["tool-use","reasoning"],"modalities":{"input":["text","image","pdf"],"output":["text"]},"reasoning_options":[{"type":"toggle"},{"type":"effort","values":["medium"]}],"temperature":true,"max_tokens":800}),
            Config::Disabled,
            "none",
            Effort::High,
        ),
        (
            json!({"id":"private/gemini","type":"language","tags":["tool-use","reasoning"],"modalities":{"input":["text","image","pdf"],"output":["text"]},"reasoning_options":[{"type":"effort","values":["low"]}],"temperature":true,"max_tokens":800}),
            Config::Effort(Effort::Low),
            "low",
            Effort::None,
        ),
    ] {
        let model = crate::catalog::generator::vercel_model(&raw).unwrap();
        let id = model.id.clone();
        let models = std::collections::BTreeMap::from([(id.clone(), model)]);
        let provider = ResponsesApiCompatibleProvider::new(
            "vercel-ai-gateway",
            "https://private.invalid/v1",
            "fixture",
            AuthStyle::Bearer,
        )
        .with_input_capabilities(crate::ProviderInputCapabilities {
            text: true,
            image: crate::InputTypeSupport::data_url_inline_only(),
            file: crate::InputTypeSupport {
                native: true,
                file_upload: false,
                data_url_inline: true,
                text_fallback: false,
            },
            ..crate::ProviderInputCapabilities::disabled_for_all_file_types()
        });
        crate::attachments::runtime::with_async_authority_scope(
            "fixture-authority".into(),
            crate::generation::with_native_models("vercel-ai-gateway", false, models, async {
                let mut req = request();
                req.model = id;
                req.reasoning = Some(selection);
                req.messages[0].content_parts.push(part(
                    Kind::Image,
                    "image/png",
                    &image(image::ImageFormat::Png, 1, 1),
                ));
                req.messages[0].content_parts.push(part(
                    Kind::File,
                    "application/pdf",
                    &crate::attachments::regression::pdf(1),
                ));
                let budget = provider.prepare_input_budget(req.clone()).await.unwrap();
                assert_eq!(budget.media.len(), 2);
                assert!(budget.media.iter().all(|media| media.input_tokens > 0));
                let prepared = provider
                    .prepare(req.clone(), &provider.capabilities())
                    .await
                    .unwrap();
                let body = provider.render(&prepared, true).unwrap();
                assert_eq!(body["reasoning"]["effort"], expected);
                assert_eq!(body["model"], req.model);
                assert_eq!(body["max_output_tokens"], 100);
                assert_eq!(body["input"][0]["content"][1]["type"], "input_image");
                assert!(
                    body["input"][0]["content"][1]["image_url"]
                        .as_str()
                        .unwrap()
                        .starts_with("data:image/png;base64,")
                );
                assert_eq!(body["input"][0]["content"][2]["type"], "input_file");
                assert!(
                    body["input"][0]["content"][2]["file_data"]
                        .as_str()
                        .unwrap()
                        .starts_with("data:application/pdf;base64,")
                );
                assert!(body["input"][0]["content"][2].get("file_id").is_none());
                req.reasoning = Some(Config::Effort(forbidden));
                assert!(
                    provider
                        .prepare(req.clone(), &provider.capabilities())
                        .await
                        .is_err()
                );
                req.reasoning = None;
                req.max_tokens = Some(801);
                assert!(
                    provider
                        .prepare(req.clone(), &provider.capabilities())
                        .await
                        .is_err()
                );
                req.max_tokens = None;
                let prepared = provider
                    .prepare(req.clone(), &provider.capabilities())
                    .await
                    .unwrap();
                let body = provider.render(&prepared, false).unwrap();
                assert!(body.get("reasoning").is_none());
                assert!(body.get("max_output_tokens").is_none());
                assert!(body.get("temperature").is_none());
                if raw["temperature"] == true {
                    for temperature in [f32::NAN, -0.1, 2.1] {
                        req.temperature = Some(temperature);
                        assert!(
                            provider
                                .prepare(req.clone(), &provider.capabilities())
                                .await
                                .is_err()
                        );
                    }
                    req.temperature = None;
                }
                req.messages[0]
                    .content_parts
                    .push(part(Kind::Audio, "audio/wav", b"not accessed"));
                assert!(
                    provider
                        .prepare(req, &provider.capabilities())
                        .await
                        .is_err()
                );
            }),
        )
        .await;
    }
}

#[tokio::test]
async fn vercel_unknown_controls_leave_defaults_and_fallback_limits_are_not_native_caps() {
    let model = crate::catalog::generator::vercel_model(&json!({"id":"private/claude-gpt-gemini","type":"language","tags":["tool-use","reasoning"],"modalities":{"input":["text"],"output":["text"]}})).unwrap();
    let provider = ResponsesApiCompatibleProvider::new(
        "vercel-ai-gateway",
        "https://private.invalid/v1",
        "fixture",
        AuthStyle::Bearer,
    );
    crate::attachments::runtime::with_async_authority_scope(
        "fixture-authority".into(),
        crate::generation::with_native_models(
            "vercel-ai-gateway",
            false,
            std::collections::BTreeMap::from([(model.id.clone(), model.clone())]),
            async {
                let mut req = request();
                req.model = model.id;
                req.max_tokens = Some(5000);
                // 4096 in normalized storage is a conservative fallback, not evidence
                // of a Vercel output ceiling. Unknown effort still fails.
                let prepared = provider
                    .prepare(req.clone(), &provider.capabilities())
                    .await
                    .unwrap();
                let body = provider.render(&prepared, false).unwrap();
                assert_eq!(body["max_output_tokens"], 5000);
                assert!(body.get("reasoning").is_none());
                req.reasoning = Some(crate::ReasoningConfig::Effort(crate::ReasoningEffort::High));
                assert!(
                    provider
                        .prepare(req.clone(), &provider.capabilities())
                        .await
                        .is_err()
                );
                req.reasoning = None;
                req.temperature = Some(0.5);
                let prepared = provider
                    .prepare(req, &provider.capabilities())
                    .await
                    .unwrap();
                for stream in [false, true] {
                    let body = provider.render(&prepared, stream).unwrap();
                    assert_eq!(body["temperature"], 0.5);
                    assert!(body.get("reasoning").is_none());
                    assert_eq!(body["max_output_tokens"], 5000);
                    assert_eq!(body["stream"], stream);
                }
            },
        ),
    )
    .await;
}
