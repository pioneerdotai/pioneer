//! Deferred regressions. These fixtures contain no private conversation data.
use super::*;
use pioneer_provider::{
    CanonicalProviderRoundEnvelope, ChatMessage, InputTypeSupport, ProviderCallIdentity,
    ProviderInputCapabilities, ProviderReplayState, ProviderToolCall,
};
use serde_json::{Value, json};

pub(super) fn resolved(
    resolution: pioneer_protocol::ToolRetryResolution,
) -> pioneer_crud::CanonicalTurnEventPayload {
    pioneer_crud::CanonicalTurnEventPayload::ItemToolRetryResolved(
        pioneer_protocol::ItemToolRetryResolvedNotification {
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            item_id: "patch-item".into(),
            item_type: TurnItemType::DynamicToolCall,
            tool_retry_episode_id: "episode".into(),
            tool_name: "apply_patch".into(),
            attempt_number: 2,
            resolution,
            budgets: vec![],
            reason: "resolved".into(),
        },
    )
}

// A loopback transport exercises the actual public provider preparation/render
// boundary. It is used only by these deferred tests, never by diagnostics.
async fn wire_server() -> (
    String,
    tokio::sync::mpsc::UnboundedReceiver<Value>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root = format!("http://{}/v1", listener.local_addr().unwrap());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let router = axum::Router::new().route("/v1/responses", axum::routing::post(
        move |axum::Json(body): axum::Json<Value>| {
            let tx = tx.clone();
            async move {
                tx.send(body).unwrap();
                axum::Json(json!({"id":"response","status":"completed","model":"gpt-6.1-sol",
                    "output":[{"type":"message","id":"answer","role":"assistant","status":"completed",
                    "phase":"final_answer","content":[{"type":"output_text","text":"done","annotations":[]}]}]}))
            }
        }
    ));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (root, rx, server)
}

#[tokio::test]
async fn retry_resolution_between_call_and_result_survives_cold_accepted_and_responses_wire() {
    use pioneer_protocol::{PersistedActorRef, SandboxMode, ToolRetryResolution, UserInput};
    use pioneer_provider::providers::{AuthStyle, ResponsesApiCompatibleProvider};
    use std::collections::BTreeSet;

    // Both one-call and parallel rounds use the same strongest boundary proof.
    for (parallel, intermediate_user) in [(false, false), (true, false), (false, true)] {
        let (root, mut wire, server) = wire_server().await;
        let mut input_types = ProviderInputCapabilities::disabled_for_all_file_types();
        input_types.image = InputTypeSupport::data_url_inline_only();
        let provider = Arc::new(
            ResponsesApiCompatibleProvider::new("openai", root, "fixture", AuthStyle::Bearer)
                .with_input_capabilities(input_types),
        );
        let f = fixture_with_canonical_payload(
            serde_json::to_string(&resolved(ToolRetryResolution::Succeeded)).unwrap(),
            vec![],
            true,
            false,
        )
        .await;
        let db = f.store.database_connection();
        db.execute_unprepared("UPDATE turn SET status='completed',created_at='2026-10-30T05:35:00Z' WHERE id='turn'; UPDATE turn_event SET sequence=38,created_at='2026-10-30T05:36:15Z' WHERE id='source'").await.unwrap();
        let mut calls = vec![ProviderToolCall {
            id: "call_patch".into(),
            name: "apply_patch".into(),
            arguments: "{}".into(),
        }];
        if parallel {
            calls.push(ProviderToolCall {
                id: "call_error".into(),
                name: "read_file".into(),
                arguments: "{}".into(),
            });
        }
        let native_calls = calls
            .iter()
            .enumerate()
            .map(|(i, call)| {
                json!({
                    "type":"function_call","id":format!("fc_{i}"),"status":"completed",
                    "call_id":call.id,"name":call.name,"arguments":call.arguments
                })
            })
            .collect::<Vec<_>>();
        let mut output = vec![
            json!({"type":"message","id":"progress","role":"assistant","status":"completed","phase":"commentary",
                "content":[{"type":"output_text","text":"Working on patch","annotations":[]}]}),
            json!({"type":"reasoning","id":"reasoning","summary":[],"encrypted_content":"opaque-fixture"}),
        ];
        output.extend(native_calls);
        let mut assistant = ChatMessage::assistant_tool_calls(None::<String>, calls.clone());
        assistant.provider_replay_state = Some(ProviderReplayState::for_model(
            "openai",
            "gpt-6.1-sol",
            json!({
                "wire_api":"responses","schema_version":1,"authority_scope":provider.authority_fingerprint().unwrap(),"output":output
            }),
        ));
        let round = CanonicalProviderRoundEnvelope {
            ui_copies: None,
            version: 1,
            round_id: "patch-round".into(),
            termination: ProviderTermination::ToolCalls,
            message: assistant.clone(),
            calls: calls
                .iter()
                .enumerate()
                .map(|(i, call)| ProviderCallIdentity {
                    provider_call_id: call.id.clone(),
                    turn_item_id: if i == 0 { "patch-item" } else { "error-item" }.into(),
                    ordinal: i as u32,
                })
                .collect(),
        };
        db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
            "INSERT INTO turn_llm_context(id,turn_id,item_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES('call-source','turn','patch-round',7,'assistant_round',?,'{}','2026-10-30T05:36:14.874340Z')",
            [serde_json::to_string(&round).unwrap().into()])).await.unwrap();
        // Completion order differs from call ordinal for parallel tools.
        let mut live_results = Vec::new();
        for (i, call) in calls.iter().enumerate().rev() {
            let item = if i == 0 { "patch-item" } else { "error-item" };
            let result = ChatMessage::tool_result(
                &call.id,
                &call.name,
                if i == 0 {
                    "patch applied"
                } else {
                    "Error: file does not exist"
                },
            );
            let payload = pioneer_tools::ToolResultView::Json {
                value: serde_json::to_value(&result).unwrap(),
                truncated: false,
            };
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Sqlite,
                "INSERT INTO turn_llm_context(id,turn_id,item_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES(?,'turn',?,?,'tool_result_v2',?,'{}',?)",
                [format!("result-{i}").into(), item.into(), (8_i64 + live_results.len() as i64).into(), serde_json::to_string(&payload).unwrap().into(),
                    if i==0 { "2026-10-30T05:36:15.302774Z" } else { "2026-10-30T05:36:15.100000Z" }.into()])).await.unwrap();
            live_results.push(result);
        }
        let images = tempfile::tempdir().unwrap();
        let image = include_bytes!(
            "../../../provider/tests/fixtures/capabilities/hermes-feature-connect.webp"
        );
        let mut inputs = vec![UserInput::Text {
            text: "Compare these two pictures".into(),
            text_elements: vec![],
        }];
        for name in ["first.webp", "second.png"] {
            let path = images.path().join(name);
            if name == "first.webp" {
                std::fs::write(&path, image).unwrap();
            } else {
                image::RgbaImage::from_pixel(2, 2, image::Rgba([40, 80, 120, 255]))
                    .save(&path)
                    .unwrap();
            }
            inputs.push(UserInput::LocalImage {
                path: path.to_string_lossy().into_owned(),
            });
        }
        let thread = f.store.get_thread_model("thread").await.unwrap().unwrap();
        let (_, mut next) = f.store.get_turn("thread", "turn").await.unwrap().unwrap();
        next.id = "next".into();
        next.status = pioneer_protocol::TurnStatus::InProgress;
        f.store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &next,
                &inputs,
                PersistedActorRef::System,
            )
            .await
            .unwrap();
        db.execute_unprepared("UPDATE turn SET created_at='2026-10-30T05:47:46Z' WHERE id='next'; UPDATE turn_input SET created_at='2026-10-30T05:47:46Z' WHERE turn_id='next'; UPDATE turn_event SET created_at='2026-10-30T05:47:46Z' WHERE turn_id='next'").await.unwrap();
        if intermediate_user {
            db.execute_unprepared("UPDATE turn SET created_at='2026-10-30T05:36:15.050000Z' WHERE id='next'; UPDATE turn_input SET created_at='2026-10-30T05:36:15.050000Z' WHERE turn_id='next'; UPDATE turn_event SET created_at='2026-10-30T05:36:15.050000Z' WHERE turn_id='next'").await.unwrap();
        }
        let cold = super::super::frozen::capture_execution_basis_prepared(
            &f.store, "ws", "thread", None, None, None,
        )
        .await
        .unwrap();
        let allowed = BTreeSet::from(["thread".into()]);
        let descriptor =
            super::super::frozen::capture(&f.store, "ws", "thread", &allowed, &cold.messages)
                .await
                .unwrap();
        let accepted = super::super::frozen::restore_accepted_history_for_execution(
            &f.store,
            "ws",
            Some("thread"),
            "thread",
            &allowed,
            &serde_json::to_string(&descriptor).unwrap(),
        )
        .await
        .unwrap();
        let request = |messages| ChatRequest {
            model: "gpt-6.1-sol".into(),
            messages,
            temperature: None,
            max_tokens: Some(100),
            tools: None,
            tool_choice: None,
            parallel_tool_calls: None,
            reasoning: None,
            compiled_prompt: None,
        };
        // The active loop retains results in call ordinal order; durable rows
        // were appended in completion order above. Resolve its real pending
        // locators and prepare the complete request, including the new input.
        use pioneer_agent::compaction::history::{PendingOriginKind, pending_origin};
        assistant.provenance = Some(pending_origin(
            "ws",
            "thread",
            "turn",
            "patch-round",
            PendingOriginKind::Assistant,
            "patch-round",
        ));
        live_results.sort_by_key(|message| {
            calls
                .iter()
                .position(|call| Some(&call.id) == message.tool_call_id.as_ref())
                .unwrap()
        });
        for result in &mut live_results {
            let item = if result.tool_call_id.as_deref() == Some("call_patch") {
                "patch-item"
            } else {
                "error-item"
            };
            result.provenance = Some(pending_origin(
                "ws",
                "thread",
                "turn",
                "patch-round",
                PendingOriginKind::ToolResult,
                item,
            ));
        }
        let mut current = super::super::history::input_message(&inputs).unwrap();
        current.provenance = Some(pending_origin(
            "ws",
            "thread",
            "next",
            "user-input",
            PendingOriginKind::Input,
            "next",
        ));
        let mut live = vec![assistant];
        live.extend(live_results);
        live.push(current);
        let providers = pioneer_provider::ProviderRegistry::new(|_| "fixture".into());
        providers
            .insert("main-fixture", Arc::new(SmallWindowMain))
            .unwrap();
        let context = pioneer_agent::compaction::controller::NativeContext {
            overflow_recovery: false,
            recovery_deadline_ms: None,
            workspace_id: "ws".into(),
            thread_id: "thread".into(),
            turn_id: "next".into(),
            conversation_thread_id: None,
            provider_instance: "main-fixture".into(),
            provider: providers
                .get_or_create_for_workspace("ws", "main-fixture")
                .unwrap(),
            events: Arc::new(ExecutionEventHub::new()),
            cancellation: CancellationToken::new(),
        };
        let live = super::super::native::prepare_native_projection_from_history(
            &f.store,
            &providers,
            &CompactionSettings::default(),
            &context,
            request(live),
            cold,
            f.observer.clone(),
            f.clock.clone(),
        )
        .await
        .unwrap()
        .request
        .messages;
        let cold = super::super::frozen::capture_execution_basis_prepared(
            &f.store, "ws", "thread", None, None, None,
        )
        .await
        .unwrap();
        let mut requests = Vec::new();
        for messages in [live, cold.messages, accepted.messages] {
            let request = provider
                .prepare_input_budget(request(messages))
                .await
                .unwrap()
                .request;
            tokio::time::timeout(std::time::Duration::from_secs(5), provider.chat(request))
                .await
                .unwrap()
                .unwrap();
            requests.push(wire.recv().await.unwrap());
        }
        assert_eq!(
            requests[0], requests[1],
            "compare rendered requests, including native replay and images"
        );
        assert_eq!(
            requests[0], requests[2],
            "accepted execution must preserve the same wire prefix"
        );
        let input = requests[0]["input"].as_array().unwrap();
        let bindings = input
            .iter()
            .filter(|item| {
                matches!(
                    item["type"].as_str(),
                    Some("function_call" | "function_call_output")
                )
            })
            .map(|item| {
                (
                    item["type"].as_str().unwrap(),
                    item["call_id"].as_str().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            bindings,
            if parallel {
                vec![
                    ("function_call", "call_patch"),
                    ("function_call", "call_error"),
                    ("function_call_output", "call_error"),
                    ("function_call_output", "call_patch"),
                ]
            } else {
                vec![
                    ("function_call", "call_patch"),
                    ("function_call_output", "call_patch"),
                ]
            }
        );
        let user_position = input
            .iter()
            .position(|item| item["role"] == "user")
            .unwrap();
        let result_position = input
            .iter()
            .position(|item| item["type"] == "function_call_output")
            .unwrap();
        assert_eq!(user_position < result_position, intermediate_user);
        assert_eq!(input[0]["phase"], "commentary");
        assert!(!requests[0].to_string().contains("item_tool_retry_resolved"));
        assert!(
            input
                .iter()
                .any(|item| item["encrypted_content"] == "opaque-fixture")
        );
        for call in &calls {
            assert_eq!(
                input
                    .iter()
                    .filter(
                        |item| item["type"] == "function_call_output" && item["call_id"] == call.id
                    )
                    .count(),
                1
            );
        }
        let image_urls = input
            .iter()
            .flat_map(|item| item["content"].as_array().into_iter().flatten())
            .filter(|part| part["type"] == "input_image")
            .map(|part| part["image_url"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(image_urls.len(), 2);
        assert!(image_urls[0].starts_with("data:image/webp;base64,"));
        assert!(image_urls[1].starts_with("data:image/png;base64,"));
        assert!(requests[0].to_string().contains("patch applied"));
        if parallel {
            assert!(
                requests[0]
                    .to_string()
                    .contains("Error: file does not exist")
            );
        }
        // Canonical journal is still readable for UI/recovery; no event deletion.
        let source = f
            .store
            .compaction_source_page("ws", "thread", "turn", PagedSource::Event, 0)
            .await
            .unwrap()
            .entries
            .into_iter()
            .find(|entry| entry.reference.id == "source")
            .unwrap();
        assert!(
            f.store
                .compaction_reference_payload("ws", "thread", &source.reference)
                .await
                .unwrap()
                .unwrap()
                .contains("succeeded")
        );
        server.abort();
    }
}

#[test]
fn retry_non_retryable_resolution_remains_model_visible() {
    let event = resolved(pioneer_protocol::ToolRetryResolution::NonRetryable);
    assert!(!pioneer_crud::canonical_event_model_projection(&event).is_omitted());
    assert!(
        super::super::history::event_message(event)
            .unwrap()
            .unwrap()
            .content
            .contains("non_retryable")
    );
}

#[tokio::test]
async fn aihubmix_signed_tool_round_survives_cold_and_accepted_history_storage() {
    use pioneer_crud::NewTurnLlmContextEntry;
    use std::collections::BTreeSet;
    let f = fixture("unused", vec![], true, false).await;
    let call = ProviderToolCall {
        id: "signed-call".into(),
        name: "lookup".into(),
        arguments: "{}".into(),
    };
    let details = json!({"type":"thinking","thinking":"lookup first","signature":"synthetic-native-signature"});
    let state = ProviderReplayState::for_model(
        "aihubmix",
        "fixture",
        json!({"schema_version":1,
        "authority_scope":"synthetic-endpoint-authority", "assistant_message":{
            "content":null,"reasoning_content":null,"reasoning_details":details,
            "tool_calls":[{"id":"signed-call","type":"function","function":{"name":"lookup","arguments":"{}"}}]}}),
    );
    let mut assistant = ChatMessage::assistant_tool_calls(None::<String>, vec![call]);
    assistant.provider_replay_state = Some(state.clone());
    let envelope = CanonicalProviderRoundEnvelope {
        ui_copies: None,
        version: 1,
        round_id: "signed-round".into(),
        termination: ProviderTermination::ToolCalls,
        message: assistant,
        calls: vec![ProviderCallIdentity {
            provider_call_id: "signed-call".into(),
            turn_item_id: "signed-item".into(),
            ordinal: 0,
        }],
    };
    let result = ChatMessage::tool_result("signed-call", "lookup", "found");
    let view = pioneer_tools::ToolResultView::Json {
        value: serde_json::to_value(result).unwrap(),
        truncated: false,
    };
    let time = chrono::Utc::now().fixed_offset();
    for (sequence, source, item, payload) in [
        (
            11,
            "assistant_round",
            "signed-round",
            serde_json::to_string(&envelope).unwrap(),
        ),
        (
            12,
            "tool_result_v2",
            "signed-item",
            serde_json::to_string(&view).unwrap(),
        ),
    ] {
        f.store
            .insert_turn_llm_context(NewTurnLlmContextEntry {
                turn_id: "turn".into(),
                item_id: Some(item.into()),
                attempt_id: None,
                sequence,
                source: source.into(),
                tool_name: (source == "tool_result_v2").then(|| "lookup".into()),
                payload,
                output_policy_snapshot: "{}".into(),
                created_at: time + chrono::Duration::seconds(sequence),
                expires_at: None,
            })
            .await
            .unwrap();
    }
    drop(envelope);
    let cold = super::super::frozen::capture_execution_basis_prepared(
        &f.store, "ws", "thread", None, None, None,
    )
    .await
    .unwrap();
    let allowed = BTreeSet::from(["thread".into()]);
    let descriptor =
        super::super::frozen::capture(&f.store, "ws", "thread", &allowed, &cold.messages)
            .await
            .unwrap();
    let accepted = super::super::frozen::restore_accepted_history_for_execution(
        &f.store,
        "ws",
        Some("thread"),
        "thread",
        &allowed,
        &serde_json::to_string(&descriptor).unwrap(),
    )
    .await
    .unwrap();
    for messages in [cold.messages, accepted.messages] {
        let projected = pioneer_provider::history::project_messages_for_provider(
            "aihubmix", "fixture", &messages,
        )
        .unwrap();
        let index = projected
            .iter()
            .position(|m| m.provider_replay_state.as_ref() == Some(&state))
            .expect("stored signed round must be loaded");
        assert_eq!(
            projected[index]
                .provider_replay_state
                .as_ref()
                .unwrap()
                .payload["assistant_message"]["reasoning_details"],
            details
        );
        let result_index = projected
            .iter()
            .position(|m| m.tool_call_id.as_deref() == Some("signed-call"))
            .unwrap();
        assert!(index < result_index);
        assert_eq!(projected[result_index].role, pioneer_provider::Role::Tool);
        assert_eq!(projected[result_index].content, "found");
    }
}
