//! Deferred only: an independently captured HTTP input from the real ACKed
//! agent loop is the oracle for cold/accepted restoration's next wire prefix.
use super::*;
use pioneer_provider::providers::{
    AuthStyle, ChatCompletionsApiCompatibleProvider, ResponsesApiCompatibleProvider,
};
use pioneer_tools::{
    ConfiguredToolSpec, ExecutionClass, FunctionToolOutput, PayloadKind, ToolEventTrace,
    ToolExtensionBundle, ToolHandler, ToolInvocation, ToolSpec,
};

const MODEL: &str = "gpt-5.1";
const NAMES: [&str; 2] = ["cache_slow", "cache_error"];

struct ParallelTools(Arc<tokio::sync::Semaphore>);

#[async_trait]
impl ToolHandler for ParallelTools {
    async fn handle(
        &self,
        invocation: ToolInvocation,
        _: ToolEventTrace,
    ) -> Result<Box<dyn pioneer_tools::ToolOutput>, pioneer_tools::ToolError> {
        if invocation.tool_name == "cache_slow" {
            // Released only by the committed fast-result event, after actual
            // Gateway persistence has ACKed it. Call ordinal is slow, error;
            // completion/persistence order is error, slow.
            self.0.acquire().await.unwrap().forget();
            // Each side alone exceeds the owner's byte ceiling. The middle
            // marker cannot survive a head/tail excerpt, regardless of tokenizer.
            let padding = "x".repeat(2 * pioneer_compaction::RESULT_BYTES);
            Ok(Box::new(FunctionToolOutput::new(
                format!("BOUNDED_BEGIN {padding} BOUNDED_MIDDLE {padding} BOUNDED_END"),
                true,
            )))
        } else {
            assert_eq!(invocation.tool_name, "cache_error");
            Ok(Box::new(FunctionToolOutput::new(
                "Error: synthetic file is absent",
                false,
            )))
        }
    }
}

struct TurnTools(Arc<dyn ToolHandler>);

#[async_trait]
impl pioneer_agent::TurnToolProvider for TurnTools {
    async fn materialize_turn_tools(
        &self,
        _: pioneer_agent::TurnToolContext,
    ) -> Result<pioneer_agent::TurnToolMaterialization, String> {
        let mut policy = pioneer_tools::dynamic_unknown_output_policy();
        policy.llm = pioneer_protocol::LlmOutputPolicy::Full { max_bytes: 2_048 };
        policy.llm_retention =
            pioneer_protocol::LlmRetentionPolicy::UntilTurnTerminal { max_bytes: 8_192 };
        Ok(pioneer_agent::TurnToolMaterialization {
            bundles: vec![ToolExtensionBundle {
                specs: NAMES
                    .iter()
                    .map(|name| {
                        ConfiguredToolSpec::new(
                            ToolSpec::new(
                                *name,
                                "Fixed cache-prefix fixture tool",
                                json!({
                                    "type":"object", "properties":{}, "additionalProperties":false
                                }),
                                PayloadKind::Function,
                            ),
                            ExecutionClass::Shared,
                            policy.clone(),
                        )
                    })
                    .collect(),
                handlers: NAMES
                    .iter()
                    .map(|name| ((*name).into(), self.0.clone()))
                    .collect(),
            }],
            ..Default::default()
        })
    }
}

// Only preflight planning and stream selection are fixture-owned. Main
// preparation, parsing, native replay, and HTTP serialization are real adapters.
struct LoopProvider {
    inner: Arc<dyn Provider>,
    requests: std::sync::Mutex<Vec<ChatRequest>>,
}

#[async_trait]
impl Provider for LoopProvider {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn usage_api(&self) -> &'static str {
        self.inner.usage_api()
    }
    fn authority_fingerprint(&self) -> Option<&str> {
        self.inner.authority_fingerprint()
    }
    fn capabilities(&self) -> ProviderCapabilities {
        let mut caps = self.inner.capabilities();
        caps.streaming = false;
        caps
    }
    async fn prepare_input_budget(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<pioneer_provider::attachments::PreparedInputBudget> {
        self.inner.prepare_input_budget(request).await
    }
    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        if is_turn_preflight_request(&request) {
            return Ok(test_turn_preflight_response_with_visible_tools(&NAMES));
        }
        self.requests.lock().unwrap().push(request.clone());
        self.inner.chat(request).await
    }
    async fn stream_chat(
        &self,
        _: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        unreachable!("this fixture selects the real nonstream transport")
    }
}

async fn transport(
    responses: bool,
) -> (
    String,
    mpsc::UnboundedReceiver<serde_json::Value>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root = format!("http://{}/v1", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::unbounded_channel();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let router = axum::Router::new().route(if responses {"/v1/responses"} else {"/v1/chat/completions"}, axum::routing::post(
        move |axum::Json(body): axum::Json<serde_json::Value>| {
            let first = count.fetch_add(1, Ordering::SeqCst) == 0;
            tx.send(body).unwrap();
            async move {
                let calls = NAMES.iter().enumerate().map(|(i, name)| if responses {
                    json!({"type":"function_call","id":format!("fc_{i}"),"call_id":format!("cache_call_{i}"),
                        "name":name,"arguments":"{}","status":"completed"})
                } else {
                    json!({"id":format!("cache_call_{i}"),"type":"function","function":{"name":name,"arguments":"{}"}})
                }).collect::<Vec<_>>();
                let reply = if responses {
                    let output = if first {
                        let mut output = vec![json!({"type":"message","id":"progress","role":"assistant","status":"completed","phase":"commentary",
                            "content":[{"type":"output_text","text":"Checking the retained sources","annotations":[]}]}),
                            json!({"type":"reasoning","id":"native-reasoning","summary":[],"encrypted_content":"native-prefix-opaque"})];
                        output.extend(calls); output
                    } else {
                        vec![json!({"type":"message","id":"final","role":"assistant","status":"completed","phase":"final_answer",
                            "content":[{"type":"output_text","text":"done","annotations":[]}]})]
                    };
                    json!({"id":"response","status":"completed","model":MODEL,"output":output})
                } else if first {
                    json!({"id":"response","model":MODEL,"choices":[{"index":0,"finish_reason":"tool_calls",
                        "message":{"role":"assistant","content":null,"tool_calls":calls}}]})
                } else {
                    json!({"id":"response","model":MODEL,"choices":[{"index":0,"finish_reason":"stop",
                        "message":{"role":"assistant","content":"done"}}]})
                };
                axum::Json(reply)
            }
        }
    ));
    (
        root,
        rx,
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() }),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_loop_sent_prefix_survives_cold_and_accepted_restore_on_both_wires() {
    for responses in [true, false] {
        crate::compaction::load_test_catalog();
        let (root, mut wire, server) = transport(responses).await;
        let mut input_types = ProviderInputCapabilities::disabled_for_all_file_types();
        input_types.image = pioneer_provider::InputTypeSupport::data_url_inline_only();
        let inner: Arc<dyn Provider> = if responses {
            Arc::new(
                ResponsesApiCompatibleProvider::new("openai", &root, "fixture", AuthStyle::Bearer)
                    .with_input_capabilities(input_types),
            )
        } else {
            Arc::new(
                ChatCompletionsApiCompatibleProvider::new(
                    "openai",
                    &root,
                    "fixture",
                    AuthStyle::Bearer,
                )
                .with_input_capabilities(input_types),
            )
        };
        let provider = Arc::new(LoopProvider {
            inner,
            requests: Default::default(),
        });
        let registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
            "openai",
            provider.clone(),
        ));
        let harness = setup_phase_13_compaction_harness(registry.clone()).await;
        let processor = &harness.processor;
        let store = &harness.crud_store;
        let workspace = &harness.workspace_id;
        processor
            .agent_manager
            .set_context_controller(Some(Arc::new(
                crate::compaction::GatewayNativeContextController::new(Arc::downgrade(processor)),
            )))
            .await;
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        processor
            .agent_manager
            .set_turn_tool_provider(Some(Arc::new(TurnTools(Arc::new(ParallelTools(
                release.clone(),
            ))))))
            .await;
        let images = tempfile::tempdir().unwrap();
        let path = images.path().join("prefix.webp");
        std::fs::write(
            &path,
            include_bytes!(
                "../../../../provider/tests/fixtures/capabilities/hermes-feature-connect.webp"
            ),
        )
        .unwrap();
        let input = vec![
            UserInput::Text {
                text: "Run both retained fixture tools".into(),
                text_elements: vec![],
            },
            UserInput::LocalImage {
                path: path.to_string_lossy().into_owned(),
            },
        ];
        let mut thread = phase_13_test_thread(workspace, "sent-prefix", phase_13_now_secs());
        thread.model = MODEL.into();
        let mut turn = phase_13_turn("sent-turn", TurnStatus::InProgress);
        turn.mode = ThreadMode::Agent;
        store
            .materialize_turn_start(
                &thread,
                SandboxMode::FullAccess,
                &turn,
                &input,
                pioneer_protocol::PersistedActorRef::Principal(
                    authenticated_test_superuser().principal_id.clone(),
                ),
            )
            .await
            .unwrap();
        let mut loaded_thread = thread.clone();
        loaded_thread.turns = vec![turn.clone()];
        processor
            .thread_manager
            .system_thread_restore_persisted(loaded_thread, Some(SandboxMode::FullAccess))
            .await
            .unwrap();
        let mut authority = crate::authorization::ExecutionAuthorizationContext::for_test(
            authenticated_test_superuser().as_ref(),
            workspace,
            &thread.id,
            &turn.permission_profile,
            None,
        );
        authority.bind_test_provider_authority(
            "openai",
            MODEL,
            registry
                .authority_fingerprint_for_workspace(workspace, "openai")
                .unwrap()
                .as_str(),
        );
        assert!(
            store
                .set_turn_execution_authorization_context(
                    &turn.id,
                    &authority.to_persisted_json().unwrap()
                )
                .await
                .unwrap()
        );
        processor
            .agent_manager
            .ensure_thread(&thread.id, workspace)
            .await
            .unwrap();
        let mut committed = processor
            .agent_manager
            .subscribe_committed(&thread.id)
            .await
            .unwrap();
        processor
            .ensure_agent_listener_task(&thread.id)
            .await
            .unwrap();
        processor
            .agent_manager
            .start_turn_with_hook_context_permission_profile_and_security_snapshot(
                &thread.id,
                &turn.id,
                ThreadMode::Agent,
                pioneer_agent::AgentTurnHookRuntimeContext::default(),
                MODEL,
                "openai",
                HashMap::new(),
                pioneer_skills::SkillCatalogSnapshot {
                    version: 1,
                    generated_at_unix: 0,
                    skills: vec![],
                },
                input,
                vec![],
                vec![],
                HashMap::new(),
                vec![],
                turn.permission_profile.clone(),
                test_full_access_execution_security_snapshot(),
            )
            .await
            .unwrap();
        let mut acked_tools = Vec::new();
        let mut failed_tool_recorded = false;
        let mut bounded_item_id = None;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                match committed.recv().await.unwrap() {
                    AgentDurableEvent::ItemCompleted { notification }
                        if matches!(&notification.item, TurnItem::DynamicToolCall {
                            tool_name, success: Some(false), ..
                        } if tool_name == "cache_error") =>
                    {
                        failed_tool_recorded = true;
                    }
                    AgentDurableEvent::TurnLlmContextAppended {
                        source,
                        tool_name,
                        item_id,
                        payload,
                        ..
                    } if source == "tool_result_v2" => {
                        if tool_name == "cache_error" {
                            release.add_permits(1);
                        } else if tool_name == "cache_slow" {
                            let pioneer_protocol::ToolResultView::Json { value, .. } = payload
                            else {
                                panic!("the full retained result must cross the durable lane");
                            };
                            let full = value["content"].as_str().unwrap();
                            assert!(full.len() > pioneer_compaction::RESULT_BYTES);
                            assert!(
                                full.contains("BOUNDED_BEGIN")
                                    && full.contains("BOUNDED_MIDDLE")
                                    && full.contains("BOUNDED_END")
                            );
                            bounded_item_id = Some(item_id);
                        }
                        acked_tools.push(tool_name);
                    }
                    AgentDurableEvent::TurnCompleted { .. } => break,
                    AgentDurableEvent::TurnFailed { .. }
                    | AgentDurableEvent::TurnBlocked { .. } => {
                        panic!("loop failed before its sent-prefix boundary")
                    }
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(acked_tools, ["cache_error", "cache_slow"]);
        assert!(
            failed_tool_recorded,
            "the error must be a real durable tool failure"
        );
        let bounded_item_id = bounded_item_id.expect("the large result must have a durable source");
        let initial = wire.recv().await.unwrap();
        let sent = wire.recv().await.unwrap();
        let retry_final = wire.recv().await.unwrap();
        let requests = provider.requests.lock().unwrap().clone();
        // The recoverable error leaves a retry instruction. The first no-tool
        // reply clears it before the loop can accept the terminal reply.
        assert_eq!(
            requests.len(),
            3,
            "tool, retry, and terminal rounds must be observed"
        );
        let sent_request = &requests[1];
        assert!(
            !sent_request
                .compiled_prompt
                .as_ref()
                .unwrap()
                .full_system_text
                .is_empty()
        );
        assert!(
            sent_request
                .tools
                .as_ref()
                .unwrap()
                .iter()
                .any(|tool| tool.name == "cache_slow")
        );
        assert_eq!(initial["tools"], sent["tools"]);
        assert!(!sent["tools"].as_array().unwrap().is_empty());
        if responses {
            assert!(!sent["instructions"].as_str().unwrap().is_empty());
        } else {
            assert!(sent["messages"].as_array().unwrap().iter().any(|message| {
                matches!(message["role"].as_str(), Some("system" | "developer"))
                    && message["content"]
                        .as_str()
                        .is_some_and(|text| !text.is_empty())
            }));
        }
        let input_key = if responses { "input" } else { "messages" };
        let prefix = sent[input_key].as_array().unwrap();
        let results = prefix
            .iter()
            .filter(|item| {
                if responses {
                    item["type"] == "function_call_output"
                } else {
                    item["role"] == "tool"
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(results.len(), 2, "non-vacuous full ordered result proof");
        let retry_results = retry_final[input_key]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| {
                if responses {
                    item["type"] == "function_call_output"
                } else {
                    item["role"] == "tool"
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(
            retry_results, results,
            "the terminal retry must retain both bounded results"
        );
        let call_key = if responses { "call_id" } else { "tool_call_id" };
        assert_eq!(
            results
                .iter()
                .map(|item| item[call_key].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["cache_call_1", "cache_call_0"]
        );
        let model_results = sent_request
            .messages
            .iter()
            .filter(|message| message.role == pioneer_provider::Role::Tool)
            .collect::<Vec<_>>();
        assert_eq!(model_results.len(), results.len());
        for (item, message) in results.iter().zip(model_results) {
            let text = if responses {
                let output = item["output"].as_array().unwrap();
                assert_eq!(output.len(), 1);
                assert_eq!(output[0]["type"], "input_text");
                output[0]["text"].as_str().unwrap()
            } else {
                item["content"].as_str().unwrap()
            };
            assert_eq!(item[call_key].as_str(), message.tool_call_id.as_deref());
            assert_eq!(text, message.content);
            // The owner budgets the serialized individual ChatMessage, including
            // its framing. Tie that measurement to the real HTTP text above;
            // protocol-specific HTTP escaping/envelopes have no 16-KiB contract.
            let framed = serde_json::to_string(message).unwrap();
            assert!(framed.len() <= pioneer_compaction::RESULT_BYTES);
            assert!(pioneer_compaction::text_tokens(&framed) <= pioneer_compaction::RESULT_TOKENS);
            if message.tool_call_id.as_deref() == Some("cache_call_0") {
                assert!(!text.contains("BOUNDED_MIDDLE"));
                let (head, shortened) = text
                    .split_once("\n[Result shortened. Full source: ")
                    .unwrap();
                let (locator, tail) = shortened
                    .split_once("; use threads_tools_result_read.]\n")
                    .unwrap();
                assert!(head.contains("BOUNDED_BEGIN"));
                assert!(tail.contains("BOUNDED_END"));
                assert_eq!(
                    serde_json::from_str::<JsonValue>(locator).unwrap(),
                    json!({
                        "workspace_id":workspace, "thread_id":thread.id, "turn_id":turn.id, "item_id":bounded_item_id,
                    })
                );
            } else {
                assert!(text.contains("Error: synthetic file is absent"));
            }
        }
        if responses {
            assert!(prefix.iter().any(|item| item["phase"] == "commentary"));
            assert!(
                prefix
                    .iter()
                    .any(|item| item["encrypted_content"] == "native-prefix-opaque")
            );
        }
        assert!(sent.to_string().contains("data:image/webp;base64,"));
        // CRUD timestamps turn/start and its input from thread.updated_at.
        // Advance only the new fixture turn beyond every saved source, even
        // when the whole first loop completed within the same wall-clock second.
        use pioneer_crud::compaction::PagedSource;
        let mut latest = chrono::DateTime::from_timestamp(thread.updated_at, 0)
            .unwrap()
            .fixed_offset();
        let mut saw_context = false;
        for kind in [
            PagedSource::Input,
            PagedSource::Event,
            PagedSource::ProviderContext,
        ] {
            let mut after = 0;
            loop {
                let page = store
                    .compaction_source_metadata_page(workspace, &thread.id, &turn.id, kind, after)
                    .await
                    .unwrap();
                for row in page.entries {
                    saw_context |= matches!(kind, PagedSource::ProviderContext);
                    latest = std::cmp::max(latest, row.created_at);
                }
                if page.next_sequence <= after {
                    break;
                }
                after = page.next_sequence;
            }
        }
        assert!(
            saw_context,
            "the prior loop must have persisted canonical contexts"
        );
        let mut next_thread = thread.clone();
        next_thread.updated_at = latest.timestamp().checked_add(1).unwrap();
        let next_input = vec![UserInput::Text {
            text: "Next independent input".into(),
            text_elements: vec![],
        }];
        store
            .materialize_turn_start(
                &next_thread,
                SandboxMode::FullAccess,
                &phase_13_turn("next-sent-turn", TurnStatus::InProgress),
                &next_input,
                pioneer_protocol::PersistedActorRef::Principal(
                    authenticated_test_superuser().principal_id.clone(),
                ),
            )
            .await
            .unwrap();
        let next_sources = store
            .compaction_source_metadata_page(
                workspace,
                &thread.id,
                "next-sent-turn",
                PagedSource::Input,
                0,
            )
            .await
            .unwrap();
        assert_eq!(next_sources.entries.len(), 1);
        assert_eq!(
            next_sources.entries[0].created_at.timestamp(),
            next_thread.updated_at
        );
        assert!(next_sources.entries[0].created_at > latest);
        let cold = crate::compaction::frozen::capture_execution_basis_prepared(
            store, workspace, &thread.id, None, None, None,
        )
        .await
        .unwrap();
        let allowed = std::collections::BTreeSet::from([thread.id.clone()]);
        let descriptor = crate::compaction::frozen::capture(
            store,
            workspace,
            &thread.id,
            &allowed,
            &cold.messages,
        )
        .await
        .unwrap();
        let accepted = crate::compaction::frozen::restore_accepted_history_for_execution(
            store,
            workspace,
            Some(&thread.id),
            &thread.id,
            &allowed,
            &serde_json::to_string(&descriptor).unwrap(),
        )
        .await
        .unwrap();
        let replay_provider = registry
            .get_or_create_for_workspace(workspace, "openai")
            .unwrap();
        for restored in [cold.messages, accepted.messages] {
            // This is the exact unchanged instruction/tool configuration sent
            // by the loop, rather than a second manually invented oracle.
            let mut next = sent_request.clone();
            next.messages = restored;
            pioneer_provider::prompt_cache::scope(workspace, &thread.id, "main", async {
                let next = replay_provider
                    .prepare_input_budget(next)
                    .await
                    .unwrap()
                    .request;
                replay_provider.chat(next).await.unwrap();
            })
            .await;
            let actual = wire.recv().await.unwrap();
            let actual_input = actual[input_key].as_array().unwrap();
            assert!(actual_input.len() > prefix.len());
            assert_eq!(
                &actual_input[..prefix.len()],
                prefix.as_slice(),
                "the previously sent full input changed on restore"
            );
            let mut previous_settings = sent.clone();
            previous_settings.as_object_mut().unwrap().remove(input_key);
            let mut actual_settings = actual.clone();
            actual_settings.as_object_mut().unwrap().remove(input_key);
            assert_eq!(
                actual_settings, previous_settings,
                "unchanged nonempty instructions/tools must render identically"
            );
            assert!(
                actual_input
                    .last()
                    .unwrap()
                    .to_string()
                    .contains("Next independent input")
            );
        }
        server.abort();
    }
}
