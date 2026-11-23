//! Deferred only: an independently captured HTTP input from the real ACKed
//! agent loop is the oracle for cold/accepted restoration's next wire prefix.
use super::*;
use pioneer_provider::providers::{
    AuthStyle, ChatCompletionsApiCompatibleProvider, OpenRouterProvider,
    ResponsesApiCompatibleProvider,
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
        if matches!(invocation.tool_name.as_str(), "cache_slow" | "cache_large") {
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

struct TurnTools(Arc<dyn ToolHandler>, &'static [&'static str]);

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
                specs: self
                    .1
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
                handlers: self
                    .1
                    .iter()
                    .map(|name| ((*name).into(), self.0.clone()))
                    .collect(),
            }],
            ..Default::default()
        })
    }
}

// Only stream selection is fixture-owned. Main
// preparation, parsing, native replay, and HTTP serialization are real adapters.
struct LoopProvider {
    inner: Arc<dyn Provider>,
    requests: std::sync::Mutex<Vec<ChatRequest>>,
    context_limit: Option<u64>,
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
    fn keeps_tools_when_disabled(&self, model: &str) -> bool {
        self.inner.keeps_tools_when_disabled(model)
    }
    async fn model_limits(
        &self,
        model: &str,
    ) -> anyhow::Result<pioneer_provider::catalog::CatalogLimits> {
        let mut limits = self.inner.model_limits(model).await?;
        if let Some(limit) = self.context_limit {
            limits.context_window = limit;
            limits.max_input = None;
        }
        Ok(limits)
    }
    async fn prepare_input_budget(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<pioneer_provider::attachments::PreparedInputBudget> {
        self.inner.prepare_input_budget(request).await
    }
    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
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
    transport_with_final(responses, "done").await
}

async fn transport_with_final(
    responses: bool,
    final_text: &str,
) -> (
    String,
    mpsc::UnboundedReceiver<serde_json::Value>,
    tokio::task::JoinHandle<()>,
) {
    scripted_transport(responses, final_text, |_| None).await
}

async fn scripted_transport(
    responses: bool,
    final_text: &str,
    scripted_reply: impl Fn(usize) -> Option<JsonValue> + Clone + Send + Sync + 'static,
) -> (
    String,
    mpsc::UnboundedReceiver<JsonValue>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let root = format!("http://{}/v1", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::unbounded_channel();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let final_text = final_text.to_owned();
    let router = axum::Router::new().route(if responses {"/v1/responses"} else {"/v1/chat/completions"}, axum::routing::post(
        move |axum::Json(body): axum::Json<serde_json::Value>| {
            let round = count.fetch_add(1, Ordering::SeqCst);
            tx.send(body).unwrap();
            let final_text = final_text.clone();
            let scripted = scripted_reply(round);
            async move {
                if let Some(reply) = scripted {
                    return axum::Json(reply);
                }
                let first = round == 0;
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
                            "content":[{"type":"output_text","text":final_text,"annotations":[]}]})]
                    };
                    json!({"id":"response","status":"completed","model":MODEL,"output":output})
                } else if first {
                    json!({"id":"response","model":MODEL,"choices":[{"index":0,"finish_reason":"tool_calls",
                        "message":{"role":"assistant","content":null,"tool_calls":calls}}]})
                } else {
                    json!({"id":"response","model":MODEL,"choices":[{"index":0,"finish_reason":"stop",
                        "message":{"role":"assistant","content":final_text}}]})
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

async fn start_fixture_turn(
    harness: &Phase13CompactionHarness,
    input: &[UserInput],
) -> (
    pioneer_protocol::Thread,
    Turn,
    tokio::sync::broadcast::Receiver<AgentDurableEvent>,
) {
    let processor = &harness.processor;
    let store = &harness.crud_store;
    let workspace = &harness.workspace_id;
    let mut thread = phase_13_test_thread(workspace, "sent-prefix", phase_13_now_secs());
    thread.model = MODEL.into();
    let mut turn = phase_13_turn("sent-turn", TurnStatus::InProgress);
    turn.mode = ThreadMode::Agent;
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &turn,
            input,
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
        processor
            .provider_registry()
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
    let committed = processor
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
            input.to_vec(),
            vec![],
            vec![],
            HashMap::new(),
            vec![],
            turn.permission_profile.clone(),
            test_full_access_execution_security_snapshot(),
        )
        .await
        .unwrap();
    (thread, turn, committed)
}

#[pioneer_postgres_test::postgres_test(flavor = "multi_thread", worker_threads = 2)]
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
            context_limit: None,
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
            .set_turn_tool_provider(Some(Arc::new(TurnTools(
                Arc::new(ParallelTools(release.clone())),
                &NAMES,
            ))))
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
        let (thread, turn, mut committed) = start_fixture_turn(&harness, &input).await;
        let mut acked_tools = Vec::new();
        let mut failed_tool_recorded = false;
        let mut bounded_item_id = None;
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                let event = match committed.recv().await {
                    Ok(event) => event,
                    // Terminal persistence may retire this unsubscribed actor
                    // before the terminal event reaches its in-memory hub.
                    // Confirm the durable outcome below, rather than requiring
                    // the actor to outlive its completed Turn.
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Err(error) => panic!("committed event stream lost prefix evidence: {error}"),
                };
                match event {
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
        let (_, completed) = store
            .get_turn(&thread.id, &turn.id)
            .await
            .unwrap()
            .expect("the loop must retain its durable Turn");
        assert_eq!(completed.status, TurnStatus::Completed, "{completed:?}");
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

#[pioneer_postgres_test::postgres_test(flavor = "multi_thread", worker_threads = 2)]
async fn responses_disclosure_appends_to_the_sent_gateway_input() {
    crate::compaction::load_test_catalog();
    let (root, mut wire, server) = scripted_transport(true, "done", |round| {
        let call = |id: &str, name: &str, arguments: String| {
            json!({"type":"function_call","id":format!("fc_{id}"),"call_id":id,
                "name":name,"arguments":arguments,"status":"completed"})
        };
        let discover = |id: &str, domain: &str| {
            call(id, "request_tools", json!({"domains":[domain],"reason":"Read fixture context"}).to_string())
        };
        let output = match round {
            0 => vec![discover("discover_threads", "threads")],
            1 => vec![
                call("slow_result", "cache_slow", "{}".into()),
                call("fast_result", "cache_error", "{}".into()),
            ],
            2 => vec![discover("discover_task", "task")],
            3 => vec![discover("repeat_task", "task")],
            _ => return None,
        };
        Some(json!({"id":format!("response_{round}"),"status":"completed","model":MODEL,"output":output}))
    })
    .await;
    let address = root
        .strip_prefix("http://")
        .unwrap()
        .strip_suffix("/v1")
        .unwrap()
        .parse()
        .unwrap();
    let inner = ResponsesApiCompatibleProvider::new(
        "openai",
        "https://api.openai.com/v1",
        "fixture",
        AuthStyle::Bearer,
    )
    .with_fixture_endpoint(address);
    let provider = Arc::new(LoopProvider {
        inner: Arc::new(inner),
        requests: Default::default(),
        context_limit: None,
    });
    let registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai", provider,
    ));
    let harness = setup_phase_13_compaction_harness(registry).await;
    let processor = &harness.processor;
    processor.bind_context_compaction().await;
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    // Materialize one optional tool from each domain. The real request_tools
    // handler and router decide when each definition becomes visible.
    processor
        .agent_manager
        .set_turn_tool_provider(Some(Arc::new(TurnTools(
            Arc::new(ParallelTools(release.clone())),
            &["cache_slow", "cache_error", "threads_list", "task_get"],
        ))))
        .await;
    let (thread, turn, mut committed) = start_fixture_turn(
        &harness,
        &[UserInput::Text {
            text: "Discover threads, check two results, then discover tasks.".into(),
            text_elements: vec![],
        }],
    )
    .await;
    let mut parallel_order = Vec::new();
    timeout(Duration::from_secs(15), async {
        loop {
            match committed.recv().await {
                Ok(AgentDurableEvent::TurnLlmContextAppended {
                    source, tool_name, ..
                }) if source == "tool_result_v2" && NAMES.contains(&tool_name.as_str()) => {
                    if tool_name == "cache_error" {
                        release.add_permits(1);
                    }
                    parallel_order.push(tool_name);
                }
                Ok(AgentDurableEvent::TurnCompleted { .. })
                | Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Ok(
                    AgentDurableEvent::TurnFailed { .. } | AgentDurableEvent::TurnBlocked { .. },
                ) => {
                    panic!("turn failed before the final request")
                }
                Err(error) => panic!("committed event stream lost result order: {error}"),
                Ok(_) => {}
            }
        }
    })
    .await
    .unwrap();
    let (_, completed) = harness
        .crud_store
        .get_turn(&thread.id, &turn.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.status, TurnStatus::Completed, "{completed:?}");
    assert_eq!(parallel_order, ["cache_error", "cache_slow"]);

    let mut sent = Vec::new();
    for _ in 0..5 {
        let body = wire
            .try_recv()
            .expect("local HTTP must capture each request");
        assert!(body.get("tools").is_none(), "lost positional declarations");
        sent.push(body["input"].as_array().unwrap().clone());
    }
    for pair in sent.windows(2) {
        let [previous, next] = pair else {
            unreachable!()
        };
        assert!(next.len() > previous.len());
        assert_eq!(
            serde_json::to_vec(&next[..previous.len()]).unwrap(),
            serde_json::to_vec(previous).unwrap(),
        );
    }
    let initial = &sent[0][0];
    assert_eq!(initial["type"], "additional_tools");
    let tools = initial["tools"].as_array().unwrap();
    assert!(tools.iter().any(|tool| tool["name"] == "request_tools"));
    assert!(
        tools
            .iter()
            .all(|tool| tool["name"] != "threads_list" && tool["name"] != "task_get")
    );
    for (round, id, name) in [
        (1, "discover_threads", "threads_list"),
        (3, "discover_task", "task_get"),
    ] {
        let new = &sent[round][sent[round - 1].len()..];
        let result = new
            .iter()
            .position(|item| item["type"] == "function_call_output" && item["call_id"] == id)
            .unwrap();
        let batches = new
            .iter()
            .enumerate()
            .filter(|(_, item)| item["type"] == "additional_tools")
            .collect::<Vec<_>>();
        let [(position, batch)] = batches.as_slice() else {
            panic!("one declaration batch required for {name}");
        };
        assert!(*position > result);
        let names = batch["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(names, [name]);
    }
    let parallel = &sent[2][sent[1].len()..];
    for (kind, expected) in [
        ("function_call", ["slow_result", "fast_result"]),
        ("function_call_output", ["fast_result", "slow_result"]),
    ] {
        assert_eq!(
            parallel
                .iter()
                .filter(|item| item["type"] == kind)
                .map(|item| item["call_id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            expected,
        );
    }
    for round in [2, 4] {
        assert!(
            sent[round][sent[round - 1].len()..]
                .iter()
                .all(|item| item["type"] != "additional_tools")
        );
    }
    server.abort();
}

#[pioneer_postgres_test::postgres_test(flavor = "multi_thread", worker_threads = 2)]
async fn chat_completions_bounds_new_result_under_pressure_and_keeps_sent_output() {
    crate::compaction::load_test_catalog();
    let (root, mut wire, server) = scripted_transport(false, "done", |round| {
        let (id, name) = match round {
            0 => ("first_result", "cache_slow"),
            1 => ("second_result", "cache_large"),
            _ => return None,
        };
        let mut reply = json!({"id":format!("response_{round}"),"model":MODEL,"choices":[{
            "index":0,"finish_reason":"tool_calls","message":{"role":"assistant","content":null,
                "tool_calls":[{"id":id,"type":"function","function":{"name":name,"arguments":"{}"}}]}
        }]});
        if round == 0 {
            // Leave room for the first bounded result while making the next
            // result compete for the remaining measured input budget.
            reply["usage"] = json!({"prompt_tokens":13_500,"completion_tokens":20,"total_tokens":13_520});
        }
        Some(reply)
    }).await;
    let provider = Arc::new(LoopProvider {
        inner: Arc::new(ChatCompletionsApiCompatibleProvider::new(
            "openai",
            root,
            "fixture",
            AuthStyle::Bearer,
        )),
        requests: Default::default(),
        context_limit: Some(40_000),
    });
    let registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai", provider,
    ));
    let harness = setup_phase_13_compaction_harness(registry).await;
    let processor = &harness.processor;
    processor.bind_context_compaction().await;
    processor
        .agent_manager
        .set_turn_tool_provider(Some(Arc::new(TurnTools(
            Arc::new(ParallelTools(Arc::new(tokio::sync::Semaphore::new(2)))),
            &["cache_slow", "cache_large"],
        ))))
        .await;
    let (thread, turn, mut committed) = start_fixture_turn(
        &harness,
        &[UserInput::Text {
            text: "Read both large fixture results.".into(),
            text_elements: vec![],
        }],
    )
    .await;

    let mut sent = Vec::new();
    for round in 1..=3 {
        match timeout(Duration::from_secs(30), wire.recv()).await {
            Ok(Some(request)) => sent.push(request),
            _ => {
                let durable_turn = harness.crud_store.get_turn(&thread.id, &turn.id).await;
                panic!("missing HTTP request {round}; durable turn: {durable_turn:?}");
            }
        }
    }
    let result = |round: usize, id: &str| {
        sent[round]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["role"] == "tool" && item["tool_call_id"] == id)
            .unwrap()
    };
    let old = result(1, "first_result");
    assert_eq!(
        serde_json::to_vec(old).unwrap(),
        serde_json::to_vec(result(2, "first_result")).unwrap(),
    );
    let large = result(2, "second_result")["content"].as_str().unwrap();
    assert!(large.len() < old["content"].as_str().unwrap().len());
    assert!(large.contains("BOUNDED_BEGIN") && large.contains("BOUNDED_END"));
    assert!(!large.contains("BOUNDED_MIDDLE"));
    let (_, shortened) = large
        .split_once("\n[Result shortened. Full source: ")
        .unwrap();
    let (locator, _) = shortened
        .split_once("; use threads_tools_result_read.]\n")
        .unwrap();
    let mut params: pioneer_protocol::ThreadToolResultReadParams =
        serde_json::from_str(locator).unwrap();
    assert_eq!(params.workspace_id, harness.workspace_id);
    assert_eq!(params.thread_id, thread.id);
    assert_eq!(params.turn_id, turn.id);
    params.max_tokens = Some(512);
    params.max_bytes = Some(2_048);
    let page = processor.read_thread_tool_result(params).await.unwrap();
    assert!(page.text.contains("BOUNDED_BEGIN"));
    assert!(!page.eof, "the source still contains the complete output");
    timeout(Duration::from_secs(15), async {
        loop {
            match committed.recv().await {
                Ok(AgentDurableEvent::TurnCompleted { .. })
                | Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                Ok(
                    AgentDurableEvent::TurnFailed { .. } | AgentDurableEvent::TurnBlocked { .. },
                ) => {
                    panic!("turn failed after sending the bounded result")
                }
                Err(error) => panic!("committed event stream unavailable: {error}"),
                Ok(_) => {}
            }
        }
    })
    .await
    .unwrap();
    let (_, completed) = harness
        .crud_store
        .get_turn(&thread.id, &turn.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.status, TurnStatus::Completed, "{completed:?}");
    let restored = crate::compaction::frozen::capture_execution_basis_prepared(
        &harness.crud_store,
        &harness.workspace_id,
        &thread.id,
        None,
        None,
        None,
    )
    .await
    .unwrap();
    let restored_result = restored
        .messages
        .iter()
        .find(|m| m.tool_call_id.as_deref() == Some("second_result"))
        .unwrap();
    assert_eq!(restored_result.content, large);
    server.abort();
}

#[pioneer_postgres_test::postgres_test(flavor = "multi_thread", worker_threads = 2)]
async fn final_no_tools_round_keeps_only_supported_wire_prefix() {
    crate::compaction::load_test_catalog();
    for public_responses in [true, false] {
        let (root, mut wire, server) = scripted_transport(true, "done", |round| {
            if round >= 2 {
                return None;
            }
            Some(json!({"id":format!("round_{round}"),"status":"completed","model":MODEL,"output":[{
                "type":"function_call","id":format!("fc_{round}"),"call_id":format!("failure_{round}"),
                "name":"cache_error","arguments":"{}","status":"completed"
            }]}))
        }).await;
        let inner = if public_responses {
            let address = root
                .strip_prefix("http://")
                .unwrap()
                .strip_suffix("/v1")
                .unwrap()
                .parse()
                .unwrap();
            ResponsesApiCompatibleProvider::new(
                "openai",
                "https://api.openai.com/v1",
                "fixture",
                AuthStyle::Bearer,
            )
            .with_fixture_endpoint(address)
        } else {
            ResponsesApiCompatibleProvider::new("openai", root, "fixture", AuthStyle::Bearer)
        };
        let provider = Arc::new(LoopProvider {
            inner: Arc::new(inner),
            requests: Default::default(),
            context_limit: None,
        });
        let registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
            "openai", provider,
        ));
        let mut config = test_tool_loop_config();
        config.retry.max_recoverable_retry_rounds_per_episode = 1;
        let harness = setup_phase_13_compaction_harness_with_tool_config(registry, config).await;
        let processor = &harness.processor;
        processor.bind_context_compaction().await;
        processor
            .agent_manager
            .set_turn_tool_provider(Some(Arc::new(TurnTools(
                Arc::new(ParallelTools(Arc::new(tokio::sync::Semaphore::new(0)))),
                &["cache_error"],
            ))))
            .await;
        let (thread, turn, mut events) = start_fixture_turn(
            &harness,
            &[UserInput::Text {
                text: "Read the fixture, report an error if unavailable.".into(),
                text_elements: vec![],
            }],
        )
        .await;
        timeout(Duration::from_secs(15), async {
            loop {
                match events.recv().await {
                    Ok(AgentDurableEvent::TurnCompleted { .. })
                    | Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                    Ok(
                        AgentDurableEvent::TurnFailed { .. }
                        | AgentDurableEvent::TurnBlocked { .. },
                    ) => panic!("turn did not complete after retry exhaustion"),
                    Err(error) => panic!("committed event stream unavailable: {error}"),
                    Ok(_) => {}
                }
            }
        })
        .await
        .unwrap();
        let (_, completed) = harness
            .crud_store
            .get_turn(&thread.id, &turn.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(completed.status, TurnStatus::Completed, "{completed:?}");
        let _initial = wire.try_recv().unwrap();
        let retry = wire.try_recv().unwrap();
        let final_request = wire.try_recv().unwrap();
        assert!(retry.get("tool_choice").is_none());
        let previous = retry["input"].as_array().unwrap();
        let next = final_request["input"].as_array().unwrap();
        assert!(
            next.iter().any(
                |item| item["call_id"] == "failure_1" && item["type"] == "function_call_output"
            )
        );
        if public_responses {
            assert_eq!(previous[0]["type"], "additional_tools");
            assert!(
                previous[0]["tools"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|tool| tool["name"] == "cache_error")
            );
            assert_eq!(&next[..previous.len()], previous);
            assert!(
                next[previous.len()..]
                    .iter()
                    .all(|item| item["type"] != "additional_tools")
            );
            assert!(final_request.get("tools").is_none());
            assert_eq!(final_request["tool_choice"], "none");
            for key in [
                "instructions",
                "model",
                "parallel_tool_calls",
                "max_output_tokens",
                "reasoning",
            ] {
                assert_eq!(final_request.get(key), retry.get(key), "{key}");
            }
        } else {
            assert!(!retry["tools"].as_array().unwrap().is_empty());
            assert!(final_request.get("tools").is_none());
            assert!(final_request.get("tool_choice").is_none());
            assert!(next.iter().all(|item| item["type"] != "additional_tools"));
        }
        server.abort();
    }
}

#[test]
fn sequential_detached_executions_keep_the_sent_input_prefix_on_both_wires() {
    run_standard_stack_message_test("detached prepared input continuity", async {
        for responses in [true, false] {
            crate::compaction::load_test_catalog();
            let answer = r#"<task_result>{"summary":"detached answer retained"}</task_result>"#;
            let (root, mut wire, server) = transport_with_final(responses, answer).await;
            let inner: Arc<dyn Provider> = if responses {
                Arc::new(
                    ResponsesApiCompatibleProvider::new(
                        "openai",
                        &root,
                        "fixture",
                        AuthStyle::Bearer,
                    )
                    .with_input_capabilities({
                        let mut types = ProviderInputCapabilities::disabled_for_all_file_types();
                        types.image = pioneer_provider::InputTypeSupport::data_url_inline_only();
                        types
                    }),
                )
            } else {
                Arc::new(OpenRouterProvider::with_base_url("fixture", &root))
            };
            let (instance, model) = if responses {
                ("openai", MODEL)
            } else {
                ("openrouter", "openai/gpt-5.1")
            };
            let provider = Arc::new(LoopProvider {
                inner,
                requests: Default::default(),
                context_limit: None,
            });
            // Real adapters and existing catalog models; only HTTP replies
            // and the fixture tool execution are controlled here.
            let registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
                instance,
                provider.clone(),
            ));
            registry
                .insert("summary-capture", provider.clone())
                .unwrap();
            let harness = setup_phase_13_compaction_harness(registry).await;
            let processor = &harness.processor;
            let store = &harness.crud_store;
            let workspace = &harness.workspace_id;
            let (tx, mut rx) = crate::session::test_support::outbound_channel(256);
            let connection =
                register_authenticated_test_connection(processor.session_manager.as_ref(), tx)
                    .await;
            processor
                .session_manager
                .set_connection_workspace(connection, Some(workspace.clone()))
                .await;
            processor.bind_task_bridge().await;
            processor.start_task_event_listener().await;
            processor.bind_context_compaction().await;
            processor
                .agent_manager
                .set_turn_tool_provider(Some(Arc::new(TurnTools(
                    Arc::new(ParallelTools(Arc::new(tokio::sync::Semaphore::new(1)))),
                    &NAMES,
                ))))
                .await;
            let parent = "detached-prefix-parent";
            let started = processor
                .thread_manager
                .thread_start_seeded(
                    connection,
                    workspace.clone(),
                    ThreadStartParams {
                        thread_id: parent.into(),
                        workspace_id: workspace.clone(),
                        name: Some("Detached prefix regression".into()),
                        model: Some(model.into()),
                        model_provider: Some(instance.into()),
                        sandbox: Some(SandboxMode::FullAccess),
                        mode: Some(ThreadMode::Agent),
                        origin_kind: Some(ThreadOriginKind::Collaborative),
                        sidebar_visibility: Some(ThreadSidebarVisibility::Visible),
                        visibility: None,
                        agent_nickname: None,
                        agent_role: None,
                    },
                    None,
                    None,
                )
                .await
                .unwrap();
            store
                .upsert_thread_model(
                    &started.response.thread,
                    pioneer_protocol::PersistedActorRef::System,
                )
                .await
                .unwrap();
            let image = ingest_typed_test_artifact_for_thread(
                processor,
                workspace,
                Some(parent),
                "detached-prefix.webp",
                include_bytes!(
                    "../../../../provider/tests/fixtures/capabilities/hermes-feature-connect.webp"
                )
                .to_vec(),
                pioneer_protocol::ArtifactKind::Image,
                "image/webp",
            )
            .await;
            let mut first_requests = Vec::new();
            let mut first_wire = Vec::new();
            let mut first_projection = None;
            let mut previous_count = 0;
            for (index, text) in ["first detached question", "second detached question"]
                .into_iter()
                .enumerate()
            {
                let turn = format!("detached-prefix-{index}");
                let request_id = generate_test_request_id("sent-prefix", &turn);
                let context = processor
                    .session_manager
                    .connection_context(connection)
                    .await
                    .unwrap();
                let input = vec![
                    UserInput::Text {
                        text: "distinct first input component".into(),
                        text_elements: vec![],
                    },
                    UserInput::Text {
                        text: text.into(),
                        text_elements: vec![],
                    },
                    UserInput::Artifact {
                        artifact_id: image.artifact_id.clone(),
                        version_id: image.version_id.clone(),
                    },
                ];
                Arc::clone(processor).process_owned_request(context, json!({
                    "jsonrpc":"2.0", "id":request_id, "method":"turn/start", "params":{
                        "thread_id":parent,"turn_id":turn,"input":input,
                        "mode":"Agent","model":model,"model_provider":instance,
                        "permission_profile":pioneer_protocol::TurnPermissionProfileSelection::full_access()
                    }
                }).to_string()).await;
                let response = recv_response_by_id(&mut rx, &request_id).await;
                let accepted: TurnStartResponse = serde_json::from_value(response.result).unwrap();
                assert_eq!(accepted.turn.id, turn);
                let tasks = processor
                    .task_runtime
                    .service()
                    .list_tasks(TaskListParams {
                        workspace_id: workspace.clone(),
                        owner_kind: Some(TaskOwnerKind::Thread),
                        owner_id: Some(parent.into()),
                        limit: Some(10),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                let task = tasks
                    .tasks
                    .iter()
                    .find(|task| task.created_by_turn_id.as_deref() == Some(turn.as_str()))
                    .unwrap();
                assert_eq!(
                    wait_for_task_status(store.clone(), &task.id, TaskStatus::Completed).await,
                    TaskStatus::Completed
                );
                processor
                    .process_due_task_deliveries(super::now_timestamp_secs().saturating_add(60), 10)
                    .await
                    .unwrap();
                let run = wait_for_task_run_id(store.clone(), &task.id).await;
                let lineage = wait_for_child_lineage_for_run(store.clone(), &run).await;
                assert_ne!(
                    lineage.child_thread_id, parent,
                    "exercise real detached execution, not two direct Agent calls"
                );
                let requests = provider.requests.lock().unwrap().clone();
                let mut sent = Vec::new();
                for _ in previous_count..requests.len() {
                    sent.push(wire.recv().await.unwrap());
                }
                if index == 0 {
                    assert!(
                        requests.len() >= 2,
                        "the first execution must send actual tool results"
                    );
                    let prepared = requests[0]
                        .messages
                        .iter()
                        .find(|message| {
                            message
                                .application_context
                                .as_ref()
                                .is_some_and(|m| m["kind"] == "turn_input")
                        })
                        .unwrap()
                        .clone();
                    assert!(prepared.content.ends_with(text));
                    assert!(prepared.content.contains("distinct first input component"));
                    assert_eq!(prepared.content_parts.len(), 1);
                    assert!(sent[0].to_string().contains("data:image/webp;base64,"));
                    first_projection = Some(prepared);
                    first_wire = sent;
                    first_requests = requests.clone();
                    // Canonical UI/input dates use whole seconds, whereas
                    // provider rounds retain fractions. Isolate this copy
                    // regression from equal-second source ordering.
                    let completed_second = phase_13_now_secs();
                    timeout(Duration::from_secs(3), async {
                        while phase_13_now_secs() <= completed_second {
                            sleep(Duration::from_millis(10)).await;
                        }
                    })
                    .await
                    .unwrap();
                } else {
                    let first = &requests[previous_count];
                    let prepared = first_projection.as_ref().unwrap();
                    let positions = first
                        .messages
                        .iter()
                        .enumerate()
                        .filter(|(_, message)| message.content == prepared.content)
                        .map(|(i, _)| i)
                        .collect::<Vec<_>>();
                    assert_eq!(positions.len(), 1);
                    assert_eq!(
                        serde_json::to_value(&first.messages[positions[0]]).unwrap(),
                        serde_json::to_value(prepared).unwrap()
                    );
                    assert!(!first.messages.iter().any(|message| message.role
                        == pioneer_provider::Role::User
                        && (message.content == "first detached question"
                            || message.content
                                == "distinct first input component\nfirst detached question")));
                    let old_final = first
                        .messages
                        .iter()
                        .position(|message| {
                            message.role == pioneer_provider::Role::Assistant
                                && message.content.contains("detached answer retained")
                        })
                        .unwrap();
                    let current = first
                        .messages
                        .iter()
                        .position(|message| {
                            message
                                .application_context
                                .as_ref()
                                .is_some_and(|m| m["kind"] == "turn_input")
                                && message.content.ends_with(text)
                        })
                        .unwrap();
                    assert!(positions[0] < old_final && old_final < current);
                    let last = first_requests.last().unwrap();
                    let old_tools = last
                        .messages
                        .iter()
                        .filter(|m| m.role == pioneer_provider::Role::Tool)
                        .map(|m| (m.tool_call_id.clone(), m.content.clone()))
                        .collect::<Vec<_>>();
                    assert_eq!(old_tools.len(), 2);
                    assert_eq!(
                        first
                            .messages
                            .iter()
                            .filter(|m| m.role == pioneer_provider::Role::Tool)
                            .map(|m| (m.tool_call_id.clone(), m.content.clone()))
                            .collect::<Vec<_>>(),
                        old_tools
                    );
                    let key = if responses { "input" } else { "messages" };
                    let prior = first_wire.last().unwrap()[key].as_array().unwrap();
                    let next = sent[0][key].as_array().unwrap();
                    // Compare message bytes; system/developer delivery belongs
                    // to the new execution's governing snapshot, not history.
                    let visible = |items: &[serde_json::Value]| {
                        items
                            .iter()
                            .filter(|item| {
                                !matches!(item["role"].as_str(), Some("system" | "developer"))
                            })
                            .cloned()
                            .collect::<Vec<_>>()
                    };
                    let prior = visible(prior);
                    let next = visible(next);
                    assert!(next.len() > prior.len());
                    assert_eq!(
                        &next[..prior.len()],
                        prior.as_slice(),
                        "raw parent input shifted the previously sent wire prefix"
                    );
                    assert!(next.last().unwrap().to_string().contains(text));
                }
                previous_count = requests.len();
            }
            server.abort();
        }
    });
}
