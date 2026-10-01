//! Provider -> gateway validation -> immutable checkpoint -> hook -> durable worker.
use super::*;
use crate::memory_tools::GatewayMemoryProvider;
use pioneer_memory::hooks::{
    AgentMemoryPostTurnExtractorProvider, AgentMemoryWriteProvider,
    MemoryDurableTerminalEffectClaim, MemoryManifest, MemoryManifestRequest,
    MemoryPostTurnExtractorContext, MemoryPostTurnExtractorRequest, MemoryTurnContext,
};
use pioneer_provider::ProviderTermination;
use sea_orm::EntityTrait;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};

const CANARY: &str = "PRIVATE_RESPONSE_CANARY_9341";
const THREAD: &str = "memory_validation_thread";
const TURN: &str = "memory_validation_turn";

struct ResponseProvider {
    replies: std::sync::Mutex<VecDeque<ChatResponse>>,
    calls: AtomicUsize,
    streams: std::sync::Mutex<Option<VecDeque<Vec<StreamChunk>>>>,
    revoke: std::sync::Mutex<Option<(Arc<CrudStore>, String, String, i64)>>,
}

impl ResponseProvider {
    fn new(replies: Vec<ChatResponse>) -> Self {
        Self {
            replies: std::sync::Mutex::new(replies.into()),
            calls: AtomicUsize::new(0),
            streams: std::sync::Mutex::new(None),
            revoke: std::sync::Mutex::new(None),
        }
    }
}

#[async_trait]
impl Provider for ResponseProvider {
    fn name(&self) -> &str {
        "openai"
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            streaming: self.streams.lock().unwrap().is_some(),
            ..Default::default()
        }
    }
    async fn chat(&self, request: ChatRequest) -> anyhow::Result<ChatResponse> {
        if is_memory_post_turn_extractor_request(&request) {
            assert!(request.tools.is_none());
            assert!(request.max_tokens.is_none());
            self.calls.fetch_add(1, Ordering::SeqCst);
            let revoke = self.revoke.lock().unwrap().take();
            if let Some((store, effect, token, now)) = revoke {
                store
                    .fail_native_terminal_effect(
                        &effect,
                        &token,
                        "test_claim_revoked",
                        "test claim revoked during request",
                        false,
                        now,
                        now,
                    )
                    .await?;
            }
            return Ok(self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("finite scripted response budget"));
        }
        Ok(text_response(json!({
            "intent":"normal", "recall":"allow", "prompt":"full", "readTools":"allow",
            "rememberTool":"allow", "forgetTool":"allow", "postTurnExtraction":"allow",
            "activeMemory":"allow", "explicitRemember":false, "explicitForget":false,
            "forgetTargetHint":null, "language":"en", "confidence":0.9, "reasonCode":"default_allow_read"
        }).to_string()))
    }
    async fn stream_chat(
        &self,
        request: ChatRequest,
    ) -> anyhow::Result<futures_util::stream::BoxStream<'static, anyhow::Result<StreamChunk>>> {
        assert!(is_memory_post_turn_extractor_request(&request));
        assert!(request.tools.is_none());
        self.calls.fetch_add(1, Ordering::SeqCst);
        let chunks = self
            .streams
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .pop_front()
            .expect("finite stream response budget");
        Ok(futures_util::stream::iter(chunks.into_iter().map(Ok)).boxed())
    }
}

struct Fixture {
    harness: MemoryAgentE2eHarness,
    provider: Arc<ResponseProvider>,
    effect_id: String,
    now: i64,
}

async fn fixture(replies: Vec<ChatResponse>) -> Fixture {
    fixture_with_write_failure(replies, false).await
}

async fn fixture_with_write_failure(replies: Vec<ChatResponse>, fail_write: bool) -> Fixture {
    let provider = Arc::new(ResponseProvider::new(replies));
    let registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        provider.clone(),
    ));
    fixture_with_registry(provider, registry, "openai", fail_write).await
}

async fn fixture_with_registry(
    provider: Arc<ResponseProvider>,
    registry: Arc<pioneer_provider::ProviderRegistry>,
    provider_name: &str,
    fail_write: bool,
) -> Fixture {
    let harness = setup_memory_agent_e2e_harness("response_validation", registry).await;
    if fail_write {
        let bridge = Arc::new(GatewayMemoryProvider::new(Arc::downgrade(
            &harness.processor,
        )));
        let write = Arc::new(FailOnceAfterWrite {
            bridge: bridge.clone(),
            calls: AtomicUsize::new(0),
        });
        let runtime =
            crate::hook_runtime::GatewayHookRuntimeBuilder::new(harness.crud_store.clone())
                .with_crud_run_store()
                .install(pioneer_memory::hooks::package(
                    bridge.clone(),
                    Some(write),
                    Some(bridge),
                    None,
                    None,
                    harness
                        .processor
                        .agent_manager
                        .memory_tool_bundle_artifact_store(),
                    harness.processor.memory_loop_config(),
                ))
                .unwrap()
                .build();
        harness
            .processor
            .agent_manager
            .set_hook_runtime(Some(runtime))
            .await;
    }
    let params = test_task_create_params(
        &harness.workspace_id,
        THREAD,
        TURN,
        "My name is Alexander",
        3,
    );
    ensure_task_create_parent_turn_for_test(&harness.processor, &params)
        .await
        .unwrap();
    let runtime = harness
        .processor
        .agent_manager
        .capture_post_turn_runtime()
        .await
        .unwrap()
        .unwrap();
    let mut preparation = runtime
        .prepare_completed_turn_hook(pioneer_agent::post_turn::CompletedTurnHookInput {
            workspace_id: harness.workspace_id.clone(),
            thread_id: THREAD.to_owned(),
            turn_id: TURN.to_owned(),
            runtime_context: Default::default(),
            input: pioneer_hooks::TurnPostTurnHookInput::from_parts_with_model(
                pioneer_hooks::TurnPostTurnStatus::Succeeded,
                Some("test-model"),
                Some(provider_name),
                Some("My name is Alexander"),
                Some("Understood."),
                None::<&str>,
                vec![],
                vec![],
                Default::default(),
            ),
        })
        .await
        .unwrap()
        .unwrap();
    preparation.effects[0].max_attempts = 3;
    let effect_id = preparation.effects[0].effect_id.clone();
    let now = now_timestamp_secs();
    harness
        .crud_store
        .prepare_native_terminal_effects(preparation, now)
        .await
        .unwrap();
    let (_, mut turn) = harness
        .crud_store
        .get_turn(THREAD, TURN)
        .await
        .unwrap()
        .unwrap();
    turn.status = TurnStatus::Completed;
    harness
        .crud_store
        .materialize_turn_completed(
            pioneer_protocol::TurnCompletedNotification {
                workspace_id: harness.workspace_id.clone(),
                thread_id: THREAD.to_owned(),
                turn,
            },
            now,
        )
        .await
        .unwrap();
    Fixture {
        harness,
        provider,
        effect_id,
        now,
    }
}

impl Fixture {
    async fn row(&self) -> pioneer_entity::native_terminal_effect_outbox::Model {
        // Same read-only fixture inspection used by existing outbox regression tests.
        pioneer_entity::native_terminal_effect_outbox::Entity::find_by_id(self.effect_id.clone())
            .one(&self.harness.crud_store.database_connection())
            .await
            .unwrap()
            .unwrap()
    }
    async fn attempt(&self) {
        assert_eq!(
            self.harness
                .processor
                .process_due_native_terminal_effects(self.now + 100_000, 1)
                .await
                .unwrap(),
            1
        );
    }
    async fn no_memory(&self) {
        assert!(
            self.harness
                .crud_store
                .list_agent_memory_records(pioneer_crud::AgentMemoryListFilter {
                    scopes: vec![
                        user_memory_scope(),
                        workspace_memory_scope(&self.harness.workspace_id)
                    ],
                    ..Default::default()
                })
                .await
                .unwrap()
                .is_empty()
        );
        assert!(
            self.harness
                .crud_store
                .list_agent_memory_candidates(pioneer_crud::AgentMemoryCandidateListFilter {
                    scopes: vec![
                        user_memory_scope(),
                        workspace_memory_scope(&self.harness.workspace_id)
                    ],
                    ..Default::default()
                })
                .await
                .unwrap()
                .is_empty()
        );
    }
    async fn seed_checkpoint(&self, raw: &str) -> String {
        let claim = self
            .harness
            .crud_store
            .claim_due_native_terminal_effects(self.now, 90, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let checkpoint = json!({"schema_version": 1, "raw_json": raw, "model": "test-model", "model_provider": "openai"}).to_string();
        self.harness
            .crud_store
            .store_native_terminal_effect_handler_checkpoint(
                &self.effect_id,
                &claim.claim_token,
                &checkpoint,
                self.now,
            )
            .await
            .unwrap();
        self.harness
            .crud_store
            .fail_native_terminal_effect(
                &self.effect_id,
                &claim.claim_token,
                "memory.post_turn_extractor.write_failed",
                "injected temporary write failure",
                true,
                self.now,
                self.now,
            )
            .await
            .unwrap();
        checkpoint
    }
}

fn valid_fact() -> String {
    json!({"facts": [{
        "semantic": {"intent":"explicit_store","explicitness":"explicit","category":"identity","subject":"current_user","attribute":"name","scope_hint":"user_global","durability":"long_lived","sensitivity":"personal","certainty":"high"},
        "ontology": {"fact_class":"user_identity","lifetime_class":"long_lived","evidence_class":"direct_user_assertion","proposed_ownership_class":"durable_user_memory"},
        "content":"User name is Alexander", "value":"Alexander",
        "evidence":{"source_ref":"turn.post_turn:user","quote_or_span":"My name is Alexander","extractor_reason":"Direct assertion"}
    }]}).to_string()
}

#[test]
fn memory_response_invalid_then_valid_durable_attempt() {
    run_gateway_message_test("memory_response_invalid_then_valid", || async {
        for raw in [
            format!("{{\"facts\":{CANARY}}}"),
            format!("{{\"facts\":\"{CANARY}\"}}"),
        ] {
            let f = fixture_with_write_failure(
                vec![text_response(raw), text_response(valid_fact())],
                true,
            )
            .await;
            f.attempt().await;
            let first = f.row().await;
            assert_eq!(first.status, "retry_wait");
            assert!(first.handler_checkpoint_json.is_none());
            assert_eq!(
                first.last_error_code.as_deref(),
                Some("memory.post_turn_extractor.fresh_response_invalid")
            );
            let message = first.last_error_message.unwrap();
            assert!(!message.contains(CANARY));
            for field in [
                "provider=openai",
                "model=test-model",
                "failure_stage=fresh_response_validation",
                "failure_class=",
                "termination=complete",
                "parse_category=",
            ] {
                assert!(message.contains(field), "{message}");
            }
            f.no_memory().await;
            f.attempt().await;
            assert_eq!(f.provider.calls.load(Ordering::SeqCst), 2);
            let row = f.row().await;
            assert_eq!(row.status, "retry_wait");
            let checkpoint = row.handler_checkpoint_json.unwrap();
            assert!(checkpoint.contains("raw_json"));
            f.attempt().await;
            assert_eq!(f.row().await.status, "succeeded");
            assert_eq!(f.provider.calls.load(Ordering::SeqCst), 2);
            let records = f
                .harness
                .crud_store
                .list_agent_memory_records(pioneer_crud::AgentMemoryListFilter {
                    scopes: vec![user_memory_scope()],
                    ..Default::default()
                })
                .await
                .unwrap();
            assert_eq!(records.len(), 1);
            let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
        }
    });
}

#[test]
fn memory_response_budget_is_not_reopened() {
    run_gateway_message_test("memory_response_budget", || async {
        let f = fixture(
            (0..3)
                .map(|_| text_response(format!("{{\"facts\":\"{CANARY}\"}}")))
                .collect(),
        )
        .await;
        for _ in 0..3 {
            f.attempt().await;
            f.no_memory().await;
        }
        let row = f.row().await;
        assert_eq!(row.status, "unresolved");
        assert_eq!(row.attempt_count, 3);
        assert!(row.next_run_at.is_none());
        assert!(row.handler_checkpoint_json.is_none());
        assert!(!row.last_error_message.unwrap().contains(CANARY));
        assert_eq!(f.provider.calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            f.harness
                .crud_store
                .requeue_retryable_unresolved_native_terminal_effects(f.now + 7200, 10)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            f.harness
                .processor
                .process_due_native_terminal_effects(f.now + 200_000, 10)
                .await
                .unwrap(),
            0
        );
        let runs = f
            .harness
            .crud_store
            .list_hook_runs_for_turn(TURN, Some(HookPhase::TurnPostTurn), 20)
            .await
            .unwrap();
        assert!(!format!("{runs:?}").contains(CANARY));
        let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
    });
}

#[test]
fn memory_response_legacy_checkpoint_validation_and_replay() {
    run_gateway_message_test("memory_response_legacy", || async {
        for (raw, expected) in [
            (valid_fact(), "succeeded"),
            (format!("{{\"facts\":\"{CANARY}\"}}"), "unresolved"),
            (format!("{{\"facts\":{CANARY}}}"), "unresolved"),
        ] {
            let f = fixture(vec![]).await;
            let saved = f.seed_checkpoint(&raw).await;
            if expected == "succeeded" {
                let claim = f
                    .harness
                    .crud_store
                    .claim_due_native_terminal_effects(f.now + 100_000, 90, 1)
                    .await
                    .unwrap()
                    .pop()
                    .unwrap();
                for _ in 0..2 {
                    f.harness
                        .processor
                        .agent_manager
                        .execute_terminal_effect(
                            &claim.effect_id,
                            &claim.claim_token,
                            claim.runtime_generation,
                            &claim.workspace_id,
                            &claim.thread_id,
                            &claim.turn_id,
                            claim.payload.clone(),
                        )
                        .await
                        .unwrap();
                    assert_eq!(
                        f.harness
                            .crud_store
                            .native_terminal_effect_handler_checkpoint(
                                &f.effect_id,
                                &claim.claim_token
                            )
                            .await
                            .unwrap()
                            .as_deref(),
                        Some(saved.as_str())
                    );
                }
                f.harness
                    .crud_store
                    .complete_native_terminal_effect(&f.effect_id, &claim.claim_token, f.now)
                    .await
                    .unwrap();
            } else {
                f.attempt().await;
            }
            let row = f.row().await;
            assert_eq!(row.status, expected);
            if expected == "unresolved" {
                assert_eq!(row.handler_checkpoint_json.as_deref(), Some(saved.as_str()));
            }
            assert_eq!(f.provider.calls.load(Ordering::SeqCst), 0);
            if expected == "unresolved" {
                assert_eq!(
                    row.last_error_code.as_deref(),
                    Some("memory.post_turn_extractor.checkpoint_invalid")
                );
                assert!(!row.last_error_message.unwrap().contains(CANARY));
                assert!(row.next_run_at.is_none());
                f.no_memory().await;
            } else {
                let records = f
                    .harness
                    .crud_store
                    .list_agent_memory_records(pioneer_crud::AgentMemoryListFilter {
                        scopes: vec![user_memory_scope()],
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                assert_eq!(records.len(), 1);
                assert_eq!(
                    f.harness
                        .processor
                        .process_due_native_terminal_effects(f.now + 200_000, 10)
                        .await
                        .unwrap(),
                    0
                );
            }
            let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
        }
    });
}

#[test]
fn memory_response_empty_and_semantically_rejected_are_successful() {
    run_gateway_message_test("memory_response_empty_rejected", || async {
        let mut rejected: serde_json::Value = serde_json::from_str(&valid_fact()).unwrap();
        rejected["facts"][0]["evidence"] = json!({});
        for raw in [r#"{"facts":[]}"#.to_owned(), rejected.to_string()] {
            let f = fixture(vec![text_response(raw)]).await;
            f.attempt().await;
            assert_eq!(f.row().await.status, "succeeded");
            assert_eq!(f.provider.calls.load(Ordering::SeqCst), 1);
            f.no_memory().await;
            let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
        }
    });
}

#[test]
fn memory_response_stale_claim_cannot_publish_checkpoint_or_memory() {
    run_gateway_message_test("memory_response_claim", || async {
        let f = fixture(vec![text_response(valid_fact())]).await;
        let old = f
            .harness
            .crud_store
            .claim_due_native_terminal_effects(f.now, 1, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let current = f
            .harness
            .crud_store
            .claim_due_native_terminal_effects(f.now + 2, 90, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let bridge = GatewayMemoryProvider::new(Arc::downgrade(&f.harness.processor));
        let context = MemoryPostTurnExtractorContext {
            workspace_id: f.harness.workspace_id.clone(),
            thread_id: THREAD.to_owned(),
            turn_id: TURN.to_owned(),
            mode: ThreadMode::Agent,
            model: Some("test-model".to_owned()),
            model_provider: Some("openai".to_owned()),
            durable_terminal_effect: Some(MemoryDurableTerminalEffectClaim {
                effect_id: f.effect_id.clone(),
                claim_token: old.claim_token.clone(),
            }),
        };
        assert!(
            bridge
                .extract_post_turn_memory_json(
                    context,
                    MemoryPostTurnExtractorRequest {
                        user_text: "My name is Alexander".to_owned(),
                        assistant_text: "Understood".to_owned(),
                        tool_events_summary: String::new(),
                        domain_events_summary: String::new(),
                        manifest: MemoryManifest::default(),
                        max_facts: 4,
                    }
                )
                .await
                .is_err()
        );
        assert!(
            f.harness
                .crud_store
                .store_native_terminal_effect_handler_checkpoint(
                    &f.effect_id,
                    &old.claim_token,
                    "{}",
                    f.now + 2
                )
                .await
                .is_err()
        );
        assert!(
            f.harness
                .crud_store
                .native_terminal_effect_handler_checkpoint(&f.effect_id, &current.claim_token)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(f.provider.calls.load(Ordering::SeqCst), 0);
        f.no_memory().await;
        let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
    });
}

struct FailOnceAfterWrite {
    bridge: Arc<GatewayMemoryProvider>,
    calls: AtomicUsize,
}

#[async_trait]
impl AgentMemoryWriteProvider for FailOnceAfterWrite {
    async fn load_memory_manifest(
        &self,
        context: MemoryTurnContext,
        request: MemoryManifestRequest,
    ) -> Result<MemoryManifest, String> {
        self.bridge.load_memory_manifest(context, request).await
    }
    async fn write_semantic_memory(
        &self,
        context: MemoryTurnContext,
        params: pioneer_protocol::MemorySemanticWriteParams,
    ) -> Result<pioneer_protocol::MemorySemanticWriteResponse, String> {
        let result = self.bridge.write_semantic_memory(context, params).await?;
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            return Err("injected failure after semantic commit".to_owned());
        }
        Ok(result)
    }
}

#[test]
fn memory_response_write_failure_replays_without_model_or_duplicate_memory() {
    run_gateway_message_test("memory_response_write_replay", || async {
        let f = fixture_with_write_failure(vec![text_response(valid_fact())], true).await;
        f.attempt().await;
        let row = f.row().await;
        assert_eq!(row.status, "retry_wait");
        assert_eq!(
            row.last_error_code.as_deref(),
            Some("memory.post_turn_extractor.write_failed")
        );
        let checkpoint = row.handler_checkpoint_json.unwrap();
        let claim = f
            .harness
            .crud_store
            .claim_due_native_terminal_effects(f.now + 100_000, 90, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        f.harness
            .processor
            .agent_manager
            .execute_terminal_effect(
                &claim.effect_id,
                &claim.claim_token,
                claim.runtime_generation,
                &claim.workspace_id,
                &claim.thread_id,
                &claim.turn_id,
                claim.payload,
            )
            .await
            .unwrap();
        assert_eq!(
            f.harness
                .crud_store
                .native_terminal_effect_handler_checkpoint(&f.effect_id, &claim.claim_token)
                .await
                .unwrap()
                .as_deref(),
            Some(checkpoint.as_str())
        );
        f.harness
            .crud_store
            .complete_native_terminal_effect(&f.effect_id, &claim.claim_token, f.now)
            .await
            .unwrap();
        assert_eq!(f.row().await.status, "succeeded");
        assert_eq!(f.provider.calls.load(Ordering::SeqCst), 1);
        let records = f
            .harness
            .crud_store
            .list_agent_memory_records(pioneer_crud::AgentMemoryListFilter {
                scopes: vec![user_memory_scope()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
    });
}

#[test]
fn memory_response_claim_lost_during_provider_request_fences_checkpoint() {
    run_gateway_message_test("memory_response_claim_during_provider", || async {
        let f = fixture(vec![text_response(valid_fact())]).await;
        let claim = f
            .harness
            .crud_store
            .claim_due_native_terminal_effects(f.now, 90, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        *f.provider.revoke.lock().unwrap() = Some((
            f.harness.crud_store.clone(),
            f.effect_id.clone(),
            claim.claim_token.clone(),
            f.now,
        ));
        let result = f
            .harness
            .processor
            .agent_manager
            .execute_terminal_effect(
                &claim.effect_id,
                &claim.claim_token,
                claim.runtime_generation,
                &claim.workspace_id,
                &claim.thread_id,
                &claim.turn_id,
                claim.payload,
            )
            .await;
        assert!(result.is_err());
        assert!(f.row().await.handler_checkpoint_json.is_none());
        assert_eq!(f.provider.calls.load(Ordering::SeqCst), 1);
        f.no_memory().await;
        let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
    });
}

#[test]
fn memory_response_completion_failure_never_reaches_checkpoint() {
    run_gateway_message_test("memory_response_completion_checkpoint", || async {
        for termination in [
            ProviderTermination::Length,
            ProviderTermination::ContentFiltered,
            ProviderTermination::Safety,
            ProviderTermination::Cancelled,
            ProviderTermination::ProviderError,
            ProviderTermination::Unknown(CANARY.to_owned()),
            ProviderTermination::ToolCalls,
        ] {
            let mut reply = text_response(r#"{"facts":[]}"#);
            reply.termination = termination;
            let f = fixture(vec![reply]).await;
            f.attempt().await;
            let row = f.row().await;
            assert_eq!(row.status, "unresolved");
            assert!(row.handler_checkpoint_json.is_none());
            assert!(row.next_run_at.is_none());
            let message = row.last_error_message.unwrap();
            assert!(message.contains("failure_stage=transport_completion"));
            assert!(message.contains("termination="));
            assert!(!message.contains(CANARY));
            assert_eq!(f.provider.calls.load(Ordering::SeqCst), 1);
            f.no_memory().await;
            let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
        }
    });
}

#[test]
fn memory_response_canary_is_absent_from_sentry_events_and_breadcrumbs() {
    run_gateway_message_test("memory_response_sentry_canary", || async {
        use sentry::SentryFutureExt;
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;
        // A local transport collects envelopes; this fixture has no network transport.
        let transport = sentry::test::TestTransport::new();
        let client = sentry::Client::from(sentry::ClientOptions {
            dsn: Some("https://public@sentry.invalid/1".parse().unwrap()),
            transport: Some(Arc::new(transport.clone())),
            ..Default::default()
        });
        let hub = Arc::new(sentry::Hub::new(Some(Arc::new(client)), Default::default()));
        let subscriber =
            tracing_subscriber::registry().with(sentry::integrations::tracing::layer());
        async {
            let f = fixture(
                (0..3)
                    .map(|_| text_response(format!("{{\"facts\":\"{CANARY}\"}}")))
                    .collect(),
            )
            .await;
            for _ in 0..3 {
                f.attempt().await;
            }
            assert!(!f.row().await.last_error_message.unwrap().contains(CANARY));
            let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
        }
        .with_subscriber(subscriber)
        .bind_hub(hub)
        .await;
        let events = transport.fetch_and_clear_events();
        assert!(!events.is_empty(), "terminal ERROR must be captured");
        let serialized = serde_json::to_string(&events).unwrap();
        assert!(!serialized.contains(CANARY));
        assert!(serialized.contains("fresh_response_validation"));
        assert!(serialized.contains("test-model"));
        assert!(serialized.contains("structure"));
    });
}

#[test]
fn memory_response_stream_eof_has_bounded_retry_and_never_checkpoints_partial_text() {
    run_gateway_message_test("memory_response_stream_eof", || async {
        for eventually_complete in [true, false] {
            let f = fixture(vec![]).await;
            let partial = || vec![StreamChunk::delta(r#"{"facts":[]}"#)];
            let last = if eventually_complete {
                vec![
                    StreamChunk::delta(r#"{"facts":[]}"#),
                    StreamChunk::final_chunk_with(ProviderTermination::Complete),
                ]
            } else {
                partial()
            };
            *f.provider.streams.lock().unwrap() = Some(vec![partial(), partial(), last].into());
            for index in 0..3 {
                f.attempt().await;
                let row = f.row().await;
                if index < 2 || !eventually_complete {
                    assert!(row.handler_checkpoint_json.is_none());
                    assert_eq!(
                        row.last_error_code.as_deref(),
                        Some("memory.post_turn_extractor.completion_stream_truncated")
                    );
                    assert!(
                        row.last_error_message
                            .unwrap()
                            .contains("failure_stage=transport_completion")
                    );
                }
                f.no_memory().await;
            }
            assert_eq!(f.provider.calls.load(Ordering::SeqCst), 3);
            assert_eq!(
                f.row().await.status,
                if eventually_complete {
                    "succeeded"
                } else {
                    "unresolved"
                }
            );
            assert_eq!(
                f.harness
                    .crud_store
                    .requeue_retryable_unresolved_native_terminal_effects(f.now + 7200, 10)
                    .await
                    .unwrap(),
                0
            );
            assert_eq!(
                f.harness
                    .processor
                    .process_due_native_terminal_effects(f.now + 200_000, 10)
                    .await
                    .unwrap(),
                0
            );
            let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
        }
    });
}

#[test]
fn memory_response_unexpected_tools_and_oversized_response_never_checkpoint() {
    run_gateway_message_test("memory_response_tools_size", || async {
        let mut tools = text_response(r#"{"facts":[]}"#);
        tools.tool_calls.push(ProviderToolCall {
            id: CANARY.to_owned(),
            name: CANARY.to_owned(),
            arguments: CANARY.to_owned(),
        });
        for reply in [tools, text_response("x".repeat(64 * 1024 + 1))] {
            let f = fixture(vec![reply]).await;
            f.attempt().await;
            let row = f.row().await;
            assert_eq!(row.status, "unresolved");
            assert!(row.handler_checkpoint_json.is_none());
            assert!(row.next_run_at.is_none());
            assert!(!row.last_error_message.unwrap().contains(CANARY));
            assert_eq!(f.provider.calls.load(Ordering::SeqCst), 1);
            f.no_memory().await;
            let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
        }
    });
}

// This fixture exercises the actual HTTP -> SSE decoder -> endpoint-redaction
// wrapper -> Gateway -> durable worker path. The server is started only by tests.
#[test]
fn memory_openrouter_incomplete_adapter_stream_exhausts_only_original_budget() {
    run_gateway_message_test("memory_openrouter_completion", || async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        for done in [false, true] {
            for valid_json in [false, true] {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let url = format!(
                    "http://{}/private-canary/v1",
                    listener.local_addr().unwrap()
                );
                let calls = Arc::new(AtomicUsize::new(0));
                let observed = calls.clone();
                let server = tokio::spawn(async move {
                    for _ in 0..3 {
                        let (mut socket, _) = listener.accept().await.unwrap();
                        let mut request = Vec::new();
                        let mut buf = [0u8; 4096];
                        // Consume the complete request so closing the connection is
                        // a clean HTTP EOF, rather than a TCP reset from unread data.
                        loop {
                            let n = socket.read(&mut buf).await.unwrap();
                            assert!(n > 0);
                            request.extend_from_slice(&buf[..n]);
                            if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                                let headers = String::from_utf8_lossy(&request[..end]);
                                let length: usize = headers
                                    .lines()
                                    .find_map(|line| {
                                        let (key, value) = line.split_once(':')?;
                                        key.eq_ignore_ascii_case("content-length")
                                            .then(|| value.trim().parse().unwrap())
                                    })
                                    .unwrap_or(0);
                                if request.len() >= end + 4 + length {
                                    break;
                                }
                            }
                        }
                        observed.fetch_add(1, Ordering::SeqCst);
                        let mut body = String::new();
                        if valid_json {
                            body.push_str(&format!("data: {}\n\n", json!({"choices":[{"delta":{"content":"{\"facts\":[]}"},"finish_reason":null}]})));
                        }
                        if done {
                            body.push_str("data: [DONE]\n\n");
                        }
                        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                        socket.shutdown().await.unwrap();
                    }
                });
                let registry = Arc::new(pioneer_provider::ProviderRegistry::new_scoped_fallible_with_timeout_policy_proxy_and_base_url(
                    |_, _| Ok("test-key".into()), |_, _| Ok(None),
                    move |_, _| Ok(Some(url.clone())), pioneer_provider::ProviderTimeoutPolicy::default(),
                ));
                let f = fixture_with_registry(
                    Arc::new(ResponseProvider::new(vec![])),
                    registry,
                    "openrouter",
                    false,
                )
                .await;
                for index in 0..3 {
                    f.attempt().await;
                    let row = f.row().await;
                    assert_eq!(
                        row.status,
                        if index < 2 {
                            "retry_wait"
                        } else {
                            "unresolved"
                        }
                    );
                    assert!(row.handler_checkpoint_json.is_none());
                    assert_eq!(
                        row.last_error_code.as_deref(),
                        Some("memory.post_turn_extractor.completion_stream_truncated")
                    );
                    let message = row.last_error_message.unwrap();
                    for value in [
                        "provider=openrouter",
                        "model=test-model",
                        "failure_stage=transport_completion",
                        "failure_class=stream_truncated",
                    ] {
                        assert!(message.contains(value), "missing safe metadata: {value}");
                    }
                    assert!(!message.contains("private-canary"));
                    f.no_memory().await;
                }
                server.await.unwrap();
                assert_eq!(calls.load(Ordering::SeqCst), 3);
                let exhausted = f.row().await;
                assert_eq!(exhausted.status, "unresolved");
                assert!(exhausted.next_run_at.is_none());
                assert_eq!(exhausted.max_attempts, 3);
                // Prove the first scan is inside the existing recovery window;
                // zero reopened rows must follow from the failure code, not age.
                let recovery_now = f.now + 7200;
                assert!(exhausted.completed_at.unwrap().timestamp() <= recovery_now - 3600);
                assert!(exhausted.prepared_at.timestamp() >= recovery_now - 86400);
                for now in [f.now + 7200, f.now + 172800] {
                    assert_eq!(
                        f.harness
                            .crud_store
                            .requeue_retryable_unresolved_native_terminal_effects(now, 10)
                            .await
                            .unwrap(),
                        0
                    );
                    assert_eq!(
                        f.harness
                            .processor
                            .process_due_native_terminal_effects(now, 10)
                            .await
                            .unwrap(),
                        0
                    );
                }
                assert_eq!(f.row().await.attempt_count, 3);
                let _ = std::fs::remove_dir_all(&f.harness.runtime_home);
            }
        }
    });
}
