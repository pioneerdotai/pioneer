use super::*;
use crate::hook_runtime::GatewayHookRuntimeBuilder;
use crate::memory_tools::GatewayMemoryProvider;
use pioneer_memory::MemoryWriteFailure;
use pioneer_memory::hooks::{AgentMemoryWriteProvider, MemoryManifest, MemoryManifestRequest};
use pioneer_protocol::{
    MemoryAttribute, MemorySemanticWriteParams, MemorySemanticWriteResponse, MemorySubject,
    MemoryWriteRelation,
};
use std::sync::Mutex;

const SOURCE_TEXT: &str = "Меня зовут CheckpointName. Отвечай по-русски. Проверяй изменения.";

#[derive(Clone, Copy)]
enum WriteMode {
    Pass,
    Permanent,
    Transient,
    Mixed,
}

// Only the allowed write-failure seam is replaced. Successful writes and
// manifest reads retain Gateway authorization and the real MemoryService.
struct ControlledCheckpointWrites {
    gateway: Arc<GatewayMemoryProvider>,
    mode: Mutex<WriteMode>,
    attempts: Mutex<Vec<(MemorySubject, MemoryAttribute)>>,
    relations: Mutex<Vec<MemoryWriteRelation>>,
}

#[async_trait]
impl AgentMemoryWriteProvider for ControlledCheckpointWrites {
    async fn load_memory_manifest(
        &self,
        context: MemoryTurnContext,
        request: MemoryManifestRequest,
    ) -> Result<MemoryManifest, String> {
        self.gateway.load_memory_manifest(context, request).await
    }

    async fn write_semantic_memory(
        &self,
        context: MemoryTurnContext,
        params: MemorySemanticWriteParams,
    ) -> Result<MemorySemanticWriteResponse, MemoryWriteFailure> {
        self.attempts
            .lock()
            .unwrap()
            .push((params.semantic.subject, params.semantic.attribute));
        let mode = *self.mode.lock().unwrap();
        let failure = match (mode, params.semantic.attribute) {
            (WriteMode::Permanent, _) => Some(MemoryWriteFailure::AuthorizationOrDomain),
            (WriteMode::Transient, _) => Some(MemoryWriteFailure::StorageTransient),
            (WriteMode::Mixed, MemoryAttribute::PreferredLanguage) => {
                Some(MemoryWriteFailure::AuthorizationOrDomain)
            }
            (WriteMode::Mixed, MemoryAttribute::ReviewStyle) => {
                Some(MemoryWriteFailure::StorageTransient)
            }
            _ => None,
        };
        if let Some(failure) = failure {
            return Err(failure);
        }
        let response = self.gateway.write_semantic_memory(context, params).await?;
        self.relations.lock().unwrap().push(response.relation);
        Ok(response)
    }
}

fn name_fact() -> serde_json::Value {
    json!({
        "semantic": {"intent":"explicit_store", "explicitness":"explicit", "category":"identity",
            "subject":"current_user", "attribute":"name", "scope_hint":"user_global",
            "durability":"long_lived", "sensitivity":"personal", "certainty":"high"},
        "ontology": {"fact_class":"user_identity", "lifetime_class":"long_lived",
            "evidence_class":"direct_user_assertion", "proposed_ownership_class":"durable_user_memory"},
        "content":"Имя пользователя: CheckpointName", "value":"CheckpointName",
        "evidence":{"source_ref":"turn.post_turn:user", "quote_or_span":"Меня зовут CheckpointName",
            "extractor_reason":"Direct assertion"}
    })
}

fn communication_fact(attribute: &str, value: &str, quote: &str) -> serde_json::Value {
    let mut fact = name_fact();
    fact["semantic"]["category"] = json!("communication_style");
    fact["semantic"]["attribute"] = json!(attribute);
    fact["ontology"]["fact_class"] = json!("communication_preference");
    fact["content"] = json!(format!("Постоянное предпочтение пользователя: {value}"));
    fact["value"] = json!(value);
    fact["evidence"]["quote_or_span"] = json!(quote);
    fact
}

fn invalid_facts() -> Vec<serde_json::Value> {
    let mut subject = name_fact();
    subject["semantic"]["subject"] = json!("project");
    subject["semantic"]["subject_key"] = json!(" -- 🚀 ");
    let mut custom_subject = name_fact();
    custom_subject["semantic"]["subject"] = json!("custom");
    let mut custom_attribute = name_fact();
    custom_attribute["semantic"]["attribute"] = json!("custom");
    vec![subject, custom_subject, custom_attribute]
}

struct LegacyCheckpointFixture {
    // Keep the weak Gateway provider's owning processor and transport alive.
    _harness: MemoryAgentE2eHarness,
    background: Arc<MessageProcessor>,
    writes: Arc<ControlledCheckpointWrites>,
    model: Arc<CaptureSummaryProvider>,
    turn_id: String,
    effect_id: String,
    legacy_completed_at: i64,
    legacy_max_attempts: u16,
    stale_claim: String,
}

impl LegacyCheckpointFixture {
    fn store(&self) -> &CrudStore {
        self.background.crud_store.as_ref()
    }

    async fn status(&self) -> pioneer_crud::NativeTerminalEffectStatusRecord {
        self.store()
            .native_terminal_effect_status(&self.effect_id)
            .await
            .unwrap()
            .unwrap()
    }

    async fn reopen_and_execute(&self) {
        let reopen_at = self.legacy_completed_at + 3_601;
        assert_eq!(
            self.store()
                .requeue_retryable_unresolved_native_terminal_effects(reopen_at, 1)
                .await
                .unwrap(),
            1
        );
        let pending = self.status().await;
        assert_eq!(pending.status, "retry_wait");
        assert_eq!(pending.max_attempts, self.legacy_max_attempts);
        assert_eq!(pending.attempt_count, pending.max_attempts - 1);
        assert_eq!(
            pending.last_error_code.as_deref(),
            Some("memory.post_turn_extractor.legacy_write_revalidate")
        );
        // An old delivery token cannot read the checkpoint or complete work.
        assert!(
            self.store()
                .native_terminal_effect_handler_checkpoint(&self.effect_id, &self.stale_claim)
                .await
                .is_err()
        );
        assert!(
            !self
                .store()
                .complete_native_terminal_effect(&self.effect_id, &self.stale_claim, reopen_at)
                .await
                .unwrap()
        );
        assert_eq!(
            self.background
                .process_due_native_terminal_effects(reopen_at, 1)
                .await
                .unwrap(),
            1
        );
        assert_eq!(self.status().await.attempt_count, self.legacy_max_attempts);
        assert_eq!(
            self.model.call_count(),
            0,
            "a usable checkpoint must bypass model requests"
        );
        // None of the rejected semantic identities may reach the write seam.
        assert!(
            self.writes
                .attempts
                .lock()
                .unwrap()
                .iter()
                .all(|(subject, attribute)| {
                    *subject == MemorySubject::CurrentUser && *attribute != MemoryAttribute::Custom
                })
        );
    }

    async fn extractor_run(&self) -> pioneer_crud::HookRunRecord {
        let runs = self
            .store()
            .list_hook_runs_for_turn(&self.turn_id, Some(HookPhase::TurnPostTurn), 8)
            .await
            .unwrap();
        runs.into_iter()
            .find(|run| run.hook_id.as_str() == "memory.post_turn_extractor")
            .unwrap()
    }

    async fn assert_safe_diagnostics(&self) {
        let run = self.extractor_run().await;
        let diagnostics = serde_json::to_string(&run.diagnostic_previews).unwrap();
        let error = serde_json::to_string(&run.error).unwrap();
        let status = self.status().await;
        let message = status.last_error_message.unwrap_or_default();
        for text in [&diagnostics, &error, &message] {
            assert!(!text.contains("CheckpointName"));
            assert!(!text.contains("Отвечай по-русски"));
            assert!(!text.contains("quote_or_span"));
            assert!(!text.contains("raw_json"));
        }
    }

    async fn memory_ids(&self) -> Vec<String> {
        self.store()
            .list_agent_memory_records(AgentMemoryListFilter {
                scopes: vec![user_memory_scope()],
                limit: Some(8),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.id)
            .collect()
    }
}

async fn legacy_checkpoint_fixture(
    case: &str,
    facts: Vec<serde_json::Value>,
    mode: WriteMode,
) -> LegacyCheckpointFixture {
    // A deliberately unusable extraction response makes accidental fresh
    // requests visible independently of the call-count assertion.
    let model = Arc::new(CaptureSummaryProvider::new("MODEL_MUST_NOT_BE_CALLED"));
    let registry = Arc::new(pioneer_provider::ProviderRegistry::with_provider(
        "openai",
        model.clone(),
    ));
    let mut config = test_tool_loop_config();
    config.memory.post_turn_extractor.enabled = true;
    config.memory.post_turn_extractor.provider_enabled = true;
    config.memory.post_turn_extractor.proactive_writes_enabled = true;
    config.memory.post_turn_extractor.provider_name = Some("openai".into());
    config.memory.post_turn_extractor.model = Some("test-model".into());
    let harness =
        setup_memory_agent_e2e_harness_with_tool_loop_config(case, registry, config).await;
    let background = harness
        .processor
        .with_database_class(pioneer_sqlite::SqliteWriteClass::Maintenance);
    let gateway = Arc::new(GatewayMemoryProvider::new(Arc::downgrade(&background)));
    let writes = Arc::new(ControlledCheckpointWrites {
        gateway: gateway.clone(),
        mode: Mutex::new(mode),
        attempts: Mutex::new(Vec::new()),
        relations: Mutex::new(Vec::new()),
    });
    let runtime = GatewayHookRuntimeBuilder::new(background.crud_store.clone())
        .with_crud_run_store()
        .install(pioneer_memory::hooks::package(
            gateway.clone(),
            Some(writes.clone()),
            Some(gateway),
            None,
            None,
            background.agent_manager.memory_tool_bundle_artifact_store(),
            background.memory_loop_config(),
        ))
        .unwrap()
        .build();
    install_recoverable_test_hook_runtime(&background, runtime).await;

    let turn_id = format!("turn-{case}");
    let thread_id = format!("thread-{case}");
    seed_cli_runtime_turn_with_text(
        background.crud_store.as_ref(),
        &harness.workspace_id,
        "codex",
        "codex",
        &thread_id,
        &turn_id,
        &format!("native-{case}"),
        SOURCE_TEXT,
    )
    .await;
    let binding = background
        .crud_store
        .get_cli_runtime_turn_binding(&turn_id)
        .await
        .unwrap()
        .unwrap();
    let answer = TurnItem::AgentMessage {
        id: "checkpoint-final".into(),
        text: "Принято".into(),
        phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
        markdown: None,
        markdown_version: None,
    };
    background
        .prepare_cli_post_turn_hook(&binding, 1, Some(&answer))
        .await
        .unwrap();
    let (_, mut turn) = background
        .crud_store
        .get_turn(&thread_id, &turn_id)
        .await
        .unwrap()
        .unwrap();
    turn.status = TurnStatus::Completed;
    background
        .crud_store
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: harness.workspace_id.clone(),
                thread_id,
                turn,
            },
            chrono::Utc::now().timestamp(),
        )
        .await
        .unwrap();

    let effect_id = format!("{turn_id}:terminal-effect:post-turn");
    // Seed ONLY the historical unclassified failure. The new error code below
    // is always obtained from the real durable worker/handler, never supplied
    // to fail_native_terminal_effect by the test.
    let checkpoint = json!({"schema_version":1, "raw_json":json!({"facts":facts}).to_string(),
        "model":"test-model", "model_provider":"openai"})
    .to_string();
    let start = chrono::Utc::now().timestamp();
    let mut now = start;
    let mut generation = None;
    let (legacy_completed_at, legacy_max_attempts, stale_claim) = loop {
        let claim = background
            .crud_store
            .claim_due_native_terminal_effects(now, 90, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_eq!(claim.effect_id, effect_id);
        assert_ne!(claim.runtime_generation, 0);
        assert_eq!(
            *generation.get_or_insert(claim.runtime_generation),
            claim.runtime_generation
        );
        if claim.attempt_count == 1 {
            background
                .crud_store
                .store_native_terminal_effect_handler_checkpoint(
                    &effect_id,
                    &claim.claim_token,
                    &checkpoint,
                    now,
                )
                .await
                .unwrap();
        }
        assert_eq!(
            background
                .crud_store
                .native_terminal_effect_handler_checkpoint(&effect_id, &claim.claim_token)
                .await
                .unwrap()
                .as_deref(),
            Some(checkpoint.as_str())
        );
        assert!(
            background
                .crud_store
                .fail_native_terminal_effect(
                    &effect_id,
                    &claim.claim_token,
                    "memory.post_turn_extractor.write_failed",
                    "legacy unclassified write failure",
                    true,
                    now + 1,
                    now,
                )
                .await
                .unwrap()
        );
        if claim.attempt_count == claim.max_attempts {
            break (now, claim.max_attempts, claim.claim_token);
        }
        now += 1;
        assert!(
            now - start < 20,
            "legacy fixture is bounded by the existing budget"
        );
    };
    assert_eq!(model.call_count(), 0);
    let fixture = LegacyCheckpointFixture {
        _harness: harness,
        background,
        writes,
        model,
        turn_id,
        effect_id,
        legacy_completed_at,
        legacy_max_attempts,
        stale_claim,
    };
    assert_eq!(fixture.status().await.status, "unresolved");
    // The existing cooldown is part of the integration scenario; no sleeping
    // or mutation of prepared/completed timestamps is needed.
    assert_eq!(
        fixture
            .store()
            .requeue_retryable_unresolved_native_terminal_effects(now + 3_599, 1)
            .await
            .unwrap(),
        0
    );
    // An old checkpoint also remains outside recovery after the existing
    // one-day prepared-at window; this read-only eligibility check does not
    // consume the single compatible attempt used by the scenario below.
    assert_eq!(
        fixture
            .store()
            .requeue_retryable_unresolved_native_terminal_effects(start + 86_401, 1)
            .await
            .unwrap(),
        0
    );
    fixture
}

#[tokio::test]
async fn legacy_checkpoint_all_invalid_completes_without_model_or_write_and_stays_resolved() {
    let fixture =
        legacy_checkpoint_fixture("checkpoint-invalid", invalid_facts(), WriteMode::Pass).await;
    fixture.reopen_and_execute().await;
    assert_eq!(fixture.status().await.status, "succeeded");
    assert!(fixture.writes.attempts.lock().unwrap().is_empty());
    assert!(fixture.memory_ids().await.is_empty());
    let run = fixture.extractor_run().await;
    assert!(
        run.diagnostic_previews.iter().any(|d| d
            .message
            .as_str()
            .contains("validation_rejected=3")
            && d.message.as_str().contains("write_successes=0"))
    );
    assert_eq!(
        fixture
            .store()
            .requeue_retryable_unresolved_native_terminal_effects(
                fixture.legacy_completed_at + 7_202,
                1
            )
            .await
            .unwrap(),
        0
    );
    fixture.assert_safe_diagnostics().await;
}

#[tokio::test]
async fn legacy_checkpoint_permanent_write_result_is_persisted_without_recovery_budget() {
    let fixture = legacy_checkpoint_fixture(
        "checkpoint-permanent",
        vec![name_fact()],
        WriteMode::Permanent,
    )
    .await;
    fixture.reopen_and_execute().await;
    let status = fixture.status().await;
    assert_eq!(status.status, "unresolved");
    assert_eq!(
        status.last_error_code.as_deref(),
        Some("memory.post_turn_extractor.write_domain_rejected")
    );
    let run = fixture.extractor_run().await;
    let error = run.error.unwrap();
    assert_eq!(
        error.code.as_str(),
        status.last_error_code.as_deref().unwrap()
    );
    assert!(!error.retryable);
    assert!(!fixture.writes.attempts.lock().unwrap().is_empty());
    assert_eq!(
        fixture
            .store()
            .requeue_retryable_unresolved_native_terminal_effects(
                fixture.legacy_completed_at + 7_202,
                1
            )
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        fixture.status().await.max_attempts,
        fixture.legacy_max_attempts
    );
    fixture.assert_safe_diagnostics().await;
}

async fn assert_transient_recovery(fixture: &LegacyCheckpointFixture) {
    fixture.reopen_and_execute().await;
    let status = fixture.status().await;
    assert_eq!(status.status, "unresolved");
    assert_eq!(
        status.last_error_code.as_deref(),
        Some("memory.post_turn_extractor.write_storage_transient")
    );
    let run = fixture.extractor_run().await;
    assert!(run.error.unwrap().retryable);
    fixture.assert_safe_diagnostics().await;
    *fixture.writes.mode.lock().unwrap() = WriteMode::Pass;
    let reopen_at = fixture.legacy_completed_at + 7_202;
    assert_eq!(
        fixture
            .store()
            .requeue_retryable_unresolved_native_terminal_effects(reopen_at, 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        fixture.status().await.attempt_count,
        0,
        "only the newly classified transient failure grants normal recovery"
    );
    assert_eq!(
        fixture
            .background
            .process_due_native_terminal_effects(reopen_at, 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(fixture.status().await.status, "succeeded");
    assert_eq!(fixture.model.call_count(), 0);
    assert_eq!(
        fixture
            .store()
            .requeue_retryable_unresolved_native_terminal_effects(reopen_at + 3_601, 1)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn legacy_checkpoint_transient_write_result_enters_existing_recovery() {
    let fixture = legacy_checkpoint_fixture(
        "checkpoint-transient",
        vec![name_fact()],
        WriteMode::Transient,
    )
    .await;
    assert_transient_recovery(&fixture).await;
    assert_eq!(fixture.memory_ids().await.len(), 1);
}

#[tokio::test]
async fn legacy_checkpoint_mixed_results_replay_real_writes_without_canonical_duplicates() {
    let mut facts = invalid_facts();
    facts.push(name_fact());
    facts.push(communication_fact(
        "preferred_language",
        "Русский",
        "Отвечай по-русски",
    ));
    facts.push(communication_fact(
        "review_style",
        "Проверять изменения",
        "Проверяй изменения",
    ));
    let fixture = legacy_checkpoint_fixture("checkpoint-mixed", facts, WriteMode::Mixed).await;
    fixture.reopen_and_execute().await;
    assert_eq!(
        fixture.status().await.last_error_code.as_deref(),
        Some("memory.post_turn_extractor.write_storage_transient")
    );
    let original_ids = fixture.memory_ids().await;
    assert_eq!(
        original_ids.len(),
        1,
        "valid name is saved despite invalid/permanent/transient facts"
    );
    let attempts = fixture.writes.attempts.lock().unwrap().clone();
    assert!(attempts.contains(&(MemorySubject::CurrentUser, MemoryAttribute::Name)));
    assert!(attempts.contains(&(
        MemorySubject::CurrentUser,
        MemoryAttribute::PreferredLanguage
    )));
    assert!(attempts.contains(&(MemorySubject::CurrentUser, MemoryAttribute::ReviewStyle)));
    assert!(attempts.iter().all(|(subject, attribute)| {
        *subject == MemorySubject::CurrentUser && *attribute != MemoryAttribute::Custom
    }));
    assert!(fixture.extractor_run().await.error.unwrap().retryable);
    fixture.assert_safe_diagnostics().await;
    *fixture.writes.mode.lock().unwrap() = WriteMode::Pass;
    let reopen_at = fixture.legacy_completed_at + 7_202;
    assert_eq!(
        fixture
            .store()
            .requeue_retryable_unresolved_native_terminal_effects(reopen_at, 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        fixture
            .background
            .process_due_native_terminal_effects(reopen_at, 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(fixture.status().await.status, "succeeded");
    let ids = fixture.memory_ids().await;
    assert_eq!(ids.len(), 3);
    assert!(
        ids.contains(&original_ids[0]),
        "replay preserves the saved canonical identity"
    );
    assert!(
        fixture
            .writes
            .relations
            .lock()
            .unwrap()
            .contains(&MemoryWriteRelation::Duplicate)
    );
    assert_eq!(fixture.model.call_count(), 0);
    assert_eq!(
        fixture
            .store()
            .requeue_retryable_unresolved_native_terminal_effects(reopen_at + 3_601, 1)
            .await
            .unwrap(),
        0
    );
    fixture.assert_safe_diagnostics().await;
}
