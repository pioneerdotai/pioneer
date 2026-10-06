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

// Successful paths retain Gateway authorization and the real MemoryService.
// Manifest fault injection supplies typed raw causes to the production classifier.
struct ControlledCheckpointWrites {
    gateway: Arc<GatewayMemoryProvider>,
    mode: Mutex<WriteMode>,
    manifest_faults: Mutex<std::collections::VecDeque<anyhow::Error>>,
    manifest_started: Mutex<Option<Arc<Notify>>>,
    attempts: Mutex<Vec<(MemorySubject, MemoryAttribute)>>,
    relations: Mutex<Vec<MemoryWriteRelation>>,
}

#[async_trait]
impl AgentMemoryWriteProvider for ControlledCheckpointWrites {
    async fn load_memory_manifest(
        &self,
        context: MemoryTurnContext,
        request: MemoryManifestRequest,
    ) -> Result<MemoryManifest, pioneer_memory::MemoryManifestFailure> {
        let started = self.manifest_started.lock().unwrap().clone();
        if let Some(started) = started {
            started.notify_one();
            std::future::pending::<()>().await;
        }
        if let Some(error) = self.manifest_faults.lock().unwrap().pop_front() {
            return Err(crate::memory_tools::classify_memory_manifest_failure(
                error,
                pioneer_memory::MemoryManifestFailureStage::Active,
            ));
        }
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
                .unwrap()
                .count,
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
    legacy_recovery_fixture(case, facts, mode, false, true).await
}

async fn legacy_recovery_fixture(
    case: &str,
    facts: Vec<serde_json::Value>,
    mode: WriteMode,
    manifest_legacy: bool,
    with_checkpoint: bool,
) -> LegacyCheckpointFixture {
    recovery_fixture(case, facts, mode, manifest_legacy, with_checkpoint, true).await
}

async fn recovery_fixture(
    case: &str,
    facts: Vec<serde_json::Value>,
    mode: WriteMode,
    manifest_legacy: bool,
    with_checkpoint: bool,
    seed_legacy: bool,
) -> LegacyCheckpointFixture {
    recovery_fixture_on_workspace(
        case,
        facts,
        mode,
        manifest_legacy,
        with_checkpoint,
        seed_legacy,
        None,
    )
    .await
}

async fn recovery_fixture_on_workspace(
    case: &str,
    facts: Vec<serde_json::Value>,
    mode: WriteMode,
    manifest_legacy: bool,
    with_checkpoint: bool,
    seed_legacy: bool,
    workspace: Option<(Arc<WorkspaceManager>, Arc<CrudStore>, String)>,
) -> LegacyCheckpointFixture {
    // Checkpoint scenarios make accidental model requests visible independently
    // of call counts; fresh manifest scenarios use an empty extraction result.
    let model = Arc::new(CaptureSummaryProvider::new(
        if manifest_legacy && !with_checkpoint {
            r#"{"facts":[]}"#
        } else {
            "MODEL_MUST_NOT_BE_CALLED"
        },
    ));
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
    let harness = match workspace {
        Some(workspace) => {
            setup_memory_agent_e2e_harness_on_workspace(case, registry, config, workspace).await
        }
        None => setup_memory_agent_e2e_harness_with_tool_loop_config(case, registry, config).await,
    };
    let background = harness
        .processor
        .with_database_class(pioneer_sqlite::SqliteWriteClass::Maintenance);
    let gateway = Arc::new(GatewayMemoryProvider::new(Arc::downgrade(&background)));
    let writes = Arc::new(ControlledCheckpointWrites {
        gateway: gateway.clone(),
        mode: Mutex::new(mode),
        manifest_faults: Mutex::new(Default::default()),
        manifest_started: Mutex::new(None),
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
    if !seed_legacy {
        return LegacyCheckpointFixture {
            _harness: harness,
            background,
            writes,
            model,
            turn_id,
            effect_id,
            legacy_completed_at: start,
            legacy_max_attempts: 8,
            stale_claim: String::new(),
        };
    }
    let mut now = start;
    let mut generation = None;
    let (legacy_completed_at, legacy_max_attempts, stale_claim) = loop {
        let claim = background
            .crud_store
            .claim_due_native_terminal_effects_at(now, 90, 1)
            .await
            .unwrap()
            .records
            .pop()
            .unwrap();
        assert_eq!(claim.effect_id, effect_id);
        assert_ne!(claim.runtime_generation, 0);
        assert_eq!(
            *generation.get_or_insert(claim.runtime_generation),
            claim.runtime_generation
        );
        if with_checkpoint && claim.attempt_count == 1 {
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
            with_checkpoint.then_some(checkpoint.as_str())
        );
        assert!(
            background
                .crud_store
                .fail_native_terminal_effect(
                    &effect_id,
                    &claim.claim_token,
                    if manifest_legacy {
                        "memory.post_turn_extractor.manifest_failed"
                    } else {
                        "memory.post_turn_extractor.write_failed"
                    },
                    "legacy unclassified failure",
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
            .unwrap()
            .count,
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
            .unwrap()
            .count,
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

const MANIFEST_CANARY: &str = "SELECT secret FROM memory /private/manifest workspace-canary thread-canary turn-canary memory-canary token=secret memory-content-canary";

async fn corrupt_manifest_candidate(fixture: &LegacyCheckpointFixture) {
    // Persisted malformed metadata reaches real MemoryService conversion, not an enum seam.
    fixture
        .store()
        .insert_agent_memory_candidate(
            pioneer_crud::NewAgentMemoryCandidate {
                id: None,
                scope: pioneer_protocol::MemoryScope {
                    kind: pioneer_protocol::MemoryScopeKind::Thread,
                    key: fixture.turn_id.replacen("turn-", "thread-", 1),
                },
                namespace: None,
                category: pioneer_protocol::MemoryCategory::Identity,
                key: None,
                status: Some(pioneer_protocol::MemoryCandidateStatus::Pending),
                candidate_text: MANIFEST_CANARY.into(),
                confidence: 0.9,
                reason: "fixture".into(),
                source_context_kind: None,
                source_thread_id: None,
                source_turn_id: None,
                source_item_id: None,
                created_by: None,
                dedupe_key: None,
                metadata_json: Some(MANIFEST_CANARY.into()),
            },
            fixture.legacy_completed_at,
        )
        .await
        .unwrap();
}

async fn execute_legacy_manifest(fixture: &LegacyCheckpointFixture) {
    let now = fixture.legacy_completed_at + 3_601;
    assert_eq!(
        fixture
            .store()
            .requeue_retryable_unresolved_native_terminal_effects(now, 1)
            .await
            .unwrap(),
        1
    );
    let pending = fixture.status().await;
    assert_eq!(pending.max_attempts, fixture.legacy_max_attempts);
    assert_eq!(pending.attempt_count, pending.max_attempts - 1);
    assert_eq!(
        pending.last_error_code.as_deref(),
        Some("memory.post_turn_extractor.legacy_manifest_revalidate")
    );
    assert_eq!(
        fixture
            .store()
            .with_maintenance_access()
            .requeue_retryable_unresolved_native_terminal_effects(now, 1)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        fixture
            .background
            .process_due_native_terminal_effects(now, 1)
            .await
            .unwrap()
            .count,
        1
    );
    assert_eq!(
        fixture.status().await.attempt_count,
        fixture.legacy_max_attempts
    );
    assert!(
        !fixture
            .store()
            .fail_native_terminal_effect(
                &fixture.effect_id,
                &fixture.stale_claim,
                "stale",
                "stale",
                false,
                now,
                now
            )
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn legacy_manifest_without_checkpoint_reclassifies_saved_candidate() {
    let fixture =
        legacy_recovery_fixture("manifest-bad-data", vec![], WriteMode::Pass, true, false).await;
    corrupt_manifest_candidate(&fixture).await;
    execute_legacy_manifest(&fixture).await;
    let status = fixture.status().await;
    assert_eq!(status.status, "unresolved");
    assert_eq!(
        status.last_error_code.as_deref(),
        Some("memory.post_turn_extractor.manifest_invalid_stored_data")
    );
    assert_eq!(fixture.model.call_count(), 0);
    assert!(fixture.writes.attempts.lock().unwrap().is_empty());
    let run = fixture.extractor_run().await;
    let error = run.error.unwrap();
    assert!(!error.retryable);
    // CRUD persists code/message/retryable, not HookError metadata. The fixed
    // safe message carries these fields through both persisted error APIs.
    assert!(error.metadata.is_empty());
    assert!(error.safe_for_user);
    assert_eq!(
        error.message.as_str(),
        "memory manifest loading failed: failure_class=invalid_stored_data failure_stage=candidates sqlite_primary_code=None sqlite_extended_code=None"
    );
    // The worker also appends its known safe metadata to the outbox message.
    assert_eq!(
        status.last_error_message.as_deref(),
        Some(
            "memory manifest loading failed: failure_class=invalid_stored_data failure_stage=candidates sqlite_primary_code=None sqlite_extended_code=None; failure_stage=candidates; failure_class=invalid_stored_data"
        )
    );
    assert!(
        !serde_json::to_string(&error)
            .unwrap()
            .contains(MANIFEST_CANARY)
    );
    assert!(
        status
            .last_error_message
            .unwrap()
            .contains("failure_stage=candidates")
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
}

#[tokio::test]
async fn legacy_manifest_failure_stops_real_checkpoint_replay() {
    let fixture = legacy_recovery_fixture(
        "manifest-checkpoint",
        vec![name_fact()],
        WriteMode::Pass,
        true,
        true,
    )
    .await;
    corrupt_manifest_candidate(&fixture).await;
    execute_legacy_manifest(&fixture).await;
    assert_eq!(
        fixture.status().await.last_error_code.as_deref(),
        Some("memory.post_turn_extractor.manifest_invalid_stored_data")
    );
    assert_eq!(fixture.model.call_count(), 0);
    assert!(fixture.writes.attempts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn legacy_manifest_success_continues_real_checkpoint_handler() {
    let fixture = legacy_recovery_fixture(
        "manifest-success",
        vec![name_fact()],
        WriteMode::Pass,
        true,
        true,
    )
    .await;
    execute_legacy_manifest(&fixture).await;
    assert_eq!(fixture.status().await.status, "succeeded");
    assert_eq!(fixture.model.call_count(), 0);
    assert_eq!(fixture.writes.attempts.lock().unwrap().len(), 1);
}

#[test]
fn manifest_worker_sentry_diagnostic_is_safe_with_real_gateway_hook_and_outbox() {
    let (_, events) = crate::public_error::test_support::capture_events(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let fixture = legacy_recovery_fixture(
                    "manifest-sentry",
                    vec![],
                    WriteMode::Pass,
                    true,
                    false,
                )
                .await;
                corrupt_manifest_candidate(&fixture).await;
                execute_legacy_manifest(&fixture).await;
                fixture.assert_safe_diagnostics().await;
                // Actual SQLite query error through the scoped DB handle and Gateway.
                let sqlite_fixture = legacy_recovery_fixture(
                    "manifest-sentry-sqlite",
                    vec![],
                    WriteMode::Pass,
                    true,
                    false,
                )
                .await;
                use sea_orm::ConnectionTrait;
                sqlite_fixture
                    .store()
                    .database_connection()
                    .execute_raw(sea_orm::Statement::from_string(
                        sea_orm::DbBackend::Sqlite,
                        "DROP TABLE agent_memory_candidate".to_owned(),
                    ))
                    .await
                    .unwrap();
                execute_legacy_manifest(&sqlite_fixture).await;
                let status = sqlite_fixture.status().await;
                assert_eq!(
                    status.last_error_code.as_deref(),
                    Some("memory.post_turn_extractor.manifest_unclassified")
                );
                assert!(
                    status
                        .last_error_message
                        .unwrap()
                        .contains("sqlite_primary_code=Some(1)")
                );
                assert_eq!(sqlite_fixture.model.call_count(), 0);
            });
    });
    let event = events
        .iter()
        .find(|event| {
            event.message.as_deref()
                == Some("memory post-turn extractor exhausted durable delivery")
        })
        .unwrap();
    let sentry::protocol::Context::Other(fields) = &event.contexts["Rust Tracing Fields"] else {
        panic!("tracing fields required")
    };
    assert_eq!(fields["failure_class"], json!("invalid_stored_data"));
    assert_eq!(fields["failure_stage"], json!("candidates"));
    assert!(!fields.contains_key("sqlite_primary_code"));
    assert!(!fields.contains_key("sqlite_extended_code"));
    let sqlite_event = events.iter().find(|event| {
        matches!(event.contexts.get("Rust Tracing Fields"), Some(sentry::protocol::Context::Other(fields)) if fields.get("sqlite_primary_code") == Some(&json!(1)))
        && event.message.as_deref() == Some("memory post-turn extractor exhausted durable delivery")
    }).unwrap();
    let sentry::protocol::Context::Other(sqlite_fields) =
        &sqlite_event.contexts["Rust Tracing Fields"]
    else {
        panic!("tracing fields required")
    };
    assert_eq!(sqlite_fields["failure_class"], json!("unclassified"));
    assert_eq!(sqlite_fields["failure_stage"], json!("candidates"));
    assert_eq!(sqlite_fields["sqlite_primary_code"], json!(1));
    assert_eq!(sqlite_fields["sqlite_extended_code"], json!(1));
    assert!(
        !serde_json::to_string(sqlite_event)
            .unwrap()
            .contains("agent_memory_candidate")
    );
    let text = serde_json::to_string(event).unwrap();
    for canary in [
        MANIFEST_CANARY,
        "SELECT secret",
        "/private/manifest",
        "workspace-canary",
        "thread-canary",
        "turn-canary",
        "memory-canary",
        "token=secret",
        "memory-content-canary",
        "CheckpointName",
    ] {
        assert!(!text.contains(canary), "leaked {canary}");
    }
}

#[tokio::test]
async fn manifest_actual_authorization_failure_has_safe_stage_and_no_invented_domain_cause() {
    let fixture =
        legacy_recovery_fixture("manifest-authority", vec![], WriteMode::Pass, true, false).await;
    let provider = GatewayMemoryProvider::new(Arc::downgrade(&fixture.background));
    let context = MemoryTurnContext {
        workspace_id: fixture._harness.workspace_id.clone(),
        thread_id: "thread-manifest-authority".into(),
        conversation_thread_id: None,
        turn_id: fixture.turn_id.clone(),
        mode: ThreadMode::Agent,
        input_text: String::new(),
        task_id: None,
        agent_id: None,
        principal_id: Some(MANIFEST_CANARY.into()),
    };
    let failure = provider
        .load_memory_manifest(
            context,
            MemoryManifestRequest {
                max_items: 8,
                max_item_chars: 100,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        failure.stage,
        pioneer_memory::MemoryManifestFailureStage::Authorization
    );
    assert_eq!(
        failure.class,
        pioneer_memory::MemoryManifestFailureClass::Unclassified
    );
    assert!(!failure.retryable());
    assert!(!format!("{failure:?}").contains(MANIFEST_CANARY));
}

#[tokio::test]
async fn legacy_manifest_without_checkpoint_success_calls_model_once() {
    let fixture = legacy_recovery_fixture(
        "manifest-no-checkpoint-success",
        vec![],
        WriteMode::Pass,
        true,
        false,
    )
    .await;
    execute_legacy_manifest(&fixture).await;
    assert_eq!(fixture.status().await.status, "succeeded");
    assert_eq!(fixture.model.call_count(), 1);
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
}

#[tokio::test]
async fn manifest_poison_does_not_block_healthy_obligation_in_same_worker_batch() {
    let fixture = legacy_recovery_fixture(
        "manifest-poison-batch",
        vec![],
        WriteMode::Pass,
        true,
        false,
    )
    .await;
    corrupt_manifest_candidate(&fixture).await;
    let healthy_turn = "turn-manifest-healthy-batch";
    let healthy_thread = "thread-manifest-healthy-batch";
    seed_cli_runtime_turn_with_text(
        fixture.store(),
        &fixture._harness.workspace_id,
        "codex",
        "codex",
        healthy_thread,
        healthy_turn,
        "native-manifest-healthy-batch",
        SOURCE_TEXT,
    )
    .await;
    let binding = fixture
        .store()
        .get_cli_runtime_turn_binding(healthy_turn)
        .await
        .unwrap()
        .unwrap();
    let answer = TurnItem::AgentMessage {
        id: "healthy-final".into(),
        text: "Принято".into(),
        phase: pioneer_protocol::AgentMessagePhase::FinalAnswer,
        markdown: None,
        markdown_version: None,
    };
    fixture
        .background
        .prepare_cli_post_turn_hook(&binding, 1, Some(&answer))
        .await
        .unwrap();
    let (_, mut turn) = fixture
        .store()
        .get_turn(healthy_thread, healthy_turn)
        .await
        .unwrap()
        .unwrap();
    turn.status = TurnStatus::Completed;
    fixture
        .store()
        .materialize_turn_completed(
            TurnCompletedNotification {
                workspace_id: fixture._harness.workspace_id.clone(),
                thread_id: healthy_thread.into(),
                turn,
            },
            fixture.legacy_completed_at,
        )
        .await
        .unwrap();
    let now = fixture.legacy_completed_at + 3_601;
    assert_eq!(
        fixture
            .store()
            .requeue_retryable_unresolved_native_terminal_effects(now, 2)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        fixture
            .background
            .process_due_native_terminal_effects(now, 2)
            .await
            .unwrap()
            .count,
        2
    );
    assert_eq!(fixture.status().await.status, "unresolved");
    assert_eq!(
        fixture.status().await.last_error_code.as_deref(),
        Some("memory.post_turn_extractor.manifest_invalid_stored_data")
    );
    let healthy = fixture
        .store()
        .native_terminal_effect_status(&format!("{healthy_turn}:terminal-effect:post-turn"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(healthy.status, "succeeded");
    assert_eq!(healthy.attempt_count, 1);
    assert_eq!(fixture.model.call_count(), 1);
}

#[tokio::test]
async fn manifest_typed_transient_worker_retries_in_original_budget_then_continues_gateway() {
    let fixture = recovery_fixture(
        "manifest-original-budget",
        vec![],
        WriteMode::Pass,
        true,
        false,
        false,
    )
    .await;
    fixture.writes.manifest_faults.lock().unwrap().push_back(
        anyhow::Error::new(sea_orm::DbErr::ConnectionAcquire(
            sea_orm::ConnAcquireErr::Timeout,
        ))
        .context(MANIFEST_CANARY),
    );
    let now = fixture.legacy_completed_at;
    assert_eq!(
        fixture
            .background
            .process_due_native_terminal_effects(now, 1)
            .await
            .unwrap()
            .count,
        1
    );
    let failed = fixture.status().await;
    assert_eq!(failed.status, "retry_wait");
    assert_eq!(failed.attempt_count, 1);
    assert_eq!(failed.max_attempts, 8);
    assert_eq!(
        failed.last_error_code.as_deref(),
        Some("memory.post_turn_extractor.manifest_storage_transient")
    );
    assert_eq!(fixture.model.call_count(), 0);
    assert!(fixture.writes.attempts.lock().unwrap().is_empty());
    assert_eq!(
        fixture
            .background
            .process_due_native_terminal_effects(now + 1, 1)
            .await
            .unwrap()
            .count,
        0
    );
    // Advance only the scanner's supplied timestamp, never real time or retry policy.
    assert_eq!(
        fixture
            .background
            .process_due_native_terminal_effects(now + 300, 1)
            .await
            .unwrap()
            .count,
        1
    );
    let succeeded = fixture.status().await;
    assert_eq!(succeeded.status, "succeeded");
    assert_eq!(succeeded.attempt_count, 2);
    assert_eq!(succeeded.max_attempts, 8);
    assert_eq!(fixture.model.call_count(), 1);
}

#[tokio::test]
async fn legacy_manifest_typed_transient_result_controls_normal_recovery() {
    let fixture = legacy_recovery_fixture(
        "manifest-legacy-transient",
        vec![],
        WriteMode::Pass,
        true,
        false,
    )
    .await;
    fixture.writes.manifest_faults.lock().unwrap().push_back(
        anyhow::Error::new(sea_orm::DbErr::ConnectionAcquire(
            sea_orm::ConnAcquireErr::Timeout,
        ))
        .context(MANIFEST_CANARY),
    );
    execute_legacy_manifest(&fixture).await;
    let status = fixture.status().await;
    assert_eq!(status.status, "unresolved");
    assert_eq!(status.max_attempts, fixture.legacy_max_attempts);
    assert_eq!(
        status.last_error_code.as_deref(),
        Some("memory.post_turn_extractor.manifest_storage_transient")
    );
    assert_eq!(fixture.model.call_count(), 0);
    let now = chrono::Utc::now().timestamp() + 3_602;
    assert_eq!(
        fixture
            .store()
            .requeue_retryable_unresolved_native_terminal_effects(now, 1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(fixture.status().await.attempt_count, 0);
    assert_eq!(
        fixture
            .background
            .process_due_native_terminal_effects(now, 1)
            .await
            .unwrap()
            .count,
        1
    );
    assert_eq!(fixture.status().await.status, "succeeded");
    assert_eq!(fixture.model.call_count(), 1);
}

#[tokio::test]
async fn legacy_live_states_receive_new_manifest_classification_on_next_delivery() {
    use pioneer_entity::native_terminal_effect_outbox as outbox;
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, sea_query::Expr};
    for state in ["ready", "retry_wait", "running"] {
        let fixture = recovery_fixture(
            &format!("manifest-live-{state}"),
            vec![],
            WriteMode::Pass,
            true,
            false,
            false,
        )
        .await;
        let now = fixture.legacy_completed_at;
        let mut execute_at = now;
        let mut stale_claim = None;
        if state != "ready" {
            let claim = fixture
                .store()
                .claim_due_native_terminal_effects_at(now, 90, 1)
                .await
                .unwrap()
                .records
                .pop()
                .unwrap();
            stale_claim = Some(claim.claim_token.clone());
            if state == "retry_wait" {
                fixture
                    .store()
                    .fail_native_terminal_effect(
                        &fixture.effect_id,
                        &claim.claim_token,
                        "memory.post_turn_extractor.manifest_failed",
                        "legacy failure",
                        true,
                        now + 1,
                        now,
                    )
                    .await
                    .unwrap();
                execute_at = now + 1;
            } else {
                execute_at = now + 91;
            }
        }
        // Seed historical last_error_code only; state and fences use production APIs.
        outbox::Entity::update_many()
            .col_expr(
                outbox::Column::LastErrorCode,
                Expr::value(Some(
                    "memory.post_turn_extractor.manifest_failed".to_owned(),
                )),
            )
            .filter(outbox::Column::EffectId.eq(&fixture.effect_id))
            .exec(&fixture.store().database_connection())
            .await
            .unwrap();
        fixture
            .writes
            .manifest_faults
            .lock()
            .unwrap()
            .push_back(anyhow::anyhow!(MANIFEST_CANARY));
        assert_eq!(
            fixture
                .background
                .process_due_native_terminal_effects(execute_at, 1)
                .await
                .unwrap()
                .count,
            1
        );
        let status = fixture.status().await;
        assert_eq!(status.status, "unresolved");
        assert_eq!(
            status.last_error_code.as_deref(),
            Some("memory.post_turn_extractor.manifest_unclassified")
        );
        assert_eq!(status.max_attempts, 8);
        assert_eq!(status.attempt_count, if state == "ready" { 1 } else { 2 });
        assert_eq!(fixture.model.call_count(), 0);
        assert!(fixture.writes.attempts.lock().unwrap().is_empty());
        if let Some(stale_claim) = stale_claim {
            assert!(
                !fixture
                    .store()
                    .fail_native_terminal_effect(
                        &fixture.effect_id,
                        &stale_claim,
                        "stale",
                        "stale",
                        true,
                        execute_at,
                        execute_at
                    )
                    .await
                    .unwrap()
            );
        }
        assert_eq!(
            fixture
                .store()
                .requeue_retryable_unresolved_native_terminal_effects(now + 3_602, 1)
                .await
                .unwrap(),
            0
        );
    }
}

#[tokio::test]
async fn manifest_actual_active_query_failure_is_unclassified_with_sqlite_codes() {
    use sea_orm::ConnectionTrait;
    let fixture = legacy_recovery_fixture(
        "manifest-active-query",
        vec![],
        WriteMode::Pass,
        true,
        false,
    )
    .await;
    fixture
        .store()
        .database_connection()
        .execute_unprepared("DROP TABLE agent_memory")
        .await
        .unwrap();
    execute_legacy_manifest(&fixture).await;
    let error = fixture.extractor_run().await.error.unwrap();
    assert_eq!(
        error.code.as_str(),
        "memory.post_turn_extractor.manifest_unclassified"
    );
    assert!(!error.retryable);
    assert!(error.metadata.is_empty());
    assert!(error.safe_for_user);
    assert_eq!(
        error.message.as_str(),
        "memory manifest loading failed: failure_class=unclassified failure_stage=active sqlite_primary_code=Some(1) sqlite_extended_code=Some(1)"
    );
    let status = fixture.status().await;
    assert_eq!(status.status, "unresolved");
    assert_eq!(status.last_error_code.as_deref(), Some(error.code.as_str()));
    assert_eq!(
        status.last_error_message.as_deref(),
        Some(
            "memory manifest loading failed: failure_class=unclassified failure_stage=active sqlite_primary_code=Some(1) sqlite_extended_code=Some(1); failure_stage=active; failure_class=unclassified"
        )
    );
    assert_eq!(fixture.model.call_count(), 0);
    assert!(fixture.writes.attempts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn manifest_disabled_runtime_remains_successful_empty_manifest() {
    let harness = setup_memory_gateway_harness("manifest-disabled", false).await;
    let provider = GatewayMemoryProvider::new(Arc::downgrade(&harness.processor));
    let manifest = provider
        .load_memory_manifest(
            memory_tool_context(&harness, "disabled"),
            MemoryManifestRequest {
                max_items: 8,
                max_item_chars: 100,
            },
        )
        .await
        .unwrap();
    assert!(manifest.active.is_empty());
    assert!(manifest.candidates.is_empty());
    assert!(!manifest.diagnostics.is_empty());
}

#[tokio::test]
async fn legacy_manifest_marker_and_single_budget_survive_database_reopen_and_claim_crash() {
    use sea_orm::{ConnectOptions, ConnectionTrait};
    for crash_after_claim in [false, true] {
        let (directory, workspace_manager, original, workspace_id) =
            setup_pooled_file_workspace_manager().await;
        let store = original.with_maintenance_access();
        let database = store.database_connection();
        assert_eq!(
            database.read_class(),
            pioneer_sqlite::SqliteReadClass::Maintenance
        );
        assert_eq!(
            database.write_class(),
            pioneer_sqlite::SqliteWriteClass::Maintenance
        );
        assert_eq!(database.writer_max_connections(), 1);
        assert!(database.reader_query_only_enabled().await.unwrap());
        let turn_id = "turn-manifest-restart";
        let thread_id = "thread-manifest-restart";
        let effect_id = "manifest-restart-effect";
        let now = chrono::Utc::now().timestamp();
        seed_cli_runtime_turn_with_text(
            &store,
            &workspace_id,
            "codex",
            "codex",
            thread_id,
            turn_id,
            "native-manifest-restart",
            SOURCE_TEXT,
        )
        .await;
        store
            .prepare_native_terminal_effects(
                pioneer_protocol::NativeTerminalEffectPreparation {
                    batch_id: "manifest-restart-batch".into(),
                    workspace_id: workspace_id.clone(),
                    thread_id: thread_id.into(),
                    turn_id: turn_id.into(),
                    runtime_generation: 1,
                    effects: vec![pioneer_protocol::NativeTerminalEffectSpec {
                        effect_id: effect_id.into(),
                        effect_kind: pioneer_protocol::NativeTerminalEffectKind::PostTurnHook,
                        gate: pioneer_protocol::NativeTerminalEffectGate::TerminalCommit,
                        payload: pioneer_protocol::NativeTerminalEffectPayload::PostTurnHook {
                            request: json!({}),
                            runtime_snapshot: json!({}),
                        },
                        max_attempts: 1,
                    }],
                },
                now,
            )
            .await
            .unwrap();
        let (_, mut turn) = store.get_turn(thread_id, turn_id).await.unwrap().unwrap();
        turn.status = TurnStatus::Completed;
        store
            .materialize_turn_completed(
                TurnCompletedNotification {
                    workspace_id,
                    thread_id: thread_id.into(),
                    turn,
                },
                now,
            )
            .await
            .unwrap();
        let original_claim = store
            .claim_due_native_terminal_effects_at(now, 90, 1)
            .await
            .unwrap()
            .records
            .pop()
            .unwrap();
        store
            .fail_native_terminal_effect(
                effect_id,
                &original_claim.claim_token,
                "memory.post_turn_extractor.manifest_failed",
                "legacy manifest",
                true,
                now + 1,
                now,
            )
            .await
            .unwrap();
        let requeue_at = now + 3_601;
        // Abort the only mutation: marker, state and budget must all roll back.
        database.execute_unprepared("CREATE TRIGGER manifest_requeue_abort BEFORE UPDATE ON native_terminal_effect_outbox WHEN NEW.last_error_code = 'memory.post_turn_extractor.legacy_manifest_revalidate' BEGIN SELECT RAISE(ABORT, 'fixture abort'); END").await.unwrap();
        assert!(
            store
                .requeue_retryable_unresolved_native_terminal_effects(requeue_at, 1)
                .await
                .is_err()
        );
        let rolled_back = store
            .native_terminal_effect_status(effect_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(rolled_back.status, "unresolved");
        assert_eq!(
            rolled_back.last_error_code.as_deref(),
            Some("memory.post_turn_extractor.manifest_failed")
        );
        assert_eq!(rolled_back.attempt_count, 1);
        assert_eq!(rolled_back.max_attempts, 1);
        database
            .execute_unprepared("DROP TRIGGER manifest_requeue_abort")
            .await
            .unwrap();
        assert_eq!(
            store
                .requeue_retryable_unresolved_native_terminal_effects(requeue_at, 1)
                .await
                .unwrap(),
            1
        );
        if crash_after_claim {
            let claim = store
                .claim_due_native_terminal_effects_at(requeue_at, 90, 1)
                .await
                .unwrap()
                .records
                .pop()
                .unwrap();
            assert_eq!(claim.attempt_count, claim.max_attempts);
        }
        drop(store);
        drop(original);
        drop(workspace_manager);
        database.close().await.unwrap();

        let url = format!(
            "sqlite://{}?mode=rw",
            directory.path().join("gateway.sqlite").display()
        );
        let mut writer_options = ConnectOptions::new(url.clone());
        writer_options.max_connections(1).sqlx_logging(false);
        let mut reader_options = ConnectOptions::new(url);
        reader_options
            .max_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
        let writer = Database::connect(writer_options).await.unwrap();
        let reader = Database::connect(reader_options).await.unwrap();
        let database = pioneer_sqlite::SqliteDatabase::new(reader, writer).maintenance();
        let restarted = CrudStore::new(database.clone());
        let persisted = restarted
            .native_terminal_effect_status(effect_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            persisted.last_error_code.as_deref(),
            Some("memory.post_turn_extractor.legacy_manifest_revalidate")
        );
        assert_eq!(persisted.max_attempts, 1);
        assert_eq!(
            persisted.attempt_count,
            if crash_after_claim { 1 } else { 0 }
        );
        assert_eq!(
            restarted
                .requeue_retryable_unresolved_native_terminal_effects(requeue_at, 1)
                .await
                .unwrap(),
            0
        );
        let claims = restarted
            .claim_due_native_terminal_effects_at(requeue_at + 91, 90, 1)
            .await
            .unwrap()
            .records;
        if crash_after_claim {
            assert!(claims.is_empty());
            assert_eq!(
                restarted
                    .native_terminal_effect_status(effect_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .status,
                "unresolved"
            );
            assert_eq!(
                restarted
                    .requeue_retryable_unresolved_native_terminal_effects(requeue_at + 3_702, 1)
                    .await
                    .unwrap(),
                0
            );
        } else {
            assert_eq!(claims.len(), 1);
            assert_eq!(claims[0].attempt_count, 1);
            assert_eq!(claims[0].max_attempts, 1);
            assert!(
                !restarted
                    .fail_native_terminal_effect(
                        effect_id,
                        &original_claim.claim_token,
                        "stale",
                        "stale",
                        false,
                        requeue_at,
                        requeue_at
                    )
                    .await
                    .unwrap()
            );
        }
        drop(restarted);
        database.close().await.unwrap();
    }
}

#[tokio::test]
async fn legacy_manifest_worker_timeout_keeps_single_budget_after_database_reopen() {
    use pioneer_entity::native_terminal_effect_outbox as outbox;
    use sea_orm::{ColumnTrait, ConnectOptions, EntityTrait, QueryFilter, sea_query::Expr};

    // Ordinary max_attempts=1 is the adjacent control: its timeout still gets
    // ordinary recovery with budget 8. Both historical legacy budgets stay fixed.
    for (legacy, budget) in [(true, 1_u16), (true, 8_u16), (false, 1_u16)] {
        let (directory, workspace_manager, store, workspace_id) =
            setup_pooled_file_workspace_manager().await;
        let fixture = recovery_fixture_on_workspace(
            &format!("manifest-worker-timeout-{legacy}-{budget}"),
            vec![],
            WriteMode::Pass,
            true,
            false,
            false,
            Some((workspace_manager, store, workspace_id)),
        )
        .await;
        let database = fixture.store().database_connection();
        // Historical delivery budget is fixture data, not a changed deadline or
        // relaxed production invariant. Only an unclaimed ready row is seeded.
        assert_eq!(
            outbox::Entity::update_many()
                .col_expr(outbox::Column::MaxAttempts, Expr::value(i64::from(budget)))
                .filter(outbox::Column::EffectId.eq(&fixture.effect_id))
                .filter(outbox::Column::Status.eq("ready"))
                .filter(outbox::Column::AttemptCount.eq(0))
                .exec(&database)
                .await
                .unwrap()
                .rows_affected,
            1
        );
        let mut execute_at = fixture.legacy_completed_at;
        if legacy {
            for _ in 0..budget {
                let claim = fixture
                    .store()
                    .claim_due_native_terminal_effects_at(execute_at, 90, 1)
                    .await
                    .unwrap()
                    .records
                    .pop()
                    .unwrap();
                assert!(!claim.legacy_manifest_revalidation);
                assert!(
                    fixture
                        .store()
                        .fail_native_terminal_effect(
                            &fixture.effect_id,
                            &claim.claim_token,
                            "memory.post_turn_extractor.manifest_failed",
                            "legacy manifest failure",
                            true,
                            execute_at + 1,
                            execute_at,
                        )
                        .await
                        .unwrap()
                );
                execute_at += 1;
            }
            execute_at += 3_601;
            assert_eq!(
                fixture
                    .store()
                    .requeue_retryable_unresolved_native_terminal_effects(execute_at, 1)
                    .await
                    .unwrap(),
                1
            );
            let pending = fixture.status().await;
            assert_eq!(pending.status, "retry_wait");
            assert_eq!(
                pending.last_error_code.as_deref(),
                Some("memory.post_turn_extractor.legacy_manifest_revalidate")
            );
            assert_eq!(pending.max_attempts, budget);
            assert_eq!(pending.attempt_count, budget - 1);
        }
        let started = Arc::new(Notify::new());
        *fixture.writes.manifest_started.lock().unwrap() = Some(started.clone());
        let processor = fixture.background.clone();
        let worker = tokio::spawn(async move {
            processor
                .process_due_native_terminal_effects(execute_at, 1)
                .await
        });
        // No clock advance until the real worker has claimed the row and the
        // manifest provider has entered its pending operation, outside DB capacity.
        started.notified().await;
        let running = outbox::Entity::find_by_id(fixture.effect_id.clone())
            .one(&database)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(running.status, "running");
        assert_eq!(running.attempt_count, i64::from(budget));
        let stale_token = running.claim_token.unwrap();
        tokio::time::pause();
        tokio::time::advance(std::time::Duration::from_secs(61)).await;
        tokio::time::resume();
        assert_eq!(worker.await.unwrap().unwrap().count, 1);
        let expected_code = if legacy {
            "memory.post_turn_extractor.legacy_manifest_revalidation_timeout"
        } else {
            "effect_timeout"
        };
        let failed = fixture.status().await;
        assert_eq!(failed.status, "unresolved");
        assert_eq!(failed.last_error_code.as_deref(), Some(expected_code));
        assert_eq!(failed.max_attempts, budget);
        assert_eq!(failed.attempt_count, budget);
        if legacy {
            assert_eq!(
                failed.last_error_message.as_deref(),
                Some("legacy manifest revalidation exceeded its execution deadline")
            );
        }
        assert_eq!(fixture.model.call_count(), 0);
        assert!(fixture.writes.attempts.lock().unwrap().is_empty());
        let effect_id = fixture.effect_id.clone();
        drop(fixture);
        database.close().await.unwrap();

        // Reopen the actual file with the existing reader/writer contour.
        let url = format!(
            "sqlite://{}?mode=rw",
            directory.path().join("gateway.sqlite").display()
        );
        let mut writer_options = ConnectOptions::new(url.clone());
        writer_options.max_connections(1).sqlx_logging(false);
        let mut reader_options = ConnectOptions::new(url);
        reader_options
            .max_connections(1)
            .sqlx_logging(false)
            .map_sqlx_sqlite_opts(|options| options.pragma("query_only", "ON"));
        let writer = Database::connect(writer_options).await.unwrap();
        let reader = Database::connect(reader_options).await.unwrap();
        let database = pioneer_sqlite::SqliteDatabase::new(reader, writer).maintenance();
        let restarted = CrudStore::new(database.clone());
        assert!(database.reader_query_only_enabled().await.unwrap());
        assert_eq!(database.writer_max_connections(), 1);
        let persisted = restarted
            .native_terminal_effect_status(&effect_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(persisted.last_error_code.as_deref(), Some(expected_code));
        assert_eq!(persisted.attempt_count, budget);
        assert_eq!(persisted.max_attempts, budget);
        let scan_at = chrono::Utc::now().timestamp() + 3_602;
        assert!(
            !restarted
                .fail_native_terminal_effect(
                    &effect_id,
                    &stale_token,
                    "stale",
                    "stale",
                    true,
                    scan_at,
                    scan_at
                )
                .await
                .unwrap()
        );
        assert_eq!(
            restarted
                .requeue_retryable_unresolved_native_terminal_effects(scan_at, 1)
                .await
                .unwrap(),
            u64::from(!legacy)
        );
        if legacy {
            assert_eq!(
                restarted
                    .requeue_retryable_unresolved_native_terminal_effects(scan_at + 1, 1)
                    .await
                    .unwrap(),
                0
            );
            assert!(
                restarted
                    .claim_due_native_terminal_effects_at(scan_at, 90, 1)
                    .await
                    .unwrap()
                    .records
                    .is_empty()
            );
            let final_status = restarted
                .native_terminal_effect_status(&effect_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(final_status.status, "unresolved");
            assert_eq!(final_status.last_error_code.as_deref(), Some(expected_code));
            assert_eq!(final_status.max_attempts, budget);
            assert_eq!(final_status.attempt_count, budget);
        } else {
            let pending = restarted
                .native_terminal_effect_status(&effect_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(pending.max_attempts, 8);
            assert_eq!(pending.attempt_count, 0);
            let claim = restarted
                .claim_due_native_terminal_effects_at(scan_at, 90, 1)
                .await
                .unwrap()
                .records
                .pop()
                .unwrap();
            assert!(!claim.legacy_manifest_revalidation);
            assert_eq!(claim.max_attempts, 8);
            assert_eq!(claim.attempt_count, 1);
        }
        drop(restarted);
        database.close().await.unwrap();
    }
}
