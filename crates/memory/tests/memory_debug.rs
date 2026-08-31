use chrono::Offset;
use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    CrudStore, HookRunScope, HookRunScopeKind, NewHookAuditEventRecord, NewHookRunRecord,
};
use pioneer_hooks::{
    HookAuditEventKind, HookContext, HookContextMode, HookId, HookMetadataKey, HookPhase,
    HookRunIdempotencyKey, HookRunScopeId, HookRunStatus, HookSubscriptionId, HookValue,
    HookWorkspaceId,
};
use pioneer_memory::{
    InMemoryMemoryBackend, MemoryBackend, MemoryDebugDecisionOutcome, MemoryDebugEntityKind,
    MemoryDebugLifecycleState, MemoryDebugMissingDataKind, MemoryDebugRecallPlannerKind,
    MemoryDebugSuppressionReason, MemoryOperationContext, MemoryService, MemoryServiceConfig,
    format_memory_debug_trace, memory_debug_inventory,
};
use pioneer_protocol::{
    MemoryActor, MemoryActorKind, MemoryAttribute, MemoryCandidatesRejectParams, MemoryCategory,
    MemoryDurability, MemoryExplicitness, MemoryExtractorCertainty, MemoryIntent,
    MemoryQualityAction, MemoryScope, MemoryScopeHint, MemoryScopeKind, MemorySemanticFields,
    MemorySemanticWriteDisposition, MemorySemanticWriteParams, MemorySemanticWriteRoute,
    MemorySensitivityHint, MemorySourceContextKind, MemorySubject, MemoryWriteEvidence,
    MemoryWriteRelation,
};
use sea_orm::Database;
use std::collections::BTreeMap;
use std::sync::Arc;

async fn setup_service() -> (Arc<CrudStore>, MemoryService) {
    setup_service_with_config(MemoryServiceConfig::default()).await
}

async fn setup_service_with_config(config: MemoryServiceConfig) -> (Arc<CrudStore>, MemoryService) {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("connect sqlite");
    Migrator::up(&connection, None).await.expect("migrate");
    let store = Arc::new(CrudStore::new(connection));
    let backend: Arc<dyn MemoryBackend> = Arc::new(InMemoryMemoryBackend::default());
    let service = MemoryService::new(store.clone(), backend, config);
    (store, service)
}

fn timestamp(offset: i64) -> chrono::DateTime<chrono::FixedOffset> {
    chrono::DateTime::from_timestamp(1_700_000_000 + offset, 0)
        .expect("valid timestamp")
        .with_timezone(&chrono::Utc.fix())
}

fn scope(kind: MemoryScopeKind, key: &str) -> MemoryScope {
    MemoryScope {
        kind,
        key: key.to_owned(),
    }
}

fn user_context(now: i64) -> MemoryOperationContext {
    MemoryOperationContext {
        now_unix: Some(now),
        actor: Some(MemoryActor {
            kind: MemoryActorKind::User,
            id: Some("user_memory_debug".to_owned()),
        }),
        ..MemoryOperationContext::default()
    }
}

fn workspace_context(workspace_id: &str, now: i64) -> MemoryOperationContext {
    MemoryOperationContext {
        workspace_id: Some(workspace_id.to_owned()),
        now_unix: Some(now),
        actor: Some(MemoryActor {
            kind: MemoryActorKind::User,
            id: Some("user_memory_debug".to_owned()),
        }),
        ..MemoryOperationContext::default()
    }
}

fn identity_name_semantic(explicitness: MemoryExplicitness) -> MemorySemanticFields {
    MemorySemanticFields {
        intent: match explicitness {
            MemoryExplicitness::Explicit => MemoryIntent::ExplicitStore,
            MemoryExplicitness::Implicit
            | MemoryExplicitness::None
            | MemoryExplicitness::Unclear => MemoryIntent::ImplicitCandidate,
        },
        explicitness,
        category: MemoryCategory::Identity,
        subject: MemorySubject::CurrentUser,
        attribute: MemoryAttribute::Name,
        subject_key: None,
        custom_subject: None,
        custom_attribute: None,
        scope_hint: MemoryScopeHint::UserGlobal,
        durability: MemoryDurability::LongLived,
        sensitivity: MemorySensitivityHint::None,
        certainty: MemoryExtractorCertainty::High,
    }
}

fn task_lifecycle_semantic() -> MemorySemanticFields {
    MemorySemanticFields {
        intent: MemoryIntent::ImplicitCandidate,
        explicitness: MemoryExplicitness::Implicit,
        category: MemoryCategory::Todo,
        subject: MemorySubject::Project,
        attribute: MemoryAttribute::Custom,
        subject_key: Some("project".to_owned()),
        custom_subject: None,
        custom_attribute: Some("task_state".to_owned()),
        scope_hint: MemoryScopeHint::ProjectWorkspace,
        durability: MemoryDurability::SessionOnly,
        sensitivity: MemorySensitivityHint::None,
        certainty: MemoryExtractorCertainty::High,
    }
}

fn unknown_custom_semantic() -> MemorySemanticFields {
    MemorySemanticFields {
        intent: MemoryIntent::ImplicitCandidate,
        explicitness: MemoryExplicitness::Implicit,
        category: MemoryCategory::Custom,
        subject: MemorySubject::Custom,
        attribute: MemoryAttribute::Custom,
        subject_key: None,
        custom_subject: Some("unknown".to_owned()),
        custom_attribute: Some("unknown".to_owned()),
        scope_hint: MemoryScopeHint::UserGlobal,
        durability: MemoryDurability::LongLived,
        sensitivity: MemorySensitivityHint::None,
        certainty: MemoryExtractorCertainty::Medium,
    }
}

fn workspace_decision_semantic() -> MemorySemanticFields {
    MemorySemanticFields {
        intent: MemoryIntent::ExplicitStore,
        explicitness: MemoryExplicitness::Explicit,
        category: MemoryCategory::ProjectDecision,
        subject: MemorySubject::Workspace,
        attribute: MemoryAttribute::MigrationPolicy,
        subject_key: None,
        custom_subject: None,
        custom_attribute: None,
        scope_hint: MemoryScopeHint::ProjectWorkspace,
        durability: MemoryDurability::ProjectLifetime,
        sensitivity: MemorySensitivityHint::None,
        certainty: MemoryExtractorCertainty::High,
    }
}

fn semantic_evidence(turn_id: &str) -> MemoryWriteEvidence {
    MemoryWriteEvidence {
        source_thread_id: Some("thread_memory_debug".to_owned()),
        source_turn_id: Some(turn_id.to_owned()),
        source_item_id: Some(format!("item_{turn_id}")),
        source_ref: Some(format!("turn:{turn_id}")),
        quote_or_span: Some("debug evidence quote".to_owned()),
        extractor_reason: Some("memory debug test extraction".to_owned()),
    }
}

fn semantic_write_params(
    semantic: MemorySemanticFields,
    content: &str,
    value: &str,
    disposition: MemorySemanticWriteDisposition,
    turn_id: &str,
) -> MemorySemanticWriteParams {
    MemorySemanticWriteParams {
        scope: scope(MemoryScopeKind::User, "default"),
        semantic,
        content: content.to_owned(),
        value: Some(value.to_owned()),
        evidence: Some(semantic_evidence(turn_id)),
        provenance: None,
        source_context_kind: Some(MemorySourceContextKind::DirectUserConversation),
        disposition: Some(disposition),
        client_provided_key: None,
        confidence: Some(0.95),
        importance: Some(0.7),
        metadata: BTreeMap::new(),
    }
}

fn hook_key(key: &str) -> HookMetadataKey {
    HookMetadataKey::new(key).expect("valid hook metadata key")
}

fn hook_object<const N: usize>(entries: [(&'static str, HookValue); N]) -> HookValue {
    HookValue::Object(
        entries
            .into_iter()
            .map(|(key, value)| (hook_key(key), value))
            .collect(),
    )
}

fn hook_text_list(values: &[&str]) -> HookValue {
    HookValue::List(
        values
            .iter()
            .map(|value| HookValue::Text((*value).to_owned()))
            .collect(),
    )
}

fn hook_run_context(workspace_id: &str, turn_id: &str) -> HookContext {
    HookContext {
        workspace_id: Some(HookWorkspaceId::new(workspace_id).expect("valid workspace id")),
        thread_id: Some(pioneer_hooks::HookThreadId::new("thread_memory_debug").expect("valid id")),
        turn_id: Some(pioneer_hooks::HookTurnId::new(turn_id).expect("valid id")),
        mode: Some(HookContextMode::Agent),
        ..HookContext::default()
    }
}

#[tokio::test]
async fn memory_debug_inventory_exposes_required_trace_sources() {
    let inventory = memory_debug_inventory();
    for field in [
        "quality_decisions",
        "score_components",
        "source_context",
        "recall_plans",
        "recall_modes",
        "suppressed_ids",
        "quarantine_state",
        "repair_jobs",
    ] {
        let item = inventory
            .iter()
            .find(|item| item.field == field)
            .unwrap_or_else(|| panic!("missing inventory item `{field}`"));
        assert!(
            item.available,
            "inventory item `{field}` should be available"
        );
        assert!(
            !item.source.is_empty(),
            "inventory item `{field}` should name source"
        );
    }
}

#[tokio::test]
async fn memory_debug_inspects_auto_approved_write_trace() {
    let (_store, service) = setup_service().await;
    let response = service
        .write_semantic_memory(
            user_context(10),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "Имя пользователя: Александр.",
                "Александр",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_debug_write_auto",
            ),
        )
        .await
        .expect("semantic write");
    let record = response.record.expect("active record");

    let trace = service
        .inspect_memory_debug(user_context(11), record.id.as_str())
        .await
        .expect("inspect memory");

    assert!(trace.found);
    assert_eq!(trace.target.kind, MemoryDebugEntityKind::Memory);
    assert_eq!(trace.lifecycle_state, MemoryDebugLifecycleState::Active);
    let write = trace.write.as_ref().expect("write trace");
    assert_eq!(write.outcome, MemoryDebugDecisionOutcome::Written);
    assert_eq!(write.relation, Some(MemoryWriteRelation::Novel));
    assert_eq!(
        write.semantic_route,
        Some(MemorySemanticWriteRoute::DurableControlPlane)
    );
    assert_eq!(
        write.latest_quality.as_ref().expect("quality").action,
        MemoryQualityAction::CandidatePolicy
    );
    assert_eq!(
        write
            .source_context
            .as_ref()
            .expect("source context")
            .source_context_kind,
        Some(MemorySourceContextKind::DirectUserConversation)
    );
    assert!(!write.events.is_empty());
    let report = format_memory_debug_trace(&trace);
    assert!(report.contains("write_outcome: Written"));
    assert!(report.contains("quality_action: CandidatePolicy"));
    assert!(!report.contains("provider prompt"));
    assert!(!report.contains("tool output"));
}

#[tokio::test]
async fn memory_debug_inspects_rejected_candidate_trace_with_score() {
    let mut config = MemoryServiceConfig::default();
    config.candidate_policy.review_enabled = true;
    let (_store, service) = setup_service_with_config(config).await;

    service
        .write_semantic_memory(
            user_context(19),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "The user's name is Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_debug_candidate_base",
            ),
        )
        .await
        .expect("base memory")
        .record
        .expect("base record");

    let candidate = service
        .write_semantic_memory(
            user_context(20),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Implicit),
                "The user's name might be Sasha.",
                "Sasha",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_debug_candidate_reject",
            ),
        )
        .await
        .expect("semantic candidate write")
        .candidate
        .expect("candidate");
    service
        .reject_candidate(
            user_context(21),
            MemoryCandidatesRejectParams {
                candidate_id: candidate.id.clone(),
                reason: Some("debug rejection".to_owned()),
                actor: None,
            },
        )
        .await
        .expect("reject candidate");

    let trace = service
        .inspect_candidate_debug(user_context(22), candidate.id.as_str())
        .await
        .expect("inspect candidate");

    assert!(trace.found);
    assert_eq!(trace.target.kind, MemoryDebugEntityKind::Candidate);
    assert_eq!(
        trace.lifecycle_state,
        MemoryDebugLifecycleState::CandidateRejected
    );
    let write = trace.write.expect("candidate write trace");
    assert_eq!(write.outcome, MemoryDebugDecisionOutcome::Rejected);
    assert_eq!(write.relation, Some(MemoryWriteRelation::Contradiction));
    assert!(write.latest_quality.is_some());
    assert!(
        write
            .score
            .as_ref()
            .expect("candidate score")
            .total_score
            .is_some()
    );
    assert!(write.events.iter().any(|event| {
        event.candidate_id.as_deref() == Some(candidate.id.as_str())
            && event.event_kind.contains("candidate")
    }));
}

#[tokio::test]
async fn memory_debug_inspects_terminal_quality_routes_by_turn() {
    let (_store, service) = setup_service().await;
    let mut task_params = semantic_write_params(
        task_lifecycle_semantic(),
        "Task runtime state: child task is waiting for provider output.",
        "child task waiting for provider output",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        "turn_debug_task_route",
    );
    task_params.scope = scope(MemoryScopeKind::Workspace, "ws_memory_debug");
    task_params.source_context_kind = Some(MemorySourceContextKind::TaskRuntime);
    let task_response = service
        .write_semantic_memory(workspace_context("ws_memory_debug", 30), task_params)
        .await
        .expect("task route");
    assert!(task_response.record.is_none());
    assert!(task_response.candidate.is_none());

    let task_trace = service
        .inspect_turn_memory_write_debug(
            workspace_context("ws_memory_debug", 31),
            "thread_memory_debug",
            "turn_debug_task_route",
            None,
        )
        .await
        .expect("inspect task route");
    assert!(task_trace.found);
    let task_write = task_trace.write.expect("task write trace");
    assert_eq!(task_write.outcome, MemoryDebugDecisionOutcome::Routed);
    assert_eq!(
        task_write.semantic_route,
        Some(MemorySemanticWriteRoute::TaskStateDeferred)
    );
    assert_eq!(
        task_write.latest_quality.expect("quality").action,
        MemoryQualityAction::RouteToTaskState
    );

    let quarantine_response = service
        .write_semantic_memory(
            user_context(32),
            semantic_write_params(
                unknown_custom_semantic(),
                "Unknown custom fact should stay audit-only.",
                "unknown custom fact",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_debug_quarantine_route",
            ),
        )
        .await
        .expect("quarantine route");
    assert!(quarantine_response.record.is_none());
    assert!(quarantine_response.candidate.is_none());

    let quarantine_trace = service
        .inspect_turn_memory_write_debug(
            user_context(33),
            "thread_memory_debug",
            "turn_debug_quarantine_route",
            None,
        )
        .await
        .expect("inspect quarantine route");
    let quarantine_write = quarantine_trace.write.expect("quarantine write trace");
    assert_eq!(
        quarantine_write.outcome,
        MemoryDebugDecisionOutcome::Quarantined
    );
    assert_eq!(
        quarantine_write.semantic_route,
        Some(MemorySemanticWriteRoute::AuditOnly)
    );
    assert_eq!(
        quarantine_write.latest_quality.expect("quality").action,
        MemoryQualityAction::Quarantine
    );
}

#[tokio::test]
async fn memory_debug_workspace_guard_blocks_cross_workspace_memory() {
    let (_store, service) = setup_service().await;
    let mut params = semantic_write_params(
        workspace_decision_semantic(),
        "Pioneer uses one current workspace migration file.",
        "one current workspace migration file",
        MemorySemanticWriteDisposition::AcceptActive,
        "turn_debug_workspace_guard",
    );
    params.scope = scope(MemoryScopeKind::Workspace, "ws_debug_a");

    let record = service
        .write_semantic_memory(workspace_context("ws_debug_a", 40), params)
        .await
        .expect("workspace memory")
        .record
        .expect("record");

    let blocked = service
        .inspect_memory_debug(workspace_context("ws_debug_b", 41), record.id.as_str())
        .await
        .expect("inspect blocked");
    assert!(!blocked.found);
    assert_eq!(blocked.lifecycle_state, MemoryDebugLifecycleState::Missing);
    assert!(
        blocked
            .missing
            .iter()
            .any(|missing| missing.kind == MemoryDebugMissingDataKind::MemoryRecord)
    );
}

#[tokio::test]
async fn memory_debug_inspects_recall_hook_audit_trace() {
    let (store, service) = setup_service().await;
    let turn_id = "turn_debug_recall";
    let context = hook_run_context("ws_debug_recall", turn_id);
    let run = store
        .create_hook_run(
            NewHookRunRecord {
                id: None,
                idempotency_key: HookRunIdempotencyKey::new(
                    "turn_debug_recall:post_preflight_prompt_context:memory.active_recall",
                )
                .expect("valid idempotency key"),
                subscription_id: HookSubscriptionId::new("subscription.memory_debug")
                    .expect("valid subscription id"),
                hook_id: HookId::new("memory.active_recall").expect("valid hook id"),
                phase: HookPhase::TurnPostPreflightPromptContext,
                status: HookRunStatus::Succeeded,
                scope: Some(HookRunScope {
                    kind: HookRunScopeKind::Turn,
                    id: HookRunScopeId::new(turn_id).expect("valid scope id"),
                }),
                context: context.clone(),
                contribution_hashes: Vec::new(),
                diagnostic_previews: Vec::new(),
                error: None,
                queued_at: Some(timestamp(1)),
                started_at: Some(timestamp(2)),
                completed_at: Some(timestamp(3)),
                deadline_at: Some(timestamp(10)),
                resume_state: None,
            },
            timestamp(0),
        )
        .await
        .expect("create hook run");

    let details = hook_object([
        ("planner_kind", HookValue::Text("provider".to_owned())),
        ("planner_status", HookValue::Text("run".to_owned())),
        (
            "planner_reason",
            HookValue::Text("provider_plan_valid".to_owned()),
        ),
        ("provider_used", HookValue::Bool(true)),
        ("provider_fallback_used", HookValue::Bool(false)),
        ("deterministic_sufficient", HookValue::Bool(false)),
        (
            "selected_modes",
            hook_text_list(&["project", "thread_episodic"]),
        ),
        (
            "dropped_modes",
            hook_text_list(&["task_context:missing_task_context"]),
        ),
        (
            "modes",
            HookValue::List(vec![
                hook_object([
                    ("mode", HookValue::Text("project".to_owned())),
                    ("hit_count", HookValue::I64(2)),
                    ("truncated", HookValue::Bool(false)),
                    ("skipped_reason", HookValue::Null),
                ]),
                hook_object([
                    ("mode", HookValue::Text("task_context".to_owned())),
                    ("hit_count", HookValue::I64(0)),
                    ("truncated", HookValue::Bool(false)),
                    (
                        "skipped_reason",
                        HookValue::Text("missing_task_context".to_owned()),
                    ),
                ]),
            ]),
        ),
        (
            "suppression_counts",
            hook_object([
                ("duplicate", HookValue::I64(1)),
                ("quality_penalty", HookValue::I64(2)),
            ]),
        ),
        ("suppressed_ids", hook_text_list(&["mem_duplicate"])),
        ("synthesized_count", HookValue::I64(2)),
        ("prompt_contribution_chars", HookValue::I64(120)),
    ]);
    store
        .append_hook_audit_events(
            vec![NewHookAuditEventRecord {
                id: None,
                hook_run_id: run.id.clone(),
                hook_run_attempt_id: None,
                subscription_id: run.subscription_id.clone(),
                hook_id: run.hook_id.clone(),
                phase: run.phase,
                context,
                event_kind: HookAuditEventKind::new("memory.recall.active")
                    .expect("valid event kind"),
                contribution_hash: None,
                details,
                safe_for_user: false,
                created_at: Some(timestamp(4)),
            }],
            timestamp(4),
        )
        .await
        .expect("append audit events");

    let hook_trace = service
        .inspect_hook_run_memory_debug(workspace_context("ws_debug_recall", 50), run.id.as_str())
        .await
        .expect("inspect hook run");
    let recall = hook_trace.recall.expect("hook recall trace");
    assert_eq!(recall.planner_kind, MemoryDebugRecallPlannerKind::Provider);
    assert_eq!(recall.provider_used, Some(true));
    assert_eq!(recall.deterministic_sufficient, Some(false));
    assert_eq!(
        recall.selected_modes,
        vec!["project".to_owned(), "thread_episodic".to_owned()]
    );
    assert_eq!(
        recall
            .suppression_counts
            .get(&MemoryDebugSuppressionReason::Duplicate),
        Some(&1)
    );
    assert_eq!(
        recall
            .suppression_counts
            .get(&MemoryDebugSuppressionReason::QualityPenalty),
        Some(&2)
    );
    assert_eq!(recall.suppressed_ids, vec!["mem_duplicate".to_owned()]);
    assert_eq!(recall.synthesized_count, Some(2));
    assert_eq!(recall.prompt_contribution_chars, Some(120));
    assert!(
        recall
            .mode_traces
            .iter()
            .any(|mode| mode.mode == "task_context"
                && mode.skipped_reason.as_deref() == Some("missing_task_context"))
    );

    let turn_trace = service
        .inspect_turn_memory_debug(workspace_context("ws_debug_recall", 51), turn_id, Some(100))
        .await
        .expect("inspect turn");
    assert!(turn_trace.found);
    assert_eq!(
        turn_trace.recall.expect("turn recall trace").planner_kind,
        MemoryDebugRecallPlannerKind::Provider
    );
}

#[tokio::test]
async fn memory_debug_missing_trace_is_typed() {
    let (_store, service) = setup_service().await;
    let trace = service
        .inspect_memory_debug(user_context(60), "missing_memory_debug_id")
        .await
        .expect("inspect missing memory");

    assert!(!trace.found);
    assert_eq!(trace.target.kind, MemoryDebugEntityKind::Memory);
    assert_eq!(trace.lifecycle_state, MemoryDebugLifecycleState::Missing);
    assert_eq!(
        trace.missing.first().map(|missing| missing.kind),
        Some(MemoryDebugMissingDataKind::MemoryRecord)
    );
}
