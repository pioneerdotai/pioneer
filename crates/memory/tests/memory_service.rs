use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    AgentMemoryListFilter, CrudStore, NewAgentMemoryQualityDecision, NewAgentMemoryRepairJob,
    global_agent_memory_scope_key, workspace_agent_memory_scope_key,
};
use pioneer_memory::{
    BackendDeleteRequest, BackendDeleteResult, BackendGetRequest, BackendPayload,
    BackendPutRequest, BackendPutResult, BackendSearchHit, BackendSearchRequest,
    InMemoryMemoryBackend, MemoryBackend, MemoryModeRecallParams, MemoryOperationContext,
    MemoryQuarantineRequest, MemoryReadPolicy, MemoryRecallMode, MemoryRecallParams,
    MemoryRecallTarget, MemoryRestoreRequest, MemoryService, MemoryServiceConfig,
};
use pioneer_protocol::{
    MemoryActor, MemoryActorKind, MemoryAttribute, MemoryCandidateStatus,
    MemoryCandidatesApproveParams, MemoryCandidatesEditAndApproveParams, MemoryCategory,
    MemoryDurability, MemoryEvidenceClass, MemoryExplicitness, MemoryExtractorCertainty,
    MemoryFactClass, MemoryForgetParams, MemoryForgetTarget, MemoryGetParams, MemoryIntent,
    MemoryLifecycleReasonCode, MemoryLifetimeClass, MemoryListParams, MemoryOwnershipClass,
    MemoryProvenance, MemoryQualityAction, MemoryQualityReasonCode, MemoryRememberParams,
    MemoryScope, MemoryScopeHint, MemoryScopeKind, MemorySearchParams, MemorySemanticFields,
    MemorySemanticWriteDisposition, MemorySemanticWriteParams, MemorySemanticWriteRoute,
    MemorySensitivity, MemorySensitivityHint, MemorySourceContextKind, MemoryStatus, MemorySubject,
    MemoryWriteEvidence, MemoryWriteRelation,
};
use sea_orm::Database;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Default)]
struct RecordingMemoryBackend {
    inner: InMemoryMemoryBackend,
    search_limits: Mutex<Vec<u32>>,
}

impl RecordingMemoryBackend {
    async fn search_limits(&self) -> Vec<u32> {
        self.search_limits.lock().await.clone()
    }
}

#[async_trait::async_trait]
impl MemoryBackend for RecordingMemoryBackend {
    async fn put(&self, request: BackendPutRequest) -> anyhow::Result<BackendPutResult> {
        self.inner.put(request).await
    }

    async fn get(&self, request: BackendGetRequest) -> anyhow::Result<Option<BackendPayload>> {
        self.inner.get(request).await
    }

    async fn search(&self, request: BackendSearchRequest) -> anyhow::Result<Vec<BackendSearchHit>> {
        self.search_limits.lock().await.push(request.limit);
        self.inner.search(request).await
    }

    async fn delete(&self, request: BackendDeleteRequest) -> anyhow::Result<BackendDeleteResult> {
        self.inner.delete(request).await
    }
}

async fn setup_service() -> (Arc<CrudStore>, Arc<InMemoryMemoryBackend>, MemoryService) {
    setup_service_with_config(MemoryServiceConfig::default()).await
}

async fn setup_service_with_config(
    config: MemoryServiceConfig,
) -> (Arc<CrudStore>, Arc<InMemoryMemoryBackend>, MemoryService) {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("connect sqlite");
    Migrator::up(&connection, None).await.expect("migrate");
    let store = Arc::new(CrudStore::new(connection));
    let backend = Arc::new(InMemoryMemoryBackend::default());
    let backend_for_service: Arc<dyn MemoryBackend> = backend.clone();
    let service = MemoryService::new(store.clone(), backend_for_service, config);
    (store, backend, service)
}

async fn setup_service_with_recording_backend()
-> (Arc<CrudStore>, Arc<RecordingMemoryBackend>, MemoryService) {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("connect sqlite");
    Migrator::up(&connection, None).await.expect("migrate");
    let store = Arc::new(CrudStore::new(connection));
    let backend = Arc::new(RecordingMemoryBackend::default());
    let backend_for_service: Arc<dyn MemoryBackend> = backend.clone();
    let service = MemoryService::new(
        store.clone(),
        backend_for_service,
        MemoryServiceConfig::default(),
    );
    (store, backend, service)
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
            id: Some("user_alexander".to_owned()),
        }),
        ..Default::default()
    }
}

fn workspace_context(workspace_id: &str, now: i64) -> MemoryOperationContext {
    MemoryOperationContext {
        workspace_id: Some(workspace_id.to_owned()),
        now_unix: Some(now),
        actor: Some(MemoryActor {
            kind: MemoryActorKind::User,
            id: Some("user_alexander".to_owned()),
        }),
        ..Default::default()
    }
}

fn agent_workspace_context(workspace_id: &str, agent_id: &str, now: i64) -> MemoryOperationContext {
    MemoryOperationContext {
        workspace_id: Some(workspace_id.to_owned()),
        agent_id: Some(agent_id.to_owned()),
        now_unix: Some(now),
        actor: Some(MemoryActor {
            kind: MemoryActorKind::User,
            id: Some("user_alexander".to_owned()),
        }),
        ..Default::default()
    }
}

fn remember_params(scope: MemoryScope, key: Option<&str>, content: &str) -> MemoryRememberParams {
    MemoryRememberParams {
        scope,
        category: MemoryCategory::Identity,
        namespace: None,
        key: key.map(str::to_owned),
        content: content.to_owned(),
        sensitivity: None,
        confidence: None,
        importance: None,
        provenance: None,
        source_context_kind: Some(MemorySourceContextKind::DirectUserConversation),
        idempotency_key: None,
        supersedes: None,
        metadata: BTreeMap::new(),
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

fn relationship_semantic(person_key: &str) -> MemorySemanticFields {
    MemorySemanticFields {
        intent: MemoryIntent::ImplicitCandidate,
        explicitness: MemoryExplicitness::Implicit,
        category: MemoryCategory::Relationship,
        subject: MemorySubject::Person,
        attribute: MemoryAttribute::Custom,
        subject_key: Some(person_key.to_owned()),
        custom_subject: None,
        custom_attribute: Some("contact".to_owned()),
        scope_hint: MemoryScopeHint::UserGlobal,
        durability: MemoryDurability::LongLived,
        sensitivity: MemorySensitivityHint::Personal,
        certainty: MemoryExtractorCertainty::High,
    }
}

fn user_preference_semantic(explicitness: MemoryExplicitness) -> MemorySemanticFields {
    MemorySemanticFields {
        intent: match explicitness {
            MemoryExplicitness::Explicit => MemoryIntent::ExplicitStore,
            MemoryExplicitness::Implicit
            | MemoryExplicitness::None
            | MemoryExplicitness::Unclear => MemoryIntent::ImplicitCandidate,
        },
        explicitness,
        category: MemoryCategory::Preference,
        subject: MemorySubject::CurrentUser,
        attribute: MemoryAttribute::ReviewStyle,
        subject_key: None,
        custom_subject: None,
        custom_attribute: None,
        scope_hint: MemoryScopeHint::UserGlobal,
        durability: MemoryDurability::LongLived,
        sensitivity: MemorySensitivityHint::None,
        certainty: MemoryExtractorCertainty::High,
    }
}

fn workspace_project_decision_semantic(explicitness: MemoryExplicitness) -> MemorySemanticFields {
    MemorySemanticFields {
        intent: match explicitness {
            MemoryExplicitness::Explicit => MemoryIntent::ExplicitStore,
            MemoryExplicitness::Implicit
            | MemoryExplicitness::None
            | MemoryExplicitness::Unclear => MemoryIntent::ImplicitCandidate,
        },
        explicitness,
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

fn agent_self_description_semantic(explicitness: MemoryExplicitness) -> MemorySemanticFields {
    MemorySemanticFields {
        intent: match explicitness {
            MemoryExplicitness::Explicit => MemoryIntent::ExplicitStore,
            MemoryExplicitness::Implicit
            | MemoryExplicitness::None
            | MemoryExplicitness::Unclear => MemoryIntent::ImplicitCandidate,
        },
        explicitness,
        category: MemoryCategory::Identity,
        subject: MemorySubject::CurrentAgent,
        attribute: MemoryAttribute::Name,
        subject_key: None,
        custom_subject: None,
        custom_attribute: None,
        scope_hint: MemoryScopeHint::AgentWorkspace,
        durability: MemoryDurability::LongLived,
        sensitivity: MemorySensitivityHint::None,
        certainty: MemoryExtractorCertainty::High,
    }
}

fn thread_local_todo_semantic() -> MemorySemanticFields {
    MemorySemanticFields {
        intent: MemoryIntent::ImplicitCandidate,
        explicitness: MemoryExplicitness::Implicit,
        category: MemoryCategory::Todo,
        subject: MemorySubject::CurrentUser,
        attribute: MemoryAttribute::Custom,
        subject_key: None,
        custom_subject: None,
        custom_attribute: Some("thread_follow_up".to_owned()),
        scope_hint: MemoryScopeHint::UserWorkspace,
        durability: MemoryDurability::SessionOnly,
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

fn tool_result_semantic() -> MemorySemanticFields {
    MemorySemanticFields {
        intent: MemoryIntent::ImplicitCandidate,
        explicitness: MemoryExplicitness::Implicit,
        category: MemoryCategory::ProjectFact,
        subject: MemorySubject::Artifact,
        attribute: MemoryAttribute::Custom,
        subject_key: Some("artifact".to_owned()),
        custom_subject: None,
        custom_attribute: Some("tool_observation".to_owned()),
        scope_hint: MemoryScopeHint::ProjectWorkspace,
        durability: MemoryDurability::Transient,
        sensitivity: MemorySensitivityHint::None,
        certainty: MemoryExtractorCertainty::High,
    }
}

fn operational_observation_semantic() -> MemorySemanticFields {
    MemorySemanticFields {
        intent: MemoryIntent::ImplicitCandidate,
        explicitness: MemoryExplicitness::Implicit,
        category: MemoryCategory::ProjectFact,
        subject: MemorySubject::Project,
        attribute: MemoryAttribute::Custom,
        subject_key: Some("project".to_owned()),
        custom_subject: None,
        custom_attribute: Some("operational_observation".to_owned()),
        scope_hint: MemoryScopeHint::ProjectWorkspace,
        durability: MemoryDurability::Transient,
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

fn generated_summary_semantic() -> MemorySemanticFields {
    MemorySemanticFields {
        intent: MemoryIntent::ImplicitCandidate,
        explicitness: MemoryExplicitness::Implicit,
        category: MemoryCategory::Custom,
        subject: MemorySubject::Custom,
        attribute: MemoryAttribute::Custom,
        subject_key: None,
        custom_subject: Some("thread_summary".to_owned()),
        custom_attribute: Some("summary".to_owned()),
        scope_hint: MemoryScopeHint::UserWorkspace,
        durability: MemoryDurability::Unknown,
        sensitivity: MemorySensitivityHint::None,
        certainty: MemoryExtractorCertainty::High,
    }
}

fn semantic_evidence(turn_id: &str) -> MemoryWriteEvidence {
    MemoryWriteEvidence {
        source_thread_id: Some("thread_semantic".to_owned()),
        source_turn_id: Some(turn_id.to_owned()),
        source_item_id: Some(format!("item_{turn_id}")),
        source_ref: Some(format!("turn:{turn_id}")),
        quote_or_span: Some("evidence quote".to_owned()),
        extractor_reason: Some("test semantic extraction".to_owned()),
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

fn metadata_evidence_count(metadata: &BTreeMap<String, serde_json::Value>) -> u64 {
    metadata
        .get("evidence")
        .and_then(|value| value.get("count"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

async fn attach_quality_decision(
    store: &CrudStore,
    memory_id: &str,
    action: MemoryQualityAction,
    target_ownership: MemoryOwnershipClass,
    source_context_kind: MemorySourceContextKind,
    fact_class: MemoryFactClass,
    lifetime_class: MemoryLifetimeClass,
    ownership_class: MemoryOwnershipClass,
    evidence_class: MemoryEvidenceClass,
    relation: MemoryWriteRelation,
    reason_codes: Vec<MemoryQualityReasonCode>,
    now: i64,
) {
    store
        .insert_agent_memory_quality_decision(NewAgentMemoryQualityDecision {
            workspace_id: None,
            thread_id: Some("thread_quality_fixture".to_owned()),
            turn_id: Some(format!("turn_quality_fixture_{now}")),
            item_id: None,
            task_id: None,
            memory_id: Some(memory_id.to_owned()),
            candidate_id: None,
            canonical_key: None,
            action,
            target_ownership,
            source_context_kind,
            fact_class,
            lifetime_class,
            ownership_class,
            evidence_class,
            relation,
            reason_codes,
            input_snapshot_json: None,
            created_at_unix: now,
            updated_at_unix: now,
        })
        .await
        .expect("insert quality decision fixture");
}

#[tokio::test]
async fn semantic_active_writes_persist_source_context_classes() {
    let (store, _backend, service) = setup_service().await;
    let mut semantic = relationship_semantic("direct-user-source-context");
    semantic.intent = MemoryIntent::ExplicitStore;
    semantic.explicitness = MemoryExplicitness::Explicit;
    let mut params = semantic_write_params(
        semantic,
        "Direct user relationship fact.",
        "direct-user-relationship-value",
        MemorySemanticWriteDisposition::AcceptActive,
        "turn_source_context_direct_user",
    );
    params.source_context_kind = Some(MemorySourceContextKind::DirectUserConversation);

    let response = service
        .write_semantic_memory(user_context(90), params)
        .await
        .expect("direct user semantic active write succeeds");
    let record = response.record.expect("active record");
    assert_eq!(
        record.source_context_kind,
        Some(MemorySourceContextKind::DirectUserConversation)
    );
    let stored = store
        .get_agent_memory_record(record.id.as_str(), false)
        .await
        .expect("load stored memory")
        .expect("stored memory exists");
    assert_eq!(
        stored.source_context_kind,
        Some(MemorySourceContextKind::DirectUserConversation)
    );

    let blocked_cases = [
        MemorySourceContextKind::AssistantResponse,
        MemorySourceContextKind::ToolResult,
        MemorySourceContextKind::TaskRuntime,
        MemorySourceContextKind::SystemRuntime,
        MemorySourceContextKind::Unknown,
    ];

    for (index, source_context_kind) in blocked_cases.into_iter().enumerate() {
        let mut semantic = relationship_semantic(format!("person-{index}").as_str());
        semantic.intent = MemoryIntent::ExplicitStore;
        semantic.explicitness = MemoryExplicitness::Explicit;
        let turn_id = format!("turn_source_context_blocked_{index}");
        let mut params = semantic_write_params(
            semantic,
            format!("Relationship fact {index}.").as_str(),
            format!("relationship-value-{index}").as_str(),
            MemorySemanticWriteDisposition::AcceptActive,
            turn_id.as_str(),
        );
        params.source_context_kind = Some(source_context_kind);

        let response = service
            .write_semantic_memory(user_context(91 + index as i64), params)
            .await
            .expect("semantic active write is quality-gated");
        assert!(response.record.is_none());
        assert!(response.candidate.is_none());

        let decisions = store
            .list_agent_memory_quality_decisions_for_thread("thread_semantic", 20)
            .await
            .expect("quality decisions");
        let decision = decisions
            .iter()
            .find(|decision| decision.turn_id.as_deref() == Some(turn_id.as_str()))
            .expect("quality decision for blocked source context");
        assert_ne!(decision.action, MemoryQualityAction::CandidatePolicy);
        assert_eq!(decision.source_context_kind, source_context_kind);
    }
}

#[tokio::test]
async fn candidate_approval_preserves_source_context() {
    let (store, _backend, service) = setup_service().await;
    let mut params = semantic_write_params(
        relationship_semantic("candidate-source-context"),
        "Candidate source context fact.",
        "candidate-source-context-value",
        MemorySemanticWriteDisposition::CreatePendingCandidate,
        "turn_candidate_source_context",
    );
    params.source_context_kind = Some(MemorySourceContextKind::DirectUserConversation);

    let candidate = service
        .write_semantic_memory(user_context(96), params)
        .await
        .expect("candidate write")
        .candidate
        .expect("candidate");
    assert_eq!(
        candidate.source_context_kind,
        Some(MemorySourceContextKind::DirectUserConversation)
    );

    let approved = service
        .approve_candidate(
            user_context(97),
            MemoryCandidatesApproveParams {
                candidate_id: candidate.id.clone(),
                reason: Some("approved in source context test".to_owned()),
                actor: None,
            },
        )
        .await
        .expect("approve candidate");

    assert_eq!(
        approved.record.source_context_kind,
        Some(MemorySourceContextKind::DirectUserConversation)
    );
    let stored = store
        .get_agent_memory_record(approved.record.id.as_str(), false)
        .await
        .expect("load approved memory")
        .expect("approved memory exists");
    assert_eq!(
        stored.source_context_kind,
        Some(MemorySourceContextKind::DirectUserConversation)
    );
}

#[tokio::test]
async fn remember_get_and_list_hydrate_content_from_backend() {
    let (store, _backend, service) = setup_service().await;
    let full_content = format!("User birthday is September 12. {}", "x".repeat(320));
    let remembered = service
        .remember(
            user_context(100),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user.birthday"),
                full_content.as_str(),
            ),
        )
        .await
        .expect("remember");

    assert_eq!(remembered.record.content, full_content);
    assert_eq!(
        remembered.record.provenance.created_by.unwrap().kind,
        MemoryActorKind::User
    );

    let loaded = service
        .get(
            user_context(101),
            MemoryGetParams {
                memory_id: remembered.record.id.clone(),
                include_deleted: false,
            },
        )
        .await
        .expect("get")
        .record
        .expect("record");
    assert_eq!(loaded.content, full_content);

    let listed = service
        .list(
            user_context(102),
            MemoryListParams {
                scopes: vec![scope(MemoryScopeKind::User, "default")],
                ..Default::default()
            },
        )
        .await
        .expect("list");
    assert_eq!(listed.records.len(), 1);
    assert_eq!(listed.records[0].content, full_content);

    let control = store
        .get_agent_memory_record(remembered.record.id.as_str(), true)
        .await
        .expect("load control row")
        .expect("control row");
    let preview = control.content_preview.expect("preview");
    assert!(preview.len() < full_content.len());
    assert!(full_content.starts_with(preview.as_str()));
}

#[tokio::test]
async fn list_get_and_forget_use_control_plane_visibility() {
    let (store, _backend, service) = setup_service().await;
    let mut params = remember_params(
        scope(MemoryScopeKind::User, "default"),
        Some("legacy.weather"),
        "User wants current Moscow weather sent to this chat every 15 minutes.",
    );
    params.category = MemoryCategory::Preference;
    params.source_context_kind = Some(MemorySourceContextKind::AssistantResponse);
    params.provenance = Some(MemoryProvenance {
        source_thread_id: Some("thread_legacy".to_owned()),
        source_turn_id: Some("turn_legacy".to_owned()),
        source_item_id: None,
        created_by: Some(MemoryActor {
            kind: MemoryActorKind::Assistant,
            id: None,
        }),
    });

    let remembered = service
        .remember(user_context(150), params)
        .await
        .expect("remember legacy memory");
    attach_quality_decision(
        &store,
        remembered.record.id.as_str(),
        MemoryQualityAction::ForceReject,
        MemoryOwnershipClass::Reject,
        MemorySourceContextKind::AssistantResponse,
        MemoryFactClass::StableUserPreference,
        MemoryLifetimeClass::LongLived,
        MemoryOwnershipClass::DurableUserMemory,
        MemoryEvidenceClass::AssistantInference,
        MemoryWriteRelation::Novel,
        vec![MemoryQualityReasonCode::AssistantInferenceNotDurableEvidence],
        151,
    )
    .await;

    let search = service
        .search(
            user_context(152),
            MemorySearchParams {
                query: "Moscow weather".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert!(search.hits.is_empty());

    let listed = service
        .list(user_context(153), MemoryListParams::default())
        .await
        .expect("list");
    assert_eq!(
        listed
            .records
            .iter()
            .map(|record| record.id.as_str())
            .collect::<Vec<_>>(),
        vec![remembered.record.id.as_str()]
    );

    let loaded = service
        .get(
            user_context(154),
            MemoryGetParams {
                memory_id: remembered.record.id.clone(),
                include_deleted: false,
            },
        )
        .await
        .expect("get")
        .record
        .expect("record");
    assert_eq!(loaded.content, remembered.record.content);

    let forgotten = service
        .forget(
            user_context(155),
            MemoryForgetParams {
                target: MemoryForgetTarget::Id {
                    memory_id: remembered.record.id.clone(),
                },
                reason: Some("cleanup".to_owned()),
                actor: None,
                dry_run: false,
            },
        )
        .await
        .expect("forget");
    assert_eq!(forgotten.forgotten_memory_ids, vec![remembered.record.id]);
}

#[tokio::test]
async fn keyed_remember_upserts_existing_active_memory() {
    let (store, _backend, service) = setup_service().await;
    let first = service
        .remember(
            user_context(200),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user.name"),
                "The user's name is Alexander.",
            ),
        )
        .await
        .expect("first remember");
    let second = service
        .remember(
            user_context(201),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user.name"),
                "The user's preferred name is Alexander.",
            ),
        )
        .await
        .expect("second remember");

    assert_eq!(first.record.id, second.record.id);
    assert!(!second.created);
    assert_eq!(
        second.record.content,
        "The user's preferred name is Alexander."
    );

    let events = store
        .list_agent_memory_events(first.record.id.as_str(), 10)
        .await
        .expect("events");
    assert!(events.iter().any(|event| event.event_kind == "created"));
    assert!(events.iter().any(|event| event.event_kind == "updated"));
}

#[tokio::test]
async fn remember_with_supersedes_marks_old_row_superseded() {
    let (store, _backend, service) = setup_service().await;
    let old = service
        .remember(
            user_context(300),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("project.style"),
                "Use casual prose.",
            ),
        )
        .await
        .expect("old remember");
    let mut replacement_params = remember_params(
        scope(MemoryScopeKind::User, "default"),
        Some("project.style"),
        "Use direct engineering prose.",
    );
    replacement_params.supersedes = Some(old.record.id.clone());
    let replacement = service
        .remember(user_context(301), replacement_params)
        .await
        .expect("replacement remember");

    let old_row = store
        .get_agent_memory_record(old.record.id.as_str(), true)
        .await
        .expect("old row")
        .expect("old exists");
    assert_eq!(old_row.status, MemoryStatus::Superseded);
    assert_eq!(old_row.active_key, None);

    let listed = service
        .list(user_context(302), MemoryListParams::default())
        .await
        .expect("list");
    assert_eq!(listed.records.len(), 1);
    assert_eq!(listed.records[0].id, replacement.record.id);

    let search = service
        .search(
            user_context(303),
            MemorySearchParams {
                query: "engineering prose".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert_eq!(search.hits.len(), 1);
    assert_eq!(search.hits[0].record.id, replacement.record.id);

    let decisions = store
        .list_agent_memory_policy_decisions_for_memory(replacement.record.id.as_str(), 10)
        .await
        .expect("policy decisions");
    assert!(
        decisions
            .iter()
            .any(|decision| { decision.action == "remember" && decision.decision == "allow" })
    );
}

#[tokio::test]
async fn semantic_identity_writes_share_canonical_key_and_merge_evidence() {
    let (store, _backend, service) = setup_service().await;
    let first = service
        .write_semantic_memory(
            user_context(310),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "Меня зовут Александр.",
                "Александр",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_semantic_1",
            ),
        )
        .await
        .expect("first semantic write");
    assert_eq!(first.relation, MemoryWriteRelation::Novel);
    assert!(first.created);
    assert_eq!(first.canonical_key.key, "user/global:identity:self:name");
    let first_record = first.record.expect("first record");

    let second = service
        .write_semantic_memory(
            user_context(311),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "My name is Alexander.",
                "Александр",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_semantic_2",
            ),
        )
        .await
        .expect("duplicate semantic write");
    assert_eq!(second.relation, MemoryWriteRelation::Duplicate);
    assert!(!second.created);
    assert!(second.evidence_merged);
    let second_record = second.record.expect("duplicate record");
    assert_eq!(second_record.id, first_record.id);
    assert_eq!(metadata_evidence_count(&second_record.metadata), 2);
    let decisions = store
        .list_agent_memory_policy_decisions_for_memory(second_record.id.as_str(), 20)
        .await
        .expect("semantic write policy decisions");
    assert!(
        decisions.iter().any(|decision| {
            decision.action == "semantic_write" && decision.decision == "novel"
        })
    );
    assert!(decisions.iter().any(|decision| {
        decision.action == "semantic_write" && decision.decision == "duplicate"
    }));

    let listed = service
        .list(
            user_context(312),
            MemoryListParams {
                scopes: vec![scope(MemoryScopeKind::User, "default")],
                categories: vec![MemoryCategory::Identity],
                ..Default::default()
            },
        )
        .await
        .expect("list identity memories");
    assert_eq!(listed.records.len(), 1);
}

#[tokio::test]
async fn semantic_concurrent_duplicate_writes_leave_one_active_memory() {
    let (_store, _backend, service) = setup_service().await;
    let service = Arc::new(service);
    let first_service = service.clone();
    let second_service = service.clone();

    let (first, second) = tokio::join!(
        first_service.write_semantic_memory(
            user_context(313),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "Меня зовут Александр.",
                "Александр",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_semantic_race_1",
            ),
        ),
        second_service.write_semantic_memory(
            user_context(313),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "My name is Alexander.",
                "Александр",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_semantic_race_2",
            ),
        )
    );
    first.expect("first concurrent semantic write");
    second.expect("second concurrent semantic write");

    let listed = service
        .list(
            user_context(314),
            MemoryListParams {
                scopes: vec![scope(MemoryScopeKind::User, "default")],
                categories: vec![MemoryCategory::Identity],
                ..Default::default()
            },
        )
        .await
        .expect("list identity memories");
    assert_eq!(listed.records.len(), 1);
    assert_eq!(
        listed.records[0].key.as_deref(),
        Some("user/global:identity:self:name")
    );
}

#[tokio::test]
async fn semantic_write_ignores_client_provided_free_form_key() {
    let (_store, _backend, service) = setup_service().await;
    let mut params = semantic_write_params(
        identity_name_semantic(MemoryExplicitness::Explicit),
        "User: Alexander.",
        "Alexander",
        MemorySemanticWriteDisposition::AcceptActive,
        "turn_semantic_key",
    );
    params.client_provided_key = Some("llm/freeform/user-name".to_owned());

    let response = service
        .write_semantic_memory(user_context(320), params)
        .await
        .expect("semantic write");
    let record = response.record.expect("record");
    assert_eq!(
        record.key.as_deref(),
        Some("user/global:identity:self:name")
    );
    assert_ne!(record.key.as_deref(), Some("llm/freeform/user-name"));
    assert_eq!(
        record
            .metadata
            .get("client_provided_key")
            .and_then(serde_json::Value::as_str),
        Some("llm/freeform/user-name")
    );
}

#[tokio::test]
async fn semantic_write_requires_evidence() {
    let (_store, _backend, service) = setup_service().await;
    let mut params = semantic_write_params(
        identity_name_semantic(MemoryExplicitness::Explicit),
        "User: Alexander.",
        "Alexander",
        MemorySemanticWriteDisposition::AcceptActive,
        "turn_semantic_no_evidence",
    );
    params.evidence = None;

    let error = service
        .write_semantic_memory(user_context(325), params)
        .await
        .expect_err("semantic writes without evidence must fail");
    assert!(error.to_string().contains("requires evidence"));
}

#[tokio::test]
async fn semantic_single_value_explicit_update_supersedes_same_key() {
    let (store, _backend, service) = setup_service().await;
    let first = service
        .write_semantic_memory(
            user_context(330),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "The user's name is Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_semantic_update_1",
            ),
        )
        .await
        .expect("first semantic write")
        .record
        .expect("first record");

    let second = service
        .write_semantic_memory(
            user_context(331),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "The user's name is Alex.",
                "Alex",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_semantic_update_2",
            ),
        )
        .await
        .expect("semantic update");

    assert_eq!(second.relation, MemoryWriteRelation::CompatibleUpdate);
    assert_eq!(
        second.superseded_memory_id.as_deref(),
        Some(first.id.as_str())
    );
    let second_record = second.record.expect("updated record");
    assert_eq!(
        second_record.key.as_deref(),
        Some("user/global:identity:self:name")
    );

    let old_row = store
        .get_agent_memory_record(first.id.as_str(), true)
        .await
        .expect("load old row")
        .expect("old row");
    assert_eq!(old_row.status, MemoryStatus::Superseded);

    let listed = service
        .list(
            user_context(332),
            MemoryListParams {
                scopes: vec![scope(MemoryScopeKind::User, "default")],
                categories: vec![MemoryCategory::Identity],
                ..Default::default()
            },
        )
        .await
        .expect("list active identity memories");
    assert_eq!(listed.records.len(), 1);
    assert_eq!(listed.records[0].id, second_record.id);
}

#[tokio::test]
async fn semantic_implicit_contradiction_does_not_create_active_duplicate() {
    let (_store, _backend, service) = setup_service().await;
    service
        .write_semantic_memory(
            user_context(340),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "The user's name is Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_semantic_contradiction_1",
            ),
        )
        .await
        .expect("first semantic write");

    let contradiction = service
        .write_semantic_memory(
            user_context(341),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Implicit),
                "Maybe the user's name is Alexey.",
                "Alexey",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_semantic_contradiction_2",
            ),
        )
        .await
        .expect("implicit contradiction");

    assert_eq!(contradiction.relation, MemoryWriteRelation::Contradiction);
    assert!(contradiction.record.is_none());
    assert_eq!(
        contradiction
            .candidate
            .as_ref()
            .expect("suppressed candidate")
            .status,
        MemoryCandidateStatus::ReviewDisabledRejected
    );

    let listed = service
        .list(
            user_context(342),
            MemoryListParams {
                scopes: vec![scope(MemoryScopeKind::User, "default")],
                categories: vec![MemoryCategory::Identity],
                ..Default::default()
            },
        )
        .await
        .expect("list active identity memories");
    assert_eq!(listed.records.len(), 1);
    assert!(listed.records[0].content.contains("Alexander"));
}

#[tokio::test]
async fn semantic_pending_candidate_duplicates_merge_evidence() {
    let (_store, _backend, service) = setup_service().await;
    let first = service
        .write_semantic_memory(
            user_context(360),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Implicit),
                "The user's name may be Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::CreatePendingCandidate,
                "turn_semantic_pending_1",
            ),
        )
        .await
        .expect("first candidate");
    assert_eq!(first.relation, MemoryWriteRelation::Novel);
    let first_candidate = first.candidate.expect("first pending candidate");

    let second = service
        .write_semantic_memory(
            user_context(361),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Implicit),
                "User is called Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::CreatePendingCandidate,
                "turn_semantic_pending_2",
            ),
        )
        .await
        .expect("duplicate candidate");
    assert_eq!(second.relation, MemoryWriteRelation::Duplicate);
    assert!(second.evidence_merged);
    let second_candidate = second.candidate.expect("merged pending candidate");
    assert_eq!(second_candidate.id, first_candidate.id);
    assert_eq!(metadata_evidence_count(&second_candidate.metadata), 2);
}

#[tokio::test]
async fn semantic_route_to_candidate_policy_applies_default_review_disabled_policy() {
    let (_store, _backend, service) = setup_service().await;
    let response = service
        .write_semantic_memory(
            user_context(365),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Implicit),
                "The user's name may be Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_semantic_policy_boundary",
            ),
        )
        .await
        .expect("policy boundary write");
    assert_eq!(response.relation, MemoryWriteRelation::Novel);
    assert!(response.record.is_none());
    let candidate = response.candidate.expect("review-disabled candidate");
    assert_eq!(
        candidate.status,
        MemoryCandidateStatus::ReviewDisabledRejected
    );
    assert_eq!(
        candidate
            .metadata
            .get("candidate_policy_reason_code")
            .and_then(serde_json::Value::as_str),
        Some("implicit_auto_approve_disabled")
    );
}

#[tokio::test]
async fn semantic_route_explicit_durable_fact_auto_approves() {
    let (store, _backend, service) = setup_service().await;
    let response = service
        .write_semantic_memory(
            user_context(366),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "Запомни, меня зовут Александр.",
                "Александр",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_semantic_policy_auto_approve",
            ),
        )
        .await
        .expect("policy write");

    assert_eq!(response.relation, MemoryWriteRelation::Novel);
    assert!(response.candidate.is_none());
    let response_route = response.route.as_ref().expect("durable route info");
    assert_eq!(
        response_route.route,
        MemorySemanticWriteRoute::DurableControlPlane
    );
    assert_eq!(
        response_route.quality_action,
        MemoryQualityAction::CandidatePolicy
    );
    assert!(response_route.quality_decision_id.is_some());
    let record = response.record.expect("auto-approved record");
    assert_eq!(
        record
            .metadata
            .get("candidate_score_bucket")
            .and_then(serde_json::Value::as_str),
        Some("high")
    );
    let score = record
        .metadata
        .get("candidate_score")
        .expect("candidate score metadata");
    assert_eq!(
        score
            .get("score_version")
            .and_then(serde_json::Value::as_str),
        Some("quality_v1")
    );
    assert!(
        score
            .get("source_trust_score")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or_default()
            > 0.0
    );
    assert!(
        score
            .get("ownership_fit_score")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or_default()
            > 0.0
    );
    let policy = record
        .metadata
        .get("candidate_policy")
        .expect("candidate policy metadata");
    assert_eq!(
        policy
            .pointer("/input/quality_action")
            .and_then(serde_json::Value::as_str),
        Some("candidate_policy")
    );
    assert_eq!(
        policy
            .pointer("/input/fact_class")
            .and_then(serde_json::Value::as_str),
        Some("user_identity")
    );
    assert_eq!(
        policy
            .pointer("/input/evidence_class")
            .and_then(serde_json::Value::as_str),
        Some("direct_user_assertion")
    );
    assert_eq!(
        record
            .metadata
            .get("candidate_policy_decision")
            .and_then(serde_json::Value::as_str),
        Some("auto_approve")
    );

    let decisions = store
        .list_agent_memory_policy_decisions_for_memory(record.id.as_str(), 20)
        .await
        .expect("policy decisions");
    let decision = decisions
        .iter()
        .find(|decision| {
            decision.action == "candidate_policy" && decision.decision == "auto_approve"
        })
        .expect("candidate policy decision");
    let details: serde_json::Value = serde_json::from_str(
        decision
            .details_json
            .as_deref()
            .expect("policy decision details"),
    )
    .expect("policy decision details json");
    assert_eq!(
        details
            .pointer("/score/score_version")
            .and_then(serde_json::Value::as_str),
        Some("quality_v1")
    );
    assert_eq!(
        details
            .pointer("/input/quality_reason_codes/0")
            .and_then(serde_json::Value::as_str),
        Some("candidate_policy_allowed")
    );
}

#[tokio::test]
async fn semantic_write_responses_include_route_for_duplicate_and_candidate_paths() {
    let (_store, _backend, service) = setup_service().await;
    let first = service
        .write_semantic_memory(
            user_context(400),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "The user's name is Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_route_response_first",
            ),
        )
        .await
        .expect("first write");
    assert_eq!(
        first.route.as_ref().map(|route| route.route),
        Some(MemorySemanticWriteRoute::DurableControlPlane)
    );

    let duplicate = service
        .write_semantic_memory(
            user_context(401),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "The user's name is Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_route_response_duplicate",
            ),
        )
        .await
        .expect("duplicate write");
    assert_eq!(duplicate.relation, MemoryWriteRelation::Duplicate);
    assert!(duplicate.evidence_merged);
    let duplicate_route = duplicate.route.expect("duplicate route");
    assert_eq!(duplicate_route.route, MemorySemanticWriteRoute::Rejected);
    assert_eq!(
        duplicate_route.quality_action,
        MemoryQualityAction::ForceReject
    );
    assert!(duplicate_route.quality_decision_id.is_some());

    let pending = service
        .write_semantic_memory(
            user_context(402),
            semantic_write_params(
                user_preference_semantic(MemoryExplicitness::Unclear),
                "Maybe the user prefers concise reviews.",
                "concise reviews",
                MemorySemanticWriteDisposition::CreatePendingCandidate,
                "turn_route_response_pending",
            ),
        )
        .await
        .expect("pending write");
    assert!(pending.candidate.is_some());
    assert_eq!(
        pending.route.as_ref().map(|route| route.route),
        Some(MemorySemanticWriteRoute::DurableControlPlane)
    );

    let pending_duplicate = service
        .write_semantic_memory(
            user_context(403),
            semantic_write_params(
                user_preference_semantic(MemoryExplicitness::Unclear),
                "Maybe the user prefers concise reviews.",
                "concise reviews",
                MemorySemanticWriteDisposition::CreatePendingCandidate,
                "turn_route_response_pending_duplicate",
            ),
        )
        .await
        .expect("pending duplicate write");
    assert_eq!(pending_duplicate.relation, MemoryWriteRelation::Duplicate);
    let pending_duplicate_route = pending_duplicate.route.expect("pending duplicate route");
    assert_eq!(
        pending_duplicate_route.route,
        MemorySemanticWriteRoute::Rejected
    );
    assert_eq!(
        pending_duplicate_route.quality_action,
        MemoryQualityAction::ForceReject
    );
}

#[tokio::test]
async fn semantic_route_durable_workspace_fact_uses_control_plane_route() {
    let (_store, _backend, service) = setup_service().await;
    let mut params = semantic_write_params(
        workspace_project_decision_semantic(MemoryExplicitness::Explicit),
        "Workspace migration policy is to keep database changes in the existing migration file.",
        "keep database changes in the existing migration file",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        "turn_semantic_route_workspace_durable",
    );
    params.scope = scope(MemoryScopeKind::Workspace, "ws_route_workspace");

    let response = service
        .write_semantic_memory(workspace_context("ws_route_workspace", 381), params)
        .await
        .expect("workspace durable write");

    assert_eq!(response.relation, MemoryWriteRelation::Novel);
    assert!(response.candidate.is_none());
    let record = response.record.expect("workspace durable record");
    assert_eq!(record.scope.kind, MemoryScopeKind::Workspace);
    assert_eq!(record.scope.key, "ws_route_workspace");
    assert_eq!(
        record
            .metadata
            .get("candidate_policy_decision")
            .and_then(serde_json::Value::as_str),
        Some("auto_approve")
    );
    assert_eq!(
        record
            .metadata
            .get("candidate_score")
            .and_then(|score| score.get("ownership_fit_score"))
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|score| score > 0.0),
        true
    );
}

#[tokio::test]
async fn semantic_route_durable_agent_fact_uses_control_plane_route() {
    let (_store, _backend, service) = setup_service().await;
    let agent_id = "agent_research";
    let workspace_id = "ws_route_agent";
    let agent_scope_key = workspace_agent_memory_scope_key(workspace_id, agent_id);
    let mut params = semantic_write_params(
        agent_self_description_semantic(MemoryExplicitness::Explicit),
        "This agent is named Pioneer.",
        "Pioneer",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        "turn_semantic_route_agent_durable",
    );
    params.scope = scope(MemoryScopeKind::Agent, agent_scope_key.as_str());

    let response = service
        .write_semantic_memory(agent_workspace_context(workspace_id, agent_id, 382), params)
        .await
        .expect("agent durable write");

    assert_eq!(response.relation, MemoryWriteRelation::Novel);
    assert!(response.candidate.is_none());
    let record = response.record.expect("agent durable record");
    assert_eq!(record.scope.kind, MemoryScopeKind::Agent);
    assert_eq!(record.scope.key, agent_scope_key);
    assert_eq!(
        record
            .metadata
            .get("candidate_policy_decision")
            .and_then(serde_json::Value::as_str),
        Some("auto_approve")
    );
    assert_eq!(
        record
            .metadata
            .get("candidate_policy")
            .and_then(|policy| policy.pointer("/input/ownership_class"))
            .and_then(serde_json::Value::as_str),
        Some("durable_agent_memory")
    );
}

#[tokio::test]
async fn semantic_route_non_durable_fact_cannot_use_durable_control_plane() {
    let (store, _backend, service) = setup_service().await;
    let mut semantic = thread_local_todo_semantic();
    semantic.explicitness = MemoryExplicitness::Explicit;
    semantic.intent = MemoryIntent::ExplicitStore;
    semantic.certainty = MemoryExtractorCertainty::High;

    let response = service
        .write_semantic_memory(
            user_context(383),
            semantic_write_params(
                semantic,
                "For this thread, follow up on the current debugging branch.",
                "follow up on current debugging branch",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_semantic_route_thread_not_durable",
            ),
        )
        .await
        .expect("thread route write");

    assert_eq!(response.relation, MemoryWriteRelation::Novel);
    assert!(response.record.is_none());
    assert!(response.candidate.is_none());

    let decisions = store
        .list_agent_memory_quality_decisions_for_thread("thread_semantic", 20)
        .await
        .expect("quality decisions");
    let decision = decisions
        .iter()
        .find(|decision| {
            decision.turn_id.as_deref() == Some("turn_semantic_route_thread_not_durable")
        })
        .expect("thread route quality decision");
    assert_eq!(decision.action, MemoryQualityAction::RouteToThreadEpisodic);
}

#[tokio::test]
async fn semantic_write_extractor_ontology_proposal_is_advisory_only() {
    let (store, _backend, service) = setup_service().await;
    let turn_id = "turn_extractor_proposal_advisory";
    let mut params = semantic_write_params(
        tool_result_semantic(),
        "Tool observed a transient project state.",
        "transient project state",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        turn_id,
    );
    params.source_context_kind = Some(MemorySourceContextKind::ToolResult);
    params.metadata.insert(
        "extractor_ontology_proposal".to_owned(),
        serde_json::json!({
            "fact_class": "user_identity",
            "lifetime_class": "long_lived",
            "evidence_class": "direct_user_assertion",
            "proposed_ownership_class": "durable_user_memory"
        }),
    );

    let response = service
        .write_semantic_memory(workspace_context("ws_extractor_proposal", 384), params)
        .await
        .expect("semantic write succeeds");

    assert!(response.record.is_none());
    assert!(response.candidate.is_none());
    let route = response.route.expect("quality route");
    assert_eq!(route.route, MemorySemanticWriteRoute::DomainStateDeferred);
    assert_eq!(
        route.target_ownership,
        MemoryOwnershipClass::DomainRuntimeState
    );

    let decisions = store
        .list_agent_memory_quality_decisions_for_thread("thread_semantic", 20)
        .await
        .expect("quality decisions");
    let decision = decisions
        .iter()
        .find(|decision| decision.turn_id.as_deref() == Some(turn_id))
        .expect("quality decision for advisory proposal");
    assert_eq!(decision.fact_class, MemoryFactClass::ToolResultFact);
    assert_eq!(
        decision.lifetime_class,
        MemoryLifetimeClass::NaturallyExpiring
    );
    assert_eq!(
        decision.ownership_class,
        MemoryOwnershipClass::DomainRuntimeState
    );
    let snapshot: serde_json::Value = serde_json::from_str(
        decision
            .input_snapshot_json
            .as_deref()
            .expect("quality input snapshot"),
    )
    .expect("quality input snapshot json");
    assert_eq!(
        snapshot.pointer("/extractor_ontology_proposal/fact_class"),
        Some(&serde_json::json!("user_identity"))
    );
    assert_eq!(
        snapshot.pointer("/extractor_ontology_proposal_comparison/all_match"),
        Some(&serde_json::json!(false))
    );
}

#[tokio::test]
async fn semantic_thread_episodic_route_response_carries_deferred_refs() {
    let (_store, _backend, service) = setup_service().await;
    let turn_id = "turn_thread_episodic_route_info";
    let response = service
        .write_semantic_memory(
            user_context(384),
            semantic_write_params(
                thread_local_todo_semantic(),
                "This thread should remember to revisit the active debugging branch.",
                "revisit active debugging branch",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                turn_id,
            ),
        )
        .await
        .expect("thread episodic route write");

    assert!(response.record.is_none());
    assert!(response.candidate.is_none());
    assert!(!response.created);
    let route = response.route.expect("thread route info");
    assert_eq!(
        route.route,
        MemorySemanticWriteRoute::ThreadEpisodicDeferred
    );
    assert_eq!(
        route.quality_action,
        MemoryQualityAction::RouteToThreadEpisodic
    );
    assert_eq!(
        route.target_ownership,
        pioneer_protocol::MemoryOwnershipClass::ThreadEpisodicContext
    );
    assert!(route.quality_decision_id.is_some());
    assert_eq!(route.thread_id.as_deref(), Some("thread_semantic"));
    assert_eq!(route.source_turn_id.as_deref(), Some(turn_id));
    assert_eq!(
        route.source_item_id.as_deref(),
        Some("item_turn_thread_episodic_route_info")
    );
    assert_eq!(
        route.canonical_key.as_deref(),
        Some("user/default:todo:self:custom_4de127d381235b26")
    );
}

#[tokio::test]
async fn semantic_generated_summary_routes_to_thread_episodic_deferral() {
    let (_store, _backend, service) = setup_service().await;
    let mut params = semantic_write_params(
        generated_summary_semantic(),
        "Summary: the current thread is debugging OpenRouter tool calls.",
        "debugging OpenRouter tool calls",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        "turn_generated_summary_thread_route",
    );
    params.source_context_kind = Some(MemorySourceContextKind::GeneratedSummary);

    let response = service
        .write_semantic_memory(user_context(385), params)
        .await
        .expect("generated summary route");

    assert!(response.record.is_none());
    assert!(response.candidate.is_none());
    let route = response.route.expect("generated summary route info");
    assert_eq!(
        route.route,
        MemorySemanticWriteRoute::ThreadEpisodicDeferred
    );
    assert_eq!(
        route.quality_action,
        MemoryQualityAction::RouteToThreadEpisodic
    );

    for query in [
        "debugging OpenRouter tool calls",
        "current thread is debugging OpenRouter",
    ] {
        let search = service
            .search(
                user_context(386),
                MemorySearchParams {
                    query: query.to_owned(),
                    ..Default::default()
                },
            )
            .await
            .expect("search");
        assert!(search.hits.is_empty(), "{query}");

        let recall = service
            .recall_for_prompt(
                user_context(387),
                MemoryRecallParams {
                    query: query.to_owned(),
                    ..Default::default()
                },
            )
            .await
            .expect("recall");
        assert!(recall.items.is_empty(), "{query}");
    }
}

#[tokio::test]
async fn semantic_task_state_route_response_is_deferred_and_not_memory() {
    let (store, _backend, service) = setup_service().await;
    let turn_id = "turn_task_state_route";
    let mut params = semantic_write_params(
        task_lifecycle_semantic(),
        "Task runtime state: child task is waiting for provider output.",
        "child task waiting for provider output",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        turn_id,
    );
    params.source_context_kind = Some(MemorySourceContextKind::TaskRuntime);

    let response = service
        .write_semantic_memory(workspace_context("ws_route_task", 388), params)
        .await
        .expect("task state route");

    assert!(response.record.is_none());
    assert!(response.candidate.is_none());
    let route = response.route.expect("task route info");
    assert_eq!(route.route, MemorySemanticWriteRoute::TaskStateDeferred);
    assert_eq!(route.quality_action, MemoryQualityAction::RouteToTaskState);
    assert_eq!(
        route.target_ownership,
        pioneer_protocol::MemoryOwnershipClass::TaskRuntimeState
    );
    assert!(route.quality_decision_id.is_some());
    assert_eq!(route.thread_id.as_deref(), Some("thread_semantic"));
    assert_eq!(route.source_turn_id.as_deref(), Some(turn_id));

    let decisions = store
        .list_agent_memory_quality_decisions_for_thread("thread_semantic", 20)
        .await
        .expect("quality decisions");
    let decision = decisions
        .iter()
        .find(|decision| decision.turn_id.as_deref() == Some(turn_id))
        .expect("task state decision");
    assert_eq!(decision.action, MemoryQualityAction::RouteToTaskState);
    assert_eq!(
        decision.target_ownership,
        pioneer_protocol::MemoryOwnershipClass::TaskRuntimeState
    );

    let search = service
        .search(
            workspace_context("ws_route_task", 389),
            MemorySearchParams {
                query: "child task waiting for provider output".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert!(search.hits.is_empty());
}

#[tokio::test]
async fn semantic_domain_state_routes_are_deferred_and_not_memory() {
    let (store, _backend, service) = setup_service().await;
    let mut tool_params = semantic_write_params(
        tool_result_semantic(),
        "Tool observation: the OpenRouter request took 118 seconds.",
        "OpenRouter request took 118 seconds",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        "turn_domain_tool_route",
    );
    tool_params.scope = scope(MemoryScopeKind::Workspace, "ws_route_domain");
    tool_params.source_context_kind = Some(MemorySourceContextKind::ToolResult);

    let tool_response = service
        .write_semantic_memory(workspace_context("ws_route_domain", 390), tool_params)
        .await
        .expect("tool domain route");
    assert!(tool_response.record.is_none());
    assert!(tool_response.candidate.is_none());
    let tool_route = tool_response.route.expect("tool route info");
    assert_eq!(
        tool_route.route,
        MemorySemanticWriteRoute::DomainStateDeferred
    );
    assert_eq!(
        tool_route.quality_action,
        MemoryQualityAction::RouteToDomainState
    );
    assert_eq!(
        tool_route.target_ownership,
        pioneer_protocol::MemoryOwnershipClass::DomainRuntimeState
    );

    let mut operational_semantic = operational_observation_semantic();
    operational_semantic.explicitness = MemoryExplicitness::Explicit;
    operational_semantic.intent = MemoryIntent::ExplicitStore;
    let mut operational_params = semantic_write_params(
        operational_semantic,
        "Operational observation: provider latency is temporarily elevated.",
        "provider latency temporarily elevated",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        "turn_domain_operational_route",
    );
    operational_params.scope = scope(MemoryScopeKind::Workspace, "ws_route_domain");
    operational_params.source_context_kind = Some(MemorySourceContextKind::ToolResult);

    let operational_response = service
        .write_semantic_memory(
            workspace_context("ws_route_domain", 391),
            operational_params,
        )
        .await
        .expect("operational domain route");
    assert!(operational_response.record.is_none());
    assert!(operational_response.candidate.is_none());
    let operational_route = operational_response.route.expect("operational route info");
    assert_eq!(
        operational_route.route,
        MemorySemanticWriteRoute::DomainStateDeferred
    );
    assert_eq!(
        operational_route.quality_action,
        MemoryQualityAction::RouteToDomainState
    );

    let decisions = store
        .list_agent_memory_quality_decisions_for_thread("thread_semantic", 30)
        .await
        .expect("quality decisions");
    assert!(decisions.iter().any(|decision| {
        decision.turn_id.as_deref() == Some("turn_domain_tool_route")
            && decision.action == MemoryQualityAction::RouteToDomainState
    }));
    assert!(decisions.iter().any(|decision| {
        decision.turn_id.as_deref() == Some("turn_domain_operational_route")
            && decision.action == MemoryQualityAction::RouteToDomainState
    }));

    for query in [
        "OpenRouter request took 118 seconds",
        "provider latency temporarily elevated",
    ] {
        let recall = service
            .recall_for_prompt(
                workspace_context("ws_route_domain", 392),
                MemoryRecallParams {
                    query: query.to_owned(),
                    ..Default::default()
                },
            )
            .await
            .expect("recall");
        assert!(recall.items.is_empty(), "{query}");
    }
}

#[tokio::test]
async fn semantic_route_implicit_durable_fact_auto_approves_when_enabled() {
    let mut config = MemoryServiceConfig::default();
    config.candidate_policy.allow_implicit_auto_approve = true;
    let (_store, _backend, service) = setup_service_with_config(config).await;

    let response = service
        .write_semantic_memory(
            user_context(367),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Implicit),
                "The user's name is Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_semantic_policy_implicit_auto_approve",
            ),
        )
        .await
        .expect("policy write");

    assert_eq!(response.relation, MemoryWriteRelation::Novel);
    assert!(response.candidate.is_none());
    assert!(response.record.is_some());
}

#[tokio::test]
async fn semantic_route_extremely_low_and_secret_facts_auto_reject() {
    let (store, _backend, service) = setup_service().await;
    let mut transient_semantic = identity_name_semantic(MemoryExplicitness::Explicit);
    transient_semantic.durability = MemoryDurability::Transient;
    let transient = service
        .write_semantic_memory(
            user_context(368),
            semantic_write_params(
                transient_semantic,
                "Right now the user might be called Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_semantic_policy_transient",
            ),
        )
        .await
        .expect("transient policy write");
    assert!(transient.record.is_none());
    assert!(transient.candidate.is_none());

    let mut secret_semantic = identity_name_semantic(MemoryExplicitness::Explicit);
    secret_semantic.sensitivity = MemorySensitivityHint::Secret;
    let secret = service
        .write_semantic_memory(
            user_context(369),
            semantic_write_params(
                secret_semantic,
                "User token is abc123.",
                "abc123",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_semantic_policy_secret",
            ),
        )
        .await
        .expect("secret policy write");
    assert!(secret.record.is_none());
    assert!(secret.candidate.is_none());

    let decisions = store
        .list_agent_memory_quality_decisions_for_thread("thread_semantic", 20)
        .await
        .expect("quality decisions");
    let transient_decision = decisions
        .iter()
        .find(|decision| decision.turn_id.as_deref() == Some("turn_semantic_policy_transient"))
        .expect("transient quality decision");
    assert_eq!(transient_decision.action, MemoryQualityAction::ForceReject);
    assert!(
        transient_decision
            .reason_codes
            .contains(&MemoryQualityReasonCode::NonDurableLifetime)
    );
    let secret_decision = decisions
        .iter()
        .find(|decision| decision.turn_id.as_deref() == Some("turn_semantic_policy_secret"))
        .expect("secret quality decision");
    assert_eq!(secret_decision.action, MemoryQualityAction::ForceReject);
    assert!(
        secret_decision
            .reason_codes
            .contains(&MemoryQualityReasonCode::SecretOrCredential)
    );
}

#[tokio::test]
async fn semantic_force_reject_response_is_rejected_terminal_route() {
    let (_store, _backend, service) = setup_service().await;
    let mut secret_semantic = identity_name_semantic(MemoryExplicitness::Explicit);
    secret_semantic.sensitivity = MemorySensitivityHint::Secret;

    let response = service
        .write_semantic_memory(
            user_context(393),
            semantic_write_params(
                secret_semantic,
                "User API token is abc123.",
                "abc123",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_force_reject_route",
            ),
        )
        .await
        .expect("secret force reject");

    assert!(!response.created);
    assert!(response.record.is_none());
    assert!(response.candidate.is_none());
    let route = response.route.expect("reject route info");
    assert_eq!(route.route, MemorySemanticWriteRoute::Rejected);
    assert_eq!(route.quality_action, MemoryQualityAction::ForceReject);
    assert_eq!(
        route.target_ownership,
        pioneer_protocol::MemoryOwnershipClass::Reject
    );
    assert!(route.quality_decision_id.is_some());

    let search = service
        .search(
            user_context(394),
            MemorySearchParams {
                query: "abc123".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert!(search.hits.is_empty());
}

#[tokio::test]
async fn semantic_quarantine_response_is_audit_only_terminal_route() {
    let (_store, _backend, service) = setup_service().await;
    let response = service
        .write_semantic_memory(
            user_context(395),
            semantic_write_params(
                unknown_custom_semantic(),
                "Unknown custom fact should stay audit-only.",
                "unknown custom fact",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_quarantine_audit_only_route",
            ),
        )
        .await
        .expect("quarantine route");

    assert!(!response.created);
    assert!(response.record.is_none());
    assert!(response.candidate.is_none());
    let route = response.route.expect("audit-only route info");
    assert_eq!(route.route, MemorySemanticWriteRoute::AuditOnly);
    assert_eq!(route.quality_action, MemoryQualityAction::Quarantine);
    assert_eq!(
        route.target_ownership,
        pioneer_protocol::MemoryOwnershipClass::AuditOnly
    );
    assert!(route.quality_decision_id.is_some());

    let recall = service
        .recall_for_prompt(
            user_context(396),
            MemoryRecallParams {
                query: "unknown custom fact".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("recall");
    assert!(recall.items.is_empty());
}

#[tokio::test]
async fn semantic_weak_evidence_and_unknown_lifetime_do_not_create_review_candidates() {
    let (store, _backend, service) = setup_service().await;
    let mut weak_params = semantic_write_params(
        identity_name_semantic(MemoryExplicitness::Explicit),
        "User name might be Alexander.",
        "Alexander",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        "turn_weak_evidence_terminal",
    );
    weak_params.source_context_kind = Some(MemorySourceContextKind::Unknown);

    let weak = service
        .write_semantic_memory(user_context(397), weak_params)
        .await
        .expect("weak evidence write");
    assert!(weak.record.is_none());
    assert!(weak.candidate.is_none());
    let weak_route = weak.route.expect("weak evidence route");
    assert_eq!(weak_route.route, MemorySemanticWriteRoute::Rejected);
    assert_eq!(weak_route.quality_action, MemoryQualityAction::ForceReject);

    let mut unknown_lifetime_semantic = identity_name_semantic(MemoryExplicitness::Explicit);
    unknown_lifetime_semantic.durability = MemoryDurability::Unknown;
    let unknown_lifetime = service
        .write_semantic_memory(
            user_context(398),
            semantic_write_params(
                unknown_lifetime_semantic,
                "User name is Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_unknown_lifetime_quarantine",
            ),
        )
        .await
        .expect("unknown lifetime write");
    assert!(unknown_lifetime.record.is_none());
    assert!(unknown_lifetime.candidate.is_none());
    let unknown_lifetime_route = unknown_lifetime.route.expect("unknown lifetime route");
    assert_eq!(
        unknown_lifetime_route.route,
        MemorySemanticWriteRoute::AuditOnly
    );
    assert_eq!(
        unknown_lifetime_route.quality_action,
        MemoryQualityAction::Quarantine
    );

    let listed = service
        .list_candidates(
            user_context(399),
            pioneer_protocol::MemoryCandidatesListParams::default(),
        )
        .await
        .expect("list candidates");
    assert!(listed.candidates.is_empty());

    let decisions = store
        .list_agent_memory_quality_decisions_for_thread("thread_semantic", 20)
        .await
        .expect("quality decisions");
    assert!(decisions.iter().any(|decision| {
        decision.turn_id.as_deref() == Some("turn_weak_evidence_terminal")
            && decision.action == MemoryQualityAction::ForceReject
    }));
    assert!(decisions.iter().any(|decision| {
        decision.turn_id.as_deref() == Some("turn_unknown_lifetime_quarantine")
            && decision.action == MemoryQualityAction::Quarantine
    }));
}

#[tokio::test]
async fn semantic_quality_gate_routes_and_quarantines_without_exposing_memory() {
    let (store, _backend, service) = setup_service().await;
    let routed_thread = service
        .write_semantic_memory(
            user_context(370),
            semantic_write_params(
                thread_local_todo_semantic(),
                "Thread-only follow-up should stay out of durable memory.",
                "thread-only follow-up",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_quality_route_thread",
            ),
        )
        .await
        .expect("thread route write");
    assert!(routed_thread.record.is_none());
    assert!(routed_thread.candidate.is_none());

    let mut task_params = semantic_write_params(
        task_lifecycle_semantic(),
        "Task runtime state should stay out of durable memory.",
        "task runtime state",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        "turn_quality_route_task",
    );
    task_params.source_context_kind = Some(MemorySourceContextKind::TaskRuntime);
    let routed_task = service
        .write_semantic_memory(workspace_context("ws_quality", 371), task_params)
        .await
        .expect("task route write");
    assert!(routed_task.record.is_none());
    assert!(routed_task.candidate.is_none());

    let mut tool_params = semantic_write_params(
        tool_result_semantic(),
        "Tool observation should stay out of durable memory.",
        "tool observation",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        "turn_quality_route_tool",
    );
    tool_params.scope = scope(MemoryScopeKind::Workspace, "ws_quality");
    tool_params.source_context_kind = Some(MemorySourceContextKind::ToolResult);
    let routed_tool = service
        .write_semantic_memory(workspace_context("ws_quality", 372), tool_params)
        .await
        .expect("tool route write");
    assert!(routed_tool.record.is_none());
    assert!(routed_tool.candidate.is_none());

    let quarantined = service
        .write_semantic_memory(
            user_context(373),
            semantic_write_params(
                unknown_custom_semantic(),
                "Unknown custom fact should be quarantined.",
                "unknown custom fact",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_quality_quarantine_unknown",
            ),
        )
        .await
        .expect("quarantine write");
    assert!(quarantined.record.is_none());
    assert!(quarantined.candidate.is_none());

    let decisions = store
        .list_agent_memory_quality_decisions_for_thread("thread_semantic", 20)
        .await
        .expect("quality decisions");
    assert!(decisions.iter().any(|decision| {
        decision.turn_id.as_deref() == Some("turn_quality_route_thread")
            && decision.action == MemoryQualityAction::RouteToThreadEpisodic
    }));
    assert!(decisions.iter().any(|decision| {
        decision.turn_id.as_deref() == Some("turn_quality_route_task")
            && decision.action == MemoryQualityAction::RouteToTaskState
    }));
    assert!(decisions.iter().any(|decision| {
        decision.turn_id.as_deref() == Some("turn_quality_route_tool")
            && decision.action == MemoryQualityAction::RouteToDomainState
    }));
    assert!(decisions.iter().any(|decision| {
        decision.turn_id.as_deref() == Some("turn_quality_quarantine_unknown")
            && decision.action == MemoryQualityAction::Quarantine
            && decision
                .reason_codes
                .contains(&MemoryQualityReasonCode::UnknownFactClass)
    }));

    for query in [
        "thread-only follow-up",
        "task runtime state",
        "tool observation",
        "unknown custom fact",
    ] {
        let search = service
            .search(
                workspace_context("ws_quality", 374),
                MemorySearchParams {
                    query: query.to_owned(),
                    ..Default::default()
                },
            )
            .await
            .expect("search");
        assert!(search.hits.is_empty(), "{query}");

        let recall = service
            .recall_for_prompt(
                workspace_context("ws_quality", 375),
                MemoryRecallParams {
                    query: query.to_owned(),
                    ..Default::default()
                },
            )
            .await
            .expect("recall");
        assert!(recall.items.is_empty(), "{query}");
    }
}

#[tokio::test]
async fn semantic_route_middle_fact_rejects_by_default_and_routes_when_review_enabled() {
    let (_store, _backend, service) = setup_service().await;
    let mut middle_semantic = user_preference_semantic(MemoryExplicitness::Unclear);
    middle_semantic.intent = MemoryIntent::ImplicitCandidate;
    middle_semantic.certainty = MemoryExtractorCertainty::Medium;

    let default_response = service
        .write_semantic_memory(
            user_context(372),
            semantic_write_params(
                middle_semantic.clone(),
                "Maybe the user prefers terse answers.",
                "terse answers",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_semantic_policy_middle",
            ),
        )
        .await
        .expect("middle policy write");
    let default_candidate = default_response.candidate.expect("middle candidate");
    assert_eq!(
        default_candidate.status,
        MemoryCandidateStatus::ReviewDisabledRejected
    );
    assert_eq!(
        default_candidate
            .metadata
            .get("candidate_policy_reason_code")
            .and_then(serde_json::Value::as_str),
        Some("review_disabled_middle_confidence")
    );
    assert_eq!(
        default_candidate
            .metadata
            .get("candidate_score")
            .and_then(|score| score.get("score_version"))
            .and_then(serde_json::Value::as_str),
        Some("quality_v1")
    );
    assert!(
        default_candidate
            .metadata
            .get("candidate_score")
            .and_then(|score| score.get("penalty_score"))
            .and_then(serde_json::Value::as_f64)
            .unwrap_or_default()
            > 0.0
    );

    let listed = service
        .list_candidates(
            user_context(373),
            pioneer_protocol::MemoryCandidatesListParams::default(),
        )
        .await
        .expect("default list candidates");
    assert!(listed.candidates.iter().all(|candidate| !matches!(
        candidate.status,
        MemoryCandidateStatus::PendingSilent
            | MemoryCandidateStatus::AskOnUse
            | MemoryCandidateStatus::NeedsReview
    )));

    let mut config = MemoryServiceConfig::default();
    config.candidate_policy.review_enabled = true;
    let (_store, _backend, review_service) = setup_service_with_config(config).await;
    let review_response = review_service
        .write_semantic_memory(
            user_context(374),
            semantic_write_params(
                middle_semantic,
                "Maybe the user prefers terse answers.",
                "terse answers",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_semantic_policy_middle_review",
            ),
        )
        .await
        .expect("review policy write");
    let review_candidate = review_response.candidate.expect("review candidate");
    assert_eq!(
        review_candidate.status,
        MemoryCandidateStatus::PendingSilent
    );
    assert_eq!(
        review_candidate
            .metadata
            .get("candidate_score")
            .and_then(|score| score.get("score_version"))
            .and_then(serde_json::Value::as_str),
        Some("quality_v1")
    );
    assert_eq!(
        review_candidate
            .metadata
            .get("candidate_policy_decision")
            .and_then(serde_json::Value::as_str),
        Some("pending_silent")
    );
}

#[tokio::test]
async fn semantic_route_contradiction_routes_to_dormant_review_only_when_enabled() {
    let mut config = MemoryServiceConfig::default();
    config.candidate_policy.review_enabled = true;
    let (_store, _backend, service) = setup_service_with_config(config).await;
    service
        .write_semantic_memory(
            user_context(375),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "The user's name is Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_semantic_policy_contradiction_base",
            ),
        )
        .await
        .expect("base memory");

    let contradiction = service
        .write_semantic_memory(
            user_context(376),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Implicit),
                "The user's name may be Alexey.",
                "Alexey",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_semantic_policy_contradiction",
            ),
        )
        .await
        .expect("contradiction");
    assert_eq!(contradiction.relation, MemoryWriteRelation::Contradiction);
    let contradiction_candidate = contradiction.candidate.expect("ask-on-use candidate");
    assert_eq!(
        contradiction_candidate.status,
        MemoryCandidateStatus::AskOnUse
    );
    assert_eq!(
        contradiction_candidate
            .metadata
            .get("candidate_policy_reason_code")
            .and_then(serde_json::Value::as_str),
        Some("review_enabled_contradiction")
    );
    assert_eq!(
        contradiction_candidate
            .metadata
            .get("candidate_score")
            .and_then(|score| score.get("score_version"))
            .and_then(serde_json::Value::as_str),
        Some("quality_v1")
    );
    assert!(contradiction.record.is_none());
}

#[tokio::test]
async fn semantic_rejected_candidate_suppresses_repeat_suggestion() {
    let (_store, _backend, service) = setup_service().await;
    let first = service
        .write_semantic_memory(
            user_context(370),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Implicit),
                "The user's name may be Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::RejectSuppressed,
                "turn_semantic_reject_1",
            ),
        )
        .await
        .expect("first suppressed candidate");
    assert_eq!(first.relation, MemoryWriteRelation::Novel);
    assert_eq!(
        first.candidate.expect("rejected candidate").status,
        MemoryCandidateStatus::Rejected
    );

    let second = service
        .write_semantic_memory(
            user_context(371),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Implicit),
                "User is called Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::CreatePendingCandidate,
                "turn_semantic_reject_2",
            ),
        )
        .await
        .expect("repeat suppressed candidate");
    assert_eq!(second.relation, MemoryWriteRelation::SuppressedByRejection);
    assert!(second.candidate.is_none());
    assert!(second.record.is_none());
}

#[tokio::test]
async fn candidate_approval_uses_semantic_canonical_pipeline() {
    let mut config = MemoryServiceConfig::default();
    config.candidate_policy.review_enabled = true;
    let (_store, _backend, service) = setup_service_with_config(config).await;
    let mut middle_semantic = identity_name_semantic(MemoryExplicitness::Unclear);
    middle_semantic.intent = MemoryIntent::ImplicitCandidate;
    middle_semantic.certainty = MemoryExtractorCertainty::Medium;

    let candidate = service
        .write_semantic_memory(
            user_context(390),
            semantic_write_params(
                middle_semantic,
                "Maybe the user's name is Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_candidate_approval",
            ),
        )
        .await
        .expect("candidate write")
        .candidate
        .expect("candidate");
    assert_eq!(candidate.status, MemoryCandidateStatus::PendingSilent);

    let approved = service
        .approve_candidate(
            user_context(391),
            MemoryCandidatesApproveParams {
                candidate_id: candidate.id.clone(),
                reason: Some("approved in test".to_owned()),
                actor: None,
            },
        )
        .await
        .expect("approve candidate");

    assert_eq!(approved.candidate.status, MemoryCandidateStatus::Approved);
    assert_eq!(
        approved.record.key.as_deref(),
        Some("user/global:identity:self:name")
    );
    assert_eq!(
        approved
            .record
            .metadata
            .get("approved_candidate_id")
            .and_then(serde_json::Value::as_str),
        Some(candidate.id.as_str())
    );
}

#[tokio::test]
async fn candidate_edit_and_approve_supersedes_existing_active_memory() {
    let mut config = MemoryServiceConfig::default();
    config.candidate_policy.review_enabled = true;
    let (store, _backend, service) = setup_service_with_config(config).await;
    let first = service
        .write_semantic_memory(
            user_context(392),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "The user's name is Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_candidate_edit_base",
            ),
        )
        .await
        .expect("base memory")
        .record
        .expect("record");

    let mut candidate_params = semantic_write_params(
        identity_name_semantic(MemoryExplicitness::Implicit),
        "The user's name may be Alexey.",
        "Alexey",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        "turn_candidate_edit_candidate",
    );
    candidate_params.source_context_kind = Some(MemorySourceContextKind::DirectUserConversation);

    let candidate = service
        .write_semantic_memory(user_context(393), candidate_params)
        .await
        .expect("candidate")
        .candidate
        .expect("candidate");
    assert_eq!(candidate.status, MemoryCandidateStatus::AskOnUse);

    let approved = service
        .edit_and_approve_candidate(
            user_context(394),
            MemoryCandidatesEditAndApproveParams {
                candidate_id: candidate.id,
                edited_text: "The user's name is Alex.".to_owned(),
                edited_value: Some("Alex".to_owned()),
                reason: Some("corrected candidate".to_owned()),
                actor: None,
            },
        )
        .await
        .expect("edit and approve");
    assert_eq!(approved.candidate.status, MemoryCandidateStatus::Approved);
    assert_eq!(approved.record.content, "The user's name is Alex.");
    assert_eq!(
        approved.record.source_context_kind,
        Some(MemorySourceContextKind::DirectUserConversation)
    );

    let old = store
        .get_agent_memory_record(first.id.as_str(), true)
        .await
        .expect("load old")
        .expect("old record");
    assert_eq!(old.status, MemoryStatus::Superseded);
}

#[tokio::test]
async fn candidate_api_preserves_workspace_isolation_and_score_audit() {
    let mut config = MemoryServiceConfig::default();
    config.candidate_policy.review_enabled = true;
    let (store, _backend, service) = setup_service_with_config(config).await;
    let mut middle_semantic = workspace_project_decision_semantic(MemoryExplicitness::Unclear);
    middle_semantic.intent = MemoryIntent::ImplicitCandidate;
    middle_semantic.certainty = MemoryExtractorCertainty::Medium;
    let mut params = semantic_write_params(
        middle_semantic,
        "Maybe this workspace prefers migration changes in one file.",
        "migration changes in one file",
        MemorySemanticWriteDisposition::RouteToCandidatePolicy,
        "turn_candidate_scope",
    );
    params.scope = scope(MemoryScopeKind::Workspace, "ws_memory_a");

    let candidate = service
        .write_semantic_memory(workspace_context("ws_memory_a", 395), params)
        .await
        .expect("workspace candidate")
        .candidate
        .expect("candidate");
    assert_eq!(candidate.status, MemoryCandidateStatus::PendingSilent);
    assert_eq!(
        candidate
            .metadata
            .get("candidate_score_bucket")
            .and_then(serde_json::Value::as_str),
        Some("middle")
    );

    let visible = service
        .get_candidate(
            workspace_context("ws_memory_a", 396),
            pioneer_protocol::MemoryCandidatesGetParams {
                candidate_id: candidate.id.clone(),
            },
        )
        .await
        .expect("get visible");
    assert!(visible.candidate.is_some());

    let hidden = service
        .get_candidate(
            workspace_context("ws_memory_b", 397),
            pioneer_protocol::MemoryCandidatesGetParams {
                candidate_id: candidate.id.clone(),
            },
        )
        .await
        .expect("get hidden");
    assert!(hidden.candidate.is_none());

    let decisions = store
        .list_agent_memory_policy_decisions_for_candidate(candidate.id.as_str(), 20)
        .await
        .expect("candidate policy decisions");
    assert!(decisions.iter().any(|decision| {
        decision.action == "candidate_policy"
            && decision.reason_code.as_deref() == Some("review_enabled_middle_confidence")
            && decision
                .details_json
                .as_deref()
                .is_some_and(|details| details.contains("\"bucket\":\"middle\""))
    }));
}

#[tokio::test]
async fn semantic_multi_value_subjects_get_distinct_child_keys() {
    let (_store, _backend, service) = setup_service().await;
    let first = service
        .write_semantic_memory(
            user_context(380),
            semantic_write_params(
                relationship_semantic("alice"),
                "Alice is a project contact.",
                "Alice is a project contact.",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_semantic_multi_1",
            ),
        )
        .await
        .expect("first relationship")
        .record
        .expect("first relationship record");
    let second = service
        .write_semantic_memory(
            user_context(381),
            semantic_write_params(
                relationship_semantic("bob"),
                "Bob is a project contact.",
                "Bob is a project contact.",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_semantic_multi_2",
            ),
        )
        .await
        .expect("second relationship")
        .record
        .expect("second relationship record");

    assert_ne!(first.key, second.key);
    assert_eq!(
        first
            .metadata
            .get("semantic")
            .and_then(|value| value.get("cardinality"))
            .and_then(serde_json::Value::as_str),
        Some("MultiValue")
    );
}

#[tokio::test]
async fn search_suppresses_superseded_backend_hits() {
    let (_store, _backend, service) = setup_service().await;
    let old = service
        .remember(
            user_context(350),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("project.style"),
                "Superseded stale search token should not be visible.",
            ),
        )
        .await
        .expect("old remember");
    let mut replacement_params = remember_params(
        scope(MemoryScopeKind::User, "default"),
        Some("project.style"),
        "Replacement style memory is authoritative.",
    );
    replacement_params.supersedes = Some(old.record.id.clone());
    service
        .remember(user_context(351), replacement_params)
        .await
        .expect("replacement remember");

    let search = service
        .search(
            user_context(352),
            MemorySearchParams {
                query: "Superseded stale search token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert!(search.hits.is_empty());
}

#[tokio::test]
async fn forget_tombstone_suppresses_stale_backend_search_hit() {
    let (_store, backend, service) = setup_service().await;
    let remembered = service
        .remember(
            user_context(400),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user.city"),
                "The user lives in Tbilisi.",
            ),
        )
        .await
        .expect("remember");
    backend.set_delete_noop(true).await;
    service
        .forget(
            user_context(401),
            MemoryForgetParams {
                target: MemoryForgetTarget::Id {
                    memory_id: remembered.record.id.clone(),
                },
                reason: Some("test".to_owned()),
                actor: None,
                dry_run: false,
            },
        )
        .await
        .expect("forget");

    let raw = backend
        .raw_search(BackendSearchRequest {
            query: "Tbilisi".to_owned(),
            scopes: vec![scope(MemoryScopeKind::User, "default")],
            resolved_scopes: Vec::new(),
            limit: 10,
        })
        .await
        .expect("raw search");
    assert_eq!(raw.len(), 1);

    let service_search = service
        .search(
            user_context(402),
            MemorySearchParams {
                query: "Tbilisi".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("service search");
    assert!(service_search.hits.is_empty());

    let service_get = service
        .get(
            user_context(403),
            MemoryGetParams {
                memory_id: remembered.record.id,
                include_deleted: false,
            },
        )
        .await
        .expect("service get");
    assert!(service_get.record.is_none());
}

#[tokio::test]
async fn recall_visibility_suppresses_polluted_search_and_prompt_records() {
    let (store, _backend, service) = setup_service().await;
    let clean = service
        .remember(
            user_context(430),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("phase13.clean"),
                "phase13 clean durable user profile memory",
            ),
        )
        .await
        .expect("clean remember");
    let deleted = service
        .remember(
            user_context(431),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("phase13.deleted"),
                "phase13 deleted polluted memory",
            ),
        )
        .await
        .expect("deleted remember");
    store
        .mark_agent_memory_deleted(
            deleted.record.id.as_str(),
            None,
            Some("fixture".to_owned()),
            432,
        )
        .await
        .expect("mark deleted");
    let superseded = service
        .remember(
            user_context(433),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("phase13.superseded"),
                "phase13 superseded polluted memory",
            ),
        )
        .await
        .expect("superseded remember");
    store
        .mark_agent_memory_superseded(superseded.record.id.as_str(), clean.record.id.as_str(), 434)
        .await
        .expect("mark superseded");
    let expired = service
        .remember(
            user_context(435),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("phase13.expired"),
                "phase13 expired polluted memory",
            ),
        )
        .await
        .expect("expired remember");
    store
        .mark_agent_memory_expired(expired.record.id.as_str(), 436)
        .await
        .expect("mark expired");
    let mut secret_params = remember_params(
        scope(MemoryScopeKind::User, "default"),
        Some("phase13.secret"),
        "phase13 secret polluted memory",
    );
    secret_params.sensitivity = Some(MemorySensitivity::SecretLike);
    service
        .remember(user_context(437), secret_params)
        .await
        .expect("secret remember");

    for (
        index,
        (
            key,
            action,
            target_ownership,
            source_context_kind,
            evidence_class,
            relation,
            reason_codes,
        ),
    ) in [
        (
            "phase13.low_source",
            MemoryQualityAction::ForceReject,
            MemoryOwnershipClass::Reject,
            MemorySourceContextKind::Unknown,
            MemoryEvidenceClass::MissingOrWeak,
            MemoryWriteRelation::Novel,
            vec![MemoryQualityReasonCode::WeakOrMissingEvidence],
        ),
        (
            "phase13.rejected_related",
            MemoryQualityAction::ForceReject,
            MemoryOwnershipClass::AuditOnly,
            MemorySourceContextKind::DirectUserConversation,
            MemoryEvidenceClass::DirectUserAssertion,
            MemoryWriteRelation::SuppressedByRejection,
            vec![MemoryQualityReasonCode::DuplicateExistingMemory],
        ),
        (
            "phase13.quarantine",
            MemoryQualityAction::Quarantine,
            MemoryOwnershipClass::AuditOnly,
            MemorySourceContextKind::DirectUserConversation,
            MemoryEvidenceClass::DirectUserAssertion,
            MemoryWriteRelation::Novel,
            vec![MemoryQualityReasonCode::NoQualityAllowRule],
        ),
        (
            "phase13.ownership_mismatch",
            MemoryQualityAction::ForceReject,
            MemoryOwnershipClass::AuditOnly,
            MemorySourceContextKind::DirectUserConversation,
            MemoryEvidenceClass::DirectUserAssertion,
            MemoryWriteRelation::Novel,
            vec![MemoryQualityReasonCode::OwnershipMismatch],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let remembered = service
            .remember(
                user_context(440 + index as i64),
                remember_params(
                    scope(MemoryScopeKind::User, "default"),
                    Some(key),
                    format!("{key} polluted memory phase13").as_str(),
                ),
            )
            .await
            .expect("polluted remember");
        attach_quality_decision(
            &store,
            remembered.record.id.as_str(),
            action,
            target_ownership,
            source_context_kind,
            MemoryFactClass::UserIdentity,
            MemoryLifetimeClass::LongLived,
            MemoryOwnershipClass::DurableWorkspaceMemory,
            evidence_class,
            relation,
            reason_codes,
            450 + index as i64,
        )
        .await;
    }

    let search = service
        .search(
            user_context(460),
            MemorySearchParams {
                query: "phase13".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    let search_ids = search
        .hits
        .iter()
        .map(|hit| hit.record.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(search_ids, vec![clean.record.id.as_str()]);

    let prompt_recall = service
        .recall_for_prompt(
            user_context(461),
            MemoryRecallParams {
                query: "phase13".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("prompt recall");
    assert_eq!(prompt_recall.items.len(), 1);
    assert_eq!(prompt_recall.items[0].memory_id, clean.record.id);
    assert!(prompt_recall.diagnostics.iter().any(|diagnostic| {
        diagnostic.starts_with("memory.recall_visibility.suppressed_count:")
    }));
    assert!(prompt_recall.diagnostics.iter().all(|diagnostic| {
        !diagnostic.contains("polluted memory") && !diagnostic.contains("clean durable")
    }));
}

#[tokio::test]
async fn recall_visibility_suppresses_mode_and_exact_canonical_records() {
    let (store, _backend, service) = setup_service().await;
    let mut clean_project_params = remember_params(
        scope(MemoryScopeKind::Workspace, "ws_phase13"),
        Some("phase13.project.clean"),
        "phase13 clean project decision",
    );
    clean_project_params.category = MemoryCategory::ProjectDecision;
    let clean_project = service
        .remember(workspace_context("ws_phase13", 470), clean_project_params)
        .await
        .expect("clean project remember");
    let mut bad_project_params = remember_params(
        scope(MemoryScopeKind::Workspace, "ws_phase13"),
        Some("phase13.project.bad"),
        "phase13 bad project decision",
    );
    bad_project_params.category = MemoryCategory::ProjectDecision;
    let bad_project = service
        .remember(workspace_context("ws_phase13", 471), bad_project_params)
        .await
        .expect("bad project remember");
    attach_quality_decision(
        &store,
        bad_project.record.id.as_str(),
        MemoryQualityAction::Quarantine,
        MemoryOwnershipClass::AuditOnly,
        MemorySourceContextKind::DirectUserConversation,
        MemoryFactClass::ProjectDecision,
        MemoryLifetimeClass::ProjectLifetime,
        MemoryOwnershipClass::DurableWorkspaceMemory,
        MemoryEvidenceClass::DirectUserAssertion,
        MemoryWriteRelation::Novel,
        vec![MemoryQualityReasonCode::NoQualityAllowRule],
        472,
    )
    .await;

    let project_recall = service
        .recall_mode_for_prompt(
            workspace_context("ws_phase13", 473),
            MemoryModeRecallParams {
                mode: MemoryRecallMode::Project,
                targets: Vec::new(),
                top_k: Some(10),
                max_chars: None,
            },
        )
        .await
        .expect("project mode recall");
    assert_eq!(project_recall.items.len(), 1);
    assert_eq!(project_recall.items[0].memory_id, clean_project.record.id);
    assert!(project_recall.diagnostics.iter().any(|diagnostic| {
        diagnostic
            .contains("memory.recall_visibility.suppressed:suppress_quarantined_or_audit_only")
    }));

    let exact_bad = service
        .recall_mode_for_prompt(
            workspace_context("ws_phase13", 474),
            MemoryModeRecallParams {
                mode: MemoryRecallMode::ExactCanonical,
                targets: vec![MemoryRecallTarget {
                    scope_kind: Some(MemoryScopeKind::Workspace),
                    canonical_key: Some("phase13.project.bad".to_owned()),
                    ..Default::default()
                }],
                top_k: Some(10),
                max_chars: None,
            },
        )
        .await
        .expect("exact bad recall");
    assert!(exact_bad.items.is_empty());
}

#[tokio::test]
async fn recall_visibility_suppresses_workspace_mismatch_backend_hits() {
    let (_store, _backend, service) = setup_service().await;
    let mut params = remember_params(
        scope(MemoryScopeKind::Workspace, "ws_phase13_other"),
        Some("phase13.workspace.other"),
        "phase13 workspace mismatch memory",
    );
    params.category = MemoryCategory::ProjectDecision;
    service
        .remember(workspace_context("ws_phase13_other", 480), params)
        .await
        .expect("other workspace remember");

    let search = service
        .search(
            workspace_context("ws_phase13_target", 481),
            MemorySearchParams {
                query: "phase13 workspace mismatch".to_owned(),
                scopes: vec![scope(MemoryScopeKind::Workspace, "ws_phase13_other")],
                ..Default::default()
            },
        )
        .await
        .expect("workspace guarded search");
    assert!(search.hits.is_empty());
}

#[tokio::test]
async fn dry_run_forget_resolves_ids_without_mutation() {
    let (store, _backend, service) = setup_service().await;
    let remembered = service
        .remember(
            user_context(500),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user.food"),
                "The user likes Georgian food.",
            ),
        )
        .await
        .expect("remember");

    let dry_run = service
        .forget(
            user_context(501),
            MemoryForgetParams {
                target: MemoryForgetTarget::ScopedKey {
                    scope: scope(MemoryScopeKind::User, "default"),
                    namespace: None,
                    key: "user.food".to_owned(),
                },
                reason: Some("dry run".to_owned()),
                actor: None,
                dry_run: true,
            },
        )
        .await
        .expect("dry run forget");
    assert_eq!(
        dry_run.forgotten_memory_ids,
        vec![remembered.record.id.clone()]
    );
    assert!(dry_run.dry_run);

    let still_loaded = service
        .get(
            user_context(502),
            MemoryGetParams {
                memory_id: remembered.record.id.clone(),
                include_deleted: false,
            },
        )
        .await
        .expect("get")
        .record;
    assert!(still_loaded.is_some());

    let events = store
        .list_agent_memory_events(remembered.record.id.as_str(), 10)
        .await
        .expect("events");
    assert!(!events.iter().any(|event| event.event_kind == "forgotten"));
}

#[tokio::test]
async fn search_filters_sensitivity_by_default() {
    let (_store, _backend, service) = setup_service().await;
    for (key, sensitivity) in [
        ("normal", MemorySensitivity::Normal),
        ("personal", MemorySensitivity::Personal),
        ("secret", MemorySensitivity::SecretLike),
        ("regulated", MemorySensitivity::Regulated),
    ] {
        let mut params = remember_params(
            scope(MemoryScopeKind::User, "default"),
            Some(key),
            format!("shared recall token {key}").as_str(),
        );
        params.sensitivity = Some(sensitivity);
        service
            .remember(user_context(600), params)
            .await
            .expect("remember sensitivity row");
    }

    let default_search = service
        .search(
            user_context(601),
            MemorySearchParams {
                query: "shared recall token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("default search");
    let mut default_keys = default_search
        .hits
        .iter()
        .filter_map(|hit| hit.record.key.as_deref())
        .collect::<Vec<_>>();
    default_keys.sort_unstable();
    assert_eq!(default_keys, vec!["normal", "personal"]);

    let allow_all_search = service
        .search(
            MemoryOperationContext {
                read_policy: Some(MemoryReadPolicy::allow_all()),
                ..user_context(602)
            },
            MemorySearchParams {
                query: "shared recall token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("allow all search");
    assert_eq!(allow_all_search.hits.len(), 4);
}

#[tokio::test]
async fn search_exact_key_match_beats_body_only_match() {
    let (_store, _backend, service) = setup_service().await;
    let exact = service
        .remember(
            user_context(650),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user.birthday"),
                "The birthday memory says September 12.",
            ),
        )
        .await
        .expect("remember exact");
    let body_only = service
        .remember(
            user_context(651),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user.note"),
                "The literal user.birthday token appears in this unrelated note.",
            ),
        )
        .await
        .expect("remember body-only");

    let search = service
        .search(
            user_context(652),
            MemorySearchParams {
                query: "user.birthday".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert_eq!(search.hits.len(), 2);
    assert_eq!(search.hits[0].record.id, exact.record.id);
    assert_eq!(search.hits[1].record.id, body_only.record.id);
    assert!(search.hits[0].score > search.hits[1].score);
}

#[tokio::test]
async fn search_backend_score_contributes_to_final_ranking() {
    let (_store, _backend, service) = setup_service().await;
    let full_match = service
        .remember(
            user_context(653),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("backend.full"),
                "Backend score alpha beta gamma appears as a full phrase.",
            ),
        )
        .await
        .expect("remember full match");
    let partial_match = service
        .remember(
            user_context(654),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("backend.partial"),
                "Backend score alpha appears without the other query terms.",
            ),
        )
        .await
        .expect("remember partial match");

    let search = service
        .search(
            user_context(655),
            MemorySearchParams {
                query: "Backend score alpha beta gamma".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert_eq!(search.hits.len(), 2);
    assert_eq!(search.hits[0].record.id, full_match.record.id);
    assert_eq!(search.hits[1].record.id, partial_match.record.id);
    assert!(search.hits[0].score > search.hits[1].score);
}

#[tokio::test]
async fn search_category_match_boost_contributes_to_final_score() {
    let mut config = MemoryServiceConfig::default();
    config.ranking.backend_score_weight = 0.0;
    config.ranking.exact_key_boost = 0.0;
    config.ranking.primary_scope_boost = 0.0;
    config.ranking.scope_rank_boost = 0.0;
    config.ranking.recency_boost_max = 0.0;
    config.ranking.importance_weight = 0.0;
    config.ranking.confidence_weight = 0.0;
    config.ranking.category_match_boost = 0.5;
    let (_store, _backend, service) = setup_service_with_config(config).await;
    let remembered = service
        .remember(
            user_context(656),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("category.boost"),
                "Category boost isolated token.",
            ),
        )
        .await
        .expect("remember");

    let unboosted = service
        .search(
            user_context(657),
            MemorySearchParams {
                query: "Category boost isolated token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("unboosted search");
    let boosted = service
        .search(
            user_context(658),
            MemorySearchParams {
                query: "Category boost isolated token".to_owned(),
                categories: vec![MemoryCategory::Identity],
                ..Default::default()
            },
        )
        .await
        .expect("boosted search");
    assert_eq!(unboosted.hits[0].record.id, remembered.record.id);
    assert_eq!(boosted.hits[0].record.id, remembered.record.id);
    assert_eq!(unboosted.hits[0].score, Some(0.0));
    assert_eq!(boosted.hits[0].score, Some(0.5));
}

#[tokio::test]
async fn search_scope_boost_prefers_workspace_over_user_fallback() {
    let (_store, _backend, service) = setup_service().await;
    let user = service
        .remember(
            user_context(660),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("scope.user"),
                "Scope boost shared token from user memory.",
            ),
        )
        .await
        .expect("remember user");
    let workspace = service
        .remember(
            workspace_context("ws_scope_boost", 661),
            remember_params(
                scope(MemoryScopeKind::Workspace, "ws_scope_boost"),
                Some("scope.workspace"),
                "Scope boost shared token from workspace memory.",
            ),
        )
        .await
        .expect("remember workspace");

    let search = service
        .search(
            MemoryOperationContext {
                allow_global_user: true,
                ..workspace_context("ws_scope_boost", 662)
            },
            MemorySearchParams {
                query: "Scope boost shared token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert_eq!(search.hits.len(), 2);
    assert_eq!(search.hits[0].record.id, workspace.record.id);
    assert_eq!(search.hits[1].record.id, user.record.id);
}

#[tokio::test]
async fn search_importance_and_recency_boosts_are_deterministic() {
    let (_store, _backend, service) = setup_service().await;
    let mut important_params = remember_params(
        scope(MemoryScopeKind::User, "default"),
        Some("rank.important"),
        "Ranking importance shared token from important memory.",
    );
    important_params.importance = Some(1.0);
    let important = service
        .remember(user_context(665), important_params)
        .await
        .expect("remember important");

    let mut less_important_params = remember_params(
        scope(MemoryScopeKind::User, "default"),
        Some("rank.less_important"),
        "Ranking importance shared token from less important memory.",
    );
    less_important_params.importance = Some(0.0);
    let less_important = service
        .remember(user_context(666), less_important_params)
        .await
        .expect("remember less important");

    let importance_search = service
        .search(
            user_context(667),
            MemorySearchParams {
                query: "Ranking importance shared token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("importance search");
    assert_eq!(importance_search.hits.len(), 2);
    assert_eq!(importance_search.hits[0].record.id, important.record.id);
    assert_eq!(
        importance_search.hits[1].record.id,
        less_important.record.id
    );

    let older = service
        .remember(
            user_context(668),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("rank.older"),
                "Ranking recency shared token from older memory.",
            ),
        )
        .await
        .expect("remember older");
    let newer = service
        .remember(
            user_context(698),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("rank.newer"),
                "Ranking recency shared token from newer memory.",
            ),
        )
        .await
        .expect("remember newer");

    let recency_search = service
        .search(
            user_context(699),
            MemorySearchParams {
                query: "Ranking recency shared token".to_owned(),
                limit: Some(2),
                ..Default::default()
            },
        )
        .await
        .expect("recency search");
    assert_eq!(recency_search.hits.len(), 2);
    assert_eq!(recency_search.hits[0].record.id, newer.record.id);
    assert_eq!(recency_search.hits[1].record.id, older.record.id);
}

#[tokio::test]
async fn search_empty_scopes_use_user_scope_fallback_when_allowed() {
    let (_store, _backend, service) = setup_service().await;
    let remembered = service
        .remember(
            user_context(670),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("fallback.user"),
                "User scope fallback token should be found.",
            ),
        )
        .await
        .expect("remember user fallback");

    let search = service
        .search(
            MemoryOperationContext {
                allow_global_user: true,
                ..workspace_context("ws_user_fallback", 671)
            },
            MemorySearchParams {
                query: "User scope fallback token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert_eq!(search.hits.len(), 1);
    assert_eq!(search.hits[0].record.id, remembered.record.id);
}

#[tokio::test]
async fn search_uses_tool_search_limit_when_limit_is_missing() {
    let mut config = MemoryServiceConfig::default();
    config.recall.tool_search_limit = 2;
    let (_store, _backend, service) = setup_service_with_config(config).await;

    for index in 0..3 {
        service
            .remember(
                user_context(680 + index),
                remember_params(
                    scope(MemoryScopeKind::User, "default"),
                    Some(format!("tool.limit.{index}").as_str()),
                    format!("Tool default limit shared token {index}.").as_str(),
                ),
            )
            .await
            .expect("remember");
    }

    let search = service
        .search(
            user_context(690),
            MemorySearchParams {
                query: "Tool default limit shared token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert_eq!(search.hits.len(), 2);
}

#[tokio::test]
async fn search_backend_limit_overfetches_before_final_ranking_limit() {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("connect sqlite");
    Migrator::up(&connection, None).await.expect("migrate");
    let store = Arc::new(CrudStore::new(connection));
    let backend = Arc::new(RecordingMemoryBackend::default());
    let backend_for_service: Arc<dyn MemoryBackend> = backend.clone();
    let mut config = MemoryServiceConfig::default();
    config.recall.tool_search_limit = 3;
    config.ranking.backend_candidate_multiplier = 4;
    config.ranking.max_backend_candidates = 100;
    let service = MemoryService::new(store, backend_for_service, config);

    let search = service
        .search(
            user_context(695),
            MemorySearchParams {
                query: "overfetch token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert!(search.hits.is_empty());
    assert_eq!(backend.search_limits().await, vec![12]);
}

#[tokio::test]
async fn recall_for_prompt_uses_prompt_top_k_and_char_budgets() {
    let mut config = MemoryServiceConfig::default();
    config.recall.prompt_top_k = 2;
    config.recall.max_item_chars = 18;
    config.recall.max_prompt_chars = 30;
    let (_store, _backend, service) = setup_service_with_config(config).await;

    for index in 0..3 {
        service
            .remember(
                user_context(700 + index),
                remember_params(
                    scope(MemoryScopeKind::User, "default"),
                    Some(format!("recall.prompt.{index}").as_str()),
                    format!("Recall prompt shared token with    extra whitespace line {index}.")
                        .as_str(),
                ),
            )
            .await
            .expect("remember");
    }

    let recall = service
        .recall_for_prompt(
            user_context(710),
            MemoryRecallParams {
                query: "Recall prompt shared token".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("recall");

    assert_eq!(recall.items.len(), 2);
    assert!(
        recall
            .items
            .iter()
            .all(|item| item.content.chars().count() <= 18)
    );
    let total_chars = recall
        .items
        .iter()
        .map(|item| item.content.chars().count())
        .sum::<usize>();
    assert!(total_chars <= 30);
    assert!(
        recall
            .items
            .iter()
            .all(|item| !item.content.contains("  ") && !item.content.contains('\n'))
    );
}

#[tokio::test]
async fn recall_for_prompt_generic_active_query_can_find_profile_identity_memory() {
    let (_store, _backend, service) = setup_service().await;
    service
        .remember(
            user_context(720),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user/global:identity:self:name"),
                "Имя пользователя — Александр.",
            ),
        )
        .await
        .expect("remember identity");

    let recall = service
        .recall_for_prompt(
            user_context(721),
            MemoryRecallParams {
                query: "durable user identity preferences biography communication style recurring instructions project facts project decisions procedures constraints todos ongoing tasks"
                    .to_owned(),
                top_k: Some(5),
                max_chars: Some(1_500),
                ..Default::default()
            },
        )
        .await
        .expect("recall");

    assert!(
        recall
            .items
            .iter()
            .any(|item| item.content.contains("Александр")),
        "generic active recall query should be able to surface stable identity memory"
    );
}

#[tokio::test]
async fn recall_mode_for_prompt_profile_project_and_durable_are_scoped() {
    let (_store, _backend, service) = setup_service().await;
    service
        .remember(
            user_context(730),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user/global:identity:self:name"),
                "Имя пользователя — Александр.",
            ),
        )
        .await
        .expect("remember profile");

    let mut project_params = remember_params(
        scope(MemoryScopeKind::Workspace, "ws_mode_a"),
        Some("workspace/ws_mode_a:project_decision:self:phase_naming"),
        "Project decision: phases keep the `phase` name.",
    );
    project_params.category = MemoryCategory::ProjectDecision;
    service
        .remember(workspace_context("ws_mode_a", 731), project_params)
        .await
        .expect("remember project");

    let mut other_workspace_params = remember_params(
        scope(MemoryScopeKind::Workspace, "ws_mode_b"),
        Some("workspace/ws_mode_b:project_decision:self:phase_naming"),
        "Other workspace decision must stay isolated.",
    );
    other_workspace_params.category = MemoryCategory::ProjectDecision;
    service
        .remember(workspace_context("ws_mode_b", 732), other_workspace_params)
        .await
        .expect("remember other workspace");

    let context = MemoryOperationContext {
        allow_global_user: true,
        ..workspace_context("ws_mode_a", 733)
    };

    let profile = service
        .recall_mode_for_prompt(
            context.clone(),
            MemoryModeRecallParams {
                mode: MemoryRecallMode::Profile,
                targets: Vec::new(),
                top_k: Some(5),
                max_chars: Some(1_500),
            },
        )
        .await
        .expect("profile recall");
    assert_eq!(profile.items.len(), 1);
    assert!(profile.items[0].content.contains("Александр"));

    let project = service
        .recall_mode_for_prompt(
            context.clone(),
            MemoryModeRecallParams {
                mode: MemoryRecallMode::Project,
                targets: Vec::new(),
                top_k: Some(5),
                max_chars: Some(1_500),
            },
        )
        .await
        .expect("project recall");
    assert_eq!(project.items.len(), 1);
    assert!(project.items[0].content.contains("phase"));
    assert!(!project.items[0].content.contains("Other workspace"));

    let durable = service
        .recall_mode_for_prompt(
            context,
            MemoryModeRecallParams {
                mode: MemoryRecallMode::Durable,
                targets: Vec::new(),
                top_k: Some(5),
                max_chars: Some(1_500),
            },
        )
        .await
        .expect("durable recall");
    assert_eq!(durable.items.len(), 2);
    assert!(
        durable
            .items
            .iter()
            .any(|item| item.content.contains("Александр"))
    );
    assert!(
        durable
            .items
            .iter()
            .any(|item| item.content.contains("phase"))
    );
}

#[tokio::test]
async fn recall_mode_for_prompt_exact_canonical_uses_key_lookup_without_search() {
    let (_store, backend, service) = setup_service_with_recording_backend().await;
    service
        .remember(
            user_context(740),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user/global:identity:self:name"),
                "Имя пользователя — Александр.",
            ),
        )
        .await
        .expect("remember identity");

    let by_key = service
        .recall_mode_for_prompt(
            user_context(741),
            MemoryModeRecallParams {
                mode: MemoryRecallMode::ExactCanonical,
                targets: vec![MemoryRecallTarget {
                    scope_kind: Some(MemoryScopeKind::User),
                    category: Some(MemoryCategory::Identity),
                    canonical_key: Some("user/global:identity:self:name".to_owned()),
                    ..MemoryRecallTarget::default()
                }],
                top_k: Some(5),
                max_chars: Some(1_500),
            },
        )
        .await
        .expect("exact by key recall");
    assert_eq!(by_key.items.len(), 1);
    assert!(by_key.items[0].content.contains("Александр"));

    let by_typed_target = service
        .recall_mode_for_prompt(
            user_context(742),
            MemoryModeRecallParams {
                mode: MemoryRecallMode::ExactCanonical,
                targets: vec![MemoryRecallTarget {
                    scope_kind: Some(MemoryScopeKind::User),
                    category: Some(MemoryCategory::Identity),
                    subject: Some(MemorySubject::CurrentUser),
                    attribute: Some(MemoryAttribute::Name),
                    ..MemoryRecallTarget::default()
                }],
                top_k: Some(5),
                max_chars: Some(1_500),
            },
        )
        .await
        .expect("exact by typed target recall");
    assert_eq!(by_typed_target.items.len(), 1);
    assert!(by_typed_target.items[0].content.contains("Александр"));
    assert!(
        backend.search_limits().await.is_empty(),
        "exact canonical recall must not call broad backend search"
    );
}

#[tokio::test]
async fn recall_mode_for_prompt_thread_and_task_context_require_native_provider() {
    let (_store, _backend, service) = setup_service().await;
    let thread = service
        .recall_mode_for_prompt(
            MemoryOperationContext {
                thread_id: Some("thr_mode_a".to_owned()),
                now_unix: Some(752),
                ..MemoryOperationContext::default()
            },
            MemoryModeRecallParams {
                mode: MemoryRecallMode::ThreadEpisodic,
                targets: Vec::new(),
                top_k: Some(5),
                max_chars: Some(1_500),
            },
        )
        .await
        .expect("thread recall");
    assert_eq!(
        thread.skipped_reason.as_deref(),
        Some("thread_episodic_native_provider_required")
    );
    assert!(thread.diagnostics.iter().any(|diagnostic| {
        diagnostic.contains("memory.active_recall.mode_native_provider_required:thread_episodic")
    }));

    let task = service
        .recall_mode_for_prompt(
            MemoryOperationContext {
                task_id: Some("task_mode_a".to_owned()),
                now_unix: Some(753),
                ..MemoryOperationContext::default()
            },
            MemoryModeRecallParams {
                mode: MemoryRecallMode::TaskContext,
                targets: Vec::new(),
                top_k: Some(5),
                max_chars: Some(1_500),
            },
        )
        .await
        .expect("task recall");
    assert_eq!(
        task.skipped_reason.as_deref(),
        Some("task_context_native_provider_required")
    );
    assert!(task.diagnostics.iter().any(|diagnostic| {
        diagnostic.contains("memory.active_recall.mode_native_provider_required:task_context")
    }));

    let missing_task = service
        .recall_mode_for_prompt(
            user_context(754),
            MemoryModeRecallParams {
                mode: MemoryRecallMode::TaskContext,
                targets: Vec::new(),
                top_k: Some(5),
                max_chars: Some(1_500),
            },
        )
        .await
        .expect("missing task recall");
    assert_eq!(
        missing_task.skipped_reason.as_deref(),
        Some("task_context_native_provider_required")
    );
}

#[tokio::test]
async fn expired_and_repair_needed_records_are_not_visible() {
    let (store, _backend, service) = setup_service().await;
    let expired = service
        .remember(
            user_context(700),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("memory.expired"),
                "This memory should expire.",
            ),
        )
        .await
        .expect("remember expired");
    let repair = service
        .remember(
            user_context(701),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("memory.repair"),
                "This memory needs repair.",
            ),
        )
        .await
        .expect("remember repair");

    store
        .mark_agent_memory_expired(expired.record.id.as_str(), 702)
        .await
        .expect("mark expired");
    store
        .mark_agent_memory_repair_status(repair.record.id.as_str(), "repair_needed", 703)
        .await
        .expect("mark repair");

    let listed = service
        .list(user_context(704), MemoryListParams::default())
        .await
        .expect("list");
    assert!(listed.records.is_empty());

    let searched = service
        .search(
            user_context(705),
            MemorySearchParams {
                query: "memory".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert!(searched.hits.is_empty());
}

#[tokio::test]
async fn workspace_local_agent_memory_cannot_leak() {
    let (_store, _backend, service) = setup_service().await;
    let agent_id = "agent_research";
    let workspace_a_agent = workspace_agent_memory_scope_key("ws_memory_a", agent_id);
    let global_agent = global_agent_memory_scope_key(agent_id);

    let workspace_memory = service
        .remember(
            workspace_context("ws_memory_a", 800),
            remember_params(
                scope(MemoryScopeKind::Agent, workspace_a_agent.as_str()),
                Some("agent.note"),
                "Workspace A agent memory.",
            ),
        )
        .await
        .expect("remember workspace agent");
    let global_memory = service
        .remember(
            MemoryOperationContext {
                allow_global_agent: true,
                now_unix: Some(801),
                ..Default::default()
            },
            remember_params(
                scope(MemoryScopeKind::Agent, global_agent.as_str()),
                Some("agent.global"),
                "Global agent memory.",
            ),
        )
        .await
        .expect("remember global agent");

    let leaked_get = service
        .get(
            workspace_context("ws_memory_b", 802),
            MemoryGetParams {
                memory_id: workspace_memory.record.id,
                include_deleted: false,
            },
        )
        .await
        .expect("get from wrong workspace");
    assert!(leaked_get.record.is_none());

    let global_without_allow = service
        .search(
            workspace_context("ws_memory_b", 803),
            MemorySearchParams {
                query: "Global agent memory".to_owned(),
                scopes: vec![scope(MemoryScopeKind::Agent, global_agent.as_str())],
                ..Default::default()
            },
        )
        .await
        .expect("search global without allow");
    assert!(global_without_allow.hits.is_empty());

    let global_with_allow = service
        .search(
            MemoryOperationContext {
                allow_global_agent: true,
                ..workspace_context("ws_memory_b", 804)
            },
            MemorySearchParams {
                query: "Global agent memory".to_owned(),
                scopes: vec![scope(MemoryScopeKind::Agent, global_agent.as_str())],
                ..Default::default()
            },
        )
        .await
        .expect("search global with allow");
    assert_eq!(global_with_allow.hits.len(), 1);
    assert_eq!(global_with_allow.hits[0].record.id, global_memory.record.id);
}

#[tokio::test]
async fn global_user_memory_requires_explicit_workspace_allowance() {
    let (_store, _backend, service) = setup_service().await;
    let remembered = service
        .remember(
            user_context(850),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("user.global"),
                "Global user memory.",
            ),
        )
        .await
        .expect("remember global user");

    let blocked = service
        .search(
            workspace_context("ws_memory_a", 851),
            MemorySearchParams {
                query: "Global user memory".to_owned(),
                scopes: vec![scope(MemoryScopeKind::User, "default")],
                ..Default::default()
            },
        )
        .await
        .expect("blocked search");
    assert!(blocked.hits.is_empty());

    let allowed = service
        .search(
            MemoryOperationContext {
                allow_global_user: true,
                ..workspace_context("ws_memory_a", 852)
            },
            MemorySearchParams {
                query: "Global user memory".to_owned(),
                scopes: vec![scope(MemoryScopeKind::User, "default")],
                ..Default::default()
            },
        )
        .await
        .expect("allowed search");
    assert_eq!(allowed.hits.len(), 1);
    assert_eq!(allowed.hits[0].record.id, remembered.record.id);
}

#[tokio::test]
async fn missing_backend_payload_creates_repair_diagnostic() {
    let (store, backend, service) = setup_service().await;
    let remembered = service
        .remember(
            user_context(900),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("memory.missing"),
                "This backend payload will be removed.",
            ),
        )
        .await
        .expect("remember");
    backend.remove_payload(remembered.record.id.as_str()).await;

    let missing = service
        .get(
            user_context(901),
            MemoryGetParams {
                memory_id: remembered.record.id.clone(),
                include_deleted: false,
            },
        )
        .await
        .expect("get missing payload");
    assert!(missing.record.is_none());

    let control = store
        .get_agent_memory_record(remembered.record.id.as_str(), true)
        .await
        .expect("control")
        .expect("row");
    assert_eq!(control.repair_status, "repair_needed");

    let claimed = service
        .claim_due_repair_jobs(902, 60, "repair_worker", 10)
        .await
        .expect("claim repair jobs");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job_kind, "backend_payload_missing");
    assert!(
        !claimed[0]
            .payload_json
            .as_deref()
            .unwrap_or_default()
            .contains("This backend payload will be removed.")
    );
}

#[tokio::test]
async fn stale_backend_payload_without_control_plane_row_creates_repair_diagnostic() {
    let (_store, backend, service) = setup_service().await;
    backend
        .insert_stale_payload(BackendPutRequest {
            memory_id: "mem_backend_only".to_owned(),
            scope: scope(MemoryScopeKind::User, "default"),
            namespace: None,
            category: MemoryCategory::Identity,
            key: Some("backend.only".to_owned()),
            content: "Backend-only stale memory.".to_owned(),
            sensitivity: MemorySensitivity::Normal,
            metadata_json: None,
            source_thread_id: None,
            source_turn_id: None,
            source_item_id: None,
            created_by_kind: None,
            created_by_id: None,
            policy_version: "test".to_owned(),
            status: MemoryStatus::Active,
            idempotency_key: None,
        })
        .await;

    let search = service
        .search(
            user_context(950),
            MemorySearchParams {
                query: "Backend-only".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("service search");
    assert!(search.hits.is_empty());

    let claimed = service
        .claim_due_repair_jobs(951, 60, "repair_worker", 10)
        .await
        .expect("claim repair jobs");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job_kind, "backend_stale_payload");
    assert!(
        !claimed[0]
            .payload_json
            .as_deref()
            .unwrap_or_default()
            .contains("Backend-only stale memory.")
    );
}

#[tokio::test]
async fn repair_job_helpers_roundtrip() {
    let (_store, _backend, service) = setup_service().await;
    let completed = service
        .enqueue_repair_job(
            NewAgentMemoryRepairJob {
                job_kind: "backend_payload_missing".to_owned(),
                workspace_id: None,
                scope_kind: Some(MemoryScopeKind::User),
                scope_key_hash: Some("hash_one".to_owned()),
                memory_id: Some("mem_repair_one".to_owned()),
                capsule_id: None,
                priority: 10,
                max_attempts: 3,
                scheduled_at_unix: 1000,
                payload_json: Some(serde_json::json!({"memory_id": "mem_repair_one"}).to_string()),
            },
            1000,
        )
        .await
        .expect("enqueue completed job");
    let claimed = service
        .claim_due_repair_jobs(1001, 60, "worker_one", 10)
        .await
        .expect("claim");
    assert_eq!(claimed[0].id, completed.id);
    let completed = service
        .complete_repair_job(claimed[0].id.as_str(), "worker_one", None, 1002)
        .await
        .expect("complete")
        .expect("completed job");
    assert_eq!(completed.status, "completed");

    let retry = service
        .enqueue_repair_job(
            NewAgentMemoryRepairJob {
                job_kind: "backend_delete_failed".to_owned(),
                workspace_id: None,
                scope_kind: Some(MemoryScopeKind::User),
                scope_key_hash: Some("hash_two".to_owned()),
                memory_id: Some("mem_repair_two".to_owned()),
                capsule_id: None,
                priority: 10,
                max_attempts: 2,
                scheduled_at_unix: 1010,
                payload_json: Some(serde_json::json!({"memory_id": "mem_repair_two"}).to_string()),
            },
            1010,
        )
        .await
        .expect("enqueue retry job");
    let claimed_retry = service
        .claim_due_repair_jobs(1011, 60, "worker_two", 10)
        .await
        .expect("claim retry");
    assert_eq!(claimed_retry[0].id, retry.id);
    let pending = service
        .fail_repair_job(
            claimed_retry[0].id.as_str(),
            "worker_two",
            "temporary".to_owned(),
            Some(1020),
            1012,
        )
        .await
        .expect("fail below max")
        .expect("pending retry");
    assert_eq!(pending.status, "pending");

    let claimed_again = service
        .claim_due_repair_jobs(1021, 60, "worker_two", 10)
        .await
        .expect("claim again");
    let failed = service
        .fail_repair_job(
            claimed_again[0].id.as_str(),
            "worker_two",
            "permanent".to_owned(),
            None,
            1022,
        )
        .await
        .expect("fail at max")
        .expect("failed job");
    assert_eq!(failed.status, "failed");
}

#[tokio::test]
async fn noop_backend_fails_closed() {
    let connection = Database::connect("sqlite::memory:")
        .await
        .expect("connect sqlite");
    Migrator::up(&connection, None).await.expect("migrate");
    let store = Arc::new(CrudStore::new(connection));
    let service = MemoryService::with_noop_backend(store.clone());

    let remember = service
        .remember(
            user_context(1100),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("noop.test"),
                "This should not be stored.",
            ),
        )
        .await;
    assert!(remember.is_err());

    let search = service
        .search(
            user_context(1101),
            MemorySearchParams {
                query: "anything".to_owned(),
                ..Default::default()
            },
        )
        .await;
    assert!(search.is_err());

    let rows = store
        .list_agent_memory_records(AgentMemoryListFilter {
            scopes: vec![scope(MemoryScopeKind::User, "default")],
            ..Default::default()
        })
        .await
        .expect("list control rows");
    assert!(rows.is_empty());
}

#[tokio::test]
async fn backend_delete_failure_keeps_tombstone_and_enqueues_repair() {
    let (store, backend, service) = setup_service().await;
    let remembered = service
        .remember(
            user_context(1200),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("delete.failure"),
                "Delete failure should not restore visibility.",
            ),
        )
        .await
        .expect("remember");
    backend
        .set_delete_error(Some("delete failed".to_owned()))
        .await;

    let forgotten = service
        .forget(
            user_context(1201),
            MemoryForgetParams {
                target: MemoryForgetTarget::Id {
                    memory_id: remembered.record.id.clone(),
                },
                reason: Some("test failure".to_owned()),
                actor: None,
                dry_run: false,
            },
        )
        .await
        .expect("forget");
    assert_eq!(
        forgotten.forgotten_memory_ids,
        vec![remembered.record.id.clone()]
    );

    let control = store
        .get_agent_memory_record(remembered.record.id.as_str(), true)
        .await
        .expect("control")
        .expect("row");
    assert_eq!(control.status, MemoryStatus::Deleted);

    let search = service
        .search(
            user_context(1202),
            MemorySearchParams {
                query: "restore visibility".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert!(search.hits.is_empty());

    let claimed = service
        .claim_due_repair_jobs(1203, 60, "repair_worker", 10)
        .await
        .expect("claim");
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].job_kind, "backend_delete_failed");
    assert!(
        !claimed[0]
            .payload_json
            .as_deref()
            .unwrap_or_default()
            .contains("Delete failure should not restore visibility.")
    );
}

#[tokio::test]
async fn quarantine_hides_memory_from_product_recall_and_restore_reenables_it() {
    let (store, _backend, service) = setup_service().await;
    let remembered = service
        .remember(
            user_context(1300),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("quarantine.identity"),
                "User name is Александр.",
            ),
        )
        .await
        .expect("remember");

    let quarantined = service
        .quarantine_memory(
            user_context(1301),
            MemoryQuarantineRequest {
                memory_id: remembered.record.id.clone(),
                reason_code: MemoryLifecycleReasonCode::ManualDeveloperAdminQuarantine,
                actor: None,
                details_json: None,
                schedule_backend_cleanup: true,
            },
        )
        .await
        .expect("quarantine");
    assert_eq!(quarantined.quarantine.memory_id, remembered.record.id);
    assert_eq!(
        quarantined
            .repair_job
            .as_ref()
            .expect("cleanup repair job")
            .job_kind,
        "backend_quarantine_cleanup"
    );

    let search = service
        .search(
            user_context(1302),
            MemorySearchParams {
                query: "Александр".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert!(search.hits.is_empty());

    let prompt = service
        .recall_for_prompt(
            user_context(1303),
            MemoryRecallParams {
                query: "Александр".to_owned(),
                scopes: vec![scope(MemoryScopeKind::User, "default")],
                categories: vec![MemoryCategory::Identity],
                top_k: Some(5),
                max_chars: Some(500),
            },
        )
        .await
        .expect("prompt recall");
    assert!(prompt.items.is_empty());
    assert!(
        prompt
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("suppress_quarantined"))
    );

    let exact = service
        .recall_mode_for_prompt(
            user_context(1304),
            MemoryModeRecallParams {
                mode: MemoryRecallMode::ExactCanonical,
                targets: vec![MemoryRecallTarget {
                    scope_kind: Some(MemoryScopeKind::User),
                    category: Some(MemoryCategory::Identity),
                    canonical_key: Some("quarantine.identity".to_owned()),
                    ..Default::default()
                }],
                top_k: Some(5),
                max_chars: Some(500),
            },
        )
        .await
        .expect("exact recall");
    assert!(exact.items.is_empty());

    let repeated = service
        .quarantine_memory(
            user_context(1305),
            MemoryQuarantineRequest {
                memory_id: remembered.record.id.clone(),
                reason_code: MemoryLifecycleReasonCode::ManualDeveloperAdminQuarantine,
                actor: None,
                details_json: None,
                schedule_backend_cleanup: true,
            },
        )
        .await
        .expect("repeat quarantine");
    assert_eq!(repeated.quarantine.id, quarantined.quarantine.id);
    assert_eq!(
        repeated.repair_job.as_ref().map(|job| job.id.clone()),
        quarantined.repair_job.as_ref().map(|job| job.id.clone())
    );

    let restored = service
        .restore_quarantined_memory(
            user_context(1306),
            MemoryRestoreRequest {
                memory_id: remembered.record.id.clone(),
                actor: None,
                schedule_backend_reindex: true,
            },
        )
        .await
        .expect("restore");
    assert!(restored.quarantine.is_some());
    assert_eq!(
        restored
            .repair_job
            .as_ref()
            .expect("reindex repair job")
            .job_kind,
        "backend_restore_reindex"
    );

    let restored_search = service
        .search(
            user_context(1307),
            MemorySearchParams {
                query: "Александр".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search after restore");
    assert_eq!(restored_search.hits.len(), 1);

    let history = store
        .list_agent_memory_quarantine_history(remembered.record.id.as_str(), 10)
        .await
        .expect("quarantine history");
    assert_eq!(history.len(), 1);
    assert!(history[0].resolved_at_unix.is_some());
}

#[tokio::test]
async fn restore_does_not_resurrect_deleted_quarantined_memory() {
    let (store, _backend, service) = setup_service().await;
    let remembered = service
        .remember(
            user_context(1400),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("quarantine.deleted"),
                "Deleted quarantined memory should stay hidden.",
            ),
        )
        .await
        .expect("remember");
    service
        .quarantine_memory(
            user_context(1401),
            MemoryQuarantineRequest {
                memory_id: remembered.record.id.clone(),
                reason_code: MemoryLifecycleReasonCode::ManualDeveloperAdminQuarantine,
                actor: None,
                details_json: None,
                schedule_backend_cleanup: false,
            },
        )
        .await
        .expect("quarantine");
    store
        .mark_agent_memory_deleted(
            remembered.record.id.as_str(),
            None,
            Some("deleted during quarantine".to_owned()),
            1402,
        )
        .await
        .expect("delete");

    let restored = service
        .restore_quarantined_memory(
            user_context(1403),
            MemoryRestoreRequest {
                memory_id: remembered.record.id.clone(),
                actor: None,
                schedule_backend_reindex: true,
            },
        )
        .await
        .expect("restore");
    assert!(restored.quarantine.is_some());
    assert!(restored.repair_job.is_none());

    let search = service
        .search(
            user_context(1404),
            MemorySearchParams {
                query: "quarantined memory".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert!(search.hits.is_empty());
}

#[tokio::test]
async fn quarantine_workspace_guard_blocks_other_workspace() {
    let (_store, _backend, service) = setup_service().await;
    let remembered = service
        .remember(
            workspace_context("ws_quarantine_a", 1500),
            remember_params(
                scope(MemoryScopeKind::Workspace, "ws_quarantine_a"),
                Some("workspace.secret"),
                "Workspace A memory.",
            ),
        )
        .await
        .expect("remember");

    let result = service
        .quarantine_memory(
            workspace_context("ws_quarantine_b", 1501),
            MemoryQuarantineRequest {
                memory_id: remembered.record.id,
                reason_code: MemoryLifecycleReasonCode::ManualDeveloperAdminQuarantine,
                actor: None,
                details_json: None,
                schedule_backend_cleanup: false,
            },
        )
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn repair_worker_processes_quarantine_cleanup_and_restore_reindex() {
    let (_store, _backend, service) = setup_service().await;
    let remembered = service
        .remember(
            user_context(1600),
            remember_params(
                scope(MemoryScopeKind::User, "default"),
                Some("repair.quarantine"),
                "Repair worker quarantine memory.",
            ),
        )
        .await
        .expect("remember");

    service
        .quarantine_memory(
            user_context(1601),
            MemoryQuarantineRequest {
                memory_id: remembered.record.id.clone(),
                reason_code: MemoryLifecycleReasonCode::ManualDeveloperAdminQuarantine,
                actor: None,
                details_json: None,
                schedule_backend_cleanup: true,
            },
        )
        .await
        .expect("quarantine");
    let cleanup = service
        .claim_due_repair_jobs(1602, 60, "repair_worker", 10)
        .await
        .expect("claim cleanup");
    assert_eq!(cleanup.len(), 1);
    let completed_cleanup = service
        .process_repair_job(cleanup[0].id.as_str(), "repair_worker", 1603)
        .await
        .expect("process cleanup")
        .expect("completed cleanup");
    assert_eq!(completed_cleanup.status, "completed");

    service
        .restore_quarantined_memory(
            user_context(1604),
            MemoryRestoreRequest {
                memory_id: remembered.record.id.clone(),
                actor: None,
                schedule_backend_reindex: true,
            },
        )
        .await
        .expect("restore");
    let reindex = service
        .claim_due_repair_jobs(1605, 60, "repair_worker", 10)
        .await
        .expect("claim reindex");
    assert_eq!(reindex.len(), 1);
    assert_eq!(reindex[0].job_kind, "backend_restore_reindex");
    let completed_reindex = service
        .process_repair_job(reindex[0].id.as_str(), "repair_worker", 1606)
        .await
        .expect("process reindex")
        .expect("completed reindex");
    assert_eq!(completed_reindex.status, "completed");

    let search = service
        .search(
            user_context(1607),
            MemorySearchParams {
                query: "quarantine memory".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search");
    assert_eq!(search.hits.len(), 1);
}

#[tokio::test]
async fn candidate_terminal_transitions_are_idempotent_and_do_not_hide_memory() {
    let (_store, _backend, service) = setup_service().await;
    let active = service
        .write_semantic_memory(
            user_context(1700),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Explicit),
                "User's name is Alexander.",
                "Alexander",
                MemorySemanticWriteDisposition::AcceptActive,
                "turn_candidate_terminal_active",
            ),
        )
        .await
        .expect("active write")
        .record
        .expect("active record");
    let candidate = service
        .write_semantic_memory(
            user_context(1701),
            semantic_write_params(
                relationship_semantic("terminal-candidate"),
                "Terminal candidate fact.",
                "terminal-candidate-value",
                MemorySemanticWriteDisposition::CreatePendingCandidate,
                "turn_candidate_terminal_pending",
            ),
        )
        .await
        .expect("candidate write")
        .candidate
        .expect("candidate");

    let rejected = service
        .reject_candidate(
            user_context(1702),
            pioneer_protocol::MemoryCandidatesRejectParams {
                candidate_id: candidate.id.clone(),
                reason: Some("not useful".to_owned()),
                actor: None,
            },
        )
        .await
        .expect("reject");
    assert_eq!(rejected.candidate.status, MemoryCandidateStatus::Rejected);

    let rejected_again = service
        .reject_candidate(
            user_context(1703),
            pioneer_protocol::MemoryCandidatesRejectParams {
                candidate_id: candidate.id.clone(),
                reason: Some("still not useful".to_owned()),
                actor: None,
            },
        )
        .await
        .expect("reject again");
    assert_eq!(
        rejected_again.candidate.status,
        MemoryCandidateStatus::Rejected
    );

    let approve_rejected = service
        .approve_candidate(
            user_context(1704),
            MemoryCandidatesApproveParams {
                candidate_id: candidate.id,
                reason: Some("should not approve terminal rejected".to_owned()),
                actor: None,
            },
        )
        .await;
    assert!(approve_rejected.is_err());

    let search = service
        .search(
            user_context(1705),
            MemorySearchParams {
                query: "Alexander".to_owned(),
                ..Default::default()
            },
        )
        .await
        .expect("search active");
    assert_eq!(search.hits.len(), 1);
    assert_eq!(search.hits[0].record.id, active.id);
}
