use migration::{Migrator, MigratorTrait};
use pioneer_crud::{
    AgentMemoryListFilter, CrudStore, NewAgentMemoryRepairJob, global_agent_memory_scope_key,
    workspace_agent_memory_scope_key,
};
use pioneer_memory::{
    BackendDeleteRequest, BackendDeleteResult, BackendGetRequest, BackendPayload,
    BackendPutRequest, BackendPutResult, BackendSearchHit, BackendSearchRequest,
    InMemoryMemoryBackend, MemoryBackend, MemoryOperationContext, MemoryReadPolicy,
    MemoryRecallParams, MemoryService, MemoryServiceConfig,
};
use pioneer_protocol::{
    MemoryActor, MemoryActorKind, MemoryAttribute, MemoryCandidateStatus,
    MemoryCandidatesApproveParams, MemoryCandidatesEditAndApproveParams, MemoryCategory,
    MemoryDurability, MemoryExplicitness, MemoryExtractorCertainty, MemoryForgetParams,
    MemoryForgetTarget, MemoryGetParams, MemoryIntent, MemoryListParams, MemoryRememberParams,
    MemoryScope, MemoryScopeHint, MemoryScopeKind, MemorySearchParams, MemorySemanticFields,
    MemorySemanticWriteDisposition, MemorySemanticWriteParams, MemorySensitivity,
    MemorySensitivityHint, MemoryStatus, MemorySubject, MemoryWriteEvidence, MemoryWriteRelation,
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
        MemoryCandidateStatus::Rejected
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
    let record = response.record.expect("auto-approved record");
    assert_eq!(
        record
            .metadata
            .get("candidate_score_bucket")
            .and_then(serde_json::Value::as_str),
        Some("high")
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
    assert!(decisions.iter().any(|decision| {
        decision.action == "candidate_policy" && decision.decision == "auto_approve"
    }));
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
    let (_store, _backend, service) = setup_service().await;
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
    let transient_candidate = transient.candidate.expect("transient candidate");
    assert_eq!(
        transient_candidate.status,
        MemoryCandidateStatus::AutoRejected
    );
    assert_eq!(
        transient_candidate
            .metadata
            .get("candidate_policy_reason_code")
            .and_then(serde_json::Value::as_str),
        Some("transient_or_session_only")
    );

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
    let secret_candidate = secret.candidate.expect("secret candidate");
    assert_eq!(secret_candidate.status, MemoryCandidateStatus::AutoRejected);
    assert_eq!(
        secret_candidate
            .metadata
            .get("candidate_policy_reason_code")
            .and_then(serde_json::Value::as_str),
        Some("secret_like_or_regulated")
    );
}

#[tokio::test]
async fn semantic_route_middle_fact_rejects_by_default_and_routes_when_review_enabled() {
    let (_store, _backend, service) = setup_service().await;
    let mut middle_semantic = identity_name_semantic(MemoryExplicitness::Unclear);
    middle_semantic.intent = MemoryIntent::ImplicitCandidate;
    middle_semantic.durability = MemoryDurability::Unknown;
    middle_semantic.certainty = MemoryExtractorCertainty::Medium;
    middle_semantic.scope_hint = MemoryScopeHint::Unknown;

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
    assert_eq!(
        review_response.candidate.expect("review candidate").status,
        MemoryCandidateStatus::PendingSilent
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
    assert_eq!(
        contradiction
            .candidate
            .expect("ask-on-use candidate")
            .status,
        MemoryCandidateStatus::AskOnUse
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
    middle_semantic.durability = MemoryDurability::Unknown;
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

    let candidate = service
        .write_semantic_memory(
            user_context(393),
            semantic_write_params(
                identity_name_semantic(MemoryExplicitness::Implicit),
                "The user's name may be Alexey.",
                "Alexey",
                MemorySemanticWriteDisposition::RouteToCandidatePolicy,
                "turn_candidate_edit_candidate",
            ),
        )
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
    let mut middle_semantic = identity_name_semantic(MemoryExplicitness::Unclear);
    middle_semantic.intent = MemoryIntent::ImplicitCandidate;
    middle_semantic.durability = MemoryDurability::Unknown;
    middle_semantic.certainty = MemoryExtractorCertainty::Medium;
    middle_semantic.scope_hint = MemoryScopeHint::ProjectWorkspace;
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
            source_kind: pioneer_protocol::MemorySourceKind::ExplicitUserRequest,
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
