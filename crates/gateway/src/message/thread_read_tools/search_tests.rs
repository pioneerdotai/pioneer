//! Tool/recall integration over real canonical rows and the existing ACL fixture.
use super::*;
use crate::thread_episodic::{
    ThreadEpisodicRecallService,
    tests::{
        mark_thread_episodic_workspace_refill_complete_for_test, seed_active_thread_episodic_item,
    },
};
use pioneer_memory::episodic_search::{
    EpisodicChannelCandidates, EpisodicQueryEmbedding, EpisodicSourceCandidate,
};
use pioneer_memory::{
    ThreadEpisodicMemvidBackend, ThreadEpisodicMemvidBackendCapabilities,
    ThreadEpisodicMemvidError, ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidIndexRequest,
    ThreadEpisodicMemvidSearchOutput, ThreadEpisodicMemvidSearchRequest,
};
use pioneer_protocol::{ThreadsSearchParams, Turn, TurnItem, TurnKind, TurnOrigin, TurnStatus};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct RecordingBackend {
    hits: std::sync::Mutex<Vec<EpisodicSourceCandidate>>,
    calls: AtomicUsize,
    semantic_calls: AtomicUsize,
    fail_semantic: AtomicBool,
    complete: AtomicBool,
    unavailable: AtomicBool,
    queries: std::sync::Mutex<Vec<String>>,
}
#[async_trait]
impl ThreadEpisodicMemvidBackend for RecordingBackend {
    fn capabilities(&self) -> ThreadEpisodicMemvidBackendCapabilities {
        ThreadEpisodicMemvidBackendCapabilities::memvid_default()
    }
    async fn index_item(
        &self,
        _: ThreadEpisodicMemvidIndexRequest,
    ) -> Result<ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidError> {
        panic!("search does not index")
    }
    async fn search(
        &self,
        _: ThreadEpisodicMemvidSearchRequest,
    ) -> Result<ThreadEpisodicMemvidSearchOutput, ThreadEpisodicMemvidError> {
        panic!("shared search must use direct channels")
    }
    async fn source_candidates(
        &self,
        request: ThreadEpisodicMemvidSearchRequest,
        embedding: Option<EpisodicQueryEmbedding>,
        _capacity: Arc<tokio::sync::OwnedSemaphorePermit>,
    ) -> EpisodicChannelCandidates {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.queries.lock().unwrap().push(request.query);
        if self.unavailable.load(Ordering::SeqCst) {
            return EpisodicChannelCandidates {
                unavailable: true,
                ..Default::default()
            };
        }
        if embedding.is_some() {
            self.semantic_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_semantic.load(Ordering::SeqCst) {
                return EpisodicChannelCandidates {
                    unavailable: true,
                    ..Default::default()
                };
            }
        }
        EpisodicChannelCandidates {
            partitions: vec![self.hits.lock().unwrap().clone()],
            coverage_limited: !self.complete.load(Ordering::SeqCst),
            unavailable: false,
        }
    }
}

async fn canonical_source(
    processor: &MessageProcessor,
    thread_id: &str,
    text: &str,
) -> EpisodicSourceCandidate {
    canonical_source_for(processor, thread_id, text, MEMBER_A_ID).await
}
async fn canonical_source_for(
    processor: &MessageProcessor,
    thread_id: &str,
    text: &str,
    principal_id: &str,
) -> EpisodicSourceCandidate {
    let thread = processor
        .crud_store
        .get_thread_model(thread_id)
        .await
        .unwrap()
        .unwrap();
    let turn_id = pioneer_protocol::generate_id(21);
    let item_id = pioneer_protocol::generate_id(21);
    let turn = Turn {
        id: turn_id.clone(),
        status: TurnStatus::Completed,
        turn_kind: TurnKind::Conversation,
        origin: TurnOrigin::User,
        mode: Default::default(),
        author: None,
        reply_to_turn_id: None,
        mentions: Vec::new(),
        message_revision: 0,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
    };
    processor
        .crud_store
        .materialize_turn_start(
            &thread,
            pioneer_protocol::SandboxMode::FullAccess,
            &turn,
            &[],
            pioneer_protocol::PersistedActorRef::Principal(PrincipalId::new(principal_id).unwrap()),
        )
        .await
        .unwrap();
    processor
        .crud_store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: WORKSPACE_RED_ID.into(),
                thread_id: thread_id.into(),
                turn_id: turn_id.clone(),
                item: TurnItem::UserMessage {
                    id: item_id.clone(),
                    text: text.into(),
                    attachments: Vec::new(),
                },
            },
            1_700_000_000,
        )
        .await
        .unwrap();
    let item = seed_active_thread_episodic_item(
        processor.crud_store.as_ref(),
        WORKSPACE_RED_ID,
        thread_id,
        &turn_id,
        &item_id,
        text,
    )
    .await;
    EpisodicSourceCandidate {
        source_id: item.id,
        workspace_id: WORKSPACE_RED_ID.into(),
        thread_id: thread_id.into(),
        turn_id,
        item_id,
        source_version: item.source_text_hash,
    }
}

async fn search_fixture() -> (
    IsolatedEpic4Harness,
    Arc<MessageProcessor>,
    Arc<RecordingBackend>,
    TurnToolContext,
) {
    let (harness, mut processor) = fixture().await;
    let backend = Arc::new(RecordingBackend::default());
    processor.thread_episodic_recall_service = Arc::new(ThreadEpisodicRecallService::new(
        processor.crud_store.clone(),
        backend.clone(),
    ));
    mark_thread_episodic_workspace_refill_complete_for_test(
        processor.crud_store.as_ref(),
        WORKSPACE_RED_ID,
    )
    .await;
    let source = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "execution source").await;
    persist_test_execution_authorization_context_for_principal(
        &processor,
        &member(MEMBER_A_ID),
        WORKSPACE_RED_ID,
        THREAD_RED_PRIVATE_A_ID,
        &source.turn_id,
    )
    .await;
    let context = TurnToolContext {
        workspace_id: WORKSPACE_RED_ID.into(),
        thread_id: THREAD_RED_PRIVATE_A_ID.into(),
        turn_id: source.turn_id,
    };
    // Explicitly admitted observation in the immutable test execution fixture.
    let mut envelope = serde_json::to_value(
        processor
            .load_turn_execution_authorization_context(&context.turn_id)
            .await
            .unwrap(),
    )
    .unwrap();
    let actions = envelope["grant_manifest"]["actions"]
        .as_array_mut()
        .unwrap();
    actions.push(json!("thread_observe_workspace"));
    actions.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
    actions.dedup();
    processor
        .crud_store
        .set_turn_execution_authorization_context(&context.turn_id, &envelope.to_string())
        .await
        .unwrap();

    (harness, Arc::new(processor), backend, context)
}

async fn page(
    processor: &MessageProcessor,
    context: &TurnToolContext,
    query: &str,
    cursor: Option<String>,
) -> Result<Value> {
    let authority = processor.thread_read_authority(context).await?;
    processor
        .thread_search_page(
            context,
            &authority,
            ThreadsSearchParams {
                query: query.into(),
                limit: 1,
                cursor,
            },
            &processor.thread_read_cursor_codec()?,
        )
        .await
}

#[tokio::test]
async fn pages_use_one_retrieval_restore_real_anchors_and_skip_changed_sources() {
    let (_harness, processor, backend, context) = search_fixture().await;
    let first = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider alpha").await;
    let second = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider beta").await;
    *backend.hits.lock().unwrap() = vec![first.clone(), second.clone()];
    let initial = page(&processor, &context, "provider", None).await.unwrap();
    assert_eq!(
        initial["threads"][0]["fragments"][0]["source"],
        json!({"threadId":first.thread_id,"anchor":{"turnId":first.turn_id,"itemId":first.item_id}})
    );
    let source = &initial["threads"][0]["fragments"][0]["source"];
    let read: ThreadsHistoryReadParams = serde_json::from_value(
        json!({"threadId":source["threadId"],"anchor":source["anchor"],"limit":1}),
    )
    .unwrap();
    let history = processor
        .read_saved_thread_history_near(
            WORKSPACE_RED_ID,
            &read.thread_id,
            1,
            None,
            read.anchor,
            read.direction,
            |_| async { Ok(false) },
        )
        .await
        .unwrap();
    assert!(!history.events.is_empty());
    let cursor = initial["nextCursor"].as_str().unwrap().to_owned();
    // New matches are not appended to the saved selection.
    let new_source = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider gamma").await;
    backend.hits.lock().unwrap().push(new_source);
    processor
        .crud_store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "DELETE FROM turn_item WHERE turn_id=? AND item_id=?",
            [second.turn_id.into(), second.item_id.into()],
        ))
        .await
        .unwrap();
    let continuation = page(&processor, &context, "provider", Some(cursor))
        .await
        .unwrap();
    assert_eq!(continuation["threads"], json!([]));
    assert_eq!(continuation["eof"], true);
    assert_eq!(continuation["limitations"]["coverageLimited"], true);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn search_and_workspace_recall_share_scope_and_do_not_trust_hook_principal() {
    use pioneer_memory::hooks::{
        AgentEpisodicRecallProvider, MemoryEpisodicRecallSourceKind,
        MemoryWorkspaceThreadRecallRequest,
    };
    let (_harness, processor, backend, context) = search_fixture().await;
    // Catalog order/title is irrelevant: candidates come from workspace indexes.
    for n in 0..12 {
        add_thread(&processor, &format!("catalog-{n:02}"), None, false).await;
    }
    let private = canonical_source(
        &processor,
        THREAD_RED_PRIVATE_A_ID,
        "private provider configuration",
    )
    .await;
    let workspace = canonical_source(
        &processor,
        THREAD_RED_WORKSPACE_ID,
        "workspace provider configuration",
    )
    .await;
    let forbidden = canonical_source(
        &processor,
        THREAD_RED_PRIVATE_B_ID,
        "forbidden provider configuration",
    )
    .await;
    *backend.hits.lock().unwrap() = vec![private.clone(), workspace.clone(), forbidden];
    let result = processor
        .search_thread_sources(&context, "provider", None)
        .await
        .unwrap();
    let ids = result
        .sources
        .iter()
        .map(|s| s.reference.thread_id.as_str())
        .collect::<Vec<_>>();
    assert!(ids.contains(&THREAD_RED_PRIVATE_A_ID));
    assert!(ids.contains(&THREAD_RED_WORKSPACE_ID));
    assert!(!ids.contains(&THREAD_RED_PRIVATE_B_ID));
    let provider = crate::thread_episodic_hooks::ThreadEpisodicMemoryRecallProvider::new(
        Arc::downgrade(&processor),
        processor.thread_episodic_recall_service.clone(),
    );
    let recall = provider
        .recall_workspace_threads(MemoryWorkspaceThreadRecallRequest {
            workspace_id: WORKSPACE_RED_ID.into(),
            current_thread_id: THREAD_RED_PRIVATE_A_ID.into(),
            turn_id: context.turn_id.clone(),
            principal_id: Some(MEMBER_B_ID.into()),
            query: "provider".into(),
            top_k: 10,
            max_chars: 2400,
            targets: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(recall.items.len(), 2);
    assert!(
        recall
            .items
            .iter()
            .any(|s| s.provenance.source == MemoryEpisodicRecallSourceKind::CurrentThread)
    );
    assert!(
        recall
            .items
            .iter()
            .any(|s| s.provenance.source == MemoryEpisodicRecallSourceKind::WorkspaceThread)
    );
    let mut missing = context.clone();
    missing.turn_id = "missing-execution".into();
    assert!(
        processor
            .search_thread_sources(&missing, "provider", None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn continuation_is_bound_to_query_principal_workspace_and_current_acl() {
    let (harness, processor, backend, context) = search_fixture().await;
    *backend.hits.lock().unwrap() = vec![
        canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider one").await,
        canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider two").await,
    ];
    let initial = page(&processor, &context, "provider", None).await.unwrap();
    let cursor = initial["nextCursor"].as_str().unwrap().to_owned();
    assert!(
        page(&processor, &context, "other query", Some(cursor.clone()))
            .await
            .is_err()
    );
    let mut tampered = cursor.clone().into_bytes();
    let last = tampered.len() - 2;
    tampered[last] = if tampered[last] == b'A' { b'B' } else { b'A' };
    assert!(
        page(
            &processor,
            &context,
            "provider",
            Some(String::from_utf8(tampered).unwrap())
        )
        .await
        .is_err()
    );
    let mut different_workspace = context.clone();
    different_workspace.workspace_id = "other-workspace".into();
    assert!(
        page(
            &processor,
            &different_workspace,
            "provider",
            Some(cursor.clone())
        )
        .await
        .is_err()
    );
    let other = canonical_source_for(
        &processor,
        THREAD_RED_PRIVATE_B_ID,
        "other execution",
        MEMBER_B_ID,
    )
    .await;
    persist_test_execution_authorization_context_for_principal(
        &processor,
        &member(MEMBER_B_ID),
        WORKSPACE_RED_ID,
        THREAD_RED_PRIVATE_B_ID,
        &other.turn_id,
    )
    .await;
    let other_context = TurnToolContext {
        workspace_id: WORKSPACE_RED_ID.into(),
        thread_id: THREAD_RED_PRIVATE_B_ID.into(),
        turn_id: other.turn_id,
    };
    assert!(
        page(&processor, &other_context, "provider", Some(cursor.clone()))
            .await
            .unwrap_err()
            .to_string()
            .contains("cursor")
    );
    harness
        .database
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "DELETE FROM workspace_membership WHERE principal_id=? AND workspace_id=?",
            [MEMBER_A_ID.into(), WORKSPACE_RED_ID.into()],
        ))
        .await
        .unwrap();
    assert!(
        page(&processor, &context, "provider", Some(cursor))
            .await
            .is_err()
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
}

struct QueryProvider {
    calls: AtomicUsize,
    fail: AtomicBool,
}
impl pioneer_memory::ThreadEpisodicEmbeddingProvider for QueryProvider {
    fn provider_id(&self) -> &str {
        "openrouter"
    }
    fn model(&self) -> &str {
        "custom/test-embedding"
    }
    fn dimension(&self) -> usize {
        3
    }
    fn normalized(&self) -> bool {
        true
    }
    fn embed_text(
        &self,
        _: &str,
    ) -> Result<Vec<f32>, pioneer_memory::ThreadEpisodicEmbeddingError> {
        panic!("retrieval must use query-specific embedding")
    }
    fn embed_query(
        &self,
        _: &str,
    ) -> Result<Vec<f32>, pioneer_memory::ThreadEpisodicEmbeddingError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            Err(
                pioneer_memory::ThreadEpisodicEmbeddingError::retryable_provider_failure(
                    "openrouter",
                    "custom/test-embedding",
                    "unavailable",
                ),
            )
        } else {
            Ok(vec![1.0, 0.0, 0.0])
        }
    }
}

#[tokio::test]
async fn hybrid_embeds_query_once_and_pages_do_not_reembed_with_lexical_fallback_on_failure() {
    use crate::thread_episodic::{
        SharedThreadEpisodicIndexEmbeddingProviderResolver, ThreadEpisodicRecallServiceConfig,
        tests::mark_thread_episodic_workspace_vector_refill_complete_for_test,
    };
    let (_harness, processor, backend, context) = search_fixture().await;
    let mut processor = Arc::try_unwrap(processor).ok().unwrap();
    let provider = Arc::new(QueryProvider {
        calls: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
    });
    let resolver = Arc::new(SharedThreadEpisodicIndexEmbeddingProviderResolver::new());
    resolver.set_active_provider(Some(provider.clone()));
    let config = pioneer_config::GatewayThreadEpisodicVectorSearchConfig {
        enabled: true,
        provider: Some(pioneer_config::GatewayThreadEpisodicVectorProviderConfig::OpenRouter),
        model: Some("custom/test-embedding".into()),
        local_model: None,
        embedding_normalized: true,
        use_search_instructions: false,
    };
    mark_thread_episodic_workspace_vector_refill_complete_for_test(
        processor.crud_store.as_ref(),
        WORKSPACE_RED_ID,
        &config,
    )
    .await;
    processor.thread_episodic_recall_service = Arc::new(
        ThreadEpisodicRecallService::with_config_and_embedding_provider_resolver(
            processor.crud_store.clone(),
            backend.clone(),
            ThreadEpisodicRecallServiceConfig {
                vector_search_enabled: true,
                vector_search: config,
                ..Default::default()
            },
            Some(resolver),
        ),
    );
    *backend.hits.lock().unwrap() = vec![
        canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider alpha").await,
        canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider beta").await,
    ];
    let first = page(&processor, &context, "provider", None).await.unwrap();
    let last = page(
        &processor,
        &context,
        "provider",
        Some(first["nextCursor"].as_str().unwrap().into()),
    )
    .await
    .unwrap();
    assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.semantic_calls.load(Ordering::SeqCst), 1);
    assert_eq!(last["limitations"]["semanticApproximate"], true);
    assert_eq!(last["limitations"]["semanticDegraded"], false);
    backend.fail_semantic.store(true, Ordering::SeqCst);
    let degraded = page(&processor, &context, "provider", None).await.unwrap();
    assert!(!degraded["threads"].as_array().unwrap().is_empty());
    assert_eq!(degraded["limitations"]["semanticDegraded"], true);
    provider.fail.store(true, Ordering::SeqCst);
    let failed_embedding = page(&processor, &context, "provider", None).await.unwrap();
    assert!(!failed_embedding["threads"].as_array().unwrap().is_empty());
    assert_eq!(failed_embedding["limitations"]["semanticDegraded"], true);
    assert_eq!(
        backend.semantic_calls.load(Ordering::SeqCst),
        2,
        "failed embedding does not call vector retrieval"
    );
    // Approximation alone is successful; a failed configured channel is not.
    provider.fail.store(false, Ordering::SeqCst);
    backend.complete.store(true, Ordering::SeqCst);
    backend.hits.lock().unwrap().clear();
    let processor = Arc::new(processor);
    let runtime = installed_recall_runtime(&processor).await;
    for degraded in [false, true] {
        backend.fail_semantic.store(degraded, Ordering::SeqCst);
        let request = recall_phase(&context, "provider configuration");
        let prior = aggregate_recall_phase(runtime.run_phase(request.clone()).await.unwrap());
        assert_eq!(
            prior
                .diagnostics
                .iter()
                .any(|d| d.code.as_str() == "thread_context.search_empty"),
            !degraded
        );
        let before = backend.calls.load(Ordering::SeqCst);
        runtime
            .run_phase(post_recall_phase(
                request,
                prior,
                "provider configuration",
                episodic_plan("current_thread", "provider configuration"),
            ))
            .await
            .unwrap();
        assert_eq!(
            backend.calls.load(Ordering::SeqCst) - before,
            if degraded { 2 } else { 0 }
        );
    }
}

#[tokio::test]
async fn immutable_read_grants_limit_search_even_when_the_user_can_read_workspace_threads() {
    let (_harness, processor, backend, context) = search_fixture().await;
    let private = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "private provider").await;
    let workspace =
        canonical_source(&processor, THREAD_RED_WORKSPACE_ID, "workspace provider").await;
    *backend.hits.lock().unwrap() = vec![private, workspace];
    let envelope = processor
        .load_turn_execution_authorization_context(&context.turn_id)
        .await
        .unwrap();
    let mut encoded = serde_json::to_value(envelope).unwrap();
    encoded["grant_manifest"]["actions"]
        .as_array_mut()
        .unwrap()
        .retain(|action| action != "thread_observe_workspace");
    processor
        .crud_store
        .set_turn_execution_authorization_context(&context.turn_id, &encoded.to_string())
        .await
        .unwrap();
    let restricted = processor
        .search_thread_sources(&context, "provider", None)
        .await
        .unwrap();
    assert_eq!(restricted.sources.len(), 1);
    assert_eq!(
        restricted.sources[0].reference.thread_id,
        THREAD_RED_PRIVATE_A_ID
    );
    encoded["grant_manifest"]["actions"]
        .as_array_mut()
        .unwrap()
        .retain(|action| action != "thread_read");
    processor
        .crud_store
        .set_turn_execution_authorization_context(&context.turn_id, &encoded.to_string())
        .await
        .unwrap();
    assert!(
        processor
            .search_thread_sources(&context, "provider", None)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn canonical_validation_suppresses_deleted_excluded_hidden_stale_and_missing_sources() {
    let (_harness, processor, backend, context) = search_fixture().await;
    let good = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider visible").await;
    let deleted = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider deleted").await;
    let excluded = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider excluded").await;
    let hidden = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider hidden").await;
    let stale = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider stale").await;
    let missing = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider missing").await;
    let db = processor.crud_store.database_connection();
    for (sql, id) in [
        (
            "UPDATE thread_episodic_items SET status='deleted',deleted_at=CURRENT_TIMESTAMP WHERE id=?",
            &deleted.source_id,
        ),
        (
            "UPDATE thread_episodic_items SET status='excluded' WHERE id=?",
            &excluded.source_id,
        ),
        (
            "UPDATE thread_episodic_items SET visibility='internal_hidden' WHERE id=?",
            &hidden.source_id,
        ),
        (
            "UPDATE thread_episodic_items SET source_text_hash='new-version' WHERE id=?",
            &stale.source_id,
        ),
    ] {
        db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            sql,
            [id.clone().into()],
        ))
        .await
        .unwrap();
    }
    db.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "DELETE FROM turn_item WHERE turn_id=? AND item_id=?",
        [
            missing.turn_id.clone().into(),
            missing.item_id.clone().into(),
        ],
    ))
    .await
    .unwrap();
    *backend.hits.lock().unwrap() = vec![
        deleted.clone(),
        excluded,
        hidden,
        stale,
        missing,
        good.clone(),
    ];
    let result = page(&processor, &context, "provider", None).await.unwrap();
    assert_eq!(
        result["threads"][0]["fragments"][0]["snippet"],
        "provider visible"
    );
    let encoded = result.to_string();
    assert!(!encoded.contains(&deleted.source_id));
    for forbidden in [
        "provider deleted",
        "provider excluded",
        "provider hidden",
        "provider stale",
        "provider missing",
    ] {
        assert!(!encoded.contains(forbidden));
    }
}

#[tokio::test]
async fn administrator_search_uses_current_superuser_rights_and_an_explicit_execution_grant() {
    let (_harness, processor, backend, _) = search_fixture().await;
    let admin_id = crate::auth::test_support::TEST_SUPERUSER_ID;
    let source = canonical_source_for(
        &processor,
        THREAD_RED_PRIVATE_A_ID,
        "admin execution",
        admin_id,
    )
    .await;
    let mut admin = member(admin_id);
    admin.kind = PrincipalKind::Superuser;
    admin.role_key = None;
    persist_test_execution_authorization_context_for_principal(
        &processor,
        &admin,
        WORKSPACE_RED_ID,
        THREAD_RED_PRIVATE_A_ID,
        &source.turn_id,
    )
    .await;
    let context = TurnToolContext {
        workspace_id: WORKSPACE_RED_ID.into(),
        thread_id: THREAD_RED_PRIVATE_A_ID.into(),
        turn_id: source.turn_id,
    };
    let mut encoded = serde_json::to_value(
        processor
            .load_turn_execution_authorization_context(&context.turn_id)
            .await
            .unwrap(),
    )
    .unwrap();
    let actions = encoded["grant_manifest"]["actions"].as_array_mut().unwrap();
    actions.push(json!("thread_observe_workspace"));
    actions.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
    actions.dedup();
    processor
        .crud_store
        .set_turn_execution_authorization_context(&context.turn_id, &encoded.to_string())
        .await
        .unwrap();
    *backend.hits.lock().unwrap() =
        vec![canonical_source(&processor, THREAD_RED_PRIVATE_B_ID, "private B provider").await];
    let output = processor
        .search_thread_sources(&context, "provider", None)
        .await
        .unwrap();
    assert_eq!(output.sources.len(), 1);
    assert_eq!(
        output.sources[0].reference.thread_id,
        THREAD_RED_PRIVATE_B_ID
    );
    encoded["grant_manifest"]["actions"]
        .as_array_mut()
        .unwrap()
        .retain(|action| action != "thread_observe_workspace");
    processor
        .crud_store
        .set_turn_execution_authorization_context(&context.turn_id, &encoded.to_string())
        .await
        .unwrap();
    assert!(
        processor
            .search_thread_sources(&context, "provider", None)
            .await
            .unwrap()
            .sources
            .is_empty()
    );
}

#[tokio::test]
async fn causal_projections_are_one_result_but_independent_equal_text_remains_searchable() {
    let (_harness, processor, backend, context) = search_fixture().await;
    let original = canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider outcome").await;
    let projection =
        canonical_source(&processor, THREAD_RED_WORKSPACE_ID, "provider outcome").await;
    let independent =
        canonical_source(&processor, THREAD_RED_PRIVATE_A_ID, "provider outcome").await;
    // The existing Pioneer projection-group record is the trusted causal
    // identity; identical text alone must never establish that identity.
    let group = processor
        .crud_store
        .find_thread_episodic_item(&original.source_id)
        .await
        .unwrap()
        .unwrap()
        .projection_group_id;
    processor
        .crud_store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE thread_episodic_items SET projection_group_id=? WHERE id=?",
            [group.into(), projection.source_id.clone().into()],
        ))
        .await
        .unwrap();
    *backend.hits.lock().unwrap() = vec![original.clone(), projection, independent.clone()];
    let output = processor
        .search_thread_sources(&context, "provider", None)
        .await
        .unwrap();
    assert_eq!(output.sources.len(), 2);
    assert_eq!(output.sources[0].reference.source_id, original.source_id);
    assert_eq!(output.sources[1].reference.source_id, independent.source_id);
    assert!((output.sources[0].score - 1.0 / 61.0).abs() < 0.00001);
}

async fn installed_recall_runtime(
    processor: &Arc<MessageProcessor>,
) -> Arc<pioneer_hooks::HookRuntime> {
    let mut config = processor.memory_loop_config();
    config.active_recall = pioneer_memory::hooks::MemoryActiveRecallConfig::default();
    processor.apply_memory_loop_config(config);
    processor.bind_memory_bridge().await;
    processor.hook_runtime.read().await.clone().unwrap()
}

// These requests traverse the installed production hook packages and runtime
// redaction, then aggregate and hand the result to the post-preflight phase.
fn recall_phase(context: &TurnToolContext, query: &str) -> pioneer_hooks::HookPhaseRequest {
    use pioneer_hooks::*;
    let mut request = crate::thread_episodic_hooks::tests::test_phase_request(
        pioneer_memory::hooks::MemoryTurnPolicy::normal_default_allow(),
    );
    request.context.workspace_id = Some(HookWorkspaceId::new(&context.workspace_id).unwrap());
    request.context.thread_id = Some(HookThreadId::new(&context.thread_id).unwrap());
    request.context.turn_id = Some(HookTurnId::new(&context.turn_id).unwrap());
    request.input = HookInput::turn_pre_prompt_context(TurnPrePromptContextHookInput::from_parts(
        query,
        Some("test-model"),
        Some("test-provider"),
    ));
    request
}

fn aggregate_recall_phase(
    response: pioneer_hooks::HookPhaseResponse,
) -> pioneer_hooks::HookPromptContextSet {
    use pioneer_hooks::*;
    let mut context = HookPromptContextSet::aggregate_contributions(
        response.contributions.into_iter().filter_map(|c| match c {
            HookContribution::PromptContext(c) => Some(c),
            _ => None,
        }),
        HookPromptContextLimits::default(),
    );
    context.diagnostics.extend(response.diagnostics);
    context
}

fn episodic_plan(mode: &str, query: &str) -> Value {
    json!({"durable":{"status":"skip","reasonCode":"provider_skip","confidence":1.0,"modes":[],"targets":[]},
        "episodic":{"status":"run","reasonCode":"provider_run","confidence":0.9,
        "queries":[{"mode":mode,"query":query,"targets":[]}]}})
}

fn post_recall_phase(
    mut request: pioneer_hooks::HookPhaseRequest,
    prior: pioneer_hooks::HookPromptContextSet,
    query: &str,
    plan: Value,
) -> pioneer_hooks::HookPhaseRequest {
    use pioneer_hooks::*;
    request.phase = HookPhase::TurnPostPreflightPromptContext;
    request.input = HookInput::turn_post_preflight_prompt_context(
        TurnPostPreflightPromptContextHookInput::from_parts(
            query,
            Some("test-model"),
            Some("test-provider"),
        )
        .with_active_memory_recall_preflight_plan(plan),
    );
    request.prompt_context_set = prior;
    request
}

#[tokio::test]
async fn runtime_reuses_only_successfully_completed_empty_search_across_phases() {
    for (complete, unavailable, expected_calls) in
        [(true, false, 1), (false, false, 2), (true, true, 2)]
    {
        let (_harness, processor, backend, context) = search_fixture().await;
        backend.complete.store(complete, Ordering::SeqCst);
        backend.unavailable.store(unavailable, Ordering::SeqCst);
        let runtime = installed_recall_runtime(&processor).await;
        let request = recall_phase(&context, "provider configuration");
        let prior = aggregate_recall_phase(runtime.run_phase(request.clone()).await.unwrap());
        let receipt = prior
            .diagnostics
            .iter()
            .find(|d| d.code.as_str() == "thread_context.search_empty");
        assert_eq!(receipt.is_some(), complete && !unavailable);
        if let Some(receipt) = receipt {
            assert_eq!(
                receipt.message.as_str(),
                pioneer_memory::episodic_search::episodic_query_fingerprint(
                    WORKSPACE_RED_ID,
                    &context.thread_id,
                    &context.turn_id,
                    "provider configuration",
                )
            );
        }
        if !complete || unavailable {
            // Ordinary diagnostics remain redacted by the actual runtime.
            assert!(
                prior
                    .diagnostics
                    .iter()
                    .any(|d| d.code.as_str() == "thread_context.search_incomplete"
                        && !d.message.as_str().contains("coverage"))
            );
        }
        runtime
            .run_phase(post_recall_phase(
                request,
                prior,
                "provider configuration",
                episodic_plan("current_thread", "provider configuration"),
            ))
            .await
            .unwrap();
        assert_eq!(backend.calls.load(Ordering::SeqCst), expected_calls);
    }
}

#[tokio::test]
async fn current_hook_searches_long_input_while_tool_enforces_documented_utf8_limit() {
    let (_harness, processor, backend, context) = search_fixture().await;
    let boundary = "я".repeat(2048);
    page(&processor, &context, &boundary, None).await.unwrap();
    for query in ["я".repeat(2049), "я".repeat(3000)] {
        assert!(query.chars().count() <= 4096);
        assert!(page(&processor, &context, &query, None).await.is_err());
    }
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    let long = format!("provider configuration\n{}", "лог с кодом\n".repeat(800));
    assert!(long.len() > 4096);
    let runtime = installed_recall_runtime(&processor).await;
    runtime
        .run_phase(recall_phase(&context, &long))
        .await
        .unwrap();
    assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
    assert_eq!(backend.queries.lock().unwrap().last(), Some(&long));
}

#[tokio::test]
async fn production_preflight_capabilities_reach_planning_and_runtime_recall_without_expanding_grants()
 {
    use pioneer_hooks::*;
    use pioneer_memory::hooks::*;
    let (_harness, processor, backend, context) = search_fixture().await;
    *backend.hits.lock().unwrap() = vec![
        canonical_source(
            &processor,
            THREAD_RED_WORKSPACE_ID,
            "workspace provider configuration decision",
        )
        .await,
    ];
    let runtime = installed_recall_runtime(&processor).await;
    let provider: Arc<dyn AgentEpisodicRecallProvider> = Arc::new(
        crate::thread_episodic_hooks::ThreadEpisodicMemoryRecallProvider::new(
            Arc::downgrade(&processor),
            processor.thread_episodic_recall_service.clone(),
        ),
    );
    let memory_context = MemoryTurnContext {
        workspace_id: context.workspace_id.clone(),
        thread_id: context.thread_id.clone(),
        conversation_thread_id: None,
        turn_id: context.turn_id.clone(),
        mode: pioneer_protocol::ThreadMode::Agent,
        input_text: "where did we discuss provider configuration?".into(),
        task_id: None,
        agent_id: None,
        principal_id: Some(MEMBER_B_ID.into()),
    };
    let input = TurnPrePromptContextHookInput::from_parts(
        &memory_context.input_text,
        Some("model"),
        Some("provider"),
    );
    // This is the preparation function called by run_agent_turn_preflight_stage,
    // rather than a capabilities literal or an injected workspace-only plan.
    let prepared = prepare_active_recall_preflight(
        &memory_context,
        &input,
        &MemoryTurnPolicy::normal_default_allow(),
        &MemoryActiveRecallConfig::default(),
        &HookPromptContextSet::default(),
        Some(&provider),
        true,
    )
    .await;
    assert!(
        prepared
            .decision_request
            .available_episodic_modes
            .iter()
            .any(|m| m == "workspace_thread")
    );
    assert!(
        prepared
            .decision_request
            .available_episodic_modes
            .iter()
            .any(|m| m == "related_thread")
    );
    // Recording planner response chooses a mode actually present in its request.
    let mode = prepared
        .decision_request
        .available_episodic_modes
        .iter()
        .find(|m| *m == "workspace_thread")
        .unwrap();
    let plan = active_recall_preflight_provider_success(
        parse_active_memory_decision_json(
            &episodic_plan(mode, "provider configuration").to_string(),
        )
        .unwrap(),
        None,
        None,
    );
    let phase = post_recall_phase(
        recall_phase(&context, &memory_context.input_text),
        HookPromptContextSet::default(),
        &memory_context.input_text,
        serde_json::to_value(&plan).unwrap(),
    );
    let response = runtime.run_phase(phase.clone()).await.unwrap();
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert!(
        response
            .contributions
            .iter()
            .any(|c| matches!(c, HookContribution::PromptContext(c)
        if c.content.as_str().contains("workspace provider configuration decision")))
    );
    let mut envelope = serde_json::to_value(
        processor
            .load_turn_execution_authorization_context(&context.turn_id)
            .await
            .unwrap(),
    )
    .unwrap();
    envelope["grant_manifest"]["actions"]
        .as_array_mut()
        .unwrap()
        .retain(|a| a != "thread_observe_workspace");
    processor
        .crud_store
        .set_turn_execution_authorization_context(&context.turn_id, &envelope.to_string())
        .await
        .unwrap();
    let restricted = prepare_active_recall_preflight(
        &memory_context,
        &input,
        &MemoryTurnPolicy::normal_default_allow(),
        &MemoryActiveRecallConfig::default(),
        &HookPromptContextSet::default(),
        Some(&provider),
        true,
    )
    .await;
    assert!(
        !restricted
            .decision_request
            .available_episodic_modes
            .iter()
            .any(|m| m == "workspace_thread" || m == "related_thread")
    );
    runtime.run_phase(phase).await.unwrap(); // stale allowed plan is reauthorized
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    let mut missing = memory_context;
    missing.turn_id = "missing-execution".into();
    let absent = prepare_active_recall_preflight(
        &missing,
        &input,
        &MemoryTurnPolicy::normal_default_allow(),
        &MemoryActiveRecallConfig::default(),
        &HookPromptContextSet::default(),
        Some(&provider),
        true,
    )
    .await;
    assert!(!absent.decision_request.episodic_capabilities.any());
}
