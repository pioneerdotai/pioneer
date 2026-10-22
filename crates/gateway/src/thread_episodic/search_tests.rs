//! Included as a child of `search`, so authority fixtures cannot become production APIs.
use super::*;
use crate::thread_episodic::tests::{
    materialize_thread_with_item, ranked_hit_for_item, seed_active_thread_episodic_item,
    seed_thread_episodic_item_with_state, setup_thread_episodic_store,
};
use pioneer_memory::ThreadEpisodicSegmentRetrieval;
use pioneer_protocol::{PrincipalId, TurnItem};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

#[derive(Default)]
pub(crate) struct PagedCandidates {
    pub(crate) pages: StdMutex<BTreeMap<(String, usize), Vec<ThreadEpisodicRankedSearchHit>>>,
    generation: AtomicU64,
    only_segment: AtomicI64,
    unavailable: AtomicBool,
    expect_hybrid: AtomicBool,
}

#[async_trait]
impl ThreadEpisodicMemvidBackend for PagedCandidates {
    fn capabilities(&self) -> pioneer_memory::ThreadEpisodicMemvidBackendCapabilities {
        pioneer_memory::ThreadEpisodicMemvidBackendCapabilities::memvid_default()
    }

    async fn index_item(
        &self,
        _: ThreadEpisodicMemvidIndexRequest,
    ) -> std::result::Result<ThreadEpisodicMemvidIndexOutput, ThreadEpisodicMemvidError> {
        panic!("search must not index")
    }

    async fn search(
        &self,
        _: ThreadEpisodicMemvidSearchRequest,
    ) -> std::result::Result<ThreadEpisodicMemvidSearchOutput, ThreadEpisodicMemvidError> {
        panic!("shared retrieval must not fall back to bounded recall search")
    }

    async fn segment_generation(
        &self,
        _: ThreadEpisodicMemvidSearchSegment,
    ) -> std::result::Result<u64, ThreadEpisodicMemvidError> {
        Ok(self.generation.load(Ordering::SeqCst))
    }

    async fn retrieve_segment(
        &self,
        request: ThreadEpisodicMemvidSearchRequest,
        cursor: Option<String>,
        embedder: Option<Arc<ThreadEpisodicMemvidEmbedder>>,
    ) -> std::result::Result<ThreadEpisodicSegmentRetrieval, ThreadEpisodicMemvidError> {
        assert_eq!(
            embedder.is_some(),
            self.expect_hybrid.load(Ordering::SeqCst),
            "retrieval must follow workspace vector readiness"
        );
        if let Some(embedder) = &embedder {
            // Multiple segment/thread retrievals must reuse the prepared query.
            memvid_core::VecEmbedder::embed_query(embedder.as_ref(), &request.query).unwrap();
        }
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(ThreadEpisodicMemvidError::retryable(
                "injected unavailable index",
            ));
        }
        let only_segment = self.only_segment.load(Ordering::SeqCst);
        if only_segment != 0 && request.segments[0].segment_index != only_segment {
            return Ok(ThreadEpisodicSegmentRetrieval {
                hits: Vec::new(),
                next_cursor: None,
                approximate: false,
                incomplete: false,
            });
        }
        let offset = cursor.as_deref().unwrap_or("0").parse::<usize>().unwrap();
        let pages = self.pages.lock().unwrap();
        let hits = pages
            .get(&(request.thread_id.clone(), offset))
            .cloned()
            .unwrap_or_default();
        let next_cursor = pages
            .contains_key(&(request.thread_id, offset + 1))
            .then(|| (offset + 1).to_string());
        Ok(ThreadEpisodicSegmentRetrieval {
            hits,
            next_cursor,
            approximate: embedder.is_some(),
            incomplete: false,
        })
    }
}

async fn authority(store: &CrudStore, workspace_id: &str) -> EpisodicSearchAuthority {
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    if pioneer_entity::gateway_principal::Entity::find()
        .filter(pioneer_entity::gateway_principal::Column::Kind.eq("superuser"))
        .one(&store.database_connection())
        .await
        .unwrap()
        .is_none()
    {
        crate::identity::bootstrap_identity(store.database_connection())
            .await
            .unwrap();
    }
    let owner = pioneer_entity::gateway_principal::Entity::find()
        .filter(pioneer_entity::gateway_principal::Column::Kind.eq("superuser"))
        .one(&store.database_connection())
        .await
        .unwrap()
        .unwrap();
    EpisodicSearchAuthority {
        workspace_id: workspace_id.to_owned(),
        execution_thread_id: "T00000000000000000001".into(),
        turn_id: "fixture-recall".into(),
        principal: collaboration_authority_principal(store, &PrincipalId::new(owner.id).unwrap())
            .await
            .unwrap(),
        root_thread_id: "T00000000000000000001".into(),
        root_only: false,
        observe_children: true,
        agent: None,
        only_thread_id: None,
        fingerprint: format!("search-fixture:{workspace_id}"),
        policy_generation: crate::authorization::observed_policy_generation(),
    }
}

pub(crate) async fn source(
    store: &CrudStore,
    workspace: &str,
    thread: &str,
    item: &str,
    text: &str,
) -> ThreadEpisodicItemRecord {
    let turn = format!("turn-{item}");
    materialize_thread_with_item(
        store,
        workspace,
        thread,
        &turn,
        TurnItem::UserMessage {
            id: item.to_owned(),
            text: text.to_owned(),
            attachments: Vec::new(),
        },
        1_700_000_000,
    )
    .await;
    seed_active_thread_episodic_item(store, workspace, thread, &turn, item, text).await
}

async fn next_page(
    service: &ThreadEpisodicRecallService,
    authority: &EpisodicSearchAuthority,
    limit: usize,
    mut cursor: Option<String>,
) -> ThreadSearchPage {
    for _ in 0..100 {
        let page = service
            .search_threads(
                authority.clone(),
                "provider settings",
                limit,
                cursor.as_deref(),
            )
            .await
            .unwrap();
        if page.status != "work_budget" {
            return page;
        }
        assert!(
            page.threads.is_empty(),
            "provisional ranking must not be published"
        );
        assert!(page.incomplete);
        cursor = Some(page.next_cursor.expect("work budget is resumable"));
    }
    panic!("search failed to advance its frontier")
}

#[tokio::test]
async fn shared_search_obeys_vector_readiness_query_reuse_drift_and_degradation() {
    use std::sync::atomic::AtomicUsize;
    struct QueryProvider {
        calls: AtomicUsize,
        drift: AtomicBool,
    }
    impl ThreadEpisodicEmbeddingProvider for QueryProvider {
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
        ) -> std::result::Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
            panic!("shared search must use query preprocessing")
        }
        fn embed_query(
            &self,
            _: &str,
        ) -> std::result::Result<Vec<f32>, ThreadEpisodicEmbeddingError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(if self.drift.load(Ordering::SeqCst) {
                vec![0.0, 1.0, 0.0]
            } else {
                vec![1.0, 0.0, 0.0]
            })
        }
    }
    let (store, workspace) = setup_thread_episodic_store().await;
    let backend = Arc::new(PagedCandidates::default());
    for n in 1..=2 {
        let item = source(
            &store,
            &workspace,
            &format!("T{n:020}"),
            &format!("hybrid-{n}"),
            "provider settings",
        )
        .await;
        backend.pages.lock().unwrap().insert(
            (item.thread_id.clone(), 0),
            vec![ranked_hit_for_item(&item, "provider settings", 0.9)],
        );
    }
    let provider = Arc::new(QueryProvider {
        calls: AtomicUsize::new(0),
        drift: AtomicBool::new(false),
    });
    let resolver = Arc::new(SharedThreadEpisodicIndexEmbeddingProviderResolver::new());
    resolver.set_active_provider(Some(provider.clone()));
    let service = ThreadEpisodicRecallService::with_embedding_provider_resolver(
        store.clone(),
        backend.clone(),
        Some(resolver.clone()),
    );
    let authority = authority(&store, &workspace).await;
    let lexical = service
        .search_threads(authority.clone(), "provider settings", 1, None)
        .await
        .unwrap();
    assert!(!lexical.approximate);
    assert_eq!(provider.calls.load(Ordering::SeqCst), 0);

    let vector = GatewayThreadEpisodicVectorSearchConfig {
        enabled: true,
        provider: Some(pioneer_config::GatewayThreadEpisodicVectorProviderConfig::OpenRouter),
        model: Some("custom/test-embedding".into()),
        ..Default::default()
    };
    crate::thread_episodic::tests::mark_thread_episodic_workspace_vector_refill_complete_for_test(
        &store, &workspace, &vector,
    )
    .await;
    service.apply_config(ThreadEpisodicRecallServiceConfig {
        vector_search_enabled: true,
        vector_search: vector,
        ..Default::default()
    });
    backend.expect_hybrid.store(true, Ordering::SeqCst);
    let hybrid = service
        .search_threads(authority.clone(), "provider settings", 1, None)
        .await
        .unwrap();
    assert!(hybrid.approximate && !hybrid.incomplete);
    assert_eq!(
        provider.calls.load(Ordering::SeqCst),
        1,
        "both thread retrievals must reuse one query vector"
    );
    provider.drift.store(true, Ordering::SeqCst);
    assert!(
        service
            .search_threads(
                authority.clone(),
                "provider settings",
                1,
                hybrid.next_cursor.as_deref()
            )
            .await
            .is_err(),
        "provider drift must invalidate continuation instead of changing ordering"
    );

    resolver.set_active_provider(None);
    backend.expect_hybrid.store(false, Ordering::SeqCst);
    let degraded = service
        .search_threads(authority, "provider settings", 1, None)
        .await
        .unwrap();
    assert_eq!(degraded.status, "partial");
    assert!(degraded.incomplete && !degraded.threads.is_empty());
}

#[tokio::test]
async fn search_continues_backend_pages_groups_threads_and_preserves_independent_equal_text() {
    let (store, workspace) = setup_thread_episodic_store().await;
    let backend = Arc::new(PagedCandidates::default());
    let a = source(
        &store,
        &workspace,
        "T00000000000000000001",
        "a",
        "same provider settings",
    )
    .await;
    let a2 = source(
        &store,
        &workspace,
        &a.thread_id,
        "a2",
        "provider settings detail",
    )
    .await;
    let b = source(
        &store,
        &workspace,
        "T00000000000000000002",
        "b",
        "same provider settings",
    )
    .await;
    let mut private_hidden = store.get_thread_model(&b.thread_id).await.unwrap().unwrap();
    private_hidden.visibility = Some(pioneer_protocol::ThreadVisibility::Private);
    private_hidden.sidebar_visibility = pioneer_protocol::ThreadSidebarVisibility::Hidden;
    store
        .upsert_thread_model(&private_hidden, pioneer_protocol::PersistedActorRef::System)
        .await
        .unwrap();
    backend.pages.lock().unwrap().extend([
        (
            (a.thread_id.clone(), 0),
            vec![ranked_hit_for_item(&a, "untrusted index excerpt", 0.4)],
        ),
        (
            (a.thread_id.clone(), 1),
            vec![ranked_hit_for_item(&a2, "untrusted index excerpt", 0.9)],
        ),
        (
            (b.thread_id.clone(), 0),
            vec![ranked_hit_for_item(&b, "untrusted index excerpt", 0.8)],
        ),
    ]);
    let service = ThreadEpisodicRecallService::new(store.clone(), backend);
    let scope = authority(&store, &workspace).await;
    let first = next_page(&service, &scope, 1, None).await;
    assert_eq!(first.threads.len(), 1);
    assert_eq!(first.threads[0].thread_id, a.thread_id);
    assert_eq!(first.threads[0].fragments.len(), 2);
    assert!(
        first.threads[0]
            .fragments
            .iter()
            .any(|f| f.item_id == "a2" && f.text == "provider settings detail")
    );
    let second = next_page(&service, &scope, 1, first.next_cursor).await;
    assert_eq!(second.threads.len(), 1);
    assert_eq!(second.threads[0].thread_id, b.thread_id);
    assert_eq!(
        second.threads[0].fragments[0].text,
        "same provider settings"
    );
    assert!(second.next_cursor.is_none());
    assert_eq!(second.status, "exhausted");
}

#[tokio::test]
async fn search_work_budget_is_resumable_and_backend_failure_is_not_eof() {
    let (store, workspace) = setup_thread_episodic_store().await;
    let backend = Arc::new(PagedCandidates::default());
    let item = source(
        &store,
        &workspace,
        "T00000000000000000001",
        "a",
        "provider settings",
    )
    .await;
    // More retrieval pages than one invocation can process. Only the last page
    // contains a usable hit, so a fixed candidate window cannot satisfy this test.
    for page in 0..40 {
        backend.pages.lock().unwrap().insert(
            (item.thread_id.clone(), page),
            if page == 39 {
                vec![ranked_hit_for_item(&item, "index text", 0.8)]
            } else {
                Vec::new()
            },
        );
    }
    let service = ThreadEpisodicRecallService::new(store.clone(), backend.clone());
    let scope = authority(&store, &workspace).await;
    let first = service
        .search_threads(scope.clone(), "provider settings", 1, None)
        .await
        .unwrap();
    assert_eq!(first.status, "work_budget");
    assert!(first.threads.is_empty());
    backend.unavailable.store(true, Ordering::SeqCst);
    let failed = service
        .search_threads(
            scope.clone(),
            "provider settings",
            1,
            first.next_cursor.as_deref(),
        )
        .await
        .unwrap();
    assert_eq!(failed.status, "index_unavailable");
    assert!(failed.incomplete && failed.next_cursor.is_some());
    backend.unavailable.store(false, Ordering::SeqCst);
    let complete = next_page(&service, &scope, 1, failed.next_cursor).await;
    assert_eq!(complete.threads[0].thread_id, item.thread_id);
    assert!(complete.next_cursor.is_none());
}

#[tokio::test]
async fn search_cursor_rejects_changed_query_authority_and_index_generation() {
    let (store, workspace) = setup_thread_episodic_store().await;
    let backend = Arc::new(PagedCandidates::default());
    for n in 1..=2 {
        let item = source(
            &store,
            &workspace,
            &format!("T{n:020}"),
            &format!("item-{n}"),
            "provider settings",
        )
        .await;
        backend.pages.lock().unwrap().insert(
            (item.thread_id.clone(), 0),
            vec![ranked_hit_for_item(&item, "index text", 1.0 / n as f32)],
        );
    }
    let service = ThreadEpisodicRecallService::new(store.clone(), backend.clone());
    let scope = authority(&store, &workspace).await;
    let cursor = next_page(&service, &scope, 1, None)
        .await
        .next_cursor
        .unwrap();
    assert!(
        service
            .search_threads(scope.clone(), "different query", 1, Some(&cursor))
            .await
            .is_err()
    );
    let mut other = scope.clone();
    other.fingerprint = "different initiating execution".into();
    assert!(
        service
            .search_threads(other, "provider settings", 1, Some(&cursor))
            .await
            .is_err()
    );
    backend.generation.fetch_add(1, Ordering::SeqCst);
    assert!(
        service
            .search_threads(scope, "provider settings", 1, Some(&cursor))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn shared_recall_keeps_current_and_other_sources_and_respects_prompt_budget() {
    let (store, workspace) = setup_thread_episodic_store().await;
    let backend = Arc::new(PagedCandidates::default());
    for n in 1..=2 {
        let item = source(
            &store,
            &workspace,
            &format!("T{n:020}"),
            &format!("item-{n}"),
            "provider settings use the configured model",
        )
        .await;
        backend.pages.lock().unwrap().insert(
            (item.thread_id.clone(), 0),
            vec![ranked_hit_for_item(&item, "index text", 0.9)],
        );
    }
    let service = ThreadEpisodicRecallService::new(store.clone(), backend);
    let scope = authority(&store, &workspace).await;
    let recall = service
        .recall_authorized_workspace(scope.clone(), "provider settings", 8, 1000)
        .await
        .unwrap();
    let threads: BTreeSet<_> = recall
        .hits
        .iter()
        .map(|hit| hit.provenance.thread_id.0.as_str())
        .collect();
    assert_eq!(
        threads,
        BTreeSet::from(["T00000000000000000001", "T00000000000000000002"])
    );
    assert!(
        recall
            .hits
            .iter()
            .all(|hit| hit.provenance.item_id.0.starts_with("item-"))
    );
    let empty_query = service
        .recall_authorized_workspace(scope.clone(), "  ", 8, 1000)
        .await
        .unwrap();
    assert!(empty_query.hits.is_empty());
    assert!(
        empty_query.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == ThreadEpisodicRecallDiagnosticCode::InvalidInput
        })
    );
    let bounded = service
        .recall_authorized_workspace(scope, "provider settings", 8, 45)
        .await
        .unwrap();
    assert!(
        bounded
            .hits
            .iter()
            .map(|hit| hit.text.chars().count())
            .sum::<usize>()
            <= 45
    );
    assert!(
        bounded
            .diagnostics
            .iter()
            .any(|d| d.code == ThreadEpisodicRecallDiagnosticCode::PromptBudgetExceeded)
    );
}

#[tokio::test]
async fn invalid_sources_do_not_hide_a_valid_hit_on_a_later_backend_page() {
    let (store, workspace) = setup_thread_episodic_store().await;
    let backend = Arc::new(PagedCandidates::default());
    let thread = "T00000000000000000001";
    let deleted = source(
        &store,
        &workspace,
        thread,
        "deleted",
        "provider settings deleted",
    )
    .await;
    let excluded = source(
        &store,
        &workspace,
        thread,
        "excluded",
        "provider settings excluded",
    )
    .await;
    let stale = source(
        &store,
        &workspace,
        thread,
        "stale",
        "canonical new provider settings",
    )
    .await;
    let stale = seed_active_thread_episodic_item(
        &store,
        &workspace,
        thread,
        &stale.turn_id,
        "stale",
        "obsolete source text",
    )
    .await;
    // The source never existed: active index metadata is not sufficient proof.
    let missing = seed_thread_episodic_item_with_state(
        &store,
        &workspace,
        thread,
        "missing-turn",
        "missing",
        "obsolete private index text",
        ThreadEpisodicItemStatus::Active,
        ThreadEpisodicItemVisibility::UserVisible,
    )
    .await;
    let valid = source(
        &store,
        &workspace,
        thread,
        "valid",
        "provider settings valid",
    )
    .await;
    store
        .tombstone_thread_episodic_items_for_item(
            &workspace,
            thread,
            &deleted.turn_id,
            "deleted",
            1_700_000_500,
        )
        .await
        .unwrap();
    store
        .exclude_thread_episodic_item(
            NewThreadEpisodicExclusionRecord {
                id: None,
                workspace_id: workspace.clone(),
                thread_id: thread.to_owned(),
                index_item_id: excluded.id.clone(),
                reason: ThreadEpisodicExclusionReason::UserRequested,
                created_by: "search-test".to_owned(),
            },
            1_700_000_500,
        )
        .await
        .unwrap();
    backend.pages.lock().unwrap().extend([
        (
            (thread.to_owned(), 0),
            [&deleted, &excluded, &stale, &missing]
                .into_iter()
                .map(|item| ranked_hit_for_item(item, "forbidden fallback text", 1.0))
                .collect(),
        ),
        (
            (thread.to_owned(), 1),
            vec![ranked_hit_for_item(&valid, "index text", 0.5)],
        ),
    ]);
    let service = ThreadEpisodicRecallService::new(store.clone(), backend);
    let page = next_page(&service, &authority(&store, &workspace).await, 1, None).await;
    assert_eq!(page.threads.len(), 1);
    assert_eq!(page.threads[0].fragments.len(), 1);
    assert_eq!(page.threads[0].fragments[0].item_id, "valid");
    assert_eq!(page.threads[0].fragments[0].text, "provider settings valid");
    assert!(
        !serde_json::to_string(&page)
            .unwrap()
            .contains("forbidden fallback")
    );
}

#[tokio::test]
async fn logically_deleted_turn_rejects_fresh_recall_and_saved_cursor_sources() {
    use pioneer_protocol::{
        ItemCompletedNotification, PersistedActorRef, SandboxMode, ThreadMode, Turn, TurnKind,
        TurnOrigin, TurnStatus,
    };
    let (store, workspace) = setup_thread_episodic_store().await;
    let thread_id = "T00000000000000000001";
    // Materialize the root first, then create a user-authored Message turn so
    // deletion goes through the real revision/author/mode checks.
    source(
        &store,
        &workspace,
        thread_id,
        "unrelated",
        "unrelated content",
    )
    .await;
    let scope = authority(&store, &workspace).await;
    let mut thread = store.get_thread_model(thread_id).await.unwrap().unwrap();
    thread.mode = ThreadMode::Message;
    let turn = Turn {
        id: "turn-logical-delete".into(),
        status: TurnStatus::Completed,
        turn_kind: TurnKind::Conversation,
        origin: TurnOrigin::User,
        mode: ThreadMode::Message,
        author: None,
        reply_to_turn_id: None,
        mentions: Vec::new(),
        message_revision: 0,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: pioneer_protocol::default_turn_permission_profile_snapshot(),
    };
    let actor = PersistedActorRef::Principal(scope.principal.principal_id.clone());
    store
        .materialize_turn_start(&thread, SandboxMode::FullAccess, &turn, &[], actor.clone())
        .await
        .unwrap();
    let item_id = "logical-delete-item";
    let text = "provider settings logically deleted secret discussion";
    store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: workspace.clone(),
                thread_id: thread_id.into(),
                turn_id: turn.id.clone(),
                item: TurnItem::UserMessage {
                    id: item_id.into(),
                    text: text.into(),
                    attachments: Vec::new(),
                },
            },
            1_700_000_001,
        )
        .await
        .unwrap();
    let item =
        seed_active_thread_episodic_item(&store, &workspace, thread_id, &turn.id, item_id, text)
            .await;
    let backend = Arc::new(PagedCandidates::default());
    backend.pages.lock().unwrap().insert(
        (thread_id.into(), 0),
        vec![ranked_hit_for_item(&item, text, 0.9)],
    );
    let other = source(
        &store,
        &workspace,
        "T00000000000000000002",
        "retained-source",
        "provider settings still readable",
    )
    .await;
    backend.pages.lock().unwrap().insert(
        (other.thread_id.clone(), 0),
        vec![ranked_hit_for_item(&other, "provider settings", 0.8)],
    );
    let service = ThreadEpisodicRecallService::new(store.clone(), backend);
    let page = next_page(&service, &scope, 1, None).await;
    assert_eq!(page.threads[0].fragments[0].item_id, item_id);
    let saved_cursor = page
        .next_cursor
        .expect("a real continuation keeps the emitted source boundary");
    assert!(
        !service
            .recall_authorized_workspace(scope.clone(), "provider settings", 8, 2000)
            .await
            .unwrap()
            .hits
            .is_empty()
    );
    store
        .delete_turn_message(pioneer_crud::DeleteTurnMessageRequest {
            workspace_id: workspace.clone(),
            thread_id: thread_id.into(),
            turn_id: turn.id.clone(),
            expected_revision: 0,
            changed_by: actor,
            changed_at_unix: 1_700_000_500,
        })
        .await
        .unwrap();
    assert!(
        store
            .get_turn_item(&turn.id, item_id)
            .await
            .unwrap()
            .is_some(),
        "logical delete retains physical source"
    );
    assert!(
        matches!(
            store
                .find_thread_episodic_item(&item.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            ThreadEpisodicItemStatus::Active
        ),
        "this is not an episodic tombstone test"
    );
    let rejected = service
        .validate_search_source(&workspace, thread_id, &item.id)
        .await
        .expect_err("physical item and active projection cannot override logical deletion");
    assert!(rejected.contains("canonical turn unavailable"));
    assert!(
        service
            .search_threads(scope.clone(), "provider settings", 1, Some(&saved_cursor))
            .await
            .is_err(),
        "the real saved cursor must reject its logically deleted canonical boundary"
    );
    let page = next_page(&service, &scope, 1, None).await;
    assert_eq!(page.threads.len(), 1);
    assert_eq!(page.threads[0].thread_id, other.thread_id);
    assert!(!serde_json::to_string(&page).unwrap().contains(text));
    let recalled = service
        .recall_authorized_workspace(scope, "provider settings", 8, 2000)
        .await
        .unwrap();
    assert!(
        recalled
            .hits
            .iter()
            .all(|hit| hit.provenance.item_id.0 != item_id)
    );
    assert!(
        recalled
            .hits
            .iter()
            .any(|hit| hit.provenance.item_id.0 == other.item_id)
    );
}

#[tokio::test]
async fn missing_durable_initiator_does_not_produce_workspace_authority() {
    let (store, workspace) = setup_thread_episodic_store().await;
    assert!(
        EpisodicSearchAuthority::resolve(
            &store,
            &workspace,
            "T00000000000000000001",
            "unknown-turn",
            None
        )
        .await
        .is_err()
    );
}

#[tokio::test]
async fn execution_root_ceiling_excludes_other_readable_threads() {
    let (store, workspace) = setup_thread_episodic_store().await;
    let backend = Arc::new(PagedCandidates::default());
    for n in 1..=2 {
        let item = source(
            &store,
            &workspace,
            &format!("T{n:020}"),
            &format!("item-{n}"),
            "provider settings",
        )
        .await;
        backend.pages.lock().unwrap().insert(
            (item.thread_id.clone(), 0),
            vec![ranked_hit_for_item(&item, "index text", n as f32)],
        );
    }
    let service = ThreadEpisodicRecallService::new(store.clone(), backend);
    let mut restricted = authority(&store, &workspace).await;
    restricted.root_only = true;
    let page = next_page(&service, &restricted, 10, None).await;
    assert_eq!(page.threads.len(), 1);
    assert_eq!(page.threads[0].thread_id, "T00000000000000000001");
    assert!(page.next_cursor.is_none());
    assert!(
        !serde_json::to_string(&page)
            .unwrap()
            .contains("T00000000000000000002")
    );
}

#[tokio::test]
async fn search_visits_later_workspace_capsules() {
    let (store, workspace) = setup_thread_episodic_store().await;
    let backend = Arc::new(PagedCandidates::default());
    let first = source(
        &store,
        &workspace,
        "T00000000000000000001",
        "first",
        "earlier unrelated content",
    )
    .await;
    store
        .transition_thread_episodic_active_write_segment(
            first.capsule_id.as_deref().unwrap(),
            ThreadEpisodicCapsuleWriteState::ReadOnly,
            1_700_000_100,
        )
        .await
        .unwrap()
        .unwrap();
    let later = source(
        &store,
        &workspace,
        "T00000000000000000002",
        "later",
        "provider settings in later capsule",
    )
    .await;
    assert!(later.segment_index > first.segment_index);
    backend
        .only_segment
        .store(later.segment_index.unwrap(), Ordering::SeqCst);
    backend.pages.lock().unwrap().insert(
        (later.thread_id.clone(), 0),
        vec![ranked_hit_for_item(&later, "index text", 0.9)],
    );
    let service = ThreadEpisodicRecallService::new(store.clone(), backend);
    let page = next_page(&service, &authority(&store, &workspace).await, 10, None).await;
    assert_eq!(page.threads.len(), 1);
    assert_eq!(page.threads[0].thread_id, later.thread_id);
    assert_eq!(page.threads[0].fragments[0].item_id, "later");
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn private_and_internal_sources_follow_exact_member_acl_and_revocation() {
    use crate::tests::authorization::{
        IsolatedEpic4Harness, MEMBER_A_ID, MEMBER_B_ID, THREAD_RED_INTERNAL_ID,
        THREAD_RED_PRIVATE_A_ID, THREAD_RED_PRIVATE_B_ID, WORKSPACE_RED_ID,
    };
    use sea_orm::ConnectionTrait;
    let harness = IsolatedEpic4Harness::new().await.unwrap();
    let store = Arc::new(CrudStore::new(harness.database.clone()));
    crate::thread_episodic::tests::mark_thread_episodic_workspace_refill_complete_for_test(
        &store,
        WORKSPACE_RED_ID,
    )
    .await;
    let backend = Arc::new(PagedCandidates::default());
    for (thread, item, score) in [
        (THREAD_RED_PRIVATE_A_ID, "a", 0.9),
        (THREAD_RED_PRIVATE_B_ID, "b", 5.0),
        (THREAD_RED_INTERNAL_ID, "child", 0.8),
    ] {
        let item = source(
            &store,
            WORKSPACE_RED_ID,
            thread,
            item,
            "provider settings for exact member",
        )
        .await;
        backend.pages.lock().unwrap().insert(
            (thread.to_owned(), 0),
            vec![ranked_hit_for_item(&item, "index", score)],
        );
    }
    // Sidebar placement never grants or removes source read access. Restore
    // the fixture's internal/root relationship after generic source materialization.
    store.database_connection().execute_unprepared(
        "UPDATE thread SET access_class='internal',sidebar_visibility='hidden',origin_kind='task_run' WHERE id='T00000000000000000004';
         UPDATE thread SET sidebar_visibility='hidden',access_class='private' WHERE id='T00000000000000000001';"
    ).await.unwrap();
    let mut a = authority(&store, WORKSPACE_RED_ID).await;
    a.principal =
        collaboration_authority_principal(&store, &PrincipalId::new(MEMBER_A_ID).unwrap())
            .await
            .unwrap();
    a.fingerprint = format!("member-a:{}", a.fingerprint);
    let parent_turn = admitted_turn(&store, &a).await;
    {
        use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
        pioneer_entity::thread_lineage::Entity::update_many()
            .col_expr(
                pioneer_entity::thread_lineage::Column::CreatedByTurnId,
                sea_orm::sea_query::Expr::value(parent_turn),
            )
            .filter(
                pioneer_entity::thread_lineage::Column::ChildThreadId.eq(THREAD_RED_INTERNAL_ID),
            )
            .exec(&store.database_connection())
            .await
            .unwrap();
    }

    let mut b = a.clone();
    b.principal =
        collaboration_authority_principal(&store, &PrincipalId::new(MEMBER_B_ID).unwrap())
            .await
            .unwrap();
    b.fingerprint = "member-b".to_owned();
    let service = ThreadEpisodicRecallService::new(store.clone(), backend);
    let first = next_page(&service, &a, 1, None).await;
    assert_eq!(first.threads[0].thread_id, THREAD_RED_PRIVATE_A_ID);
    assert!(first.next_cursor.is_some());
    let for_b = next_page(&service, &b, 10, None).await;
    assert_eq!(
        for_b
            .threads
            .iter()
            .map(|c| c.thread_id.as_str())
            .collect::<Vec<_>>(),
        vec![THREAD_RED_PRIVATE_B_ID]
    );
    assert!(
        !serde_json::to_string(&first)
            .unwrap()
            .contains(THREAD_RED_PRIVATE_B_ID)
    );
    assert!(
        service
            .search_threads(b, "provider settings", 1, first.next_cursor.as_deref())
            .await
            .is_err()
    );
    // Revoke A's private root membership without touching the index. The next
    // page cannot disclose either its root or internal child's source.
    store.database_connection().execute_unprepared(
        "DELETE FROM thread_membership WHERE principal_id='P0000000000000000000A' AND thread_id='T00000000000000000001'"
    ).await.unwrap();
    assert!(
        service
            .search_threads(a, "provider settings", 1, first.next_cursor.as_deref())
            .await
            .is_err()
    );
}

async fn admitted_turn(store: &CrudStore, authority: &EpisodicSearchAuthority) -> String {
    use pioneer_protocol::{
        PersistedActorRef, SandboxMode, Turn, TurnKind, TurnOrigin, TurnStatus,
    };
    use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};
    let thread = store
        .get_thread_model(&authority.root_thread_id)
        .await
        .unwrap()
        .unwrap();
    let turn = Turn {
        id: "U00000000000000000001".to_owned(),
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
    store
        .materialize_turn_start(
            &thread,
            SandboxMode::FullAccess,
            &turn,
            &[],
            PersistedActorRef::Principal(authority.principal.principal_id.clone()),
        )
        .await
        .unwrap();
    let execution = ExecutionAuthorizationContext::for_test(
        &authority.principal,
        &authority.workspace_id,
        &authority.root_thread_id,
        &turn.permission_profile,
        None,
    );
    pioneer_entity::turn::Entity::update_many()
        .col_expr(
            pioneer_entity::turn::Column::ExecutionAuthorizationContextJson,
            sea_orm::sea_query::Expr::value(execution.to_persisted_json().unwrap()),
        )
        .filter(pioneer_entity::turn::Column::Id.eq(&turn.id))
        .exec(&store.database_connection())
        .await
        .unwrap();
    turn.id
}

#[tokio::test]
async fn denied_discovery_frontier_is_not_disclosed_in_cursor_payload() {
    let (store, workspace) = setup_thread_episodic_store().await;
    let backend = Arc::new(PagedCandidates::default());
    for n in 1..=40 {
        let item = source(
            &store,
            &workspace,
            &format!("T{n:020}"),
            &format!("source-{n}"),
            "provider settings",
        )
        .await;
        backend.pages.lock().unwrap().insert(
            (item.thread_id.clone(), 0),
            vec![ranked_hit_for_item(&item, "provider settings", 0.9)],
        );
    }
    let mut scope = authority(&store, &workspace).await;
    scope.root_only = true;
    let service = ThreadEpisodicRecallService::new(store.clone(), backend);
    let first = service
        .search_threads(scope.clone(), "provider settings", 10, None)
        .await
        .unwrap();
    assert_eq!(first.status, "work_budget");
    let encoded = first.next_cursor.as_ref().unwrap();
    let envelope = URL_SAFE_NO_PAD.decode(encoded).unwrap();
    let payload = String::from_utf8(envelope[..envelope.len() - 32].to_vec()).unwrap();
    for n in 2..=40 {
        assert!(!payload.contains(&format!("T{n:020}")));
    }
    let complete = next_page(&service, &scope, 10, first.next_cursor).await;
    assert_eq!(complete.threads.len(), 1);
    assert_eq!(complete.threads[0].thread_id, scope.root_thread_id);
}
