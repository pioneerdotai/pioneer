//! Production admission, persisted Agent ceiling and both observation surfaces.
//! No runtime/model process is started by these fixtures.
use super::*;
use crate::authorization::{
    AgentAuthorizationFacts, ExecutionAdmissionEntryPoint, ExecutionAdmissionRequest,
    ExecutionAdmissionService, ExecutionAuthorizationContext, ResourceAction,
    RoleDefinitionRegistry, collaboration_authority_principal,
    materialize_persisted_task_agent_action_binding,
};
use crate::tests::authorization::{
    IsolatedEpic4Harness, MEMBER_A_ID, THREAD_RED_INTERNAL_ID, THREAD_RED_PRIVATE_A_ID,
    THREAD_RED_PRIVATE_B_ID, THREAD_RED_WORKSPACE_ID, WORKSPACE_RED_ID,
};
use crate::thread_episodic::{
    EpisodicSearchAuthority, SearchTestBackend, ThreadEpisodicRecallService,
    WorkspaceEpisodicRecallService, mark_search_test_ready, rank_search_test_hit,
    seed_search_test_source,
};
use pioneer_agent::TurnToolContext;
use pioneer_memory::hooks::{
    AgentEpisodicRecallProvider, MemoryEpisodicRecallSourceKind, MemoryWorkspaceThreadRecallRequest,
};
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

async fn fixture() -> (Arc<MessageProcessor>, Arc<SearchTestBackend>) {
    let harness = IsolatedEpic4Harness::new().await.unwrap();
    let store = Arc::new(CrudStore::new(harness.database.clone()));
    let backend = Arc::new(SearchTestBackend::default());
    for (thread, item, score) in [
        (THREAD_RED_PRIVATE_A_ID, "root-a-source", 0.9),
        (THREAD_RED_WORKSPACE_ID, "root-b-source", 0.8),
        (THREAD_RED_PRIVATE_B_ID, "denied-source", 5.0),
        (THREAD_RED_INTERNAL_ID, "internal-source", 1.0),
    ] {
        let source = seed_search_test_source(
            &store,
            WORKSPACE_RED_ID,
            thread,
            item,
            "provider settings discussed here",
        )
        .await;
        let hit = rank_search_test_hit(&source, "provider settings", score);
        backend
            .pages
            .lock()
            .unwrap()
            .insert((thread.into(), 0), vec![hit]);
        if thread == THREAD_RED_WORKSPACE_ID {
            let summary_id = "canonical-task-summary";
            let notification = pioneer_protocol::ItemCompletedNotification {
                workspace_id: WORKSPACE_RED_ID.into(),
                thread_id: thread.into(),
                turn_id: source.turn_id.clone(),
                item: TurnItem::Task {
                    item: TaskTurnItem {
                        id: summary_id.into(),
                        task_id: "summary-task".into(),
                        created_by_turn_id: None,
                        run_id: None,
                        parent_task_id: None,
                        root_task_id: None,
                        title: "Provider settings".into(),
                        status: TaskStatus::Completed,
                        attachment: pioneer_protocol::TaskAttachmentMode::Attached,
                        trigger_kind: TaskTriggerKind::Immediate,
                        executor_kind: TaskExecutorKind::Agent,
                        child_thread_id: None,
                        child_turn_id: None,
                        agent_role: None,
                        depth: 0,
                        max_depth: 3,
                        next_fire_at: None,
                        progress_preview: None,
                        result_preview: Some("provider settings summary result".into()),
                        error_preview: None,
                        started_at: Some(1_700_000_000),
                        created_at: 1_700_000_000,
                        updated_at: 1_700_000_001,
                    },
                },
            };
            store
                .materialize_item_completed(notification.clone(), 1_700_000_001)
                .await
                .unwrap();
            let input =
                pioneer_crud::thread_episodic_source::committed_item_ingestion_input(&notification)
                    .unwrap();
            StoreThreadEpisodicIngestor::new(store.clone())
                .ingest_committed_item(input)
                .await
                .unwrap();
            let pending = store
                .list_thread_episodic_items_for_thread(WORKSPACE_RED_ID, thread, 16)
                .await
                .unwrap()
                .into_iter()
                .find(|item| item.item_id == summary_id)
                .unwrap();
            let uri = pioneer_crud::thread_episodic_item_uri(
                WORKSPACE_RED_ID,
                thread,
                &source.turn_id,
                summary_id,
                &pending.id,
            )
            .unwrap();
            let indexed = store
                .mark_thread_episodic_item_indexed(
                    &pending.id,
                    pioneer_crud::ThreadEpisodicItemIndexedUpdate {
                        capsule_id: source.capsule_id.clone().unwrap(),
                        capsule_ref: source.capsule_ref.clone().unwrap(),
                        segment_index: source.segment_index.unwrap(),
                        frame_id: 43,
                        frame_uri: uri,
                        embedding_artifact_id: None,
                    },
                    1_700_000_002,
                )
                .await
                .unwrap()
                .unwrap();
            backend
                .pages
                .lock()
                .unwrap()
                .get_mut(&(thread.into(), 0))
                .unwrap()
                .push(rank_search_test_hit(&indexed, "provider settings", 0.85));
        }
    }
    // Generic source materialization carries its own Thread projection. Retain
    // the access classes of this authorization fixture explicitly.
    for (thread, access) in [
        (THREAD_RED_PRIVATE_A_ID, "private"),
        (THREAD_RED_PRIVATE_B_ID, "private"),
        (THREAD_RED_WORKSPACE_ID, "workspace"),
        (THREAD_RED_INTERNAL_ID, "internal"),
    ] {
        pioneer_entity::thread::Entity::update_many()
            .col_expr(
                pioneer_entity::thread::Column::AccessClass,
                sea_orm::sea_query::Expr::value(access.to_owned()),
            )
            .filter(pioneer_entity::thread::Column::Id.eq(thread))
            .exec(&store.database_connection())
            .await
            .unwrap();
    }
    mark_search_test_ready(&store, WORKSPACE_RED_ID).await;
    let mut processor = MessageProcessor::new(
        Arc::new(ThreadManager::new("model-a", "openai")),
        test_provider(),
        Arc::new(SessionManager::new()),
        Arc::new(WorkspaceManager::new(harness.database)),
        store.clone(),
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    );
    processor.thread_episodic_recall_service =
        Arc::new(ThreadEpisodicRecallService::new(store, backend.clone()));
    (Arc::new(processor), backend)
}

/// Uses real user admission, canonical Agent graph writer and the same persisted
/// binding factory as native/CLI recovery. Role narrowing is fixed before commit.
async fn admit(
    processor: &Arc<MessageProcessor>,
    turn_id: &str,
    role: &str,
    observe_workspace: bool,
) -> TurnToolContext {
    let store = &processor.crud_store;
    let principal =
        collaboration_authority_principal(store, &PrincipalId::new(MEMBER_A_ID).unwrap())
            .await
            .unwrap();
    let generation = std::cmp::max(processor.current_authorization_revision().await.unwrap(), 1);
    let request = ExecutionAdmissionRequest {
        entry_point: ExecutionAdmissionEntryPoint::InteractiveTurn,
        required_root_action: ResourceAction::AgentTurnStart,
        additional_required_actions: Vec::new(),
        workspace_id: WORKSPACE_RED_ID.into(),
        root_thread_id: THREAD_RED_PRIVATE_A_ID.into(),
        provider: "openai".into(),
        model: "model-a".into(),
        execution_backend: None,
        capabilities: Vec::new(),
        artifacts: Vec::new(),
        has_local_attachment_sources: false,
        has_url_attachment_sources: false,
        provider_authority_fingerprint: Some(
            processor
                .provider_registry
                .authority_fingerprint_for_workspace(WORKSPACE_RED_ID, "openai")
                .expect("test provider authority should resolve")
                .as_str()
                .to_owned(),
        ),
    };
    let envelope = ExecutionAdmissionService::new(store.as_ref().clone())
        .admit_context(&principal, generation, &request, None)
        .await
        .unwrap();
    assert!(envelope.grants_action(ResourceAction::ThreadObserveWorkspace));
    // Model a historical/root-only immutable ceiling, before it is stored.
    let mut serialized: serde_json::Value =
        serde_json::from_str(&envelope.to_persisted_json().unwrap()).unwrap();
    if !observe_workspace {
        serialized["grant_manifest"]["actions"]
            .as_array_mut()
            .unwrap()
            .retain(|action| action != "thread_observe_workspace");
    }
    let envelope =
        ExecutionAuthorizationContext::from_persisted_json(&serialized.to_string()).unwrap();
    let mut thread = store
        .get_thread_model(THREAD_RED_PRIVATE_A_ID)
        .await
        .unwrap()
        .unwrap();
    thread.model_provider = "openai".into();
    thread.model = "model-a".into();
    let turn = Turn {
        id: turn_id.into(),
        status: TurnStatus::InProgress,
        turn_kind: TurnKind::Conversation,
        origin: TurnOrigin::User,
        mode: ThreadMode::Agent,
        author: None,
        reply_to_turn_id: None,
        mentions: Vec::new(),
        message_revision: 0,
        message_deleted: false,
        error: None,
        prompt_manifest: None,
        permission_profile: pioneer_protocol::task_permission_cap_snapshot(
            envelope.permission_profile_cap(),
        ),
    };
    store
        .materialize_turn_start(
            &thread,
            pioneer_protocol::SandboxMode::FullAccess,
            &turn,
            &[],
            pioneer_protocol::PersistedActorRef::Principal(principal.principal_id),
        )
        .await
        .unwrap();
    assert!(
        store
            .set_turn_execution_authorization_context(
                turn_id,
                &envelope.to_persisted_json().unwrap()
            )
            .await
            .unwrap()
    );
    pioneer_crud::ensure_pioneer_for_workspace(
        &store.database_connection(),
        WORKSPACE_RED_ID,
        pioneer_crud::utc_now(),
    )
    .await
    .unwrap();
    let context = TurnToolContext {
        workspace_id: WORKSPACE_RED_ID.into(),
        thread_id: thread.id.clone(),
        turn_id: turn_id.into(),
    };
    let prepared = crate::message::agent_action_tools::prepare_root_agent_execution_admission(
        processor, &context, &thread, &envelope, None, None, None, None, None,
    )
    .await
    .unwrap();
    let mut graph = prepared.graph;
    let mut grant: serde_json::Value = serde_json::from_str(&graph.grant.grant_json).unwrap();
    let identity: pioneer_protocol::AgentIdentityProjection =
        serde_json::from_value(grant["identity"].clone()).unwrap();
    let profile: pioneer_protocol::AgentExecutionProfileProjection =
        serde_json::from_value(grant["profile"].clone()).unwrap();
    let agent = AgentAuthorizationFacts {
        identity_id: identity.id.clone(),
        identity_status: pioneer_protocol::AgentIdentityStatus::Active,
        role_key: role.into(),
        root_capsule_id: thread.id.clone(),
        parent_envelope: None,
        policy_generation: generation,
    }
    .derive_envelope(RoleDefinitionRegistry::new())
    .unwrap();
    grant["role_key"] = serde_json::json!(role);
    grant["allowed_actions"] = serde_json::json!(agent.allowed_action_names());
    grant["agent_authorization_fingerprint"] = serde_json::json!(agent.fingerprint);
    graph.grant.grant_json = grant.to_string();
    graph.grant.grant_fingerprint =
        pioneer_crud::agent_execution_grant_fingerprint(&graph.grant.grant_json).unwrap();
    let execution_id =
        pioneer_protocol::AgentExecutionId::new(graph.child_execution.id.clone()).unwrap();
    store.commit_agent_execution_graph(graph).await.unwrap();
    let (adapter, options, capabilities) = materialize_persisted_task_agent_action_binding(
        execution_id.clone(),
        &thread.id,
        execution_id,
        identity,
        profile,
        1,
        1,
        0,
        "search-observation",
        role,
        generation,
        generation,
        &agent.fingerprint,
        &agent.allowed_action_names(),
    )
    .unwrap();
    processor
        .register_agent_action_binding(
            turn_id,
            crate::message::agent_action_tools::AgentActionRuntimeBinding::new(
                adapter,
                options,
                capabilities,
            ),
        )
        .await;
    processor.register_execution_lease(turn_id).await.unwrap();
    // Internal canonical source access also requires the parent's durable grant.
    pioneer_entity::thread_lineage::Entity::update_many()
        .col_expr(
            pioneer_entity::thread_lineage::Column::CreatedByTurnId,
            sea_orm::sea_query::Expr::value(turn_id.to_owned()),
        )
        .filter(pioneer_entity::thread_lineage::Column::ChildThreadId.eq(THREAD_RED_INTERNAL_ID))
        .exec(&store.database_connection())
        .await
        .unwrap();
    context
}

fn params() -> pioneer_tools::threads_search::ThreadsSearchParams {
    pioneer_tools::threads_search::ThreadsSearchParams {
        query: "provider settings".into(),
        limit: 20,
        cursor: None,
    }
}

#[tokio::test]
async fn member_production_resolve_search_and_recall_cover_authorized_workspace_roots_only() {
    let (processor, _) = fixture().await;
    let context = admit(&processor, "workspace-search-turn", "thread_agent", true).await;
    let result = processor
        .search_threads_for_tool(&context, params())
        .await
        .unwrap();
    let ids: Vec<_> = result["threads"]
        .as_array()
        .unwrap()
        .iter()
        .map(|card| card["threadId"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&THREAD_RED_PRIVATE_A_ID));
    assert!(ids.contains(&THREAD_RED_WORKSPACE_ID));
    assert!(ids.contains(&THREAD_RED_INTERNAL_ID));
    assert!(!result.to_string().contains(THREAD_RED_PRIVATE_B_ID));
    assert!(
        result["threads"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|card| card["fragments"].as_array().unwrap())
            .any(|fragment| fragment["itemId"] == "canonical-task-summary"
                && fragment["sourceKind"] == "user_visible_task_summary")
    );
    // Every reference addresses an actual canonical occurrence; summaries use
    // the same item IDs instead of synthesized index-frame anchors.
    for card in result["threads"].as_array().unwrap() {
        for fragment in card["fragments"].as_array().unwrap() {
            assert!(
                processor
                    .crud_store
                    .get_thread_episodic_canonical_item(
                        fragment["workspaceId"].as_str().unwrap(),
                        fragment["threadId"].as_str().unwrap(),
                        fragment["turnId"].as_str().unwrap(),
                        fragment["itemId"].as_str().unwrap()
                    )
                    .await
                    .unwrap()
                    .is_some()
            );
        }
    }
    let provider =
        crate::thread_episodic_hooks::ThreadEpisodicMemoryRecallProvider::with_workspace_service(
            processor.thread_episodic_recall_service.clone(),
            Arc::new(WorkspaceEpisodicRecallService::new(
                processor.crud_store.clone(),
                processor.thread_episodic_recall_service.clone(),
            )),
            processor.crud_store.clone(),
        );
    let recalled = provider
        .recall_workspace_threads(MemoryWorkspaceThreadRecallRequest {
            workspace_id: context.workspace_id.clone(),
            current_thread_id: context.thread_id.clone(),
            turn_id: context.turn_id.clone(),
            principal_id: None,
            query: "provider settings".into(),
            targets: Vec::new(),
            top_k: 8,
            max_chars: 2400,
        })
        .await
        .unwrap();
    assert!(
        recalled
            .items
            .iter()
            .any(|hit| hit.provenance.source == MemoryEpisodicRecallSourceKind::CurrentThread)
    );
    assert!(
        recalled
            .items
            .iter()
            .any(
                |hit| hit.provenance.thread_id.as_deref() == Some(THREAD_RED_WORKSPACE_ID)
                    && hit.provenance.source == MemoryEpisodicRecallSourceKind::WorkspaceThread
            )
    );
    assert!(
        recalled
            .items
            .iter()
            .all(|hit| hit.provenance.thread_id.as_deref() != Some(THREAD_RED_PRIVATE_B_ID))
    );

    let old = admit(&processor, "legacy-search-turn", "thread_agent", false).await;
    let old_result = processor
        .search_threads_for_tool(&old, params())
        .await
        .unwrap();
    assert!(!old_result.to_string().contains(THREAD_RED_WORKSPACE_ID));
    let old_scope = EpisodicSearchAuthority::resolve_for_turn(
        &processor.crud_store,
        &old.workspace_id,
        &old.turn_id,
        None,
    )
    .await
    .unwrap();
    let old_recall = processor
        .thread_episodic_recall_service
        .recall_authorized_workspace(old_scope, "provider settings", 8, 2400)
        .await
        .unwrap();
    assert!(
        old_recall
            .hits
            .iter()
            .all(|hit| hit.provenance.thread_id.0 != THREAD_RED_WORKSPACE_ID)
    );
}

#[tokio::test]
async fn real_reviewer_binding_cannot_materialize_or_dispatch_thread_text() {
    let (processor, _) = fixture().await;
    let context = admit(&processor, "reviewer-search-turn", "agent_reviewer", true).await;
    processor
        .revalidate_tool_execution_authorization(
            &context.workspace_id,
            &context.thread_id,
            &context.turn_id,
            None,
            ResourceAction::ThreadRead,
        )
        .await
        .unwrap();
    assert!(
        crate::turn_mcp::threads_search::materialize(processor.clone(), context.clone())
            .await
            .is_none()
    );
    let cli =
        crate::turn_mcp::native_tools::materialize_task_tools(processor.clone(), context.clone())
            .await
            .unwrap();
    assert!(
        !cli.iter()
            .flat_map(|bundle| &bundle.specs)
            .any(|spec| spec.spec.name == "threads_search")
    );
    assert!(
        processor
            .search_threads_for_tool(&context, params())
            .await
            .is_err()
    );
    assert!(
        EpisodicSearchAuthority::resolve_for_turn(
            &processor.crud_store,
            &context.workspace_id,
            &context.turn_id,
            None
        )
        .await
        .is_err(),
        "recall must honor the same persisted Agent ceiling"
    );
}

#[tokio::test]
async fn real_messenger_binding_can_read_current_root_but_cannot_observe_sibling_child() {
    let (processor, _) = fixture().await;
    let context = admit(&processor, "messenger-search-turn", "agent_messenger", true).await;
    assert!(
        crate::turn_mcp::threads_search::materialize(processor.clone(), context.clone())
            .await
            .is_some()
    );
    let cli =
        crate::turn_mcp::native_tools::materialize_task_tools(processor.clone(), context.clone())
            .await
            .unwrap();
    assert!(
        cli.iter()
            .flat_map(|bundle| &bundle.specs)
            .any(|spec| spec.spec.name == "threads_search")
    );
    let result = processor
        .search_threads_for_tool(&context, params())
        .await
        .unwrap();
    assert!(result.to_string().contains(THREAD_RED_PRIVATE_A_ID));
    assert!(!result.to_string().contains(THREAD_RED_INTERNAL_ID));
    assert!(!result.to_string().contains(THREAD_RED_WORKSPACE_ID));
    let scope = EpisodicSearchAuthority::resolve_for_turn(
        &processor.crud_store,
        &context.workspace_id,
        &context.turn_id,
        None,
    )
    .await
    .unwrap();
    let recall = processor
        .thread_episodic_recall_service
        .recall_authorized_workspace(scope, "provider settings", 8, 2400)
        .await
        .unwrap();
    assert!(
        recall
            .hits
            .iter()
            .all(|hit| hit.provenance.thread_id.0 == THREAD_RED_PRIVATE_A_ID)
    );
}
