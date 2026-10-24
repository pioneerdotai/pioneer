//! Persisted sources and real indexed retrieval; no runtime/provider is started.
use super::*;
use crate::message::thread_read_tools::tests::{authority, fixture};
use crate::tests::authorization::{
    MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, THREAD_RED_PRIVATE_B_ID, WORKSPACE_RED_ID,
};
use pioneer_crud::*;
use pioneer_memory::{
    MemvidThreadEpisodicBackend, ThreadEpisodicMemvidBackend, ThreadEpisodicMemvidIndexRequest,
    thread_episodic_memvid_metadata,
};
use pioneer_protocol::{ItemCompletedNotification, TurnItem};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

async fn capsule(p: &MessageProcessor, root: &str) -> ThreadEpisodicCapsuleRecord {
    p.crud_store
        .resolve_thread_episodic_workspace_active_write_segment(
            ThreadEpisodicWorkspaceActiveWriteSegmentRequest {
                workspace_id: WORKSPACE_RED_ID.into(),
                storage_uri_root: root.into(),
            },
            1_800_000_000,
        )
        .await
        .unwrap()
}

pub(in crate::message) async fn seed(
    p: &MessageProcessor,
    c: &ThreadEpisodicCapsuleRecord,
    thread: &str,
    id: &str,
    text: &str,
) -> ThreadEpisodicItemRecord {
    let turn = format!("search-turn-{id}");
    p.crud_store.database_connection().execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES (?,?,'completed','conversation','user','system',NULL,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [turn.clone().into(), thread.into()])).await.unwrap();
    p.crud_store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: c.workspace_id.clone().into(),
                thread_id: thread.into(),
                turn_id: turn.clone(),
                item: TurnItem::AgentMessage {
                    id: id.into(),
                    text: text.into(),
                    phase: Default::default(),
                    markdown: None,
                    markdown_version: None,
                },
            },
            1_800_000_000,
        )
        .await
        .unwrap();
    // Keep the shared fixture honest when called from a different workspace:
    // the stored item must be a committed canonical assistant source.
    let canonical = p
        .crud_store
        .get_thread_episodic_canonical_item(&c.workspace_id, thread, &turn, id)
        .await
        .unwrap()
        .unwrap();
    assert!(canonical.committed);
    assert!(
        matches!(&canonical.item, TurnItem::AgentMessage { id: actual, text: actual_text, .. } if actual == id && actual_text == text)
    );
    let hash = pioneer_crud::thread_episodic_source::source_text_hash(text);
    let item = p
        .crud_store
        .upsert_thread_episodic_item(
            NewThreadEpisodicItemRecord {
                id: Some(format!("search-index-{id}")),
                workspace_id: c.workspace_id.clone().into(),
                thread_id: thread.into(),
                turn_id: turn,
                item_id: id.into(),
                source_actor_role: ThreadEpisodicSourceActorRole::Assistant,
                source_runtime_kind: ThreadEpisodicSourceRuntimeKind::AssistantTurn,
                source_context:
                    pioneer_protocol::ThreadEpisodicSourceContext::UserVisibleThreadItem,
                visibility: ThreadEpisodicItemVisibility::UserVisible,
                status: ThreadEpisodicItemStatus::Active,
                text_hash: hash.clone(),
                source_text_hash: hash,
                projection_group_id: format!("projection-{id}"),
                language_hint: None,
                token_estimate: 20,
                capsule_id: Some(c.id.clone()),
                capsule_ref: Some(c.capsule_ref.clone()),
                segment_index: Some(c.segment_index),
                frame_id: None,
                frame_uri: None,
                indexed_at: None,
                deleted_at: None,
            },
            1_800_000_000,
        )
        .await
        .unwrap();
    index(p, c, &item, text).await;
    item
}

async fn index(
    p: &MessageProcessor,
    c: &ThreadEpisodicCapsuleRecord,
    item: &ThreadEpisodicItemRecord,
    text: &str,
) {
    let uri = thread_episodic_item_uri(
        &item.workspace_id,
        &item.thread_id,
        &item.turn_id,
        &item.item_id,
        &item.id,
    )
    .unwrap();
    let output = MemvidThreadEpisodicBackend::new()
        .index_item(ThreadEpisodicMemvidIndexRequest {
            storage_uri: c.storage_uri.clone(),
            capsule_id: c.id.clone(),
            capsule_ref: c.capsule_ref.clone(),
            workspace_capsule: true,
            index_item_id: item.id.clone(),
            frame_uri: uri,
            text: text.into(),
            metadata: thread_episodic_memvid_metadata(
                &item.workspace_id,
                &item.thread_id,
                &item.turn_id,
                &item.item_id,
                "assistant",
                "agent",
                "user_visible_thread_item",
                &item.text_hash,
                &item.source_text_hash,
            ),
            embedding: None,
        })
        .await
        .unwrap();
    p.crud_store
        .mark_thread_episodic_item_indexed(
            &item.id,
            ThreadEpisodicItemIndexedUpdate {
                capsule_id: c.id.clone(),
                capsule_ref: c.capsule_ref.clone(),
                segment_index: c.segment_index,
                frame_id: output.frame_id,
                frame_uri: output.frame_uri,
                embedding_artifact_id: None,
            },
            1_800_000_001,
        )
        .await
        .unwrap();
}

async fn search(
    p: &MessageProcessor,
    a: &ThreadReadAuthority,
    position: usize,
    limit: usize,
) -> (Vec<FoundSource>, bool, bool) {
    p.search_thread_sources(
        a,
        ThreadSearchRequest {
            query: "searchneedle",
            position,
            limit,
        },
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn all_files_and_conversations_participate_with_canonical_dedup_and_position_pages() {
    let (_h, p) = fixture().await;
    let temp = tempfile::tempdir().unwrap();
    let root = format!("file://{}", temp.path().display());
    let a = authority(MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, true);
    let mut expected = std::collections::BTreeSet::new();
    let mut duplicate = None;
    for number in 0..18 {
        let thread = if number < 9 {
            format!("search-conversation-{number}")
        } else {
            THREAD_RED_PRIVATE_A_ID.into()
        };
        if number < 9 {
            p.crud_store.database_connection().execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                "INSERT INTO thread(id,workspace_id,name,preview,mode,model,model_provider,status,origin_kind,sidebar_visibility,access_class,created_at,updated_at,created_by_actor_kind,created_by_actor_id) SELECT ?,workspace_id,?,'',mode,model,model_provider,status,origin_kind,sidebar_visibility,'workspace',created_at,updated_at,created_by_actor_kind,created_by_actor_id FROM thread WHERE id=?",
                [thread.clone().into(), format!("Full title {number}").into(), THREAD_RED_PRIVATE_A_ID.into()])).await.unwrap();
        }
        let c = capsule(&p, &root).await;
        let item = seed(
            &p,
            &c,
            &thread,
            &format!("result-{number}"),
            "searchneedle saved decision",
        )
        .await;
        expected.insert((thread.clone(), item.turn_id.clone(), item.item_id.clone()));
        if number == 0 {
            duplicate = Some(item);
        }
        if number == 17 {
            index(
                &p,
                &c,
                duplicate.as_ref().unwrap(),
                "searchneedle saved decision",
            )
            .await;
        }
        p.crud_store
            .transition_thread_episodic_active_write_segment(
                &c.id,
                ThreadEpisodicCapsuleWriteState::Full,
                1_800_000_002,
            )
            .await
            .unwrap();
    }
    let mut seen = std::collections::BTreeSet::new();
    for position in [0, 7, 14] {
        let (found, more, incomplete) = search(&p, &a, position, 7).await;
        assert!(!incomplete);
        assert_eq!(found.len(), if position == 14 { 4 } else { 7 });
        assert_eq!(more, position != 14);
        for f in found {
            assert!(
                seen.insert(f.canonical_key.clone()),
                "duplicate source across files/pages"
            );
            assert_eq!(f.fragment["source"]["workspaceId"], WORKSPACE_RED_ID);
            let anchor: pioneer_protocol::ThreadHistoryAnchor = serde_json::from_value(json!({"turnId":f.fragment["source"]["turnId"],"itemId":f.fragment["source"]["itemId"]})).unwrap();
            let canonical = p
                .crud_store
                .get_thread_episodic_canonical_item(
                    WORKSPACE_RED_ID,
                    &f.thread_id,
                    &anchor.turn_id,
                    anchor.item_id.as_deref().unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
            assert!(canonical.committed);
        }
    }
    assert_eq!(seen, expected);
    // A broader user's ACL cannot widen an execution restricted to its capsule.
    let restricted = authority(MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, false);
    let (own, _, _) = search(&p, &restricted, 0, 100).await;
    assert_eq!(own.len(), 9);
    assert!(own.iter().all(|f| f.thread_id == THREAD_RED_PRIVATE_A_ID));
}

#[tokio::test]
async fn filtered_engine_pages_fill_results_and_disclosure_rechecks_source_and_acl() {
    let (_h, p) = fixture().await;
    let temp = tempfile::tempdir().unwrap();
    let c = capsule(&p, &format!("file://{}", temp.path().display())).await;
    // More than one engine quantum of inaccessible candidates must not hide a later own source.
    for n in 0..105 {
        seed(
            &p,
            &c,
            THREAD_RED_PRIVATE_B_ID,
            &format!("denied-{n}"),
            "searchneedle saved decision",
        )
        .await;
    }
    let item = seed(
        &p,
        &c,
        THREAD_RED_PRIVATE_A_ID,
        "readable",
        "searchneedle saved decision",
    )
    .await;
    let a = authority(MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, true);
    let (found, more, incomplete) = search(&p, &a, 0, 3).await;
    assert_eq!(found.len(), 1);
    assert!(!more);
    assert!(!incomplete);
    let response = search_response(&found, None, true, false);
    assert!(p.search_response_readable(&a, &response).await.unwrap());
    p.crud_store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "DELETE FROM thread_membership WHERE thread_id=? AND principal_id=?",
            [THREAD_RED_PRIVATE_A_ID.into(), MEMBER_A_ID.into()],
        ))
        .await
        .unwrap();
    assert!(!p.search_response_readable(&a, &response).await.unwrap());
    p.crud_store.database_connection().execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO thread_membership(thread_id,principal_id,added_by_actor_kind,created_at,updated_at) VALUES (?,?,'system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)", [THREAD_RED_PRIVATE_A_ID.into(),MEMBER_A_ID.into()])).await.unwrap();
    assert!(p.search_response_readable(&a, &response).await.unwrap());
    p.crud_store
        .exclude_thread_episodic_item(
            NewThreadEpisodicExclusionRecord {
                id: None,
                workspace_id: WORKSPACE_RED_ID.into(),
                thread_id: THREAD_RED_PRIVATE_A_ID.into(),
                index_item_id: item.id,
                reason: ThreadEpisodicExclusionReason::UserRequested,
                created_by: MEMBER_A_ID.into(),
            },
            1_800_000_003,
        )
        .await
        .unwrap();
    assert!(!p.search_response_readable(&a, &response).await.unwrap());
    assert!(search(&p, &a, 0, 3).await.0.is_empty());
}

#[tokio::test]
async fn oversized_card_preserves_full_unicode_title_and_rejects_deleted_source_continuation() {
    let (_h, p) = fixture().await;
    let temp = tempfile::tempdir().unwrap();
    let c = capsule(&p, &format!("file://{}", temp.path().display())).await;
    let item = seed(
        &p,
        &c,
        THREAD_RED_PRIVATE_A_ID,
        "large-card",
        "searchneedle saved decision",
    )
    .await;
    let title = "Полное название 🌍 ".repeat(100_000);
    p.crud_store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE thread SET name=? WHERE id=?",
            [title.clone().into(), THREAD_RED_PRIVATE_A_ID.into()],
        ))
        .await
        .unwrap();
    let a = authority(MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, true);
    let (found, _, _) = search(&p, &a, 0, 1).await;
    assert_eq!(found[0].title.as_deref(), Some(title.as_str()));
    assert!(
        serde_json::to_vec(&search_response(&found, None, true, false))
            .unwrap()
            .len()
            > PAGE_BYTES
    );
    let codec = p.thread_read_cursor_codec().unwrap();
    let scope = "oversized-search-card-fixture";
    let mut fragment = CardFragmentCursor {
        index_item_id: item.id.clone(),
        version: String::new(),
        byte_offset: 0,
        coverage_incomplete: false,
        text: found[0].fragment["text"].as_str().unwrap().into(),
        score: found[0].score,
    };
    let mut joined = String::new();
    let mut continuation = None;
    for _ in 0..1000 {
        let page = p
            .continue_search_card(&a, 0, fragment, &codec, scope)
            .await
            .unwrap();
        assert!(serde_json::to_vec(&page).unwrap().len() <= PAGE_BYTES);
        assert!(p.search_response_readable(&a, &page).await.unwrap());
        joined.push_str(page["pageFragment"]["text"].as_str().unwrap());
        let state: SearchCursor = codec
            .decode(page["nextCursor"].as_str().unwrap(), scope)
            .unwrap();
        if page["pageFragment"]["complete"] == true {
            assert_eq!(state.position, 1);
            break;
        }
        continuation = Some(page["nextCursor"].as_str().unwrap().to_owned());
        fragment = state.fragment.unwrap();
    }
    let restored: Value = serde_json::from_str(&joined).unwrap();
    assert_eq!(restored["title"], title);
    assert_eq!(restored["fragments"][0]["source"]["itemId"], "large-card");
    assert_eq!(
        restored["fragments"][0]["source"]["turnId"],
        "search-turn-large-card"
    );
    p.crud_store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE turn SET message_deleted_at=CURRENT_TIMESTAMP,message_revision=message_revision+1 WHERE id=?",
            [item.turn_id.into()],
        ))
        .await
        .unwrap();
    let state: SearchCursor = codec
        .decode(continuation.as_deref().unwrap(), scope)
        .unwrap();
    assert!(
        p.continue_search_card(&a, state.position, state.fragment.unwrap(), &codec, scope)
            .await
            .is_err()
    );
    assert!(
        !p.search_response_readable(&a, &json!({"threads":[restored]}))
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn production_bound_recall_provider_uses_indexed_sources_and_trusted_execution_identity() {
    use crate::message::thread_read_tools::tests::member;
    use pioneer_memory::hooks::{AgentEpisodicRecallProvider, MemoryCurrentThreadRecallRequest};
    let (_h, p) = fixture().await;
    let temp = tempfile::tempdir().unwrap();
    let c = capsule(&p, &format!("file://{}", temp.path().display())).await;
    seed(
        &p,
        &c,
        THREAD_RED_PRIVATE_A_ID,
        "recall-source",
        "searchneedle saved decision",
    )
    .await;
    seed(
        &p,
        &c,
        THREAD_RED_PRIVATE_B_ID,
        "foreign-source",
        "searchneedle foreign decision",
    )
    .await;
    let turn = "search-turn-recall-source";
    p.crud_store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE turn SET status='in_progress',initiated_by_actor_kind='principal',initiated_by_actor_id=? WHERE id=?",
            [MEMBER_A_ID.into(), turn.into()],
        ))
        .await
        .unwrap();
    let profile = pioneer_protocol::TurnPermissionProfileSnapshot::from_mode(
        pioneer_protocol::TurnPermissionMode::Supervised,
        pioneer_protocol::TurnPermissionProfileSource::Composer,
    );
    let mut authorization = crate::authorization::ExecutionAuthorizationContext::for_test(
        &member(MEMBER_A_ID),
        WORKSPACE_RED_ID,
        THREAD_RED_PRIVATE_A_ID,
        &profile,
        None,
    );
    authorization.bind_test_provider_authority(
        "openai",
        "test-model",
        p.provider_registry
            .authority_fingerprint_for_workspace(WORKSPACE_RED_ID, "openai")
            .unwrap()
            .as_str(),
    );
    assert!(
        p.crud_store
            .set_turn_execution_authorization_context(
                turn,
                &authorization.to_persisted_json().unwrap()
            )
            .await
            .unwrap()
    );
    let p = Arc::new(p);
    p.bind_memory_bridge().await;
    let provider = p
        .memory_bridge_providers
        .read()
        .await
        .as_ref()
        .unwrap()
        .episodic_provider
        .clone();
    let output = provider
        .recall_current_thread(MemoryCurrentThreadRecallRequest {
            workspace_id: WORKSPACE_RED_ID.into(),
            thread_id: THREAD_RED_PRIVATE_A_ID.into(),
            turn_id: turn.into(),
            principal_id: Some(crate::tests::authorization::MEMBER_B_ID.into()),
            query: "searchneedle".into(),
            targets: Vec::new(),
            top_k: 10,
            max_chars: 1000,
        })
        .await
        .unwrap();
    assert_eq!(output.items.len(), 1);
    assert!(output.items[0].content.contains("saved decision"));
    assert_eq!(
        output.items[0].provenance.item_id.as_deref(),
        Some("recall-source")
    );
    assert_eq!(
        output.items[0].provenance.thread_id.as_deref(),
        Some(THREAD_RED_PRIVATE_A_ID)
    );
    let tool = p
        .search_threads_page(
            &TurnToolContext {
                workspace_id: WORKSPACE_RED_ID.into(),
                thread_id: THREAD_RED_PRIVATE_A_ID.into(),
                turn_id: turn.into(),
            },
            ThreadsSearchParams {
                query: "searchneedle".into(),
                limit: 10,
                cursor: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(
        tool["threads"][0]["fragments"][0]["source"]["itemId"],
        "recall-source"
    );
    assert_eq!(tool["threads"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn saved_search_over_256_kib_reconstructs_exact_payload_and_revalidates_disclosure() {
    let (_h, p) = fixture().await;
    let temp = tempfile::tempdir().unwrap();
    let c = capsule(&p, &format!("file://{}", temp.path().display())).await;
    let source = seed(
        &p,
        &c,
        THREAD_RED_PRIVATE_A_ID,
        "saved-source",
        "searchneedle saved decision",
    )
    .await;
    let title = "Saved full title 🌍 ".repeat(30_000);
    p.crud_store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE thread SET name=? WHERE id=?",
            [title.into(), THREAD_RED_PRIVATE_A_ID.into()],
        ))
        .await
        .unwrap();
    let a = authority(MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, true);
    let (found, _, _) = search(&p, &a, 0, 1).await;
    let response = search_response(&found, None, true, false);
    let tool: TurnItem = serde_json::from_value(json!({
        "type":"dynamicToolCall","id":"saved-search","toolName":"threads_search","arguments":{"query":"searchneedle","limit":1},"status":"completed",
        "outputPolicy":pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name("threads_search"),
        "display":{"kind":"hidden"},"storage":{"kind":"none"},"success":true
    })).unwrap();
    p.crud_store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: WORKSPACE_RED_ID.into(),
                thread_id: THREAD_RED_PRIVATE_A_ID.into(),
                turn_id: source.turn_id.clone(),
                item: tool.clone(),
            },
            1_800_000_010,
        )
        .await
        .unwrap();
    let payload = json!({"kind":"json","truncated":false,"value":{"role":"tool","name":"threads_search","tool_call_id":"saved-search","content":response.to_string()}});
    let serialized = payload.to_string();
    assert!(serialized.len() > 256 * 1024 && serialized.len() < PAGE_BYTES);
    p.crud_store.database_connection().execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn_llm_context(id,turn_id,item_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES ('saved-search-context',?,'saved-search',1,'tool_result_v2',?,'{}',CURRENT_TIMESTAMP)",
        [source.turn_id.clone().into(),serialized.into()])).await.unwrap();
    let tool = serde_json::to_value(&tool).unwrap();
    let params = pioneer_protocol::ThreadToolResultReadParams {
        workspace_id: WORKSPACE_RED_ID.into(),
        thread_id: THREAD_RED_PRIVATE_A_ID.into(),
        turn_id: source.turn_id.clone(),
        item_id: "saved-search".into(),
        output_log: false,
        cursor: None,
        max_tokens: None,
        max_bytes: None,
    };
    assert!(
        p.saved_thread_read_result_disclosable(&a, &params, &tool)
            .await
            .unwrap()
    );
    let mut request = params.clone();
    let mut joined = String::new();
    let mut eof = false;
    for _ in 0..1000 {
        // This is the same disclosure gate the result tool applies before each byte page.
        assert!(
            p.saved_thread_read_result_disclosable(&a, &request, &tool)
                .await
                .unwrap()
        );
        let page = p.read_thread_tool_result(request.clone()).await.unwrap();
        joined.push_str(&page.text);
        if page.eof {
            eof = true;
            break;
        }
        request.cursor = page.next;
    }
    assert!(eof);
    assert_eq!(serde_json::from_str::<Value>(&joined).unwrap(), payload);
    p.crud_store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "DELETE FROM thread_membership WHERE thread_id=? AND principal_id=?",
            [THREAD_RED_PRIVATE_A_ID.into(), MEMBER_A_ID.into()],
        ))
        .await
        .unwrap();
    assert!(
        !p.saved_thread_read_result_disclosable(&a, &params, &tool)
            .await
            .unwrap()
    );
    p.crud_store.database_connection().execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO thread_membership(thread_id,principal_id,added_by_actor_kind,created_at,updated_at) VALUES (?,?,'system',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",[THREAD_RED_PRIVATE_A_ID.into(),MEMBER_A_ID.into()])).await.unwrap();
    assert!(
        p.saved_thread_read_result_disclosable(&a, &params, &tool)
            .await
            .unwrap()
    );
    p.crud_store
        .exclude_thread_episodic_item(
            NewThreadEpisodicExclusionRecord {
                id: None,
                workspace_id: WORKSPACE_RED_ID.into(),
                thread_id: THREAD_RED_PRIVATE_A_ID.into(),
                index_item_id: source.id,
                reason: ThreadEpisodicExclusionReason::UserRequested,
                created_by: MEMBER_A_ID.into(),
            },
            1_800_000_011,
        )
        .await
        .unwrap();
    assert!(
        !p.saved_thread_read_result_disclosable(&a, &params, &tool)
            .await
            .unwrap()
    );
    p.crud_store.database_connection().execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE turn SET message_deleted_at=CURRENT_TIMESTAMP,message_revision=message_revision+1 WHERE id=?",[source.turn_id.into()])).await.unwrap();
    assert!(p.read_thread_tool_result(params).await.is_err());
}
