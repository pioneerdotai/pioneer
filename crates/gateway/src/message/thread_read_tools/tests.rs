//! Gateway disclosure and traversal regressions. No provider or CLI is started.
use super::*;
use crate::message::tests::{
    persist_test_execution_authorization_context_for_principal, test_gateway_secrets,
    test_provider, test_summary_config, test_tool_loop_config,
};
use crate::tests::authorization::{
    IsolatedEpic4Harness, MEMBER_A_ID, MEMBER_B_ID, THREAD_RED_PRIVATE_A_ID,
    THREAD_RED_PRIVATE_B_ID, THREAD_RED_WORKSPACE_ID, WORKSPACE_RED_ID,
};
use pioneer_protocol::{AuthSessionId, DeviceId, GatewayId, PrincipalId, PrincipalKind, RoleKey};
use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

fn member(id: &str) -> AuthenticatedSessionPrincipal {
    AuthenticatedSessionPrincipal {
        gateway_id: GatewayId::new("G00000000000000000001").unwrap(),
        principal_id: PrincipalId::new(id).unwrap(),
        kind: PrincipalKind::User,
        role_key: Some(RoleKey::member()),
        device_id: DeviceId::new("D0000000000000000000Z").unwrap(),
        session_id: AuthSessionId::new("S0000000000000000000Z").unwrap(),
        access_jti: "non-bearer-test".into(),
        access_expires_at_unix: u64::MAX,
    }
}

fn authority(id: &str, root: &str, observe_workspace: bool) -> ThreadReadAuthority {
    ThreadReadAuthority {
        principal: member(id),
        workspace_id: WORKSPACE_RED_ID.into(),
        root_thread_id: root.into(),
        current_thread_id: root.into(),
        observe_children: true,
        observe_workspace,
    }
}

async fn fixture() -> (IsolatedEpic4Harness, MessageProcessor) {
    let harness = IsolatedEpic4Harness::new().await.unwrap();
    let store = Arc::new(pioneer_crud::CrudStore::new(harness.database.clone()));
    let processor = MessageProcessor::new(
        Arc::new(crate::thread::ThreadManager::new("fixture", "echo")),
        test_provider(),
        Arc::new(crate::session::SessionManager::new()),
        Arc::new(crate::workspace::WorkspaceManager::new(
            harness.database.clone(),
        )),
        store,
        test_gateway_secrets(),
        test_summary_config(),
        test_tool_loop_config(),
    );
    (harness, processor)
}

async fn list_all(processor: &MessageProcessor, authority: &ThreadReadAuthority) -> Value {
    let codec = processor.thread_read_cursor_codec().unwrap();
    let mut cursor = None;
    let mut rows = Vec::new();
    for _ in 0..100 {
        let page = processor
            .thread_list_page(authority, ThreadsListParams { limit: 1, cursor }, &codec)
            .await
            .unwrap();
        rows.extend(page["threads"].as_array().unwrap().iter().cloned());
        if page["eof"] == true {
            return json!(rows);
        }
        cursor = Some(page["nextCursor"].as_str().unwrap().to_owned());
    }
    panic!("bounded fixture list failed to terminate");
}

#[tokio::test]
async fn list_filters_private_metadata_and_rechecks_acl_on_cursor_continuation() {
    let (harness, processor) = fixture().await;
    harness.database.execute_unprepared("UPDATE thread SET preview='secret-preview-B' WHERE id='T00000000000000000002'; INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at) VALUES ('T00000000000000000003','T00000000000000000002','T00000000000000000002',1,CURRENT_TIMESTAMP)").await.unwrap();
    let a = authority(MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, true);
    let b = authority(MEMBER_B_ID, THREAD_RED_PRIVATE_B_ID, true);
    let a_rows = list_all(&processor, &a).await;
    let b_rows = list_all(&processor, &b).await;
    assert!(
        a_rows
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["threadId"] == THREAD_RED_PRIVATE_A_ID)
    );
    assert!(
        b_rows
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["threadId"] == THREAD_RED_PRIVATE_B_ID)
    );
    for (rows, hidden_id, hidden_title) in [
        (&a_rows, THREAD_RED_PRIVATE_B_ID, "Red private B"),
        (&b_rows, THREAD_RED_PRIVATE_A_ID, "Red private A"),
    ] {
        let encoded = rows.to_string();
        assert!(!encoded.contains(hidden_id));
        assert!(!encoded.contains(hidden_title));
    }
    assert!(!a_rows.to_string().contains("secret-preview-B"));
    let workspace_row = a_rows
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["threadId"] == THREAD_RED_WORKSPACE_ID)
        .unwrap();
    assert!(workspace_row.get("parentThreadId").is_none());
    assert!(workspace_row.get("rootThreadId").is_none());
    assert!(
        !processor
            .thread_source_readable(&a, THREAD_RED_PRIVATE_B_ID)
            .await
            .unwrap()
    );
    // A broader user ACL does not widen a capsule-restricted execution.
    let capsule_only = authority(MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, false);
    assert!(
        !processor
            .thread_source_readable(&capsule_only, THREAD_RED_WORKSPACE_ID)
            .await
            .unwrap()
    );

    let codec = processor.thread_read_cursor_codec().unwrap();
    let first = processor
        .thread_list_page(
            &a,
            ThreadsListParams {
                limit: 1,
                cursor: None,
            },
            &codec,
        )
        .await
        .unwrap();
    let cursor = first["nextCursor"].as_str().unwrap().to_owned();
    // Same cursor cannot be transferred to another user or page-size contract.
    assert!(
        processor
            .thread_list_page(
                &b,
                ThreadsListParams {
                    limit: 1,
                    cursor: Some(cursor.clone())
                },
                &codec
            )
            .await
            .is_err()
    );
    assert!(
        processor
            .thread_list_page(
                &a,
                ThreadsListParams {
                    limit: 2,
                    cursor: Some(cursor.clone())
                },
                &codec
            )
            .await
            .is_err()
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
        !processor
            .thread_source_readable(&a, THREAD_RED_PRIVATE_A_ID)
            .await
            .unwrap()
    );
    let next = processor
        .thread_list_page(
            &a,
            ThreadsListParams {
                limit: 1,
                cursor: Some(cursor),
            },
            &codec,
        )
        .await
        .unwrap();
    assert_eq!(next["threads"], json!([]));
}

async fn add_thread(processor: &MessageProcessor, id: &str, parent: Option<&str>, internal: bool) {
    let db = processor.crud_store.database_connection();
    db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO thread(id,workspace_id,name,preview,mode,model,model_provider,status,origin_kind,sidebar_visibility,access_class,created_at,updated_at,created_by_actor_kind) VALUES (?,?,'child title','child preview','agent','m','echo','closed','system','hidden',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'system')",
        [id.into(), WORKSPACE_RED_ID.into(), (if internal { "internal" } else { "workspace" }).into()])).await.unwrap();
    if let Some(parent) = parent {
        db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "INSERT INTO thread_lineage(child_thread_id,parent_thread_id,root_thread_id,depth,created_at,created_by_thread_id,created_by_turn_id) VALUES (?,?,?,1,CURRENT_TIMESTAMP,?,'read-root-turn')",
            [id.into(), parent.into(), THREAD_RED_WORKSPACE_ID.into(), THREAD_RED_WORKSPACE_ID.into()])).await.unwrap();
    }
}

#[tokio::test]
async fn recursive_pages_cover_large_intermediate_subtree_without_siblings_or_cycle() {
    let (_harness, processor) = fixture().await;
    let a = authority(MEMBER_A_ID, THREAD_RED_WORKSPACE_ID, true);
    add_thread(&processor, "branch", Some(THREAD_RED_WORKSPACE_ID), false).await;
    add_thread(&processor, "sibling", Some(THREAD_RED_WORKSPACE_ID), false).await;
    for n in 0..141 {
        add_thread(&processor, &format!("leaf-{n:03}"), Some("branch"), false).await;
    }
    add_thread(&processor, "grandchild", Some("leaf-000"), false).await;
    // Malformed edge back to the requested node must terminate, never emit it.
    processor.crud_store.database_connection().execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE thread_lineage SET parent_thread_id='grandchild' WHERE child_thread_id='branch'", [])).await.unwrap();
    let codec = processor.thread_read_cursor_codec().unwrap();
    let mut cursor = None;
    let mut found = std::collections::BTreeMap::new();
    let mut ended = false;
    for page_index in 0..100 {
        let page = processor
            .thread_children_page(
                &a,
                ThreadsChildrenListParams {
                    thread_id: "branch".into(),
                    recursive: true,
                    limit: 7,
                    cursor,
                },
                &codec,
            )
            .await
            .unwrap();
        for row in page["threads"].as_array().unwrap() {
            let id = row["threadId"].as_str().unwrap().to_owned();
            assert!(
                found.insert(id, row["depth"].as_u64().unwrap()).is_none(),
                "duplicate descendant"
            );
        }
        if page_index == 0 {
            assert_eq!(page["eof"], false);
            // Neither a sibling's metadata/activity nor an inaccessible thread
            // elsewhere in this workspace belongs to this subtree's revision.
            processor.crud_store.database_connection().execute_raw(Statement::from_sql_and_values(
                DatabaseBackend::Sqlite,
                "UPDATE thread SET name='unrelated edit',preview='new preview',updated_at='2030-01-01 00:00:00+00:00' WHERE id IN ('sibling',?)",
                [THREAD_RED_PRIVATE_B_ID.into()],
            )).await.unwrap();
        }
        if page["eof"] == true {
            ended = true;
            break;
        }
        cursor = Some(page["nextCursor"].as_str().unwrap().to_owned());
    }
    assert!(ended);
    assert_eq!(found.len(), 142);
    assert_eq!(found.get("grandchild"), Some(&2));
    for n in 0..141 {
        assert_eq!(found.get(&format!("leaf-{n:03}")), Some(&1));
    }
    assert!(!found.contains_key("branch"));
    assert!(!found.contains_key("sibling"));
    assert!(!found.contains_key(THREAD_RED_WORKSPACE_ID));
    let first = processor
        .thread_children_page(
            &a,
            ThreadsChildrenListParams {
                thread_id: "branch".into(),
                recursive: true,
                limit: 7,
                cursor: None,
            },
            &codec,
        )
        .await
        .unwrap();
    processor
        .crud_store
        .database_connection()
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE thread_lineage SET parent_thread_id='sibling' WHERE child_thread_id='leaf-140'",
            [],
        ))
        .await
        .unwrap();
    let error = processor
        .thread_children_page(
            &a,
            ThreadsChildrenListParams {
                thread_id: "branch".into(),
                recursive: true,
                limit: 7,
                cursor: Some(first["nextCursor"].as_str().unwrap().to_owned()),
            },
            &codec,
        )
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("stale"),
        "reparenting an unvisited descendant must not silently skip it"
    );
}

#[tokio::test]
async fn children_cursor_pins_pending_metadata_and_detects_deleted_unvisited_child() {
    let (harness, processor) = fixture().await;
    add_thread(&processor, "fragment-root", None, false).await;
    add_thread(&processor, "fragment-child", Some("fragment-root"), false).await;
    add_thread(&processor, "fragment-tail", Some("fragment-root"), false).await;
    add_thread(&processor, "outside-child", Some("fragment-tail"), false).await;
    let title = "🌍\"\n".repeat(450_000);
    harness
        .database
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE thread SET name=? WHERE id='fragment-child'",
            [title.into()],
        ))
        .await
        .unwrap();
    let a = authority(MEMBER_A_ID, THREAD_RED_WORKSPACE_ID, true);
    let codec = processor.thread_read_cursor_codec().unwrap();
    let params = ThreadsChildrenListParams {
        thread_id: "fragment-root".into(),
        recursive: false,
        limit: 1,
        cursor: None,
    };
    let first = processor
        .thread_children_page(&a, params.clone(), &codec)
        .await
        .unwrap();
    let fragment = &first["threads"][0]["fragment"];
    assert_eq!(fragment["offset"], 0);
    assert_eq!(fragment["eof"], false);
    assert!(pioneer_tools::thread_read_page_fits(&first));
    let cursor = first["nextCursor"].as_str().unwrap().to_owned();
    // A grandchild is outside a direct-children traversal, even though it would
    // be relevant to recursive=true. The pending fragment must still continue.
    harness.database.execute_unprepared(
        "UPDATE thread SET name='outside direct traversal',preview='activity',updated_at='2030-01-01 00:00:00+00:00' WHERE id='outside-child'",
    ).await.unwrap();
    let continuation = ThreadsChildrenListParams {
        cursor: Some(cursor),
        ..params.clone()
    };
    let second = processor
        .thread_children_page(&a, continuation.clone(), &codec)
        .await
        .unwrap();
    assert_eq!(
        second["threads"][0]["fragment"]["offset"],
        fragment["nextOffset"]
    );
    assert_eq!(
        second["threads"][0]["fragment"]["version"],
        fragment["version"]
    );
    assert!(pioneer_tools::thread_read_page_fits(&second));
    harness
        .database
        .execute_unprepared(
            "UPDATE thread SET name='changed pending title' WHERE id='fragment-child'",
        )
        .await
        .unwrap();
    assert!(
        processor
            .thread_children_page(&a, continuation, &codec)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale")
    );

    // After restarting with the new metadata, deletion beyond the first page
    // must invalidate that walk rather than silently report an earlier EOF.
    let restarted = processor
        .thread_children_page(&a, params.clone(), &codec)
        .await
        .unwrap();
    assert_eq!(restarted["threads"][0]["title"], "changed pending title");
    harness
        .database
        .execute_unprepared("DELETE FROM thread WHERE id='fragment-tail'")
        .await
        .unwrap();
    let continuation = ThreadsChildrenListParams {
        cursor: Some(restarted["nextCursor"].as_str().unwrap().to_owned()),
        ..params
    };
    assert!(
        processor
            .thread_children_page(&a, continuation, &codec)
            .await
            .unwrap_err()
            .to_string()
            .contains("stale")
    );
}

#[tokio::test]
async fn internal_child_requires_authorized_root_and_parent_history_cursor_is_independent() {
    let (harness, processor) = fixture().await;
    harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES ('read-root-turn',?,'completed','conversation','user','principal',?,'2026-01-01 00:00:00 +00:00','2026-01-01 00:00:00 +00:00')",
        [THREAD_RED_WORKSPACE_ID.into(), MEMBER_A_ID.into()])).await.unwrap();
    persist_test_execution_authorization_context_for_principal(
        &processor,
        &member(MEMBER_A_ID),
        WORKSPACE_RED_ID,
        THREAD_RED_WORKSPACE_ID,
        "read-root-turn",
    )
    .await;
    add_thread(
        &processor,
        "internal-child",
        Some(THREAD_RED_WORKSPACE_ID),
        true,
    )
    .await;
    let a = authority(MEMBER_A_ID, THREAD_RED_WORKSPACE_ID, true);
    assert!(
        processor
            .thread_source_readable(&a, "internal-child")
            .await
            .unwrap()
    );
    let mut no_child_grant = authority(MEMBER_A_ID, THREAD_RED_WORKSPACE_ID, true);
    no_child_grant.observe_children = false;
    assert!(
        !processor
            .thread_source_readable(&no_child_grant, "internal-child")
            .await
            .unwrap()
    );
    let unavailable = processor
        .read_saved_thread_history(
            WORKSPACE_RED_ID,
            THREAD_RED_WORKSPACE_ID,
            1,
            None,
            |_| async { Err(anyhow::anyhow!("authorization source unavailable")) },
        )
        .await;
    assert!(
        unavailable.is_err(),
        "authorization failure must not masquerade as filtered EOF"
    );
    // The real stored parent context is required by internal-child resolution.
    let first = processor
        .read_saved_thread_history(
            WORKSPACE_RED_ID,
            THREAD_RED_WORKSPACE_ID,
            1,
            None,
            |_| async { Ok(true) },
        )
        .await
        .unwrap();
    assert!(!first.eof);
    let parent_cursor = first.next_cursor.clone().unwrap();
    let child = processor
        .read_saved_thread_history(WORKSPACE_RED_ID, "internal-child", 1, None, |_| async {
            Ok(false)
        })
        .await
        .unwrap();
    assert!(child.eof);
    let resumed = processor
        .read_saved_thread_history(
            WORKSPACE_RED_ID,
            THREAD_RED_WORKSPACE_ID,
            1,
            Some(parent_cursor),
            |_| async { Ok(true) },
        )
        .await
        .unwrap();
    assert_ne!(first.events[0]["eventId"], resumed.events[0]["eventId"]);
    assert!(resumed.eof);
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
        !processor
            .thread_source_readable(&a, "internal-child")
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn oversized_saved_input_reconstructs_exactly_and_edits_invalidate_continuation() {
    let (harness, processor) = fixture().await;
    harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES ('large-turn',?,'completed','conversation','user','principal',?,'2026-01-01 00:00:00 +00:00','2026-01-01 00:00:00 +00:00')",
        [THREAD_RED_PRIVATE_A_ID.into(), MEMBER_A_ID.into()])).await.unwrap();
    let text = "🌍漢字\"\n".repeat(240000);
    let input = pioneer_protocol::UserInput::Text {
        text: text.clone(),
        text_elements: Vec::new(),
    };
    harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('large-input','large-turn',0,'text',?,?,'2026-01-01 00:00:01 +00:00')",
        [text.clone().into(), serde_json::to_string(&input).unwrap().into()])).await.unwrap();
    let before = harness
        .database
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS n FROM thread_read_cursor".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "n")
        .unwrap();
    let mut cursor = None;
    let mut saved_cursor = None;
    let mut fragments = String::new();
    let mut ended = false;
    for _ in 0..200 {
        let page = processor
            .read_saved_thread_history(
                WORKSPACE_RED_ID,
                THREAD_RED_PRIVATE_A_ID,
                1,
                cursor,
                |_| async { Ok(false) },
            )
            .await
            .unwrap();
        assert!(
            serde_json::to_vec(&page.events).unwrap().len()
                < pioneer_protocol::TOOL_RESPONSE_MAX_BYTES
        );
        for event in &page.events {
            if let Some(fragment) = event.get("fragment") {
                assert_eq!(
                    fragment["offset"].as_u64().unwrap() as usize,
                    fragments.len()
                );
                fragments.push_str(fragment["text"].as_str().unwrap());
            }
        }
        if saved_cursor.is_none() {
            saved_cursor = page.next_cursor.clone();
        }
        if page.eof {
            ended = true;
            break;
        }
        cursor = page.next_cursor;
    }
    assert!(ended);
    let reconstructed: Value = serde_json::from_str(&fragments).unwrap();
    assert_eq!(reconstructed["content"]["input"]["text"], text);
    let after = harness
        .database
        .query_one_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            "SELECT COUNT(*) AS n FROM thread_read_cursor".to_owned(),
        ))
        .await
        .unwrap()
        .unwrap()
        .try_get::<i64>("", "n")
        .unwrap();
    assert_eq!(
        before, after,
        "history observation must not mark any conversation read"
    );
    harness.database.execute_unprepared("UPDATE turn SET message_revision=message_revision+1, message_deleted_at=CURRENT_TIMESTAMP WHERE id='large-turn'").await.unwrap();
    let stale = processor
        .read_saved_thread_history(
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            1,
            saved_cursor,
            |_| async { Ok(false) },
        )
        .await;
    assert!(stale.err().unwrap().to_string().contains("stale"));
    let deleted = processor
        .read_saved_thread_history(
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            100,
            None,
            |_| async { Ok(false) },
        )
        .await
        .unwrap();
    assert!(
        !serde_json::to_string(&deleted.events)
            .unwrap()
            .contains("🌍")
    );
}

#[tokio::test]
async fn legacy_tool_result_and_retained_log_are_readable_but_deleted_turn_is_not() {
    use pioneer_protocol::{
        ThreadToolResultReadParams, ToolCallStatus, ToolDisplayPayload, ToolOutputPolicySnapshot,
        ToolStoragePayload, TurnItem, TurnItemExecutionClass,
    };
    let (harness, processor) = fixture().await;
    harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES ('legacy-tool-turn',?,'completed','conversation','user','principal',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [THREAD_RED_PRIVATE_A_ID.into(), MEMBER_A_ID.into()])).await.unwrap();
    let text = "retained 🌍 output\n".repeat(1000);
    // A legacy shell item can retain its typed display without a separate full
    // result. Unknown external tools only allow summary display payloads.
    let item = TurnItem::DynamicToolCall {
        id: "legacy-tool".into(),
        tool_name: "exec_command".into(),
        arguments: json!({"exact":42}),
        status: ToolCallStatus::Completed,
        execution_class: TurnItemExecutionClass::Standard,
        recovery_policy: None,
        output_policy: ToolOutputPolicySnapshot::for_external_runtime_tool_name("exec_command"),
        display: ToolDisplayPayload::Shell {
            stdout: Some(text.clone()),
            stderr: None,
            aggregated_output: None,
            exit_code: Some(0),
            duration_ms: None,
            timed_out: None,
            truncated: false,
        },
        storage: ToolStoragePayload::None,
        recovery: None,
        success: Some(true),
        outcome: None,
        observation: None,
    };
    processor
        .crud_store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: WORKSPACE_RED_ID.into(),
                thread_id: THREAD_RED_PRIVATE_A_ID.into(),
                turn_id: "legacy-tool-turn".into(),
                item,
            },
            1_800_000_000,
        )
        .await
        .unwrap();
    processor
        .crud_store
        .record_tool_output(
            "legacy-log",
            &pioneer_protocol::ItemDeltaNotification {
                workspace_id: WORKSPACE_RED_ID.into(),
                thread_id: THREAD_RED_PRIVATE_A_ID.into(),
                turn_id: "legacy-tool-turn".into(),
                item_id: "legacy-tool".into(),
                delta: "retained stdout".into(),
                stream: Some(pioneer_protocol::ItemDeltaStream::Stdout),
                payload: None,
                markdown: None,
                markdown_version: None,
            },
        )
        .await
        .unwrap();
    let mut params = ThreadToolResultReadParams {
        workspace_id: WORKSPACE_RED_ID.into(),
        thread_id: THREAD_RED_PRIVATE_A_ID.into(),
        turn_id: "legacy-tool-turn".into(),
        item_id: "legacy-tool".into(),
        output_log: false,
        cursor: None,
        max_tokens: Some(500),
        max_bytes: Some(1000),
    };
    let mut restored = String::new();
    let mut eof = false;
    for _ in 0..200 {
        let page = processor
            .read_thread_tool_result(params.clone())
            .await
            .unwrap();
        let wire = serde_json::to_string(&page).unwrap();
        assert!(wire.len() <= 1000 && pioneer_compaction::text_tokens(&wire) <= 500);
        restored.push_str(&page.text);
        if page.eof {
            eof = true;
            break;
        }
        params.cursor = page.next;
    }
    assert!(eof);
    let saved: Value = serde_json::from_str(&restored).unwrap();
    assert_eq!(saved["fullResultSaved"], false);
    assert_eq!(saved["item"]["display"]["stdout"], text);
    params.output_log = true;
    params.cursor = None;
    let log = processor
        .read_thread_tool_result(params.clone())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&log.text).unwrap()["text"],
        "retained stdout"
    );
    harness.database.execute_unprepared("UPDATE turn SET message_revision=message_revision+1,message_deleted_at=CURRENT_TIMESTAMP WHERE id='legacy-tool-turn'").await.unwrap();
    assert!(
        processor
            .read_thread_tool_result(params.clone())
            .await
            .is_err()
    );
    params.output_log = false;
    assert!(processor.read_thread_tool_result(params).await.is_err());
}

#[tokio::test]
async fn public_result_reader_honors_token_budget_with_real_cursors_for_all_sources() {
    use pioneer_protocol::{
        ThreadToolResultReadParams, ThreadToolResultReadResponse, ToolCallStatus,
        ToolDisplayPayload, ToolOutputPolicySnapshot, ToolStoragePayload, TurnItem,
        TurnItemExecutionClass,
    };

    async fn call(
        handler: &Arc<dyn ToolHandler>,
        params: &ThreadToolResultReadParams,
    ) -> Result<Box<dyn ToolOutput>, ToolError> {
        handler
            .handle(
                ToolInvocation {
                    call_id: "budget-read".into(),
                    tool_name: "threads_tools_result_read".into(),
                    source: pioneer_tools::ToolCallSource::Model,
                    payload: ToolPayload::Function {
                        arguments: serde_json::to_value(params).unwrap(),
                    },
                    workdir: Default::default(),
                    environment: Default::default(),
                    attempt_id: 0,
                    idempotency_key: None,
                    recovery: Default::default(),
                    permission_metadata: Default::default(),
                    execution_security_snapshot: None,
                    apply_patch_preflight: None,
                    cancellation: tokio_util::sync::CancellationToken::new(),
                },
                pioneer_tools::ToolEventBus::new(8).start_trace(
                    "budget-turn",
                    "budget-read",
                    "threads_tools_result_read",
                ),
            )
            .await
    }

    let (harness, processor) = fixture().await;
    harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES ('budget-turn',?,'in_progress','conversation','user','principal',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [THREAD_RED_PRIVATE_A_ID.into(), MEMBER_A_ID.into()],
    )).await.unwrap();
    // Reach the public reader through a live member execution within the
    // member role's supervised ceiling, as in the anchored-handler fixture.
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
        processor
            .provider_registry
            .authority_fingerprint_for_workspace(WORKSPACE_RED_ID, "openai")
            .unwrap()
            .as_str(),
    );
    assert!(
        processor
            .crud_store
            .set_turn_execution_authorization_context(
                "budget-turn",
                &authorization.to_persisted_json().unwrap(),
            )
            .await
            .unwrap()
    );
    let text = (0..180)
        .map(|n| format!("{n:04}:🌍e\u{301}漢字 \"\\\n"))
        .collect::<String>();
    for id in ["canonical-budget", "saved-item-budget"] {
        let item = TurnItem::DynamicToolCall {
            id: id.into(),
            tool_name: "exec_command".into(),
            arguments: json!({"exact":42}),
            status: ToolCallStatus::Completed,
            execution_class: TurnItemExecutionClass::Standard,
            recovery_policy: None,
            output_policy: ToolOutputPolicySnapshot::for_external_runtime_tool_name("exec_command"),
            display: ToolDisplayPayload::Shell {
                stdout: Some(text.clone()),
                stderr: None,
                aggregated_output: None,
                exit_code: Some(0),
                duration_ms: None,
                timed_out: None,
                truncated: false,
            },
            storage: ToolStoragePayload::None,
            recovery: None,
            success: Some(true),
            outcome: None,
            observation: None,
        };
        processor
            .crud_store
            .materialize_item_completed(
                pioneer_protocol::ItemCompletedNotification {
                    workspace_id: WORKSPACE_RED_ID.into(),
                    thread_id: THREAD_RED_PRIVATE_A_ID.into(),
                    turn_id: "budget-turn".into(),
                    item,
                },
                1_800_000_000,
            )
            .await
            .unwrap();
    }
    let canonical = json!({"output":text,"sentinel":"complete🌍"}).to_string();
    harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn_llm_context(id,turn_id,item_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES ('budget-source','budget-turn','canonical-budget',1,'tool_result_v2',?,'{}',CURRENT_TIMESTAMP)",
        [canonical.clone().into()],
    )).await.unwrap();
    let mut log = String::new();
    for (index, delta) in [text.clone(), "last 🌍 chunk\n".repeat(90)]
        .into_iter()
        .enumerate()
    {
        processor
            .crud_store
            .record_tool_output(
                &format!("budget-log-{index}"),
                &pioneer_protocol::ItemDeltaNotification {
                    workspace_id: WORKSPACE_RED_ID.into(),
                    thread_id: THREAD_RED_PRIVATE_A_ID.into(),
                    turn_id: "budget-turn".into(),
                    item_id: "saved-item-budget".into(),
                    delta: delta.clone(),
                    stream: Some(pioneer_protocol::ItemDeltaStream::Stdout),
                    payload: None,
                    markdown: None,
                    markdown_version: None,
                },
            )
            .await
            .unwrap();
        log.push_str(&json!({"stream":"stdout","text":delta,"metadata":null}).to_string());
    }
    let processor = Arc::new(processor);
    let bundle = processor.thread_result_tools(TurnToolContext {
        workspace_id: WORKSPACE_RED_ID.into(),
        thread_id: THREAD_RED_PRIVATE_A_ID.into(),
        turn_id: "budget-turn".into(),
    });
    let handler = &bundle
        .handlers
        .iter()
        .find(|(name, _)| name == "threads_tools_result_read")
        .unwrap()
        .1;
    for (item_id, output_log, expected) in [
        ("canonical-budget", false, Some(canonical)),
        ("saved-item-budget", false, None),
        ("saved-item-budget", true, Some(log)),
    ] {
        let mut params = ThreadToolResultReadParams {
            workspace_id: WORKSPACE_RED_ID.into(),
            thread_id: THREAD_RED_PRIVATE_A_ID.into(),
            turn_id: "budget-turn".into(),
            item_id: item_id.into(),
            output_log,
            cursor: None,
            max_tokens: Some(1),
            max_bytes: None,
        };
        let error = call(handler, &params)
            .await
            .err()
            .expect("framing cannot fit one token");
        assert!(error.to_string().contains("budget"));
        params.max_tokens = Some(500);
        // The legacy saved view has typed metadata as well as stdout. Compare
        // every byte with its complete public, unbudgeted representation too.
        let expected = if let Some(expected) = expected {
            expected
        } else {
            let unbudgeted = ThreadToolResultReadParams {
                max_tokens: None,
                ..params.clone()
            };
            let whole: ThreadToolResultReadResponse =
                serde_json::from_str(&call(handler, &unbudgeted).await.unwrap().raw_text())
                    .unwrap();
            assert!(whole.eof && whole.next.is_none());
            let saved: Value = serde_json::from_str(&whole.text).unwrap();
            assert_eq!(saved["kind"], "saved_item");
            assert_eq!(saved["fullResultSaved"], false);
            assert_eq!(saved["item"]["id"], item_id);
            assert_eq!(saved["item"]["display"]["stdout"], text);
            whole.text
        };
        let mut restored = String::new();
        let mut pages = 0;
        let mut rollovers = 0;
        loop {
            let output = call(handler, &params).await.unwrap();
            let wire = output.raw_text();
            assert!(output.success());
            assert!(
                pioneer_compaction::text_tokens(&wire) <= 500,
                "{item_id}, log={output_log}"
            );
            assert_eq!(
                serde_json::from_str::<Value>(&wire).unwrap(),
                output.raw_json()
            );
            assert!(pioneer_tools::thread_read_page_fits(&output.raw_json()));
            let page: ThreadToolResultReadResponse = serde_json::from_str(&wire).unwrap();
            let start = params.cursor.as_ref().map_or(0, |cursor| cursor.offset);
            let end = start + page.text.chars().count() as u64;
            restored.push_str(&page.text);
            pages += 1;
            assert!(pages < 200, "reader must make progress");
            if page.eof {
                assert!(page.next.is_none());
                break;
            }
            let next = page.next.expect("nonterminal page needs a public cursor");
            assert!(serde_json::to_vec(&next).unwrap().len() <= 16384);
            if output_log && next.offset == 0 {
                rollovers += 1;
            } else {
                assert!(!page.text.is_empty());
                assert_eq!(next.offset, end, "offsets count Unicode scalars, not bytes");
            }
            params.cursor = Some(next);
        }
        assert!(pages > 1, "fixture must exercise protected continuation");
        assert_eq!(restored, expected, "{item_id}, log={output_log}");
        if output_log {
            assert_eq!(rollovers, 1, "continue through both retained log chunks");
        }
    }
}

#[tokio::test]
async fn saved_observation_result_cannot_replay_a_now_inaccessible_thread() {
    let (harness, processor) = fixture().await;
    harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES ('saved-list-turn',?,'completed','conversation','user','principal',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [THREAD_RED_PRIVATE_A_ID.into(), MEMBER_A_ID.into()])).await.unwrap();
    let item:pioneer_protocol::TurnItem=serde_json::from_value(json!({
        "type":"dynamicToolCall","id":"saved-list","toolName":"threads_list","arguments":{"limit":20},"status":"completed",
        "outputPolicy":pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name("threads_list"),
        "display":{"kind":"hidden"},"storage":{"kind":"none"},"success":true,
    })).unwrap();
    processor
        .crud_store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: WORKSPACE_RED_ID.into(),
                thread_id: THREAD_RED_PRIVATE_A_ID.into(),
                turn_id: "saved-list-turn".into(),
                item: item.clone(),
            },
            1_800_000_000,
        )
        .await
        .unwrap();
    let response = json!({"threads":[{"threadId":THREAD_RED_PRIVATE_B_ID,"title":"private B saved title"}],"nextCursor":null,"eof":true});
    let mut envelope = json!({"kind":"json","truncated":false,"value":{"role":"tool","name":"threads_list","tool_call_id":"saved-list","content":response.to_string()},"ignored": "padding ".repeat(150_000)});
    for _ in 0..24 {
        envelope = json!({"value":envelope});
    }
    let payload = envelope.to_string();
    harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn_llm_context(id,turn_id,item_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES ('saved-list-source','saved-list-turn','saved-list',1,'tool_result_v2',?,'{}',CURRENT_TIMESTAMP)",[payload.into()])).await.unwrap();
    let params = pioneer_protocol::ThreadToolResultReadParams {
        workspace_id: WORKSPACE_RED_ID.into(),
        thread_id: THREAD_RED_PRIVATE_A_ID.into(),
        turn_id: "saved-list-turn".into(),
        item_id: "saved-list".into(),
        output_log: false,
        cursor: None,
        max_tokens: None,
        max_bytes: None,
    };
    let metadata = processor
        .crud_store
        .tool_result_access_source(
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            "saved-list-turn",
            "saved-list",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(metadata["toolName"], "threads_list");
    assert!(
        processor
            .crud_store
            .tool_result_access_source(
                WORKSPACE_RED_ID,
                THREAD_RED_PRIVATE_B_ID,
                "saved-list-turn",
                "saved-list"
            )
            .await
            .unwrap()
            .is_none()
    );
    let mut foreign = params.clone();
    foreign.thread_id = THREAD_RED_PRIVATE_B_ID.into();
    assert!(
        processor.read_thread_tool_result(foreign).await.is_err(),
        "an accessible source cannot authorize a result in another thread"
    );
    let cached_page = processor
        .read_thread_tool_result(params.clone())
        .await
        .unwrap();
    let cached_item: pioneer_protocol::TurnItem = serde_json::from_value(json!({
        "type":"dynamicToolCall","id":"cached-read","toolName":"threads_tools_result_read","arguments":params,"status":"completed",
        "outputPolicy":pioneer_protocol::ToolOutputPolicySnapshot::for_tool_name("threads_tools_result_read"),
        "display":{"kind":"hidden"},"storage":{"kind":"none"},"success":true,
    })).unwrap();
    processor
        .crud_store
        .materialize_item_completed(
            pioneer_protocol::ItemCompletedNotification {
                workspace_id: WORKSPACE_RED_ID.into(),
                thread_id: THREAD_RED_PRIVATE_A_ID.into(),
                turn_id: "saved-list-turn".into(),
                item: cached_item.clone(),
            },
            1_800_000_000,
        )
        .await
        .unwrap();
    let cached_payload = json!({"kind":"json","truncated":false,"value":{"role":"tool","name":"threads_tools_result_read","tool_call_id":"cached-read","content":serde_json::to_string(&cached_page).unwrap()}}).to_string();
    harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn_llm_context(id,turn_id,item_id,sequence,source,payload,output_policy_snapshot,created_at) VALUES ('cached-read-source','saved-list-turn','cached-read',2,'tool_result_v2',?,'{}',CURRENT_TIMESTAMP)", [cached_payload.into()])).await.unwrap();
    let mut cached_params = params.clone();
    cached_params.item_id = "cached-read".into();
    let a = authority(MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, true);
    assert!(
        !processor
            .saved_thread_read_result_disclosable(
                &a,
                &params,
                &serde_json::to_value(&item).unwrap()
            )
            .await
            .unwrap()
    );
    let response = json!({"threads":[{"threadId":THREAD_RED_PRIVATE_A_ID,"title":"available A"}],"nextCursor":null,"eof":true});
    let mut envelope = json!({"kind":"json","truncated":false,"value":{"role":"tool","name":"threads_list","tool_call_id":"saved-list","content":response.to_string()},"ignored": "padding ".repeat(500_000)});
    for _ in 0..24 {
        envelope = json!({"value":envelope});
    }
    let payload = envelope.to_string();
    harness
        .database
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE turn_llm_context SET payload=? WHERE id='saved-list-source'",
            [payload.into()],
        ))
        .await
        .unwrap();
    assert!(
        processor
            .saved_thread_read_result_disclosable(
                &a,
                &params,
                &serde_json::to_value(&item).unwrap()
            )
            .await
            .unwrap()
    );
    assert!(
        !processor
            .saved_thread_read_result_disclosable(
                &a,
                &cached_params,
                &serde_json::to_value(&cached_item).unwrap()
            )
            .await
            .unwrap(),
        "access to a replacement result must not authorize stale cached private metadata"
    );
    let mut deep = json!({"retained":"metadata"});
    for _ in 0..180 {
        deep = json!({"nested":deep});
    }
    let mut saved_item = serde_json::to_value(&item).unwrap();
    saved_item["storage"] = json!({"kind":"metadata","metadata":deep});
    harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "UPDATE turn_item SET payload=? WHERE turn_id='saved-list-turn' AND item_id='saved-list'", [saved_item.to_string().into()])).await.unwrap();
    let metadata = processor
        .crud_store
        .tool_result_access_source(
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            "saved-list-turn",
            "saved-list",
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        processor
            .saved_thread_read_result_disclosable(&a, &params, &metadata)
            .await
            .unwrap(),
        "authorization must not deserialize arbitrary retained output/storage"
    );
    let mut cursor = None;
    let mut restored = String::new();
    let mut pages = 0;
    loop {
        let mut request = params.clone();
        request.cursor = cursor;
        let page = processor.read_thread_tool_result(request).await.unwrap();
        let projected = serde_json::to_value(&page).unwrap();
        assert!(pioneer_tools::thread_read_page_fits(&projected));
        restored.push_str(&page.text);
        pages += 1;
        if page.eof {
            break;
        }
        cursor = page.next;
        assert!(cursor.is_some());
    }
    assert!(pages > 1);
    assert_eq!(serde_json::from_str::<Value>(&restored).unwrap(), envelope);
}

#[path = "anchor_tests.rs"]
mod anchor_tests;

#[tokio::test]
async fn history_keeps_messages_above_old_budgets_whole() {
    let (harness, processor) = fixture().await;
    harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES ('whole-turn',?,'completed','conversation','user','principal',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [THREAD_RED_PRIVATE_A_ID.into(), MEMBER_A_ID.into()])).await.unwrap();
    for (index, text) in ["a".repeat(5000), "漢字🌍\n\"".repeat(16000)]
        .into_iter()
        .enumerate()
    {
        let input = pioneer_protocol::UserInput::Text {
            text: text.clone(),
            text_elements: Vec::new(),
        };
        harness.database.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES (?,'whole-turn',?,'text',?,?,CURRENT_TIMESTAMP)",
            [format!("whole-input-{index}").into(), (index as i64).into(), text.clone().into(), serde_json::to_string(&input).unwrap().into()])).await.unwrap();
    }
    let page = processor
        .read_saved_thread_history(
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            100,
            None,
            |_| async { Ok(false) },
        )
        .await
        .unwrap();
    let inputs = page
        .events
        .iter()
        .filter(|event| event["kind"] == "input")
        .collect::<Vec<_>>();
    assert_eq!(inputs.len(), 2);
    assert!(inputs.iter().all(|event| event.get("fragment").is_none()));
    let mut lengths = inputs
        .iter()
        .map(|event| event["content"]["input"]["text"].as_str().unwrap().len())
        .collect::<Vec<_>>();
    lengths.sort();
    assert_eq!(lengths, vec![5000, "漢字🌍\n\"".repeat(16000).len()]);
    assert!(page.eof);
}

#[tokio::test]
async fn metadata_returns_full_titles_and_explicit_preview_truncation_across_pages() {
    let (harness, processor) = fixture().await;
    let title = "long title 漢🌍 ".repeat(200);
    let preview = "p".repeat(170);
    harness
        .database
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE thread SET name=?,preview=? WHERE id=?",
            [
                title.clone().into(),
                preview.into(),
                THREAD_RED_PRIVATE_A_ID.into(),
            ],
        ))
        .await
        .unwrap();
    let a = authority(MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, true);
    let metadata = processor
        .thread_read_metadata(&a, THREAD_RED_PRIVATE_A_ID)
        .await
        .unwrap();
    assert_eq!(metadata["title"], title);
    assert_eq!(metadata["titleTruncated"], false);
    assert_eq!(metadata["preview"].as_str().unwrap().chars().count(), 160);
    assert_eq!(metadata["previewTruncated"], true);
    for index in 0..205 {
        add_thread(&processor, &format!("list-{index:03}"), None, false).await;
    }
    let codec = processor.thread_read_cursor_codec().unwrap();
    let mut cursor = None;
    let mut ids = std::collections::BTreeSet::new();
    loop {
        let page = processor
            .thread_list_page(&a, ThreadsListParams { limit: 100, cursor }, &codec)
            .await
            .unwrap();
        assert!(page["threads"].as_array().unwrap().len() <= 100);
        for row in page["threads"].as_array().unwrap() {
            assert!(ids.insert(row["threadId"].as_str().unwrap().to_owned()));
        }
        if page["eof"] == true {
            break;
        }
        cursor = Some(page["nextCursor"].as_str().unwrap().to_owned());
    }
    assert_eq!(ids.iter().filter(|id| id.starts_with("list-")).count(), 205);
    let first = processor
        .thread_list_page(
            &a,
            ThreadsListParams {
                limit: 1,
                cursor: None,
            },
            &codec,
        )
        .await
        .unwrap();
    harness
        .database
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE thread SET name='changed' WHERE id=?",
            [THREAD_RED_PRIVATE_A_ID.into()],
        ))
        .await
        .unwrap();
    assert!(
        processor
            .thread_list_page(
                &a,
                ThreadsListParams {
                    limit: 1,
                    cursor: Some(first["nextCursor"].as_str().unwrap().to_owned())
                },
                &codec
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("stale")
    );
}

#[tokio::test]
async fn oversized_title_reconstructs_without_losing_metadata_or_continuation() {
    let (harness, processor) = fixture().await;
    let title = "🌍\"\n".repeat(450_000);
    harness
        .database
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "UPDATE thread SET name=? WHERE id=?",
            [title.clone().into(), THREAD_RED_PRIVATE_A_ID.into()],
        ))
        .await
        .unwrap();
    let a = authority(MEMBER_A_ID, THREAD_RED_PRIVATE_A_ID, true);
    let codec = processor.thread_read_cursor_codec().unwrap();
    let mut cursor = None;
    let mut restored = String::new();
    loop {
        let page = processor
            .thread_list_page(&a, ThreadsListParams { limit: 100, cursor }, &codec)
            .await
            .unwrap();
        assert!(pioneer_tools::thread_read_page_fits(&page));
        for row in page["threads"].as_array().unwrap() {
            if row["threadId"] == THREAD_RED_PRIVATE_A_ID {
                let fragment = &row["fragment"];
                assert_eq!(
                    fragment["offset"].as_u64().unwrap() as usize,
                    restored.len()
                );
                restored.push_str(fragment["text"].as_str().unwrap());
            }
        }
        if page["eof"] == true {
            break;
        }
        cursor = Some(page["nextCursor"].as_str().unwrap().to_owned());
        assert!(cursor.as_ref().unwrap().len() <= 16384);
    }
    assert_eq!(
        serde_json::from_str::<Value>(&restored).unwrap()["title"],
        title
    );
}
