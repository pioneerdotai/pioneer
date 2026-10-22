//! Anchored navigation contracts over persisted canonical items; never starts a runtime.
use super::*;
use pioneer_protocol::{ThreadHistoryAnchor, ThreadHistoryDirection, TurnItem};

const TURN: &str = "anchor-turn";

async fn seed_turn(processor: &MessageProcessor) {
    processor.crud_store.database_connection().execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES (?,?,'completed','conversation','user','principal',?,'2026-01-01 00:00:00+00:00','2026-01-01 00:00:00+00:00')",
        [TURN.into(), THREAD_RED_PRIVATE_A_ID.into(), MEMBER_A_ID.into()],
    )).await.unwrap();
}

async fn save_item(processor: &MessageProcessor, id: &str, text: &str) {
    // Fixed source IDs deliberately make the expected tie order independent of
    // insertion order. Normal canonical INSERT triggers maintain revision state.
    let item = TurnItem::AgentMessage {
        id: id.into(),
        text: text.into(),
        phase: Default::default(),
        markdown: None,
        markdown_version: None,
    };
    processor.crud_store.database_connection().execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,payload,created_at,updated_at) VALUES (?,?,?,'agent_message',?,'2026-01-01 00:00:01+00:00','2026-01-01 00:00:01+00:00')",
        [id.into(), TURN.into(), id.into(), serde_json::to_string(&item).unwrap().into()],
    )).await.unwrap();
}

async fn save_hidden_item(processor: &MessageProcessor, id: &str) {
    let item = TurnItem::SystemEvent {
        id: id.into(),
        level: pioneer_protocol::SystemEventLevel::Info,
        message: "hidden internal content".into(),
        code: Some("agent_thread_status_changed".into()),
        details: None,
    };
    processor.crud_store.database_connection().execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,payload,created_at,updated_at) VALUES (?,?,?,'system_event',?,'2026-01-01 00:00:01+00:00','2026-01-01 00:00:01+00:00')",
        [id.into(), TURN.into(), id.into(), serde_json::to_string(&item).unwrap().into()],
    )).await.unwrap();
}

fn anchor(id: &str) -> ThreadHistoryAnchor {
    ThreadHistoryAnchor {
        turn_id: TURN.into(),
        item_id: Some(id.into()),
    }
}

fn ids(events: &[Value]) -> Vec<&str> {
    events
        .iter()
        .map(|e| e["itemId"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn anchored_equal_time_neighbors_continue_both_directions_and_exclude_new_inserts() {
    use ThreadHistoryDirection::{Newer, Older};
    let (_harness, processor) = fixture().await;
    seed_turn(&processor).await;
    for id in ["d", "b", "f", "a", "e", "c"] {
        save_item(&processor, id, id).await;
    }
    let first = processor
        .read_saved_thread_history_near(
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            3,
            None,
            Some(anchor("c")),
            Older,
            |_| async { Ok(false) },
        )
        .await
        .unwrap();
    assert_eq!(ids(&first.events), ["d", "c", "b"]);
    // Insert on each side of the old position with exactly the same timestamp.
    save_item(&processor, "bb", "new older").await;
    save_item(&processor, "ee", "new newer").await;
    let older = processor
        .read_saved_thread_history_near(
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            3,
            first.older_cursor.clone(),
            None,
            Older,
            |_| async { Ok(false) },
        )
        .await
        .unwrap();
    assert_eq!(older.events[0]["itemId"], "a");
    assert!(
        !serde_json::to_string(&older.events)
            .unwrap()
            .contains("new older")
    );
    let newer = processor
        .read_saved_thread_history_near(
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            3,
            first.newer_cursor.clone(),
            None,
            Newer,
            |_| async { Ok(false) },
        )
        .await
        .unwrap();
    assert_eq!(ids(&newer.events), ["f", "e"]);
    // The fixture's newer child reference is filtered by the callback. Consuming
    // that final candidate must still end the short scan without another cursor.
    assert!(newer.eof);
    assert!(newer.next_cursor.is_none());
    let mismatch = processor
        .read_saved_thread_history_near(
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            3,
            first.older_cursor,
            None,
            Newer,
            |_| async { Ok(false) },
        )
        .await;
    assert!(mismatch.err().unwrap().to_string().contains("cursor"));
    let default = processor
        .read_saved_thread_history(
            WORKSPACE_RED_ID,
            THREAD_RED_PRIVATE_A_ID,
            1,
            None,
            |_| async { Ok(false) },
        )
        .await
        .unwrap();
    assert_eq!(default.events[0]["itemId"], "f");
}

#[tokio::test]
async fn anchor_rejects_wrong_thread_turn_item_and_deleted_source() {
    let (harness, processor) = fixture().await;
    seed_turn(&processor).await;
    save_item(&processor, "source", "private text").await;
    for (thread, turn, item) in [
        (THREAD_RED_PRIVATE_B_ID, TURN, Some("source")),
        (THREAD_RED_PRIVATE_A_ID, "missing-turn", Some("source")),
        (THREAD_RED_PRIVATE_A_ID, TURN, Some("missing-item")),
    ] {
        let result = processor
            .read_saved_thread_history_near(
                WORKSPACE_RED_ID,
                thread,
                1,
                None,
                Some(ThreadHistoryAnchor {
                    turn_id: turn.into(),
                    item_id: item.map(str::to_owned),
                }),
                ThreadHistoryDirection::Older,
                |_| async { Ok(false) },
            )
            .await;
        assert!(result.is_err());
        assert!(!result.err().unwrap().to_string().contains("private text"));
    }
    save_hidden_item(&processor, "hidden").await;
    save_item(&processor, "deleted-item", "deleted private content").await;
    harness
        .database
        .execute_unprepared("DELETE FROM turn_item WHERE item_id='deleted-item'")
        .await
        .unwrap();
    for id in ["hidden", "deleted-item"] {
        let error = processor
            .read_saved_thread_history_near(
                WORKSPACE_RED_ID,
                THREAD_RED_PRIVATE_A_ID,
                1,
                None,
                Some(anchor(id)),
                ThreadHistoryDirection::Older,
                |_| async { Ok(false) },
            )
            .await
            .err()
            .unwrap();
        let text = error.to_string();
        assert!(!text.contains("hidden internal content"));
        assert!(!text.contains("deleted private content"));
    }
    harness.database.execute_unprepared("UPDATE turn SET message_deleted_at=CURRENT_TIMESTAMP,message_revision=message_revision+1 WHERE id='anchor-turn'").await.unwrap();
    for item_id in [None, Some("source".to_owned())] {
        assert!(
            processor
                .read_saved_thread_history_near(
                    WORKSPACE_RED_ID,
                    THREAD_RED_PRIVATE_A_ID,
                    1,
                    None,
                    Some(ThreadHistoryAnchor {
                        turn_id: TURN.into(),
                        item_id
                    }),
                    ThreadHistoryDirection::Older,
                    |_| async { Ok(false) }
                )
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn both_anchor_continuations_become_stale_after_edit_or_delete() {
    for delete in [false, true] {
        let (harness, processor) = fixture().await;
        seed_turn(&processor).await;
        for id in ["a", "b", "c"] {
            save_item(&processor, id, id).await;
        }
        let page = processor
            .read_saved_thread_history_near(
                WORKSPACE_RED_ID,
                THREAD_RED_PRIVATE_A_ID,
                1,
                None,
                Some(anchor("b")),
                ThreadHistoryDirection::Older,
                |_| async { Ok(false) },
            )
            .await
            .unwrap();
        let sql = if delete {
            "UPDATE turn SET message_revision=message_revision+1,message_deleted_at=CURRENT_TIMESTAMP WHERE id='anchor-turn'"
        } else {
            "UPDATE turn_item SET payload=replace(payload,'\"text\":\"a\"','\"text\":\"edited\"') WHERE item_id='a'"
        };
        harness.database.execute_unprepared(sql).await.unwrap();
        for (cursor, direction) in [
            (page.older_cursor, ThreadHistoryDirection::Older),
            (page.newer_cursor, ThreadHistoryDirection::Newer),
        ] {
            assert!(cursor.is_some());
            let error = processor
                .read_saved_thread_history_near(
                    WORKSPACE_RED_ID,
                    THREAD_RED_PRIVATE_A_ID,
                    1,
                    cursor,
                    None,
                    direction,
                    |_| async { Ok(false) },
                )
                .await
                .err()
                .unwrap();
            assert!(error.to_string().contains("stale"));
        }
    }
}

#[tokio::test]
async fn oversized_anchored_unicode_item_reconstructs_in_each_direction() {
    for direction in [ThreadHistoryDirection::Older, ThreadHistoryDirection::Newer] {
        let (_harness, processor) = fixture().await;
        seed_turn(&processor).await;
        let text = "🌍漢字\n\"".repeat(2000);
        save_item(&processor, "large", &text).await;
        // The first candidate is filtered in either direction. Fragment resume
        // must pin the large record rather than revisit that earlier candidate.
        save_hidden_item(&processor, "a-hidden").await;
        save_hidden_item(&processor, "z-hidden").await;
        let mut cursor = None;
        let mut opening = Some(anchor("large"));
        let mut joined = String::new();
        let mut complete = false;
        for _ in 0..300 {
            let page = processor
                .read_saved_thread_history_near(
                    WORKSPACE_RED_ID,
                    THREAD_RED_PRIVATE_A_ID,
                    3,
                    cursor,
                    opening.take(),
                    direction,
                    |_| async { Ok(false) },
                )
                .await
                .unwrap();
            let fragment = &page.events[0]["fragment"];
            assert_eq!(fragment["offset"].as_u64().unwrap() as usize, joined.len());
            joined.push_str(fragment["text"].as_str().unwrap());
            if fragment["eof"] == true {
                complete = true;
                break;
            }
            assert!(!page.eof);
            cursor = page.next_cursor;
            assert!(cursor.is_some());
        }
        assert!(complete, "fragment traversal must terminate");
        let restored: Value = serde_json::from_str(&joined).unwrap();
        assert_eq!(restored["itemId"], "large");
        assert_eq!(restored["content"]["item"]["text"], text);
    }
}

#[tokio::test]
async fn anchored_handler_checks_source_acl_and_revalidates_membership_on_continuation() {
    let (harness, processor) = fixture().await;
    seed_turn(&processor).await;
    for id in ["a", "b", "c"] {
        save_item(&processor, id, id).await;
    }
    harness
        .database
        .execute_unprepared("UPDATE turn SET status='in_progress' WHERE id='anchor-turn'")
        .await
        .unwrap();
    // Member executions must stay within the role's supervised ceiling. The
    // generic helper admits full access, so its lease is correctly fenced here.
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
            .expect("test provider authority should resolve")
            .as_str(),
    );
    assert!(
        processor
            .crud_store
            .set_turn_execution_authorization_context(
                TURN,
                &authorization.to_persisted_json().unwrap(),
            )
            .await
            .unwrap()
    );
    let processor = Arc::new(processor);
    let handler = super::super::ThreadReadHandler {
        processor: processor.clone(),
        context: TurnToolContext {
            workspace_id: WORKSPACE_RED_ID.into(),
            thread_id: THREAD_RED_PRIVATE_A_ID.into(),
            turn_id: TURN.into(),
        },
    };
    // Positive control ensures the execution/lease fixture reaches the reader.
    let page = handler
        .read(
            "threads_history_read",
            json!({
                "threadId":THREAD_RED_PRIVATE_A_ID,"limit":1,
                "anchor":{"turnId":TURN,"itemId":"b"}
            }),
        )
        .await
        .expect("own authorized anchor must be readable before ACL negatives");
    assert_eq!(page["events"][0]["itemId"], "b");
    let cursor = page["nextCursor"].as_str().unwrap();
    // Persist the denied target too: denial must come from ACL, not a missing anchor.
    harness.database.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO turn(id,thread_id,status,turn_kind,origin,initiated_by_actor_kind,initiated_by_actor_id,created_at,updated_at) VALUES ('foreign-private-turn',?,'completed','conversation','user','principal',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [THREAD_RED_PRIVATE_B_ID.into(), MEMBER_B_ID.into()],
    )).await.unwrap();
    let foreign_item = TurnItem::AgentMessage {
        id: "foreign-private-item".into(),
        text: "private B saved content".into(),
        phase: Default::default(),
        markdown: None,
        markdown_version: None,
    };
    harness.database.execute_raw(Statement::from_sql_and_values(
        DatabaseBackend::Sqlite,
        "INSERT INTO turn_item(id,turn_id,item_id,item_type,payload,created_at,updated_at) VALUES ('foreign-source','foreign-private-turn','foreign-private-item','agent_message',?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)",
        [serde_json::to_string(&foreign_item).unwrap().into()],
    )).await.unwrap();
    let foreign = handler
        .read(
            "threads_history_read",
            json!({
                "threadId":THREAD_RED_PRIVATE_B_ID,"limit":1,
                "anchor":{"turnId":"foreign-private-turn","itemId":"foreign-private-item"}
            }),
        )
        .await
        .err()
        .unwrap();
    let safe = super::super::safe_read_error(&foreign).to_string();
    for secret in [
        THREAD_RED_PRIVATE_B_ID,
        "foreign-private-turn",
        "foreign-private-item",
        "Red private B",
        "private B saved content",
    ] {
        assert!(!safe.contains(secret));
    }
    assert!(safe.contains("source unavailable or access denied"));
    harness
        .database
        .execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            "DELETE FROM workspace_membership WHERE principal_id=? AND workspace_id=?",
            [MEMBER_A_ID.into(), WORKSPACE_RED_ID.into()],
        ))
        .await
        .unwrap();
    let revoked = handler
        .read(
            "threads_history_read",
            json!({
                "threadId":THREAD_RED_PRIVATE_A_ID,"limit":1,"cursor":cursor,"direction":"older"
            }),
        )
        .await
        .err()
        .unwrap();
    let safe = super::super::safe_read_error(&revoked).to_string();
    assert!(safe.contains("source unavailable or access denied"));
    assert!(!safe.contains(THREAD_RED_PRIVATE_A_ID));
    assert!(!safe.contains(TURN));
}
