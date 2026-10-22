use super::{start_test_turn, test_store_with_workspace};
use pioneer_entity::turn;
use pioneer_protocol::UserInput;
use sea_orm::sea_query::Expr;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter};

#[tokio::test]
async fn canonical_history_keyset_handles_timestamp_ties_snapshot_appends_and_edits() {
    let mut store = test_store_with_workspace("saved-history").await;
    let at = crate::util::unix_to_datetime(1_700_000_000);
    for id in ["turn-a", "turn-c", "turn-b"] {
        (store, _, _) = start_test_turn(store, "saved-history", "parent", id).await;
        turn::Entity::update_many()
            .col_expr(turn::Column::CreatedAt, Expr::value(at))
            .filter(turn::Column::Id.eq(id))
            .exec(&store.connection)
            .await
            .unwrap();
    }
    let snapshot = store.saved_history_snapshot("parent").await.unwrap();
    let first = store
        .saved_history_page_at_snapshot("parent", None, None, 2, &snapshot)
        .await
        .unwrap();
    assert_eq!(
        first.iter().map(|r| r.turn_id.as_str()).collect::<Vec<_>>(),
        ["turn-c", "turn-b"]
    );
    let boundary = first[0].key.clone();
    let before = first[1].key.clone();
    let revision = store
        .saved_history_revision_at_snapshot("parent", &boundary, &snapshot)
        .await
        .unwrap();
    (store, _, _) = start_test_turn(store, "saved-history", "parent", "new-turn").await;
    // Even a newly saved record at the same timestamp and inside the old
    // lexicographic boundary is outside this traversal's insertion watermark.
    turn::Entity::update_many()
        .col_expr(turn::Column::CreatedAt, Expr::value(at))
        .filter(turn::Column::Id.eq("new-turn"))
        .exec(&store.connection)
        .await
        .unwrap();
    let old = store
        .saved_history_page_at_snapshot("parent", Some(&boundary), Some(&before), 2, &snapshot)
        .await
        .unwrap();
    assert_eq!(
        old.iter().map(|r| r.turn_id.as_str()).collect::<Vec<_>>(),
        ["turn-a"]
    );
    assert_eq!(
        store
            .saved_history_revision_at_snapshot("parent", &boundary, &snapshot)
            .await
            .unwrap(),
        revision
    );
    assert!(
        store
            .saved_history_page_at_snapshot("child", Some(&boundary), None, 2, &snapshot)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .replace_turn_input_projection_for_test(
            "turn-a",
            &[UserInput::Text {
                text: "edited".into(),
                text_elements: vec![],
            }],
            1,
            1_700_000_010,
        )
        .await
        .unwrap();
    assert_ne!(
        store
            .saved_history_revision_at_snapshot("parent", &boundary, &snapshot)
            .await
            .unwrap(),
        revision
    );
}

#[tokio::test]
async fn deleted_turn_inputs_are_not_returned_by_canonical_history_scan() {
    let store = test_store_with_workspace("deleted-history").await;
    let (store, _, _) = start_test_turn(store, "deleted-history", "thread", "turn").await;
    store
        .replace_turn_input_projection_for_test(
            "turn",
            &[UserInput::Text {
                text: "removed".into(),
                text_elements: vec![],
            }],
            1,
            1_700_000_010,
        )
        .await
        .unwrap();
    let snapshot = store.saved_history_snapshot("thread").await.unwrap();
    assert!(
        store
            .saved_history_page_at_snapshot("thread", None, None, 100, &snapshot)
            .await
            .unwrap()
            .iter()
            .any(|r| r.key.kind == "input")
    );
    turn::Entity::update_many()
        .col_expr(
            turn::Column::MessageDeletedAt,
            Expr::value(crate::util::unix_to_datetime(1_700_000_020)),
        )
        .filter(turn::Column::Id.eq("turn"))
        .exec(&store.connection)
        .await
        .unwrap();
    let rows = store
        .saved_history_page_at_snapshot("thread", None, None, 100, &snapshot)
        .await
        .unwrap();
    assert!(
        !rows
            .iter()
            .any(|r| r.key.kind == "input" || r.key.kind == "item")
    );
    assert_eq!(
        rows.len(),
        1,
        "the authorized deleted-turn tombstone remains readable"
    );
}

// Exercise the same public traversal over tables and real sqlite-zstd views.
// In the compressed case the traversal starts BEFORE enable_transparent: storage
// conversion and compression must not make the existing snapshot stale.
#[tokio::test]
async fn saved_payload_history_plain_storage() {
    saved_payload_history(false).await;
}

#[tokio::test]
async fn saved_payload_history_survives_zstd_conversion_and_compression() {
    saved_payload_history(true).await;
}

async fn saved_payload_history(compressed: bool) {
    use pioneer_protocol::{AgentMessagePhase, ItemCompletedNotification, TurnItem};
    use sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

    pioneer_sqlite::zstd::register_auto_extension_once().unwrap();
    let store = test_store_with_workspace("payload-history").await;
    let (store, _, _) = start_test_turn(store, "payload-history", "thread", "turn").await;
    let db = store.database_connection();
    let input = UserInput::Text {
        text: "Вход 🦀 日本語".into(),
        text_elements: vec![],
    };
    for (index, id) in ["a", "b", "c"].into_iter().enumerate() {
        db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
            "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES (?,'turn',?,'text',?,?, '2026-01-01 00:00:00+00:00')",
            vec![format!("input-{id}").into(), (index as i64).into(), "Вход 🦀 日本語".into(), serde_json::to_string(&input).unwrap().into()])).await.unwrap();
        store
            .materialize_item_completed(
                ItemCompletedNotification {
                    workspace_id: "payload-history".into(),
                    thread_id: "thread".into(),
                    turn_id: "turn".into(),
                    item: TurnItem::AgentMessage {
                        id: format!("item-{id}"),
                        text: "Ответ 😀 中文".into(),
                        phase: AgentMessagePhase::FinalAnswer,
                        markdown: None,
                        markdown_version: None,
                    },
                },
                1_767_225_600,
            )
            .await
            .unwrap();
    }
    // Put every source on the same timestamp, making the unique tie breaker
    // observable independently of the fixture's insertion order.
    for table in ["turn", "turn_input", "turn_item"] {
        db.execute_raw(Statement::from_string(
            DatabaseBackend::Sqlite,
            format!("UPDATE {table} SET created_at='2026-01-01 00:00:00+00:00'"),
        ))
        .await
        .unwrap();
    }
    // Legacy rows predate revision-ledger registration. They still belong to
    // saved history; demanding a revision JOIN would silently lose them.
    db.execute_unprepared("DELETE FROM compaction_input_revision WHERE source_id='input-a'; DELETE FROM compaction_item_revision WHERE source_id IN (SELECT id FROM turn_item WHERE item_id='item-a')").await.unwrap();
    let snapshot = store.saved_history_snapshot("thread").await.unwrap();
    let first = store
        .saved_history_page_at_snapshot("thread", None, None, 2, &snapshot)
        .await
        .unwrap();
    let boundary = first[0].key.clone();
    let revision = store
        .saved_history_revision_at_snapshot("thread", &boundary, &snapshot)
        .await
        .unwrap();
    if compressed {
        for table in ["turn_input", "turn_item"] {
            db.query_one_write_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
                "SELECT zstd_enable_transparent(?)",
                [serde_json::json!({"table":table,"column":"payload","compression_level":3,"dict_chooser":"'[nodict]'"}).to_string().into()])).await.unwrap();
            // Use the established real-compressed-row fixture, with compression
            // outside database capacity and the production writer route.
            let rows = db
                .query_all_raw(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    format!("SELECT id,payload FROM _{table}_zstd WHERE _payload_dict IS NULL"),
                ))
                .await
                .unwrap();
            for row in rows {
                let id: String = row.try_get("", "id").unwrap();
                let payload: String = row.try_get("", "payload").unwrap();
                let bytes =
                    pioneer_sqlite::zstd::compress_column_value(payload.as_bytes(), 3, None)
                        .unwrap();
                db.execute_raw(Statement::from_sql_and_values(
                    DatabaseBackend::Sqlite,
                    format!("UPDATE _{table}_zstd SET payload=?,_payload_dict=-1 WHERE id=?"),
                    [bytes.into(), id.into()],
                ))
                .await
                .unwrap();
            }
        }
        assert_eq!(
            store.saved_history_snapshot("thread").await.unwrap(),
            snapshot
        );
        assert_eq!(
            store
                .saved_history_revision_at_snapshot("thread", &boundary, &snapshot)
                .await
                .unwrap(),
            revision
        );
    }
    assert_eq!(
        store.saved_history_input("turn", "input-a").await.unwrap(),
        Some(input.clone())
    );
    let item = store
        .get_turn_item("turn", "item-a")
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(item, TurnItem::AgentMessage { text, .. } if text == "Ответ 😀 中文"));

    let anchor = store
        .saved_history_anchor("thread", "turn", Some("item-a"), &snapshot)
        .await
        .unwrap()
        .expect("legacy anchor works through either storage representation");
    let near = store
        .saved_history_page_direction_at_snapshot(
            "thread",
            Some(&boundary),
            Some(&anchor.key),
            true,
            false,
            1,
            &snapshot,
        )
        .await
        .unwrap();
    assert_eq!(near[0].item_id.as_deref(), Some("item-a"));
    let newer = store
        .saved_history_page_direction_at_snapshot(
            "thread",
            Some(&boundary),
            Some(&anchor.key),
            false,
            true,
            100,
            &snapshot,
        )
        .await
        .unwrap();
    assert!(!newer.iter().any(|r| r.key == anchor.key));
    assert!(
        store
            .saved_history_anchor("another-thread", "turn", Some("item-a"), &snapshot)
            .await
            .unwrap()
            .is_none()
    );
    // New same-time sources sort inside the old key range but must remain out
    // of the frozen traversal, including when written through a zstd view.
    db.execute_raw(Statement::from_sql_and_values(DatabaseBackend::Sqlite,
        "INSERT INTO turn_input(id,turn_id,input_index,input_type,text,payload,created_at) VALUES ('input-aa','turn',9,'text','new',?,'2026-01-01 00:00:00+00:00')",
        [serde_json::to_string(&input).unwrap().into()])).await.unwrap();
    store
        .materialize_item_completed(
            ItemCompletedNotification {
                workspace_id: "payload-history".into(),
                thread_id: "thread".into(),
                turn_id: "turn".into(),
                item: TurnItem::AgentMessage {
                    id: "item-aa".into(),
                    text: "new".into(),
                    phase: AgentMessagePhase::FinalAnswer,
                    markdown: None,
                    markdown_version: None,
                },
            },
            1_767_225_600,
        )
        .await
        .unwrap();
    let mut rows = first;
    loop {
        let page = store
            .saved_history_page_at_snapshot(
                "thread",
                Some(&boundary),
                Some(&rows.last().unwrap().key),
                2,
                &snapshot,
            )
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        rows.extend(page);
    }
    assert_eq!(rows.len(), 7);
    assert_eq!(
        rows.iter()
            .filter(|r| r.key.kind == "input")
            .map(|r| r.key.id.as_str())
            .collect::<Vec<_>>(),
        ["input-c", "input-b", "input-a"]
    );
    let mut items = rows
        .iter()
        .filter(|r| r.key.kind == "item")
        .map(|r| r.item_id.as_deref().unwrap())
        .collect::<Vec<_>>();
    items.sort_unstable();
    assert_eq!(items, ["item-a", "item-b", "item-c"]);
    assert!(rows.windows(2).all(|pair| {
        let a = &pair[0].key;
        let b = &pair[1].key;
        (&a.created_at, &a.kind, &a.id) > (&b.created_at, &b.kind, &b.id)
    }));
    assert_eq!(
        store
            .saved_history_revision_at_snapshot("thread", &boundary, &snapshot)
            .await
            .unwrap(),
        revision
    );

    for table in ["turn_input", "turn_item"] {
        let column = if table == "turn_input" {
            "id"
        } else {
            "item_id"
        };
        let id = if table == "turn_input" {
            "input-a"
        } else {
            "item-a"
        };
        let before_edit = store
            .saved_history_revision_at_snapshot("thread", &boundary, &snapshot)
            .await
            .unwrap();
        let edited_payload = if table == "turn_input" {
            serde_json::to_string(&UserInput::Text {
                text: "Изменено 🦀".into(),
                text_elements: vec![],
            })
            .unwrap()
        } else {
            serde_json::to_string(&TurnItem::AgentMessage {
                id: id.into(),
                text: "Изменено 🦀".into(),
                phase: AgentMessagePhase::FinalAnswer,
                markdown: None,
                markdown_version: None,
            })
            .unwrap()
        };
        db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!("UPDATE {table} SET payload=? WHERE {column}=?"),
            [edited_payload.into(), id.into()],
        ))
        .await
        .unwrap();
        let edited = store
            .saved_history_revision_at_snapshot("thread", &boundary, &snapshot)
            .await
            .unwrap();
        assert_ne!(
            edited, before_edit,
            "logical edit invalidates frozen traversal"
        );
        db.execute_raw(Statement::from_sql_and_values(
            DatabaseBackend::Sqlite,
            format!("DELETE FROM {table} WHERE {column}=?"),
            [id.into()],
        ))
        .await
        .unwrap();
        assert_ne!(
            store
                .saved_history_revision_at_snapshot("thread", &boundary, &snapshot)
                .await
                .unwrap(),
            edited,
            "tombstone invalidates continuation after deletion"
        );
        let remaining = store
            .saved_history_page_at_snapshot("thread", Some(&boundary), None, 100, &snapshot)
            .await
            .unwrap();
        assert!(!remaining.iter().any(|r| r.key.id == "input-a"
            || r.item_id.as_deref() == Some("item-a") && table == "turn_item"));
    }
}
