use super::*;
use sea_orm::{DbBackend, Statement, TransactionTrait};

#[pioneer_postgres_test::postgres_test]
async fn exact_tool_item_reads_preserve_canonical_revisions_with_compression() {
    use pioneer_crud_conversations::turn_item::{ItemWrite, upsert};
    use pioneer_crud_payloads::storage::PreparedPayload;

    for compressed in [false, true] {
        let db = fixture().await;
        for id in ["compressed", "inline"] {
            if compressed {
                let wire = serde_json::json!({
                    "storage": {"kind": "shell"},
                    "output": "retained result 🌍".repeat(8_192),
                })
                .to_string();
                let body = PreparedPayload::new_turn_item(&wire).unwrap();
                let now = sea_orm::prelude::DateTimeUtc::from(std::time::SystemTime::now())
                    .fixed_offset();
                let tx = db.begin().await.unwrap();
                upsert(
                    &tx,
                    ItemWrite {
                        id: id.into(),
                        turn_id: "turn".into(),
                        item_id: id.into(),
                        item_type: "command_execution".into(),
                        status: Some("completed".into()),
                        created_at: now,
                        updated_at: now,
                    },
                    &body,
                )
                .await
                .unwrap();
                tx.commit().await.unwrap();
                let blocks = db.query_one_raw(Statement::from_sql_and_values(
                    DbBackend::Postgres,
                    "SELECT count(*) AS n FROM portable_payload_part WHERE payload_id=$1 AND codec=1",
                    [body.id().into()],
                )).await.unwrap().unwrap();
                assert!(blocks.try_get::<i64>("", "n").unwrap() > 1);
            } else {
                db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                    r#"INSERT INTO turn_item(id,turn_id,item_id,item_type,status,payload,created_at,updated_at) VALUES ($1,'turn',$2,'command_execution','completed','{"storage":{"kind":"shell"},"output":"retained result"}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)"#,
                    [id.into(), id.into()])).await.unwrap();
            }
        }
        // Both committed representations have revisions from the canonical
        // insert trigger and must be readable through the query-only pool.
        for id in ["compressed", "inline"] {
            let reference = compaction_tool_item_reference(&db, "ws", "thread", "turn", id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(reference.version, "item-revision:1");
            let fragment = compaction_reference_fragment(&db, "ws", "thread", &reference, 0)
                .await
                .unwrap()
                .unwrap();
            assert!(fragment.text.contains("retained result"));
            assert_eq!(fragment.reference, reference);
            assert_eq!(
                compaction_replay_item_id(&db, "ws", "thread", &reference)
                    .await
                    .unwrap()
                    .as_deref(),
                Some(id)
            );
            for (workspace, thread) in [("other", "thread"), ("ws", "other")] {
                assert!(
                    compaction_reference_fragment(&db, workspace, thread, &reference, 0)
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(
                    compaction_replay_item_id(&db, workspace, thread, &reference)
                        .await
                        .unwrap()
                        .is_none()
                );
            }
            db.execute_raw(Statement::from_sql_and_values(DbBackend::Postgres,
                "UPDATE turn_item SET payload=to_jsonb(json_build_object('storage',json_build_object('kind','shell'),'output','changed result'))::json,payload_payload_id=NULL WHERE id=$1", [id.into()]
            )).await.unwrap();
            assert!(
                compaction_reference_fragment(&db, "ws", "thread", &reference, 0)
                    .await
                    .is_err()
            );
            assert!(
                compaction_replay_item_id(&db, "ws", "thread", &reference)
                    .await
                    .unwrap()
                    .is_none()
            );
            let current = compaction_tool_result_fragment(&db, "ws", "thread", "turn", id, None, 0)
                .await
                .unwrap()
                .unwrap();
            assert!(current.text.contains("changed result"));
            assert_ne!(current.reference.version, reference.version);
            db.execute_raw(Statement::from_sql_and_values(
                DbBackend::Postgres,
                "DELETE FROM turn_item WHERE id=$1",
                [id.into()],
            ))
            .await
            .unwrap();
            assert!(
                compaction_reference_fragment(&db, "ws", "thread", &current.reference, 0)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
    }
}

async fn fixture() -> PostgresDatabase {
    let fixture = pioneer_postgres_test::scoped_database().await.unwrap();
    let writer = fixture.connection();
    writer
        .execute_unprepared(
            "INSERT INTO workspace(id,name) VALUES ('ws','fixture');
             INSERT INTO thread(id,workspace_id,preview,mode,model,model_provider,status)
                 VALUES ('thread','ws','','agent','m','p','active');
             INSERT INTO turn(id,thread_id,status) VALUES ('turn','thread','completed');",
        )
        .await
        .unwrap();
    let db = PostgresDatabase::new(fixture.connect_read_only().await.unwrap(), writer.clone())
        .maintenance();
    assert!(db.reader_is_read_only().await.unwrap());
    db
}

#[pioneer_postgres_test::postgres_test]
async fn history_capture_preserves_compressed_event_across_a_fence() {
    let db = fixture().await;
    use crate::history_fence::compaction_history_read_fence;
    use crate::history_turn_pages::compaction_history_turn_page;
    use crate::plan_test_support::seed_portable_event;
    let wire = format!(r#"{{"text":"{}"}}"#, "retained original 🌍".repeat(4096));
    seed_portable_event(&db, "first", 1, &wire).await;
    let backed = db.query_one_raw(Statement::from_string(DbBackend::Postgres,
        "SELECT p.original_bytes, EXISTS (SELECT 1 FROM portable_payload_part b WHERE b.payload_id=p.id AND b.codec=1) AS compressed
         FROM turn_event e JOIN portable_payload_value p ON p.id=e.payload_payload_id WHERE e.id='first'"
    )).await.unwrap().unwrap();
    assert_eq!(
        backed.try_get::<i64>("", "original_bytes").unwrap(),
        wire.len() as i64
    );
    assert!(backed.try_get::<bool>("", "compressed").unwrap());
    let first = compaction_source_metadata_page(&db, "ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap()
        .entries
        .remove(0)
        .reference;
    let fence = compaction_history_read_fence(&db).await.unwrap();
    let before = compaction_history_turn_page(&db, "ws", "thread", "", &fence)
        .await
        .unwrap();
    assert_eq!(before.len(), 1);
    seed_portable_event(&db, "later", 2, r#"{"text":"after the captured fence"}"#).await;
    let after = compaction_history_turn_page(&db, "ws", "thread", "", &fence)
        .await
        .unwrap();
    assert_eq!(after.len(), before.len());
    let (before, after) = (&before[0], &after[0]);
    assert_eq!(after.id, before.id);
    assert_eq!(after.created_at, before.created_at);
    assert_eq!(after.creation_order, before.creation_order);
    assert_eq!(after.legacy_creation_order, before.legacy_creation_order);
    assert_eq!(after.status, before.status);
    assert_eq!(after.turn_kind, before.turn_kind);
    assert_eq!(after.send_mode, before.send_mode);
    assert_eq!(after.input_high_water, before.input_high_water);
    assert_eq!(after.event_high_water, before.event_high_water);
    assert_eq!(after.context_high_water, before.context_high_water);
    assert_eq!(after.event_high_water, 1);
    let next_fence = compaction_history_read_fence(&db).await.unwrap();
    assert_eq!(
        compaction_history_turn_page(&db, "ws", "thread", "", &next_fence)
            .await
            .unwrap()[0]
            .event_high_water,
        2
    );
    assert!(
        compaction_history_turn_page(&db, "other", "thread", "", &fence)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        compaction_history_turn_page(&db, "ws", "thread", &after.id, &fence)
            .await
            .unwrap()
            .is_empty()
    );
    let mut rebuilt = String::new();
    let mut offset = 0;
    loop {
        let page = compaction_reference_fragment(&db, "ws", "thread", &first, offset)
            .await
            .unwrap()
            .unwrap();
        rebuilt.push_str(&page.text);
        match page.next_character {
            Some(next) => offset = next,
            None => break,
        }
    }
    assert_eq!(rebuilt.as_bytes(), wire.as_bytes());
}

#[pioneer_postgres_test::postgres_test]
async fn poison_source_progress_and_scope_are_bounded() {
    let db = fixture().await;
    db.execute_raw(Statement::from_sql_and_values(
        DbBackend::Postgres,
        "INSERT INTO turn_event(id,thread_id,turn_id,sequence,event_type,payload)
             VALUES ('huge','thread','turn',1,'fixture',to_json($1::text)),
                    ('next','thread','turn',2,'fixture',to_json('small'::text))",
        ["x".repeat(SOURCE_PAGE_BYTES + 1).into()],
    ))
    .await
    .unwrap();
    let page = compaction_source_page(&db, "ws", "thread", "turn", PagedSource::Event, 0)
        .await
        .unwrap();
    assert!(page.entries[0].incomplete);
    assert_eq!(page.entries[1].payload.as_deref(), Some(r#""small""#));
    assert_eq!(page.next_sequence, 2);
    assert!(
        compaction_source_page(&db, "other", "thread", "turn", PagedSource::Event, 0)
            .await
            .unwrap()
            .entries
            .is_empty()
    );
    assert_eq!(
        db.read_class(),
        pioneer_postgres_pools::PostgresReadClass::Maintenance
    );
    assert_eq!(
        db.write_class(),
        pioneer_postgres_pools::PostgresWriteClass::Maintenance
    );
}
